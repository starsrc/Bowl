package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

// DiffDiskParams 描述差异对比请求的控制面参数。
type DiffDiskParams struct {
	SourceDiskID  string
	TargetDiskID  string
	PathPrefix    []byte
	ConflictsOnly bool
	PageSize      uint32
	PageToken     string
}

// DiffDiskResult 描述差异对比返回的公共祖先、统计摘要与分页条目。
type DiffDiskResult struct {
	LCACommitID   string
	Summary       store.DiffSummary
	Entries       []store.DiffEntry
	NextPageToken string
}

// ReadCommitNodeParams 描述从指定不可变历史提交切面分片读取节点内容的参数。
type ReadCommitNodeParams struct {
	DiskID   string
	CommitID string
	Ino      uint64
	Offset   uint64
	Size     uint32
}

// MergeDiskParams 描述三路合并请求的控制面参数。
type MergeDiskParams struct {
	SourceDiskID   string
	TargetDiskID   string
	Strategy       store.MergeStrategy
	PathStrategies map[string]store.MergeStrategy
	DryRun         bool
	Force          bool
	MaxBytes       *uint64
}

// MergeDiskResult 描述三路合并返回的结果视图。
type MergeDiskResult struct {
	Disk               *Disk
	LCACommitID        string
	MergeCommitID      string
	EstimatedUsedBytes uint64
	TargetMaxBytes     uint64
}

// MergeConflictError 携带底层存储引擎检出的三路合并冲突报告，供服务端编码为 ECONFLICT 详情。
type MergeConflictError struct {
	LCACommitID string
	Report      store.MergeConflictReport
	Err         error
}

// Error 返回冲突错误的可读摘要。
func (e *MergeConflictError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return fmt.Sprintf("merge conflicts detected (%d total): %s", e.Report.TotalConflicts, e.Err.Error())
	}
	return fmt.Sprintf("merge conflicts detected (%d total)", e.Report.TotalConflicts)
}

// Unwrap 暴露底层哨兵错误链，保持与 store.ErrMergeConflict 的双向判定。
func (e *MergeConflictError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (m *manager) stagingMergeDir(diskID string, commitID uuid.UUID) string {
	return filepath.Join(m.storageRoot, "disks", diskID+".merge."+commitID.String())
}

func (m *manager) oldMergeDir(diskID string, commitID uuid.UUID) string {
	return filepath.Join(m.storageRoot, "disks", diskID+".old."+commitID.String())
}

func (m *manager) srcCommitMarkerPath(diskID string, mergeCommitID, srcCommitID uuid.UUID) string {
	return filepath.Join(m.storageRoot, "disks", fmt.Sprintf("%s.srccommit.%s.%s", diskID, mergeCommitID.String(), srcCommitID.String()))
}

func (m *manager) tmpSnapshotPath(prefix string) string {
	return filepath.Join(m.storageRoot, ".tmp", fmt.Sprintf("%s-%s", prefix, uuid.Must(uuid.NewV7()).String()))
}

func syncDirPath(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	return errors.Join(syncErr, closeErr)
}

func parseCommitID(commitID string) (uuid.UUID, error) {
	id, err := uuid.Parse(commitID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: invalid commit id %q", ErrInvalidArgument, commitID)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: commit id cannot be nil uuid", ErrInvalidArgument)
	}
	return id, nil
}

// lockDiskPair 按 UUID 字典序严格升序获取双盘控制面互斥锁，杜绝互锁并发请求产生死锁。
func (m *manager) lockDiskPair(ctx context.Context, diskA, diskB string) func() {
	if diskA == diskB {
		return m.lockDisk(ctx, diskA)
	}
	first, second := diskA, diskB
	if second < first {
		first, second = second, first
	}
	unlockFirst := m.lockDisk(ctx, first)
	unlockSecond := m.lockDisk(ctx, second)
	return func() {
		unlockSecond()
		unlockFirst()
	}
}

func (m *manager) sharedCache() (*pebble.Cache, error) {
	m.poolMu.RLock()
	defer m.poolMu.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}
	return m.cache, nil
}

// snapshotDiskForInspect 在不与 RW Pebble 句柄并发打开同一目录的前提下，
// 通过在线 Actor 的 Checkpoint 机制生成一致性只读快照并返回清理函数。
func (m *manager) snapshotDiskForInspect(
	ctx context.Context,
	diskID string,
	lineageID uuid.UUID,
	prefix string,
) (snapDir string, hasOrphans bool, cleanup func(), err error) {
	actor, err := m.getOrOpenActorWithLineageLocked(ctx, diskID, lineageID)
	if err != nil {
		return "", false, nil, err
	}
	tmpRoot := filepath.Join(m.storageRoot, ".tmp")
	if mkErr := os.MkdirAll(tmpRoot, 0o755); mkErr != nil {
		return "", false, nil, mkErr
	}
	snapDir = m.tmpSnapshotPath(prefix)
	stats, err := actor.Checkpoint(ctx, snapDir)
	if err != nil {
		_ = m.removeAll(snapDir)
		return "", false, nil, err
	}
	cleanup = func() {
		if rmErr := m.removeAll(snapDir); rmErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "cleanup temporary inspect snapshot failed",
				"disk_id", diskID,
				"dir", snapDir,
				"err", rmErr,
			)
		}
	}
	return snapDir, stats.HasOrphans, cleanup, nil
}

// findLCA 基于同血缘提交 DAG 计算 commitA 与 commitB 的唯一极大公共祖先（MCA）。
// 当且仅当 |MCA| == 1 时返回该提交；若存在多个不可比较公共祖先（Criss-Cross）则返回 ErrAmbiguousLCA。
func findLCA(commits []db.CommitRecord, commitA, commitB uuid.UUID) (uuid.UUID, error) {
	if commitA == uuid.Nil || commitB == uuid.Nil {
		return uuid.Nil, ErrNoBaseCommit
	}
	byID := make(map[uuid.UUID]db.CommitRecord, len(commits))
	for _, c := range commits {
		byID[c.CommitID] = c
	}
	if _, ok := byID[commitA]; !ok {
		return uuid.Nil, fmt.Errorf("%w: commit %s not found in lineage", ErrNotFound, commitA)
	}
	if _, ok := byID[commitB]; !ok {
		return uuid.Nil, fmt.Errorf("%w: commit %s not found in lineage", ErrNotFound, commitB)
	}
	if commitA == commitB {
		return commitA, nil
	}

	collectAncestors := func(start uuid.UUID) map[uuid.UUID]struct{} {
		visited := make(map[uuid.UUID]struct{})
		queue := []uuid.UUID{start}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			if cur == uuid.Nil {
				continue
			}
			if _, seen := visited[cur]; seen {
				continue
			}
			rec, exists := byID[cur]
			if !exists {
				continue
			}
			visited[cur] = struct{}{}
			if rec.ParentCommitID != uuid.Nil {
				queue = append(queue, rec.ParentCommitID)
			}
			if rec.MergeParentCommitID != uuid.Nil {
				queue = append(queue, rec.MergeParentCommitID)
			}
		}
		return visited
	}

	ancA := collectAncestors(commitA)
	ancB := collectAncestors(commitB)

	common := make(map[uuid.UUID]struct{})
	for id := range ancA {
		if _, inB := ancB[id]; inB {
			common[id] = struct{}{}
		}
	}
	if len(common) == 0 {
		return uuid.Nil, fmt.Errorf("%w: commits %s and %s share no common ancestor", ErrNoBaseCommit, commitA, commitB)
	}

	// 因为 ancA ∩ ancB 对祖先闭包封闭，任何被其他公共祖先支配的节点必为其某个公共祖先的直接或间接父代；
	// 沿着公共集合内的父链向上标记即可精确筛选所有极大公共祖先（MCA）。
	dominated := make(map[uuid.UUID]bool, len(common))
	for id := range common {
		rec := byID[id]
		if rec.ParentCommitID != uuid.Nil {
			if _, ok := common[rec.ParentCommitID]; ok {
				dominated[rec.ParentCommitID] = true
			}
		}
		if rec.MergeParentCommitID != uuid.Nil {
			if _, ok := common[rec.MergeParentCommitID]; ok {
				dominated[rec.MergeParentCommitID] = true
			}
		}
	}

	var mca []uuid.UUID
	for id := range common {
		if !dominated[id] {
			mca = append(mca, id)
		}
	}
	if len(mca) == 0 {
		return uuid.Nil, fmt.Errorf("%w: malformed commit DAG cycle detected", ErrNoBaseCommit)
	}
	if len(mca) > 1 {
		return uuid.Nil, fmt.Errorf("%w: %d maximal common ancestors found between %s and %s", ErrAmbiguousLCA, len(mca), commitA, commitB)
	}
	return mca[0], nil
}

func (m *manager) DiffDisk(ctx context.Context, params *DiffDiskParams) (*DiffDiskResult, error) {
	if params == nil {
		return nil, fmt.Errorf("%w: params cannot be nil", ErrInvalidArgument)
	}
	if params.PageSize > store.MaxDiffPageSize {
		return nil, fmt.Errorf("%w: page_size %d exceeds max %d", ErrInvalidArgument, params.PageSize, store.MaxDiffPageSize)
	}
	sourceID, err := parseDiskID(params.SourceDiskID)
	if err != nil {
		return nil, err
	}
	sourceKey := sourceID.String()

	var (
		targetID  uuid.UUID
		targetKey string
		hasTarget bool
	)
	if params.TargetDiskID != "" {
		targetID, err = parseDiskID(params.TargetDiskID)
		if err != nil {
			return nil, err
		}
		if targetID == sourceID {
			return nil, fmt.Errorf("%w: source_disk_id and target_disk_id must differ", ErrInvalidArgument)
		}
		targetKey = targetID.String()
		hasTarget = true
	}

	cache, err := m.sharedCache()
	if err != nil {
		return nil, err
	}

	if hasTarget {
		unlock := m.lockDiskPair(ctx, sourceKey, targetKey)
		defer unlock()
	} else {
		unlock := m.lockDisk(ctx, sourceKey)
		defer unlock()
	}

	sourceRow, err := m.store.GetDisk(ctx, sourceID)
	if err != nil {
		return nil, mapDBErr(err)
	}

	var lcaCommitID uuid.UUID
	var targetRow db.DiskRecord
	if hasTarget {
		targetRow, err = m.store.GetDisk(ctx, targetID)
		if err != nil {
			return nil, mapDBErr(err)
		}
		if sourceRow.LineageID != targetRow.LineageID {
			return nil, fmt.Errorf("%w: source lineage %s != target lineage %s", ErrCrossLineage, sourceRow.LineageID, targetRow.LineageID)
		}
		if sourceRow.BaseCommitID == uuid.Nil || targetRow.BaseCommitID == uuid.Nil {
			return nil, ErrNoBaseCommit
		}
		commits, listErr := m.store.ListLineageCommits(ctx, sourceRow.LineageID)
		if listErr != nil {
			return nil, mapDBErr(listErr)
		}
		lcaCommitID, err = findLCA(commits, sourceRow.BaseCommitID, targetRow.BaseCommitID)
		if err != nil {
			return nil, err
		}
	} else {
		if sourceRow.BaseCommitID == uuid.Nil {
			return nil, ErrNoBaseCommit
		}
		lcaCommitID = sourceRow.BaseCommitID
	}

	sourceSnapDir, _, cleanupSrc, err := m.snapshotDiskForInspect(ctx, sourceKey, sourceRow.LineageID, "diff-src")
	if err != nil {
		return nil, err
	}
	defer cleanupSrc()

	var targetSnapDir string
	if hasTarget {
		var cleanupTgt func()
		targetSnapDir, _, cleanupTgt, err = m.snapshotDiskForInspect(ctx, targetKey, targetRow.LineageID, "diff-tgt")
		if err != nil {
			return nil, err
		}
		defer cleanupTgt()
	}

	m.lineageCommitMu.RLock()
	defer m.lineageCommitMu.RUnlock()

	diffRes, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:      m.commitDir(lcaCommitID.String()),
		TargetRoot:    targetSnapDir,
		SourceRoot:    sourceSnapDir,
		PathPrefix:    params.PathPrefix,
		ConflictsOnly: params.ConflictsOnly,
		PageSize:      params.PageSize,
		PageToken:     params.PageToken,
		Cache:         cache,
	})
	if err != nil {
		return nil, mapInspectErr(err)
	}

	return &DiffDiskResult{
		LCACommitID:   lcaCommitID.String(),
		Summary:       diffRes.Summary,
		Entries:       diffRes.Entries,
		NextPageToken: diffRes.NextPageToken,
	}, nil
}

func (m *manager) ReadCommitNode(ctx context.Context, params *ReadCommitNodeParams) (*store.ReadNodeResult, error) {
	if params == nil {
		return nil, fmt.Errorf("%w: params cannot be nil", ErrInvalidArgument)
	}
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	commitID, err := parseCommitID(params.CommitID)
	if err != nil {
		return nil, err
	}
	if params.Ino == 0 || params.Ino > uint64(math.MaxInt64) || params.Offset > store.MaxFileSize || params.Size > store.MaxReadNodeSliceBytes {
		return nil, ErrInvalidArgument
	}
	if _, err = m.sharedCache(); err != nil {
		return nil, err
	}

	diskRow, err := m.store.GetDisk(ctx, diskID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	commits, err := m.store.ListLineageCommits(ctx, diskRow.LineageID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	belongsToLineage := false
	for _, c := range commits {
		if c.CommitID == commitID {
			belongsToLineage = true
			break
		}
	}

	m.lineageCommitMu.RLock()
	defer m.lineageCommitMu.RUnlock()

	cDir := m.commitDir(commitID.String())
	if !belongsToLineage {
		if _, statErr := os.Stat(cDir); statErr == nil {
			return nil, fmt.Errorf("%w: commit %s does not belong to disk lineage %s", ErrCrossLineage, commitID, diskRow.LineageID)
		}
		return nil, fmt.Errorf("%w: commit %s not found in lineage %s", ErrNotFound, commitID, diskRow.LineageID)
	}

	res, err := store.ReadCommitNodeSlice(cDir, params.Ino, params.Offset, params.Size)
	if err != nil {
		return nil, mapInspectErr(err)
	}
	return res, nil
}

func (m *manager) MergeDisk(ctx context.Context, params *MergeDiskParams) (*MergeDiskResult, error) {
	if params == nil {
		return nil, fmt.Errorf("%w: params cannot be nil", ErrInvalidArgument)
	}
	sourceID, err := parseDiskID(params.SourceDiskID)
	if err != nil {
		return nil, err
	}
	targetID, err := parseDiskID(params.TargetDiskID)
	if err != nil {
		return nil, err
	}
	if sourceID == targetID {
		return nil, fmt.Errorf("%w: source_disk_id and target_disk_id must differ", ErrInvalidArgument)
	}

	var overrideMaxBytes uint64
	if params.MaxBytes != nil {
		if *params.MaxBytes == 0 || *params.MaxBytes > uint64(math.MaxInt64) {
			return nil, fmt.Errorf("%w: max_bytes must be within [1, MaxInt64]", ErrInvalidArgument)
		}
		overrideMaxBytes = *params.MaxBytes
	}

	strategy := params.Strategy
	if strategy == store.MergeStrategyUnspecified {
		strategy = store.MergeStrategyFail
	}
	switch strategy {
	case store.MergeStrategyFail, store.MergeStrategyOurs, store.MergeStrategyTheirs:
	default:
		return nil, fmt.Errorf("%w: unsupported merge strategy %d", ErrInvalidArgument, strategy)
	}

	cache, err := m.sharedCache()
	if err != nil {
		return nil, err
	}

	sourceKey := sourceID.String()
	targetKey := targetID.String()
	unlock := m.lockDiskPair(ctx, sourceKey, targetKey)
	defer unlock()

	sourceRow, err := m.store.GetDisk(ctx, sourceID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	targetRow, err := m.store.GetDisk(ctx, targetID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	if sourceRow.LineageID != targetRow.LineageID {
		return nil, fmt.Errorf("%w: source lineage %s != target lineage %s", ErrCrossLineage, sourceRow.LineageID, targetRow.LineageID)
	}
	if sourceRow.BaseCommitID == uuid.Nil || targetRow.BaseCommitID == uuid.Nil {
		return nil, ErrNoBaseCommit
	}

	commits, err := m.store.ListLineageCommits(ctx, sourceRow.LineageID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	lcaCommitID, err := findLCA(commits, sourceRow.BaseCommitID, targetRow.BaseCommitID)
	if err != nil {
		return nil, err
	}

	if !params.DryRun && !params.Force {
		if gateErr := m.checkMergeMountGate(ctx, &sourceRow, &targetRow); gateErr != nil {
			return nil, gateErr
		}
	}

	// Phase 1：目标盘保持在线，利用只读 Checkpoint 快照运行三路合并演算。
	sourceSnapDir, srcHasOrphans, cleanupSrc, err := m.snapshotDiskForInspect(ctx, sourceKey, sourceRow.LineageID, "merge-src")
	if err != nil {
		return nil, err
	}
	defer cleanupSrc()

	targetSnapDir, _, cleanupTgt, err := m.snapshotDiskForInspect(ctx, targetKey, targetRow.LineageID, "merge-tgt")
	if err != nil {
		return nil, err
	}
	defer cleanupTgt()

	epoch0, nextWriteSeq0, _, err := store.InspectBranchStat(targetSnapDir)
	if err != nil {
		return nil, mapInspectErr(err)
	}

	newMergeCommitID := uuid.Must(uuid.NewV7())
	stagingDir := m.stagingMergeDir(targetKey, newMergeCommitID)
	var (
		newSourceCommitID uuid.UUID
		srcCommitDir      string
		mergeCommitDir    string
		srcMarkerPath     string
		skipSourceCommit  bool
	)

	cleanupUncommittedDirs := func() {
		if stagingDir != "" {
			_ = m.removeAll(stagingDir)
		}
		if mergeCommitDir != "" {
			_ = m.removeAll(mergeCommitDir)
		}
		if srcCommitDir != "" {
			_ = m.removeAll(srcCommitDir)
		}
		if srcMarkerPath != "" {
			_ = os.Remove(srcMarkerPath)
		}
	}

	m.lineageCommitMu.RLock()
	baseCommitDir := m.commitDir(sourceRow.BaseCommitID.String())
	srcDiffRes, srcDiffErr := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseCommitDir,
		SourceRoot: sourceSnapDir,
		PageSize:   1,
		Cache:      cache,
	})
	if srcDiffErr != nil {
		m.lineageCommitMu.RUnlock()
		cleanupUncommittedDirs()
		return nil, mapInspectErr(srcDiffErr)
	}
	if srcDiffRes.Summary.Added == 0 && srcDiffRes.Summary.Modified == 0 && srcDiffRes.Summary.Deleted == 0 {
		skipSourceCommit = true
	} else {
		newSourceCommitID = uuid.Must(uuid.NewV7())
	}

	var (
		inoAlloc  store.InodeSegmentAllocator
		packAlloc store.PackSegmentAllocator
	)
	if !params.DryRun {
		inoAlloc, packAlloc = m.newSegmentAllocators(sourceRow.LineageID)
	}

	mergeRes, err := store.MergeTrees(ctx, &store.MergeOptions{
		BaseRoot:            m.commitDir(lcaCommitID.String()),
		TargetRoot:          targetSnapDir,
		SourceRoot:          sourceSnapDir,
		StagingDir:          stagingDir,
		Strategy:            strategy,
		PathStrategies:      params.PathStrategies,
		DryRun:              params.DryRun,
		OverrideMaxBytes:    overrideMaxBytes,
		ExpectedWriterEpoch: epoch0,
		InoAllocator:        inoAlloc,
		PackAllocator:       packAlloc,
		Cache:               cache,
	})
	if err != nil {
		m.lineageCommitMu.RUnlock()
		cleanupUncommittedDirs()
		if conflictErr, ok := errors.AsType[*store.ErrMergeConflict](err); ok {
			return nil, &MergeConflictError{
				LCACommitID: lcaCommitID.String(),
				Report:      conflictErr.Report,
				Err:         conflictErr,
			}
		}
		return nil, mapInspectErr(err)
	}

	if params.DryRun {
		m.lineageCommitMu.RUnlock()
		cleanupUncommittedDirs()
		return &MergeDiskResult{
			LCACommitID:        lcaCommitID.String(),
			EstimatedUsedBytes: mergeRes.UsedBytes,
			TargetMaxBytes:     mergeRes.TargetMaxBytes,
		}, nil
	}

	// 在数据库提交之前预先物化不可变历史切面，并落盘 srccommit 关联标记以支持崩溃回滚精确清理。
	mergeCommitDir = m.commitDir(newMergeCommitID.String())
	if cloneErr := store.CloneCheckpointDir(ctx, stagingDir, mergeCommitDir); cloneErr != nil {
		m.lineageCommitMu.RUnlock()
		cleanupUncommittedDirs()
		return nil, fmt.Errorf("物化合并切面目录: %w", cloneErr)
	}
	if !skipSourceCommit {
		srcMarkerPath = m.srcCommitMarkerPath(targetKey, newMergeCommitID, newSourceCommitID)
		if mkErr := os.WriteFile(srcMarkerPath, nil, 0o644); mkErr != nil {
			m.lineageCommitMu.RUnlock()
			cleanupUncommittedDirs()
			return nil, fmt.Errorf("写入源提交崩溃恢复标记: %w", mkErr)
		}
		_ = syncDirPath(filepath.Dir(srcMarkerPath))
		srcCommitDir = m.commitDir(newSourceCommitID.String())
		if cloneErr := store.CloneCheckpointDir(ctx, sourceSnapDir, srcCommitDir); cloneErr != nil {
			m.lineageCommitMu.RUnlock()
			cleanupUncommittedDirs()
			return nil, fmt.Errorf("物化源盘提交切面目录: %w", cloneErr)
		}
		if srcHasOrphans {
			if _, sanErr := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{
				Dir:       srcCommitDir,
				CommitID:  newSourceCommitID.String(),
				Cache:     cache,
				Logger:    m.baseLogger,
				Metrics:   m.storeMetrics,
				OpenActor: m.openDiskActor,
			}); sanErr != nil {
				m.lineageCommitMu.RUnlock()
				cleanupUncommittedDirs()
				return nil, fmt.Errorf("净化源盘提交切面孤儿节点: %w", sanErr)
			}
		}
	}
	m.lineageCommitMu.RUnlock()

	if m.beforePhase2CutoverHook != nil {
		m.beforePhase2CutoverHook()
	}

	// Phase 2：摘除并关闭旧目标 Actor，执行 (WriterEpoch, NextWriteSeq) CAS 校验与原子切换。
	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		cleanupUncommittedDirs()
		return nil, ErrClosed
	}
	oldTargetActor := m.actors[targetKey]
	if oldTargetActor != nil {
		delete(m.actors, targetKey)
	}
	m.poolMu.Unlock()

	if oldTargetActor != nil {
		if closeErr := oldTargetActor.Close(); closeErr != nil {
			cleanupUncommittedDirs()
			return nil, fmt.Errorf("关闭旧目标盘实例: %w", closeErr)
		}
	}

	tgtDir := m.diskDir(targetKey)
	epoch1, nextWriteSeq1, _, err := store.InspectBranchStat(tgtDir)
	if err != nil {
		cleanupUncommittedDirs()
		return nil, mapInspectErr(err)
	}
	if epoch1 != epoch0 || nextWriteSeq1 != nextWriteSeq0 {
		cleanupUncommittedDirs()
		return nil, fmt.Errorf(
			"%w: target disk watermark changed during merge ((%d,%d) != (%d,%d))",
			ErrConcurrentMerge, epoch1, nextWriteSeq1, epoch0, nextWriteSeq0,
		)
	}

	updatedTargetRow, err := m.store.MergeDisk(ctx, &db.MergeDiskInput{
		LineageID:                  sourceRow.LineageID,
		SourceDiskID:               sourceID,
		TargetDiskID:               targetID,
		ExpectedSourceBaseCommitID: sourceRow.BaseCommitID,
		ExpectedTargetBaseCommitID: targetRow.BaseCommitID,
		NewSourceCommitID:          newSourceCommitID,
		NewMergeCommitID:           newMergeCommitID,
		SkipSourceCommit:           skipSourceCommit,
	})
	if err != nil {
		cleanupUncommittedDirs()
		return nil, mapDBErr(err)
	}
	if srcMarkerPath != "" {
		_ = os.Remove(srcMarkerPath)
	}

	// 数据库已提交 C_merge，后续目录切换若意外中断将由 openDiskActor 崩溃前滚接管。
	oldDir := m.oldMergeDir(targetKey, newMergeCommitID)
	_ = m.removeAll(oldDir)
	if err = os.Rename(tgtDir, oldDir); err != nil {
		return nil, fmt.Errorf("归档旧目标盘目录: %w", err)
	}
	if err = os.Rename(stagingDir, tgtDir); err != nil {
		return nil, fmt.Errorf("原子替换目标盘目录: %w", err)
	}
	disksRoot := filepath.Join(m.storageRoot, "disks")
	if syncErr := syncDirPath(disksRoot); syncErr != nil {
		return nil, fmt.Errorf("持久化目录切换元数据: %w", syncErr)
	}
	if rmErr := m.removeAll(oldDir); rmErr != nil {
		m.collector.incCleanupError()
		m.logger.WarnContext(ctx, "remove archived old target directory failed",
			"disk_id", targetKey,
			"dir", oldDir,
			"err", rmErr,
		)
	}

	inoAlloc, packAlloc = m.newSegmentAllocators(updatedTargetRow.LineageID)
	newTargetActor, err := m.openDiskActor(ctx, store.Options{
		Dir:           tgtDir,
		DiskID:        targetKey,
		MaxBytes:      mergeRes.TargetMaxBytes,
		AtimeMode:     m.atimeMode,
		MaxPackSize:   m.maxPackSize,
		Cache:         cache,
		Logger:        m.baseLogger,
		Metrics:       m.storeMetrics,
		InoAllocator:  inoAlloc,
		PackAllocator: packAlloc,
	})
	if err != nil {
		return nil, fmt.Errorf("重新打开合并后目标盘实例: %w", err)
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		_ = newTargetActor.Close()
		return nil, ErrClosed
	}
	m.actors[targetKey] = newTargetActor
	m.poolMu.Unlock()

	stat, statErr := newTargetActor.StatFS(ctx)
	if statErr != nil {
		return nil, fmt.Errorf("查询合并后目标盘容量统计: %w", statErr)
	}
	return &MergeDiskResult{
		Disk:               assembleDisk(&updatedTargetRow, &stat),
		LCACommitID:        lcaCommitID.String(),
		MergeCommitID:      newMergeCommitID.String(),
		EstimatedUsedBytes: mergeRes.UsedBytes,
		TargetMaxBytes:     mergeRes.TargetMaxBytes,
	}, nil
}

// reconcilePendingMergeLocked 在打开读写 DiskActor 前扫描 disks/ 中残余的 .merge.<C_merge>、.old.<C_merge> 与 .srccommit.* 产物，
// 以 PostgreSQL 记录的 target.BaseCommitID 及已物化切换事实为依据幂等执行前滚或回滚清理。
func (m *manager) reconcilePendingMergeLocked(ctx context.Context, diskID string) error {
	disksRoot := filepath.Join(m.storageRoot, "disks")
	entries, err := os.ReadDir(disksRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	mergePrefix := diskID + ".merge."
	oldPrefix := diskID + ".old."
	srcPrefix := diskID + ".srccommit."
	pendingCommitIDs := make(map[uuid.UUID]struct{})
	linkedSrcCommits := make(map[uuid.UUID][]uuid.UUID)
	markerPaths := make(map[uuid.UUID][]string)
	var rawLeftovers []string

	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasPrefix(name, mergePrefix):
			cid, parseErr := uuid.Parse(strings.TrimPrefix(name, mergePrefix))
			if parseErr != nil || cid == uuid.Nil {
				rawLeftovers = append(rawLeftovers, filepath.Join(disksRoot, name))
				continue
			}
			pendingCommitIDs[cid] = struct{}{}
		case strings.HasPrefix(name, oldPrefix):
			cid, parseErr := uuid.Parse(strings.TrimPrefix(name, oldPrefix))
			if parseErr != nil || cid == uuid.Nil {
				rawLeftovers = append(rawLeftovers, filepath.Join(disksRoot, name))
				continue
			}
			pendingCommitIDs[cid] = struct{}{}
		case strings.HasPrefix(name, srcPrefix):
			rest := strings.TrimPrefix(name, srcPrefix)
			parts := strings.Split(rest, ".")
			if len(parts) != 2 {
				rawLeftovers = append(rawLeftovers, filepath.Join(disksRoot, name))
				continue
			}
			mergeCID, errMerge := uuid.Parse(parts[0])
			srcCID, errSrc := uuid.Parse(parts[1])
			if errMerge != nil || errSrc != nil || mergeCID == uuid.Nil || srcCID == uuid.Nil {
				rawLeftovers = append(rawLeftovers, filepath.Join(disksRoot, name))
				continue
			}
			pendingCommitIDs[mergeCID] = struct{}{}
			linkedSrcCommits[mergeCID] = append(linkedSrcCommits[mergeCID], srcCID)
			markerPaths[mergeCID] = append(markerPaths[mergeCID], filepath.Join(disksRoot, name))
		}
	}

	for _, dir := range rawLeftovers {
		_ = m.removeAll(dir)
	}
	if len(pendingCommitIDs) == 0 {
		return nil
	}

	parsedDiskID, err := parseDiskID(diskID)
	if err != nil {
		return err
	}
	row, err := m.store.GetDisk(ctx, parsedDiskID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			for cid := range pendingCommitIDs {
				_ = m.removeAll(m.stagingMergeDir(diskID, cid))
				_ = m.removeAll(m.oldMergeDir(diskID, cid))
				for _, mp := range markerPaths[cid] {
					_ = os.Remove(mp)
				}
			}
			return nil
		}
		return mapDBErr(err)
	}

	knownCommits := make(map[uuid.UUID]struct{})
	commits, listErr := m.store.ListLineageCommits(ctx, row.LineageID)
	if listErr == nil {
		for _, c := range commits {
			knownCommits[c.CommitID] = struct{}{}
		}
	}

	// 确定性排序：若 row.BaseCommitID 存在于待处理集合，优先执行其切换，再升序清理其余历史残留。
	orderedCIDs := make([]uuid.UUID, 0, len(pendingCommitIDs))
	if _, hasBase := pendingCommitIDs[row.BaseCommitID]; hasBase && row.BaseCommitID != uuid.Nil {
		orderedCIDs = append(orderedCIDs, row.BaseCommitID)
	}
	var restCIDs []uuid.UUID
	for cid := range pendingCommitIDs {
		if cid != row.BaseCommitID {
			restCIDs = append(restCIDs, cid)
		}
	}
	slices.SortFunc(restCIDs, func(a, b uuid.UUID) int {
		return strings.Compare(a.String(), b.String())
	})
	orderedCIDs = append(orderedCIDs, restCIDs...)

	tgtDir := m.diskDir(diskID)
	for _, cid := range orderedCIDs {
		stagingDir := m.stagingMergeDir(diskID, cid)
		oldDir := m.oldMergeDir(diskID, cid)

		_, tgtErr := os.Stat(tgtDir)
		tgtExists := tgtErr == nil
		tgtMissing := errors.Is(tgtErr, os.ErrNotExist)
		_, stgErr := os.Stat(stagingDir)
		stagingExists := stgErr == nil
		_, oldErr := os.Stat(oldDir)
		oldExists := oldErr == nil

		// oldDir 仅在 PG 提交之后才会被创建；若目标目录缺失且 staging 与 old 同时存在，说明已越过 PG 提交点，必须前滚。
		shouldRollForward := (row.BaseCommitID == cid) || (tgtMissing && stagingExists && oldExists)
		if shouldRollForward {
			if stagingExists {
				if tgtExists {
					_ = m.removeAll(oldDir)
					if renErr := os.Rename(tgtDir, oldDir); renErr != nil {
						return fmt.Errorf("崩溃前滚归档旧盘目录: %w", renErr)
					}
				}
				if renErr := os.Rename(stagingDir, tgtDir); renErr != nil {
					return fmt.Errorf("崩溃前滚激活新合并目录: %w", renErr)
				}
			}
			_ = m.removeAll(oldDir)
		} else {
			if tgtMissing && oldExists {
				if renErr := os.Rename(oldDir, tgtDir); renErr != nil {
					return fmt.Errorf("崩溃回滚恢复原盘目录: %w", renErr)
				}
			}
			_ = m.removeAll(stagingDir)
			_ = m.removeAll(oldDir)
			if listErr == nil {
				m.lineageCommitMu.Lock()
				if _, committed := knownCommits[cid]; !committed {
					_ = m.removeAll(m.commitDir(cid.String()))
				}
				for _, srcCID := range linkedSrcCommits[cid] {
					if _, committed := knownCommits[srcCID]; !committed {
						_ = m.removeAll(m.commitDir(srcCID.String()))
					}
				}
				m.lineageCommitMu.Unlock()
			}
		}

		for _, mp := range markerPaths[cid] {
			_ = os.Remove(mp)
		}
	}
	return syncDirPath(disksRoot)
}

func (m *manager) reconcileOtherPendingMergesBeforeDelete(ctx context.Context, deletingDiskID string) error {
	disksRoot := filepath.Join(m.storageRoot, "disks")
	entries, err := os.ReadDir(disksRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	otherDiskSet := make(map[string]struct{})
	for _, e := range entries {
		name := e.Name()
		for _, marker := range []string{".merge.", ".old.", ".srccommit."} {
			if prefix, _, found := strings.Cut(name, marker); found && prefix != "" && prefix != deletingDiskID {
				if parsed, parseErr := uuid.Parse(prefix); parseErr == nil && parsed != uuid.Nil {
					otherDiskSet[parsed.String()] = struct{}{}
				}
				break
			}
		}
	}
	if len(otherDiskSet) == 0 {
		return nil
	}

	otherDisks := make([]string, 0, len(otherDiskSet))
	for id := range otherDiskSet {
		otherDisks = append(otherDisks, id)
	}
	slices.Sort(otherDisks)

	for _, otherID := range otherDisks {
		unlock := m.lockDisk(ctx, otherID)
		recErr := m.reconcilePendingMergeLocked(ctx, otherID)
		unlock()
		if recErr != nil {
			return recErr
		}
	}
	return nil
}

func (m *manager) cleanupPendingMergeArtifactsForDisk(ctx context.Context, diskID string) {
	disksRoot := filepath.Join(m.storageRoot, "disks")
	entries, err := os.ReadDir(disksRoot)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, diskID+".merge.") ||
			strings.HasPrefix(name, diskID+".old.") ||
			strings.HasPrefix(name, diskID+".srccommit.") {
			p := filepath.Join(disksRoot, name)
			if rmErr := m.removeAll(p); rmErr != nil {
				m.collector.incCleanupError()
				m.logger.WarnContext(ctx, "delete disk pending merge artifact cleanup failed",
					"disk_id", diskID,
					"path", p,
					"err", rmErr,
				)
			}
		}
	}
}

func mapInspectErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	return err
}
