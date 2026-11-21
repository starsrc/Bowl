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

type DiffDiskParams struct {
	SourceDiskID  string
	TargetDiskID  string
	PathPrefix    []byte
	ConflictsOnly bool
	PageSize      uint32
	PageToken     string
}

type DiffDiskResult struct {
	LCASnapshotID string
	Summary       store.DiffSummary
	Entries       []store.DiffEntry
	NextPageToken string
}

type ReadSnapshotNodeParams struct {
	DiskID     string
	SnapshotID string
	Ino        uint64
	Offset     uint64
	Size       uint32
}

type MergeDiskParams struct {
	SourceDiskID   string
	TargetDiskID   string
	Strategy       store.MergeStrategy
	PathStrategies map[string]store.MergeStrategy
	DryRun         bool
	Force          bool
	MaxBytes       *uint64
}

type MergeDiskResult struct {
	Disk               *Disk
	LCASnapshotID      string
	MergeSnapshotID    string
	EstimatedUsedBytes uint64
	TargetMaxBytes     uint64
}

type MergeConflictError struct {
	LCASnapshotID string
	Report        store.MergeConflictReport
	Err           error
}

func (e *MergeConflictError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return fmt.Sprintf("merge conflicts detected (%d total): %s", e.Report.TotalConflicts, e.Err.Error())
	}
	return fmt.Sprintf("merge conflicts detected (%d total)", e.Report.TotalConflicts)
}

func (e *MergeConflictError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (m *manager) stagingMergeDir(diskID string, snapshotID uuid.UUID) string {
	return filepath.Join(m.storageRoot, "disks", diskID+".merge."+snapshotID.String())
}

func (m *manager) oldMergeDir(diskID string, snapshotID uuid.UUID) string {
	return filepath.Join(m.storageRoot, "disks", diskID+".old."+snapshotID.String())
}

func (m *manager) srcSnapshotMarkerPath(diskID string, mergeSnapshotID, srcSnapshotID uuid.UUID) string {
	return filepath.Join(m.storageRoot, "disks", fmt.Sprintf("%s.srcsnap.%s.%s", diskID, mergeSnapshotID.String(), srcSnapshotID.String()))
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

func parseSnapshotID(snapshotID string) (uuid.UUID, error) {
	id, err := uuid.Parse(snapshotID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: invalid snapshot id %q", ErrInvalidArgument, snapshotID)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: snapshot id cannot be nil uuid", ErrInvalidArgument)
	}
	return id, nil
}

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

func (m *manager) snapshotDiskForInspect(
	ctx context.Context,
	row *db.DiskRecord,
	prefix string,
) (snapDir string, stats store.CheckpointStats, cleanup func(), err error) {
	entry, err := m.getOrOpenActorWithRecordLocked(ctx, row)
	if err != nil {
		return "", store.CheckpointStats{}, nil, err
	}
	diskID := row.DiskID.String()
	tmpRoot := filepath.Join(m.storageRoot, ".tmp")
	if mkErr := os.MkdirAll(tmpRoot, 0o755); mkErr != nil {
		return "", store.CheckpointStats{}, nil, mkErr
	}
	snapDir = m.tmpSnapshotPath(prefix)
	stats, err = entry.actor.Checkpoint(ctx, snapDir)
	if err != nil {
		_ = m.removeAll(snapDir)
		return "", store.CheckpointStats{}, nil, err
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
	return snapDir, stats, cleanup, nil
}

func findLCA(snapshots []db.SnapshotRecord, snapA, snapB uuid.UUID) (uuid.UUID, error) {
	if snapA == uuid.Nil || snapB == uuid.Nil {
		return uuid.Nil, ErrNoBaseSnapshot
	}
	byID := make(map[uuid.UUID]db.SnapshotRecord, len(snapshots))
	for i := range snapshots {
		byID[snapshots[i].SnapshotID] = snapshots[i]
	}
	if _, ok := byID[snapA]; !ok {
		return uuid.Nil, fmt.Errorf("%w: snapshot %s not found in lineage", ErrNotFound, snapA)
	}
	if _, ok := byID[snapB]; !ok {
		return uuid.Nil, fmt.Errorf("%w: snapshot %s not found in lineage", ErrNotFound, snapB)
	}
	if snapA == snapB {
		return snapA, nil
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
			if rec.ParentSnapshotID != uuid.Nil {
				queue = append(queue, rec.ParentSnapshotID)
			}
			if rec.MergeParentSnapshotID != uuid.Nil {
				queue = append(queue, rec.MergeParentSnapshotID)
			}
		}
		return visited
	}

	ancA := collectAncestors(snapA)
	ancB := collectAncestors(snapB)

	common := make(map[uuid.UUID]struct{})
	for id := range ancA {
		if _, inB := ancB[id]; inB {
			common[id] = struct{}{}
		}
	}
	if len(common) == 0 {
		return uuid.Nil, fmt.Errorf("%w: snapshots %s and %s share no common ancestor", ErrNoBaseSnapshot, snapA, snapB)
	}

	dominated := make(map[uuid.UUID]bool, len(common))
	for id := range common {
		rec := byID[id]
		if rec.ParentSnapshotID != uuid.Nil {
			if _, ok := common[rec.ParentSnapshotID]; ok {
				dominated[rec.ParentSnapshotID] = true
			}
		}
		if rec.MergeParentSnapshotID != uuid.Nil {
			if _, ok := common[rec.MergeParentSnapshotID]; ok {
				dominated[rec.MergeParentSnapshotID] = true
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
		return uuid.Nil, fmt.Errorf("%w: malformed snapshot DAG cycle detected", ErrNoBaseSnapshot)
	}
	if len(mca) > 1 {
		return uuid.Nil, fmt.Errorf("%w: %d maximal common ancestors found between %s and %s", ErrAmbiguousLCA, len(mca), snapA, snapB)
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

	var lcaSnapshotID uuid.UUID
	var targetRow db.DiskRecord
	if hasTarget {
		targetRow, err = m.store.GetDisk(ctx, targetID)
		if err != nil {
			return nil, mapDBErr(err)
		}
		if sourceRow.LineageID != targetRow.LineageID {
			return nil, fmt.Errorf("%w: source lineage %s != target lineage %s", ErrCrossLineage, sourceRow.LineageID, targetRow.LineageID)
		}
		if sourceRow.BaseSnapshotID == uuid.Nil || targetRow.BaseSnapshotID == uuid.Nil {
			return nil, ErrNoBaseSnapshot
		}
		snapshots, listErr := m.store.ListLineageSnapshots(ctx, sourceRow.LineageID)
		if listErr != nil {
			return nil, mapDBErr(listErr)
		}
		lcaSnapshotID, err = findLCA(snapshots, sourceRow.BaseSnapshotID, targetRow.BaseSnapshotID)
		if err != nil {
			return nil, err
		}
	} else {
		if sourceRow.BaseSnapshotID == uuid.Nil {
			return nil, ErrNoBaseSnapshot
		}
		lcaSnapshotID = sourceRow.BaseSnapshotID
	}

	sourceSnapDir, _, cleanupSrc, err := m.snapshotDiskForInspect(ctx, &sourceRow, "diff-src")
	if err != nil {
		return nil, err
	}
	defer cleanupSrc()

	var targetSnapDir string
	if hasTarget {
		var cleanupTgt func()
		targetSnapDir, _, cleanupTgt, err = m.snapshotDiskForInspect(ctx, &targetRow, "diff-tgt")
		if err != nil {
			return nil, err
		}
		defer cleanupTgt()
	}

	m.lineageSnapshotMu.RLock()
	defer m.lineageSnapshotMu.RUnlock()

	diffRes, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:      m.snapshotDir(lcaSnapshotID.String()),
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
		LCASnapshotID: lcaSnapshotID.String(),
		Summary:       diffRes.Summary,
		Entries:       diffRes.Entries,
		NextPageToken: diffRes.NextPageToken,
	}, nil
}

func (m *manager) ReadSnapshotNode(ctx context.Context, params *ReadSnapshotNodeParams) (*store.ReadNodeResult, error) {
	if params == nil {
		return nil, fmt.Errorf("%w: params cannot be nil", ErrInvalidArgument)
	}
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	snapshotID, err := parseSnapshotID(params.SnapshotID)
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
	snapshots, err := m.store.ListLineageSnapshots(ctx, diskRow.LineageID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	belongsToLineage := false
	for i := range snapshots {
		if snapshots[i].SnapshotID == snapshotID {
			belongsToLineage = true
			break
		}
	}

	m.lineageSnapshotMu.RLock()
	defer m.lineageSnapshotMu.RUnlock()

	sDir := m.snapshotDir(snapshotID.String())
	if !belongsToLineage {
		if _, statErr := os.Stat(sDir); statErr == nil {
			return nil, fmt.Errorf("%w: snapshot %s does not belong to disk lineage %s", ErrCrossLineage, snapshotID, diskRow.LineageID)
		}
		return nil, fmt.Errorf("%w: snapshot %s not found in lineage %s", ErrNotFound, snapshotID, diskRow.LineageID)
	}

	res, err := store.ReadSnapshotNodeSlice(sDir, params.Ino, params.Offset, params.Size)
	if err != nil {
		return nil, mapInspectErr(err)
	}
	return res, nil
}

type mergeCutoverParams struct {
	sourceRow          *db.DiskRecord
	targetRow          *db.DiskRecord
	lcaSnapshotID      uuid.UUID
	newSourceSnapID    uuid.UUID
	newMergeSnapID     uuid.UUID
	skipSourceSnapshot bool
	fastForward        bool
	epoch0             uint64
	nextWriteSeq0      uint64
	targetMaxBytes     uint64
	usedBytes          uint64
	inodes             uint64
	stagingDir         string
	srcMarkerPath      string
	cache              *pebble.Cache
	cleanupUncommitted func()
}

func (m *manager) mergeAlreadyUpToDate(
	ctx context.Context,
	targetRow *db.DiskRecord,
	lcaSnapshotID uuid.UUID,
	params *MergeDiskParams,
) (*MergeDiskResult, error) {
	targetEntry, err := m.getOrOpenActorWithRecordLocked(ctx, targetRow)
	if err != nil {
		return nil, err
	}
	liveStat, err := targetEntry.actor.StatFS(ctx)
	if err != nil {
		return nil, err
	}

	effectiveMaxBytes := liveStat.TotalBytes
	if params.MaxBytes != nil {
		if *params.MaxBytes < liveStat.UsedBytes {
			return nil, ErrInvalidShrink
		}
		effectiveMaxBytes = *params.MaxBytes
	}

	if params.DryRun {
		return &MergeDiskResult{
			LCASnapshotID:      lcaSnapshotID.String(),
			MergeSnapshotID:    targetRow.BaseSnapshotID.String(),
			EstimatedUsedBytes: liveStat.UsedBytes,
			TargetMaxBytes:     effectiveMaxBytes,
		}, nil
	}

	updatedTargetRow := *targetRow
	if params.MaxBytes != nil {
		updatedRow, updateErr := m.store.UpdateDisk(ctx, db.UpdateDiskInput{
			DiskID:   targetRow.DiskID,
			MaxBytes: params.MaxBytes,
		})
		if updateErr != nil {
			return nil, mapDBErr(updateErr)
		}
		if maxErr := targetEntry.actor.UpdateMaxBytes(ctx, *params.MaxBytes); maxErr != nil {
			if _, rbErr := m.store.UpdateDisk(context.WithoutCancel(ctx), db.UpdateDiskInput{
				DiskID:   targetRow.DiskID,
				MaxBytes: &targetRow.MaxBytes,
			}); rbErr != nil {
				m.logger.WarnContext(ctx, "merge no-op quota rollback pg failed",
					"disk_id", targetRow.DiskID.String(),
					"err", rbErr,
				)
			}
			return nil, maxErr
		}
		liveStat, err = targetEntry.actor.StatFS(ctx)
		if err != nil {
			return nil, err
		}
		updatedTargetRow = updatedRow
	}

	return &MergeDiskResult{
		Disk:               assembleDisk(&updatedTargetRow, &liveStat),
		LCASnapshotID:      lcaSnapshotID.String(),
		MergeSnapshotID:    targetRow.BaseSnapshotID.String(),
		EstimatedUsedBytes: liveStat.UsedBytes,
		TargetMaxBytes:     effectiveMaxBytes,
	}, nil
}

func (m *manager) commitAndSwapMergedTarget(ctx context.Context, cp *mergeCutoverParams) (*MergeDiskResult, error) {
	targetKey := cp.targetRow.DiskID.String()

	if m.beforePhase2CutoverHook != nil {
		m.beforePhase2CutoverHook()
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		cp.cleanupUncommitted()
		return nil, ErrClosed
	}
	oldTargetEntry := m.actors[targetKey]
	if oldTargetEntry != nil {
		delete(m.actors, targetKey)
	}
	m.poolMu.Unlock()

	if oldTargetEntry != nil {
		if closeErr := oldTargetEntry.actor.Close(); closeErr != nil {
			cp.cleanupUncommitted()
			return nil, fmt.Errorf("关闭旧目标盘实例: %w", closeErr)
		}
	}

	tgtDir := m.diskDir(targetKey)
	epoch1, nextWriteSeq1, _, err := store.InspectBranchStat(tgtDir)
	if err != nil {
		cp.cleanupUncommitted()
		return nil, mapInspectErr(err)
	}
	if epoch1 != cp.epoch0 || nextWriteSeq1 != cp.nextWriteSeq0 {
		cp.cleanupUncommitted()
		return nil, fmt.Errorf(
			"%w: target disk watermark changed during merge ((%d,%d) != (%d,%d))",
			ErrConcurrentMerge, epoch1, nextWriteSeq1, cp.epoch0, cp.nextWriteSeq0,
		)
	}

	updatedTargetRow, err := m.store.MergeDisk(ctx, &db.MergeDiskInput{
		LineageID:                    cp.sourceRow.LineageID,
		SourceDiskID:                 cp.sourceRow.DiskID,
		TargetDiskID:                 cp.targetRow.DiskID,
		ExpectedSourceBaseSnapshotID: cp.sourceRow.BaseSnapshotID,
		ExpectedTargetBaseSnapshotID: cp.targetRow.BaseSnapshotID,
		NewSourceSnapshotID:          cp.newSourceSnapID,
		NewMergeSnapshotID:           cp.newMergeSnapID,
		SkipSourceSnapshot:           cp.skipSourceSnapshot,
		FastForward:                  cp.fastForward,
		TargetMaxBytes:               cp.targetMaxBytes,
		TargetUsedBytes:              cp.usedBytes,
		TargetUsedInodes:             cp.inodes,
	})
	if err != nil {
		cp.cleanupUncommitted()
		return nil, mapDBErr(err)
	}
	if cp.srcMarkerPath != "" {
		_ = os.Remove(cp.srcMarkerPath)
	}

	oldDir := m.oldMergeDir(targetKey, cp.newMergeSnapID)
	_ = m.removeAll(oldDir)
	if err = os.Rename(tgtDir, oldDir); err != nil {
		return nil, fmt.Errorf("归档旧目标盘目录: %w", err)
	}
	if err = os.Rename(cp.stagingDir, tgtDir); err != nil {
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

	inoAlloc, packAlloc := m.newSegmentAllocators(updatedTargetRow.LineageID)
	newTargetActor, err := m.openDiskActor(ctx, store.Options{
		Dir:           tgtDir,
		DiskID:        targetKey,
		MaxBytes:      cp.targetMaxBytes,
		AtimeMode:     m.atimeMode,
		MaxPackSize:   m.maxPackSize,
		Cache:         cp.cache,
		Logger:        m.baseLogger,
		Metrics:       m.storeMetrics,
		InoAllocator:  inoAlloc,
		PackAllocator: packAlloc,
	})
	if err != nil {
		return nil, fmt.Errorf("重新打开合并后目标盘实例: %w", err)
	}

	newTargetEntry := newActorEntry(
		updatedTargetRow.DiskID,
		newTargetActor,
		m.currentTime().UnixNano(),
		updatedTargetRow.UsedBytes,
		updatedTargetRow.UsedInodes,
	)

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		_ = newTargetActor.Close()
		return nil, ErrClosed
	}
	m.actors[targetKey] = newTargetEntry
	m.poolMu.Unlock()

	if _, clearErr := m.clearDiskMounts(ctx, cp.targetRow.DiskID); clearErr != nil {
		m.logger.WarnContext(ctx, "merge disk clear mount leases failed",
			"disk_id", targetKey,
			"err", clearErr,
		)
	}
	if flushErr := m.flushEntryUsage(ctx, newTargetEntry); flushErr != nil {
		m.logger.WarnContext(ctx, "merge disk usage flush failed",
			"disk_id", targetKey,
			"err", flushErr,
		)
	}

	stat, statErr := newTargetActor.StatFS(ctx)
	if statErr != nil {
		return nil, fmt.Errorf("查询合并后目标盘容量统计: %w", statErr)
	}
	return &MergeDiskResult{
		Disk:               assembleDisk(&updatedTargetRow, &stat),
		LCASnapshotID:      cp.lcaSnapshotID.String(),
		MergeSnapshotID:    cp.newMergeSnapID.String(),
		EstimatedUsedBytes: cp.usedBytes,
		TargetMaxBytes:     cp.targetMaxBytes,
	}, nil
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
	if sourceRow.BaseSnapshotID == uuid.Nil || targetRow.BaseSnapshotID == uuid.Nil {
		return nil, ErrNoBaseSnapshot
	}

	snapshots, err := m.store.ListLineageSnapshots(ctx, sourceRow.LineageID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	lcaSnapshotID, err := findLCA(snapshots, sourceRow.BaseSnapshotID, targetRow.BaseSnapshotID)
	if err != nil {
		return nil, err
	}

	if !params.DryRun && !params.Force {
		if gateErr := m.checkMergeMountGate(ctx, sourceID, targetID); gateErr != nil {
			return nil, gateErr
		}
	}

	sourceSnapDir, srcStats, cleanupSrc, err := m.snapshotDiskForInspect(ctx, &sourceRow, "merge-src")
	if err != nil {
		return nil, err
	}
	defer cleanupSrc()

	targetSnapDir, _, cleanupTgt, err := m.snapshotDiskForInspect(ctx, &targetRow, "merge-tgt")
	if err != nil {
		return nil, err
	}
	defer cleanupTgt()

	tgtBranchStat0, _, err := store.InspectBranchStatFull(targetSnapDir)
	if err != nil {
		return nil, mapInspectErr(err)
	}
	epoch0 := tgtBranchStat0.WriterEpoch
	nextWriteSeq0 := tgtBranchStat0.NextWriteSeq

	m.lineageSnapshotMu.RLock()
	lcaDir := m.snapshotDir(lcaSnapshotID.String())
	srcBaseDir := m.snapshotDir(sourceRow.BaseSnapshotID.String())
	srcDiffRes, srcDiffErr := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   srcBaseDir,
		SourceRoot: sourceSnapDir,
		PageSize:   1,
		Cache:      cache,
	})
	if srcDiffErr != nil {
		m.lineageSnapshotMu.RUnlock()
		return nil, mapInspectErr(srcDiffErr)
	}
	skipSourceSnapshot := srcDiffRes.Summary.Added == 0 && srcDiffRes.Summary.Modified == 0 && srcDiffRes.Summary.Deleted == 0

	if sourceRow.BaseSnapshotID == lcaSnapshotID && skipSourceSnapshot {
		m.lineageSnapshotMu.RUnlock()
		return m.mergeAlreadyUpToDate(ctx, &targetRow, lcaSnapshotID, params)
	}

	isFastForward := false
	if targetRow.BaseSnapshotID == lcaSnapshotID {
		tgtDiffRes, tgtDiffErr := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:   lcaDir,
			SourceRoot: targetSnapDir,
			PageSize:   1,
			Cache:      cache,
		})
		if tgtDiffErr != nil {
			m.lineageSnapshotMu.RUnlock()
			return nil, mapInspectErr(tgtDiffErr)
		}
		if tgtDiffRes.Summary.Added == 0 && tgtDiffRes.Summary.Modified == 0 && tgtDiffRes.Summary.Deleted == 0 {
			isFastForward = true
		}
	}

	if isFastForward {
		return m.mergeFastForwardLocked(
			ctx,
			&sourceRow,
			&targetRow,
			lcaSnapshotID,
			sourceSnapDir,
			srcStats,
			skipSourceSnapshot,
			tgtBranchStat0,
			overrideMaxBytes,
			params.DryRun,
			cache,
		)
	}

	return m.mergeThreeWayLocked(
		ctx,
		&sourceRow,
		&targetRow,
		lcaSnapshotID,
		sourceSnapDir,
		targetSnapDir,
		srcStats.HasOrphans,
		skipSourceSnapshot,
		epoch0,
		nextWriteSeq0,
		strategy,
		params,
		overrideMaxBytes,
		cache,
	)
}

func (m *manager) mergeFastForwardLocked(
	ctx context.Context,
	sourceRow *db.DiskRecord,
	targetRow *db.DiskRecord,
	lcaSnapshotID uuid.UUID,
	sourceSnapDir string,
	srcStats store.CheckpointStats,
	skipSourceSnapshot bool,
	tgtBranchStat0 store.BranchStat,
	overrideMaxBytes uint64,
	dryRun bool,
	cache *pebble.Cache,
) (*MergeDiskResult, error) {
	srcLiveStat, _, err := store.InspectBranchStatFull(sourceSnapDir)
	if err != nil {
		m.lineageSnapshotMu.RUnlock()
		return nil, mapInspectErr(err)
	}
	srcNextWriteSeq := srcLiveStat.NextWriteSeq

	var srcUsedBytes, srcInodes uint64
	if skipSourceSnapshot {
		sffStat, _, statErr := store.InspectBranchStatFull(m.snapshotDir(sourceRow.BaseSnapshotID.String()))
		if statErr != nil {
			m.lineageSnapshotMu.RUnlock()
			return nil, mapInspectErr(statErr)
		}
		srcUsedBytes = sffStat.LogicalBytes
		srcInodes = sffStat.Inodes
		srcNextWriteSeq = max(srcNextWriteSeq, sffStat.NextWriteSeq)
	} else {
		if srcStats.HasOrphans {
			sanStat, sanErr := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{
				Dir:        sourceSnapDir,
				SnapshotID: filepath.Base(sourceSnapDir),
				Cache:      cache,
				Logger:     m.baseLogger,
				Metrics:    m.storeMetrics,
				OpenActor:  m.openDiskActor,
			})
			if sanErr != nil {
				m.lineageSnapshotMu.RUnlock()
				return nil, fmt.Errorf("净化快进源盘孤儿节点: %w", sanErr)
			}
			srcUsedBytes = sanStat.UsedBytes
			srcInodes = sanStat.Files
			sanBranchStat, _, bErr := store.InspectBranchStatFull(sourceSnapDir)
			if bErr != nil {
				m.lineageSnapshotMu.RUnlock()
				return nil, mapInspectErr(bErr)
			}
			srcNextWriteSeq = max(srcNextWriteSeq, sanBranchStat.NextWriteSeq)
		} else {
			srcUsedBytes = srcLiveStat.LogicalBytes
			srcInodes = srcLiveStat.Inodes
		}
	}

	targetMaxBytes := tgtBranchStat0.MaxBytes
	if targetMaxBytes == 0 {
		targetMaxBytes = targetRow.MaxBytes
	}
	if overrideMaxBytes > 0 {
		targetMaxBytes = overrideMaxBytes
		if srcUsedBytes > targetMaxBytes {
			m.lineageSnapshotMu.RUnlock()
			return nil, ErrInvalidShrink
		}
	} else if targetMaxBytes > 0 && srcUsedBytes > targetMaxBytes {
		m.lineageSnapshotMu.RUnlock()
		return nil, ErrNoSpace
	}

	if dryRun {
		m.lineageSnapshotMu.RUnlock()
		return &MergeDiskResult{
			LCASnapshotID:      lcaSnapshotID.String(),
			EstimatedUsedBytes: srcUsedBytes,
			TargetMaxBytes:     targetMaxBytes,
		}, nil
	}

	targetKey := targetRow.DiskID.String()
	var (
		ffSnapID         uuid.UUID
		newSourceSnapID  uuid.UUID
		ffCreatedSnapDir string
		srcMarkerPath    string
		stagingDir       string
	)
	cleanupUncommitted := func() {
		if stagingDir != "" {
			_ = m.removeAll(stagingDir)
		}
		if ffCreatedSnapDir != "" {
			_ = m.removeAll(ffCreatedSnapDir)
		}
		if srcMarkerPath != "" {
			_ = os.Remove(srcMarkerPath)
		}
	}

	if skipSourceSnapshot {
		ffSnapID = sourceRow.BaseSnapshotID
	} else {
		ffSnapID = uuid.Must(uuid.NewV7())
		newSourceSnapID = ffSnapID
		srcMarkerPath = m.srcSnapshotMarkerPath(targetKey, ffSnapID, ffSnapID)
		if mkErr := os.WriteFile(srcMarkerPath, nil, 0o644); mkErr != nil {
			m.lineageSnapshotMu.RUnlock()
			cleanupUncommitted()
			return nil, fmt.Errorf("写入快进快照崩溃恢复标记: %w", mkErr)
		}
		_ = syncDirPath(filepath.Dir(srcMarkerPath))
		ffCreatedSnapDir = m.snapshotDir(ffSnapID.String())
		if cloneErr := store.CloneCheckpointDir(ctx, sourceSnapDir, ffCreatedSnapDir); cloneErr != nil {
			m.lineageSnapshotMu.RUnlock()
			cleanupUncommitted()
			return nil, fmt.Errorf("物化快进快照目录: %w", cloneErr)
		}
	}

	stagingDir = m.stagingMergeDir(targetKey, ffSnapID)
	if cloneErr := store.CloneCheckpointDir(ctx, m.snapshotDir(ffSnapID.String()), stagingDir); cloneErr != nil {
		m.lineageSnapshotMu.RUnlock()
		cleanupUncommitted()
		return nil, fmt.Errorf("零拷贝构造快进暂存目录: %w", cloneErr)
	}
	m.lineageSnapshotMu.RUnlock()

	nextWriteSeqFF := max(tgtBranchStat0.NextWriteSeq, srcNextWriteSeq) + 1
	if _, alignErr := store.AlignBranchStat(stagingDir, targetMaxBytes, tgtBranchStat0.WriterEpoch+1, nextWriteSeqFF, cache); alignErr != nil {
		cleanupUncommitted()
		return nil, mapInspectErr(alignErr)
	}

	return m.commitAndSwapMergedTarget(ctx, &mergeCutoverParams{
		sourceRow:          sourceRow,
		targetRow:          targetRow,
		lcaSnapshotID:      lcaSnapshotID,
		newSourceSnapID:    newSourceSnapID,
		newMergeSnapID:     ffSnapID,
		skipSourceSnapshot: skipSourceSnapshot,
		fastForward:        true,
		epoch0:             tgtBranchStat0.WriterEpoch,
		nextWriteSeq0:      tgtBranchStat0.NextWriteSeq,
		targetMaxBytes:     targetMaxBytes,
		usedBytes:          srcUsedBytes,
		inodes:             srcInodes,
		stagingDir:         stagingDir,
		srcMarkerPath:      srcMarkerPath,
		cache:              cache,
		cleanupUncommitted: cleanupUncommitted,
	})
}

func (m *manager) mergeThreeWayLocked(
	ctx context.Context,
	sourceRow *db.DiskRecord,
	targetRow *db.DiskRecord,
	lcaSnapshotID uuid.UUID,
	sourceSnapDir string,
	targetSnapDir string,
	srcHasOrphans bool,
	skipSourceSnapshot bool,
	epoch0 uint64,
	nextWriteSeq0 uint64,
	strategy store.MergeStrategy,
	params *MergeDiskParams,
	overrideMaxBytes uint64,
	cache *pebble.Cache,
) (*MergeDiskResult, error) {
	targetKey := targetRow.DiskID.String()
	newMergeSnapID := uuid.Must(uuid.NewV7())
	stagingDir := m.stagingMergeDir(targetKey, newMergeSnapID)
	var (
		newSourceSnapID uuid.UUID
		srcSnapDir      string
		mergeSnapDir    string
		srcMarkerPath   string
	)
	if !skipSourceSnapshot {
		newSourceSnapID = uuid.Must(uuid.NewV7())
	}

	cleanupUncommitted := func() {
		if stagingDir != "" {
			_ = m.removeAll(stagingDir)
		}
		if mergeSnapDir != "" {
			_ = m.removeAll(mergeSnapDir)
		}
		if srcSnapDir != "" {
			_ = m.removeAll(srcSnapDir)
		}
		if srcMarkerPath != "" {
			_ = os.Remove(srcMarkerPath)
		}
	}

	var (
		inoAlloc  store.InodeSegmentAllocator
		packAlloc store.PackSegmentAllocator
	)
	if !params.DryRun {
		inoAlloc, packAlloc = m.newSegmentAllocators(sourceRow.LineageID)
	}

	mergeRes, err := store.MergeTrees(ctx, &store.MergeOptions{
		BaseRoot:            m.snapshotDir(lcaSnapshotID.String()),
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
		m.lineageSnapshotMu.RUnlock()
		cleanupUncommitted()
		if conflictErr, ok := errors.AsType[*store.ErrMergeConflict](err); ok {
			return nil, &MergeConflictError{
				LCASnapshotID: lcaSnapshotID.String(),
				Report:        conflictErr.Report,
				Err:           conflictErr,
			}
		}
		return nil, mapInspectErr(err)
	}

	if params.DryRun {
		m.lineageSnapshotMu.RUnlock()
		cleanupUncommitted()
		return &MergeDiskResult{
			LCASnapshotID:      lcaSnapshotID.String(),
			EstimatedUsedBytes: mergeRes.UsedBytes,
			TargetMaxBytes:     mergeRes.TargetMaxBytes,
		}, nil
	}

	mergeSnapDir = m.snapshotDir(newMergeSnapID.String())
	if cloneErr := store.CloneCheckpointDir(ctx, stagingDir, mergeSnapDir); cloneErr != nil {
		m.lineageSnapshotMu.RUnlock()
		cleanupUncommitted()
		return nil, fmt.Errorf("物化合并快照目录: %w", cloneErr)
	}
	if !skipSourceSnapshot {
		srcMarkerPath = m.srcSnapshotMarkerPath(targetKey, newMergeSnapID, newSourceSnapID)
		if mkErr := os.WriteFile(srcMarkerPath, nil, 0o644); mkErr != nil {
			m.lineageSnapshotMu.RUnlock()
			cleanupUncommitted()
			return nil, fmt.Errorf("写入源快照崩溃恢复标记: %w", mkErr)
		}
		_ = syncDirPath(filepath.Dir(srcMarkerPath))
		srcSnapDir = m.snapshotDir(newSourceSnapID.String())
		if cloneErr := store.CloneCheckpointDir(ctx, sourceSnapDir, srcSnapDir); cloneErr != nil {
			m.lineageSnapshotMu.RUnlock()
			cleanupUncommitted()
			return nil, fmt.Errorf("物化源盘快照目录: %w", cloneErr)
		}
		if srcHasOrphans {
			if _, sanErr := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{
				Dir:        srcSnapDir,
				SnapshotID: newSourceSnapID.String(),
				Cache:      cache,
				Logger:     m.baseLogger,
				Metrics:    m.storeMetrics,
				OpenActor:  m.openDiskActor,
			}); sanErr != nil {
				m.lineageSnapshotMu.RUnlock()
				cleanupUncommitted()
				return nil, fmt.Errorf("净化源盘快照孤儿节点: %w", sanErr)
			}
		}
	}
	m.lineageSnapshotMu.RUnlock()

	return m.commitAndSwapMergedTarget(ctx, &mergeCutoverParams{
		sourceRow:          sourceRow,
		targetRow:          targetRow,
		lcaSnapshotID:      lcaSnapshotID,
		newSourceSnapID:    newSourceSnapID,
		newMergeSnapID:     newMergeSnapID,
		skipSourceSnapshot: skipSourceSnapshot,
		fastForward:        false,
		epoch0:             epoch0,
		nextWriteSeq0:      nextWriteSeq0,
		targetMaxBytes:     mergeRes.TargetMaxBytes,
		usedBytes:          mergeRes.UsedBytes,
		inodes:             mergeRes.Inodes,
		stagingDir:         stagingDir,
		srcMarkerPath:      srcMarkerPath,
		cache:              cache,
		cleanupUncommitted: cleanupUncommitted,
	})
}

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
	srcPrefix := diskID + ".srcsnap."
	pendingSnapIDs := make(map[uuid.UUID]struct{})
	linkedSrcSnaps := make(map[uuid.UUID][]uuid.UUID)
	markerPaths := make(map[uuid.UUID][]string)
	var rawLeftovers []string

	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasPrefix(name, mergePrefix):
			sid, parseErr := uuid.Parse(strings.TrimPrefix(name, mergePrefix))
			if parseErr != nil || sid == uuid.Nil {
				rawLeftovers = append(rawLeftovers, filepath.Join(disksRoot, name))
				continue
			}
			pendingSnapIDs[sid] = struct{}{}
		case strings.HasPrefix(name, oldPrefix):
			sid, parseErr := uuid.Parse(strings.TrimPrefix(name, oldPrefix))
			if parseErr != nil || sid == uuid.Nil {
				rawLeftovers = append(rawLeftovers, filepath.Join(disksRoot, name))
				continue
			}
			pendingSnapIDs[sid] = struct{}{}
		case strings.HasPrefix(name, srcPrefix):
			rest := strings.TrimPrefix(name, srcPrefix)
			parts := strings.Split(rest, ".")
			if len(parts) != 2 {
				rawLeftovers = append(rawLeftovers, filepath.Join(disksRoot, name))
				continue
			}
			mergeSID, errMerge := uuid.Parse(parts[0])
			srcSID, errSrc := uuid.Parse(parts[1])
			if errMerge != nil || errSrc != nil || mergeSID == uuid.Nil || srcSID == uuid.Nil {
				rawLeftovers = append(rawLeftovers, filepath.Join(disksRoot, name))
				continue
			}
			pendingSnapIDs[mergeSID] = struct{}{}
			linkedSrcSnaps[mergeSID] = append(linkedSrcSnaps[mergeSID], srcSID)
			markerPaths[mergeSID] = append(markerPaths[mergeSID], filepath.Join(disksRoot, name))
		}
	}

	for _, dir := range rawLeftovers {
		_ = m.removeAll(dir)
	}
	if len(pendingSnapIDs) == 0 {
		return nil
	}

	parsedDiskID, err := parseDiskID(diskID)
	if err != nil {
		return err
	}
	row, err := m.store.GetDisk(ctx, parsedDiskID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			for sid := range pendingSnapIDs {
				_ = m.removeAll(m.stagingMergeDir(diskID, sid))
				_ = m.removeAll(m.oldMergeDir(diskID, sid))
				for _, mp := range markerPaths[sid] {
					_ = os.Remove(mp)
				}
			}
			return nil
		}
		return mapDBErr(err)
	}

	knownSnapshots := make(map[uuid.UUID]struct{})
	snapshots, listErr := m.store.ListLineageSnapshots(ctx, row.LineageID)
	if listErr == nil {
		for i := range snapshots {
			knownSnapshots[snapshots[i].SnapshotID] = struct{}{}
		}
	}

	orderedSIDs := make([]uuid.UUID, 0, len(pendingSnapIDs))
	if _, hasBase := pendingSnapIDs[row.BaseSnapshotID]; hasBase && row.BaseSnapshotID != uuid.Nil {
		orderedSIDs = append(orderedSIDs, row.BaseSnapshotID)
	}
	var restSIDs []uuid.UUID
	for sid := range pendingSnapIDs {
		if sid != row.BaseSnapshotID {
			restSIDs = append(restSIDs, sid)
		}
	}
	slices.SortFunc(restSIDs, func(a, b uuid.UUID) int {
		return strings.Compare(a.String(), b.String())
	})
	orderedSIDs = append(orderedSIDs, restSIDs...)

	tgtDir := m.diskDir(diskID)
	for _, sid := range orderedSIDs {
		stagingDir := m.stagingMergeDir(diskID, sid)
		oldDir := m.oldMergeDir(diskID, sid)

		_, tgtErr := os.Stat(tgtDir)
		tgtExists := tgtErr == nil
		tgtMissing := errors.Is(tgtErr, os.ErrNotExist)
		_, stgErr := os.Stat(stagingDir)
		stagingExists := stgErr == nil
		_, oldErr := os.Stat(oldDir)
		oldExists := oldErr == nil

		shouldRollForward := (row.BaseSnapshotID == sid) || (tgtMissing && stagingExists && oldExists)
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
				m.lineageSnapshotMu.Lock()
				if _, committed := knownSnapshots[sid]; !committed {
					_ = m.removeAll(m.snapshotDir(sid.String()))
				}
				for _, srcSID := range linkedSrcSnaps[sid] {
					if _, committed := knownSnapshots[srcSID]; !committed {
						_ = m.removeAll(m.snapshotDir(srcSID.String()))
					}
				}
				m.lineageSnapshotMu.Unlock()
			}
		}

		for _, mp := range markerPaths[sid] {
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
		for _, marker := range []string{".merge.", ".old.", ".srcsnap."} {
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
			strings.HasPrefix(name, diskID+".srcsnap.") {
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
