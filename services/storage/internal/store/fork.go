package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	checkpointLinkThreshold  = 32
	maxCheckpointLinkWorkers = 8
)

// InodeSegmentAllocator 定义同源血缘树跨实例防撞的 Inode 连续号段分配函数。
// minNext 为当前实例已知最小可用起始节点号（max(本地最大已有 Inode, RootIno) + 1），
// 返回的闭区间 [first, last] 须满足 first >= minNext 且 last >= first。
type InodeSegmentAllocator func(ctx context.Context, minNext uint64) (first, last uint64, err error)

// CheckpointStats 承载在线无感 Checkpoint 固化时刻的一致性统计快照。
type CheckpointStats struct {
	LogicalBytes uint64
	Inodes       uint64
	MaxBytes     uint64
	HasOrphans   bool
	LivePacks    int
}

type checkpointOp struct {
	destDir string
	stats   CheckpointStats
}

// HasOrphans 在生命周期读守卫下通过迭代器快速判定当前实例是否存在孤儿节点（orphan:<ino>）。
func (s *StoreActor) HasOrphans() (has bool, err error) {
	unlock, err := s.enter()
	if err != nil {
		return false, err
	}
	defer unlock()

	return s.hasOrphansInDB()
}

func (s *StoreActor) hasOrphansInDB() (has bool, err error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	prefix := OrphanPrefix()
	valid := iter.SeekGE(prefix) && bytes.HasPrefix(iter.Key(), prefix)
	if iterErr := iter.Error(); iterErr != nil {
		return false, iterErr
	}
	return valid, nil
}

// Checkpoint 通过 Actor 内部同步屏障命令 opCheckpoint 在线生成一致性切面目录：
//  1. 先将当前批次、dirtyAtime、dirtyPackStats 与 sys:stat 完整提交并完成 WAL Sync；
//  2. 执行 db.Flush() 将活跃 MemTable 落盘为不可变 L0 .sst；
//  3. 读取孤儿节点标记并收集所有 LiveBytes > 0 的存活 .pack 编号；
//  4. 生成 Pebble Checkpoint 并将封口刷盘后的存活 .pack 硬链接至切面 packs/ 目录（全程不递增源盘 WriterEpoch）。
func (s *StoreActor) Checkpoint(ctx context.Context, destDir string) (CheckpointStats, error) {
	if s.readOnly {
		return CheckpointStats{}, ErrReadOnly
	}
	if destDir == "" {
		return CheckpointStats{}, ErrInvalid
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return CheckpointStats{}, err
		}
	}
	cleanDest := filepath.Clean(destDir)
	if _, statErr := os.Stat(cleanDest); statErr == nil {
		return CheckpointStats{}, ErrExist
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return CheckpointStats{}, statErr
	}

	op := &checkpointOp{destDir: cleanDest}
	cmd := s.acquireCmd()
	cmd.op = opCheckpoint
	cmd.req = op
	if err := s.submit(ctx, cmd); err != nil {
		return CheckpointStats{}, err
	}
	return op.stats, nil
}

func (s *StoreActor) validateCheckpointCmd(cmd *writeCmd) error {
	op, ok := cmd.req.(*checkpointOp)
	if !ok || op == nil || op.destDir == "" {
		return ErrInvalid
	}
	if _, statErr := os.Stat(op.destDir); statErr == nil {
		return ErrExist
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	return nil
}

func (s *StoreActor) mutateCheckpointCmd(cmd *writeCmd) {
	cmd.syncBarrier = true
}

func (s *StoreActor) execCheckpointCmd(ctx context.Context, cmd *writeCmd) error {
	op, ok := cmd.req.(*checkpointOp)
	if !ok || op == nil {
		return ErrInvalid
	}
	if s.blobs == nil {
		return ErrIO
	}

	// 1. 将活跃 MemTable 刷为不可变 L0 .sst，消除切面打开时的 WAL 回放开销。
	if flushErr := s.db.Flush(); flushErr != nil {
		return flushErr
	}

	// 2. 检测孤儿节点并提取已提交且存活字节 > 0 的 .pack 集合。
	hasOrphans, orphanErr := s.hasOrphansInDB()
	if orphanErr != nil {
		return orphanErr
	}
	livePackIDs := make([]uint64, 0, len(s.packStats))
	for packID, meta := range s.packStats {
		if packID != 0 && meta.LiveBytes > 0 {
			livePackIDs = append(livePackIDs, packID)
		}
	}
	slices.Sort(livePackIDs)

	// 3. Pebble DB.Checkpoint 要求目标目录预先不存在（否则返回 os.ErrExist），
	//    故先在 runLoop 静止屏障内创建 Pebble Checkpoint，再封口刷盘活跃 .pack 并硬链接存活包至切面目录。
	if ckErr := s.db.Checkpoint(op.destDir); ckErr != nil {
		return ckErr
	}
	if sealErr := s.blobs.sealAndSync(ctx, livePackIDs); sealErr != nil {
		_ = os.RemoveAll(op.destDir)
		return sealErr
	}
	// 还没下载回来的包链接它的占位文件；持有 hydrMu 让判断与链接之间不会有包就位。
	s.blobs.hydrMu.Lock()
	packNames := s.blobs.checkpointPackNamesLocked(livePackIDs)
	s.blobs.filesMu.RLock()
	linkErr := linkPackFilesToDir(s.blobs.dir, filepath.Join(op.destDir, "packs"), packNames, s.metrics)
	s.blobs.filesMu.RUnlock()
	s.blobs.hydrMu.Unlock()
	if linkErr != nil {
		_ = os.RemoveAll(op.destDir)
		return linkErr
	}
	if syncErr := syncDirPath(op.destDir); syncErr != nil {
		_ = os.RemoveAll(op.destDir)
		return syncErr
	}

	s.publishAtomicStats()

	maxB := s.maxBytes.Load()
	if maxB == 0 {
		maxB = s.stat.MaxBytes
	}
	op.stats = CheckpointStats{
		LogicalBytes: s.stat.LogicalBytes,
		Inodes:       s.stat.Inodes,
		MaxBytes:     maxB,
		HasOrphans:   hasOrphans,
		LivePacks:    len(livePackIDs),
	}
	return nil
}

// SanitizeOptions 定义独立执行 Checkpoint 孤儿净化与统计恢复的可观测配置。
type SanitizeOptions struct {
	Dir       string
	CommitID  string
	Cache     *pebble.Cache
	Logger    *slog.Logger
	Metrics   *Metrics
	OpenActor func(context.Context, Options) (*StoreActor, error)
}

// SanitizeCheckpoint 在临时可写 StoreActor（ResetEpoch: true）上清理切面目录中残留的孤儿节点及其独占死包，
// 刷盘并关闭临时实例后返回净化后的容量与节点统计。
func SanitizeCheckpoint(ctx context.Context, opts SanitizeOptions) (st StatFSResult, retErr error) {
	if opts.Dir == "" {
		return StatFSResult{}, ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var span trace.Span
	if isSpanRecording(ctx) {
		ctx, span = startChildSpan(ctx, "store.SanitizeCheckpoint",
			attribute.String("bowl.commit_id", opts.CommitID),
		)
		defer func() { finishSpan(span, retErr) }()
	}

	diskID := "commit:" + opts.CommitID
	if opts.CommitID == "" {
		diskID = "commit:" + filepath.Base(filepath.Clean(opts.Dir))
	}

	openFn := opts.OpenActor
	if openFn == nil {
		openFn = func(_ context.Context, o Options) (*StoreActor, error) {
			return Open(o)
		}
	}

	actor, err := openFn(ctx, Options{
		Dir:        opts.Dir,
		DiskID:     diskID,
		Cache:      opts.Cache,
		Logger:     opts.Logger,
		Metrics:    opts.Metrics,
		ResetEpoch: true,
	})
	if err != nil {
		return StatFSResult{}, err
	}

	if cleanErr := actor.CleanOrphans(ctx); cleanErr != nil {
		_ = actor.Close()
		return StatFSResult{}, cleanErr
	}
	if walErr := actor.FlushWAL(); walErr != nil {
		_ = actor.Close()
		return StatFSResult{}, walErr
	}
	if flushErr := actor.db.Flush(); flushErr != nil {
		_ = actor.Close()
		return StatFSResult{}, flushErr
	}
	st, statErr := actor.StatFS(ctx)
	if statErr != nil {
		_ = actor.Close()
		return StatFSResult{}, statErr
	}
	if closeErr := actor.Close(); closeErr != nil {
		return StatFSResult{}, closeErr
	}
	if span != nil {
		span.SetAttributes(
			attribute.Int64("sanitize.used_bytes", int64(st.UsedBytes)),
			attribute.Int64("sanitize.files", int64(st.Files)),
		)
	}
	return st, nil
}

// CloneCheckpointDir 将不可变基准切面目录克隆至新盘工作目录：
//   - 对不可变数据文件（*.sst、*.blob、packs/*.pack）执行 os.Link 零拷贝硬链接；
//   - 对其余 Pebble 元数据文件（CURRENT、MANIFEST-*、OPTIONS-*、format-version.*、marker.*、*.log，跳过 LOCK）执行字节复制；
//   - 同步目标目录与父目录，任意步骤失败时自动清理 dstDir。
func CloneCheckpointDir(ctx context.Context, srcDir, dstDir string) (retErr error) {
	if srcDir == "" || dstDir == "" {
		return ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var span trace.Span
	if isSpanRecording(ctx) {
		_, span = startChildSpan(ctx, "store.CloneCheckpointDir")
		defer func() { finishSpan(span, retErr) }()
	}
	srcClean := filepath.Clean(srcDir)
	dstClean := filepath.Clean(dstDir)
	if srcClean == dstClean {
		return ErrInvalid
	}

	srcInfo, err := os.Stat(srcClean)
	if err != nil {
		return err
	}
	if !srcInfo.IsDir() {
		return ErrNotDir
	}

	if _, statErr := os.Stat(dstClean); statErr == nil {
		return ErrExist
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}

	parentDir := filepath.Dir(dstClean)
	if mkParentErr := os.MkdirAll(parentDir, 0o755); mkParentErr != nil {
		return mkParentErr
	}
	if mkErr := os.Mkdir(dstClean, 0o755); mkErr != nil {
		return mkErr
	}
	defer func() {
		if retErr != nil {
			_ = os.RemoveAll(dstClean)
		}
	}()

	entries, readErr := os.ReadDir(srcClean)
	if readErr != nil {
		return readErr
	}

	for _, entry := range entries {
		name := entry.Name()
		if name == "LOCK" || name == PebbleEventsLogFileName {
			continue
		}
		srcPath := filepath.Join(srcClean, name)
		dstPath := filepath.Join(dstClean, name)

		if entry.IsDir() {
			if name == "packs" {
				if packErr := clonePacksSubdir(srcPath, dstPath); packErr != nil {
					return packErr
				}
				continue
			}
			if subErr := copySubdirRecursive(srcPath, dstPath); subErr != nil {
				return subErr
			}
			continue
		}

		if isImmutablePebbleDataFile(name) {
			if linkErr := os.Link(srcPath, dstPath); linkErr != nil {
				return linkErr
			}
			continue
		}
		if copyErr := copyFileWithSync(srcPath, dstPath); copyErr != nil {
			return copyErr
		}
	}

	if syncDstErr := syncDirPath(dstClean); syncDstErr != nil {
		return syncDstErr
	}
	return syncDirPath(parentDir)
}

func isImmutablePebbleDataFile(name string) bool {
	return strings.HasSuffix(name, ".sst") || strings.HasSuffix(name, ".blob")
}

func clonePacksSubdir(srcPacksDir, dstPacksDir string) error {
	if mkErr := os.MkdirAll(dstPacksDir, 0o755); mkErr != nil {
		return mkErr
	}
	entries, readErr := os.ReadDir(srcPacksDir)
	if readErr != nil {
		return readErr
	}

	packNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if _, isStub := PackNameOfStub(name); isStub || strings.HasSuffix(name, ".pack") {
			// 占位文件写完后同样不可变，和数据包一样硬链接。
			packNames = append(packNames, name)
			continue
		}
		if copyErr := copyFileWithSync(filepath.Join(srcPacksDir, name), filepath.Join(dstPacksDir, name)); copyErr != nil {
			return copyErr
		}
	}

	return linkPackFilesToDir(srcPacksDir, dstPacksDir, packNames, nil)
}

func linkPackFilesToDir(srcPacksDir, dstPacksDir string, packNames []string, metrics *Metrics) error {
	if dstPacksDir == "" {
		return ErrInvalid
	}
	if mkErr := os.MkdirAll(dstPacksDir, 0o755); mkErr != nil {
		return mkErr
	}

	if len(packNames) < checkpointLinkThreshold {
		for _, name := range packNames {
			if linkErr := os.Link(filepath.Join(srcPacksDir, name), filepath.Join(dstPacksDir, name)); linkErr != nil {
				return linkErr
			}
		}
	} else {
		workers := min(len(packNames), maxCheckpointLinkWorkers)
		errList := make([]error, len(packNames))
		var failed atomic.Bool
		var nextIdx atomic.Uint32
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				for {
					if failed.Load() {
						return
					}
					i := int(nextIdx.Add(1) - 1)
					if i >= len(packNames) {
						return
					}
					name := packNames[i]
					if linkErr := os.Link(filepath.Join(srcPacksDir, name), filepath.Join(dstPacksDir, name)); linkErr != nil {
						errList[i] = linkErr
						failed.Store(true)
						return
					}
				}
			})
		}
		wg.Wait()
		var errs []error
		for _, linkErr := range errList {
			if linkErr != nil {
				errs = append(errs, linkErr)
			}
		}
		if joinErr := errors.Join(errs...); joinErr != nil {
			return joinErr
		}
	}

	var dirSyncStart time.Time
	if metrics != nil {
		dirSyncStart = time.Now()
	}
	dirSyncErr := syncDirPath(dstPacksDir)
	if metrics != nil {
		metrics.observeBlobSync(syncTargetPacksDir, syncTriggerFlushWAL, time.Since(dirSyncStart))
	}
	return dirSyncErr
}

func copySubdirRecursive(srcDir, dstDir string) error {
	if mkErr := os.MkdirAll(dstDir, 0o755); mkErr != nil {
		return mkErr
	}
	entries, readErr := os.ReadDir(srcDir)
	if readErr != nil {
		return readErr
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "LOCK" || name == PebbleEventsLogFileName {
			continue
		}
		srcPath := filepath.Join(srcDir, name)
		dstPath := filepath.Join(dstDir, name)
		if entry.IsDir() {
			if subErr := copySubdirRecursive(srcPath, dstPath); subErr != nil {
				return subErr
			}
			continue
		}
		if copyErr := copyFileWithSync(srcPath, dstPath); copyErr != nil {
			return copyErr
		}
	}
	return syncDirPath(dstDir)
}

func copyFileWithSync(srcPath, dstPath string) error {
	srcFile, openErr := os.Open(srcPath)
	if openErr != nil {
		return openErr
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, createErr := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if createErr != nil {
		return createErr
	}
	defer func() {
		if dstFile != nil {
			_ = dstFile.Close()
		}
	}()

	if _, copyErr := io.Copy(dstFile, srcFile); copyErr != nil {
		return copyErr
	}
	if syncErr := dstFile.Sync(); syncErr != nil {
		return syncErr
	}
	closeErr := dstFile.Close()
	dstFile = nil
	return closeErr
}

func syncDirPath(dir string) error {
	handle, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.Join(handle.Sync(), handle.Close())
}
