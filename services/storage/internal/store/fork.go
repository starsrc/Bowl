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

type InodeSegmentAllocator func(ctx context.Context, minNext uint64) (first, last uint64, err error)

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

	if flushErr := s.db.Flush(); flushErr != nil {
		return flushErr
	}

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

	if ckErr := s.db.Checkpoint(op.destDir); ckErr != nil {
		return ckErr
	}
	if sealErr := s.blobs.sealAndSync(ctx, livePackIDs); sealErr != nil {
		_ = os.RemoveAll(op.destDir)
		return sealErr
	}
	packNames := make([]string, 0, len(livePackIDs))
	for _, id := range livePackIDs {
		if id != 0 {
			packNames = append(packNames, packFileName(id))
		}
	}
	s.blobs.filesMu.RLock()
	linkErr := linkPackFilesToDir(s.blobs.dir, filepath.Join(op.destDir, "packs"), packNames, s.metrics)
	s.blobs.filesMu.RUnlock()
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

type SanitizeOptions struct {
	Dir       string
	CommitID  string
	Cache     *pebble.Cache
	Logger    *slog.Logger
	Metrics   *Metrics
	OpenActor func(context.Context, Options) (*StoreActor, error)
}

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
		if strings.HasSuffix(name, ".pack") {
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
