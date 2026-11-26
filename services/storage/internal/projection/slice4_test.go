package projection

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

type slice4LogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *slice4LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *slice4LogBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func (b *slice4LogBuffer) Records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	raw := b.buf.String()
	b.mu.Unlock()

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	lines := strings.Split(trimmed, "\n")
	out := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		out = append(out, rec)
	}
	return out
}

func newTestLogger(level slog.Level) (*slog.Logger, *slice4LogBuffer) {
	buf := &slice4LogBuffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level}))
	return logger, buf
}

func TestSlice4_Step14_LoggerWiringAndNilDefaultResolution(t *testing.T) {
	ctx := context.Background()

	explicitLogger, _ := newTestLogger(slog.LevelDebug)
	mgr, err := ProvideControlManager(ControlManagerParams{
		Store: NewMemStore(),
		MetaCfg: &config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
			GCIntervalVal:     0,
		},
		Logger: explicitLogger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })
	require.Same(t, explicitLogger, mgr.logger)
	require.Same(t, explicitLogger, mgr.queue.logger)

	prevDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prevDefault) })

	defaultA, _ := newTestLogger(slog.LevelDebug)
	defaultB, _ := newTestLogger(slog.LevelDebug)
	slog.SetDefault(defaultA)

	nilLoggerMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, nilLoggerMgr.Close(ctx)) })

	nilLoggerQueue := newQueue(NewMemStore(), queueConfig{
		flushInterval: time.Hour,
	})
	t.Cleanup(func() { require.NoError(t, nilLoggerQueue.close(ctx)) })

	slog.SetDefault(defaultB)
	require.Same(t, defaultA, nilLoggerMgr.logger)
	require.Same(t, defaultA, nilLoggerMgr.queue.logger)
	require.Same(t, defaultA, nilLoggerQueue.logger)

	fxLogger, _ := newTestLogger(slog.LevelInfo)
	var fxMgr *ControlManager
	app := fx.New(
		fx.NopLogger,
		fx.Supply(
			&config.MetaConfig{
				StorageDataDirVal: t.TempDir(),
				GCIntervalVal:     0,
			},
			fxLogger,
		),
		fx.Provide(
			func() Store { return NewMemStore() },
			ProvideControlManager,
		),
		fx.Populate(&fxMgr),
	)
	require.NoError(t, app.Start(ctx))
	t.Cleanup(func() { require.NoError(t, app.Stop(context.Background())) })
	require.Same(t, fxLogger, fxMgr.logger)
	require.Same(t, fxLogger, fxMgr.queue.logger)

	var fxNoLoggerMgr *ControlManager
	appNoLogger := fx.New(
		fx.NopLogger,
		fx.Supply(&config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
			GCIntervalVal:     0,
		}),
		fx.Provide(
			func() Store { return NewMemStore() },
			ProvideControlManager,
		),
		fx.Populate(&fxNoLoggerMgr),
	)
	require.NoError(t, appNoLogger.Start(ctx))
	t.Cleanup(func() { require.NoError(t, appNoLogger.Stop(context.Background())) })
	require.Same(t, defaultB, fxNoLoggerMgr.logger)
	require.Same(t, defaultB, fxNoLoggerMgr.queue.logger)
}

func recordsByLevel(recs []map[string]any, level string) []map[string]any {
	var out []map[string]any
	for _, r := range recs {
		if r["level"] == level {
			out = append(out, r)
		}
	}
	return out
}

func assertHasKeys(t *testing.T, rec map[string]any, keys ...string) {
	t.Helper()
	for _, k := range keys {
		require.Contains(t, rec, k)
	}
}

func TestSlice4_Step15_HealthyHotPathAndNilStoreStandaloneSilent(t *testing.T) {
	ctx := context.Background()

	healthyLogger, healthyBuf := newTestLogger(slog.LevelDebug)
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Logger:                  healthyLogger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })

	disk, err := mgr.CreateDisk(ctx, "healthy-disk", 16<<20)
	require.NoError(t, err)
	newName := "healthy-disk-renamed"
	_, err = mgr.UpdateDisk(ctx, disk.DiskID, &newName, nil)
	require.NoError(t, err)
	_, err = mgr.Commit(ctx, disk.DiskID, "main", "c1")
	require.NoError(t, err)
	_, _, err = mgr.ForkBranch(ctx, disk.DiskID, "main", "feat", "fork")
	require.NoError(t, err)
	mountRes, err := mgr.MountBranch(ctx, disk.DiskID, "main", false, false)
	require.NoError(t, err)
	_, err = mgr.RenewMount(ctx, disk.DiskID, "main", mountRes.Mount.MountID, false, mountRes.Mount.WriterEpoch)
	require.NoError(t, err)
	require.NoError(t, mgr.UnmountBranch(ctx, disk.DiskID, mountRes.Mount.MountID))
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.ListBranches(ctx, disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.ListCommits(ctx, ListCommitsQuery{DiskID: disk.DiskID})
	require.NoError(t, err)
	_, err = mgr.ListMounts(ctx, disk.DiskID, "main")
	require.NoError(t, err)
	require.Empty(t, healthyBuf.Records(t))

	nilLogger, nilBuf := newTestLogger(slog.LevelDebug)
	nilMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   nil,
		Logger:                  nilLogger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, nilMgr.Close(ctx)) })

	nilDisk, err := nilMgr.CreateDisk(ctx, "standalone-disk", 16<<20)
	require.NoError(t, err)
	_, err = nilMgr.Commit(ctx, nilDisk.DiskID, "main", "c1")
	require.NoError(t, err)
	_, err = nilMgr.GetDisk(ctx, nilDisk.DiskID)
	require.NoError(t, err)
	_, err = nilMgr.ListBranches(ctx, nilDisk.DiskID)
	require.NoError(t, err)
	_, err = nilMgr.ListCommits(ctx, ListCommitsQuery{DiskID: nilDisk.DiskID})
	require.NoError(t, err)
	_, err = nilMgr.ListMounts(ctx, nilDisk.DiskID, "main")
	require.NoError(t, err)
	nilMgr.queue.recordUsage(nilDisk.DiskID, 512)
	nilMgr.queue.recordMountRenew(nilDisk.DiskID, nilDisk.DiskID, time.Now().Add(time.Minute))
	nilMgr.queue.flushBatches(ctx)
	require.NoError(t, nilMgr.reconcileDirtyDisks(ctx))
	require.Empty(t, nilBuf.Records(t))
}

func TestSlice4_Step15_SyncProjectFailureWarnAndDirtyStormDebugControl(t *testing.T) {
	ctx := context.Background()
	logger, buf := newTestLogger(slog.LevelDebug)
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Logger:                  logger,
		SyncWriteTimeout:        100 * time.Millisecond,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })

	disk, err := mgr.CreateDisk(ctx, "sync-storm", 16<<20)
	require.NoError(t, err)
	buf.Reset()

	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return ErrUnavailable
		},
	})

	_, err = mgr.Commit(ctx, disk.DiskID, "main", "first-fail")
	require.NoError(t, err)
	require.True(t, mgr.queue.isDirty(disk.DiskID))

	for range 4 {
		_, err = mgr.Commit(ctx, disk.DiskID, "main", "dirty-skip")
		require.NoError(t, err)
	}

	recs := buf.Records(t)
	warns := recordsByLevel(recs, "WARN")
	debugs := recordsByLevel(recs, "DEBUG")
	require.Len(t, warns, 1)
	require.Equal(t, "commit", warns[0]["op"])
	require.Equal(t, disk.DiskID.String(), warns[0]["disk_id"])
	require.Equal(t, false, warns[0]["catalog"])
	assertHasKeys(t, warns[0], "duration", "error")

	require.Len(t, debugs, 4)
	for _, d := range debugs {
		require.Equal(t, "commit", d["op"])
		require.Equal(t, disk.DiskID.String(), d["disk_id"])
		require.Equal(t, false, d["catalog"])
	}

	store.SetHooks(MemStoreHooks{})
	require.NoError(t, mgr.reconcileDisk(ctx, disk.DiskID, false, false))
	buf.Reset()

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	mgr.syncProject(canceledCtx, "update_disk", disk.DiskID, true, func(context.Context, Store) error {
		return nil
	})
	require.True(t, mgr.queue.isDirty(disk.DiskID))
	canceledWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, canceledWarns, 1)
	require.Equal(t, "update_disk", canceledWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), canceledWarns[0]["disk_id"])
	require.Equal(t, true, canceledWarns[0]["catalog"])
	assertHasKeys(t, canceledWarns[0], "duration", "error")
}

func TestSlice4_Step15_SyncProjectAll13OpNames(t *testing.T) {
	ctx := context.Background()
	logger, buf := newTestLogger(slog.LevelWarn)
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Logger:                  logger,
		SyncWriteTimeout:        100 * time.Millisecond,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })

	failWrites := func() {
		store.SetHooks(MemStoreHooks{
			BeforeWrite: func(context.Context, string, uuid.UUID) error {
				return ErrUnavailable
			},
		})
	}
	clearAndReconcile := func(diskID uuid.UUID) {
		store.SetHooks(MemStoreHooks{})
		require.NoError(t, mgr.reconcileDisk(ctx, diskID, false, false))
	}

	failWrites()
	disk, err := mgr.CreateDisk(ctx, "ops-disk", 16<<20)
	require.NoError(t, err)
	clearAndReconcile(disk.DiskID)

	failWrites()
	updated := "ops-disk-2"
	_, err = mgr.UpdateDisk(ctx, disk.DiskID, &updated, nil)
	require.NoError(t, err)
	clearAndReconcile(disk.DiskID)

	failWrites()
	c1, err := mgr.Commit(ctx, disk.DiskID, "main", "c1")
	require.NoError(t, err)
	clearAndReconcile(disk.DiskID)

	failWrites()
	_, _, err = mgr.ForkBranch(ctx, disk.DiskID, "main", "feat", "fork")
	require.NoError(t, err)
	clearAndReconcile(disk.DiskID)

	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "feat", "f.txt", []byte("data"))
	_, err = mgr.Commit(ctx, disk.DiskID, "feat", "feat-c")
	require.NoError(t, err)

	failWrites()
	_, err = mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
	})
	require.NoError(t, err)
	clearAndReconcile(disk.DiskID)

	failWrites()
	_, err = mgr.ResetBranch(ctx, disk.DiskID, "feat", c1.CommitID)
	require.NoError(t, err)
	clearAndReconcile(disk.DiskID)

	_, err = mgr.Commit(ctx, disk.DiskID, "feat", "orphan-1")
	require.NoError(t, err)
	orphan2, err := mgr.Commit(ctx, disk.DiskID, "feat", "orphan-2")
	require.NoError(t, err)
	_, err = mgr.ResetBranch(ctx, disk.DiskID, "feat", c1.CommitID)
	require.NoError(t, err)

	failWrites()
	require.NoError(t, mgr.DeleteCommit(ctx, disk.DiskID, orphan2.CommitID))
	clearAndReconcile(disk.DiskID)

	failWrites()
	pruned, err := mgr.PruneCommits(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Positive(t, pruned)
	clearAndReconcile(disk.DiskID)

	failWrites()
	require.NoError(t, mgr.DeleteBranch(ctx, disk.DiskID, "feat"))
	clearAndReconcile(disk.DiskID)

	failWrites()
	m1, err := mgr.MountBranch(ctx, disk.DiskID, "main", true, false)
	require.NoError(t, err)
	clearAndReconcile(disk.DiskID)

	failWrites()
	require.NoError(t, mgr.UnmountBranch(ctx, disk.DiskID, m1.Mount.MountID))
	clearAndReconcile(disk.DiskID)

	_, err = mgr.MountBranch(ctx, disk.DiskID, "main", true, false)
	require.NoError(t, err)
	failWrites()
	_, err = mgr.EvictMounts(ctx, disk.DiskID, "main", uuid.Nil)
	require.NoError(t, err)
	clearAndReconcile(disk.DiskID)

	failWrites()
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
	store.SetHooks(MemStoreHooks{})

	warns := recordsByLevel(buf.Records(t), "WARN")
	gotOps := make([]string, 0, len(warns))
	for _, w := range warns {
		op, ok := w["op"].(string)
		require.True(t, ok)
		gotOps = append(gotOps, op)
	}
	wantOps := []string{
		"create_disk",
		"update_disk",
		"commit",
		"fork_branch",
		"merge_branch",
		"reset_branch",
		"delete_commit",
		"prune_commits",
		"delete_branch",
		"mount_branch",
		"unmount_branch",
		"evict_mounts",
		"delete_disk",
	}
	require.Equal(t, wantOps, gotOps)
}

func TestSlice4_Step15_ReadWithFallbackWarnAndDirtyDebug(t *testing.T) {
	ctx := context.Background()
	logger, buf := newTestLogger(slog.LevelDebug)
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Logger:                  logger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })

	disk, err := mgr.CreateDisk(ctx, "read-fallback", 16<<20)
	require.NoError(t, err)
	buf.Reset()

	store.SetHooks(MemStoreHooks{
		BeforeRead: func(context.Context, string) error {
			return ErrUnavailable
		},
	})
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.ListBranches(ctx, disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.ListCommits(ctx, ListCommitsQuery{DiskID: disk.DiskID})
	require.NoError(t, err)
	_, err = mgr.ListMounts(ctx, disk.DiskID, "main")
	require.NoError(t, err)

	warns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, warns, 4)
	wantReadOps := []string{"get_disk", "list_branches", "list_commits", "list_mounts"}
	for i, wantOp := range wantReadOps {
		require.Equal(t, wantOp, warns[i]["op"])
		require.Equal(t, disk.DiskID.String(), warns[i]["disk_id"])
		assertHasKeys(t, warns[i], "error")
	}

	buf.Reset()
	store.SetHooks(MemStoreHooks{})
	mgr.queue.markDirty(disk.DiskID, false)

	_, err = mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.ListBranches(ctx, disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.ListCommits(ctx, ListCommitsQuery{DiskID: disk.DiskID})
	require.NoError(t, err)
	_, err = mgr.ListMounts(ctx, disk.DiskID, "main")
	require.NoError(t, err)

	recs := buf.Records(t)
	require.Empty(t, recordsByLevel(recs, "WARN"))
	debugs := recordsByLevel(recs, "DEBUG")
	require.Len(t, debugs, 4)
	for i, wantOp := range wantReadOps {
		require.Equal(t, wantOp, debugs[i]["op"])
		require.Equal(t, disk.DiskID.String(), debugs[i]["disk_id"])
	}
}

func TestSlice4_Step15_FlushBatchesSingleWarnPerBatchAndMaintenanceSummary(t *testing.T) {
	ctx := context.Background()
	logger, buf := newTestLogger(slog.LevelDebug)
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Logger:                  logger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })

	d1, err := mgr.CreateDisk(ctx, "maint-1", 16<<20)
	require.NoError(t, err)
	d2, err := mgr.CreateDisk(ctx, "maint-2", 16<<20)
	require.NoError(t, err)
	d3, err := mgr.CreateDisk(ctx, "maint-3", 16<<20)
	require.NoError(t, err)

	buf.Reset()
	store.SetHooks(MemStoreHooks{
		BeforeBatch: func(context.Context, string) error {
			return ErrUnavailable
		},
	})
	mgr.queue.recordUsage(d1.DiskID, 100)
	mgr.queue.recordUsage(d2.DiskID, 200)
	mgr.queue.recordMountRenew(d1.DiskID, uuid.New(), time.Now().Add(time.Minute))
	mgr.queue.recordMountRenew(d2.DiskID, uuid.New(), time.Now().Add(time.Minute))
	mgr.queue.recordMountRenew(d3.DiskID, uuid.New(), time.Now().Add(time.Minute))
	mgr.queue.flushBatches(ctx)

	batchWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, batchWarns, 2)
	require.Equal(t, "usage", batchWarns[0]["batch_type"])
	require.InDelta(t, float64(2), batchWarns[0]["batch_size"], 0)
	assertHasKeys(t, batchWarns[0], "duration", "error")
	require.Equal(t, "renew", batchWarns[1]["batch_type"])
	require.InDelta(t, float64(3), batchWarns[1]["batch_size"], 0)
	assertHasKeys(t, batchWarns[1], "duration", "error")

	buf.Reset()
	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return ErrUnavailable
		},
	})
	mgr.queue.markDirty(d1.DiskID, false)
	mgr.queue.markDirty(d2.DiskID, false)
	mgr.queue.markDirty(d3.DiskID, false)

	err = mgr.reconcileDirtyDisks(ctx)
	require.Error(t, err)

	reconcileRecs := buf.Records(t)
	reconcileWarns := recordsByLevel(reconcileRecs, "WARN")
	reconcileDebugs := recordsByLevel(reconcileRecs, "DEBUG")
	require.Len(t, reconcileWarns, 1)
	require.InDelta(t, float64(3), reconcileWarns[0]["dirty_disks"], 0)
	require.InDelta(t, float64(3), reconcileWarns[0]["failed_disks"], 0)
	assertHasKeys(t, reconcileWarns[0], "sample_disk_id", "duration", "error")
	require.Len(t, reconcileDebugs, 3)

	store.SetHooks(MemStoreHooks{})
	require.NoError(t, mgr.reconcileDirtyDisks(ctx))

	loopLogger, loopBuf := newTestLogger(slog.LevelWarn)
	loopStore := NewMemStore()
	loopStore.SetHooks(MemStoreHooks{
		BeforeBatch: func(_ context.Context, op string) error {
			if op == "PruneExpiredMounts" {
				return ErrUnavailable
			}
			return nil
		},
	})
	loopMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   loopStore,
		Logger:                  loopLogger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       15 * time.Millisecond,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, loopMgr.Close(ctx)) })

	require.Eventually(t, func() bool {
		for _, r := range recordsByLevel(loopBuf.Records(t), "WARN") {
			if r["op"] == "prune_expired_mounts" {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
	loopStore.SetHooks(MemStoreHooks{})

	var sawPruneWarn bool
	for _, r := range recordsByLevel(loopBuf.Records(t), "WARN") {
		if r["op"] == "prune_expired_mounts" {
			assertHasKeys(t, r, "duration", "error")
			sawPruneWarn = true
		}
	}
	require.True(t, sawPruneWarn)
}

func injectCorruptMountInPebble(t *testing.T, storageRoot string, diskID uuid.UUID, branchName, badMountID string, expiresAt time.Time) {
	t.Helper()
	cmp := *pebble.DefaultComparer
	cmp.Name = "bowl.engine.comparer"
	cmp.Split = engine.SplitKey

	pebbleDir := filepath.Join(storageRoot, "disks", diskID.String(), "pebble")
	db, err := pebble.Open(pebbleDir, &pebble.Options{Comparer: &cmp})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	mm := engine.MountMeta{
		ID:          badMountID,
		BranchName:  branchName,
		ReadOnly:    true,
		WriterEpoch: 0,
		MountedAt:   expiresAt.Add(-time.Minute).UnixNano(),
		ExpiresAt:   expiresAt.UnixNano(),
	}
	require.NoError(t, db.Set(engine.MetaMountKey(badMountID), engine.EncodeMountMeta(mm), pebble.Sync))
}

func TestSlice4_Step16_StartupReconcileHealthySilentNilSuppressedAndRealFailureWarn(t *testing.T) {
	ctx := context.Background()

	healthyLogger, healthyBuf := newTestLogger(slog.LevelDebug)
	healthyMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:       t.TempDir(),
		Store:             NewMemStore(),
		Logger:            healthyLogger,
		FlushInterval:     time.Hour,
		ReconcileInterval: time.Hour,
		GCMode:            GCModeDisabled,
	})
	require.NoError(t, err)
	require.NoError(t, healthyMgr.waitStartupReconcile(ctx))
	require.NoError(t, healthyMgr.Close(ctx))
	require.Empty(t, healthyBuf.Records(t))

	nilLogger, nilBuf := newTestLogger(slog.LevelDebug)
	nilMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:       t.TempDir(),
		Store:             nil,
		Logger:            nilLogger,
		FlushInterval:     time.Hour,
		ReconcileInterval: time.Hour,
		GCMode:            GCModeDisabled,
	})
	require.NoError(t, err)
	require.ErrorIs(t, nilMgr.waitStartupReconcile(ctx), ErrUnavailable)
	require.NoError(t, nilMgr.Close(ctx))
	require.Empty(t, nilBuf.Records(t))

	failLogger, failBuf := newTestLogger(slog.LevelWarn)
	failStore := NewMemStore()
	failStore.SetHooks(MemStoreHooks{
		BeforeRead: func(context.Context, string) error {
			return ErrUnavailable
		},
	})
	failMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:       t.TempDir(),
		Store:             failStore,
		Logger:            failLogger,
		FlushInterval:     time.Hour,
		ReconcileInterval: time.Hour,
		GCMode:            GCModeDisabled,
	})
	require.NoError(t, err)
	require.ErrorIs(t, failMgr.waitStartupReconcile(ctx), ErrUnavailable)
	require.NoError(t, failMgr.Close(ctx))

	warns := recordsByLevel(failBuf.Records(t), "WARN")
	require.Len(t, warns, 1)
	require.Equal(t, "startup_reconcile", warns[0]["op"])
	assertHasKeys(t, warns[0], "duration", "error")
}

func TestSlice4_Step16_GracefulShutdownSuppressesExpectedCancellationErrors(t *testing.T) {
	ctx := context.Background()

	startupLogger, startupBuf := newTestLogger(slog.LevelWarn)
	startupStore := NewMemStore()
	startupEntered := make(chan struct{})
	var startupOnce sync.Once
	startupStore.SetHooks(MemStoreHooks{
		BeforeRead: func(hookCtx context.Context, op string) error {
			if op == "ListAllDiskIDs" {
				startupOnce.Do(func() { close(startupEntered) })
				<-hookCtx.Done()
				return hookCtx.Err()
			}
			return nil
		},
	})
	startupMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:       t.TempDir(),
		Store:             startupStore,
		Logger:            startupLogger,
		FlushInterval:     time.Hour,
		ReconcileInterval: time.Hour,
		GCMode:            GCModeDisabled,
	})
	require.NoError(t, err)
	<-startupEntered
	require.NoError(t, startupMgr.Close(ctx))
	require.Empty(t, recordsByLevel(startupBuf.Records(t), "WARN"))

	maintLogger, maintBuf := newTestLogger(slog.LevelWarn)
	maintStore := NewMemStore()
	maintEntered := make(chan struct{})
	var maintOnce sync.Once
	maintMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   maintStore,
		Logger:                  maintLogger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       10 * time.Millisecond,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	disk, err := maintMgr.CreateDisk(ctx, "shutdown-maint", 16<<20)
	require.NoError(t, err)
	maintStore.SetHooks(MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, op string, _ uuid.UUID) error {
			if op == "ReplaceDiskSnapshot" {
				maintOnce.Do(func() { close(maintEntered) })
				<-hookCtx.Done()
				return hookCtx.Err()
			}
			return nil
		},
	})
	maintMgr.queue.markDirty(disk.DiskID, false)
	<-maintEntered
	require.NoError(t, maintMgr.Close(ctx))
	require.Empty(t, recordsByLevel(maintBuf.Records(t), "WARN"))

	pruneLogger, pruneBuf := newTestLogger(slog.LevelWarn)
	pruneStore := NewMemStore()
	pruneEntered := make(chan struct{})
	var pruneOnce sync.Once
	pruneStore.SetHooks(MemStoreHooks{
		BeforeBatch: func(hookCtx context.Context, op string) error {
			if op == "PruneExpiredMounts" {
				pruneOnce.Do(func() { close(pruneEntered) })
				<-hookCtx.Done()
				return hookCtx.Err()
			}
			return nil
		},
	})
	pruneMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   pruneStore,
		Logger:                  pruneLogger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       10 * time.Millisecond,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	<-pruneEntered
	require.NoError(t, pruneMgr.Close(ctx))
	require.Empty(t, recordsByLevel(pruneBuf.Records(t), "WARN"))

	batchLogger, batchBuf := newTestLogger(slog.LevelWarn)
	batchStore := NewMemStore()
	batchEntered := make(chan struct{})
	var batchOnce sync.Once
	batchStore.SetHooks(MemStoreHooks{
		BeforeBatch: func(hookCtx context.Context, op string) error {
			if op == "BatchUpdateDiskUsage" {
				batchOnce.Do(func() { close(batchEntered) })
				<-hookCtx.Done()
				return hookCtx.Err()
			}
			return nil
		},
	})
	q := newQueue(batchStore, queueConfig{
		flushInterval: 10 * time.Millisecond,
		logger:        batchLogger,
	})
	q.recordUsage(uuid.New(), 256)
	<-batchEntered
	closeCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, q.close(closeCtx), context.DeadlineExceeded)
	require.Empty(t, recordsByLevel(batchBuf.Records(t), "WARN"))
}

func TestSlice4_Step16_PostPebbleSecondaryMetadataReadFailureWarns(t *testing.T) {
	ctx := context.Background()
	logger, buf := newTestLogger(slog.LevelWarn)
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Logger:                  logger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })

	disk, err := mgr.CreateDisk(ctx, "post-pebble-warn", 16<<20)
	require.NoError(t, err)
	sentinel := errors.New("synthetic secondary metadata failure")

	origGetBranch := mgr.getBranch
	origGetCommit := mgr.getCommit
	origSnap := mgr.snapshotControlMeta
	origMount := mgr.mountBranch

	buf.Reset()
	mgr.getBranch = func(*engine.DiskStore, string) (engine.BranchMeta, error) {
		return engine.BranchMeta{}, sentinel
	}
	_, _, err = mgr.ForkBranch(ctx, disk.DiskID, "main", "f1", "fork")
	mgr.getBranch = origGetBranch
	require.NoError(t, err)
	require.True(t, mgr.queue.isDirty(disk.DiskID))
	forkWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, forkWarns, 1)
	require.Equal(t, "fork_branch", forkWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), forkWarns[0]["disk_id"])
	require.Equal(t, "main", forkWarns[0]["branch"])
	assertHasKeys(t, forkWarns[0], "error")

	require.NoError(t, mgr.reconcileDisk(ctx, disk.DiskID, false, false))
	buf.Reset()
	mgr.getBranch = func(*engine.DiskStore, string) (engine.BranchMeta, error) {
		return engine.BranchMeta{}, sentinel
	}
	_, err = mgr.Commit(ctx, disk.DiskID, "main", "c-fail-read")
	mgr.getBranch = origGetBranch
	require.NoError(t, err)
	require.True(t, mgr.queue.isDirty(disk.DiskID))
	commitWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, commitWarns, 1)
	require.Equal(t, "commit", commitWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), commitWarns[0]["disk_id"])
	require.Equal(t, "main", commitWarns[0]["branch"])
	assertHasKeys(t, commitWarns[0], "commit_id", "error")

	require.NoError(t, mgr.reconcileDisk(ctx, disk.DiskID, false, false))
	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "f1", "a.txt", []byte("a"))
	_, err = mgr.Commit(ctx, disk.DiskID, "f1", "f1-c1")
	require.NoError(t, err)

	buf.Reset()
	mgr.getBranch = func(*engine.DiskStore, string) (engine.BranchMeta, error) {
		return engine.BranchMeta{}, sentinel
	}
	mgr.getCommit = func(*engine.DiskStore, uint64) (engine.CommitMeta, error) {
		return engine.CommitMeta{}, sentinel
	}
	mergeRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "f1",
		TargetBranch: "main",
	})
	mgr.getBranch = origGetBranch
	mgr.getCommit = origGetCommit
	require.NoError(t, err)
	require.False(t, mergeRes.UpToDate)
	mergeDirtyWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, mergeDirtyWarns, 2)
	require.Equal(t, "merge_branch", mergeDirtyWarns[0]["op"])
	require.Equal(t, "merge_branch", mergeDirtyWarns[1]["op"])
	assertHasKeys(t, mergeDirtyWarns[1], "commit_id", "error")

	require.NoError(t, mgr.reconcileDisk(ctx, disk.DiskID, false, false))
	buf.Reset()
	mgr.getBranch = func(*engine.DiskStore, string) (engine.BranchMeta, error) {
		return engine.BranchMeta{}, sentinel
	}
	mgr.getCommit = func(*engine.DiskStore, uint64) (engine.CommitMeta, error) {
		return engine.CommitMeta{}, sentinel
	}
	utdRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "f1",
		TargetBranch: "main",
	})
	mgr.getBranch = origGetBranch
	mgr.getCommit = origGetCommit
	require.NoError(t, err)
	require.True(t, utdRes.UpToDate)
	mergeUTDWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, mergeUTDWarns, 2)
	require.Equal(t, "merge_branch", mergeUTDWarns[0]["op"])
	require.Equal(t, "merge_branch", mergeUTDWarns[1]["op"])

	baseCmt, err := mgr.Commit(ctx, disk.DiskID, "f1", "base-for-prune")
	require.NoError(t, err)
	_, err = mgr.Commit(ctx, disk.DiskID, "f1", "orphan-prune")
	require.NoError(t, err)
	_, err = mgr.ResetBranch(ctx, disk.DiskID, "f1", baseCmt.CommitID)
	require.NoError(t, err)

	buf.Reset()
	mgr.snapshotControlMeta = func(*engine.DiskStore, time.Time) (engine.ControlSnapshot, error) {
		return engine.ControlSnapshot{}, sentinel
	}
	pruned, err := mgr.PruneCommits(ctx, disk.DiskID)
	mgr.snapshotControlMeta = origSnap
	require.NoError(t, err)
	require.Positive(t, pruned)
	require.True(t, mgr.queue.isDirty(disk.DiskID))
	pruneWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, pruneWarns, 1)
	require.Equal(t, "prune_commits", pruneWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), pruneWarns[0]["disk_id"])
	assertHasKeys(t, pruneWarns[0], "error")

	buf.Reset()
	mgr.mountBranch = func(*engine.DiskStore, context.Context, string, bool, bool, time.Duration) (engine.MountBranchResult, error) {
		return engine.MountBranchResult{
			Mount: engine.MountMeta{
				ID:         "not-a-valid-uuid",
				BranchName: "main",
			},
		}, nil
	}
	_, err = mgr.MountBranch(ctx, disk.DiskID, "main", true, false)
	mgr.mountBranch = origMount
	require.ErrorIs(t, err, ErrInvalidArgument)
	mountWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, mountWarns, 1)
	require.Equal(t, "mount_branch", mountWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), mountWarns[0]["disk_id"])
	require.Equal(t, "not-a-valid-uuid", mountWarns[0]["mount_id"])
	assertHasKeys(t, mountWarns[0], "branch", "error")
}

func TestSlice4_Step16_EvictionListDisksRollbackAndCorruptMountIDWarns(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC))
	logger, buf := newTestLogger(slog.LevelWarn)
	store := NewMemStore()
	root := t.TempDir()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Logger:                  logger,
		Now:                     clk.Now,
		ActorIdleTTL:            time.Second,
		SyncWriteTimeout:        100 * time.Millisecond,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })

	disk, err := mgr.CreateDisk(ctx, "step16-misc", 16<<20)
	require.NoError(t, err)
	clk.Advance(5 * time.Second)

	buf.Reset()
	origHasActive := mgr.hasActiveMounts
	mgr.hasActiveMounts = func(*engine.DiskStore, string, time.Time) (bool, error) {
		return false, errors.New("has active mounts failed")
	}
	require.Zero(t, mgr.evictIdleDisks(ctx))
	mgr.hasActiveMounts = origHasActive
	hasActiveWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, hasActiveWarns, 1)
	require.Equal(t, "evict_idle_disk", hasActiveWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), hasActiveWarns[0]["disk_id"])
	assertHasKeys(t, hasActiveWarns[0], "error")

	buf.Reset()
	origClose := mgr.closeDisk
	mgr.closeDisk = func(ds *engine.DiskStore) error {
		_ = ds.Close()
		return errors.New("evict close failed")
	}
	require.Equal(t, 1, mgr.evictIdleDisks(ctx))
	mgr.closeDisk = origClose
	evictCloseWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, evictCloseWarns, 1)
	require.Equal(t, "evict_idle_disk", evictCloseWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), evictCloseWarns[0]["disk_id"])
	assertHasKeys(t, evictCloseWarns[0], "error")

	buf.Reset()
	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return ErrUnavailable
		},
	})
	mgr.queue.markDirty(disk.DiskID, true)
	_, err = mgr.ListDisks(ctx, ListDisksQuery{})
	require.ErrorIs(t, err, ErrUnavailable)
	listWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, listWarns, 1)
	require.Equal(t, "list_disks", listWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), listWarns[0]["disk_id"])
	assertHasKeys(t, listWarns[0], "duration", "error")

	buf.Reset()
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = mgr.ListDisks(canceledCtx, ListDisksQuery{})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, recordsByLevel(buf.Records(t), "WARN"))
	store.SetHooks(MemStoreHooks{})
	require.NoError(t, mgr.reconcileDisk(ctx, disk.DiskID, false, false))

	buf.Reset()
	origGetBranch := mgr.getBranch
	origRemoveAll := mgr.removeAll
	mgr.getBranch = func(*engine.DiskStore, string) (engine.BranchMeta, error) {
		return engine.BranchMeta{}, errors.New("create getBranch failed")
	}
	mgr.closeDisk = func(ds *engine.DiskStore) error {
		_ = ds.Close()
		return errors.New("create rollback close failed")
	}
	mgr.removeAll = func(path string) error {
		_ = origRemoveAll(path)
		return errors.New("create rollback remove failed")
	}
	_, err = mgr.CreateDisk(ctx, "rollback-disk", 16<<20)
	mgr.getBranch = origGetBranch
	mgr.closeDisk = origClose
	mgr.removeAll = origRemoveAll
	require.Error(t, err)
	rollbackWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, rollbackWarns, 2)
	require.Equal(t, "create_disk", rollbackWarns[0]["op"])
	require.Equal(t, "create_disk", rollbackWarns[1]["op"])
	assertHasKeys(t, rollbackWarns[0], "disk_id", "error")
	assertHasKeys(t, rollbackWarns[1], "disk_id", "error")

	buf.Reset()
	mgr.closeDisk = func(ds *engine.DiskStore) error {
		_ = ds.Close()
		return errors.New("temp disk close failed")
	}
	cbErr := errors.New("callback failed")
	err = mgr.withTempDisk(disk.DiskID, func(*engine.DiskStore) error {
		return cbErr
	})
	mgr.closeDisk = origClose
	require.ErrorIs(t, err, cbErr)
	tempWarns := recordsByLevel(buf.Records(t), "WARN")
	require.Len(t, tempWarns, 1)
	require.Equal(t, "temp_disk_close", tempWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), tempWarns[0]["disk_id"])
	assertHasKeys(t, tempWarns[0], "error")

	closeRaceLogger, closeRaceBuf := newTestLogger(slog.LevelWarn)
	var (
		closeRaceMgr *ControlManager
		triggerClose atomic.Bool
	)
	closeRaceMgr, err = NewControlManager(ManagerOptions{
		StorageRoot: root,
		Store:       NewMemStore(),
		Logger:      closeRaceLogger,
		Now: func() time.Time {
			if triggerClose.Load() {
				closeRaceMgr.closed.Store(true)
			}
			return clk.Now()
		},
		ActorIdleTTL:            time.Hour,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	closeRaceMgr.closeDisk = func(ds *engine.DiskStore) error {
		_ = ds.Close()
		return errors.New("openEntrySlow rollback close failed")
	}
	triggerClose.Store(true)
	_, err = closeRaceMgr.openEntrySlow(closeRaceMgr.getSlot(disk.DiskID), disk.DiskID)
	triggerClose.Store(false)
	require.ErrorIs(t, err, engine.ErrClosed)
	openWarns := recordsByLevel(closeRaceBuf.Records(t), "WARN")
	require.Len(t, openWarns, 1)
	require.Equal(t, "open_disk", openWarns[0]["op"])
	require.Equal(t, disk.DiskID.String(), openWarns[0]["disk_id"])
	assertHasKeys(t, openWarns[0], "error")
	closeRaceMgr.closed.Store(false)
	closeRaceMgr.closeDisk = (*engine.DiskStore).Close
	require.NoError(t, closeRaceMgr.Close(ctx))

	require.NoError(t, mgr.Close(ctx))
	corruptRoot := t.TempDir()
	corruptLogger, corruptBuf := newTestLogger(slog.LevelWarn)
	corruptStore := NewMemStore()
	cMgr1, err := NewControlManager(ManagerOptions{
		StorageRoot:             corruptRoot,
		Store:                   corruptStore,
		Logger:                  corruptLogger,
		Now:                     clk.Now,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	corruptDisk, err := cMgr1.CreateDisk(ctx, "corrupt-mount-disk", 16<<20)
	require.NoError(t, err)
	require.NoError(t, cMgr1.Close(ctx))

	injectCorruptMountInPebble(t, corruptRoot, corruptDisk.DiskID, "main", "corrupt-mount-1", clk.Now().Add(time.Hour))

	cMgr2, err := NewControlManager(ManagerOptions{
		StorageRoot:             corruptRoot,
		Store:                   corruptStore,
		Logger:                  corruptLogger,
		Now:                     clk.Now,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cMgr2.Close(ctx)) })

	corruptBuf.Reset()
	cMgr2.queue.markDirty(corruptDisk.DiskID, false)
	mounts, err := cMgr2.ListMounts(ctx, corruptDisk.DiskID, "main")
	require.NoError(t, err)
	require.Empty(t, mounts)
	listMountWarns := recordsByLevel(corruptBuf.Records(t), "WARN")
	require.Len(t, listMountWarns, 1)
	require.Equal(t, "list_mounts", listMountWarns[0]["op"])
	require.Equal(t, "corrupt-mount-1", listMountWarns[0]["mount_id"])

	corruptBuf.Reset()
	require.NoError(t, cMgr2.reconcileDisk(ctx, corruptDisk.DiskID, false, false))
	snapWarns := recordsByLevel(corruptBuf.Records(t), "WARN")
	require.Len(t, snapWarns, 1)
	require.Equal(t, "control_snapshot", snapWarns[0]["op"])
	require.Equal(t, "corrupt-mount-1", snapWarns[0]["mount_id"])

	corruptBuf.Reset()
	_, err = cMgr2.EvictMounts(ctx, corruptDisk.DiskID, "main", uuid.Nil)
	require.NoError(t, err)
	evictMountWarns := recordsByLevel(corruptBuf.Records(t), "WARN")
	require.Len(t, evictMountWarns, 1)
	require.Equal(t, "evict_mounts", evictMountWarns[0]["op"])
	require.Equal(t, "corrupt-mount-1", evictMountWarns[0]["mount_id"])

	require.NoError(t, cMgr2.Close(ctx))
	injectCorruptMountInPebble(t, corruptRoot, corruptDisk.DiskID, "main", "corrupt-mount-expired", clk.Now().Add(-time.Second))

	cMgr3, err := NewControlManager(ManagerOptions{
		StorageRoot:             corruptRoot,
		Store:                   corruptStore,
		Logger:                  corruptLogger,
		Now:                     clk.Now,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cMgr3.Close(ctx)) })

	corruptBuf.Reset()
	_, err = cMgr3.MountBranch(ctx, corruptDisk.DiskID, "main", true, false)
	require.NoError(t, err)
	mountDelWarns := recordsByLevel(corruptBuf.Records(t), "WARN")
	require.Len(t, mountDelWarns, 1)
	require.Equal(t, "mount_branch", mountDelWarns[0]["op"])
	require.Equal(t, "corrupt-mount-expired", mountDelWarns[0]["mount_id"])
}

func gatherMetricValues(t *testing.T, collector prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(collector))
	fams, err := reg.Gather()
	require.NoError(t, err)
	out := make(map[string]*dto.MetricFamily, len(fams))
	for _, fam := range fams {
		out[fam.GetName()] = fam
	}
	return out
}

func requireProjectionMetricValue(t *testing.T, fams map[string]*dto.MetricFamily, name string, wantType dto.MetricType, want float64) {
	t.Helper()
	fam, ok := fams[name]
	require.True(t, ok, "missing metric family %s", name)
	require.Equal(t, wantType, fam.GetType(), "metric %s type mismatch", name)
	require.Len(t, fam.GetMetric(), 1, "metric %s must be flat unlabeled", name)
	m := fam.GetMetric()[0]
	require.Empty(t, m.GetLabel(), "metric %s must have zero labels", name)
	var got float64
	if wantType == dto.MetricType_GAUGE {
		got = m.GetGauge().GetValue()
	} else {
		got = m.GetCounter().GetValue()
	}
	require.InDelta(t, want, got, 1e-9, "metric %s value mismatch", name)
}

func TestSlice4_Step17_QueueDirtyCountsAndNilManagerEmitsAll11MetricsAtZero(t *testing.T) {
	ctx := context.Background()
	q := newQueue(NewMemStore(), queueConfig{flushInterval: time.Hour})
	t.Cleanup(func() { require.NoError(t, q.close(ctx)) })

	total, catalog := q.dirtyCounts()
	require.Zero(t, total)
	require.Zero(t, catalog)

	d1 := uuid.New()
	d2 := uuid.New()
	d3 := uuid.New()
	g1 := q.markDirty(d1, false)
	q.markDirty(d2, true)
	q.markDirty(d3, false)
	q.markDirty(d3, true)

	total, catalog = q.dirtyCounts()
	require.Equal(t, 3, total)
	require.Equal(t, 2, catalog)

	require.True(t, q.clearDirty(d1, g1))
	total, catalog = q.dirtyCounts()
	require.Equal(t, 2, total)
	require.Equal(t, 2, catalog)

	fams := gatherMetricValues(t, NewStorageMetricsCollector(nil))
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_dirty_disks", dto.MetricType_GAUGE, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_dirty_catalog_disks", dto.MetricType_GAUGE, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_sync_writes_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_sync_write_errors_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_sync_write_skips_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_read_fallbacks_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_reconcile_disks_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_reconcile_errors_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_batch_flush_errors_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_idle_evictions_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_disk_opens_total", dto.MetricType_COUNTER, 0)
}

func TestSlice4_Step17_All11ProjectionMetricsTrackTransitionsAndExclusions(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 26, 14, 0, 0, 0, time.UTC))
	logger, _ := newTestLogger(slog.LevelError)
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Logger:                  logger,
		Now:                     clk.Now,
		ActorIdleTTL:            time.Second,
		SyncWriteTimeout:        100 * time.Millisecond,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close(ctx)) })

	collector := NewStorageMetricsCollector(mgr)

	d1, err := mgr.CreateDisk(ctx, "m-disk-1", 16<<20)
	require.NoError(t, err)
	d2, err := mgr.CreateDisk(ctx, "m-disk-2", 16<<20)
	require.NoError(t, err)

	fams := gatherMetricValues(t, collector)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_sync_writes_total", dto.MetricType_COUNTER, 2)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_disk_opens_total", dto.MetricType_COUNTER, 2)

	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return ErrUnavailable
		},
	})
	_, err = mgr.Commit(ctx, d1.DiskID, "main", "fail-sync-1")
	require.NoError(t, err)
	_, err = mgr.Commit(ctx, d1.DiskID, "main", "skip-dirty-1")
	require.NoError(t, err)
	newName := "m-disk-2-dirty"
	_, err = mgr.UpdateDisk(ctx, d2.DiskID, &newName, nil)
	require.NoError(t, err)

	fams = gatherMetricValues(t, collector)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_dirty_disks", dto.MetricType_GAUGE, 2)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_dirty_catalog_disks", dto.MetricType_GAUGE, 1)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_sync_writes_total", dto.MetricType_COUNTER, 2)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_sync_write_errors_total", dto.MetricType_COUNTER, 2)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_sync_write_skips_total", dto.MetricType_COUNTER, 1)

	_, err = mgr.GetDisk(ctx, d1.DiskID)
	require.NoError(t, err)
	store.SetHooks(MemStoreHooks{
		BeforeRead: func(context.Context, string) error {
			return ErrUnavailable
		},
	})
	d3, err := mgr.CreateDisk(ctx, "m-disk-3", 16<<20)
	require.NoError(t, err)
	_, err = mgr.GetDisk(ctx, d3.DiskID)
	require.NoError(t, err)

	fams = gatherMetricValues(t, collector)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_read_fallbacks_total", dto.MetricType_COUNTER, 2)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_sync_writes_total", dto.MetricType_COUNTER, 3)

	store.SetHooks(MemStoreHooks{})
	require.NoError(t, mgr.reconcileDisk(ctx, d3.DiskID, true, false))
	fams = gatherMetricValues(t, collector)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_reconcile_disks_total", dto.MetricType_COUNTER, 0)

	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return ErrUnavailable
		},
	})
	require.ErrorIs(t, mgr.reconcileDisk(ctx, d1.DiskID, true, false), ErrUnavailable)
	store.SetHooks(MemStoreHooks{})
	require.NoError(t, mgr.reconcileDisk(ctx, d1.DiskID, true, false))
	require.NoError(t, mgr.reconcileDisk(ctx, d2.DiskID, true, false))

	fams = gatherMetricValues(t, collector)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_dirty_disks", dto.MetricType_GAUGE, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_dirty_catalog_disks", dto.MetricType_GAUGE, 0)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_reconcile_disks_total", dto.MetricType_COUNTER, 2)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_reconcile_errors_total", dto.MetricType_COUNTER, 1)

	store.SetHooks(MemStoreHooks{
		BeforeBatch: func(context.Context, string) error {
			return ErrUnavailable
		},
	})
	mgr.queue.recordUsage(d1.DiskID, 1024)
	mgr.queue.recordMountRenew(d1.DiskID, uuid.New(), clk.Now().Add(time.Minute))
	mgr.queue.flushBatches(ctx)
	store.SetHooks(MemStoreHooks{})

	fams = gatherMetricValues(t, collector)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_batch_flush_errors_total", dto.MetricType_COUNTER, 2)

	clk.Advance(5 * time.Second)
	evicted := mgr.evictIdleDisks(ctx)
	require.Equal(t, 3, evicted)
	fams = gatherMetricValues(t, collector)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_idle_evictions_total", dto.MetricType_COUNTER, 3)
	requireProjectionMetricValue(t, fams, "bowl_storage_projection_disk_opens_total", dto.MetricType_COUNTER, 3)

	nilMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   nil,
		Logger:                  logger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, nilMgr.Close(ctx)) })

	nilDisk, err := nilMgr.CreateDisk(ctx, "nil-metrics", 16<<20)
	require.NoError(t, err)
	require.ErrorIs(t, nilMgr.reconcileDisk(ctx, nilDisk.DiskID, false, false), ErrUnavailable)
	nilMgr.queue.recordUsage(nilDisk.DiskID, 128)
	nilMgr.queue.flushBatches(ctx)

	nilFams := gatherMetricValues(t, NewStorageMetricsCollector(nilMgr))
	requireProjectionMetricValue(t, nilFams, "bowl_storage_projection_sync_write_skips_total", dto.MetricType_COUNTER, 1)
	requireProjectionMetricValue(t, nilFams, "bowl_storage_projection_reconcile_errors_total", dto.MetricType_COUNTER, 0)
	requireProjectionMetricValue(t, nilFams, "bowl_storage_projection_batch_flush_errors_total", dto.MetricType_COUNTER, 0)

	shutStore := NewMemStore()
	shutEntered := make(chan struct{})
	var shutOnce sync.Once
	shutMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   shutStore,
		Logger:                  logger,
		FlushInterval:           time.Hour,
		ReconcileInterval:       10 * time.Millisecond,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	shutDisk, err := shutMgr.CreateDisk(ctx, "shut-metrics", 16<<20)
	require.NoError(t, err)
	shutStore.SetHooks(MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, op string, _ uuid.UUID) error {
			if op == "ReplaceDiskSnapshot" {
				shutOnce.Do(func() { close(shutEntered) })
				<-hookCtx.Done()
				return hookCtx.Err()
			}
			return nil
		},
	})
	shutMgr.queue.markDirty(shutDisk.DiskID, false)
	<-shutEntered
	require.NoError(t, shutMgr.Close(ctx))
	shutFams := gatherMetricValues(t, NewStorageMetricsCollector(shutMgr))
	requireProjectionMetricValue(t, shutFams, "bowl_storage_projection_reconcile_errors_total", dto.MetricType_COUNTER, 0)

	shutBatchStore := NewMemStore()
	shutBatchEntered := make(chan struct{})
	var shutBatchOnce sync.Once
	shutBatchStore.SetHooks(MemStoreHooks{
		BeforeBatch: func(hookCtx context.Context, op string) error {
			if op == "BatchUpdateDiskUsage" {
				shutBatchOnce.Do(func() { close(shutBatchEntered) })
				<-hookCtx.Done()
				return hookCtx.Err()
			}
			return nil
		},
	})
	shutBatchMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   shutBatchStore,
		Logger:                  logger,
		FlushInterval:           10 * time.Millisecond,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	shutBatchMgr.queue.recordUsage(uuid.New(), 512)
	<-shutBatchEntered
	closeCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, shutBatchMgr.Close(closeCtx), context.DeadlineExceeded)
	shutBatchFams := gatherMetricValues(t, NewStorageMetricsCollector(shutBatchMgr))
	requireProjectionMetricValue(t, shutBatchFams, "bowl_storage_projection_batch_flush_errors_total", dto.MetricType_COUNTER, 0)
}
