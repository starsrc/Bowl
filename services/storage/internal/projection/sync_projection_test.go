package projection

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSyncProjection_DeleteDiskLifecycleNoGoroutineLeak(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewMemStore()

	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	warmup, err := mgr.CreateDisk(ctx, "warmup", 16<<20)
	require.NoError(t, err)
	require.NoError(t, mgr.DeleteDisk(ctx, warmup.DiskID))
	runtime.GC()
	time.Sleep(20 * time.Millisecond)

	base := runtime.NumGoroutine()

	const cycles = 20
	for i := range cycles {
		d, cErr := mgr.CreateDisk(ctx, fmt.Sprintf("ephemeral-%d", i), 16<<20)
		require.NoError(t, cErr)
		require.NoError(t, mgr.DeleteDisk(ctx, d.DiskID))
	}

	require.Eventually(t, func() bool {
		runtime.GC()
		return runtime.NumGoroutine()-base <= 2
	}, 500*time.Millisecond, 20*time.Millisecond, "expected no goroutine leak after %d create/delete cycles (base=%d, current=%d)", cycles, base, runtime.NumGoroutine())

	page, err := mgr.ListDisks(ctx, ListDisksQuery{})
	require.NoError(t, err)
	require.Empty(t, page.Disks)
}

func TestSyncProjection_DirtyDiskSkipsSubsequentPGWrites(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewMemStore()

	const syncTimeout = 80 * time.Millisecond
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		SyncWriteTimeout:        syncTimeout,
		FlushInterval:           time.Hour,
		ReconcileInterval:       40 * time.Millisecond,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "dirty-burst", 32<<20)
	require.NoError(t, err)

	var blocked atomic.Bool
	blocked.Store(true)
	var upsertAttempts atomic.Int64
	unblock := make(chan struct{})

	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, op string, diskID uuid.UUID) error {
			if diskID != disk.DiskID || !blocked.Load() {
				return nil
			}
			if op == "ReplaceDiskSnapshot" {
				return ErrUnavailable
			}
			upsertAttempts.Add(1)
			select {
			case <-unblock:
				return nil
			case <-hookCtx.Done():
				return hookCtx.Err()
			}
		},
	})

	startFirst := time.Now()
	c1, err := mgr.Commit(ctx, disk.DiskID, "main", "first blocked commit")
	elapsedFirst := time.Since(startFirst)
	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsedFirst, syncTimeout/2)
	require.True(t, mgr.queue.isDirty(disk.DiskID))
	require.Equal(t, int64(1), upsertAttempts.Load())

	startBurst := time.Now()
	const burstCount = 5
	var lastCommitID uint64
	for i := range burstCount {
		cmt, cErr := mgr.Commit(ctx, disk.DiskID, "main", fmt.Sprintf("burst-%d", i))
		require.NoError(t, cErr)
		lastCommitID = cmt.CommitID
	}
	elapsedBurst := time.Since(startBurst)
	require.Less(t, elapsedBurst, syncTimeout)
	require.Equal(t, int64(1), upsertAttempts.Load())

	commitsPage, err := mgr.ListCommits(ctx, ListCommitsQuery{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Len(t, commitsPage.Commits, 1+1+burstCount)
	require.Equal(t, lastCommitID, commitsPage.Commits[0].CommitID)
	require.Greater(t, lastCommitID, c1.CommitID)

	blocked.Store(false)
	close(unblock)

	require.Eventually(t, func() bool {
		if mgr.queue.isDirty(disk.DiskID) {
			return false
		}
		pgCommits, lErr := store.ListCommits(ctx, ListCommitsQuery{DiskID: disk.DiskID})
		return lErr == nil && len(pgCommits.Commits) == 1+1+burstCount
	}, 3*time.Second, 15*time.Millisecond)
}

func TestSyncProjection_ListDisksInlineReconcilesDirtyCatalog(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewMemStore()

	const syncTimeout = 100 * time.Millisecond
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		SyncWriteTimeout:        syncTimeout,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return ErrUnavailable
		},
	})

	d1, err := mgr.CreateDisk(ctx, "dirty-create", 16<<20)
	require.NoError(t, err)
	require.True(t, mgr.queue.isDirty(d1.DiskID))

	rawPage, err := store.ListDisks(ctx, ListDisksQuery{})
	require.NoError(t, err)
	require.Empty(t, rawPage.Disks)

	_, err = mgr.ListDisks(ctx, ListDisksQuery{})
	require.ErrorIs(t, err, ErrUnavailable)

	store.SetHooks(MemStoreHooks{})

	page, err := mgr.ListDisks(ctx, ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, page.Disks, 1)
	require.Equal(t, d1.DiskID, page.Disks[0].DiskID)
	require.False(t, mgr.queue.isDirty(d1.DiskID))

	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return ErrUnavailable
		},
	})
	require.NoError(t, mgr.DeleteDisk(ctx, d1.DiskID))
	require.True(t, mgr.queue.isDirty(d1.DiskID))

	rawBeforeDelReconcile, err := store.ListDisks(ctx, ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, rawBeforeDelReconcile.Disks, 1)

	store.SetHooks(MemStoreHooks{})

	pageAfterDel, err := mgr.ListDisks(ctx, ListDisksQuery{})
	require.NoError(t, err)
	require.Empty(t, pageAfterDel.Disks)
	require.False(t, mgr.queue.isDirty(d1.DiskID))

	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, _ string, _ uuid.UUID) error {
			<-hookCtx.Done()
			return hookCtx.Err()
		},
	})
	d2, err := mgr.CreateDisk(ctx, "blocked-catalog", 16<<20)
	require.NoError(t, err)
	require.True(t, mgr.queue.isDirty(d2.DiskID))

	_, err = mgr.ListDisks(ctx, ListDisksQuery{})
	require.ErrorIs(t, err, ErrUnavailable)
	require.NotErrorIs(t, err, context.DeadlineExceeded)

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = mgr.ListDisks(cancelCtx, ListDisksQuery{})
	require.ErrorIs(t, err, context.Canceled)

	store.SetHooks(MemStoreHooks{})
	recoveredPage, err := mgr.ListDisks(ctx, ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, recoveredPage.Disks, 1)
	require.Equal(t, d2.DiskID, recoveredPage.Disks[0].DiskID)

	var reconcileCalls atomic.Int64
	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(_ context.Context, op string, _ uuid.UUID) error {
			if op == "ReplaceDiskSnapshot" {
				reconcileCalls.Add(1)
			}
			return ErrUnavailable
		},
	})
	_, err = mgr.Commit(ctx, d2.DiskID, "main", "non-catalog dirty")
	require.NoError(t, err)
	require.True(t, mgr.queue.isDirty(d2.DiskID))

	listWithDirtyCommit, err := mgr.ListDisks(ctx, ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, listWithDirtyCommit.Disks, 1)
	require.Equal(t, d2.DiskID, listWithDirtyCommit.Disks[0].DiskID)
	require.Zero(t, reconcileCalls.Load())
	require.True(t, mgr.queue.isDirty(d2.DiskID))
}

func TestSyncProjection_CallerCancelAfterPebbleCommitAbortsPGWriteAndMarksDirty(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewMemStore()

	const syncTimeout = 600 * time.Millisecond
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		SyncWriteTimeout:        syncTimeout,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "cancel-mid-pg", 16<<20)
	require.NoError(t, err)

	entered := make(chan struct{})
	var enterOnce sync.Once
	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, _ string, diskID uuid.UUID) error {
			if diskID == disk.DiskID {
				enterOnce.Do(func() { close(entered) })
				<-hookCtx.Done()
				return hookCtx.Err()
			}
			return nil
		},
	})

	callCtx, cancel := context.WithCancel(ctx)
	type commitResult struct {
		cmt CommitRecord
		err error
	}
	done := make(chan commitResult, 1)
	go func() {
		cmt, cErr := mgr.Commit(callCtx, disk.DiskID, "main", "cancel during pg write")
		done <- commitResult{cmt: cmt, err: cErr}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sync PG write to start")
	}

	cancelAt := time.Now()
	cancel()

	res := <-done
	abortElapsed := time.Since(cancelAt)
	require.NoError(t, res.err)
	require.Greater(t, res.cmt.CommitID, uint64(1))
	require.Less(t, abortElapsed, 200*time.Millisecond)
	require.True(t, mgr.queue.isDirty(disk.DiskID))

	afterStart := time.Now()
	c2, err := mgr.Commit(ctx, disk.DiskID, "main", "post-cancel commit")
	require.NoError(t, err)
	require.Less(t, time.Since(afterStart), 100*time.Millisecond)
	require.Greater(t, c2.CommitID, res.cmt.CommitID)

	commitsPage, err := mgr.ListCommits(ctx, ListCommitsQuery{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Len(t, commitsPage.Commits, 3)
	require.Equal(t, c2.CommitID, commitsPage.Commits[0].CommitID)
	require.Equal(t, res.cmt.CommitID, commitsPage.Commits[1].CommitID)
}
