package projection_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/projection"
)

func TestStep17_AsyncQueue_PerDiskFIFOOrderingBatchFlushDirtyMarkAndGracefulDrain(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemStore()

	var mu sync.Mutex
	var order []string
	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(_ context.Context, op string, _ uuid.UUID) error {
			mu.Lock()
			order = append(order, op)
			mu.Unlock()
			return nil
		},
	})

	q := projection.NewQueue(store, projection.QueueConfig{
		DiskQueueSize: 4,
		FlushInterval: 25 * time.Millisecond,
	})

	now := time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC)
	diskID := uuid.New()
	mountID := uuid.New()

	disk := projection.DiskRecord{
		DiskID:        diskID,
		Name:          "fifo-disk",
		DefaultBranch: "main",
		MaxBytes:      1024 * 1024,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	initBranch := projection.BranchRecord{
		DiskID:       diskID,
		Name:         "main",
		HeadCommitID: 1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	initCommit := projection.CommitRecord{
		DiskID:    diskID,
		CommitID:  1,
		Kind:      projection.CommitKindInit,
		Message:   "init",
		CreatedAt: now,
	}

	q.EnqueueUpsertDisk(disk, &initBranch, &initCommit)
	q.EnqueueUpsertBranchAndCommit(diskID,
		[]projection.BranchRecord{{DiskID: diskID, Name: "main", HeadCommitID: 2, CreatedAt: now, UpdatedAt: now.Add(time.Second)}},
		[]projection.CommitRecord{{DiskID: diskID, CommitID: 2, ParentID: 1, Kind: projection.CommitKindManual, Message: "c2", CreatedAt: now.Add(time.Second)}},
		nil,
	)
	q.EnqueueUpsertMount(projection.MountRecord{
		DiskID:      diskID,
		MountID:     mountID,
		BranchName:  "main",
		ReadOnly:    false,
		WriterEpoch: 1,
		MountedAt:   now,
		ExpiresAt:   now.Add(10 * time.Second),
	}, nil, nil)

	require.NoError(t, q.WaitDisk(ctx, diskID, time.Second))

	mu.Lock()
	require.Equal(t, []string{"UpsertDisk", "UpsertBranchAndCommit", "UpsertMount"}, order)
	mu.Unlock()

	q.RecordUsage(diskID, 1000)
	q.RecordUsage(diskID, 4096)
	q.RecordMountRenew(diskID, mountID, now.Add(30*time.Second))
	q.RecordMountRenew(diskID, mountID, now.Add(60*time.Second))

	require.Eventually(t, func() bool {
		got, err := store.GetDisk(ctx, diskID)
		if err != nil || got.UsedBytes != 4096 {
			return false
		}
		mounts, err := store.ListMounts(ctx, diskID, "main", now.Add(45*time.Second))
		return err == nil && len(mounts) == 1 && mounts[0].ExpiresAt.Equal(now.Add(60*time.Second))
	}, time.Second, 10*time.Millisecond)

	blockedDisk := uuid.New()
	gate := make(chan struct{})
	firstEntered := make(chan struct{})
	var enteredOnce sync.Once
	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(_ context.Context, _ string, id uuid.UUID) error {
			if id == blockedDisk {
				enteredOnce.Do(func() { close(firstEntered) })
				<-gate
			}
			return nil
		},
	})

	blockedRec := projection.DiskRecord{
		DiskID:        blockedDisk,
		Name:          "overflow",
		DefaultBranch: "main",
		MaxBytes:      1024,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	q.EnqueueUpsertDisk(blockedRec, nil, nil)
	<-firstEntered
	for i := range 6 {
		blockedRec.UsedBytes = uint64(i + 1)
		q.EnqueueUpdateDisk(blockedRec)
	}
	require.True(t, q.IsDirty(blockedDisk))
	dirtyMap := q.TakeDirtyDisks()
	require.Contains(t, dirtyMap, blockedDisk)
	gen1 := dirtyMap[blockedDisk]

	gen2 := q.MarkDirty(blockedDisk)
	require.Greater(t, gen2, gen1)
	require.False(t, q.ClearDirty(blockedDisk, gen1))
	require.True(t, q.IsDirty(blockedDisk))
	require.True(t, q.ClearDirty(blockedDisk, gen2))
	require.False(t, q.IsDirty(blockedDisk))

	close(gate)
	require.NoError(t, q.WaitDisk(ctx, blockedDisk, time.Second))

	errDisk := uuid.New()
	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(_ context.Context, _ string, id uuid.UUID) error {
			if id == errDisk {
				return projection.ErrUnavailable
			}
			return nil
		},
		BeforeBatch: func(_ context.Context, op string) error {
			if op == "BatchUpdateDiskUsage" {
				return projection.ErrUnavailable
			}
			return nil
		},
	})
	q.EnqueueUpsertDisk(projection.DiskRecord{
		DiskID:        errDisk,
		Name:          "fail-write",
		DefaultBranch: "main",
		MaxBytes:      1024,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil, nil)
	require.Eventually(t, func() bool { return q.IsDirty(errDisk) }, time.Second, 5*time.Millisecond)
	require.ErrorIs(t, q.WaitDisk(ctx, errDisk, time.Second), projection.ErrDiskDirty)

	batchFailDisk := uuid.New()
	q.RecordUsage(batchFailDisk, 777)
	require.Eventually(t, func() bool { return q.IsDirty(batchFailDisk) }, time.Second, 10*time.Millisecond)

	store.SetHooks(projection.MemStoreHooks{})
	drainDisk := uuid.New()
	q.EnqueueUpsertDisk(projection.DiskRecord{
		DiskID:        drainDisk,
		Name:          "drain-me",
		DefaultBranch: "main",
		MaxBytes:      2048,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil, nil)
	q.RecordUsage(drainDisk, 555)

	require.NoError(t, q.Close(ctx))
	require.NoError(t, q.Close(ctx))

	gotDrain, err := store.GetDisk(ctx, drainDisk)
	require.NoError(t, err)
	require.Equal(t, "drain-me", gotDrain.Name)
	require.Equal(t, uint64(555), gotDrain.UsedBytes)
}

func TestStep18_FlushBarrier_ReadAfterWriteWaitsAndColdDiskZeroWait(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemStore()

	coldDisk := uuid.New()
	q := projection.NewQueue(store, projection.QueueConfig{
		DiskQueueSize: 16,
		FlushInterval: time.Hour,
	})
	t.Cleanup(func() {
		require.NoError(t, q.Close(ctx))
	})

	start := time.Now()
	require.NoError(t, q.WaitDisk(ctx, coldDisk, 50*time.Millisecond))
	require.NoError(t, q.WaitCatalog(ctx, 50*time.Millisecond))
	require.Less(t, time.Since(start), 20*time.Millisecond)

	diskA := uuid.New()
	diskB := uuid.New()
	gateA := make(chan struct{})
	enteredA := make(chan struct{})
	var onceA sync.Once
	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(_ context.Context, _ string, id uuid.UUID) error {
			if id == diskA {
				onceA.Do(func() { close(enteredA) })
				<-gateA
			}
			return nil
		},
	})

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	q.EnqueueUpsertDisk(projection.DiskRecord{
		DiskID:        diskA,
		Name:          "disk-a",
		DefaultBranch: "main",
		MaxBytes:      1024,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil, nil)
	<-enteredA

	q.EnqueueUpsertDisk(projection.DiskRecord{
		DiskID:        diskB,
		Name:          "disk-b",
		DefaultBranch: "main",
		MaxBytes:      1024,
		CreatedAt:     now.Add(time.Second),
		UpdatedAt:     now.Add(time.Second),
	}, nil, nil)

	require.NoError(t, q.WaitDisk(ctx, diskB, time.Second))
	gotB, err := store.GetDisk(ctx, diskB)
	require.NoError(t, err)
	require.Equal(t, "disk-b", gotB.Name)

	require.ErrorIs(t, q.WaitDisk(ctx, diskA, 25*time.Millisecond), projection.ErrBarrierTimeout)
	require.ErrorIs(t, q.WaitCatalog(ctx, 25*time.Millisecond), projection.ErrBarrierTimeout)

	waitDiskDone := make(chan error, 1)
	waitCatalogDone := make(chan error, 1)
	go func() {
		waitDiskDone <- q.WaitDisk(ctx, diskA, time.Second)
	}()
	go func() {
		waitCatalogDone <- q.WaitCatalog(ctx, time.Second)
	}()

	close(gateA)

	require.NoError(t, <-waitDiskDone)
	require.NoError(t, <-waitCatalogDone)

	gotA, err := store.GetDisk(ctx, diskA)
	require.NoError(t, err)
	require.Equal(t, "disk-a", gotA.Name)

	q.EnqueueDeleteDisk(diskB)
	require.NoError(t, q.WaitCatalog(ctx, time.Second))
	_, err = store.GetDisk(ctx, diskB)
	require.ErrorIs(t, err, projection.ErrNotFound)
}

func TestQueue_WaitDiskWakesOnDirtyAndCloseCancel(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemStore()
	q := projection.NewQueue(store, projection.QueueConfig{
		DiskQueueSize: 4,
		FlushInterval: time.Hour,
	})

	now := time.Date(2026, 11, 24, 13, 0, 0, 0, time.UTC)
	diskID := uuid.New()
	gate := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, _ string, id uuid.UUID) error {
			if id == diskID {
				once.Do(func() { close(entered) })
				select {
				case <-gate:
					return projection.ErrUnavailable
				case <-hookCtx.Done():
					return hookCtx.Err()
				}
			}
			return nil
		},
	})

	q.EnqueueUpsertDisk(projection.DiskRecord{
		DiskID:        diskID,
		Name:          "dirty-wake",
		DefaultBranch: "main",
		MaxBytes:      1024,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil, nil)
	<-entered

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- q.WaitDisk(ctx, diskID, time.Second)
	}()

	close(gate)
	require.ErrorIs(t, <-waitErr, projection.ErrDiskDirty)

	stuckDisk := uuid.New()
	stuckEntered := make(chan struct{})
	var stuckOnce sync.Once
	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, _ string, id uuid.UUID) error {
			if id == stuckDisk {
				stuckOnce.Do(func() { close(stuckEntered) })
				<-hookCtx.Done()
				return hookCtx.Err()
			}
			return nil
		},
		BeforeBatch: func(_ context.Context, op string) error {
			if op == "BatchRenewMounts" {
				return projection.ErrUnavailable
			}
			return nil
		},
	})

	renewFailDisk := uuid.New()
	q.RecordMountRenew(renewFailDisk, uuid.New(), now.Add(time.Minute))
	q.FlushBatches(ctx)
	require.True(t, q.IsDirty(renewFailDisk))
	require.True(t, q.HasDirtyDisks())

	q.EnqueueUpsertDisk(projection.DiskRecord{
		DiskID:        stuckDisk,
		Name:          "stuck",
		DefaultBranch: "main",
		MaxBytes:      1024,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil, nil)
	<-stuckEntered

	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, q.Close(closeCtx), context.DeadlineExceeded)
}

func TestQueue_DeleteOpsEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemStore()
	q := projection.NewQueue(store, projection.QueueConfig{
		DiskQueueSize: 8,
		FlushInterval: time.Hour,
	})
	t.Cleanup(func() {
		require.NoError(t, q.Close(ctx))
	})

	now := time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC)
	diskID := uuid.New()
	mountID := uuid.New()
	q.EnqueueUpsertDisk(
		projection.DiskRecord{DiskID: diskID, Name: "d", DefaultBranch: "main", MaxBytes: 1024, CreatedAt: now, UpdatedAt: now},
		&projection.BranchRecord{DiskID: diskID, Name: "main", HeadCommitID: 1, CreatedAt: now, UpdatedAt: now},
		&projection.CommitRecord{DiskID: diskID, CommitID: 1, Kind: projection.CommitKindInit, CreatedAt: now},
	)
	q.EnqueueUpsertBranchAndCommit(diskID,
		[]projection.BranchRecord{{DiskID: diskID, Name: "dev", HeadCommitID: 2, CreatedAt: now, UpdatedAt: now}},
		[]projection.CommitRecord{{DiskID: diskID, CommitID: 2, ParentID: 1, Kind: projection.CommitKindFork, CreatedAt: now}},
		nil,
	)
	q.EnqueueUpsertMount(projection.MountRecord{
		DiskID:     diskID,
		MountID:    mountID,
		BranchName: "dev",
		MountedAt:  now,
		ExpiresAt:  now.Add(time.Hour),
	}, nil, nil)
	q.EnqueueDeleteMounts(diskID, []uuid.UUID{mountID}, nil)
	q.EnqueueDeleteBranch(diskID, "dev")
	q.EnqueueDeleteCommits(diskID, []uint64{2})

	require.NoError(t, q.WaitDisk(ctx, diskID, time.Second))

	branches, err := store.ListBranches(ctx, diskID)
	require.NoError(t, err)
	require.Len(t, branches, 1)
	require.Equal(t, "main", branches[0].Name)

	commits, err := store.ListCommits(ctx, projection.ListCommitsQuery{DiskID: diskID})
	require.NoError(t, err)
	require.Len(t, commits.Commits, 1)
	require.Equal(t, uint64(1), commits.Commits[0].CommitID)

	mounts, err := store.ListMounts(ctx, diskID, "", now)
	require.NoError(t, err)
	require.Empty(t, mounts)
}
