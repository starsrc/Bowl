package projection

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestQueue_DirtyTrackingCatalogUpgradeGenerationAndClear(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	q := newQueue(store, queueConfig{
		flushInterval: time.Hour,
	})
	t.Cleanup(func() {
		require.NoError(t, q.close(ctx))
	})

	require.False(t, q.hasDirtyDisks())
	require.Empty(t, q.takeDirtyDisks())
	require.Empty(t, q.takeDirtyCatalogDisks())

	require.Zero(t, q.markDirty(uuid.Nil, true))
	require.False(t, q.isDirty(uuid.Nil))
	_, ok := q.dirtyGeneration(uuid.Nil)
	require.False(t, ok)

	diskA := uuid.New()
	diskB := uuid.New()

	genA1 := q.markDirty(diskA, false)
	require.NotZero(t, genA1)
	require.True(t, q.isDirty(diskA))
	require.True(t, q.hasDirtyDisks())
	gotGen, ok := q.dirtyGeneration(diskA)
	require.True(t, ok)
	require.Equal(t, genA1, gotGen)
	require.Contains(t, q.takeDirtyDisks(), diskA)
	require.Empty(t, q.takeDirtyCatalogDisks())

	genA2 := q.markDirty(diskA, true)
	require.Greater(t, genA2, genA1)
	require.Equal(t, map[uuid.UUID]uint64{diskA: genA2}, q.takeDirtyCatalogDisks())

	genA3 := q.markDirty(diskA, false)
	require.Greater(t, genA3, genA2)
	require.Equal(t, map[uuid.UUID]uint64{diskA: genA3}, q.takeDirtyCatalogDisks())

	genB1 := q.markDirty(diskB, false)
	require.Greater(t, genB1, genA3)
	require.Equal(t, map[uuid.UUID]uint64{diskA: genA3, diskB: genB1}, q.takeDirtyDisks())
	require.Equal(t, map[uuid.UUID]uint64{diskA: genA3}, q.takeDirtyCatalogDisks())

	require.False(t, q.clearDirty(diskA, genA2))
	require.True(t, q.isDirty(diskA))
	require.True(t, q.clearDirty(diskA, genA3))
	require.False(t, q.isDirty(diskA))
	require.Empty(t, q.takeDirtyCatalogDisks())

	require.True(t, q.clearDirty(diskB, genB1))
	require.False(t, q.hasDirtyDisks())
}

func TestQueue_BatchUsageAndMountRenewFlushFailureDirtyMarkAndGracefulDrain(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	now := time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC)
	diskID := uuid.New()
	mountID := uuid.New()

	require.NoError(t, store.UpsertDisk(ctx,
		DiskRecord{
			DiskID:        diskID,
			Name:          "batch-disk",
			DefaultBranch: "main",
			MaxBytes:      1024 * 1024,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		&BranchRecord{
			DiskID:       diskID,
			Name:         "main",
			HeadCommitID: 1,
			CreatedAt:    now,
			UpdatedAt:    now,
		},
		&CommitRecord{
			DiskID:    diskID,
			CommitID:  1,
			Kind:      CommitKindInit,
			Message:   "init",
			CreatedAt: now,
		},
	))
	require.NoError(t, store.UpsertMount(ctx, MountRecord{
		DiskID:      diskID,
		MountID:     mountID,
		BranchName:  "main",
		ReadOnly:    false,
		WriterEpoch: 1,
		MountedAt:   now,
		ExpiresAt:   now.Add(10 * time.Second),
	}, nil, nil))

	q := newQueue(store, queueConfig{
		flushInterval: 25 * time.Millisecond,
	})

	q.recordUsage(diskID, 1000)
	q.recordUsage(diskID, 4096)
	q.recordMountRenew(diskID, mountID, now.Add(60*time.Second))
	q.recordMountRenew(diskID, mountID, now.Add(30*time.Second))

	require.Eventually(t, func() bool {
		got, err := store.GetDisk(ctx, diskID)
		if err != nil || got.UsedBytes != 4096 {
			return false
		}
		mounts, err := store.ListMounts(ctx, diskID, "main", now.Add(45*time.Second))
		return err == nil && len(mounts) == 1 && mounts[0].ExpiresAt.Equal(now.Add(60*time.Second))
	}, time.Second, 10*time.Millisecond)

	store.SetHooks(MemStoreHooks{
		BeforeBatch: func(_ context.Context, op string) error {
			if op == "BatchUpdateDiskUsage" || op == "BatchRenewMounts" {
				return ErrUnavailable
			}
			return nil
		},
	})

	usageFailDisk := uuid.New()
	renewFailDisk := uuid.New()
	q.recordUsage(usageFailDisk, 777)
	q.recordMountRenew(renewFailDisk, uuid.New(), now.Add(time.Minute))
	q.flushBatches(ctx)

	require.True(t, q.isDirty(usageFailDisk))
	require.True(t, q.isDirty(renewFailDisk))
	require.Empty(t, q.takeDirtyCatalogDisks())

	store.SetHooks(MemStoreHooks{})
	drainDisk := uuid.New()
	require.NoError(t, store.UpsertDisk(ctx, DiskRecord{
		DiskID:        drainDisk,
		Name:          "drain-me",
		DefaultBranch: "main",
		MaxBytes:      2048,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil, nil))
	q.recordUsage(drainDisk, 555)

	require.NoError(t, q.close(ctx))
	require.NoError(t, q.close(ctx))

	gotDrain, err := store.GetDisk(ctx, drainDisk)
	require.NoError(t, err)
	require.Equal(t, uint64(555), gotDrain.UsedBytes)
}

func TestQueue_CloseCancelsBlockedBatcherOnTimeout(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	entered := make(chan struct{})
	var once sync.Once
	store.SetHooks(MemStoreHooks{
		BeforeBatch: func(hookCtx context.Context, _ string) error {
			once.Do(func() { close(entered) })
			<-hookCtx.Done()
			return hookCtx.Err()
		},
	})

	q := newQueue(store, queueConfig{
		flushInterval: 10 * time.Millisecond,
	})
	q.recordUsage(uuid.New(), 128)

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batcher flush to start")
	}

	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, q.close(closeCtx), context.DeadlineExceeded)
}
