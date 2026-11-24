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

func TestQueue_DirtyTrackingCatalogUpgradeGenerationAndClear(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemStore()
	q := projection.NewQueue(store, projection.QueueConfig{
		FlushInterval: time.Hour,
	})
	t.Cleanup(func() {
		require.NoError(t, q.Close(ctx))
	})

	require.False(t, q.HasDirtyDisks())
	require.Empty(t, q.TakeDirtyDisks())
	require.Empty(t, q.TakeDirtyCatalogDisks())

	require.Zero(t, q.MarkDirty(uuid.Nil, true))
	require.False(t, q.IsDirty(uuid.Nil))
	_, ok := q.DirtyGeneration(uuid.Nil)
	require.False(t, ok)

	diskA := uuid.New()
	diskB := uuid.New()

	genA1 := q.MarkDirty(diskA, false)
	require.NotZero(t, genA1)
	require.True(t, q.IsDirty(diskA))
	require.True(t, q.HasDirtyDisks())
	gotGen, ok := q.DirtyGeneration(diskA)
	require.True(t, ok)
	require.Equal(t, genA1, gotGen)
	require.Contains(t, q.TakeDirtyDisks(), diskA)
	require.Empty(t, q.TakeDirtyCatalogDisks())

	genA2 := q.MarkDirty(diskA, true)
	require.Greater(t, genA2, genA1)
	require.Equal(t, map[uuid.UUID]uint64{diskA: genA2}, q.TakeDirtyCatalogDisks())

	genA3 := q.MarkDirty(diskA, false)
	require.Greater(t, genA3, genA2)
	require.Equal(t, map[uuid.UUID]uint64{diskA: genA3}, q.TakeDirtyCatalogDisks())

	genB1 := q.MarkDirty(diskB, false)
	require.Greater(t, genB1, genA3)
	require.Equal(t, map[uuid.UUID]uint64{diskA: genA3, diskB: genB1}, q.TakeDirtyDisks())
	require.Equal(t, map[uuid.UUID]uint64{diskA: genA3}, q.TakeDirtyCatalogDisks())

	require.False(t, q.ClearDirty(diskA, genA2))
	require.True(t, q.IsDirty(diskA))
	require.True(t, q.ClearDirty(diskA, genA3))
	require.False(t, q.IsDirty(diskA))
	require.Empty(t, q.TakeDirtyCatalogDisks())

	require.True(t, q.ClearDirty(diskB, genB1))
	require.False(t, q.HasDirtyDisks())
}

func TestQueue_BatchUsageAndMountRenewFlushFailureDirtyMarkAndGracefulDrain(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemStore()

	now := time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC)
	diskID := uuid.New()
	mountID := uuid.New()

	require.NoError(t, store.UpsertDisk(ctx,
		projection.DiskRecord{
			DiskID:        diskID,
			Name:          "batch-disk",
			DefaultBranch: "main",
			MaxBytes:      1024 * 1024,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		&projection.BranchRecord{
			DiskID:       diskID,
			Name:         "main",
			HeadCommitID: 1,
			CreatedAt:    now,
			UpdatedAt:    now,
		},
		&projection.CommitRecord{
			DiskID:    diskID,
			CommitID:  1,
			Kind:      projection.CommitKindInit,
			Message:   "init",
			CreatedAt: now,
		},
	))
	require.NoError(t, store.UpsertMount(ctx, projection.MountRecord{
		DiskID:      diskID,
		MountID:     mountID,
		BranchName:  "main",
		ReadOnly:    false,
		WriterEpoch: 1,
		MountedAt:   now,
		ExpiresAt:   now.Add(10 * time.Second),
	}, nil, nil))

	q := projection.NewQueue(store, projection.QueueConfig{
		FlushInterval: 25 * time.Millisecond,
	})

	q.RecordUsage(diskID, 1000)
	q.RecordUsage(diskID, 4096)
	q.RecordMountRenew(diskID, mountID, now.Add(60*time.Second))
	q.RecordMountRenew(diskID, mountID, now.Add(30*time.Second))

	require.Eventually(t, func() bool {
		got, err := store.GetDisk(ctx, diskID)
		if err != nil || got.UsedBytes != 4096 {
			return false
		}
		mounts, err := store.ListMounts(ctx, diskID, "main", now.Add(45*time.Second))
		return err == nil && len(mounts) == 1 && mounts[0].ExpiresAt.Equal(now.Add(60*time.Second))
	}, time.Second, 10*time.Millisecond)

	store.SetHooks(projection.MemStoreHooks{
		BeforeBatch: func(_ context.Context, op string) error {
			if op == "BatchUpdateDiskUsage" || op == "BatchRenewMounts" {
				return projection.ErrUnavailable
			}
			return nil
		},
	})

	usageFailDisk := uuid.New()
	renewFailDisk := uuid.New()
	q.RecordUsage(usageFailDisk, 777)
	q.RecordMountRenew(renewFailDisk, uuid.New(), now.Add(time.Minute))
	q.FlushBatches(ctx)

	require.True(t, q.IsDirty(usageFailDisk))
	require.True(t, q.IsDirty(renewFailDisk))
	require.Empty(t, q.TakeDirtyCatalogDisks())

	store.SetHooks(projection.MemStoreHooks{})
	drainDisk := uuid.New()
	require.NoError(t, store.UpsertDisk(ctx, projection.DiskRecord{
		DiskID:        drainDisk,
		Name:          "drain-me",
		DefaultBranch: "main",
		MaxBytes:      2048,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil, nil))
	q.RecordUsage(drainDisk, 555)

	require.NoError(t, q.Close(ctx))
	require.NoError(t, q.Close(ctx))

	gotDrain, err := store.GetDisk(ctx, drainDisk)
	require.NoError(t, err)
	require.Equal(t, uint64(555), gotDrain.UsedBytes)
}

func TestQueue_CloseCancelsBlockedBatcherOnTimeout(t *testing.T) {
	ctx := context.Background()
	store := projection.NewMemStore()

	entered := make(chan struct{})
	var once sync.Once
	store.SetHooks(projection.MemStoreHooks{
		BeforeBatch: func(hookCtx context.Context, _ string) error {
			once.Do(func() { close(entered) })
			<-hookCtx.Done()
			return hookCtx.Err()
		},
	})

	q := projection.NewQueue(store, projection.QueueConfig{
		FlushInterval: 10 * time.Millisecond,
	})
	q.RecordUsage(uuid.New(), 128)

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batcher flush to start")
	}

	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, q.Close(closeCtx), context.DeadlineExceeded)
}
