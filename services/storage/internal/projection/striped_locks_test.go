package projection

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

var slotSink *diskSlot

func findCollidingUUIDs(t *testing.T) (diskA, diskB uuid.UUID) {
	t.Helper()
	seen := make(map[uint64]uuid.UUID, diskSlotStripes)
	for i := range uint64(4096) {
		var id uuid.UUID
		binary.BigEndian.PutUint64(id[:8], 0x0190000000007000)
		binary.BigEndian.PutUint64(id[8:], 0x8000000000000000|i)
		stripe := diskSlotIndex(id)
		if prev, ok := seen[stripe]; ok {
			return prev, id
		}
		seen[stripe] = id
	}
	t.Fatalf("expected two distinct UUIDs to collide onto a shared stripe within 4096 probes, got %d unique stripes", len(seen))
	return uuid.Nil, uuid.Nil
}

func seedColdDisk(t *testing.T, root string, diskID uuid.UUID, name string) {
	t.Helper()
	ds, err := engine.OpenDisk(engine.DiskOptions{
		Dir:      filepath.Join(root, "disks", diskID.String()),
		DiskID:   diskID.String(),
		Name:     name,
		MaxBytes: 32 << 20,
	})
	require.NoError(t, err)
	require.NoError(t, ds.Close())
}

func TestStripedLocks_LayoutAndUUIDv7Distribution(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	require.Equal(t, uintptr(64), unsafe.Sizeof(diskSlot{}), "diskSlot must be padded to a 64-byte cache line")
	require.Len(t, mgr.slots[:], 1024, "ControlManager.slots must be a fixed 1024-stripe array")

	const sampleCount = 512
	seenStripes := make(map[uint64]struct{}, sampleCount)
	for range sampleCount {
		id, uErr := uuid.NewV7()
		require.NoError(t, uErr)
		stripe := diskSlotIndex(id)
		require.Less(t, stripe, uint64(diskSlotStripes))
		require.Same(t, &mgr.slots[stripe], mgr.getSlot(id))
		seenStripes[stripe] = struct{}{}
	}
	require.GreaterOrEqual(t, len(seenStripes), 300, "512 rapid UUIDv7 IDs should scatter across at least 300 stripes")
	require.Len(t, mgr.slots[:], 1024, "ControlManager.slots length must remain fixed at 1024 after lookups")

	var allocIDs [128]uuid.UUID
	for i := range allocIDs {
		id, uErr := uuid.NewV7()
		require.NoError(t, uErr)
		allocIDs[i] = id
	}
	idx := 0
	allocs := testing.AllocsPerRun(100, func() {
		slotSink = mgr.getSlot(allocIDs[idx])
		idx++
	})
	require.Zero(t, allocs, "getSlot must perform zero heap allocations")
}

func TestStripedLocks_CollidingDisksConcurrentSafety(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewMemStore()

	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	diskA, diskB := findCollidingUUIDs(t)
	require.NotEqual(t, diskA, diskB)
	require.Equal(t, diskSlotIndex(diskA), diskSlotIndex(diskB))
	require.Same(t, mgr.getSlot(diskA), mgr.getSlot(diskB))

	seedColdDisk(t, root, diskA, "collide-a")
	seedColdDisk(t, root, diskB, "collide-b")
	require.False(t, mgr.IsOpen(diskA))
	require.False(t, mgr.IsOpen(diskB))

	startCold := make(chan struct{})
	enteredA := make(chan struct{})
	enteredB := make(chan struct{})
	coldErrs := make(chan error, 2)

	var coldWG sync.WaitGroup
	coldWG.Add(2)
	go func() {
		defer coldWG.Done()
		<-startCold
		coldErrs <- mgr.WithDisk(ctx, diskA, func(ds *engine.DiskStore) error {
			close(enteredA)
			select {
			case <-enteredB:
			case <-time.After(3 * time.Second):
				return errors.New("timed out waiting for colliding diskB to enter WithDisk concurrently")
			}
			_, stErr := ds.StatFS(ctx)
			return stErr
		})
	}()
	go func() {
		defer coldWG.Done()
		<-startCold
		coldErrs <- mgr.WithDisk(ctx, diskB, func(ds *engine.DiskStore) error {
			close(enteredB)
			select {
			case <-enteredA:
			case <-time.After(3 * time.Second):
				return errors.New("timed out waiting for colliding diskA to enter WithDisk concurrently")
			}
			_, stErr := ds.StatFS(ctx)
			return stErr
		})
	}()

	close(startCold)
	coldWG.Wait()
	close(coldErrs)
	for cErr := range coldErrs {
		require.NoError(t, cErr)
	}
	require.True(t, mgr.IsOpen(diskA))
	require.True(t, mgr.IsOpen(diskB))

	require.NoError(t, mgr.ReconcileDisk(ctx, diskA))
	require.NoError(t, mgr.ReconcileDisk(ctx, diskB))

	const rounds = 6
	mutErrs := make(chan error, 4)
	var mutWG sync.WaitGroup
	mutWG.Add(4)

	go func() {
		defer mutWG.Done()
		for i := range rounds {
			name := fmt.Sprintf("collide-a-%d", i)
			if _, uErr := mgr.UpdateDisk(ctx, diskA, &name, nil); uErr != nil {
				mutErrs <- fmt.Errorf("update diskA round %d: %w", i, uErr)
				return
			}
			if _, cErr := mgr.Commit(ctx, diskA, "main", fmt.Sprintf("a-commit-%d", i)); cErr != nil {
				mutErrs <- fmt.Errorf("commit diskA round %d: %w", i, cErr)
				return
			}
		}
		mutErrs <- nil
	}()

	go func() {
		defer mutWG.Done()
		for i := range rounds * 2 {
			wErr := mgr.WithDisk(ctx, diskA, func(ds *engine.DiskStore) error {
				_, stErr := ds.StatFS(ctx)
				return stErr
			})
			if wErr != nil {
				mutErrs <- fmt.Errorf("withDisk diskA round %d: %w", i, wErr)
				return
			}
		}
		mutErrs <- nil
	}()

	go func() {
		defer mutWG.Done()
		for i := range rounds {
			if _, cErr := mgr.Commit(ctx, diskB, "main", fmt.Sprintf("b-commit-%d", i)); cErr != nil {
				mutErrs <- fmt.Errorf("commit diskB round %d: %w", i, cErr)
				return
			}
			name := fmt.Sprintf("collide-b-%d", i)
			if _, uErr := mgr.UpdateDisk(ctx, diskB, &name, nil); uErr != nil {
				mutErrs <- fmt.Errorf("update diskB round %d: %w", i, uErr)
				return
			}
		}
		mutErrs <- nil
	}()

	go func() {
		defer mutWG.Done()
		for i := range rounds * 2 {
			wErr := mgr.WithDisk(ctx, diskB, func(ds *engine.DiskStore) error {
				_, stErr := ds.StatFS(ctx)
				return stErr
			})
			if wErr != nil {
				mutErrs <- fmt.Errorf("withDisk diskB round %d: %w", i, wErr)
				return
			}
		}
		mutErrs <- nil
	}()

	mutWG.Wait()
	close(mutErrs)
	for mErr := range mutErrs {
		require.NoError(t, mErr)
	}

	recA, err := mgr.GetDisk(ctx, diskA)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("collide-a-%d", rounds-1), recA.Name)

	recB, err := mgr.GetDisk(ctx, diskB)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("collide-b-%d", rounds-1), recB.Name)

	commitsA, err := mgr.ListCommits(ctx, ListCommitsQuery{DiskID: diskA})
	require.NoError(t, err)
	require.Len(t, commitsA.Commits, 1+rounds)

	commitsB, err := mgr.ListCommits(ctx, ListCommitsQuery{DiskID: diskB})
	require.NoError(t, err)
	require.Len(t, commitsB.Commits, 1+rounds)

	delErrs := make(chan error, 2)
	var delWG sync.WaitGroup
	delWG.Add(2)
	go func() {
		defer delWG.Done()
		delErrs <- mgr.DeleteDisk(ctx, diskA)
	}()
	go func() {
		defer delWG.Done()
		delErrs <- mgr.DeleteDisk(ctx, diskB)
	}()
	delWG.Wait()
	close(delErrs)
	for dErr := range delErrs {
		require.NoError(t, dErr)
	}

	require.False(t, mgr.IsOpen(diskA))
	require.False(t, mgr.IsOpen(diskB))
	_, err = mgr.GetDisk(ctx, diskA)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = mgr.GetDisk(ctx, diskB)
	require.ErrorIs(t, err, ErrNotFound)
}
