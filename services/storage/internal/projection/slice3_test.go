package projection

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func tombstoneRootInodeInPebble(t *testing.T, storageRoot string, diskID uuid.UUID, branchName string) {
	t.Helper()
	cmp := *pebble.DefaultComparer
	cmp.Name = "bowl.engine.comparer"
	cmp.Split = engine.SplitKey

	pebbleDir := filepath.Join(storageRoot, "disks", diskID.String(), "pebble")
	db, err := pebble.Open(pebbleDir, &pebble.Options{Comparer: &cmp})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	val, closer, err := db.Get(engine.MetaBranchKey(branchName))
	require.NoError(t, err)
	br, decErr := engine.DecodeBranchMeta(val)
	require.NoError(t, closer.Close())
	require.NoError(t, decErr)

	require.NoError(t, db.Set(engine.LayerInodeKey(br.WorkLayerID, engine.RootIno), engine.EncodeTombstone(), pebble.Sync))
}

func TestMountBranch_Step12_MissingRootInodeFailsAtomicallyPreservingActiveMountAndCleanQueue(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewMemStore()
	clk := newFakeClock(time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC))

	mgr1, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)

	disk, err := mgr1.CreateDisk(ctx, "step12-atomic", 16<<20)
	require.NoError(t, err)

	rw1, err := mgr1.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	require.NoError(t, err)
	require.Equal(t, engine.RootIno, rw1.RootInode.Ino)
	require.False(t, mgr1.queue.isDirty(disk.DiskID))

	require.NoError(t, mgr1.Close(ctx))

	tombstoneRootInodeInPebble(t, root, disk.DiskID, engine.DefaultBranchName)

	mgr2, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr2.Close(ctx)) }()

	_, err = mgr2.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, true, false)
	require.ErrorIs(t, err, engine.ErrNotFound)
	require.False(t, mgr2.queue.isDirty(disk.DiskID))

	_, err = mgr2.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, true)
	require.ErrorIs(t, err, engine.ErrNotFound)
	require.False(t, mgr2.queue.isDirty(disk.DiskID))

	storeMounts, err := store.ListMounts(ctx, disk.DiskID, engine.DefaultBranchName, clk.Now())
	require.NoError(t, err)
	require.Len(t, storeMounts, 1)
	require.Equal(t, rw1.Mount.MountID, storeMounts[0].MountID)

	storeBranches, err := store.ListBranches(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Len(t, storeBranches, 1)
	require.Equal(t, rw1.Mount.WriterEpoch, storeBranches[0].WriterEpoch)

	var (
		pebbleMounts []engine.MountMeta
		pebbleBranch engine.BranchMeta
	)
	require.NoError(t, mgr2.WithDisk(ctx, disk.DiskID, func(ds *engine.DiskStore) error {
		var lErr error
		pebbleMounts, lErr = ds.ListMounts(engine.DefaultBranchName, clk.Now())
		if lErr != nil {
			return lErr
		}
		pebbleBranch, lErr = ds.GetBranch(engine.DefaultBranchName)
		return lErr
	}))
	require.Len(t, pebbleMounts, 1)
	require.Equal(t, rw1.Mount.MountID.String(), pebbleMounts[0].ID)
	require.Equal(t, rw1.Mount.WriterEpoch, pebbleBranch.WriterEpoch)
}

type upsertMountCall struct {
	mount           MountRecord
	updatedBranch   *BranchRecord
	deletedMountIDs []uuid.UUID
}

type recordingMountStore struct {
	*MemStore
	mu    sync.Mutex
	calls []upsertMountCall
}

func (s *recordingMountStore) UpsertMount(ctx context.Context, mount MountRecord, updatedBranch *BranchRecord, deletedMountIDs []uuid.UUID) error {
	var branchCopy *BranchRecord
	if updatedBranch != nil {
		cp := *updatedBranch
		branchCopy = &cp
	}
	deletedCopy := append([]uuid.UUID(nil), deletedMountIDs...)
	s.mu.Lock()
	s.calls = append(s.calls, upsertMountCall{
		mount:           mount,
		updatedBranch:   branchCopy,
		deletedMountIDs: deletedCopy,
	})
	s.mu.Unlock()
	return s.MemStore.UpsertMount(ctx, mount, updatedBranch, deletedMountIDs)
}

func (s *recordingMountStore) takeCalls() []upsertMountCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]upsertMountCall(nil), s.calls...)
	s.calls = nil
	return out
}

func TestMountBranch_Step13_SyncsDeletedExpiredAndEvictedMountsAndBranchWithoutGetBranch(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 26, 11, 0, 0, 0, time.UTC)
	clk := newFakeClock(t0)
	store := &recordingMountStore{MemStore: NewMemStore()}

	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Now:                     clk.Now,
		MountLeaseTTL:           10 * time.Second,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "step13-sync", 16<<20)
	require.NoError(t, err)

	origGetBranch := mgr.getBranch
	mgr.getBranch = func(*engine.DiskStore, string) (engine.BranchMeta, error) {
		return engine.BranchMeta{}, errors.New("getBranch must not be called by MountBranch")
	}
	defer func() { mgr.getBranch = origGetBranch }()

	ro1, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, true, false)
	require.NoError(t, err)
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	calls := store.takeCalls()
	require.Len(t, calls, 1)
	require.Equal(t, ro1.Mount.MountID, calls[0].mount.MountID)
	require.Nil(t, calls[0].updatedBranch)
	require.Empty(t, calls[0].deletedMountIDs)

	rw1, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	require.NoError(t, err)
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	calls = store.takeCalls()
	require.Len(t, calls, 1)
	require.Equal(t, rw1.Mount.MountID, calls[0].mount.MountID)
	require.NotNil(t, calls[0].updatedBranch)
	require.Equal(t, rw1.Mount.WriterEpoch, calls[0].updatedBranch.WriterEpoch)
	require.Empty(t, calls[0].deletedMountIDs)

	clk.Advance(15 * time.Second)

	ro2, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, true, false)
	require.NoError(t, err)
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	calls = store.takeCalls()
	require.Len(t, calls, 1)
	require.Equal(t, ro2.Mount.MountID, calls[0].mount.MountID)
	require.Nil(t, calls[0].updatedBranch)
	wantDeletedRO := []uuid.UUID{ro1.Mount.MountID, rw1.Mount.MountID}
	slices.SortFunc(wantDeletedRO, compareUUID)
	require.Equal(t, wantDeletedRO, calls[0].deletedMountIDs)

	mountsInStoreAfterRO, err := store.ListMounts(ctx, disk.DiskID, engine.DefaultBranchName, t0.Add(-time.Second))
	require.NoError(t, err)
	require.Len(t, mountsInStoreAfterRO, 1)
	require.Equal(t, ro2.Mount.MountID, mountsInStoreAfterRO[0].MountID)

	clk.Advance(15 * time.Second)

	rw2, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	require.NoError(t, err)
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	calls = store.takeCalls()
	require.Len(t, calls, 1)
	require.Equal(t, rw2.Mount.MountID, calls[0].mount.MountID)
	require.NotNil(t, calls[0].updatedBranch)
	require.Equal(t, rw2.Mount.WriterEpoch, calls[0].updatedBranch.WriterEpoch)
	require.Equal(t, []uuid.UUID{ro2.Mount.MountID}, calls[0].deletedMountIDs)

	mountsInStoreAfterRW, err := store.ListMounts(ctx, disk.DiskID, engine.DefaultBranchName, t0.Add(-time.Second))
	require.NoError(t, err)
	require.Len(t, mountsInStoreAfterRW, 1)
	require.Equal(t, rw2.Mount.MountID, mountsInStoreAfterRW[0].MountID)

	clk.Advance(4 * time.Second)
	ro3, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, true, false)
	require.NoError(t, err)
	store.takeCalls()

	clk.Advance(7 * time.Second)
	rw3, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	require.NoError(t, err)
	store.takeCalls()

	clk.Advance(5 * time.Second)
	rw4, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, true)
	require.NoError(t, err)
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	calls = store.takeCalls()
	require.Len(t, calls, 1)
	require.Equal(t, rw4.Mount.MountID, calls[0].mount.MountID)
	require.NotNil(t, calls[0].updatedBranch)
	require.Equal(t, rw3.Mount.WriterEpoch+1, calls[0].updatedBranch.WriterEpoch)
	require.Equal(t, rw4.Mount.WriterEpoch, calls[0].updatedBranch.WriterEpoch)
	wantDeletedForce := []uuid.UUID{ro3.Mount.MountID, rw3.Mount.MountID}
	slices.SortFunc(wantDeletedForce, compareUUID)
	require.Equal(t, wantDeletedForce, calls[0].deletedMountIDs)

	mountsInStoreAfterForce, err := store.ListMounts(ctx, disk.DiskID, engine.DefaultBranchName, t0.Add(-time.Second))
	require.NoError(t, err)
	require.Len(t, mountsInStoreAfterForce, 1)
	require.Equal(t, rw4.Mount.MountID, mountsInStoreAfterForce[0].MountID)

	branchesInStore, err := store.ListBranches(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Len(t, branchesInStore, 1)
	require.Equal(t, rw4.Mount.WriterEpoch, branchesInStore[0].WriterEpoch)
}
