package control

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestCreateSnapshot_OnlineWhileMounted_OffDAGOrphansAndReadSnapshotNode(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "primary", MaxBytes: 32 << 20})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	epoch0 := mnt.Epoch

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	fileIno := writeRegularFile(t, ctx, actor, epoch0, "live.txt", []byte("live-v1"))

	orphanIno := writeRegularFile(t, ctx, actor, epoch0, "orphan.txt", []byte("orphan-payload"))
	require.NoError(t, actor.Unlink(ctx, RootIno, []byte("orphan.txt"), true, epoch0))
	hasOrphans, err := actor.HasOrphans()
	require.NoError(t, err)
	require.True(t, hasOrphans)

	snap1, err := mgr.CreateSnapshot(ctx, CreateSnapshotParams{
		DiskID: disk.DiskID,
		Name:   "before-fork",
	})
	require.NoError(t, err)
	require.NotEmpty(t, snap1.SnapshotID)
	require.Equal(t, disk.DiskID, snap1.SourceDiskID)
	require.Empty(t, snap1.ParentSnapshotID)
	require.Empty(t, snap1.MergeParentSnapshotID)
	require.Equal(t, SnapshotKindManual, snap1.Kind)
	require.Equal(t, "before-fork", snap1.Name)
	require.False(t, snap1.Protected)
	require.False(t, snap1.CreatedAt.IsZero())

	require.Equal(t, epoch0, actor.WriterEpoch())
	mounts, err := mgr.ListMounts(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	require.Equal(t, mnt.MountID, mounts[0].MountID)

	diskRow, err := memDB.GetDisk(ctx, uuid.MustParse(disk.DiskID))
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, diskRow.BaseSnapshotID)

	nodeRes, err := mgr.ReadSnapshotNode(ctx, &ReadSnapshotNodeParams{
		DiskID:     disk.DiskID,
		SnapshotID: snap1.SnapshotID,
		Ino:        fileIno,
		Offset:     0,
		Size:       1024,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("live-v1"), nodeRes.Data)
	require.True(t, nodeRes.EOF)

	_, err = mgr.ReadSnapshotNode(ctx, &ReadSnapshotNodeParams{
		DiskID:     disk.DiskID,
		SnapshotID: snap1.SnapshotID,
		Ino:        orphanIno,
		Offset:     0,
		Size:       1024,
	})
	require.ErrorIs(t, err, ErrNotFound)

	child, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: disk.DiskID, Name: "child"})
	require.NoError(t, err)
	childActor, err := mgr.Actor(ctx, child.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, childActor, 0, "child-only.txt", []byte("child-v1"))

	diskRowBefore, err := memDB.GetDisk(ctx, uuid.MustParse(disk.DiskID))
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, diskRowBefore.BaseSnapshotID)

	overwriteRegularFile(t, ctx, actor, epoch0, "live.txt", []byte("live-v2"))
	snap2, err := mgr.CreateSnapshot(ctx, CreateSnapshotParams{
		DiskID: disk.DiskID,
		Name:   "",
	})
	require.NoError(t, err)
	require.Equal(t, diskRowBefore.BaseSnapshotID.String(), snap2.ParentSnapshotID)
	require.Empty(t, snap2.Name)
	require.Equal(t, SnapshotKindManual, snap2.Kind)
	require.False(t, snap2.Protected)

	diskRowAfter, err := memDB.GetDisk(ctx, uuid.MustParse(disk.DiskID))
	require.NoError(t, err)
	require.Equal(t, diskRowBefore.BaseSnapshotID, diskRowAfter.BaseSnapshotID)

	nodeRes2, err := mgr.ReadSnapshotNode(ctx, &ReadSnapshotNodeParams{
		DiskID:     disk.DiskID,
		SnapshotID: snap2.SnapshotID,
		Ino:        fileIno,
		Offset:     0,
		Size:       1024,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("live-v2"), nodeRes2.Data)

	diffRes, err := mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID: child.DiskID,
		TargetDiskID: disk.DiskID,
	})
	require.NoError(t, err)
	require.Equal(t, diskRowBefore.BaseSnapshotID.String(), diffRes.LCASnapshotID)
	require.Equal(t, uint64(1), diffRes.Summary.Added)

	mergeRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: child.DiskID,
		TargetDiskID: disk.DiskID,
		Force:        true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, mergeRes.MergeSnapshotID)
	reopenedPrimary, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("child-v1"), readRegularFile(t, ctx, reopenedPrimary, "child-only.txt", 1024))
}

func TestCreateSnapshot_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	validDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "d", MaxBytes: 8 << 20})
	require.NoError(t, err)

	cases := []struct {
		name    string
		params  CreateSnapshotParams
		wantErr error
	}{
		{name: "invalid_disk_id", params: CreateSnapshotParams{DiskID: "not-a-uuid"}, wantErr: ErrInvalidArgument},
		{name: "nil_disk_id", params: CreateSnapshotParams{DiskID: uuid.Nil.String()}, wantErr: ErrInvalidArgument},
		{name: "name_too_long", params: CreateSnapshotParams{DiskID: validDisk.DiskID, Name: strings.Repeat("a", MaxDiskNameBytes+1)}, wantErr: ErrNameTooLong},
		{name: "name_with_nul", params: CreateSnapshotParams{DiskID: validDisk.DiskID, Name: "bad\x00name"}, wantErr: ErrInvalidArgument},
		{name: "missing_disk", params: CreateSnapshotParams{DiskID: uuid.Must(uuid.NewV7()).String(), Name: "ok"}, wantErr: ErrNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, cErr := mgr.CreateSnapshot(ctx, tc.params)
			require.ErrorIs(t, cErr, tc.wantErr)
		})
	}
}

func TestListSnapshots_PurePGZeroPebbleColdOpen_FiltersPaginationAndProtected(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr1 := New(memDB, WithStorageRoot(rootDir))

	rootDisk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "root", MaxBytes: 32 << 20})
	require.NoError(t, err)
	rootActor, err := mgr1.Actor(ctx, rootDisk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, rootActor, 0, "base.txt", []byte("v0"))

	childA, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "child-a"})
	require.NoError(t, err)
	actorA, err := mgr1.Actor(ctx, childA.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, actorA, 0, "a1.txt", []byte("a1"))

	merge1, err := mgr1.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: childA.DiskID,
		TargetDiskID: rootDisk.DiskID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, merge1.MergeSnapshotID)

	writeRegularFile(t, ctx, actorA, 0, "a2.txt", []byte("a2"))
	merge2, err := mgr1.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: childA.DiskID,
		TargetDiskID: rootDisk.DiskID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, merge2.MergeSnapshotID)

	manualSnap, err := mgr1.CreateSnapshot(ctx, CreateSnapshotParams{
		DiskID: rootDisk.DiskID,
		Name:   "release-1",
	})
	require.NoError(t, err)

	require.NoError(t, mgr1.Close())

	mgr2, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
	require.True(t, ok)
	t.Cleanup(func() { _ = mgr2.Close() })

	var all []*Snapshot
	token := ""
	for {
		page, lErr := mgr2.ListSnapshots(ctx, ListSnapshotsParams{
			DiskID:    rootDisk.DiskID,
			PageSize:  2,
			PageToken: token,
		})
		require.NoError(t, lErr)
		all = append(all, page.Snapshots...)
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}

	require.Len(t, all, 4)

	mgr2.poolMu.RLock()
	loadedActors := len(mgr2.actors)
	mgr2.poolMu.RUnlock()
	require.Equal(t, 0, loadedActors, "ListSnapshots must not cold-open any Pebble StoreActor")

	protectedByID := make(map[string]bool, len(all))
	for _, s := range all {
		protectedByID[s.SnapshotID] = s.Protected
	}
	require.False(t, protectedByID[manualSnap.SnapshotID], "manual snapshot is off-DAG and unprotected")
	require.True(t, protectedByID[merge2.MergeSnapshotID], "active base snapshot merge2 must be protected")
	require.False(t, protectedByID[merge1.MergeSnapshotID], "superseded merge1 must be unprotected")
	require.False(t, protectedByID[merge1.LCASnapshotID], "superseded initial fork snapshot must be unprotected")

	manualOnly, err := mgr2.ListSnapshots(ctx, ListSnapshotsParams{
		DiskID: rootDisk.DiskID,
		Kind:   SnapshotKindManual,
	})
	require.NoError(t, err)
	require.Len(t, manualOnly.Snapshots, 1)
	require.Equal(t, manualSnap.SnapshotID, manualOnly.Snapshots[0].SnapshotID)

	forkOnly, err := mgr2.ListSnapshots(ctx, ListSnapshotsParams{
		DiskID: rootDisk.DiskID,
		Kind:   SnapshotKindFork,
	})
	require.NoError(t, err)
	require.Len(t, forkOnly.Snapshots, 1)

	mergeOnly, err := mgr2.ListSnapshots(ctx, ListSnapshotsParams{
		DiskID: rootDisk.DiskID,
		Kind:   SnapshotKindMerge,
	})
	require.NoError(t, err)
	require.Len(t, mergeOnly.Snapshots, 2)

	_, err = mgr2.ListSnapshots(ctx, ListSnapshotsParams{
		DiskID:   rootDisk.DiskID,
		PageSize: MaxListDisksPageSize + 1,
	})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = mgr2.ListSnapshots(ctx, ListSnapshotsParams{
		DiskID: rootDisk.DiskID,
		Kind:   SnapshotKind("bogus"),
	})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = mgr2.ListSnapshots(ctx, ListSnapshotsParams{
		DiskID:    rootDisk.DiskID,
		PageToken: "not-valid-base64!!!",
	})
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestDeleteSnapshot_CrossLineageHullProtectionAndPhysicalReclaim(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	reg := prometheus.NewRegistry()
	mgr := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, reg.Register(mgr.Collector()))

	rootDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "root", MaxBytes: 32 << 20})
	require.NoError(t, err)
	rootActor, err := mgr.Actor(ctx, rootDisk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, rootActor, 0, "root.txt", []byte("root"))

	childDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "child"})
	require.NoError(t, err)
	childActor, err := mgr.Actor(ctx, childDisk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, childActor, 0, "c1.txt", []byte("c1"))

	mergeRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: childDisk.DiskID,
		TargetDiskID: rootDisk.DiskID,
	})
	require.NoError(t, err)
	obsoleteForkSnapID := mergeRes.LCASnapshotID
	protectedMergeSnapID := mergeRes.MergeSnapshotID

	manualSnap, err := mgr.CreateSnapshot(ctx, CreateSnapshotParams{
		DiskID: rootDisk.DiskID,
		Name:   "manual-a",
	})
	require.NoError(t, err)

	otherDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "other", MaxBytes: 16 << 20})
	require.NoError(t, err)
	otherSnap, err := mgr.CreateSnapshot(ctx, CreateSnapshotParams{
		DiskID: otherDisk.DiskID,
		Name:   "other-manual",
	})
	require.NoError(t, err)

	err = mgr.DeleteSnapshot(ctx, DeleteSnapshotParams{
		DiskID:     rootDisk.DiskID,
		SnapshotID: otherSnap.SnapshotID,
	})
	require.ErrorIs(t, err, ErrCrossLineage)
	requireMappedStatus(t, err, codes.FailedPrecondition, "EXDEV")

	err = mgr.DeleteSnapshot(ctx, DeleteSnapshotParams{
		DiskID:     rootDisk.DiskID,
		SnapshotID: protectedMergeSnapID,
	})
	require.ErrorIs(t, err, ErrSnapshotInUse)
	requireMappedStatus(t, err, codes.FailedPrecondition, "EBUSY")
	_, err = os.Stat(filepath.Join(rootDir, "snapshots", protectedMergeSnapID))
	require.NoError(t, err)

	require.NoError(t, mgr.DeleteSnapshot(ctx, DeleteSnapshotParams{
		DiskID:     rootDisk.DiskID,
		SnapshotID: manualSnap.SnapshotID,
	}))
	_, err = os.Stat(filepath.Join(rootDir, "snapshots", manualSnap.SnapshotID))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.InDelta(t, 1.0, gatherCounterValue(t, reg, "bowl_storage_control_snapshots_reclaimed_total"), 1e-9)

	require.NoError(t, mgr.DeleteSnapshot(ctx, DeleteSnapshotParams{
		DiskID:     rootDisk.DiskID,
		SnapshotID: obsoleteForkSnapID,
	}))
	_, err = os.Stat(filepath.Join(rootDir, "snapshots", obsoleteForkSnapID))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.InDelta(t, 2.0, gatherCounterValue(t, reg, "bowl_storage_control_snapshots_reclaimed_total"), 1e-9)

	err = mgr.DeleteSnapshot(ctx, DeleteSnapshotParams{
		DiskID:     rootDisk.DiskID,
		SnapshotID: obsoleteForkSnapID,
	})
	require.ErrorIs(t, err, ErrNotFound)

	writeRegularFile(t, ctx, childActor, 0, "c2.txt", []byte("c2"))
	diffRes, err := mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID: childDisk.DiskID,
		TargetDiskID: rootDisk.DiskID,
	})
	require.NoError(t, err)
	require.Equal(t, protectedMergeSnapID, diffRes.LCASnapshotID)
	require.Equal(t, uint64(1), diffRes.Summary.Added)
}

func TestPruneSnapshots_DryRunAndApply_SkipsManualAndHullPreservingDiffMerge(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	reg := prometheus.NewRegistry()
	mgr := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, reg.Register(mgr.Collector()))

	rootDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "root", MaxBytes: 32 << 20})
	require.NoError(t, err)
	rootActor, err := mgr.Actor(ctx, rootDisk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, rootActor, 0, "base.txt", []byte("v0"))

	childDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "child"})
	require.NoError(t, err)
	childActor, err := mgr.Actor(ctx, childDisk.DiskID)
	require.NoError(t, err)

	writeRegularFile(t, ctx, childActor, 0, "m1.txt", []byte("m1"))
	merge1, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: childDisk.DiskID,
		TargetDiskID: rootDisk.DiskID,
	})
	require.NoError(t, err)

	writeRegularFile(t, ctx, childActor, 0, "m2.txt", []byte("m2"))
	merge2, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: childDisk.DiskID,
		TargetDiskID: rootDisk.DiskID,
	})
	require.NoError(t, err)

	manualSnap, err := mgr.CreateSnapshot(ctx, CreateSnapshotParams{
		DiskID: rootDisk.DiskID,
		Name:   "keep-me",
	})
	require.NoError(t, err)

	dryRes, err := mgr.PruneSnapshots(ctx, PruneSnapshotsParams{
		DiskID: rootDisk.DiskID,
		DryRun: true,
	})
	require.NoError(t, err)
	require.True(t, dryRes.DryRun)
	require.Equal(t, uint32(2), dryRes.ReclaimedCount)
	require.Len(t, dryRes.Snapshots, 2)

	candidateIDs := map[string]struct{}{
		dryRes.Snapshots[0].SnapshotID: {},
		dryRes.Snapshots[1].SnapshotID: {},
	}
	require.Contains(t, candidateIDs, merge1.LCASnapshotID)
	require.Contains(t, candidateIDs, merge1.MergeSnapshotID)
	require.NotContains(t, candidateIDs, merge2.MergeSnapshotID)
	require.NotContains(t, candidateIDs, manualSnap.SnapshotID)

	for sid := range candidateIDs {
		_, sErr := os.Stat(filepath.Join(rootDir, "snapshots", sid))
		require.NoError(t, sErr, "dry_run must keep snapshot dir on disk: %s", sid)
	}
	require.InDelta(t, 0.0, gatherCounterValue(t, reg, "bowl_storage_control_snapshots_reclaimed_total"), 1e-9)

	applyRes, err := mgr.PruneSnapshots(ctx, PruneSnapshotsParams{
		DiskID: rootDisk.DiskID,
		DryRun: false,
	})
	require.NoError(t, err)
	require.False(t, applyRes.DryRun)
	require.Equal(t, uint32(2), applyRes.ReclaimedCount)
	require.Len(t, applyRes.Snapshots, 2)

	for sid := range candidateIDs {
		_, sErr := os.Stat(filepath.Join(rootDir, "snapshots", sid))
		require.ErrorIs(t, sErr, os.ErrNotExist, "pruned snapshot dir must be removed: %s", sid)
	}
	_, err = os.Stat(filepath.Join(rootDir, "snapshots", merge2.MergeSnapshotID))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(rootDir, "snapshots", manualSnap.SnapshotID))
	require.NoError(t, err)
	require.InDelta(t, 2.0, gatherCounterValue(t, reg, "bowl_storage_control_snapshots_reclaimed_total"), 1e-9)

	secondPrune, err := mgr.PruneSnapshots(ctx, PruneSnapshotsParams{
		DiskID: rootDisk.DiskID,
		DryRun: false,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(0), secondPrune.ReclaimedCount)
	require.Empty(t, secondPrune.Snapshots)

	writeRegularFile(t, ctx, childActor, 0, "m3.txt", []byte("m3"))
	diffRes, err := mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID: childDisk.DiskID,
		TargetDiskID: rootDisk.DiskID,
	})
	require.NoError(t, err)
	require.Equal(t, merge2.MergeSnapshotID, diffRes.LCASnapshotID)
	require.Equal(t, uint64(1), diffRes.Summary.Added)

	merge3, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: childDisk.DiskID,
		TargetDiskID: rootDisk.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, merge3.MergeSnapshotID)
	reopenedRoot, err := mgr.Actor(ctx, rootDisk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("m3"), readRegularFile(t, ctx, reopenedRoot, "m3.txt", 1024))
}
