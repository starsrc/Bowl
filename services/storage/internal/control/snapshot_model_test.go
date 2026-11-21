package control

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestDeleteDisk_IndependentDefault_PreservesForkedChildrenForMountWriteDiffAndMerge(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	rootDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "root", MaxBytes: 32 << 20})
	require.NoError(t, err)
	rootActor, err := mgr.Actor(ctx, rootDisk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, rootActor, 0, "base.txt", []byte("base-v1"))

	childA, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "child-a"})
	require.NoError(t, err)
	childB, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "child-b"})
	require.NoError(t, err)

	require.NoError(t, mgr.DeleteDisk(ctx, DeleteDiskParams{DiskID: rootDisk.DiskID, Recursive: false}))

	_, err = mgr.GetDisk(ctx, rootDisk.DiskID)
	require.ErrorIs(t, err, ErrNotFound)

	mntA, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: childA.DiskID})
	require.NoError(t, err)
	actorA, err := mgr.Actor(ctx, childA.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, actorA, mntA.Epoch, "from-a.txt", []byte("child-a-data"))
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: childA.DiskID, MountID: mntA.MountID}))

	diffRes, err := mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID: childA.DiskID,
		TargetDiskID: childB.DiskID,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), diffRes.Summary.Added)

	mergeRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: childA.DiskID,
		TargetDiskID: childB.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, mergeRes.MergeSnapshotID)

	actorB, err := mgr.Actor(ctx, childB.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("child-a-data"), readRegularFile(t, ctx, actorB, "from-a.txt", 1024))
}

func TestDeleteDisk_RecursiveSubtree_MountGateAtomicAndPhysicalCleanup(t *testing.T) {
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
	writeRegularFile(t, ctx, rootActor, 0, "seed.bin", bytes.Repeat([]byte{0x42}, 4096))

	branchB, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "branch-b"})
	require.NoError(t, err)
	siblingC, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "sibling-c"})
	require.NoError(t, err)
	grandB1, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: branchB.DiskID, Name: "grand-b1"})
	require.NoError(t, err)
	greatB2, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: grandB1.DiskID, Name: "great-b2"})
	require.NoError(t, err)

	b2Mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: greatB2.DiskID, ReadOnly: true})
	require.NoError(t, err)

	err = mgr.DeleteDisk(ctx, DeleteDiskParams{DiskID: branchB.DiskID, Recursive: true})
	requireDiskMountedStatus(t, err)

	for _, id := range []string{rootDisk.DiskID, branchB.DiskID, siblingC.DiskID, grandB1.DiskID, greatB2.DiskID} {
		_, gErr := mgr.GetDisk(ctx, id)
		require.NoError(t, gErr, "failed recursive delete must leave every disk intact: %s", id)
	}

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: greatB2.DiskID, MountID: b2Mnt.MountID}))

	lineageID := uuid.MustParse(rootDisk.DiskID)
	b2ID := uuid.MustParse(greatB2.DiskID)
	b2Row, err := memDB.GetDisk(ctx, b2ID)
	require.NoError(t, err)
	manualSnapID := uuid.Must(uuid.NewV7())
	require.NoError(t, memDB.SeedSnapshot(db.SnapshotRecord{
		LineageID:        lineageID,
		SnapshotID:       manualSnapID,
		SourceDiskID:     b2ID,
		ParentSnapshotID: b2Row.BaseSnapshotID,
		Kind:             db.SnapshotKindManual,
	}, uuid.Nil))
	manualDir := filepath.Join(rootDir, "snapshots", manualSnapID.String())
	require.NoError(t, os.MkdirAll(manualDir, 0o755))

	require.NoError(t, mgr.DeleteDisk(ctx, DeleteDiskParams{DiskID: branchB.DiskID, Recursive: true}))

	for _, deletedID := range []string{branchB.DiskID, grandB1.DiskID, greatB2.DiskID} {
		_, gErr := mgr.GetDisk(ctx, deletedID)
		require.ErrorIs(t, gErr, ErrNotFound)
		_, statErr := os.Stat(filepath.Join(rootDir, "disks", deletedID))
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
	for _, keptID := range []string{rootDisk.DiskID, siblingC.DiskID} {
		_, gErr := mgr.GetDisk(ctx, keptID)
		require.NoError(t, gErr)
	}

	_, manualStatErr := os.Stat(manualDir)
	require.NoError(t, manualStatErr, "manual snapshot directory on disk must survive recursive delete")
	require.InDelta(t, 2.0, gatherCounterValue(t, reg, "bowl_storage_control_snapshots_reclaimed_total"), 1e-9,
		"recursive delete must reclaim 2 unreachable fork snapshots and increment metric")
	require.InDelta(t, 3.0, gatherGaugeValue(t, reg, "bowl_storage_snapshots_active"), 1e-9,
		"2 reachable fork snapshots + 1 manual snapshot must remain on disk")
}

func TestMergeDisk_AlreadyUpToDate_NoOpPreservesEpochMountsSupportsDryRunAndMaxBytes(t *testing.T) {
	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.bin", bytes.Repeat([]byte{0x11}, 8192))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)

	tgtMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)
	epoch0 := tgtMnt.Epoch
	writeRegularFile(t, ctx, tActor, epoch0, "target-only.txt", []byte("keep-target-change"))

	targetID := uuid.MustParse(target.DiskID)
	tRowBefore, err := memDB.GetDisk(ctx, targetID)
	require.NoError(t, err)
	snapsBefore, err := memDB.ListLineageSnapshots(ctx, tRowBefore.LineageID)
	require.NoError(t, err)

	dryRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		DryRun:       true,
	})
	require.NoError(t, err)
	require.Equal(t, tRowBefore.BaseSnapshotID.String(), dryRes.LCASnapshotID)
	require.Equal(t, tRowBefore.BaseSnapshotID.String(), dryRes.MergeSnapshotID)
	require.Greater(t, dryRes.EstimatedUsedBytes, uint64(8192))
	require.Equal(t, uint64(32<<20), dryRes.TargetMaxBytes)

	tooSmall := uint64(1024)
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Force:        true,
		MaxBytes:     &tooSmall,
	})
	require.ErrorIs(t, err, ErrInvalidShrink)

	expanded := uint64(64 << 20)
	noopRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Force:        true,
		MaxBytes:     &expanded,
	})
	require.NoError(t, err)
	require.Equal(t, tRowBefore.BaseSnapshotID.String(), noopRes.LCASnapshotID)
	require.Equal(t, tRowBefore.BaseSnapshotID.String(), noopRes.MergeSnapshotID)
	require.NotNil(t, noopRes.Disk)
	require.Equal(t, expanded, noopRes.Disk.MaxBytes)
	require.Equal(t, expanded, noopRes.TargetMaxBytes)

	snapsAfter, err := memDB.ListLineageSnapshots(ctx, tRowBefore.LineageID)
	require.NoError(t, err)
	require.Len(t, snapsAfter, len(snapsBefore), "already up-to-date must create 0 new snapshots")

	tActorAfter, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	require.Equal(t, epoch0, tActorAfter.WriterEpoch(), "already up-to-date must not advance WriterEpoch")
	require.Equal(t, []byte("keep-target-change"), readRegularFile(t, ctx, tActorAfter, "target-only.txt", 1024))

	mountsAfter, err := mgr.ListMounts(ctx, target.DiskID)
	require.NoError(t, err)
	require.Len(t, mountsAfter, 1, "already up-to-date must not clear active target mounts")
	require.Equal(t, tgtMnt.MountID, mountsAfter[0].MountID)

	tRowAfter, err := memDB.GetDisk(ctx, targetID)
	require.NoError(t, err)
	require.Equal(t, expanded, tRowAfter.MaxBytes)
}

func TestMergeDisk_FastForward_NewAndReuseSnapshot_QuotaDryRunEpochAndCrashReconcile(t *testing.T) {
	ctx := context.Background()

	t.Run("uncommitted_source_changes_creates_one_merge_snapshot_enforces_quota_and_fences_epoch", func(t *testing.T) {
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr.Close() })

		target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 16 << 20})
		require.NoError(t, err)
		tActor, err := mgr.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

		source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "feat", MaxBytes: new(uint64(64 << 20))})
		require.NoError(t, err)
		sActor, err := mgr.Actor(ctx, source.DiskID)
		require.NoError(t, err)
		bigPayload := bytes.Repeat([]byte{0x7A}, 16384)
		writeRegularFile(t, ctx, sActor, 0, "feat.bin", bigPayload)

		tgtMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
		require.NoError(t, err)
		epoch0 := tgtMnt.Epoch

		tooSmall := uint64(4096)
		_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
			SourceDiskID: source.DiskID,
			TargetDiskID: target.DiskID,
			DryRun:       true,
			MaxBytes:     &tooSmall,
		})
		require.ErrorIs(t, err, ErrInvalidShrink)

		lineageID := uuid.MustParse(target.DiskID)
		snapsBefore, err := memDB.ListLineageSnapshots(ctx, lineageID)
		require.NoError(t, err)
		require.Len(t, snapsBefore, 1)

		dryRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
			SourceDiskID: source.DiskID,
			TargetDiskID: target.DiskID,
			DryRun:       true,
		})
		require.NoError(t, err)
		require.Empty(t, dryRes.MergeSnapshotID)
		require.GreaterOrEqual(t, dryRes.EstimatedUsedBytes, uint64(16384))
		require.Equal(t, uint64(16<<20), dryRes.TargetMaxBytes)

		snapsAfterDry, err := memDB.ListLineageSnapshots(ctx, lineageID)
		require.NoError(t, err)
		require.Len(t, snapsAfterDry, 1)

		ffRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
			SourceDiskID: source.DiskID,
			TargetDiskID: target.DiskID,
			Force:        true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, ffRes.MergeSnapshotID)
		require.NotEqual(t, ffRes.LCASnapshotID, ffRes.MergeSnapshotID)

		snapsAfterFF, err := memDB.ListLineageSnapshots(ctx, lineageID)
		require.NoError(t, err)
		require.Len(t, snapsAfterFF, 2, "fast-forward with uncommitted source changes must create exactly 1 merge snapshot and never delete prior fork snapshot")
		require.Equal(t, db.SnapshotKindFork, snapsAfterFF[0].Kind)
		require.Equal(t, db.SnapshotKindMerge, snapsAfterFF[1].Kind)
		require.Equal(t, ffRes.MergeSnapshotID, snapsAfterFF[1].SnapshotID.String())

		newTActor, err := mgr.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		require.Equal(t, epoch0+1, newTActor.WriterEpoch())
		require.Equal(t, bigPayload, readRegularFile(t, ctx, newTActor, "feat.bin", 32768))

		mountsAfter, err := mgr.ListMounts(ctx, target.DiskID)
		require.NoError(t, err)
		require.Empty(t, mountsAfter, "fast-forward must clear target mounts")
	})

	t.Run("zero_uncommitted_source_changes_reuses_source_base_snapshot_creating_zero_new_snapshots", func(t *testing.T) {
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)
		t.Cleanup(func() { _ = mgr.Close() })

		target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("v0"))

		source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "feat"})
		require.NoError(t, err)
		sActor, err := mgr.Actor(ctx, source.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("v1-reused"))

		_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: source.DiskID, Name: "feat-sub"})
		require.NoError(t, err)

		sourceID := uuid.MustParse(source.DiskID)
		srcRowBefore, err := memDB.GetDisk(ctx, sourceID)
		require.NoError(t, err)
		snapsBefore, err := memDB.ListLineageSnapshots(ctx, srcRowBefore.LineageID)
		require.NoError(t, err)
		require.Len(t, snapsBefore, 2)

		ffRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
			SourceDiskID: source.DiskID,
			TargetDiskID: target.DiskID,
		})
		require.NoError(t, err)
		require.Equal(t, srcRowBefore.BaseSnapshotID.String(), ffRes.MergeSnapshotID,
			"fast-forward with zero uncommitted source changes must reuse source base snapshot")

		snapsAfter, err := memDB.ListLineageSnapshots(ctx, srcRowBefore.LineageID)
		require.NoError(t, err)
		require.Len(t, snapsAfter, 2, "fast-forward reuse must create 0 new snapshots")

		reopenedTgt, err := mgr.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		require.Equal(t, []byte("v1-reused"), readRegularFile(t, ctx, reopenedTgt, "feat.txt", 1024))
	})

	t.Run("fast_forward_crash_rollback_preserves_reused_snapshot_and_cleans_uncommitted_snapshot", func(t *testing.T) {
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "keep.txt", []byte("orig"))

		source, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "feat"})
		require.NoError(t, err)
		sActor, err := mgr1.Actor(ctx, source.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, sActor, 0, "next.txt", []byte("next"))
		_, err = mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: source.DiskID, Name: "feat-sub"})
		require.NoError(t, err)

		srcRow, err := memDB.GetDisk(ctx, uuid.MustParse(source.DiskID))
		require.NoError(t, err)
		reusedSnapID := srcRow.BaseSnapshotID
		uncommittedSnapID := uuid.Must(uuid.NewV7())
		require.NoError(t, mgr1.Close())

		reusedStaging := mgr1.stagingMergeDir(target.DiskID, reusedSnapID)
		require.NoError(t, os.MkdirAll(reusedStaging, 0o755))

		uncommittedStaging := mgr1.stagingMergeDir(target.DiskID, uncommittedSnapID)
		uncommittedSnapDir := mgr1.snapshotDir(uncommittedSnapID.String())
		uncommittedMarker := mgr1.srcSnapshotMarkerPath(target.DiskID, uncommittedSnapID, uncommittedSnapID)
		require.NoError(t, os.MkdirAll(uncommittedStaging, 0o755))
		require.NoError(t, os.MkdirAll(uncommittedSnapDir, 0o755))
		require.NoError(t, os.WriteFile(uncommittedMarker, nil, 0o644))

		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })
		reopened, err := mgr2.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		require.Equal(t, []byte("orig"), readRegularFile(t, ctx, reopened, "keep.txt", 1024))

		_, err = os.Stat(mgr1.snapshotDir(reusedSnapID.String()))
		require.NoError(t, err, "committed reused snapshot must NOT be deleted on rollback")
		for _, gone := range []string{reusedStaging, uncommittedStaging, uncommittedSnapDir, uncommittedMarker} {
			_, sErr := os.Stat(gone)
			require.ErrorIs(t, sErr, os.ErrNotExist, "uncommitted artifact must be cleaned: %s", gone)
		}
	})
}
