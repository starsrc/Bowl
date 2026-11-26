package engine

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func deleteRootInodeFromBranch(t *testing.T, disk *testDisk, branch string) {
	t.Helper()
	_, layers, err := disk.resolveBranchLayers(disk.db, branch)
	require.NoError(t, err)
	for _, layerID := range layers {
		require.NoError(t, disk.db.Delete(disk.keys.LayerInodeKey(layerID, RootIno), pebble.Sync))
	}
}

func TestMountBranch_Step12_MissingRootInodeFailsAtomicallyWithoutMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clk := newStepClock(time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC))

	disk, err := openTestDisk(testDiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "disk-step12-atomic-root",
		BlockSize: 4096,
		Now:       clk.Now,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, err = disk.MountBranch(ctx, DefaultBranchName, true, false, 5*time.Second)
	require.NoError(t, err)
	_, err = disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	clk.Advance(10 * time.Second)

	activeMountsBefore, err := disk.ListMounts(DefaultBranchName, clk.Now())
	require.NoError(t, err)
	require.Len(t, activeMountsBefore, 1)
	activeRW := activeMountsBefore[0]
	require.False(t, activeRW.ReadOnly)

	rwCtx := WithWriterEpoch(ctx, activeRW.WriterEpoch)
	orphan, err := disk.CreateTmpfile(rwCtx, DefaultBranchName, RootIno, 0o600, 0, 0, false)
	require.NoError(t, err)
	_, err = disk.Write(rwCtx, DefaultBranchName, orphan.Ino, 0, bytes.Repeat([]byte("A"), 4096))
	require.NoError(t, err)

	branchBefore, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, activeRW.WriterEpoch, branchBefore.WriterEpoch)
	require.Positive(t, branchBefore.WorkOwnedBytes)

	allMountsBefore, err := listAllMountsFromReader(disk.db, disk.keys, DefaultBranchName)
	require.NoError(t, err)
	require.Len(t, allMountsBefore, 2)

	deleteRootInodeFromBranch(t, disk, DefaultBranchName)

	_, err = disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.ErrorIs(t, err, ErrDiskMounted)

	_, err = disk.MountBranch(ctx, DefaultBranchName, true, false, 30*time.Second)
	require.ErrorIs(t, err, ErrNotFound)

	allMountsAfterRO, err := listAllMountsFromReader(disk.db, disk.keys, DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, allMountsBefore, allMountsAfterRO)

	branchAfterRO, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, branchBefore, branchAfterRO)

	_, err = disk.MountBranch(ctx, DefaultBranchName, false, true, 30*time.Second)
	require.ErrorIs(t, err, ErrNotFound)

	allMountsAfterForceRW, err := listAllMountsFromReader(disk.db, disk.keys, DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, allMountsBefore, allMountsAfterForceRW)

	branchAfterForceRW, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, branchBefore, branchAfterForceRW)

	_, err = disk.GetAttr(ctx, DefaultBranchName, orphan.Ino)
	require.NoError(t, err)
}

func TestMountBranch_Step13_ReturnsRootInodeUpdatedBranchAndSortedDeletedMountIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clk := newStepClock(time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC))

	disk, err := openTestDisk(testDiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "disk-step13-mount-result",
		BlockSize: 4096,
		Now:       clk.Now,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork feat")
	require.NoError(t, err)

	roClean, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 300*time.Second)
	require.NoError(t, err)
	require.True(t, roClean.Mount.ReadOnly)
	require.Zero(t, roClean.Mount.WriterEpoch)
	require.Equal(t, RootIno, roClean.RootInode.Ino)
	require.Equal(t, ModeDir, roClean.RootInode.Mode&ModeTypeMask)
	require.Nil(t, roClean.UpdatedBranch)
	require.Nil(t, roClean.DeletedMountIDs)

	expRO1, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 5*time.Second)
	require.NoError(t, err)
	expRW1, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 5*time.Second)
	require.NoError(t, err)
	featExp, err := disk.MountBranch(ctx, "feat", true, false, 5*time.Second)
	require.NoError(t, err)

	clk.Advance(10 * time.Second)

	roPrune, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 300*time.Second)
	require.NoError(t, err)
	require.True(t, roPrune.Mount.ReadOnly)
	require.Equal(t, RootIno, roPrune.RootInode.Ino)
	require.Nil(t, roPrune.UpdatedBranch)
	wantRODeleted := []string{expRO1.Mount.ID, expRW1.Mount.ID}
	sort.Strings(wantRODeleted)
	require.Equal(t, wantRODeleted, roPrune.DeletedMountIDs)

	_, err = disk.GetMount(featExp.Mount.ID)
	require.NoError(t, err)

	orphan, err := disk.CreateTmpfile(ctx, DefaultBranchName, RootIno, 0o600, 0, 0, false)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, orphan.Ino, 0, bytes.Repeat([]byte("Z"), 4096))
	require.NoError(t, err)

	expRO2, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 5*time.Second)
	require.NoError(t, err)
	clk.Advance(10 * time.Second)

	rwPrune, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 300*time.Second)
	require.NoError(t, err)
	require.False(t, rwPrune.Mount.ReadOnly)
	require.Equal(t, RootIno, rwPrune.RootInode.Ino)
	require.NotNil(t, rwPrune.UpdatedBranch)
	require.Equal(t, DefaultBranchName, rwPrune.UpdatedBranch.Name)
	require.Equal(t, rwPrune.Mount.WriterEpoch, rwPrune.UpdatedBranch.WriterEpoch)
	require.Zero(t, rwPrune.UpdatedBranch.WorkOwnedBytes)
	require.Equal(t, []string{expRO2.Mount.ID}, rwPrune.DeletedMountIDs)
	gotBranchAfterRW, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, gotBranchAfterRW, *rwPrune.UpdatedBranch)

	expRO3, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 5*time.Second)
	require.NoError(t, err)
	expRO4, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 5*time.Second)
	require.NoError(t, err)
	clk.Advance(10 * time.Second)

	rwForce, err := disk.MountBranch(ctx, DefaultBranchName, false, true, 300*time.Second)
	require.NoError(t, err)
	require.False(t, rwForce.Mount.ReadOnly)
	require.Equal(t, RootIno, rwForce.RootInode.Ino)
	require.NotNil(t, rwForce.UpdatedBranch)
	require.Equal(t, rwPrune.Mount.WriterEpoch+1, rwForce.UpdatedBranch.WriterEpoch)
	require.Equal(t, rwForce.Mount.WriterEpoch, rwForce.UpdatedBranch.WriterEpoch)
	wantForceDeleted := []string{expRO3.Mount.ID, expRO4.Mount.ID, rwPrune.Mount.ID}
	sort.Strings(wantForceDeleted)
	require.Equal(t, wantForceDeleted, rwForce.DeletedMountIDs)
	require.True(t, sort.StringsAreSorted(rwForce.DeletedMountIDs))
	gotBranchAfterForce, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, gotBranchAfterForce, *rwForce.UpdatedBranch)

	require.NoError(t, disk.Unmount(ctx, rwForce.Mount.ID))
	rwClean, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 300*time.Second)
	require.NoError(t, err)
	require.NotNil(t, rwClean.UpdatedBranch)
	require.Equal(t, rwForce.Mount.WriterEpoch+1, rwClean.UpdatedBranch.WriterEpoch)
	require.Nil(t, rwClean.DeletedMountIDs)
}
