package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStep30_MetaCodecs_RoundTripWorkOwnedOwnedAndLogicalBytes(t *testing.T) {
	t.Parallel()

	bm := BranchMeta{
		Name:           "feat-accounting",
		HeadCommitID:   42,
		WorkLayerID:    99,
		WriterEpoch:    7,
		WorkOwnedBytes: 12288,
		LogicalBytes:   32768,
		CreatedAt:      1000,
		UpdatedAt:      2000,
	}
	bmDecoded, err := DecodeBranchMeta(EncodeBranchMeta(bm))
	require.NoError(t, err)
	require.Equal(t, bm, bmDecoded)

	cm := CommitMeta{
		ID:            11,
		ParentID:      10,
		MergeParentID: 9,
		LayerID:       88,
		Kind:          CommitKindManual,
		OwnedBytes:    16384,
		LogicalBytes:  65536,
		Message:       "accounting",
		CreatedAt:     3000,
	}
	cmDecoded, err := DecodeCommitMeta(EncodeCommitMeta(cm))
	require.NoError(t, err)
	require.Equal(t, cm, cmDecoded)
}

func TestStep30_QuotaAndAccounting_WriteOverwriteUnlinkTruncateFallocateOrphanAndReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-step30-core",
		MaxBytes:    16384,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)

	f1, err := disk.Create(ctx, DefaultBranchName, RootIno, "f1.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f1.Ino, 0, bytes.Repeat([]byte{0x11}, 8192))
	require.NoError(t, err)

	require.Equal(t, uint64(8192), disk.UsedBytes())
	bm, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(8192), bm.WorkOwnedBytes)
	require.Equal(t, uint64(8192), bm.LogicalBytes)

	f2, err := disk.Create(ctx, DefaultBranchName, RootIno, "f2.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f2.Ino, 0, bytes.Repeat([]byte{0x22}, 8192))
	require.NoError(t, err)
	require.Equal(t, uint64(16384), disk.UsedBytes())

	_, err = disk.Write(ctx, DefaultBranchName, f1.Ino, 0, bytes.Repeat([]byte{0x33}, 8192))
	require.NoError(t, err)
	require.Equal(t, uint64(16384), disk.UsedBytes())
	require.Greater(t, disk.PackPhysicalBytes(), uint64(16384))

	f3, err := disk.Create(ctx, DefaultBranchName, RootIno, "f3.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f3.Ino, 0, bytes.Repeat([]byte{0x44}, 4096))
	require.ErrorIs(t, err, ErrNoSpace)
	require.Equal(t, uint64(16384), disk.UsedBytes())

	_, err = disk.Link(ctx, DefaultBranchName, f2.Ino, RootIno, "f2-hardlink.bin")
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "f2-hardlink.bin", false))
	require.Equal(t, uint64(16384), disk.UsedBytes())

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "f2.bin", true))
	require.Equal(t, uint64(16384), disk.UsedBytes())
	bm, err = disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(16384), bm.LogicalBytes)

	require.NoError(t, disk.PurgeOrphanInode(ctx, DefaultBranchName, f2.Ino))
	require.Equal(t, uint64(8192), disk.UsedBytes())
	bm, err = disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(8192), bm.WorkOwnedBytes)
	require.Equal(t, uint64(8192), bm.LogicalBytes)

	truncSize := uint64(5000)
	_, err = disk.SetAttr(ctx, DefaultBranchName, f1.Ino, SetAttrReq{Size: &truncSize})
	require.NoError(t, err)
	require.Equal(t, uint64(5000), disk.UsedBytes())
	bm, err = disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(5000), bm.WorkOwnedBytes)
	require.Equal(t, uint64(5000), bm.LogicalBytes)

	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f1.Ino, 0, 4096, FallocPunchHole|FallocKeepSize))
	require.Equal(t, uint64(904), disk.UsedBytes())
	bm, err = disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(904), bm.WorkOwnedBytes)
	require.Equal(t, uint64(904), bm.LogicalBytes)

	_, err = disk.Write(ctx, DefaultBranchName, f3.Ino, 65536, []byte("sparse"))
	require.NoError(t, err)
	require.Equal(t, uint64(910), disk.UsedBytes())

	f4, err := disk.Create(ctx, DefaultBranchName, RootIno, "f4.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f4.Ino, 0, bytes.Repeat([]byte{0x55}, 4096))
	require.NoError(t, err)
	require.Equal(t, uint64(910+4096), disk.UsedBytes())

	require.NoError(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: RootIno,
		OldName:      "f3.bin",
		NewParentIno: RootIno,
		NewName:      "f4.bin",
	}))
	require.Equal(t, uint64(910), disk.UsedBytes())

	metrics := disk.MetricsSnapshot()
	require.Equal(t, uint64(910), metrics.UsedBytes)
	require.Equal(t, disk.PackPhysicalBytes(), metrics.PackPhysicalBytes)
	require.Greater(t, metrics.PackPhysicalBytes, metrics.UsedBytes)

	snap, err := disk.SnapshotControlMeta(time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(910), snap.UsedBytes)

	tooSmall := uint64(909)
	_, err = disk.UpdateDiskMeta(ctx, nil, &tooSmall)
	require.ErrorIs(t, err, ErrQuotaShrinkBelowUsed)

	exactShrink := uint64(910)
	_, err = disk.UpdateDiskMeta(ctx, nil, &exactShrink)
	require.NoError(t, err)

	require.NoError(t, disk.Close())

	reopened, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	require.Equal(t, uint64(910), reopened.UsedBytes())
	bmReopened, err := reopened.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(910), bmReopened.WorkOwnedBytes)
	require.Equal(t, uint64(910), bmReopened.LogicalBytes)
}

func TestStep30_GitOpsAndMergeAccounting_CommitForkOrphanMigrationResetDeletePruneAndMerge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-step30-git",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)

	fBase, err := disk.Create(ctx, DefaultBranchName, RootIno, "base.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fBase.Ino, 0, bytes.Repeat([]byte{0xaa}, 4096))
	require.NoError(t, err)

	fOrphan, err := disk.Create(ctx, DefaultBranchName, RootIno, "orphan.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fOrphan.Ino, 0, bytes.Repeat([]byte{0xbb}, 2048))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "orphan.bin", true))

	c1, err := disk.Commit(ctx, DefaultBranchName, "c1 with open orphan")
	require.NoError(t, err)
	require.Equal(t, uint64(4096), c1.OwnedBytes)
	require.Equal(t, uint64(4096), c1.LogicalBytes)
	require.Equal(t, uint64(6144), disk.UsedBytes())

	mainAfterC1, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(2048), mainAfterC1.WorkOwnedBytes)
	require.Equal(t, uint64(6144), mainAfterC1.LogicalBytes)

	cFork, featBranch, err := disk.Fork(ctx, DefaultBranchName, "feat", "fork with orphan on main")
	require.NoError(t, err)
	require.Equal(t, uint64(0), cFork.LayerID)
	require.Equal(t, uint64(0), cFork.OwnedBytes)
	require.Equal(t, uint64(4096), cFork.LogicalBytes)
	require.Equal(t, uint64(0), featBranch.WorkOwnedBytes)
	require.Equal(t, uint64(4096), featBranch.LogicalBytes)
	require.Equal(t, uint64(6144), disk.UsedBytes())

	_, err = disk.MountBranch(ctx, DefaultBranchName, false, true, time.Minute)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), disk.UsedBytes())
	mainAfterMount, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(0), mainAfterMount.WorkOwnedBytes)
	require.Equal(t, uint64(4096), mainAfterMount.LogicalBytes)
	_, err = disk.EvictMounts(ctx, DefaultBranchName, "")
	require.NoError(t, err)

	fMainNew, err := disk.Create(ctx, DefaultBranchName, RootIno, "main-only.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fMainNew.Ino, 0, bytes.Repeat([]byte{0xcc}, 3000))
	require.NoError(t, err)
	cMain, err := disk.Commit(ctx, DefaultBranchName, "main-only")
	require.NoError(t, err)
	require.Equal(t, uint64(3000), cMain.OwnedBytes)
	require.Equal(t, uint64(7096), cMain.LogicalBytes)

	fFeatNew, err := disk.Create(ctx, "feat", RootIno, "feat-only.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", fFeatNew.Ino, 0, bytes.Repeat([]byte{0xdd}, 5000))
	require.NoError(t, err)
	cFeat, err := disk.Commit(ctx, "feat", "feat-only")
	require.NoError(t, err)
	require.Equal(t, uint64(5000), cFeat.OwnedBytes)
	require.Equal(t, uint64(9096), cFeat.LogicalBytes)
	require.Equal(t, uint64(12096), disk.UsedBytes())

	mergeRes, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
		Message:      "merge feat into main",
	})
	require.NoError(t, err)
	cMerge, err := disk.GetCommit(mergeRes.CommitID)
	require.NoError(t, err)
	require.Equal(t, CommitKindMerge, cMerge.Kind)
	require.Equal(t, uint64(0), cMerge.OwnedBytes)
	require.Equal(t, uint64(12096), cMerge.LogicalBytes)
	require.Equal(t, uint64(12096), disk.UsedBytes())

	mainAfterMerge, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(0), mainAfterMerge.WorkOwnedBytes)
	require.Equal(t, uint64(12096), mainAfterMerge.LogicalBytes)

	_, err = disk.Write(ctx, "feat", fFeatNew.Ino, 0, bytes.Repeat([]byte{0xee}, 4096))
	require.NoError(t, err)
	require.Equal(t, uint64(12096+4096), disk.UsedBytes())

	featReset, err := disk.Reset(ctx, "feat", c1.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(0), featReset.WorkOwnedBytes)
	require.Equal(t, uint64(4096), featReset.LogicalBytes)
	require.Equal(t, uint64(12096), disk.UsedBytes())

	fFeatTemp, err := disk.Create(ctx, "feat", RootIno, "temp.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", fFeatTemp.Ino, 0, bytes.Repeat([]byte{0xff}, 2000))
	require.NoError(t, err)
	require.Equal(t, uint64(14096), disk.UsedBytes())

	require.NoError(t, disk.DeleteBranch(ctx, "feat"))
	require.Equal(t, uint64(12096), disk.UsedBytes())

	_, err = disk.Reset(ctx, DefaultBranchName, c1.ID)
	require.NoError(t, err)
	pruned, err := disk.PruneCommits(ctx)
	require.NoError(t, err)
	require.Positive(t, pruned)
	require.Equal(t, uint64(4096), disk.UsedBytes())

	require.NoError(t, disk.Close())

	reopened, err := openTestDisk(testDiskOptions{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})
	require.Equal(t, uint64(4096), reopened.UsedBytes())
}

func TestStep30_ConcurrentBranchActorsCASQuotaAndCommitFailureRollback(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-step30-concurrent",
		MaxBytes:    16384,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	branches := []string{DefaultBranchName, "b1", "b2", "b3"}
	for _, b := range branches[1:] {
		_, _, err = disk.Fork(ctx, DefaultBranchName, b, "fork "+b)
		require.NoError(t, err)
	}

	files := make([]uint64, len(branches))
	for i, b := range branches {
		f, cErr := disk.Create(ctx, b, RootIno, "race.bin", 0o644, 0, 0)
		require.NoError(t, cErr)
		files[i] = f.Ino
	}

	var (
		wg        sync.WaitGroup
		succeeded atomic.Int32
		noSpace   atomic.Int32
	)
	for i, b := range branches {
		for j := range 2 {
			wg.Add(1)
			go func(branch string, ino uint64, blk int) {
				defer wg.Done()
				_, wErr := disk.Write(ctx, branch, ino, int64(blk*4096), bytes.Repeat([]byte{byte(blk + 1)}, 4096))
				if wErr == nil {
					succeeded.Add(1)
				} else if errors.Is(wErr, ErrNoSpace) {
					noSpace.Add(1)
				}
			}(b, files[i], j)
		}
	}
	wg.Wait()

	require.Equal(t, int32(4), succeeded.Load())
	require.Equal(t, int32(4), noSpace.Load())
	require.Equal(t, uint64(16384), disk.UsedBytes())

	newQuota := uint64(32768)
	_, err = disk.UpdateDiskMeta(ctx, nil, &newQuota)
	require.NoError(t, err)

	fFail, err := disk.Create(ctx, DefaultBranchName, RootIno, "rollback.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fFail.Ino, 0, bytes.Repeat([]byte{0x77}, 4096))
	require.NoError(t, err)

	injectedSyncErr := errors.New("injected pack sync failure")
	disk.packs.syncFileHook = func(uint64) error {
		return injectedSyncErr
	}

	usedBefore := disk.UsedBytes()
	_, resetErr := disk.Reset(ctx, DefaultBranchName, 1)
	require.ErrorIs(t, resetErr, injectedSyncErr)
	require.Equal(t, usedBefore, disk.UsedBytes())

	disk.packs.syncFileHook = nil
	require.NoError(t, disk.Fsync(ctx, DefaultBranchName, fFail.Ino))
	require.Equal(t, usedBefore, disk.UsedBytes())
}

func TestStep31_StatFSBranch_ReportsPerBranchLogicalUsedAndSharedDiskAvail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	const totalQuota uint64 = 100 * 4096
	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-step31-statfs",
		MaxBytes:    totalQuota,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	fMain, err := disk.Create(ctx, DefaultBranchName, RootIno, "shared.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fMain.Ino, 0, bytes.Repeat([]byte{0x11}, 4*4096))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "base 16KB")
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork feat")
	require.NoError(t, err)

	fFeat, err := disk.Create(ctx, "feat", RootIno, "big.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", fFeat.Ino, 0, bytes.Repeat([]byte{0x22}, 36*4096))
	require.NoError(t, err)

	stMain, err := disk.StatFSBranch(ctx, DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, totalQuota, stMain.TotalBytes)
	require.Equal(t, uint64(4*4096), stMain.UsedBytes)
	require.Equal(t, uint64(60*4096), stMain.AvailBytes)
	require.Equal(t, DefaultStatFSBlockSize, stMain.Bsize)
	require.Equal(t, DefaultStatFSBlockSize, stMain.Frsize)
	require.Equal(t, uint64(100), stMain.Blocks)
	require.Equal(t, uint64(96), stMain.Bfree)
	require.Equal(t, uint64(60), stMain.Bavail)

	stFeat, err := disk.StatFSBranch(ctx, "feat")
	require.NoError(t, err)
	require.Equal(t, totalQuota, stFeat.TotalBytes)
	require.Equal(t, uint64(40*4096), stFeat.UsedBytes)
	require.Equal(t, uint64(60*4096), stFeat.AvailBytes)
	require.Equal(t, uint64(100), stFeat.Blocks)
	require.Equal(t, uint64(60), stFeat.Bfree)
	require.Equal(t, uint64(60), stFeat.Bavail)

	stDisk, err := disk.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, totalQuota, stDisk.TotalBytes)
	require.Equal(t, uint64(40*4096), stDisk.UsedBytes)
	require.Equal(t, uint64(60*4096), stDisk.AvailBytes)
	require.Equal(t, uint64(100), stDisk.Blocks)
	require.Equal(t, uint64(60), stDisk.Bfree)
	require.Equal(t, uint64(60), stDisk.Bavail)

	fSmall, err := disk.Create(ctx, DefaultBranchName, RootIno, "tiny.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fSmall.Ino, 0, []byte("hello"))
	require.NoError(t, err)

	stMainTiny, err := disk.StatFSBranch(ctx, DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(4*4096+5), stMainTiny.UsedBytes)
	require.Equal(t, uint64(95), stMainTiny.Bfree)
	require.Equal(t, uint64(59), stMainTiny.Bavail)
}

func TestStep28_BackgroundGCWakeup_TriggersOnDeleteCommitPruneDeleteBranchAndReset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:          dir,
		DiskID:       "disk-step28-gc-wake",
		MaxBytes:     1 << 20,
		BlockSize:    4096,
		MaxPackSize:  8192,
		GCInterval:   0,
		EnableGCWake: true,
	})
	require.NoError(t, err)

	initBranch, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	initCmtID := initBranch.HeadCommitID

	f1, err := disk.Create(ctx, DefaultBranchName, RootIno, "c1.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f1.Ino, 0, bytes.Repeat([]byte{0x11}, 8192))
	require.NoError(t, err)
	c1, err := disk.Commit(ctx, DefaultBranchName, "c1")
	require.NoError(t, err)

	fKeep, err := disk.Create(ctx, DefaultBranchName, RootIno, "keep.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fKeep.Ino, 0, bytes.Repeat([]byte{0x22}, 4096))
	require.NoError(t, err)

	_, err = disk.Reset(ctx, DefaultBranchName, initCmtID)
	require.NoError(t, err)
	require.NoError(t, disk.DeleteCommit(ctx, c1.ID))

	packsDir := disk.packs.dir
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(packFilePath(packsDir, 1))
		return errors.Is(statErr, os.ErrNotExist)
	}, 3*time.Second, 10*time.Millisecond)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "ephemeral", "fork ephemeral")
	require.NoError(t, err)
	fBranch, err := disk.Create(ctx, "ephemeral", RootIno, "branch.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "ephemeral", fBranch.Ino, 0, bytes.Repeat([]byte{0x33}, 8192))
	require.NoError(t, err)
	fSeal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fSeal.Ino, 0, bytes.Repeat([]byte{0x44}, 8192))
	require.NoError(t, err)
	physBeforeDelBranch := disk.PackPhysicalBytes()

	require.NoError(t, disk.DeleteBranch(ctx, "ephemeral"))
	require.Eventually(t, func() bool {
		return disk.PackPhysicalBytes() < physBeforeDelBranch
	}, 3*time.Second, 10*time.Millisecond)

	_, err = disk.Commit(ctx, DefaultBranchName, "commit seal")
	require.NoError(t, err)
	fDropWork, err := disk.Create(ctx, DefaultBranchName, RootIno, "drop-work.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDropWork.Ino, 0, bytes.Repeat([]byte{0x55}, 16384))
	require.NoError(t, err)
	physBeforeReset := disk.PackPhysicalBytes()

	_, err = disk.Reset(ctx, DefaultBranchName, initCmtID)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return disk.PackPhysicalBytes() < physBeforeReset
	}, 3*time.Second, 10*time.Millisecond)

	physBeforePrune := disk.PackPhysicalBytes()
	pruned, err := disk.PruneCommits(ctx)
	require.NoError(t, err)
	require.Positive(t, pruned)
	require.Eventually(t, func() bool {
		return disk.PackPhysicalBytes() < physBeforePrune
	}, 3*time.Second, 10*time.Millisecond)

	for range 16 {
		disk.TriggerGC()
	}
	require.NoError(t, disk.Close())
	disk.TriggerGC()
}

func TestStep29_ActivePackGC_InspectsConditionallySealsAndReclaimsLowWatermarkPack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-step29-active-gc",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	fDead, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDead.Ino, 0, bytes.Repeat([]byte{0x11}, 4096))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

	require.Equal(t, uint64(4096), disk.PackPhysicalBytes())
	sealedWrong, err := disk.packs.SealActiveIf(ctx, 999)
	require.NoError(t, err)
	require.False(t, sealedWrong)

	statsZero, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, statsZero.PacksDeleted)
	require.Equal(t, uint64(4096), statsZero.BytesReclaimed)
	require.Zero(t, disk.PackPhysicalBytes())

	fKeep, err := disk.Create(ctx, DefaultBranchName, RootIno, "keep.bin", 0o644, 0, 0)
	require.NoError(t, err)
	keepPayload := bytes.Repeat([]byte{0x22}, 4096)
	_, err = disk.Write(ctx, DefaultBranchName, fKeep.Ino, 0, keepPayload)
	require.NoError(t, err)

	fDrop, err := disk.Create(ctx, DefaultBranchName, RootIno, "drop.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDrop.Ino, 0, bytes.Repeat([]byte{0x33}, 4096))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "drop.bin", false))

	statsHalf, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, statsHalf.PacksDeleted)
	require.Equal(t, 1, statsHalf.BlocksRelocated)
	require.Equal(t, uint64(8192), statsHalf.BytesReclaimed)
	require.Equal(t, uint64(4096), disk.PackPhysicalBytes())

	gotKeep, err := disk.Read(ctx, DefaultBranchName, fKeep.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotKeep)

	inspBefore, err := disk.packs.GCInspectionSnapshot(ctx)
	require.NoError(t, err)
	require.Positive(t, inspBefore.ActivePackID)
	require.Equal(t, uint64(4096), inspBefore.ActivePackSize)
	require.False(t, inspBefore.ActiveInFlight)

	statsHigh, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Zero(t, statsHigh.PacksDeleted)

	inspAfter, err := disk.packs.GCInspectionSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, inspBefore.ActivePackID, inspAfter.ActivePackID)
}

func TestStep29_ActivePackGC_PreservesConcurrentWriteAndInFlightBlockBetweenScanAndSeal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-step29-race",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	fDead, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead1.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDead.Ino, 0, bytes.Repeat([]byte{0x10}, 4096))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead1.bin", false))

	fRace, err := disk.Create(ctx, DefaultBranchName, RootIno, "concurrent.bin", 0o644, 0, 0)
	require.NoError(t, err)
	racePayload := bytes.Repeat([]byte{0x9a}, 4096)

	injectedWrite := false
	disk.beforeGCSealActiveHook = func(uint64) error {
		if !injectedWrite {
			injectedWrite = true
			_, wErr := disk.Write(ctx, DefaultBranchName, fRace.Ino, 0, racePayload)
			return wErr
		}
		return nil
	}

	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.True(t, injectedWrite)
	require.Equal(t, 1, stats.PacksDeleted)
	require.Equal(t, 1, stats.BlocksRelocated)

	gotRace, err := disk.Read(ctx, DefaultBranchName, fRace.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, racePayload, gotRace)

	fDead2, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead2.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDead2.Ino, 0, bytes.Repeat([]byte{0x20}, 8192))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead2.bin", false))

	var pinnedRefs []BlobRef
	disk.beforeGCSealActiveHook = func(uint64) error {
		refs, appErr := disk.packs.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0x55}, 4096)})
		if appErr != nil {
			return appErr
		}
		pinnedRefs = refs
		return nil
	}

	statsInFlight, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, pinnedRefs)
	require.Zero(t, statsInFlight.PacksDeleted)

	disk.beforeGCSealActiveHook = nil
	disk.packs.ReleaseInFlight(pinnedRefs)

	statsCleanup, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, statsCleanup.PacksDeleted)

	gotRaceStill, err := disk.Read(ctx, DefaultBranchName, fRace.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, racePayload, gotRaceStill)
}

func TestStep30_FastForwardMergeAndCommittedFileCoWTruncateFallocateAndReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-step30-ff-cow",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)

	f1, err := disk.Create(ctx, DefaultBranchName, RootIno, "committed.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f1.Ino, 0, bytes.Repeat([]byte{0xaa}, 8192))
	require.NoError(t, err)
	c1, err := disk.Commit(ctx, DefaultBranchName, "c1")
	require.NoError(t, err)
	require.Equal(t, uint64(8192), c1.OwnedBytes)
	require.Equal(t, uint64(8192), c1.LogicalBytes)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "ff-src", "fork ff-src")
	require.NoError(t, err)
	f2, err := disk.Create(ctx, "ff-src", RootIno, "extra.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "ff-src", f2.Ino, 0, bytes.Repeat([]byte{0xbb}, 4096))
	require.NoError(t, err)
	cFF, err := disk.Commit(ctx, "ff-src", "ff commit")
	require.NoError(t, err)
	require.Equal(t, uint64(4096), cFF.OwnedBytes)
	require.Equal(t, uint64(12288), cFF.LogicalBytes)

	usedBeforeFF := disk.UsedBytes()
	require.Equal(t, uint64(12288), usedBeforeFF)

	ffRes, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "ff-src",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.True(t, ffRes.FastForward)
	require.Equal(t, usedBeforeFF, disk.UsedBytes())

	mainAfterFF, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(0), mainAfterFF.WorkOwnedBytes)
	require.Equal(t, uint64(12288), mainAfterFF.LogicalBytes)

	truncSize := uint64(1000)
	_, err = disk.SetAttr(ctx, DefaultBranchName, f1.Ino, SetAttrReq{Size: &truncSize})
	require.NoError(t, err)

	mainAfterTrunc, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), mainAfterTrunc.WorkOwnedBytes)
	require.Equal(t, uint64(5096), mainAfterTrunc.LogicalBytes)
	require.Equal(t, uint64(12288+1000), disk.UsedBytes())

	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f2.Ino, 100, 200, FallocPunchHole|FallocKeepSize))
	mainAfterFalloc, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(1000+4096), mainAfterFalloc.WorkOwnedBytes)
	require.Equal(t, uint64(5096), mainAfterFalloc.LogicalBytes)
	require.Equal(t, uint64(12288+1000+4096), disk.UsedBytes())

	require.NoError(t, disk.Close())

	reopened, err := openTestDisk(testDiskOptions{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})
	require.Equal(t, uint64(12288+1000+4096), reopened.UsedBytes())
	bmReopened, err := reopened.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, uint64(1000+4096), bmReopened.WorkOwnedBytes)
	require.Equal(t, uint64(5096), bmReopened.LogicalBytes)
	cFFReopened, err := reopened.GetCommit(cFF.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), cFFReopened.OwnedBytes)
	require.Equal(t, uint64(12288), cFFReopened.LogicalBytes)
}
