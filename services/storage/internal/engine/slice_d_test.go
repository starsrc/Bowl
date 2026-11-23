package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStep6_GC_SealedWatermarkProtectsActiveAndInFlightPacks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-gc-wm",
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	blk := bytes.Repeat([]byte{0x11}, 4096)
	inFlightRefs, err := disk.packs.AppendBlocks(ctx, [][]byte{blk}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), inFlightRefs[0].PackID)

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "filler.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{0x22}, 8192))
	require.NoError(t, err)

	wmWhileInFlight, err := disk.packs.SealedWatermark(ctx)
	require.NoError(t, err)
	require.Zero(t, wmWhileInFlight)

	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.PacksDeleted)

	_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.NoError(t, statErr)

	disk.packs.ReleaseInFlight(inFlightRefs)

	wmAfterRelease, err := disk.packs.SealedWatermark(ctx)
	require.NoError(t, err)
	require.Positive(t, wmAfterRelease)
}

func TestStep6_GC_ReapsDeadPacksAfterResetAndPruneCommits(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-gc-dead",
		MaxBytes:    64 << 10,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	initBranch, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	initCmtID := initBranch.HeadCommitID

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{0x33}, 8192))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "will-be-pruned")
	require.NoError(t, err)

	f2, err := disk.Create(ctx, DefaultBranchName, RootIno, "live.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f2.Ino, 0, bytes.Repeat([]byte{0x44}, 4096))
	require.NoError(t, err)

	_, err = disk.Reset(ctx, DefaultBranchName, initCmtID)
	require.NoError(t, err)

	pruned, err := disk.PruneCommits(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, pruned)

	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PacksDeleted)
	require.Equal(t, uint64(8192), stats.BytesReclaimed)

	_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestStep6_GC_RelocatesLowWatermarkPackAcrossWorkingAndImmutableCommitLayers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-gc-relocate",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 16384,
	})
	require.NoError(t, err)

	fCommit, err := disk.Create(ctx, DefaultBranchName, RootIno, "committed.bin", 0o644, 0, 0)
	require.NoError(t, err)
	commitPayload := bytes.Repeat([]byte{0xa1}, 4096)
	_, err = disk.Write(ctx, DefaultBranchName, fCommit.Ino, 0, commitPayload)
	require.NoError(t, err)
	c1, err := disk.Commit(ctx, DefaultBranchName, "keep-commit-block")
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat-gc", "fork for gc")
	require.NoError(t, err)

	fWork, err := disk.Create(ctx, "feat-gc", RootIno, "working.bin", 0o644, 0, 0)
	require.NoError(t, err)
	workPayload := bytes.Repeat([]byte{0xb2}, 4096)
	_, err = disk.Write(ctx, "feat-gc", fWork.Ino, 0, workPayload)
	require.NoError(t, err)

	fDead, err := disk.Create(ctx, DefaultBranchName, RootIno, "ephemeral.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDead.Ino, 0, bytes.Repeat([]byte{0xc3}, 8192))
	require.NoError(t, err)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "ephemeral.bin"))

	fSeal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
	require.NoError(t, err)
	sealPayload := bytes.Repeat([]byte{0xd4}, 4096)
	_, err = disk.Write(ctx, DefaultBranchName, fSeal.Ino, 0, sealPayload)
	require.NoError(t, err)

	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PacksDeleted)
	require.Equal(t, 2, stats.BlocksRelocated)
	require.Equal(t, uint64(16384), stats.BytesReclaimed)

	_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	gotCommit, err := disk.ReadAtCommit(ctx, c1.ID, fCommit.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, commitPayload, gotCommit)

	gotMainCommit, err := disk.Read(ctx, DefaultBranchName, fCommit.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, commitPayload, gotMainCommit)

	gotWork, err := disk.Read(ctx, "feat-gc", fWork.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, workPayload, gotWork)

	require.NoError(t, disk.Close())

	reopened, err := OpenDisk(DiskOptions{
		Dir:         dir,
		MaxPackSize: 16384,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	gotCommitAfter, err := reopened.ReadAtCommit(ctx, c1.ID, fCommit.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, commitPayload, gotCommitAfter)

	gotWorkAfter, err := reopened.Read(ctx, "feat-gc", fWork.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, workPayload, gotWorkAfter)
}

func TestStep6_GC_CASSkipsConcurrentlyOverwrittenWorkingBlockWithoutClobbering(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-gc-cas",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	fLive, err := disk.Create(ctx, DefaultBranchName, RootIno, "target.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fLive.Ino, 0, bytes.Repeat([]byte{0x10}, 4096))
	require.NoError(t, err)

	fDead, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDead.Ino, 0, bytes.Repeat([]byte{0x20}, 4096))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin"))

	fSeal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fSeal.Ino, 0, bytes.Repeat([]byte{0x30}, 4096))
	require.NoError(t, err)

	newerPayload := bytes.Repeat([]byte{0x99}, 4096)
	overwritten := false
	disk.beforeGCRelocateHook = func(_ uint64, loc BlockRefLocation) error {
		if !overwritten && loc.Ino == fLive.Ino {
			overwritten = true
			_, wErr := disk.Write(ctx, DefaultBranchName, fLive.Ino, 0, newerPayload)
			return wErr
		}
		return nil
	}

	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.True(t, overwritten)
	require.Equal(t, 1, stats.PacksDeleted)
	require.Equal(t, 1, stats.BlocksSkipped)

	got, err := disk.Read(ctx, DefaultBranchName, fLive.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, newerPayload, got)
}

func TestStep6_GC_AbortsPackDeleteWhenAnyRelocationFailsAndRetriesCleanly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-gc-abort",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	fKeep, err := disk.Create(ctx, DefaultBranchName, RootIno, "keep.bin", 0o644, 0, 0)
	require.NoError(t, err)
	keepPayload := bytes.Repeat([]byte{0x77}, 4096)
	_, err = disk.Write(ctx, DefaultBranchName, fKeep.Ino, 0, keepPayload)
	require.NoError(t, err)

	fDrop, err := disk.Create(ctx, DefaultBranchName, RootIno, "drop.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDrop.Ino, 0, bytes.Repeat([]byte{0x88}, 4096))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "drop.bin"))

	fSeal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fSeal.Ino, 0, bytes.Repeat([]byte{0x66}, 4096))
	require.NoError(t, err)

	injectedErr := errors.New("injected relocate timeout")
	disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
		if packID == 1 {
			return injectedErr
		}
		return nil
	}

	statsFail, err := disk.RunGC(ctx)
	require.ErrorIs(t, err, injectedErr)
	require.Zero(t, statsFail.PacksDeleted)

	_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.NoError(t, statErr)

	gotStillIntact, err := disk.Read(ctx, DefaultBranchName, fKeep.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotStillIntact)

	disk.beforeGCRelocateHook = nil
	statsRetry, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, statsRetry.PacksDeleted)
	require.Equal(t, 1, statsRetry.BlocksRelocated)

	_, statErr = os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	gotAfterRetry, err := disk.Read(ctx, DefaultBranchName, fKeep.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotAfterRetry)
}

func TestStep7_GC_QuotaExemptRelocationReclaimsSpaceOnFullDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-gc-full",
		MaxBytes:    16384,
		BlockSize:   4096,
		MaxPackSize: 8192,
		GCInterval:  50 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	fKeep, err := disk.Create(ctx, DefaultBranchName, RootIno, "keep.bin", 0o644, 0, 0)
	require.NoError(t, err)
	keepPayload := bytes.Repeat([]byte{0xaa}, 4096)
	_, err = disk.Write(ctx, DefaultBranchName, fKeep.Ino, 0, keepPayload)
	require.NoError(t, err)

	fDrop, err := disk.Create(ctx, DefaultBranchName, RootIno, "drop.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDrop.Ino, 0, bytes.Repeat([]byte{0xbb}, 4096))
	require.NoError(t, err)

	fFull, err := disk.Create(ctx, DefaultBranchName, RootIno, "full.bin", 0o644, 0, 0)
	require.NoError(t, err)
	fullPayload := bytes.Repeat([]byte{0xcc}, 8192)
	_, err = disk.Write(ctx, DefaultBranchName, fFull.Ino, 0, fullPayload)
	require.NoError(t, err)

	fExtra, err := disk.Create(ctx, DefaultBranchName, RootIno, "extra.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fExtra.Ino, 0, bytes.Repeat([]byte{0xdd}, 4096))
	require.ErrorIs(t, err, ErrNoSpace)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "drop.bin"))

	_, err = disk.RunGC(ctx)
	require.NoError(t, err)

	statAfter, err := disk.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(12288), statAfter.UsedBytes)
	require.Equal(t, uint64(4096), statAfter.AvailBytes)

	extraPayload := bytes.Repeat([]byte{0xee}, 4096)
	_, err = disk.Write(ctx, DefaultBranchName, fExtra.Ino, 0, extraPayload)
	require.NoError(t, err)

	gotKeep, err := disk.Read(ctx, DefaultBranchName, fKeep.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotKeep)

	gotFull, err := disk.Read(ctx, DefaultBranchName, fFull.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, fullPayload, gotFull)

	gotExtra, err := disk.Read(ctx, DefaultBranchName, fExtra.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, extraPayload, gotExtra)
}
