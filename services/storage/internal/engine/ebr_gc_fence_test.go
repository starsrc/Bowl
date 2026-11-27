package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEBR_AccountingDuringPinnedWindow_GCStatsImmediatePhysicalBytesDeferredUntilUnpin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         t.TempDir(),
		DiskID:      "ebr-accounting",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	deadPayload := bytes.Repeat([]byte{0xaa}, 8192)
	deadFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, deadFile.Ino, 0, deadPayload)
	require.NoError(t, err)

	sealFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, sealFile.Ino, 0, bytes.Repeat([]byte{0xbb}, 4096))
	require.NoError(t, err)

	rv := disk.openReadView()
	pinnedRef, ok, err := func() (BlobRef, bool, error) {
		bm, bmErr := readBranchMeta(rv.snap, disk.keys, DefaultBranchName)
		if bmErr != nil {
			return BlobRef{}, false, bmErr
		}
		var cursor branchLayerCursor
		disk.initBranchCursor(&cursor, rv.snap, bm, rv.snapGen)
		return cursor.getDataRef(deadFile.Ino, 0)
	}()
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

	physBefore := disk.PackPhysicalBytes()
	require.Equal(t, uint64(12288), physBefore)

	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PacksDeleted)
	require.Equal(t, uint64(8192), stats.BytesReclaimed)
	require.Equal(t, physBefore, disk.PackPhysicalBytes())

	_, statErr := os.Stat(packFilePath(disk.packs.dir, pinnedRef.PackID))
	require.NoError(t, statErr)

	gotPinned, err := rv.readBlock(pinnedRef)
	require.NoError(t, err)
	require.Equal(t, deadPayload[:4096], gotPinned)

	statsSecond, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Zero(t, statsSecond.PacksDeleted)
	require.Zero(t, statsSecond.BytesReclaimed)
	require.Equal(t, physBefore, disk.PackPhysicalBytes())

	require.NoError(t, rv.Close())

	require.Equal(t, uint64(4096), disk.PackPhysicalBytes())
	_, statErr = os.Stat(packFilePath(disk.packs.dir, pinnedRef.PackID))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestEBR_UnlinkFailure_DoesNotFailCompletedReadRetriesOnNextDrainAndCloseResources(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("inline reader drain failure does not fail read and retries on next drain", func(t *testing.T) {
		t.Parallel()
		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "ebr-unlink-retry",
			MaxBytes:    1 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		dead1, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead1.bin", 0o644, 0, 0)
		require.NoError(t, err)
		payload1 := bytes.Repeat([]byte{0x11}, 8192)
		_, err = disk.Write(ctx, DefaultBranchName, dead1.Ino, 0, payload1)
		require.NoError(t, err)

		dead2, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead2.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, dead2.Ino, 0, bytes.Repeat([]byte{0x22}, 8192))
		require.NoError(t, err)

		seal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, seal.Ino, 0, bytes.Repeat([]byte{0x33}, 4096))
		require.NoError(t, err)

		rv := disk.openReadView()
		bm, err := readBranchMeta(rv.snap, disk.keys, DefaultBranchName)
		require.NoError(t, err)
		var cursor branchLayerCursor
		disk.initBranchCursor(&cursor, rv.snap, bm, rv.snapGen)
		ref1, ok, err := cursor.getDataRef(dead1.Ino, 0)
		require.NoError(t, err)
		require.True(t, ok)

		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead1.bin", false))

		var failPack1 atomic.Bool
		failPack1.Store(true)
		var pack1RemoveAttempts atomic.Int32
		unlinkErr := errors.New("injected pack unlink failure")
		disk.packs.removePackHook = func(packID uint64) error {
			if packID == ref1.PackID {
				pack1RemoveAttempts.Add(1)
				if failPack1.Load() {
					return unlinkErr
				}
			}
			return nil
		}

		stats1, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, stats1.PacksDeleted)
		require.Equal(t, uint64(8192), stats1.BytesReclaimed)
		require.Zero(t, pack1RemoveAttempts.Load())

		got, err := rv.readBlock(ref1)
		require.NoError(t, err)
		require.Equal(t, payload1[:4096], got)

		require.NoError(t, rv.Close())
		require.Equal(t, int32(1), pack1RemoveAttempts.Load())
		require.Equal(t, uint64(20480), disk.PackPhysicalBytes())

		insp, err := disk.packs.GCInspectionSnapshot(ctx)
		require.NoError(t, err)
		require.NotContains(t, insp.SealedSizes, ref1.PackID)

		disk.packs.filesMu.RLock()
		_, cachedStillPresent := disk.packs.readFiles[ref1.PackID]
		disk.packs.filesMu.RUnlock()
		require.True(t, cachedStillPresent)

		failPack1.Store(false)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead2.bin", false))

		stats2, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, stats2.PacksDeleted)
		require.Equal(t, int32(2), pack1RemoveAttempts.Load())
		require.Equal(t, uint64(4096), disk.PackPhysicalBytes())

		_, statErr := os.Stat(packFilePath(disk.packs.dir, ref1.PackID))
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("closeResources force-drains retired packs and joins unlink error", func(t *testing.T) {
		t.Parallel()
		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "ebr-close-drain-err",
			MaxBytes:    1 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)

		dead, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, dead.Ino, 0, bytes.Repeat([]byte{0x44}, 8192))
		require.NoError(t, err)

		seal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, seal.Ino, 0, bytes.Repeat([]byte{0x55}, 4096))
		require.NoError(t, err)

		rv := disk.openReadView()
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

		closeUnlinkErr := errors.New("persistent unlink failure at close")
		disk.packs.removePackHook = func(packID uint64) error {
			if packID == 1 {
				return closeUnlinkErr
			}
			return nil
		}

		stats, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, stats.PacksDeleted)

		require.NoError(t, rv.Close())
		require.ErrorIs(t, disk.Close(), closeUnlinkErr)
	})
}

func TestGC_FlushWALForGC_CoalescesZeroLivePacksAndZeroFlushesWhenNoWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         t.TempDir(),
		DiskID:      "gc-wal-fence-coalesce",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	var flushCalls atomic.Int32
	disk.node.flushWALForGCHook = func() error {
		flushCalls.Add(1)
		return nil
	}

	statsEmpty, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Zero(t, statsEmpty.PacksDeleted)
	require.Zero(t, flushCalls.Load())

	for i := range 3 {
		name := fmt.Sprintf("dead-%d.bin", i)
		f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, name, 0o644, 0, 0)
		require.NoError(t, cErr)
		_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{byte(i + 1)}, 8192))
		require.NoError(t, wErr)
	}

	liveFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "live.bin", 0o644, 0, 0)
	require.NoError(t, err)
	livePayload := bytes.Repeat([]byte{0x77}, 4096)
	_, err = disk.Write(ctx, DefaultBranchName, liveFile.Ino, 0, livePayload)
	require.NoError(t, err)

	deadHalf, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead-half.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, deadHalf.Ino, 0, bytes.Repeat([]byte{0x88}, 4096))
	require.NoError(t, err)

	sealFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, sealFile.Ino, 0, bytes.Repeat([]byte{0x99}, 8192))
	require.NoError(t, err)

	for i := range 3 {
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, fmt.Sprintf("dead-%d.bin", i), false))
	}
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead-half.bin", false))

	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, stats.PacksDeleted)
	require.Equal(t, 1, stats.BlocksRelocated)
	require.Zero(t, stats.BlocksSkipped)
	require.Equal(t, int32(1), flushCalls.Load())

	flushCalls.Store(0)
	statsNoOp, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Zero(t, statsNoOp.PacksDeleted)
	require.Zero(t, flushCalls.Load())

	gotLive, err := disk.Read(ctx, DefaultBranchName, liveFile.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, livePayload, gotLive)
}

func TestGC_FlushWALForGC_FlushesOnSkippedRelocationAndNewlySealedZeroLiveActivePack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("zero-live newly sealed active pack flushes WAL before retire", func(t *testing.T) {
		t.Parallel()
		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-wal-fence-active-zero",
			MaxBytes:    1 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		f, err := disk.Create(ctx, DefaultBranchName, RootIno, "active-dead.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{0x5a}, 4096))
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "active-dead.bin", false))

		var flushCalls atomic.Int32
		disk.node.flushWALForGCHook = func() error {
			flushCalls.Add(1)
			return nil
		}

		stats, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, stats.PacksDeleted)
		require.Equal(t, uint64(4096), stats.BytesReclaimed)
		require.Equal(t, int32(1), flushCalls.Load())
		require.Zero(t, disk.PackPhysicalBytes())
	})

	t.Run("relocatePack with skipped > 0 flushes WAL before retire", func(t *testing.T) {
		t.Parallel()
		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-wal-fence-skipped",
			MaxBytes:    1 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		fLive, err := disk.Create(ctx, DefaultBranchName, RootIno, "live.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, fLive.Ino, 0, bytes.Repeat([]byte{0x10}, 4096))
		require.NoError(t, err)

		fDead, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, fDead.Ino, 0, bytes.Repeat([]byte{0x20}, 4096))
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

		fSeal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, fSeal.Ino, 0, bytes.Repeat([]byte{0x30}, 8192))
		require.NoError(t, err)

		var flushCalls atomic.Int32
		disk.node.flushWALForGCHook = func() error {
			flushCalls.Add(1)
			return nil
		}

		overwritten := false
		disk.beforeGCRelocateHook = func(_ uint64, loc BlockRefLocation) error {
			if !overwritten && loc.Ino == fLive.Ino {
				overwritten = true
				_, wErr := disk.Write(ctx, DefaultBranchName, fLive.Ino, 0, bytes.Repeat([]byte{0x99}, 4096))
				return wErr
			}
			return nil
		}

		stats, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.True(t, overwritten)
		require.Equal(t, 1, stats.BlocksSkipped)
		require.Equal(t, 1, stats.PacksDeleted)
		require.Equal(t, int32(1), flushCalls.Load())
	})
}

func TestGC_FlushWALForGC_FailClosedRetiresZeroAffectedPacks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("fail closed on zero-live sealed packs", func(t *testing.T) {
		t.Parallel()
		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-wal-failclosed-sealed",
			MaxBytes:    1 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		for i := range 2 {
			f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("d-%d.bin", i), 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{byte(i + 1)}, 8192))
			require.NoError(t, wErr)
			require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, fmt.Sprintf("d-%d.bin", i), false))
		}
		seal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, seal.Ino, 0, bytes.Repeat([]byte{0x77}, 4096))
		require.NoError(t, err)

		flushErr := errors.New("injected wal flush error")
		disk.node.flushWALForGCHook = func() error {
			return flushErr
		}

		stats, err := disk.RunGC(ctx)
		require.ErrorIs(t, err, flushErr)
		require.Zero(t, stats.PacksDeleted)
		require.Zero(t, stats.BytesReclaimed)
		require.Equal(t, uint64(20480), disk.PackPhysicalBytes())

		_, statErr := os.Stat(packFilePath(disk.packs.dir, 1))
		require.NoError(t, statErr)
		_, statErr = os.Stat(packFilePath(disk.packs.dir, 2))
		require.NoError(t, statErr)

		disk.node.flushWALForGCHook = nil
		statsRetry, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, statsRetry.PacksDeleted)
		require.Equal(t, uint64(4096), disk.PackPhysicalBytes())
	})

	t.Run("fail closed on skipped relocation", func(t *testing.T) {
		t.Parallel()
		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-wal-failclosed-skipped",
			MaxBytes:    1 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		fLive, err := disk.Create(ctx, DefaultBranchName, RootIno, "live.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, fLive.Ino, 0, bytes.Repeat([]byte{0x11}, 4096))
		require.NoError(t, err)

		fDead, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, fDead.Ino, 0, bytes.Repeat([]byte{0x22}, 4096))
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

		fSeal, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, fSeal.Ino, 0, bytes.Repeat([]byte{0x33}, 8192))
		require.NoError(t, err)

		flushErr := errors.New("injected skipped wal flush error")
		disk.node.flushWALForGCHook = func() error {
			return flushErr
		}
		overwritten := false
		disk.beforeGCRelocateHook = func(_ uint64, loc BlockRefLocation) error {
			if !overwritten && loc.Ino == fLive.Ino {
				overwritten = true
				_, wErr := disk.Write(ctx, DefaultBranchName, fLive.Ino, 0, bytes.Repeat([]byte{0x99}, 4096))
				return wErr
			}
			return nil
		}

		stats, err := disk.RunGC(ctx)
		require.ErrorIs(t, err, flushErr)
		require.Equal(t, 1, stats.BlocksSkipped)
		require.Zero(t, stats.PacksDeleted)
		_, statErr := os.Stat(packFilePath(disk.packs.dir, 1))
		require.NoError(t, statErr)
	})

	t.Run("fail closed on zero-live newly sealed active pack", func(t *testing.T) {
		t.Parallel()
		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-wal-failclosed-active",
			MaxBytes:    1 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		f, err := disk.Create(ctx, DefaultBranchName, RootIno, "active-only.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{0x44}, 4096))
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "active-only.bin", false))

		flushErr := errors.New("injected active wal flush error")
		disk.node.flushWALForGCHook = func() error {
			return flushErr
		}

		stats, err := disk.RunGC(ctx)
		require.ErrorIs(t, err, flushErr)
		require.Zero(t, stats.PacksDeleted)
		require.Equal(t, uint64(4096), disk.PackPhysicalBytes())
		_, statErr := os.Stat(packFilePath(disk.packs.dir, 1))
		require.NoError(t, statErr)

		disk.node.flushWALForGCHook = nil
		require.NoError(t, disk.node.FlushWALForGC(ctx))
		statsRetry, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, statsRetry.PacksDeleted)
		require.Zero(t, disk.PackPhysicalBytes())
	})
}

func TestGC_FlushWALForGC_CrashReopenCleansUpRetiredPacksWithoutDataLoss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "gc-crash-reopen",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)

	dead, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, dead.Ino, 0, bytes.Repeat([]byte{0xaa}, 8192))
	require.NoError(t, err)

	livePayload := bytes.Repeat([]byte{0xbb}, 4096)
	live, err := disk.Create(ctx, DefaultBranchName, RootIno, "live.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, live.Ino, 0, livePayload)
	require.NoError(t, err)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

	simUnlinkErr := errors.New("simulated crash before unlink")
	disk.packs.removePackHook = func(packID uint64) error {
		if packID == 1 {
			return simUnlinkErr
		}
		return nil
	}

	stats, err := disk.RunGC(ctx)
	require.ErrorIs(t, err, simUnlinkErr)
	require.Equal(t, 1, stats.PacksDeleted)

	require.ErrorIs(t, disk.Close(), simUnlinkErr)

	reopened, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	require.Equal(t, uint64(12288), reopened.PackPhysicalBytes())

	_, _, err = reopened.Lookup(ctx, DefaultBranchName, RootIno, "dead.bin")
	require.ErrorIs(t, err, ErrNotFound)

	gotLive, err := reopened.Read(ctx, DefaultBranchName, live.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, livePayload, gotLive)

	statsAfter, err := reopened.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, statsAfter.PacksDeleted)
	require.Equal(t, uint64(8192), statsAfter.BytesReclaimed)
	require.Equal(t, uint64(4096), reopened.PackPhysicalBytes())

	_, statErr := os.Stat(packFilePath(reopened.packs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestSetAttr_WriteWithAttr_Fallocate_RMWPinLifetimeAndNoPrematureClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         t.TempDir(),
		DiskID:      "rmw-pin-lifetime",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "rmw.bin", 0o644, 0, 0)
	require.NoError(t, err)
	initial := bytes.Repeat([]byte{0x41}, 4096)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, initial)
	require.NoError(t, err)

	var (
		appendHookCalls atomic.Int32
		maxRefsInAppend atomic.Int64
		maxRefsInCommit atomic.Int64
	)
	origOnDirty := disk.packs.onDirty
	disk.packs.onDirty = func() {
		appendHookCalls.Add(1)
		refs := disk.ebr.cur.Load().refs.Load()
		for {
			prev := maxRefsInAppend.Load()
			if refs <= prev || maxRefsInAppend.CompareAndSwap(prev, refs) {
				break
			}
		}
		if origOnDirty != nil {
			origOnDirty()
		}
	}
	disk.walCommitHook = func(_ bool) {
		refs := disk.ebr.cur.Load().refs.Load()
		for {
			prev := maxRefsInCommit.Load()
			if refs <= prev || maxRefsInCommit.CompareAndSwap(prev, refs) {
				break
			}
		}
	}

	_, _, err = disk.WriteWithAttr(ctx, DefaultBranchName, f.Ino, 100, []byte("partial-write"))
	require.NoError(t, err)

	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 200, 100, FallocZeroRange))

	newSize := uint64(1500)
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Size: &newSize})
	require.NoError(t, err)

	require.Equal(t, int32(3), appendHookCalls.Load())
	require.Equal(t, int64(1), maxRefsInAppend.Load())
	require.Equal(t, int64(1), maxRefsInCommit.Load())

	got, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, int(newSize))
	require.NoError(t, err)
	require.Len(t, got, int(newSize))
	require.Equal(t, []byte("partial-write"), got[100:113])
	require.Equal(t, make([]byte, 100), got[200:300])
	require.Equal(t, bytes.Repeat([]byte{0x41}, 100), got[0:100])
}

func TestGC_ConcurrentWithAllBlockReadingPaths_NoENOENT(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         t.TempDir(),
		DiskID:      "gc-all-10-paths",
		MaxBytes:    64 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	const blockSize = 4096

	commitFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "committed.bin", 0o644, 0, 0)
	require.NoError(t, err)
	commitV1 := bytes.Repeat([]byte{0x11}, blockSize)
	_, err = disk.Write(ctx, DefaultBranchName, commitFile.Ino, 0, commitV1)
	require.NoError(t, err)

	deadInit, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead-init.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, deadInit.Ino, 0, bytes.Repeat([]byte{0xee}, blockSize))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead-init.bin", false))

	c1, err := disk.Commit(ctx, DefaultBranchName, "c1")
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork feat")
	require.NoError(t, err)

	commitV2 := bytes.Repeat([]byte{0x22}, blockSize)
	_, err = disk.Write(ctx, "feat", commitFile.Ino, 0, commitV2)
	require.NoError(t, err)
	cFeat, err := disk.Commit(ctx, "feat", "feat-modify")
	require.NoError(t, err)

	commitMainV2 := bytes.Repeat([]byte{0x33}, blockSize)
	_, err = disk.Write(ctx, DefaultBranchName, commitFile.Ino, 0, commitMainV2)
	require.NoError(t, err)
	c2, err := disk.Commit(ctx, DefaultBranchName, "main-modify")
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "work-branch", "fork work")
	require.NoError(t, err)

	workDiffFile, err := disk.Create(ctx, "work-branch", RootIno, "work-diff.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "work-branch", workDiffFile.Ino, 0, bytes.Repeat([]byte{0x44}, blockSize))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "work-branch", "work-diff-base")
	require.NoError(t, err)
	_, err = disk.Write(ctx, "work-branch", workDiffFile.Ino, 0, bytes.Repeat([]byte{0x45}, blockSize))
	require.NoError(t, err)

	readFile, err := disk.Create(ctx, "work-branch", RootIno, "read.bin", 0o644, 0, 0)
	require.NoError(t, err)
	readExpected := bytes.Repeat([]byte{0x55}, 4*blockSize)
	_, err = disk.Write(ctx, "work-branch", readFile.Ino, 0, readExpected)
	require.NoError(t, err)

	rmwWriteFile, err := disk.Create(ctx, "work-branch", RootIno, "rmw-write.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "work-branch", rmwWriteFile.Ino, 0, bytes.Repeat([]byte{0x66}, blockSize))
	require.NoError(t, err)

	rmwFallocFile, err := disk.Create(ctx, "work-branch", RootIno, "rmw-falloc.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "work-branch", rmwFallocFile.Ino, 0, bytes.Repeat([]byte{0x77}, blockSize))
	require.NoError(t, err)

	rmwSetAttrFile, err := disk.Create(ctx, "work-branch", RootIno, "rmw-setattr.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "work-branch", rmwSetAttrFile.Ino, 0, bytes.Repeat([]byte{0x88}, blockSize))
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "churn-branch", "churn")
	require.NoError(t, err)

	var (
		stop    atomic.Bool
		errMu   sync.Mutex
		first   error
		workers sync.WaitGroup
	)
	recordErr := func(pathName string, e error) {
		if e == nil {
			return
		}
		errMu.Lock()
		if first == nil {
			first = fmt.Errorf("%s: %w", pathName, e)
		}
		errMu.Unlock()
		stop.Store(true)
	}

	workers.Go(func() {
		for !stop.Load() {
			got, rErr := disk.Read(ctx, "work-branch", readFile.Ino, 0, len(readExpected))
			if rErr != nil {
				recordErr("Read", rErr)
				return
			}
			if !bytes.Equal(got, readExpected) {
				recordErr("Read", errors.New("payload mismatch"))
				return
			}
		}
	})

	workers.Go(func() {
		for !stop.Load() {
			got, rErr := disk.ReadAtCommit(ctx, c1.ID, commitFile.Ino, 0, blockSize)
			if rErr != nil {
				recordErr("ReadAtCommit", rErr)
				return
			}
			if !bytes.Equal(got, commitV1) {
				recordErr("ReadAtCommit", errors.New("payload mismatch"))
				return
			}
		}
	})

	workers.Go(func() {
		for !stop.Load() {
			res, rErr := disk.ReadCommitNode(ctx, cFeat.ID, commitFile.Ino, 0, blockSize)
			if rErr != nil {
				recordErr("ReadCommitNode", rErr)
				return
			}
			if !bytes.Equal(res.Data, commitV2) {
				recordErr("ReadCommitNode", errors.New("payload mismatch"))
				return
			}
		}
	})

	workers.Go(func() {
		for i := 0; !stop.Load(); i++ {
			_, _, wErr := disk.WriteWithAttr(ctx, "work-branch", rmwWriteFile.Ino, 128, []byte{byte(i%251 + 1)})
			if wErr != nil {
				recordErr("WriteWithAttr", wErr)
				return
			}
		}
	})

	workers.Go(func() {
		for i := 0; !stop.Load(); i++ {
			if _, _, wErr := disk.WriteWithAttr(ctx, "work-branch", rmwFallocFile.Ino, 0, bytes.Repeat([]byte{byte(i%250 + 1)}, blockSize)); wErr != nil {
				recordErr("Fallocate-seed", wErr)
				return
			}
			if fErr := disk.Fallocate(ctx, "work-branch", rmwFallocFile.Ino, 64, 128, FallocZeroRange); fErr != nil {
				recordErr("Fallocate", fErr)
				return
			}
		}
	})

	workers.Go(func() {
		for i := 0; !stop.Load(); i++ {
			fill := byte(i%249 + 1)
			if _, _, wErr := disk.WriteWithAttr(ctx, "work-branch", rmwSetAttrFile.Ino, 0, bytes.Repeat([]byte{fill}, blockSize)); wErr != nil {
				recordErr("SetAttr-seed", wErr)
				return
			}
			truncSize := uint64(1024 + (i % 512))
			if _, sErr := disk.SetAttr(ctx, "work-branch", rmwSetAttrFile.Ino, SetAttrReq{Size: &truncSize}); sErr != nil {
				recordErr("SetAttr", sErr)
				return
			}
			got, rErr := disk.Read(ctx, "work-branch", rmwSetAttrFile.Ino, 0, int(truncSize))
			if rErr != nil {
				recordErr("SetAttr-verify", rErr)
				return
			}
			if !bytes.Equal(got, bytes.Repeat([]byte{fill}, int(truncSize))) {
				recordErr("SetAttr-verify", errors.New("truncated tail payload mismatch"))
				return
			}
		}
	})

	workers.Go(func() {
		for !stop.Load() {
			res, dErr := disk.Diff(ctx, DiffOptions{
				SourceBranch: "feat",
				TargetBranch: DefaultBranchName,
			})
			if dErr != nil {
				recordErr("Diff", dErr)
				return
			}
			if res.Summary.Modified == 0 {
				recordErr("Diff", errors.New("expected modified file in diff"))
				return
			}
		}
	})

	workers.Go(func() {
		for !stop.Load() {
			res, dErr := disk.DiffCommits(ctx, DiffCommitsOptions{
				BaseCommitID:   c1.ID,
				SourceCommitID: c2.ID,
			})
			if dErr != nil {
				recordErr("DiffCommits", dErr)
				return
			}
			if res.Summary.Modified == 0 {
				recordErr("DiffCommits", errors.New("expected modified file in commit diff"))
				return
			}
		}
	})

	workers.Go(func() {
		for !stop.Load() {
			res, dErr := disk.DiffWork(ctx, "work-branch", DiffWorkOptions{})
			if dErr != nil {
				recordErr("DiffWork", dErr)
				return
			}
			if res.Summary.Modified == 0 {
				recordErr("DiffWork", errors.New("expected modified file in work diff"))
				return
			}
		}
	})

	workers.Go(func() {
		for !stop.Load() {
			res, mErr := disk.Merge(ctx, MergeOptions{
				SourceBranch: "feat",
				TargetBranch: DefaultBranchName,
				Strategy:     MergeStrategyTheirs,
				DryRun:       true,
			})
			if mErr != nil {
				recordErr("MergeDryRun", mErr)
				return
			}
			if res.Summary.Modified == 0 {
				recordErr("MergeDryRun", errors.New("expected modified summary in merge dry-run"))
				return
			}
		}
	})

	var totalDeleted int
	var totalRelocated int
	for round := range 25 {
		if stop.Load() {
			break
		}
		tmpName := fmt.Sprintf("churn-%d.bin", round)
		tmp, cErr := disk.Create(ctx, "churn-branch", RootIno, tmpName, 0o644, 0, 0)
		require.NoError(t, cErr)
		_, wErr := disk.Write(ctx, "churn-branch", tmp.Ino, 0, bytes.Repeat([]byte{0xcd}, 2*blockSize))
		require.NoError(t, wErr)
		require.NoError(t, disk.Unlink(ctx, "churn-branch", RootIno, tmpName, false))

		activeDeadName := fmt.Sprintf("active-dead-%d.bin", round)
		activeDead, cErr := disk.Create(ctx, "churn-branch", RootIno, activeDeadName, 0o644, 0, 0)
		require.NoError(t, cErr)
		_, wErr = disk.Write(ctx, "churn-branch", activeDead.Ino, 0, bytes.Repeat([]byte{0xef}, blockSize))
		require.NoError(t, wErr)
		require.NoError(t, disk.Unlink(ctx, "churn-branch", RootIno, activeDeadName, false))

		gcStats, gcErr := disk.RunGC(ctx)
		require.NoError(t, gcErr)
		totalDeleted += gcStats.PacksDeleted
		totalRelocated += gcStats.BlocksRelocated
	}

	stop.Store(true)
	workers.Wait()

	require.NoError(t, first)
	require.Positive(t, totalDeleted)
	require.Positive(t, totalRelocated)
}
