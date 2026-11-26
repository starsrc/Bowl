package engine

import (
	"bytes"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mergeCallOutcome struct {
	res *MergeResult
	err error
}

type gcCallOutcome struct {
	stats GCStats
	err   error
}

func setupSealedPackDivergedDisk(t *testing.T, diskID string, keepPayload []byte) (disk *testDisk, dir string, featKeepIno uint64) {
	t.Helper()
	dir = t.TempDir()
	ctx := context.Background()

	var err error
	disk, err = openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      diskID,
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork feat for gc-merge race")
	require.NoError(t, err)

	fKeep, err := disk.Create(ctx, "feat", RootIno, "keep.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", fKeep.Ino, 0, keepPayload)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "feat", "feat adds keep.bin")
	require.NoError(t, err)

	fDrop, err := disk.Create(ctx, DefaultBranchName, RootIno, "drop.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDrop.Ino, 0, bytes.Repeat([]byte{0xb2}, 4096))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "drop.bin", false))

	fMain, err := disk.Create(ctx, DefaultBranchName, RootIno, "main-only.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fMain.Ino, 0, bytes.Repeat([]byte{0xc3}, 4096))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "main diverges and seals pack 1")
	require.NoError(t, err)

	return disk, dir, fKeep.Ino
}

func TestGCMerge_SealedPackGCRacingWithThreeWayMerge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	keepPayload := bytes.Repeat([]byte{0xa1}, 4096)

	disk, _, featKeepIno := setupSealedPackDivergedDisk(t, "disk-gc-merge-sealed", keepPayload)

	mergeEntered := make(chan struct{})
	mergeOutcomeCh := make(chan mergeCallOutcome, 1)
	var enterOnce sync.Once
	var launchOnce sync.Once

	disk.beforeMergeExecHook = func(sourceBranch, targetBranch string) error {
		if sourceBranch == "feat" && targetBranch == DefaultBranchName {
			enterOnce.Do(func() { close(mergeEntered) })
		}
		return nil
	}

	disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
		if packID != 1 {
			return nil
		}
		launchOnce.Do(func() {
			go func() {
				res, mErr := disk.Merge(ctx, MergeOptions{
					SourceBranch: "feat",
					TargetBranch: DefaultBranchName,
				})
				mergeOutcomeCh <- mergeCallOutcome{res: res, err: mErr}
			}()
			select {
			case <-mergeEntered:
				select {
				case out := <-mergeOutcomeCh:
					mergeOutcomeCh <- out
				case <-time.After(5 * time.Second):
				}
			case <-time.After(100 * time.Millisecond):
			}
		})
		return nil
	}

	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PacksDeleted)

	var mergeOut mergeCallOutcome
	select {
	case mergeOut = <-mergeOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent Merge to complete")
	}
	require.NoError(t, mergeOut.err)
	require.NotNil(t, mergeOut.res)
	require.False(t, mergeOut.res.FastForward)

	_, statErr := os.Stat(packFilePath(disk.packs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	mainKeepEntry, _, err := disk.Lookup(ctx, DefaultBranchName, RootIno, "keep.bin")
	require.NoError(t, err)
	gotMain, err := disk.Read(ctx, DefaultBranchName, mainKeepEntry.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotMain)

	gotFeat, err := disk.Read(ctx, "feat", featKeepIno, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotFeat)
}

func TestGCMerge_InFlightThreeWayMergeSerializesRunGC(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	keepPayload := bytes.Repeat([]byte{0xd4}, 4096)

	disk, _, featKeepIno := setupSealedPackDivergedDisk(t, "disk-gc-merge-reverse", keepPayload)

	mergeEntered := make(chan struct{})
	releaseMerge := make(chan struct{})
	var enterOnce sync.Once
	disk.beforeMergeExecHook = func(sourceBranch, targetBranch string) error {
		if sourceBranch == "feat" && targetBranch == DefaultBranchName {
			enterOnce.Do(func() { close(mergeEntered) })
			<-releaseMerge
		}
		return nil
	}

	mergeOutcomeCh := make(chan mergeCallOutcome, 1)
	go func() {
		res, mErr := disk.Merge(ctx, MergeOptions{
			SourceBranch: "feat",
			TargetBranch: DefaultBranchName,
		})
		mergeOutcomeCh <- mergeCallOutcome{res: res, err: mErr}
	}()

	select {
	case <-mergeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Merge to pause in beforeMergeExecHook")
	}

	gcOutcomeCh := make(chan gcCallOutcome, 1)
	go func() {
		stats, gErr := disk.RunGC(ctx)
		gcOutcomeCh <- gcCallOutcome{stats: stats, err: gErr}
	}()

	select {
	case early := <-gcOutcomeCh:
		close(releaseMerge)
		t.Fatalf("expected RunGC to wait until in-flight Merge commits, finished early with stats=%+v err=%v", early.stats, early.err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseMerge)

	var mergeOut mergeCallOutcome
	select {
	case mergeOut = <-mergeOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Merge to complete")
	}
	require.NoError(t, mergeOut.err)
	require.NotNil(t, mergeOut.res)
	require.False(t, mergeOut.res.FastForward)

	var gcOut gcCallOutcome
	select {
	case gcOut = <-gcOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RunGC to complete")
	}
	require.NoError(t, gcOut.err)
	require.Equal(t, 1, gcOut.stats.PacksDeleted)
	require.Equal(t, 2, gcOut.stats.BlocksRelocated)

	_, statErr := os.Stat(packFilePath(disk.packs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	mainKeepEntry, _, err := disk.Lookup(ctx, DefaultBranchName, RootIno, "keep.bin")
	require.NoError(t, err)
	gotMain, err := disk.Read(ctx, DefaultBranchName, mainKeepEntry.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotMain)

	gotFeat, err := disk.Read(ctx, "feat", featKeepIno, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotFeat)
}

func TestGCMerge_ActivePackGCRacingWithThreeWayMerge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	keepPayload := bytes.Repeat([]byte{0xe5}, 4096)
	mainPayload := bytes.Repeat([]byte{0xf6}, 4096)

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-gc-merge-active",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork feat for active-pack gc-merge")
	require.NoError(t, err)

	fKeep, err := disk.Create(ctx, "feat", RootIno, "keep.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", fKeep.Ino, 0, keepPayload)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "feat", "feat adds keep.bin in active pack")
	require.NoError(t, err)

	fDrop, err := disk.Create(ctx, DefaultBranchName, RootIno, "drop.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fDrop.Ino, 0, bytes.Repeat([]byte{0x44}, 8192))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "drop.bin", false))

	fMain, err := disk.Create(ctx, DefaultBranchName, RootIno, "main-only.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, fMain.Ino, 0, mainPayload)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "main diverges in active pack")
	require.NoError(t, err)

	mergeEntered := make(chan struct{})
	gcRelocating := make(chan struct{})
	gcFinished := make(chan struct{})
	mergeOutcomeCh := make(chan mergeCallOutcome, 1)
	var mergeRacedSeal atomic.Bool
	var enterOnce sync.Once
	var sealOnce sync.Once
	var relocOnce sync.Once

	disk.beforeMergeExecHook = func(sourceBranch, targetBranch string) error {
		if sourceBranch == "feat" && targetBranch == DefaultBranchName {
			enterOnce.Do(func() { close(mergeEntered) })
			select {
			case <-gcRelocating:
			case <-gcFinished:
			case <-time.After(5 * time.Second):
			}
		}
		return nil
	}

	disk.beforeGCSealActiveHook = func(packID uint64) error {
		if packID != 1 {
			return nil
		}
		sealOnce.Do(func() {
			go func() {
				res, mErr := disk.Merge(ctx, MergeOptions{
					SourceBranch: "feat",
					TargetBranch: DefaultBranchName,
				})
				mergeOutcomeCh <- mergeCallOutcome{res: res, err: mErr}
			}()
			select {
			case <-mergeEntered:
				mergeRacedSeal.Store(true)
			case <-time.After(100 * time.Millisecond):
			}
		})
		return nil
	}

	disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
		if packID != 1 {
			return nil
		}
		relocOnce.Do(func() {
			close(gcRelocating)
			if mergeRacedSeal.Load() {
				select {
				case out := <-mergeOutcomeCh:
					mergeOutcomeCh <- out
				case <-time.After(5 * time.Second):
				}
			}
		})
		return nil
	}

	stats, gcErr := disk.RunGC(ctx)
	close(gcFinished)
	require.NoError(t, gcErr)
	require.Equal(t, 1, stats.PacksDeleted)

	var mergeOut mergeCallOutcome
	select {
	case mergeOut = <-mergeOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent Merge during active-pack GC")
	}
	require.NoError(t, mergeOut.err)
	require.NotNil(t, mergeOut.res)
	require.False(t, mergeOut.res.FastForward)
	require.False(t, mergeRacedSeal.Load())

	_, statErr := os.Stat(packFilePath(disk.packs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	mainKeepEntry, _, err := disk.Lookup(ctx, DefaultBranchName, RootIno, "keep.bin")
	require.NoError(t, err)
	gotMain, err := disk.Read(ctx, DefaultBranchName, mainKeepEntry.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotMain)

	gotFeat, err := disk.Read(ctx, "feat", fKeep.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotFeat)
}

func TestGCMerge_MergeContextCancelledWhileRunGCHoldsLock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	keepPayload := bytes.Repeat([]byte{0x5a}, 4096)

	disk, _, featKeepIno := setupSealedPackDivergedDisk(t, "disk-gc-merge-ctx-merge", keepPayload)

	gcEntered := make(chan struct{})
	releaseGC := make(chan struct{})
	var enterOnce sync.Once
	disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
		if packID == 1 {
			enterOnce.Do(func() { close(gcEntered) })
			<-releaseGC
		}
		return nil
	}

	gcOutcomeCh := make(chan gcCallOutcome, 1)
	go func() {
		stats, gErr := disk.RunGC(ctx)
		gcOutcomeCh <- gcCallOutcome{stats: stats, err: gErr}
	}()

	select {
	case <-gcEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RunGC to pause in beforeGCRelocateHook")
	}

	mergeCtx, cancelMerge := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelMerge()

	start := time.Now()
	mergeRes, mergeErr := disk.Merge(mergeCtx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	elapsed := time.Since(start)

	close(releaseGC)

	require.ErrorIs(t, mergeErr, context.DeadlineExceeded)
	require.Nil(t, mergeRes)
	require.Less(t, elapsed, time.Second)

	var gcOut gcCallOutcome
	select {
	case gcOut = <-gcOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RunGC to complete cleanly")
	}
	require.NoError(t, gcOut.err)
	require.Equal(t, 1, gcOut.stats.PacksDeleted)

	_, statErr := os.Stat(packFilePath(disk.packs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	gotFeat, err := disk.Read(ctx, "feat", featKeepIno, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotFeat)
}

func TestGCMerge_RunGCContextCancelledWhileMergeHoldsLock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	keepPayload := bytes.Repeat([]byte{0x6b}, 4096)

	disk, _, _ := setupSealedPackDivergedDisk(t, "disk-gc-merge-ctx-gc", keepPayload)

	mergeEntered := make(chan struct{})
	releaseMerge := make(chan struct{})
	var enterOnce sync.Once
	disk.beforeMergeExecHook = func(sourceBranch, targetBranch string) error {
		if sourceBranch == "feat" && targetBranch == DefaultBranchName {
			enterOnce.Do(func() { close(mergeEntered) })
			<-releaseMerge
		}
		return nil
	}

	mergeOutcomeCh := make(chan mergeCallOutcome, 1)
	go func() {
		res, mErr := disk.Merge(ctx, MergeOptions{
			SourceBranch: "feat",
			TargetBranch: DefaultBranchName,
		})
		mergeOutcomeCh <- mergeCallOutcome{res: res, err: mErr}
	}()

	select {
	case <-mergeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Merge to pause in beforeMergeExecHook")
	}

	gcCtx, cancelGC := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelGC()

	start := time.Now()
	stats, gcErr := disk.RunGC(gcCtx)
	elapsed := time.Since(start)

	close(releaseMerge)

	require.ErrorIs(t, gcErr, context.DeadlineExceeded)
	require.Zero(t, stats.PacksDeleted)
	require.Less(t, elapsed, time.Second)

	var mergeOut mergeCallOutcome
	select {
	case mergeOut = <-mergeOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Merge to complete cleanly")
	}
	require.NoError(t, mergeOut.err)
	require.NotNil(t, mergeOut.res)
	require.False(t, mergeOut.res.FastForward)

	mainKeepEntry, _, err := disk.Lookup(ctx, DefaultBranchName, RootIno, "keep.bin")
	require.NoError(t, err)
	gotMain, err := disk.Read(ctx, DefaultBranchName, mainKeepEntry.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, keepPayload, gotMain)
}

func TestGCMerge_AlreadyCancelledContextFailsFast(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	keepPayload := bytes.Repeat([]byte{0x7c}, 4096)

	disk, _, _ := setupSealedPackDivergedDisk(t, "disk-gc-merge-precancel", keepPayload)

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	for range 16 {
		mergeRes, mergeErr := disk.Merge(canceledCtx, MergeOptions{
			SourceBranch: "feat",
			TargetBranch: DefaultBranchName,
		})
		require.ErrorIs(t, mergeErr, context.Canceled)
		require.Nil(t, mergeRes)

		gcStats, gcErr := disk.RunGC(canceledCtx)
		require.ErrorIs(t, gcErr, context.Canceled)
		require.Zero(t, gcStats.PacksDeleted)
	}

	_, statErr := os.Stat(packFilePath(disk.packs.dir, 1))
	require.NoError(t, statErr)

	_, _, err := disk.Lookup(ctx, DefaultBranchName, RootIno, "keep.bin")
	require.ErrorIs(t, err, ErrNotFound)
}
