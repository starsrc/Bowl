package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type commitCallOutcome struct {
	cmt CommitMeta
	err error
}

type forkCallOutcome struct {
	cmt    CommitMeta
	branch BranchMeta
	err    error
}

func setupSealedPackOrphanDisk(t *testing.T, diskID string, orphanPayload []byte, useTmpfile bool) (disk *DiskStore, dir string, orphanIno uint64) {
	t.Helper()
	dir = t.TempDir()
	ctx := context.Background()

	var err error
	disk, err = OpenDisk(DiskOptions{
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

	if useTmpfile {
		tmpInode, createErr := disk.CreateTmpfile(ctx, DefaultBranchName, RootIno, 0o644, 0, 0, false)
		require.NoError(t, createErr)
		orphanIno = tmpInode.Ino
		_, err = disk.Write(ctx, DefaultBranchName, orphanIno, 0, orphanPayload)
		require.NoError(t, err)
	} else {
		openInode, createErr := disk.Create(ctx, DefaultBranchName, RootIno, "open-unlinked.bin", 0o644, 0, 0)
		require.NoError(t, createErr)
		orphanIno = openInode.Ino
		_, err = disk.Write(ctx, DefaultBranchName, orphanIno, 0, orphanPayload)
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "open-unlinked.bin", true))
	}

	dropInode, err := disk.Create(ctx, DefaultBranchName, RootIno, "drop.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, dropInode.Ino, 0, bytes.Repeat([]byte{0xb8}, 4096))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "drop.bin", false))

	sealInode, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, sealInode.Ino, 0, bytes.Repeat([]byte{0xc9}, 4096))
	require.NoError(t, err)

	return disk, dir, orphanIno
}

func setupActivePackOrphanDisk(t *testing.T, diskID string, orphanPayload []byte, useTmpfile bool) (disk *DiskStore, dir string, orphanIno uint64) {
	t.Helper()
	dir = t.TempDir()
	ctx := context.Background()

	var err error
	disk, err = OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      diskID,
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	if useTmpfile {
		tmpInode, createErr := disk.CreateTmpfile(ctx, DefaultBranchName, RootIno, 0o644, 0, 0, false)
		require.NoError(t, createErr)
		orphanIno = tmpInode.Ino
		_, err = disk.Write(ctx, DefaultBranchName, orphanIno, 0, orphanPayload)
		require.NoError(t, err)
	} else {
		openInode, createErr := disk.Create(ctx, DefaultBranchName, RootIno, "open-unlinked.bin", 0o644, 0, 0)
		require.NoError(t, createErr)
		orphanIno = openInode.Ino
		_, err = disk.Write(ctx, DefaultBranchName, orphanIno, 0, orphanPayload)
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "open-unlinked.bin", true))
	}

	dropInode, err := disk.Create(ctx, DefaultBranchName, RootIno, "drop.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, dropInode.Ino, 0, bytes.Repeat([]byte{0x44}, 8192))
	require.NoError(t, err)
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "drop.bin", false))

	return disk, dir, orphanIno
}

func waitForWaitingExclusive(t *testing.T, disk *DiskStore, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		disk.gcGate.mu.Lock()
		got := disk.gcGate.waitingExclusive
		disk.gcGate.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	disk.gcGate.mu.Lock()
	got := disk.gcGate.waitingExclusive
	disk.gcGate.mu.Unlock()
	t.Fatalf("timed out waiting for waitingExclusive=%d, got %d", want, got)
}

func TestGCOrphan_SealedPackGCRacingWithCommitTmpfile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tmpPayload := bytes.Repeat([]byte{0xa7}, 4096)

	disk, dir, tmpIno := setupSealedPackOrphanDisk(t, "disk-gc-orphan-commit-tmpfile", tmpPayload, true)

	commitEntered := make(chan struct{})
	commitOutcomeCh := make(chan commitCallOutcome, 1)
	var enterOnce sync.Once
	var launchOnce sync.Once

	disk.beforeCommitExecHook = func(branch string) error {
		if branch == DefaultBranchName {
			enterOnce.Do(func() { close(commitEntered) })
		}
		return nil
	}

	disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
		if packID != 1 {
			return nil
		}
		launchOnce.Do(func() {
			go func() {
				cmt, cErr := disk.Commit(ctx, DefaultBranchName, "commit racing with GC orphan relocate")
				commitOutcomeCh <- commitCallOutcome{cmt: cmt, err: cErr}
			}()
			select {
			case <-commitEntered:
				select {
				case out := <-commitOutcomeCh:
					commitOutcomeCh <- out
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

	var commitOut commitCallOutcome
	select {
	case commitOut = <-commitOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent Commit to complete")
	}
	require.NoError(t, commitOut.err)
	require.NotZero(t, commitOut.cmt.ID)

	_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	_, err = disk.Link(ctx, DefaultBranchName, tmpIno, RootIno, "promoted.bin")
	require.NoError(t, err)

	got, err := disk.Read(ctx, DefaultBranchName, tmpIno, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, tmpPayload, got)
}

func TestGCOrphan_SealedPackGCRacingWithCommitOpenUnlinked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	openPayload := bytes.Repeat([]byte{0xb3}, 4096)

	disk, dir, openIno := setupSealedPackOrphanDisk(t, "disk-gc-orphan-commit-unlinked", openPayload, false)

	commitEntered := make(chan struct{})
	commitOutcomeCh := make(chan commitCallOutcome, 1)
	var enterOnce sync.Once
	var launchOnce sync.Once

	disk.beforeCommitExecHook = func(branch string) error {
		if branch == DefaultBranchName {
			enterOnce.Do(func() { close(commitEntered) })
		}
		return nil
	}

	disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
		if packID != 1 {
			return nil
		}
		launchOnce.Do(func() {
			go func() {
				cmt, cErr := disk.Commit(ctx, DefaultBranchName, "commit racing with GC open-unlinked relocate")
				commitOutcomeCh <- commitCallOutcome{cmt: cmt, err: cErr}
			}()
			select {
			case <-commitEntered:
				select {
				case out := <-commitOutcomeCh:
					commitOutcomeCh <- out
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

	var commitOut commitCallOutcome
	select {
	case commitOut = <-commitOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent Commit to complete")
	}
	require.NoError(t, commitOut.err)
	require.NotZero(t, commitOut.cmt.ID)

	_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	got, err := disk.Read(ctx, DefaultBranchName, openIno, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, openPayload, got)
}

func TestGCOrphan_SealedPackGCRacingWithForkTmpfile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tmpPayload := bytes.Repeat([]byte{0xc5}, 4096)

	disk, dir, tmpIno := setupSealedPackOrphanDisk(t, "disk-gc-orphan-fork-tmpfile", tmpPayload, true)

	forkEntered := make(chan struct{})
	forkOutcomeCh := make(chan forkCallOutcome, 1)
	var enterOnce sync.Once
	var launchOnce sync.Once

	disk.beforeForkExecHook = func(srcBranch, newBranch string) error {
		if srcBranch == DefaultBranchName && newBranch == "feat" {
			enterOnce.Do(func() { close(forkEntered) })
		}
		return nil
	}

	disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
		if packID != 1 {
			return nil
		}
		launchOnce.Do(func() {
			go func() {
				cmt, bm, fErr := disk.Fork(ctx, DefaultBranchName, "feat", "fork racing with GC orphan relocate")
				forkOutcomeCh <- forkCallOutcome{cmt: cmt, branch: bm, err: fErr}
			}()
			select {
			case <-forkEntered:
				select {
				case out := <-forkOutcomeCh:
					forkOutcomeCh <- out
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

	var forkOut forkCallOutcome
	select {
	case forkOut = <-forkOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent Fork to complete")
	}
	require.NoError(t, forkOut.err)
	require.Equal(t, "feat", forkOut.branch.Name)

	_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	_, err = disk.Link(ctx, DefaultBranchName, tmpIno, RootIno, "promoted.bin")
	require.NoError(t, err)

	got, err := disk.Read(ctx, DefaultBranchName, tmpIno, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, tmpPayload, got)
}

func TestGCOrphan_SealedPackGCRacingWithForkOpenUnlinked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	openPayload := bytes.Repeat([]byte{0xd7}, 4096)

	disk, dir, openIno := setupSealedPackOrphanDisk(t, "disk-gc-orphan-fork-unlinked", openPayload, false)

	forkEntered := make(chan struct{})
	forkOutcomeCh := make(chan forkCallOutcome, 1)
	var enterOnce sync.Once
	var launchOnce sync.Once

	disk.beforeForkExecHook = func(srcBranch, newBranch string) error {
		if srcBranch == DefaultBranchName && newBranch == "feat" {
			enterOnce.Do(func() { close(forkEntered) })
		}
		return nil
	}

	disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
		if packID != 1 {
			return nil
		}
		launchOnce.Do(func() {
			go func() {
				cmt, bm, fErr := disk.Fork(ctx, DefaultBranchName, "feat", "fork racing with GC open-unlinked relocate")
				forkOutcomeCh <- forkCallOutcome{cmt: cmt, branch: bm, err: fErr}
			}()
			select {
			case <-forkEntered:
				select {
				case out := <-forkOutcomeCh:
					forkOutcomeCh <- out
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

	var forkOut forkCallOutcome
	select {
	case forkOut = <-forkOutcomeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent Fork to complete")
	}
	require.NoError(t, forkOut.err)
	require.Equal(t, "feat", forkOut.branch.Name)

	_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	got, err := disk.Read(ctx, DefaultBranchName, openIno, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, openPayload, got)
}

func TestGCOrphan_ActivePackGCRacingWithCommitAndFork(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		useFork    bool
		useTmpfile bool
		fillByte   byte
	}{
		{name: "CommitTmpfile", useFork: false, useTmpfile: true, fillByte: 0xe1},
		{name: "CommitOpenUnlinked", useFork: false, useTmpfile: false, fillByte: 0xe2},
		{name: "ForkTmpfile", useFork: true, useTmpfile: true, fillByte: 0xe3},
		{name: "ForkOpenUnlinked", useFork: true, useTmpfile: false, fillByte: 0xe4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			payload := bytes.Repeat([]byte{tc.fillByte}, 4096)

			disk, dir, orphanIno := setupActivePackOrphanDisk(t, "disk-gc-orphan-active-"+tc.name, payload, tc.useTmpfile)

			opEntered := make(chan struct{})
			gcRelocating := make(chan struct{})
			gcFinished := make(chan struct{})
			opErrCh := make(chan error, 1)
			var opRacedSeal atomic.Bool
			var enterOnce sync.Once
			var sealOnce sync.Once
			var relocOnce sync.Once

			waitAtHook := func() {
				enterOnce.Do(func() { close(opEntered) })
				select {
				case <-gcRelocating:
				case <-gcFinished:
				case <-time.After(5 * time.Second):
				}
			}

			if tc.useFork {
				disk.beforeForkExecHook = func(srcBranch, newBranch string) error {
					if srcBranch == DefaultBranchName && newBranch == "feat" {
						waitAtHook()
					}
					return nil
				}
			} else {
				disk.beforeCommitExecHook = func(branch string) error {
					if branch == DefaultBranchName {
						waitAtHook()
					}
					return nil
				}
			}

			disk.beforeGCSealActiveHook = func(packID uint64) error {
				if packID != 1 {
					return nil
				}
				sealOnce.Do(func() {
					go func() {
						if tc.useFork {
							_, _, fErr := disk.Fork(ctx, DefaultBranchName, "feat", "fork racing active pack GC")
							opErrCh <- fErr
							return
						}
						_, cErr := disk.Commit(ctx, DefaultBranchName, "commit racing active pack GC")
						opErrCh <- cErr
					}()
					select {
					case <-opEntered:
						opRacedSeal.Store(true)
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
					if opRacedSeal.Load() {
						select {
						case err := <-opErrCh:
							opErrCh <- err
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

			var opErr error
			select {
			case opErr = <-opErrCh:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for concurrent operation during active-pack GC")
			}
			require.NoError(t, opErr)
			require.False(t, opRacedSeal.Load())

			_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
			require.ErrorIs(t, statErr, os.ErrNotExist)

			if tc.useTmpfile {
				_, err := disk.Link(ctx, DefaultBranchName, orphanIno, RootIno, "promoted.bin")
				require.NoError(t, err)
			}

			got, err := disk.Read(ctx, DefaultBranchName, orphanIno, 0, 4096)
			require.NoError(t, err)
			require.Equal(t, payload, got)
		})
	}
}

func TestGCOrphan_InFlightCommitAndForkSerializeRunGC(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		useFork    bool
		useTmpfile bool
		fillByte   byte
	}{
		{name: "Commit", useFork: false, useTmpfile: true, fillByte: 0xf1},
		{name: "Fork", useFork: true, useTmpfile: false, fillByte: 0xf2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			payload := bytes.Repeat([]byte{tc.fillByte}, 4096)

			disk, dir, orphanIno := setupSealedPackOrphanDisk(t, "disk-gc-orphan-inflight-"+tc.name, payload, tc.useTmpfile)

			opEntered := make(chan struct{})
			releaseOp := make(chan struct{})
			var enterOnce sync.Once

			if tc.useFork {
				disk.beforeForkExecHook = func(srcBranch, newBranch string) error {
					if srcBranch == DefaultBranchName && newBranch == "feat" {
						enterOnce.Do(func() { close(opEntered) })
						<-releaseOp
					}
					return nil
				}
			} else {
				disk.beforeCommitExecHook = func(branch string) error {
					if branch == DefaultBranchName {
						enterOnce.Do(func() { close(opEntered) })
						<-releaseOp
					}
					return nil
				}
			}

			opErrCh := make(chan error, 1)
			go func() {
				if tc.useFork {
					_, _, fErr := disk.Fork(ctx, DefaultBranchName, "feat", "in-flight fork")
					opErrCh <- fErr
					return
				}
				_, cErr := disk.Commit(ctx, DefaultBranchName, "in-flight commit")
				opErrCh <- cErr
			}()

			select {
			case <-opEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for operation to pause in exec hook")
			}

			gcOutcomeCh := make(chan gcCallOutcome, 1)
			go func() {
				stats, gErr := disk.RunGC(ctx)
				gcOutcomeCh <- gcCallOutcome{stats: stats, err: gErr}
			}()

			select {
			case early := <-gcOutcomeCh:
				close(releaseOp)
				t.Fatalf("expected RunGC to wait for in-flight %s, finished early with stats=%+v err=%v", tc.name, early.stats, early.err)
			case <-time.After(100 * time.Millisecond):
			}

			close(releaseOp)

			select {
			case opErr := <-opErrCh:
				require.NoError(t, opErr)
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for %s to complete", tc.name)
			}

			var gcOut gcCallOutcome
			select {
			case gcOut = <-gcOutcomeCh:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for RunGC to complete")
			}
			require.NoError(t, gcOut.err)
			require.Equal(t, 1, gcOut.stats.PacksDeleted)
			require.Equal(t, 1, gcOut.stats.BlocksRelocated)

			_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
			require.ErrorIs(t, statErr, os.ErrNotExist)

			if tc.useTmpfile {
				_, err := disk.Link(ctx, DefaultBranchName, orphanIno, RootIno, "promoted.bin")
				require.NoError(t, err)
			}

			got, err := disk.Read(ctx, DefaultBranchName, orphanIno, 0, 4096)
			require.NoError(t, err)
			require.Equal(t, payload, got)
		})
	}
}

func TestGCOrphan_WriterPreferringAndCancelledRunGCWakesShared(t *testing.T) {
	t.Parallel()

	t.Run("WriterPreferring", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		disk, _, _ := setupSealedPackOrphanDisk(t, "disk-gc-orphan-writer-pref", bytes.Repeat([]byte{0x31}, 4096), true)

		_, _, err := disk.Fork(ctx, DefaultBranchName, "dev", "fork dev")
		require.NoError(t, err)

		mainEntered := make(chan struct{})
		releaseMain := make(chan struct{})
		devEntered := make(chan struct{})
		var mainOnce sync.Once
		var devOnce sync.Once

		var orderMu sync.Mutex
		var order []string
		record := func(event string) {
			orderMu.Lock()
			order = append(order, event)
			orderMu.Unlock()
		}

		disk.beforeCommitExecHook = func(branch string) error {
			switch branch {
			case DefaultBranchName:
				mainOnce.Do(func() { close(mainEntered) })
				<-releaseMain
				record("main-commit-release")
			case "dev":
				record("dev-commit-enter")
				devOnce.Do(func() { close(devEntered) })
			}
			return nil
		}

		var gcHookOnce sync.Once
		disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
			if packID == 1 {
				gcHookOnce.Do(func() { record("gc-relocate") })
			}
			return nil
		}

		mainCommitCh := make(chan commitCallOutcome, 1)
		go func() {
			cmt, cErr := disk.Commit(ctx, DefaultBranchName, "hold shared gate")
			mainCommitCh <- commitCallOutcome{cmt: cmt, err: cErr}
		}()

		select {
		case <-mainEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for main Commit to enter hook")
		}

		gcOutcomeCh := make(chan gcCallOutcome, 1)
		go func() {
			stats, gErr := disk.RunGC(ctx)
			gcOutcomeCh <- gcCallOutcome{stats: stats, err: gErr}
		}()

		waitForWaitingExclusive(t, disk, 1)

		devCommitCh := make(chan commitCallOutcome, 1)
		go func() {
			cmt, cErr := disk.Commit(ctx, "dev", "queued behind waiting RunGC")
			devCommitCh <- commitCallOutcome{cmt: cmt, err: cErr}
		}()

		select {
		case <-devEntered:
			close(releaseMain)
			t.Fatal("expected dev Commit to wait behind pending RunGC")
		case <-time.After(100 * time.Millisecond):
		}

		close(releaseMain)

		select {
		case out := <-mainCommitCh:
			require.NoError(t, out.err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for main Commit")
		}

		select {
		case out := <-gcOutcomeCh:
			require.NoError(t, out.err)
			require.Equal(t, 1, out.stats.PacksDeleted)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for RunGC")
		}

		select {
		case out := <-devCommitCh:
			require.NoError(t, out.err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for queued dev Commit")
		}

		orderMu.Lock()
		gotOrder := append([]string(nil), order...)
		orderMu.Unlock()
		require.Equal(t, []string{"main-commit-release", "gc-relocate", "dev-commit-enter"}, gotOrder)
	})

	t.Run("CancelledRunGCWakesShared", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		disk, _, _ := setupSealedPackOrphanDisk(t, "disk-gc-orphan-cancel-wake", bytes.Repeat([]byte{0x32}, 4096), true)

		_, _, err := disk.Fork(ctx, DefaultBranchName, "dev", "fork dev")
		require.NoError(t, err)

		mainEntered := make(chan struct{})
		releaseMain := make(chan struct{})
		devEntered := make(chan struct{})
		var mainOnce sync.Once
		var devOnce sync.Once

		disk.beforeCommitExecHook = func(branch string) error {
			switch branch {
			case DefaultBranchName:
				mainOnce.Do(func() { close(mainEntered) })
				<-releaseMain
			case "dev":
				devOnce.Do(func() { close(devEntered) })
			}
			return nil
		}

		mainCommitCh := make(chan commitCallOutcome, 1)
		go func() {
			cmt, cErr := disk.Commit(ctx, DefaultBranchName, "hold shared gate")
			mainCommitCh <- commitCallOutcome{cmt: cmt, err: cErr}
		}()

		select {
		case <-mainEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for main Commit to enter hook")
		}

		gcCtx, cancelGC := context.WithCancel(ctx)
		gcOutcomeCh := make(chan gcCallOutcome, 1)
		go func() {
			stats, gErr := disk.RunGC(gcCtx)
			gcOutcomeCh <- gcCallOutcome{stats: stats, err: gErr}
		}()

		waitForWaitingExclusive(t, disk, 1)

		devCommitCh := make(chan commitCallOutcome, 1)
		go func() {
			cmt, cErr := disk.Commit(ctx, "dev", "queued behind cancellable RunGC")
			devCommitCh <- commitCallOutcome{cmt: cmt, err: cErr}
		}()

		select {
		case <-devEntered:
			cancelGC()
			close(releaseMain)
			t.Fatal("expected dev Commit to wait behind pending RunGC before cancel")
		case <-time.After(100 * time.Millisecond):
		}

		cancelGC()

		select {
		case gcOut := <-gcOutcomeCh:
			require.ErrorIs(t, gcOut.err, context.Canceled)
			require.Zero(t, gcOut.stats.PacksDeleted)
		case <-time.After(5 * time.Second):
			close(releaseMain)
			t.Fatal("timed out waiting for cancelled RunGC")
		}

		select {
		case devOut := <-devCommitCh:
			require.NoError(t, devOut.err)
			require.NotZero(t, devOut.cmt.ID)
		case <-time.After(5 * time.Second):
			close(releaseMain)
			t.Fatal("expected queued dev Commit to wake immediately after waiting RunGC cancelled")
		}

		close(releaseMain)

		select {
		case mainOut := <-mainCommitCh:
			require.NoError(t, mainOut.err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for main Commit")
		}
	})
}

func TestGCOrphan_ContextCancellationAndAlreadyCancelledFailFast(t *testing.T) {
	t.Parallel()

	t.Run("CommitAndForkCancelledWhileRunGCHoldsExclusive", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		disk, dir, _ := setupSealedPackOrphanDisk(t, "disk-gc-orphan-ctx-shared", bytes.Repeat([]byte{0x51}, 4096), true)

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
			t.Fatal("timed out waiting for RunGC to pause")
		}

		commitCtx, cancelCommit := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancelCommit()
		start := time.Now()
		cmt, commitErr := disk.Commit(commitCtx, DefaultBranchName, "should timeout")
		require.ErrorIs(t, commitErr, context.DeadlineExceeded)
		require.Zero(t, cmt.ID)
		require.Less(t, time.Since(start), time.Second)

		forkCtx, cancelFork := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancelFork()
		start = time.Now()
		fCmt, fBranch, forkErr := disk.Fork(forkCtx, DefaultBranchName, "feat-timeout", "should timeout")
		require.ErrorIs(t, forkErr, context.DeadlineExceeded)
		require.Zero(t, fCmt.ID)
		require.Empty(t, fBranch.Name)
		require.Less(t, time.Since(start), time.Second)

		close(releaseGC)

		select {
		case gcOut := <-gcOutcomeCh:
			require.NoError(t, gcOut.err)
			require.Equal(t, 1, gcOut.stats.PacksDeleted)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for RunGC")
		}

		_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
		require.ErrorIs(t, statErr, os.ErrNotExist)

		_, getErr := disk.GetBranch("feat-timeout")
		require.ErrorIs(t, getErr, ErrNotFound)
	})

	t.Run("RunGCCancelledWhileCommitHoldsShared", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		disk, dir, _ := setupSealedPackOrphanDisk(t, "disk-gc-orphan-ctx-gc-commit", bytes.Repeat([]byte{0x52}, 4096), true)

		commitEntered := make(chan struct{})
		releaseCommit := make(chan struct{})
		var enterOnce sync.Once
		disk.beforeCommitExecHook = func(branch string) error {
			if branch == DefaultBranchName {
				enterOnce.Do(func() { close(commitEntered) })
				<-releaseCommit
			}
			return nil
		}

		commitCh := make(chan commitCallOutcome, 1)
		go func() {
			cmt, cErr := disk.Commit(ctx, DefaultBranchName, "hold shared")
			commitCh <- commitCallOutcome{cmt: cmt, err: cErr}
		}()

		select {
		case <-commitEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Commit to pause")
		}

		gcCtx, cancelGC := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancelGC()
		start := time.Now()
		stats, gcErr := disk.RunGC(gcCtx)
		elapsed := time.Since(start)

		close(releaseCommit)

		require.ErrorIs(t, gcErr, context.DeadlineExceeded)
		require.Zero(t, stats.PacksDeleted)
		require.Less(t, elapsed, time.Second)

		select {
		case out := <-commitCh:
			require.NoError(t, out.err)
			require.NotZero(t, out.cmt.ID)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Commit")
		}

		_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
		require.NoError(t, statErr)
	})

	t.Run("RunGCCancelledWhileForkHoldsShared", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		disk, dir, _ := setupSealedPackOrphanDisk(t, "disk-gc-orphan-ctx-gc-fork", bytes.Repeat([]byte{0x53}, 4096), false)

		forkEntered := make(chan struct{})
		releaseFork := make(chan struct{})
		var enterOnce sync.Once
		disk.beforeForkExecHook = func(srcBranch, newBranch string) error {
			if srcBranch == DefaultBranchName && newBranch == "feat" {
				enterOnce.Do(func() { close(forkEntered) })
				<-releaseFork
			}
			return nil
		}

		forkCh := make(chan forkCallOutcome, 1)
		go func() {
			cmt, bm, fErr := disk.Fork(ctx, DefaultBranchName, "feat", "hold shared")
			forkCh <- forkCallOutcome{cmt: cmt, branch: bm, err: fErr}
		}()

		select {
		case <-forkEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Fork to pause")
		}

		gcCtx, cancelGC := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancelGC()
		start := time.Now()
		stats, gcErr := disk.RunGC(gcCtx)
		elapsed := time.Since(start)

		close(releaseFork)

		require.ErrorIs(t, gcErr, context.DeadlineExceeded)
		require.Zero(t, stats.PacksDeleted)
		require.Less(t, elapsed, time.Second)

		select {
		case out := <-forkCh:
			require.NoError(t, out.err)
			require.Equal(t, "feat", out.branch.Name)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Fork")
		}

		_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
		require.NoError(t, statErr)
	})

	t.Run("AlreadyCancelledFailsFast", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		disk, dir, _ := setupSealedPackOrphanDisk(t, "disk-gc-orphan-precancel", bytes.Repeat([]byte{0x54}, 4096), true)

		beforeBranch, err := disk.GetBranch(DefaultBranchName)
		require.NoError(t, err)

		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		for range 16 {
			cmt, cErr := disk.Commit(canceledCtx, DefaultBranchName, "precancel commit")
			require.ErrorIs(t, cErr, context.Canceled)
			require.Zero(t, cmt.ID)

			fCmt, fBranch, fErr := disk.Fork(canceledCtx, DefaultBranchName, "feat-precancel", "precancel fork")
			require.ErrorIs(t, fErr, context.Canceled)
			require.Zero(t, fCmt.ID)
			require.Empty(t, fBranch.Name)

			gcStats, gErr := disk.RunGC(canceledCtx)
			require.ErrorIs(t, gErr, context.Canceled)
			require.Zero(t, gcStats.PacksDeleted)
		}

		afterBranch, err := disk.GetBranch(DefaultBranchName)
		require.NoError(t, err)
		require.Equal(t, beforeBranch.HeadCommitID, afterBranch.HeadCommitID)

		_, getErr := disk.GetBranch("feat-precancel")
		require.ErrorIs(t, getErr, ErrNotFound)

		_, statErr := os.Stat(packFilePath(filepath.Join(dir, "packs"), 1))
		require.NoError(t, statErr)
	})
}

func TestGCOrphan_ConcurrentCommitsAcrossBranchesDoNotBlockEachOther(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("CommitAndCommit", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		disk, err := OpenDisk(DiskOptions{
			Dir:         dir,
			DiskID:      "disk-gc-orphan-concurrent-commits",
			MaxBytes:    1 << 20,
			BlockSize:   4096,
			MaxPackSize: 64 << 10,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		_, _, err = disk.Fork(ctx, DefaultBranchName, "dev", "fork dev")
		require.NoError(t, err)

		fMain, err := disk.Create(ctx, DefaultBranchName, RootIno, "main.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, fMain.Ino, 0, bytes.Repeat([]byte{0x61}, 4096))
		require.NoError(t, err)

		fDev, err := disk.Create(ctx, "dev", RootIno, "dev.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, "dev", fDev.Ino, 0, bytes.Repeat([]byte{0x62}, 4096))
		require.NoError(t, err)

		mainEntered := make(chan struct{})
		releaseMain := make(chan struct{})
		var mainOnce sync.Once
		disk.beforeCommitExecHook = func(branch string) error {
			if branch == DefaultBranchName {
				mainOnce.Do(func() { close(mainEntered) })
				<-releaseMain
			}
			return nil
		}

		mainCommitCh := make(chan commitCallOutcome, 1)
		go func() {
			cmt, cErr := disk.Commit(ctx, DefaultBranchName, "commit main")
			mainCommitCh <- commitCallOutcome{cmt: cmt, err: cErr}
		}()

		select {
		case <-mainEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for main Commit to pause")
		}

		devCmt, devErr := disk.Commit(ctx, "dev", "commit dev while main paused")
		close(releaseMain)

		require.NoError(t, devErr)
		require.NotZero(t, devCmt.ID)

		select {
		case mainOut := <-mainCommitCh:
			require.NoError(t, mainOut.err)
			require.NotZero(t, mainOut.cmt.ID)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for main Commit")
		}
	})

	t.Run("CommitDuringInFlightMerge", func(t *testing.T) {
		t.Parallel()
		disk, _, _ := setupSealedPackDivergedDisk(t, "disk-gc-orphan-commit-during-merge", bytes.Repeat([]byte{0x63}, 4096))

		_, _, err := disk.Fork(ctx, DefaultBranchName, "dev", "fork dev")
		require.NoError(t, err)

		fDev, err := disk.Create(ctx, "dev", RootIno, "dev-only.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, "dev", fDev.Ino, 0, bytes.Repeat([]byte{0x64}, 4096))
		require.NoError(t, err)

		mergeEntered := make(chan struct{})
		releaseMerge := make(chan struct{})
		var mergeOnce sync.Once
		disk.beforeMergeExecHook = func(sourceBranch, targetBranch string) error {
			if sourceBranch == "feat" && targetBranch == DefaultBranchName {
				mergeOnce.Do(func() { close(mergeEntered) })
				<-releaseMerge
			}
			return nil
		}

		mergeCh := make(chan mergeCallOutcome, 1)
		go func() {
			res, mErr := disk.Merge(ctx, MergeOptions{
				SourceBranch: "feat",
				TargetBranch: DefaultBranchName,
			})
			mergeCh <- mergeCallOutcome{res: res, err: mErr}
		}()

		select {
		case <-mergeEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Merge to pause")
		}

		devCmt, devErr := disk.Commit(ctx, "dev", "commit dev during in-flight merge")
		close(releaseMerge)

		require.NoError(t, devErr)
		require.NotZero(t, devCmt.ID)

		select {
		case mergeOut := <-mergeCh:
			require.NoError(t, mergeOut.err)
			require.NotNil(t, mergeOut.res)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Merge")
		}
	})
}
