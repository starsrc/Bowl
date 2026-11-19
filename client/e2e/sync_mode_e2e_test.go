//go:build e2e && linux

package e2e_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func createQuotaTestDisk(t *testing.T, client storagev1connect.StorageServiceClient, maxBytes uint64) *storagev1.Disk {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-sync-quota-%d", time.Now().UnixNano()),
		MaxBytes: maxBytes,
	}))
	require.NoError(t, err)
	disk := resp.Msg.GetDisk()
	require.NotNil(t, disk)
	registerDiskCleanup(t, client, disk.GetDiskId())
	return disk
}

func requireZeroDaemonHostStageOrTmpFDs(t *testing.T, daemonPID int, scene string) {
	t.Helper()
	fdDir := fmt.Sprintf("/proc/%d/fd", daemonPID)
	entries, err := os.ReadDir(fdDir)
	require.NoError(t, err, "scene=%s fdDir=%s", scene, fdDir)
	for _, entry := range entries {
		target, readErr := os.Readlink(filepath.Join(fdDir, entry.Name()))
		if readErr != nil {
			continue
		}
		if strings.Contains(target, "bowl-stage") ||
			strings.HasSuffix(target, ".stage") ||
			strings.Contains(target, "tmpfile-") ||
			strings.HasSuffix(target, " (deleted)") {
			t.Fatalf("scene=%s pid=%d want=zero_host_stage_or_tmpfile_fd got_fd=%s->%s", scene, daemonPID, entry.Name(), target)
		}
	}
}

func openEINTR(path string, mode int, perm uint32) (int, error) {
	for {
		fd, err := unix.Open(path, mode, perm)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return fd, err
	}
}

func writeEINTR(fd int, p []byte) (int, error) {
	total := 0
	for {
		n, err := unix.Write(fd, p[total:])
		if n > 0 {
			total += n
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil || total == len(p) || n == 0 {
			return total, err
		}
	}
}

func fallocateEINTR(fd int, mode uint32, off, length int64) error {
	for {
		err := unix.Fallocate(fd, mode, off, length)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

func fsyncEINTR(fd int) error {
	for {
		err := unix.Fsync(fd)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

func linkatEINTR(olddirfd int, oldpath string, newdirfd int, newpath string, flags int) error {
	for {
		err := unix.Linkat(olddirfd, oldpath, newdirfd, newpath, flags)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

func statfsEINTR(path string, buf *unix.Statfs_t) error {
	for {
		err := unix.Statfs(path, buf)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

func closeFDGuard(fd int) func() error {
	var (
		once sync.Once
		err  error
	)
	return func() error {
		once.Do(func() {
			err = unix.Close(fd)
		})
		return err
	}
}

// TestSyncMode_CLIFlagsAndAmbientEnv_RejectsExplicitStageFlagsWithExit2AndIgnoresStageEnv verifies CLI validation and ambient stage environment variable handling under --mode=sync.
func TestSyncMode_CLIFlagsAndAmbientEnv_RejectsExplicitStageFlagsWithExit2AndIgnoresStageEnv(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	mp := t.TempDir()
	dummyID := uuid.NewString()

	// Explicit --stage-size or --stage-dir under --mode=sync or BOWL_MOUNT_MODE=sync exits with code 2.
	outFlagSize := runBowlExpectExit(t, 2, "mount", "--endpoint", endpoint, "--mode", "sync", "--stage-size", "8MiB", dummyID, mp)
	require.Contains(t, outFlagSize, "--stage-size cannot be set when --mode is sync")

	outFlagDir := runBowlExpectExit(t, 2, "mount", "--endpoint", endpoint, "--mode=sync", "--stage-dir", "/tmp/sync-forbidden", dummyID, mp)
	require.Contains(t, outFlagDir, "--stage-dir cannot be set when --mode is sync")

	t.Setenv("BOWL_MOUNT_MODE", "  SyNc  ")
	outEnvSize := runBowlExpectExit(t, 2, "mount", "--endpoint", endpoint, "--stage-size", "8MiB", dummyID, mp)
	require.Contains(t, outEnvSize, "--stage-size cannot be set when --mode is sync")

	outEnvDir := runBowlExpectExit(t, 2, "mount", "--endpoint", endpoint, "--stage-dir", "/tmp/sync-forbidden", dummyID, mp)
	require.Contains(t, outEnvDir, "--stage-dir cannot be set when --mode is sync")

	// Mounting via BOWL_MOUNT_MODE ignores invalid BOWL_STAGE_SIZE and tmpfs BOWL_STAGE_DIR values.
	diskID := createTestDisk(t, client).GetDiskId()
	unusedShmStageDir := fmt.Sprintf("/dev/shm/bowl-sync-unused-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = os.RemoveAll(unusedShmStageDir) })
	t.Setenv("BOWL_STAGE_SIZE", "1MiB")
	t.Setenv("BOWL_STAGE_DIR", unusedShmStageDir)

	logPath := filepath.Join(t.TempDir(), "mount-sync-env.log")
	mountMP, daemonPID, adminAddr, stopMount := startMountedDaemon(t, endpoint, diskID, "--log-file", logPath)
	defer stopMount()

	require.NoError(t, os.WriteFile(filepath.Join(mountMP, "env-sync.txt"), []byte("sync-env-ok"), 0o644))
	snap := fetchDebugStats(t, adminAddr)
	requireZeroDaemonHostStageOrTmpFDs(t, daemonPID, "env_sync_active")

	// Zero stage directories exist and no tmpfs warning appears in logs.
	require.NoDirExists(t, unusedShmStageDir)
	require.Zero(t, snap.Stage.CapacityBytes)
	require.Zero(t, snap.Stage.StagedCalls)
	logBytes, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.NotContains(t, string(logBytes), "stage_dir_on_tmpfs")
}

// TestSyncMode_RegularFileWrites_ExecutesSynchronouslyWithZeroStageFilesAndSurfacesENOSPCAndEBUSYOnWrite verifies synchronous passthrough writes, ENOSPC quota failure, and EBUSY fencing.
func TestSyncMode_RegularFileWrites_ExecutesSynchronouslyWithZeroStageFilesAndSurfacesENOSPCAndEBUSYOnWrite(t *testing.T) {
	client := newTestStorageClient(t)
	realEndpoint := getStorageEndpoint()

	t.Run("SynchronousPassthroughAndZeroHostStageFiles", func(t *testing.T) {
		// Given
		proxy := startControllableStorageProxy(t, realEndpoint)
		diskID := createTestDisk(t, client).GetDiskId()
		isolatedTmpDir := t.TempDir()
		t.Setenv("TMPDIR", isolatedTmpDir)

		mp, daemonPID, adminAddr, stopMount := startMountedDaemon(t, proxy.Endpoint, diskID, "--mode", "sync")
		defer stopMount()

		f, err := os.OpenFile(filepath.Join(mp, "sync-gate.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		defer func() { _ = f.Close() }()

		// When the proxy gates Write RPCs and the caller issues a 64 KiB write syscall.
		proxy.BlockWrites()
		payload64K := deterministicPayload("sync-64k", 65_536)
		type writeResult struct {
			n   int
			err error
		}
		writeDoneCh := make(chan writeResult, 1)
		go func() {
			n, wErr := writeEINTR(int(f.Fd()), payload64K)
			writeDoneCh <- writeResult{n: n, err: wErr}
		}()

		require.Eventually(t, func() bool {
			return proxy.WriteInflight() == 1
		}, 5*time.Second, 10*time.Millisecond, "scene=sync_write_inflight want=1")

		// Then write remains blocked in the kernel until the server RPC responds, and zero host stage files exist.
		select {
		case early := <-writeDoneCh:
			t.Fatalf("scene=sync_write_must_wait_for_rpc want=blocked got=returned_%d_%v", early.n, early.err)
		case <-time.After(100 * time.Millisecond):
		}
		requireZeroDaemonHostStageOrTmpFDs(t, daemonPID, "sync_write_blocked")
		requireNoDirEntries(t, isolatedTmpDir, "sync_isolated_tmpdir")

		proxy.UnblockWrites()
		res := <-writeDoneCh
		require.NoError(t, res.err)
		require.Equal(t, len(payload64K), res.n)
		require.NoError(t, f.Close())

		// A 10 MiB write persists directly and reports zero staged calls.
		bigPath := filepath.Join(mp, "sync-10m.bin")
		want10M := deterministicPayload("sync-10m-regular", 10_485_760)
		writeFullFile(t, bigPath, want10M, 524_288)

		snap := fetchDebugStats(t, adminAddr)
		require.Zero(t, snap.Stage.CapacityBytes)
		require.Zero(t, snap.Stage.StagedCalls)
		require.Zero(t, snap.Stage.StagedBytes)
		require.Positive(t, snap.Stage.PassthroughCalls)
		require.GreaterOrEqual(t, snap.Stage.PassthroughBytes, uint64(len(payload64K)+len(want10M)))

		stopMount()
		mpRO, stopRO := mountWithCleanup(t, realEndpoint, diskID, "--mode", "ro", "--direct-io")
		defer stopRO()
		require.Equal(t, sha256.Sum256(want10M), fileSHA256(t, filepath.Join(mpRO, "sync-10m.bin")))
	})

	t.Run("QuotaENOSPCReturnedDirectlyOnFailingWrite", func(t *testing.T) {
		// Given a 512 KiB quota disk mounted with --mode=sync.
		diskID := createQuotaTestDisk(t, client, 512*1024).GetDiskId()
		mp, stopMount := mountWithCleanup(t, realEndpoint, diskID, "--mode", "sync")
		defer stopMount()

		fd, err := openEINTR(filepath.Join(mp, "quota.bin"), unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o644)
		require.NoError(t, err)
		closeFD := closeFDGuard(fd)
		defer func() { _ = closeFD() }()

		// When the first 256 KiB write fits within quota and the second 512 KiB write exceeds quota.
		n1, err1 := writeEINTR(fd, deterministicPayload("quota-ok", 256*1024))
		require.NoError(t, err1)
		require.Equal(t, 256*1024, n1)

		_, err2 := writeEINTR(fd, deterministicPayload("quota-exceed", 512*1024))

		// Then the failing write syscall returns ENOSPC synchronously.
		require.ErrorIs(t, err2, syscall.ENOSPC, "scene=sync_quota_write want=ENOSPC got=%v", err2)
	})

	t.Run("StaleEpochEBUSYReturnedDirectlyOnFailingWrite", func(t *testing.T) {
		// Given writer 1 mounted with --mode=sync and an open writable file descriptor.
		diskID := createTestDisk(t, client).GetDiskId()
		mp, _, adminAddr, stopMount := startMountedDaemon(t, realEndpoint, diskID, "--mode", "sync")
		defer stopMount()

		readyBefore, _ := httpGetStatusAndBody(t, "http://"+adminAddr+"/readyz")
		require.Equal(t, http.StatusOK, readyBefore)

		fd, err := openEINTR(filepath.Join(mp, "fenced.bin"), unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o644)
		require.NoError(t, err)
		closeFD := closeFDGuard(fd)
		defer func() { _ = closeFD() }()

		n1, err1 := writeEINTR(fd, []byte("before-fence"))
		require.NoError(t, err1)
		require.Equal(t, 12, n1)

		// When a newer forced writer handshake bumps the server epoch and fences writer 1.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		preemptResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
			DiskId:   diskID,
			ReadOnly: false,
			Force:    true,
		}))
		require.NoError(t, err)
		t.Cleanup(func() {
			uCtx, uCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer uCancel()
			_, _ = client.UnmountDisk(uCtx, connect.NewRequest(&storagev1.UnmountDiskRequest{
				DiskId:  diskID,
				MountId: preemptResp.Msg.GetMountId(),
			}))
		})

		_, fenceErr := writeEINTR(fd, []byte("after-fence"))

		// Then the failing write syscall returns EBUSY synchronously and /readyz flips to 503.
		require.ErrorIs(t, fenceErr, syscall.EBUSY, "scene=sync_fenced_write want=EBUSY got=%v", fenceErr)
		readyAfter, readyAfterBody := httpGetStatusAndBody(t, "http://"+adminAddr+"/readyz")
		require.Equal(t, http.StatusServiceUnavailable, readyAfter, "body=%s", readyAfterBody)
	})
}

// TestSyncMode_TmpfileLifecycle_SupportsLargeWritesFallocateFsyncLinkatExclusiveAndUnlinkedOrphanReclaim verifies server-backed O_TMPFILE under --mode=sync.
func TestSyncMode_TmpfileLifecycle_SupportsLargeWritesFallocateFsyncLinkatExclusiveAndUnlinkedOrphanReclaim(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	diskID := createTestDisk(t, client).GetDiskId()
	mp, daemonPID, adminAddr, stopMount := startMountedDaemon(t, endpoint, diskID, "--mode", "sync")
	defer stopMount()

	baseBytes, baseInodes := fetchDiskUsage(t, client, diskID)
	var baseStatfs unix.Statfs_t
	require.NoError(t, statfsEINTR(mp, &baseStatfs))

	// Step 1 creates an unlinked 12 MiB O_TMPFILE on the server and reclaims quota on close.
	unlinkedFD, err := openEINTR(mp, unix.O_TMPFILE|unix.O_RDWR, 0o600)
	require.NoError(t, err)
	closeUnlinked := closeFDGuard(unlinkedFD)
	defer func() { _ = closeUnlinked() }()
	writeRepeatedChunksFD(t, unlinkedFD, deterministicPayload("sync-unlinked-tmp-12m", 4_194_304), 3)

	requireZeroDaemonHostStageOrTmpFDs(t, daemonPID, "sync_tmpfile_unlinked_open")
	midSnap := fetchDebugStats(t, adminAddr)
	require.Zero(t, midSnap.TmpFile.ActiveFiles, "scene=sync_tmpfile_open want=active_files_0 got=%d", midSnap.TmpFile.ActiveFiles)
	require.Zero(t, midSnap.TmpFile.ResidentBytes, "scene=sync_tmpfile_open want=resident_bytes_0 got=%d", midSnap.TmpFile.ResidentBytes)
	require.Equal(t, 1, midSnap.Queues.OrphanInodes, "scene=sync_tmpfile_open want=orphan_inodes_1 got=%d", midSnap.Queues.OrphanInodes)

	openBytes, openInodes := fetchDiskUsage(t, client, diskID)
	require.Equal(t, baseBytes+12_582_912, openBytes, "scene=sync_tmpfile_server_bytes want=%d got=%d", baseBytes+12_582_912, openBytes)
	require.Equal(t, baseInodes+1, openInodes, "scene=sync_tmpfile_server_inodes want=%d got=%d", baseInodes+1, openInodes)

	require.NoError(t, closeUnlinked())
	require.Eventually(t, func() bool {
		curBytes, curInodes := fetchDiskUsage(t, client, diskID)
		var curStatfs unix.Statfs_t
		if sfErr := statfsEINTR(mp, &curStatfs); sfErr != nil {
			return false
		}
		snap := fetchDebugStats(t, adminAddr)
		return curBytes == baseBytes &&
			curInodes == baseInodes &&
			curStatfs.Files == baseStatfs.Files &&
			curStatfs.Bfree == baseStatfs.Bfree &&
			snap.Queues.OrphanInodes == 0 &&
			snap.TmpFile.ActiveFiles == 0 &&
			snap.TmpFile.ResidentBytes == 0
	}, 5*time.Second, 20*time.Millisecond, "scene=sync_unlinked_tmpfile_reclaim want=quota_and_statfs_restored")

	// Step 2 verifies that O_TMPFILE with O_EXCL rejects linkat with ENOENT and reclaims quota on close.
	exclFD, err := openEINTR(mp, unix.O_TMPFILE|unix.O_EXCL|unix.O_RDWR, 0o600)
	require.NoError(t, err)
	closeExcl := closeFDGuard(exclFD)
	defer func() { _ = closeExcl() }()
	writeFullFD(t, exclFD, deterministicPayload("sync-excl-4m", 4_194_304), 0)

	exclLinkErr := linkatEINTR(
		unix.AT_FDCWD,
		fmt.Sprintf("/proc/self/fd/%d", exclFD),
		unix.AT_FDCWD,
		filepath.Join(mp, "excl-rejected.bin"),
		unix.AT_SYMLINK_FOLLOW,
	)
	require.ErrorIs(t, exclLinkErr, syscall.ENOENT, "scene=sync_excl_linkat want=ENOENT got=%v", exclLinkErr)
	require.NoError(t, closeExcl())

	require.Eventually(t, func() bool {
		curBytes, curInodes := fetchDiskUsage(t, client, diskID)
		return curBytes == baseBytes && curInodes == baseInodes
	}, 5*time.Second, 20*time.Millisecond, "scene=sync_excl_tmpfile_reclaim want=quota_restored")

	// Step 3 writes 8 MiB, preallocates 2 MiB, punches a 512 KiB hole, calls fsync, and promotes via linkat.
	promFD, err := openEINTR(mp, unix.O_TMPFILE|unix.O_RDWR, 0o640)
	require.NoError(t, err)
	closeProm := closeFDGuard(promFD)
	defer func() { _ = closeProm() }()

	const (
		writtenLen = 8_388_608
		prealloc   = 2_097_152
		totalLen   = 10_485_760
		holeOff    = 1_048_576
		holeLen    = 524_288
	)
	basePayload := deterministicPayload("sync-promoted-8m", writtenLen)
	wantPromoted := make([]byte, totalLen)
	copy(wantPromoted, basePayload)
	clear(wantPromoted[holeOff : holeOff+holeLen])

	writeFullFD(t, promFD, basePayload, 0)
	require.NoError(t, fallocateEINTR(promFD, 0, writtenLen, prealloc))
	require.NoError(t, fallocateEINTR(promFD, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, holeOff, holeLen))
	require.NoError(t, fsyncEINTR(promFD))

	promotedPath := filepath.Join(mp, "promoted-sync-10m.bin")
	require.NoError(t, linkatEINTR(
		unix.AT_FDCWD,
		fmt.Sprintf("/proc/self/fd/%d", promFD),
		unix.AT_FDCWD,
		promotedPath,
		unix.AT_SYMLINK_FOLLOW,
	))
	require.NoError(t, closeProm())

	require.Eventually(t, func() bool {
		snap := fetchDebugStats(t, adminAddr)
		return snap.Handles.OpenFiles == 0 && snap.Queues.OrphanInodes == 0
	}, 5*time.Second, 20*time.Millisecond, "scene=sync_promoted_close want=0_open_files_and_0_orphans")

	finalSnap := fetchDebugStats(t, adminAddr)
	require.Equal(t, uint64(1), finalSnap.TmpFile.Promotes)
	require.Zero(t, finalSnap.TmpFile.ActiveFiles)
	require.Zero(t, finalSnap.TmpFile.ResidentBytes)
	require.Zero(t, finalSnap.Stage.StagedCalls)
	require.Zero(t, finalSnap.Stage.CapacityBytes)

	afterPromBytes, afterPromInodes := fetchDiskUsage(t, client, diskID)
	require.Equal(t, baseBytes+totalLen, afterPromBytes, "scene=sync_promoted_used_bytes want=%d got=%d", baseBytes+totalLen, afterPromBytes)
	require.Equal(t, baseInodes+1, afterPromInodes, "scene=sync_promoted_used_inodes want=%d got=%d", baseInodes+1, afterPromInodes)

	// Remount with --mode=ro and --direct-io to verify the persisted 10 MiB file content.
	stopMount()
	mpRO, stopRO := mountWithCleanup(t, endpoint, diskID, "--mode", "ro", "--direct-io")
	defer stopRO()

	roFile := filepath.Join(mpRO, "promoted-sync-10m.bin")
	info, err := os.Stat(roFile)
	require.NoError(t, err)
	require.Equal(t, int64(totalLen), info.Size())
	require.Equal(t, sha256.Sum256(wantPromoted), fileSHA256(t, roFile), "scene=sync_promoted_remount_sha256")
}
