//go:build e2e && linux

package e2e_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/starsrc/pkg/serverx"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestClientE2E_ForkDiffPatchMergeAndFUSEMountVerification(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	disk := createTestDisk(t, client)
	diskID := disk.GetDiskId()

	mpMain1, stopMain1 := mountWithCleanup(t, endpoint, diskID, "--branch", "main")
	require.NoError(t, os.WriteFile(filepath.Join(mpMain1, "shared.txt"), []byte("line1\nbase\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpMain1, "base.txt"), []byte("keep\n"), 0o644))
	stopMain1()

	featOut := runBowlSuccess(t, "branch", "create", "feat", "-d", diskID, "--from", "main", "--endpoint", endpoint, "-q")
	featBranch := strings.TrimSpace(featOut)
	require.Equal(t, "feat", featBranch)

	mpMain2, stopMain2 := mountWithCleanup(t, endpoint, diskID, "--branch", "main")
	require.NoError(t, os.WriteFile(filepath.Join(mpMain2, "shared.txt"), []byte("line1\nmain\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpMain2, "main-only.txt"), []byte("m\n"), 0o644))
	stopMain2()
	runBowlSuccess(t, "commit", "create", "-d", diskID, "--branch", "main", "--endpoint", endpoint, "-m", "main changes", "-q")

	mpFeat, stopFeat := mountWithCleanup(t, endpoint, diskID, "--branch", featBranch)
	require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "shared.txt"), []byte("line1\nfeat\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "feat-only.txt"), []byte("f\n"), 0o644))
	stopFeat()
	runBowlSuccess(t, "commit", "create", "-d", diskID, "--branch", featBranch, "--endpoint", endpoint, "-m", "feat changes", "-q")

	diffOut := runBowlSuccess(t, "diff", "-d", diskID, "main.."+featBranch, "--endpoint", endpoint, "--patch")
	require.Contains(t, diffOut, "shared.txt")
	require.Contains(t, diffOut, "CONTENT")
	require.Contains(t, diffOut, "+feat")

	conflictOut := runBowlExpectExit(t, 1, "branch", "merge", featBranch, "-d", diskID, "--endpoint", endpoint, "--into", "main")
	require.Contains(t, conflictOut, "ECONFLICT")
	require.Contains(t, conflictOut, "shared.txt")

	dryOut := runBowlSuccess(t, "branch", "merge", featBranch, "-d", diskID, "--endpoint", endpoint, "--into", "main", "--strategy=ours", "--dry-run")
	require.Contains(t, dryOut, "Dry Run")

	mergeOut := runBowlSuccess(t, "branch", "merge", featBranch, "-d", diskID, "--endpoint", endpoint, "--into", "main", "--strategy=ours", "--resolve", "/shared.txt=theirs")
	require.Contains(t, mergeOut, diskID)

	mpMain3, stopMain3 := mountWithCleanup(t, endpoint, diskID, "--branch", "main")
	defer stopMain3()

	gotShared, err := os.ReadFile(filepath.Join(mpMain3, "shared.txt"))
	require.NoError(t, err)
	require.Equal(t, "line1\nfeat\n", string(gotShared))

	gotFeat, err := os.ReadFile(filepath.Join(mpMain3, "feat-only.txt"))
	require.NoError(t, err)
	require.Equal(t, "f\n", string(gotFeat))

	gotMainOnly, err := os.ReadFile(filepath.Join(mpMain3, "main-only.txt"))
	require.NoError(t, err)
	require.Equal(t, "m\n", string(gotMainOnly))
}

func forceLazyUnmountMountpoint(t *testing.T, mountpoint string) {
	t.Helper()
	if _, err := exec.LookPath("fusermount3"); err == nil {
		out, umountErr := exec.Command("fusermount3", "-uz", mountpoint).CombinedOutput()
		require.NoError(t, umountErr, "fusermount3 -uz 清理挂载点失败: %s", string(out))
	} else {
		out, umountErr := exec.Command("umount", "-l", mountpoint).CombinedOutput()
		require.NoError(t, umountErr, "umount -l 清理挂载点失败: %s", string(out))
	}
	waitForUnmount(t, mountpoint, "懒卸载后 mountinfo 必须移除挂载点")
}

func TestClientE2E_MergeMountGateAndEvictFencing(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("TargetReadOnlyFUSEMounted_BlocksDirectRPCWithEBUSY_AllowsDryRun", func(t *testing.T) {
		diskID := createTestDisk(t, client).GetDiskId()
		mpMain, stopMain := mountWithCleanup(t, endpoint, diskID, "--branch", "main")
		require.NoError(t, os.WriteFile(filepath.Join(mpMain, "base.txt"), []byte("v1\n"), 0o644))
		stopMain()

		featOut := runBowlSuccess(t, "branch", "create", "ro-gate-feat", "-d", diskID, "--from", "main", "--endpoint", endpoint, "-q")
		featBranch := strings.TrimSpace(featOut)

		mpFeat, stopFeat := mountWithCleanup(t, endpoint, diskID, "--branch", featBranch)
		require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "feat-ro.txt"), []byte("from-feat\n"), 0o644))
		stopFeat()
		runBowlSuccess(t, "commit", "create", "-d", diskID, "--branch", featBranch, "--endpoint", endpoint, "-q")

		_, stopMainRO := mountWithCleanup(t, endpoint, diskID, "--branch", "main", "--mode", "ro")
		defer stopMainRO()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, rpcErr := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
			DiskId:       diskID,
			SourceBranch: featBranch,
			TargetBranch: "main",
			Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		}))
		require.Error(t, rpcErr)
		require.Equal(t, connect.CodeAborted, connect.CodeOf(rpcErr))
		info := serverx.ExtractErrorInfo(rpcErr)
		require.NotNil(t, info)
		require.Equal(t, "EBUSY", info.GetReason())

		dryOut := runBowlSuccess(t, "branch", "merge", featBranch, "-d", diskID, "--endpoint", endpoint, "--into", "main", "--dry-run")
		require.Contains(t, dryOut, "Dry Run")
	})

	t.Run("DirtyBranchBlocksMerge_CommitAndUmountThenMergeSucceeds", func(t *testing.T) {
		diskID := createTestDisk(t, client).GetDiskId()
		mpMain, stopMain := mountWithCleanup(t, endpoint, diskID, "--branch", "main")
		require.NoError(t, os.WriteFile(filepath.Join(mpMain, "base.txt"), []byte("base-v1\n"), 0o644))
		stopMain()

		featOut := runBowlSuccess(t, "branch", "create", "rw-feat", "-d", diskID, "--from", "main", "--endpoint", endpoint, "-q")
		featBranch := strings.TrimSpace(featOut)

		featMP := t.TempDir()
		runBowlSuccess(t, "mount", "--endpoint", endpoint, "--branch", featBranch, diskID, featMP)
		t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", featMP).Run() })
		require.True(t, isMounted(featMP))

		const wantContent = "live-fuse-payload\n"
		require.NoError(t, os.WriteFile(filepath.Join(featMP, "feat-live.txt"), []byte(wantContent), 0o644))

		blockedOut := runBowlExpectExit(t, 1, "branch", "merge", featBranch, "-d", diskID, "--endpoint", endpoint, "--into", "main")
		require.Contains(t, blockedOut, "EDIRTY")

		runBowlSuccess(t, "umount", featMP)
		runBowlSuccess(t, "commit", "create", "-d", diskID, "--branch", featBranch, "--endpoint", endpoint, "-q")
		mergeOut := runBowlSuccess(t, "branch", "merge", featBranch, "-d", diskID, "--endpoint", endpoint, "--into", "main")
		require.Contains(t, mergeOut, diskID)

		mpVerify, stopVerify := mountWithCleanup(t, endpoint, diskID, "--branch", "main")
		defer stopVerify()
		got, err := os.ReadFile(filepath.Join(mpVerify, "feat-live.txt"))
		require.NoError(t, err)
		require.Equal(t, wantContent, string(got))
	})

	t.Run("SIGKILLClientWithActiveLease_BlocksWithoutEvict_EvictThenMergeSucceeds", func(t *testing.T) {
		diskID := createTestDisk(t, client).GetDiskId()
		mpMainInit, stopMainInit := mountWithCleanup(t, endpoint, diskID, "--branch", "main")
		require.NoError(t, os.WriteFile(filepath.Join(mpMainInit, "base.txt"), []byte("base\n"), 0o644))
		stopMainInit()

		featOut := runBowlSuccess(t, "branch", "create", "kill9-feat", "-d", diskID, "--from", "main", "--endpoint", endpoint, "-q")
		featBranch := strings.TrimSpace(featOut)

		mpFeat, stopFeat := mountWithCleanup(t, endpoint, diskID, "--branch", featBranch)
		const wantContent = "after-kill9-evict-merge\n"
		require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "feat-kill9.txt"), []byte(wantContent), 0o644))
		stopFeat()
		runBowlSuccess(t, "commit", "create", "-d", diskID, "--branch", featBranch, "--endpoint", endpoint, "-q")

		mainMP := t.TempDir()
		runBowlSuccess(t, "mount", "--endpoint", endpoint, "--branch", "main", diskID, mainMP)
		t.Cleanup(func() {
			_ = exec.Command("fusermount3", "-uz", mainMP).Run()
			_ = os.Remove(mountMetaPath(t, mainMP))
		})
		daemonPID := findBowlDaemonPID(t, mainMP)
		require.NoError(t, syscall.Kill(daemonPID, syscall.SIGKILL))
		waitForProcessExit(t, daemonPID)
		forceLazyUnmountMountpoint(t, mainMP)
		require.False(t, isMounted(mainMP))

		blockedOut := runBowlExpectExit(t, 1, "branch", "merge", featBranch, "-d", diskID, "--endpoint", endpoint, "--into", "main")
		require.Contains(t, blockedOut, "bowl lease list")

		runBowlSuccess(t, "lease", "evict", "-d", diskID, "--branch", "main", "--endpoint", endpoint)
		mergedOut := runBowlSuccess(t, "branch", "merge", featBranch, "-d", diskID, "--endpoint", endpoint, "--into", "main")
		require.Contains(t, mergedOut, diskID)

		mpVerify, stopVerify := mountWithCleanup(t, endpoint, diskID, "--branch", "main")
		defer stopVerify()
		got, err := os.ReadFile(filepath.Join(mpVerify, "feat-kill9.txt"))
		require.NoError(t, err)
		require.Equal(t, wantContent, string(got))
	})
}
