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
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpcerr"
)

func TestClientE2E_ForkDiffPatchMergeAndFUSEMountVerification(t *testing.T) {
	// Given: 通过 FUSE 挂载主盘写入基线文件 -> 卸载并 Fork 子盘 -> 双边修改制造冲突并各自新增文件
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	mainDisk := createTestDisk(t, client)
	mainID := mainDisk.GetDiskId()

	mpMain1, stopMain1 := mountWithCleanup(t, endpoint, mainID)
	require.NoError(t, os.WriteFile(filepath.Join(mpMain1, "shared.txt"), []byte("line1\nbase\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpMain1, "base.txt"), []byte("keep\n"), 0o644))
	stopMain1()

	featOut := runBowlSuccess(t, "disk", "fork", mainID, "--endpoint", endpoint, "--name", "e2e-feat-branch", "-q")
	featID := featOut[:len(featOut)-1]
	registerDiskCleanup(t, client, featID)

	mpMain2, stopMain2 := mountWithCleanup(t, endpoint, mainID)
	require.NoError(t, os.WriteFile(filepath.Join(mpMain2, "shared.txt"), []byte("line1\nmain\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpMain2, "main-only.txt"), []byte("m\n"), 0o644))
	stopMain2()

	mpFeat, stopFeat := mountWithCleanup(t, endpoint, featID)
	require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "shared.txt"), []byte("line1\nfeat\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "feat-only.txt"), []byte("f\n"), 0o644))
	stopFeat()

	// When 1: `bowl disk diff --target --patch` 检出差异、冲突标记与统一补丁输出
	diffOut := runBowlSuccess(t, "disk", "diff", featID, "--endpoint", endpoint, "--target", mainID, "--patch")
	require.Contains(t, diffOut, "shared.txt")
	require.Contains(t, diffOut, "CONTENT")
	require.Contains(t, diffOut, "+feat")

	// When 2: `bowl disk merge` 默认 Fail 策略触发 ECONFLICT 并返回退出码 1
	conflictOut := runBowlExpectExit(t, 1, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID)
	require.Contains(t, conflictOut, "ECONFLICT")
	require.Contains(t, conflictOut, "shared.txt")

	// When 3: `bowl disk merge --dry-run` 预演通过
	dryOut := runBowlSuccess(t, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID, "--strategy=ours", "--dry-run")
	require.Contains(t, dryOut, "Dry Run")

	// When 4: 使用 `--resolve /shared.txt=theirs` + `--strategy=ours` 正式合并
	mergeOut := runBowlSuccess(t, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID, "--strategy=ours", "--resolve", "/shared.txt=theirs")
	require.Contains(t, mergeOut, mainID)

	// Then: 重新挂载主盘，POSIX 文件读取确认保留 main-only.txt、新增 feat-only.txt 且 shared.txt 采用 theirs
	mpMain3, stopMain3 := mountWithCleanup(t, endpoint, mainID)
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

func TestClientE2E_MergeMountGateAndForceFencing(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("ParentReadOnlyFUSEMounted_BlocksDirectRPCWithEBUSY_AllowsDryRun", func(t *testing.T) {
		mainID := createTestDisk(t, client).GetDiskId()
		mpMain, stopMain := mountWithCleanup(t, endpoint, mainID)
		require.NoError(t, os.WriteFile(filepath.Join(mpMain, "base.txt"), []byte("v1\n"), 0o644))
		stopMain()

		featOut := runBowlSuccess(t, "disk", "fork", mainID, "--endpoint", endpoint, "--name", "e2e-ro-gate-feat", "-q")
		featID := strings.TrimSpace(featOut)
		registerDiskCleanup(t, client, featID)

		mpFeat, stopFeat := mountWithCleanup(t, endpoint, featID)
		require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "feat-ro.txt"), []byte("from-feat\n"), 0o644))
		stopFeat()

		// 父盘以只读方式在线 FUSE 挂载
		_, stopMainRO := mountWithCleanup(t, endpoint, mainID, "--ro")
		defer stopMainRO()

		// 绕过本机 CLI 直调 StorageServiceClient.MergeDisk，服务端必须拦截为 CodeAborted + EBUSY
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, rpcErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
			SourceDiskId: featID,
			TargetDiskId: mainID,
			Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		}))
		require.Error(t, rpcErr)
		require.Equal(t, connect.CodeAborted, connect.CodeOf(rpcErr))
		reason, ok := rpcerr.ExtractReason(rpcErr)
		require.True(t, ok)
		require.Equal(t, rpcerr.ReasonBusy, reason)

		// 本机 `bowl disk merge --dry-run` 在父盘 --ro 挂载态下必须正常成功
		dryOut := runBowlSuccess(t, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID, "--dry-run")
		require.Contains(t, dryOut, "Dry Run")
	})

	t.Run("ChildRWMountedBlocksMerge_UmountThenImmediateMergeSucceeds", func(t *testing.T) {
		mainID := createTestDisk(t, client).GetDiskId()
		mpMain, stopMain := mountWithCleanup(t, endpoint, mainID)
		require.NoError(t, os.WriteFile(filepath.Join(mpMain, "base.txt"), []byte("base-v1\n"), 0o644))
		stopMain()

		featOut := runBowlSuccess(t, "disk", "fork", mainID, "--endpoint", endpoint, "--name", "e2e-rw-feat", "-q")
		featID := strings.TrimSpace(featOut)
		registerDiskCleanup(t, client, featID)

		featMP := t.TempDir()
		runBowlSuccess(t, "mount", "--endpoint", endpoint, featID, featMP)
		t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", featMP).Run() })
		require.True(t, isMounted(featMP))

		const wantContent = "live-fuse-payload\n"
		require.NoError(t, os.WriteFile(filepath.Join(featMP, "feat-live.txt"), []byte(wantContent), 0o644))

		// 子盘未卸载时执行合并必须被拒绝（退出码 1）
		blockedOut := runBowlExpectExit(t, 1, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID)
		require.Contains(t, blockedOut, "bowl umount")

		// 卸载子盘后零等待立即执行 bowl disk merge
		runBowlSuccess(t, "umount", featMP)
		mergeOut := runBowlSuccess(t, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID)
		require.Contains(t, mergeOut, mainID)

		// 重新挂载父盘可完整读取子盘写入的文件内容
		mpVerify, stopVerify := mountWithCleanup(t, endpoint, mainID)
		defer stopVerify()
		got, err := os.ReadFile(filepath.Join(mpVerify, "feat-live.txt"))
		require.NoError(t, err)
		require.Equal(t, wantContent, string(got))
	})

	t.Run("SIGKILLClientWithActiveLease_BlocksWithoutForce_ForceMergeSucceeds", func(t *testing.T) {
		mainID := createTestDisk(t, client).GetDiskId()
		mpMainInit, stopMainInit := mountWithCleanup(t, endpoint, mainID)
		require.NoError(t, os.WriteFile(filepath.Join(mpMainInit, "base.txt"), []byte("base\n"), 0o644))
		stopMainInit()

		featOut := runBowlSuccess(t, "disk", "fork", mainID, "--endpoint", endpoint, "--name", "e2e-kill9-feat", "-q")
		featID := strings.TrimSpace(featOut)
		registerDiskCleanup(t, client, featID)

		mpFeat, stopFeat := mountWithCleanup(t, endpoint, featID)
		const wantContent = "after-kill9-force-merge\n"
		require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "feat-kill9.txt"), []byte(wantContent), 0o644))
		stopFeat()

		// 启动父盘挂载守护进程后使用 SIGKILL 强杀并懒卸载清理本机挂载点（服务端 30s 租约仍活跃）
		mainMP := t.TempDir()
		runBowlSuccess(t, "mount", "--endpoint", endpoint, mainID, mainMP)
		t.Cleanup(func() {
			_ = exec.Command("fusermount3", "-uz", mainMP).Run()
			_ = os.Remove(mountMetaPath(t, mainMP))
		})
		daemonPID := findBowlDaemonPID(t, mainMP)
		require.NoError(t, syscall.Kill(daemonPID, syscall.SIGKILL))
		waitForProcessExit(t, daemonPID)
		forceLazyUnmountMountpoint(t, mainMP)
		require.False(t, isMounted(mainMP))

		// 无 --force 时被服务端活跃租约拦截（退出码 1）
		blockedOut := runBowlExpectExit(t, 1, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID)
		require.Contains(t, blockedOut, "仍处于挂载状态")

		// 加 --force 立即成功合并
		forcedOut := runBowlSuccess(t, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID, "--force")
		require.Contains(t, forcedOut, mainID)

		mpVerify, stopVerify := mountWithCleanup(t, endpoint, mainID)
		defer stopVerify()
		got, err := os.ReadFile(filepath.Join(mpVerify, "feat-kill9.txt"))
		require.NoError(t, err)
		require.Equal(t, wantContent, string(got))
	})
}
