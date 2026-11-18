//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

var (
	statsTwoColumnPattern = regexp.MustCompile(`^(?:[a-zA-Z0-9_]+ -?\d+(?:\.\d+)?\n)+$`)
	jfsOpsTotalLineRegex  = regexp.MustCompile(`(?m)^juicefs_fuse_ops_durations_histogram_seconds_total (\d+)$`)
)

func parseStatsOpsTotal(t *testing.T, content string) uint64 {
	t.Helper()
	matches := jfsOpsTotalLineRegex.FindStringSubmatch(content)
	require.Len(t, matches, 2, "未找到 `juicefs_fuse_ops_durations_histogram_seconds_total` 统计行")
	val, err := strconv.ParseUint(matches[1], 10, 64)
	require.NoError(t, err)
	return val
}

func startMountBusyOccupier(t *testing.T, mountpoint string) func() {
	t.Helper()
	occupier := exec.Command("cat")
	occupier.Dir = mountpoint
	stdinPipe, err := occupier.StdinPipe()
	require.NoError(t, err, "创建目录占用进程标准输入管道失败")
	require.NoError(t, occupier.Start(), "启动目录占用进程失败")
	return func() {
		_ = stdinPipe.Close()
		_ = occupier.Process.Signal(syscall.SIGKILL)
		_ = occupier.Wait()
	}
}

// TestDaemonLifecycle_MountAdminStatsAndUmount 验证真实后台挂载守护进程启停、
// `mountinfo`、本地管理端口、`bowl stats` 发现与 `.stats` `sendfile`、`XDG_STATE_HOME` 日志回退及忙碌懒卸载清理。
func TestDaemonLifecycle_MountAdminStatsAndUmount(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	mountpoint := t.TempDir()

	// Given / When: 后台模式启动守护进程并开启管理端、虚拟 .stats 与调试日志
	runBowlSuccess(t, "mount", "--endpoint", endpoint, "--admin-addr", "127.0.0.1:0", "--stats-file", "--debug", disk.GetDiskId(), mountpoint)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mountpoint).Run() })

	// Then: 零延迟立即可查 mountinfo、Stat 与 Statfs，重复挂载被拦截
	require.True(t, isMounted(mountpoint), "后台挂载返回后 `mountinfo` 必须立即包含挂载点")

	var st unix.Stat_t
	require.NoError(t, unix.Stat(mountpoint, &st))
	require.Equal(t, uint64(1), st.Ino, "根目录 Inode 必须重写为 1")

	var stfs unix.Statfs_t
	require.NoError(t, unix.Statfs(mountpoint, &stfs))
	require.Equal(t, int64(4096), stfs.Bsize)
	require.Equal(t, disk.GetMaxBytes()/4096, stfs.Blocks)

	dupOut := runBowlExpectExit(t, 1, "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
	require.Contains(t, dupOut, "already mounted")

	daemonPID := findBowlDaemonPID(t, mountpoint)
	require.Positive(t, daemonPID)

	// When: 通过挂载点读写并经由管理端、bowl stats 与 sendfile 读取 .stats 观测指标
	probePath := filepath.Join(mountpoint, "admin-probe.txt")
	require.NoError(t, os.WriteFile(probePath, []byte("stats-probe"), 0o644))

	entries, err := os.ReadDir(mountpoint)
	require.NoError(t, err)
	require.Len(t, entries, 1, "`ReadDir` 不得暴露隐藏 `.stats` 文件")
	require.Equal(t, "admin-probe.txt", entries[0].Name())

	metaFile := mountMetaPath(t, mountpoint)
	metaBytes, err := os.ReadFile(metaFile)
	require.NoError(t, err, "挂载发现元数据文件必须存在")
	var meta struct {
		AdminAddr string `json:"admin_addr"`
		PID       int    `json:"pid"`
		DiskID    string `json:"disk_id"`
	}
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	require.Equal(t, daemonPID, meta.PID)
	require.Equal(t, disk.GetDiskId(), meta.DiskID)
	require.NotEmpty(t, meta.AdminAddr)

	healthCode, healthBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/healthz")
	require.Equal(t, http.StatusOK, healthCode)
	require.Contains(t, healthBody, "ok")

	readyCode, readyBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/readyz")
	require.Equal(t, http.StatusOK, readyCode)
	require.Contains(t, readyBody, "ok")

	metricsCode, metricsBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/metrics")
	require.Equal(t, http.StatusOK, metricsCode)
	require.Contains(t, metricsBody, "bowl_client_fuse_ops_total")

	statsOut := runBowlSuccess(t, "stats")
	require.Contains(t, statsOut, "Uptime:")
	require.Contains(t, statsOut, "FUSE:")

	rawOut := runBowlSuccess(t, "stats", mountpoint, "--raw")
	require.Regexp(t, statsTwoColumnPattern, rawOut)
	require.Contains(t, rawOut, "bowl_client_fuse_ops_total ")

	srcFile, err := os.Open(filepath.Join(mountpoint, ".stats"))
	require.NoError(t, err)
	dstPath := filepath.Join(t.TempDir(), "sendfile-stats.txt")
	dstFile, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	var sendOff int64
	sent, err := unix.Sendfile(int(dstFile.Fd()), int(srcFile.Fd()), &sendOff, 65536)
	require.NoError(t, srcFile.Close(), "关闭 `.stats` 句柄避免占用卸载")
	require.NoError(t, dstFile.Close())
	require.NoError(t, err, "内核 `sendfile` 读取虚拟 `.stats` 必须成功")
	require.Positive(t, sent)
	sentRaw, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.Regexp(t, statsTwoColumnPattern, string(sentRaw))
	require.Positive(t, parseStatsOpsTotal(t, string(sentRaw)))

	// When: 目录被占用时常规卸载拒绝，bowl unmount -f 懒卸载并清理元数据与端口
	cleanupOccupier := startMountBusyOccupier(t, mountpoint)
	t.Cleanup(cleanupOccupier)

	_ = runBowlExpectExit(t, 1, "umount", mountpoint)
	runBowlSuccess(t, "unmount", "-f", mountpoint)
	waitForUnmount(t, mountpoint)
	cleanupOccupier()
	waitForProcessExit(t, daemonPID)

	_, err = os.Stat(metaFile)
	require.ErrorIs(t, err, os.ErrNotExist, "卸载后挂载发现元数据必须被清理")

	_, dialErr := net.DialTimeout("tcp", meta.AdminAddr, time.Second)
	require.Error(t, dialErr, "卸载后管理端监听端口必须已关闭")

	expectedLog := fmt.Sprintf("/var/log/bowl/mount-%s.log", disk.GetDiskId())
	logFi, err := os.Stat(expectedLog)
	require.NoError(t, err, "默认系统日志文件必须存在: %s", expectedLog)
	require.Positive(t, logFi.Size())

	// When: 非 root 用户无权写 /var/log/bowl 时自动回退至 XDG_STATE_HOME
	t.Run("非特权用户日志回退至XDG_STATE_HOME", func(t *testing.T) {
		nonRootDisk := createTestDisk(t, client)
		require.NoError(t, os.MkdirAll("/var/log/bowl", 0o755))
		xdgState, mkErr := os.MkdirTemp("", "bowl-xdg-")
		require.NoError(t, mkErr)
		t.Cleanup(func() { _ = os.RemoveAll(xdgState) })
		require.NoError(t, os.Chmod(xdgState, 0o777))
		nonRootMP, mkMPErr := os.MkdirTemp("", "bowl-mount-")
		require.NoError(t, mkMPErr)
		t.Cleanup(func() { _ = os.RemoveAll(nonRootMP) })
		require.NoError(t, os.Chmod(nonRootMP, 0o777))
		t.Setenv("XDG_STATE_HOME", xdgState)

		exitCode, stdout, stderr := runAs(t, 65534, 65534, getBowlBin(),
			"mount", "--endpoint", endpoint, "--debug", nonRootDisk.GetDiskId(), nonRootMP)
		require.Zero(t, exitCode, "非 root 挂载失败: stdout=%s stderr=%s", stdout, stderr)
		t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", nonRootMP).Run() })

		nonRootPID := findBowlDaemonPID(t, nonRootMP)
		waitForMount(t, nonRootMP)

		fallbackLog := filepath.Join(xdgState, "bowl", "logs", fmt.Sprintf("mount-%s.log", nonRootDisk.GetDiskId()))
		fi, statErr := os.Stat(fallbackLog)
		require.NoError(t, statErr, "必须回退写入 `XDG_STATE_HOME` 日志: %s", fallbackLog)
		require.Positive(t, fi.Size())

		runBowlSuccess(t, "umount", nonRootMP)
		waitForProcessExit(t, nonRootPID)
	})
}

// TestDaemonLifecycle_RemountRecoveryAndKill9Delete 验证磁盘 CLI 全生命周期、
// 跨卸载重挂持久化与在线扩容可见性、本地挂载删盘拦截以及 `kill -9` 崩溃解除挂载后零等待删盘。
func TestDaemonLifecycle_RemountRecoveryAndKill9Delete(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	// Given: 通过 CLI 创建 1 GiB 磁盘
	createOut := runBowlSuccess(t, "disk", "create", "--endpoint", endpoint, "--size", "1GiB", "--name", "e2e-daemon-persist", "--json")
	var created struct {
		DiskID   string `json:"disk_id"`
		MaxBytes uint64 `json:"max_bytes"`
	}
	require.NoError(t, json.Unmarshal([]byte(createOut), &created))
	require.NotEmpty(t, created.DiskID)
	require.Equal(t, uint64(1<<30), created.MaxBytes)
	diskID := created.DiskID
	registerDiskCleanup(t, client, diskID)

	// When: 首次挂载写入 64 KiB 数据并在线扩容至 2 GiB，期间本地挂载态删盘必须被拦截
	mp1 := t.TempDir()
	runBowlSuccess(t, "mount", "--endpoint", endpoint, diskID, mp1)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mp1).Run() })
	pid1 := findBowlDaemonPID(t, mp1)

	payload := bytes.Repeat([]byte("BOWL-REMOUNT-PERSIST-"), 3200)[:65536]
	f, err := os.OpenFile(filepath.Join(mp1, "persist.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = f.Write(payload)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	updateOut := runBowlSuccess(t, "disk", "update", "--endpoint", endpoint, diskID, "--size", "2GiB", "--name", "e2e-daemon-resized", "--json")
	var updated struct {
		Name     string `json:"name"`
		MaxBytes uint64 `json:"max_bytes"`
	}
	require.NoError(t, json.Unmarshal([]byte(updateOut), &updated))
	require.Equal(t, "e2e-daemon-resized", updated.Name)
	require.Equal(t, uint64(2<<30), updated.MaxBytes)

	blockedDelOut := runBowlExpectExit(t, 1, "disk", "delete", "--endpoint", endpoint, diskID, "-y")
	require.Contains(t, blockedDelOut, "bowl umount")

	runBowlSuccess(t, "umount", mp1)
	waitForUnmount(t, mp1)
	waitForProcessExit(t, pid1)

	// Then: 重挂新挂载点，验证写入数据与扩容后配额跨守护进程重启完好恢复，随后卸载并删盘
	mp2 := t.TempDir()
	runBowlSuccess(t, "mount", "--endpoint", endpoint, diskID, mp2)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mp2).Run() })
	pid2 := findBowlDaemonPID(t, mp2)

	readBack, err := os.ReadFile(filepath.Join(mp2, "persist.bin"))
	require.NoError(t, err)
	require.Equal(t, payload, readBack)

	var stfs unix.Statfs_t
	require.NoError(t, unix.Statfs(mp2, &stfs))
	require.Equal(t, uint64(2<<30)/4096, stfs.Blocks)

	runBowlSuccess(t, "umount", mp2)
	waitForUnmount(t, mp2)
	waitForProcessExit(t, pid2)

	delOut := runBowlSuccess(t, "disk", "delete", "--endpoint", endpoint, diskID, "-y", "--json")
	var delRes struct {
		DiskID  string `json:"disk_id"`
		Deleted bool   `json:"deleted"`
	}
	require.NoError(t, json.Unmarshal([]byte(delOut), &delRes))
	require.True(t, delRes.Deleted)
	_ = runBowlExpectExit(t, 1, "disk", "get", "--endpoint", endpoint, diskID)

	// When: 守护进程被 kill -9 强杀并解除本机挂载后，无需等待会话租约即可立即删盘
	disk2 := createTestDisk(t, client)
	disk2ID := disk2.GetDiskId()
	mpKill := t.TempDir()
	metaKill := mountMetaPath(t, mpKill)
	fallbackMetaKill := filepath.Join(os.TempDir(), "bowl-mounts", filepath.Base(metaKill))
	t.Cleanup(func() {
		_ = os.Remove(metaKill)
		_ = os.Remove(fallbackMetaKill)
	})

	runBowlSuccess(t, "mount", "--endpoint", endpoint, "--admin-addr", "off", disk2ID, mpKill)
	pidKill := findBowlDaemonPID(t, mpKill)
	require.NoError(t, syscall.Kill(pidKill, syscall.SIGKILL))
	waitForProcessExit(t, pidKill)

	_ = unix.Unmount(mpKill, unix.MNT_DETACH)
	waitForUnmount(t, mpKill)
	_ = os.Remove(metaKill)
	_ = os.Remove(fallbackMetaKill)

	delStart := time.Now()
	runBowlSuccess(t, "disk", "delete", "--endpoint", endpoint, disk2ID, "-y", "--json")
	require.Less(t, time.Since(delStart), 2*time.Second, "`kill -9` 清理挂载后删盘必须零等待完成")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, getErr := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: disk2ID}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(getErr))
}

// TestDaemonLifecycle_BackgroundMountDetachesWorkingDirectoryAndResolvesRelativePaths verifies
// background mount resolves relative paths against the caller working directory, detaches cwd to /,
// allows synchronous tmpfs unmount without EBUSY, and keeps foreground mode in the caller directory.
func TestDaemonLifecycle_BackgroundMountDetachesWorkingDirectoryAndResolvesRelativePaths(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)

	baseDir := t.TempDir()
	scratchTmpfs := filepath.Join(baseDir, "scratch-tmpfs")
	require.NoError(t, os.MkdirAll(scratchTmpfs, 0o755))
	require.NoError(t, unix.Mount("tmpfs", scratchTmpfs, "tmpfs", 0, "size=16m"))
	tmpfsUnmounted := false
	t.Cleanup(func() {
		if !tmpfsUnmounted {
			_ = unix.Unmount(scratchTmpfs, unix.MNT_DETACH)
		}
	})

	mountpoint := filepath.Join(baseDir, "mnt")
	require.NoError(t, os.MkdirAll(mountpoint, 0o755))
	stageDir := filepath.Join(baseDir, "stage")
	logFile := filepath.Join(baseDir, "logs", "daemon.log")

	relMP := filepath.Join("..", "mnt")
	relStageDir := filepath.Join("..", "stage")
	relLogFile := filepath.Join("..", "logs", "daemon.log")

	// When: invoke background bowl mount from inside scratchTmpfs with relative paths outside it
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, getBowlBin(),
		"mount",
		"--endpoint", endpoint,
		"--admin-addr", "off",
		"--stage-dir", relStageDir,
		"--log-file", relLogFile,
		disk.GetDiskId(),
		relMP,
	)
	cmd.Dir = scratchTmpfs
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "bowl mount from tmpfs scratch directory failed: %s", string(out))
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mountpoint).Run() })

	// Then: daemon detaches cwd to / so tmpfs unmounts cleanly while Bowl mount remains active
	require.True(t, isMounted(mountpoint), "bowl mount must appear in mountinfo")
	metaBytes, err := os.ReadFile(mountMetaPath(t, mountpoint))
	require.NoError(t, err, "mount metadata must exist for %s", mountpoint)
	var meta struct {
		PID int `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	require.Positive(t, meta.PID)

	daemonCwd, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", meta.PID))
	require.NoError(t, err)
	require.Equal(t, "/", daemonCwd, "background daemon working directory must be /")

	require.NoError(t, unix.Unmount(scratchTmpfs, 0), "unmounting caller tmpfs must succeed without EBUSY")
	tmpfsUnmounted = true

	require.True(t, isMounted(mountpoint), "bowl mount must stay active after tmpfs unmount")
	probeFile := filepath.Join(mountpoint, "after-tmpfs-umount.txt")
	require.NoError(t, os.WriteFile(probeFile, []byte("cwd-detached-ok"), 0o644))
	gotData, err := os.ReadFile(probeFile)
	require.NoError(t, err)
	require.Equal(t, "cwd-detached-ok", string(gotData))

	logData, err := os.ReadFile(logFile)
	require.NoError(t, err, "relative --log-file must be created at caller-relative path %s", logFile)
	require.Contains(t, string(logData), `"msg":"mount_ready"`)

	stageEntries, err := os.ReadDir(stageDir)
	require.NoError(t, err)
	require.NotEmpty(t, stageEntries, "relative --stage-dir must resolve under %s", stageDir)

	runBowlSuccess(t, "umount", mountpoint)
	waitForUnmount(t, mountpoint)
	waitForProcessExit(t, meta.PID)

	t.Run("ForegroundPreservesCallerWorkingDirectory", func(t *testing.T) {
		fgDisk := createTestDisk(t, client)
		fgWorkDir := t.TempDir()
		fgMP := t.TempDir()
		fgCtx, fgCancel := context.WithTimeout(context.Background(), mountProcessTimeout)
		defer fgCancel()

		fgCmd := exec.CommandContext(fgCtx, getBowlBin(), "mount", "-f", "--endpoint", endpoint, "--admin-addr", "off", fgDisk.GetDiskId(), fgMP)
		fgCmd.Dir = fgWorkDir
		var fgStderr bytes.Buffer
		fgCmd.Stderr = &fgStderr
		require.NoError(t, fgCmd.Start())
		t.Cleanup(func() {
			if fgCmd.Process != nil && fgCmd.ProcessState == nil {
				_ = fgCmd.Process.Signal(syscall.SIGTERM)
			}
			_ = exec.Command(getBowlBin(), "umount", "-f", fgMP).Run()
		})

		waitForMount(t, fgMP, "foreground mount did not become ready: %s", fgStderr.String())
		resolvedWorkDir, evalErr := filepath.EvalSymlinks(fgWorkDir)
		require.NoError(t, evalErr)
		fgCwd, readlinkErr := os.Readlink(fmt.Sprintf("/proc/%d/cwd", fgCmd.Process.Pid))
		require.NoError(t, readlinkErr)
		require.Equal(t, resolvedWorkDir, fgCwd)

		stopBowlMountGracefully(t, fgCmd, fgMP)
	})
}

// TestDaemonLifecycle_SymlinkMountpointAndWedgedForceUmount verifies symlinked mountpoints across
// mount, duplicate-mount checks, stats, normal umount drain, data persistence, and wedged force umount.
func TestDaemonLifecycle_SymlinkMountpointAndWedgedForceUmount(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)

	realMP := filepath.Join(t.TempDir(), "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	canonRealMP, err := filepath.EvalSymlinks(realMP)
	require.NoError(t, err)

	symMP := filepath.Join(t.TempDir(), "sym-mp")
	require.NoError(t, os.Symlink(realMP, symMP))

	// When: mount through the symlinked mountpoint
	runBowlSuccess(t, "mount", "--endpoint", endpoint, "--admin-addr", "127.0.0.1:0", disk.GetDiskId(), symMP)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", canonRealMP).Run() })

	require.True(t, isMounted(canonRealMP), "mountinfo must record the canonical mountpoint")
	canonMetaPath := mountMetaPath(t, canonRealMP)
	symMetaPath := mountMetaPath(t, symMP)
	metaBytes, err := os.ReadFile(canonMetaPath)
	require.NoError(t, err, "canonical mountMeta must exist at %s", canonMetaPath)
	_, symMetaErr := os.Stat(symMetaPath)
	require.ErrorIs(t, symMetaErr, os.ErrNotExist, "symlink mountMeta path must not be created")

	var meta1 struct {
		Mountpoint string `json:"mountpoint"`
		PID        int    `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(metaBytes, &meta1))
	require.Equal(t, canonRealMP, meta1.Mountpoint)
	require.Positive(t, meta1.PID)

	// When: write a file via symlink and check duplicate mount rejection via both paths
	probePayload := []byte("symlink-persist-ok")
	require.NoError(t, os.WriteFile(filepath.Join(symMP, "symlink-probe.txt"), probePayload, 0o644))
	readViaReal, err := os.ReadFile(filepath.Join(canonRealMP, "symlink-probe.txt"))
	require.NoError(t, err)
	require.Equal(t, probePayload, readViaReal)

	dupSymOut := runBowlExpectExit(t, 1, "mount", "--endpoint", endpoint, disk.GetDiskId(), symMP)
	require.Contains(t, dupSymOut, "already mounted")
	dupRealOut := runBowlExpectExit(t, 1, "mount", "--endpoint", endpoint, disk.GetDiskId(), canonRealMP)
	require.Contains(t, dupRealOut, "already mounted")

	// When: query stats via symlink and run normal umount via symlink
	statsOut := runBowlSuccess(t, "stats", symMP)
	require.Contains(t, statsOut, "Uptime:")

	runBowlSuccess(t, "umount", symMP)
	waitForUnmount(t, canonRealMP)
	waitForProcessExit(t, meta1.PID)
	_, err = os.Stat(canonMetaPath)
	require.ErrorIs(t, err, os.ErrNotExist, "mountMeta must be removed after normal umount")

	// When: remount via symlink, verify persistence, stop the daemon with SIGSTOP, and force unmount
	runBowlSuccess(t, "mount", "--endpoint", endpoint, "--admin-addr", "off", disk.GetDiskId(), symMP)
	waitForMount(t, canonRealMP)

	metaBytes2, err := os.ReadFile(canonMetaPath)
	require.NoError(t, err)
	var meta2 struct {
		PID int `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(metaBytes2, &meta2))
	require.Positive(t, meta2.PID)

	t.Cleanup(func() {
		_ = syscall.Kill(meta2.PID, syscall.SIGCONT)
		_ = syscall.Kill(meta2.PID, syscall.SIGKILL)
		_ = exec.Command(getBowlBin(), "umount", "-f", canonRealMP).Run()
		_ = os.Remove(canonMetaPath)
	})

	persisted, err := os.ReadFile(filepath.Join(symMP, "symlink-probe.txt"))
	require.NoError(t, err)
	require.Equal(t, probePayload, persisted)

	require.NoError(t, syscall.Kill(meta2.PID, syscall.SIGSTOP))
	// Wait longer than the 1s attr cache TTL so any stat on the FUSE root would trigger FUSE_GETATTR and block.
	time.Sleep(1300 * time.Millisecond)

	forceCtx, forceCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer forceCancel()
	start := time.Now()
	forceCmd := exec.CommandContext(forceCtx, getBowlBin(), "umount", "-f", symMP)
	forceOut, forceErr := forceCmd.CombinedOutput()
	elapsed := time.Since(start)
	require.NoError(t, forceErr, "bowl umount -f %s failed in %s: %s", symMP, elapsed, string(forceOut))
	require.Less(t, elapsed, 3*time.Second)
	waitForUnmount(t, canonRealMP)

	// Resume the stopped daemon so it observes the lazy unmount and releases its lease before disk cleanup.
	require.NoError(t, syscall.Kill(meta2.PID, syscall.SIGCONT))
	waitForProcessExit(t, meta2.PID)
}
