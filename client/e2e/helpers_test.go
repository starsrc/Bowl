//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	helperProcessTimeout = 30 * time.Second

	mountProcessTimeout = 2 * time.Minute
)

func getStorageEndpoint() string {
	return cmp.Or(os.Getenv("BOWL_STORAGE_ENDPOINT"), "http://storage-service:9000")
}

func getBowlBin() string {
	return cmp.Or(os.Getenv("BOWL_BIN"), "/tmp/bowl")
}

func newTestStorageClient(t *testing.T) storagev1connect.StorageServiceClient {
	t.Helper()
	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint: getStorageEndpoint(),
	})
	require.NoError(t, err, "连接远端存储失败")
	return client
}

func createTestDisk(t *testing.T, client storagev1connect.StorageServiceClient) *storagev1.Disk {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-disk-%d", time.Now().UnixNano()),
		MaxBytes: 10 * 1024 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := resp.Msg.GetDisk()
	require.NotNil(t, disk)
	registerDiskCleanup(t, client, disk.GetDiskId())
	return disk
}

func registerDiskCleanup(t *testing.T, client storagev1connect.StorageServiceClient, diskID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.EvictMounts(ctx, connect.NewRequest(&storagev1.EvictMountsRequest{DiskId: diskID}))
		_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			require.NoError(t, err, "清理测试磁盘失败: %s", diskID)
		}
	})
}

func isMounted(mountpoint string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	target := filepath.Clean(mountpoint)
	return err == nil && strings.Contains(string(data), " "+target+" ")
}

func waitForMount(t *testing.T, mountpoint string, msgAndArgs ...any) {
	t.Helper()
	require.Eventually(t, func() bool { return isMounted(mountpoint) }, 10*time.Second, 10*time.Millisecond, msgAndArgs...)
}

func waitForUnmount(t *testing.T, mountpoint string, msgAndArgs ...any) {
	t.Helper()
	require.Eventually(t, func() bool { return !isMounted(mountpoint) }, 5*time.Second, 10*time.Millisecond, msgAndArgs...)
}

func findBowlDaemonPID(t *testing.T, mountpoint string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "pgrep", "-f", regexp.QuoteMeta(mountpoint)).Output()
	require.NoError(t, err, "未找到挂载点 %s 的后台守护进程", mountpoint)
	fields := strings.Fields(string(out))
	require.NotEmpty(t, fields)
	pid, err := strconv.Atoi(fields[0])
	require.NoError(t, err)
	return pid
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	require.Eventually(t, func() bool {
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		return err != nil || strings.Contains(string(stat), ") Z ")
	}, 5*time.Second, 10*time.Millisecond, "进程 PID %d 在超时内未退出", pid)
}

func runAs(t *testing.T, uid, gid uint32, name string, args ...string) (exitCode int, stdoutStr, stderrStr string) {
	t.Helper()
	return runAsWithStdin(t, uid, gid, "", name, args...)
}

func runAsWithStdin(t *testing.T, uid, gid uint32, stdin, name string, args ...string) (exitCode int, stdoutStr, stderrStr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: gid}}
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	require.NoError(t, ctx.Err(), "以 uid=%d 运行 %s 超时: %v", uid, name, err)
	require.NotNil(t, cmd.ProcessState, "以 uid=%d 启动 %s 失败: %v", uid, name, err)
	return cmd.ProcessState.ExitCode(), stdout.String(), stderr.String()
}

func startBowlMount(t *testing.T, endpoint, diskID, mountpoint string, extraFlags ...string) (cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
	t.Helper()
	args := []string{"mount", "-f", "--endpoint", endpoint, diskID, mountpoint}
	args = append(args, extraFlags...)

	ctx, cancel := context.WithTimeout(context.Background(), mountProcessTimeout)
	t.Cleanup(cancel)
	cmd = exec.CommandContext(ctx, getBowlBin(), args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	require.NoError(t, cmd.Start(), "启动 `bowl mount` 进程失败")
	return cmd, &outBuf, &errBuf
}

func mountWithCleanup(t *testing.T, endpoint, diskID string, extraFlags ...string) (mountpoint string, stop func()) {
	t.Helper()
	mountpoint = t.TempDir()
	cmd, _, stderr := startBowlMount(t, endpoint, diskID, mountpoint, extraFlags...)
	var once sync.Once
	stop = func() { once.Do(func() { stopBowlMountGracefully(t, cmd, mountpoint) }) }
	t.Cleanup(stop)
	waitForMount(t, mountpoint, "挂载点就绪失败: %s", stderr.String())
	return mountpoint, stop
}

func stopBowlMountGracefully(t *testing.T, cmd *exec.Cmd, mountpoint string) {
	t.Helper()
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM), "发送 SIGTERM 失败")
	require.NoError(t, cmd.Wait(), "`bowl mount` 进程应以退出码 0 干净退出")
	waitForUnmount(t, mountpoint, "卸载后 `mountinfo` 不应残留该挂载点")
}

func runBowlSuccess(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, getBowlBin(), args...).CombinedOutput()
	require.NoError(t, err, "执行 `bowl %v` 失败: %s", args, string(out))
	return string(out)
}

func runBowlExpectExit(t *testing.T, wantCode int, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, getBowlBin(), args...).CombinedOutput()
	require.Error(t, err, "预期 `bowl %v` 返回非零退出码 %d，实际成功: %s", args, wantCode, string(out))
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, wantCode, exitErr.ExitCode(), "命令输出: %s", string(out))
	return string(out)
}

func mountMetaPath(t *testing.T, mountpoint string) string {
	t.Helper()
	absMP, err := filepath.Abs(mountpoint)
	require.NoError(t, err)
	sum := sha256.Sum256([]byte(filepath.Clean(absMP)))
	return "/run/bowl/mounts/" + hex.EncodeToString(sum[:16]) + ".json"
}

func httpGetStatusAndBody(t *testing.T, targetURL string) (statusCode int, bodyText string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func deterministicPayload(tag string, size int) []byte {
	out := make([]byte, size)
	var blockIdx uint64
	seedPrefix := []byte(tag + ":")
	for off := 0; off < size; off += sha256.Size {
		var buf [128]byte
		n := copy(buf[:], seedPrefix)
		binary.BigEndian.PutUint64(buf[n:n+8], blockIdx)
		sum := sha256.Sum256(buf[:n+8])
		copy(out[off:], sum[:min(sha256.Size, size-off)])
		blockIdx++
	}
	return out
}
