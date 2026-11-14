//go:build e2e

package e2e_test

import (
	"bytes"
	"cmp"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	defaultStorageTestDSN   = "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"
	storageBuildTimeout     = 5 * time.Minute
	storageLifecycleTimeout = 90 * time.Second
)

// syncBuffer 提供并发安全的进程标准输出与错误日志采集缓冲。
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// storageTestDSN 返回 E2E 使用的 PostgreSQL DSN。
func storageTestDSN() string {
	return cmp.Or(os.Getenv("BOWL_TEST_POSTGRES_DSN"), defaultStorageTestDSN)
}

// buildStorageBinary 编译当前平台的 `storage-service` 二进制，供真实进程生命周期测试使用。
func buildStorageBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "storage-service")
	ctx, cancel := context.WithTimeout(context.Background(), storageBuildTimeout)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "-C", "../services/storage", "build", "-o", binary, "./cmd")
	build.Env = append(os.Environ(), "GOWORK=off")
	output, err := build.CombinedOutput()
	require.NoError(t, ctx.Err(), "编译 `storage-service` 超时: %s", output)
	require.NoError(t, err, "编译 `storage-service` 失败: %s", output)
	return binary
}

// freeTCPPort 申请一个当前可用的本机端口。
func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	port := tcpAddr.Port
	require.NoError(t, listener.Close())
	return port
}

// freeTCPPortPair 同时锁定并返回两个互不冲突的本机空闲端口。
func freeTCPPortPair(t *testing.T) (pubPort, adminPort int) {
	t.Helper()
	pubListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	adminListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	pubAddr, pubOK := pubListener.Addr().(*net.TCPAddr)
	require.True(t, pubOK)
	adminAddr, adminOK := adminListener.Addr().(*net.TCPAddr)
	require.True(t, adminOK)
	pubPort, adminPort = pubAddr.Port, adminAddr.Port
	require.NoError(t, pubListener.Close())
	require.NoError(t, adminListener.Close())
	return pubPort, adminPort
}

// storageProcessEnv 构造 `storage-service` 的完整测试环境变量。
func storageProcessEnv(publicPort, internalPort int, dataDir, dsn string, extra ...string) []string {
	base := []string{
		"SERVER_HOST=127.0.0.1",
		"SERVER_PORT=" + strconv.Itoa(publicPort),
		"INTERNAL_SERVER_HOST=127.0.0.1",
		"INTERNAL_SERVER_PORT=" + strconv.Itoa(internalPort),
		"STORAGE_DATA_DIR=" + dataDir,
		"DB_DSN=" + dsn,
	}
	return append(base, extra...)
}

// runStorageProcessExpectFailure 启动 `storage-service` 并断言其在超时内以非零退出码失败。
func runStorageProcessExpectFailure(t *testing.T, binary string, env []string) (exitCode int, out string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(append(os.Environ(), "OTEL_ENABLED=false"), env...)
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "`storage-service` 未在超时内退出: %s", output)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "`storage-service` 必须以非零退出码失败: %s", output)
	return exitErr.ExitCode(), string(output)
}

// startStorageDaemon 启动真实双端口 `storage-service` 子进程并等待管理端 `/readyz` 就绪。
func startStorageDaemon(t *testing.T, binary, dataDir string) (pubURL, adminURL string, logs *syncBuffer, stopAndWait func() int) {
	t.Helper()
	pubPort, adminPort := freeTCPPortPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), storageLifecycleTimeout)
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(
		append(os.Environ(), "OTEL_ENABLED=false"),
		storageProcessEnv(
			pubPort,
			adminPort,
			dataDir,
			storageTestDSN(),
			"METRICS_ENABLED=true",
			"PPROF_ENABLED=true",
			"LOG_FORMAT=json",
			"LOG_LEVEL=info",
			"LOG_CONNECT_MESSAGE=handled rpc",
		)...,
	)
	logs = &syncBuffer{}
	cmd.Stdout = logs
	cmd.Stderr = logs
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	pubURL = "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(pubPort))
	adminURL = "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(adminPort))
	readyBody := waitHTTPStatus(t, adminURL+"/readyz", http.StatusOK)
	require.Contains(t, readyBody, `"postgres":"ok"`, "管理端 `/readyz` 必须报告 PostgreSQL 就绪")

	stopAndWait = func() int {
		require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
		require.NoError(t, cmd.Wait(), "SIGTERM 必须触发零退出码优雅停机: %s", logs.String())
		return cmd.ProcessState.ExitCode()
	}
	return pubURL, adminURL, logs, stopAndWait
}

// waitHTTPStatus 轮询等待目标 HTTP 端点返回预期状态码并返回响应体。
func waitHTTPStatus(t *testing.T, rawURL string, wantStatus int) string {
	t.Helper()
	httpClient := &http.Client{Timeout: time.Second}
	var bodyText string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, http.NoBody)
		require.NoError(c, reqErr)
		resp, doErr := httpClient.Do(req)
		require.NoError(c, doErr)
		defer func() { _ = resp.Body.Close() }()
		raw, readErr := io.ReadAll(resp.Body)
		require.NoError(c, readErr)
		require.Equal(c, wantStatus, resp.StatusCode)
		bodyText = string(raw)
	}, 30*time.Second, 25*time.Millisecond, "等待 %s 返回状态码 %d 超时", rawURL, wantStatus)
	return bodyText
}

// getHTTPStatusAndBody 执行单次同步 HTTP GET 并校验状态码。
func getHTTPStatusAndBody(t *testing.T, rawURL string, wantStatus int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, http.NoBody)
	require.NoError(t, reqErr)
	resp, doErr := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	require.NoError(t, doErr)
	defer func() { _ = resp.Body.Close() }()
	raw, readErr := io.ReadAll(resp.Body)
	require.NoError(t, readErr)
	require.Equal(t, wantStatus, resp.StatusCode, "GET %s 状态码不匹配: want=%d got=%d body=%s", rawURL, wantStatus, resp.StatusCode, string(raw))
	return string(raw)
}

// TestStorageProcess_RejectsInvalidStartupConfig 验证真实二进制在缺少连接串或端口已被占用时拒绝启动。
func TestStorageProcess_RejectsInvalidStartupConfig(t *testing.T) {
	binary := buildStorageBinary(t)

	t.Run("missing DSN", func(t *testing.T) {
		pubPort, adminPort := freeTCPPortPair(t)
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(pubPort, adminPort, t.TempDir(), ""))
		require.NotZero(t, exitCode, "缺少 `DB_DSN` 必须非零退出: %s", output)
		require.Contains(t, output, "DB_DSN")
	})

	t.Run("port already in use", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })
		tcpAddr, ok := listener.Addr().(*net.TCPAddr)
		require.True(t, ok)
		adminPort := freeTCPPort(t)
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(tcpAddr.Port, adminPort, t.TempDir(), storageTestDSN()))
		require.NotZero(t, exitCode, "端口冲突必须非零退出: %s", output)
		require.Contains(t, output, "address already in use")
	})
}

// TestStorageProcess_DualPortAdminAndRestartRecovery 验证真实子进程双端口隔离、
// `X-Request-Id` 日志传播、指标高基数防线、SIGTERM 重启后 PostgreSQL 与磁盘状态恢复及物理目录清理。
func TestStorageProcess_DualPortAdminAndRestartRecovery(t *testing.T) {
	// Given: 编译并启动首个双端口存储守护进程
	binary := buildStorageBinary(t)
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), storageLifecycleTimeout)
	defer cancel()

	pubURL1, adminURL1, logs1, stop1 := startStorageDaemon(t, binary, dataDir)

	// Then: 管理端口探针与诊断接口可达，公网端口物理隔离返回 404
	healthBody := getHTTPStatusAndBody(t, adminURL1+"/healthz", http.StatusOK)
	require.Contains(t, healthBody, `"status":"ok"`)
	readyBody := getHTTPStatusAndBody(t, adminURL1+"/readyz", http.StatusOK)
	require.Contains(t, readyBody, `"postgres":"ok"`)
	_ = getHTTPStatusAndBody(t, adminURL1+"/debug/pprof/cmdline", http.StatusOK)

	_ = getHTTPStatusAndBody(t, pubURL1+"/healthz", http.StatusNotFound)
	_ = getHTTPStatusAndBody(t, pubURL1+"/readyz", http.StatusNotFound)
	_ = getHTTPStatusAndBody(t, pubURL1+"/metrics", http.StatusNotFound)
	_ = getHTTPStatusAndBody(t, pubURL1+"/debug/pprof/cmdline", http.StatusNotFound)

	// When: 创建磁盘、携带 X-Request-Id 挂载写入并触发过期世代号 EBUSY
	client1 := testutil.NewStorageClientAt(pubURL1)
	created, err := client1.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "process-recovery-disk",
		MaxBytes: 4 * 1024 * 1024,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	t.Cleanup(func() {
		cleanupCtx, stopCleanup := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCleanup()
		_, _ = testutil.NewStorageClient().DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	})

	diskDir := filepath.Join(dataDir, "disks", diskID)
	dirInfo, statErr := os.Stat(diskDir)
	require.NoError(t, statErr)
	require.True(t, dirInfo.IsDir())

	const tracedReqID = "e2e-req-trace-42"
	tracedMount := connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID})
	tracedMount.Header().Set("X-Request-Id", tracedReqID)
	mount1, err := client1.MountDisk(ctx, tracedMount)
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch1)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, logs1.String(), `"request_id":"`+tracedReqID+`"`)
	}, 5*time.Second, 15*time.Millisecond, "进程标准输出结构化日志必须携带入站 `X-Request-Id`")

	fileResp, err := client1.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("persisted.bin"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	persistedPayload := []byte("persisted-across-sigterm")
	_, err = client1.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    fileIno,
		Data:   persistedPayload,
	}))
	require.NoError(t, err)

	_, busyErr1 := client1.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, busyErr1, connect.CodeAborted, "EBUSY")

	mount2, err := client1.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID, Force: true}))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mount2.Msg.GetEpoch())

	_, err = client1.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    fileIno,
		Data:   []byte("stale"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	metricsBody := getHTTPStatusAndBody(t, adminURL1+"/metrics", http.StatusOK)
	require.Contains(t, metricsBody, "bowl_storage_disks_loaded")
	require.Contains(t, metricsBody, "bowl_storage_logical_bytes")
	require.Contains(t, metricsBody, "bowl_storage_inodes")
	require.Contains(t, metricsBody, "rpc_server_call_duration")
	require.Regexp(t, `(?m)^rpc_server_call_duration[^\n]*rpc_error_reason="EBUSY"`, metricsBody)
	require.NotContains(t, metricsBody, diskID, "高基数 `disk_id` 严禁泄露至公开 Prometheus 指标")
	require.NotContains(t, metricsBody, "disk_id=")
	require.NotContains(t, metricsBody, "bowl_disk_id=")
	require.NotContains(t, metricsBody, "bowl_ino=")

	require.Zero(t, stop1())

	// When: 同一数据目录重启新进程并验证跨重启恢复与删盘清理
	pubURL2, _, _, stop2 := startStorageDaemon(t, binary, dataDir)
	client2 := testutil.NewStorageClientAt(pubURL2)

	gotAfterRestart, err := client2.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, uint64(len(persistedPayload)), gotAfterRestart.Msg.GetDisk().GetUsedBytes())

	readAfterRestart, err := client2.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Size:   uint32(len(persistedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, persistedPayload, readAfterRestart.Msg.GetData())

	// 跨进程重启后未过期 Pebble 租约仍有效，非强制 MountDisk 返回 EBUSY，Force=true 抢占后递增至 3
	_, busyAfterRestartErr := client2.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, busyAfterRestartErr, connect.CodeAborted, "EBUSY")

	mount3, err := client2.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID, Force: true}))
	require.NoError(t, err)
	require.Equal(t, uint64(3), mount3.Msg.GetEpoch(), "重启后写入世代号必须在持久化世代 2 基础上递增至 3")

	_, err = client2.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	_, deletedStatErr := os.Stat(diskDir)
	require.ErrorIs(t, deletedStatErr, os.ErrNotExist, "`DeleteDisk` 必须彻底删除本地磁盘物理目录")

	_, err = client2.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")
	require.Zero(t, stop2())
}
