//go:build e2e

package e2e_test

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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

func storageTestDSN() string {
	return cmp.Or(os.Getenv("BOWL_TEST_POSTGRES_DSN"), defaultStorageTestDSN)
}

func buildStorageBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "storage-service")
	ctx, cancel := context.WithTimeout(context.Background(), storageBuildTimeout)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "-C", "../services/storage", "build", "-o", binary, "./cmd")
	build.Env = append(os.Environ(), "GOWORK=off")
	output, err := build.CombinedOutput()
	require.NoError(t, ctx.Err(), "building storage-service timed out: %s", output)
	require.NoError(t, err, "building storage-service failed: %s", output)
	return binary
}

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

func runStorageProcessExpectFailure(t *testing.T, binary string, env []string) (exitCode int, out string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(append(os.Environ(), "OTEL_ENABLED=false"), env...)
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "storage-service did not exit within timeout: %s", output)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "storage-service must exit with non-zero status: %s", output)
	return exitErr.ExitCode(), string(output)
}

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
	require.Contains(t, readyBody, `"postgres":"ok"`, "admin /readyz must report PostgreSQL ready")

	stopAndWait = func() int {
		require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
		require.NoError(t, cmd.Wait(), "SIGTERM must trigger graceful zero-exit shutdown: %s", logs.String())
		return cmd.ProcessState.ExitCode()
	}
	return pubURL, adminURL, logs, stopAndWait
}

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
	}, 30*time.Second, 25*time.Millisecond, "waiting for %s status %d timed out", rawURL, wantStatus)
	return bodyText
}

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
	require.Equal(t, wantStatus, resp.StatusCode, "GET %s status mismatch: want=%d got=%d body=%s", rawURL, wantStatus, resp.StatusCode, string(raw))
	return string(raw)
}

func TestStorageProcess_RejectsInvalidStartupConfig(t *testing.T) {
	binary := buildStorageBinary(t)

	t.Run("missing DSN", func(t *testing.T) {
		pubPort, adminPort := freeTCPPortPair(t)
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(pubPort, adminPort, t.TempDir(), ""))
		require.NotZero(t, exitCode, "missing DB_DSN must exit non-zero: %s", output)
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
		require.NotZero(t, exitCode, "port conflict must exit non-zero: %s", output)
		require.Contains(t, output, "address already in use")
	})

	t.Run("unreachable postgres", func(t *testing.T) {
		pubPort, adminPort := freeTCPPortPair(t)
		closedPGPort := freeTCPPort(t)
		unreachableDSN := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/bowl?sslmode=disable&connect_timeout=2", closedPGPort)
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(pubPort, adminPort, t.TempDir(), unreachableDSN))
		require.NotZero(t, exitCode, "unreachable PostgreSQL must fail fast on boot: %s", output)
	})
}

func TestStorageProcess_DualPortAdminAndRestartRecovery(t *testing.T) {
	binary := buildStorageBinary(t)
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), storageLifecycleTimeout)
	defer cancel()

	pubURL1, adminURL1, logs1, stop1 := startStorageDaemon(t, binary, dataDir)

	healthBody := getHTTPStatusAndBody(t, adminURL1+"/healthz", http.StatusOK)
	require.Contains(t, healthBody, `"status":"ok"`)
	readyBody := getHTTPStatusAndBody(t, adminURL1+"/readyz", http.StatusOK)
	require.Contains(t, readyBody, `"postgres":"ok"`)
	_ = getHTTPStatusAndBody(t, adminURL1+"/debug/pprof/cmdline", http.StatusOK)

	_ = getHTTPStatusAndBody(t, pubURL1+"/healthz", http.StatusNotFound)
	_ = getHTTPStatusAndBody(t, pubURL1+"/readyz", http.StatusNotFound)
	_ = getHTTPStatusAndBody(t, pubURL1+"/metrics", http.StatusNotFound)
	_ = getHTTPStatusAndBody(t, pubURL1+"/debug/pprof/cmdline", http.StatusNotFound)

	client1 := testutil.NewStorageClientAt(pubURL1)
	created, err := client1.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "process-recovery-disk",
		MaxBytes: 4 * 1024 * 1024,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	require.Equal(t, testutil.DefaultBranch, created.Msg.GetDisk().GetDefaultBranch())
	t.Cleanup(func() {
		cleanupCtx, stopCleanup := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCleanup()
		cleanupClient := testutil.NewStorageClient()
		_, _ = cleanupClient.EvictMounts(cleanupCtx, connect.NewRequest(&storagev1.EvictMountsRequest{DiskId: diskID}))
		_, _ = cleanupClient.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	})

	diskDir := filepath.Join(dataDir, "disks", diskID)
	dirInfo, statErr := os.Stat(diskDir)
	require.NoError(t, statErr)
	require.True(t, dirInfo.IsDir())

	const tracedReqID = "e2e-req-trace-42"
	tracedMount := connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	})
	tracedMount.Header().Set("X-Request-Id", tracedReqID)
	mount1, err := client1.MountBranch(ctx, tracedMount)
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Positive(t, epoch1)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, logs1.String(), `"request_id":"`+tracedReqID+`"`)
	}, 5*time.Second, 15*time.Millisecond, "structured stdout logs must carry inbound X-Request-Id")

	fileResp, err := client1.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("persisted.bin"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	persistedPayload := []byte("persisted-across-sigterm")
	_, err = client1.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  epoch1,
		Ino:    fileIno,
		Data:   persistedPayload,
	}))
	require.NoError(t, err)

	commitResp, err := client1.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  testutil.DefaultBranch,
		Message: "pre-restart-commit",
	}))
	require.NoError(t, err)
	preRestartCommitID := commitResp.Msg.GetCommit().GetCommitId()

	forkResp, err := client1.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: testutil.DefaultBranch,
		NewBranch:    "feat",
		Message:      "pre-restart-fork",
	}))
	require.NoError(t, err)
	forkCommitID := forkResp.Msg.GetCommit().GetCommitId()

	mount2, err := client1.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Force:  true,
	}))
	require.NoError(t, err)
	epoch2 := mount2.Msg.GetEpoch()
	require.Equal(t, epoch1+1, epoch2)

	_, err = client1.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  epoch1,
		Ino:    fileIno,
		Data:   []byte("stale"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	metricsBody := getHTTPStatusAndBody(t, adminURL1+"/metrics", http.StatusOK)
	require.Contains(t, metricsBody, "bowl_storage_disks_loaded")
	require.Contains(t, metricsBody, "bowl_storage_logical_bytes")
	require.Contains(t, metricsBody, "rpc_server_call_duration")
	require.Regexp(t, `(?m)^rpc_server_call_duration[^\n]*rpc_error_reason="EBUSY"`, metricsBody)
	require.NotContains(t, metricsBody, diskID, "high-cardinality disk_id must not leak into Prometheus metrics")
	require.NotContains(t, metricsBody, "disk_id=")
	require.NotContains(t, metricsBody, "bowl_disk_id=")
	require.NotContains(t, metricsBody, "bowl_ino=")

	require.Zero(t, stop1())

	pubURL2, _, _, stop2 := startStorageDaemon(t, binary, dataDir)
	client2 := testutil.NewStorageClientAt(pubURL2)

	gotAfterRestart, err := client2.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, testutil.DefaultBranch, gotAfterRestart.Msg.GetDisk().GetDefaultBranch())
	require.GreaterOrEqual(t, gotAfterRestart.Msg.GetDisk().GetUsedBytes(), uint64(len(persistedPayload)))

	readMainAfterRestart, err := client2.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    fileIno,
		Size:   uint32(len(persistedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, persistedPayload, readMainAfterRestart.Msg.GetData())

	readFeatAfterRestart, err := client2.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: "feat",
		Ino:    fileIno,
		Size:   uint32(len(persistedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, persistedPayload, readFeatAfterRestart.Msg.GetData())

	branchesAfterRestart, err := client2.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Len(t, branchesAfterRestart.Msg.GetBranches(), 2)
	require.True(t, slices.ContainsFunc(branchesAfterRestart.Msg.GetBranches(), func(b *storagev1.Branch) bool {
		return b.GetName() == "feat" && b.GetHeadCommitId() == forkCommitID
	}))

	commitsAfterRestart, err := client2.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(commitsAfterRestart.Msg.GetCommits(), func(c *storagev1.Commit) bool {
		return c.GetCommitId() == preRestartCommitID && c.GetMessage() == "pre-restart-commit"
	}))
	require.True(t, slices.ContainsFunc(commitsAfterRestart.Msg.GetCommits(), func(c *storagev1.Commit) bool {
		return c.GetCommitId() == forkCommitID && c.GetKind() == storagev1.CommitKind_COMMIT_KIND_FORK
	}))

	_, err = client2.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	testutil.RequireConnectError(t, err, connect.CodeAborted, "EBUSY")

	mount3, err := client2.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Force:  true,
	}))
	require.NoError(t, err)
	require.Equal(t, epoch2+1, mount3.Msg.GetEpoch(), "writer epoch must increment from persisted epoch after restart")

	_, err = client2.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, err, connect.CodeAborted, "EBUSY")

	_, err = client2.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mount3.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	_, err = client2.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	_, deletedStatErr := os.Stat(diskDir)
	require.ErrorIs(t, deletedStatErr, os.ErrNotExist, "DeleteDisk must remove physical disk directory")

	_, err = client2.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")
	require.Zero(t, stop2())
}
