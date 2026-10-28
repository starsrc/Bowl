package cli_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/starsrc/bowl/client/internal/cli"
	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type cliMockStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	getDiskFn func(context.Context, *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error)
}

func (h *cliMockStorageHandler) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	if h.getDiskFn != nil {
		return h.getDiskFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeNotFound, os.ErrNotExist)
}

func setupTestStorageServer(t *testing.T, handler storagev1connect.StorageServiceHandler) string {
	mux := http.NewServeMux()
	path, h := storagev1connect.NewStorageServiceHandler(handler)
	mux.Handle(path, h)

	h2cServer := &http2.Server{}
	ts := httptest.NewServer(h2c.NewHandler(mux, h2cServer))
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestMount_ArgValidation(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	t.Run("missing positional arguments returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"mount"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "用法:")

		stderr.Reset()
		code = cli.Execute([]string{"mount", validUUID}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "用法:")
	})

	t.Run("invalid or nil uuid returns exit code 2", func(t *testing.T) {
		testCases := []string{
			"not-a-uuid",
			"12345",
			"00000000-0000-0000-0000-000000000000",
		}
		for _, invalidID := range testCases {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"mount", invalidID, tmpDir}, &stdout, &stderr)
			require.Equal(t, 2, code)
			require.Contains(t, stderr.String(), "UUID")
		}
	})

	t.Run("nonexistent mountpoint returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		nonExistent := filepath.Join(tmpDir, "does-not-exist")
		code := cli.Execute([]string{"mount", validUUID, nonExistent}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "挂载点")
	})

	t.Run("mountpoint is a file not dir returns exit code 2", func(t *testing.T) {
		testFile := filepath.Join(tmpDir, "somefile.txt")
		require.NoError(t, os.WriteFile(testFile, []byte("hello"), 0o644))

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"mount", validUUID, testFile}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "目录")
	})
}

func TestMount_FlagPositioning(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	// 模拟远程不存在的 Disk
	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return nil, connect.NewError(connect.CodeNotFound, os.ErrNotExist)
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	// 测试 flags 出现在位置参数之前和之后均能被正确解析到 endpoint
	cases := [][]string{
		{"mount", "-f", "--endpoint", endpoint, validUUID, tmpDir},
		{"mount", validUUID, tmpDir, "-f", "--endpoint", endpoint},
		{"mount", validUUID, "-f", tmpDir, "--endpoint", endpoint},
	}

	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		code := cli.Execute(args, &stdout, &stderr)
		// 因为到达了 GetDisk 且 Disk 不存在，所以返回退出码 1（说明 flag 解析成功并连接到了 endpoint）
		require.Equal(t, 1, code, "args: %v", args)
		require.Contains(t, stderr.String(), "磁盘不存在")
	}
}

func TestMount_DefaultDaemonDelegation(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:   validUUID,
					RootIno:  1001,
					MaxBytes: 1024 * 1024,
				},
			}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	var launcherCalled bool
	cli.SetProcessLauncherForTest(t, func(cmd *exec.Cmd) error {
		launcherCalled = true
		require.Contains(t, cmd.Args, "mount")
		require.Contains(t, cmd.Args, validUUID)
		w := cmd.ExtraFiles[0]
		_, _ = w.WriteString("OK\n")
		return nil
	})

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"mount", "--endpoint", endpoint, validUUID, tmpDir}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.True(t, launcherCalled)
}

func TestMount_ForegroundRunnerSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:   validUUID,
					RootIno:  1001,
					MaxBytes: 1024 * 1024,
				},
			}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	var runnerCalled bool
	cli.SetMountRunnerForTest(t, func(ctx context.Context, client storagev1connect.StorageServiceClient, cfg fusefs.Config, mountpoint string, stdout, stderr io.Writer) error {
		runnerCalled = true
		require.Equal(t, validUUID, cfg.DiskID)
		require.Equal(t, uint64(1001), cfg.RootIno)
		require.Equal(t, tmpDir, mountpoint)
		return nil
	})

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"mount", "-f", "--endpoint", endpoint, validUUID, tmpDir}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.True(t, runnerCalled)
}
