package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/cli"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type diskDeleteMockStorageHandler struct {
	cliMockStorageHandler

	deleteDiskFn func(context.Context, *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error)
}

func (h *diskDeleteMockStorageHandler) DeleteDisk(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	if h.deleteDiskFn != nil {
		return h.deleteDiskFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func TestDiskDelete_HelpAndValidation(t *testing.T) {
	validUUID := uuid.New().String()

	t.Run("help flag returns exit code 0", func(t *testing.T) {
		for _, arg := range []string{"--help", "-h"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "delete", arg}, &stdout, &stderr)
			require.Equal(t, 0, code, "arg: %s", arg)
			require.Empty(t, stderr.String())
			require.Contains(t, stdout.String(), "bowl disk delete <disk-id>")
			require.Contains(t, stdout.String(), "-y, --yes")
			require.Contains(t, stdout.String(), "--json")
		}
	})

	t.Run("missing or extra positional args return exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "delete", "-y"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "用法:")

		stderr.Reset()
		code = cli.Execute([]string{"disk", "delete", validUUID, "extra-arg", "-y"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "用法:")
	})

	t.Run("invalid or nil uuid returns exit code 2", func(t *testing.T) {
		for _, badID := range []string{
			"not-a-uuid",
			"12345",
			"00000000-0000-0000-0000-000000000000",
		} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "delete", badID, "-y"}, &stdout, &stderr)
			require.Equal(t, 2, code, "disk-id: %s", badID)
			require.Contains(t, stderr.String(), "UUID")
		}
	})

	t.Run("unknown flag or missing flag values return exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "delete", validUUID, "--unknown"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "未知标志")

		for _, flag := range []string{"--endpoint", "--token"} {
			stderr.Reset()
			code = cli.Execute([]string{"disk", "delete", validUUID, flag}, &stdout, &stderr)
			require.Equal(t, 2, code, "flag: %s", flag)
			require.Contains(t, stderr.String(), "需要指定参数值")
		}
	})

	t.Run("invalid endpoint returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "delete", validUUID, "-y", "--endpoint", "ftp://bad"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "endpoint")
	})
}

func TestDiskDelete_LocalMountpointCheck(t *testing.T) {
	targetDiskID := uuid.New().String()

	sampleMountInfo := `30 61 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro
100 30 0:50 / /mnt/my\040disk rw,nosuid,nodev shared:55 - fuse.bowl bowl:` + targetDiskID + ` rw
`
	tmpFile := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(tmpFile, []byte(sampleMountInfo), 0o644))
	cli.SetMountInfoPathForTest(t, tmpFile)

	var rpcCalled bool
	mockSrv := &diskDeleteMockStorageHandler{
		deleteDiskFn: func(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
			rpcCalled = true
			return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"disk", "delete", targetDiskID, "-y", "--endpoint", endpoint}, &stdout, &stderr)
	require.Equal(t, 1, code)
	require.False(t, rpcCalled, "本机挂载表命中时不得发起 RPC 请求")
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "/mnt/my disk")
	require.Contains(t, stderr.String(), "bowl umount")
}

func TestDiskDelete_InteractiveConfirmation(t *testing.T) {
	cli.SetMountInfoPathForTest(t, filepath.Join(t.TempDir(), "nonexistent"))
	targetDiskID := uuid.New().String()

	var rpcCalled bool
	var capturedDiskID string
	mockSrv := &diskDeleteMockStorageHandler{
		deleteDiskFn: func(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
			rpcCalled = true
			capturedDiskID = req.Msg.GetDiskId()
			return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	t.Run("abort when user inputs n, empty line, or EOF", func(t *testing.T) {
		for _, input := range []string{"n\n", "no\n", "\n", ""} {
			rpcCalled = false
			cli.SetStdinForTest(t, strings.NewReader(input))

			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "delete", targetDiskID, "--endpoint", endpoint}, &stdout, &stderr)
			require.Equal(t, 1, code, "input=%q", input)
			require.False(t, rpcCalled, "未通过确认时不得发起 RPC 请求 (input=%q)", input)
			require.Empty(t, stdout.String())
			require.Contains(t, stderr.String(), "确认删除")
			require.Contains(t, stderr.String(), "已取消")
		}
	})

	t.Run("proceed when user inputs y or YES", func(t *testing.T) {
		for _, input := range []string{"y\n", "YES\n", " yes "} {
			rpcCalled = false
			capturedDiskID = ""
			cli.SetStdinForTest(t, strings.NewReader(input))

			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "delete", targetDiskID, "--endpoint", endpoint}, &stdout, &stderr)
			require.Equal(t, 0, code, "input=%q, stderr=%s", input, stderr.String())
			require.True(t, rpcCalled, "input=%q", input)
			require.Equal(t, targetDiskID, capturedDiskID)
			require.Contains(t, stderr.String(), "确认删除")
			require.Contains(t, stdout.String(), targetDiskID)
		}
	})
}

func TestDiskDelete_SuccessModes(t *testing.T) {
	cli.SetMountInfoPathForTest(t, filepath.Join(t.TempDir(), "nonexistent"))
	targetDiskID := uuid.New().String()

	var capturedDiskID string
	var capturedAuth string
	mockSrv := &diskDeleteMockStorageHandler{
		deleteDiskFn: func(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
			capturedDiskID = req.Msg.GetDiskId()
			capturedAuth = req.Header().Get("Authorization")
			return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	t.Run("skip confirmation with -y and output human-readable message", func(t *testing.T) {
		for _, yesFlag := range []string{"-y", "--yes"} {
			capturedDiskID = ""
			capturedAuth = ""
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{
				"disk", "delete", targetDiskID,
				yesFlag,
				"--endpoint=" + endpoint,
				"--token=del-tok-1",
			}, &stdout, &stderr)
			require.Equal(t, 0, code, "stderr: %s", stderr.String())
			require.Empty(t, stderr.String())
			require.Equal(t, targetDiskID, capturedDiskID)
			require.Equal(t, "Bearer del-tok-1", capturedAuth)
			require.Contains(t, stdout.String(), targetDiskID)
			require.Contains(t, stdout.String(), "已删除")
		}
	})

	t.Run("--json mode outputs disk_id and deleted true", func(t *testing.T) {
		t.Setenv("BOWL_STORAGE_ENDPOINT", endpoint)
		t.Setenv("BOWL_TOKEN", "env-del-tok")

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "delete", "-y", "--json", "--", targetDiskID}, &stdout, &stderr)
		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.Equal(t, targetDiskID, capturedDiskID)
		require.Equal(t, "Bearer env-del-tok", capturedAuth)

		var parsed map[string]any
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &parsed))
		require.Equal(t, targetDiskID, parsed["disk_id"])
		require.Equal(t, true, parsed["deleted"])
	})
}

func TestDiskDelete_RPCErrors(t *testing.T) {
	cli.SetMountInfoPathForTest(t, filepath.Join(t.TempDir(), "nonexistent"))
	targetDiskID := uuid.New().String()

	t.Run("CodeNotFound returns exit code 1 with 磁盘不存在", func(t *testing.T) {
		mockSrv := &diskDeleteMockStorageHandler{
			deleteDiskFn: func(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
				return nil, connect.NewError(connect.CodeNotFound, os.ErrNotExist)
			},
		}
		endpoint := setupTestStorageServer(t, mockSrv)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "delete", targetDiskID, "-y", "--endpoint", endpoint}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "磁盘不存在")
		require.Contains(t, stderr.String(), targetDiskID)
	})

	t.Run("CodeFailedPrecondition / EBUSY returns exit code 1 with active session message", func(t *testing.T) {
		mockSrv := &diskDeleteMockStorageHandler{
			deleteDiskFn: func(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
				cerr := connect.NewError(connect.CodeFailedPrecondition, errors.New("meta: device or resource busy"))
				if detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
					Reason: "EBUSY",
					Domain: "bowl.storage",
				}); err == nil {
					cerr.AddDetail(detail)
				}
				return nil, cerr
			},
		}
		endpoint := setupTestStorageServer(t, mockSrv)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "delete", targetDiskID, "-y", "--endpoint", endpoint}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "挂载状态")
		require.Contains(t, stderr.String(), targetDiskID)
	})

	t.Run("other RPC error returns exit code 1", func(t *testing.T) {
		mockSrv := &diskDeleteMockStorageHandler{
			deleteDiskFn: func(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied"))
			},
		}
		endpoint := setupTestStorageServer(t, mockSrv)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "delete", targetDiskID, "-y", "--endpoint", endpoint}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "删除")
	})
}
