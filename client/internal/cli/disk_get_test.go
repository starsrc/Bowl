package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/cli"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestDiskGet_HelpAndValidation(t *testing.T) {
	validUUID := uuid.New().String()

	t.Run("help flag returns exit code 0", func(t *testing.T) {
		for _, arg := range []string{"--help", "-h"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "get", arg}, &stdout, &stderr)
			require.Equal(t, 0, code)
			require.Empty(t, stderr.String())
			require.Contains(t, stdout.String(), "bowl disk get <disk-id>")
			require.Contains(t, stdout.String(), "--json")
		}
	})

	t.Run("missing or extra positional args return exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "get"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "用法:")

		stderr.Reset()
		code = cli.Execute([]string{"disk", "get", validUUID, "extra-arg"}, &stdout, &stderr)
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
			code := cli.Execute([]string{"disk", "get", badID}, &stdout, &stderr)
			require.Equal(t, 2, code, "disk-id: %s", badID)
			require.Contains(t, stderr.String(), "UUID")
		}
	})

	t.Run("unknown flag or missing flag values return exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "get", validUUID, "--unknown"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "未知标志")

		for _, flag := range []string{"--endpoint", "--token"} {
			stderr.Reset()
			code = cli.Execute([]string{"disk", "get", validUUID, flag}, &stdout, &stderr)
			require.Equal(t, 2, code, "flag: %s", flag)
			require.Contains(t, stderr.String(), "需要指定参数值")
		}
	})

	t.Run("invalid endpoint returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "get", validUUID, "--endpoint", "ftp://bad"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "endpoint")
	})
}

func TestDiskGet_SuccessModes(t *testing.T) {
	targetDiskID := uuid.New().String()
	createdTime := time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)
	updatedTime := time.Date(2026, 10, 29, 12, 30, 0, 0, time.UTC)

	var capturedDiskID string
	var capturedAuth string

	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			capturedDiskID = req.Msg.GetDiskId()
			capturedAuth = req.Header().Get("Authorization")
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:     targetDiskID,
					Name:       "analytics-vol",
					RootIno:    1024,
					MaxBytes:   10 * 1024 * 1024 * 1024, // 10 GiB
					UsedBytes:  2560 * 1024 * 1024,      // 2.50 GiB (25.00%)
					UsedInodes: 42,
					CreatedAt:  timestamppb.New(createdTime),
					UpdatedAt:  timestamppb.New(updatedTime),
				},
			}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	t.Run("default human-readable detail output with usage percentage", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{
			"disk", "get", targetDiskID,
			"--endpoint=" + endpoint,
			"--token=token-abc",
		}, &stdout, &stderr)

		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.Equal(t, targetDiskID, capturedDiskID)
		require.Equal(t, "Bearer token-abc", capturedAuth)

		out := stdout.String()
		require.Contains(t, out, targetDiskID)
		require.Contains(t, out, "analytics-vol")
		require.Contains(t, out, "1024")
		require.Contains(t, out, "10737418240 (10.00 GiB)")
		require.Contains(t, out, "2684354560 (2.50 GiB)")
		require.Contains(t, out, "Usage:        25.00%")
		require.Contains(t, out, "42")
		require.Contains(t, out, "2026-10-29T10:00:00Z")
		require.Contains(t, out, "2026-10-29T12:30:00Z")
	})

	t.Run("json output mode and env fallback with flags before positional arg", func(t *testing.T) {
		t.Setenv("BOWL_STORAGE_ENDPOINT", endpoint)
		t.Setenv("BOWL_TOKEN", "env-token-xyz")

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "get", "--json", "--", targetDiskID}, &stdout, &stderr)
		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.Equal(t, targetDiskID, capturedDiskID)
		require.Equal(t, "Bearer env-token-xyz", capturedAuth)

		var parsed map[string]any
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &parsed))
		require.Equal(t, targetDiskID, parsed["disk_id"])
		require.Equal(t, "analytics-vol", parsed["name"])
		require.Equal(t, float64(1024), parsed["root_ino"])
		require.Equal(t, float64(10*1024*1024*1024), parsed["max_bytes"])
		require.Equal(t, float64(2560*1024*1024), parsed["used_bytes"])
		require.Equal(t, float64(42), parsed["used_inodes"])
		require.Equal(t, "2026-10-29T10:00:00Z", parsed["created_at"])
		require.Equal(t, "2026-10-29T12:30:00Z", parsed["updated_at"])
	})
}

func TestDiskGet_Errors(t *testing.T) {
	targetDiskID := uuid.New().String()

	t.Run("NotFound error outputs chinese not found message and returns exit code 1", func(t *testing.T) {
		mockSrv := &cliMockStorageHandler{
			getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
				return nil, connect.NewError(connect.CodeNotFound, os.ErrNotExist)
			},
		}
		endpoint := setupTestStorageServer(t, mockSrv)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "get", targetDiskID, "--endpoint", endpoint}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "磁盘不存在")
		require.Contains(t, stderr.String(), targetDiskID)
	})

	t.Run("other RPC error returns exit code 1", func(t *testing.T) {
		mockSrv := &cliMockStorageHandler{
			getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("connection refused"))
			},
		}
		endpoint := setupTestStorageServer(t, mockSrv)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "get", targetDiskID, "--endpoint", endpoint}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "获取磁盘元数据失败")
	})
}
