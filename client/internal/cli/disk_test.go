package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/cli"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type diskMockStorageHandler struct {
	cliMockStorageHandler

	createDiskFn func(context.Context, *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error)
}

func (h *diskMockStorageHandler) CreateDisk(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	if h.createDiskFn != nil {
		return h.createDiskFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func TestDiskCreate_HelpAndValidation(t *testing.T) {
	t.Run("help flag returns exit code 0", func(t *testing.T) {
		for _, arg := range []string{"--help", "-h"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "create", arg}, &stdout, &stderr)
			require.Equal(t, 0, code)
			require.Empty(t, stderr.String())
			require.Contains(t, stdout.String(), "bowl disk create")
			require.Contains(t, stdout.String(), "--size")
		}
	})

	t.Run("missing --size returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "create"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "--size")
	})

	t.Run("invalid --size values return exit code 2", func(t *testing.T) {
		badSizes := []string{"0", "0GiB", "-10GiB", "abc", "10EiB", "9223372036854775808", "8192PiB"}
		for _, sz := range badSizes {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "create", "--size", sz}, &stdout, &stderr)
			require.Equal(t, 2, code, "size: %s", sz)
			require.NotEmpty(t, stderr.String())
		}
	})

	t.Run("invalid --name (exceeds 255 bytes or contains NUL) returns exit code 2", func(t *testing.T) {
		longName := strings.Repeat("a", 256)
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "create", "--size", "1GiB", "--name", longName}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "255")

		stderr.Reset()
		nulName := "bad\x00name"
		code = cli.Execute([]string{"disk", "create", "--size", "1GiB", "--name", nulName}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "空字节")
	})

	t.Run("-q and --json mutually exclusive returns exit code 2", func(t *testing.T) {
		for _, qFlag := range []string{"-q", "--quiet"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "create", "--size", "1GiB", qFlag, "--json"}, &stdout, &stderr)
			require.Equal(t, 2, code)
			require.Contains(t, stderr.String(), "不能同时使用")
		}
	})

	t.Run("unknown flag or positional args return exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "create", "--size", "1GiB", "--unknown-flag"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "未知标志")

		stderr.Reset()
		code = cli.Execute([]string{"disk", "create", "--size", "1GiB", "extra-arg"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "位置参数")
	})

	t.Run("missing flag values return exit code 2", func(t *testing.T) {
		for _, flag := range []string{"--size", "--name", "--endpoint", "--token"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "create", flag}, &stdout, &stderr)
			require.Equal(t, 2, code, "flag: %s", flag)
			require.Contains(t, stderr.String(), "需要指定参数值")
		}
	})

	t.Run("invalid endpoint returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "create", "--size", "1GiB", "--endpoint", "ftp://invalid"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "endpoint")
	})
}

func TestDiskCreate_SuccessModes(t *testing.T) {
	createdDiskID := uuid.New().String()
	createdTime := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

	var capturedReq *storagev1.CreateDiskRequest
	var capturedAuth string

	mockSrv := &diskMockStorageHandler{
		createDiskFn: func(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
			capturedReq = req.Msg
			capturedAuth = req.Header().Get("Authorization")
			return connect.NewResponse(&storagev1.CreateDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:     createdDiskID,
					Name:       req.Msg.GetName(),
					RootIno:    2048,
					MaxBytes:   req.Msg.GetMaxBytes(),
					UsedBytes:  0,
					UsedInodes: 1,
					CreatedAt:  timestamppb.New(createdTime),
					UpdatedAt:  timestamppb.New(createdTime),
				},
			}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	t.Run("default human-readable detail output with = syntax", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{
			"disk", "create",
			"--size=10GiB",
			"--name=prod-data",
			"--endpoint=" + endpoint,
			"--token=secret-token",
		}, &stdout, &stderr)

		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.NotNil(t, capturedReq)
		require.Equal(t, "prod-data", capturedReq.GetName())
		require.Equal(t, uint64(10*1024*1024*1024), capturedReq.GetMaxBytes())
		require.Equal(t, "Bearer secret-token", capturedAuth)

		out := stdout.String()
		require.Contains(t, out, createdDiskID)
		require.Contains(t, out, "prod-data")
		require.Contains(t, out, "2048")
		require.Contains(t, out, "10737418240")
		require.Contains(t, out, "10.00 GiB")
		require.Contains(t, out, "2026-10-29T12:00:00Z")
	})

	t.Run("json output mode and env fallback", func(t *testing.T) {
		t.Setenv("BOWL_STORAGE_ENDPOINT", endpoint)
		t.Setenv("BOWL_TOKEN", "env-token-123")

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "create", "--size", "512MiB", "--json"}, &stdout, &stderr)
		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.Equal(t, "Bearer env-token-123", capturedAuth)
		require.Equal(t, "", capturedReq.GetName())
		require.Equal(t, uint64(512*1024*1024), capturedReq.GetMaxBytes())

		var parsed map[string]any
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &parsed))
		require.Equal(t, createdDiskID, parsed["disk_id"])
		require.Equal(t, "", parsed["name"])
		require.Equal(t, float64(2048), parsed["root_ino"])
		require.Equal(t, float64(512*1024*1024), parsed["max_bytes"])
		require.Equal(t, float64(0), parsed["used_bytes"])
		require.Equal(t, float64(1), parsed["used_inodes"])
		require.Equal(t, "2026-10-29T12:00:00Z", parsed["created_at"])
		require.Equal(t, "2026-10-29T12:00:00Z", parsed["updated_at"])
	})

	t.Run("quiet mode outputs only disk-id and newline", func(t *testing.T) {
		for _, qFlag := range []string{"-q", "--quiet"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "create", "--size", "1GiB", qFlag, "--endpoint", endpoint}, &stdout, &stderr)
			require.Equal(t, 0, code)
			require.Empty(t, stderr.String())
			require.Equal(t, createdDiskID+"\n", stdout.String())
		}
	})
}

func TestDiskCreate_RPCFailure(t *testing.T) {
	mockSrv := &diskMockStorageHandler{
		createDiskFn: func(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
			return nil, connect.NewError(connect.CodePermissionDenied, errors.New("unauthorized"))
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"disk", "create", "--size", "1GiB", "--endpoint", endpoint}, &stdout, &stderr)
	require.Equal(t, 1, code)
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "创建虚拟磁盘失败")
}
