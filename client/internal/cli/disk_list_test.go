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

func TestDiskList_HelpAndValidation(t *testing.T) {
	t.Run("help flag returns exit code 0", func(t *testing.T) {
		for _, arg := range []string{"--help", "-h"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "list", arg}, &stdout, &stderr)
			require.Equal(t, 0, code)
			require.Empty(t, stderr.String())
			require.Contains(t, stdout.String(), "bowl disk list")
			require.Contains(t, stdout.String(), "--limit")
			require.Contains(t, stdout.String(), "--page-token")
			require.Contains(t, stdout.String(), "--json")
		}
	})

	t.Run("positional arguments rejected with exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "list", "extra-arg"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "位置参数")

		stderr.Reset()
		code = cli.Execute([]string{"disk", "list", "--", "extra-arg"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "位置参数")
	})

	t.Run("invalid --limit values return exit code 2", func(t *testing.T) {
		for _, badLimit := range []string{"0", "-1", "1001", "abc", "1.5", ""} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "list", "--limit=" + badLimit}, &stdout, &stderr)
			require.Equal(t, 2, code, "limit: %s", badLimit)
			require.NotEmpty(t, stderr.String())
		}
	})

	t.Run("unknown flag or missing flag values return exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "list", "--unknown-flag"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "未知标志")

		for _, flag := range []string{"--limit", "--page-token", "--endpoint", "--token"} {
			stderr.Reset()
			code = cli.Execute([]string{"disk", "list", flag}, &stdout, &stderr)
			require.Equal(t, 2, code, "flag: %s", flag)
			require.Contains(t, stderr.String(), "需要指定参数值")
		}
	})

	t.Run("invalid endpoint returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "list", "--endpoint", "ftp://invalid"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "endpoint")
	})
}

func TestDiskList_SuccessModes(t *testing.T) {
	id1 := uuid.New().String()
	id2 := uuid.New().String()
	t1 := time.Date(2026, 10, 29, 14, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

	var capturedReq *storagev1.ListDisksRequest
	var capturedAuth string
	var mockResp *storagev1.ListDisksResponse

	mockSrv := &diskMockStorageHandler{
		listDisksFn: func(ctx context.Context, req *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
			capturedReq = req.Msg
			capturedAuth = req.Header().Get("Authorization")
			return connect.NewResponse(mockResp), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	t.Run("table output with disks and next_page_token", func(t *testing.T) {
		mockResp = &storagev1.ListDisksResponse{
			Disks: []*storagev1.Disk{
				{
					DiskId:     id1,
					Name:       "vol-alpha",
					RootIno:    1001,
					MaxBytes:   10 * 1024 * 1024 * 1024, // 10.00 GiB
					UsedBytes:  2560 * 1024 * 1024,      // 2.50 GiB (25.00%)
					UsedInodes: 15,
					CreatedAt:  timestamppb.New(t1),
					UpdatedAt:  timestamppb.New(t1),
				},
				{
					DiskId:     id2,
					Name:       "vol-beta",
					RootIno:    1002,
					MaxBytes:   1024 * 1024 * 1024, // 1.00 GiB
					UsedBytes:  0,                  // 0 B (0.00%)
					UsedInodes: 1,
					CreatedAt:  timestamppb.New(t2),
					UpdatedAt:  timestamppb.New(t2),
				},
			},
			NextPageToken: "cursor-next-xyz",
		}

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{
			"disk", "list",
			"--limit", "2",
			"--page-token", "cursor-prev-abc",
			"--endpoint", endpoint,
			"--token", "my-token",
		}, &stdout, &stderr)

		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.NotNil(t, capturedReq)
		require.Equal(t, uint32(2), capturedReq.GetPageSize())
		require.Equal(t, "cursor-prev-abc", capturedReq.GetPageToken())
		require.Equal(t, "Bearer my-token", capturedAuth)

		out := stdout.String()
		lines := strings.Split(strings.TrimSpace(out), "\n")
		require.GreaterOrEqual(t, len(lines), 4)
		// 验证表头包含所有要求的列
		for _, col := range []string{"DISK ID", "NAME", "USED", "MAX", "USAGE%", "INODES", "CREATED AT"} {
			require.Contains(t, lines[0], col)
		}
		require.Contains(t, out, id1)
		require.Contains(t, out, "vol-alpha")
		require.Contains(t, out, "2.50 GiB")
		require.Contains(t, out, "10.00 GiB")
		require.Contains(t, out, "25.00%")
		require.Contains(t, out, "15")
		require.Contains(t, out, "2026-10-29T14:00:00Z")

		require.Contains(t, out, id2)
		require.Contains(t, out, "vol-beta")
		require.Contains(t, out, "0 B")
		require.Contains(t, out, "1.00 GiB")
		require.Contains(t, out, "0.00%")
		require.Contains(t, out, "2026-10-29T12:00:00Z")

		require.Contains(t, out, "Next Page Token: cursor-next-xyz")
	})

	t.Run("empty list table output without next_page_token and default page_size=0", func(t *testing.T) {
		mockResp = &storagev1.ListDisksResponse{
			Disks:         nil,
			NextPageToken: "",
		}

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "list", "--endpoint=" + endpoint}, &stdout, &stderr)
		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.Equal(t, uint32(0), capturedReq.GetPageSize())
		require.Equal(t, "", capturedReq.GetPageToken())

		out := stdout.String()
		for _, col := range []string{"DISK ID", "NAME", "USED", "MAX", "USAGE%", "INODES", "CREATED AT"} {
			require.Contains(t, out, col)
		}
		require.NotContains(t, out, "Next Page Token:")
	})

	t.Run("json output with empty disks serializes as empty array [] not null", func(t *testing.T) {
		mockResp = &storagev1.ListDisksResponse{
			Disks:         nil,
			NextPageToken: "",
		}

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "list", "--json", "--endpoint", endpoint}, &stdout, &stderr)
		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())

		rawJSON := stdout.String()
		require.Contains(t, rawJSON, `"disks": []`)

		var parsed struct {
			Disks         []map[string]any `json:"disks"`
			NextPageToken string           `json:"next_page_token"`
		}
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &parsed))
		require.NotNil(t, parsed.Disks)
		require.Empty(t, parsed.Disks)
		require.Equal(t, "", parsed.NextPageToken)
	})

	t.Run("json output with disks and next_page_token", func(t *testing.T) {
		mockResp = &storagev1.ListDisksResponse{
			Disks: []*storagev1.Disk{
				{
					DiskId:     id1,
					Name:       "vol-alpha",
					RootIno:    1001,
					MaxBytes:   1024,
					UsedBytes:  512,
					UsedInodes: 2,
					CreatedAt:  timestamppb.New(t1),
					UpdatedAt:  timestamppb.New(t1),
				},
			},
			NextPageToken: "tok-123",
		}

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{
			"disk", "list",
			"--limit=1000",
			"--page-token=in-tok",
			"--json",
			"--endpoint=" + endpoint,
		}, &stdout, &stderr)
		require.Equal(t, 0, code)
		require.Empty(t, stderr.String())
		require.Equal(t, uint32(1000), capturedReq.GetPageSize())
		require.Equal(t, "in-tok", capturedReq.GetPageToken())

		var parsed struct {
			Disks []struct {
				DiskID     string `json:"disk_id"`
				Name       string `json:"name"`
				RootIno    uint64 `json:"root_ino"`
				MaxBytes   uint64 `json:"max_bytes"`
				UsedBytes  uint64 `json:"used_bytes"`
				UsedInodes uint64 `json:"used_inodes"`
				CreatedAt  string `json:"created_at"`
				UpdatedAt  string `json:"updated_at"`
			} `json:"disks"`
			NextPageToken string `json:"next_page_token"`
		}
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &parsed))
		require.Len(t, parsed.Disks, 1)
		require.Equal(t, id1, parsed.Disks[0].DiskID)
		require.Equal(t, "vol-alpha", parsed.Disks[0].Name)
		require.Equal(t, uint64(1001), parsed.Disks[0].RootIno)
		require.Equal(t, uint64(1024), parsed.Disks[0].MaxBytes)
		require.Equal(t, uint64(512), parsed.Disks[0].UsedBytes)
		require.Equal(t, uint64(2), parsed.Disks[0].UsedInodes)
		require.Equal(t, "2026-10-29T14:00:00Z", parsed.Disks[0].CreatedAt)
		require.Equal(t, "tok-123", parsed.NextPageToken)
	})
}

func TestDiskList_RPCFailure(t *testing.T) {
	mockSrv := &diskMockStorageHandler{
		listDisksFn: func(ctx context.Context, req *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid page_token"))
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"disk", "list", "--page-token", "bad", "--endpoint", endpoint}, &stdout, &stderr)
	require.Equal(t, 1, code)
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "列出虚拟磁盘失败")
}
