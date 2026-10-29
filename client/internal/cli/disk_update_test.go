package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
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

func TestDiskUpdate_HelpAndValidation(t *testing.T) {
	validUUID := uuid.New().String()

	t.Run("help flag returns exit code 0 for update and resize", func(t *testing.T) {
		for _, subcmd := range []string{"update", "resize"} {
			for _, arg := range []string{"--help", "-h"} {
				var stdout, stderr bytes.Buffer
				code := cli.Execute([]string{"disk", subcmd, arg}, &stdout, &stderr)
				require.Equal(t, 0, code, "subcmd=%s arg=%s", subcmd, arg)
				require.Empty(t, stderr.String())
				require.Contains(t, stdout.String(), "bowl disk update <disk-id>")
				require.Contains(t, stdout.String(), "bowl disk resize <disk-id>")
				require.Contains(t, stdout.String(), "--size")
				require.Contains(t, stdout.String(), "--name")
				require.Contains(t, stdout.String(), "--json")
			}
		}
	})

	t.Run("missing or extra positional args return exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "update", "--size", "10GiB"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "用法:")

		stderr.Reset()
		code = cli.Execute([]string{"disk", "update", validUUID, "extra-arg", "--size", "10GiB"}, &stdout, &stderr)
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
			code := cli.Execute([]string{"disk", "update", badID, "--size", "10GiB"}, &stdout, &stderr)
			require.Equal(t, 2, code, "disk-id: %s", badID)
			require.Contains(t, stderr.String(), "UUID")
		}
	})

	t.Run("neither --size nor --name specified returns exit code 2", func(t *testing.T) {
		for _, subcmd := range []string{"update", "resize"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", subcmd, validUUID}, &stdout, &stderr)
			require.Equal(t, 2, code, "subcmd: %s", subcmd)
			require.Contains(t, stderr.String(), "--size")
			require.Contains(t, stderr.String(), "--name")
		}
	})

	t.Run("invalid --size values return exit code 2", func(t *testing.T) {
		badSizes := []string{"", "0", "0GiB", "-10GiB", "abc", "10EiB", "9223372036854775808", "8192PiB"}
		for _, sz := range badSizes {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"disk", "update", validUUID, "--size", sz}, &stdout, &stderr)
			require.Equal(t, 2, code, "size: %s", sz)
			require.NotEmpty(t, stderr.String())
		}
	})

	t.Run("invalid --name (exceeds 255 bytes or contains NUL) returns exit code 2", func(t *testing.T) {
		longName := strings.Repeat("a", 256)
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "update", validUUID, "--name", longName}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "255")

		stderr.Reset()
		nulName := "bad\x00name"
		code = cli.Execute([]string{"disk", "update", validUUID, "--name", nulName}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "空字节")
	})

	t.Run("unknown flag or missing flag values return exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "update", validUUID, "--unknown"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "未知标志")

		for _, flag := range []string{"--size", "--name", "--endpoint", "--token"} {
			stderr.Reset()
			code = cli.Execute([]string{"disk", "update", validUUID, flag}, &stdout, &stderr)
			require.Equal(t, 2, code, "flag: %s", flag)
			require.Contains(t, stderr.String(), "需要指定参数值")
		}
	})

	t.Run("invalid endpoint returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "update", validUUID, "--size", "10GiB", "--endpoint", "ftp://bad"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "endpoint")
	})
}

func TestDiskUpdate_NameOnly(t *testing.T) {
	targetDiskID := uuid.New().String()
	createdTime := time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)
	updatedTime := time.Date(2026, 10, 29, 14, 0, 0, 0, time.UTC)

	var getDiskCalled bool
	var capturedUpdateReq *storagev1.UpdateDiskRequest
	var capturedAuth string

	mockSrv := &diskMockStorageHandler{
		cliMockStorageHandler: cliMockStorageHandler{
			getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
				getDiskCalled = true
				return nil, errors.New("unexpected GetDisk call when --size is not specified")
			},
		},
		updateDiskFn: func(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
			capturedUpdateReq = req.Msg
			capturedAuth = req.Header().Get("Authorization")
			return connect.NewResponse(&storagev1.UpdateDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:     targetDiskID,
					Name:       req.Msg.GetName(),
					RootIno:    1024,
					MaxBytes:   10 * 1024 * 1024 * 1024,
					UsedBytes:  2 * 1024 * 1024 * 1024,
					UsedInodes: 10,
					CreatedAt:  timestamppb.New(createdTime),
					UpdatedAt:  timestamppb.New(updatedTime),
				},
			}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	t.Run("update --name only skips GetDisk and calls UpdateDisk", func(t *testing.T) {
		getDiskCalled = false
		capturedUpdateReq = nil

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{
			"disk", "update", targetDiskID,
			"--name", "renamed-vol",
			"--endpoint", endpoint,
			"--token", "tok-123",
		}, &stdout, &stderr)

		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.False(t, getDiskCalled, "仅更新 --name 时不应发起前置 GetDisk")
		require.NotNil(t, capturedUpdateReq)
		require.Equal(t, targetDiskID, capturedUpdateReq.GetDiskId())
		require.NotNil(t, capturedUpdateReq.Name)
		require.Equal(t, "renamed-vol", *capturedUpdateReq.Name)
		require.Nil(t, capturedUpdateReq.MaxBytes)
		require.Equal(t, "Bearer tok-123", capturedAuth)

		out := stdout.String()
		require.Contains(t, out, targetDiskID)
		require.Contains(t, out, "renamed-vol")
		require.Contains(t, out, "10737418240 (10.00 GiB)")
	})

	t.Run("update --name=\"\" clears disk name and outputs JSON", func(t *testing.T) {
		getDiskCalled = false
		capturedUpdateReq = nil

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{
			"disk", "update", targetDiskID,
			"--name=",
			"--json",
			"--endpoint", endpoint,
		}, &stdout, &stderr)

		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.False(t, getDiskCalled)
		require.NotNil(t, capturedUpdateReq)
		require.NotNil(t, capturedUpdateReq.Name, "显式传入 --name=\"\" 时 Name 指针应非 nil")
		require.Equal(t, "", *capturedUpdateReq.Name)
		require.Nil(t, capturedUpdateReq.MaxBytes)

		var parsed map[string]any
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &parsed))
		require.Equal(t, targetDiskID, parsed["disk_id"])
		require.Equal(t, "", parsed["name"])
	})
}

func TestDiskUpdate_SizeAndResizeAlias(t *testing.T) {
	targetDiskID := uuid.New().String()
	createdTime := time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)
	updatedTime := time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC)

	var callOrder []string
	var capturedUpdateReq *storagev1.UpdateDiskRequest

	mockSrv := &diskMockStorageHandler{
		cliMockStorageHandler: cliMockStorageHandler{
			getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
				callOrder = append(callOrder, "GetDisk")
				require.Equal(t, targetDiskID, req.Msg.GetDiskId())
				return connect.NewResponse(&storagev1.GetDiskResponse{
					Disk: &storagev1.Disk{
						DiskId:     targetDiskID,
						Name:       "orig-name",
						RootIno:    1024,
						MaxBytes:   10 * 1024 * 1024 * 1024, // 10 GiB
						UsedBytes:  5 * 1024 * 1024 * 1024,  // 5 GiB
						UsedInodes: 20,
						CreatedAt:  timestamppb.New(createdTime),
						UpdatedAt:  timestamppb.New(createdTime),
					},
				}), nil
			},
		},
		updateDiskFn: func(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
			callOrder = append(callOrder, "UpdateDisk")
			capturedUpdateReq = req.Msg
			name := "orig-name"
			if req.Msg.Name != nil {
				name = *req.Msg.Name
			}
			return connect.NewResponse(&storagev1.UpdateDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:     targetDiskID,
					Name:       name,
					RootIno:    1024,
					MaxBytes:   req.Msg.GetMaxBytes(),
					UsedBytes:  5 * 1024 * 1024 * 1024,
					UsedInodes: 20,
					CreatedAt:  timestamppb.New(createdTime),
					UpdatedAt:  timestamppb.New(updatedTime),
				},
			}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	t.Run("resize alias with --size calls GetDisk then UpdateDisk", func(t *testing.T) {
		callOrder = nil
		capturedUpdateReq = nil

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{
			"disk", "resize", targetDiskID,
			"--size", "20GiB",
			"--endpoint", endpoint,
		}, &stdout, &stderr)

		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.Equal(t, []string{"GetDisk", "UpdateDisk"}, callOrder)
		require.NotNil(t, capturedUpdateReq)
		require.Nil(t, capturedUpdateReq.Name)
		require.NotNil(t, capturedUpdateReq.MaxBytes)
		require.Equal(t, uint64(20*1024*1024*1024), *capturedUpdateReq.MaxBytes)

		out := stdout.String()
		require.Contains(t, out, "21474836480 (20.00 GiB)")
		require.Contains(t, out, "Usage:        25.00%")
	})

	t.Run("update both --size (equal to used_bytes) and --name with --json", func(t *testing.T) {
		callOrder = nil
		capturedUpdateReq = nil
		t.Setenv("BOWL_STORAGE_ENDPOINT", endpoint)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{
			"disk", "update",
			"--size=5GiB",
			"--name=tight-vol",
			"--json",
			"--", targetDiskID,
		}, &stdout, &stderr)

		require.Equal(t, 0, code, "stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.Equal(t, []string{"GetDisk", "UpdateDisk"}, callOrder)
		require.NotNil(t, capturedUpdateReq)
		require.NotNil(t, capturedUpdateReq.Name)
		require.Equal(t, "tight-vol", *capturedUpdateReq.Name)
		require.NotNil(t, capturedUpdateReq.MaxBytes)
		require.Equal(t, uint64(5*1024*1024*1024), *capturedUpdateReq.MaxBytes)

		var parsed map[string]any
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &parsed))
		require.Equal(t, targetDiskID, parsed["disk_id"])
		require.Equal(t, "tight-vol", parsed["name"])
		require.Equal(t, float64(5*1024*1024*1024), parsed["max_bytes"])
		require.Equal(t, float64(5*1024*1024*1024), parsed["used_bytes"])
	})
}

func TestDiskUpdate_TwoLayerShrinkAndErrors(t *testing.T) {
	targetDiskID := uuid.New().String()

	t.Run("Layer 1: client pre-check rejects shrink below used_bytes without calling UpdateDisk", func(t *testing.T) {
		var getDiskCalled, updateDiskCalled bool

		mockSrv := &diskMockStorageHandler{
			cliMockStorageHandler: cliMockStorageHandler{
				getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
					getDiskCalled = true
					return connect.NewResponse(&storagev1.GetDiskResponse{
						Disk: &storagev1.Disk{
							DiskId:    targetDiskID,
							MaxBytes:  10 * 1024 * 1024 * 1024, // 10 GiB
							UsedBytes: 5 * 1024 * 1024 * 1024,  // 5 GiB
						},
					}), nil
				},
			},
			updateDiskFn: func(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
				updateDiskCalled = true
				return nil, errors.New("UpdateDisk should not be called")
			},
		}
		endpoint := setupTestStorageServer(t, mockSrv)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "resize", targetDiskID, "--size", "2GiB", "--endpoint", endpoint}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.True(t, getDiskCalled)
		require.False(t, updateDiskCalled, "客户端前置水位线拦截后不得调用 UpdateDisk")

		errMsg := stderr.String()
		require.Contains(t, errMsg, "2.00 GiB")
		require.Contains(t, errMsg, "5.00 GiB")
		require.Contains(t, errMsg, "已用容量")
	})

	t.Run("Layer 2: server CodeFailedPrecondition (concurrent write triggered ErrInvalidShrink) returns exit code 1", func(t *testing.T) {
		var callOrder []string

		mockSrv := &diskMockStorageHandler{
			cliMockStorageHandler: cliMockStorageHandler{
				getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
					callOrder = append(callOrder, "GetDisk")
					return connect.NewResponse(&storagev1.GetDiskResponse{
						Disk: &storagev1.Disk{
							DiskId:    targetDiskID,
							MaxBytes:  10 * 1024 * 1024 * 1024, // 10 GiB
							UsedBytes: 1 * 1024 * 1024 * 1024,  // 1 GiB at pre-check time
						},
					}), nil
				},
			},
			updateDiskFn: func(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
				callOrder = append(callOrder, "UpdateDisk")
				return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("cannot shrink disk below used_bytes"))
			},
		}
		endpoint := setupTestStorageServer(t, mockSrv)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "update", targetDiskID, "--size", "2GiB", "--endpoint", endpoint}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.Equal(t, []string{"GetDisk", "UpdateDisk"}, callOrder)
		require.Contains(t, stderr.String(), "缩容")
		require.Contains(t, stderr.String(), "已用容量")
	})

	t.Run("NotFound on GetDisk or UpdateDisk outputs 磁盘不存在 and returns exit code 1", func(t *testing.T) {
		// 1. GetDisk returns CodeNotFound (when --size is specified)
		mockSrvGetNotFound := &diskMockStorageHandler{
			cliMockStorageHandler: cliMockStorageHandler{
				getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
					return nil, connect.NewError(connect.CodeNotFound, os.ErrNotExist)
				},
			},
		}
		ep1 := setupTestStorageServer(t, mockSrvGetNotFound)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "update", targetDiskID, "--size", "10GiB", "--endpoint", ep1}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "磁盘不存在")

		// 2. UpdateDisk returns CodeNotFound (when only --name is specified, or deleted concurrently)
		mockSrvUpdateNotFound := &diskMockStorageHandler{
			updateDiskFn: func(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
				return nil, connect.NewError(connect.CodeNotFound, os.ErrNotExist)
			},
		}
		ep2 := setupTestStorageServer(t, mockSrvUpdateNotFound)

		stdout.Reset()
		stderr.Reset()
		code = cli.Execute([]string{"disk", "update", targetDiskID, "--name", "new-name", "--endpoint", ep2}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "磁盘不存在")
	})

	t.Run("generic RPC error on GetDisk or UpdateDisk returns exit code 1", func(t *testing.T) {
		mockSrvGetErr := &diskMockStorageHandler{
			cliMockStorageHandler: cliMockStorageHandler{
				getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
					return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied"))
				},
			},
		}
		ep1 := setupTestStorageServer(t, mockSrvGetErr)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "update", targetDiskID, "--size", "10GiB", "--endpoint", ep1}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Contains(t, stderr.String(), "获取磁盘元数据失败")

		mockSrvUpdateErr := &diskMockStorageHandler{
			updateDiskFn: func(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied"))
			},
		}
		ep2 := setupTestStorageServer(t, mockSrvUpdateErr)

		stdout.Reset()
		stderr.Reset()
		code = cli.Execute([]string{"disk", "update", targetDiskID, "--name", "new-name", "--endpoint", ep2}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Contains(t, stderr.String(), "更新磁盘元数据失败")
	})
}
