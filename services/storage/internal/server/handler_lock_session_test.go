package server_test

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

// setupTestServer 启动基于 MockManager 的测试 HTTP 服务器并返回客户端与清理函数。
func setupTestServer(t *testing.T, mockMgr *metamock.MockManager) (storagev1connect.StorageServiceClient, func()) {
	t.Helper()

	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)

	return client, ts.Close
}

// TestServer_GetLk 验证 GetLk RPC 的成功路径与领域错误映射。
func TestServer_GetLk(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径: 检测到冲突锁", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		reqMsg := &storagev1.GetLkRequest{
			Ino:       101,
			Fh:        202,
			LockOwner: 303,
			Lk: &storagev1.FileLock{
				Start: 10,
				End:   200,
				Type:  1, // F_WRLCK
				Pid:   1001,
			},
			LkFlags:   0,
			SessionId: 404,
		}

		expectedParams := meta.GetLkParams{
			Ino:       101,
			Fh:        202,
			LockOwner: 303,
			Lk: meta.FileLock{
				Start: 10,
				End:   200,
				Type:  1,
				PID:   1001,
			},
			LkFlags:   0,
			SessionID: 404,
		}

		conflictResult := &meta.FileLock{
			Start: 50,
			End:   150,
			Type:  1,
			PID:   2002,
		}

		mockMgr.EXPECT().GetLk(testifymock.Anything, expectedParams).Return(conflictResult, nil).Once()

		resp, err := client.GetLk(ctx, connect.NewRequest(reqMsg))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Msg.GetLk())

		assert.Equal(t, uint64(50), resp.Msg.GetLk().GetStart())
		assert.Equal(t, uint64(150), resp.Msg.GetLk().GetEnd())
		assert.Equal(t, uint32(1), resp.Msg.GetLk().GetType())
		assert.Equal(t, uint32(2002), resp.Msg.GetLk().GetPid())

		mockMgr.AssertExpectations(t)
	})

	t.Run("成功路径: 无冲突锁返回 F_UNLCK", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		reqMsg := &storagev1.GetLkRequest{
			Ino:       101,
			Fh:        202,
			LockOwner: 303,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   math.MaxInt64,
				Type:  0, // F_RDLCK
				Pid:   1001,
			},
			LkFlags:   1, // FUSE_LK_FLOCK
			SessionId: 404,
		}

		expectedParams := meta.GetLkParams{
			Ino:       101,
			Fh:        202,
			LockOwner: 303,
			Lk: meta.FileLock{
				Start: 0,
				End:   math.MaxInt64,
				Type:  0,
				PID:   1001,
			},
			LkFlags:   1,
			SessionID: 404,
		}

		unlckResult := &meta.FileLock{
			Start: 0,
			End:   math.MaxInt64,
			Type:  2, // F_UNLCK
			PID:   0,
		}

		mockMgr.EXPECT().GetLk(testifymock.Anything, expectedParams).Return(unlckResult, nil).Once()

		resp, err := client.GetLk(ctx, connect.NewRequest(reqMsg))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Msg.GetLk())

		assert.Equal(t, uint64(0), resp.Msg.GetLk().GetStart())
		assert.Equal(t, uint64(math.MaxInt64), resp.Msg.GetLk().GetEnd())
		assert.Equal(t, uint32(2), resp.Msg.GetLk().GetType())
		assert.Equal(t, uint32(0), resp.Msg.GetLk().GetPid())

		mockMgr.AssertExpectations(t)
	})

	t.Run("典型领域错误映射", func(t *testing.T) {
		tests := []struct {
			name           string
			domainErr      error
			expectedCode   connect.Code
			expectedReason string
		}{
			{"文件句柄非法 (EBADF)", meta.ErrBadFileHandle, connect.CodeFailedPrecondition, "EBADF"},
			{"目标为目录 (EISDIR)", meta.ErrIsDir, connect.CodeFailedPrecondition, "EISDIR"},
			{"节点不存在 (ENOENT)", meta.ErrNotFound, connect.CodeNotFound, "ENOENT"},
			{"参数非法 (EINVAL)", meta.ErrInvalidArgument, connect.CodeInvalidArgument, "EINVAL"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				mockMgr := metamock.NewMockManager(t)
				client, teardown := setupTestServer(t, mockMgr)
				defer teardown()

				reqMsg := &storagev1.GetLkRequest{
					Ino:       101,
					Fh:        202,
					LockOwner: 303,
					Lk: &storagev1.FileLock{
						Start: 0,
						End:   100,
						Type:  0,
						Pid:   1,
					},
					LkFlags:   0,
					SessionId: 404,
				}

				mockMgr.EXPECT().GetLk(testifymock.Anything, testifymock.Anything).Return(nil, tc.domainErr).Once()

				resp, err := client.GetLk(ctx, connect.NewRequest(reqMsg))
				require.Error(t, err)
				assert.Nil(t, resp)

				assert.Equal(t, tc.expectedCode, connect.CodeOf(err))
				info := server.ExtractErrorInfo(err)
				require.NotNil(t, info, "必须返回 ErrorInfo")
				assert.Equal(t, tc.expectedReason, info.GetReason())
				assert.Equal(t, meta.ErrorDomain, info.GetDomain())

				mockMgr.AssertExpectations(t)
			})
		}
	})
}

// TestServer_SetLk 验证 SetLk RPC 的成功路径与领域错误映射。
func TestServer_SetLk(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径: 申请/释放锁成功", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		reqMsg := &storagev1.SetLkRequest{
			Ino:       501,
			Fh:        602,
			LockOwner: 703,
			Lk: &storagev1.FileLock{
				Start: 100,
				End:   500,
				Type:  1, // F_WRLCK
				Pid:   999,
			},
			LkFlags:   0,
			SessionId: 804,
		}

		expectedParams := meta.SetLkParams{
			Ino:       501,
			Fh:        602,
			LockOwner: 703,
			Lk: meta.FileLock{
				Start: 100,
				End:   500,
				Type:  1,
				PID:   999,
			},
			LkFlags:   0,
			SessionID: 804,
		}

		mockMgr.EXPECT().SetLk(testifymock.Anything, expectedParams).Return(nil).Once()

		resp, err := client.SetLk(ctx, connect.NewRequest(reqMsg))
		require.NoError(t, err)
		require.NotNil(t, resp)

		mockMgr.AssertExpectations(t)
	})

	t.Run("典型领域错误映射", func(t *testing.T) {
		tests := []struct {
			name           string
			domainErr      error
			expectedCode   connect.Code
			expectedReason string
		}{
			{"冲突锁或排队冲突 (EAGAIN)", meta.ErrAgain, connect.CodeResourceExhausted, "EAGAIN"},
			{"句柄读写权限不匹配 (EBADF)", meta.ErrBadFileHandle, connect.CodeFailedPrecondition, "EBADF"},
			{"目标为目录 (EISDIR)", meta.ErrIsDir, connect.CodeFailedPrecondition, "EISDIR"},
			{"节点不存在 (ENOENT)", meta.ErrNotFound, connect.CodeNotFound, "ENOENT"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				mockMgr := metamock.NewMockManager(t)
				client, teardown := setupTestServer(t, mockMgr)
				defer teardown()

				reqMsg := &storagev1.SetLkRequest{
					Ino:       501,
					Fh:        602,
					LockOwner: 703,
					Lk: &storagev1.FileLock{
						Start: 0,
						End:   100,
						Type:  1,
						Pid:   999,
					},
					LkFlags:   0,
					SessionId: 804,
				}

				mockMgr.EXPECT().SetLk(testifymock.Anything, testifymock.Anything).Return(tc.domainErr).Once()

				resp, err := client.SetLk(ctx, connect.NewRequest(reqMsg))
				require.Error(t, err)
				assert.Nil(t, resp)

				assert.Equal(t, tc.expectedCode, connect.CodeOf(err))
				info := server.ExtractErrorInfo(err)
				require.NotNil(t, info, "必须返回 ErrorInfo")
				assert.Equal(t, tc.expectedReason, info.GetReason())
				assert.Equal(t, meta.ErrorDomain, info.GetDomain())

				mockMgr.AssertExpectations(t)
			})
		}
	})
}

// TestServer_SetLkw 验证 SetLkw RPC 的成功路径与典型领域错误（EDEADLK, EINTR 等）。
func TestServer_SetLkw(t *testing.T) {
	t.Run("成功路径: 阻塞获取锁成功", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		reqMsg := &storagev1.SetLkwRequest{
			Ino:       501,
			Fh:        602,
			LockOwner: 703,
			Lk: &storagev1.FileLock{
				Start: 100,
				End:   500,
				Type:  1, // F_WRLCK
				Pid:   999,
			},
			LkFlags:   0,
			SessionId: 804,
			RequestId: 905,
		}

		expectedParams := meta.SetLkwParams{
			Ino:       501,
			Fh:        602,
			LockOwner: 703,
			Lk: meta.FileLock{
				Start: 100,
				End:   500,
				Type:  1,
				PID:   999,
			},
			LkFlags:   0,
			SessionID: 804,
			RequestID: 905,
		}

		mockMgr.EXPECT().SetLkw(testifymock.Anything, expectedParams).Return(nil).Once()

		resp, err := client.SetLkw(ctx, connect.NewRequest(reqMsg))
		require.NoError(t, err)
		require.NotNil(t, resp)

		mockMgr.AssertExpectations(t)
	})

	t.Run("典型领域错误映射", func(t *testing.T) {
		tests := []struct {
			name           string
			domainErr      error
			expectedCode   connect.Code
			expectedReason string
		}{
			{"死锁检测 (EDEADLK)", meta.ErrDeadlock, connect.CodeAborted, "EDEADLK"},
			{"信号中断或取消 (EINTR)", meta.ErrInterrupted, connect.CodeCanceled, "EINTR"},
			{"非法文件句柄 (EBADF)", meta.ErrBadFileHandle, connect.CodeFailedPrecondition, "EBADF"},
			{"资源暂不可用 (EAGAIN)", meta.ErrAgain, connect.CodeResourceExhausted, "EAGAIN"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				mockMgr := metamock.NewMockManager(t)
				client, teardown := setupTestServer(t, mockMgr)
				defer teardown()

				reqMsg := &storagev1.SetLkwRequest{
					Ino:       501,
					Fh:        602,
					LockOwner: 703,
					Lk: &storagev1.FileLock{
						Start: 0,
						End:   100,
						Type:  1,
						Pid:   999,
					},
					LkFlags:   0,
					SessionId: 804,
					RequestId: 905,
				}

				mockMgr.EXPECT().SetLkw(testifymock.Anything, testifymock.Anything).Return(tc.domainErr).Once()

				resp, err := client.SetLkw(ctx, connect.NewRequest(reqMsg))
				require.Error(t, err)
				assert.Nil(t, resp)

				assert.Equal(t, tc.expectedCode, connect.CodeOf(err))
				info := server.ExtractErrorInfo(err)
				require.NotNil(t, info, "必须返回 ErrorInfo")
				assert.Equal(t, tc.expectedReason, info.GetReason())
				assert.Equal(t, meta.ErrorDomain, info.GetDomain())

				mockMgr.AssertExpectations(t)
			})
		}
	})
}

// TestServer_RenewSession 验证 RenewSession RPC 的成功路径与领域错误映射。
func TestServer_RenewSession(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径: 会话租约成功续期", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		sessionID := uint64(888)
		renewedTime := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)

		mockMgr.EXPECT().RenewSession(testifymock.Anything, sessionID).Return(renewedTime, nil).Once()

		reqMsg := &storagev1.RenewSessionRequest{
			SessionId: sessionID,
		}

		resp, err := client.RenewSession(ctx, connect.NewRequest(reqMsg))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Msg.GetExpiresAt())

		expectedTimestamp := timestamppb.New(renewedTime)
		assert.Equal(t, expectedTimestamp.GetSeconds(), resp.Msg.GetExpiresAt().GetSeconds())
		assert.Equal(t, expectedTimestamp.GetNanos(), resp.Msg.GetExpiresAt().GetNanos())

		mockMgr.AssertExpectations(t)
	})

	t.Run("典型领域错误映射", func(t *testing.T) {
		tests := []struct {
			name           string
			domainErr      error
			expectedCode   connect.Code
			expectedReason string
		}{
			{"会话不存在 (ENOENT)", meta.ErrNotFound, connect.CodeNotFound, "ENOENT"},
			{"非法参数 (EINVAL)", meta.ErrInvalidArgument, connect.CodeInvalidArgument, "EINVAL"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				mockMgr := metamock.NewMockManager(t)
				client, teardown := setupTestServer(t, mockMgr)
				defer teardown()

				sessionID := uint64(888)
				mockMgr.EXPECT().RenewSession(testifymock.Anything, sessionID).Return(time.Time{}, tc.domainErr).Once()

				reqMsg := &storagev1.RenewSessionRequest{
					SessionId: sessionID,
				}

				resp, err := client.RenewSession(ctx, connect.NewRequest(reqMsg))
				require.Error(t, err)
				assert.Nil(t, resp)

				assert.Equal(t, tc.expectedCode, connect.CodeOf(err))
				info := server.ExtractErrorInfo(err)
				require.NotNil(t, info, "必须返回 ErrorInfo")
				assert.Equal(t, tc.expectedReason, info.GetReason())
				assert.Equal(t, meta.ErrorDomain, info.GetDomain())

				mockMgr.AssertExpectations(t)
			})
		}
	})
}
