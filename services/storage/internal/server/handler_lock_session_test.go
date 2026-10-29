package server_test

import (
	"context"
	"math"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
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

	ts := httptest.NewServer(h2c.NewHandler(srv.Handler(), &http2.Server{}))
	h2cClient, _ := newH2CTestClient()
	client := storagev1connect.NewStorageServiceClient(h2cClient, ts.URL)

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

		mockMgr.EXPECT().RenewSessionWithStatus(testifymock.Anything, sessionID).Return(meta.RenewSessionResult{
			ExpiresAt:        renewedTime,
			SessionRecreated: false,
		}, nil).Once()

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
		assert.False(t, resp.Msg.GetSessionRecreated())

		mockMgr.AssertExpectations(t)
	})

	t.Run("成功路径: 会话已过期重建返回 session_recreated = true", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		sessionID := uint64(888)
		renewedTime := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)

		mockMgr.EXPECT().RenewSessionWithStatus(testifymock.Anything, sessionID).Return(meta.RenewSessionResult{
			ExpiresAt:        renewedTime,
			SessionRecreated: true,
		}, nil).Once()

		reqMsg := &storagev1.RenewSessionRequest{
			SessionId: sessionID,
		}

		resp, err := client.RenewSession(ctx, connect.NewRequest(reqMsg))
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Msg.GetSessionRecreated(), "当会话过期重建时，session_recreated 必须返回 true")
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
				mockMgr.EXPECT().RenewSessionWithStatus(testifymock.Anything, sessionID).Return(meta.RenewSessionResult{}, tc.domainErr).Once()

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

	t.Run("成功路径: 携带 disk_id 路由至 RenewSessionWithDisk", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		sessionID := uint64(888)
		diskID := "11111111-1111-7111-8111-111111111111"
		renewedTime := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)

		mockMgr.EXPECT().RenewSessionWithDisk(testifymock.Anything, sessionID, testifymock.MatchedBy(func(d *string) bool {
			return d != nil && *d == diskID
		})).Return(meta.RenewSessionResult{
			ExpiresAt:        renewedTime,
			SessionRecreated: true,
		}, nil).Once()

		reqMsg := &storagev1.RenewSessionRequest{
			SessionId: sessionID,
			DiskId:    &diskID,
		}

		resp, err := client.RenewSession(ctx, connect.NewRequest(reqMsg))
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Msg.GetSessionRecreated())
		mockMgr.AssertExpectations(t)
	})
}

// TestServer_CloseSession 验证 CloseSession RPC 的成功路径与领域错误映射。
func TestServer_CloseSession(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径: 正常注销客户端会话", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		sessionID := uint64(999)
		mockMgr.EXPECT().CloseSession(testifymock.Anything, sessionID).Return(nil).Once()

		resp, err := client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{
			SessionId: sessionID,
		}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		mockMgr.AssertExpectations(t)
	})

	t.Run("典型领域错误映射", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		sessionID := uint64(999)
		mockMgr.EXPECT().CloseSession(testifymock.Anything, sessionID).Return(meta.ErrInvalidArgument).Once()

		resp, err := client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{
			SessionId: sessionID,
		}))
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EINVAL", info.GetReason())
		mockMgr.AssertExpectations(t)
	})
}

// TestServer_WatchSession 验证 WatchSession 双向流注册、Ping-Pong 探活、流断连感知清理与替换接管行为。
func TestServer_WatchSession(t *testing.T) {
	t.Run("首帧校验失败返回 InvalidArgument (EINVAL) 且不触发 CloseSession", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		invalidDiskID := "00000000-0000-0000-0000-000000000000"
		mockMgr.EXPECT().ValidateWatchSession(uint64(101), testifymock.MatchedBy(func(d *string) bool {
			return d != nil && *d == invalidDiskID
		})).Return(meta.ErrInvalidArgument).Once()

		stream := client.WatchSession(context.Background())
		require.NoError(t, stream.Send(&storagev1.WatchSessionRequest{
			SessionId: 101,
			DiskId:    &invalidDiskID,
		}))

		_, err := stream.Receive()
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EINVAL", info.GetReason())
		mockMgr.AssertExpectations(t)
	})

	t.Run("正常注册收到首帧确认 PingSeq=0、支持按需 Probe Ping-Pong 且流断开时立即调用 CloseSession", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		hub := meta.NewSessionWatchHub()
		const sid = uint64(202)
		diskID := "11111111-1111-7111-8111-111111111111"

		mockMgr.EXPECT().ValidateWatchSession(sid, testifymock.MatchedBy(func(d *string) bool {
			return d != nil && *d == diskID
		})).Return(nil).Once()
		mockMgr.EXPECT().WatchHub().Return(hub).Once()

		closedCh := make(chan uint64, 1)
		mockMgr.EXPECT().CloseSession(testifymock.Anything, sid).Run(func(ctx context.Context, sessionID uint64) {
			closedCh <- sessionID
		}).Return(nil).Once()

		streamCtx, streamCancel := context.WithCancel(context.Background())
		defer streamCancel()

		stream := client.WatchSession(streamCtx)
		require.NoError(t, stream.Send(&storagev1.WatchSessionRequest{
			SessionId: sid,
			DiskId:    &diskID,
		}))

		// 1. 收到服务端首帧确认 PingSeq = 0
		ack, err := stream.Receive()
		require.NoError(t, err)
		require.Equal(t, uint64(0), ack.GetPingSeq())
		require.True(t, hub.HasActiveStream(sid))

		// 2. 启动客户端 Pong 响应协程并执行服务端 Probe
		clientDone := make(chan struct{})
		go func() {
			defer close(clientDone)
			for {
				msg, recvErr := stream.Receive()
				if recvErr != nil {
					return
				}
				if msg.GetPingSeq() > 0 {
					_ = stream.Send(&storagev1.WatchSessionRequest{
						SessionId: sid,
						PongSeq:   msg.GetPingSeq(),
					})
				}
			}
		}()

		alive := hub.Probe(context.Background(), sid, time.Second)
		require.True(t, alive, "服务端 Probe 应收到客户端 Pong 并返回 true")

		// 3. 模拟客户端进程终止/断连（取消 streamCtx 并关闭请求流）
		streamCancel()
		_ = stream.CloseRequest()
		<-clientDone

		select {
		case closedSID := <-closedCh:
			require.Equal(t, sid, closedSID)
		case <-time.After(2 * time.Second):
			t.Fatal("流断开后服务端未及时调用 CloseSession")
		}
		require.False(t, hub.HasActiveStream(sid))
		mockMgr.AssertExpectations(t)
	})

	t.Run("同一 sessionID 新流覆盖旧流时，旧流退出不误调 CloseSession", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		hub := meta.NewSessionWatchHub()
		const sid = uint64(303)

		mockMgr.EXPECT().ValidateWatchSession(sid, (*string)(nil)).Return(nil).Twice()
		mockMgr.EXPECT().WatchHub().Return(hub).Twice()

		closedCh := make(chan uint64, 2)
		mockMgr.EXPECT().CloseSession(testifymock.Anything, sid).Run(func(ctx context.Context, sessionID uint64) {
			closedCh <- sessionID
		}).Return(nil).Once()

		ctx1, cancel1 := context.WithCancel(context.Background())
		defer cancel1()
		stream1 := client.WatchSession(ctx1)
		require.NoError(t, stream1.Send(&storagev1.WatchSessionRequest{SessionId: sid}))
		ack1, err := stream1.Receive()
		require.NoError(t, err)
		require.Equal(t, uint64(0), ack1.GetPingSeq())

		// 建立同一 sessionID 的新流 stream2 覆盖 stream1
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		stream2 := client.WatchSession(ctx2)
		require.NoError(t, stream2.Send(&storagev1.WatchSessionRequest{SessionId: sid}))
		ack2, err := stream2.Receive()
		require.NoError(t, err)
		require.Equal(t, uint64(0), ack2.GetPingSeq())

		// 关闭旧流 stream1，由于已被 stream2 替换，不应触发 CloseSession
		_ = stream1.CloseRequest()
		cancel1()
		time.Sleep(50 * time.Millisecond)
		select {
		case <-closedCh:
			t.Fatal("被替换的旧流退出时严禁调用 CloseSession")
		default:
		}
		require.True(t, hub.HasActiveStream(sid))

		// 关闭新流 stream2，此时应触发 CloseSession
		_ = stream2.CloseRequest()
		cancel2()
		select {
		case closedSID := <-closedCh:
			require.Equal(t, sid, closedSID)
		case <-time.After(2 * time.Second):
			t.Fatal("新流退出后应触发 CloseSession")
		}
	})
}
