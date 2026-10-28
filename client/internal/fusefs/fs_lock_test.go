package fusefs_test

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type lockMockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	getLkFn        func(context.Context, *connect.Request[storagev1.GetLkRequest]) (*connect.Response[storagev1.GetLkResponse], error)
	setLkFn        func(context.Context, *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error)
	setLkwFn       func(context.Context, *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *lockMockStorageServer) GetLk(ctx context.Context, req *connect.Request[storagev1.GetLkRequest]) (*connect.Response[storagev1.GetLkResponse], error) {
	if m.getLkFn != nil {
		return m.getLkFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *lockMockStorageServer) SetLk(ctx context.Context, req *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error) {
	if m.setLkFn != nil {
		return m.setLkFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *lockMockStorageServer) SetLkw(ctx context.Context, req *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error) {
	if m.setLkwFn != nil {
		return m.setLkwFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *lockMockStorageServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func setupLockTestRPCServer(t *testing.T, srv *lockMockStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func newConnectErrorWithReason(code connect.Code, reason string) error {
	connErr := connect.NewError(code, errors.New(reason))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "bowl.storage",
	})
	if err == nil {
		connErr.AddDetail(detail)
	}
	return connErr
}

// Slice 1: 锁类型双向显式转换
func TestToProtoLockType_And_ToHostLockType(t *testing.T) {
	// 宿主机 -> 协议
	pType, status := fusefs.ToProtoLockType(uint32(syscall.F_RDLCK))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, fusefs.ProtoLockTypeRead, pType)

	pType, status = fusefs.ToProtoLockType(uint32(syscall.F_WRLCK))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, fusefs.ProtoLockTypeWrite, pType)

	pType, status = fusefs.ToProtoLockType(uint32(syscall.F_UNLCK))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, fusefs.ProtoLockTypeUnlock, pType)

	_, status = fusefs.ToProtoLockType(999)
	require.Equal(t, fuse.EINVAL, status)

	// 协议 -> 宿主机
	hType, status := fusefs.ToHostLockType(fusefs.ProtoLockTypeRead)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint32(syscall.F_RDLCK), hType)

	hType, status = fusefs.ToHostLockType(fusefs.ProtoLockTypeWrite)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint32(syscall.F_WRLCK), hType)

	hType, status = fusefs.ToHostLockType(fusefs.ProtoLockTypeUnlock)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint32(syscall.F_UNLCK), hType)

	_, status = fusefs.ToHostLockType(999)
	require.Equal(t, fuse.EINVAL, status)
}

// Slice 2: 锁命名空间与无穷远区间钳位归一化
func TestNormalizeLockRange(t *testing.T) {
	t.Run("BSD flock normalizes to [0, MaxInt64]", func(t *testing.T) {
		s, e, status := fusefs.NormalizeLockRange(1, 100, 200)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(0), s)
		require.Equal(t, uint64(math.MaxInt64), e)

		// lkFlags 高位屏蔽保留最低位
		s, e, status = fusefs.NormalizeLockRange(0x3, 50, 60)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(0), s)
		require.Equal(t, uint64(math.MaxInt64), e)
	})

	t.Run("POSIX valid range within MaxInt64", func(t *testing.T) {
		s, e, status := fusefs.NormalizeLockRange(0, 100, 200)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(100), s)
		require.Equal(t, uint64(200), e)
	})

	t.Run("POSIX start > MaxInt64 returns EINVAL", func(t *testing.T) {
		_, _, status := fusefs.NormalizeLockRange(0, uint64(math.MaxInt64)+1, uint64(math.MaxInt64)+10)
		require.Equal(t, fuse.EINVAL, status)
	})

	t.Run("POSIX end > MaxInt64 clamps to MaxInt64", func(t *testing.T) {
		s, e, status := fusefs.NormalizeLockRange(0, 0, uint64(math.MaxInt64)+500)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(0), s)
		require.Equal(t, uint64(math.MaxInt64), e)
	})

	t.Run("POSIX end < start returns EINVAL", func(t *testing.T) {
		_, _, status := fusefs.NormalizeLockRange(0, 200, 100)
		require.Equal(t, fuse.EINVAL, status)
	})
}

// Slice 3: GetLk 冲突探测、句柄校验与无冲突还原
func TestRawFS_GetLk(t *testing.T) {
	srv := &lockMockStorageServer{}
	client := setupLockTestRPCServer(t, srv)
	sessions := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, sessions, fusefs.Config{
		DiskID:  "test-disk",
		RootIno: 1001,
	})

	t.Run("unbound handle returns EBADF", func(t *testing.T) {
		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Fh:       99999,
			Lk: fuse.FileLock{
				Start: 0,
				End:   100,
				Typ:   uint32(syscall.F_RDLCK),
			},
		}
		var out fuse.LkOut
		status := fs.GetLk(nil, input, &out)
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("invalid lock type returns EINVAL", func(t *testing.T) {
		const testFh = uint64(101)
		sessions.BindHandle(testFh, sessions.CurrentSessionID())

		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Fh:       testFh,
			Lk: fuse.FileLock{
				Typ: 999,
			},
		}
		var out fuse.LkOut
		status := fs.GetLk(nil, input, &out)
		require.Equal(t, fuse.EINVAL, status)
	})

	t.Run("start > MaxInt64 returns EINVAL without RPC", func(t *testing.T) {
		const testFh = uint64(102)
		sessions.BindHandle(testFh, sessions.CurrentSessionID())

		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Fh:       testFh,
			Lk: fuse.FileLock{
				Start: uint64(math.MaxInt64) + 1,
				End:   uint64(math.MaxInt64) + 10,
				Typ:   uint32(syscall.F_RDLCK),
			},
		}
		var out fuse.LkOut
		status := fs.GetLk(nil, input, &out)
		require.Equal(t, fuse.EINVAL, status)
	})

	t.Run("no conflict returns F_UNLCK and preserves range", func(t *testing.T) {
		const testFh = uint64(103)
		expectedSID := sessions.CurrentSessionID()
		sessions.BindHandle(testFh, expectedSID)

		srv.getLkFn = func(ctx context.Context, req *connect.Request[storagev1.GetLkRequest]) (*connect.Response[storagev1.GetLkResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetIno()) // 根 Inode 1 映射为 1001
			require.Equal(t, testFh, req.Msg.GetFh())
			require.Equal(t, expectedSID, req.Msg.GetSessionId())
			require.Equal(t, uint64(888), req.Msg.GetLockOwner())
			require.Equal(t, uint32(0), req.Msg.GetLkFlags())
			require.Equal(t, uint64(0), req.Msg.GetLk().GetStart())
			require.Equal(t, uint64(500), req.Msg.GetLk().GetEnd())
			require.Equal(t, fusefs.ProtoLockTypeRead, req.Msg.GetLk().GetType())

			return connect.NewResponse(&storagev1.GetLkResponse{
				Lk: &storagev1.FileLock{
					Start: req.Msg.GetLk().GetStart(),
					End:   req.Msg.GetLk().GetEnd(),
					Type:  fusefs.ProtoLockTypeUnlock,
					Pid:   0,
				},
			}), nil
		}

		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Fh:       testFh,
			Owner:    888,
			Lk: fuse.FileLock{
				Start: 0,
				End:   500,
				Typ:   uint32(syscall.F_RDLCK),
				Pid:   1234,
			},
		}
		var out fuse.LkOut
		status := fs.GetLk(nil, input, &out)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(0), out.Lk.Start)
		require.Equal(t, uint64(500), out.Lk.End)
		require.Equal(t, uint32(syscall.F_UNLCK), out.Lk.Typ)
		require.Equal(t, uint32(0), out.Lk.Pid)
	})

	t.Run("conflict returns conflicting lock details", func(t *testing.T) {
		const testFh = uint64(104)
		sessions.BindHandle(testFh, sessions.CurrentSessionID())

		srv.getLkFn = func(ctx context.Context, req *connect.Request[storagev1.GetLkRequest]) (*connect.Response[storagev1.GetLkResponse], error) {
			return connect.NewResponse(&storagev1.GetLkResponse{
				Lk: &storagev1.FileLock{
					Start: 100,
					End:   300,
					Type:  fusefs.ProtoLockTypeWrite,
					Pid:   5678,
				},
			}), nil
		}

		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Fh:       testFh,
			Owner:    888,
			Lk: fuse.FileLock{
				Start: 0,
				End:   500,
				Typ:   uint32(syscall.F_RDLCK),
				Pid:   1234,
			},
		}
		var out fuse.LkOut
		status := fs.GetLk(nil, input, &out)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(100), out.Lk.Start)
		require.Equal(t, uint64(300), out.Lk.End)
		require.Equal(t, uint32(syscall.F_WRLCK), out.Lk.Typ)
		require.Equal(t, uint32(5678), out.Lk.Pid)
	})
}

// Slice 4: SetLk 非阻塞设置锁、EAGAIN 与 flock 归一化
func TestRawFS_SetLk(t *testing.T) {
	srv := &lockMockStorageServer{}
	client := setupLockTestRPCServer(t, srv)
	sessions := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, sessions, fusefs.Config{
		DiskID:  "test-disk",
		RootIno: 1001,
	})

	t.Run("unbound handle returns EBADF", func(t *testing.T) {
		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Fh:       99999,
			Lk: fuse.FileLock{
				Start: 0,
				End:   100,
				Typ:   uint32(syscall.F_WRLCK),
			},
		}
		status := fs.SetLk(nil, input)
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("successful SetLk with flock clamps to [0, MaxInt64]", func(t *testing.T) {
		const testFh = uint64(201)
		expectedSID := sessions.CurrentSessionID()
		sessions.BindHandle(testFh, expectedSID)

		var capturedReq *storagev1.SetLkRequest
		srv.setLkFn = func(ctx context.Context, req *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error) {
			capturedReq = proto.Clone(req.Msg).(*storagev1.SetLkRequest)
			return connect.NewResponse(&storagev1.SetLkResponse{}), nil
		}

		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Fh:       testFh,
			Owner:    999,
			LkFlags:  1, // BSD flock
			Lk: fuse.FileLock{
				Start: 123,
				End:   456,
				Typ:   uint32(syscall.F_WRLCK),
				Pid:   8888,
			},
		}
		status := fs.SetLk(nil, input)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.Equal(t, uint64(2002), capturedReq.GetIno())
		require.Equal(t, testFh, capturedReq.GetFh())
		require.Equal(t, uint64(999), capturedReq.GetLockOwner())
		require.Equal(t, expectedSID, capturedReq.GetSessionId())
		require.Equal(t, uint32(1), capturedReq.GetLkFlags())
		require.Equal(t, uint64(0), capturedReq.GetLk().GetStart())
		require.Equal(t, uint64(math.MaxInt64), capturedReq.GetLk().GetEnd())
		require.Equal(t, fusefs.ProtoLockTypeWrite, capturedReq.GetLk().GetType())
		require.Equal(t, uint32(8888), capturedReq.GetLk().GetPid())
	})

	t.Run("conflict returns EAGAIN", func(t *testing.T) {
		const testFh = uint64(202)
		sessions.BindHandle(testFh, sessions.CurrentSessionID())

		srv.setLkFn = func(ctx context.Context, req *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error) {
			return nil, newConnectErrorWithReason(connect.CodeFailedPrecondition, "EAGAIN")
		}

		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Fh:       testFh,
			Owner:    999,
			Lk: fuse.FileLock{
				Start: 0,
				End:   100,
				Typ:   uint32(syscall.F_WRLCK),
			},
		}
		status := fs.SetLk(nil, input)
		require.Equal(t, fuse.EAGAIN, status)
	})
}

// Slice 5: SetLkw 阻塞锁、RequestId 单调递增、死锁 EDEADLK 与 cancel 取消 EINTR
func TestRawFS_SetLkw(t *testing.T) {
	srv := &lockMockStorageServer{}
	client := setupLockTestRPCServer(t, srv)
	sessions := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, sessions, fusefs.Config{
		DiskID:  "test-disk",
		RootIno: 1001,
	})

	t.Run("allocates monotonic non-zero RequestId in [1, MaxInt64]", func(t *testing.T) {
		const testFh = uint64(301)
		sessions.BindHandle(testFh, sessions.CurrentSessionID())

		var reqIDs []uint64
		srv.setLkwFn = func(ctx context.Context, req *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error) {
			reqIDs = append(reqIDs, req.Msg.GetRequestId())
			return connect.NewResponse(&storagev1.SetLkwResponse{}), nil
		}

		for i := 0; i < 3; i++ {
			input := &fuse.LkIn{
				InHeader: fuse.InHeader{NodeId: 3003},
				Fh:       testFh,
				Owner:    111,
				Lk: fuse.FileLock{
					Start: 0,
					End:   100,
					Typ:   uint32(syscall.F_WRLCK),
				},
			}
			status := fs.SetLkw(nil, input)
			require.Equal(t, fuse.OK, status)
		}

		require.Len(t, reqIDs, 3)
		require.Greater(t, reqIDs[0], uint64(0))
		require.LessOrEqual(t, reqIDs[0], uint64(math.MaxInt64))
		require.Less(t, reqIDs[0], reqIDs[1])
		require.Less(t, reqIDs[1], reqIDs[2])
	})

	t.Run("deadlock returns EDEADLK", func(t *testing.T) {
		const testFh = uint64(302)
		sessions.BindHandle(testFh, sessions.CurrentSessionID())

		srv.setLkwFn = func(ctx context.Context, req *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error) {
			return nil, newConnectErrorWithReason(connect.CodeFailedPrecondition, "EDEADLK")
		}

		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 3003},
			Fh:       testFh,
			Owner:    111,
			Lk: fuse.FileLock{
				Start: 0,
				End:   100,
				Typ:   uint32(syscall.F_WRLCK),
			},
		}
		status := fs.SetLkw(nil, input)
		require.Equal(t, fuse.Status(syscall.EDEADLK), status)
	})

	t.Run("cancel channel closed during in-flight SetLkw returns EINTR", func(t *testing.T) {
		const testFh = uint64(303)
		sessions.BindHandle(testFh, sessions.CurrentSessionID())

		cancelChan := make(chan struct{})
		var entered atomic.Bool

		srv.setLkwFn = func(ctx context.Context, req *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error) {
			entered.Store(true)
			// 等待被取消
			<-ctx.Done()
			return nil, ctx.Err()
		}

		go func() {
			for !entered.Load() {
				time.Sleep(5 * time.Millisecond)
			}
			close(cancelChan)
		}()

		input := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 3003},
			Fh:       testFh,
			Owner:    111,
			Lk: fuse.FileLock{
				Start: 0,
				End:   100,
				Typ:   uint32(syscall.F_WRLCK),
			},
		}

		start := time.Now()
		status := fs.SetLkw(cancelChan, input)
		elapsed := time.Since(start)

		require.Equal(t, fuse.EINTR, status)
		require.Less(t, elapsed, 1*time.Second, "中断取消应立即返回")
	})
}
