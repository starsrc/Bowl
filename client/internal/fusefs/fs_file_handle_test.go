package fusefs_test

import (
	"context"
	"errors"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type handleMockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	createFn       func(context.Context, *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error)
	openFn         func(context.Context, *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error)
	flushFn        func(context.Context, *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error)
	fsyncFn        func(context.Context, *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error)
	releaseFn      func(context.Context, *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *handleMockStorageServer) Create(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	if m.createFn != nil {
		return m.createFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) Open(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
	if m.openFn != nil {
		return m.openFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) Flush(ctx context.Context, req *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error) {
	if m.flushFn != nil {
		return m.flushFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) Fsync(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	if m.fsyncFn != nil {
		return m.fsyncFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) Release(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
	if m.releaseFn != nil {
		return m.releaseFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func setupHandleTestRPCServer(t *testing.T, srv *handleMockStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func setupTestSessionManager(t *testing.T, client storagev1connect.StorageServiceClient) *session.Manager {
	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.Close()
	})
	return mgr
}

func TestToProtoOpenFlags_Sanitization(t *testing.T) {
	// 1. 基础读写模式映射
	require.Equal(t, uint32(0o0), fusefs.ToProtoOpenFlags(uint32(syscall.O_RDONLY)))
	require.Equal(t, uint32(0o1), fusefs.ToProtoOpenFlags(uint32(syscall.O_WRONLY)))
	require.Equal(t, uint32(0o2), fusefs.ToProtoOpenFlags(uint32(syscall.O_RDWR)))

	// 2. 控制位单独映射
	require.Equal(t, uint32(0o100), fusefs.ToProtoOpenFlags(uint32(syscall.O_CREAT)))
	require.Equal(t, uint32(0o200), fusefs.ToProtoOpenFlags(uint32(syscall.O_EXCL)))
	require.Equal(t, uint32(0o1000), fusefs.ToProtoOpenFlags(uint32(syscall.O_TRUNC)))
	require.Equal(t, uint32(0o2000), fusefs.ToProtoOpenFlags(uint32(syscall.O_APPEND)))

	// 3. 常见组合映射
	flags1 := uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC)
	require.Equal(t, uint32(0o1|0o100|0o1000), fusefs.ToProtoOpenFlags(flags1))

	flags2 := uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL)
	require.Equal(t, uint32(0o2|0o100|0o200), fusefs.ToProtoOpenFlags(flags2))

	flags3 := uint32(syscall.O_WRONLY | syscall.O_APPEND)
	require.Equal(t, uint32(0o1|0o2000), fusefs.ToProtoOpenFlags(flags3))

	// 4. 跨架构宿主机高位标志剥离（防碰撞服务端 0o20000000 O_TMPFILE）
	var highBits uint32 = 0x8000 | 0x4000 | 0x20000 | 0x80000 | 0x800 | 0x101000 | 0o20000000
	cleaned := fusefs.ToProtoOpenFlags(flags1 | highBits)
	require.Equal(t, uint32(0o1|0o100|0o1000), cleaned, "高位必须被彻底剥离")
	require.Zero(t, cleaned&0o20000000, "严禁碰撞服务端 O_TMPFILE (0o20000000)")
}

func TestRawFS_Create(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)

	t.Run("read-only mode blocks create with EROFS", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, mgr, fusefs.Config{ReadOnly: true, RootIno: 1001})
		var out fuse.CreateOut
		status := roFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
			Mode:     0o644,
		}, "file.txt", &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("anti-aliasing invalid parent ino returns ENOENT", func(t *testing.T) {
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
		var out fuse.CreateOut
		status := fs.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1001}, // 非 1 且等于 RootIno
		}, "file.txt", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("successful create with direct-io and handle binding", func(t *testing.T) {
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{
			RootIno:  1001,
			DirectIO: true,
		})

		var calledParentIno uint64
		var calledName string
		var calledMode uint32
		var calledUid, calledGid uint32
		var calledFlags uint32
		var calledSID *uint64

		mockSrv.createFn = func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
			calledParentIno = req.Msg.GetParentIno()
			calledName = string(req.Msg.GetName())
			calledMode = req.Msg.GetMode()
			calledUid = req.Msg.GetUid()
			calledGid = req.Msg.GetGid()
			calledFlags = req.Msg.GetFlags()
			calledSID = req.Msg.SessionId

			return connect.NewResponse(&storagev1.CreateResponse{
				Inode: &storagev1.Inode{
					Ino:        2002,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Mode:  0o644 | syscall.S_IFREG,
						Size:  0,
						Nlink: 1,
						Uid:   1000,
						Gid:   1000,
					},
				},
				Fh:                  42,
				OpenFlags:           0,
				EntryTimeoutSeconds: 1.5,
				AttrTimeoutSeconds:  2.5,
			}), nil
		}

		var out fuse.CreateOut
		status := fs.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{
				NodeId: 1,
				Caller: fuse.Caller{
					Owner: fuse.Owner{Uid: 1000, Gid: 1000},
				},
			},
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | 0x8000), // 携带高位
			Mode:  0o100644,
		}, "test.txt", &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(1001), calledParentIno)
		require.Equal(t, "test.txt", calledName)
		require.Equal(t, uint32(0o100644&0xffff), calledMode)
		require.Equal(t, uint32(1000), calledUid)
		require.Equal(t, uint32(1000), calledGid)
		require.Equal(t, uint32(0o1|0o100), calledFlags, "Flags 必须经 ToProtoOpenFlags 清洗")
		require.NotNil(t, calledSID)
		require.Equal(t, mgr.CurrentSessionID(), *calledSID)

		// 验证 out 填充
		require.Equal(t, uint64(2002), out.EntryOut.NodeId)
		require.Equal(t, uint64(42), out.OpenOut.Fh)
		require.True(t, (out.OpenOut.OpenFlags&fuse.FOPEN_DIRECT_IO) != 0, "DirectIO 必须附加 FOPEN_DIRECT_IO")

		// 验证句柄会话血缘已绑定
		sid, err := mgr.ResolveHandleSession(42)
		require.NoError(t, err)
		require.Equal(t, mgr.CurrentSessionID(), sid)
	})

	t.Run("server error mapped to fuse.Status", func(t *testing.T) {
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
		mockSrv.createFn = func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("file exists"))
		}

		var out fuse.CreateOut
		status := fs.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
			Mode:     0o644,
		}, "exists.txt", &out)
		require.Equal(t, fuse.Status(syscall.EEXIST), status)
	})
}

func TestRawFS_Open(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)

	t.Run("read-only mode blocks write open but allows read open", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, mgr, fusefs.Config{ReadOnly: true, RootIno: 1001})

		// 1. O_WRONLY -> EROFS
		var out fuse.OpenOut
		status := roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_WRONLY),
		}, &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)

		// 2. O_RDWR -> EROFS
		status = roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_RDWR),
		}, &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)

		// 3. O_RDONLY | O_TRUNC -> EROFS
		status = roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_RDONLY | syscall.O_TRUNC),
		}, &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)

		// 4. O_RDONLY -> 成功放行
		mockSrv.openFn = func(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
			return connect.NewResponse(&storagev1.OpenResponse{
				Fh:        100,
				OpenFlags: 0,
			}), nil
		}
		status = roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_RDONLY),
		}, &out)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(100), out.Fh)
	})

	t.Run("open passes cleaned flags and session_id", func(t *testing.T) {
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{
			RootIno:  1001,
			DirectIO: true,
		})

		var calledIno uint64
		var calledFlags uint32
		var calledSID *uint64

		mockSrv.openFn = func(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
			calledIno = req.Msg.GetIno()
			calledFlags = req.Msg.GetFlags()
			calledSID = req.Msg.SessionId

			return connect.NewResponse(&storagev1.OpenResponse{
				Fh:        88,
				OpenFlags: 0,
			}), nil
		}

		var out fuse.OpenOut
		status := fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Flags:    uint32(syscall.O_RDWR | syscall.O_APPEND | 0x8000), // 携带高位
		}, &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2002), calledIno)
		require.Equal(t, uint32(0o2|0o2000), calledFlags)
		require.NotNil(t, calledSID)
		require.Equal(t, mgr.CurrentSessionID(), *calledSID)
		require.Equal(t, uint64(88), out.Fh)
		require.True(t, (out.OpenFlags&fuse.FOPEN_DIRECT_IO) != 0)

		// 检查 handle session
		sid, err := mgr.ResolveHandleSession(88)
		require.NoError(t, err)
		require.Equal(t, mgr.CurrentSessionID(), sid)
	})
}

func TestRawFS_Flush(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})

	t.Run("unbound handle returns EBADF", func(t *testing.T) {
		status := fs.Flush(nil, &fuse.FlushIn{
			InHeader:  fuse.InHeader{NodeId: 2001},
			Fh:        999,
			LockOwner: 12345,
		})
		require.Equal(t, fuse.Status(syscall.EBADF), status)
	})

	t.Run("successful flush passes lock_owner and session_id", func(t *testing.T) {
		mgr.BindHandle(55, mgr.CurrentSessionID())

		var calledIno, calledFh uint64
		var calledLockOwner, calledSID *uint64

		mockSrv.flushFn = func(ctx context.Context, req *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error) {
			calledIno = req.Msg.GetIno()
			calledFh = req.Msg.GetFh()
			calledLockOwner = req.Msg.LockOwner
			calledSID = req.Msg.SessionId
			return connect.NewResponse(&storagev1.FlushResponse{}), nil
		}

		status := fs.Flush(nil, &fuse.FlushIn{
			InHeader:  fuse.InHeader{NodeId: 2001},
			Fh:        55,
			LockOwner: 777,
		})

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2001), calledIno)
		require.Equal(t, uint64(55), calledFh)
		require.NotNil(t, calledLockOwner)
		require.Equal(t, uint64(777), *calledLockOwner)
		require.NotNil(t, calledSID)
		require.Equal(t, mgr.CurrentSessionID(), *calledSID)
	})
}

func TestRawFS_Fsync(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})

	t.Run("unbound handle returns EBADF", func(t *testing.T) {
		status := fs.Fsync(nil, &fuse.FsyncIn{
			InHeader:   fuse.InHeader{NodeId: 2001},
			Fh:         999,
			FsyncFlags: 0,
		})
		require.Equal(t, fuse.Status(syscall.EBADF), status)
	})

	t.Run("datasync flag differentiated correctly", func(t *testing.T) {
		mgr.BindHandle(66, mgr.CurrentSessionID())

		var calledDatasync bool
		mockSrv.fsyncFn = func(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
			calledDatasync = req.Msg.GetDatasync()
			return connect.NewResponse(&storagev1.FsyncResponse{}), nil
		}

		// 1. FsyncFlags = 0 -> datasync = false
		status := fs.Fsync(nil, &fuse.FsyncIn{
			InHeader:   fuse.InHeader{NodeId: 2001},
			Fh:         66,
			FsyncFlags: 0,
		})
		require.Equal(t, fuse.OK, status)
		require.False(t, calledDatasync)

		// 2. FsyncFlags = 1 -> datasync = true
		status = fs.Fsync(nil, &fuse.FsyncIn{
			InHeader:   fuse.InHeader{NodeId: 2001},
			Fh:         66,
			FsyncFlags: 1,
		})
		require.Equal(t, fuse.OK, status)
		require.True(t, calledDatasync)
	})
}

func TestRawFS_Release(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})

	t.Run("release unbinds handle and passes sanitized flags", func(t *testing.T) {
		fh := uint64(77)
		mgr.BindHandle(fh, mgr.CurrentSessionID())
		_, ok := mgr.LookupHandleSession(fh)
		require.True(t, ok, "句柄必须先已绑定")

		var calledIno, calledFh uint64
		var calledLockOwner *uint64
		var calledReleaseFlags uint32
		var calledSID *uint64

		mockSrv.releaseFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			calledIno = req.Msg.GetIno()
			calledFh = req.Msg.GetFh()
			calledLockOwner = req.Msg.LockOwner
			calledReleaseFlags = req.Msg.GetReleaseFlags()
			calledSID = req.Msg.SessionId
			return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
		}

		fs.Release(nil, &fuse.ReleaseIn{
			InHeader:     fuse.InHeader{NodeId: 2001},
			Fh:           fh,
			LockOwner:    999,
			ReleaseFlags: 0x1f, // 高位包含杂乱标志，期望 & 0x3 保留 0x3
		})

		// 验证参数透传与清洗
		require.Equal(t, uint64(2001), calledIno)
		require.Equal(t, fh, calledFh)
		require.NotNil(t, calledLockOwner)
		require.Equal(t, uint64(999), *calledLockOwner)
		require.Equal(t, uint32(0x3), calledReleaseFlags)
		require.NotNil(t, calledSID)
		require.Equal(t, mgr.CurrentSessionID(), *calledSID)

		// 验证 handle 必定已被解绑
		_, ok = mgr.LookupHandleSession(fh)
		require.False(t, ok, "Release 后句柄血缘必须已被清理")
	})

	t.Run("transient network error enqueues async retry until server succeeds or returns EBADF", func(t *testing.T) {
		retryFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
			RootIno:              1001,
			ReleaseRetryInterval: 15 * time.Millisecond,
		})
		t.Cleanup(retryFS.OnUnmount)

		// 1. 初次超时/不可用失败，后续重试成功
		fh1 := uint64(177)
		sid := mgr.CurrentSessionID()
		mgr.BindHandle(fh1, sid)

		var attempts1 atomic.Int32
		mockSrv.releaseFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			if req.Msg.GetFh() != fh1 {
				return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
			}
			n := attempts1.Add(1)
			if n < 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporary network failure"))
			}
			return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
		}

		retryFS.Release(nil, &fuse.ReleaseIn{
			InHeader:     fuse.InHeader{NodeId: 2001},
			Fh:           fh1,
			LockOwner:    888,
			ReleaseFlags: 0x1,
		})

		// 本地句柄立即解绑
		_, ok := mgr.LookupHandleSession(fh1)
		require.False(t, ok, "初次 Release 失败后本地句柄仍须立即解绑")

		require.Eventually(t, func() bool {
			return attempts1.Load() == 3
		}, 150*time.Millisecond, 5*time.Millisecond, "后台重试队列应自动重发直至服务端返回成功")

		// 成功后不应再继续重试
		time.Sleep(45 * time.Millisecond)
		require.Equal(t, int32(3), attempts1.Load())

		// 2. 初次失败入队，重试期间服务端返回 EBADF（视为已完成销毁并出队）
		fh2 := uint64(178)
		mgr.BindHandle(fh2, sid)

		var attempts2 atomic.Int32
		mockSrv.releaseFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			if req.Msg.GetFh() != fh2 {
				return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
			}
			n := attempts2.Add(1)
			if n == 1 {
				return nil, connect.NewError(connect.CodeDeadlineExceeded, errors.New("timeout"))
			}
			cerr := connect.NewError(connect.CodeFailedPrecondition, errors.New("bad file descriptor"))
			detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
				Reason: "EBADF",
				Domain: "bowl.storage",
			})
			require.NoError(t, err)
			cerr.AddDetail(detail)
			return nil, cerr
		}

		retryFS.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Fh:       fh2,
		})

		require.Eventually(t, func() bool {
			return attempts2.Load() == 2
		}, 150*time.Millisecond, 5*time.Millisecond, "重试收到 EBADF 后应停止重试")

		time.Sleep(45 * time.Millisecond)
		require.Equal(t, int32(2), attempts2.Load(), "EBADF 视为已完成销毁，出队后不再重发")
	})

	t.Run("session rotation drops stale session pending release items", func(t *testing.T) {
		var rotateFlag atomic.Bool
		localSrv := &handleMockStorageServer{
			renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				recreated := rotateFlag.Swap(false)
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Millisecond)),
					SessionRecreated: recreated,
				}), nil
			},
		}
		localClient := setupHandleTestRPCServer(t, localSrv)
		localMgr, err := session.New(context.Background(), localClient, session.Config{
			MinInterval: 15 * time.Millisecond,
			MaxInterval: 30 * time.Millisecond,
		})
		require.NoError(t, err)
		require.NoError(t, localMgr.Start(context.Background()))
		t.Cleanup(func() { _ = localMgr.Close() })

		oldSID := localMgr.CurrentSessionID()
		retryFS := fusefs.NewRawFS(localClient, localMgr, fusefs.Config{
			RootIno:              1001,
			ReleaseRetryInterval: 80 * time.Millisecond,
		})
		t.Cleanup(retryFS.OnUnmount)

		fh := uint64(179)
		localMgr.BindHandle(fh, oldSID)

		var releaseCalls atomic.Int32
		localSrv.releaseFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			releaseCalls.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("network down"))
		}

		// 初次 Release 失败入队（仅触发 1 次同步调用）
		retryFS.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Fh:       fh,
		})
		require.Equal(t, int32(1), releaseCalls.Load())

		// 触发会话超期轮换
		rotateFlag.Store(true)
		require.Eventually(t, func() bool {
			return localMgr.CurrentSessionID() != oldSID
		}, 500*time.Millisecond, 10*time.Millisecond, "会话应完成轮换")

		// 等待超过一个 ReleaseRetryInterval 周期，验证旧会话项被直接丢弃，不再发起额外 Release RPC
		time.Sleep(120 * time.Millisecond)
		require.Equal(t, int32(1), releaseCalls.Load(), "会话轮换后队列应自动丢弃旧会话待释放项，不再发送 RPC")
	})
}
