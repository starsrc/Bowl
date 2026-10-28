package fusefs_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type mockSetAttrFallocateServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	setAttrFn      func(context.Context, *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error)
	getAttrFn      func(context.Context, *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error)
	fallocateFn    func(context.Context, *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *mockSetAttrFallocateServer) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	if m.setAttrFn != nil {
		return m.setAttrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockSetAttrFallocateServer) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	if m.getAttrFn != nil {
		return m.getAttrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockSetAttrFallocateServer) Fallocate(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	if m.fallocateFn != nil {
		return m.fallocateFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockSetAttrFallocateServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func setupSetAttrFallocateRPCServer(t *testing.T, srv *mockSetAttrFallocateServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func newTestSessionManagerForSetAttr(t *testing.T, client storagev1connect.StorageServiceClient) *session.Manager {
	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	return mgr
}

func sampleInode(ino uint64, mode uint32, size uint64) *storagev1.Inode {
	return &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    mode,
			Size:    size,
			Nlink:   1,
			Uid:     1000,
			Gid:     1000,
			Blksize: 4096,
			Blocks:  (size + 511) / 512,
			Atime:   timestamppb.Now(),
			Mtime:   timestamppb.Now(),
			Ctime:   timestamppb.Now(),
		},
	}
}

func TestRawFS_SetAttr(t *testing.T) {
	t.Run("readonly filesystem returns EROFS", func(t *testing.T) {
		fs := fusefs.NewRawFS(nil, nil, fusefs.Config{
			ReadOnly: true,
			RootIno:  1001,
		})
		var out fuse.AttrOut
		status := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: 2},
				Valid:    fuse.FATTR_MODE,
				Mode:     0644,
			},
		}, &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("invalid root inode aliasing returns ENOENT", func(t *testing.T) {
		fs := fusefs.NewRawFS(nil, nil, fusefs.Config{
			RootIno: 1001,
		})
		var out fuse.AttrOut
		status := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: 1001}, // RootIno != 1 直接传 1001 被拒绝
				Valid:    fuse.FATTR_MODE,
				Mode:     0644,
			},
		}, &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("root inode 1 maps to RootIno 1001", func(t *testing.T) {
		var capturedReq *storagev1.SetAttrRequest
		srv := &mockSetAttrFallocateServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				capturedReq = req.Msg
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode:              sampleInode(1001, 0755|syscall.S_IFDIR, 4096),
					AttrTimeoutSeconds: 1.0,
				}), nil
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		fs := fusefs.NewRawFS(client, nil, fusefs.Config{RootIno: 1001})

		var out fuse.AttrOut
		status := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: 1}, // 根目录
				Valid:    fuse.FATTR_MODE,
				Mode:     0750,
			},
		}, &out)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.Equal(t, uint64(1001), capturedReq.GetIno())
		require.Equal(t, uint32(0750), capturedReq.GetMode())
		require.Equal(t, uint64(1), out.Attr.Ino) // 出站映射为 1
	})

	t.Run("single attribute updates: MODE, UID, GID, SIZE", func(t *testing.T) {
		var capturedReq *storagev1.SetAttrRequest
		srv := &mockSetAttrFallocateServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				capturedReq = req.Msg
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode:              sampleInode(2002, 0640|syscall.S_IFREG, 1024),
					AttrTimeoutSeconds: 2.0,
				}), nil
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		fs := fusefs.NewRawFS(client, nil, fusefs.Config{RootIno: 1001})

		in := &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: 2002},
				Valid:    fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_SIZE,
				Mode:     0640,
				Owner:    fuse.Owner{Uid: 1001, Gid: 1002},
				Size:     1024,
			},
		}
		var out fuse.AttrOut
		status := fs.SetAttr(nil, in, &out)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.Equal(t, uint64(2002), capturedReq.GetIno())
		require.Equal(t, uint32(0640), capturedReq.GetMode())
		require.Equal(t, uint32(1001), capturedReq.GetUid())
		require.Equal(t, uint32(1002), capturedReq.GetGid())
		require.Equal(t, uint64(1024), capturedReq.GetSize())
		require.Nil(t, capturedReq.Atime)
		require.Nil(t, capturedReq.Mtime)
		require.False(t, capturedReq.GetAtimeNow())
		require.False(t, capturedReq.GetMtimeNow())
		require.False(t, capturedReq.GetCtimeNow())
		require.Nil(t, capturedReq.Fh)
		require.Nil(t, capturedReq.SessionId)
	})

	t.Run("explicit ATIME and MTIME timestamps", func(t *testing.T) {
		var capturedReq *storagev1.SetAttrRequest
		srv := &mockSetAttrFallocateServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				capturedReq = req.Msg
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode:              sampleInode(2002, 0644|syscall.S_IFREG, 100),
					AttrTimeoutSeconds: 1.5,
				}), nil
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		fs := fusefs.NewRawFS(client, nil, fusefs.Config{RootIno: 1001})

		in := &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader:  fuse.InHeader{NodeId: 2002},
				Valid:     fuse.FATTR_ATIME | fuse.FATTR_MTIME | fuse.FATTR_CTIME,
				Atime:     1700000000,
				Atimensec: 123456,
				Mtime:     1700000001,
				Mtimensec: 654321,
			},
		}
		var out fuse.AttrOut
		status := fs.SetAttr(nil, in, &out)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.NotNil(t, capturedReq.Atime)
		require.Equal(t, int64(1700000000), capturedReq.Atime.GetSeconds())
		require.Equal(t, int32(123456), capturedReq.Atime.GetNanos())
		require.NotNil(t, capturedReq.Mtime)
		require.Equal(t, int64(1700000001), capturedReq.Mtime.GetSeconds())
		require.Equal(t, int32(654321), capturedReq.Mtime.GetNanos())
		require.False(t, capturedReq.GetAtimeNow())
		require.False(t, capturedReq.GetMtimeNow())
		require.True(t, capturedReq.GetCtimeNow())
	})

	t.Run("priority protection: ATIME_NOW and MTIME_NOW override explicit timestamps", func(t *testing.T) {
		var capturedReq *storagev1.SetAttrRequest
		srv := &mockSetAttrFallocateServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				capturedReq = req.Msg
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode:              sampleInode(2002, 0644|syscall.S_IFREG, 100),
					AttrTimeoutSeconds: 1.0,
				}), nil
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		fs := fusefs.NewRawFS(client, nil, fusefs.Config{RootIno: 1001})

		// 同时置位 ATIME_NOW + ATIME 以及 MTIME_NOW + MTIME
		in := &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader:  fuse.InHeader{NodeId: 2002},
				Valid:     fuse.FATTR_ATIME_NOW | fuse.FATTR_ATIME | fuse.FATTR_MTIME_NOW | fuse.FATTR_MTIME,
				Atime:     1700000000,
				Atimensec: 123456,
				Mtime:     1700000001,
				Mtimensec: 654321,
			},
		}
		var out fuse.AttrOut
		status := fs.SetAttr(nil, in, &out)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.True(t, capturedReq.GetAtimeNow(), "ATIME_NOW 必须为 true")
		require.Nil(t, capturedReq.Atime, "ATIME 必须为 nil 以免触发服务端互斥 CEL")
		require.True(t, capturedReq.GetMtimeNow(), "MTIME_NOW 必须为 true")
		require.Nil(t, capturedReq.Mtime, "MTIME 必须为 nil 以免触发服务端互斥 CEL")
	})

	t.Run("FATTR_FH binding with valid session handle", func(t *testing.T) {
		var capturedReq *storagev1.SetAttrRequest
		srv := &mockSetAttrFallocateServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				capturedReq = req.Msg
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode:              sampleInode(2002, 0644|syscall.S_IFREG, 500),
					AttrTimeoutSeconds: 1.0,
				}), nil
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		mgr := newTestSessionManagerForSetAttr(t, client)
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})

		mgr.BindHandle(42, mgr.CurrentSessionID())

		in := &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: 2002},
				Valid:    fuse.FATTR_SIZE | fuse.FATTR_FH,
				Size:     500,
				Fh:       42,
			},
		}
		var out fuse.AttrOut
		status := fs.SetAttr(nil, in, &out)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.NotNil(t, capturedReq.Fh)
		require.Equal(t, uint64(42), capturedReq.GetFh())
		require.NotNil(t, capturedReq.SessionId)
		require.Equal(t, mgr.CurrentSessionID(), capturedReq.GetSessionId())
	})

	t.Run("FATTR_FH with invalid handle returns EBADF", func(t *testing.T) {
		client := setupSetAttrFallocateRPCServer(t, &mockSetAttrFallocateServer{})
		mgr := newTestSessionManagerForSetAttr(t, client)
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})

		// 未绑定 handle 999
		in := &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: 2002},
				Valid:    fuse.FATTR_SIZE | fuse.FATTR_FH,
				Size:     500,
				Fh:       999,
			},
		}
		var out fuse.AttrOut
		status := fs.SetAttr(nil, in, &out)
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("empty update bitmask falls back to GetAttr without sending SetAttr RPC", func(t *testing.T) {
		setAttrCalled := false
		getAttrCalled := false
		srv := &mockSetAttrFallocateServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				setAttrCalled = true
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("empty update not allowed"))
			},
			getAttrFn: func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
				getAttrCalled = true
				require.Equal(t, uint64(2002), req.Msg.GetIno())
				return connect.NewResponse(&storagev1.GetAttrResponse{
					Inode:              sampleInode(2002, 0644|syscall.S_IFREG, 1024),
					AttrTimeoutSeconds: 3.5,
				}), nil
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		fs := fusefs.NewRawFS(client, nil, fusefs.Config{RootIno: 1001})

		// 仅传入 FATTR_LOCKOWNER，无任何有效属性更新位
		in := &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader:  fuse.InHeader{NodeId: 2002},
				Valid:     fuse.FATTR_LOCKOWNER,
				LockOwner: 12345,
			},
		}
		var out fuse.AttrOut
		status := fs.SetAttr(nil, in, &out)
		require.Equal(t, fuse.OK, status)
		require.False(t, setAttrCalled, "不得发送 SetAttr RPC")
		require.True(t, getAttrCalled, "必须回退调用 GetAttr RPC")
		require.Equal(t, uint64(2002), out.Attr.Ino)
		require.Equal(t, uint64(1024), out.Attr.Size)
		require.Equal(t, 3500*time.Millisecond, out.Timeout())
	})

	t.Run("server error mapped to fuse.Status", func(t *testing.T) {
		srv := &mockSetAttrFallocateServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				return nil, connect.NewError(connect.CodeNotFound, errors.New("inode not found"))
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		fs := fusefs.NewRawFS(client, nil, fusefs.Config{RootIno: 1001})

		in := &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: 9999},
				Valid:    fuse.FATTR_MODE,
				Mode:     0644,
			},
		}
		var out fuse.AttrOut
		status := fs.SetAttr(nil, in, &out)
		require.Equal(t, fuse.ENOENT, status)
	})
}

func TestRawFS_Fallocate(t *testing.T) {
	t.Run("readonly filesystem returns EROFS", func(t *testing.T) {
		fs := fusefs.NewRawFS(nil, nil, fusefs.Config{
			ReadOnly: true,
			RootIno:  1001,
		})
		status := fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: 2},
			Fh:       1,
			Offset:   0,
			Length:   4096,
			Mode:     0,
		})
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("invalid handle returns EBADF", func(t *testing.T) {
		client := setupSetAttrFallocateRPCServer(t, &mockSetAttrFallocateServer{})
		mgr := newTestSessionManagerForSetAttr(t, client)
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})

		status := fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Fh:       999, // 未绑定的句柄
			Offset:   0,
			Length:   4096,
			Mode:     0,
		})
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("unsupported modes return EOPNOTSUPP", func(t *testing.T) {
		client := setupSetAttrFallocateRPCServer(t, &mockSetAttrFallocateServer{})
		mgr := newTestSessionManagerForSetAttr(t, client)
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
		mgr.BindHandle(42, mgr.CurrentSessionID())

		unsupportedModes := []uint32{
			unix.FALLOC_FL_PUNCH_HOLE,                                // 单独 0x02 未带 KEEP_SIZE
			unix.FALLOC_FL_COLLAPSE_RANGE,                            // 0x08
			unix.FALLOC_FL_ZERO_RANGE,                                // 0x10
			unix.FALLOC_FL_INSERT_RANGE,                              // 0x20
			unix.FALLOC_FL_COLLAPSE_RANGE | unix.FALLOC_FL_KEEP_SIZE, // 混合不支持
		}

		for _, mode := range unsupportedModes {
			status := fs.Fallocate(nil, &fuse.FallocateIn{
				InHeader: fuse.InHeader{NodeId: 2002},
				Fh:       42,
				Offset:   0,
				Length:   4096,
				Mode:     mode,
			})
			require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), status, "mode %x 应当返回 EOPNOTSUPP", mode)
		}
	})

	t.Run("supported modes call StorageService.Fallocate with correct parameters", func(t *testing.T) {
		supportedModes := []uint32{
			0,
			unix.FALLOC_FL_KEEP_SIZE,
			unix.FALLOC_FL_PUNCH_HOLE | unix.FALLOC_FL_KEEP_SIZE,
		}

		for _, mode := range supportedModes {
			var capturedReq *storagev1.FallocateRequest
			srv := &mockSetAttrFallocateServer{
				fallocateFn: func(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
					capturedReq = req.Msg
					return connect.NewResponse(&storagev1.FallocateResponse{}), nil
				},
			}
			client := setupSetAttrFallocateRPCServer(t, srv)
			mgr := newTestSessionManagerForSetAttr(t, client)
			fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
			mgr.BindHandle(42, mgr.CurrentSessionID())

			status := fs.Fallocate(nil, &fuse.FallocateIn{
				InHeader: fuse.InHeader{NodeId: 2002},
				Fh:       42,
				Offset:   8192,
				Length:   16384,
				Mode:     mode,
			})
			require.Equal(t, fuse.OK, status)
			require.NotNil(t, capturedReq)
			require.Equal(t, uint64(2002), capturedReq.GetIno())
			require.Equal(t, uint64(42), capturedReq.GetFh())
			require.Equal(t, uint64(8192), capturedReq.GetOffset())
			require.Equal(t, uint64(16384), capturedReq.GetLength())
			require.Equal(t, mode, capturedReq.GetMode())
			require.NotNil(t, capturedReq.SessionId)
			require.Equal(t, mgr.CurrentSessionID(), capturedReq.GetSessionId())
		}
	})

	t.Run("root inode 1 maps to RootIno in Fallocate", func(t *testing.T) {
		var capturedReq *storagev1.FallocateRequest
		srv := &mockSetAttrFallocateServer{
			fallocateFn: func(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
				capturedReq = req.Msg
				return connect.NewResponse(&storagev1.FallocateResponse{}), nil
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		mgr := newTestSessionManagerForSetAttr(t, client)
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
		mgr.BindHandle(10, mgr.CurrentSessionID())

		status := fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: 1}, // 根目录
			Fh:       10,
			Offset:   0,
			Length:   4096,
			Mode:     0,
		})
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.Equal(t, uint64(1001), capturedReq.GetIno())
	})

	t.Run("server error mapped to fuse.Status", func(t *testing.T) {
		srv := &mockSetAttrFallocateServer{
			fallocateFn: func(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
				return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("disk space full"))
			},
		}
		client := setupSetAttrFallocateRPCServer(t, srv)
		mgr := newTestSessionManagerForSetAttr(t, client)
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
		mgr.BindHandle(42, mgr.CurrentSessionID())

		status := fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Fh:       42,
			Offset:   0,
			Length:   4096,
			Mode:     0,
		})
		require.Equal(t, fuse.Status(syscall.ENOSPC), status)
	})
}
