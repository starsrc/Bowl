package fusefs_test

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type mockIoctlServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	getAttrFn      func(context.Context, *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error)
	setAttrFn      func(context.Context, *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *mockIoctlServer) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	if m.getAttrFn != nil {
		return m.getAttrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockIoctlServer) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	if m.setAttrFn != nil {
		return m.setAttrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockIoctlServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func setupIoctlRPCServer(t *testing.T, srv *mockIoctlServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func newTestSessionManagerForIoctl(t *testing.T, client storagev1connect.StorageServiceClient) *session.Manager {
	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	return mgr
}

func sampleInodeWithFlags(ino uint64, flags uint32) *storagev1.Inode {
	now := timestamppb.Now()
	return &storagev1.Inode{
		Ino: ino,
		Attr: &storagev1.InodeAttribute{
			Size:    1024,
			Blocks:  2,
			Mode:    0o100644,
			Nlink:   1,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Blksize: 4096,
			Atime:   now,
			Mtime:   now,
			Ctime:   now,
			Flags:   flags,
		},
	}
}

func TestRawFS_Ioctl(t *testing.T) {
	const rootIno = 100

	t.Run("FS_IOC_GETFLAGS_64Bit_Success", func(t *testing.T) {
		srv := &mockIoctlServer{
			getAttrFn: func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
				assert.Equal(t, uint64(200), req.Msg.GetIno())
				return connect.NewResponse(&storagev1.GetAttrResponse{
					Inode: sampleInodeWithFlags(200, fusefs.ProtoFlagImmutable),
				}), nil
			},
		}
		client := setupIoctlRPCServer(t, srv)
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 200},
			Cmd:      fusefs.FS_IOC_GETFLAGS,
			OutSize:  8,
		}
		out := &fuse.IoctlOut{}
		outbuf := make([]byte, 8)

		st := fs.Ioctl(nil, in, nil, out, outbuf)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, int32(0), out.Result)
		val := binary.LittleEndian.Uint64(outbuf)
		require.Equal(t, uint64(fusefs.FS_IMMUTABLE_FL), val)
	})

	t.Run("FS_IOC_GETFLAGS_32Bit_AppendFlag_Success", func(t *testing.T) {
		srv := &mockIoctlServer{
			getAttrFn: func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
				assert.Equal(t, uint64(201), req.Msg.GetIno())
				return connect.NewResponse(&storagev1.GetAttrResponse{
					Inode: sampleInodeWithFlags(201, fusefs.ProtoFlagAppend),
				}), nil
			},
		}
		client := setupIoctlRPCServer(t, srv)
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 201},
			Cmd:      fusefs.FS_IOC32_GETFLAGS,
			OutSize:  4,
		}
		out := &fuse.IoctlOut{}
		outbuf := make([]byte, 4)

		st := fs.Ioctl(nil, in, nil, out, outbuf)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, int32(0), out.Result)
		val := binary.LittleEndian.Uint32(outbuf)
		require.Equal(t, fusefs.FS_APPEND_FL, val)
	})

	t.Run("FS_IOC_GETFLAGS_BothFlags_And_RootInoMapping", func(t *testing.T) {
		srv := &mockIoctlServer{
			getAttrFn: func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
				assert.Equal(t, uint64(rootIno), req.Msg.GetIno(), "fuse.FUSE_ROOT_ID 应正确映射为 cfg.RootIno")
				return connect.NewResponse(&storagev1.GetAttrResponse{
					Inode: sampleInodeWithFlags(rootIno, fusefs.ProtoFlagImmutable|fusefs.ProtoFlagAppend),
				}), nil
			},
		}
		client := setupIoctlRPCServer(t, srv)
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Cmd:      fusefs.FS_IOC_GETFLAGS,
			OutSize:  8,
		}
		out := &fuse.IoctlOut{}
		outbuf := make([]byte, 8)

		st := fs.Ioctl(nil, in, nil, out, outbuf)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, int32(0), out.Result)
		val := binary.LittleEndian.Uint64(outbuf)
		require.Equal(t, uint64(fusefs.FS_IMMUTABLE_FL|fusefs.FS_APPEND_FL), val)
	})

	t.Run("FS_IOC_GETFLAGS_ShortBuffer_ReturnsEINVAL", func(t *testing.T) {
		client := setupIoctlRPCServer(t, &mockIoctlServer{})
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 200},
			Cmd:      fusefs.FS_IOC_GETFLAGS,
			OutSize:  2,
		}
		out := &fuse.IoctlOut{}
		outbuf := make([]byte, 2)

		st := fs.Ioctl(nil, in, nil, out, outbuf)
		require.Equal(t, fuse.Status(syscall.EINVAL), st)
	})

	t.Run("FS_IOC_GETFLAGS_RPCError_ReturnsMappedStatus", func(t *testing.T) {
		srv := &mockIoctlServer{
			getAttrFn: func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
				return nil, connect.NewError(connect.CodeNotFound, errors.New("inode not found"))
			},
		}
		client := setupIoctlRPCServer(t, srv)
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 999},
			Cmd:      fusefs.FS_IOC_GETFLAGS,
			OutSize:  4,
		}
		out := &fuse.IoctlOut{}
		outbuf := make([]byte, 4)

		st := fs.Ioctl(nil, in, nil, out, outbuf)
		require.Equal(t, fuse.ENOENT, st)
	})

	t.Run("FS_IOC_SETFLAGS_64Bit_Success", func(t *testing.T) {
		var capturedFlags *uint32
		srv := &mockIoctlServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				assert.Equal(t, uint64(300), req.Msg.GetIno())
				capturedFlags = req.Msg.Flags
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode: sampleInodeWithFlags(300, req.Msg.GetFlags()),
				}), nil
			},
		}
		client := setupIoctlRPCServer(t, srv)
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 300},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
			InSize:   8,
		}
		inbuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(inbuf, uint64(fusefs.FS_IMMUTABLE_FL))
		out := &fuse.IoctlOut{}

		st := fs.Ioctl(nil, in, inbuf, out, nil)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, int32(0), out.Result)
		require.NotNil(t, capturedFlags)
		require.Equal(t, fusefs.ProtoFlagImmutable, *capturedFlags)
	})

	t.Run("FS_IOC_SETFLAGS_32Bit_AppendFlag_Success", func(t *testing.T) {
		var capturedFlags *uint32
		srv := &mockIoctlServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				assert.Equal(t, uint64(301), req.Msg.GetIno())
				capturedFlags = req.Msg.Flags
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode: sampleInodeWithFlags(301, req.Msg.GetFlags()),
				}), nil
			},
		}
		client := setupIoctlRPCServer(t, srv)
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 301},
			Cmd:      fusefs.FS_IOC32_SETFLAGS,
			InSize:   4,
		}
		inbuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(inbuf, fusefs.FS_APPEND_FL)
		out := &fuse.IoctlOut{}

		st := fs.Ioctl(nil, in, inbuf, out, nil)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, int32(0), out.Result)
		require.NotNil(t, capturedFlags)
		require.Equal(t, fusefs.ProtoFlagAppend, *capturedFlags)
	})

	t.Run("FS_IOC_SETFLAGS_Clear_Success", func(t *testing.T) {
		var capturedFlags *uint32
		srv := &mockIoctlServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				capturedFlags = req.Msg.Flags
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode: sampleInodeWithFlags(req.Msg.GetIno(), req.Msg.GetFlags()),
				}), nil
			},
		}
		client := setupIoctlRPCServer(t, srv)
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 302},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
			InSize:   4,
		}
		inbuf := make([]byte, 4) // 0
		out := &fuse.IoctlOut{}

		st := fs.Ioctl(nil, in, inbuf, out, nil)
		require.Equal(t, fuse.OK, st)
		require.NotNil(t, capturedFlags)
		require.Equal(t, uint32(0), *capturedFlags)
	})

	t.Run("FS_IOC_SETFLAGS_ReadOnly_ReturnsEROFS", func(t *testing.T) {
		client := setupIoctlRPCServer(t, &mockIoctlServer{})
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno, ReadOnly: true})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 300},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
			InSize:   4,
		}
		inbuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(inbuf, fusefs.FS_IMMUTABLE_FL)

		st := fs.Ioctl(nil, in, inbuf, &fuse.IoctlOut{}, nil)
		require.Equal(t, fuse.Status(syscall.EROFS), st)
	})

	t.Run("FS_IOC_SETFLAGS_ShortBuffer_ReturnsEINVAL", func(t *testing.T) {
		client := setupIoctlRPCServer(t, &mockIoctlServer{})
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 300},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
			InSize:   2,
		}
		inbuf := make([]byte, 2)

		st := fs.Ioctl(nil, in, inbuf, &fuse.IoctlOut{}, nil)
		require.Equal(t, fuse.Status(syscall.EINVAL), st)
	})

	t.Run("FS_IOC_SETFLAGS_UnsupportedFlags_ReturnsEOPNOTSUPP", func(t *testing.T) {
		client := setupIoctlRPCServer(t, &mockIoctlServer{})
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		// 0x00000040 为 FS_NODUMP_FL，不在支持列表中
		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 300},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
			InSize:   4,
		}
		inbuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(inbuf, 0x00000040)

		st := fs.Ioctl(nil, in, inbuf, &fuse.IoctlOut{}, nil)
		require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), st)
	})

	t.Run("FS_IOC_SETFLAGS_HighBitUnsupported_ReturnsEOPNOTSUPP", func(t *testing.T) {
		client := setupIoctlRPCServer(t, &mockIoctlServer{})
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 300},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
			InSize:   8,
		}
		inbuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(inbuf, 0x100000000|uint64(fusefs.FS_IMMUTABLE_FL))

		st := fs.Ioctl(nil, in, inbuf, &fuse.IoctlOut{}, nil)
		require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), st)
	})

	t.Run("FS_IOC_SETFLAGS_RPCError_ReturnsMappedStatus", func(t *testing.T) {
		srv := &mockIoctlServer{
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				connErr := connect.NewError(connect.CodePermissionDenied, errors.New("permission denied"))
				detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EPERM", Domain: "bowl.storage"})
				if err == nil {
					connErr.AddDetail(detail)
				}
				return nil, connErr
			},
		}
		client := setupIoctlRPCServer(t, srv)
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 300},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
			InSize:   4,
		}
		inbuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(inbuf, fusefs.FS_IMMUTABLE_FL)

		st := fs.Ioctl(nil, in, inbuf, &fuse.IoctlOut{}, nil)
		require.Equal(t, fuse.EPERM, st)
	})

	t.Run("UnknownCmd_ReturnsENOTTY", func(t *testing.T) {
		client := setupIoctlRPCServer(t, &mockIoctlServer{})
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: 300},
			Cmd:      0x5414, // TIOCSWINSZ
		}

		st := fs.Ioctl(nil, in, nil, &fuse.IoctlOut{}, nil)
		require.Equal(t, fuse.Status(syscall.ENOTTY), st)
	})

	t.Run("InvalidNodeId_ReturnsENOENT", func(t *testing.T) {
		client := setupIoctlRPCServer(t, &mockIoctlServer{})
		sm := newTestSessionManagerForIoctl(t, client)
		fs := fusefs.NewRawFS(client, sm, fusefs.Config{RootIno: rootIno})

		// 当 NodeId 等于 RootIno (100) 且 RootIno != FUSE_ROOT_ID 时，ToServerIno 返回 ENOENT
		in := &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: rootIno},
			Cmd:      fusefs.FS_IOC_GETFLAGS,
			OutSize:  4,
		}

		st := fs.Ioctl(nil, in, nil, &fuse.IoctlOut{}, make([]byte, 4))
		require.Equal(t, fuse.ENOENT, st)
	})
}
