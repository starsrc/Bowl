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
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type mockTopologyStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mkdirFn  func(context.Context, *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error)
	mknodFn  func(context.Context, *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error)
	rmdirFn  func(context.Context, *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error)
	unlinkFn func(context.Context, *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error)
}

func (m *mockTopologyStorageServer) Mkdir(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	if m.mkdirFn != nil {
		return m.mkdirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockTopologyStorageServer) Mknod(ctx context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	if m.mknodFn != nil {
		return m.mknodFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockTopologyStorageServer) Rmdir(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
	if m.rmdirFn != nil {
		return m.rmdirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockTopologyStorageServer) Unlink(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	if m.unlinkFn != nil {
		return m.unlinkFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func setupTestTopologyRPCServer(t *testing.T, srv *mockTopologyStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func makeStorageError(code connect.Code, reason, message string) error {
	connErr := connect.NewError(code, errors.New(message))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "bowl.storage",
	})
	if err == nil {
		connErr.AddDetail(detail)
	}
	return connErr
}

func TestRawFS_Mkdir(t *testing.T) {
	mockSrv := &mockTopologyStorageServer{}
	client := setupTestTopologyRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("mkdir in root rewrites parent_ino, passes uid/gid/mode, and fills entry out", func(t *testing.T) {
		var receivedReq *storagev1.MkdirRequest
		mockSrv.mkdirFn = func(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.MkdirResponse{
				Inode: &storagev1.Inode{
					Ino:        2002,
					Generation: 10,
					Attr: &storagev1.InodeAttribute{
						Mode:  0o755 | syscall.S_IFDIR,
						Uid:   1000,
						Gid:   1000,
						Nlink: 2,
						Size:  4096,
					},
				},
				EntryTimeoutSeconds: 1.5,
				AttrTimeoutSeconds:  2.5,
			}), nil
		}

		in := &fuse.MkdirIn{
			InHeader: fuse.InHeader{
				NodeId: 1, // FUSE_ROOT_ID
			},
			Mode:  0o755 | syscall.S_IFDIR,
			Umask: 0o022,
		}
		in.Uid = 1000
		in.Gid = 1000
		var out fuse.EntryOut
		status := rawFS.Mkdir(nil, in, "testdir", &out)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(1001), receivedReq.GetParentIno(), "根节点 1 应重写为服务端 root_ino 1001")
		require.Equal(t, []byte("testdir"), receivedReq.GetName())
		require.Equal(t, uint32(0o755|syscall.S_IFDIR), receivedReq.GetMode(), "未开启 CAP_DONT_MASK 时内核已扣减 umask，模式位直接透传")
		require.Equal(t, uint32(1000), receivedReq.GetUid())
		require.Equal(t, uint32(1000), receivedReq.GetGid())

		require.Equal(t, uint64(2002), out.NodeId)
		require.Equal(t, uint64(10), out.Generation)
		require.Equal(t, 1500*time.Millisecond, out.EntryTimeout())
		require.Equal(t, 2500*time.Millisecond, out.AttrTimeout())
		require.Equal(t, uint32(0o755|syscall.S_IFDIR), out.Attr.Mode)
	})

	t.Run("mkdir in ro mode returns EROFS without issuing RPC", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{
			DiskID:   "00000000-0000-0000-0000-000000000001",
			RootIno:  1001,
			ReadOnly: true,
		})

		mockSrv.mkdirFn = func(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
			t.Fatal("只读挂载下严禁向服务端发起 Mkdir RPC")
			return nil, nil
		}

		in := &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Mode:     0o755,
		}
		var out fuse.EntryOut
		status := roFS.Mkdir(nil, in, "sub", &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("mkdir with anti-aliasing violation returns ENOENT", func(t *testing.T) {
		in := &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: 1001}, // 直接传入等于 RootIno 的非 1 编号
			Mode:     0o755,
		}
		var out fuse.EntryOut
		status := rawFS.Mkdir(nil, in, "sub", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("mkdir already exists returns EEXIST", func(t *testing.T) {
		mockSrv.mkdirFn = func(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
			return nil, makeStorageError(connect.CodeAlreadyExists, "EEXIST", "directory already exists")
		}

		in := &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Mode:     0o755,
		}
		var out fuse.EntryOut
		status := rawFS.Mkdir(nil, in, "exists", &out)
		require.Equal(t, fuse.Status(syscall.EEXIST), status)
	})

	t.Run("mkdir in non-root directory keeps parent ino unchanged", func(t *testing.T) {
		var receivedReq *storagev1.MkdirRequest
		mockSrv.mkdirFn = func(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.MkdirResponse{
				Inode: &storagev1.Inode{
					Ino:        3001,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Mode: 0o755 | syscall.S_IFDIR,
					},
				},
			}), nil
		}

		in := &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Mode:     0o755,
		}
		var out fuse.EntryOut
		status := rawFS.Mkdir(nil, in, "nested", &out)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2002), receivedReq.GetParentIno(), "非根目录 NodeId 应原样传给服务端")
	})
}

func TestRawFS_Mknod(t *testing.T) {
	mockSrv := &mockTopologyStorageServer{}
	client := setupTestTopologyRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("mknod fifo in root rewrites parent_ino, passes uid/gid/mode/rdev, and fills entry out", func(t *testing.T) {
		var receivedReq *storagev1.MknodRequest
		mockSrv.mknodFn = func(ctx context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.MknodResponse{
				Inode: &storagev1.Inode{
					Ino:        3003,
					Generation: 7,
					Attr: &storagev1.InodeAttribute{
						Mode:  0o644 | syscall.S_IFIFO,
						Uid:   1001,
						Gid:   1002,
						Nlink: 1,
						Rdev:  0,
					},
				},
				EntryTimeoutSeconds: 1.0,
				AttrTimeoutSeconds:  2.0,
			}), nil
		}

		in := &fuse.MknodIn{
			InHeader: fuse.InHeader{
				NodeId: 1,
			},
			Mode: 0o644 | syscall.S_IFIFO,
			Rdev: 0,
		}
		in.Uid = 1001
		in.Gid = 1002
		var out fuse.EntryOut
		status := rawFS.Mknod(nil, in, "myfifo", &out)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(1001), receivedReq.GetParentIno())
		require.Equal(t, []byte("myfifo"), receivedReq.GetName())
		require.Equal(t, uint32(0o644|syscall.S_IFIFO), receivedReq.GetMode())
		require.Equal(t, uint32(1001), receivedReq.GetUid())
		require.Equal(t, uint32(1002), receivedReq.GetGid())
		require.Equal(t, uint32(0), receivedReq.GetRdev())

		require.Equal(t, uint64(3003), out.NodeId)
		require.Equal(t, uint64(7), out.Generation)
		require.Equal(t, 1*time.Second, out.EntryTimeout())
		require.Equal(t, 2*time.Second, out.AttrTimeout())
		require.Equal(t, uint32(0o644|syscall.S_IFIFO), out.Attr.Mode)
	})

	t.Run("mknod in ro mode returns EROFS without issuing RPC", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{
			DiskID:   "00000000-0000-0000-0000-000000000001",
			RootIno:  1001,
			ReadOnly: true,
		})

		mockSrv.mknodFn = func(ctx context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
			t.Fatal("只读挂载下严禁向服务端发起 Mknod RPC")
			return nil, nil
		}

		in := &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Mode:     0o644 | syscall.S_IFIFO,
		}
		var out fuse.EntryOut
		status := roFS.Mknod(nil, in, "fifo", &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("mknod with anti-aliasing violation returns ENOENT", func(t *testing.T) {
		in := &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: 1001},
			Mode:     0o644 | syscall.S_IFIFO,
		}
		var out fuse.EntryOut
		status := rawFS.Mknod(nil, in, "fifo", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("mknod in non-root directory keeps parent ino unchanged", func(t *testing.T) {
		var receivedReq *storagev1.MknodRequest
		mockSrv.mknodFn = func(ctx context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.MknodResponse{
				Inode: &storagev1.Inode{
					Ino: 4001,
				},
			}), nil
		}

		in := &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Mode:     0o644 | syscall.S_IFIFO,
		}
		var out fuse.EntryOut
		status := rawFS.Mknod(nil, in, "fifo2", &out)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2002), receivedReq.GetParentIno())
	})
}

func TestRawFS_Rmdir(t *testing.T) {
	mockSrv := &mockTopologyStorageServer{}
	client := setupTestTopologyRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("rmdir in root rewrites parent_ino to root_ino and succeeds", func(t *testing.T) {
		var receivedReq *storagev1.RmdirRequest
		mockSrv.rmdirFn = func(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.RmdirResponse{}), nil
		}

		header := &fuse.InHeader{NodeId: 1}
		status := rawFS.Rmdir(nil, header, "emptydir")

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(1001), receivedReq.GetParentIno())
		require.Equal(t, []byte("emptydir"), receivedReq.GetName())
	})

	t.Run("rmdir in ro mode returns EROFS without issuing RPC", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{
			DiskID:   "00000000-0000-0000-0000-000000000001",
			RootIno:  1001,
			ReadOnly: true,
		})

		mockSrv.rmdirFn = func(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
			t.Fatal("只读挂载下严禁向服务端发起 Rmdir RPC")
			return nil, nil
		}

		header := &fuse.InHeader{NodeId: 1}
		status := roFS.Rmdir(nil, header, "emptydir")
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("rmdir with anti-aliasing violation returns ENOENT", func(t *testing.T) {
		header := &fuse.InHeader{NodeId: 1001}
		status := rawFS.Rmdir(nil, header, "emptydir")
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("rmdir in non-root directory keeps parent ino unchanged", func(t *testing.T) {
		var receivedReq *storagev1.RmdirRequest
		mockSrv.rmdirFn = func(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.RmdirResponse{}), nil
		}

		header := &fuse.InHeader{NodeId: 2002}
		status := rawFS.Rmdir(nil, header, "subdir")
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2002), receivedReq.GetParentIno())
	})

	t.Run("rmdir non-empty directory maps to ENOTEMPTY", func(t *testing.T) {
		mockSrv.rmdirFn = func(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
			return nil, makeStorageError(connect.CodeFailedPrecondition, "ENOTEMPTY", "directory not empty")
		}

		header := &fuse.InHeader{NodeId: 1}
		status := rawFS.Rmdir(nil, header, "nonempty")
		require.Equal(t, fuse.Status(syscall.ENOTEMPTY), status)
	})

	t.Run("rmdir non-directory maps to ENOTDIR", func(t *testing.T) {
		mockSrv.rmdirFn = func(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
			return nil, makeStorageError(connect.CodeInvalidArgument, "ENOTDIR", "not a directory")
		}

		header := &fuse.InHeader{NodeId: 1}
		status := rawFS.Rmdir(nil, header, "regfile")
		require.Equal(t, fuse.Status(syscall.ENOTDIR), status)
	})
}

func TestRawFS_Unlink(t *testing.T) {
	mockSrv := &mockTopologyStorageServer{}
	client := setupTestTopologyRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("unlink in root rewrites parent_ino to root_ino and succeeds", func(t *testing.T) {
		var receivedReq *storagev1.UnlinkRequest
		mockSrv.unlinkFn = func(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
		}

		header := &fuse.InHeader{NodeId: 1}
		status := rawFS.Unlink(nil, header, "file.txt")

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(1001), receivedReq.GetParentIno())
		require.Equal(t, []byte("file.txt"), receivedReq.GetName())
	})

	t.Run("unlink in ro mode returns EROFS without issuing RPC", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{
			DiskID:   "00000000-0000-0000-0000-000000000001",
			RootIno:  1001,
			ReadOnly: true,
		})

		mockSrv.unlinkFn = func(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
			t.Fatal("只读挂载下严禁向服务端发起 Unlink RPC")
			return nil, nil
		}

		header := &fuse.InHeader{NodeId: 1}
		status := roFS.Unlink(nil, header, "file.txt")
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("unlink with anti-aliasing violation returns ENOENT", func(t *testing.T) {
		header := &fuse.InHeader{NodeId: 1001}
		status := rawFS.Unlink(nil, header, "file.txt")
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("unlink in non-root directory keeps parent ino unchanged", func(t *testing.T) {
		var receivedReq *storagev1.UnlinkRequest
		mockSrv.unlinkFn = func(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
		}

		header := &fuse.InHeader{NodeId: 2002}
		status := rawFS.Unlink(nil, header, "child.txt")
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2002), receivedReq.GetParentIno())
	})

	t.Run("unlink directory returns EISDIR", func(t *testing.T) {
		mockSrv.unlinkFn = func(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
			return nil, makeStorageError(connect.CodeInvalidArgument, "EISDIR", "is a directory")
		}

		header := &fuse.InHeader{NodeId: 1}
		status := rawFS.Unlink(nil, header, "subdir")
		require.Equal(t, fuse.Status(syscall.EISDIR), status)
	})

	t.Run("unlink nonexistent returns ENOENT", func(t *testing.T) {
		mockSrv.unlinkFn = func(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
			return nil, makeStorageError(connect.CodeNotFound, "ENOENT", "entry not found")
		}

		header := &fuse.InHeader{NodeId: 1}
		status := rawFS.Unlink(nil, header, "notfound.txt")
		require.Equal(t, fuse.Status(syscall.ENOENT), status)
	})
}
