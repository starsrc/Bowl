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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type mockLinkStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	symlinkFn  func(context.Context, *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error)
	readlinkFn func(context.Context, *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error)
	linkFn     func(context.Context, *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error)
}

func (m *mockLinkStorageServer) Symlink(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
	if m.symlinkFn != nil {
		return m.symlinkFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockLinkStorageServer) Readlink(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	if m.readlinkFn != nil {
		return m.readlinkFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockLinkStorageServer) Link(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	if m.linkFn != nil {
		return m.linkFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func setupTestLinkRPCServer(t *testing.T, srv *mockLinkStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func TestRawFS_Symlink(t *testing.T) {
	mockSrv := &mockLinkStorageServer{}
	client := setupTestLinkRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("symlink in root directory rewrites parent_ino, passes target/name and fills EntryOut", func(t *testing.T) {
		var receivedReq *storagev1.SymlinkRequest
		mockSrv.symlinkFn = func(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.SymlinkResponse{
				Inode: &storagev1.Inode{
					Ino:        2002,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Size:    11,
						Blocks:  1,
						Mode:    uint32(syscall.S_IFLNK | 0777),
						Nlink:   1,
						Uid:     1000,
						Gid:     1000,
						Atime:   timestamppb.New(time.Unix(1700000000, 0)),
						Mtime:   timestamppb.New(time.Unix(1700000000, 0)),
						Ctime:   timestamppb.New(time.Unix(1700000000, 0)),
						Blksize: 4096,
					},
				},
				EntryTimeoutSeconds: 2.5,
				AttrTimeoutSeconds:  3.5,
			}), nil
		}

		header := &fuse.InHeader{
			NodeId: 1, // root inode in kernel
			Caller: fuse.Caller{
				Owner: fuse.Owner{
					Uid: 1000,
					Gid: 1000,
				},
			},
		}

		var out fuse.EntryOut
		// go-fuse Symlink: pointedTo 是目标路径，linkName 是在父目录下创建的文件名
		status := rawFS.Symlink(nil, header, "/var/target", "symlink_name", &out)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(1001), receivedReq.GetParentIno(), "root Inode 1 必须重写为服务端的 RootIno 1001")
		require.Equal(t, []byte("symlink_name"), receivedReq.GetName(), "linkName 必须映射为 Name")
		require.Equal(t, []byte("/var/target"), receivedReq.GetTarget(), "pointedTo 必须映射为 Target")
		require.Equal(t, uint32(1000), receivedReq.GetUid())
		require.Equal(t, uint32(1000), receivedReq.GetGid())

		require.Equal(t, uint64(2002), out.NodeId)
		require.Equal(t, uint64(1), out.Generation)
		require.Equal(t, uint32(syscall.S_IFLNK|0777), out.Attr.Mode)
		require.Equal(t, uint64(11), out.Attr.Size)
		require.Equal(t, uint32(1), out.Attr.Nlink)
		require.Equal(t, 2*time.Second+500*time.Millisecond, out.EntryTimeout())
		require.Equal(t, 3*time.Second+500*time.Millisecond, out.AttrTimeout())
	})

	t.Run("symlink in non-root directory keeps parent_ino unchanged", func(t *testing.T) {
		var receivedReq *storagev1.SymlinkRequest
		mockSrv.symlinkFn = func(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.SymlinkResponse{
				Inode: &storagev1.Inode{
					Ino: 2003,
					Attr: &storagev1.InodeAttribute{
						Mode: uint32(syscall.S_IFLNK | 0777),
					},
				},
			}), nil
		}

		header := &fuse.InHeader{NodeId: 3003}
		var out fuse.EntryOut
		status := rawFS.Symlink(nil, header, "relative/path", "sub_link", &out)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(3003), receivedReq.GetParentIno())
		require.Equal(t, []byte("sub_link"), receivedReq.GetName())
		require.Equal(t, []byte("relative/path"), receivedReq.GetTarget())
	})

	t.Run("symlink in ro mode returns EROFS without issuing RPC", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{
			DiskID:   "00000000-0000-0000-0000-000000000001",
			RootIno:  1001,
			ReadOnly: true,
		})

		mockSrv.symlinkFn = func(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
			t.Fatal("只读挂载下严禁向服务端发起 Symlink RPC")
			return nil, nil
		}

		header := &fuse.InHeader{NodeId: 1}
		var out fuse.EntryOut
		status := roFS.Symlink(nil, header, "target", "link", &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("symlink with anti-aliasing violation returns ENOENT", func(t *testing.T) {
		header := &fuse.InHeader{NodeId: 1001} // 内核直接传入与 RootIno 相同的值且不为 1
		var out fuse.EntryOut
		status := rawFS.Symlink(nil, header, "target", "link", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("symlink name conflict returns EEXIST", func(t *testing.T) {
		mockSrv.symlinkFn = func(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
			return nil, makeStorageError(connect.CodeAlreadyExists, "EEXIST", "entry already exists")
		}

		header := &fuse.InHeader{NodeId: 1}
		var out fuse.EntryOut
		status := rawFS.Symlink(nil, header, "target", "existing_link", &out)
		require.Equal(t, fuse.Status(syscall.EEXIST), status)
	})

	t.Run("symlink parent not found returns ENOENT", func(t *testing.T) {
		mockSrv.symlinkFn = func(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
			return nil, makeStorageError(connect.CodeNotFound, "ENOENT", "parent not found")
		}

		header := &fuse.InHeader{NodeId: 4004}
		var out fuse.EntryOut
		status := rawFS.Symlink(nil, header, "target", "link", &out)
		require.Equal(t, fuse.Status(syscall.ENOENT), status)
	})
}

func TestRawFS_Readlink(t *testing.T) {
	mockSrv := &mockLinkStorageServer{}
	client := setupTestLinkRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("readlink normal symlink returns byte slice without loss", func(t *testing.T) {
		expectedTarget := []byte("/data/special_\xff\xfe_path/dest")
		var receivedReq *storagev1.ReadlinkRequest
		mockSrv.readlinkFn = func(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.ReadlinkResponse{
				Target: expectedTarget,
			}), nil
		}

		header := &fuse.InHeader{NodeId: 2005}
		target, status := rawFS.Readlink(nil, header)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(2005), receivedReq.GetIno())
		require.Equal(t, expectedTarget, target)
	})

	t.Run("readlink root Inode rewrites to RootIno", func(t *testing.T) {
		var receivedReq *storagev1.ReadlinkRequest
		mockSrv.readlinkFn = func(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.ReadlinkResponse{
				Target: []byte("target"),
			}), nil
		}

		header := &fuse.InHeader{NodeId: 1}
		target, status := rawFS.Readlink(nil, header)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(1001), receivedReq.GetIno())
		require.Equal(t, []byte("target"), target)
	})

	t.Run("readlink anti-aliasing violation returns ENOENT", func(t *testing.T) {
		header := &fuse.InHeader{NodeId: 1001}
		target, status := rawFS.Readlink(nil, header)
		require.Equal(t, fuse.ENOENT, status)
		require.Nil(t, target)
	})

	t.Run("readlink non-symlink returns EINVAL", func(t *testing.T) {
		mockSrv.readlinkFn = func(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
			return nil, makeStorageError(connect.CodeInvalidArgument, "EINVAL", "not a symlink")
		}

		header := &fuse.InHeader{NodeId: 2006}
		target, status := rawFS.Readlink(nil, header)

		require.Equal(t, fuse.Status(syscall.EINVAL), status)
		require.Nil(t, target)
	})

	t.Run("readlink nonexistent node returns ENOENT", func(t *testing.T) {
		mockSrv.readlinkFn = func(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
			return nil, makeStorageError(connect.CodeNotFound, "ENOENT", "node not found")
		}

		header := &fuse.InHeader{NodeId: 9999}
		target, status := rawFS.Readlink(nil, header)

		require.Equal(t, fuse.Status(syscall.ENOENT), status)
		require.Nil(t, target)
	})
}

func TestRawFS_Link(t *testing.T) {
	mockSrv := &mockLinkStorageServer{}
	client := setupTestLinkRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("link file in root rewrites newParentIno, sends LinkRequest and fills EntryOut", func(t *testing.T) {
		var receivedReq *storagev1.LinkRequest
		mockSrv.linkFn = func(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.LinkResponse{
				Inode: &storagev1.Inode{
					Ino:        2001,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Size:    1024,
						Blocks:  2,
						Mode:    uint32(syscall.S_IFREG | 0644),
						Nlink:   2, // 递增后硬链接数
						Uid:     1000,
						Gid:     1000,
						Atime:   timestamppb.New(time.Unix(1700000000, 0)),
						Mtime:   timestamppb.New(time.Unix(1700000000, 0)),
						Ctime:   timestamppb.New(time.Unix(1700000005, 0)),
						Blksize: 4096,
					},
				},
				EntryTimeoutSeconds: 2.0,
				AttrTimeoutSeconds:  3.0,
			}), nil
		}

		input := &fuse.LinkIn{
			InHeader: fuse.InHeader{
				NodeId: 1, // 目标父目录为根目录
			},
			Oldnodeid: 2001, // 源节点 Inode
		}

		var out fuse.EntryOut
		status := rawFS.Link(nil, input, "hardlink.txt", &out)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(2001), receivedReq.GetIno(), "源节点 Inode 正确传递")
		require.Equal(t, uint64(1001), receivedReq.GetNewParentIno(), "目标父目录 Inode 1 重写为 RootIno 1001")
		require.Equal(t, []byte("hardlink.txt"), receivedReq.GetNewName())

		require.Equal(t, uint64(2001), out.NodeId)
		require.Equal(t, uint64(1), out.Generation)
		require.Equal(t, uint32(2), out.Attr.Nlink, "Nlink 应回填递增后的 2")
		require.Equal(t, uint64(1024), out.Attr.Size)
		require.Equal(t, 2*time.Second, out.EntryTimeout())
		require.Equal(t, 3*time.Second, out.AttrTimeout())
	})

	t.Run("link file to non-root directory keeps newParentIno unchanged", func(t *testing.T) {
		var receivedReq *storagev1.LinkRequest
		mockSrv.linkFn = func(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.LinkResponse{
				Inode: &storagev1.Inode{
					Ino: 2001,
					Attr: &storagev1.InodeAttribute{
						Nlink: 3,
					},
				},
			}), nil
		}

		input := &fuse.LinkIn{
			InHeader: fuse.InHeader{
				NodeId: 3003, // 目标父目录为非根子目录
			},
			Oldnodeid: 2001,
		}

		var out fuse.EntryOut
		status := rawFS.Link(nil, input, "hl2.txt", &out)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(2001), receivedReq.GetIno())
		require.Equal(t, uint64(3003), receivedReq.GetNewParentIno())
		require.Equal(t, []byte("hl2.txt"), receivedReq.GetNewName())
	})

	t.Run("link in ro mode returns EROFS without issuing RPC", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{
			DiskID:   "00000000-0000-0000-0000-000000000001",
			RootIno:  1001,
			ReadOnly: true,
		})

		mockSrv.linkFn = func(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
			t.Fatal("只读挂载下严禁向服务端发起 Link RPC")
			return nil, nil
		}

		input := &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: 1},
			Oldnodeid: 2001,
		}
		var out fuse.EntryOut
		status := roFS.Link(nil, input, "hl.txt", &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("link with anti-aliasing violation on Oldnodeid returns ENOENT", func(t *testing.T) {
		input := &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: 1},
			Oldnodeid: 1001, // 混叠
		}
		var out fuse.EntryOut
		status := rawFS.Link(nil, input, "hl.txt", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("link with anti-aliasing violation on NodeId returns ENOENT", func(t *testing.T) {
		input := &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: 1001}, // 混叠
			Oldnodeid: 2001,
		}
		var out fuse.EntryOut
		status := rawFS.Link(nil, input, "hl.txt", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("link directory returns EPERM", func(t *testing.T) {
		mockSrv.linkFn = func(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
			return nil, makeStorageError(connect.CodePermissionDenied, "EPERM", "hard link not allowed for directory")
		}

		input := &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: 1},
			Oldnodeid: 3003, // 目录 Inode
		}
		var out fuse.EntryOut
		status := rawFS.Link(nil, input, "dir_link", &out)
		require.Equal(t, fuse.Status(syscall.EPERM), status)
	})

	t.Run("link existing target name returns EEXIST", func(t *testing.T) {
		mockSrv.linkFn = func(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
			return nil, makeStorageError(connect.CodeAlreadyExists, "EEXIST", "entry already exists")
		}

		input := &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: 1},
			Oldnodeid: 2001,
		}
		var out fuse.EntryOut
		status := rawFS.Link(nil, input, "existing_file", &out)
		require.Equal(t, fuse.Status(syscall.EEXIST), status)
	})
}
