package fusefs_test

import (
	"context"
	"errors"
	"math"
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

func TestRawFS_BasicsAndInoMapping(t *testing.T) {
	cfg := fusefs.Config{
		DiskID:     "00000000-0000-0000-0000-000000000001",
		RootIno:    1001,
		ReadOnly:   true,
		DirectIO:   true,
		AllowOther: true,
		Debug:      true,
	}

	rawFS := fusefs.NewRawFS(nil, nil, cfg)
	require.NotNil(t, rawFS)

	// 1. Inode 映射测试
	// 内核根目录 1 应映射为服务端 1001
	serverIno, status := rawFS.ToServerIno(1)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(1001), serverIno)

	// 非 1 且等于 RootIno 的内核访问必须被拦截返回 ENOENT（防混叠）
	_, status = rawFS.ToServerIno(1001)
	require.Equal(t, fuse.ENOENT, status)

	// 常规非根 Inode 原样返回
	serverIno, status = rawFS.ToServerIno(2002)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(2002), serverIno)

	// 服务端 Inode 转内核 Inode
	require.Equal(t, uint64(1), rawFS.ToKernelIno(1001))
	require.Equal(t, uint64(2002), rawFS.ToKernelIno(2002))

	// 当 RootIno 本身就是 1 时的特殊情况
	cfgRoot1 := fusefs.Config{RootIno: 1}
	fsRoot1 := fusefs.NewRawFS(nil, nil, cfgRoot1)
	serverIno, status = fsRoot1.ToServerIno(1)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(1), serverIno)

	// 2. 只读校验
	require.Equal(t, fuse.Status(syscall.EROFS), rawFS.CheckWritable())
	writableFS := fusefs.NewRawFS(nil, nil, fusefs.Config{ReadOnly: false})
	require.Equal(t, fuse.OK, writableFS.CheckWritable())

	// 3. NextRequestID 严格在 [1, math.MaxInt64] 范围内单调递增
	id1 := rawFS.NextRequestID()
	id2 := rawFS.NextRequestID()
	require.True(t, id1 >= 1 && id1 <= math.MaxInt64)
	require.True(t, id2 >= 1 && id2 <= math.MaxInt64)
	require.True(t, id2 > id1)

	// 4. OnUnmount 回调触发
	unmounted := false
	cbFS := fusefs.NewRawFS(nil, nil, fusefs.Config{
		OnUnmount: func() {
			unmounted = true
		},
	})
	cbFS.OnUnmount()
	require.True(t, unmounted)
}

func TestRawFS_BuildMountOptions(t *testing.T) {
	cfg := fusefs.Config{
		DiskID:     "test-disk-123",
		RootIno:    42,
		ReadOnly:   true,
		AllowOther: true,
		Debug:      true,
	}

	opts := fusefs.BuildMountOptions(cfg)
	require.NotNil(t, opts)
	require.Equal(t, "bowl:test-disk-123", opts.FsName)
	require.Equal(t, "bowl", opts.Name)
	require.Contains(t, opts.Options, "default_permissions")
	require.Contains(t, opts.Options, "ro")
	require.True(t, opts.AllowOther)
	require.True(t, opts.EnableLocks)
	require.False(t, opts.DisableXAttrs)
	require.Equal(t, 1<<20, opts.MaxWrite)
	require.Equal(t, 1<<20, opts.MaxReadAhead)
	require.True(t, opts.DirectMount)
	require.True(t, opts.Debug)

	// 非只读时 Options 不应包含 ro
	optsRW := fusefs.BuildMountOptions(fusefs.Config{DiskID: "rw", ReadOnly: false})
	require.NotContains(t, optsRW.Options, "ro")
}

func TestRawFS_FillAttrAndEntry(t *testing.T) {
	rawFS := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 100})

	now := time.Now()
	ino := &storagev1.Inode{
		Ino:        100, // 映射到 1
		Generation: 5,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o755 | syscall.S_IFDIR,
			Size:    4096,
			Nlink:   2,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Blksize: 65536,
			Blocks:  8,
			Atime:   timestamppb.New(now),
			Mtime:   timestamppb.New(now.Add(time.Second)),
			Ctime:   timestamppb.New(now.Add(2 * time.Second)),
			Flags:   16,
		},
	}

	var attr fuse.Attr
	rawFS.FillAttr(&attr, ino)
	require.Equal(t, uint64(1), attr.Ino)
	require.Equal(t, uint64(4096), attr.Size)
	require.Equal(t, uint32(0o755|syscall.S_IFDIR), attr.Mode)
	require.Equal(t, uint32(2), attr.Nlink)
	require.Equal(t, uint32(1000), attr.Uid)
	require.Equal(t, uint32(1000), attr.Gid)
	require.Equal(t, uint32(65536), attr.Blksize)
	require.Equal(t, uint64(8), attr.Blocks)
	require.Equal(t, uint64(now.Unix()), attr.Atime)
	require.Equal(t, uint32(now.Nanosecond()), attr.Atimensec)

	var entryOut fuse.EntryOut
	rawFS.FillEntryOut(&entryOut, ino, 1.5, 2.5)
	require.Equal(t, uint64(1), entryOut.NodeId)
	require.Equal(t, uint64(5), entryOut.Generation)
	require.Equal(t, 1500*time.Millisecond, entryOut.EntryTimeout())
	require.Equal(t, 2500*time.Millisecond, entryOut.AttrTimeout())
	require.Equal(t, uint64(1), entryOut.Attr.Ino)

	var attrOut fuse.AttrOut
	rawFS.FillAttrOut(&attrOut, ino, 3.0)
	require.Equal(t, 3*time.Second, attrOut.Timeout())
	require.Equal(t, uint64(1), attrOut.Attr.Ino)
}

type mockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	lookupFn  func(context.Context, *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error)
	getAttrFn func(context.Context, *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error)
	statFSFn  func(context.Context, *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error)
}

func (m *mockStorageServer) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	if m.lookupFn != nil {
		return m.lookupFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockStorageServer) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	if m.getAttrFn != nil {
		return m.getAttrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockStorageServer) StatFS(ctx context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	if m.statFSFn != nil {
		return m.statFSFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func setupTestRPCServer(t *testing.T, srv *mockStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func TestRawFS_Lookup(t *testing.T) {
	mockSrv := &mockStorageServer{}
	client := setupTestRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("lookup in root succeeds with server ino translation", func(t *testing.T) {
		mockSrv.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetParentIno(), "内核 NodeId 1 应转换为服务端的 1001")
			require.Equal(t, "file.txt", string(req.Msg.GetName()))

			return connect.NewResponse(&storagev1.LookupResponse{
				Inode: &storagev1.Inode{
					Ino:        2001,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Mode:    0o644 | syscall.S_IFREG,
						Size:    1024,
						Nlink:   1,
						Blksize: 4096,
						Blocks:  2,
					},
				},
				EntryTimeoutSeconds: 1.0,
				AttrTimeoutSeconds:  2.0,
			}), nil
		}

		var out fuse.EntryOut
		header := &fuse.InHeader{NodeId: 1} // FUSE_ROOT_ID
		status := rawFS.Lookup(nil, header, "file.txt", &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2001), out.NodeId)
		require.Equal(t, uint64(1024), out.Attr.Size)
		require.Equal(t, 1*time.Second, out.EntryTimeout())
		require.Equal(t, 2*time.Second, out.AttrTimeout())
	})

	t.Run("lookup not found returns ENOENT", func(t *testing.T) {
		mockSrv.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("entry not found"))
		}

		var out fuse.EntryOut
		header := &fuse.InHeader{NodeId: 1}
		status := rawFS.Lookup(nil, header, "notexist.txt", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("lookup anti-aliasing prevents direct access to root_ino", func(t *testing.T) {
		called := false
		mockSrv.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			called = true
			return nil, nil
		}

		var out fuse.EntryOut
		header := &fuse.InHeader{NodeId: 1001} // 内核越过映射直接访问 1001
		status := rawFS.Lookup(nil, header, "file.txt", &out)
		require.Equal(t, fuse.ENOENT, status)
		require.False(t, called, "防混叠拦截后不应向远端发送 RPC")
	})
}

func TestRawFS_GetAttr(t *testing.T) {
	mockSrv := &mockStorageServer{}
	client := setupTestRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("getattr root translates 1001 back to 1", func(t *testing.T) {
		mockSrv.getAttrFn = func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetIno())

			return connect.NewResponse(&storagev1.GetAttrResponse{
				Inode: &storagev1.Inode{
					Ino:        1001,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Mode:    0o755 | syscall.S_IFDIR,
						Size:    4096,
						Nlink:   2,
						Blksize: 4096,
						Blocks:  8,
					},
				},
				AttrTimeoutSeconds: 1.5,
			}), nil
		}

		var out fuse.AttrOut
		in := &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 1}}
		status := rawFS.GetAttr(nil, in, &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(1), out.Attr.Ino, "出站 Ino 应转为 1 (ToKernelIno)")
		require.Equal(t, uint64(4096), out.Attr.Size)
		require.Equal(t, 1500*time.Millisecond, out.Timeout())
	})

	t.Run("getattr not found returns ENOENT", func(t *testing.T) {
		mockSrv.getAttrFn = func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("inode not found"))
		}

		var out fuse.AttrOut
		in := &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 9999}}
		status := rawFS.GetAttr(nil, in, &out)
		require.Equal(t, fuse.ENOENT, status)
	})
}

func TestRawFS_StatFs(t *testing.T) {
	mockSrv := &mockStorageServer{}
	client := setupTestRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("statfs fills all fields accurately", func(t *testing.T) {
		mockSrv.statFSFn = func(ctx context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetIno())

			return connect.NewResponse(&storagev1.StatFSResponse{
				TotalBytes: 10 * 1024 * 1024 * 1024,
				UsedBytes:  2 * 1024 * 1024 * 1024,
				AvailBytes: 8 * 1024 * 1024 * 1024,
				Bsize:      4096,
				Frsize:     4096,
				Blocks:     2621440,
				Bfree:      2097152,
				Bavail:     2097152,
				Files:      128,
				Ffree:      0,
				Namelen:    255,
			}), nil
		}

		var out fuse.StatfsOut
		in := &fuse.InHeader{NodeId: 1}
		status := rawFS.StatFs(nil, in, &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2621440), out.Blocks)
		require.Equal(t, uint64(2097152), out.Bfree)
		require.Equal(t, uint64(2097152), out.Bavail)
		require.Equal(t, uint64(128), out.Files)
		require.Equal(t, uint64(0), out.Ffree)
		require.Equal(t, uint32(4096), out.Bsize)
		require.Equal(t, uint32(4096), out.Frsize)
		require.Equal(t, uint32(255), out.NameLen)
	})
}
