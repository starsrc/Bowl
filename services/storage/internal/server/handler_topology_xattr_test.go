package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

func setupTopologyXattrTestServer(t *testing.T) (*metamock.MockManager, storagev1connect.StorageServiceClient, func()) {
	t.Helper()

	mockMgr := metamock.NewMockManager(t)
	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)

	return mockMgr, client, ts.Close
}

func TestHandler_Link(t *testing.T) {
	ctx := context.Background()

	t.Run("成功创建硬链接并返回更新后的 Inode 与缓存时效", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		atime := time.Date(2026, 10, 26, 10, 1, 2, 123456789, time.UTC)
		mtime := time.Date(2026, 10, 26, 11, 2, 3, 234567890, time.UTC)
		ctime := time.Date(2026, 10, 26, 12, 3, 4, 345678901, time.UTC)
		expectedInode := &meta.Inode{
			Ino:        1001,
			Generation: 1,
			Attr: meta.Attribute{
				Mode:      0o100644,
				Size:      2048,
				Nlink:     2,
				UID:       1001,
				GID:       2002,
				Rdev:      259,
				Blksize:   4096,
				Blocks:    4,
				Atime:     atime,
				Mtime:     mtime,
				Ctime:     ctime,
				Flags:     meta.FlagImmutable,
				ParentIno: 1,
			},
			EntryTimeoutSeconds: 2.5,
			AttrTimeoutSeconds:  1.5,
		}

		mockMgr.EXPECT().
			Link(mock.Anything, meta.LinkParams{
				Ino:          1001,
				NewParentIno: 2,
				NewName:      []byte("hardlink.txt"),
			}).
			Return(expectedInode, nil).
			Once()

		req := connect.NewRequest(&storagev1.LinkRequest{
			Ino:          1001,
			NewParentIno: 2,
			NewName:      []byte("hardlink.txt"),
		})
		resp, err := client.Link(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 2.5, resp.Msg.GetEntryTimeoutSeconds())
		assert.Equal(t, 1.5, resp.Msg.GetAttrTimeoutSeconds())

		inode := resp.Msg.GetInode()
		require.NotNil(t, inode)
		assert.Equal(t, uint64(1001), inode.GetIno())
		assert.Equal(t, uint64(1), inode.GetGeneration())

		attr := inode.GetAttr()
		require.NotNil(t, attr)
		assert.Equal(t, uint32(0o100644), attr.GetMode())
		assert.Equal(t, uint64(2048), attr.GetSize())
		assert.Equal(t, uint32(2), attr.GetNlink())
		assert.Equal(t, uint32(1001), attr.GetUid())
		assert.Equal(t, uint32(2002), attr.GetGid())
		assert.Equal(t, uint32(259), attr.GetRdev())
		assert.Equal(t, uint32(meta.FlagImmutable), attr.GetFlags())
		assert.Equal(t, uint32(4096), attr.GetBlksize())
		assert.Equal(t, uint64(4), attr.GetBlocks())
		assert.True(t, attr.GetAtime().AsTime().Equal(atime), "Atime 必须满足纳秒级时间戳相等")
		assert.True(t, attr.GetMtime().AsTime().Equal(mtime), "Mtime 必须满足纳秒级时间戳相等")
		assert.True(t, attr.GetCtime().AsTime().Equal(ctime), "Ctime 必须满足纳秒级时间戳相等")
	})

	t.Run("跨设备链接失败映射为 EXDEV", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Link(mock.Anything, meta.LinkParams{
				Ino:          1001,
				NewParentIno: 2,
				NewName:      []byte("cross_dev.txt"),
			}).
			Return(nil, meta.ErrCrossDevice).
			Once()

		req := connect.NewRequest(&storagev1.LinkRequest{
			Ino:          1001,
			NewParentIno: 2,
			NewName:      []byte("cross_dev.txt"),
		})
		resp, err := client.Link(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeFailedPrecondition, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EXDEV", info.GetReason())
	})

	t.Run("链接数超限映射为 EMLINK", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Link(mock.Anything, meta.LinkParams{
				Ino:          1001,
				NewParentIno: 2,
				NewName:      []byte("too_many.txt"),
			}).
			Return(nil, meta.ErrTooManyLinks).
			Once()

		req := connect.NewRequest(&storagev1.LinkRequest{
			Ino:          1001,
			NewParentIno: 2,
			NewName:      []byte("too_many.txt"),
		})
		resp, err := client.Link(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeResourceExhausted, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EMLINK", info.GetReason())
	})

	t.Run("权限不足映射为 EPERM", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Link(mock.Anything, meta.LinkParams{
				Ino:          1001,
				NewParentIno: 2,
				NewName:      []byte("noperm.txt"),
			}).
			Return(nil, meta.ErrPerm).
			Once()

		req := connect.NewRequest(&storagev1.LinkRequest{
			Ino:          1001,
			NewParentIno: 2,
			NewName:      []byte("noperm.txt"),
		})
		resp, err := client.Link(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodePermissionDenied, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EPERM", info.GetReason())
	})
}

func TestHandler_Unlink(t *testing.T) {
	ctx := context.Background()

	t.Run("成功删除非目录项", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Unlink(mock.Anything, uint64(1), []byte("file.txt")).
			Return(nil).
			Once()

		req := connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: 1,
			Name:      []byte("file.txt"),
		})
		resp, err := client.Unlink(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("目标为目录错误映射为 EISDIR", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Unlink(mock.Anything, uint64(1), []byte("subdir")).
			Return(meta.ErrIsDir).
			Once()

		req := connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: 1,
			Name:      []byte("subdir"),
		})
		resp, err := client.Unlink(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeFailedPrecondition, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EISDIR", info.GetReason())
	})

	t.Run("目标不存在错误映射为 ENOENT", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Unlink(mock.Anything, uint64(1), []byte("not_found.txt")).
			Return(meta.ErrNotFound).
			Once()

		req := connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: 1,
			Name:      []byte("not_found.txt"),
		})
		resp, err := client.Unlink(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeNotFound, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOENT", info.GetReason())
	})
}

func TestHandler_Rmdir(t *testing.T) {
	ctx := context.Background()

	t.Run("成功删除空目录", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Rmdir(mock.Anything, uint64(1), []byte("emptydir")).
			Return(nil).
			Once()

		req := connect.NewRequest(&storagev1.RmdirRequest{
			ParentIno: 1,
			Name:      []byte("emptydir"),
		})
		resp, err := client.Rmdir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("目标目录非空映射为 ENOTEMPTY", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Rmdir(mock.Anything, uint64(1), []byte("nonempty")).
			Return(meta.ErrNotEmpty).
			Once()

		req := connect.NewRequest(&storagev1.RmdirRequest{
			ParentIno: 1,
			Name:      []byte("nonempty"),
		})
		resp, err := client.Rmdir(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeFailedPrecondition, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOTEMPTY", info.GetReason())
	})

	t.Run("目标非目录映射为 ENOTDIR", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Rmdir(mock.Anything, uint64(1), []byte("regular.txt")).
			Return(meta.ErrNotDir).
			Once()

		req := connect.NewRequest(&storagev1.RmdirRequest{
			ParentIno: 1,
			Name:      []byte("regular.txt"),
		})
		resp, err := client.Rmdir(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeFailedPrecondition, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOTDIR", info.GetReason())
	})
}

func TestHandler_Rename(t *testing.T) {
	ctx := context.Background()

	t.Run("成功执行原子重命名并透传参数", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Rename(mock.Anything, meta.RenameParams{
				OldParentIno: 1,
				OldName:      []byte("old.txt"),
				NewParentIno: 2,
				NewName:      []byte("new.txt"),
				Flags:        1,
			}).
			Return(nil).
			Once()

		req := connect.NewRequest(&storagev1.RenameRequest{
			OldParentIno: 1,
			OldName:      []byte("old.txt"),
			NewParentIno: 2,
			NewName:      []byte("new.txt"),
			Flags:        1,
		})
		resp, err := client.Rename(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("跨设备移动映射为 EXDEV", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Rename(mock.Anything, meta.RenameParams{
				OldParentIno: 1,
				OldName:      []byte("src.txt"),
				NewParentIno: 2,
				NewName:      []byte("dst.txt"),
				Flags:        0,
			}).
			Return(meta.ErrCrossDevice).
			Once()

		req := connect.NewRequest(&storagev1.RenameRequest{
			OldParentIno: 1,
			OldName:      []byte("src.txt"),
			NewParentIno: 2,
			NewName:      []byte("dst.txt"),
			Flags:        0,
		})
		resp, err := client.Rename(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeFailedPrecondition, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EXDEV", info.GetReason())
	})

	t.Run("目标已存在映射为 EEXIST", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Rename(mock.Anything, meta.RenameParams{
				OldParentIno: 1,
				OldName:      []byte("src.txt"),
				NewParentIno: 1,
				NewName:      []byte("exists.txt"),
				Flags:        1,
			}).
			Return(meta.ErrAlreadyExists).
			Once()

		req := connect.NewRequest(&storagev1.RenameRequest{
			OldParentIno: 1,
			OldName:      []byte("src.txt"),
			NewParentIno: 1,
			NewName:      []byte("exists.txt"),
			Flags:        1,
		})
		resp, err := client.Rename(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeAlreadyExists, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EEXIST", info.GetReason())
	})

	t.Run("覆盖非空目录映射为 ENOTEMPTY", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			Rename(mock.Anything, meta.RenameParams{
				OldParentIno: 1,
				OldName:      []byte("src_dir"),
				NewParentIno: 1,
				NewName:      []byte("dst_dir"),
				Flags:        0,
			}).
			Return(meta.ErrNotEmpty).
			Once()

		req := connect.NewRequest(&storagev1.RenameRequest{
			OldParentIno: 1,
			OldName:      []byte("src_dir"),
			NewParentIno: 1,
			NewName:      []byte("dst_dir"),
			Flags:        0,
		})
		resp, err := client.Rename(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeFailedPrecondition, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOTEMPTY", info.GetReason())
	})
}

func TestHandler_SetXattr(t *testing.T) {
	ctx := context.Background()

	t.Run("成功设置扩展属性", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			SetXattr(mock.Anything, meta.SetXattrParams{
				Ino:   10,
				Name:  []byte("user.test"),
				Value: []byte("value-123"),
				Flags: 0,
			}).
			Return(nil).
			Once()

		req := connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   10,
			Name:  []byte("user.test"),
			Value: []byte("value-123"),
			Flags: 0,
		})
		resp, err := client.SetXattr(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("元数据空间不足映射为 ENOSPC", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			SetXattr(mock.Anything, meta.SetXattrParams{
				Ino:   10,
				Name:  []byte("user.overflow"),
				Value: []byte("large-val"),
				Flags: 0,
			}).
			Return(meta.ErrNoSpace).
			Once()

		req := connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   10,
			Name:  []byte("user.overflow"),
			Value: []byte("large-val"),
			Flags: 0,
		})
		resp, err := client.SetXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeResourceExhausted, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOSPC", info.GetReason())
	})

	t.Run("属性不存在替换失败映射为 ENODATA", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			SetXattr(mock.Anything, meta.SetXattrParams{
				Ino:   10,
				Name:  []byte("user.replace"),
				Value: []byte("val"),
				Flags: 2, // XATTR_REPLACE
			}).
			Return(meta.ErrNoData).
			Once()

		req := connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   10,
			Name:  []byte("user.replace"),
			Value: []byte("val"),
			Flags: 2,
		})
		resp, err := client.SetXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeNotFound, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENODATA", info.GetReason())
	})

	t.Run("Flags 3 且属性已存在映射为 EEXIST", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			SetXattr(mock.Anything, meta.SetXattrParams{
				Ino:   10,
				Name:  []byte("user.both_flags"),
				Value: []byte("val"),
				Flags: 3, // XATTR_CREATE | XATTR_REPLACE
			}).
			Return(meta.ErrAlreadyExists).
			Once()

		req := connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   10,
			Name:  []byte("user.both_flags"),
			Value: []byte("val"),
			Flags: 3,
		})
		resp, err := client.SetXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeAlreadyExists, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EEXIST", info.GetReason())
	})

	t.Run("Flags 3 且属性不存在映射为 ENODATA", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			SetXattr(mock.Anything, meta.SetXattrParams{
				Ino:   10,
				Name:  []byte("user.both_flags"),
				Value: []byte("val"),
				Flags: 3, // XATTR_CREATE | XATTR_REPLACE
			}).
			Return(meta.ErrNoData).
			Once()

		req := connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   10,
			Name:  []byte("user.both_flags"),
			Value: []byte("val"),
			Flags: 3,
		})
		resp, err := client.SetXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeNotFound, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENODATA", info.GetReason())
	})
}

func TestHandler_GetXattr(t *testing.T) {
	ctx := context.Background()

	t.Run("成功获取扩展属性值", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			GetXattr(mock.Anything, uint64(10), []byte("user.comment")).
			Return([]byte("hello world"), nil).
			Once()

		req := connect.NewRequest(&storagev1.GetXattrRequest{
			Ino:  10,
			Name: []byte("user.comment"),
		})
		resp, err := client.GetXattr(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, []byte("hello world"), resp.Msg.GetValue())
	})

	t.Run("属性未定义映射为 ENODATA", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			GetXattr(mock.Anything, uint64(10), []byte("user.missing")).
			Return(nil, meta.ErrNoData).
			Once()

		req := connect.NewRequest(&storagev1.GetXattrRequest{
			Ino:  10,
			Name: []byte("user.missing"),
		})
		resp, err := client.GetXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeNotFound, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENODATA", info.GetReason())
	})

	t.Run("权限不足映射为 EPERM", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			GetXattr(mock.Anything, uint64(10), []byte("trusted.secret")).
			Return(nil, meta.ErrPerm).
			Once()

		req := connect.NewRequest(&storagev1.GetXattrRequest{
			Ino:  10,
			Name: []byte("trusted.secret"),
		})
		resp, err := client.GetXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodePermissionDenied, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EPERM", info.GetReason())
	})
}

func TestHandler_ListXattr(t *testing.T) {
	ctx := context.Background()

	t.Run("成功列出扩展属性名称列表", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		expectedNames := [][]byte{
			[]byte("user.a"),
			[]byte("user.b"),
		}
		mockMgr.EXPECT().
			ListXattr(mock.Anything, uint64(10)).
			Return(expectedNames, nil).
			Once()

		req := connect.NewRequest(&storagev1.ListXattrRequest{
			Ino: 10,
		})
		resp, err := client.ListXattr(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, expectedNames, resp.Msg.GetNames())
	})

	t.Run("权限不足映射为 EPERM", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			ListXattr(mock.Anything, uint64(10)).
			Return(nil, meta.ErrPerm).
			Once()

		req := connect.NewRequest(&storagev1.ListXattrRequest{
			Ino: 10,
		})
		resp, err := client.ListXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodePermissionDenied, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EPERM", info.GetReason())
	})

	t.Run("节点不存在映射为 ENOENT", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			ListXattr(mock.Anything, uint64(999)).
			Return(nil, meta.ErrNotFound).
			Once()

		req := connect.NewRequest(&storagev1.ListXattrRequest{
			Ino: 999,
		})
		resp, err := client.ListXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeNotFound, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOENT", info.GetReason())
	})
}

func TestHandler_RemoveXattr(t *testing.T) {
	ctx := context.Background()

	t.Run("成功移除扩展属性", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			RemoveXattr(mock.Anything, uint64(10), []byte("user.test")).
			Return(nil).
			Once()

		req := connect.NewRequest(&storagev1.RemoveXattrRequest{
			Ino:  10,
			Name: []byte("user.test"),
		})
		resp, err := client.RemoveXattr(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("属性不存在映射为 ENODATA", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			RemoveXattr(mock.Anything, uint64(10), []byte("user.nonexist")).
			Return(meta.ErrNoData).
			Once()

		req := connect.NewRequest(&storagev1.RemoveXattrRequest{
			Ino:  10,
			Name: []byte("user.nonexist"),
		})
		resp, err := client.RemoveXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeNotFound, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENODATA", info.GetReason())
	})

	t.Run("权限不足映射为 EPERM", func(t *testing.T) {
		mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().
			RemoveXattr(mock.Anything, uint64(10), []byte("system.posix_acl")).
			Return(meta.ErrPerm).
			Once()

		req := connect.NewRequest(&storagev1.RemoveXattrRequest{
			Ino:  10,
			Name: []byte("system.posix_acl"),
		})
		resp, err := client.RemoveXattr(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)

		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodePermissionDenied, connErr.Code())

		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EPERM", info.GetReason())
	})
}
