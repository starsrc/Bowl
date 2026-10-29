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
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

// setupDiskNodeTestClient 创建带有 MockManager 与测试服务器的 StorageServiceClient。
func setupDiskNodeTestClient(t *testing.T) (storagev1connect.StorageServiceClient, *metamock.MockManager) {
	mockMgr := metamock.NewMockManager(t)
	srv, err := server.New(server.Params{
		Config: config.ServerConfig{
			ServerHost:              "127.0.0.1",
			ServerPort:              0,
			ServerReadHeaderTimeout: 5 * time.Second,
			ServerStopTimeout:       5 * time.Second,
			ServerReadMaxBytes:      131072,
		},
		MetaManager: mockMgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)
	return client, mockMgr
}

// 示例通用 Inode 构造辅助。
func sampleInode(ino uint64, mode uint32) *meta.Inode {
	atime := time.Date(2026, 10, 26, 10, 1, 2, 123456789, time.UTC)
	mtime := time.Date(2026, 10, 26, 11, 2, 3, 234567890, time.UTC)
	ctime := time.Date(2026, 10, 26, 12, 3, 4, 345678901, time.UTC)
	return &meta.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: meta.Attribute{
			Mode:      mode,
			Size:      1024,
			Nlink:     1,
			UID:       1001,
			GID:       2002,
			Rdev:      259,
			Blksize:   meta.DefaultBlockSize,
			Blocks:    2,
			Atime:     atime,
			Mtime:     mtime,
			Ctime:     ctime,
			Flags:     meta.FlagImmutable,
			ParentIno: 1,
		},
		EntryTimeoutSeconds: 1.5,
		AttrTimeoutSeconds:  2.0,
	}
}

// 示例通用 Disk 构造辅助。
func sampleDisk(id string, name string, rootIno uint64, maxBytes uint64) *meta.Disk {
	now := time.Now().Truncate(time.Microsecond)
	return &meta.Disk{
		DiskID:     id,
		Name:       name,
		RootIno:    rootIno,
		MaxBytes:   maxBytes,
		UsedBytes:  4096,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func TestServer_DiskRPCs(t *testing.T) {
	ctx := context.Background()
	validUUID := "018f6738-4e89-7381-8000-123456789abc"

	t.Run("CreateDisk_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		disk := sampleDisk(validUUID, "disk-alpha", 1, 1073741824)

		mockMgr.EXPECT().
			CreateDisk(mock.Anything, meta.CreateDiskParams{
				Name:     "disk-alpha",
				MaxBytes: 1073741824,
			}).
			Return(disk, nil).
			Once()

		req := connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "disk-alpha",
			MaxBytes: 1073741824,
		})
		res, err := client.CreateDisk(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Disk)

		assert.Equal(t, validUUID, res.Msg.Disk.DiskId)
		assert.Equal(t, "disk-alpha", res.Msg.Disk.Name)
		assert.Equal(t, uint64(1), res.Msg.Disk.RootIno)
		assert.Equal(t, uint64(1073741824), res.Msg.Disk.MaxBytes)
		assert.Equal(t, uint64(4096), res.Msg.Disk.UsedBytes)
		assert.Equal(t, uint64(1), res.Msg.Disk.UsedInodes)
		assert.Equal(t, disk.CreatedAt.Unix(), res.Msg.Disk.CreatedAt.AsTime().Unix())
		assert.Equal(t, disk.UpdatedAt.Unix(), res.Msg.Disk.UpdatedAt.AsTime().Unix())
	})

	t.Run("CreateDisk_DomainError", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			CreateDisk(mock.Anything, mock.Anything).
			Return(nil, meta.ErrAlreadyExists).
			Once()

		req := connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "disk-alpha",
			MaxBytes: 1073741824,
		})
		_, err := client.CreateDisk(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
	})

	t.Run("CreateDisk_ValidationError", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		// max_bytes = 0 违反 gt: 0
		req := connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "disk-alpha",
			MaxBytes: 0,
		})
		_, err := client.CreateDisk(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("GetDisk_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		disk := sampleDisk(validUUID, "disk-alpha", 1, 1073741824)

		mockMgr.EXPECT().
			GetDisk(mock.Anything, validUUID).
			Return(disk, nil).
			Once()

		req := connect.NewRequest(&storagev1.GetDiskRequest{
			DiskId: validUUID,
		})
		res, err := client.GetDisk(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Disk)
		assert.Equal(t, validUUID, res.Msg.Disk.DiskId)
		assert.Equal(t, "disk-alpha", res.Msg.Disk.Name)
	})

	t.Run("GetDisk_NotFound", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			GetDisk(mock.Anything, validUUID).
			Return(nil, meta.ErrNotFound).
			Once()

		req := connect.NewRequest(&storagev1.GetDiskRequest{
			DiskId: validUUID,
		})
		_, err := client.GetDisk(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOENT", info.Reason)
	})

	t.Run("GetDisk_ValidationError_NilUUID", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		req := connect.NewRequest(&storagev1.GetDiskRequest{
			DiskId: "00000000-0000-0000-0000-000000000000",
		})
		_, err := client.GetDisk(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("ListDisks_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		d1 := sampleDisk(validUUID, "disk-1", 101, 1073741824)
		d2 := sampleDisk("018f6738-4e89-7381-8000-123456789abd", "disk-2", 102, 2147483648)

		mockMgr.EXPECT().
			ListDisks(mock.Anything, meta.ListDisksParams{
				PageSize:  2,
				PageToken: "cursor-in",
			}).
			Return(&meta.ListDisksResult{
				Disks:         []*meta.Disk{d1, d2},
				NextPageToken: "cursor-out",
			}, nil).
			Once()

		req := connect.NewRequest(&storagev1.ListDisksRequest{
			PageSize:  2,
			PageToken: "cursor-in",
		})
		res, err := client.ListDisks(ctx, req)
		require.NoError(t, err)
		require.Len(t, res.Msg.Disks, 2)
		assert.Equal(t, validUUID, res.Msg.Disks[0].DiskId)
		assert.Equal(t, "disk-1", res.Msg.Disks[0].Name)
		assert.Equal(t, "018f6738-4e89-7381-8000-123456789abd", res.Msg.Disks[1].DiskId)
		assert.Equal(t, "disk-2", res.Msg.Disks[1].Name)
		assert.Equal(t, "cursor-out", res.Msg.NextPageToken)
	})

	t.Run("ListDisks_InvalidPageToken_DomainError", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			ListDisks(mock.Anything, meta.ListDisksParams{
				PageSize:  10,
				PageToken: "bad-token",
			}).
			Return(nil, meta.ErrInvalidArgument).
			Once()

		req := connect.NewRequest(&storagev1.ListDisksRequest{
			PageSize:  10,
			PageToken: "bad-token",
		})
		_, err := client.ListDisks(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EINVAL", info.Reason)
		assert.Equal(t, "bowl.storage", info.Domain)
	})

	t.Run("ListDisks_ValidationError_PageSizeExceeds1000", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		req := connect.NewRequest(&storagev1.ListDisksRequest{
			PageSize: 1001,
		})
		_, err := client.ListDisks(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EINVAL", info.Reason)
		assert.Equal(t, "bowl.storage", info.Domain)
	})

	t.Run("UpdateDisk_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		disk := sampleDisk(validUUID, "disk-renamed", 1, 2147483648)
		newName := "disk-renamed"
		newMaxBytes := uint64(2147483648)

		mockMgr.EXPECT().
			UpdateDisk(mock.Anything, meta.UpdateDiskParams{
				DiskID:   validUUID,
				Name:     &newName,
				MaxBytes: &newMaxBytes,
			}).
			Return(disk, nil).
			Once()

		req := connect.NewRequest(&storagev1.UpdateDiskRequest{
			DiskId:   validUUID,
			Name:     &newName,
			MaxBytes: &newMaxBytes,
		})
		res, err := client.UpdateDisk(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Disk)
		assert.Equal(t, "disk-renamed", res.Msg.Disk.Name)
		assert.Equal(t, uint64(2147483648), res.Msg.Disk.MaxBytes)
	})

	t.Run("UpdateDisk_ShrinkProtectionError", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		newMaxBytes := uint64(1024)

		mockMgr.EXPECT().
			UpdateDisk(mock.Anything, meta.UpdateDiskParams{
				DiskID:   validUUID,
				MaxBytes: &newMaxBytes,
			}).
			Return(nil, meta.ErrInvalidShrink).
			Once()

		req := connect.NewRequest(&storagev1.UpdateDiskRequest{
			DiskId:   validUUID,
			MaxBytes: &newMaxBytes,
		})
		_, err := client.UpdateDisk(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EINVAL", info.Reason)
	})

	t.Run("UpdateDisk_ValidationError_NoFields", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		// 既未传 name 也未传 max_bytes
		req := connect.NewRequest(&storagev1.UpdateDiskRequest{
			DiskId: validUUID,
		})
		_, err := client.UpdateDisk(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("DeleteDisk_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			DeleteDisk(mock.Anything, validUUID).
			Return(nil).
			Once()

		req := connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: validUUID,
		})
		res, err := client.DeleteDisk(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg)
	})

	t.Run("DeleteDisk_NotFound", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			DeleteDisk(mock.Anything, validUUID).
			Return(meta.ErrNotFound).
			Once()

		req := connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: validUUID,
		})
		_, err := client.DeleteDisk(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("DeleteDisk_Busy", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			DeleteDisk(mock.Anything, validUUID).
			Return(meta.ErrBusy).
			Once()

		req := connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: validUUID,
		})
		_, err := client.DeleteDisk(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EBUSY", info.Reason)
		assert.Equal(t, "bowl.storage", info.Domain)
	})

	t.Run("StatFS_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			StatFS(mock.Anything, uint64(1)).
			Return(&meta.StatFSResult{
				TotalBytes: 10485760,
				UsedBytes:  40960,
				AvailBytes: 10444800,
				Bsize:      4096,
				Frsize:     4096,
				Blocks:     2560,
				Bfree:      2550,
				Bavail:     2550,
				Files:      10,
				Ffree:      0,
				Namelen:    255,
			}, nil).
			Once()

		req := connect.NewRequest(&storagev1.StatFSRequest{
			Ino: 1,
		})
		res, err := client.StatFS(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, uint64(10485760), res.Msg.TotalBytes)
		assert.Equal(t, uint64(40960), res.Msg.UsedBytes)
		assert.Equal(t, uint64(10444800), res.Msg.AvailBytes)
		assert.Equal(t, uint32(4096), res.Msg.Bsize)
		assert.Equal(t, uint32(4096), res.Msg.Frsize)
		assert.Equal(t, uint64(2560), res.Msg.Blocks)
		assert.Equal(t, uint64(2550), res.Msg.Bfree)
		assert.Equal(t, uint64(2550), res.Msg.Bavail)
		assert.Equal(t, uint64(10), res.Msg.Files)
		assert.Equal(t, uint64(0), res.Msg.Ffree)
		assert.Equal(t, uint32(255), res.Msg.Namelen)
	})

	t.Run("StatFS_NotFound", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			StatFS(mock.Anything, uint64(999)).
			Return(nil, meta.ErrNotFound).
			Once()

		req := connect.NewRequest(&storagev1.StatFSRequest{
			Ino: 999,
		})
		_, err := client.StatFS(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("StatFS_ValidationError_InoZero", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		req := connect.NewRequest(&storagev1.StatFSRequest{
			Ino: 0,
		})
		_, err := client.StatFS(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

func TestServer_NodeRPCs(t *testing.T) {
	ctx := context.Background()

	t.Run("Lookup_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		inode := sampleInode(2, meta.S_IFREG|0o644)

		mockMgr.EXPECT().
			Lookup(mock.Anything, uint64(1), []byte("foo.txt")).
			Return(inode, nil).
			Once()

		req := connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("foo.txt"),
		})
		res, err := client.Lookup(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Inode)
		assert.Equal(t, uint64(2), res.Msg.Inode.Ino)
		assert.Equal(t, uint64(1), res.Msg.Inode.Generation)
		assert.Equal(t, meta.S_IFREG|0o644, res.Msg.Inode.Attr.Mode)
		assert.Equal(t, 1.5, res.Msg.EntryTimeoutSeconds)
		assert.Equal(t, 2.0, res.Msg.AttrTimeoutSeconds)
	})

	t.Run("Lookup_NotDir", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			Lookup(mock.Anything, uint64(2), []byte("sub")).
			Return(nil, meta.ErrNotDir).
			Once()

		req := connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 2,
			Name:      []byte("sub"),
		})
		_, err := client.Lookup(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOTDIR", info.Reason)
	})

	t.Run("Lookup_NotFound", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			Lookup(mock.Anything, uint64(1), []byte("missing")).
			Return(nil, meta.ErrNotFound).
			Once()

		req := connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("missing"),
		})
		_, err := client.Lookup(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOENT", info.Reason)
	})

	t.Run("Lookup_ValidationError_SlashInName", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		req := connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("dir/file"),
		})
		_, err := client.Lookup(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("GetAttr_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		inode := sampleInode(2, meta.S_IFREG|0o644)

		mockMgr.EXPECT().
			GetAttr(mock.Anything, uint64(2)).
			Return(inode, nil).
			Once()

		req := connect.NewRequest(&storagev1.GetAttrRequest{
			Ino: 2,
		})
		res, err := client.GetAttr(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Inode)
		assert.Equal(t, uint64(2), res.Msg.Inode.Ino)
		assert.Equal(t, 2.0, res.Msg.AttrTimeoutSeconds)

		// 逐项校验 UID、GID、Rdev、Flags 与纳秒级时间戳精度
		attr := res.Msg.Inode.Attr
		require.NotNil(t, attr)
		assert.Equal(t, uint32(meta.S_IFREG|0o644), attr.Mode)
		assert.Equal(t, uint64(1024), attr.Size)
		assert.Equal(t, uint32(1), attr.Nlink)
		assert.Equal(t, uint32(1001), attr.Uid)
		assert.Equal(t, uint32(2002), attr.Gid)
		assert.Equal(t, uint32(259), attr.Rdev)
		assert.Equal(t, uint32(meta.FlagImmutable), attr.Flags)
		assert.Equal(t, uint32(meta.DefaultBlockSize), attr.Blksize)
		assert.Equal(t, uint64(2), attr.Blocks)
		assert.True(t, attr.Atime.AsTime().Equal(inode.Attr.Atime), "Atime 必须满足纳秒级时间戳相等")
		assert.True(t, attr.Mtime.AsTime().Equal(inode.Attr.Mtime), "Mtime 必须满足纳秒级时间戳相等")
		assert.True(t, attr.Ctime.AsTime().Equal(inode.Attr.Ctime), "Ctime 必须满足纳秒级时间戳相等")
	})

	t.Run("GetAttr_NotFound", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			GetAttr(mock.Anything, uint64(999)).
			Return(nil, meta.ErrNotFound).
			Once()

		req := connect.NewRequest(&storagev1.GetAttrRequest{
			Ino: 999,
		})
		_, err := client.GetAttr(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("SetAttr_Success_AllOptionalFields", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		inode := sampleInode(2, meta.S_IFREG|0o755)

		mode := uint32(0o755)
		uid := uint32(1001)
		gid := uint32(2002)
		size := uint64(2048)
		rdev := uint32(259)
		atime := time.Date(2026, 10, 26, 10, 1, 2, 123456789, time.UTC)
		mtime := time.Date(2026, 10, 26, 11, 2, 3, 234567890, time.UTC)
		atimeProto := timestamppb.New(atime)
		mtimeProto := timestamppb.New(mtime)
		fh := uint64(42)
		sessionID := uint64(100)
		flags := uint32(meta.FlagImmutable)

		mockMgr.EXPECT().
			SetAttr(mock.Anything, uint64(2), mock.MatchedBy(func(in meta.SetAttrInput) bool {
				if in.Mode == nil || *in.Mode != mode {
					return false
				}
				if in.UID == nil || *in.UID != uid {
					return false
				}
				if in.GID == nil || *in.GID != gid {
					return false
				}
				if in.Size == nil || *in.Size != size {
					return false
				}
				if in.Atime == nil || !in.Atime.Equal(atime) {
					return false
				}
				if in.Mtime == nil || !in.Mtime.Equal(mtime) {
					return false
				}
				if in.Fh == nil || *in.Fh != fh {
					return false
				}
				if in.SessionID == nil || *in.SessionID != sessionID {
					return false
				}
				if in.Flags == nil || *in.Flags != flags {
					return false
				}
				return in.CtimeNow == true
			})).
			Return(inode, nil).
			Once()

		req := connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:       2,
			Mode:      &mode,
			Uid:       &uid,
			Gid:       &gid,
			Size:      &size,
			Atime:     atimeProto,
			Mtime:     mtimeProto,
			Fh:        &fh,
			SessionId: &sessionID,
			CtimeNow:  true,
			Flags:     &flags,
		})
		res, err := client.SetAttr(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Inode)
		assert.Equal(t, uint64(2), res.Msg.Inode.Ino)
		assert.Equal(t, 2.0, res.Msg.AttrTimeoutSeconds)

		// 响应断言逐项校验 UID、GID、Rdev、Flags 与纳秒级时间戳相等
		attr := res.Msg.Inode.Attr
		require.NotNil(t, attr)
		assert.Equal(t, uid, attr.Uid)
		assert.Equal(t, gid, attr.Gid)
		assert.Equal(t, rdev, attr.Rdev)
		assert.Equal(t, flags, attr.Flags)
		assert.True(t, attr.Atime.AsTime().Equal(inode.Attr.Atime), "Atime 必须满足纳秒级时间戳相等")
		assert.True(t, attr.Mtime.AsTime().Equal(inode.Attr.Mtime), "Mtime 必须满足纳秒级时间戳相等")
		assert.True(t, attr.Ctime.AsTime().Equal(inode.Attr.Ctime), "Ctime 必须满足纳秒级时间戳相等")
	})

	t.Run("SetAttr_Success_AtimeNowMtimeNow", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		inode := sampleInode(2, meta.S_IFREG|0o644)

		mockMgr.EXPECT().
			SetAttr(mock.Anything, uint64(2), mock.MatchedBy(func(in meta.SetAttrInput) bool {
				return in.AtimeNow == true && in.MtimeNow == true && in.Atime == nil && in.Mtime == nil
			})).
			Return(inode, nil).
			Once()

		req := connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:      2,
			AtimeNow: true,
			MtimeNow: true,
		})
		res, err := client.SetAttr(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Inode)
	})

	t.Run("SetAttr_DomainError_PermissionDenied", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		mode := uint32(0o777)

		mockMgr.EXPECT().
			SetAttr(mock.Anything, uint64(2), mock.Anything).
			Return(nil, meta.ErrPerm).
			Once()

		req := connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:  2,
			Mode: &mode,
		})
		_, err := client.SetAttr(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EPERM", info.Reason)
	})

	t.Run("SetAttr_DomainError_IsDir", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		size := uint64(1024)

		mockMgr.EXPECT().
			SetAttr(mock.Anything, uint64(1), mock.Anything).
			Return(nil, meta.ErrIsDir).
			Once()

		req := connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:  1,
			Size: &size,
		})
		_, err := client.SetAttr(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EISDIR", info.Reason)
	})

	t.Run("SetAttr_ValidationError_ConflictAtime", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)
		atime := timestamppb.Now()

		req := connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:      2,
			Atime:    atime,
			AtimeNow: true,
		})
		_, err := client.SetAttr(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("SetAttr_ValidationError_NoUpdates", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		req := connect.NewRequest(&storagev1.SetAttrRequest{
			Ino: 2,
		})
		_, err := client.SetAttr(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("Mknod_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		inode := sampleInode(3, meta.S_IFREG|0o644)

		mockMgr.EXPECT().
			Mknod(mock.Anything, meta.MknodParams{
				ParentIno: 1,
				Name:      []byte("regular.txt"),
				Mode:      meta.S_IFREG | 0o644,
				UID:       1000,
				GID:       1000,
				Rdev:      0,
			}).
			Return(inode, nil).
			Once()

		req := connect.NewRequest(&storagev1.MknodRequest{
			ParentIno: 1,
			Name:      []byte("regular.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Uid:       1000,
			Gid:       1000,
			Rdev:      0,
		})
		res, err := client.Mknod(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Inode)
		assert.Equal(t, uint64(3), res.Msg.Inode.Ino)
		assert.Equal(t, 1.5, res.Msg.EntryTimeoutSeconds)
		assert.Equal(t, 2.0, res.Msg.AttrTimeoutSeconds)
	})

	t.Run("Mknod_AlreadyExists", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			Mknod(mock.Anything, mock.Anything).
			Return(nil, meta.ErrAlreadyExists).
			Once()

		req := connect.NewRequest(&storagev1.MknodRequest{
			ParentIno: 1,
			Name:      []byte("exists.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Uid:       1000,
			Gid:       1000,
		})
		_, err := client.Mknod(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EEXIST", info.Reason)
	})

	t.Run("Mknod_ValidationError_EmptyName", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		req := connect.NewRequest(&storagev1.MknodRequest{
			ParentIno: 1,
			Name:      []byte(""),
			Mode:      meta.S_IFREG | 0o644,
		})
		_, err := client.Mknod(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("Mkdir_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		inode := sampleInode(4, meta.S_IFDIR|0o755)

		mockMgr.EXPECT().
			Mkdir(mock.Anything, meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("newdir"),
				Mode:      meta.S_IFDIR | 0o755,
				UID:       1000,
				GID:       1000,
			}).
			Return(inode, nil).
			Once()

		req := connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: 1,
			Name:      []byte("newdir"),
			Mode:      meta.S_IFDIR | 0o755,
			Uid:       1000,
			Gid:       1000,
		})
		res, err := client.Mkdir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Inode)
		assert.Equal(t, uint64(4), res.Msg.Inode.Ino)
		assert.Equal(t, 1.5, res.Msg.EntryTimeoutSeconds)
		assert.Equal(t, 2.0, res.Msg.AttrTimeoutSeconds)
	})

	t.Run("Mkdir_NotDir", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			Mkdir(mock.Anything, mock.Anything).
			Return(nil, meta.ErrNotDir).
			Once()

		req := connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: 2,
			Name:      []byte("subdir"),
			Mode:      0o755,
			Uid:       1000,
			Gid:       1000,
		})
		_, err := client.Mkdir(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOTDIR", info.Reason)
	})

	t.Run("Symlink_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)
		inode := sampleInode(5, meta.S_IFLNK|0o777)

		mockMgr.EXPECT().
			Symlink(mock.Anything, meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("link_to_file"),
				Target:    []byte("/path/to/target"),
				UID:       1000,
				GID:       1000,
			}).
			Return(inode, nil).
			Once()

		req := connect.NewRequest(&storagev1.SymlinkRequest{
			ParentIno: 1,
			Name:      []byte("link_to_file"),
			Target:    []byte("/path/to/target"),
			Uid:       1000,
			Gid:       1000,
		})
		res, err := client.Symlink(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, res.Msg.Inode)
		assert.Equal(t, uint64(5), res.Msg.Inode.Ino)
		assert.Equal(t, 1.5, res.Msg.EntryTimeoutSeconds)
		assert.Equal(t, 2.0, res.Msg.AttrTimeoutSeconds)
	})

	t.Run("Symlink_AlreadyExists", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			Symlink(mock.Anything, mock.Anything).
			Return(nil, meta.ErrAlreadyExists).
			Once()

		req := connect.NewRequest(&storagev1.SymlinkRequest{
			ParentIno: 1,
			Name:      []byte("link_to_file"),
			Target:    []byte("/path/to/target"),
			Uid:       1000,
			Gid:       1000,
		})
		_, err := client.Symlink(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
	})

	t.Run("Symlink_ValidationError_NulInTarget", func(t *testing.T) {
		client, _ := setupDiskNodeTestClient(t)

		req := connect.NewRequest(&storagev1.SymlinkRequest{
			ParentIno: 1,
			Name:      []byte("link1"),
			Target:    []byte("bad\x00target"),
			Uid:       1000,
			Gid:       1000,
		})
		_, err := client.Symlink(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("Readlink_Success", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			Readlink(mock.Anything, uint64(5)).
			Return([]byte("/path/to/target"), nil).
			Once()

		req := connect.NewRequest(&storagev1.ReadlinkRequest{
			Ino: 5,
		})
		res, err := client.Readlink(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, []byte("/path/to/target"), res.Msg.Target)
	})

	t.Run("Readlink_NotFound", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			Readlink(mock.Anything, uint64(999)).
			Return(nil, meta.ErrNotFound).
			Once()

		req := connect.NewRequest(&storagev1.ReadlinkRequest{
			Ino: 999,
		})
		_, err := client.Readlink(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("Readlink_NotSymlink_EINVAL", func(t *testing.T) {
		client, mockMgr := setupDiskNodeTestClient(t)

		mockMgr.EXPECT().
			Readlink(mock.Anything, uint64(2)).
			Return(nil, meta.ErrInvalidArgument).
			Once()

		req := connect.NewRequest(&storagev1.ReadlinkRequest{
			Ino: 2,
		})
		_, err := client.Readlink(ctx, req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EINVAL", info.Reason)
	})
}
