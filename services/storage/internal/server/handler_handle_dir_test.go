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
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

func setupHandleDirTestServer(t *testing.T) (*metamock.MockManager, storagev1connect.StorageServiceClient, func()) {
	t.Helper()
	mockMgr := metamock.NewMockManager(t)
	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)

	cleanup := func() {
		ts.Close()
		mockMgr.AssertExpectations(t)
	}
	return mockMgr, client, cleanup
}

func sampleTestInode(ino uint64, mode uint32) *meta.Inode {
	now := time.Now().Truncate(time.Microsecond)
	return &meta.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: meta.Attribute{
			Mode:      mode,
			Size:      1024,
			Nlink:     1,
			UID:       1000,
			GID:       1000,
			Rdev:      0,
			Blksize:   meta.DefaultBlockSize,
			Blocks:    2,
			Atime:     now,
			Mtime:     now,
			Ctime:     now,
			Flags:     0,
			ParentIno: meta.RootIno,
		},
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}
}

func TestServer_Create(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含可选SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(9999)
		expectedInode := sampleTestInode(101, meta.S_IFREG|0o644)
		expectedResult := &meta.CreateResult{
			Inode:               expectedInode,
			Fh:                  42,
			OpenFlags:           meta.FOPEN_KEEP_CACHE,
			EntryTimeoutSeconds: 1.5,
			AttrTimeoutSeconds:  2.5,
		}

		mockMgr.EXPECT().Create(mock.Anything, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("hello.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR | meta.O_CREAT,
			SessionID: &sessionID,
			Umask:     0,
		}).Return(expectedResult, nil).Once()

		req := connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: 1,
			Name:      []byte("hello.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Uid:       1000,
			Gid:       1000,
			Flags:     meta.O_RDWR | meta.O_CREAT,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.Create(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(42), resp.Msg.Fh)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, resp.Msg.OpenFlags)
		assert.Equal(t, 1.5, resp.Msg.EntryTimeoutSeconds)
		assert.Equal(t, 2.5, resp.Msg.AttrTimeoutSeconds)
		require.NotNil(t, resp.Msg.Inode)
		assert.Equal(t, uint64(101), resp.Msg.Inode.Ino)
		assert.Equal(t, meta.S_IFREG|0o644, resp.Msg.Inode.Attr.Mode)
		assert.Equal(t, expectedInode.Attr.Atime.Unix(), resp.Msg.Inode.Attr.Atime.Seconds)
	})

	t.Run("成功路径未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		expectedInode := sampleTestInode(102, meta.S_IFREG|0o600)
		expectedResult := &meta.CreateResult{
			Inode:               expectedInode,
			Fh:                  55,
			OpenFlags:           0,
			EntryTimeoutSeconds: 1.0,
			AttrTimeoutSeconds:  1.0,
		}

		mockMgr.EXPECT().Create(mock.Anything, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("empty_session.txt"),
			Mode:      meta.S_IFREG | 0o600,
			UID:       0,
			GID:       0,
			Flags:     meta.O_WRONLY | meta.O_CREAT,
			SessionID: nil,
			Umask:     0,
		}).Return(expectedResult, nil).Once()

		req := connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: 1,
			Name:      []byte("empty_session.txt"),
			Mode:      meta.S_IFREG | 0o600,
			Uid:       0,
			Gid:       0,
			Flags:     meta.O_WRONLY | meta.O_CREAT,
			SessionId: nil,
		})

		resp, err := client.Create(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(55), resp.Msg.Fh)
		assert.Equal(t, uint64(102), resp.Msg.Inode.Ino)
	})

	t.Run("领域错误映射EEXIST", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Create(mock.Anything, mock.Anything).Return(nil, meta.ErrAlreadyExists).Once()

		req := connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: 1,
			Name:      []byte("exists.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Uid:       1000,
			Gid:       1000,
			Flags:     meta.O_RDWR | meta.O_CREAT,
		})

		resp, err := client.Create(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EEXIST", info.GetReason())
	})
}

func TestServer_TmpFile(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含可选SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(8888)
		expectedInode := sampleTestInode(201, meta.S_IFREG|0o600)
		expectedResult := &meta.CreateResult{
			Inode:               expectedInode,
			Fh:                  101,
			OpenFlags:           meta.FOPEN_DIRECT_IO,
			EntryTimeoutSeconds: 0,
			AttrTimeoutSeconds:  0,
		}

		mockMgr.EXPECT().TmpFile(mock.Anything, meta.TmpFileParams{
			ParentIno: 1,
			Mode:      meta.S_IFREG | 0o600,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &sessionID,
			Umask:     0,
		}).Return(expectedResult, nil).Once()

		req := connect.NewRequest(&storagev1.TmpFileRequest{
			ParentIno: 1,
			Mode:      meta.S_IFREG | 0o600,
			Uid:       1000,
			Gid:       1000,
			Flags:     meta.O_RDWR,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.TmpFile(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(101), resp.Msg.Fh)
		assert.Equal(t, uint64(201), resp.Msg.Inode.Ino)
		assert.Equal(t, meta.FOPEN_DIRECT_IO, resp.Msg.OpenFlags)
	})

	t.Run("成功路径未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		expectedInode := sampleTestInode(202, meta.S_IFREG|0o644)
		expectedResult := &meta.CreateResult{
			Inode:               expectedInode,
			Fh:                  102,
			OpenFlags:           0,
			EntryTimeoutSeconds: 0,
			AttrTimeoutSeconds:  0,
		}

		mockMgr.EXPECT().TmpFile(mock.Anything, meta.TmpFileParams{
			ParentIno: 1,
			Mode:      meta.S_IFREG | 0o644,
			UID:       0,
			GID:       0,
			Flags:     meta.O_WRONLY,
			SessionID: nil,
			Umask:     0,
		}).Return(expectedResult, nil).Once()

		req := connect.NewRequest(&storagev1.TmpFileRequest{
			ParentIno: 1,
			Mode:      meta.S_IFREG | 0o644,
			Uid:       0,
			Gid:       0,
			Flags:     meta.O_WRONLY,
			SessionId: nil,
		})

		resp, err := client.TmpFile(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(102), resp.Msg.Fh)
	})

	t.Run("领域错误映射ENOENT", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().TmpFile(mock.Anything, mock.Anything).Return(nil, meta.ErrNotFound).Once()

		req := connect.NewRequest(&storagev1.TmpFileRequest{
			ParentIno: 999,
			Mode:      meta.S_IFREG | 0o644,
			Uid:       1000,
			Gid:       1000,
			Flags:     meta.O_RDWR,
		})

		resp, err := client.TmpFile(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOENT", info.GetReason())
	})
}

func TestServer_Open(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含可选SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(7777)
		mockMgr.EXPECT().Open(mock.Anything, meta.OpenParams{
			Ino:       100,
			Flags:     meta.O_RDWR,
			SessionID: &sessionID,
		}).Return(&meta.OpenResult{
			Fh:        201,
			OpenFlags: meta.FOPEN_KEEP_CACHE,
		}, nil).Once()

		req := connect.NewRequest(&storagev1.OpenRequest{
			Ino:       100,
			Flags:     meta.O_RDWR,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.Open(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(201), resp.Msg.Fh)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, resp.Msg.OpenFlags)
	})

	t.Run("成功路径未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Open(mock.Anything, meta.OpenParams{
			Ino:       100,
			Flags:     meta.O_RDONLY,
			SessionID: nil,
		}).Return(&meta.OpenResult{
			Fh:        202,
			OpenFlags: 0,
		}, nil).Once()

		req := connect.NewRequest(&storagev1.OpenRequest{
			Ino:       100,
			Flags:     meta.O_RDONLY,
			SessionId: nil,
		})

		resp, err := client.Open(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(202), resp.Msg.Fh)
	})

	t.Run("领域错误映射EISDIR", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Open(mock.Anything, mock.Anything).Return(nil, meta.ErrIsDir).Once()

		req := connect.NewRequest(&storagev1.OpenRequest{
			Ino:   1,
			Flags: meta.O_RDONLY,
		})

		resp, err := client.Open(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EISDIR", info.GetReason())
	})
}

func TestServer_Flush(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含LockOwner与SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		lockOwner := uint64(12345)
		sessionID := uint64(6666)

		mockMgr.EXPECT().Flush(mock.Anything, meta.FlushParams{
			Ino:       100,
			Fh:        201,
			LockOwner: &lockOwner,
			SessionID: &sessionID,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.FlushRequest{
			Ino:       100,
			Fh:        201,
			LockOwner: proto.Uint64(lockOwner),
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.Flush(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("成功路径LockOwner与SessionID均为nil", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Flush(mock.Anything, meta.FlushParams{
			Ino:       100,
			Fh:        201,
			LockOwner: nil,
			SessionID: nil,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.FlushRequest{
			Ino:       100,
			Fh:        201,
			LockOwner: nil,
			SessionId: nil,
		})

		resp, err := client.Flush(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("领域错误映射EBADF", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Flush(mock.Anything, mock.Anything).Return(meta.ErrBadFileHandle).Once()

		req := connect.NewRequest(&storagev1.FlushRequest{
			Ino: 100,
			Fh:  999,
		})

		resp, err := client.Flush(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EBADF", info.GetReason())
	})
}

func TestServer_Fsync(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含Datasync与SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(5555)

		mockMgr.EXPECT().Fsync(mock.Anything, meta.FsyncParams{
			Ino:       100,
			Fh:        201,
			Datasync:  true,
			SessionID: &sessionID,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.FsyncRequest{
			Ino:       100,
			Fh:        201,
			Datasync:  true,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.Fsync(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("成功路径Datasync为false且未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Fsync(mock.Anything, meta.FsyncParams{
			Ino:       100,
			Fh:        201,
			Datasync:  false,
			SessionID: nil,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.FsyncRequest{
			Ino:       100,
			Fh:        201,
			Datasync:  false,
			SessionId: nil,
		})

		resp, err := client.Fsync(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("领域错误映射EBADF", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Fsync(mock.Anything, mock.Anything).Return(meta.ErrBadFileHandle).Once()

		req := connect.NewRequest(&storagev1.FsyncRequest{
			Ino:      100,
			Fh:       999,
			Datasync: false,
		})

		resp, err := client.Fsync(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EBADF", info.GetReason())
	})
}

func TestServer_Release(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含LockOwner与SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		lockOwner := uint64(9876)
		sessionID := uint64(4444)

		mockMgr.EXPECT().Release(mock.Anything, meta.ReleaseParams{
			Ino:          100,
			Fh:           201,
			LockOwner:    &lockOwner,
			ReleaseFlags: meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock,
			SessionID:    &sessionID,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.ReleaseRequest{
			Ino:          100,
			Fh:           201,
			LockOwner:    proto.Uint64(lockOwner),
			ReleaseFlags: meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock,
			SessionId:    proto.Uint64(sessionID),
		})

		resp, err := client.Release(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("成功路径未传LockOwner与SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Release(mock.Anything, meta.ReleaseParams{
			Ino:          100,
			Fh:           201,
			LockOwner:    nil,
			ReleaseFlags: 0,
			SessionID:    nil,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.ReleaseRequest{
			Ino:          100,
			Fh:           201,
			LockOwner:    nil,
			ReleaseFlags: 0,
			SessionId:    nil,
		})

		resp, err := client.Release(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("领域错误映射EBADF", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Release(mock.Anything, mock.Anything).Return(meta.ErrBadFileHandle).Once()

		req := connect.NewRequest(&storagev1.ReleaseRequest{
			Ino:          100,
			Fh:           999,
			ReleaseFlags: 0,
		})

		resp, err := client.Release(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EBADF", info.GetReason())
	})
}

func TestServer_OpenDir(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含可选SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(3333)

		mockMgr.EXPECT().OpenDir(mock.Anything, meta.OpenDirParams{
			Ino:       1,
			Flags:     meta.O_RDONLY,
			SessionID: &sessionID,
		}).Return(&meta.OpenDirResult{
			Fh:        301,
			OpenFlags: meta.FOPEN_CACHE_DIR,
		}, nil).Once()

		req := connect.NewRequest(&storagev1.OpenDirRequest{
			Ino:       1,
			Flags:     meta.O_RDONLY,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.OpenDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(301), resp.Msg.Fh)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, resp.Msg.OpenFlags)
	})

	t.Run("成功路径未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().OpenDir(mock.Anything, meta.OpenDirParams{
			Ino:       1,
			Flags:     meta.O_RDONLY,
			SessionID: nil,
		}).Return(&meta.OpenDirResult{
			Fh:        302,
			OpenFlags: 0,
		}, nil).Once()

		req := connect.NewRequest(&storagev1.OpenDirRequest{
			Ino:       1,
			Flags:     meta.O_RDONLY,
			SessionId: nil,
		})

		resp, err := client.OpenDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(302), resp.Msg.Fh)
	})

	t.Run("领域错误映射ENOTDIR", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().OpenDir(mock.Anything, mock.Anything).Return(nil, meta.ErrNotDir).Once()

		req := connect.NewRequest(&storagev1.OpenDirRequest{
			Ino:   100,
			Flags: meta.O_RDONLY,
		})

		resp, err := client.OpenDir(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOTDIR", info.GetReason())
	})
}

func TestServer_FsyncDir(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含Datasync与SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(2222)

		mockMgr.EXPECT().FsyncDir(mock.Anything, meta.FsyncDirParams{
			Ino:       1,
			Fh:        301,
			Datasync:  true,
			SessionID: &sessionID,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.FsyncDirRequest{
			Ino:       1,
			Fh:        301,
			Datasync:  true,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.FsyncDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("成功路径Datasync为false且未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().FsyncDir(mock.Anything, meta.FsyncDirParams{
			Ino:       1,
			Fh:        301,
			Datasync:  false,
			SessionID: nil,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.FsyncDirRequest{
			Ino:       1,
			Fh:        301,
			Datasync:  false,
			SessionId: nil,
		})

		resp, err := client.FsyncDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("领域错误映射EBADF", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().FsyncDir(mock.Anything, mock.Anything).Return(meta.ErrBadFileHandle).Once()

		req := connect.NewRequest(&storagev1.FsyncDirRequest{
			Ino:      1,
			Fh:       999,
			Datasync: false,
		})

		resp, err := client.FsyncDir(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EBADF", info.GetReason())
	})
}

func TestServer_ReleaseDir(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含可选SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(1111)

		mockMgr.EXPECT().ReleaseDir(mock.Anything, meta.ReleaseDirParams{
			Ino:       1,
			Fh:        301,
			SessionID: &sessionID,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.ReleaseDirRequest{
			Ino:       1,
			Fh:        301,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.ReleaseDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("成功路径未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().ReleaseDir(mock.Anything, meta.ReleaseDirParams{
			Ino:       1,
			Fh:        301,
			SessionID: nil,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.ReleaseDirRequest{
			Ino:       1,
			Fh:        301,
			SessionId: nil,
		})

		resp, err := client.ReleaseDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("领域错误映射EBADF", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().ReleaseDir(mock.Anything, mock.Anything).Return(meta.ErrBadFileHandle).Once()

		req := connect.NewRequest(&storagev1.ReleaseDirRequest{
			Ino: 1,
			Fh:  999,
		})

		resp, err := client.ReleaseDir(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EBADF", info.GetReason())
	})
}

func TestServer_ReadDir(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含条目与SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(5001)
		expectedEntries := []meta.DirEntry{
			{Ino: 1, Name: []byte("."), Mode: meta.S_IFDIR | 0o755, NextOffset: 1},
			{Ino: 1, Name: []byte(".."), Mode: meta.S_IFDIR | 0o755, NextOffset: 2},
			{Ino: 100, Name: []byte("file.txt"), Mode: meta.S_IFREG | 0o644, NextOffset: 3},
		}

		mockMgr.EXPECT().ReadDir(mock.Anything, meta.ReadDirParams{
			Ino:       1,
			Fh:        301,
			Offset:    0,
			Limit:     100,
			SessionID: &sessionID,
		}).Return(expectedEntries, nil).Once()

		req := connect.NewRequest(&storagev1.ReadDirRequest{
			Ino:       1,
			Fh:        301,
			Offset:    0,
			Limit:     100,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.ReadDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Msg.Entries, 3)

		assert.Equal(t, uint64(1), resp.Msg.Entries[0].Ino)
		assert.Equal(t, []byte("."), resp.Msg.Entries[0].Name)
		assert.Equal(t, meta.S_IFDIR|0o755, resp.Msg.Entries[0].Mode)
		assert.Equal(t, uint64(1), resp.Msg.Entries[0].NextOffset)

		assert.Equal(t, uint64(100), resp.Msg.Entries[2].Ino)
		assert.Equal(t, []byte("file.txt"), resp.Msg.Entries[2].Name)
		assert.Equal(t, meta.S_IFREG|0o644, resp.Msg.Entries[2].Mode)
		assert.Equal(t, uint64(3), resp.Msg.Entries[2].NextOffset)
	})

	t.Run("成功路径空切片与未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().ReadDir(mock.Anything, meta.ReadDirParams{
			Ino:       1,
			Fh:        301,
			Offset:    3,
			Limit:     50,
			SessionID: nil,
		}).Return([]meta.DirEntry{}, nil).Once()

		req := connect.NewRequest(&storagev1.ReadDirRequest{
			Ino:       1,
			Fh:        301,
			Offset:    3,
			Limit:     50,
			SessionId: nil,
		})

		resp, err := client.ReadDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Msg.Entries)
	})

	t.Run("领域错误映射EBADF", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().ReadDir(mock.Anything, mock.Anything).Return(nil, meta.ErrBadFileHandle).Once()

		req := connect.NewRequest(&storagev1.ReadDirRequest{
			Ino:    1,
			Fh:     999,
			Offset: 0,
			Limit:  10,
		})

		resp, err := client.ReadDir(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EBADF", info.GetReason())
	})
}

func TestServer_ReadDirPlus(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含扩展条目与SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(6001)
		fileInode := sampleTestInode(105, meta.S_IFREG|0o644)
		expectedEntries := []meta.DirEntryPlus{
			{
				Ino:                 105,
				Name:                []byte("sample.dat"),
				NextOffset:          1,
				Inode:               fileInode,
				EntryTimeoutSeconds: 2.0,
				AttrTimeoutSeconds:  3.0,
			},
			{
				Ino:                 106,
				Name:                []byte("orphan.dat"),
				NextOffset:          2,
				Inode:               nil,
				EntryTimeoutSeconds: 0,
				AttrTimeoutSeconds:  0,
			},
		}

		mockMgr.EXPECT().ReadDirPlus(mock.Anything, meta.ReadDirPlusParams{
			Ino:       1,
			Fh:        301,
			Offset:    0,
			Limit:     200,
			SessionID: &sessionID,
		}).Return(expectedEntries, nil).Once()

		req := connect.NewRequest(&storagev1.ReadDirPlusRequest{
			Ino:       1,
			Fh:        301,
			Offset:    0,
			Limit:     200,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.ReadDirPlus(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Msg.Entries, 2)

		entry := resp.Msg.Entries[0]
		assert.Equal(t, uint64(105), entry.Ino)
		assert.Equal(t, []byte("sample.dat"), entry.Name)
		assert.Equal(t, uint64(1), entry.NextOffset)
		assert.Equal(t, 2.0, entry.EntryTimeoutSeconds)
		assert.Equal(t, 3.0, entry.AttrTimeoutSeconds)
		require.NotNil(t, entry.Inode)
		assert.Equal(t, uint64(105), entry.Inode.Ino)
		assert.Equal(t, meta.S_IFREG|0o644, entry.Inode.Attr.Mode)
		assert.Equal(t, fileInode.Attr.Mtime.Unix(), entry.Inode.Attr.Mtime.Seconds)

		entry2 := resp.Msg.Entries[1]
		assert.Equal(t, uint64(106), entry2.Ino)
		assert.Equal(t, []byte("orphan.dat"), entry2.Name)
		assert.Equal(t, uint64(2), entry2.NextOffset)
		assert.Nil(t, entry2.Inode)
	})

	t.Run("成功路径空切片与未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().ReadDirPlus(mock.Anything, meta.ReadDirPlusParams{
			Ino:       1,
			Fh:        301,
			Offset:    1,
			Limit:     100,
			SessionID: nil,
		}).Return([]meta.DirEntryPlus{}, nil).Once()

		req := connect.NewRequest(&storagev1.ReadDirPlusRequest{
			Ino:       1,
			Fh:        301,
			Offset:    1,
			Limit:     100,
			SessionId: nil,
		})

		resp, err := client.ReadDirPlus(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Msg.Entries)
	})

	t.Run("领域错误映射EBADF", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().ReadDirPlus(mock.Anything, mock.Anything).Return(nil, meta.ErrBadFileHandle).Once()

		req := connect.NewRequest(&storagev1.ReadDirPlusRequest{
			Ino:    1,
			Fh:     999,
			Offset: 0,
			Limit:  10,
		})

		resp, err := client.ReadDirPlus(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EBADF", info.GetReason())
	})
}

func TestServer_Fallocate(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径包含可选SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		sessionID := uint64(9001)

		mockMgr.EXPECT().Fallocate(mock.Anything, meta.FallocateParams{
			Ino:       100,
			Fh:        201,
			Offset:    1024,
			Length:    4096,
			Mode:      meta.FallocFlKeepSize,
			SessionID: &sessionID,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.FallocateRequest{
			Ino:       100,
			Fh:        201,
			Offset:    1024,
			Length:    4096,
			Mode:      meta.FallocFlKeepSize,
			SessionId: proto.Uint64(sessionID),
		})

		resp, err := client.Fallocate(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("成功路径未传SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Fallocate(mock.Anything, meta.FallocateParams{
			Ino:       100,
			Fh:        201,
			Offset:    0,
			Length:    8192,
			Mode:      meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			SessionID: nil,
		}).Return(nil).Once()

		req := connect.NewRequest(&storagev1.FallocateRequest{
			Ino:       100,
			Fh:        201,
			Offset:    0,
			Length:    8192,
			Mode:      meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			SessionId: nil,
		})

		resp, err := client.Fallocate(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("领域错误映射ENOSPC", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Fallocate(mock.Anything, mock.Anything).Return(meta.ErrNoSpace).Once()

		req := connect.NewRequest(&storagev1.FallocateRequest{
			Ino:    100,
			Fh:     201,
			Offset: 0,
			Length: 1024,
			Mode:   0,
		})

		resp, err := client.Fallocate(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "ENOSPC", info.GetReason())
	})

	t.Run("领域错误映射EOPNOTSUPP", func(t *testing.T) {
		mockMgr, client, cleanup := setupHandleDirTestServer(t)
		defer cleanup()

		mockMgr.EXPECT().Fallocate(mock.Anything, mock.Anything).Return(meta.ErrNotSupported).Once()

		req := connect.NewRequest(&storagev1.FallocateRequest{
			Ino:    100,
			Fh:     201,
			Offset: 0,
			Length: 1024,
			Mode:   meta.FallocFlCollapseRange,
		})

		resp, err := client.Fallocate(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EOPNOTSUPP", info.GetReason())
	})
}
