package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// setupRenewClientSessionMocks 设置通用的会话续期 Mock 期望序列（包含 DeleteExpiredClientSessions、UpsertClientSession、DeleteUnreferencedOrphanInodes）。
func setupRenewClientSessionMocks(mockQuerier *dbmock.MockQuerier, sessionID uint64) {
	mockQuerier.EXPECT().
		DeleteExpiredClientSessions(mock.Anything).
		Return(int64(0), nil).
		Once()

	mockQuerier.EXPECT().
		UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
			return arg.SessionID == int64(sessionID)
		})).
		Return(db.ClientSession{SessionID: int64(sessionID)}, nil).
		Once()

	mockQuerier.EXPECT().
		DeleteUnreferencedOrphanInodes(mock.Anything).
		Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
		Once()
}

func TestRelease_InputValidation(t *testing.T) {
	ctx := context.Background()

	zeroSession := uint64(0)
	overflowSession := uint64(math.MaxInt64) + 1

	for _, tc := range []struct {
		name         string
		ino          uint64
		fh           uint64
		releaseFlags uint32
		sessionID    *uint64
	}{
		{
			name: "ino 为 0",
			ino:  0,
			fh:   1,
		},
		{
			name: "ino 超过 math.MaxInt64",
			ino:  uint64(math.MaxInt64) + 1,
			fh:   1,
		},
		{
			name: "fh 为 0",
			ino:  1,
			fh:   0,
		},
		{
			name: "fh 超过 math.MaxInt64",
			ino:  1,
			fh:   uint64(math.MaxInt64) + 1,
		},
		{
			name: "ino 与 fh 均为 0",
			ino:  0,
			fh:   0,
		},
		{
			name:         "release_flags 包含未知位 (0x4)",
			ino:          1,
			fh:           1,
			releaseFlags: 0x4,
		},
		{
			name:         "release_flags 包含未知高位 (0x7)",
			ino:          1,
			fh:           1,
			releaseFlags: meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock | 0x4,
		},
		{
			name:      "session_id 为 0",
			ino:       1,
			fh:        1,
			sessionID: &zeroSession,
		},
		{
			name:      "session_id 超过 math.MaxInt64",
			ino:       1,
			fh:        1,
			sessionID: &overflowSession,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			err := mgr.Release(ctx, meta.ReleaseParams{
				Ino:          tc.ino,
				Fh:           tc.fh,
				ReleaseFlags: tc.releaseFlags,
				SessionID:    tc.sessionID,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}

	t.Run("允许携带 FuseReleaseFlush (0x1) 标志位正常销毁文件句柄", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{Ino: 10, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  1,
				Ino: 10,
			}).
			Return(int64(1), nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:          10,
			Fh:           1,
			ReleaseFlags: meta.FuseReleaseFlush,
		})
		require.NoError(t, err)
	})
}

func TestRelease_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseInode := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("正常节点释放 (nlink > 0): 销毁句柄且保留 Inode 行", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("携带 FuseReleaseFlockUnlock 但未传 session_id: 销毁句柄并按 (ino, fh) 清理 BSD flock 锁", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFlockFileLocksByFh(mock.Anything, db.DeleteFlockFileLocksByFhParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(int64(1), nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:          ino,
			Fh:           fh,
			ReleaseFlags: meta.FuseReleaseFlockUnlock,
		})
		require.NoError(t, err)
	})

	t.Run("同时携带 FuseReleaseFlockUnlock 与 session_id: 销毁句柄、按配置的 WithSessionLeaseTTL 续期会话并按 (session_id, ino, fh) 清理 BSD flock 锁", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		customTTL := 30 * time.Second
		mgr := meta.New(mockStore, meta.WithSessionLeaseTTL(customTTL))

		setupMockWriteTx(mockStore, mockQuerier)

		sessionID := uint64(888)
		lockOwner := uint64(12345)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		beforeCall := time.Now()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sessionID) &&
					arg.ExpiresAt.Valid &&
					arg.ExpiresAt.Time.After(beforeCall.Add(25*time.Second)) &&
					arg.ExpiresAt.Time.Before(beforeCall.Add(35*time.Second))
			})).
			Return(db.ClientSession{SessionID: int64(sessionID)}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFlockFileLocksBySessionAndFh(mock.Anything, db.DeleteFlockFileLocksBySessionAndFhParams{
				SessionID: int64(sessionID),
				Ino:       int64(ino),
				Fh:        int64(fh),
			}).
			Return(int64(1), nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:          ino,
			Fh:           fh,
			LockOwner:    &lockOwner,
			ReleaseFlags: meta.FuseReleaseFlockUnlock,
			SessionID:    &sessionID,
		})
		require.NoError(t, err)
	})

	t.Run("多句柄并发打开的 Orphan Inode (nlink == 0): 释放其中一个句柄，仍有剩余句柄时不删除 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(true, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan Inode (nlink == 0): 无剩余活跃句柄时物理回收 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan 常规文件 (nlink == 0, RootIno > 0, Size = 8192): 无剩余活跃句柄时物理回收 Inode 并退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0
		orphanInode.RootIno = 10
		orphanInode.Size = 8192

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     10,
				DeltaBytes:  -8192,
				DeltaInodes: -1,
			}).
			Return(nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("存在剩余活跃句柄的 Orphan 常规文件 (nlink == 0, RootIno > 0): 仅释放句柄，不调用 DeleteInode 与 AddDiskQuotaByRootIno", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0
		orphanInode.RootIno = 10
		orphanInode.Size = 8192

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(true, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("携带 SessionID 关闭 Orphan Inode (nlink == 0) 的最后一个句柄：先续期会话再删句柄且配额仅扣减一次", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		sessionID := uint64(888)
		orphanInode := baseInode
		orphanInode.Nlink = 0
		orphanInode.RootIno = 10
		orphanInode.Size = 4096

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		// 1. 先续期会话 (renewOptionalClientSession)
		setupRenewClientSessionMocks(mockQuerier, sessionID)

		// 2. 再删除句柄 (DeleteFileHandle)
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(int64(fh), nil).
			Once()

		// 3. 最终无句柄后回收孤儿节点 (reclaimOrphanIfNoHandles)
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     10,
				DeltaBytes:  -4096,
				DeltaInodes: -1,
			}).
			Return(nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:       ino,
			Fh:        fh,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
	})
}

func TestRelease_NegativeAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseInode := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("目标节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("目标节点为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirInode := baseInode
		dirInode.Mode = int32(meta.S_IFDIR | 0o755)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(dirInode, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("句柄不存在或归属不匹配返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), pgx.ErrNoRows).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("二次重复释放同一句柄返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), pgx.ErrNoRows).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("GetInodeForUpdate 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db query failed")
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("DeleteFileHandle 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		dbErr := errors.New("db delete handle failed")
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("HasFileHandlesByIno 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		dbErr := errors.New("db check handles failed")
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("DeleteInode 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		dbErr := errors.New("db delete inode failed")
		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("AddDiskQuotaByRootIno 底层数据库错误导致事务中止并透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0
		orphanInode.RootIno = 10
		orphanInode.Size = 8192

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		dbErr := errors.New("db add quota failed")
		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     10,
				DeltaBytes:  -8192,
				DeltaInodes: -1,
			}).
			Return(dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})
}

func TestRelease_PostgresFlockAndOrphanCleanupSemantics(t *testing.T) {
	dsn := os.Getenv("STORAGE_DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	rootDir, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	defer func() {
		_ = store.DeleteInode(context.Background(), rootDir.Ino)
	}()

	now := time.Now()
	sess1 := uint64(930001)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sess1))
	}()

	_, err = store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: int64(sess1),
		ExpiresAt: pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
	})
	require.NoError(t, err)

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: uint64(rootDir.Ino),
		Name:      []byte("release_lock_test.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh1 := createRes.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(fileIno))
	}()

	openRes2, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	fh2 := openRes2.Fh

	openRes3, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   fileIno,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh3 := openRes3.Fh

	ownerA := uint64(3001)

	// 1. 通过 fh1 申请 POSIX 记录锁 [0, 500]
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh1),
		LockOwner:   int64(ownerA),
		LkFlags:     0,
		StartOffset: 0,
		EndOffset:   500,
		LockType:    int16(meta.F_WRLCK),
		Pid:         1111,
	})
	require.NoError(t, err)

	// 2. fh2 与 fh3 各自持有一把 BSD flock 共享锁
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh2),
		LockOwner:   0,
		LkFlags:     int32(meta.FuseLkFlock),
		StartOffset: 0,
		EndOffset:   int64(meta.OffsetMax),
		LockType:    int16(meta.F_RDLCK),
		Pid:         2222,
	})
	require.NoError(t, err)

	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh3),
		LockOwner:   0,
		LkFlags:     int32(meta.FuseLkFlock),
		StartOffset: 0,
		EndOffset:   int64(meta.OffsetMax),
		LockType:    int16(meta.F_RDLCK),
		Pid:         3333,
	})
	require.NoError(t, err)

	// AC3: 对曾用于申请 POSIX 锁的 fh1 调用 Release（即使携带 FuseReleaseFlockUnlock）：
	// 由于无 fh 外键级联且 Release 绝不越权删 POSIX 锁，ownerA 的 POSIX 锁依然保持有效
	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino:          fileIno,
		Fh:           fh1,
		LockOwner:    &ownerA,
		ReleaseFlags: meta.FuseReleaseFlockUnlock,
		SessionID:    &sess1,
	})
	require.NoError(t, err)

	posixLocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)
	require.Len(t, posixLocks, 1, "销毁 fh1 后 POSIX 锁必须依然有效")
	assert.Equal(t, int64(ownerA), posixLocks[0].LockOwner)

	// AC4: 对 fh2 调用携带 FuseReleaseFlockUnlock 的 Release：
	// 精确释放 (sess1, fileIno, fh2) 绑定的 flock 锁，不影响同文件上的 POSIX 锁或 fh3 的 flock 锁
	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino:          fileIno,
		Fh:           fh2,
		ReleaseFlags: meta.FuseReleaseFlockUnlock,
		SessionID:    &sess1,
	})
	require.NoError(t, err)

	flockLocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: int32(meta.FuseLkFlock),
	})
	require.NoError(t, err)
	require.Len(t, flockLocks, 1, "fh2 的 flock 锁应被释放，仅保留 fh3 的 flock 锁")
	assert.Equal(t, int64(fh3), flockLocks[0].Fh)

	// AC5: 将文件 Unlink 变为 Orphan Inode (nlink == 0)，随后关闭最后一个句柄 fh3：
	// 触发节点物理回收，数据库 ON DELETE CASCADE 自动清理残留的全部 POSIX 锁与 flock 锁
	err = mgr.Unlink(ctx, uint64(rootDir.Ino), []byte("release_lock_test.txt"))
	require.NoError(t, err)

	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino: fileIno,
		Fh:  fh3,
	})
	require.NoError(t, err)

	_, err = mgr.GetAttr(ctx, fileIno)
	assert.ErrorIs(t, err, meta.ErrNotFound, "孤儿节点最后一个句柄释放后 Inode 应被物理回收")

	var remainingLocks int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(fileIno)).Scan(&remainingLocks)
	require.NoError(t, err)
	assert.Zero(t, remainingLocks, "孤儿节点物理回收后级联清理所有残留文件锁")
}

func TestRelease_QuotaReclaim_PostgresIntegration(t *testing.T) {
	dsn := os.Getenv("STORAGE_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("STORAGE_DB_DSN")
	}
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	t.Run("CreateDisk -> Create -> SetAttr(size=4096) -> Unlink(有活跃句柄暂不退还) -> Release(最后一个句柄关闭时退还 used_bytes 与 used_inodes)", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_quota_file_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		// 初始配额：仅包含根目录 Inode
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		// Create 创建常规文件并获得初始句柄
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_unlink.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NotNil(t, createRes)
		fileIno := createRes.Inode.Ino
		fileFh := createRes.Fh

		// 验证创建后配额：used_inodes + 1
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		// SetAttr 扩容常规文件至 4096 字节
		size4096 := uint64(4096)
		_, err = mgr.SetAttr(ctx, fileIno, meta.SetAttrInput{
			Size: &size4096,
		})
		require.NoError(t, err)

		// 验证扩容后配额：used_bytes + 4096
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(4096), d.UsedBytes)

		// Unlink 文件：存在活跃句柄 fileFh，进入 unlink-whilst-open，nlink 变 0 但暂不回收 Inode 也不退还配额
		err = mgr.Unlink(ctx, disk.RootIno, []byte("file_unlink.txt"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "有活跃句柄时 Unlink 不得退还 used_inodes")
		assert.Equal(t, uint64(4096), d.UsedBytes, "有活跃句柄时 Unlink 不得退还 used_bytes")

		// Release 关闭最后一个句柄：触发物理删除与配额归还
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: fileIno,
			Fh:  fileFh,
		})
		require.NoError(t, err)

		// 验证配额已退还：used_inodes 恢复为 1，used_bytes 恢复为 0
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes, "关闭最后一个句柄时应扣减 1 个 inode 配额")
		assert.Equal(t, uint64(0), d.UsedBytes, "关闭最后一个句柄时应退还 4096 字节配额")

		// Inode 物理回收确认
		_, err = mgr.GetAttr(ctx, fileIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound), "物理回收后 Inode 应不可见")
	})

	t.Run("CreateDisk -> TmpFile -> SetAttr(size=8192) -> Release(最后一个句柄关闭时退还 used_bytes 与 used_inodes)", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_quota_tmpfile_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		// TmpFile 创建匿名临时常规文件
		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: disk.RootIno,
			Mode:      meta.S_IFREG | 0o600,
			Flags:     meta.O_RDWR,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NotNil(t, tmpRes)
		tmpIno := tmpRes.Inode.Ino
		tmpFh := tmpRes.Fh

		// 验证配额：used_inodes + 1
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		// SetAttr 扩容至 8192 字节
		size8192 := uint64(8192)
		_, err = mgr.SetAttr(ctx, tmpIno, meta.SetAttrInput{
			Size: &size8192,
		})
		require.NoError(t, err)

		// 验证配额：used_bytes + 8192
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(8192), d.UsedBytes)

		// Release 关闭临时文件唯一句柄：物理回收 Inode 并退还配额
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: tmpIno,
			Fh:  tmpFh,
		})
		require.NoError(t, err)

		// 验证配额归还
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes, "释放 TmpFile 句柄应退还 1 个 inode 配额")
		assert.Equal(t, uint64(0), d.UsedBytes, "释放 TmpFile 句柄应退还 8192 字节配额")

		// Inode 物理回收确认
		_, err = mgr.GetAttr(ctx, tmpIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound), "物理回收后 TmpFile Inode 应不可见")
	})

	t.Run("CreateDisk -> Mkdir -> OpenDir -> Rmdir(有活跃句柄暂不退还) -> ReleaseDir(最后一个句柄关闭时退还 used_inodes)", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_quota_dir_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		// Mkdir 创建子目录
		dirInode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("subdir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NotNil(t, dirInode)
		dirIno := dirInode.Ino

		// 验证配额：used_inodes + 1
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		// OpenDir 打开目录获取目录句柄
		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino: dirIno,
		})
		require.NoError(t, err)
		require.NotNil(t, openDirRes)
		dirFh := openDirRes.Fh

		// Rmdir 删除目录：因存在活跃目录句柄 dirFh，进入 rmdir-whilst-open，nlink 变 0 但暂不回收 Inode 也不退还配额
		err = mgr.Rmdir(ctx, disk.RootIno, []byte("subdir"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "有活跃目录句柄时 Rmdir 不得退还 used_inodes")

		// ReleaseDir 关闭最后一个句柄：触发物理删除与退还 used_inodes 配额
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: dirIno,
			Fh:  dirFh,
		})
		require.NoError(t, err)

		// 验证配额归还
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes, "关闭最后一个目录句柄时应扣减 1 个 inode 配额")
		assert.Equal(t, uint64(0), d.UsedBytes)

		// Inode 物理回收确认
		_, err = mgr.GetAttr(ctx, dirIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound), "物理回收后目录 Inode 应不可见")
	})

	t.Run("携带 SessionID 关闭孤儿常规文件的最后一个句柄：验证 Size 和 UsedInodes 仅被扣减一次（无双重扣减）", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_session_quota_file_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessID := uint64(770001)

		// 1. 创建常驻文件 normal.txt，大小 8192 字节，用于提供配额基底
		normRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("normal.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		size8192 := uint64(8192)
		_, err = mgr.SetAttr(ctx, normRes.Inode.Ino, meta.SetAttrInput{
			Size:      &size8192,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 2. 创建即将被 unlink 的文件 orphan.txt，大小 4096 字节
		orphanRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		size4096 := uint64(4096)
		_, err = mgr.SetAttr(ctx, orphanRes.Inode.Ino, meta.SetAttrInput{
			Size:      &size4096,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 当前配额状态：根目录 + normal.txt + orphan.txt = 3 inodes；8192 + 4096 = 12288 bytes
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)
		assert.Equal(t, uint64(12288), d.UsedBytes)

		// 3. Unlink orphan.txt：此时 orphanRes.Fh 依然打开，nlink 变 0 但节点暂不回收，配额不变
		err = mgr.Unlink(ctx, disk.RootIno, []byte("orphan.txt"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)
		assert.Equal(t, uint64(12288), d.UsedBytes)

		// 4. Release 关闭 orphan.txt 句柄（携带 SessionID）
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:       orphanRes.Inode.Ino,
			Fh:        orphanRes.Fh,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 5. 校验配额：必须且仅扣减 1 个 inode 和 4096 字节（剩余 2 个 inodes 和 8192 字节）
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "孤儿文件释放后应恰好剩余 2 个 inode (根目录与 normal.txt)")
		assert.Equal(t, uint64(8192), d.UsedBytes, "孤儿文件释放后应恰好剩余 8192 字节 (normal.txt 占用)")
	})
}

