package meta_test

import (
	"context"
	"errors"
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
			name:         "release_flags 包含未知位 (0x1)",
			ino:          1,
			fh:           1,
			releaseFlags: 0x1,
		},
		{
			name:         "release_flags 包含未知高位 (0x3)",
			ino:          1,
			fh:           1,
			releaseFlags: meta.FuseReleaseFlockUnlock | 0x1,
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

	t.Run("同时携带 FuseReleaseFlockUnlock 与 session_id: 销毁句柄、续期会话并按 (session_id, ino, fh) 清理 BSD flock 锁", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		sessionID := uint64(888)
		lockOwner := uint64(12345)

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
			DeleteExpiredClientSessions(mock.Anything).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sessionID) && arg.ExpiresAt.Valid && arg.ExpiresAt.Time.After(time.Now())
			})).
			Return(db.ClientSession{SessionID: int64(sessionID)}, nil).
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

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: uint64(rootDir.Ino),
		Name:      []byte("release_lock_test.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh1 := createRes.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(fileIno))
	}()

	openRes2, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   fileIno,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh2 := openRes2.Fh

	openRes3, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   fileIno,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh3 := openRes3.Fh

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

