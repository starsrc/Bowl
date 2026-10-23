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

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// setupMockReadOnlyRepeatableReadTx 设置只读 RepeatableRead 事务期望，并指定预期调用次数。
func setupMockReadOnlyRepeatableReadTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier, times int) {
	mockStore.EXPECT().
		ExecTxWithOptions(
			mock.Anything,
			mock.Anything,
			matchReadOnlyOpt(),
			matchRepeatableReadOpt(),
		).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			cfg := tx.ApplyOptions(opts...)
			if cfg.AccessMode != tx.ReadOnly || cfg.IsoLevel != tx.RepeatableRead {
				return fmt.Errorf("unexpected tx options: AccessMode=%v, IsoLevel=%v", cfg.AccessMode, cfg.IsoLevel)
			}
			return fn(mockQuerier)
		}).
		Times(times)
}

func TestFlush_PositivePath(t *testing.T) {
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

	baseHandle := db.FileHandle{
		Fh:        int64(fh),
		Ino:       int64(ino),
		Flags:     int32(meta.O_RDWR),
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("常规文件合法句柄冲刷成功 (无 lock_owner)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(baseHandle, nil).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: nil,
		})
		require.NoError(t, err)
	})

	t.Run("常规文件合法句柄冲刷成功 (携带 lock_owner 但未传 session_id: 写事务按 (ino, lock_owner) 清理 POSIX 锁)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		lockOwner := uint64(0x8000000000000042)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(baseHandle, nil).
			Once()

		mockQuerier.EXPECT().
			DeletePosixFileLocksByOwner(mock.Anything, db.DeletePosixFileLocksByOwnerParams{
				Ino:       int64(ino),
				LockOwner: int64(lockOwner),
			}).
			Return(int64(2), nil).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: &lockOwner,
		})
		require.NoError(t, err)
	})

	t.Run("常规文件合法句柄冲刷成功 (同时携带 lock_owner 与 session_id: 按配置的 WithSessionLeaseTTL 续期会话并按 (session_id, ino, lock_owner) 清理 POSIX 锁)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		customTTL := 30 * time.Second
		mgr := meta.New(mockStore, meta.WithSessionLeaseTTL(customTTL))

		setupMockWriteTx(mockStore, mockQuerier)

		lockOwner := uint64(987654)
		sessionID := uint64(777)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(baseHandle, nil).
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
			DeletePosixFileLocksBySessionAndOwner(mock.Anything, db.DeletePosixFileLocksBySessionAndOwnerParams{
				SessionID: int64(sessionID),
				Ino:       int64(ino),
				LockOwner: int64(lockOwner),
			}).
			Return(int64(1), nil).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: &lockOwner,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
	})
}

func TestFlush_IdempotencyAndHandleSurvival(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(200)
	fh := uint64(42)

	baseInode := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       2048,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	baseHandle := db.FileHandle{
		Fh:        int64(fh),
		Ino:       int64(ino),
		Flags:     int32(meta.O_RDWR),
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("连续多次调用 Flush 均成功且不销毁 FileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		// 预期调用 3 次 Flush
		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 3)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Times(3)

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(baseHandle, nil).
			Times(3)

		params := meta.FlushParams{
			Ino: ino,
			Fh:  fh,
		}

		for i := 0; i < 3; i++ {
			err := mgr.Flush(ctx, params)
			require.NoError(t, err, "第 %d 次 Flush 必须成功", i+1)
		}
	})
}

func TestFlush_InputValidation(t *testing.T) {
	ctx := context.Background()

	zeroSession := uint64(0)
	overflowSession := uint64(math.MaxInt64) + 1

	for _, tc := range []struct {
		name      string
		ino       uint64
		fh        uint64
		sessionID *uint64
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

			err := mgr.Flush(ctx, meta.FlushParams{
				Ino:       tc.ino,
				Fh:        tc.fh,
				SessionID: tc.sessionID,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestFlush_InvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	ino := uint64(300)
	fh := uint64(5)

	t.Run("节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("节点为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFDIR | 0o755),
			}, nil).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
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

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{Ino: int64(ino)}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		expectedErr := errors.New("database connection failed")
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, expectedErr).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
	})

	t.Run("携带 lock_owner 时节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		lockOwner := uint64(1001)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: &lockOwner,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("携带 lock_owner 时节点为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		lockOwner := uint64(1001)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFDIR | 0o755),
			}, nil).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: &lockOwner,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("携带 lock_owner 时句柄不存在或归属不匹配返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		lockOwner := uint64(1001)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: &lockOwner,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})
}

func TestFlush_PostgresLockCleanupSemantics(t *testing.T) {
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
		Name:      []byte("flush_lock_test.txt"),
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

	now := time.Now()
	sess1 := uint64(920001)
	sess2 := uint64(920002)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sess1), int64(sess2))
	}()

	_, err = store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: int64(sess1),
		ExpiresAt: pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
	})
	require.NoError(t, err)

	_, err = store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: int64(sess2),
		ExpiresAt: pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
	})
	require.NoError(t, err)

	openRes2, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	fh2 := openRes2.Fh

	ownerA := uint64(0x9000000000000001)
	ownerB := uint64(2002)

	// 1. ownerA 通过 fh1 加 POSIX 锁 [0, 100] 与 [200, 300]
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh1),
		LockOwner:   int64(ownerA),
		LkFlags:     0,
		StartOffset: 0,
		EndOffset:   100,
		LockType:    int16(meta.F_WRLCK),
		Pid:         1111,
	})
	require.NoError(t, err)

	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh1),
		LockOwner:   int64(ownerA),
		LkFlags:     0,
		StartOffset: 200,
		EndOffset:   300,
		LockType:    int16(meta.F_RDLCK),
		Pid:         1111,
	})
	require.NoError(t, err)

	// 2. ownerB 在同文件加 POSIX 锁 [500, 600]
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh1),
		LockOwner:   int64(ownerB),
		LkFlags:     0,
		StartOffset: 500,
		EndOffset:   600,
		LockType:    int16(meta.F_WRLCK),
		Pid:         2222,
	})
	require.NoError(t, err)

	// 3. fh2 同时持有一把 BSD flock 排他锁
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh2),
		LockOwner:   int64(ownerA),
		LkFlags:     int32(meta.FuseLkFlock),
		StartOffset: 0,
		EndOffset:   int64(meta.OffsetMax),
		LockType:    int16(meta.F_WRLCK),
		Pid:         1111,
	})
	require.NoError(t, err)

	// AC1 & AC2: 对 fh2 调用携带 ownerA 与 sess1 的 Flush：
	// - 必须立即释放 ownerA 在该 Inode 上持有的全部 POSIX 锁（即使原本通过 fh1 申请）
	// - 绝不释放 fh2 上的 BSD flock 锁
	// - 绝不影响 ownerB 的 POSIX 锁
	// - 不销毁 fh2
	err = mgr.Flush(ctx, meta.FlushParams{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: &ownerA,
		SessionID: &sess1,
	})
	require.NoError(t, err)

	posixLocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)
	require.Len(t, posixLocks, 1, "ownerA 的两把 POSIX 锁应被全部清除，仅剩 ownerB 的一把 POSIX 锁")
	assert.Equal(t, int64(ownerB), posixLocks[0].LockOwner)

	flockLocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: int32(meta.FuseLkFlock),
	})
	require.NoError(t, err)
	require.Len(t, flockLocks, 1, "Flush 绝不清理 BSD flock 锁")
	assert.Equal(t, int64(fh2), flockLocks[0].Fh)

	// AC5: 存量未传 session_id 的 Flush 调用按 (ino, lock_owner) 清理 ownerB 的 POSIX 锁
	err = mgr.Flush(ctx, meta.FlushParams{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: &ownerB,
	})
	require.NoError(t, err)

	posixLocksAfterLegacy, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, posixLocksAfterLegacy)
}

func TestFsync_PositivePath(t *testing.T) {
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

	baseHandle := db.FileHandle{
		Fh:        int64(fh),
		Ino:       int64(ino),
		Flags:     int32(meta.O_RDWR),
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("全量元数据同步成功 (datasync = false)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(baseHandle, nil).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.NoError(t, err)
	})

	t.Run("仅数据同步成功 (datasync = true)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(baseHandle, nil).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: true,
		})
		require.NoError(t, err)
	})
}

func TestFsync_InputValidation(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		ino  uint64
		fh   uint64
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			err := mgr.Fsync(ctx, meta.FsyncParams{
				Ino:      tc.ino,
				Fh:       tc.fh,
				Datasync: true,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestFsync_InvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	ino := uint64(300)
	fh := uint64(5)

	t.Run("节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("节点为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFDIR | 0o755),
			}, nil).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("句柄不存在或归属不匹配返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{Ino: int64(ino)}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: true,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		expectedErr := errors.New("database connection failed")
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, expectedErr).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
	})
}
