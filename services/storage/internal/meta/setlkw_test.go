package meta_test

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"
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

func TestSetLkw_InputValidation(t *testing.T) {
	ctx := context.Background()

	validBase := meta.SetLkwParams{
		Ino:       100,
		Fh:        1,
		LockOwner: 12345,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   999,
		},
		LkFlags:   0,
		SessionID: 1,
		RequestID: 10001,
	}

	testCases := []struct {
		name   string
		mutate func(p *meta.SetLkwParams)
	}{
		{
			name:   "ino 为 0",
			mutate: func(p *meta.SetLkwParams) { p.Ino = 0 },
		},
		{
			name:   "ino 超过 math.MaxInt64",
			mutate: func(p *meta.SetLkwParams) { p.Ino = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "fh 为 0",
			mutate: func(p *meta.SetLkwParams) { p.Fh = 0 },
		},
		{
			name:   "fh 超过 math.MaxInt64",
			mutate: func(p *meta.SetLkwParams) { p.Fh = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "session_id 为 0",
			mutate: func(p *meta.SetLkwParams) { p.SessionID = 0 },
		},
		{
			name:   "session_id 超过 math.MaxInt64",
			mutate: func(p *meta.SetLkwParams) { p.SessionID = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "request_id 为 0",
			mutate: func(p *meta.SetLkwParams) { p.RequestID = 0 },
		},
		{
			name:   "request_id 超过 OffsetMax",
			mutate: func(p *meta.SetLkwParams) { p.RequestID = meta.OffsetMax + 1 },
		},
		{
			name:   "lk_flags 非法 (> FuseLkFlock)",
			mutate: func(p *meta.SetLkwParams) { p.LkFlags = 2 },
		},
		{
			name:   "lk.Type 非法 (> F_UNLCK)",
			mutate: func(p *meta.SetLkwParams) { p.Lk.Type = 3 },
		},
		{
			name: "lk.Start > lk.End",
			mutate: func(p *meta.SetLkwParams) {
				p.Lk.Start = 200
				p.Lk.End = 100
			},
		},
		{
			name: "lk.Start 超过 OffsetMax",
			mutate: func(p *meta.SetLkwParams) {
				p.Lk.Start = meta.OffsetMax + 1
				p.Lk.End = meta.OffsetMax + 1
			},
		},
		{
			name:   "lk.End 超过 OffsetMax",
			mutate: func(p *meta.SetLkwParams) { p.Lk.End = meta.OffsetMax + 1 },
		},
		{
			name: "FUSE_LK_FLOCK 但 start != 0",
			mutate: func(p *meta.SetLkwParams) {
				p.LkFlags = meta.FuseLkFlock
				p.Lk.Start = 10
				p.Lk.End = meta.OffsetMax
			},
		},
		{
			name: "FUSE_LK_FLOCK 但 end != OffsetMax",
			mutate: func(p *meta.SetLkwParams) {
				p.LkFlags = meta.FuseLkFlock
				p.Lk.Start = 0
				p.Lk.End = 4096
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			p := validBase
			tc.mutate(&p)

			err := mgr.SetLkw(ctx, p)
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestSetLkw_InodeAndOpenModeValidation(t *testing.T) {
	ctx := context.Background()
	ino := uint64(310)
	fh := uint64(15)

	validParams := meta.SetLkwParams{
		Ino:       ino,
		Fh:        fh,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   1234,
		},
		LkFlags:   0,
		SessionID: 1,
		RequestID: 5001,
	}

	t.Run("节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.SetLkw(ctx, validParams)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("节点为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFDIR | 0o755),
			}, nil).
			Once()

		err := mgr.SetLkw(ctx, validParams)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("POSIX 锁在只读 FileHandle (O_RDONLY) 上申请写锁 (F_WRLCK) 返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileLockWaiter(mock.Anything, db.GetFileLockWaiterParams{
				SessionID: 1,
				RequestID: 5001,
			}).
			Return(db.FileLockWaiter{}, pgx.ErrNoRows).
			Maybe()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 1, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		err := mgr.SetLkw(ctx, validParams)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("首轮无冲突时直接原子授锁并清理 waiter，支持全范围 uint64 lock_owner", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		highLockOwner := uint64(0x9000000000000099)
		highPID := uint32(math.MaxInt32) + 99

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileLockWaiter(mock.Anything, db.GetFileLockWaiterParams{
				SessionID: 1,
				RequestID: 5002,
			}).
			Return(db.FileLockWaiter{}, pgx.ErrNoRows).
			Maybe()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 1, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]int64(nil), nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			ListPendingFileLockWaitersByInoAndFlags(mock.Anything, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   1,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   int64(highLockOwner),
				LkFlags:     0,
				StartOffset: 0,
				EndOffset:   100,
				LockType:    int16(meta.F_WRLCK),
				Pid:         int32(highPID),
			}).
			Return(db.FileLock{ID: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileLockWaiter(mock.Anything, db.DeleteFileLockWaiterParams{
				SessionID: 1,
				RequestID: 5002,
			}).
			Return(0, nil).
			Maybe()

		p := validParams
		p.LockOwner = highLockOwner
		p.Lk.PID = highPID
		p.RequestID = 5002
		err := mgr.SetLkw(ctx, p)
		require.NoError(t, err)
	})
}

func TestSetLkw_UnitDeadlockAndStateMachine(t *testing.T) {
	ctx := context.Background()
	ino := uint64(700)
	fh1 := uint64(31)
	fh2 := uint64(32)

	baseInode := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
	}

	t.Run("同节点双读锁升级写锁环路 (A <-> B) 立即返回 ErrDeadlock 且不插入新 waiter", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileLockWaiter(mock.Anything, db.GetFileLockWaiterParams{
				SessionID: 2,
				RequestID: 9002,
			}).
			Return(db.FileLockWaiter{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh2),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 2, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh2),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 2}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]int64(nil), nil).
			Once()

		// A (session=1, owner=101) 与 B (session=2, owner=202) 均持有 [0, 100] 的 F_RDLCK
		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          1,
					SessionID:   1,
					Ino:         int64(ino),
					Fh:          int64(fh1),
					LockOwner:   101,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   100,
					LockType:    int16(meta.F_RDLCK),
					Pid:         1001,
				},
				{
					ID:          2,
					SessionID:   2,
					Ino:         int64(ino),
					Fh:          int64(fh2),
					LockOwner:   202,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   100,
					LockType:    int16(meta.F_RDLCK),
					Pid:         2002,
				},
			}, nil).
			Once()

		// A 已经在 PENDING 队列中等待 [0, 100] 的 F_WRLCK（等待 B 释放读锁）
		mockQuerier.EXPECT().
			ListPendingFileLockWaitersByInoAndFlags(mock.Anything, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLockWaiter{
				{
					SessionID:   1,
					RequestID:   9001,
					Ino:         int64(ino),
					Fh:          int64(fh1),
					LockOwner:   101,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   100,
					LockType:    int16(meta.F_WRLCK),
					Pid:         1001,
					Status:      meta.WaiterStatusPending,
				},
			}, nil).
			Once()

		// B 此时调用 SetLkw 请求升级为 [0, 100] 的 F_WRLCK，形成 B -> A -> B 环路，必须返回 ErrDeadlock
		err := mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       ino,
			Fh:        fh2,
			LockOwner: 202,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   2002,
			},
			LkFlags:   0,
			SessionID: 2,
			RequestID: 9002,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrDeadlock))
	})

	t.Run("同节点三方等待环路 (A -> B -> C -> A) 立即返回 ErrDeadlock", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileLockWaiter(mock.Anything, db.GetFileLockWaiterParams{
				SessionID: 3,
				RequestID: 9003,
			}).
			Return(db.FileLockWaiter{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh1),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 3, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh1),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 3}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]int64(nil), nil).
			Once()

		// A (1, 101) 持有 [0, 99] 写锁；B (2, 202) 持有 [100, 199] 写锁；C (3, 303) 持有 [200, 299] 写锁
		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          1,
					SessionID:   1,
					Ino:         int64(ino),
					Fh:          int64(fh1),
					LockOwner:   101,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   99,
					LockType:    int16(meta.F_WRLCK),
				},
				{
					ID:          2,
					SessionID:   2,
					Ino:         int64(ino),
					Fh:          int64(fh1),
					LockOwner:   202,
					LkFlags:     0,
					StartOffset: 100,
					EndOffset:   199,
					LockType:    int16(meta.F_WRLCK),
				},
				{
					ID:          3,
					SessionID:   3,
					Ino:         int64(ino),
					Fh:          int64(fh1),
					LockOwner:   303,
					LkFlags:     0,
					StartOffset: 200,
					EndOffset:   299,
					LockType:    int16(meta.F_WRLCK),
				},
			}, nil).
			Once()

		// A 正在等待 [100, 199] (A -> B)；B 正在等待 [200, 299] (B -> C)
		mockQuerier.EXPECT().
			ListPendingFileLockWaitersByInoAndFlags(mock.Anything, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLockWaiter{
				{
					SessionID:   1,
					RequestID:   9001,
					Ino:         int64(ino),
					Fh:          int64(fh1),
					LockOwner:   101,
					LkFlags:     0,
					StartOffset: 100,
					EndOffset:   199,
					LockType:    int16(meta.F_WRLCK),
					Status:      meta.WaiterStatusPending,
				},
				{
					SessionID:   2,
					RequestID:   9002,
					Ino:         int64(ino),
					Fh:          int64(fh1),
					LockOwner:   202,
					LkFlags:     0,
					StartOffset: 200,
					EndOffset:   299,
					LockType:    int16(meta.F_WRLCK),
					Status:      meta.WaiterStatusPending,
				},
			}, nil).
			Once()

		// C 请求 [0, 99] (C -> A)，形成 C -> A -> B -> C 环路
		err := mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       ino,
			Fh:        fh1,
			LockOwner: 303,
			Lk: meta.FileLock{
				Start: 0,
				End:   99,
				Type:  meta.F_WRLCK,
				PID:   3003,
			},
			LkFlags:   0,
			SessionID: 3,
			RequestID: 9003,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrDeadlock))
	})

	t.Run("原子状态机：当 waiter 已被标记为 CANCELLED 时，授锁事务立即删除 waiter 并返回 ErrInterrupted，绝不写入 file_locks", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileLockWaiter(mock.Anything, db.GetFileLockWaiterParams{
				SessionID: 1,
				RequestID: 9009,
			}).
			Return(db.FileLockWaiter{
				SessionID: 1,
				RequestID: 9009,
				Ino:       int64(ino),
				Status:    meta.WaiterStatusCancelled,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileLockWaiter(mock.Anything, db.DeleteFileLockWaiterParams{
				SessionID: 1,
				RequestID: 9009,
			}).
			Return(1, nil).
			Once()

		err := mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       ino,
			Fh:        fh1,
			LockOwner: 101,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   1001,
			},
			LkFlags:   0,
			SessionID: 1,
			RequestID: 9009,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInterrupted))
	})
}

func TestSetLkw_PostgresAcceptanceCriteria(t *testing.T) {
	dsn := os.Getenv("STORAGE_DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

	s1 := uint64(950001)
	s2 := uint64(950002)
	s3 := uint64(950003)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2, $3)", int64(s1), int64(s2), int64(s3))
	}()

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: uint64(rootDir.Ino),
		Name:      []byte("setlkw_acceptance.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &s1,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh1 := createRes.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(fileIno))
	}()

	open2, err := mgr.Open(ctx, meta.OpenParams{Ino: fileIno, Flags: meta.O_RDWR, SessionID: &s2})
	require.NoError(t, err)
	fh2 := open2.Fh

	open3, err := mgr.Open(ctx, meta.OpenParams{Ino: fileIno, Flags: meta.O_RDWR, SessionID: &s3})
	require.NoError(t, err)
	fh3 := open3.Fh

	waitForWaiterCount := func(t *testing.T, lkFlags uint32, expected int) {
		t.Helper()
		require.Eventually(t, func() bool {
			waiters, err := store.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(fileIno),
				LkFlags: int32(lkFlags),
			})
			return err == nil && len(waiters) == expected
		}, 3*time.Second, 5*time.Millisecond)
	}

	// =========================================================================
	// AC 1: SetLkw 在遇到 POSIX 或 BSD flock 冲突锁时释放数据库行锁并在事务外阻塞等待；
	// 当持锁方通过 SetLk(F_UNLCK)、锁降级、Flush(lock_owner) 或 Release(FUSE_RELEASE_FLOCK_UNLOCK)
	// 释放冲突锁后，SetLkw 自动唤醒并成功获得锁。
	// =========================================================================
	t.Run("AC1_阻塞等待与四种释放唤醒路径(SetLk解锁_降级_Flush_Release)", func(t *testing.T) {
		// 1.1 SetLk(F_UNLCK) 唤醒
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))

		errCh := make(chan error, 1)
		go func() {
			errCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: 202,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 2002},
				LkFlags:   0,
				SessionID: s2,
				RequestID: 1001,
			})
		}()

		waitForWaiterCount(t, 0, 1)

		// 验证 SetLkw 已释放数据库 Inode 行锁，s1 可立即执行 SetLk(F_UNLCK) 而不被行锁阻塞
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))

		require.NoError(t, <-errCh)
		waitForWaiterCount(t, 0, 0)

		// 1.2 锁降级 (F_WRLCK -> F_RDLCK) 唤醒等待读锁的 SetLkw
		errCh = make(chan error, 1)
		go func() {
			errCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh1,
				LockOwner: 101,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
				LkFlags:   0,
				SessionID: s1,
				RequestID: 1002,
			})
		}()

		waitForWaiterCount(t, 0, 1)

		// s2 将 [0, 100] 从 F_WRLCK 降级为 F_RDLCK，s1 的读锁等待立即被唤醒并获锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 2002},
			LkFlags:   0,
			SessionID: s2,
		}))

		require.NoError(t, <-errCh)

		// 清理 s1 的读锁，保留 s2 的读锁并将 s2 升为写锁以测试 Flush 唤醒
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 2002},
			LkFlags:   0,
			SessionID: s2,
		}))

		// 1.3 Flush(lock_owner) 唤醒 POSIX SetLkw
		errCh = make(chan error, 1)
		go func() {
			errCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh1,
				LockOwner: 101,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1001},
				LkFlags:   0,
				SessionID: s1,
				RequestID: 1003,
			})
		}()

		waitForWaiterCount(t, 0, 1)

		owner202 := uint64(202)
		require.NoError(t, mgr.Flush(ctx, meta.FlushParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: &owner202,
			SessionID: &s2,
		}))

		require.NoError(t, <-errCh)

		// 清理 s1 POSIX 锁
		owner101 := uint64(101)
		require.NoError(t, mgr.Flush(ctx, meta.FlushParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: &owner101,
			SessionID: &s1,
		}))

		// 1.4 Release(FUSE_RELEASE_FLOCK_UNLOCK) 唤醒 BSD flock SetLkw
		openTemp, err := mgr.Open(ctx, meta.OpenParams{Ino: fileIno, Flags: meta.O_RDONLY, SessionID: &s1})
		require.NoError(t, err)
		fhTemp := openTemp.Fh

		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhTemp,
			LockOwner: 999,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 9001},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s1,
		}))

		errCh = make(chan error, 1)
		go func() {
			errCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: 888,
				Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 9002},
				LkFlags:   meta.FuseLkFlock,
				SessionID: s2,
				RequestID: 1004,
			})
		}()

		waitForWaiterCount(t, meta.FuseLkFlock, 1)

		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:          fileIno,
			Fh:           fhTemp,
			ReleaseFlags: meta.FuseReleaseFlockUnlock,
			SessionID:    &s1,
		}))

		require.NoError(t, <-errCh)

		// 释放 fh2 的 flock
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 888,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 9002},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s2,
		}))
	})

	// =========================================================================
	// AC 2: 当同一 Inode 上两个持有者分别持有重叠区间的 F_RDLCK，第二个持有者调用 SetLkw 请求升级为 F_WRLCK
	// （与第一个持有者已 PENDING 的 F_WRLCK 形成环路），或形成三方等待环路 A -> B -> C -> A 时，
	// 立即返回 ErrDeadlock (EDEADLK)，且被阻塞的第一个等待者可在死锁方解锁后正常获锁。
	// =========================================================================
	t.Run("AC2_同Inode双读升写与三方等待环路死锁检测及存活方正常获锁", func(t *testing.T) {
		// A 与 B 均获取 [0, 100] 的读锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 2002},
			LkFlags:   0,
			SessionID: s2,
		}))

		// A 调用 SetLkw 请求升级为 [0, 100] 的 F_WRLCK，进入 PENDING 等待
		aErrCh := make(chan error, 1)
		go func() {
			aErrCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh1,
				LockOwner: 101,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1001},
				LkFlags:   0,
				SessionID: s1,
				RequestID: 2001,
			})
		}()

		waitForWaiterCount(t, 0, 1)

		// B 调用 SetLkw 请求升级为 [50, 150] 的 F_WRLCK（与 [0, 100] 重叠），立即检测到环路返回 ErrDeadlock
		err := mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 50, End: 150, Type: meta.F_WRLCK, PID: 2002},
			LkFlags:   0,
			SessionID: s2,
			RequestID: 2002,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrDeadlock))

		// B 解锁其读锁后，原本被阻塞的 A 自动唤醒并成功升级为 F_WRLCK
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 2002},
			LkFlags:   0,
			SessionID: s2,
		}))

		require.NoError(t, <-aErrCh)

		// 清理 A 的锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))

		// 验证三方环路 A -> B -> C -> A
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 99, Type: meta.F_WRLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 100, End: 199, Type: meta.F_WRLCK, PID: 2002},
			LkFlags:   0,
			SessionID: s2,
		}))
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh3,
			LockOwner: 303,
			Lk:        meta.FileLock{Start: 200, End: 299, Type: meta.F_WRLCK, PID: 3003},
			LkFlags:   0,
			SessionID: s3,
		}))

		aCycleCh := make(chan error, 1)
		go func() {
			aCycleCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh1,
				LockOwner: 101,
				Lk:        meta.FileLock{Start: 100, End: 199, Type: meta.F_WRLCK, PID: 1001},
				LkFlags:   0,
				SessionID: s1,
				RequestID: 2011,
			})
		}()
		waitForWaiterCount(t, 0, 1)

		bCycleCh := make(chan error, 1)
		go func() {
			bCycleCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: 202,
				Lk:        meta.FileLock{Start: 200, End: 299, Type: meta.F_WRLCK, PID: 2002},
				LkFlags:   0,
				SessionID: s2,
				RequestID: 2012,
			})
		}()
		waitForWaiterCount(t, 0, 2)

		// C 请求 [0, 99] 触发三方环路 C -> A -> B -> C，立即返回 ErrDeadlock
		err = mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fh3,
			LockOwner: 303,
			Lk:        meta.FileLock{Start: 0, End: 99, Type: meta.F_WRLCK, PID: 3003},
			LkFlags:   0,
			SessionID: s3,
			RequestID: 2013,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrDeadlock))

		// C 释放 [200, 299]，B 随即获锁；B 释放 [100, 299]，A 随即获锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh3,
			LockOwner: 303,
			Lk:        meta.FileLock{Start: 200, End: 299, Type: meta.F_UNLCK, PID: 3003},
			LkFlags:   0,
			SessionID: s3,
		}))
		require.NoError(t, <-bCycleCh)

		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 2002},
			LkFlags:   0,
			SessionID: s2,
		}))
		require.NoError(t, <-aCycleCh)

		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))
	})

	// =========================================================================
	// AC 3: 当持有者 A 持有读锁、等待者 B 正在 SetLkw 中 PENDING 等待写锁时，
	// 新到达且未在该 Inode 持锁的调用者 C 调用 SetLk(F_RDLCK) 返回 ErrAgain (EAGAIN)，
	// 调用 SetLkw(F_RDLCK) 则排在 B 之后等待，直至 B 获得并释放写锁后 C 方可获锁。
	// =========================================================================
	t.Run("AC3_防写锁饥饿排队_新到达读锁不可插队越过PENDING写锁", func(t *testing.T) {
		// A 持有 [0, 100] 的读锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))

		var orderMu sync.Mutex
		var grantOrder []string

		// B 调用 SetLkw 请求 [0, 100] 写锁，进入 PENDING
		bDone := make(chan error, 1)
		go func() {
			err := mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: 202,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 2002},
				LkFlags:   0,
				SessionID: s2,
				RequestID: 3001,
			})
			if err == nil {
				orderMu.Lock()
				grantOrder = append(grantOrder, "B")
				orderMu.Unlock()
			}
			bDone <- err
		}()

		waitForWaiterCount(t, 0, 1)

		// C 当前未在该 Inode 持有任何锁，调用 SetLk(F_RDLCK) 必须因排在 B 之后而返回 ErrAgain
		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh3,
			LockOwner: 303,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 3003},
			LkFlags:   0,
			SessionID: s3,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrAgain))

		// C 调用 SetLkw(F_RDLCK) 则排在 B 之后进入 PENDING 队列
		var cFinished atomic.Bool
		cDone := make(chan error, 1)
		go func() {
			err := mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh3,
				LockOwner: 303,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 3003},
				LkFlags:   0,
				SessionID: s3,
				RequestID: 3002,
			})
			if err == nil {
				orderMu.Lock()
				grantOrder = append(grantOrder, "C")
				orderMu.Unlock()
			}
			cFinished.Store(true)
			cDone <- err
		}()

		waitForWaiterCount(t, 0, 2)
		assert.False(t, cFinished.Load(), "C must block behind pending writer B")

		// A 解锁读锁后，B 必须先获得写锁，而 C 仍被 B 的写锁阻塞
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))

		require.NoError(t, <-bDone)
		assert.False(t, cFinished.Load(), "C must still wait while B holds the write lock")

		// B 释放写锁后，C 自动唤醒并获得读锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 2002},
			LkFlags:   0,
			SessionID: s2,
		}))

		require.NoError(t, <-cDone)

		orderMu.Lock()
		assert.Equal(t, []string{"B", "C"}, grantOrder)
		orderMu.Unlock()

		// 清理 C 的读锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh3,
			LockOwner: 303,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 3003},
			LkFlags:   0,
			SessionID: s3,
		}))
	})

	// =========================================================================
	// AC 4: 当 SetLkw 阻塞等待期间 ctx 被取消（模拟 FUSE_INTERRUPT）时，返回 ErrInterrupted (EINTR)，
	// 且 file_lock_waiters 与 file_locks 中绝不残留该请求的孤儿等待记录或幽灵悬挂锁。
	// =========================================================================
	t.Run("AC4_阻塞期间ctx取消返回EINTR且不残留孤儿waiter或幽灵悬挂锁", func(t *testing.T) {
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))

		cancelCtx, cancelWaiter := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func() {
			errCh <- mgr.SetLkw(cancelCtx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: 202,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 2002},
				LkFlags:   0,
				SessionID: s2,
				RequestID: 4001,
			})
		}()

		waitForWaiterCount(t, 0, 1)

		// 触发取消（模拟 FUSE_INTERRUPT）
		cancelWaiter()

		err := <-errCh
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInterrupted))

		// 验证 file_lock_waiters 中无残留记录
		waiters, err := store.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)
		assert.Empty(t, waiters)

		_, err = store.GetFileLockWaiter(ctx, db.GetFileLockWaiterParams{
			SessionID: int64(s2),
			RequestID: 4001,
		})
		assert.True(t, errors.Is(err, pgx.ErrNoRows))

		// 当 s1 随后解锁时，验证 file_locks 中绝无 s2 的幽灵悬挂锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 1001},
			LkFlags:   0,
			SessionID: s1,
		}))

		activeLocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)
		assert.Empty(t, activeLocks)
	})

	// =========================================================================
	// AC 5: BSD flock 持有共享锁的句柄 fh1 调用 SetLkw 升级排他锁且与 fh2 的共享锁冲突时，
	// 首轮事务即持久化删除 fh1 的原共享锁并唤醒可能等待它的其他句柄；
	// 若随后 fh1 的 SetLkw 被取消返回 EINTR，原共享锁亦已按非原子语义释放。
	// =========================================================================
	t.Run("AC5_BSD_flock非原子模式转换在SetLkw首轮事务即释放旧锁且取消后不恢复", func(t *testing.T) {
		beforeInode, err := mgr.GetAttr(ctx, fileIno)
		require.NoError(t, err)

		// 场景 A：fh1 持有排他 flock (F_WRLCK)，fh3 正在 SetLkw 等待共享 flock (F_RDLCK)，fh2 持有共享锁的预备验证：
		// 先验证 fh1 与 fh2 均获取共享 flock (F_RDLCK)
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 1001},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s1,
		}))
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 2002},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s2,
		}))

		// fh2 先启动 SetLkw 升级排他锁 F_WRLCK（首轮删除 fh2 的旧共享锁，因 fh1 仍持有共享锁而进入 PENDING 等待 fh1）
		fh2UpgradeCh := make(chan error, 1)
		go func() {
			fh2UpgradeCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: 202,
				Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 2003},
				LkFlags:   meta.FuseLkFlock,
				SessionID: s2,
				RequestID: 5000,
			})
		}()

		waitForWaiterCount(t, meta.FuseLkFlock, 1)

		// 此时 fh1 调用 SetLkw 升级排他锁 F_WRLCK：
		// 首轮事务按非原子语义删除 fh1 的原共享锁（若检测到与 fh2 环路或唤醒 fh2，fh1 原共享锁均已释放并唤醒等待它的 fh2！）
		// 为同时验证“fh1 与持有共享锁的句柄冲突进入等待后被取消返回 EINTR，且原共享锁已释放唤醒其他等待者”：
		// 让 fh2 先获锁，随后测 fh1 与 fh3 共享锁冲突并取消
		_ = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 1001},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s1,
		})
		require.NoError(t, <-fh2UpgradeCh)

		// 释放 fh2，正式布置：fh1 与 fh2 均持有共享锁 F_RDLCK
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 2003},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s2,
		}))
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 1001},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s1,
		}))
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 2002},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s2,
		}))

		// fh1 调用 SetLkw 请求升级为排他锁 F_WRLCK（与 fh2 的共享锁冲突）
		cancelCtx, cancelFh1 := context.WithCancel(ctx)
		fh1ErrCh := make(chan error, 1)
		go func() {
			fh1ErrCh <- mgr.SetLkw(cancelCtx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh1,
				LockOwner: 101,
				Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 1002},
				LkFlags:   meta.FuseLkFlock,
				SessionID: s1,
				RequestID: 5001,
			})
		}()

		waitForWaiterCount(t, meta.FuseLkFlock, 1)

		// 验证首轮事务已持久化删除 fh1 的原共享锁（活跃 flock 锁仅剩 fh2 一条）
		activeFlocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: int32(meta.FuseLkFlock),
		})
		require.NoError(t, err)
		require.Len(t, activeFlocks, 1)
		assert.Equal(t, int64(fh2), activeFlocks[0].Fh)

		// 取消 fh1 的 SetLkw 请求，返回 ErrInterrupted (EINTR)
		cancelFh1()
		err = <-fh1ErrCh
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInterrupted))

		// fh1 原共享锁已按非原子语义释放，此时 fh2 可无冲突直接升级为排他锁 F_WRLCK
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 2003},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s2,
		}))

		// 清理 fh2 的 flock
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 2003},
			LkFlags:   meta.FuseLkFlock,
			SessionID: s2,
		}))

		// 验证全流程 SetLkw 操作严格不修改 Inode 的 atime / mtime / ctime
		afterInode, err := mgr.GetAttr(ctx, fileIno)
		require.NoError(t, err)
		assert.True(t, beforeInode.Attr.Atime.Equal(afterInode.Attr.Atime))
		assert.True(t, beforeInode.Attr.Mtime.Equal(afterInode.Attr.Mtime))
		assert.True(t, beforeInode.Attr.Ctime.Equal(afterInode.Attr.Ctime))
	})
}
