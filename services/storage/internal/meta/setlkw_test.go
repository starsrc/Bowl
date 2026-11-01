package meta_test

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

func TestSetLkw_InputValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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

		// 支持 ErrBadFileHandle 触发的 cleanExpiredSessionByID 异步事务清理
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				return fn(mockQuerier)
			}).
			Maybe()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Maybe()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Maybe()

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
			Maybe()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{
				SessionID: 1,
				CreatedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: time.Now().Add(time.Second), Valid: true},
			}, nil).
			Maybe()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Maybe()

		mockQuerier.EXPECT().
			UpsertFileLockWaiter(mock.Anything, mock.Anything).
			Return(db.FileLockWaiter{}, nil).
			Maybe()

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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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
			Maybe()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{
				SessionID: 2,
				CreatedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: time.Now().Add(time.Second), Valid: true},
			}, nil).
			Maybe()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Maybe()

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
			Maybe()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{
				SessionID: 3,
				CreatedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: time.Now().Add(time.Second), Valid: true},
			}, nil).
			Maybe()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Maybe()

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
	dsn := getStorageTestDSN()

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

// TestSetLkw_NotifyChanLeak_OnImmediateReturnOrError 验证 G-08：
// SetLkw 在无需阻塞直接返回或校验失败时未回收 lockNotifyChans[ino] 导致内存泄漏。
func TestSetLkw_NotifyChanLeak_OnImmediateReturnOrError(t *testing.T) {
	dsn := getStorageTestDSN()

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

	t.Run("校验失败返回 ErrNotFound 时未清理 lockNotifyChans 内存泄漏", func(t *testing.T) {
		nonExistentIno := uint64(987654321)
		err := mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       nonExistentIno,
			Fh:        1,
			LockOwner: 100,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   101,
			},
			SessionID: 1,
			RequestID: 1,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)

		assert.False(t, meta.HasLockNotifyChan(mgr, nonExistentIno),
			"G-08: 对不存在的 Inode 执行 SetLkw 校验失败退出时，lockNotifyChans 绝不应残留通知通道条目")
	})

	t.Run("无冲突直接获锁成功退出时未清理 lockNotifyChans 内存泄漏", func(t *testing.T) {
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-leak-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sess := uint64(555001)
		_, err = mgr.RenewSession(ctx, sess)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("direct_grant_leak.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		fh := createRes.Fh

		// 无冲突直接申请排他锁并成功
		err = mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fh,
			LockOwner: 200,
			Lk: meta.FileLock{
				Start: 0,
				End:   50,
				Type:  meta.F_WRLCK,
				PID:   201,
			},
			SessionID: sess,
			RequestID: 2,
		})
		require.NoError(t, err)

		assert.False(t, meta.HasLockNotifyChan(mgr, fileIno),
			"G-08: 无冲突直接获锁退出时，lockNotifyChans 绝不应残留通知通道条目")
	})
}

type cancelAfterTxStore struct {
	db.Store
	cancelOnGrant     func()
	hookActive        bool
	returnNilOnCancel bool
}

func (s *cancelAfterTxStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil && s.hookActive && s.cancelOnGrant != nil {
		s.cancelOnGrant()
		if s.returnNilOnCancel {
			return nil
		}
		return context.Canceled
	}
	return err
}

// TestSetLkw_ContextCanceledAfterGrantCommit_NoHangingLock 验证 G-04 与 G-06：
// SetLkw 授锁事务已在 DB 提交但返回前 ctx 取消，必须回滚/撤销已授予的锁，绝不残留悬挂锁。
// 涵盖两种模式：
// 1. returnNilOnCancel=false: 覆盖 txErr != nil 分支
// 2. returnNilOnCancel=true: 覆盖 txErr == nil && ctx.Err() != nil 分支
// 每种模式均测试 POSIX 记录锁和 BSD flock，并在每次调用后直接查询底层数据库 file_locks 与 file_lock_waiters 表断言记录数均为 0。
func TestSetLkw_ContextCanceledAfterGrantCommit_NoHangingLock(t *testing.T) {
	dsn := getStorageTestDSN()

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

	baseStore := db.NewStore(pool)
	hookStore := &cancelAfterTxStore{Store: baseStore}
	mgr := meta.New(hookStore)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-hanging-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	testCases := []struct {
		name              string
		returnNilOnCancel bool
		isFlock           bool
	}{
		{name: "txErr!=nil/POSIX", returnNilOnCancel: false, isFlock: false},
		{name: "txErr!=nil/BSD_flock", returnNilOnCancel: false, isFlock: true},
		{name: "txErr==nil_ctxErr!=nil/POSIX", returnNilOnCancel: true, isFlock: false},
		{name: "txErr==nil_ctxErr!=nil/BSD_flock", returnNilOnCancel: true, isFlock: true},
	}

	for idx, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sessA := uint64(666100 + idx*2)
			sessB := uint64(666100 + idx*2 + 1)
			_, err = mgr.RenewSession(ctx, sessA)
			require.NoError(t, err)
			_, err = mgr.RenewSession(ctx, sessB)
			require.NoError(t, err)

			createResA, err := mgr.Create(ctx, meta.CreateParams{
				ParentIno: disk.RootIno,
				Name:      []byte(fmt.Sprintf("hanging_%d_%v.bin", idx, tc.isFlock)),
				Mode:      meta.S_IFREG | 0o644,
				Flags:     meta.O_RDWR,
				SessionID: &sessA,
			})
			require.NoError(t, err)
			fileIno := createResA.Inode.Ino
			fhA := createResA.Fh

			openResB, err := mgr.Open(ctx, meta.OpenParams{
				Ino:       fileIno,
				Flags:     meta.O_RDWR,
				SessionID: &sessB,
			})
			require.NoError(t, err)
			fhB := openResB.Fh

			// 激活接缝：当 SetLkw 提交授锁事务后，立即取消 ctx
			callCtx, callCancel := context.WithCancel(ctx)
			hookStore.cancelOnGrant = callCancel
			hookStore.hookActive = true
			hookStore.returnNilOnCancel = tc.returnNilOnCancel

			var lkFlags uint32
			var lkA meta.FileLock
			if tc.isFlock {
				lkFlags = meta.FuseLkFlock
				lkA = meta.FileLock{
					Start: 0,
					End:   meta.OffsetMax,
					Type:  meta.F_WRLCK,
					PID:   301,
				}
			} else {
				lkFlags = 0
				lkA = meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   301,
				}
			}

			err = mgr.SetLkw(callCtx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: 300,
				Lk:        lkA,
				LkFlags:   lkFlags,
				SessionID: sessA,
				RequestID: 1,
			})
			require.ErrorIs(t, err, meta.ErrInterrupted, "在授锁提交与返回间取消必须向客户端返回 ErrInterrupted")

			// 禁用 hook，恢复正常执行
			hookStore.hookActive = false

			// 在每次调用后直接查询底层数据库 file_locks 与 file_lock_waiters 表断言锁记录数与等待记录数均为 0
			var lockCount, waiterCount int64
			err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(fileIno)).Scan(&lockCount)
			require.NoError(t, err)
			assert.Equal(t, int64(0), lockCount, "file_locks 记录数必须为 0")

			err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_lock_waiters WHERE ino = $1", int64(fileIno)).Scan(&waiterCount)
			require.NoError(t, err)
			assert.Equal(t, int64(0), waiterCount, "file_lock_waiters 记录数必须为 0")

			// 此时客户端 B 尝试获取同一区间的排他锁：验证无悬挂锁阻碍后续客户端
			var lkB meta.FileLock
			if tc.isFlock {
				lkB = meta.FileLock{
					Start: 0,
					End:   meta.OffsetMax,
					Type:  meta.F_WRLCK,
					PID:   401,
				}
			} else {
				lkB = meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   401,
				}
			}

			err = mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       fileIno,
				Fh:        fhB,
				LockOwner: 400,
				Lk:        lkB,
				LkFlags:   lkFlags,
				SessionID: sessB,
			})
			require.NoError(t, err, "G-04/G-06: SetLkw 因取消返回 ErrInterrupted 时，绝不能在 file_locks 残留悬挂锁阻碍后续客户端获锁")
		})
	}
}

// TestSetLkw_CancelAfterGrant_PreservesPreExistingOwnerLocks 验证 G-01：
// SetLkw 授锁提交后取消回滚时，必须精确保留或恢复调用方原有的 POSIX 记录锁，
// 绝不得直接对请求区间执行解锁而误删调用方在授锁前合法持有的重叠记录锁。
func TestSetLkw_CancelAfterGrant_PreservesPreExistingOwnerLocks(t *testing.T) {
	dsn := getStorageTestDSN()

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

	baseStore := db.NewStore(pool)
	hookStore := &cancelAfterTxStore{Store: baseStore}
	mgr := meta.New(hookStore)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-g01-lock-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sessA := uint64(777001)
	sessB := uint64(777002)
	_, err = mgr.RenewSession(ctx, sessA)
	require.NoError(t, err)
	_, err = mgr.RenewSession(ctx, sessB)
	require.NoError(t, err)

	createResA, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("cancel_preserve_lock.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	fileIno := createResA.Inode.Ino
	fhA := createResA.Fh

	openResB, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sessB,
	})
	require.NoError(t, err)
	fhB := openResB.Fh

	// 1. 同一 (session_id, ino, lock_owner) 先通过 SetLk 成功持有 [0, 99] 的 POSIX 读锁
	ownerA := uint64(501)
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: ownerA,
		Lk: meta.FileLock{
			Start: 0,
			End:   99,
			Type:  meta.F_RDLCK,
			PID:   501,
		},
		LkFlags:   0,
		SessionID: sessA,
	})
	require.NoError(t, err)

	// 验证持有锁状态：[0, 99] 读锁已在底表建立
	preLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)
	require.Len(t, preLocks, 1)
	require.Equal(t, int64(0), preLocks[0].StartOffset)
	require.Equal(t, int64(99), preLocks[0].EndOffset)
	require.Equal(t, int16(meta.F_RDLCK), preLocks[0].LockType)

	// 2. 模拟 SetLkw 请求将子区间 [50, 99] 升级为写锁；授锁事务提交成功后、返回前上下文取消
	callCtx, callCancel := context.WithCancel(ctx)
	hookStore.cancelOnGrant = callCancel
	hookStore.hookActive = true

	err = mgr.SetLkw(callCtx, meta.SetLkwParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: ownerA,
		Lk: meta.FileLock{
			Start: 50,
			End:   99,
			Type:  meta.F_WRLCK,
			PID:   501,
		},
		LkFlags:   0,
		SessionID: sessA,
		RequestID: 999,
	})
	require.ErrorIs(t, err, meta.ErrInterrupted, "在授锁提交与返回间取消必须向客户端返回 ErrInterrupted")

	// 禁用 hook
	hookStore.hookActive = false

	// 3. 断言底层 file_locks 表：该 owner 在 [0, 99] 上的原有读锁必须完整保留
	// 若存在 G-01 缺陷，回滚逻辑直接对请求区间 [50, 99] 执行 unlock，导致 [50, 99] 被误删，仅残留 [0, 49]！
	postLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)
	require.Len(t, postLocks, 1, "G-01: 回滚后原有读锁必须完整保留为 1 条记录，不能被抹除或截断")
	assert.Equal(t, int64(0), postLocks[0].StartOffset, "G-01: 原有读锁起始偏移应为 0")
	assert.Equal(t, int64(99), postLocks[0].EndOffset, "G-01: 原有读锁结束偏移应为 99，不能误删 [50, 99] 导致只剩 [0, 49]")
	assert.Equal(t, int16(meta.F_RDLCK), postLocks[0].LockType, "G-01: 恢复的锁类型应为 F_RDLCK")

	// 4. 冲突锁检测：客户端 B 尝试在 [50, 99] 上获取排他写锁，必须因冲突返回 ErrAgain
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhB,
		LockOwner: 602,
		Lk: meta.FileLock{
			Start: 50,
			End:   99,
			Type:  meta.F_WRLCK,
			PID:   602,
		},
		LkFlags:   0,
		SessionID: sessB,
	})
	assert.ErrorIs(t, err, meta.ErrAgain, "G-01: 客户端 A 的原有读锁 [50, 99] 应当有效存在，客户端 B 获取写锁必须冲突失败返回 ErrAgain")
}

// TestSetLkw_ReleaseClosingHandleCleansWaiter 验证 G-03：
// Release 关闭正在 SetLkw 排队的句柄后，file_lock_waiters 必须零残留，且后续客户端加锁不被阻塞。
func TestSetLkw_ReleaseClosingHandleCleansWaiter(t *testing.T) {
	dsn := getStorageTestDSN()

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

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-g03-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sessA := uint64(777001)
	sessB := uint64(777002)
	sessC := uint64(777003)
	_, err = mgr.RenewSession(ctx, sessA)
	require.NoError(t, err)
	_, err = mgr.RenewSession(ctx, sessB)
	require.NoError(t, err)
	_, err = mgr.RenewSession(ctx, sessC)
	require.NoError(t, err)

	createResA, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("zombie_waiter.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	fileIno := createResA.Inode.Ino
	fhA := createResA.Fh

	openResB, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sessB,
	})
	require.NoError(t, err)
	fhB := openResB.Fh

	openResC, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sessC,
	})
	require.NoError(t, err)
	fhC := openResC.Fh

	// 1. 客户端 A 获取排他锁
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: 501,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   501,
		},
		LkFlags:   0,
		SessionID: sessA,
	})
	require.NoError(t, err)

	// 2. 客户端 B 在后台发起阻塞式 SetLkw
	setlkwErrCh := make(chan error, 1)
	setlkwCtx, cancelSetLkw := context.WithTimeout(ctx, 3*time.Second)
	defer cancelSetLkw()

	go func() {
		setlkwErrCh <- mgr.SetLkw(setlkwCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhB,
			LockOwner: 502,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   502,
			},
			LkFlags:   0,
			SessionID: sessB,
			RequestID: 88,
		})
	}()

	// 等待客户端 B 的 waiter 写入数据库
	require.Eventually(t, func() bool {
		var cnt int64
		err := pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE ino = $1 AND session_id = $2", fileIno, sessB).Scan(&cnt)
		return err == nil && cnt > 0
	}, 2*time.Second, 10*time.Millisecond, "客户端 B 的 waiter 必须已写入数据库")

	// 3. 客户端 B 并发调用 Release 关闭正在等待加锁的句柄 fhB
	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino:       fileIno,
		Fh:        fhB,
		SessionID: &sessB,
	})
	require.NoError(t, err)

	// 等待 SetLkw 协程退出（因句柄关闭返回 ErrBadFileHandle）
	setlkwErr := <-setlkwErrCh
	require.ErrorIs(t, setlkwErr, meta.ErrBadFileHandle, "句柄被 Release 后 SetLkw 必须返回 ErrBadFileHandle")

	// 4. 断言：file_lock_waiters 中绝不能残留客户端 B 的僵尸记录
	var waiterCount int64
	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE ino = $1 AND session_id = $2", fileIno, sessB).Scan(&waiterCount)
	require.NoError(t, err)
	assert.Equal(t, int64(0), waiterCount, "G-03: Release 关闭等待句柄后，file_lock_waiters 表中绝不应残留僵尸记录")

	// 5. 客户端 A 释放锁
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: 501,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_UNLCK,
			PID:   501,
		},
		LkFlags:   0,
		SessionID: sessA,
	})
	require.NoError(t, err)

	// 6. 客户端 C 尝试获锁：如果残留了僵尸等待者，客户端 C 的加锁会被防饥饿机制永久拦截
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhC,
		LockOwner: 503,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   503,
		},
		LkFlags:   0,
		SessionID: sessC,
	})
	require.NoError(t, err, "G-03: 前序持锁者已解锁且等待句柄已关闭，后续客户端 SetLk 必须立即获锁成功")
}

func testRangesOverlap(s1, e1, s2, e2 uint64) bool {
	return s1 <= e2 && s2 <= e1
}

// TestSetLkw_Rollback_P1_Defects 针对缺陷 P-1 的两个确定性违规路径进行复现：
// 路径 A: Owner A 降级写锁为读锁，事务提交后外部 Owner B 获取并发读锁；此时 SetLkw 因取消触发 rollbackGrantedLock，
// 盲目将 Owner A 的旧写锁快照插回底表，导致排他写锁与共享读锁冲突并存，破坏 LOCK-INV-1。
// 路径 B: 同一 (sessionID, lockOwner) 在 SetLkw 授锁提交后、回滚前，并发新增不相交区间的合法锁并提交；
// rollbackGrantedLock 全量删除该 Owner 的锁并仅恢复旧快照，导致不相交区间的合法已提交锁被静默抹除。
func TestSetLkw_Rollback_P1_Defects(t *testing.T) {
	dsn := getStorageTestDSN()

	t.Run("PathA_DowngradeRollback_ViolatesLockInv1_ConflictWithConcurrentReadLock", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		baseStore := db.NewStore(pool)
		hookStore := &cancelAfterTxStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-p1-patha-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(888101)
		sessB := uint64(888102)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sessB)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("patha_lock_inv1.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh

		openResB, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessB,
		})
		require.NoError(t, err)
		fhB := openResB.Fh

		ownerA := uint64(1001)
		ownerB := uint64(1002)

		// 1. Owner A 预先持有 [0, 100] 的排他写锁 (F_WRLCK)
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   1001,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, err)

		// 2. Owner A 调用 SetLkw 将 [0, 100] 降级为读锁 (F_RDLCK)
		// 在降级事务成功提交后、SetLkw 返回前：
		// Owner B 并发成功获取 [0, 100] 读锁，随后取消 Owner A 的上下文触发 rollbackGrantedLock
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		hookStore.returnNilOnCancel = true
		hookStore.cancelOnGrant = func() {
			hookStore.hookActive = false
			// 此时降级已提交，Owner A 在 DB 中为 F_RDLCK，Owner B 获取 F_RDLCK 读锁成功
			bErr := mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       fileIno,
				Fh:        fhB,
				LockOwner: ownerB,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_RDLCK,
					PID:   1002,
				},
				LkFlags:   0,
				SessionID: sessB,
			})
			require.NoError(t, bErr, "降级事务提交后，Owner B 获取同区间读锁应成功")

			// 取消 Owner A 的上下文触发回滚
			callCancel()
		}
		hookStore.hookActive = true

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_RDLCK,
				PID:   1001,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 3001,
		})
		require.ErrorIs(t, err, meta.ErrInterrupted)

		// 3. 断言 LOCK-INV-1 不变量：底表中排他写锁与任何重叠区间的外部锁绝对不可并存
		activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)

		// 显式断言 Owner B 成功获取的读锁与 Owner A 回滚后保留的读锁仍完整存在于底表中，避免删空假绿
		var hasOwnerBLock, hasOwnerALock bool
		for _, l := range activeLocks {
			if l.SessionID == int64(sessB) && l.LockOwner == int64(ownerB) &&
				l.LockType == int16(meta.F_RDLCK) && l.StartOffset == 0 && l.EndOffset == 100 {
				hasOwnerBLock = true
			}
			if l.SessionID == int64(sessA) && l.LockOwner == int64(ownerA) &&
				l.LockType == int16(meta.F_RDLCK) && l.StartOffset == 0 && l.EndOffset == 100 {
				hasOwnerALock = true
			}
		}
		require.True(t, hasOwnerBLock, "Owner B 的 [0, 100] 读锁必须保留在底表中")
		require.True(t, hasOwnerALock, "Owner A 回滚后保留的 [0, 100] 读锁必须保留在底表中")

		var lockInv1Violations []string
		for i := 0; i < len(activeLocks); i++ {
			for j := i + 1; j < len(activeLocks); j++ {
				l1, l2 := activeLocks[i], activeLocks[j]
				if (l1.SessionID != l2.SessionID || l1.LockOwner != l2.LockOwner) &&
					testRangesOverlap(uint64(l1.StartOffset), uint64(l1.EndOffset), uint64(l2.StartOffset), uint64(l2.EndOffset)) {
					if l1.LockType == int16(meta.F_WRLCK) || l2.LockType == int16(meta.F_WRLCK) {
						lockInv1Violations = append(lockInv1Violations,
							fmt.Sprintf("lock1(session=%d,owner=%d,type=%d,[%d,%d]) conflicts with lock2(session=%d,owner=%d,type=%d,[%d,%d])",
								l1.SessionID, l1.LockOwner, l1.LockType, l1.StartOffset, l1.EndOffset,
								l2.SessionID, l2.LockOwner, l2.LockType, l2.StartOffset, l2.EndOffset))
					}
				}
			}
		}
		require.Empty(t, lockInv1Violations, "LOCK-INV-1 违规: rollbackGrantedLock 导致底表中排他写锁与共享读锁冲突并存:\n%s", strings.Join(lockInv1Violations, "\n"))
	})

	t.Run("PathB_RollbackErasesConcurrentDisjointLockOfSameOwner", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		baseStore := db.NewStore(pool)
		hookStore := &cancelAfterTxStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-p1-pathb-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(888201)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("pathb_disjoint_erase.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh

		ownerA := uint64(2001)

		// 1. Owner A 预先持有 [0, 100] 的读锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_RDLCK,
				PID:   2001,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, err)

		// 2. Owner A 调用 SetLkw 尝试对 [0, 100] 扩展为 [0, 150]
		// 在授锁事务提交后、SetLkw 返回前：
		// 同一 Owner A 并发申请不相交区间 [200, 300] 的锁并提交成功，随后取消 SetLkw 的上下文
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		hookStore.returnNilOnCancel = true
		hookStore.cancelOnGrant = func() {
			hookStore.hookActive = false
			// 同一 (sessA, ownerA) 在不相交区间 [200, 300] 获取写锁
			disjointErr := mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: ownerA,
				Lk: meta.FileLock{
					Start: 200,
					End:   300,
					Type:  meta.F_WRLCK,
					PID:   2001,
				},
				LkFlags:   0,
				SessionID: sessA,
			})
			require.NoError(t, disjointErr, "在不相交区间 [200, 300] 申请锁应成功")

			callCancel()
		}
		hookStore.hookActive = true

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   150,
				Type:  meta.F_RDLCK,
				PID:   2001,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 4001,
		})
		require.ErrorIs(t, err, meta.ErrInterrupted)

		// 3. 断言底层 file_locks 表：同一 Owner 并发新增的不相交区间 [200, 300] 锁必须保留，绝不能被抹除
		activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)

		var hasDisjointLock bool
		for _, l := range activeLocks {
			if l.SessionID == int64(sessA) && l.LockOwner == int64(ownerA) && l.StartOffset == 200 && l.EndOffset == 300 {
				hasDisjointLock = true
				break
			}
		}
		require.True(t, hasDisjointLock, "回滚绝不能抹除同一 Owner 在不相交区间 [200, 300] 并发新增的合法锁 (当前活跃锁: %+v)", activeLocks)
	})
}

// TestSetLkw_Rollback_P2_2_PartialUpgradePreservesDisjointConcurrentLock 复现缺陷 P2-2：
// Owner A 原持有 [0, 100] F_RDLCK，调用 SetLkw 将子区间 [40, 60] 升级为 F_WRLCK。
// 授锁事务提交后、SetLkw 返回前，Owner A 并发调用 SetLk 将未受影响的不相交子区间 [0, 10] 升级为 F_WRLCK。
// 随后 SetLkw 因上下文取消触发 rollbackGrantedLock 时，错误地以扩展后的 [0, 100] 范围全量删除并写回旧快照，
// 导致后提交的 [0, 10] F_WRLCK 排他写锁被静默抹除降级为读锁。
func TestSetLkw_Rollback_P2_2_PartialUpgradePreservesDisjointConcurrentLock(t *testing.T) {
	dsn := getStorageTestDSN()
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

	baseStore := db.NewStore(pool)
	hookStore := &cancelAfterTxStore{Store: baseStore}
	mgr := meta.New(hookStore)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-p2-2-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sessA := uint64(888301)
	sessB := uint64(888302)
	_, err = mgr.RenewSession(ctx, sessA)
	require.NoError(t, err)
	_, err = mgr.RenewSession(ctx, sessB)
	require.NoError(t, err)

	createResA, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("p2_2_partial_upgrade.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	fileIno := createResA.Inode.Ino
	fhA := createResA.Fh

	openResB, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sessB,
	})
	require.NoError(t, err)
	fhB := openResB.Fh

	ownerA := uint64(3001)
	ownerB := uint64(3002)

	// 1. Owner A 预先持有 [0, 100] 的读锁 (F_RDLCK)
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: ownerA,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_RDLCK,
			PID:   3001,
		},
		LkFlags:   0,
		SessionID: sessA,
	})
	require.NoError(t, err)

	// 2. Owner A 调用 SetLkw 请求将子区间 [40, 60] 升级为写锁 (F_WRLCK)。
	// 在授锁事务提交后、回滚前，Owner A 调用 SetLk 将不相交区间 [0, 10] 升级为 F_WRLCK 并取消 callCtx。
	callCtx, callCancel := context.WithCancel(ctx)
	defer callCancel()

	hookStore.returnNilOnCancel = true
	hookStore.cancelOnGrant = func() {
		hookStore.hookActive = false
		// 同一 Owner A 在与请求区间 [40, 60] 不相交的子区间 [0, 10] 升级为排他写锁并提交成功
		upgradeErr := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   10,
				Type:  meta.F_WRLCK,
				PID:   3001,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, upgradeErr, "在不相交区间 [0, 10] 升级为排他写锁应成功")

		callCancel()
	}
	hookStore.hookActive = true

	err = mgr.SetLkw(callCtx, meta.SetLkwParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: ownerA,
		Lk: meta.FileLock{
			Start: 40,
			End:   60,
			Type:  meta.F_WRLCK,
			PID:   3001,
		},
		LkFlags:   0,
		SessionID: sessA,
		RequestID: 6001,
	})
	require.ErrorIs(t, err, meta.ErrInterrupted)

	// 3. 断言底表中 Owner A 的锁状态：
	// 底表中 Owner A 依然持有 [0, 10] F_WRLCK 以及合并还原后的 [11, 100] F_RDLCK
	activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)

	var hasDisjointWriteLock bool
	var hasMergedReadLock bool
	for _, l := range activeLocks {
		if l.SessionID == int64(sessA) && l.LockOwner == int64(ownerA) {
			if l.StartOffset == 0 && l.EndOffset == 10 && l.LockType == int16(meta.F_WRLCK) {
				hasDisjointWriteLock = true
			}
			if l.StartOffset == 11 && l.EndOffset == 100 && l.LockType == int16(meta.F_RDLCK) {
				hasMergedReadLock = true
			}
		}
	}
	require.True(t, hasDisjointWriteLock, "回滚绝不能抹除同一 Owner 在请求区间外并发获取的排他写锁 [0, 10] F_WRLCK (当前活跃锁: %+v)", activeLocks)
	require.True(t, hasMergedReadLock, "回滚后应保持剩余区间的读锁 [11, 100] F_RDLCK (当前活跃锁: %+v)", activeLocks)

	// 并且外部 Owner B 尝试对 [0, 10] 获取 F_RDLCK 必须冲突失败 (ErrAgain)
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhB,
		LockOwner: ownerB,
		Lk: meta.FileLock{
			Start: 0,
			End:   10,
			Type:  meta.F_RDLCK,
			PID:   3002,
		},
		LkFlags:   0,
		SessionID: sessB,
	})
	require.ErrorIs(t, err, meta.ErrAgain, "Owner B 对 [0, 10] 加读锁必须被 Owner A 的 [0, 10] 排他写锁阻止返回 ErrAgain")
}

// TestSetLkw_CancelRollbackDoesNotReviveFlushedOrOverwriteNewerOwnerLock 验证 G1-1 缺陷场景：
// rollbackGrantedLock 回滚时：
// 1. 绝不能复活已被 Flush 清除的旧 POSIX 锁
// 2. 绝不能剪裁或抹除同一 Owner 在授锁后最新提交的新锁
func TestSetLkw_CancelRollbackDoesNotReviveFlushedOrOverwriteNewerOwnerLock(t *testing.T) {
	dsn := getStorageTestDSN()

	t.Run("Subtest1_FlushedLockNotRevived", func(t *testing.T) {
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

		baseStore := db.NewStore(pool)
		hookStore := &cancelAfterTxStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-flush-revive-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889101)
		sessB := uint64(889102)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sessB)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("flush_revive.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh

		openResB, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessB,
		})
		require.NoError(t, err)
		fhB := openResB.Fh

		ownerA := uint64(7001)
		ownerB := uint64(7002)

		// 1. Owner A 预先持有 [0, 100] 的共享读锁 (F_RDLCK)
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_RDLCK,
				PID:   7001,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, err)

		// 2. Owner A 调用 SetLkw 将 [0, 100] 升级为排他写锁 (F_WRLCK)
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		hookStore.returnNilOnCancel = true
		hookStore.cancelOnGrant = func() {
			hookStore.hookActive = false
			// 在 tryAcquireLock 升级事务提交后、rollbackGrantedLock 之前：
			// Owner A 并发调用 Flush 清除该 (session_id, ino, lock_owner) 的全部 POSIX 锁
			flushErr := mgr.Flush(ctx, meta.FlushParams{
				Ino:       fileIno,
				Fh:        fhA,
				SessionID: &sessA,
				LockOwner: &ownerA,
			})
			require.NoError(t, flushErr, "Flush 清除 POSIX 锁应当成功")

			// 取消 SetLkw 的上下文触发 rollbackGrantedLock
			callCancel()
		}
		hookStore.hookActive = true

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   7001,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 7101,
		})
		require.True(t, errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.Canceled),
			"SetLkw 在上下文取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err)

		// 3. 断言：随后冲突 Owner B 执行 GetLk 或 SetLk 时无锁冲突（证明旧读锁未被复活）
		// 首先使用 GetLk 检查：此时应该没有锁冲突（返回 F_UNLCK）
		getLkRes, err := mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       fileIno,
			Fh:        fhB,
			LockOwner: ownerB,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   7002,
			},
			LkFlags:   0,
			SessionID: sessB,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(meta.F_UNLCK), getLkRes.Type,
			"Owner A 的锁已被 Flush 清除且 SetLkw 已取消退出，GetLk 绝不应检测到已复活的旧读锁")

		// 其次使用 SetLk 申请排他写锁：应成功获锁，不应返回 ErrAgain 或 ErrLocked
		bErr := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhB,
			LockOwner: ownerB,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   7002,
			},
			LkFlags:   0,
			SessionID: sessB,
		})
		require.NoError(t, bErr, "Flush 清除锁后，Owner B 对 [0, 100] 加排他写锁应当成功，旧读锁绝不能被 rollbackGrantedLock 复活")
	})

	t.Run("Subtest2_NewerOwnerLockNotOverwritten", func(t *testing.T) {
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

		baseStore := db.NewStore(pool)
		hookStore := &cancelAfterTxStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-newer-lock-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889201)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("newer_lock.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh

		ownerA := uint64(8001)

		// 1. Owner A 预先持有 [0, 50] 的读锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   50,
				Type:  meta.F_RDLCK,
				PID:   8001,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, err)

		// 2. Owner A 调用 SetLkw 尝试申请 [0, 100] 的写锁
		// 在 tryAcquireLock 事务提交后、rollbackGrantedLock 之前：
		// 同一 Owner A 发起新的 SetLk，在重叠且扩展的区间 [0, 200] 设置新排他写锁，然后 cancel()
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		hookStore.returnNilOnCancel = true
		hookStore.cancelOnGrant = func() {
			hookStore.hookActive = false
			// 同一 Owner A 发起新的 SetLk 设置 [0, 200] 写锁
			newLkErr := mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: ownerA,
				Lk: meta.FileLock{
					Start: 0,
					End:   200,
					Type:  meta.F_WRLCK,
					PID:   8001,
				},
				LkFlags:   0,
				SessionID: sessA,
			})
			require.NoError(t, newLkErr, "同一 Owner 设置新的 [0, 200] 写锁应成功")

			callCancel()
		}
		hookStore.hookActive = true

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   8001,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 8101,
		})
		require.True(t, errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.Canceled),
			"SetLkw 在上下文取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err)

		// 3. 断言底层数据库：新 SetLk 设置的锁 [0, 200] F_WRLCK 依然完整保留在数据库中，未被 rollbackGrantedLock 抹除或篡改
		activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)

		var ownerLocks []db.FileLock
		for _, l := range activeLocks {
			if l.SessionID == int64(sessA) && l.LockOwner == int64(ownerA) {
				ownerLocks = append(ownerLocks, l)
			}
		}
		require.Len(t, ownerLocks, 1, "同一 Owner 最新设置的锁必须保留为单一完整锁段，不能被回滚切碎或覆盖 (当前活跃锁: %+v)", ownerLocks)
		assert.Equal(t, int64(0), ownerLocks[0].StartOffset, "起始偏移量应为 0")
		assert.Equal(t, int64(200), ownerLocks[0].EndOffset, "结束偏移量应为 200")
		assert.Equal(t, int16(meta.F_WRLCK), ownerLocks[0].LockType, "锁类型应为 F_WRLCK")
	})
}

var errSimulatedFlushTxRollback = errors.New("simulated error to trigger rollback")

type rollbackFlushHookStore struct {
	db.Store
	cancelOnGrant func()
	hookActive    bool
	failNextTx    bool
}

func (s *rollbackFlushHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	if s.failNextTx {
		return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
			if err := fn(q); err != nil {
				return err
			}
			return errSimulatedFlushTxRollback
		}, opts...)
	}

	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil && s.hookActive && s.cancelOnGrant != nil {
		s.cancelOnGrant()
		return nil
	}
	return err
}

type consecutiveCancelStore struct {
	db.Store
	onTxCommitted func(ctx context.Context) error
	hookActive    bool
}

func (s *consecutiveCancelStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil && s.hookActive && s.onTxCommitted != nil {
		return s.onTxCommitted(ctx)
	}
	return err
}

// TestSetLkw_CancelRollbackEdgeCases 验证取消回滚边缘场景下的缺陷（真红）：
// SubtestA: 局部外部读锁冲突时的区间拆分恢复
// SubtestB: Flock SetLkw(F_UNLCK) 解锁取消不误删后续新 Flock
// SubtestC: 无 SessionID 的 legacy 句柄 Flush 不误失效带 SessionID 的在途 SetLkw
// SubtestD: 后续 Flush 或 SetLk 事务回滚时不污染 inFlightGrants 内存标记
// SubtestE: 连续两个同 Owner 重叠 SetLkw 均取消时不复活前驱未确认锁
func TestSetLkw_CancelRollbackEdgeCases(t *testing.T) {
	dsn := getStorageTestDSN()

	t.Run("SubtestA_PartialExternalReadLockConflict_SegmentSplittingRestoration", func(t *testing.T) {
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

		baseStore := db.NewStore(pool)
		hookStore := &cancelAfterTxStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-edge-a-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889301)
		sessB := uint64(889302)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sessB)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("edge_a.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh

		openResB, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessB,
		})
		require.NoError(t, err)
		fhB := openResB.Fh

		ownerA := uint64(9001)
		ownerB := uint64(9002)

		// 1. Owner A 预先持有 [0, 100] 的排他写锁 (F_WRLCK)
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9001,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, err)

		// 2. Owner A 调用 SetLkw 将 [0, 100] 降级为读锁 (F_RDLCK)
		// 在降级事务提交后、rollbackGrantedLock 之前：
		// 外部 Owner B 并发对 [0, 10] 加共享读锁 (F_RDLCK)，随后取消 Owner A 的 SetLkw
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		hookStore.returnNilOnCancel = true
		hookStore.cancelOnGrant = func() {
			hookStore.hookActive = false
			// 外部 Owner B 获取 [0, 10] 读锁
			bErr := mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       fileIno,
				Fh:        fhB,
				LockOwner: ownerB,
				Lk: meta.FileLock{
					Start: 0,
					End:   10,
					Type:  meta.F_RDLCK,
					PID:   9002,
				},
				LkFlags:   0,
				SessionID: sessB,
			})
			require.NoError(t, bErr, "Owner B 获取 [0, 10] 读锁应成功")

			callCancel()
		}
		hookStore.hookActive = true

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_RDLCK,
				PID:   9001,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 9101,
		})
		require.True(t, errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.Canceled),
			"SetLkw 在上下文取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err)

		// 3. 断言底层数据库：
		// 重叠冲突的 [0, 10] 保留读锁，而无外部冲突的 [11, 100] 必须恢复为排他写锁 F_WRLCK
		activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)

		var ownerALocks []db.FileLock
		for _, l := range activeLocks {
			if l.SessionID == int64(sessA) && l.LockOwner == int64(ownerA) {
				ownerALocks = append(ownerALocks, l)
			}
		}

		var hasConflictReadLock bool
		var hasRestoredWriteLock bool
		for _, l := range ownerALocks {
			if l.StartOffset == 0 && l.EndOffset == 10 && l.LockType == int16(meta.F_RDLCK) {
				hasConflictReadLock = true
			}
			if l.StartOffset == 11 && l.EndOffset == 100 && l.LockType == int16(meta.F_WRLCK) {
				hasRestoredWriteLock = true
			}
		}
		assert.True(t, hasConflictReadLock, "与外部读锁重叠的 [0, 10] 应保留为 F_RDLCK, 当前 Owner A 锁: %+v", ownerALocks)
		assert.True(t, hasRestoredWriteLock, "无外部冲突的 [11, 100] 必须恢复为排他写锁 F_WRLCK, 当前 Owner A 锁: %+v", ownerALocks)
	})

	t.Run("SubtestB_FlockUnlockCancelDoesNotDeleteNewFlock", func(t *testing.T) {
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

		baseStore := db.NewStore(pool)
		hookStore := &cancelAfterTxStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-edge-b-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889401)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("edge_b.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh
		ownerA := uint64(9011)

		// 1. 句柄 fhA 预先持有 Flock 写锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   9011,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sessA,
		})
		require.NoError(t, err)

		// 2. 调用 SetLkw(Flock, F_UNLCK) 解锁
		// 在解锁事务提交后、rollbackGrantedLock 之前：
		// 同一 (session_id, fh) 调用 SetLk(Flock, F_WRLCK) 重新获得新 Flock 写锁，随后 cancel()
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		hookStore.returnNilOnCancel = true
		hookStore.cancelOnGrant = func() {
			hookStore.hookActive = false
			// 重新加 Flock 写锁
			newLockErr := mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: ownerA,
				Lk: meta.FileLock{
					Start: 0,
					End:   meta.OffsetMax,
					Type:  meta.F_WRLCK,
					PID:   9011,
				},
				LkFlags:   meta.FuseLkFlock,
				SessionID: sessA,
			})
			require.NoError(t, newLockErr, "同一句柄重新获取 Flock 写锁应成功")

			callCancel()
		}
		hookStore.hookActive = true

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_UNLCK,
				PID:   9011,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sessA,
			RequestID: 9111,
		})
		require.True(t, errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.Canceled),
			"SetLkw 在上下文取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err)

		// 3. 断言底层数据库：新获得的 Flock 写锁依然存在，绝不能被 rollbackGrantedLock 误删
		activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: int32(meta.FuseLkFlock),
		})
		require.NoError(t, err)

		var flockLocks []db.FileLock
		for _, l := range activeLocks {
			if l.SessionID == int64(sessA) && l.Fh == int64(fhA) {
				flockLocks = append(flockLocks, l)
			}
		}
		require.Len(t, flockLocks, 1, "同一 (session_id, fh) 新获取的 Flock 写锁必须保留在数据库中 (当前活跃锁: %+v)", flockLocks)
		assert.Equal(t, int16(meta.F_WRLCK), flockLocks[0].LockType, "锁类型应为 F_WRLCK")
	})

	t.Run("SubtestC_LegacyFlushWithoutSessionDoesNotInvalidateSessionInFlightGrant", func(t *testing.T) {
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

		baseStore := db.NewStore(pool)
		hookStore := &cancelAfterTxStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-edge-c-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889501)
		sessB := uint64(889502)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sessB)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("edge_c.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh

		openResB, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessB,
		})
		require.NoError(t, err)
		fhB := openResB.Fh

		// 打开一个 SessionID 为 nil 的 legacy 句柄
		openResLegacy, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: nil,
		})
		require.NoError(t, err)
		fhLegacy := openResLegacy.Fh

		ownerA := uint64(9021)
		ownerB := uint64(9022)

		// 1. 发起 SetLkw([0, 100], F_WRLCK, LockOwner: ownerA, SessionID: sessA)
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		hookStore.returnNilOnCancel = true
		hookStore.cancelOnGrant = func() {
			hookStore.hookActive = false
			// 在 tryAcquireLock 提交后的钩子窗口内，调用 legacy Flush (SessionID: nil)
			flushErr := mgr.Flush(ctx, meta.FlushParams{
				Ino:       fileIno,
				Fh:        fhLegacy,
				SessionID: nil,
				LockOwner: &ownerA,
			})
			require.NoError(t, flushErr, "Legacy Flush 应成功执行")

			callCancel()
		}
		hookStore.hookActive = true

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9021,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 9121,
		})
		require.True(t, errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.Canceled),
			"SetLkw 在上下文取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err)

		// 2. 断言：带 SessionID 的 SetLkw 取消时必须正常回滚清除 [0, 100] 写锁，外部 Owner B 可成功对 [0, 100] 加排他写锁
		bErr := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhB,
			LockOwner: ownerB,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9022,
			},
			LkFlags:   0,
			SessionID: sessB,
		})
		require.NoError(t, bErr, "SetLkw 回滚后，外部 Owner B 对 [0, 100] 加排他写锁应当成功，不能因 legacy Flush 误失效内存标记导致残留悬挂写锁")
	})

	t.Run("SubtestD_RolledBackFlushOrSetLkDoesNotPolluteInFlightGrant", func(t *testing.T) {
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

		baseStore := db.NewStore(pool)
		hookStore := &rollbackFlushHookStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-edge-d-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889601)
		sessB := uint64(889602)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sessB)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("edge_d.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh

		openResB, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessB,
		})
		require.NoError(t, err)
		fhB := openResB.Fh

		ownerA := uint64(9031)
		ownerB := uint64(9032)

		// 1. 发起 SetLkw([0, 100], F_WRLCK, LockOwner: ownerA)
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		hookStore.cancelOnGrant = func() {
			hookStore.hookActive = false
			// 在 tryAcquireLock 提交后，同一 Owner 发起一次 Flush，但通过 Store 注入错误令其回滚
			hookStore.failNextTx = true
			flushErr := mgr.Flush(ctx, meta.FlushParams{
				Ino:       fileIno,
				Fh:        fhA,
				SessionID: &sessA,
				LockOwner: &ownerA,
			})
			require.ErrorIs(t, flushErr, errSimulatedFlushTxRollback, "Flush 事务应因模拟错误回滚")
			hookStore.failNextTx = false

			callCancel()
		}
		hookStore.hookActive = true

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9031,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 9131,
		})
		require.True(t, errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.Canceled),
			"SetLkw 在上下文取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err)

		// 2. 断言：由于 Flush 事务已回滚，原 SetLkw 取消时必须正常回滚清除 [0, 100] 写锁，绝不能因内存标记被污染而跳过回滚留悬挂写锁
		bErr := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhB,
			LockOwner: ownerB,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9032,
			},
			LkFlags:   0,
			SessionID: sessB,
		})
		require.NoError(t, bErr, "SetLkw 取消后，外部 Owner B 对 [0, 100] 加排他写锁应当成功，不能遗留悬挂写锁")
	})

	t.Run("SubtestE_ConsecutiveCancelsDoNotRevivePriorUnconfirmedLock", func(t *testing.T) {
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

		baseStore := db.NewStore(pool)

		aCommitted := make(chan struct{})
		bCommitted := make(chan struct{})
		unblockA := make(chan struct{})
		unblockB := make(chan struct{})
		releaseA := sync.OnceFunc(func() { close(unblockA) })
		releaseB := sync.OnceFunc(func() { close(unblockB) })
		t.Cleanup(releaseA)
		t.Cleanup(releaseB)

		var grantCount atomic.Int32
		hookStore := &consecutiveCancelStore{
			Store: baseStore,
			onTxCommitted: func(opCtx context.Context) error {
				idx := grantCount.Add(1)
				if idx == 1 {
					close(aCommitted)
					select {
					case <-unblockA:
					case <-opCtx.Done():
						return opCtx.Err()
					}
				} else if idx == 2 {
					close(bCommitted)
					select {
					case <-unblockB:
					case <-opCtx.Done():
						return opCtx.Err()
					}
				}
				return nil
			},
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-edge-e-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889701)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("edge_e.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh
		ownerA := uint64(9041)

		// 1. 同一 (SessionID, LockOwner) 初始仅持有 [0, 50] 读锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   50,
				Type:  meta.F_RDLCK,
				PID:   9041,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, err)

		// 2. 先后发起请求 A ([0, 100] 写锁) 与请求 B ([0, 200] 写锁)
		hookStore.hookActive = true

		ctxA, cancelA := context.WithCancel(ctx)
		defer cancelA()
		ctxB, cancelB := context.WithCancel(ctx)
		defer cancelB()

		var errA, errB error
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			errA = mgr.SetLkw(ctxA, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: ownerA,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   9041,
				},
				LkFlags:   0,
				SessionID: sessA,
				RequestID: 9141,
			})
		}()

		// 等待请求 A 的 tryAcquireLock 提交并挂在未确认窗口
		select {
		case <-aCommitted:
		case <-ctx.Done():
			t.Fatal("等待请求 A 事务提交超时")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			errB = mgr.SetLkw(ctxB, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: ownerA,
				Lk: meta.FileLock{
					Start: 0,
					End:   200,
					Type:  meta.F_WRLCK,
					PID:   9041,
				},
				LkFlags:   0,
				SessionID: sessA,
				RequestID: 9142,
			})
		}()

		// 等待请求 B 的 tryAcquireLock 提交并挂在未确认窗口
		select {
		case <-bCommitted:
		case <-ctx.Done():
			t.Fatal("等待请求 B 事务提交超时")
		}

		// 3. 此时请求 A 与请求 B 均已完成写事务提交，但都未确认退出；
		// 依次取消请求 B 和请求 A
		hookStore.hookActive = false

		cancelB()
		releaseB()

		cancelA()
		releaseA()

		wg.Wait()

		require.True(t, errors.Is(errA, meta.ErrInterrupted) || errors.Is(errA, context.Canceled),
			"请求 A 应返回取消中断错误，实际: %v", errA)
		require.True(t, errors.Is(errB, meta.ErrInterrupted) || errors.Is(errB, context.Canceled),
			"请求 B 应返回取消中断错误，实际: %v", errB)

		// 4. 断言底层数据库：两个未确认请求均取消后，最终状态必须仅保留初始的 [0, 50] 读锁，绝不能残留写锁
		activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)

		var ownerALocks []db.FileLock
		for _, l := range activeLocks {
			if l.SessionID == int64(sessA) && l.LockOwner == int64(ownerA) {
				ownerALocks = append(ownerALocks, l)
			}
		}
		require.Len(t, ownerALocks, 1, "回滚后必须仅保留初始读锁，当前活跃锁: %+v", ownerALocks)
		assert.Equal(t, int64(0), ownerALocks[0].StartOffset, "起始偏移量应为 0")
		assert.Equal(t, int64(50), ownerALocks[0].EndOffset, "结束偏移量应为 50")
		assert.Equal(t, int16(meta.F_RDLCK), ownerALocks[0].LockType, "锁类型应为 F_RDLCK")
	})
}

var errSimulatedBTxRollback = errors.New("simulated B tx failure to trigger restoreInFlightGrants")

type p1SnapshotWipeHookStore struct {
	db.Store
	bWaitBeforeTx chan struct{}
	bResumeTx     chan struct{}
	bFailTx       atomic.Bool
	onACommitted  func()
}

func (s *p1SnapshotWipeHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	role, _ := ctx.Value("p1Role").(string)
	if role == "B" {
		select {
		case s.bWaitBeforeTx <- struct{}{}:
		default:
		}
		select {
		case <-s.bResumeTx:
		case <-ctx.Done():
			return ctx.Err()
		}
		if s.bFailTx.Load() {
			return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
				if err := fn(q); err != nil {
					return err
				}
				return errSimulatedBTxRollback
			}, opts...)
		}
		return s.Store.ExecTxWithOptions(ctx, fn, opts...)
	}

	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil && role == "A" && s.onACommitted != nil {
		s.onACommitted()
	}
	return err
}

// TestSetLkw_ConcurrentSnapshotRestore_WipesInFlightGrant_DanglingLockLeak
// 复现缺陷 P-1：SetLk/SetLkw/Flush/Release 在行锁外按整个 Inode 恢复旧 inFlightGrants 快照，
// 抹除并发 SetLkw 的授锁撤销记录导致悬挂锁泄漏。
func TestSetLkw_ConcurrentSnapshotRestore_WipesInFlightGrant_DanglingLockLeak(t *testing.T) {
	dsn := getStorageTestDSN()
	parentCtx, parentCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer parentCancel()

	pool, err := pgxpool.New(parentCtx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(parentCtx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	baseStore := db.NewStore(pool)
	hookStore := &p1SnapshotWipeHookStore{
		Store:         baseStore,
		bWaitBeforeTx: make(chan struct{}, 1),
		bResumeTx:     make(chan struct{}),
	}
	mgr := meta.New(hookStore)

	disk, err := mgr.CreateDisk(parentCtx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-p1-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sessA := uint64(889701)
	sessB := uint64(889702)
	_, err = mgr.RenewSession(parentCtx, sessA)
	require.NoError(t, err)
	_, err = mgr.RenewSession(parentCtx, sessB)
	require.NoError(t, err)

	createResA, err := mgr.Create(parentCtx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("p1_leak.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	fileIno := createResA.Inode.Ino
	fhA := createResA.Fh

	openResB, err := mgr.Open(parentCtx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sessB,
	})
	require.NoError(t, err)
	fhB := openResB.Fh

	ownerA := uint64(9101)
	ownerB := uint64(9102)

	// 1. 并发请求 B：在请求 A 授锁前拍摄空快照并进入 ExecTxWithOptions 等待
	ctxB := context.WithValue(parentCtx, "p1Role", "B")
	bDone := make(chan error, 1)
	go func() {
		bDone <- mgr.Flush(ctxB, meta.FlushParams{
			Ino:       fileIno,
			Fh:        fhB,
			SessionID: &sessB,
			LockOwner: &ownerB,
		})
	}()

	// 等待 B 拍完快照 snap 并停留在 ExecTxWithOptions 入口
	select {
	case <-hookStore.bWaitBeforeTx:
	case <-parentCtx.Done():
		t.Fatal("等待请求 B 拍摄快照并进入事务超时")
	}

	// 2. 请求 A 发起 SetLkw([0, 100], F_WRLCK)
	callCtx, callCancel := context.WithCancel(parentCtx)
	defer callCancel()
	ctxA := context.WithValue(callCtx, "p1Role", "A")

	hookStore.onACommitted = func() {
		hookStore.onACommitted = nil
		// 请求 A 已经完成授锁并在 m.inFlightGrants 登记了 grantA，事务已提交。
		// 此时让请求 B 恢复运行并失败，触发其 defer 中的 restoreInFlightGrants(ino, snap)。
		hookStore.bFailTx.Store(true)
		close(hookStore.bResumeTx)

		select {
		case bErr := <-bDone:
			require.ErrorIs(t, bErr, errSimulatedBTxRollback, "请求 B 应因模拟错误回滚")
		case <-parentCtx.Done():
			t.Fatal("等待请求 B 事务失败并恢复旧快照超时")
		}

		// 请求 B 的旧快照已将 m.inFlightGrants 中 A 的登记项抹除。
		// 此时取消请求 A 的上下文，触发 rollbackGrantedLock。
		callCancel()
	}

	err = mgr.SetLkw(ctxA, meta.SetLkwParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: ownerA,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   9101,
		},
		LkFlags:   0,
		SessionID: sessA,
		RequestID: 91011,
	})
	require.True(t, errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.Canceled),
		"SetLkw 取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err)

	// 3. 断言底层数据库：请求 A 的 SetLkw 取消回滚后，已授予的锁必须被彻底清理，不能在底表残留悬挂锁
	var lockCount int
	countErr := pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(fileIno)).Scan(&lockCount)
	require.NoError(t, countErr)
	assert.Equal(t, 0, lockCount,
		"P-1 缺陷验证：SetLkw 取消回滚后，file_locks 底表绝不能残留悬挂写锁（当前因旧快照抹除 inFlightGrant 导致跳过回滚）")

	// 4. 断言业务语义：外部冲突 Owner B 加排他写锁应当成功，不能因残留悬挂锁冲突失败
	bErr := mgr.SetLk(parentCtx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhB,
		LockOwner: ownerB,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   9102,
		},
		LkFlags:   0,
		SessionID: sessB,
	})
	require.NoError(t, bErr, "P-1 缺陷验证：SetLkw 取消回滚后，外部冲突 Owner B 加排他写锁应当成功，不能遭遇残留悬挂锁冲突")
}

var errSimulatedSetLkwTxRollback = errors.New("simulated SetLkw tx rollback")

type setLkwRollbackHookStore struct {
	db.Store
	aCommittedCh chan struct{}
	aResumeCh    chan struct{}
	bFailTx      atomic.Bool
}

func (s *setLkwRollbackHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	role, _ := ctx.Value("setLkwRollbackRole").(string)
	if role == "B" && s.bFailTx.Load() {
		return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
			if err := fn(q); err != nil {
				return err
			}
			return errSimulatedSetLkwTxRollback
		}, opts...)
	}

	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil && role == "A" {
		select {
		case s.aCommittedCh <- struct{}{}:
		default:
		}
		select {
		case <-s.aResumeCh:
		case <-ctx.Done():
		}
	}
	return err
}

func TestSetLkw_SameOwnerRegisterInFlightGrant_TxRollbackRestoresPredecessorRanges(t *testing.T) {
	dsn := getStorageTestDSN()
	parentCtx, parentCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer parentCancel()

	pool, err := pgxpool.New(parentCtx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(parentCtx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	baseStore := db.NewStore(pool)
	hookStore := &setLkwRollbackHookStore{
		Store:        baseStore,
		aCommittedCh: make(chan struct{}, 1),
		aResumeCh:    make(chan struct{}),
	}
	mgr := meta.New(hookStore)

	disk, err := mgr.CreateDisk(parentCtx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-undo-test-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sessA := uint64(889751)
	sessOther := uint64(889752)
	_, err = mgr.RenewSession(parentCtx, sessA)
	require.NoError(t, err)
	_, err = mgr.RenewSession(parentCtx, sessOther)
	require.NoError(t, err)

	createResA, err := mgr.Create(parentCtx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("undo_predecessor.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	fileIno := createResA.Inode.Ino
	fhA := createResA.Fh
	ownerA := uint64(9151)

	openResOther, err := mgr.Open(parentCtx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sessOther,
	})
	require.NoError(t, err)
	fhOther := openResOther.Fh
	ownerOther := uint64(9152)

	// 1. 请求 A 发起 SetLkw([0, 100], F_WRLCK)，授锁事务提交成功后停在返回前
	aCtx, aCancel := context.WithCancel(parentCtx)
	defer aCancel()
	ctxA := context.WithValue(aCtx, "setLkwRollbackRole", "A")

	aDoneCh := make(chan error, 1)
	go func() {
		aDoneCh <- mgr.SetLkw(ctxA, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9151,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 91511,
		})
	}()

	select {
	case <-hookStore.aCommittedCh:
	case <-parentCtx.Done():
		t.Fatal("等待请求 A 授锁事务提交超时")
	}

	// 2. 同 Owner 发起重叠请求 B ([0, 200], F_WRLCK)：
	// 进入事务后执行 registerInFlightGrant (登记 attemptRegID 并剪裁前驱 A 的 pendingRanges 为空)，
	// 随后 hook 注入错误使得事务回滚 (txErr != nil)。
	hookStore.bFailTx.Store(true)

	bCtx, bCancel := context.WithCancel(parentCtx)
	defer bCancel()
	ctxB := context.WithValue(bCtx, "setLkwRollbackRole", "B")

	bErr := mgr.SetLkw(ctxB, meta.SetLkwParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: ownerA,
		Lk: meta.FileLock{
			Start: 0,
			End:   200,
			Type:  meta.F_WRLCK,
			PID:   9151,
		},
		LkFlags:   0,
		SessionID: sessA,
		RequestID: 91512,
	})
	require.True(t, errors.Is(bErr, meta.ErrInterrupted) || errors.Is(bErr, context.Canceled),
		"SetLkw 自身事务回滚后应返回 ErrInterrupted 或 context.Canceled, actual: %v", bErr)

	// 验证底表此时仅存前驱 A 的初始写锁，新请求 B 的锁未被持久化
	var midLockCount int
	err = pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(fileIno)).Scan(&midLockCount)
	require.NoError(t, err)
	require.Equal(t, 1, midLockCount, "请求 B 回滚后底表应仅保留请求 A 的 [0, 100] 锁")

	// 3. 此时新 attemptRegID 已注销，且前驱 A 的 pendingRanges 已恢复为 [0, 100]。
	// 取消前驱请求 A 的上下文并唤醒放行，A 触发 rollbackGrantedLock：
	// 若 pendingRanges 恢复成功，则 A 能完整清理底表中的 [0, 100] 锁；若未恢复（为空），A 将跳过清理导致残留悬挂锁。
	aCancel()
	close(hookStore.aResumeCh)

	select {
	case aErr := <-aDoneCh:
		require.True(t, errors.Is(aErr, meta.ErrInterrupted) || errors.Is(aErr, context.Canceled),
			"前驱请求 A 取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", aErr)
	case <-parentCtx.Done():
		t.Fatal("等待前驱请求 A 取消返回超时")
	}

	// 4. 断言底层数据库：请求 A 取消后，底表中前驱持有的锁被完整清理，残留锁数量必须为 0
	var finalLockCount int
	err = pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(fileIno)).Scan(&finalLockCount)
	require.NoError(t, err)
	assert.Equal(t, 0, finalLockCount, "前驱 SetLkw 取消后，底表 file_locks 必须被完整清理，残留锁数应为 0")

	// 5. 断言业务语义：外部冲突 Owner 加排他锁应当成功，不能遭遇残留悬挂锁冲突
	otherErr := mgr.SetLk(parentCtx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhOther,
		LockOwner: ownerOther,
		Lk: meta.FileLock{
			Start: 0,
			End:   200,
			Type:  meta.F_WRLCK,
			PID:   9152,
		},
		LkFlags:   0,
		SessionID: sessOther,
	})
	require.NoError(t, otherErr, "外部 Owner 尝试对同一区间加排他锁必须成功，证明前驱锁已彻底释放且未被幽灵锁定")
}

type p2ExpiryHookStore struct {
	db.Store
	setLkwBeforeReturnHook func()
}

func (s *p2ExpiryHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil && s.setLkwBeforeReturnHook != nil {
		s.setLkwBeforeReturnHook()
	}
	return err
}

// TestSetLkw_ExpiredSessionCancel_LeavesDirtyFileLock
// 复现缺陷 P-2 及孤儿回收与配额退还覆盖：
// 场景 ① (rollbackGrantedLock)：获锁后返回前租约过期并取消上下文，物理删除非空孤儿并精确退还磁盘配额
// 场景 ② (cleanupCancelledWaiter)：PENDING 等待期间租约过期并取消等待，清理等待队列、物理回收孤儿并退还配额
func TestSetLkw_ExpiredSessionCancel_LeavesDirtyFileLock(t *testing.T) {
	dsn := getStorageTestDSN()

	t.Run("Scenario1_RollbackGrantedLock_ExpiredOrphanReclaimAndQuotaRefund", func(t *testing.T) {
		parentCtx, parentCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer parentCancel()

		pool, err := pgxpool.New(parentCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(parentCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		baseStore := db.NewStore(pool)
		hookStore := &p2ExpiryHookStore{Store: baseStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(parentCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-p2-s1-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889811)
		_, err = mgr.RenewSession(parentCtx, sessA)
		require.NoError(t, err)

		// 1. 创建文件并打开句柄
		createResA, err := mgr.Create(parentCtx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("p2_orphan_s1.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		orphanIno := createResA.Inode.Ino
		orphanFh := createResA.Fh
		ownerA := uint64(9211)

		// 2. 通过 SetAttr 设置非零大小 (Size = 4096)
		newSize := uint64(4096)
		_, err = mgr.SetAttr(parentCtx, orphanIno, meta.SetAttrInput{
			Size: &newSize,
		})
		require.NoError(t, err)

		// 3. 调用 Unlink 使其成为 nlink == 0 的孤儿文件
		err = mgr.Unlink(parentCtx, disk.RootIno, []byte("p2_orphan_s1.bin"))
		require.NoError(t, err)

		// 验证当前处于孤儿状态：因句柄仍打开，used_inodes == 2, used_bytes == 4096
		preDisk, err := mgr.GetDisk(parentCtx, disk.DiskID)
		require.NoError(t, err)
		require.Equal(t, uint64(2), preDisk.UsedInodes)
		require.Equal(t, uint64(4096), preDisk.UsedBytes)

		callCtx, callCancel := context.WithCancel(parentCtx)
		defer callCancel()

		hookStore.setLkwBeforeReturnHook = func() {
			hookStore.setLkwBeforeReturnHook = nil
			// 在 setLkwBeforeReturnHook 中使会话租约过期并取消 ctx
			_, updateErr := pool.Exec(parentCtx, "UPDATE client_sessions SET expires_at = NOW() - INTERVAL '1 hour' WHERE session_id = $1", int64(sessA))
			require.NoError(t, updateErr)

			callCancel()
		}

		err = mgr.SetLkw(callCtx, meta.SetLkwParams{
			Ino:       orphanIno,
			Fh:        orphanFh,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9211,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 92111,
		})
		require.True(t, errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.Canceled),
			"SetLkw 在上下文取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err)

		// 4. 断言底表 file_locks 记录数为 0
		var lockCount int
		err = pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(orphanIno)).Scan(&lockCount)
		require.NoError(t, err)
		assert.Equal(t, 0, lockCount, "回滚后 file_locks 底表绝不能残留脏记录")

		// 5. 断言该孤儿 inodes 记录已被物理删除 (记录数为 0)
		var inodeCount int
		err = pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM inodes WHERE ino = $1", int64(orphanIno)).Scan(&inodeCount)
		require.NoError(t, err)
		assert.Equal(t, 0, inodeCount, "孤儿文件在关联过期会话清理后必须被物理删除")

		// 6. 断言所属磁盘配额 used_inodes 与 used_bytes 已准确退还
		postDisk, err := mgr.GetDisk(parentCtx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), postDisk.UsedInodes, "所属磁盘配额 used_inodes 必须恢复为 1 (仅剩根目录)")
		assert.Equal(t, uint64(0), postDisk.UsedBytes, "所属磁盘配额 used_bytes 必须精确退还为 0")
	})

	t.Run("Scenario2_CleanupCancelledWaiter_PendingExpiryCancellationAndOrphanReclaim", func(t *testing.T) {
		parentCtx, parentCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer parentCancel()

		pool, err := pgxpool.New(parentCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(parentCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		baseStore := db.NewStore(pool)
		mgr := meta.New(baseStore)

		disk, err := mgr.CreateDisk(parentCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-p2-s2-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sess1 := uint64(889821)
		sess2 := uint64(889822)
		_, err = mgr.RenewSession(parentCtx, sess1)
		require.NoError(t, err)
		_, err = mgr.RenewSession(parentCtx, sess2)
		require.NoError(t, err)

		// 1. 创建目标文件 target.bin，会话 1 持有写锁阻塞该文件
		createTarget, err := mgr.Create(parentCtx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("target.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		targetIno := createTarget.Inode.Ino
		targetFh1 := createTarget.Fh
		owner1 := uint64(9221)

		err = mgr.SetLk(parentCtx, meta.SetLkParams{
			Ino:       targetIno,
			Fh:        targetFh1,
			LockOwner: owner1,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9221,
			},
			LkFlags:   0,
			SessionID: sess1,
		})
		require.NoError(t, err)

		// 2. 会话 2 创建并打开另一个孤儿文件 orphan2.bin (Size = 4096, Unlink)
		createOrphan2, err := mgr.Create(parentCtx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan2.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		orphan2Ino := createOrphan2.Inode.Ino
		_ = createOrphan2.Fh

		newSize := uint64(4096)
		_, err = mgr.SetAttr(parentCtx, orphan2Ino, meta.SetAttrInput{
			Size: &newSize,
		})
		require.NoError(t, err)

		err = mgr.Unlink(parentCtx, disk.RootIno, []byte("orphan2.bin"))
		require.NoError(t, err)

		// 验证此时配额：used_inodes == 3 (根目录 + target.bin + orphan2.bin), used_bytes == 4096
		preDisk2, err := mgr.GetDisk(parentCtx, disk.DiskID)
		require.NoError(t, err)
		require.Equal(t, uint64(3), preDisk2.UsedInodes)
		require.Equal(t, uint64(4096), preDisk2.UsedBytes)

		// 3. 会话 2 打开目标文件并调用阻塞 SetLkw 进入 PENDING 等待队列
		openTarget2, err := mgr.Open(parentCtx, meta.OpenParams{
			Ino:       targetIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		targetFh2 := openTarget2.Fh
		owner2 := uint64(9222)
		reqID2 := uint64(92221)

		s2Ctx, s2Cancel := context.WithCancel(parentCtx)
		defer s2Cancel()

		s2Done := make(chan error, 1)
		go func() {
			s2Done <- mgr.SetLkw(s2Ctx, meta.SetLkwParams{
				Ino:       targetIno,
				Fh:        targetFh2,
				LockOwner: owner2,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   9222,
				},
				LkFlags:   0,
				SessionID: sess2,
				RequestID: reqID2,
			})
		}()

		// 确认会话 2 已在 file_lock_waiters 中登记为 PENDING 状态
		require.Eventually(t, func() bool {
			var cnt int
			_ = pool.QueryRow(parentCtx,
				"SELECT COUNT(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2 AND status = $3",
				int64(sess2), int64(reqID2), int16(meta.WaiterStatusPending)).Scan(&cnt)
			return cnt == 1
		}, 5*time.Second, 10*time.Millisecond, "会话 2 的等待记录应在底表中处于 PENDING 状态")

		// 4. 将会话 2 的 client_sessions.expires_at 设为过去时间并取消会话 2 的 SetLkw 上下文 (触发 cleanupCancelledWaiter)
		_, err = pool.Exec(parentCtx, "UPDATE client_sessions SET expires_at = NOW() - INTERVAL '1 hour' WHERE session_id = $1", int64(sess2))
		require.NoError(t, err)

		s2Cancel()

		select {
		case err2 := <-s2Done:
			require.True(t, errors.Is(err2, meta.ErrInterrupted) || errors.Is(err2, context.Canceled),
				"SetLkw 取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", err2)
		case <-parentCtx.Done():
			t.Fatal("等待会话 2 SetLkw 取消超时")
		}

		// 5. 断言 file_lock_waiters 中会话 2 的等待记录数为 0
		var waiterCount int
		err = pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM file_lock_waiters WHERE session_id = $1", int64(sess2)).Scan(&waiterCount)
		require.NoError(t, err)
		assert.Equal(t, 0, waiterCount, "cleanupCancelledWaiter 后 file_lock_waiters 底表绝不能残留等待记录")

		// 6. 断言会话 2 持有的孤儿 inodes 记录已被物理回收 (记录数为 0)
		var orphanCount int
		err = pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM inodes WHERE ino = $1", int64(orphan2Ino)).Scan(&orphanCount)
		require.NoError(t, err)
		assert.Equal(t, 0, orphanCount, "会话 2 过期并取消等待后，其持有的孤儿文件必须被物理回收")

		// 7. 断言所属磁盘配额已准确退还
		postDisk2, err := mgr.GetDisk(parentCtx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), postDisk2.UsedInodes, "所属磁盘配额 used_inodes 必须恢复为 2 (仅剩根目录与 target.bin)")
		assert.Equal(t, uint64(0), postDisk2.UsedBytes, "所属磁盘配额 used_bytes 必须退还为 0")
	})
}

func TestSetLkw_BlockingTestsEnforceContextTimeout(t *testing.T) {
	fset := token.NewFileSet()

	// 自检反例 1: 合规函数（WithTimeout + select { case <-waiterRegistered: case <-ctx.Done(): }）
	const testCodeCompliant = `package sample
import (
	"context"
	"time"
)
func testCompliant() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waiterRegistered := make(chan struct{})
	select {
	case <-waiterRegistered:
	case <-ctx.Done():
	}
	_ = mgr.SetLkw(ctx, nil)
}
`
	node1, err := parser.ParseFile(fset, "sample_compliant.go", testCodeCompliant, 0)
	require.NoError(t, err)
	violations1 := auditASTFileForH11(fset, "sample_compliant.go", node1)
	require.Empty(t, violations1, "合规函数不应报出任何 AST 违规，但报出: %v", violations1)

	// 自检反例 2: 作用域遮蔽违规（外层有 WithTimeout，后一个子测试遮蔽为 context.Background()）
	const testCodeShadowed = `package sample
import (
	"context"
	"testing"
	"time"
)
func testShadowed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Run("sub1", func(t *testing.T) {
		_ = mgr.SetLkw(ctx, nil)
	})
	t.Run("sub2", func(t *testing.T) {
		ctx := context.Background()
		_ = mgr.SetLkw(ctx, nil)
	})
}
`
	node2, err := parser.ParseFile(fset, "sample_shadowed.go", testCodeShadowed, 0)
	require.NoError(t, err)
	violations2 := auditASTFileForH11(fset, "sample_shadowed.go", node2)
	require.Len(t, violations2, 1, "遮蔽违规测试必须准确检出 1 条违规，实际: %v", violations2)

	// 自检反例 3: 继承有界上下文（WithCancel 继承自 WithTimeout）
	const testCodeInherited = `package sample
import (
	"context"
	"time"
)
func testInherited() {
	parentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	childCtx, childCancel := context.WithCancel(parentCtx)
	defer childCancel()
	_ = mgr.SetLkw(childCtx, nil)
}
`
	node3, err := parser.ParseFile(fset, "sample_inherited.go", testCodeInherited, 0)
	require.NoError(t, err)
	violations3 := auditASTFileForH11(fset, "sample_inherited.go", node3)
	require.Empty(t, violations3, "继承有界上下文的调用不应报出任何违规，但报出: %v", violations3)

	// 自检反例 4: WithCancel 直接派生自 context.Background() 必须报错
	const testCodeUnboundedCancel = `package sample
import (
	"context"
)
func testUnboundedCancel() {
	wCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = mgr.SetLkw(wCtx, nil)
}
`
	node4, err := parser.ParseFile(fset, "sample_unbounded_cancel.go", testCodeUnboundedCancel, 0)
	require.NoError(t, err)
	violations4 := auditFileForUnboundedSetLkw(fset, "sample_unbounded_cancel.go", node4)
	require.Len(t, violations4, 1, "直接派生自 context.Background() 的 WithCancel 必须检出 1 条违规，实际: %v", violations4)

	// 自检反例 5: 两层嵌套 WithCancel 派生自 context.Background() 必须报错
	const testCodeNestedCancel = `package sample
import (
	"context"
)
func testNestedCancel() {
	c1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	c2, cancel2 := context.WithCancel(c1)
	defer cancel2()
	_ = mgr.SetLkw(c2, nil)
}
`
	node5, err := parser.ParseFile(fset, "sample_nested_cancel.go", testCodeNestedCancel, 0)
	require.NoError(t, err)
	violations5 := auditFileForUnboundedSetLkw(fset, "sample_nested_cancel.go", node5)
	require.Len(t, violations5, 1, "两层嵌套 WithCancel 派生自 context.Background() 必须检出 1 条违规，实际: %v", violations5)

	// 审计当前目录 '.' 下所有 *_test.go 文件
	violations := auditTestSuiteSetLkwTimeouts(t)
	assert.Empty(t, violations, "单元测试套件必须符合 CODING_STANDARDS.md §2 规范，阻塞操作与通道等待必须绑定 context.WithTimeout 超时保护：\n%s", strings.Join(violations, "\n"))
}

func auditTestSuiteSetLkwTimeouts(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	var violations []string

	files, err := filepath.Glob("*_test.go")
	require.NoError(t, err)

	for _, file := range files {
		node, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
		require.NoError(t, err, "解析 %s 失败", file)
		violations = append(violations, auditASTFileForH11(fset, file, node)...)
	}

	return violations
}

func auditFileForUnboundedSetLkw(fset *token.FileSet, filePath string, fileNode *ast.File) []string {
	return auditASTFileForH11(fset, filePath, fileNode)
}

func auditASTFileForH11(fset *token.FileSet, filePath string, fileNode *ast.File) []string {
	var violations []string
	var stack []ast.Node

	ast.Inspect(fileNode, func(n ast.Node) bool {
		if n == nil {
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			return true
		}
		stack = append(stack, n)

		switch stmt := n.(type) {
		case *ast.UnaryExpr:
			if stmt.Op == token.ARROW {
				if ident, ok := stmt.X.(*ast.Ident); ok && ident.Name == "waiterRegistered" {
					var ancestorSelect *ast.SelectStmt
					for i := len(stack) - 2; i >= 0; i-- {
						if sel, ok := stack[i].(*ast.SelectStmt); ok {
							ancestorSelect = sel
							break
						}
					}

					pos := fset.Position(stmt.Pos())
					if ancestorSelect == nil {
						violations = append(violations, fmt.Sprintf("%s:%d: 存在未包裹在带超时或 ctx.Done() 分支 select 中的裸通道接收 <-waiterRegistered",
							filepath.ToSlash(filepath.Clean(filePath)), pos.Line))
					} else if !hasTimeoutOrCancellationBranch(ancestorSelect) {
						violations = append(violations, fmt.Sprintf("%s:%d: select 中接收 <-waiterRegistered 但未包含任何超时 (time.After/timer.C) 或 ctx.Done() 取消分支",
							filepath.ToSlash(filepath.Clean(filePath)), pos.Line))
					}
				}
			}

		case *ast.CallExpr:
			if sel, ok := stmt.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SetLkw" {
				if len(stmt.Args) > 0 {
					arg0 := stmt.Args[0]
					pos := fset.Position(stmt.Pos())
					if isCallTo(arg0, "context", "Background") || isCallTo(arg0, "context", "TODO") {
						violations = append(violations, fmt.Sprintf("%s:%d: 调用 SetLkw 直接传入无超时的 context",
							filepath.ToSlash(filepath.Clean(filePath)), pos.Line))
					} else if ident, ok := arg0.(*ast.Ident); ok {
						if !isVarBoundedAtPos(stack, ident.Name, stmt.Pos()) {
							violations = append(violations, fmt.Sprintf("%s:%d: 调用 SetLkw 使用的 context 变量 %q 未绑定 WithTimeout/WithDeadline",
								filepath.ToSlash(filepath.Clean(filePath)), pos.Line, ident.Name))
						}
					}
				}
			}
		}

		return true
	})

	return violations
}

func hasTimeoutOrCancellationBranch(selectStmt *ast.SelectStmt) bool {
	if selectStmt.Body == nil {
		return false
	}
	for _, clause := range selectStmt.Body.List {
		comm, ok := clause.(*ast.CommClause)
		if !ok || comm.Comm == nil {
			continue
		}
		var recvExpr ast.Expr
		switch s := comm.Comm.(type) {
		case *ast.ExprStmt:
			recvExpr = s.X
		case *ast.AssignStmt:
			if len(s.Rhs) > 0 {
				recvExpr = s.Rhs[0]
			}
		}
		if recvExpr == nil {
			continue
		}
		unary, ok := recvExpr.(*ast.UnaryExpr)
		if !ok || unary.Op != token.ARROW {
			continue
		}
		ch := unary.X
		if call, ok := ch.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Done" {
				return true
			}
			if isCallToExpr(call, "time", "After") {
				return true
			}
		}
		if sel, ok := ch.(*ast.SelectorExpr); ok && sel.Sel.Name == "C" {
			return true
		}
	}
	return false
}

func isVarBoundedAtPos(stack []ast.Node, varName string, usePos token.Pos) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		var stmts []ast.Stmt
		switch b := stack[i].(type) {
		case *ast.BlockStmt:
			stmts = b.List
		case *ast.CaseClause:
			stmts = b.Body
		default:
			continue
		}

		var lastAssign *ast.AssignStmt
		for _, stmt := range stmts {
			if stmt.Pos() >= usePos {
				continue
			}
			assign, ok := stmt.(*ast.AssignStmt)
			if !ok {
				continue
			}
			for _, lhs := range assign.Lhs {
				if ident, ok := lhs.(*ast.Ident); ok && ident.Name == varName {
					lastAssign = assign
					break
				}
			}
		}

		if lastAssign != nil {
			return isAssignmentBounded(stack[:i+1], lastAssign, varName)
		}
	}
	return false
}

func isAssignmentBounded(stack []ast.Node, assign *ast.AssignStmt, varName string) bool {
	varIndex := -1
	for idx, lhs := range assign.Lhs {
		if ident, ok := lhs.(*ast.Ident); ok && ident.Name == varName {
			varIndex = idx
			break
		}
	}
	if varIndex == -1 {
		return false
	}

	var rhsExpr ast.Expr
	if len(assign.Rhs) == 1 {
		rhsExpr = assign.Rhs[0]
	} else if varIndex < len(assign.Rhs) {
		rhsExpr = assign.Rhs[varIndex]
	} else {
		return false
	}

	return isContextExprBounded(stack, rhsExpr, assign.Pos())
}

func isContextExprBounded(stack []ast.Node, expr ast.Expr, usePos token.Pos) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *ast.Ident:
		return isVarBoundedAtPos(stack, e.Name, usePos)
	case *ast.CallExpr:
		if isCallToExpr(e, "context", "WithTimeout") || isCallToExpr(e, "context", "WithDeadline") {
			return true
		}
		if isCallToExpr(e, "context", "Background") || isCallToExpr(e, "context", "TODO") {
			return false
		}
		if isCallToExpr(e, "context", "WithCancel") || isCallToExpr(e, "context", "WithValue") {
			if len(e.Args) > 0 {
				return isContextExprBounded(stack, e.Args[0], usePos)
			}
		}
		return false
	default:
		return false
	}
}

func isCallTo(expr ast.Expr, pkg, fn string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	return isCallToExpr(call, pkg, fn)
}

func isCallToExpr(call *ast.CallExpr, pkg, fn string) bool {
	if call == nil {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return pkgIdent.Name == pkg && sel.Sel.Name == fn
}

type p1UndoSnapshotWipeHookStore struct {
	db.Store
	aCommittedCh    chan struct{}
	aResumeCh       chan struct{}
	bBeforeRollback func()
}

func (s *p1UndoSnapshotWipeHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	role, _ := ctx.Value("p1UndoRole").(string)
	if role == "B" {
		err := s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
			if err := fn(q); err != nil {
				return err
			}
			return errors.New("simulated B tx rollback")
		}, opts...)
		if s.bBeforeRollback != nil {
			s.bBeforeRollback()
		}
		return err
	}

	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil && role == "A" {
		select {
		case s.aCommittedCh <- struct{}{}:
		default:
		}
		select {
		case <-s.aResumeCh:
		case <-ctx.Done():
		}
	}
	return err
}

func TestSetLkw_Round2_P1_UndoRollbackSnapshotOverwrite_WipesCommittedRange(t *testing.T) {
	dsn := getStorageTestDSN()
	parentCtx, parentCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer parentCancel()

	pool, err := pgxpool.New(parentCtx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(parentCtx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	baseStore := db.NewStore(pool)
	hookStore := &p1UndoSnapshotWipeHookStore{
		Store:        baseStore,
		aCommittedCh: make(chan struct{}, 1),
		aResumeCh:    make(chan struct{}),
	}
	mgr := meta.New(hookStore)

	disk, err := mgr.CreateDisk(parentCtx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-r2p1-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sessA := uint64(889801)
	_, err = mgr.RenewSession(parentCtx, sessA)
	require.NoError(t, err)

	createResA, err := mgr.Create(parentCtx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("r2p1_leak.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	fileIno := createResA.Inode.Ino
	fhA := createResA.Fh
	ownerA := uint64(9201)

	// 1. 请求 A 通过 SetLkw 获得 [0, 100] 排他写锁，并在数据库事务提交后停在返回前
	aCtx, aCancel := context.WithCancel(parentCtx)
	defer aCancel()
	ctxA := context.WithValue(aCtx, "p1UndoRole", "A")

	aDoneCh := make(chan error, 1)
	go func() {
		aDoneCh <- mgr.SetLkw(ctxA, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9201,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 92011,
		})
	}()

	select {
	case <-hookStore.aCommittedCh:
	case <-parentCtx.Done():
		t.Fatal("等待请求 A 授锁事务提交超时")
	}

	// 2. 请求 B 解锁 [0, 49]：事务执行并修改内存快照后回滚；
	// 在 B 的 ExecTxWithOptions 返回错误给 SetLk 调用 lastUndo.Rollback() 之前，
	// 同步执行请求 C 对 [50, 100] 加排他写锁并提交。
	hookStore.bBeforeRollback = func() {
		// 此时 B 的事务已经 ROLLBACK，PostgreSQL 行锁已被释放；
		// 此时 A 的在途 grant 已被 B 裁剪为 pendingRanges = [50, 100]；
		// 执行请求 C，加写锁 [50, 100] 并提交
		cErr := mgr.SetLk(parentCtx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 50,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9201,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, cErr, "请求 C 应该成功提交")
	}

	ctxB := context.WithValue(parentCtx, "p1UndoRole", "B")
	bErr := mgr.SetLk(ctxB, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: ownerA,
		Lk: meta.FileLock{
			Start: 0,
			End:   49,
			Type:  meta.F_UNLCK,
			PID:   9201,
		},
		LkFlags:   0,
		SessionID: sessA,
	})
	require.Error(t, bErr, "请求 B 的事务被模拟回滚应返回错误")

	// 3. 取消请求 A 的上下文并放行，A 退出并执行 rollbackGrantedLock
	aCancel()
	close(hookStore.aResumeCh)

	select {
	case aErr := <-aDoneCh:
		require.True(t, errors.Is(aErr, meta.ErrInterrupted) || errors.Is(aErr, context.Canceled),
			"SetLkw 取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", aErr)
	case <-parentCtx.Done():
		t.Fatal("等待请求 A 取消返回超时")
	}

	// 4. 验证断言：C 成功提交的 [50, 100] 写锁必须存活且绝不能被误删
	var lockCount int
	countErr := pool.QueryRow(parentCtx,
		"SELECT COUNT(*) FROM file_locks WHERE ino = $1 AND lock_owner = $2 AND start_offset = 50 AND end_offset = 100 AND lock_type = $3",
		int64(fileIno), int64(ownerA), int16(meta.F_WRLCK)).Scan(&lockCount)
	require.NoError(t, countErr)
	assert.Equal(t, 1, lockCount,
		"Round 2 P-1 缺陷验证：请求 C 成功提交的 [50, 100] 写锁必须在数据库中存活且未被误删（当前因 B 的 lastUndo.Rollback 用全量旧快照覆盖抹除了 C 的区间扣减，导致 A 取消时误删了 C 的已提交锁）")
}

type p2UnlockFailHookStore struct {
	db.Store
	aCommittedCh chan struct{}
	aResumeCh    chan struct{}
}

func (s *p2UnlockFailHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	role, _ := ctx.Value("p2Role").(string)
	if role == "B" {
		return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
			if err := fn(q); err != nil {
				return err
			}
			return errors.New("simulated B unlock tx failure")
		}, opts...)
	}

	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil && role == "A" {
		select {
		case s.aCommittedCh <- struct{}{}:
		default:
		}
		select {
		case <-s.aResumeCh:
		case <-ctx.Done():
		}
	}
	return err
}

func TestSetLkw_Round2_P2_UnlockTxFailure_SkipsUndo_HangingLockLeak(t *testing.T) {
	dsn := getStorageTestDSN()

	t.Run("POSIX", func(t *testing.T) {
		parentCtx, parentCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer parentCancel()

		pool, err := pgxpool.New(parentCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(parentCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		baseStore := db.NewStore(pool)
		hookStore := &p2UnlockFailHookStore{
			Store:        baseStore,
			aCommittedCh: make(chan struct{}, 1),
			aResumeCh:    make(chan struct{}),
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(parentCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-r2p2-posix-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889901)
		sessOther := uint64(889902)
		_, err = mgr.RenewSession(parentCtx, sessA)
		require.NoError(t, err)
		_, err = mgr.RenewSession(parentCtx, sessOther)
		require.NoError(t, err)

		createResA, err := mgr.Create(parentCtx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("r2p2_posix.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh
		ownerA := uint64(9301)

		openResOther, err := mgr.Open(parentCtx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessOther,
		})
		require.NoError(t, err)
		fhOther := openResOther.Fh
		ownerOther := uint64(9302)

		// 1. 请求 A 授锁事务已提交但停在返回前
		aCtx, aCancel := context.WithCancel(parentCtx)
		defer aCancel()
		ctxA := context.WithValue(aCtx, "p2Role", "A")

		aDoneCh := make(chan error, 1)
		go func() {
			aDoneCh <- mgr.SetLkw(ctxA, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: ownerA,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   9301,
				},
				LkFlags:   0,
				SessionID: sessA,
				RequestID: 93011,
			})
		}()

		select {
		case <-hookStore.aCommittedCh:
		case <-parentCtx.Done():
			t.Fatal("等待请求 A 授锁事务提交超时")
		}

		// 2. 请求 B 调用 SetLkw(F_UNLCK) 解锁，在 fn(q) 完成后事务失败回滚
		ctxB := context.WithValue(parentCtx, "p2Role", "B")
		bErr := mgr.SetLkw(ctxB, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_UNLCK,
				PID:   9301,
			},
			LkFlags:   0,
			SessionID: sessA,
			RequestID: 93012,
		})
		require.Error(t, bErr, "SetLkw(F_UNLCK) 事务失败应返回错误")

		// 3. 取消请求 A 的上下文并唤醒放行
		aCancel()
		close(hookStore.aResumeCh)

		select {
		case aErr := <-aDoneCh:
			require.True(t, errors.Is(aErr, meta.ErrInterrupted) || errors.Is(aErr, context.Canceled),
				"SetLkw 取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", aErr)
		case <-parentCtx.Done():
			t.Fatal("等待请求 A 取消返回超时")
		}

		// 4. 断言底表残留锁数量必须为 0
		var lockCount int
		countErr := pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(fileIno)).Scan(&lockCount)
		require.NoError(t, countErr)
		assert.Equal(t, 0, lockCount,
			"Round 2 P-2 缺陷验证 (POSIX)：SetLkw 取消回滚后，file_locks 底表绝不能残留悬挂锁（当前因 SetLkw 解锁失败因 granted=true 跳过 undo 导致前驱锁失效标记未撤销，使得 rollbackGrantedLock 提前退出产生悬挂锁）")

		// 5. 断言外部冲突 Owner 加排他锁应当成功
		otherErr := mgr.SetLk(parentCtx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhOther,
			LockOwner: ownerOther,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   9302,
			},
			LkFlags:   0,
			SessionID: sessOther,
		})
		require.NoError(t, otherErr, "Round 2 P-2 缺陷验证 (POSIX)：外部冲突 Owner 尝试加排他锁必须成功，不能因残留悬挂锁冲突失败")
	})

	t.Run("Flock", func(t *testing.T) {
		parentCtx, parentCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer parentCancel()

		pool, err := pgxpool.New(parentCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(parentCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		baseStore := db.NewStore(pool)
		hookStore := &p2UnlockFailHookStore{
			Store:        baseStore,
			aCommittedCh: make(chan struct{}, 1),
			aResumeCh:    make(chan struct{}),
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(parentCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-r2p2-flock-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(889911)
		sessOther := uint64(889912)
		_, err = mgr.RenewSession(parentCtx, sessA)
		require.NoError(t, err)
		_, err = mgr.RenewSession(parentCtx, sessOther)
		require.NoError(t, err)

		createResA, err := mgr.Create(parentCtx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("r2p2_flock.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh
		ownerA := uint64(9311)

		openResOther, err := mgr.Open(parentCtx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessOther,
		})
		require.NoError(t, err)
		fhOther := openResOther.Fh
		ownerOther := uint64(9312)

		// 1. 请求 A 授锁事务已提交但停在返回前
		aCtx, aCancel := context.WithCancel(parentCtx)
		defer aCancel()
		ctxA := context.WithValue(aCtx, "p2Role", "A")

		aDoneCh := make(chan error, 1)
		go func() {
			aDoneCh <- mgr.SetLkw(ctxA, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: ownerA,
				Lk: meta.FileLock{
					Start: 0,
					End:   meta.OffsetMax,
					Type:  meta.F_WRLCK,
					PID:   9311,
				},
				LkFlags:   meta.FuseLkFlock,
				SessionID: sessA,
				RequestID: 93111,
			})
		}()

		select {
		case <-hookStore.aCommittedCh:
		case <-parentCtx.Done():
			t.Fatal("等待请求 A 授锁事务提交超时")
		}

		// 2. 请求 B 调用 SetLkw(F_UNLCK) 解锁，在 fn(q) 完成后事务失败回滚
		ctxB := context.WithValue(parentCtx, "p2Role", "B")
		bErr := mgr.SetLkw(ctxB, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_UNLCK,
				PID:   9311,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sessA,
			RequestID: 93112,
		})
		require.Error(t, bErr, "SetLkw(F_UNLCK) 事务失败应返回错误")

		// 3. 取消请求 A 的上下文并唤醒放行
		aCancel()
		close(hookStore.aResumeCh)

		select {
		case aErr := <-aDoneCh:
			require.True(t, errors.Is(aErr, meta.ErrInterrupted) || errors.Is(aErr, context.Canceled),
				"SetLkw 取消后应返回 ErrInterrupted 或 context.Canceled, actual: %v", aErr)
		case <-parentCtx.Done():
			t.Fatal("等待请求 A 取消返回超时")
		}

		// 4. 断言底表残留锁数量必须为 0
		var lockCount int
		countErr := pool.QueryRow(parentCtx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(fileIno)).Scan(&lockCount)
		require.NoError(t, countErr)
		assert.Equal(t, 0, lockCount,
			"Round 2 P-2 缺陷验证 (Flock)：SetLkw 取消回滚后，file_locks 底表绝不能残留悬挂锁（当前因 SetLkw 解锁失败因 granted=true 跳过 undo 导致前驱锁失效标记未撤销，使得 rollbackGrantedLock 提前退出产生悬挂锁）")

		// 5. 断言外部冲突 Owner 加排他锁应当成功
		otherErr := mgr.SetLk(parentCtx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhOther,
			LockOwner: ownerOther,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   9312,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sessOther,
		})
		require.NoError(t, otherErr, "Round 2 P-2 缺陷验证 (Flock)：外部冲突 Owner 尝试加排他锁必须成功，不能因残留悬挂锁冲突失败")
	})
}

type transientHookQuerier struct {
	db.Querier
	onDeleteFileLockWaiter func(ctx context.Context, q db.Querier, params db.DeleteFileLockWaiterParams) (int64, error)
	onCreateFileLock       func(ctx context.Context, q db.Querier, params db.CreateFileLockParams) (db.FileLock, error)
	onDeleteFileLocksByIDs func(ctx context.Context, q db.Querier, ids []int64) error
}

func (q *transientHookQuerier) DeleteFileLockWaiter(ctx context.Context, params db.DeleteFileLockWaiterParams) (int64, error) {
	if q.onDeleteFileLockWaiter != nil {
		return q.onDeleteFileLockWaiter(ctx, q.Querier, params)
	}
	return q.Querier.DeleteFileLockWaiter(ctx, params)
}

func (q *transientHookQuerier) CreateFileLock(ctx context.Context, params db.CreateFileLockParams) (db.FileLock, error) {
	if q.onCreateFileLock != nil {
		return q.onCreateFileLock(ctx, q.Querier, params)
	}
	return q.Querier.CreateFileLock(ctx, params)
}

func (q *transientHookQuerier) DeleteFileLocksByIDs(ctx context.Context, ids []int64) error {
	if q.onDeleteFileLocksByIDs != nil {
		return q.onDeleteFileLocksByIDs(ctx, q.Querier, ids)
	}
	return q.Querier.DeleteFileLocksByIDs(ctx, ids)
}

type transientRetryHookStore struct {
	db.Store
	onDeleteFileLockWaiter func(ctx context.Context, q db.Querier, params db.DeleteFileLockWaiterParams) (int64, error)
	onCreateFileLock       func(ctx context.Context, q db.Querier, params db.CreateFileLockParams) (db.FileLock, error)
	onDeleteFileLocksByIDs func(ctx context.Context, q db.Querier, ids []int64) error
	onExecTx               func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error
}

func (s *transientRetryHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	wrappedFn := func(q db.Querier) error {
		hookQ := &transientHookQuerier{
			Querier:                q,
			onDeleteFileLockWaiter: s.onDeleteFileLockWaiter,
			onCreateFileLock:       s.onCreateFileLock,
			onDeleteFileLocksByIDs: s.onDeleteFileLocksByIDs,
		}
		return fn(hookQ)
	}
	if s.onExecTx != nil {
		return s.onExecTx(ctx, wrappedFn, opts...)
	}
	return s.Store.ExecTxWithOptions(ctx, wrappedFn, opts...)
}

// TestSetLkw_CancelCleanupAndRollback_TransientConflictRetry 复现缺陷 P-1：
// 1. cleanupCancelledWaiter 在遇到瞬态 40P01 等冲突错误时，未重试且吞没 SQL 错误，导致残留 PENDING 等待者。
// 2. rollbackGrantedLock 在恢复原锁阶段遇到瞬态 40P01 时，未重试且吞没 CreateFileLock 错误，导致调用方原有锁被永久误删。
// 3. rollbackGrantedLock 在删除刚授予的 Flock 锁阶段遇到瞬态 40P01 时，未重试且吞没 DeleteFileLocksByIDs 错误。
func TestSetLkw_CancelCleanupAndRollback_TransientConflictRetry(t *testing.T) {
	dsn := getStorageTestDSN()

	t.Run("Subtest1_CleanupCancelledWaiter_Transient40P01Retry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		baseStore := db.NewStore(pool)
		hookStore := &transientRetryHookStore{
			Store: baseStore,
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-waiter-retry-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sess1 := uint64(700101)
		sess2 := uint64(700102)
		sess3 := uint64(700103)
		_, err = mgr.RenewSession(ctx, sess1)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sess2)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sess3)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("waiter_retry.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		fh1 := createRes.Fh

		openRes2, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		fh2 := openRes2.Fh

		openRes3, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess3,
		})
		require.NoError(t, err)
		fh3 := openRes3.Fh

		owner1 := uint64(101)
		owner2 := uint64(202)
		owner3 := uint64(303)

		// 1. 客户端 1 持有 [0, 100] 排他写锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: owner1,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   1001,
			},
			LkFlags:   0,
			SessionID: sess1,
		})
		require.NoError(t, err)

		// 2. 客户端 2 发起阻塞 SetLkw 请求获取 [0, 100] 写锁
		waiterCtx, cancelWaiter := context.WithCancel(ctx)
		defer cancelWaiter()

		var deleteAttempts atomic.Int32
		var inject40P01 atomic.Bool

		hookStore.onDeleteFileLockWaiter = func(callCtx context.Context, q db.Querier, p db.DeleteFileLockWaiterParams) (int64, error) {
			if inject40P01.Load() && deleteAttempts.Add(1) == 1 {
				// 模拟首次清理事务遇到瞬态 40P01 deadlock_detected
				return 0, &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
			}
			return q.DeleteFileLockWaiter(callCtx, p)
		}

		errCh := make(chan error, 1)
		go func() {
			errCh <- mgr.SetLkw(waiterCtx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: owner2,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   2002,
				},
				LkFlags:   0,
				SessionID: sess2,
				RequestID: 5001,
			})
		}()

		// 确保客户端 2 已成功在底表登记为 PENDING 等待者
		require.Eventually(t, func() bool {
			waiters, listErr := baseStore.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(fileIno),
				LkFlags: 0,
			})
			return listErr == nil && len(waiters) == 1
		}, 3*time.Second, 5*time.Millisecond, "客户端 2 必须在底表成功登记等待者")

		// 激活 40P01 注入并取消 waiterCtx
		inject40P01.Store(true)
		cancelWaiter()

		select {
		case waitErr := <-errCh:
			require.ErrorIs(t, waitErr, meta.ErrInterrupted, "取消后的 SetLkw 必须返回 ErrInterrupted")
		case <-time.After(5 * time.Second):
			t.Fatal("等待 SetLkw 取消返回超时")
		}

		// 显式断言：确保首次 40P01 故障确实被注入且触发了至少第 2 次重试
		require.GreaterOrEqual(t, deleteAttempts.Load(), int32(2), "cleanupCancelledWaiter 必须经历首次 40P01 故障并至少重试 1 次")

		// 3. 断言：底表 file_lock_waiters 中该等待者已被彻底清除（数量为 0）
		waiters, err := baseStore.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)
		assert.Empty(t, waiters, "P-1 缺陷验证：cleanupCancelledWaiter 遇到瞬态 40P01 必须自动重试并彻底清除底表 waiter 记录，绝不能吞错残留")

		// 4. 断言：客户端 1 释放锁后，其他竞争方（客户端 3）随后可以正常获取锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: owner1,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_UNLCK,
				PID:   1001,
			},
			LkFlags:   0,
			SessionID: sess1,
		})
		require.NoError(t, err)

		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh3,
			LockOwner: owner3,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   3003,
			},
			LkFlags:   0,
			SessionID: sess3,
		})
		require.NoError(t, err, "客户端 1 释放后，竞争方客户端 3 必须能正常获取排他锁")
	})

	t.Run("Subtest2_RollbackGrantedLock_RestoreOldLocks_Transient40P01Retry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		baseStore := db.NewStore(pool)
		hookStore := &transientRetryHookStore{
			Store: baseStore,
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-rollback-retry-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessA := uint64(800101)
		sessB := uint64(800102)
		_, err = mgr.RenewSession(ctx, sessA)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sessB)
		require.NoError(t, err)

		createResA, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("rollback_retry.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		fileIno := createResA.Inode.Ino
		fhA := createResA.Fh

		openResB, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessB,
		})
		require.NoError(t, err)
		fhB := openResB.Fh

		ownerA := uint64(1001)
		ownerB := uint64(1002)

		// 1. 客户端 A 原本持有 [0, 100] 读锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: ownerA,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_RDLCK,
				PID:   1001,
			},
			LkFlags:   0,
			SessionID: sessA,
		})
		require.NoError(t, err)

		// 客户端 B 也持有 [0, 100] 读锁，阻止客户端 A 升级为写锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhB,
			LockOwner: ownerB,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_RDLCK,
				PID:   1002,
			},
			LkFlags:   0,
			SessionID: sessB,
		})
		require.NoError(t, err)

		// 2. 客户端 A 发起阻塞 SetLkw 升级为 [0, 100] 写锁
		type ctxTag string
		upgradeCtxKey := ctxTag("is_upgrade_a")
		callCtx, callCancel := context.WithCancel(context.WithValue(ctx, upgradeCtxKey, true))
		defer callCancel()

		var writeLockCreatedInTx atomic.Bool
		var injectCreateLock40P01 atomic.Bool
		var createLockAttempts atomic.Int32

		hookStore.onCreateFileLock = func(callCtx context.Context, q db.Querier, p db.CreateFileLockParams) (db.FileLock, error) {
			if p.SessionID == int64(sessA) && p.LockType == int16(meta.F_WRLCK) {
				writeLockCreatedInTx.Store(true)
			}
			if injectCreateLock40P01.Load() && createLockAttempts.Add(1) == 1 {
				// 模拟 rollbackGrantedLock 内恢复原读锁时首次 CreateFileLock 返回瞬态 40P01
				return db.FileLock{}, &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
			}
			return q.CreateFileLock(callCtx, p)
		}

		hookStore.onExecTx = func(txCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			err := hookStore.Store.ExecTxWithOptions(txCtx, fn, opts...)
			if err == nil && writeLockCreatedInTx.Load() && txCtx.Value(upgradeCtxKey) != nil {
				// 在客户端 A 授锁写锁事务成功提交后触发取消，并激活 rollback 期间 CreateFileLock 40P01 注入
				writeLockCreatedInTx.Store(false)
				injectCreateLock40P01.Store(true)
				callCancel()
			}
			return err
		}

		upgradeErrCh := make(chan error, 1)
		go func() {
			upgradeErrCh <- mgr.SetLkw(callCtx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhA,
				LockOwner: ownerA,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   1001,
				},
				LkFlags:   0,
				SessionID: sessA,
				RequestID: 6001,
			})
		}()

		// 等待客户端 A 在底表登记为 PENDING 等待者
		require.Eventually(t, func() bool {
			waiters, listErr := baseStore.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(fileIno),
				LkFlags: 0,
			})
			return listErr == nil && len(waiters) == 1
		}, 3*time.Second, 5*time.Millisecond, "客户端 A 必须在底表成功登记等待者")

		// 客户端 B 释放读锁，唤醒客户端 A
		bUnlockErr := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhB,
			LockOwner: ownerB,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_UNLCK,
				PID:   1002,
			},
			LkFlags:   0,
			SessionID: sessB,
		})
		require.NoError(t, bUnlockErr)

		// 等待客户端 A 的 SetLkw 返回
		select {
		case retErr := <-upgradeErrCh:
			require.ErrorIs(t, retErr, meta.ErrInterrupted, "回滚后的 SetLkw 必须返回 ErrInterrupted")
		case <-time.After(5 * time.Second):
			t.Fatal("等待客户端 A 的 SetLkw 回滚返回超时")
		}

		// 断言：回滚恢复旧读锁的 CreateFileLock 确实经历了首次 40P01 失败并重试成功
		require.GreaterOrEqual(t, createLockAttempts.Load(), int32(2), "rollbackGrantedLock 恢复原读锁时必须经历首次 40P01 故障并至少重试 1 次")

		// 断言：底表 file_lock_waiters 为空
		waiters, err := baseStore.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)
		assert.Empty(t, waiters, "回滚完成后底表 file_lock_waiters 必须为空")

		// 3. 断言：新授予的写锁被撤销，且原有的 [0, 100] 读锁完整恢复保留！
		activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)

		require.Len(t, activeLocks, 1, "P-1 缺陷验证：客户端 A 原有的 [0, 100] 读锁必须完整恢复保留，绝不能因 40P01 吞错而永久误删")
		assert.Equal(t, int64(ownerA), activeLocks[0].LockOwner)
		assert.Equal(t, int64(sessA), activeLocks[0].SessionID)
		assert.Equal(t, int16(meta.F_RDLCK), activeLocks[0].LockType, "原锁类型必须为读锁 F_RDLCK")
		assert.Equal(t, int64(0), activeLocks[0].StartOffset)
		assert.Equal(t, int64(100), activeLocks[0].EndOffset)
	})

	t.Run("Subtest3_RollbackGrantedFlock_DeleteLock_Transient40P01Retry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		baseStore := db.NewStore(pool)
		hookStore := &transientRetryHookStore{
			Store: baseStore,
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-flock-rollback-retry-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sess1 := uint64(900101)
		sess2 := uint64(900102)
		sess3 := uint64(900103)
		_, err = mgr.RenewSession(ctx, sess1)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sess2)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sess3)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("flock_rollback_retry.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		fh1 := createRes.Fh

		openRes2, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		fh2 := openRes2.Fh

		openRes3, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess3,
		})
		require.NoError(t, err)
		fh3 := openRes3.Fh

		owner1 := uint64(2001)
		owner2 := uint64(2002)
		owner3 := uint64(2003)

		// 1. 客户端 1 持有排他 flock 锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: owner1,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   2001,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sess1,
		})
		require.NoError(t, err)

		// 2. 客户端 2 发起阻塞 SetLkw 请求获取排他 flock 锁
		type ctxTag string
		flockCtxKey := ctxTag("is_flock_2")
		callCtx, callCancel := context.WithCancel(context.WithValue(ctx, flockCtxKey, true))
		defer callCancel()

		var flockCreatedInTx atomic.Bool
		var injectDeleteLocks40P01 atomic.Bool
		var deleteLocksAttempts atomic.Int32

		hookStore.onCreateFileLock = func(callCtx context.Context, q db.Querier, p db.CreateFileLockParams) (db.FileLock, error) {
			if p.SessionID == int64(sess2) && p.LkFlags == int32(meta.FuseLkFlock) && p.LockType == int16(meta.F_WRLCK) {
				flockCreatedInTx.Store(true)
			}
			return q.CreateFileLock(callCtx, p)
		}

		hookStore.onExecTx = func(txCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			err := hookStore.Store.ExecTxWithOptions(txCtx, fn, opts...)
			if err == nil && flockCreatedInTx.Load() && txCtx.Value(flockCtxKey) != nil {
				// 在客户端 2 授锁事务成功提交后触发取消，并激活 rollback 期间 DeleteFileLocksByIDs 40P01 注入
				flockCreatedInTx.Store(false)
				injectDeleteLocks40P01.Store(true)
				callCancel()
			}
			return err
		}

		hookStore.onDeleteFileLocksByIDs = func(callCtx context.Context, q db.Querier, ids []int64) error {
			if injectDeleteLocks40P01.Load() && deleteLocksAttempts.Add(1) == 1 {
				// 模拟 rollbackGrantedLock 内删除刚授予的 flock 时首次 DeleteFileLocksByIDs 返回瞬态 40P01
				return &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
			}
			return q.DeleteFileLocksByIDs(callCtx, ids)
		}

		flockErrCh := make(chan error, 1)
		go func() {
			flockErrCh <- mgr.SetLkw(callCtx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: owner2,
				Lk: meta.FileLock{
					Start: 0,
					End:   meta.OffsetMax,
					Type:  meta.F_WRLCK,
					PID:   2002,
				},
				LkFlags:   meta.FuseLkFlock,
				SessionID: sess2,
				RequestID: 7001,
			})
		}()

		// 等待客户端 2 在底表登记为 PENDING 等待者
		require.Eventually(t, func() bool {
			waiters, listErr := baseStore.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(fileIno),
				LkFlags: int32(meta.FuseLkFlock),
			})
			return listErr == nil && len(waiters) == 1
		}, 3*time.Second, 5*time.Millisecond, "客户端 2 必须在底表成功登记等待者")

		// 客户端 1 释放 flock 锁，唤醒客户端 2 授锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: owner1,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_UNLCK,
				PID:   2001,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sess1,
		})
		require.NoError(t, err)

		// 等待客户端 2 的 SetLkw 返回
		select {
		case retErr := <-flockErrCh:
			require.ErrorIs(t, retErr, meta.ErrInterrupted, "回滚后的 SetLkw 必须返回 ErrInterrupted")
		case <-time.After(5 * time.Second):
			t.Fatal("等待客户端 2 的 SetLkw 回滚返回超时")
		}

		// 断言 DeleteFileLocksByIDs 尝试次数 >= 2
		require.GreaterOrEqual(t, deleteLocksAttempts.Load(), int32(2), "rollbackGrantedLock 删除刚授予的 flock 时必须经历首次 40P01 故障并至少重试 1 次")

		// 断言底表无残留 flock 锁与 waiter
		activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: int32(meta.FuseLkFlock),
		})
		require.NoError(t, err)
		assert.Empty(t, activeLocks, "底表不能有残留的 flock 锁")

		waiters, err := baseStore.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: int32(meta.FuseLkFlock),
		})
		require.NoError(t, err)
		assert.Empty(t, waiters, "底表不能有残留的 flock 等待者")

		// 断言另一句柄随后能成功非阻塞获取排他 flock 锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh3,
			LockOwner: owner3,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   3003,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sess3,
		})
		require.NoError(t, err, "残留清理干净后，第三方客户端 3 必须能成功非阻塞获取排他 flock 锁")
	})
}

type p1UndoRollbackHookStore struct {
	db.Store
	onExecTx         func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error
	onCreateFileLock func(ctx context.Context, q db.Querier, params db.CreateFileLockParams) (db.FileLock, error)
}

func (s *p1UndoRollbackHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	wrappedFn := func(q db.Querier) error {
		hookQ := &p1UndoRollbackHookQuerier{
			Querier:          q,
			onCreateFileLock: s.onCreateFileLock,
		}
		return fn(hookQ)
	}
	if s.onExecTx != nil {
		return s.onExecTx(ctx, wrappedFn, opts...)
	}
	return s.Store.ExecTxWithOptions(ctx, wrappedFn, opts...)
}

type p1UndoRollbackHookQuerier struct {
	db.Querier
	onCreateFileLock func(ctx context.Context, q db.Querier, params db.CreateFileLockParams) (db.FileLock, error)
}

func (q *p1UndoRollbackHookQuerier) CreateFileLock(ctx context.Context, params db.CreateFileLockParams) (db.FileLock, error) {
	if q.onCreateFileLock != nil {
		return q.onCreateFileLock(ctx, q.Querier, params)
	}
	return q.Querier.CreateFileLock(ctx, params)
}

// TestSetLkw_Round2_UndoRollbackDoesNotRestoreRangeClaimedByConcurrentSetLk 复现缺陷 P-1：
// 当请求 A 在 inFlightGrants 中登记后准备取消，请求 B 尝试授锁将其区间剪除并记入 undo，
// B 事务失败回滚并转入 rollbackGrantedLock。在此期间并发请求 C 成功获取并提交被剪除区间，
// 随后 B 的 lastUndo.Rollback() 不应错误恢复已被 C 接管的区间，否则会导致 A 后续取消时误删 C 已提交的锁。
func TestSetLkw_Round2_UndoRollbackDoesNotRestoreRangeClaimedByConcurrentSetLk(t *testing.T) {
	dsn := getStorageTestDSN()

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

	baseStore := db.NewStore(pool)
	hookStore := &p1UndoRollbackHookStore{Store: baseStore}
	mgr := meta.New(hookStore)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-p1-undo-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sess := uint64(8001)
	owner := uint64(9001)

	_, err = mgr.RenewSession(ctx, sess)
	require.NoError(t, err)

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("p1_test_file.txt"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sess,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh := createRes.Fh

	type ctxKey string
	const (
		tagKey ctxKey = "p1_req_tag"
		tagA          = "req_a"
		tagB          = "req_b"
		tagC          = "req_c"
	)

	aCleanupReadyCh := make(chan struct{}, 1)
	allowACleanupCh := make(chan struct{})
	releaseACleanup := sync.OnceFunc(func() { close(allowACleanupCh) })
	t.Cleanup(releaseACleanup)

	bGrantAbortedCh := make(chan struct{}, 1)
	bCleanupCommittedCh := make(chan struct{}, 1)
	allowBLastUndoRollbackCh := make(chan struct{})
	releaseBLastUndoRollback := sync.OnceFunc(func() { close(allowBLastUndoRollbackCh) })
	t.Cleanup(releaseBLastUndoRollback)

	var hookActive atomic.Bool
	var aCleanupSeen atomic.Bool
	var bCleanupSeen atomic.Bool
	errAbortB := errors.New("simulated B transaction abort")

	callCtxA, callCancelA := context.WithCancel(context.WithValue(ctx, tagKey, tagA))
	defer callCancelA()

	callCtxB, callCancelB := context.WithCancel(context.WithValue(ctx, tagKey, tagB))
	defer callCancelB()

	callCtxC := context.WithValue(ctx, tagKey, tagC)

	hookStore.onCreateFileLock = func(callCtx context.Context, q db.Querier, p db.CreateFileLockParams) (db.FileLock, error) {
		if !hookActive.Load() {
			return q.CreateFileLock(callCtx, p)
		}
		// A 的主事务请求写锁 [0, 100] 时，返回 dummy lock，使其在内存 inFlightGrants 成功登记但在 DB 不留物理锁
		if callCtx.Value(tagKey) == tagA {
			return db.FileLock{
				ID:          888888,
				SessionID:   p.SessionID,
				Ino:         p.Ino,
				Fh:          p.Fh,
				LockOwner:   p.LockOwner,
				LkFlags:     p.LkFlags,
				StartOffset: p.StartOffset,
				EndOffset:   p.EndOffset,
				LockType:    p.LockType,
				Pid:         p.Pid,
			}, nil
		}
		return q.CreateFileLock(callCtx, p)
	}

	hookStore.onExecTx = func(txCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		if !hookActive.Load() {
			return hookStore.Store.ExecTxWithOptions(txCtx, fn, opts...)
		}

		tag := txCtx.Value(tagKey)
		switch tag {
		case tagA:
			// A 的主授锁事务：执行提交并在退出时取消 callCtxA，促使 A 走向 rollbackGrantedLock
			err := hookStore.Store.ExecTxWithOptions(txCtx, fn, opts...)
			if err == nil {
				callCancelA()
			}
			return err

		case tagB:
			// B 的主授锁事务：在闭包内执行 registerInFlightGrant 将 A 的 pendingRanges 剪除为 [50, 100]，
			// 并将被剪除的 [0, 49] 暂存至 B.lastUndo.removedRanges，随后令外层事务回滚
			_ = hookStore.Store.ExecTxWithOptions(txCtx, func(q db.Querier) error {
				if err := fn(q); err != nil {
					return err
				}
				return errAbortB
			}, opts...)
			bGrantAbortedCh <- struct{}{}
			return errAbortB

		case tagC:
			// C 的 SetLk 事务：正常提交写锁 [0, 49]
			return hookStore.Store.ExecTxWithOptions(txCtx, fn, opts...)

		default:
			// 无 tag 的事务来自于 rollbackGrantedLock 内部的 cleanupCtx
			if !aCleanupSeen.Load() {
				aCleanupSeen.Store(true)
				aCleanupReadyCh <- struct{}{}
				select {
				case <-allowACleanupCh:
				case <-txCtx.Done():
					return txCtx.Err()
				}
				return hookStore.Store.ExecTxWithOptions(txCtx, fn, opts...)
			}
			if !bCleanupSeen.Load() {
				bCleanupSeen.Store(true)
				// B 的 cleanup 事务在数据库成功提交（此时 pendingUndoAction 已置为 1），
				// 但在第 2234 行执行 lastUndo.Rollback() 前暂停，给并发请求 C 插入并提交的时间窗口
				err := hookStore.Store.ExecTxWithOptions(txCtx, fn, opts...)
				if err == nil {
					bCleanupCommittedCh <- struct{}{}
					select {
					case <-allowBLastUndoRollbackCh:
					case <-txCtx.Done():
						return txCtx.Err()
					}
				}
				return err
			}
			return hookStore.Store.ExecTxWithOptions(txCtx, fn, opts...)
		}
	}

	hookActive.Store(true)
	defer hookActive.Store(false)

	// 1. 请求 A 发起 SetLkw([0, 100], F_WRLCK)，授锁并在 inFlightGrants 登记 pendingRanges = [0, 100]，
	// 随后因 callCtxA 被取消进入 rollbackGrantedLock，并在清理事务入口挂起等待
	aErrCh := make(chan error, 1)
	go func() {
		aErrCh <- mgr.SetLkw(callCtxA, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fh,
			LockOwner: owner,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   1001,
			},
			LkFlags:   0,
			SessionID: sess,
			RequestID: 1001,
		})
	}()

	select {
	case <-aCleanupReadyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("等待请求 A 挂起在 cleanup 事务入口超时")
	}

	// 2. 请求 B（同一 SessionID, LockOwner）发起 SetLkw([0, 49], F_WRLCK)，
	// 在闭包内将 A 的 pendingRanges 剪除为 [50, 100]，将被剪除的 [0, 49] 暂存至 B.lastUndo.removedRanges[A.regID]。
	// 随后外层事务失败回滚，进入 rollbackGrantedLock，清理事务成功提交后在 lastUndo.Rollback() 前挂起
	bErrCh := make(chan error, 1)
	go func() {
		bErrCh <- mgr.SetLkw(callCtxB, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fh,
			LockOwner: owner,
			Lk: meta.FileLock{
				Start: 0,
				End:   49,
				Type:  meta.F_WRLCK,
				PID:   1002,
			},
			LkFlags:   0,
			SessionID: sess,
			RequestID: 1002,
		})
	}()

	select {
	case <-bCleanupCommittedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("等待请求 B 的 cleanup 事务提交超时")
	}

	// 3. 在 B 的 lastUndo.Rollback() 执行前，请求 C（同一 SessionID, LockOwner）成功执行 SetLk([0, 49], F_WRLCK) 并提交返回。
	// 由于此时 [0, 49] 暂存在 B.lastUndo.removedRanges 中而不在 A.pendingRanges 中，C 未能从中扣除 [0, 49]
	cErr := mgr.SetLk(callCtxC, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh,
		LockOwner: owner,
		Lk: meta.FileLock{
			Start: 0,
			End:   49,
			Type:  meta.F_WRLCK,
			PID:   1003,
		},
		LkFlags:   0,
		SessionID: sess,
	})
	require.NoError(t, cErr, "请求 C 的 SetLk([0, 49]) 必须成功提交返回")

	// 验证 C 提交后，数据库中确实已存在该 [0, 49] 写锁
	midLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)
	require.Len(t, midLocks, 1, "请求 C 提交后数据库必须且仅存在 C 的 [0, 49] 写锁")
	assert.Equal(t, int64(0), midLocks[0].StartOffset)
	assert.Equal(t, int64(49), midLocks[0].EndOffset)

	// 4. 恢复并放行 B 执行 lastUndo.Rollback()：
	// B 执行 lastUndo.Rollback()，将已经被 C 接管的 [0, 49] 错误补回 A.pendingRanges（变回 [0, 100]）
	releaseBLastUndoRollback()
	select {
	case bErr := <-bErrCh:
		require.ErrorIs(t, bErr, meta.ErrInterrupted, "回滚后的请求 B 必须返回 ErrInterrupted")
	case <-time.After(5 * time.Second):
		t.Fatal("等待请求 B 回滚完成超时")
	}

	// 5. 恢复并放行 A 完成 rollbackGrantedLock：
	// A 因 pendingRanges 被错误恢复为 [0, 100]，在清理阶段会误将 C 已提交的 [0, 49] 锁一并删除
	releaseACleanup()
	select {
	case aErr := <-aErrCh:
		require.ErrorIs(t, aErr, meta.ErrInterrupted, "回滚后的请求 A 必须返回 ErrInterrupted")
	case <-time.After(5 * time.Second):
		t.Fatal("等待请求 A 回滚完成超时")
	}

	// 6. 断言：最终数据库 file_locks 中必须完整保留 C 已成功提交的 [0, 49] 写锁！
	activeLocks, err := baseStore.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)

	require.Len(t, activeLocks, 1, "P-1 缺陷验证：C 已成功提交的 [0, 49] 写锁必须完整保留在数据库中，绝不能因 B 的 Rollback 错误恢复 A 的范围而被 A 取消误删")
	assert.Equal(t, int64(owner), activeLocks[0].LockOwner)
	assert.Equal(t, int64(sess), activeLocks[0].SessionID)
	assert.Equal(t, int16(meta.F_WRLCK), activeLocks[0].LockType, "锁类型必须为写锁 F_WRLCK")
	assert.Equal(t, int64(0), activeLocks[0].StartOffset)
	assert.Equal(t, int64(49), activeLocks[0].EndOffset)
}

func TestSetLkw_Integration_PersistedWaiterCleanedUpOnHandleInvalidBadFileHandle(t *testing.T) {
	pool := setupFormalPostgresPool(t)
	store := db.NewStore(pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. 创建独立测试磁盘与常规文件
	mgrHolder := meta.New(store, meta.WithSessionLeaseTTL(10*time.Second))
	diskName := fmt.Sprintf("setlkw_waiter_handle_disk_%d", time.Now().UnixNano())
	disk, err := mgrHolder.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	sess1 := uint64(time.Now().UnixNano()&0x3fffffffffffffff) + 101
	sess2 := sess1 + 1

	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_ = mgrHolder.DeleteDisk(cleanupCtx, disk.DiskID)
		sids := []int64{int64(sess1), int64(sess2)}
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_lock_waiters WHERE session_id = ANY($1::bigint[])", sids)
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_locks WHERE session_id = ANY($1::bigint[])", sids)
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_handles WHERE session_id = ANY($1::bigint[])", sids)
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id = ANY($1::bigint[])", sids)
	})

	// 会话 sess1 续期注册
	_, err = mgrHolder.RenewSession(ctx, sess1)
	require.NoError(t, err)

	// 在独立磁盘下创建测试文件并为 sess1 打开句柄 fh1
	createRes, err := mgrHolder.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("test_waiter_handle.dat"),
		Mode:      0o644,
		Flags:     meta.O_CREAT | meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh1 := createRes.Fh

	// 2. sess1 成功获取 [0, 100] 排他写锁 F_WRLCK
	err = mgrHolder.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   1,
		},
		LkFlags:   0,
		SessionID: sess1,
	})
	require.NoError(t, err)

	activeLocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)
	require.Len(t, activeLocks, 1, "会话 sess1 必须成功持有 [0, 100] 写锁")
	require.Equal(t, int64(101), activeLocks[0].LockOwner)
	require.Equal(t, int64(0), activeLocks[0].StartOffset)
	require.Equal(t, int64(100), activeLocks[0].EndOffset)

	// 3. 会话 sess2 打开同一文件获得 fh2
	mgrWaiter := meta.New(store, meta.WithSessionLeaseTTL(10*time.Second))
	_, err = mgrWaiter.RenewSession(ctx, sess2)
	require.NoError(t, err)

	openRes, err := mgrWaiter.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sess2,
	})
	require.NoError(t, err)
	fh2 := openRes.Fh

	// 在带 3 秒超时保护的 waitCtx 下调用 SetLkw，发生阻塞等待
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- mgrWaiter.SetLkw(waitCtx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   2,
			},
			LkFlags:   0,
			SessionID: sess2,
			RequestID: 9901,
		})
	}()

	// 证实 sess2 确实首轮阻塞并在底表持久化了 status = PENDING 等待记录
	require.Eventually(t, func() bool {
		var count int64
		qErr := pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2 AND status = $3", int64(sess2), int64(9901), meta.WaiterStatusPending).Scan(&count)
		return qErr == nil && count == 1
	}, 2*time.Second, 10*time.Millisecond, "sess2 的 SetLkw 必须已在 file_lock_waiters 底表持久化 PENDING 记录")

	// 主动释放/销毁句柄 fh2（模拟句柄失效/关闭，例如客户端并发 Release）
	err = mgrWaiter.Release(ctx, meta.ReleaseParams{
		Ino:       fileIno,
		Fh:        fh2,
		SessionID: &sess2,
	})
	require.NoError(t, err)

	// 依赖 SetLkw 自身轮询定时器（最大 50ms）或释放句柄通知自然唤醒重检
	var setLkwErr error
	select {
	case setLkwErr = <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("等待 mgrWaiter.SetLkw 退出超时")
	}

	// 4. 显式断言：
	// mgrWaiter.SetLkw 必须返回错误且为 ErrBadFileHandle
	require.Error(t, setLkwErr)
	require.ErrorIs(t, setLkwErr, meta.ErrBadFileHandle, "SetLkw 在句柄失效重试时必须返回 ErrBadFileHandle")

	// SetLkw 返回后，立即直接查询 PostgreSQL 底表，断言计数必须严格等于 0（零脏数据残留）！
	var remainingWaiters int64
	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2", int64(sess2), int64(9901)).Scan(&remainingWaiters)
	require.NoError(t, err)
	require.Equal(t, int64(0), remainingWaiters, "SetLkw 因句柄失效返回 ErrBadFileHandle 终止退出后，file_lock_waiters 底表中不得残留 PENDING 脏数据")
}

type waiterExpectedFhHookQuerier struct {
	db.Querier
	onGetFileHandle func(ctx context.Context, q db.Querier, params db.GetFileHandleParams) (db.FileHandle, error)
}

func (q *waiterExpectedFhHookQuerier) GetFileHandle(ctx context.Context, params db.GetFileHandleParams) (db.FileHandle, error) {
	if q.onGetFileHandle != nil {
		return q.onGetFileHandle(ctx, q.Querier, params)
	}
	return q.Querier.GetFileHandle(ctx, params)
}

type waiterExpectedFhHookStore struct {
	db.Store
	onGetFileHandle func(ctx context.Context, q db.Querier, params db.GetFileHandleParams) (db.FileHandle, error)
	onExecTx        func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error
}

func (s *waiterExpectedFhHookStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.ExecTxWithOptions(ctx, fn)
}

func (s *waiterExpectedFhHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	wrappedFn := func(q db.Querier) error {
		hookQ := &waiterExpectedFhHookQuerier{
			Querier:         q,
			onGetFileHandle: s.onGetFileHandle,
		}
		return fn(hookQ)
	}
	if s.onExecTx != nil {
		return s.onExecTx(ctx, wrappedFn, opts...)
	}
	return s.Store.ExecTxWithOptions(ctx, wrappedFn, opts...)
}

// TestSetLkw_Integration_CleanupCancelledWaiter_PreservesReplacedHandleOnSameRequestID 验证 Issue #4 / Ch2-5：
// 覆盖同一活跃会话下，旧句柄 fhOld 的 SetLkw 退出执行 cleanupCancelledWaiter(expectedFh = fhOld) 时，
// 同一 (session_id, request_id) 已被该活跃会话的新句柄 fhNew 复用的场景。
// 必须严格触发并保护 cleanupCancelledWaiter 中 if waiter.Fh != expectedFh { return nil } 防误删/防误回滚分支：
// 子场景 A：fhNew 处于 PENDING 等待状态，旧句柄延迟清理绝不误删 fhNew 的等待者记录；sess1 解锁后 fhNew 成功获锁。
// 子场景 B：fhNew 已推进至 GRANTED 并持有排他锁，旧句柄延迟清理绝不误回滚 fhNew 的已授排他锁。
func TestSetLkw_Integration_CleanupCancelledWaiter_PreservesReplacedHandleOnSameRequestID(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	t.Run("SubscenarioA_PendingWaiterPreservedAgainstDelayedOldWaiterCleanup", func(t *testing.T) {
		sess1 := uint64(77301)
		sess2 := uint64(77302)
		reqID := int64(88001)

		cleanTables := func() {
			cleanupCtx := context.Background()
			sids := []int64{int64(sess1), int64(sess2)}
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_lock_waiters WHERE session_id = ANY($1::bigint[])", sids)
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_locks WHERE session_id = ANY($1::bigint[])", sids)
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_handles WHERE session_id = ANY($1::bigint[])", sids)
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id = ANY($1::bigint[])", sids)
		}
		cleanTables()

		rawStore := db.NewStore(pool)
		hookStore := &waiterExpectedFhHookStore{Store: rawStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-waiter-expected-fh-a-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			cleanupCtx := context.Background()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
			cleanTables()
		})

		_, err = mgr.RenewSession(ctx, sess1)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sess2)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("test_waiter_replace_a.dat"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		fh1 := createRes.Fh

		// 1. sess1 获取 [0, 100] 排他写锁 F_WRLCK
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   1,
			},
			LkFlags:   0,
			SessionID: sess1,
		})
		require.NoError(t, err)

		// 2. 活跃长租约会话 sess2 打开同一文件两次获得两个不同句柄 fhOld 与 fhNew
		openResOld, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		fhOld := openResOld.Fh

		openResNew, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		fhNew := openResNew.Fh
		require.NotEqual(t, fhOld, fhNew, "fhOld 与 fhNew 必须是同一会话下的不同句柄")

		// 设置挂起拦截机制：当 fhOld 被 Release 删除后，SetLkw 检测到 ErrBadFileHandle，
		// 在即将进入 cleanupCancelledWaiter 事务前精确暂停
		var (
			pauseBeforeCleanup atomic.Bool
			cleanupPaused      = make(chan struct{})
			cleanupPausedOnce  sync.Once
			resumeCleanup      = make(chan struct{})
		)
		closeResume := sync.OnceFunc(func() { close(resumeCleanup) })
		t.Cleanup(closeResume)

		hookStore.onGetFileHandle = func(ctx context.Context, q db.Querier, params db.GetFileHandleParams) (db.FileHandle, error) {
			handle, hErr := q.GetFileHandle(ctx, params)
			if params.Fh == int64(fhOld) && errors.Is(hErr, pgx.ErrNoRows) {
				pauseBeforeCleanup.Store(true)
			}
			return handle, hErr
		}

		hookStore.onExecTx = func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			if pauseBeforeCleanup.CompareAndSwap(true, false) {
				cleanupPausedOnce.Do(func() {
					close(cleanupPaused)
				})
				select {
				case <-resumeCleanup:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return rawStore.ExecTxWithOptions(ctx, fn, opts...)
		}

		// 3. sess2 使用 (Fh: fhOld, RequestID: reqID) 发起阻塞 SetLkw
		ctxOld, cancelOld := context.WithTimeout(ctx, 10*time.Second)
		defer cancelOld()

		errChOld := make(chan error, 1)
		go func() {
			errChOld <- mgr.SetLkw(ctxOld, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhOld,
				LockOwner: 202,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   2,
				},
				LkFlags:   0,
				SessionID: sess2,
				RequestID: uint64(reqID),
			})
		}()

		// 轮询底表确认 file_lock_waiters 中 (sess2, reqID) 的 fh == fhOld && status == 'PENDING' 已落库
		require.Eventually(t, func() bool {
			var count int64
			qErr := pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2 AND fh = $3 AND status = $4", int64(sess2), reqID, int64(fhOld), meta.WaiterStatusPending).Scan(&count)
			return qErr == nil && count == 1
		}, 2*time.Second, 10*time.Millisecond, "fhOld 必须已在 file_lock_waiters 持久化 PENDING 记录")

		// 4. 调用 mgr.Release 关闭 fhOld
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        fhOld,
			SessionID: &sess2,
		})
		require.NoError(t, err)

		// 等待 fhOld 的 SetLkw 循环检测到句柄关闭并暂停在即将执行 cleanupCancelledWaiter 事务前
		select {
		case <-cleanupPaused:
		case <-time.After(5 * time.Second):
			t.Fatal("等待 fhOld 进入 cleanupCancelledWaiter 挂起超时")
		}

		// 5. 在挂起窗口内，sess2 使用新句柄 (Fh: fhNew, RequestID: reqID) 发起新的 SetLkw
		ctxNew, cancelNew := context.WithTimeout(ctx, 10*time.Second)
		defer cancelNew()

		errChNew := make(chan error, 1)
		go func() {
			errChNew <- mgr.SetLkw(ctxNew, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhNew,
				LockOwner: 202,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   2,
				},
				LkFlags:   0,
				SessionID: sess2,
				RequestID: uint64(reqID),
			})
		}()

		// 轮询底表确认 (sess2, reqID) 现已被更新/写入为 fh == fhNew && status == 'PENDING'
		require.Eventually(t, func() bool {
			var waiterFh int64
			var waiterStatus int16
			qErr := pool.QueryRow(ctx, "SELECT fh, status FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2", int64(sess2), reqID).Scan(&waiterFh, &waiterStatus)
			return qErr == nil && waiterFh == int64(fhNew) && waiterStatus == meta.WaiterStatusPending
		}, 2*time.Second, 10*time.Millisecond, "fhNew 必须已将 (sess2, reqID) 更新/覆盖为 fh == fhNew && status == PENDING")

		// 6. 放行旧请求完成 cleanupCancelledWaiter(..., fhOld)
		closeResume()

		// 等待旧 SetLkw 返回 ErrBadFileHandle
		select {
		case oldErr := <-errChOld:
			require.ErrorIs(t, oldErr, meta.ErrBadFileHandle, "旧句柄 fhOld 的 SetLkw 必须返回 ErrBadFileHandle")
		case <-time.After(5 * time.Second):
			t.Fatal("等待旧句柄 fhOld 的 SetLkw 退出超时")
		}

		// 严格断言：直接查询底表，新句柄 fhNew 的 waiter 记录必须依然存在且 fh == fhNew && status == PENDING
		// （若缺少 lock.go 中的 waiter.Fh != expectedFh 保护，该记录会被旧请求误删，导致断言失败变红！）
		var currentFh int64
		var currentStatus int16
		err = pool.QueryRow(ctx, "SELECT fh, status FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2", int64(sess2), reqID).Scan(&currentFh, &currentStatus)
		require.NoError(t, err, "新句柄 fhNew 的 waiter 记录必须依然存在，绝不能被旧句柄 fhOld 误删")
		require.Equal(t, int64(fhNew), currentFh, "waiter 记录的 fh 必须依然是 fhNew")
		require.Equal(t, meta.WaiterStatusPending, currentStatus, "waiter 记录的状态必须依然是 PENDING")

		// 7. 释放 sess1 的写锁，断言 fhNew 的 SetLkw 正常被唤醒并成功获锁返回 NoError（有效成功样本 > 0）
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_UNLCK,
				PID:   1,
			},
			LkFlags:   0,
			SessionID: sess1,
		})
		require.NoError(t, err)

		select {
		case newErr := <-errChNew:
			require.NoError(t, newErr, "fhNew 的 SetLkw 在 sess1 解锁后必须被正常唤醒并成功获锁返回 NoError")
		case <-time.After(5 * time.Second):
			t.Fatal("等待 fhNew 的 SetLkw 获锁超时")
		}

		// 验证底表 file_locks 确实持久化了 sess2 在 fhNew 上的排他锁
		var lockCount int64
		err = pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1 AND ino = $2 AND fh = $3 AND lock_owner = $4", int64(sess2), int64(fileIno), int64(fhNew), int64(202)).Scan(&lockCount)
		require.NoError(t, err)
		require.Equal(t, int64(1), lockCount, "sess2 在 fhNew 上必须持有排他锁")
	})

	t.Run("SubscenarioB_GrantedLockPreservedAgainstDelayedOldWaiterCleanup", func(t *testing.T) {
		sess1 := uint64(77303)
		sess2 := uint64(77304)
		reqID := int64(88002)

		cleanTables := func() {
			cleanupCtx := context.Background()
			sids := []int64{int64(sess1), int64(sess2)}
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_lock_waiters WHERE session_id = ANY($1::bigint[])", sids)
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_locks WHERE session_id = ANY($1::bigint[])", sids)
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_handles WHERE session_id = ANY($1::bigint[])", sids)
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id = ANY($1::bigint[])", sids)
		}
		cleanTables()

		rawStore := db.NewStore(pool)
		hookStore := &waiterExpectedFhHookStore{Store: rawStore}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-waiter-expected-fh-b-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			cleanupCtx := context.Background()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
			cleanTables()
		})

		_, err = mgr.RenewSession(ctx, sess1)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sess2)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("test_waiter_replace_b.dat"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		fh1 := createRes.Fh

		// 1. sess1 获取 [0, 100] 排他写锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   1,
			},
			LkFlags:   0,
			SessionID: sess1,
		})
		require.NoError(t, err)

		// 2. sess2 打开同一文件两次获得 fhOld 与 fhNew
		openResOld, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		fhOld := openResOld.Fh

		openResNew, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		fhNew := openResNew.Fh
		require.NotEqual(t, fhOld, fhNew)

		var (
			pauseBeforeCleanup atomic.Bool
			cleanupPaused      = make(chan struct{})
			cleanupPausedOnce  sync.Once
			resumeCleanup      = make(chan struct{})
		)
		closeResume := sync.OnceFunc(func() { close(resumeCleanup) })
		t.Cleanup(closeResume)

		hookStore.onGetFileHandle = func(ctx context.Context, q db.Querier, params db.GetFileHandleParams) (db.FileHandle, error) {
			handle, hErr := q.GetFileHandle(ctx, params)
			if params.Fh == int64(fhOld) && errors.Is(hErr, pgx.ErrNoRows) {
				pauseBeforeCleanup.Store(true)
			}
			return handle, hErr
		}

		hookStore.onExecTx = func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			if pauseBeforeCleanup.CompareAndSwap(true, false) {
				cleanupPausedOnce.Do(func() {
					close(cleanupPaused)
				})
				select {
				case <-resumeCleanup:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return rawStore.ExecTxWithOptions(ctx, fn, opts...)
		}

		// 3. sess2 使用 (fhOld, reqID) 发起阻塞 SetLkw
		ctxOld, cancelOld := context.WithTimeout(ctx, 10*time.Second)
		defer cancelOld()

		errChOld := make(chan error, 1)
		go func() {
			errChOld <- mgr.SetLkw(ctxOld, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fhOld,
				LockOwner: 202,
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   2,
				},
				LkFlags:   0,
				SessionID: sess2,
				RequestID: uint64(reqID),
			})
		}()

		require.Eventually(t, func() bool {
			var count int64
			qErr := pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2 AND fh = $3 AND status = $4", int64(sess2), reqID, int64(fhOld), meta.WaiterStatusPending).Scan(&count)
			return qErr == nil && count == 1
		}, 2*time.Second, 10*time.Millisecond, "fhOld 必须已持久化 PENDING 记录")

		// 4. 调用 mgr.Release 关闭 fhOld 并等待进入 cleanupCancelledWaiter 挂起
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        fhOld,
			SessionID: &sess2,
		})
		require.NoError(t, err)

		select {
		case <-cleanupPaused:
		case <-time.After(5 * time.Second):
			t.Fatal("等待 fhOld 进入 cleanupCancelledWaiter 挂起超时")
		}

		// 5. 在挂起窗口内，sess1 释放锁，sess2 使用新句柄 (Fh: fhNew, RequestID: reqID) 成功获锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_UNLCK,
				PID:   1,
			},
			LkFlags:   0,
			SessionID: sess1,
		})
		require.NoError(t, err)

		setLkwErr := mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhNew,
			LockOwner: 202,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   2,
			},
			LkFlags:   0,
			SessionID: sess2,
			RequestID: uint64(reqID),
		})
		require.NoError(t, setLkwErr, "新句柄 fhNew 的 SetLkw 必须成功获锁")

		// 将 (sess2, reqID) 在 file_lock_waiters 中设置为 status = GRANTED (2) 且 fh = fhNew
		_, err = pool.Exec(ctx, `
			INSERT INTO file_lock_waiters (session_id, request_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid, status)
			VALUES ($1, $2, $3, $4, 202, 0, 0, 100, 1, 2, $5)
			ON CONFLICT (session_id, request_id) DO UPDATE SET
				fh = EXCLUDED.fh,
				status = EXCLUDED.status,
				lock_owner = EXCLUDED.lock_owner,
				start_offset = EXCLUDED.start_offset,
				end_offset = EXCLUDED.end_offset,
				lock_type = EXCLUDED.lock_type
		`, int64(sess2), reqID, int64(fileIno), int64(fhNew), meta.WaiterStatusGranted)
		require.NoError(t, err)

		// 确认底表状态：fhNew 持有排他锁且 waiter status 为 GRANTED
		var preLockCount int64
		err = pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1 AND ino = $2 AND fh = $3 AND lock_owner = $4", int64(sess2), int64(fileIno), int64(fhNew), int64(202)).Scan(&preLockCount)
		require.NoError(t, err)
		require.Equal(t, int64(1), preLockCount, "sess2 在 fhNew 上必须持有排他锁")

		var preWaiterCount int64
		err = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2 AND fh = $3 AND status = $4", int64(sess2), reqID, int64(fhNew), meta.WaiterStatusGranted).Scan(&preWaiterCount)
		require.NoError(t, err)
		require.Equal(t, int64(1), preWaiterCount, "file_lock_waiters 必须为 fhNew 且 status 为 GRANTED")

		// 6. 放行旧请求执行 cleanupCancelledWaiter(..., fhOld)
		closeResume()

		select {
		case oldErr := <-errChOld:
			require.ErrorIs(t, oldErr, meta.ErrBadFileHandle, "旧句柄 fhOld 的 SetLkw 必须返回 ErrBadFileHandle")
		case <-time.After(5 * time.Second):
			t.Fatal("等待旧句柄 fhOld 的 SetLkw 退出超时")
		}

		// 严格断言：底表 file_locks 中 sess2 的排他写锁依然完好存在（计数为 1），绝未被旧句柄误回滚！
		// （若缺少 waiter.Fh != expectedFh 分支保护，cleanupCancelledWaiter 会将 waiter.Status == GRANTED 误识别为自身已授锁，
		// 调用 applyPosixUnlock 误删除 fhNew 的排他锁，导致测试变红！）
		var finalLockCount int64
		err = pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1 AND ino = $2 AND fh = $3 AND lock_owner = $4", int64(sess2), int64(fileIno), int64(fhNew), int64(202)).Scan(&finalLockCount)
		require.NoError(t, err)
		require.Equal(t, int64(1), finalLockCount, "Subscenario B: 底表 file_locks 中 sess2 的排他写锁必须依然完好存在，未被误回滚")

		// waiter 记录未被旧句柄误删
		var finalWaiterCount int64
		err = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = $2 AND fh = $3 AND status = $4", int64(sess2), reqID, int64(fhNew), meta.WaiterStatusGranted).Scan(&finalWaiterCount)
		require.NoError(t, err)
		require.Equal(t, int64(1), finalWaiterCount, "Subscenario B: waiter 记录必须依然存在且 fh == fhNew")
	})
}
