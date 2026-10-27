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

func TestUnitTestSuiteAudit_BlockingSetLkwMissingContextTimeout(t *testing.T) {
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
				if xIdent, ok := sel.X.(*ast.Ident); ok && xIdent.Name == "tinyMgr" {
					return true
				}
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
