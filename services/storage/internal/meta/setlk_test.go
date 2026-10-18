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

func TestSetLk_InputValidation(t *testing.T) {
	ctx := context.Background()

	validBase := meta.SetLkParams{
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
	}

	testCases := []struct {
		name   string
		mutate func(p *meta.SetLkParams)
	}{
		{
			name:   "ino 为 0",
			mutate: func(p *meta.SetLkParams) { p.Ino = 0 },
		},
		{
			name:   "ino 超过 math.MaxInt64",
			mutate: func(p *meta.SetLkParams) { p.Ino = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "fh 为 0",
			mutate: func(p *meta.SetLkParams) { p.Fh = 0 },
		},
		{
			name:   "fh 超过 math.MaxInt64",
			mutate: func(p *meta.SetLkParams) { p.Fh = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "session_id 为 0",
			mutate: func(p *meta.SetLkParams) { p.SessionID = 0 },
		},
		{
			name:   "session_id 超过 math.MaxInt64",
			mutate: func(p *meta.SetLkParams) { p.SessionID = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "lk_flags 非法 (> FuseLkFlock)",
			mutate: func(p *meta.SetLkParams) { p.LkFlags = 2 },
		},
		{
			name:   "lk.Type 非法 (> F_UNLCK)",
			mutate: func(p *meta.SetLkParams) { p.Lk.Type = 3 },
		},
		{
			name: "lk.Start > lk.End",
			mutate: func(p *meta.SetLkParams) {
				p.Lk.Start = 200
				p.Lk.End = 100
			},
		},
		{
			name: "lk.Start 超过 OffsetMax",
			mutate: func(p *meta.SetLkParams) {
				p.Lk.Start = meta.OffsetMax + 1
				p.Lk.End = meta.OffsetMax + 1
			},
		},
		{
			name:   "lk.End 超过 OffsetMax",
			mutate: func(p *meta.SetLkParams) { p.Lk.End = meta.OffsetMax + 1 },
		},
		{
			name: "FUSE_LK_FLOCK 但 start != 0",
			mutate: func(p *meta.SetLkParams) {
				p.LkFlags = meta.FuseLkFlock
				p.Lk.Start = 10
				p.Lk.End = meta.OffsetMax
			},
		},
		{
			name: "FUSE_LK_FLOCK 但 end != OffsetMax",
			mutate: func(p *meta.SetLkParams) {
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

			err := mgr.SetLk(ctx, p)
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestSetLk_InodeAndOpenModeValidation(t *testing.T) {
	ctx := context.Background()
	ino := uint64(300)
	fh := uint64(5)

	validParams := meta.SetLkParams{
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

		err := mgr.SetLk(ctx, validParams)
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

		err := mgr.SetLk(ctx, validParams)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("句柄不存在或归属其他节点返回 ErrBadFileHandle", func(t *testing.T) {
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
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		err := mgr.SetLk(ctx, validParams)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("POSIX 锁在只写 FileHandle (O_WRONLY) 上申请读锁 (F_RDLCK) 返回 ErrBadFileHandle", func(t *testing.T) {
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
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_APPEND),
			}, nil).
			Once()

		p := validParams
		p.Lk.Type = meta.F_RDLCK
		err := mgr.SetLk(ctx, p)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
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
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		p := validParams
		p.Lk.Type = meta.F_WRLCK
		err := mgr.SetLk(ctx, p)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})
}

func TestSetLk_PosixLockAndUnlock(t *testing.T) {
	ctx := context.Background()
	ino := uint64(500)
	fh := uint64(15)

	baseInode := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
	}

	t.Run("只读句柄 (O_RDONLY) 申请共享读锁 (F_RDLCK) 在其他持有者已持读锁时成功获锁并续期租约", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		customTTL := 30 * time.Second
		mgr := meta.New(mockStore, meta.WithSessionLeaseTTL(customTTL))

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(nil).
			Once()

		beforeCall := time.Now()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == 10 &&
					arg.ExpiresAt.Valid &&
					arg.ExpiresAt.Time.After(beforeCall.Add(25*time.Second)) &&
					arg.ExpiresAt.Time.Before(beforeCall.Add(35*time.Second))
			})).
			Return(db.ClientSession{SessionID: 10}, nil).
			Once()

		// 已有另一持有者 (session=20, owner=2002) 在重叠区间 [0, 200] 持有 F_RDLCK，读读不冲突
		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          1,
					SessionID:   20,
					Ino:         int64(ino),
					Fh:          99,
					LockOwner:   2002,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   200,
					LockType:    int16(meta.F_RDLCK),
					Pid:         8888,
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListPendingFileLockWaitersByInoAndFlags(mock.Anything, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return(nil, nil).
			Once()

		highBitOwner := uint64(0x9000000000000001)
		highBitPID := uint32(math.MaxInt32) + 42
		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   int64(highBitOwner),
				LkFlags:     0,
				StartOffset: 50,
				EndOffset:   150,
				LockType:    int16(meta.F_RDLCK),
				Pid:         int32(highBitPID),
			}).
			Return(db.FileLock{ID: 2}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: highBitOwner,
			Lk: meta.FileLock{
				Start: 50,
				End:   150,
				Type:  meta.F_RDLCK,
				PID:   highBitPID,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.NoError(t, err)
	})

	t.Run("申请写锁 (F_WRLCK) 遇异主读锁冲突立即返回 ErrAgain", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 10}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          1,
					SessionID:   20,
					Ino:         int64(ino),
					Fh:          99,
					LockOwner:   2002,
					LkFlags:     0,
					StartOffset: 100,
					EndOffset:   300,
					LockType:    int16(meta.F_RDLCK),
					Pid:         8888,
				},
			}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 0,
				End:   150,
				Type:  meta.F_WRLCK,
				PID:   1234,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrAgain))
	})

	t.Run("申请读锁 (F_RDLCK) 遇异主写锁冲突立即返回 ErrAgain", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 10}, nil).
			Once()

		// 同一 lock_owner 但不同 session_id 属于异主，写锁冲突返回 ErrAgain
		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          1,
					SessionID:   20,
					Ino:         int64(ino),
					Fh:          99,
					LockOwner:   1001,
					LkFlags:     0,
					StartOffset: 100,
					EndOffset:   300,
					LockType:    int16(meta.F_WRLCK),
					Pid:         8888,
				},
			}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 100,
				End:   200,
				Type:  meta.F_RDLCK,
				PID:   1234,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrAgain))
	})

	t.Run("F_UNLCK 在只读或只写合法句柄及 Orphan Inode 上均成功执行且仅清理同持有者重叠锁", func(t *testing.T) {
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

		// 即使该锁曾是写锁，通过只读句柄 (O_RDONLY) 执行 F_UNLCK 也必须成功
		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 10}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          11,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   1001,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   100,
					LockType:    int16(meta.F_WRLCK),
					Pid:         1234,
				},
				{
					ID:          12,
					SessionID:   20,
					Ino:         int64(ino),
					Fh:          99,
					LockOwner:   2002,
					LkFlags:     0,
					StartOffset: 200,
					EndOffset:   300,
					LockType:    int16(meta.F_WRLCK),
					Pid:         5678,
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileLocksByIDs(mock.Anything, []int64{11}).
			Return(nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_UNLCK,
				PID:   1234,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.NoError(t, err)
	})
}

func TestSetLk_PostgresSemantics(t *testing.T) {
	dsn := os.Getenv("STORAGE_DB_DSN")
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
		Name:      []byte("setlk_posix_test.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fhRDWR := createRes.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(fileIno))
	}()

	openRO, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   fileIno,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)
	fhRO := openRO.Fh

	openWO, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   fileIno,
		Flags: meta.O_WRONLY,
	})
	require.NoError(t, err)
	fhWO := openWO.Fh

	session1 := uint64(920001)
	session2 := uint64(920002)
	expiredSession := int64(920003)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2, $3)", int64(session1), int64(session2), expiredSession)
	}()

	// 1. 对目录 Inode 调用 SetLk 返回 ErrIsDir
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       uint64(rootDir.Ino),
		Fh:        fhRDWR,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_RDLCK,
			PID:   1111,
		},
		LkFlags:   0,
		SessionID: session1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrIsDir))

	// 2. 打开模式位校验：O_WRONLY 申 F_RDLCK 与 O_RDONLY 申 F_WRLCK 均返回 ErrBadFileHandle
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhWO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_RDLCK,
			PID:   1111,
		},
		LkFlags:   0,
		SessionID: session1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   1111,
		},
		LkFlags:   0,
		SessionID: session1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

	// 3. 超期会话预先持有排他锁 [0, 500]，SetLk 内联驱逐超期会话并成功加锁 + 续期当前 session
	_, err = store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: expiredSession,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(-10 * time.Second), Valid: true},
	})
	require.NoError(t, err)

	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   expiredSession,
		Ino:         int64(fileIno),
		Fh:          int64(fhRDWR),
		LockOwner:   9999,
		LkFlags:     0,
		StartOffset: 0,
		EndOffset:   500,
		LockType:    int16(meta.F_WRLCK),
		Pid:         9999,
	})
	require.NoError(t, err)

	beforeInode, err := mgr.GetAttr(ctx, fileIno)
	require.NoError(t, err)

	// 持有者 1 (session1, owner=1001) 在 [0, 200] 申请 F_RDLCK 成功，驱逐 expiredSession
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   200,
			Type:  meta.F_RDLCK,
			PID:   1111,
		},
		LkFlags:   0,
		SessionID: session1,
	})
	require.NoError(t, err)

	var expiredCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM client_sessions WHERE session_id = $1", expiredSession).Scan(&expiredCount)
	require.NoError(t, err)
	assert.Equal(t, 0, expiredCount, "expired session must be lazily evicted by SetLk")

	// 4. 多个不同 (session_id, lock_owner) 在同一区间并发持有共享读锁 (F_RDLCK)
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: 2002,
		Lk: meta.FileLock{
			Start: 50,
			End:   250,
			Type:  meta.F_RDLCK,
			PID:   2222,
		},
		LkFlags:   0,
		SessionID: session2,
	})
	require.NoError(t, err)

	// 5. 写锁 (F_WRLCK) 与异主读锁冲突返回 ErrAgain
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhWO,
		LockOwner: 3003,
		Lk: meta.FileLock{
			Start: 100,
			End:   150,
			Type:  meta.F_WRLCK,
			PID:   3333,
		},
		LkFlags:   0,
		SessionID: session2,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrAgain))

	// 6. F_UNLCK 在任意合法 FileHandle (包括 fhWO 只写句柄解锁读锁) 上均可成功执行
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhWO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   200,
			Type:  meta.F_UNLCK,
			PID:   1111,
		},
		LkFlags:   0,
		SessionID: session1,
	})
	require.NoError(t, err)

	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRO,
		LockOwner: 2002,
		Lk: meta.FileLock{
			Start: 50,
			End:   250,
			Type:  meta.F_UNLCK,
			PID:   2222,
		},
		LkFlags:   0,
		SessionID: session2,
	})
	require.NoError(t, err)

	// 7. 并发写锁竞争：READ COMMITTED + GetInodeForUpdate(ino) 串行化，仅一个成功，其余返回 ErrAgain
	const workers = 6
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var againCount atomic.Int32

	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wErr := mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       fileIno,
				Fh:        fhRDWR,
				LockOwner: uint64(5000 + idx),
				Lk: meta.FileLock{
					Start: 0,
					End:   1000,
					Type:  meta.F_WRLCK,
					PID:   uint32(5000 + idx),
				},
				LkFlags:   0,
				SessionID: session1,
			})
			if wErr == nil {
				successCount.Add(1)
			} else if errors.Is(wErr, meta.ErrAgain) {
				againCount.Add(1)
			} else {
				t.Errorf("unexpected error in concurrent SetLk: %v", wErr)
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int32(1), successCount.Load(), "exactly one concurrent exclusive lock should succeed")
	assert.Equal(t, int32(workers-1), againCount.Load(), "all other concurrent exclusive locks should get ErrAgain")

	// 8. Unlink 使文件成为 Orphan Inode (nlink == 0 且持有活跃 FileHandle)，验证孤儿节点正常加解锁且时间戳严格不变
	err = mgr.Unlink(ctx, uint64(rootDir.Ino), []byte("setlk_posix_test.txt"))
	require.NoError(t, err)

	orphanBefore, err := mgr.GetAttr(ctx, fileIno)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), orphanBefore.Attr.Nlink)

	// 清理刚才并发获胜者的锁，并在孤儿节点上加新写锁与解锁
	for i := range workers {
		_ = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhRO,
			LockOwner: uint64(5000 + i),
			Lk: meta.FileLock{
				Start: 0,
				End:   1000,
				Type:  meta.F_UNLCK,
				PID:   uint32(5000 + i),
			},
			LkFlags:   0,
			SessionID: session1,
		})
	}

	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhWO,
		LockOwner: 8888,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_WRLCK,
			PID:   8888,
		},
		LkFlags:   0,
		SessionID: session1,
	})
	require.NoError(t, err)

	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRO,
		LockOwner: 8888,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_UNLCK,
			PID:   8888,
		},
		LkFlags:   0,
		SessionID: session1,
	})
	require.NoError(t, err)

	orphanAfter, err := mgr.GetAttr(ctx, fileIno)
	require.NoError(t, err)
	assert.Equal(t, beforeInode.Attr.Atime, orphanAfter.Attr.Atime)
	assert.Equal(t, beforeInode.Attr.Mtime, orphanAfter.Attr.Mtime)
	assert.Equal(t, orphanBefore.Attr.Ctime, orphanAfter.Attr.Ctime)
}



