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

func TestGetLk_InputValidation(t *testing.T) {
	ctx := context.Background()

	validBase := meta.GetLkParams{
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
		mutate func(p *meta.GetLkParams)
	}{
		{
			name:   "ino 为 0",
			mutate: func(p *meta.GetLkParams) { p.Ino = 0 },
		},
		{
			name:   "ino 超过 math.MaxInt64",
			mutate: func(p *meta.GetLkParams) { p.Ino = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "fh 为 0",
			mutate: func(p *meta.GetLkParams) { p.Fh = 0 },
		},
		{
			name:   "fh 超过 math.MaxInt64",
			mutate: func(p *meta.GetLkParams) { p.Fh = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "session_id 为 0",
			mutate: func(p *meta.GetLkParams) { p.SessionID = 0 },
		},
		{
			name:   "session_id 超过 math.MaxInt64",
			mutate: func(p *meta.GetLkParams) { p.SessionID = uint64(math.MaxInt64) + 1 },
		},
		{
			name:   "lk_flags 非法 (> FuseLkFlock)",
			mutate: func(p *meta.GetLkParams) { p.LkFlags = 2 },
		},
		{
			name:   "lk.Type 为 F_UNLCK",
			mutate: func(p *meta.GetLkParams) { p.Lk.Type = meta.F_UNLCK },
		},
		{
			name:   "lk.Type 非法 (> F_UNLCK)",
			mutate: func(p *meta.GetLkParams) { p.Lk.Type = 3 },
		},
		{
			name: "lk.Start > lk.End",
			mutate: func(p *meta.GetLkParams) {
				p.Lk.Start = 200
				p.Lk.End = 100
			},
		},
		{
			name: "lk.Start 超过 OffsetMax",
			mutate: func(p *meta.GetLkParams) {
				p.Lk.Start = meta.OffsetMax + 1
				p.Lk.End = meta.OffsetMax + 1
			},
		},
		{
			name:   "lk.End 超过 OffsetMax",
			mutate: func(p *meta.GetLkParams) { p.Lk.End = meta.OffsetMax + 1 },
		},
		{
			name: "FUSE_LK_FLOCK 但 start != 0",
			mutate: func(p *meta.GetLkParams) {
				p.LkFlags = meta.FuseLkFlock
				p.Lk.Start = 10
				p.Lk.End = meta.OffsetMax
			},
		},
		{
			name: "FUSE_LK_FLOCK 但 end != OffsetMax",
			mutate: func(p *meta.GetLkParams) {
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

			res, err := mgr.GetLk(ctx, p)
			require.Error(t, err)
			assert.Nil(t, res)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestGetLk_InodeAndHandleValidation(t *testing.T) {
	ctx := context.Background()
	ino := uint64(300)
	fh := uint64(5)

	validParams := meta.GetLkParams{
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

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		res, err := mgr.GetLk(ctx, validParams)
		require.Error(t, err)
		assert.Nil(t, res)
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

		res, err := mgr.GetLk(ctx, validParams)
		require.Error(t, err)
		assert.Nil(t, res)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("句柄不存在或归属其他节点返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 1, Valid: true},
			}).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		res, err := mgr.GetLk(ctx, validParams)
		require.Error(t, err)
		assert.Nil(t, res)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		expectedErr := errors.New("db connection error")
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, expectedErr).
			Once()

		res, err := mgr.GetLk(ctx, validParams)
		require.Error(t, err)
		assert.Nil(t, res)
		assert.True(t, errors.Is(err, expectedErr))
	})
}

func TestGetLk_ConflictDetection(t *testing.T) {
	ctx := context.Background()
	ino := uint64(400)
	fh := uint64(10)

	baseInode := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
	}

	t.Run("POSIX 锁无冲突时返回 F_UNLCK 且保留请求区间、PID 置 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		// 即使句柄以 O_RDONLY 打开且探测 F_WRLCK，GetLk 也不限制打开模式位
		roHandle := db.FileHandle{
			Fh:    int64(fh),
			Ino:   int64(ino),
			Flags: int32(meta.O_RDONLY),
		}

		// 测试最高位为 1 的 64 位无符号 lock_owner
		highBitOwner := uint64(0x8000000000000042)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 77, Valid: true},
			}).
			Return(roHandle, nil).
			Once()

		mockQuerier.EXPECT().
			GetFirstConflictingFileLock(mock.Anything, db.GetFirstConflictingFileLockParams{
				Ino:         int64(ino),
				LkFlags:     0,
				StartOffset: 100,
				EndOffset:   500,
				ReqType:     int16(meta.F_WRLCK),
				SessionID:   77,
				LockOwner:   int64(highBitOwner),
				Fh:          int64(fh),
			}).
			Return(db.FileLock{}, pgx.ErrNoRows).
			Once()

		res, err := mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: highBitOwner,
			Lk: meta.FileLock{
				Start: 100,
				End:   500,
				Type:  meta.F_WRLCK,
				PID:   9876,
			},
			LkFlags:   0,
			SessionID: 77,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, &meta.FileLock{
			Start: 100,
			End:   500,
			Type:  meta.F_UNLCK,
			PID:   0,
		}, res)
	})

	t.Run("POSIX 锁存在冲突时返回首个冲突锁详情 (含持锁者 PID)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		// 句柄以 O_WRONLY 打开，探测 F_RDLCK 同样允许
		woHandle := db.FileHandle{
			Fh:    int64(fh),
			Ino:   int64(ino),
			Flags: int32(meta.O_WRONLY),
		}

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 1, Valid: true},
			}).
			Return(woHandle, nil).
			Once()

		mockQuerier.EXPECT().
			GetFirstConflictingFileLock(mock.Anything, db.GetFirstConflictingFileLockParams{
				Ino:         int64(ino),
				LkFlags:     0,
				StartOffset: 0,
				EndOffset:   1000,
				ReqType:     int16(meta.F_RDLCK),
				SessionID:   1,
				LockOwner:   1001,
				Fh:          int64(fh),
			}).
			Return(db.FileLock{
				ID:          9,
				SessionID:   2,
				Ino:         int64(ino),
				Fh:          99,
				LockOwner:   2002,
				LkFlags:     0,
				StartOffset: 200,
				EndOffset:   600,
				LockType:    int16(meta.F_WRLCK),
				Pid:         4321,
			}, nil).
			Once()

		res, err := mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 0,
				End:   1000,
				Type:  meta.F_RDLCK,
				PID:   1111,
			},
			LkFlags:   0,
			SessionID: 1,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, &meta.FileLock{
			Start: 200,
			End:   600,
			Type:  meta.F_WRLCK,
			PID:   4321,
		}, res)
	})

	t.Run("BSD flock 整文件锁在孤儿节点 (nlink == 0) 上正常探测冲突", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 10, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFirstConflictingFileLock(mock.Anything, db.GetFirstConflictingFileLockParams{
				Ino:         int64(ino),
				LkFlags:     int32(meta.FuseLkFlock),
				StartOffset: 0,
				EndOffset:   int64(meta.OffsetMax),
				ReqType:     int16(meta.F_WRLCK),
				SessionID:   10,
				LockOwner:   0,
				Fh:          int64(fh),
			}).
			Return(db.FileLock{
				ID:          15,
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh + 1),
				LockOwner:   0,
				LkFlags:     int32(meta.FuseLkFlock),
				StartOffset: 0,
				EndOffset:   int64(meta.OffsetMax),
				LockType:    int16(meta.F_RDLCK),
				Pid:         8888,
			}, nil).
			Once()

		res, err := mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 0,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   7777,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, &meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_RDLCK,
			PID:   8888,
		}, res)
	})
}

func TestGetLk_PostgresSemantics(t *testing.T) {
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

	// 创建测试父目录与文件
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
		Name:      []byte("lock_test.txt"),
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
	activeSession1 := int64(910001)
	activeSession2 := int64(910002)
	expiredSession := int64(910003)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2, $3)", activeSession1, activeSession2, expiredSession)
	}()

	_, err = store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: activeSession1,
		ExpiresAt: pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
	})
	require.NoError(t, err)

	_, err = store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: activeSession2,
		ExpiresAt: pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
	})
	require.NoError(t, err)

	actSess1 := uint64(activeSession1)
	openRes2, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDONLY,
		SessionID: &actSess1,
	})
	require.NoError(t, err)
	fh2 := openRes2.Fh

	actSess2 := uint64(activeSession2)
	openRes3, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDONLY,
		SessionID: &actSess2,
	})
	require.NoError(t, err)
	fh3 := openRes3.Fh

	_, err = store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: expiredSession,
		ExpiresAt: pgtype.Timestamptz{Time: now.Add(-time.Minute), Valid: true},
	})
	require.NoError(t, err)

	// 1. 超期会话持有的冲突写锁 [0, 1000] 应被 GetLk 自动忽略
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   expiredSession,
		Ino:         int64(fileIno),
		Fh:          int64(fh1),
		LockOwner:   5000,
		LkFlags:     0,
		StartOffset: 0,
		EndOffset:   1000,
		LockType:    int16(meta.F_WRLCK),
		Pid:         9999,
	})
	require.NoError(t, err)

	res, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   500,
			Type:  meta.F_WRLCK,
			PID:   1111,
		},
		LkFlags:   0,
		SessionID: uint64(activeSession1),
	})
	require.NoError(t, err)
	assert.Equal(t, meta.F_UNLCK, res.Type)

	// 2. 插入两个活跃冲突锁：[200, 400] 与 [100, 300]，验证按 start_offset ASC, end_offset ASC, id ASC 确定性返回 [100, 300]
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   activeSession2,
		Ino:         int64(fileIno),
		Fh:          int64(fh1),
		LockOwner:   2002,
		LkFlags:     0,
		StartOffset: 200,
		EndOffset:   400,
		LockType:    int16(meta.F_WRLCK),
		Pid:         2222,
	})
	require.NoError(t, err)

	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   activeSession2,
		Ino:         int64(fileIno),
		Fh:          int64(fh1),
		LockOwner:   2003,
		LkFlags:     0,
		StartOffset: 100,
		EndOffset:   300,
		LockType:    int16(meta.F_WRLCK),
		Pid:         3333,
	})
	require.NoError(t, err)

	// 同一持有者 (activeSession2, 2003) 探测 [100, 300] 不与自身冲突，但会与 2002 的 [200, 400] 冲突
	resSelf, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fh3,
		LockOwner: 2003,
		Lk: meta.FileLock{
			Start: 100,
			End:   300,
			Type:  meta.F_WRLCK,
			PID:   3333,
		},
		LkFlags:   0,
		SessionID: uint64(activeSession2),
	})
	require.NoError(t, err)
	assert.Equal(t, &meta.FileLock{
		Start: 200,
		End:   400,
		Type:  meta.F_WRLCK,
		PID:   2222,
	}, resSelf)

	// 第三者 (activeSession1, 1001) 探测 [0, 500] 确定性返回 [100, 300]
	resOther, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   500,
			Type:  meta.F_RDLCK,
			PID:   1111,
		},
		LkFlags:   0,
		SessionID: uint64(activeSession1),
	})
	require.NoError(t, err)
	assert.Equal(t, &meta.FileLock{
		Start: 100,
		End:   300,
		Type:  meta.F_WRLCK,
		PID:   3333,
	}, resOther)

	// 3. 验证 POSIX 锁 (lk_flags = 0) 与 BSD flock (lk_flags = FuseLkFlock) 正交隔离：探测 flock 返回 F_UNLCK
	resFlock, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 0,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_WRLCK,
			PID:   1111,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: uint64(activeSession1),
	})
	require.NoError(t, err)
	assert.Equal(t, &meta.FileLock{
		Start: 0,
		End:   meta.OffsetMax,
		Type:  meta.F_UNLCK,
		PID:   0,
	}, resFlock)

	// 4. 验证删除 FileHandle (fh1) 不会级联删除 file_locks 中的 POSIX 锁，且 Inode 时间戳完全不变
	beforeInode, err := mgr.GetAttr(ctx, fileIno)
	require.NoError(t, err)

	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino: fileIno,
		Fh:  fh1,
	})
	require.NoError(t, err)

	resAfterRelease, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   500,
			Type:  meta.F_RDLCK,
			PID:   1111,
		},
		LkFlags:   0,
		SessionID: uint64(activeSession1),
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(3333), resAfterRelease.PID)

	afterInode, err := mgr.GetAttr(ctx, fileIno)
	require.NoError(t, err)
	assert.Equal(t, beforeInode.Attr.Atime, afterInode.Attr.Atime)
	assert.Equal(t, beforeInode.Attr.Mtime, afterInode.Attr.Mtime)
	assert.Equal(t, beforeInode.Attr.Ctime, afterInode.Attr.Ctime)
}
