package meta_test

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestSetLk_FlockUnitSemantics(t *testing.T) {
	ctx := context.Background()
	ino := uint64(600)
	fh1 := uint64(21)
	fh2 := uint64(22)

	baseInode := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
	}

	t.Run("只读句柄 (O_RDONLY) 申请排他 flock (F_WRLCK) 成功获取锁，不触发 EBADF", func(t *testing.T) {
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
				Fh:        int64(fh1),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 10, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh1),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 10}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFlockFileLocksBySessionAndFh(mock.Anything, db.DeleteFlockFileLocksBySessionAndFhParams{
				SessionID: 10,
				Ino:       int64(ino),
				Fh:        int64(fh1),
			}).
			Return(0, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: int32(meta.FuseLkFlock),
			}).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			ListPendingFileLockWaitersByInoAndFlags(mock.Anything, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: int32(meta.FuseLkFlock),
			}).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh1),
				LockOwner:   1001,
				LkFlags:     int32(meta.FuseLkFlock),
				StartOffset: 0,
				EndOffset:   int64(meta.OffsetMax),
				LockType:    int16(meta.F_WRLCK),
				Pid:         4321,
			}).
			Return(db.FileLock{ID: 1}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh1,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   4321,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: 10,
		})
		require.NoError(t, err)
	})

	t.Run("同一 session_id 与同一 lock_owner 下不同 FileHandle 竞争排他 flock 判定冲突", func(t *testing.T) {
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
				Fh:        int64(fh2),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 10, Valid: true},
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
			Return(db.ClientSession{SessionID: 10}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFlockFileLocksBySessionAndFh(mock.Anything, db.DeleteFlockFileLocksBySessionAndFhParams{
				SessionID: 10,
				Ino:       int64(ino),
				Fh:        int64(fh2),
			}).
			Return(0, nil).
			Once()

		// fh1 (21) 与 fh2 (22) 属于同一 session_id=10 和 lock_owner=1001，但 fh 不同，因此冲突
		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: int32(meta.FuseLkFlock),
			}).
			Return([]db.FileLock{
				{
					ID:          10,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh1),
					LockOwner:   1001,
					LkFlags:     int32(meta.FuseLkFlock),
					StartOffset: 0,
					EndOffset:   int64(meta.OffsetMax),
					LockType:    int16(meta.F_RDLCK),
					Pid:         1111,
				},
			}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh2,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   2222,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: 10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrAgain))
	})

	t.Run("非原子模式转换：fh1 升级为 F_WRLCK 遇 fh2 持 F_RDLCK 冲突时，事务正常提交删除旧锁且返回 ErrAgain", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		var txCommitted bool
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, fn func(db.Querier) error, _ ...tx.Option) error {
				err := fn(mockQuerier)
				if err == nil {
					txCommitted = true
				}
				return err
			}).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh1),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 10, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh1),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 10}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		// 第一步：在事务内删除 fh1 原本持有的 F_RDLCK (返回删除了 1 行)
		mockQuerier.EXPECT().
			DeleteFlockFileLocksBySessionAndFh(mock.Anything, db.DeleteFlockFileLocksBySessionAndFhParams{
				SessionID: 10,
				Ino:       int64(ino),
				Fh:        int64(fh1),
			}).
			Return(1, nil).
			Once()

		// 第二步：发现 fh2 仍持有 F_RDLCK，与请求的 F_WRLCK 冲突
		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: int32(meta.FuseLkFlock),
			}).
			Return([]db.FileLock{
				{
					ID:          11,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh2),
					LockOwner:   1002,
					LkFlags:     int32(meta.FuseLkFlock),
					StartOffset: 0,
					EndOffset:   int64(meta.OffsetMax),
					LockType:    int16(meta.F_RDLCK),
					Pid:         2222,
				},
			}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh1,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   1111,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: 10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrAgain))
		assert.True(t, txCommitted, "transaction callback must return nil so deletion of caller's old flock is committed")
	})

	t.Run("同一 (session_id, ino, fh) 持有排他 flock 时降级为 F_RDLCK，即使存在 PENDING 排他等待者也不被拦截（已持锁豁免排队防死锁）", func(t *testing.T) {
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
				Fh:        int64(fh1),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: 10, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh1),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: 10}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		// fh1 先前已持有排他 flock，DeleteFlockFileLocksBySessionAndFh 删除 1 条记录
		mockQuerier.EXPECT().
			DeleteFlockFileLocksBySessionAndFh(mock.Anything, db.DeleteFlockFileLocksBySessionAndFhParams{
				SessionID: 10,
				Ino:       int64(ino),
				Fh:        int64(fh1),
			}).
			Return(1, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: int32(meta.FuseLkFlock),
			}).
			Return(nil, nil).
			Once()

		// 由于 fh1 原本已持有该节点 flock（降级/重入），不检查 PENDING 等待队列，直接创建降级后的 F_RDLCK
		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh1),
				LockOwner:   1001,
				LkFlags:     int32(meta.FuseLkFlock),
				StartOffset: 0,
				EndOffset:   int64(meta.OffsetMax),
				LockType:    int16(meta.F_RDLCK),
				Pid:         5555,
			}).
			Return(db.FileLock{ID: 12}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh1,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_RDLCK,
				PID:   5555,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: 10,
		})
		require.NoError(t, err)
	})
}

func TestSetLk_FlockPostgresSemantics(t *testing.T) {
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

	session1 := uint64(940001)
	session2 := uint64(940002)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(session1), int64(session2))
	}()

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: uint64(rootDir.Ino),
		Name:      []byte("setlk_flock_test.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDONLY,
		SessionID: &session1,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh1RO := createRes.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(fileIno))
	}()

	open2RO, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDONLY,
		SessionID: &session1,
	})
	require.NoError(t, err)
	fh2RO := open2RO.Fh

	open3RDWR, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &session2,
	})
	require.NoError(t, err)
	fh3RDWR := open3RDWR.Fh

	// AC 1 & AC 2: 使用 O_RDONLY 的 fh1RO 申请排他 BSD flock (lk_flags == 1, F_WRLCK) 成功，不触发 EBADF；
	// 且在同一 Inode 上持有排他 flock 时，另一持有者仍可成功申请 POSIX 读写锁 (lk_flags == 0)，反之亦然。
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh1RO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_WRLCK,
			PID:   1111,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.NoError(t, err)

	// 另一持有者在 POSIX 命名空间 (lk_flags == 0) 申请整文件排他写锁成功（正交不冲突）
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh3RDWR,
		LockOwner: 2002,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_WRLCK,
			PID:   2222,
		},
		LkFlags:   0,
		SessionID: session2,
	})
	require.NoError(t, err)

	// GetLk 验证：fh2RO 探测 flock 看到 fh1RO 的排他锁，而探测 POSIX 命名空间互不干扰
	flockProbe, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fh2RO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_RDLCK,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.NoError(t, err)
	assert.Equal(t, meta.F_WRLCK, flockProbe.Type)
	assert.Equal(t, uint32(1111), flockProbe.PID)

	// AC 3: 不同 FileHandle (fh2RO vs fh1RO，即使同一 session1 且同一 lock_owner=1001) 申请 flock 判定冲突
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh2RO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_RDLCK,
			PID:   3333,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrAgain))

	// 同一 (session1, fileIno, fh1RO) 重复申请相同模式或转换模式（无外部冲突时）原地更新锁模式与 pid：
	// 将 fh1RO 从 F_WRLCK (pid=1111) 转换为 F_RDLCK (pid=1112)
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh1RO,
		LockOwner: 9999, // lock_owner 改变不影响 flock 绑定键
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_RDLCK,
			PID:   1112,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.NoError(t, err)

	// 此时 fh1RO 已降级为共享锁 F_RDLCK (pid=1112)，fh2RO 申请共享锁 F_RDLCK (pid=2223) 成功
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh2RO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_RDLCK,
			PID:   2223,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.NoError(t, err)

	// AC 4: 当 fh1RO 与 fh2RO 均持有共享 flock (F_RDLCK)，且 fh1RO 调用 SetLk 请求升级为排他 flock (F_WRLCK) 时，
	// 返回 ErrAgain (EAGAIN)，且 fh1RO 原先持有的共享 flock 已被持久化移除！
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh1RO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_WRLCK,
			PID:   1113,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrAgain))

	// 验证 fh1RO 的原共享锁已被持久化移除：此时仅剩 fh2RO 持有共享锁，因此 fh2RO 可以立即无冲突升级为排他 flock (F_WRLCK)！
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh2RO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_WRLCK,
			PID:   2224,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.NoError(t, err, "fh2RO should immediately upgrade to F_WRLCK without conflict because fh1RO's shared flock was removed before EAGAIN")

	// 验证反向正交：在 POSIX 命名空间已持有整文件排他锁 (fh3RDWR) 且 flock 命名空间已释放后，新申请 flock 依然不受 POSIX 排他锁阻碍
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh2RO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_UNLCK,
			PID:   2224,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.NoError(t, err)

	// 此时 POSIX 排他锁 (fh3RDWR, session2) 仍在，fh1RO 重新申请排他 flock 成功
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh1RO,
		LockOwner: 1001,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  meta.F_WRLCK,
			PID:   1115,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: session1,
	})
	require.NoError(t, err)
}
