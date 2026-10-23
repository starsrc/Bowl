package meta_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestSetLk_PosixRangeSplitAndCoalesce(t *testing.T) {
	ctx := context.Background()
	ino := uint64(600)
	fh := uint64(25)

	baseInode := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
	}

	rwHandle := db.FileHandle{
		Fh:    int64(fh),
		Ino:   int64(ino),
		Flags: int32(meta.O_RDWR),
	}

	expectSessionStep := func(mockQuerier *dbmock.MockQuerier, sessionID int64) {
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()
		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: sessionID, Valid: true},
			}).
			Return(rwHandle, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: sessionID}, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()
	}

	t.Run("中间挖空解锁: [0, 100] (pid=1001) 对 [20, 50] 执行 F_UNLCK 裂解为 [0, 19] 与 [51, 100] 且均保留 pid=1001", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		expectSessionStep(mockQuerier, 10)

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          101,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   100,
					LockType:    int16(meta.F_WRLCK),
					Pid:         1001,
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileLocksByIDs(mock.Anything, []int64{101}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 0,
				EndOffset:   19,
				LockType:    int16(meta.F_WRLCK),
				Pid:         1001,
			}).
			Return(db.FileLock{ID: 102}, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 51,
				EndOffset:   100,
				LockType:    int16(meta.F_WRLCK),
				Pid:         1001,
			}).
			Return(db.FileLock{ID: 103}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 5001,
			Lk: meta.FileLock{
				Start: 20,
				End:   50,
				Type:  meta.F_UNLCK,
				PID:   9999,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.NoError(t, err)
	})

	t.Run("左截断、右截断及覆盖多碎片解锁: [10, 30]、[40, 60]、[70, 100] 对 [20, 80] 执行 F_UNLCK 精确裁剪为 [10, 19] 与 [81, 100]", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		expectSessionStep(mockQuerier, 10)

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          201,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 10,
					EndOffset:   30,
					LockType:    int16(meta.F_RDLCK),
					Pid:         1001,
				},
				{
					ID:          202,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 40,
					EndOffset:   60,
					LockType:    int16(meta.F_WRLCK),
					Pid:         1002,
				},
				{
					ID:          203,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 70,
					EndOffset:   100,
					LockType:    int16(meta.F_RDLCK),
					Pid:         1003,
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileLocksByIDs(mock.Anything, []int64{201, 202, 203}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 10,
				EndOffset:   19,
				LockType:    int16(meta.F_RDLCK),
				Pid:         1001,
			}).
			Return(db.FileLock{ID: 204}, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 81,
				EndOffset:   100,
				LockType:    int16(meta.F_RDLCK),
				Pid:         1003,
			}).
			Return(db.FileLock{ID: 205}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 5001,
			Lk: meta.FileLock{
				Start: 20,
				End:   80,
				Type:  meta.F_UNLCK,
				PID:   9999,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.NoError(t, err)
	})

	t.Run("首尾紧邻与重叠合并: 已有 [0, 10] (pid=1001) 与 [20, 30] (pid=1002)，申请 [11, 25] (pid=2002) 合并为 [0, 30] 且 pid 更新为 2002", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		expectSessionStep(mockQuerier, 10)

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          301,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   10,
					LockType:    int16(meta.F_WRLCK),
					Pid:         1001,
				},
				{
					ID:          302,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 20,
					EndOffset:   30,
					LockType:    int16(meta.F_WRLCK),
					Pid:         1002,
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileLocksByIDs(mock.Anything, []int64{301, 302}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 0,
				EndOffset:   30,
				LockType:    int16(meta.F_WRLCK),
				Pid:         2002,
			}).
			Return(db.FileLock{ID: 303}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 5001,
			Lk: meta.FileLock{
				Start: 11,
				End:   25,
				Type:  meta.F_WRLCK,
				PID:   2002,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.NoError(t, err)
	})

	t.Run("OffsetMax 右端点边界保护: [0, OffsetMax] 对 [100, OffsetMax] 执行 F_UNLCK 仅保留左残段 [0, 99] 不发生 end+1 溢出", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		expectSessionStep(mockQuerier, 10)

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          401,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   int64(meta.OffsetMax),
					LockType:    int16(meta.F_RDLCK),
					Pid:         1001,
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileLocksByIDs(mock.Anything, []int64{401}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 0,
				EndOffset:   99,
				LockType:    int16(meta.F_RDLCK),
				Pid:         1001,
			}).
			Return(db.FileLock{ID: 402}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 5001,
			Lk: meta.FileLock{
				Start: 100,
				End:   meta.OffsetMax,
				Type:  meta.F_UNLCK,
				PID:   9999,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.NoError(t, err)
	})

	t.Run("同持有者部分升级无冲突: [0, 100] F_RDLCK (pid=1001) 对 [20, 50] 申请 F_WRLCK (pid=2002) 裂解为 [0, 19] F_RDLCK、[20, 50] F_WRLCK、[51, 100] F_RDLCK", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		expectSessionStep(mockQuerier, 10)

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          501,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   100,
					LockType:    int16(meta.F_RDLCK),
					Pid:         1001,
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileLocksByIDs(mock.Anything, []int64{501}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 0,
				EndOffset:   19,
				LockType:    int16(meta.F_RDLCK),
				Pid:         1001,
			}).
			Return(db.FileLock{ID: 502}, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 51,
				EndOffset:   100,
				LockType:    int16(meta.F_RDLCK),
				Pid:         1001,
			}).
			Return(db.FileLock{ID: 503}, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileLock(mock.Anything, db.CreateFileLockParams{
				SessionID:   10,
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   5001,
				LkFlags:     0,
				StartOffset: 20,
				EndOffset:   50,
				LockType:    int16(meta.F_WRLCK),
				Pid:         2002,
			}).
			Return(db.FileLock{ID: 504}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 5001,
			Lk: meta.FileLock{
				Start: 20,
				End:   50,
				Type:  meta.F_WRLCK,
				PID:   2002,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.NoError(t, err)
	})

	t.Run("原子升级失败保护: [0, 100] F_RDLCK 对 [20, 50] 申请 F_WRLCK 遇异主读锁冲突返回 ErrAgain 且不触碰原读锁", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		expectSessionStep(mockQuerier, 10)

		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(ino),
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          601,
					SessionID:   10,
					Ino:         int64(ino),
					Fh:          int64(fh),
					LockOwner:   5001,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   100,
					LockType:    int16(meta.F_RDLCK),
					Pid:         1001,
				},
				{
					ID:          602,
					SessionID:   20,
					Ino:         int64(ino),
					Fh:          99,
					LockOwner:   6002,
					LkFlags:     0,
					StartOffset: 40,
					EndOffset:   80,
					LockType:    int16(meta.F_RDLCK),
					Pid:         3003,
				},
			}, nil).
			Once()

		err := mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: 5001,
			Lk: meta.FileLock{
				Start: 20,
				End:   50,
				Type:  meta.F_WRLCK,
				PID:   2002,
			},
			LkFlags:   0,
			SessionID: 10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrAgain))
	})
}

func TestSetLk_PosixRangeSplitAndCoalesce_Postgres(t *testing.T) {
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

	session1 := uint64(930001)
	session2 := uint64(930002)
	owner1 := uint64(10001)
	owner2 := uint64(20002)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(session1), int64(session2))
	}()

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: uint64(rootDir.Ino),
		Name:      []byte("posix_range_split_coalesce.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &session1,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fhRDWR := createRes.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(fileIno))
	}()

	openRDWR2, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &session2,
	})
	require.NoError(t, err)
	fhRDWR2 := openRDWR2.Fh

	assertOwnerLocks := func(t *testing.T, expected []meta.FileLock) {
		t.Helper()
		locks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(fileIno),
			LkFlags: 0,
		})
		require.NoError(t, err)
		var actual []meta.FileLock
		for _, l := range locks {
			if uint64(l.SessionID) == session1 && uint64(l.LockOwner) == owner1 {
				actual = append(actual, meta.FileLock{
					Start: uint64(l.StartOffset),
					End:   uint64(l.EndOffset),
					Type:  uint32(l.LockType),
					PID:   uint32(l.Pid),
				})
			}
		}
		assert.Equal(t, expected, actual)
	}

	// 1. 同一持有者在 [0, 100] (pid = 1001) 持有写锁，对中间 [20, 50] 执行 F_UNLCK -> 裂解为 [0, 19] 与 [51, 100]，均保留 pid = 1001
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1001},
		LkFlags:   0,
		SessionID: session1,
	}))
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 20, End: 50, Type: meta.F_UNLCK, PID: 9999},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 0, End: 19, Type: meta.F_WRLCK, PID: 1001},
		{Start: 51, End: 100, Type: meta.F_WRLCK, PID: 1001},
	})

	// 通过 GetLk 验证裂解后的外部可观测行为：[20, 50] 已无锁，[0, 100] 首个冲突段为 [0, 19] (pid=1001)，[50, 100] 首个冲突段为 [51, 100] (pid=1001)
	probeHole, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR2,
		LockOwner: owner2,
		Lk:        meta.FileLock{Start: 20, End: 50, Type: meta.F_RDLCK},
		LkFlags:   0,
		SessionID: session2,
	})
	require.NoError(t, err)
	assert.Equal(t, meta.F_UNLCK, probeHole.Type)

	probeLeft, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR2,
		LockOwner: owner2,
		Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK},
		LkFlags:   0,
		SessionID: session2,
	})
	require.NoError(t, err)
	assert.Equal(t, &meta.FileLock{Start: 0, End: 19, Type: meta.F_WRLCK, PID: 1001}, probeLeft)

	probeRight, err := mgr.GetLk(ctx, meta.GetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR2,
		LockOwner: owner2,
		Lk:        meta.FileLock{Start: 50, End: 100, Type: meta.F_RDLCK},
		LkFlags:   0,
		SessionID: session2,
	})
	require.NoError(t, err)
	assert.Equal(t, &meta.FileLock{Start: 51, End: 100, Type: meta.F_WRLCK, PID: 1001}, probeRight)

	// 2. 左截断解锁 [0, 5]、右截断解锁 [90, 100] 以及跨多个碎片覆盖解锁
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 0, End: 5, Type: meta.F_UNLCK, PID: 9999},
		LkFlags:   0,
		SessionID: session1,
	}))
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 90, End: 100, Type: meta.F_UNLCK, PID: 9999},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 6, End: 19, Type: meta.F_WRLCK, PID: 1001},
		{Start: 51, End: 89, Type: meta.F_WRLCK, PID: 1001},
	})

	// 覆盖跨两个碎片的解锁 [10, 80] -> 裁剪为 [6, 9] 与 [81, 89]
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 10, End: 80, Type: meta.F_UNLCK, PID: 9999},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 6, End: 9, Type: meta.F_WRLCK, PID: 1001},
		{Start: 81, End: 89, Type: meta.F_WRLCK, PID: 1001},
	})

	// 全区间解锁清除所有碎片
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 9999},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, nil)

	// 3. 首尾紧邻 ([0, 10] 与 [11, 20]) 与重叠 ([15, 30]) 同类型锁自动合并为单条记录且 pid 更新为最新请求的 pid
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 0, End: 10, Type: meta.F_WRLCK, PID: 1001},
		LkFlags:   0,
		SessionID: session1,
	}))
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 11, End: 20, Type: meta.F_WRLCK, PID: 1002},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 0, End: 20, Type: meta.F_WRLCK, PID: 1002},
	})

	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 15, End: 30, Type: meta.F_WRLCK, PID: 1003},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 0, End: 30, Type: meta.F_WRLCK, PID: 1003},
	})

	// 4. OffsetMax 边界无溢出验证：持有 [31, OffsetMax] 与 [0, 30] 紧邻合并为 [0, OffsetMax]，随后解锁 [101, OffsetMax] 截断为 [0, 100]
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 31, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 1004},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 1004},
	})

	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 101, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 9999},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1004},
	})

	// 5. 原子升级保护与同持有者异类覆盖裂解：
	// 将 [0, 100] 降级为 F_RDLCK (pid = 1001)
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
	})

	// 异主 (session2, owner2) 在 [40, 60] 持有 F_RDLCK
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR2,
		LockOwner: owner2,
		Lk:        meta.FileLock{Start: 40, End: 60, Type: meta.F_RDLCK, PID: 2002},
		LkFlags:   0,
		SessionID: session2,
	}))

	// 持有者 1 尝试将 [20, 50] 升级为 F_WRLCK -> 因异主读锁冲突返回 ErrAgain，原 [0, 100] F_RDLCK (pid=1001) 丝毫无损
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 20, End: 50, Type: meta.F_WRLCK, PID: 1005},
		LkFlags:   0,
		SessionID: session1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrAgain))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
	})

	// 异主释放 [40, 60] 读锁后，持有者 1 再次对 [20, 50] 申请 F_WRLCK (pid=1005) -> 裂解为 [0, 19] F_RDLCK (1001)、[20, 50] F_WRLCK (1005)、[51, 100] F_RDLCK (1001)
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR2,
		LockOwner: owner2,
		Lk:        meta.FileLock{Start: 40, End: 60, Type: meta.F_UNLCK, PID: 2002},
		LkFlags:   0,
		SessionID: session2,
	}))
	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhRDWR,
		LockOwner: owner1,
		Lk:        meta.FileLock{Start: 20, End: 50, Type: meta.F_WRLCK, PID: 1005},
		LkFlags:   0,
		SessionID: session1,
	}))
	assertOwnerLocks(t, []meta.FileLock{
		{Start: 0, End: 19, Type: meta.F_RDLCK, PID: 1001},
		{Start: 20, End: 50, Type: meta.F_WRLCK, PID: 1005},
		{Start: 51, End: 100, Type: meta.F_RDLCK, PID: 1001},
	})
}
