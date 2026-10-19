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

func TestFsyncDir_InputValidation(t *testing.T) {
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
			name: "ino 与 fh 均为 0",
			ino:  0,
			fh:   0,
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

			err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
				Ino:       tc.ino,
				Fh:        tc.fh,
				Datasync:  true,
				SessionID: tc.sessionID,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestFsyncDir_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseDirInode := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      2,
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
		Flags:     int32(meta.O_RDONLY),
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("全量目录元数据同步成功 (datasync = false, 无 session_id)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDirInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(baseHandle, nil).
			Once()

		err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.NoError(t, err)
	})

	t.Run("仅目录数据同步成功 (datasync = true, 携带有效 session_id)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		sessionID := uint64(888)
		handleWithSession := baseHandle
		handleWithSession.SessionID = pgtype.Int8{Int64: int64(sessionID), Valid: true}

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDirInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(handleWithSession, nil).
			Once()

		err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:       ino,
			Fh:        fh,
			Datasync:  true,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
	})

	t.Run("处于 rmdir-whilst-open 状态的孤儿目录 (nlink == 0) 正常同步成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 2)

		orphanDirInode := baseDirInode
		orphanDirInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDirInode, nil).
			Times(2)

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(baseHandle, nil).
			Times(2)

		require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		}))
		require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: true,
		}))
	})
}

func TestFsyncDir_InvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(300)
	fh := uint64(5)

	baseDirInode := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      2,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
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

		err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("目标节点为常规文件或符号链接等非目录返回 ErrNotDir", func(t *testing.T) {
		for _, mode := range []uint32{
			meta.S_IFREG | 0o644,
			meta.S_IFLNK | 0o777,
			meta.S_IFIFO | 0o644,
		} {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(ino)).
				Return(db.Inode{
					Ino:  int64(ino),
					Mode: int32(mode),
				}, nil).
				Once()

			err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
				Ino:      ino,
				Fh:       fh,
				Datasync: false,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrNotDir))
		}
	})

	t.Run("句柄不存在、跨目录不匹配或所属会话已过期返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDirInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("底层 GetInode 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		expectedErr := errors.New("database connection failed")
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, expectedErr).
			Once()

		err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
	})

	t.Run("底层 GetFileHandle 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		expectedErr := errors.New("query file handle failed")
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDirInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{}, expectedErr).
			Once()

		err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:      ino,
			Fh:       fh,
			Datasync: false,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
	})
}

func TestFsyncDir_PostgresIntegration(t *testing.T) {
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
	rootIno := uint64(rootDir.Ino)
	defer func() {
		_ = store.DeleteInode(context.Background(), rootDir.Ino)
	}()

	sess1 := uint64(990001)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sess1))
	}()

	// 1. 创建两个子目录 dirA 与 dirB，以及一个常规文件 regFile
	dirA, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: rootIno,
		Name:      []byte("dir_a"),
		Mode:      meta.S_IFDIR | 0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(dirA.Ino))
	}()

	dirB, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: rootIno,
		Name:      []byte("dir_b"),
		Mode:      meta.S_IFDIR | 0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(dirB.Ino))
	}()

	regFileRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: rootIno,
		Name:      []byte("reg_file.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	regFileIno := regFileRes.Inode.Ino
	regFileFh := regFileRes.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(regFileIno))
	}()

	// 2. 打开 dirA（无 session_id）与 dirB（绑定 sess1）
	openDirA, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:   dirA.Ino,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)

	openDirB, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       dirB.Ino,
		Flags:     meta.O_RDONLY,
		SessionID: &sess1,
	})
	require.NoError(t, err)

	// 3. 正常目录 FsyncDir 幂等成功（datasync = false 与 true）
	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      dirA.Ino,
		Fh:       openDirA.Fh,
		Datasync: false,
	}))
	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      dirA.Ino,
		Fh:       openDirA.Fh,
		Datasync: true,
	}))
	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:       dirB.Ino,
		Fh:        openDirB.Fh,
		Datasync:  false,
		SessionID: &sess1,
	}))

	// 4. Rmdir dirA 使其进入 rmdir-whilst-open 孤儿目录状态 (nlink == 0)，FsyncDir 依然幂等成功
	require.NoError(t, mgr.Rmdir(ctx, rootIno, []byte("dir_a")))
	dirAAttr, err := mgr.GetAttr(ctx, dirA.Ino)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), dirAAttr.Attr.Nlink)

	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      dirA.Ino,
		Fh:       openDirA.Fh,
		Datasync: false,
	}))
	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      dirA.Ino,
		Fh:       openDirA.Fh,
		Datasync: true,
	}))

	// 5. 错误分支：不存在的 Inode 返回 ErrNotFound (ENOENT)
	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      999999999,
		Fh:       openDirA.Fh,
		Datasync: false,
	})
	assert.ErrorIs(t, err, meta.ErrNotFound)

	// 6. 错误分支：对非目录 Inode (常规文件) 调用 FsyncDir 返回 ErrNotDir (ENOTDIR)
	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      regFileIno,
		Fh:       regFileFh,
		Datasync: false,
	})
	assert.ErrorIs(t, err, meta.ErrNotDir)

	// 7. 错误分支：跨目录混用句柄 (dirA 的 ino + dirB 的 fh) 返回 ErrBadFileHandle (EBADF)
	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      dirA.Ino,
		Fh:       openDirB.Fh,
		Datasync: false,
	})
	assert.ErrorIs(t, err, meta.ErrBadFileHandle)

	// 8. 错误分支：ReleaseDir 关闭 dirA 句柄后，再次 FsyncDir（且孤儿目录已被物理回收）
	// 先在 dirB 上测试正常目录 ReleaseDir 后的 EBADF
	openDirB2, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:   dirB.Ino,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino: dirB.Ino,
		Fh:  openDirB2.Fh,
	}))
	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      dirB.Ino,
		Fh:       openDirB2.Fh,
		Datasync: false,
	})
	assert.ErrorIs(t, err, meta.ErrBadFileHandle)

	// 释放孤儿目录 dirA 的最后一个句柄，dirA 被物理回收，再次 FsyncDir 返回 ErrNotFound
	require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino: dirA.Ino,
		Fh:  openDirA.Fh,
	}))
	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      dirA.Ino,
		Fh:       openDirA.Fh,
		Datasync: false,
	})
	assert.ErrorIs(t, err, meta.ErrNotFound)

	// 9. 错误分支：令 sess1 过期后，对 dirB 的 openDirB.Fh 调用 FsyncDir 返回 ErrBadFileHandle (EBADF)
	_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(sess1))
	require.NoError(t, err)

	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:       dirB.Ino,
		Fh:        openDirB.Fh,
		Datasync:  false,
		SessionID: &sess1,
	})
	assert.ErrorIs(t, err, meta.ErrBadFileHandle)
	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:      dirB.Ino,
		Fh:       openDirB.Fh,
		Datasync: true,
	})
	assert.ErrorIs(t, err, meta.ErrBadFileHandle)
}



