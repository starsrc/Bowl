package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
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

func TestReleaseDir_InputValidation(t *testing.T) {
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
		{
			name: "ino 与 fh 均为 0",
			ino:  0,
			fh:   0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
				Ino: tc.ino,
				Fh:  tc.fh,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestReleaseDir_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseDir := db.Inode{
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

	t.Run("正常目录释放 (nlink > 0): 销毁句柄且保留 Inode 行 (免除 GetInodeForUpdate)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("多句柄并发打开的 Orphan 目录 (nlink == 0): 释放其中一个句柄，仍有剩余句柄时不删除 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
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

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan 目录 (nlink == 0): 无剩余活跃句柄时物理回收 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
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

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan 目录 (nlink == 0, RootIno > 0): 无剩余活跃句柄时物理回收 Inode 并退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDir := baseDir
		orphanDir.Nlink = 0
		orphanDir.RootIno = 10

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
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

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("存在剩余活跃句柄的 Orphan 目录 (nlink == 0, RootIno > 0): 仅释放句柄，不调用 DeleteInode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDir := baseDir
		orphanDir.Nlink = 0
		orphanDir.RootIno = 10

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
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

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("携带 SessionID 关闭 Orphan 目录 (nlink == 0) 的最后一个句柄：先续期会话再删句柄且配额仅扣减一次", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		sessionID := uint64(888)
		orphanDir := baseDir
		orphanDir.Nlink = 0
		orphanDir.RootIno = 10

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		// 1. 先续期会话 (renewOptionalClientSession)
		setupRenewClientSessionMocks(mockQuerier, sessionID)

		// 2. 再删除句柄 (DeleteFileHandle)
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(int64(fh), nil).
			Once()

		// 3. 最终无句柄后回收孤儿节点 (reclaimOrphanIfNoHandles)
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       ino,
			Fh:        fh,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
	})
}

func TestReleaseDir_NegativeAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseDir := db.Inode{
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

	t.Run("目标节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("目标节点为普通文件返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileInode := baseDir
		fileInode.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(fileInode, nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotDir))
	})

	t.Run("句柄不存在或归属不匹配返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), pgx.ErrNoRows).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("底层 GetInode 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, errors.New("db error")).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get inode")
	})

	t.Run("底层 DeleteFileHandle 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), errors.New("db delete handle error")).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete file handle")
	})

	t.Run("底层 HasFileHandlesByIno 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
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
			Return(false, errors.New("db query remaining handles error")).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "check remaining file handles")
	})

	t.Run("底层 DeleteInode 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
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
			Return(errors.New("db delete inode error")).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete orphan inode")
	})
}

func TestReleaseDir_QuotaReclaim_PostgresIntegration(t *testing.T) {
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

	t.Run("携带 SessionID 关闭孤儿目录的最后一个句柄：验证 UsedInodes 仅被扣减一次（无双重扣减）", func(t *testing.T) {
		diskName := fmt.Sprintf("test_releasedir_session_quota_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessID := uint64(770002)

		// 1. 创建常驻子目录 normal_dir，提供配额基底
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("normal_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)

		// 2. 创建即将被 rmdir 的目录 orphan_dir
		orphanDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)

		// 打开 orphan_dir 句柄（携带 SessionID）
		openRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       orphanDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 当前配额状态：根目录 + normal_dir + orphan_dir = 3 inodes
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)

		// 3. Rmdir orphan_dir：因句柄打开，nlink 变 0 但暂不回收，配额不变
		err = mgr.Rmdir(ctx, disk.RootIno, []byte("orphan_dir"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)

		// 4. ReleaseDir 关闭句柄（携带 SessionID）
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       orphanDir.Ino,
			Fh:        openRes.Fh,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 5. 校验配额：必须且仅扣减 1 个 inode（剩余 2 个 inodes：根目录与 normal_dir）
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "孤儿目录释放后应恰好剩余 2 个 inode (根目录与 normal_dir)")
	})
}
