package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestOpen_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("常规文件只读模式正常打开", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{
				Fh:        1,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDONLY),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(1), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	t.Run("常规文件读写模式正常打开", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}).
			Return(db.FileHandle{
				Fh:        2,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(2), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	for _, spec := range []struct {
		name string
		mode uint32
		rdev uint32
	}{
		{name: "FIFO 命名管道", mode: meta.S_IFIFO | 0o644, rdev: 0},
		{name: "字符设备文件", mode: meta.S_IFCHR | 0o660, rdev: 136},
		{name: "块设备文件", mode: meta.S_IFBLK | 0o660, rdev: 8},
		{name: "UNIX 本地域套接字", mode: meta.S_IFSOCK | 0o777, rdev: 0},
	} {
		t.Run("特殊设备文件正常打开: "+spec.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			setupMockWriteTx(mockStore, mockQuerier)

			specialRow := baseFileRow
			specialRow.Mode = int32(spec.mode)
			specialRow.Rdev = int64(spec.rdev)

			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(ino)).
				Return(specialRow, nil).
				Once()

			mockQuerier.EXPECT().
				CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
					Ino:   int64(ino),
					Flags: int32(meta.O_RDWR),
				}).
				Return(db.FileHandle{
					Fh:        42,
					Ino:       int64(ino),
					Flags:     int32(meta.O_RDWR),
					CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				}, nil).
				Once()

			res, err := mgr.Open(ctx, meta.OpenParams{
				Ino:   ino,
				Flags: meta.O_RDWR,
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, uint64(42), res.Fh)
			assert.Equal(t, uint32(0), res.OpenFlags)
		})
	}

	t.Run("多次打开同一 Inode 分配不同 fh", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		// 第一次打开
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{
				Fh:        10,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDONLY),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res1, err1 := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err1)
		assert.Equal(t, uint64(10), res1.Fh)

		// 第二次打开
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{
				Fh:        11,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDONLY),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res2, err2 := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err2)
		assert.Equal(t, uint64(11), res2.Fh)
		assert.NotEqual(t, res1.Fh, res2.Fh)
	})
}

func TestOpen_InputValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("ino 为 0 时直接返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   0,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("ino 超过 math.MaxInt64 时直接返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   uint64(math.MaxInt64) + 1,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	for _, tc := range []struct {
		name  string
		flags uint32
	}{
		{name: "flags 仅携带 O_TMPFILE", flags: meta.O_TMPFILE},
		{name: "flags 携带 O_TMPFILE | O_RDONLY", flags: meta.O_TMPFILE | meta.O_RDONLY},
		{name: "flags 携带 O_TMPFILE | O_WRONLY", flags: meta.O_TMPFILE | meta.O_WRONLY},
		{name: "flags 携带 O_TMPFILE | O_RDWR", flags: meta.O_TMPFILE | meta.O_RDWR},
		{name: "flags 携带 O_TMPFILE | O_EXCL | O_RDWR", flags: meta.O_TMPFILE | meta.O_EXCL | meta.O_RDWR},
	} {
		t.Run("flags 伪造 O_TMPFILE 标志位时静态入参校验直接拒绝: "+tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			// 零 Mock 预期设置：静态入参校验严禁触发任何数据库读写算子，不分配新 FileHandle
			res, err := mgr.Open(ctx, meta.OpenParams{
				Ino:   100,
				Flags: tc.flags,
			})

			assert.Nil(t, res)
			require.Error(t, err)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)

			// 验证错误契约映射为 EINVAL
			st := meta.ToStatus(err)
			require.NotNil(t, st)
			require.NotEmpty(t, st.Details())
			info, ok := st.Details()[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			assert.Equal(t, "EINVAL", info.Reason)
			assert.Equal(t, meta.ErrorDomain, info.Domain)
		})
	}
}

func TestOpen_TypeChecking(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	t.Run("目标为目录 (S_IFDIR) 时返回 ErrIsDir (POSIX EISDIR)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       4096,
			Nlink:      2,
			Uid:        0,
			Gid:        0,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(dirRow, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("目标为符号链接 (S_IFLNK) 时返回 ErrInvalidArgument (POSIX EINVAL)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		symlinkRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFLNK | 0o777),
			Size:       12,
			Nlink:      1,
			Uid:        0,
			Gid:        0,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(symlinkRow, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})
}

func TestOpen_NotFoundAndErrors(t *testing.T) {
	ctx := context.Background()
	ino := uint64(100)

	t.Run("目标节点不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("底层查询 Inode 发生非 NoRows 数据库错误时透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("database connection lost")
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, dbErr).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})
}

func TestOpen_OrphanInodeAdmission(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	orphanRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      0, // 已解链孤儿节点
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("死孤儿节点 (nlink == 0 且无活跃句柄) 禁止打开并返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanRow, nil).
			Once()
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("活跃孤儿节点 (nlink == 0 且存在活跃句柄) 准入通过并成功分配句柄", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanRow, nil).
			Once()
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(true, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}).
			Return(db.FileHandle{
				Fh:        88,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(88), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	t.Run("查询孤儿节点活跃句柄失败时透传底层错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanRow, nil).
			Once()
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanRow, nil).
			Once()

		dbErr := errors.New("query has_file_handles failed")
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, dbErr).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})
}

func TestOpen_OTrunc(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755),
		Size:       4096,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("只读模式携带 O_TRUNC 返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY | meta.O_TRUNC,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("常规文件在 O_WRONLY|O_TRUNC 时原子截断并剥离 S_ISUID/S_ISGID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		// 剥离 S_ISUID 与 S_ISGID 后的模式
		expectedTruncatedMode := int32(meta.S_IFREG | 0o755)

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedTruncatedMode,
				Size:        0,
				Uid:         baseFileRow.Uid,
				Gid:         baseFileRow.Gid,
				SetAtimeNow: false,
				Atime:       baseFileRow.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:        int64(ino),
				Generation: 1,
				Mode:       expectedTruncatedMode,
				Size:       0,
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Rdev:       0,
				Atime:      baseFileRow.Atime,
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        99,
				Ino:       int64(ino),
				Flags:     int32(meta.O_WRONLY | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_WRONLY | meta.O_TRUNC,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(99), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	t.Run("常规文件在 O_RDWR|O_TRUNC 时原子截断", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		normalRegRow := baseFileRow
		normalRegRow.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(normalRegRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        int32(meta.S_IFREG | 0o644),
				Size:        0,
				Uid:         normalRegRow.Uid,
				Gid:         normalRegRow.Gid,
				SetAtimeNow: false,
				Atime:       normalRegRow.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(normalRegRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        100,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(100), res.Fh)
	})

	t.Run("特殊文件 (FIFO) 携带 O_TRUNC 时忽略截断正常分配句柄", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fifoRow := baseFileRow
		fifoRow.Mode = int32(meta.S_IFIFO | 0o644)
		fifoRow.Size = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fifoRow, nil).
			Once()

		// 特殊文件不调用 UpdateInodeAttr，直接调用 CreateFileHandle
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        101,
				Ino:       int64(ino),
				Flags:     int32(meta.O_WRONLY | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_WRONLY | meta.O_TRUNC,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(101), res.Fh)
	})

	t.Run("截断更新属性失败时透传底层错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		dbErr := errors.New("update inode attr failed")
		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.Anything).
			Return(db.Inode{}, dbErr).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("创建句柄失败时透传底层错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.Anything).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		dbErr := errors.New("insert file handle failed")
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{}, dbErr).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("常规文件在携带 O_TRUNC 打开且 RootIno>0, Size>0 时同一事务内联动扣减 used_bytes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		rootIno := int64(1)
		fileSize := int64(4096)
		fileRow := baseFileRow
		fileRow.RootIno = rootIno
		fileRow.Size = fileSize

		diskUUID := pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Valid: true}
		diskState := db.GetDiskRow{
			DiskID:     diskUUID,
			Name:       "test-disk",
			RootIno:    rootIno,
			MaxBytes:   10 * 1024 * 1024,
			UsedBytes:  fileSize,
			UsedInodes: 1,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        201,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(201), res.Fh)

		// 验证通过 GetDisk 观察到 used_bytes 准确扣减归零
		mockStore.EXPECT().
			GetDisk(mock.Anything, diskUUID).
			RunAndReturn(func(ctx context.Context, id pgtype.UUID) (db.GetDiskRow, error) {
				return diskState, nil
			}).
			Once()

		disk, err := mgr.GetDisk(ctx, "01020304-0506-0708-090a-0b0c0d0e0f10")
		require.NoError(t, err)
		assert.Equal(t, uint64(0), disk.UsedBytes)

		// 验证通过 StatFS 观察到 used_bytes 准确扣减归零（Bfree/Bavail 增加）
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, rootIno).
			RunAndReturn(func(ctx context.Context, rIno int64) (db.GetDiskByRootInoRow, error) {
				return db.GetDiskByRootInoRow(diskState), nil
			}).
			Once()

		statRes, err := mgr.StatFS(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, uint64(diskState.MaxBytes)/uint64(meta.DefaultStatFSBlockSize), statRes.Bfree)
	})

	t.Run("常规文件在携带 O_WRONLY|O_TRUNC 打开且 RootIno>0, Size>0 时联动扣减 used_bytes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		rootIno := int64(2)
		fileSize := int64(8192)
		fileRow := baseFileRow
		fileRow.RootIno = rootIno
		fileRow.Size = fileSize

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        202,
				Ino:       int64(ino),
				Flags:     int32(meta.O_WRONLY | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_WRONLY | meta.O_TRUNC,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(202), res.Fh)
	})

	t.Run("常规文件 Size 为 0 且携带 O_TRUNC 打开时正常截断", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		rootIno := int64(1)
		fileRow := baseFileRow
		fileRow.RootIno = rootIno
		fileRow.Size = 0

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        203,
				Ino:       int64(ino),
				Flags:     int32(meta.O_WRONLY | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_WRONLY | meta.O_TRUNC,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(203), res.Fh)
	})
}

func TestOpen_CreateFileHandlePgErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseFileRow := db.Inode{
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

	for _, tc := range []struct {
		name           string
		createErr      error
		expectNotFound bool
	}{
		{
			name: "直接返回 23503 外键约束冲突映射为 ErrNotFound",
			createErr: &pgconn.PgError{
				Code:    "23503",
				Message: "insert or update on table \"file_handles\" violates foreign key constraint",
			},
			expectNotFound: true,
		},
		{
			name: "包装后的 wrapped 23503 外键冲突同样映射为 ErrNotFound",
			createErr: fmt.Errorf("wrapped fk error: %w", &pgconn.PgError{
				Code:    "23503",
				Message: "foreign key violation",
			}),
			expectNotFound: true,
		},
		{
			name: "非 23503 错误 (23505 唯一键冲突) 负面边界对照不得映射为 ErrNotFound",
			createErr: &pgconn.PgError{
				Code:    "23505",
				Message: "duplicate key value violates unique constraint",
			},
			expectNotFound: false,
		},
		{
			name: "非 23503 错误 (08006 连接断开) 负面边界对照不得映射为 ErrNotFound",
			createErr: &pgconn.PgError{
				Code:    "08006",
				Message: "connection failure",
			},
			expectNotFound: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			setupMockWriteTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(ino)).
				Return(baseFileRow, nil).
				Once()

			mockQuerier.EXPECT().
				CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
					Ino:   int64(ino),
					Flags: int32(meta.O_RDONLY),
				}).
				Return(db.FileHandle{}, tc.createErr).
				Once()

			res, err := mgr.Open(ctx, meta.OpenParams{
				Ino:   ino,
				Flags: meta.O_RDONLY,
			})

			assert.Nil(t, res)
			require.Error(t, err)

			if tc.expectNotFound {
				require.ErrorIs(t, err, meta.ErrNotFound)

				st := meta.ToStatus(err)
				require.NotNil(t, st)
				assert.Equal(t, codes.NotFound, st.Code())
				require.NotEmpty(t, st.Details())
				info, ok := st.Details()[0].(*errdetails.ErrorInfo)
				require.True(t, ok)
				assert.Equal(t, meta.ErrorDomain, info.Domain)
				assert.Equal(t, "ENOENT", info.Reason)
			} else {
				assert.False(t, errors.Is(err, meta.ErrNotFound), "非 23503 错误严禁误映射为 ErrNotFound")

				st := meta.ToStatus(err)
				require.NotNil(t, st)
				assert.NotEqual(t, codes.NotFound, st.Code(), "非 23503 错误状态码严禁映射为 NotFound")
				// 契约锚定：未匹配 24 种领域哨兵的底层未知错误按 status.go 既有契约映射为 Unknown
				assert.Equal(t, codes.Unknown, st.Code())
				assert.Empty(t, st.Details(), "底层未映射数据库错误不应携带 ErrorInfo 详情")
			}
		})
	}
}
