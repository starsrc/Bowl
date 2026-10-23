package meta_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestCreateDisk_StaticValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("max_bytes 为 0 返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "test-disk",
			MaxBytes: 0,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "max_bytes must be between 1 and")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("max_bytes 超过 math.MaxInt64 返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "test-disk",
			MaxBytes: uint64(math.MaxInt64) + 1,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "max_bytes must be between 1 and")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("数值边界优先于名称校验：max_bytes 非法且名称超长时返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     strings.Repeat("a", 256),
			MaxBytes: 0,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("名称长度超过 255 字节返回 ErrNameTooLong", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     strings.Repeat("a", 256),
			MaxBytes: 1024 * 1024,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNameTooLong))
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("名称包含空字节返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "disk\x00bad",
			MaxBytes: 1024 * 1024,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "disk name contains null byte")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("字符串边界校验优先超长后空字节：超长且含空字节时返回 ErrNameTooLong", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     strings.Repeat("a", 256) + "\x00",
			MaxBytes: 1024 * 1024,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNameTooLong))
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})
}

func TestCreateDisk_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	t.Run("正常创建虚拟磁盘并原子分配根目录 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		expectedRootIno := int64(1001)
		rootInodeRow := db.Inode{
			Ino:        expectedRootIno,
			Generation: 0,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       int64(meta.DefaultDirSize),
			Nlink:      2,
			Uid:        0,
			Gid:        0,
			Rdev:       0,
			ParentIno:  expectedRootIno,
			RootIno:    expectedRootIno,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateRootInode(mock.Anything, db.CreateRootInodeParams{
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
				Uid:   0,
				Gid:   0,
				Rdev:  0,
				Size:  int64(meta.DefaultDirSize),
			}).
			Return(rootInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateDisk(mock.Anything, mock.MatchedBy(func(arg db.CreateDiskParams) bool {
				return arg.DiskID.Valid &&
					arg.Name == "primary-disk" &&
					arg.RootIno == expectedRootIno &&
					arg.MaxBytes == 10*1024*1024*1024
			})).
			RunAndReturn(func(ctx context.Context, arg db.CreateDiskParams) (db.Disk, error) {
				return db.Disk{
					DiskID:     arg.DiskID,
					Name:       arg.Name,
					RootIno:    arg.RootIno,
					MaxBytes:   arg.MaxBytes,
					UsedBytes:  0,
					UsedInodes: 1,
					CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
					UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
				}, nil
			}).
			Once()

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "primary-disk",
			MaxBytes: 10 * 1024 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		assert.NotEmpty(t, disk.DiskID)
		assert.Equal(t, "primary-disk", disk.Name)
		assert.Equal(t, uint64(expectedRootIno), disk.RootIno)
		assert.Equal(t, uint64(10*1024*1024*1024), disk.MaxBytes)
		assert.Equal(t, uint64(0), disk.UsedBytes)
		assert.Equal(t, uint64(1), disk.UsedInodes)
		assert.Equal(t, now, disk.CreatedAt)
		assert.Equal(t, now, disk.UpdatedAt)
	})

	t.Run("边界值正常创建：空名称与极值配额 math.MaxInt64", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		expectedRootIno := int64(2002)
		rootInodeRow := db.Inode{
			Ino:        expectedRootIno,
			Generation: 0,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       int64(meta.DefaultDirSize),
			Nlink:      2,
			Uid:        0,
			Gid:        0,
			Rdev:       0,
			ParentIno:  expectedRootIno,
			RootIno:    expectedRootIno,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateRootInode(mock.Anything, mock.Anything).
			Return(rootInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateDisk(mock.Anything, mock.MatchedBy(func(arg db.CreateDiskParams) bool {
				return arg.Name == "" &&
					arg.RootIno == expectedRootIno &&
					arg.MaxBytes == math.MaxInt64
			})).
			RunAndReturn(func(ctx context.Context, arg db.CreateDiskParams) (db.Disk, error) {
				return db.Disk{
					DiskID:     arg.DiskID,
					Name:       arg.Name,
					RootIno:    arg.RootIno,
					MaxBytes:   arg.MaxBytes,
					UsedBytes:  0,
					UsedInodes: 1,
					CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
					UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
				}, nil
			}).
			Once()

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "",
			MaxBytes: math.MaxInt64,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		assert.Equal(t, "", disk.Name)
		assert.Equal(t, uint64(math.MaxInt64), disk.MaxBytes)
	})

	t.Run("边界值正常创建：恰好 255 字节名称与最小配额 1 字节", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		expectedRootIno := int64(3003)
		rootInodeRow := db.Inode{
			Ino:        expectedRootIno,
			Generation: 0,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       int64(meta.DefaultDirSize),
			Nlink:      2,
			Uid:        0,
			Gid:        0,
			Rdev:       0,
			ParentIno:  expectedRootIno,
			RootIno:    expectedRootIno,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateRootInode(mock.Anything, mock.Anything).
			Return(rootInodeRow, nil).
			Once()

		maxName := strings.Repeat("x", 255)
		mockQuerier.EXPECT().
			CreateDisk(mock.Anything, mock.MatchedBy(func(arg db.CreateDiskParams) bool {
				return arg.Name == maxName &&
					arg.RootIno == expectedRootIno &&
					arg.MaxBytes == 1
			})).
			RunAndReturn(func(ctx context.Context, arg db.CreateDiskParams) (db.Disk, error) {
				return db.Disk{
					DiskID:     arg.DiskID,
					Name:       arg.Name,
					RootIno:    arg.RootIno,
					MaxBytes:   arg.MaxBytes,
					UsedBytes:  0,
					UsedInodes: 1,
					CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
					UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
				}, nil
			}).
			Once()

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     maxName,
			MaxBytes: 1,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		assert.Equal(t, maxName, disk.Name)
		assert.Equal(t, uint64(1), disk.MaxBytes)
	})
}

func TestCreateDisk_DatabaseErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("CreateRootInode 失败透传错误并回滚事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db connection failure")
		mockQuerier.EXPECT().
			CreateRootInode(mock.Anything, mock.Anything).
			Return(db.Inode{}, dbErr).
			Once()

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "test-disk",
			MaxBytes: 4096,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create root inode: db connection failure")
	})

	t.Run("CreateDisk 失败透传错误并回滚事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			CreateRootInode(mock.Anything, mock.Anything).
			Return(db.Inode{Ino: 5005}, nil).
			Once()

		dbErr := errors.New("disk insertion failure")
		mockQuerier.EXPECT().
			CreateDisk(mock.Anything, mock.Anything).
			Return(db.Disk{}, dbErr).
			Once()

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "test-disk",
			MaxBytes: 4096,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create disk: disk insertion failure")
	})

	t.Run("CreateDisk 唯一约束冲突 23505 映射为 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			CreateRootInode(mock.Anything, mock.Anything).
			Return(db.Inode{Ino: 6006}, nil).
			Once()

		pgConflictErr := &pgconn.PgError{
			Code:    "23505",
			Message: "duplicate key value violates unique constraint \"disks_root_ino_key\"",
		}
		mockQuerier.EXPECT().
			CreateDisk(mock.Anything, mock.Anything).
			Return(db.Disk{}, pgConflictErr).
			Once()

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "test-disk",
			MaxBytes: 4096,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrAlreadyExists))
		assert.Contains(t, err.Error(), "root_ino 6006 already exists")
	})

	t.Run("ExecTxWithOptions 失败透传事务错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		txErr := errors.New("begin transaction failed")
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			Return(txErr).
			Once()

		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     "test-disk",
			MaxBytes: 4096,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, txErr))
	})
}

func TestGetDisk(t *testing.T) {
	ctx := context.Background()
	const (
		validDiskIDStr = "0192a3b4-c5d6-7e8f-9012-3456789abcde"
		nilDiskIDStr   = "00000000-0000-0000-0000-000000000000"
	)

	var validDiskID pgtype.UUID
	err := validDiskID.Scan(validDiskIDStr)
	require.NoError(t, err)

	now := time.Now().Truncate(time.Microsecond)
	sampleDBDisk := db.Disk{
		DiskID:     validDiskID,
		Name:       "primary-disk",
		RootIno:    2001,
		MaxBytes:   20 * 1024 * 1024 * 1024,
		UsedBytes:  5 * 1024 * 1024 * 1024,
		UsedInodes: 10,
		CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("成功查询存在的磁盘详情", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validDiskID).
			Return(sampleDBDisk, nil).
			Once()

		mgr := meta.New(mockStore)
		result, err := mgr.GetDisk(ctx, validDiskIDStr)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, validDiskIDStr, result.DiskID)
		assert.Equal(t, sampleDBDisk.Name, result.Name)
		assert.Equal(t, uint64(sampleDBDisk.RootIno), result.RootIno)
		assert.Equal(t, uint64(sampleDBDisk.MaxBytes), result.MaxBytes)
		assert.Equal(t, uint64(sampleDBDisk.UsedBytes), result.UsedBytes)
		assert.Equal(t, uint64(sampleDBDisk.UsedInodes), result.UsedInodes)
		assert.Equal(t, now, result.CreatedAt)
		assert.Equal(t, now, result.UpdatedAt)
	})

	t.Run("静态校验拦截空 UUID 且不调用底层数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)

		mgr := meta.New(mockStore)
		result, err := mgr.GetDisk(ctx, "")

		require.Error(t, err)
		assert.Nil(t, result)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "invalid disk id")
		mockStore.AssertNotCalled(t, "GetDisk", mock.Anything, mock.Anything)
	})

	t.Run("静态校验拦截非法格式 UUID 且不调用底层数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)

		mgr := meta.New(mockStore)
		result, err := mgr.GetDisk(ctx, "invalid-uuid-format")

		require.Error(t, err)
		assert.Nil(t, result)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "invalid disk id")
		mockStore.AssertNotCalled(t, "GetDisk", mock.Anything, mock.Anything)
	})

	t.Run("静态校验拦截 Nil UUID 且不调用底层数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)

		mgr := meta.New(mockStore)
		result, err := mgr.GetDisk(ctx, nilDiskIDStr)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "disk id cannot be nil uuid")
		mockStore.AssertNotCalled(t, "GetDisk", mock.Anything, mock.Anything)
	})

	t.Run("磁盘不存在时映射 pgx.ErrNoRows 为 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validDiskID).
			Return(db.Disk{}, pgx.ErrNoRows).
			Once()

		mgr := meta.New(mockStore)
		result, err := mgr.GetDisk(ctx, validDiskIDStr)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("底层数据库其他错误透明透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		dbErr := errors.New("database connection refused")
		mockStore.EXPECT().
			GetDisk(mock.Anything, validDiskID).
			Return(db.Disk{}, dbErr).
			Once()

		mgr := meta.New(mockStore)
		result, err := mgr.GetDisk(ctx, validDiskIDStr)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.True(t, errors.Is(err, dbErr))
		assert.False(t, errors.Is(err, meta.ErrNotFound))
	})
}
