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
	"google.golang.org/grpc/codes"

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

func TestUpdateDisk_StaticValidation(t *testing.T) {
	ctx := context.Background()
	const validDiskIDStr = "0192a3b4-c5d6-7e8f-9012-3456789abcde"

	t.Run("非法 disk_id 拦截且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)
		name := "valid-name"

		for _, badID := range []string{"", "not-a-uuid", "12345678-1234-1234-1234", "00000000-0000-0000-0000-000000000000"} {
			disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
				DiskID: badID,
				Name:   &name,
			})
			require.Nil(t, disk)
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		}
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("name 与 max_bytes 均未指定时返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID: validDiskIDStr,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "at least one of name or max_bytes must be set")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("max_bytes 为 0 返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)
		zeroCap := uint64(0)

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID:   validDiskIDStr,
			MaxBytes: &zeroCap,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "max_bytes must be in range")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("max_bytes 超过 math.MaxInt64 返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)
		overflowCap := uint64(math.MaxInt64) + 1

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID:   validDiskIDStr,
			MaxBytes: &overflowCap,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "max_bytes must be in range")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("数值边界优先于名称校验：max_bytes 非法且名称超长时返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)
		zeroCap := uint64(0)
		tooLongName := strings.Repeat("a", 256)

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID:   validDiskIDStr,
			Name:     &tooLongName,
			MaxBytes: &zeroCap,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "max_bytes must be in range")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("name 长度超过 255 字节返回 ErrNameTooLong 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)
		tooLongName := strings.Repeat("a", 256)

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID: validDiskIDStr,
			Name:   &tooLongName,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNameTooLong))
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("name 包含空字节返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)
		badName := "bad\x00name"

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID: validDiskIDStr,
			Name:   &badName,
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
		name := strings.Repeat("a", 255) + "\x00"

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID: validDiskIDStr,
			Name:   &name,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNameTooLong))
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})
}

func TestUpdateDisk_BusinessLogic(t *testing.T) {
	ctx := context.Background()
	const validDiskIDStr = "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	now := time.Now().Truncate(time.Microsecond)

	var validDiskID pgtype.UUID
	require.NoError(t, validDiskID.Scan(validDiskIDStr))

	t.Run("目标磁盘不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{}, pgx.ErrNoRows).
			Once()

		newName := "new-disk-name"
		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID: validDiskIDStr,
			Name:   &newName,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("缩容至当前已用空间以下触发保护并返回 ErrInvalidShrink", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		currentDiskRow := db.Disk{
			DiskID:     validDiskID,
			Name:       "old-name",
			RootIno:    101,
			MaxBytes:   10000,
			UsedBytes:  5000,
			UsedInodes: 10,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(currentDiskRow, nil).
			Once()

		// 试图将容量缩容至 4999（小于已用 5000）
		shrinkCap := uint64(4999)
		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID:   validDiskIDStr,
			MaxBytes: &shrinkCap,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidShrink))
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))

		// 状态码验证
		st := meta.ToStatus(err)
		require.NotNil(t, st)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("允许缩容至恰好等于当前已用空间（边界水位线）", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		currentDiskRow := db.Disk{
			DiskID:     validDiskID,
			Name:       "disk-1",
			RootIno:    101,
			MaxBytes:   10000,
			UsedBytes:  5000,
			UsedInodes: 10,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(currentDiskRow, nil).
			Once()

		exactCap := uint64(5000)
		mockQuerier.EXPECT().
			UpdateDisk(mock.Anything, db.UpdateDiskParams{
				DiskID:   validDiskID,
				Name:     "disk-1",
				MaxBytes: 5000,
			}).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       "disk-1",
				RootIno:    101,
				MaxBytes:   5000,
				UsedBytes:  5000,
				UsedInodes: 10,
				CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:  pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
			}, nil).
			Once()

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID:   validDiskIDStr,
			MaxBytes: &exactCap,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		assert.Equal(t, uint64(5000), disk.MaxBytes)
		assert.Equal(t, "disk-1", disk.Name)
	})

	t.Run("仅更新 name 时保持原 max_bytes 不变", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		currentDiskRow := db.Disk{
			DiskID:     validDiskID,
			Name:       "old-name",
			RootIno:    101,
			MaxBytes:   20000,
			UsedBytes:  1000,
			UsedInodes: 5,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(currentDiskRow, nil).
			Once()

		newName := "renamed-disk"
		mockQuerier.EXPECT().
			UpdateDisk(mock.Anything, db.UpdateDiskParams{
				DiskID:   validDiskID,
				Name:     newName,
				MaxBytes: 20000,
			}).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       newName,
				RootIno:    101,
				MaxBytes:   20000,
				UsedBytes:  1000,
				UsedInodes: 5,
				CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:  pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
			}, nil).
			Once()

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID: validDiskIDStr,
			Name:   &newName,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		assert.Equal(t, newName, disk.Name)
		assert.Equal(t, uint64(20000), disk.MaxBytes)
	})

	t.Run("仅更新 max_bytes 时保持原 name 不变（扩容）", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		currentDiskRow := db.Disk{
			DiskID:     validDiskID,
			Name:       "stable-name",
			RootIno:    101,
			MaxBytes:   20000,
			UsedBytes:  1000,
			UsedInodes: 5,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(currentDiskRow, nil).
			Once()

		expandedCap := uint64(50000)
		mockQuerier.EXPECT().
			UpdateDisk(mock.Anything, db.UpdateDiskParams{
				DiskID:   validDiskID,
				Name:     "stable-name",
				MaxBytes: 50000,
			}).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       "stable-name",
				RootIno:    101,
				MaxBytes:   50000,
				UsedBytes:  1000,
				UsedInodes: 5,
				CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:  pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
			}, nil).
			Once()

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID:   validDiskIDStr,
			MaxBytes: &expandedCap,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		assert.Equal(t, "stable-name", disk.Name)
		assert.Equal(t, uint64(50000), disk.MaxBytes)
	})

	t.Run("同时更新 name 与 max_bytes 成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		currentDiskRow := db.Disk{
			DiskID:     validDiskID,
			Name:       "initial-name",
			RootIno:    101,
			MaxBytes:   20000,
			UsedBytes:  1000,
			UsedInodes: 5,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(currentDiskRow, nil).
			Once()

		newName := "final-name"
		newCap := uint64(60000)
		mockQuerier.EXPECT().
			UpdateDisk(mock.Anything, db.UpdateDiskParams{
				DiskID:   validDiskID,
				Name:     newName,
				MaxBytes: 60000,
			}).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       newName,
				RootIno:    101,
				MaxBytes:   60000,
				UsedBytes:  1000,
				UsedInodes: 5,
				CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:  pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
			}, nil).
			Once()

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID:   validDiskIDStr,
			Name:     &newName,
			MaxBytes: &newCap,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		assert.Equal(t, newName, disk.Name)
		assert.Equal(t, newCap, disk.MaxBytes)
	})

	t.Run("GetDiskForUpdate 数据库底层错误透明透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("query timeout")
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{}, dbErr).
			Once()

		newName := "new-name"
		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID: validDiskIDStr,
			Name:   &newName,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("UpdateDisk 数据库底层错误透明透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		currentDiskRow := db.Disk{
			DiskID:     validDiskID,
			Name:       "initial-name",
			RootIno:    101,
			MaxBytes:   20000,
			UsedBytes:  1000,
			UsedInodes: 5,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(currentDiskRow, nil).
			Once()

		updateErr := errors.New("disk write failure")
		newName := "fail-name"
		mockQuerier.EXPECT().
			UpdateDisk(mock.Anything, mock.Anything).
			Return(db.Disk{}, updateErr).
			Once()

		disk, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID: validDiskIDStr,
			Name:   &newName,
		})
		require.Nil(t, disk)
		require.Error(t, err)
		assert.True(t, errors.Is(err, updateErr))
	})
}

func TestStatFS_StaticValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("ino 为 0 返回 ErrInvalidArgument 且不查询数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.StatFS(ctx, 0)
		require.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "ino must be between 1 and")
		mockStore.AssertNotCalled(t, "GetInode", mock.Anything, mock.Anything)
	})

	t.Run("ino 超过 math.MaxInt64 返回 ErrInvalidArgument 且不查询数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.StatFS(ctx, uint64(math.MaxInt64)+1)
		require.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "ino must be between 1 and")
		mockStore.AssertNotCalled(t, "GetInode", mock.Anything, mock.Anything)
	})
}

func TestStatFS_DatabaseErrors(t *testing.T) {
	ctx := context.Background()
	const targetIno = uint64(100)

	t.Run("GetInode 返回 pgx.ErrNoRows 时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		mockStore.AssertNotCalled(t, "GetDiskByRootIno", mock.Anything, mock.Anything)
	})

	t.Run("GetInode 返回通用错误时透明透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		dbErr := errors.New("database connection broken")
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{}, dbErr).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
		assert.Contains(t, err.Error(), "get inode 100 for statfs")
		mockStore.AssertNotCalled(t, "GetDiskByRootIno", mock.Anything, mock.Anything)
	})

	t.Run("GetInode 成功但 root_ino <= 0 时返回 ErrNotFound 且不查磁盘", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{
				Ino:     int64(targetIno),
				RootIno: 0,
			}, nil).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		mockStore.AssertNotCalled(t, "GetDiskByRootIno", mock.Anything, mock.Anything)
	})

	t.Run("GetDiskByRootIno 返回 pgx.ErrNoRows 时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{
				Ino:     int64(targetIno),
				RootIno: 1,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, int64(1)).
			Return(db.Disk{}, pgx.ErrNoRows).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("GetDiskByRootIno 返回通用错误时透明透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		dbErr := errors.New("read replica down")
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{
				Ino:     int64(targetIno),
				RootIno: 1,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, int64(1)).
			Return(db.Disk{}, dbErr).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
		assert.Contains(t, err.Error(), "get disk by root_ino 1")
	})
}

func TestStatFS_Calculation(t *testing.T) {
	ctx := context.Background()
	const targetIno = uint64(200)
	const rootIno = int64(1)
	const validDiskIDStr = "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var validDiskID pgtype.UUID
	require.NoError(t, validDiskID.Scan(validDiskIDStr))
	now := time.Now().Truncate(time.Microsecond)

	t.Run("常规容量与已用 Inode 统计正确计算", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		// 10 GiB 容量，已用 1 GiB
		totalBytes := int64(10 * 1024 * 1024 * 1024)
		usedBytes := int64(1024 * 1024 * 1024)
		usedInodes := int64(42)

		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{
				Ino:     int64(targetIno),
				RootIno: rootIno,
				Nlink:   1,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, rootIno).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       "statfs-disk",
				RootIno:    rootIno,
				MaxBytes:   totalBytes,
				UsedBytes:  usedBytes,
				UsedInodes: usedInodes,
				CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.NoError(t, err)
		require.NotNil(t, res)

		assert.Equal(t, uint64(totalBytes), res.TotalBytes)
		assert.Equal(t, uint64(usedBytes), res.UsedBytes)
		assert.Equal(t, uint64(totalBytes-usedBytes), res.AvailBytes)
		assert.Equal(t, uint32(meta.DefaultStatFSBlockSize), res.Bsize)
		assert.Equal(t, uint32(meta.DefaultStatFSBlockSize), res.Frsize)
		assert.Equal(t, uint64(totalBytes)/uint64(meta.DefaultStatFSBlockSize), res.Blocks)
		assert.Equal(t, uint64(totalBytes-usedBytes)/uint64(meta.DefaultStatFSBlockSize), res.Bavail)
		assert.Equal(t, uint64(totalBytes-usedBytes)/uint64(meta.DefaultStatFSBlockSize), res.Bfree)
		assert.Equal(t, uint64(usedInodes), res.Files)
		assert.Equal(t, uint64(0), res.Ffree)
		assert.Equal(t, uint32(meta.MaxDiskNameBytes), res.Namelen)
	})

	t.Run("支持已解链孤儿节点 (nlink == 0) 按 root_ino 正常查询统计", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{
				Ino:     int64(targetIno),
				RootIno: rootIno,
				Nlink:   0, // 孤儿节点
			}, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, rootIno).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       "orphan-disk",
				RootIno:    rootIno,
				MaxBytes:   40960,
				UsedBytes:  8192,
				UsedInodes: 1,
			}, nil).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(40960), res.TotalBytes)
		assert.Equal(t, uint64(8192), res.UsedBytes)
		assert.Equal(t, uint64(32768), res.AvailBytes)
		assert.Equal(t, uint64(10), res.Blocks)
		assert.Equal(t, uint64(8), res.Bavail)
		assert.Equal(t, uint64(8), res.Bfree)
		assert.Equal(t, uint64(1), res.Files)
	})

	t.Run("极小容量 (小于 4096 字节) 保底 blocks 为 1", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{
				Ino:     int64(targetIno),
				RootIno: rootIno,
				Nlink:   1,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, rootIno).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       "tiny-disk",
				RootIno:    rootIno,
				MaxBytes:   100, // 小于 4096
				UsedBytes:  0,
				UsedInodes: 1,
			}, nil).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(100), res.TotalBytes)
		assert.Equal(t, uint64(0), res.UsedBytes)
		assert.Equal(t, uint64(100), res.AvailBytes)
		assert.Equal(t, uint64(1), res.Blocks, "极小容量 blocks 必须至少为 1")
		assert.Equal(t, uint64(0), res.Bavail)
		assert.Equal(t, uint64(0), res.Bfree)
		assert.Equal(t, uint64(1), res.Files)
	})

	t.Run("已用容量等于或超过最大配额时可用空间与数据块钳位归零", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{
				Ino:     int64(targetIno),
				RootIno: rootIno,
				Nlink:   1,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, rootIno).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       "full-disk",
				RootIno:    rootIno,
				MaxBytes:   8192,
				UsedBytes:  8192,
				UsedInodes: 10,
			}, nil).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(8192), res.TotalBytes)
		assert.Equal(t, uint64(8192), res.UsedBytes)
		assert.Equal(t, uint64(0), res.AvailBytes)
		assert.Equal(t, uint64(2), res.Blocks)
		assert.Equal(t, uint64(0), res.Bavail)
		assert.Equal(t, uint64(0), res.Bfree)
		assert.Equal(t, uint64(10), res.Files)
	})

	t.Run("used_inodes 为 0 时 files 设为 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(targetIno)).
			Return(db.Inode{
				Ino:     int64(targetIno),
				RootIno: rootIno,
				Nlink:   1,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, rootIno).
			Return(db.Disk{
				DiskID:     validDiskID,
				Name:       "zero-inodes-disk",
				RootIno:    rootIno,
				MaxBytes:   8192,
				UsedBytes:  0,
				UsedInodes: 0,
			}, nil).
			Once()

		mgr := meta.New(mockStore)
		res, err := mgr.StatFS(ctx, targetIno)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(0), res.Files)
		assert.Equal(t, uint64(0), res.Ffree)
	})
}

