package meta_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/starsrc/Aether/pkg/tx"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/server"
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

func TestListDisks_Unit(t *testing.T) {
	ctx := context.Background()

	t.Run("page_size > 1000 返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.ListDisks(ctx, meta.ListDisksParams{
			PageSize: 1001,
		})
		require.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		mockStore.AssertNotCalled(t, "ListDisks", mock.Anything, mock.Anything)
	})

	t.Run("非法 page_token 拦截并返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		badTokens := []string{
			"!!!not-base64!!!",
			"e30",                                // "{}" 缺少 id
			"eyJ0IjoxLCJpZCI6Im5vdC1hLXV1aWQifQ", // {"t":1,"id":"not-a-uuid"}
			"eyJ0IjoxLCJpZCI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCJ9", // {"t":1,"id":"00000000-0000-0000-0000-000000000000"} Nil UUID
			"eyJpZCI6IjAxOTJhM2I0LWM1ZDYtN2U4Zi05MDEyLTM0NTY3ODlhYmNkZSJ9",         // {"id":"0192a3b4-c5d6-7e8f-9012-3456789abcde"} 缺少 t
		}
		for _, tok := range badTokens {
			res, err := mgr.ListDisks(ctx, meta.ListDisksParams{
				PageSize:  10,
				PageToken: tok,
			})
			require.Nil(t, res, "token=%s", tok)
			require.Error(t, err, "token=%s", tok)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument), "token=%s, err=%v", tok, err)
		}
		mockStore.AssertNotCalled(t, "ListDisks", mock.Anything, mock.Anything)
	})

	t.Run("page_size 为 0 时默认使用 50 并请求 limit=51，空列表返回空切片与空 next_page_token", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ListDisks(mock.Anything, db.ListDisksParams{
				HasCursor: false,
				PageLimit: int32(meta.DefaultListDisksPageSize + 1),
			}).
			Return(nil, nil).
			Once()

		res, err := mgr.ListDisks(ctx, meta.ListDisksParams{})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Empty(t, res.Disks)
		assert.Equal(t, "", res.NextPageToken)
	})

	t.Run("返回行数超过 page_size 时截断前 page_size 条并生成可还原的 next_page_token", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		var id1, id2, id3 pgtype.UUID
		require.NoError(t, id1.Scan("0192a3b4-c5d6-7e8f-9012-3456789abcd3"))
		require.NoError(t, id2.Scan("0192a3b4-c5d6-7e8f-9012-3456789abcd2"))
		require.NoError(t, id3.Scan("0192a3b4-c5d6-7e8f-9012-3456789abcd1"))

		t1 := time.Date(2026, 10, 29, 12, 0, 2, 123456000, time.UTC)
		t2 := time.Date(2026, 10, 29, 12, 0, 1, 654321000, time.UTC)
		t3 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

		// 第一页：page_size=2，底层查询 PageLimit=3，返回 3 条
		mockStore.EXPECT().
			ListDisks(mock.Anything, db.ListDisksParams{
				HasCursor: false,
				PageLimit: 3,
			}).
			Return([]db.Disk{
				{DiskID: id1, Name: "d1", RootIno: 101, MaxBytes: 1024, CreatedAt: pgtype.Timestamptz{Time: t1, Valid: true}, UpdatedAt: pgtype.Timestamptz{Time: t1, Valid: true}},
				{DiskID: id2, Name: "d2", RootIno: 102, MaxBytes: 2048, CreatedAt: pgtype.Timestamptz{Time: t2, Valid: true}, UpdatedAt: pgtype.Timestamptz{Time: t2, Valid: true}},
				{DiskID: id3, Name: "d3", RootIno: 103, MaxBytes: 4096, CreatedAt: pgtype.Timestamptz{Time: t3, Valid: true}, UpdatedAt: pgtype.Timestamptz{Time: t3, Valid: true}},
			}, nil).
			Once()

		page1, err := mgr.ListDisks(ctx, meta.ListDisksParams{PageSize: 2})
		require.NoError(t, err)
		require.NotNil(t, page1)
		require.Len(t, page1.Disks, 2)
		assert.Equal(t, "d1", page1.Disks[0].Name)
		assert.Equal(t, "d2", page1.Disks[1].Name)
		require.NotEmpty(t, page1.NextPageToken)

		// 第二页：传入 page1.NextPageToken，验证解码出的 CursorCreatedAt 与 CursorDiskID 严格对应第 2 条记录 (t2, id2)
		mockStore.EXPECT().
			ListDisks(mock.Anything, mock.MatchedBy(func(arg db.ListDisksParams) bool {
				return arg.HasCursor &&
					arg.CursorCreatedAt.Valid &&
					arg.CursorCreatedAt.Time.Equal(t2) &&
					arg.CursorDiskID == id2 &&
					arg.PageLimit == 3
			})).
			Return([]db.Disk{
				{DiskID: id3, Name: "d3", RootIno: 103, MaxBytes: 4096, CreatedAt: pgtype.Timestamptz{Time: t3, Valid: true}, UpdatedAt: pgtype.Timestamptz{Time: t3, Valid: true}},
			}, nil).
			Once()

		page2, err := mgr.ListDisks(ctx, meta.ListDisksParams{
			PageSize:  2,
			PageToken: page1.NextPageToken,
		})
		require.NoError(t, err)
		require.NotNil(t, page2)
		require.Len(t, page2.Disks, 1)
		assert.Equal(t, "d3", page2.Disks[0].Name)
		assert.Equal(t, "", page2.NextPageToken)
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

func TestDeleteDisk_StaticValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("disk_id 为空串返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		err := mgr.DeleteDisk(ctx, "")
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "invalid disk id")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("disk_id 为非法 UUID 格式返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		err := mgr.DeleteDisk(ctx, "not-a-valid-uuid")
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "invalid disk id")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})

	t.Run("disk_id 为全零 Nil UUID 返回 ErrInvalidArgument 且不触达数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		err := mgr.DeleteDisk(ctx, "00000000-0000-0000-0000-000000000000")
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		assert.Contains(t, err.Error(), "disk id cannot be nil uuid")
		mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	})
}

func TestDeleteDisk_MockStore(t *testing.T) {
	ctx := context.Background()
	const validDiskIDStr = "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var validDiskID pgtype.UUID
	require.NoError(t, validDiskID.Scan(validDiskIDStr))
	const rootIno = int64(1001)

	t.Run("目标磁盘不存在 (GetDiskForUpdate 返回 ErrNoRows) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{}, pgx.ErrNoRows).
			Once()

		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("GetDiskForUpdate 数据库错误透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db connection failure")
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{}, dbErr).
			Once()

		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("DeleteDisk 返回 0 行受影响返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}).
			Return([]int64(nil), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteDisk(mock.Anything, validDiskID).
			Return(int64(0), nil).
			Once()

		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("DeleteDisk 数据库错误透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}).
			Return([]int64(nil), nil).
			Once()

		dbErr := errors.New("delete disk internal error")
		mockQuerier.EXPECT().
			DeleteDisk(mock.Anything, validDiskID).
			Return(int64(0), dbErr).
			Once()

		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("DeleteInodesByRootIno 数据库错误透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}).
			Return([]int64(nil), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteDisk(mock.Anything, validDiskID).
			Return(int64(1), nil).
			Once()

		dbErr := errors.New("delete inodes failure")
		mockQuerier.EXPECT().
			DeleteInodesByRootIno(mock.Anything, rootIno).
			Return(nil, dbErr).
			Once()

		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("正常删除依次调用 GetDiskForUpdate -> ListActiveSessionIDsByDisk -> DeleteDisk -> DeleteInodesByRootIno", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}).
			Return([]int64(nil), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteDisk(mock.Anything, validDiskID).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodesByRootIno(mock.Anything, rootIno).
			Return([]int64{rootIno, 1002, 1003}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, []int64{rootIno, 1002, 1003}).
			Return([][]byte(nil), nil).
			Once()

		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.NoError(t, err)
	})

	t.Run("存在活跃会话且 WatchHub 探活回复 Pong 时返回 ErrBusy 且不执行 DeleteDisk", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		setupMockWriteTx(mockStore, mockQuerier)

		activeSID := uint64(80801)
		var streamID uint64
		streamID, unregister := mgr.WatchHub().Register(activeSID, func(pingSeq uint64) error {
			go mgr.WatchHub().HandlePong(activeSID, streamID, pingSeq)
			return nil
		})
		defer unregister()

		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}).
			Return([]int64{int64(activeSID)}, nil).
			Once()

		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrBusy)
		mockQuerier.AssertNotCalled(t, "DeleteDisk", mock.Anything, mock.Anything)
	})

	t.Run("存在活跃会话但 WatchHub 探活超时未响应 Pong 时自动清理死会话并在下一轮成功删除磁盘", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithSessionProbeTimeout(30*time.Millisecond))

		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				return fn(mockQuerier)
			})

		deadSID := uint64(80802)
		_, unregister := mgr.WatchHub().Register(deadSID, func(pingSeq uint64) error {
			// 模拟客户端死锁不回复 Pong
			return nil
		})
		defer unregister()

		// 第一轮：发现活跃流，回滚事务触发探活
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}).
			Return([]int64{int64(deadSID)}, nil).
			Once()

		// 探活超时后触发 CloseSession(deadSID)
		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteClientSessionByID(mock.Anything, int64(deadSID)).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		// 第二轮：死会话已被 Evict 和 CloseSession，成功删除磁盘
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}).
			Return([]int64(nil), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteDisk(mock.Anything, validDiskID).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodesByRootIno(mock.Anything, rootIno).
			Return([]int64{rootIno}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, []int64{rootIno}).
			Return([][]byte(nil), nil).
			Once()

		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.NoError(t, err)
		assert.False(t, mgr.WatchHub().HasActiveStream(deadSID), "超时死会话的流应已被 Evict")
	})

	t.Run("DeleteDisk 删除磁盘后唤醒并中断阻塞在目标 Inode 上的 SetLkw 等待者返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetIno := int64(2001)
		targetFh := int64(11)
		sessionID := int64(9999)
		requestID := int64(8888)
		owner := int64(7777)

		// 事务执行模拟：所有 ExecTxWithOptions 均直接执行回调函数
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				return fn(mockQuerier)
			})

		baseInode := db.Inode{
			Ino:   targetIno,
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
		}

		// 1. SetLkw 第一次循环（因写锁冲突登记为等待者并阻塞）
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, targetIno).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileLockWaiter(mock.Anything, db.GetFileLockWaiterParams{
				SessionID: sessionID,
				RequestID: requestID,
			}).
			Return(db.FileLockWaiter{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        targetFh,
				Ino:       targetIno,
				SessionID: pgtype.Int8{Int64: sessionID, Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    targetFh,
				Ino:   targetIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
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

		// 模拟存在冲突锁
		mockQuerier.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     targetIno,
				LkFlags: 0,
			}).
			Return([]db.FileLock{
				{
					ID:          1,
					SessionID:   sessionID + 1,
					Ino:         targetIno,
					Fh:          targetFh + 1,
					LockOwner:   owner + 1,
					LkFlags:     0,
					StartOffset: 0,
					EndOffset:   100,
					LockType:    int16(meta.F_WRLCK),
					Pid:         1001,
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListPendingFileLockWaitersByInoAndFlags(mock.Anything, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     targetIno,
				LkFlags: 0,
			}).
			Return([]db.FileLockWaiter(nil), nil).
			Once()

		waiterRegistered := make(chan struct{})
		mockQuerier.EXPECT().
			UpsertFileLockWaiter(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, arg db.UpsertFileLockWaiterParams) (db.FileLockWaiter, error) {
				close(waiterRegistered)
				return db.FileLockWaiter{
					SessionID: sessionID,
					RequestID: requestID,
					Ino:       targetIno,
					Status:    meta.WaiterStatusPending,
				}, nil
			}).
			Once()

		// 2. DeleteDisk 预期执行
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, validDiskID).
			Return(db.Disk{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
				DiskID:  validDiskID,
				RootIno: rootIno,
			}).
			Return([]int64(nil), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteDisk(mock.Anything, validDiskID).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodesByRootIno(mock.Anything, rootIno).
			Return([]int64{rootIno, targetIno}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, []int64{rootIno, targetIno}).
			Return([][]byte(nil), nil).
			Once()

		// 3. SetLkw 被 DeleteDisk 唤醒后进入第二轮循环，因 Inode 已被删除返回 pgx.ErrNoRows -> ErrNotFound
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, targetIno).
			Return(db.Inode{}, pgx.ErrNoRows).
			Maybe()

		mockQuerier.EXPECT().
			GetFileLockWaiter(mock.Anything, db.GetFileLockWaiterParams{
				SessionID: sessionID,
				RequestID: requestID,
			}).
			Return(db.FileLockWaiter{}, pgx.ErrNoRows).
			Maybe()

		// 启动后台协程发起阻塞加锁请求
		errCh := make(chan error, 1)
		lkwCtx, lkwCancel := context.WithTimeout(ctx, 5*time.Second)
		defer lkwCancel()
		go func() {
			errCh <- mgr.SetLkw(lkwCtx, meta.SetLkwParams{
				Ino:       uint64(targetIno),
				Fh:        uint64(targetFh),
				LockOwner: uint64(owner),
				SessionID: uint64(sessionID),
				RequestID: uint64(requestID),
				Lk: meta.FileLock{
					Start: 0,
					End:   100,
					Type:  meta.F_WRLCK,
					PID:   2002,
				},
				LkFlags: 0,
			})
		}()

		// 等待 SetLkw 完成第一轮事务并登记等待者
		select {
		case <-waiterRegistered:
		case <-time.After(3 * time.Second):
			t.Fatal("等待 waiterRegistered 超时")
		}
		time.Sleep(10 * time.Millisecond)

		// 主协程执行 DeleteDisk
		err := mgr.DeleteDisk(ctx, validDiskIDStr)
		require.NoError(t, err)

		// 验证阻塞中的 SetLkw 立即被唤醒并返回 ErrNotFound
		select {
		case lkwErr := <-errCh:
			require.Error(t, lkwErr)
			assert.True(t, errors.Is(lkwErr, meta.ErrNotFound), "期望得到 ErrNotFound，实际得到: %v", lkwErr)
		case <-time.After(2 * time.Second):
			t.Fatal("SetLkw 超时未被唤醒")
		}
	})
}

func TestDeleteDisk_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)

	store := db.NewStore(pool)
	mgr := meta.New(store)

	t.Run("空盘创建后直接删除，验证 GetDisk 与 GetAttr 均返回 ErrNotFound", func(t *testing.T) {
		diskName := fmt.Sprintf("test_del_empty_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)

		// 确保删除前可查
		fetchedDisk, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, disk.DiskID, fetchedDisk.DiskID)

		rootAttr, err := mgr.GetAttr(ctx, disk.RootIno)
		require.NoError(t, err)
		assert.Equal(t, disk.RootIno, rootAttr.Ino)

		// 执行 DeleteDisk
		err = mgr.DeleteDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		// 删除后再次查询应返回 ErrNotFound
		_, err = mgr.GetDisk(ctx, disk.DiskID)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))

		_, err = mgr.GetAttr(ctx, disk.RootIno)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("非空目录树（多层子目录、常规文件、符号链接、扩展属性）强制级联删除", func(t *testing.T) {
		diskName := fmt.Sprintf("test_del_tree_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024 * 1024,
		})
		require.NoError(t, err)
		rootIno := disk.RootIno

		// 1. 创建子目录 dir1
		dir1, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("dir1"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		// 2. 在 dir1 下创建子目录 dir2
		dir2, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: dir1.Ino,
			Name:      []byte("dir2"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		// 3. 在 dir2 下创建常规文件 file.txt
		fileRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: dir2.Ino,
			Name:      []byte("file.txt"),
			Mode:      0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		fileIno := fileRes.Inode.Ino

		// 4. 为常规文件设置扩展属性
		err = mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   fileIno,
			Name:  []byte("user.test_attr"),
			Value: []byte("attr_value_123"),
			Flags: meta.XattrCreate,
		})
		require.NoError(t, err)

		// 5. 在 rootIno 下创建指向 file.txt 的符号链接 symlink1
		symlink, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: rootIno,
			Name:      []byte("symlink1"),
			Target:    []byte("dir1/dir2/file.txt"),
		})
		require.NoError(t, err)

		// 验证节点存在
		_, err = mgr.GetAttr(ctx, dir1.Ino)
		require.NoError(t, err)
		_, err = mgr.GetAttr(ctx, dir2.Ino)
		require.NoError(t, err)
		_, err = mgr.GetAttr(ctx, fileIno)
		require.NoError(t, err)
		_, err = mgr.GetAttr(ctx, symlink.Ino)
		require.NoError(t, err)

		// 验证扩展属性存在
		xval, err := mgr.GetXattr(ctx, fileIno, []byte("user.test_attr"))
		require.NoError(t, err)
		assert.Equal(t, []byte("attr_value_123"), xval)

		// 执行 DeleteDisk 强制级联删除
		err = mgr.DeleteDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		// 验证所有节点均已被清理
		_, err = mgr.GetDisk(ctx, disk.DiskID)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		for _, ino := range []uint64{rootIno, dir1.Ino, dir2.Ino, fileIno, symlink.Ino} {
			_, err = mgr.GetAttr(ctx, ino)
			assert.True(t, errors.Is(err, meta.ErrNotFound), "ino %d 应已被级联删除", ino)
		}

		// 扩展属性应无法查到
		_, err = mgr.GetXattr(ctx, fileIno, []byte("user.test_attr"))
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("含活跃打开 FileHandle 和 FileLock 时的强制级联删除", func(t *testing.T) {
		diskName := fmt.Sprintf("test_del_locks_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024 * 1024,
		})
		require.NoError(t, err)
		rootIno := disk.RootIno

		sessionID := uint64(time.Now().UnixNano() & 0x7FFFFFFFFFFFFFFF)

		// 1. 创建常规文件
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("locked_file.bin"),
			Mode:      0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		fh := createRes.Fh

		// 2. 对文件加锁
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh,
			LockOwner: 12345,
			SessionID: sessionID,
			Lk: meta.FileLock{
				Type:  meta.F_WRLCK,
				Start: 0,
				End:   100,
				PID:   999,
			},
			LkFlags: 0,
		})
		require.NoError(t, err)

		// 验证锁存在
		lk, err := mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       fileIno,
			Fh:        fh,
			LockOwner: 54321, // 探测者 owner 不同
			SessionID: sessionID,
			Lk: meta.FileLock{
				Type:  meta.F_WRLCK,
				Start: 0,
				End:   100,
				PID:   888,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, meta.F_WRLCK, lk.Type)

		// 3. 执行 DeleteDisk 强制级联删除
		err = mgr.DeleteDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		// 4. 验证 Disk、Inode 均被清理
		_, err = mgr.GetDisk(ctx, disk.DiskID)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		_, err = mgr.GetAttr(ctx, fileIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		_, err = mgr.GetAttr(ctx, rootIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("DeleteDisk 强制删除时立即唤醒并中断阻塞中的 SetLkw 等待者返回 ErrNotFound", func(t *testing.T) {
		diskName := fmt.Sprintf("test_del_setlkw_wake_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024 * 1024,
		})
		require.NoError(t, err)
		rootIno := disk.RootIno

		s1 := uint64(time.Now().UnixNano() & 0x7FFFFFFFFFFFFFFF)
		s2 := s1 + 1
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(s1), int64(s2))
		}()

		// 1. 创建常规文件
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("locked_waiter_file.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &s1,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		fh1 := createRes.Fh

		// 2. 会话 2 打开该文件
		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &s2,
		})
		require.NoError(t, err)
		fh2 := openRes.Fh

		// 3. 会话 1 持有排他写锁 SetLk(F_WRLCK)
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 1001,
			SessionID: s1,
			Lk: meta.FileLock{
				Type:  meta.F_WRLCK,
				Start: 0,
				End:   meta.OffsetMax,
				PID:   1234,
			},
			LkFlags: 0,
		})
		require.NoError(t, err)

		// 4. 会话 2 在后台 goroutine 中发起阻塞写锁请求 SetLkw(F_WRLCK)
		errCh := make(chan error, 1)
		lkwCtx, lkwCancel := context.WithTimeout(ctx, 5*time.Second)
		defer lkwCancel()
		go func() {
			errCh <- mgr.SetLkw(lkwCtx, meta.SetLkwParams{
				Ino:       fileIno,
				Fh:        fh2,
				LockOwner: 2002,
				SessionID: s2,
				RequestID: 7701,
				Lk: meta.FileLock{
					Type:  meta.F_WRLCK,
					Start: 0,
					End:   meta.OffsetMax,
					PID:   5678,
				},
				LkFlags: 0,
			})
		}()

		// 5. 确认会话 2 已在数据库中登记等待者并进入内存阻塞等待状态
		require.Eventually(t, func() bool {
			waiters, err := store.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
				Ino:     int64(fileIno),
				LkFlags: 0,
			})
			return err == nil && len(waiters) == 1
		}, 3*time.Second, 5*time.Millisecond)

		// 6. 执行 DeleteDisk 强制删除磁盘实体及其所有元数据
		startTime := time.Now()
		err = mgr.DeleteDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		// 7. 验证阻塞中的 SetLkw 立即被唤醒并返回 ErrNotFound (ENOENT)
		select {
		case lkwErr := <-errCh:
			require.Error(t, lkwErr)
			assert.True(t, errors.Is(lkwErr, meta.ErrNotFound), "期望得到 ErrNotFound，实际得到: %v", lkwErr)
		case <-time.After(3 * time.Second):
			t.Fatal("SetLkw 超时未被唤醒")
		}

		elapsed := time.Since(startTime)
		assert.Less(t, elapsed, 1*time.Second, "唤醒时间应远快于轮询超时")

		// 8. 验证 Disk 与文件元数据已被删除
		_, err = mgr.GetDisk(ctx, disk.DiskID)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		_, err = mgr.GetAttr(ctx, fileIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("活跃会话注册了 WatchHub 流并自动回复 Pong 时 DeleteDisk 返回 ErrBusy 且磁盘与会话均完好保留", func(t *testing.T) {
		diskName := fmt.Sprintf("test_del_busy_pong_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sid := uint64(time.Now().UnixNano() & 0x7FFFFFFFFFFFFFFF)
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sid))
		}()

		diskID := disk.DiskID
		_, err = mgr.RenewSessionWithDisk(ctx, sid, &diskID)
		require.NoError(t, err)

		var streamID uint64
		streamID, unregister := mgr.WatchHub().Register(sid, func(pingSeq uint64) error {
			go mgr.WatchHub().HandlePong(sid, streamID, pingSeq)
			return nil
		})
		defer unregister()

		// 执行 DeleteDisk，应被探活拦截返回 ErrBusy
		err = mgr.DeleteDisk(ctx, disk.DiskID)
		require.Error(t, err)
		require.ErrorIs(t, err, meta.ErrBusy)

		// 磁盘与根目录依然完好
		gotDisk, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, disk.DiskID, gotDisk.DiskID)

		_, err = mgr.GetAttr(ctx, disk.RootIno)
		require.NoError(t, err)

		// 会话依然存活
		var count int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("活跃会话注册了 WatchHub 流但死锁不回复 Pong 时在探活超时后强制注销会话并成功删除磁盘", func(t *testing.T) {
		fastMgr := meta.New(store, meta.WithSessionProbeTimeout(100*time.Millisecond))

		diskName := fmt.Sprintf("test_del_dead_session_%d", time.Now().UnixNano())
		disk, err := fastMgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = fastMgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sid := uint64(time.Now().UnixNano() & 0x7FFFFFFFFFFFFFFF)
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sid))
		}()

		diskID := disk.DiskID
		_, err = fastMgr.RenewSessionWithDisk(ctx, sid, &diskID)
		require.NoError(t, err)

		// 注册一个不回复 Pong 的流（模拟死锁/半开连接）
		_, unregister := fastMgr.WatchHub().Register(sid, func(pingSeq uint64) error {
			return nil
		})
		defer unregister()

		err = fastMgr.DeleteDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		// 磁盘与根目录已被删除
		_, err = fastMgr.GetDisk(ctx, disk.DiskID)
		require.ErrorIs(t, err, meta.ErrNotFound)
		_, err = fastMgr.GetAttr(ctx, disk.RootIno)
		require.ErrorIs(t, err, meta.ErrNotFound)

		// 死会话已被强制清理
		var count int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.False(t, fastMgr.WatchHub().HasActiveStream(sid))
	})

	t.Run("活跃会话已执行 CloseSession 或无活跃 WatchHub 流时零等待立即删除磁盘", func(t *testing.T) {
		diskName := fmt.Sprintf("test_del_closed_or_nostream_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sid1 := uint64(time.Now().UnixNano() & 0x7FFFFFFFFFFFFFFF)
		sid2 := sid1 + 1
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sid1), int64(sid2))
		}()

		diskID := disk.DiskID
		_, err = mgr.RenewSessionWithDisk(ctx, sid1, &diskID)
		require.NoError(t, err)
		_, err = mgr.RenewSessionWithDisk(ctx, sid2, &diskID)
		require.NoError(t, err)

		// sid1 正常卸载调用 CloseSession，sid2 断开流（无 WatchHub 注册）
		require.NoError(t, mgr.CloseSession(ctx, sid1))

		start := time.Now()
		err = mgr.DeleteDisk(ctx, disk.DiskID)
		elapsed := time.Since(start)
		require.NoError(t, err)
		assert.Less(t, elapsed, 500*time.Millisecond, "无活跃流时无需等待 1.5s 探活超时，应立即删除")

		_, err = mgr.GetDisk(ctx, disk.DiskID)
		require.ErrorIs(t, err, meta.ErrNotFound)

		var count int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id IN ($1, $2)", int64(sid1), int64(sid2)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "disks ON DELETE CASCADE 应自动级联清理绑定该 disk_id 的残留会话")
	})
}

// TestWriteOps_DeadlockRetry_Unit 验证写操作与 DeleteDisk/配额等并发竞争触发 40P01 死锁时，
// 通过 execWriteTxWithRetry 自动重试成功或在目标已被删除时优雅返回 ErrNotFound。
func TestWriteOps_DeadlockRetry_Unit(t *testing.T) {
	ctx := context.Background()
	ino := uint64(100)
	now := time.Now().Truncate(time.Microsecond)

	baseInode := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		RootIno:    1,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	deadlockErr := &pgconn.PgError{
		Code:    "40P01",
		Message: "deadlock detected",
	}

	mockDeadlockOnce := func(mockStore *dbmock.MockStore, mockQuerier db.Querier) *int {
		attempts := 0
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				attempts++
				if attempts == 1 {
					return deadlockErr
				}
				return fn(mockQuerier)
			}).
			Twice()
		return &attempts
	}

	t.Run("SetAttr 与并发事务死锁(40P01)时自动重试并最终成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		attempts := mockDeadlockOnce(mockStore, mockQuerier)

		size2048 := uint64(2048)
		updatedInode := baseInode
		updatedInode.Size = int64(size2048)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, int64(1)).
			Return(db.Disk{
				DiskID:     pgtype.UUID{Valid: true},
				RootIno:    1,
				MaxBytes:   100 * 1024,
				UsedBytes:  1024,
				UsedInodes: 1,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     1,
				DeltaBytes:  1024,
				DeltaInodes: 0,
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == int64(size2048)
			})).
			Return(updatedInode, nil).
			Once()

		res, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: &size2048,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, size2048, res.Attr.Size)
		assert.Equal(t, 2, *attempts, "应当触发 1 次死锁重试后成功")
	})

	t.Run("SetAttr 死锁重试期间目标节点被并发 DeleteDisk 级联删除，自动重试后返回 ErrNotFound 而非原始死锁错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		attempts := mockDeadlockOnce(mockStore, mockQuerier)

		// 重试时，节点已被并发的 DeleteDisk 删除，返回 pgx.ErrNoRows
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		size2048 := uint64(2048)
		res, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: &size2048,
		})
		require.Error(t, err)
		require.Nil(t, res)
		assert.True(t, errors.Is(err, meta.ErrNotFound), "重试后应正确映射为 ErrNotFound，实际为: %v", err)
		assert.Equal(t, 2, *attempts)
	})

	t.Run("Create 与并发事务死锁(40P01)时自动重试并成功创建", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		parentDir := baseInode
		parentDir.Ino = 1
		parentDir.Mode = int32(meta.S_IFDIR | 0o755)
		parentDir.Nlink = 2

		newFile := baseInode
		newFile.Ino = 200
		newFile.Size = 0

		attempts := mockDeadlockOnce(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(parentDir, nil).
			Once()

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, mock.Anything).
			Return(newFile, nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     1,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, mock.Anything).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, mock.Anything).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{Fh: 1, Ino: 200}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("newfile.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(200), res.Inode.Ino)
		assert.Equal(t, 2, *attempts)
	})

	t.Run("SetLk 与并发事务死锁(40P01)时自动重试并最终成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		fh := uint64(10)
		sessionID := uint64(50)
		owner := uint64(1001)

		attempts := mockDeadlockOnce(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(db.FileHandle{
				Fh:        int64(fh),
				Ino:       int64(ino),
				Flags:     meta.O_RDWR,
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}, nil).
			Once()

		// renewClientSession 调用
		setupRenewClientSessionMocks(mockQuerier, sessionID)

		// applyFlockSetLk 调用
		mockQuerier.EXPECT().
			DeleteFlockFileLocksBySessionAndFh(mock.Anything, db.DeleteFlockFileLocksBySessionAndFhParams{
				SessionID: int64(sessionID),
				Ino:       int64(ino),
				Fh:        int64(fh),
			}).
			Return(int64(0), nil).
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
				SessionID:   int64(sessionID),
				Ino:         int64(ino),
				Fh:          int64(fh),
				LockOwner:   int64(owner),
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
			Fh:        fh,
			LockOwner: owner,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   4321,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sessionID,
		})
		require.NoError(t, err)
		assert.Equal(t, 2, *attempts)
	})
}

func TestCrossDisk_TopologyIsolation_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)

	store := db.NewStore(pool)
	mgr := meta.New(store)

	// 1. 创建两块独立磁盘 Disk A 与 Disk B
	diskAName := fmt.Sprintf("disk_a_%d", time.Now().UnixNano())
	diskBName := fmt.Sprintf("disk_b_%d", time.Now().UnixNano())

	diskA, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskAName,
		MaxBytes: 10 * 1024 * 1024,
	})
	require.NoError(t, err)
	require.NotNil(t, diskA)

	diskB, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskBName,
		MaxBytes: 20 * 1024 * 1024,
	})
	require.NoError(t, err)
	require.NotNil(t, diskB)

	defer func() {
		_ = mgr.DeleteDisk(ctx, diskA.DiskID)
		_ = mgr.DeleteDisk(ctx, diskB.DiskID)
	}()

	// 2. 在 Disk A 下创建文件 fileA 和子目录 dirA
	createFileARes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: diskA.RootIno,
		Name:      []byte("fileA.txt"),
		Mode:      0o644,
		Flags:     meta.O_CREAT | meta.O_RDWR,
	})
	require.NoError(t, err)
	fileAIno := createFileARes.Inode.Ino

	// 写入一些内容（通过 SetAttr 扩展 size 以便测试配额）
	size1024 := uint64(1024)
	_, err = mgr.SetAttr(ctx, fileAIno, meta.SetAttrInput{
		Size: &size1024,
	})
	require.NoError(t, err)

	dirARes, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: diskA.RootIno,
		Name:      []byte("dirA"),
		Mode:      0o755,
	})
	require.NoError(t, err)
	dirAIno := dirARes.Ino

	// 在 Disk B 下创建文件 fileB
	createFileBRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: diskB.RootIno,
		Name:      []byte("fileB.txt"),
		Mode:      0o644,
		Flags:     meta.O_CREAT | meta.O_RDWR,
	})
	require.NoError(t, err)
	fileBIno := createFileBRes.Inode.Ino

	size2048 := uint64(2048)
	_, err = mgr.SetAttr(ctx, fileBIno, meta.SetAttrInput{
		Size: &size2048,
	})
	require.NoError(t, err)

	// 3. 验证将 fileA 硬链接（Link）到 Disk B 返回 ErrCrossDevice
	linkedInode, linkErr := mgr.Link(ctx, meta.LinkParams{
		Ino:          fileAIno,
		NewParentIno: diskB.RootIno,
		NewName:      []byte("hardlink_fileA_in_B.txt"),
	})
	require.Nil(t, linkedInode)
	require.Error(t, linkErr)
	assert.True(t, errors.Is(linkErr, meta.ErrCrossDevice), "跨盘 Link 必须返回 ErrCrossDevice")

	// 4. 验证将 fileA 重命名/移动（Rename）到 Disk B 返回 ErrCrossDevice
	renameFileErr := mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: diskA.RootIno,
		OldName:      []byte("fileA.txt"),
		NewParentIno: diskB.RootIno,
		NewName:      []byte("fileA_moved.txt"),
	})
	require.Error(t, renameFileErr)
	assert.True(t, errors.Is(renameFileErr, meta.ErrCrossDevice), "跨盘移动常规文件必须返回 ErrCrossDevice")

	// 验证将 dirA 重命名/移动（Rename）到 Disk B 返回 ErrCrossDevice
	renameDirErr := mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: diskA.RootIno,
		OldName:      []byte("dirA"),
		NewParentIno: diskB.RootIno,
		NewName:      []byte("dirA_moved"),
	})
	require.Error(t, renameDirErr)
	assert.True(t, errors.Is(renameDirErr, meta.ErrCrossDevice), "跨盘移动目录必须返回 ErrCrossDevice")

	// 验证将 fileA 与 fileB 原子互换（RenameExchange）返回 ErrCrossDevice
	renameExchangeErr := mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: diskA.RootIno,
		OldName:      []byte("fileA.txt"),
		NewParentIno: diskB.RootIno,
		NewName:      []byte("fileB.txt"),
		Flags:        meta.RenameExchange,
	})
	require.Error(t, renameExchangeErr)
	assert.True(t, errors.Is(renameExchangeErr, meta.ErrCrossDevice), "跨盘 RenameExchange 必须返回 ErrCrossDevice")

	// 5. 验证删除 Disk A（DeleteDisk）后，Disk B 下的所有目录项与文件完好无损，配额完全独立
	err = mgr.DeleteDisk(ctx, diskA.DiskID)
	require.NoError(t, err)

	// 验证 Disk A 已被完全删除
	_, err = mgr.GetDisk(ctx, diskA.DiskID)
	assert.True(t, errors.Is(err, meta.ErrNotFound))
	_, err = mgr.GetAttr(ctx, fileAIno)
	assert.True(t, errors.Is(err, meta.ErrNotFound))
	_, err = mgr.GetAttr(ctx, dirAIno)
	assert.True(t, errors.Is(err, meta.ErrNotFound))

	// 验证 Disk B 完好无损
	fetchedDiskB, err := mgr.GetDisk(ctx, diskB.DiskID)
	require.NoError(t, err)
	assert.Equal(t, diskB.DiskID, fetchedDiskB.DiskID)
	// Disk B 初始 1 个 root inode + 1 个 fileB = 2 个 used_inodes, used_bytes = 2048
	assert.Equal(t, uint64(2), fetchedDiskB.UsedInodes)
	assert.Equal(t, uint64(2048), fetchedDiskB.UsedBytes)

	// 验证 Disk B 根目录与 fileB 元数据及 Lookup 正常
	rootBAttr, err := mgr.GetAttr(ctx, diskB.RootIno)
	require.NoError(t, err)
	assert.Equal(t, diskB.RootIno, rootBAttr.Ino)

	fileBAttr, err := mgr.GetAttr(ctx, fileBIno)
	require.NoError(t, err)
	assert.Equal(t, fileBIno, fileBAttr.Ino)
	assert.Equal(t, uint64(2048), fileBAttr.Attr.Size)

	lookedUpB, err := mgr.Lookup(ctx, diskB.RootIno, []byte("fileB.txt"))
	require.NoError(t, err)
	assert.Equal(t, fileBIno, lookedUpB.Ino)
}

// TestDisk_DeleteDiskVsSetAttr_ConcurrentNoDeadlock 验证 DeleteDisk 与并发 SetAttr 不得发生 PostgreSQL 40P01 死锁。
func TestDisk_DeleteDiskVsSetAttr_ConcurrentNoDeadlock(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)

	store := db.NewStore(pool)
	mgr := meta.New(store)

	const numDisks = 2
	const filesPerDisk = 6
	var totalSetAttrSamples atomic.Int64

	for d := 0; d < numDisks; d++ {
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-del-vs-setattr-%d-%d", d, time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)

		fileInos := make([]uint64, filesPerDisk)
		for f := 0; f < filesPerDisk; f++ {
			res, err := mgr.Create(ctx, meta.CreateParams{
				ParentIno: disk.RootIno,
				Name:      []byte(fmt.Sprintf("file_%d", f)),
				Mode:      meta.S_IFREG | 0o644,
				Flags:     meta.O_CREAT | meta.O_RDWR,
				UID:       1000,
				GID:       1000,
			})
			require.NoError(t, err)
			fileInos[f] = res.Inode.Ino
		}

		const numSetAttrWorkers = 4
		const setattrLoops = 5
		setattrErrCh := make(chan error, numSetAttrWorkers*setattrLoops)

		var setattrWg sync.WaitGroup
		startCh := make(chan struct{})
		startDeleteCh := make(chan struct{})
		var triggerOnce sync.Once
		triggerDelete := func() {
			triggerOnce.Do(func() {
				close(startDeleteCh)
			})
		}

		for w := 0; w < numSetAttrWorkers; w++ {
			setattrWg.Add(1)
			go func(workerID int) {
				defer setattrWg.Done()
				<-startCh
				for l := 0; l < setattrLoops; l++ {
					ino := fileInos[(workerID+l)%filesPerDisk]
					newSize := uint64(1024 * (l + 1))
					_, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
						Size: &newSize,
					})
					if err == nil {
						triggerDelete()
					}
					setattrErrCh <- err
				}
			}(w)
		}

		// 保底：若所有 worker 均异常未成功，也释放 startDeleteCh 避免死锁
		go func() {
			setattrWg.Wait()
			triggerDelete()
		}()

		var delErr error
		var delWg sync.WaitGroup
		delWg.Add(1)
		go func() {
			defer delWg.Done()
			select {
			case <-startDeleteCh:
			case <-ctx.Done():
				return
			}
			delErr = mgr.DeleteDisk(ctx, disk.DiskID)
		}()

		close(startCh)
		setattrWg.Wait()
		delWg.Wait()
		close(setattrErrCh)

		require.NoError(t, delErr, "每轮 DeleteDisk 调用必须成功")

		for e := range setattrErrCh {
			if e == nil {
				totalSetAttrSamples.Add(1)
				continue
			}
			if errors.Is(e, meta.ErrNotFound) {
				// 删盘后的合法竞争终止态，不计入成功样本
				continue
			}

			var pgErr *pgconn.PgError
			if (errors.As(e, &pgErr) && pgErr.Code == "40P01") || strings.Contains(strings.ToLower(e.Error()), "deadlock") {
				t.Fatalf("检测到 PostgreSQL 40P01 死锁: %v", e)
			}
			t.Fatalf("SetAttr 返回非法错误 (仅允许 nil 或 ErrNotFound): %v", e)
		}

		var diskCount, inodeCount int64
		qErr := pool.QueryRow(ctx, "SELECT count(*) FROM disks WHERE disk_id = $1", disk.DiskID).Scan(&diskCount)
		require.NoError(t, qErr)
		require.Zero(t, diskCount, "被删除磁盘在 disks 表中的记录数必须为 0")

		qErr = pool.QueryRow(ctx, "SELECT count(*) FROM inodes WHERE root_ino = $1", disk.RootIno).Scan(&inodeCount)
		require.NoError(t, qErr)
		require.Zero(t, inodeCount, "被删除磁盘在 inodes 表中的记录数必须为 0")
	}

	require.Greater(t, totalSetAttrSamples.Load(), int64(0), "SetAttr 实际执行完成的有效成功样本数必须大于 0")
	require.NoError(t, ctx.Err())
}

// TestDisk_UpdateDiskShrinkVsExpand_CriticalWatermarkConcurrency 验证真实数据库下磁盘合法缩容与文件扩容在临界水位的并发竞争。
// 严禁发生未恢复的 40P01 死锁与 23514 CHECK 约束泄漏，并发结果必须严格串行化为（扩容先赢 + ErrInvalidShrink）或（缩容先赢 + ErrNoSpace）。
func TestDisk_UpdateDiskShrinkVsExpand_CriticalWatermarkConcurrency(t *testing.T) {
	pool := setupFormalPostgresPool(t)
	store := db.NewStore(pool)
	mgr := meta.New(store)

	const numRounds = 16
	const wLow = uint64(4096)
	const wHigh = uint64(8192)

	assertNoForbiddenPgErrors := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			return
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "40P01" {
				t.Fatalf("检测到未恢复的 PostgreSQL 40P01 死锁: %v", err)
			}
			if pgErr.Code == "23514" {
				t.Fatalf("检测到原始 PostgreSQL 23514 CHECK 约束泄漏: %v", err)
			}
		}
		if strings.Contains(strings.ToLower(err.Error()), "deadlock") {
			t.Fatalf("检测到未恢复的死锁: %v", err)
		}
	}

	subtests := []struct {
		name       string
		expandFunc func(ctx context.Context, mgr meta.Manager, fileIno uint64, fh uint64, targetSize uint64) error
	}{
		{
			name: "expand_via_setattr",
			expandFunc: func(ctx context.Context, mgr meta.Manager, fileIno uint64, fh uint64, targetSize uint64) error {
				_, err := mgr.SetAttr(ctx, fileIno, meta.SetAttrInput{
					Size: &targetSize,
				})
				return err
			},
		},
		{
			name: "expand_via_fallocate",
			expandFunc: func(ctx context.Context, mgr meta.Manager, fileIno uint64, fh uint64, targetSize uint64) error {
				return mgr.Fallocate(ctx, meta.FallocateParams{
					Ino:    fileIno,
					Fh:     fh,
					Offset: 0,
					Length: targetSize,
					Mode:   0,
				})
			},
		},
	}

	for _, tc := range subtests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			var expandWins, shrinkWins int

			for round := 0; round < numRounds; round++ {
				// 1. 每轮新建一个初始容量 MaxBytes = 8192 的独立测试磁盘
				diskName := fmt.Sprintf("disk-wm-%s-r%d-%d", tc.name, round, time.Now().UnixNano())
				disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
					Name:     diskName,
					MaxBytes: wHigh,
				})
				require.NoError(t, err)

				rootIno := int64(disk.RootIno)
				t.Cleanup(func() {
					cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cleanCancel()
					_, _ = pool.Exec(cleanCtx, `DELETE FROM disks WHERE root_ino = $1`, rootIno)
					_, _ = pool.Exec(cleanCtx, `DELETE FROM inodes WHERE root_ino = $1`, rootIno)
				})

				// 2. 核实 CreateDisk 后根目录占用的真实 baseUsedBytes
				fetchedDisk, err := mgr.GetDisk(ctx, disk.DiskID)
				require.NoError(t, err)
				baseUsedBytes := fetchedDisk.UsedBytes
				require.LessOrEqual(t, baseUsedBytes, wLow, "baseUsedBytes 不得超过临界低水位 wLow")

				initialFileSize := wLow - baseUsedBytes
				targetFileSize := initialFileSize + 4096

				// 3. 创建 1 个常规文件并初始化大小，使磁盘当前 UsedBytes == W_low (4096)
				createRes, err := mgr.Create(ctx, meta.CreateParams{
					ParentIno: disk.RootIno,
					Name:      []byte(fmt.Sprintf("file_%d.bin", round)),
					Mode:      meta.S_IFREG | 0o644,
					Flags:     meta.O_CREAT | meta.O_RDWR,
				})
				require.NoError(t, err)
				fileIno := createRes.Inode.Ino
				fh := createRes.Fh

				if initialFileSize > 0 {
					_, err = mgr.SetAttr(ctx, fileIno, meta.SetAttrInput{
						Size: &initialFileSize,
					})
					require.NoError(t, err)
				}

				// 验证竞争前磁盘状态：MaxBytes == W_high, UsedBytes == W_low
				preDisk, err := mgr.GetDisk(ctx, disk.DiskID)
				require.NoError(t, err)
				require.Equal(t, wHigh, preDisk.MaxBytes, "竞争前磁盘 MaxBytes 必须严格为 W_high")
				require.Equal(t, wLow, preDisk.UsedBytes, "竞争前磁盘 UsedBytes 必须严格为 W_low")

				// 4. 并发竞争屏障：协程 A 扩容文件 (+4096)，协程 B 将磁盘 MaxBytes 缩容至 W_low
				var wg sync.WaitGroup
				startCh := make(chan struct{})
				var expandErr, shrinkErr error

				wg.Add(2)
				go func() {
					defer wg.Done()
					<-startCh
					expandErr = tc.expandFunc(ctx, mgr, fileIno, fh, targetFileSize)
				}()

				go func() {
					defer wg.Done()
					<-startCh
					targetMaxBytes := wLow
					_, shrinkErr = mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
						DiskID:   disk.DiskID,
						MaxBytes: &targetMaxBytes,
					})
				}()

				close(startCh)
				wg.Wait()

				// 5. 严格断言：绝不泄露 40P01 死锁，绝不泄露 23514 CHECK 约束违规
				assertNoForbiddenPgErrors(t, expandErr)
				assertNoForbiddenPgErrors(t, shrinkErr)

				// 判定串行化结局
				switch {
				case expandErr == nil && errors.Is(shrinkErr, meta.ErrInvalidShrink):
					expandWins++
					// 扩容先获胜：磁盘 MaxBytes 保持 W_high，UsedBytes 达到 W_high，文件大小为扩容后大小
					postDisk, err := mgr.GetDisk(ctx, disk.DiskID)
					require.NoError(t, err)
					assert.Equal(t, wHigh, postDisk.MaxBytes, "扩容先获胜时 MaxBytes 必须为 W_high")
					assert.Equal(t, wHigh, postDisk.UsedBytes, "扩容先获胜时 UsedBytes 必须为 W_high")

					attr, err := mgr.GetAttr(ctx, fileIno)
					require.NoError(t, err)
					assert.Equal(t, targetFileSize, attr.Attr.Size, "扩容先获胜时文件大小必须为扩容后大小")

				case shrinkErr == nil && errors.Is(expandErr, meta.ErrNoSpace):
					shrinkWins++
					// 缩容先获胜：磁盘 MaxBytes 变为 W_low，UsedBytes 保持 W_low，文件大小保持初始大小
					postDisk, err := mgr.GetDisk(ctx, disk.DiskID)
					require.NoError(t, err)
					assert.Equal(t, wLow, postDisk.MaxBytes, "缩容先获胜时 MaxBytes 必须为 W_low")
					assert.Equal(t, wLow, postDisk.UsedBytes, "缩容先获胜时 UsedBytes 必须为 W_low")

					attr, err := mgr.GetAttr(ctx, fileIno)
					require.NoError(t, err)
					assert.Equal(t, initialFileSize, attr.Attr.Size, "缩容先获胜时文件大小必须保持初始大小未变")

				default:
					t.Fatalf("第 %d 轮并发结果不是合法串行化结局之一: expandErr=%v, shrinkErr=%v", round, expandErr, shrinkErr)
				}
			}

			// 6. 断言总有效成功操作样本数
			totalSuccesses := expandWins + shrinkWins
			assert.Equal(t, numRounds, totalSuccesses, "16 轮合计成功操作数严格为 16")
			assert.Greater(t, totalSuccesses, 0, "总有效成功操作样本数必须大于 0")
			t.Logf("[%s] 16 轮并发结果: 扩容获胜=%d, 缩容获胜=%d, 合计成功=%d", tc.name, expandWins, shrinkWins, totalSuccesses)
		})
	}
}

func TestListDisks_PostgresIntegration(t *testing.T) {
	// 使用独立临时数据库验证全表分页，严禁在共享测试库无条件执行 DELETE FROM disks 污染其他并行测试
	pool, ctx := setupIsolatedMetaTestDB(t, "bowl_list_disks_test")

	store := db.NewStore(pool)
	mgr := meta.New(store)

	// 1. 空表查询
	emptyRes, err := mgr.ListDisks(ctx, meta.ListDisksParams{PageSize: 10})
	require.NoError(t, err)
	require.NotNil(t, emptyRes)
	assert.Empty(t, emptyRes.Disks)
	assert.Equal(t, "", emptyRes.NextPageToken)

	// 2. 构造 5 个磁盘，其中前 2 个具有不同 created_at，中间 2 个具有完全相同的 created_at（验证 disk_id DESC 决胜），最后 1 个最新
	baseTime := time.Date(2026, 10, 29, 15, 0, 0, 123456000, time.UTC)
	sameTime := baseTime.Add(10 * time.Second)
	newestTime := baseTime.Add(20 * time.Second)

	disksToInsert := []struct {
		id        string
		name      string
		createdAt time.Time
	}{
		{id: "0192a3b4-c5d6-7e8f-9012-000000000001", name: "disk-oldest", createdAt: baseTime},
		{id: "0192a3b4-c5d6-7e8f-9012-000000000002", name: "disk-older", createdAt: baseTime.Add(5 * time.Second)},
		{id: "0192a3b4-c5d6-7e8f-9012-000000000003", name: "disk-same-ts-low-id", createdAt: sameTime},
		{id: "0192a3b4-c5d6-7e8f-9012-000000000004", name: "disk-same-ts-high-id", createdAt: sameTime},
		{id: "0192a3b4-c5d6-7e8f-9012-000000000005", name: "disk-newest", createdAt: newestTime},
	}

	var createdDiskIDs []string
	var createdRootInos []int64
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		if len(createdDiskIDs) > 0 {
			_, cleanErr := pool.Exec(cleanCtx, "DELETE FROM disks WHERE disk_id = ANY($1)", createdDiskIDs)
			require.NoError(t, cleanErr)
		}
		if len(createdRootInos) > 0 {
			_, cleanErr := pool.Exec(cleanCtx, "DELETE FROM inodes WHERE ino = ANY($1)", createdRootInos)
			require.NoError(t, cleanErr)
		}
	})

	for _, item := range disksToInsert {
		d, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     item.name,
			MaxBytes: 1024 * 1024,
		})
		require.NoError(t, err)
		createdDiskIDs = append(createdDiskIDs, item.id)
		createdRootInos = append(createdRootInos, int64(d.RootIno))

		_, err = pool.Exec(ctx, "UPDATE disks SET disk_id = $1, created_at = $2, updated_at = $2 WHERE disk_id = $3",
			item.id, item.createdAt, d.DiskID)
		require.NoError(t, err)
	}

	// 期望全局严格排序顺序：created_at DESC, disk_id DESC
	expectedOrder := []string{
		"0192a3b4-c5d6-7e8f-9012-000000000005", // newestTime
		"0192a3b4-c5d6-7e8f-9012-000000000004", // sameTime, high id
		"0192a3b4-c5d6-7e8f-9012-000000000003", // sameTime, low id
		"0192a3b4-c5d6-7e8f-9012-000000000002", // baseTime + 5s
		"0192a3b4-c5d6-7e8f-9012-000000000001", // baseTime
	}

	// 3. 按 PageSize = 2 逐页遍历（同时验证在相同 created_at 的两条记录之间截断翻页不丢不重）
	var collectedIDs []string
	pageToken := ""
	pageCount := 0
	for {
		pageCount++
		require.LessOrEqual(t, pageCount, 5, "翻页次数超出预期，疑似死循环")
		res, err := mgr.ListDisks(ctx, meta.ListDisksParams{
			PageSize:  2,
			PageToken: pageToken,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		for _, d := range res.Disks {
			collectedIDs = append(collectedIDs, d.DiskID)
		}
		if res.NextPageToken == "" {
			break
		}
		pageToken = res.NextPageToken
	}

	assert.Equal(t, 3, pageCount, "5 条记录按每页 2 条应分为 3 页 (2 + 2 + 1)")
	assert.Equal(t, expectedOrder, collectedIDs, "游标逐页翻页结果必须按 created_at DESC, disk_id DESC 严格排列且无遗漏无重复")
}

// TestDeleteDisk_PreservesReplacementStream 验证同一会话在删盘探测期间完成流重连后，新流仍能响应探活，磁盘必须保留。
func TestDeleteDisk_PreservesReplacementStream(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	mgr := meta.New(db.NewStore(pool), meta.WithSessionProbeTimeout(200*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano() & 0x7fffffffffffffff)
	t.Cleanup(func() {
		mgr.WatchHub().Evict(sid)
		require.NoError(t, mgr.CloseSession(context.Background(), sid))
		_, err := pool.Exec(context.Background(), "DELETE FROM disks WHERE disk_id = $1", disk.DiskID)
		require.NoError(t, err)
		_, err = pool.Exec(context.Background(), "DELETE FROM inodes WHERE root_ino = $1", int64(disk.RootIno))
		require.NoError(t, err)
	})
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)

	oldPing := make(chan struct{})
	replacementReady := make(chan struct{})
	_, unregisterOld := mgr.WatchHub().Register(sid, func(seq uint64) error {
		close(oldPing)
		select {
		case <-replacementReady:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	defer unregisterOld()
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID) }()
	select {
	case <-oldPing:
	case <-ctx.Done():
		t.Fatal("未收到旧流探活")
	}
	var newStreamID uint64
	newStreamID, unregisterNew := mgr.WatchHub().Register(sid, func(seq uint64) error {
		mgr.WatchHub().HandlePong(sid, newStreamID, seq)
		return nil
	})
	defer unregisterNew()
	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond))
	close(replacementReady)
	select {
	case err := <-deleteDone:
		assert.ErrorIs(t, err, meta.ErrBusy, "替换旧流不能成为删除存活挂载磁盘的依据")
	case <-ctx.Done():
		t.Fatal("删盘未在上下文期限内完成")
	}
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	assert.NoError(t, err, "已确认新流存活时磁盘必须保留")
	var sessions int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	assert.Equal(t, 1, sessions, "新流所属会话必须保留")
}

// TestDeleteDisk_ReconnectAfterEviction 验证在旧流被驱逐、会话删除等待数据库行锁期间重连，新流的存活证据必须阻止删盘。
func TestDeleteDisk_ReconnectAfterEviction(t *testing.T) {
	pool, parent := setupMetaTestDB(t)
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	mgr := meta.New(db.NewStore(pool), meta.WithSessionProbeTimeout(50*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		mgr.WatchHub().Evict(sid)
		require.NoError(t, mgr.CloseSession(cleanCtx, sid))
		_, cleanErr := pool.Exec(cleanCtx, "DELETE FROM disks WHERE disk_id = $1", disk.DiskID)
		require.NoError(t, cleanErr)
		_, cleanErr = pool.Exec(cleanCtx, "DELETE FROM inodes WHERE root_ino = $1", int64(disk.RootIno))
		require.NoError(t, cleanErr)
	})
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	_, unregisterOld := mgr.WatchHub().Register(sid, func(uint64) error { return nil })
	defer unregisterOld()

	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sid))
	require.NoError(t, err)
	deleteDone := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID)
	}()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("删盘协程未响应取消")
		}
	}()
	require.Eventually(t, func() bool { return !mgr.WatchHub().HasActiveStream(sid) }, time.Second, time.Millisecond, "旧流应在探活超时后被驱逐")

	var streamID uint64
	streamID, unregisterNew := mgr.WatchHub().Register(sid, func(seq uint64) error {
		mgr.WatchHub().HandlePong(sid, streamID, seq)
		return nil
	})
	defer unregisterNew()
	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "放行数据库回收前，新流已完成 Ping-Pong")
	var sessions int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.Equal(t, 1, sessions, "重连时租约仍有效")
	require.NoError(t, blocker.Commit(ctx))

	select {
	case deleteErr := <-deleteDone:
		assert.ErrorIs(t, deleteErr, meta.ErrBusy, "已确认重连客户端存活，必须拒绝删盘")
	case <-ctx.Done():
		t.Fatal("删盘未在期限内结束")
	}
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	assert.NoError(t, err, "存活挂载的磁盘必须保留")
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	assert.Equal(t, 1, sessions, "新流的租约必须保留")
	assert.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "误删后新流仍能应答探活")
}

// TestWatchSession_ProbeEvictionDrainsBlockedHTTPSenders 验证客户端将 HTTP/2 接收窗口设为零导致流发送阻塞时，探活退出与流驱逐会释放发送协程。
func TestWatchSession_ProbeEvictionDrainsBlockedHTTPSenders(t *testing.T) {
	pool, testCtx := setupMetaTestDB(t)
	mgr := meta.New(db.NewStore(pool))
	disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		mgr.WatchHub().Evict(sid)
		require.NoError(t, mgr.CloseSession(cleanCtx, sid))
		_, cleanErr := pool.Exec(cleanCtx, "DELETE FROM disks WHERE disk_id = $1", disk.DiskID)
		require.NoError(t, cleanErr)
		_, cleanErr = pool.Exec(cleanCtx, "DELETE FROM inodes WHERE root_ino = $1", int64(disk.RootIno))
		require.NoError(t, cleanErr)
	})
	_, err = mgr.RenewSessionWithDisk(testCtx, sid, &disk.DiskID)
	require.NoError(t, err)
	hub := mgr.WatchHub()
	srv, err := server.New(server.Params{
		Config:      config.ServerConfig{ServerHost: "127.0.0.1", ServerReadMaxBytes: config.DefaultServerReadMaxBytes},
		MetaManager: mgr,
	})
	require.NoError(t, err)
	ts := httptest.NewUnstartedServer(srv.Handler())
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	ts.Config.Protocols = &protocols
	ts.Start()
	defer ts.Close()
	conn, err := net.DialTimeout("tcp", ts.Listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	framer := http2.NewFramer(conn, conn)
	_, err = conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)
	require.NoError(t, framer.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}))
	var headers bytes.Buffer
	encoder := hpack.NewEncoder(&headers)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: ts.Listener.Addr().String()},
		{Name: ":path", Value: storagev1connect.StorageServiceWatchSessionProcedure},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
	} {
		require.NoError(t, encoder.WriteField(field))
	}
	require.NoError(t, framer.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: headers.Bytes(), EndHeaders: true}))
	payload, err := proto.Marshal(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID})
	require.NoError(t, err)
	envelope := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(envelope[1:5], uint32(len(payload)))
	copy(envelope[5:], payload)
	require.NoError(t, framer.WriteData(1, false, envelope))
	readerDone := make(chan struct{})
	var writeMu sync.Mutex
	go func() {
		defer close(readerDone)
		for {
			frame, readErr := framer.ReadFrame()
			if readErr != nil {
				return
			}
			if settings, ok := frame.(*http2.SettingsFrame); ok && !settings.IsAck() {
				writeMu.Lock()
				ackErr := framer.WriteSettingsAck()
				writeMu.Unlock()
				if ackErr != nil {
					return
				}
			}
		}
	}()
	defer func() {
		require.NoError(t, conn.Close())
		select {
		case <-readerDone:
		case <-time.After(time.Second):
			t.Error("HTTP/2 读协程未退出")
		}
	}()
	require.Eventually(t, func() bool { return hub.HasActiveStream(sid) }, time.Second, time.Millisecond)

	countSenders := func() int {
		var stacks bytes.Buffer
		require.NoError(t, pprof.Lookup("goroutine").WriteTo(&stacks, 2))
		count := 0
		for _, stack := range strings.Split(stacks.String(), "\n\n") {
			if strings.Contains(stack, "meta.(*SessionWatchHub).ProbeStream") {
				count++
			}
		}
		return count
	}
	require.Zero(t, countSenders())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 8; i++ {
		require.False(t, hub.Probe(ctx, sid, 10*time.Millisecond))
	}
	hub.Evict(sid)
	require.False(t, hub.HasActiveStream(sid))
	assert.Eventually(t, func() bool { return countSenders() == 0 }, 200*time.Millisecond, 5*time.Millisecond,
		"流驱逐后仍遗留探活发送协程；当前数量=%d", countSenders())
	writeMu.Lock()
	windowErr := framer.WriteWindowUpdate(1, 1024)
	writeMu.Unlock()
	require.NoError(t, windowErr)
	require.Eventually(t, func() bool { return countSenders() == 0 }, time.Second, time.Millisecond)
}

// TestWatchSession_DisconnectCleanupPreservesReconnectedSession 验证旧 HTTP/2 流断连后会话清理等待行锁期间，新流重连并应答探活使旧流清理失效。
func TestWatchSession_DisconnectCleanupPreservesReconnectedSession(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "disconnect_preserve_reconnect")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	mgr := meta.New(db.NewStore(pool))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	tmp, err := mgr.TmpFile(ctx, meta.TmpFileParams{
		ParentIno: disk.RootIno, Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)
	srv, err := server.New(server.Params{
		Config: config.ServerConfig{ServerHost: "127.0.0.1", ServerReadMaxBytes: config.DefaultServerReadMaxBytes}, MetaManager: mgr,
	})
	require.NoError(t, err)
	oldHandlerDone := make(chan struct{})
	var requests atomic.Int32
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			defer close(oldHandlerDone)
		}
		srv.Handler().ServeHTTP(w, r)
	}))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer func() {
		ts.CloseClientConnections()
		ts.Close()
	}()
	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL, connect.WithGRPC())
	oldCtx, oldCancel := context.WithCancel(ctx)
	defer oldCancel()
	oldStream := client.WatchSession(oldCtx)
	require.NoError(t, oldStream.Send(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID}))
	ack, err := oldStream.Receive()
	require.NoError(t, err)
	require.Zero(t, ack.GetPingSeq())

	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sid))
	require.NoError(t, err)
	var blockerPID int32
	require.NoError(t, blocker.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID))
	require.NoError(t, oldStream.CloseRequest())
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE $1 = ANY(pg_blocking_pids(pid))
			AND query LIKE '%DeleteClientSessionByID%'
		)`, blockerPID).Scan(&waiting)
		require.NoError(t, err)
		return waiting
	}, time.Second, time.Millisecond, "旧流清理应等待会话行锁")
	require.False(t, mgr.WatchHub().HasActiveStream(sid))

	newCtx, newCancel := context.WithCancel(ctx)
	defer newCancel()
	newStream := client.WatchSession(newCtx)
	require.NoError(t, newStream.Send(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID}))
	ack, err = newStream.Receive()
	require.NoError(t, err)
	require.Zero(t, ack.GetPingSeq())
	probeNew := func() {
		probeDone := make(chan bool, 1)
		go func() { probeDone <- mgr.WatchHub().Probe(ctx, sid, 300*time.Millisecond) }()
		ping, recvErr := newStream.Receive()
		require.NoError(t, recvErr)
		require.Positive(t, ping.GetPingSeq())
		require.NoError(t, newStream.Send(&storagev1.WatchSessionRequest{SessionId: sid, PongSeq: ping.GetPingSeq()}))
		require.True(t, <-probeDone, "真实重连流必须完成 Ping-Pong")
	}
	probeNew()
	require.NoError(t, blocker.Commit(ctx))
	select {
	case <-oldHandlerDone:
	case <-ctx.Done():
		t.Fatal("旧流清理未结束")
	}
	var sessions, handles int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	assert.Equal(t, 1, sessions, "旧流退出不得注销已重连的存活会话")
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(tmp.Fh)).Scan(&handles))
	assert.Equal(t, 1, handles, "旧流退出不得销毁已重连会话的文件句柄")
	probeNew()
}

// TestDeleteDisk_ReconnectDuringOrphanQuotaReclaim 验证会话删除在孤儿回收退还配额等待磁盘行锁期间重连时，存活会话与磁盘均必须保留。
func TestDeleteDisk_ReconnectDuringOrphanQuotaReclaim(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "reconnect_orphan_quota")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	mgr := meta.New(db.NewStore(pool), meta.WithSessionProbeTimeout(300*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	tmp, err := mgr.TmpFile(ctx, meta.TmpFileParams{
		ParentIno: disk.RootIno, Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)

	oldPing := make(chan struct{}, 1)
	_, unregisterOld := mgr.WatchHub().Register(sid, func(uint64) error {
		oldPing <- struct{}{}
		return nil
	})
	defer unregisterOld()
	deleteDone := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID)
	}()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("删盘协程未响应取消")
		}
	}()
	select {
	case <-oldPing:
	case <-ctx.Done():
		t.Fatal("未收到旧流探活")
	}

	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "SELECT disk_id FROM disks WHERE disk_id = $1 FOR UPDATE", disk.DiskID)
	require.NoError(t, err)
	var blockerPID int32
	require.NoError(t, blocker.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID))
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE $1 = ANY(pg_blocking_pids(pid))
			AND query LIKE '%AddDiskQuotaByRootIno%'
		)`, blockerPID).Scan(&waiting)
		require.NoError(t, err)
		return waiting
	}, 2*time.Second, time.Millisecond, "会话回收应在流复核之后等待配额行锁")
	require.False(t, mgr.WatchHub().HasActiveStream(sid))

	var streamID uint64
	streamID, unregisterNew := mgr.WatchHub().Register(sid, func(seq uint64) error {
		mgr.WatchHub().HandlePong(sid, streamID, seq)
		return nil
	})
	defer unregisterNew()
	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "事务提交前，新流已完成 Ping-Pong")
	var sessions int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.Equal(t, 1, sessions, "重连时租约尚未提交删除")
	require.NoError(t, blocker.Commit(ctx))

	select {
	case deleteErr := <-deleteDone:
		assert.ErrorIs(t, deleteErr, meta.ErrBusy, "已确认重连客户端存活，必须拒绝删盘")
	case <-ctx.Done():
		t.Fatal("删盘未在期限内结束")
	}
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	assert.NoError(t, err, "存活挂载的磁盘必须保留")
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	assert.Equal(t, 1, sessions, "存活会话必须保留")
	var handles int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(tmp.Fh)).Scan(&handles))
	assert.Equal(t, 1, handles, "重连会话的已打开匿名文件句柄必须保留")
	assert.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "新流仍可响应探活")
}

type commitBarrierStore struct {
	db.Store
	beforeCommit func(ctx context.Context) error
}

func (s *commitBarrierStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		if err := fn(q); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			return s.beforeCommit(ctx)
		}
		return nil
	}, opts...)
}

// TestCloseSession_AutoCloseGateBlocksRegisterUntilCommit 验证会话自动回收事务在获取 AcquireAutoCloseGate 后、真实 Commit 前严格阻塞同会话新流登记。
func TestCloseSession_AutoCloseGateBlocksRegisterUntilCommit(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "auto_close_commit_gate")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()

	atCommitBoundary := make(chan struct{})
	releaseCommit := make(chan struct{})
	var (
		interceptMu sync.Mutex
		intercept   bool
		barrierOnce sync.Once
	)
	store := &commitBarrierStore{
		Store: db.NewStore(pool),
		beforeCommit: func(txCtx context.Context) error {
			interceptMu.Lock()
			enabled := intercept
			interceptMu.Unlock()
			if !enabled {
				return nil
			}
			barrierOnce.Do(func() {
				close(atCommitBoundary)
				select {
				case <-releaseCommit:
				case <-txCtx.Done():
				}
			})
			return nil
		},
	}

	mgr := meta.New(store, meta.WithSessionProbeTimeout(300*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	file, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno, Name: []byte("commit-boundary"), Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)

	oldStreamID, unregisterOld := mgr.WatchHub().Register(sid, func(uint64) error { return nil })
	unregisterOld()

	interceptMu.Lock()
	intercept = true
	interceptMu.Unlock()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- mgr.CloseSession(meta.WithDisconnectedStreamID(ctx, oldStreamID), sid)
	}()

	select {
	case <-atCommitBoundary:
	case <-ctx.Done():
		t.Fatal("断连自动回收未到达事务提交边界")
	}

	regEntered := make(chan struct{})
	regDone := make(chan struct{})
	var (
		newStreamID   uint64
		unregisterNew func() bool
	)
	go func() {
		close(regEntered)
		newStreamID, unregisterNew = mgr.WatchHub().Register(sid, func(seq uint64) error {
			mgr.WatchHub().HandlePong(sid, newStreamID, seq)
			return nil
		})
		close(regDone)
	}()
	<-regEntered

	var leakedBeforeCommit bool
	select {
	case <-regDone:
		leakedBeforeCommit = true
		require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "提前越过门控的新流已完成 Ping-Pong")
	case <-time.After(50 * time.Millisecond):
	}
	assert.False(t, leakedBeforeCommit, "自动回收事务未 Commit 前，同会话的新流 Register 必须被门控阻塞")

	close(releaseCommit)
	require.NoError(t, <-closeDone)

	select {
	case <-regDone:
	case <-ctx.Done():
		t.Fatal("回收事务提交后新流 Register 仍未完成")
	}
	defer unregisterNew()

	if !leakedBeforeCommit {
		_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
		require.NoError(t, err)
		_, err = mgr.Open(ctx, meta.OpenParams{Ino: file.Inode.Ino, Flags: meta.O_RDWR, SessionID: &sid})
		require.NoError(t, err)
	}

	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "新流必须可正常响应 Ping-Pong")
	var sessions, handles int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE session_id = $1", int64(sid)).Scan(&handles))
	assert.Equal(t, 1, sessions, "新流完成登记与探活后，所属会话租约不得被滞后的回收事务提交删空")
	assert.Equal(t, 1, handles, "新流完成登记与探活后，所属打开句柄不得被滞后的回收事务提交删空")
}

// TestDeleteDisk_ReconnectDuringFinalCascade 验证最终删盘事务等待级联会话行锁期间，已完成 Ping-Pong 的重连流必须撤销删盘资格。
func TestDeleteDisk_ReconnectDuringFinalCascade(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "delete_final_cascade")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	mgr := meta.New(db.NewStore(pool))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	file, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno, Name: []byte("保留文件"), Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)
	srv, err := server.New(server.Params{
		Config: config.ServerConfig{ServerHost: "127.0.0.1", ServerReadMaxBytes: config.DefaultServerReadMaxBytes}, MetaManager: mgr,
	})
	require.NoError(t, err)
	ts := httptest.NewUnstartedServer(srv.Handler())
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer func() { ts.CloseClientConnections(); ts.Close() }()
	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL, connect.WithGRPC())
	old := client.WatchSession(ctx)
	require.NoError(t, old.Send(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID}))
	_, err = old.Receive()
	require.NoError(t, err)

	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sid))
	require.NoError(t, err)
	require.NoError(t, old.CloseRequest())
	require.Eventually(t, func() bool { return !mgr.WatchHub().HasActiveStream(sid) }, time.Second, time.Millisecond)
	deleteDone := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID) }()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("删盘协程未退出")
		}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND cardinality(pg_blocking_pids(pid)) > 0
			AND query LIKE '%name: DeleteDisk :execrows%'
		)`).Scan(&waiting)
		require.NoError(t, err)
		return waiting
	}, time.Second, time.Millisecond, "删盘应已越过流检查并等待级联会话行锁")

	stream := client.WatchSession(ctx)
	require.NoError(t, stream.Send(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID}))
	ack, err := stream.Receive()
	require.NoError(t, err)
	require.Zero(t, ack.GetPingSeq())
	probeDone := make(chan bool, 1)
	go func() { probeDone <- mgr.WatchHub().Probe(ctx, sid, 300*time.Millisecond) }()
	ping, err := stream.Receive()
	require.NoError(t, err)
	require.Positive(t, ping.GetPingSeq())
	require.NoError(t, stream.Send(&storagev1.WatchSessionRequest{SessionId: sid, PongSeq: ping.GetPingSeq()}))
	require.True(t, <-probeDone, "放行删除前，新流已完成真实 Ping-Pong")
	require.NoError(t, blocker.Commit(ctx))

	select {
	case deleteErr := <-deleteDone:
		assert.ErrorIs(t, deleteErr, meta.ErrBusy, "重连流已确认存活，必须撤销最终删盘事务")
	case <-ctx.Done():
		t.Fatal("删盘未在期限内结束")
	}
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	assert.NoError(t, err, "存活挂载的磁盘必须保留")
	var sessions, handles, inodes int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(file.Fh)).Scan(&handles))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM inodes WHERE ino = $1", int64(file.Inode.Ino)).Scan(&inodes))
	assert.Equal(t, 1, sessions, "重连会话租约必须保留")
	assert.Equal(t, 1, handles, "重连会话句柄必须保留")
	assert.Equal(t, 1, inodes, "磁盘内文件必须保留")
}

// TestDeleteDisk_CommitGateBlocksRegisterUntilCommit 验证 DeleteDisk 在获取 AcquireSessionsCommitGate 后、真实 Commit 前严格阻塞同会话新流登记。
func TestDeleteDisk_CommitGateBlocksRegisterUntilCommit(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "delete_commit_gate")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()

	atCommitBoundary := make(chan struct{})
	releaseCommit := make(chan struct{})
	var (
		interceptMu sync.Mutex
		intercept   bool
		barrierOnce sync.Once
	)
	store := &commitBarrierStore{
		Store: db.NewStore(pool),
		beforeCommit: func(txCtx context.Context) error {
			interceptMu.Lock()
			enabled := intercept
			interceptMu.Unlock()
			if !enabled {
				return nil
			}
			barrierOnce.Do(func() {
				close(atCommitBoundary)
				select {
				case <-releaseCommit:
				case <-txCtx.Done():
				}
			})
			return nil
		},
	}

	mgr := meta.New(store, meta.WithSessionProbeTimeout(300*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	file, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno, Name: []byte("delete-commit-boundary"), Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)

	interceptMu.Lock()
	intercept = true
	interceptMu.Unlock()

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID)
	}()

	select {
	case <-atCommitBoundary:
	case <-ctx.Done():
		t.Fatal("DeleteDisk 未到达事务提交边界")
	}

	regEntered := make(chan struct{})
	regDone := make(chan struct{})
	var (
		newStreamID   uint64
		unregisterNew func() bool
	)
	go func() {
		close(regEntered)
		newStreamID, unregisterNew = mgr.WatchHub().Register(sid, func(seq uint64) error {
			mgr.WatchHub().HandlePong(sid, newStreamID, seq)
			return nil
		})
		close(regDone)
	}()
	<-regEntered

	var leakedBeforeCommit bool
	select {
	case <-regDone:
		leakedBeforeCommit = true
		require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "提前越过批量门控的新流已完成 Ping-Pong")
	case <-time.After(50 * time.Millisecond):
	}
	assert.False(t, leakedBeforeCommit, "DeleteDisk 事务未 Commit 前，同会话的新流 Register 必须被批量提交门控阻塞")

	close(releaseCommit)
	require.NoError(t, <-deleteDone)

	select {
	case <-regDone:
	case <-ctx.Done():
		t.Fatal("DeleteDisk 事务提交后新流 Register 仍未完成")
	}
	defer unregisterNew()

	targetFh := file.Fh
	targetIno := file.Inode.Ino
	if !leakedBeforeCommit {
		newDisk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name() + "_recreated", MaxBytes: 1024})
		require.NoError(t, err)
		_, err = mgr.RenewSessionWithDisk(ctx, sid, &newDisk.DiskID)
		require.NoError(t, err)
		newFile, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: newDisk.RootIno, Name: []byte("recreated-after-commit"), Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
		})
		require.NoError(t, err)
		targetFh = newFile.Fh
		targetIno = newFile.Inode.Ino
	}

	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "新流必须可正常响应 Ping-Pong")
	var sessions, handles, inodes int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(targetFh)).Scan(&handles))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM inodes WHERE ino = $1", int64(targetIno)).Scan(&inodes))
	assert.Equal(t, 1, sessions, "已完成登记与探活的会话租约不得被滞后的 DeleteDisk 提交删空")
	assert.Equal(t, 1, handles, "已完成登记与探活的打开句柄不得被滞后的 DeleteDisk 提交删空")
	assert.Equal(t, 1, inodes, "已完成登记与探活的磁盘内 Inode 不得被滞后的 DeleteDisk 提交删空")
}
