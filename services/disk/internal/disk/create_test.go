package disk_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/disk/internal/db"
	dbmock "github.com/starsrc/bowl/services/disk/internal/db/mock"
	"github.com/starsrc/bowl/services/disk/internal/disk"
)

// TestCreateDisk_Success 验证正常创建磁盘的完整流程，包括 UUIDv7 生成、初始配额与时间戳装配。
func TestCreateDisk_Success(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		params   disk.CreateDiskParams
		rowUsed  int64
		setupRow func(arg db.CreateDiskParams, now time.Time) db.Disk
	}{
		{
			name: "正常创建标准磁盘",
			params: disk.CreateDiskParams{
				Name:     "data-disk-01",
				RootIno:  2001,
				MaxBytes: 10737418240, // 10 GiB
			},
			rowUsed: 0,
		},
		{
			name: "创建名称刚好达到 255 字节上限的磁盘",
			params: disk.CreateDiskParams{
				Name:     strings.Repeat("a", disk.MaxDiskNameBytes),
				RootIno:  math.MaxInt64,
				MaxBytes: math.MaxInt64,
			},
			rowUsed: 0,
		},
		{
			name: "创建名称为空字符串的磁盘",
			params: disk.CreateDiskParams{
				Name:     "",
				RootIno:  1,
				MaxBytes: 1,
			},
			rowUsed: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := disk.New(mockStore)

			now := time.Now().Truncate(time.Microsecond)
			var capturedID pgtype.UUID

			mockStore.EXPECT().
				CreateDisk(ctx, mock.MatchedBy(func(arg db.CreateDiskParams) bool {
					if arg.Name != tc.params.Name || arg.RootIno != int64(tc.params.RootIno) || arg.MaxBytes != int64(tc.params.MaxBytes) {
						return false
					}
					if !arg.DiskID.Valid {
						return false
					}
					u, err := uuid.FromBytes(arg.DiskID.Bytes[:])
					if err != nil || u.Version() != 7 || u.Variant() != uuid.RFC4122 {
						return false
					}
					capturedID = arg.DiskID
					return true
				})).
				RunAndReturn(func(ctx context.Context, arg db.CreateDiskParams) (db.Disk, error) {
					return db.Disk{
						DiskID:    arg.DiskID,
						Name:      arg.Name,
						RootIno:   arg.RootIno,
						MaxBytes:  arg.MaxBytes,
						UsedBytes: tc.rowUsed,
						CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
						UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
					}, nil
				}).
				Once()

			d, err := mgr.CreateDisk(ctx, tc.params)
			require.NoError(t, err)
			require.NotNil(t, d)

			assert.Equal(t, capturedID.String(), d.DiskID)
			assert.Equal(t, tc.params.Name, d.Name)
			assert.Equal(t, tc.params.RootIno, d.RootIno)
			assert.Equal(t, tc.params.MaxBytes, d.MaxBytes)
			assert.Equal(t, uint64(tc.rowUsed), d.UsedBytes)
			assert.Equal(t, now, d.CreatedAt)
			assert.Equal(t, now, d.UpdatedAt)
		})
	}
}

// TestCreateDisk_StaticParameterValidation 验证所有静态入参校验分支均不触达数据库。
func TestCreateDisk_StaticParameterValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name        string
		params      disk.CreateDiskParams
		expectedErr error
		errDetail   string
	}{
		{
			name: "RootIno 为 0 拒绝",
			params: disk.CreateDiskParams{
				Name:     "valid-disk",
				RootIno:  0,
				MaxBytes: 1024,
			},
			expectedErr: disk.ErrInvalidArgument,
			errDetail:   "root_ino",
		},
		{
			name: "RootIno 超过 MaxInt64 拒绝",
			params: disk.CreateDiskParams{
				Name:     "valid-disk",
				RootIno:  uint64(math.MaxInt64) + 1,
				MaxBytes: 1024,
			},
			expectedErr: disk.ErrInvalidArgument,
			errDetail:   "root_ino",
		},
		{
			name: "MaxBytes 为 0 拒绝",
			params: disk.CreateDiskParams{
				Name:     "valid-disk",
				RootIno:  1001,
				MaxBytes: 0,
			},
			expectedErr: disk.ErrInvalidArgument,
			errDetail:   "max_bytes",
		},
		{
			name: "MaxBytes 超过 MaxInt64 拒绝",
			params: disk.CreateDiskParams{
				Name:     "valid-disk",
				RootIno:  1001,
				MaxBytes: uint64(math.MaxInt64) + 1,
			},
			expectedErr: disk.ErrInvalidArgument,
			errDetail:   "max_bytes",
		},
		{
			name: "Name 长度超过 255 字节拒绝",
			params: disk.CreateDiskParams{
				Name:     strings.Repeat("x", disk.MaxDiskNameBytes+1),
				RootIno:  1001,
				MaxBytes: 1024,
			},
			expectedErr: disk.ErrNameTooLong,
			errDetail:   "exceeds max 255",
		},
		{
			name: "Name 包含空字符 (\\x00) 拒绝",
			params: disk.CreateDiskParams{
				Name:     "disk\x00bad",
				RootIno:  1001,
				MaxBytes: 1024,
			},
			expectedErr: disk.ErrInvalidArgument,
			errDetail:   "null byte",
		},
		{
			name: "Name 长度超过 255 且包含 \\x00 时优先返回 ErrNameTooLong",
			params: disk.CreateDiskParams{
				Name:     strings.Repeat("a", 256) + "\x00",
				RootIno:  1001,
				MaxBytes: 1024,
			},
			expectedErr: disk.ErrNameTooLong,
			errDetail:   "exceeds max 255",
		},
		{
			name: "RootIno 无效且 Name 超长时优先返回 ErrInvalidArgument (数值字段优先)",
			params: disk.CreateDiskParams{
				Name:     strings.Repeat("a", 300),
				RootIno:  0,
				MaxBytes: 1024,
			},
			expectedErr: disk.ErrInvalidArgument,
			errDetail:   "root_ino",
		},
		{
			name: "MaxBytes 无效且 Name 超长时优先返回 ErrInvalidArgument (数值字段优先)",
			params: disk.CreateDiskParams{
				Name:     strings.Repeat("a", 300),
				RootIno:  1001,
				MaxBytes: 0,
			},
			expectedErr: disk.ErrInvalidArgument,
			errDetail:   "max_bytes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			// 断言未调用 mockStore 的任何方法（包含 CreateDisk）
			mgr := disk.New(mockStore)

			d, err := mgr.CreateDisk(ctx, tc.params)
			require.Error(t, err)
			assert.Nil(t, d)
			assert.ErrorIs(t, err, tc.expectedErr)
			if tc.errDetail != "" {
				assert.Contains(t, err.Error(), tc.errDetail)
			}
		})
	}
}

// TestCreateDisk_DuplicateRootIno 验证当底层数据库抛出唯一约束冲突 (SQLSTATE 23505) 时映射为 ErrAlreadyExists。
func TestCreateDisk_DuplicateRootIno(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := disk.New(mockStore)

	params := disk.CreateDiskParams{
		Name:     "conflict-disk",
		RootIno:  3001,
		MaxBytes: 10737418240,
	}

	mockStore.EXPECT().
		CreateDisk(ctx, mock.MatchedBy(func(arg db.CreateDiskParams) bool {
			return arg.RootIno == int64(params.RootIno)
		})).
		Return(db.Disk{}, &pgconn.PgError{
			Code:    "23505",
			Message: `duplicate key value violates unique constraint "disks_root_ino_key"`,
		}).
		Once()

	d, err := mgr.CreateDisk(ctx, params)
	require.Error(t, err)
	assert.Nil(t, d)
	assert.ErrorIs(t, err, disk.ErrAlreadyExists)
	assert.Contains(t, err.Error(), "3001 already exists")
}

// TestCreateDisk_DatabaseErrorPropagation 验证底层非 23505 数据库错误透明透传。
func TestCreateDisk_DatabaseErrorPropagation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name      string
		dbErr     error
		isAlready bool
	}{
		{
			name:      "通用数据库连接断开错误",
			dbErr:     errors.New("connection closed by remote peer"),
			isAlready: false,
		},
		{
			name: "PostgreSQL 非 23505 错误码（如 23514 检查约束冲突）",
			dbErr: &pgconn.PgError{
				Code:    "23514",
				Message: `check constraint "chk_disks_capacity" violated`,
			},
			isAlready: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := disk.New(mockStore)

			params := disk.CreateDiskParams{
				Name:     "error-disk",
				RootIno:  4001,
				MaxBytes: 5368709120,
			}

			mockStore.EXPECT().
				CreateDisk(ctx, mock.MatchedBy(func(arg db.CreateDiskParams) bool {
					return arg.RootIno == int64(params.RootIno)
				})).
				Return(db.Disk{}, tc.dbErr).
				Once()

			d, err := mgr.CreateDisk(ctx, params)
			require.Error(t, err)
			assert.Nil(t, d)
			assert.ErrorIs(t, err, tc.dbErr)
			assert.False(t, errors.Is(err, disk.ErrAlreadyExists))
			assert.Contains(t, err.Error(), "create disk:")
		})
	}
}
