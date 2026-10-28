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

func TestOpenDir_InputValidation(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		ino   uint64
		flags uint32
	}{
		{
			name:  "ino 为 0",
			ino:   0,
			flags: meta.O_RDONLY,
		},
		{
			name:  "ino 超过 math.MaxInt64",
			ino:   uint64(math.MaxInt64) + 1,
			flags: meta.O_RDONLY,
		},
		{
			name:  "flags 包含 O_WRONLY",
			ino:   100,
			flags: meta.O_WRONLY,
		},
		{
			name:  "flags 包含 O_RDWR",
			ino:   100,
			flags: meta.O_RDWR,
		},
		{
			name:  "flags 包含 O_TRUNC",
			ino:   100,
			flags: meta.O_RDONLY | meta.O_TRUNC,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
				Ino:   tc.ino,
				Flags: tc.flags,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
			assert.Nil(t, res)
		})
	}
}

func TestOpenDir_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseDirRow := db.Inode{
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

	t.Run("常规目录只读模式正常打开", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseDirRow, nil).
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

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(1), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	t.Run("孤儿目录 (nlink == 0) 存在活跃句柄时允许打开", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDirRow := baseDirRow
		orphanDirRow.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(true, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{
				Fh:        2,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDONLY),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(2), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})
}

func TestOpenDir_NegativeAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseDirRow := db.Inode{
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
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		assert.Nil(t, res)
	})

	t.Run("目标节点为普通文件返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := baseDirRow
		fileRow.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotDir))
		assert.Nil(t, res)
	})

	t.Run("死孤儿目录 (nlink == 0 且无活跃句柄) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		deadOrphanRow := baseDirRow
		deadOrphanRow.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(deadOrphanRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		assert.Nil(t, res)
	})

	t.Run("底层 GetInodeForUpdate 错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, errors.New("db error")).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.Nil(t, res)
		assert.Contains(t, err.Error(), "get inode for update")
	})

	t.Run("底层 HasFileHandlesByIno 错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDirRow := baseDirRow
		orphanDirRow.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, errors.New("db query handles error")).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.Nil(t, res)
		assert.Contains(t, err.Error(), "check has file handles")
	})

	t.Run("底层 CreateFileHandle 错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{}, errors.New("create handle error")).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.Nil(t, res)
		assert.Contains(t, err.Error(), "create file handle")
	})

	t.Run("启用 WithOpenDirCache(true) 时返回 FOPEN_CACHE_DIR", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithOpenDirCache(true))

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{Fh: 77, Ino: int64(ino), Flags: int32(meta.O_RDONLY)}, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(77), res.Fh)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, res.OpenFlags)
	})
}

func TestOpenDir_CreateFileHandlePgErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseDirRow := db.Inode{
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
				GetInodeForUpdate(mock.Anything, int64(ino)).
				Return(baseDirRow, nil).
				Once()

			mockQuerier.EXPECT().
				CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
					Ino:   int64(ino),
					Flags: int32(meta.O_RDONLY),
				}).
				Return(db.FileHandle{}, tc.createErr).
				Once()

			res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
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
