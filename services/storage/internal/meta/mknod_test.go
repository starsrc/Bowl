package meta_test

import (
	"context"
	"errors"
	"fmt"
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

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestMknod_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	newIno := uint64(100)

	baseParentRow := db.Inode{
		Ino:        int64(parentIno),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      2,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("创建普通文件并归一化非零 rdev 为 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       0,
			Nlink:      1,
			Uid:        1001,
			Gid:        1002,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
				Uid:   1001,
				Gid:   1002,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "test_file.txt",
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "test_file.txt",
			Mode:      meta.S_IFREG | 0o644,
			UID:       1001,
			GID:       1002,
			Rdev:      9999, // 非字符/块设备，必须被归一化为 0
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, newIno, inode.Ino)
		assert.Equal(t, uint64(0), inode.Generation)
		assert.Equal(t, meta.S_IFREG|0o644, inode.Attr.Mode)
		assert.Equal(t, uint64(0), inode.Attr.Size)
		assert.Equal(t, uint32(1), inode.Attr.Nlink)
		assert.Equal(t, uint32(1001), inode.Attr.UID)
		assert.Equal(t, uint32(1002), inode.Attr.GID)
		assert.Equal(t, uint32(0), inode.Attr.Rdev)
		assert.Equal(t, uint32(4096), inode.Attr.Blksize)
		assert.Equal(t, uint64(0), inode.Attr.Blocks)
		assert.Equal(t, now, inode.Attr.Atime)
		assert.Equal(t, now, inode.Attr.Mtime)
		assert.Equal(t, now, inode.Attr.Ctime)
	})

	t.Run("创建字符设备并如实保留 rdev", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       int32(meta.S_IFCHR | 0o660),
			Size:       0,
			Nlink:      1,
			Uid:        1001,
			Gid:        1002,
			Rdev:       int64(0x1234),
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFCHR | 0o660),
				Nlink: 1,
				Uid:   1001,
				Gid:   1002,
				Rdev:  int64(0x1234),
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "chardev",
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "chardev",
			Mode:      meta.S_IFCHR | 0o660,
			UID:       1001,
			GID:       1002,
			Rdev:      0x1234,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(0x1234), inode.Attr.Rdev)
	})

	t.Run("创建块设备并如实保留 rdev", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       int32(meta.S_IFBLK | 0o660),
			Size:       0,
			Nlink:      1,
			Uid:        1001,
			Gid:        1002,
			Rdev:       int64(0x5678),
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFBLK | 0o660),
				Nlink: 1,
				Uid:   1001,
				Gid:   1002,
				Rdev:  int64(0x5678),
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "blkdev",
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "blkdev",
			Mode:      meta.S_IFBLK | 0o660,
			UID:       1001,
			GID:       1002,
			Rdev:      0x5678,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(0x5678), inode.Attr.Rdev)
	})

	t.Run("创建套接字并归一化非零 rdev 为 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       int32(meta.S_IFSOCK | 0o777),
			Size:       0,
			Nlink:      1,
			Uid:        1001,
			Gid:        1002,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFSOCK | 0o777),
				Nlink: 1,
				Uid:   1001,
				Gid:   1002,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "test_sock",
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "test_sock",
			Mode:      meta.S_IFSOCK | 0o777,
			UID:       1001,
			GID:       1002,
			Rdev:      8888, // 必须归一化为 0
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, meta.S_IFSOCK|0o777, inode.Attr.Mode)
		assert.Equal(t, uint32(0), inode.Attr.Rdev)
	})

	t.Run("父目录具有 S_ISGID 标志时子节点强制继承父目录 GID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		parentWithSGID := baseParentRow
		parentWithSGID.Mode = int32(meta.S_IFDIR | meta.S_ISGID | 0o755)
		parentWithSGID.Gid = 8888

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentWithSGID, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       int32(meta.S_IFIFO | 0o644),
			Size:       0,
			Nlink:      1,
			Uid:        1001,
			Gid:        8888, // 继承自父目录
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFIFO | 0o644),
				Nlink: 1,
				Uid:   1001,
				Gid:   8888, // 继承
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "fifo_pipe",
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "fifo_pipe",
			Mode:      meta.S_IFIFO | 0o644,
			UID:       1001,
			GID:       2002, // 传入的不同 GID，应被父目录 GID 覆盖
			Rdev:      1234, // 归一化为 0
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(8888), inode.Attr.GID)
		assert.Equal(t, uint32(0), inode.Attr.Rdev)
	})
}

func TestMknod_InputValidation(t *testing.T) {
	ctx := context.Background()
	mgr := meta.New(nil)

	validParams := meta.MknodParams{
		ParentIno: 1,
		Name:      "valid_node",
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Rdev:      0,
	}

	tests := []struct {
		name   string
		mutate func(p *meta.MknodParams)
	}{
		{
			name: "ParentIno 为 0",
			mutate: func(p *meta.MknodParams) {
				p.ParentIno = 0
			},
		},
		{
			name: "ParentIno 超过 MaxInt64",
			mutate: func(p *meta.MknodParams) {
				p.ParentIno = uint64(math.MaxInt64) + 1
			},
		},
		{
			name: "Name 为空",
			mutate: func(p *meta.MknodParams) {
				p.Name = ""
			},
		},
		{
			name: "Name 长度超过 255 字节",
			mutate: func(p *meta.MknodParams) {
				p.Name = strings.Repeat("a", 256)
			},
		},
		{
			name: "Name 包含斜杠 /",
			mutate: func(p *meta.MknodParams) {
				p.Name = "foo/bar"
			},
		},
		{
			name: "Name 包含空字符 NUL",
			mutate: func(p *meta.MknodParams) {
				p.Name = "foo\x00bar"
			},
		},
		{
			name: "Mode 超过 0o177777",
			mutate: func(p *meta.MknodParams) {
				p.Mode = 0o200000
			},
		},
		{
			name: "Mode 文件类型为 0 (未指定文件类型)",
			mutate: func(p *meta.MknodParams) {
				p.Mode = 0o644
			},
		},
		{
			name: "Mode 文件类型为 S_IFDIR (禁止 mknod 目录)",
			mutate: func(p *meta.MknodParams) {
				p.Mode = meta.S_IFDIR | 0o755
			},
		},
		{
			name: "Mode 文件类型为 S_IFLNK (禁止 mknod 符号链接)",
			mutate: func(p *meta.MknodParams) {
				p.Mode = meta.S_IFLNK | 0o777
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := validParams
			tc.mutate(&p)
			inode, err := mgr.Mknod(ctx, p)
			assert.Nil(t, inode)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

func TestMknod_NegativeCases(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)

	baseParentRow := db.Inode{
		Ino:   int64(parentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
	}

	t.Run("父目录不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "child",
			Mode:      meta.S_IFREG | 0o644,
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("父目录 Nlink 为 0 (孤儿目录) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		deadParent := baseParentRow
		deadParent.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(deadParent, nil).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "child",
			Mode:      meta.S_IFREG | 0o644,
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("父目录不是目录返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		regParent := baseParentRow
		regParent.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(regParent, nil).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "child",
			Mode:      meta.S_IFREG | 0o644,
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("保留名 . 或 .. 直接返回 ErrAlreadyExists 且不调用 CreateInode", func(t *testing.T) {
		for _, reservedName := range []string{".", ".."} {
			t.Run(reservedName, func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mockQuerier := dbmock.NewMockQuerier(t)
				mgr := meta.New(mockStore)

				setupMockWriteTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().
					GetInodeForUpdate(mock.Anything, int64(parentIno)).
					Return(baseParentRow, nil).
					Once()

				inode, err := mgr.Mknod(ctx, meta.MknodParams{
					ParentIno: parentIno,
					Name:      reservedName,
					Mode:      meta.S_IFREG | 0o644,
				})
				assert.Nil(t, inode)
				assert.ErrorIs(t, err, meta.ErrAlreadyExists)
			})
		}
	})

	t.Run("条目已存在唯一键冲突 (23505) 返回 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, mock.Anything).
			Return(db.Inode{Ino: 101}, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, mock.Anything).
			Return(&pgconn.PgError{Code: "23505"}).
			Once()

		inode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: parentIno,
			Name:      "conflict_file",
			Mode:      meta.S_IFREG | 0o644,
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("底层数据库错误被正确包裹", func(t *testing.T) {
		t.Run("GetInodeForUpdate 报错", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			setupMockWriteTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(parentIno)).
				Return(db.Inode{}, errors.New("db connection down")).
				Once()

			inode, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: parentIno,
				Name:      "test",
				Mode:      meta.S_IFREG | 0o644,
			})
			assert.Nil(t, inode)
			assert.ErrorContains(t, err, "db connection down")
		})

		t.Run("CreateInode 报错", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			setupMockWriteTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(parentIno)).
				Return(baseParentRow, nil).
				Once()

			mockQuerier.EXPECT().
				CreateInode(mock.Anything, mock.Anything).
				Return(db.Inode{}, errors.New("failed to insert inode")).
				Once()

			inode, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: parentIno,
				Name:      "test",
				Mode:      meta.S_IFREG | 0o644,
			})
			assert.Nil(t, inode)
			assert.ErrorContains(t, err, "failed to insert inode")
		})

		t.Run("TouchInodeTimestamps 报错", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			setupMockWriteTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(parentIno)).
				Return(baseParentRow, nil).
				Once()

			mockQuerier.EXPECT().
				CreateInode(mock.Anything, mock.Anything).
				Return(db.Inode{Ino: 101}, nil).
				Once()

			mockQuerier.EXPECT().
				CreateEntry(mock.Anything, mock.Anything).
				Return(nil).
				Once()

			mockQuerier.EXPECT().
				TouchInodeTimestamps(mock.Anything, int64(parentIno)).
				Return(errors.New("touch timestamp error")).
				Once()

			inode, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: parentIno,
				Name:      "test",
				Mode:      meta.S_IFREG | 0o644,
			})
			assert.Nil(t, inode)
			assert.ErrorContains(t, err, "touch timestamp error")
		})
	})
}

func TestMknod_TransactionOptionsContract(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)

	// 验证 Mknod 必须发起读写事务（未指定 ReadOnly）
	mockStore.EXPECT().
		ExecTxWithOptions(
			mock.Anything,
			mock.Anything,
		).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			cfg := tx.ApplyOptions(opts...)
			if cfg.AccessMode == tx.ReadOnly {
				return fmt.Errorf("expected write transaction, got read-only")
			}
			return fn(mockQuerier)
		}).
		Once()

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(1)).
		Return(db.Inode{}, pgx.ErrNoRows).
		Once()

	_, err := mgr.Mknod(ctx, meta.MknodParams{
		ParentIno: 1,
		Name:      "test",
		Mode:      meta.S_IFREG | 0o644,
	})
	assert.ErrorIs(t, err, meta.ErrNotFound)
}
