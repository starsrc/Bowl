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

func TestLink_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	srcIno := uint64(100)

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

	baseSrcRow := db.Inode{
		Ino:        int64(srcIno),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       2048,
		Nlink:      1,
		Uid:        1001,
		Gid:        1002,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Minute), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Minute), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Minute), Valid: true},
	}

	t.Run("成功为普通文件创建硬链接: 源节点 nlink 递增 1 且仅刷新 ctime，父目录 nlink 保持不变", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		// 两级加锁顺序：先锁父目录，再锁源节点
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(srcIno)).
			Return(baseSrcRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "hardlink.txt",
				Ino:       int64(srcIno),
			}).
			Return(nil).
			Once()

		updatedSrcRow := baseSrcRow
		updatedSrcRow.Nlink = 2
		updatedSrcRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			IncrementInodeNlinkAndTouchCtime(mock.Anything, int64(srcIno)).
			Return(updatedSrcRow, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "hardlink.txt",
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, srcIno, inode.Ino)
		assert.Equal(t, uint32(2), inode.Attr.Nlink)
		assert.Equal(t, uint64(2048), inode.Attr.Size)
		assert.Equal(t, uint32(baseSrcRow.Mode), inode.Attr.Mode)
	})

	t.Run("成功为符号链接创建硬链接", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		symlinkRow := baseSrcRow
		symlinkRow.Mode = int32(meta.S_IFLNK | 0o777)
		symlinkRow.SymlinkTarget = pgtype.Text{String: "/tmp/foo", Valid: true}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(srcIno)).
			Return(symlinkRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "symlink_alias",
				Ino:       int64(srcIno),
			}).
			Return(nil).
			Once()

		updatedSymlinkRow := symlinkRow
		updatedSymlinkRow.Nlink = 2
		updatedSymlinkRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			IncrementInodeNlinkAndTouchCtime(mock.Anything, int64(srcIno)).
			Return(updatedSymlinkRow, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "symlink_alias",
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(2), inode.Attr.Nlink)
		assert.Equal(t, uint32(meta.S_IFLNK|0o777), inode.Attr.Mode)
	})
}

func TestLink_InputValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name   string
		params meta.LinkParams
	}{
		{
			name: "ino 为 0",
			params: meta.LinkParams{
				Ino:          0,
				NewParentIno: 1,
				NewName:      "link",
			},
		},
		{
			name: "ino 超过 math.MaxInt64",
			params: meta.LinkParams{
				Ino:          uint64(math.MaxInt64) + 1,
				NewParentIno: 1,
				NewName:      "link",
			},
		},
		{
			name: "new_parent_ino 为 0",
			params: meta.LinkParams{
				Ino:          100,
				NewParentIno: 0,
				NewName:      "link",
			},
		},
		{
			name: "new_parent_ino 超过 math.MaxInt64",
			params: meta.LinkParams{
				Ino:          100,
				NewParentIno: uint64(math.MaxInt64) + 1,
				NewName:      "link",
			},
		},
		{
			name: "new_name 为空字符串",
			params: meta.LinkParams{
				Ino:          100,
				NewParentIno: 1,
				NewName:      "",
			},
		},
		{
			name: "new_name 长度超过 255 字节",
			params: meta.LinkParams{
				Ino:          100,
				NewParentIno: 1,
				NewName:      strings.Repeat("a", 256),
			},
		},
		{
			name: "new_name 包含斜杠 /",
			params: meta.LinkParams{
				Ino:          100,
				NewParentIno: 1,
				NewName:      "sub/link",
			},
		},
		{
			name: "new_name 包含空字符 \\x00",
			params: meta.LinkParams{
				Ino:          100,
				NewParentIno: 1,
				NewName:      "link\x00name",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			inode, err := mgr.Link(ctx, tc.params)
			assert.Nil(t, inode)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

func TestLink_LockOrderAndInvariants(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)
	srcIno := uint64(100)

	baseParentRow := db.Inode{
		Ino:   int64(parentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
	}

	baseSrcRow := db.Inode{
		Ino:   int64(srcIno),
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
	}

	t.Run("目标父目录不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "link",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标父节点非目录 (S_IFREG) 时返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{Ino: int64(parentIno), Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "link",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("目标父目录为孤儿目录 (nlink == 0) 时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{Ino: int64(parentIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 0}, nil).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "link",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("源节点不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(srcIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "link",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("禁止目录硬链接: 源节点为目录 (S_IFDIR) 时返回 ErrPerm", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(srcIno)).
			Return(db.Inode{Ino: int64(srcIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "dir_link",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrPerm)
	})

	t.Run("禁止复活孤儿节点: 源节点 nlink == 0 时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(srcIno)).
			Return(db.Inode{Ino: int64(srcIno), Mode: int32(meta.S_IFREG | 0o644), Nlink: 0}, nil).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "resurrect_link",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("new_name 为 . 时返回 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      ".",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("new_name 为 .. 时返回 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "..",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("同名 Entry 已存在 (SQLSTATE 23505) 返回 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(srcIno)).
			Return(baseSrcRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, mock.Anything).
			Return(&pgconn.PgError{Code: "23505"}).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "existing_link",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("connection failed")
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, dbErr).
			Once()

		inode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          srcIno,
			NewParentIno: parentIno,
			NewName:      "link",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, dbErr)
	})
}
