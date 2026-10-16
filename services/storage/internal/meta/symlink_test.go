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

func TestSymlink_PositivePath(t *testing.T) {
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

	t.Run("成功创建符号链接: 模式固定为 0o120777，大小等于目标路径长度，父目录硬链接数保持不变", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		target := "../shared/lib.so"
		expectedMode := int32(meta.S_IFLNK | 0o777)
		createdInodeRow := db.Inode{
			Ino:           int64(newIno),
			Generation:    0,
			Mode:          expectedMode,
			Size:          int64(len(target)),
			Nlink:         1,
			Uid:           1001,
			Gid:           1002,
			Rdev:          0,
			Atime:         pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:         pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:         pgtype.Timestamptz{Time: now, Valid: true},
			SymlinkTarget: pgtype.Text{String: target, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateSymlinkInode(mock.Anything, db.CreateSymlinkInodeParams{
				Mode:          expectedMode,
				Size:          int64(len(target)),
				Nlink:         1,
				Uid:           1001,
				Gid:           1002,
				Rdev:          0,
				SymlinkTarget: pgtype.Text{String: target, Valid: true},
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "lib_symlink",
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      "lib_symlink",
			Target:    target,
			UID:       1001,
			GID:       1002,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, newIno, inode.Ino)
		assert.Equal(t, uint32(expectedMode), inode.Attr.Mode)
		assert.Equal(t, uint32(1), inode.Attr.Nlink)
		assert.Equal(t, uint64(len(target)), inode.Attr.Size)
		assert.Equal(t, uint32(1001), inode.Attr.UID)
		assert.Equal(t, uint32(1002), inode.Attr.GID)
		assert.Equal(t, uint32(0), inode.Attr.Rdev)
	})

	t.Run("父目录包含 S_ISGID (0o2000): 符号链接继承父目录 GID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		parentWithSGID := baseParentRow
		parentWithSGID.Mode = int32(meta.S_IFDIR | meta.S_ISGID | 0o755)
		parentWithSGID.Gid = 2000

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentWithSGID, nil).
			Once()

		target := "/etc/hosts"
		expectedMode := int32(meta.S_IFLNK | 0o777)
		createdInodeRow := db.Inode{
			Ino:           int64(newIno),
			Generation:    0,
			Mode:          expectedMode,
			Size:          int64(len(target)),
			Nlink:         1,
			Uid:           1001,
			Gid:           2000,
			Rdev:          0,
			Atime:         pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:         pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:         pgtype.Timestamptz{Time: now, Valid: true},
			SymlinkTarget: pgtype.Text{String: target, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateSymlinkInode(mock.Anything, db.CreateSymlinkInodeParams{
				Mode:          expectedMode,
				Size:          int64(len(target)),
				Nlink:         1,
				Uid:           1001,
				Gid:           2000,
				Rdev:          0,
				SymlinkTarget: pgtype.Text{String: target, Valid: true},
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      "hosts_link",
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      "hosts_link",
			Target:    target,
			UID:       1001,
			GID:       3000, // 应当被父目录 GID 覆盖
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(2000), inode.Attr.GID)
		assert.Equal(t, uint32(expectedMode), inode.Attr.Mode)
	})
}

func TestSymlink_InputValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name   string
		params meta.SymlinkParams
	}{
		{
			name: "parent_ino 为 0",
			params: meta.SymlinkParams{
				ParentIno: 0,
				Name:      "link",
				Target:    "target",
			},
		},
		{
			name: "parent_ino 超过 math.MaxInt64",
			params: meta.SymlinkParams{
				ParentIno: uint64(math.MaxInt64) + 1,
				Name:      "link",
				Target:    "target",
			},
		},
		{
			name: "name 为空字符串",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      "",
				Target:    "target",
			},
		},
		{
			name: "name 长度超过 255 字节",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      strings.Repeat("a", 256),
				Target:    "target",
			},
		},
		{
			name: "name 包含斜杠 /",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      "dir/link",
				Target:    "target",
			},
		},
		{
			name: "name 包含空字符 \\x00",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      "link\x00name",
				Target:    "target",
			},
		},
		{
			name: "target 为空字符串",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      "link",
				Target:    "",
			},
		},
		{
			name: "target 长度超过 4096 字节",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      "link",
				Target:    strings.Repeat("a", 4097),
			},
		},
		{
			name: "target 包含空字符 \\x00",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      "link",
				Target:    "tar\x00get",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			inode, err := mgr.Symlink(ctx, tc.params)
			assert.Nil(t, inode)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

func TestSymlink_ParentInvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)

	baseParentRow := db.Inode{
		Ino:   int64(parentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Uid:   1000,
		Gid:   1000,
	}

	t.Run("父节点不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      "link",
			Target:    "target",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("父节点非目录 (S_IFREG) 时返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		parentFile := baseParentRow
		parentFile.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentFile, nil).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      "link",
			Target:    "target",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("父目录为已解链孤儿目录 (nlink == 0) 时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanParent := baseParentRow
		orphanParent.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(orphanParent, nil).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      "link",
			Target:    "target",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("创建名称为 . 时返回 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      ".",
			Target:    "target",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("创建名称为 .. 时返回 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      "..",
			Target:    "target",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("同名 Entry 已存在 (SQLSTATE 23505) 映射为 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateSymlinkInode(mock.Anything, mock.Anything).
			Return(db.Inode{Ino: 101, Mode: int32(meta.S_IFLNK | 0o777), Nlink: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, mock.Anything).
			Return(&pgconn.PgError{Code: "23505"}).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      "duplicate_link",
			Target:    "target",
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

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      "link",
			Target:    "target",
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestReadlink_PositivePath(t *testing.T) {
	ctx := context.Background()

	t.Run("成功读取正常符号链接目标路径", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(55)
		target := "/usr/bin/python3"
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: pgtype.Text{String: target, Valid: true},
			}, nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, target, result)
	})

	t.Run("成功读取孤儿符号链接节点目标路径 (兼容 nlink == 0)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(66)
		target := "../relative/target"
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: pgtype.Text{String: target, Valid: true},
			}, nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, target, result)
	})
}

func TestReadlink_NegativeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("ino 为 0 时直接返回 ErrInvalidArgument 且不查询数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		_, err := mgr.Readlink(ctx, 0)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("ino 超过 math.MaxInt64 时直接返回 ErrInvalidArgument 且不查询数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		_, err := mgr.Readlink(ctx, uint64(math.MaxInt64)+1)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("目标节点不存在 (pgx.ErrNoRows) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(999)
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{}, pgx.ErrNoRows).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标节点为普通文件 (S_IFREG) 返回 ErrInvalidArgument (POSIX EINVAL)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(10)
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFREG | 0o644),
				SymlinkTarget: pgtype.Text{Valid: false},
			}, nil).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("目标节点为目录 (S_IFDIR) 返回 ErrInvalidArgument (POSIX EINVAL)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(1)
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFDIR | 0o755),
				SymlinkTarget: pgtype.Text{Valid: false},
			}, nil).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("目标节点为符号链接但 symlink_target 字段无效返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(20)
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: pgtype.Text{Valid: false},
			}, nil).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("底层数据库查询错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(30)
		dbErr := errors.New("database connection refused")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{}, dbErr).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		assert.ErrorIs(t, err, dbErr)
	})
}
