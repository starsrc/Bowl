package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

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

		target := []byte("../shared/lib.so")
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
			SymlinkTarget: target,
		}

		mockQuerier.EXPECT().
			CreateSymlinkInode(mock.Anything, db.CreateSymlinkInodeParams{
				Mode:          expectedMode,
				Size:          int64(len(target)),
				Nlink:         1,
				Uid:           1001,
				Gid:           1002,
				Rdev:          0,
				SymlinkTarget: target,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("lib_symlink"),
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
			Name:      []byte("lib_symlink"),
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

	t.Run("成功创建非 UTF-8 原始字节序列符号链接并无损存储", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		rawLinkName := []byte{0xfa, 0xfb, 0xfc}
		target := []byte{0xff, 0xfe, 0xfd, 0x80, 'l', 'i', 'b', 0x90, '.', 's', 'o'}
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
			SymlinkTarget: target,
		}

		mockQuerier.EXPECT().
			CreateSymlinkInode(mock.Anything, db.CreateSymlinkInodeParams{
				Mode:          expectedMode,
				Size:          int64(len(target)),
				Nlink:         1,
				Uid:           1001,
				Gid:           1002,
				Rdev:          0,
				SymlinkTarget: target,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      rawLinkName,
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
			Name:      rawLinkName,
			Target:    target,
			UID:       1001,
			GID:       1002,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, newIno, inode.Ino)
		assert.Equal(t, uint32(expectedMode), inode.Attr.Mode)
		assert.Equal(t, uint64(len(target)), inode.Attr.Size)
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

		target := []byte("/etc/hosts")
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
			SymlinkTarget: target,
		}

		mockQuerier.EXPECT().
			CreateSymlinkInode(mock.Anything, db.CreateSymlinkInodeParams{
				Mode:          expectedMode,
				Size:          int64(len(target)),
				Nlink:         1,
				Uid:           1001,
				Gid:           2000,
				Rdev:          0,
				SymlinkTarget: target,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("hosts_link"),
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
			Name:      []byte("hosts_link"),
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
		name        string
		params      meta.SymlinkParams
		expectedErr error
	}{
		{
			name: "parent_ino 为 0",
			params: meta.SymlinkParams{
				ParentIno: 0,
				Name:      []byte("link"),
				Target:    []byte("target"),
			},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name: "parent_ino 超过 math.MaxInt64",
			params: meta.SymlinkParams{
				ParentIno: uint64(math.MaxInt64) + 1,
				Name:      []byte("link"),
				Target:    []byte("target"),
			},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name: "name 为空切片",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte{},
				Target:    []byte("target"),
			},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name: "name 长度超过 255 字节",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      bytes.Repeat([]byte("a"), 256),
				Target:    []byte("target"),
			},
			expectedErr: meta.ErrNameTooLong,
		},
		{
			name: "name 包含斜杠 /",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("dir/link"),
				Target:    []byte("target"),
			},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name: "name 包含空字符 \\x00",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("link\x00name"),
				Target:    []byte("target"),
			},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name: "target 为 nil",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("link"),
				Target:    nil,
			},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name: "target 为空切片",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("link"),
				Target:    []byte{},
			},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name: "target 长度恰好 4096 字节超过 4095 字节上限返回 ErrNameTooLong",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("link"),
				Target:    bytes.Repeat([]byte("a"), 4096),
			},
			expectedErr: meta.ErrNameTooLong,
		},
		{
			name: "target 长度超过 4095 字节返回 ErrNameTooLong",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("link"),
				Target:    bytes.Repeat([]byte("a"), 4097),
			},
			expectedErr: meta.ErrNameTooLong,
		},
		{
			name: "target 包含空字符 \\x00 返回 ErrInvalidArgument",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("link"),
				Target:    []byte("tar\x00get"),
			},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name: "target 非 UTF-8 字节中包含空字符 \\x00 返回 ErrInvalidArgument",
			params: meta.SymlinkParams{
				ParentIno: 1,
				Name:      []byte("link"),
				Target:    []byte{0xff, 0x00, 0xfe},
			},
			expectedErr: meta.ErrInvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			inode, err := mgr.Symlink(ctx, tc.params)
			assert.Nil(t, inode)
			assert.ErrorIs(t, err, tc.expectedErr)
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
			Name:      []byte("link"),
			Target:    []byte("target"),
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
			Name:      []byte("link"),
			Target:    []byte("target"),
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
			Name:      []byte("link"),
			Target:    []byte("target"),
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
			Name:      []byte("."),
			Target:    []byte("target"),
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
			Name:      []byte(".."),
			Target:    []byte("target"),
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
			Name:      []byte("duplicate_link"),
			Target:    []byte("target"),
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
			Name:      []byte("link"),
			Target:    []byte("target"),
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
		target := []byte("/usr/bin/python3")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: target,
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
		target := []byte("../relative/target")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: target,
			}, nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, target, result)
	})

	t.Run("成功读取非 UTF-8 原始字节序列目标路径无损往返", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(77)
		target := []byte{0xfe, 0xed, 0xfa, 0xce, '/', 0x80, 0x90, 0xff}
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: target,
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
				SymlinkTarget: nil,
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
				SymlinkTarget: nil,
			}, nil).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("目标节点为符号链接但 symlink_target 字段无效返回 ErrIO 且 ToStatus 为 CodeInternal", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(20)
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: nil,
			}, nil).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrIO, "ADR-0007 规定损坏的空目标符号链接必须返回 ErrIO")
		assert.NotErrorIs(t, err, meta.ErrInvalidArgument, "空符号链接属于数据损坏(EIO)，不得匹配 ErrInvalidArgument (EINVAL)")
		st := meta.ToStatus(err)
		assert.Equal(t, codes.Internal, st.Code(), "ADR-0007 要求损坏的空 symlink_target 必须映射为 CodeInternal (EIO)")
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

func TestSymlink_DiskQuotaAndRootInoInheritance(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	rootIno := int64(200)
	newIno := uint64(201)

	parentRowWithRoot := db.Inode{
		Ino:        int64(parentIno),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      2,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		RootIno:    rootIno,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	target := []byte("../target/path")
	expectedMode := int32(meta.S_IFLNK | 0o777)

	t.Run("父目录包含 root_ino 时，CreateSymlinkInode 继承 root_ino 且同一事务内调用 AddDiskQuotaByRootIno 递增 used_inodes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRowWithRoot, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:           int64(newIno),
			Generation:    0,
			Mode:          expectedMode,
			Size:          int64(len(target)),
			Nlink:         1,
			Uid:           1001,
			Gid:           1002,
			Rdev:          0,
			SymlinkTarget: target,
			RootIno:       rootIno,
			Atime:         pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:         pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:         pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateSymlinkInode(mock.Anything, db.CreateSymlinkInodeParams{
				Mode:          expectedMode,
				Size:          int64(len(target)),
				Nlink:         1,
				Uid:           1001,
				Gid:           1002,
				Rdev:          0,
				SymlinkTarget: target,
				RootIno:       rootIno,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("symlink_with_quota"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}).
			Return(nil).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      []byte("symlink_with_quota"),
			Target:    target,
			UID:       1001,
			GID:       1002,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, newIno, inode.Ino)
		mockQuerier.AssertExpectations(t)
	})

	t.Run("AddDiskQuotaByRootIno 返回数据库错误时事务失败并向上传播错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRowWithRoot, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:           int64(newIno),
			Generation:    0,
			Mode:          expectedMode,
			Size:          int64(len(target)),
			Nlink:         1,
			Uid:           1001,
			Gid:           1002,
			Rdev:          0,
			SymlinkTarget: target,
			RootIno:       rootIno,
			Atime:         pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:         pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:         pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateSymlinkInode(mock.Anything, db.CreateSymlinkInodeParams{
				Mode:          expectedMode,
				Size:          int64(len(target)),
				Nlink:         1,
				Uid:           1001,
				Gid:           1002,
				Rdev:          0,
				SymlinkTarget: target,
				RootIno:       rootIno,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("symlink_fail"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		expectedErr := errors.New("db disk quota update connection refused")
		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}).
			Return(expectedErr).
			Once()

		inode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: parentIno,
			Name:      []byte("symlink_fail"),
			Target:    target,
			UID:       1001,
			GID:       1002,
		})

		assert.Nil(t, inode)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), fmt.Sprintf("increment disk used_inodes for root_ino %d", rootIno))
		mockQuerier.AssertExpectations(t)
	})
}

// TestSymlink_PostgresIntegration_ReadlinkCorruptedEmptyTargetReturnsEIO 使用独立临时数据库验证损坏的符号链接（空目标或 NULL）调用 Readlink 返回 ErrIO (EIO)。
func TestSymlink_PostgresIntegration_ReadlinkCorruptedEmptyTargetReturnsEIO(t *testing.T) {
	dsn := getStorageTestDSN()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Skipf("跳过集成测试: 解析 dsn 失败: %v", err)
		return
	}
	host := u.Host
	u.Path = "/postgres"
	adminDSN := u.String()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Skipf("跳过集成测试: 连接管理库 postgres 失败: %v", err)
		return
	}
	defer adminPool.Close()

	if err := adminPool.Ping(ctx); err != nil {
		t.Skipf("跳过集成测试: ping 管理库 postgres 失败: %v", err)
		return
	}

	tempDBName := fmt.Sprintf("bowl_symlink_corrupt_%d", time.Now().UnixNano())
	_, err = adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", tempDBName))
	require.NoError(t, err)

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", tempDBName))
	}()

	tempDSN := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", host, tempDBName)
	tempPool, err := pgxpool.New(ctx, tempDSN)
	require.NoError(t, err)
	defer tempPool.Close()

	// 在独立临时库中执行完整 schema 迁移
	migrationFiles, err := filepath.Glob("../../db/migrations/*.sql")
	require.NoError(t, err)
	sort.Strings(migrationFiles)
	for _, f := range migrationFiles {
		content, err := os.ReadFile(f)
		require.NoError(t, err)
		parts := strings.Split(string(content), "-- +goose Down")
		upSQL := parts[0]
		upSQL = strings.ReplaceAll(upSQL, "-- +goose Up", "")
		upSQL = strings.ReplaceAll(upSQL, "-- +goose StatementBegin", "")
		upSQL = strings.ReplaceAll(upSQL, "-- +goose StatementEnd", "")
		_, err = tempPool.Exec(ctx, strings.TrimSpace(upSQL))
		require.NoError(t, err, "执行迁移 %s 失败", filepath.Base(f))
	}

	// 临时 DROP CHECK 约束（仅在独立临时库上操作，严禁污染共享测试主库），以便构造损坏脏数据
	_, err = tempPool.Exec(ctx, "ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;")
	require.NoError(t, err)

	store := db.NewStore(tempPool)
	mgr := meta.New(store)

	// 1. 创建测试虚拟磁盘
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-readlink-eio-%d", time.Now().UnixNano()),
		MaxBytes: 10 * 1024 * 1024,
	})
	require.NoError(t, err)

	// 2. 构造损坏节点 1：symlink_target 为空字节串
	symEmpty, err := mgr.Symlink(ctx, meta.SymlinkParams{
		ParentIno: disk.RootIno,
		Name:      []byte("empty_target_link"),
		Target:    []byte("/valid/target/empty"),
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	tag, err := tempPool.Exec(ctx, "UPDATE inodes SET symlink_target = ''::bytea WHERE ino = $1;", int64(symEmpty.Ino))
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())

	// 3. 构造损坏节点 2：symlink_target 为 NULL
	symNull, err := mgr.Symlink(ctx, meta.SymlinkParams{
		ParentIno: disk.RootIno,
		Name:      []byte("null_target_link"),
		Target:    []byte("/valid/target/null"),
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	tag, err = tempPool.Exec(ctx, "UPDATE inodes SET symlink_target = NULL WHERE ino = $1;", int64(symNull.Ino))
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())

	// 4. 调用 mgr.Readlink，验证两者均返回 ErrIO，不匹配 ErrInvalidArgument，且 ToStatus 为 CodeInternal
	testCases := []struct {
		desc string
		ino  uint64
	}{
		{desc: "symlink_target 为空字节串", ino: symEmpty.Ino},
		{desc: "symlink_target 为 NULL", ino: symNull.Ino},
	}

	for _, tc := range testCases {
		target, err := mgr.Readlink(ctx, tc.ino)
		require.Nil(t, target, tc.desc)
		require.Error(t, err, tc.desc)
		assert.True(t, errors.Is(err, meta.ErrIO), "%s 损坏符号链接必须返回 ErrIO，实际为: %v", tc.desc, err)
		assert.False(t, errors.Is(err, meta.ErrInvalidArgument), "%s 损坏符号链接不得匹配 ErrInvalidArgument", tc.desc)
		st := meta.ToStatus(err)
		assert.Equal(t, codes.Internal, st.Code(), "%s 损坏符号链接必须映射为 CodeInternal (EIO)", tc.desc)
	}
}
