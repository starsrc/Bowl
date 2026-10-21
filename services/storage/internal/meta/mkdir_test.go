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

func TestMkdir_PositivePath(t *testing.T) {
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

	t.Run("创建子目录: 仅传低 12 位权限模式 (0o755) 自动合成为 S_IFDIR", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		expectedTargetMode := int32(meta.S_IFDIR | 0o755)
		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       expectedTargetMode,
			Size:       0,
			Nlink:      2,
			Uid:        1001,
			Gid:        1002,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  expectedTargetMode,
				Nlink: 2,
				Uid:   1001,
				Gid:   1002,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("subfolder"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			IncrementInodeNlinkAndTouch(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("subfolder"),
			Mode:      0o755,
			UID:       1001,
			GID:       1002,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, newIno, inode.Ino)
		assert.Equal(t, uint32(expectedTargetMode), inode.Attr.Mode)
		assert.Equal(t, uint32(2), inode.Attr.Nlink)
		assert.Equal(t, meta.DefaultDirSize, inode.Attr.Size)
		assert.Equal(t, uint32(1001), inode.Attr.UID)
		assert.Equal(t, uint32(1002), inode.Attr.GID)
		assert.Equal(t, uint32(0), inode.Attr.Rdev)
	})

	t.Run("创建子目录: 显式携带 S_IFDIR 位模式 (0o040755)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		expectedTargetMode := int32(meta.S_IFDIR | 0o755)
		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       expectedTargetMode,
			Size:       0,
			Nlink:      2,
			Uid:        1001,
			Gid:        1002,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  expectedTargetMode,
				Nlink: 2,
				Uid:   1001,
				Gid:   1002,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("subfolder"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			IncrementInodeNlinkAndTouch(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("subfolder"),
			Mode:      meta.S_IFDIR | 0o755,
			UID:       1001,
			GID:       1002,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(expectedTargetMode), inode.Attr.Mode)
		assert.Equal(t, uint32(2), inode.Attr.Nlink)
	})

	t.Run("父目录包含 S_ISGID (0o2000): 子目录双重继承父目录 GID 与 S_ISGID 模式位", func(t *testing.T) {
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

		expectedTargetMode := int32(meta.S_IFDIR | meta.S_ISGID | 0o750)
		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       expectedTargetMode,
			Size:       0,
			Nlink:      2,
			Uid:        1001,
			Gid:        2000,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  expectedTargetMode,
				Nlink: 2,
				Uid:   1001,
				Gid:   2000,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("shared_subfolder"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			IncrementInodeNlinkAndTouch(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("shared_subfolder"),
			Mode:      0o750,
			UID:       1001,
			GID:       3000, // 应当被父目录 GID 覆盖
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(2000), inode.Attr.GID)
		assert.Equal(t, uint32(expectedTargetMode), inode.Attr.Mode)
		assert.NotZero(t, inode.Attr.Mode&meta.S_ISGID)
	})

	t.Run("创建子目录: 零权限模式 (mode = 0) 合成为 S_IFDIR | 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		expectedTargetMode := int32(meta.S_IFDIR)
		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       expectedTargetMode,
			Size:       0,
			Nlink:      2,
			Uid:        1001,
			Gid:        1002,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  expectedTargetMode,
				Nlink: 2,
				Uid:   1001,
				Gid:   1002,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("zero_perm_dir"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			IncrementInodeNlinkAndTouch(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("zero_perm_dir"),
			Mode:      0,
			UID:       1001,
			GID:       1002,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(meta.S_IFDIR), inode.Attr.Mode)
	})

	t.Run("非 UTF-8 原始字节序列正常创建子目录", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		rawName := []byte{0xbb, 0xcc, 0xdd, 0xee}
		expectedTargetMode := int32(meta.S_IFDIR | 0o755)
		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       expectedTargetMode,
			Size:       0,
			Nlink:      2,
			Uid:        1001,
			Gid:        1002,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  expectedTargetMode,
				Nlink: 2,
				Uid:   1001,
				Gid:   1002,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      rawName,
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			IncrementInodeNlinkAndTouch(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      rawName,
			Mode:      0o755,
			UID:       1001,
			GID:       1002,
		})

		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, newIno, inode.Ino)
	})
}

func TestMkdir_InputValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name        string
		params      meta.MkdirParams
		expectedErr error
	}{
		{
			name: "parent_ino 为 0",
			params: meta.MkdirParams{
				ParentIno: 0,
				Name:      []byte("sub"),
				Mode:      0o755,
			},
		},
		{
			name: "parent_ino 超过 math.MaxInt64",
			params: meta.MkdirParams{
				ParentIno: uint64(math.MaxInt64) + 1,
				Name:      []byte("sub"),
				Mode:      0o755,
			},
		},
		{
			name: "name 为空字符串",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte(""),
				Mode:      0o755,
			},
		},
		{
			name: "name 长度超过 255 字节 (256 字节)",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte(strings.Repeat("a", 256)),
				Mode:      0o755,
			},
			expectedErr: meta.ErrNameTooLong,
		},
		{
			name: "name 包含斜杠 /",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub/dir"),
				Mode:      0o755,
			},
		},
		{
			name: "name 包含空字符 \\x00",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub\x00dir"),
				Mode:      0o755,
			},
		},
		{
			name: "mode 超过 65535",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub"),
				Mode:      65536,
			},
		},
		{
			name: "mode 包含 S_IFREG 常规文件类型位",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub"),
				Mode:      meta.S_IFREG | 0o755,
			},
		},
		{
			name: "mode 包含 S_IFLNK 符号链接类型位",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub"),
				Mode:      meta.S_IFLNK | 0o755,
			},
		},
		{
			name: "mode 包含 S_IFCHR 字符设备类型位",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub"),
				Mode:      meta.S_IFCHR | 0o755,
			},
		},
		{
			name: "mode 包含 S_IFBLK 块设备类型位",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub"),
				Mode:      meta.S_IFBLK | 0o755,
			},
		},
		{
			name: "mode 包含 S_IFIFO 管道类型位",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub"),
				Mode:      meta.S_IFIFO | 0o755,
			},
		},
		{
			name: "mode 包含 S_IFSOCK 套接字类型位",
			params: meta.MkdirParams{
				ParentIno: 1,
				Name:      []byte("sub"),
				Mode:      meta.S_IFSOCK | 0o755,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			// 验证不发起数据库交互，直接返回预期错误
			inode, err := mgr.Mkdir(ctx, tc.params)
			assert.Nil(t, inode)
			exp := tc.expectedErr
			if exp == nil {
				exp = meta.ErrInvalidArgument
			}
			assert.ErrorIs(t, err, exp)
		})
	}
}

func TestMkdir_ParentInvariantsAndErrors(t *testing.T) {
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

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("sub"),
			Mode:      0o755,
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

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("sub"),
			Mode:      0o755,
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

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("sub"),
			Mode:      0o755,
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

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("."),
			Mode:      0o755,
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

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte(".."),
			Mode:      0o755,
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
			CreateInode(mock.Anything, mock.Anything).
			Return(db.Inode{Ino: 101, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, mock.Anything).
			Return(&pgconn.PgError{Code: "23505"}).
			Once()

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("duplicate_folder"),
			Mode:      0o755,
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

		inode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: parentIno,
			Name:      []byte("sub"),
			Mode:      0o755,
		})

		assert.Nil(t, inode)
		assert.ErrorIs(t, err, dbErr)
	})
}
