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
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestCreate_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	newIno := uint64(100)
	expectedFh := int64(1)

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

	t.Run("常规文件原子创建并归一化仅含权限位的 mode", func(t *testing.T) {
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
				Name:      []byte("test_create.txt"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		createdHandleRow := db.FileHandle{
			Fh:        expectedFh,
			Ino:       int64(newIno),
			Flags:     int32(meta.O_CREAT | meta.O_RDWR),
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(newIno),
				Flags: int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(createdHandleRow, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      []byte("test_create.txt"),
			Mode:      0o644, // 仅传入权限位，预期自动补齐 S_IFREG
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(expectedFh), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
		require.NotNil(t, res.Inode)
		assert.Equal(t, newIno, res.Inode.Ino)
		assert.Equal(t, meta.S_IFREG|0o644, res.Inode.Attr.Mode)
		assert.Equal(t, uint64(0), res.Inode.Attr.Size)
		assert.Equal(t, uint32(1), res.Inode.Attr.Nlink)
		assert.Equal(t, uint32(1001), res.Inode.Attr.UID)
		assert.Equal(t, uint32(1002), res.Inode.Attr.GID)
		assert.Equal(t, uint32(0), res.Inode.Attr.Rdev)
	})

	t.Run("显式携带 S_IFREG 模式位创建常规文件", func(t *testing.T) {
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
			Mode:       int32(meta.S_IFREG | 0o755),
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
				Mode:  int32(meta.S_IFREG | 0o755),
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
				Name:      []byte("script.sh"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		createdHandleRow := db.FileHandle{
			Fh:        2,
			Ino:       int64(newIno),
			Flags:     int32(meta.O_CREAT | meta.O_WRONLY),
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(newIno),
				Flags: int32(meta.O_CREAT | meta.O_WRONLY),
			}).
			Return(createdHandleRow, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      []byte("script.sh"),
			Mode:      meta.S_IFREG | 0o755,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_CREAT | meta.O_WRONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(2), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
		assert.Equal(t, meta.S_IFREG|0o755, res.Inode.Attr.Mode)
	})

	t.Run("父目录包含 S_ISGID 标志时子文件继承父目录 GID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		parentWithSGID := baseParentRow
		parentWithSGID.Mode = int32(meta.S_IFDIR | meta.S_ISGID | 0o755)
		parentWithSGID.Gid = 9999

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentWithSGID, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       0,
			Nlink:      1,
			Uid:        1001,
			Gid:        9999, // 继承自父目录 GID
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
				Gid:   9999, // 继承父目录
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("sgid_child.txt"),
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		createdHandleRow := db.FileHandle{
			Fh:        3,
			Ino:       int64(newIno),
			Flags:     int32(meta.O_CREAT | meta.O_RDWR),
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(newIno),
				Flags: int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(createdHandleRow, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      []byte("sgid_child.txt"),
			Mode:      0o644,
			UID:       1001,
			GID:       1002, // 传入 1002，但由于父目录开启 S_ISGID，预期被覆盖为 9999
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint32(9999), res.Inode.Attr.GID)
	})

	t.Run("非 UTF-8 原始字节序列正常创建文件与目录项", func(t *testing.T) {
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

		rawName := []byte{0xff, 0xfe, 0x80, 0x81, 't', 'e', 's', 't'}

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
				Name:      rawName,
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		createdHandleRow := db.FileHandle{
			Fh:        4,
			Ino:       int64(newIno),
			Flags:     int32(meta.O_CREAT | meta.O_RDWR),
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(newIno),
				Flags: int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(createdHandleRow, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      rawName,
			Mode:      0o644,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(4), res.Fh)
		assert.Equal(t, newIno, res.Inode.Ino)
	})
}

func TestCreate_InputValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	testCases := []struct {
		name        string
		params      meta.CreateParams
		expectedErr error
	}{
		{
			name: "parent_ino 为 0",
			params: meta.CreateParams{
				ParentIno: 0,
				Name:      []byte("file.txt"),
				Mode:      0o644,
			},
		},
		{
			name: "parent_ino 超过 math.MaxInt64",
			params: meta.CreateParams{
				ParentIno: uint64(math.MaxInt64) + 1,
				Name:      []byte("file.txt"),
				Mode:      0o644,
			},
		},
		{
			name: "name 为空字符串",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte(""),
				Mode:      0o644,
			},
		},
		{
			name: "name 长度超过 255 字节",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte(strings.Repeat("a", 256)),
				Mode:      0o644,
			},
			expectedErr: meta.ErrNameTooLong,
		},
		{
			name: "name 包含斜杠 /",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("sub/file.txt"),
				Mode:      0o644,
			},
		},
		{
			name: "name 包含空字符 \\x00",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file\x00.txt"),
				Mode:      0o644,
			},
		},
		{
			name: "mode 超过 0xffff (65535)",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      65536,
			},
		},
		{
			name: "mode 文件类型为目录 S_IFDIR",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFDIR | 0o755,
			},
		},
		{
			name: "mode 文件类型为符号链接 S_IFLNK",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFLNK | 0o777,
			},
		},
		{
			name: "mode 文件类型为字符设备 S_IFCHR",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFCHR | 0o666,
			},
		},
		{
			name: "mode 文件类型为块设备 S_IFBLK",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFBLK | 0o666,
			},
		},
		{
			name: "mode 文件类型为管道 S_IFIFO",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFIFO | 0o644,
			},
		},
		{
			name: "mode 文件类型为套接字 S_IFSOCK",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFSOCK | 0o666,
			},
		},
		{
			name: "flags 携带 O_TMPFILE 标志位",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      0o644,
				Flags:     meta.O_TMPFILE,
			},
		},
		{
			name: "flags 携带 O_TMPFILE | O_RDWR 组合标志位",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      0o644,
				Flags:     meta.O_TMPFILE | meta.O_RDWR,
			},
		},
		{
			name: "flags 携带 O_TMPFILE | O_CREAT | O_WRONLY 组合标志位",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      0o644,
				Flags:     meta.O_TMPFILE | meta.O_CREAT | meta.O_WRONLY,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := mgr.Create(ctx, tc.params)
			assert.Nil(t, res)
			exp := tc.expectedErr
			if exp == nil {
				exp = meta.ErrInvalidArgument
			}
			assert.ErrorIs(t, err, exp)
		})
	}
}

func TestCreate_ParentInvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)

	baseParentRow := db.Inode{
		Ino:        int64(parentIno),
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

	validParams := meta.CreateParams{
		ParentIno: parentIno,
		Name:      []byte("valid_file.txt"),
		Mode:      0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_CREAT | meta.O_RDWR,
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

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("父节点非目录 (S_IFREG) 时返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileParent := baseParentRow
		fileParent.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(fileParent, nil).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
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

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("name 为 . 或 .. 时在确认父目录有效后返回 ErrAlreadyExists", func(t *testing.T) {
		for _, reserved := range [][]byte{[]byte("."), []byte("..")} {
			t.Run(string(reserved), func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mockQuerier := dbmock.NewMockQuerier(t)
				mgr := meta.New(mockStore)

				setupMockWriteTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().
					GetInodeForUpdate(mock.Anything, int64(parentIno)).
					Return(baseParentRow, nil).
					Once()

				params := validParams
				params.Name = reserved

				res, err := mgr.Create(ctx, params)
				assert.Nil(t, res)
				assert.ErrorIs(t, err, meta.ErrAlreadyExists)
			})
		}
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db disk failure")
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, dbErr).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get parent inode for update")
	})
}

func TestCreate_AlreadyExistsAndFailureHandling(t *testing.T) {
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
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	validParams := meta.CreateParams{
		ParentIno: parentIno,
		Name:      []byte("conflict_file.txt"),
		Mode:      0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_CREAT | meta.O_RDWR,
	}

	t.Run("已有同名 Entry 捕获 SQLSTATE 23505 映射为 ErrAlreadyExists", func(t *testing.T) {
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
			Uid:        1000,
			Gid:        1000,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
				Uid:   1000,
				Gid:   1000,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		pgErr := &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint pk_entries"}
		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      validParams.Name,
				Ino:       int64(newIno),
			}).
			Return(pgErr).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("CreateFileHandle 失败时透传底层错误", func(t *testing.T) {
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
			Uid:        1000,
			Gid:        1000,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
				Uid:   1000,
				Gid:   1000,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: int64(parentIno),
				Name:      validParams.Name,
				Ino:       int64(newIno),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		handleErr := errors.New("file handle sequence overflow")
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(newIno),
				Flags: int32(validParams.Flags),
			}).
			Return(db.FileHandle{}, handleErr).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create file handle")
	})
}

// TestCreate_RejectOTmpfile 验证 Create 收到携带 O_TMPFILE 标志位时静态入参校验直接拒绝。
func TestCreate_RejectOTmpfile(t *testing.T) {
	ctx := context.Background()

	t.Run("Create 收到 flags 携带 O_TMPFILE 时直接静态拒绝并映射为 EINVAL", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		// 零 Mock 预期设置：静态入参校验严禁触发任何数据库读写算子
		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("forged_tmpfile.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR | meta.O_TMPFILE,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)

		// 验证错误契约映射为 EINVAL
		st := meta.ToStatus(err)
		require.NotNil(t, st)
		require.NotEmpty(t, st.Details())
		info, ok := st.Details()[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, "EINVAL", info.Reason)
		assert.Equal(t, meta.ErrorDomain, info.Domain)
	})

	t.Run("Create 应用 Umask 并下发 WithOpenKeepCache 及自定义缓存时效", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore,
			meta.WithOpenKeepCache(true),
			meta.WithEntryTimeout(3.5),
			meta.WithAttrTimeout(4.5),
		)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).
			Once()

		expectedMode := int32(meta.S_IFREG | 0o640)
		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  expectedMode,
				Nlink: 1,
				Uid:   1000,
				Gid:   1000,
			}).
			Return(db.Inode{Ino: 100, Mode: expectedMode, Nlink: 1, Uid: 1000, Gid: 1000}, nil).
			Once()

		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: 1,
				Name:      []byte("masked.txt"),
				Ino:       100,
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(1)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   100,
				Flags: int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(db.FileHandle{Fh: 55, Ino: 100, Flags: int32(meta.O_CREAT | meta.O_RDWR)}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("masked.txt"),
			Mode:      0o666,
			Umask:     0o026,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint32(expectedMode), res.Inode.Attr.Mode)
		assert.Equal(t, uint64(0), res.Inode.Attr.ParentIno)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, res.OpenFlags)
		assert.Equal(t, 3.5, res.EntryTimeoutSeconds)
		assert.Equal(t, 4.5, res.AttrTimeoutSeconds)
	})
}

