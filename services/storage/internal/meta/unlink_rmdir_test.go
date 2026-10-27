package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestUnlink_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	childIno := uint64(100)

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

	t.Run("成功删除多硬链接普通文件: entries 行被删，目标 nlink 减 1 且仅更新 ctime，父目录 nlink 保持不变", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		childRow := db.Inode{
			Ino:        int64(childIno),
			Generation: 1,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       2048,
			Nlink:      2,
			Uid:        1001,
			Gid:        1001,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		// 1. 预读与持锁二次校验目录项
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
				Ino:       int64(childIno),
			}, nil).
			Twice()

		// 2. 按全局 ino 升序锁定父目录与子节点
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).
			Once()

		// 4. 删除目录项
		mockQuerier.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
			}).
			Return(nil).
			Once()

		// 5. 递减子节点 nlink 并刷新 ctime
		decrementedRow := childRow
		decrementedRow.Nlink = 1
		mockQuerier.EXPECT().
			DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(decrementedRow, nil).
			Once()

		// 6. 刷新父目录 mtime 和 ctime
		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		require.NoError(t, err)
	})

	t.Run("成功删除单硬链接普通文件: 目标变为孤儿节点 (nlink=0, 保留 Inode 行)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		childRow := db.Inode{
			Ino:        int64(childIno),
			Generation: 1,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       1024,
			Nlink:      1,
			Uid:        1001,
			Gid:        1001,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("orphan.txt"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("orphan.txt"),
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("orphan.txt"),
			}).
			Return(nil).
			Once()

		orphanRow := childRow
		orphanRow.Nlink = 0
		mockQuerier.EXPECT().
			DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(orphanRow, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(true, nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("orphan.txt"))
		require.NoError(t, err)
	})

	t.Run("成功删除符号链接: 目标符号链接变为孤儿节点 (nlink=0)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		childRow := db.Inode{
			Ino:           int64(childIno),
			Generation:    1,
			Mode:          int32(meta.S_IFLNK | 0o777),
			Size:          8,
			Nlink:         1,
			Uid:           1001,
			Gid:           1001,
			SymlinkTarget: []byte("/tmp/foo"),
			Atime:         pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:         pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:         pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("symlink"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("symlink"),
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("symlink"),
			}).
			Return(nil).
			Once()

		orphanRow := childRow
		orphanRow.Nlink = 0
		mockQuerier.EXPECT().
			DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(orphanRow, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(true, nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("symlink"))
		require.NoError(t, err)
	})

	t.Run("成功删除非 UTF-8 原始字节名称文件", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		rawName := []byte{0xfa, 0xfb, 0xfc}
		childRow := db.Inode{
			Ino:        int64(childIno),
			Generation: 1,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       512,
			Nlink:      1,
			Uid:        1001,
			Gid:        1001,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      rawName,
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      rawName,
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{
				ParentIno: int64(parentIno),
				Name:      rawName,
			}).
			Return(nil).
			Once()

		orphanRow := childRow
		orphanRow.Nlink = 0
		mockQuerier.EXPECT().
			DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(orphanRow, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(childIno)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, rawName)
		require.NoError(t, err)
	})

	t.Run("全局 Inode 升序加锁与并发漂移重试: 当 child_ino < parent_ino 时严格按升序加锁，且目录项并发漂移时自动重试", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier1 := dbmock.NewMockQuerier(t)
		mockQuerier2 := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		olderChildIno := int64(5)
		driftedChildIno := int64(8)
		newerParentIno := uint64(50)

		// 第一次事务：预读到 olderChildIno(5)，加锁后二次校验发现漂移为 driftedChildIno(8)，触发重试
		setupMockWriteTx(mockStore, mockQuerier1)

		mockQuerier1.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newerParentIno), Name: []byte("drift.txt")}).
			Return(db.Entry{ParentIno: int64(newerParentIno), Name: []byte("drift.txt"), Ino: olderChildIno}, nil).
			Once()
		mockQuerier1.EXPECT().
			GetInodeForUpdate(mock.Anything, olderChildIno).
			Return(db.Inode{Ino: olderChildIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()
		mockQuerier1.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(newerParentIno)).
			Return(db.Inode{Ino: int64(newerParentIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).
			Once()
		mockQuerier1.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newerParentIno), Name: []byte("drift.txt")}).
			Return(db.Entry{ParentIno: int64(newerParentIno), Name: []byte("drift.txt"), Ino: driftedChildIno}, nil).
			Once()

		// 第二次事务：预读到 driftedChildIno(8)，按 8 -> 50 升序加锁并顺利完成删除
		setupMockWriteTx(mockStore, mockQuerier2)

		var lockOrder []int64
		mockQuerier2.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newerParentIno), Name: []byte("drift.txt")}).
			Return(db.Entry{ParentIno: int64(newerParentIno), Name: []byte("drift.txt"), Ino: driftedChildIno}, nil).
			Twice()
		mockQuerier2.EXPECT().
			GetInodeForUpdate(mock.Anything, driftedChildIno).
			RunAndReturn(func(_ context.Context, ino int64) (db.Inode, error) {
				lockOrder = append(lockOrder, ino)
				return db.Inode{Ino: ino, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil
			}).
			Once()
		mockQuerier2.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(newerParentIno)).
			RunAndReturn(func(_ context.Context, ino int64) (db.Inode, error) {
				lockOrder = append(lockOrder, ino)
				return db.Inode{Ino: ino, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil
			}).
			Once()
		mockQuerier2.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{ParentIno: int64(newerParentIno), Name: []byte("drift.txt")}).
			Return(nil).
			Once()
		mockQuerier2.EXPECT().
			DecrementInodeNlinkAndTouchCtime(mock.Anything, driftedChildIno).
			Return(db.Inode{Ino: driftedChildIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 0}, nil).
			Once()
		mockQuerier2.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(newerParentIno)).
			Return(nil).
			Once()

		mockQuerier2.EXPECT().
			HasFileHandlesByIno(mock.Anything, driftedChildIno).
			Return(false, nil).
			Once()

		mockQuerier2.EXPECT().
			DeleteChunksByIno(mock.Anything, driftedChildIno).
			Return(nil, nil).
			Once()

		mockQuerier2.EXPECT().
			DeleteInode(mock.Anything, driftedChildIno).
			Return(nil).
			Once()

		err := mgr.Unlink(ctx, newerParentIno, []byte("drift.txt"))
		require.NoError(t, err)
		assert.Equal(t, []int64{driftedChildIno, int64(newerParentIno)}, lockOrder)
	})
}

func TestUnlink_InputValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	testCases := []struct {
		name        string
		parentIno   uint64
		entryName   []byte
		expectedErr error
	}{
		{name: "parent_ino 为 0", parentIno: 0, entryName: []byte("file.txt"), expectedErr: meta.ErrInvalidArgument},
		{name: "parent_ino 超过 math.MaxInt64", parentIno: uint64(math.MaxInt64) + 1, entryName: []byte("file.txt"), expectedErr: meta.ErrInvalidArgument},
		{name: "name 长度超过 255 字节", parentIno: 1, entryName: []byte(strings.Repeat("a", 256)), expectedErr: meta.ErrNameTooLong},
		{name: "name 包含斜杠 /", parentIno: 1, entryName: []byte("sub/file"), expectedErr: meta.ErrInvalidArgument},
		{name: "name 包含空字符 \\x00", parentIno: 1, entryName: []byte("file\x00name"), expectedErr: meta.ErrInvalidArgument},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := mgr.Unlink(ctx, tc.parentIno, tc.entryName)
			assert.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestUnlink_ParentInvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)
	childIno := uint64(100)

	t.Run("父节点不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
			}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("父节点非目录 (S_IFREG) 时返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
			}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("父目录为已解链孤儿目录 (nlink == 0) 时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
				Ino:       int64(childIno),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 0,
			}, nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标目录项不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("missing.txt"),
			}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("missing.txt"))
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标 Inode 不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("ghost.txt"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("ghost.txt"),
				Ino:       int64(childIno),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("ghost.txt"))
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标为目录 (S_IFDIR) 时返回 ErrIsDir (POSIX EISDIR)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("subdir"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("subdir"),
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 3,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(db.Inode{
				Ino:   int64(childIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("subdir"))
		assert.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("name 为 . 或 .. 时在确认父目录有效后返回 ErrInvalidArgument", func(t *testing.T) {
		for _, name := range [][]byte{[]byte("."), []byte("..")} {
			t.Run(string(name), func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mockQuerier := dbmock.NewMockQuerier(t)
				mgr := meta.New(mockStore)

				setupMockWriteTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().
					GetInodeForUpdate(mock.Anything, int64(parentIno)).
					Return(db.Inode{
						Ino:   int64(parentIno),
						Mode:  int32(meta.S_IFDIR | 0o755),
						Nlink: 2,
					}, nil).
					Once()

				err := mgr.Unlink(ctx, parentIno, name)
				assert.ErrorIs(t, err, meta.ErrInvalidArgument)
			})
		}
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db error")
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
			}).
			Return(db.Entry{}, dbErr).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestUnlink_DiskQuotaAndHandleLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	childIno := uint64(100)
	rootIno := int64(1)

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

	baseChildRow := db.Inode{
		Ino:        int64(childIno),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       4096,
		Nlink:      1,
		Uid:        1001,
		Gid:        1001,
		ParentIno:  int64(parentIno),
		RootIno:    rootIno,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	setupCommonUnlinkMocks := func(mockQuerier *dbmock.MockQuerier, child db.Inode, decrementedChild db.Inode, entryName []byte) {
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      entryName,
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      entryName,
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(child, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{
				ParentIno: int64(parentIno),
				Name:      entryName,
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(decrementedChild, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()
	}

	t.Run("nlink > 0 (多硬链接文件解链其一): 不调用 HasFileHandlesByIno / DeleteInode / AddDiskQuotaByRootIno", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		multiChild := baseChildRow
		multiChild.Nlink = 2
		decrementedChild := multiChild
		decrementedChild.Nlink = 1

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonUnlinkMocks(mockQuerier, multiChild, decrementedChild, []byte("file.txt"))

		// 不调用 HasFileHandlesByIno / DeleteInode / AddDiskQuotaByRootIno

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		require.NoError(t, err)
	})

	t.Run("nlink == 0 且无活跃 FileHandle (常规文件，RootIno > 0, Size: 4096): 物理删除 Inode 并退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		child := baseChildRow
		child.Size = 4096
		decrementedChild := child
		decrementedChild.Nlink = 0

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonUnlinkMocks(mockQuerier, child, decrementedChild, []byte("file.txt"))

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(childIno)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  -4096,
				DeltaInodes: -1,
			}).
			Return(nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		require.NoError(t, err)
	})

	t.Run("nlink == 0 且无活跃 FileHandle (符号链接，RootIno > 0, Size: 8): 物理删除 Inode 且仅扣减 used_inodes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		child := baseChildRow
		child.Mode = int32(meta.S_IFLNK | 0o777)
		child.Size = 8
		decrementedChild := child
		decrementedChild.Nlink = 0

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonUnlinkMocks(mockQuerier, child, decrementedChild, []byte("symlink"))

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  0,
				DeltaInodes: -1,
			}).
			Return(nil).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("symlink"))
		require.NoError(t, err)
	})

	t.Run("nlink == 0 但存在活跃 FileHandle (unlink-whilst-open): 保留孤儿 Inode 且不扣减配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		child := baseChildRow
		decrementedChild := child
		decrementedChild.Nlink = 0

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonUnlinkMocks(mockQuerier, child, decrementedChild, []byte("orphan.txt"))

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(true, nil).
			Once()

		// 不调用 DeleteInode 与 AddDiskQuotaByRootIno

		err := mgr.Unlink(ctx, parentIno, []byte("orphan.txt"))
		require.NoError(t, err)
	})

	t.Run("无活跃句柄但 child.RootIno == 0 时物理删除 Inode 但不调用 AddDiskQuotaByRootIno", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		child := baseChildRow
		child.RootIno = 0
		decrementedChild := child
		decrementedChild.Nlink = 0

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonUnlinkMocks(mockQuerier, child, decrementedChild, []byte("file.txt"))

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(childIno)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		// 不调用 AddDiskQuotaByRootIno

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		require.NoError(t, err)
	})

	t.Run("HasFileHandlesByIno 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		child := baseChildRow
		decrementedChild := child
		decrementedChild.Nlink = 0

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonUnlinkMocks(mockQuerier, child, decrementedChild, []byte("file.txt"))

		handleErr := errors.New("check handle failed")
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, handleErr).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		assert.ErrorIs(t, err, handleErr)
	})

	t.Run("DeleteInode 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		child := baseChildRow
		decrementedChild := child
		decrementedChild.Nlink = 0

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonUnlinkMocks(mockQuerier, child, decrementedChild, []byte("file.txt"))

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(childIno)).
			Return(nil, nil).
			Once()

		delErr := errors.New("delete inode failed")
		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(delErr).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		assert.ErrorIs(t, err, delErr)
	})

	t.Run("AddDiskQuotaByRootIno 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		child := baseChildRow
		decrementedChild := child
		decrementedChild.Nlink = 0

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonUnlinkMocks(mockQuerier, child, decrementedChild, []byte("file.txt"))

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(childIno)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		quotaErr := errors.New("decrement disk quota failed")
		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  -4096,
				DeltaInodes: -1,
			}).
			Return(quotaErr).
			Once()

		err := mgr.Unlink(ctx, parentIno, []byte("file.txt"))
		assert.ErrorIs(t, err, quotaErr)
	})
}

func TestRmdir_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	childIno := uint64(200)

	baseParentRow := db.Inode{
		Ino:        int64(parentIno),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      3, // 包含子目录 ..
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("成功移除空子目录: entries 行被删，目标 nlink 置 0，父目录 nlink 递减 1 且更新 mtime/ctime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		childRow := db.Inode{
			Ino:        int64(childIno),
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       4096,
			Nlink:      2,
			Uid:        1001,
			Gid:        1001,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		// 1. 预读与持锁二次校验目录项
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("emptydir"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("emptydir"),
				Ino:       int64(childIno),
			}, nil).
			Twice()

		// 2. 按全局 ino 升序锁定父目录与子目录
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).
			Once()

		// 4. 空目录检查
		mockQuerier.EXPECT().
			HasChildEntries(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		// 5. 删除目录项
		mockQuerier.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("emptydir"),
			}).
			Return(nil).
			Once()

		// 6. 子目录 nlink 置 0 并刷新 ctime
		orphanChild := childRow
		orphanChild.Nlink = 0
		mockQuerier.EXPECT().
			ClearInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(orphanChild, nil).
			Once()

		// 7. 父目录 nlink 递减 1 并刷新 mtime/ctime
		mockQuerier.EXPECT().
			DecrementInodeNlinkAndTouch(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		// 8. 检查是否存在活跃目录句柄
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		// 9. 物理删除 Inode 行
		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("emptydir"))
		require.NoError(t, err)
	})

	t.Run("成功移除非 UTF-8 原始字节名称空子目录", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		rawDirName := []byte{0xf0, 0xf1, 0xf2}
		childRow := db.Inode{
			Ino:        int64(childIno),
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       4096,
			Nlink:      2,
			Uid:        1001,
			Gid:        1001,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      rawDirName,
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      rawDirName,
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasChildEntries(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{
				ParentIno: int64(parentIno),
				Name:      rawDirName,
			}).
			Return(nil).
			Once()

		orphanChild := childRow
		orphanChild.Nlink = 0
		mockQuerier.EXPECT().
			ClearInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(orphanChild, nil).
			Once()

		mockQuerier.EXPECT().
			DecrementInodeNlinkAndTouch(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, rawDirName)
		require.NoError(t, err)
	})
}

func TestRmdir_InputValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	testCases := []struct {
		name        string
		parentIno   uint64
		entryName   []byte
		expectedErr error
	}{
		{name: "parent_ino 为 0", parentIno: 0, entryName: []byte("sub"), expectedErr: meta.ErrInvalidArgument},
		{name: "parent_ino 超过 math.MaxInt64", parentIno: uint64(math.MaxInt64) + 1, entryName: []byte("sub"), expectedErr: meta.ErrInvalidArgument},
		{name: "name 长度超过 255 字节", parentIno: 1, entryName: []byte(strings.Repeat("a", 256)), expectedErr: meta.ErrNameTooLong},
		{name: "name 包含斜杠 /", parentIno: 1, entryName: []byte("sub/dir"), expectedErr: meta.ErrInvalidArgument},
		{name: "name 包含空字符 \\x00", parentIno: 1, entryName: []byte("dir\x00name"), expectedErr: meta.ErrInvalidArgument},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := mgr.Rmdir(ctx, tc.parentIno, tc.entryName)
			assert.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestRmdir_ParentInvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)
	childIno := uint64(200)

	t.Run("父节点不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("sub"),
			}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("sub"))
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("父节点非目录 (S_IFREG) 时返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("sub"),
			}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("sub"))
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("父目录为已解链孤儿目录 (nlink == 0) 时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("sub"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("sub"),
				Ino:       int64(childIno),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 0,
			}, nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("sub"))
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标目录项不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("missing_dir"),
			}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("missing_dir"))
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标 Inode 不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("ghost_dir"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("ghost_dir"),
				Ino:       int64(childIno),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("ghost_dir"))
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标为非目录 (S_IFREG) 时返回 ErrNotDir (POSIX ENOTDIR)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("file.txt"),
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(db.Inode{
				Ino:   int64(childIno),
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("file.txt"))
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("目标目录非空时返回 ErrNotEmpty (POSIX ENOTEMPTY)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("nonempty_dir"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("nonempty_dir"),
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 3,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(db.Inode{
				Ino:   int64(childIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			HasChildEntries(mock.Anything, int64(childIno)).
			Return(true, nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("nonempty_dir"))
		assert.ErrorIs(t, err, meta.ErrNotEmpty)
	})

	t.Run("name 为 . 时在确认父目录有效后返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("."))
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("name 为 .. 时在确认父目录有效后返回 ErrNotEmpty (POSIX ENOTEMPTY)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}, nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte(".."))
		assert.ErrorIs(t, err, meta.ErrNotEmpty)
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db error")
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("sub"),
			}).
			Return(db.Entry{}, dbErr).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("sub"))
		assert.ErrorIs(t, err, dbErr)
	})

	t.Run("Unlink 与 Rmdir 空名称先校验父目录再返回 ErrNotFound 或 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		// 1. 父目录合法时返回 ErrNotFound
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{Ino: int64(parentIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).
			Once()
		assert.ErrorIs(t, mgr.Unlink(ctx, parentIno, []byte{}), meta.ErrNotFound)

		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{Ino: int64(parentIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).
			Once()
		assert.ErrorIs(t, mgr.Rmdir(ctx, parentIno, []byte{}), meta.ErrNotFound)

		// 2. 父节点非目录时返回 ErrNotDir
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{Ino: int64(parentIno), Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()
		assert.ErrorIs(t, mgr.Unlink(ctx, parentIno, []byte{}), meta.ErrNotDir)

		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{Ino: int64(parentIno), Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()
		assert.ErrorIs(t, mgr.Rmdir(ctx, parentIno, []byte{}), meta.ErrNotDir)
	})
}

func TestRmdir_DiskQuotaAndHandleLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	childIno := uint64(200)
	rootIno := int64(1)

	baseParentRow := db.Inode{
		Ino:        int64(parentIno),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      3,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		RootIno:    rootIno,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	baseChildRow := db.Inode{
		Ino:        int64(childIno),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      2,
		Uid:        1001,
		Gid:        1001,
		ParentIno:  int64(parentIno),
		RootIno:    rootIno,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	setupCommonRmdirMocks := func(mockQuerier *dbmock.MockQuerier, child db.Inode) {
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("emptydir"),
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      []byte("emptydir"),
				Ino:       int64(childIno),
			}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(child, nil).
			Once()

		mockQuerier.EXPECT().
			HasChildEntries(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteEntry(mock.Anything, db.DeleteEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("emptydir"),
			}).
			Return(nil).
			Once()

		orphanChild := child
		orphanChild.Nlink = 0
		mockQuerier.EXPECT().
			ClearInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(orphanChild, nil).
			Once()

		mockQuerier.EXPECT().
			DecrementInodeNlinkAndTouch(mock.Anything, int64(parentIno)).
			Return(nil).
			Once()
	}

	t.Run("无活跃句柄且 child.RootIno > 0 时物理删除 Inode 并扣减 used_inodes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonRmdirMocks(mockQuerier, baseChildRow)

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  0,
				DeltaInodes: -1,
			}).
			Return(nil).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("emptydir"))
		require.NoError(t, err)
	})

	t.Run("存在活跃句柄 (rmdir-whilst-open) 时保留孤儿目录 Inode 且不扣减 used_inodes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonRmdirMocks(mockQuerier, baseChildRow)

		// 存在活跃目录句柄：HasFileHandlesByIno 返回 true
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(true, nil).
			Once()

		// 不应调用 DeleteInode 与 AddDiskQuotaByRootIno

		err := mgr.Rmdir(ctx, parentIno, []byte("emptydir"))
		require.NoError(t, err)
	})

	t.Run("无活跃句柄但 child.RootIno == 0 时物理删除 Inode 但不调用 AddDiskQuotaByRootIno", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		childNoRoot := baseChildRow
		childNoRoot.RootIno = 0

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonRmdirMocks(mockQuerier, childNoRoot)

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		// 不应调用 AddDiskQuotaByRootIno

		err := mgr.Rmdir(ctx, parentIno, []byte("emptydir"))
		require.NoError(t, err)
	})

	t.Run("HasFileHandlesByIno 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonRmdirMocks(mockQuerier, baseChildRow)

		handleErr := errors.New("check handle failed")
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, handleErr).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("emptydir"))
		assert.ErrorIs(t, err, handleErr)
	})

	t.Run("DeleteInode 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonRmdirMocks(mockQuerier, baseChildRow)

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		delErr := errors.New("delete inode failed")
		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(delErr).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("emptydir"))
		assert.ErrorIs(t, err, delErr)
	})

	t.Run("AddDiskQuotaByRootIno 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)
		setupCommonRmdirMocks(mockQuerier, baseChildRow)

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).
			Once()

		quotaErr := errors.New("decrement disk quota failed")
		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  0,
				DeltaInodes: -1,
			}).
			Return(quotaErr).
			Once()

		err := mgr.Rmdir(ctx, parentIno, []byte("emptydir"))
		assert.ErrorIs(t, err, quotaErr)
	})
}

func TestMkdir_Rmdir_StatFS_Integration(t *testing.T) {
	dsn := os.Getenv("STORAGE_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过集成测试: 连接 postgres 失败: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("跳过集成测试 (ping 失败): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	// 创建独立测试虚拟磁盘
	diskName := fmt.Sprintf("test_statfs_%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 10 * 1024 * 1024 * 1024,
	})
	require.NoError(t, err)
	rootIno := disk.RootIno
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM disks WHERE disk_id = $1", disk.DiskID)
		_ = store.DeleteInode(context.Background(), int64(rootIno))
	}()

	// 1. 验证初始磁盘 StatFS：files 应当为 1（根目录节点占用）
	statBefore, err := mgr.StatFS(ctx, rootIno)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), statBefore.Files)

	// 2. Mkdir 创建子目录
	subDirName := []byte("integ_sub_dir")
	subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: rootIno,
		Name:      subDirName,
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)

	childRow, err := store.GetInode(ctx, int64(subDir.Ino))
	require.NoError(t, err)
	assert.Equal(t, int64(rootIno), childRow.RootIno, "子目录必须继承 parent 的 RootIno")

	// 3. Mkdir 后 StatFS：files 递增 1，变为 2
	statAfterMkdir, err := mgr.StatFS(ctx, rootIno)
	require.NoError(t, err)
	assert.Equal(t, statBefore.Files+1, statAfterMkdir.Files, "Mkdir 后 files 必须递增 1")

	// 通过子目录自身 ino 查询 StatFS 同样有效且统计一致
	statFromSubDir, err := mgr.StatFS(ctx, subDir.Ino)
	require.NoError(t, err)
	assert.Equal(t, statAfterMkdir.Files, statFromSubDir.Files)

	// 4. Rmdir（无活跃句柄）删除空子目录
	err = mgr.Rmdir(ctx, rootIno, subDirName)
	require.NoError(t, err)

	// 5. Rmdir 后 StatFS：files 递减 1，恢复为初始值
	statAfterRmdir, err := mgr.StatFS(ctx, rootIno)
	require.NoError(t, err)
	assert.Equal(t, statBefore.Files, statAfterRmdir.Files, "Rmdir 后 files 必须递减 1 恢复初始值")

	// 6. 验证子目录 Inode 已被物理删除
	_, err = store.GetInode(ctx, int64(subDir.Ino))
	assert.ErrorIs(t, err, pgx.ErrNoRows, "无打开句柄的空目录在 Rmdir 后 Inode 必须被物理清理")
}
