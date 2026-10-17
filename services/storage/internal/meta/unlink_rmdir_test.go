package meta_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
				Name:      "file.txt",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "file.txt",
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
				Name:      "file.txt",
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

		err := mgr.Unlink(ctx, parentIno, "file.txt")
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
				Name:      "orphan.txt",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "orphan.txt",
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
				Name:      "orphan.txt",
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

		err := mgr.Unlink(ctx, parentIno, "orphan.txt")
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
				Name:      "symlink",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "symlink",
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
				Name:      "symlink",
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

		err := mgr.Unlink(ctx, parentIno, "symlink")
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
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newerParentIno), Name: "drift.txt"}).
			Return(db.Entry{ParentIno: int64(newerParentIno), Name: "drift.txt", Ino: olderChildIno}, nil).
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
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newerParentIno), Name: "drift.txt"}).
			Return(db.Entry{ParentIno: int64(newerParentIno), Name: "drift.txt", Ino: driftedChildIno}, nil).
			Once()

		// 第二次事务：预读到 driftedChildIno(8)，按 8 -> 50 升序加锁并顺利完成删除
		setupMockWriteTx(mockStore, mockQuerier2)

		var lockOrder []int64
		mockQuerier2.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newerParentIno), Name: "drift.txt"}).
			Return(db.Entry{ParentIno: int64(newerParentIno), Name: "drift.txt", Ino: driftedChildIno}, nil).
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
			DeleteEntry(mock.Anything, db.DeleteEntryParams{ParentIno: int64(newerParentIno), Name: "drift.txt"}).
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

		err := mgr.Unlink(ctx, newerParentIno, "drift.txt")
		require.NoError(t, err)
		assert.Equal(t, []int64{driftedChildIno, int64(newerParentIno)}, lockOrder)
	})
}

func TestUnlink_InputValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	testCases := []struct {
		name      string
		parentIno uint64
		entryName string
	}{
		{name: "parent_ino 为 0", parentIno: 0, entryName: "file.txt"},
		{name: "parent_ino 超过 math.MaxInt64", parentIno: uint64(math.MaxInt64) + 1, entryName: "file.txt"},
		{name: "name 为空字符串", parentIno: 1, entryName: ""},
		{name: "name 长度超过 255 字节", parentIno: 1, entryName: strings.Repeat("a", 256)},
		{name: "name 包含斜杠 /", parentIno: 1, entryName: "sub/file"},
		{name: "name 包含空字符 \\x00", parentIno: 1, entryName: "file\x00name"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := mgr.Unlink(ctx, tc.parentIno, tc.entryName)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
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
				Name:      "file.txt",
			}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Unlink(ctx, parentIno, "file.txt")
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
				Name:      "file.txt",
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

		err := mgr.Unlink(ctx, parentIno, "file.txt")
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
				Name:      "file.txt",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "file.txt",
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

		err := mgr.Unlink(ctx, parentIno, "file.txt")
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
				Name:      "missing.txt",
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

		err := mgr.Unlink(ctx, parentIno, "missing.txt")
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
				Name:      "ghost.txt",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "ghost.txt",
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

		err := mgr.Unlink(ctx, parentIno, "ghost.txt")
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
				Name:      "subdir",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "subdir",
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

		err := mgr.Unlink(ctx, parentIno, "subdir")
		assert.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("name 为 . 或 .. 时在确认父目录有效后返回 ErrInvalidArgument", func(t *testing.T) {
		for _, name := range []string{".", ".."} {
			t.Run(name, func(t *testing.T) {
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
				Name:      "file.txt",
			}).
			Return(db.Entry{}, dbErr).
			Once()

		err := mgr.Unlink(ctx, parentIno, "file.txt")
		assert.ErrorIs(t, err, dbErr)
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
				Name:      "emptydir",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "emptydir",
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
				Name:      "emptydir",
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

		err := mgr.Rmdir(ctx, parentIno, "emptydir")
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
		entryName   string
		expectedErr error
	}{
		{name: "parent_ino 为 0", parentIno: 0, entryName: "sub", expectedErr: meta.ErrInvalidArgument},
		{name: "parent_ino 超过 math.MaxInt64", parentIno: uint64(math.MaxInt64) + 1, entryName: "sub", expectedErr: meta.ErrInvalidArgument},
		{name: "name 为空字符串", parentIno: 1, entryName: "", expectedErr: meta.ErrInvalidArgument},
		{name: "name 长度超过 255 字节", parentIno: 1, entryName: strings.Repeat("a", 256), expectedErr: meta.ErrInvalidArgument},
		{name: "name 包含斜杠 /", parentIno: 1, entryName: "sub/dir", expectedErr: meta.ErrInvalidArgument},
		{name: "name 包含空字符 \\x00", parentIno: 1, entryName: "dir\x00name", expectedErr: meta.ErrInvalidArgument},
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
				Name:      "sub",
			}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Rmdir(ctx, parentIno, "sub")
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
				Name:      "sub",
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

		err := mgr.Rmdir(ctx, parentIno, "sub")
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
				Name:      "sub",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "sub",
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

		err := mgr.Rmdir(ctx, parentIno, "sub")
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
				Name:      "missing_dir",
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

		err := mgr.Rmdir(ctx, parentIno, "missing_dir")
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
				Name:      "ghost_dir",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "ghost_dir",
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

		err := mgr.Rmdir(ctx, parentIno, "ghost_dir")
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
				Name:      "file.txt",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "file.txt",
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

		err := mgr.Rmdir(ctx, parentIno, "file.txt")
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
				Name:      "nonempty_dir",
			}).
			Return(db.Entry{
				ParentIno: int64(parentIno),
				Name:      "nonempty_dir",
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

		err := mgr.Rmdir(ctx, parentIno, "nonempty_dir")
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

		err := mgr.Rmdir(ctx, parentIno, ".")
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

		err := mgr.Rmdir(ctx, parentIno, "..")
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
				Name:      "sub",
			}).
			Return(db.Entry{}, dbErr).
			Once()

		err := mgr.Rmdir(ctx, parentIno, "sub")
		assert.ErrorIs(t, err, dbErr)
	})
}

