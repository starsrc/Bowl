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

func TestRename_InputValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	testCases := []struct {
		name   string
		params meta.RenameParams
	}{
		{name: "old_parent_ino 为 0", params: meta.RenameParams{OldParentIno: 0, OldName: "a", NewParentIno: 1, NewName: "b"}},
		{name: "old_parent_ino 超过 math.MaxInt64", params: meta.RenameParams{OldParentIno: uint64(math.MaxInt64) + 1, OldName: "a", NewParentIno: 1, NewName: "b"}},
		{name: "new_parent_ino 为 0", params: meta.RenameParams{OldParentIno: 1, OldName: "a", NewParentIno: 0, NewName: "b"}},
		{name: "new_parent_ino 超过 math.MaxInt64", params: meta.RenameParams{OldParentIno: 1, OldName: "a", NewParentIno: uint64(math.MaxInt64) + 1, NewName: "b"}},
		{name: "old_name 为空", params: meta.RenameParams{OldParentIno: 1, OldName: "", NewParentIno: 1, NewName: "b"}},
		{name: "old_name 超过 255 字节", params: meta.RenameParams{OldParentIno: 1, OldName: strings.Repeat("a", 256), NewParentIno: 1, NewName: "b"}},
		{name: "old_name 包含 /", params: meta.RenameParams{OldParentIno: 1, OldName: "sub/a", NewParentIno: 1, NewName: "b"}},
		{name: "old_name 包含 \\x00", params: meta.RenameParams{OldParentIno: 1, OldName: "a\x00b", NewParentIno: 1, NewName: "b"}},
		{name: "new_name 为空", params: meta.RenameParams{OldParentIno: 1, OldName: "a", NewParentIno: 1, NewName: ""}},
		{name: "new_name 超过 255 字节", params: meta.RenameParams{OldParentIno: 1, OldName: "a", NewParentIno: 1, NewName: strings.Repeat("b", 256)}},
		{name: "new_name 包含 /", params: meta.RenameParams{OldParentIno: 1, OldName: "a", NewParentIno: 1, NewName: "sub/b"}},
		{name: "new_name 包含 \\x00", params: meta.RenameParams{OldParentIno: 1, OldName: "a", NewParentIno: 1, NewName: "b\x00c"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := mgr.Rename(ctx, tc.params)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

func TestRename_ShortCircuit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	parentIno := uint64(10)
	parentRow := db.Inode{
		Ino:   int64(parentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("同路径短路 (No-op): old_parent == new_parent 且 old_name == new_name 直接成功返回", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      "same.txt",
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: "same.txt", Ino: 100}, nil).
			Once()

		// newEntry 也查询相同的 key，返回同条目
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      "same.txt",
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: "same.txt", Ino: 100}, nil).
			Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      "same.txt",
			NewParentIno: parentIno,
			NewName:      "same.txt",
		})
		require.NoError(t, err)
	})

	t.Run("同节点硬链接互转短路 (POSIX No-op): 目标已存在且 old_ino == new_ino 无副作用返回", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      "link1",
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: "link1", Ino: 100}, nil).
			Once()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      "link2",
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: "link2", Ino: 100}, nil).
			Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      "link1",
			NewParentIno: parentIno,
			NewName:      "link2",
		})
		require.NoError(t, err)
	})
}

func TestRename_File_Positive(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	oldParentIno := uint64(10)
	newParentIno := uint64(20)

	oldParentRow := db.Inode{
		Ino:   int64(oldParentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 3,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}
	newParentRow := db.Inode{
		Ino:   int64(newParentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	srcFileIno := int64(100)
	srcFileRow := db.Inode{
		Ino:   srcFileIno,
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("跨目录移动普通文件 (无覆盖): 锁序 min->max，更新目录项，刷新源 ctime 与双父目录时间戳，父目录 nlink 保持不变", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		// 1. 父目录升序加锁: 10 -> 20
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()

		// 2. 查源目录项与目标目录项
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "a.txt"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "a.txt", Ino: srcFileIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: "b.txt"}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()

		// 3. 子节点加锁 (仅源节点)
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcFileIno).Return(srcFileRow, nil).Once()

		// 4. 更新目录项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      "a.txt",
			NewParentIno: int64(newParentIno),
			NewName:      "b.txt",
		}).Return(nil).Once()

		// 5. 刷新源节点 ctime
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcFileIno).Return(nil).Once()

		// 6. 刷新两个父目录时间戳
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      "a.txt",
			NewParentIno: newParentIno,
			NewName:      "b.txt",
		})
		require.NoError(t, err)
	})

	t.Run("跨目录覆盖已有文件: 被覆盖文件减 nlink 且保留 Inode，更新源目录项，双父目录 nlink 保持不变", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		targetFileIno := int64(200)
		targetFileRow := db.Inode{
			Ino:   targetFileIno,
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 2,
		}

		// 1. 父目录加锁
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()

		// 2. 查目录项
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "a.txt"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "a.txt", Ino: srcFileIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: "b.txt"}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: "b.txt", Ino: targetFileIno}, nil).Once()

		// 3. 子节点升序加锁: min(100, 200) -> 100, max -> 200
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcFileIno).Return(srcFileRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, targetFileIno).Return(targetFileRow, nil).Once()

		// 4. 删除原目标目录项并递减其 nlink
		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(newParentIno),
			Name:      "b.txt",
		}).Return(nil).Once()

		targetDecremented := targetFileRow
		targetDecremented.Nlink = 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetFileIno).
			Return(targetDecremented, nil).Once()

		// 5. 更新源项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      "a.txt",
			NewParentIno: int64(newParentIno),
			NewName:      "b.txt",
		}).Return(nil).Once()

		// 6. 刷新源 ctime
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcFileIno).Return(nil).Once()

		// 7. 刷新父目录时间戳
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      "a.txt",
			NewParentIno: newParentIno,
			NewName:      "b.txt",
		})
		require.NoError(t, err)
	})
}

func TestRename_Directory_Positive(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	oldParentIno := uint64(10)
	newParentIno := uint64(20)

	oldParentRow := db.Inode{
		Ino:   int64(oldParentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 3,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}
	newParentRow := db.Inode{
		Ino:   int64(newParentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	srcDirIno := int64(300)
	srcDirRow := db.Inode{
		Ino:   srcDirIno,
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("跨目录移动子目录 (无覆盖): oldParent nlink 减 1, newParent nlink 加 1, 环检测通过", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		// 1. 父目录加锁
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()

		// 2. 查目录项
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "subdir"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "subdir", Ino: srcDirIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: "destdir"}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()

		// 3. 子目录加锁
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcDirIno).Return(srcDirRow, nil).Once()

		// 4. 环检测: IsSubdirectory 返回 false
		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: srcDirIno,
		}).Return(false, nil).Once()

		// 5. 更新项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      "subdir",
			NewParentIno: int64(newParentIno),
			NewName:      "destdir",
		}).Return(nil).Once()

		// 6. 刷新源 ctime
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcDirIno).Return(nil).Once()

		// 7. oldParent nlink - 1, newParent nlink + 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().IncrementInodeNlinkAndTouch(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      "subdir",
			NewParentIno: newParentIno,
			NewName:      "destdir",
		})
		require.NoError(t, err)
	})

	t.Run("跨目录覆盖空目录: oldParent nlink 减 1, newParent nlink 保持不变 (+1 与 -1 抵消)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		targetDirIno := int64(400)
		targetDirRow := db.Inode{
			Ino:   targetDirIno,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "subdir"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "subdir", Ino: srcDirIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: "targetdir"}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: "targetdir", Ino: targetDirIno}, nil).Once()

		// 子节点按 300 -> 400 升序锁
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcDirIno).Return(srcDirRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, targetDirIno).Return(targetDirRow, nil).Once()

		// 环检测
		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: srcDirIno,
		}).Return(false, nil).Once()

		// 目标空目录校验: HasChildEntries 为 false
		mockQuerier.EXPECT().HasChildEntries(mock.Anything, targetDirIno).Return(false, nil).Once()

		// 删除目标项
		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(newParentIno),
			Name:      "targetdir",
		}).Return(nil).Once()

		// 目标目录置 nlink = 0
		orphanTarget := targetDirRow
		orphanTarget.Nlink = 0
		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetDirIno).Return(orphanTarget, nil).Once()

		// 更新源项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      "subdir",
			NewParentIno: int64(newParentIno),
			NewName:      "targetdir",
		}).Return(nil).Once()

		// 刷新源 ctime
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcDirIno).Return(nil).Once()

		// oldParent nlink 减 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()

		// newParent 仅刷新时间戳 (nlink 抵消)
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      "subdir",
			NewParentIno: newParentIno,
			NewName:      "targetdir",
		})
		require.NoError(t, err)
	})

	t.Run("同父目录内覆盖空目录: 父目录 nlink 减 1", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		targetDirIno := int64(400)
		targetDirRow := db.Inode{
			Ino:   targetDirIno,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "dir1"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "dir1", Ino: srcDirIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "dir2"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "dir2", Ino: targetDirIno}, nil).Once()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcDirIno).Return(srcDirRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, targetDirIno).Return(targetDirRow, nil).Once()

		mockQuerier.EXPECT().HasChildEntries(mock.Anything, targetDirIno).Return(false, nil).Once()

		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(oldParentIno),
			Name:      "dir2",
		}).Return(nil).Once()

		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetDirIno).Return(targetDirRow, nil).Once()

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      "dir1",
			NewParentIno: int64(oldParentIno),
			NewName:      "dir2",
		}).Return(nil).Once()

		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcDirIno).Return(nil).Once()

		// 同父目录覆盖空目录: 父目录 nlink - 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      "dir1",
			NewParentIno: oldParentIno,
			NewName:      "dir2",
		})
		require.NoError(t, err)
	})
}

func TestRename_NegativeCases(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	oldParentIno := uint64(10)
	newParentIno := uint64(20)

	parentRow := db.Inode{
		Ino:   int64(oldParentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("源父目录不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(db.Inode{}, pgx.ErrNoRows).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "a", NewParentIno: oldParentIno, NewName: "b"})
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("源父目录非目录返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		notDirParent := parentRow
		notDirParent.Mode = int32(meta.S_IFREG | 0o644)
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(notDirParent, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "a", NewParentIno: oldParentIno, NewName: "b"})
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("源父目录为孤儿目录 (nlink == 0) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanParent := parentRow
		orphanParent.Nlink = 0
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(orphanParent, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "a", NewParentIno: oldParentIno, NewName: "b"})
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("源项不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "missing"}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "missing", NewParentIno: oldParentIno, NewName: "b"})
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目录移动到自身返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirIno := int64(20) // new_parent_ino == old_ino == 20
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(10)).Return(parentRow, nil).Once()
		newParent := parentRow
		newParent.Ino = dirIno
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(newParent, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(10), Name: "dir"}).
			Return(db.Entry{ParentIno: 10, Name: "dir", Ino: dirIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: dirIno, Name: "dest"}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()

		dirRow := db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(dirRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: 10, OldName: "dir", NewParentIno: uint64(dirIno), NewName: "dest"})
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("目录移动到其子孙目录返回 ErrInvalidArgument (环检测)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirIno := int64(30)
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(10)).Return(parentRow, nil).Once()
		newParent := parentRow
		newParent.Ino = int64(newParentIno)
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParent, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(10), Name: "dir"}).
			Return(db.Entry{ParentIno: 10, Name: "dir", Ino: dirIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: "dest"}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()

		dirRow := db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(dirRow, nil).Once()

		// 环检测命中: is_sub = true
		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: dirIno,
		}).Return(true, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: 10, OldName: "dir", NewParentIno: newParentIno, NewName: "dest"})
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("源为目录覆盖非目录文件返回 ErrNotDir (POSIX ENOTDIR)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirIno := int64(100)
		fileIno := int64(200)

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "dir"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "dir", Ino: dirIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "file"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "file", Ino: fileIno}, nil).Once()

		dirRow := db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}
		fileRow := db.Inode{Ino: fileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(dirRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, fileIno).Return(fileRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "dir", NewParentIno: oldParentIno, NewName: "file"})
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("源为非目录文件覆盖目录返回 ErrIsDir (POSIX EISDIR)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileIno := int64(100)
		dirIno := int64(200)

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "file"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "file", Ino: fileIno}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "dir"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "dir", Ino: dirIno}, nil).Once()

		fileRow := db.Inode{Ino: fileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}
		dirRow := db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, fileIno).Return(fileRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(dirRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "file", NewParentIno: oldParentIno, NewName: "dir"})
		assert.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("源为目录覆盖非空目录返回 ErrNotEmpty (POSIX ENOTEMPTY)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dir1Ino := int64(100)
		dir2Ino := int64(200)

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "dir1"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "dir1", Ino: dir1Ino}, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: "dir2"}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: "dir2", Ino: dir2Ino}, nil).Once()

		dir1Row := db.Inode{Ino: dir1Ino, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}
		dir2Row := db.Inode{Ino: dir2Ino, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 3}

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dir1Ino).Return(dir1Row, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dir2Ino).Return(dir2Row, nil).Once()

		mockQuerier.EXPECT().HasChildEntries(mock.Anything, dir2Ino).Return(true, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "dir1", NewParentIno: oldParentIno, NewName: "dir2"})
		assert.ErrorIs(t, err, meta.ErrNotEmpty)
	})

	t.Run("old_name 或 new_name 为 . 或 .. 时在确认父目录有效后返回 ErrInvalidArgument", func(t *testing.T) {
		for _, name := range []string{".", ".."} {
			t.Run("old_name="+name, func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mockQuerier := dbmock.NewMockQuerier(t)
				mgr := meta.New(mockStore)

				setupMockWriteTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

				err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: name, NewParentIno: oldParentIno, NewName: "b"})
				assert.ErrorIs(t, err, meta.ErrInvalidArgument)
			})
			t.Run("new_name="+name, func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mockQuerier := dbmock.NewMockQuerier(t)
				mgr := meta.New(mockStore)

				setupMockWriteTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

				err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "a", NewParentIno: oldParentIno, NewName: name})
				assert.ErrorIs(t, err, meta.ErrInvalidArgument)
			})
		}
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db connection failure")
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(db.Inode{}, dbErr).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: "a", NewParentIno: oldParentIno, NewName: "b"})
		assert.ErrorIs(t, err, dbErr)
	})
}

