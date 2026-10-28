package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
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

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestRename_InputValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	testCases := []struct {
		name        string
		params      meta.RenameParams
		expectedErr error
	}{
		{name: "old_parent_ino 为 0", params: meta.RenameParams{OldParentIno: 0, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("b")}, expectedErr: meta.ErrInvalidArgument},
		{name: "old_parent_ino 超过 math.MaxInt64", params: meta.RenameParams{OldParentIno: uint64(math.MaxInt64) + 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("b")}, expectedErr: meta.ErrInvalidArgument},
		{name: "new_parent_ino 为 0", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: 0, NewName: []byte("b")}, expectedErr: meta.ErrInvalidArgument},
		{name: "new_parent_ino 超过 math.MaxInt64", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: uint64(math.MaxInt64) + 1, NewName: []byte("b")}, expectedErr: meta.ErrInvalidArgument},
		{name: "old_name 超过 255 字节", params: meta.RenameParams{OldParentIno: 1, OldName: []byte(strings.Repeat("a", 256)), NewParentIno: 1, NewName: []byte("b")}, expectedErr: meta.ErrNameTooLong},
		{name: "old_name 包含 /", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("sub/a"), NewParentIno: 1, NewName: []byte("b")}, expectedErr: meta.ErrInvalidArgument},
		{name: "old_name 包含 \\x00", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a\x00b"), NewParentIno: 1, NewName: []byte("b")}, expectedErr: meta.ErrInvalidArgument},
		{name: "new_name 为空", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte{}}, expectedErr: meta.ErrInvalidArgument},
		{name: "new_name 超过 255 字节", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte(strings.Repeat("b", 256))}, expectedErr: meta.ErrNameTooLong},
		{name: "new_name 包含 /", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("sub/b")}, expectedErr: meta.ErrInvalidArgument},
		{name: "new_name 包含 \\x00", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("b\x00c")}, expectedErr: meta.ErrInvalidArgument},
		{name: "flags 互斥组合 RenameNoReplace | RenameExchange (0x3)", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("b"), Flags: meta.RenameNoReplace | meta.RenameExchange}, expectedErr: meta.ErrInvalidArgument},
		{name: "flags 为 RENAME_WHITEOUT (0x4) 返回 ErrNotSupported", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("b"), Flags: meta.RenameWhiteout}, expectedErr: meta.ErrNotSupported},
		{name: "flags 为未知标志位 (math.MaxUint32)", params: meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("b"), Flags: math.MaxUint32}, expectedErr: meta.ErrInvalidArgument},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := mgr.Rename(ctx, tc.params)
			assert.ErrorIs(t, err, tc.expectedErr)
		})
	}

	mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
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
				Name:      []byte("same.txt"),
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("same.txt"), Ino: 100}, nil).
			Twice()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      []byte("same.txt"),
			NewParentIno: parentIno,
			NewName:      []byte("same.txt"),
		})
		require.NoError(t, err)
	})

	t.Run("同节点硬链接互转短路 (POSIX No-op): 目标已存在且 old_ino == new_ino 无副作用返回", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		parentWithDisk := parentRow
		parentWithDisk.RootIno = 100

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentWithDisk, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(db.Inode{
				Ino:     100,
				Mode:    int32(meta.S_IFREG | 0o644),
				Nlink:   2,
				RootIno: 100,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("link1"),
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("link1"), Ino: 100}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("link2"),
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("link2"), Ino: 100}, nil).
			Twice()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      []byte("link1"),
			NewParentIno: parentIno,
			NewName:      []byte("link2"),
		})
		require.NoError(t, err)
	})

	t.Run("同节点硬链接跨虚拟磁盘检测: oldInode.RootIno 与 parent.RootIno 不一致返回 ErrCrossDevice", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		parentWithDisk := parentRow
		parentWithDisk.RootIno = 1

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentWithDisk, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(db.Inode{
				Ino:     100,
				Mode:    int32(meta.S_IFREG | 0o644),
				Nlink:   2,
				RootIno: 2, // 异盘 root_ino
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("link1"),
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("link1"), Ino: 100}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("link2"),
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("link2"), Ino: 100}, nil).
			Twice()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      []byte("link1"),
			NewParentIno: parentIno,
			NewName:      []byte("link2"),
		})
		require.ErrorIs(t, err, meta.ErrCrossDevice)
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

		// 1. 预读与持锁二次校验源目录项与目标目录项
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("a.txt")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("a.txt"), Ino: srcFileIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("b.txt")}).
			Return(db.Entry{}, pgx.ErrNoRows).Twice()

		// 2. 全局升序加锁: 10 -> 20 -> 100
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcFileIno).Return(srcFileRow, nil).Once()

		// 4. 更新目录项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("a.txt"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("b.txt"),
		}).Return(nil).Once()

		// 5. 刷新源节点 ctime
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcFileIno).Return(nil).Once()

		// 6. 刷新两个父目录时间戳
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("a.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("b.txt"),
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

		// 1. 预读与持锁二次校验目录项
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("a.txt")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("a.txt"), Ino: srcFileIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("b.txt")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("b.txt"), Ino: targetFileIno}, nil).Twice()

		// 2. 全局升序加锁: 10 -> 20 -> 100 -> 200
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcFileIno).Return(srcFileRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, targetFileIno).Return(targetFileRow, nil).Once()

		// 4. 删除原目标目录项并递减其 nlink
		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(newParentIno),
			Name:      []byte("b.txt"),
		}).Return(nil).Once()

		targetDecremented := targetFileRow
		targetDecremented.Nlink = 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetFileIno).
			Return(targetDecremented, nil).Once()

		// 5. 更新源项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("a.txt"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("b.txt"),
		}).Return(nil).Once()

		// 6. 刷新源 ctime
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcFileIno).Return(nil).Once()

		// 7. 刷新父目录时间戳
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("a.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("b.txt"),
		})
		require.NoError(t, err)
	})

	t.Run("跨目录移动非 UTF-8 原始字节序列名称文件", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		rawOldName := []byte{0xff, 0xfe, 0x01}
		rawNewName := []byte{0x80, 0x90, 0x02}

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: rawOldName}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: rawOldName, Ino: srcFileIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: rawNewName}).
			Return(db.Entry{}, pgx.ErrNoRows).Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcFileIno).Return(srcFileRow, nil).Once()

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      rawOldName,
			NewParentIno: int64(newParentIno),
			NewName:      rawNewName,
		}).Return(nil).Once()

		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcFileIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      rawOldName,
			NewParentIno: newParentIno,
			NewName:      rawNewName,
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

		// 1. 预读与持锁二次校验目录项
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("subdir")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("subdir"), Ino: srcDirIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("destdir")}).
			Return(db.Entry{}, pgx.ErrNoRows).Twice()

		// 2. 全局升序加锁
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcDirIno).Return(srcDirRow, nil).Once()

		// 4. 环检测: IsSubdirectory 返回 false
		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: srcDirIno,
		}).Return(false, nil).Once()

		// 5. 更新项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("subdir"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("destdir"),
		}).Return(nil).Once()

		// 6. 更新源目录 parent_ino 并刷新 ctime
		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       srcDirIno,
			ParentIno: int64(newParentIno),
		}).Return(nil).Once()

		// 7. oldParent nlink - 1, newParent nlink + 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().IncrementInodeNlinkAndTouch(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("subdir"),
			NewParentIno: newParentIno,
			NewName:      []byte("destdir"),
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

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("subdir")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("subdir"), Ino: srcDirIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("targetdir")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("targetdir"), Ino: targetDirIno}, nil).Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
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
			Name:      []byte("targetdir"),
		}).Return(nil).Once()

		// 目标目录置 nlink = 0
		orphanTarget := targetDirRow
		orphanTarget.Nlink = 0
		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetDirIno).Return(orphanTarget, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetDirIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetDirIno).Return(nil).Once()

		// 更新源项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("subdir"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("targetdir"),
		}).Return(nil).Once()

		// 更新源目录 parent_ino 并刷新 ctime
		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       srcDirIno,
			ParentIno: int64(newParentIno),
		}).Return(nil).Once()

		// oldParent nlink 减 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()

		// newParent 仅刷新时间戳 (nlink 抵消)
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("subdir"),
			NewParentIno: newParentIno,
			NewName:      []byte("targetdir"),
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

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir1")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir1"), Ino: srcDirIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir2")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir2"), Ino: targetDirIno}, nil).Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcDirIno).Return(srcDirRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, targetDirIno).Return(targetDirRow, nil).Once()

		mockQuerier.EXPECT().HasChildEntries(mock.Anything, targetDirIno).Return(false, nil).Once()

		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(oldParentIno),
			Name:      []byte("dir2"),
		}).Return(nil).Once()

		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetDirIno).Return(targetDirRow, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetDirIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetDirIno).Return(nil).Once()

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("dir1"),
			NewParentIno: int64(oldParentIno),
			NewName:      []byte("dir2"),
		}).Return(nil).Once()

		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcDirIno).Return(nil).Once()

		// 同父目录覆盖空目录: 父目录 nlink - 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("dir1"),
			NewParentIno: oldParentIno,
			NewName:      []byte("dir2"),
		})
		require.NoError(t, err)
	})

	t.Run("全局 Inode 升序加锁: 当旧子节点 ino 小于父目录 ino 时严格按全局 min->max 顺序加锁，消除跨层死锁", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		// 模拟较早创建的子目录 B(ino=10) 曾被移入较晚创建的父目录 A(ino=20)，现移往另一目录 C(ino=15)
		parentA := uint64(20)
		parentC := uint64(15)
		childB := int64(10)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(parentA), Name: []byte("b")}).
			Return(db.Entry{ParentIno: int64(parentA), Name: []byte("b"), Ino: childB}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(parentC), Name: []byte("b_moved")}).
			Return(db.Entry{}, pgx.ErrNoRows).Twice()

		var lockOrder []int64
		for _, ino := range []int64{childB, int64(parentC), int64(parentA)} {
			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, ino).
				RunAndReturn(func(_ context.Context, lockedIno int64) (db.Inode, error) {
					lockOrder = append(lockOrder, lockedIno)
					return db.Inode{Ino: lockedIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil
				}).
				Once()
		}

		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(parentC),
			AncestorIno: childB,
		}).Return(false, nil).Once()

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(parentA),
			OldName:      []byte("b"),
			NewParentIno: int64(parentC),
			NewName:      []byte("b_moved"),
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       childB,
			ParentIno: int64(parentC),
		}).Return(nil).Once()
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(parentA)).Return(nil).Once()
		mockQuerier.EXPECT().IncrementInodeNlinkAndTouch(mock.Anything, int64(parentC)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentA,
			OldName:      []byte("b"),
			NewParentIno: parentC,
			NewName:      []byte("b_moved"),
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{10, 15, 20}, lockOrder)
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

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("a")}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(db.Inode{}, pgx.ErrNoRows).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("a"), NewParentIno: oldParentIno, NewName: []byte("b")})
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("源父目录非目录返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		notDirParent := parentRow
		notDirParent.Mode = int32(meta.S_IFREG | 0o644)
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("a")}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(notDirParent, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("a"), NewParentIno: oldParentIno, NewName: []byte("b")})
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("源父目录为孤儿目录 (nlink == 0) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanParent := parentRow
		orphanParent.Nlink = 0
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("a")}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(orphanParent, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("a"), NewParentIno: oldParentIno, NewName: []byte("b")})
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("源项不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("missing")}).
			Return(db.Entry{}, pgx.ErrNoRows).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("missing"), NewParentIno: oldParentIno, NewName: []byte("b")})
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目录移动到自身返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirIno := int64(20) // new_parent_ino == old_ino == 20
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(10), Name: []byte("dir")}).
			Return(db.Entry{ParentIno: 10, Name: []byte("dir"), Ino: dirIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: dirIno, Name: []byte("dest")}).
			Return(db.Entry{}, pgx.ErrNoRows).Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(10)).Return(parentRow, nil).Once()
		dirRow := db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}
		// new_parent_ino 与 old_ino 同为 20，uniqueSortedInos 自动去重，仅锁定一次
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(dirRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: 10, OldName: []byte("dir"), NewParentIno: uint64(dirIno), NewName: []byte("dest")})
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("目录移动到其子孙目录返回 ErrInvalidArgument (环检测)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirIno := int64(30)
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(10), Name: []byte("dir")}).
			Return(db.Entry{ParentIno: 10, Name: []byte("dir"), Ino: dirIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dest")}).
			Return(db.Entry{}, pgx.ErrNoRows).Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(10)).Return(parentRow, nil).Once()
		newParent := parentRow
		newParent.Ino = int64(newParentIno)
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParent, nil).Once()

		dirRow := db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(dirRow, nil).Once()

		// 环检测命中: is_sub = true
		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: dirIno,
		}).Return(true, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: 10, OldName: []byte("dir"), NewParentIno: newParentIno, NewName: []byte("dest")})
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("源为目录覆盖非目录文件返回 ErrNotDir (POSIX ENOTDIR)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirIno := int64(100)
		fileIno := int64(200)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir"), Ino: dirIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("file")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("file"), Ino: fileIno}, nil).Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

		dirRow := db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}
		fileRow := db.Inode{Ino: fileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(dirRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, fileIno).Return(fileRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("dir"), NewParentIno: oldParentIno, NewName: []byte("file")})
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("源为非目录文件覆盖目录返回 ErrIsDir (POSIX EISDIR)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileIno := int64(100)
		dirIno := int64(200)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("file")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("file"), Ino: fileIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir"), Ino: dirIno}, nil).Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

		fileRow := db.Inode{Ino: fileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}
		dirRow := db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, fileIno).Return(fileRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(dirRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("file"), NewParentIno: oldParentIno, NewName: []byte("dir")})
		assert.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("源为目录覆盖非空目录返回 ErrNotEmpty (POSIX ENOTEMPTY)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dir1Ino := int64(100)
		dir2Ino := int64(200)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir1")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir1"), Ino: dir1Ino}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir2")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir2"), Ino: dir2Ino}, nil).Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

		dir1Row := db.Inode{Ino: dir1Ino, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}
		dir2Row := db.Inode{Ino: dir2Ino, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 3}

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dir1Ino).Return(dir1Row, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dir2Ino).Return(dir2Row, nil).Once()

		mockQuerier.EXPECT().HasChildEntries(mock.Anything, dir2Ino).Return(true, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("dir1"), NewParentIno: oldParentIno, NewName: []byte("dir2")})
		assert.ErrorIs(t, err, meta.ErrNotEmpty)
	})

	t.Run("old_name 或 new_name 为 . 或 .. 时在确认父目录有效后返回 ErrInvalidArgument", func(t *testing.T) {
		for _, name := range [][]byte{[]byte("."), []byte("..")} {
			t.Run("old_name="+string(name), func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mockQuerier := dbmock.NewMockQuerier(t)
				mgr := meta.New(mockStore)

				setupMockWriteTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

				err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: name, NewParentIno: oldParentIno, NewName: []byte("b")})
				assert.ErrorIs(t, err, meta.ErrInvalidArgument)
			})
			t.Run("new_name="+string(name), func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mockQuerier := dbmock.NewMockQuerier(t)
				mgr := meta.New(mockStore)

				setupMockWriteTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow, nil).Once()

				err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("a"), NewParentIno: oldParentIno, NewName: name})
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
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("a")}).
			Return(db.Entry{}, dbErr).Once()

		err := mgr.Rename(ctx, meta.RenameParams{OldParentIno: oldParentIno, OldName: []byte("a"), NewParentIno: oldParentIno, NewName: []byte("b")})
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestRename_GlobalLockOrderAndDriftRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("所有涉及的父目录与子节点按全局 Ino 严格升序加锁，且并发漂移时自动重试", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier1 := dbmock.NewMockQuerier(t)
		mockQuerier2 := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		// 刻意构造子节点 ino (5, 8) 小于父目录 ino (50, 30) 的反向拓扑
		oldParentIno := uint64(50)
		newParentIno := uint64(30)
		srcIno := int64(8)
		initialTargetIno := int64(5)
		driftedTargetIno := int64(6)

		// 第一次事务：预读到 target=5，按 5 -> 8 -> 30 -> 50 加锁后发现 target 漂移为 6，触发重试
		setupMockWriteTx(mockStore, mockQuerier1)
		mockQuerier1.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("src")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("src"), Ino: srcIno}, nil).Twice()
		mockQuerier1.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dst")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("dst"), Ino: initialTargetIno}, nil).Once()

		mockQuerier1.EXPECT().GetInodeForUpdate(mock.Anything, initialTargetIno).
			Return(db.Inode{Ino: initialTargetIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()
		mockQuerier1.EXPECT().GetInodeForUpdate(mock.Anything, srcIno).
			Return(db.Inode{Ino: srcIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()
		mockQuerier1.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).
			Return(db.Inode{Ino: int64(newParentIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).Once()
		mockQuerier1.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).
			Return(db.Inode{Ino: int64(oldParentIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).Once()

		mockQuerier1.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dst")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("dst"), Ino: driftedTargetIno}, nil).Once()

		// 第二次事务：预读到 target=6，严格按 6 -> 8 -> 30 -> 50 升序加锁并完成覆盖重命名
		setupMockWriteTx(mockStore, mockQuerier2)
		var lockOrder []int64

		mockQuerier2.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("src")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("src"), Ino: srcIno}, nil).Twice()
		mockQuerier2.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dst")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("dst"), Ino: driftedTargetIno}, nil).Twice()

		for _, ino := range []int64{driftedTargetIno, srcIno, int64(newParentIno), int64(oldParentIno)} {
			mode := int32(meta.S_IFREG | 0o644)
			nlink := int32(1)
			if ino == int64(newParentIno) || ino == int64(oldParentIno) {
				mode = int32(meta.S_IFDIR | 0o755)
				nlink = 2
			}
			mockQuerier2.EXPECT().GetInodeForUpdate(mock.Anything, ino).
				RunAndReturn(func(_ context.Context, lockedIno int64) (db.Inode, error) {
					lockOrder = append(lockOrder, lockedIno)
					return db.Inode{Ino: lockedIno, Mode: mode, Nlink: nlink}, nil
				}).Once()
		}

		mockQuerier2.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{ParentIno: int64(newParentIno), Name: []byte("dst")}).
			Return(nil).Once()
		mockQuerier2.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, driftedTargetIno).
			Return(db.Inode{Ino: driftedTargetIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 0}, nil).Once()
		mockQuerier2.EXPECT().HasFileHandlesByIno(mock.Anything, driftedTargetIno).Return(false, nil).Once()
		mockQuerier2.EXPECT().DeleteChunksByIno(mock.Anything, driftedTargetIno).Return(nil, nil).Once()
		mockQuerier2.EXPECT().DeleteInode(mock.Anything, driftedTargetIno).Return(nil).Once()
		mockQuerier2.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst"),
		}).Return(nil).Once()
		mockQuerier2.EXPECT().TouchInodeCtime(mock.Anything, srcIno).Return(nil).Once()
		mockQuerier2.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier2.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst"),
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{driftedTargetIno, srcIno, int64(newParentIno), int64(oldParentIno)}, lockOrder)
	})
}

func TestRename_NoReplace(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)
	newParentIno := uint64(20)
	parentRow := db.Inode{
		Ino:   int64(parentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
	}
	newParentRow := db.Inode{
		Ino:   int64(newParentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
	}

	t.Run("同路径存在时返回 ErrAlreadyExists (EEXIST) 而不短路成功", func(t *testing.T) {
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
				Name:      []byte("same.txt"),
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("same.txt"), Ino: 100}, nil).
			Twice()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      []byte("same.txt"),
			NewParentIno: parentIno,
			NewName:      []byte("same.txt"),
			Flags:        meta.RenameNoReplace,
		})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("同 Inode 不同硬链接已存在时返回 ErrAlreadyExists (EEXIST) 而不短路成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(db.Inode{
				Ino:     100,
				Mode:    int32(meta.S_IFREG | 0o644),
				Nlink:   2,
				RootIno: parentRow.RootIno,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("link1"),
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("link1"), Ino: 100}, nil).
			Twice()

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{
				ParentIno: int64(parentIno),
				Name:      []byte("link2"),
			}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("link2"), Ino: 100}, nil).
			Twice()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      []byte("link1"),
			NewParentIno: parentIno,
			NewName:      []byte("link2"),
			Flags:        meta.RenameNoReplace,
		})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("异 Inode 目标已存在时返回 ErrAlreadyExists (EEXIST)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		srcIno := int64(100)
		dstIno := int64(200)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(parentIno), Name: []byte("a.txt")}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("a.txt"), Ino: srcIno}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("b.txt")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("b.txt"), Ino: dstIno}, nil).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(parentIno)).Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcIno).Return(db.Inode{Ino: srcIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dstIno).Return(db.Inode{Ino: dstIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      []byte("a.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("b.txt"),
			Flags:        meta.RenameNoReplace,
		})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("目标不存在时正常完成移动", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		srcIno := int64(100)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(parentIno), Name: []byte("a.txt")}).
			Return(db.Entry{ParentIno: int64(parentIno), Name: []byte("a.txt"), Ino: srcIno}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("b.txt")}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(parentIno)).Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcIno).Return(db.Inode{Ino: srcIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(parentIno),
			OldName:      []byte("a.txt"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("b.txt"),
		}).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(parentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: parentIno,
			OldName:      []byte("a.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("b.txt"),
			Flags:        meta.RenameNoReplace,
		})
		require.NoError(t, err)
	})
}

func TestRename_Exchange(t *testing.T) {
	ctx := context.Background()
	oldParentIno := uint64(10)
	newParentIno := uint64(20)
	oldParentRow := db.Inode{
		Ino:   int64(oldParentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 3,
	}
	newParentRow := db.Inode{
		Ino:   int64(newParentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 3,
	}

	t.Run("源 Entry 不存在时返回 ErrNotFound (ENOENT)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("missing")}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("missing"),
			NewParentIno: newParentIno,
			NewName:      []byte("target"),
			Flags:        meta.RenameExchange,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标 Entry 不存在时在锁定校验新旧父目录后返回 ErrNotFound (ENOENT)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("src")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("src"), Ino: 100}, nil).
			Once()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("missing_dst")}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src"),
			NewParentIno: newParentIno,
			NewName:      []byte("missing_dst"),
			Flags:        meta.RenameExchange,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("同路径存在时无副作用短路返回成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("same")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("same"), Ino: 100}, nil).
			Twice()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("same"),
			NewParentIno: oldParentIno,
			NewName:      []byte("same"),
			Flags:        meta.RenameExchange,
		})
		require.NoError(t, err)
	})

	t.Run("同 Inode 不同硬链接均存在时无副作用短路返回成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("link1")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("link1"), Ino: 100}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("link2")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("link2"), Ino: 100}, nil).
			Twice()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(100)).Return(db.Inode{
			Ino:     100,
			Mode:    int32(meta.S_IFREG | 0o644),
			Nlink:   2,
			RootIno: oldParentRow.RootIno,
		}, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("link1"),
			NewParentIno: newParentIno,
			NewName:      []byte("link2"),
			Flags:        meta.RenameExchange,
		})
		require.NoError(t, err)
	})

	t.Run("正向祖先环检测: old_ino 为目录且 new_parent_ino 为其自身或子孙目录时返回 ErrInvalidArgument (EINVAL)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		oldDirIno := int64(100)
		newFileIno := int64(200)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir_a")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir_a"), Ino: oldDirIno}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("file_b")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("file_b"), Ino: newFileIno}, nil).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, oldDirIno).Return(db.Inode{Ino: oldDirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, newFileIno).Return(db.Inode{Ino: newFileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()

		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: oldDirIno,
		}).Return(true, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("dir_a"),
			NewParentIno: newParentIno,
			NewName:      []byte("file_b"),
			Flags:        meta.RenameExchange,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("反向祖先环检测: new_ino 为目录且 old_parent_ino 为其子孙目录时返回 ErrInvalidArgument (EINVAL)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		oldFileIno := int64(100)
		newDirIno := int64(200)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("file_a")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("file_a"), Ino: oldFileIno}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dir_b")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("dir_b"), Ino: newDirIno}, nil).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, oldFileIno).Return(db.Inode{Ino: oldFileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, newDirIno).Return(db.Inode{Ino: newDirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 3}, nil).Once()

		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(oldParentIno),
			AncestorIno: newDirIno,
		}).Return(true, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("file_a"),
			NewParentIno: newParentIno,
			NewName:      []byte("dir_b"),
			Flags:        meta.RenameExchange,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("反向祖先环检测: new_ino 为目录且 old_parent_ino 恰好等于 new_ino 时返回 ErrInvalidArgument (EINVAL)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		// old_parent_ino = 200, new_parent_ino = 10, old_ino = 300 (file inside 200), new_ino = 200 (dir 200 inside 10)
		oldFileIno := int64(300)
		newDirIno := int64(200)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: newDirIno, Name: []byte("child_file")}).
			Return(db.Entry{ParentIno: newDirIno, Name: []byte("child_file"), Ino: oldFileIno}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("parent_dir")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("parent_dir"), Ino: newDirIno}, nil).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, newDirIno).Return(db.Inode{Ino: newDirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, oldFileIno).Return(db.Inode{Ino: oldFileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: uint64(newDirIno),
			OldName:      []byte("child_file"),
			NewParentIno: oldParentIno,
			NewName:      []byte("parent_dir"),
			Flags:        meta.RenameExchange,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("同父目录下文件与非空目录原子互换: 原地互换 ino、双节点刷新 ctime、父目录 nlink 不变仅刷新时间戳", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileIno := int64(100)
		nonEmptyDirIno := int64(200)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("file")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("file"), Ino: fileIno}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("nonempty_dir")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("nonempty_dir"), Ino: nonEmptyDirIno}, nil).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, fileIno).Return(db.Inode{Ino: fileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, nonEmptyDirIno).Return(db.Inode{Ino: nonEmptyDirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 5}, nil).Once()

		mockQuerier.EXPECT().UpdateEntryIno(mock.Anything, db.UpdateEntryInoParams{
			ParentIno: int64(oldParentIno),
			Name:      []byte("file"),
			Ino:       nonEmptyDirIno,
		}).Return(nil).Once()
		mockQuerier.EXPECT().UpdateEntryIno(mock.Anything, db.UpdateEntryInoParams{
			ParentIno: int64(oldParentIno),
			Name:      []byte("nonempty_dir"),
			Ino:       fileIno,
		}).Return(nil).Once()

		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, fileIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, nonEmptyDirIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("file"),
			NewParentIno: oldParentIno,
			NewName:      []byte("nonempty_dir"),
			Flags:        meta.RenameExchange,
		})
		require.NoError(t, err)
	})

	t.Run("跨父目录非空目录与非空目录原子互换: 双向环检测通过，不查 HasChildEntries，双父目录 nlink 不变仅刷新时间戳", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dir1Ino := int64(100)
		dir2Ino := int64(200)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir1")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir1"), Ino: dir1Ino}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dir2")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("dir2"), Ino: dir2Ino}, nil).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dir1Ino).Return(db.Inode{Ino: dir1Ino, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 4}, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dir2Ino).Return(db.Inode{Ino: dir2Ino, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 5}, nil).Once()

		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: dir1Ino,
		}).Return(false, nil).Once()
		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(oldParentIno),
			AncestorIno: dir2Ino,
		}).Return(false, nil).Once()

		mockQuerier.EXPECT().UpdateEntryIno(mock.Anything, db.UpdateEntryInoParams{
			ParentIno: int64(oldParentIno),
			Name:      []byte("dir1"),
			Ino:       dir2Ino,
		}).Return(nil).Once()
		mockQuerier.EXPECT().UpdateEntryIno(mock.Anything, db.UpdateEntryInoParams{
			ParentIno: int64(newParentIno),
			Name:      []byte("dir2"),
			Ino:       dir1Ino,
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       dir1Ino,
			ParentIno: int64(newParentIno),
		}).Return(nil).Once()
		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       dir2Ino,
			ParentIno: int64(oldParentIno),
		}).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("dir1"),
			NewParentIno: newParentIno,
			NewName:      []byte("dir2"),
			Flags:        meta.RenameExchange,
		})
		require.NoError(t, err)
	})

	t.Run("跨父目录源为目录、目标为非目录互换: oldParent nlink 减 1, newParent nlink 加 1", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirIno := int64(100)
		fileIno := int64(200)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("dir")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("dir"), Ino: dirIno}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("file")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("file"), Ino: fileIno}, nil).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 3}, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, fileIno).Return(db.Inode{Ino: fileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()

		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: dirIno,
		}).Return(false, nil).Once()

		mockQuerier.EXPECT().UpdateEntryIno(mock.Anything, db.UpdateEntryInoParams{
			ParentIno: int64(oldParentIno),
			Name:      []byte("dir"),
			Ino:       fileIno,
		}).Return(nil).Once()
		mockQuerier.EXPECT().UpdateEntryIno(mock.Anything, db.UpdateEntryInoParams{
			ParentIno: int64(newParentIno),
			Name:      []byte("file"),
			Ino:       dirIno,
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       dirIno,
			ParentIno: int64(newParentIno),
		}).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, fileIno).Return(nil).Once()
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().IncrementInodeNlinkAndTouch(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("dir"),
			NewParentIno: newParentIno,
			NewName:      []byte("file"),
			Flags:        meta.RenameExchange,
		})
		require.NoError(t, err)
	})

	t.Run("跨父目录源为非目录、目标为目录互换: oldParent nlink 加 1, newParent nlink 减 1", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileIno := int64(100)
		dirIno := int64(200)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("file")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("file"), Ino: fileIno}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dir")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("dir"), Ino: dirIno}, nil).
			Twice()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newParentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, fileIno).Return(db.Inode{Ino: fileIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, dirIno).Return(db.Inode{Ino: dirIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 3}, nil).Once()

		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(oldParentIno),
			AncestorIno: dirIno,
		}).Return(false, nil).Once()

		mockQuerier.EXPECT().UpdateEntryIno(mock.Anything, db.UpdateEntryInoParams{
			ParentIno: int64(oldParentIno),
			Name:      []byte("file"),
			Ino:       dirIno,
		}).Return(nil).Once()
		mockQuerier.EXPECT().UpdateEntryIno(mock.Anything, db.UpdateEntryInoParams{
			ParentIno: int64(newParentIno),
			Name:      []byte("dir"),
			Ino:       fileIno,
		}).Return(nil).Once()

		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, fileIno).Return(nil).Once()
		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       dirIno,
			ParentIno: int64(oldParentIno),
		}).Return(nil).Once()
		mockQuerier.EXPECT().IncrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("file"),
			NewParentIno: newParentIno,
			NewName:      []byte("dir"),
			Flags:        meta.RenameExchange,
		})
		require.NoError(t, err)
	})
}

func TestRename_PostgresSemantics(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	rootDir, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	rootIno := uint64(rootDir.Ino)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM entries WHERE parent_ino = $1", rootDir.Ino)
		_ = store.DeleteInode(context.Background(), rootDir.Ino)
	}()

	// 1. 创建文件 a 与硬链接 a_link、文件 b
	fileA, err := mgr.Mknod(ctx, meta.MknodParams{
		ParentIno: rootIno,
		Name:      []byte("a.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() { _ = store.DeleteInode(context.Background(), int64(fileA.Ino)) }()

	_, err = mgr.Link(ctx, meta.LinkParams{
		Ino:          fileA.Ino,
		NewParentIno: rootIno,
		NewName:      []byte("a_link.txt"),
	})
	require.NoError(t, err)

	fileB, err := mgr.Mknod(ctx, meta.MknodParams{
		ParentIno: rootIno,
		Name:      []byte("b.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() { _ = store.DeleteInode(context.Background(), int64(fileB.Ino)) }()

	// 2. RenameNoReplace 拦截同路径、同 Inode 硬链接、异 Inode 已存在目标
	err = mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: rootIno,
		OldName:      []byte("a.txt"),
		NewParentIno: rootIno,
		NewName:      []byte("a.txt"),
		Flags:        meta.RenameNoReplace,
	})
	require.ErrorIs(t, err, meta.ErrAlreadyExists)

	err = mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: rootIno,
		OldName:      []byte("a.txt"),
		NewParentIno: rootIno,
		NewName:      []byte("a_link.txt"),
		Flags:        meta.RenameNoReplace,
	})
	require.ErrorIs(t, err, meta.ErrAlreadyExists)

	err = mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: rootIno,
		OldName:      []byte("a.txt"),
		NewParentIno: rootIno,
		NewName:      []byte("b.txt"),
		Flags:        meta.RenameNoReplace,
	})
	require.ErrorIs(t, err, meta.ErrAlreadyExists)

	// RenameNoReplace 移动到不存在的目标成功
	err = mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: rootIno,
		OldName:      []byte("b.txt"),
		NewParentIno: rootIno,
		NewName:      []byte("b_moved.txt"),
		Flags:        meta.RenameNoReplace,
	})
	require.NoError(t, err)

	// 3. RenameExchange 跨目录交换非空目录与普通文件，并验证双向祖先环检测
	dirP1, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: rootIno,
		Name:      []byte("p1"),
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM entries WHERE parent_ino = $1", int64(dirP1.Ino))
		_ = store.DeleteInode(context.Background(), int64(dirP1.Ino))
	}()

	dirP2, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: rootIno,
		Name:      []byte("p2"),
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM entries WHERE parent_ino = $1", int64(dirP2.Ino))
		_ = store.DeleteInode(context.Background(), int64(dirP2.Ino))
	}()

	// 在 p1 下创建非空目录 sub_dir (内部含 inner.txt)
	subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: dirP1.Ino,
		Name:      []byte("sub_dir"),
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM entries WHERE parent_ino = $1", int64(subDir.Ino))
		_ = store.DeleteInode(context.Background(), int64(subDir.Ino))
	}()

	innerFile, err := mgr.Mknod(ctx, meta.MknodParams{
		ParentIno: subDir.Ino,
		Name:      []byte("inner.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() { _ = store.DeleteInode(context.Background(), int64(innerFile.Ino)) }()

	// 在 p2 下创建普通文件 reg.txt
	regFile, err := mgr.Mknod(ctx, meta.MknodParams{
		ParentIno: dirP2.Ino,
		Name:      []byte("reg.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	defer func() { _ = store.DeleteInode(context.Background(), int64(regFile.Ino)) }()

	// 正向祖先环检测：将 p1/sub_dir 与 sub_dir/inner.txt 交换 -> ErrInvalidArgument
	err = mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: dirP1.Ino,
		OldName:      []byte("sub_dir"),
		NewParentIno: subDir.Ino,
		NewName:      []byte("inner.txt"),
		Flags:        meta.RenameExchange,
	})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)

	// 反向祖先环检测：将 sub_dir/inner.txt 与 p1/sub_dir 交换 -> ErrInvalidArgument
	err = mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: subDir.Ino,
		OldName:      []byte("inner.txt"),
		NewParentIno: dirP1.Ino,
		NewName:      []byte("sub_dir"),
		Flags:        meta.RenameExchange,
	})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)

	// 跨父目录交换 p1/sub_dir (非空目录, nlink=2) 与 p2/reg.txt (文件, nlink=1)
	// 交换前：p1.nlink == 3, p2.nlink == 2
	p1Before, err := mgr.GetAttr(ctx, dirP1.Ino)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), p1Before.Attr.Nlink)

	p2Before, err := mgr.GetAttr(ctx, dirP2.Ino)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), p2Before.Attr.Nlink)

	err = mgr.Rename(ctx, meta.RenameParams{
		OldParentIno: dirP1.Ino,
		OldName:      []byte("sub_dir"),
		NewParentIno: dirP2.Ino,
		NewName:      []byte("reg.txt"),
		Flags:        meta.RenameExchange,
	})
	require.NoError(t, err)

	// 交换后：p1/sub_dir 指向 regFile.Ino，p2/reg.txt 指向 subDir.Ino
	lookupP1Sub, err := mgr.Lookup(ctx, dirP1.Ino, []byte("sub_dir"))
	require.NoError(t, err)
	assert.Equal(t, regFile.Ino, lookupP1Sub.Ino)

	lookupP2Reg, err := mgr.Lookup(ctx, dirP2.Ino, []byte("reg.txt"))
	require.NoError(t, err)
	assert.Equal(t, subDir.Ino, lookupP2Reg.Ino)

	// 交换后：p1.nlink == 2 (减 1)，p2.nlink == 3 (加 1)
	p1After, err := mgr.GetAttr(ctx, dirP1.Ino)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), p1After.Attr.Nlink)

	p2After, err := mgr.GetAttr(ctx, dirP2.Ino)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), p2After.Attr.Nlink)
}

func TestRename_EmptyOldNameAndMaxNlink(t *testing.T) {
	ctx := context.Background()

	t.Run("old_name 为空时先加锁校验父目录再返回 ErrNotFound 或 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		// 1. 父目录有效时返回 ErrNotFound
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).
			Once()
		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: 1,
			OldName:      []byte{},
			NewParentIno: 1,
			NewName:      []byte("dst"),
		})
		assert.ErrorIs(t, err, meta.ErrNotFound)

		// 2. 父节点非目录时返回 ErrNotDir
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(2)).
			Return(db.Inode{Ino: 2, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()
		err = mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: 2,
			OldName:      []byte{},
			NewParentIno: 2,
			NewName:      []byte("dst"),
		})
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("跨父目录移动目录且目标父目录 nlink 已达 MaxNlink 时返回 ErrTooManyLinks", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: 10, Name: []byte("src_dir")}).
			Return(db.Entry{ParentIno: 10, Name: []byte("src_dir"), Ino: 30}, nil).
			Twice()
		mockQuerier.EXPECT().
			GetEntry(mock.Anything, db.GetEntryParams{ParentIno: 20, Name: []byte("dst_dir")}).
			Return(db.Entry{}, pgx.ErrNoRows).
			Twice()

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{Ino: 10, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 3}, nil).
			Once()
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(20)).
			Return(db.Inode{Ino: 20, Mode: int32(meta.S_IFDIR | 0o755), Nlink: int32(meta.MaxNlink)}, nil).
			Once()
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(30)).
			Return(db.Inode{Ino: 30, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, ParentIno: 10}, nil).
			Once()

		mockQuerier.EXPECT().
			IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{ChildIno: 20, AncestorIno: 30}).
			Return(false, nil).
			Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: 10,
			OldName:      []byte("src_dir"),
			NewParentIno: 20,
			NewName:      []byte("dst_dir"),
		})
		assert.ErrorIs(t, err, meta.ErrTooManyLinks)
	})
}

func TestRename_Overwrite_QuotaAndLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	oldParentIno := uint64(10)
	newParentIno := uint64(20)
	srcIno := int64(100)
	targetIno := int64(200)
	rootIno := int64(1)

	oldParentRow := db.Inode{
		Ino:     int64(oldParentIno),
		Mode:    int32(meta.S_IFDIR | 0o755),
		Nlink:   2,
		RootIno: rootIno,
		Atime:   pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:   pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:   pgtype.Timestamptz{Time: now, Valid: true},
	}
	newParentRow := db.Inode{
		Ino:     int64(newParentIno),
		Mode:    int32(meta.S_IFDIR | 0o755),
		Nlink:   2,
		RootIno: rootIno,
		Atime:   pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:   pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:   pgtype.Timestamptz{Time: now, Valid: true},
	}
	srcFileRow := db.Inode{
		Ino:     srcIno,
		Mode:    int32(meta.S_IFREG | 0o644),
		Nlink:   1,
		RootIno: rootIno,
		Size:    1024,
	}

	setupFileOverwriteBaseMocks := func(mockQuerier *dbmock.MockQuerier, targetRow db.Inode) {
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("src.txt")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("src.txt"), Ino: srcIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dst.txt")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("dst.txt"), Ino: targetIno}, nil).Twice()

		oldP := oldParentRow
		newP := newParentRow
		srcF := srcFileRow
		rootIno := targetRow.RootIno
		oldP.RootIno = rootIno
		newP.RootIno = rootIno
		srcF.RootIno = rootIno

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldP, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newP, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcIno).Return(srcF, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, targetIno).Return(targetRow, nil).Once()

		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(newParentIno),
			Name:      []byte("dst.txt"),
		}).Return(nil).Once()
	}

	t.Run("Rename 覆盖普通非空文件 (nlink 从 1 降为 0 且无活跃句柄): 当场调用 DeleteInode 并退还 used_bytes 与 used_inodes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetRow := db.Inode{
			Ino:     targetIno,
			Mode:    int32(meta.S_IFREG | 0o644),
			Nlink:   1,
			RootIno: rootIno,
			Size:    4096,
		}

		setupMockWriteTx(mockStore, mockQuerier)
		setupFileOverwriteBaseMocks(mockQuerier, targetRow)

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetIno).
			Return(targetDecremented, nil).Once()

		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteChunksByIno(mock.Anything, targetIno).Return(nil, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(nil).Once()
		mockQuerier.EXPECT().AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
			RootIno:     rootIno,
			DeltaBytes:  -4096,
			DeltaInodes: -1,
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src.txt"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst.txt"),
		}).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst.txt"),
		})
		require.NoError(t, err)
	})

	t.Run("Rename 覆盖符号链接 (nlink 从 1 降为 0 且无活跃句柄): 当场物理删除 Inode 且退还 DeltaBytes: 0, DeltaInodes: -1", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetRow := db.Inode{
			Ino:     targetIno,
			Mode:    int32(meta.S_IFLNK | 0o777),
			Nlink:   1,
			RootIno: rootIno,
			Size:    24, // 符号链接 target 路径长度，不应作为 deltaBytes 扣减
		}

		setupMockWriteTx(mockStore, mockQuerier)
		setupFileOverwriteBaseMocks(mockQuerier, targetRow)

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetIno).
			Return(targetDecremented, nil).Once()

		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(nil).Once()
		mockQuerier.EXPECT().AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
			RootIno:     rootIno,
			DeltaBytes:  0,
			DeltaInodes: -1,
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src.txt"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst.txt"),
		}).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst.txt"),
		})
		require.NoError(t, err)
	})

	t.Run("Rename 覆盖多硬链接文件 (nlink 从 2 降为 1): 不调用 HasFileHandlesByIno、DeleteInode 和 AddDiskQuotaByRootIno", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetRow := db.Inode{
			Ino:     targetIno,
			Mode:    int32(meta.S_IFREG | 0o644),
			Nlink:   2,
			RootIno: rootIno,
			Size:    4096,
		}

		setupMockWriteTx(mockStore, mockQuerier)
		setupFileOverwriteBaseMocks(mockQuerier, targetRow)

		targetDecremented := targetRow
		targetDecremented.Nlink = 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetIno).
			Return(targetDecremented, nil).Once()

		// 不应调用 HasFileHandlesByIno、DeleteInode、AddDiskQuotaByRootIno

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src.txt"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst.txt"),
		}).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst.txt"),
		})
		require.NoError(t, err)
	})

	t.Run("Rename 覆盖正在打开中的文件 (nlink 降为 0 但 HasFileHandlesByIno 返回 true): 保留孤儿节点，不调用 DeleteInode 和 AddDiskQuotaByRootIno", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetRow := db.Inode{
			Ino:     targetIno,
			Mode:    int32(meta.S_IFREG | 0o644),
			Nlink:   1,
			RootIno: rootIno,
			Size:    4096,
		}

		setupMockWriteTx(mockStore, mockQuerier)
		setupFileOverwriteBaseMocks(mockQuerier, targetRow)

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetIno).
			Return(targetDecremented, nil).Once()

		// 存在活跃打开句柄
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(true, nil).Once()

		// 不应调用 DeleteInode 与 AddDiskQuotaByRootIno

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src.txt"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst.txt"),
		}).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst.txt"),
		})
		require.NoError(t, err)
	})

	t.Run("Rename 覆盖无活跃句柄但 target.RootIno == 0 的文件: 物理删除 Inode 但不退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetRow := db.Inode{
			Ino:     targetIno,
			Mode:    int32(meta.S_IFREG | 0o644),
			Nlink:   1,
			RootIno: 0,
			Size:    4096,
		}

		setupMockWriteTx(mockStore, mockQuerier)
		setupFileOverwriteBaseMocks(mockQuerier, targetRow)

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetIno).
			Return(targetDecremented, nil).Once()

		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteChunksByIno(mock.Anything, targetIno).Return(nil, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(nil).Once()

		// 不应调用 AddDiskQuotaByRootIno

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src.txt"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst.txt"),
		}).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, srcIno).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst.txt"),
		})
		require.NoError(t, err)
	})

	setupDirOverwriteBaseMocks := func(mockQuerier *dbmock.MockQuerier, srcDirRow db.Inode, targetDirRow db.Inode) {
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: []byte("src_dir")}).
			Return(db.Entry{ParentIno: int64(oldParentIno), Name: []byte("src_dir"), Ino: srcIno}, nil).Twice()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: []byte("dst_dir")}).
			Return(db.Entry{ParentIno: int64(newParentIno), Name: []byte("dst_dir"), Ino: targetIno}, nil).Twice()

		oldP := oldParentRow
		newP := newParentRow
		rootIno := srcDirRow.RootIno
		oldP.RootIno = rootIno
		newP.RootIno = rootIno

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(oldP, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(newP, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, srcIno).Return(srcDirRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, targetIno).Return(targetDirRow, nil).Once()

		// 祖先环检测
		mockQuerier.EXPECT().IsSubdirectory(mock.Anything, db.IsSubdirectoryParams{
			ChildIno:    int64(newParentIno),
			AncestorIno: srcIno,
		}).Return(false, nil).Once()

		// 空目录校验
		mockQuerier.EXPECT().HasChildEntries(mock.Anything, targetIno).Return(false, nil).Once()

		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(newParentIno),
			Name:      []byte("dst_dir"),
		}).Return(nil).Once()
	}

	t.Run("Rename 覆盖空目录 (无活跃句柄且 RootIno > 0): 当场物理删除并退还 DeltaInodes: -1", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		srcDirRow := db.Inode{
			Ino:       srcIno,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			RootIno:   rootIno,
			ParentIno: int64(oldParentIno),
		}
		targetDirRow := db.Inode{
			Ino:       targetIno,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			RootIno:   rootIno,
			ParentIno: int64(newParentIno),
		}

		setupMockWriteTx(mockStore, mockQuerier)
		setupDirOverwriteBaseMocks(mockQuerier, srcDirRow, targetDirRow)

		orphanTarget := targetDirRow
		orphanTarget.Nlink = 0
		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(orphanTarget, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(nil).Once()
		mockQuerier.EXPECT().AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
			RootIno:     rootIno,
			DeltaBytes:  0,
			DeltaInodes: -1,
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src_dir"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst_dir"),
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       srcIno,
			ParentIno: int64(newParentIno),
		}).Return(nil).Once()

		// 跨父目录移动目录且目标覆盖空目录：oldParent nlink 减 1，newParent 时间戳刷新
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src_dir"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst_dir"),
		})
		require.NoError(t, err)
	})

	t.Run("Rename 覆盖空目录 (存在活跃句柄): 保留孤儿目录，不调用 DeleteInode 和 AddDiskQuotaByRootIno", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		srcDirRow := db.Inode{
			Ino:       srcIno,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			RootIno:   rootIno,
			ParentIno: int64(oldParentIno),
		}
		targetDirRow := db.Inode{
			Ino:       targetIno,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			RootIno:   rootIno,
			ParentIno: int64(newParentIno),
		}

		setupMockWriteTx(mockStore, mockQuerier)
		setupDirOverwriteBaseMocks(mockQuerier, srcDirRow, targetDirRow)

		orphanTarget := targetDirRow
		orphanTarget.Nlink = 0
		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(orphanTarget, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(true, nil).Once()

		// 不应调用 DeleteInode 与 AddDiskQuotaByRootIno

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src_dir"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst_dir"),
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       srcIno,
			ParentIno: int64(newParentIno),
		}).Return(nil).Once()

		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src_dir"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst_dir"),
		})
		require.NoError(t, err)
	})

	t.Run("Rename 覆盖空目录 (无活跃句柄但 target.RootIno == 0): 物理删除 Inode 但不退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		srcDirRow := db.Inode{
			Ino:       srcIno,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			RootIno:   0,
			ParentIno: int64(oldParentIno),
		}
		targetDirRow := db.Inode{
			Ino:       targetIno,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			RootIno:   0,
			ParentIno: int64(newParentIno),
		}

		setupMockWriteTx(mockStore, mockQuerier)
		setupDirOverwriteBaseMocks(mockQuerier, srcDirRow, targetDirRow)

		orphanTarget := targetDirRow
		orphanTarget.Nlink = 0
		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(orphanTarget, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(nil).Once()

		// 不应调用 AddDiskQuotaByRootIno

		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      []byte("src_dir"),
			NewParentIno: int64(newParentIno),
			NewName:      []byte("dst_dir"),
		}).Return(nil).Once()

		mockQuerier.EXPECT().UpdateInodeParentInoAndTouchCtime(mock.Anything, db.UpdateInodeParentInoAndTouchCtimeParams{
			Ino:       srcIno,
			ParentIno: int64(newParentIno),
		}).Return(nil).Once()

		mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src_dir"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst_dir"),
		})
		require.NoError(t, err)
	})

	// 错误路径测试
	t.Run("覆盖文件时 HasFileHandlesByIno 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetRow := db.Inode{Ino: targetIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1, RootIno: rootIno, Size: 4096}
		setupMockWriteTx(mockStore, mockQuerier)
		setupFileOverwriteBaseMocks(mockQuerier, targetRow)

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(targetDecremented, nil).Once()

		dbErr := errors.New("db handle check failed")
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, dbErr).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst.txt"),
		})
		assert.ErrorIs(t, err, dbErr)
	})

	t.Run("覆盖文件时 DeleteInode 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetRow := db.Inode{Ino: targetIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1, RootIno: rootIno, Size: 4096}
		setupMockWriteTx(mockStore, mockQuerier)
		setupFileOverwriteBaseMocks(mockQuerier, targetRow)

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(targetDecremented, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteChunksByIno(mock.Anything, targetIno).Return(nil, nil).Once()

		delErr := errors.New("delete inode failed")
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(delErr).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst.txt"),
		})
		assert.ErrorIs(t, err, delErr)
	})

	t.Run("覆盖文件时 AddDiskQuotaByRootIno 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		targetRow := db.Inode{Ino: targetIno, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1, RootIno: rootIno, Size: 4096}
		setupMockWriteTx(mockStore, mockQuerier)
		setupFileOverwriteBaseMocks(mockQuerier, targetRow)

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(targetDecremented, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteChunksByIno(mock.Anything, targetIno).Return(nil, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(nil).Once()

		quotaErr := errors.New("add disk quota failed")
		mockQuerier.EXPECT().AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
			RootIno:     rootIno,
			DeltaBytes:  -4096,
			DeltaInodes: -1,
		}).Return(quotaErr).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src.txt"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst.txt"),
		})
		assert.ErrorIs(t, err, quotaErr)
	})

	t.Run("覆盖空目录时 HasFileHandlesByIno 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		srcDirRow := db.Inode{Ino: srcIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, RootIno: rootIno, ParentIno: int64(oldParentIno)}
		targetDirRow := db.Inode{Ino: targetIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, RootIno: rootIno, ParentIno: int64(newParentIno)}
		setupMockWriteTx(mockStore, mockQuerier)
		setupDirOverwriteBaseMocks(mockQuerier, srcDirRow, targetDirRow)

		orphanTarget := targetDirRow
		orphanTarget.Nlink = 0
		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(orphanTarget, nil).Once()

		dbErr := errors.New("check dir handles failed")
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, dbErr).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src_dir"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst_dir"),
		})
		assert.ErrorIs(t, err, dbErr)
	})

	t.Run("覆盖空目录时 DeleteInode 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		srcDirRow := db.Inode{Ino: srcIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, RootIno: rootIno, ParentIno: int64(oldParentIno)}
		targetDirRow := db.Inode{Ino: targetIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, RootIno: rootIno, ParentIno: int64(newParentIno)}
		setupMockWriteTx(mockStore, mockQuerier)
		setupDirOverwriteBaseMocks(mockQuerier, srcDirRow, targetDirRow)

		orphanTarget := targetDirRow
		orphanTarget.Nlink = 0
		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(orphanTarget, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()

		delErr := errors.New("delete dir inode failed")
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(delErr).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src_dir"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst_dir"),
		})
		assert.ErrorIs(t, err, delErr)
	})

	t.Run("覆盖空目录时 AddDiskQuotaByRootIno 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		srcDirRow := db.Inode{Ino: srcIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, RootIno: rootIno, ParentIno: int64(oldParentIno)}
		targetDirRow := db.Inode{Ino: targetIno, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, RootIno: rootIno, ParentIno: int64(newParentIno)}
		setupMockWriteTx(mockStore, mockQuerier)
		setupDirOverwriteBaseMocks(mockQuerier, srcDirRow, targetDirRow)

		orphanTarget := targetDirRow
		orphanTarget.Nlink = 0
		mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(mock.Anything, targetIno).Return(orphanTarget, nil).Once()
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, targetIno).Return(false, nil).Once()
		mockQuerier.EXPECT().DeleteInode(mock.Anything, targetIno).Return(nil).Once()

		quotaErr := errors.New("quota refund failed")
		mockQuerier.EXPECT().AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
			RootIno:     rootIno,
			DeltaBytes:  0,
			DeltaInodes: -1,
		}).Return(quotaErr).Once()

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      []byte("src_dir"),
			NewParentIno: newParentIno,
			NewName:      []byte("dst_dir"),
		})
		assert.ErrorIs(t, err, quotaErr)
	})
}

// TestRename_ConcurrentCrossDirCycle_RealDB 验证 G-01：
// 跨父目录并发 Rename 会在无根节点拓扑排他锁时形成持久化孤立目录环，脱离根节点。
func TestRename_ConcurrentCrossDirCycle_RealDB(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	const rounds = 10

	for r := 0; r < rounds; r++ {
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-rename-cycle-%d-%d", r, time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)

		// 构建 /dirX/dirA/dirB 与 /dirY/dirC/dirD 拓扑
		dirX, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: disk.RootIno, Name: []byte("dirX"), Mode: 0o755})
		require.NoError(t, err)
		dirA, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirX.Ino, Name: []byte("dirA"), Mode: 0o755})
		require.NoError(t, err)
		dirB, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirA.Ino, Name: []byte("dirB"), Mode: 0o755})
		require.NoError(t, err)

		dirY, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: disk.RootIno, Name: []byte("dirY"), Mode: 0o755})
		require.NoError(t, err)
		dirC, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirY.Ino, Name: []byte("dirC"), Mode: 0o755})
		require.NoError(t, err)
		dirD, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirC.Ino, Name: []byte("dirD"), Mode: 0o755})
		require.NoError(t, err)

		var wg sync.WaitGroup
		startBarrier := make(chan struct{})
		var err1, err2 error

		// 为两个并发 goroutine 创建独立的 Manager 实例，绕开进程内单实例内存锁 crossDirRenameMu，
		// 覆盖多 Manager 实例在数据库事务层的真实并发交错。
		mgr1 := meta.New(store)
		mgr2 := meta.New(store)

		wg.Add(2)
		// T1: 把 dirA 移动到 dirD 下
		go func() {
			defer wg.Done()
			<-startBarrier
			err1 = mgr1.Rename(ctx, meta.RenameParams{
				OldParentIno: dirX.Ino,
				OldName:      []byte("dirA"),
				NewParentIno: dirD.Ino,
				NewName:      []byte("dirA"),
			})
		}()

		// T2: 把 dirC 移动到 dirB 下
		go func() {
			defer wg.Done()
			<-startBarrier
			err2 = mgr2.Rename(ctx, meta.RenameParams{
				OldParentIno: dirY.Ino,
				OldName:      []byte("dirC"),
				NewParentIno: dirB.Ino,
				NewName:      []byte("dirC"),
			})
		}()

		close(startBarrier)
		wg.Wait()

		// 断言并发两个 Rename 操作不应返回包含 deadlock detected / 40P01 的数据库底层死锁错误，
		// 且失败错误不应映射为 CodeInternal
		for _, err := range []error{err1, err2} {
			if err != nil {
				require.False(t, strings.Contains(strings.ToLower(err.Error()), "deadlock") || strings.Contains(err.Error(), "40P01"),
					"并发跨目录 Rename 不应触发 PostgreSQL 40P01 deadlock detected 死锁，实际返回: %v", err)
				st := meta.ToStatus(err)
				require.NotEqual(t, codes.Internal, st.Code(),
					"并发跨目录 Rename 不应因未处理死锁而返回 CodeInternal 内部错误，实际状态码: %s, 错误: %v", st.Code(), err)
			}
		}

		// 严格断言恰好 1 个成功、另 1 个失败且错误必须为 ErrInvalidArgument
		successCount := 0
		var failedErr error
		if err1 == nil {
			successCount++
		} else {
			failedErr = err1
		}
		if err2 == nil {
			successCount++
		} else {
			failedErr = err2
		}
		require.Equal(t, 1, successCount, "并发 Rename 必须恰好 1 个成功，err1: %v, err2: %v", err1, err2)
		require.ErrorIs(t, failedErr, meta.ErrInvalidArgument, "并发 Rename 失败分支必须返回 ErrInvalidArgument，实际: %v", failedErr)

		// 对 dirA.Ino、dirB.Ino、dirC.Ino、dirD.Ino 全部四个目录逐一沿 parent_ino 向上回溯验证
		dirsToCheck := []uint64{dirA.Ino, dirB.Ino, dirC.Ino, dirD.Ino}
		for _, targetIno := range dirsToCheck {
			curr := targetIno
			visited := make(map[uint64]bool)
			steps := 0
			const maxSteps = 100
			for {
				require.NotZero(t, curr, "目录 inode %d 在追溯过程中遇到了无效的 parent_ino 0", targetIno)
				if curr == disk.RootIno {
					break
				}
				require.False(t, visited[curr], "检测到环路: 节点 %d 已被访问过 (起点 %d)", curr, targetIno)
				visited[curr] = true

				steps++
				require.LessOrEqual(t, steps, maxSteps, "追溯超过最大深度 %d，可能存在死循环 (起点 %d)", maxSteps, targetIno)

				var pIno int64
				qErr := pool.QueryRow(ctx, "SELECT parent_ino FROM inodes WHERE ino = $1", curr).Scan(&pIno)
				require.NoError(t, qErr, "查询节点 %d 的 parent_ino 失败 (起点 %d)", curr, targetIno)
				curr = uint64(pIno)
			}
		}

		delErr := mgr.DeleteDisk(context.Background(), disk.DiskID)
		require.NoError(t, delErr, "清理磁盘失败")
	}
}

// TestRenameExchange_ConcurrentCrossDirCycle_RealDB 验证 G-01：
// 跨父目录并发 RenameExchange 在无根节点拓扑排他锁时形成持久化孤立目录环。
func TestRenameExchange_ConcurrentCrossDirCycle_RealDB(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	const rounds = 10

	for r := 0; r < rounds; r++ {
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-rename-ex-cycle-%d-%d", r, time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)

		// 构建 /dirX/dirA/dirB 与 /dirY/dirC/dirD 拓扑
		dirX, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: disk.RootIno, Name: []byte("dirX"), Mode: 0o755})
		require.NoError(t, err)
		dirA, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirX.Ino, Name: []byte("dirA"), Mode: 0o755})
		require.NoError(t, err)
		dirB, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirA.Ino, Name: []byte("dirB"), Mode: 0o755})
		require.NoError(t, err)

		dirY, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: disk.RootIno, Name: []byte("dirY"), Mode: 0o755})
		require.NoError(t, err)
		dirC, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirY.Ino, Name: []byte("dirC"), Mode: 0o755})
		require.NoError(t, err)
		dirD, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirC.Ino, Name: []byte("dirD"), Mode: 0o755})
		require.NoError(t, err)

		// 在 dirD 和 dirB 下分别创建占位目录
		phD, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirD.Ino, Name: []byte("phD"), Mode: 0o755})
		require.NoError(t, err)
		phB, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: dirB.Ino, Name: []byte("phB"), Mode: 0o755})
		require.NoError(t, err)
		_ = phD
		_ = phB

		var wg sync.WaitGroup
		startBarrier := make(chan struct{})
		var err1, err2 error

		// 为两个并发 goroutine 创建独立的 Manager 实例，绕开进程内单实例内存锁 crossDirRenameMu，
		// 覆盖多 Manager 实例在数据库事务层的真实并发交错。
		mgr1 := meta.New(store)
		mgr2 := meta.New(store)

		wg.Add(2)
		// T1: 交换 dirX/dirA 与 dirD/phD
		go func() {
			defer wg.Done()
			<-startBarrier
			err1 = mgr1.Rename(ctx, meta.RenameParams{
				OldParentIno: dirX.Ino,
				OldName:      []byte("dirA"),
				NewParentIno: dirD.Ino,
				NewName:      []byte("phD"),
				Flags:        meta.RenameExchange,
			})
		}()

		// T2: 交换 dirY/dirC 与 dirB/phB
		go func() {
			defer wg.Done()
			<-startBarrier
			err2 = mgr2.Rename(ctx, meta.RenameParams{
				OldParentIno: dirY.Ino,
				OldName:      []byte("dirC"),
				NewParentIno: dirB.Ino,
				NewName:      []byte("phB"),
				Flags:        meta.RenameExchange,
			})
		}()

		close(startBarrier)
		wg.Wait()

		// 断言并发两个 RenameExchange 操作不应返回包含 deadlock detected / 40P01 的数据库底层死锁错误，
		// 且失败错误不应映射为 CodeInternal
		for _, err := range []error{err1, err2} {
			if err != nil {
				require.False(t, strings.Contains(strings.ToLower(err.Error()), "deadlock") || strings.Contains(err.Error(), "40P01"),
					"并发跨目录 RenameExchange 不应触发 PostgreSQL 40P01 deadlock detected 死锁，实际返回: %v", err)
				st := meta.ToStatus(err)
				require.NotEqual(t, codes.Internal, st.Code(),
					"并发跨目录 RenameExchange 不应因未处理死锁而返回 CodeInternal 内部错误，实际状态码: %s, 错误: %v", st.Code(), err)
			}
		}

		// 严格断言恰好 1 个成功、另 1 个失败且错误必须为 ErrInvalidArgument
		successCount := 0
		var failedErr error
		if err1 == nil {
			successCount++
		} else {
			failedErr = err1
		}
		if err2 == nil {
			successCount++
		} else {
			failedErr = err2
		}
		require.Equal(t, 1, successCount, "并发 RenameExchange 必须恰好 1 个成功，err1: %v, err2: %v", err1, err2)
		require.ErrorIs(t, failedErr, meta.ErrInvalidArgument, "并发 RenameExchange 失败分支必须返回 ErrInvalidArgument，实际: %v", failedErr)

		// 对 dirA.Ino、dirB.Ino、dirC.Ino、dirD.Ino 全部四个目录逐一沿 parent_ino 向上回溯验证
		dirsToCheck := []uint64{dirA.Ino, dirB.Ino, dirC.Ino, dirD.Ino}
		for _, targetIno := range dirsToCheck {
			curr := targetIno
			visited := make(map[uint64]bool)
			steps := 0
			const maxSteps = 100
			for {
				require.NotZero(t, curr, "目录 inode %d 在追溯过程中遇到了无效的 parent_ino 0", targetIno)
				if curr == disk.RootIno {
					break
				}
				require.False(t, visited[curr], "检测到环路: 节点 %d 已被访问过 (起点 %d)", curr, targetIno)
				visited[curr] = true

				steps++
				require.LessOrEqual(t, steps, maxSteps, "追溯超过最大深度 %d，可能存在死循环 (起点 %d)", maxSteps, targetIno)

				var pIno int64
				qErr := pool.QueryRow(ctx, "SELECT parent_ino FROM inodes WHERE ino = $1", curr).Scan(&pIno)
				require.NoError(t, qErr, "查询节点 %d 的 parent_ino 失败 (起点 %d)", curr, targetIno)
				curr = uint64(pIno)
			}
		}

		delErr := mgr.DeleteDisk(context.Background(), disk.DiskID)
		require.NoError(t, delErr, "清理磁盘失败")
	}
}

type renameAncestorHookStore struct {
	db.Store
	onBeforeIsSubdirectory func(ctx context.Context, params db.IsSubdirectoryParams)
}

func (s *renameAncestorHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	err := s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		return fn(&renameAncestorHookQuerier{Querier: q, onBeforeIsSubdirectory: s.onBeforeIsSubdirectory})
	}, opts...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
			// 将底层捕获的 40P01 死锁暴露为非 retryable 错误，直接向上透传死锁失败，
			// 真实反映生产代码在 IsSubdirectory 逆序加锁引发的 deadlock detected
			return fmt.Errorf("transaction failed with deadlock: %s", err.Error())
		}
	}
	return err
}

type renameAncestorHookQuerier struct {
	db.Querier
	onBeforeIsSubdirectory func(ctx context.Context, params db.IsSubdirectoryParams)
}

func (q *renameAncestorHookQuerier) IsSubdirectory(ctx context.Context, params db.IsSubdirectoryParams) (bool, error) {
	if q.onBeforeIsSubdirectory != nil {
		q.onBeforeIsSubdirectory(ctx, params)
	}
	return q.Querier.IsSubdirectory(ctx, params)
}

// TestRename_AncestorLockOrder_DeadlockDetected_RealDB 验证 P-3：
// 跨目录 Rename 在 IsSubdirectory 逆序阻塞等待祖先 FOR SHARE 导致 40P01 死锁。
func TestRename_AncestorLockOrder_DeadlockDetected_RealDB(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	baseStore := db.NewStore(pool)
	mgrBase := meta.New(baseStore)

	disk, err := mgrBase.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-rename-ancestor-deadlock-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgrBase.DeleteDisk(context.Background(), disk.DiskID)
	}()

	// 构建目录拓扑：
	// /dirX/dirA/dirB
	// /dirY/dirC/dirD
	dirX, err := mgrBase.Mkdir(ctx, meta.MkdirParams{ParentIno: disk.RootIno, Name: []byte("dirX"), Mode: 0o755})
	require.NoError(t, err)
	dirA, err := mgrBase.Mkdir(ctx, meta.MkdirParams{ParentIno: dirX.Ino, Name: []byte("dirA"), Mode: 0o755})
	require.NoError(t, err)
	dirB, err := mgrBase.Mkdir(ctx, meta.MkdirParams{ParentIno: dirA.Ino, Name: []byte("dirB"), Mode: 0o755})
	require.NoError(t, err)

	dirY, err := mgrBase.Mkdir(ctx, meta.MkdirParams{ParentIno: disk.RootIno, Name: []byte("dirY"), Mode: 0o755})
	require.NoError(t, err)
	dirC, err := mgrBase.Mkdir(ctx, meta.MkdirParams{ParentIno: dirY.Ino, Name: []byte("dirC"), Mode: 0o755})
	require.NoError(t, err)
	dirD, err := mgrBase.Mkdir(ctx, meta.MkdirParams{ParentIno: dirC.Ino, Name: []byte("dirD"), Mode: 0o755})
	require.NoError(t, err)

	t1Arrived := make(chan struct{})
	t2Arrived := make(chan struct{})
	var syncOnce1, syncOnce2 sync.Once

	store1 := &renameAncestorHookStore{
		Store: baseStore,
		onBeforeIsSubdirectory: func(ctx context.Context, params db.IsSubdirectoryParams) {
			syncOnce1.Do(func() {
				close(t1Arrived)
				select {
				case <-t2Arrived:
				case <-ctx.Done():
				}
			})
		},
	}

	store2 := &renameAncestorHookStore{
		Store: baseStore,
		onBeforeIsSubdirectory: func(ctx context.Context, params db.IsSubdirectoryParams) {
			syncOnce2.Do(func() {
				close(t2Arrived)
				select {
				case <-t1Arrived:
				case <-ctx.Done():
				}
			})
		},
	}

	mgr1 := meta.New(store1)
	mgr2 := meta.New(store2)

	var wg sync.WaitGroup
	var err1, err2 error

	wg.Add(2)
	// T1: 移动 /dirX/dirA 到 /dirY/dirC/dirD/subA
	go func() {
		defer wg.Done()
		err1 = mgr1.Rename(ctx, meta.RenameParams{
			OldParentIno: dirX.Ino,
			OldName:      []byte("dirA"),
			NewParentIno: dirD.Ino,
			NewName:      []byte("subA"),
		})
	}()

	// T2: 移动 /dirY/dirC 到 /dirX/dirA/dirB/subC
	go func() {
		defer wg.Done()
		err2 = mgr2.Rename(ctx, meta.RenameParams{
			OldParentIno: dirY.Ino,
			OldName:      []byte("dirC"),
			NewParentIno: dirB.Ino,
			NewName:      []byte("subC"),
		})
	}()

	wg.Wait()

	// 1. 断言并发两个 Rename 操作不应返回包含 deadlock detected / 40P01 的数据库底层死锁错误，
	//    且失败错误不应映射为 CodeInternal
	for _, err := range []error{err1, err2} {
		if err != nil {
			require.False(t, strings.Contains(strings.ToLower(err.Error()), "deadlock") || strings.Contains(err.Error(), "40P01"),
				"并发跨目录 Rename 不应触发 PostgreSQL 40P01 deadlock detected 死锁，实际返回: %v", err)
			st := meta.ToStatus(err)
			require.NotEqual(t, codes.Internal, st.Code(),
				"并发跨目录 Rename 不应因未处理死锁而返回 CodeInternal 内部错误，实际状态码: %s, 错误: %v", st.Code(), err)
		}
	}

	// 2. 严格断言在理想无死锁并发机制下：恰好 1 个操作成功（nil），另 1 个因祖先环检测返回 ErrInvalidArgument
	successCount := 0
	var failedErr error
	if err1 == nil {
		successCount++
	} else {
		failedErr = err1
	}
	if err2 == nil {
		successCount++
	} else {
		failedErr = err2
	}
	require.Equal(t, 1, successCount, "并发 Rename 必须恰好 1 个成功，err1: %v, err2: %v", err1, err2)
	require.ErrorIs(t, failedErr, meta.ErrInvalidArgument, "并发 Rename 失败分支必须返回 ErrInvalidArgument，实际: %v", failedErr)

	// 3. 断言最终目录树绝不产生环路
	dirsToCheck := []uint64{dirA.Ino, dirB.Ino, dirC.Ino, dirD.Ino}
	for _, targetIno := range dirsToCheck {
		curr := targetIno
		visited := make(map[uint64]bool)
		steps := 0
		const maxSteps = 100
		for {
			require.NotZero(t, curr, "目录 inode %d 在追溯过程中遇到了无效的 parent_ino 0", targetIno)
			if curr == disk.RootIno {
				break
			}
			require.False(t, visited[curr], "检测到环路: 节点 %d 已被访问过 (起点 %d)", curr, targetIno)
			visited[curr] = true

			steps++
			require.LessOrEqual(t, steps, maxSteps, "追溯超过最大深度 %d，可能存在死循环 (起点 %d)", maxSteps, targetIno)

			var pIno int64
			qErr := pool.QueryRow(ctx, "SELECT parent_ino FROM inodes WHERE ino = $1", curr).Scan(&pIno)
			require.NoError(t, qErr, "查询节点 %d 的 parent_ino 失败 (起点 %d)", curr, targetIno)
			curr = uint64(pIno)
		}
	}
}
