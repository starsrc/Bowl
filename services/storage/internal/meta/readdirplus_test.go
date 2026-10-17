package meta_test

import (
	"context"
	"errors"
	"math"
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

func TestReadDirPlus_InputValidation(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		ino    uint64
		fh     uint64
		offset uint64
		limit  uint32
	}{
		{
			name:   "ino 为 0",
			ino:    0,
			fh:     1,
			offset: 0,
			limit:  10,
		},
		{
			name:   "ino 超过 math.MaxInt64",
			ino:    uint64(math.MaxInt64) + 1,
			fh:     1,
			offset: 0,
			limit:  10,
		},
		{
			name:   "fh 为 0",
			ino:    1,
			fh:     0,
			offset: 0,
			limit:  10,
		},
		{
			name:   "fh 超过 math.MaxInt64",
			ino:    1,
			fh:     uint64(math.MaxInt64) + 1,
			offset: 0,
			limit:  10,
		},
		{
			name:   "limit 为 0",
			ino:    1,
			fh:     1,
			offset: 0,
			limit:  0,
		},
		{
			name:   "limit 超过 MaxReadDirLimit (4096)",
			ino:    1,
			fh:     1,
			offset: 0,
			limit:  meta.MaxReadDirLimit + 1,
		},
		{
			name:   "offset 超过 math.MaxInt64",
			ino:    1,
			fh:     1,
			offset: uint64(math.MaxInt64) + 1,
			limit:  10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
				Ino:    tc.ino,
				Fh:     tc.fh,
				Offset: tc.offset,
				Limit:  tc.limit,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
			assert.Nil(t, entries)
		})
	}
}

func TestReadDirPlus_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	dirIno := int64(100)
	parentIno := int64(1)
	fh := int64(10)

	dirDBInode := db.Inode{
		Ino:        dirIno,
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

	parentDBInode := db.Inode{
		Ino:        parentIno,
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      3,
		Uid:        0,
		Gid:        0,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	childRow1 := db.ListDirEntriesRow{
		Name:       []byte("apple.txt"),
		Ino:        201,
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	childRow2 := db.ListDirEntriesRow{
		Name:       []byte("banana.txt"),
		Ino:        202,
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       2048,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	activeHandle := db.FileHandle{
		Fh:        fh,
		Ino:       dirIno,
		Flags:     meta.O_RDONLY,
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("首批完整读取: 合成 . 与 .. (entry_timeout=0) 并内联真实子项完整属性", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{
			Fh:  fh,
			Ino: dirIno,
		}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, dirIno).Return(parentDBInode, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     8,
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{childRow1, childRow2}, nil).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.NoError(t, err)
		require.Len(t, entries, 4)

		// 验证 . 条目
		assert.Equal(t, uint64(dirIno), entries[0].Ino)
		assert.Equal(t, []byte("."), entries[0].Name)
		assert.Equal(t, uint64(1), entries[0].NextOffset)
		assert.Equal(t, 0.0, entries[0].EntryTimeoutSeconds)
		assert.Equal(t, meta.DefaultAttrTimeoutSeconds, entries[0].AttrTimeoutSeconds)
		require.NotNil(t, entries[0].Inode)
		assert.Equal(t, uint64(dirIno), entries[0].Inode.Ino)
		assert.Equal(t, uint32(dirDBInode.Mode), entries[0].Inode.Attr.Mode)
		assert.Equal(t, uint32(4096), entries[0].Inode.Attr.Blksize)
		assert.Equal(t, uint64(8), entries[0].Inode.Attr.Blocks) // (4096+511)/512

		// 验证 .. 条目
		assert.Equal(t, uint64(parentIno), entries[1].Ino)
		assert.Equal(t, []byte(".."), entries[1].Name)
		assert.Equal(t, uint64(2), entries[1].NextOffset)
		assert.Equal(t, 0.0, entries[1].EntryTimeoutSeconds)
		assert.Equal(t, meta.DefaultAttrTimeoutSeconds, entries[1].AttrTimeoutSeconds)
		require.NotNil(t, entries[1].Inode)
		assert.Equal(t, uint64(parentIno), entries[1].Inode.Ino)
		assert.Equal(t, uint32(parentDBInode.Mode), entries[1].Inode.Attr.Mode)

		// 验证子项 1 (apple.txt)
		assert.Equal(t, uint64(childRow1.Ino), entries[2].Ino)
		assert.Equal(t, []byte("apple.txt"), entries[2].Name)
		assert.Equal(t, uint64(3), entries[2].NextOffset)
		assert.Equal(t, meta.DefaultEntryTimeoutSeconds, entries[2].EntryTimeoutSeconds)
		assert.Equal(t, meta.DefaultAttrTimeoutSeconds, entries[2].AttrTimeoutSeconds)
		require.NotNil(t, entries[2].Inode)
		assert.Equal(t, uint64(childRow1.Ino), entries[2].Inode.Ino)
		assert.Equal(t, uint32(childRow1.Mode), entries[2].Inode.Attr.Mode)
		assert.Equal(t, uint64(1024), entries[2].Inode.Attr.Size)
		assert.Equal(t, uint32(4096), entries[2].Inode.Attr.Blksize)
		assert.Equal(t, uint64(2), entries[2].Inode.Attr.Blocks) // (1024+511)/512

		// 验证子项 2 (banana.txt)
		assert.Equal(t, uint64(childRow2.Ino), entries[3].Ino)
		assert.Equal(t, []byte("banana.txt"), entries[3].Name)
		assert.Equal(t, uint64(4), entries[3].NextOffset)
		assert.Equal(t, meta.DefaultEntryTimeoutSeconds, entries[3].EntryTimeoutSeconds)
		assert.Equal(t, meta.DefaultAttrTimeoutSeconds, entries[3].AttrTimeoutSeconds)
		require.NotNil(t, entries[3].Inode)
		assert.Equal(t, uint64(childRow2.Ino), entries[3].Inode.Ino)
		assert.Equal(t, uint64(2048), entries[3].Inode.Attr.Size)
		assert.Equal(t, uint64(4), entries[3].Inode.Attr.Blocks) // (2048+511)/512
	})

	t.Run("多批次游标接续: limit=1 逐项读取与越界 EOF", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 5)

		// 批次 1: offset=0, limit=1 -> 返回 [.]
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		entries1, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries1, 1)
		assert.Equal(t, []byte("."), entries1[0].Name)
		assert.Equal(t, uint64(1), entries1[0].NextOffset)
		assert.Equal(t, 0.0, entries1[0].EntryTimeoutSeconds)

		// 批次 2: offset=1, limit=1 -> 返回 [..]
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, dirIno).Return(parentDBInode, nil).Once()
		entries2, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 1,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries2, 1)
		assert.Equal(t, []byte(".."), entries2[0].Name)
		assert.Equal(t, uint64(2), entries2[0].NextOffset)
		assert.Equal(t, 0.0, entries2[0].EntryTimeoutSeconds)

		// 批次 3: offset=2, limit=1 -> 返回 [apple.txt]
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     1,
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{childRow1}, nil).Once()
		entries3, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 2,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries3, 1)
		assert.Equal(t, []byte("apple.txt"), entries3[0].Name)
		assert.Equal(t, uint64(3), entries3[0].NextOffset)
		assert.Equal(t, meta.DefaultEntryTimeoutSeconds, entries3[0].EntryTimeoutSeconds)

		// 批次 4: offset=3, limit=1 -> 返回 [banana.txt]
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     1,
			Offset:    1,
		}).Return([]db.ListDirEntriesRow{childRow2}, nil).Once()
		entries4, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 3,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries4, 1)
		assert.Equal(t, []byte("banana.txt"), entries4[0].Name)
		assert.Equal(t, uint64(4), entries4[0].NextOffset)
		assert.Equal(t, meta.DefaultEntryTimeoutSeconds, entries4[0].EntryTimeoutSeconds)

		// 批次 5: offset=4, limit=10 -> EOF (返回空切片)
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     10,
			Offset:    2,
		}).Return([]db.ListDirEntriesRow{}, nil).Once()
		entries5, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 4,
			Limit:  10,
		})
		require.NoError(t, err)
		assert.Empty(t, entries5)
	})

	t.Run("跨边界游标接续: offset=1, limit=2 返回 .. 与首个真实子项", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, dirIno).Return(parentDBInode, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     1,
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{childRow1}, nil).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 1,
			Limit:  2,
		})
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, []byte(".."), entries[0].Name)
		assert.Equal(t, uint64(2), entries[0].NextOffset)
		assert.Equal(t, 0.0, entries[0].EntryTimeoutSeconds)
		assert.Equal(t, []byte("apple.txt"), entries[1].Name)
		assert.Equal(t, uint64(3), entries[1].NextOffset)
		assert.Equal(t, meta.DefaultEntryTimeoutSeconds, entries[1].EntryTimeoutSeconds)
	})

	t.Run("游标超过 math.MaxInt32 保持 64 位整型传递而不截断", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		largeOffset := uint64(math.MaxInt32) + 100
		expectedSQLOffset := int64(largeOffset - 2)

		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     1,
			Offset:    expectedSQLOffset,
		}).Return([]db.ListDirEntriesRow{childRow1}, nil).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: largeOffset,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, []byte("apple.txt"), entries[0].Name)
		assert.Equal(t, uint64(expectedSQLOffset)+3, entries[0].NextOffset)
	})

	t.Run("根目录 .. 回退: 无父目录时 .. 回退指向自身 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		rootIno := int64(1)
		rootDBInode := db.Inode{
			Ino:        rootIno,
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       4096,
			Nlink:      2,
			Uid:        0,
			Gid:        0,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().GetInode(mock.Anything, rootIno).Return(rootDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: rootIno}).Return(db.FileHandle{
			Fh:    fh,
			Ino:   rootIno,
			Flags: meta.O_RDONLY,
		}, nil).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, rootIno).Return(db.Inode{}, pgx.ErrNoRows).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: rootIno,
			Limit:     8,
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{}, nil).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(rootIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, []byte("."), entries[0].Name)
		assert.Equal(t, uint64(rootIno), entries[0].Ino)
		assert.Equal(t, []byte(".."), entries[1].Name)
		assert.Equal(t, uint64(rootIno), entries[1].Ino)
		assert.Equal(t, 0.0, entries[1].EntryTimeoutSeconds)
	})

	t.Run("非 UTF-8 目录项名称读取", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		rawName := []byte{0xff, 0xfe, 0xfd}
		nonUTF8Row := db.ListDirEntriesRow{
			Ino:        1001,
			Generation: 1,
			Mode:       int32(meta.S_IFREG | 0o644),
			Nlink:      1,
			Uid:        1000,
			Gid:        1000,
			Rdev:       0,
			Size:       128,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			Name:       rawName,
		}

		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     1,
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{nonUTF8Row}, nil).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 2,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, rawName, entries[0].Name)
		assert.Equal(t, uint64(3), entries[0].NextOffset)
	})

	t.Run("孤儿目录拦截: nlink == 0 时直接返回空切片", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		orphanDBInode := dirDBInode
		orphanDBInode.Nlink = 0

		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(orphanDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestReadDirPlus_InvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	dirIno := int64(100)
	fh := int64(10)

	t.Run("目录节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(db.Inode{}, pgx.ErrNoRows).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		assert.Nil(t, entries)
	})

	t.Run("节点非目录返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		regInode := db.Inode{
			Ino:   dirIno,
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
		}
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(regInode, nil).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotDir))
		assert.Nil(t, entries)
	})

	t.Run("目录句柄不存在或不匹配返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		dirInode := db.Inode{
			Ino:   dirIno,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(db.FileHandle{}, pgx.ErrNoRows).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
		assert.Nil(t, entries)
	})

	t.Run("底层数据库查询 GetInode 失败返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		expectedErr := errors.New("db connection failure")
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(db.Inode{}, expectedErr).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "readdirplus tx:")
		assert.Nil(t, entries)
	})

	t.Run("底层数据库查询 GetParentInode 失败返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		dirInode := db.Inode{
			Ino:   dirIno,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(db.FileHandle{Fh: fh, Ino: dirIno}, nil).Once()

		expectedErr := errors.New("parent query failure")
		mockQuerier.EXPECT().GetParentInode(mock.Anything, dirIno).Return(db.Inode{}, expectedErr).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "readdirplus tx:")
		assert.Nil(t, entries)
	})

	t.Run("底层数据库查询 ListDirEntries 失败返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		dirInode := db.Inode{
			Ino:   dirIno,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
			Atime: pgtype.Timestamptz{Time: now, Valid: true},
			Mtime: pgtype.Timestamptz{Time: now, Valid: true},
			Ctime: pgtype.Timestamptz{Time: now, Valid: true},
		}
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(db.FileHandle{Fh: fh, Ino: dirIno}, nil).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, dirIno).Return(dirInode, nil).Once()

		expectedErr := errors.New("list dir entries failure")
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     8,
			Offset:    0,
		}).Return(nil, expectedErr).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "readdirplus tx:")
		assert.Nil(t, entries)
	})
}
