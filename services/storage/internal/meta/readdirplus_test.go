package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
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
		assert.Equal(t, meta.DefaultBlockSize, entries[0].Inode.Attr.Blksize)
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
		assert.Equal(t, meta.DefaultBlockSize, entries[2].Inode.Attr.Blksize)
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

	t.Run("自定义缓存时效: 目录子项使用 dirEntryTimeout 而非目录子项使用 entryTimeout", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(
			mockStore,
			meta.WithEntryTimeout(2.5),
			meta.WithDirEntryTimeout(7.5),
			meta.WithAttrTimeout(4.5),
		)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		subDirRow := db.ListDirEntriesRow{
			Name:       []byte("subdir"),
			Ino:        205,
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       4096,
			Nlink:      2,
			Uid:        1000,
			Gid:        1000,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

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
		}).Return([]db.ListDirEntriesRow{childRow1, subDirRow}, nil).Once()

		entries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.NoError(t, err)
		require.Len(t, entries, 4)

		// . 与 .. 条目的 EntryTimeoutSeconds 固定为 0，AttrTimeoutSeconds 使用配置的 attrTimeout
		assert.Equal(t, 0.0, entries[0].EntryTimeoutSeconds)
		assert.Equal(t, 4.5, entries[0].AttrTimeoutSeconds)
		assert.Equal(t, 0.0, entries[0].Inode.EntryTimeoutSeconds)
		assert.Equal(t, 4.5, entries[0].Inode.AttrTimeoutSeconds)

		assert.Equal(t, 0.0, entries[1].EntryTimeoutSeconds)
		assert.Equal(t, 4.5, entries[1].AttrTimeoutSeconds)
		assert.Equal(t, 0.0, entries[1].Inode.EntryTimeoutSeconds)
		assert.Equal(t, 4.5, entries[1].Inode.AttrTimeoutSeconds)

		// 常规文件子项使用 entryTimeout (2.5) 与 attrTimeout (4.5)
		assert.Equal(t, 2.5, entries[2].EntryTimeoutSeconds)
		assert.Equal(t, 4.5, entries[2].AttrTimeoutSeconds)
		assert.Equal(t, 2.5, entries[2].Inode.EntryTimeoutSeconds)
		assert.Equal(t, 4.5, entries[2].Inode.AttrTimeoutSeconds)

		// 目录子项使用 dirEntryTimeout (7.5) 与 attrTimeout (4.5)
		assert.Equal(t, 7.5, entries[3].EntryTimeoutSeconds)
		assert.Equal(t, 4.5, entries[3].AttrTimeoutSeconds)
		assert.Equal(t, 7.5, entries[3].Inode.EntryTimeoutSeconds)
		assert.Equal(t, 4.5, entries[3].Inode.AttrTimeoutSeconds)
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

// TestReadDirPlus_P4_P5_ReproduceDefects 针对 P-4 与 P-5 缺陷提供 ReadDirPlus 的红测验证：
// P-4: 目录变更后续读第 3 页重置 dirMutated=false 导致切回偏移分页漏读条目 + 事务提交前提前污染游标缓存。
// P-5: 具名存活目录上的过期客户端会话被清理或句柄失效后，内存中 dirCursors 永不回收导致内存泄漏。
func TestReadDirPlus_P4_P5_ReproduceDefects(t *testing.T) {
	ctx := context.Background()

	t.Run("Subtest1_P4_DirMutatedResetLossOnThirdPage", func(t *testing.T) {
		dsn := getStorageTestDSN()

		testCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		pool, err := pgxpool.New(testCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(testCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		store := db.NewStore(pool)
		mgr := meta.New(store)

		disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdirplus-p4-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		}()

		dirP4, err := mgr.Mkdir(testCtx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dirP4"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		// 目录下创建 4 个文件: a, b, c, d
		for _, name := range []string{"a", "b", "c", "d"} {
			_, err := mgr.Mknod(testCtx, meta.MknodParams{
				ParentIno: dirP4.Ino,
				Name:      []byte(name),
				Mode:      meta.S_IFREG | 0o644,
			})
			require.NoError(t, err)
		}

		openRes, err := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:   dirP4.Ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.ReleaseDir(testCtx, meta.ReleaseDirParams{
				Ino: dirP4.Ino,
				Fh:  openRes.Fh,
			})
		}()

		// 第 1 页: offset=0, limit=3 -> 返回 ., .., a (末项 NextOffset=3)
		p1, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:    dirP4.Ino,
			Fh:     openRes.Fh,
			Offset: 0,
			Limit:  3,
		})
		require.NoError(t, err)
		require.Len(t, p1, 3)
		require.Equal(t, ".", string(p1[0].Name))
		require.Equal(t, "..", string(p1[1].Name))
		require.Equal(t, "a", string(p1[2].Name))
		require.Equal(t, uint64(3), p1[2].NextOffset)

		// 删除条目 a，触发 onDirEntryRemoved，将 offset=3 标记为 dirMutated=true
		err = mgr.Unlink(testCtx, dirP4.Ino, []byte("a"))
		require.NoError(t, err)

		// 第 2 页: offset=3, limit=2 续读 -> 返回 b (NextOffset=4), c (NextOffset=5)
		// 但当前实现错误将新生成的 offset=5 锚点的 dirMutated 设为了 false，且 sqlOffset=3
		p2, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:    dirP4.Ino,
			Fh:     openRes.Fh,
			Offset: 3,
			Limit:  2,
		})
		require.NoError(t, err)
		require.Len(t, p2, 2)
		require.Equal(t, "b", string(p2[0].Name))
		require.Equal(t, "c", string(p2[1].Name))
		require.Equal(t, uint64(5), p2[1].NextOffset)

		// 第 3 页: offset=5, limit=2 续读
		// 正确行为: 必须继续读取并返回剩余存活条目 d！
		// 缺陷行为: 因 dirMutated=false，切回 ListDirEntries(OFFSET 3)，此时目录仅剩 [b, c, d] 3项，
		// OFFSET 3 查出空列表，导致条目 d 被静默漏读！
		p3, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:    dirP4.Ino,
			Fh:     openRes.Fh,
			Offset: 5,
			Limit:  2,
		})
		require.NoError(t, err)

		var names3 []string
		for _, e := range p3 {
			names3 = append(names3, string(e.Name))
		}
		require.NotEmpty(t, p3, "P-4 缺陷复现：第 2 页 ReadDirPlus 后 dirMutated 被重置为 false 导致第 3 页切回 OFFSET 分页查空，漏读条目 d")
		require.Equal(t, []string{"d"}, names3, "第 3 页必须正确读出存活条目 d")
	})

	t.Run("Subtest2_P4_TxFailureShouldNotPolluteCursorCache", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		dirIno := int64(100)
		fh := int64(10)
		now := time.Now().Truncate(time.Microsecond)
		dirDBInode := db.Inode{
			Ino:   dirIno,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}
		activeHandle := db.FileHandle{
			Fh:        fh,
			Ino:       dirIno,
			Flags:     meta.O_RDONLY,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		childRow1 := db.ListDirEntriesRow{
			Name:  []byte("entry1"),
			Ino:   201,
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
		}

		require.Equal(t, 0, meta.DirCursorsLen(mgr), "初始状态下游标缓存必须为空")

		commitErr := errors.New("simulated tx commit failure")

		// 模拟事务在闭包执行期间读取成功，但最终提交失败 (ExecTxWithOptions 返回错误)
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				require.NoError(t, fn(mockQuerier))
				return commitErr
			}).Once()

		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{
			Fh:  fh,
			Ino: dirIno,
		}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, mock.Anything).Return([]db.ListDirEntriesRow{childRow1}, nil).Once()

		_, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 2,
			Limit:  1,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "readdirplus tx:")
		require.ErrorIs(t, err, commitErr)

		// 事务提交失败，闭包中途产生的游标绝不能泄露/污染进内存缓存
		require.Equal(t, 0, meta.DirCursorsLen(mgr), "P-4 缺陷复现：ReadDirPlus 事务提交失败后不得在内存中残留或新增游标")
	})

	t.Run("Subtest3_P5_BadFileHandleCleansOrphanCursors", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		dirIno := int64(100)
		fh := int64(10)
		now := time.Now().Truncate(time.Microsecond)
		dirDBInode := db.Inode{
			Ino:   dirIno,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}
		activeHandle := db.FileHandle{
			Fh:        fh,
			Ino:       dirIno,
			Flags:     meta.O_RDONLY,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		childRow1 := db.ListDirEntriesRow{
			Name:  []byte("entry1"),
			Ino:   201,
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
		}

		// 1. 首次正常读取，成功建立游标
		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{
			Fh:  fh,
			Ino: dirIno,
		}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, mock.Anything).Return([]db.ListDirEntriesRow{childRow1}, nil).Once()

		_, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 2,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Equal(t, 1, meta.InoDirCursorsCount(mgr, uint64(dirIno)), "成功读取后必须记录游标")

		// 2. 模拟句柄已失效 (GetFileHandle 返回 pgx.ErrNoRows)
		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{
			Fh:  fh,
			Ino: dirIno,
		}).Return(db.FileHandle{}, pgx.ErrNoRows).Once()

		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 3,
			Limit:  1,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 3. 句柄失效返回 ErrBadFileHandle 时，必须主动回收该句柄已残留在内存中的游标
		require.Equal(t, 0, meta.InoDirCursorsCount(mgr, uint64(dirIno)), "P-5 缺陷复现：句柄失效返回 ErrBadFileHandle 后必须清理该句柄关联的游标缓存，防止内存泄漏")
	})

	t.Run("Subtest4_P5_ExpiredSessionNamedDirCursorCleanup", func(t *testing.T) {
		dsn := getStorageTestDSN()

		testCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		pool, err := pgxpool.New(testCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(testCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		store := db.NewStore(pool)
		mgr := meta.New(store)

		disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdirplus-p5-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		}()

		dirP5, err := mgr.Mkdir(testCtx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dirP5"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		for _, name := range []string{"f1", "f2", "f3", "f4"} {
			_, err := mgr.Mknod(testCtx, meta.MknodParams{
				ParentIno: dirP5.Ino,
				Name:      []byte(name),
				Mode:      meta.S_IFREG | 0o644,
			})
			require.NoError(t, err)
		}

		// 客户端 session 打开具名目录
		sessID := uint64(time.Now().UnixNano() & 0x7FFFFFFF)
		openRes, err := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:       dirP5.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 翻页读取，生成游标
		p1, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dirP5.Ino,
			Fh:        openRes.Fh,
			Offset:    0,
			Limit:     3,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, p1)

		p2, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dirP5.Ino,
			Fh:        openRes.Fh,
			Offset:    p1[len(p1)-1].NextOffset,
			Limit:     2,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, p2)

		require.Greater(t, meta.InoDirCursorsCount(mgr, dirP5.Ino), 0, "ReadDirPlus 翻页后必须记录游标")

		// 模拟 session 过期：直接将 client_sessions 中的 expires_at 更新为过去时间
		_, err = pool.Exec(testCtx, "UPDATE client_sessions SET expires_at = $1 WHERE session_id = $2", time.Now().Add(-10*time.Second), int64(sessID))
		require.NoError(t, err)

		// 触发 RenewSession (使用另一个全新的 sessionID 触发后台过期会话清理)
		otherSessID := uint64(time.Now().UnixNano()&0x7FFFFFFF + 1)
		_, err = mgr.RenewSession(testCtx, otherSessID)
		require.NoError(t, err)

		// 具名目录（nlink >= 2）因非孤儿节点不会被 orphan 清理，但失效句柄关联的游标必须已被 RenewSession 主动释放
		require.Equal(t, 0, meta.InoDirCursorsCount(mgr, dirP5.Ino), "P-5 缺陷复现：具名目录在会话过期清理后，关联失效句柄的游标缓存必须已被主动释放，避免内存泄漏")

		// 此时已过期的 session 及其级联的 file_handles 均被物理删除
		// 客户端带着原已失效句柄再次尝试 ReadDirPlus 时，校验返回 ErrBadFileHandle
		_, err = mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dirP5.Ino,
			Fh:        openRes.Fh,
			Offset:    p2[len(p2)-1].NextOffset,
			Limit:     2,
			SessionID: &sessID,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})
}

type readDirPlusP24HookStore struct {
	db.Store
	pool              *pgxpool.Pool
	onGetPool         func()
	onWriteTxCommit   func()
	onReadDirTxCommit func()
}

func (s *readDirPlusP24HookStore) Pool() *pgxpool.Pool {
	if s.onGetPool != nil {
		s.onGetPool()
	}
	return s.pool
}

func (s *readDirPlusP24HookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	err := s.Store.ExecTxWithOptions(ctx, fn, opts...)
	if err == nil {
		if s.onWriteTxCommit != nil {
			cb := s.onWriteTxCommit
			s.onWriteTxCommit = nil
			cb()
		}
		if s.onReadDirTxCommit != nil {
			cb := s.onReadDirTxCommit
			s.onReadDirTxCommit = nil
			cb()
		}
	}
	return err
}

func TestReadDirPlus_P2_1_3_4_ReproduceDefects(t *testing.T) {
	ctx := context.Background()

	t.Run("Subtest1_P2_1_SinglePoolDeadlock", func(t *testing.T) {
		dsn := getStorageTestDSN()
		poolCfg, err := pgxpool.ParseConfig(dsn)
		require.NoError(t, err)
		poolCfg.MaxConns = 1

		testCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		singlePool, err := pgxpool.NewWithConfig(testCtx, poolCfg)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer singlePool.Close()

		if err := singlePool.Ping(testCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		store := db.NewStore(singlePool)
		mgr := meta.New(store)

		disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdirplus-p21-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		}()

		dir, err := mgr.Mkdir(testCtx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dirP21"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		_, err = mgr.Mknod(testCtx, meta.MknodParams{
			ParentIno: dir.Ino,
			Name:      []byte("item1"),
			Mode:      meta.S_IFREG | 0o644,
		})
		require.NoError(t, err)

		sessID := uint64(time.Now().UnixNano() & 0x7FFFFFFF)
		openRes, err := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:       dir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		entries, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dir.Ino,
			Fh:        openRes.Fh,
			Offset:    2,
			Limit:     1,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, entries)
		require.Greater(t, meta.InoDirCursorsCount(mgr, dir.Ino), 0)

		renewCtx, renewCancel := context.WithTimeout(testCtx, 1*time.Second)
		defer renewCancel()

		_, err = mgr.RenewSession(renewCtx, sessID)
		require.NoError(t, err, "P2-1: 单连接池下 RenewSession 不应因 cleanExpiredDirCursors 申请第二连接而自死锁")
	})

	t.Run("Subtest2_P2_3_CrossSessionHandleValidationClearsOwnerCursor", func(t *testing.T) {
		dsn := getStorageTestDSN()
		testCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		pool, err := pgxpool.New(testCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(testCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		store := db.NewStore(pool)
		mgr := meta.New(store)

		disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdirplus-p23-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		}()

		dir, err := mgr.Mkdir(testCtx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dirP23"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		_, err = mgr.Mknod(testCtx, meta.MknodParams{
			ParentIno: dir.Ino,
			Name:      []byte("a"),
			Mode:      meta.S_IFREG | 0o644,
		})
		require.NoError(t, err)

		_, err = mgr.Mknod(testCtx, meta.MknodParams{
			ParentIno: dir.Ino,
			Name:      []byte("b"),
			Mode:      meta.S_IFREG | 0o644,
		})
		require.NoError(t, err)

		sessA := uint64(time.Now().UnixNano() & 0x7FFFFFFF)
		openRes, err := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:       dir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessA,
		})
		require.NoError(t, err)

		p1, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dir.Ino,
			Fh:        openRes.Fh,
			Offset:    2,
			Limit:     1,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		require.Len(t, p1, 1)
		require.Equal(t, "a", string(p1[0].Name))
		nextOffset := p1[0].NextOffset

		require.Equal(t, 1, meta.InoDirCursorsCount(mgr, dir.Ino))

		err = mgr.Unlink(testCtx, dir.Ino, []byte("a"))
		require.NoError(t, err)

		sessB := sessA + 99999
		_, err = mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dir.Ino,
			Fh:        openRes.Fh,
			Offset:    2,
			Limit:     1,
			SessionID: &sessB,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "跨会话调用必须返回 ErrBadFileHandle")

		require.Equal(t, 1, meta.InoDirCursorsCount(mgr, dir.Ino), "P2-3: 跨会话调用失败不应清除合法会话的游标")

		p2, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dir.Ino,
			Fh:        openRes.Fh,
			Offset:    nextOffset,
			Limit:     10,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		require.NotEmpty(t, p2, "P2-3: 会话 A 后续分页读取必须能读取到存活条目 'b'")
		require.Equal(t, "b", string(p2[0].Name))
	})

	t.Run("Subtest3_P2_4_PathA_CleanExpiredCursorsDeletesConcurrentCursor", func(t *testing.T) {
		dsn := getStorageTestDSN()
		testCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		pool, err := pgxpool.New(testCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(testCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		baseStore := db.NewStore(pool)
		hookStore := &readDirPlusP24HookStore{
			Store: baseStore,
			pool:  pool,
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdirplus-p24a-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		}()

		dir, err := mgr.Mkdir(testCtx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dirP24A"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		_, err = mgr.Mknod(testCtx, meta.MknodParams{
			ParentIno: dir.Ino,
			Name:      []byte("file1"),
			Mode:      meta.S_IFREG | 0o644,
		})
		require.NoError(t, err)

		sessA := uint64(time.Now().UnixNano() & 0x7FFFFFFF)
		openResA, err := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:       dir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessA,
		})
		require.NoError(t, err)

		_, err = mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dir.Ino,
			Fh:        openResA.Fh,
			Offset:    2,
			Limit:     1,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		require.Equal(t, 1, meta.DirCursorsLen(mgr), "会话 A 建立游标")

		sessB := sessA + 1000
		openResB, err := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:       dir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessB,
		})
		require.NoError(t, err)

		// 在 cleanExpiredDirCursors 抓取快照并释放锁之后、发起 Query 查询之前，
		// 并发执行会话 B 的 ReadDirPlus 建立游标
		poolCallCount := 0
		hookStore.onGetPool = func() {
			poolCallCount++
			if poolCallCount == 2 {
				_, readErr := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
					Ino:       dir.Ino,
					Fh:        openResB.Fh,
					Offset:    2,
					Limit:     1,
					SessionID: &sessB,
				})
				require.NoError(t, readErr)
				require.Equal(t, 2, meta.DirCursorsLen(mgr), "会话 B 游标已记录在内存中")
			}
		}

		_, err = mgr.RenewSession(testCtx, sessA)
		require.NoError(t, err)

		// P2-4 路径 A 缺陷断言：
		// 在未修复前，cleanExpiredDirCursors 查库结果中只有快照里的 sessA，
		// 遍历游标时发现 sessB 不在查询结果中（!ok），将 sessB 的合法游标误删，导致游标数变成 1
		require.Equal(t, 2, meta.DirCursorsLen(mgr), "P2-4 路径 A: cleanExpiredDirCursors 不应误删并发新会话的有效游标")
	})

	t.Run("Subtest4_P2_4_PathB_UnlinkRaceDuringPendingCursorPublish", func(t *testing.T) {
		dsn := getStorageTestDSN()
		testCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		pool, err := pgxpool.New(testCtx, dsn)
		if err != nil {
			t.Skipf("skipping postgres test: %v", err)
		}
		defer pool.Close()

		if err := pool.Ping(testCtx); err != nil {
			t.Skipf("skipping postgres test (ping failed): %v", err)
		}

		baseStore := db.NewStore(pool)
		hookStore := &readDirPlusP24HookStore{
			Store: baseStore,
			pool:  pool,
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdirplus-p24b-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		}()

		dir, err := mgr.Mkdir(testCtx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dirP24B"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		_, err = mgr.Mknod(testCtx, meta.MknodParams{
			ParentIno: dir.Ino,
			Name:      []byte("a"),
			Mode:      meta.S_IFREG | 0o644,
		})
		require.NoError(t, err)

		_, err = mgr.Mknod(testCtx, meta.MknodParams{
			ParentIno: dir.Ino,
			Name:      []byte("b"),
			Mode:      meta.S_IFREG | 0o644,
		})
		require.NoError(t, err)

		sessA := uint64(time.Now().UnixNano() & 0x7FFFFFFF)
		openRes, err := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:       dir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessA,
		})
		require.NoError(t, err)

		hookStore.onReadDirTxCommit = func() {
			require.Equal(t, 0, meta.InoDirCursorsCount(mgr, dir.Ino), "此时游标尚未发布")
			unlinkErr := mgr.Unlink(testCtx, dir.Ino, []byte("a"))
			require.NoError(t, unlinkErr)
		}

		p1, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dir.Ino,
			Fh:        openRes.Fh,
			Offset:    2,
			Limit:     1,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		require.Len(t, p1, 1)
		require.Equal(t, "a", string(p1[0].Name))

		p2, err := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       dir.Ino,
			Fh:        openRes.Fh,
			Offset:    p1[0].NextOffset,
			Limit:     10,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		require.NotEmpty(t, p2, "P2-4 路径 B: 并发 Unlink 不应导致下一页漏读存活条目 'b'")
		require.Equal(t, "b", string(p2[0].Name))
	})
}
