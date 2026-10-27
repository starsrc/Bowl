package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
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

func TestReadDir_InputValidation(t *testing.T) {
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

			entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
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

func TestReadDir_PositivePath(t *testing.T) {
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

	t.Run("首批完整读取: 合成 . 与 .. 并按名称升序返回真实子项", func(t *testing.T) {
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
			Limit:     8, // limit 10 - 2 (for . and ..)
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{childRow1, childRow2}, nil).Once()

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.NoError(t, err)
		require.Len(t, entries, 4)

		assert.Equal(t, meta.DirEntry{
			Ino:        uint64(dirIno),
			Name:       []byte("."),
			Mode:       uint32(dirDBInode.Mode),
			NextOffset: 1,
		}, entries[0])

		assert.Equal(t, meta.DirEntry{
			Ino:        uint64(parentIno),
			Name:       []byte(".."),
			Mode:       uint32(parentDBInode.Mode),
			NextOffset: 2,
		}, entries[1])

		assert.Equal(t, meta.DirEntry{
			Ino:        uint64(childRow1.Ino),
			Name:       []byte("apple.txt"),
			Mode:       uint32(childRow1.Mode),
			NextOffset: 3,
		}, entries[2])

		assert.Equal(t, meta.DirEntry{
			Ino:        uint64(childRow2.Ino),
			Name:       []byte("banana.txt"),
			Mode:       uint32(childRow2.Mode),
			NextOffset: 4,
		}, entries[3])
	})

	t.Run("多批次游标接续: limit=1 逐项读取与越界 EOF", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 5)

		// 批次 1: offset=0, limit=1 -> 返回 [.]
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		entries1, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries1, 1)
		assert.Equal(t, []byte("."), entries1[0].Name)
		assert.Equal(t, uint64(1), entries1[0].NextOffset)

		// 批次 2: offset=1, limit=1 -> 返回 [..]
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, dirIno).Return(parentDBInode, nil).Once()
		entries2, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 1,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries2, 1)
		assert.Equal(t, []byte(".."), entries2[0].Name)
		assert.Equal(t, uint64(2), entries2[0].NextOffset)

		// 批次 3: offset=2, limit=1 -> 返回 [apple.txt]
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     1,
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{childRow1}, nil).Once()
		entries3, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 2,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries3, 1)
		assert.Equal(t, []byte("apple.txt"), entries3[0].Name)
		assert.Equal(t, uint64(3), entries3[0].NextOffset)

		// 批次 4: offset=3, limit=1 -> 返回 [banana.txt]
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     1,
			Offset:    1,
		}).Return([]db.ListDirEntriesRow{childRow2}, nil).Once()
		entries4, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 3,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries4, 1)
		assert.Equal(t, []byte("banana.txt"), entries4[0].Name)
		assert.Equal(t, uint64(4), entries4[0].NextOffset)

		// 批次 5: offset=4, limit=10 -> EOF (返回空切片)
		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     10,
			Offset:    2,
		}).Return([]db.ListDirEntriesRow{}, nil).Once()
		entries5, err := mgr.ReadDir(ctx, meta.ReadDirParams{
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 1,
			Limit:  2,
		})
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, []byte(".."), entries[0].Name)
		assert.Equal(t, uint64(2), entries[0].NextOffset)
		assert.Equal(t, []byte("apple.txt"), entries[1].Name)
		assert.Equal(t, uint64(3), entries[1].NextOffset)
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
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
		// 根目录在 entries 中无记录，GetParentInode 返回 pgx.ErrNoRows
		mockQuerier.EXPECT().GetParentInode(mock.Anything, rootIno).Return(db.Inode{}, pgx.ErrNoRows).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: rootIno,
			Limit:     8,
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{}, nil).Once()

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
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
		assert.Equal(t, uint64(rootIno), entries[1].Ino) // 回退指向根目录自身
	})

	t.Run("非 UTF-8 原始字节序列条目正常按字节返回", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		rawChildRow := db.ListDirEntriesRow{
			Name:       []byte{0x80, 0x81, 0xfe, 0xff},
			Ino:        203,
			Generation: 1,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       512,
			Nlink:      1,
		}

		mockQuerier.EXPECT().GetInode(mock.Anything, dirIno).Return(dirDBInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: fh, Ino: dirIno}).Return(activeHandle, nil).Once()
		mockQuerier.EXPECT().ListDirEntries(mock.Anything, db.ListDirEntriesParams{
			ParentIno: dirIno,
			Limit:     1,
			Offset:    0,
		}).Return([]db.ListDirEntriesRow{rawChildRow}, nil).Once()

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 2,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, []byte{0x80, 0x81, 0xfe, 0xff}, entries[0].Name)
		assert.Equal(t, uint64(203), entries[0].Ino)
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestReadDir_InvariantsAndErrors(t *testing.T) {
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "readdir tx:")
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "readdir tx:")
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

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 0,
			Limit:  10,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "readdir tx:")
		assert.Nil(t, entries)
	})
}

// TestReadDir_G05_SameDirRenameAndCursorLifecycle 验证 G-05：
// 1. 子场景 A：同目录重命名导致基于 offset 的游标补偿失步，在后续分页中重复读取已返回条目或遗漏条目。
// 2. 子场景 B：多页遍历目录后未调用 ReleaseDir 直接 DeleteDisk 销毁磁盘时，dirCursors 内存游标发生泄漏。
func TestReadDir_G05_SameDirRenameAndCursorLifecycle(t *testing.T) {
	dsn := os.Getenv("STORAGE_DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:9432/bowl?sslmode=disable"
	}

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

	t.Run("SubA_SameDirRenameCursorOffsetMismatch", func(t *testing.T) {
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdir-rename-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)

		dirD, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dirD"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		// 在目录 D 下创建 4 个文件："a", "b", "c", "d"
		for _, name := range []string{"a", "b", "c", "d"} {
			_, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: dirD.Ino,
				Name:      []byte(name),
				Mode:      meta.S_IFREG | 0o644,
			})
			require.NoError(t, err)
		}

		// 打开目录获取句柄
		openRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   dirD.Ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
				Ino: dirD.Ino,
				Fh:  openRes.Fh,
			})
		}()

		// 第一页 ReadDir(offset=0, limit=4)：返回 ".", "..", "a", "b"
		p1, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    dirD.Ino,
			Fh:     openRes.Fh,
			Offset: 0,
			Limit:  4,
		})
		require.NoError(t, err)
		require.Len(t, p1, 4)
		require.Equal(t, ".", string(p1[0].Name))
		require.Equal(t, "..", string(p1[1].Name))
		require.Equal(t, "a", string(p1[2].Name))
		require.Equal(t, "b", string(p1[3].Name))
		require.Equal(t, uint64(4), p1[3].NextOffset)

		// 在读取下一页（offset=4）前，执行同目录重命名：将 "a" 重命名为 "aa"
		// 此时目录中的文件项按字典序为："aa", "b", "c", "d"
		err = mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: dirD.Ino,
			OldName:      []byte("a"),
			NewParentIno: dirD.Ino,
			NewName:      []byte("aa"),
		})
		require.NoError(t, err)

		// 继续调用 ReadDir(offset=4, limit=10) 读取剩余目录项
		p2, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    dirD.Ino,
			Fh:     openRes.Fh,
			Offset: 4,
			Limit:  10,
		})
		require.NoError(t, err)

		var names2 []string
		for _, e := range p2 {
			names2 = append(names2, string(e.Name))
		}
		// 正确的目录流语义：第二页必须严格且只返回剩余未读取的 ["c", "d"]
		// 当前生产代码在同目录 Rename 时将 offset 从 2 减到 1，导致 ListDirEntries 从 OFFSET 1 读取，
		// 从而重复返回了已经在第一页读取过的 "b"（返回 ["b", "c", "d"]）
		require.Equal(t, []string{"c", "d"}, names2, "第二页读取必须严格返回剩余未读取的 [c, d]，既不能重复返回已读过的 b，也不能漏读")
	})

	t.Run("SubB_DeleteDiskCursorCleanupLeak", func(t *testing.T) {
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdir-leak-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)

		dirLeak, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dirLeak"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		for _, name := range []string{"f1", "f2", "f3", "f4"} {
			_, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: dirLeak.Ino,
				Name:      []byte(name),
				Mode:      meta.S_IFREG | 0o644,
			})
			require.NoError(t, err)
		}

		openRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   dirLeak.Ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err)

		// 翻页读取产生游标
		p1, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    dirLeak.Ino,
			Fh:     openRes.Fh,
			Offset: 0,
			Limit:  3,
		})
		require.NoError(t, err)
		require.NotEmpty(t, p1)

		p2, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    dirLeak.Ino,
			Fh:     openRes.Fh,
			Offset: p1[len(p1)-1].NextOffset,
			Limit:  3,
		})
		require.NoError(t, err)
		require.NotEmpty(t, p2)

		// 验证当前内存中记录了该目录的游标
		cursorsBefore := meta.InoDirCursorsCount(mgr, dirLeak.Ino)
		require.Greater(t, cursorsBefore, 0, "ReadDir 翻页后必须记录目录游标")

		// 不调用 ReleaseDir，直接通过 DeleteDisk 销毁该磁盘
		err = mgr.DeleteDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		// DeleteDisk 销毁磁盘后，所属目录的游标必须被同步清理以防内存泄漏
		cursorsAfter := meta.InoDirCursorsCount(mgr, dirLeak.Ino)
		require.Equal(t, 0, cursorsAfter, "DeleteDisk 销毁磁盘后必须清理所属目录的 dirCursors，避免内存泄漏")
	})
}

