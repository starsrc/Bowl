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

	t.Run("SubA_SameDirRenameCursorOffsetMismatch", func(t *testing.T) {
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdir-rename-%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		}()

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
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		}()

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

// TestReadDir_P4_P5_ReproduceDefects 针对 P-4 与 P-5 缺陷提供红测验证：
// P-4: 目录变更后续读第 3 页重置 dirMutated=false 导致切回偏移分页漏读条目 + 事务提交前提前污染游标缓存。
// P-5: 具名存活目录上的过期客户端会话被清理或句柄失效后，内存中 dirCursors 永不回收导致内存泄漏。
func TestReadDir_P4_P5_ReproduceDefects(t *testing.T) {
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
			Name:     fmt.Sprintf("disk-readdir-p4-%d", time.Now().UnixNano()),
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
		p1, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
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
		p2, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
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
		p3, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
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
		require.NotEmpty(t, p3, "P-4 缺陷复现：第 2 页读取重置 dirMutated=false 导致第 3 页切回 OFFSET 分页查空，漏读条目 d")
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

		_, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    uint64(dirIno),
			Fh:     uint64(fh),
			Offset: 2,
			Limit:  1,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "readdir tx:")
		require.ErrorIs(t, err, commitErr)

		// 事务提交失败，闭包中途产生的游标绝不能泄露/污染进内存缓存
		require.Equal(t, 0, meta.DirCursorsLen(mgr), "P-4 缺陷复现：事务提交失败后不得在内存中残留或新增游标")
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

		_, err := mgr.ReadDir(ctx, meta.ReadDirParams{
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

		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{
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
			Name:     fmt.Sprintf("disk-readdir-p5-%d", time.Now().UnixNano()),
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
		p1, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
			Ino:       dirP5.Ino,
			Fh:        openRes.Fh,
			Offset:    0,
			Limit:     3,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, p1)

		p2, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
			Ino:       dirP5.Ino,
			Fh:        openRes.Fh,
			Offset:    p1[len(p1)-1].NextOffset,
			Limit:     2,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.NotEmpty(t, p2)

		require.Greater(t, meta.InoDirCursorsCount(mgr, dirP5.Ino), 0, "ReadDir 翻页后必须记录游标")

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
		// 客户端带着原已失效句柄再次尝试 ReadDir 时，校验返回 ErrBadFileHandle
		_, err = mgr.ReadDir(testCtx, meta.ReadDirParams{
			Ino:       dirP5.Ino,
			Fh:        openRes.Fh,
			Offset:    p2[len(p2)-1].NextOffset,
			Limit:     2,
			SessionID: &sessID,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})
}

type readDirP24HookStore struct {
	db.Store
	pool              *pgxpool.Pool
	onGetPool         func()
	onWriteTxCommit   func()
	onReadDirTxCommit func()
}

func (s *readDirP24HookStore) Pool() *pgxpool.Pool {
	if s.onGetPool != nil {
		s.onGetPool()
	}
	return s.pool
}

func (s *readDirP24HookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
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

func TestReadDir_P2_1_3_4_ReproduceDefects(t *testing.T) {
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
			Name:     fmt.Sprintf("disk-readdir-p21-%d", time.Now().UnixNano()),
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

		entries, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
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
			Name:     fmt.Sprintf("disk-readdir-p23-%d", time.Now().UnixNano()),
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

		p1, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
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
		_, err = mgr.ReadDir(testCtx, meta.ReadDirParams{
			Ino:       dir.Ino,
			Fh:        openRes.Fh,
			Offset:    2,
			Limit:     1,
			SessionID: &sessB,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "跨会话调用必须返回 ErrBadFileHandle")

		require.Equal(t, 1, meta.InoDirCursorsCount(mgr, dir.Ino), "P2-3: 跨会话调用失败不应清除合法会话的游标")

		p2, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
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
		hookStore := &readDirP24HookStore{
			Store: baseStore,
			pool:  pool,
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdir-p24a-%d", time.Now().UnixNano()),
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

		_, err = mgr.ReadDir(testCtx, meta.ReadDirParams{
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
		// 并发执行会话 B 的 ReadDir 建立游标
		poolCallCount := 0
		hookStore.onGetPool = func() {
			poolCallCount++
			t.Logf("onGetPool called #%d", poolCallCount)
			if poolCallCount == 2 {
				_, readErr := mgr.ReadDir(testCtx, meta.ReadDirParams{
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
		hookStore := &readDirP24HookStore{
			Store: baseStore,
			pool:  pool,
		}
		mgr := meta.New(hookStore)

		disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk-readdir-p24b-%d", time.Now().UnixNano()),
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

		p1, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
			Ino:       dir.Ino,
			Fh:        openRes.Fh,
			Offset:    2,
			Limit:     1,
			SessionID: &sessA,
		})
		require.NoError(t, err)
		require.Len(t, p1, 1)
		require.Equal(t, "a", string(p1[0].Name))

		p2, err := mgr.ReadDir(testCtx, meta.ReadDirParams{
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
