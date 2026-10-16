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

func TestGetAttr_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	testCases := []struct {
		name       string
		ino        uint64
		generation int64
		mode       int32
		size       int64
		nlink      int32
		uid        int32
		gid        int32
		rdev       int64
		expectedBlk uint64
	}{
		{
			name:        "常规文件 (0o100644)",
			ino:         100,
			generation:  2,
			mode:        0o100644,
			size:        1024,
			nlink:       1,
			uid:         1001,
			gid:         1002,
			rdev:        0,
			expectedBlk: 2,
		},
		{
			name:        "目录 (S_IFDIR | 0o755)",
			ino:         2,
			generation:  1,
			mode:        int32(meta.S_IFDIR | 0o755),
			size:        4096,
			nlink:       2,
			uid:         0,
			gid:         0,
			rdev:        0,
			expectedBlk: 8,
		},
		{
			name:        "符号链接 (0o120777)",
			ino:         200,
			generation:  1,
			mode:        0o120777,
			size:        12,
			nlink:       1,
			uid:         1000,
			gid:         1000,
			rdev:        0,
			expectedBlk: 1,
		},
		{
			name:        "FIFO 管道 (0o010666)",
			ino:         300,
			generation:  1,
			mode:        0o010666,
			size:        0,
			nlink:       1,
			uid:         1000,
			gid:         1000,
			rdev:        0,
			expectedBlk: 0,
		},
		{
			name:        "Socket (0o140777)",
			ino:         400,
			generation:  1,
			mode:        0o140777,
			size:        0,
			nlink:       1,
			uid:         1000,
			gid:         1000,
			rdev:        0,
			expectedBlk: 0,
		},
		{
			name:        "块设备 (0o060660)",
			ino:         500,
			generation:  1,
			mode:        0o060660,
			size:        0,
			nlink:       1,
			uid:         0,
			gid:         6,
			rdev:        0x0801,
			expectedBlk: 0,
		},
		{
			name:        "字符设备 (0o020660)",
			ino:         600,
			generation:  1,
			mode:        0o020660,
			size:        0,
			nlink:       1,
			uid:         0,
			gid:         5,
			rdev:        0x0103,
			expectedBlk: 0,
		},
		{
			name:        "已解链未释放孤儿节点 (unlink-whilst-open, nlink == 0)",
			ino:         700,
			generation:  5,
			mode:        0o100644,
			size:        2048,
			nlink:       0,
			uid:         1001,
			gid:         1001,
			rdev:        0,
			expectedBlk: 4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			atime := now.Add(-time.Hour)
			mtime := now.Add(-30 * time.Minute)
			ctime := now.Add(-10 * time.Minute)

			row := db.Inode{
				Ino:        int64(tc.ino),
				Generation: tc.generation,
				Mode:       tc.mode,
				Size:       tc.size,
				Nlink:      tc.nlink,
				Uid:        tc.uid,
				Gid:        tc.gid,
				Rdev:       tc.rdev,
				Atime:      pgtype.Timestamptz{Time: atime, Valid: true},
				Mtime:      pgtype.Timestamptz{Time: mtime, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: ctime, Valid: true},
			}

			mockStore.EXPECT().
				GetInode(mock.Anything, int64(tc.ino)).
				Return(row, nil).
				Once()

			inode, err := mgr.GetAttr(ctx, tc.ino)
			require.NoError(t, err)
			require.NotNil(t, inode)

			assert.Equal(t, tc.ino, inode.Ino)
			assert.Equal(t, uint64(tc.generation), inode.Generation)
			assert.Equal(t, uint32(tc.mode), inode.Attr.Mode)
			assert.Equal(t, uint64(tc.size), inode.Attr.Size)
			assert.Equal(t, uint32(tc.nlink), inode.Attr.Nlink)
			assert.Equal(t, uint32(tc.uid), inode.Attr.UID)
			assert.Equal(t, uint32(tc.gid), inode.Attr.GID)
			assert.Equal(t, uint32(tc.rdev), inode.Attr.Rdev)
			assert.Equal(t, uint32(4096), inode.Attr.Blksize)
			assert.Equal(t, tc.expectedBlk, inode.Attr.Blocks)
			assert.True(t, atime.Equal(inode.Attr.Atime))
			assert.True(t, mtime.Equal(inode.Attr.Mtime))
			assert.True(t, ctime.Equal(inode.Attr.Ctime))
		})
	}
}

func TestGetAttr_DynamicBlockCalculations(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name           string
		size           int64
		expectedBlocks uint64
	}{
		{name: "0 字节对应 0 块", size: 0, expectedBlocks: 0},
		{name: "1 字节对应 1 块", size: 1, expectedBlocks: 1},
		{name: "512 字节边界对应 1 块", size: 512, expectedBlocks: 1},
		{name: "513 字节跨边界对应 2 块", size: 513, expectedBlocks: 2},
		{name: "1024 字节边界对应 2 块", size: 1024, expectedBlocks: 2},
		{name: "4096 字节边界对应 8 块", size: 4096, expectedBlocks: 8},
		{name: "4097 字节跨边界对应 9 块", size: 4097, expectedBlocks: 9},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			ino := uint64(42)
			row := db.Inode{
				Ino:  int64(ino),
				Mode: 0o100644,
				Size: tc.size,
			}

			mockStore.EXPECT().
				GetInode(mock.Anything, int64(ino)).
				Return(row, nil).
				Once()

			inode, err := mgr.GetAttr(ctx, ino)
			require.NoError(t, err)
			require.NotNil(t, inode)
			assert.Equal(t, uint32(4096), inode.Attr.Blksize)
			assert.Equal(t, tc.expectedBlocks, inode.Attr.Blocks)
		})
	}
}

func TestGetAttr_InputValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name string
		ino  uint64
	}{
		{name: "ino 为 0 非法", ino: 0},
		{name: "ino 为 math.MaxInt64 + 1 溢出非法", ino: uint64(math.MaxInt64) + 1},
		{name: "ino 为 math.MaxUint64 溢出非法", ino: math.MaxUint64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			inode, err := mgr.GetAttr(ctx, tc.ino)
			assert.Nil(t, inode)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)

			// 断言未调用底层数据库查询与事务
			mockStore.AssertNotCalled(t, "GetInode")
			mockStore.AssertNotCalled(t, "ExecTxWithOptions")
		})
	}
}

func TestGetAttr_NegativeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("记录不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(9999)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.GetAttr(ctx, ino)
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("底层数据库错误被包装并透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(1234)
		dbErr := errors.New("connection reset by peer")

		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, dbErr).
			Once()

		inode, err := mgr.GetAttr(ctx, ino)
		assert.Nil(t, inode)
		require.Error(t, err)
		assert.ErrorIs(t, err, dbErr)
		assert.NotErrorIs(t, err, meta.ErrNotFound)
		assert.NotErrorIs(t, err, meta.ErrNotDir)
		assert.NotErrorIs(t, err, meta.ErrInvalidArgument)
	})
}

func TestGetAttr_DirectQueryContract(t *testing.T) {
	ctx := context.Background()

	t.Run("单语句直查契约：仅调用 GetInode 且未调用 ExecTxWithOptions", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(88)
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{Ino: int64(ino), Mode: 0o100644}, nil).
			Once()

		inode, err := mgr.GetAttr(ctx, ino)
		require.NoError(t, err)
		require.NotNil(t, inode)

		mockStore.AssertCalled(t, "GetInode", mock.Anything, int64(ino))
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})
}
