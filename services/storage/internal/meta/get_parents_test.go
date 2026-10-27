package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// TestGetParents_InputValidation 验证 GetParents 的入参防御逻辑。
func TestGetParents_InputValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name string
		ino  uint64
	}{
		{
			name: "ino 为 0",
			ino:  0,
		},
		{
			name: "ino 超过 math.MaxInt64",
			ino:  uint64(math.MaxInt64) + 1,
		},
		{
			name: "ino 为 uint64 最大值",
			ino:  math.MaxUint64,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			parents, err := mgr.GetParents(ctx, tc.ino)
			require.Error(t, err)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
			assert.Nil(t, parents)
			mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
		})
	}
}

// TestGetParents_Unit 验证 GetParents 领域层在 Mock Store/Querier 下的正常流与异常流。
func TestGetParents_Unit(t *testing.T) {
	ctx := context.Background()

	t.Run("正常流: 单父目录单条目", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		ino := uint64(100)
		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			ListParentEntryCountsByIno(mock.Anything, int64(ino)).
			Return([]db.ListParentEntryCountsByInoRow{
				{ParentIno: 1, EntryCount: 1},
			}, nil).
			Once()

		parents, err := mgr.GetParents(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]uint32{1: 1}, parents)
	})

	t.Run("正常流: 多父目录与多硬链接计数", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		ino := uint64(200)
		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			ListParentEntryCountsByIno(mock.Anything, int64(ino)).
			Return([]db.ListParentEntryCountsByInoRow{
				{ParentIno: 10, EntryCount: 2},
				{ParentIno: 20, EntryCount: 1},
			}, nil).
			Once()

		parents, err := mgr.GetParents(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]uint32{10: 2, 20: 1}, parents)
	})

	t.Run("正常流: 无父目录条目返回空且非 nil 的 map", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		ino := uint64(300)
		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			ListParentEntryCountsByIno(mock.Anything, int64(ino)).
			Return([]db.ListParentEntryCountsByInoRow{}, nil).
			Once()

		parents, err := mgr.GetParents(ctx, ino)
		require.NoError(t, err)
		assert.NotNil(t, parents)
		assert.Empty(t, parents)
	})

	t.Run("异常流: 底层查询 ListParentEntryCountsByIno 报错向上传播包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		ino := uint64(400)
		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			ListParentEntryCountsByIno(mock.Anything, int64(ino)).
			Return(nil, errors.New("underlying db query error")).
			Once()

		parents, err := mgr.GetParents(ctx, ino)
		require.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("list parent entry counts for ino %d", ino))
		assert.Contains(t, err.Error(), "underlying db query error")
		assert.Nil(t, parents)
	})

	t.Run("异常流: 事务执行 ExecTxWithOptions 报错向上传播错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(500)
		expectedErr := errors.New("tx begin error")

		mockStore.EXPECT().
			ExecTxWithOptions(
				mock.Anything,
				mock.Anything,
				matchReadOnlyOpt(),
				matchRepeatableReadOpt(),
			).
			Return(expectedErr).
			Once()

		parents, err := mgr.GetParents(ctx, ino)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Nil(t, parents)
	})
}

// TestGetParents_PostgresIntegration 验证 GetParents 在真实 PostgreSQL 环境下的功能与硬链接拓扑计数。
func TestGetParents_PostgresIntegration(t *testing.T) {
	dsn := getStorageTestDSN()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过 PostgreSQL 测试 (连接初始化失败): %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("跳过 PostgreSQL 测试 (ping 失败): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	// 创建独立测试虚拟磁盘
	diskName := fmt.Sprintf("test_get_parents_%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 10 * 1024 * 1024 * 1024,
	})
	require.NoError(t, err)
	rootIno := disk.RootIno
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	}()

	t.Run("不存在的 Inode 返回空且非 nil 的 map", func(t *testing.T) {
		parents, err := mgr.GetParents(ctx, 999999999)
		require.NoError(t, err)
		assert.NotNil(t, parents)
		assert.Empty(t, parents)
	})

	t.Run("单父目录、多硬链接及跨父目录硬链接的动态计数变化", func(t *testing.T) {
		// 1. 创建普通文件
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("file1.txt"),
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino

		// 验证初始状态：仅在 root 目录下有 1 个条目
		parents, err := mgr.GetParents(ctx, fileIno)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]uint32{rootIno: 1}, parents)

		// 2. 同一父目录下创建第 2 个硬链接
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          fileIno,
			NewParentIno: rootIno,
			NewName:      []byte("file1_link.txt"),
		})
		require.NoError(t, err)

		// 验证同目录计数递增为 2
		parents, err = mgr.GetParents(ctx, fileIno)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]uint32{rootIno: 2}, parents)

		// 3. 创建子目录，并在子目录下创建跨父目录硬链接
		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("sub_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		subDirIno := subDir.Ino

		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          fileIno,
			NewParentIno: subDirIno,
			NewName:      []byte("file1_sublink.txt"),
		})
		require.NoError(t, err)

		// 验证跨父目录计数：root 为 2，sub_dir 为 1
		parents, err = mgr.GetParents(ctx, fileIno)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]uint32{rootIno: 2, subDirIno: 1}, parents)

		// 4. Unlink 子目录下的硬链接
		err = mgr.Unlink(ctx, subDirIno, []byte("file1_sublink.txt"))
		require.NoError(t, err)

		// 验证 sub_dir 计数归零（不再出现在 map 中），root 仍为 2
		parents, err = mgr.GetParents(ctx, fileIno)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]uint32{rootIno: 2}, parents)
		assert.NotContains(t, parents, subDirIno)

		// 5. Unlink root 下的其中一个硬链接
		err = mgr.Unlink(ctx, rootIno, []byte("file1_link.txt"))
		require.NoError(t, err)

		// 验证 root 计数递减为 1
		parents, err = mgr.GetParents(ctx, fileIno)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]uint32{rootIno: 1}, parents)

		// 6. Unlink root 下最后一个条目
		err = mgr.Unlink(ctx, rootIno, []byte("file1.txt"))
		require.NoError(t, err)

		// 验证所有条目被移除后返回空且非 nil 的 map
		parents, err = mgr.GetParents(ctx, fileIno)
		require.NoError(t, err)
		assert.NotNil(t, parents)
		assert.Empty(t, parents)
	})
}
