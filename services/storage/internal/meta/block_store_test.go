package meta_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

func TestSliceBlocks(t *testing.T) {
	const fourMiB = 4 * 1024 * 1024

	t.Run("0 字节返回空切片", func(t *testing.T) {
		blocks := meta.SliceBlocks(0)
		assert.Empty(t, blocks)
	})

	t.Run("小于 4 MiB 的单个尾块", func(t *testing.T) {
		blocks := meta.SliceBlocks(1024)
		require.Len(t, blocks, 1)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(1024), blocks[0].BlockSize)
	})

	t.Run("恰好 4 MiB 单个整块", func(t *testing.T) {
		blocks := meta.SliceBlocks(fourMiB)
		require.Len(t, blocks, 1)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[0].BlockSize)
	})

	t.Run("4 MiB + 1 字节切分为 1 整块 + 1 尾块", func(t *testing.T) {
		blocks := meta.SliceBlocks(fourMiB + 1)
		require.Len(t, blocks, 2)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[0].BlockSize)
		assert.Equal(t, uint32(1), blocks[1].BlockIdx)
		assert.Equal(t, uint32(1), blocks[1].BlockSize)
	})

	t.Run("8 MiB 切分为 2 个整块", func(t *testing.T) {
		blocks := meta.SliceBlocks(2 * fourMiB)
		require.Len(t, blocks, 2)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[0].BlockSize)
		assert.Equal(t, uint32(1), blocks[1].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[1].BlockSize)
	})

	t.Run("9 MiB 切分为 2 个整块 + 1 尾块", func(t *testing.T) {
		blocks := meta.SliceBlocks(2*fourMiB + 1024*1024)
		require.Len(t, blocks, 3)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[0].BlockSize)
		assert.Equal(t, uint32(1), blocks[1].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[1].BlockSize)
		assert.Equal(t, uint32(2), blocks[2].BlockIdx)
		assert.Equal(t, uint32(1024*1024), blocks[2].BlockSize)
	})
}

func TestLocateBlocks(t *testing.T) {
	const fourMiB = 4 * 1024 * 1024

	t.Run("单块内局部读取", func(t *testing.T) {
		locs, err := meta.LocateBlocks(1000, 100, 500)
		require.NoError(t, err)
		require.Len(t, locs, 1)
		assert.Equal(t, uint32(0), locs[0].BlockIdx)
		assert.Equal(t, uint32(1000), locs[0].BlockSize)
		assert.Equal(t, uint32(100), locs[0].BlockOff)
		assert.Equal(t, uint32(500), locs[0].Length)
	})

	t.Run("跨两块边界读取", func(t *testing.T) {
		sliceSize := uint32(5 * 1024 * 1024) // 5 MiB (block0=4MiB, block1=1MiB)
		sliceOff := uint32(fourMiB - 100)
		reqLen := uint32(250)

		locs, err := meta.LocateBlocks(sliceSize, sliceOff, reqLen)
		require.NoError(t, err)
		require.Len(t, locs, 2)

		// 块 0：读末尾 100 字节
		assert.Equal(t, uint32(0), locs[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), locs[0].BlockSize)
		assert.Equal(t, uint32(fourMiB-100), locs[0].BlockOff)
		assert.Equal(t, uint32(100), locs[0].Length)

		// 块 1：读头部 150 字节
		assert.Equal(t, uint32(1), locs[1].BlockIdx)
		assert.Equal(t, uint32(1024*1024), locs[1].BlockSize)
		assert.Equal(t, uint32(0), locs[1].BlockOff)
		assert.Equal(t, uint32(150), locs[1].Length)
	})

	t.Run("跨多块全量读取", func(t *testing.T) {
		sliceSize := uint32(9 * 1024 * 1024)
		locs, err := meta.LocateBlocks(sliceSize, 0, sliceSize)
		require.NoError(t, err)
		require.Len(t, locs, 3)

		assert.Equal(t, uint32(0), locs[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), locs[0].BlockSize)
		assert.Equal(t, uint32(0), locs[0].BlockOff)
		assert.Equal(t, uint32(fourMiB), locs[0].Length)

		assert.Equal(t, uint32(1), locs[1].BlockIdx)
		assert.Equal(t, uint32(fourMiB), locs[1].BlockSize)
		assert.Equal(t, uint32(0), locs[1].BlockOff)
		assert.Equal(t, uint32(fourMiB), locs[1].Length)

		assert.Equal(t, uint32(2), locs[2].BlockIdx)
		assert.Equal(t, uint32(1024*1024), locs[2].BlockSize)
		assert.Equal(t, uint32(0), locs[2].BlockOff)
		assert.Equal(t, uint32(1024*1024), locs[2].Length)
	})

	t.Run("reqLen 为 0 返回空切片", func(t *testing.T) {
		locs, err := meta.LocateBlocks(1000, 100, 0)
		require.NoError(t, err)
		assert.Empty(t, locs)
	})

	t.Run("请求超出 sliceSize 报错 ErrInvalidArgument", func(t *testing.T) {
		_, err := meta.LocateBlocks(1000, 800, 300)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
	})

	t.Run("起始偏移超出 sliceSize 报错 ErrInvalidArgument", func(t *testing.T) {
		_, err := meta.LocateBlocks(1000, 1001, 10)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
	})
}

func TestLocalFSBlockStore_Lifecycle(t *testing.T) {
	dataDir := t.TempDir()
	store := meta.NewLocalFSBlockStore(dataDir)
	ctx := context.Background()

	sliceID := uint64(42)
	blockIdx := uint32(0)
	testData := []byte("hello nvme block storage!")
	blockSize := uint32(len(testData))

	t.Run("PutBlock 原子写入并落盘", func(t *testing.T) {
		err := store.PutBlock(ctx, sliceID, blockIdx, testData)
		require.NoError(t, err)

		// 验证文件路径格式：<data_dir>/chunks/%02X/%v/<slice_id>_<block_idx>_<block_size>
		// 42 % 256 = 42 = 0x2A; 42 / 1000 / 1000 = 0
		expectedPath := filepath.Join(dataDir, "chunks", "2A", "0", fmt.Sprintf("42_0_%d", blockSize))
		content, err := os.ReadFile(expectedPath)
		require.NoError(t, err)
		assert.Equal(t, testData, content)
	})

	t.Run("GetBlock 全量读取", func(t *testing.T) {
		dst := make([]byte, blockSize)
		err := store.GetBlock(ctx, sliceID, blockIdx, blockSize, 0, dst)
		require.NoError(t, err)
		assert.Equal(t, testData, dst)
	})

	t.Run("GetBlock 分段偏移读取", func(t *testing.T) {
		offset := uint32(6)
		readLen := uint32(4) // "nvme"
		dst := make([]byte, readLen)
		err := store.GetBlock(ctx, sliceID, blockIdx, blockSize, offset, dst)
		require.NoError(t, err)
		assert.Equal(t, []byte("nvme"), dst)
	})

	t.Run("DeleteBlock 物理删除", func(t *testing.T) {
		err := store.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
		require.NoError(t, err)

		expectedPath := filepath.Join(dataDir, "chunks", "2A", "0", fmt.Sprintf("42_0_%d", blockSize))
		_, err = os.Stat(expectedPath)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("DeleteBlock 幂等删除不存在的文件成功", func(t *testing.T) {
		err := store.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
		assert.NoError(t, err)
	})
}

func TestLocalFSBlockStore_LargeBlock(t *testing.T) {
	dataDir := t.TempDir()
	store := meta.NewLocalFSBlockStore(dataDir)
	ctx := context.Background()

	// 模拟 4 MiB 整块写入与读取
	const blockSize = 4 * 1024 * 1024
	largeData := make([]byte, blockSize)
	_, err := rand.Read(largeData)
	require.NoError(t, err)

	sliceID := uint64(1000000) // 1000000 % 256 = 64 (0x40), 1000000 / 1000 / 1000 = 1
	blockIdx := uint32(0)

	err = store.PutBlock(ctx, sliceID, blockIdx, largeData)
	require.NoError(t, err)

	expectedPath := filepath.Join(dataDir, "chunks", "40", "1", fmt.Sprintf("1000000_0_%d", blockSize))
	stat, err := os.Stat(expectedPath)
	require.NoError(t, err)
	assert.Equal(t, int64(blockSize), stat.Size())

	// 块内末尾部分读取
	offset := uint32(blockSize - 1024)
	dst := make([]byte, 1024)
	err = store.GetBlock(ctx, sliceID, blockIdx, blockSize, offset, dst)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(largeData[offset:], dst))
}

func TestLocalFSBlockStore_Errors(t *testing.T) {
	dataDir := t.TempDir()
	store := meta.NewLocalFSBlockStore(dataDir)
	ctx := context.Background()

	t.Run("文件不存在时 GetBlock 返回包含 ErrIO 的错误", func(t *testing.T) {
		dst := make([]byte, 100)
		err := store.GetBlock(ctx, 9999, 0, 100, 0, dst)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIO), "expected ErrIO but got: %v", err)
	})

	t.Run("读取超出块大小时 GetBlock 返回包含 ErrIO 的错误", func(t *testing.T) {
		sliceID := uint64(88)
		blockIdx := uint32(0)
		data := []byte("short")
		blockSize := uint32(len(data))

		require.NoError(t, store.PutBlock(ctx, sliceID, blockIdx, data))

		// offset + len > blockSize
		dst := make([]byte, 10)
		err := store.GetBlock(ctx, sliceID, blockIdx, blockSize, 0, dst)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIO))
	})

	t.Run("物理文件长度短于预期时 GetBlock 返回包含 ErrIO 的错误", func(t *testing.T) {
		sliceID := uint64(89)
		blockIdx := uint32(0)
		blockSize := uint32(100)

		// 手工创建一个只有 10 字节的文件但声明 blockSize 为 100
		dir := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", sliceID%256), fmt.Sprintf("%d", sliceID/1000/1000))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		filePath := filepath.Join(dir, fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize))
		require.NoError(t, os.WriteFile(filePath, []byte("short_data"), 0o644))

		dst := make([]byte, 50)
		err := store.GetBlock(ctx, sliceID, blockIdx, blockSize, 0, dst)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIO))
	})

	t.Run("Context 取消时操作立即返回错误", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err := store.PutBlock(canceledCtx, 1, 0, []byte("test"))
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))

		dst := make([]byte, 10)
		err = store.GetBlock(canceledCtx, 1, 0, 10, 0, dst)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))

		err = store.DeleteBlock(canceledCtx, 1, 0, 10)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})
}

func TestLocalFSBlockStore_ConcurrentWrites(t *testing.T) {
	dataDir := t.TempDir()
	store := meta.NewLocalFSBlockStore(dataDir)
	ctx := context.Background()

	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			sliceID := uint64(idx + 100)
			data := fmt.Appendf(nil, "payload-from-goroutine-%d", idx)
			blockSize := uint32(len(data))

			err := store.PutBlock(ctx, sliceID, 0, data)
			assert.NoError(t, err)

			dst := make([]byte, blockSize)
			err = store.GetBlock(ctx, sliceID, 0, blockSize, 0, dst)
			assert.NoError(t, err)
			assert.Equal(t, data, dst)
		}(i)
	}

	wg.Wait()
}

type managerAccessor interface {
	BlockStore() meta.BlockStore
}

func TestManager_BlockStoreInjection(t *testing.T) {
	t.Run("meta.New 不传 WithBlockStore 默认向后兼容", func(t *testing.T) {
		mgr := meta.New(nil)
		require.NotNil(t, mgr)
		acc, ok := mgr.(managerAccessor)
		require.True(t, ok)
		assert.Nil(t, acc.BlockStore())
	})

	t.Run("meta.New 显式注入 BlockStore 生效", func(t *testing.T) {
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(nil, meta.WithBlockStore(mockBS))
		require.NotNil(t, mgr)
		acc, ok := mgr.(managerAccessor)
		require.True(t, ok)
		assert.Equal(t, mockBS, acc.BlockStore())
	})

	t.Run("Fx Module 注入自定义 BlockStore 生效", func(t *testing.T) {
		mockBS := metamock.NewMockBlockStore(t)
		mockStore := dbmock.NewMockStore(t)
		var extractedMgr meta.Manager

		app := fxtest.New(
			t,
			fx.Provide(
				func() db.Store { return mockStore },
				func() meta.BlockStore { return mockBS },
				func() config.MetaConfig {
					return config.MetaConfig{
						DataDirVal: t.TempDir(),
					}
				},
			),
			meta.Module,
			fx.Populate(&extractedMgr),
		)
		require.NoError(t, app.Err())
		require.NotNil(t, extractedMgr)

		acc, ok := extractedMgr.(managerAccessor)
		require.True(t, ok)
		assert.Equal(t, mockBS, acc.BlockStore())
	})

	t.Run("Fx Module 未提供 BlockStore 时根据 MetaConfig 自动构造默认 LocalFSBlockStore", func(t *testing.T) {
		tempDir := t.TempDir()
		mockStore := dbmock.NewMockStore(t)
		var extractedMgr meta.Manager

		app := fxtest.New(
			t,
			fx.Provide(
				func() db.Store { return mockStore },
				func() config.MetaConfig {
					return config.MetaConfig{
						DataDirVal: tempDir,
					}
				},
			),
			meta.Module,
			fx.Populate(&extractedMgr),
		)
		require.NoError(t, app.Err())
		require.NotNil(t, extractedMgr)

		acc, ok := extractedMgr.(managerAccessor)
		require.True(t, ok)
		require.NotNil(t, acc.BlockStore())
		_, isLocalFS := acc.BlockStore().(*meta.LocalFSBlockStore)
		assert.True(t, isLocalFS)
	})
}
