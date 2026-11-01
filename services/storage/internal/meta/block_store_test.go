package meta_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
		bs, isLocalFS := acc.BlockStore().(*meta.LocalFSBlockStore)
		assert.True(t, isLocalFS)

		ctx := context.Background()
		probeData := []byte("datadir-probe")
		err := bs.PutBlock(ctx, 42, 0, probeData)
		require.NoError(t, err)

		// 验证数据确实落盘在 tempDir 对应的分片路径下，而非默认目录或其它路径
		expectedPath := filepath.Join(tempDir, "chunks", "2A", "0", "42_0_13")
		content, err := os.ReadFile(expectedPath)
		require.NoError(t, err)
		assert.Equal(t, probeData, content)

		// 构造第二个独立 tempDir2 的 Manager 实例，验证不同 DataDir 配置下物理落盘目录完全隔离互不干扰
		tempDir2 := t.TempDir()
		mockStore2 := dbmock.NewMockStore(t)
		var extractedMgr2 meta.Manager

		app2 := fxtest.New(
			t,
			fx.Provide(
				func() db.Store { return mockStore2 },
				func() config.MetaConfig {
					return config.MetaConfig{
						DataDirVal: tempDir2,
					}
				},
			),
			meta.Module,
			fx.Populate(&extractedMgr2),
		)
		require.NoError(t, app2.Err())
		require.NotNil(t, extractedMgr2)

		acc2, ok := extractedMgr2.(managerAccessor)
		require.True(t, ok)
		require.NotNil(t, acc2.BlockStore())
		bs2, ok := acc2.BlockStore().(*meta.LocalFSBlockStore)
		require.True(t, ok)

		probeData2 := []byte("datadir-probe-isolated-2")
		err = bs2.PutBlock(ctx, 42, 0, probeData2)
		require.NoError(t, err)

		expectedPath2 := filepath.Join(tempDir2, "chunks", "2A", "0", fmt.Sprintf("42_0_%d", len(probeData2)))
		content2, err := os.ReadFile(expectedPath2)
		require.NoError(t, err)
		assert.Equal(t, probeData2, content2)

		// 确保 tempDir 与 tempDir2 互不干扰：tempDir 内的文件保持不变，tempDir2 的文件不在 tempDir 中
		origContent, err := os.ReadFile(expectedPath)
		require.NoError(t, err)
		assert.Equal(t, probeData, origContent)

		_, statErr := os.Stat(filepath.Join(tempDir, "chunks", "2A", "0", fmt.Sprintf("42_0_%d", len(probeData2))))
		assert.True(t, errors.Is(statErr, os.ErrNotExist))

		_, statErr2 := os.Stat(filepath.Join(tempDir2, "chunks", "2A", "0", "42_0_13"))
		assert.True(t, errors.Is(statErr2, os.ErrNotExist))
	})
}

type spyDirHandle struct {
	file   *os.File
	dir    string
	onSync func(dir string, f *os.File) error
}

func (h *spyDirHandle) Sync() error {
	if h.onSync != nil {
		return h.onSync(h.dir, h.file)
	}
	return h.file.Sync()
}

func (h *spyDirHandle) Close() error {
	return h.file.Close()
}

func TestLocalFSBlockStore_DirectorySyncEnforcedOnPutAndDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("PutBlock 原子 Rename 后必须对分片目录执行 dir.Sync", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("requires non-root user to enforce directory 0o300 read permission")
		}
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		const sliceID = uint64(1)
		const blockIdx = uint32(0)
		payload := []byte("durable-payload")
		blockSize := uint32(len(payload))

		// 真实分片路径：%02X = 1%256 ("01"), %d = 1/1000/1000 ("0")
		shardDir := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", sliceID%256), fmt.Sprintf("%d", sliceID/1000/1000))
		require.NoError(t, os.MkdirAll(shardDir, 0o755))

		t.Cleanup(func() {
			_ = os.Chmod(shardDir, 0o755)
		})

		// 设为 0o300（d-wx------）：允许在该目录内创建临时文件、写入并 os.Rename，
		// 但禁止 os.Open(shardDir) 获取目录 fd 执行 Sync()，必然触发内核 EACCES: permission denied
		require.NoError(t, os.Chmod(shardDir, 0o300))

		err := store.PutBlock(ctx, sliceID, blockIdx, payload)
		require.Error(t, err, "PutBlock 必须在 Rename 后同步分片目录，无目录读权限时应报错 permission denied")

		// 恢复 0o755 权限后检查 shardDir，断言当目录 Sync 失败返回错误时，PutBlock 已清理回滚未完成持久化确认的目标块文件
		require.NoError(t, os.Chmod(shardDir, 0o755))
		finalPath := filepath.Join(shardDir, fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize))
		_, statErr := os.Stat(finalPath)
		assert.True(t, errors.Is(statErr, os.ErrNotExist), "目录 Sync 失败后目标块文件应被清理回滚，不应残留半发布文件: %s", finalPath)
	})

	t.Run("DeleteBlock 删除实际存在的文件时直接删除而不阻塞等待 dir.Sync", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("requires non-root user to enforce directory 0o300 read permission")
		}
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		const sliceID = uint64(2)
		const blockIdx = uint32(0)
		payload := []byte("to-delete")
		blockSize := uint32(len(payload))

		// 在正常 0o755 权限下先成功落盘
		require.NoError(t, store.PutBlock(ctx, sliceID, blockIdx, payload))

		shardDir := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", sliceID%256), fmt.Sprintf("%d", sliceID/1000/1000))
		t.Cleanup(func() {
			_ = os.Chmod(shardDir, 0o755)
		})

		// 设为 0o300（允许 os.Remove 删除目录项，但禁止 os.Open 同步目录）
		require.NoError(t, os.Chmod(shardDir, 0o300))

		err := store.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
		require.NoError(t, err, "DeleteBlock 直接 os.Remove 块文件，不再同步触发目录 Sync")

		require.NoError(t, os.Chmod(shardDir, 0o755))
		finalPath := filepath.Join(shardDir, fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize))
		_, statErr := os.Stat(finalPath)
		assert.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("PutBlock 与 DeleteBlock 目录 Sync 计数及 Sync 失败回滚清理断言", func(t *testing.T) {
		dataDir := t.TempDir()
		var syncedDirs []string
		var failNextShardSync bool

		shardDir := filepath.Join(dataDir, "chunks", "03", "0")
		store := meta.NewLocalFSBlockStore(dataDir, meta.WithOpenDirHook(func(dir string) (meta.DirHandle, error) {
			f, err := os.Open(dir)
			if err != nil {
				return nil, err
			}
			return &spyDirHandle{
				file: f,
				dir:  filepath.Clean(dir),
				onSync: func(cleaned string, file *os.File) error {
					syncedDirs = append(syncedDirs, cleaned)
					if failNextShardSync && cleaned == filepath.Clean(shardDir) {
						return errors.New("injected dir sync failure")
					}
					return file.Sync()
				},
			}, nil
		}))

		// 1. 首次写入 sliceID=3：应同步祖先目录（dataDir, chunks, chunks/03）以及 Rename 后的叶子目录 chunks/03/0
		payload := []byte("hook-verified-payload")
		require.NoError(t, store.PutBlock(ctx, 3, 0, payload))
		require.Contains(t, syncedDirs, filepath.Clean(shardDir), "PutBlock 必须对叶子分片目录执行实际 Sync")

		// 2. 同分片目录再次写入 sliceID=259：祖先目录已缓存，但 Rename 后仍必须对 shardDir 执行 1 次 Sync
		syncedDirs = nil
		require.NoError(t, store.PutBlock(ctx, 259, 0, payload))
		assert.Equal(t, []string{filepath.Clean(shardDir)}, syncedDirs, "同分片目录后续 PutBlock 必须在 Rename 后精确执行 1 次分片目录 Sync")

		// 3. 注入分片目录 Sync 失败：PutBlock 必须返回 ErrIO 并回滚删除已 Rename 的目标文件
		syncedDirs = nil
		failNextShardSync = true
		err := store.PutBlock(ctx, 515, 0, payload)
		require.ErrorIs(t, err, meta.ErrIO)
		assert.Equal(t, []string{filepath.Clean(shardDir)}, syncedDirs)
		rolledBackPath := filepath.Join(shardDir, fmt.Sprintf("515_0_%d", len(payload)))
		_, statErr := os.Stat(rolledBackPath)
		assert.ErrorIs(t, statErr, os.ErrNotExist, "目录 Sync 失败时必须回滚清理目标块文件")

		// 4. 恢复正常后 DeleteBlock 删除已存在块：不触发目录 Sync，且文件被移除
		failNextShardSync = false
		syncedDirs = nil
		require.NoError(t, store.DeleteBlock(ctx, 3, 0, uint32(len(payload))))
		assert.Empty(t, syncedDirs, "DeleteBlock 删除块文件时不触发同步目录 Sync")
		deletedPath := filepath.Join(shardDir, fmt.Sprintf("3_0_%d", len(payload)))
		_, delStatErr := os.Stat(deletedPath)
		assert.ErrorIs(t, delStatErr, os.ErrNotExist)
	})
}

func TestLocalFSBlockStore_AncestorDirSyncBarrierAndRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("前次祖先目录 syncDir 失败遗留已创建子目录后同散列桶后续写入不得跳过祖先目录同步", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("跳过权限测试: root 用户忽略 POSIX 0o300 目录只写不可读限制")
		}
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		chunksDir := filepath.Join(dataDir, "chunks")
		require.NoError(t, os.MkdirAll(chunksDir, 0o755))
		t.Cleanup(func() {
			_ = os.Chmod(chunksDir, 0o755)
		})

		// 设为 0o300 (-wx------)：允许在其下创建子目录与路径穿梭，但禁止 os.Open 执行 syncDir
		require.NoError(t, os.Chmod(chunksDir, 0o300))

		// 第 1 次调用：创建 chunks/01/0 子目录，但 syncDir(chunksDir) 失败未完成持久化
		err1 := store.PutBlock(ctx, 1, 0, []byte("first"))
		require.ErrorIs(t, err1, meta.ErrIO)

		// 保持 chunksDir 为 0o300 不变，第 2 次调用同散列分片目录
		// 绝不能因为 chunks/01/0 目录已存在就直接无锁返回 nil 跳过尚未同步成功的祖先目录 chunksDir
		err2 := store.PutBlock(ctx, 257, 0, []byte("second"))
		require.ErrorIs(t, err2, meta.ErrIO, "祖先目录 sync 失败后同散列分片后续写入不得跳过祖先目录同步")

		// 恢复 0o755，第 3 次调用必须成功且能完整读出
		require.NoError(t, os.Chmod(chunksDir, 0o755))
		err3 := store.PutBlock(ctx, 257, 0, []byte("second"))
		require.NoError(t, err3)

		got := make([]byte, len("second"))
		readErr := store.GetBlock(ctx, 257, 0, uint32(len("second")), 0, got)
		require.NoError(t, readErr)
		assert.Equal(t, []byte("second"), got)
	})

	t.Run("16 协程并发首写同一全新散列分片目录竞态检验", func(t *testing.T) {
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		const concurrency = 16
		startCh := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, concurrency)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-startCh
				payload := []byte(fmt.Sprintf("concurrency-payload-%d", idx))
				sliceID := uint64(idx*256 + 3)
				errs[idx] = store.PutBlock(ctx, sliceID, 0, payload)
			}(i)
		}

		close(startCh)
		wg.Wait()

		for i := 0; i < concurrency; i++ {
			require.NoError(t, errs[i], "并发首写全新散列分片目录不得报错 (协程 %d)", i)
			expected := []byte(fmt.Sprintf("concurrency-payload-%d", i))
			sliceID := uint64(i*256 + 3)
			data := make([]byte, len(expected))
			err := store.GetBlock(ctx, sliceID, 0, uint32(len(expected)), 0, data)
			require.NoError(t, err)
			assert.Equal(t, expected, data)
		}
	})
}

func TestLocalFSBlockStore_EnsureDirSynced_NewSubdirUnderCachedParent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("跳过权限测试: root 用户忽略 POSIX 0o300 目录只写不可读限制")
	}

	ctx := context.Background()

	t.Run("已缓存的 chunks 目录下新建第二个一级散列桶 02 时必须重新 syncDir 父目录", func(t *testing.T) {
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		require.NoError(t, store.PutBlock(ctx, 1, 0, []byte("first")))

		chunksDir := filepath.Join(dataDir, "chunks")
		t.Cleanup(func() {
			_ = os.Chmod(chunksDir, 0o755)
		})
		require.NoError(t, os.Chmod(chunksDir, 0o300))

		err := store.PutBlock(ctx, 2, 0, []byte("second-bucket"))
		require.Error(t, err)
		require.ErrorIs(t, err, meta.ErrIO)
	})

	t.Run("已缓存的 chunks/01 目录下新建第二个二级分组目录 1 时必须重新 syncDir 父目录", func(t *testing.T) {
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		require.NoError(t, store.PutBlock(ctx, 1, 0, []byte("group0")))

		bucket01Dir := filepath.Join(dataDir, "chunks", "01")
		t.Cleanup(func() {
			_ = os.Chmod(bucket01Dir, 0o755)
		})
		require.NoError(t, os.Chmod(bucket01Dir, 0o300))

		// 1000193 = 0x0f4301: 1000193 % 256 == 1 ("01"), 1000193 / 1_000_000 == 1 ("1")
		err := store.PutBlock(ctx, 1000193, 0, []byte("group1"))
		require.Error(t, err)
		require.ErrorIs(t, err, meta.ErrIO)
	})
}

func TestLocalFSBlockStore_EnsureDirSynced_RootAndAboveParentRetry(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("跳过权限测试: root 用户忽略 POSIX 0o300 目录只写不可读限制")
	}

	ctx := context.Background()

	t.Run("不存在的 dataDir 创建后其父目录 syncDir 失败，再次调用仍须报错直至父目录同步成功", func(t *testing.T) {
		parent := t.TempDir()
		dataDir := filepath.Join(parent, "data")
		store := meta.NewLocalFSBlockStore(dataDir)

		t.Cleanup(func() {
			_ = os.Chmod(parent, 0o755)
		})
		require.NoError(t, os.Chmod(parent, 0o300))

		// 第 1 次调用：创建 parent/data 子目录，但 syncDir(parent) 失败
		err1 := store.PutBlock(ctx, 1, 0, []byte("first"))
		require.ErrorIs(t, err1, meta.ErrIO)

		// 保持 parent 为 0o300 不变，第 2 次调用不得因为 dataDir 已创建就跳过 syncDir(parent)
		err2 := store.PutBlock(ctx, 1, 0, []byte("first"))
		require.ErrorIs(t, err2, meta.ErrIO)

		// 恢复 parent 为 0o755，第 3 次调用必须成功且能完整读出
		require.NoError(t, os.Chmod(parent, 0o755))
		err3 := store.PutBlock(ctx, 1, 0, []byte("first"))
		require.NoError(t, err3)

		got := make([]byte, len("first"))
		readErr := store.GetBlock(ctx, 1, 0, uint32(len("first")), 0, got)
		require.NoError(t, readErr)
		assert.Equal(t, []byte("first"), got)
	})

	t.Run("dataDir 之上存在多级未创建目录如 parent/bowl/data 时必须同步最外层 parent", func(t *testing.T) {
		parent := t.TempDir()
		dataDir := filepath.Join(parent, "bowl", "data")
		store := meta.NewLocalFSBlockStore(dataDir)

		t.Cleanup(func() {
			_ = os.Chmod(parent, 0o755)
		})
		require.NoError(t, os.Chmod(parent, 0o300))

		err := store.PutBlock(ctx, 1, 0, []byte("nested-base"))
		require.ErrorIs(t, err, meta.ErrIO)
	})
}

func TestLocalFSBlockStore_DirSyncedCacheFastPathAndSelfHeal(t *testing.T) {
	ctx := context.Background()

	t.Run("目标散列子目录首次创建并同步后，后续 StageBlock 与 PutBlock 直接命中内存缓存零重复目录 Stat", func(t *testing.T) {
		dataDir := t.TempDir()
		var statCalls atomic.Int64
		var syncMu sync.Mutex
		var syncedDirs []string

		store := meta.NewLocalFSBlockStore(
			dataDir,
			meta.WithStatHook(func(name string) (os.FileInfo, error) {
				statCalls.Add(1)
				return os.Stat(name)
			}),
			meta.WithOpenDirHook(func(dir string) (meta.DirHandle, error) {
				f, err := os.Open(dir)
				if err != nil {
					return nil, err
				}
				return &spyDirHandle{
					file: f,
					dir:  filepath.Clean(dir),
					onSync: func(cleaned string, file *os.File) error {
						syncMu.Lock()
						syncedDirs = append(syncedDirs, cleaned)
						syncMu.Unlock()
						return file.Sync()
					},
				}, nil
			}),
		)

		var stagedStore meta.StagedBlockStore = store

		// 1. 首次写入 sliceID=7（散列目录 chunks/07/0）：创建目录并同步祖先目录
		payload1 := []byte("first-block-in-bucket-07")
		dir1, err := stagedStore.StageBlock(ctx, 7, 0, payload1)
		require.NoError(t, err)
		expectedShardDir := filepath.Join(dataDir, "chunks", "07", "0")
		assert.Equal(t, expectedShardDir, dir1)
		assert.Greater(t, statCalls.Load(), int64(0), "首次创建散列子目录应触发目录 Stat")
		require.NoError(t, stagedStore.SyncDirs(ctx, []string{dir1}))

		// 2. 重置 Stat 与目录 Sync 计数，连续 10 次向同一散列目录（sliceID = 7 + i*256）执行 StageBlock
		statCalls.Store(0)
		syncMu.Lock()
		syncedDirs = nil
		syncMu.Unlock()

		var batchDirs []string
		for i := 1; i <= 10; i++ {
			sid := uint64(7 + i*256)
			data := fmt.Appendf(nil, "cached-dir-stage-payload-%d", i)
			parentDir, sErr := stagedStore.StageBlock(ctx, sid, 0, data)
			require.NoError(t, sErr)
			assert.Equal(t, expectedShardDir, parentDir)
			batchDirs = append(batchDirs, parentDir)
		}

		// 核心断言：缓存命中快速放行后，10 次 StageBlock 产生 0 次目录 Stat 与 0 次祖先目录 Sync
		assert.Equal(t, int64(0), statCalls.Load(), "已缓存散列目录后续 StageBlock 必须直接命中内存缓存返回，不得重复执行 os.Stat")
		syncMu.Lock()
		assert.Empty(t, syncedDirs, "StageBlock 阶段仅完成临时文件 Sync 与 Rename，不触发目录 Sync")
		syncMu.Unlock()

		// 批次调用 SyncDirs：10 个相同父目录去重后仅执行 1 次目录 Sync
		require.NoError(t, stagedStore.SyncDirs(ctx, batchDirs))
		syncMu.Lock()
		assert.Equal(t, []string{filepath.Clean(expectedShardDir)}, syncedDirs, "SyncDirs 对同一父目录必须去重仅执行 1 次 Sync")
		syncMu.Unlock()

		// 3. 再次调用 PutBlock 写入同一散列目录：同样 0 次目录 Stat，且仅在 Rename 后对叶子目录执行 1 次 Sync
		statCalls.Store(0)
		syncMu.Lock()
		syncedDirs = nil
		syncMu.Unlock()

		require.NoError(t, store.PutBlock(ctx, 7+11*256, 0, []byte("put-block-cached-dir")))
		assert.Equal(t, int64(0), statCalls.Load(), "已缓存散列目录后续 PutBlock 不得重复执行目录 Stat")
		syncMu.Lock()
		assert.Equal(t, []string{filepath.Clean(expectedShardDir)}, syncedDirs)
		syncMu.Unlock()
	})

	t.Run("已缓存散列子目录被外部误删后，后续写入自动剔除失效缓存并重建目录自愈成功", func(t *testing.T) {
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		require.NoError(t, store.PutBlock(ctx, 9, 0, []byte("initial-block")))

		shardDir := filepath.Join(dataDir, "chunks", "09", "0")
		require.NoError(t, os.RemoveAll(shardDir))

		healedData := []byte("self-healed-after-external-rmdir")
		require.NoError(t, store.PutBlock(ctx, 265, 0, healedData))

		got := make([]byte, len(healedData))
		require.NoError(t, store.GetBlock(ctx, 265, 0, uint32(len(healedData)), 0, got))
		assert.Equal(t, healedData, got)
	})
}

func TestLocalFSBlockStore_StageBlockAndSyncDirs(t *testing.T) {
	ctx := context.Background()

	t.Run("多块 StageBlock 与跨散列目录去重 SyncDirs 完整发布与清理验证", func(t *testing.T) {
		dataDir := t.TempDir()
		var syncedDirs []string
		var failSyncForDir string

		store := meta.NewLocalFSBlockStore(
			dataDir,
			meta.WithOpenDirHook(func(dir string) (meta.DirHandle, error) {
				f, err := os.Open(dir)
				if err != nil {
					return nil, err
				}
				return &spyDirHandle{
					file: f,
					dir:  filepath.Clean(dir),
					onSync: func(cleaned string, file *os.File) error {
						syncedDirs = append(syncedDirs, cleaned)
						if failSyncForDir != "" && cleaned == filepath.Clean(failSyncForDir) {
							return errors.New("injected syncDirs failure")
						}
						return file.Sync()
					},
				}, nil
			}),
		)

		// 预热两个散列目录 chunks/0A/0 (sliceID%256==10) 与 chunks/0B/0 (sliceID%256==11)
		require.NoError(t, store.PutBlock(ctx, 10, 0, []byte("warmup-a")))
		require.NoError(t, store.PutBlock(ctx, 11, 0, []byte("warmup-b")))

		syncedDirs = nil
		dirA := filepath.Join(dataDir, "chunks", "0A", "0")
		dirB := filepath.Join(dataDir, "chunks", "0B", "0")

		// 在两个已缓存目录下阶段化写入 5 个新块
		stageSpecs := []struct {
			sliceID uint64
			data    []byte
			expDir  string
		}{
			{sliceID: 266, data: []byte("block-a-1"), expDir: dirA},
			{sliceID: 522, data: []byte("block-a-2"), expDir: dirA},
			{sliceID: 267, data: []byte("block-b-1"), expDir: dirB},
			{sliceID: 778, data: []byte("block-a-3"), expDir: dirA},
			{sliceID: 523, data: []byte("block-b-2"), expDir: dirB},
		}

		dirs := []string{""}
		for _, spec := range stageSpecs {
			d, err := store.StageBlock(ctx, spec.sliceID, 0, spec.data)
			require.NoError(t, err)
			assert.Equal(t, spec.expDir, d)
			dirs = append(dirs, d)
		}
		assert.Empty(t, syncedDirs, "已缓存目录下 StageBlock 不应提前触发目录 Sync")

		// 批量去重同步目录：dirA 与 dirB 按首次出现顺序各同步 1 次
		require.NoError(t, store.SyncDirs(ctx, dirs))
		assert.Equal(t, []string{filepath.Clean(dirA), filepath.Clean(dirB)}, syncedDirs)

		// 验证所有块内容可读且无残留 .tmp_* 文件
		for _, spec := range stageSpecs {
			got := make([]byte, len(spec.data))
			require.NoError(t, store.GetBlock(ctx, spec.sliceID, 0, uint32(len(spec.data)), 0, got))
			assert.Equal(t, spec.data, got)
		}
		for _, checkDir := range []string{dirA, dirB} {
			entries, err := os.ReadDir(checkDir)
			require.NoError(t, err)
			for _, e := range entries {
				assert.False(t, strings.HasPrefix(e.Name(), ".tmp_"), "目录 %s 下不应残留临时文件 %s", checkDir, e.Name())
			}
		}

		// 验证 SyncDirs 失败时返回 ErrIO
		failSyncForDir = dirB
		err := store.SyncDirs(ctx, []string{dirA, dirB})
		require.ErrorIs(t, err, meta.ErrIO)

		// 验证 Context 取消时 StageBlock、SyncDirs 与 SyncPending 立即返回 context.Canceled
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, err = store.StageBlock(canceledCtx, 1034, 0, []byte("canceled"))
		require.ErrorIs(t, err, context.Canceled)
		err = store.SyncDirs(canceledCtx, []string{dirA})
		require.ErrorIs(t, err, context.Canceled)
		err = store.SyncPending(canceledCtx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestLocalFSBlockStore_SyncPending_NoopCoalesceAndRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("无新写入时重复 SyncPending 零磁盘操作，StageBlock 后 SyncPending 精确同步且回滚删除文件忽略 ErrNotExist，失败后保留状态支持重试", func(t *testing.T) {
		dataDir := t.TempDir()
		var syncMu sync.Mutex
		var syncedDirs []string
		var failSyncForDir string

		store := meta.NewLocalFSBlockStore(
			dataDir,
			meta.WithOpenDirHook(func(dir string) (meta.DirHandle, error) {
				f, err := os.Open(dir)
				if err != nil {
					return nil, err
				}
				return &spyDirHandle{
					file: f,
					dir:  filepath.Clean(dir),
					onSync: func(cleaned string, file *os.File) error {
						syncMu.Lock()
						syncedDirs = append(syncedDirs, cleaned)
						failDir := failSyncForDir
						syncMu.Unlock()
						if failDir != "" && cleaned == filepath.Clean(failDir) {
							return errors.New("injected syncPending dir failure")
						}
						return file.Sync()
					},
				}, nil
			}),
		)

		// 1. 初始无写入时 SyncPending 直接返回 nil 且零目录 Sync
		require.NoError(t, store.SyncPending(ctx))
		syncMu.Lock()
		assert.Empty(t, syncedDirs)
		syncMu.Unlock()

		// 2. 预热散列目录 chunks/0C/0 (sliceID%256==12) 与 chunks/0D/0 (sliceID%256==13)
		_, err := store.StageBlock(ctx, 12, 0, []byte("warmup-12"))
		require.NoError(t, err)
		_, err = store.StageBlock(ctx, 13, 0, []byte("warmup-13"))
		require.NoError(t, err)
		require.NoError(t, store.SyncPending(ctx))

		dir12 := filepath.Clean(filepath.Join(dataDir, "chunks", "0C", "0"))
		dir13 := filepath.Clean(filepath.Join(dataDir, "chunks", "0D", "0"))
		syncMu.Lock()
		assert.Contains(t, syncedDirs, dir12)
		assert.Contains(t, syncedDirs, dir13)
		syncedDirs = nil
		syncMu.Unlock()

		// 3. 无新写入时连续重复调用 SyncPending，不触发任何冗余刷盘
		require.NoError(t, store.SyncPending(ctx))
		require.NoError(t, store.SyncPending(ctx))
		syncMu.Lock()
		assert.Empty(t, syncedDirs, "无新写入时重复调用 SyncPending 不得触发冗余磁盘同步")
		syncMu.Unlock()

		// 4. 在已预热目录下 StageBlock 写入 3 个块，其中 1 个通过 DeleteBlock 删除、1 个被外部 os.Remove 删除（模拟回滚清理）
		_, err = store.StageBlock(ctx, 12+256, 0, []byte("block-12-keep"))
		require.NoError(t, err)
		_, err = store.StageBlock(ctx, 12+512, 0, []byte("block-12-deleted-via-api"))
		require.NoError(t, err)
		_, err = store.StageBlock(ctx, 13+256, 0, []byte("block-13-unlinked"))
		require.NoError(t, err)

		// StageBlock 热路径不触发叶子目录 Sync
		syncMu.Lock()
		assert.Empty(t, syncedDirs, "StageBlock 仅发布至 OS Page Cache，不触发叶子目录 Sync")
		syncMu.Unlock()

		require.NoError(t, store.DeleteBlock(ctx, 12+512, 0, uint32(len("block-12-deleted-via-api"))))
		unlinkedPath := filepath.Join(dir13, fmt.Sprintf("%d_0_%d", 13+256, len("block-13-unlinked")))
		require.NoError(t, os.Remove(unlinkedPath))

		// 5. 注入 dir13 同步故障：首次 SyncPending 应返回 ErrIO，且保留待刷批次供后续重试
		syncMu.Lock()
		failSyncForDir = dir13
		syncMu.Unlock()

		err = store.SyncPending(ctx)
		require.ErrorIs(t, err, meta.ErrIO, "SyncPending 失败时必须返回包装 ErrIO 的错误")

		// 6. 解除故障后再次调用 SyncPending：上次未完成的批次必须重试并成功刷盘
		syncMu.Lock()
		failSyncForDir = ""
		syncedDirs = nil
		syncMu.Unlock()

		require.NoError(t, store.SyncPending(ctx))
		syncMu.Lock()
		assert.Equal(t, []string{dir12, dir13}, syncedDirs, "失败重试后必须完整刷入待刷目录")
		syncedDirs = nil
		syncMu.Unlock()

		// 7. 刷盘成功后再次调用 SyncPending 應为幂等空操作
		require.NoError(t, store.SyncPending(ctx))
		syncMu.Lock()
		assert.Empty(t, syncedDirs)
		syncMu.Unlock()
	})

	t.Run("多协程并发调用 SyncPending 自动合并等待同一次刷盘", func(t *testing.T) {
		dataDir := t.TempDir()
		var dirSyncCalls atomic.Int64
		enteredSyncCh := make(chan struct{})
		releaseSyncCh := make(chan struct{})
		var enterOnce sync.Once

		shardDir := filepath.Clean(filepath.Join(dataDir, "chunks", "0E", "0"))
		store := meta.NewLocalFSBlockStore(
			dataDir,
			meta.WithOpenDirHook(func(dir string) (meta.DirHandle, error) {
				f, err := os.Open(dir)
				if err != nil {
					return nil, err
				}
				return &spyDirHandle{
					file: f,
					dir:  filepath.Clean(dir),
					onSync: func(cleaned string, file *os.File) error {
						if cleaned == shardDir {
							dirSyncCalls.Add(1)
							enterOnce.Do(func() {
								close(enteredSyncCh)
							})
							<-releaseSyncCh
						}
						return file.Sync()
					},
				}, nil
			}),
		)

		// 先写入 1 个块使 writeGen = 1
		_, err := store.StageBlock(ctx, 14, 0, []byte("coalesce-payload"))
		require.NoError(t, err)

		const callers = 8
		var wg sync.WaitGroup
		errs := make([]error, callers)
		wg.Add(callers)

		// 启动第 1 个调用方进入刷盘并阻塞在 releaseSyncCh
		go func() {
			defer wg.Done()
			errs[0] = store.SyncPending(ctx)
		}()

		<-enteredSyncCh

		// 其余 7 个并发调用方在同一批次刷盘期间到达，应合并等待该轮完成
		for i := 1; i < callers; i++ {
			go func(idx int) {
				defer wg.Done()
				errs[idx] = store.SyncPending(ctx)
			}(i)
		}

		// 给并发调用方进入等待队列的时间窗口后放行
		close(releaseSyncCh)
		wg.Wait()

		for i, cErr := range errs {
			require.NoError(t, cErr, "并发调用方 %d 应成功返回", i)
		}
		assert.Equal(t, int64(1), dirSyncCalls.Load(), "8 个并发 SyncPending 针对同一写入批次必须合并为 1 次底层刷盘")
	})
}
