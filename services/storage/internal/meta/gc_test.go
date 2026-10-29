package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

// setupMockGCTxRepeated 配置 MockStore 支持多次事务透传。
func setupMockGCTxRepeated(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			return fn(mockQuerier)
		})
}

// TestCollectGarbage_Unit 单元测试 CollectGarbage 各种边界与容错逻辑。
func TestCollectGarbage_Unit(t *testing.T) {
	ctx := context.Background()

	t.Run("limit <= 0 时默认使用 DefaultGCBatchLimit (100) 且待回收列表为空时返回 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			ListGarbageSliceRefs(mock.Anything, meta.DefaultGCBatchLimit).
			Return(nil, nil).
			Once()

		reclaimed, err := mgr.CollectGarbage(ctx, 0)
		require.NoError(t, err)
		assert.Equal(t, 0, reclaimed)
		mockBS.AssertNotCalled(t, "DeleteBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("非法 slice_id 或零大小异常记录直接删除元数据行", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		garbageRows := []db.SliceRef{
			{SliceID: -1, Size: 4096, Refs: 0},
			{SliceID: 10, Size: 0, Refs: 0},
		}

		mockQuerier.EXPECT().
			ListGarbageSliceRefs(mock.Anything, int32(50)).
			Return(garbageRows, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteSliceRef(mock.Anything, int64(-1)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			DeleteSliceRef(mock.Anything, int64(10)).
			Return(nil).
			Once()

		reclaimed, err := mgr.CollectGarbage(ctx, 50)
		require.NoError(t, err)
		assert.Equal(t, 2, reclaimed)
		mockBS.AssertNotCalled(t, "DeleteBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("多 Block 大切片完整删除所有子块后清理 slice_refs", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		// 10 MiB 大小：含 3 个块（4 MiB, 4 MiB, 2 MiB）
		sliceSize := int32(10 * 1024 * 1024)
		sliceID := int64(1001)

		garbageRows := []db.SliceRef{
			{SliceID: sliceID, Size: sliceSize, Refs: 0},
		}

		mockQuerier.EXPECT().
			ListGarbageSliceRefs(mock.Anything, meta.DefaultGCBatchLimit).
			Return(garbageRows, nil).
			Once()

		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(sliceID), uint32(0), uint32(meta.BlockSize)).
			Return(nil).
			Once()
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(sliceID), uint32(1), uint32(meta.BlockSize)).
			Return(nil).
			Once()
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(sliceID), uint32(2), uint32(2*1024*1024)).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			DeleteSliceRef(mock.Anything, sliceID).
			Return(nil).
			Once()

		reclaimed, err := mgr.CollectGarbage(ctx, -5)
		require.NoError(t, err)
		assert.Equal(t, 1, reclaimed)
	})

	t.Run("关键容错：物理块删除失败跳过清理元数据且不回滚已成功的其它切片", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		// 3 个待回收切片：
		// slice 1: 成功回收
		// slice 2: 物理块删除失败（磁盘故障） -> 跳过 DeleteSliceRef(2)
		// slice 3: 成功回收
		garbageRows := []db.SliceRef{
			{SliceID: 101, Size: 4096, Refs: 0},
			{SliceID: 102, Size: 4096, Refs: 0},
			{SliceID: 103, Size: 4096, Refs: 0},
		}

		mockQuerier.EXPECT().
			ListGarbageSliceRefs(mock.Anything, int32(10)).
			Return(garbageRows, nil).
			Once()

		// slice 101: 成功
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(101), uint32(0), uint32(4096)).
			Return(nil).
			Once()
		mockQuerier.EXPECT().
			DeleteSliceRef(mock.Anything, int64(101)).
			Return(nil).
			Once()

		// slice 102: 失败
		diskErr := errors.New("nvme disk write error: I/O failure")
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(102), uint32(0), uint32(4096)).
			Return(diskErr).
			Once()
		// 严防调用 DeleteSliceRef(102)
		mockQuerier.AssertNotCalled(t, "DeleteSliceRef", mock.Anything, int64(102))

		// slice 103: 成功
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(103), uint32(0), uint32(4096)).
			Return(nil).
			Once()
		mockQuerier.EXPECT().
			DeleteSliceRef(mock.Anything, int64(103)).
			Return(nil).
			Once()

		reclaimed, err := mgr.CollectGarbage(ctx, 10)
		require.Error(t, err)
		assert.ErrorIs(t, err, diskErr)
		assert.Equal(t, 2, reclaimed, "成功回收了 101 与 103 两个切片")
	})

	t.Run("故障恢复后再次重试成功清除 slice_refs", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		garbageRows := []db.SliceRef{
			{SliceID: 102, Size: 4096, Refs: 0},
		}

		mockQuerier.EXPECT().
			ListGarbageSliceRefs(mock.Anything, int32(10)).
			Return(garbageRows, nil).
			Once()

		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(102), uint32(0), uint32(4096)).
			Return(nil).
			Once()
		mockQuerier.EXPECT().
			DeleteSliceRef(mock.Anything, int64(102)).
			Return(nil).
			Once()

		reclaimed, err := mgr.CollectGarbage(ctx, 10)
		require.NoError(t, err)
		assert.Equal(t, 1, reclaimed)
	})

	t.Run("ListGarbageSliceRefs 数据库错误透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db connection failure")
		mockQuerier.EXPECT().
			ListGarbageSliceRefs(mock.Anything, int32(10)).
			Return(nil, dbErr).
			Once()

		reclaimed, err := mgr.CollectGarbage(ctx, 10)
		require.Error(t, err)
		assert.ErrorIs(t, err, dbErr)
		assert.Equal(t, 0, reclaimed)
	})

	t.Run("WithGCOrphanGracePeriod 自定义安全宽限期对账回收单元测试", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		dataDir := t.TempDir()
		localBS := meta.NewLocalFSBlockStore(dataDir)
		customGrace := 10 * time.Minute
		mgr := meta.New(mockStore, meta.WithBlockStore(localBS), meta.WithGCOrphanGracePeriod(customGrace))

		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			ListGarbageSliceRefs(mock.Anything, int32(20)).
			Return(nil, nil).
			Once()

		// 写入 3 个物理块：
		// 1) slice 501: mtime 15 分钟前（超过 10 分钟宽限期），在 slice_refs 中无记录 -> 应被删除
		// 2) slice 502: mtime 5 分钟前（未超过 10 分钟宽限期），在 slice_refs 中无记录 -> 保留
		// 3) slice 503: mtime 15 分钟前（超过 10 分钟宽限期），但在 slice_refs 中已登记 refs=1 -> 保留
		require.NoError(t, localBS.PutBlock(ctx, 501, 0, []byte("orphan-old")))
		require.NoError(t, localBS.PutBlock(ctx, 502, 0, []byte("in-flight-fresh")))
		require.NoError(t, localBS.PutBlock(ctx, 503, 0, []byte("registered-old")))

		path501 := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", 501%256), "0", fmt.Sprintf("501_0_%d", len("orphan-old")))
		path502 := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", 502%256), "0", fmt.Sprintf("502_0_%d", len("in-flight-fresh")))
		path503 := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", 503%256), "0", fmt.Sprintf("503_0_%d", len("registered-old")))

		fifteenMinAgo := time.Now().Add(-15 * time.Minute)
		fiveMinAgo := time.Now().Add(-5 * time.Minute)
		require.NoError(t, os.Chtimes(path501, fifteenMinAgo, fifteenMinAgo))
		require.NoError(t, os.Chtimes(path502, fiveMinAgo, fiveMinAgo))
		require.NoError(t, os.Chtimes(path503, fifteenMinAgo, fifteenMinAgo))

		mockStore.EXPECT().
			GetSliceRef(mock.Anything, int64(501)).
			Return(db.SliceRef{}, pgx.ErrNoRows).
			Once()
		mockStore.EXPECT().
			GetSliceRef(mock.Anything, int64(503)).
			Return(db.SliceRef{SliceID: 503, Size: int32(len("registered-old")), Refs: 1}, nil).
			Once()

		reclaimed, err := mgr.CollectGarbage(ctx, 20)
		require.NoError(t, err)
		assert.Equal(t, 1, reclaimed)

		assert.NoFileExists(t, path501, "超过 10 分钟自定义宽限期且未登记的孤儿块应被删除")
		assert.FileExists(t, path502, "未满 10 分钟自定义宽限期的在途块必须保留")
		assert.FileExists(t, path503, "已在 slice_refs 登记的物理块必须保留")
	})
}

// TestStartGCWorker_Unit 验证 StartGCWorker 后台周期协程的运行与退出机制。
func TestStartGCWorker_Unit(t *testing.T) {
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	var callCount atomic.Int32

	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			callCount.Add(1)
			return fn(mockQuerier)
		}).
		Maybe()

	mockQuerier.EXPECT().
		ListGarbageSliceRefs(mock.Anything, int32(5)).
		Return(nil, nil).
		Maybe()

	workerCtx, cancel := context.WithCancel(context.Background())
	mgr.StartGCWorker(workerCtx, 5*time.Millisecond, 5)

	// 等待几次周期触发
	assert.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, 500*time.Millisecond, 10*time.Millisecond)

	// 取消 context 并确保平滑退出
	cancel()
	currentCalls := callCount.Load()
	time.Sleep(30 * time.Millisecond)
	// 验证退出后不再频繁增加
	assert.InDelta(t, currentCalls, callCount.Load(), 1)
}

// setupPostgresIntegration 初始化真实 PostgreSQL 连接并遵循统一 DSN 配置契约。
func setupPostgresIntegration(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	return setupMetaTestDB(t)
}

// faultInjectBlockStore 包装 BlockStore，支持针对指定 sliceID 注入删除错误，并透传 BlockLister 扫描能力。
type faultInjectBlockStore struct {
	meta.BlockStore
	failSliceID atomic.Uint64
}

func (f *faultInjectBlockStore) DeleteBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error {
	if f.failSliceID.Load() == sliceID {
		return fmt.Errorf("%w: simulated disk hardware failure on slice %d", meta.ErrIO, sliceID)
	}
	return f.BlockStore.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
}

func (f *faultInjectBlockStore) ListBlocksBefore(ctx context.Context, cutoff time.Time, limit int) ([]meta.PhysicalBlockFile, error) {
	if lister, ok := f.BlockStore.(meta.BlockLister); ok {
		return lister.ListBlocksBefore(ctx, cutoff, limit)
	}
	return nil, nil
}

// TestGC_PostgresIntegration 在真实 PostgreSQL 环境下结合 LocalFSBlockStore 进行完整生命周期端到端集成测试。
func TestGC_PostgresIntegration(t *testing.T) {
	pool, ctx := setupPostgresIntegration(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	rawBlockStore := meta.NewLocalFSBlockStore(dataDir)
	faultBS := &faultInjectBlockStore{BlockStore: rawBlockStore}
	mgr := meta.New(store, meta.WithBlockStore(faultBS))

	// 启动集成测试前排空历史存量垃圾切片，确保基线纯净
	for {
		n, err := mgr.CollectGarbage(ctx, 500)
		if err != nil || n == 0 {
			break
		}
	}

	// helper: 计算物理块在 LocalFS 中的路径
	blockFilePath := func(sliceID uint64, blockIdx uint32, blockSize uint32) string {
		return filepath.Join(
			dataDir,
			"chunks",
			fmt.Sprintf("%02X", sliceID%256),
			fmt.Sprintf("%d", sliceID/1000/1000),
			fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize),
		)
	}

	t.Run("生命周期场景 1: Open(O_TRUNC) 截断后触发 CollectGarbage 清理物理文件与 slice_refs", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_trunc_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 10 * 1024 * 1024})
		require.NoError(t, err)
		defer func() { _ = mgr.DeleteDisk(context.Background(), disk.DiskID) }()

		// 创建并写入文件
		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_trunc.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		payload := bytes.Repeat([]byte("T"), 8192)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)

		// 获取写入的 slice_id
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		require.NotEmpty(t, slices)
		sliceID := slices[0].ID

		// 断言物理文件存在且 slice_refs.refs == 1
		pPath := blockFilePath(sliceID, 0, 8192)
		require.FileExists(t, pPath, "写入后物理 Block 文件必须存在")
		refRow, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refRow.Refs)

		// 触发 Open(O_TRUNC)
		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   fRes.Inode.Ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})
		require.NoError(t, err)
		require.NotNil(t, openRes)

		// 此时 chunks 行已被清空，slice_refs.refs 已归零，但物理文件尚未被 GC 删除
		refRowAfterTrunc, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRowAfterTrunc.Refs)
		require.FileExists(t, pPath, "GC 执行前物理 Block 文件依然保留")

		// 执行 GC
		reclaimed, err := mgr.CollectGarbage(ctx, 100)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, reclaimed, 1)

		// 断言：物理文件被删除，slice_refs 表中对应记录已被彻底清除
		assert.NoFileExists(t, pPath, "GC 回收后物理 Block 文件必须被删除")
		_, err = store.GetSliceRef(ctx, int64(sliceID))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "GC 回收后 slice_refs 记录必须被删除")
	})

	t.Run("生命周期场景 2: SetAttr 非零缩容截断后触发 CollectGarbage 清理多余切片", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_setattr_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 200 * 1024 * 1024})
		require.NoError(t, err)
		defer func() { _ = mgr.DeleteDisk(context.Background(), disk.DiskID) }()

		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_setattr.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		// 跨越 64 MiB Chunk 边界写入两个 Chunk (indx 0 与 indx 1)
		// 写入 indx 0
		payload1 := bytes.Repeat([]byte("A"), 4096)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: 0,
			Data:   payload1,
		})
		require.NoError(t, err)

		// 写入 indx 1
		payload2 := bytes.Repeat([]byte("B"), 4096)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: meta.ChunkSize,
			Data:   payload2,
		})
		require.NoError(t, err)

		chunk1Row, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 1})
		require.NoError(t, err)
		slices1, err := meta.DecodeSlices(chunk1Row.Slices)
		require.NoError(t, err)
		sliceID1 := slices1[0].ID
		pPath1 := blockFilePath(sliceID1, 0, 4096)
		require.FileExists(t, pPath1)

		// SetAttr 缩容回 4096（完全截断 indx 1）
		newSize := uint64(4096)
		_, err = mgr.SetAttr(ctx, fRes.Inode.Ino, meta.SetAttrInput{Size: &newSize})
		require.NoError(t, err)

		// 断言 sliceID1 引用归零
		refRow, err := store.GetSliceRef(ctx, int64(sliceID1))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRow.Refs)

		// 执行 GC
		reclaimed, err := mgr.CollectGarbage(ctx, 100)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, reclaimed, 1)

		// 验证 sliceID1 物理文件被删除，元数据被清除
		assert.NoFileExists(t, pPath1)
		_, err = store.GetSliceRef(ctx, int64(sliceID1))
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})

	t.Run("生命周期场景 3: Fallocate 整块打洞后触发 CollectGarbage 回收物理块", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_punch_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 200 * 1024 * 1024})
		require.NoError(t, err)
		defer func() { _ = mgr.DeleteDisk(context.Background(), disk.DiskID) }()

		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_punch.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		// 将文件扩容至 2 个 Chunk (128 MiB)
		newSize := uint64(2 * meta.ChunkSize)
		_, err = mgr.SetAttr(ctx, fRes.Inode.Ino, meta.SetAttrInput{Size: &newSize})
		require.NoError(t, err)

		// 在 indx 1 (Offset: ChunkSize) 写入一个切片
		payload := bytes.Repeat([]byte("P"), 8192)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: meta.ChunkSize,
			Data:   payload,
		})
		require.NoError(t, err)

		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 1})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		sliceID := slices[0].ID
		pPath := blockFilePath(sliceID, 0, 8192)
		require.FileExists(t, pPath)

		// 对 indx 1 的整整 64 MiB 区间 [ChunkSize, 2*ChunkSize) 打洞
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			Offset: meta.ChunkSize,
			Length: meta.ChunkSize,
		})
		require.NoError(t, err)

		// 断言 refs 归零
		refRow, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRow.Refs)

		// 执行 GC
		reclaimed, err := mgr.CollectGarbage(ctx, 100)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, reclaimed, 1)

		assert.NoFileExists(t, pPath)
		_, err = store.GetSliceRef(ctx, int64(sliceID))
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})

	t.Run("生命周期场景 4: Unlink + Release 孤儿文件后触发 CollectGarbage 清理物理块", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_unlink_release_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 100 * 1024 * 1024})
		require.NoError(t, err)
		defer func() { _ = mgr.DeleteDisk(context.Background(), disk.DiskID) }()

		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_unlink.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		payload := bytes.Repeat([]byte("U"), 4096)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)

		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		sliceID := slices[0].ID
		pPath := blockFilePath(sliceID, 0, 4096)
		require.FileExists(t, pPath)

		// Unlink 成为孤儿节点（持有打开句柄）
		err = mgr.Unlink(ctx, disk.RootIno, []byte("file_unlink.bin"))
		require.NoError(t, err)

		// 释放最后句柄
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: fRes.Inode.Ino, Fh: fRes.Fh})
		require.NoError(t, err)

		// 断言 refs 归零
		refRow, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRow.Refs)

		// 执行 GC
		reclaimed, err := mgr.CollectGarbage(ctx, 100)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, reclaimed, 1)

		assert.NoFileExists(t, pPath)
		_, err = store.GetSliceRef(ctx, int64(sliceID))
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})

	t.Run("生命周期场景 5: DeleteDisk 级联销毁整盘后触发 CollectGarbage 清理全部物理块", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_deletedisk_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 100 * 1024 * 1024})
		require.NoError(t, err)

		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_disk.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		payload := bytes.Repeat([]byte("D"), 4096)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)

		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		sliceID := slices[0].ID
		pPath := blockFilePath(sliceID, 0, 4096)
		require.FileExists(t, pPath)

		// 销毁整盘
		err = mgr.DeleteDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		refRow, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRow.Refs)

		// 执行 GC
		reclaimed, err := mgr.CollectGarbage(ctx, 100)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, reclaimed, 1)

		assert.NoFileExists(t, pPath)
		_, err = store.GetSliceRef(ctx, int64(sliceID))
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})

	t.Run("保护活跃切片：refs > 0 的切片绝不被 GC 误删", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_active_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 100 * 1024 * 1024})
		require.NoError(t, err)
		defer func() { _ = mgr.DeleteDisk(context.Background(), disk.DiskID) }()

		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_active.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		payload := bytes.Repeat([]byte("ACTIVE"), 500)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)

		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		sliceID := slices[0].ID
		pPath := blockFilePath(sliceID, 0, uint32(len(payload)))
		require.FileExists(t, pPath)

		// 执行 GC
		reclaimed, err := mgr.CollectGarbage(ctx, 100)
		require.NoError(t, err)
		// 活跃切片不应被回收
		_ = reclaimed

		// 验证活跃切片依然完好
		require.FileExists(t, pPath, "活跃切片的物理文件绝不能被误删")
		refRow, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refRow.Refs)

		// 测试收尾显式清理该切片，防止在共享数据库中残留
		_ = store.DeleteSliceRef(context.Background(), int64(sliceID))
	})

	t.Run("磁盘删除错误时保留 slice_refs 且故障恢复后重试彻底清除", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_retry_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 100 * 1024 * 1024})
		require.NoError(t, err)
		defer func() { _ = mgr.DeleteDisk(context.Background(), disk.DiskID) }()

		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_retry.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		payload := []byte("fault-retry-data")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)

		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		sliceID := slices[0].ID
		pPath := blockFilePath(sliceID, 0, uint32(len(payload)))
		require.FileExists(t, pPath)

		// 截断使 refs 归零
		_, err = mgr.Open(ctx, meta.OpenParams{Ino: fRes.Inode.Ino, Flags: meta.O_RDWR | meta.O_TRUNC})
		require.NoError(t, err)

		// 注入物理删除硬件故障
		faultBS.failSliceID.Store(sliceID)

		// 执行 GC：预期捕获磁盘错误
		reclaimed, err := mgr.CollectGarbage(ctx, 100)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrIO)
		_ = reclaimed

		// 核心断言：物理文件依然存在，数据库 slice_refs 依然被保留 (refs == 0)
		require.FileExists(t, pPath, "删除失败后物理文件必须保留")
		refRowAfterFail, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRowAfterFail.Refs, "slice_refs 必须保留以便下次重试")

		// 故障恢复
		faultBS.failSliceID.Store(0)

		// 再次执行 GC
		reclaimedRetry, err := mgr.CollectGarbage(ctx, 100)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, reclaimedRetry, 1)

		// 核心断言：故障恢复后物理文件被删除，slice_refs 记录彻底清除
		assert.NoFileExists(t, pPath, "重试成功后物理文件被清除")
		_, err = store.GetSliceRef(ctx, int64(sliceID))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "重试成功后 slice_refs 彻底被清除")
	})

	t.Run("后台 StartGCWorker 周期自动清理验证", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_worker_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 100 * 1024 * 1024})
		require.NoError(t, err)
		defer func() { _ = mgr.DeleteDisk(context.Background(), disk.DiskID) }()

		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_worker.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		payload := []byte("worker-periodic-data")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)

		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		sliceID := slices[0].ID
		pPath := blockFilePath(sliceID, 0, uint32(len(payload)))
		require.FileExists(t, pPath)

		// 启动后台 GC Worker，每 20ms 执行一次
		workerCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mgr.StartGCWorker(workerCtx, 20*time.Millisecond, 50)

		// 截断文件使 refs 归零
		_, err = mgr.Open(ctx, meta.OpenParams{Ino: fRes.Inode.Ino, Flags: meta.O_RDWR | meta.O_TRUNC})
		require.NoError(t, err)

		// 轮询等待后台 Worker 自动清除物理文件与 slice_refs 记录
		assert.Eventually(t, func() bool {
			if _, err := os.Stat(pPath); !os.IsNotExist(err) {
				return false
			}
			_, err := store.GetSliceRef(ctx, int64(sliceID))
			return errors.Is(err, pgx.ErrNoRows)
		}, 3*time.Second, 50*time.Millisecond, "后台 Worker 必须在周期运行中自动清除垃圾切片与物理文件")
	})

	t.Run("生命周期场景 6: 后台 GC 带宽限期未登记孤儿物理块扫描与对账回收", func(t *testing.T) {
		diskName := fmt.Sprintf("test_gc_orphan_blocks_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 100 * 1024 * 1024})
		require.NoError(t, err)
		defer func() { _ = mgr.DeleteDisk(context.Background(), disk.DiskID) }()

		// 1) 创建并正常写入一个有效文件（在 slice_refs 中登记 refs = 1），随后将其物理文件 mtime 改为 2 小时前
		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_registered_old.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		registeredPayload := bytes.Repeat([]byte("R"), 4096)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fRes.Inode.Ino,
			Fh:     fRes.Fh,
			Offset: 0,
			Data:   registeredPayload,
		})
		require.NoError(t, err)

		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fRes.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		require.NotEmpty(t, slices)
		registeredSliceID := slices[0].ID
		registeredPath := blockFilePath(registeredSliceID, 0, uint32(len(registeredPayload)))
		require.FileExists(t, registeredPath)

		twoHoursAgo := time.Now().Add(-2 * time.Hour)
		require.NoError(t, os.Chtimes(registeredPath, twoHoursAgo, twoHoursAgo))

		// 2) 直接通过 BlockStore.PutBlock 写入一个未在 slice_refs 中登记的陈旧孤儿物理块，并将其 mtime 设为 2 小时前
		staleOrphanSliceID := uint64(900000001)
		_, err = store.GetSliceRef(ctx, int64(staleOrphanSliceID))
		require.ErrorIs(t, err, pgx.ErrNoRows)

		staleOrphanPayload := bytes.Repeat([]byte("O"), 4096)
		require.NoError(t, rawBlockStore.PutBlock(ctx, staleOrphanSliceID, 0, staleOrphanPayload))
		staleOrphanPath := blockFilePath(staleOrphanSliceID, 0, uint32(len(staleOrphanPayload)))
		require.FileExists(t, staleOrphanPath)
		require.NoError(t, os.Chtimes(staleOrphanPath, twoHoursAgo, twoHoursAgo))

		// 3) 直接通过 BlockStore.PutBlock 写入一个未超宽限期（刚创建）的未登记在途物理块文件
		freshInFlightSliceID := uint64(900000002)
		_, err = store.GetSliceRef(ctx, int64(freshInFlightSliceID))
		require.ErrorIs(t, err, pgx.ErrNoRows)

		freshPayload := bytes.Repeat([]byte("F"), 4096)
		require.NoError(t, rawBlockStore.PutBlock(ctx, freshInFlightSliceID, 0, freshPayload))
		freshPath := blockFilePath(freshInFlightSliceID, 0, uint32(len(freshPayload)))
		require.FileExists(t, freshPath)
		defer func() {
			_ = rawBlockStore.DeleteBlock(context.Background(), freshInFlightSliceID, 0, uint32(len(freshPayload)))
		}()

		// 4) 调用 CollectGarbage 执行对账回收（默认宽限期 1 小时）
		reclaimed, err := mgr.CollectGarbage(ctx, 100)
		require.NoError(t, err)
		assert.Equal(t, 1, reclaimed, "应恰好回收 1 个超宽限期的未登记孤儿物理块")

		// 断言：仅超宽限期且未登记的孤儿物理块被物理删除，未超宽限期的在途物理块与已登记 refs=1 的有效物理块均完好保留
		assert.NoFileExists(t, staleOrphanPath, "超宽限期且未在 slice_refs 登记的孤儿物理块必须被物理删除")
		assert.FileExists(t, freshPath, "未超宽限期的在途未登记物理块绝不能被误删")
		assert.FileExists(t, registeredPath, "已在 slice_refs 登记 refs=1 的有效物理块（即使 mtime 超过 2 小时）绝不能被误删")

		refRow, err := store.GetSliceRef(ctx, int64(registeredSliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refRow.Refs)

		// 收尾清理已登记切片
		_ = rawBlockStore.DeleteBlock(context.Background(), registeredSliceID, 0, uint32(len(registeredPayload)))
		_ = store.DeleteSliceRef(context.Background(), int64(registeredSliceID))
	})
}
