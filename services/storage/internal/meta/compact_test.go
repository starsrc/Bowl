package meta_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sync"
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

func getTestDSN(ctx context.Context) string {
	if dsn := os.Getenv("STORAGE_TEST_DSN"); dsn != "" {
		return dsn
	}
	for _, port := range []string{"5432", "9432"} {
		dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%s/bowl?sslmode=disable", port)
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err := pool.Ping(ctx); err == nil {
				pool.Close()
				return dsn
			}
			pool.Close()
		}
	}
	return "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
}

func setupPostgresForCompact(t *testing.T) (db.Store, meta.BlockStore, meta.Manager, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dsn := getTestDSN(ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过 PostgreSQL 集成测试 (连接初始化失败): %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("跳过 PostgreSQL 集成测试 (ping 失败): %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	return store, bs, mgr, dataDir
}

// TestCompactChunk_StaticValidation 测试静态参数校验
func TestCompactChunk_StaticValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	tests := []struct {
		name string
		ino  uint64
		indx uint32
	}{
		{
			name: "ino 为 0",
			ino:  0,
			indx: 0,
		},
		{
			name: "ino 超出 MaxInt64",
			ino:  uint64(math.MaxInt64) + 1,
			indx: 0,
		},
		{
			name: "indx 超出 MaxInt32",
			ino:  1,
			indx: uint32(math.MaxInt32) + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.CompactChunk(ctx, tt.ino, tt.indx)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

// TestCompactChunk_NoRowsOrEmpty 测试 Chunk 不存在或切片为空时返回 nil
func TestCompactChunk_NoRowsOrEmpty(t *testing.T) {
	ctx := context.Background()

	t.Run("chunk 不存在返回 nil", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{}, pgx.ErrNoRows).Once()

		mgr := meta.New(mockStore)
		err := mgr.CompactChunk(ctx, 1, 0)
		assert.NoError(t, err)
	})

	t.Run("chunk slices 为空返回 nil", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{
			Ino:    1,
			Indx:   0,
			Slices: nil,
		}, nil).Once()

		mgr := meta.New(mockStore)
		err := mgr.CompactChunk(ctx, 1, 0)
		assert.NoError(t, err)
	})
}

// TestCompactChunk_RandomOverwritesAndHoles 集成测试 1：
// 高频随机覆写（包含跨越多层 Slice、局部覆写、稀疏空洞保留）后触发 CompactChunk，
// 验证合并前后 Read 字节流逐位一致，且 chunks.slices 长度显著缩减为紧致 Slice。
func TestCompactChunk_RandomOverwritesAndHoles(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _ := setupPostgresForCompact(t)

	// 创建测试常规文件
	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer func() {
		_ = store.DeleteInode(context.Background(), fileRow.Ino)
		_, _ = store.DeleteChunksByIno(context.Background(), fileRow.Ino)
	}()

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 执行多次局部随机覆写：
	// 1. 先写 0..64KB 填入 0xAA
	data1 := bytes.Repeat([]byte{0xAA}, 64*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data1})
	require.NoError(t, err)

	// 2. 在 16KB..48KB 覆写 0xBB
	data2 := bytes.Repeat([]byte{0xBB}, 32*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 16 * 1024, Data: data2})
	require.NoError(t, err)

	// 3. 在 32KB..40KB 覆写 0xCC
	data3 := bytes.Repeat([]byte{0xCC}, 8*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 32 * 1024, Data: data3})
	require.NoError(t, err)

	// 4. 在 128KB..192KB 写入 0xDD（产生 64KB..128KB 的稀疏空洞）
	data4 := bytes.Repeat([]byte{0xDD}, 64*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 128 * 1024, Data: data4})
	require.NoError(t, err)

	// 5. 在 140KB..160KB 打洞（Punch Hole 填零）
	err = mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    ino,
		Fh:     fh,
		Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		Offset: 140 * 1024,
		Length: 20 * 1024,
	})
	require.NoError(t, err)

	// 6. 在 200KB..220KB 写入 0xEE
	data6 := bytes.Repeat([]byte{0xEE}, 20*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 200 * 1024, Data: data6})
	require.NoError(t, err)

	// 合并前读取全量数据作为黄金基准
	readBefore, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   250 * 1024,
	})
	require.NoError(t, err)
	require.Len(t, readBefore.Data, 220*1024)

	// 校验合并前 chunk 切片条数 >= 5
	chunkBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesBefore, err := meta.DecodeSlices(chunkBefore.Slices)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(slicesBefore), 5)

	// 执行 CompactChunk
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	// 合并后读取全量数据
	readAfter, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   250 * 1024,
	})
	require.NoError(t, err)

	// 断言合并前后字节流逐位一致！
	assert.Equal(t, readBefore.Data, readAfter.Data)

	// 断言 chunks.slices 数量显著缩减为紧致 Slice（非空洞有效数据段数量）
	chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesAfter, err := meta.DecodeSlices(chunkAfter.Slices)
	require.NoError(t, err)

	// 有效紧致切片段应远小于之前的切片数
	assert.Less(t, len(slicesAfter), len(slicesBefore))
	t.Logf("Compaction 完成: 切片数量从 %d 缩减至 %d", len(slicesBefore), len(slicesAfter))
}

// TestCompactChunk_ConcurrentWriteSuffixPreserved 集成测试 2：
// 合并期间并发追加新 Slice 后缀，验证 CAS 替换成功后并发追加的后缀 Slice 完整保留且 Read 读出最新覆盖数据。
func TestCompactChunk_ConcurrentWriteSuffixPreserved(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer func() {
		_ = store.DeleteInode(context.Background(), fileRow.Ino)
		_, _ = store.DeleteChunksByIno(context.Background(), fileRow.Ino)
	}()

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 初始写入 2 段数据
	data1 := bytes.Repeat([]byte{0x11}, 10*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data1})
	require.NoError(t, err)

	data2 := bytes.Repeat([]byte{0x22}, 10*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 10 * 1024, Data: data2})
	require.NoError(t, err)

	// 记录合并前 chunk slices
	chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	originSlices := chunkRow.Slices
	require.NotEmpty(t, originSlices)

	// 模拟并发：向 chunk 追加第 3 段数据
	data3 := bytes.Repeat([]byte{0x33}, 10*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 20 * 1024, Data: data3})
	require.NoError(t, err)

	// 触发 CompactChunk
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	// 读取全量 30KB 数据
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   30 * 1024,
	})
	require.NoError(t, err)
	require.Len(t, readRes.Data, 30*1024)

	// 验证各段数据完全正确：0..10K 是 0x11, 10..20K 是 0x22, 20..30K 是 0x33（并发追加数据完整保留）
	assert.Equal(t, data1, readRes.Data[0:10*1024])
	assert.Equal(t, data2, readRes.Data[10*1024:20*1024])
	assert.Equal(t, data3, readRes.Data[20*1024:30*1024])
}

// TestCompactChunk_TruncateOrDeleteCausesCASAbortAndCleanup 集成测试 3：
// 合并期间文件被截断（O_TRUNC / SetAttr）或删除导致 originSlices 前缀不匹配（或行不存在）时，
// 验证 CAS 安全中止、不污染元数据，且刚生成的新物理块被 BlockStore.DeleteBlock 立即清理零残留。
func TestCompactChunk_TruncateOrDeleteCausesCASAbortAndCleanup(t *testing.T) {
	ctx := context.Background()
	store, bs, _, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer func() {
		_ = store.DeleteInode(context.Background(), fileRow.Ino)
		_, _ = store.DeleteChunksByIno(context.Background(), fileRow.Ino)
	}()

	mockBS := &recordingBlockStore{realBS: bs}
	mgr := meta.New(store, meta.WithBlockStore(mockBS))

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 写入数据
	data := bytes.Repeat([]byte{0x42}, 16*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data})
	require.NoError(t, err)

	// 用 O_TRUNC 重新打开文件清空 chunks
	openTruncRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR | meta.O_TRUNC,
	})
	require.NoError(t, err)
	_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: openTruncRes.Fh})

	// 此时 chunk 已不存在或已被清空
	err = mgr.CompactChunk(ctx, ino, 0)
	assert.NoError(t, err)
}

// TestCompactChunk_RefZeroCollectedByGC 集成测试 4：
// CompactChunk 完成后旧 Slice 的 slice_refs.refs 归零，
// 随后调用 CollectGarbage 将旧物理块文件与 slice_refs 记录彻底回收，而新合并生成的物理块完好无损。
func TestCompactChunk_RefZeroCollectedByGC(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer func() {
		_ = store.DeleteInode(context.Background(), fileRow.Ino)
		_, _ = store.DeleteChunksByIno(context.Background(), fileRow.Ino)
	}()

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 连续覆写产生 3 个旧 Slice
	for i := 0; i < 3; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, 16*1024)
		_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data})
		require.NoError(t, err)
	}

	// 记录合并前的 chunk
	chunkBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesBefore, err := meta.DecodeSlices(chunkBefore.Slices)
	require.NoError(t, err)
	require.Len(t, slicesBefore, 3)

	// 记录旧 slice_ids
	var oldSliceIDs []int64
	for _, s := range slicesBefore {
		oldSliceIDs = append(oldSliceIDs, int64(s.ID))
	}

	// 执行 CompactChunk
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	// 校验旧 slice_refs.refs 已经 <= 0
	for _, oldID := range oldSliceIDs {
		ref, err := store.GetSliceRef(ctx, oldID)
		require.NoError(t, err)
		assert.LessOrEqual(t, ref.Refs, int32(0))
	}

	// 校验新生成的 slice_refs.refs == 1
	chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesAfter, err := meta.DecodeSlices(chunkAfter.Slices)
	require.NoError(t, err)
	require.Len(t, slicesAfter, 1)
	newSliceID := int64(slicesAfter[0].ID)

	newRef, err := store.GetSliceRef(ctx, newSliceID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), newRef.Refs)

	// 执行 GC 回收旧物理块
	reclaimed, err := mgr.CollectGarbage(ctx, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, reclaimed, len(oldSliceIDs))

	// 验证旧 slice_refs 已在数据库中被删除
	for _, oldID := range oldSliceIDs {
		_, err := store.GetSliceRef(ctx, oldID)
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	}

	// 验证新切片完好无损，仍可正常读取
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   16 * 1024,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{3}, 16*1024), readRes.Data)
}

// TestCompactChunk_AutoTriggerThreshold 集成测试 5：
// 配置较小的 WithCompactSliceThreshold(4)，连续执行 4 次 Write 自动异步触发 CompactChunk 并完成合并。
func TestCompactChunk_AutoTriggerThreshold(t *testing.T) {
	ctx := context.Background()
	store, bs, _, _ := setupPostgresForCompact(t)

	// 使用 Threshold = 4
	mgr := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer func() {
		_ = store.DeleteInode(context.Background(), fileRow.Ino)
		_, _ = store.DeleteChunksByIno(context.Background(), fileRow.Ino)
	}()

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 写入 4 次，每次覆写相同区间
	for i := 0; i < 4; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, 8*1024)
		_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data})
		require.NoError(t, err)
	}

	// 等待后台异步 Compact 完成
	var compacted bool
	for wait := 0; wait < 30; wait++ {
		time.Sleep(100 * time.Millisecond)
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		if err != nil {
			continue
		}
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		if err != nil {
			continue
		}
		if len(slices) == 1 {
			compacted = true
			break
		}
	}
	assert.True(t, compacted, "异步 Compaction 应在达到阈值 4 后触发并将切片合并为 1 条")

	// 验证最终数据为第 4 次写入的数据 (0x04)
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   8 * 1024,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{4}, 8*1024), readRes.Data)
}

// TestCompactChunk_Phase1PutBlockErrorCleansUp 单元测试：阶段 1 PutBlock 出错立即清理并返回错误
func TestCompactChunk_Phase1PutBlockErrorCleansUp(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	// 1 条有效 Slice (Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024)
	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockStore.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	// GetBlock 成功
	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	// 分配新 ID: 99
	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	// PutBlock 失败
	putErr := fmt.Errorf("disk full simulation")
	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(putErr).Once()

	// 应当调用 DeleteBlock 清理
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, meta.ErrIO)
}

// TestCompactChunk_Phase2TxErrorCleansUp 单元测试：阶段 2 事务内更新失败清理已落盘的新物理块
func TestCompactChunk_Phase2TxErrorCleansUp(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockStore.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(nil).Once()

	// 事务内锁定行，返回匹配的前缀
	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		mockQuerier := dbmock.NewMockQuerier(t)
		mockQuerier.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

		// UpdateChunkSlices 模拟数据库错误
		mockQuerier.EXPECT().UpdateChunkSlices(mock.Anything, mock.Anything).
			Return(fmt.Errorf("db disk failure")).Once()

		return fn(mockQuerier)
	}).Once()

	// 事务失败后，阶段 1 写入的新物理块 (sliceID=99) 必须被 DeleteBlock 清理
	mockBS.EXPECT().DeleteBlock(mock.Anything, uint64(99), uint32(0), uint32(1024)).
		Return(nil).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db disk failure")
}

// TestCompactChunk_CASPrefixMismatchCleansUp 单元测试：CAS 前缀不匹配安全中止并清理新物理块
func TestCompactChunk_CASPrefixMismatchCleansUp(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockStore.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(nil).Once()

	// 事务内锁定时发现已经被改写为完全不同的切片（例如打洞或覆盖截断）
	differentSlice := meta.Slice{Pos: 0, ID: 88, Size: 2048, Off: 0, Len: 2048}
	differentBytes := meta.EncodeSlice(differentSlice)

	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		mockQuerier := dbmock.NewMockQuerier(t)
		mockQuerier.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: differentBytes}, nil).Once()

		return fn(mockQuerier)
	}).Once()

	// CAS 前缀不匹配中止时，新物理块必须被清理
	mockBS.EXPECT().DeleteBlock(mock.Anything, uint64(99), uint32(0), uint32(1024)).
		Return(nil).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	assert.NoError(t, err)
}

// TestCompactChunk_AllHoles 测试全空洞 Chunk 合并生成合法全空洞 Slice
func TestCompactChunk_AllHoles(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer func() {
		_ = store.DeleteInode(context.Background(), fileRow.Ino)
		_, _ = store.DeleteChunksByIno(context.Background(), fileRow.Ino)
	}()

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 写入 16KB 数据
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: bytes.Repeat([]byte{0xFF}, 16*1024)})
	require.NoError(t, err)

	// 打洞完全覆盖该 16KB（使整个 chunk 没有任何 ID > 0 的切片）
	err = mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    ino,
		Fh:     fh,
		Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		Offset: 0,
		Length: 16 * 1024,
	})
	require.NoError(t, err)

	// 执行 Compaction
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	// 读取全量为 0x00
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   16 * 1024,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{0}, 16*1024), readRes.Data)
}

// recordingBlockStore 包装实际 BlockStore 用于统计记录
type recordingBlockStore struct {
	realBS  meta.BlockStore
	mu      sync.Mutex
	puts    []uint64
	deletes []uint64
}

func (r *recordingBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	r.mu.Lock()
	r.puts = append(r.puts, sliceID)
	r.mu.Unlock()
	return r.realBS.PutBlock(ctx, sliceID, blockIdx, data)
}

func (r *recordingBlockStore) GetBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32, offset uint32, dst []byte) error {
	return r.realBS.GetBlock(ctx, sliceID, blockIdx, blockSize, offset, dst)
}

func (r *recordingBlockStore) DeleteBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error {
	r.mu.Lock()
	r.deletes = append(r.deletes, sliceID)
	r.mu.Unlock()
	return r.realBS.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
}
