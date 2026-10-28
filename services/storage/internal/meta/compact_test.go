package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

func setupPostgresForCompact(t *testing.T) (db.Store, meta.BlockStore, meta.Manager, string, *pgxpool.Pool) {
	t.Helper()
	pool, _ := setupMetaTestDB(t)
	if pool == nil {
		return nil, nil, nil, "", nil
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	return store, bs, mgr, dataDir, pool
}

func cleanupCompactTestIno(ctx context.Context, store db.Store, ino int64) {
	for indx := int32(0); indx < 16; indx++ {
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: ino, Indx: indx})
		if err == nil && len(chunkRow.Slices) > 0 {
			if slices, err := meta.DecodeSlices(chunkRow.Slices); err == nil {
				for _, s := range slices {
					_ = store.DeleteSliceRef(ctx, int64(s.ID))
				}
			}
		}
	}
	_ = meta.DeleteAllChunksByInos(ctx, store, []int64{ino})
	_ = store.DeleteInode(ctx, ino)
}

// recordingBlockStore 包装实际 BlockStore 用于统计记录与注入回调
type recordingBlockStore struct {
	realBS           meta.BlockStore
	mu               sync.Mutex
	puts             []uint64
	deletes          []uint64
	onBeforePutBlock func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error
	onAfterPutBlock  func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error
}

func (r *recordingBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	if r.onBeforePutBlock != nil {
		if err := r.onBeforePutBlock(ctx, sliceID, blockIdx, data); err != nil {
			return err
		}
	}
	if err := r.realBS.PutBlock(ctx, sliceID, blockIdx, data); err != nil {
		return err
	}
	r.mu.Lock()
	r.puts = append(r.puts, sliceID)
	r.mu.Unlock()
	if r.onAfterPutBlock != nil {
		if err := r.onAfterPutBlock(ctx, sliceID, blockIdx, data); err != nil {
			return err
		}
	}
	return nil
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
		mockQuerier := dbmock.NewMockQuerier(t)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{}, pgx.ErrNoRows).Once()

		mgr := meta.New(mockStore)
		err := mgr.CompactChunk(ctx, 1, 0)
		assert.NoError(t, err)
	})

	t.Run("chunk slices 为空返回 nil", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
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
// 消除 Self-Oracle 自证：基于独立构造的内存黄金期望数据分别断言合并前后 Read 逐字节一致，
// 且 chunks.slices 长度显著缩减为紧致 Slice。
func TestCompactChunk_RandomOverwritesAndHoles(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _, _ := setupPostgresForCompact(t)

	// 创建测试常规文件
	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 独立构造期望内存黄金字节数组（220KB）
	expected := make([]byte, 220*1024)

	// 执行多次局部随机覆写：
	// 1. 先写 0..64KB 填入 0xAA
	data1 := bytes.Repeat([]byte{0xAA}, 64*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data1})
	require.NoError(t, err)
	copy(expected[0:64*1024], data1)

	// 2. 在 16KB..48KB 覆写 0xBB
	data2 := bytes.Repeat([]byte{0xBB}, 32*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 16 * 1024, Data: data2})
	require.NoError(t, err)
	copy(expected[16*1024:48*1024], data2)

	// 3. 在 32KB..40KB 覆写 0xCC
	data3 := bytes.Repeat([]byte{0xCC}, 8*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 32 * 1024, Data: data3})
	require.NoError(t, err)
	copy(expected[32*1024:40*1024], data3)

	// 4. 在 128KB..192KB 写入 0xDD（产生 64KB..128KB 的稀疏空洞）
	data4 := bytes.Repeat([]byte{0xDD}, 64*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 128 * 1024, Data: data4})
	require.NoError(t, err)
	copy(expected[128*1024:192*1024], data4)

	// 5. 在 140KB..160KB 打洞（Punch Hole 填零）
	err = mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    ino,
		Fh:     fh,
		Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		Offset: 140 * 1024,
		Length: 20 * 1024,
	})
	require.NoError(t, err)
	copy(expected[140*1024:160*1024], make([]byte, 20*1024))

	// 6. 在 200KB..220KB 写入 0xEE
	data6 := bytes.Repeat([]byte{0xEE}, 20*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 200 * 1024, Data: data6})
	require.NoError(t, err)
	copy(expected[200*1024:220*1024], data6)

	// 合并前读取全量数据
	readBefore, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   250 * 1024,
	})
	require.NoError(t, err)
	require.Len(t, readBefore.Data, 220*1024)
	// 消除自证：与内存黄金期望数据比对
	assert.Equal(t, expected, readBefore.Data, "合并前 Read 必须与独立期望黄金数组严格一致")

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

	// 消除自证：合并后 Read 直接与内存黄金期望数据比对（严禁仅与 readBefore 比对）
	assert.Equal(t, expected, readAfter.Data, "合并后 Read 必须与独立期望黄金数组严格一致")

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
// 真实命中阶段 1 ➔ 阶段 2 并发追加 concurrentSuffix 分支：
// 在阶段 1 调用 PutBlock 落盘时通过钩子同步执行第 3 次 mgr.Write 追加片段；
// CompactChunk 返回后断言合并后的 chunk.Slices 精确为 2 条（1 条合并 + 1 条保留的 concurrentSuffix），
// 旧切片 refs=0，新合并与并发写入切片 refs=1，mgr.Read 精确读出最新完整数据。
func TestCompactChunk_ConcurrentWriteSuffixPreserved(t *testing.T) {
	ctx := context.Background()
	store, bs, mgr, _, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

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

	// 记录合并前旧 2 条 Slice 的 ID
	chunkRowBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesBefore, err := meta.DecodeSlices(chunkRowBefore.Slices)
	require.NoError(t, err)
	require.Len(t, slicesBefore, 2)
	oldID0 := int64(slicesBefore[0].ID)
	oldID1 := int64(slicesBefore[1].ID)

	data3 := bytes.Repeat([]byte{0x33}, 10*1024)
	var write3SliceID uint64
	var hookOnce sync.Once

	hookBS := &recordingBlockStore{
		realBS: bs,
		onBeforePutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
			var hookErr error
			hookOnce.Do(func() {
				// 在阶段 1 PutBlock 落盘时，模拟并发执行第 3 次 Write 追加覆盖片段
				_, writeErr := mgr.Write(context.Background(), &meta.WriteRequest{
					Ino:    ino,
					Fh:     fh,
					Offset: 20 * 1024,
					Data:   data3,
				})
				if writeErr != nil {
					hookErr = writeErr
					return
				}
				// 从底表获取并发第 3 次写入所分配的 sliceID
				c, err := store.GetChunk(context.Background(), db.GetChunkParams{Ino: int64(ino), Indx: 0})
				if err != nil {
					hookErr = err
					return
				}
				sl, err := meta.DecodeSlices(c.Slices)
				if err != nil {
					hookErr = err
					return
				}
				if len(sl) >= 3 {
					write3SliceID = sl[2].ID
				}
			})
			return hookErr
		},
	}

	mgrWithHook := meta.New(store, meta.WithBlockStore(hookBS))

	// 触发 CompactChunk
	err = mgrWithHook.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	// 查询底表 GetChunk 断言合并后的 len(chunk.Slices) == 2 * meta.SliceBytes（精确为 1 条合并 Slice + 1 条被保留的 concurrentSuffix Slice）
	chunkRowAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	assert.Equal(t, 2*meta.SliceBytes, len(chunkRowAfter.Slices), "合并后 slices 字节数必须精确为 2 * SliceBytes (1 条合并切片 + 1 条保留的 concurrentSuffix 切片)")

	slicesAfter, err := meta.DecodeSlices(chunkRowAfter.Slices)
	require.NoError(t, err)
	require.Len(t, slicesAfter, 2)
	compactedSliceID := slicesAfter[0].ID
	suffixSliceID := slicesAfter[1].ID
	assert.Equal(t, write3SliceID, suffixSliceID, "保留的后缀切片必须与并发追加切片 ID 精确一致")

	// 旧 2 条 slice_refs.refs == 0
	ref0, err := store.GetSliceRef(ctx, oldID0)
	require.NoError(t, err)
	assert.Equal(t, int32(0), ref0.Refs, "旧切片 0 的 refs 必须降为 0")

	ref1, err := store.GetSliceRef(ctx, oldID1)
	require.NoError(t, err)
	assert.Equal(t, int32(0), ref1.Refs, "旧切片 1 的 refs 必须降为 0")

	// 新合并 Slice 与第 3 次并发写 Slice 的 refs == 1
	refCompacted, err := store.GetSliceRef(ctx, int64(compactedSliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(1), refCompacted.Refs, "新合并切片的 refs 必须为 1")

	refSuffix, err := store.GetSliceRef(ctx, int64(suffixSliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(1), refSuffix.Refs, "保留的并发追加切片 refs 必须为 1")

	// 读取全量 30KB 数据，mgr.Read 精确读出包含第 3 次并发覆盖的完整数据
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   30 * 1024,
	})
	require.NoError(t, err)
	require.Len(t, readRes.Data, 30*1024)

	assert.Equal(t, data1, readRes.Data[0:10*1024])
	assert.Equal(t, data2, readRes.Data[10*1024:20*1024])
	assert.Equal(t, data3, readRes.Data[20*1024:30*1024])
}

// TestCompactChunk_TruncateOrDeleteCausesCASAbortAndCleanup 集成测试 3：
// 真实命中阶段 1 落盘后并发 O_TRUNC 回滚删块分支：
// 在 recordingBlockStore.PutBlock 完成新合并物理块落盘后的钩子内，同步执行 mgr.Open(..., O_RDWR|O_TRUNC) 清空该文件的 chunks；
// 待 CompactChunk 返回 nil 后，显式对 recordingBlockStore 断言新合并物理块确实经历了 PutBlock >= 1 且 DeleteBlock == PutBlock（回滚删块），
// 且磁盘与底表 chunks 中无孤儿残留。
func TestCompactChunk_TruncateOrDeleteCausesCASAbortAndCleanup(t *testing.T) {
	ctx := context.Background()
	store, bs, _, dataDir, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

	initMgr := meta.New(store, meta.WithBlockStore(bs))

	openRes, err := initMgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = initMgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 写入 16KB 数据
	data := bytes.Repeat([]byte{0x42}, 16*1024)
	_, err = initMgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data})
	require.NoError(t, err)

	blockFilePath := func(sliceID uint64, blockIdx uint32, blockSize uint32) string {
		return filepath.Join(
			dataDir,
			"chunks",
			fmt.Sprintf("%02X", sliceID%256),
			fmt.Sprintf("%d", sliceID/1000/1000),
			fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize),
		)
	}

	var truncOnce sync.Once
	mockBS := &recordingBlockStore{
		realBS: bs,
		onAfterPutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, putData []byte) error {
			var hookErr error
			truncOnce.Do(func() {
				// 在 recordingBlockStore.PutBlock 完成新合并物理块落盘后的钩子内，同步执行 mgr.Open(..., O_RDWR|O_TRUNC) 清空该文件的 chunks
				openTruncRes, err := initMgr.Open(context.Background(), meta.OpenParams{
					Ino:   ino,
					Flags: meta.O_RDWR | meta.O_TRUNC,
				})
				if err != nil {
					hookErr = err
					return
				}
				_ = initMgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: openTruncRes.Fh})
			})
			return hookErr
		},
	}

	mgr := meta.New(store, meta.WithBlockStore(mockBS))

	// 触发 CompactChunk，阶段 2 CAS 发现前缀不匹配中止并清理已落盘的新物理块，返回 nil
	err = mgr.CompactChunk(ctx, ino, 0)
	assert.NoError(t, err)

	// 显式对 recordingBlockStore 断言新合并物理块确实经历了 PutBlock 计数 >= 1 且 DeleteBlock 计数与 PutBlock 相等（已回滚物理删块）
	mockBS.mu.Lock()
	putsCount := len(mockBS.puts)
	deletesCount := len(mockBS.deletes)
	compactedSliceIDs := append([]uint64(nil), mockBS.puts...)
	mockBS.mu.Unlock()

	assert.GreaterOrEqual(t, putsCount, 1, "阶段 1 新合并物理块必须至少经历 1 次 PutBlock 落盘")
	assert.Equal(t, putsCount, deletesCount, "DeleteBlock 计数必须与 PutBlock 相等（新合并块已彻底回滚物理删块）")

	// 验证磁盘无新合并块孤儿残留
	for _, sid := range compactedSliceIDs {
		pPath := blockFilePath(sid, 0, 16*1024)
		assert.NoFileExists(t, pPath, "回滚删块后磁盘上绝不能残留新合并物理块孤儿文件")
	}

	// 验证底表 chunks 中无残留
	chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	assert.True(t, errors.Is(err, pgx.ErrNoRows) || len(chunkAfter.Slices) == 0, "底表 chunks 中不得残留切片")
}

// TestCompactChunk_RefZeroCollectedByGC 集成测试 4：
// CompactChunk 完成后旧 Slice 的 slice_refs.refs 归零，
// 随后调用 CollectGarbage 将旧物理块文件与 slice_refs 记录彻底回收，而新合并生成的物理块完好无损。
func TestCompactChunk_RefZeroCollectedByGC(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

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
	store, bs, _, _, _ := setupPostgresForCompact(t)

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

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

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
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	setupMockWriteTx(mockStore, mockQuerier)

	// 1 条有效 Slice (Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024)
	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

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
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(nil).Once()

	// 阶段 2 事务内锁定行
	mockQuerier2 := dbmock.NewMockQuerier(t)
	mockQuerier2.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	// UpdateChunkSlices 模拟数据库错误
	mockQuerier2.EXPECT().UpdateChunkSlices(mock.Anything, mock.Anything).
		Return(fmt.Errorf("db disk failure")).Once()

	var txCallCount int
	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		txCallCount++
		if txCallCount == 1 {
			return fn(mockQuerier)
		}
		return fn(mockQuerier2)
	}).Twice()

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
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(nil).Once()

	// 事务内锁定时发现已经被改写为完全不同的切片（例如打洞或覆盖截断）
	differentSlice := meta.Slice{Pos: 0, ID: 88, Size: 2048, Off: 0, Len: 2048}
	differentBytes := meta.EncodeSlice(differentSlice)

	mockQuerier2 := dbmock.NewMockQuerier(t)
	mockQuerier2.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: differentBytes}, nil).Once()

	var txCallCount int
	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		txCallCount++
		if txCallCount == 1 {
			return fn(mockQuerier)
		}
		return fn(mockQuerier2)
	}).Twice()

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
	store, _, mgr, _, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

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

type compactCommitAckErrorStore struct {
	db.Store
	onExecTxWithOptions func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error
}

func (s *compactCommitAckErrorStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	if s.onExecTxWithOptions != nil {
		return s.onExecTxWithOptions(ctx, fn, opts...)
	}
	return s.Store.ExecTxWithOptions(ctx, fn, opts...)
}

func (s *compactCommitAckErrorStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.ExecTxWithOptions(ctx, fn)
}

type multiBlockCancelBlockStore struct {
	meta.BlockStore
	onPutBlock func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error
}

func (s *multiBlockCancelBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	if s.onPutBlock != nil {
		if err := s.onPutBlock(ctx, sliceID, blockIdx, data); err != nil {
			return err
		}
	}
	return s.BlockStore.PutBlock(ctx, sliceID, blockIdx, data)
}

func TestCompactChunk_CommitAckErrorAfterServerCommit_DoesNotDeleteCompactedBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, bs, _, dataDir, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
	})

	initMgr := meta.New(store, meta.WithBlockStore(bs))
	openRes, err := initMgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = initMgr.Release(context.Background(), meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
	})

	// 写入两段数据制造碎片切片
	data1 := bytes.Repeat([]byte("A"), 4096)
	_, err = initMgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   data1,
	})
	require.NoError(t, err)

	data2 := bytes.Repeat([]byte("B"), 4096)
	_, err = initMgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 4096,
		Data:   data2,
	})
	require.NoError(t, err)

	expectedData := append(data1, data2...)

	t.Run("CompactedBlockPreservedOnCommitAckError", func(t *testing.T) {
		mockAckErr := errors.New("commit ack network lost")
		var txCount int
		wrappedStore := &compactCommitAckErrorStore{
			Store: store,
			onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				txCount++
				if txCount == 1 {
					// 阶段 1 短读保护事务正常提交
					return store.ExecTxWithOptions(attemptCtx, fn, opts...)
				}
				// 阶段 2 写事务模拟 COMMIT ACK 丢失
				if err := store.ExecTxWithOptions(attemptCtx, fn, opts...); err != nil {
					return err
				}
				return mockAckErr
			},
		}
		mgr := meta.New(wrappedStore, meta.WithBlockStore(bs))

		compactErr := mgr.CompactChunk(ctx, ino, 0)
		require.Error(t, compactErr)
		assert.Contains(t, compactErr.Error(), "commit ack network lost")

		// 阶段 2 事务服务端已 COMMIT，元数据已替换为合并切片，已落盘物理块不得被误删
		readRes, readErr := initMgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   uint32(len(expectedData)),
		})
		require.NoError(t, readErr, "阶段 2 已成功 COMMIT 的合并数据块绝不能因确认断连被误删，Read 必须成功且不报 ErrIO")
		require.NotNil(t, readRes)
		assert.Equal(t, expectedData, readRes.Data)
	})

	t.Run("InFlightServerCommitNotYetVisible_DoesNotDeleteCompactedBlock", func(t *testing.T) {
		subFileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		subIno := uint64(subFileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, subFileRow.Ino)
		})

		subOpenRes, err := initMgr.Open(ctx, meta.OpenParams{
			Ino:   subIno,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		subFh := subOpenRes.Fh
		t.Cleanup(func() {
			_ = initMgr.Release(context.Background(), meta.ReleaseParams{
				Ino: subIno,
				Fh:  subFh,
			})
		})

		d1 := bytes.Repeat([]byte("X"), 4096)
		_, err = initMgr.Write(ctx, &meta.WriteRequest{
			Ino:    subIno,
			Fh:     subFh,
			Offset: 0,
			Data:   d1,
		})
		require.NoError(t, err)

		d2 := bytes.Repeat([]byte("Y"), 4096)
		_, err = initMgr.Write(ctx, &meta.WriteRequest{
			Ino:    subIno,
			Fh:     subFh,
			Offset: 4096,
			Data:   d2,
		})
		require.NoError(t, err)

		subExpectedData := append(d1, d2...)

		allowServerCommitCh := make(chan struct{})
		var closeOnce sync.Once
		closeAllowCommit := func() {
			closeOnce.Do(func() {
				close(allowServerCommitCh)
			})
		}
		t.Cleanup(closeAllowCommit)

		reachedCommitCh := make(chan struct{})
		var onceReached sync.Once
		var bgErr error
		var bgWg sync.WaitGroup
		var txCount int
		var txMu sync.Mutex

		wrappedStore := &compactCommitAckErrorStore{
			Store: store,
			onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				txMu.Lock()
				txCount++
				currentCount := txCount
				txMu.Unlock()

				if currentCount == 1 {
					// 阶段 1：短读只读事务正常提交
					return store.ExecTxWithOptions(attemptCtx, fn, opts...)
				}

				// 阶段 2：写事务在后台 goroutine 中执行
				bgWg.Add(1)
				go func() {
					defer bgWg.Done()
					bgCtx, bgCancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer bgCancel()
					bgErr = store.ExecTxWithOptions(bgCtx, func(q db.Querier) error {
						if err := fn(q); err != nil {
							return err
						}
						onceReached.Do(func() {
							close(reachedCommitCh)
						})
						select {
						case <-allowServerCommitCh:
							return nil
						case <-bgCtx.Done():
							return bgCtx.Err()
						}
					}, opts...)
				}()

				select {
				case <-reachedCommitCh:
					// 立即向 CompactChunk 返回 DeadlineExceeded，模拟客户端先超时并走完补偿分支
					return context.DeadlineExceeded
				case <-attemptCtx.Done():
					return attemptCtx.Err()
				}
			},
		}

		mgr := meta.New(wrappedStore, meta.WithBlockStore(bs))
		compactErr := mgr.CompactChunk(ctx, subIno, 0)
		require.Error(t, compactErr)
		assert.True(t, errors.Is(compactErr, context.DeadlineExceeded) || strings.Contains(compactErr.Error(), context.DeadlineExceeded.Error()))

		// 放行后台独立 goroutine 执行真正的 PostgreSQL COMMIT 并等待其完成
		closeAllowCommit()
		bgWg.Wait()
		require.NoError(t, bgErr, "后台服务端事务 COMMIT 必须成功")

		// 阶段 2 事务服务端已 COMMIT，元数据已替换为合并切片，物理块不得被误删，
		// Read 必须能完整读出历史已提交数据！
		readRes, readErr := initMgr.Read(ctx, &meta.ReadRequest{
			Ino:    subIno,
			Fh:     subFh,
			Offset: 0,
			Size:   uint32(len(subExpectedData)),
		})
		require.NoError(t, readErr, "在途 COMMIT 的合并数据块绝不能因外部超时检查被误删，Read 必须成功且不报 ErrIO")
		require.NotNil(t, readRes)
		assert.Equal(t, subExpectedData, readRes.Data)
	})

	t.Run("MultiBlockPutBlockErrorWithCanceledContext_CleansUpBlocks", func(t *testing.T) {
		// 构造 5 MiB 数据（跨 2 个 4 MiB 物理块：BlockIdx 0 4 MiB，BlockIdx 1 1 MiB）
		fileRow2, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino2 := uint64(fileRow2.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow2.Ino)
		})

		openRes2, err := initMgr.Open(ctx, meta.OpenParams{
			Ino:   ino2,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh2 := openRes2.Fh
		t.Cleanup(func() {
			_ = initMgr.Release(context.Background(), meta.ReleaseParams{
				Ino: ino2,
				Fh:  fh2,
			})
		})

		part1 := bytes.Repeat([]byte{0x55}, int(2.5*1024*1024))
		_, err = initMgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino2,
			Fh:     fh2,
			Offset: 0,
			Data:   part1,
		})
		require.NoError(t, err)

		part2 := bytes.Repeat([]byte{0x66}, int(2.5*1024*1024))
		_, err = initMgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino2,
			Fh:     fh2,
			Offset: uint64(len(part1)),
			Data:   part2,
		})
		require.NoError(t, err)

		var initialFiles int
		_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, walkErr error) error {
			if !d.IsDir() {
				initialFiles++
			}
			return nil
		})

		reqCtx, reqCancel := context.WithCancel(ctx)
		wrappedBS := &multiBlockCancelBlockStore{
			BlockStore: bs,
			onPutBlock: func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
				if blockIdx == 1 {
					reqCancel()
					return errors.New("simulated put block 1 disk error")
				}
				return nil
			},
		}
		mgr2 := meta.New(store, meta.WithBlockStore(wrappedBS))

		compactErr := mgr2.CompactChunk(reqCtx, ino2, 0)
		require.Error(t, compactErr)

		var currentFiles int
		_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, walkErr error) error {
			if !d.IsDir() {
				currentFiles++
			}
			return nil
		})
		assert.Equal(t, initialFiles, currentFiles, "即使上下文取消，多块合并中 Block 1 失败时 Block 0 必须被立即清理干净")
	})
}

func TestCompactChunk_TriggeredByPunchHoleAndTruncateAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, bs, _, _, _ := setupPostgresForCompact(t)

	t.Run("PunchHoleAccumulationTriggersCompaction", func(t *testing.T) {
		mgr := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
		})

		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{
				Ino: ino,
				Fh:  fh,
			})
		})

		// 1. 写入 16 KiB 初始非零数据
		initData := bytes.Repeat([]byte{0xAA}, 16*1024)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   initData,
		})
		require.NoError(t, err)

		// 2. 连续 3 次打洞（分别对 [0, 4096), [4096, 8192), [8192, 12288) 打洞）
		for i := 0; i < 3; i++ {
			err = mgr.Fallocate(ctx, meta.FallocateParams{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(i * 4096),
				Length: 4096,
				Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			})
			require.NoError(t, err)
		}

		// 打洞完成后，加上初始写入切片，数据库中该 chunk 的 slices 累积达到 4 条切片（4 * 24 = 96 字节）
		// 断言异步 Compaction 被自动触发并压缩至 < 4 条切片
		require.Eventually(t, func() bool {
			chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			})
			if err != nil {
				return false
			}
			return len(chunkRow.Slices)/meta.SliceBytes < 4
		}, 2*time.Second, 20*time.Millisecond, "打洞累积切片达到阈值必须自动触发异步 Compaction 并压缩切片数至 < 4")

		// 验证 Read 读取数据精确无误：前 12 KiB 为 0，后 4 KiB 仍为 0xAA
		expected := make([]byte, 16*1024)
		copy(expected[12*1024:], bytes.Repeat([]byte{0xAA}, 4*1024))

		readRes, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   16 * 1024,
		})
		require.NoError(t, err)
		assert.Equal(t, expected, readRes.Data)
	})

	t.Run("TruncateAcrossRestartTriggersCompaction", func(t *testing.T) {
		mgr1 := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
		})

		openRes, err := mgr1.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			_ = mgr1.Release(context.Background(), meta.ReleaseParams{
				Ino: ino,
				Fh:  fh,
			})
		})

		// 用 mgr1 写入 3 次产生 3 条切片（文件总大小 12288）
		for i := 0; i < 3; i++ {
			_, err = mgr1.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(i * 4096),
				Data:   bytes.Repeat([]byte{byte(i + 1)}, 4096),
			})
			require.NoError(t, err)
		}

		// 创建全新 mgr2（模拟进程重启，丢失任何内存写入计数）
		mgr2 := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

		// 执行 1 次非对齐半块缩容截断（从 12288 截断至 10000），追加第 4 条空洞覆盖切片，达到阈值 4
		targetSize := uint64(10000)
		_, err = mgr2.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: &targetSize,
		})
		require.NoError(t, err)

		// 断言跨重启后基于权威元数据切片数量，SetAttr 同样自动触发异步 Compaction 并压缩至 < 4
		require.Eventually(t, func() bool {
			chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			})
			if err != nil {
				return false
			}
			return len(chunkRow.Slices)/meta.SliceBytes < 4
		}, 2*time.Second, 20*time.Millisecond, "跨重启后半块截断达到切片阈值必须自动触发异步 Compaction 并压缩切片数至 < 4")
	})
}

// asyncCompactRerunsBlockStore 包装 BlockStore，精确拦截首轮后台异步 CompactChunk 的 PutBlock 一次。
type asyncCompactRerunsBlockStore struct {
	meta.BlockStore
	inFlightEntered chan struct{}
	releaseCompact  chan struct{}
	onceEntered     sync.Once
}

type fgWriteContextKey struct{}

func (s *asyncCompactRerunsBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	// 区分前台写入与后台异步 Compaction：
	// 前台 Write 显式注入 fgWriteContextKey，后台异步 Compact 使用独立的 context.WithTimeout，无此 key。
	if ctx.Value(fgWriteContextKey{}) == nil {
		s.onceEntered.Do(func() {
			close(s.inFlightEntered)
		})
		select {
		case <-s.releaseCompact:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.BlockStore.PutBlock(ctx, sliceID, blockIdx, data)
}

func TestAsyncCompact_RerunsWhenConcurrentWritesExceedThresholdDuringInFlightCompact(t *testing.T) {
	ctx := context.Background()
	store, rawBS, _, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
	})

	inFlightEntered := make(chan struct{})
	releaseFirstCompact := make(chan struct{})
	var closeOnce sync.Once
	releaseCompact := func() {
		closeOnce.Do(func() {
			close(releaseFirstCompact)
		})
	}
	t.Cleanup(releaseCompact)

	interceptBS := &asyncCompactRerunsBlockStore{
		BlockStore:      rawBS,
		inFlightEntered: inFlightEntered,
		releaseCompact:  releaseFirstCompact,
	}

	mgr := meta.New(store, meta.WithBlockStore(interceptBS), meta.WithCompactSliceThreshold(4))

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
	})

	fgCtx := context.WithValue(ctx, fgWriteContextKey{}, true)

	// 前台连续执行 4 次 Write，触发首轮异步 CompactChunk
	for i := 0; i < 4; i++ {
		writeData := bytes.Repeat([]byte{byte(i + 1)}, 4096)
		_, err := mgr.Write(fgCtx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(i * 4096),
			Data:   writeData,
		})
		require.NoError(t, err)
	}

	// 等待首轮后台异步 CompactChunk 进入 PutBlock（此时 inFlightCompacts 中已登记该 chunkKey）
	select {
	case <-inFlightEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("超时: 首轮后台异步 CompactChunk 未在预期时间内进入 PutBlock")
	}

	// 在途运行期间，前台立即在同一 Chunk 0 上再连续执行 4 次新的 Write
	// 使 concurrentSuffix 自身达到 4 条 Slice >= 阈值 4，且每次 maybeTriggerAsyncCompact 均撞上在途 inFlightCompacts 被丢弃
	for i := 4; i < 8; i++ {
		writeData := bytes.Repeat([]byte{byte(i + 1)}, 4096)
		_, err := mgr.Write(fgCtx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(i * 4096),
			Data:   writeData,
		})
		require.NoError(t, err)
	}

	// 前台彻底停止一切后续写入，放行首轮后台 Compaction 完成
	releaseCompact()

	// 首轮 CAS 提交后，Chunk 0 剩 1 条合并前缀 + 4 条并发后缀 = 5 条 Slice，仍 >= 阈值 4
	// 断言无需任何新前台写入，后台自动续跑第 2 轮 Compaction 将切片数收敛至 1
	require.Eventually(t, func() bool {
		chunkRow, err := store.GetChunk(context.Background(), db.GetChunkParams{
			Ino:  int64(ino),
			Indx: 0,
		})
		if err != nil {
			return false
		}
		sliceCount := len(chunkRow.Slices) / meta.SliceBytes
		return sliceCount == 1
	}, 3*time.Second, 25*time.Millisecond, "在途 Compaction 遇突发超阈值写入，停止写入后必须自动续跑收敛切片数至 1")

	// 验证数据完整一致
	var expectedData []byte
	for i := 0; i < 8; i++ {
		expectedData = append(expectedData, bytes.Repeat([]byte{byte(i + 1)}, 4096)...)
	}
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(len(expectedData)),
	})
	require.NoError(t, err)
	assert.Equal(t, expectedData, readRes.Data)
}

// TestCompactChunk_SliceRefLockRetryAndExhaustion Mock 单测验证 CompactChunk 的 LockValidSliceRefsKeyShare 重试与耗尽行为
func TestCompactChunk_SliceRefLockRetryAndExhaustion(t *testing.T) {
	ctx := context.Background()

	t.Run("首次锁返回不足重读 GetChunk 成功后完成合并", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier1 := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)

		slices1011 := append(
			meta.EncodeSlice(meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}),
			meta.EncodeSlice(meta.Slice{Pos: 1024, ID: 11, Size: 1024, Off: 0, Len: 1024})...,
		)
		slice20 := meta.EncodeSlice(
			meta.Slice{Pos: 0, ID: 20, Size: 2048, Off: 0, Len: 2048},
		)

		// 阶段 1：读事务 attempt 0 查出 [10, 11]
		mockQuerier1.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slices1011}, nil).Once()

		// 锁定不足：锁定 [10, 11] 仅返回 [10]
		mockQuerier1.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10, 11}).
			Return([]int64{10}, nil).Once()

		// attempt 1 触发重读 GetChunk 拿到新 slice [20]
		mockQuerier1.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slice20}, nil).Once()

		// 成功锁定 [20]
		mockQuerier1.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{20}).
			Return([]int64{20}, nil).Once()

		// 仅从 sliceID=20 读取数据，绝不调用 10 或 11
		mockBS.EXPECT().GetBlock(mock.Anything, uint64(20), uint32(0), uint32(2048), uint32(0), mock.Anything).
			Return(nil).Once()

		mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()
		mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).Return(nil).Once()

		// 阶段 2：写事务
		mockQuerier2 := dbmock.NewMockQuerier(t)
		mockQuerier2.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slice20}, nil).Once()

		mockQuerier2.EXPECT().UpdateChunkSlices(mock.Anything, mock.Anything).Return(nil).Once()
		mockQuerier2.EXPECT().InsertSliceRef(mock.Anything, mock.Anything).Return(nil).Once()
		mockQuerier2.EXPECT().AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
			SliceID: 20,
			Delta:   -1,
		}).Return(nil).Once()

		var txCallCount int
		mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			txCallCount++
			if txCallCount == 1 {
				return fn(mockQuerier1)
			}
			return fn(mockQuerier2)
		}).Twice()

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		err := mgr.CompactChunk(ctx, 1, 0)
		require.NoError(t, err)

		mockBS.AssertNotCalled(t, "GetBlock", mock.Anything, uint64(10), mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		mockBS.AssertNotCalled(t, "GetBlock", mock.Anything, uint64(11), mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("锁查询报错立即中止且不调 NextSliceID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)

		setupMockWriteTx(mockStore, mockQuerier)

		slice10 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024})
		mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slice10}, nil).Once()

		lockErr := errors.New("database connection refused on lock")
		mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
			Return(nil, lockErr).Once()

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		err := mgr.CompactChunk(ctx, 1, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "lock valid slice refs for compact")
		mockStore.AssertNotCalled(t, "NextSliceID", mock.Anything)
		mockBS.AssertNotCalled(t, "GetBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		mockBS.AssertNotCalled(t, "PutBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("连续达最大重试上限 8 次锁定不足后返回 meta.ErrIO", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)

		setupMockWriteTx(mockStore, mockQuerier)

		slice10 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024})
		mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slice10}, nil).Times(8)

		// 8 次 LockValidSliceRefsKeyShare 返回空切片
		mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
			Return([]int64{}, nil).Times(8)

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		err := mgr.CompactChunk(ctx, 1, 0)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrIO)
		mockStore.AssertNotCalled(t, "NextSliceID", mock.Anything)
		mockBS.AssertNotCalled(t, "PutBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}

// TestCompactChunk_TxRetryResetsEverReachedCommit_CleansUpBlockOnSecondAttemptFailure 验证写事务重试首行重置 everReachedCommit 并精确清理物理块
func TestCompactChunk_TxRetryResetsEverReachedCommit_CleansUpBlockOnSecondAttemptFailure(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier1 := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	// 阶段 1：读事务
	mockQuerier1.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier1.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(nil).Once()

	// 阶段 2 事务第 1 次尝试
	mockQuerier2_1 := dbmock.NewMockQuerier(t)
	mockQuerier2_1.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier2_1.EXPECT().UpdateChunkSlices(mock.Anything, mock.Anything).Return(nil).Once()
	mockQuerier2_1.EXPECT().InsertSliceRef(mock.Anything, mock.Anything).Return(nil).Once()
	mockQuerier2_1.EXPECT().AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
		SliceID: 10,
		Delta:   -1,
	}).Return(nil).Once()

	// 阶段 2 事务第 2 次尝试（在 GetChunkForUpdate 直接返回不可重试 DB 错误）
	mockQuerier2_2 := dbmock.NewMockQuerier(t)
	dbErr := errors.New("fatal db connection failure on retry")
	mockQuerier2_2.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{}, dbErr).Once()

	var txCallCount int
	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		txCallCount++
		if txCallCount == 1 {
			// 阶段 1 读事务
			return fn(mockQuerier1)
		}
		if txCallCount == 2 {
			// 阶段 2 写事务第 1 次执行：完整执行 fn(mockQuerier2_1)，置 everReachedCommit = true，但事务提交返回 40001
			if err := fn(mockQuerier2_1); err != nil {
				return err
			}
			return &pgconn.PgError{Code: "40001"}
		}
		// 阶段 2 写事务第 2 次重试
		return fn(mockQuerier2_2)
	}).Times(3)

	// 精确调用 1 次 DeleteBlock 清理阶段 1 写入的新物理块 (sliceID=99)
	mockBS.EXPECT().DeleteBlock(mock.Anything, uint64(99), uint32(0), uint32(1024)).Return(nil).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fatal db connection failure on retry")
}
