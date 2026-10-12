package adapter

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) (*pgxpool.Pool, string, string, func()) {
	t.Helper()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	diskID := uuid.New().String()
	schemaName, err := FormatDiskSchema(diskID)
	require.NoError(t, err)

	ddl := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS "%s";
CREATE TABLE IF NOT EXISTS "%s".chunks (
	ino BIGINT NOT NULL,
	indx INT NOT NULL,
	slices BYTEA NOT NULL,
	PRIMARY KEY (ino, indx)
);
CREATE TABLE IF NOT EXISTS "%s".slice_refs (
	id BIGINT PRIMARY KEY,
	chunk_indx INT NOT NULL DEFAULT 0,
	size INT NOT NULL,
	refs INT NOT NULL DEFAULT 1
);
`, schemaName, schemaName, schemaName)

	_, err = pool.Exec(ctx, ddl)
	require.NoError(t, err)

	cleanup := func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE;`, schemaName))
		pool.Close()
	}

	return pool, diskID, schemaName, cleanup
}

// TestMetaAdapter_NewSlice 测试切片编号单调递增与并发安全分配
func TestMetaAdapter_NewSlice(t *testing.T) {
	pool, _, _, cleanup := setupTestDB(t)
	defer cleanup()

	adapter := NewMetaAdapter(pool)
	ctx := context.Background()

	id1, err := adapter.NewSlice(ctx)
	require.NoError(t, err)
	assert.Greater(t, id1, uint64(0))

	id2, err := adapter.NewSlice(ctx)
	require.NoError(t, err)
	assert.Greater(t, id2, id1)

	// 并发分配测试
	const concurrency = 50
	ids := make([]uint64, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			allocated, err := adapter.NewSlice(ctx)
			assert.NoError(t, err)
			ids[idx] = allocated
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]bool)
	for _, id := range ids {
		assert.False(t, seen[id], "切片 ID 必须全局唯一无冲突: %d", id)
		seen[id] = true
	}
}

// TestMetaAdapter_WriteAndReadSlice_Single 测试单条切片写入与精确读取
func TestMetaAdapter_WriteAndReadSlice_Single(t *testing.T) {
	pool, diskID, schemaName, cleanup := setupTestDB(t)
	defer cleanup()

	adapter := NewMetaAdapter(pool)
	ctx := context.Background()

	sliceID, err := adapter.NewSlice(ctx)
	require.NoError(t, err)

	const ino = uint64(100)
	const chunkIndx = uint32(0)

	s := Slice{
		Pos:  0,
		ID:   sliceID,
		Size: 4096,
		Off:  0,
		Len:  4096,
	}

	err = adapter.WriteSlice(ctx, diskID, ino, chunkIndx, s)
	require.NoError(t, err)

	// 1. 验证 chunks 表二进制内容
	var rawSlices []byte
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName), int64(ino), int32(chunkIndx)).Scan(&rawSlices)
	require.NoError(t, err)
	assert.Equal(t, EncodeSlice(s), rawSlices)

	// 2. 验证 slice_refs 引用计数为 1
	var (
		refSize int32
		refCnt  int32
	)
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT size, refs FROM "%s".slice_refs WHERE id = $1`, schemaName), int64(sliceID)).Scan(&refSize, &refCnt)
	require.NoError(t, err)
	assert.Equal(t, int32(4096), refSize)
	assert.Equal(t, int32(1), refCnt)

	// 3. 通过适配器 ReadSlices 查询
	readSlices, err := adapter.ReadSlices(ctx, diskID, ino, chunkIndx)
	require.NoError(t, err)
	require.Len(t, readSlices, 1)
	assert.Equal(t, s, readSlices[0])

	// 4. 查询未写入的稀疏 Chunk
	sparseSlices, err := adapter.ReadSlices(ctx, diskID, ino, 999)
	require.NoError(t, err)
	assert.Nil(t, sparseSlices)
}

// TestMetaAdapter_WriteSlices_AtomicAppend 测试多次写入同一 Chunk 时 PostgreSQL BYTEA || BYTEA 原子追加与多切片重构
func TestMetaAdapter_WriteSlices_AtomicAppend(t *testing.T) {
	pool, diskID, schemaName, cleanup := setupTestDB(t)
	defer cleanup()

	adapter := NewMetaAdapter(pool)
	ctx := context.Background()

	const ino = uint64(200)
	const chunkIndx = uint32(0)

	id1, err := adapter.NewSlice(ctx)
	require.NoError(t, err)
	s1 := Slice{
		Pos:  0,
		ID:   id1,
		Size: 4096,
		Off:  0,
		Len:  4096,
	}

	id2, err := adapter.NewSlice(ctx)
	require.NoError(t, err)
	s2 := Slice{
		Pos:  2048,
		ID:   id2,
		Size: 1024,
		Off:  0,
		Len:  1024,
	}

	// 第一次写入
	err = adapter.WriteSlice(ctx, diskID, ino, chunkIndx, s1)
	require.NoError(t, err)

	// 第二次写入（覆盖区间 2048~3072）
	err = adapter.WriteSlice(ctx, diskID, ino, chunkIndx, s2)
	require.NoError(t, err)

	// 验证 chunks 表总字节数为 48 字节 (2 * 24B)
	var rawSlices []byte
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName), int64(ino), int32(chunkIndx)).Scan(&rawSlices)
	require.NoError(t, err)
	assert.Len(t, rawSlices, 48)

	// 验证原始时序切片读取
	readSlices, err := adapter.ReadSlices(ctx, diskID, ino, chunkIndx)
	require.NoError(t, err)
	require.Len(t, readSlices, 2)
	assert.Equal(t, s1, readSlices[0])
	assert.Equal(t, s2, readSlices[1])

	// 验证重组后的无重叠逻辑图层
	reconstituted, err := adapter.ReadReconstitutedSlices(ctx, diskID, ino, chunkIndx)
	require.NoError(t, err)
	require.Len(t, reconstituted, 3)

	assert.Equal(t, id1, reconstituted[0].Id)
	assert.Equal(t, uint32(2048), reconstituted[0].Len)
	assert.Equal(t, uint32(0), reconstituted[0].Off)

	assert.Equal(t, id2, reconstituted[1].Id)
	assert.Equal(t, uint32(1024), reconstituted[1].Len)
	assert.Equal(t, uint32(0), reconstituted[1].Off)

	assert.Equal(t, id1, reconstituted[2].Id)
	assert.Equal(t, uint32(1024), reconstituted[2].Len)
	assert.Equal(t, uint32(3072), reconstituted[2].Off)
}

// TestMetaAdapter_SliceReferenceCounting 测试切片引用计数的增减与查验
func TestMetaAdapter_SliceReferenceCounting(t *testing.T) {
	pool, diskID, _, cleanup := setupTestDB(t)
	defer cleanup()

	adapter := NewMetaAdapter(pool)
	ctx := context.Background()

	sliceID, err := adapter.NewSlice(ctx)
	require.NoError(t, err)

	s := Slice{
		Pos:  0,
		ID:   sliceID,
		Size: 4096,
		Off:  0,
		Len:  4096,
	}

	// 写入生成初始引用计数 1
	err = adapter.WriteSlice(ctx, diskID, 300, 0, s)
	require.NoError(t, err)

	refs, size, err := adapter.GetSliceRef(ctx, diskID, sliceID)
	require.NoError(t, err)
	assert.Equal(t, 1, refs)
	assert.Equal(t, uint32(4096), size)

	// 递增引用计数 (例如 CopyFileRange)
	err = adapter.IncSliceRef(ctx, diskID, sliceID, 2, 4096)
	require.NoError(t, err)

	refs, size, err = adapter.GetSliceRef(ctx, diskID, sliceID)
	require.NoError(t, err)
	assert.Equal(t, 3, refs)
	assert.Equal(t, uint32(4096), size)

	// 递减引用计数
	err = adapter.DecSliceRef(ctx, diskID, sliceID, 1)
	require.NoError(t, err)

	refs, _, err = adapter.GetSliceRef(ctx, diskID, sliceID)
	require.NoError(t, err)
	assert.Equal(t, 2, refs)
}

// TestMetaAdapter_LocateAndDeleteChunk 测试切片全局定位与 Chunk 清理
func TestMetaAdapter_LocateAndDeleteChunk(t *testing.T) {
	pool, diskID, _, cleanup := setupTestDB(t)
	defer cleanup()

	adapter := NewMetaAdapter(pool)
	ctx := context.Background()

	sliceID, err := adapter.NewSlice(ctx)
	require.NoError(t, err)

	const ino = uint64(400)
	const chunkIndx = uint32(3)

	s := Slice{
		Pos:  1024,
		ID:   sliceID,
		Size: 2048,
		Off:  0,
		Len:  2048,
	}

	err = adapter.WriteSlice(ctx, diskID, ino, chunkIndx, s)
	require.NoError(t, err)

	// 1. 定位存在的切片
	foundIno, foundIndx, found, err := adapter.LocateSlice(ctx, diskID, sliceID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, ino, foundIno)
	assert.Equal(t, chunkIndx, foundIndx)

	// 2. 定位不存在的切片
	_, _, found, err = adapter.LocateSlice(ctx, diskID, 99999999)
	require.NoError(t, err)
	assert.False(t, found)

	// 3. 删除 Chunk 并获取被丢弃的切片
	discarded, err := adapter.DeleteChunk(ctx, diskID, ino, chunkIndx)
	require.NoError(t, err)
	require.Len(t, discarded, 1)
	assert.Equal(t, s, discarded[0])

	// 4. 再次查找已被删除的切片
	_, _, found, err = adapter.LocateSlice(ctx, diskID, sliceID)
	require.NoError(t, err)
	assert.False(t, found)
}

// TestMetaAdapter_TruncateChunkSlices 测试截断并更新 Chunk 内部切片
func TestMetaAdapter_TruncateChunkSlices(t *testing.T) {
	pool, diskID, _, cleanup := setupTestDB(t)
	defer cleanup()

	adapter := NewMetaAdapter(pool)
	ctx := context.Background()

	const ino = uint64(500)
	const chunkIndx = uint32(0)

	id1, _ := adapter.NewSlice(ctx)
	id2, _ := adapter.NewSlice(ctx)

	s1 := Slice{Pos: 0, ID: id1, Size: 4096, Off: 0, Len: 4096}
	s2 := Slice{Pos: 4096, ID: id2, Size: 4096, Off: 0, Len: 4096}

	err := adapter.WriteSlices(ctx, diskID, ino, chunkIndx, []Slice{s1, s2})
	require.NoError(t, err)

	// 截断更新：仅保留修改后的 s1
	s1Adjusted := Slice{Pos: 0, ID: id1, Size: 4096, Off: 0, Len: 2048}
	err = adapter.TruncateChunkSlices(ctx, diskID, ino, chunkIndx, []Slice{s1Adjusted})
	require.NoError(t, err)

	updatedSlices, err := adapter.ReadSlices(ctx, diskID, ino, chunkIndx)
	require.NoError(t, err)
	require.Len(t, updatedSlices, 1)
	assert.Equal(t, s1Adjusted, updatedSlices[0])

	// 截断更新为 0 个切片（相当于删除该 Chunk）
	err = adapter.TruncateChunkSlices(ctx, diskID, ino, chunkIndx, nil)
	require.NoError(t, err)

	emptySlices, err := adapter.ReadSlices(ctx, diskID, ino, chunkIndx)
	require.NoError(t, err)
	assert.Nil(t, emptySlices)
}

// TestMetaAdapter_ConcurrentWrites 测试高并发下多协程同时追加写入同一 Chunk
func TestMetaAdapter_ConcurrentWrites(t *testing.T) {
	pool, diskID, _, cleanup := setupTestDB(t)
	defer cleanup()

	adapter := NewMetaAdapter(pool)
	ctx := context.Background()

	const ino = uint64(600)
	const chunkIndx = uint32(0)
	const concurrency = 20

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			sliceID, err := adapter.NewSlice(ctx)
			assert.NoError(t, err)

			s := Slice{
				Pos:  uint32(idx * 1024),
				ID:   sliceID,
				Size: 1024,
				Off:  0,
				Len:  1024,
			}
			err = adapter.WriteSlice(ctx, diskID, ino, chunkIndx, s)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// 验证所有切片完整写入且长度恰好为 concurrency * 24 字节
	allSlices, err := adapter.ReadSlices(ctx, diskID, ino, chunkIndx)
	require.NoError(t, err)
	assert.Len(t, allSlices, concurrency, "并发追加必须保证所有切片无丢失")

	// 验证每个切片 ID 均独一无二
	seenIDs := make(map[uint64]bool)
	for _, s := range allSlices {
		assert.False(t, seenIDs[s.ID])
		seenIDs[s.ID] = true
	}
}

// TestMetaAdapter_DeleteInodeChunks_DecrementsRefsAndReturnsOrphans 测试删除 Inode 时切片引用安全递减与孤儿切片查验
func TestMetaAdapter_DeleteInodeChunks_DecrementsRefsAndReturnsOrphans(t *testing.T) {
	pool, diskID, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	adapter := NewMetaAdapter(pool)
	ino := uint64(500)

	// 在 Chunk 0 与 Chunk 1 写入切片
	s1 := Slice{Pos: 0, ID: 101, Size: 4096, Off: 0, Len: 4096}
	s2 := Slice{Pos: 4096, ID: 102, Size: 4096, Off: 0, Len: 4096}
	s3 := Slice{Pos: 0, ID: 103, Size: 4096, Off: 0, Len: 4096}

	err := adapter.WriteSlices(ctx, diskID, ino, 0, []Slice{s1, s2})
	require.NoError(t, err)
	err = adapter.WriteSlice(ctx, diskID, ino, 1, s3)
	require.NoError(t, err)

	// 切片 101 同时被另一个 Inode 引用（克隆场景），将 refs 增至 2
	err = adapter.IncSliceRef(ctx, diskID, 101, 1, 4096)
	require.NoError(t, err)

	ref101, _, err := adapter.GetSliceRef(ctx, diskID, 101)
	require.NoError(t, err)
	assert.Equal(t, 2, ref101)

	// 执行 DeleteInodeChunks
	orphans, err := adapter.DeleteInodeChunks(ctx, diskID, ino)
	require.NoError(t, err)

	// 切片 102 与 103 引用归零，应判定为物理孤儿切片并返回；切片 101 仍有引用 (refs=1)，不属于孤儿
	assert.ElementsMatch(t, []uint64{102, 103}, orphans)

	// 验证切片 101 当前 refs 降为 1
	ref101After, _, err := adapter.GetSliceRef(ctx, diskID, 101)
	require.NoError(t, err)
	assert.Equal(t, 1, ref101After)

	// 验证切片 102 与 103 当前 refs 为 0
	ref102, _, err := adapter.GetSliceRef(ctx, diskID, 102)
	require.NoError(t, err)
	assert.Equal(t, 0, ref102)

	ref103, _, err := adapter.GetSliceRef(ctx, diskID, 103)
	require.NoError(t, err)
	assert.Equal(t, 0, ref103)

	// 验证 Inode 500 的 chunks 全部行被删除
	slices0, err := adapter.ReadSlices(ctx, diskID, ino, 0)
	require.NoError(t, err)
	assert.Nil(t, slices0)

	slices1, err := adapter.ReadSlices(ctx, diskID, ino, 1)
	require.NoError(t, err)
	assert.Nil(t, slices1)
}


