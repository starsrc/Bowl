package db_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// testSliceGCAdvisoryLockKey 是跨测试包隔离 slice_refs 全表 GC 与并发切片测试的固定 64 位 PostgreSQL 咨询锁键。
const testSliceGCAdvisoryLockKey int64 = 0x424f574c5f474301

func acquireChunksTestSliceGCLock(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err, "获取专用连接以加 GC 咨询锁失败")

	_, err = conn.Exec(context.Background(), "SELECT pg_advisory_lock($1)", testSliceGCAdvisoryLockKey)
	require.NoError(t, err, "加会话级 GC 咨询锁失败")

	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testSliceGCAdvisoryLockKey)
		conn.Release()
	})
}

func setupChunksTestDB(t *testing.T) (db.Store, *pgxpool.Pool) {
	t.Helper()
	for _, envName := range []string{"STORAGE_TEST_DSN", "STORAGE_DB_DSN", "TEST_DB_DSN"} {
		rawVal := strings.TrimSpace(os.Getenv(envName))
		if rawVal != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			poolCfg, err := pgxpool.ParseConfig(rawVal)
			if err != nil {
				t.Fatalf("显式指定的数据库 DSN (%s) 配置解析失败: %v", envName, err)
			}
			pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
			if err != nil {
				t.Fatalf("显式指定的数据库 DSN (%s) 连接失败: %v", envName, err)
			}
			if err := pool.Ping(ctx); err != nil {
				pool.Close()
				t.Fatalf("显式指定的数据库 DSN (%s) 连接失败: %v", envName, err)
			}
			t.Cleanup(func() {
				pool.Close()
			})
			return db.NewStore(pool), pool
		}
	}

	candidates := []string{
		"postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable",
		"postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable",
	}

	for _, cand := range candidates {
		probeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		poolCfg, err := pgxpool.ParseConfig(cand)
		if err == nil {
			pool, err := pgxpool.NewWithConfig(probeCtx, poolCfg)
			if err == nil {
				if pingErr := pool.Ping(probeCtx); pingErr == nil {
					cancel()
					t.Cleanup(func() {
						pool.Close()
					})
					return db.NewStore(pool), pool
				}
				pool.Close()
			}
		}
		cancel()
	}

	if strings.TrimSpace(os.Getenv("BOWL_FORMAL_REQUIRE_DB")) == "1" {
		t.Fatalf("未探测到可用 PostgreSQL 实例 (9432/5432 均不可达) 且设置了 BOWL_FORMAL_REQUIRE_DB=1")
	}

	t.Skip("跳过 PostgreSQL 测试 (未显式配置 DSN 且 127.0.0.1:9432 和 127.0.0.1:5432 均不可达)")
	return nil, nil
}

// makeTestSliceDescriptor 生成模拟的 24 字节 Slice 描述符
func makeTestSliceDescriptor(pos uint32, id uint64, size uint32, off uint32, length uint32) []byte {
	buf := make([]byte, 24)
	buf[0] = byte(pos >> 24)
	buf[1] = byte(pos >> 16)
	buf[2] = byte(pos >> 8)
	buf[3] = byte(pos)

	buf[4] = byte(id >> 56)
	buf[5] = byte(id >> 48)
	buf[6] = byte(id >> 40)
	buf[7] = byte(id >> 32)
	buf[8] = byte(id >> 24)
	buf[9] = byte(id >> 16)
	buf[10] = byte(id >> 8)
	buf[11] = byte(id)

	buf[12] = byte(size >> 24)
	buf[13] = byte(size >> 16)
	buf[14] = byte(size >> 8)
	buf[15] = byte(size)

	buf[16] = byte(off >> 24)
	buf[17] = byte(off >> 16)
	buf[18] = byte(off >> 8)
	buf[19] = byte(off)

	buf[20] = byte(length >> 24)
	buf[21] = byte(length >> 16)
	buf[22] = byte(length >> 8)
	buf[23] = byte(length)

	return buf
}

// isPgCheckViolation 判断是否为 PostgreSQL CHECK 约束违规 (SQLSTATE 23514)
func isPgCheckViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23514"
	}
	return false
}

func TestStore_Integration_SliceIDSeq(t *testing.T) {
	store, _ := setupChunksTestDB(t)
	ctx := context.Background()

	id1, err := store.NextSliceID(ctx)
	require.NoError(t, err)
	assert.Greater(t, id1, int64(0), "分配的 slice_id 必须大于 0")

	id2, err := store.NextSliceID(ctx)
	require.NoError(t, err)
	assert.Greater(t, id2, id1, "后续分配的 slice_id 必须严格递增")
}

func TestStore_Integration_ChunksConstraints(t *testing.T) {
	store, _ := setupChunksTestDB(t)
	ctx := context.Background()

	validSlice := makeTestSliceDescriptor(0, 100, 4096, 0, 4096)
	ino := nextTestIno()

	// 清理测试残留
	t.Cleanup(func() {
		_, _ = store.DeleteChunksByIno(context.Background(), ino)
	})

	// 1. 正常插入合法 24 字节与 48 字节数据
	_, err := store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    ino,
		Indx:   0,
		Slices: validSlice,
	})
	require.NoError(t, err, "合法 24 字节 slice 插入必须成功")

	// 2. 违反 chk_chunks_ino (ino <= 0)
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    0,
		Indx:   0,
		Slices: validSlice,
	})
	require.Error(t, err)
	assert.True(t, isPgCheckViolation(err), "ino = 0 必须触发 chk_chunks_ino CHECK 违规: %v", err)

	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    -1,
		Indx:   0,
		Slices: validSlice,
	})
	require.Error(t, err)
	assert.True(t, isPgCheckViolation(err), "ino < 0 必须触发 chk_chunks_ino CHECK 违规: %v", err)

	// 3. 违反 chk_chunks_indx (indx < 0)
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    ino,
		Indx:   -1,
		Slices: validSlice,
	})
	require.Error(t, err)
	assert.True(t, isPgCheckViolation(err), "indx < 0 必须触发 chk_chunks_indx CHECK 违规: %v", err)

	// 4. 违反 chk_chunks_slices_len: 长度为空 (0 字节)
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    ino,
		Indx:   1,
		Slices: []byte{},
	})
	require.Error(t, err)
	assert.True(t, isPgCheckViolation(err), "空 slices 必须触发 chk_chunks_slices_len 违规: %v", err)

	// 5. 违反 chk_chunks_slices_len: 长度非 24 字节整数倍 (例如 10 字节、25 字节)
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    ino,
		Indx:   2,
		Slices: make([]byte, 10),
	})
	require.Error(t, err)
	assert.True(t, isPgCheckViolation(err), "10 字节 slices 必须触发 chk_chunks_slices_len 违规: %v", err)

	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    ino,
		Indx:   3,
		Slices: make([]byte, 25),
	})
	require.Error(t, err)
	assert.True(t, isPgCheckViolation(err), "25 字节 slices 必须触发 chk_chunks_slices_len 违规: %v", err)
}

func TestStore_Integration_ChunksCRUDAndRanges(t *testing.T) {
	store, _ := setupChunksTestDB(t)
	ctx := context.Background()

	ino := nextTestIno()
	t.Cleanup(func() {
		_, _ = store.DeleteChunksByIno(context.Background(), ino)
	})

	slice1 := makeTestSliceDescriptor(0, 101, 4096, 0, 4096)
	slice2 := makeTestSliceDescriptor(4096, 102, 4096, 0, 4096)
	slice3 := makeTestSliceDescriptor(8192, 103, 4096, 0, 4096)

	// 1. AppendChunkSlice: 首次插入与追加合并
	octetLen1, err := store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    ino,
		Indx:   0,
		Slices: slice1,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(24), octetLen1)

	octetLen2, err := store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
		Ino:    ino,
		Indx:   0,
		Slices: slice2,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(48), octetLen2)

	// 2. GetChunk 读取
	chunk0, err := store.GetChunk(ctx, db.GetChunkParams{
		Ino:  ino,
		Indx: 0,
	})
	require.NoError(t, err)
	assert.Equal(t, ino, chunk0.Ino)
	assert.Equal(t, int32(0), chunk0.Indx)
	expectedSlices := append(append([]byte(nil), slice1...), slice2...)
	assert.Equal(t, expectedSlices, chunk0.Slices, "slices 应该为追加拼接后的 48 字节")

	// 3. AppendExistingChunkSlice: 仅在已有 chunk 上追加
	octetLen3, err := store.AppendExistingChunkSlice(ctx, db.AppendExistingChunkSliceParams{
		Slices: slice3,
		Ino:    ino,
		Indx:   0,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(72), octetLen3, "对已存在的 chunk 行追加应返回追加后的 72 字节")

	chunk0AfterAppend, err := store.GetChunk(ctx, db.GetChunkParams{
		Ino:  ino,
		Indx: 0,
	})
	require.NoError(t, err)
	assert.Equal(t, 72, len(chunk0AfterAppend.Slices))

	// 对不存在的 chunk 追加，应返回 0 字节且不创建新记录
	octetLenNonExistent, err := store.AppendExistingChunkSlice(ctx, db.AppendExistingChunkSliceParams{
		Slices: slice1,
		Ino:    ino,
		Indx:   999,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), octetLenNonExistent, "对不存在的 chunk 追加应返回 0 字节")

	_, err = store.GetChunk(ctx, db.GetChunkParams{
		Ino:  ino,
		Indx: 999,
	})
	require.Error(t, err, "原本不存在的 chunk 不应被意外创建")

	// 4. UpdateChunkSlices: 替换 slices (Compaction)
	compactedSlice := makeTestSliceDescriptor(0, 999, 12288, 0, 12288)
	err = store.UpdateChunkSlices(ctx, db.UpdateChunkSlicesParams{
		Slices: compactedSlice,
		Ino:    ino,
		Indx:   0,
	})
	require.NoError(t, err)

	chunk0Compacted, err := store.GetChunk(ctx, db.GetChunkParams{
		Ino:  ino,
		Indx: 0,
	})
	require.NoError(t, err)
	assert.Equal(t, compactedSlice, chunk0Compacted.Slices, "UpdateChunkSlices 必须替换为紧致的 slices")

	// 5. GetChunksInRange: 插入多个 indx 并按范围查询升序返回
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{Ino: ino, Indx: 1, Slices: slice1})
	require.NoError(t, err)
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{Ino: ino, Indx: 2, Slices: slice2})
	require.NoError(t, err)
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{Ino: ino, Indx: 4, Slices: slice3})
	require.NoError(t, err)

	rangeChunks, err := store.GetChunksInRange(ctx, db.GetChunksInRangeParams{
		Ino:       ino,
		StartIndx: 1,
		EndIndx:   3,
	})
	require.NoError(t, err)
	require.Len(t, rangeChunks, 2, "范围 [1, 3] 应匹配 indx=1 和 indx=2")
	assert.Equal(t, int32(1), rangeChunks[0].Indx)
	assert.Equal(t, int32(2), rangeChunks[1].Indx)

	// 6. DeleteChunk: 单块删除返回 RETURNING slices
	delSlices, err := store.DeleteChunk(ctx, db.DeleteChunkParams{
		Ino:  ino,
		Indx: 4,
	})
	require.NoError(t, err)
	require.Len(t, delSlices, 1)
	assert.Equal(t, slice3, delSlices[0])

	// 再次查询已不存在
	_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: ino, Indx: 4})
	require.Error(t, err)
}

func TestStore_Integration_ChunksTruncateAndBatchDeletes(t *testing.T) {
	store, _ := setupChunksTestDB(t)
	ctx := context.Background()

	ino1 := nextTestIno()
	ino2 := nextTestIno()
	t.Cleanup(func() {
		_, _ = store.DeleteChunksByIno(context.Background(), ino1)
		_, _ = store.DeleteChunksByIno(context.Background(), ino2)
	})

	s0 := makeTestSliceDescriptor(0, 200, 4096, 0, 4096)
	s1 := makeTestSliceDescriptor(0, 201, 4096, 0, 4096)
	s2 := makeTestSliceDescriptor(0, 202, 4096, 0, 4096)
	s3 := makeTestSliceDescriptor(0, 203, 4096, 0, 4096)

	for idx, s := range [][]byte{s0, s1, s2, s3} {
		_, err := store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
			Ino:    ino1,
			Indx:   int32(idx),
			Slices: s,
		})
		require.NoError(t, err)
	}

	// 1. DeleteChunksAfterIndx: 删除 indx > 1 (即删除 indx=2 和 indx=3)
	afterSlices, err := store.DeleteChunksAfterIndx(ctx, db.DeleteChunksAfterIndxParams{
		Ino:       ino1,
		AfterIndx: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, len(afterSlices), "应删除 indx=2 和 indx=3")

	// 2. DeleteChunksFromIndx: 删除 indx >= 1 (即删除 indx=1)
	fromSlices, err := store.DeleteChunksFromIndx(ctx, db.DeleteChunksFromIndxParams{
		Ino:      ino1,
		FromIndx: 1,
	})
	require.NoError(t, err)
	require.Len(t, fromSlices, 1)
	assert.Equal(t, s1, fromSlices[0])

	// 3. DeleteChunksByIno: 删除剩余的 indx=0
	inoSlices, err := store.DeleteChunksByIno(ctx, ino1)
	require.NoError(t, err)
	require.Len(t, inoSlices, 1)
	assert.Equal(t, s0, inoSlices[0])

	// 4. DeleteChunksByInos: 批量删除多个 Inode
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{Ino: ino1, Indx: 0, Slices: s0})
	require.NoError(t, err)
	_, err = store.AppendChunkSlice(ctx, db.AppendChunkSliceParams{Ino: ino2, Indx: 0, Slices: s1})
	require.NoError(t, err)

	batchSlices, err := store.DeleteChunksByInos(ctx, []int64{ino1, ino2})
	require.NoError(t, err)
	assert.Equal(t, 2, len(batchSlices), "批量删除应返回 2 个 Inode 的 slices")
}

func TestStore_Integration_SliceRefsConstraintsAndCRUD(t *testing.T) {
	store, pool := setupChunksTestDB(t)
	acquireChunksTestSliceGCLock(t, pool)
	ctx := context.Background()

	sliceID, err := store.NextSliceID(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = store.DeleteSliceRef(context.Background(), sliceID)
	})

	// 1. 约束测试: chk_slice_refs_id (slice_id <= 0)
	err = store.InsertSliceRef(ctx, db.InsertSliceRefParams{
		SliceID: 0,
		Size:    4096,
		Refs:    1,
	})
	require.Error(t, err)
	assert.True(t, isPgCheckViolation(err), "slice_id = 0 必须触发 CHECK 违规: %v", err)

	// 2. 约束测试: chk_slice_refs_size (size <= 0)
	err = store.InsertSliceRef(ctx, db.InsertSliceRefParams{
		SliceID: sliceID,
		Size:    0,
		Refs:    1,
	})
	require.Error(t, err)
	assert.True(t, isPgCheckViolation(err), "size <= 0 必须触发 CHECK 违规: %v", err)

	// 3. 正常插入
	err = store.InsertSliceRef(ctx, db.InsertSliceRefParams{
		SliceID: sliceID,
		Size:    4096,
		Refs:    1,
	})
	require.NoError(t, err)

	// 4. 读取验证
	ref, err := store.GetSliceRef(ctx, sliceID)
	require.NoError(t, err)
	assert.Equal(t, sliceID, ref.SliceID)
	assert.Equal(t, int32(4096), ref.Size)
	assert.Equal(t, int32(1), ref.Refs)
	assert.False(t, ref.CreatedAt.Time.IsZero())

	// 5. AddSliceRefCount: 增减引用计数
	err = store.AddSliceRefCount(ctx, db.AddSliceRefCountParams{
		SliceID: sliceID,
		Delta:   2,
	})
	require.NoError(t, err)

	ref, err = store.GetSliceRef(ctx, sliceID)
	require.NoError(t, err)
	assert.Equal(t, int32(3), ref.Refs, "refs 应增加至 3")

	err = store.AddSliceRefCount(ctx, db.AddSliceRefCountParams{
		SliceID: sliceID,
		Delta:   -3,
	})
	require.NoError(t, err)

	ref, err = store.GetSliceRef(ctx, sliceID)
	require.NoError(t, err)
	assert.Equal(t, int32(0), ref.Refs, "refs 应扣减至 0")

	// 6. DeleteSliceRef
	err = store.DeleteSliceRef(ctx, sliceID)
	require.NoError(t, err)

	_, err = store.GetSliceRef(ctx, sliceID)
	require.Error(t, err, "已删除的 slice_refs 应无法查询")
}

func TestStore_Integration_ListGarbageSliceRefs_SkipLocked(t *testing.T) {
	store, pool := setupChunksTestDB(t)
	acquireChunksTestSliceGCLock(t, pool)
	ctx := context.Background()

	// 清理共享库残留的非活跃垃圾切片，确保本用例隔离不受历史脏数据干扰
	_, err := pool.Exec(ctx, "DELETE FROM slice_refs WHERE refs <= 0")
	require.NoError(t, err)

	id1, err := store.NextSliceID(ctx)
	require.NoError(t, err)
	id2, err := store.NextSliceID(ctx)
	require.NoError(t, err)
	id3, err := store.NextSliceID(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = store.DeleteSliceRef(context.Background(), id1)
		_ = store.DeleteSliceRef(context.Background(), id2)
		_ = store.DeleteSliceRef(context.Background(), id3)
	})

	// id1: refs = 0 (垃圾)
	require.NoError(t, store.InsertSliceRef(ctx, db.InsertSliceRefParams{SliceID: id1, Size: 4096, Refs: 0}))
	// id2: refs = -1 (垃圾)
	require.NoError(t, store.InsertSliceRef(ctx, db.InsertSliceRefParams{SliceID: id2, Size: 8192, Refs: -1}))
	// id3: refs = 1 (活跃有效切片，不应出现在垃圾列表)
	require.NoError(t, store.InsertSliceRef(ctx, db.InsertSliceRefParams{SliceID: id3, Size: 1024, Refs: 1}))

	// 1. 普通查询: 只应扫描出 refs <= 0 的切片 (id1, id2)，且按 slice_id 升序
	garbage, err := store.ListGarbageSliceRefs(ctx, 10)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(garbage), 2)

	// 确认 id1 与 id2 均在结果中，而 id3 绝不在
	var foundIDs []int64
	for _, g := range garbage {
		foundIDs = append(foundIDs, g.SliceID)
		assert.LessOrEqual(t, g.Refs, int32(0), "返回的记录 refs 必须 <= 0")
	}
	assert.Contains(t, foundIDs, id1)
	assert.Contains(t, foundIDs, id2)
	assert.NotContains(t, foundIDs, id3, "refs = 1 的有效记录绝不能作为垃圾返回")

	// 2. 验证 FOR UPDATE SKIP LOCKED 并发跳过
	// 开启事务 A，查询并锁定 1 条记录
	txA, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = txA.Rollback(ctx) }()

	qA := db.New(txA)
	lockedA, err := qA.ListGarbageSliceRefs(ctx, 1)
	require.NoError(t, err)
	require.Len(t, lockedA, 1)
	lockedID := lockedA[0].SliceID

	// 开启事务 B，再次调用 ListGarbageSliceRefs(1)。
	// 由于事务 A 锁定了 lockedID 且查询带有 SKIP LOCKED，事务 B 必须立刻返回另一条记录，而不是阻塞等待！
	txB, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = txB.Rollback(ctx) }()

	qB := db.New(txB)
	doneCh := make(chan struct{})
	var lockedB []db.SliceRef
	var errB error

	go func() {
		lockedB, errB = qB.ListGarbageSliceRefs(ctx, 1)
		close(doneCh)
	}()

	select {
	case <-doneCh:
		require.NoError(t, errB)
		require.Len(t, lockedB, 1)
		assert.NotEqual(t, lockedID, lockedB[0].SliceID, "SKIP LOCKED 必须跳过被事务 A 锁定的行")
	case <-time.After(2 * time.Second):
		t.Fatal("ListGarbageSliceRefs 发生锁等待阻塞，SKIP LOCKED 未生效！")
	}
}
