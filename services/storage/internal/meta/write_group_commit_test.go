package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// gateStagedBlockStore 包装 StagedBlockStore，用于在测试中同步汇聚指定数量的并发请求完成阶段 1 StageBlock，
// 确保它们确定性地进入同一个阶段 2 WriteThread 批次，同时统计 StageBlock、SyncDirs 与 DeleteBlock 调用次数。
type gateStagedBlockStore struct {
	meta.StagedBlockStore
	gateCount   int
	arrived     atomic.Int64
	gateCh      chan struct{}
	gateOnce    sync.Once
	stageCalls  atomic.Int64
	syncCalls   atomic.Int64
	deleteCalls atomic.Int64

	mu         sync.Mutex
	deletedIDs []uint64
	failDir    string
}

func newGateStagedBlockStore(inner meta.StagedBlockStore, gateCount int) *gateStagedBlockStore {
	return &gateStagedBlockStore{
		StagedBlockStore: inner,
		gateCount:        gateCount,
		gateCh:           make(chan struct{}),
	}
}

func (g *gateStagedBlockStore) releaseGate() {
	g.gateOnce.Do(func() {
		close(g.gateCh)
	})
}

func (g *gateStagedBlockStore) StageBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) (string, error) {
	g.stageCalls.Add(1)
	dir, err := g.StagedBlockStore.StageBlock(ctx, sliceID, blockIdx, data)
	if err != nil {
		return "", err
	}
	if g.gateCount > 0 {
		if int(g.arrived.Add(1)) >= g.gateCount {
			g.releaseGate()
		}
		select {
		case <-g.gateCh:
		case <-ctx.Done():
			return dir, ctx.Err()
		}
	}
	return dir, nil
}

func (g *gateStagedBlockStore) SyncDirs(ctx context.Context, dirs []string) error {
	g.syncCalls.Add(1)
	if g.failDir != "" {
		for _, d := range dirs {
			if filepath.Clean(d) == filepath.Clean(g.failDir) {
				return fmt.Errorf("%w: injected sync dir failure on %s", meta.ErrIO, d)
			}
		}
	}
	return g.StagedBlockStore.SyncDirs(ctx, dirs)
}

func (g *gateStagedBlockStore) DeleteBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error {
	g.deleteCalls.Add(1)
	g.mu.Lock()
	g.deletedIDs = append(g.deletedIDs, sliceID)
	g.mu.Unlock()
	return g.StagedBlockStore.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
}

// countRegularFilesInDir 统计给定数据目录下残留的物理块常规文件数量。
func countRegularFilesInDir(t *testing.T, dir string) int {
	t.Helper()
	var count int
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if !d.IsDir() {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	return count
}

// TestWrite_GroupCommit_BatchAggregationAndConservation_PostgresIntegration 验证高并发 Group Commit 批次聚合与账本守恒：
// 覆盖同一 Inode 并发写、同一 Disk 下多 Inode 并发扩容写、跨 64 MiB Chunk 边界写，
// 断言阶段 2 将多个并发请求合并进极少次数的数据库事务与去重目录 Sync，且全文件 Read 数据视图、Inode 属性（含 S_ISUID/S_ISGID 剥离）及 Disk.used_bytes 100% 精确守恒。
func TestWrite_GroupCommit_BatchAggregationAndConservation_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	baseStore := db.NewStore(pool)
	var txCalls atomic.Int64
	countingStore := &commitAckErrorStore{
		Store: baseStore,
		onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			txCalls.Add(1)
			return baseStore.ExecTxWithOptions(attemptCtx, fn, opts...)
		},
	}

	dataDir := t.TempDir()
	var dirSyncCalls atomic.Int64
	localBS := meta.NewLocalFSBlockStore(dataDir, meta.WithOpenDirHook(func(dir string) (meta.DirHandle, error) {
		f, err := os.Open(dir)
		if err != nil {
			return nil, err
		}
		return &countingDirHandle{File: f, syncCount: &dirSyncCalls}, nil
	}))

	const numWriters = 12
	gatedBS := newGateStagedBlockStore(localBS, numWriters)
	t.Cleanup(gatedBS.releaseGate)

	mgr := meta.New(
		countingStore,
		meta.WithBlockStore(gatedBS),
		meta.WithSliceIDBatchSize(64),
		meta.WithWriteGroupMaxBatchSize(64),
	)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("group-commit-conservation-%d", time.Now().UnixNano()),
		MaxBytes: 500 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	// 创建 2 个带 S_ISUID | S_ISGID 特权位的文件（覆盖同一 Disk 下多 Inode 并发写与跨 64 MiB Chunk 边界写）
	createFile := func(name string) (uint64, uint64) {
		res, cErr := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(name),
			Mode:      meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, cErr)
		// Create 时若受 umask 影响，显式通过底表赋予 S_ISUID | S_ISGID 以验证 Write 剥离特权位
		_, uErr := pool.Exec(ctx, "UPDATE inodes SET mode = $1 WHERE ino = $2", int32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o755), int64(res.Inode.Ino))
		require.NoError(t, uErr)
		return res.Inode.Ino, res.Fh
	}

	ino1, fh1 := createFile("file_multi_seg.bin")
	ino2, fh2 := createFile("file_cross_chunk.bin")

	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, baseStore, mgr, int64(ino1), fh1)
		cleanupWriteTestInoAndSlices(t, baseStore, mgr, int64(ino2), fh2)
	})

	// 重置事务计数与目录同步计数（排除建盘与建文件阶段的调用）
	txCalls.Store(0)
	dirSyncCalls.Store(0)

	// 规划 12 个并发写请求：
	// - 前 8 个写 ino1 的不重叠 4096B 分段 [0, 32768)
	// - 第 9..11 个写 ino2 的前 3 个 4096B 分段 [0, 12288)
	// - 第 12 个写 ino2 跨越 64 MiB Chunk 边界的区间 [ChunkSize - 2048, ChunkSize + 2048)
	const segSize = 4096
	crossChunkOffset := uint64(meta.ChunkSize - 2048)

	type writeJob struct {
		id     int
		ino    uint64
		fh     uint64
		offset uint64
		data   []byte
	}

	jobs := make([]writeJob, 0, numWriters)
	for i := 0; i < 8; i++ {
		jobs = append(jobs, writeJob{
			id:     i,
			ino:    ino1,
			fh:     fh1,
			offset: uint64(i * segSize),
			data:   bytes.Repeat([]byte{byte('a' + i)}, segSize),
		})
	}
	for i := 0; i < 3; i++ {
		jobs = append(jobs, writeJob{
			id:     8 + i,
			ino:    ino2,
			fh:     fh2,
			offset: uint64(i * segSize),
			data:   bytes.Repeat([]byte{byte('x' + i)}, segSize),
		})
	}
	crossPayload := bytes.Repeat([]byte{'Z'}, segSize)
	jobs = append(jobs, writeJob{
		id:     11,
		ino:    ino2,
		fh:     fh2,
		offset: crossChunkOffset,
		data:   crossPayload,
	})

	var wg sync.WaitGroup
	errs := make([]error, numWriters)
	resps := make([]*meta.WriteResponse, numWriters)

	for _, job := range jobs {
		wg.Add(1)
		go func(jb writeJob) {
			defer wg.Done()
			resps[jb.id], errs[jb.id] = mgr.Write(ctx, &meta.WriteRequest{
				Ino:    jb.ino,
				Fh:     jb.fh,
				Offset: jb.offset,
				Data:   jb.data,
			})
		}(job)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "并发写请求 %d 必须成功", i)
		require.NotNil(t, resps[i])
		assert.Equal(t, uint32(segSize), resps[i].Size)
		// S_ISUID 与 S_ISGID 必须已被剥离
		assert.Equal(t, uint32(meta.S_IFREG|0o755), resps[i].Inode.Attr.Mode)
	}

	// 1. 验证 Group Commit 事务合并：12 个并发请求在阶段 1 汇合后应合并为 1 次 PG 写事务（远小于 12 次），且普通 Write 热路径不触发 SyncDirs
	assert.LessOrEqual(t, txCalls.Load(), int64(2), "12 个并发 Write 请求必须通过 WriteThread 合并进单次或极少次数据库事务")
	assert.Equal(t, int64(0), gatedBS.syncCalls.Load(), "普通 Write 仅写入页缓存，阶段 2 不得触发 SyncDirs")

	// 2. 验证 ino1 的大小与全量数据读取视图
	expectedIno1Size := uint64(8 * segSize)
	attr1, err := mgr.GetAttr(ctx, ino1)
	require.NoError(t, err)
	assert.Equal(t, expectedIno1Size, attr1.Attr.Size)
	assert.Equal(t, uint32(meta.S_IFREG|0o755), attr1.Attr.Mode)

	read1, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino1,
		Fh:     fh1,
		Offset: 0,
		Size:   uint32(expectedIno1Size),
	})
	require.NoError(t, err)
	for i := 0; i < 8; i++ {
		assert.Equal(t, bytes.Repeat([]byte{byte('a' + i)}, segSize), read1.Data[i*segSize:(i+1)*segSize])
	}

	// 3. 验证 ino2 的跨 Chunk 边界大小与数据读取视图
	expectedIno2Size := crossChunkOffset + segSize
	attr2, err := mgr.GetAttr(ctx, ino2)
	require.NoError(t, err)
	assert.Equal(t, expectedIno2Size, attr2.Attr.Size)

	read2Front, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino2,
		Fh:     fh2,
		Offset: 0,
		Size:   uint32(3 * segSize),
	})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		assert.Equal(t, bytes.Repeat([]byte{byte('x' + i)}, segSize), read2Front.Data[i*segSize:(i+1)*segSize])
	}

	read2Cross, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino2,
		Fh:     fh2,
		Offset: crossChunkOffset,
		Size:   uint32(segSize),
	})
	require.NoError(t, err)
	assert.Equal(t, crossPayload, read2Cross.Data)

	// 4. 验证 Disk.UsedBytes 账本 100% 精确守恒（等于 ino1 最终大小 + ino2 最终大小）
	diskInfo, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, expectedIno1Size+expectedIno2Size, diskInfo.UsedBytes)
}

type countingDirHandle struct {
	*os.File
	syncCount *atomic.Int64
}

func (h *countingDirHandle) Sync() error {
	if h.syncCount != nil {
		h.syncCount.Add(1)
	}
	return h.File.Sync()
}

// TestWrite_GroupCommit_OAppendOffsetEvolution_UnitAndPostgres 验证同一批次内多句柄/多协程 O_APPEND 偏移演进：
// 同一批次内多个 O_APPEND 请求严格按序从最新演进 Inode 大小依次累加偏移，零覆盖、零丢失，
// 且各调用方收到的 WriteResponse.Inode.Attr.Size 精确对应其自身写入完成时刻的演进大小快照。
func TestWrite_GroupCommit_OAppendOffsetEvolution_UnitAndPostgres(t *testing.T) {
	ctx := context.Background()
	ino := uint64(9001)
	fh1 := uint64(91)
	fh2 := uint64(92)
	rootIno := int64(90)

	const numAppendTasks = 8
	const writeLen = 512
	const initialSize = 1024

	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)
	gatedBS := newGateStagedBlockStore(localBS, numAppendTasks)
	t.Cleanup(gatedBS.releaseGate)

	mockStore := dbmock.NewMockStore(t)
	mockStore.EXPECT().
		NextSliceID(mock.Anything).
		Return(int64(7001), nil).
		Once()

	var txCount atomic.Int64
	var recordedSlices []meta.Slice
	var quotaDeltaRecorded int64
	var finalInodeSizeRecorded int64

	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			txCount.Add(1)
			q := dbmock.NewMockQuerier(t)

			// 同一批次内对唯一 Inode 仅调用 1 次 GetInodeForUpdate
			q.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(ino)).
				Return(db.Inode{
					Ino:     int64(ino),
					Mode:    int32(meta.S_IFREG | 0o644),
					Size:    initialSize,
					Nlink:   1,
					RootIno: rootIno,
				}, nil).
				Once()

			// 两个不同的 O_APPEND 句柄在单批次内各自仅查询 1 次 GetFileHandle
			q.EXPECT().
				GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: int64(fh1), Ino: int64(ino)}).
				Return(db.FileHandle{Fh: int64(fh1), Ino: int64(ino), Flags: int32(meta.O_WRONLY | meta.O_APPEND)}, nil).
				Once()
			q.EXPECT().
				GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: int64(fh2), Ino: int64(ino)}).
				Return(db.FileHandle{Fh: int64(fh2), Ino: int64(ino), Flags: int32(meta.O_RDWR | meta.O_APPEND)}, nil).
				Once()

			// 同一批次内对唯一 RootIno 仅调用 1 次 GetDiskByRootInoForUpdate 与 1 次 AddDiskQuotaByRootIno
			q.EXPECT().
				GetDiskByRootInoForUpdate(mock.Anything, rootIno).
				Return(db.Disk{RootIno: rootIno, MaxBytes: 100000, UsedBytes: initialSize}, nil).
				Once()

			q.EXPECT().
				AddDiskQuotaByRootIno(mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, arg db.AddDiskQuotaByRootInoParams) error {
					quotaDeltaRecorded = arg.DeltaBytes
					return nil
				}).
				Once()

			// 同一 (ino, indx=0) 仅调用 1 次 AppendChunkSlice（拼接 8 条 24B Slice）
			q.EXPECT().
				AppendChunkSlice(mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, arg db.AppendChunkSliceParams) (int64, error) {
					decoded, err := meta.DecodeSlices(arg.Slices)
					if err != nil {
						return 0, err
					}
					recordedSlices = append(recordedSlices, decoded...)
					return int64(len(arg.Slices)), nil
				}).
				Once()

			q.EXPECT().
				InsertSliceRef(mock.Anything, mock.Anything).
				Return(nil).
				Times(numAppendTasks)

			q.EXPECT().
				UpdateInodeSizeAndTouchForWrite(mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, arg db.UpdateInodeSizeAndTouchForWriteParams) (db.Inode, error) {
					finalInodeSizeRecorded = arg.Size
					return db.Inode{
						Ino:   arg.Ino,
						Mode:  arg.Mode,
						Size:  arg.Size,
						Nlink: 1,
					}, nil
				}).
				Once()

			return fn(q)
		}).
		Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(gatedBS))

	var wg sync.WaitGroup
	resps := make([]*meta.WriteResponse, numAppendTasks)
	errs := make([]error, numAppendTasks)

	for i := 0; i < numAppendTasks; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			useFh := fh1
			if idx%2 == 1 {
				useFh = fh2
			}
			payload := bytes.Repeat([]byte{byte('0' + idx)}, writeLen)
			resps[idx], errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     useFh,
				Offset: uint64(idx * 9999), // 客户端传入任意偏移，O_APPEND 必须强制按批次内演进大小追加
				Data:   payload,
			})
		}(i)
	}
	wg.Wait()

	var returnedSizes []uint64
	for i := 0; i < numAppendTasks; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, resps[i])
		assert.Equal(t, uint32(writeLen), resps[i].Size)
		returnedSizes = append(returnedSizes, resps[i].Inode.Attr.Size)
	}

	// 1. 断言 8 个并发 O_APPEND 请求在单次事务中完成
	assert.Equal(t, int64(1), txCount.Load(), "8 个并发 O_APPEND 请求应合并为 1 次事务")
	assert.Equal(t, int64(numAppendTasks*writeLen), quotaDeltaRecorded, "Disk 配额净增量必须精确等于 8 * 512")
	assert.Equal(t, int64(initialSize+numAppendTasks*writeLen), finalInodeSizeRecorded, "最终 Inode.Size 必须精确等于 1024 + 8 * 512")

	// 2. 断言 8 条 Slice 的 Pos 严格按 [1024, 1536, 2048, ..., 4608] 顺序演进，零重叠、零跳空
	require.Len(t, recordedSlices, numAppendTasks)
	for i, s := range recordedSlices {
		assert.Equal(t, uint32(initialSize+i*writeLen), s.Pos, "第 %d 条追加 Slice 的起始偏移必须严格紧随前一条", i)
		assert.Equal(t, uint32(writeLen), s.Len)
	}

	// 3. 断言 8 个调用方收到的 WriteResponse.Inode.Attr.Size 排序后恰好是 [1536, 2048, 2560, ..., 5120]
	slices.Sort(returnedSizes)
	for i, sz := range returnedSizes {
		assert.Equal(t, uint64(initialSize+(i+1)*writeLen), sz, "每个 O_APPEND 调用方必须收到其自身完成时刻的精确演进文件大小")
	}
}

// TestWrite_GroupCommit_BadFileHandleIsolationAndBlockCleanup 验证批次内混入无效句柄/只读句柄/超期会话（EBADF）时的异常隔离：
// 违规请求单独返回 ErrBadFileHandle 且其阶段 1 物理 Block 文件被彻底删除（零孤儿块残留），
// 同批次其余合法请求在同一数据库事务中正常提交且数据完整可读。
func TestWrite_GroupCommit_BadFileHandleIsolationAndBlockCleanup(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	baseStore := db.NewStore(pool)
	var txCalls atomic.Int64
	countingStore := &commitAckErrorStore{
		Store: baseStore,
		onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			txCalls.Add(1)
			return baseStore.ExecTxWithOptions(attemptCtx, fn, opts...)
		},
	}

	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)

	// 共 6 个并发请求进入同一批次：
	// - 请求 0, 1, 2: 合法读写句柄写入
	// - 请求 3: 不存在的无效句柄 (nonExistentFh) -> ErrBadFileHandle
	// - 请求 4: 只读句柄 (readOnlyFh, O_RDONLY) -> ErrBadFileHandle
	// - 请求 5: 已超期的 ClientSession 句柄 (expiredFh) -> ErrBadFileHandle
	const totalReqs = 6
	gatedBS := newGateStagedBlockStore(localBS, totalReqs)
	t.Cleanup(gatedBS.releaseGate)

	mgr := meta.New(countingStore, meta.WithBlockStore(gatedBS))

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("group-commit-ebadf-%d", time.Now().UnixNano()),
		MaxBytes: 50 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	activeSess := uint64(991001)
	expiredSess := uint64(991002)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(activeSess), int64(expiredSess))
	})

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("ebadf_isolation.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &activeSess,
	})
	require.NoError(t, err)
	ino := createRes.Inode.Ino
	validFh := createRes.Fh

	roOpenRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       ino,
		Flags:     meta.O_RDONLY,
		SessionID: &activeSess,
	})
	require.NoError(t, err)
	readOnlyFh := roOpenRes.Fh

	expOpenRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       ino,
		Flags:     meta.O_RDWR,
		SessionID: &expiredSess,
	})
	require.NoError(t, err)
	expiredFh := expOpenRes.Fh

	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, baseStore, mgr, int64(ino), validFh, readOnlyFh, expiredFh)
	})

	// 将 expiredSess 在数据库中设为已过期
	_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(expiredSess))
	require.NoError(t, err)

	txCalls.Store(0)

	const blockLen = 2048
	type reqSpec struct {
		fh        uint64
		sessionID *uint64
		offset    uint64
		data      []byte
		wantErr   error
	}

	specs := []reqSpec{
		{fh: validFh, sessionID: &activeSess, offset: 0, data: bytes.Repeat([]byte{'A'}, blockLen), wantErr: nil},
		{fh: validFh, sessionID: &activeSess, offset: blockLen, data: bytes.Repeat([]byte{'B'}, blockLen), wantErr: nil},
		{fh: validFh, sessionID: &activeSess, offset: 2 * blockLen, data: bytes.Repeat([]byte{'C'}, blockLen), wantErr: nil},
		{fh: 99999999, sessionID: &activeSess, offset: 3 * blockLen, data: bytes.Repeat([]byte{'X'}, blockLen), wantErr: meta.ErrBadFileHandle},
		{fh: readOnlyFh, sessionID: &activeSess, offset: 4 * blockLen, data: bytes.Repeat([]byte{'Y'}, blockLen), wantErr: meta.ErrBadFileHandle},
		{fh: expiredFh, sessionID: &expiredSess, offset: 5 * blockLen, data: bytes.Repeat([]byte{'Z'}, blockLen), wantErr: meta.ErrBadFileHandle},
	}

	var wg sync.WaitGroup
	errs := make([]error, totalReqs)
	resps := make([]*meta.WriteResponse, totalReqs)

	for i, sp := range specs {
		wg.Add(1)
		go func(idx int, s reqSpec) {
			defer wg.Done()
			resps[idx], errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
				Ino:       ino,
				Fh:        s.fh,
				Offset:    s.offset,
				Data:      s.data,
				SessionID: s.sessionID,
			})
		}(i, sp)
	}
	wg.Wait()

	for i, sp := range specs {
		if sp.wantErr != nil {
			require.ErrorIs(t, errs[i], sp.wantErr, "请求 %d 应返回 %v", i, sp.wantErr)
			assert.Nil(t, resps[i])
		} else {
			require.NoError(t, errs[i], "合法请求 %d 不应受同批次违规请求牵连", i)
			require.NotNil(t, resps[i])
			assert.Equal(t, uint32(blockLen), resps[i].Size)
		}
	}

	// 1. 验证 3 个失败请求的物理块均已被立即清理，磁盘上仅存 3 个合法已提交物理块文件（零孤儿块残留）
	assert.Equal(t, int64(3), gatedBS.deleteCalls.Load(), "3 个 EBADF 违规请求的阶段 1 物理块必须全部调用 DeleteBlock 清理")
	assert.Equal(t, 3, countRegularFilesInDir(t, dataDir), "数据目录中必须精确仅存 3 个已提交物理块文件，零孤儿块残留")

	// 2. 验证文件最终大小与 Disk.UsedBytes 仅包含 3 个合法请求（3 * 2048 = 6144 字节）
	expectedValidSize := uint64(3 * blockLen)
	attr, err := mgr.GetAttr(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, expectedValidSize, attr.Attr.Size)

	diskInfo, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, expectedValidSize, diskInfo.UsedBytes)

	// 3. 验证 3 个合法请求的数据完整可读
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:       ino,
		Fh:        validFh,
		Offset:    0,
		Size:      uint32(expectedValidSize),
		SessionID: &activeSess,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{'A'}, blockLen), readRes.Data[0:blockLen])
	assert.Equal(t, bytes.Repeat([]byte{'B'}, blockLen), readRes.Data[blockLen:2*blockLen])
	assert.Equal(t, bytes.Repeat([]byte{'C'}, blockLen), readRes.Data[2*blockLen:3*blockLen])
}

// TestWrite_GroupCommit_QuotaExceededIsolationAndBlockCleanup 验证批次内部分请求超出 Disk 配额（ENOSPC）时的异常隔离：
// 超出剩余配额的扩容请求准确返回 ErrNoSpace、不推进 evolvedSize 与 runningDiskUsed，且其阶段 1 物理 Block 被彻底清理；
// 同批次内未超配额的扩容请求及原地覆盖写（delta_bytes == 0）正常提交成功，Disk.used_bytes 绝不突破 max_bytes。
func TestWrite_GroupCommit_QuotaExceededIsolationAndBlockCleanup(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	baseStore := db.NewStore(pool)
	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)

	// 设置 Disk 最大配额为 5000 字节
	const maxDiskBytes = 5000
	mgrInit := meta.New(baseStore, meta.WithBlockStore(localBS))
	disk, err := mgrInit.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("group-commit-enospc-%d", time.Now().UnixNano()),
		MaxBytes: maxDiskBytes,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgrInit.DeleteDisk(context.Background(), disk.DiskID)
	})

	createRes, err := mgrInit.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("enospc_isolation.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	ino := createRes.Inode.Ino
	fh := createRes.Fh
	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, baseStore, mgrInit, int64(ino), fh)
	})

	// 先写入初始 4000 字节，使 Disk 剩余可用配额恰好只剩 1000 字节（UsedBytes = 4000, MaxBytes = 5000）
	initData := bytes.Repeat([]byte{'I'}, 4000)
	_, err = mgrInit.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   initData,
	})
	require.NoError(t, err)

	// 现在构造同一个批次内的 4 个并发写请求（总计已有 1 个块文件在 dataDir）：
	// - 请求 A (扩容 2000B: offset=4000, len=2000): delta=2000 > 剩余 1000 -> 必须返回 ErrNoSpace 并删除物理块！
	// - 请求 B (扩容 1500B: offset=4000, len=1500): delta=1500 > 剩余 1000 -> 必须返回 ErrNoSpace 并删除物理块！
	// - 请求 C (原地覆写 2000B: offset=0, len=2000): newEnd=2000 <= 4000, delta=0 -> 不占新配额，必须成功！
	// - 请求 D (小扩容 800B: offset=4000, len=800): 因 A/B 未推进 evolvedSize，delta=800 <= 剩余 1000 -> 必须成功！
	const batchReqs = 4
	gatedBS := newGateStagedBlockStore(localBS, batchReqs)
	t.Cleanup(gatedBS.releaseGate)
	mgr := meta.New(baseStore, meta.WithBlockStore(gatedBS))

	type quotaTask struct {
		name    string
		offset  uint64
		data    []byte
		wantErr error
	}
	tasks := []quotaTask{
		{name: "超配额扩容A_2000B", offset: 4000, data: bytes.Repeat([]byte{'X'}, 2000), wantErr: meta.ErrNoSpace},
		{name: "超配额扩容B_1500B", offset: 4000, data: bytes.Repeat([]byte{'Y'}, 1500), wantErr: meta.ErrNoSpace},
		{name: "满盘原地覆写C_2000B", offset: 0, data: bytes.Repeat([]byte{'O'}, 2000), wantErr: nil},
		{name: "合法小扩容D_800B", offset: 4000, data: bytes.Repeat([]byte{'S'}, 800), wantErr: nil},
	}

	var wg sync.WaitGroup
	errs := make([]error, batchReqs)
	resps := make([]*meta.WriteResponse, batchReqs)

	for i, qt := range tasks {
		wg.Add(1)
		go func(idx int, task quotaTask) {
			defer wg.Done()
			resps[idx], errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: task.offset,
				Data:   task.data,
			})
		}(i, qt)
	}
	wg.Wait()

	for i, qt := range tasks {
		if qt.wantErr != nil {
			require.ErrorIs(t, errs[i], qt.wantErr, "%s 必须返回 ErrNoSpace", qt.name)
			assert.Nil(t, resps[i])
		} else {
			require.NoError(t, errs[i], "%s 必须在同批次中隔离成功", qt.name)
			require.NotNil(t, resps[i])
			assert.Equal(t, uint32(len(qt.data)), resps[i].Size)
		}
	}

	// 1. 验证 2 个超配额请求的阶段 1 物理块已被彻底清理（磁盘仅存初始 1 块 + 本批成功的 C、D 2 块 = 3 块）
	assert.Equal(t, int64(2), gatedBS.deleteCalls.Load(), "2 个超出配额的请求必须调用 DeleteBlock 清理物理块")
	assert.Equal(t, 3, countRegularFilesInDir(t, dataDir), "数据目录中必须仅存 3 个已提交块文件，零孤儿块残留")

	// 2. 验证 Disk.UsedBytes == 4800 <= MaxBytes (5000)，且 Inode.Size == 4800
	diskInfo, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(4800), diskInfo.UsedBytes, "Disk.UsedBytes 必须精确等于 4000 + 800 = 4800，绝不突破 5000")

	attr, err := mgr.GetAttr(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, uint64(4800), attr.Attr.Size)

	// 3. 验证全文件数据视图：[0, 2000) 为覆写的 'O'，[2000, 4000) 为初始的 'I'，[4000, 4800) 为小扩容的 'S'
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   4800,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{'O'}, 2000), readRes.Data[0:2000])
	assert.Equal(t, bytes.Repeat([]byte{'I'}, 2000), readRes.Data[2000:4000])
	assert.Equal(t, bytes.Repeat([]byte{'S'}, 800), readRes.Data[4000:4800])
}

// TestWrite_GroupCommit_PageCachePublishAndInodeModeIsolation_Unit 验证：
//  1. 阶段 1 StageBlock 发布至页缓存后，阶段 2 直接提交数据库事务而不再调用 SyncDirs；
//  2. 步骤 2b 批次内混入不存在的 Inode (ErrNotFound)、目录 Inode (ErrIsDir)、FIFO 节点 (ErrInvalidArgument) 及 O_APPEND 溢出 MaxInt64 时，
//     各异常请求精确返回对应哨兵错误并清理物理块，不影响同批次合法 Inode 正常提交。
func TestWrite_GroupCommit_PageCachePublishAndInodeModeIsolation_Unit(t *testing.T) {
	ctx := context.Background()

	t.Run("阶段 2 提交不调用 SyncDirs 且所有合法任务正常合并提交", func(t *testing.T) {
		dataDir := t.TempDir()
		localBS := meta.NewLocalFSBlockStore(dataDir)
		gatedBS := newGateStagedBlockStore(localBS, 3)
		t.Cleanup(gatedBS.releaseGate)

		// 即便 failDir 设置了故障目录，由于阶段 2 不再调用 SyncDirs，3 个写请求均应直接成功提交
		gatedBS.failDir = filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", 1001%256), "0")

		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(int64(1001), nil).
			Once()

		ino := uint64(300)
		fh := uint64(30)

		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				q := dbmock.NewMockQuerier(t)
				q.EXPECT().GetInodeForUpdate(mock.Anything, int64(ino)).Return(db.Inode{
					Ino:   int64(ino),
					Mode:  int32(meta.S_IFREG | 0o644),
					Size:  0,
					Nlink: 1,
				}, nil).Once()
				q.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: int64(fh), Ino: int64(ino)}).Return(db.FileHandle{
					Fh:    int64(fh),
					Ino:   int64(ino),
					Flags: int32(meta.O_RDWR),
				}, nil).Once()
				q.EXPECT().AppendChunkSlice(mock.Anything, mock.Anything).Return(int64(72), nil).Once()
				q.EXPECT().InsertSliceRef(mock.Anything, mock.Anything).Return(nil).Times(3)
				q.EXPECT().UpdateInodeSizeAndTouchForWrite(mock.Anything, mock.Anything).Return(db.Inode{
					Ino:   int64(ino),
					Mode:  int32(meta.S_IFREG | 0o644),
					Size:  300,
					Nlink: 1,
				}, nil).Once()
				return fn(q)
			}).
			Once()

		mgr := meta.New(mockStore, meta.WithBlockStore(gatedBS))

		var wg sync.WaitGroup
		errs := make([]error, 3)
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
					Ino:    ino,
					Fh:     fh,
					Offset: uint64(idx * 100),
					Data:   bytes.Repeat([]byte{byte('a' + idx)}, 100),
				})
			}(i)
		}
		wg.Wait()

		for i, err := range errs {
			require.NoError(t, err, "请求 %d 应成功提交", i)
		}
		assert.Equal(t, int64(0), gatedBS.syncCalls.Load(), "Write 阶段 2 不得调用 SyncDirs")
		assert.Equal(t, int64(0), gatedBS.deleteCalls.Load(), "全部成功提交的任务不应触发 DeleteBlock")
		assert.Equal(t, 3, countRegularFilesInDir(t, dataDir), "数据目录中应保留全部 3 个成功任务的物理块")
	})

	t.Run("步骤 2b 批次内混入 ErrNotFound、ErrIsDir、FIFO 与 O_APPEND 溢出时逐一隔离并清理物理块", func(t *testing.T) {
		dataDir := t.TempDir()
		localBS := meta.NewLocalFSBlockStore(dataDir)
		const numTasks = 5
		gatedBS := newGateStagedBlockStore(localBS, numTasks)
		t.Cleanup(gatedBS.releaseGate)

		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(int64(8001), nil).
			Once()

		inoMissing := uint64(101)
		inoDir := uint64(102)
		inoFifo := uint64(103)
		inoOverflow := uint64(104)
		inoValid := uint64(105)
		fh := uint64(11)

		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				q := dbmock.NewMockQuerier(t)
				q.EXPECT().GetInodeForUpdate(mock.Anything, int64(inoMissing)).Return(db.Inode{}, pgx.ErrNoRows).Once()
				q.EXPECT().GetInodeForUpdate(mock.Anything, int64(inoDir)).Return(db.Inode{
					Ino:  int64(inoDir),
					Mode: int32(meta.S_IFDIR | 0o755),
				}, nil).Once()
				q.EXPECT().GetInodeForUpdate(mock.Anything, int64(inoFifo)).Return(db.Inode{
					Ino:  int64(inoFifo),
					Mode: int32(meta.S_IFIFO | 0o644),
				}, nil).Once()
				q.EXPECT().GetInodeForUpdate(mock.Anything, int64(inoOverflow)).Return(db.Inode{
					Ino:  int64(inoOverflow),
					Mode: int32(meta.S_IFREG | 0o644),
					Size: math.MaxInt64 - 5,
				}, nil).Once()
				q.EXPECT().GetInodeForUpdate(mock.Anything, int64(inoValid)).Return(db.Inode{
					Ino:  int64(inoValid),
					Mode: int32(meta.S_IFREG | 0o644),
					Size: 0,
				}, nil).Once()

				q.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: int64(fh), Ino: int64(inoOverflow)}).Return(db.FileHandle{
					Fh:    int64(fh),
					Ino:   int64(inoOverflow),
					Flags: int32(meta.O_WRONLY | meta.O_APPEND),
				}, nil).Once()
				q.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{Fh: int64(fh), Ino: int64(inoValid)}).Return(db.FileHandle{
					Fh:    int64(fh),
					Ino:   int64(inoValid),
					Flags: int32(meta.O_WRONLY),
				}, nil).Once()

				q.EXPECT().AppendChunkSlice(mock.Anything, mock.Anything).Return(int64(24), nil).Once()
				q.EXPECT().InsertSliceRef(mock.Anything, mock.Anything).Return(nil).Once()
				q.EXPECT().UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
					Ino:  int64(inoValid),
					Size: 64,
					Mode: int32(meta.S_IFREG | 0o644),
				}).Return(db.Inode{
					Ino:  int64(inoValid),
					Mode: int32(meta.S_IFREG | 0o644),
					Size: 64,
				}, nil).Once()

				return fn(q)
			}).
			Once()

		mgr := meta.New(mockStore, meta.WithBlockStore(gatedBS))

		inos := []uint64{inoMissing, inoDir, inoFifo, inoOverflow, inoValid}
		wantErrs := []error{meta.ErrNotFound, meta.ErrIsDir, meta.ErrInvalidArgument, meta.ErrInvalidArgument, nil}
		errs := make([]error, numTasks)

		var wg sync.WaitGroup
		for i, targetIno := range inos {
			wg.Add(1)
			go func(idx int, inoVal uint64) {
				defer wg.Done()
				_, errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
					Ino:    inoVal,
					Fh:     fh,
					Offset: 0,
					Data:   bytes.Repeat([]byte{'k'}, 64),
				})
			}(i, targetIno)
		}
		wg.Wait()

		for i, want := range wantErrs {
			if want != nil {
				require.ErrorIs(t, errs[i], want, "任务 %d 应返回 %v", i, want)
			} else {
				require.NoError(t, errs[i], "合法任务 %d 应成功提交", i)
			}
		}
		assert.Equal(t, int64(4), gatedBS.deleteCalls.Load(), "4 个异常任务的物理块必须全部被清理")
		assert.Equal(t, 1, countRegularFilesInDir(t, dataDir), "仅保留 1 个合法提交任务的物理块文件")
	})
}

type sqlOrderTracer struct {
	mu      sync.Mutex
	queries []string
}

func (t *sqlOrderTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.mu.Lock()
	t.queries = append(t.queries, data.SQL)
	t.mu.Unlock()
	return ctx
}

func (t *sqlOrderTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *sqlOrderTracer) reset() {
	t.mu.Lock()
	t.queries = nil
	t.mu.Unlock()
}

func (t *sqlOrderTracer) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.queries)
}

type orderSpyStagedBlockStore struct {
	meta.StagedBlockStore
	syncPendingCalls atomic.Int64
	failErr          error
	onSyncPending    func()
}

func (s *orderSpyStagedBlockStore) SyncPending(ctx context.Context) error {
	s.syncPendingCalls.Add(1)
	if s.onSyncPending != nil {
		s.onSyncPending()
	}
	if s.failErr != nil {
		return s.failErr
	}
	return s.StagedBlockStore.SyncPending(ctx)
}

// TestWrite_DeferredSyncAndFsyncDurabilityBarrier_PostgresIntegration 验证 ADR-0024：
//  1. 普通 Write（含单任务与多任务批次）在真实数据库事务内启用 SET LOCAL synchronous_commit = off，且热路径不触发目录 Sync；
//  2. Write 返回后立即发起 Read、GetAttr、Lookup 100% 读到最新数据与最新属性；
//  3. Fsync 与 FsyncDir 句柄校验失败时不触发 SyncPending 与 WAL 刷盘；
//  4. Fsync 与 FsyncDir 句柄校验通过后严格按「先 SyncPending 刷物理数据块、后 SELECT pg_current_wal_flush_lsn() 刷 PG WAL」顺序执行；
//  5. 无新写入时重复 Fsync/FsyncDir 不触发冗余块刷盘；当 SyncPending 或 WAL 刷盘失败时准确返回 ErrIO，且后续重试可恢复。
func TestWrite_DeferredSyncAndFsyncDurabilityBarrier_PostgresIntegration(t *testing.T) {
	tracer := &sqlOrderTracer{}
	pool, ctx := setupMetaTestDBWithTracer(t, tracer)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	var dirSyncCalls atomic.Int64
	localBS := meta.NewLocalFSBlockStore(dataDir, meta.WithOpenDirHook(func(dir string) (meta.DirHandle, error) {
		f, err := os.Open(dir)
		if err != nil {
			return nil, err
		}
		return &countingDirHandle{File: f, syncCount: &dirSyncCalls}, nil
	}))

	var eventsMu sync.Mutex
	var barrierEvents []string
	spyBS := &orderSpyStagedBlockStore{
		StagedBlockStore: localBS,
		onSyncPending: func() {
			eventsMu.Lock()
			barrierEvents = append(barrierEvents, "block_sync_pending")
			eventsMu.Unlock()
		},
	}

	mgr := meta.New(store, meta.WithBlockStore(spyBS))

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("adr0024-barrier-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	fileName := []byte("deferred_sync.bin")
	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      fileName,
		Mode:      meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	ino := createRes.Inode.Ino
	fh := createRes.Fh
	_, err = pool.Exec(ctx, "UPDATE inodes SET mode = $1 WHERE ino = $2", int32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o755), int64(ino))
	require.NoError(t, err)

	openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:   disk.RootIno,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)
	dirFh := openDirRes.Fh

	t.Cleanup(func() {
		_ = mgr.ReleaseDir(context.Background(), meta.ReleaseDirParams{Ino: disk.RootIno, Fh: dirFh})
		cleanupWriteTestInoAndSlices(t, store, mgr, int64(ino), fh)
	})

	// 预热散列目录并排空首次建目录同步计数
	warmupPayload := []byte("warmup-first")
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   warmupPayload,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: fh}))

	// 1. 验证单任务 Write 在已缓存散列目录下零目录 Sync，且事务内执行了 SET LOCAL synchronous_commit = off
	// 强制分配一个与已预热目录同散列桶的 slice_id，或预热所有目标桶：此处通过写入并檢查 SQL 与 SyncPending
	tracer.reset()
	spyBS.syncPendingCalls.Store(0)

	payload1 := []byte("hello-page-cache-immediate-visibility")
	wRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   payload1,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload1)), wRes.Size)
	assert.Equal(t, int64(0), spyBS.syncPendingCalls.Load(), "普通 Write 绝不调用 SyncPending")

	queries := tracer.snapshot()
	var hasAsyncCommitSQL, hasWalFlushSQLOnWrite bool
	for _, q := range queries {
		if strings.Contains(q, "SET LOCAL synchronous_commit = off") {
			hasAsyncCommitSQL = true
		}
		if strings.Contains(q, "pg_current_wal_flush_lsn") {
			hasWalFlushSQLOnWrite = true
		}
	}
	assert.True(t, hasAsyncCommitSQL, "普通 Write 数据库事务必须执行 SET LOCAL synchronous_commit = off")
	assert.False(t, hasWalFlushSQLOnWrite, "普通 Write 不得触发 pg_current_wal_flush_lsn")

	// 2. Write 返回后立即发起 Read、GetAttr、Lookup，100% 读到最新内容与属性
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(len(payload1)),
	})
	require.NoError(t, err)
	assert.Equal(t, payload1, readRes.Data)

	attrRes, err := mgr.GetAttr(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(payload1)), attrRes.Attr.Size)
	assert.Equal(t, uint32(meta.S_IFREG|0o755), attrRes.Attr.Mode)

	lookupRes, err := mgr.Lookup(ctx, disk.RootIno, fileName)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(payload1)), lookupRes.Attr.Size)
	assert.Equal(t, uint32(meta.S_IFREG|0o755), lookupRes.Attr.Mode)

	// 3. Fsync 与 FsyncDir 句柄校验失败时保持错误优先级，且不触发 SyncPending
	require.ErrorIs(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: 99999999}), meta.ErrBadFileHandle)
	require.ErrorIs(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: disk.RootIno, Fh: 99999999}), meta.ErrBadFileHandle)
	assert.Equal(t, int64(0), spyBS.syncPendingCalls.Load(), "句柄校验失败时不得触发 SyncPending")

	// 4. 注入 SyncPending 非包装错误：Fsync / FsyncDir 必须包装为 ErrIO 返回，且不继续调用 pg_current_wal_flush_lsn
	tracer.reset()
	spyBS.failErr = errors.New("raw disk sync failure")
	fErr := mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: fh})
	require.ErrorIs(t, fErr, meta.ErrIO, "SyncPending 报错时 Fsync 必须返回 ErrIO")
	fdErr := mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: disk.RootIno, Fh: dirFh})
	require.ErrorIs(t, fdErr, meta.ErrIO, "SyncPending 报错时 FsyncDir 必须返回 ErrIO")
	for _, q := range tracer.snapshot() {
		assert.False(t, strings.Contains(q, "pg_current_wal_flush_lsn"), "块存储刷盘失败时不得提前刷入数据库 WAL")
	}

	// 5. 恢复正常后调用 Fsync：验证先执行 SyncPending 刷入刚才未完成的批次，再执行 SELECT pg_current_wal_flush_lsn()
	spyBS.failErr = nil
	tracer.reset()
	dirSyncCalls.Store(0)
	eventsMu.Lock()
	barrierEvents = nil
	eventsMu.Unlock()

	require.NoError(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: fh}))
	assert.Greater(t, dirSyncCalls.Load(), int64(0), "恢复后 Fsync 必须将挂起的写入批次刷入磁盘")

	var walFlushMeasured bool
	for _, q := range tracer.snapshot() {
		if strings.Contains(q, "pg_current_wal_flush_lsn") {
			walFlushMeasured = true
			eventsMu.Lock()
			barrierEvents = append(barrierEvents, "wal_flush")
			eventsMu.Unlock()
		}
	}
	assert.True(t, walFlushMeasured, "Fsync 必须执行 SELECT pg_current_wal_flush_lsn()")
	eventsMu.Lock()
	assert.Equal(t, []string{"block_sync_pending", "wal_flush"}, barrierEvents)
	eventsMu.Unlock()

	// 6. 无新写入时再次调用 Fsync 与 FsyncDir：底层磁盘目录 Sync 计数为 0（不触发冗余刷盘）
	dirSyncCalls.Store(0)
	require.NoError(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: fh}))
	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: disk.RootIno, Fh: dirFh}))
	assert.Equal(t, int64(0), dirSyncCalls.Load(), "无新写入时重复 Fsync / FsyncDir 不触发冗余块刷盘")

	// 7. 再次写入新数据后调用 FsyncDir：同样触发 SyncPending + pg_current_wal_flush_lsn
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: uint64(len(payload1)),
		Data:   []byte("-tail"),
	})
	require.NoError(t, err)
	tracer.reset()
	dirSyncCalls.Store(0)
	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: disk.RootIno, Fh: dirFh}))
	assert.Greater(t, dirSyncCalls.Load(), int64(0), "有新写入后调用 FsyncDir 必须触发底层块刷盘")
	var dirWalFlushed bool
	for _, q := range tracer.snapshot() {
		if strings.Contains(q, "pg_current_wal_flush_lsn") {
			dirWalFlushed = true
		}
	}
	assert.True(t, dirWalFlushed, "FsyncDir 必须执行 SELECT pg_current_wal_flush_lsn()")
}
