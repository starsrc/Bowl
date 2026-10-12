package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/services/storage/internal/chunk"
)

// chunkFileHandle represents open file context for ChunkDriver.
type chunkFileHandle struct {
	mu       sync.Mutex
	diskID   string
	ino      uint64
	openMode bowlv1.OpenMode
	flags    uint32
}

// ChunkDriver 基于 NVMe Staging 本地暂存缓存与 PostgreSQL 元数据适配层实现 FileStorageDriver 接口
type ChunkDriver struct {
	staging     *chunk.StagingManager
	db          DBExecutor
	metaAdapter MetaAdapter
	uploader    *chunk.S3Uploader
	reader      *chunk.S3Reader
	gc          *chunk.GarbageCollector
}

var _ FileStorageDriver = (*ChunkDriver)(nil)

// NewChunkDriver 构造基于 NVMe Staging 缓存与数据库执行器的切片存储驱动实例。
// 可选传入 S3Uploader 实现后台异步 S3 推流与持久化。
func NewChunkDriver(staging *chunk.StagingManager, db DBExecutor, uploader ...*chunk.S3Uploader) *ChunkDriver {
	var up *chunk.S3Uploader
	if len(uploader) > 0 {
		up = uploader[0]
	}
	var r *chunk.S3Reader
	var gc *chunk.GarbageCollector
	if up != nil {
		r = chunk.NewS3Reader(up.Client(), up.Bucket(), staging)
		gc = chunk.NewGarbageCollector(db, staging, chunk.WithGCClient(up.Client(), up.Bucket()))
	} else {
		gc = chunk.NewGarbageCollector(db, staging)
	}
	var meta MetaAdapter
	if db != nil {
		meta = NewMetaAdapter(db)
	}
	return &ChunkDriver{
		staging:     staging,
		db:          db,
		metaAdapter: meta,
		uploader:    up,
		reader:      r,
		gc:          gc,
	}
}

// MetaAdapter 返回当前绑定的 JuiceFS 元数据切片账本适配器
func (d *ChunkDriver) MetaAdapter() MetaAdapter {
	if d.metaAdapter == nil && d.db != nil {
		d.metaAdapter = NewMetaAdapter(d.db)
	}
	return d.metaAdapter
}

// SetMetaAdapter 设置或更新 JuiceFS 元数据切片账本适配器
func (d *ChunkDriver) SetMetaAdapter(meta MetaAdapter) {
	d.metaAdapter = meta
}

// Uploader returns the configured S3Uploader, if any.
func (d *ChunkDriver) Uploader() *chunk.S3Uploader {
	return d.uploader
}

// SetUploader configures or updates the S3Uploader and initializes the S3Reader fallback if not set.
func (d *ChunkDriver) SetUploader(uploader *chunk.S3Uploader) {
	d.uploader = uploader
	if uploader != nil {
		d.reader = chunk.NewS3Reader(uploader.Client(), uploader.Bucket(), d.staging)
		if d.gc != nil {
			d.gc.SetClient(uploader.Client(), uploader.Bucket())
		}
	}
}

// Reader returns the configured S3Reader, if any.
func (d *ChunkDriver) Reader() *chunk.S3Reader {
	return d.reader
}

// SetReader configures or updates the S3Reader.
func (d *ChunkDriver) SetReader(reader *chunk.S3Reader) {
	d.reader = reader
}

// GC returns the underlying GarbageCollector.
func (d *ChunkDriver) GC() *chunk.GarbageCollector {
	return d.gc
}

// SetGC configures or updates the GarbageCollector.
func (d *ChunkDriver) SetGC(gc *chunk.GarbageCollector) {
	d.gc = gc
}

// RunGC runs an on-demand garbage collection cycle for the given virtual disk.
func (d *ChunkDriver) RunGC(ctx context.Context, diskID string) (int, error) {
	if d.gc == nil {
		return 0, nil
	}
	return d.gc.RunGC(ctx, diskID)
}



// StagingManager returns the underlying StagingManager.
func (d *ChunkDriver) StagingManager() *chunk.StagingManager {
	return d.staging
}

// DB returns the underlying DBExecutor.
func (d *ChunkDriver) DB() DBExecutor {
	return d.db
}

// LocateSlice 查询指定切片编号所归属的 (ino, chunk_indx)
func (d *ChunkDriver) LocateSlice(ctx context.Context, diskID string, sliceID uint64) (uint64, uint32, bool, error) {
	meta := d.MetaAdapter()
	if meta != nil {
		return meta.LocateSlice(ctx, diskID, sliceID)
	}
	return 0, 0, false, errors.New("元数据适配器未初始化")
}

// RecoverStaging triggers the recovery pipeline, scanning local NVMe staging for unflushed slices,
// resolving metadata from PostgreSQL, and re-enqueueing unpersisted slices to S3.
func (d *ChunkDriver) RecoverStaging(ctx context.Context) (*chunk.RecoveryReport, error) {
	if d.staging == nil {
		return &chunk.RecoveryReport{}, nil
	}
	return chunk.RecoverStagingPipeline(ctx, d.staging, d.uploader, d)
}

// Open opens an existing file or creates a driver handle.
func (d *ChunkDriver) Open(ctx context.Context, diskID string, ino uint64, openMode bowlv1.OpenMode, flags uint32) (HandleContext, error) {
	return &chunkFileHandle{
		diskID:   diskID,
		ino:      ino,
		openMode: openMode,
		flags:    flags,
	}, nil
}

// Read reads file data at a given offset and size from NVMe staging slices or sparse holes,
// reconstructing overlapping slices and spanning across 64MB Chunk boundaries.
func (d *ChunkDriver) Read(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, size uint32) ([]byte, bool, error) {
	if size == 0 {
		return []byte{}, false, nil
	}

	if handle != nil {
		if ch, ok := handle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if diskID == "" {
				diskID = ch.diskID
			}
			if ino == 0 {
				ino = ch.ino
			}
			ch.mu.Unlock()
		}
	}

	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return nil, false, connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	// 1. Query Inode size from PostgreSQL
	var fileSize int64
	queryInodeSize := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, queryInodeSize, int64(ino)).Scan(&fileSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, connect.Errorf(connect.CodeNotFound, "inode %d not found", ino)
		}
		return nil, false, connect.Errorf(connect.CodeInternal, "failed to query inode size: %s", err)
	}

	// 2. Boundary check: if offset >= fileSize, return EOF
	if offset >= uint64(fileSize) {
		return []byte{}, true, nil
	}

	available := uint64(fileSize) - offset
	toRead := uint64(size)
	if toRead > available {
		toRead = available
	}

	isEOF := (offset + uint64(size)) >= uint64(fileSize)

	result := make([]byte, toRead)
	var resultOffset uint64 = 0
	curOffset := offset
	endOffset := offset + toRead

	queryChunks := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)

	for curOffset < endOffset {
		curChunk := uint32(curOffset / chunk.ChunkSize)
		chunkEnd := uint64(curChunk+1) * chunk.ChunkSize
		chunkRemaining := chunkEnd - curOffset

		chunkReadLen := endOffset - curOffset
		if chunkReadLen > chunkRemaining {
			chunkReadLen = chunkRemaining
		}
		chunkPos := uint32(curOffset % chunk.ChunkSize)

		var rawSlices []byte
		err := d.db.QueryRow(ctx, queryChunks, int64(ino), int32(curChunk)).Scan(&rawSlices)
		if errors.Is(err, pgx.ErrNoRows) {
			// Chunk row does not exist: entire range is sparse hole filled with 0x00.
			// Since result is already initialized with 0x00, no copy is necessary.
		} else if err != nil {
			return nil, false, connect.Errorf(connect.CodeInternal, "failed to query chunk slices: %s", err)
		} else if len(rawSlices) > 0 {
			slices, err := chunk.DecodeSlices(rawSlices)
			if err != nil {
				return nil, false, connect.Errorf(connect.CodeInternal, "failed to decode chunk slices: %s", err)
			}

			dst := result[resultOffset : resultOffset+chunkReadLen]
			var sliceReader chunk.SliceReader = d.staging
			if d.reader != nil {
				sliceReader = d.reader.ForChunkWithContext(ctx, curChunk)
			}
			if err := chunk.ReconstituteRangeTo(dst, diskID, slices, chunkPos, uint32(chunkReadLen), sliceReader); err != nil {
				return nil, false, connect.Errorf(connect.CodeInternal, "failed to reconstitute chunk slice: %s", err)
			}
		}

		resultOffset += chunkReadLen
		curOffset += chunkReadLen
	}

	return result, isEOF, nil
}

// Write 将指定数据分片写入 NVMe 本地暂存缓存并提交 24 字节切片账本元数据至 PostgreSQL。
// 若写入区间跨越 64MB Chunk 边界，将自动切分为独立切片并提交至各自 Chunk 的元数据行。
func (d *ChunkDriver) Write(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if handle != nil {
		if ch, ok := handle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if diskID == "" {
				diskID = ch.diskID
			}
			if ino == 0 {
				ino = ch.ino
			}
			ch.mu.Unlock()
		}
	}

	if diskID == "" {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "缺少虚拟磁盘标识 diskID")
	}
	if ino == 0 {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "无效的 Inode 编号 %d", ino)
	}

	if d.staging == nil {
		return 0, connect.Errorf(connect.CodeInternal, "NVMe staging 缓存管理器未初始化")
	}
	meta := d.MetaAdapter()
	if meta == nil {
		return 0, connect.Errorf(connect.CodeInternal, "JuiceFS 元数据适配器未初始化")
	}

	curOffset := offset
	remaining := data
	endOffset := offset + uint64(len(data))

	// 循环切分并处理跨 64MB Chunk 边界的数据切片
	for curOffset < endOffset {
		curChunk := uint32(curOffset / ChunkSize)
		chunkEnd := uint64(curChunk+1) * ChunkSize
		chunkRemaining := chunkEnd - curOffset

		toWrite := uint64(len(remaining))
		if toWrite > chunkRemaining {
			toWrite = chunkRemaining
		}
		chunkPos := uint32(curOffset % ChunkSize)
		chunkData := remaining[:toWrite]

		// 1. 分配单调递增的切片唯一编号
		sliceID := d.staging.NextSliceID()

		// 2. 极速写入本地 NVMe Staging 缓存文件 (<10ms ACK)
		if err := d.staging.WriteSlice(diskID, sliceID, chunkData); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "写入 NVMe Staging 缓存切片失败: %s", err)
		}

		// 3. 构建 JuiceFS 24 字节大端序二进制切片元数据结构
		s := Slice{
			Pos:  chunkPos,
			ID:   sliceID,
			Size: uint32(toWrite),
			Off:  0,
			Len:  uint32(toWrite),
		}

		// 4. 通过 MetaAdapter 将切片原子追加至 PostgreSQL chunks 表并同步维护 slice_refs 引用账本
		if err := meta.WriteSlice(ctx, diskID, ino, curChunk, s); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "提交 JuiceFS 切片元数据失败: %s", err)
		}

		// 5. 若配置了后台 S3 上传器，异步入队推流任务
		if d.uploader != nil {
			_ = d.uploader.Enqueue(ctx, &chunk.UploadTask{
				DiskID:    diskID,
				Ino:       ino,
				ChunkIndx: curChunk,
				SliceID:   sliceID,
			})
		}

		curOffset += toWrite
		remaining = remaining[toWrite:]
	}

	return uint32(len(data)), nil
}



func (d *ChunkDriver) syncInodeBarrier(ctx context.Context, diskID string, ino uint64) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return connect.NewError(connect.CodeDeadlineExceeded, err.Error())
		}
		if errors.Is(err, context.Canceled) {
			return connect.NewError(connect.CodeCanceled, err.Error())
		}
		return err
	}

	if d.staging != nil && diskID != "" {
		if err := d.staging.SyncDisk(diskID); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to sync staging disk: %s", err)
		}
	}

	if d.uploader != nil && diskID != "" && ino > 0 {
		if err := d.uploader.WaitForIno(ctx, diskID, ino); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return connect.NewError(connect.CodeDeadlineExceeded, err.Error())
			}
			if errors.Is(err, context.Canceled) {
				return connect.NewError(connect.CodeCanceled, err.Error())
			}
			return connect.Errorf(connect.CodeInternal, "s3 persistence failure for ino %d: %s", ino, err)
		}
	}

	return nil
}

// Flush flushes dirty data for an open handle (implemented in STARSRC-123).
func (d *ChunkDriver) Flush(ctx context.Context, handle HandleContext, diskID string, ino uint64) error {
	if handle != nil {
		if ch, ok := handle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if diskID == "" {
				diskID = ch.diskID
			}
			if ino == 0 {
				ino = ch.ino
			}
			ch.mu.Unlock()
		}
	}

	return d.syncInodeBarrier(ctx, diskID, ino)
}

// Fsync synchronizes file data to persistent storage (implemented in STARSRC-123).
func (d *ChunkDriver) Fsync(ctx context.Context, handle HandleContext, diskID string, ino uint64, datasync bool) error {
	if handle != nil {
		if ch, ok := handle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if diskID == "" {
				diskID = ch.diskID
			}
			if ino == 0 {
				ino = ch.ino
			}
			ch.mu.Unlock()
		}
	}

	return d.syncInodeBarrier(ctx, diskID, ino)
}

// Truncate truncates or extends a file to the specified size.
// If shrinking to size, finds chunks and slices beyond size, decrements slice_refs for discarded slices,
// and adjusts or removes truncated slices.
func (d *ChunkDriver) Truncate(ctx context.Context, diskID string, ino uint64, size uint64) error {
	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	// 1. Query current file size from database
	var currentSize int64
	querySizeSQL := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, querySizeSQL, int64(ino)).Scan(&currentSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connect.Errorf(connect.CodeNotFound, "inode %d not found", ino)
		}
		return connect.Errorf(connect.CodeInternal, "failed to query inode size: %s", err)
	}

	// If expanding or keeping size unchanged, no slices need to be discarded.
	if size >= uint64(currentSize) {
		now := time.Now()
		updateInodeSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = $1,
    blocks = ($1::bigint + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
		_, _ = d.db.Exec(ctx, updateInodeSQL, int64(size), now, int64(ino))
		return nil
	}

	// 2. Shrinking: calculate boundary chunk and position within chunk
	targetChunkIndx := uint32(size / chunk.ChunkSize)
	targetChunkPos := uint32(size % chunk.ChunkSize)

	discardedSliceCounts := make(map[uint64]int)

	// 2.1. Find all chunks strictly beyond targetChunkIndx (or targetChunkIndx itself if targetChunkPos == 0)
	var boundaryCondition string
	if targetChunkPos == 0 {
		boundaryCondition = fmt.Sprintf("indx >= %d", targetChunkIndx)
	} else {
		boundaryCondition = fmt.Sprintf("indx > %d", targetChunkIndx)
	}

	queryBeyondChunks := fmt.Sprintf(`SELECT indx, slices FROM "%s".chunks WHERE ino = $1 AND %s`, schemaName, boundaryCondition)
	rows, err := d.db.Query(ctx, queryBeyondChunks, int64(ino))
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to query beyond chunks: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cIndx int32
		var rawSlices []byte
		if err := rows.Scan(&cIndx, &rawSlices); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to scan beyond chunk: %s", err)
		}
		if len(rawSlices) > 0 {
			slices, err := chunk.DecodeSlices(rawSlices)
			if err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to decode beyond chunk slices: %s", err)
			}
			for _, s := range slices {
				discardedSliceCounts[s.ID]++
			}
		}
	}
	if err := rows.Err(); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to iterate beyond chunks: %s", err)
	}
	rows.Close()

	// Delete fully discarded chunks rows
	deleteBeyondSQL := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1 AND %s`, schemaName, boundaryCondition)
	if _, err := d.db.Exec(ctx, deleteBeyondSQL, int64(ino)); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to delete beyond chunks: %s", err)
	}

	// 2.2. If targetChunkPos > 0, inspect boundary chunk at targetChunkIndx
	if targetChunkPos > 0 {
		var rawSlices []byte
		queryBoundaryChunk := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
		err := d.db.QueryRow(ctx, queryBoundaryChunk, int64(ino), int32(targetChunkIndx)).Scan(&rawSlices)
		if err == nil && len(rawSlices) > 0 {
			slices, err := chunk.DecodeSlices(rawSlices)
			if err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to decode boundary chunk slices: %s", err)
			}

			var preservedSlices []chunk.Slice
			for _, s := range slices {
				if s.Pos >= targetChunkPos {
					// Subcase B1: Slice starts at or after targetChunkPos -> fully discarded
					discardedSliceCounts[s.ID]++
				} else if s.Pos+s.Len > targetChunkPos {
					// Subcase B2: Slice crosses targetChunkPos -> partially truncated
					adjusted := chunk.Slice{
						Pos:  s.Pos,
						ID:   s.ID,
						Size: s.Size,
						Off:  s.Off,
						Len:  targetChunkPos - s.Pos,
					}
					preservedSlices = append(preservedSlices, adjusted)
				} else {
					// Subcase B3: Slice is entirely within [0, targetChunkPos) -> fully preserved
					preservedSlices = append(preservedSlices, s)
				}
			}

			if len(preservedSlices) == 0 {
				deleteBoundarySQL := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
				if _, err := d.db.Exec(ctx, deleteBoundarySQL, int64(ino), int32(targetChunkIndx)); err != nil {
					return connect.Errorf(connect.CodeInternal, "failed to delete boundary chunk: %s", err)
				}
			} else {
				encoded := chunk.EncodeSlices(preservedSlices)
				updateBoundarySQL := fmt.Sprintf(`UPDATE "%s".chunks SET slices = $1 WHERE ino = $2 AND indx = $3`, schemaName)
				if _, err := d.db.Exec(ctx, updateBoundarySQL, encoded, int64(ino), int32(targetChunkIndx)); err != nil {
					return connect.Errorf(connect.CodeInternal, "failed to update boundary chunk slices: %s", err)
				}
			}
		} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return connect.Errorf(connect.CodeInternal, "failed to query boundary chunk: %s", err)
		}
	}

	// 2.3. Decrement reference count in slice_refs for each fully discarded slice
	for sliceID, count := range discardedSliceCounts {
		updateRefSQL := fmt.Sprintf(`UPDATE "%s".slice_refs SET refs = refs - $1 WHERE id = $2;`, schemaName)
		if _, err := d.db.Exec(ctx, updateRefSQL, int32(count), int64(sliceID)); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to decrement slice ref: %s", err)
		}
	}

	// 2.4. Update inode size in PostgreSQL
	now := time.Now()
	updateInodeSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = $1,
    blocks = ($1::bigint + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
	if _, err := d.db.Exec(ctx, updateInodeSQL, int64(size), now, int64(ino)); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to update inode size: %s", err)
	}

	return nil
}

// Fallocate preallocates space or punches a hole in the file.
func (d *ChunkDriver) Fallocate(ctx context.Context, handle HandleContext, diskID string, ino uint64, mode uint32, offset uint64, length uint64) error {
	return nil
}

// CopyFileRange copies a range of data between files or within the same file via metadata-only zero-copy clone.
func (d *ChunkDriver) CopyFileRange(ctx context.Context, srcHandle HandleContext, srcDiskID string, srcIno uint64, srcOffset uint64, dstHandle HandleContext, dstDiskID string, dstIno uint64, dstOffset uint64, length uint64) (uint64, error) {
	if srcHandle != nil {
		if ch, ok := srcHandle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if srcDiskID == "" {
				srcDiskID = ch.diskID
			}
			if srcIno == 0 {
				srcIno = ch.ino
			}
			ch.mu.Unlock()
		}
	}
	if dstHandle != nil {
		if ch, ok := dstHandle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if dstDiskID == "" {
				dstDiskID = ch.diskID
			}
			if dstIno == 0 {
				dstIno = ch.ino
			}
			ch.mu.Unlock()
		}
	}

	if length == 0 {
		return 0, nil
	}

	srcDiskUUID, err := uuid.Parse(srcDiskID)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid src disk id: %s", err)
	}
	dstDiskUUID, err := uuid.Parse(dstDiskID)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid dst disk id: %s", err)
	}
	if srcDiskUUID != dstDiskUUID {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "cross-disk copy_file_range is not supported")
	}
	schemaName := FormatDiskSchema(srcDiskUUID)

	// 1. Query source Inode size and destination Inode existence
	var srcSize int64
	querySrcInode := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, querySrcInode, int64(srcIno)).Scan(&srcSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, connect.Errorf(connect.CodeNotFound, "src inode %d not found", srcIno)
		}
		return 0, connect.Errorf(connect.CodeInternal, "failed to query src inode: %s", err)
	}

	var dstExists bool
	checkDstInode := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM "%s".inodes WHERE ino = $1)`, schemaName)
	if err := d.db.QueryRow(ctx, checkDstInode, int64(dstIno)).Scan(&dstExists); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to query dst inode: %s", err)
	}
	if !dstExists {
		return 0, connect.Errorf(connect.CodeNotFound, "dst inode %d not found", dstIno)
	}

	if srcOffset >= uint64(srcSize) {
		return 0, nil
	}
	toCopy := length
	available := uint64(srcSize) - srcOffset
	if toCopy > available {
		toCopy = available
	}
	if toCopy == 0 {
		return 0, nil
	}

	srcStart := srcOffset
	srcEnd := srcOffset + toCopy

	startChunkIndx := int32(srcStart / chunk.ChunkSize)
	endChunkIndx := int32((srcEnd - 1) / chunk.ChunkSize)

	// 2. Query source Inode chunks intersecting [srcOffset, srcOffset+toCopy)
	queryChunks := fmt.Sprintf(`
SELECT indx, slices FROM "%s".chunks
WHERE ino = $1 AND indx >= $2 AND indx <= $3
ORDER BY indx ASC;
`, schemaName)

	rows, err := d.db.Query(ctx, queryChunks, int64(srcIno), startChunkIndx, endChunkIndx)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to query src chunks: %s", err)
	}
	defer rows.Close()

	type chunkRow struct {
		indx   uint32
		slices []chunk.Slice
	}
	var srcRows []chunkRow

	for rows.Next() {
		var cIndx int32
		var rawSlices []byte
		if err := rows.Scan(&cIndx, &rawSlices); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to scan src chunk: %s", err)
		}
		slices, err := chunk.DecodeSlices(rawSlices)
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to decode src chunk slices: %s", err)
		}
		srcRows = append(srcRows, chunkRow{
			indx:   uint32(cIndx),
			slices: slices,
		})
	}
	if err := rows.Err(); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to iterate src chunks: %s", err)
	}
	rows.Close()

	// 3. Map intersecting source slices to destination chunk intervals
	dstChunks := make(map[uint32][]byte)
	var dstChunkOrder []uint32

	type sliceRefUpdate struct {
		id   uint64
		size uint32
	}
	var sliceRefsToInc []sliceRefUpdate

	for _, cr := range srcRows {
		chunkBase := uint64(cr.indx) * uint64(chunk.ChunkSize)
		for _, s := range cr.slices {
			sliceStart := chunkBase + uint64(s.Pos)
			sliceEnd := sliceStart + uint64(s.Len)

			overlapStart := max(sliceStart, srcStart)
			overlapEnd := min(sliceEnd, srcEnd)
			if overlapStart >= overlapEnd {
				continue
			}

			subLen := overlapEnd - overlapStart
			subOff := s.Off + uint32(overlapStart-sliceStart)
			dstFileStart := dstOffset + (overlapStart - srcStart)

			curDstPos := dstFileStart
			remainingLen := subLen
			curPayloadOff := subOff

			for remainingLen > 0 {
				dstChunkIndx := uint32(curDstPos / chunk.ChunkSize)
				dstChunkOffset := uint32(curDstPos % chunk.ChunkSize)
				dstChunkRemaining := uint64(chunk.ChunkSize - dstChunkOffset)

				pieceLen := remainingLen
				if pieceLen > dstChunkRemaining {
					pieceLen = dstChunkRemaining
				}

				clonedSlice := chunk.Slice{
					Pos:  dstChunkOffset,
					ID:   s.ID,
					Size: s.Size,
					Off:  curPayloadOff,
					Len:  uint32(pieceLen),
				}
				if err := clonedSlice.Validate(); err != nil {
					return 0, connect.Errorf(connect.CodeInternal, "cloned slice invalid: %s", err)
				}

				encoded := chunk.EncodeSlice(clonedSlice)
				if _, exists := dstChunks[dstChunkIndx]; !exists {
					dstChunkOrder = append(dstChunkOrder, dstChunkIndx)
				}
				dstChunks[dstChunkIndx] = append(dstChunks[dstChunkIndx], encoded...)

				curDstPos += pieceLen
				curPayloadOff += uint32(pieceLen)
				remainingLen -= pieceLen
			}

			sliceRefsToInc = append(sliceRefsToInc, sliceRefUpdate{
				id:   s.ID,
				size: s.Size,
			})
		}
	}

	// 4. Begin database transaction for slice_refs, chunks, and inode updates
	var exec DBExecutor = d.db
	var tx pgx.Tx
	if tb, ok := d.db.(txBeginner); ok {
		var err error
		tx, err = tb.Begin(ctx)
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to begin transaction for copy_file_range: %s", err)
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
			}
		}()
		exec = tx
	}

	// 5. Increment reference count in slice_refs for each mapped source slice
	if len(sliceRefsToInc) > 0 {
		refCounts := make(map[uint64]int)
		refSizes := make(map[uint64]uint32)
		for _, ref := range sliceRefsToInc {
			refCounts[ref.id]++
			refSizes[ref.id] = ref.size
		}

		countsMap := make(map[int][]int64)
		for id, cnt := range refCounts {
			countsMap[cnt] = append(countsMap[cnt], int64(id))
		}

		for cnt, ids := range countsMap {
			updateRefSQL := fmt.Sprintf(`UPDATE "%s".slice_refs SET refs = refs + $1 WHERE id = ANY($2);`, schemaName)
			res, err := exec.Exec(ctx, updateRefSQL, int32(cnt), ids)
			if err != nil {
				return 0, connect.Errorf(connect.CodeInternal, "failed to increment slice reference count: %s", err)
			}
			if res.RowsAffected() < int64(len(ids)) {
				for _, id := range ids {
					insertRefSQL := fmt.Sprintf(`
INSERT INTO "%s".slice_refs (id, size, refs)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET refs = "%s".slice_refs.refs + EXCLUDED.refs;
`, schemaName, schemaName)
					if _, err := exec.Exec(ctx, insertRefSQL, id, int32(refSizes[uint64(id)]), int32(cnt+1)); err != nil {
						return 0, connect.Errorf(connect.CodeInternal, "failed to insert slice reference count: %s", err)
					}
				}
			}
		}
	}

	// 6. Append cloned slice metadata to destination Inode chunks
	if len(dstChunkOrder) > 0 {
		valuePlaceholders := make([]string, len(dstChunkOrder))
		args := make([]any, 0, len(dstChunkOrder)*3)
		for i, indx := range dstChunkOrder {
			p1 := i*3 + 1
			p2 := i*3 + 2
			p3 := i*3 + 3
			valuePlaceholders[i] = fmt.Sprintf("($%d, $%d, $%d)", p1, p2, p3)
			args = append(args, int64(dstIno), int32(indx), dstChunks[indx])
		}

		insertChunksSQL := fmt.Sprintf(`
INSERT INTO "%s".chunks (ino, indx, slices)
VALUES %s
ON CONFLICT (ino, indx)
DO UPDATE SET slices = chunks.slices || EXCLUDED.slices;
`, schemaName, strings.Join(valuePlaceholders, ", "))

		if _, err := exec.Exec(ctx, insertChunksSQL, args...); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to append dst chunk metadata: %s", err)
		}
	}

	// 7. Update destination Inode size/mtime/ctime in PostgreSQL
	now := time.Now()
	newEnd := int64(dstOffset + toCopy)
	updateInodeSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = GREATEST(size, $1::bigint),
    blocks = (GREATEST(size, $1::bigint) + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
	if _, err := exec.Exec(ctx, updateInodeSQL, newEnd, now, int64(dstIno)); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to update dst inode metadata: %s", err)
	}

	if tx != nil {
		if err := tx.Commit(ctx); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to commit copy_file_range transaction: %s", err)
		}
		tx = nil
	}

	return toCopy, nil
}

// Lseek seeks for data or holes within the file.
func (d *ChunkDriver) Lseek(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, whence uint32) (uint64, error) {
	return offset, nil
}

// Close closes the driver handle and frees resources.
func (d *ChunkDriver) Close(ctx context.Context, handle HandleContext) error {
	return nil
}

// DeleteInode reclaims chunks metadata and decrements slice reference counts for an unlinked/deleted Inode.
func (d *ChunkDriver) DeleteInode(ctx context.Context, diskID string, ino uint64) error {
	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	// 1. Fetch all chunks rows for the Inode
	queryChunks := fmt.Sprintf(`SELECT indx, slices FROM "%s".chunks WHERE ino = $1`, schemaName)
	rows, err := d.db.Query(ctx, queryChunks, int64(ino))
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to query inode chunks: %s", err)
	}
	defer rows.Close()

	sliceCounts := make(map[uint64]int)
	for rows.Next() {
		var indx int32
		var rawSlices []byte
		if err := rows.Scan(&indx, &rawSlices); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to scan chunk slices: %s", err)
		}
		if len(rawSlices) > 0 {
			slices, err := chunk.DecodeSlices(rawSlices)
			if err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to decode chunk slices: %s", err)
			}
			for _, s := range slices {
				sliceCounts[s.ID]++
			}
		}
	}
	if err := rows.Err(); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to iterate inode chunks: %s", err)
	}
	rows.Close()

	// 2. Decrement reference count in slice_refs for each discarded slice
	for sliceID, count := range sliceCounts {
		updateRefSQL := fmt.Sprintf(`UPDATE "%s".slice_refs SET refs = refs - $1 WHERE id = $2;`, schemaName)
		if _, err := d.db.Exec(ctx, updateRefSQL, int32(count), int64(sliceID)); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to decrement slice ref: %s", err)
		}
	}

	// 3. Delete chunks rows for this Inode
	deleteChunksSQL := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1;`, schemaName)
	if _, err := d.db.Exec(ctx, deleteChunksSQL, int64(ino)); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to delete inode chunks: %s", err)
	}

	return nil
}

// Unlink is an alias for DeleteInode.
func (d *ChunkDriver) Unlink(ctx context.Context, diskID string, ino uint64) error {
	return d.DeleteInode(ctx, diskID, ino)
}

