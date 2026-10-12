package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/services/storage/internal/chunk"
	jfsChunk "github.com/starsrc/bowl/services/storage/internal/juicefs/chunk"
	"github.com/starsrc/bowl/services/storage/internal/juicefs/object"
)

// chunkFileHandle 维护切片驱动层面的打开文件上下文、读写状态与内存缓存页
type chunkFileHandle struct {
	mu       sync.Mutex
	diskID   string
	ino      uint64
	openMode bowlv1.OpenMode
	flags    uint32
	closed   bool
	offset   uint64
	pages    []*jfsChunk.Page
	buffer   []byte
}

func (h *chunkFileHandle) updateOffset(off uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.offset = off
}

func (h *chunkFileHandle) getOffset() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.offset
}

// resolveHandle 校验文件句柄状态并提取 diskID 与 ino 缺省值
func resolveHandle(handle HandleContext, diskID *string, ino *uint64) (*chunkFileHandle, error) {
	if handle == nil {
		return nil, nil
	}
	ch, ok := handle.(*chunkFileHandle)
	if !ok {
		return nil, nil
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "文件句柄已关闭")
	}
	if *diskID == "" {
		*diskID = ch.diskID
	}
	if *ino == 0 {
		*ino = ch.ino
	}
	return ch, nil
}


// AttachPage 向当前文件句柄移交内存 Page 缓冲区所有权，交由句柄生命周期统一管理与释放
func (h *chunkFileHandle) AttachPage(p *jfsChunk.Page) {
	if p == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.pages = append(h.pages, p)
	} else {
		p.Release()
	}
}

// Pages 获取当前文件句柄持有的全部内存 Page 缓冲区列表副本（主要用于测试与状态断言）
func (h *chunkFileHandle) Pages() []*jfsChunk.Page {
	h.mu.Lock()
	defer h.mu.Unlock()
	res := make([]*jfsChunk.Page, len(h.pages))
	copy(res, h.pages)
	return res
}

// IsClosed 检查当前文件句柄是否已安全关闭
func (h *chunkFileHandle) IsClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
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

// ObjectStorage 返回底层内联的 JuiceFS 对象存储引擎驱动（若已配置）
func (d *ChunkDriver) ObjectStorage() object.ObjectStorage {
	if d.uploader != nil {
		return d.uploader.ObjectStorage()
	}
	return nil
}

// Barrier 返回针对指定 Inode 的持久化同步屏障信道
func (d *ChunkDriver) Barrier(diskID string, ino uint64) <-chan error {
	if d.uploader != nil {
		return d.uploader.Barrier(diskID, ino)
	}
	ch := make(chan error, 1)
	ch <- nil
	return ch
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

// TrackedOrphans 查询指定磁盘当前记录的待物理回收孤儿切片编号列表（主要用于测试与状态断言）
func (d *ChunkDriver) TrackedOrphans(diskID string) []uint64 {
	if d.gc == nil {
		return nil
	}
	return d.gc.TrackedOrphans(diskID)
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

	ch, err := resolveHandle(handle, &diskID, &ino)
	if err != nil {
		return nil, false, err
	}

	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return nil, false, connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	// 1. 查询 Inode 文件大小
	var fileSize int64
	queryInodeSize := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, queryInodeSize, int64(ino)).Scan(&fileSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, connect.Errorf(connect.CodeNotFound, "inode %d not found", ino)
		}
		return nil, false, connect.Errorf(connect.CodeInternal, "failed to query inode size: %s", err)
	}

	// 2. 边界检查：若 offset 已超出或等于文件大小，返回空数据与 EOF
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

	meta := d.MetaAdapter()
	if meta == nil {
		return nil, false, connect.Errorf(connect.CodeInternal, "JuiceFS 元数据适配器未初始化")
	}

	// 3. 循环跨 64MB Chunk 边界读取数据并重构切片图层
	for curOffset < endOffset {
		curChunk := uint32(curOffset / ChunkSize)
		chunkEnd := uint64(curChunk+1) * ChunkSize
		chunkRemaining := chunkEnd - curOffset

		chunkReadLen := endOffset - curOffset
		if chunkReadLen > chunkRemaining {
			chunkReadLen = chunkRemaining
		}
		chunkPos := uint32(curOffset % ChunkSize)

		// 3.1. 通过 MetaAdapter 获取经过 JuiceFS 图层重构后的无重叠切片视图
		reconstitutedSlices, err := meta.ReadReconstitutedSlices(ctx, diskID, ino, curChunk)
		if err != nil {
			return nil, false, connect.Errorf(connect.CodeInternal, "查询并重组切片元数据失败: %s", err)
		}

		if len(reconstitutedSlices) > 0 {
			dst := result[resultOffset : resultOffset+chunkReadLen]
			reqStart := chunkPos
			reqEnd := chunkPos + uint32(chunkReadLen)

			var curSlicePos uint32 = 0
			for _, s := range reconstitutedSlices {
				sliceStart := curSlicePos
				sliceEnd := curSlicePos + s.Len
				curSlicePos += s.Len

				// 检查当前重构切片与请求区间的重叠交集
				if sliceEnd <= reqStart || sliceStart >= reqEnd {
					continue
				}

				overlapStart := max(sliceStart, reqStart)
				overlapEnd := min(sliceEnd, reqEnd)
				overlapLen := overlapEnd - overlapStart

				dstStart := overlapStart - reqStart
				dstEnd := dstStart + overlapLen

				// 稀疏空洞 (Id == 0): 不查询 S3 或本地缓存，自动填充为全零 0x00
				if s.Id == 0 {
					clear(dst[dstStart:dstEnd])
					continue
				}

				// 计算在物理切片载荷中的有效偏移量
				sliceOff := s.Off + (overlapStart - sliceStart)

				// 从本地 NVMe Staging 缓存（热路径）或 S3 HTTP Range（冷路径）拉取切片局部数据
				subPayload, err := d.readSliceRange(ctx, diskID, curChunk, s.Id, sliceOff, overlapLen)
				if err != nil {
					return nil, false, connect.Errorf(connect.CodeInternal, "读取切片 %d 数据失败: %s", s.Id, err)
				}
				if uint32(len(subPayload)) < overlapLen {
					return nil, false, connect.Errorf(connect.CodeInternal, "切片 %d 载荷损坏: 期望长度 %d，实际读取 %d", s.Id, overlapLen, len(subPayload))
				}

				copy(dst[dstStart:dstEnd], subPayload[:overlapLen])
			}
		}

		resultOffset += chunkReadLen
		curOffset += chunkReadLen
	}

	if ch != nil {
		ch.updateOffset(offset + uint64(len(result)))
	}
	return result, isEOF, nil
}

// readSliceRange 读取指定切片的指定有效载荷区间 [offset, offset+length)。
// 优先命中本地 NVMe Staging 缓存热路径；本地缺失时透明通过 S3 HTTP Range 发起 4MB 物理分块局部拉取冷路径。
func (d *ChunkDriver) readSliceRange(ctx context.Context, diskID string, chunkIndx uint32, sliceID uint64, offset uint32, length uint32) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	// 1. 热路径 (Hot path): 优先尝试从本地 NVMe Staging 缓存读取
	if d.staging != nil {
		data, err := d.staging.ReadSliceRange(diskID, sliceID, offset, length)
		if err == nil && uint32(len(data)) == length {
			return data, nil
		}
	}

	// 2. 冷路径 (Cold path): 本地缺失时，透明通过 S3 HTTP Range 拉取 4MB Block 区间
	if d.reader != nil {
		return d.reader.ReadSliceRange(ctx, diskID, chunkIndx, sliceID, offset, length)
	}

	return nil, fmt.Errorf("切片 %d 本地缓存缺失且未配置 S3 回源读取器", sliceID)
}

// Write 将指定数据分片写入 NVMe 本地暂存缓存并提交 24 字节切片账本元数据至 PostgreSQL。
// 若写入区间跨越 64MB Chunk 边界，将自动切分为独立切片并提交至各自 Chunk 的元数据行。
func (d *ChunkDriver) Write(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ch, err := resolveHandle(handle, &diskID, &ino)
	if err != nil {
		return 0, err
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

	if ch != nil {
		ch.updateOffset(offset + uint64(len(data)))
	}
	return uint32(len(data)), nil
}



// syncInodeBarrier 执行针对指定 Inode 的持久化同步屏障：
// 1. 同步刷新本地 NVMe Staging 缓存目录元数据与文件缓冲至持久介质。
// 2. 阻塞等待该 Inode 在途的全部 4MB S3/MinIO 物理分块异步上传任务持久化完成。
// 3. 处理瞬时错误重试衰竭、不可恢复错误与超时/取消，向 Connect-RPC 调用方正确回传错误。
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

	// 1. 同步本地 NVMe Staging 缓存目录与文件数据
	if d.staging != nil && diskID != "" {
		if err := d.staging.SyncDisk(diskID); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to sync staging disk: %s", err)
		}
	}

	// 2. 阻塞等待该 Inode 在途的全部 4MB 物理分块持久化至 S3/MinIO
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

// Flush 针对打开文件句柄同步脏切片并等待后台 S3 推流屏障达成 (FUSE_FLUSH)
func (d *ChunkDriver) Flush(ctx context.Context, handle HandleContext, diskID string, ino uint64) error {
	if _, err := resolveHandle(handle, &diskID, &ino); err != nil {
		return err
	}
	return d.syncInodeBarrier(ctx, diskID, ino)
}

// Fsync 针对文件数据与元数据执行持久化同步屏障，挂起等待全部在途 4MB 物理分块刷盘至 S3 (FUSE_FSYNC)
func (d *ChunkDriver) Fsync(ctx context.Context, handle HandleContext, diskID string, ino uint64, datasync bool) error {
	if _, err := resolveHandle(handle, &diskID, &ino); err != nil {
		return err
	}
	return d.syncInodeBarrier(ctx, diskID, ino)
}

// Truncate 截断或扩容文件至指定大小。
// 缩短时：通过 MetaAdapter 裁剪边界切片并清除越界切片元数据，递减引用计数并联动 GC 孤儿追踪；读取超出新大小的区间将返回 EOF。
// 扩容时：文件扩展形成稀疏空洞，无需分配物理块，读取扩容区间自动返回全零 (0x00)。
func (d *ChunkDriver) Truncate(ctx context.Context, diskID string, ino uint64, size uint64) error {
	if diskID == "" {
		return connect.Errorf(connect.CodeInvalidArgument, "缺少虚拟磁盘标识 diskID")
	}
	if ino == 0 {
		return connect.Errorf(connect.CodeInvalidArgument, "无效的 Inode 编号 %d", ino)
	}

	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	// 1. 从数据库查询当前文件大小
	var currentSize int64
	querySizeSQL := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, querySizeSQL, int64(ino)).Scan(&currentSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connect.Errorf(connect.CodeNotFound, "inode %d not found", ino)
		}
		return connect.Errorf(connect.CodeInternal, "failed to query inode size: %s", err)
	}

	// 2. 若扩容或保持大小不变，无需淘汰切片，扩容区间自然形成稀疏空洞
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
		if _, err := d.db.Exec(ctx, updateInodeSQL, int64(size), now, int64(ino)); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to update inode size: %s", err)
		}
		return nil
	}

	// 3. 缩短截断：通过 MetaAdapter 清理越界切片并裁剪边界切片
	meta := d.MetaAdapter()
	if meta == nil {
		return connect.Errorf(connect.CodeInternal, "JuiceFS 元数据适配器未初始化")
	}

	orphans, err := meta.TruncateSlices(ctx, diskID, ino, size)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "截断切片元数据失败: %s", err)
	}

	// 联动垃圾回收器跟踪孤儿切片
	if d.gc != nil && len(orphans) > 0 {
		d.gc.RegisterDisk(diskID)
		d.gc.TrackOrphans(diskID, orphans...)
	}

	// 4. 更新 Inode 元数据 (size/blocks/mtime/ctime)
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

// Fallocate 空间预分配与区间打洞 (FALLOC_FL_PUNCH_HOLE)。
// 打洞模式下通过 MetaAdapter 覆盖空洞切片释放物理存储；预分配模式下扩展文件大小形成稀疏空洞。
func (d *ChunkDriver) Fallocate(ctx context.Context, handle HandleContext, diskID string, ino uint64, mode uint32, offset uint64, length uint64) error {
	if _, err := resolveHandle(handle, &diskID, &ino); err != nil {
		return err
	}

	if diskID == "" {
		return connect.Errorf(connect.CodeInvalidArgument, "缺少虚拟磁盘标识 diskID")
	}
	if ino == 0 {
		return connect.Errorf(connect.CodeInvalidArgument, "无效的 Inode 编号 %d", ino)
	}
	if length == 0 {
		return connect.Errorf(connect.CodeInvalidArgument, "length must be greater than 0")
	}

	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	// 1. 查询当前 Inode 大小
	var currentSize int64
	querySizeSQL := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, querySizeSQL, int64(ino)).Scan(&currentSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connect.Errorf(connect.CodeNotFound, "inode %d not found", ino)
		}
		return connect.Errorf(connect.CodeInternal, "failed to query inode size: %s", err)
	}

	// 2. 区分打洞模式 (FALLOC_FL_PUNCH_HOLE = 2) 与标准预分配模式
	isPunchHole := (mode & uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE)) != 0 || mode == 2
	if isPunchHole {
		// 打洞超出 EOF 为合法空操作
		if offset >= uint64(currentSize) {
			return nil
		}
		punchEnd := offset + length
		if punchEnd > uint64(currentSize) {
			punchEnd = uint64(currentSize)
		}
		punchLen := punchEnd - offset

		meta := d.MetaAdapter()
		if meta == nil {
			return connect.Errorf(connect.CodeInternal, "JuiceFS 元数据适配器未初始化")
		}

		if err := meta.PunchHole(ctx, diskID, ino, offset, punchLen); err != nil {
			return connect.Errorf(connect.CodeInternal, "打洞失败: %s", err)
		}

		now := time.Now()
		updateInodeSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
		if _, err := d.db.Exec(ctx, updateInodeSQL, now, int64(ino)); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to update inode metadata: %s", err)
		}
		return nil
	}

	// 标准预分配行为：若未设置 FALLOC_FL_KEEP_SIZE 且超出当前文件大小，则扩容 Inode
	keepSize := (mode & uint32(bowlv1.FallocateMode_FALLOCATE_MODE_KEEP_SIZE)) != 0
	if !keepSize && offset+length > uint64(currentSize) {
		newSize := offset + length
		now := time.Now()
		updateInodeSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = GREATEST(size, $1::bigint),
    blocks = (GREATEST(size, $1::bigint) + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
		if _, err := d.db.Exec(ctx, updateInodeSQL, int64(newSize), now, int64(ino)); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to preallocate space: %s", err)
		}
	}

	return nil
}

// CopyFileRange 基于 JuiceFS 元数据切片账本实现秒级写时复制 (COW) 克隆语义。
// 在同一虚拟磁盘内，仅通过 MetaAdapter 克隆 chunks 表中切片索引记录并原子递增 slice_refs.refs 引用计数，
// 无需搬迁物理 S3/NVMe 载荷字节，具备亚毫秒级元数据克隆与严格的写时复制 (COW) 隔离性。
func (d *ChunkDriver) CopyFileRange(ctx context.Context, srcHandle HandleContext, srcDiskID string, srcIno uint64, srcOffset uint64, dstHandle HandleContext, dstDiskID string, dstIno uint64, dstOffset uint64, length uint64) (uint64, error) {
	if _, err := resolveHandle(srcHandle, &srcDiskID, &srcIno); err != nil {
		return 0, err
	}
	if _, err := resolveHandle(dstHandle, &dstDiskID, &dstIno); err != nil {
		return 0, err
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

	meta := d.MetaAdapter()
	if meta == nil {
		return 0, connect.Errorf(connect.CodeInternal, "JuiceFS 元数据适配器未初始化")
	}

	copied, err := meta.CloneRange(ctx, srcDiskID, srcIno, srcOffset, dstIno, dstOffset, length)
	if err != nil {
		if errors.Is(err, ErrSrcInodeNotFound) {
			return 0, connect.Errorf(connect.CodeNotFound, "src inode %d not found", srcIno)
		}
		if errors.Is(err, ErrDstInodeNotFound) {
			return 0, connect.Errorf(connect.CodeNotFound, "dst inode %d not found", dstIno)
		}
		return 0, connect.Errorf(connect.CodeInternal, "执行写时复制切片克隆失败: %s", err)
	}

	return copied, nil
}

// Lseek 定位文件内部数据或稀疏空洞偏移量 (SEEK_DATA / SEEK_HOLE / 标准 whence)
func (d *ChunkDriver) Lseek(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, whence uint32) (uint64, error) {
	ch, err := resolveHandle(handle, &diskID, &ino)
	if err != nil {
		return 0, err
	}

	if diskID == "" {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "缺少虚拟磁盘标识 diskID")
	}
	if ino == 0 {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "无效的 Inode 编号 %d", ino)
	}

	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	var currentSize int64
	querySizeSQL := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, querySizeSQL, int64(ino)).Scan(&currentSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, connect.Errorf(connect.CodeNotFound, "inode %d not found", ino)
		}
		return 0, connect.Errorf(connect.CodeInternal, "failed to query inode size: %s", err)
	}
	fileSize := uint64(currentSize)

	var targetOffset uint64
	switch whence {
	case 0, 1: // SEEK_SET (0), SEEK_CUR (1)
		targetOffset = offset
	case 2: // SEEK_END (2)
		targetOffset = fileSize + offset
	case uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA): // 3
		if offset >= fileSize {
			return 0, connect.Errorf(connect.CodeOutOfRange, "offset %d is at or beyond EOF %d", offset, fileSize)
		}
		foundOffset, found, err := d.scanData(ctx, diskID, ino, offset, fileSize)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, connect.Errorf(connect.CodeOutOfRange, "no data found at or after offset %d", offset)
		}
		targetOffset = foundOffset
	case uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE): // 4
		if offset > fileSize {
			return 0, connect.Errorf(connect.CodeOutOfRange, "offset %d is beyond EOF %d", offset, fileSize)
		}
		if offset == fileSize {
			targetOffset = fileSize
		} else {
			holeOffset, err := d.scanHole(ctx, diskID, ino, offset, fileSize)
			if err != nil {
				return 0, err
			}
			targetOffset = holeOffset
		}
	default:
		return 0, connect.Errorf(connect.CodeInvalidArgument, "unsupported lseek whence: %d", whence)
	}

	if ch != nil {
		ch.updateOffset(targetOffset)
	}
	return targetOffset, nil
}

// scanData 扫描文件在 [startOffset, fileSize) 区间内的第一个有效数据偏移量
func (d *ChunkDriver) scanData(ctx context.Context, diskID string, ino uint64, startOffset, fileSize uint64) (uint64, bool, error) {
	meta := d.MetaAdapter()
	if meta == nil {
		return 0, false, connect.Errorf(connect.CodeInternal, "JuiceFS 元数据适配器未初始化")
	}

	curOffset := startOffset
	for curOffset < fileSize {
		curChunk := uint32(curOffset / ChunkSize)
		chunkEnd := uint64(curChunk+1) * ChunkSize
		if chunkEnd > fileSize {
			chunkEnd = fileSize
		}

		reconstitutedSlices, err := meta.ReadReconstitutedSlices(ctx, diskID, ino, curChunk)
		if err != nil {
			return 0, false, connect.Errorf(connect.CodeInternal, "读取重构切片失败: %s", err)
		}

		if len(reconstitutedSlices) == 0 {
			// 当前 64MB Chunk 完全未分配，属于纯稀疏空洞，跳至下一个 Chunk
			curOffset = chunkEnd
			continue
		}

		reqStart := uint32(curOffset % ChunkSize)
		var curSlicePos uint32 = 0
		foundDataInChunk := false
		var dataOffsetInFile uint64

		for _, s := range reconstitutedSlices {
			sliceStart := curSlicePos
			sliceEnd := curSlicePos + s.Len
			curSlicePos += s.Len

			if sliceEnd <= reqStart {
				continue
			}

			if s.Id > 0 {
				dataStartInChunk := max(sliceStart, reqStart)
				candidateOffset := uint64(curChunk)*ChunkSize + uint64(dataStartInChunk)
				if candidateOffset < fileSize {
					dataOffsetInFile = candidateOffset
					foundDataInChunk = true
					break
				}
			}
		}

		if foundDataInChunk {
			return dataOffsetInFile, true, nil
		}

		curOffset = chunkEnd
	}

	return 0, false, nil
}

// scanHole 扫描文件在 [startOffset, fileSize) 区间内的第一个稀疏空洞偏移量
func (d *ChunkDriver) scanHole(ctx context.Context, diskID string, ino uint64, startOffset, fileSize uint64) (uint64, error) {
	meta := d.MetaAdapter()
	if meta == nil {
		return 0, connect.Errorf(connect.CodeInternal, "JuiceFS 元数据适配器未初始化")
	}

	curOffset := startOffset
	for curOffset < fileSize {
		curChunk := uint32(curOffset / ChunkSize)
		chunkEnd := uint64(curChunk+1) * ChunkSize
		if chunkEnd > fileSize {
			chunkEnd = fileSize
		}

		reconstitutedSlices, err := meta.ReadReconstitutedSlices(ctx, diskID, ino, curChunk)
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "读取重构切片失败: %s", err)
		}

		if len(reconstitutedSlices) == 0 {
			// 当前 64MB Chunk 未分配，属于稀疏空洞，curOffset 本身即处于空洞内
			return curOffset, nil
		}

		reqStart := uint32(curOffset % ChunkSize)
		var curSlicePos uint32 = 0
		foundHoleInChunk := false
		var holeOffsetInFile uint64

		for _, s := range reconstitutedSlices {
			sliceStart := curSlicePos
			sliceEnd := curSlicePos + s.Len
			curSlicePos += s.Len

			if sliceEnd <= reqStart {
				continue
			}

			if s.Id == 0 {
				holeStartInChunk := max(sliceStart, reqStart)
				candidateOffset := uint64(curChunk)*ChunkSize + uint64(holeStartInChunk)
				if candidateOffset < fileSize {
					holeOffsetInFile = candidateOffset
					foundHoleInChunk = true
					break
				} else {
					return fileSize, nil
				}
			}
		}

		if foundHoleInChunk {
			return holeOffsetInFile, nil
		}

		// 检查 Chunk 尾部未被切片覆盖的区间是否构成空洞
		chunkLenInFile := uint32(chunkEnd - uint64(curChunk)*ChunkSize)
		if curSlicePos < chunkLenInFile {
			holeStartInChunk := max(curSlicePos, reqStart)
			candidateOffset := uint64(curChunk)*ChunkSize + uint64(holeStartInChunk)
			if candidateOffset < fileSize {
				return candidateOffset, nil
			}
			return fileSize, nil
		}

		curOffset = chunkEnd
	}

	return fileSize, nil
}

// Close 关闭文件句柄，安全释放关联的内存 Page 缓冲区并置位关闭状态，防止后续 I/O 操作
func (d *ChunkDriver) Close(ctx context.Context, handle HandleContext) error {
	if handle == nil {
		return nil
	}
	ch, ok := handle.(*chunkFileHandle)
	if !ok {
		return nil
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		return nil
	}
	ch.closed = true
	for _, p := range ch.pages {
		if p != nil {
			p.Release()
		}
	}
	ch.pages = nil
	ch.buffer = nil
	return nil
}

// DeleteInode 删除指定 Inode 时，安全递减切片引用计数，删除元数据，并联动触发物理孤儿切片追踪与 GC 回收登记
func (d *ChunkDriver) DeleteInode(ctx context.Context, diskID string, ino uint64) error {
	var orphanSliceIDs []uint64
	meta := d.MetaAdapter()
	if meta != nil {
		var err error
		orphanSliceIDs, err = meta.DeleteInodeChunks(ctx, diskID, ino)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to delete inode chunks via meta adapter: %s", err)
		}
	} else {
		parsedDiskUUID, err := uuid.Parse(diskID)
		if err != nil {
			return connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
		}
		schemaName := FormatDiskSchema(parsedDiskUUID)

		// 1. 查询该 Inode 下属的所有 Chunk 行及切片二进制数据
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
				slices, err := DecodeSlices(rawSlices)
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

		// 2. 依次递减切片引用计数，并查验标记物理孤儿切片 (refs <= 0)
		for sliceID, count := range sliceCounts {
			updateRefSQL := fmt.Sprintf(`UPDATE "%s".slice_refs SET refs = refs - $1 WHERE id = $2;`, schemaName)
			if _, err := d.db.Exec(ctx, updateRefSQL, int32(count), int64(sliceID)); err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to decrement slice ref: %s", err)
			}
			var currentRefs int32
			checkRefSQL := fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = $1`, schemaName)
			if err := d.db.QueryRow(ctx, checkRefSQL, int64(sliceID)).Scan(&currentRefs); err == nil && currentRefs <= 0 {
				orphanSliceIDs = append(orphanSliceIDs, sliceID)
			}
		}

		// 3. 删除该 Inode 的全部 chunks 行
		deleteChunksSQL := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1;`, schemaName)
		if _, err := d.db.Exec(ctx, deleteChunksSQL, int64(ino)); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to delete inode chunks: %s", err)
		}
	}

	// 联动垃圾回收器：登记该磁盘并跟踪孤儿切片编号，等待 GC 周期物理清理
	if d.gc != nil {
		d.gc.RegisterDisk(diskID)
		if len(orphanSliceIDs) > 0 {
			d.gc.TrackOrphans(diskID, orphanSliceIDs...)
		}
	}

	return nil
}

// Unlink is an alias for DeleteInode.
func (d *ChunkDriver) Unlink(ctx context.Context, diskID string, ino uint64) error {
	return d.DeleteInode(ctx, diskID, ino)
}

