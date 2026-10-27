package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

type compactDataSegment struct {
	pos    uint32
	length uint32
	slices []Slice
}

type compactWrittenBlock struct {
	sliceID   uint64
	blockIdx  uint32
	blockSize uint32
}

type compactNewSliceDesc struct {
	id     uint64
	size   uint32
	pos    uint32
	length uint32
}

// CompactChunk 对指定 Inode 与 ChunkIndx 执行切片碎片合并与乐观前缀 CAS 替换。
// 遵循 ADR-0019 数据层碎片整理与 CAS 替换架构：
// 1. 静态边界校验：ino 非零且在 int64 范围内，indx 在 int32 范围内；
// 2. 阶段 1（锁外读取与新物理块落盘）：
//   - 调用 m.store.GetChunk 获取 originSlices（若 pgx.ErrNoRows 或 len(originSlices) == 0 直接返回 nil）；
//   - DecodeSlices 并调用 BuildSlice 计算有效切片视图 view []Slice；
//   - 将 view 中连续的非空洞数据切片（s.ID > 0 且相邻切片连续）合并为紧致连续数据段；
//   - 对每个连续有效数据段：
//   - 从 BlockStore 读出各有效 Slice 对应的字节数据拼接为完整 segData；
//   - 调用 NextSliceID 分配新 newSliceID；
//   - 按 SliceBlocks 切分为不超过 4 MiB 的物理块并调用 PutBlock 落盘；
//   - 将紧致 Slice{Pos: segPos, ID: newSliceID, Size: segLen, Off: 0, Len: segLen} 编码追加到 compactedSlices；
//   - 若中途发生任何读取、NextSliceID 或 PutBlock 错误，立即清理已写入的物理块并返回错误；
//
// 3. 阶段 2（写事务内乐观前缀 CAS 替换）：
//   - 在事务内锁定当前目标行（GetChunkForUpdate）；
//   - 若行不存在或 !bytes.HasPrefix(currentChunk.Slices, originSlices)：
//   - 安全中止放弃替换，事务返回 nil，并在事务外立即清理阶段 1 生成的全部新物理块；
//   - 若 bytes.HasPrefix 成立：
//   - 构造 updatedSlices = compactedSlices || currentChunk.Slices[len(originSlices):]；
//   - 更新 chunks.slices；
//   - 插入新 slice_refs（refs = 1）；
//   - 调用 decrementDeletedSlicesRefs 递减 originSlices 中旧 slice_id 的引用计数；
//   - 若写事务执行失败，事务外立即清理新物理块并返回错误。
func (m *manager) CompactChunk(ctx context.Context, ino uint64, indx uint32) error {
	if ino == 0 || ino > math.MaxInt64 || indx > math.MaxInt32 {
		return ErrInvalidArgument
	}

	// 阶段 1：行锁外读取与新物理块落盘
	chunk, err := m.store.GetChunk(ctx, db.GetChunkParams{
		Ino:  int64(ino),
		Indx: int32(indx),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("get chunk for compact (ino=%d, indx=%d): %w", ino, indx, err)
	}
	if len(chunk.Slices) == 0 {
		return nil
	}

	originSlices := chunk.Slices
	rawSlices, err := DecodeSlices(originSlices)
	if err != nil {
		return fmt.Errorf("decode origin slices (ino=%d, indx=%d): %w", ino, indx, err)
	}

	view := BuildSlice(rawSlices)

	var segments []compactDataSegment
	var curSeg *compactDataSegment

	for _, s := range view {
		if s.ID == 0 {
			if curSeg != nil {
				segments = append(segments, *curSeg)
				curSeg = nil
			}
			continue
		}

		if curSeg == nil {
			curSeg = &compactDataSegment{
				pos:    s.Pos,
				length: s.Len,
				slices: []Slice{s},
			}
		} else {
			if curSeg.pos+curSeg.length == s.Pos {
				curSeg.length += s.Len
				curSeg.slices = append(curSeg.slices, s)
			} else {
				segments = append(segments, *curSeg)
				curSeg = &compactDataSegment{
					pos:    s.Pos,
					length: s.Len,
					slices: []Slice{s},
				}
			}
		}
	}
	if curSeg != nil {
		segments = append(segments, *curSeg)
	}

	bs := m.getBlockStore()
	var writtenBlocks []compactWrittenBlock

	cleanupBlocks := func() {
		for _, b := range writtenBlocks {
			_ = bs.DeleteBlock(context.Background(), b.sliceID, b.blockIdx, b.blockSize)
		}
	}

	var newSlices []compactNewSliceDesc
	var compactedSlices []byte

	for _, seg := range segments {
		segData := make([]byte, seg.length)
		var segOff uint32

		for _, s := range seg.slices {
			locs, err := LocateBlocks(s.Size, s.Off, s.Len)
			if err != nil {
				cleanupBlocks()
				return fmt.Errorf("locate blocks for compact slice %d: %w", s.ID, err)
			}
			var sOff uint32
			for _, loc := range locs {
				dst := segData[segOff+sOff : segOff+sOff+loc.Length]
				if err := bs.GetBlock(ctx, s.ID, loc.BlockIdx, loc.BlockSize, loc.BlockOff, dst); err != nil {
					cleanupBlocks()
					return fmt.Errorf("%w: get block for compact (slice_id=%d, block_idx=%d): %v", ErrIO, s.ID, loc.BlockIdx, err)
				}
				sOff += loc.Length
			}
			segOff += s.Len
		}

		newID, err := m.store.NextSliceID(ctx)
		if err != nil {
			cleanupBlocks()
			return fmt.Errorf("next slice id for compact: %w", err)
		}

		blocks := SliceBlocks(seg.length)
		for _, b := range blocks {
			start := b.BlockIdx * BlockSize
			end := start + b.BlockSize
			bData := segData[start:end]
			if err := bs.PutBlock(ctx, uint64(newID), b.BlockIdx, bData); err != nil {
				cleanupBlocks()
				return fmt.Errorf("%w: put block for compact (slice_id=%d, block_idx=%d): %v", ErrIO, newID, b.BlockIdx, err)
			}
			writtenBlocks = append(writtenBlocks, compactWrittenBlock{
				sliceID:   uint64(newID),
				blockIdx:  b.BlockIdx,
				blockSize: b.BlockSize,
			})
		}

		compactedSlice := Slice{
			Pos:  seg.pos,
			ID:   uint64(newID),
			Size: seg.length,
			Off:  0,
			Len:  seg.length,
		}
		compactedSlices = AppendEncodedSlice(compactedSlices, compactedSlice)
		newSlices = append(newSlices, compactNewSliceDesc{
			id:     uint64(newID),
			size:   seg.length,
			pos:    seg.pos,
			length: seg.length,
		})
	}

	if len(compactedSlices) == 0 {
		compactedSlices = EncodeSlice(Slice{
			Pos:  0,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  ChunkSize,
		})
	}

	// 阶段 2：数据库写事务内乐观前缀 CAS 替换
	var casAborted bool

	txErr := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		casAborted = false
		currentChunk, err := q.GetChunkForUpdate(ctx, db.GetChunkForUpdateParams{
			Ino:  int64(ino),
			Indx: int32(indx),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				casAborted = true
				return nil
			}
			return fmt.Errorf("get chunk for update in compact (ino=%d, indx=%d): %w", ino, indx, err)
		}

		if !bytes.HasPrefix(currentChunk.Slices, originSlices) {
			casAborted = true
			return nil
		}

		var updatedSlices []byte
		concurrentSuffix := currentChunk.Slices[len(originSlices):]
		if len(concurrentSuffix) > 0 {
			updatedSlices = make([]byte, 0, len(compactedSlices)+len(concurrentSuffix))
			updatedSlices = append(updatedSlices, compactedSlices...)
			updatedSlices = append(updatedSlices, concurrentSuffix...)
		} else {
			updatedSlices = compactedSlices
		}

		if err := q.UpdateChunkSlices(ctx, db.UpdateChunkSlicesParams{
			Ino:    int64(ino),
			Indx:   int32(indx),
			Slices: updatedSlices,
		}); err != nil {
			return fmt.Errorf("update chunk slices in compact (ino=%d, indx=%d): %w", ino, indx, err)
		}

		for _, ns := range newSlices {
			if err := q.InsertSliceRef(ctx, db.InsertSliceRefParams{
				SliceID: int64(ns.id),
				Size:    int32(ns.size),
				Refs:    1,
			}); err != nil {
				return fmt.Errorf("insert slice ref in compact (slice_id=%d): %w", ns.id, err)
			}
		}

		if err := decrementDeletedSlicesRefs(ctx, q, originSlices); err != nil {
			return fmt.Errorf("decrement deleted slices refs in compact: %w", err)
		}

		return nil
	})

	if txErr != nil {
		cleanupBlocks()
		return txErr
	}

	if casAborted {
		cleanupBlocks()
		return nil
	}

	return nil
}

// recordChunkWriteAndTriggerCompact 在单次写操作成功追加 Slice 后递增分块计数，达到阈值时异步触发 Compaction。
func (m *manager) recordChunkWriteAndTriggerCompact(ino uint64, indx uint32) {
	if m.compactSliceThreshold <= 0 {
		return
	}
	key := chunkKey{ino: ino, indx: indx}
	m.chunkWriteCountsMu.Lock()
	m.chunkWriteCounts[key]++
	count := m.chunkWriteCounts[key]
	trigger := count >= m.compactSliceThreshold
	if trigger {
		delete(m.chunkWriteCounts, key)
	}
	m.chunkWriteCountsMu.Unlock()

	if trigger {
		go func() {
			_ = m.CompactChunk(context.Background(), ino, indx)
		}()
	}
}
