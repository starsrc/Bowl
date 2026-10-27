package meta

import (
	"context"
	"fmt"
	"slices"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// decrementDeletedSlicesRefs 统计多段原始切片字节流中所有有效物理切片 (ID > 0) 的引用频次，
// 并按 slice_id 升序依次对 slice_refs.refs 进行原子扣减（负向增量），以确保全局一致的行级锁顺序，防止并发死锁。
func decrementDeletedSlicesRefs(ctx context.Context, q db.Querier, rawSlices ...[]byte) error {
	counts, err := CountSliceRefs(rawSlices...)
	if err != nil {
		return fmt.Errorf("count slice refs: %w", err)
	}
	if len(counts) == 0 {
		return nil
	}

	ids := make([]uint64, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	for _, id := range ids {
		count := counts[id]
		if count <= 0 {
			continue
		}
		if err := q.AddSliceRefCount(ctx, db.AddSliceRefCountParams{
			SliceID: int64(id),
			Delta:   -count,
		}); err != nil {
			return fmt.Errorf("decrement slice ref count for slice_id %d: %w", id, err)
		}
	}
	return nil
}

// deleteAllChunksByIno 在事务内删除目标 Inode 的全部 chunks 行，并递减其关联物理切片的引用计数。
func deleteAllChunksByIno(ctx context.Context, q db.Querier, ino int64) error {
	deletedSlices, err := q.DeleteChunksByIno(ctx, ino)
	if err != nil {
		return fmt.Errorf("delete chunks by ino %d: %w", ino, err)
	}
	if len(deletedSlices) == 0 {
		return nil
	}
	if err := decrementDeletedSlicesRefs(ctx, q, deletedSlices...); err != nil {
		return fmt.Errorf("decrement deleted slices refs for ino %d: %w", ino, err)
	}
	return nil
}

// deleteAllChunksByInos 在事务内批量删除指定 Inode 列表的全部 chunks 行，并递减其关联物理切片的引用计数。
func deleteAllChunksByInos(ctx context.Context, q db.Querier, inos []int64) error {
	if len(inos) == 0 {
		return nil
	}
	deletedSlices, err := q.DeleteChunksByInos(ctx, inos)
	if err != nil {
		return fmt.Errorf("delete chunks by inos: %w", err)
	}
	if len(deletedSlices) == 0 {
		return nil
	}
	if err := decrementDeletedSlicesRefs(ctx, q, deletedSlices...); err != nil {
		return fmt.Errorf("decrement deleted slices refs for inos: %w", err)
	}
	return nil
}

// truncateChunksToSize 在事务内处理常规文件非零缩容截断 (0 < newSize < oldSize)：
//  1. 计算截断边界 borderIndx := uint32(newSize / ChunkSize) 与 borderPos := uint32(newSize % ChunkSize)；
//  2. 当 borderPos == 0 时，删除 indx >= borderIndx 的所有 chunks 行并递减对应 slice_refs.refs；
//  3. 当 borderPos > 0 时，删除 indx > borderIndx 的所有 chunks 行并递减对应 slice_refs.refs，
//     随后向 indx == borderIndx 的边界 Chunk 追加覆盖 [borderPos, ChunkSize) 的 24 字节空洞切片（若该行存在）。
func truncateChunksToSize(ctx context.Context, q db.Querier, ino int64, newSize uint64) error {
	borderIndx := uint32(newSize / ChunkSize)
	borderPos := uint32(newSize % ChunkSize)

	if borderPos == 0 {
		deletedSlices, err := q.DeleteChunksFromIndx(ctx, db.DeleteChunksFromIndxParams{
			Ino:      ino,
			FromIndx: int32(borderIndx),
		})
		if err != nil {
			return fmt.Errorf("delete chunks from indx %d for ino %d: %w", borderIndx, ino, err)
		}
		if len(deletedSlices) > 0 {
			if err := decrementDeletedSlicesRefs(ctx, q, deletedSlices...); err != nil {
				return fmt.Errorf("decrement deleted slices refs from indx %d for ino %d: %w", borderIndx, ino, err)
			}
		}
		return nil
	}

	// borderPos > 0
	deletedSlices, err := q.DeleteChunksAfterIndx(ctx, db.DeleteChunksAfterIndxParams{
		Ino:       ino,
		AfterIndx: int32(borderIndx),
	})
	if err != nil {
		return fmt.Errorf("delete chunks after indx %d for ino %d: %w", borderIndx, ino, err)
	}
	if len(deletedSlices) > 0 {
		if err := decrementDeletedSlicesRefs(ctx, q, deletedSlices...); err != nil {
			return fmt.Errorf("decrement deleted slices refs after indx %d for ino %d: %w", borderIndx, ino, err)
		}
	}

	holeSlice := Slice{
		Pos:  borderPos,
		ID:   0,
		Size: 0,
		Off:  0,
		Len:  uint32(ChunkSize) - borderPos,
	}
	holeBytes := EncodeSlice(holeSlice)

	if _, err := q.AppendExistingChunkSlice(ctx, db.AppendExistingChunkSliceParams{
		Ino:    ino,
		Indx:   int32(borderIndx),
		Slices: holeBytes,
	}); err != nil {
		return fmt.Errorf("append hole slice to chunk (ino=%d, indx=%d): %w", ino, borderIndx, err)
	}

	return nil
}
