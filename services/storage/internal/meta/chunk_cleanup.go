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
