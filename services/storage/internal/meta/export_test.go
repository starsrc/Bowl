package meta

import (
	"context"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// DecrementDeletedSlicesRefs 导出未导出的 decrementDeletedSlicesRefs 供 meta_test 黑盒测试使用。
func DecrementDeletedSlicesRefs(ctx context.Context, q db.Querier, rawSlices ...[]byte) error {
	return decrementDeletedSlicesRefs(ctx, q, rawSlices...)
}

// DeleteAllChunksByIno 导出未导出的 deleteAllChunksByIno 供 meta_test 黑盒测试使用。
func DeleteAllChunksByIno(ctx context.Context, q db.Querier, ino int64) error {
	return deleteAllChunksByIno(ctx, q, ino)
}

// TruncateChunksToSize 导出未导出的 truncateChunksToSize 供 meta_test 黑盒测试使用。
func TruncateChunksToSize(ctx context.Context, q db.Querier, ino int64, newSize uint64) error {
	return truncateChunksToSize(ctx, q, ino, newSize)
}
