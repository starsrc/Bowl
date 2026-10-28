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

// DeleteAllChunksByInos 导出未导出的 deleteAllChunksByInos 供 meta_test 黑盒测试使用。
func DeleteAllChunksByInos(ctx context.Context, q db.Querier, inos []int64) error {
	return deleteAllChunksByInos(ctx, q, inos)
}

// TruncateChunksToSize 导出未导出的 truncateChunksToSize 供 meta_test 黑盒测试使用。
func TruncateChunksToSize(ctx context.Context, q db.Querier, ino int64, newSize uint64) error {
	return truncateChunksToSize(ctx, q, ino, newSize)
}

// LockNotifyChansLen 供测试检查 Manager 实现中 lockNotifyChans 的大小。
func LockNotifyChansLen(mgr Manager) int {
	if m, ok := mgr.(*manager); ok {
		m.lockNotifyMu.Lock()
		defer m.lockNotifyMu.Unlock()
		return len(m.lockNotifyChans)
	}
	return 0
}

// HasLockNotifyChan 供测试检查 Manager 实现中是否残留指定 ino 的通知通道。
func HasLockNotifyChan(mgr Manager, ino uint64) bool {
	if m, ok := mgr.(*manager); ok {
		m.lockNotifyMu.Lock()
		defer m.lockNotifyMu.Unlock()
		if m.lockNotifyChans == nil {
			return false
		}
		_, ok := m.lockNotifyChans[ino]
		return ok
	}
	return false
}

// DirCursorsLen 供测试检查 Manager 实现中 dirCursors 的条目数。
func DirCursorsLen(mgr Manager) int {
	if m, ok := mgr.(*manager); ok {
		m.dirCursorMu.Lock()
		defer m.dirCursorMu.Unlock()
		return len(m.dirCursors)
	}
	return 0
}

// InoDirCursorsCount 供测试检查 Manager 实现中指定 ino 的游标条目数。
func InoDirCursorsCount(mgr Manager, ino uint64) int {
	if m, ok := mgr.(*manager); ok {
		m.dirCursorMu.Lock()
		defer m.dirCursorMu.Unlock()
		count := 0
		for k := range m.dirCursors {
			if k.ino == ino {
				count++
			}
		}
		return count
	}
	return 0
}

// DirCursorTrackingEntriesCount 供测试检查 Manager 实现中内部清理/在途追踪 map 条目总数。
func DirCursorTrackingEntriesCount(mgr Manager) int {
	if m, ok := mgr.(*manager); ok {
		m.dirCursorMu.Lock()
		defer m.dirCursorMu.Unlock()
		return len(m.activeDirReads)
	}
	return 0
}
