package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// renameNoReplace 对应 Linux RENAME_NOREPLACE 标志，要求目标目录项不存在。
	renameNoReplace = 0x1
	// renameExchange 对应 Linux RENAME_EXCHANGE 标志，原子双向交换两个目录项。
	renameExchange = 0x2
)

// Rename 实现目录项重命名、跨目录移动与双向原子交换操作，支持 Linux FUSE_RENAME2 协议标志位。
func (fs *RawFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName, newName string) (status fuse.Status) {
	start := fs.metrics.beginOp(OpRename)
	opCtx, endSpan := fs.startFuseOp(OpRename)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpRename, start, status, 0)
	}()

	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}
	if fs.isVirtualStatsName(input.NodeId, oldName) || fs.isVirtualStatsName(input.Newdir, newName) {
		return fuse.EACCES
	}
	if fs.isVirtualStatsIno(input.NodeId) || fs.isVirtualStatsIno(input.Newdir) {
		return fuse.Status(syscall.ENOTDIR)
	}
	if input.Flags&^(renameNoReplace|renameExchange) != 0 ||
		(input.Flags&renameNoReplace != 0 && input.Flags&renameExchange != 0) {
		return fuse.EINVAL
	}

	oldParentIno, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return st
	}

	newParentIno, st := fs.ToServerIno(input.Newdir)
	if st != fuse.OK {
		return st
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()

	source, _ := fs.cache.entry(input.NodeId, oldName)
	target, targetKnown := fs.cache.entry(input.Newdir, newName)
	var targetAttr *storagev1.Inode
	if target != 0 {
		if cached, ok := fs.cache.peekInode(target); ok {
			targetAttr = cached.inode
		}
	}

	needLookup := fs.handles.AnyOpen() || fs.writeBehind.HasAnyPending() ||
		(input.Flags&(renameExchange|renameNoReplace) == 0 && fs.hasDirtyInodes() && (!targetKnown || (target != 0 && (targetAttr == nil || targetAttr.GetAttr() == nil))))
	if needLookup {
		old, oldStatus := fs.lookupInodeWithContext(opCtx, cancel, input.NodeId, oldName)
		if oldStatus != fuse.OK {
			return oldStatus
		}
		source = fs.ToKernelIno(old.inode.GetIno())
		cached, targetStatus := fs.lookupInodeWithContext(opCtx, cancel, input.Newdir, newName)
		if targetStatus != fuse.OK && targetStatus != fuse.ENOENT {
			return targetStatus
		}
		if targetStatus == fuse.OK {
			targetAttr = cached.inode
			target = fs.ToKernelIno(targetAttr.GetIno())
		} else {
			target = 0
			targetAttr = nil
		}
	}

	if source != 0 {
		if sourceServerIno, sSt := fs.ToServerIno(source); sSt == fuse.OK {
			fs.writeBehind.BarrierInode(opCtx, sourceServerIno)
		}
	}
	var targetServerIno uint64
	var hasTargetServerIno bool
	if target != 0 {
		if tIno, tSt := fs.ToServerIno(target); tSt == fuse.OK {
			targetServerIno = tIno
			hasTargetServerIno = true
			if target != source {
				fs.writeBehind.BarrierInode(opCtx, targetServerIno)
			}
		}
		// 与并发 Release 按 Inode 串行，避免目标节点孤儿标记与最后句柄关闭互相错过。
		unlock := fs.inodeLocks.lock(target)
		defer unlock()
	}
	targetOpen := target != 0 && fs.handles.IsOpen(target)

	var protoFlags uint32
	switch input.Flags {
	case renameNoReplace:
		protoFlags = 0x1
	case renameExchange:
		protoFlags = 0x2
	}

	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	epoch := fs.cache.version()
	_, err := fs.client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		OldParentIno:       oldParentIno,
		OldName:            []byte(oldName),
		NewParentIno:       newParentIno,
		NewName:            []byte(newName),
		Flags:              protoFlags,
		TargetOpenInClient: &targetOpen,
		DiskId:             fs.cfg.DiskID,
		Epoch:              fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	// 保留父目录 Inode 缓存并用当前时间刷新属性，避免 rename 后内核 fuse_invalidate_attr
	// 触发的 GETATTR 每次穿透到服务端。目录跨父目录移动时同步修正父目录 nlink。
	sourceIsDir := false
	if source != 0 {
		if cached, ok := fs.cache.inode(source); ok && cached.inode != nil && cached.inode.GetAttr() != nil {
			sourceIsDir = cached.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
		}
	}
	movedDir := sourceIsDir && input.NodeId != input.Newdir && input.Flags&renameExchange == 0
	oldDelta := int32(0)
	if movedDir {
		oldDelta = -1
	}
	fs.cache.invalidateDirListingTouchParentNowAt(epoch, input.NodeId, oldDelta)
	if input.Newdir != input.NodeId {
		newDelta := int32(0)
		if movedDir {
			newDelta = 1
		}
		fs.cache.invalidateDirListingTouchParentNowAt(epoch, input.Newdir, newDelta)
	}
	if input.Flags&renameExchange != 0 {
		// 交换后旧名指向原目标节点，新名指向原源节点。
		if target != 0 {
			fs.cache.putEntryAt(epoch, input.NodeId, oldName, target)
		} else {
			fs.cache.removeEntry(input.NodeId, oldName)
		}
		if source != 0 {
			fs.cache.putEntryAt(epoch, input.Newdir, newName, source)
		} else {
			fs.cache.removeEntry(input.Newdir, newName)
		}
	} else {
		// 普通重命名后旧名不再存在，回填负缓存避免后续查找穿透 RPC；同名同目录自重命名由新名回填处理。
		if input.NodeId != input.Newdir || oldName != newName {
			fs.cache.putEntryAt(epoch, input.NodeId, oldName, 0)
		}
		if source != 0 {
			fs.cache.putEntryAt(epoch, input.Newdir, newName, source)
		} else {
			fs.cache.removeEntry(input.Newdir, newName)
		}
	}
	if source != 0 {
		fs.cache.invalidateInode(source)
	}
	if target != 0 && target != source {
		fs.cache.invalidateInode(target)
	}
	if target != 0 && target != source && input.Flags&renameExchange == 0 &&
		targetAttr != nil && targetAttr.GetAttr() != nil && targetAttr.GetAttr().GetNlink() == 1 {
		if targetOpen {
			fs.markOrphanWithContext(opCtx, target)
		} else if hasTargetServerIno {
			fs.writeBehind.RemoveInode(opCtx, targetServerIno)
			fs.readAhead.InvalidateInode(targetServerIno)
			fs.clearDirty(targetServerIno)
		}
	}
	return fuse.OK
}
