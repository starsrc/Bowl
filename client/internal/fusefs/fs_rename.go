package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// renameNoReplace 对应 Linux RENAME_NOREPLACE (0x1)，要求目标目录项不存在。
	renameNoReplace = 0x1
	// renameExchange 对应 Linux RENAME_EXCHANGE (0x2)，原子双向交换两个目录项。
	renameExchange = 0x2
	// renameWhiteout 对应 Linux RENAME_WHITEOUT (0x4)，在源项位置创建 whiteout 特殊节点。
	renameWhiteout = 0x4
)

// Rename 实现目录项重命名、跨目录移动与双向原子交换操作（支持 Linux FUSE_RENAME2 协议标志位）。
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

	seq0 := fs.topologySeq.Load()
	source, _ := fs.cache.entry(input.NodeId, oldName)
	target, targetKnown := fs.cache.entry(input.Newdir, newName)
	var targetAttr *storagev1.Inode
	if target != 0 {
		if cached, ok := fs.cache.peekInode(target); ok {
			targetAttr = cached.inode
		}
	}

	needLookup := fs.handles.AnyOpen() || fs.stage.HasAnyPending() ||
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
			fs.stage.BarrierInode(opCtx, sourceServerIno)
		}
	}
	var targetServerIno uint64
	var hasTargetServerIno bool
	if target != 0 {
		if tIno, tSt := fs.ToServerIno(target); tSt == fuse.OK {
			targetServerIno = tIno
			hasTargetServerIno = true
			if target != source {
				fs.stage.BarrierInode(opCtx, targetServerIno)
			}
		}
		// 与并发 Release 按 Inode 串行，避免目标节点孤儿标记与最后句柄关闭互相错过。
		unlock := fs.inodeLocks.lock(target)
		defer unlock()
		if target != source &&
			input.Flags&(renameExchange|renameNoReplace) == 0 &&
			fs.topologySeq.Load() != seq0 &&
			(fs.handles.AnyOpen() || fs.stage.HasAnyPending() || fs.hasDirtyInodes()) {
			refreshed, st := fs.getInodeWithContext(opCtx, cancel, target)
			switch st {
			case fuse.OK:
				targetAttr = refreshed.inode
			case fuse.ENOENT:
				targetAttr = nil
			default:
				return st
			}
		}
	}
	targetOpen := target != 0 && fs.handles.IsOpen(target)

	var protoFlags uint32
	if (input.Flags & renameNoReplace) != 0 {
		protoFlags |= 0x1
	}
	if (input.Flags & renameExchange) != 0 {
		protoFlags |= 0x2
	}
	if (input.Flags & renameWhiteout) != 0 {
		protoFlags |= 0x4
	}
	// 保留未知位透传给服务端，由服务端契约校验返回 EINVAL
	if unknown := input.Flags &^ (renameNoReplace | renameExchange | renameWhiteout); unknown != 0 {
		protoFlags |= unknown
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
	fs.markMetaDirty()

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
		// 旧名消失。WHITEOUT 会在旧名留下特殊节点，保守地按未命中处理；
		// 普通重命名的旧名已确定不存在，回填负缓存（ino=0），避免随后 access()/stat()
		// 再次穿透 RPC。同名同目录自重命名由下面的新名回填统一处理。
		if input.Flags&renameWhiteout != 0 {
			fs.cache.removeEntry(input.NodeId, oldName)
		} else if input.NodeId != input.Newdir || oldName != newName {
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
	fs.topologySeq.Add(1)
	if target != 0 && target != source && input.Flags&renameExchange == 0 &&
		targetAttr != nil && targetAttr.GetAttr() != nil && targetAttr.GetAttr().GetNlink() == 1 {
		if targetOpen {
			fs.markOrphanWithContext(opCtx, target)
		} else if hasTargetServerIno {
			fs.stage.DiscardInode(targetServerIno)
			fs.readAhead.InvalidateInode(targetServerIno)
			fs.clearDirty(targetServerIno)
			fs.forgetInodeSpecial(target)
		}
	}
	return fuse.OK
}
