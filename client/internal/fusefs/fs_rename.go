package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	renameNoReplace = 0x1

	renameExchange = 0x2

	renameWhiteout = 0x4
)

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
	var sourceAttr *storagev1.Inode
	if source != 0 {
		if cached, ok := fs.cache.peekInode(source); ok {
			sourceAttr = cached.inode
		}
	}
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
		sourceAttr = old.inode
		source = fs.ToKernelIno(sourceAttr.GetIno())
		cached, targetStatus := fs.lookupInodeWithContext(opCtx, cancel, input.Newdir, newName)
		if targetStatus != fuse.OK && targetStatus != fuse.ENOENT {
			return targetStatus
		}
		targetKnown = true
		if targetStatus == fuse.OK {
			targetAttr = cached.inode
			target = fs.ToKernelIno(targetAttr.GetIno())
		} else {
			target = 0
			targetAttr = nil
		}
	}

	barrierCtx, barrierCancel := fs.stageBarrierContext(opCtx, cancel)
	defer barrierCancel()
	if source != 0 {
		if sourceServerIno, sSt := fs.ToServerIno(source); sSt == fuse.OK {
			if err := fs.stage.BarrierInode(barrierCtx, sourceServerIno); err != nil {
				return fs.stageErrToStatus(err)
			}
		}
	}
	var targetServerIno uint64
	var hasTargetServerIno bool
	if target != 0 {
		if tIno, tSt := fs.ToServerIno(target); tSt == fuse.OK {
			targetServerIno = tIno
			hasTargetServerIno = true
			if target != source {
				if err := fs.stage.BarrierInode(barrierCtx, targetServerIno); err != nil {
					return fs.stageErrToStatus(err)
				}
			}
		}

		unlock := fs.inodeLocks.lock(target)
		defer unlock()
		if target != source && input.Flags&(renameExchange|renameNoReplace) == 0 {
			needRevalidate := fs.topologySeq.Load() != seq0 &&
				(fs.handles.AnyOpen() || fs.stage.HasAnyPending() || fs.hasDirtyInodes())
			needOpenTargetAttr := fs.handles.IsOpen(target) && (targetAttr == nil || targetAttr.GetAttr() == nil)
			if needRevalidate || needOpenTargetAttr {
				refreshed, refSt := fs.getInodeWithContext(barrierCtx, nil, target)
				switch refSt {
				case fuse.OK:
					targetAttr = refreshed.inode
				case fuse.ENOENT:
					targetAttr = nil
					targetKnown = false
				default:
					return refSt
				}
			}
		}
	}
	targetIsDir := targetAttr != nil && targetAttr.GetAttr() != nil && targetAttr.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
	targetOpen := target != 0 && !targetIsDir && fs.handles.IsOpen(target)

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

	if unknown := input.Flags &^ (renameNoReplace | renameExchange | renameWhiteout); unknown != 0 {
		protoFlags |= unknown
	}

	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
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
		Branch:             fs.cfg.Branch,
		Epoch:              fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()

	oldDelta, oldKnown, newDelta, newKnown := renameParentNlinkDeltas(
		input.NodeId, input.Newdir, oldName, newName, input.Flags,
		source, sourceAttr, target, targetKnown, targetAttr,
	)
	if oldKnown {
		fs.cache.invalidateDirListingTouchParentNowAt(epoch, input.NodeId, oldDelta)
	} else {
		fs.cache.invalidateDirListingDropParent(input.NodeId)
	}
	if input.Newdir != input.NodeId {
		if newKnown {
			fs.cache.invalidateDirListingTouchParentNowAt(epoch, input.Newdir, newDelta)
		} else {
			fs.cache.invalidateDirListingDropParent(input.Newdir)
		}
	}
	if input.Flags&renameExchange != 0 {

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
	sourceTypeKnown := source != 0 && sourceAttr != nil && sourceAttr.GetAttr() != nil
	sourceIsDir := sourceTypeKnown && sourceAttr.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
	dstTypeKnown := target != 0 && targetAttr != nil && targetAttr.GetAttr() != nil
	dstIsDir := dstTypeKnown && targetAttr.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
	if source != 0 {
		fs.cache.invalidateInode(source)
	}
	if target != 0 && target != source {
		if input.Flags&renameExchange == 0 && dstIsDir {
			fs.handles.MarkRemovedDir(target, targetAttr)
		}
		fs.cache.invalidateInode(target)
		if input.Flags&renameExchange == 0 && (dstIsDir || (!dstTypeKnown && (!sourceTypeKnown || sourceIsDir))) {
			fs.cache.invalidateDir(target)
		}
	}
	if input.NodeId != input.Newdir {
		if source == 0 {
			fs.cache.invalidateChildParents(input.NodeId)
		}
		if input.Flags&renameExchange != 0 && target == 0 {
			fs.cache.invalidateChildParents(input.Newdir)
		}
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

func renameParentNlinkDeltas(
	oldDir, newDir uint64,
	oldName, newName string,
	flags uint32,
	source uint64,
	sourceAttr *storagev1.Inode,
	target uint64,
	targetKnown bool,
	targetAttr *storagev1.Inode,
) (oldDelta int32, oldKnown bool, newDelta int32, newKnown bool) {
	sameParent := oldDir == newDir
	if (sameParent && oldName == newName) || (source != 0 && target != 0 && source == target) {
		return 0, true, 0, true
	}

	srcTypeKnown := source != 0 && sourceAttr != nil && sourceAttr.GetAttr() != nil
	srcIsDir := srcTypeKnown && sourceAttr.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR

	if flags&renameExchange != 0 {
		if sameParent {
			return 0, true, 0, true
		}
		dstTypeKnown := target != 0 && targetAttr != nil && targetAttr.GetAttr() != nil
		if !srcTypeKnown || !dstTypeKnown {
			return 0, false, 0, false
		}
		dstIsDir := targetAttr.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
		if srcIsDir == dstIsDir {
			return 0, true, 0, true
		}
		if srcIsDir {
			return -1, true, 1, true
		}
		return 1, true, -1, true
	}

	var targetExistenceKnown, hasTarget bool
	switch {
	case flags&renameNoReplace != 0:
		targetExistenceKnown = true
	case targetKnown && target == 0:
		targetExistenceKnown = true
	case targetKnown && target != 0:
		targetExistenceKnown = true
		hasTarget = true
	}

	if !srcTypeKnown {
		if sameParent && targetExistenceKnown && !hasTarget {
			return 0, true, 0, true
		}
		return 0, false, 0, false
	}
	if !srcIsDir {
		return 0, true, 0, true
	}
	if sameParent {
		if !targetExistenceKnown {
			return 0, false, 0, false
		}
		if hasTarget {
			return -1, true, 0, true
		}
		return 0, true, 0, true
	}
	if !targetExistenceKnown {
		return -1, true, 0, false
	}
	if hasTarget {
		return -1, true, 0, true
	}
	return -1, true, 1, true
}
