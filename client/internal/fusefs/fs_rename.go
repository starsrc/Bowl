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
	source, sourceKnown := fs.dentries.get(input.NodeId, oldName)
	target, targetKnown := fs.dentries.get(input.Newdir, newName)
	var targetAttr *storagev1.Inode

	anyActive := fs.handles.AnyOpenFiles() || fs.handles.AnyOpenDirs() || fs.stage.HasAnyPending()
	if !sourceKnown && fs.stage.HasAnyPending() {
		oldInode, _, _, oldStatus := fs.lookupInodeWithContext(opCtx, cancel, input.NodeId, oldName)
		if oldStatus != fuse.OK {
			return oldStatus
		}
		source = fs.ToKernelIno(oldInode.GetIno())
	}
	if !targetKnown && input.Flags&renameNoReplace == 0 && anyActive {
		tgtInode, _, _, targetStatus := fs.lookupInodeWithContext(opCtx, cancel, input.Newdir, newName)
		if targetStatus != fuse.OK && targetStatus != fuse.ENOENT {
			return targetStatus
		}
		if targetStatus == fuse.OK {
			targetAttr = tgtInode
			target = fs.ToKernelIno(targetAttr.GetIno())
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
			targetOpenOrStaged := fs.handles.IsOpen(target) || (hasTargetServerIno && fs.stage.HasPending(targetServerIno))
			needRevalidate := fs.topologySeq.Load() != seq0 && targetOpenOrStaged
			if targetOpenOrStaged && (targetAttr == nil || targetAttr.GetAttr() == nil || needRevalidate) {
				refreshed, _, refSt := fs.getAttrInodeWithContext(barrierCtx, nil, target)
				switch refSt {
				case fuse.OK:
					targetAttr = refreshed
				case fuse.ENOENT:
					targetAttr = nil
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

	if input.Flags&renameExchange != 0 {
		if target != 0 {
			fs.dentries.put(input.NodeId, oldName, target)
		} else {
			fs.dentries.deleteName(input.NodeId, oldName)
		}
		if source != 0 {
			fs.dentries.put(input.Newdir, newName, source)
		} else {
			fs.dentries.deleteName(input.Newdir, newName)
		}
	} else {
		if input.Flags&renameWhiteout != 0 {
			fs.dentries.deleteName(input.NodeId, oldName)
		} else if input.NodeId != input.Newdir || oldName != newName {
			fs.dentries.deleteName(input.NodeId, oldName)
		}
		if source != 0 {
			fs.dentries.put(input.Newdir, newName, source)
		} else {
			fs.dentries.deleteName(input.Newdir, newName)
		}
	}

	fs.topologySeq.Add(1)
	if target != 0 && target != source && input.Flags&renameExchange == 0 {
		fs.forgetKnownInode(target)
		if targetIsDir {
			fs.handles.MarkRemovedDir(target, targetAttr)
			fs.dentries.forgetChild(target)
		}
		if targetAttr != nil && targetAttr.GetAttr() != nil && targetAttr.GetAttr().GetNlink() == 1 {
			if targetOpen {
				fs.markOrphanWithContext(opCtx, target)
			} else if hasTargetServerIno {
				fs.stage.DiscardInode(targetServerIno)
				fs.readAhead.InvalidateInode(targetServerIno)
			}
		} else if !targetOpen && hasTargetServerIno {
			fs.readAhead.InvalidateInode(targetServerIno)
		}
	}
	return fuse.OK
}
