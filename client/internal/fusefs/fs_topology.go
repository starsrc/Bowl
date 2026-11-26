package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func (fs *RawFS) Mkdir(_ <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpMkdir)
	opCtx, endSpan := fs.startFuseOp(OpMkdir)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpMkdir, start, status, 0)
	}()

	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}
	if fs.isVirtualStatsName(input.NodeId, name) {
		return fuse.EACCES
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}

	parentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()

	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()

	resp, err := fs.client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      input.Mode,
		Uid:       input.Uid,
		Gid:       input.Gid,
		DiskId:    fs.cfg.DiskID,
		Branch:    fs.cfg.Branch,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()

	inode := resp.Msg.GetInode()
	if inode != nil {
		child := fs.ToKernelIno(inode.GetIno())
		fs.dentries.put(input.NodeId, name, child)
		fs.recordKnownInode(child, inode)
	}
	fs.FillEntryOut(out, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

func (fs *RawFS) Mknod(_ <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpMknod)
	opCtx, endSpan := fs.startFuseOp(OpMknod)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpMknod, start, status, 0)
	}()

	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}
	if fs.isVirtualStatsName(input.NodeId, name) {
		return fuse.EACCES
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}

	parentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()

	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()

	resp, err := fs.client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      input.Mode,
		Uid:       input.Uid,
		Gid:       input.Gid,
		Rdev:      input.Rdev,
		DiskId:    fs.cfg.DiskID,
		Branch:    fs.cfg.Branch,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()

	inode := resp.Msg.GetInode()
	if inode != nil {
		child := fs.ToKernelIno(inode.GetIno())
		fs.dentries.put(input.NodeId, name, child)
		fs.recordKnownInode(child, inode)
		fs.caps.markAbsent(child)
	}
	fs.FillEntryOut(out, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

func (fs *RawFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (status fuse.Status) {
	start := fs.metrics.beginOp(OpRmdir)
	opCtx, endSpan := fs.startFuseOp(OpRmdir)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpRmdir, start, status, 0)
	}()

	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}
	if fs.isVirtualStatsName(header.NodeId, name) || fs.isVirtualStatsIno(header.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}

	parentIno, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return status
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()

	child, known := fs.dentries.get(header.NodeId, name)
	var inode *storagev1.Inode
	if !known && fs.handles.AnyOpenDirs() {
		looked, _, _, st := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
		if st != fuse.OK {
			return st
		}
		inode = looked
		child = fs.ToKernelIno(inode.GetIno())
	}
	if child != 0 {
		unlock := fs.inodeLocks.lock(child)
		defer unlock()
		if fs.handles.IsOpen(child) && (inode == nil || inode.GetAttr() == nil) {
			refreshed, _, st := fs.getAttrInodeWithContext(opCtx, cancel, child)
			switch st {
			case fuse.OK:
				inode = refreshed
			case fuse.ENOENT:
				inode = nil
			default:
				return st
			}
		}
	}
	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()
	_, err := fs.client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		DiskId:    fs.cfg.DiskID,
		Branch:    fs.cfg.Branch,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()

	fs.dentries.deleteName(header.NodeId, name)
	if child != 0 {
		fs.dentries.forgetChild(child)
		fs.forgetKnownInode(child)
		fs.handles.MarkRemovedDir(child, inode)
	}
	return fuse.OK
}

func (fs *RawFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (status fuse.Status) {
	start := fs.metrics.beginOp(OpUnlink)
	opCtx, endSpan := fs.startFuseOp(OpUnlink)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpUnlink, start, status, 0)
	}()

	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}
	if fs.isVirtualStatsName(header.NodeId, name) {
		return fuse.EACCES
	}
	if fs.isVirtualStatsIno(header.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}

	parentIno, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return status
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()

	seq0 := fs.topologySeq.Load()
	child, known := fs.dentries.get(header.NodeId, name)
	var inode *storagev1.Inode
	if !known && (fs.handles.AnyOpenFiles() || fs.stage.HasAnyPending()) {
		looked, _, _, st := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
		if st != fuse.OK {
			return st
		}
		inode = looked
		child = fs.ToKernelIno(inode.GetIno())
	}

	barrierCtx, barrierCancel := fs.stageBarrierContext(opCtx, cancel)
	defer barrierCancel()
	var childServerIno uint64
	var hasChildServerIno bool
	if child != 0 {
		if sIno, st := fs.ToServerIno(child); st == fuse.OK {
			childServerIno = sIno
			hasChildServerIno = true
		}
		if hasChildServerIno && inode != nil && inode.GetAttr().GetFlags()&ProtoFlagImmutable != 0 {
			if err := fs.stage.BarrierInode(barrierCtx, childServerIno); err != nil {
				return fs.stageErrToStatus(err)
			}
		}

		unlock := fs.inodeLocks.lock(child)
		defer unlock()
		childOpen := fs.handles.IsOpen(child)
		childStaged := hasChildServerIno && fs.stage.HasPending(childServerIno)
		needRevalidate := fs.topologySeq.Load() != seq0 && (childOpen || childStaged)
		if (childOpen || childStaged) && (inode == nil || inode.GetAttr() == nil || needRevalidate) {
			refreshed, _, st := fs.getAttrInodeWithContext(barrierCtx, nil, child)
			switch st {
			case fuse.OK:
				inode = refreshed
				if hasChildServerIno && inode.GetAttr().GetFlags()&ProtoFlagImmutable != 0 {
					if err := fs.stage.BarrierInode(barrierCtx, childServerIno); err != nil {
						return fs.stageErrToStatus(err)
					}
				}
			case fuse.ENOENT:
				inode = nil
			default:
				return st
			}
		}
	}
	open := child != 0 && fs.handles.IsOpen(child)
	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()
	_, err := fs.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno:    parentIno,
		Name:         []byte(name),
		OpenInClient: &open,
		DiskId:       fs.cfg.DiskID,
		Branch:       fs.cfg.Branch,
		Epoch:        fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()

	fs.dentries.deleteName(header.NodeId, name)
	if child != 0 {
		fs.forgetKnownInode(child)
	}
	fs.topologySeq.Add(1)
	if inode != nil && inode.GetAttr() != nil && inode.GetAttr().GetNlink() == 1 {
		if open {
			fs.markOrphanWithContext(opCtx, child)
		} else if hasChildServerIno {
			fs.stage.DiscardInode(childServerIno)
			fs.readAhead.InvalidateInode(childServerIno)
		}
	} else if !open && hasChildServerIno {
		fs.readAhead.InvalidateInode(childServerIno)
	}
	return fuse.OK
}
