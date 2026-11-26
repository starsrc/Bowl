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

	epoch := fs.cache.version()
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

	fs.cache.invalidateDirListingTouchParentAt(epoch, input.NodeId, resp.Msg.GetInode(), 1, resp.Msg.GetAttrTimeoutSeconds())
	inode := resp.Msg.GetInode()
	if inode != nil {
		child := fs.ToKernelIno(inode.GetIno())
		fs.cache.putEntryAt(epoch, input.NodeId, name, child)
		fs.cache.putInodeAt(epoch, child, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())

		fs.cache.markNewDirWithParentAt(epoch, child, input.NodeId)
	}
	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
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

	epoch := fs.cache.version()
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

	fs.cache.invalidateDirListingTouchParentAt(epoch, input.NodeId, resp.Msg.GetInode(), 0, resp.Msg.GetAttrTimeoutSeconds())
	inode := resp.Msg.GetInode()
	if inode != nil {
		child := fs.ToKernelIno(inode.GetIno())
		fs.cache.putEntryAt(epoch, input.NodeId, name, child)
		fs.cache.putInodeAt(epoch, child, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
		fs.caps.markAbsent(child)
	}
	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
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

	child, _ := fs.cache.entry(header.NodeId, name)
	var inode *storagev1.Inode
	if child != 0 {
		if cached, ok := fs.cache.peekInode(child); ok {
			inode = cached.inode
		}
	}
	if fs.handles.AnyOpen() {
		cached, st := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
		if st != fuse.OK {
			return st
		}
		inode = cached.inode
		child = fs.ToKernelIno(inode.GetIno())
	}
	if child != 0 {
		unlock := fs.inodeLocks.lock(child)
		defer unlock()
		if fs.handles.IsOpen(child) && (inode == nil || inode.GetAttr() == nil) {
			if cached, ok := fs.cache.peekInode(child); ok {
				inode = cached.inode
			}
			if inode == nil || inode.GetAttr() == nil {
				refreshed, st := fs.getInodeWithContext(opCtx, cancel, child)
				switch st {
				case fuse.OK:
					inode = refreshed.inode
				case fuse.ENOENT:
					inode = nil
				default:
					return st
				}
			}
		}
	}
	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()
	epoch := fs.cache.version()
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

	fs.cache.invalidateDirListingTouchParentNowAt(epoch, header.NodeId, -1)

	fs.cache.putEntryAt(epoch, header.NodeId, name, 0)
	if child != 0 {
		fs.handles.MarkRemovedDir(child, inode)
		fs.cache.invalidateInode(child)
		fs.cache.invalidateDir(child)
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
	child, _ := fs.cache.entry(header.NodeId, name)
	var inode *storagev1.Inode
	if child != 0 {
		if cached, ok := fs.cache.peekInode(child); ok {
			inode = cached.inode
		}
	}
	needLookup := fs.handles.AnyOpen() || fs.stage.HasAnyPending()
	if needLookup {
		cached, st := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
		if st != fuse.OK {
			return st
		}
		inode = cached.inode
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
		if hasChildServerIno && inode.GetAttr().GetFlags()&ProtoFlagImmutable != 0 {
			if err := fs.stage.BarrierInode(barrierCtx, childServerIno); err != nil {
				return fs.stageErrToStatus(err)
			}
		}

		unlock := fs.inodeLocks.lock(child)
		defer unlock()
		needRevalidate := fs.topologySeq.Load() != seq0 && (fs.handles.AnyOpen() || fs.stage.HasAnyPending())
		needOpenChildAttr := fs.handles.IsOpen(child) && (inode == nil || inode.GetAttr() == nil)
		if needRevalidate || needOpenChildAttr {
			refreshed, st := fs.getInodeWithContext(barrierCtx, nil, child)
			switch st {
			case fuse.OK:
				inode = refreshed.inode
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
	epoch := fs.cache.version()
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

	fs.cache.invalidateDirListingTouchParentNowAt(epoch, header.NodeId, 0)

	fs.cache.putEntryAt(epoch, header.NodeId, name, 0)
	if child != 0 {
		fs.cache.invalidateInode(child)
	}
	fs.topologySeq.Add(1)
	if inode != nil && inode.GetAttr() != nil && inode.GetAttr().GetNlink() == 1 {
		if open {
			fs.markOrphanWithContext(opCtx, child)
		} else {
			if hasChildServerIno {
				fs.stage.DiscardInode(childServerIno)
				fs.readAhead.InvalidateInode(childServerIno)
			}
			if child != 0 {
				fs.forgetInodeSpecial(child)
			}
		}
	}
	return fuse.OK
}
