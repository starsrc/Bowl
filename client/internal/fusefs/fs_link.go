package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func (fs *RawFS) Symlink(_ <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpSymlink)
	opCtx, endSpan := fs.startFuseOp(OpSymlink)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpSymlink, start, status, 0)
	}()

	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}
	if fs.isVirtualStatsName(header.NodeId, linkName) {
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
	epoch := fs.cache.version()
	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()

	resp, err := fs.client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		ParentIno: parentIno,
		Name:      []byte(linkName),
		Target:    []byte(pointedTo),
		Uid:       header.Uid,
		Gid:       header.Gid,
		DiskId:    fs.cfg.DiskID,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()

	attrTTL := resp.Msg.GetAttrTimeoutSeconds()
	fs.cache.invalidateDirListingTouchParentAt(epoch, header.NodeId, resp.Msg.GetInode(), 0, attrTTL)
	inode := resp.Msg.GetInode()
	if inode != nil {
		child := fs.ToKernelIno(inode.GetIno())
		fs.cache.putEntryAt(epoch, header.NodeId, linkName, child)
		fs.cache.putInodeAt(epoch, child, inode, resp.Msg.GetEntryTimeoutSeconds(), attrTTL)
		linkTTL := attrTTL
		if linkTTL <= 0 {
			linkTTL = fs.cfg.AttrTimeoutSeconds
		}
		fs.cache.putReadlinkAt(epoch, child, []byte(pointedTo), linkTTL)
	}
	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), attrTTL)
	return fuse.OK
}

func (fs *RawFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (target []byte, status fuse.Status) {
	start := fs.metrics.beginOp(OpReadlink)
	opCtx, endSpan := fs.startFuseOp(OpReadlink)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpReadlink, start, status, 0)
	}()

	if fs.isVirtualStatsIno(header.NodeId) {
		return nil, fuse.EINVAL
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	ino, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return nil, status
	}

	if !fs.IsFenced() {
		if cachedTarget, ok := fs.cache.readlink(header.NodeId); ok {
			return cachedTarget, fuse.OK
		}
	}

	epoch, generation := fs.cache.snapshot()
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
		Ino:    ino,
		DiskId: fs.cfg.DiskID,
	}))
	fenced := fs.IsFenced()
	if !fenced && fs.cache.version() != epoch {
		return nil, fuse.Status(syscall.ESTALE)
	}
	if err != nil {
		return nil, fs.ToStatus(err)
	}

	target = resp.Msg.GetTarget()
	if !fenced {
		ttl := fs.cache.inodeAttrTimeout(header.NodeId, fs.cfg.AttrTimeoutSeconds)
		fs.cache.putReadlinkAtIf(generation, epoch, header.NodeId, target, ttl)
	}
	return target, fuse.OK
}

func (fs *RawFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpLink)
	opCtx, endSpan := fs.startFuseOp(OpLink)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpLink, start, status, 0)
	}()

	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}
	if fs.isVirtualStatsIno(input.Oldnodeid) || fs.isVirtualStatsName(input.NodeId, filename) {
		return fuse.EACCES
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if st, local := fs.linkTmp(opCtx, input, filename, out); local {
		return st
	}
	srcIno, status := fs.ToServerIno(input.Oldnodeid)
	if status != fuse.OK {
		return status
	}
	if st := fs.barrierInode(opCtx, cancel, srcIno); st != fuse.OK {
		return st
	}

	newParentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	unlock := fs.inodeLocks.lock(input.Oldnodeid)
	defer unlock()

	epoch := fs.cache.version()
	detachedCtx := context.WithoutCancel(opCtx)
	ctx, cancelFunc := fs.RPCContextFrom(detachedCtx, nil)
	defer cancelFunc()

	resp, err := fs.client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		Ino:          srcIno,
		NewParentIno: newParentIno,
		NewName:      []byte(filename),
		DiskId:       fs.cfg.DiskID,
		Epoch:        fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()
	if fs.clearOrphan(input.Oldnodeid) {
		fs.metrics.incTmpPromote()
	}

	inode := resp.Msg.GetInode()
	fs.cache.invalidateDirListingTouchParentAt(epoch, input.NodeId, inode, 0, resp.Msg.GetAttrTimeoutSeconds())
	fs.cache.putEntryAt(epoch, input.NodeId, filename, input.Oldnodeid)
	if inode != nil && inode.GetAttr() != nil {
		fs.cache.putInodeAt(epoch, input.Oldnodeid, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	} else {
		fs.cache.invalidateInode(input.Oldnodeid)
	}
	fs.topologySeq.Add(1)
	fs.recordInodeSpecial(input.Oldnodeid, inode)
	fs.FillEntryOut(out, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}
