package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func (fs *RawFS) lookupInodeWithContext(parentCtx context.Context, cancel <-chan struct{}, parent uint64, name string) (inode *storagev1.Inode, entryTTL, attrTTL float64, status fuse.Status) {
	parentIno, st := fs.ToServerIno(parent)
	if st != fuse.OK {
		return nil, 0, 0, st
	}
	if len(name) > MaxFilenameBytes {
		return nil, 0, 0, fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.RPCContextFrom(parentCtx, cancel)
	defer cancelFunc()
	resp, err := fs.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		DiskId:    fs.cfg.DiskID,
		Branch:    fs.cfg.Branch,
	}))
	if err != nil {
		return nil, 0, 0, fs.ToStatus(err)
	}
	inode = resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return nil, 0, 0, fuse.EIO
	}
	if fs.IsFenced() {
		return inode, 0, 0, fuse.OK
	}
	return inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

func (fs *RawFS) getAttrInodeWithContext(parentCtx context.Context, cancel <-chan struct{}, ino uint64) (inode *storagev1.Inode, attrTTL float64, status fuse.Status) {
	if localInode, ok := fs.tmpMgr.LocalInode(ino); ok {
		return localInode, 0, fuse.OK
	}
	serverIno, st := fs.ToServerIno(ino)
	if st != fuse.OK {
		return nil, 0, st
	}
	g0 := fs.metaClock.Load()
	ctx, cancelFunc := fs.RPCContextFrom(parentCtx, cancel)
	defer cancelFunc()
	resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino:    serverIno,
		DiskId: fs.cfg.DiskID,
		Branch: fs.cfg.Branch,
	}))
	if err != nil {
		return nil, 0, fs.ToStatus(err)
	}
	inode = resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return nil, 0, fuse.EIO
	}
	if fs.IsFenced() {
		return inode, 0, fuse.OK
	}
	fs.recordKnownInode(ino, inode, g0)
	return inode, resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

func (fs *RawFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpLookup)
	opCtx, endSpan := fs.startFuseOp(OpLookup)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpLookup, start, status, 0)
	}()

	if fs.isVirtualStatsName(header.NodeId, name) {
		fs.fillVirtualStatsEntryOut(out)
		return fuse.OK
	}
	if _, st := fs.ToServerIno(header.NodeId); st != fuse.OK {
		return st
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	g0 := fs.metaClock.Load()
	inode, entryTTL, attrTTL, status := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
	if status == fuse.OK {
		fs.FillEntryOut(out, inode, entryTTL, attrTTL)
		fs.stage.OverlayDirty(inode.GetIno(), &out.Attr)
		out.NodeId = fs.tmpMgr.ObserveKernelLookup(inode.GetIno())
		out.Ino = out.NodeId
		if !fs.IsFenced() {
			fs.recordKnownInode(out.NodeId, inode, g0)
			fs.dentries.put(header.NodeId, name, out.NodeId)
		}
	}
	return status
}

func (fs *RawFS) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpGetAttr)
	opCtx, endSpan := fs.startFuseOp(OpGetAttr)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpGetAttr, start, status, 0)
	}()

	if fs.isVirtualStatsIno(in.NodeId) {
		out.SetTimeout(0)
		fs.fillVirtualStatsAttr(&out.Attr)
		return fuse.OK
	}
	serverIno, st := fs.ToServerIno(in.NodeId)
	if st != fuse.OK {
		return st
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if inode, removed := fs.handles.RemovedDir(in.NodeId); removed {
		fs.FillAttrOut(out, inode, 0)
		return fuse.OK
	}
	if handled, tmpSt := fs.tmpMgr.GetAttr(in.NodeId, &out.Attr); handled {
		out.SetTimeout(0)
		return tmpSt
	}
	inode, attrTTL, status := fs.getAttrInodeWithContext(opCtx, cancel, in.NodeId)
	if status == fuse.OK {
		fs.FillAttrOut(out, inode, attrTTL)
		fs.stage.OverlayDirty(serverIno, &out.Attr)
	}
	return status
}

func (fs *RawFS) StatFs(cancel <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpStatFs)
	opCtx, endSpan := fs.startFuseOp(OpStatFs)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpStatFs, start, status, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if fs.isVirtualStatsIno(in.NodeId) {
		inCopy := *in
		inCopy.NodeId = fuse.FUSE_ROOT_ID
		in = &inCopy
	}
	ino, status := fs.ToServerIno(in.NodeId)
	if status != fuse.OK {
		return status
	}
	barrierCtx, barrierCancel := fs.stageBarrierContext(opCtx, cancel)
	defer barrierCancel()
	if err := fs.stage.BarrierAll(barrierCtx); err != nil {
		return fs.stageErrToStatus(err)
	}

	ctx, cancelFunc := fs.RPCContextFrom(barrierCtx, nil)
	defer cancelFunc()

	resp, err := fs.client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		Ino:    ino,
		DiskId: fs.cfg.DiskID,
		Branch: fs.cfg.Branch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}

	msg := resp.Msg
	out.Blocks = msg.GetBlocks()
	out.Bfree = msg.GetBfree()
	out.Bavail = msg.GetBavail()
	out.Files = msg.GetFiles()
	out.Ffree = msg.GetFfree()
	out.Bsize = msg.GetBsize()
	out.Frsize = msg.GetFrsize()
	out.NameLen = msg.GetNamelen()

	return fuse.OK
}
