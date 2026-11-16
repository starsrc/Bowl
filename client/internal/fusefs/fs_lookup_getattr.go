package fusefs

import (
	"context"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func (fs *RawFS) lookupInodeWithContext(parentCtx context.Context, cancel <-chan struct{}, parent uint64, name string) (cachedInode, fuse.Status) {
	parentIno, status := fs.ToServerIno(parent)
	if status != fuse.OK {
		return cachedInode{}, status
	}
	if len(name) > MaxFilenameBytes {
		return cachedInode{}, fuse.Status(syscall.ENAMETOOLONG)
	}
	cached, _, negative, ok := fs.cache.lookupReadOnly(parent, name)
	return fs.lookupInodeAfterCache(parentCtx, cancel, parent, parentIno, name, cached, negative, ok)
}

func (fs *RawFS) lookupInodeAfterCache(
	parentCtx context.Context,
	cancel <-chan struct{},
	parent, parentIno uint64,
	name string,
	cached cachedInode,
	negative, ok bool,
) (cachedInode, fuse.Status) {
	if ok {
		if negative {
			return cachedInode{}, fuse.ENOENT
		}
		return cached, fuse.OK
	}
	epoch, generation := fs.cache.snapshot()
	ctx, cancelFunc := fs.RPCContextFrom(parentCtx, cancel)
	defer cancelFunc()
	resp, err := fs.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		DiskId:    fs.cfg.DiskID,
	}))
	if fs.cache.version() != epoch {
		return cachedInode{}, fuse.Status(syscall.ESTALE)
	}
	if err != nil {
		status := fs.ToStatus(err, cancel)
		if status == fuse.ENOENT {
			fs.metrics.incNSCacheMiss(nsCacheNegativeEntry)
			fs.cache.putEntryAtIf(generation, epoch, parent, name, 0)
		}
		return cachedInode{}, status
	}
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return cachedInode{}, fuse.EIO
	}
	ino := fs.ToKernelIno(inode.GetIno())
	fs.recordInodeSpecial(ino, inode)
	fs.stage.OverlayInode(inode.GetIno(), inode)
	cached = cachedInode{inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds()}
	fs.cache.putEntryAtIf(generation, epoch, parent, name, ino)
	fs.cache.putInodeAtIf(generation, epoch, ino, inode, cached.entryTimeout, cached.attrTimeout)
	return cached, fuse.OK
}

func (fs *RawFS) getInodeWithContext(parentCtx context.Context, cancel <-chan struct{}, ino uint64) (cachedInode, fuse.Status) {
	if inode, ok := fs.tmpMgr.LocalInode(ino); ok {
		return cachedInode{inode: inode}, fuse.OK
	}
	serverIno, status := fs.ToServerIno(ino)
	if status != fuse.OK {
		return cachedInode{}, status
	}
	if cached, ok := fs.cache.inodeReadOnly(ino); ok {
		return cached, fuse.OK
	}
	return fs.getInodeMissWithContext(parentCtx, cancel, ino, serverIno)
}

func (fs *RawFS) getInodeMissWithContext(parentCtx context.Context, cancel <-chan struct{}, ino, serverIno uint64) (cachedInode, fuse.Status) {
	epoch, generation := fs.cache.snapshot()
	ctx, cancelFunc := fs.RPCContextFrom(parentCtx, cancel)
	defer cancelFunc()
	resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino:    serverIno,
		DiskId: fs.cfg.DiskID,
	}))
	if fs.cache.version() != epoch {
		return cachedInode{}, fuse.Status(syscall.ESTALE)
	}
	if err != nil {
		return cachedInode{}, fs.ToStatus(err, cancel)
	}
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return cachedInode{}, fuse.EIO
	}
	fs.recordInodeSpecial(ino, inode)
	fs.stage.OverlayInode(serverIno, inode)
	cached := cachedInode{inode: inode, attrTimeout: resp.Msg.GetAttrTimeoutSeconds()}
	fs.cache.refreshInodeAtIf(generation, epoch, ino, inode, cached.attrTimeout)
	return cached, fuse.OK
}

// Lookup 在缓存命中时直接返回目录项，并零阻塞叠加本地暂存尺寸与时间戳。
// 主体保持零 defer、零闭包，确保本地缓存命中快路径 0 allocs/op。
func (fs *RawFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpLookup)
	if !fs.isVirtualStatsName(header.NodeId, name) && len(name) <= MaxFilenameBytes && fs.cfg.AttrTimeoutSeconds > 0 {
		if _, st := fs.ToServerIno(header.NodeId); st == fuse.OK {
			fs.lifecycleMu.RLock()
			cached, _, negative, ok := fs.cache.lookupReadOnly(header.NodeId, name)
			if ok && !negative {
				fs.FillEntryOut(out, cached.inode, cached.entryTimeout, cached.attrTimeout)
				fs.stage.OverlayAttr(cached.inode.GetIno(), &out.Attr)
				out.NodeId = fs.tmpMgr.ObserveKernelLookup(cached.inode.GetIno())
				out.Ino = out.NodeId
				fs.lifecycleMu.RUnlock()
				fs.metrics.endOp(OpLookup, start, fuse.OK, 0)
				return fuse.OK
			}
			parentIno, _ := fs.ToServerIno(header.NodeId)
			opCtx, endSpan := fs.startFuseOp(OpLookup)
			cached, status = fs.lookupInodeAfterCache(opCtx, cancel, header.NodeId, parentIno, name, cached, negative, ok)
			if status == fuse.OK {
				fs.FillEntryOut(out, cached.inode, cached.entryTimeout, cached.attrTimeout)
				fs.stage.OverlayAttr(cached.inode.GetIno(), &out.Attr)
				out.NodeId = fs.tmpMgr.ObserveKernelLookup(cached.inode.GetIno())
				out.Ino = out.NodeId
			}
			fs.lifecycleMu.RUnlock()
			endSpan(status)
			fs.metrics.endOp(OpLookup, start, status, 0)
			return status
		}
	}
	return fs.lookupSlow(cancel, header, name, out, start)
}

func (fs *RawFS) lookupSlow(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut, start time.Time) (status fuse.Status) {
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
	cached, status := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
	if status == fuse.OK {
		fs.FillEntryOut(out, cached.inode, cached.entryTimeout, cached.attrTimeout)
		fs.stage.OverlayAttr(cached.inode.GetIno(), &out.Attr)
		out.NodeId = fs.tmpMgr.ObserveKernelLookup(cached.inode.GetIno())
		out.Ino = out.NodeId
	}
	return status
}

// GetAttr 在缓存命中时直接返回节点属性，并零阻塞叠加本地暂存尺寸与时间戳。
// 主体保持零 defer、零闭包，确保本地缓存命中快路径 0 allocs/op。
func (fs *RawFS) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpGetAttr)
	if !fs.isVirtualStatsIno(in.NodeId) && fs.cfg.AttrTimeoutSeconds > 0 {
		if serverIno, st := fs.ToServerIno(in.NodeId); st == fuse.OK {
			fs.lifecycleMu.RLock()
			if inode, removed := fs.handles.RemovedDir(in.NodeId); removed {
				fs.FillAttrOut(out, inode, 0)
				fs.lifecycleMu.RUnlock()
				fs.metrics.endOp(OpGetAttr, start, fuse.OK, 0)
				return fuse.OK
			}
			if handled, tmpSt := fs.tmpMgr.GetAttr(in.NodeId, &out.Attr); handled {
				out.SetTimeout(0)
				fs.lifecycleMu.RUnlock()
				if tmpSt != fuse.OK {
					_, endSpan := fs.startFuseOp(OpGetAttr)
					endSpan(tmpSt)
				}
				fs.metrics.endOp(OpGetAttr, start, tmpSt, 0)
				return tmpSt
			}
			if cached, ok := fs.cache.inodeReadOnly(in.NodeId); ok {
				fs.FillAttrOut(out, cached.inode, cached.attrTimeout)
				fs.stage.OverlayAttr(serverIno, &out.Attr)
				fs.lifecycleMu.RUnlock()
				fs.metrics.endOp(OpGetAttr, start, fuse.OK, 0)
				return fuse.OK
			}
			opCtx, endSpan := fs.startFuseOp(OpGetAttr)
			var cached cachedInode
			cached, status = fs.getInodeMissWithContext(opCtx, cancel, in.NodeId, serverIno)
			if status == fuse.OK {
				fs.FillAttrOut(out, cached.inode, cached.attrTimeout)
				fs.stage.OverlayAttr(serverIno, &out.Attr)
			}
			fs.lifecycleMu.RUnlock()
			endSpan(status)
			fs.metrics.endOp(OpGetAttr, start, status, 0)
			return status
		}
	}
	return fs.getAttrSlow(cancel, in, out, start)
}

func (fs *RawFS) getAttrSlow(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut, start time.Time) (status fuse.Status) {
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
	if handled, st := fs.tmpMgr.GetAttr(in.NodeId, &out.Attr); handled {
		out.SetTimeout(0)
		return st
	}
	cached, status := fs.getInodeWithContext(opCtx, cancel, in.NodeId)
	if status == fuse.OK {
		fs.FillAttrOut(out, cached.inode, cached.attrTimeout)
		fs.stage.OverlayAttr(serverIno, &out.Attr)
	}
	return status
}

// StatFs 实现文件系统容量与 Inode 配额统计查询，对应 Linux FUSE FUSE_STATFS 协议。
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
	fs.stage.BarrierAll(opCtx)

	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		Ino:    ino,
		DiskId: fs.cfg.DiskID,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
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
