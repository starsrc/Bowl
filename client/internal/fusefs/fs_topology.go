package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Mkdir 实现创建子目录操作，校验只读挂载、重写父 Inode，透传模式位与调用进程 UID/GID。
func (fs *RawFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) (status fuse.Status) {
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
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      input.Mode,
		Uid:       input.Uid,
		Gid:       input.Gid,
		DiskId:    fs.cfg.DiskID,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}
	fs.markMetaDirty()

	fs.cache.invalidateDirListingTouchParentAt(epoch, input.NodeId, resp.Msg.GetInode(), 1, resp.Msg.GetAttrTimeoutSeconds())
	inode := resp.Msg.GetInode()
	if inode != nil {
		child := fs.ToKernelIno(inode.GetIno())
		fs.cache.putEntryAt(epoch, input.NodeId, name, child)
		fs.cache.putInodeAt(epoch, child, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
		// 新建目录在本次会话内为空：登记为权威完整目录并记录其父目录，目录内首次 Create/Mkdir 的
		// Lookup 可直接返回 ENOENT，且后续冷读 ReadDirPlus 可直接从本地缓存合成完整目录流。
		fs.cache.markNewDirWithParentAt(epoch, child, input.NodeId)
	}
	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

// Mknod 实现非目录特殊节点（如 FIFO 命名管道、设备节点、普通文件）的创建操作。
func (fs *RawFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) (status fuse.Status) {
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
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      input.Mode,
		Uid:       input.Uid,
		Gid:       input.Gid,
		Rdev:      input.Rdev,
		DiskId:    fs.cfg.DiskID,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
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

// Rmdir 移除指定父目录下的空目录项。
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
	if fs.handles.AnyOpen() {
		cached, st := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
		if st != fuse.OK {
			return st
		}
		inode = cached.inode
		child = fs.ToKernelIno(inode.GetIno())
	}
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()
	epoch := fs.cache.version()
	_, err := fs.client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		DiskId:    fs.cfg.DiskID,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}
	fs.markMetaDirty()

	// Rmdir 使父目录 nlink 减 1；保留父目录 Inode 缓存避免随后的 GetAttr 穿透。
	fs.cache.invalidateDirListingTouchParentNowAt(epoch, header.NodeId, -1)
	// 目录项已确定不存在，回填负缓存（ino=0），避免随后的 access()/stat() 再次穿透 RPC。
	fs.cache.putEntryAt(epoch, header.NodeId, name, 0)
	if child != 0 {
		fs.handles.MarkRemovedDir(child, inode)
		fs.cache.invalidateInode(child)
		fs.cache.invalidateDir(child)
	}
	return fuse.OK
}

// Unlink 删除指定父目录下的非目录项（解链）。
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
	needLookup := fs.handles.AnyOpen() || fs.stage.HasAnyPending() ||
		(fs.hasDirtyInodes() && (child == 0 || inode == nil || inode.GetAttr() == nil))
	if needLookup {
		cached, st := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
		if st != fuse.OK {
			return st
		}
		inode = cached.inode
		child = fs.ToKernelIno(inode.GetIno())
	}

	var childServerIno uint64
	var hasChildServerIno bool
	if child != 0 {
		if sIno, st := fs.ToServerIno(child); st == fuse.OK {
			childServerIno = sIno
			hasChildServerIno = true
		}
		// 与并发 Release 的最后一个句柄关闭按 Inode 串行，保证孤儿标记不会互相错过。
		unlock := fs.inodeLocks.lock(child)
		defer unlock()
		if fs.topologySeq.Load() != seq0 && (fs.handles.AnyOpen() || fs.stage.HasAnyPending() || fs.hasDirtyInodes()) {
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
	open := child != 0 && fs.handles.IsOpen(child)
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()
	epoch := fs.cache.version()
	_, err := fs.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno:    parentIno,
		Name:         []byte(name),
		OpenInClient: &open,
		DiskId:       fs.cfg.DiskID,
		Epoch:        fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}
	fs.markMetaDirty()

	// Unlink 不改变父目录 nlink；保留父目录 Inode 缓存避免下一次 unlink 前
	// inode_permission(MAY_EXEC) 触发一次跨机 GetAttr。
	fs.cache.invalidateDirListingTouchParentNowAt(epoch, header.NodeId, 0)
	// 目录项已确定不存在，回填负缓存（ino=0）。SQLite DELETE 模式在提交后
	// unlink("*-journal")，下一个事务的 hasHotJournal() 会立即 access() 该名字；
	// 负缓存命中可消除这次 100% 穿透的跨机 Lookup RPC。
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
				fs.clearDirty(childServerIno)
			}
			if child != 0 {
				fs.forgetInodeSpecial(child)
			}
		}
	}
	return fuse.OK
}
