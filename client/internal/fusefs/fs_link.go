package fusefs

import (
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Symlink 实现创建符号链接操作。
// 校验只读挂载、重写父 Inode，透传调用进程 UID/GID、目标路径与链接名称。
// 注意 go-fuse 参数顺序：pointedTo 为符号链接指向的目标路径，linkName 为待创建的目录项名称。
func (fs *RawFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) (status fuse.Status) {
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
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
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
		return fs.ToStatus(err, cancel)
	}

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
		fs.cache.putReadlinkAt(epoch, child, []byte(pointedTo), linkTTL, time.Now())
	}
	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), attrTTL)
	return fuse.OK
}

// Readlink 实现读取符号链接目标路径操作。
// 在受 TTL 管控的缓存有效期（attrTimeout > 0）内直接命中本地缓存；strict 0s 模式下自动穿透服务端。
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

	now := time.Now()
	if cachedTarget, ok := fs.cache.readlink(header.NodeId, now); ok {
		return cachedTarget, fuse.OK
	}

	epoch, generation := fs.cache.snapshot()
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
		Ino:    ino,
		DiskId: fs.cfg.DiskID,
	}))
	if fs.cache.version() != epoch {
		return nil, fuse.Status(syscall.ESTALE)
	}
	if err != nil {
		return nil, fs.ToStatus(err, cancel)
	}

	target = resp.Msg.GetTarget()
	ttl := fs.cache.inodeAttrTimeout(header.NodeId, fs.cfg.AttrTimeoutSeconds)
	fs.cache.putReadlinkAtIf(generation, epoch, header.NodeId, target, ttl, time.Now())
	return target, fuse.OK
}

// Link 实现为非目录已有文件创建同盘硬链接操作。
// 校验只读挂载，同时对源文件节点与目标父目录执行根 Inode 双向重写，回填递增 nlink 后的 Inode 属性。
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
	if st, local := fs.linkTmp(opCtx, cancel, input, filename, out); local {
		return st
	}
	srcIno, status := fs.ToServerIno(input.Oldnodeid)
	if status != fuse.OK {
		return status
	}
	fs.stage.BarrierInode(opCtx, srcIno)

	newParentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	epoch := fs.cache.version()
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		Ino:          srcIno,
		NewParentIno: newParentIno,
		NewName:      []byte(filename),
		DiskId:       fs.cfg.DiskID,
		Epoch:        fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.cache.invalidateDirListingTouchParentAt(epoch, input.NodeId, resp.Msg.GetInode(), 0, resp.Msg.GetAttrTimeoutSeconds())
	fs.cache.putEntryAt(epoch, input.NodeId, filename, input.Oldnodeid)
	fs.cache.putInodeAt(epoch, input.Oldnodeid, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	fs.recordInodeSpecial(input.Oldnodeid, resp.Msg.GetInode())
	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}
