package fusefs

import (
	"context"
	"os"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type rawFSTmpMaterializer struct {
	fs    *RawFS
	epoch uint64
}

func (fs *RawFS) newTmpMaterializer(epoch uint64) *rawFSTmpMaterializer {
	return &rawFSTmpMaterializer{
		fs:    fs,
		epoch: epoch,
	}
}

func (m *rawFSTmpMaterializer) CreateTmpInode(ctx context.Context, parentIno uint64, name string, mode, uid, gid uint32) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status) {
	reqCtx := rpc.WithRequestID(ctx, m.fs.NextRequestID())
	resp, err := m.fs.client.Mknod(reqCtx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      mode,
		Uid:       uid,
		Gid:       gid,
		DiskId:    m.fs.cfg.DiskID,
		Epoch:     m.fs.cfg.Epoch,
	}))
	if err != nil {
		return nil, 0, 0, m.fs.ToStatus(err)
	}
	m.fs.markMetaDirty()
	return resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

func (m *rawFSTmpMaterializer) PromoteTmpfile(ctx context.Context, serverIno, nodeID uint64, file *os.File, meta StagePromotedMeta, extents []stageExtent) {
	m.fs.stage.PromoteTmpfile(ctx, serverIno, nodeID, m.epoch, file, meta, extents)
}

// Tmpfile creates an anonymous temporary file via local staging in staged mode or StorageService.CreateTmpfile in sync mode.
func (fs *RawFS) Tmpfile(cancel <-chan struct{}, input *fuse.CreateIn, out *fuse.CreateOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpTmpfile)
	opCtx, endSpan := fs.startFuseOp(OpTmpfile)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpTmpfile, start, status, 0)
	}()

	if status = fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}
	parentIno, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return st
	}
	mode := input.Flags & uint32(syscall.O_ACCMODE)
	if mode != syscall.O_WRONLY && mode != syscall.O_RDWR {
		return fuse.EINVAL
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if !fs.cfg.IsStaged() {
		return fs.tmpfileSync(opCtx, parentIno, input, out)
	}
	if _, removed := fs.handles.RemovedDir(input.NodeId); removed {
		return fuse.ENOENT
	}
	parent, st := fs.getInodeWithContext(opCtx, cancel, input.NodeId)
	if st != fuse.OK {
		return st
	}
	parentAttr := parent.inode.GetAttr()
	if parentAttr == nil {
		return fuse.EIO
	}
	if parentAttr.GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return fuse.Status(syscall.ENOTDIR)
	}
	gid := input.Gid
	if parentAttr.GetMode()&syscall.S_ISGID != 0 {
		gid = parentAttr.GetGid()
	}
	ino, inode, status := fs.tmpMgr.Create(input.NodeId, input.Uid, gid, input.Mode, input.Flags)
	if status != fuse.OK {
		return status
	}
	fh, status := fs.openHandle(ino, input.Flags, false)
	if status != fuse.OK {
		fs.tmpMgr.OnForget(ino, 1, fs.handles.IsOpen)
		return status
	}
	fs.caps.markAbsent(ino)
	fs.recordInodeSpecial(ino, inode)
	fs.FillEntryOut(&out.EntryOut, inode, 0, 0)
	out.Fh = fh
	if fs.cfg.DirectIO {
		out.OpenFlags = fuse.FOPEN_DIRECT_IO
	}
	return fuse.OK
}

func (fs *RawFS) tmpfileSync(opCtx context.Context, parentIno uint64, input *fuse.CreateIn, out *fuse.CreateOut) fuse.Status {
	epoch := fs.cache.version()
	detachedCtx := context.WithoutCancel(opCtx)
	ctx, cancelFunc := fs.RPCContextFrom(detachedCtx, nil)
	defer cancelFunc()
	resp, err := fs.client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		ParentIno: parentIno,
		Mode:      syscall.S_IFREG | (input.Mode & 0o7777),
		Uid:       input.Uid,
		Gid:       input.Gid,
		Exclusive: input.Flags&uint32(syscall.O_EXCL) != 0,
		DiskId:    fs.cfg.DiskID,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil || inode.GetIno() == 0 {
		return fuse.EIO
	}
	ino := fs.ToKernelIno(inode.GetIno())
	isDir := inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
	fh, status := fs.openHandle(ino, input.Flags, isDir)
	if status != fuse.OK {
		fs.markOrphanWithContext(opCtx, ino)
		return status
	}
	fs.markOrphanWithContext(opCtx, ino)
	fs.caps.markAbsent(ino)
	fs.recordInodeSpecial(ino, inode)
	fs.cache.putInodeAt(epoch, ino, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	fs.FillEntryOut(&out.EntryOut, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	out.Fh = fh
	if fs.cfg.DirectIO {
		out.OpenFlags = fuse.FOPEN_DIRECT_IO
	} else {
		out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	}
	return fuse.OK
}

// Forget 释放内核对具名或匿名节点的引用，并在无挂起任务时清理本地跟踪状态。
func (fs *RawFS) Forget(nodeID, nlookup uint64) {
	start := fs.metrics.beginOp(OpForget)
	defer fs.metrics.endOp(OpForget, start, fuse.OK, 0)

	if nodeID == VirtualStatsIno {
		return
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	serverIno, st := fs.ToServerIno(nodeID)
	fs.caps.forget(nodeID)
	fs.forgetInodeSpecial(nodeID)
	if fs.tmpMgr.OnForget(nodeID, nlookup, fs.handles.IsOpen) {
		fs.cache.invalidateInode(nodeID)
	}
	if st == fuse.OK && !fs.handles.IsOpen(nodeID) {
		fs.stage.RemoveIdleInode(serverIno)
	}
}

// linkTmp 将匿名文件通过 Mknod + StageCoordinator 后台接管具名化，已具名化节点不能再次通过此路径 Link。
func (fs *RawFS) linkTmp(parentCtx context.Context, cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (fuse.Status, bool) {
	if fs.tmpMgr.getFile(input.Oldnodeid) == nil {
		return fuse.OK, false
	}
	unlock := fs.inodeLocks.lock(input.Oldnodeid)
	defer unlock()
	ctx, done := fs.RPCContextFrom(parentCtx, cancel)
	defer done()
	epoch := fs.cache.version()
	mat := fs.newTmpMaterializer(epoch)
	res, handled, status := fs.tmpMgr.Link(
		ctx, input.Oldnodeid, input.NodeId, filename, fs.handles.IsOpen(input.Oldnodeid), mat, out,
	)
	if !handled {
		return fuse.OK, false
	}
	if status != fuse.OK {
		return status, true
	}
	fs.markDirty(res.ServerIno)
	if res.HasSecurityCap {
		fs.caps.markPresent(input.Oldnodeid)
	} else {
		fs.caps.markAbsent(input.Oldnodeid)
	}
	fs.recordInodeSpecial(input.Oldnodeid, res.Inode)
	fs.cache.invalidateDirListingTouchParentAt(epoch, input.NodeId, res.Inode, 0, res.AttrTimeout)
	fs.cache.putEntryAt(epoch, input.NodeId, filename, input.Oldnodeid)
	if res.Inode != nil && res.Inode.GetAttr() != nil {
		fs.cache.putInodeAt(epoch, input.Oldnodeid, res.Inode, res.EntryTimeout, res.AttrTimeout)
	} else {
		fs.cache.invalidateInode(input.Oldnodeid)
	}
	fs.topologySeq.Add(1)
	return fuse.OK, true
}
