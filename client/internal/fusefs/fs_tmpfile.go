package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type rawFSTmpMaterializer struct {
	fs *RawFS
}

func (fs *RawFS) newTmpMaterializer() *rawFSTmpMaterializer {
	return &rawFSTmpMaterializer{fs: fs}
}

func (m *rawFSTmpMaterializer) CreateTmpInode(ctx context.Context, parentIno uint64, name string, mode, uid, gid uint32) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status) {
	if st := m.fs.FenceStatus(); st != fuse.OK {
		return nil, 0, 0, st
	}
	if m.fs.client == nil {
		return nil, 0, 0, fuse.EIO
	}
	reqCtx, cancel := m.fs.RPCContextFrom(ctx, nil)
	defer cancel()
	resp, err := m.fs.client.Mknod(reqCtx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      mode,
		Uid:       uid,
		Gid:       gid,
		DiskId:    m.fs.cfg.DiskID,
		Branch:    m.fs.cfg.Branch,
		Epoch:     m.fs.cfg.Epoch,
	}))
	if err != nil {
		return nil, 0, 0, m.fs.ToStatus(err)
	}
	m.fs.markMetaDirty()
	return resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

func (m *rawFSTmpMaterializer) WriteTmpExtent(ctx context.Context, serverIno, offset uint64, data []byte) fuse.Status {
	if st := m.fs.FenceStatus(); st != fuse.OK {
		return st
	}
	if m.fs.client == nil {
		return fuse.EIO
	}
	reqCtx, cancel := m.fs.RPCContextFrom(ctx, nil)
	defer cancel()
	resp, err := m.fs.client.Write(reqCtx, connect.NewRequest(&storagev1.WriteRequest{
		Ino:    serverIno,
		Fh:     0,
		Offset: offset,
		Data:   data,
		DiskId: m.fs.cfg.DiskID,
		Branch: m.fs.cfg.Branch,
		Epoch:  m.fs.cfg.Epoch,
	}))
	if err != nil {
		return m.fs.ToStatus(err)
	}
	m.fs.markMetaDirty()
	if resp.Msg.GetSize() < uint32(len(data)) {
		return fuse.EIO
	}
	return fuse.OK
}

func (m *rawFSTmpMaterializer) CommitTmpMeta(ctx context.Context, serverIno uint64, meta TmpPromotedMeta) (*storagev1.Inode, fuse.Status) {
	if st := m.fs.FenceStatus(); st != fuse.OK {
		return nil, st
	}
	if m.fs.client == nil {
		return nil, fuse.EIO
	}
	size := meta.Size
	mode := meta.Mode
	gid := meta.Gid
	reqCtx, cancel := m.fs.RPCContextFrom(ctx, nil)
	resp, err := m.fs.client.SetAttr(reqCtx, connect.NewRequest(&storagev1.SetAttrRequest{
		Ino:    serverIno,
		Size:   &size,
		Atime:  meta.Atime,
		Mtime:  meta.Mtime,
		Mode:   &mode,
		Gid:    &gid,
		DiskId: m.fs.cfg.DiskID,
		Branch: m.fs.cfg.Branch,
		Epoch:  m.fs.cfg.Epoch,
	}))
	cancel()
	if err != nil {
		return nil, m.fs.ToStatus(err)
	}
	m.fs.markMetaDirty()
	finalInode := resp.Msg.GetInode()

	for _, xa := range meta.Xattrs {
		if st := m.fs.FenceStatus(); st != fuse.OK {
			return nil, st
		}
		xReqCtx, xCancel := m.fs.RPCContextFrom(ctx, nil)
		_, xErr := m.fs.client.SetXattr(xReqCtx, connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:    serverIno,
			Name:   []byte(xa.Name),
			Value:  xa.Value,
			Flags:  0,
			DiskId: m.fs.cfg.DiskID,
			Branch: m.fs.cfg.Branch,
			Epoch:  m.fs.cfg.Epoch,
		}))
		xCancel()
		if xErr != nil {
			return nil, m.fs.ToStatus(xErr)
		}
		m.fs.markMetaDirty()
	}

	if meta.Flags != 0 {
		if st := m.fs.FenceStatus(); st != fuse.OK {
			return nil, st
		}
		flags := meta.Flags
		fReqCtx, fCancel := m.fs.RPCContextFrom(ctx, nil)
		fResp, fErr := m.fs.client.SetAttr(fReqCtx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:    serverIno,
			Flags:  &flags,
			DiskId: m.fs.cfg.DiskID,
			Branch: m.fs.cfg.Branch,
			Epoch:  m.fs.cfg.Epoch,
		}))
		fCancel()
		if fErr != nil {
			return nil, m.fs.ToStatus(fErr)
		}
		m.fs.markMetaDirty()
		if fResp.Msg.GetInode() != nil {
			finalInode = fResp.Msg.GetInode()
		}
	}
	return finalInode, fuse.OK
}

func (m *rawFSTmpMaterializer) RollbackTmpInode(ctx context.Context, parentIno uint64, name string) {
	if m.fs.client == nil || m.fs.IsFenced() {
		return
	}
	rbCtx, cancel := m.fs.RPCContextFrom(context.WithoutCancel(ctx), nil)
	defer cancel()
	open := false
	_, err := m.fs.client.Unlink(rbCtx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno:    parentIno,
		Name:         []byte(name),
		OpenInClient: &open,
		DiskId:       m.fs.cfg.DiskID,
		Branch:       m.fs.cfg.Branch,
		Epoch:        m.fs.cfg.Epoch,
	}))
	if err != nil {
		m.fs.fenceIfStaleEpoch(err)
		return
	}
	m.fs.markMetaDirty()
}

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
	parent, _, st := fs.getAttrInodeWithContext(opCtx, cancel, input.NodeId)
	if st != fuse.OK {
		return st
	}
	parentAttr := parent.GetAttr()
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
	fs.recordKnownInodeAuthoritative(ino, inode)
	fs.FillEntryOut(&out.EntryOut, inode, 0, 0)
	out.Fh = fh
	if fs.cfg.DirectIO {
		out.OpenFlags = fuse.FOPEN_DIRECT_IO
	}
	return fuse.OK
}

func (fs *RawFS) tmpfileSync(opCtx context.Context, parentIno uint64, input *fuse.CreateIn, out *fuse.CreateOut) fuse.Status {
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
		Branch:    fs.cfg.Branch,
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
	fs.recordKnownInodeAuthoritative(ino, inode)
	fs.FillEntryOut(&out.EntryOut, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	out.Fh = fh
	if fs.cfg.DirectIO {
		out.OpenFlags = fuse.FOPEN_DIRECT_IO
	} else {
		out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	}
	return fuse.OK
}

func (fs *RawFS) Forget(nodeID, nlookup uint64) {
	start := fs.metrics.beginOp(OpForget)
	defer fs.metrics.endOp(OpForget, start, fuse.OK, 0)

	if nodeID == VirtualStatsIno {
		return
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	fs.caps.forget(nodeID)
	fs.forgetKnownInode(nodeID)
	fs.dentries.forgetChild(nodeID)
	fs.tmpMgr.OnForget(nodeID, nlookup, fs.handles.IsOpen)
}

func (fs *RawFS) linkTmp(parentCtx context.Context, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (fuse.Status, bool) {
	if fs.tmpMgr.getFile(input.Oldnodeid) == nil {
		return fuse.OK, false
	}
	unlock := fs.inodeLocks.lock(input.Oldnodeid)
	defer unlock()
	detachedCtx := context.WithoutCancel(parentCtx)
	mat := fs.newTmpMaterializer()
	res, handled, status := fs.tmpMgr.Link(
		detachedCtx, input.Oldnodeid, input.NodeId, filename, fs.handles.IsOpen(input.Oldnodeid), mat, out,
	)
	if !handled {
		return fuse.OK, false
	}
	if status != fuse.OK {
		return status, true
	}
	fs.readAhead.InvalidateInode(res.ServerIno)
	fs.markMetaDirty()
	if res.HasSecurityCap {
		fs.caps.markPresent(input.Oldnodeid)
	} else {
		fs.caps.markAbsent(input.Oldnodeid)
	}
	fs.recordKnownInodeAuthoritative(input.Oldnodeid, res.Inode)
	fs.dentries.put(input.NodeId, filename, input.Oldnodeid)
	fs.topologySeq.Add(1)
	return fuse.OK, true
}
