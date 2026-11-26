package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func (fs *RawFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpOpenDir)
	opCtx, endSpan := fs.startFuseOp(OpOpenDir)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpOpenDir, start, status, 0)
	}()

	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}
	if _, st := fs.ToServerIno(input.NodeId); st != fuse.OK {
		return st
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if !fs.cfg.IsReadOnly() {
		unlock := fs.inodeLocks.rlock(input.NodeId)
		defer unlock()
	}
	if !fs.IsFenced() {
		if info, ok := fs.lookupKnownInode(input.NodeId); ok {
			if !info.isDir {
				return fuse.Status(syscall.ENOTDIR)
			}
			fh, st := fs.openHandle(input.NodeId, input.Flags, true)
			if st == fuse.OK {
				out.Fh = fh
			}
			return st
		}
	}
	inode, _, status := fs.getAttrInodeWithContext(opCtx, cancel, input.NodeId)
	if status != fuse.OK {
		return status
	}
	if inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return fuse.Status(syscall.ENOTDIR)
	}
	fh, status := fs.openHandle(input.NodeId, input.Flags, true)
	if status == fuse.OK {
		out.Fh = fh
	}
	return status
}

func (fs *RawFS) readDirPage(parentCtx context.Context, cancel <-chan struct{}, input *fuse.ReadIn) ([]*storagev1.DirEntryPlus, fuse.Status) {
	if _, status := fs.checkedHandle(input.NodeId, input.Fh, true); status != fuse.OK {
		return nil, status
	}
	if _, removed := fs.handles.RemovedDir(input.NodeId); removed {
		return nil, fuse.OK
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return nil, status
	}
	ctx, done := fs.RPCContextFrom(parentCtx, cancel)
	defer done()
	resp, err := fs.client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
		Ino:    ino,
		Offset: input.Offset,
		Limit:  512,
		DiskId: fs.cfg.DiskID,
		Branch: fs.cfg.Branch,
	}))
	if err != nil {
		return nil, fs.ToStatus(err)
	}
	fenced := fs.IsFenced()
	prevOffset := input.Offset
	page := resp.Msg.GetEntries()
	for _, e := range page {
		if e.GetNextOffset() <= prevOffset {
			return nil, fuse.EIO
		}
		prevOffset = e.GetNextOffset()
		serverEntryIno := e.GetIno()
		if !fenced && e.GetInode() != nil {
			fs.stage.OverlayInode(serverEntryIno, e.GetInode())
		}
		e.Ino = fs.ToKernelIno(serverEntryIno)
	}
	return page, fuse.OK
}

func (fs *RawFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (status fuse.Status) {
	start := fs.metrics.beginOp(OpReadDir)
	opCtx, endSpan := fs.startFuseOp(OpReadDir)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpReadDir, start, status, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	entries, status := fs.readDirPage(opCtx, cancel, input)
	if status != fuse.OK {
		return status
	}
	for _, entry := range entries {
		if entry.GetNextOffset() <= input.Offset {
			continue
		}
		mode := uint32(syscall.S_IFDIR)
		if entry.GetInode() != nil && entry.GetInode().GetAttr() != nil {
			mode = entry.GetInode().GetAttr().GetMode()
		}
		if !out.AddDirEntry(fuse.DirEntry{Ino: entry.GetIno(), Name: string(entry.GetName()), Mode: mode, Off: entry.GetNextOffset()}) {
			break
		}
	}
	return fuse.OK
}

func (fs *RawFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (status fuse.Status) {
	start := fs.metrics.beginOp(OpReadDirPlus)
	opCtx, endSpan := fs.startFuseOp(OpReadDirPlus)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpReadDirPlus, start, status, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	entries, status := fs.readDirPage(opCtx, cancel, input)
	if status != fuse.OK {
		return status
	}
	fenced := fs.IsFenced()
	for _, entry := range entries {
		if entry.GetNextOffset() <= input.Offset {
			continue
		}
		mode := uint32(syscall.S_IFDIR)
		if entry.GetInode() != nil && entry.GetInode().GetAttr() != nil {
			mode = entry.GetInode().GetAttr().GetMode()
		}
		name := string(entry.GetName())
		eo := out.AddDirLookupEntry(fuse.DirEntry{Ino: entry.GetIno(), Name: name, Mode: mode, Off: entry.GetNextOffset()})
		if eo == nil {
			break
		}
		if name == "." || name == ".." {
			*eo = fuse.EntryOut{}
		} else {
			entryTTL := entry.GetEntryTimeoutSeconds()
			attrTTL := entry.GetAttrTimeoutSeconds()
			if fenced {
				entryTTL = 0
				attrTTL = 0
			} else if entry.GetInode() != nil {
				fs.stage.ObserveServerInode(entry.GetInode().GetIno(), entry.GetInode(), false)
			}
			fs.FillEntryOut(eo, entry.GetInode(), entryTTL, attrTTL)
			if entry.GetInode() != nil {
				fs.stage.OverlayAttr(entry.GetInode().GetIno(), &eo.Attr)
			}
			eo.NodeId = fs.tmpMgr.ObserveKernelLookup(entry.GetIno())
			eo.Ino = eo.NodeId
			if !fenced {
				fs.recordKnownInode(eo.NodeId, entry.GetInode())
				fs.dentries.put(input.NodeId, name, eo.NodeId)
			}
		}
	}
	return fuse.OK
}

func (fs *RawFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (status fuse.Status) {
	start := fs.metrics.beginOp(OpFsyncDir)
	opCtx, endSpan := fs.startFuseOp(OpFsyncDir)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpFsyncDir, start, status, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if _, st := fs.checkedHandle(input.NodeId, input.Fh, true); st != fuse.OK {
		return st
	}
	if _, removed := fs.handles.RemovedDir(input.NodeId); removed {
		return fuse.OK
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}
	if st := fs.FenceStatus(); st != fuse.OK {
		return st
	}
	seq := fs.metaDirtySeq.Load()
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()
	_, err := fs.client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
		Ino:      ino,
		Fh:       0,
		Datasync: input.FsyncFlags&1 != 0,
		DiskId:   fs.cfg.DiskID,
		Branch:   fs.cfg.Branch,
		Epoch:    fs.cfg.Epoch,
	}))
	if err == nil || connect.CodeOf(err) == connect.CodeUnimplemented {
		fs.advanceMetaSyncedSeq(seq)
	}
	return fs.ToStatus(err)
}

func (fs *RawFS) ReleaseDir(input *fuse.ReleaseIn) {
	start := fs.metrics.beginOp(OpReleaseDir)
	_, endSpan := fs.startFuseOp(OpReleaseDir)
	defer func() {
		endSpan(fuse.OK)
		fs.metrics.endOp(OpReleaseDir, start, fuse.OK, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	h, ok := fs.handles.Get(input.Fh)
	if !ok || !h.IsDir || h.Ino != input.NodeId {
		return
	}
	if !fs.cfg.IsReadOnly() {
		unlock := fs.inodeLocks.lock(input.NodeId)
		defer unlock()
		h, ok = fs.handles.Get(input.Fh)
		if !ok || !h.IsDir || h.Ino != input.NodeId {
			return
		}
	}
	fs.handles.Release(input.Fh)
}
