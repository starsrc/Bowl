package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/starsrc/bowl/client/internal/rpc"
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
	cached, status := fs.getInodeWithContext(opCtx, cancel, input.NodeId)
	if status != fuse.OK {
		return status
	}
	if cached.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return fuse.Status(syscall.ENOTDIR)
	}
	fh, status := fs.openHandle(input.NodeId, input.Flags, true)
	if status == fuse.OK {
		out.Fh = fh
	}
	return status
}

func (fs *RawFS) readDirEntries(parentCtx context.Context, cancel <-chan struct{}, input *fuse.ReadIn) (cachedDir, fuse.Status) {
	if _, status := fs.checkedHandle(input.NodeId, input.Fh, true); status != fuse.OK {
		return cachedDir{}, status
	}
	if !fs.IsFenced() {
		if cached, ok := fs.cache.dir(input.NodeId); ok {
			return cached, fuse.OK
		}
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return cachedDir{}, status
	}
	epoch, generation := fs.cache.snapshot()
	ctx, done := fs.RPCContextFrom(parentCtx, cancel)
	defer done()
	offset := input.Offset
	var entries []*storagev1.DirEntryPlus
	firstPage := true
	for {
		reqCtx := ctx
		if !firstPage {
			reqCtx = rpc.WithRequestID(ctx, fs.NextRequestID())
		}
		firstPage = false
		resp, err := fs.client.ReadDirPlus(reqCtx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
			Ino:    ino,
			Offset: offset,
			Limit:  512,
			DiskId: fs.cfg.DiskID,
		}))
		fenced := fs.IsFenced()
		if !fenced && fs.cache.version() != epoch {
			return cachedDir{}, fuse.Status(syscall.ESTALE)
		}
		if err != nil {
			return cachedDir{}, fs.ToStatus(err)
		}
		page := resp.Msg.GetEntries()
		for _, e := range page {
			if e.GetNextOffset() <= offset {
				return cachedDir{}, fuse.EIO
			}
			offset = e.GetNextOffset()
			serverEntryIno := e.GetIno()
			if !fenced && e.GetInode() != nil {
				if fs.cfg.IsReadOnly() {
					fs.readAhead.ObserveInode(serverEntryIno, e.GetInode())
				}
				fs.stage.OverlayInode(serverEntryIno, e.GetInode())
			}
			e.Ino = fs.ToKernelIno(serverEntryIno)
		}
		if !fenced && fs.cfg.IsReadOnly() {
			fs.cache.refreshExistingInodesFromReadDirAt(epoch, page)
		}
		entries = append(entries, page...)
		if input.Offset != 0 || len(entries)+1 > fs.cache.capacity {
			return cachedDir{entries: entries}, fuse.OK
		}
		if len(page) < 512 {
			if !fenced {
				fs.cache.putDirAtIf(generation, epoch, input.NodeId, entries)
			}
			return cachedDir{entries: entries}, fuse.OK
		}
	}
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
	res, status := fs.readDirEntries(opCtx, cancel, input)
	if status != fuse.OK {
		return status
	}
	for _, entry := range res.entries {
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
	res, status := fs.readDirEntries(opCtx, cancel, input)
	if status != fuse.OK {
		return status
	}
	for i, entry := range res.entries {
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
			if fs.IsFenced() {
				entryTTL = 0
				attrTTL = 0
			} else if res.fromCache && fs.cfg.IsReadOnly() {
				entryTTL = min(entryTTL, remainingSeconds(res.entryExpiryAt(i), res.nowNano))
				attrTTL = min(attrTTL, remainingSeconds(res.attrExpiryAt(i), res.nowNano))
			}
			fs.FillEntryOut(eo, entry.GetInode(), entryTTL, attrTTL)
			if entry.GetInode() != nil {
				fs.stage.OverlayAttr(entry.GetInode().GetIno(), &eo.Attr)
			}
			eo.NodeId = fs.tmpMgr.ObserveKernelLookup(entry.GetIno())
			eo.Ino = eo.NodeId
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
	fs.handles.Release(input.Fh)
}
