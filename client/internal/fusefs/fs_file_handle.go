package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func ToProtoOpenFlags(sysFlags uint32) uint32 {
	var protoFlags uint32

	switch sysFlags & uint32(syscall.O_ACCMODE) {
	case uint32(syscall.O_RDONLY):
		protoFlags |= 0o0
	case uint32(syscall.O_WRONLY):
		protoFlags |= 0o1
	case uint32(syscall.O_RDWR):
		protoFlags |= 0o2
	}

	if (sysFlags & uint32(syscall.O_CREAT)) != 0 {
		protoFlags |= 0o100
	}
	if (sysFlags & uint32(syscall.O_EXCL)) != 0 {
		protoFlags |= 0o200
	}
	if (sysFlags & uint32(syscall.O_TRUNC)) != 0 {
		protoFlags |= 0o1000
	}
	if (sysFlags & uint32(syscall.O_APPEND)) != 0 {
		protoFlags |= 0o2000
	}

	return protoFlags
}

func (fs *RawFS) Create(_ <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpCreate)
	opCtx, endSpan := fs.startFuseOp(OpCreate)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpCreate, start, status, 0)
	}()

	if status = fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if fs.isVirtualStatsName(input.NodeId, name) {
		return fuse.EACCES
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}
	if input.Flags&uint32(syscall.O_ACCMODE) == syscall.O_ACCMODE {
		return fuse.EINVAL
	}
	parentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()
	resp, err := fs.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      input.Mode & 0xffff,
		Uid:       input.Uid,
		Gid:       input.Gid,
		Flags:     ToProtoOpenFlags(input.Flags),
		DiskId:    fs.cfg.DiskID,
		Branch:    fs.cfg.Branch,
		Epoch:     fs.cfg.Epoch,
	}))
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return fuse.EIO
	}
	ino := fs.ToKernelIno(inode.GetIno())
	fh, status := fs.openHandle(ino, input.Flags, false)
	if status != fuse.OK {
		return status
	}
	ino = fs.tmpMgr.ObserveKernelLookup(inode.GetIno())
	fs.dentries.put(input.NodeId, name, ino)
	fs.caps.markAbsent(ino)
	fs.recordKnownInode(ino, inode)
	if inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFREG {
		fs.stage.SeedCreatedInode(inode.GetIno())
	}
	fs.FillEntryOut(&out.EntryOut, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	out.Fh = fh
	out.OpenFlags = resp.Msg.GetOpenFlags()
	switch {
	case fs.cfg.DirectIO:
		out.OpenFlags &^= fuse.FOPEN_KEEP_CACHE
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	case input.Flags&uint32(syscall.O_TRUNC) != 0:
		out.OpenFlags &^= fuse.FOPEN_KEEP_CACHE
	default:
		out.OpenFlags |= fuse.FOPEN_KEEP_CACHE
	}
	return fuse.OK
}

func (fs *RawFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpOpen)
	opCtx, endSpan := fs.startFuseOp(OpOpen)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpOpen, start, status, 0)
	}()

	if fs.isVirtualStatsIno(input.NodeId) {
		fs.lifecycleMu.RLock()
		defer fs.lifecycleMu.RUnlock()
		return fs.openVirtualStats(input, out)
	}
	mode := input.Flags & uint32(syscall.O_ACCMODE)
	if mode == syscall.O_ACCMODE {
		return fuse.EINVAL
	}
	if mode != syscall.O_RDONLY || input.Flags&syscall.O_TRUNC != 0 {
		if st := fs.CheckWritable(); st != fuse.OK {
			return st
		}
	}
	if _, st := fs.ToServerIno(input.NodeId); st != fuse.OK {
		return st
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if !fs.cfg.IsReadOnly() {
		if input.Flags&syscall.O_TRUNC != 0 {
			unlock := fs.inodeLocks.lock(input.NodeId)
			defer unlock()
		} else {
			unlock := fs.inodeLocks.rlock(input.NodeId)
			defer unlock()
		}
	}

	if input.Flags&syscall.O_TRUNC != 0 {
		if !fs.IsFenced() {
			if info, ok := fs.lookupKnownInode(input.NodeId); ok && info.isDir {
				return fuse.Status(syscall.EISDIR)
			}
		}
		set := &fuse.SetAttrIn{}
		set.NodeId = input.NodeId
		set.Valid = fuse.FATTR_SIZE
		if updatedInode, local, truncStatus := fs.tmpMgr.SetAttr(input.NodeId, set, nil, &fuse.Attr{}); local {
			if truncStatus != fuse.OK {
				return truncStatus
			}
			fs.recordKnownInode(input.NodeId, updatedInode)
			fs.caps.markAbsent(input.NodeId)
		} else {
			ino, st := fs.ToServerIno(input.NodeId)
			if st != fuse.OK {
				return st
			}
			barrierCtx, barrierCancel := fs.stageBarrierContext(opCtx, cancel)
			if err := fs.stage.BarrierInode(barrierCtx, ino); err != nil {
				barrierCancel()
				return fs.stageErrToStatus(err)
			}
			barrierCancel()
			ctx, done := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
			zero := uint64(0)
			resp, err := fs.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
				Ino:    ino,
				Size:   &zero,
				DiskId: fs.cfg.DiskID,
				Branch: fs.cfg.Branch,
				Epoch:  fs.cfg.Epoch,
			}))
			done()
			fs.readAhead.InvalidateInode(ino)
			if err != nil {
				return fs.ToStatus(err)
			}
			fs.markMetaDirty()
			fs.stage.ResetTruncatedInode(ino)
			fs.recordKnownInode(input.NodeId, resp.Msg.GetInode())
			fs.caps.markAbsent(input.NodeId)
		}
	} else {
		fenced := fs.IsFenced()
		openedFromKnown := false
		if !fenced {
			if info, ok := fs.lookupKnownInode(input.NodeId); ok {
				if info.isDir {
					return fuse.Status(syscall.EISDIR)
				}
				openedFromKnown = true
			}
		}
		if !openedFromKnown {
			inode, _, st := fs.getAttrInodeWithContext(opCtx, cancel, input.NodeId)
			if st != fuse.OK {
				return st
			}
			if inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR {
				return fuse.Status(syscall.EISDIR)
			}
		}
	}

	fh, status := fs.openHandle(input.NodeId, input.Flags, false)
	if status != fuse.OK {
		return status
	}
	out.Fh = fh
	if fs.cfg.DirectIO {
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	} else if input.Flags&uint32(syscall.O_TRUNC) == 0 && !fs.IsFenced() {
		out.OpenFlags |= fuse.FOPEN_KEEP_CACHE
	}
	return fuse.OK
}

func (fs *RawFS) Flush(_ <-chan struct{}, input *fuse.FlushIn) (status fuse.Status) {
	start := fs.metrics.beginOp(OpFlush)
	_, endSpan := fs.startFuseOp(OpFlush)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpFlush, start, status, 0)
	}()

	if fs.isVirtualStatsIno(input.NodeId) {
		fs.lifecycleMu.RLock()
		defer fs.lifecycleMu.RUnlock()
		if _, st := fs.checkedHandle(VirtualStatsIno, input.Fh, false); st != fuse.OK {
			return st
		}
		return fuse.OK
	}
	if _, st := fs.ToServerIno(input.NodeId); st != fuse.OK {
		return st
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	handle, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return status
	}
	if fs.tmpMgr.IsUnpromoted(input.NodeId) {
		return fuse.OK
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}
	if !handle.CanWrite() {
		return fuse.OK
	}
	if err := fs.stage.FlushHandle(input.Fh, ino); err != nil {
		return fs.stageErrToStatus(err)
	}
	return fuse.OK
}

func (fs *RawFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) (status fuse.Status) {
	start := fs.metrics.beginOp(OpFsync)
	opCtx, endSpan := fs.startFuseOp(OpFsync)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpFsync, start, status, 0)
	}()

	if fs.isVirtualStatsIno(input.NodeId) {
		fs.lifecycleMu.RLock()
		defer fs.lifecycleMu.RUnlock()
		_, status = fs.checkedHandle(VirtualStatsIno, input.Fh, false)
		return status
	}
	if _, st := fs.ToServerIno(input.NodeId); st != fuse.OK {
		return st
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if _, st := fs.checkedHandle(input.NodeId, input.Fh, false); st != fuse.OK {
		return st
	}
	if local, st := fs.tmpMgr.Fsync(input.NodeId); local {
		return st
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}
	barrierCtx, barrierCancel := fs.stageBarrierContext(opCtx, cancel)
	defer barrierCancel()
	if err := fs.stage.FlushInodeForHandle(barrierCtx, input.Fh, ino); err != nil {
		return fs.stageErrToStatus(err)
	}
	if st := fs.FenceStatus(); st != fuse.OK {
		return st
	}
	seq := fs.metaDirtySeq.Load()
	ctx, cancelFunc := fs.RPCContextFrom(barrierCtx, nil)
	defer cancelFunc()
	_, err := fs.client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
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
	if err == nil {
		fs.metrics.incDirtyFlushOK()
	} else {
		fs.metrics.incDirtyFlushError()
	}
	return fs.ToStatus(err)
}

func (fs *RawFS) Release(_ <-chan struct{}, input *fuse.ReleaseIn) {
	start := fs.metrics.beginOp(OpRelease)
	opCtx, endSpan := fs.startFuseOp(OpRelease)
	defer func() {
		endSpan(fuse.OK)
		fs.metrics.endOp(OpRelease, start, fuse.OK, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	h, ok := fs.handles.Get(input.Fh)
	if !ok || h.Ino != input.NodeId || h.IsDir {
		return
	}
	if input.NodeId == VirtualStatsIno {
		fs.handles.Release(input.Fh)
		fs.stateMu.Lock()
		delete(fs.statsSnapshots, input.Fh)
		fs.stateMu.Unlock()
		return
	}
	serverIno, serverStatus := fs.ToServerIno(input.NodeId)

	unlock := fs.inodeLocks.lock(h.Ino)
	defer unlock()
	if _, exists := fs.handles.Get(input.Fh); !exists {
		return
	}
	fs.readAhead.RemoveHandle(input.Fh)
	fs.stage.ForgetHandle(input.Fh)
	orphan := fs.handles.IsOrphan(input.NodeId)
	lastHandle := fs.handles.OpenCount(input.NodeId) == 1
	isRealServerIno := serverStatus == fuse.OK && !fs.tmpMgr.IsUnpromoted(input.NodeId)
	if orphan && lastHandle {
		if isRealServerIno {
			fs.stage.DiscardInode(serverIno)
			fs.readAhead.InvalidateInode(serverIno)
		}
		fs.forgetKnownInode(input.NodeId)
		fs.caps.forget(input.NodeId)
		fs.handles.ReleaseContext(opCtx, input.Fh)
	} else {
		if isRealServerIno {
			fs.stage.SealInode(serverIno)
		}
		fs.handles.Release(input.Fh)
	}
	if fs.tmpMgr.OnHandleReleased(input.NodeId, orphan, fs.handles.IsOpen) {
		fs.forgetKnownInode(input.NodeId)
	}
}
