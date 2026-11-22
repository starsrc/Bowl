package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	fallocFlKeepSize  = 0x01
	fallocFlPunchHole = 0x02

	setAttrUpdateMask = fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_SIZE |
		fuse.FATTR_ATIME | fuse.FATTR_ATIME_NOW | fuse.FATTR_MTIME | fuse.FATTR_MTIME_NOW | fuse.FATTR_CTIME
)

func (fs *RawFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpSetAttr)
	opCtx, endSpan := fs.startFuseOp(OpSetAttr)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpSetAttr, start, status, 0)
	}()

	if status = fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.EACCES
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	epoch := fs.cache.version()
	valid := input.Valid
	hasUpdate := valid&setAttrUpdateMask != 0
	if hasUpdate {
		unlock := fs.inodeLocks.lock(input.NodeId)
		defer unlock()
	}
	if updatedInode, local, localSt := fs.tmpMgr.SetAttr(input.NodeId, input, func(fh uint64, needWrite bool) fuse.Status {
		h, st := fs.checkedHandle(input.NodeId, fh, false)
		if st != fuse.OK {
			return st
		}
		if needWrite && !h.CanWrite() {
			return fuse.EBADF
		}
		return fuse.OK
	}, &out.Attr); local {
		out.SetTimeout(0)
		if localSt == fuse.OK {
			fs.recordInodeSpecial(input.NodeId, updatedInode)
			if valid&(fuse.FATTR_SIZE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_MODE) != 0 {
				fs.caps.markAbsent(input.NodeId)
			}
		}
		return localSt
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	req := &storagev1.SetAttrRequest{
		Ino:    ino,
		DiskId: fs.cfg.DiskID,
		Epoch:  fs.cfg.Epoch,
	}

	if (valid & fuse.FATTR_MODE) != 0 {
		mode := input.Mode
		req.Mode = &mode
	}
	if (valid & fuse.FATTR_UID) != 0 {
		uid := input.Uid
		req.Uid = &uid
	}
	if (valid & fuse.FATTR_GID) != 0 {
		gid := input.Gid
		req.Gid = &gid
	}
	if (valid & fuse.FATTR_SIZE) != 0 {
		size := input.Size
		req.Size = &size
	}

	if (valid & fuse.FATTR_ATIME_NOW) != 0 {
		req.AtimeNow = true
	} else if (valid & fuse.FATTR_ATIME) != 0 {
		req.Atime = &timestamppb.Timestamp{
			Seconds: int64(input.Atime),
			Nanos:   int32(input.Atimensec),
		}
	}

	if (valid & fuse.FATTR_MTIME_NOW) != 0 {
		req.MtimeNow = true
	} else if (valid & fuse.FATTR_MTIME) != 0 {
		req.Mtime = &timestamppb.Timestamp{
			Seconds: int64(input.Mtime),
			Nanos:   int32(input.Mtimensec),
		}
	}

	if (valid & fuse.FATTR_CTIME) != 0 {
		req.CtimeNow = true
	}

	if (valid & fuse.FATTR_FH) != 0 {
		h, st := fs.checkedHandle(input.NodeId, input.Fh, false)
		if st != fuse.OK {
			return st
		}
		if valid&fuse.FATTR_SIZE != 0 && !h.CanWrite() {
			return fuse.EBADF
		}
	}

	if !hasUpdate {
		cached, st := fs.getInodeWithContext(opCtx, cancel, input.NodeId)
		if st == fuse.OK {
			fs.FillAttrOut(out, cached.inode, cached.attrTimeout)
			fs.stage.OverlayAttr(ino, &out.Attr)
		}
		return st
	}

	barrierCtx, barrierCancel := fs.stageBarrierContext(opCtx, cancel)
	resetErr := fs.stage.ResetInodeStream(barrierCtx, ino)
	barrierCancel()
	if resetErr != nil {
		return fs.stageErrToStatus(resetErr)
	}
	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()

	fs.readAhead.InvalidateInode(ino)
	defer fs.readAhead.InvalidateInode(ino)

	resp, err := fs.client.SetAttr(ctx, connect.NewRequest(req))
	if err != nil {
		fs.cache.invalidateInode(input.NodeId)
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()
	if valid&fuse.FATTR_SIZE != 0 && isRegularOrUnknownInode(resp.Msg.GetInode()) {
		fs.markDirty(ino)
	}

	fs.recordInodeSpecial(input.NodeId, resp.Msg.GetInode())
	fs.stage.ObserveServerInode(ino, resp.Msg.GetInode(), valid&fuse.FATTR_SIZE != 0 && input.Size == 0)
	fs.cache.refreshInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), resp.Msg.GetAttrTimeoutSeconds())
	if valid&(fuse.FATTR_SIZE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_MODE) != 0 {
		fs.caps.markAbsent(input.NodeId)
	}
	fs.FillAttrOut(out, resp.Msg.GetInode(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

func (fs *RawFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) (status fuse.Status) {
	start := fs.metrics.beginOp(OpFallocate)
	opCtx, endSpan := fs.startFuseOp(OpFallocate)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpFallocate, start, status, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if status = fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.EACCES
	}

	if _, st := fs.ToServerIno(input.NodeId); st != fuse.OK {
		return st
	}

	handle, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return status
	}
	if !handle.CanWrite() {
		return fuse.EBADF
	}

	if input.Mode != 0 &&
		input.Mode != fallocFlKeepSize &&
		input.Mode != (fallocFlPunchHole|fallocFlKeepSize) {
		return fuse.Status(syscall.EOPNOTSUPP)
	}

	unlock := fs.inodeLocks.lock(input.NodeId)
	defer unlock()

	if updatedInode, local, localSt := fs.tmpMgr.Allocate(input.NodeId, input.Offset, input.Length, input.Mode); local {
		if localSt == fuse.OK {
			fs.recordInodeSpecial(input.NodeId, updatedInode)
			fs.caps.markAbsent(input.NodeId)
		}
		return localSt
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}
	barrierCtx, barrierCancel := fs.stageBarrierContext(opCtx, cancel)
	resetErr := fs.stage.ResetInodeStream(barrierCtx, ino)
	barrierCancel()
	if resetErr != nil {
		return fs.stageErrToStatus(resetErr)
	}
	ctx, cancelFunc := fs.RPCContextFrom(context.WithoutCancel(opCtx), nil)
	defer cancelFunc()

	req := &storagev1.FallocateRequest{
		Ino:    ino,
		Fh:     0,
		Offset: input.Offset,
		Length: input.Length,
		Mode:   input.Mode,
		DiskId: fs.cfg.DiskID,
		Epoch:  fs.cfg.Epoch,
	}

	fs.readAhead.InvalidateInode(ino)
	defer fs.readAhead.InvalidateInode(ino)

	_, err := fs.client.Fallocate(ctx, connect.NewRequest(req))
	fs.cache.invalidateInode(input.NodeId)
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()
	fs.markDirty(ino)
	fs.stage.InvalidateKnownSize(ino)
	fs.caps.markAbsent(input.NodeId)

	return fuse.OK
}

func isRegularOrUnknownInode(inode *storagev1.Inode) bool {
	if inode == nil || inode.GetAttr() == nil {
		return true
	}
	return inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFREG
}
