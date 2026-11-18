package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// FUSE/Linux fallocate ABI 位标志（跨平台编译测试保持与 Linux uapi 一致）。
	fallocFlKeepSize  = 0x01
	fallocFlPunchHole = 0x02
)

// SetAttr 实现按需部分更新 Inode 属性或截断文件空间，对应 Linux FUSE FUSE_SETATTR 协议。
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
	if _, local, localSt := fs.tmpMgr.SetAttr(input.NodeId, input, func(fh uint64, needWrite bool) fuse.Status {
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
	hasUpdate := false
	valid := input.Valid

	if (valid & fuse.FATTR_MODE) != 0 {
		mode := input.Mode
		req.Mode = &mode
		hasUpdate = true
	}
	if (valid & fuse.FATTR_UID) != 0 {
		uid := input.Uid
		req.Uid = &uid
		hasUpdate = true
	}
	if (valid & fuse.FATTR_GID) != 0 {
		gid := input.Gid
		req.Gid = &gid
		hasUpdate = true
	}
	if (valid & fuse.FATTR_SIZE) != 0 {
		size := input.Size
		req.Size = &size
		hasUpdate = true
	}

	// 优先级保护：当 FATTR_ATIME_NOW 与 FATTR_ATIME 同时置位时，以 ATIME_NOW 优先，绝不设置 req.Atime，
	// 避免触发服务端互斥 CEL 规则校验失败。
	if (valid & fuse.FATTR_ATIME_NOW) != 0 {
		req.AtimeNow = true
		hasUpdate = true
	} else if (valid & fuse.FATTR_ATIME) != 0 {
		req.Atime = &timestamppb.Timestamp{
			Seconds: int64(input.Atime),
			Nanos:   int32(input.Atimensec),
		}
		hasUpdate = true
	}

	// 同理，当 FATTR_MTIME_NOW 与 FATTR_MTIME 同时置位时，以 MTIME_NOW 优先。
	if (valid & fuse.FATTR_MTIME_NOW) != 0 {
		req.MtimeNow = true
		hasUpdate = true
	} else if (valid & fuse.FATTR_MTIME) != 0 {
		req.Mtime = &timestamppb.Timestamp{
			Seconds: int64(input.Mtime),
			Nanos:   int32(input.Mtimensec),
		}
		hasUpdate = true
	}

	if (valid & fuse.FATTR_CTIME) != 0 {
		req.CtimeNow = true
		hasUpdate = true
	}

	// 当 FATTR_FH 置位时校验本地句柄；服务端请求不携带旧版句柄。
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

	fs.stage.ResetInodeStream(opCtx, ino)
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	fs.readAhead.InvalidateInode(ino)
	defer fs.readAhead.InvalidateInode(ino)

	resp, err := fs.client.SetAttr(ctx, connect.NewRequest(req))
	if err != nil {
		fs.cache.invalidateInode(input.NodeId)
		return fs.ToStatus(err, cancel)
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

// Fallocate 实现文件空间预分配与稀疏空洞打洞，对应 Linux FUSE FUSE_FALLOCATE 协议。
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

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	handle, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return status
	}
	if !handle.CanWrite() {
		return fuse.EBADF
	}

	// 模式位校验：仅支持默认预留 0、FALLOC_FL_KEEP_SIZE 与 FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE。
	// 其余不支持模式（如单独 PUNCH_HOLE、COLLAPSE_RANGE、ZERO_RANGE、INSERT_RANGE 等）按规范直接拦截返回 EOPNOTSUPP。
	if input.Mode != 0 &&
		input.Mode != fallocFlKeepSize &&
		input.Mode != (fallocFlPunchHole|fallocFlKeepSize) {
		return fuse.Status(syscall.EOPNOTSUPP)
	}

	if local, localSt := fs.tmpMgr.Allocate(input.NodeId, input.Offset, input.Length, input.Mode); local {
		if localSt == fuse.OK {
			fs.caps.markAbsent(input.NodeId)
		}
		return localSt
	}
	fs.stage.ResetInodeStream(opCtx, ino)
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
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
		return fs.ToStatus(err, cancel)
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
