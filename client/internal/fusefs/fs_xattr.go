package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func copyXattrValue(val, dest []byte) (uint32, fuse.Status) {
	valLen := uint32(len(val))
	if len(dest) == 0 {
		return valLen, fuse.OK
	}
	if len(dest) < len(val) {
		return valLen, fuse.Status(syscall.ERANGE)
	}
	copy(dest, val)
	return valLen, fuse.OK
}

func formatXattrList(names [][]byte, dest []byte) (uint32, fuse.Status) {
	var total int
	for _, name := range names {
		total += len(name) + 1
	}
	totalLen := uint32(total)
	if len(dest) == 0 {
		return totalLen, fuse.OK
	}
	if len(dest) < total {
		return totalLen, fuse.Status(syscall.ERANGE)
	}
	var offset int
	for _, name := range names {
		copy(dest[offset:], name)
		offset += len(name)
		dest[offset] = 0
		offset++
	}
	return totalLen, fuse.OK
}

// SetXAttr 实现设置扩展属性请求，透传属性名、属性值与创建/替换标志位。
// 在只读挂载模式下直接拦截返回 EROFS。
func (fs *RawFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) (status fuse.Status) {
	start := fs.metrics.beginOp(OpSetXAttr)
	opCtx, endSpan := fs.startFuseOp(OpSetXAttr)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpSetXAttr, start, status, 0)
	}()

	if status = fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.EACCES
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if local, localSt := fs.tmpMgr.SetXAttr(input.NodeId, attr, data, input.Flags); local {
		if localSt == fuse.OK && attr == securityCapabilityXattr {
			fs.caps.markPresent(input.NodeId)
		}
		return localSt
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	fs.stage.BarrierInode(opCtx, ino)
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	_, err := fs.client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:    ino,
		Name:   []byte(attr),
		Value:  data,
		Flags:  input.Flags,
		DiskId: fs.cfg.DiskID,
		Epoch:  fs.cfg.Epoch,
	}))
	fs.cache.invalidateInode(input.NodeId)
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()
	if attr == securityCapabilityXattr {
		fs.caps.markPresent(input.NodeId)
	}

	return fuse.OK
}

// GetXAttr 实现读取扩展属性请求，支持零长度探测与 ERANGE 缓冲区过小语义。
func (fs *RawFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (sz uint32, status fuse.Status) {
	start := fs.metrics.beginOp(OpGetXAttr)
	opCtx, endSpan := fs.startFuseOp(OpGetXAttr)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpGetXAttr, start, status, 0)
	}()

	if fs.isVirtualStatsIno(header.NodeId) {
		return 0, fuse.Status(syscall.ENODATA)
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	fenced := fs.IsFenced()
	if !fenced && attr == securityCapabilityXattr && fs.caps.knownAbsent(header.NodeId) {
		return 0, fuse.Status(syscall.ENODATA)
	}
	if localSz, local, localSt := fs.tmpMgr.GetXAttr(header.NodeId, attr, dest); local {
		if !fenced && attr == securityCapabilityXattr {
			switch localSt {
			case fuse.Status(syscall.ENODATA):
				fs.caps.markAbsent(header.NodeId)
			case fuse.OK, fuse.Status(syscall.ERANGE):
				fs.caps.markPresent(header.NodeId)
			}
		}
		return localSz, localSt
	}
	ino, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return 0, status
	}
	fs.stage.BarrierInode(opCtx, ino)

	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		Ino:    ino,
		Name:   []byte(attr),
		DiskId: fs.cfg.DiskID,
	}))
	if err != nil {
		status = fs.ToStatus(err)
		if !fenced && !fs.IsFenced() && attr == securityCapabilityXattr && status == fuse.Status(syscall.ENODATA) {
			fs.caps.markAbsent(header.NodeId)
		}
		return 0, status
	}
	if !fenced && !fs.IsFenced() && attr == securityCapabilityXattr {
		fs.caps.markPresent(header.NodeId)
	}

	return copyXattrValue(resp.Msg.GetValue(), dest)
}

// ListXAttr 实现列出扩展属性名称请求，组装以 '\x00' 结尾的连续字节序列。
func (fs *RawFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (sz uint32, status fuse.Status) {
	start := fs.metrics.beginOp(OpListXAttr)
	opCtx, endSpan := fs.startFuseOp(OpListXAttr)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpListXAttr, start, status, 0)
	}()

	if fs.isVirtualStatsIno(header.NodeId) {
		return 0, fuse.OK
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if localSz, local, localSt := fs.tmpMgr.ListXAttr(header.NodeId, dest); local {
		return localSz, localSt
	}
	ino, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return 0, status
	}
	fs.stage.BarrierInode(opCtx, ino)

	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{
		Ino:    ino,
		DiskId: fs.cfg.DiskID,
	}))
	if err != nil {
		return 0, fs.ToStatus(err)
	}

	return formatXattrList(resp.Msg.GetNames(), dest)
}

// RemoveXAttr 实现删除指定扩展属性请求。
// 在只读挂载模式下直接拦截返回 EROFS。
func (fs *RawFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) (status fuse.Status) {
	start := fs.metrics.beginOp(OpRemoveXAttr)
	opCtx, endSpan := fs.startFuseOp(OpRemoveXAttr)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpRemoveXAttr, start, status, 0)
	}()

	if status = fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if fs.isVirtualStatsIno(header.NodeId) {
		return fuse.EACCES
	}

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if local, localSt := fs.tmpMgr.RemoveXAttr(header.NodeId, attr); local {
		if localSt == fuse.OK && attr == securityCapabilityXattr {
			fs.caps.markAbsent(header.NodeId)
		}
		return localSt
	}

	ino, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return status
	}

	fs.stage.BarrierInode(opCtx, ino)
	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	_, err := fs.client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
		Ino:    ino,
		Name:   []byte(attr),
		DiskId: fs.cfg.DiskID,
		Epoch:  fs.cfg.Epoch,
	}))
	fs.cache.invalidateInode(header.NodeId)
	if err != nil {
		return fs.ToStatus(err)
	}
	fs.markMetaDirty()
	if attr == securityCapabilityXattr {
		fs.caps.markAbsent(header.NodeId)
	}

	return fuse.OK
}
