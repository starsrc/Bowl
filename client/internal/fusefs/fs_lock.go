package fusefs

import (
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	ProtoLockTypeRead   uint32 = 0
	ProtoLockTypeWrite  uint32 = 1
	ProtoLockTypeUnlock uint32 = 2
)

func ToProtoLockType(hostType uint32) (uint32, fuse.Status) {
	switch hostType {
	case uint32(syscall.F_RDLCK):
		return ProtoLockTypeRead, fuse.OK
	case uint32(syscall.F_WRLCK):
		return ProtoLockTypeWrite, fuse.OK
	case uint32(syscall.F_UNLCK):
		return ProtoLockTypeUnlock, fuse.OK
	default:
		return 0, fuse.EINVAL
	}
}

func ToHostLockType(protoType uint32) (uint32, fuse.Status) {
	switch protoType {
	case ProtoLockTypeRead:
		return uint32(syscall.F_RDLCK), fuse.OK
	case ProtoLockTypeWrite:
		return uint32(syscall.F_WRLCK), fuse.OK
	case ProtoLockTypeUnlock:
		return uint32(syscall.F_UNLCK), fuse.OK
	default:
		return 0, fuse.EINVAL
	}
}

func NormalizeLockRange(lkFlags uint32, start, end uint64) (normStart, normEnd uint64, status fuse.Status) {
	if lkFlags&fuse.FUSE_LK_FLOCK != 0 {
		return 0, ^uint64(0), fuse.OK
	}
	if end < start {
		return 0, 0, fuse.EINVAL
	}
	return start, end, fuse.OK
}

func (fs *RawFS) prepareLock(input *fuse.LkIn, get bool) (fuse.LkIn, fuse.Status) {
	if _, status := fs.ToServerIno(input.NodeId); status != fuse.OK {
		return fuse.LkIn{}, status
	}
	if _, status := fs.checkedHandle(input.NodeId, input.Fh, false); status != fuse.OK {
		return fuse.LkIn{}, status
	}
	if _, status := ToProtoLockType(input.Lk.Typ); status != fuse.OK {
		return fuse.LkIn{}, status
	}
	if get && input.Lk.Typ == uint32(syscall.F_UNLCK) {
		return fuse.LkIn{}, fuse.EINVAL
	}
	start, end, status := NormalizeLockRange(input.LkFlags, input.Lk.Start, input.Lk.End)
	if status != fuse.OK {
		return fuse.LkIn{}, status
	}
	in := *input
	in.Lk.Start, in.Lk.End = start, end
	return in, fuse.OK
}

func (fs *RawFS) GetLk(_ <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpGetLk)
	_, endSpan := fs.startFuseOp(OpGetLk)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpGetLk, start, status, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	in, status := fs.prepareLock(input, true)
	if status != fuse.OK {
		return status
	}
	out.Lk = fs.locks.GetLk(&in)
	return fuse.OK
}

func (fs *RawFS) SetLk(_ <-chan struct{}, input *fuse.LkIn) (status fuse.Status) {
	start := fs.metrics.beginOp(OpSetLk)
	_, endSpan := fs.startFuseOp(OpSetLk)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpSetLk, start, status, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	in, status := fs.prepareLock(input, false)
	if status != fuse.OK {
		return status
	}
	return fs.locks.SetLk(&in)
}

func (fs *RawFS) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) (status fuse.Status) {
	start := fs.metrics.beginOp(OpSetLkw)
	opCtx, endSpan := fs.startFuseOp(OpSetLkw)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpSetLkw, start, status, 0)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	in, status := fs.prepareLock(input, false)
	if status != fuse.OK {
		return status
	}
	return fs.locks.SetLkw(opCtx, cancel, &in)
}
