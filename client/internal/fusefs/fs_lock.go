package fusefs

import (
	"math"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// 锁类型协议常量（对应 storagev1.FileLock.type）
const (
	ProtoLockTypeRead   uint32 = 0 // F_RDLCK (共享读锁)
	ProtoLockTypeWrite  uint32 = 1 // F_WRLCK (排他写锁)
	ProtoLockTypeUnlock uint32 = 2 // F_UNLCK (释放锁)
)

// ToProtoLockType 将宿主机操作系统锁类型显式转换为服务端协议锁类型枚举。
// 宿主机 syscall.F_RDLCK -> 0，syscall.F_WRLCK -> 1，syscall.F_UNLCK -> 2；非法值返回 fuse.EINVAL。
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

// ToHostLockType 将服务端协议锁类型枚举显式转换为宿主机操作系统锁类型。
// 协议 0 -> syscall.F_RDLCK，1 -> syscall.F_WRLCK，2 -> syscall.F_UNLCK；非法值返回 fuse.EINVAL。
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

// NormalizeLockRange 归一化并校验锁区间。
// lkFlags & 0x1 == 1 时为 BSD flock，强制归一化为整文件区间 [0, math.MaxInt64]；
// lkFlags & 0x1 == 0 时为 POSIX 记录锁：若 start > math.MaxInt64 返回 EINVAL，若 end > math.MaxInt64 钳位为 math.MaxInt64；若 end < start 返回 EINVAL。
func NormalizeLockRange(lkFlags uint32, start, end uint64) (uint64, uint64, fuse.Status) {
	if (lkFlags & 0x1) == 1 {
		return 0, math.MaxInt64, fuse.OK
	}
	if start > math.MaxInt64 {
		return 0, 0, fuse.EINVAL
	}
	if end > math.MaxInt64 {
		end = math.MaxInt64
	}
	if end < start {
		return 0, 0, fuse.EINVAL
	}
	return start, end, fuse.OK
}

// GetLk 实现探测冲突文件锁，对应 Linux FUSE FUSE_GETLK (31) 协议。
func (fs *RawFS) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	sid, status := fs.ResolveHandle(input.Fh)
	if status != fuse.OK {
		return status
	}

	protoType, status := ToProtoLockType(input.Lk.Typ)
	if status != fuse.OK {
		return status
	}

	lkFlags := input.LkFlags & 0x1
	start, end, status := NormalizeLockRange(lkFlags, input.Lk.Start, input.Lk.End)
	if status != fuse.OK {
		return status
	}

	ctx, cancelRPC := fs.RPCContext(cancel)
	defer cancelRPC()

	resp, err := fs.client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
		Ino:       ino,
		Fh:        input.Fh,
		LockOwner: input.Owner,
		Lk: &storagev1.FileLock{
			Start: start,
			End:   end,
			Type:  protoType,
			Pid:   input.Lk.Pid,
		},
		LkFlags:   lkFlags,
		SessionId: sid,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	respLk := resp.Msg.GetLk()
	if respLk != nil {
		hostType, status := ToHostLockType(respLk.GetType())
		if status != fuse.OK {
			return status
		}
		out.Lk.Start = respLk.GetStart()
		out.Lk.End = respLk.GetEnd()
		out.Lk.Typ = hostType
		out.Lk.Pid = respLk.GetPid()
	}

	return fuse.OK
}

// SetLk 实现非阻塞设置或释放文件锁，对应 Linux FUSE FUSE_SETLK (32) 协议。
func (fs *RawFS) SetLk(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	sid, status := fs.ResolveHandle(input.Fh)
	if status != fuse.OK {
		return status
	}

	protoType, status := ToProtoLockType(input.Lk.Typ)
	if status != fuse.OK {
		return status
	}

	lkFlags := input.LkFlags & 0x1
	start, end, status := NormalizeLockRange(lkFlags, input.Lk.Start, input.Lk.End)
	if status != fuse.OK {
		return status
	}

	ctx, cancelRPC := fs.RPCContext(cancel)
	defer cancelRPC()

	_, err := fs.client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       ino,
		Fh:        input.Fh,
		LockOwner: input.Owner,
		Lk: &storagev1.FileLock{
			Start: start,
			End:   end,
			Type:  protoType,
			Pid:   input.Lk.Pid,
		},
		LkFlags:   lkFlags,
		SessionId: sid,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}

// SetLkw 实现阻塞设置或释放文件锁，对应 Linux FUSE FUSE_SETLKW (33) 协议。
// 使用原子递增分配器生成唯一 RequestId，并在 cancel 通道触发关闭时立即取消在途 RPC 并返回 EINTR。
func (fs *RawFS) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	sid, status := fs.ResolveHandle(input.Fh)
	if status != fuse.OK {
		return status
	}

	protoType, status := ToProtoLockType(input.Lk.Typ)
	if status != fuse.OK {
		return status
	}

	lkFlags := input.LkFlags & 0x1
	start, end, status := NormalizeLockRange(lkFlags, input.Lk.Start, input.Lk.End)
	if status != fuse.OK {
		return status
	}

	reqID := fs.NextRequestID()

	ctx, cancelRPC := fs.RPCContext(cancel)
	defer cancelRPC()

	_, err := fs.client.SetLkw(ctx, connect.NewRequest(&storagev1.SetLkwRequest{
		Ino:       ino,
		Fh:        input.Fh,
		LockOwner: input.Owner,
		Lk: &storagev1.FileLock{
			Start: start,
			End:   end,
			Type:  protoType,
			Pid:   input.Lk.Pid,
		},
		LkFlags:   lkFlags,
		SessionId: sid,
		RequestId: reqID,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}
