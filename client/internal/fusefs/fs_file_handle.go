package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// ToProtoOpenFlags 将宿主机操作系统特定的打开标志位转换为平台无关的服务端协议标志位。
// 仅保留访问模式（O_RDONLY / O_WRONLY / O_RDWR）及 O_CREAT、O_EXCL、O_TRUNC、O_APPEND 控制位，
// 彻底剥离 Linux O_LARGEFILE、O_DIRECT、O_NOFOLLOW、O_CLOEXEC 等内核高位，严防碰撞服务端 O_TMPFILE (0o20000000) 禁区。
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

// Create 实现原子创建常规文件并打开分配文件句柄，对应 Linux FUSE FUSE_CREATE 协议。
func (fs *RawFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	if status := fs.CheckWritable(); status != fuse.OK {
		return status
	}

	parentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	var sessionID *uint64
	if fs.sessions != nil {
		if sid := fs.sessions.CurrentSessionID(); sid > 0 {
			sessionID = &sid
		}
	}

	resp, err := fs.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      input.Mode & 0xffff,
		Uid:       input.Uid,
		Gid:       input.Gid,
		Flags:     ToProtoOpenFlags(input.Flags),
		SessionId: sessionID,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fh := resp.Msg.GetFh()
	if fs.sessions != nil && sessionID != nil {
		fs.sessions.BindHandle(fh, *sessionID)
	}

	fs.FillEntryOut(&out.EntryOut, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	out.Fh = fh
	out.OpenFlags = resp.Msg.GetOpenFlags()
	if fs.cfg.DirectIO {
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	}

	return fuse.OK
}

// Open 实现打开指定 Inode 常规文件并分配文件句柄，对应 Linux FUSE FUSE_OPEN 协议。
func (fs *RawFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if fs.cfg.ReadOnly {
		accMode := input.Flags & uint32(syscall.O_ACCMODE)
		if accMode != uint32(syscall.O_RDONLY) || (input.Flags&uint32(syscall.O_TRUNC)) != 0 {
			return fuse.Status(syscall.EROFS)
		}
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	var sessionID *uint64
	if fs.sessions != nil {
		if sid := fs.sessions.CurrentSessionID(); sid > 0 {
			sessionID = &sid
		}
	}

	resp, err := fs.client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       ino,
		Flags:     ToProtoOpenFlags(input.Flags),
		SessionId: sessionID,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fh := resp.Msg.GetFh()
	if fs.sessions != nil && sessionID != nil {
		fs.sessions.BindHandle(fh, *sessionID)
	}

	out.Fh = fh
	out.OpenFlags = resp.Msg.GetOpenFlags()
	if fs.cfg.DirectIO {
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	}

	return fuse.OK
}

// Flush 实现文件描述符关闭时的冲刷操作，对应 Linux FUSE FUSE_FLUSH 协议。
// 携带 lock_owner 与 session_id 触发服务端清理对应进程所持有的全部 POSIX 记录锁。
func (fs *RawFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	sid, status := fs.ResolveHandle(input.Fh)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	lockOwner := input.LockOwner
	_, err := fs.client.Flush(ctx, connect.NewRequest(&storagev1.FlushRequest{
		Ino:       ino,
		Fh:        input.Fh,
		LockOwner: &lockOwner,
		SessionId: &sid,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}

// Fsync 实现文件数据与元数据落盘同步操作，对应 Linux FUSE FUSE_FSYNC 协议。
// 根据 input.FsyncFlags 区分完整 fsync 与仅数据同步 fdatasync。
func (fs *RawFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	sid, status := fs.ResolveHandle(input.Fh)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	datasync := (input.FsyncFlags & 0x1) != 0
	_, err := fs.client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		Ino:       ino,
		Fh:        input.Fh,
		Datasync:  datasync,
		SessionId: &sid,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}

// Release 实现文件句柄关闭与销毁，对应 Linux FUSE FUSE_RELEASE 协议。
// 无论服务端调用成功与否，必定在退出时解绑客户端内存中的句柄会话映射；
// 携带 lock_owner 与 release_flags 触发服务端清理 BSD flock 锁并在孤儿节点无句柄引用时执行最终物理回收。
func (fs *RawFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	if fs.sessions != nil {
		defer fs.sessions.UnbindHandle(input.Fh)
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return
	}

	if fs.sessions == nil {
		return
	}

	sid, ok := fs.sessions.LookupHandleSession(input.Fh)
	if !ok {
		return
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	lockOwner := input.LockOwner
	releaseFlags := input.ReleaseFlags & 0x3

	_, _ = fs.client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
		Ino:          ino,
		Fh:           input.Fh,
		LockOwner:    &lockOwner,
		ReleaseFlags: releaseFlags,
		SessionId:    &sid,
	}))
}
