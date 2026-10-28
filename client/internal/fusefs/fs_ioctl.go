package fusefs

import (
	"encoding/binary"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Linux ioctl 文件系统标志位命令号（兼容 64 位与 32 位 ABI，以及 VFS fileattr 扩展属性桥接）。
const (
	FS_IOC_GETFLAGS   uint32 = 0x80086601
	FS_IOC32_GETFLAGS uint32 = 0x80046601
	FS_IOC_SETFLAGS   uint32 = 0x40086602
	FS_IOC32_SETFLAGS uint32 = 0x40046602

	// Linux VFS fileattr 扩展属性 ioctl 命令号（内核 fuse_fileattr_get/set 私有调用）。
	FS_IOC_FSGETXATTR uint32 = 0x801c581f
	FS_IOC_FSSETXATTR uint32 = 0x401c5820
)

// Linux VFS 标志位常量（对应 lsattr / chattr）。
const (
	FS_IMMUTABLE_FL uint32 = 0x00000010
	FS_APPEND_FL    uint32 = 0x00000020

	// Linux fsxattr 扩展标志位常量（对应 FS_IOC_FSGETXATTR / FS_IOC_FSSETXATTR）。
	FS_XFLAG_IMMUTABLE uint32 = 0x00000008
	FS_XFLAG_APPEND    uint32 = 0x00000010
)

// 服务端协议标志位常量（对应 storage.proto / meta.Flag*）。
const (
	ProtoFlagImmutable uint32 = 0x1
	ProtoFlagAppend    uint32 = 0x2
)

// Ioctl 实现 FUSE_IOCTL 请求处理，将 Linux 标准 FS_IOC_GETFLAGS / FS_IOC_SETFLAGS 双向桥接至 StorageService RPC。
func (fs *RawFS) Ioctl(cancel <-chan struct{}, input *fuse.IoctlIn, inbuf []byte, output *fuse.IoctlOut, outbuf []byte) fuse.Status {
	ino, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return st
	}

	switch input.Cmd {
	case FS_IOC_GETFLAGS, FS_IOC32_GETFLAGS:
		if len(outbuf) < 4 {
			return fuse.Status(syscall.EINVAL)
		}

		ctx, cancelFunc := fs.RPCContext(cancel)
		defer cancelFunc()

		resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			Ino: ino,
		}))
		if err != nil {
			return fs.ToStatus(err, cancel)
		}

		protoFlags := resp.Msg.GetInode().GetAttr().GetFlags()
		var vfsFlags uint32
		if (protoFlags & ProtoFlagImmutable) != 0 {
			vfsFlags |= FS_IMMUTABLE_FL
		}
		if (protoFlags & ProtoFlagAppend) != 0 {
			vfsFlags |= FS_APPEND_FL
		}

		clear(outbuf)
		if len(outbuf) >= 8 {
			binary.LittleEndian.PutUint64(outbuf[:8], uint64(vfsFlags))
		} else {
			binary.LittleEndian.PutUint32(outbuf[:4], vfsFlags)
		}

		if output != nil {
			output.Result = 0
		}
		return fuse.OK

	case FS_IOC_FSGETXATTR:
		if len(outbuf) < 28 {
			return fuse.Status(syscall.EINVAL)
		}

		ctx, cancelFunc := fs.RPCContext(cancel)
		defer cancelFunc()

		resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			Ino: ino,
		}))
		if err != nil {
			return fs.ToStatus(err, cancel)
		}

		protoFlags := resp.Msg.GetInode().GetAttr().GetFlags()
		var xflags uint32
		if (protoFlags & ProtoFlagImmutable) != 0 {
			xflags |= FS_XFLAG_IMMUTABLE
		}
		if (protoFlags & ProtoFlagAppend) != 0 {
			xflags |= FS_XFLAG_APPEND
		}

		clear(outbuf)
		binary.LittleEndian.PutUint32(outbuf[:4], xflags)

		if output != nil {
			output.Result = 0
		}
		return fuse.OK

	case FS_IOC_SETFLAGS, FS_IOC32_SETFLAGS:
		if st := fs.CheckWritable(); st != fuse.OK {
			return st
		}

		if len(inbuf) < 4 {
			return fuse.Status(syscall.EINVAL)
		}

		var rawFlags uint64
		if len(inbuf) >= 8 {
			rawFlags = binary.LittleEndian.Uint64(inbuf[:8])
		} else {
			rawFlags = uint64(binary.LittleEndian.Uint32(inbuf[:4]))
		}

		supportedMask := uint64(FS_IMMUTABLE_FL | FS_APPEND_FL)
		if (rawFlags & ^supportedMask) != 0 {
			return fuse.Status(syscall.EOPNOTSUPP)
		}

		vfsFlags := uint32(rawFlags)
		var protoFlags uint32
		if (vfsFlags & FS_IMMUTABLE_FL) != 0 {
			protoFlags |= ProtoFlagImmutable
		}
		if (vfsFlags & FS_APPEND_FL) != 0 {
			protoFlags |= ProtoFlagAppend
		}

		ctx, cancelFunc := fs.RPCContext(cancel)
		defer cancelFunc()

		_, err := fs.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:   ino,
			Flags: &protoFlags,
		}))
		if err != nil {
			return fs.ToStatus(err, cancel)
		}

		if output != nil {
			output.Result = 0
		}
		return fuse.OK

	case FS_IOC_FSSETXATTR:
		if st := fs.CheckWritable(); st != fuse.OK {
			return st
		}

		if len(inbuf) < 28 {
			return fuse.Status(syscall.EINVAL)
		}

		xflags := binary.LittleEndian.Uint32(inbuf[:4])
		supportedXMask := FS_XFLAG_IMMUTABLE | FS_XFLAG_APPEND
		if (xflags & ^supportedXMask) != 0 {
			return fuse.Status(syscall.EOPNOTSUPP)
		}

		var protoFlags uint32
		if (xflags & FS_XFLAG_IMMUTABLE) != 0 {
			protoFlags |= ProtoFlagImmutable
		}
		if (xflags & FS_XFLAG_APPEND) != 0 {
			protoFlags |= ProtoFlagAppend
		}

		ctx, cancelFunc := fs.RPCContext(cancel)
		defer cancelFunc()

		_, err := fs.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:   ino,
			Flags: &protoFlags,
		}))
		if err != nil {
			return fs.ToStatus(err, cancel)
		}

		if output != nil {
			output.Result = 0
		}
		return fuse.OK

	default:
		return fuse.Status(syscall.ENOTTY)
	}
}
