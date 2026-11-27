package fusefs

import (
	"context"
	"encoding/binary"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	FS_IOC_GETFLAGS   uint32 = 0x80086601
	FS_IOC32_GETFLAGS uint32 = 0x80046601
	FS_IOC_SETFLAGS   uint32 = 0x40086602
	FS_IOC32_SETFLAGS uint32 = 0x40046602

	FS_IOC_FSGETXATTR uint32 = 0x801c581f
	FS_IOC_FSSETXATTR uint32 = 0x401c5820
)

const (
	FS_IMMUTABLE_FL uint32 = 0x00000010
	FS_APPEND_FL    uint32 = 0x00000020

	FS_XFLAG_IMMUTABLE uint32 = 0x00000008
	FS_XFLAG_APPEND    uint32 = 0x00000010
)

const (
	ProtoFlagImmutable uint32 = 0x1
	ProtoFlagAppend    uint32 = 0x2
)

func protoToVFSFlags(protoFlags uint32) uint32 {
	var vfsFlags uint32
	if protoFlags&ProtoFlagImmutable != 0 {
		vfsFlags |= FS_IMMUTABLE_FL
	}
	if protoFlags&ProtoFlagAppend != 0 {
		vfsFlags |= FS_APPEND_FL
	}
	return vfsFlags
}

func protoToXFlags(protoFlags uint32) uint32 {
	var xflags uint32
	if protoFlags&ProtoFlagImmutable != 0 {
		xflags |= FS_XFLAG_IMMUTABLE
	}
	if protoFlags&ProtoFlagAppend != 0 {
		xflags |= FS_XFLAG_APPEND
	}
	return xflags
}

func parseVFSFlagsIn(inbuf []byte) (uint32, fuse.Status) {
	if len(inbuf) < 4 {
		return 0, fuse.Status(syscall.EINVAL)
	}
	var rawFlags uint64
	if len(inbuf) >= 8 {
		rawFlags = binary.LittleEndian.Uint64(inbuf[:8])
	} else {
		rawFlags = uint64(binary.LittleEndian.Uint32(inbuf[:4]))
	}
	supportedMask := uint64(FS_IMMUTABLE_FL | FS_APPEND_FL)
	if rawFlags&^supportedMask != 0 {
		return 0, fuse.Status(syscall.EOPNOTSUPP)
	}
	vfsFlags := uint32(rawFlags)
	var protoFlags uint32
	if vfsFlags&FS_IMMUTABLE_FL != 0 {
		protoFlags |= ProtoFlagImmutable
	}
	if vfsFlags&FS_APPEND_FL != 0 {
		protoFlags |= ProtoFlagAppend
	}
	return protoFlags, fuse.OK
}

func parseXFlagsIn(inbuf []byte) (uint32, fuse.Status) {
	if len(inbuf) < 28 {
		return 0, fuse.Status(syscall.EINVAL)
	}
	xflags := binary.LittleEndian.Uint32(inbuf[:4])
	supportedXMask := FS_XFLAG_IMMUTABLE | FS_XFLAG_APPEND
	if xflags&^supportedXMask != 0 {
		return 0, fuse.Status(syscall.EOPNOTSUPP)
	}
	var protoFlags uint32
	if xflags&FS_XFLAG_IMMUTABLE != 0 {
		protoFlags |= ProtoFlagImmutable
	}
	if xflags&FS_XFLAG_APPEND != 0 {
		protoFlags |= ProtoFlagAppend
	}
	return protoFlags, fuse.OK
}

func writeVFSFlagsOut(outbuf []byte, protoFlags uint32) {
	vfsFlags := protoToVFSFlags(protoFlags)
	clear(outbuf)
	if len(outbuf) >= 8 {
		binary.LittleEndian.PutUint64(outbuf[:8], uint64(vfsFlags))
	} else {
		binary.LittleEndian.PutUint32(outbuf[:4], vfsFlags)
	}
}

func writeXFlagsOut(outbuf []byte, protoFlags uint32) {
	clear(outbuf)
	binary.LittleEndian.PutUint32(outbuf[:4], protoToXFlags(protoFlags))
}

func (fs *RawFS) Ioctl(cancel <-chan struct{}, input *fuse.IoctlIn, inbuf []byte, output *fuse.IoctlOut, outbuf []byte) (status fuse.Status) {
	start := fs.metrics.beginOp(OpIoctl)
	opCtx, endSpan := fs.startFuseOp(OpIoctl)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpIoctl, start, status, 0)
	}()

	switch input.Cmd {
	case FS_IOC_GETFLAGS, FS_IOC32_GETFLAGS,
		FS_IOC_SETFLAGS, FS_IOC32_SETFLAGS,
		FS_IOC_FSGETXATTR, FS_IOC_FSSETXATTR:
	default:
		return fuse.Status(syscall.ENOTTY)
	}

	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.Status(syscall.ENOTTY)
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()

	switch input.Cmd {
	case FS_IOC_GETFLAGS, FS_IOC32_GETFLAGS, FS_IOC_FSGETXATTR:
		minOutLen := 4
		if input.Cmd == FS_IOC_FSGETXATTR {
			minOutLen = 28
		}
		if len(outbuf) < minOutLen {
			return fuse.Status(syscall.EINVAL)
		}
		var protoFlags uint32
		if localFlags, local := fs.tmpMgr.GetFlags(input.NodeId); local {
			protoFlags = localFlags
		} else {
			ino, st := fs.ToServerIno(input.NodeId)
			if st != fuse.OK {
				return st
			}
			barrierCtx, barrierCancel := fs.stageBarrierContext(opCtx, cancel)
			defer barrierCancel()
			if err := fs.stage.BarrierInode(barrierCtx, ino); err != nil {
				return fs.stageErrToStatus(err)
			}
			g0 := fs.metaClock.Load()
			ctx, cancelFunc := fs.RPCContextFrom(barrierCtx, nil)
			defer cancelFunc()

			resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
				Ino:    ino,
				DiskId: fs.cfg.DiskID,
				Branch: fs.cfg.Branch,
			}))
			if err != nil {
				return fs.ToStatus(err)
			}
			if !fs.IsFenced() {
				fs.recordKnownInode(input.NodeId, resp.Msg.GetInode(), g0)
			}
			protoFlags = resp.Msg.GetInode().GetAttr().GetFlags()
		}
		if input.Cmd == FS_IOC_FSGETXATTR {
			writeXFlagsOut(outbuf, protoFlags)
		} else {
			writeVFSFlagsOut(outbuf, protoFlags)
		}
		if output != nil {
			output.Result = 0
		}
		return fuse.OK

	case FS_IOC_SETFLAGS, FS_IOC32_SETFLAGS, FS_IOC_FSSETXATTR:
		if st := fs.CheckWritable(); st != fuse.OK {
			return st
		}
		var protoFlags uint32
		var parseSt fuse.Status
		if input.Cmd == FS_IOC_FSSETXATTR {
			protoFlags, parseSt = parseXFlagsIn(inbuf)
		} else {
			protoFlags, parseSt = parseVFSFlagsIn(inbuf)
		}
		if parseSt != fuse.OK {
			return parseSt
		}

		unlock := fs.inodeLocks.lock(input.NodeId)
		defer unlock()

		if updatedInode, local, localSt := fs.tmpMgr.SetFlags(input.NodeId, protoFlags); local {
			if localSt != fuse.OK {
				return localSt
			}
			fs.recordKnownInodeAuthoritative(input.NodeId, updatedInode)
			if output != nil {
				output.Result = 0
			}
			return fuse.OK
		}

		ino, st := fs.ToServerIno(input.NodeId)
		if st != fuse.OK {
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

		resp, err := fs.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:    ino,
			Flags:  &protoFlags,
			DiskId: fs.cfg.DiskID,
			Branch: fs.cfg.Branch,
			Epoch:  fs.cfg.Epoch,
		}))
		if err != nil {
			return fs.ToStatus(err)
		}
		fs.markMetaDirty()
		switch {
		case resp.Msg.GetInode() != nil:
			fs.recordKnownInodeAuthoritative(input.NodeId, resp.Msg.GetInode())
		case protoFlags&(ProtoFlagImmutable|ProtoFlagAppend) != 0:
			fs.setKnownInodeSpecial(input.NodeId, true)
		default:
			fs.forgetKnownInode(input.NodeId)
		}

		if output != nil {
			output.Result = 0
		}
		return fuse.OK

	default:
		return fuse.Status(syscall.ENOTTY)
	}
}
