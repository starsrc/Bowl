package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	fallocFlKeepSize  = unix.FALLOC_FL_KEEP_SIZE
	fallocFlPunchHole = unix.FALLOC_FL_PUNCH_HOLE
)

// SetAttr 实现按需部分更新 Inode 属性或截断文件空间，对应 Linux FUSE FUSE_SETATTR 协议。
func (fs *RawFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	if status := fs.CheckWritable(); status != fuse.OK {
		return status
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	req := &storagev1.SetAttrRequest{
		Ino: ino,
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

	// 仅当 FATTR_FH 置位时解析并绑定文件句柄与会话标识；未置位时严格保持 req.Fh == nil 且 req.SessionId == nil。
	if (valid & fuse.FATTR_FH) != 0 {
		sid, st := fs.ResolveHandle(input.Fh)
		if st != fuse.OK {
			return st
		}
		fh := input.Fh
		req.Fh = &fh
		req.SessionId = &sid
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	// 空更新回退保护：当没有任何受支持属性位被置位时（例如仅包含 FATTR_LOCKOWNER），
	// 不发送空更新请求以规避服务端 setattr_has_update CEL 校验拒绝，自动回退调用 GetAttr 并填充返回。
	if !hasUpdate {
		resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			Ino: ino,
		}))
		if err != nil {
			return fs.ToStatus(err, cancel)
		}
		fs.FillAttrOut(out, resp.Msg.GetInode(), resp.Msg.GetAttrTimeoutSeconds())
		return fuse.OK
	}

	resp, err := fs.client.SetAttr(ctx, connect.NewRequest(req))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.FillAttrOut(out, resp.Msg.GetInode(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

// Fallocate 实现文件空间预分配与稀疏空洞打洞，对应 Linux FUSE FUSE_FALLOCATE 协议。
func (fs *RawFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	if status := fs.CheckWritable(); status != fuse.OK {
		return status
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	sid, status := fs.ResolveHandle(input.Fh)
	if status != fuse.OK {
		return status
	}

	// 模式位校验：仅支持默认预留 0、FALLOC_FL_KEEP_SIZE 与 FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE。
	// 其余不支持模式（如单独 PUNCH_HOLE、COLLAPSE_RANGE、ZERO_RANGE、INSERT_RANGE 等）按规范直接拦截返回 EOPNOTSUPP。
	if input.Mode != 0 &&
		input.Mode != fallocFlKeepSize &&
		input.Mode != (fallocFlPunchHole|fallocFlKeepSize) {
		return fuse.Status(syscall.EOPNOTSUPP)
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	req := &storagev1.FallocateRequest{
		Ino:       ino,
		Fh:        input.Fh,
		Offset:    input.Offset,
		Length:    input.Length,
		Mode:      input.Mode,
		SessionId: &sid,
	}

	_, err := fs.client.Fallocate(ctx, connect.NewRequest(req))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}
