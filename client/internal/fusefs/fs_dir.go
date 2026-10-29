package fusefs

import (
	"context"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// OpenDir 打开指定目录节点并分配目录 FileHandle，绑定到当前活跃 ClientSession。
func (fs *RawFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ino, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return st
	}

	var sessionIDPtr *uint64
	var sid uint64
	if fs.sessions != nil {
		sid = fs.sessions.CurrentSessionID()
		if sid != 0 {
			sessionIDPtr = &sid
		}
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.OpenDir(ctx, connect.NewRequest(&storagev1.OpenDirRequest{
		Ino:       ino,
		Flags:     0, // 目录仅支持读模式，清洗宿主机可能携带的 O_DIRECTORY/O_LARGEFILE 等额外标志位
		SessionId: sessionIDPtr,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	if fs.sessions != nil && sid != 0 {
		fs.sessions.BindHandle(resp.Msg.GetFh(), sid)
	}

	out.Fh = resp.Msg.GetFh()
	out.OpenFlags = resp.Msg.GetOpenFlags()
	return fuse.OK
}

// ReadDir 读取指定目录的子项流并填入 DirEntryList，缓冲区写满时安全中断由内核无缝续读。
func (fs *RawFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ino, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return st
	}

	sid, st := fs.ResolveHandle(input.Fh)
	if st != fuse.OK {
		return st
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		Ino:       ino,
		Fh:        input.Fh,
		Offset:    input.Offset,
		Limit:     512,
		SessionId: &sid,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	for _, e := range resp.Msg.GetEntries() {
		de := fuse.DirEntry{
			Ino:  fs.ToKernelIno(e.GetIno()),
			Name: string(e.GetName()),
			Mode: e.GetMode(),
			Off:  e.GetNextOffset(),
		}
		if !out.AddDirEntry(de) {
			break
		}
	}

	return fuse.OK
}

// ReadDirPlus 读取指定目录的子项流及内联 Inode 属性，对 "." 与 ".." 严格保持 EntryOut 为全零以防内核引用计数膨胀。
func (fs *RawFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ino, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return st
	}

	sid, st := fs.ResolveHandle(input.Fh)
	if st != fuse.OK {
		return st
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
		Ino:       ino,
		Fh:        input.Fh,
		Offset:    input.Offset,
		Limit:     512,
		SessionId: &sid,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	for _, e := range resp.Msg.GetEntries() {
		mode := uint32(syscall.S_IFDIR)
		if e.GetInode() != nil && e.GetInode().GetAttr() != nil {
			mode = e.GetInode().GetAttr().GetMode()
		}
		name := string(e.GetName())
		de := fuse.DirEntry{
			Ino:  fs.ToKernelIno(e.GetIno()),
			Name: name,
			Mode: mode,
			Off:  e.GetNextOffset(),
		}
		eo := out.AddDirLookupEntry(de)
		if eo == nil {
			break
		}
		if name == "." || name == ".." {
			// 严格保持全零 (NodeId = 0)，绝不向内核注册额外 lookup 引用计数
			*eo = fuse.EntryOut{}
			continue
		}
		fs.FillEntryOut(eo, e.GetInode(), e.GetEntryTimeoutSeconds(), e.GetAttrTimeoutSeconds())
	}

	return fuse.OK
}

// FsyncDir 同步并持久化目录节点与目录句柄状态。
func (fs *RawFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ino, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return st
	}

	sid, st := fs.ResolveHandle(input.Fh)
	if st != fuse.OK {
		return st
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	datasync := (input.FsyncFlags & 0x1) != 0

	_, err := fs.client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
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

// ReleaseDir 关闭并销毁目录 FileHandle，确保清理本地句柄与会话血缘记录。
func (fs *RawFS) ReleaseDir(input *fuse.ReleaseIn) {
	if fs.sessions != nil {
		defer fs.sessions.UnbindHandle(input.Fh)
	}

	ino, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return
	}

	var sessionIDPtr *uint64
	var sid uint64
	if fs.sessions != nil {
		var ok bool
		sid, ok = fs.sessions.LookupHandleSession(input.Fh)
		if !ok {
			return
		}
		if sid != 0 {
			sessionIDPtr = &sid
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &storagev1.ReleaseDirRequest{
		Ino:       ino,
		Fh:        input.Fh,
		SessionId: sessionIDPtr,
	}
	_, err := fs.client.ReleaseDir(ctx, connect.NewRequest(req))
	if shouldEnqueueReleaseRetry(err) {
		fs.enqueueReleaseDirRetry(req, sid)
	}
}
