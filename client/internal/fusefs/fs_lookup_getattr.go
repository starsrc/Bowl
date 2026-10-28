package fusefs

import (
	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Lookup 实现 VFS 查找子项请求，将 parent_ino 双向转换并填充 EntryOut。
func (fs *RawFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	parentIno, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

// GetAttr 实现获取 Inode 属性请求，出站属性通过 ToKernelIno 还原根节点 Inode 为 1。
func (fs *RawFS) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	ino, status := fs.ToServerIno(in.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino: ino,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.FillAttrOut(out, resp.Msg.GetInode(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

// StatFs 实现文件系统容量与 Inode 配额统计查询，对应 Linux FUSE FUSE_STATFS 协议。
func (fs *RawFS) StatFs(cancel <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	ino, status := fs.ToServerIno(in.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		Ino: ino,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	msg := resp.Msg
	out.Blocks = msg.GetBlocks()
	out.Bfree = msg.GetBfree()
	out.Bavail = msg.GetBavail()
	out.Files = msg.GetFiles()
	out.Ffree = msg.GetFfree()
	out.Bsize = msg.GetBsize()
	out.Frsize = msg.GetFrsize()
	out.NameLen = msg.GetNamelen()

	return fuse.OK
}
