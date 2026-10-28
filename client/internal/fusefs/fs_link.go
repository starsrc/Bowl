package fusefs

import (
	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Symlink 实现创建符号链接操作。
// 校验只读挂载、重写父 Inode，透传调用进程 UID/GID、目标路径与链接名称。
// 注意 go-fuse 参数顺序：pointedTo 为符号链接指向的目标路径，linkName 为待创建的目录项名称。
func (fs *RawFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) fuse.Status {
	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}

	parentIno, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		ParentIno: parentIno,
		Name:      []byte(linkName),
		Target:    []byte(pointedTo),
		Uid:       header.Uid,
		Gid:       header.Gid,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

// Readlink 实现读取符号链接目标路径操作。
// 将 Inode 映射为服务端 Inode 后调用 StorageService，原样返回目标字节切片，不进行有损字符集过滤。
func (fs *RawFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	ino, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return nil, status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
		Ino: ino,
	}))
	if err != nil {
		return nil, fs.ToStatus(err, cancel)
	}

	return resp.Msg.GetTarget(), fuse.OK
}

// Link 实现为非目录已有文件创建同盘硬链接操作。
// 校验只读挂载，同时对源文件节点与目标父目录执行根 Inode 双向重写，回填递增 nlink 后的 Inode 属性。
func (fs *RawFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}

	srcIno, status := fs.ToServerIno(input.Oldnodeid)
	if status != fuse.OK {
		return status
	}

	newParentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		Ino:          srcIno,
		NewParentIno: newParentIno,
		NewName:      []byte(filename),
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}
