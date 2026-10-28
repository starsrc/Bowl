package fusefs

import (
	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Mkdir 实现创建子目录操作，校验只读挂载、重写父 Inode，透传模式位与调用进程 UID/GID。
func (fs *RawFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}

	parentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      input.Mode,
		Uid:       input.Uid,
		Gid:       input.Gid,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

// Mknod 实现非目录特殊节点（如 FIFO 命名管道、设备节点、普通文件）的创建操作。
func (fs *RawFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}

	parentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      input.Mode,
		Uid:       input.Uid,
		Gid:       input.Gid,
		Rdev:      input.Rdev,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.FillEntryOut(out, resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	return fuse.OK
}

// Rmdir 移除指定父目录下的空目录项。
func (fs *RawFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}

	parentIno, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	_, err := fs.client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}

// Unlink 删除指定父目录下的非目录项（解链）。
func (fs *RawFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}

	parentIno, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	_, err := fs.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}
