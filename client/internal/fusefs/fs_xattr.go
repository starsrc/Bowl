package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// SetXAttr 实现设置扩展属性请求，透传属性名、属性值与创建/替换标志位。
// 在只读挂载模式下直接拦截返回 EROFS。
func (fs *RawFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	if status := fs.CheckWritable(); status != fuse.OK {
		return status
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	_, err := fs.client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:   ino,
		Name:  []byte(attr),
		Value: data,
		Flags: input.Flags,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}

// GetXAttr 实现读取扩展属性请求，支持零长度探测与 ERANGE 缓冲区过小语义。
func (fs *RawFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	ino, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return 0, status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		Ino:  ino,
		Name: []byte(attr),
	}))
	if err != nil {
		return 0, fs.ToStatus(err, cancel)
	}

	val := resp.Msg.GetValue()
	valLen := uint32(len(val))

	if len(dest) == 0 {
		return valLen, fuse.OK
	}
	if len(dest) < len(val) {
		return valLen, fuse.Status(syscall.ERANGE)
	}

	copy(dest, val)
	return valLen, fuse.OK
}

// ListXAttr 实现列出扩展属性名称请求，组装以 '\x00' 结尾的连续字节序列。
func (fs *RawFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	ino, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return 0, status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{
		Ino: ino,
	}))
	if err != nil {
		return 0, fs.ToStatus(err, cancel)
	}

	names := resp.Msg.GetNames()
	var total int
	for _, name := range names {
		total += len(name) + 1
	}

	totalLen := uint32(total)
	if len(dest) == 0 {
		return totalLen, fuse.OK
	}
	if len(dest) < total {
		return totalLen, fuse.Status(syscall.ERANGE)
	}

	var offset int
	for _, name := range names {
		copy(dest[offset:], name)
		offset += len(name)
		dest[offset] = 0
		offset++
	}

	return totalLen, fuse.OK
}

// RemoveXAttr 实现删除指定扩展属性请求。
// 在只读挂载模式下直接拦截返回 EROFS。
func (fs *RawFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	if status := fs.CheckWritable(); status != fuse.OK {
		return status
	}

	ino, status := fs.ToServerIno(header.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	_, err := fs.client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
		Ino:  ino,
		Name: []byte(attr),
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	return fuse.OK
}
