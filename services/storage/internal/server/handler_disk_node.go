package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// toProtoAttribute 将 meta.Attribute 领域实体转换为 Protobuf InodeAttribute。
func toProtoAttribute(attr meta.Attribute) *storagev1.InodeAttribute {
	return &storagev1.InodeAttribute{
		Mode:    attr.Mode,
		Size:    attr.Size,
		Nlink:   attr.Nlink,
		Uid:     attr.UID,
		Gid:     attr.GID,
		Rdev:    attr.Rdev,
		Blksize: attr.Blksize,
		Blocks:  attr.Blocks,
		Atime:   timestamppb.New(attr.Atime),
		Mtime:   timestamppb.New(attr.Mtime),
		Ctime:   timestamppb.New(attr.Ctime),
		Flags:   attr.Flags,
	}
}

// toProtoInode 将 meta.Inode 领域实体转换为 Protobuf Inode。
func toProtoInode(in *meta.Inode) *storagev1.Inode {
	if in == nil {
		return nil
	}
	return &storagev1.Inode{
		Ino:        in.Ino,
		Generation: in.Generation,
		Attr:       toProtoAttribute(in.Attr),
	}
}

// toProtoDisk 将 meta.Disk 领域实体转换为 Protobuf Disk。
func toProtoDisk(d *meta.Disk) *storagev1.Disk {
	if d == nil {
		return nil
	}
	return &storagev1.Disk{
		DiskId:     d.DiskID,
		Name:       d.Name,
		RootIno:    d.RootIno,
		MaxBytes:   d.MaxBytes,
		UsedBytes:  d.UsedBytes,
		UsedInodes: d.UsedInodes,
		CreatedAt:  timestamppb.New(d.CreatedAt),
		UpdatedAt:  timestamppb.New(d.UpdatedAt),
	}
}

// CreateDisk 创建新的虚拟磁盘实体并原子创建其根目录 Inode。
func (s *Server) CreateDisk(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	disk, err := s.metaManager.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     req.Msg.Name,
		MaxBytes: req.Msg.MaxBytes,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.CreateDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

// GetDisk 查询指定 disk_id 的虚拟磁盘实体详情与配额状态。
func (s *Server) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	disk, err := s.metaManager.GetDisk(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.GetDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

// UpdateDisk 动态调整虚拟磁盘名称或容量配额上限。
func (s *Server) UpdateDisk(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
	disk, err := s.metaManager.UpdateDisk(ctx, meta.UpdateDiskParams{
		DiskID:   req.Msg.DiskId,
		Name:     req.Msg.Name,
		MaxBytes: req.Msg.MaxBytes,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.UpdateDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

// DeleteDisk 删除指定的虚拟磁盘实体及其全部关联 Inode 与数据块。
func (s *Server) DeleteDisk(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	if err := s.metaManager.DeleteDisk(ctx, req.Msg.DiskId); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
}

// StatFS 查询指定 Inode 所在虚拟磁盘的文件系统统计状态。
func (s *Server) StatFS(ctx context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	res, err := s.metaManager.StatFS(ctx, req.Msg.Ino)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.StatFSResponse{
		TotalBytes: res.TotalBytes,
		UsedBytes:  res.UsedBytes,
		AvailBytes: res.AvailBytes,
		Bsize:      res.Bsize,
		Frsize:     res.Frsize,
		Blocks:     res.Blocks,
		Bfree:      res.Bfree,
		Bavail:     res.Bavail,
		Files:      res.Files,
		Ffree:      res.Ffree,
		Namelen:    res.Namelen,
	}), nil
}

// Lookup 执行单级目录项查找。
func (s *Server) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	inode, err := s.metaManager.Lookup(ctx, req.Msg.ParentIno, req.Msg.Name)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
	}), nil
}

// GetAttr 获取指定 Inode 的属性与内核缓存时效。
func (s *Server) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	inode, err := s.metaManager.GetAttr(ctx, req.Msg.Ino)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              toProtoInode(inode),
		AttrTimeoutSeconds: inode.AttrTimeoutSeconds,
	}), nil
}

// SetAttr 修改指定 Inode 的元数据属性。
func (s *Server) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	input := meta.SetAttrInput{
		Mode:      req.Msg.Mode,
		UID:       req.Msg.Uid,
		GID:       req.Msg.Gid,
		Size:      req.Msg.Size,
		AtimeNow:  req.Msg.AtimeNow,
		MtimeNow:  req.Msg.MtimeNow,
		Fh:        req.Msg.Fh,
		SessionID: req.Msg.SessionId,
		CtimeNow:  req.Msg.CtimeNow,
		Flags:     req.Msg.Flags,
	}
	if req.Msg.Atime != nil {
		atime := req.Msg.Atime.AsTime()
		input.Atime = &atime
	}
	if req.Msg.Mtime != nil {
		mtime := req.Msg.Mtime.AsTime()
		input.Mtime = &mtime
	}

	inode, err := s.metaManager.SetAttr(ctx, req.Msg.Ino, input)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              toProtoInode(inode),
		AttrTimeoutSeconds: inode.AttrTimeoutSeconds,
	}), nil
}

// Mknod 在指定父目录下创建常规文件或特殊设备节点。
func (s *Server) Mknod(ctx context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	inode, err := s.metaManager.Mknod(ctx, meta.MknodParams{
		ParentIno: req.Msg.ParentIno,
		Name:      req.Msg.Name,
		Mode:      req.Msg.Mode,
		UID:       req.Msg.Uid,
		GID:       req.Msg.Gid,
		Rdev:      req.Msg.Rdev,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.MknodResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
	}), nil
}

// Mkdir 在指定父目录下创建子目录。
func (s *Server) Mkdir(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	inode, err := s.metaManager.Mkdir(ctx, meta.MkdirParams{
		ParentIno: req.Msg.ParentIno,
		Name:      req.Msg.Name,
		Mode:      req.Msg.Mode,
		UID:       req.Msg.Uid,
		GID:       req.Msg.Gid,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
	}), nil
}

// Symlink 在指定父目录下创建指向目标路径的符号链接节点。
func (s *Server) Symlink(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
	inode, err := s.metaManager.Symlink(ctx, meta.SymlinkParams{
		ParentIno: req.Msg.ParentIno,
		Name:      req.Msg.Name,
		Target:    req.Msg.Target,
		UID:       req.Msg.Uid,
		GID:       req.Msg.Gid,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.SymlinkResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
	}), nil
}

// Readlink 获取符号链接节点指向的目标路径。
func (s *Server) Readlink(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	target, err := s.metaManager.Readlink(ctx, req.Msg.Ino)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ReadlinkResponse{
		Target: target,
	}), nil
}
