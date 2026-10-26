package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// inodeToProtoForTopology 将元数据领域层 *meta.Inode 转换为 Protobuf *storagev1.Inode。
// 函数命名保持专属前缀，避免与并行分支的 toProtoInode / toProtoAttribute 产生符号冲突。
func inodeToProtoForTopology(in *meta.Inode) *storagev1.Inode {
	if in == nil {
		return nil
	}

	var atime, mtime, ctime *timestamppb.Timestamp
	if !in.Attr.Atime.IsZero() {
		atime = timestamppb.New(in.Attr.Atime)
	}
	if !in.Attr.Mtime.IsZero() {
		mtime = timestamppb.New(in.Attr.Mtime)
	}
	if !in.Attr.Ctime.IsZero() {
		ctime = timestamppb.New(in.Attr.Ctime)
	}

	return &storagev1.Inode{
		Ino:        in.Ino,
		Generation: in.Generation,
		Attr: &storagev1.InodeAttribute{
			Mode:    in.Attr.Mode,
			Size:    in.Attr.Size,
			Nlink:   in.Attr.Nlink,
			Uid:     in.Attr.UID,
			Gid:     in.Attr.GID,
			Rdev:    in.Attr.Rdev,
			Blksize: in.Attr.Blksize,
			Blocks:  in.Attr.Blocks,
			Atime:   atime,
			Mtime:   mtime,
			Ctime:   ctime,
			Flags:   in.Attr.Flags,
		},
	}
}

// Link 创建硬链接，返回更新后的 Inode 实体元数据与内核缓存超时时长。
func (s *Server) Link(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	inode, err := s.metaManager.Link(ctx, meta.LinkParams{
		Ino:          req.Msg.GetIno(),
		NewParentIno: req.Msg.GetNewParentIno(),
		NewName:      req.Msg.GetNewName(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.LinkResponse{
		Inode:               inodeToProtoForTopology(inode),
		EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
	}), nil
}

// Unlink 删除指定父目录下的非目录项（解链）。
func (s *Server) Unlink(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	err := s.metaManager.Unlink(ctx, req.Msg.GetParentIno(), req.Msg.GetName())
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
}

// Rmdir 移除指定父目录下的空目录项。
func (s *Server) Rmdir(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
	err := s.metaManager.Rmdir(ctx, req.Msg.GetParentIno(), req.Msg.GetName())
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RmdirResponse{}), nil
}

// Rename 执行原子文件/目录重命名、移动或双向交换。
func (s *Server) Rename(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	err := s.metaManager.Rename(ctx, meta.RenameParams{
		OldParentIno: req.Msg.GetOldParentIno(),
		OldName:      req.Msg.GetOldName(),
		NewParentIno: req.Msg.GetNewParentIno(),
		NewName:      req.Msg.GetNewName(),
		Flags:        req.Msg.GetFlags(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RenameResponse{}), nil
}

// SetXattr 设置目标节点的扩展属性键值。
func (s *Server) SetXattr(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	err := s.metaManager.SetXattr(ctx, meta.SetXattrParams{
		Ino:   req.Msg.GetIno(),
		Name:  req.Msg.GetName(),
		Value: req.Msg.GetValue(),
		Flags: req.Msg.GetFlags(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.SetXattrResponse{}), nil
}

// GetXattr 获取目标节点指定扩展属性键的属性值。
func (s *Server) GetXattr(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
	val, err := s.metaManager.GetXattr(ctx, req.Msg.GetIno(), req.Msg.GetName())
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.GetXattrResponse{
		Value: val,
	}), nil
}

// ListXattr 列出目标节点绑定的所有扩展属性名称列表。
func (s *Server) ListXattr(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
	names, err := s.metaManager.ListXattr(ctx, req.Msg.GetIno())
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.ListXattrResponse{
		Names: names,
	}), nil
}

// RemoveXattr 移除目标节点指定的扩展属性键。
func (s *Server) RemoveXattr(ctx context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	err := s.metaManager.RemoveXattr(ctx, req.Msg.GetIno(), req.Msg.GetName())
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RemoveXattrResponse{}), nil
}
