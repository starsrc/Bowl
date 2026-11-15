package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

// Link 创建硬链接，返回更新后的 Inode 实体元数据与内核缓存超时时长。
func (s *Server) Link(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	attr, err := actor.Link(ctx, store.LinkReq{
		Ino:          req.Msg.GetIno(),
		NewParentIno: req.Msg.GetNewParentIno(),
		NewName:      req.Msg.GetNewName(),
		Epoch:        req.Msg.GetEpoch(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.LinkResponse{
		Inode:               toProtoInode(&attr),
		EntryTimeoutSeconds: s.control.EntryTimeoutForMode(attr.Mode),
		AttrTimeoutSeconds:  s.control.AttrTimeoutSeconds(),
	}), nil
}

// Unlink 删除指定父目录下的非目录项（解链）。
func (s *Server) Unlink(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if err := actor.Unlink(ctx, req.Msg.GetParentIno(), req.Msg.GetName(), req.Msg.GetOpenInClient(), req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
}

// Rmdir 移除指定父目录下的空目录项。
func (s *Server) Rmdir(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if err := actor.Rmdir(ctx, req.Msg.GetParentIno(), req.Msg.GetName(), req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RmdirResponse{}), nil
}

// Rename 执行原子文件/目录重命名、移动或双向交换。
func (s *Server) Rename(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	err = actor.Rename(ctx, store.RenameReq{
		OldParentIno:     req.Msg.GetOldParentIno(),
		OldName:          req.Msg.GetOldName(),
		NewParentIno:     req.Msg.GetNewParentIno(),
		NewName:          req.Msg.GetNewName(),
		Flags:            req.Msg.GetFlags(),
		TargetOpenHandle: req.Msg.GetTargetOpenInClient(),
		Epoch:            req.Msg.GetEpoch(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RenameResponse{}), nil
}

// SetXattr 设置目标节点的扩展属性键值。
func (s *Server) SetXattr(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if err := actor.SetXattr(ctx, req.Msg.GetIno(), req.Msg.GetName(), req.Msg.GetValue(), req.Msg.GetFlags(), req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.SetXattrResponse{}), nil
}

// GetXattr 获取目标节点指定扩展属性键的属性值。
func (s *Server) GetXattr(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	val, err := actor.GetXattr(ctx, req.Msg.GetIno(), req.Msg.GetName())
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.GetXattrResponse{
		Value: val,
	}), nil
}

// ListXattr 列出目标节点绑定的所有扩展属性名称列表。
func (s *Server) ListXattr(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	names, err := actor.ListXattr(ctx, req.Msg.GetIno())
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.ListXattrResponse{
		Names: names,
	}), nil
}

// RemoveXattr 移除目标节点指定的扩展属性键。
func (s *Server) RemoveXattr(ctx context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if err := actor.RemoveXattr(ctx, req.Msg.GetIno(), req.Msg.GetName(), req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RemoveXattrResponse{}), nil
}

// ReclaimOrphan 回收最后一个本地句柄关闭的孤儿节点。
func (s *Server) ReclaimOrphan(ctx context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if err := actor.PurgeOrphanInode(ctx, req.Msg.GetIno(), req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ReclaimOrphanResponse{}), nil
}
