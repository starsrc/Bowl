package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func (s *Server) Link(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	if err := validateWriteEpoch(req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
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

func (s *Server) Unlink(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	if err := validateWriteEpoch(req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
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

func (s *Server) Rmdir(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
	if err := validateWriteEpoch(req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
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

func (s *Server) Rename(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	if err := validateWriteEpoch(req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
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

func (s *Server) SetXattr(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	if err := validateWriteEpoch(req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
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

func (s *Server) RemoveXattr(ctx context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	if err := validateWriteEpoch(req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
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

func (s *Server) ReclaimOrphan(ctx context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	if err := validateWriteEpoch(req.Msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
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
