package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func (s *Server) Link(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.GetEpoch())
	var inode engine.Inode
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var lErr error
		inode, lErr = ds.Link(epochCtx, req.Msg.GetBranch(), req.Msg.GetIno(), req.Msg.GetNewParentIno(), string(req.Msg.GetNewName()))
		return lErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.LinkResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: s.entryTimeoutForMode(inode.Mode),
		AttrTimeoutSeconds:  s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) Unlink(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.GetEpoch())
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.Unlink(epochCtx, req.Msg.GetBranch(), req.Msg.GetParentIno(), string(req.Msg.GetName()), req.Msg.GetOpenInClient())
	}); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
}

func (s *Server) Rmdir(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.GetEpoch())
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.Rmdir(epochCtx, req.Msg.GetBranch(), req.Msg.GetParentIno(), string(req.Msg.GetName()))
	}); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RmdirResponse{}), nil
}

func (s *Server) Rename(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.GetEpoch())
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.Rename(epochCtx, req.Msg.GetBranch(), engine.RenameReq{
			OldParentIno:       req.Msg.GetOldParentIno(),
			OldName:            string(req.Msg.GetOldName()),
			NewParentIno:       req.Msg.GetNewParentIno(),
			NewName:            string(req.Msg.GetNewName()),
			Flags:              req.Msg.GetFlags(),
			TargetOpenInClient: req.Msg.GetTargetOpenInClient(),
		})
	}); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RenameResponse{}), nil
}

func (s *Server) SetXattr(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.GetEpoch())
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.SetXattr(epochCtx, req.Msg.GetBranch(), req.Msg.GetIno(), string(req.Msg.GetName()), req.Msg.GetValue(), req.Msg.GetFlags())
	}); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.SetXattrResponse{}), nil
}

func (s *Server) GetXattr(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	var val []byte
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var gErr error
		val, gErr = ds.GetXattr(ctx, req.Msg.GetBranch(), req.Msg.GetIno(), string(req.Msg.GetName()))
		return gErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.GetXattrResponse{
		Value: val,
	}), nil
}

func (s *Server) ListXattr(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	var names []string
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var lErr error
		names, lErr = ds.ListXattr(ctx, req.Msg.GetBranch(), req.Msg.GetIno())
		return lErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	out := make([][]byte, len(names))
	for i, n := range names {
		out[i] = []byte(n)
	}

	return connect.NewResponse(&storagev1.ListXattrResponse{
		Names: out,
	}), nil
}

func (s *Server) RemoveXattr(ctx context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.GetEpoch())
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.RemoveXattr(epochCtx, req.Msg.GetBranch(), req.Msg.GetIno(), string(req.Msg.GetName()))
	}); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RemoveXattrResponse{}), nil
}

func (s *Server) ReclaimOrphan(ctx context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.GetEpoch())
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.PurgeOrphanInode(epochCtx, req.Msg.GetBranch(), req.Msg.GetIno())
	}); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ReclaimOrphanResponse{}), nil
}
