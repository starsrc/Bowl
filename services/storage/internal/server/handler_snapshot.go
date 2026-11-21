package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/control"
)

func toProtoSnapshotKind(k control.SnapshotKind) storagev1.SnapshotKind {
	switch k {
	case control.SnapshotKindFork:
		return storagev1.SnapshotKind_SNAPSHOT_KIND_FORK
	case control.SnapshotKindMerge:
		return storagev1.SnapshotKind_SNAPSHOT_KIND_MERGE
	case control.SnapshotKindManual:
		return storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL
	default:
		return storagev1.SnapshotKind_SNAPSHOT_KIND_UNSPECIFIED
	}
}

func toControlSnapshotKind(k storagev1.SnapshotKind) (control.SnapshotKind, error) {
	switch k {
	case storagev1.SnapshotKind_SNAPSHOT_KIND_UNSPECIFIED:
		return "", nil
	case storagev1.SnapshotKind_SNAPSHOT_KIND_FORK:
		return control.SnapshotKindFork, nil
	case storagev1.SnapshotKind_SNAPSHOT_KIND_MERGE:
		return control.SnapshotKindMerge, nil
	case storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL:
		return control.SnapshotKindManual, nil
	default:
		return "", control.ErrInvalidArgument
	}
}

func toProtoSnapshot(s *control.Snapshot) *storagev1.Snapshot {
	if s == nil {
		return nil
	}
	return &storagev1.Snapshot{
		SnapshotId:            s.SnapshotID,
		SourceDiskId:          s.SourceDiskID,
		ParentSnapshotId:      s.ParentSnapshotID,
		MergeParentSnapshotId: s.MergeParentSnapshotID,
		Kind:                  toProtoSnapshotKind(s.Kind),
		Name:                  s.Name,
		Protected:             s.Protected,
		CreatedAt:             timestamppb.New(s.CreatedAt),
	}
}

func (s *Server) CreateSnapshot(ctx context.Context, req *connect.Request[storagev1.CreateSnapshotRequest]) (*connect.Response[storagev1.CreateSnapshotResponse], error) {
	snap, err := s.control.CreateSnapshot(ctx, control.CreateSnapshotParams{
		DiskID: req.Msg.DiskId,
		Name:   req.Msg.Name,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.CreateSnapshotResponse{
		Snapshot: toProtoSnapshot(snap),
	}), nil
}

func (s *Server) ListSnapshots(ctx context.Context, req *connect.Request[storagev1.ListSnapshotsRequest]) (*connect.Response[storagev1.ListSnapshotsResponse], error) {
	kind, err := toControlSnapshotKind(req.Msg.Kind)
	if err != nil {
		return nil, toConnectError(err)
	}
	res, err := s.control.ListSnapshots(ctx, control.ListSnapshotsParams{
		DiskID:    req.Msg.DiskId,
		Kind:      kind,
		PageSize:  req.Msg.PageSize,
		PageToken: req.Msg.PageToken,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	protoSnaps := make([]*storagev1.Snapshot, 0, len(res.Snapshots))
	for _, snap := range res.Snapshots {
		protoSnaps = append(protoSnaps, toProtoSnapshot(snap))
	}
	return connect.NewResponse(&storagev1.ListSnapshotsResponse{
		Snapshots:     protoSnaps,
		NextPageToken: res.NextPageToken,
	}), nil
}

func (s *Server) DeleteSnapshot(ctx context.Context, req *connect.Request[storagev1.DeleteSnapshotRequest]) (*connect.Response[storagev1.DeleteSnapshotResponse], error) {
	if err := s.control.DeleteSnapshot(ctx, control.DeleteSnapshotParams{
		DiskID:     req.Msg.DiskId,
		SnapshotID: req.Msg.SnapshotId,
	}); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.DeleteSnapshotResponse{}), nil
}

func (s *Server) PruneSnapshots(ctx context.Context, req *connect.Request[storagev1.PruneSnapshotsRequest]) (*connect.Response[storagev1.PruneSnapshotsResponse], error) {
	res, err := s.control.PruneSnapshots(ctx, control.PruneSnapshotsParams{
		DiskID: req.Msg.DiskId,
		DryRun: req.Msg.DryRun,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	protoSnaps := make([]*storagev1.Snapshot, 0, len(res.Snapshots))
	for _, snap := range res.Snapshots {
		protoSnaps = append(protoSnaps, toProtoSnapshot(snap))
	}
	return connect.NewResponse(&storagev1.PruneSnapshotsResponse{
		Snapshots:      protoSnaps,
		ReclaimedCount: res.ReclaimedCount,
		DryRun:         res.DryRun,
	}), nil
}
