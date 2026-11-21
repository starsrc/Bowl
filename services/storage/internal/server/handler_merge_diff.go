package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func toProtoDiffNodeSummary(s *store.DiffNodeSummary) *storagev1.DiffNodeSummary {
	if s == nil {
		return nil
	}
	return &storagev1.DiffNodeSummary{
		Ino:  s.Ino,
		Mode: s.Mode,
		Size: s.Size,
	}
}

func toProtoDiffKind(k store.DiffKind) storagev1.DiffKind {
	switch k {
	case store.DiffKindAdded:
		return storagev1.DiffKind_DIFF_KIND_ADDED
	case store.DiffKindModified:
		return storagev1.DiffKind_DIFF_KIND_MODIFIED
	case store.DiffKindDeleted:
		return storagev1.DiffKind_DIFF_KIND_DELETED
	default:
		return storagev1.DiffKind_DIFF_KIND_UNSPECIFIED
	}
}

func toProtoConflictKind(k store.ConflictKind) storagev1.ConflictKind {
	switch k {
	case store.ConflictAddAdd:
		return storagev1.ConflictKind_CONFLICT_KIND_ADD_ADD
	case store.ConflictContent:
		return storagev1.ConflictKind_CONFLICT_KIND_CONTENT
	case store.ConflictModifyDelete:
		return storagev1.ConflictKind_CONFLICT_KIND_MODIFY_DELETE
	case store.ConflictDeleteModify:
		return storagev1.ConflictKind_CONFLICT_KIND_DELETE_MODIFY
	case store.ConflictTreeDeleteModify:
		return storagev1.ConflictKind_CONFLICT_KIND_TREE_DELETE_MODIFY
	case store.ConflictType:
		return storagev1.ConflictKind_CONFLICT_KIND_TYPE
	case store.ConflictDirFile:
		return storagev1.ConflictKind_CONFLICT_KIND_DIR_FILE
	case store.ConflictAttr:
		return storagev1.ConflictKind_CONFLICT_KIND_ATTR
	case store.ConflictXattr:
		return storagev1.ConflictKind_CONFLICT_KIND_XATTR
	case store.ConflictXattrQuota:
		return storagev1.ConflictKind_CONFLICT_KIND_XATTR_QUOTA
	case store.ConflictSecurityAttr:
		return storagev1.ConflictKind_CONFLICT_KIND_SECURITY_ATTR
	case store.ConflictHardlink:
		return storagev1.ConflictKind_CONFLICT_KIND_HARDLINK
	default:
		return storagev1.ConflictKind_CONFLICT_KIND_UNSPECIFIED
	}
}

func toStoreMergeStrategy(s storagev1.MergeStrategy) (store.MergeStrategy, error) {
	switch s {
	case storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED:
		return store.MergeStrategyUnspecified, nil
	case storagev1.MergeStrategy_MERGE_STRATEGY_FAIL:
		return store.MergeStrategyFail, nil
	case storagev1.MergeStrategy_MERGE_STRATEGY_OURS:
		return store.MergeStrategyOurs, nil
	case storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS:
		return store.MergeStrategyTheirs, nil
	default:
		return store.MergeStrategyUnspecified, store.ErrInvalid
	}
}

func toProtoMergeConflictReport(lcaSnapshotID string, report *store.MergeConflictReport) *storagev1.MergeConflictReport {
	if report == nil {
		return nil
	}
	conflicts := make([]*storagev1.MergeConflict, 0, len(report.Conflicts))
	for _, c := range report.Conflicts {
		conflicts = append(conflicts, &storagev1.MergeConflict{
			Path:   append([]byte(nil), c.Path...),
			Kind:   toProtoConflictKind(c.Kind),
			Base:   toProtoDiffNodeSummary(c.Base),
			Target: toProtoDiffNodeSummary(c.Target),
			Source: toProtoDiffNodeSummary(c.Source),
		})
	}
	return &storagev1.MergeConflictReport{
		LcaSnapshotId:      lcaSnapshotID,
		TotalConflicts:     report.TotalConflicts,
		Conflicts:          conflicts,
		ConflictsTruncated: report.ConflictsTruncated,
	}
}

func (s *Server) DiffDisk(ctx context.Context, req *connect.Request[storagev1.DiffDiskRequest]) (*connect.Response[storagev1.DiffDiskResponse], error) {
	var targetDiskID string
	if req.Msg.TargetDiskId != nil {
		targetDiskID = *req.Msg.TargetDiskId
	}
	res, err := s.control.DiffDisk(ctx, &control.DiffDiskParams{
		SourceDiskID:  req.Msg.SourceDiskId,
		TargetDiskID:  targetDiskID,
		PathPrefix:    req.Msg.PathPrefix,
		ConflictsOnly: req.Msg.ConflictsOnly,
		PageSize:      req.Msg.PageSize,
		PageToken:     req.Msg.PageToken,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	entries := make([]*storagev1.DiffEntry, 0, len(res.Entries))
	for _, e := range res.Entries {
		entries = append(entries, &storagev1.DiffEntry{
			Path:     append([]byte(nil), e.Path...),
			Kind:     toProtoDiffKind(e.Kind),
			Conflict: toProtoConflictKind(e.Conflict),
			Base:     toProtoDiffNodeSummary(e.Base),
			Target:   toProtoDiffNodeSummary(e.Target),
			Source:   toProtoDiffNodeSummary(e.Source),
		})
	}

	return connect.NewResponse(&storagev1.DiffDiskResponse{
		LcaSnapshotId: res.LCASnapshotID,
		Summary: &storagev1.DiffSummary{
			Added:     res.Summary.Added,
			Modified:  res.Summary.Modified,
			Deleted:   res.Summary.Deleted,
			Conflicts: res.Summary.Conflicts,
		},
		Entries:       entries,
		NextPageToken: res.NextPageToken,
	}), nil
}

func (s *Server) ReadSnapshotNode(ctx context.Context, req *connect.Request[storagev1.ReadSnapshotNodeRequest]) (*connect.Response[storagev1.ReadSnapshotNodeResponse], error) {
	res, err := s.control.ReadSnapshotNode(ctx, &control.ReadSnapshotNodeParams{
		DiskID:     req.Msg.DiskId,
		SnapshotID: req.Msg.SnapshotId,
		Ino:        req.Msg.Ino,
		Offset:     req.Msg.Offset,
		Size:       req.Msg.Size,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.ReadSnapshotNodeResponse{
		Mode:          res.Mode,
		FileSize:      res.FileSize,
		Data:          res.Data,
		Eof:           res.EOF,
		SymlinkTarget: res.SymlinkTarget,
	}), nil
}

func (s *Server) MergeDisk(ctx context.Context, req *connect.Request[storagev1.MergeDiskRequest]) (*connect.Response[storagev1.MergeDiskResponse], error) {
	strategy, err := toStoreMergeStrategy(req.Msg.Strategy)
	if err != nil {
		return nil, toConnectError(err)
	}
	var pathStrategies map[string]store.MergeStrategy
	if len(req.Msg.PathStrategies) > 0 {
		pathStrategies = make(map[string]store.MergeStrategy, len(req.Msg.PathStrategies))
		for p, ps := range req.Msg.PathStrategies {
			mapped, mapErr := toStoreMergeStrategy(ps)
			if mapErr != nil {
				return nil, toConnectError(mapErr)
			}
			pathStrategies[p] = mapped
		}
	}

	res, err := s.control.MergeDisk(ctx, &control.MergeDiskParams{
		SourceDiskID:   req.Msg.SourceDiskId,
		TargetDiskID:   req.Msg.TargetDiskId,
		Strategy:       strategy,
		PathStrategies: pathStrategies,
		DryRun:         req.Msg.DryRun,
		Force:          req.Msg.Force,
		MaxBytes:       req.Msg.MaxBytes,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.MergeDiskResponse{
		Disk:               toProtoDisk(res.Disk),
		LcaSnapshotId:      res.LCASnapshotID,
		MergeSnapshotId:    res.MergeSnapshotID,
		EstimatedUsedBytes: res.EstimatedUsedBytes,
		TargetMaxBytes:     res.TargetMaxBytes,
	}), nil
}
