package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func toProtoDiffNodeSummary(s *engine.DiffNodeSummary) *storagev1.DiffNodeSummary {
	if s == nil {
		return nil
	}
	return &storagev1.DiffNodeSummary{
		Ino:  s.Ino,
		Mode: s.Mode,
		Size: s.Size,
	}
}

func toProtoDiffKind(k engine.DiffKind) storagev1.DiffKind {
	switch k {
	case engine.DiffKindAdded:
		return storagev1.DiffKind_DIFF_KIND_ADDED
	case engine.DiffKindModified:
		return storagev1.DiffKind_DIFF_KIND_MODIFIED
	case engine.DiffKindDeleted:
		return storagev1.DiffKind_DIFF_KIND_DELETED
	default:
		return storagev1.DiffKind_DIFF_KIND_UNSPECIFIED
	}
}

func toProtoConflictKind(k engine.ConflictKind) storagev1.ConflictKind {
	switch k {
	case engine.ConflictAddAdd:
		return storagev1.ConflictKind_CONFLICT_KIND_ADD_ADD
	case engine.ConflictContent:
		return storagev1.ConflictKind_CONFLICT_KIND_CONTENT
	case engine.ConflictModifyDelete:
		return storagev1.ConflictKind_CONFLICT_KIND_MODIFY_DELETE
	case engine.ConflictDeleteModify:
		return storagev1.ConflictKind_CONFLICT_KIND_DELETE_MODIFY
	case engine.ConflictTreeDeleteModify:
		return storagev1.ConflictKind_CONFLICT_KIND_TREE_DELETE_MODIFY
	case engine.ConflictType:
		return storagev1.ConflictKind_CONFLICT_KIND_TYPE
	case engine.ConflictDirFile:
		return storagev1.ConflictKind_CONFLICT_KIND_DIR_FILE
	case engine.ConflictAttr:
		return storagev1.ConflictKind_CONFLICT_KIND_ATTR
	case engine.ConflictXattr:
		return storagev1.ConflictKind_CONFLICT_KIND_XATTR
	case engine.ConflictXattrQuota:
		return storagev1.ConflictKind_CONFLICT_KIND_XATTR_QUOTA
	case engine.ConflictSecurityAttr:
		return storagev1.ConflictKind_CONFLICT_KIND_SECURITY_ATTR
	case engine.ConflictHardlink:
		return storagev1.ConflictKind_CONFLICT_KIND_HARDLINK
	default:
		return storagev1.ConflictKind_CONFLICT_KIND_UNSPECIFIED
	}
}

func toEngineMergeStrategy(s storagev1.MergeStrategy) (engine.MergeStrategy, error) {
	switch s {
	case storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED:
		return engine.MergeStrategyUnspecified, nil
	case storagev1.MergeStrategy_MERGE_STRATEGY_FAIL:
		return engine.MergeStrategyFail, nil
	case storagev1.MergeStrategy_MERGE_STRATEGY_OURS:
		return engine.MergeStrategyOurs, nil
	case storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS:
		return engine.MergeStrategyTheirs, nil
	default:
		return engine.MergeStrategyUnspecified, engine.ErrInvalid
	}
}

func toProtoMergeConflictReport(report *engine.MergeConflictReport) *storagev1.MergeConflictReport {
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
		LcaCommitId:        report.LCACommitID,
		TotalConflicts:     report.TotalConflicts,
		Conflicts:          conflicts,
		ConflictsTruncated: report.ConflictsTruncated,
	}
}

func toProtoDiffResponse(res *engine.DiffResult) *storagev1.DiffResponse {
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
	return &storagev1.DiffResponse{
		BaseCommitId:   res.BaseCommitID,
		TargetCommitId: res.TargetCommitID,
		SourceCommitId: res.SourceCommitID,
		Summary: &storagev1.DiffSummary{
			Added:     res.Summary.Added,
			Modified:  res.Summary.Modified,
			Deleted:   res.Summary.Deleted,
			Conflicts: res.Summary.Conflicts,
		},
		Entries:       entries,
		NextPageToken: res.NextPageToken,
	}
}

func (s *Server) DiffBranches(ctx context.Context, req *connect.Request[storagev1.DiffBranchesRequest]) (*connect.Response[storagev1.DiffResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var res *engine.DiffResult
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var dErr error
		res, dErr = ds.Diff(ctx, engine.DiffOptions{
			SourceBranch:  req.Msg.SourceBranch,
			TargetBranch:  req.Msg.TargetBranch,
			PathPrefix:    req.Msg.PathPrefix,
			ConflictsOnly: req.Msg.ConflictsOnly,
			PageSize:      req.Msg.PageSize,
			PageToken:     req.Msg.PageToken,
		})
		return dErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(toProtoDiffResponse(res)), nil
}

func (s *Server) DiffCommits(ctx context.Context, req *connect.Request[storagev1.DiffCommitsRequest]) (*connect.Response[storagev1.DiffResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var res *engine.DiffResult
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var dErr error
		res, dErr = ds.DiffCommits(ctx, engine.DiffCommitsOptions{
			BaseCommitID:   req.Msg.BaseCommitId,
			SourceCommitID: req.Msg.SourceCommitId,
			PathPrefix:     req.Msg.PathPrefix,
			ConflictsOnly:  req.Msg.ConflictsOnly,
			PageSize:       req.Msg.PageSize,
			PageToken:      req.Msg.PageToken,
		})
		return dErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(toProtoDiffResponse(res)), nil
}

func (s *Server) DiffWork(ctx context.Context, req *connect.Request[storagev1.DiffWorkRequest]) (*connect.Response[storagev1.DiffResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var res *engine.DiffResult
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var dErr error
		res, dErr = ds.DiffWork(ctx, req.Msg.Branch, engine.DiffWorkOptions{
			PathPrefix:    req.Msg.PathPrefix,
			ConflictsOnly: req.Msg.ConflictsOnly,
			PageSize:      req.Msg.PageSize,
			PageToken:     req.Msg.PageToken,
		})
		return dErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(toProtoDiffResponse(res)), nil
}

func (s *Server) ReadCommitNode(ctx context.Context, req *connect.Request[storagev1.ReadCommitNodeRequest]) (*connect.Response[storagev1.ReadCommitNodeResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var res engine.CommitNodeResult
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var rErr error
		res, rErr = ds.ReadCommitNode(ctx, req.Msg.CommitId, req.Msg.Ino, int64(req.Msg.Offset), int(req.Msg.Size))
		return rErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ReadCommitNodeResponse{
		Mode:          res.Mode,
		FileSize:      res.FileSize,
		Data:          res.Data,
		Eof:           res.EOF,
		SymlinkTarget: res.SymlinkTarget,
	}), nil
}

func (s *Server) MergeBranch(ctx context.Context, req *connect.Request[storagev1.MergeBranchRequest]) (*connect.Response[storagev1.MergeBranchResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	strategy, err := toEngineMergeStrategy(req.Msg.Strategy)
	if err != nil {
		return nil, toConnectError(err)
	}
	var pathStrategies map[string]engine.MergeStrategy
	if len(req.Msg.PathStrategies) > 0 {
		pathStrategies = make(map[string]engine.MergeStrategy, len(req.Msg.PathStrategies))
		for p, ps := range req.Msg.PathStrategies {
			mapped, mapErr := toEngineMergeStrategy(ps)
			if mapErr != nil {
				return nil, toConnectError(mapErr)
			}
			pathStrategies[p] = mapped
		}
	}

	res, err := s.manager.MergeBranch(ctx, diskID, engine.MergeOptions{
		SourceBranch:   req.Msg.SourceBranch,
		TargetBranch:   req.Msg.TargetBranch,
		Strategy:       strategy,
		PathStrategies: pathStrategies,
		DryRun:         req.Msg.DryRun,
		NoFastForward:  req.Msg.NoFastForward,
		Message:        req.Msg.Message,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	resp := &storagev1.MergeBranchResponse{
		LcaCommitId: res.BaseCommitID,
		CommitId:    res.CommitID,
		FastForward: res.FastForward,
		UpToDate:    res.UpToDate,
		WriterEpoch: res.WriterEpoch,
	}
	if res.Branch != nil {
		resp.Branch = toProtoBranch(*res.Branch)
	}
	if res.Commit != nil {
		resp.Commit = toProtoCommit(*res.Commit)
	}
	return connect.NewResponse(resp), nil
}
