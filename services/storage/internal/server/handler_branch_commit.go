package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func (s *Server) ForkBranch(ctx context.Context, req *connect.Request[storagev1.ForkBranchRequest]) (*connect.Response[storagev1.ForkBranchResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	cmt, br, err := s.node.ForkBranch(ctx, diskID, req.Msg.SourceBranch, req.Msg.NewBranch, req.Msg.Message)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ForkBranchResponse{
		Commit: toProtoCommit(diskID, cmt),
		Branch: toProtoBranch(diskID, br),
	}), nil
}

func (s *Server) ListBranches(ctx context.Context, req *connect.Request[storagev1.ListBranchesRequest]) (*connect.Response[storagev1.ListBranchesResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	branches, err := s.node.ListBranches(ctx, diskID)
	if err != nil {
		return nil, toConnectError(err)
	}
	out := make([]*storagev1.Branch, 0, len(branches))
	for _, b := range branches {
		out = append(out, toProtoBranch(diskID, b))
	}
	return connect.NewResponse(&storagev1.ListBranchesResponse{
		Branches: out,
	}), nil
}

func (s *Server) DeleteBranch(ctx context.Context, req *connect.Request[storagev1.DeleteBranchRequest]) (*connect.Response[storagev1.DeleteBranchResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if err = s.node.DeleteBranch(ctx, diskID, req.Msg.Branch); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.DeleteBranchResponse{}), nil
}

func (s *Server) ResetBranch(ctx context.Context, req *connect.Request[storagev1.ResetBranchRequest]) (*connect.Response[storagev1.ResetBranchResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	br, err := s.node.ResetBranch(ctx, diskID, req.Msg.Branch, req.Msg.TargetCommitId)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ResetBranchResponse{
		Branch: toProtoBranch(diskID, br),
	}), nil
}

func (s *Server) CreateCommit(ctx context.Context, req *connect.Request[storagev1.CreateCommitRequest]) (*connect.Response[storagev1.CreateCommitResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	cmt, err := s.node.Commit(ctx, diskID, req.Msg.Branch, req.Msg.Message)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.CreateCommitResponse{
		Commit: toProtoCommit(diskID, cmt),
	}), nil
}

func (s *Server) ListCommits(ctx context.Context, req *connect.Request[storagev1.ListCommitsRequest]) (*connect.Response[storagev1.ListCommitsResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	kind, err := toEngineCommitKind(req.Msg.Kind)
	if err != nil {
		return nil, toConnectError(err)
	}
	page, err := s.node.ListCommits(ctx, engine.ListCommitsQuery{
		DiskID:    diskID,
		Kind:      kind,
		PageSize:  req.Msg.PageSize,
		PageToken: req.Msg.PageToken,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	commits := make([]*storagev1.Commit, 0, len(page.Commits))
	for _, c := range page.Commits {
		commits = append(commits, toProtoCommit(diskID, c))
	}
	return connect.NewResponse(&storagev1.ListCommitsResponse{
		Commits:       commits,
		NextPageToken: page.NextPageToken,
	}), nil
}

func (s *Server) DeleteCommit(ctx context.Context, req *connect.Request[storagev1.DeleteCommitRequest]) (*connect.Response[storagev1.DeleteCommitResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if err = s.node.DeleteCommit(ctx, diskID, req.Msg.CommitId); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.DeleteCommitResponse{}), nil
}

func (s *Server) PruneCommits(ctx context.Context, req *connect.Request[storagev1.PruneCommitsRequest]) (*connect.Response[storagev1.PruneCommitsResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	reclaimed, err := s.node.PruneCommits(ctx, diskID)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.PruneCommitsResponse{
		ReclaimedCount: uint32(max(0, reclaimed)),
	}), nil
}
