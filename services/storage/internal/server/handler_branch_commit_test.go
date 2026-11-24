package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func TestHandlerBranchAndCommit_FullGitModelLifecycle(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, nil)

	disk := env.createDisk(t, "git-disk", 8*1024*1024)
	diskID := disk.GetDiskId()
	mainBranch := defaultBranch()

	mount := env.mountBranchRW(t, diskID, mainBranch, false)
	epoch := mount.GetEpoch()

	f1, err := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    mainBranch,
		ParentIno: engine.RootIno,
		Name:      []byte("a.txt"),
		Mode:      0o644,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	_, err = env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: mainBranch,
		Ino:    f1.Msg.GetInode().GetIno(),
		Data:   []byte("v1"),
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	c1Resp, err := env.client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  mainBranch,
		Message: "commit v1",
	}))
	require.NoError(t, err)
	c1 := c1Resp.Msg.GetCommit()
	require.Equal(t, storagev1.CommitKind_COMMIT_KIND_MANUAL, c1.GetKind())
	require.Equal(t, "commit v1", c1.GetMessage())

	forkResp, err := env.client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: mainBranch,
		NewBranch:    "feat",
		Message:      "fork feat",
	}))
	require.NoError(t, err)
	require.Equal(t, storagev1.CommitKind_COMMIT_KIND_FORK, forkResp.Msg.GetCommit().GetKind())
	require.Equal(t, "feat", forkResp.Msg.GetBranch().GetName())
	require.Equal(t, forkResp.Msg.GetCommit().GetCommitId(), forkResp.Msg.GetBranch().GetHeadCommitId())

	branchesResp, err := env.client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Len(t, branchesResp.Msg.GetBranches(), 2)

	_, err = env.client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: diskID,
		Branch: mainBranch,
	}))
	requireConnectErr(t, err, connect.CodeFailedPrecondition, "EPERM")

	featMount := env.mountBranchRW(t, diskID, "feat", false)
	_, err = env.client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	requireConnectErr(t, err, connect.CodeAborted, "EBUSY")

	f2, err := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    "feat",
		ParentIno: engine.RootIno,
		Name:      []byte("feat.txt"),
		Mode:      0o644,
		Epoch:     featMount.GetEpoch(),
	}))
	require.NoError(t, err)
	_, err = env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: "feat",
		Ino:    f2.Msg.GetInode().GetIno(),
		Data:   []byte("feat-only"),
		Epoch:  featMount.GetEpoch(),
	}))
	require.NoError(t, err)

	featCmtResp, err := env.client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  "feat",
		Message: "feat commit",
	}))
	require.NoError(t, err)
	featCommitID := featCmtResp.Msg.GetCommit().GetCommitId()

	_, err = env.client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: featMount.GetMountId(),
	}))
	require.NoError(t, err)

	_, err = env.client.DeleteCommit(ctx, connect.NewRequest(&storagev1.DeleteCommitRequest{
		DiskId:   diskID,
		CommitId: featCommitID,
	}))
	requireConnectErr(t, err, connect.CodeFailedPrecondition, "EBUSY")

	resetResp, err := env.client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         diskID,
		Branch:         "feat",
		TargetCommitId: c1.GetCommitId(),
	}))
	require.NoError(t, err)
	require.Equal(t, c1.GetCommitId(), resetResp.Msg.GetBranch().GetHeadCommitId())

	_, err = env.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    "feat",
		ParentIno: engine.RootIno,
		Name:      []byte("feat.txt"),
	}))
	requireConnectErr(t, err, connect.CodeNotFound, "ENOENT")

	undoResp, err := env.client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         diskID,
		Branch:         "feat",
		TargetCommitId: featCommitID,
	}))
	require.NoError(t, err)
	require.Equal(t, featCommitID, undoResp.Msg.GetBranch().GetHeadCommitId())

	_, err = env.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    "feat",
		ParentIno: engine.RootIno,
		Name:      []byte("feat.txt"),
	}))
	require.NoError(t, err)

	_, err = env.client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)

	manualList, err := env.client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
		DiskId:   diskID,
		Kind:     storagev1.CommitKind_COMMIT_KIND_MANUAL,
		PageSize: 1,
	}))
	require.NoError(t, err)
	require.Len(t, manualList.Msg.GetCommits(), 1)
	require.NotEmpty(t, manualList.Msg.GetNextPageToken())

	manualPage2, err := env.client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
		DiskId:    diskID,
		Kind:      storagev1.CommitKind_COMMIT_KIND_MANUAL,
		PageSize:  1,
		PageToken: manualList.Msg.GetNextPageToken(),
	}))
	require.NoError(t, err)
	require.Len(t, manualPage2.Msg.GetCommits(), 1)
	require.Empty(t, manualPage2.Msg.GetNextPageToken())
	require.Greater(t, manualList.Msg.GetCommits()[0].GetCommitId(), manualPage2.Msg.GetCommits()[0].GetCommitId())

	_, err = env.client.DeleteCommit(ctx, connect.NewRequest(&storagev1.DeleteCommitRequest{
		DiskId:   diskID,
		CommitId: featCommitID,
	}))
	require.NoError(t, err)

	_, err = env.client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mount.GetMountId(),
	}))
	require.NoError(t, err)

	_, err = env.client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         diskID,
		Branch:         mainBranch,
		TargetCommitId: 1,
	}))
	require.NoError(t, err)

	pruneResp, err := env.client.PruneCommits(ctx, connect.NewRequest(&storagev1.PruneCommitsRequest{
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.GreaterOrEqual(t, pruneResp.Msg.GetReclaimedCount(), uint32(2))
}
