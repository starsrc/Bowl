//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestE2E_CommitDAGPaginationResetDeleteAndPruneLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()
	suffix := time.Now().UnixNano()

	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-commit-mgmt-%d", suffix),
		MaxBytes: 64 << 20,
	}))
	require.NoError(t, err)
	diskID := diskResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, diskID)

	mainMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	mainEpoch := mainMnt.Msg.GetEpoch()
	mainMountID := mainMnt.Msg.GetMountId()

	cfgIno := e2eWriteNewFile(t, ctx, client, diskID, testutil.DefaultBranch, mainEpoch, "app.cfg", []byte("ver=1\n"))
	cmt1Resp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  testutil.DefaultBranch,
		Message: "manual-v1",
	}))
	require.NoError(t, err)
	cmt1 := cmt1Resp.Msg.GetCommit()
	require.Equal(t, storagev1.CommitKind_COMMIT_KIND_MANUAL, cmt1.GetKind())
	require.Equal(t, "manual-v1", cmt1.GetMessage())

	e2eOverwriteFile(t, ctx, client, diskID, testutil.DefaultBranch, mainEpoch, "app.cfg", []byte("ver=2\n"))
	cmt2Resp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  testutil.DefaultBranch,
		Message: "manual-v2",
	}))
	require.NoError(t, err)
	cmt2 := cmt2Resp.Msg.GetCommit()
	require.Equal(t, cmt1.GetCommitId(), cmt2.GetParentId())

	e2eOverwriteFile(t, ctx, client, diskID, testutil.DefaultBranch, mainEpoch, "app.cfg", []byte("ver=3-dirty\n"))

	_, resetBusyErr := client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         diskID,
		Branch:         testutil.DefaultBranch,
		TargetCommitId: cmt1.GetCommitId(),
	}))
	testutil.RequireConnectError(t, resetBusyErr, connect.CodeAborted, "EBUSY")

	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mainMountID,
	}))
	require.NoError(t, err)

	resetBack, err := client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         diskID,
		Branch:         testutil.DefaultBranch,
		TargetCommitId: cmt1.GetCommitId(),
	}))
	require.NoError(t, err)
	require.Equal(t, cmt1.GetCommitId(), resetBack.Msg.GetBranch().GetHeadCommitId())
	require.Equal(t, mainEpoch+1, resetBack.Msg.GetBranch().GetWriterEpoch())
	require.Equal(t, []byte("ver=1\n"), e2eReadFileByName(t, ctx, client, diskID, testutil.DefaultBranch, "app.cfg"))

	resetUndo, err := client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         diskID,
		Branch:         testutil.DefaultBranch,
		TargetCommitId: cmt2.GetCommitId(),
	}))
	require.NoError(t, err)
	require.Equal(t, cmt2.GetCommitId(), resetUndo.Msg.GetBranch().GetHeadCommitId())
	require.Equal(t, []byte("ver=2\n"), e2eReadFileByName(t, ctx, client, diskID, testutil.DefaultBranch, "app.cfg"))

	forkResp, err := client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: testutil.DefaultBranch,
		NewBranch:    "feat",
		Message:      "fork-feat",
	}))
	require.NoError(t, err)
	forkCmt := forkResp.Msg.GetCommit()
	require.Equal(t, storagev1.CommitKind_COMMIT_KIND_FORK, forkCmt.GetKind())

	featMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	e2eWriteNewFile(t, ctx, client, diskID, "feat", featMnt.Msg.GetEpoch(), "feat.txt", []byte("feat-data\n"))
	featCmtResp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  "feat",
		Message: "feat-manual",
	}))
	require.NoError(t, err)
	featCmt := featCmtResp.Msg.GetCommit()

	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: featMnt.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	mergeResp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:        diskID,
		SourceBranch:  "feat",
		TargetBranch:  testutil.DefaultBranch,
		NoFastForward: true,
		Message:       "merge-feat",
	}))
	require.NoError(t, err)
	mergeCommitID := mergeResp.Msg.GetCommitId()
	require.Positive(t, mergeCommitID)

	var allCommits []*storagev1.Commit
	pageToken := ""
	pages := 0
	for {
		listResp, listErr := client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
			DiskId:    diskID,
			PageSize:  2,
			PageToken: pageToken,
		}))
		require.NoError(t, listErr)
		pages++
		allCommits = append(allCommits, listResp.Msg.GetCommits()...)
		if listResp.Msg.GetNextPageToken() == "" {
			break
		}
		pageToken = listResp.Msg.GetNextPageToken()
		require.Less(t, pages, 50)
	}
	require.Len(t, allCommits, 6)
	require.GreaterOrEqual(t, pages, 3)
	for i := 1; i < len(allCommits); i++ {
		require.Greater(t, allCommits[i-1].GetCommitId(), allCommits[i].GetCommitId())
	}

	initFilter, err := client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
		DiskId: diskID,
		Kind:   storagev1.CommitKind_COMMIT_KIND_INIT,
	}))
	require.NoError(t, err)
	require.Len(t, initFilter.Msg.GetCommits(), 1)

	manualFilter, err := client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
		DiskId: diskID,
		Kind:   storagev1.CommitKind_COMMIT_KIND_MANUAL,
	}))
	require.NoError(t, err)
	require.Len(t, manualFilter.Msg.GetCommits(), 3)

	forkFilter, err := client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
		DiskId: diskID,
		Kind:   storagev1.CommitKind_COMMIT_KIND_FORK,
	}))
	require.NoError(t, err)
	require.Len(t, forkFilter.Msg.GetCommits(), 1)

	mergeFilter, err := client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
		DiskId: diskID,
		Kind:   storagev1.CommitKind_COMMIT_KIND_MERGE,
	}))
	require.NoError(t, err)
	require.Len(t, mergeFilter.Msg.GetCommits(), 1)
	require.Equal(t, mergeCommitID, mergeFilter.Msg.GetCommits()[0].GetCommitId())

	_, delReachableErr := client.DeleteCommit(ctx, connect.NewRequest(&storagev1.DeleteCommitRequest{
		DiskId:   diskID,
		CommitId: cmt1.GetCommitId(),
	}))
	testutil.RequireConnectError(t, delReachableErr, connect.CodeFailedPrecondition, "EBUSY")

	_, delHeadErr := client.DeleteCommit(ctx, connect.NewRequest(&storagev1.DeleteCommitRequest{
		DiskId:   diskID,
		CommitId: mergeCommitID,
	}))
	testutil.RequireConnectError(t, delHeadErr, connect.CodeFailedPrecondition, "EBUSY")

	branchesResp, err := client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Len(t, branchesResp.Msg.GetBranches(), 2)
	branchNames := make([]string, 0, len(branchesResp.Msg.GetBranches()))
	for _, b := range branchesResp.Msg.GetBranches() {
		branchNames = append(branchNames, b.GetName())
	}
	require.Equal(t, []string{"feat", testutil.DefaultBranch}, branchNames)

	_, delDefaultErr := client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	testutil.RequireConnectError(t, delDefaultErr, connect.CodeFailedPrecondition, "EPERM")

	featMntBusy, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)

	_, delMountedErr := client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	testutil.RequireConnectError(t, delMountedErr, connect.CodeAborted, "EBUSY")

	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: featMntBusy.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	_, err = client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)

	branchesAfterDel, err := client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Len(t, branchesAfterDel.Msg.GetBranches(), 1)
	require.Equal(t, testutil.DefaultBranch, branchesAfterDel.Msg.GetBranches()[0].GetName())

	_, err = client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         diskID,
		Branch:         testutil.DefaultBranch,
		TargetCommitId: cmt1.GetCommitId(),
	}))
	require.NoError(t, err)

	_, err = client.DeleteCommit(ctx, connect.NewRequest(&storagev1.DeleteCommitRequest{
		DiskId:   diskID,
		CommitId: mergeCommitID,
	}))
	require.NoError(t, err)

	_, readDeletedErr := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: mergeCommitID,
		Ino:      cfgIno,
		Offset:   0,
		Size:     64,
	}))
	testutil.RequireConnectError(t, readDeletedErr, connect.CodeNotFound, "ENOENT")

	pruneResp, err := client.PruneCommits(ctx, connect.NewRequest(&storagev1.PruneCommitsRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, uint32(3), pruneResp.Msg.GetReclaimedCount())

	pruneAgain, err := client.PruneCommits(ctx, connect.NewRequest(&storagev1.PruneCommitsRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Zero(t, pruneAgain.Msg.GetReclaimedCount())

	remainingCommits, err := client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Len(t, remainingCommits.Msg.GetCommits(), 2)
	remainingIDs := make([]uint64, 0, len(remainingCommits.Msg.GetCommits()))
	for _, c := range remainingCommits.Msg.GetCommits() {
		remainingIDs = append(remainingIDs, c.GetCommitId())
	}
	require.Contains(t, remainingIDs, cmt1.GetCommitId())
	require.False(t, slices.Contains(remainingIDs, cmt2.GetCommitId()))
	require.False(t, slices.Contains(remainingIDs, forkCmt.GetCommitId()))
	require.False(t, slices.Contains(remainingIDs, featCmt.GetCommitId()))

	survivingRead, err := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: cmt1.GetCommitId(),
		Ino:      cfgIno,
		Offset:   0,
		Size:     64,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("ver=1\n"), survivingRead.Msg.GetData())
	require.True(t, survivingRead.Msg.GetEof())
}
