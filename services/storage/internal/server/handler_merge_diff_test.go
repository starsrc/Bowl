package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func extractConflictReport(t *testing.T, err error) *storagev1.MergeConflictReport {
	t.Helper()
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	for _, detail := range connectErr.Details() {
		if val, vErr := detail.Value(); vErr == nil {
			if report, ok := val.(*storagev1.MergeConflictReport); ok {
				return report
			}
		}
	}
	return nil
}

func TestHandlerDiffAndReadCommitNode_Step24Contract(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, nil)

	disk := env.createDisk(t, "diff-disk", 8*1024*1024)
	diskID := disk.GetDiskId()
	mainBranch := defaultBranch()

	mount := env.mountBranchRW(t, diskID, mainBranch, false)
	epoch := mount.GetEpoch()

	fResp, err := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    mainBranch,
		ParentIno: engine.RootIno,
		Name:      []byte("readme.txt"),
		Mode:      0o644,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	fileIno := fResp.Msg.GetInode().GetIno()

	_, err = env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: mainBranch,
		Ino:    fileIno,
		Data:   []byte("base-content-0123456789"),
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	symResp, err := env.client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		DiskId:    diskID,
		Branch:    mainBranch,
		ParentIno: engine.RootIno,
		Name:      []byte("link.txt"),
		Target:    []byte("readme.txt"),
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	symIno := symResp.Msg.GetInode().GetIno()

	workDiff, err := env.client.DiffWork(ctx, connect.NewRequest(&storagev1.DiffWorkRequest{
		DiskId: diskID,
		Branch: mainBranch,
	}))
	require.NoError(t, err)
	require.Zero(t, workDiff.Msg.GetSourceCommitId())
	require.Equal(t, uint64(2), workDiff.Msg.GetSummary().GetAdded())

	c1Resp, err := env.client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  mainBranch,
		Message: "base",
	}))
	require.NoError(t, err)
	c1ID := c1Resp.Msg.GetCommit().GetCommitId()

	nodeChunk1, err := env.client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: c1ID,
		Ino:      fileIno,
		Offset:   0,
		Size:     4,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("base"), nodeChunk1.Msg.GetData())
	require.False(t, nodeChunk1.Msg.GetEof())
	require.Equal(t, uint64(23), nodeChunk1.Msg.GetFileSize())

	nodeChunkEOF, err := env.client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: c1ID,
		Ino:      fileIno,
		Offset:   13,
		Size:     100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("0123456789"), nodeChunkEOF.Msg.GetData())
	require.True(t, nodeChunkEOF.Msg.GetEof())

	symNode, err := env.client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: c1ID,
		Ino:      symIno,
		Size:     64,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("readme.txt"), symNode.Msg.GetSymlinkTarget())

	_, err = env.client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: 99999,
		Ino:      fileIno,
		Size:     64,
	}))
	requireConnectErr(t, err, connect.CodeNotFound, "ENOENT")

	commitsDiff, err := env.client.DiffCommits(ctx, connect.NewRequest(&storagev1.DiffCommitsRequest{
		DiskId:         diskID,
		BaseCommitId:   1,
		SourceCommitId: c1ID,
		PageSize:       1,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(1), commitsDiff.Msg.GetBaseCommitId())
	require.Equal(t, c1ID, commitsDiff.Msg.GetSourceCommitId())
	require.Len(t, commitsDiff.Msg.GetEntries(), 1)
	require.NotEmpty(t, commitsDiff.Msg.GetNextPageToken())

	commitsDiff2, err := env.client.DiffCommits(ctx, connect.NewRequest(&storagev1.DiffCommitsRequest{
		DiskId:         diskID,
		BaseCommitId:   1,
		SourceCommitId: c1ID,
		PageSize:       1,
		PageToken:      commitsDiff.Msg.GetNextPageToken(),
	}))
	require.NoError(t, err)
	require.Len(t, commitsDiff2.Msg.GetEntries(), 1)
	require.Empty(t, commitsDiff2.Msg.GetNextPageToken())
}

func TestHandlerMergeBranch_FFNoFFConflictsAndGuards(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, nil)

	disk := env.createDisk(t, "merge-disk", 8*1024*1024)
	diskID := disk.GetDiskId()
	mainBranch := defaultBranch()

	mainMount := env.mountBranchRW(t, diskID, mainBranch, false)
	fResp, err := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    mainBranch,
		ParentIno: engine.RootIno,
		Name:      []byte("shared.txt"),
		Mode:      0o644,
		Epoch:     mainMount.GetEpoch(),
	}))
	require.NoError(t, err)
	sharedIno := fResp.Msg.GetInode().GetIno()
	_, err = env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: mainBranch,
		Ino:    sharedIno,
		Data:   []byte("base"),
		Epoch:  mainMount.GetEpoch(),
	}))
	require.NoError(t, err)

	_, err = env.client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  mainBranch,
		Message: "base commit",
	}))
	require.NoError(t, err)

	forkResp, err := env.client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: mainBranch,
		NewBranch:    "feat",
		Message:      "fork feat",
	}))
	require.NoError(t, err)
	lcaCommitID := forkResp.Msg.GetCommit().GetCommitId()

	featMount := env.mountBranchRW(t, diskID, "feat", false)
	featFile, err := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
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
		Ino:    featFile.Msg.GetInode().GetIno(),
		Data:   []byte("from-feat"),
		Epoch:  featMount.GetEpoch(),
	}))
	require.NoError(t, err)

	_, err = env.client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: mainBranch,
	}))
	requireConnectErr(t, err, connect.CodeAborted, "EBUSY")

	_, err = env.client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mainMount.GetMountId(),
	}))
	require.NoError(t, err)
	_, err = env.client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: featMount.GetMountId(),
	}))
	require.NoError(t, err)

	_, err = env.client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: mainBranch,
	}))
	requireConnectErr(t, err, connect.CodeFailedPrecondition, "EDIRTY")

	_, err = env.client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  "feat",
		Message: "feat commit",
	}))
	require.NoError(t, err)

	ffResp, err := env.client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: mainBranch,
	}))
	require.NoError(t, err)
	require.True(t, ffResp.Msg.GetFastForward())
	require.False(t, ffResp.Msg.GetUpToDate())
	require.Equal(t, lcaCommitID, ffResp.Msg.GetLcaCommitId())
	require.NotNil(t, ffResp.Msg.GetBranch())
	require.NotNil(t, ffResp.Msg.GetCommit())

	upToDateResp, err := env.client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: mainBranch,
	}))
	require.NoError(t, err)
	require.True(t, upToDateResp.Msg.GetUpToDate())

	divFork, err := env.client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: mainBranch,
		NewBranch:    "conflict-branch",
	}))
	require.NoError(t, err)
	conflictLCA := divFork.Msg.GetCommit().GetCommitId()

	mMain := env.mountBranchRW(t, diskID, mainBranch, false)
	_, err = env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: mainBranch,
		Ino:    sharedIno,
		Data:   []byte("main-edit"),
		Epoch:  mMain.GetEpoch(),
	}))
	require.NoError(t, err)
	_, err = env.client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  mainBranch,
		Message: "main edit",
	}))
	require.NoError(t, err)
	_, err = env.client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mMain.GetMountId(),
	}))
	require.NoError(t, err)

	mConflict := env.mountBranchRW(t, diskID, "conflict-branch", false)
	_, err = env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: "conflict-branch",
		Ino:    sharedIno,
		Data:   []byte("conflict-edit"),
		Epoch:  mConflict.GetEpoch(),
	}))
	require.NoError(t, err)
	_, err = env.client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  "conflict-branch",
		Message: "conflict edit",
	}))
	require.NoError(t, err)
	_, err = env.client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mConflict.GetMountId(),
	}))
	require.NoError(t, err)

	diffBr, err := env.client.DiffBranches(ctx, connect.NewRequest(&storagev1.DiffBranchesRequest{
		DiskId:        diskID,
		SourceBranch:  "conflict-branch",
		TargetBranch:  mainBranch,
		ConflictsOnly: true,
	}))
	require.NoError(t, err)
	require.Equal(t, conflictLCA, diffBr.Msg.GetBaseCommitId())
	require.Equal(t, uint64(1), diffBr.Msg.GetSummary().GetConflicts())

	_, err = env.client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "conflict-branch",
		TargetBranch: mainBranch,
	}))
	requireConnectErr(t, err, connect.CodeAborted, "ECONFLICT")
	report := extractConflictReport(t, err)
	require.NotNil(t, report)
	require.Equal(t, conflictLCA, report.GetLcaCommitId())
	require.Equal(t, uint64(1), report.GetTotalConflicts())
	require.Len(t, report.GetConflicts(), 1)
	require.Equal(t, []byte("shared.txt"), report.GetConflicts()[0].GetPath())

	dryResp, err := env.client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "conflict-branch",
		TargetBranch: mainBranch,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		DryRun:       true,
	}))
	require.NoError(t, err)
	require.Equal(t, conflictLCA, dryResp.Msg.GetLcaCommitId())
	require.Zero(t, dryResp.Msg.GetCommitId())

	resolvedResp, err := env.client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "conflict-branch",
		TargetBranch: mainBranch,
		PathStrategies: map[string]storagev1.MergeStrategy{
			"shared.txt": storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		},
		Message: "resolve theirs",
	}))
	require.NoError(t, err)
	require.False(t, resolvedResp.Msg.GetFastForward())
	require.NotZero(t, resolvedResp.Msg.GetCommitId())
	require.Equal(t, storagev1.CommitKind_COMMIT_KIND_MERGE, resolvedResp.Msg.GetCommit().GetKind())
	require.NotZero(t, resolvedResp.Msg.GetCommit().GetMergeParentId())

	readResolved, err := env.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: mainBranch,
		Ino:    sharedIno,
		Size:   64,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("conflict-edit"), readResolved.Msg.GetData())
}
