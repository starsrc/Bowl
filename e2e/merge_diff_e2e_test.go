//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func e2eRegisterDiskCleanup(t *testing.T, client storagev1connect.StorageServiceClient, diskID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.EvictMounts(ctx, connect.NewRequest(&storagev1.EvictMountsRequest{DiskId: diskID}))
		_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			require.NoError(t, err, "cleanup disk failed: %s", diskID)
		}
	})
}

func e2eWriteNewFile(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID, branch string,
	epoch uint64,
	name string,
	data []byte,
) uint64 {
	t.Helper()
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: 1,
		Name:      []byte(name),
		Mode:      0o100644,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	ino := createResp.Msg.GetInode().GetIno()
	if len(data) > 0 {
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID,
			Branch: branch,
			Ino:    ino,
			Offset: 0,
			Data:   data,
			Epoch:  epoch,
		}))
		require.NoError(t, err)
	}
	return ino
}

func e2eOverwriteFile(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID, branch string,
	epoch uint64,
	name string,
	data []byte,
) {
	t.Helper()
	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: 1,
		Name:      []byte(name),
	}))
	require.NoError(t, err)
	ino := lookupResp.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    ino,
		Offset: 0,
		Data:   data,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
	size := uint64(len(data))
	_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    ino,
		Size:   &size,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
}

func e2eReadFileByName(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID, branch, name string,
) []byte {
	t.Helper()
	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: 1,
		Name:      []byte(name),
	}))
	require.NoError(t, err)
	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    lookupResp.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   1024,
	}))
	require.NoError(t, err)
	return readResp.Msg.GetData()
}

func e2eFindBranch(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID, branch string,
) *storagev1.Branch {
	t.Helper()
	resp, err := client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{DiskId: diskID}))
	require.NoError(t, err)
	for _, br := range resp.Msg.GetBranches() {
		if br.GetName() == branch {
			return br
		}
	}
	require.Failf(t, "branch not found", "disk=%s branch=%s", diskID, branch)
	return nil
}

func TestE2E_DiffWorkCommitsBranchesReadCommitNodeAndThreeWayMerge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()
	suffix := time.Now().UnixNano()

	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-merge-git-%d", suffix),
		MaxBytes: 64 << 20,
	}))
	require.NoError(t, err)
	diskID := diskResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, diskID)

	mainInitHead := e2eFindBranch(t, ctx, client, diskID, testutil.DefaultBranch).GetHeadCommitId()
	require.Positive(t, mainInitHead)

	mainMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	mainEpoch := mainMnt.Msg.GetEpoch()
	mainMountID := mainMnt.Msg.GetMountId()

	sharedPayload := []byte("base-shared-payload-0123456789\n")
	sharedIno := e2eWriteNewFile(t, ctx, client, diskID, testutil.DefaultBranch, mainEpoch, "shared.txt", sharedPayload)
	e2eWriteNewFile(t, ctx, client, diskID, testutil.DefaultBranch, mainEpoch, "ours_or_theirs.txt", []byte("base-other\n"))
	symResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     mainEpoch,
		ParentIno: 1,
		Name:      []byte("cfg.link"),
		Target:    []byte("/etc/bowl/config.toml"),
	}))
	require.NoError(t, err)
	symIno := symResp.Msg.GetInode().GetIno()

	workDiffBefore, err := client.DiffWork(ctx, connect.NewRequest(&storagev1.DiffWorkRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	require.Equal(t, mainInitHead, workDiffBefore.Msg.GetBaseCommitId())
	require.Equal(t, uint64(3), workDiffBefore.Msg.GetSummary().GetAdded())
	require.Len(t, workDiffBefore.Msg.GetEntries(), 3)

	baseCommitResp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  testutil.DefaultBranch,
		Message: "seed base files",
	}))
	require.NoError(t, err)
	baseCommitID := baseCommitResp.Msg.GetCommit().GetCommitId()
	require.Equal(t, storagev1.CommitKind_COMMIT_KIND_MANUAL, baseCommitResp.Msg.GetCommit().GetKind())

	workDiffClean, err := client.DiffWork(ctx, connect.NewRequest(&storagev1.DiffWorkRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	require.Empty(t, workDiffClean.Msg.GetEntries())

	commitDiff, err := client.DiffCommits(ctx, connect.NewRequest(&storagev1.DiffCommitsRequest{
		DiskId:         diskID,
		BaseCommitId:   mainInitHead,
		SourceCommitId: baseCommitID,
	}))
	require.NoError(t, err)
	require.Equal(t, mainInitHead, commitDiff.Msg.GetBaseCommitId())
	require.Equal(t, baseCommitID, commitDiff.Msg.GetSourceCommitId())
	require.Equal(t, uint64(3), commitDiff.Msg.GetSummary().GetAdded())

	chunk1, err := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: baseCommitID,
		Ino:      sharedIno,
		Offset:   0,
		Size:     10,
	}))
	require.NoError(t, err)
	require.Equal(t, sharedPayload[:10], chunk1.Msg.GetData())
	require.Equal(t, uint64(len(sharedPayload)), chunk1.Msg.GetFileSize())
	require.False(t, chunk1.Msg.GetEof())

	chunk2, err := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: baseCommitID,
		Ino:      sharedIno,
		Offset:   10,
		Size:     1024,
	}))
	require.NoError(t, err)
	require.Equal(t, sharedPayload[10:], chunk2.Msg.GetData())
	require.True(t, chunk2.Msg.GetEof())

	symNode, err := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: baseCommitID,
		Ino:      symIno,
		Offset:   0,
		Size:     1024,
	}))
	require.NoError(t, err)
	require.True(t, symNode.Msg.GetEof())
	require.Equal(t, []byte("/etc/bowl/config.toml"), symNode.Msg.GetSymlinkTarget())

	_, badCommitErr := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: 999999,
		Ino:      sharedIno,
		Offset:   0,
		Size:     64,
	}))
	testutil.RequireConnectError(t, badCommitErr, connect.CodeNotFound, "ENOENT")

	_, zeroCommitErr := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   diskID,
		CommitId: 0,
		Ino:      sharedIno,
		Offset:   0,
		Size:     64,
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(zeroCommitErr))

	forkResp, err := client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: testutil.DefaultBranch,
		NewBranch:    "feat",
		Message:      "fork feat branch",
	}))
	require.NoError(t, err)
	lcaCommitID := forkResp.Msg.GetCommit().GetCommitId()
	require.Equal(t, storagev1.CommitKind_COMMIT_KIND_FORK, forkResp.Msg.GetCommit().GetKind())
	require.Equal(t, lcaCommitID, forkResp.Msg.GetBranch().GetHeadCommitId())

	featMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	featEpoch := featMnt.Msg.GetEpoch()
	featMountID := featMnt.Msg.GetMountId()

	e2eOverwriteFile(t, ctx, client, diskID, testutil.DefaultBranch, mainEpoch, "shared.txt", []byte("main-edited\n"))
	e2eOverwriteFile(t, ctx, client, diskID, testutil.DefaultBranch, mainEpoch, "ours_or_theirs.txt", []byte("main-choice\n"))
	e2eOverwriteFile(t, ctx, client, diskID, "feat", featEpoch, "shared.txt", []byte("feat-edited\n"))
	e2eOverwriteFile(t, ctx, client, diskID, "feat", featEpoch, "ours_or_theirs.txt", []byte("feat-choice\n"))
	e2eWriteNewFile(t, ctx, client, diskID, "feat", featEpoch, "feat.txt", []byte("feat-added\n"))

	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mainMountID,
	}))
	require.NoError(t, err)
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: featMountID,
	}))
	require.NoError(t, err)

	_, dirtyTargetErr := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}))
	testutil.RequireConnectError(t, dirtyTargetErr, connect.CodeFailedPrecondition, "EDIRTY")

	mainCommitResp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  testutil.DefaultBranch,
		Message: "main divergent edits",
	}))
	require.NoError(t, err)
	mainHeadBeforeMerge := mainCommitResp.Msg.GetCommit().GetCommitId()

	_, dirtySourceErr := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}))
	testutil.RequireConnectError(t, dirtySourceErr, connect.CodeFailedPrecondition, "EDIRTY")

	featCommitResp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  "feat",
		Message: "feat divergent edits",
	}))
	require.NoError(t, err)
	featHead := featCommitResp.Msg.GetCommit().GetCommitId()

	diffBranchesAll, err := client.DiffBranches(ctx, connect.NewRequest(&storagev1.DiffBranchesRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	require.Equal(t, lcaCommitID, diffBranchesAll.Msg.GetBaseCommitId())
	require.Equal(t, mainHeadBeforeMerge, diffBranchesAll.Msg.GetTargetCommitId())
	require.Equal(t, featHead, diffBranchesAll.Msg.GetSourceCommitId())
	require.Equal(t, uint64(1), diffBranchesAll.Msg.GetSummary().GetAdded())
	require.Equal(t, uint64(2), diffBranchesAll.Msg.GetSummary().GetModified())
	require.Equal(t, uint64(2), diffBranchesAll.Msg.GetSummary().GetConflicts())

	diffBranchesConflicts, err := client.DiffBranches(ctx, connect.NewRequest(&storagev1.DiffBranchesRequest{
		DiskId:        diskID,
		SourceBranch:  "feat",
		TargetBranch:  testutil.DefaultBranch,
		ConflictsOnly: true,
	}))
	require.NoError(t, err)
	require.Len(t, diffBranchesConflicts.Msg.GetEntries(), 2)
	for _, entry := range diffBranchesConflicts.Msg.GetEntries() {
		require.Equal(t, storagev1.ConflictKind_CONFLICT_KIND_CONTENT, entry.GetConflict())
	}

	dryResp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
		DryRun:       true,
	}))
	require.NoError(t, err)
	require.Equal(t, lcaCommitID, dryResp.Msg.GetLcaCommitId())
	require.Zero(t, dryResp.Msg.GetCommitId())
	require.Equal(t, mainHeadBeforeMerge, e2eFindBranch(t, ctx, client, diskID, testutil.DefaultBranch).GetHeadCommitId())

	targetROMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId:   diskID,
		Branch:   testutil.DefaultBranch,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	_, busyTargetErr := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}))
	testutil.RequireConnectError(t, busyTargetErr, connect.CodeAborted, "EBUSY")
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: targetROMnt.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	sourceRWMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	_, busySourceRWErr := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}))
	testutil.RequireConnectError(t, busySourceRWErr, connect.CodeAborted, "EBUSY")
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: sourceRWMnt.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	_, conflictErr := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_FAIL,
	}))
	report := testutil.RequireMergeConflictReport(t, conflictErr)
	require.Equal(t, lcaCommitID, report.GetLcaCommitId())
	require.Equal(t, uint64(2), report.GetTotalConflicts())
	require.Len(t, report.GetConflicts(), 2)

	_, err = client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: testutil.DefaultBranch,
		NewBranch:    "main-theirs",
		Message:      "fork main for global theirs merge check",
	}))
	require.NoError(t, err)

	sourceROMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId:   diskID,
		Branch:   "feat",
		ReadOnly: true,
	}))
	require.NoError(t, err)

	mergeResp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
		PathStrategies: map[string]storagev1.MergeStrategy{
			"/shared.txt": storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		},
		Message: "resolve conflicts with mixed strategies",
	}))
	require.NoError(t, err)
	require.Equal(t, lcaCommitID, mergeResp.Msg.GetLcaCommitId())
	require.False(t, mergeResp.Msg.GetFastForward())
	require.False(t, mergeResp.Msg.GetUpToDate())
	require.Positive(t, mergeResp.Msg.GetCommitId())
	require.Greater(t, mergeResp.Msg.GetWriterEpoch(), mainEpoch)

	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: sourceROMnt.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	_, staleErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		ParentIno: 1,
		Name:      []byte("stale.txt"),
		Mode:      0o100644,
		Epoch:     mainEpoch,
	}))
	testutil.RequireConnectError(t, staleErr, connect.CodeFailedPrecondition, "EBUSY")

	require.Equal(t, []byte("feat-added\n"), e2eReadFileByName(t, ctx, client, diskID, testutil.DefaultBranch, "feat.txt"))
	require.Equal(t, []byte("feat-edited\n"), e2eReadFileByName(t, ctx, client, diskID, testutil.DefaultBranch, "shared.txt"))
	require.Equal(t, []byte("main-choice\n"), e2eReadFileByName(t, ctx, client, diskID, testutil.DefaultBranch, "ours_or_theirs.txt"))

	theirsMergeResp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: "main-theirs",
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		Message:      "resolve all conflicts with theirs",
	}))
	require.NoError(t, err)
	require.False(t, theirsMergeResp.Msg.GetFastForward())
	require.False(t, theirsMergeResp.Msg.GetUpToDate())
	require.Equal(t, lcaCommitID, theirsMergeResp.Msg.GetLcaCommitId())
	require.Positive(t, theirsMergeResp.Msg.GetCommitId())
	require.Equal(t, []byte("feat-added\n"), e2eReadFileByName(t, ctx, client, diskID, "main-theirs", "feat.txt"))
	require.Equal(t, []byte("feat-edited\n"), e2eReadFileByName(t, ctx, client, diskID, "main-theirs", "shared.txt"))
	require.Equal(t, []byte("feat-choice\n"), e2eReadFileByName(t, ctx, client, diskID, "main-theirs", "ours_or_theirs.txt"))
}

func TestE2E_MergeBranchFastForwardNoFastForwardAndUpToDate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()
	suffix := time.Now().UnixNano()

	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-merge-ff-%d", suffix),
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
	e2eWriteNewFile(t, ctx, client, diskID, testutil.DefaultBranch, mainMnt.Msg.GetEpoch(), "seed.txt", []byte("seed-v1\n"))
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mainMnt.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	forkResp, err := client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: testutil.DefaultBranch,
		NewBranch:    "feat",
		Message:      "fork for ff checks",
	}))
	require.NoError(t, err)
	forkCommitID := forkResp.Msg.GetCommit().GetCommitId()
	mainEpochBeforeNoOp := e2eFindBranch(t, ctx, client, diskID, testutil.DefaultBranch).GetWriterEpoch()

	noopResp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	require.True(t, noopResp.Msg.GetUpToDate())
	require.False(t, noopResp.Msg.GetFastForward())
	require.Equal(t, forkCommitID, noopResp.Msg.GetLcaCommitId())
	require.Equal(t, forkCommitID, noopResp.Msg.GetCommitId())
	require.Equal(t, mainEpochBeforeNoOp, noopResp.Msg.GetWriterEpoch())

	featMnt1, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	e2eWriteNewFile(t, ctx, client, diskID, "feat", featMnt1.Msg.GetEpoch(), "ff.txt", []byte("fast-forward\n"))
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: featMnt1.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	ffCommitResp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  "feat",
		Message: "add ff.txt",
	}))
	require.NoError(t, err)
	ffCommitID := ffCommitResp.Msg.GetCommit().GetCommitId()

	ffResp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	require.True(t, ffResp.Msg.GetFastForward())
	require.False(t, ffResp.Msg.GetUpToDate())
	require.Equal(t, forkCommitID, ffResp.Msg.GetLcaCommitId())
	require.Equal(t, ffCommitID, ffResp.Msg.GetCommitId())
	require.Equal(t, mainEpochBeforeNoOp+1, ffResp.Msg.GetWriterEpoch())
	require.Equal(t, []byte("fast-forward\n"), e2eReadFileByName(t, ctx, client, diskID, testutil.DefaultBranch, "ff.txt"))

	featMnt2, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	e2eWriteNewFile(t, ctx, client, diskID, "feat", featMnt2.Msg.GetEpoch(), "noff.txt", []byte("no-fast-forward\n"))
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: featMnt2.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	noffSrcCommitResp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  "feat",
		Message: "add noff.txt",
	}))
	require.NoError(t, err)
	noffSrcCommitID := noffSrcCommitResp.Msg.GetCommit().GetCommitId()

	noffResp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:        diskID,
		SourceBranch:  "feat",
		TargetBranch:  testutil.DefaultBranch,
		NoFastForward: true,
		Message:       "explicit two-parent merge",
	}))
	require.NoError(t, err)
	require.False(t, noffResp.Msg.GetFastForward())
	require.False(t, noffResp.Msg.GetUpToDate())
	require.Equal(t, ffCommitID, noffResp.Msg.GetLcaCommitId())
	require.NotEqual(t, noffSrcCommitID, noffResp.Msg.GetCommitId())
	require.Equal(t, []byte("no-fast-forward\n"), e2eReadFileByName(t, ctx, client, diskID, testutil.DefaultBranch, "noff.txt"))

	mergeCommits, err := client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
		DiskId: diskID,
		Kind:   storagev1.CommitKind_COMMIT_KIND_MERGE,
	}))
	require.NoError(t, err)
	require.Len(t, mergeCommits.Msg.GetCommits(), 1)
	mergeCmt := mergeCommits.Msg.GetCommits()[0]
	require.Equal(t, noffResp.Msg.GetCommitId(), mergeCmt.GetCommitId())
	require.Equal(t, ffCommitID, mergeCmt.GetParentId())
	require.Equal(t, noffSrcCommitID, mergeCmt.GetMergeParentId())
	require.Equal(t, "explicit two-parent merge", mergeCmt.GetMessage())

	repeatNoOp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:       diskID,
		SourceBranch: "feat",
		TargetBranch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	require.True(t, repeatNoOp.Msg.GetUpToDate())
	require.Equal(t, noffResp.Msg.GetCommitId(), repeatNoOp.Msg.GetCommitId())
}
