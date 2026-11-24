package cli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestBranch_GivenValidationAndDiskScopeErrors_ReturnsExit2(t *testing.T) {
	t.Setenv(EnvDiskID, "")

	requireCLIError2(t, "bowl branch [command]", "branch")
	requireCLIError2(t, `unknown command "bogus" for "bowl branch"`, "branch", "bogus")
	requireCLIError2(t, "disk ID is required", "branch", "list")
	requireCLIError2(t, "invalid disk-id", "branch", "list", "-d", "not-a-uuid")
	requireCLIError2(t, "invalid disk-id", "branch", "list", "--disk", testNilUUID)
	requireCLIError2(t, "cannot be used together", "branch", "list", "-d", testDiskID1, "-q", "--json")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "branch", "create", "-d", testDiskID1)
	requireCLIError2(t, "must be different", "branch", "create", "main", "-d", testDiskID1)
	requireCLIError2(t, "must be different", "branch", "fork", "dev", "--from", "dev", "-d", testDiskID1)
	requireCLIError2(t, "cannot be used together", "branch", "create", "dev", "-d", testDiskID1, "-q", "--json")
	requireCLIError2(t, "cannot exceed 255 bytes", "branch", "create", strings.Repeat("b", 256), "-d", testDiskID1)
	requireCLIError2(t, "cannot contain NUL", "branch", "create", "bad\x00br", "-d", testDiskID1)
	requireCLIError2(t, "cannot contain NUL", "branch", "create", "dev", "-m", "bad\x00msg", "-d", testDiskID1)

	requireCLIError2(t, "accepts 1 arg(s), received 0", "branch", "delete", "-d", testDiskID1)
	requireCLIError2(t, "accepts 1 arg(s), received 0", "branch", "reset", "-d", testDiskID1)
	requireCLIError2(t, "--to is required", "branch", "reset", "main", "-d", testDiskID1)
	requireCLIError2(t, "invalid --to", "branch", "reset", "main", "--to", "0", "-d", testDiskID1)
	requireCLIError2(t, "invalid --to", "branch", "reset", "main", "--to", "abc", "-d", testDiskID1)

	requireCLIError2(t, "accepts 1 arg(s), received 0", "branch", "merge", "-d", testDiskID1)
	requireCLIError2(t, "--into is required", "branch", "merge", "feat", "-d", testDiskID1)
	requireCLIError2(t, "cannot be the same", "branch", "merge", "main", "--into", "main", "-d", testDiskID1)
	requireCLIError2(t, "invalid --strategy", "branch", "merge", "feat", "--into", "main", "--strategy", "bad", "-d", testDiskID1)
	requireCLIError2(t, "invalid --resolve", "branch", "merge", "feat", "--into", "main", "--resolve", "missing-eq", "-d", testDiskID1)
	requireCLIError2(t, "strategy must be ours or theirs", "branch", "merge", "feat", "--into", "main", "--resolve", "/a.txt=fail", "-d", testDiskID1)
	requireCLIError2(t, "unknown flag: --force", "branch", "merge", "feat", "--into", "main", "--force", "-d", testDiskID1)
	requireCLIError2(t, "cannot be used together", "branch", "merge", "feat", "--into", "main", "-q", "--json", "-d", testDiskID1)
}

func TestBranch_GivenLifecycleOps_ListsCreatesResetsAndDeletesWithMountGuard(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "repo", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	t.Setenv(EnvDiskID, testDiskID1)

	code, createOut, errOut := runCLI("branch", "fork", "feat", "--from", "main", "-m", "start feat", "-q")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "feat\n", createOut)
	require.Equal(t, "main", storage.lastForkBranchReq.GetSourceBranch())
	require.Equal(t, "feat", storage.lastForkBranchReq.GetNewBranch())
	require.Equal(t, "start feat", storage.lastForkBranchReq.GetMessage())

	code, createJSONOut, errOut := runCLI("branch", "create", "dev", "--from", "feat", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var created branchCreateJSON
	require.NoError(t, json.Unmarshal([]byte(createJSONOut), &created))
	require.Equal(t, "dev", created.Branch.Name)
	require.Equal(t, "fork", created.Commit.Kind)

	code, listQuietOut, errOut := runCLI("branch", "ls", "-q")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "dev\nfeat\nmain\n", listQuietOut)

	code, listJSONOut, errOut := runCLI("branch", "list", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var listed branchListJSON
	require.NoError(t, json.Unmarshal([]byte(listJSONOut), &listed))
	require.Equal(t, testDiskID1, listed.DiskID)
	require.Len(t, listed.Branches, 3)

	withMountInfoContent(t, "100 30 0:50 / /mnt/feat rw - fuse.bowl bowl:"+testDiskID1+"@feat rw\n")
	code, _, errOut = runCLI("branch", "delete", "feat", "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "bowl umount /mnt/feat")
	require.Zero(t, storage.deleteBranchCalls)

	code, _, errOut = runCLI("branch", "reset", "feat", "--to", "1", "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "bowl umount /mnt/feat")
	require.Zero(t, storage.resetBranchCalls)

	code, resetOut, errOut := runCLI("branch", "reset", "dev", "--to", "1", "-y", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var resetRes branchJSON
	require.NoError(t, json.Unmarshal([]byte(resetOut), &resetRes))
	require.Equal(t, "dev", resetRes.Name)
	require.Equal(t, uint64(1), resetRes.HeadCommitID)

	withDiskStdin(t, strings.NewReader("n\n"))
	code, _, errOut = runCLI("branch", "rm", "dev")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "Operation canceled")
	require.Zero(t, storage.deleteBranchCalls)

	code, delOut, errOut := runCLI("branch", "rm", "dev", "-y", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var delRes branchDeleteJSON
	require.NoError(t, json.Unmarshal([]byte(delOut), &delRes))
	require.Equal(t, "dev", delRes.Branch)
	require.True(t, delRes.Deleted)
}

func TestBranchMerge_GivenSuccessDirtyAndConflict_HandlesQuietJSONAndDiagnostics(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "merge-disk", 1<<30, 0)
	storage.seedBranch(testDiskID1, "feat", 2, 0)
	withDiskStorageFake(t, storage)

	code, qOut, errOut := runCLI(
		"branch", "merge", "feat",
		"--into", "main",
		"--strategy", "ours",
		"--resolve", "/app.conf=theirs",
		"--no-ff",
		"-m", "merge feat",
		"-q",
		"-d", testDiskID1,
	)
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "2\n", qOut)
	require.Equal(t, "feat", storage.lastMergeBranchReq.GetSourceBranch())
	require.Equal(t, "main", storage.lastMergeBranchReq.GetTargetBranch())
	require.Equal(t, storagev1.MergeStrategy_MERGE_STRATEGY_OURS, storage.lastMergeBranchReq.GetStrategy())
	require.Equal(t, storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS, storage.lastMergeBranchReq.GetPathStrategies()["/app.conf"])
	require.True(t, storage.lastMergeBranchReq.GetNoFastForward())
	require.Equal(t, "merge feat", storage.lastMergeBranchReq.GetMessage())

	dirtyErr := connect.NewError(connect.CodeFailedPrecondition, errors.New("branch has dirty work layer"))
	dirtyDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: "bowl.storage",
		Reason: "EDIRTY",
	})
	require.NoError(t, err)
	dirtyErr.AddDetail(dirtyDetail)
	storage.forcedErr["MergeBranch"] = dirtyErr

	code, _, errOut = runCLI("branch", "merge", "feat", "--into", "main", "-d", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "EDIRTY")
	require.Contains(t, errOut, "bowl commit create")
	require.Contains(t, errOut, "bowl branch reset")

	conflictErr := connect.NewError(connect.CodeAborted, errors.New("merge conflict"))
	infoDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: "bowl.storage",
		Reason: "ECONFLICT",
	})
	require.NoError(t, err)
	reportDetail, err := connect.NewErrorDetail(&storagev1.MergeConflictReport{
		LcaCommitId:        11,
		TotalConflicts:     2,
		ConflictsTruncated: true,
		Conflicts: []*storagev1.MergeConflict{
			{
				Path:   []byte("/conflict.txt"),
				Kind:   storagev1.ConflictKind_CONFLICT_KIND_CONTENT,
				Base:   &storagev1.DiffNodeSummary{Ino: 2, Mode: 0o100644, Size: 10},
				Target: &storagev1.DiffNodeSummary{Ino: 3, Mode: 0o100644, Size: 20},
				Source: &storagev1.DiffNodeSummary{Ino: 4, Mode: 0o100644, Size: 30},
			},
		},
	})
	require.NoError(t, err)
	conflictErr.AddDetail(infoDetail)
	conflictErr.AddDetail(reportDetail)
	storage.forcedErr["MergeBranch"] = conflictErr

	code, out, errOut := runCLI("branch", "merge", "feat", "--into", "main", "-d", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "ECONFLICT")
	require.Contains(t, errOut, "LCA Commit: 11")
	require.Contains(t, errOut, "/conflict.txt")
	require.Contains(t, errOut, "bowl diff -d "+testDiskID1+" main..feat --conflicts-only")

	code, jsonOut, errOut := runCLI("branch", "merge", "feat", "--into", "main", "-d", testDiskID1, "--json")
	require.Equal(t, 1, code)
	require.Empty(t, errOut)
	var mergeRes branchMergeJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &mergeRes))
	require.False(t, mergeRes.Merged)
	require.Equal(t, uint64(11), mergeRes.LCACommitID)
	require.NotNil(t, mergeRes.ConflictReport)
	require.Equal(t, uint64(11), mergeRes.ConflictReport.LCACommitID)
	require.Equal(t, uint64(2), mergeRes.ConflictReport.TotalConflicts)
}

func TestMount_GivenBranchFlag_CallsMountBranchAndPersistsBranchInMountMeta(t *testing.T) {
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, false)
	withMissingMountInfo(t)
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)
	mp := t.TempDir()
	stageDir := t.TempDir()

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "branch-mount", 1<<30, 0)
	storage.seedBranch(testDiskID1, "dev", 1, 0)
	withMountStorageFake(t, storage)

	var gotMeta mountMeta
	var gotMetaOK bool
	var gotCfg fusefs.Config
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, _ string, ready func()) error {
		gotCfg = *cfg
		ready()
		gotMeta, gotMetaOK = findMountMeta(mp)
		return nil
	})

	code, _, errOut := runCLI(
		"--endpoint", "http://127.0.0.1:9000",
		"mount", testDiskID1, mp,
		"--branch", "dev",
		"-f",
		"--stage-dir", stageDir,
		"--admin-addr", "off",
	)
	require.Equal(t, 0, code, errOut)
	require.True(t, gotMetaOK)
	require.Equal(t, "dev", gotMeta.Branch)
	require.Equal(t, "dev", gotCfg.Branch)
	reqs := storage.getMountReqs()
	require.Len(t, reqs, 1)
	require.Equal(t, testDiskID1, reqs[0].GetDiskId())
	require.Equal(t, "dev", reqs[0].GetBranch())
}
