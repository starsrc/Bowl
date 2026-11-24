package cli

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestCommit_GivenValidationErrors_ReturnsExit2(t *testing.T) {
	t.Setenv(EnvDiskID, "")

	requireCLIError2(t, "bowl commit [command]", "commit")
	requireCLIError2(t, "disk ID is required", "commit", "list")
	requireCLIError2(t, "cannot be used together", "commit", "create", "-d", testDiskID1, "-q", "--json")
	requireCLIError2(t, "cannot be used together", "commit", "list", "-d", testDiskID1, "-q", "--json")
	requireCLIError2(t, "invalid --kind", "commit", "list", "-d", testDiskID1, "--kind", "bogus")
	requireCLIError2(t, "must be an integer in [1, 1000]", "commit", "list", "-d", testDiskID1, "--limit", "0")
	requireCLIError2(t, "must be an integer in [1, 1000]", "commit", "list", "-d", testDiskID1, "--limit", "1001")
	requireCLIError2(t, "accepts 1 arg(s), received 0", "commit", "delete", "-d", testDiskID1)
	requireCLIError2(t, "invalid commit-id", "commit", "delete", "0", "-d", testDiskID1)
	requireCLIError2(t, "invalid commit-id", "commit", "delete", "not-num", "-d", testDiskID1)
}

func TestCommit_GivenCRUDAndPrune_HandlesQuietJSONAndFilters(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "commit-disk", 1<<30, 0)
	storage.seedBranch(testDiskID1, "feat", 1, 0)
	storage.pruneReclaimedCount = 3
	withDiskStorageFake(t, storage)

	code, qOut, errOut := runCLI("commit", "create", "-d", testDiskID1, "--branch", "feat", "-m", "checkpoint", "-q")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "2\n", qOut)
	require.Equal(t, "feat", storage.lastCreateCommitReq.GetBranch())
	require.Equal(t, "checkpoint", storage.lastCreateCommitReq.GetMessage())

	storage.seedCommit(&storagev1.Commit{
		DiskId:    testDiskID1,
		CommitId:  5,
		ParentId:  2,
		Kind:      storagev1.CommitKind_COMMIT_KIND_MERGE,
		Message:   "merge feat",
		CreatedAt: timestamppb.New(time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)),
	})

	code, listQuietOut, errOut := runCLI("commit", "ls", "-d", testDiskID1, "-q")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "5\n2\n1\n", listQuietOut)

	code, filteredOut, errOut := runCLI("commit", "list", "-d", testDiskID1, "--kind", "init", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var listRes commitListJSON
	require.NoError(t, json.Unmarshal([]byte(filteredOut), &listRes))
	require.Len(t, listRes.Commits, 1)
	require.Equal(t, uint64(1), listRes.Commits[0].CommitID)
	require.Equal(t, "init", listRes.Commits[0].Kind)

	withDiskStdin(t, strings.NewReader("n\n"))
	code, _, errOut = runCLI("commit", "rm", "5", "-d", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "Operation canceled")
	require.Zero(t, storage.deleteCommitCalls)

	code, delOut, errOut := runCLI("commit", "delete", "5", "-d", testDiskID1, "-y", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var delRes commitDeleteJSON
	require.NoError(t, json.Unmarshal([]byte(delOut), &delRes))
	require.Equal(t, uint64(5), delRes.CommitID)
	require.True(t, delRes.Deleted)

	code, pruneOut, errOut := runCLI("commit", "prune", "-d", testDiskID1, "-y")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, pruneOut, "Pruned 3 unreachable commit(s)")

	code, pruneJSONOut, errOut := runCLI("commit", "prune", "-d", testDiskID1, "-y", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var pruneRes commitPruneJSON
	require.NoError(t, json.Unmarshal([]byte(pruneJSONOut), &pruneRes))
	require.Equal(t, uint32(3), pruneRes.ReclaimedCount)
}
