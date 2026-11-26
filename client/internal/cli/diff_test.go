package cli

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestDiff_GivenInvalidOrAmbiguousModes_ReturnsExit2(t *testing.T) {
	t.Setenv(EnvDiskID, "")

	requireCLIError2(t, "disk ID is required", "diff", "main..feat")
	requireCLIError2(t, "specify one diff mode", "diff", "-d", testDiskID1)
	requireCLIError2(t, "cannot combine", "diff", "main..feat", "--work", "-d", testDiskID1)
	requireCLIError2(t, "cannot combine", "diff", "main..feat", "--commits", "1..2", "-d", testDiskID1)
	requireCLIError2(t, "cannot combine", "diff", "--work", "--commits", "1..2", "-d", testDiskID1)
	requireCLIError2(t, "--branch can only be used with --work", "diff", "main..feat", "--branch", "main", "-d", testDiskID1)
	requireCLIError2(t, "expected <left>..<right>", "diff", "main", "-d", testDiskID1)
	requireCLIError2(t, "both sides of '..' are required", "diff", "main..", "-d", testDiskID1)
	requireCLIError2(t, "must be different", "diff", "main..main", "-d", testDiskID1)
	requireCLIError2(t, "invalid base commit ID", "diff", "--commits", "0..2", "-d", testDiskID1)
	requireCLIError2(t, "must be different", "diff", "--commits", "2..2", "-d", testDiskID1)
}

func TestDiff_GivenBranchCommitAndWorkModesWithPatch_ReadsExpectedNodes(t *testing.T) {
	rt := defaultCLIRuntime()
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "diff-disk", 1<<30, 0)
	setDiskStorageFake(rt, storage)

	t.Run("Mode1BranchDiffWithPatch", func(t *testing.T) {
		storage.seedCommitFile(testDiskID1, 10, 100, 0o100644, []byte("hello\nold\n"))
		storage.seedCommitFile(testDiskID1, 20, 200, 0o100644, []byte("hello\nnew\n"))
		storage.diffResponse = &storagev1.DiffResponse{
			BaseCommitId:   5,
			TargetCommitId: 10,
			SourceCommitId: 20,
			Summary:        &storagev1.DiffSummary{Modified: 1},
			Entries: []*storagev1.DiffEntry{
				{
					Path:   []byte("/notes.txt"),
					Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
					Target: &storagev1.DiffNodeSummary{Ino: 100, Mode: 0o100644, Size: 10},
					Source: &storagev1.DiffNodeSummary{Ino: 200, Mode: 0o100644, Size: 10},
				},
			},
		}

		code, out, errOut := runCLIWithRuntime(rt, "diff", "main..feat", "-p", "--prefix", "/notes", "--conflicts-only", "--limit", "10", "-d", testDiskID1)
		require.Equal(t, 0, code, errOut)
		require.Empty(t, errOut)
		require.Equal(t, "feat", storage.lastDiffBranchesReq.GetSourceBranch())
		require.Equal(t, "main", storage.lastDiffBranchesReq.GetTargetBranch())
		require.Equal(t, []byte("/notes"), storage.lastDiffBranchesReq.GetPathPrefix())
		require.True(t, storage.lastDiffBranchesReq.GetConflictsOnly())
		require.Equal(t, uint32(10), storage.lastDiffBranchesReq.GetPageSize())
		require.Contains(t, out, "--- a/notes.txt")
		require.Contains(t, out, "+++ b/notes.txt")
		require.Contains(t, out, "-old")
		require.Contains(t, out, "+new")
		require.Equal(t, 2, storage.readCommitNodeCalls)
	})

	t.Run("Mode2CommitDiffWithPatch", func(t *testing.T) {
		storage.seedCommitFile(testDiskID1, 3, 300, 0o100644, []byte("v1\n"))
		storage.seedCommitFile(testDiskID1, 7, 301, 0o100644, []byte("v2\n"))
		storage.diffResponse = &storagev1.DiffResponse{
			BaseCommitId:   3,
			SourceCommitId: 7,
			Summary:        &storagev1.DiffSummary{Modified: 1},
			Entries: []*storagev1.DiffEntry{
				{
					Path:   []byte("/ver.txt"),
					Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
					Base:   &storagev1.DiffNodeSummary{Ino: 300, Mode: 0o100644, Size: 3},
					Source: &storagev1.DiffNodeSummary{Ino: 301, Mode: 0o100644, Size: 3},
				},
			},
		}

		code, jsonOut, errOut := runCLIWithRuntime(rt, "diff", "--commits", "3..7", "-p", "--json", "-d", testDiskID1)
		require.Equal(t, 0, code, errOut)
		require.Empty(t, errOut)
		require.Equal(t, uint64(3), storage.lastDiffCommitsReq.GetBaseCommitId())
		require.Equal(t, uint64(7), storage.lastDiffCommitsReq.GetSourceCommitId())
		var res diffResultJSON
		require.NoError(t, json.Unmarshal([]byte(jsonOut), &res))
		require.Equal(t, "commits", res.Mode)
		require.Len(t, res.Entries, 1)
		require.Contains(t, res.Entries[0].Patch, "-v1")
		require.Contains(t, res.Entries[0].Patch, "+v2")
	})

	t.Run("Mode3WorkDiffWithLiveReadAndReadlink", func(t *testing.T) {
		storage.seedCommitFile(testDiskID1, 9, 400, 0o100644, []byte("committed\n"))
		storage.seedLiveFile(testDiskID1, "dev", 401, []byte("dirty\n"))
		storage.seedCommitSymlink(testDiskID1, 9, 500, "/old")
		storage.seedLiveSymlink(testDiskID1, "dev", 501, "/new")
		storage.diffResponse = &storagev1.DiffResponse{
			BaseCommitId: 9,
			Summary:      &storagev1.DiffSummary{Modified: 2},
			Entries: []*storagev1.DiffEntry{
				{
					Path:   []byte("/work.txt"),
					Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
					Base:   &storagev1.DiffNodeSummary{Ino: 400, Mode: 0o100644, Size: 10},
					Source: &storagev1.DiffNodeSummary{Ino: 401, Mode: 0o100644, Size: 6},
				},
				{
					Path:   []byte("/sym"),
					Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
					Base:   &storagev1.DiffNodeSummary{Ino: 500, Mode: 0o120777, Size: 4},
					Source: &storagev1.DiffNodeSummary{Ino: 501, Mode: 0o120777, Size: 4},
				},
			},
		}

		t.Setenv(EnvDiskID, testDiskID1)
		code, out, errOut := runCLIWithRuntime(rt, "diff", "--work", "--branch", "dev", "-p")
		require.Equal(t, 0, code, errOut)
		require.Empty(t, errOut)
		require.Equal(t, "dev", storage.lastDiffWorkReq.GetBranch())
		require.Contains(t, out, "-committed")
		require.Contains(t, out, "+dirty")
		require.Contains(t, out, "-/old")
		require.Contains(t, out, "+/new")
		require.Positive(t, storage.readCalls)
		require.Positive(t, storage.readlinkCalls)
	})
}
