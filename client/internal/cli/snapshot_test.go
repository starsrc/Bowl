package cli

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	testSnapID1 = "01950000-0000-7000-a000-000000000001"
	testSnapID2 = "01950000-0000-7000-a000-000000000002"
	testSnapID3 = "01950000-0000-7000-a000-000000000003"
	testSnapID4 = "01950000-0000-7000-a000-000000000004"
)

func connectErrWithReason(t *testing.T, code connect.Code, msg, reason string) error {
	t.Helper()
	err := connect.NewError(code, errors.New(msg))
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: reason})
	require.NoError(t, detailErr)
	err.AddDetail(detail)
	return err
}

func TestSnapshot_GivenHelpAndRoutingArgs_ReturnsExpectedUsageAndExitCodes(t *testing.T) {
	requireCLIError2(t, "bowl snapshot [command]", "snapshot")
	requireCLIError2(t, `unknown command "unknown" for "bowl snapshot"`, "snapshot", "unknown")

	for _, args := range [][]string{{"help", "snapshot"}, {"snapshot", "-h"}, {"snapshot", "--help"}} {
		for _, sub := range []string{"create", "list", "delete", "prune"} {
			requireHelpExit0(t, sub, args...)
		}
	}

	requireHelpExit0(t, "bowl snapshot create <disk-id>", "snapshot", "create", "-h")
	requireHelpExit0(t, "list, ls", "snapshot", "list", "-h")
	requireHelpExit0(t, "list, ls", "snapshot", "ls", "--help")
	requireHelpExit0(t, "delete, rm", "snapshot", "delete", "-h")
	requireHelpExit0(t, "delete, rm", "snapshot", "rm", "--help")
	requireHelpExit0(t, "bowl snapshot prune <disk-id>", "snapshot", "prune", "-h")
}

func TestSnapshot_GivenInvalidValidationInputs_ReturnsExit2WithDiagnostics(t *testing.T) {
	requireCLIError2(t, "accepts 1 arg(s), received 0", "snapshot", "create")
	requireCLIError2(t, "accepts 1 arg(s), received 2", "snapshot", "create", testDiskID1, "extra")
	requireCLIError2(t, "invalid disk-id", "snapshot", "create", "not-a-uuid")
	requireCLIError2(t, "invalid disk-id", "snapshot", "create", testNilUUID)
	requireCLIError2(t, "cannot be used together", "snapshot", "create", testDiskID1, "-q", "--json")
	requireCLIError2(t, "cannot exceed 255 bytes", "snapshot", "create", testDiskID1, "--name", strings.Repeat("a", 256))
	requireCLIError2(t, "cannot contain NUL", "snapshot", "create", testDiskID1, "--name", "bad\x00name")
	requireCLIError2(t, "invalid endpoint", "snapshot", "create", testDiskID1, "--endpoint", "ftp://127.0.0.1:9000")
	requireCLIError2(t, "unknown flag: --token", "snapshot", "create", testDiskID1, "--token", "snap-tok")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "snapshot", "list")
	requireCLIError2(t, "accepts 1 arg(s), received 2", "snapshot", "ls", testDiskID1, "extra")
	requireCLIError2(t, "invalid disk-id", "snapshot", "list", "bad")
	requireCLIError2(t, "invalid disk-id", "snapshot", "list", testNilUUID)
	requireCLIError2(t, "unknown shorthand flag", "snapshot", "list", testDiskID1, "-q")
	requireCLIError2(t, "invalid --kind", "snapshot", "list", testDiskID1, "--kind", "bogus")
	requireCLIError2(t, "invalid --kind", "snapshot", "list", testDiskID1, "--kind", "")
	requireCLIError2(t, "invalid --page-size", "snapshot", "list", testDiskID1, "--page-size", "0")
	requireCLIError2(t, "invalid --page-size", "snapshot", "list", testDiskID1, "--page-size", "1001")
	requireCLIError2(t, "Error:", "snapshot", "list", testDiskID1, "--page-size", "-1")
	requireCLIError2(t, "--page-token cannot exceed", "snapshot", "list", testDiskID1, "--page-token", strings.Repeat("t", 4097))
	requireCLIError2(t, "--page-token cannot contain NUL", "snapshot", "list", testDiskID1, "--page-token", "tok\x00en")

	requireCLIError2(t, "accepts 2 arg(s), received 0", "snapshot", "delete")
	requireCLIError2(t, "accepts 2 arg(s), received 1", "snapshot", "delete", testDiskID1)
	requireCLIError2(t, "accepts 2 arg(s), received 3", "snapshot", "rm", testDiskID1, testSnapID1, "extra")
	requireCLIError2(t, "invalid disk-id", "snapshot", "delete", "bad", testSnapID1)
	requireCLIError2(t, "invalid disk-id", "snapshot", "delete", testNilUUID, testSnapID1)
	requireCLIError2(t, "invalid snapshot-id", "snapshot", "delete", testDiskID1, "bad")
	requireCLIError2(t, "invalid snapshot-id", "snapshot", "delete", testDiskID1, testNilUUID)
	requireCLIError2(t, "unknown shorthand flag", "snapshot", "delete", testDiskID1, testSnapID1, "-q")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "snapshot", "prune")
	requireCLIError2(t, "accepts 1 arg(s), received 2", "snapshot", "prune", testDiskID1, "extra")
	requireCLIError2(t, "invalid disk-id", "snapshot", "prune", "bad")
	requireCLIError2(t, "invalid disk-id", "snapshot", "prune", testNilUUID)
	requireCLIError2(t, "unknown shorthand flag", "snapshot", "prune", testDiskID1, "-q")
}

func TestSnapshotCreate_GivenNamedUnnamedQuietAndJSON_RendersExpectedOutputs(t *testing.T) {
	t.Setenv("BOWL_TOKEN", "env-snap-tok")
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "work", 1<<30, 0)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("snapshot", "create", testDiskID1, "--name", "before-upgrade")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Empty(t, storage.lastAuth)
	require.Equal(t, "before-upgrade", storage.lastCreateSnapshotReq.GetName())
	require.Contains(t, out, "Snapshot ID:")
	require.Contains(t, out, "Source Disk:   "+testDiskID1)
	require.Contains(t, out, "Name:          before-upgrade")
	require.Contains(t, out, "Kind:          manual")
	require.Contains(t, out, "Parent:")
	require.NotContains(t, out, "Merge Parent:")
	require.Contains(t, out, "Protected:     false")
	require.Contains(t, out, "Created At:")

	code, unnamedOut, errOut := runCLI("snapshot", "create", testDiskID1)
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.NotContains(t, unnamedOut, "Name:")

	code, quietOut, errOut := runCLI("snapshot", "create", testDiskID1, "--name", "quiet-snap", "-q")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Len(t, strings.TrimSpace(quietOut), 36)

	code, jsonOut, errOut := runCLI("snapshot", "create", testDiskID1, "--name", "json-snap", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var parsed snapshotJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &parsed))
	require.Equal(t, testDiskID1, parsed.SourceDiskID)
	require.Equal(t, "manual", parsed.Kind)
	require.Equal(t, "json-snap", parsed.Name)
	require.False(t, parsed.Protected)
	require.NotEmpty(t, parsed.SnapshotID)
	require.NotEmpty(t, parsed.CreatedAt)
	require.NotContains(t, jsonOut, "parent_snapshot_id")
	require.NotContains(t, jsonOut, "merge_parent_snapshot_id")

	code, unnamedJSON, errOut := runCLI("snapshot", "create", testDiskID1, "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.NotContains(t, unnamedJSON, `"name"`)

	var detailBuf strings.Builder
	printSnapshotDetail(&detailBuf, &storagev1.Snapshot{
		SnapshotId:            testSnapID3,
		SourceDiskId:          testDiskID1,
		ParentSnapshotId:      testSnapID1,
		MergeParentSnapshotId: testSnapID2,
		Kind:                  storagev1.SnapshotKind_SNAPSHOT_KIND_MERGE,
		Name:                  "merge-snap",
		Protected:             true,
		CreatedAt:             timestamppb.New(time.Date(2026, 11, 21, 10, 5, 0, 0, time.UTC)),
	})
	detail := detailBuf.String()
	require.Contains(t, detail, "Snapshot ID:   "+testSnapID3)
	require.Contains(t, detail, "Parent:        "+testSnapID1)
	require.Contains(t, detail, "Merge Parent:  "+testSnapID2)
	require.Contains(t, detail, "Kind:          merge")
	require.Contains(t, detail, "Protected:     true")
	require.Equal(t, "unknown", snapshotKindLabel(storagev1.SnapshotKind_SNAPSHOT_KIND_UNSPECIFIED))
}

func TestSnapshotList_GivenSeededSnapshotsFiltersAndPagination_RendersTableAndJSON(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "primary", 1<<30, 0)
	storage.seedDisk(testDiskID2, "empty", 1<<30, 0)
	t1 := timestamppb.New(time.Date(2026, 11, 21, 10, 0, 1, 0, time.UTC))
	t2 := timestamppb.New(time.Date(2026, 11, 21, 10, 0, 2, 0, time.UTC))
	t3 := timestamppb.New(time.Date(2026, 11, 21, 10, 0, 3, 0, time.UTC))
	storage.seedSnapshot(&storagev1.Snapshot{
		SnapshotId:   testSnapID1,
		SourceDiskId: testDiskID1,
		Kind:         storagev1.SnapshotKind_SNAPSHOT_KIND_FORK,
		Protected:    true,
		CreatedAt:    t1,
	})
	storage.seedSnapshot(&storagev1.Snapshot{
		SnapshotId:            testSnapID2,
		SourceDiskId:          testDiskID1,
		ParentSnapshotId:      testSnapID1,
		MergeParentSnapshotId: testSnapID4,
		Kind:                  storagev1.SnapshotKind_SNAPSHOT_KIND_MERGE,
		Protected:             false,
		CreatedAt:             t2,
	})
	storage.seedSnapshot(&storagev1.Snapshot{
		SnapshotId:       testSnapID3,
		SourceDiskId:     testDiskID1,
		ParentSnapshotId: testSnapID2,
		Kind:             storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL,
		Name:             "checkpoint-1",
		Protected:        false,
		CreatedAt:        t3,
	})
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("snapshot", "list", testDiskID1, "--page-size", "2")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, uint32(2), storage.lastListSnapshotsReq.GetPageSize())
	require.Contains(t, out, "SNAPSHOT ID")
	require.Contains(t, out, "KIND")
	require.Contains(t, out, "PROTECTED")
	require.Contains(t, out, "NAME")
	require.Contains(t, out, "SOURCE DISK")
	require.Contains(t, out, "CREATED AT")
	require.Contains(t, out, testSnapID3)
	require.Contains(t, out, "checkpoint-1")
	require.Contains(t, out, testSnapID2)
	require.Contains(t, out, "Next Page Token: "+testSnapID1)

	code, page2JSON, errOut := runCLI("snapshot", "ls", testDiskID1, "--page-size", "2", "--page-token", testSnapID1, "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var listRes snapshotListJSON
	require.NoError(t, json.Unmarshal([]byte(page2JSON), &listRes))
	require.Len(t, listRes.Snapshots, 1)
	require.Equal(t, testSnapID1, listRes.Snapshots[0].SnapshotID)
	require.Equal(t, "fork", listRes.Snapshots[0].Kind)
	require.True(t, listRes.Snapshots[0].Protected)
	require.Empty(t, listRes.NextPageToken)

	code, kindJSON, errOut := runCLI("snapshot", "list", testDiskID1, "--kind", "MANUAL", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL, storage.lastListSnapshotsReq.GetKind())
	var manualRes snapshotListJSON
	require.NoError(t, json.Unmarshal([]byte(kindJSON), &manualRes))
	require.Len(t, manualRes.Snapshots, 1)
	require.Equal(t, testSnapID3, manualRes.Snapshots[0].SnapshotID)
	require.Equal(t, "manual", manualRes.Snapshots[0].Kind)
	require.Equal(t, "checkpoint-1", manualRes.Snapshots[0].Name)

	code, mergeJSON, errOut := runCLI("snapshot", "list", testDiskID1, "--kind", "merge", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, storagev1.SnapshotKind_SNAPSHOT_KIND_MERGE, storage.lastListSnapshotsReq.GetKind())
	var mergeRes snapshotListJSON
	require.NoError(t, json.Unmarshal([]byte(mergeJSON), &mergeRes))
	require.Len(t, mergeRes.Snapshots, 1)
	require.Equal(t, testSnapID1, mergeRes.Snapshots[0].ParentSnapshotID)
	require.Equal(t, testSnapID4, mergeRes.Snapshots[0].MergeParentSnapshotID)

	code, forkOut, errOut := runCLI("snapshot", "list", testDiskID1, "--kind", "fork")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, storagev1.SnapshotKind_SNAPSHOT_KIND_FORK, storage.lastListSnapshotsReq.GetKind())
	require.Contains(t, forkOut, testSnapID1)

	code, emptyOut, errOut := runCLI("snapshot", "list", testDiskID2)
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "No snapshots found.\n", emptyOut)

	code, emptyJSON, errOut := runCLI("snapshot", "list", testDiskID2, "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, emptyJSON, `"snapshots": []`)
}

func TestSnapshotDelete_GivenConfirmationYesJSONAndErrors_HandlesAllPaths(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "primary", 1<<30, 0)
	storage.seedSnapshot(&storagev1.Snapshot{
		SnapshotId:   testSnapID1,
		SourceDiskId: testDiskID1,
		Kind:         storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL,
		Name:         "snap-1",
	})
	storage.seedSnapshot(&storagev1.Snapshot{
		SnapshotId:   testSnapID2,
		SourceDiskId: testDiskID1,
		Kind:         storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL,
		Name:         "snap-2",
	})
	withDiskStorageFake(t, storage)

	withDiskStdin(t, strings.NewReader("n\n"))
	code, out, errOut := runCLI("snapshot", "delete", testDiskID1, testSnapID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Delete snapshot "+testSnapID1+" on disk "+testDiskID1+"? This action is irreversible [y/N]: ")
	require.Contains(t, errOut, "Operation canceled")
	require.NotContains(t, errOut, "Error:")
	require.Equal(t, 0, storage.deleteSnapshotCalls)

	withDiskStdin(t, strings.NewReader(""))
	code, out, errOut = runCLI("snapshot", "delete", testDiskID1, testSnapID1, "--yes=false")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Operation canceled")
	require.Equal(t, 0, storage.deleteSnapshotCalls)

	withDiskStdin(t, errReader{err: errors.New("stdin failed")})
	code, out, errOut = runCLI("snapshot", "delete", testDiskID1, testSnapID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "read confirmation input")
	require.Equal(t, 0, storage.deleteSnapshotCalls)

	withDiskStdin(t, strings.NewReader("yes\n"))
	code, out, errOut = runCLI("snapshot", "rm", testDiskID1, testSnapID1)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, errOut, "Delete snapshot "+testSnapID1+" on disk "+testDiskID1+"? This action is irreversible [y/N]: ")
	require.Equal(t, "Deleted snapshot "+testSnapID1+"\n", out)
	require.Equal(t, 1, storage.deleteSnapshotCalls)

	code, jsonOut, errOut := runCLI("snapshot", "delete", testDiskID1, testSnapID2, "--yes", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var delRes snapshotDeleteJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &delRes))
	require.Equal(t, testDiskID1, delRes.DiskID)
	require.Equal(t, testSnapID2, delRes.SnapshotID)
	require.True(t, delRes.Deleted)
	require.Equal(t, 2, storage.deleteSnapshotCalls)

	storage.forcedErr["DeleteSnapshot"] = connectErrWithReason(t, connect.CodeFailedPrecondition, "protected", "EBUSY")
	code, out, errOut = runCLI("snapshot", "delete", testDiskID1, testSnapID3, "-y")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "protected")
	require.Contains(t, errOut, "bowl snapshot list "+testDiskID1)

	storage.forcedErr["DeleteSnapshot"] = connectErrWithReason(t, connect.CodeFailedPrecondition, "wrong lineage", "EXDEV")
	code, out, errOut = runCLI("snapshot", "delete", testDiskID1, testSnapID3, "-y")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "lineage")
	require.Contains(t, errOut, testDiskID1)

	storage.forcedErr["DeleteSnapshot"] = connect.NewError(connect.CodeFailedPrecondition, errors.New("other precondition"))
	code, out, errOut = runCLI("snapshot", "delete", testDiskID1, testSnapID3, "-y")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "delete snapshot precondition failed")

	storage.forcedErr["DeleteSnapshot"] = connect.NewError(connect.CodeInvalidArgument, errors.New("bad id"))
	code, out, errOut = runCLI("snapshot", "delete", testDiskID1, testSnapID3, "-y")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "invalid request argument")

	storage.forcedErr["DeleteSnapshot"] = connect.NewError(connect.CodeInternal, errors.New("delete boom"))
	code, out, errOut = runCLI("snapshot", "delete", testDiskID1, testSnapID3, "-y")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "delete snapshot failed")

	delete(storage.forcedErr, "DeleteSnapshot")
	code, out, errOut = runCLI("snapshot", "delete", testDiskID1, testSnapID3, "-y")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "not found")
	require.Contains(t, errOut, testSnapID3)
}

func TestSnapshotPrune_GivenDryRunConfirmationYesAndEmpty_HandlesAllPaths(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "primary", 1<<30, 0)
	storage.seedDisk(testDiskID2, "clean", 1<<30, 0)
	now := timestamppb.New(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
	storage.seedSnapshot(&storagev1.Snapshot{
		SnapshotId:   testSnapID1,
		SourceDiskId: testDiskID1,
		Kind:         storagev1.SnapshotKind_SNAPSHOT_KIND_FORK,
		Protected:    false,
		CreatedAt:    now,
	})
	storage.seedSnapshot(&storagev1.Snapshot{
		SnapshotId:   testSnapID2,
		SourceDiskId: testDiskID1,
		Kind:         storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL,
		Name:         "keep-manual",
		Protected:    false,
		CreatedAt:    now,
	})
	storage.seedSnapshot(&storagev1.Snapshot{
		SnapshotId:   testSnapID3,
		SourceDiskId: testDiskID1,
		Kind:         storagev1.SnapshotKind_SNAPSHOT_KIND_FORK,
		Protected:    true,
		CreatedAt:    now,
	})
	withDiskStorageFake(t, storage)

	withDiskStdin(t, errReader{err: errors.New("must not read stdin on dry-run")})
	code, dryOut, errOut := runCLI("snapshot", "prune", testDiskID1, "--dry-run")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.True(t, storage.lastPruneSnapshotsReq.GetDryRun())
	require.Contains(t, dryOut, "1")
	require.Contains(t, dryOut, testSnapID1)
	require.NotContains(t, dryOut, testSnapID2)
	require.NotContains(t, dryOut, testSnapID3)

	code, dryJSON, errOut := runCLI("snapshot", "prune", testDiskID1, "--dry-run", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var dryRes snapshotPruneJSON
	require.NoError(t, json.Unmarshal([]byte(dryJSON), &dryRes))
	require.Equal(t, testDiskID1, dryRes.DiskID)
	require.True(t, dryRes.DryRun)
	require.Equal(t, uint32(1), dryRes.ReclaimedCount)
	require.Len(t, dryRes.Snapshots, 1)
	require.Equal(t, testSnapID1, dryRes.Snapshots[0].SnapshotID)

	code, dryEmptyOut, errOut := runCLI("snapshot", "prune", testDiskID2, "--dry-run")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "No reclaimable snapshots found.\n", dryEmptyOut)

	callsBeforeCancel := storage.pruneSnapshotsCalls
	withDiskStdin(t, strings.NewReader("n\n"))
	code, out, errOut := runCLI("snapshot", "prune", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Prune obsolete fork/merge snapshots for disk "+testDiskID1+" (manual and protected snapshots are preserved)? [y/N]: ")
	require.Contains(t, errOut, "Operation canceled")
	require.Equal(t, callsBeforeCancel, storage.pruneSnapshotsCalls)

	withDiskStdin(t, strings.NewReader("y\n"))
	code, pruneOut, errOut := runCLI("snapshot", "prune", testDiskID1)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, errOut, "Prune obsolete fork/merge snapshots for disk "+testDiskID1+" (manual and protected snapshots are preserved)? [y/N]: ")
	require.False(t, storage.lastPruneSnapshotsReq.GetDryRun())
	require.Contains(t, pruneOut, "1")
	require.Contains(t, pruneOut, testSnapID1)

	code, emptyPruneOut, errOut := runCLI("snapshot", "prune", testDiskID1, "--yes")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "No snapshots pruned.\n", emptyPruneOut)

	code, emptyPruneJSON, errOut := runCLI("snapshot", "prune", testDiskID1, "-y", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var emptyPruneRes snapshotPruneJSON
	require.NoError(t, json.Unmarshal([]byte(emptyPruneJSON), &emptyPruneRes))
	require.Equal(t, testDiskID1, emptyPruneRes.DiskID)
	require.False(t, emptyPruneRes.DryRun)
	require.Equal(t, uint32(0), emptyPruneRes.ReclaimedCount)
	require.Empty(t, emptyPruneRes.Snapshots)
	require.Contains(t, emptyPruneJSON, `"snapshots": []`)
}

func TestSnapshot_GivenServerErrorsAndTracing_MapsErrorsAndRecordsSpans(t *testing.T) {
	exporter := setupDiskTestTracer(t)
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "traced", 1<<30, 0)
	withDiskStorageFake(t, storage)

	exporter.Reset()
	code, createOut, errOut := runCLI("snapshot", "create", testDiskID1, "-q")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	createdSnapID := strings.TrimSpace(createOut)
	createSpan := requireSpanByName(t, exporter.GetSpans(), "cli.snapshot.create")
	require.Contains(t, storage.lastTraceparent, createSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, createSpan, "bowl.disk_id", testDiskID1)
	requireSpanStringAttr(t, createSpan, "bowl.snapshot_id", createdSnapID)

	exporter.Reset()
	code, _, errOut = runCLI("snapshot", "list", testDiskID1)
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	listSpan := requireSpanByName(t, exporter.GetSpans(), "cli.snapshot.list")
	require.Contains(t, storage.lastTraceparent, listSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, listSpan, "bowl.disk_id", testDiskID1)

	exporter.Reset()
	code, _, errOut = runCLI("snapshot", "prune", testDiskID1, "--dry-run")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	pruneSpan := requireSpanByName(t, exporter.GetSpans(), "cli.snapshot.prune")
	require.Contains(t, storage.lastTraceparent, pruneSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, pruneSpan, "bowl.disk_id", testDiskID1)

	exporter.Reset()
	code, _, errOut = runCLI("snapshot", "delete", testDiskID1, createdSnapID, "-y")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	deleteSpan := requireSpanByName(t, exporter.GetSpans(), "cli.snapshot.delete")
	require.Contains(t, storage.lastTraceparent, deleteSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, deleteSpan, "bowl.disk_id", testDiskID1)
	requireSpanStringAttr(t, deleteSpan, "bowl.snapshot_id", createdSnapID)

	exporter.Reset()
	code, _, errOut = runCLI("snapshot", "create", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID2)
	errSpan := requireSpanByName(t, exporter.GetSpans(), "cli.snapshot.create")
	require.Equal(t, codes.Error, errSpan.Status.Code)

	storage.returnNilSnapshot = true
	code, _, errOut = runCLI("snapshot", "create", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "empty snapshot metadata")
	storage.returnNilSnapshot = false

	storage.forcedErr["ListSnapshots"] = connect.NewError(connect.CodeInvalidArgument, errors.New("bad cursor"))
	code, _, errOut = runCLI("snapshot", "list", testDiskID1)
	require.Equal(t, 2, code)
	require.Contains(t, errOut, "invalid request argument")
	delete(storage.forcedErr, "ListSnapshots")

	storage.forcedErr["PruneSnapshots"] = connect.NewError(connect.CodeInternal, errors.New("prune boom"))
	code, _, errOut = runCLI("snapshot", "prune", testDiskID1, "--dry-run")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "prune snapshots failed")
	delete(storage.forcedErr, "PruneSnapshots")

	code, _, errOut = runCLI("snapshot", "prune", testDiskID2, "--dry-run")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID2)
}
