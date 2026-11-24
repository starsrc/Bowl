package cli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

func requireCLIError2(t *testing.T, wantSubstr string, args ...string) {
	t.Helper()
	code, out, errOut := runCLI(args...)
	require.Equal(t, 2, code, "args=%v stderr=%s", args, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, wantSubstr, "args=%v", args)
}

func requireHelpExit0(t *testing.T, wantSubstr string, args ...string) {
	t.Helper()
	code, out, errOut := runCLI(args...)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, wantSubstr)
}

func setupDiskTestTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return exporter
}

func TestDisk_GivenHelpAndRoutingArgs_ReturnsExpectedUsageAndExitCodes(t *testing.T) {
	requireCLIError2(t, "bowl disk [command]", "disk")
	requireCLIError2(t, `unknown command "unknown" for "bowl disk"`, "disk", "unknown")
	for _, removed := range []string{"fork", "diff", "merge", "mounts", "evict"} {
		requireCLIError2(t, `unknown command "`+removed+`" for "bowl disk"`, "disk", removed)
	}

	for _, args := range [][]string{{"help", "disk"}, {"disk", "-h"}, {"disk", "--help"}} {
		for _, sub := range []string{"create", "list", "get", "update", "delete"} {
			requireHelpExit0(t, sub, args...)
		}
	}

	requireHelpExit0(t, "bowl disk create", "disk", "create", "-h")
	requireHelpExit0(t, "list, ls", "disk", "list", "-h")
	requireHelpExit0(t, "get, view", "disk", "get", "--help")
	requireHelpExit0(t, "update, edit", "disk", "update", "-h")
	requireHelpExit0(t, "update, edit", "disk", "edit", "--help")
	requireHelpExit0(t, "delete, rm", "disk", "delete", "-h")
}

func TestDisk_GivenInvalidValidationInputs_ReturnsExit2WithDiagnostics(t *testing.T) {
	requireCLIError2(t, "unknown flag: --token", "disk", "create", "--size", "1GiB", "--token", "sec-tok")
	requireCLIError2(t, `unknown command "extra"`, "disk", "create", "--size", "1GiB", "extra")
	requireCLIError2(t, "--size is required", "disk", "create")
	requireCLIError2(t, "cannot be used together", "disk", "create", "--size", "1GiB", "-q", "--json")
	requireCLIError2(t, "invalid --size", "disk", "create", "--size", "bad")
	requireCLIError2(t, "cannot exceed 255 bytes", "disk", "create", "--size", "1GiB", "--name", strings.Repeat("a", 256))
	requireCLIError2(t, "cannot contain NUL", "disk", "create", "--size", "1GiB", "--name", "bad\x00")
	requireCLIError2(t, "invalid endpoint", "disk", "create", "--size", "1GiB", "--endpoint", "ftp://127.0.0.1:9000")
	requireCLIError2(t, "Error:", "disk", "create", "--size", "1GiB", "-q=bad")
	requireCLIError2(t, "Error:", "disk", "create", "--size", "1GiB", "--json=bad")

	requireCLIError2(t, `unknown command "extra"`, "disk", "list", "extra")
	requireCLIError2(t, "cannot be used together", "disk", "list", "-q", "--json")
	requireCLIError2(t, "flag needs an argument: --limit", "disk", "list", "--limit")
	requireCLIError2(t, "flag needs an argument: --page-token", "disk", "list", "--page-token")
	requireCLIError2(t, "must be an integer in [1, 1000]", "disk", "list", "--limit", "0")
	requireCLIError2(t, "must be an integer in [1, 1000]", "disk", "list", "--limit", "1001")
	requireCLIError2(t, "Error:", "disk", "list", "--limit", "abc")
	requireCLIError2(t, "Error:", "disk", "list", "--json=bad")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "get")
	requireCLIError2(t, "invalid disk-id", "disk", "get", "bad")
	requireCLIError2(t, "unknown shorthand flag", "disk", "get", testDiskID1, "-q")
	requireCLIError2(t, "bad flag syntax: ---json", "disk", "get", testDiskID1, "---json")
	requireCLIError2(t, "Error:", "disk", "get", testDiskID1, "--json=bad")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "update")
	requireCLIError2(t, "invalid disk-id", "disk", "update", testNilUUID, "--size", "1GiB")
	requireCLIError2(t, "unknown flag: --quiet", "disk", "update", testDiskID1, "--size", "1GiB", "--quiet")
	requireCLIError2(t, "at least one of --size or --name must be specified", "disk", "update", testDiskID1)
	requireCLIError2(t, "at least one of --size or --name must be specified", "disk", "edit", testDiskID1)
	requireCLIError2(t, "cannot exceed 255 bytes", "disk", "update", testDiskID1, "--name", strings.Repeat("b", 256))
	requireCLIError2(t, "invalid --size", "disk", "update", testDiskID1, "--size", "0")
	requireCLIError2(t, "Error:", "disk", "update", testDiskID1, "--size", "1GiB", "--json=bad")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "delete")
	requireCLIError2(t, "invalid disk-id", "disk", "delete", "bad")
	requireCLIError2(t, "unknown shorthand flag", "disk", "delete", testDiskID1, "-q")
	requireCLIError2(t, "unknown flag: --recursive", "disk", "delete", testDiskID1, "--recursive")
	requireCLIError2(t, "unknown shorthand flag", "disk", "delete", testDiskID1, "-r")
	requireCLIError2(t, "unknown shorthand flag", "disk", "delete", testDiskID1, "-yes")
	requireCLIError2(t, "unknown flag: --y", "disk", "delete", testDiskID1, "--y")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "--yes=abc")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "-y=bad")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "--json=bad")
}

func TestDiskCreate_GivenValidSizeAndBowlTokenEnv_CreatesDiskInQuietAndJSONModesWithoutAuth(t *testing.T) {
	t.Setenv("BOWL_TOKEN", "env-sec-tok")
	storage := newFakeStorage()
	withDiskStorageFake(t, storage)

	code, quietOut, errOut := runCLI("disk", "create", "--size", "1GiB", "--name", "c1", "-q")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	createdID := strings.TrimSpace(quietOut)
	require.NotEmpty(t, createdID)
	require.Empty(t, storage.lastAuth)

	code, jsonOut, errOut := runCLI("--json", "disk", "create", "--size=2GiB", "--name=c2")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &dj))
	require.Equal(t, "c2", dj.Name)
	require.Equal(t, "main", dj.DefaultBranch)
	require.Equal(t, uint64(2<<30), dj.MaxBytes)
	require.NotContains(t, jsonOut, "parent_disk_id")
	require.NotContains(t, jsonOut, "root_ino")
	require.NotContains(t, jsonOut, "used_inodes")
}

func TestDiskGet_GivenSeededDisk_RendersGitModelFieldsAndViewAlias(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root-disk", 1<<30, 4096)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "view", testDiskID1)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "Disk ID:        "+testDiskID1)
	require.Contains(t, out, "Default Branch: main")
	require.NotContains(t, out, "Parent Disk:")
	require.NotContains(t, out, "Root Ino:")
	require.NotContains(t, out, "Used Inodes:")

	code, jsonOut, errOut := runCLI("disk", "get", testDiskID1, "--json")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &dj))
	require.Equal(t, testDiskID1, dj.DiskID)
	require.Equal(t, "main", dj.DefaultBranch)
}

func TestDiskList_GivenTwoDisksWithLimit_RendersTableQuietPaginationAndJSON(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "alpha", 2<<30, 1<<30)
	storage.seedDisk(testDiskID2, "beta", 4<<30, 0)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "ls", "--limit", "1")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "DISK ID")
	require.Contains(t, out, "DEFAULT BRANCH")
	require.Contains(t, out, "50.00%")
	require.Contains(t, out, "Next Page Token: "+testDiskID2)

	code, quietOut, errOut := runCLI("disk", "list", "-q")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, testDiskID1+"\n"+testDiskID2+"\n", quietOut)

	code, jsonOut, errOut := runCLI("disk", "list", "--limit", "1", "--page-token", testDiskID2, "--json")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var listRes diskListJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &listRes))
	require.Len(t, listRes.Disks, 1)
	require.Equal(t, testDiskID2, listRes.Disks[0].DiskID)
	require.Empty(t, listRes.NextPageToken)
}

func TestDiskEdit_GivenValidExpansionAndEmptyName_UpdatesMetadata(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "old-name", 2<<30, 4096)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "edit", testDiskID1, "--size", "4GiB", "--name", "", "--json")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(out), &dj))
	require.Empty(t, dj.Name)
	require.Equal(t, uint64(4<<30), dj.MaxBytes)
}

func TestDiskUpdate_GivenTargetBelowUsedBytes_RejectsBeforeUpdateRPC(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "used-disk", 100<<20, 10<<20)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "update", testDiskID1, "--size", "1MiB")

	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "shrinking below used space is not allowed")
	require.Equal(t, 0, storage.updateCalls, "pre-check rejection must not call UpdateDisk")
}

func TestDiskDelete_GivenLocalBranchMountOrInteractiveCancel_BlocksBeforeRPC(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "busy-disk", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMountInfoContent(t, "100 30 0:50 / /mnt/bowl-feat rw - fuse.bowl bowl:"+testDiskID1+"@feat rw\n")

	code, out, errOut := runCLI("disk", "rm", testDiskID1, "-y")

	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "bowl umount /mnt/bowl-feat")
	require.Equal(t, 0, storage.deleteCalls)

	withUnreadableMountInfo(t)
	code, out, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "check local mount table")
	require.Equal(t, 0, storage.deleteCalls)

	withMissingMountInfo(t)
	withDiskStdin(t, strings.NewReader("n\n"))
	code, out, errOut = runCLI("disk", "delete", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Operation canceled")
	require.NotContains(t, errOut, "Error:")
	require.Equal(t, 0, storage.deleteCalls)

	withDiskStdin(t, errReader{err: errors.New("broken pipe")})
	code, out, errOut = runCLI("disk", "delete", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "read confirmation input")
	require.Equal(t, 0, storage.deleteCalls)
}

func TestDiskDelete_GivenYesFlagJSONAndInteractiveYES_DeletesDisk(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	storage.seedDisk(testDiskID2, "d2", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	code, jsonOut, errOut := runCLI("disk", "delete", testDiskID1, "--yes", "--json")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var delRes diskDeleteJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &delRes))
	require.Equal(t, testDiskID1, delRes.DiskID)
	require.True(t, delRes.Deleted)
	require.NotNil(t, storage.lastDeleteReq)

	withDiskStdin(t, strings.NewReader("YES\n"))
	code, textOut, errOut := runCLI("disk", "delete", testDiskID2)
	require.Equal(t, 0, code)
	require.Contains(t, errOut, "Delete virtual disk "+testDiskID2)
	require.Equal(t, "Deleted virtual disk "+testDiskID2+"\n", textOut)
}

func TestDisk_GivenServerErrorsAndEmptyResponses_MapsExpectedExitCodes(t *testing.T) {
	storage := newFakeStorage()
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	code, _, errOut := runCLI("disk", "get", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID1)

	code, _, errOut = runCLI("disk", "update", testDiskID1, "--name", "only-name")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID1)

	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID1)

	storage.seedDisk(testDiskID1, "root", 100<<20, 10<<20)
	storage.mounted[testDiskID1] = true
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "still mounted")
	require.Contains(t, errOut, "bowl lease list -d "+testDiskID1)
	require.Contains(t, errOut, "bowl lease evict -d "+testDiskID1)
	delete(storage.mounted, testDiskID1)

	ebusyErr := connect.NewError(connect.CodeAborted, errors.New("busy"))
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EBUSY"})
	require.NoError(t, detailErr)
	ebusyErr.AddDetail(detail)
	storage.forcedErr["DeleteDisk"] = ebusyErr
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "bowl lease list -d "+testDiskID1)
	require.Contains(t, errOut, "bowl lease evict -d "+testDiskID1)
	delete(storage.forcedErr, "DeleteDisk")

	storage.returnNilDisk["CreateDisk"] = true
	code, _, errOut = runCLI("disk", "create", "--size", "1GiB")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "empty virtual disk metadata")
	delete(storage.returnNilDisk, "CreateDisk")

	storage.returnNilDisk["UpdateDisk"] = true
	code, _, errOut = runCLI("disk", "update", testDiskID1, "--name", "only-name")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "empty virtual disk metadata")
}

func TestDisk_GivenActiveOTelTracer_PropagatesTraceparentRequestIDAndSpanStatus(t *testing.T) {
	exporter := setupDiskTestTracer(t)
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	exporter.Reset()
	code, createOut, errOut := runCLI("disk", "create", "--size", "1GiB", "-q")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	createdID := strings.TrimSpace(createOut)
	createSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.create")
	require.Contains(t, storage.lastTraceparent, createSpan.SpanContext.TraceID().String())
	require.NotEmpty(t, storage.lastRequestID)
	require.NotEqual(t, "1", storage.lastRequestID)

	exporter.Reset()
	code, _, errOut = runCLI("disk", "list")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	listSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.list")
	require.Contains(t, storage.lastTraceparent, listSpan.SpanContext.TraceID().String())

	exporter.Reset()
	code, _, errOut = runCLI("disk", "get", createdID)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	getSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.get")
	require.Contains(t, storage.lastTraceparent, getSpan.SpanContext.TraceID().String())

	exporter.Reset()
	code, _, errOut = runCLI("disk", "edit", createdID, "--name", "renamed")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	updateSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.update")
	require.Contains(t, storage.lastTraceparent, updateSpan.SpanContext.TraceID().String())

	exporter.Reset()
	code, _, errOut = runCLI("disk", "delete", createdID, "-y")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	deleteSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.delete")
	require.Contains(t, storage.lastTraceparent, deleteSpan.SpanContext.TraceID().String())

	exporter.Reset()
	code, _, errOut = runCLI("disk", "get", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found")
	errSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.get")
	require.Equal(t, codes.Error, errSpan.Status.Code)
}
