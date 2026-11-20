package cli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

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

	for _, args := range [][]string{{"help", "disk"}, {"disk", "-h"}, {"disk", "--help"}} {
		for _, sub := range []string{"create", "fork", "list", "get", "update", "delete", "mounts", "evict", "diff", "merge"} {
			requireHelpExit0(t, sub, args...)
		}
	}

	requireHelpExit0(t, "bowl disk create", "disk", "create", "-h")
	requireHelpExit0(t, "bowl disk fork <source-disk-id>", "disk", "fork", "--help")
	requireHelpExit0(t, "bowl disk list", "disk", "list", "-h")
	requireHelpExit0(t, "bowl disk get <disk-id>", "disk", "get", "--help")
	requireHelpExit0(t, "update, resize", "disk", "update", "-h")
	requireHelpExit0(t, "update, resize", "disk", "resize", "--help")
	requireHelpExit0(t, "bowl disk delete <disk-id>", "disk", "delete", "-h")
	requireHelpExit0(t, "bowl disk mounts <disk-id>", "disk", "mounts", "-h")
	requireHelpExit0(t, "bowl disk evict <disk-id>", "disk", "evict", "--help")
}

func TestDisk_GivenInvalidValidationInputs_ReturnsExit2WithDiagnostics(t *testing.T) {

	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "fork")
	requireCLIError2(t, "accepts 1 arg(s), received 2", "disk", "fork", testDiskID1, "extra")
	requireCLIError2(t, "invalid source-disk-id", "disk", "fork", "not-a-uuid")
	requireCLIError2(t, "invalid source-disk-id", "disk", "fork", testNilUUID)
	requireCLIError2(t, "unknown flag: --unknown", "disk", "fork", testDiskID1, "--unknown")
	requireCLIError2(t, "flag needs an argument: --size", "disk", "fork", testDiskID1, "--size")
	requireCLIError2(t, "flag needs an argument: --name", "disk", "fork", testDiskID1, "--name")
	requireCLIError2(t, "flag needs an argument: --endpoint", "disk", "fork", testDiskID1, "--endpoint")
	requireCLIError2(t, "flag needs an argument: --token", "disk", "fork", testDiskID1, "--token")
	requireCLIError2(t, "cannot be used together", "disk", "fork", testDiskID1, "-q", "--json")
	requireCLIError2(t, "invalid --size", "disk", "fork", testDiskID1, "--size=")
	requireCLIError2(t, "invalid --size", "disk", "fork", testDiskID1, "--size", "0")
	requireCLIError2(t, "cannot exceed 255 bytes", "disk", "fork", testDiskID1, "--name", strings.Repeat("n", 256))
	requireCLIError2(t, "cannot contain NUL", "disk", "fork", testDiskID1, "--name", "bad\x00name")
	requireCLIError2(t, "invalid endpoint", "disk", "fork", testDiskID1, "--endpoint", "ftp://127.0.0.1:9000")
	requireCLIError2(t, "Error:", "disk", "fork", testDiskID1, "-q=bad")
	requireCLIError2(t, "Error:", "disk", "fork", testDiskID1, "--json=bad")

	requireCLIError2(t, `unknown command "extra"`, "disk", "create", "--size", "1GiB", "extra")
	requireCLIError2(t, "--size is required", "disk", "create")
	requireCLIError2(t, "cannot be used together", "disk", "create", "--size", "1GiB", "-q", "--json")
	requireCLIError2(t, "invalid --size", "disk", "create", "--size", "bad")
	requireCLIError2(t, "cannot exceed 255 bytes", "disk", "create", "--size", "1GiB", "--name", strings.Repeat("a", 256))
	requireCLIError2(t, "cannot contain NUL", "disk", "create", "--size", "1GiB", "--name", "bad\x00")
	requireCLIError2(t, "Error:", "disk", "create", "--size", "1GiB", "-q=bad")
	requireCLIError2(t, "Error:", "disk", "create", "--size", "1GiB", "--json=bad")

	requireCLIError2(t, `unknown command "extra"`, "disk", "list", "extra")
	requireCLIError2(t, "unknown shorthand flag", "disk", "list", "-q")
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
	requireCLIError2(t, "at least one of --size or --name must be specified", "disk", "resize", testDiskID1)
	requireCLIError2(t, "cannot exceed 255 bytes", "disk", "update", testDiskID1, "--name", strings.Repeat("b", 256))
	requireCLIError2(t, "invalid --size", "disk", "update", testDiskID1, "--size", "0")
	requireCLIError2(t, "Error:", "disk", "update", testDiskID1, "--size", "1GiB", "--json=bad")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "delete")
	requireCLIError2(t, "invalid disk-id", "disk", "delete", "bad")
	requireCLIError2(t, "unknown shorthand flag", "disk", "delete", testDiskID1, "-q")
	requireCLIError2(t, "unknown shorthand flag", "disk", "delete", testDiskID1, "-yes")
	requireCLIError2(t, "unknown flag: --y", "disk", "delete", testDiskID1, "--y")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "--yes=abc")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "-y=bad")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "--json=bad")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "mounts")
	requireCLIError2(t, "accepts 1 arg(s), received 2", "disk", "mounts", testDiskID1, "extra")
	requireCLIError2(t, "invalid disk-id", "disk", "mounts", "bad")
	requireCLIError2(t, "invalid disk-id", "disk", "mounts", testNilUUID)
	requireCLIError2(t, "unknown shorthand flag", "disk", "mounts", testDiskID1, "-q")
	requireCLIError2(t, "Error:", "disk", "mounts", testDiskID1, "--json=bad")

	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "evict")
	requireCLIError2(t, "accepts 1 arg(s), received 2", "disk", "evict", testDiskID1, "extra")
	requireCLIError2(t, "invalid disk-id", "disk", "evict", "bad")
	requireCLIError2(t, "invalid disk-id", "disk", "evict", testNilUUID)
	requireCLIError2(t, "unknown shorthand flag", "disk", "evict", testDiskID1, "-q")
	requireCLIError2(t, "flag needs an argument: --mount-id", "disk", "evict", testDiskID1, "--mount-id")
	requireCLIError2(t, "invalid mount-id", "disk", "evict", testDiskID1, "--mount-id", "bad")
	requireCLIError2(t, "invalid mount-id", "disk", "evict", testDiskID1, "--mount-id", testNilUUID)
	requireCLIError2(t, "invalid mount-id", "disk", "evict", testDiskID1, "--mount-id=")
	requireCLIError2(t, "Error:", "disk", "evict", testDiskID1, "--json=bad")
}

func TestDiskCreate_GivenValidSizeAndToken_CreatesDiskInQuietAndJSONModes(t *testing.T) {

	storage := newFakeStorage()
	withDiskStorageFake(t, storage)

	code, quietOut, errOut := runCLI("disk", "create", "--size", "1GiB", "--name", "c1", "-q", "--token", "sec-tok")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	createdID := strings.TrimSpace(quietOut)
	require.NotEmpty(t, createdID)
	require.Equal(t, "Bearer sec-tok", storage.lastAuth)

	code, jsonOut, errOut := runCLI("disk", "create", "--size=2GiB", "--name=c2", "--json")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &dj))
	require.Equal(t, "c2", dj.Name)
	require.Equal(t, uint64(2<<30), dj.MaxBytes)
	require.Empty(t, dj.ParentDiskID)
}

func TestDiskFork_GivenSeededSourceDisk_InheritsOrOverridesQuotaAndDisplaysParent(t *testing.T) {

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root-disk", 1<<30, 4096)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "fork", testDiskID1, "--name", "branch-exp", "--token", "sec-tok")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "Bearer sec-tok", storage.lastAuth)
	require.Contains(t, out, "Parent Disk:  "+testDiskID1+"\nName:         branch-exp\n")
	require.Contains(t, out, "1.00 GiB")

	code, jsonOut, errOut := runCLI("disk", "fork", "--name=json-branch", "--size=2GiB", "--json", "--token=tok2", "--", testDiskID1)

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &dj))
	require.Equal(t, testDiskID1, dj.ParentDiskID)
	require.Equal(t, "json-branch", dj.Name)
	require.Equal(t, uint64(2<<30), dj.MaxBytes)

	code, qOut, errOut := runCLI("disk", "fork", testDiskID1, "-q")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Len(t, strings.TrimSpace(qOut), 36)
}

func TestDiskGet_GivenRootDiskWithoutParent_HidesParentDiskField(t *testing.T) {

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root-disk", 1<<30, 4096)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "get", testDiskID1)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "Disk ID:      "+testDiskID1)
	require.NotContains(t, out, "Parent Disk:")

	code, jsonOut, errOut := runCLI("disk", "get", testDiskID1, "--json")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotContains(t, jsonOut, "parent_disk_id")
}

func TestDiskList_GivenTwoDisksWithLimit_RendersTablePaginationAndJSON(t *testing.T) {

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "alpha", 2<<30, 1<<30)
	storage.seedDisk(testDiskID2, "beta", 4<<30, 0)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "list", "--limit", "1")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "DISK ID")
	require.Contains(t, out, "50.00%")
	require.Contains(t, out, "Next Page Token: "+testDiskID2)

	code, jsonOut, errOut := runCLI("disk", "list", "--limit", "1", "--page-token", testDiskID2, "--json")

	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var listRes diskListJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &listRes))
	require.Len(t, listRes.Disks, 1)
	require.Equal(t, testDiskID2, listRes.Disks[0].DiskID)
	require.Empty(t, listRes.NextPageToken)
}

func TestDiskResize_GivenValidExpansionAndEmptyName_UpdatesMetadata(t *testing.T) {

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "old-name", 2<<30, 4096)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "resize", testDiskID1, "--size", "4GiB", "--name", "", "--json")

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

func TestDiskDelete_GivenLocalMountOrInteractiveCancel_BlocksBeforeRPC(t *testing.T) {

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "busy-disk", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMountInfoContent(t, "100 30 0:50 / /mnt/bowl-test rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	code, out, errOut := runCLI("disk", "delete", testDiskID1, "-y")

	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "bowl umount /mnt/bowl-test")
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

	withDiskStdin(t, strings.NewReader(""))
	code, out, errOut = runCLI("disk", "delete", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Operation canceled")
	require.Equal(t, 0, storage.deleteCalls)

	withDiskStdin(t, errReader{err: errors.New("broken pipe")})
	code, out, errOut = runCLI("disk", "delete", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "read confirmation input")
	require.Equal(t, 0, storage.deleteCalls)
}

func TestDiskDelete_GivenYesFalseFlag_PreservesInteractiveConfirmation(t *testing.T) {

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	storage.seedDisk(testDiskID2, "d2", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	withDiskStdin(t, strings.NewReader("n\n"))
	code, out, errOut := runCLI("disk", "delete", testDiskID1, "--yes=false")

	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Delete virtual disk "+testDiskID1+"? This action is irreversible [y/N]: ")
	require.Contains(t, errOut, "Operation canceled")
	require.Equal(t, 0, storage.deleteCalls)

	withDiskStdin(t, strings.NewReader("y\n"))
	code, out, errOut = runCLI("disk", "delete", testDiskID1, "--yes=false")

	require.Equal(t, 0, code)
	require.Contains(t, errOut, "Delete virtual disk "+testDiskID1+"? This action is irreversible [y/N]: ")
	require.Equal(t, "Deleted virtual disk "+testDiskID1+"\n", out)
	require.Equal(t, 1, storage.deleteCalls)

	code, _, errOut = runCLI("disk", "delete", testDiskID2, "-y")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, 2, storage.deleteCalls)

	storage.seedDisk(testDiskID2, "d2-reseeded", 1<<30, 0)
	withDiskStdin(t, strings.NewReader("n\n"))
	code, out, errOut = runCLI("disk", "delete", testDiskID2)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Operation canceled")
	require.Equal(t, 2, storage.deleteCalls)
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

	withDiskStdin(t, strings.NewReader("YES\n"))
	code, textOut, errOut := runCLI("disk", "delete", testDiskID2)

	require.Equal(t, 0, code)
	require.Contains(t, errOut, "Delete virtual disk "+testDiskID2+"? This action is irreversible [y/N]: ")
	require.Equal(t, "Deleted virtual disk "+testDiskID2+"\n", textOut)
}

func TestDisk_GivenJSONFalseFlag_KeepsHumanOutput(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root", 2<<30, 4096)
	storage.seedDisk(testDiskID2, "target", 2<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	code, out, errOut := runCLI("disk", "create", "--size", "1GiB", "--name", "c-human", "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "Disk ID:")

	code, out, errOut = runCLI("disk", "fork", testDiskID1, "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "Parent Disk:  "+testDiskID1)

	code, out, errOut = runCLI("disk", "list", "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "DISK ID")

	code, out, errOut = runCLI("disk", "get", testDiskID1, "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "Disk ID:      "+testDiskID1)

	code, out, errOut = runCLI("disk", "update", testDiskID1, "--name", "updated-human", "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "Name:         updated-human")

	code, out, errOut = runCLI("disk", "diff", testDiskID1, "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "Summary:")

	code, out, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "Merged successfully")

	code, out, errOut = runCLI("disk", "delete", testDiskID1, "--yes", "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, "Deleted virtual disk "+testDiskID1+"\n", out)

	code, out, errOut = runCLI("disk", "mounts", testDiskID2, "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "MOUNT ID")

	code, out, errOut = runCLI("disk", "evict", testDiskID2, "--json=false")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "Evicted 0 mount lease(s)")
}

func TestDiskMounts_GivenActiveLeasesAndEmpty_RendersTableAndJSON(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "mounted-disk", 1<<30, 0)
	storage.seedDisk(testDiskID2, "idle-disk", 1<<30, 0)
	exp1 := time.Date(2026, 11, 20, 12, 0, 30, 0, time.UTC)
	exp2 := time.Date(2026, 11, 20, 12, 0, 45, 0, time.UTC)
	const (
		mountRW = "01950000-0000-7000-9000-000000000001"
		mountRO = "01950000-0000-7000-9000-000000000002"
	)
	storage.seedMountLease(testDiskID1, mountRW, false, 3, exp1)
	storage.seedMountLease(testDiskID1, mountRO, true, 0, exp2)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "mounts", testDiskID1)
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "MOUNT ID")
	require.Contains(t, out, "MODE")
	require.Contains(t, out, "EPOCH")
	require.Contains(t, out, "EXPIRES AT")
	require.Contains(t, out, mountRW)
	require.Contains(t, out, "rw")
	require.Contains(t, out, "2026-11-20T12:00:30Z")
	require.Contains(t, out, mountRO)
	require.Contains(t, out, "ro")
	require.Contains(t, out, "2026-11-20T12:00:45Z")

	code, jsonOut, errOut := runCLI("disk", "mounts", testDiskID1, "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var parsed struct {
		DiskID string `json:"disk_id"`
		Mounts []struct {
			MountID   string `json:"mount_id"`
			Mode      string `json:"mode"`
			Epoch     uint64 `json:"epoch"`
			ExpiresAt string `json:"expires_at"`
		} `json:"mounts"`
	}
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &parsed))
	require.Equal(t, testDiskID1, parsed.DiskID)
	require.Len(t, parsed.Mounts, 2)
	require.Equal(t, mountRW, parsed.Mounts[0].MountID)
	require.Equal(t, "rw", parsed.Mounts[0].Mode)
	require.Equal(t, uint64(3), parsed.Mounts[0].Epoch)
	require.Equal(t, "2026-11-20T12:00:30Z", parsed.Mounts[0].ExpiresAt)
	require.Equal(t, mountRO, parsed.Mounts[1].MountID)
	require.Equal(t, "ro", parsed.Mounts[1].Mode)
	require.Equal(t, uint64(0), parsed.Mounts[1].Epoch)
	require.Equal(t, "2026-11-20T12:00:45Z", parsed.Mounts[1].ExpiresAt)

	code, emptyJSON, errOut := runCLI("disk", "mounts", testDiskID2, "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, emptyJSON, `"mounts": []`)
}

func TestDiskEvict_GivenSingleAndAllModes_RendersHumanAndJSON(t *testing.T) {
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "evict-cli-disk", 1<<30, 0)
	exp := time.Date(2026, 11, 20, 12, 0, 30, 0, time.UTC)
	const (
		mountRW = "01950000-0000-7000-9000-000000000011"
		mountRO = "01950000-0000-7000-9000-000000000012"
	)
	storage.seedMountLease(testDiskID1, mountRW, false, 1, exp)
	storage.seedMountLease(testDiskID1, mountRO, true, 0, exp)
	withDiskStorageFake(t, storage)

	code, out, errOut := runCLI("disk", "evict", testDiskID1, "--mount-id", mountRO)
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "Evicted 1 mount lease(s)")
	require.Contains(t, out, "writer epoch: 0")
	require.NotNil(t, storage.lastEvictReq.MountId)
	require.Equal(t, mountRO, storage.lastEvictReq.GetMountId())

	code, jsonOut, errOut := runCLI("disk", "evict", testDiskID1, "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Nil(t, storage.lastEvictReq.MountId)
	var parsed struct {
		DiskID       string `json:"disk_id"`
		EvictedCount uint32 `json:"evicted_count"`
		WriterEpoch  uint64 `json:"writer_epoch"`
	}
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &parsed))
	require.Equal(t, testDiskID1, parsed.DiskID)
	require.Equal(t, uint32(1), parsed.EvictedCount)
	require.Equal(t, uint64(1), parsed.WriterEpoch)
}

func TestDisk_GivenServerErrorsAndEmptyResponses_MapsExpectedExitCodes(t *testing.T) {

	storage := newFakeStorage()
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	code, _, errOut := runCLI("disk", "fork", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID1)

	code, _, errOut = runCLI("disk", "get", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID1)

	code, _, errOut = runCLI("disk", "update", testDiskID1, "--name", "only-name")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID1)

	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID1)

	storage.seedDisk(testDiskID1, "root", 100<<20, 10<<20)
	code, _, errOut = runCLI("disk", "fork", testDiskID1, "--size", "1MiB")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "target size is smaller than source disk used bytes")

	storage.mounted[testDiskID1] = true
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "still mounted")
	delete(storage.mounted, testDiskID1)

	storage.forcedErr["UpdateDisk"] = connect.NewError(connect.CodeFailedPrecondition, errors.New("shrink conflict"))
	code, _, errOut = runCLI("disk", "update", testDiskID1, "--name", "only-name")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "target size is smaller than current used bytes")
	delete(storage.forcedErr, "UpdateDisk")

	storage.forcedErr["DeleteDisk"] = connect.NewError(connect.CodeInvalidArgument, errors.New("bad disk id"))
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 2, code)
	require.Contains(t, errOut, "invalid request argument")

	ebusyErr := connect.NewError(connect.CodeAborted, errors.New("busy"))
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EBUSY"})
	require.NoError(t, detailErr)
	ebusyErr.AddDetail(detail)
	storage.forcedErr["DeleteDisk"] = ebusyErr
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "still mounted")
	require.Contains(t, errOut, "bowl disk mounts")
	require.Contains(t, errOut, "bowl disk evict")

	storage.forcedErr["DeleteDisk"] = connect.NewError(connect.CodeInternal, errors.New("delete boom"))
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "delete virtual disk failed")
	delete(storage.forcedErr, "DeleteDisk")

	code, _, errOut = runCLI("disk", "mounts", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID2)

	storage.forcedErr["ListMounts"] = connect.NewError(connect.CodeInvalidArgument, errors.New("bad disk"))
	code, _, errOut = runCLI("disk", "mounts", testDiskID1)
	require.Equal(t, 2, code)
	require.Contains(t, errOut, "invalid request argument")
	delete(storage.forcedErr, "ListMounts")

	storage.forcedErr["ListMounts"] = connect.NewError(connect.CodeInternal, errors.New("mounts boom"))
	code, _, errOut = runCLI("disk", "mounts", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "list disk mounts failed")
	delete(storage.forcedErr, "ListMounts")

	code, _, errOut = runCLI("disk", "evict", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found: "+testDiskID2)

	storage.forcedErr["EvictMounts"] = connect.NewError(connect.CodeInvalidArgument, errors.New("bad mount"))
	code, _, errOut = runCLI("disk", "evict", testDiskID1)
	require.Equal(t, 2, code)
	require.Contains(t, errOut, "invalid request argument")
	delete(storage.forcedErr, "EvictMounts")

	storage.forcedErr["EvictMounts"] = connect.NewError(connect.CodeInternal, errors.New("evict boom"))
	code, _, errOut = runCLI("disk", "evict", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "evict disk mounts failed")
	delete(storage.forcedErr, "EvictMounts")

	storage.forcedErr["ListDisks"] = connect.NewError(connect.CodeInternal, errors.New("list boom"))
	code, _, errOut = runCLI("disk", "list")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "list virtual disks failed")
	delete(storage.forcedErr, "ListDisks")

	storage.returnNilDisk["CreateDisk"] = true
	code, _, errOut = runCLI("disk", "create", "--size", "1GiB")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "empty virtual disk metadata")
	delete(storage.returnNilDisk, "CreateDisk")

	storage.returnNilDisk["ForkDisk"] = true
	code, _, errOut = runCLI("disk", "fork", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "empty virtual disk metadata")
	delete(storage.returnNilDisk, "ForkDisk")

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
	code, forkOut, errOut := runCLI("disk", "fork", createdID, "--name", "traced-fork", "-q")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	forkedID := strings.TrimSpace(forkOut)
	forkSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.fork")
	require.Contains(t, storage.lastTraceparent, forkSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, forkSpan, "bowl.source_disk_id", createdID)
	requireSpanStringAttr(t, forkSpan, "bowl.disk_id", forkedID)

	exporter.Reset()
	code, _, errOut = runCLI("disk", "list")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	listSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.list")
	require.Contains(t, storage.lastTraceparent, listSpan.SpanContext.TraceID().String())

	exporter.Reset()
	code, _, errOut = runCLI("disk", "get", forkedID)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	getSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.get")
	require.Contains(t, storage.lastTraceparent, getSpan.SpanContext.TraceID().String())

	exporter.Reset()
	code, _, errOut = runCLI("disk", "resize", forkedID, "--name", "renamed")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	updateSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.update")
	require.Contains(t, storage.lastTraceparent, updateSpan.SpanContext.TraceID().String())

	exporter.Reset()
	code, _, errOut = runCLI("disk", "mounts", forkedID)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	mountsSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.mounts")
	require.Contains(t, storage.lastTraceparent, mountsSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, mountsSpan, "bowl.disk_id", forkedID)

	exporter.Reset()
	code, _, errOut = runCLI("disk", "evict", forkedID)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	evictSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.evict")
	require.Contains(t, storage.lastTraceparent, evictSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, evictSpan, "bowl.disk_id", forkedID)

	exporter.Reset()
	code, _, errOut = runCLI("disk", "delete", forkedID, "-y")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	deleteSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.delete")
	require.Contains(t, storage.lastTraceparent, deleteSpan.SpanContext.TraceID().String())

	exporter.Reset()
	code, _, errOut = runCLI("disk", "fork", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found")
	errSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.fork")
	require.Equal(t, codes.Error, errSpan.Status.Code)
}
