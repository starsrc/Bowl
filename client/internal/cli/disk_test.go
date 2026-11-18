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
	// Given / When / Then
	requireCLIError2(t, "bowl disk [command]", "disk")
	requireCLIError2(t, `unknown command "unknown" for "bowl disk"`, "disk", "unknown")

	for _, args := range [][]string{{"help", "disk"}, {"disk", "-h"}, {"disk", "--help"}} {
		for _, sub := range []string{"create", "fork", "list", "get", "update", "delete", "diff", "merge"} {
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
}

func TestDisk_GivenInvalidValidationInputs_ReturnsExit2WithDiagnostics(t *testing.T) {
	// fork validation
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

	// create validation
	requireCLIError2(t, `unknown command "extra"`, "disk", "create", "--size", "1GiB", "extra")
	requireCLIError2(t, "--size is required", "disk", "create")
	requireCLIError2(t, "cannot be used together", "disk", "create", "--size", "1GiB", "-q", "--json")
	requireCLIError2(t, "invalid --size", "disk", "create", "--size", "bad")
	requireCLIError2(t, "cannot exceed 255 bytes", "disk", "create", "--size", "1GiB", "--name", strings.Repeat("a", 256))
	requireCLIError2(t, "cannot contain NUL", "disk", "create", "--size", "1GiB", "--name", "bad\x00")
	requireCLIError2(t, "Error:", "disk", "create", "--size", "1GiB", "-q=bad")
	requireCLIError2(t, "Error:", "disk", "create", "--size", "1GiB", "--json=bad")

	// list validation
	requireCLIError2(t, `unknown command "extra"`, "disk", "list", "extra")
	requireCLIError2(t, "unknown shorthand flag", "disk", "list", "-q")
	requireCLIError2(t, "flag needs an argument: --limit", "disk", "list", "--limit")
	requireCLIError2(t, "flag needs an argument: --page-token", "disk", "list", "--page-token")
	requireCLIError2(t, "must be an integer in [1, 1000]", "disk", "list", "--limit", "0")
	requireCLIError2(t, "must be an integer in [1, 1000]", "disk", "list", "--limit", "1001")
	requireCLIError2(t, "Error:", "disk", "list", "--limit", "abc")
	requireCLIError2(t, "Error:", "disk", "list", "--json=bad")

	// get validation
	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "get")
	requireCLIError2(t, "invalid disk-id", "disk", "get", "bad")
	requireCLIError2(t, "unknown shorthand flag", "disk", "get", testDiskID1, "-q")
	requireCLIError2(t, "bad flag syntax: ---json", "disk", "get", testDiskID1, "---json")
	requireCLIError2(t, "Error:", "disk", "get", testDiskID1, "--json=bad")

	// update / resize validation
	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "update")
	requireCLIError2(t, "invalid disk-id", "disk", "update", testNilUUID, "--size", "1GiB")
	requireCLIError2(t, "unknown flag: --quiet", "disk", "update", testDiskID1, "--size", "1GiB", "--quiet")
	requireCLIError2(t, "at least one of --size or --name must be specified", "disk", "update", testDiskID1)
	requireCLIError2(t, "at least one of --size or --name must be specified", "disk", "resize", testDiskID1)
	requireCLIError2(t, "cannot exceed 255 bytes", "disk", "update", testDiskID1, "--name", strings.Repeat("b", 256))
	requireCLIError2(t, "invalid --size", "disk", "update", testDiskID1, "--size", "0")
	requireCLIError2(t, "Error:", "disk", "update", testDiskID1, "--size", "1GiB", "--json=bad")

	// delete validation
	requireCLIError2(t, "accepts 1 arg(s), received 0", "disk", "delete")
	requireCLIError2(t, "invalid disk-id", "disk", "delete", "bad")
	requireCLIError2(t, "unknown shorthand flag", "disk", "delete", testDiskID1, "-q")
	requireCLIError2(t, "unknown shorthand flag", "disk", "delete", testDiskID1, "-yes")
	requireCLIError2(t, "unknown flag: --y", "disk", "delete", testDiskID1, "--y")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "--yes=abc")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "-y=bad")
	requireCLIError2(t, "Error:", "disk", "delete", testDiskID1, "--json=bad")
}

func TestDiskCreate_GivenValidSizeAndToken_CreatesDiskInQuietAndJSONModes(t *testing.T) {
	// Given
	storage := newFakeStorage()
	withDiskStorageFake(t, storage)

	// When: quiet 模式创建磁盘并携带鉴权令牌
	code, quietOut, errOut := runCLI("disk", "create", "--size", "1GiB", "--name", "c1", "-q", "--token", "sec-tok")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	createdID := strings.TrimSpace(quietOut)
	require.NotEmpty(t, createdID)
	require.Equal(t, "Bearer sec-tok", storage.lastAuth)

	// When: --json 模式创建第二块磁盘
	code, jsonOut, errOut := runCLI("disk", "create", "--size=2GiB", "--name=c2", "--json")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &dj))
	require.Equal(t, "c2", dj.Name)
	require.Equal(t, uint64(2<<30), dj.MaxBytes)
	require.Empty(t, dj.ParentDiskID)
}

func TestDiskFork_GivenSeededSourceDisk_InheritsOrOverridesQuotaAndDisplaysParent(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root-disk", 1<<30, 4096)
	withDiskStorageFake(t, storage)

	// When: 省略 --size，继承源盘 1GiB 配额，验证详情紧接 Disk ID 打印 Parent Disk
	code, out, errOut := runCLI("disk", "fork", testDiskID1, "--name", "branch-exp", "--token", "sec-tok")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "Bearer sec-tok", storage.lastAuth)
	require.Contains(t, out, "Parent Disk:  "+testDiskID1+"\nName:         branch-exp\n")
	require.Contains(t, out, "1.00 GiB")

	// When: 显式覆盖 --size=2GiB 与 --json（使用 -- 分隔位置参数）
	code, jsonOut, errOut := runCLI("disk", "fork", "--name=json-branch", "--size=2GiB", "--json", "--token=tok2", "--", testDiskID1)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &dj))
	require.Equal(t, testDiskID1, dj.ParentDiskID)
	require.Equal(t, "json-branch", dj.Name)
	require.Equal(t, uint64(2<<30), dj.MaxBytes)

	// When: -q 静默模式
	code, qOut, errOut := runCLI("disk", "fork", testDiskID1, "-q")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Len(t, strings.TrimSpace(qOut), 36)
}

func TestDiskGet_GivenRootDiskWithoutParent_HidesParentDiskField(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root-disk", 1<<30, 4096)
	withDiskStorageFake(t, storage)

	// When / Then: 人类可读模式
	code, out, errOut := runCLI("disk", "get", testDiskID1)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "Disk ID:      "+testDiskID1)
	require.NotContains(t, out, "Parent Disk:")

	// When / Then: JSON 模式
	code, jsonOut, errOut := runCLI("disk", "get", testDiskID1, "--json")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotContains(t, jsonOut, "parent_disk_id")
}

func TestDiskList_GivenTwoDisksWithLimit_RendersTablePaginationAndJSON(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "alpha", 2<<30, 1<<30)
	storage.seedDisk(testDiskID2, "beta", 4<<30, 0)
	withDiskStorageFake(t, storage)

	// When: --limit=1 分页拉取首条
	code, out, errOut := runCLI("disk", "list", "--limit", "1")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "DISK ID")
	require.Contains(t, out, "50.00%")
	require.Contains(t, out, "Next Page Token: "+testDiskID2)

	// When: 携带 --page-token --json 拉取下一页
	code, jsonOut, errOut := runCLI("disk", "list", "--limit", "1", "--page-token", testDiskID2, "--json")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var listRes diskListJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &listRes))
	require.Len(t, listRes.Disks, 1)
	require.Equal(t, testDiskID2, listRes.Disks[0].DiskID)
	require.Empty(t, listRes.NextPageToken)
}

func TestDiskResize_GivenValidExpansionAndEmptyName_UpdatesMetadata(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "old-name", 2<<30, 4096)
	withDiskStorageFake(t, storage)

	// When
	code, out, errOut := runCLI("disk", "resize", testDiskID1, "--size", "4GiB", "--name", "", "--json")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(out), &dj))
	require.Empty(t, dj.Name)
	require.Equal(t, uint64(4<<30), dj.MaxBytes)
}

func TestDiskUpdate_GivenTargetBelowUsedBytes_RejectsBeforeUpdateRPC(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "used-disk", 100<<20, 10<<20)
	withDiskStorageFake(t, storage)

	// When
	code, out, errOut := runCLI("disk", "update", testDiskID1, "--size", "1MiB")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "shrinking below used space is not allowed")
	require.Equal(t, 0, storage.updateCalls, "pre-check rejection must not call UpdateDisk")
}

func TestDiskDelete_GivenLocalMountOrInteractiveCancel_BlocksBeforeRPC(t *testing.T) {
	// Given: local mountinfo shows the disk is mounted
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "busy-disk", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMountInfoContent(t, "100 30 0:50 / /mnt/bowl-test rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	// When
	code, out, errOut := runCLI("disk", "delete", testDiskID1, "-y")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "bowl umount /mnt/bowl-test")
	require.Equal(t, 0, storage.deleteCalls)

	// Given: unreadable local mountinfo
	withUnreadableMountInfo(t)
	code, out, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "check local mount table")
	require.Equal(t, 0, storage.deleteCalls)

	// Given: unmounted but interactive n or EOF cancels
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

	// Given: stdin read error
	withDiskStdin(t, errReader{err: errors.New("broken pipe")})
	code, out, errOut = runCLI("disk", "delete", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "read confirmation input")
	require.Equal(t, 0, storage.deleteCalls)
}

func TestDiskDelete_GivenYesFalseFlag_PreservesInteractiveConfirmation(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	storage.seedDisk(testDiskID2, "d2", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: --yes=false with "n" cancels without deleting
	withDiskStdin(t, strings.NewReader("n\n"))
	code, out, errOut := runCLI("disk", "delete", testDiskID1, "--yes=false")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Delete virtual disk "+testDiskID1+"? This action is irreversible [y/N]: ")
	require.Contains(t, errOut, "Operation canceled")
	require.Equal(t, 0, storage.deleteCalls)

	// When: --yes=false with "y" proceeds to delete
	withDiskStdin(t, strings.NewReader("y\n"))
	code, out, errOut = runCLI("disk", "delete", testDiskID1, "--yes=false")

	// Then
	require.Equal(t, 0, code)
	require.Contains(t, errOut, "Delete virtual disk "+testDiskID1+"? This action is irreversible [y/N]: ")
	require.Equal(t, "Deleted virtual disk "+testDiskID1+"\n", out)
	require.Equal(t, 1, storage.deleteCalls)

	// When: repeated Execute call without -y after -y must still prompt
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
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	storage.seedDisk(testDiskID2, "d2", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: --yes --json deletes first disk
	code, jsonOut, errOut := runCLI("disk", "delete", testDiskID1, "--yes", "--json")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var delRes diskDeleteJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &delRes))
	require.Equal(t, testDiskID1, delRes.DiskID)
	require.True(t, delRes.Deleted)

	// When: interactive YES deletes second disk
	withDiskStdin(t, strings.NewReader("YES\n"))
	code, textOut, errOut := runCLI("disk", "delete", testDiskID2)

	// Then
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
}

func TestDisk_GivenServerErrorsAndEmptyResponses_MapsExpectedExitCodes(t *testing.T) {
	// Given
	storage := newFakeStorage()
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// CodeNotFound across fork, get, update, delete
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

	// CodeFailedPrecondition across fork, delete, update
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

	// delete: CodeInvalidArgument -> exit 2; ErrorInfo Reason EBUSY -> exit 1
	storage.forcedErr["DeleteDisk"] = connect.NewError(connect.CodeInvalidArgument, errors.New("bad disk id"))
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 2, code)
	require.Contains(t, errOut, "invalid request argument")

	ebusyErr := connect.NewError(connect.CodeUnavailable, errors.New("busy"))
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EBUSY"})
	require.NoError(t, detailErr)
	ebusyErr.AddDetail(detail)
	storage.forcedErr["DeleteDisk"] = ebusyErr
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "still mounted")

	storage.forcedErr["DeleteDisk"] = connect.NewError(connect.CodeInternal, errors.New("delete boom"))
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "delete virtual disk failed")
	delete(storage.forcedErr, "DeleteDisk")

	// ListDisks internal error and empty disk metadata responses
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
	// Given
	exporter := setupDiskTestTracer(t)
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: create tracing
	exporter.Reset()
	code, createOut, errOut := runCLI("disk", "create", "--size", "1GiB", "-q")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	createdID := strings.TrimSpace(createOut)
	createSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.create")
	require.Contains(t, storage.lastTraceparent, createSpan.SpanContext.TraceID().String())
	require.NotEmpty(t, storage.lastRequestID)
	require.NotEqual(t, "1", storage.lastRequestID)

	// When: fork tracing and disk attributes
	exporter.Reset()
	code, forkOut, errOut := runCLI("disk", "fork", createdID, "--name", "traced-fork", "-q")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	forkedID := strings.TrimSpace(forkOut)
	forkSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.fork")
	require.Contains(t, storage.lastTraceparent, forkSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, forkSpan, "bowl.source_disk_id", createdID)
	requireSpanStringAttr(t, forkSpan, "bowl.disk_id", forkedID)

	// When: list / get / resize / delete tracing
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
	code, _, errOut = runCLI("disk", "delete", forkedID, "-y")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	deleteSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.delete")
	require.Contains(t, storage.lastTraceparent, deleteSpan.SpanContext.TraceID().String())

	// When: failure path marks span status as error
	exporter.Reset()
	code, _, errOut = runCLI("disk", "fork", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "disk not found")
	errSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.fork")
	require.Equal(t, codes.Error, errSpan.Status.Code)
}
