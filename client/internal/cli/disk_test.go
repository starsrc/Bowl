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

	"github.com/starsrc/bowl/pkg/rpcerr"
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
	requireCLIError2(t, "bowl disk <subcommand>", "disk")
	requireCLIError2(t, `未知 disk 子命令 "unknown"`, "disk", "unknown")

	requireHelpExit0(t, "create", "disk", "help")
	requireHelpExit0(t, "fork", "disk", "-h")
	requireHelpExit0(t, "delete", "disk", "--help")

	requireHelpExit0(t, "bowl disk create --size <size>", "disk", "create", "-h")
	requireHelpExit0(t, "bowl disk fork <source-disk-id>", "disk", "fork", "--help")
	requireHelpExit0(t, "bowl disk list [flags]", "disk", "list", "-h")
	requireHelpExit0(t, "bowl disk get <disk-id>", "disk", "get", "--help")
	requireHelpExit0(t, "bowl disk resize <disk-id>", "disk", "update", "-h")
	requireHelpExit0(t, "bowl disk resize <disk-id>", "disk", "resize", "--help")
	requireHelpExit0(t, "bowl disk delete <disk-id>", "disk", "delete", "-h")
}

func TestDisk_GivenInvalidValidationInputs_ReturnsExit2WithDiagnostics(t *testing.T) {
	// fork 校验
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "fork")
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "fork", testDiskID1, "extra")
	requireCLIError2(t, "无效的 source-disk-id", "disk", "fork", "not-a-uuid")
	requireCLIError2(t, "无效的 source-disk-id", "disk", "fork", testNilUUID)
	requireCLIError2(t, "未知标志", "disk", "fork", testDiskID1, "--unknown")
	requireCLIError2(t, "--size 需要指定参数值", "disk", "fork", testDiskID1, "--size")
	requireCLIError2(t, "--name 需要指定参数值", "disk", "fork", testDiskID1, "--name")
	requireCLIError2(t, "--endpoint 需要指定参数值", "disk", "fork", testDiskID1, "--endpoint")
	requireCLIError2(t, "--token 需要指定参数值", "disk", "fork", testDiskID1, "--token")
	requireCLIError2(t, "不能同时使用", "disk", "fork", testDiskID1, "-q", "--json")
	requireCLIError2(t, "无效的 --size 参数", "disk", "fork", testDiskID1, "--size=")
	requireCLIError2(t, "无效的 --size 参数", "disk", "fork", testDiskID1, "--size", "0")
	requireCLIError2(t, "不能超过 255 字节", "disk", "fork", testDiskID1, "--name", strings.Repeat("n", 256))
	requireCLIError2(t, "不能包含空字节", "disk", "fork", testDiskID1, "--name", "bad\x00name")
	requireCLIError2(t, "无效的 endpoint", "disk", "fork", testDiskID1, "--endpoint", "ftp://127.0.0.1:9000")

	// create 校验
	requireCLIError2(t, "create 命令不接受位置参数", "disk", "create", "--size", "1GiB", "extra")
	requireCLIError2(t, "必须指定 --size 参数", "disk", "create")
	requireCLIError2(t, "不能同时使用", "disk", "create", "--size", "1GiB", "-q", "--json")
	requireCLIError2(t, "无效的 --size 参数", "disk", "create", "--size", "bad")
	requireCLIError2(t, "不能超过 255 字节", "disk", "create", "--size", "1GiB", "--name", strings.Repeat("a", 256))
	requireCLIError2(t, "不能包含空字节", "disk", "create", "--size", "1GiB", "--name", "bad\x00")

	// list 校验
	requireCLIError2(t, "list 命令不接受位置参数", "disk", "list", "extra")
	requireCLIError2(t, "未知标志", "disk", "list", "-q")
	requireCLIError2(t, "--limit 需要指定参数值", "disk", "list", "--limit")
	requireCLIError2(t, "--page-token 需要指定参数值", "disk", "list", "--page-token")
	requireCLIError2(t, "必须为 [1, 1000] 范围内的整数", "disk", "list", "--limit", "0")
	requireCLIError2(t, "必须为 [1, 1000] 范围内的整数", "disk", "list", "--limit", "1001")
	requireCLIError2(t, "必须为 [1, 1000] 范围内的整数", "disk", "list", "--limit", "abc")

	// get 校验
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "get")
	requireCLIError2(t, "无效的 disk-id", "disk", "get", "bad")
	requireCLIError2(t, "未知标志", "disk", "get", testDiskID1, "-q")

	// update / resize 校验
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "update")
	requireCLIError2(t, "无效的 disk-id", "disk", "update", testNilUUID, "--size", "1GiB")
	requireCLIError2(t, "未知标志", "disk", "update", testDiskID1, "--size", "1GiB", "--quiet")
	requireCLIError2(t, "必须至少指定 --size 或 --name 之一", "disk", "update", testDiskID1)
	requireCLIError2(t, "必须至少指定 --size 或 --name 之一", "disk", "resize", testDiskID1)
	requireCLIError2(t, "不能超过 255 字节", "disk", "update", testDiskID1, "--name", strings.Repeat("b", 256))
	requireCLIError2(t, "无效的 --size 参数", "disk", "update", testDiskID1, "--size", "0")

	// delete 校验
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "delete")
	requireCLIError2(t, "无效的 disk-id", "disk", "delete", "bad")
	requireCLIError2(t, "未知标志", "disk", "delete", testDiskID1, "-q")
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
	require.Contains(t, errOut, "不允许缩容至已用水位以下")
	require.Equal(t, 0, storage.updateCalls, "前置水位校验拦截后不得发起 UpdateDisk 调用")
}

func TestDiskDelete_GivenLocalMountOrInteractiveCancel_BlocksBeforeRPC(t *testing.T) {
	// Given: 本地 mountinfo 显示磁盘处于挂载状态
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

	// Given: 本机挂载信息不可读
	withUnreadableMountInfo(t)
	code, out, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "检查本机挂载表失败")
	require.Equal(t, 0, storage.deleteCalls)

	// Given: 未挂载但交互输入 n 或 EOF 取消
	withMissingMountInfo(t)
	withDiskStdin(t, strings.NewReader("n\n"))
	code, out, errOut = runCLI("disk", "delete", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "操作已取消")
	require.NotContains(t, errOut, "错误:")
	require.Equal(t, 0, storage.deleteCalls)

	withDiskStdin(t, strings.NewReader(""))
	code, out, errOut = runCLI("disk", "delete", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "操作已取消")
	require.Equal(t, 0, storage.deleteCalls)

	// Given: stdin 读取报错
	withDiskStdin(t, errReader{err: errors.New("broken pipe")})
	code, out, errOut = runCLI("disk", "delete", testDiskID1)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "读取终端确认输入失败")
	require.Equal(t, 0, storage.deleteCalls)
}

func TestDiskDelete_GivenYesFlagJSONAndInteractiveYES_DeletesDisk(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	storage.seedDisk(testDiskID2, "d2", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: --yes --json 删除第一块
	code, jsonOut, errOut := runCLI("disk", "delete", testDiskID1, "--yes", "--json")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var delRes diskDeleteJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &delRes))
	require.Equal(t, testDiskID1, delRes.DiskID)
	require.True(t, delRes.Deleted)

	// When: 交互输入 YES 删除第二块
	withDiskStdin(t, strings.NewReader("YES\n"))
	code, textOut, errOut := runCLI("disk", "delete", testDiskID2)

	// Then
	require.Equal(t, 0, code)
	require.Contains(t, errOut, "确认删除虚拟磁盘")
	require.Equal(t, "已删除虚拟磁盘 "+testDiskID2+"\n", textOut)
}

func TestDisk_GivenServerErrorsAndEmptyResponses_MapsExpectedExitCodes(t *testing.T) {
	// Given
	storage := newFakeStorage()
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// CodeNotFound 场景 (fork / get / update / delete)
	code, _, errOut := runCLI("disk", "fork", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在: "+testDiskID1)

	code, _, errOut = runCLI("disk", "get", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在: "+testDiskID1)

	code, _, errOut = runCLI("disk", "update", testDiskID1, "--name", "only-name")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在: "+testDiskID1)

	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在: "+testDiskID1)

	// CodeFailedPrecondition 场景 (fork 缩容 / update 并发缩容 / delete 活跃会话)
	storage.seedDisk(testDiskID1, "root", 100<<20, 10<<20)
	code, _, errOut = runCLI("disk", "fork", testDiskID1, "--size", "1MiB")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "目标容量小于源磁盘已用容量")

	storage.mounted[testDiskID1] = true
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "仍处于挂载状态")
	delete(storage.mounted, testDiskID1)

	storage.forcedErr["UpdateDisk"] = connect.NewError(connect.CodeFailedPrecondition, errors.New("shrink conflict"))
	code, _, errOut = runCLI("disk", "update", testDiskID1, "--name", "only-name")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "目标容量小于磁盘当前已用容量")
	delete(storage.forcedErr, "UpdateDisk")

	// delete: CodeInvalidArgument -> 退出码 2; ErrorInfo{Reason:"EBUSY"} -> 挂载提示退出码 1
	storage.forcedErr["DeleteDisk"] = connect.NewError(connect.CodeInvalidArgument, errors.New("bad disk id"))
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 2, code)
	require.Contains(t, errOut, "无效的请求参数")

	ebusyErr := connect.NewError(connect.CodeUnavailable, errors.New("busy"))
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: rpcerr.Domain,
		Reason: rpcerr.ReasonBusy,
	})
	require.NoError(t, detailErr)
	ebusyErr.AddDetail(detail)
	storage.forcedErr["DeleteDisk"] = ebusyErr
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "仍处于挂载状态")

	storage.forcedErr["DeleteDisk"] = connect.NewError(connect.CodeInternal, errors.New("delete boom"))
	code, _, errOut = runCLI("disk", "delete", testDiskID1, "-y")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "删除虚拟磁盘失败")
	delete(storage.forcedErr, "DeleteDisk")

	// ListDisks 内部错误与空元数据返回
	storage.forcedErr["ListDisks"] = connect.NewError(connect.CodeInternal, errors.New("list boom"))
	code, _, errOut = runCLI("disk", "list")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "列出虚拟磁盘失败")
	delete(storage.forcedErr, "ListDisks")

	storage.returnNilDisk["CreateDisk"] = true
	code, _, errOut = runCLI("disk", "create", "--size", "1GiB")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "空的虚拟磁盘元数据")
	delete(storage.returnNilDisk, "CreateDisk")

	storage.returnNilDisk["ForkDisk"] = true
	code, _, errOut = runCLI("disk", "fork", testDiskID1)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "空的虚拟磁盘元数据")
	delete(storage.returnNilDisk, "ForkDisk")

	storage.returnNilDisk["UpdateDisk"] = true
	code, _, errOut = runCLI("disk", "update", testDiskID1, "--name", "only-name")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "空的虚拟磁盘元数据")
}

func TestDisk_GivenActiveOTelTracer_PropagatesTraceparentRequestIDAndSpanStatus(t *testing.T) {
	// Given
	exporter := setupDiskTestTracer(t)
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "root", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: create 链路追踪
	exporter.Reset()
	code, createOut, errOut := runCLI("disk", "create", "--size", "1GiB", "-q")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	createdID := strings.TrimSpace(createOut)
	createSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.create")
	require.Contains(t, storage.lastTraceparent, createSpan.SpanContext.TraceID().String())
	require.NotEmpty(t, storage.lastRequestID)
	require.NotEqual(t, "1", storage.lastRequestID)

	// When: fork 链路追踪与磁盘属性记录
	exporter.Reset()
	code, forkOut, errOut := runCLI("disk", "fork", createdID, "--name", "traced-fork", "-q")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	forkedID := strings.TrimSpace(forkOut)
	forkSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.fork")
	require.Contains(t, storage.lastTraceparent, forkSpan.SpanContext.TraceID().String())
	requireSpanStringAttr(t, forkSpan, "bowl.source_disk_id", createdID)
	requireSpanStringAttr(t, forkSpan, "bowl.disk_id", forkedID)

	// When: list / get / resize / delete 链路追踪
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

	// When: 失败路径标记 Span Error 状态
	exporter.Reset()
	code, _, errOut = runCLI("disk", "fork", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在")
	errSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.fork")
	require.Equal(t, codes.Error, errSpan.Status.Code)
}
