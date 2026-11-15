package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpcerr"
)

const (
	testCommitID1 = "01950000-0000-7000-8000-0000000000aa"
)

func TestDiskDiffMerge_GivenHelpAndRoutingArgs_ReturnsExpectedUsage(t *testing.T) {
	// Given / When / Then: disk 总帮助包含 diff 与 merge
	requireHelpExit0(t, "diff", "disk", "help")
	requireHelpExit0(t, "merge", "disk", "help")

	requireHelpExit0(t, "bowl disk diff <source-disk-id>", "disk", "diff", "-h")
	requireHelpExit0(t, "bowl disk merge <source-disk-id> --into <target-disk-id>", "disk", "merge", "--help")
}

func TestDiskDiff_GivenInvalidInputs_ReturnsExit2WithDiagnostics(t *testing.T) {
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "diff")
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "diff", testDiskID1, "extra")
	requireCLIError2(t, "无效的 source-disk-id", "disk", "diff", "bad-uuid")
	requireCLIError2(t, "无效的 source-disk-id", "disk", "diff", testNilUUID)
	requireCLIError2(t, "未知标志", "disk", "diff", testDiskID1, "-q")
	requireCLIError2(t, "无效的 target-disk-id", "disk", "diff", testDiskID1, "--target", "bad")
	requireCLIError2(t, "无效的 target-disk-id", "disk", "diff", testDiskID1, "--target", testNilUUID)
	requireCLIError2(t, "不能与源磁盘相同", "disk", "diff", testDiskID1, "--target", testDiskID1)
	requireCLIError2(t, "必须为 [1, 1000] 范围内的整数", "disk", "diff", testDiskID1, "--limit", "0")
	requireCLIError2(t, "必须为 [1, 1000] 范围内的整数", "disk", "diff", testDiskID1, "--limit", "1001")
	requireCLIError2(t, "必须为 [1, 1000] 范围内的整数", "disk", "diff", testDiskID1, "--limit", "abc")
	requireCLIError2(t, "不能包含空字节", "disk", "diff", testDiskID1, "--prefix", "bad\x00prefix")
	requireCLIError2(t, "不能超过 4096 字节", "disk", "diff", testDiskID1, "--prefix", strings.Repeat("a", 4097))
	requireCLIError2(t, "不能包含空字节", "disk", "diff", testDiskID1, "--page-token", "tok\x001")
	requireCLIError2(t, "不能超过 4096 字节", "disk", "diff", testDiskID1, "--page-token", strings.Repeat("t", 4097))
}

func TestDiskDiff_GivenEntriesAndPagination_RendersSummaryTableAndJSON(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.diffResponse = &storagev1.DiffDiskResponse{
		LcaCommitId: testCommitID1,
		Summary: &storagev1.DiffSummary{
			Added:     1,
			Modified:  1,
			Deleted:   1,
			Conflicts: 1,
		},
		Entries: []*storagev1.DiffEntry{
			{
				Path: []byte("/new.txt"),
				Kind: storagev1.DiffKind_DIFF_KIND_ADDED,
				Source: &storagev1.DiffNodeSummary{
					Ino:  10,
					Mode: 0o100644,
					Size: 12,
				},
			},
			{
				Path:     []byte("/conflict.txt"),
				Kind:     storagev1.DiffKind_DIFF_KIND_MODIFIED,
				Conflict: storagev1.ConflictKind_CONFLICT_KIND_CONTENT,
				Base:     &storagev1.DiffNodeSummary{Ino: 11, Mode: 0o100644, Size: 5},
				Target:   &storagev1.DiffNodeSummary{Ino: 11, Mode: 0o100644, Size: 8},
				Source:   &storagev1.DiffNodeSummary{Ino: 11, Mode: 0o100644, Size: 9},
			},
			{
				Path: []byte("/old.txt"),
				Kind: storagev1.DiffKind_DIFF_KIND_DELETED,
				Base: &storagev1.DiffNodeSummary{Ino: 12, Mode: 0o100644, Size: 4},
			},
		},
		NextPageToken: "cursor-2",
	}
	withDiskStorageFake(t, storage)

	// When: 人类可读输出 + 分页与过滤参数透传
	code, out, errOut := runCLI(
		"disk", "diff", testDiskID1,
		"--target", testDiskID2,
		"--prefix", "/sub",
		"--conflicts-only",
		"--limit", "20",
		"--page-token", "cursor-1",
	)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, 1, storage.diffCalls)
	require.NotNil(t, storage.lastDiffReq)
	require.Equal(t, testDiskID1, storage.lastDiffReq.GetSourceDiskId())
	require.Equal(t, testDiskID2, storage.lastDiffReq.GetTargetDiskId())
	require.Equal(t, []byte("/sub"), storage.lastDiffReq.GetPathPrefix())
	require.True(t, storage.lastDiffReq.GetConflictsOnly())
	require.Equal(t, uint32(20), storage.lastDiffReq.GetPageSize())
	require.Equal(t, "cursor-1", storage.lastDiffReq.GetPageToken())

	require.Contains(t, out, testCommitID1)
	require.Contains(t, out, "/new.txt")
	require.Contains(t, out, "/conflict.txt")
	require.Contains(t, out, "CONTENT")
	require.Contains(t, out, "/old.txt")
	require.Contains(t, out, "Next Page Token: cursor-2")

	// When: --json 输出
	code, jsonOut, errOut := runCLI("disk", "diff", testDiskID1, "--target", testDiskID2, "--json")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var parsed diskDiffJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &parsed))
	require.Equal(t, testDiskID1, parsed.SourceDiskID)
	require.Equal(t, testDiskID2, parsed.TargetDiskID)
	require.Equal(t, testCommitID1, parsed.LCACommitID)
	require.Equal(t, uint64(1), parsed.Summary.Added)
	require.Equal(t, uint64(1), parsed.Summary.Modified)
	require.Equal(t, uint64(1), parsed.Summary.Deleted)
	require.Equal(t, uint64(1), parsed.Summary.Conflicts)
	require.Equal(t, "cursor-2", parsed.NextPageToken)
	require.Len(t, parsed.Entries, 3)
	require.Equal(t, "added", parsed.Entries[0].Kind)
	require.Equal(t, "CONTENT", parsed.Entries[1].Conflict)
}

func TestDiskDiff_GivenPatchMode_RendersUnifiedTextSymlinkAndBinaryDiffs(t *testing.T) {
	// Given: 2-way diff (LCA -> Source) 包含文本修改（>4MiB 分片或常规多行）、符号链接变更、二进制含 NUL 文件
	storage := newFakeStorage()
	baseText := "line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\n"
	srcText := "line1\nline2-edited\nline3\nline4\nline5\nline6\nline7\nline8\nline9\n"
	storage.seedCommitFile(testDiskID1, testCommitID1, 100, 0o100644, []byte(baseText))
	storage.seedLiveFile(testDiskID1, 100, []byte(srcText))

	storage.seedCommitSymlink(testDiskID1, testCommitID1, 101, "/old/target")
	storage.seedLiveSymlink(testDiskID1, 101, "/new/target")

	storage.seedCommitFile(testDiskID1, testCommitID1, 102, 0o100644, []byte{0x01, 0x00, 0x02})
	storage.seedLiveFile(testDiskID1, 102, []byte{0x01, 0x00, 0x03})

	storage.seedLiveFile(testDiskID1, 103, []byte("brand new line\n"))

	storage.diffResponse = &storagev1.DiffDiskResponse{
		LcaCommitId: testCommitID1,
		Summary: &storagev1.DiffSummary{
			Added:    1,
			Modified: 3,
		},
		Entries: []*storagev1.DiffEntry{
			{
				Path:   []byte("/code.txt"),
				Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
				Base:   &storagev1.DiffNodeSummary{Ino: 100, Mode: 0o100644, Size: uint64(len(baseText))},
				Source: &storagev1.DiffNodeSummary{Ino: 100, Mode: 0o100644, Size: uint64(len(srcText))},
			},
			{
				Path:   []byte("/link"),
				Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
				Base:   &storagev1.DiffNodeSummary{Ino: 101, Mode: 0o120777, Size: 11},
				Source: &storagev1.DiffNodeSummary{Ino: 101, Mode: 0o120777, Size: 11},
			},
			{
				Path:   []byte("/blob.bin"),
				Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
				Base:   &storagev1.DiffNodeSummary{Ino: 102, Mode: 0o100644, Size: 3},
				Source: &storagev1.DiffNodeSummary{Ino: 102, Mode: 0o100644, Size: 3},
			},
			{
				Path:   []byte("/added.txt"),
				Kind:   storagev1.DiffKind_DIFF_KIND_ADDED,
				Source: &storagev1.DiffNodeSummary{Ino: 103, Mode: 0o100644, Size: 15},
			},
		},
	}
	withDiskStorageFake(t, storage)

	// When
	code, out, errOut := runCLI("disk", "diff", testDiskID1, "-p")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.GreaterOrEqual(t, storage.readCommitCalls, 3)
	require.GreaterOrEqual(t, storage.readCalls, 3)
	require.Equal(t, 1, storage.readlinkCalls)

	// Unified diff header & hunk assertions
	require.Contains(t, out, "--- a/code.txt")
	require.Contains(t, out, "+++ b/code.txt")
	require.Contains(t, out, "-line2")
	require.Contains(t, out, "+line2-edited")
	require.Contains(t, out, "+line9")
	require.Contains(t, out, "@@ ")

	// Symlink target diff
	require.Contains(t, out, "-/old/target")
	require.Contains(t, out, "+/new/target")

	// Binary NUL diff
	require.Contains(t, out, "Binary files a/blob.bin and b/blob.bin differ")

	// Added file diff
	require.Contains(t, out, "--- /dev/null")
	require.Contains(t, out, "+++ b/added.txt")
	require.Contains(t, out, "+brand new line")
}

func TestDiskDiff_GivenPatchLargeFileOver4MiB_ReadsMultipleChunks(t *testing.T) {
	// Given: > 4MiB 文本文件验证分片读取拼接
	storage := newFakeStorage()
	largePrefix := strings.Repeat("a\n", 2*1024*1024+100) // > 4 MiB
	baseData := []byte(largePrefix + "tail-old\n")
	srcData := []byte(largePrefix + "tail-new\n")
	storage.seedCommitFile(testDiskID1, testCommitID1, 200, 0o100644, baseData)
	storage.seedLiveFile(testDiskID1, 200, srcData)
	storage.diffResponse = &storagev1.DiffDiskResponse{
		LcaCommitId: testCommitID1,
		Summary:     &storagev1.DiffSummary{Modified: 1},
		Entries: []*storagev1.DiffEntry{
			{
				Path:   []byte("/big.txt"),
				Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
				Base:   &storagev1.DiffNodeSummary{Ino: 200, Mode: 0o100644, Size: uint64(len(baseData))},
				Source: &storagev1.DiffNodeSummary{Ino: 200, Mode: 0o100644, Size: uint64(len(srcData))},
			},
		},
	}
	withDiskStorageFake(t, storage)

	// When
	code, out, errOut := runCLI("disk", "diff", testDiskID1, "--patch")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.GreaterOrEqual(t, storage.readCommitCalls, 2)
	require.GreaterOrEqual(t, storage.readCalls, 2)
	require.Contains(t, out, "-tail-old")
	require.Contains(t, out, "+tail-new")
}

func TestDiskMerge_GivenInvalidInputs_ReturnsExit2WithDiagnostics(t *testing.T) {
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "merge")
	requireCLIError2(t, "需要且仅需要 1 个位置参数", "disk", "merge", testDiskID1, "extra", "--into", testDiskID2)
	requireCLIError2(t, "无效的 source-disk-id", "disk", "merge", "bad", "--into", testDiskID2)
	requireCLIError2(t, "必须指定 --into", "disk", "merge", testDiskID1)
	requireCLIError2(t, "无效的 target-disk-id", "disk", "merge", testDiskID1, "--into", "bad")
	requireCLIError2(t, "无效的 target-disk-id", "disk", "merge", testDiskID1, "--into", testNilUUID)
	requireCLIError2(t, "不能与源磁盘相同", "disk", "merge", testDiskID1, "--into", testDiskID1)
	requireCLIError2(t, "无效的 --strategy 参数", "disk", "merge", testDiskID1, "--into", testDiskID2, "--strategy", "auto")
	requireCLIError2(t, "无效的 --resolve 参数", "disk", "merge", testDiskID1, "--into", testDiskID2, "--resolve", "missing-eq")
	requireCLIError2(t, "无效的 --resolve 参数", "disk", "merge", testDiskID1, "--into", testDiskID2, "--resolve", "=ours")
	requireCLIError2(t, "无效的 --resolve 参数", "disk", "merge", testDiskID1, "--into", testDiskID2, "--resolve", "/a=fail")
	requireCLIError2(t, "无效的 --resolve 参数", "disk", "merge", testDiskID1, "--into", testDiskID2, "--resolve", "/a=bad")
	requireCLIError2(t, "不能包含空字节", "disk", "merge", testDiskID1, "--into", testDiskID2, "--resolve", "/a\x00b=ours")
	requireCLIError2(t, "无效的 --size 参数", "disk", "merge", testDiskID1, "--into", testDiskID2, "--size", "0")
	requireCLIError2(t, "不能同时使用", "disk", "merge", testDiskID1, "--into", testDiskID2, "-q", "--json")
}

func TestDiskMerge_GivenLocalMountGuards_EnforcesTargetSourceModesDryRunAndForce(t *testing.T) {
	// Given: 目标盘本机已挂载 -> 非 dry-run 非 force 强制拦截并提示先执行 bowl umount
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "src", 1<<30, 1024)
	storage.seedDisk(testDiskID2, "tgt", 1<<30, 1024)
	withDiskStorageFake(t, storage)
	withMountInfoContent(t, "100 30 0:50 / /mnt/target rw - fuse.bowl bowl:"+testDiskID2+" rw\n")

	// When
	code, out, errOut := runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "bowl umount /mnt/target")
	require.NotContains(t, errOut, "bowl unmount")
	require.Equal(t, 0, storage.mergeCalls)

	// Given: 目标盘本机已挂载，但使用 --dry-run -> 不被本地 preRPC 拦截，正常调用服务端返回预演结果
	code, out, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "--dry-run")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "Dry Run")
	require.Equal(t, 1, storage.mergeCalls)

	// Given: 目标盘与源盘均在本机 rw 挂载，指定 --force / -f -> 跳过本地 preRPC 拦截并透传 Force: true
	withMountInfoContent(t, "100 30 0:50 / /mnt/target rw - fuse.bowl bowl:"+testDiskID2+" rw\n101 30 0:51 / /mnt/source rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	code, out, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "--force")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, 2, storage.mergeCalls)
	require.True(t, storage.lastMergeReq.GetForce())
	require.Contains(t, out, testDiskID2)

	code, _, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "-f")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, 3, storage.mergeCalls)
	require.True(t, storage.lastMergeReq.GetForce())

	// Given: mountinfo 不可读（非 dry-run 非 force）-> 拦截
	withUnreadableMountInfo(t)
	code, out, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "检查本机挂载表失败")
	require.Equal(t, 3, storage.mergeCalls)

	// Given: 仅源盘在本机处于 rw 挂载且未传 --force -> preRPC 直接拒绝并提示先执行 bowl umount
	withMountInfoContent(t, "101 30 0:51 / /mnt/source rw,nosuid - fuse.bowl bowl:"+testDiskID1+" rw\n")
	code, out, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "读写挂载")
	require.Contains(t, errOut, "bowl umount /mnt/source")
	require.Equal(t, 3, storage.mergeCalls)

	// Given: 源盘在本机仅处于 ro 只读挂载 -> preRPC 放行
	withMountInfoContent(t, "101 30 0:51 / /mnt/source-ro ro,nosuid - fuse.bowl bowl:"+testDiskID1+" ro\n")
	code, out, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, 4, storage.mergeCalls)
	require.False(t, storage.lastMergeReq.GetForce())
	require.Contains(t, out, testDiskID2)
}

func TestDiskMerge_GivenFlagsAndDryRun_PropagatesOptionsAndRendersOutputs(t *testing.T) {
	// Given
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "branch", 2<<30, 4096)
	storage.seedDisk(testDiskID2, "main", 2<<30, 8192)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: --dry-run + --strategy + 可重复 --resolve (含路径带 '=') + --size
	code, out, errOut := runCLI(
		"disk", "merge", testDiskID1,
		"--into", testDiskID2,
		"--strategy", "ours",
		"--resolve", "/cfg/a=b.env=theirs",
		"--resolve", "/docs=ours",
		"--dry-run",
		"--size", "4GiB",
	)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, 1, storage.mergeCalls)
	req := storage.lastMergeReq
	require.NotNil(t, req)
	require.Equal(t, testDiskID1, req.GetSourceDiskId())
	require.Equal(t, testDiskID2, req.GetTargetDiskId())
	require.Equal(t, storagev1.MergeStrategy_MERGE_STRATEGY_OURS, req.GetStrategy())
	require.Equal(t, map[string]storagev1.MergeStrategy{
		"/cfg/a=b.env": storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		"/docs":        storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}, req.GetPathStrategies())
	require.True(t, req.GetDryRun())
	require.NotNil(t, req.MaxBytes)
	require.Equal(t, uint64(4<<30), req.GetMaxBytes())
	require.Contains(t, out, "Dry Run")
	require.Contains(t, out, "4.00 GiB")

	// When: 正式合并 --json
	code, jsonOut, errOut := runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "--strategy=theirs", "--json")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var parsed diskMergeJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &parsed))
	require.True(t, parsed.Merged)
	require.False(t, parsed.DryRun)
	require.Equal(t, testDiskID1, parsed.SourceDiskID)
	require.Equal(t, testDiskID2, parsed.TargetDiskID)
	require.NotEmpty(t, parsed.MergeCommitID)
	require.NotNil(t, parsed.Disk)
	require.Equal(t, testDiskID2, parsed.Disk.DiskID)

	// When: -q 静默模式
	code, qOut, errOut := runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "-q")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, testDiskID2+"\n", qOut)
}

func TestDiskMerge_GivenECONFLICTReport_RendersHumanGuidanceAndStructuredJSONWithExit1(t *testing.T) {
	// Given: 构造 CodeAborted + ErrorInfo{Reason:"ECONFLICT"} + *storagev1.MergeConflictReport
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "branch", 2<<30, 4096)
	storage.seedDisk(testDiskID2, "main", 2<<30, 8192)

	conflictErr := connect.NewError(connect.CodeAborted, errors.New("merge conflicts detected"))
	errInfoDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: rpcerr.Domain,
		Reason: rpcerr.ReasonConflict,
	})
	require.NoError(t, err)
	conflictErr.AddDetail(errInfoDetail)

	reportDetail, err := connect.NewErrorDetail(&storagev1.MergeConflictReport{
		LcaCommitId:        testCommitID1,
		TotalConflicts:     3,
		ConflictsTruncated: true,
		Conflicts: []*storagev1.MergeConflict{
			{
				Path:   []byte("/src/main.go"),
				Kind:   storagev1.ConflictKind_CONFLICT_KIND_CONTENT,
				Base:   &storagev1.DiffNodeSummary{Ino: 10, Mode: 0o100644, Size: 100},
				Target: &storagev1.DiffNodeSummary{Ino: 10, Mode: 0o100644, Size: 120},
				Source: &storagev1.DiffNodeSummary{Ino: 10, Mode: 0o100644, Size: 130},
			},
			{
				Path:   []byte("/README.md"),
				Kind:   storagev1.ConflictKind_CONFLICT_KIND_ADD_ADD,
				Target: &storagev1.DiffNodeSummary{Ino: 20, Mode: 0o100644, Size: 50},
				Source: &storagev1.DiffNodeSummary{Ino: 30, Mode: 0o100644, Size: 60},
			},
		},
	})
	require.NoError(t, err)
	conflictErr.AddDetail(reportDetail)
	storage.forcedErr["MergeDisk"] = conflictErr
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: 人类可读输出
	code, out, errOut := runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "ECONFLICT")
	require.Contains(t, errOut, testCommitID1)
	require.Contains(t, errOut, "3")
	require.Contains(t, errOut, "/src/main.go")
	require.Contains(t, errOut, "CONTENT")
	require.Contains(t, errOut, "/README.md")
	require.Contains(t, errOut, "ADD_ADD")
	require.Contains(t, errOut, "--resolve")
	require.Contains(t, errOut, "bowl mount")
	require.Contains(t, errOut, "bowl umount")
	require.NotContains(t, errOut, "bowl unmount")

	// When: --json 结构化输出
	code, jsonOut, errOut := runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "--json")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, errOut)
	var parsed diskMergeJSON
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &parsed))
	require.False(t, parsed.Merged)
	require.NotNil(t, parsed.ConflictReport)
	require.Equal(t, testCommitID1, parsed.ConflictReport.LCACommitID)
	require.Equal(t, uint64(3), parsed.ConflictReport.TotalConflicts)
	require.True(t, parsed.ConflictReport.ConflictsTruncated)
	require.Len(t, parsed.ConflictReport.Conflicts, 2)
	require.Equal(t, "/src/main.go", parsed.ConflictReport.Conflicts[0].Path)
	require.Equal(t, "CONTENT", parsed.ConflictReport.Conflicts[0].Kind)
}

func TestDiskDiffMerge_GivenActiveOTelTracer_PropagatesAttrsAndErrorStatus(t *testing.T) {
	// Given
	exporter := setupDiskTestTracer(t)
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "src", 1<<30, 0)
	storage.seedDisk(testDiskID2, "tgt", 1<<30, 0)
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: diff span
	exporter.Reset()
	code, _, errOut := runCLI("disk", "diff", testDiskID1, "--target", testDiskID2)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	diffSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.diff")
	requireSpanStringAttr(t, diffSpan, "bowl.source_disk_id", testDiskID1)
	requireSpanStringAttr(t, diffSpan, "bowl.target_disk_id", testDiskID2)

	// When: merge span
	exporter.Reset()
	code, _, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	mergeSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.merge")
	requireSpanStringAttr(t, mergeSpan, "bowl.source_disk_id", testDiskID1)
	requireSpanStringAttr(t, mergeSpan, "bowl.target_disk_id", testDiskID2)

	// When: merge error sets span error
	exporter.Reset()
	storage.forcedErr["MergeDisk"] = connect.NewError(connect.CodeFailedPrecondition, errors.New("criss-cross"))
	code, _, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "criss-cross")
	errSpan := requireSpanByName(t, exporter.GetSpans(), "cli.disk.merge")
	require.Equal(t, codes.Error, errSpan.Status.Code)
}

func TestUnifiedDiff_GivenEdgeCases_HandlesEmptyIdenticalAndMultipleHunks(t *testing.T) {
	// Identical -> empty
	require.Empty(t, formatUnifiedDiff("a.txt", "a.txt", []byte("x\ny\n"), []byte("x\ny\n")))

	// Multiple separated hunks
	var oldBuf, newBuf bytes.Buffer
	for i := 1; i <= 15; i++ {
		line := strings.Repeat("x", 2) + string(rune('a'+i-1)) + "\n"
		oldBuf.WriteString(line)
		switch i {
		case 2:
			newBuf.WriteString("xxB-mod\n")
		case 14:
			newBuf.WriteString("xxN-mod\n")
		default:
			newBuf.WriteString(line)
		}
	}
	diffOut := formatUnifiedDiff("a/multi.txt", "b/multi.txt", oldBuf.Bytes(), newBuf.Bytes())
	require.Contains(t, diffOut, "--- a/multi.txt\n+++ b/multi.txt\n")
	require.Equal(t, 2, strings.Count(diffOut, "@@ "))
	require.Contains(t, diffOut, "-xxb\n+xxB-mod\n")
	require.Contains(t, diffOut, "-xxn\n+xxN-mod\n")
}

func TestDiskDiff_GivenTargetThreeWayPatchAndDeletions_DiffsTargetAgainstSource(t *testing.T) {
	// Given: 3-way diff --target --patch 比较 Target 与 Source 实时盘内容，并覆盖文件与软链删除
	storage := newFakeStorage()
	storage.seedLiveFile(testDiskID2, 301, []byte("target-v1\n"))
	storage.seedLiveFile(testDiskID1, 302, []byte("source-v2\n"))
	storage.seedLiveSymlink(testDiskID2, 303, "/gone/target")
	storage.diffResponse = &storagev1.DiffDiskResponse{
		LcaCommitId: testCommitID1,
		Summary:     &storagev1.DiffSummary{Modified: 1, Deleted: 1},
		Entries: []*storagev1.DiffEntry{
			{
				Path:     []byte("/app.conf"),
				Kind:     storagev1.DiffKind_DIFF_KIND_MODIFIED,
				Conflict: storagev1.ConflictKind_CONFLICT_KIND_CONTENT,
				Base:     &storagev1.DiffNodeSummary{Ino: 300, Mode: 0o100644, Size: 5},
				Target:   &storagev1.DiffNodeSummary{Ino: 301, Mode: 0o100644, Size: 10},
				Source:   &storagev1.DiffNodeSummary{Ino: 302, Mode: 0o100644, Size: 10},
			},
			{
				Path:   []byte("/old-link"),
				Kind:   storagev1.DiffKind_DIFF_KIND_DELETED,
				Target: &storagev1.DiffNodeSummary{Ino: 303, Mode: 0o120777, Size: 12},
			},
		},
	}
	withDiskStorageFake(t, storage)

	// When
	code, out, errOut := runCLI("disk", "diff", testDiskID1, "--target", testDiskID2, "-p")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, 0, storage.readCommitCalls)
	require.Contains(t, out, "-target-v1")
	require.Contains(t, out, "+source-v2")
	require.Contains(t, out, "+++ /dev/null (symlink)")
	require.Contains(t, out, "-/gone/target")
}

func TestDiskMerge_GivenServerErrors_MapsExpectedExitCodes(t *testing.T) {
	// Given
	storage := newFakeStorage()
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// EBUSY on CodeAborted (目标盘挂载/并发写入)
	ebusyErr := connect.NewError(connect.CodeAborted, errors.New("concurrent write on target disk"))
	ebusyDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: rpcerr.Domain,
		Reason: rpcerr.ReasonBusy,
	})
	require.NoError(t, err)
	ebusyErr.AddDetail(ebusyDetail)
	storage.forcedErr["MergeDisk"] = ebusyErr
	code, _, errOut := runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "目标磁盘")
	require.Contains(t, errOut, "并发写入或仍处于挂载状态")

	// EBUSY on CodeAborted (源盘读写挂载)
	srcBusyErr := connect.NewError(connect.CodeAborted, errors.New("source disk "+testDiskID1+" is currently mounted read-write"))
	srcBusyDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: rpcerr.Domain,
		Reason: rpcerr.ReasonBusy,
	})
	require.NoError(t, err)
	srcBusyErr.AddDetail(srcBusyDetail)
	storage.forcedErr["MergeDisk"] = srcBusyErr
	code, _, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "源磁盘")
	require.Contains(t, errOut, "读写挂载状态")
	require.Contains(t, errOut, "bowl umount")

	// CodeResourceExhausted
	storage.forcedErr["MergeDisk"] = connect.NewError(connect.CodeResourceExhausted, errors.New("quota exceeded"))
	code, _, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "容量配额不足")

	// CodeInvalidArgument -> exit 2
	storage.forcedErr["MergeDisk"] = connect.NewError(connect.CodeInvalidArgument, errors.New("invalid arg"))
	code, _, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2)
	require.Equal(t, 2, code)
	require.Contains(t, errOut, "无效的请求参数")
}

func TestDiskMerge_GivenDryRunWithConflictReport_ReturnsExit1AndRendersConflicts(t *testing.T) {
	// Given: dry-run 检测到未决冲突时服务端正常返回带 ConflictReport 的 MergeDiskResponse（Disk 为 nil）
	storage := newFakeStorage()
	storage.mergeResponse = &storagev1.MergeDiskResponse{
		LcaCommitId:        testCommitID1,
		EstimatedUsedBytes: 2048,
		TargetMaxBytes:     4096,
		ConflictReport: &storagev1.MergeConflictReport{
			LcaCommitId:    testCommitID1,
			TotalConflicts: 1,
			Conflicts: []*storagev1.MergeConflict{
				{
					Path:   []byte("/conflict.txt"),
					Kind:   storagev1.ConflictKind_CONFLICT_KIND_CONTENT,
					Base:   &storagev1.DiffNodeSummary{Ino: 10, Mode: 0o100644, Size: 12},
					Target: &storagev1.DiffNodeSummary{Ino: 11, Mode: 0o100644, Size: 18},
					Source: &storagev1.DiffNodeSummary{Ino: 12, Mode: 0o100644, Size: 24},
				},
			},
		},
	}
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When: 人类可读模式
	code, out, errOut := runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "--dry-run")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "[Dry Run]")
	require.Contains(t, errOut, "未决冲突")
	require.Contains(t, errOut, "CONTENT")
	require.Contains(t, errOut, "/conflict.txt")
	require.Contains(t, errOut, "消解指导")

	// When: --json 模式
	code, out, errOut = runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "--dry-run", "--json")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, errOut)
	var payload diskMergeJSON
	require.NoError(t, json.Unmarshal([]byte(out), &payload))
	require.True(t, payload.DryRun)
	require.False(t, payload.Merged)
	require.Equal(t, testCommitID1, payload.LCACommitID)
	require.NotNil(t, payload.ConflictReport)
	require.EqualValues(t, 1, payload.ConflictReport.TotalConflicts)
	require.Len(t, payload.ConflictReport.Conflicts, 1)
	require.Equal(t, "/conflict.txt", payload.ConflictReport.Conflicts[0].Path)
}

func TestDiskMerge_GivenDryRunQuietWithoutConflicts_PrintsTargetDiskIDAndExit0(t *testing.T) {
	// Given: dry-run 无冲突时服务端返回 Disk=nil 且 ConflictReport=nil
	storage := newFakeStorage()
	storage.mergeResponse = &storagev1.MergeDiskResponse{
		LcaCommitId:        testCommitID1,
		EstimatedUsedBytes: 1024,
		TargetMaxBytes:     4096,
	}
	withDiskStorageFake(t, storage)
	withMissingMountInfo(t)

	// When
	code, out, errOut := runCLI("disk", "merge", testDiskID1, "--into", testDiskID2, "--dry-run", "-q")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, testDiskID2+"\n", out)
}

func TestDiskDiff_GivenSpecialFilePatch_SkipsTextDiffAndReportsTypeChange(t *testing.T) {
	// Given: FIFO(0010644) 变更及仅特殊文件改动场景不应把特殊文件当 0 字节文本执行 Unified Diff
	storage := newFakeStorage()
	storage.seedLiveFile(testDiskID1, 402, []byte("now-a-regular-file\n"))
	storage.diffResponse = &storagev1.DiffDiskResponse{
		LcaCommitId: testCommitID1,
		Summary:     &storagev1.DiffSummary{Modified: 2, Added: 1},
		Entries: []*storagev1.DiffEntry{
			{
				Path:   []byte("/pipe-to-file"),
				Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
				Base:   &storagev1.DiffNodeSummary{Ino: 401, Mode: 0o010644, Size: 0},
				Source: &storagev1.DiffNodeSummary{Ino: 402, Mode: 0o100644, Size: 19},
			},
			{
				Path:   []byte("/new-sock"),
				Kind:   storagev1.DiffKind_DIFF_KIND_ADDED,
				Source: &storagev1.DiffNodeSummary{Ino: 403, Mode: 0o140755, Size: 0},
			},
			{
				Path:   []byte("/same-fifo-meta"),
				Kind:   storagev1.DiffKind_DIFF_KIND_MODIFIED,
				Base:   &storagev1.DiffNodeSummary{Ino: 404, Mode: 0o010644, Size: 0},
				Source: &storagev1.DiffNodeSummary{Ino: 404, Mode: 0o010600, Size: 0},
			},
		},
	}
	withDiskStorageFake(t, storage)

	// When
	code, out, errOut := runCLI("disk", "diff", testDiskID1, "--patch")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "diff a/pipe-to-file b/pipe-to-file\nFile type changed\n")
	require.NotContains(t, out, "+now-a-regular-file")
	require.NotContains(t, out, "+++ b/new-sock")
	require.NotContains(t, out, "+++ b/same-fifo-meta")
}
