package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/starsrc/pkg/serverx"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	diskMergeTimeout      = 60 * time.Second
	maxMergeResolvePairs  = 4096
	maxMergeResolveKeyLen = 4096
)

const diskMergeUsageHelp = `用法:
  bowl disk merge <source-disk-id> --into <target-disk-id> [flags]

参数:
  <source-disk-id>        源虚拟磁盘 UUID（必填，非 Nil UUID）

标志:
  --into string           目标虚拟磁盘 UUID（必填，非 Nil UUID，且不能与源盘相同）
  --strategy string       全局冲突解决策略: fail|ours|theirs (默认 fail)
  --resolve string        按路径覆写冲突解决策略 <path>=ours|theirs (可重复指定)
  --dry-run               仅执行冲突与容量预演检查，不实际修改目标盘
  -f, --force             忽略在线挂载门禁强制执行合并
  --size string           合并后目标盘新容量配额上限 (支持 B/K/KB/KiB/M/MB/MiB/G/GB/GiB/T/TB/TiB/P/PB/PiB)
  -q, --quiet             静默模式，仅输出合并后的目标磁盘 ID
  --json                  以 JSON 格式输出合并结果或冲突报告
  --endpoint string       StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string          访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help              查看帮助信息
`

type mergeConflictJSON struct {
	Path   string               `json:"path"`
	Kind   string               `json:"kind"`
	Base   *diffNodeSummaryJSON `json:"base,omitempty"`
	Target *diffNodeSummaryJSON `json:"target,omitempty"`
	Source *diffNodeSummaryJSON `json:"source,omitempty"`
}

type mergeConflictReportJSON struct {
	LCACommitID        string              `json:"lca_commit_id,omitempty"`
	TotalConflicts     uint64              `json:"total_conflicts"`
	ConflictsTruncated bool                `json:"conflicts_truncated"`
	Conflicts          []mergeConflictJSON `json:"conflicts"`
}

type diskMergeJSON struct {
	SourceDiskID       string                   `json:"source_disk_id"`
	TargetDiskID       string                   `json:"target_disk_id"`
	DryRun             bool                     `json:"dry_run"`
	Merged             bool                     `json:"merged"`
	LCACommitID        string                   `json:"lca_commit_id,omitempty"`
	MergeCommitID      string                   `json:"merge_commit_id,omitempty"`
	EstimatedUsedBytes uint64                   `json:"estimated_used_bytes"`
	TargetMaxBytes     uint64                   `json:"target_max_bytes"`
	Disk               *diskJSON                `json:"disk,omitempty"`
	ConflictReport     *mergeConflictReportJSON `json:"conflict_report,omitempty"`
}

var diskMergeSpec = diskSubcommandSpec{
	name:        "merge",
	usageHelp:   diskMergeUsageHelp,
	posArgLabel: "<source-disk-id>",
	uuidLabel:   "source-disk-id",
	allowQuiet:  true,
	spanName:    "cli.disk.merge",
	timeout:     diskMergeTimeout,
	newAdapter:  func() diskRequestAdapter { return &diskMergeAdapter{} },
}

type diskMergeAdapter struct {
	sourceDiskID   string
	intoRaw        *string
	targetDiskID   string
	strategyRaw    *string
	strategy       storagev1.MergeStrategy
	resolveRaw     []string
	pathStrategies map[string]storagev1.MergeStrategy
	dryRun         bool
	force          bool
	sizeRaw        *string
	sizePtr        *uint64
}

func (a *diskMergeAdapter) parseFlag(name, val string, hasVal bool, args []string, idx *int) (bool, error) {
	switch name {
	case "into":
		v, err := takeFlagValue(args, idx, name, val, hasVal)
		if err != nil {
			return true, err
		}
		a.intoRaw = &v
		return true, nil
	case "strategy":
		v, err := takeFlagValue(args, idx, name, val, hasVal)
		if err != nil {
			return true, err
		}
		a.strategyRaw = &v
		return true, nil
	case "resolve":
		v, err := takeFlagValue(args, idx, name, val, hasVal)
		if err != nil {
			return true, err
		}
		a.resolveRaw = append(a.resolveRaw, v)
		return true, nil
	case "dry-run":
		b, err := parseBoolFlag(val, hasVal)
		if err != nil {
			return true, err
		}
		a.dryRun = b
		return true, nil
	case "f", "force":
		b, err := parseBoolFlag(val, hasVal)
		if err != nil {
			return true, err
		}
		a.force = b
		return true, nil
	case "size":
		v, err := takeFlagValue(args, idx, name, val, hasVal)
		if err != nil {
			return true, err
		}
		a.sizeRaw = &v
		return true, nil
	default:
		return false, nil
	}
}

func (a *diskMergeAdapter) finalize(posArgs []string) (bool, error) {
	a.sourceDiskID = posArgs[0]
	if a.intoRaw == nil || strings.TrimSpace(*a.intoRaw) == "" {
		return true, errors.New("必须指定 --into 参数")
	}
	if _, err := parseNonNilUUID(*a.intoRaw, "target-disk-id"); err != nil {
		return false, err
	}
	if *a.intoRaw == a.sourceDiskID {
		return false, errors.New("目标磁盘不能与源磁盘相同")
	}
	a.targetDiskID = *a.intoRaw

	if a.strategyRaw != nil {
		strategy, err := parseMergeStrategy(*a.strategyRaw)
		if err != nil {
			return false, err
		}
		a.strategy = strategy
	}

	if len(a.resolveRaw) > 0 {
		a.pathStrategies = make(map[string]storagev1.MergeStrategy, len(a.resolveRaw))
		for _, item := range a.resolveRaw {
			pathKey, sideStrategy, err := parseResolveRule(item)
			if err != nil {
				return false, err
			}
			a.pathStrategies[pathKey] = sideStrategy
			if len(a.pathStrategies) > maxMergeResolvePairs {
				return false, fmt.Errorf("--resolve 规则数量不能超过 %d 条", maxMergeResolvePairs)
			}
		}
	}

	if a.sizeRaw != nil {
		maxBytes, err := ParseSize(*a.sizeRaw)
		if err != nil {
			return false, fmt.Errorf("无效的 --size 参数: %w", err)
		}
		a.sizePtr = &maxBytes
	}
	return false, nil
}

func parseMergeStrategy(raw string) (storagev1.MergeStrategy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "fail":
		return storagev1.MergeStrategy_MERGE_STRATEGY_FAIL, nil
	case "ours":
		return storagev1.MergeStrategy_MERGE_STRATEGY_OURS, nil
	case "theirs":
		return storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS, nil
	default:
		return storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, fmt.Errorf("无效的 --strategy 参数 %q，可选值: fail, ours, theirs", raw)
	}
}

func parseResolveRule(raw string) (string, storagev1.MergeStrategy, error) {
	sep := strings.LastIndex(raw, "=")
	if sep <= 0 {
		return "", storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, fmt.Errorf("无效的 --resolve 参数 %q，格式必须为 <path>=ours|theirs", raw)
	}
	pathKey := raw[:sep]
	sideRaw := strings.ToLower(strings.TrimSpace(raw[sep+1:]))
	if len(pathKey) > maxMergeResolveKeyLen {
		return "", storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, fmt.Errorf("--resolve 路径长度不能超过 %d 字节", maxMergeResolveKeyLen)
	}
	if strings.Contains(pathKey, "\x00") {
		return "", storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, errors.New("--resolve 路径不能包含空字节 (NUL)")
	}
	switch sideRaw {
	case "ours":
		return pathKey, storagev1.MergeStrategy_MERGE_STRATEGY_OURS, nil
	case "theirs":
		return pathKey, storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS, nil
	default:
		return "", storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, fmt.Errorf("无效的 --resolve 参数 %q，策略必须为 ours 或 theirs", raw)
	}
}

func (a *diskMergeAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.source_disk_id", a.sourceDiskID),
		attribute.String("bowl.target_disk_id", a.targetDiskID),
	}
}

func (a *diskMergeAdapter) preRPC(ports diskPorts, _ io.Writer) *diskCLIError {
	if a.dryRun || a.force {
		return nil
	}

	targetInfo, err := ports.findMountpoint(a.targetDiskID)
	if err != nil {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("检查本机挂载表失败: %v", err),
		}
	}
	if targetInfo.Mounted && !targetInfo.ReadOnly {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("目标磁盘 %s 当前正在本机挂载点 %s 处于读写挂载状态，请先执行 bowl umount %s 卸载后再合并", a.targetDiskID, targetInfo.Mountpoint, targetInfo.Mountpoint),
		}
	}

	srcInfo, srcErr := ports.findMountpoint(a.sourceDiskID)
	if srcErr != nil {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("检查本机挂载表失败: %v", srcErr),
		}
	}
	if srcInfo.Mounted && !srcInfo.ReadOnly {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("源磁盘 %s 当前正在本机挂载点 %s 处于读写挂载状态，请先执行 bowl umount %s 卸载后再合并", a.sourceDiskID, srcInfo.Mountpoint, srcInfo.Mountpoint),
		}
	}
	return nil
}

func (a *diskMergeAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId:   a.sourceDiskID,
		TargetDiskId:   a.targetDiskID,
		Strategy:       a.strategy,
		PathStrategies: a.pathStrategies,
		DryRun:         a.dryRun,
		MaxBytes:       a.sizePtr,
		Force:          a.force,
	}))
	if err != nil {
		return diskOutcome{}, a.mapMergeRPCError(err)
	}

	msg := resp.Msg
	var diskPtr *diskJSON
	if msg.GetDisk() != nil {
		dj := toDiskJSON(msg.GetDisk())
		diskPtr = &dj
	}

	report := msg.GetConflictReport()
	reportJSON := toMergeConflictReportJSON(report)
	payload := diskMergeJSON{
		SourceDiskID:       a.sourceDiskID,
		TargetDiskID:       a.targetDiskID,
		DryRun:             a.dryRun,
		Merged:             !a.dryRun,
		LCACommitID:        msg.GetLcaCommitId(),
		MergeCommitID:      msg.GetMergeCommitId(),
		EstimatedUsedBytes: msg.GetEstimatedUsedBytes(),
		TargetMaxBytes:     msg.GetTargetMaxBytes(),
		Disk:               diskPtr,
		ConflictReport:     reportJSON,
	}

	if report != nil && (report.GetTotalConflicts() > 0 || len(report.GetConflicts()) > 0) {
		payload.Merged = false
		if payload.LCACommitID == "" && reportJSON != nil {
			payload.LCACommitID = reportJSON.LCACommitID
		}
		return diskOutcome{}, &diskCLIError{
			exitCode:    1,
			jsonPayload: payload,
			renderStderr: func(w io.Writer) {
				renderMergeConflictHuman(w, a.sourceDiskID, a.targetDiskID, reportJSON, a.dryRun)
			},
		}
	}

	return diskOutcome{
		disk:        msg.GetDisk(),
		quietID:     a.targetDiskID,
		jsonPayload: payload,
		renderHuman: func(w io.Writer) {
			renderDiskMergeHuman(w, &payload, msg.GetDisk())
		},
	}, nil
}

func (a *diskMergeAdapter) mapMergeRPCError(err error) *diskCLIError {
	connectErr, isConnect := errors.AsType[*connect.Error](err)
	info := serverx.ExtractErrorInfo(err)
	if isConnect && connectErr.Code() == connect.CodeAborted && info.GetReason() == "ECONFLICT" {
		report := extractMergeConflictReport(connectErr)
		reportJSON := toMergeConflictReportJSON(report)
		if reportJSON == nil {
			reportJSON = &mergeConflictReportJSON{Conflicts: []mergeConflictJSON{}}
		}
		payload := diskMergeJSON{
			SourceDiskID:   a.sourceDiskID,
			TargetDiskID:   a.targetDiskID,
			DryRun:         a.dryRun,
			Merged:         false,
			LCACommitID:    reportJSON.LCACommitID,
			ConflictReport: reportJSON,
		}
		return &diskCLIError{
			exitCode:    1,
			cause:       err,
			jsonPayload: payload,
			renderStderr: func(w io.Writer) {
				renderMergeConflictHuman(w, a.sourceDiskID, a.targetDiskID, reportJSON, a.dryRun)
			},
		}
	}
	if isConnect && connectErr.Code() == connect.CodeAborted && info.GetReason() == "EBUSY" {
		lowerMsg := strings.ToLower(connectErr.Message())
		if strings.Contains(lowerMsg, "source disk") || strings.Contains(lowerMsg, strings.ToLower(a.sourceDiskID)) {
			return &diskCLIError{
				exitCode: 1,
				msg:      fmt.Sprintf("源磁盘 %s 当前处于读写挂载状态，请先执行 bowl umount 卸载后再合并（或使用 --force 强制合并）: %v", a.sourceDiskID, err),
				cause:    err,
			}
		}
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("目标磁盘 %s 在合并期间发生并发写入或仍处于挂载状态，请先执行 bowl umount 卸载并重试: %v", a.targetDiskID, err),
			cause:    err,
		}
	}
	if isConnect && connectErr.Code() == connect.CodeResourceExhausted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("目标磁盘 %s 容量配额不足，请通过 --size 扩容后重试: %v", a.targetDiskID, err),
			cause:    err,
		}
	}
	return mapDiskRPCError(err, diskRPCErrorPolicy{
		defaultOp:       "合并虚拟磁盘失败",
		checkInvalidArg: true,
		preconditionMsg: func(_ string, rpcErr error) string {
			return fmt.Sprintf("合并前置条件校验失败: %v", rpcErr)
		},
	})
}

func extractMergeConflictReport(connectErr *connect.Error) *storagev1.MergeConflictReport {
	if connectErr == nil {
		return nil
	}
	for _, detail := range connectErr.Details() {
		msg, err := detail.Value()
		if err != nil {
			continue
		}
		if report, ok := msg.(*storagev1.MergeConflictReport); ok {
			return report
		}
	}
	return nil
}

func toMergeConflictReportJSON(r *storagev1.MergeConflictReport) *mergeConflictReportJSON {
	if r == nil {
		return nil
	}
	items := make([]mergeConflictJSON, 0, len(r.GetConflicts()))
	for _, c := range r.GetConflicts() {
		items = append(items, mergeConflictJSON{
			Path:   string(c.GetPath()),
			Kind:   conflictKindLabel(c.GetKind()),
			Base:   toDiffNodeSummaryJSON(c.GetBase()),
			Target: toDiffNodeSummaryJSON(c.GetTarget()),
			Source: toDiffNodeSummaryJSON(c.GetSource()),
		})
	}
	return &mergeConflictReportJSON{
		LCACommitID:        r.GetLcaCommitId(),
		TotalConflicts:     r.GetTotalConflicts(),
		ConflictsTruncated: r.GetConflictsTruncated(),
		Conflicts:          items,
	}
}

func renderDiskMergeHuman(w io.Writer, res *diskMergeJSON, disk *storagev1.Disk) {
	if res.DryRun {
		fmt.Fprintln(w, "[Dry Run] 合并预演通过，无未决冲突（未修改目标盘）")
	} else {
		fmt.Fprintln(w, "合并成功")
	}
	fmt.Fprintf(w, "Source Disk:    %s\n", res.SourceDiskID)
	fmt.Fprintf(w, "Target Disk:    %s\n", res.TargetDiskID)
	if res.LCACommitID != "" {
		fmt.Fprintf(w, "LCA Commit:     %s\n", res.LCACommitID)
	}
	if res.MergeCommitID != "" {
		fmt.Fprintf(w, "Merge Commit:   %s\n", res.MergeCommitID)
	}
	fmt.Fprintf(
		w,
		"Estimated Used: %d (%s) / %d (%s) [%.2f%%]\n",
		res.EstimatedUsedBytes, FormatSize(res.EstimatedUsedBytes),
		res.TargetMaxBytes, FormatSize(res.TargetMaxBytes),
		diskUsagePct(res.EstimatedUsedBytes, res.TargetMaxBytes),
	)
	if disk != nil {
		fmt.Fprintln(w)
		printDiskDetail(w, disk)
	}
}

func renderMergeConflictHuman(w io.Writer, sourceDiskID, targetDiskID string, report *mergeConflictReportJSON, dryRun bool) {
	if dryRun {
		fmt.Fprintf(w, "[Dry Run] 合并预演中止 (ECONFLICT)，共检测到 %d 处未决冲突\n", report.TotalConflicts)
	} else {
		fmt.Fprintf(w, "错误: 合并中止 (ECONFLICT)，共检测到 %d 处未决冲突\n", report.TotalConflicts)
	}
	if report.LCACommitID != "" {
		fmt.Fprintf(w, "LCA Commit: %s\n", report.LCACommitID)
	}
	if len(report.Conflicts) > 0 {
		fmt.Fprintln(w)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "CONFLICT\tPATH\tBASE\tTARGET\tSOURCE")
		for _, c := range report.Conflicts {
			fmt.Fprintf(
				tw,
				"%s\t%s\t%s\t%s\t%s\n",
				c.Kind,
				c.Path,
				formatConflictNodeCell(c.Base),
				formatConflictNodeCell(c.Target),
				formatConflictNodeCell(c.Source),
			)
		}
		_ = tw.Flush()
	}
	if report.ConflictsTruncated {
		fmt.Fprintf(w, "\n注意: 冲突列表已截断，仅展示前 %d 条；完整清单请执行:\n  bowl disk diff %s --target %s --conflicts-only\n",
			len(report.Conflicts), sourceDiskID, targetDiskID)
	}

	fmt.Fprintf(w, "\n消解指导:\n"+
		"  1. 若按文件级快速裁决，请补充 --resolve <path>=ours|theirs 或 --strategy ours|theirs 后重试。\n"+
		"  2. 若需人工合并文本内容，建议先将目标盘变更合入分支盘并在本地挂载沙箱中编辑验证:\n"+
		"     bowl disk merge %s --into %s --strategy ours\n"+
		"     bowl mount %s <mountpoint>   # 编辑消解并验证\n"+
		"     bowl umount <mountpoint>\n"+
		"     bowl disk merge %s --into %s\n",
		targetDiskID, sourceDiskID,
		sourceDiskID,
		sourceDiskID, targetDiskID,
	)
}

func formatConflictNodeCell(n *diffNodeSummaryJSON) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprintf("ino=%d(%s)", n.Ino, FormatSize(n.Size))
}
