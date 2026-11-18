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
	"github.com/spf13/cobra"
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
	use:        "merge <source-disk-id>",
	short:      "Three-way merge changes from a source disk into a target disk",
	args:       cobra.ExactArgs(1),
	uuidLabel:  "source-disk-id",
	allowQuiet: true,
	spanName:   "cli.disk.merge",
	timeout:    diskMergeTimeout,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskMergeAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.intoRaw, "into", "", "Required target virtual disk UUID, distinct from source disk")
		flags.StringVar(&a.strategyRaw, "strategy", "fail", "Global conflict resolution strategy: fail, ours, or theirs")
		flags.StringArrayVar(&a.resolveRaw, "resolve", nil, "Per-path conflict strategy override as <path>=ours|theirs")
		flags.BoolVar(&a.dryRun, "dry-run", false, "Run conflict and capacity checks without modifying target disk")
		flags.BoolVarP(&a.force, "force", "f", false, "Ignore online mount guards and force merge")
		flags.StringVar(&a.sizeRaw, "size", "", "New capacity quota limit for the target disk after merge")
		return a
	},
}

type diskMergeAdapter struct {
	sourceDiskID   string
	intoRaw        string
	targetDiskID   string
	strategyRaw    string
	strategy       storagev1.MergeStrategy
	resolveRaw     []string
	pathStrategies map[string]storagev1.MergeStrategy
	dryRun         bool
	force          bool
	sizeRaw        string
	sizePtr        *uint64
}

func (a *diskMergeAdapter) finalize(cmd *cobra.Command, posArgs []string) (bool, error) {
	a.sourceDiskID = posArgs[0]
	flags := cmd.Flags()

	if !flags.Changed("into") || strings.TrimSpace(a.intoRaw) == "" {
		return true, errors.New("--into is required")
	}
	if _, err := parseNonNilUUID(a.intoRaw, "target-disk-id"); err != nil {
		return false, err
	}
	if a.intoRaw == a.sourceDiskID {
		return false, errors.New("target disk cannot be the same as source disk")
	}
	a.targetDiskID = a.intoRaw

	if flags.Changed("strategy") {
		strategy, err := parseMergeStrategy(a.strategyRaw)
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
				return false, fmt.Errorf("--resolve rule count cannot exceed %d", maxMergeResolvePairs)
			}
		}
	}

	if flags.Changed("size") {
		maxBytes, err := ParseSize(a.sizeRaw)
		if err != nil {
			return false, fmt.Errorf("invalid --size: %w", err)
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
		return storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, fmt.Errorf("invalid --strategy %q: must be fail, ours, or theirs", raw)
	}
}

func parseResolveRule(raw string) (string, storagev1.MergeStrategy, error) {
	sep := strings.LastIndex(raw, "=")
	if sep <= 0 {
		return "", storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, fmt.Errorf("invalid --resolve %q: expected <path>=ours|theirs", raw)
	}
	pathKey := raw[:sep]
	sideRaw := strings.ToLower(strings.TrimSpace(raw[sep+1:]))
	if len(pathKey) > maxMergeResolveKeyLen {
		return "", storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, fmt.Errorf("--resolve path cannot exceed %d bytes", maxMergeResolveKeyLen)
	}
	if strings.Contains(pathKey, "\x00") {
		return "", storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, errors.New("--resolve path cannot contain NUL bytes")
	}
	switch sideRaw {
	case "ours":
		return pathKey, storagev1.MergeStrategy_MERGE_STRATEGY_OURS, nil
	case "theirs":
		return pathKey, storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS, nil
	default:
		return "", storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED, fmt.Errorf("invalid --resolve %q: strategy must be ours or theirs", raw)
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
			msg:      fmt.Sprintf("check local mount table: %v", err),
		}
	}
	if targetInfo.Mounted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("target disk %s is currently mounted at %s; run bowl umount %s before merging", a.targetDiskID, targetInfo.Mountpoint, targetInfo.Mountpoint),
		}
	}

	srcInfo, srcErr := ports.findMountpoint(a.sourceDiskID)
	if srcErr != nil {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("check local mount table: %v", srcErr),
		}
	}
	if srcInfo.Mounted && !srcInfo.ReadOnly {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("source disk %s is currently mounted read-write at %s; run bowl umount %s before merging", a.sourceDiskID, srcInfo.Mountpoint, srcInfo.Mountpoint),
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
				msg:      fmt.Sprintf("source disk %s is currently mounted read-write; run bowl umount before merging or pass --force: %v", a.sourceDiskID, err),
				cause:    err,
			}
		}
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("target disk %s had concurrent writes or is still mounted; run bowl umount and retry: %v", a.targetDiskID, err),
			cause:    err,
		}
	}
	if isConnect && connectErr.Code() == connect.CodeResourceExhausted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("target disk %s quota exceeded; expand with --size and retry: %v", a.targetDiskID, err),
			cause:    err,
		}
	}
	return mapDiskRPCError(err, diskRPCErrorPolicy{
		defaultOp:       "merge virtual disks failed",
		checkInvalidArg: true,
		preconditionMsg: func(_ string, rpcErr error) string {
			return fmt.Sprintf("merge precondition failed: %v", rpcErr)
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
		fmt.Fprintln(w, "[Dry Run] Merge check passed with no unresolved conflicts; target disk was not modified")
	} else {
		fmt.Fprintln(w, "Merged successfully")
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
		fmt.Fprintf(w, "[Dry Run] Merge check aborted with ECONFLICT: %d unresolved conflicts detected\n", report.TotalConflicts)
	} else {
		fmt.Fprintf(w, "Error: merge aborted with ECONFLICT: %d unresolved conflicts detected\n", report.TotalConflicts)
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
		fmt.Fprintf(w, "\nNote: conflict list truncated to the first %d entries. Run this command for the full list:\n  bowl disk diff %s --target %s --conflicts-only\n",
			len(report.Conflicts), sourceDiskID, targetDiskID)
	}

	fmt.Fprintf(w, "\nResolution steps:\n"+
		"  1. For file-level resolution, re-run with --resolve <path>=ours|theirs or --strategy ours|theirs.\n"+
		"  2. For manual text conflict resolution, merge target changes into the source disk and edit in a local mount:\n"+
		"     bowl disk merge %s --into %s --strategy ours\n"+
		"     bowl mount %s <mountpoint>\n"+
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
