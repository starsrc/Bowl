package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
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
	branchMergeTimeout    = 60 * time.Second
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
	LCACommitID        uint64              `json:"lca_commit_id,omitempty"`
	TotalConflicts     uint64              `json:"total_conflicts"`
	ConflictsTruncated bool                `json:"conflicts_truncated"`
	Conflicts          []mergeConflictJSON `json:"conflicts"`
}

type branchMergeJSON struct {
	DiskID         string                   `json:"disk_id"`
	SourceBranch   string                   `json:"source_branch"`
	TargetBranch   string                   `json:"target_branch"`
	DryRun         bool                     `json:"dry_run"`
	Merged         bool                     `json:"merged"`
	FastForward    bool                     `json:"fast_forward"`
	UpToDate       bool                     `json:"up_to_date"`
	LCACommitID    uint64                   `json:"lca_commit_id,omitempty"`
	CommitID       uint64                   `json:"commit_id,omitempty"`
	WriterEpoch    uint64                   `json:"writer_epoch,omitempty"`
	Branch         *branchJSON              `json:"branch,omitempty"`
	Commit         *commitJSON              `json:"commit,omitempty"`
	ConflictReport *mergeConflictReportJSON `json:"conflict_report,omitempty"`
}

var branchMergeSpec = diskSubcommandSpec{
	use:              "merge <source-branch>",
	short:            "Three-way merge changes from a source branch into a target branch",
	args:             cobra.ExactArgs(1),
	requireDiskScope: true,
	allowQuiet:       true,
	quietHelp:        "Quiet mode, output only the resulting commit ID",
	spanName:         "cli.branch.merge",
	timeout:          branchMergeTimeout,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &branchMergeAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.intoBranch, "into", "", "Required target branch name, distinct from source branch")
		flags.StringVar(&a.strategyRaw, "strategy", "fail", "Global conflict resolution strategy: fail, ours, or theirs")
		flags.StringArrayVar(&a.resolveRaw, "resolve", nil, "Per-path conflict strategy override as <path>=ours|theirs")
		flags.BoolVar(&a.noFastForward, "no-ff", false, "Create a merge commit even when fast-forward is possible")
		flags.BoolVar(&a.dryRun, "dry-run", false, "Run conflict checks without modifying the target branch")
		flags.StringVarP(&a.message, "message", "m", "", "Optional merge commit message")
		return a
	},
}

type branchMergeAdapter struct {
	diskID         string
	sourceBranch   string
	intoBranch     string
	strategyRaw    string
	strategy       storagev1.MergeStrategy
	resolveRaw     []string
	pathStrategies map[string]storagev1.MergeStrategy
	noFastForward  bool
	dryRun         bool
	message        string
}

func (a *branchMergeAdapter) finalize(cmd *cobra.Command, diskID string, posArgs []string) (bool, error) {
	a.diskID = diskID
	a.sourceBranch = posArgs[0]
	flags := cmd.Flags()

	if err := validateBranchName(a.sourceBranch, "source-branch"); err != nil {
		return false, err
	}
	if !flags.Changed("into") || strings.TrimSpace(a.intoBranch) == "" {
		return true, errors.New("--into is required")
	}
	if err := validateBranchName(a.intoBranch, "--into"); err != nil {
		return false, err
	}
	if a.intoBranch == a.sourceBranch {
		return false, errors.New("target branch cannot be the same as source branch")
	}

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

	if err := validateCommitMessage(a.message); err != nil {
		return false, err
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

func (a *branchMergeAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.String("bowl.source_branch", a.sourceBranch),
		attribute.String("bowl.target_branch", a.intoBranch),
	}
}

func (a *branchMergeAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *branchMergeAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.MergeBranch(ctx, connect.NewRequest(&storagev1.MergeBranchRequest{
		DiskId:         a.diskID,
		SourceBranch:   a.sourceBranch,
		TargetBranch:   a.intoBranch,
		Strategy:       a.strategy,
		PathStrategies: a.pathStrategies,
		DryRun:         a.dryRun,
		NoFastForward:  a.noFastForward,
		Message:        a.message,
	}))
	if err != nil {
		return diskOutcome{}, a.mapMergeRPCError(err)
	}

	msg := resp.Msg
	var branchPtr *branchJSON
	if msg.GetBranch() != nil {
		bj := toBranchJSON(msg.GetBranch())
		branchPtr = &bj
	}
	var commitPtr *commitJSON
	if msg.GetCommit() != nil {
		cj := toCommitJSON(msg.GetCommit())
		commitPtr = &cj
	}

	report := msg.GetConflictReport()
	reportJSON := toMergeConflictReportJSON(report)
	payload := branchMergeJSON{
		DiskID:         a.diskID,
		SourceBranch:   a.sourceBranch,
		TargetBranch:   a.intoBranch,
		DryRun:         a.dryRun,
		Merged:         !a.dryRun,
		FastForward:    msg.GetFastForward(),
		UpToDate:       msg.GetUpToDate(),
		LCACommitID:    msg.GetLcaCommitId(),
		CommitID:       msg.GetCommitId(),
		WriterEpoch:    msg.GetWriterEpoch(),
		Branch:         branchPtr,
		Commit:         commitPtr,
		ConflictReport: reportJSON,
	}

	if report != nil && (report.GetTotalConflicts() > 0 || len(report.GetConflicts()) > 0) {
		payload.Merged = false
		if payload.LCACommitID == 0 && reportJSON != nil {
			payload.LCACommitID = reportJSON.LCACommitID
		}
		return diskOutcome{}, &diskCLIError{
			exitCode:    1,
			jsonPayload: payload,
			renderStderr: func(w io.Writer) {
				renderMergeConflictHuman(w, a.diskID, a.sourceBranch, a.intoBranch, reportJSON, a.dryRun)
			},
		}
	}

	quietCommitID := msg.GetCommitId()
	if quietCommitID == 0 && msg.GetCommit() != nil {
		quietCommitID = msg.GetCommit().GetCommitId()
	}
	if quietCommitID == 0 && msg.GetBranch() != nil {
		quietCommitID = msg.GetBranch().GetHeadCommitId()
	}
	var quietID string
	if quietCommitID > 0 {
		quietID = strconv.FormatUint(quietCommitID, 10)
	}

	return diskOutcome{
		quietID:     quietID,
		jsonPayload: payload,
		renderHuman: func(w io.Writer) {
			renderBranchMergeHuman(w, &payload, msg.GetBranch())
		},
	}, nil
}

func (a *branchMergeAdapter) mapMergeRPCError(err error) *diskCLIError {
	connectErr, isConnect := errors.AsType[*connect.Error](err)
	info := serverx.ExtractErrorInfo(err)
	reason := info.GetReason()

	if isConnect && connectErr.Code() == connect.CodeAborted && reason == "ECONFLICT" {
		report := extractMergeConflictReport(connectErr)
		reportJSON := toMergeConflictReportJSON(report)
		if reportJSON == nil {
			reportJSON = &mergeConflictReportJSON{Conflicts: []mergeConflictJSON{}}
		}
		payload := branchMergeJSON{
			DiskID:         a.diskID,
			SourceBranch:   a.sourceBranch,
			TargetBranch:   a.intoBranch,
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
				renderMergeConflictHuman(w, a.diskID, a.sourceBranch, a.intoBranch, reportJSON, a.dryRun)
			},
		}
	}

	if reason == "EDIRTY" {
		return &diskCLIError{
			exitCode: 1,
			msg: fmt.Sprintf(
				"branch has uncommitted working-layer changes (EDIRTY); commit with `bowl commit create -d %s --branch <branch>` or discard with `bowl branch reset <branch> -d %s --to <commit-id>` before merging: %v",
				a.diskID, a.diskID, err,
			),
			cause: err,
		}
	}

	if isConnect && (connectErr.Code() == connect.CodeAborted || connectErr.Code() == connect.CodeFailedPrecondition) && reason == "EBUSY" {
		return &diskCLIError{
			exitCode: 1,
			msg: fmt.Sprintf(
				"branch has an active mount lease or concurrent writes; unmount or evict leases with `bowl lease list -d %s` before merging: %v",
				a.diskID, err,
			),
			cause: err,
		}
	}

	if isConnect && connectErr.Code() == connect.CodeResourceExhausted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("disk %s quota exceeded; expand with `bowl disk update %s --size <new-size>` and retry: %v", a.diskID, a.diskID, err),
			cause:    err,
		}
	}

	return mapDiskRPCError(err, diskRPCErrorPolicy{
		diskID:          a.diskID,
		defaultOp:       "merge branches failed",
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

func renderBranchMergeHuman(w io.Writer, res *branchMergeJSON, branch *storagev1.Branch) {
	switch {
	case res.DryRun:
		fmt.Fprintln(w, "[Dry Run] Merge check passed with no unresolved conflicts; target branch was not modified")
	case res.UpToDate:
		fmt.Fprintln(w, "Already up to date")
	case res.FastForward:
		fmt.Fprintln(w, "Fast-forward merge completed")
	default:
		fmt.Fprintln(w, "Merged successfully")
	}
	fmt.Fprintf(w, "Disk ID:       %s\n", res.DiskID)
	fmt.Fprintf(w, "Source Branch: %s\n", res.SourceBranch)
	fmt.Fprintf(w, "Target Branch: %s\n", res.TargetBranch)
	if res.LCACommitID > 0 {
		fmt.Fprintf(w, "LCA Commit:    %d\n", res.LCACommitID)
	}
	if res.CommitID > 0 {
		fmt.Fprintf(w, "Commit ID:     %d\n", res.CommitID)
	}
	if res.WriterEpoch > 0 {
		fmt.Fprintf(w, "Writer Epoch:  %d\n", res.WriterEpoch)
	}
	if branch != nil {
		fmt.Fprintln(w)
		printBranchDetail(w, branch)
	}
}

func renderMergeConflictHuman(w io.Writer, diskID, sourceBranch, targetBranch string, report *mergeConflictReportJSON, dryRun bool) {
	if dryRun {
		fmt.Fprintf(w, "[Dry Run] Merge check aborted with ECONFLICT: %d unresolved conflicts detected\n", report.TotalConflicts)
	} else {
		fmt.Fprintf(w, "Error: merge aborted with ECONFLICT: %d unresolved conflicts detected\n", report.TotalConflicts)
	}
	if report.LCACommitID > 0 {
		fmt.Fprintf(w, "LCA Commit: %d\n", report.LCACommitID)
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
		fmt.Fprintf(w, "\nNote: conflict list truncated to the first %d entries. Run this command for the full list:\n  bowl diff -d %s %s..%s --conflicts-only\n",
			len(report.Conflicts), diskID, targetBranch, sourceBranch)
	}

	fmt.Fprintf(w, "\nResolution steps:\n"+
		"  1. For file-level resolution, re-run with --resolve <path>=ours|theirs or --strategy ours|theirs.\n"+
		"  2. For manual text conflict resolution, inspect changes with `bowl diff -d %s %s..%s -p`, edit in a local mount, commit, and re-run merge.\n",
		diskID, targetBranch, sourceBranch,
	)
}

func formatConflictNodeCell(n *diffNodeSummaryJSON) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprintf("ino=%d(%s)", n.Ino, FormatSize(n.Size))
}
