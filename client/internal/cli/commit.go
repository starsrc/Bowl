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
	"go.opentelemetry.io/otel/attribute"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	maxCommitPageSize       = 1000
	maxCommitPageTokenBytes = 4096
)

type commitJSON struct {
	DiskID        string `json:"disk_id"`
	CommitID      uint64 `json:"commit_id"`
	ParentID      uint64 `json:"parent_id,omitempty"`
	MergeParentID uint64 `json:"merge_parent_id,omitempty"`
	Kind          string `json:"kind"`
	Message       string `json:"message,omitempty"`
	CreatedAt     string `json:"created_at"`
}

type commitListJSON struct {
	DiskID        string       `json:"disk_id"`
	Commits       []commitJSON `json:"commits"`
	NextPageToken string       `json:"next_page_token"`
}

type commitDeleteJSON struct {
	DiskID   string `json:"disk_id"`
	CommitID uint64 `json:"commit_id"`
	Deleted  bool   `json:"deleted"`
}

type commitPruneJSON struct {
	DiskID         string `json:"disk_id"`
	ReclaimedCount uint32 `json:"reclaimed_count"`
}

func newCommitCmd(rt *cliRuntime, rootOpts *rootOptions) *cobra.Command {
	specs := []*diskSubcommandSpec{
		&commitCreateSpec,
		&commitListSpec,
		&commitDeleteSpec,
		&commitPruneSpec,
	}
	return newGroupCmd(rt, "commit", "Manage commits on a virtual disk", specs, rootOpts)
}

func commitKindLabel(kind storagev1.CommitKind) string {
	switch kind {
	case storagev1.CommitKind_COMMIT_KIND_INIT:
		return "init"
	case storagev1.CommitKind_COMMIT_KIND_MANUAL:
		return "manual"
	case storagev1.CommitKind_COMMIT_KIND_FORK:
		return "fork"
	case storagev1.CommitKind_COMMIT_KIND_MERGE:
		return "merge"
	default:
		return "unknown"
	}
}

func parseCommitKind(raw string) (storagev1.CommitKind, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "init":
		return storagev1.CommitKind_COMMIT_KIND_INIT, nil
	case "manual":
		return storagev1.CommitKind_COMMIT_KIND_MANUAL, nil
	case "fork":
		return storagev1.CommitKind_COMMIT_KIND_FORK, nil
	case "merge":
		return storagev1.CommitKind_COMMIT_KIND_MERGE, nil
	default:
		return storagev1.CommitKind_COMMIT_KIND_UNSPECIFIED, fmt.Errorf("invalid --kind %q: must be init, manual, fork, or merge", raw)
	}
}

func toCommitJSON(c *storagev1.Commit) commitJSON {
	var createdAt string
	if c.GetCreatedAt() != nil {
		createdAt = c.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	return commitJSON{
		DiskID:        c.GetDiskId(),
		CommitID:      c.GetCommitId(),
		ParentID:      c.GetParentId(),
		MergeParentID: c.GetMergeParentId(),
		Kind:          commitKindLabel(c.GetKind()),
		Message:       c.GetMessage(),
		CreatedAt:     createdAt,
	}
}

func requireNonNilCommit(c *storagev1.Commit) (*storagev1.Commit, *diskCLIError) {
	if c == nil {
		return nil, &diskCLIError{
			exitCode: 1,
			msg:      "server returned empty commit metadata",
		}
	}
	return c, nil
}

func printCommitDetail(w io.Writer, c *storagev1.Commit) {
	cj := toCommitJSON(c)
	fmt.Fprintf(w, "Commit ID:    %d\n", cj.CommitID)
	fmt.Fprintf(w, "Disk ID:      %s\n", cj.DiskID)
	fmt.Fprintf(w, "Kind:         %s\n", cj.Kind)
	if cj.ParentID > 0 {
		fmt.Fprintf(w, "Parent:       %d\n", cj.ParentID)
	} else {
		fmt.Fprintln(w, "Parent:       -")
	}
	if cj.MergeParentID > 0 {
		fmt.Fprintf(w, "Merge Parent: %d\n", cj.MergeParentID)
	}
	if cj.Message != "" {
		fmt.Fprintf(w, "Message:      %s\n", cj.Message)
	}
	fmt.Fprintf(w, "Created At:   %s\n", cj.CreatedAt)
}

func renderCommitTable(w io.Writer, items []commitJSON) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "COMMIT ID\tKIND\tPARENT\tMERGE PARENT\tMESSAGE\tCREATED AT")
	for _, cj := range items {
		parent := "-"
		if cj.ParentID > 0 {
			parent = strconv.FormatUint(cj.ParentID, 10)
		}
		mergeParent := "-"
		if cj.MergeParentID > 0 {
			mergeParent = strconv.FormatUint(cj.MergeParentID, 10)
		}
		msg := cj.Message
		if msg == "" {
			msg = "-"
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n",
			cj.CommitID,
			cj.Kind,
			parent,
			mergeParent,
			msg,
			cj.CreatedAt,
		)
	}
	_ = tw.Flush()
}

var commitCreateSpec = diskSubcommandSpec{
	use:              "create",
	short:            "Create a manual commit from a branch working layer",
	args:             cobra.NoArgs,
	requireDiskScope: true,
	allowQuiet:       true,
	quietHelp:        "Quiet mode, output only the created commit ID",
	spanName:         "cli.commit.create",
	timeout:          15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &commitCreateAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.branch, "branch", fusefs.DefaultBranch, "Target branch name to commit")
		flags.StringVarP(&a.message, "message", "m", "", "Optional commit message")
		return a
	},
}

type commitCreateAdapter struct {
	diskID  string
	branch  string
	message string
}

func (a *commitCreateAdapter) finalize(_ *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	if err := validateBranchName(a.branch, "--branch"); err != nil {
		return false, err
	}
	if err := validateCommitMessage(a.message); err != nil {
		return false, err
	}
	return false, nil
}

func (a *commitCreateAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.String("bowl.branch", a.branch),
	}
}

func (a *commitCreateAdapter) preRPC(_ *cliRuntime, _ io.Writer) *diskCLIError {
	return nil
}

func (a *commitCreateAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  a.diskID,
		Branch:  a.branch,
		Message: a.message,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			branch:          a.branch,
			defaultOp:       "create commit failed",
			checkInvalidArg: true,
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("create commit failed: %v", rpcErr)
			},
		})
	}
	commit, cliErr := requireNonNilCommit(resp.Msg.GetCommit())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	commitIDStr := strconv.FormatUint(commit.GetCommitId(), 10)
	return diskOutcome{
		quietID:     commitIDStr,
		jsonPayload: toCommitJSON(commit),
		renderHuman: func(w io.Writer) {
			printCommitDetail(w, commit)
		},
		spanAttrs: []attribute.KeyValue{
			attribute.String("bowl.commit_id", commitIDStr),
		},
	}, nil
}

var commitListSpec = diskSubcommandSpec{
	use:              "list",
	aliases:          []string{"ls"},
	short:            "List commits on a virtual disk",
	args:             cobra.NoArgs,
	requireDiskScope: true,
	allowQuiet:       true,
	quietHelp:        "Quiet mode, output only commit IDs one per line",
	spanName:         "cli.commit.list",
	timeout:          10 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &commitListAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.kindRaw, "kind", "", "Filter commits by kind: init, manual, fork, or merge")
		flags.IntVar(&a.limit, "limit", 0, "Maximum number of commits per page in [1, 1000]")
		flags.StringVar(&a.pageToken, "page-token", "", "Pagination cursor token")
		return a
	},
}

type commitListAdapter struct {
	diskID    string
	kindRaw   string
	kind      storagev1.CommitKind
	limit     int
	pageSize  uint32
	pageToken string
}

func (a *commitListAdapter) finalize(cmd *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	flags := cmd.Flags()
	if flags.Changed("kind") {
		kind, err := parseCommitKind(a.kindRaw)
		if err != nil {
			return false, err
		}
		a.kind = kind
	}
	if flags.Changed("limit") {
		if a.limit < 1 || a.limit > maxCommitPageSize {
			return false, fmt.Errorf("invalid --limit %d: must be an integer in [1, %d]", a.limit, maxCommitPageSize)
		}
		a.pageSize = uint32(a.limit)
	}
	if len(a.pageToken) > maxCommitPageTokenBytes {
		return false, fmt.Errorf("--page-token cannot exceed %d bytes", maxCommitPageTokenBytes)
	}
	if strings.Contains(a.pageToken, "\x00") {
		return false, errors.New("--page-token cannot contain NUL bytes")
	}
	return false, nil
}

func (a *commitListAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *commitListAdapter) preRPC(_ *cliRuntime, _ io.Writer) *diskCLIError {
	return nil
}

func (a *commitListAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ListCommits(ctx, connect.NewRequest(&storagev1.ListCommitsRequest{
		DiskId:    a.diskID,
		Kind:      a.kind,
		PageSize:  a.pageSize,
		PageToken: a.pageToken,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "list commits failed",
			checkInvalidArg: true,
		})
	}

	commits := resp.Msg.GetCommits()
	items := make([]commitJSON, 0, len(commits))
	ids := make([]string, 0, len(commits))
	for _, c := range commits {
		cj := toCommitJSON(c)
		items = append(items, cj)
		ids = append(ids, strconv.FormatUint(cj.CommitID, 10))
	}
	nextPageToken := resp.Msg.GetNextPageToken()

	return diskOutcome{
		quietLines:    ids,
		hasQuietLines: true,
		jsonPayload: commitListJSON{
			DiskID:        a.diskID,
			Commits:       items,
			NextPageToken: nextPageToken,
		},
		renderHuman: func(w io.Writer) {
			if len(items) == 0 {
				fmt.Fprintln(w, "No commits found.")
			} else {
				renderCommitTable(w, items)
			}
			if nextPageToken != "" {
				fmt.Fprintf(w, "\nNext Page Token: %s\n", nextPageToken)
			}
		},
	}, nil
}

var commitDeleteSpec = diskSubcommandSpec{
	use:              "delete <commit-id>",
	aliases:          []string{"rm"},
	short:            "Delete an unreachable commit from a virtual disk",
	args:             cobra.ExactArgs(1),
	requireDiskScope: true,
	spanName:         "cli.commit.delete",
	timeout:          15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &commitDeleteAdapter{}
		cmd.Flags().BoolVarP(&a.yes, "yes", "y", false, "Skip interactive confirmation prompt")
		return a
	},
}

type commitDeleteAdapter struct {
	diskID   string
	commitID uint64
	yes      bool
}

func (a *commitDeleteAdapter) finalize(_ *cobra.Command, diskID string, posArgs []string) (bool, error) {
	a.diskID = diskID
	commitID, err := parsePositiveCommitID(posArgs[0], "commit-id")
	if err != nil {
		return false, err
	}
	a.commitID = commitID
	return false, nil
}

func (a *commitDeleteAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.String("bowl.commit_id", strconv.FormatUint(a.commitID, 10)),
	}
}

func (a *commitDeleteAdapter) preRPC(rt *cliRuntime, stderr io.Writer) *diskCLIError {
	if a.yes {
		return nil
	}
	prompt := fmt.Sprintf("Delete commit %d on disk %s? This action is irreversible [y/N]: ", a.commitID, a.diskID)
	return confirmInteractive(rt.diskStdin, stderr, prompt)
}

func (a *commitDeleteAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	_, err := client.DeleteCommit(ctx, connect.NewRequest(&storagev1.DeleteCommitRequest{
		DiskId:   a.diskID,
		CommitId: a.commitID,
	}))
	if err != nil {
		if connectErr, ok := errors.AsType[*connect.Error](err); ok && connectErr.Code() == connect.CodeNotFound {
			return diskOutcome{}, &diskCLIError{
				exitCode: 1,
				msg:      fmt.Sprintf("commit %d not found on disk %s", a.commitID, a.diskID),
				cause:    err,
			}
		}
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "delete commit failed",
			checkInvalidArg: true,
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("commit %d cannot be deleted because it is still reachable by a branch: %v", a.commitID, rpcErr)
			},
		})
	}

	return diskOutcome{
		jsonPayload: commitDeleteJSON{
			DiskID:   a.diskID,
			CommitID: a.commitID,
			Deleted:  true,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "Deleted commit %d on disk %s\n", a.commitID, a.diskID)
		},
	}, nil
}

var commitPruneSpec = diskSubcommandSpec{
	use:              "prune",
	short:            "Reclaim all unreachable commits on a virtual disk",
	args:             cobra.NoArgs,
	requireDiskScope: true,
	spanName:         "cli.commit.prune",
	timeout:          30 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &commitPruneAdapter{}
		cmd.Flags().BoolVarP(&a.yes, "yes", "y", false, "Skip interactive confirmation prompt")
		return a
	},
}

type commitPruneAdapter struct {
	diskID string
	yes    bool
}

func (a *commitPruneAdapter) finalize(_ *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	return false, nil
}

func (a *commitPruneAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *commitPruneAdapter) preRPC(rt *cliRuntime, stderr io.Writer) *diskCLIError {
	if a.yes {
		return nil
	}
	prompt := fmt.Sprintf("Prune all unreachable commits on disk %s? [y/N]: ", a.diskID)
	return confirmInteractive(rt.diskStdin, stderr, prompt)
}

func (a *commitPruneAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.PruneCommits(ctx, connect.NewRequest(&storagev1.PruneCommitsRequest{
		DiskId: a.diskID,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "prune commits failed",
			checkInvalidArg: true,
		})
	}

	reclaimed := resp.Msg.GetReclaimedCount()
	return diskOutcome{
		jsonPayload: commitPruneJSON{
			DiskID:         a.diskID,
			ReclaimedCount: reclaimed,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "Pruned %d unreachable commit(s) for disk %s\n", reclaimed, a.diskID)
		},
	}, nil
}
