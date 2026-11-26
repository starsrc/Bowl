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

type branchJSON struct {
	DiskID       string `json:"disk_id"`
	Name         string `json:"name"`
	HeadCommitID uint64 `json:"head_commit_id"`
	WriterEpoch  uint64 `json:"writer_epoch"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

type branchListJSON struct {
	DiskID   string       `json:"disk_id"`
	Branches []branchJSON `json:"branches"`
}

type branchCreateJSON struct {
	Branch branchJSON `json:"branch"`
	Commit commitJSON `json:"commit"`
}

type branchDeleteJSON struct {
	DiskID  string `json:"disk_id"`
	Branch  string `json:"branch"`
	Deleted bool   `json:"deleted"`
}

func newBranchCmd(rt *cliRuntime, rootOpts *rootOptions) *cobra.Command {
	specs := []*diskSubcommandSpec{
		&branchListSpec,
		&branchCreateSpec,
		&branchDeleteSpec,
		&branchResetSpec,
		&branchMergeSpec,
	}
	return newGroupCmd(rt, "branch", "Manage branches on a virtual disk", specs, rootOpts)
}

func toBranchJSON(b *storagev1.Branch) branchJSON {
	var createdAt, updatedAt string
	if b.GetCreatedAt() != nil {
		createdAt = b.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	if b.GetUpdatedAt() != nil {
		updatedAt = b.GetUpdatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	return branchJSON{
		DiskID:       b.GetDiskId(),
		Name:         b.GetName(),
		HeadCommitID: b.GetHeadCommitId(),
		WriterEpoch:  b.GetWriterEpoch(),
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}
}

func requireNonNilBranch(b *storagev1.Branch) (*storagev1.Branch, *diskCLIError) {
	if b == nil {
		return nil, &diskCLIError{
			exitCode: 1,
			msg:      "server returned empty branch metadata",
		}
	}
	return b, nil
}

func printBranchDetail(w io.Writer, b *storagev1.Branch) {
	bj := toBranchJSON(b)
	fmt.Fprintf(w, "Disk ID:      %s\n", bj.DiskID)
	fmt.Fprintf(w, "Branch:       %s\n", bj.Name)
	fmt.Fprintf(w, "Head Commit:  %d\n", bj.HeadCommitID)
	fmt.Fprintf(w, "Writer Epoch: %d\n", bj.WriterEpoch)
	fmt.Fprintf(w, "Created At:   %s\n", bj.CreatedAt)
	fmt.Fprintf(w, "Updated At:   %s\n", bj.UpdatedAt)
}

func renderBranchTable(w io.Writer, items []branchJSON) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BRANCH\tHEAD COMMIT\tWRITER EPOCH\tCREATED AT\tUPDATED AT")
	for _, bj := range items {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\n",
			bj.Name,
			bj.HeadCommitID,
			bj.WriterEpoch,
			bj.CreatedAt,
			bj.UpdatedAt,
		)
	}
	_ = tw.Flush()
}

var branchListSpec = diskSubcommandSpec{
	use:              "list",
	aliases:          []string{"ls"},
	short:            "List branches on a virtual disk",
	args:             cobra.NoArgs,
	requireDiskScope: true,
	allowQuiet:       true,
	quietHelp:        "Quiet mode, output only branch names one per line",
	spanName:         "cli.branch.list",
	timeout:          10 * time.Second,
	buildAdapter: func(_ *cobra.Command) diskRequestAdapter {
		return &branchListAdapter{}
	},
}

type branchListAdapter struct {
	diskID string
}

func (a *branchListAdapter) finalize(_ *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	return false, nil
}

func (a *branchListAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *branchListAdapter) preRPC(_ *cliRuntime, _ io.Writer) *diskCLIError {
	return nil
}

func (a *branchListAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{
		DiskId: a.diskID,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "list branches failed",
			checkInvalidArg: true,
		})
	}

	branches := resp.Msg.GetBranches()
	items := make([]branchJSON, 0, len(branches))
	names := make([]string, 0, len(branches))
	for _, b := range branches {
		bj := toBranchJSON(b)
		items = append(items, bj)
		names = append(names, bj.Name)
	}

	return diskOutcome{
		quietLines:    names,
		hasQuietLines: true,
		jsonPayload: branchListJSON{
			DiskID:   a.diskID,
			Branches: items,
		},
		renderHuman: func(w io.Writer) {
			renderBranchTable(w, items)
		},
	}, nil
}

var branchCreateSpec = diskSubcommandSpec{
	use:              "create <new-branch>",
	aliases:          []string{"fork"},
	short:            "Create a new branch by forking from an existing branch",
	args:             cobra.ExactArgs(1),
	requireDiskScope: true,
	allowQuiet:       true,
	quietHelp:        "Quiet mode, output only the created branch name",
	spanName:         "cli.branch.create",
	timeout:          15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &branchCreateAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.fromBranch, "from", fusefs.DefaultBranch, "Source branch to fork from")
		flags.StringVarP(&a.message, "message", "m", "", "Optional fork commit message")
		return a
	},
}

type branchCreateAdapter struct {
	diskID     string
	fromBranch string
	newBranch  string
	message    string
}

func (a *branchCreateAdapter) finalize(_ *cobra.Command, diskID string, posArgs []string) (bool, error) {
	a.diskID = diskID
	a.newBranch = posArgs[0]
	if err := validateBranchName(a.newBranch, "new-branch"); err != nil {
		return false, err
	}
	if err := validateBranchName(a.fromBranch, "--from"); err != nil {
		return false, err
	}
	if a.fromBranch == a.newBranch {
		return false, errors.New("new branch must be different from source branch")
	}
	if err := validateCommitMessage(a.message); err != nil {
		return false, err
	}
	return false, nil
}

func (a *branchCreateAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.String("bowl.source_branch", a.fromBranch),
		attribute.String("bowl.new_branch", a.newBranch),
	}
}

func (a *branchCreateAdapter) preRPC(_ *cliRuntime, _ io.Writer) *diskCLIError {
	return nil
}

func (a *branchCreateAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       a.diskID,
		SourceBranch: a.fromBranch,
		NewBranch:    a.newBranch,
		Message:      a.message,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			branch:          a.fromBranch,
			defaultOp:       "create branch failed",
			checkInvalidArg: true,
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("create branch failed: %v", rpcErr)
			},
		})
	}

	branch, cliErr := requireNonNilBranch(resp.Msg.GetBranch())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	commit, cliErr := requireNonNilCommit(resp.Msg.GetCommit())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}

	payload := branchCreateJSON{
		Branch: toBranchJSON(branch),
		Commit: toCommitJSON(commit),
	}

	return diskOutcome{
		quietID:     branch.GetName(),
		jsonPayload: payload,
		renderHuman: func(w io.Writer) {
			printBranchDetail(w, branch)
			fmt.Fprintf(w, "Fork Commit:  %d\n", commit.GetCommitId())
		},
	}, nil
}

var branchDeleteSpec = diskSubcommandSpec{
	use:              "delete <branch>",
	aliases:          []string{"rm"},
	short:            "Delete a non-default branch from a virtual disk",
	args:             cobra.ExactArgs(1),
	requireDiskScope: true,
	spanName:         "cli.branch.delete",
	timeout:          15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &branchDeleteAdapter{}
		cmd.Flags().BoolVarP(&a.yes, "yes", "y", false, "Skip interactive confirmation prompt")
		return a
	},
}

type branchDeleteAdapter struct {
	diskID string
	branch string
	yes    bool
}

func (a *branchDeleteAdapter) finalize(_ *cobra.Command, diskID string, posArgs []string) (bool, error) {
	a.diskID = diskID
	a.branch = posArgs[0]
	if err := validateBranchName(a.branch, "branch"); err != nil {
		return false, err
	}
	return false, nil
}

func (a *branchDeleteAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.String("bowl.branch", a.branch),
	}
}

func (a *branchDeleteAdapter) preRPC(rt *cliRuntime, stderr io.Writer) *diskCLIError {
	info, err := rt.findBranchMountpoint(a.diskID, a.branch)
	if err != nil {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("check local mount table: %v", err),
		}
	}
	if info.Mounted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("branch %q on disk %s is currently mounted at %s; run bowl umount %s before deleting", a.branch, a.diskID, info.Mountpoint, info.Mountpoint),
		}
	}
	if !a.yes {
		prompt := fmt.Sprintf("Delete branch %q on disk %s? This action is irreversible [y/N]: ", a.branch, a.diskID)
		return confirmInteractive(rt.diskStdin, stderr, prompt)
	}
	return nil
}

func (a *branchDeleteAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	_, err := client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: a.diskID,
		Branch: a.branch,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:           a.diskID,
			branch:           a.branch,
			defaultOp:        "delete branch failed",
			checkInvalidArg:  true,
			checkEBUSYReason: true,
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("cannot delete branch %q on disk %s: %v", a.branch, a.diskID, rpcErr)
			},
		})
	}

	return diskOutcome{
		jsonPayload: branchDeleteJSON{
			DiskID:  a.diskID,
			Branch:  a.branch,
			Deleted: true,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "Deleted branch %s on disk %s\n", a.branch, a.diskID)
		},
	}, nil
}

var branchResetSpec = diskSubcommandSpec{
	use:              "reset <branch>",
	short:            "Reset a branch head to a target commit and clear its working layer",
	args:             cobra.ExactArgs(1),
	requireDiskScope: true,
	spanName:         "cli.branch.reset",
	timeout:          15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &branchResetAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.toRaw, "to", "", "Required target commit ID")
		flags.BoolVarP(&a.yes, "yes", "y", false, "Skip interactive confirmation prompt")
		return a
	},
}

type branchResetAdapter struct {
	diskID         string
	branch         string
	toRaw          string
	targetCommitID uint64
	yes            bool
}

func (a *branchResetAdapter) finalize(cmd *cobra.Command, diskID string, posArgs []string) (bool, error) {
	a.diskID = diskID
	a.branch = posArgs[0]
	if err := validateBranchName(a.branch, "branch"); err != nil {
		return false, err
	}
	if !cmd.Flags().Changed("to") || strings.TrimSpace(a.toRaw) == "" {
		return true, errors.New("--to is required")
	}
	commitID, err := parsePositiveCommitID(a.toRaw, "--to")
	if err != nil {
		return false, err
	}
	a.targetCommitID = commitID
	return false, nil
}

func (a *branchResetAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.String("bowl.branch", a.branch),
		attribute.String("bowl.target_commit_id", strconv.FormatUint(a.targetCommitID, 10)),
	}
}

func (a *branchResetAdapter) preRPC(rt *cliRuntime, stderr io.Writer) *diskCLIError {
	info, err := rt.findBranchMountpoint(a.diskID, a.branch)
	if err != nil {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("check local mount table: %v", err),
		}
	}
	if info.Mounted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("branch %q on disk %s is currently mounted at %s; run bowl umount %s before resetting", a.branch, a.diskID, info.Mountpoint, info.Mountpoint),
		}
	}
	if !a.yes {
		prompt := fmt.Sprintf("Reset branch %q on disk %s to commit %d and discard uncommitted work? [y/N]: ", a.branch, a.diskID, a.targetCommitID)
		return confirmInteractive(rt.diskStdin, stderr, prompt)
	}
	return nil
}

func (a *branchResetAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         a.diskID,
		Branch:         a.branch,
		TargetCommitId: a.targetCommitID,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:           a.diskID,
			branch:           a.branch,
			defaultOp:        "reset branch failed",
			checkInvalidArg:  true,
			checkEBUSYReason: true,
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("cannot reset branch %q on disk %s: %v", a.branch, a.diskID, rpcErr)
			},
		})
	}

	branch, cliErr := requireNonNilBranch(resp.Msg.GetBranch())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}

	return diskOutcome{
		jsonPayload: toBranchJSON(branch),
		renderHuman: func(w io.Writer) {
			printBranchDetail(w, branch)
		},
	}, nil
}
