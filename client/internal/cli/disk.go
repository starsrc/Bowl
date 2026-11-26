package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/starsrc/pkg/serverx"
	"go.opentelemetry.io/otel/attribute"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	MaxDiskNameBytes      = 255
	MaxBranchNameBytes    = 255
	MaxCommitMessageBytes = 4096
)

type diskSubcommandSpec struct {
	use              string
	aliases          []string
	short            string
	args             cobra.PositionalArgs
	uuidLabel        string
	requireDiskScope bool
	allowQuiet       bool
	quietHelp        string
	spanName         string
	timeout          time.Duration
	buildAdapter     func(cmd *cobra.Command) diskRequestAdapter
}

type diskRequestAdapter interface {
	finalize(cmd *cobra.Command, diskID string, posArgs []string) (showUsage bool, err error)
	spanAttrs() []attribute.KeyValue
	preRPC(rt *cliRuntime, stderr io.Writer) *diskCLIError
	invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError)
}

type diskOutcome struct {
	disk          *storagev1.Disk
	quietID       string
	quietLines    []string
	hasQuietLines bool
	jsonPayload   any
	renderHuman   func(w io.Writer)
	setSpanDiskID bool
	spanAttrs     []attribute.KeyValue
}

type diskCLIError struct {
	exitCode     int
	msg          string
	cause        error
	jsonPayload  any
	renderStderr func(w io.Writer)
}

type diskRPCErrorPolicy struct {
	diskID           string
	branch           string
	defaultOp        string
	preconditionMsg  func(diskID string, err error) string
	checkInvalidArg  bool
	checkEBUSYReason bool
}

type diskJSON struct {
	DiskID        string `json:"disk_id"`
	Name          string `json:"name"`
	DefaultBranch string `json:"default_branch"`
	MaxBytes      uint64 `json:"max_bytes"`
	UsedBytes     uint64 `json:"used_bytes"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

type diskListJSON struct {
	Disks         []diskJSON `json:"disks"`
	NextPageToken string     `json:"next_page_token"`
}

type diskDeleteJSON struct {
	DiskID  string `json:"disk_id"`
	Deleted bool   `json:"deleted"`
}

func toDiskJSON(d *storagev1.Disk) diskJSON {
	var createdAt, updatedAt string
	if d.GetCreatedAt() != nil {
		createdAt = d.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	if d.GetUpdatedAt() != nil {
		updatedAt = d.GetUpdatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	return diskJSON{
		DiskID:        d.GetDiskId(),
		Name:          d.GetName(),
		DefaultBranch: d.GetDefaultBranch(),
		MaxBytes:      d.GetMaxBytes(),
		UsedBytes:     d.GetUsedBytes(),
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}
}

func diskUsagePct(usedBytes, maxBytes uint64) float64 {
	if maxBytes == 0 {
		return 0
	}
	return float64(usedBytes) * 100.0 / float64(maxBytes)
}

func printDiskDetail(w io.Writer, d *storagev1.Disk) {
	dj := toDiskJSON(d)
	fmt.Fprintf(w, "Disk ID:        %s\n", dj.DiskID)
	fmt.Fprintf(w, "Name:           %s\n", dj.Name)
	fmt.Fprintf(w, "Default Branch: %s\n", dj.DefaultBranch)
	fmt.Fprintf(w, "Max Bytes:      %d (%s)\n", dj.MaxBytes, FormatSize(dj.MaxBytes))
	fmt.Fprintf(w, "Used Bytes:     %d (%s)\n", dj.UsedBytes, FormatSize(dj.UsedBytes))
	fmt.Fprintf(w, "Usage:          %.2f%%\n", diskUsagePct(dj.UsedBytes, dj.MaxBytes))
	fmt.Fprintf(w, "Created At:     %s\n", dj.CreatedAt)
	fmt.Fprintf(w, "Updated At:     %s\n", dj.UpdatedAt)
}

func writeDiskJSON(stdout, stderr io.Writer, payload any) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(payload); err != nil {
		fmt.Fprintf(stderr, "Error: marshal JSON output: %v\n", err)
		return 1
	}
	return 0
}

func parseNonNilUUID(raw, label string) (string, error) {
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil {
		return "", fmt.Errorf("invalid %s %q: must be a valid non-nil UUID", label, raw)
	}
	return raw, nil
}

func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("disk name cannot exceed %d bytes, got %d bytes", MaxDiskNameBytes, len(name))
	}
	if strings.Contains(name, "\x00") {
		return errors.New("disk name cannot contain NUL bytes")
	}
	return nil
}

func validateBranchName(name, label string) error {
	if name == "" {
		return fmt.Errorf("%s cannot be empty", label)
	}
	return validateOptionalBranchName(name, label)
}

func validateOptionalBranchName(name, label string) error {
	if len(name) > MaxBranchNameBytes {
		return fmt.Errorf("%s cannot exceed %d bytes, got %d bytes", label, MaxBranchNameBytes, len(name))
	}
	if strings.Contains(name, "\x00") {
		return fmt.Errorf("%s cannot contain NUL bytes", label)
	}
	return nil
}

func validateCommitMessage(msg string) error {
	if len(msg) > MaxCommitMessageBytes {
		return fmt.Errorf("message cannot exceed %d bytes, got %d bytes", MaxCommitMessageBytes, len(msg))
	}
	if strings.Contains(msg, "\x00") {
		return errors.New("message cannot contain NUL bytes")
	}
	return nil
}

func parsePositiveCommitID(raw, label string) (uint64, error) {
	trimmed := strings.TrimSpace(raw)
	id, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil || id == 0 || id > math.MaxInt64 {
		return 0, fmt.Errorf("invalid %s %q: must be a positive integer commit ID", label, raw)
	}
	return id, nil
}

func confirmInteractive(stdin io.Reader, stderr io.Writer, prompt string) *diskCLIError {
	if stdin == nil {
		stdin = os.Stdin
	}
	fmt.Fprint(stderr, prompt)
	line, readErr := bufio.NewReader(stdin).ReadString('\n')
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("read confirmation input: %v", readErr),
		}
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	if answer != "y" && answer != "yes" {
		fmt.Fprintln(stderr, "Operation canceled")
		return &diskCLIError{exitCode: 1}
	}
	return nil
}

func requireNonNilDisk(d *storagev1.Disk) (*storagev1.Disk, *diskCLIError) {
	if d == nil {
		return nil, &diskCLIError{
			exitCode: 1,
			msg:      "server returned empty virtual disk metadata",
		}
	}
	return d, nil
}

func mapDiskRPCError(err error, policy diskRPCErrorPolicy) *diskCLIError {
	if connectErr, ok := errors.AsType[*connect.Error](err); ok {
		switch connectErr.Code() {
		case connect.CodeNotFound:
			if policy.branch != "" && policy.diskID != "" {
				return &diskCLIError{
					exitCode: 1,
					msg:      fmt.Sprintf("disk %s branch %q not found: %v", policy.diskID, policy.branch, err),
					cause:    err,
				}
			}
			if policy.diskID != "" {
				return &diskCLIError{
					exitCode: 1,
					msg:      "disk not found: " + policy.diskID,
					cause:    err,
				}
			}
		case connect.CodeFailedPrecondition:
			if policy.preconditionMsg != nil {
				return &diskCLIError{
					exitCode: 1,
					msg:      policy.preconditionMsg(policy.diskID, err),
					cause:    err,
				}
			}
		case connect.CodeInvalidArgument:
			if policy.checkInvalidArg {
				return &diskCLIError{
					exitCode: 2,
					msg:      fmt.Sprintf("invalid request argument: %v", err),
					cause:    err,
				}
			}
		}
	}
	if policy.checkEBUSYReason && serverx.ExtractErrorInfo(err).GetReason() == "EBUSY" {
		if policy.branch != "" {
			return &diskCLIError{
				exitCode: 1,
				msg:      fmt.Sprintf("branch %q on disk %s has an active mount lease; unmount it, or run `bowl lease list -d %s --branch %s` / `bowl lease evict -d %s --branch %s`", policy.branch, policy.diskID, policy.diskID, policy.branch, policy.diskID, policy.branch),
				cause:    err,
			}
		}
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("disk %s is still mounted with an active client session; unmount it, or run `bowl lease list -d %s` / `bowl lease evict -d %s` before deleting", policy.diskID, policy.diskID, policy.diskID),
			cause:    err,
		}
	}
	return &diskCLIError{
		exitCode: 1,
		msg:      fmt.Sprintf("%s: %v", policy.defaultOp, err),
		cause:    err,
	}
}

func (rt *cliRuntime) newDiskStorageClient(endpoint string) (storagev1connect.StorageServiceClient, int, error) {
	resolvedEndpoint, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		return nil, 2, fmt.Errorf("invalid endpoint: %w", err)
	}

	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint:   resolvedEndpoint,
		HTTPClient: rt.diskHTTPClient,
	})
	if err != nil {
		return nil, 1, fmt.Errorf("create RPC client: %w", err)
	}
	return client, 0, nil
}

func newGroupCmd(rt *cliRuntime, use, short string, specs []*diskSubcommandSpec, rootOpts *rootOptions) *cobra.Command {
	groupCmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args: func(cmd *cobra.Command, posArgs []string) error {
			if len(posArgs) > 0 {
				return fmt.Errorf("unknown command %q for %q", posArgs[0], cmd.CommandPath())
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprint(cmd.ErrOrStderr(), cmd.UsageString())
			return &cliExitError{code: 2, silent: true}
		},
	}

	for _, spec := range specs {
		groupCmd.AddCommand(newDiskSubcommand(rt, spec, rootOpts))
	}
	return groupCmd
}

func newDiskCmd(rt *cliRuntime, rootOpts *rootOptions) *cobra.Command {
	specs := []*diskSubcommandSpec{
		&diskCreateSpec,
		&diskListSpec,
		&diskGetSpec,
		&diskUpdateSpec,
		&diskDeleteSpec,
	}
	return newGroupCmd(rt, "disk", "Manage virtual disks", specs, rootOpts)
}

func newDiskSubcommand(rt *cliRuntime, spec *diskSubcommandSpec, rootOpts *rootOptions) *cobra.Command {
	var (
		quiet   bool
		adapter diskRequestAdapter
	)

	cmd := &cobra.Command{
		Use:     spec.use,
		Aliases: spec.aliases,
		Short:   spec.short,
		Args:    spec.args,
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			if quiet && rootOpts.jsonOut {
				return usageExitErr(errors.New("-q/--quiet and --json cannot be used together"), false)
			}
			var diskID string
			if spec.requireDiskScope {
				resolved, err := resolveDiskScope(rootOpts.diskID)
				if err != nil {
					return usageExitErr(err, false)
				}
				diskID = resolved
			}
			if spec.uuidLabel != "" {
				if _, err := parseNonNilUUID(posArgs[0], spec.uuidLabel); err != nil {
					return usageExitErr(err, false)
				}
				diskID = posArgs[0]
			}
			if showUsage, err := adapter.finalize(cmd, diskID, posArgs); err != nil {
				return usageExitErr(err, showUsage)
			}
			return runDiskValidated(rt, spec, adapter, quiet, rootOpts.jsonOut, rootOpts.endpoint, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	adapter = spec.buildAdapter(cmd)
	flags := cmd.Flags()
	if spec.allowQuiet {
		quietUsage := spec.quietHelp
		if quietUsage == "" {
			quietUsage = "Quiet mode, output only the resource identifier"
		}
		flags.BoolVarP(&quiet, "quiet", "q", false, quietUsage)
	}

	return cmd
}

func runDiskValidated(
	rt *cliRuntime,
	spec *diskSubcommandSpec,
	adapter diskRequestAdapter,
	quiet, jsonOut bool,
	endpoint string,
	stdout, stderr io.Writer,
) error {
	if cliErr := adapter.preRPC(rt, stderr); cliErr != nil {
		if cliErr.msg != "" {
			return &cliExitError{code: cliErr.exitCode, err: errors.New(cliErr.msg)}
		}
		return &cliExitError{code: cliErr.exitCode, silent: true}
	}

	spanCtx, span, finishSpan := startDiskCLISpan(
		context.Background(),
		spec.spanName,
		adapter.spanAttrs()...,
	)
	defer finishSpan()

	client, exitCode, clientErr := rt.newDiskStorageClient(endpoint)
	if clientErr != nil {
		finishDiskCLISpanError(span, clientErr)
		return &cliExitError{code: exitCode, err: clientErr}
	}
	defer rpc.CloseIdleConnections(client)

	rpcCtx, rpcCancel := context.WithTimeout(spanCtx, spec.timeout)
	defer rpcCancel()

	outcome, cliErr := adapter.invoke(rpcCtx, client)
	if cliErr != nil {
		if cliErr.cause != nil {
			finishDiskCLISpanError(span, cliErr.cause)
		}
		if jsonOut && cliErr.jsonPayload != nil {
			if code := writeDiskJSON(stdout, stderr, cliErr.jsonPayload); code != 0 {
				return &cliExitError{code: code, silent: true}
			}
			return &cliExitError{code: cliErr.exitCode, silent: true}
		}
		if cliErr.renderStderr != nil {
			cliErr.renderStderr(stderr)
			return &cliExitError{code: cliErr.exitCode, silent: true}
		}
		if cliErr.msg != "" {
			return &cliExitError{code: cliErr.exitCode, err: errors.New(cliErr.msg)}
		}
		return &cliExitError{code: cliErr.exitCode, silent: true}
	}

	if outcome.setSpanDiskID && outcome.disk != nil {
		span.SetAttributes(attribute.String("bowl.disk_id", outcome.disk.GetDiskId()))
	}
	if len(outcome.spanAttrs) > 0 {
		span.SetAttributes(outcome.spanAttrs...)
	}

	if quiet {
		switch {
		case outcome.hasQuietLines:
			for _, line := range outcome.quietLines {
				fmt.Fprintln(stdout, line)
			}
			return nil
		case outcome.quietID != "":
			fmt.Fprintln(stdout, outcome.quietID)
			return nil
		case outcome.disk != nil:
			fmt.Fprintln(stdout, outcome.disk.GetDiskId())
			return nil
		}
		return nil
	}

	if jsonOut {
		payload := outcome.jsonPayload
		if payload == nil && outcome.disk != nil {
			payload = toDiskJSON(outcome.disk)
		}
		if code := writeDiskJSON(stdout, stderr, payload); code != 0 {
			return &cliExitError{code: code, silent: true}
		}
		return nil
	}

	if outcome.renderHuman != nil {
		outcome.renderHuman(stdout)
		return nil
	}
	if outcome.disk != nil {
		printDiskDetail(stdout, outcome.disk)
	}
	return nil
}

var diskCreateSpec = diskSubcommandSpec{
	use:        "create",
	short:      "Create a new virtual disk",
	args:       cobra.NoArgs,
	allowQuiet: true,
	quietHelp:  "Quiet mode, output only the created disk ID",
	spanName:   "cli.disk.create",
	timeout:    10 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskCreateAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.sizeRaw, "size", "", "Required capacity quota limit such as 10GiB")
		flags.StringVar(&a.name, "name", "", "Optional virtual disk name up to 255 bytes")
		return a
	},
}

type diskCreateAdapter struct {
	sizeRaw  string
	name     string
	maxBytes uint64
}

func (a *diskCreateAdapter) finalize(cmd *cobra.Command, _ string, _ []string) (bool, error) {
	if !cmd.Flags().Changed("size") || strings.TrimSpace(a.sizeRaw) == "" {
		return true, errors.New("--size is required")
	}
	maxBytes, err := ParseSize(a.sizeRaw)
	if err != nil {
		return false, fmt.Errorf("invalid --size: %w", err)
	}
	if err := validateDiskName(a.name); err != nil {
		return false, err
	}
	a.maxBytes = maxBytes
	return false, nil
}

func (a *diskCreateAdapter) spanAttrs() []attribute.KeyValue {
	return nil
}

func (a *diskCreateAdapter) preRPC(_ *cliRuntime, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskCreateAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     a.name,
		MaxBytes: a.maxBytes,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			defaultOp: "create virtual disk failed",
		})
	}
	disk, cliErr := requireNonNilDisk(resp.Msg.GetDisk())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{disk: disk, setSpanDiskID: true}, nil
}

var diskListSpec = diskSubcommandSpec{
	use:        "list",
	aliases:    []string{"ls"},
	short:      "List virtual disks with pagination",
	args:       cobra.NoArgs,
	allowQuiet: true,
	quietHelp:  "Quiet mode, output only disk IDs one per line",
	spanName:   "cli.disk.list",
	timeout:    10 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskListAdapter{}
		flags := cmd.Flags()
		flags.IntVar(&a.limit, "limit", 50, "Maximum number of disks per page in [1, 1000]")
		flags.StringVar(&a.pageToken, "page-token", "", "Pagination cursor token")
		return a
	},
}

type diskListAdapter struct {
	limit     int
	pageToken string
	pageSize  uint32
}

func (a *diskListAdapter) finalize(cmd *cobra.Command, _ string, _ []string) (bool, error) {
	if cmd.Flags().Changed("limit") {
		if a.limit < 1 || a.limit > 1000 {
			return false, fmt.Errorf("invalid --limit %d: must be an integer in [1, 1000]", a.limit)
		}
		a.pageSize = uint32(a.limit)
	}
	return false, nil
}

func (a *diskListAdapter) spanAttrs() []attribute.KeyValue {
	return nil
}

func (a *diskListAdapter) preRPC(_ *cliRuntime, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskListAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize:  a.pageSize,
		PageToken: a.pageToken,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			defaultOp: "list virtual disks failed",
		})
	}

	disks := resp.Msg.GetDisks()
	nextPageToken := resp.Msg.GetNextPageToken()

	items := make([]diskJSON, 0, len(disks))
	ids := make([]string, 0, len(disks))
	for _, d := range disks {
		dj := toDiskJSON(d)
		items = append(items, dj)
		ids = append(ids, dj.DiskID)
	}

	return diskOutcome{
		quietLines:    ids,
		hasQuietLines: true,
		jsonPayload: diskListJSON{
			Disks:         items,
			NextPageToken: nextPageToken,
		},
		renderHuman: func(w io.Writer) {
			renderDiskListTable(w, items, nextPageToken)
		},
	}, nil
}

func renderDiskListTable(w io.Writer, items []diskJSON, nextPageToken string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "DISK ID\tNAME\tDEFAULT BRANCH\tUSED\tMAX\tUSAGE%\tCREATED AT")
	for _, dj := range items {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%.2f%%\t%s\n",
			dj.DiskID,
			dj.Name,
			dj.DefaultBranch,
			FormatSize(dj.UsedBytes),
			FormatSize(dj.MaxBytes),
			diskUsagePct(dj.UsedBytes, dj.MaxBytes),
			dj.CreatedAt,
		)
	}
	_ = tw.Flush()

	if nextPageToken != "" {
		fmt.Fprintf(w, "\nNext Page Token: %s\n", nextPageToken)
	}
}

var diskGetSpec = diskSubcommandSpec{
	use:       "get <disk-id>",
	aliases:   []string{"view"},
	short:     "Show details of a virtual disk",
	args:      cobra.ExactArgs(1),
	uuidLabel: "disk-id",
	spanName:  "cli.disk.get",
	timeout:   10 * time.Second,
	buildAdapter: func(_ *cobra.Command) diskRequestAdapter {
		return &diskGetAdapter{}
	},
}

type diskGetAdapter struct {
	diskID string
}

func (a *diskGetAdapter) finalize(_ *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	return false, nil
}

func (a *diskGetAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *diskGetAdapter) preRPC(_ *cliRuntime, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskGetAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: a.diskID,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:    a.diskID,
			defaultOp: "get disk metadata failed",
		})
	}
	disk, cliErr := requireNonNilDisk(resp.Msg.GetDisk())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{disk: disk}, nil
}

var diskUpdateSpec = diskSubcommandSpec{
	use:       "update <disk-id>",
	aliases:   []string{"edit"},
	short:     "Update a virtual disk name or capacity quota",
	args:      cobra.ExactArgs(1),
	uuidLabel: "disk-id",
	spanName:  "cli.disk.update",
	timeout:   10 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskUpdateAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.sizeRaw, "size", "", "New capacity quota limit such as 20GiB")
		flags.StringVar(&a.nameRaw, "name", "", "New virtual disk name up to 255 bytes; pass \"\" to clear")
		return a
	},
}

type diskUpdateAdapter struct {
	diskID  string
	sizeRaw string
	nameRaw string
	namePtr *string
	sizePtr *uint64
}

func (a *diskUpdateAdapter) finalize(cmd *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	flags := cmd.Flags()
	if flags.Changed("size") {
		maxBytes, err := ParseSize(a.sizeRaw)
		if err != nil {
			return false, fmt.Errorf("invalid --size: %w", err)
		}
		a.sizePtr = &maxBytes
	}
	if flags.Changed("name") {
		if err := validateDiskName(a.nameRaw); err != nil {
			return false, err
		}
		nameCopy := a.nameRaw
		a.namePtr = &nameCopy
	}
	if a.sizePtr == nil && a.namePtr == nil {
		return true, errors.New("at least one of --size or --name must be specified")
	}
	return false, nil
}

func (a *diskUpdateAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *diskUpdateAdapter) preRPC(_ *cliRuntime, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskUpdateAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	updateResp, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   a.diskID,
		Name:     a.namePtr,
		MaxBytes: a.sizePtr,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:    a.diskID,
			defaultOp: "update disk metadata failed",
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("shrink failed because target size is smaller than current used bytes: %v", rpcErr)
			},
		})
	}
	updatedDisk, cliErr := requireNonNilDisk(updateResp.Msg.GetDisk())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{disk: updatedDisk}, nil
}

var diskDeleteSpec = diskSubcommandSpec{
	use:       "delete <disk-id>",
	aliases:   []string{"rm"},
	short:     "Delete a virtual disk and all its branches and commits",
	args:      cobra.ExactArgs(1),
	uuidLabel: "disk-id",
	spanName:  "cli.disk.delete",
	timeout:   15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskDeleteAdapter{}
		cmd.Flags().BoolVarP(&a.yes, "yes", "y", false, "Skip interactive confirmation prompt")
		return a
	},
}

type diskDeleteAdapter struct {
	diskID string
	yes    bool
}

func (a *diskDeleteAdapter) finalize(_ *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	return false, nil
}

func (a *diskDeleteAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
	}
}

func (a *diskDeleteAdapter) preRPC(rt *cliRuntime, stderr io.Writer) *diskCLIError {
	info, err := rt.findDiskMountpoint(a.diskID)
	if err != nil {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("check local mount table: %v", err),
		}
	}
	if info.Mounted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("disk %s is currently mounted at %s; run bowl umount %s before deleting", a.diskID, info.Mountpoint, info.Mountpoint),
		}
	}

	if !a.yes {
		prompt := fmt.Sprintf("Delete virtual disk %s and all its branches and commits? This action is irreversible [y/N]: ", a.diskID)
		return confirmInteractive(rt.diskStdin, stderr, prompt)
	}

	return nil
}

func (a *diskDeleteAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: a.diskID,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:           a.diskID,
			defaultOp:        "delete virtual disk failed",
			checkInvalidArg:  true,
			checkEBUSYReason: true,
			preconditionMsg: func(id string, _ error) string {
				return fmt.Sprintf("disk %s is still mounted with an active client session; unmount it, or run `bowl lease list -d %s` / `bowl lease evict -d %s` before deleting", id, id, id)
			},
		})
	}

	return diskOutcome{
		jsonPayload: diskDeleteJSON{
			DiskID:  a.diskID,
			Deleted: true,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "Deleted virtual disk %s\n", a.diskID)
		},
	}, nil
}
