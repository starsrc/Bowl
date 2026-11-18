package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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

// MaxDiskNameBytes defines the maximum allowed byte length for a virtual disk name.
const MaxDiskNameBytes = 255

// diskPorts defines the external seams for the disk control-plane module.
type diskPorts struct {
	newClient      func(endpoint, token string) (storagev1connect.StorageServiceClient, int, error)
	findMountpoint func(diskID string) (DiskMountInfo, error)
	stdin          io.Reader
}

var (
	diskHTTPClient *http.Client
	diskStdin      io.Reader = os.Stdin
)

func defaultDiskPorts() diskPorts {
	return diskPorts{
		newClient:      newDiskStorageClient,
		findMountpoint: FindDiskMountpoint,
		stdin:          diskStdin,
	}
}

type diskSubcommandSpec struct {
	use          string
	aliases      []string
	short        string
	args         cobra.PositionalArgs
	uuidLabel    string
	allowQuiet   bool
	spanName     string
	timeout      time.Duration
	buildAdapter func(cmd *cobra.Command) diskRequestAdapter
}

type diskRequestAdapter interface {
	finalize(cmd *cobra.Command, posArgs []string) (showUsage bool, err error)
	spanAttrs() []attribute.KeyValue
	preRPC(ports diskPorts, stderr io.Writer) *diskCLIError
	invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError)
}

type diskOutcome struct {
	disk          *storagev1.Disk
	quietID       string
	jsonPayload   any
	renderHuman   func(w io.Writer)
	setSpanDiskID bool
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
	defaultOp        string
	preconditionMsg  func(diskID string, err error) string
	checkInvalidArg  bool
	checkEBUSYReason bool
}

type diskJSON struct {
	DiskID       string `json:"disk_id"`
	ParentDiskID string `json:"parent_disk_id,omitempty"`
	Name         string `json:"name"`
	RootIno      uint64 `json:"root_ino"`
	MaxBytes     uint64 `json:"max_bytes"`
	UsedBytes    uint64 `json:"used_bytes"`
	UsedInodes   uint64 `json:"used_inodes"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
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
		DiskID:       d.GetDiskId(),
		ParentDiskID: d.GetParentDiskId(),
		Name:         d.GetName(),
		RootIno:      d.GetRootIno(),
		MaxBytes:     d.GetMaxBytes(),
		UsedBytes:    d.GetUsedBytes(),
		UsedInodes:   d.GetUsedInodes(),
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
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
	fmt.Fprintf(w, "Disk ID:      %s\n", dj.DiskID)
	if dj.ParentDiskID != "" {
		fmt.Fprintf(w, "Parent Disk:  %s\n", dj.ParentDiskID)
	}
	fmt.Fprintf(w, "Name:         %s\n", dj.Name)
	fmt.Fprintf(w, "Root Ino:     %d\n", dj.RootIno)
	fmt.Fprintf(w, "Max Bytes:    %d (%s)\n", dj.MaxBytes, FormatSize(dj.MaxBytes))
	fmt.Fprintf(w, "Used Bytes:   %d (%s)\n", dj.UsedBytes, FormatSize(dj.UsedBytes))
	fmt.Fprintf(w, "Usage:        %.2f%%\n", diskUsagePct(dj.UsedBytes, dj.MaxBytes))
	fmt.Fprintf(w, "Used Inodes:  %d\n", dj.UsedInodes)
	fmt.Fprintf(w, "Created At:   %s\n", dj.CreatedAt)
	fmt.Fprintf(w, "Updated At:   %s\n", dj.UpdatedAt)
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
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("disk %s is still mounted with an active client session; unmount it before deleting", policy.diskID),
			cause:    err,
		}
	}
	return &diskCLIError{
		exitCode: 1,
		msg:      fmt.Sprintf("%s: %v", policy.defaultOp, err),
		cause:    err,
	}
}

func newDiskStorageClient(endpoint, token string) (storagev1connect.StorageServiceClient, int, error) {
	resolvedEndpoint, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		return nil, 2, fmt.Errorf("invalid endpoint: %w", err)
	}
	resolvedToken := rpc.ResolveToken(token)

	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint:   resolvedEndpoint,
		Token:      resolvedToken,
		HTTPClient: diskHTTPClient,
	})
	if err != nil {
		return nil, 1, fmt.Errorf("create RPC client: %w", err)
	}
	return client, 0, nil
}

func newDiskCmd(ports diskPorts) *cobra.Command {
	diskCmd := &cobra.Command{
		Use:   "disk",
		Short: "Manage virtual disks",
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

	specs := []*diskSubcommandSpec{
		&diskCreateSpec,
		&diskForkSpec,
		&diskListSpec,
		&diskGetSpec,
		&diskUpdateSpec,
		&diskDeleteSpec,
		&diskDiffSpec,
		&diskMergeSpec,
	}
	for _, spec := range specs {
		diskCmd.AddCommand(newDiskSubcommand(spec, ports))
	}
	return diskCmd
}

func newDiskSubcommand(spec *diskSubcommandSpec, ports diskPorts) *cobra.Command {
	var (
		quiet    bool
		jsonOut  bool
		endpoint string
		token    string
		adapter  diskRequestAdapter
	)

	cmd := &cobra.Command{
		Use:     spec.use,
		Aliases: spec.aliases,
		Short:   spec.short,
		Args:    spec.args,
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			if quiet && jsonOut {
				return usageExitErr(errors.New("-q/--quiet and --json cannot be used together"), false)
			}
			if spec.uuidLabel != "" {
				if _, err := parseNonNilUUID(posArgs[0], spec.uuidLabel); err != nil {
					return usageExitErr(err, false)
				}
			}
			if showUsage, err := adapter.finalize(cmd, posArgs); err != nil {
				return usageExitErr(err, showUsage)
			}
			return runDiskValidated(spec, adapter, ports, quiet, jsonOut, endpoint, token, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	adapter = spec.buildAdapter(cmd)
	flags := cmd.Flags()
	if spec.allowQuiet {
		flags.BoolVarP(&quiet, "quiet", "q", false, "Quiet mode, output only the disk ID")
	}
	flags.BoolVar(&jsonOut, "json", false, "Output result in JSON format")
	flags.StringVar(&endpoint, "endpoint", "", "StorageService base URL, defaults to $BOWL_STORAGE_ENDPOINT or http://127.0.0.1:9000")
	flags.StringVar(&token, "token", "", "Access token, defaults to $BOWL_TOKEN")

	return cmd
}

func runDiskValidated(
	spec *diskSubcommandSpec,
	adapter diskRequestAdapter,
	ports diskPorts,
	quiet, jsonOut bool,
	endpoint, token string,
	stdout, stderr io.Writer,
) error {
	if ports.newClient == nil {
		ports.newClient = newDiskStorageClient
	}
	if ports.findMountpoint == nil {
		ports.findMountpoint = FindDiskMountpoint
	}
	if ports.stdin == nil {
		ports.stdin = os.Stdin
	}

	if cliErr := adapter.preRPC(ports, stderr); cliErr != nil {
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

	client, exitCode, clientErr := ports.newClient(endpoint, token)
	if clientErr != nil {
		finishDiskCLISpanError(span, clientErr)
		return &cliExitError{code: exitCode, err: clientErr}
	}

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

	if quiet {
		switch {
		case outcome.disk != nil:
			fmt.Fprintln(stdout, outcome.disk.GetDiskId())
			return nil
		case outcome.quietID != "":
			fmt.Fprintln(stdout, outcome.quietID)
			return nil
		}
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
