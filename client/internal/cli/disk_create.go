package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

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

func (a *diskCreateAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
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
