package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

var diskForkSpec = diskSubcommandSpec{
	use:        "fork <source-disk-id>",
	short:      "Create a zero-copy fork from an existing virtual disk",
	args:       cobra.ExactArgs(1),
	uuidLabel:  "source-disk-id",
	allowQuiet: true,
	spanName:   "cli.disk.fork",
	timeout:    30 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskForkAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.name, "name", "", "Optional name for the forked virtual disk up to 255 bytes")
		flags.StringVar(&a.sizeRaw, "size", "", "Optional capacity quota limit; defaults to the source disk quota")
		return a
	},
}

type diskForkAdapter struct {
	sourceDiskID string
	sizeRaw      string
	name         string
	sizePtr      *uint64
}

func (a *diskForkAdapter) finalize(cmd *cobra.Command, posArgs []string) (bool, error) {
	a.sourceDiskID = posArgs[0]
	if cmd.Flags().Changed("size") {
		maxBytes, err := ParseSize(a.sizeRaw)
		if err != nil {
			return false, fmt.Errorf("invalid --size: %w", err)
		}
		a.sizePtr = &maxBytes
	}
	if err := validateDiskName(a.name); err != nil {
		return false, err
	}
	return false, nil
}

func (a *diskForkAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.source_disk_id", a.sourceDiskID)}
}

func (a *diskForkAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskForkAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: a.sourceDiskID,
		Name:         a.name,
		MaxBytes:     a.sizePtr,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:    a.sourceDiskID,
			defaultOp: "fork virtual disk failed",
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("target size is smaller than source disk used bytes: %v", rpcErr)
			},
		})
	}
	disk, cliErr := requireNonNilDisk(resp.Msg.GetDisk())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{disk: disk, setSpanDiskID: true}, nil
}
