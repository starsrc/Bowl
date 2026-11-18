package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

var diskUpdateSpec = diskSubcommandSpec{
	use:       "update <disk-id>",
	aliases:   []string{"resize"},
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

func (a *diskUpdateAdapter) finalize(cmd *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
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

func (a *diskUpdateAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskUpdateAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	// Precheck used bytes before sending UpdateDisk when --size is requested.
	if a.sizePtr != nil {
		newMaxBytes := *a.sizePtr
		getResp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
			DiskId: a.diskID,
		}))
		if err != nil {
			return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
				diskID:    a.diskID,
				defaultOp: "get disk metadata failed",
			})
		}
		currentDisk, cliErr := requireNonNilDisk(getResp.Msg.GetDisk())
		if cliErr != nil {
			return diskOutcome{}, cliErr
		}
		if newMaxBytes < currentDisk.GetUsedBytes() {
			return diskOutcome{}, &diskCLIError{
				exitCode: 1,
				msg: fmt.Sprintf(
					"target size %d (%s) is smaller than current used bytes %d (%s); shrinking below used space is not allowed",
					newMaxBytes, FormatSize(newMaxBytes),
					currentDisk.GetUsedBytes(), FormatSize(currentDisk.GetUsedBytes()),
				),
			}
		}
	}

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
