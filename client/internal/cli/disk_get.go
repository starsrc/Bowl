package cli

import (
	"context"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

var diskGetSpec = diskSubcommandSpec{
	use:       "get <disk-id>",
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

func (a *diskGetAdapter) finalize(_ *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	return false, nil
}

func (a *diskGetAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *diskGetAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
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
