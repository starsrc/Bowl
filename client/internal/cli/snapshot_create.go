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

var snapshotCreateSpec = diskSubcommandSpec{
	use:        "create <disk-id>",
	short:      "Create a manual snapshot of a virtual disk",
	args:       cobra.ExactArgs(1),
	uuidLabel:  "disk-id",
	allowQuiet: true,
	quietHelp:  "Quiet mode, output only the snapshot ID",
	spanName:   "cli.snapshot.create",
	timeout:    30 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &snapshotCreateAdapter{}
		cmd.Flags().StringVar(&a.name, "name", "", "Optional snapshot name up to 255 bytes")
		return a
	},
}

type snapshotCreateAdapter struct {
	diskID string
	name   string
}

func (a *snapshotCreateAdapter) finalize(_ *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	if err := validateDiskName(a.name); err != nil {
		return false, err
	}
	return false, nil
}

func (a *snapshotCreateAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *snapshotCreateAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *snapshotCreateAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.CreateSnapshot(ctx, connect.NewRequest(&storagev1.CreateSnapshotRequest{
		DiskId: a.diskID,
		Name:   a.name,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "create snapshot failed",
			checkInvalidArg: true,
		})
	}
	snap, cliErr := requireNonNilSnapshot(resp.Msg.GetSnapshot())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{
		quietID:     snap.GetSnapshotId(),
		jsonPayload: toSnapshotJSON(snap),
		spanAttrs:   []attribute.KeyValue{attribute.String("bowl.snapshot_id", snap.GetSnapshotId())},
		renderHuman: func(w io.Writer) {
			printSnapshotDetail(w, snap)
		},
	}, nil
}
