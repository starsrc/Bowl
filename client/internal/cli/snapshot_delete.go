package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"github.com/starsrc/pkg/serverx"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type snapshotDeleteJSON struct {
	DiskID     string `json:"disk_id"`
	SnapshotID string `json:"snapshot_id"`
	Deleted    bool   `json:"deleted"`
}

var snapshotDeleteSpec = diskSubcommandSpec{
	use:       "delete <disk-id> <snapshot-id>",
	aliases:   []string{"rm"},
	short:     "Delete an unprotected snapshot",
	args:      cobra.ExactArgs(2),
	uuidLabel: "disk-id",
	spanName:  "cli.snapshot.delete",
	timeout:   15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &snapshotDeleteAdapter{}
		cmd.Flags().BoolVarP(&a.yes, "yes", "y", false, "Skip interactive confirmation prompt")
		return a
	},
}

type snapshotDeleteAdapter struct {
	diskID     string
	snapshotID string
	yes        bool
}

func (a *snapshotDeleteAdapter) finalize(_ *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	if _, err := parseNonNilUUID(posArgs[1], "snapshot-id"); err != nil {
		return false, err
	}
	a.snapshotID = posArgs[1]
	return false, nil
}

func (a *snapshotDeleteAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.String("bowl.snapshot_id", a.snapshotID),
	}
}

func (a *snapshotDeleteAdapter) preRPC(ports diskPorts, stderr io.Writer) *diskCLIError {
	if a.yes {
		return nil
	}
	prompt := fmt.Sprintf("Delete snapshot %s on disk %s? This action is irreversible [y/N]: ", a.snapshotID, a.diskID)
	return confirmInteractive(ports.stdin, stderr, prompt)
}

func (a *snapshotDeleteAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	_, err := client.DeleteSnapshot(ctx, connect.NewRequest(&storagev1.DeleteSnapshotRequest{
		DiskId:     a.diskID,
		SnapshotId: a.snapshotID,
	}))
	if err != nil {
		return diskOutcome{}, a.mapDeleteRPCError(err)
	}

	return diskOutcome{
		jsonPayload: snapshotDeleteJSON{
			DiskID:     a.diskID,
			SnapshotID: a.snapshotID,
			Deleted:    true,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "Deleted snapshot %s\n", a.snapshotID)
		},
	}, nil
}

func (a *snapshotDeleteAdapter) mapDeleteRPCError(err error) *diskCLIError {
	if connectErr, ok := errors.AsType[*connect.Error](err); ok && connectErr.Code() == connect.CodeNotFound {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("disk %s or snapshot %s not found: %v", a.diskID, a.snapshotID, err),
			cause:    err,
		}
	}
	return mapDiskRPCError(err, diskRPCErrorPolicy{
		defaultOp:       "delete snapshot failed",
		checkInvalidArg: true,
		preconditionMsg: func(_ string, rpcErr error) string {
			switch serverx.ExtractErrorInfo(rpcErr).GetReason() {
			case "EBUSY":
				return fmt.Sprintf("snapshot %s is protected by active disk lineage or LCA and cannot be deleted; run `bowl snapshot list %s` to inspect protected snapshots", a.snapshotID, a.diskID)
			case "EXDEV":
				return fmt.Sprintf("snapshot %s does not belong to disk %s lineage", a.snapshotID, a.diskID)
			default:
				return fmt.Sprintf("delete snapshot precondition failed: %v", rpcErr)
			}
		},
	})
}
