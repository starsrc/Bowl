package cli

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type diskMountLeaseJSON struct {
	MountID   string `json:"mount_id"`
	Mode      string `json:"mode"`
	Epoch     uint64 `json:"epoch"`
	ExpiresAt string `json:"expires_at"`
}

type diskMountsJSON struct {
	DiskID string               `json:"disk_id"`
	Mounts []diskMountLeaseJSON `json:"mounts"`
}

var diskMountsSpec = diskSubcommandSpec{
	use:       "mounts <disk-id>",
	short:     "List active mount leases for a virtual disk",
	args:      cobra.ExactArgs(1),
	uuidLabel: "disk-id",
	spanName:  "cli.disk.mounts",
	timeout:   10 * time.Second,
	buildAdapter: func(_ *cobra.Command) diskRequestAdapter {
		return &diskMountsAdapter{}
	},
}

type diskMountsAdapter struct {
	diskID string
}

func (a *diskMountsAdapter) finalize(_ *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	return false, nil
}

func (a *diskMountsAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *diskMountsAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskMountsAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: a.diskID,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "list disk mounts failed",
			checkInvalidArg: true,
		})
	}

	mounts := resp.Msg.GetMounts()
	items := make([]diskMountLeaseJSON, 0, len(mounts))
	for _, m := range mounts {
		mode := "rw"
		if m.GetReadOnly() {
			mode = "ro"
		}
		var expiresAt string
		if m.GetExpiresAt() != nil {
			expiresAt = m.GetExpiresAt().AsTime().UTC().Format(time.RFC3339)
		}
		items = append(items, diskMountLeaseJSON{
			MountID:   m.GetMountId(),
			Mode:      mode,
			Epoch:     m.GetEpoch(),
			ExpiresAt: expiresAt,
		})
	}

	return diskOutcome{
		jsonPayload: diskMountsJSON{
			DiskID: a.diskID,
			Mounts: items,
		},
		renderHuman: func(w io.Writer) {
			renderDiskMountsTable(w, items)
		},
	}, nil
}

func renderDiskMountsTable(w io.Writer, items []diskMountLeaseJSON) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MOUNT ID\tMODE\tEPOCH\tEXPIRES AT")
	for _, m := range items {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", m.MountID, m.Mode, m.Epoch, m.ExpiresAt)
	}
	_ = tw.Flush()
}
