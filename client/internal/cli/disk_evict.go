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

type diskEvictJSON struct {
	DiskID       string `json:"disk_id"`
	MountID      string `json:"mount_id,omitempty"`
	EvictedCount uint32 `json:"evicted_count"`
	WriterEpoch  uint64 `json:"writer_epoch"`
}

var diskEvictSpec = diskSubcommandSpec{
	use:       "evict <disk-id>",
	short:     "Evict active mount leases for a virtual disk",
	args:      cobra.ExactArgs(1),
	uuidLabel: "disk-id",
	spanName:  "cli.disk.evict",
	timeout:   15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskEvictAdapter{}
		cmd.Flags().StringVar(&a.mountID, "mount-id", "", "Evict only the specified mount lease UUID")
		return a
	},
}

type diskEvictAdapter struct {
	diskID     string
	mountID    string
	hasMountID bool
}

func (a *diskEvictAdapter) finalize(cmd *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	if cmd.Flags().Changed("mount-id") {
		if _, err := parseNonNilUUID(a.mountID, "mount-id"); err != nil {
			return false, err
		}
		a.hasMountID = true
	}
	return false, nil
}

func (a *diskEvictAdapter) spanAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
	if a.hasMountID {
		attrs = append(attrs, attribute.String("bowl.mount_id", a.mountID))
	}
	return attrs
}

func (a *diskEvictAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskEvictAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	req := &storagev1.EvictMountsRequest{
		DiskId: a.diskID,
	}
	if a.hasMountID {
		mountID := a.mountID
		req.MountId = &mountID
	}

	resp, err := client.EvictMounts(ctx, connect.NewRequest(req))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "evict disk mounts failed",
			checkInvalidArg: true,
		})
	}

	evictedCount := resp.Msg.GetEvictedCount()
	writerEpoch := resp.Msg.GetWriterEpoch()

	return diskOutcome{
		jsonPayload: diskEvictJSON{
			DiskID:       a.diskID,
			MountID:      a.mountID,
			EvictedCount: evictedCount,
			WriterEpoch:  writerEpoch,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "Evicted %d mount lease(s) for disk %s (writer epoch: %d)\n", evictedCount, a.diskID, writerEpoch)
		},
	}, nil
}
