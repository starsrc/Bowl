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

type snapshotPruneJSON struct {
	DiskID         string         `json:"disk_id"`
	DryRun         bool           `json:"dry_run"`
	ReclaimedCount uint32         `json:"reclaimed_count"`
	Snapshots      []snapshotJSON `json:"snapshots"`
}

var snapshotPruneSpec = diskSubcommandSpec{
	use:       "prune <disk-id>",
	short:     "Reclaim unprotected obsolete fork and merge snapshots",
	args:      cobra.ExactArgs(1),
	uuidLabel: "disk-id",
	spanName:  "cli.snapshot.prune",
	timeout:   30 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &snapshotPruneAdapter{}
		flags := cmd.Flags()
		flags.BoolVar(&a.dryRun, "dry-run", false, "Preview reclaimable snapshots without deleting")
		flags.BoolVarP(&a.yes, "yes", "y", false, "Skip interactive confirmation prompt")
		return a
	},
}

type snapshotPruneAdapter struct {
	diskID string
	dryRun bool
	yes    bool
}

func (a *snapshotPruneAdapter) finalize(_ *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	return false, nil
}

func (a *snapshotPruneAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.Bool("bowl.dry_run", a.dryRun),
	}
}

func (a *snapshotPruneAdapter) preRPC(ports diskPorts, stderr io.Writer) *diskCLIError {
	if a.dryRun || a.yes {
		return nil
	}
	prompt := fmt.Sprintf("Prune obsolete fork/merge snapshots for disk %s (manual and protected snapshots are preserved)? [y/N]: ", a.diskID)
	return confirmInteractive(ports.stdin, stderr, prompt)
}

func (a *snapshotPruneAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.PruneSnapshots(ctx, connect.NewRequest(&storagev1.PruneSnapshotsRequest{
		DiskId: a.diskID,
		DryRun: a.dryRun,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "prune snapshots failed",
			checkInvalidArg: true,
		})
	}

	items := toSnapshotJSONList(resp.Msg.GetSnapshots())
	reclaimedCount := resp.Msg.GetReclaimedCount()
	dryRun := a.dryRun || resp.Msg.GetDryRun()

	return diskOutcome{
		jsonPayload: snapshotPruneJSON{
			DiskID:         a.diskID,
			DryRun:         dryRun,
			ReclaimedCount: reclaimedCount,
			Snapshots:      items,
		},
		renderHuman: func(w io.Writer) {
			renderSnapshotPruneHuman(w, a.diskID, dryRun, reclaimedCount, items)
		},
	}, nil
}

func renderSnapshotPruneHuman(w io.Writer, diskID string, dryRun bool, reclaimedCount uint32, items []snapshotJSON) {
	if dryRun {
		if len(items) == 0 && reclaimedCount == 0 {
			fmt.Fprintln(w, "No reclaimable snapshots found.")
			return
		}
		fmt.Fprintf(w, "[Dry Run] %d reclaimable snapshot(s) found for disk %s:\n", reclaimedCount, diskID)
		if len(items) > 0 {
			renderSnapshotTable(w, items)
		}
		return
	}
	if len(items) == 0 && reclaimedCount == 0 {
		fmt.Fprintln(w, "No snapshots pruned.")
		return
	}
	fmt.Fprintf(w, "Pruned %d snapshot(s) for disk %s:\n", reclaimedCount, diskID)
	if len(items) > 0 {
		renderSnapshotTable(w, items)
	}
}
