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

type snapshotListJSON struct {
	Snapshots     []snapshotJSON `json:"snapshots"`
	NextPageToken string         `json:"next_page_token"`
}

var snapshotListSpec = diskSubcommandSpec{
	use:       "list <disk-id>",
	aliases:   []string{"ls"},
	short:     "List snapshots in a virtual disk lineage",
	args:      cobra.ExactArgs(1),
	uuidLabel: "disk-id",
	spanName:  "cli.snapshot.list",
	timeout:   10 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &snapshotListAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.kindRaw, "kind", "", "Filter snapshots by kind: fork, merge, or manual")
		flags.Uint32Var(&a.pageSize, "page-size", 0, "Maximum number of snapshots per page in [1, 1000]")
		flags.StringVar(&a.pageToken, "page-token", "", "Pagination cursor token")
		return a
	},
}

type snapshotListAdapter struct {
	diskID    string
	kindRaw   string
	kind      storagev1.SnapshotKind
	pageSize  uint32
	pageToken string
}

func (a *snapshotListAdapter) finalize(cmd *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	flags := cmd.Flags()

	if flags.Changed("kind") {
		kind, err := parseSnapshotKind(a.kindRaw)
		if err != nil {
			return false, err
		}
		a.kind = kind
	}
	if flags.Changed("page-size") {
		if a.pageSize == 0 || a.pageSize > maxSnapshotPageSize {
			return false, fmt.Errorf("invalid --page-size %d: must be an integer in [1, %d]", a.pageSize, maxSnapshotPageSize)
		}
	}
	if len(a.pageToken) > maxSnapshotPageTokenBytes {
		return false, fmt.Errorf("--page-token cannot exceed %d bytes", maxSnapshotPageTokenBytes)
	}
	if strings.Contains(a.pageToken, "\x00") {
		return false, errors.New("--page-token cannot contain NUL bytes")
	}
	return false, nil
}

func (a *snapshotListAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *snapshotListAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *snapshotListAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ListSnapshots(ctx, connect.NewRequest(&storagev1.ListSnapshotsRequest{
		DiskId:    a.diskID,
		Kind:      a.kind,
		PageSize:  a.pageSize,
		PageToken: a.pageToken,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			defaultOp:       "list snapshots failed",
			checkInvalidArg: true,
		})
	}

	items := toSnapshotJSONList(resp.Msg.GetSnapshots())
	nextPageToken := resp.Msg.GetNextPageToken()

	return diskOutcome{
		jsonPayload: snapshotListJSON{
			Snapshots:     items,
			NextPageToken: nextPageToken,
		},
		renderHuman: func(w io.Writer) {
			if len(items) == 0 {
				fmt.Fprintln(w, "No snapshots found.")
			} else {
				renderSnapshotTable(w, items)
			}
			if nextPageToken != "" {
				fmt.Fprintf(w, "\nNext Page Token: %s\n", nextPageToken)
			}
		},
	}, nil
}
