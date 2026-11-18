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

type diskListJSON struct {
	Disks         []diskJSON `json:"disks"`
	NextPageToken string     `json:"next_page_token"`
}

var diskListSpec = diskSubcommandSpec{
	use:      "list",
	short:    "List virtual disks with pagination",
	args:     cobra.NoArgs,
	spanName: "cli.disk.list",
	timeout:  10 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskListAdapter{}
		flags := cmd.Flags()
		flags.IntVar(&a.limit, "limit", 50, "Maximum number of disks per page in [1, 1000]")
		flags.StringVar(&a.pageToken, "page-token", "", "Pagination cursor token")
		return a
	},
}

type diskListAdapter struct {
	limit     int
	pageToken string
	pageSize  uint32
}

func (a *diskListAdapter) finalize(cmd *cobra.Command, _ []string) (bool, error) {
	if cmd.Flags().Changed("limit") {
		if a.limit < 1 || a.limit > 1000 {
			return false, fmt.Errorf("invalid --limit %d: must be an integer in [1, 1000]", a.limit)
		}
		a.pageSize = uint32(a.limit)
	}
	return false, nil
}

func (a *diskListAdapter) spanAttrs() []attribute.KeyValue {
	return nil
}

func (a *diskListAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskListAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize:  a.pageSize,
		PageToken: a.pageToken,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			defaultOp: "list virtual disks failed",
		})
	}

	disks := resp.Msg.GetDisks()
	nextPageToken := resp.Msg.GetNextPageToken()

	items := make([]diskJSON, 0, len(disks))
	for _, d := range disks {
		items = append(items, toDiskJSON(d))
	}

	return diskOutcome{
		jsonPayload: diskListJSON{
			Disks:         items,
			NextPageToken: nextPageToken,
		},
		renderHuman: func(w io.Writer) {
			renderDiskListTable(w, items, nextPageToken)
		},
	}, nil
}

func renderDiskListTable(w io.Writer, items []diskJSON, nextPageToken string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "DISK ID\tNAME\tUSED\tMAX\tUSAGE%\tINODES\tCREATED AT")
	for _, dj := range items {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.2f%%\t%d\t%s\n",
			dj.DiskID,
			dj.Name,
			FormatSize(dj.UsedBytes),
			FormatSize(dj.MaxBytes),
			diskUsagePct(dj.UsedBytes, dj.MaxBytes),
			dj.UsedInodes,
			dj.CreatedAt,
		)
	}
	_ = tw.Flush()

	if nextPageToken != "" {
		fmt.Fprintf(w, "\nNext Page Token: %s\n", nextPageToken)
	}
}
