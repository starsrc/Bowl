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

type leaseItemJSON struct {
	MountID   string `json:"mount_id"`
	Branch    string `json:"branch"`
	Mode      string `json:"mode"`
	Epoch     uint64 `json:"epoch"`
	MountedAt string `json:"mounted_at,omitempty"`
	ExpiresAt string `json:"expires_at"`
}

type leaseListJSON struct {
	DiskID string          `json:"disk_id"`
	Branch string          `json:"branch,omitempty"`
	Mounts []leaseItemJSON `json:"mounts"`
}

type leaseEvictJSON struct {
	DiskID          string       `json:"disk_id"`
	Branch          string       `json:"branch,omitempty"`
	MountID         string       `json:"mount_id,omitempty"`
	EvictedCount    uint32       `json:"evicted_count"`
	UpdatedBranches []branchJSON `json:"updated_branches"`
}

func newLeaseCmd(ports diskPorts, rootOpts *rootOptions) *cobra.Command {
	specs := []*diskSubcommandSpec{
		&leaseListSpec,
		&leaseEvictSpec,
	}
	return newGroupCmd("lease", "Manage active mount leases on a virtual disk", specs, ports, rootOpts)
}

var leaseListSpec = diskSubcommandSpec{
	use:              "list",
	aliases:          []string{"ls"},
	short:            "List active mount leases for a virtual disk or branch",
	args:             cobra.NoArgs,
	requireDiskScope: true,
	allowQuiet:       true,
	quietHelp:        "Quiet mode, output only mount lease IDs one per line",
	spanName:         "cli.lease.list",
	timeout:          10 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &leaseListAdapter{}
		cmd.Flags().StringVar(&a.branch, "branch", "", "Optional branch name filter")
		return a
	},
}

type leaseListAdapter struct {
	diskID string
	branch string
}

func (a *leaseListAdapter) finalize(cmd *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	if cmd.Flags().Changed("branch") {
		if err := validateBranchName(a.branch, "--branch"); err != nil {
			return false, err
		}
	}
	return false, nil
}

func (a *leaseListAdapter) spanAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
	if a.branch != "" {
		attrs = append(attrs, attribute.String("bowl.branch", a.branch))
	}
	return attrs
}

func (a *leaseListAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *leaseListAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: a.diskID,
		Branch: a.branch,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			branch:          a.branch,
			defaultOp:       "list mount leases failed",
			checkInvalidArg: true,
		})
	}

	mounts := resp.Msg.GetMounts()
	items := make([]leaseItemJSON, 0, len(mounts))
	ids := make([]string, 0, len(mounts))
	for _, m := range mounts {
		mode := "rw"
		if m.GetReadOnly() {
			mode = "ro"
		}
		var mountedAt, expiresAt string
		if m.GetMountedAt() != nil {
			mountedAt = m.GetMountedAt().AsTime().UTC().Format(time.RFC3339)
		}
		if m.GetExpiresAt() != nil {
			expiresAt = m.GetExpiresAt().AsTime().UTC().Format(time.RFC3339)
		}
		items = append(items, leaseItemJSON{
			MountID:   m.GetMountId(),
			Branch:    m.GetBranch(),
			Mode:      mode,
			Epoch:     m.GetEpoch(),
			MountedAt: mountedAt,
			ExpiresAt: expiresAt,
		})
		ids = append(ids, m.GetMountId())
	}

	return diskOutcome{
		quietLines:    ids,
		hasQuietLines: true,
		jsonPayload: leaseListJSON{
			DiskID: a.diskID,
			Branch: a.branch,
			Mounts: items,
		},
		renderHuman: func(w io.Writer) {
			renderLeaseTable(w, items)
		},
	}, nil
}

func renderLeaseTable(w io.Writer, items []leaseItemJSON) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MOUNT ID\tBRANCH\tMODE\tEPOCH\tMOUNTED AT\tEXPIRES AT")
	for _, m := range items {
		mountedAt := m.MountedAt
		if mountedAt == "" {
			mountedAt = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n", m.MountID, m.Branch, m.Mode, m.Epoch, mountedAt, m.ExpiresAt)
	}
	_ = tw.Flush()
}

var leaseEvictSpec = diskSubcommandSpec{
	use:              "evict",
	short:            "Evict active mount leases for a virtual disk or branch",
	args:             cobra.NoArgs,
	requireDiskScope: true,
	spanName:         "cli.lease.evict",
	timeout:          15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &leaseEvictAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.branch, "branch", "", "Optional branch name to evict leases from")
		flags.StringVar(&a.mountID, "mount-id", "", "Evict only the specified mount lease UUID")
		return a
	},
}

type leaseEvictAdapter struct {
	diskID     string
	branch     string
	mountID    string
	hasMountID bool
}

func (a *leaseEvictAdapter) finalize(cmd *cobra.Command, diskID string, _ []string) (bool, error) {
	a.diskID = diskID
	flags := cmd.Flags()
	if flags.Changed("branch") {
		if err := validateBranchName(a.branch, "--branch"); err != nil {
			return false, err
		}
	}
	if flags.Changed("mount-id") {
		if _, err := parseNonNilUUID(a.mountID, "mount-id"); err != nil {
			return false, err
		}
		a.hasMountID = true
	}
	return false, nil
}

func (a *leaseEvictAdapter) spanAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
	if a.branch != "" {
		attrs = append(attrs, attribute.String("bowl.branch", a.branch))
	}
	if a.hasMountID {
		attrs = append(attrs, attribute.String("bowl.mount_id", a.mountID))
	}
	return attrs
}

func (a *leaseEvictAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *leaseEvictAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	req := &storagev1.EvictMountsRequest{
		DiskId: a.diskID,
		Branch: a.branch,
	}
	if a.hasMountID {
		mountID := a.mountID
		req.MountId = &mountID
	}

	resp, err := client.EvictMounts(ctx, connect.NewRequest(req))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:          a.diskID,
			branch:          a.branch,
			defaultOp:       "evict mount leases failed",
			checkInvalidArg: true,
		})
	}

	evictedCount := resp.Msg.GetEvictedCount()
	updatedBranches := resp.Msg.GetUpdatedBranches()
	updatedJSON := make([]branchJSON, 0, len(updatedBranches))
	for _, b := range updatedBranches {
		updatedJSON = append(updatedJSON, toBranchJSON(b))
	}

	return diskOutcome{
		jsonPayload: leaseEvictJSON{
			DiskID:          a.diskID,
			Branch:          a.branch,
			MountID:         a.mountID,
			EvictedCount:    evictedCount,
			UpdatedBranches: updatedJSON,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "Evicted %d mount lease(s) for disk %s\n", evictedCount, a.diskID)
			if len(updatedJSON) > 0 {
				fmt.Fprintln(w, "Updated branch writer epochs:")
				tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
				fmt.Fprintln(tw, "BRANCH\tWRITER EPOCH")
				for _, b := range updatedJSON {
					fmt.Fprintf(tw, "%s\t%d\n", b.Name, b.WriterEpoch)
				}
				_ = tw.Flush()
			}
		},
	}, nil
}
