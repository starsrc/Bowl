package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	maxSnapshotPageSize       = 1000
	maxSnapshotPageTokenBytes = 4096
)

type snapshotJSON struct {
	SnapshotID            string `json:"snapshot_id"`
	SourceDiskID          string `json:"source_disk_id"`
	ParentSnapshotID      string `json:"parent_snapshot_id,omitempty"`
	MergeParentSnapshotID string `json:"merge_parent_snapshot_id,omitempty"`
	Kind                  string `json:"kind"`
	Name                  string `json:"name,omitempty"`
	Protected             bool   `json:"protected"`
	CreatedAt             string `json:"created_at"`
}

func newSnapshotCmd(ports diskPorts) *cobra.Command {
	snapshotCmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Manage virtual disk snapshots",
		Args: func(cmd *cobra.Command, posArgs []string) error {
			if len(posArgs) > 0 {
				return fmt.Errorf("unknown command %q for %q", posArgs[0], cmd.CommandPath())
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprint(cmd.ErrOrStderr(), cmd.UsageString())
			return &cliExitError{code: 2, silent: true}
		},
	}

	specs := []*diskSubcommandSpec{
		&snapshotCreateSpec,
		&snapshotListSpec,
		&snapshotDeleteSpec,
		&snapshotPruneSpec,
	}
	for _, spec := range specs {
		snapshotCmd.AddCommand(newDiskSubcommand(spec, ports))
	}
	return snapshotCmd
}

func snapshotKindLabel(kind storagev1.SnapshotKind) string {
	switch kind {
	case storagev1.SnapshotKind_SNAPSHOT_KIND_FORK:
		return "fork"
	case storagev1.SnapshotKind_SNAPSHOT_KIND_MERGE:
		return "merge"
	case storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL:
		return "manual"
	default:
		return "unknown"
	}
}

func parseSnapshotKind(raw string) (storagev1.SnapshotKind, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "fork":
		return storagev1.SnapshotKind_SNAPSHOT_KIND_FORK, nil
	case "merge":
		return storagev1.SnapshotKind_SNAPSHOT_KIND_MERGE, nil
	case "manual":
		return storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL, nil
	default:
		return storagev1.SnapshotKind_SNAPSHOT_KIND_UNSPECIFIED, fmt.Errorf("invalid --kind %q: must be fork, merge, or manual", raw)
	}
}

func toSnapshotJSON(s *storagev1.Snapshot) snapshotJSON {
	var createdAt string
	if s.GetCreatedAt() != nil {
		createdAt = s.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	return snapshotJSON{
		SnapshotID:            s.GetSnapshotId(),
		SourceDiskID:          s.GetSourceDiskId(),
		ParentSnapshotID:      s.GetParentSnapshotId(),
		MergeParentSnapshotID: s.GetMergeParentSnapshotId(),
		Kind:                  snapshotKindLabel(s.GetKind()),
		Name:                  s.GetName(),
		Protected:             s.GetProtected(),
		CreatedAt:             createdAt,
	}
}

func toSnapshotJSONList(snaps []*storagev1.Snapshot) []snapshotJSON {
	items := make([]snapshotJSON, 0, len(snaps))
	for _, s := range snaps {
		items = append(items, toSnapshotJSON(s))
	}
	return items
}

func requireNonNilSnapshot(s *storagev1.Snapshot) (*storagev1.Snapshot, *diskCLIError) {
	if s == nil {
		return nil, &diskCLIError{
			exitCode: 1,
			msg:      "server returned empty snapshot metadata",
		}
	}
	return s, nil
}

func printSnapshotDetail(w io.Writer, s *storagev1.Snapshot) {
	sj := toSnapshotJSON(s)
	fmt.Fprintf(w, "Snapshot ID:   %s\n", sj.SnapshotID)
	fmt.Fprintf(w, "Source Disk:   %s\n", sj.SourceDiskID)
	if sj.Name != "" {
		fmt.Fprintf(w, "Name:          %s\n", sj.Name)
	}
	fmt.Fprintf(w, "Kind:          %s\n", sj.Kind)
	parent := sj.ParentSnapshotID
	if parent == "" {
		parent = "-"
	}
	fmt.Fprintf(w, "Parent:        %s\n", parent)
	if sj.MergeParentSnapshotID != "" {
		fmt.Fprintf(w, "Merge Parent:  %s\n", sj.MergeParentSnapshotID)
	}
	fmt.Fprintf(w, "Protected:     %t\n", sj.Protected)
	fmt.Fprintf(w, "Created At:    %s\n", sj.CreatedAt)
}

func renderSnapshotTable(w io.Writer, items []snapshotJSON) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SNAPSHOT ID\tKIND\tPROTECTED\tNAME\tSOURCE DISK\tCREATED AT")
	for _, sj := range items {
		name := sj.Name
		if name == "" {
			name = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%t\t%s\t%s\t%s\n",
			sj.SnapshotID,
			sj.Kind,
			sj.Protected,
			name,
			sj.SourceDiskID,
			sj.CreatedAt,
		)
	}
	_ = tw.Flush()
}

func confirmInteractive(stdin io.Reader, stderr io.Writer, prompt string) *diskCLIError {
	fmt.Fprint(stderr, prompt)
	line, readErr := bufio.NewReader(stdin).ReadString('\n')
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("read confirmation input: %v", readErr),
		}
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	if answer != "y" && answer != "yes" {
		fmt.Fprintln(stderr, "Operation canceled")
		return &diskCLIError{exitCode: 1}
	}
	return nil
}
