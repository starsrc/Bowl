package cli

import (
	"bufio"
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

type diskDeleteJSON struct {
	DiskID    string `json:"disk_id"`
	Deleted   bool   `json:"deleted"`
	Recursive bool   `json:"recursive,omitempty"`
}

var diskDeleteSpec = diskSubcommandSpec{
	use:       "delete <disk-id>",
	short:     "Delete a virtual disk",
	args:      cobra.ExactArgs(1),
	uuidLabel: "disk-id",
	spanName:  "cli.disk.delete",
	timeout:   15 * time.Second,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskDeleteAdapter{}
		cmd.Flags().BoolVarP(&a.yes, "yes", "y", false, "Skip interactive confirmation prompt")
		cmd.Flags().BoolVarP(&a.recursive, "recursive", "r", false, "Recursively delete this disk and all forked descendant disks")
		return a
	},
}

type diskDeleteAdapter struct {
	diskID    string
	yes       bool
	recursive bool
}

func (a *diskDeleteAdapter) finalize(_ *cobra.Command, posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	return false, nil
}

func (a *diskDeleteAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("bowl.disk_id", a.diskID),
		attribute.Bool("bowl.recursive", a.recursive),
	}
}

func (a *diskDeleteAdapter) preRPC(ports diskPorts, stderr io.Writer) *diskCLIError {
	info, err := ports.findMountpoint(a.diskID)
	if err != nil {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("check local mount table: %v", err),
		}
	}
	if info.Mounted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("disk %s is currently mounted at %s; run bowl umount %s before deleting", a.diskID, info.Mountpoint, info.Mountpoint),
		}
	}

	if !a.yes {
		if a.recursive {
			fmt.Fprintf(stderr, "Delete virtual disk %s and all forked descendants? This action is irreversible [y/N]: ", a.diskID)
		} else {
			fmt.Fprintf(stderr, "Delete virtual disk %s? This action is irreversible [y/N]: ", a.diskID)
		}
		line, readErr := bufio.NewReader(ports.stdin).ReadString('\n')
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
	}

	return nil
}

func (a *diskDeleteAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId:    a.diskID,
		Recursive: a.recursive,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:           a.diskID,
			defaultOp:        "delete virtual disk failed",
			checkInvalidArg:  true,
			checkEBUSYReason: true,
			preconditionMsg: func(id string, _ error) string {
				return fmt.Sprintf("disk %s is still mounted with an active client session; unmount it, or run `bowl disk mounts %s` / `bowl disk evict %s` before deleting", id, id, id)
			},
		})
	}

	return diskOutcome{
		jsonPayload: diskDeleteJSON{
			DiskID:    a.diskID,
			Deleted:   true,
			Recursive: a.recursive,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "Deleted virtual disk %s\n", a.diskID)
		},
	}, nil
}
