package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/sandbox"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// A template is an ordinary disk whose name starts with templateDiskPrefix and which holds a
// whole system plus the guest agent. Sandboxes fork it.
const templateDiskPrefix = "template/"

func newSandboxTemplateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "template",
		Short: "Manage templates: disks holding a system image that sandboxes are forked from",
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
	cmd.AddCommand(newTemplateCreateCmd(), newTemplateListCmd(), newTemplateDeleteCmd())
	return cmd
}

func newTemplateCreateCmd() *cobra.Command {
	var name, size, binary, endpoint, token string
	var block bool
	var prewarm string
	cmd := &cobra.Command{
		Use:   "create <image>",
		Short: "Turn a container image (e.g. ubuntu:24.04) into a template disk; needs a local Docker",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			image := posArgs[0]
			name = strings.TrimSpace(cmp.Or(name, image))
			maxBytes, err := ParseSize(size)
			if err != nil {
				return usageExitErr(fmt.Errorf("invalid --size: %w", err), false)
			}
			if err = validateDiskName(templateDiskPrefix + name); err != nil {
				return usageExitErr(err, false)
			}
			if block && sboxRuntimeName != runtimeMicroVM {
				return usageExitErr(errors.New("--block templates boot only on the microvm runtime"), false)
			}
			if sboxLocal {
				return createLocalTemplate(cmd, image, name, int64(maxBytes), prewarm) //nolint:gosec // a disk size
			}
			// microvm mounts the disk on this host with the host's bowl; the Docker runtimes inject a
			// Linux client into a helper container and build one when --binary is empty.
			if binary == "" && sboxRuntimeName == runtimeMicroVM {
				if binary, err = exec.LookPath("bowl"); err != nil {
					return runtimeExitErr(errors.New("bowl client not found in PATH; pass --binary"))
				}
			}
			client, code, err := newDiskStorageClient(endpoint, token)
			if err != nil {
				return &cliExitError{code: code, err: err}
			}
			ctx := context.Background()
			if id, findErr := findTemplate(ctx, client, name); findErr == nil {
				return runtimeExitErr(fmt.Errorf("template %q already exists (disk %s); delete it first", name, id))
			}
			resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
				Name: templateDiskPrefix + name, MaxBytes: maxBytes,
			}))
			if err != nil {
				return runtimeExitErr(fmt.Errorf("create template disk: %w", err))
			}
			diskID := resp.Msg.GetDisk().GetDiskId()
			start := time.Now()
			if err = fillTemplate(ctx, binary, diskID, image, endpoint, token, blockSize(block, maxBytes)); err != nil {
				_ = sboxDeleteDisk(endpoint, token, diskID)
				return runtimeExitErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Template %s is ready in %s\n  Disk:  %s\n  Use:   bowl-sandbox create --template %s\n",
				name, time.Since(start).Round(time.Millisecond), diskID, name)
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&name, "name", "", "Template name (default: the image name)")
	flags.StringVar(&size, "size", "4GiB", "Capacity of the template disk; sandboxes forked from it get the same")
	flags.BoolVar(&block, "block", false, "Store the system as an ext4 block-device image on the disk (microvm only); "+
		"faster for many small files, but diff and merge see one image file")
	flags.StringVar(&prewarm, "prewarm", "", "With --local: run this shell command in the template VM before its snapshot is taken, "+
		"so every sandbox starts with its effects in memory (e.g. a warm page cache)")
	flags.StringVar(&binary, "binary", "", "Linux bowl client that mounts the disk: on this host for microvm (default: bowl in PATH), "+
		"inside a helper container otherwise (default: build one)")
	addSandboxStorageFlags(cmd, &endpoint, &token)
	return cmd
}

// createLocalTemplate builds the template as an ext4 image in the state directory.
func createLocalTemplate(cmd *cobra.Command, image, name string, sizeBytes int64, prewarm string) error {
	vm := &sandbox.MicroVM{StateDir: sboxStateDir}
	dir := vm.LocalTemplateDir(name)
	if _, err := os.Stat(dir); err == nil {
		return runtimeExitErr(fmt.Errorf("local template %q already exists; remove %s first", name, dir))
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return runtimeExitErr(err)
	}
	start := time.Now()
	if err := vm.BuildBlockImage(context.Background(), image, runtime.GOARCH, dir, sizeBytes, cmd.ErrOrStderr()); err != nil {
		_ = os.RemoveAll(dir)
		return runtimeExitErr(err)
	}
	// A snapshot of a booted VM lets create restore instead of booting. Without it the template still works.
	var warm []string
	if prewarm != "" {
		warm = []string{"sh", "-c", prewarm}
	}
	if err := vm.SnapshotTemplate(context.Background(), dir, warm); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: no snapshot, sandboxes from this template will boot normally: %v\n", err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Local template %s is ready in %s\n  Use:   bowl-sandbox --local create --template %s\n",
		name, time.Since(start).Round(time.Millisecond), name)
	return nil
}

// fillTemplate unpacks the image and the guest agent into the new disk and unmounts it, which
// flushes everything to the storage node. The microvm runtime mounts the disk on this host;
// the Docker-based ones mount it in a throwaway container, so it also works where the host has
// no FUSE (macOS).
func fillTemplate(ctx context.Context, binary, diskID, image, endpoint, token string, blockBytes int64) error {
	if sboxRuntimeName != runtimeMicroVM {
		return fillTemplateInContainer(ctx, binary, diskID, image, endpoint, token)
	}
	dir, err := os.MkdirTemp("", "bowl-template-")
	if err != nil {
		return err
	}
	defer os.Remove(dir)
	resolved, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		return err
	}
	mount := exec.CommandContext(ctx, binary, "mount", diskID, dir, "--endpoint", resolved) //nolint:gosec // our own client
	mount.Env = append(os.Environ(), "BOWL_TOKEN="+rpc.ResolveToken(token))
	if out, mountErr := mount.CombinedOutput(); mountErr != nil {
		return fmt.Errorf("mount template disk: %w: %s", mountErr, strings.TrimSpace(string(out)))
	}
	umount := func() error {
		out, umountErr := exec.Command(binary, "umount", dir).CombinedOutput() //nolint:gosec // our own client
		if umountErr != nil {
			return fmt.Errorf("unmount template disk: %w: %s", umountErr, strings.TrimSpace(string(out)))
		}
		return nil
	}
	vm := &sandbox.MicroVM{StateDir: sboxStateDir}
	if blockBytes > 0 {
		if err = vm.BuildBlockImage(ctx, image, runtime.GOARCH, dir, blockBytes, os.Stderr); err != nil {
			_ = umount()
			return err
		}
		return umount()
	}
	if err = sandbox.ExportImage(ctx, image, runtime.GOARCH, dir, os.Stderr); err != nil {
		_ = umount()
		return err
	}
	if err = vm.InstallAgent(dir); err != nil {
		_ = umount()
		return fmt.Errorf("install guest agent: %w", err)
	}
	return umount()
}

func fillTemplateInContainer(ctx context.Context, binary, diskID, image, endpoint, token string) error {
	resolved, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		return err
	}
	helper := &sandbox.Runtime{DefaultCaps: true}
	const at = "/mnt/template"
	spec := &sandbox.CreateSpec{ID: diskID, MountPath: at, HostEndpoint: resolved, Token: rpc.ResolveToken(token)}
	if err = sboxStartOn(helper, spec, binary, false, os.Stderr); err != nil {
		return fmt.Errorf("start a container to fill the template: %w", err)
	}
	defer func() { _ = helper.Remove(context.WithoutCancel(ctx), diskID) }()
	if importErr := helper.ImportImage(ctx, diskID, image, spec.Arch, at, os.Stderr); importErr != nil {
		return importErr
	}
	// The guest agent only matters for the microvm runtime; put it in when this host has one so
	// the template works there too.
	if agent := (&sandbox.MicroVM{StateDir: sboxStateDir}).GuestAgentFile(); agent != "" {
		code, execErr := helper.Exec(ctx, diskID, "/", []string{"mkdir", "-p", at + "/.bowl"}, sandbox.ExecOptions{}, nil, io.Discard, os.Stderr)
		if execErr != nil || code != 0 {
			return fmt.Errorf("prepare guest agent directory: %w", errors.Join(execErr, fmt.Errorf("exit code %d", code)))
		}
		if err = helper.CopyInto(ctx, diskID, agent, at+"/.bowl/guest", os.Stderr); err != nil {
			return fmt.Errorf("install guest agent: %w", err)
		}
	}
	// Unmount before the deferred Remove so a flush failure is reported, not swallowed.
	return helper.Unmount(ctx, diskID, at)
}

// findTemplate returns the disk ID of the newest template with this name.
//
// ponytail: walks every disk because ListDisks has no name filter; add one server-side when
// disk counts get large.
func findTemplate(ctx context.Context, client storagev1connect.StorageServiceClient, name string) (string, error) {
	disks, err := listTemplateDisks(ctx, client)
	if err != nil {
		return "", err
	}
	for _, d := range disks {
		if strings.TrimPrefix(d.GetName(), templateDiskPrefix) == name {
			return d.GetDiskId(), nil
		}
	}
	return "", fmt.Errorf("template %q not found", name)
}

// listTemplateDisks returns template disks, newest first (the server's order).
func listTemplateDisks(ctx context.Context, client storagev1connect.StorageServiceClient) ([]*storagev1.Disk, error) {
	var out []*storagev1.Disk
	token := ""
	for {
		resp, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{PageSize: 1000, PageToken: token}))
		if err != nil {
			return nil, fmt.Errorf("list disks: %w", err)
		}
		for _, d := range resp.Msg.GetDisks() {
			if strings.HasPrefix(d.GetName(), templateDiskPrefix) {
				out = append(out, d)
			}
		}
		if token = resp.Msg.GetNextPageToken(); token == "" {
			return out, nil
		}
	}
}

func newTemplateListCmd() *cobra.Command {
	var endpoint, token string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List templates",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, code, err := newDiskStorageClient(endpoint, token)
			if err != nil {
				return &cliExitError{code: code, err: err}
			}
			disks, err := listTemplateDisks(context.Background(), client)
			if err != nil {
				return runtimeExitErr(err)
			}
			out := cmd.OutOrStdout()
			if len(disks) == 0 {
				fmt.Fprintln(out, "No templates")
				return nil
			}
			const format = "%-24s %-38s %-10s %s\n"
			fmt.Fprintf(out, format, "NAME", "DISK ID", "USED", "CREATED")
			for _, d := range disks {
				dj := toDiskJSON(d)
				fmt.Fprintf(out, format, strings.TrimPrefix(dj.Name, templateDiskPrefix), dj.DiskID, FormatSize(dj.UsedBytes), dj.CreatedAt)
			}
			return nil
		},
	}
	addSandboxStorageFlags(cmd, &endpoint, &token)
	return cmd
}

func newTemplateDeleteCmd() *cobra.Command {
	var endpoint, token string
	cmd := &cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"rm"},
		Short:   "Delete a template; sandboxes already forked from it keep working",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			client, code, err := newDiskStorageClient(endpoint, token)
			if err != nil {
				return &cliExitError{code: code, err: err}
			}
			id, err := findTemplate(context.Background(), client, posArgs[0])
			if err != nil {
				return runtimeExitErr(err)
			}
			if err = sboxDeleteDisk(endpoint, token, id); err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted template %s (disk %s)\n", posArgs[0], id)
			return nil
		},
	}
	addSandboxStorageFlags(cmd, &endpoint, &token)
	return cmd
}

// blockSize leaves headroom on the disk for the image file's own metadata.
func blockSize(block bool, diskBytes uint64) int64 {
	if !block {
		return 0
	}
	return int64(diskBytes - min(diskBytes/10, 256<<20)) //nolint:gosec // disk sizes fit in int64
}
