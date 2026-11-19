package cli

import (
	"bytes"
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
	"golang.org/x/term"

	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/sandbox"
	"github.com/starsrc/bowl/client/internal/version"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// This file is only the command line: parse flags, create and delete the disk through
// StorageService, call the sandbox runtime, print. VMs, containers and mounting live in internal/sandbox.

// sboxRuntime is the backend picked by --runtime before any subcommand runs.
var (
	sboxRuntime     sandbox.Backend
	sboxRuntimeName string
	sboxStateDir    string
)

const (
	runtimeMicroVM = "microvm"
	runtimeDocker  = "docker"
)

// pickSandboxRuntime resolves --runtime: an explicit value wins, then $BOWL_SANDBOX_RUNTIME,
// then microvm on Linux hosts with /dev/kvm and docker everywhere else.
func pickSandboxRuntime(flagValue, stateDir string) (sandbox.Backend, string, error) {
	name := cmp.Or(flagValue, os.Getenv("BOWL_SANDBOX_RUNTIME"))
	if name == "" {
		name = runtimeDocker
		if _, err := os.Stat("/dev/kvm"); err == nil && runtime.GOOS == "linux" {
			name = runtimeMicroVM
		}
	}
	switch name {
	case runtimeMicroVM:
		return &sandbox.MicroVM{StateDir: stateDir}, name, nil
	case runtimeDocker:
		return &sandbox.Runtime{}, name, nil
	default:
		return nil, "", fmt.Errorf("unknown --runtime %q: use %s or %s", name, runtimeMicroVM, runtimeDocker)
	}
}

func newSandboxCmd() *cobra.Command {
	var runtimeFlag, stateDir string
	cmd := &cobra.Command{
		Use:   "bowl-sandbox",
		Short: "Run sandboxes whose disk is a Bowl virtual disk",
		Long: "Run sandboxes whose disk is a Bowl virtual disk. The sandbox ID is the disk ID.\n\n" +
			"Runtimes:\n" +
			"  microvm  Cloud Hypervisor VM; the disk is the VM's root filesystem. Linux with /dev/kvm, run as root.\n" +
			"           create needs --from <template disk> holding an installed system.\n" +
			"  docker   container with the disk mounted at --mount. --endpoint must be reachable from the\n" +
			"           container; loopback addresses are rewritten to host.docker.internal.",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			backend, name, err := pickSandboxRuntime(runtimeFlag, stateDir)
			if err != nil {
				return usageExitErr(err, false)
			}
			sboxRuntime, sboxRuntimeName, sboxStateDir = backend, name, stateDir
			return nil
		},
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
	cmd.AddCommand(
		newSandboxCreateCmd(),
		newSandboxListCmd(),
		newSandboxInfoCmd(),
		newSandboxExecCmd(),
		newSandboxLogsCmd(),
		newSandboxWaitCmd(),
		newSandboxShellCmd(),
		newSandboxCpCmd(),
		newSandboxKillCmd(),
		newSandboxPauseCmd(true),
		newSandboxPauseCmd(false),
		newSandboxTimeoutCmd(),
		newSandboxReapCmd(),
		newSandboxTemplateCmd(),
	)
	cmd.PersistentFlags().StringVar(&runtimeFlag, "runtime", "", "Sandbox runtime: microvm or docker (default: $BOWL_SANDBOX_RUNTIME, else microvm when /dev/kvm exists)")
	cmd.PersistentFlags().StringVar(&stateDir, "state-dir", "", "MicroVM state directory holding vmlinux and bowl-sandbox-guest (default /var/lib/bowl-sandbox)")
	return cmd
}

func newSandboxCreateCmd() *cobra.Command {
	var (
		from, template, size, image, name, mountAt, network, binary, endpoint, token string
		rebuild, quiet                                                               bool
		cpus, memoryMiB                                                              int
		timeout                                                                      time.Duration
	)
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a sandbox, forking its disk from --from or creating an empty one",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if from != "" && template != "" {
				return usageExitErr(errors.New("--from and --template cannot be used together"), false)
			}
			if sboxRuntimeName == runtimeMicroVM && from == "" && template == "" {
				return usageExitErr(errors.New("the microvm runtime boots from the disk, so --template <name> or --from <disk> is required"), false)
			}
			if sboxRuntimeName == runtimeDocker {
				if _, err := exec.LookPath("docker"); err != nil {
					return runtimeExitErr(errors.New("docker command not found; the docker runtime needs a local Docker"))
				}
			}
			resolvedEndpoint, err := rpc.ResolveEndpoint(endpoint)
			if err != nil {
				return usageExitErr(fmt.Errorf("invalid endpoint: %w", err), false)
			}
			client, code, err := newDiskStorageClient(resolvedEndpoint, token)
			if err != nil {
				return &cliExitError{code: code, err: err}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			if template != "" {
				if from, err = findTemplate(ctx, client, template); err != nil {
					cancel()
					return runtimeExitErr(err)
				}
			}
			diskID, cliErr := sboxCreateDisk(ctx, client, from, size, name)
			cancel()
			if cliErr != nil {
				return &cliExitError{code: cliErr.exitCode, err: errors.New(cliErr.msg)}
			}

			spec := &sandbox.CreateSpec{
				ID: diskID, Image: image, Name: name, MountPath: mountAt,
				Network: network, HostEndpoint: resolvedEndpoint, Token: rpc.ResolveToken(token),
				CPUs: cpus, MemoryMiB: memoryMiB,
			}
			if sboxRuntimeName == runtimeMicroVM {
				// The disk is mounted on this host, so the host's own Linux bowl client does it.
				spec.Image = from
				err = sboxStartMicroVM(spec, binary)
			} else {
				err = sboxStart(spec, binary, rebuild, cmd.ErrOrStderr())
			}
			if err != nil {
				// Best effort: the caller is already getting the start error.
				dropCtx, dropCancel := context.WithTimeout(context.Background(), 15*time.Second)
				_, _ = client.DeleteDisk(dropCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
				dropCancel()
				return runtimeExitErr(err)
			}

			if timeout > 0 {
				if err = sboxSetDeadline(diskID, timeout); err != nil {
					return runtimeExitErr(fmt.Errorf("sandbox %s is up but the timeout was not set: %w", diskID, err))
				}
			}
			out := cmd.OutOrStdout()
			if quiet {
				fmt.Fprintln(out, diskID)
				return nil
			}
			fmt.Fprintf(out, "Sandbox %s is ready (%s)\n", diskID, sboxRuntimeName)
			if sboxRuntimeName == runtimeMicroVM {
				fmt.Fprintf(out, "  Template:  %s\n", cmp.Or(template, from))
				fmt.Fprintf(out, "  Disk:      %s (the VM's root filesystem)\n", diskID)
			} else {
				fmt.Fprintf(out, "  Image:     %s\n", image)
				fmt.Fprintf(out, "  Disk:      %s (mounted at %s)\n", diskID, mountAt)
			}
			fmt.Fprintf(out, "  Run:       bowl-sandbox exec %s -- <command>\n", diskID)
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&from, "from", "", "Fork the sandbox disk from an existing disk (the source is untouched); required for microvm")
	flags.StringVar(&template, "template", "", "Fork the sandbox disk from this template (see bowl-sandbox template list)")
	flags.StringVar(&size, "size", "1GiB", "Capacity of the new disk; ignored with --from")
	flags.StringVar(&image, "image", sandbox.DefaultImage, "Container image (docker only)")
	flags.StringVar(&name, "name", "", "Disk name and container hostname")
	flags.StringVar(&mountAt, "mount", sandbox.DefaultMountPath, "Disk mountpoint inside the container (docker only)")
	flags.StringVar(&network, "network", "", "Attach the sandbox to this docker network (docker only)")
	flags.StringVar(&binary, "binary", "", "Linux bowl client: injected into the container for docker (default: build and cache one), run on this host for microvm (default: bowl in PATH)")
	flags.BoolVar(&rebuild, "rebuild", false, "Force rebuilding the Linux client")
	flags.BoolVarP(&quiet, "quiet", "q", false, "Quiet mode, output only the sandbox ID")
	flags.IntVar(&cpus, "cpus", 0, "CPU limit (default: 1 for microvm, unlimited for docker)")
	flags.IntVar(&memoryMiB, "memory", 0, "Memory limit in MiB (default: 1024 for microvm, unlimited for docker)")
	flags.DurationVar(&timeout, "timeout", 0, "Remove the sandbox and its disk after this long, e.g. 30m; renew with bowl-sandbox timeout")
	addSandboxStorageFlags(cmd, &endpoint, &token)
	return cmd
}

func addSandboxStorageFlags(cmd *cobra.Command, endpoint, token *string) {
	cmd.Flags().StringVar(endpoint, "endpoint", "", "StorageService base URL, defaults to $BOWL_STORAGE_ENDPOINT or http://127.0.0.1:9000")
	cmd.Flags().StringVar(token, "token", "", "Access token, defaults to $BOWL_TOKEN")
}

func sboxStartMicroVM(spec *sandbox.CreateSpec, binary string) error {
	if binary == "" {
		var err error
		if binary, err = exec.LookPath("bowl"); err != nil {
			return errors.New("bowl client not found in PATH; pass --binary")
		}
	}
	spec.ClientBinary = binary
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return sboxRuntime.Create(ctx, spec)
}

// sboxStart prepares a Linux client matching the local Docker architecture and starts the sandbox.
func sboxStart(spec *sandbox.CreateSpec, binary string, rebuild bool, stderr io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	docker, ok := sboxRuntime.(*sandbox.Runtime)
	if !ok {
		return errors.New("internal error: docker start called for another runtime")
	}
	arch, err := docker.Arch(ctx)
	if err != nil {
		return err
	}
	bin, err := sandbox.EnsureClientBinary(binary, arch, rebuild, stderr)
	if err != nil {
		return err
	}
	spec.Arch, spec.ClientBinary = arch, bin
	return sboxRuntime.Create(ctx, spec)
}

// sboxCreateDisk forks or creates the sandbox disk and returns its ID.
func sboxCreateDisk(
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	from, size, name string,
) (string, *diskCLIError) {
	if err := validateDiskName(name); err != nil {
		return "", &diskCLIError{exitCode: 2, msg: err.Error(), cause: err}
	}
	if from != "" {
		resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{SourceDiskId: from, Name: name}))
		if err != nil {
			return "", mapDiskRPCError(err, diskRPCErrorPolicy{diskID: from, defaultOp: "fork sandbox disk failed"})
		}
		return resp.Msg.GetDisk().GetDiskId(), nil
	}
	maxBytes, err := ParseSize(size)
	if err != nil {
		return "", &diskCLIError{exitCode: 2, msg: fmt.Sprintf("invalid --size: %v", err), cause: err}
	}
	resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: name, MaxBytes: maxBytes}))
	if err != nil {
		return "", mapDiskRPCError(err, diskRPCErrorPolicy{defaultOp: "create sandbox disk failed"})
	}
	return resp.Msg.GetDisk().GetDiskId(), nil
}

func newSandboxListCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List sandboxes on this machine for the selected runtime",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			// The list is rebuilt from container labels; there is no local state file.
			rows, err := sboxRuntime.List(ctx)
			if err != nil {
				return runtimeExitErr(err)
			}
			out := cmd.OutOrStdout()
			if len(rows) == 0 {
				fmt.Fprintln(out, "No sandboxes")
				return nil
			}
			// NODE is the storage node holding the sandbox disk.
			const format = "%-38s %-24s %-22s %-12s %-26s %s\n"
			fmt.Fprintf(out, format, "SANDBOX ID", "STATUS", "IMAGE", "MOUNT", "NODE", "CREATED")
			for _, r := range rows {
				fmt.Fprintf(out, format, r.ID, r.Status, r.Image, r.Mount, cmp.Or(r.Endpoint, "-"), r.Created)
			}
			return nil
		},
	}
}

func newSandboxInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info <sandbox-id>",
		Short: "Show a sandbox's status, image, and mounts",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			info, err := sboxRuntime.Info(context.Background(), sandbox.StripID(posArgs[0]))
			if err != nil {
				return runtimeExitErr(err)
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Sandbox:    %s\n", info.ID)
			fmt.Fprintf(out, "Status:     %s\n", info.Status)
			fmt.Fprintf(out, "Image:      %s\n", info.Image)
			fmt.Fprintf(out, "Created:    %s\n", info.Created)
			fmt.Fprintf(out, "Container:  %s\n", info.Container)
			if info.Endpoint != "" {
				fmt.Fprintf(out, "Node:       %s\n", info.Endpoint)
			}
			if info.Address != "" {
				fmt.Fprintf(out, "Address:    %s\n", info.Address)
			}
			if deadline, ok := sboxDeadline(info.ID); ok {
				fmt.Fprintf(out, "Expires:    %s (in %s)\n", deadline.Format(time.RFC3339), time.Until(deadline).Round(time.Second))
			}
			if len(info.Mounts) == 0 {
				fmt.Fprintln(out, "Mount:      (none)")
			}
			for _, m := range info.Mounts {
				mode := "rw"
				if m.ReadOnly {
					mode = "ro"
				}
				fmt.Fprintf(out, "Mount:      %s -> %s (%s)\n", m.DiskID, m.Path, mode)
			}
			return nil
		},
	}
}

// exitCodeErr passes a sandboxed command's exit code through as bowl's own.
func exitCodeErr(code int) error {
	if code == 0 {
		return nil
	}
	return &cliExitError{code: code, silent: true}
}

func newSandboxExecCmd() *cobra.Command {
	var (
		background bool
		cwd        string
	)
	cmd := &cobra.Command{
		Use:   "exec [flags] <sandbox-id> [--] <command> [args...]",
		Short: "Run a command in a sandbox and exit with its exit code (-b runs it in the background)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id, argv := sandbox.StripID(posArgs[0]), posArgs[1:]
			if len(argv) > 0 && argv[0] == "--" {
				argv = argv[1:]
			}
			if len(argv) == 0 {
				return usageExitErr(errors.New("missing command to run"), true)
			}
			ctx := context.Background()
			workdir := cwd
			if workdir == "" {
				mountAt, err := sboxRuntime.DefaultMount(ctx, id)
				if err != nil {
					return runtimeExitErr(err)
				}
				workdir = mountAt
			}
			if background {
				runID, err := sboxRuntime.ExecBackground(ctx, id, workdir, argv)
				if err != nil {
					return runtimeExitErr(err)
				}
				fmt.Fprintln(cmd.OutOrStdout(), runID)
				return nil
			}
			code, err := sboxRuntime.Exec(ctx, id, workdir, argv,
				sandbox.ExecOptions{TTY: term.IsTerminal(int(os.Stdin.Fd()))}, os.Stdin, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return runtimeExitErr(err)
			}
			return exitCodeErr(code)
		},
	}
	// Everything after the sandbox ID belongs to the command being run, so bowl's own flags go before it.
	cmd.Flags().SetInterspersed(false)
	cmd.Flags().BoolVarP(&background, "background", "b", false, "Run in the background and print the run ID")
	cmd.Flags().StringVar(&cwd, "cwd", "", "Working directory (default: the disk mountpoint)")
	return cmd
}

func newSandboxLogsCmd() *cobra.Command {
	var (
		follow bool
		runID  string
	)
	cmd := &cobra.Command{
		Use:     "logs <sandbox-id>",
		Aliases: []string{"log"},
		Short:   "Show or follow the output of a background run",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			err := sboxRuntime.Logs(context.Background(), sandbox.StripID(posArgs[0]), runID, follow, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return runtimeExitErr(err)
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow the output")
	cmd.Flags().StringVar(&runID, "exec", "", "Run ID (default: the most recent background run)")
	return cmd
}

func newSandboxWaitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "wait <sandbox-id> <run-id>",
		Short: "Wait for a background run to finish and exit with its exit code",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			code, err := sboxRuntime.Wait(context.Background(), sandbox.StripID(posArgs[0]), posArgs[1])
			if err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), code)
			return exitCodeErr(code)
		},
	}
}

func newSandboxShellCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "shell <sandbox-id>",
		Short: "Open an interactive shell in a sandbox (bash if present, else sh)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id := sandbox.StripID(posArgs[0])
			ctx := context.Background()
			mountAt, err := sboxRuntime.DefaultMount(ctx, id)
			if err != nil {
				return runtimeExitErr(err)
			}
			shell := "sh"
			var probe bytes.Buffer
			if code, probeErr := sboxRuntime.Exec(ctx, id, mountAt, []string{"sh", "-c", "command -v bash"},
				sandbox.ExecOptions{}, nil, &probe, io.Discard); probeErr == nil && code == 0 {
				shell = "bash"
			}
			code, err := sboxRuntime.Exec(ctx, id, mountAt, []string{shell},
				sandbox.ExecOptions{TTY: true}, os.Stdin, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return runtimeExitErr(err)
			}
			return exitCodeErr(code)
		},
	}
}

func newSandboxCpCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "cp <source> <destination>",
		Aliases: []string{"copy"},
		Short:   "Copy files or directories between this machine and a sandbox (<sandbox-id>:<path>)",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			srcID, srcPath, srcInSandbox := sboxSplitCpSpec(posArgs[0])
			dstID, dstPath, dstInSandbox := sboxSplitCpSpec(posArgs[1])
			if srcInSandbox == dstInSandbox {
				return usageExitErr(errors.New("exactly one side must be <sandbox-id>:<path>, e.g. <sandbox-id>:/mnt/data/note.txt"), false)
			}
			ctx := context.Background()
			var err error
			if srcInSandbox {
				err = sboxRuntime.CopyFrom(ctx, srcID, srcPath, dstPath, cmd.ErrOrStderr())
			} else {
				err = sboxRuntime.CopyInto(ctx, dstID, srcPath, dstPath, cmd.ErrOrStderr())
			}
			if err != nil {
				return runtimeExitErr(err)
			}
			return nil
		},
	}
}

// sboxSplitCpSpec parses `<sandbox-id>:<path>`; a local path comes back unchanged as path.
func sboxSplitCpSpec(spec string) (id, path string, inSandbox bool) {
	left, right, ok := strings.Cut(spec, ":")
	if !ok || left == "" || strings.Contains(left, "/") {
		return "", spec, false
	}
	return sandbox.StripID(left), right, true
}

func newSandboxKillCmd() *cobra.Command {
	var (
		keepDisk, all   bool
		endpoint, token string
	)
	cmd := &cobra.Command{
		Use:     "kill <sandbox-id>",
		Aliases: []string{"rm", "delete"},
		Short:   "Remove a sandbox's container and, unless --keep-disk, its disk",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			if all == (len(posArgs) == 1) {
				return usageExitErr(errors.New("pass either a sandbox ID or --all"), true)
			}
			ctx := context.Background()
			var ids []string
			if all {
				var err error
				if ids, err = sboxRuntime.IDs(ctx); err != nil {
					return runtimeExitErr(err)
				}
				if len(ids) == 0 {
					fmt.Fprintln(cmd.OutOrStdout(), "No sandboxes")
					return nil
				}
			} else {
				ids = []string{sandbox.StripID(posArgs[0])}
			}
			failed := false
			for _, id := range ids {
				if err := sboxKillOne(ctx, id, keepDisk, endpoint, token, cmd.OutOrStdout()); err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
					failed = true
				}
			}
			if failed {
				return &cliExitError{code: 1, silent: true}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&keepDisk, "keep-disk", false, "Remove only the container and keep the disk")
	cmd.Flags().BoolVarP(&all, "all", "a", false, "Remove every sandbox on this machine")
	addSandboxStorageFlags(cmd, &endpoint, &token)
	return cmd
}

func sboxKillOne(ctx context.Context, id string, keepDisk bool, endpoint, token string, stdout io.Writer) error {
	if endpoint == "" {
		// The disk must be deleted on the storage node the sandbox was created against.
		if info, err := sboxRuntime.Info(ctx, id); err == nil {
			endpoint = info.Endpoint
		}
	}
	if err := sboxRuntime.Remove(ctx, id); err != nil {
		return err
	}
	sboxClearDeadline(id)
	if keepDisk {
		fmt.Fprintf(stdout, "Removed the container of sandbox %s; disk kept\n", id)
		return nil
	}
	if err := sboxDeleteDisk(endpoint, token, id); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Removed sandbox %s\n", id)
	return nil
}

// sboxDeleteDisk deletes the disk, retrying while the mount lease of the just-removed container
// has not expired yet (EBUSY).
func sboxDeleteDisk(endpoint, token, diskID string) error {
	client, _, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		return err
	}
	for range 6 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		cancel()
		if err == nil {
			return nil
		}
		connErr, ok := errors.AsType[*connect.Error](err)
		if !ok || (!strings.Contains(connErr.Message(), "EBUSY") && !strings.Contains(connErr.Message(), "still mounted")) {
			return fmt.Errorf("delete disk failed: %w", err)
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("disk %s is still held by a mount session; retry later with: bowl disk delete %s", diskID, diskID)
}
