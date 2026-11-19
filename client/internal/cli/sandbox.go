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
	runtimeMicroVM   = "microvm"
	runtimeContainer = "container"
	runtimeDocker    = "docker"
)

// sboxBootsFromDisk reports whether the selected runtime's root filesystem is the disk, so a
// sandbox needs a template to start from.
func sboxBootsFromDisk() bool { return sboxRuntimeName != runtimeDocker }

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
	case runtimeContainer:
		return &sandbox.Runtime{RootFS: true}, name, nil
	case runtimeDocker:
		return &sandbox.Runtime{}, name, nil
	default:
		return nil, "", fmt.Errorf("unknown --runtime %q: use %s, %s or %s", name, runtimeMicroVM, runtimeContainer, runtimeDocker)
	}
}

func newSandboxCmd() *cobra.Command {
	var runtimeFlag, stateDir string
	cmd := &cobra.Command{
		Use:   "bowl-sandbox",
		Short: "Run sandboxes whose disk is a Bowl virtual disk",
		Long: "Run sandboxes whose disk is a Bowl virtual disk. The sandbox ID is the disk ID.\n\n" +
			"Runtimes:\n" +
			"  microvm    Cloud Hypervisor VM; the disk is the VM's root filesystem. Linux with /dev/kvm, run as root.\n" +
			"  container  Docker container; the disk is the root filesystem, commands run chrooted into it.\n" +
			"             Uses the same templates as microvm. Works wherever Docker does, including macOS.\n" +
			"  docker     Docker container with the disk mounted at --mount as a data disk.\n" +
			"microvm and container need --template or --from. For the Docker-based runtimes --endpoint must be\n" +
			"reachable from the container; loopback addresses are rewritten to host.docker.internal.",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		// Parse --runtime and --state-dir before the subcommand name too, including for diff and
		// merge, which hand their own flags to bowl disk unparsed.
		TraverseChildren: true,
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
		newSandboxForkCmd(),
		newSandboxDiffCmd(),
		newSandboxMergeCmd(),
		newSandboxStopCmd(true),
		newSandboxStopCmd(false),
		newSandboxServeCmd(),
		newSandboxMoveCmd(),
	)
	cmd.PersistentFlags().StringVar(&runtimeFlag, "runtime", "", "Sandbox runtime: microvm, container or docker (default: $BOWL_SANDBOX_RUNTIME, else microvm when /dev/kvm exists, else docker)")
	cmd.PersistentFlags().StringVar(&stateDir, "state-dir", "", "MicroVM state directory holding vmlinux and bowl-sandbox-guest (default /var/lib/bowl-sandbox)")
	return cmd
}

// sboxCreateOpts is everything create needs; the CLI command and the HTTP API both fill it.
type sboxCreateOpts struct {
	From, Template, Size, Image, Name, MountPath, Network, Binary, Endpoint, Token string
	Rebuild                                                                        bool
	CPUs, MemoryMiB                                                                int
	Timeout                                                                        time.Duration
	Log                                                                            io.Writer
}

// sboxCreate creates or forks the sandbox disk, starts the sandbox on it and returns its ID.
// Errors carry the CLI exit code (usage 2, runtime 1).
func sboxCreate(o *sboxCreateOpts) (string, error) {
	if o.From != "" && o.Template != "" {
		return "", usageExitErr(errors.New("--from and --template cannot be used together"), false)
	}
	if sboxBootsFromDisk() && o.From == "" && o.Template == "" {
		return "", usageExitErr(fmt.Errorf("the %s runtime boots from the disk, so --template <name> or --from <disk> is required", sboxRuntimeName), false)
	}
	if sboxRuntimeName != runtimeMicroVM {
		if _, err := exec.LookPath("docker"); err != nil {
			return "", runtimeExitErr(errors.New("docker command not found; the docker runtime needs a local Docker"))
		}
	}
	resolvedEndpoint, err := rpc.ResolveEndpoint(o.Endpoint)
	if err != nil {
		return "", usageExitErr(fmt.Errorf("invalid endpoint: %w", err), false)
	}
	client, code, err := newDiskStorageClient(resolvedEndpoint, o.Token)
	if err != nil {
		return "", &cliExitError{code: code, err: err}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	from := o.From
	if o.Template != "" {
		if from, err = findTemplate(ctx, client, o.Template); err != nil {
			cancel()
			return "", runtimeExitErr(err)
		}
	}
	diskID, cliErr := sboxCreateDisk(ctx, client, from, cmp.Or(o.Size, "1GiB"), o.Name)
	cancel()
	if cliErr != nil {
		return "", &cliExitError{code: cliErr.exitCode, err: errors.New(cliErr.msg)}
	}

	spec := &sandbox.CreateSpec{
		ID: diskID, Image: cmp.Or(o.Image, sandbox.DefaultImage), Name: o.Name, MountPath: o.MountPath,
		Network: o.Network, HostEndpoint: resolvedEndpoint, Token: rpc.ResolveToken(o.Token),
		CPUs: o.CPUs, MemoryMiB: o.MemoryMiB,
	}
	if sboxBootsFromDisk() {
		spec.Image = cmp.Or(o.Template, from)
	}
	if sboxRuntimeName == runtimeMicroVM {
		// The disk is mounted on this host, so the host's own Linux bowl client does it.
		err = sboxStartMicroVM(spec, o.Binary)
	} else {
		err = sboxStart(spec, o.Binary, o.Rebuild, cmp.Or[io.Writer](o.Log, io.Discard))
	}
	if err != nil {
		// Best effort: the caller is already getting the start error.
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, _ = client.DeleteDisk(dropCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		dropCancel()
		return "", runtimeExitErr(err)
	}
	if o.Timeout > 0 {
		if err = sboxSetDeadline(diskID, o.Timeout); err != nil {
			return diskID, runtimeExitErr(fmt.Errorf("sandbox %s is up but the timeout was not set: %w", diskID, err))
		}
	}
	return diskID, nil
}

func newSandboxCreateCmd() *cobra.Command {
	var (
		o     sboxCreateOpts
		quiet bool
	)
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a sandbox, forking its disk from --from or creating an empty one",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			o.Log = cmd.ErrOrStderr()
			diskID, err := sboxCreate(&o)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if quiet {
				fmt.Fprintln(out, diskID)
				return nil
			}
			fmt.Fprintf(out, "Sandbox %s is ready (%s)\n", diskID, sboxRuntimeName)
			if sboxBootsFromDisk() {
				fmt.Fprintf(out, "  Template:  %s\n", cmp.Or(o.Template, o.From))
				fmt.Fprintf(out, "  Disk:      %s (the root filesystem)\n", diskID)
			} else {
				fmt.Fprintf(out, "  Image:     %s\n", o.Image)
				fmt.Fprintf(out, "  Disk:      %s (mounted at %s)\n", diskID, o.MountPath)
			}
			fmt.Fprintf(out, "  Run:       bowl-sandbox exec %s -- <command>\n", diskID)
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&o.From, "from", "", "Fork the sandbox disk from an existing disk (the source is untouched)")
	flags.StringVar(&o.Template, "template", "", "Fork the sandbox disk from this template (see bowl-sandbox template list)")
	flags.StringVar(&o.Size, "size", "1GiB", "Capacity of the new disk; ignored with --from")
	flags.StringVar(&o.Image, "image", sandbox.DefaultImage, "Container image (docker only)")
	flags.StringVar(&o.Name, "name", "", "Disk name and container hostname")
	flags.StringVar(&o.MountPath, "mount", sandbox.DefaultMountPath, "Disk mountpoint inside the container (docker only)")
	flags.StringVar(&o.Network, "network", "", "Attach the sandbox to this docker network (docker only)")
	flags.StringVar(&o.Binary, "binary", "", "Linux bowl client: injected into the container for docker (default: build and cache one), run on this host for microvm (default: bowl in PATH)")
	flags.BoolVar(&o.Rebuild, "rebuild", false, "Force rebuilding the Linux client")
	flags.BoolVarP(&quiet, "quiet", "q", false, "Quiet mode, output only the sandbox ID")
	flags.IntVar(&o.CPUs, "cpus", 0, "CPU limit (default: 1 for microvm, unlimited for docker)")
	flags.IntVar(&o.MemoryMiB, "memory", 0, "Memory limit in MiB (default: 1024 for microvm, unlimited for docker)")
	flags.DurationVar(&o.Timeout, "timeout", 0, "Remove the sandbox and its disk after this long, e.g. 30m; renew with bowl-sandbox timeout")
	addSandboxStorageFlags(cmd, &o.Endpoint, &o.Token)
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
	docker, ok := sboxRuntime.(*sandbox.Runtime)
	if !ok {
		return errors.New("internal error: docker start called for another runtime")
	}
	return sboxStartOn(docker, spec, binary, rebuild, stderr)
}

func sboxStartOn(docker *sandbox.Runtime, spec *sandbox.CreateSpec, binary string, rebuild bool, stderr io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	arch, err := docker.Arch(ctx)
	if err != nil {
		return err
	}
	bin, err := sandbox.EnsureClientBinary(binary, arch, rebuild, stderr)
	if err != nil {
		return err
	}
	spec.Arch, spec.ClientBinary = arch, bin
	return docker.Create(ctx, spec)
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
