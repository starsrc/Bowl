package sandboxcli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/starsrc/bowl/client/internal/sandbox"
	"github.com/starsrc/bowl/client/internal/version"
)

var (
	vm           = &sandbox.MicroVM{}
	sboxStateDir string
)

type exitError struct {
	code      int
	err       error
	silent    bool
	showUsage bool
}

func (e *exitError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}

func (e *exitError) Unwrap() error { return e.err }

func usageExitErr(err error, showUsage bool) error {
	return &exitError{code: 2, err: err, showUsage: showUsage}
}

func runtimeExitErr(err error) error {
	return &exitError{code: 1, err: err}
}

func exitCodeErr(code int) error {
	if code == 0 {
		return nil
	}
	return &exitError{code: code, silent: true}
}

func Execute(args []string, stdout, stderr io.Writer) int {
	rootCmd := newRootCmd()
	rootCmd.SetOut(stdout)
	rootCmd.SetErr(stderr)
	if args == nil {
		args = []string{}
	}
	rootCmd.SetArgs(args)
	executedCmd, err := rootCmd.ExecuteC()
	if err == nil {
		return 0
	}
	if exitErr, ok := errors.AsType[*exitError](err); ok {
		if !exitErr.silent && exitErr.err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", exitErr.err)
		}
		if exitErr.showUsage && executedCmd != nil {
			fmt.Fprintf(stderr, "\n%s", executedCmd.UsageString())
		}
		return exitErr.code
	}
	fmt.Fprintf(stderr, "Error: %v\n", err)
	if executedCmd != nil {
		fmt.Fprintf(stderr, "\n%s", executedCmd.UsageString())
	}
	return 2
}

func requireNoArgs(cmd *cobra.Command, posArgs []string) error {
	if len(posArgs) > 0 {
		return fmt.Errorf("unknown command %q for %q", posArgs[0], cmd.CommandPath())
	}
	return nil
}

func printUsage(cmd *cobra.Command, _ []string) error {
	fmt.Fprint(cmd.ErrOrStderr(), cmd.UsageString())
	return &exitError{code: 2, silent: true}
}

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bowl-sandbox",
		Short: "Run sandboxes as Cloud Hypervisor MicroVMs on this host",
		Long: "Run sandboxes as Cloud Hypervisor MicroVMs on this host. Each sandbox boots from its own copy of a\n" +
			"template's ext4 image. Linux with /dev/kvm, run as root.",
		Version:          version.Version,
		SilenceUsage:     true,
		SilenceErrors:    true,
		TraverseChildren: true,
		PersistentPreRun: func(*cobra.Command, []string) {
			vm = &sandbox.MicroVM{StateDir: sboxStateDir}
		},
		Args: requireNoArgs,
		RunE: printUsage,
	}
	cmd.AddCommand(
		newCreateCmd(),
		newListCmd(),
		newInfoCmd(),
		newExecCmd(),
		newLogsCmd(),
		newWaitCmd(),
		newShellCmd(),
		newCpCmd(),
		newKillCmd(),
		newPauseCmd(true),
		newPauseCmd(false),
		newStopCmd(true),
		newStopCmd(false),
		newTimeoutCmd(),
		newReapCmd(),
		newTemplateCmd(),
		newServeCmd(),
	)
	cmd.PersistentFlags().StringVar(&sboxStateDir, "state-dir", "",
		"State directory holding vmlinux, bowl-sandbox-guest, templates and sandboxes (default /var/lib/bowl-sandbox). "+
			"Put it on a reflink filesystem (XFS, btrfs), or every create writes a full copy of the template image")
	return cmd
}

type createOpts struct {
	Template        string
	CPUs, MemoryMiB int
	Timeout         time.Duration
}

func sboxCreate(o *createOpts) (string, error) {
	if o.Template == "" {
		return "", usageExitErr(errors.New("--template <name> is required (see bowl-sandbox template list)"), false)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", runtimeExitErr(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	spec := &sandbox.CreateSpec{ID: id.String(), Template: o.Template, CPUs: o.CPUs, MemoryMiB: o.MemoryMiB}
	if err = vm.Create(ctx, spec); err != nil {
		return "", runtimeExitErr(err)
	}
	if o.Timeout > 0 {
		if err = sboxSetDeadline(spec.ID, o.Timeout); err != nil {
			return spec.ID, runtimeExitErr(fmt.Errorf("sandbox %s is up but the timeout was not set: %w", spec.ID, err))
		}
	}
	return spec.ID, nil
}

func newCreateCmd() *cobra.Command {
	var (
		o     createOpts
		quiet bool
	)
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a sandbox from a template",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			id, err := sboxCreate(&o)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if quiet {
				fmt.Fprintln(out, id)
				return nil
			}
			fmt.Fprintf(out, "Sandbox %s is ready\n", id)
			fmt.Fprintf(out, "  Template:  %s\n", o.Template)
			fmt.Fprintf(out, "  Run:       bowl-sandbox exec %s -- <command>\n", id)
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&o.Template, "template", "", "Template to boot from (see bowl-sandbox template list)")
	flags.BoolVarP(&quiet, "quiet", "q", false, "Quiet mode, output only the sandbox ID")
	flags.IntVar(&o.CPUs, "cpus", 0, "Number of vCPUs (default 1)")
	flags.IntVar(&o.MemoryMiB, "memory", 0, "Memory in MiB (default 1024)")
	flags.DurationVar(&o.Timeout, "timeout", 0, "Remove the sandbox after this long, e.g. 30m; renew with bowl-sandbox timeout")
	return cmd
}

func newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List sandboxes on this machine",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			rows, err := vm.List()
			if err != nil {
				return runtimeExitErr(err)
			}
			out := cmd.OutOrStdout()
			if len(rows) == 0 {
				fmt.Fprintln(out, "No sandboxes")
				return nil
			}
			const format = "%-38s %-10s %-24s %-16s %s\n"
			fmt.Fprintf(out, format, "SANDBOX ID", "STATUS", "TEMPLATE", "ADDRESS", "CREATED")
			for _, r := range rows {
				fmt.Fprintf(out, format, r.ID, r.Status, r.Template, r.Address, r.Created)
			}
			return nil
		},
	}
}

func newInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info <sandbox-id>",
		Short: "Show a sandbox's status, template, address and size",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			info, err := vm.Info(posArgs[0])
			if err != nil {
				return runtimeExitErr(err)
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Sandbox:    %s\n", info.ID)
			fmt.Fprintf(out, "Status:     %s\n", info.Status)
			fmt.Fprintf(out, "Template:   %s\n", info.Template)
			fmt.Fprintf(out, "Created:    %s\n", info.Created)
			if info.Status != "stopped" {
				fmt.Fprintf(out, "PID:        %d\n", info.PID)
			}
			fmt.Fprintf(out, "Address:    %s\n", info.Address)
			fmt.Fprintf(out, "Size:       %d vCPU, %d MiB\n", info.CPUs, info.MemoryMiB)
			if deadline, ok := sboxDeadline(info.ID); ok {
				fmt.Fprintf(out, "Expires:    %s (in %s)\n", deadline.Format(time.RFC3339), time.Until(deadline).Round(time.Second))
			}
			return nil
		},
	}
}

func newExecCmd() *cobra.Command {
	var (
		background bool
		cwd        string
	)
	cmd := &cobra.Command{
		Use:   "exec [flags] <sandbox-id> [--] <command> [args...]",
		Short: "Run a command in a sandbox and exit with its exit code (-b runs it in the background)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id, argv := posArgs[0], posArgs[1:]
			if len(argv) > 0 && argv[0] == "--" {
				argv = argv[1:]
			}
			if len(argv) == 0 {
				return usageExitErr(errors.New("missing command to run"), true)
			}
			ctx := context.Background()
			if background {
				runID, err := vm.ExecBackground(ctx, id, cwd, argv)
				if err != nil {
					return runtimeExitErr(err)
				}
				fmt.Fprintln(cmd.OutOrStdout(), runID)
				return nil
			}
			code, err := vm.Exec(ctx, id, cwd, argv,
				sandbox.ExecOptions{TTY: term.IsTerminal(int(os.Stdin.Fd()))}, os.Stdin, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return runtimeExitErr(err)
			}
			return exitCodeErr(code)
		},
	}
	cmd.Flags().SetInterspersed(false)
	cmd.Flags().BoolVarP(&background, "background", "b", false, "Run in the background and print the run ID")
	cmd.Flags().StringVar(&cwd, "cwd", "", "Working directory (default /root)")
	return cmd
}

func newLogsCmd() *cobra.Command {
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
			if err := vm.Logs(context.Background(), posArgs[0], runID, follow, cmd.OutOrStdout(), cmd.ErrOrStderr()); err != nil {
				return runtimeExitErr(err)
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow the output")
	cmd.Flags().StringVar(&runID, "exec", "", "Run ID (default: the most recent background run)")
	return cmd
}

func newWaitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "wait <sandbox-id> <run-id>",
		Short: "Wait for a background run to finish and exit with its exit code",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			code, err := vm.Wait(context.Background(), posArgs[0], posArgs[1])
			if err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), code)
			return exitCodeErr(code)
		},
	}
}

func newShellCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "shell <sandbox-id>",
		Short: "Open an interactive shell in a sandbox (bash if present, else sh)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id := posArgs[0]
			ctx := context.Background()
			shell := "sh"
			var probe bytes.Buffer
			if code, probeErr := vm.Exec(ctx, id, "", []string{"sh", "-c", "command -v bash"},
				sandbox.ExecOptions{}, nil, &probe, io.Discard); probeErr == nil && code == 0 {
				shell = "bash"
			}
			code, err := vm.Exec(ctx, id, "", []string{shell},
				sandbox.ExecOptions{TTY: true}, os.Stdin, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return runtimeExitErr(err)
			}
			return exitCodeErr(code)
		},
	}
}

func newCpCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "cp <source> <destination>",
		Aliases: []string{"copy"},
		Short:   "Copy files or directories between this machine and a sandbox (<sandbox-id>:<path>)",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			srcID, srcPath, srcInSandbox := splitCpSpec(posArgs[0])
			dstID, dstPath, dstInSandbox := splitCpSpec(posArgs[1])
			if srcInSandbox == dstInSandbox {
				return usageExitErr(errors.New("exactly one side must be <sandbox-id>:<path>, e.g. <sandbox-id>:/root/note.txt"), false)
			}
			ctx := context.Background()
			var err error
			if srcInSandbox {
				err = vm.CopyFrom(ctx, srcID, srcPath, dstPath, cmd.ErrOrStderr())
			} else {
				err = vm.CopyInto(ctx, dstID, srcPath, dstPath, cmd.ErrOrStderr())
			}
			if err != nil {
				return runtimeExitErr(err)
			}
			return nil
		},
	}
}

func splitCpSpec(spec string) (id, path string, inSandbox bool) {
	left, right, ok := strings.Cut(spec, ":")
	if !ok || left == "" || strings.Contains(left, "/") {
		return "", spec, false
	}
	return left, right, true
}

func newKillCmd() *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:     "kill <sandbox-id>",
		Aliases: []string{"rm", "delete"},
		Short:   "Remove a sandbox and its disk",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			if all == (len(posArgs) == 1) {
				return usageExitErr(errors.New("pass either a sandbox ID or --all"), true)
			}
			ids := posArgs
			if all {
				var err error
				if ids, err = vm.IDs(); err != nil {
					return runtimeExitErr(err)
				}
				if len(ids) == 0 {
					fmt.Fprintln(cmd.OutOrStdout(), "No sandboxes")
					return nil
				}
			}
			failed := false
			for _, id := range ids {
				if err := sboxKill(context.Background(), id); err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
					failed = true
					continue
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Removed sandbox %s\n", id)
			}
			if failed {
				return &exitError{code: 1, silent: true}
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&all, "all", "a", false, "Remove every sandbox on this machine")
	return cmd
}

func sboxKill(ctx context.Context, id string) error {
	if err := vm.Remove(ctx, id); err != nil {
		return err
	}
	sboxClearDeadline(id)
	return nil
}

func newPauseCmd(pause bool) *cobra.Command {
	use, short, done := "resume <sandbox-id>", "Resume a paused sandbox", "Resumed"
	if pause {
		use, short, done = "pause <sandbox-id>", "Freeze a sandbox's vCPUs; memory is kept", "Paused"
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			op := vm.Resume
			if pause {
				op = vm.Pause
			}
			if err := op(context.Background(), posArgs[0]); err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s sandbox %s\n", done, posArgs[0])
			return nil
		},
	}
}

func newStopCmd(stop bool) *cobra.Command {
	use, short, done := "start <sandbox-id>", "Start a stopped sandbox again from its disk (cold boot)", "Started"
	if stop {
		use, short, done = "stop <sandbox-id>", "Stop a sandbox and release its CPU and memory; the disk and the record stay", "Stopped"
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			op := vm.Start
			if stop {
				op = vm.Stop
			}
			if err := op(context.Background(), posArgs[0]); err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s sandbox %s\n", done, posArgs[0])
			return nil
		},
	}
}
