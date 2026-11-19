package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/sandbox"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// sboxFork pauses the source, syncs it, forks its disk and resumes it, so the fork is a consistent
// point in time; then it starts a new sandbox on the fork. forkTook covers pause through resume.
func sboxFork(src, name, binary, token string, log io.Writer) (newID string, forkTook time.Duration, err error) {
	ctx := context.Background()
	info, err := sboxRuntime.Info(ctx, src)
	if err != nil {
		return "", 0, runtimeExitErr(err)
	}
	client, code, err := newDiskStorageClient(info.Endpoint, token)
	if err != nil {
		return "", 0, &cliExitError{code: code, err: err}
	}
	start := time.Now()
	wasRunning := info.Status == "running"
	// The docker runtime's bowl client runs inside the container, so it cannot flush once the
	// container is paused: flush first. Writes made between this flush and the pause can miss
	// the fork. The other runtimes flush after the pause and get an exact point in time.
	flushFirst := sboxRuntimeName == runtimeDocker
	if flushFirst {
		if err = sboxRuntime.Sync(ctx, src); err != nil {
			return "", 0, runtimeExitErr(fmt.Errorf("sync source: %w", err))
		}
	}
	if wasRunning {
		if err = sboxRuntime.Pause(ctx, src); err != nil {
			return "", 0, runtimeExitErr(fmt.Errorf("pause source: %w", err))
		}
	}
	if !flushFirst {
		err = sboxRuntime.Sync(ctx, src)
	}
	var resp *connect.Response[storagev1.ForkDiskResponse]
	if err == nil {
		resp, err = client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{SourceDiskId: src, Name: name}))
	}
	if wasRunning {
		if resumeErr := sboxRuntime.Resume(ctx, src); resumeErr != nil {
			err = errors.Join(err, fmt.Errorf("resume source: %w", resumeErr))
		}
	}
	if err != nil {
		return "", 0, runtimeExitErr(fmt.Errorf("fork sandbox disk: %w", err))
	}
	forkTook = time.Since(start)
	newID = resp.Msg.GetDisk().GetDiskId()

	spec := &sandbox.CreateSpec{
		ID: newID, Image: info.Image, Name: name, MountPath: info.MountPath, Network: info.Network,
		HostEndpoint: info.Endpoint, Token: rpc.ResolveToken(token),
		CPUs: info.CPUs, MemoryMiB: info.MemoryMiB, // a fork keeps the source's size
	}
	if sboxRuntimeName == runtimeMicroVM {
		err = sboxStartMicroVM(spec, binary)
	} else {
		err = sboxStart(spec, binary, false, log)
	}
	if err != nil {
		_ = sboxDeleteDisk(info.Endpoint, token, newID)
		return "", 0, runtimeExitErr(err)
	}
	return newID, forkTook, nil
}

func newSandboxForkCmd() *cobra.Command {
	var name, binary, token string
	var quiet bool
	cmd := &cobra.Command{
		Use:   "fork <sandbox-id>",
		Short: "Fork a sandbox's disk and start a new sandbox on the copy; the original keeps running",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			start := time.Now()
			src := sandbox.StripID(posArgs[0])
			newID, forkTook, err := sboxFork(src, name, binary, token, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if quiet {
				fmt.Fprintln(out, newID)
				return nil
			}
			fmt.Fprintf(out, "Sandbox %s forked from %s\n", newID, src)
			fmt.Fprintf(out, "  Disk fork:  %s (source paused meanwhile)\n", forkTook.Round(time.Millisecond))
			fmt.Fprintf(out, "  Ready:      %s after the fork started\n", time.Since(start).Round(time.Millisecond))
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Name for the forked disk")
	cmd.Flags().StringVar(&binary, "binary", "", "Linux bowl client, as for create")
	cmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Quiet mode, output only the new sandbox ID")
	cmd.Flags().StringVar(&token, "token", "", "Access token, defaults to $BOWL_TOKEN")
	return cmd
}

// runDiskSubcommand runs a bowl disk subcommand in-process so sandbox diff and merge print and
// exit exactly like bowl disk diff and merge.
func runDiskSubcommand(stdout, stderr io.Writer, args ...string) error {
	diskCmd := newDiskCmd(defaultDiskPorts())
	diskCmd.SetOut(stdout)
	diskCmd.SetErr(stderr)
	diskCmd.SetArgs(args)
	diskCmd.SilenceErrors, diskCmd.SilenceUsage = true, true
	_, err := diskCmd.ExecuteC()
	return err
}

func newSandboxDiffCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "diff <sandbox-id> <other-sandbox-id> [bowl disk diff flags]",
		Short: "List files that differ between two sandboxes (flags as in bowl disk diff, e.g. --patch, --prefix)",
		// The rest of the flags belong to bowl disk diff.
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if slices.Contains(args, "-h") || slices.Contains(args, "--help") {
				return cmd.Help()
			}
			if len(args) < 2 {
				return usageExitErr(errors.New("need two sandbox IDs"), true)
			}
			a, b := sandbox.StripID(args[0]), sandbox.StripID(args[1])
			for _, id := range []string{a, b} {
				if err := sboxSyncIfRunning(id); err != nil {
					return runtimeExitErr(err)
				}
			}
			return runDiskSubcommand(cmd.OutOrStdout(), cmd.ErrOrStderr(), append([]string{"diff", a, "--target", b}, args[2:]...)...)
		},
	}
}

func newSandboxMergeCmd() *cobra.Command {
	return &cobra.Command{
		Use: "merge <source-sandbox-id> --into <target-sandbox-id> [bowl disk merge flags]",
		Short: "Merge a sandbox's changes into another; both are stopped during the merge and started again " +
			"(processes do not survive). Flags as in bowl disk merge, e.g. --dry-run, --strategy",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if slices.Contains(args, "-h") || slices.Contains(args, "--help") {
				return cmd.Help()
			}
			i := slices.Index(args, "--into")
			if len(args) < 3 || i < 1 || i+1 >= len(args) {
				return usageExitErr(errors.New("usage: merge <source-sandbox-id> --into <target-sandbox-id>"), true)
			}
			src, dst := sandbox.StripID(args[0]), sandbox.StripID(args[i+1])
			rest := append(slices.Clone(args[1:i]), args[i+2:]...)
			ctx := context.Background()
			for _, id := range []string{src, dst} {
				if info, err := sboxRuntime.Info(ctx, id); err == nil && info.Block {
					return usageExitErr(fmt.Errorf("sandbox %s boots from a block-device image; merge works on files, "+
						"and its disk holds a single image file", id), false)
				}
			}

			// bowl disk merge refuses a mounted target and a read-write mounted source.
			var stopped []string
			restart := func() error {
				var errs []error
				for _, id := range stopped {
					if err := sboxRuntime.Start(ctx, id); err != nil {
						errs = append(errs, fmt.Errorf("start %s again: %w", id, err))
					}
				}
				return errors.Join(errs...)
			}
			for _, id := range []string{src, dst} {
				info, err := sboxRuntime.Info(ctx, id)
				if err != nil {
					return runtimeExitErr(errors.Join(err, restart()))
				}
				if info.Status == "stopped" {
					continue
				}
				if err = sboxRuntime.Stop(ctx, id); err != nil {
					return runtimeExitErr(errors.Join(fmt.Errorf("stop %s: %w", id, err), restart()))
				}
				stopped = append(stopped, id)
			}
			mergeErr := runDiskSubcommand(cmd.OutOrStdout(), cmd.ErrOrStderr(), append([]string{"merge", src, "--into", dst}, rest...)...)
			if err := restart(); err != nil {
				return runtimeExitErr(errors.Join(mergeErr, err))
			}
			return mergeErr
		},
	}
}

func sboxSyncIfRunning(id string) error {
	ctx := context.Background()
	info, err := sboxRuntime.Info(ctx, id)
	if err != nil {
		return err
	}
	if info.Status == "stopped" {
		return nil
	}
	return sboxRuntime.Sync(ctx, id)
}

func newSandboxStopCmd(stop bool) *cobra.Command {
	use, short, done := "start <sandbox-id>", "Start a stopped sandbox again from its disk (cold boot)", "Started"
	if stop {
		use, short, done = "stop <sandbox-id>", "Stop a sandbox and release its CPU and memory; the disk and the record stay", "Stopped"
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id := sandbox.StripID(posArgs[0])
			op := sboxRuntime.Start
			if stop {
				op = sboxRuntime.Stop
			}
			if err := op(context.Background(), id); err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s sandbox %s\n", done, id)
			return nil
		},
	}
}
