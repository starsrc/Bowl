package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/starsrc/bowl/client/internal/sandbox"
)

// A sandbox timeout is a deadline file plus a detached reaper process that removes the sandbox
// and its disk once the deadline passes. Renewing only rewrites the file; the reaper rereads it.
//
// ponytail: deadlines live on this machine and the reaper is a plain process, so a host reboot
// forgets them. A control-plane service should own this once there is one.

var sboxReapPoll = 5 * time.Second

func sboxDeadlinePath(id string) string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "bowl-sandbox", "deadlines", sandbox.StripID(id))
}

func sboxDeadline(id string) (time.Time, bool) {
	b, err := os.ReadFile(sboxDeadlinePath(id))
	if err != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(string(b)))
	return t, err == nil
}

func sboxClearDeadline(id string) { _ = os.Remove(sboxDeadlinePath(id)) }

// sboxSetDeadline sets the deadline to now+d and starts a reaper unless one is already watching.
func sboxSetDeadline(id string, d time.Duration) error {
	p := sboxDeadlinePath(id)
	_, hadDeadline := sboxDeadline(id)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(p, []byte(time.Now().Add(d).UTC().Format(time.RFC3339)+"\n"), 0o600); err != nil {
		return err
	}
	if hadDeadline {
		return nil
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	args := []string{"--runtime", sboxRuntimeName}
	if sboxStateDir != "" {
		args = append(args, "--state-dir", sboxStateDir)
	}
	reaper := exec.Command(self, append(args, "__reap", id)...) //nolint:gosec // our own executable
	reaper.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := reaper.Start(); err != nil {
		return err
	}
	return reaper.Process.Release()
}

func newSandboxTimeoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "timeout <sandbox-id> <duration>",
		Short: "Set or renew when a sandbox is removed, counted from now (e.g. 30m); 0 cancels it",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id := sandbox.StripID(posArgs[0])
			d, err := time.ParseDuration(posArgs[1])
			if err != nil || d < 0 {
				return usageExitErr(fmt.Errorf("invalid duration %q", posArgs[1]), false)
			}
			if !sboxRuntime.Exists(context.Background(), id) {
				return runtimeExitErr(fmt.Errorf("sandbox %s not found", id))
			}
			if d == 0 {
				sboxClearDeadline(id)
				fmt.Fprintf(cmd.OutOrStdout(), "Sandbox %s no longer expires\n", id)
				return nil
			}
			if err := sboxSetDeadline(id, d); err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Sandbox %s expires in %s\n", id, d)
			return nil
		},
	}
}

func newSandboxReapCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "__reap <sandbox-id>",
		Short:  "Internal: wait for a sandbox's deadline and remove it",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, posArgs []string) error {
			id := sandbox.StripID(posArgs[0])
			for {
				deadline, ok := sboxDeadline(id)
				if !ok {
					return nil // removed, cancelled, or the sandbox is gone
				}
				if wait := time.Until(deadline); wait > 0 {
					time.Sleep(min(wait, sboxReapPoll))
					continue
				}
				err := sboxKillOne(context.Background(), id, false, "", "", os.Stderr)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					// Leave the deadline in place so the next timeout or kill can retry.
					return runtimeExitErr(err)
				}
				sboxClearDeadline(id)
				return nil
			}
		},
	}
}

func newSandboxPauseCmd(pause bool) *cobra.Command {
	use, short, done := "resume <sandbox-id>", "Resume a paused sandbox", "Resumed"
	if pause {
		use, short, done = "pause <sandbox-id>", "Freeze a sandbox's processes; memory is kept", "Paused"
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id := sandbox.StripID(posArgs[0])
			op := sboxRuntime.Resume
			if pause {
				op = sboxRuntime.Pause
			}
			if err := op(context.Background(), id); err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s sandbox %s\n", done, id)
			return nil
		},
	}
}
