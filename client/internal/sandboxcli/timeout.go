package sandboxcli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var sboxReapPoll = 5 * time.Second

func sboxDeadlinePath(id string) string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "bowl-sandbox", "deadlines", id)
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
	reaper := exec.Command(self, "--state-dir", vm.StateDir, "__reap", id)
	reaper.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := reaper.Start(); err != nil {
		return err
	}
	return reaper.Process.Release()
}

func newTimeoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "timeout <sandbox-id> <duration>",
		Short: "Set or renew when a sandbox is removed, counted from now (e.g. 30m); 0 cancels it",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id := posArgs[0]
			d, err := time.ParseDuration(posArgs[1])
			if err != nil || d < 0 {
				return usageExitErr(fmt.Errorf("invalid duration %q", posArgs[1]), false)
			}
			if !vm.Exists(id) {
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

func newReapCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "__reap <sandbox-id>",
		Short:  "Internal: wait for a sandbox's deadline and remove it",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, posArgs []string) error {
			id := posArgs[0]
			for {
				deadline, ok := sboxDeadline(id)
				if !ok {
					return nil
				}
				if wait := time.Until(deadline); wait > 0 {
					time.Sleep(min(wait, sboxReapPoll))
					continue
				}
				if vm.Exists(id) {
					if err := sboxKill(context.Background(), id); err != nil {
						return runtimeExitErr(err)
					}
				}
				sboxClearDeadline(id)
				return nil
			}
		},
	}
}
