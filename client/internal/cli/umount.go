package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

const umountProgressRequestTimeout = 200 * time.Millisecond

var (
	lookPath   = exec.LookPath
	runCommand = func(name string, args ...string) ([]byte, error) {
		return exec.Command(name, args...).CombinedOutput()
	}
	umountIsTerminal = func(w io.Writer) bool {
		f, ok := w.(*os.File)
		if !ok || f == nil {
			return false
		}
		return term.IsTerminal(int(f.Fd()))
	}
	umountHTTPClient           = http.DefaultClient
	umountDrainTimeout         = 5 * time.Minute
	umountDrainExitGrace       = 5 * time.Second
	umountDrainPollInterval    = 20 * time.Millisecond
	umountProgressPollInterval = 100 * time.Millisecond
	umountNonTTYLogInterval    = 2 * time.Second
	umountFallbackGracePeriod  = 1 * time.Second
	umountFallbackLogInterval  = 5 * time.Second
)

func findUmountCommand(absMountpoint string, force bool) (binPath string, cmdArgs []string, found bool) {
	candidates := []struct {
		bin       string
		args      []string
		forceArgs []string
	}{
		{bin: "fusermount3", args: []string{"-u", absMountpoint}, forceArgs: []string{"-uz", absMountpoint}},
		{bin: "fusermount", args: []string{"-u", absMountpoint}, forceArgs: []string{"-uz", absMountpoint}},
		{bin: "umount", args: []string{absMountpoint}, forceArgs: []string{"-l", absMountpoint}},
	}
	for _, c := range candidates {
		path, err := lookPath(c.bin)
		if err != nil {
			continue
		}
		if force {
			return path, c.forceArgs, true
		}
		return path, c.args, true
	}
	return "", nil, false
}

func isMountMetaDrained(metaPath string, meta *mountMeta) bool {
	if meta == nil || meta.PID <= 0 {
		return true
	}
	data, err := os.ReadFile(metaPath)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if err == nil {
		var cur mountMeta
		if json.Unmarshal(data, &cur) == nil && cur.PID > 0 {
			if cur.PID != meta.PID || (!meta.StartedAt.IsZero() && !cur.StartedAt.Equal(meta.StartedAt)) {
				return true
			}
		}
	}
	return !mountMetaProcessAlive(*meta)
}

func normalizeUmountAdminAddr(raw string) string {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "http://")
	trimmed = strings.TrimSuffix(trimmed, "/")
	return loopbackAdminAddr(trimmed)
}

func fetchUnmountProgress(addr string, timeout time.Duration) (fusefs.UnmountProgress, bool) {
	u := url.URL{Scheme: "http", Host: addr, Path: "/debug/unmount"}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return fusefs.UnmountProgress{}, false
	}
	resp, err := umountHTTPClient.Do(req)
	if err != nil {
		return fusefs.UnmountProgress{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fusefs.UnmountProgress{}, false
	}
	var prog fusefs.UnmountProgress
	if decErr := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&prog); decErr != nil {
		return fusefs.UnmountProgress{}, false
	}
	return prog, true
}

type umountProgressReporter struct {
	stderr             io.Writer
	isTTY              bool
	sawProgress        bool
	detStarted         bool
	detBar             *progressbar.ProgressBar
	lastTotal          uint64
	lastCompleted      uint64
	lastNonTTYLogAt    time.Time
	printed100         bool
	fallbackStarted    bool
	fallbackSpinner    *progressbar.ProgressBar
	lastFallbackSpinAt time.Time
	lastFallbackLogAt  time.Time
}

func newUmountProgressReporter(stderr io.Writer) *umountProgressReporter {
	return &umountProgressReporter{
		stderr: stderr,
		isTTY:  umountIsTerminal(stderr),
	}
}

func finishProgressBar(bar *progressbar.ProgressBar, timedOut bool) {
	if bar == nil {
		return
	}
	if timedOut {
		if !bar.IsFinished() {
			_ = bar.Exit()
		}
		return
	}
	_ = bar.Finish()
}

func (r *umountProgressReporter) updateDeterminate(prog fusefs.UnmountProgress, now time.Time) {
	r.sawProgress = true
	if r.fallbackSpinner != nil {
		finishProgressBar(r.fallbackSpinner, false)
		r.fallbackSpinner = nil
	}
	if prog.Total == 0 {
		return
	}
	total := prog.Total
	completed := min(prog.Completed, total)
	if r.detStarted && total == r.lastTotal && completed < r.lastCompleted {
		completed = r.lastCompleted
	}
	prevStarted := r.detStarted
	prevTotal := r.lastTotal
	prevCompleted := r.lastCompleted
	r.detStarted = true
	r.lastTotal = total
	r.lastCompleted = completed

	if r.isTTY {
		maxTotal := int64(min(total, uint64(math.MaxInt64)))
		curVal := int64(min(completed, uint64(math.MaxInt64)))
		if r.detBar == nil {
			r.detBar = progressbar.NewOptions64(
				maxTotal,
				progressbar.OptionSetWriter(r.stderr),
				progressbar.OptionSetDescription("Syncing to cloud"),
				progressbar.OptionShowCount(),
				progressbar.OptionOnCompletion(func() {
					_, _ = io.WriteString(r.stderr, "\n")
				}),
			)
			_ = r.detBar.Set64(curVal)
			return
		}
		if total != prevTotal {
			r.detBar.ChangeMax64(maxTotal)
		}
		if completed != prevCompleted {
			_ = r.detBar.Set64(curVal)
		}
		return
	}

	pct := int((completed * 100) / total)
	shouldPrint := !prevStarted || (!r.printed100 && now.Sub(r.lastNonTTYLogAt) >= umountNonTTYLogInterval)
	if !shouldPrint {
		return
	}
	r.lastNonTTYLogAt = now
	if completed == total && pct == 100 {
		r.printed100 = true
	}
	fmt.Fprintf(r.stderr, "Syncing to cloud: %d%% (%d/%d)\n", pct, completed, total)
}

func (r *umountProgressReporter) updateFallback(now, start time.Time) {
	if r.sawProgress || now.Sub(start) < umountFallbackGracePeriod {
		return
	}
	if r.isTTY {
		if r.fallbackSpinner == nil {
			r.fallbackSpinner = progressbar.NewOptions64(
				-1,
				progressbar.OptionSetWriter(r.stderr),
				progressbar.OptionSetDescription("Syncing to cloud"),
				progressbar.OptionSetSpinnerChangeInterval(0),
				progressbar.OptionSpinnerType(14),
				progressbar.OptionOnCompletion(func() {
					_, _ = io.WriteString(r.stderr, "\n")
				}),
			)
			_ = r.fallbackSpinner.Add(1)
			r.lastFallbackSpinAt = now
			return
		}
		if now.Sub(r.lastFallbackSpinAt) >= umountProgressPollInterval {
			_ = r.fallbackSpinner.Add(1)
			r.lastFallbackSpinAt = now
		}
		return
	}
	if !r.fallbackStarted || now.Sub(r.lastFallbackLogAt) >= umountFallbackLogInterval {
		r.fallbackStarted = true
		r.lastFallbackLogAt = now
		fmt.Fprintln(r.stderr, "Syncing to cloud...")
	}
}

func (r *umountProgressReporter) finish(timedOut bool) {
	if r.isTTY {
		finishProgressBar(r.detBar, timedOut)
		finishProgressBar(r.fallbackSpinner, timedOut)
		return
	}
	if timedOut || !r.detStarted || r.printed100 {
		return
	}
	fmt.Fprintf(r.stderr, "Syncing to cloud: 100%% (%d/%d)\n", r.lastTotal, r.lastTotal)
	r.printed100 = true
}

func waitMountMetaDrained(stderr io.Writer, metaPath string, meta *mountMeta) error {
	if stderr == nil {
		stderr = io.Discard
	}
	var rawAdminAddr string
	if meta != nil {
		rawAdminAddr = meta.AdminAddr
	}
	if isMountMetaDrained(metaPath, meta) {
		return nil
	}

	adminAddr := normalizeUmountAdminAddr(rawAdminAddr)
	hasAdmin := adminAddr != "" && !strings.EqualFold(adminAddr, adminAddrOff)
	start := time.Now()
	drainTimeout := umountDrainTimeout
	if meta != nil && meta.UnmountTimeoutMs > 0 {
		drainTimeout = time.Duration(meta.UnmountTimeoutMs)*time.Millisecond + umountDrainExitGrace
	}
	deadline := start.Add(drainTimeout)
	reporter := newUmountProgressReporter(stderr)
	var lastPollAt time.Time

	for {
		if isMountMetaDrained(metaPath, meta) {
			reporter.finish(false)
			return nil
		}
		now := time.Now()
		if now.After(deadline) {
			reporter.finish(true)
			return fmt.Errorf("unmount command returned, but timed out waiting for mount process drain and lease release for %s", metaPath)
		}
		if hasAdmin && (lastPollAt.IsZero() || now.Sub(lastPollAt) >= umountProgressPollInterval) {
			lastPollAt = now
			reqTimeout := min(umountProgressRequestTimeout, max(10*time.Millisecond, time.Until(deadline)))
			if prog, ok := fetchUnmountProgress(adminAddr, reqTimeout); ok {
				reporter.updateDeterminate(prog, now)
			}
		}
		reporter.updateFallback(now, start)
		time.Sleep(umountDrainPollInterval)
	}
}

func newUmountCmd() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:     "umount <mountpoint>",
		Aliases: []string{"unmount"},
		Short:   "Unmount a locally mounted virtual disk",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			return runUmount(cmd.ErrOrStderr(), posArgs[0], force)
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "Force or lazy unmount a busy mountpoint")
	return cmd
}

func runUmount(stderr io.Writer, mountpoint string, force bool) error {
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		return runtimeExitErr(fmt.Errorf("failed to resolve mountpoint path: %w", err))
	}
	absMountpoint = filepath.Clean(absMountpoint)

	info, mounted, err := ResolveMountPointInfo(absMountpoint)
	if err != nil {
		return runtimeExitErr(fmt.Errorf("failed to check mountpoint status: %w", err))
	}
	if !mounted {
		return runtimeExitErr(fmt.Errorf("path %q is not a mounted filesystem", absMountpoint))
	}
	if !info.IsBowl() {
		fsType := info.FSType
		if fsType == "" {
			fsType = "unknown"
		}
		return runtimeExitErr(fmt.Errorf("path %q is mounted as %q, not a bowl filesystem", absMountpoint, fsType))
	}
	canonMountpoint := info.Path

	binPath, cmdArgs, found := findUmountCommand(canonMountpoint, force)
	if !found {
		return runtimeExitErr(errors.New("no system unmount tool found: expected fusermount3, fusermount, or umount"))
	}

	var (
		metaPath string
		meta     mountMeta
		hasMeta  bool
	)
	if !force {
		metaPath, meta, hasMeta = resolveUmountMountMeta(canonMountpoint)
	}

	if out, runErr := runCommand(binPath, cmdArgs...); runErr != nil {
		outStr := string(out)
		if outStr != "" {
			return runtimeExitErr(fmt.Errorf("unmount failed: %w\n%s", runErr, outStr))
		}
		return runtimeExitErr(fmt.Errorf("unmount failed: %w", runErr))
	}

	if !force && hasMeta {
		if drainErr := waitMountMetaDrained(stderr, metaPath, &meta); drainErr != nil {
			return runtimeExitErr(drainErr)
		}
	}
	return nil
}
