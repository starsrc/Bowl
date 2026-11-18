package cli

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

// Test seams for unmount tool discovery, execution, and drain polling.
var (
	lookPath   = exec.LookPath
	runCommand = func(name string, args ...string) ([]byte, error) {
		return exec.Command(name, args...).CombinedOutput()
	}
	// 卸载命令返回后挂载进程还要刷盘并释放租约，耗时随盘上文件数增长（1.8 万个小文件实测 34 秒）。
	// 等待在挂载进程退出时立即结束，这个时限只用来兜住挂载进程卡死的情况。
	umountDrainTimeout      = 5 * time.Minute
	umountDrainPollInterval = 20 * time.Millisecond
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

func waitMountMetaDrained(metaPath string, pid int) error {
	deadline := time.Now().Add(umountDrainTimeout)
	for {
		if _, err := os.Stat(metaPath); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if pid > 0 && !processAlive(pid) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("unmount command returned, but timed out waiting for mount process drain and lease release for %s", metaPath)
		}
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
		RunE: func(_ *cobra.Command, posArgs []string) error {
			return runUmount(posArgs[0], force)
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "Force or lazy unmount a busy mountpoint")
	return cmd
}

func runUmount(mountpoint string, force bool) error {
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		return runtimeExitErr(fmt.Errorf("failed to resolve mountpoint path: %w", err))
	}
	absMountpoint = filepath.Clean(absMountpoint)

	mounted, err := IsMountPoint(absMountpoint)
	if err != nil {
		return runtimeExitErr(fmt.Errorf("failed to check mountpoint status: %w", err))
	}
	if !mounted {
		return runtimeExitErr(fmt.Errorf("path %q is not a mounted filesystem", absMountpoint))
	}

	binPath, cmdArgs, found := findUmountCommand(absMountpoint, force)
	if !found {
		return runtimeExitErr(errors.New("no system unmount tool found: expected fusermount3, fusermount, or umount"))
	}

	var (
		metaPath string
		hasMeta  bool
		metaPID  int
	)
	if !force {
		metaPath, hasMeta = mountMetaFilePath(absMountpoint)
		if hasMeta {
			metaPID = readMountMetaPID(metaPath)
		}
	}

	if out, runErr := runCommand(binPath, cmdArgs...); runErr != nil {
		outStr := string(out)
		if outStr != "" {
			return runtimeExitErr(fmt.Errorf("unmount failed: %w\n%s", runErr, outStr))
		}
		return runtimeExitErr(fmt.Errorf("unmount failed: %w", runErr))
	}

	if !force && hasMeta {
		if drainErr := waitMountMetaDrained(metaPath, metaPID); drainErr != nil {
			return runtimeExitErr(drainErr)
		}
	}
	return nil
}
