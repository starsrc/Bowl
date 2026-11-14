//go:build linux

package fusefs

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// directMountFlags 返回 DirectMount 传给 mount(2) 的内核挂载标志；只读挂载显式携带 MS_RDONLY。
func directMountFlags(readOnly bool) uintptr {
	flags := uintptr(unix.MS_NOSUID | unix.MS_NODEV)
	if readOnly {
		flags |= unix.MS_RDONLY
	}
	return flags
}

func resolveFuseSysfsConnectionDir(mountpoint string) string {
	if strings.TrimSpace(mountpoint) == "" {
		return ""
	}
	var st unix.Stat_t
	if err := unix.Stat(mountpoint, &st); err != nil {
		return ""
	}
	major := unix.Major(st.Dev)
	minor := unix.Minor(st.Dev)
	kdev := (uint64(major) << 20) | uint64(minor)

	candidates := [2]uint64{kdev, st.Dev}
	for _, dev := range candidates {
		dir := fmt.Sprintf("/sys/fs/fuse/connections/%d", dev)
		if _, err := os.Stat(filepath.Join(dir, "waiting")); err == nil {
			return dir
		}
	}
	return ""
}

func readFuseConnectionGauges(sysfsDir string) (waiting, congestionThreshold int) {
	if sysfsDir == "" {
		return 0, 0
	}
	return readSysfsIntFile(filepath.Join(sysfsDir, "waiting")),
		readSysfsIntFile(filepath.Join(sysfsDir, "congestion_threshold"))
}

func readSysfsIntFile(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || v < 0 {
		return 0
	}
	return v
}
