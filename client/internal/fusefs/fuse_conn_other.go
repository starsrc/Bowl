//go:build !linux

package fusefs

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// directMountFlags 在非 Linux 平台返回 0，供跨平台编译占位。
func directMountFlags(_ bool) uintptr { return 0 }

func resolveFuseSysfsConnectionDir(_ string) string {
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
