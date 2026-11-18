package cli

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

var mountInfoPath = "/proc/self/mountinfo"

// DiskMountInfo describes the local mount state and read-only mode of a virtual disk from /proc/self/mountinfo.
type DiskMountInfo struct {
	Mountpoint string
	Mounted    bool
	ReadOnly   bool
}

type mountPoint struct {
	path     string
	source   string
	readOnly bool
}

func hasReadOnlyMountOption(vfsOpts string) bool {
	return slices.Contains(strings.Split(vfsOpts, ","), "ro")
}

// unescapeMountInfo decodes octal escape sequences such as \040 for spaces in /proc/self/mountinfo.
func unescapeMountInfo(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}

	var buf bytes.Buffer
	buf.Grow(len(s))
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+3 < len(s) && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			val, err := strconv.ParseUint(s[i+1:i+4], 8, 8)
			if err == nil {
				buf.WriteByte(byte(val))
				i += 4
				continue
			}
		}
		buf.WriteByte(s[i])
		i++
	}
	return buf.String()
}

func isOctal(b byte) bool {
	return b >= '0' && b <= '7'
}

// parseMountPointsFromReader parses mountinfo text and skips blank or malformed lines.
func parseMountPointsFromReader(r io.Reader) ([]mountPoint, error) {
	var points []mountPoint
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(strings.TrimSpace(scanner.Text()))
		if len(fields) < 5 {
			continue
		}
		source := ""
		for i, field := range fields {
			if field == "-" && i+2 < len(fields) {
				source = unescapeMountInfo(fields[i+2])
				break
			}
		}
		var readOnly bool
		if len(fields) > 5 {
			readOnly = hasReadOnlyMountOption(fields[5])
		}
		points = append(points, mountPoint{
			path:     filepath.Clean(unescapeMountInfo(fields[4])),
			source:   source,
			readOnly: readOnly,
		})
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("scan mountinfo: %w", err)
	}
	return points, nil
}

func readMountPoints() ([]mountPoint, error) {
	file, err := os.Open(mountInfoPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Treat absent procfs mountinfo as unmounted on non-Linux hosts.
			return nil, nil
		}
		return nil, fmt.Errorf("read mountinfo file: %w", err)
	}
	defer file.Close()
	return parseMountPointsFromReader(file)
}

func containsMountPoint(points []mountPoint, target string) bool {
	for _, point := range points {
		if point.path == target {
			return true
		}
	}
	return false
}

func findDiskMountpoint(points []mountPoint, diskID string) DiskMountInfo {
	expectedSource := "bowl:" + strings.TrimSpace(diskID)
	var found DiskMountInfo
	for _, point := range points {
		if !strings.EqualFold(point.source, expectedSource) {
			continue
		}
		if !found.Mounted || !point.readOnly {
			found = DiskMountInfo{
				Mountpoint: point.path,
				Mounted:    true,
				ReadOnly:   point.readOnly,
			}
			if !point.readOnly {
				return found
			}
		}
	}
	return found
}

// IsMountPoint reports whether path is an active mountpoint in /proc/self/mountinfo.
func IsMountPoint(path string) (bool, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, fmt.Errorf("resolve absolute path: %w", err)
	}
	points, err := readMountPoints()
	if err != nil {
		return false, err
	}
	return containsMountPoint(points, filepath.Clean(absPath)), nil
}

// FindDiskMountpoint scans /proc/self/mountinfo to check if diskID is actively mounted locally.
// FUSE fsname is set to "bowl:<disk-id>" after the "-" separator, and column 6 VFS options determine read-only mode.
func FindDiskMountpoint(diskID string) (DiskMountInfo, error) {
	points, err := readMountPoints()
	if err != nil {
		return DiskMountInfo{}, err
	}
	return findDiskMountpoint(points, diskID), nil
}
