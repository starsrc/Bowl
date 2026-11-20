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

const maxMountInfoSymlinkHops = 40

var (
	mountInfoPath     = "/proc/self/mountinfo"
	mountInfoLstat    = os.Lstat
	mountInfoReadlink = os.Readlink
)

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

			return nil, nil
		}
		return nil, fmt.Errorf("read mountinfo file: %w", err)
	}
	defer file.Close()
	return parseMountPointsFromReader(file)
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

func splitAbsComponents(cleanAbs string) []string {
	trimmed := strings.TrimPrefix(cleanAbs, string(filepath.Separator))
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, string(filepath.Separator))
}

func walkMountPointComponents(candidate string, hops *int, mountSet map[string]struct{}) (nextCandidate string, mounted, expanded bool) {
	parts := splitAbsComponents(candidate)
	curPrefix := string(filepath.Separator)
	for idx, comp := range parts {

		checkCandidate := filepath.Clean(filepath.Join(append([]string{curPrefix}, parts[idx:]...)...))
		if _, ok := mountSet[checkCandidate]; ok {
			return checkCandidate, true, false
		}
		nextPath := filepath.Join(curPrefix, comp)
		if _, isMount := mountSet[nextPath]; isMount {

			curPrefix = nextPath
			continue
		}
		info, lerr := mountInfoLstat(nextPath)
		if lerr != nil || info == nil {
			return candidate, false, false
		}
		if info.Mode()&os.ModeSymlink == 0 {
			curPrefix = nextPath
			continue
		}
		*hops++
		if *hops > maxMountInfoSymlinkHops {
			return candidate, false, false
		}
		target, rerr := mountInfoReadlink(nextPath)
		if rerr != nil {
			return candidate, false, false
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(curPrefix, target)
		}
		joined := append([]string{target}, parts[idx+1:]...)
		return filepath.Clean(filepath.Join(joined...)), false, true
	}
	return candidate, false, false
}

func ResolveMountPoint(path string) (resolvedPath string, mounted bool, err error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", false, fmt.Errorf("resolve absolute path: %w", err)
	}
	cleanAbs := filepath.Clean(absPath)

	points, err := readMountPoints()
	if err != nil {
		return "", false, err
	}
	mountSet := make(map[string]struct{}, len(points))
	for _, point := range points {
		mountSet[point.path] = struct{}{}
	}
	if _, ok := mountSet[cleanAbs]; ok {
		return cleanAbs, true, nil
	}

	candidate := cleanAbs
	hops := 0
	for {
		if _, ok := mountSet[candidate]; ok {
			return candidate, true, nil
		}
		nextCandidate, matched, expanded := walkMountPointComponents(candidate, &hops, mountSet)
		if matched {
			return nextCandidate, true, nil
		}
		if !expanded {
			return nextCandidate, false, nil
		}
		candidate = nextCandidate
	}
}

func IsMountPoint(path string) (bool, error) {
	_, mounted, err := ResolveMountPoint(path)
	return mounted, err
}

func FindDiskMountpoint(diskID string) (DiskMountInfo, error) {
	points, err := readMountPoints()
	if err != nil {
		return DiskMountInfo{}, err
	}
	return findDiskMountpoint(points, diskID), nil
}
