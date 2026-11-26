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

type DiskMountInfo struct {
	Mountpoint string
	Mounted    bool
	ReadOnly   bool
}

type MountPointInfo struct {
	Path     string
	FSType   string
	Source   string
	ReadOnly bool
}

func (m MountPointInfo) IsBowl() bool {
	return m.FSType == "fuse.bowl" || strings.HasPrefix(m.Source, "bowl:")
}

type mountPoint struct {
	path     string
	fsType   string
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
		fsType := ""
		source := ""
		for i, field := range fields {
			if field != "-" {
				continue
			}
			if i+1 < len(fields) {
				fsType = unescapeMountInfo(fields[i+1])
			}
			if i+2 < len(fields) {
				source = unescapeMountInfo(fields[i+2])
			}
			break
		}
		var readOnly bool
		if len(fields) > 5 {
			readOnly = hasReadOnlyMountOption(fields[5])
		}
		points = append(points, mountPoint{
			path:     filepath.Clean(unescapeMountInfo(fields[4])),
			fsType:   fsType,
			source:   source,
			readOnly: readOnly,
		})
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("scan mountinfo: %w", err)
	}
	return points, nil
}

func (rt *cliRuntime) readMountPoints() ([]mountPoint, error) {
	file, err := os.Open(rt.mountInfoPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read mountinfo file: %w", err)
	}
	defer file.Close()
	return parseMountPointsFromReader(file)
}

func parseBowlMountSource(source string) (diskID, branch string, ok bool) {
	rest, found := strings.CutPrefix(source, "bowl:")
	if !found || rest == "" {
		return "", "", false
	}
	if before, after, hasSep := strings.Cut(rest, "@"); hasSep {
		return before, after, true
	}
	return rest, "", true
}

func findDiskMountpointInPoints(points []mountPoint, diskID string) DiskMountInfo {
	targetDiskID := strings.TrimSpace(diskID)
	var found DiskMountInfo
	for _, point := range points {
		parsedDiskID, _, ok := parseBowlMountSource(point.source)
		if !ok || !strings.EqualFold(parsedDiskID, targetDiskID) {
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

func (rt *cliRuntime) findBranchMountpointInPoints(points []mountPoint, diskID, branch string) DiskMountInfo {
	targetDiskID := strings.TrimSpace(diskID)
	var found DiskMountInfo
	for _, point := range points {
		parsedDiskID, parsedBranch, ok := parseBowlMountSource(point.source)
		if !ok || !strings.EqualFold(parsedDiskID, targetDiskID) {
			continue
		}
		if parsedBranch != "" {
			if parsedBranch != branch {
				continue
			}
		} else {
			meta, hasMeta := rt.findMountMeta(point.path)
			if !hasMeta || meta.Branch != branch {
				continue
			}
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

func (rt *cliRuntime) walkMountPointComponents(candidate string, hops *int, mountMap map[string]mountPoint) (nextCandidate string, matchedPoint mountPoint, mounted, expanded bool) {
	parts := splitAbsComponents(candidate)
	curPrefix := string(filepath.Separator)
	for idx, comp := range parts {
		checkCandidate := filepath.Clean(filepath.Join(append([]string{curPrefix}, parts[idx:]...)...))
		if mp, ok := mountMap[checkCandidate]; ok {
			return checkCandidate, mp, true, false
		}
		nextPath := filepath.Join(curPrefix, comp)
		if _, isMount := mountMap[nextPath]; isMount {
			curPrefix = nextPath
			continue
		}
		info, lerr := rt.mountInfoLstat(nextPath)
		if lerr != nil || info == nil {
			return candidate, mountPoint{}, false, false
		}
		if info.Mode()&os.ModeSymlink == 0 {
			curPrefix = nextPath
			continue
		}
		*hops++
		if *hops > maxMountInfoSymlinkHops {
			return candidate, mountPoint{}, false, false
		}
		target, rerr := rt.mountInfoReadlink(nextPath)
		if rerr != nil {
			return candidate, mountPoint{}, false, false
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(curPrefix, target)
		}
		joined := append([]string{target}, parts[idx+1:]...)
		return filepath.Clean(filepath.Join(joined...)), mountPoint{}, false, true
	}
	return candidate, mountPoint{}, false, false
}

func (rt *cliRuntime) resolveMountPointEntry(path string) (resolvedPath string, point mountPoint, mounted bool, err error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", mountPoint{}, false, fmt.Errorf("resolve absolute path: %w", err)
	}
	cleanAbs := filepath.Clean(absPath)

	points, err := rt.readMountPoints()
	if err != nil {
		return "", mountPoint{}, false, err
	}
	mountMap := make(map[string]mountPoint, len(points))
	for _, p := range points {
		mountMap[p.path] = p
	}
	if mp, ok := mountMap[cleanAbs]; ok {
		return cleanAbs, mp, true, nil
	}

	candidate := cleanAbs
	hops := 0
	for {
		if mp, ok := mountMap[candidate]; ok {
			return candidate, mp, true, nil
		}
		nextCandidate, mp, matched, expanded := rt.walkMountPointComponents(candidate, &hops, mountMap)
		if matched {
			return nextCandidate, mp, true, nil
		}
		if !expanded {
			return nextCandidate, mountPoint{}, false, nil
		}
		candidate = nextCandidate
	}
}

func (rt *cliRuntime) resolveMountPointInfo(path string) (MountPointInfo, bool, error) {
	_, mp, mounted, err := rt.resolveMountPointEntry(path)
	if err != nil || !mounted {
		return MountPointInfo{}, false, err
	}
	return MountPointInfo{
		Path:     mp.path,
		FSType:   mp.fsType,
		Source:   mp.source,
		ReadOnly: mp.readOnly,
	}, true, nil
}

func (rt *cliRuntime) resolveMountPoint(path string) (resolvedPath string, mounted bool, err error) {
	resolvedPath, _, mounted, err = rt.resolveMountPointEntry(path)
	return resolvedPath, mounted, err
}

func (rt *cliRuntime) isMountPoint(path string) (bool, error) {
	_, mounted, err := rt.resolveMountPoint(path)
	return mounted, err
}

func (rt *cliRuntime) findDiskMountpoint(diskID string) (DiskMountInfo, error) {
	points, err := rt.readMountPoints()
	if err != nil {
		return DiskMountInfo{}, err
	}
	return findDiskMountpointInPoints(points, diskID), nil
}

func (rt *cliRuntime) findBranchMountpoint(diskID, branch string) (DiskMountInfo, error) {
	points, err := rt.readMountPoints()
	if err != nil {
		return DiskMountInfo{}, err
	}
	return rt.findBranchMountpointInPoints(points, diskID, branch), nil
}
