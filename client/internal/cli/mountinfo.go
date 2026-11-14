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

// DiskMountInfo 描述本机 /proc/self/mountinfo 中某块虚拟磁盘的挂载状态与读写模式。
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

// unescapeMountInfo 解析 /proc/self/mountinfo 中被八进制转义的字符（例如 \040 为空格）。
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

// parseMountPointsFromReader 解析 mountinfo 文本，跳过空行与字段不足的畸形行。
func parseMountPointsFromReader(r io.Reader) ([]mountPoint, error) {
	var points []mountPoint
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(strings.TrimSpace(scanner.Text()))
		if len(fields) < 5 {
			continue
		}
		source := ""
		superOpts := ""
		for i, field := range fields {
			if field == "-" && i+2 < len(fields) {
				source = unescapeMountInfo(fields[i+2])
				if i+3 < len(fields) {
					superOpts = fields[i+3]
				}
				break
			}
		}
		readOnly := hasReadOnlyMountOption(superOpts)
		if len(fields) > 5 {
			readOnly = readOnly || hasReadOnlyMountOption(fields[5])
		}
		points = append(points, mountPoint{
			path:     filepath.Clean(unescapeMountInfo(fields[4])),
			source:   source,
			readOnly: readOnly,
		})
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("扫描挂载信息失败: %w", err)
	}
	return points, nil
}

func readMountPoints() ([]mountPoint, error) {
	file, err := os.Open(mountInfoPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// 在非 procfs 环境（如宿主机单元测试无 /proc 时）视为未挂载
			return nil, nil
		}
		return nil, fmt.Errorf("读取挂载信息文件失败: %w", err)
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

// IsMountPoint 检查指定路径在当前系统 (/proc/self/mountinfo) 中是否为已生效的活跃挂载点。
func IsMountPoint(path string) (bool, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, fmt.Errorf("解析绝对路径失败: %w", err)
	}
	points, err := readMountPoints()
	if err != nil {
		return false, err
	}
	return containsMountPoint(points, filepath.Clean(absPath)), nil
}

// FindDiskMountpoint 扫描 /proc/self/mountinfo 检查指定 diskID 是否在当前机器处于活跃挂载状态。
// 挂载时 FUSE fsname 固定为 "bowl:<disk-id>"（位于 mountinfo "-" 分隔符后的第 2 个字段），并解析第 6 列 VFS 选项判定只读模式。
// 若 /proc/self/mountinfo 不存在（如非 Linux 环境），返回零值 DiskMountInfo 与 nil。
func FindDiskMountpoint(diskID string) (DiskMountInfo, error) {
	points, err := readMountPoints()
	if err != nil {
		return DiskMountInfo{}, err
	}
	return findDiskMountpoint(points, diskID), nil
}
