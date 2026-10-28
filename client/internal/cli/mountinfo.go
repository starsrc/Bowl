package cli

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var defaultMountInfoPath = "/proc/self/mountinfo"

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

// IsMountPoint 检查指定路径在当前系统 (/proc/self/mountinfo) 中是否为已生效的活跃挂载点。
func IsMountPoint(path string) (bool, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, fmt.Errorf("解析绝对路径失败: %w", err)
	}
	target := filepath.Clean(absPath)

	file, err := os.Open(defaultMountInfoPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// 在非 procfs 环境（如宿主机单元测试无 /proc 时）视为未挂载
			return false, nil
		}
		return false, fmt.Errorf("读取挂载信息文件失败: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mountpoint := filepath.Clean(unescapeMountInfo(fields[4]))
		if mountpoint == target {
			return true, nil
		}
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("扫描挂载信息失败: %w", err)
	}

	return false, nil
}
