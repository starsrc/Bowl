package cli

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	unitByte uint64 = 1
	unitKiB  uint64 = 1 << 10
	unitMiB  uint64 = 1 << 20
	unitGiB  uint64 = 1 << 30
	unitTiB  uint64 = 1 << 40
	unitPiB  uint64 = 1 << 50
)

// ParseSize 解析人类可读的容量字符串并返回字节数。
// 支持不区分大小写的 1024 二进制幂单位：B 或无单位、K/KB/KiB、M/MB/MiB、G/GB/GiB、T/TB/TiB、P/PB/PiB。
// 结果必须为大于 0 且不超过 math.MaxInt64 的正整数。
func ParseSize(raw string) (uint64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("容量不能为空")
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("容量必须为正整数: %q", raw)
	}

	idx := 0
	for idx < len(s) && s[idx] >= '0' && s[idx] <= '9' {
		idx++
	}
	if idx == 0 {
		return 0, fmt.Errorf("无效的容量格式: %q", raw)
	}

	numStr := s[:idx]
	unitStr := strings.ToUpper(strings.TrimSpace(s[idx:]))

	val, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析容量数值失败 %q: %w", raw, err)
	}
	if val == 0 {
		return 0, fmt.Errorf("容量必须大于 0: %q", raw)
	}

	var mult uint64
	switch unitStr {
	case "", "B":
		mult = unitByte
	case "K", "KB", "KIB":
		mult = unitKiB
	case "M", "MB", "MIB":
		mult = unitMiB
	case "G", "GB", "GIB":
		mult = unitGiB
	case "T", "TB", "TIB":
		mult = unitTiB
	case "P", "PB", "PIB":
		mult = unitPiB
	default:
		return 0, fmt.Errorf("不支持的容量单位 %q (原始输入: %q)", strings.TrimSpace(s[idx:]), raw)
	}

	if val > uint64(math.MaxInt64)/mult {
		return 0, fmt.Errorf("容量超出最大允许上限 (%d 字节): %q", int64(math.MaxInt64), raw)
	}

	return val * mult, nil
}

// FormatSize 将字节数格式化为人类可读的 1024 二进制幂单位字符串。
func FormatSize(bytes uint64) string {
	switch {
	case bytes >= unitPiB:
		return fmt.Sprintf("%.2f PiB", float64(bytes)/float64(unitPiB))
	case bytes >= unitTiB:
		return fmt.Sprintf("%.2f TiB", float64(bytes)/float64(unitTiB))
	case bytes >= unitGiB:
		return fmt.Sprintf("%.2f GiB", float64(bytes)/float64(unitGiB))
	case bytes >= unitMiB:
		return fmt.Sprintf("%.2f MiB", float64(bytes)/float64(unitMiB))
	case bytes >= unitKiB:
		return fmt.Sprintf("%.2f KiB", float64(bytes)/float64(unitKiB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
