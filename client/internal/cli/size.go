package cli

import (
	"errors"
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

func ParseSize(raw string) (uint64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("size cannot be empty")
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("size must be a positive integer: %q", raw)
	}

	idx := 0
	for idx < len(s) && s[idx] >= '0' && s[idx] <= '9' {
		idx++
	}
	if idx == 0 {
		return 0, fmt.Errorf("invalid size format: %q", raw)
	}

	numStr := s[:idx]
	unitStr := strings.ToUpper(strings.TrimSpace(s[idx:]))

	val, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size value %q: %w", raw, err)
	}
	if val == 0 {
		return 0, fmt.Errorf("size must be greater than 0: %q", raw)
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
		return 0, fmt.Errorf("unsupported size unit %q in %q", strings.TrimSpace(s[idx:]), raw)
	}

	if val > uint64(math.MaxInt64)/mult {
		return 0, fmt.Errorf("size %q exceeds maximum limit of %d bytes", raw, int64(math.MaxInt64))
	}

	return val * mult, nil
}

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
