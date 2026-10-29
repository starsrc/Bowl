package cli_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/cli"
)

func TestParseSize_Valid(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		expected uint64
	}{
		{name: "plain bytes", raw: "1", expected: 1},
		{name: "plain bytes with spaces", raw: "  4096  ", expected: 4096},
		{name: "unit B", raw: "512B", expected: 512},
		{name: "unit b lowercase", raw: "512b", expected: 512},
		{name: "unit B with inner space", raw: "512 B", expected: 512},
		{name: "unit K", raw: "1K", expected: 1024},
		{name: "unit KB", raw: "2KB", expected: 2 * 1024},
		{name: "unit KiB", raw: "4KiB", expected: 4 * 1024},
		{name: "unit kib lowercase", raw: "4kib", expected: 4 * 1024},
		{name: "unit M", raw: "1M", expected: 1024 * 1024},
		{name: "unit MB", raw: "10MB", expected: 10 * 1024 * 1024},
		{name: "unit MiB", raw: "128MiB", expected: 128 * 1024 * 1024},
		{name: "unit G", raw: "1G", expected: 1024 * 1024 * 1024},
		{name: "unit GB", raw: "5GB", expected: 5 * 1024 * 1024 * 1024},
		{name: "unit GiB", raw: "10GiB", expected: 10 * 1024 * 1024 * 1024},
		{name: "unit gib lowercase", raw: "10gib", expected: 10 * 1024 * 1024 * 1024},
		{name: "unit T", raw: "1T", expected: 1024 * 1024 * 1024 * 1024},
		{name: "unit TB", raw: "2TB", expected: 2 * 1024 * 1024 * 1024 * 1024},
		{name: "unit TiB", raw: "3TiB", expected: 3 * 1024 * 1024 * 1024 * 1024},
		{name: "unit P", raw: "1P", expected: 1024 * 1024 * 1024 * 1024 * 1024},
		{name: "unit PB", raw: "2PB", expected: 2 * 1024 * 1024 * 1024 * 1024 * 1024},
		{name: "unit PiB", raw: "4PiB", expected: 4 * 1024 * 1024 * 1024 * 1024 * 1024},
		{name: "max PiB without int64 overflow", raw: "8191PiB", expected: 8191 * 1024 * 1024 * 1024 * 1024 * 1024},
		{name: "exact math.MaxInt64", raw: "9223372036854775807", expected: uint64(math.MaxInt64)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cli.ParseSize(tc.raw)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestParseSize_Invalid(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "whitespace only", raw: "   "},
		{name: "zero plain", raw: "0"},
		{name: "zero with unit", raw: "0GiB"},
		{name: "negative plain", raw: "-10"},
		{name: "negative with unit", raw: "-10GiB"},
		{name: "unit only", raw: "GiB"},
		{name: "decimal float", raw: "1.5GiB"},
		{name: "unknown unit", raw: "10EiB"},
		{name: "unknown unit xyz", raw: "100xyz"},
		{name: "exceeds math.MaxInt64 by 1", raw: "9223372036854775808"},
		{name: "exceeds math.MaxInt64 in PiB", raw: "8192PiB"},
		{name: "uint64 overflow in PiB", raw: "20000PiB"},
		{name: "exceeds uint64 digits", raw: "99999999999999999999999999999"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cli.ParseSize(tc.raw)
			require.Error(t, err, "expected error for input %q", tc.raw)
		})
	}
}

func TestFormatSize(t *testing.T) {
	cases := []struct {
		bytes    uint64
		expected string
	}{
		{bytes: 0, expected: "0 B"},
		{bytes: 512, expected: "512 B"},
		{bytes: 1023, expected: "1023 B"},
		{bytes: 1024, expected: "1.00 KiB"},
		{bytes: 1536, expected: "1.50 KiB"},
		{bytes: 1024 * 1024, expected: "1.00 MiB"},
		{bytes: 10 * 1024 * 1024 * 1024, expected: "10.00 GiB"},
		{bytes: 2 * 1024 * 1024 * 1024 * 1024, expected: "2.00 TiB"},
		{bytes: 3 * 1024 * 1024 * 1024 * 1024 * 1024, expected: "3.00 PiB"},
	}

	for _, tc := range cases {
		t.Run(tc.expected, func(t *testing.T) {
			require.Equal(t, tc.expected, cli.FormatSize(tc.bytes))
		})
	}
}
