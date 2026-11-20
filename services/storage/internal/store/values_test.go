package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestGivenValidOrphanKindBytesWhenDecodedThenReturnsExpectedKind(t *testing.T) {

	cases := []struct {
		name     string
		raw      []byte
		wantKind byte
	}{
		{
			name:     "unlinked_open_handle",
			raw:      []byte{0x01},
			wantKind: store.OrphanKindUnlinked,
		},
		{
			name:     "tmpfile_exclusive",
			raw:      []byte{0x02},
			wantKind: store.OrphanKindTmpfileExclusive,
		},
		{
			name:     "tmpfile_linkable",
			raw:      []byte{0x03},
			wantKind: store.OrphanKindTmpfileLinkable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			got, err := store.DecodeOrphanKind(tc.raw)

			require.NoErrorf(t, err, "case %s: decode orphan kind %x failed", tc.name, tc.raw)
			require.Equalf(t, tc.wantKind, got, "case %s: orphan kind mismatch: want=0x%02x got=0x%02x", tc.name, tc.wantKind, got)
		})
	}
}

func TestGivenInvalidOrphanKindBytesWhenDecodedThenReturnsErrInvalidValue(t *testing.T) {

	cases := []struct {
		name string
		raw  []byte
	}{
		{
			name: "nil_slice",
			raw:  nil,
		},
		{
			name: "empty_slice",
			raw:  []byte{},
		},
		{
			name: "zero_kind",
			raw:  []byte{0x00},
		},
		{
			name: "unknown_kind_0x04",
			raw:  []byte{0x04},
		},
		{
			name: "multi_byte_payload",
			raw:  []byte{0x01, 0x02},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			got, err := store.DecodeOrphanKind(tc.raw)

			require.ErrorIsf(t, err, store.ErrInvalidValue, "case %s: raw=%x want ErrInvalidValue got=%v", tc.name, tc.raw, err)
			require.Zerof(t, got, "case %s: invalid decode must return zero kind, got=0x%02x", tc.name, got)
		})
	}
}
