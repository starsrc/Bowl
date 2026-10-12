package chunk_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/chunk"
)

type mockSliceReader struct {
	data map[uint64][]byte
}

func (m *mockSliceReader) ReadSlice(diskID string, sliceID uint64) ([]byte, error) {
	d, ok := m.data[sliceID]
	if !ok {
		return nil, fmt.Errorf("slice %d not found", sliceID)
	}
	return d, nil
}

func TestReconstituteRange_SparseHoleAndSingleSlice(t *testing.T) {
	reader := &mockSliceReader{
		data: map[uint64][]byte{
			1: []byte("HELLO_WORLD_TEST"), // 16 bytes
		},
	}

	// Slice located at pos=10 in chunk, length 16
	slices := []chunk.Slice{
		{
			Pos:  10,
			ID:   1,
			Size: 16,
			Off:  0,
			Len:  16,
		},
	}

	// Read 40 bytes from pos=0 to pos=40:
	// 0..9 should be 0x00
	// 10..25 should be "HELLO_WORLD_TEST"
	// 26..39 should be 0x00
	result, err := chunk.ReconstituteRange("disk-1", slices, 0, 40, reader)
	require.NoError(t, err)
	require.Len(t, result, 40)

	expected := make([]byte, 40)
	copy(expected[10:26], []byte("HELLO_WORLD_TEST"))
	assert.Equal(t, expected, result)
}

func TestReconstituteRange_MultipleOverlappingSlices(t *testing.T) {
	// Base write: 100 bytes of 'A' at offset 0
	// Overwrite 1: 20 bytes of 'B' at offset 10
	// Overwrite 2: 10 bytes of 'C' at offset 50
	reader := &mockSliceReader{
		data: map[uint64][]byte{
			1: bytes.Repeat([]byte("A"), 100),
			2: bytes.Repeat([]byte("B"), 20),
			3: bytes.Repeat([]byte("C"), 10),
		},
	}

	slices := []chunk.Slice{
		{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100},
		{Pos: 10, ID: 2, Size: 20, Off: 0, Len: 20},
		{Pos: 50, ID: 3, Size: 10, Off: 0, Len: 10},
	}

	// 1. Read entire range 0..100
	result, err := chunk.ReconstituteRange("disk-1", slices, 0, 100, reader)
	require.NoError(t, err)

	expected := make([]byte, 100)
	copy(expected[0:100], bytes.Repeat([]byte("A"), 100))
	copy(expected[10:30], bytes.Repeat([]byte("B"), 20))
	copy(expected[50:60], bytes.Repeat([]byte("C"), 10))

	assert.Equal(t, expected, result)

	// 2. Read sub-range 5..35 (straddling overwrite 1 boundaries)
	subResult, err := chunk.ReconstituteRange("disk-1", slices, 5, 30, reader)
	require.NoError(t, err)
	assert.Equal(t, expected[5:35], subResult)

	// 3. Read sub-range 45..65 (straddling overwrite 2 boundaries)
	subResult2, err := chunk.ReconstituteRange("disk-1", slices, 45, 20, reader)
	require.NoError(t, err)
	assert.Equal(t, expected[45:65], subResult2)
}

func TestReconstituteRange_SparseHoleBetweenSlices(t *testing.T) {
	// Slices at 0..10 ('X') and 50..60 ('Y')
	// Read 0..60 -> verify 10..49 are all zero
	reader := &mockSliceReader{
		data: map[uint64][]byte{
			1: bytes.Repeat([]byte("X"), 10),
			2: bytes.Repeat([]byte("Y"), 10),
		},
	}

	slices := []chunk.Slice{
		{Pos: 0, ID: 1, Size: 10, Off: 0, Len: 10},
		{Pos: 50, ID: 2, Size: 10, Off: 0, Len: 10},
	}

	result, err := chunk.ReconstituteRange("disk-1", slices, 0, 60, reader)
	require.NoError(t, err)
	require.Len(t, result, 60)

	assert.Equal(t, bytes.Repeat([]byte("X"), 10), result[0:10])
	assert.Equal(t, make([]byte, 40), result[10:50], "bytes 10..49 must be zero sparse hole")
	assert.Equal(t, bytes.Repeat([]byte("Y"), 10), result[50:60])
}

func TestReconstituteRange_SliceWithNonZeroOffset(t *testing.T) {
	// Physical slice payload is 32 bytes: 8 bytes prefix + 16 bytes useful data + 8 bytes suffix
	payload := []byte("PREFIX__VALID_DATA_CONTENT_SUFFIX__")
	reader := &mockSliceReader{
		data: map[uint64][]byte{
			1: payload,
		},
	}

	// Slice starts at pos 100 in chunk, Off 8, Len 19 ("VALID_DATA_CONTENT_")
	slices := []chunk.Slice{
		{
			Pos:  100,
			ID:   1,
			Size: uint32(len(payload)),
			Off:  8,
			Len:  19,
		},
	}

	// Read exactly from 105 to 115 (10 bytes)
	result, err := chunk.ReconstituteRange("disk-1", slices, 105, 10, reader)
	require.NoError(t, err)
	require.Len(t, result, 10)

	// In payload: Off is 8, pos offset is 105 - 100 = 5.
	// So payload offset is 8 + 5 = 13.
	// Payload[13:23] = "DATA_CONTE"
	assert.Equal(t, payload[13:23], result)
}

func TestReconstituteRange_EdgeCasesAndErrors(t *testing.T) {
	reader := &mockSliceReader{
		data: map[uint64][]byte{
			1: []byte("TEST"),
		},
	}

	slices := []chunk.Slice{
		{Pos: 0, ID: 1, Size: 4, Off: 0, Len: 4},
	}

	// 1. Size 0 returns empty slice
	empty, err := chunk.ReconstituteRange("disk-1", slices, 0, 0, reader)
	require.NoError(t, err)
	assert.Empty(t, empty)

	// 2. Start + Size > ChunkSize
	_, err = chunk.ReconstituteRange("disk-1", slices, chunk.ChunkSize-10, 20, reader)
	assert.ErrorIs(t, err, chunk.ErrOutOfBounds)

	// 3. Slice not found in reader
	badSlices := []chunk.Slice{
		{Pos: 0, ID: 999, Size: 4, Off: 0, Len: 4},
	}
	_, err = chunk.ReconstituteRange("disk-1", badSlices, 0, 4, reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "slice 999 not found")

	// 4. Corrupted slice payload (shorter than required)
	corruptedReader := &mockSliceReader{
		data: map[uint64][]byte{
			2: []byte("AB"), // only 2 bytes, but slice len is 4
		},
	}
	shortSlice := []chunk.Slice{
		{Pos: 0, ID: 2, Size: 4, Off: 0, Len: 4},
	}
	_, err = chunk.ReconstituteRange("disk-1", shortSlice, 0, 4, corruptedReader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "corrupted slice payload")

	// 5. ReconstituteRangeTo with buffer too short
	bufTooShort := make([]byte, 5)
	err = chunk.ReconstituteRangeTo(bufTooShort, "disk-1", slices, 0, 10, reader)
	assert.ErrorIs(t, err, chunk.ErrTruncatedBuffer)
}

type mockRangeSliceReader struct {
	data             map[uint64][]byte
	readSliceCalls   int
	rangeSliceCalls  int
	lastRequestedOff uint32
	lastRequestedLen uint32
}

func (m *mockRangeSliceReader) ReadSlice(diskID string, sliceID uint64) ([]byte, error) {
	m.readSliceCalls++
	d, ok := m.data[sliceID]
	if !ok {
		return nil, fmt.Errorf("slice %d not found", sliceID)
	}
	return d, nil
}

func (m *mockRangeSliceReader) ReadSliceRange(diskID string, sliceID uint64, off uint32, length uint32) ([]byte, error) {
	m.rangeSliceCalls++
	m.lastRequestedOff = off
	m.lastRequestedLen = length
	d, ok := m.data[sliceID]
	if !ok {
		return nil, fmt.Errorf("slice %d not found", sliceID)
	}
	if int(off+length) > len(d) {
		return nil, fmt.Errorf("range out of bounds")
	}
	return d[off : off+length], nil
}

func TestReconstituteRange_RangeSliceReader(t *testing.T) {
	reader := &mockRangeSliceReader{
		data: map[uint64][]byte{
			1: []byte("0123456789ABCDEF0123456789ABCDEF"), // 32 bytes
		},
	}

	// Slice at chunk pos=100, length=32, slice offset=4
	slices := []chunk.Slice{
		{
			Pos:  100,
			ID:   1,
			Size: 32,
			Off:  4,
			Len:  20,
		},
	}

	// Read interval [105, 115): length 10
	// Intersects slice interval [100, 120) from 105 to 115.
	// Overlap start: 105, end: 115, overlapLen: 10
	// Slice offset within slice: s.Off + (105 - 100) = 4 + 5 = 9
	result, err := chunk.ReconstituteRange("disk-1", slices, 105, 10, reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("9ABCDEF012"), result)

	// Verify that ReadSliceRange was called instead of ReadSlice
	assert.Equal(t, 0, reader.readSliceCalls)
	assert.Equal(t, 1, reader.rangeSliceCalls)
	assert.Equal(t, uint32(9), reader.lastRequestedOff)
	assert.Equal(t, uint32(10), reader.lastRequestedLen)
}

