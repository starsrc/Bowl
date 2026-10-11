package chunk_test

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/chunk"
)

func TestEncodeDecodeSlice_RoundTrip(t *testing.T) {
	original := chunk.Slice{
		Pos:  1024,
		ID:   9876543210,
		Size: 4096,
		Off:  512,
		Len:  2048,
	}

	buf := chunk.EncodeSlice(original)
	require.Len(t, buf, chunk.SliceBytes)

	// Verify big-endian byte layout explicitly against specification
	assert.Equal(t, uint32(1024), binary.BigEndian.Uint32(buf[0:4]), "Pos at bytes 0..3")
	assert.Equal(t, uint64(9876543210), binary.BigEndian.Uint64(buf[4:12]), "ID at bytes 4..11")
	assert.Equal(t, uint32(4096), binary.BigEndian.Uint32(buf[12:16]), "Size at bytes 12..15")
	assert.Equal(t, uint32(512), binary.BigEndian.Uint32(buf[16:20]), "Off at bytes 16..19")
	assert.Equal(t, uint32(2048), binary.BigEndian.Uint32(buf[20:24]), "Len at bytes 20..23")

	decoded, err := chunk.DecodeSlices(buf)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	assert.Equal(t, original, decoded[0])
}

func TestAppendSlice_Multiple(t *testing.T) {
	slices := []chunk.Slice{
		{Pos: 0, ID: 101, Size: 1024, Off: 0, Len: 1024},
		{Pos: 1024, ID: 102, Size: 2048, Off: 0, Len: 2048},
		{Pos: 3072, ID: 103, Size: 4096, Off: 512, Len: 1024},
	}

	var buf []byte
	for _, s := range slices {
		buf = chunk.AppendSlice(buf, s)
	}

	require.Len(t, buf, 3*chunk.SliceBytes)

	decoded, err := chunk.DecodeSlices(buf)
	require.NoError(t, err)
	assert.Equal(t, slices, decoded)
}

func TestDecodeSlices_Empty(t *testing.T) {
	decoded, err := chunk.DecodeSlices(nil)
	require.NoError(t, err)
	assert.Empty(t, decoded)

	decoded, err = chunk.DecodeSlices([]byte{})
	require.NoError(t, err)
	assert.Empty(t, decoded)
}

func TestDecodeSlices_TruncatedBuffer(t *testing.T) {
	testLengths := []int{1, 10, 23, 25, 47}
	for _, l := range testLengths {
		buf := make([]byte, l)
		_, err := chunk.DecodeSlices(buf)
		require.Error(t, err, "length %d should fail", l)
		assert.ErrorIs(t, err, chunk.ErrTruncatedBuffer)
	}

	// Also test DecodeSlice with truncated buffer
	_, err := chunk.DecodeSlice([]byte{1, 2, 3})
	require.Error(t, err)
	assert.ErrorIs(t, err, chunk.ErrTruncatedBuffer)
}

func TestSlice_Validate(t *testing.T) {
	t.Run("valid slice", func(t *testing.T) {
		s := chunk.Slice{
			Pos:  0,
			ID:   1,
			Size: 1024,
			Off:  0,
			Len:  1024,
		}
		assert.NoError(t, s.Validate())
	})

	t.Run("valid slice at chunk boundary", func(t *testing.T) {
		s := chunk.Slice{
			Pos:  chunk.ChunkSize - 4096,
			ID:   1,
			Size: 4096,
			Off:  0,
			Len:  4096,
		}
		assert.NoError(t, s.Validate())
	})

	t.Run("out of bounds - slice payload offset + len > size", func(t *testing.T) {
		s := chunk.Slice{
			Pos:  0,
			ID:   1,
			Size: 1000,
			Off:  600,
			Len:  500, // 600 + 500 = 1100 > 1000
		}
		err := s.Validate()
		require.Error(t, err)
		assert.ErrorIs(t, err, chunk.ErrOutOfBounds)
	})

	t.Run("out of bounds - chunk boundary exceeded", func(t *testing.T) {
		s := chunk.Slice{
			Pos:  chunk.ChunkSize - 100,
			ID:   1,
			Size: 1000,
			Off:  0,
			Len:  200, // Pos + Len = ChunkSize + 100 > ChunkSize
		}
		err := s.Validate()
		require.Error(t, err)
		assert.ErrorIs(t, err, chunk.ErrOutOfBounds)
	})

	t.Run("out of bounds - integer overflow protection", func(t *testing.T) {
		s := chunk.Slice{
			Pos:  0xFFFFFFF0,
			ID:   1,
			Size: 1000,
			Off:  0,
			Len:  100,
		}
		err := s.Validate()
		require.Error(t, err)
		assert.ErrorIs(t, err, chunk.ErrOutOfBounds)

		sOverflowPayload := chunk.Slice{
			Pos:  0,
			ID:   1,
			Size: 1000,
			Off:  0xFFFFFFF0,
			Len:  100,
		}
		err = sOverflowPayload.Validate()
		require.Error(t, err)
		assert.ErrorIs(t, err, chunk.ErrOutOfBounds)
	})
}

func TestDecodeSlices_CorruptedBytes(t *testing.T) {
	// Corrupted slice where Off + Len > Size
	badSlice := chunk.Slice{
		Pos:  0,
		ID:   1,
		Size: 500,
		Off:  300,
		Len:  400,
	}
	buf := chunk.EncodeSlice(badSlice)

	_, err := chunk.DecodeSlices(buf)
	require.Error(t, err)
	assert.ErrorIs(t, err, chunk.ErrOutOfBounds)

	// Single DecodeSlice also returns error
	_, err = chunk.DecodeSlice(buf)
	require.Error(t, err)
	assert.ErrorIs(t, err, chunk.ErrOutOfBounds)
}

func TestDecodeSlices_PerformanceUnderOneMicrosecondPerSlice(t *testing.T) {
	const count = 1000
	var buf []byte
	for i := 0; i < count; i++ {
		buf = chunk.AppendSlice(buf, chunk.Slice{
			Pos:  uint32(i * 1024),
			ID:   uint64(i + 1),
			Size: 1024,
			Off:  0,
			Len:  1024,
		})
	}

	const iterations = 200
	start := time.Now()
	for i := 0; i < iterations; i++ {
		decoded, err := chunk.DecodeSlices(buf)
		require.NoError(t, err)
		require.Len(t, decoded, count)
	}
	totalDuration := time.Since(start)
	avgPerSlice := totalDuration / time.Duration(iterations*count)

	t.Logf("Average deserialization time per slice: %v", avgPerSlice)
	assert.Less(t, avgPerSlice, 1*time.Microsecond, "deserialization time must be < 1µs per slice")
}

func BenchmarkDecodeSlice(b *testing.B) {
	s := chunk.Slice{Pos: 1024, ID: 12345, Size: 4096, Off: 0, Len: 4096}
	buf := chunk.EncodeSlice(s)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = chunk.DecodeSlice(buf)
	}
}

func BenchmarkDecodeSlices_100(b *testing.B) {
	var buf []byte
	for i := 0; i < 100; i++ {
		buf = chunk.AppendSlice(buf, chunk.Slice{
			Pos:  uint32(i * 1024),
			ID:   uint64(i + 1),
			Size: 1024,
			Off:  0,
			Len:  1024,
		})
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = chunk.DecodeSlices(buf)
	}
}

func BenchmarkDecodeSlices_1000(b *testing.B) {
	var buf []byte
	for i := 0; i < 1000; i++ {
		buf = chunk.AppendSlice(buf, chunk.Slice{
			Pos:  uint32(i * 1024),
			ID:   uint64(i + 1),
			Size: 1024,
			Off:  0,
			Len:  1024,
		})
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = chunk.DecodeSlices(buf)
	}
}



