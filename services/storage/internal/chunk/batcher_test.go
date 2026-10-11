package chunk

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatObjectKey(t *testing.T) {
	diskID := "d4b4a11b-1234-4567-89ab-cdef01234567"

	tests := []struct {
		name        string
		chunkIndx   uint32
		sliceID     uint64
		blockIndx   uint32
		expectedKey string
	}{
		{
			name:        "chunk index 0",
			chunkIndx:   0,
			sliceID:     1,
			blockIndx:   0,
			expectedKey: fmt.Sprintf("chunks/%s/000/0/1_0", diskID),
		},
		{
			name:        "chunk index 999",
			chunkIndx:   999,
			sliceID:     10,
			blockIndx:   1,
			expectedKey: fmt.Sprintf("chunks/%s/000/999/10_1", diskID),
		},
		{
			name:        "chunk index 1000",
			chunkIndx:   1000,
			sliceID:     100,
			blockIndx:   2,
			expectedKey: fmt.Sprintf("chunks/%s/001/1000/100_2", diskID),
		},
		{
			name:        "chunk index 2542",
			chunkIndx:   2542,
			sliceID:     9999,
			blockIndx:   3,
			expectedKey: fmt.Sprintf("chunks/%s/002/2542/9999_3", diskID),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := FormatObjectKey(diskID, tc.chunkIndx, tc.sliceID, tc.blockIndx)
			assert.Equal(t, tc.expectedKey, key)

			// Symmetrical roundtrip parse
			parsedDiskID, parsedChunkIndx, parsedSliceID, parsedBlockIndx, err := ParseObjectKey(key)
			require.NoError(t, err)
			assert.Equal(t, diskID, parsedDiskID)
			assert.Equal(t, tc.chunkIndx, parsedChunkIndx)
			assert.Equal(t, tc.sliceID, parsedSliceID)
			assert.Equal(t, tc.blockIndx, parsedBlockIndx)
		})
	}
}

func TestParseObjectKey_Errors(t *testing.T) {
	invalidKeys := []string{
		"",
		"invalid/path",
		"chunks/disk1/000/0",
		"chunks/disk1/000/0/invalid",
		"chunks/disk1/000/abc/1_0",
		"chunks/disk1/000/0/abc_0",
		"chunks/disk1/000/0/1_abc",
	}
	for _, key := range invalidKeys {
		_, _, _, _, err := ParseObjectKey(key)
		assert.Error(t, err, "expected error for key: %s", key)
	}
}

func TestBlockBatcher_SplitSlice(t *testing.T) {
	batcher := NewBlockBatcher()
	diskID := "disk-uuid-1234"
	chunkIndx := uint32(5)
	sliceID := uint64(42)

	t.Run("empty slice payload", func(t *testing.T) {
		blocks := batcher.SplitSlice(diskID, chunkIndx, sliceID, []byte{})
		assert.Empty(t, blocks)
	})

	t.Run("small slice under 4MB (128KB)", func(t *testing.T) {
		data := bytes.Repeat([]byte("A"), 128*1024)
		blocks := batcher.SplitSlice(diskID, chunkIndx, sliceID, data)

		require.Len(t, blocks, 1)
		b0 := blocks[0]
		assert.Equal(t, diskID, b0.DiskID)
		assert.Equal(t, chunkIndx, b0.ChunkIndx)
		assert.Equal(t, sliceID, b0.SliceID)
		assert.Equal(t, uint32(0), b0.BlockIndx)
		assert.Equal(t, uint32(0), b0.Offset)
		assert.Equal(t, uint32(128*1024), uint32(len(b0.Data)))
		assert.Equal(t, FormatObjectKey(diskID, chunkIndx, sliceID, 0), b0.Key)
		assert.Equal(t, data, b0.Data)
	})

	t.Run("exact 4MB block", func(t *testing.T) {
		data := make([]byte, BlockSize)
		data[0] = 0xAA
		data[BlockSize-1] = 0xBB
		blocks := batcher.SplitSlice(diskID, chunkIndx, sliceID, data)

		require.Len(t, blocks, 1)
		assert.Equal(t, uint32(0), blocks[0].BlockIndx)
		assert.Equal(t, BlockSize, len(blocks[0].Data))
		assert.Equal(t, FormatObjectKey(diskID, chunkIndx, sliceID, 0), blocks[0].Key)
	})

	t.Run("multiple blocks (9MB = 4MB + 4MB + 1MB)", func(t *testing.T) {
		totalSize := 9 * 1024 * 1024
		data := make([]byte, totalSize)
		// fill with pattern
		for i := range data {
			data[i] = byte(i % 251)
		}

		blocks := batcher.SplitSlice(diskID, chunkIndx, sliceID, data)
		require.Len(t, blocks, 3)

		// Block 0: 4MB
		assert.Equal(t, uint32(0), blocks[0].BlockIndx)
		assert.Equal(t, uint32(0), blocks[0].Offset)
		assert.Equal(t, BlockSize, len(blocks[0].Data))
		assert.Equal(t, data[:BlockSize], blocks[0].Data)
		assert.Equal(t, FormatObjectKey(diskID, chunkIndx, sliceID, 0), blocks[0].Key)

		// Block 1: 4MB
		assert.Equal(t, uint32(1), blocks[1].BlockIndx)
		assert.Equal(t, uint32(BlockSize), blocks[1].Offset)
		assert.Equal(t, BlockSize, len(blocks[1].Data))
		assert.Equal(t, data[BlockSize:2*BlockSize], blocks[1].Data)
		assert.Equal(t, FormatObjectKey(diskID, chunkIndx, sliceID, 1), blocks[1].Key)

		// Block 2: 1MB
		assert.Equal(t, uint32(2), blocks[2].BlockIndx)
		assert.Equal(t, uint32(2*BlockSize), blocks[2].Offset)
		assert.Equal(t, 1024*1024, len(blocks[2].Data))
		assert.Equal(t, data[2*BlockSize:], blocks[2].Data)
		assert.Equal(t, FormatObjectKey(diskID, chunkIndx, sliceID, 2), blocks[2].Key)
	})

	t.Run("full 64MB chunk", func(t *testing.T) {
		// Test partitioning of 64MB without allocating full 64MB in test (use custom block size)
		customBatcher := NewBlockBatcher(1024) // 1KB blocks
		data := make([]byte, 3500)             // 3.5 KB -> 4 blocks: 1024, 1024, 1024, 428
		blocks := customBatcher.SplitSlice(diskID, chunkIndx, sliceID, data)
		require.Len(t, blocks, 4)
		assert.Equal(t, 1024, len(blocks[0].Data))
		assert.Equal(t, 1024, len(blocks[1].Data))
		assert.Equal(t, 1024, len(blocks[2].Data))
		assert.Equal(t, 428, len(blocks[3].Data))
	})
}

func TestBlockBatcher_SplitSliceFromStaging(t *testing.T) {
	tempDir := t.TempDir()
	staging := NewStagingManager(tempDir)
	batcher := NewBlockBatcher()

	diskID := "disk-test-staging"
	sliceID := staging.NextSliceID()
	data := bytes.Repeat([]byte("hello staging block batcher "), 100) // ~2.8KB

	err := staging.WriteSlice(diskID, sliceID, data)
	require.NoError(t, err)

	blocks, err := batcher.SplitSliceFromStaging(staging, diskID, 0, sliceID)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	assert.Equal(t, data, blocks[0].Data)
	assert.Equal(t, FormatObjectKey(diskID, 0, sliceID, 0), blocks[0].Key)

	// Non-existent slice should error
	_, err = batcher.SplitSliceFromStaging(staging, diskID, 0, 999999)
	assert.Error(t, err)
}
