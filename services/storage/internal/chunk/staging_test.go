package chunk_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/chunk"
)

func TestStagingManager_NextSliceID(t *testing.T) {
	tempDir := t.TempDir()
	mgr := chunk.NewStagingManager(tempDir)

	// Sequential generation
	id1 := mgr.NextSliceID()
	id2 := mgr.NextSliceID()
	id3 := mgr.NextSliceID()
	assert.Equal(t, uint64(1), id1)
	assert.Equal(t, uint64(2), id2)
	assert.Equal(t, uint64(3), id3)

	// Concurrent generation uniqueness
	const numGoroutines = 100
	const numPerGoroutine = 50
	var wg sync.WaitGroup
	ids := make(chan uint64, numGoroutines*numPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numPerGoroutine; j++ {
				ids <- mgr.NextSliceID()
			}
		}()
	}

	wg.Wait()
	close(ids)

	seen := make(map[uint64]bool)
	for id := range ids {
		assert.False(t, seen[id], "slice ID must be globally unique: %d", id)
		seen[id] = true
	}
	assert.Equal(t, numGoroutines*numPerGoroutine, len(seen))
}

func TestStagingManager_WriteAndReadSlice(t *testing.T) {
	tempDir := t.TempDir()
	mgr := chunk.NewStagingManager(tempDir)

	diskID := "test-disk-uuid-1"
	sliceID := mgr.NextSliceID()
	payload := []byte("hello nvme staging world! this is chunk slice data.")

	expectedPath := filepath.Join(tempDir, diskID, "slice_1.bin")
	assert.Equal(t, expectedPath, mgr.SlicePath(diskID, sliceID))

	// Write slice
	err := mgr.WriteSlice(diskID, sliceID, payload)
	require.NoError(t, err)

	// Verify file exists on disk
	fi, err := os.Stat(expectedPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(payload)), fi.Size())

	// Read slice back via manager
	data, err := mgr.ReadSlice(diskID, sliceID)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(payload, data))
}

func TestStagingManager_DeleteSlice(t *testing.T) {
	tempDir := t.TempDir()
	mgr := chunk.NewStagingManager(tempDir)

	diskID := "test-disk-uuid-2"
	sliceID := mgr.NextSliceID()
	payload := []byte("data to be deleted")

	err := mgr.WriteSlice(diskID, sliceID, payload)
	require.NoError(t, err)

	// Verify file exists
	path := mgr.SlicePath(diskID, sliceID)
	_, err = os.Stat(path)
	require.NoError(t, err)

	// Delete slice
	err = mgr.DeleteSlice(diskID, sliceID)
	require.NoError(t, err)

	// Verify file is removed
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))

	// Subsequent ReadSlice fails
	_, err = mgr.ReadSlice(diskID, sliceID)
	assert.Error(t, err)
}

func TestStagingManager_ReadNonExistentSlice(t *testing.T) {
	tempDir := t.TempDir()
	mgr := chunk.NewStagingManager(tempDir)

	diskID := "test-disk-uuid-3"
	_, err := mgr.ReadSlice(diskID, 999999)
	assert.Error(t, err)
}
