package chunk

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// StagingManager manages local NVMe staging cache files for chunk slices.
// Slices are stored locally as `<staging_dir>/<disk_id>/slice_<slice_id>.bin`.
type StagingManager struct {
	dir         string
	nextSliceID atomic.Uint64
}

// NewStagingManager creates a new StagingManager for the given base directory.
// An optional initial slice ID can be passed to initialize the ID generator counter.
func NewStagingManager(dir string, initialSliceID ...uint64) *StagingManager {
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "bowl_staging")
	}
	mgr := &StagingManager{
		dir: dir,
	}
	if len(initialSliceID) > 0 {
		mgr.nextSliceID.Store(initialSliceID[0])
	}
	return mgr
}

// Dir returns the base staging directory path.
func (m *StagingManager) Dir() string {
	return m.dir
}

// NextSliceID atomically generates a monotonically increasing unique slice identifier.
func (m *StagingManager) NextSliceID() uint64 {
	return m.nextSliceID.Add(1)
}

// UpdateMaxSliceID ensures nextSliceID is at least maxID so subsequent allocations do not collide.
func (m *StagingManager) UpdateMaxSliceID(maxID uint64) {
	for {
		cur := m.nextSliceID.Load()
		if maxID <= cur {
			break
		}
		if m.nextSliceID.CompareAndSwap(cur, maxID) {
			break
		}
	}
}

// SlicePath returns the filesystem path for a specific slice within a virtual disk.
// Format: `<staging_dir>/<disk_id>/slice_<slice_id>.bin`
func (m *StagingManager) SlicePath(diskID string, sliceID uint64) string {
	return filepath.Join(m.dir, diskID, fmt.Sprintf("slice_%d.bin", sliceID))
}

// WriteSlice writes slice payload bytes to the local NVMe staging directory.
func (m *StagingManager) WriteSlice(diskID string, sliceID uint64, data []byte) error {
	path := m.SlicePath(diskID, sliceID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("staging: failed to create directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("staging: failed to write slice file: %w", err)
	}
	return nil
}

// ReadSlice reads slice payload bytes from the local NVMe staging directory.
func (m *StagingManager) ReadSlice(diskID string, sliceID uint64) ([]byte, error) {
	path := m.SlicePath(diskID, sliceID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("staging: failed to read slice file: %w", err)
	}
	return data, nil
}

// DeleteSlice removes a slice payload file from the local NVMe staging directory.
func (m *StagingManager) DeleteSlice(diskID string, sliceID uint64) error {
	path := m.SlicePath(diskID, sliceID)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("staging: failed to delete slice file: %w", err)
	}
	return nil
}

// SyncDisk flushes staging directory metadata for a virtual disk to persistent storage.
func (m *StagingManager) SyncDisk(diskID string) error {
	if m == nil || m.dir == "" || diskID == "" {
		return nil
	}
	dirPath := filepath.Join(m.dir, diskID)
	f, err := os.Open(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("staging: failed to sync disk directory: %w", err)
	}
	defer f.Close()
	return f.Sync()
}

