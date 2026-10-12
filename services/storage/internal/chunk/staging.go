package chunk

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

// StagingManager 管理切片本地 NVMe Staging 暂存缓存文件。
// 本地暂存文件命名规范为：<staging_dir>/<disk_id>/slice_<slice_id>.bin
type StagingManager struct {
	dir         string
	nextSliceID atomic.Uint64
}

// NewStagingManager 构造指定目录的 StagingManager 实例。
// 支持传入可选的初始 sliceID 用于初始化原子序列发生器计数。
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

// Dir 返回 Staging 暂存根目录路径。
func (m *StagingManager) Dir() string {
	return m.dir
}

// NextSliceID 原子递增生成全局唯一的切片唯一标识。
func (m *StagingManager) NextSliceID() uint64 {
	return m.nextSliceID.Add(1)
}

// UpdateMaxSliceID 确保 nextSliceID 至少不低于 maxID，防止重启后产生切片标识冲突。
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

// SlicePath 返回虚拟磁盘内特定切片的本地文件绝对路径。
// 格式为：<staging_dir>/<disk_id>/slice_<slice_id>.bin
func (m *StagingManager) SlicePath(diskID string, sliceID uint64) string {
	return filepath.Join(m.dir, diskID, fmt.Sprintf("slice_%d.bin", sliceID))
}

// WriteSlice 将切片载荷数据极速写入本地 NVMe Staging 暂存文件。
func (m *StagingManager) WriteSlice(diskID string, sliceID uint64, data []byte) error {
	path := m.SlicePath(diskID, sliceID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("staging: 创建暂存目录失败: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("staging: 写入切片暂存文件失败: %w", err)
	}
	return nil
}

// ReadSlice 从本地 NVMe Staging 暂存目录读取切片完整数据。
func (m *StagingManager) ReadSlice(diskID string, sliceID uint64) ([]byte, error) {
	path := m.SlicePath(diskID, sliceID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("staging: 读取切片暂存文件失败: %w", err)
	}
	return data, nil
}

// ReadSliceRange 从本地 NVMe Staging 暂存文件按偏移与长度读取子区间载荷 [off, off+length)。
func (m *StagingManager) ReadSliceRange(diskID string, sliceID uint64, off uint32, length uint32) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	path := m.SlicePath(diskID, sliceID)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("staging: 打开切片暂存文件失败: %w", err)
	}
	defer f.Close()

	buf := make([]byte, length)
	n, err := f.ReadAt(buf, int64(off))
	if err != nil && (!errors.Is(err, io.EOF) || n < int(length)) {
		return nil, fmt.Errorf("staging: 读取切片暂存区间 [%d, %d) 失败: %w", off, off+length, err)
	}
	return buf[:n], nil
}

// DeleteSlice 从本地 NVMe Staging 暂存目录移除切片文件。
func (m *StagingManager) DeleteSlice(diskID string, sliceID uint64) error {
	path := m.SlicePath(diskID, sliceID)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("staging: 删除切片暂存文件失败: %w", err)
	}
	return nil
}

// SyncDisk 将虚拟磁盘对应的本地 Staging 暂存目录元数据与文件数据持久化刷盘。
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
		return fmt.Errorf("staging: 刷盘同步磁盘暂存目录失败: %w", err)
	}
	defer f.Close()
	return f.Sync()
}
