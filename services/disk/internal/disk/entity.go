package disk

import (
	"time"
)

// 领域常量定义。
const (
	// DefaultStatFSBlockSize 定义 StatFS 默认换算块大小（4096 字节）。
	DefaultStatFSBlockSize = 4096
	// MaxDiskNameBytes 定义磁盘名称最大字节长度（255 字节）。
	MaxDiskNameBytes = 255
)

// Disk 描述磁盘领域实体。
type Disk struct {
	DiskID    string
	Name      string
	RootIno   uint64
	MaxBytes  uint64
	UsedBytes uint64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// StatFSResult 描述磁盘文件系统统计状态。
type StatFSResult struct {
	TotalBytes uint64
	UsedBytes  uint64
	AvailBytes uint64
	Bsize      uint32
	Frsize     uint32
	Blocks     uint64
	Bfree      uint64
	Bavail     uint64
	Files      uint64
	Ffree      uint64
	Namelen    uint32
}

// CreateDiskParams 描述创建磁盘入参。
type CreateDiskParams struct {
	Name     string
	RootIno  uint64
	MaxBytes uint64
}

// UpdateDiskParams 描述更新磁盘入参。
type UpdateDiskParams struct {
	DiskID   string
	Name     *string
	MaxBytes *uint64
}

// UpdateDiskUsageParams 描述更新磁盘容量账本入参。
type UpdateDiskUsageParams struct {
	DiskID     string
	DeltaBytes int64
}
