package disk

import "context"

// Manager 定义磁盘领域层管理器接口。
type Manager interface {
	CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error)
	GetDisk(ctx context.Context, diskID string) (*Disk, error)
	UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error)
	DeleteDisk(ctx context.Context, diskID string) error
	StatFS(ctx context.Context, diskID string) (*StatFSResult, error)
	UpdateDiskUsage(ctx context.Context, params UpdateDiskUsageParams) (*Disk, error)
}
