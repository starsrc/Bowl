package disk

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/fx"

	"github.com/starsrc/bowl/services/disk/internal/db"
)

// Manager 定义磁盘领域层管理器接口。
type Manager interface {
	CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error)
	GetDisk(ctx context.Context, diskID string) (*Disk, error)
	UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error)
	DeleteDisk(ctx context.Context, diskID string) error
	StatFS(ctx context.Context, diskID string) (*StatFSResult, error)
	UpdateDiskUsage(ctx context.Context, params UpdateDiskUsageParams) (*Disk, error)
}

// service 实现 Manager 接口。
type service struct {
	store db.Store
}

var _ Manager = (*service)(nil)

// New 创建 Manager 实例。
func New(store db.Store) Manager {
	return &service{store: store}
}

// Module 提供 disk 依赖注入模块。
var Module = fx.Module("disk",
	fx.Provide(New),
)

// parseDiskID 解析字符串形式的 UUID 为 pgtype.UUID，并拦截全零 Nil UUID。
func parseDiskID(diskID string) (pgtype.UUID, error) {
	var id pgtype.UUID
	if err := id.Scan(diskID); err != nil || !id.Valid {
		return pgtype.UUID{}, fmt.Errorf("%w: invalid disk id %q", ErrInvalidArgument, diskID)
	}
	if id.Bytes == [16]byte{} {
		return pgtype.UUID{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	return id, nil
}

// validateDiskName 校验磁盘名称的长度上限与非法空字节。
func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("%w: disk name length %d exceeds max %d", ErrNameTooLong, len(name), MaxDiskNameBytes)
	}
	if strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: disk name contains null byte", ErrInvalidArgument)
	}
	return nil
}

// toDomainDisk 将数据库持久层 Disk 行模型转换为领域层 Disk 实体。
func toDomainDisk(row db.Disk) *Disk {
	var diskID string
	if row.DiskID.Valid {
		diskID = row.DiskID.String()
	}
	return &Disk{
		DiskID:    diskID,
		Name:      row.Name,
		RootIno:   uint64(row.RootIno),
		MaxBytes:  uint64(row.MaxBytes),
		UsedBytes: uint64(row.UsedBytes),
		CreatedAt: row.CreatedAt.Time,
		UpdatedAt: row.UpdatedAt.Time,
	}
}
