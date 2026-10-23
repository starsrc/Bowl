package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// validateDiskName 校验磁盘名称合法性。
// 优先校验超长（> MaxDiskNameBytes），后校验是否包含空字节（'\x00'）。
func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("%w: disk name length %d exceeds max %d", ErrNameTooLong, len(name), MaxDiskNameBytes)
	}
	if strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: disk name contains null byte", ErrInvalidArgument)
	}
	return nil
}

// toDomainDisk 将数据库持久层 db.Disk 行模型转换为领域层 Disk 实体。
func toDomainDisk(row db.Disk) *Disk {
	var diskID string
	if row.DiskID.Valid {
		diskID = row.DiskID.String()
	}
	return &Disk{
		DiskID:     diskID,
		Name:       row.Name,
		RootIno:    uint64(row.RootIno),
		MaxBytes:   uint64(row.MaxBytes),
		UsedBytes:  uint64(row.UsedBytes),
		UsedInodes: uint64(row.UsedInodes),
		CreatedAt:  row.CreatedAt.Time,
		UpdatedAt:  row.UpdatedAt.Time,
	}
}

// CreateDisk 创建新的虚拟磁盘实体并原子创建其根目录 Inode。
func (m *manager) CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error) {
	// 静态参数校验（无需触达数据库）：
	// 1. 数值边界优先：max_bytes 必须在 1 到 math.MaxInt64 之间
	if params.MaxBytes == 0 || params.MaxBytes > math.MaxInt64 {
		return nil, fmt.Errorf("%w: max_bytes must be between 1 and %d, got %d", ErrInvalidArgument, int64(math.MaxInt64), params.MaxBytes)
	}

	// 2. 字符串边界校验：优先校验超长，后校验空字节
	if err := validateDiskName(params.Name); err != nil {
		return nil, err
	}

	// 3. 生成 UUIDv7 磁盘主键
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate uuidv7: %w", err)
	}

	diskID := pgtype.UUID{
		Bytes: id,
		Valid: true,
	}

	// 4. 单次读写事务内原子创建根目录 Inode 与 Disk 记录
	var createdDisk db.Disk
	err = m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		rootInode, err := q.CreateRootInode(ctx, db.CreateRootInodeParams{
			Mode:  int32(S_IFDIR | 0o755),
			Nlink: 2,
			Uid:   0,
			Gid:   0,
			Rdev:  0,
			Size:  int64(DefaultDirSize),
		})
		if err != nil {
			return fmt.Errorf("create root inode: %w", err)
		}

		row, err := q.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     params.Name,
			RootIno:  rootInode.Ino,
			MaxBytes: int64(params.MaxBytes),
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return fmt.Errorf("%w: root_ino %d already exists", ErrAlreadyExists, rootInode.Ino)
			}
			return fmt.Errorf("create disk: %w", err)
		}

		createdDisk = row
		return nil
	})
	if err != nil {
		return nil, err
	}

	return toDomainDisk(createdDisk), nil
}
