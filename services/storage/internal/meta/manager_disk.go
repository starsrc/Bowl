package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// parseDiskID 解析字符串形式的 UUID 为 pgtype.UUID，并拦截非法格式或全零 Nil UUID。
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

// GetDisk 根据磁盘 ID 查询虚拟磁盘详情与配额状态。
func (m *manager) GetDisk(ctx context.Context, diskID string) (*Disk, error) {
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}

	row, err := m.store.GetDisk(ctx, pgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return toDomainDisk(row), nil
}

// UpdateDisk 更新虚拟磁盘属性（名称、最大容量）。
// 在事务内通过 FOR UPDATE 行锁排他锁定磁盘，校验缩容水位线（new_max_bytes >= used_bytes）并原子持久化新配额与名称。
func (m *manager) UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error) {
	// 静态参数校验（无需触达数据库）：
	pgID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}

	if params.Name == nil && params.MaxBytes == nil {
		return nil, fmt.Errorf("%w: at least one of name or max_bytes must be set", ErrInvalidArgument)
	}

	if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		return nil, fmt.Errorf("%w: max_bytes must be in range [1, %d]", ErrInvalidArgument, int64(math.MaxInt64))
	}

	if params.Name != nil {
		if err := validateDiskName(*params.Name); err != nil {
			return nil, err
		}
	}

	var result *Disk
	err = m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		current, err := q.GetDiskForUpdate(ctx, pgID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get disk for update %s: %w", params.DiskID, err)
		}

		newName := current.Name
		if params.Name != nil {
			newName = *params.Name
		}

		newMaxBytes := uint64(current.MaxBytes)
		if params.MaxBytes != nil {
			newMaxBytes = *params.MaxBytes
		}

		// 缩容水位保护：严禁缩容至当前已用空间以下
		if params.MaxBytes != nil && *params.MaxBytes < uint64(current.UsedBytes) {
			return ErrInvalidShrink
		}

		updated, err := q.UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID:   pgID,
			Name:     newName,
			MaxBytes: int64(newMaxBytes),
		})
		if err != nil {
			return fmt.Errorf("update disk %s: %w", params.DiskID, err)
		}

		result = toDomainDisk(updated)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

