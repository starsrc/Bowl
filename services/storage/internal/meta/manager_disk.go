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

// DeleteDisk 级联删除指定虚拟磁盘实体并强制回收其全部元数据节点。
// 在单次读写事务内排他锁定目标磁盘并删除 Disk 记录，同时按 root_ino 批量强制删除该磁盘下的全部 Inode（依赖外键 ON DELETE CASCADE 自动级联清除关联的 entries、inode_xattrs、file_handles、file_locks）。
// 事务提交成功后，主动关闭并清理内存中对应 Inode 的阻塞等待通道，唤醒正在等待锁的协程。
func (m *manager) DeleteDisk(ctx context.Context, diskID string) error {
	// 静态参数校验（无需触达数据库）：非法格式或全零 Nil UUID 直接返回 ErrInvalidArgument
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return err
	}

	var deletedInos []int64
	err = m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		// 1. 获取并排他锁定目标 Disk 行
		diskRow, err := q.GetDiskForUpdate(ctx, pgID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get disk for update %s: %w", diskID, err)
		}

		// 2. 删除该 Disk 行
		rows, err := q.DeleteDisk(ctx, pgID)
		if err != nil {
			return fmt.Errorf("delete disk %s: %w", diskID, err)
		}
		if rows == 0 {
			return ErrNotFound
		}

		// 3. 按 root_ino 批量强制删除该磁盘下的全部 Inode（依赖外键 ON DELETE CASCADE 自动级联清除关联项）
		deletedInos, err = q.DeleteInodesByRootIno(ctx, diskRow.RootIno)
		if err != nil {
			return fmt.Errorf("delete inodes by root_ino %d for disk %s: %w", diskRow.RootIno, diskID, err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// 4. 事务成功提交后，主动清理内存中的锁等待通道，唤醒等待该磁盘下所有节点的阻塞协程
	if len(deletedInos) > 0 {
		m.cleanLockWaitersForInos(deletedInos)
	}

	return nil
}

// StatFS 查询指定 Inode 所属虚拟磁盘的字节容量与 POSIX 4096 字节块统计信息（对接 Linux FUSE FUSE_STATFS = 17 协议）。
// 接收目标节点的 uint64 ino，按 root_ino O(1) 查询所属 Disk 配额并返回 4096 字节块及已用 Inode 统计信息。
func (m *manager) StatFS(ctx context.Context, ino uint64) (*StatFSResult, error) {
	// 静态参数校验（无需触达数据库）：ino 必须在 [1, math.MaxInt64] 区间内
	if ino == 0 || ino > math.MaxInt64 {
		return nil, fmt.Errorf("%w: ino must be between 1 and %d, got %d", ErrInvalidArgument, int64(math.MaxInt64), ino)
	}

	// 1. 查询目标 Inode（支持已解链但仍打开的孤儿节点 nlink == 0）
	inode, err := m.store.GetInode(ctx, int64(ino))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get inode %d for statfs: %w", ino, err)
	}
	if inode.RootIno <= 0 {
		return nil, ErrNotFound
	}

	// 2. 根据 root_ino 查询其绑定的 Disk 实体
	row, err := m.store.GetDiskByRootIno(ctx, inode.RootIno)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get disk by root_ino %d: %w", inode.RootIno, err)
	}

	// 3. 计算容量与 4096 字节块统计信息
	totalBytes := uint64(row.MaxBytes)
	usedBytes := uint64(row.UsedBytes)

	var availBytes uint64
	if totalBytes > usedBytes {
		availBytes = totalBytes - usedBytes
	}

	const blockSize uint64 = DefaultStatFSBlockSize
	blocks := totalBytes / blockSize
	if blocks == 0 && totalBytes > 0 {
		blocks = 1
	}

	bavail := availBytes / blockSize
	bfree := bavail

	var files uint64
	if row.UsedInodes > 0 {
		files = uint64(row.UsedInodes)
	}

	return &StatFSResult{
		TotalBytes: totalBytes,
		UsedBytes:  usedBytes,
		AvailBytes: availBytes,
		Bsize:      DefaultStatFSBlockSize,
		Frsize:     DefaultStatFSBlockSize,
		Blocks:     blocks,
		Bfree:      bfree,
		Bavail:     bavail,
		Files:      files,
		Ffree:      0,
		Namelen:    MaxDiskNameBytes,
	}, nil
}


