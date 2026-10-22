package disk

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/disk/internal/db"
)

// CreateDisk 创建新磁盘并初始化元数据。
func (s *service) CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error) {
	// 静态参数校验（无需查询数据库）：
	// 1. 数值边界校验
	if params.RootIno == 0 || params.RootIno > math.MaxInt64 {
		return nil, fmt.Errorf("%w: root_ino must be between 1 and %d, got %d", ErrInvalidArgument, int64(math.MaxInt64), params.RootIno)
	}
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

	// 4. 持久化至数据库
	row, err := s.store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:   diskID,
		Name:     params.Name,
		RootIno:  int64(params.RootIno),
		MaxBytes: int64(params.MaxBytes),
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, fmt.Errorf("%w: root_ino %d already exists", ErrAlreadyExists, params.RootIno)
		}
		return nil, fmt.Errorf("create disk: %w", err)
	}

	return toDomainDisk(row), nil
}
