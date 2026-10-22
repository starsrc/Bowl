package disk

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/disk/internal/db"
)

// UpdateDisk 更新磁盘属性（名称、最大容量）。
func (s *service) UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error) {
	// 静态参数校验（不查库，不开启事务）
	pgID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if pgID.Bytes == [16]byte{} {
		return nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}

	if params.Name == nil && params.MaxBytes == nil {
		return nil, fmt.Errorf("%w: at least one of name or max_bytes must be set", ErrInvalidArgument)
	}

	if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		return nil, fmt.Errorf("%w: max_bytes must be in range [1, %d]", ErrInvalidArgument, math.MaxInt64)
	}

	if params.Name != nil {
		if len(*params.Name) > MaxDiskNameBytes {
			return nil, fmt.Errorf("%w: disk name exceeds %d bytes", ErrNameTooLong, MaxDiskNameBytes)
		}
		if strings.IndexByte(*params.Name, 0) >= 0 {
			return nil, fmt.Errorf("%w: disk name cannot contain NUL byte", ErrInvalidArgument)
		}
	}

	var result *Disk
	err = s.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
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
