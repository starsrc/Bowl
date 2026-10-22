package disk

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/disk/internal/db"
)

// UpdateDiskUsage 更新磁盘容量使用账本（增量更新与下溢保护）。
func (s *service) UpdateDiskUsage(ctx context.Context, params UpdateDiskUsageParams) (*Disk, error) {
	// 1. 静态参数校验：校验 DiskID 格式合法性且非 Nil UUID，不查库且不开启事务。
	pgID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}

	var result *Disk

	// 2. 开启读写事务执行原子排他锁校验与账本更新。
	err = s.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 悲观排他锁锁定目标磁盘行。
		current, getErr := q.GetDiskForUpdate(ctx, pgID)
		if getErr != nil {
			if errors.Is(getErr, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get disk for update: %w", getErr)
		}

		// 变动量为 0 时幂等短路返回当前实体，避免多余写操作与写冲突。
		if params.DeltaBytes == 0 {
			result = toDomainDisk(current)
			return nil
		}

		var newUsed uint64

		if params.DeltaBytes > 0 {
			delta := uint64(params.DeltaBytes)
			maxBytes := uint64(current.MaxBytes)
			usedBytes := uint64(current.UsedBytes)

			// 检查是否超出容量配额上限。
			if usedBytes > maxBytes || delta > maxBytes-usedBytes {
				return ErrNoSpace
			}
			newUsed = usedBytes + delta
		} else {
			// DeltaBytes < 0：安全计算绝对扣减量，避免 math.MinInt64 取反溢出。
			var absDelta uint64
			if params.DeltaBytes == math.MinInt64 {
				absDelta = uint64(math.MaxInt64) + 1
			} else {
				absDelta = uint64(-params.DeltaBytes)
			}

			// 下溢保护：若扣减量超过当前已用量则钳位归零。
			if current.UsedBytes <= 0 || absDelta >= uint64(current.UsedBytes) {
				newUsed = 0
			} else {
				newUsed = uint64(current.UsedBytes) - absDelta
			}
		}

		// 写入更新后的用量账本。
		updated, updateErr := q.UpdateDiskUsedBytes(ctx, db.UpdateDiskUsedBytesParams{
			DiskID:    pgID,
			UsedBytes: int64(newUsed),
		})
		if updateErr != nil {
			return fmt.Errorf("update disk used bytes: %w", updateErr)
		}

		result = toDomainDisk(updated)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
