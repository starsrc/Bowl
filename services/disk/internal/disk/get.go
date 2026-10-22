package disk

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// GetDisk 根据磁盘 ID 查询磁盘详情。
func (s *service) GetDisk(ctx context.Context, diskID string) (*Disk, error) {
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}

	row, err := s.store.GetDisk(ctx, pgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return toDomainDisk(row), nil
}
