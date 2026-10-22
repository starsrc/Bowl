package disk

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// StatFS 获取磁盘文件系统统计状态。
func (s *service) StatFS(ctx context.Context, diskID string) (*StatFSResult, error) {
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}
	if pgID.Bytes == [16]byte{} {
		return nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}

	row, err := s.store.GetDisk(ctx, pgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

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

	return &StatFSResult{
		TotalBytes: totalBytes,
		UsedBytes:  usedBytes,
		AvailBytes: availBytes,
		Bsize:      DefaultStatFSBlockSize,
		Frsize:     DefaultStatFSBlockSize,
		Blocks:     blocks,
		Bfree:      bfree,
		Bavail:     bavail,
		Files:      0,
		Ffree:      0,
		Namelen:    MaxDiskNameBytes,
	}, nil
}
