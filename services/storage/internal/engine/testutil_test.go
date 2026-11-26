package engine

import (
	"context"
	"math"
	"time"

	"github.com/google/uuid"
)

type testDiskOptions struct {
	Dir          string
	DiskID       string
	Name         string
	MaxBytes     uint64
	BlockSize    uint32
	MaxPackSize  uint32
	Now          func() time.Time
	DiskIdleTTL  time.Duration
	GCInterval   time.Duration
	EnableGCWake bool
}

type testDisk struct {
	*DiskStore
	node   *NodeStore
	diskID uuid.UUID
}

func (td *testDisk) Close() error {
	if td == nil || td.node == nil {
		return nil
	}
	return td.node.Close(context.Background())
}

func openTestDisk(opts testDiskOptions) (*testDisk, error) {
	gcMode := GCModeDisabled
	if opts.GCInterval > 0 {
		gcMode = GCModePeriodic
	} else if opts.EnableGCWake {
		gcMode = GCModeWakeOnly
	}

	blockSize := opts.BlockSize
	if blockSize == 0 && opts.MaxPackSize > 0 && opts.MaxPackSize < DefaultBlockSize {
		blockSize = MinBlockSize
	}

	node, err := OpenNode(NodeOptions{
		DataDir:     opts.Dir,
		BlockSize:   blockSize,
		MaxPackSize: opts.MaxPackSize,
		Now:         opts.Now,
		DiskIdleTTL: opts.DiskIdleTTL,
		GCMode:      gcMode,
		GCInterval:  opts.GCInterval,
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	page, err := node.ListDisks(ctx, ListDisksQuery{PageSize: 1})
	if err != nil {
		_ = node.Close(ctx)
		return nil, err
	}

	var diskID uuid.UUID
	if len(page.Disks) > 0 {
		diskID, err = uuid.Parse(page.Disks[0].DiskID)
		if err != nil {
			_ = node.Close(ctx)
			return nil, err
		}
		if opts.MaxBytes > 0 && opts.MaxBytes != page.Disks[0].MaxBytes {
			maxB := opts.MaxBytes
			if _, uErr := node.UpdateDisk(ctx, diskID, nil, &maxB); uErr != nil {
				_ = node.Close(ctx)
				return nil, uErr
			}
		}
	} else {
		maxBytes := opts.MaxBytes
		if maxBytes == 0 {
			maxBytes = math.MaxInt64
		}
		info, cErr := node.CreateDisk(ctx, opts.Name, maxBytes)
		if cErr != nil {
			_ = node.Close(ctx)
			return nil, cErr
		}
		diskID, err = uuid.Parse(info.DiskID)
		if err != nil {
			_ = node.Close(ctx)
			return nil, err
		}
	}

	var ds *DiskStore
	if err = node.WithDisk(ctx, diskID, func(s *DiskStore) error {
		ds = s
		return nil
	}); err != nil {
		_ = node.Close(ctx)
		return nil, err
	}

	return &testDisk{
		DiskStore: ds,
		node:      node,
		diskID:    diskID,
	}, nil
}
