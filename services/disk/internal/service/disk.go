package service

import (
	"context"
	"errors"
	"strings"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
	"github.com/starsrc/bowl/services/disk/internal/db"
)

// DiskService implements bowl.v1.DiskService.
type DiskService struct {
	bowlv1connect.UnimplementedDiskServiceHandler
	store db.Store
}

// NewDiskService creates a new DiskService instance.
func NewDiskService(store db.Store) *DiskService {
	return &DiskService{
		store: store,
	}
}

// CreateDisk creates a new virtual disk entity with initial status DISK_STATUS_READY.
func (s *DiskService) CreateDisk(ctx context.Context, req *bowlv1.CreateDiskRequest) (*bowlv1.CreateDiskResponse, error) {
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "disk name cannot be empty")
	}

	if req.GetCapacityBytes() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "capacity_bytes must be greater than 0")
	}

	diskID := uuid.New()
	row, err := s.store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          name,
		CapacityBytes: int64(req.GetCapacityBytes()),
		InodesQuota:   int64(req.GetInodesQuota()),
		Status:        int16(bowlv1.DiskStatus_DISK_STATUS_READY),
	})
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to create disk: %s", err).WithCause(err)
	}

	return &bowlv1.CreateDiskResponse{
		Disk: toProtoDisk(row),
	}, nil
}

// GetDisk retrieves full virtual disk metadata by disk UUID.
func (s *DiskService) GetDisk(ctx context.Context, req *bowlv1.GetDiskRequest) (*bowlv1.GetDiskResponse, error) {
	diskIDStr := strings.TrimSpace(req.GetDiskId())
	if diskIDStr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "disk_id is required")
	}

	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid disk_id UUID format: %s", err)
	}

	row, err := s.store.GetDisk(ctx, pgtype.UUID{Bytes: parsedUUID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "disk not found: %s", diskIDStr)
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to get disk: %s", err).WithCause(err)
	}

	return &bowlv1.GetDiskResponse{
		Disk: toProtoDisk(row),
	}, nil
}

func toProtoDisk(row db.Disk) *bowlv1.Disk {
	disk := &bowlv1.Disk{
		DiskId:        uuid.UUID(row.DiskID.Bytes).String(),
		Name:          row.Name,
		CapacityBytes: uint64(row.CapacityBytes),
		UsedBytes:     uint64(row.UsedBytes),
		InodesQuota:   uint64(row.InodesQuota),
		InodesUsed:    uint64(row.InodesUsed),
		Status:        bowlv1.DiskStatus(row.Status),
	}
	if row.CreatedAt.Valid {
		disk.CreatedAt = timestamppb.New(row.CreatedAt.Time)
	}
	if row.UpdatedAt.Valid {
		disk.UpdatedAt = timestamppb.New(row.UpdatedAt.Time)
	}
	return disk
}
