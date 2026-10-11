package service

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"strconv"
	"strings"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
		Disk: buildDiskProto(row, nil),
	}, nil
}

// GetDisk retrieves full virtual disk metadata by disk UUID, including reader and writer attachments.
func (s *DiskService) GetDisk(ctx context.Context, req *bowlv1.GetDiskRequest) (*bowlv1.GetDiskResponse, error) {
	diskIDStr := strings.TrimSpace(req.GetDiskId())
	if diskIDStr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "disk_id is required")
	}

	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid disk_id UUID format: %s", err)
	}

	diskUUID := pgtype.UUID{Bytes: parsedUUID, Valid: true}
	row, err := s.store.GetDisk(ctx, diskUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "disk not found: %s", diskIDStr)
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to get disk: %s", err).WithCause(err)
	}

	attachments, err := s.store.ListDiskAttachmentsByDiskID(ctx, diskUUID)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to list disk attachments: %s", err).WithCause(err)
	}

	return &bowlv1.GetDiskResponse{
		Disk: buildDiskProto(row, attachments),
	}, nil
}

// UpdateDisk updates dynamic configuration of a virtual disk (name, capacity_bytes, inodes_quota)
// with FieldMask support and downsizing protection against used_bytes.
func (s *DiskService) UpdateDisk(ctx context.Context, req *bowlv1.UpdateDiskRequest) (*bowlv1.UpdateDiskResponse, error) {
	diskIDStr := strings.TrimSpace(req.GetDiskId())
	if diskIDStr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "disk_id is required")
	}

	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid disk_id UUID format: %s", err)
	}

	diskUUID := pgtype.UUID{Bytes: parsedUUID, Valid: true}

	var (
		updateName     bool
		updateCapacity bool
		updateInodes   bool
	)

	mask := req.GetFieldMask()
	if mask == nil || len(mask.GetPaths()) == 0 {
		updateName = true
		updateCapacity = true
		updateInodes = true
	} else {
		for _, p := range mask.GetPaths() {
			switch p {
			case "name":
				updateName = true
			case "capacity_bytes", "capacityBytes":
				updateCapacity = true
			case "inodes_quota", "inodesQuota":
				updateInodes = true
			default:
				return nil, connect.Errorf(connect.CodeInvalidArgument, "unsupported field_mask path: %s", p)
			}
		}
	}

	var newName string
	if updateName {
		newName = strings.TrimSpace(req.GetName())
		if newName == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, "disk name cannot be empty")
		}
	}

	var newCapacity uint64
	if updateCapacity {
		newCapacity = req.GetCapacityBytes()
		if newCapacity == 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, "capacity_bytes must be greater than 0")
		}
		if newCapacity > math.MaxInt64 {
			return nil, connect.NewError(connect.CodeInvalidArgument, "capacity_bytes exceeds maximum allowed value")
		}
	}

	if updateInodes {
		if req.GetInodesQuota() > math.MaxInt64 {
			return nil, connect.NewError(connect.CodeInvalidArgument, "inodes_quota exceeds maximum allowed value")
		}
	}

	var updatedRow db.Disk
	var attachments []db.DiskAttachment

	err = s.store.ExecTx(ctx, func(q db.Querier) error {
		existing, err := q.GetDisk(ctx, diskUUID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return connect.Errorf(connect.CodeNotFound, "disk not found: %s", diskIDStr)
			}
			return connect.Errorf(connect.CodeInternal, "failed to get disk: %s", err).WithCause(err)
		}

		targetName := existing.Name
		if updateName {
			targetName = newName
		}

		targetCapacity := existing.CapacityBytes
		if updateCapacity {
			if int64(newCapacity) < existing.UsedBytes {
				return connect.Errorf(
					connect.CodeInvalidArgument,
					"cannot downsize disk capacity below used bytes (%d < %d)",
					newCapacity,
					existing.UsedBytes,
				)
			}
			targetCapacity = int64(newCapacity)
		}

		targetInodes := existing.InodesQuota
		if updateInodes {
			targetInodes = int64(req.GetInodesQuota())
		}

		updatedRow, err = q.UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID:        diskUUID,
			Name:          targetName,
			CapacityBytes: targetCapacity,
			InodesQuota:   targetInodes,
		})
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to update disk: %s", err).WithCause(err)
		}

		attachments, err = q.ListDiskAttachmentsByDiskID(ctx, diskUUID)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to list disk attachments: %s", err).WithCause(err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &bowlv1.UpdateDiskResponse{
		Disk: buildDiskProto(updatedRow, attachments),
	}, nil
}

// ListDisks retrieves a paginated list of virtual disks with optional status filtering.
func (s *DiskService) ListDisks(ctx context.Context, req *bowlv1.ListDisksRequest) (*bowlv1.ListDisksResponse, error) {
	if req.GetFilterStatus() < bowlv1.DiskStatus_DISK_STATUS_UNSPECIFIED || req.GetFilterStatus() > bowlv1.DiskStatus_DISK_STATUS_ERROR {
		return nil, connect.NewError(connect.CodeInvalidArgument, "invalid filter_status")
	}

	pageSize := req.GetPageSize()
	if pageSize <= 0 {
		pageSize = 20
	} else if pageSize > 100 {
		pageSize = 100
	}

	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, err
	}

	statusFilter := int16(req.GetFilterStatus())

	totalCount, err := s.store.CountDisks(ctx, statusFilter)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to count disks: %s", err).WithCause(err)
	}

	rows, err := s.store.ListDisks(ctx, db.ListDisksParams{
		StatusFilter: statusFilter,
		OffsetVal:    int32(offset),
		LimitVal:     int32(pageSize),
	})
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to list disks: %s", err).WithCause(err)
	}

	disks := make([]*bowlv1.Disk, 0, len(rows))
	for _, row := range rows {
		attachments, err := s.store.ListDiskAttachmentsByDiskID(ctx, row.DiskID)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to list disk attachments: %s", err).WithCause(err)
		}
		disks = append(disks, buildDiskProto(row, attachments))
	}

	var nextPageToken string
	if offset+int64(len(disks)) < totalCount {
		nextPageToken = strconv.FormatInt(offset+int64(len(disks)), 10)
	}

	return &bowlv1.ListDisksResponse{
		Disks:         disks,
		NextPageToken: nextPageToken,
		TotalCount:    totalCount,
	}, nil
}

// AttachDisk binds a client to a virtual disk in read-only or exclusive read-write mode.
func (s *DiskService) AttachDisk(ctx context.Context, req *bowlv1.AttachDiskRequest) (*bowlv1.AttachDiskResponse, error) {
	diskIDStr := strings.TrimSpace(req.GetDiskId())
	if diskIDStr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "disk_id is required")
	}

	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid disk_id UUID format: %s", err)
	}

	clientID := strings.TrimSpace(req.GetClientId())
	if clientID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "client_id is required")
	}

	diskUUID := pgtype.UUID{Bytes: parsedUUID, Valid: true}
	var resDisk *bowlv1.Disk

	err = s.store.ExecTx(ctx, func(q db.Querier) error {
		diskRow, err := q.GetDisk(ctx, diskUUID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return connect.Errorf(connect.CodeNotFound, "disk not found: %s", diskIDStr)
			}
			return connect.Errorf(connect.CodeInternal, "failed to get disk: %s", err).WithCause(err)
		}

		if diskRow.Status == int16(bowlv1.DiskStatus_DISK_STATUS_ERROR) {
			return connect.Errorf(connect.CodeFailedPrecondition, "cannot attach disk in error status: %s", diskIDStr)
		}

		_, err = q.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
			ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
			DiskID:   diskUUID,
			ClientID: clientID,
			ReadOnly: req.GetReadOnly(),
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				if pgErr.ConstraintName == "idx_disk_attachments_writer" {
					return connect.Errorf(connect.CodeFailedPrecondition, "disk %s already has an exclusive writer attached", diskIDStr)
				}
				if pgErr.ConstraintName == "idx_disk_attachments_client" {
					return connect.Errorf(connect.CodeAlreadyExists, "client %s is already attached to disk %s", clientID, diskIDStr)
				}
				return connect.Errorf(connect.CodeFailedPrecondition, "attachment constraint violation: %s", pgErr.Message)
			}
			return connect.Errorf(connect.CodeInternal, "failed to create disk attachment: %s", err).WithCause(err)
		}

		if diskRow.Status == int16(bowlv1.DiskStatus_DISK_STATUS_READY) {
			diskRow, err = q.UpdateDiskStatus(ctx, db.UpdateDiskStatusParams{
				DiskID: diskUUID,
				Status: int16(bowlv1.DiskStatus_DISK_STATUS_ATTACHED),
			})
			if err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to update disk status: %s", err).WithCause(err)
			}
		}

		attachments, err := q.ListDiskAttachmentsByDiskID(ctx, diskUUID)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to list disk attachments: %s", err).WithCause(err)
		}

		resDisk = buildDiskProto(diskRow, attachments)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &bowlv1.AttachDiskResponse{
		Disk: resDisk,
	}, nil
}

// DetachDisk unbinds a client from a virtual disk, restoring status to READY when no attachments remain.
func (s *DiskService) DetachDisk(ctx context.Context, req *bowlv1.DetachDiskRequest) (*bowlv1.DetachDiskResponse, error) {
	diskIDStr := strings.TrimSpace(req.GetDiskId())
	if diskIDStr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "disk_id is required")
	}

	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid disk_id UUID format: %s", err)
	}

	clientID := strings.TrimSpace(req.GetClientId())
	if clientID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "client_id is required")
	}

	diskUUID := pgtype.UUID{Bytes: parsedUUID, Valid: true}
	var resDisk *bowlv1.Disk

	err = s.store.ExecTx(ctx, func(q db.Querier) error {
		diskRow, err := q.GetDisk(ctx, diskUUID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return connect.Errorf(connect.CodeNotFound, "disk not found: %s", diskIDStr)
			}
			return connect.Errorf(connect.CodeInternal, "failed to get disk: %s", err).WithCause(err)
		}

		rowsAffected, err := q.DeleteDiskAttachment(ctx, db.DeleteDiskAttachmentParams{
			DiskID:   diskUUID,
			ClientID: clientID,
		})
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to delete disk attachment: %s", err).WithCause(err)
		}

		if rowsAffected == 0 && !req.GetForce() {
			return connect.Errorf(connect.CodeNotFound, "client %s is not attached to disk %s", clientID, diskIDStr)
		}

		remainingCount, err := q.CountDiskAttachmentsByDiskID(ctx, diskUUID)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to count disk attachments: %s", err).WithCause(err)
		}

		if remainingCount == 0 && diskRow.Status == int16(bowlv1.DiskStatus_DISK_STATUS_ATTACHED) {
			diskRow, err = q.UpdateDiskStatus(ctx, db.UpdateDiskStatusParams{
				DiskID: diskUUID,
				Status: int16(bowlv1.DiskStatus_DISK_STATUS_READY),
			})
			if err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to update disk status: %s", err).WithCause(err)
			}
		}

		attachments, err := q.ListDiskAttachmentsByDiskID(ctx, diskUUID)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to list disk attachments: %s", err).WithCause(err)
		}

		resDisk = buildDiskProto(diskRow, attachments)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &bowlv1.DetachDiskResponse{
		Disk: resDisk,
	}, nil
}

// GetDiskStatfs retrieves filesystem statfs statistics and quota usage for a virtual disk.
func (s *DiskService) GetDiskStatfs(ctx context.Context, req *bowlv1.GetDiskStatfsRequest) (*bowlv1.GetDiskStatfsResponse, error) {
	diskIDStr := strings.TrimSpace(req.GetDiskId())
	if diskIDStr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "disk_id is required")
	}

	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid disk_id UUID format: %s", err)
	}

	diskUUID := pgtype.UUID{Bytes: parsedUUID, Valid: true}
	row, err := s.store.GetDisk(ctx, diskUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "disk not found: %s", diskIDStr)
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to get disk: %s", err).WithCause(err)
	}

	return &bowlv1.GetDiskStatfsResponse{
		DiskId: uuid.UUID(row.DiskID.Bytes).String(),
		Statfs: calculateStatfs(row),
	}, nil
}

func calculateStatfs(disk db.Disk) *bowlv1.Statfs {
	const blockSize = 4096

	var blocks uint64
	if disk.CapacityBytes > 0 {
		blocks = uint64(disk.CapacityBytes / blockSize)
	}

	var blocksFree uint64
	if disk.CapacityBytes > disk.UsedBytes {
		blocksFree = uint64((disk.CapacityBytes - disk.UsedBytes) / blockSize)
	}

	var files uint64
	if disk.InodesQuota > 0 {
		files = uint64(disk.InodesQuota)
	}

	var filesFree uint64
	if disk.InodesQuota > 0 && disk.InodesQuota > disk.InodesUsed {
		filesFree = uint64(disk.InodesQuota - disk.InodesUsed)
	}

	return &bowlv1.Statfs{
		BlockSize:    blockSize,
		FragmentSize: blockSize,
		Blocks:       blocks,
		BlocksFree:   blocksFree,
		BlocksAvail:  blocksFree,
		Files:        files,
		FilesFree:    filesFree,
		MaxNameLen:   255,
	}
}

func buildDiskProto(row db.Disk, attachments []db.DiskAttachment) *bowlv1.Disk {
	disk := &bowlv1.Disk{
		DiskId:        uuid.UUID(row.DiskID.Bytes).String(),
		Name:          row.Name,
		CapacityBytes: uint64(row.CapacityBytes),
		UsedBytes:     uint64(row.UsedBytes),
		InodesQuota:   uint64(row.InodesQuota),
		InodesUsed:    uint64(row.InodesUsed),
		Status:        bowlv1.DiskStatus(row.Status),
		Statfs:        calculateStatfs(row),
	}
	if row.CreatedAt.Valid {
		disk.CreatedAt = timestamppb.New(row.CreatedAt.Time)
	}
	if row.UpdatedAt.Valid {
		disk.UpdatedAt = timestamppb.New(row.UpdatedAt.Time)
	}
	for _, att := range attachments {
		if att.ReadOnly {
			disk.ReaderClientIds = append(disk.ReaderClientIds, att.ClientID)
		} else {
			disk.WriterClientId = att.ClientID
		}
	}
	return disk
}

func parsePageToken(token string) (int64, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, nil
	}

	// Try base64 standard decoding
	if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
		if val, err := strconv.ParseInt(string(decoded), 10, 64); err == nil {
			if val < 0 || val > math.MaxInt32 {
				return 0, connect.NewError(connect.CodeInvalidArgument, "invalid page_token: offset out of bounds")
			}
			return val, nil
		}
	}

	// Try base64 URL decoding
	if decoded, err := base64.RawURLEncoding.DecodeString(token); err == nil {
		if val, err := strconv.ParseInt(string(decoded), 10, 64); err == nil {
			if val < 0 || val > math.MaxInt32 {
				return 0, connect.NewError(connect.CodeInvalidArgument, "invalid page_token: offset out of bounds")
			}
			return val, nil
		}
	}

	// Fallback to direct decimal integer string
	val, err := strconv.ParseInt(token, 10, 64)
	if err != nil || val < 0 || val > math.MaxInt32 {
		return 0, connect.NewError(connect.CodeInvalidArgument, "invalid page_token: must be a non-negative integer or valid base64 offset")
	}

	return val, nil
}
