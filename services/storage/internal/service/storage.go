package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
	"github.com/starsrc/bowl/services/storage/internal/session"
)

// DefaultLeaseDuration defines the default write lease duration (10 seconds).
const DefaultLeaseDuration = session.DefaultLeaseTimeout

// DBExecutor abstracts database execution for schema management.
type DBExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// StorageService implements bowl.v1.StorageService.
type StorageService struct {
	bowlv1connect.UnimplementedStorageServiceHandler
	diskClient    bowlv1connect.DiskServiceClient
	db            DBExecutor
	sessions      session.Manager
	dataDir       string
	leaseDuration time.Duration
}

// Option configures StorageService parameters.
type Option func(*StorageService)

// WithLeaseDuration overrides the default write lease duration.
func WithLeaseDuration(d time.Duration) Option {
	return func(s *StorageService) {
		s.leaseDuration = d
	}
}

// NewStorageService constructs a new StorageService.
func NewStorageService(
	diskClient bowlv1connect.DiskServiceClient,
	db DBExecutor,
	sessions session.Manager,
	dataDir string,
	opts ...Option,
) *StorageService {
	svc := &StorageService{
		diskClient:    diskClient,
		db:            db,
		sessions:      sessions,
		dataDir:       dataDir,
		leaseDuration: DefaultLeaseDuration,
	}
	for _, opt := range opts {
		opt(svc)
	}
	return svc
}

// FormatDiskSchema formats the PostgreSQL schema name for a given virtual disk UUID.
func FormatDiskSchema(diskID uuid.UUID) string {
	return fmt.Sprintf("disk_%s", strings.ReplaceAll(diskID.String(), "-", "_"))
}

func toConnectError(err error) error {
	if err == nil {
		return nil
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connect.NewError(connectErr.Code(), connectErr.Message())
	}
	code := connect.CodeOf(err)
	if code != connect.CodeUnknown {
		return connect.NewError(code, err.Error())
	}
	return connect.Errorf(connect.CodeInternal, "%s", err.Error())
}

// Mount mounts a virtual disk, establishes a client session, verifies leases via DiskService,
// initializes per-disk schema and local chunk directory, and returns root Inode metadata.
func (s *StorageService) Mount(ctx context.Context, req *bowlv1.MountRequest) (*bowlv1.MountResponse, error) {
	diskIDStr := strings.TrimSpace(req.GetDiskId())
	if diskIDStr == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "disk_id is required")
	}

	parsedDiskUUID, err := uuid.Parse(diskIDStr)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid disk_id UUID format: %s", err)
	}

	clientID := strings.TrimSpace(req.GetClientId())
	if clientID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "client_id is required")
	}

	// ADR 0005: Synchronously invoke DiskService.AttachDisk to acquire client binding and lease
	attachResp, err := s.diskClient.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   parsedDiskUUID.String(),
		ClientId: clientID,
		ReadOnly: req.GetReadOnly(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	// ADR 0006: PostgreSQL Schema Isolation per disk
	schemaName := FormatDiskSchema(parsedDiskUUID)
	if s.db != nil {
		query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, schemaName)
		if _, err := s.db.Exec(ctx, query); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to create disk schema %s: %s", schemaName, err).WithCause(err)
		}
	}

	// ADR 0009: Local directory object storage driver for chunks
	if s.dataDir != "" {
		chunkDir := filepath.Join(s.dataDir, parsedDiskUUID.String())
		if err := os.MkdirAll(chunkDir, 0o755); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to create chunk directory %s: %s", chunkDir, err).WithCause(err)
		}
	}

	now := time.Now()
	leaseTimeout := s.leaseDuration
	isWriteLease := !req.GetReadOnly()
	var leaseExpire time.Time
	var leaseExpirePb *timestamppb.Timestamp
	if isWriteLease {
		leaseExpire = now.Add(leaseTimeout)
		leaseExpirePb = timestamppb.New(leaseExpire)
	}

	sessionID := uuid.New().String()
	sess := &session.Session{
		ID:             sessionID,
		DiskID:         parsedDiskUUID.String(),
		ClientID:       clientID,
		ReadOnly:       req.GetReadOnly(),
		IsWriteLease:   isWriteLease,
		LeaseExpiresAt: leaseExpire,
		CreatedAt:      now,
	}
	s.sessions.Create(sess)

	rootInode := &bowlv1.Inode{
		Ino:         1,
		ParentIno:   1,
		Type:        bowlv1.InodeType_INODE_TYPE_DIRECTORY,
		Mode:        0o755,
		Nlink:       2,
		Uid:         0,
		Gid:         0,
		Atime:       timestamppb.New(now),
		Mtime:       timestamppb.New(now),
		Ctime:       timestamppb.New(now),
		AttrTimeout: durationpb.New(1 * time.Second),
	}

	disk := attachResp.GetDisk()
	var statfs *bowlv1.Statfs
	if disk != nil && disk.GetStatfs() != nil {
		statfs = disk.GetStatfs()
	} else {
		var capacity, used, inodesQuota, inodesUsed uint64
		if disk != nil {
			capacity = disk.GetCapacityBytes()
			used = disk.GetUsedBytes()
			inodesQuota = disk.GetInodesQuota()
			inodesUsed = disk.GetInodesUsed()
		}
		blockSize := uint32(4096)
		var blocks, freeBlocks, filesFree uint64
		if capacity > 0 {
			blocks = capacity / uint64(blockSize)
			if capacity > used {
				freeBlocks = (capacity - used) / uint64(blockSize)
			}
		}
		if inodesQuota > 0 && inodesQuota > inodesUsed {
			filesFree = inodesQuota - inodesUsed
		}
		statfs = &bowlv1.Statfs{
			BlockSize:    blockSize,
			FragmentSize: blockSize,
			Blocks:       blocks,
			BlocksFree:   freeBlocks,
			BlocksAvail:  freeBlocks,
			Files:        inodesQuota,
			FilesFree:    filesFree,
			MaxNameLen:   255,
		}
	}

	return &bowlv1.MountResponse{
		SessionId:       sessionID,
		RootInode:       rootInode,
		Statfs:          statfs,
		IsWriteLease:    isWriteLease,
		LeaseExpireTime: leaseExpirePb,
		LeaseTimeout:    durationpb.New(leaseTimeout),
	}, nil
}

// Unmount unmounts a client session, releases write leases, synchronizes with DiskService, and removes the session.
func (s *StorageService) Unmount(ctx context.Context, req *bowlv1.UnmountRequest) (*bowlv1.UnmountResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}

	sess, found := s.sessions.Get(sessionID)
	if !found {
		if req.GetForce() {
			return &bowlv1.UnmountResponse{}, nil
		}
		return nil, connect.Errorf(connect.CodeNotFound, "session not found: %s", sessionID)
	}

	// ADR 0005: Synchronously invoke DiskService.DetachDisk to release client attachment and lease
	_, err := s.diskClient.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
		DiskId:   sess.DiskID,
		ClientId: sess.ClientID,
		Force:    req.GetForce(),
	})
	if err != nil && !req.GetForce() {
		return nil, toConnectError(err)
	}

	s.sessions.Delete(sessionID)

	return &bowlv1.UnmountResponse{}, nil
}

// KeepAlive handles heartbeat and write lease renewal for an active mount session.
func (s *StorageService) KeepAlive(ctx context.Context, req *bowlv1.KeepAliveRequest) (*bowlv1.KeepAliveResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}

	sess, err := s.sessions.RenewLease(sessionID)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return nil, connect.Errorf(connect.CodeNotFound, "session not found: %s", sessionID)
		}
		return nil, toConnectError(err)
	}

	var expirePb *timestamppb.Timestamp
	if !sess.LeaseExpiresAt.IsZero() {
		expirePb = timestamppb.New(sess.LeaseExpiresAt)
	}

	return &bowlv1.KeepAliveResponse{
		IsWriteLease:    sess.IsWriteLease,
		LeaseExpireTime: expirePb,
		LeaseRevocation: sess.Revoked,
	}, nil
}
