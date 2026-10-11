package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
	"github.com/starsrc/bowl/services/storage/internal/chunk"
	"github.com/starsrc/bowl/services/storage/internal/session"
)

// DefaultLeaseDuration defines the default write lease duration (10 seconds).
const DefaultLeaseDuration = session.DefaultLeaseTimeout

// OpenFileHandle represents an open file handle registered to a session.
type OpenFileHandle struct {
	Handle       *bowlv1.FileHandle
	SessionID    string
	DiskID       string
	CreatedAt    time.Time
	mu           sync.Mutex
	driverHandle HandleContext
}

// OpenDirHandle represents an open directory handle registered to a session.
type OpenDirHandle struct {
	Handle    *bowlv1.FileHandle
	DH        uint64
	Ino       uint64
	SessionID string
	DiskID    string
	Flags     uint32
	CreatedAt time.Time
}

// DBExecutor abstracts database execution for schema and metadata management.
type DBExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// StorageService implements bowl.v1.StorageService.
type StorageService struct {
	bowlv1connect.UnimplementedStorageServiceHandler
	diskClient     bowlv1connect.DiskServiceClient
	db             DBExecutor
	sessions       session.Manager
	dataDir        string
	driver         FileStorageDriver
	leaseDuration  time.Duration
	nextFH         atomic.Uint64
	openHandles    sync.Map // uint64 -> *OpenFileHandle
	nextDH         atomic.Uint64
	openDirHandles sync.Map // uint64 -> *OpenDirHandle
	lockMgr        *LockManager
	lockTimeout    time.Duration
}

// Option configures StorageService parameters.
type Option func(*StorageService)

// WithLeaseDuration overrides the default write lease duration.
func WithLeaseDuration(d time.Duration) Option {
	return func(s *StorageService) {
		s.leaseDuration = d
	}
}

// WithLockTimeout overrides the default lock acquisition timeout.
func WithLockTimeout(d time.Duration) Option {
	return func(s *StorageService) {
		s.lockTimeout = d
	}
}

// WithFileStorageDriver overrides the default file storage driver.
func WithFileStorageDriver(driver FileStorageDriver) Option {
	return func(s *StorageService) {
		s.driver = driver
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
		driver:        NewLegacyFileDriver(dataDir),
		leaseDuration: DefaultLeaseDuration,
		lockMgr:       NewLockManager(sessions),
		lockTimeout:   DefaultLockTimeout,
	}
	for _, opt := range opts {
		opt(svc)
	}
	if revoker, ok := sessions.(interface{ SetOnRevoke(func(*session.Session)) }); ok {
		revoker.SetOnRevoke(func(sess *session.Session) {
			svc.cleanupSessionResources(sess.ID)
		})
	}
	return svc
}

// RecoverStaging triggers NVMe staging recovery if ChunkDriver is configured.
func (s *StorageService) RecoverStaging(ctx context.Context) (*chunk.RecoveryReport, error) {
	if cd, ok := s.driver.(*ChunkDriver); ok {
		return cd.RecoverStaging(ctx)
	}
	return &chunk.RecoveryReport{}, nil
}

// FormatDiskSchema formats the PostgreSQL schema name for a given virtual disk UUID.
func FormatDiskSchema(diskID uuid.UUID) string {
	return fmt.Sprintf("disk_%s", strings.ReplaceAll(diskID.String(), "-", "_"))
}

func toConnectError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err.Error())
	}
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err.Error())
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
		if err := s.ensureDiskSchema(ctx, schemaName); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to initialize disk schema %s: %s", schemaName, err).WithCause(err)
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

	var rootInode *bowlv1.Inode
	if s.db != nil {
		var err error
		rootInode, err = s.getInode(ctx, schemaName, 1)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to query root inode for disk %s: %s", schemaName, err).WithCause(err)
		}
	} else {
		rootInode = &bowlv1.Inode{
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
	}

	statfs := buildStatfs(attachResp.GetDisk())

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
			s.cleanupSessionResources(sessionID)
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

	// ADR 0011: Clean up session-bound open handles and buffers on unmount
	s.cleanupSessionResources(sessionID)

	return &bowlv1.UnmountResponse{}, nil
}

// cleanupSessionResources reclaims all open file handles, directory handles, and flushes buffers for the given session (ADR 0011).
func (s *StorageService) cleanupSessionResources(sessionID string) {
	s.openHandles.Range(func(key, value any) bool {
		h, ok := value.(*OpenFileHandle)
		if ok && h.SessionID == sessionID {
			s.openHandles.Delete(key)
			h.mu.Lock()
			driverHandle := h.driverHandle
			h.driverHandle = nil
			h.mu.Unlock()
			if driverHandle != nil && s.driver != nil {
				_ = s.driver.Flush(context.Background(), driverHandle, h.DiskID, h.Handle.GetIno())
				_ = s.driver.Close(context.Background(), driverHandle)
			}
		}
		return true
	})

	s.openDirHandles.Range(func(key, value any) bool {
		h, ok := value.(*OpenDirHandle)
		if ok && h.SessionID == sessionID {
			s.openDirHandles.Delete(key)
		}
		return true
	})

	// Release session locks on unmount / eviction
	if s.lockMgr != nil {
		s.lockMgr.ReleaseSessionLocks(sessionID)
	}
}

// CleanupSessionResources reclaims all open file handles, directory handles, and flushes buffers for the given session.
func (s *StorageService) CleanupSessionResources(sessionID string) {
	s.cleanupSessionResources(sessionID)
}

// CheckExpiredLeases scans active sessions for expired write leases, marks them revoked,
// and automatically reclaims all orphaned handles and file buffers for those sessions.
func (s *StorageService) CheckExpiredLeases(now time.Time) []*session.Session {
	revoked := s.sessions.CheckExpiredLeases(now)
	for _, sess := range revoked {
		s.cleanupSessionResources(sess.ID)
	}
	return revoked
}

// OpenFileHandlesCount returns the number of active open file handles for a given session.
func (s *StorageService) OpenFileHandlesCount(sessionID string) int {
	count := 0
	s.openHandles.Range(func(_, value any) bool {
		if h, ok := value.(*OpenFileHandle); ok && h.SessionID == sessionID {
			count++
		}
		return true
	})
	return count
}

// OpenDirHandlesCount returns the number of active open directory handles for a given session.
func (s *StorageService) OpenDirHandlesCount(sessionID string) int {
	count := 0
	s.openDirHandles.Range(func(_, value any) bool {
		if h, ok := value.(*OpenDirHandle); ok && h.SessionID == sessionID {
			count++
		}
		return true
	})
	return count
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

	if sess.Revoked {
		s.cleanupSessionResources(sessionID)
	}

	// Verify upstream disk existence (ADR 0004 & ADR 0005)
	if s.diskClient != nil {
		_, err := s.diskClient.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: sess.DiskID})
		if err != nil && connect.CodeOf(err) == connect.CodeNotFound {
			s.sessions.Delete(sessionID)
			s.cleanupSessionResources(sessionID)
			return &bowlv1.KeepAliveResponse{
				IsWriteLease:    false,
				LeaseRevocation: true,
			}, nil
		}
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

// buildStatfs constructs a Statfs proto message from a Disk proto entity or defaults.
func buildStatfs(disk *bowlv1.Disk) *bowlv1.Statfs {
	if disk != nil && disk.GetStatfs() != nil {
		return disk.GetStatfs()
	}
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
	return &bowlv1.Statfs{
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

// Statfs returns file system capacity, usage statistics, and Inode metadata counters.
func (s *StorageService) Statfs(ctx context.Context, req *bowlv1.StatfsRequest) (*bowlv1.StatfsResponse, error) {
	sess, _, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	var statfs *bowlv1.Statfs
	if s.diskClient != nil {
		statfsResp, err := s.diskClient.GetDiskStatfs(ctx, &bowlv1.GetDiskStatfsRequest{
			DiskId: sess.DiskID,
		})
		if err == nil && statfsResp.GetStatfs() != nil {
			statfs = statfsResp.GetStatfs()
		} else if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
			return nil, toConnectError(err)
		} else {
			diskResp, err := s.diskClient.GetDisk(ctx, &bowlv1.GetDiskRequest{
				DiskId: sess.DiskID,
			})
			if err != nil {
				return nil, toConnectError(err)
			}
			statfs = buildStatfs(diskResp.GetDisk())
		}
	} else {
		statfs = buildStatfs(nil)
	}

	return &bowlv1.StatfsResponse{
		Statfs: statfs,
	}, nil
}

// getDiskSchemaForSession retrieves the session and computes the PostgreSQL disk schema name.
func (s *StorageService) getDiskSchemaForSession(sessionID string) (*session.Session, string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}

	sess, found := s.sessions.Get(sessionID)
	if !found {
		return nil, "", connect.Errorf(connect.CodeNotFound, "session not found: %s", sessionID)
	}

	parsedDiskUUID, err := uuid.Parse(sess.DiskID)
	if err != nil {
		return nil, "", connect.Errorf(connect.CodeInternal, "invalid disk_id in session: %s", err)
	}

	return sess, FormatDiskSchema(parsedDiskUUID), nil
}

// ensureDiskSchema creates the per-disk PostgreSQL schema, table inodes, and initializes the root inode.
func (s *StorageService) ensureDiskSchema(ctx context.Context, schemaName string) error {
	if s.db == nil {
		return errors.New("database not initialized")
	}

	ddl := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS "%s";

CREATE TABLE IF NOT EXISTS "%s".inodes (
	ino BIGINT NOT NULL,
	parent BIGINT NOT NULL,
	name TEXT NOT NULL,
	type INT NOT NULL,
	mode INT NOT NULL,
	nlink INT NOT NULL,
	uid INT NOT NULL DEFAULT 0,
	gid INT NOT NULL DEFAULT 0,
	size BIGINT NOT NULL DEFAULT 0,
	blocks BIGINT NOT NULL DEFAULT 0,
	rdev BIGINT NOT NULL DEFAULT 0,
	atime TIMESTAMPTZ NOT NULL,
	mtime TIMESTAMPTZ NOT NULL,
	ctime TIMESTAMPTZ NOT NULL,
	target TEXT NOT NULL DEFAULT '',
	generation BIGINT NOT NULL DEFAULT 0,
	lookup_count BIGINT NOT NULL DEFAULT 1,
	PRIMARY KEY (parent, name)
);

ALTER TABLE "%s".inodes ADD COLUMN IF NOT EXISTS lookup_count BIGINT NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_inodes_ino ON "%s".inodes (ino);

CREATE TABLE IF NOT EXISTS "%s".xattrs (
	ino BIGINT NOT NULL,
	name TEXT NOT NULL,
	value BYTEA NOT NULL,
	PRIMARY KEY (ino, name)
);

CREATE TABLE IF NOT EXISTS "%s".chunks (
	ino BIGINT NOT NULL,
	indx INT NOT NULL,
	slices BYTEA NOT NULL,
	PRIMARY KEY (ino, indx)
);

CREATE TABLE IF NOT EXISTS "%s".slice_refs (
	id BIGINT PRIMARY KEY,
	size INT NOT NULL,
	refs INT NOT NULL DEFAULT 1
);
`, schemaName, schemaName, schemaName, schemaName, schemaName, schemaName, schemaName)

	if _, err := s.db.Exec(ctx, ddl); err != nil {
		return err
	}

	now := time.Now()
	insertRoot := fmt.Sprintf(`
INSERT INTO "%s".inodes (ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, lookup_count)
VALUES (1, 1, '/', $1, $2, 2, 0, 0, 4096, 8, 0, $3, $3, $3, 1)
ON CONFLICT (parent, name) DO NOTHING;
`, schemaName)

	if _, err := s.db.Exec(ctx, insertRoot, int(bowlv1.InodeType_INODE_TYPE_DIRECTORY), int(0o755), now); err != nil {
		return err
	}
	return nil
}

// getInode queries an Inode record by its inode number.
func (s *StorageService) getInode(ctx context.Context, schemaName string, ino uint64) (*bowlv1.Inode, error) {
	if s.db == nil {
		return nil, errors.New("database not initialized")
	}

	query := fmt.Sprintf(`
SELECT ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, target, generation
FROM "%s".inodes WHERE ino = $1 LIMIT 1;
`, schemaName)

	var (
		resIno        int64
		resParent     int64
		resName       string
		resType       int
		resMode       int
		resNlink      int
		resUid        int
		resGid        int
		resSize       int64
		resBlocks     int64
		resRdev       int64
		resAtime      time.Time
		resMtime      time.Time
		resCtime      time.Time
		resTarget     string
		resGeneration int64
	)

	err := s.db.QueryRow(ctx, query, int64(ino)).Scan(
		&resIno,
		&resParent,
		&resName,
		&resType,
		&resMode,
		&resNlink,
		&resUid,
		&resGid,
		&resSize,
		&resBlocks,
		&resRdev,
		&resAtime,
		&resMtime,
		&resCtime,
		&resTarget,
		&resGeneration,
	)
	if err != nil {
		return nil, err
	}

	return &bowlv1.Inode{
		Ino:         uint64(resIno),
		ParentIno:   uint64(resParent),
		Type:        bowlv1.InodeType(resType),
		Mode:        uint32(resMode),
		Uid:         uint32(resUid),
		Gid:         uint32(resGid),
		Size:        uint64(resSize),
		Nlink:       uint32(resNlink),
		Rdev:        uint32(resRdev),
		Atime:       timestamppb.New(resAtime),
		Mtime:       timestamppb.New(resMtime),
		Ctime:       timestamppb.New(resCtime),
		Target:      resTarget,
		Blocks:      uint64(resBlocks),
		Generation:  uint64(resGeneration),
		AttrTimeout: durationpb.New(1 * time.Second),
	}, nil
}

// getInodeByParentAndName queries an Inode record by parent inode and entry name.
func (s *StorageService) getInodeByParentAndName(ctx context.Context, schemaName string, parent uint64, name string) (*bowlv1.Inode, error) {
	if s.db == nil {
		return nil, errors.New("database not initialized")
	}

	if name == "." {
		return s.getInode(ctx, schemaName, parent)
	}
	if name == ".." {
		if parent == 1 {
			return s.getInode(ctx, schemaName, 1)
		}
		var parentOfParent int64
		query := fmt.Sprintf(`SELECT parent FROM "%s".inodes WHERE ino = $1;`, schemaName)
		if err := s.db.QueryRow(ctx, query, int64(parent)).Scan(&parentOfParent); err != nil {
			return nil, err
		}
		return s.getInode(ctx, schemaName, uint64(parentOfParent))
	}

	query := fmt.Sprintf(`
SELECT ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, target, generation
FROM "%s".inodes WHERE parent = $1 AND name = $2;
`, schemaName)

	var (
		resIno        int64
		resParent     int64
		resName       string
		resType       int
		resMode       int
		resNlink      int
		resUid        int
		resGid        int
		resSize       int64
		resBlocks     int64
		resRdev       int64
		resAtime      time.Time
		resMtime      time.Time
		resCtime      time.Time
		resTarget     string
		resGeneration int64
	)

	err := s.db.QueryRow(ctx, query, int64(parent), name).Scan(
		&resIno,
		&resParent,
		&resName,
		&resType,
		&resMode,
		&resNlink,
		&resUid,
		&resGid,
		&resSize,
		&resBlocks,
		&resRdev,
		&resAtime,
		&resMtime,
		&resCtime,
		&resTarget,
		&resGeneration,
	)
	if err != nil {
		return nil, err
	}

	return &bowlv1.Inode{
		Ino:         uint64(resIno),
		ParentIno:   uint64(resParent),
		Type:        bowlv1.InodeType(resType),
		Mode:        uint32(resMode),
		Uid:         uint32(resUid),
		Gid:         uint32(resGid),
		Size:        uint64(resSize),
		Nlink:       uint32(resNlink),
		Rdev:        uint32(resRdev),
		Atime:       timestamppb.New(resAtime),
		Mtime:       timestamppb.New(resMtime),
		Ctime:       timestamppb.New(resCtime),
		Target:      resTarget,
		Blocks:      uint64(resBlocks),
		Generation:  uint64(resGeneration),
		AttrTimeout: durationpb.New(1 * time.Second),
	}, nil
}

// Lookup looks up a directory entry by parent inode and name (FUSE_LOOKUP).
func (s *StorageService) Lookup(ctx context.Context, req *bowlv1.LookupRequest) (*bowlv1.LookupResponse, error) {
	if req.GetParentIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_ino must be greater than 0")
	}
	name := req.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "name is required")
	}

	_, schemaName, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	inode, err := s.getInodeByParentAndName(ctx, schemaName, req.GetParentIno(), name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "entry not found: parent %d, name %s", req.GetParentIno(), name)
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to lookup entry: %s", err)
	}

	if s.db != nil {
		updateQuery := fmt.Sprintf(`UPDATE "%s".inodes SET lookup_count = lookup_count + 1 WHERE ino = $1;`, schemaName)
		if _, err := s.db.Exec(ctx, updateQuery, int64(inode.GetIno())); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to increment lookup count: %s", err)
		}
	}

	return &bowlv1.LookupResponse{
		Inode:        inode,
		EntryTimeout: durationpb.New(1 * time.Second),
	}, nil
}

// GetAttr retrieves metadata attributes for an Inode (FUSE_GETATTR).
func (s *StorageService) GetAttr(ctx context.Context, req *bowlv1.GetAttrRequest) (*bowlv1.GetAttrResponse, error) {
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	_, schemaName, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	inode, err := s.getInode(ctx, schemaName, req.GetIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to get inode attributes: %s", err)
	}

	return &bowlv1.GetAttrResponse{
		Inode: inode,
	}, nil
}

// SetAttr modifies metadata attributes for an Inode (FUSE_SETATTR).
func (s *StorageService) SetAttr(ctx context.Context, req *bowlv1.SetAttrRequest) (*bowlv1.SetAttrResponse, error) {
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	if sess.ReadOnly || !sess.IsWriteLease || sess.Revoked {
		return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: session is read-only or lease expired")
	}

	inode, err := s.getInode(ctx, schemaName, req.GetIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query inode: %s", err)
	}

	valid := req.GetValid()
	if valid == 0 {
		return &bowlv1.SetAttrResponse{
			Inode: inode,
		}, nil
	}

	newMode := int(inode.GetMode())
	newUid := int(inode.GetUid())
	newGid := int(inode.GetGid())
	newSize := int64(inode.GetSize())
	newBlocks := int64(inode.GetBlocks())
	newAtime := inode.GetAtime().AsTime()
	newMtime := inode.GetMtime().AsTime()
	newCtime := time.Now()

	if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_MODE) != 0 {
		newMode = int(req.GetMode())
	}
	if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_KILL_SUIDGID) != 0 {
		newMode = newMode &^ 0o6000
	}
	if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_UID) != 0 {
		newUid = int(req.GetUid())
	}
	if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_GID) != 0 {
		newGid = int(req.GetGid())
	}
	if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_SIZE) != 0 {
		newSize = int64(req.GetSize())
		newBlocks = (newSize + 511) / 512
		if s.driver != nil {
			if err := s.driver.Truncate(ctx, sess.DiskID, req.GetIno(), req.GetSize()); err != nil {
				return nil, toConnectError(err)
			}
		}
	}
	if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_ATIME_NOW) != 0 {
		newAtime = time.Now()
	} else if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_ATIME) != 0 && req.GetAtime() != nil {
		newAtime = req.GetAtime().AsTime()
	}
	if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_MTIME_NOW) != 0 {
		newMtime = time.Now()
	} else if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_MTIME) != 0 && req.GetMtime() != nil {
		newMtime = req.GetMtime().AsTime()
	}
	if valid&uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_CTIME) != 0 && req.GetCtime() != nil {
		newCtime = req.GetCtime().AsTime()
	}

	updateQuery := fmt.Sprintf(`
UPDATE "%s".inodes
SET mode = $1, uid = $2, gid = $3, size = $4, blocks = $5, atime = $6, mtime = $7, ctime = $8
WHERE ino = $9;
`, schemaName)

	if _, err := s.db.Exec(ctx, updateQuery, newMode, newUid, newGid, newSize, newBlocks, newAtime, newMtime, newCtime, int64(req.GetIno())); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to update inode: %s", err)
	}

	updatedInode := &bowlv1.Inode{
		Ino:         req.GetIno(),
		ParentIno:   inode.GetParentIno(),
		Type:        inode.GetType(),
		Mode:        uint32(newMode),
		Uid:         uint32(newUid),
		Gid:         uint32(newGid),
		Size:        uint64(newSize),
		Blocks:      uint64(newBlocks),
		Nlink:       inode.GetNlink(),
		Rdev:        inode.GetRdev(),
		Atime:       timestamppb.New(newAtime),
		Mtime:       timestamppb.New(newMtime),
		Ctime:       timestamppb.New(newCtime),
		Target:      inode.GetTarget(),
		Generation:  inode.GetGeneration(),
		AttrTimeout: durationpb.New(1 * time.Second),
	}

	return &bowlv1.SetAttrResponse{
		Inode: updatedInode,
	}, nil
}

// Access verifies caller permissions against Inode mode and session state (FUSE_ACCESS).
func (s *StorageService) Access(ctx context.Context, req *bowlv1.AccessRequest) (*bowlv1.AccessResponse, error) {
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	inode, err := s.getInode(ctx, schemaName, req.GetIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query inode: %s", err)
	}

	mask := req.GetMask()
	mode := inode.GetMode()

	// Write check (W_OK = 2)
	if mask&2 != 0 {
		if sess.ReadOnly {
			return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: session is read-only")
		}
		if mode&0o222 == 0 {
			return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: file mode does not permit write")
		}
	}

	// Read check (R_OK = 4)
	if mask&4 != 0 {
		if mode&0o444 == 0 {
			return nil, connect.NewError(connect.CodePermissionDenied, "read permission denied: file mode does not permit read")
		}
	}

	// Execute check (X_OK = 1)
	if mask&1 != 0 {
		if mode&0o111 == 0 {
			return nil, connect.NewError(connect.CodePermissionDenied, "execute permission denied: file mode does not permit execute")
		}
	}

	return &bowlv1.AccessResponse{
		Allowed: true,
	}, nil
}
