package projection

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	CommitKindInit   = "init"
	CommitKindManual = "manual"
	CommitKindFork   = "fork"
	CommitKindMerge  = "merge"
)

var (
	ErrNotFound        = errors.New("projection: not found")
	ErrInvalidArgument = errors.New("projection: invalid argument")
	ErrUnavailable     = errors.New("projection: unavailable")
	ErrBarrierTimeout  = errors.New("projection: flush barrier timeout")
	ErrDiskDirty       = errors.New("projection: disk projection dirty")
	ErrQueueClosed     = errors.New("projection: queue closed")
)

type DiskRecord struct {
	DiskID        uuid.UUID
	Name          string
	DefaultBranch string
	MaxBytes      uint64
	UsedBytes     uint64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type BranchRecord struct {
	DiskID       uuid.UUID
	Name         string
	HeadCommitID uint64
	WriterEpoch  uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type CommitRecord struct {
	DiskID        uuid.UUID
	CommitID      uint64
	ParentID      uint64
	MergeParentID uint64
	Kind          string
	Message       string
	CreatedAt     time.Time
}

type MountRecord struct {
	DiskID      uuid.UUID
	MountID     uuid.UUID
	BranchName  string
	ReadOnly    bool
	WriterEpoch uint64
	MountedAt   time.Time
	ExpiresAt   time.Time
}

type DiskSnapshot struct {
	Disk     DiskRecord
	Branches []BranchRecord
	Commits  []CommitRecord
	Mounts   []MountRecord
}

type ListDisksQuery struct {
	PageSize  uint32
	PageToken string
}

type ListDisksPage struct {
	Disks         []DiskRecord
	NextPageToken string
}

type ListCommitsQuery struct {
	DiskID    uuid.UUID
	Kind      string
	PageSize  uint32
	PageToken string
}

type ListCommitsPage struct {
	Commits       []CommitRecord
	NextPageToken string
}

type DiskUsageUpdate struct {
	DiskID    uuid.UUID
	UsedBytes uint64
}

type MountRenewUpdate struct {
	DiskID    uuid.UUID
	MountID   uuid.UUID
	ExpiresAt time.Time
}

type Store interface {
	ListDisks(ctx context.Context, q ListDisksQuery) (ListDisksPage, error)
	GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error)
	ListBranches(ctx context.Context, diskID uuid.UUID) ([]BranchRecord, error)
	ListCommits(ctx context.Context, q ListCommitsQuery) (ListCommitsPage, error)
	ListMounts(ctx context.Context, diskID uuid.UUID, branchName string, now time.Time) ([]MountRecord, error)

	UpsertDisk(ctx context.Context, disk DiskRecord, initialBranch *BranchRecord, initialCommit *CommitRecord) error
	UpdateDisk(ctx context.Context, disk DiskRecord) error
	DeleteDisk(ctx context.Context, diskID uuid.UUID) error
	UpsertBranchAndCommit(ctx context.Context, branches []BranchRecord, commits []CommitRecord, deletedMountIDs []uuid.UUID) error
	DeleteBranch(ctx context.Context, diskID uuid.UUID, branchName string) error
	DeleteCommits(ctx context.Context, diskID uuid.UUID, commitIDs []uint64) error
	UpsertMount(ctx context.Context, mount MountRecord, updatedBranch *BranchRecord, deletedMountIDs []uuid.UUID) error
	DeleteMounts(ctx context.Context, diskID uuid.UUID, mountIDs []uuid.UUID, updatedBranches []BranchRecord) error

	BatchUpdateDiskUsage(ctx context.Context, updates []DiskUsageUpdate) error
	BatchRenewMounts(ctx context.Context, updates []MountRenewUpdate) error

	ReplaceDiskSnapshot(ctx context.Context, snap DiskSnapshot) error
	ListAllDiskIDs(ctx context.Context) ([]uuid.UUID, error)
	PruneExpiredMounts(ctx context.Context, now time.Time) (int64, error)
}

func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrUnavailable) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		pgconn.SafeToRetry(err) ||
		pgconn.Timeout(err) ||
		strings.Contains(err.Error(), "closed pool") {
		return true
	}
	if _, ok := errors.AsType[*pgconn.ConnectError](err); ok {
		return true
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		if strings.HasPrefix(pgErr.Code, "08") ||
			strings.HasPrefix(pgErr.Code, "53") ||
			strings.HasPrefix(pgErr.Code, "57") ||
			strings.HasPrefix(pgErr.Code, "58") {
			return true
		}
	}
	return false
}

func normalizeTimestamp(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC().Truncate(time.Microsecond)
	}
	return t.UTC().Truncate(time.Microsecond)
}

func validCommitKind(kind string) bool {
	switch kind {
	case CommitKindInit, CommitKindManual, CommitKindFork, CommitKindMerge:
		return true
	default:
		return false
	}
}

func uint64ToInt64(v uint64) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("%w: value %d overflows int64", ErrInvalidArgument, v)
	}
	return int64(v), nil
}

func int64ToUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func validateDiskRecord(d DiskRecord, requireDefaultBranch bool) error {
	if d.DiskID == uuid.Nil {
		return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if requireDefaultBranch && d.DefaultBranch == "" {
		return fmt.Errorf("%w: default_branch cannot be empty", ErrInvalidArgument)
	}
	if d.MaxBytes == 0 || d.MaxBytes > math.MaxInt64 {
		return fmt.Errorf("%w: invalid max_bytes %d", ErrInvalidArgument, d.MaxBytes)
	}
	if d.UsedBytes > math.MaxInt64 {
		return fmt.Errorf("%w: invalid used_bytes %d", ErrInvalidArgument, d.UsedBytes)
	}
	return nil
}

func validateBranchRecord(b BranchRecord) error {
	if b.DiskID == uuid.Nil {
		return fmt.Errorf("%w: branch disk_id cannot be nil", ErrInvalidArgument)
	}
	if b.Name == "" {
		return fmt.Errorf("%w: branch name cannot be empty", ErrInvalidArgument)
	}
	if b.HeadCommitID > math.MaxInt64 {
		return fmt.Errorf("%w: invalid head_commit_id %d", ErrInvalidArgument, b.HeadCommitID)
	}
	if b.WriterEpoch > math.MaxInt64 {
		return fmt.Errorf("%w: invalid writer_epoch %d", ErrInvalidArgument, b.WriterEpoch)
	}
	return nil
}

func validateCommitRecord(c CommitRecord) error {
	if c.DiskID == uuid.Nil {
		return fmt.Errorf("%w: commit disk_id cannot be nil", ErrInvalidArgument)
	}
	if c.CommitID == 0 || c.CommitID > math.MaxInt64 {
		return fmt.Errorf("%w: invalid commit_id %d", ErrInvalidArgument, c.CommitID)
	}
	if c.ParentID > math.MaxInt64 {
		return fmt.Errorf("%w: invalid parent_id %d", ErrInvalidArgument, c.ParentID)
	}
	if c.MergeParentID > math.MaxInt64 {
		return fmt.Errorf("%w: invalid merge_parent_id %d", ErrInvalidArgument, c.MergeParentID)
	}
	if !validCommitKind(c.Kind) {
		return fmt.Errorf("%w: invalid commit kind %q", ErrInvalidArgument, c.Kind)
	}
	return nil
}

func validateMountRecord(m MountRecord) error {
	if m.DiskID == uuid.Nil {
		return fmt.Errorf("%w: mount disk_id cannot be nil", ErrInvalidArgument)
	}
	if m.MountID == uuid.Nil {
		return fmt.Errorf("%w: mount_id cannot be nil", ErrInvalidArgument)
	}
	if m.BranchName == "" {
		return fmt.Errorf("%w: mount branch_name cannot be empty", ErrInvalidArgument)
	}
	if m.WriterEpoch > math.MaxInt64 {
		return fmt.Errorf("%w: invalid mount writer_epoch %d", ErrInvalidArgument, m.WriterEpoch)
	}
	return nil
}
