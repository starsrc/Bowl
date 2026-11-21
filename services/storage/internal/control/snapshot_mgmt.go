package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type SnapshotKind = db.SnapshotKind

const (
	SnapshotKindFork   = db.SnapshotKindFork
	SnapshotKindMerge  = db.SnapshotKindMerge
	SnapshotKindManual = db.SnapshotKindManual
)

type Snapshot struct {
	SnapshotID            string
	SourceDiskID          string
	ParentSnapshotID      string
	MergeParentSnapshotID string
	Kind                  SnapshotKind
	Name                  string
	Protected             bool
	CreatedAt             time.Time
}

type CreateSnapshotParams struct {
	DiskID string
	Name   string
}

type ListSnapshotsParams struct {
	DiskID    string
	Kind      SnapshotKind
	PageSize  uint32
	PageToken string
}

type ListSnapshotsResult struct {
	Snapshots     []*Snapshot
	NextPageToken string
}

type DeleteSnapshotParams struct {
	DiskID     string
	SnapshotID string
}

type PruneSnapshotsParams struct {
	DiskID string
	DryRun bool
}

type PruneSnapshotsResult struct {
	Snapshots      []*Snapshot
	ReclaimedCount uint32
	DryRun         bool
}

func formatOptionalUUID(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func assembleSnapshot(rec *db.SnapshotRecord) *Snapshot {
	if rec == nil {
		return nil
	}
	return &Snapshot{
		SnapshotID:            formatOptionalUUID(rec.SnapshotID),
		SourceDiskID:          formatOptionalUUID(rec.SourceDiskID),
		ParentSnapshotID:      formatOptionalUUID(rec.ParentSnapshotID),
		MergeParentSnapshotID: formatOptionalUUID(rec.MergeParentSnapshotID),
		Kind:                  rec.Kind,
		Name:                  rec.Name,
		Protected:             rec.Protected,
		CreatedAt:             rec.CreatedAt,
	}
}

func validateSnapshotKind(kind SnapshotKind) error {
	switch kind {
	case "", SnapshotKindFork, SnapshotKindMerge, SnapshotKindManual:
		return nil
	default:
		return fmt.Errorf("%w: invalid snapshot kind %q", ErrInvalidArgument, kind)
	}
}

func (m *manager) CreateSnapshot(ctx context.Context, params CreateSnapshotParams) (*Snapshot, error) {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if nameErr := validateDiskName(params.Name); nameErr != nil {
		return nil, nameErr
	}
	diskID := diskUUID.String()

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	cache, err := m.sharedCache()
	if err != nil {
		return nil, err
	}

	diskRow, err := m.store.GetDisk(ctx, diskUUID)
	if err != nil {
		return nil, mapDBErr(err)
	}

	entry, err := m.getOrOpenActorWithRecordLocked(ctx, &diskRow)
	if err != nil {
		return nil, err
	}

	snapshotUUID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	snapshotID := snapshotUUID.String()
	snapDir := m.snapshotDir(snapshotID)

	ckStats, err := entry.actor.Checkpoint(ctx, snapDir)
	if err != nil {
		return nil, fmt.Errorf("create snapshot checkpoint: %w", err)
	}

	removeSnapshotDirOnRollback := func(stage string) {
		if removeErr := m.removeAll(snapDir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "create snapshot rollback remove snapshot dir failed",
				"stage", stage,
				"disk_id", diskID,
				"snapshot_id", snapshotID,
				"dir", snapDir,
				"err", removeErr,
			)
		}
	}

	if ckStats.HasOrphans {
		if _, sanErr := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{
			Dir:        snapDir,
			SnapshotID: snapshotID,
			Cache:      cache,
			Logger:     m.baseLogger,
			Metrics:    m.storeMetrics,
			OpenActor:  m.openDiskActor,
		}); sanErr != nil {
			removeSnapshotDirOnRollback("sanitize")
			return nil, fmt.Errorf("sanitize snapshot checkpoint orphans: %w", sanErr)
		}
	}

	rec, err := m.store.CreateSnapshot(ctx, db.CreateSnapshotInput{
		DiskID:     diskUUID,
		SnapshotID: snapshotUUID,
		Name:       params.Name,
	})
	if err != nil {
		removeSnapshotDirOnRollback("tx")
		return nil, fmt.Errorf("create snapshot: %w", mapDBErr(err))
	}

	if flushErr := m.flushEntryUsage(ctx, entry); flushErr != nil {
		m.logger.WarnContext(ctx, "create snapshot source usage flush failed",
			"disk_id", diskID,
			"err", flushErr,
		)
	}

	m.logger.InfoContext(ctx, "snapshot created",
		"disk_id", diskID,
		"snapshot_id", snapshotID,
		"name", params.Name,
	)
	return assembleSnapshot(&rec), nil
}

func (m *manager) ListSnapshots(ctx context.Context, params ListSnapshotsParams) (*ListSnapshotsResult, error) {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if params.PageSize > MaxListDisksPageSize {
		return nil, fmt.Errorf("%w: page_size %d exceeds max %d", ErrInvalidArgument, params.PageSize, MaxListDisksPageSize)
	}
	if kindErr := validateSnapshotKind(params.Kind); kindErr != nil {
		return nil, kindErr
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	pageSize := params.PageSize
	if pageSize == 0 {
		pageSize = DefaultListDisksPageSize
	}

	page, err := m.store.ListSnapshots(ctx, db.ListSnapshotsQuery{
		DiskID:    diskUUID,
		Kind:      params.Kind,
		PageSize:  pageSize,
		PageToken: params.PageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", mapDBErr(err))
	}

	snapshots := make([]*Snapshot, 0, len(page.Snapshots))
	for i := range page.Snapshots {
		snapshots = append(snapshots, assembleSnapshot(&page.Snapshots[i]))
	}
	return &ListSnapshotsResult{
		Snapshots:     snapshots,
		NextPageToken: page.NextPageToken,
	}, nil
}

func (m *manager) DeleteSnapshot(ctx context.Context, params DeleteSnapshotParams) error {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return err
	}
	snapUUID, err := parseSnapshotID(params.SnapshotID)
	if err != nil {
		return err
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return openErr
	}

	if _, err = m.store.DeleteSnapshot(ctx, diskUUID, snapUUID); err != nil {
		return mapDBErr(err)
	}

	diskID := diskUUID.String()
	sidStr := snapUUID.String()
	sDir := m.snapshotDir(sidStr)

	m.lineageSnapshotMu.Lock()
	removeErr := m.removeAll(sDir)
	if removeErr == nil {
		m.collector.addSnapshotsReclaimed(1)
	} else {
		m.collector.incCleanupError()
		m.logger.WarnContext(ctx, "delete snapshot directory cleanup failed",
			"disk_id", diskID,
			"snapshot_id", sidStr,
			"dir", sDir,
			"err", removeErr,
		)
	}
	m.lineageSnapshotMu.Unlock()

	if removeErr != nil {
		return fmt.Errorf("remove snapshot dir %s: %w", sidStr, removeErr)
	}

	m.logger.InfoContext(ctx, "snapshot deleted",
		"disk_id", diskID,
		"snapshot_id", sidStr,
	)
	return nil
}

func (m *manager) PruneSnapshots(ctx context.Context, params PruneSnapshotsParams) (*PruneSnapshotsResult, error) {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	records, err := m.store.PruneSnapshots(ctx, diskUUID, params.DryRun)
	if err != nil {
		return nil, mapDBErr(err)
	}

	snapshots := make([]*Snapshot, 0, len(records))
	for i := range records {
		snapshots = append(snapshots, assembleSnapshot(&records[i]))
	}

	if !params.DryRun && len(records) > 0 {
		diskID := diskUUID.String()
		var cleanupErrs []error
		m.lineageSnapshotMu.Lock()
		for i := range records {
			sID := records[i].SnapshotID
			if sID == uuid.Nil {
				continue
			}
			sidStr := sID.String()
			sDir := m.snapshotDir(sidStr)
			if removeErr := m.removeAll(sDir); removeErr != nil {
				m.collector.incCleanupError()
				m.logger.WarnContext(ctx, "prune snapshot directory cleanup failed",
					"disk_id", diskID,
					"snapshot_id", sidStr,
					"dir", sDir,
					"err", removeErr,
				)
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove snapshot dir %s: %w", sidStr, removeErr))
			} else {
				m.collector.addSnapshotsReclaimed(1)
			}
		}
		m.lineageSnapshotMu.Unlock()

		if len(cleanupErrs) > 0 {
			return nil, errors.Join(cleanupErrs...)
		}

		m.logger.InfoContext(ctx, "snapshots pruned",
			"disk_id", diskID,
			"reclaimed_count", len(records),
		)
	}

	return &PruneSnapshotsResult{
		Snapshots:      snapshots,
		ReclaimedCount: uint32(len(snapshots)),
		DryRun:         params.DryRun,
	}, nil
}
