package projection

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

type MemStoreHooks struct {
	BeforeWrite func(ctx context.Context, op string, diskID uuid.UUID) error
	BeforeBatch func(ctx context.Context, op string) error
	BeforeRead  func(ctx context.Context, op string) error
}

type MemStore struct {
	mu       sync.RWMutex
	disks    map[uuid.UUID]DiskRecord
	branches map[uuid.UUID]map[string]BranchRecord
	commits  map[uuid.UUID]map[uint64]CommitRecord
	mounts   map[uuid.UUID]map[uuid.UUID]MountRecord

	hooksMu sync.RWMutex
	hooks   MemStoreHooks
}

func NewMemStore() *MemStore {
	return &MemStore{
		disks:    make(map[uuid.UUID]DiskRecord),
		branches: make(map[uuid.UUID]map[string]BranchRecord),
		commits:  make(map[uuid.UUID]map[uint64]CommitRecord),
		mounts:   make(map[uuid.UUID]map[uuid.UUID]MountRecord),
	}
}

func (s *MemStore) SetHooks(hooks MemStoreHooks) {
	s.hooksMu.Lock()
	s.hooks = hooks
	s.hooksMu.Unlock()
}

func (s *MemStore) callReadHook(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	s.hooksMu.RLock()
	fn := s.hooks.BeforeRead
	s.hooksMu.RUnlock()
	if fn != nil {
		return fn(ctx, op)
	}
	return nil
}

func (s *MemStore) callWriteHook(ctx context.Context, op string, diskID uuid.UUID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	s.hooksMu.RLock()
	fn := s.hooks.BeforeWrite
	s.hooksMu.RUnlock()
	if fn != nil {
		return fn(ctx, op, diskID)
	}
	return nil
}

func (s *MemStore) callBatchHook(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	s.hooksMu.RLock()
	fn := s.hooks.BeforeBatch
	s.hooksMu.RUnlock()
	if fn != nil {
		return fn(ctx, op)
	}
	return nil
}

func compareUUID(a, b uuid.UUID) int {
	return bytes.Compare(a[:], b[:])
}

func (s *MemStore) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if err := s.callReadHook(ctx, "GetDisk"); err != nil {
		return DiskRecord{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.disks[diskID]
	if !ok {
		return DiskRecord{}, ErrNotFound
	}
	return rec, nil
}

func (s *MemStore) ListDisks(ctx context.Context, q ListDisksQuery) (ListDisksPage, error) {
	limit, err := NormalizePageSize(q.PageSize)
	if err != nil {
		return ListDisksPage{}, err
	}
	var (
		hasCursor  bool
		cursorTime time.Time
		cursorID   uuid.UUID
	)
	if q.PageToken != "" {
		cursorTime, cursorID, err = DecodeDiskPageCursor(q.PageToken)
		if err != nil {
			return ListDisksPage{}, err
		}
		hasCursor = true
	}
	if hookErr := s.callReadHook(ctx, "ListDisks"); hookErr != nil {
		return ListDisksPage{}, hookErr
	}

	s.mu.RLock()
	all := make([]DiskRecord, 0, len(s.disks))
	for _, rec := range s.disks {
		if hasCursor {
			if rec.CreatedAt.After(cursorTime) {
				continue
			}
			if rec.CreatedAt.Equal(cursorTime) && compareUUID(rec.DiskID, cursorID) >= 0 {
				continue
			}
		}
		all = append(all, rec)
	}
	s.mu.RUnlock()

	slices.SortFunc(all, func(a, b DiskRecord) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return compareUUID(b.DiskID, a.DiskID)
	})

	page := ListDisksPage{}
	if len(all) > int(limit) {
		all = all[:limit]
		last := all[len(all)-1]
		page.NextPageToken = EncodeDiskPageCursor(last.CreatedAt, last.DiskID)
	}
	page.Disks = all
	return page, nil
}

func (s *MemStore) ListBranches(ctx context.Context, diskID uuid.UUID) ([]BranchRecord, error) {
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if err := s.callReadHook(ctx, "ListBranches"); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.disks[diskID]; !ok {
		return nil, ErrNotFound
	}
	bm := s.branches[diskID]
	out := make([]BranchRecord, 0, len(bm))
	for _, b := range bm {
		out = append(out, b)
	}
	slices.SortFunc(out, func(a, b BranchRecord) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return out, nil
}

func (s *MemStore) ListCommits(ctx context.Context, q ListCommitsQuery) (ListCommitsPage, error) {
	if q.DiskID == uuid.Nil {
		return ListCommitsPage{}, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	limit, err := NormalizePageSize(q.PageSize)
	if err != nil {
		return ListCommitsPage{}, err
	}
	if q.Kind != "" && !validCommitKind(q.Kind) {
		return ListCommitsPage{}, fmt.Errorf("%w: invalid commit kind %q", ErrInvalidArgument, q.Kind)
	}
	var (
		hasCursor bool
		cursorID  uint64
	)
	if q.PageToken != "" {
		cursorID, err = DecodeCommitPageCursor(q.PageToken)
		if err != nil {
			return ListCommitsPage{}, err
		}
		hasCursor = true
	}
	if hookErr := s.callReadHook(ctx, "ListCommits"); hookErr != nil {
		return ListCommitsPage{}, hookErr
	}

	s.mu.RLock()
	if _, ok := s.disks[q.DiskID]; !ok {
		s.mu.RUnlock()
		return ListCommitsPage{}, ErrNotFound
	}
	cm := s.commits[q.DiskID]
	all := make([]CommitRecord, 0, len(cm))
	for _, c := range cm {
		if q.Kind != "" && c.Kind != q.Kind {
			continue
		}
		if hasCursor && c.CommitID >= cursorID {
			continue
		}
		all = append(all, c)
	}
	s.mu.RUnlock()

	slices.SortFunc(all, func(a, b CommitRecord) int {
		return cmp.Compare(b.CommitID, a.CommitID)
	})

	page := ListCommitsPage{}
	if len(all) > int(limit) {
		all = all[:limit]
		page.NextPageToken = EncodeCommitPageCursor(all[len(all)-1].CommitID)
	}
	page.Commits = all
	return page, nil
}

func (s *MemStore) ListMounts(ctx context.Context, diskID uuid.UUID, branchName string, now time.Time) ([]MountRecord, error) {
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if err := s.callReadHook(ctx, "ListMounts"); err != nil {
		return nil, err
	}
	cutoff := normalizeTimestamp(now)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.disks[diskID]; !ok {
		return nil, ErrNotFound
	}
	mm := s.mounts[diskID]
	out := make([]MountRecord, 0, len(mm))
	for _, m := range mm {
		if !m.ExpiresAt.After(cutoff) {
			continue
		}
		if branchName != "" && m.BranchName != branchName {
			continue
		}
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b MountRecord) int {
		return compareUUID(a.MountID, b.MountID)
	})
	return out, nil
}

func normalizeDiskRecord(d DiskRecord) DiskRecord {
	d.CreatedAt = normalizeTimestamp(d.CreatedAt)
	d.UpdatedAt = normalizeTimestamp(d.UpdatedAt)
	return d
}

func normalizeBranchRecord(b BranchRecord) BranchRecord {
	b.CreatedAt = normalizeTimestamp(b.CreatedAt)
	b.UpdatedAt = normalizeTimestamp(b.UpdatedAt)
	return b
}

func normalizeCommitRecord(c CommitRecord) CommitRecord {
	c.CreatedAt = normalizeTimestamp(c.CreatedAt)
	return c
}

func normalizeMountRecord(m MountRecord) MountRecord {
	m.MountedAt = normalizeTimestamp(m.MountedAt)
	m.ExpiresAt = normalizeTimestamp(m.ExpiresAt)
	return m
}

func (s *MemStore) ensureDiskMapsLocked(diskID uuid.UUID) {
	if _, ok := s.branches[diskID]; !ok {
		s.branches[diskID] = make(map[string]BranchRecord)
	}
	if _, ok := s.commits[diskID]; !ok {
		s.commits[diskID] = make(map[uint64]CommitRecord)
	}
	if _, ok := s.mounts[diskID]; !ok {
		s.mounts[diskID] = make(map[uuid.UUID]MountRecord)
	}
}

func (s *MemStore) UpsertDisk(ctx context.Context, disk DiskRecord, initialBranch *BranchRecord, initialCommit *CommitRecord) error {
	if err := validateDiskRecord(disk, true); err != nil {
		return err
	}
	if initialBranch != nil {
		if err := validateBranchRecord(*initialBranch); err != nil {
			return err
		}
		if initialBranch.DiskID != disk.DiskID {
			return fmt.Errorf("%w: initial branch disk_id mismatch", ErrInvalidArgument)
		}
	}
	if initialCommit != nil {
		if err := validateCommitRecord(*initialCommit); err != nil {
			return err
		}
		if initialCommit.DiskID != disk.DiskID {
			return fmt.Errorf("%w: initial commit disk_id mismatch", ErrInvalidArgument)
		}
	}
	if err := s.callWriteHook(ctx, "UpsertDisk", disk.DiskID); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.disks[disk.DiskID] = normalizeDiskRecord(disk)
	s.ensureDiskMapsLocked(disk.DiskID)
	if initialBranch != nil {
		s.branches[disk.DiskID][initialBranch.Name] = normalizeBranchRecord(*initialBranch)
	}
	if initialCommit != nil {
		s.commits[disk.DiskID][initialCommit.CommitID] = normalizeCommitRecord(*initialCommit)
	}
	return nil
}

func (s *MemStore) UpdateDisk(ctx context.Context, disk DiskRecord) error {
	if err := validateDiskRecord(disk, false); err != nil {
		return err
	}
	if err := s.callWriteHook(ctx, "UpdateDisk", disk.DiskID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.disks[disk.DiskID]
	if !ok {
		return ErrNotFound
	}
	existing.Name = disk.Name
	if disk.DefaultBranch != "" {
		existing.DefaultBranch = disk.DefaultBranch
	}
	existing.MaxBytes = disk.MaxBytes
	existing.UsedBytes = disk.UsedBytes
	existing.UpdatedAt = normalizeTimestamp(disk.UpdatedAt)
	s.disks[disk.DiskID] = existing
	return nil
}

func (s *MemStore) DeleteDisk(ctx context.Context, diskID uuid.UUID) error {
	if diskID == uuid.Nil {
		return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if err := s.callWriteHook(ctx, "DeleteDisk", diskID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.disks, diskID)
	delete(s.branches, diskID)
	delete(s.commits, diskID)
	delete(s.mounts, diskID)
	return nil
}

func (s *MemStore) UpsertBranchAndCommit(ctx context.Context, branches []BranchRecord, commits []CommitRecord, deletedMountIDs []uuid.UUID) error {
	if len(branches) == 0 && len(commits) == 0 && len(deletedMountIDs) == 0 {
		return nil
	}
	var hookDiskID uuid.UUID
	for _, b := range branches {
		if err := validateBranchRecord(b); err != nil {
			return err
		}
		if hookDiskID == uuid.Nil {
			hookDiskID = b.DiskID
		}
	}
	for _, c := range commits {
		if err := validateCommitRecord(c); err != nil {
			return err
		}
		if hookDiskID == uuid.Nil {
			hookDiskID = c.DiskID
		}
	}
	if slices.Contains(deletedMountIDs, uuid.Nil) {
		return fmt.Errorf("%w: deleted mount_id cannot be nil", ErrInvalidArgument)
	}
	if err := s.callWriteHook(ctx, "UpsertBranchAndCommit", hookDiskID); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range branches {
		if _, ok := s.disks[b.DiskID]; !ok {
			return ErrNotFound
		}
	}
	for _, c := range commits {
		if _, ok := s.disks[c.DiskID]; !ok {
			return ErrNotFound
		}
	}
	for _, b := range branches {
		s.ensureDiskMapsLocked(b.DiskID)
		s.branches[b.DiskID][b.Name] = normalizeBranchRecord(b)
	}
	for _, c := range commits {
		s.ensureDiskMapsLocked(c.DiskID)
		s.commits[c.DiskID][c.CommitID] = normalizeCommitRecord(c)
	}
	if len(deletedMountIDs) > 0 {
		if hookDiskID != uuid.Nil {
			for _, mid := range deletedMountIDs {
				delete(s.mounts[hookDiskID], mid)
			}
		} else {
			for _, mm := range s.mounts {
				for _, mid := range deletedMountIDs {
					delete(mm, mid)
				}
			}
		}
	}
	return nil
}

func (s *MemStore) DeleteBranch(ctx context.Context, diskID uuid.UUID, branchName string) error {
	if diskID == uuid.Nil || branchName == "" {
		return fmt.Errorf("%w: disk_id and branch_name are required", ErrInvalidArgument)
	}
	if err := s.callWriteHook(ctx, "DeleteBranch", diskID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.branches[diskID], branchName)
	return nil
}

func (s *MemStore) DeleteCommits(ctx context.Context, diskID uuid.UUID, commitIDs []uint64) error {
	if diskID == uuid.Nil {
		return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if len(commitIDs) == 0 {
		return nil
	}
	for _, cid := range commitIDs {
		if cid == 0 {
			return fmt.Errorf("%w: commit_id cannot be zero", ErrInvalidArgument)
		}
		if _, err := uint64ToInt64(cid); err != nil {
			return err
		}
	}
	if err := s.callWriteHook(ctx, "DeleteCommits", diskID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cid := range commitIDs {
		delete(s.commits[diskID], cid)
	}
	return nil
}

func (s *MemStore) UpsertMount(ctx context.Context, mount MountRecord, updatedBranch *BranchRecord, deletedMountIDs []uuid.UUID) error {
	if err := validateMountRecord(mount); err != nil {
		return err
	}
	if updatedBranch != nil {
		if err := validateBranchRecord(*updatedBranch); err != nil {
			return err
		}
		if updatedBranch.DiskID != mount.DiskID {
			return fmt.Errorf("%w: updated branch disk_id mismatch", ErrInvalidArgument)
		}
	}
	if slices.Contains(deletedMountIDs, uuid.Nil) {
		return fmt.Errorf("%w: deleted mount_id cannot be nil", ErrInvalidArgument)
	}
	if err := s.callWriteHook(ctx, "UpsertMount", mount.DiskID); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.disks[mount.DiskID]; !ok {
		return ErrNotFound
	}
	s.ensureDiskMapsLocked(mount.DiskID)
	for _, mid := range deletedMountIDs {
		delete(s.mounts[mount.DiskID], mid)
	}
	if updatedBranch != nil {
		s.branches[mount.DiskID][updatedBranch.Name] = normalizeBranchRecord(*updatedBranch)
	}
	s.mounts[mount.DiskID][mount.MountID] = normalizeMountRecord(mount)
	return nil
}

func (s *MemStore) DeleteMounts(ctx context.Context, diskID uuid.UUID, mountIDs []uuid.UUID, updatedBranches []BranchRecord) error {
	if diskID == uuid.Nil {
		return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if slices.Contains(mountIDs, uuid.Nil) {
		return fmt.Errorf("%w: mount_id cannot be nil", ErrInvalidArgument)
	}
	for _, b := range updatedBranches {
		if err := validateBranchRecord(b); err != nil {
			return err
		}
		if b.DiskID != diskID {
			return fmt.Errorf("%w: updated branch disk_id mismatch", ErrInvalidArgument)
		}
	}
	if len(mountIDs) == 0 && len(updatedBranches) == 0 {
		return nil
	}
	if err := s.callWriteHook(ctx, "DeleteMounts", diskID); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(updatedBranches) > 0 {
		if _, ok := s.disks[diskID]; !ok {
			return ErrNotFound
		}
	}
	for _, mid := range mountIDs {
		delete(s.mounts[diskID], mid)
	}
	for _, b := range updatedBranches {
		s.ensureDiskMapsLocked(diskID)
		s.branches[diskID][b.Name] = normalizeBranchRecord(b)
	}
	return nil
}

func (s *MemStore) BatchUpdateDiskUsage(ctx context.Context, updates []DiskUsageUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	for _, u := range updates {
		if u.DiskID == uuid.Nil {
			return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
		}
		if _, err := uint64ToInt64(u.UsedBytes); err != nil {
			return err
		}
	}
	if err := s.callBatchHook(ctx, "BatchUpdateDiskUsage"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range updates {
		if rec, ok := s.disks[u.DiskID]; ok {
			rec.UsedBytes = u.UsedBytes
			s.disks[u.DiskID] = rec
		}
	}
	return nil
}

func (s *MemStore) BatchRenewMounts(ctx context.Context, updates []MountRenewUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	for _, u := range updates {
		if u.DiskID == uuid.Nil || u.MountID == uuid.Nil {
			return fmt.Errorf("%w: disk_id and mount_id are required", ErrInvalidArgument)
		}
	}
	if err := s.callBatchHook(ctx, "BatchRenewMounts"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range updates {
		if mm, ok := s.mounts[u.DiskID]; ok {
			if m, exists := mm[u.MountID]; exists {
				m.ExpiresAt = normalizeTimestamp(u.ExpiresAt)
				mm[u.MountID] = m
			}
		}
	}
	return nil
}

func (s *MemStore) ReplaceDiskSnapshot(ctx context.Context, snap DiskSnapshot) error {
	if err := validateDiskRecord(snap.Disk, true); err != nil {
		return err
	}
	for _, b := range snap.Branches {
		if err := validateBranchRecord(b); err != nil {
			return err
		}
		if b.DiskID != snap.Disk.DiskID {
			return fmt.Errorf("%w: snapshot branch disk_id mismatch", ErrInvalidArgument)
		}
	}
	for _, c := range snap.Commits {
		if err := validateCommitRecord(c); err != nil {
			return err
		}
		if c.DiskID != snap.Disk.DiskID {
			return fmt.Errorf("%w: snapshot commit disk_id mismatch", ErrInvalidArgument)
		}
	}
	for _, m := range snap.Mounts {
		if err := validateMountRecord(m); err != nil {
			return err
		}
		if m.DiskID != snap.Disk.DiskID {
			return fmt.Errorf("%w: snapshot mount disk_id mismatch", ErrInvalidArgument)
		}
	}
	if err := s.callWriteHook(ctx, "ReplaceDiskSnapshot", snap.Disk.DiskID); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	diskID := snap.Disk.DiskID
	s.disks[diskID] = normalizeDiskRecord(snap.Disk)
	bm := make(map[string]BranchRecord, len(snap.Branches))
	for _, b := range snap.Branches {
		bm[b.Name] = normalizeBranchRecord(b)
	}
	cm := make(map[uint64]CommitRecord, len(snap.Commits))
	for _, c := range snap.Commits {
		cm[c.CommitID] = normalizeCommitRecord(c)
	}
	mm := make(map[uuid.UUID]MountRecord, len(snap.Mounts))
	for _, m := range snap.Mounts {
		mm[m.MountID] = normalizeMountRecord(m)
	}
	s.branches[diskID] = bm
	s.commits[diskID] = cm
	s.mounts[diskID] = mm
	return nil
}

func (s *MemStore) ListAllDiskIDs(ctx context.Context) ([]uuid.UUID, error) {
	if err := s.callReadHook(ctx, "ListAllDiskIDs"); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]uuid.UUID, 0, len(s.disks))
	for id := range s.disks {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, compareUUID)
	return ids, nil
}

func (s *MemStore) PruneExpiredMounts(ctx context.Context, now time.Time) (int64, error) {
	if err := s.callBatchHook(ctx, "PruneExpiredMounts"); err != nil {
		return 0, err
	}
	cutoff := normalizeTimestamp(now)
	s.mu.Lock()
	defer s.mu.Unlock()
	var pruned int64
	for _, mm := range s.mounts {
		for mid, m := range mm {
			if !m.ExpiresAt.After(cutoff) {
				delete(mm, mid)
				pruned++
			}
		}
	}
	return pruned, nil
}
