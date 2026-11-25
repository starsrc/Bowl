package projection

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

const (
	DefaultMountLeaseTTL     = 30 * time.Second
	DefaultSyncWriteTimeout  = 200 * time.Millisecond
	DefaultReconcileInterval = 5 * time.Second
	DefaultSharedCacheBytes  = 64 << 20
	DefaultGCInterval        = 30 * time.Second
)

type ManagerOptions struct {
	StorageRoot             string
	Store                   Store
	Now                     func() time.Time
	MountLeaseTTL           time.Duration
	SyncWriteTimeout        time.Duration
	FlushInterval           time.Duration
	ReconcileInterval       time.Duration
	ActorIdleTTL            time.Duration
	GCInterval              time.Duration
	DisablePeriodicGC       bool
	DisableGC               bool
	OpTimeout               time.Duration
	SharedCacheBytes        int64
	DisableStartupReconcile bool
}

const (
	diskSlotStripes   = 1024
	diskSlotCacheLine = 64
)

type stripeDisk struct {
	id    uuid.UUID
	entry *diskEntry
}

type diskSlotCore struct {
	opMu   sync.RWMutex
	openMu sync.Mutex
	disks  atomic.Pointer[[]stripeDisk]
}

type diskSlot struct {
	diskSlotCore
	_ [(diskSlotCacheLine - unsafe.Sizeof(diskSlotCore{})%diskSlotCacheLine) % diskSlotCacheLine]byte
}

type diskEntry struct {
	store           *engine.DiskStore
	lastUsedNano    atomic.Int64
	lastSampledUsed atomic.Int64
}

type ControlManager struct {
	disksDir          string
	store             Store
	queue             *Queue
	cache             *pebble.Cache
	now               func() time.Time
	closeDisk         func(*engine.DiskStore) error
	mountLeaseTTL     time.Duration
	syncWriteTimeout  time.Duration
	reconcileInterval time.Duration
	actorIdleTTL      time.Duration
	touchIntervalNano int64
	gcInterval        time.Duration
	disableGC         bool
	opTimeout         time.Duration

	slots  [diskSlotStripes]diskSlot
	closed atomic.Bool

	openCount atomic.Int64

	bgCtx       context.Context
	bgCancel    context.CancelFunc
	bgWG        sync.WaitGroup
	startupDone chan struct{}
	startupErr  error

	closeOnce sync.Once
	closeErr  error
}

func NewControlManager(opts ManagerOptions) (*ControlManager, error) {
	if opts.StorageRoot == "" {
		return nil, fmt.Errorf("%w: storage root is required", ErrInvalidArgument)
	}
	disksDir := filepath.Join(opts.StorageRoot, "disks")
	if err := os.MkdirAll(disksDir, 0o755); err != nil {
		return nil, err
	}

	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	leaseTTL := opts.MountLeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultMountLeaseTTL
	}
	syncWriteTimeout := opts.SyncWriteTimeout
	if syncWriteTimeout <= 0 {
		syncWriteTimeout = DefaultSyncWriteTimeout
	}
	flushInterval := opts.FlushInterval
	if flushInterval <= 0 {
		flushInterval = DefaultFlushInterval
	}
	reconcileInterval := opts.ReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = DefaultReconcileInterval
	}
	opTimeout := opts.OpTimeout
	if opTimeout <= 0 {
		opTimeout = DefaultOpTimeout
	}
	cacheBytes := opts.SharedCacheBytes
	if cacheBytes <= 0 {
		cacheBytes = DefaultSharedCacheBytes
	}
	gcInterval := opts.GCInterval
	if opts.DisableGC || opts.DisablePeriodicGC {
		gcInterval = 0
	} else if gcInterval <= 0 {
		gcInterval = DefaultGCInterval
	}
	var touchIntervalNano int64
	if opts.ActorIdleTTL > 0 {
		touchIntervalNano = max(opts.ActorIdleTTL.Nanoseconds()/8, int64(1))
	}

	cache := pebble.NewCache(cacheBytes)
	q := NewQueue(opts.Store, QueueConfig{
		FlushInterval: flushInterval,
		OpTimeout:     opTimeout,
	})

	bgCtx, bgCancel := context.WithCancel(context.Background())
	m := &ControlManager{
		disksDir:          disksDir,
		store:             opts.Store,
		queue:             q,
		cache:             cache,
		now:               nowFn,
		closeDisk:         (*engine.DiskStore).Close,
		mountLeaseTTL:     leaseTTL,
		syncWriteTimeout:  syncWriteTimeout,
		reconcileInterval: reconcileInterval,
		actorIdleTTL:      opts.ActorIdleTTL,
		touchIntervalNano: touchIntervalNano,
		gcInterval:        gcInterval,
		disableGC:         opts.DisableGC,
		opTimeout:         opTimeout,
		bgCtx:             bgCtx,
		bgCancel:          bgCancel,
		startupDone:       make(chan struct{}),
	}

	if opts.DisableStartupReconcile {
		close(m.startupDone)
	} else {
		m.bgWG.Go(func() {
			defer close(m.startupDone)
			m.startupErr = m.ReconcileAll(m.bgCtx)
		})
	}

	m.bgWG.Go(m.runMaintenanceLoop)

	return m, nil
}

func (m *ControlManager) Queue() *Queue {
	return m.queue
}

func (m *ControlManager) OpenCount() int64 {
	return m.openCount.Load()
}

func (m *ControlManager) IsOpen(diskID uuid.UUID) bool {
	if m.closed.Load() {
		return false
	}
	return lookupOpenEntry(m.getSlot(diskID), diskID) != nil
}

func (m *ControlManager) WaitStartupReconcile(ctx context.Context) error {
	select {
	case <-m.startupDone:
		return m.startupErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *ControlManager) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.closeOnce.Do(func() {
		m.bgCancel()
		m.bgWG.Wait()

		m.closed.Store(true)

		var detached []stripeDisk
		for i := range m.slots {
			m.slots[i].opMu.Lock()
			if entries := m.slots[i].disks.Swap(nil); entries != nil {
				detached = append(detached, *entries...)
			}
			m.slots[i].opMu.Unlock()
		}

		for _, item := range detached {
			m.queue.RecordUsage(item.id, item.entry.store.UsedBytes())
		}

		var errs []error
		if qErr := m.queue.Close(ctx); qErr != nil {
			errs = append(errs, qErr)
		}
		for _, item := range detached {
			if cErr := item.entry.store.Close(); cErr != nil {
				errs = append(errs, cErr)
			}
		}
		if m.cache != nil {
			m.cache.Unref()
		}
		m.closeErr = errors.Join(errs...)
	})
	return m.closeErr
}

func (m *ControlManager) diskDir(diskID uuid.UUID) string {
	return filepath.Join(m.disksDir, diskID.String())
}

func diskExistsOnFS(dir string) bool {
	info, err := os.Stat(filepath.Join(dir, "pebble"))
	return err == nil && info.IsDir()
}

func diskSlotIndex(diskID uuid.UUID) uint64 {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	for _, b := range diskID {
		h ^= uint64(b)
		h *= prime64
	}
	return h & (diskSlotStripes - 1)
}

func (m *ControlManager) getSlot(diskID uuid.UUID) *diskSlot {
	return &m.slots[diskSlotIndex(diskID)]
}

func lookupOpenEntry(slot *diskSlot, diskID uuid.UUID) *diskEntry {
	entries := slot.disks.Load()
	if entries == nil {
		return nil
	}
	for _, item := range *entries {
		if item.id == diskID {
			return item.entry
		}
	}
	return nil
}

func (s *diskSlot) upsert(diskID uuid.UUID, entry *diskEntry) {
	cur := s.disks.Load()
	var next []stripeDisk
	if cur != nil {
		next = make([]stripeDisk, len(*cur), len(*cur)+1)
		copy(next, *cur)
		for i := range next {
			if next[i].id == diskID {
				next[i].entry = entry
				s.disks.Store(&next)
				return
			}
		}
	} else {
		next = make([]stripeDisk, 0, 1)
	}
	next = append(next, stripeDisk{id: diskID, entry: entry})
	s.disks.Store(&next)
}

func (s *diskSlot) remove(diskID uuid.UUID) (*diskEntry, bool) {
	cur := s.disks.Load()
	if cur == nil {
		return nil, false
	}
	old := *cur
	idx := -1
	for i := range old {
		if old[i].id == diskID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, false
	}
	removed := old[idx].entry
	if len(old) == 1 {
		s.disks.Store(nil)
		return removed, true
	}
	next := make([]stripeDisk, 0, len(old)-1)
	next = append(next, old[:idx]...)
	next = append(next, old[idx+1:]...)
	s.disks.Store(&next)
	return removed, true
}

func (m *ControlManager) newDiskEntry(ds *engine.DiskStore, sampledUsed int64) *diskEntry {
	entry := &diskEntry{store: ds}
	entry.lastSampledUsed.Store(sampledUsed)
	if m.actorIdleTTL > 0 {
		entry.lastUsedNano.Store(m.now().UnixNano())
	}
	return entry
}

func (m *ControlManager) touchLastUsed(entry *diskEntry) {
	if m.actorIdleTTL <= 0 {
		return
	}
	nowNano := m.now().UnixNano()
	if nowNano-entry.lastUsedNano.Load() >= m.touchIntervalNano {
		entry.lastUsedNano.Store(nowNano)
	}
}

func (m *ControlManager) openDiskRaw(diskID uuid.UUID, name string, maxBytes uint64, enableBackgroundGC bool) (*engine.DiskStore, error) {
	m.openCount.Add(1)
	var (
		gcInterval   time.Duration
		enableGCWake bool
	)
	if enableBackgroundGC && !m.disableGC {
		gcInterval = m.gcInterval
		enableGCWake = true
	}
	return engine.OpenDisk(engine.DiskOptions{
		Dir:          m.diskDir(diskID),
		DiskID:       diskID.String(),
		Name:         name,
		MaxBytes:     maxBytes,
		Cache:        m.cache,
		Now:          m.now,
		ActorIdleTTL: m.actorIdleTTL,
		GCInterval:   gcInterval,
		EnableGCWake: enableGCWake,
	})
}

func (m *ControlManager) acquireOpenEntry(slot *diskSlot, diskID uuid.UUID) (*diskEntry, error) {
	if m.closed.Load() {
		return nil, engine.ErrClosed
	}
	if entry := lookupOpenEntry(slot, diskID); entry != nil {
		return entry, nil
	}
	return m.openEntrySlow(slot, diskID)
}

func (m *ControlManager) openEntrySlow(slot *diskSlot, diskID uuid.UUID) (*diskEntry, error) {
	slot.openMu.Lock()
	defer slot.openMu.Unlock()

	if m.closed.Load() {
		return nil, engine.ErrClosed
	}
	if entry := lookupOpenEntry(slot, diskID); entry != nil {
		return entry, nil
	}
	if !diskExistsOnFS(m.diskDir(diskID)) {
		return nil, engine.ErrNotFound
	}

	ds, err := m.openDiskRaw(diskID, "", 0, true)
	if err != nil {
		return nil, err
	}

	entry := m.newDiskEntry(ds, -1)
	if m.closed.Load() {
		_ = m.closeDisk(ds)
		return nil, engine.ErrClosed
	}
	slot.upsert(diskID, entry)
	return entry, nil
}

func (m *ControlManager) withEntry(diskID uuid.UUID, exclusive bool, fn func(*diskEntry) error) error {
	if diskID == uuid.Nil {
		return engine.ErrInvalid
	}
	slot := m.getSlot(diskID)
	if exclusive {
		slot.opMu.Lock()
		defer slot.opMu.Unlock()
	} else {
		slot.opMu.RLock()
		defer slot.opMu.RUnlock()
	}
	entry, err := m.acquireOpenEntry(slot, diskID)
	if err != nil {
		return err
	}
	err = fn(entry)
	m.touchLastUsed(entry)
	return err
}

func (m *ControlManager) withTempDisk(diskID uuid.UUID, fn func(*engine.DiskStore) error) (retErr error) {
	if !diskExistsOnFS(m.diskDir(diskID)) {
		return engine.ErrNotFound
	}
	ds, err := m.openDiskRaw(diskID, "", 0, false)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := m.closeDisk(ds)
		if retErr == nil {
			retErr = closeErr
		}
	}()
	return fn(ds)
}

func (m *ControlManager) withDiskReadOnly(diskID uuid.UUID, fn func(*engine.DiskStore) error) error {
	slot := m.getSlot(diskID)
	slot.opMu.RLock()
	defer slot.opMu.RUnlock()

	if m.closed.Load() {
		return engine.ErrClosed
	}
	if entry := lookupOpenEntry(slot, diskID); entry != nil {
		err := fn(entry.store)
		m.touchLastUsed(entry)
		return err
	}

	return m.withDiskReadOnlyCold(slot, diskID, fn)
}

func (m *ControlManager) withDiskReadOnlyCold(slot *diskSlot, diskID uuid.UUID, fn func(*engine.DiskStore) error) error {
	slot.openMu.Lock()
	if m.closed.Load() {
		slot.openMu.Unlock()
		return engine.ErrClosed
	}
	if entry := lookupOpenEntry(slot, diskID); entry != nil {
		slot.openMu.Unlock()
		err := fn(entry.store)
		m.touchLastUsed(entry)
		return err
	}
	defer slot.openMu.Unlock()
	return m.withTempDisk(diskID, fn)
}

func (m *ControlManager) CreateDisk(ctx context.Context, name string, maxBytes uint64) (DiskRecord, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return DiskRecord{}, err
		}
	}
	if len(name) > engine.MaxFileNameBytes || strings.ContainsRune(name, '\x00') {
		return DiskRecord{}, engine.ErrInvalid
	}
	if maxBytes == 0 || maxBytes > math.MaxInt64 {
		return DiskRecord{}, engine.ErrInvalid
	}

	diskID, err := uuid.NewV7()
	if err != nil {
		return DiskRecord{}, err
	}

	slot := m.getSlot(diskID)
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if m.closed.Load() {
		return DiskRecord{}, engine.ErrClosed
	}

	ds, err := m.openDiskRaw(diskID, name, maxBytes, true)
	if err != nil {
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, err
	}

	meta := ds.DiskMeta()
	mainBranch, brErr := ds.GetBranch(meta.DefaultBranch)
	if brErr != nil {
		_ = m.closeDisk(ds)
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, brErr
	}
	initCommit, cmtErr := ds.GetCommit(mainBranch.HeadCommitID)
	if cmtErr != nil {
		_ = m.closeDisk(ds)
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, cmtErr
	}
	usedBytes := ds.UsedBytes()

	entry := m.newDiskEntry(ds, int64(usedBytes))
	if m.closed.Load() {
		_ = m.closeDisk(ds)
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, engine.ErrClosed
	}
	slot.upsert(diskID, entry)

	diskRec := diskMetaToRecord(diskID, meta, usedBytes)
	branchRec := branchMetaToRecord(diskID, mainBranch)
	commitRec := commitMetaToRecord(diskID, initCommit)
	m.syncProject(ctx, diskID, true, func(pgCtx context.Context, store Store) error {
		return store.UpsertDisk(pgCtx, diskRec, &branchRec, &commitRec)
	})
	return diskRec, nil
}

func (m *ControlManager) UpdateDisk(ctx context.Context, diskID uuid.UUID, name *string, maxBytes *uint64) (DiskRecord, error) {
	var diskRec DiskRecord
	err := m.withEntry(diskID, true, func(entry *diskEntry) error {
		meta, uErr := entry.store.UpdateDiskMeta(ctx, name, maxBytes)
		if uErr != nil {
			return uErr
		}
		usedBytes := entry.store.UsedBytes()
		entry.lastSampledUsed.Store(int64(usedBytes))
		diskRec = diskMetaToRecord(diskID, meta, usedBytes)
		m.syncProject(ctx, diskID, true, func(pgCtx context.Context, store Store) error {
			return store.UpdateDisk(pgCtx, diskRec)
		})
		return nil
	})
	if err != nil {
		return DiskRecord{}, err
	}
	return diskRec, nil
}

func (m *ControlManager) syncProject(ctx context.Context, diskID uuid.UUID, isCatalog bool, fn func(context.Context, Store) error) {
	if m.store == nil || m.queue.IsDirty(diskID) {
		m.queue.MarkDirty(diskID, isCatalog)
		return
	}
	parentCtx := ctx
	if parentCtx == nil {
		parentCtx = m.bgCtx
	}
	if parentCtx.Err() != nil {
		m.queue.MarkDirty(diskID, isCatalog)
		return
	}
	pgCtx, cancel := context.WithTimeout(parentCtx, m.syncWriteTimeout)
	defer cancel()
	if err := fn(pgCtx, m.store); err != nil {
		m.queue.MarkDirty(diskID, isCatalog)
	}
}

func (m *ControlManager) snapshotDiskUsageRecord(diskID uuid.UUID, entry *diskEntry) DiskRecord {
	used := entry.store.UsedBytes()
	entry.lastSampledUsed.Store(int64(used))
	return diskMetaToRecord(diskID, entry.store.DiskMeta(), used)
}

func (m *ControlManager) openDiskUsedBytes(diskID uuid.UUID) (uint64, bool) {
	if m.closed.Load() {
		return 0, false
	}
	slot := m.getSlot(diskID)
	slot.opMu.RLock()
	defer slot.opMu.RUnlock()
	if m.closed.Load() {
		return 0, false
	}
	entry := lookupOpenEntry(slot, diskID)
	if entry == nil || entry.store == nil {
		return 0, false
	}
	return entry.store.UsedBytes(), true
}

func (m *ControlManager) DeleteDisk(ctx context.Context, diskID uuid.UUID) error {
	if diskID == uuid.Nil {
		return engine.ErrInvalid
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	slot := m.getSlot(diskID)
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if m.closed.Load() {
		return engine.ErrClosed
	}
	entry := lookupOpenEntry(slot, diskID)

	now := m.now()
	if entry != nil {
		mounted, err := entry.store.HasActiveMounts("", now)
		if err != nil {
			return err
		}
		if mounted {
			return engine.ErrDiskMounted
		}
		slot.remove(diskID)
		if closeErr := m.closeDisk(entry.store); closeErr != nil {
			return closeErr
		}
	} else {
		var mounted bool
		err := m.withTempDisk(diskID, func(ds *engine.DiskStore) error {
			var mErr error
			mounted, mErr = ds.HasActiveMounts("", now)
			return mErr
		})
		if err != nil {
			return err
		}
		if mounted {
			return engine.ErrDiskMounted
		}
	}

	if err := os.RemoveAll(m.diskDir(diskID)); err != nil {
		return err
	}
	m.syncProject(ctx, diskID, true, func(pgCtx context.Context, store Store) error {
		return store.DeleteDisk(pgCtx, diskID)
	})
	return nil
}

func (m *ControlManager) ForkBranch(ctx context.Context, diskID uuid.UUID, srcBranch, newBranch, message string) (CommitRecord, BranchRecord, error) {
	var (
		cmtRec   CommitRecord
		newBrRec BranchRecord
	)
	err := m.withEntry(diskID, true, func(entry *diskEntry) error {
		cmt, newBr, fErr := entry.store.Fork(ctx, srcBranch, newBranch, message)
		if fErr != nil {
			return fErr
		}
		cmtRec = commitMetaToRecord(diskID, cmt)
		newBrRec = branchMetaToRecord(diskID, newBr)

		if srcBr, srcErr := entry.store.GetBranch(srcBranch); srcErr == nil {
			branches := []BranchRecord{branchMetaToRecord(diskID, srcBr), newBrRec}
			commits := []CommitRecord{cmtRec}
			m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
				return store.UpsertBranchAndCommit(pgCtx, branches, commits, nil)
			})
		} else {
			m.queue.MarkDirty(diskID, false)
		}
		return nil
	})
	if err != nil {
		return CommitRecord{}, BranchRecord{}, err
	}
	return cmtRec, newBrRec, nil
}

func (m *ControlManager) DeleteBranch(ctx context.Context, diskID uuid.UUID, branch string) error {
	return m.withEntry(diskID, true, func(entry *diskEntry) error {
		if delErr := entry.store.DeleteBranch(ctx, branch); delErr != nil {
			return delErr
		}
		diskRec := m.snapshotDiskUsageRecord(diskID, entry)
		m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
			if err := store.DeleteBranch(pgCtx, diskID, branch); err != nil {
				return err
			}
			return store.UpdateDisk(pgCtx, diskRec)
		})
		return nil
	})
}

func (m *ControlManager) Commit(ctx context.Context, diskID uuid.UUID, branch, message string) (CommitRecord, error) {
	var cmtRec CommitRecord
	err := m.withEntry(diskID, true, func(entry *diskEntry) error {
		cmt, cErr := entry.store.Commit(ctx, branch, message)
		if cErr != nil {
			return cErr
		}
		cmtRec = commitMetaToRecord(diskID, cmt)
		if br, brErr := entry.store.GetBranch(branch); brErr == nil {
			branches := []BranchRecord{branchMetaToRecord(diskID, br)}
			commits := []CommitRecord{cmtRec}
			m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
				return store.UpsertBranchAndCommit(pgCtx, branches, commits, nil)
			})
		} else {
			m.queue.MarkDirty(diskID, false)
		}
		return nil
	})
	if err != nil {
		return CommitRecord{}, err
	}
	return cmtRec, nil
}

func (m *ControlManager) ResetBranch(ctx context.Context, diskID uuid.UUID, branch string, targetCommitID uint64) (BranchRecord, error) {
	var brRec BranchRecord
	err := m.withEntry(diskID, true, func(entry *diskEntry) error {
		br, rErr := entry.store.Reset(ctx, branch, targetCommitID)
		if rErr != nil {
			return rErr
		}
		brRec = branchMetaToRecord(diskID, br)
		diskRec := m.snapshotDiskUsageRecord(diskID, entry)
		m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
			if err := store.UpsertBranchAndCommit(pgCtx, []BranchRecord{brRec}, nil, nil); err != nil {
				return err
			}
			return store.UpdateDisk(pgCtx, diskRec)
		})
		return nil
	})
	if err != nil {
		return BranchRecord{}, err
	}
	return brRec, nil
}

func (m *ControlManager) MergeBranch(ctx context.Context, diskID uuid.UUID, opts engine.MergeOptions) (*engine.MergeResult, error) {
	var res *engine.MergeResult
	err := m.withEntry(diskID, !opts.DryRun, func(entry *diskEntry) error {
		var mErr error
		res, mErr = entry.store.Merge(ctx, opts)
		if mErr != nil {
			return mErr
		}
		if opts.DryRun || res.UpToDate {
			return nil
		}
		tgtBr, brErr := entry.store.GetBranch(opts.TargetBranch)
		if brErr == nil {
			branches := []BranchRecord{branchMetaToRecord(diskID, tgtBr)}
			var commits []CommitRecord
			ready := true
			if !res.FastForward && res.CommitID != 0 {
				if cmt, cmtErr := entry.store.GetCommit(res.CommitID); cmtErr == nil {
					commits = []CommitRecord{commitMetaToRecord(diskID, cmt)}
				} else {
					ready = false
				}
			}
			if ready {
				m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
					return store.UpsertBranchAndCommit(pgCtx, branches, commits, nil)
				})
			} else {
				m.queue.MarkDirty(diskID, false)
			}
		} else {
			m.queue.MarkDirty(diskID, false)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *ControlManager) DeleteCommit(ctx context.Context, diskID uuid.UUID, commitID uint64) error {
	return m.withEntry(diskID, true, func(entry *diskEntry) error {
		if delErr := entry.store.DeleteCommit(ctx, commitID); delErr != nil {
			return delErr
		}
		diskRec := m.snapshotDiskUsageRecord(diskID, entry)
		m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
			if err := store.DeleteCommits(pgCtx, diskID, []uint64{commitID}); err != nil {
				return err
			}
			return store.UpdateDisk(pgCtx, diskRec)
		})
		return nil
	})
}

func (m *ControlManager) PruneCommits(ctx context.Context, diskID uuid.UUID) (int, error) {
	var pruned int
	err := m.withEntry(diskID, true, func(entry *diskEntry) error {
		var pErr error
		pruned, pErr = entry.store.PruneCommits(ctx)
		if pErr != nil {
			return pErr
		}
		if pruned > 0 {
			if rawSnap, snapErr := entry.store.SnapshotControlMeta(m.now()); snapErr == nil {
				entry.lastSampledUsed.Store(int64(rawSnap.UsedBytes))
				snap := controlSnapshotToDiskSnapshot(diskID, rawSnap)
				m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
					return store.ReplaceDiskSnapshot(pgCtx, snap)
				})
			} else {
				m.queue.MarkDirty(diskID, false)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return pruned, nil
}

func (m *ControlManager) MountBranch(ctx context.Context, diskID uuid.UUID, branch string, readOnly, force bool) (MountRecord, error) {
	var mountRec MountRecord
	err := m.withEntry(diskID, true, func(entry *diskEntry) error {
		var deletedRWIDs []uuid.UUID
		if !readOnly {
			if existing, listErr := entry.store.ListMounts(branch, m.now()); listErr == nil {
				for _, em := range existing {
					if !em.ReadOnly {
						if parsed, pErr := uuid.Parse(em.ID); pErr == nil && parsed != uuid.Nil {
							deletedRWIDs = append(deletedRWIDs, parsed)
						}
					}
				}
			}
		}

		mm, mErr := entry.store.MountBranch(ctx, branch, readOnly, force, m.mountLeaseTTL)
		if mErr != nil {
			return mErr
		}
		rec, convErr := mountMetaToRecord(diskID, mm)
		if convErr != nil {
			m.queue.MarkDirty(diskID, false)
			return convErr
		}
		mountRec = rec

		if !readOnly {
			if br, brErr := entry.store.GetBranch(branch); brErr == nil {
				brRec := branchMetaToRecord(diskID, br)
				m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
					return store.UpsertMount(pgCtx, mountRec, &brRec, deletedRWIDs)
				})
			} else {
				m.queue.MarkDirty(diskID, false)
			}
		} else {
			m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
				return store.UpsertMount(pgCtx, mountRec, nil, nil)
			})
		}
		return nil
	})
	if err != nil {
		return MountRecord{}, err
	}
	return mountRec, nil
}

func (m *ControlManager) RenewMount(ctx context.Context, diskID uuid.UUID, branch string, mountID uuid.UUID, readOnly bool, epoch uint64) (MountRecord, error) {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return MountRecord{}, engine.ErrInvalid
	}
	var mountRec MountRecord
	err := m.withEntry(diskID, false, func(entry *diskEntry) error {
		mm, rErr := entry.store.RenewMount(ctx, branch, mountID.String(), readOnly, epoch, m.mountLeaseTTL)
		if rErr != nil {
			return rErr
		}
		rec, convErr := mountMetaToRecord(diskID, mm)
		if convErr != nil {
			return convErr
		}
		mountRec = rec
		m.queue.RecordMountRenew(diskID, mountRec.MountID, mountRec.ExpiresAt)
		return nil
	})
	if err != nil {
		return MountRecord{}, err
	}
	return mountRec, nil
}

func (m *ControlManager) UnmountBranch(ctx context.Context, diskID, mountID uuid.UUID) error {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return engine.ErrInvalid
	}
	return m.withEntry(diskID, true, func(entry *diskEntry) error {
		if umErr := entry.store.Unmount(ctx, mountID.String()); umErr != nil {
			return umErr
		}
		m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
			return store.DeleteMounts(pgCtx, diskID, []uuid.UUID{mountID}, nil)
		})
		return nil
	})
}

func (m *ControlManager) EvictMounts(ctx context.Context, diskID uuid.UUID, branch string, mountID uuid.UUID) (engine.EvictMountsResult, error) {
	var res engine.EvictMountsResult
	err := m.withEntry(diskID, true, func(entry *diskEntry) error {
		mountIDStr := ""
		if mountID != uuid.Nil {
			mountIDStr = mountID.String()
		}
		var eErr error
		res, eErr = entry.store.EvictMounts(ctx, branch, mountIDStr)
		if eErr != nil {
			return eErr
		}

		evictedUUIDs := make([]uuid.UUID, 0, len(res.EvictedMountIDs))
		for _, rawID := range res.EvictedMountIDs {
			if parsed, pErr := uuid.Parse(rawID); pErr == nil && parsed != uuid.Nil {
				evictedUUIDs = append(evictedUUIDs, parsed)
			}
		}
		updatedBranches := make([]BranchRecord, 0, len(res.UpdatedBranches))
		for _, b := range res.UpdatedBranches {
			updatedBranches = append(updatedBranches, branchMetaToRecord(diskID, b))
		}
		if len(evictedUUIDs) > 0 || len(updatedBranches) > 0 {
			m.syncProject(ctx, diskID, false, func(pgCtx context.Context, store Store) error {
				return store.DeleteMounts(pgCtx, diskID, evictedUUIDs, updatedBranches)
			})
		}
		return nil
	})
	if err != nil {
		return engine.EvictMountsResult{}, err
	}
	return res, nil
}

func (m *ControlManager) Diff(ctx context.Context, diskID uuid.UUID, opts engine.DiffOptions) (*engine.DiffResult, error) {
	var res *engine.DiffResult
	err := m.withEntry(diskID, false, func(entry *diskEntry) error {
		var dErr error
		res, dErr = entry.store.Diff(ctx, opts)
		return dErr
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *ControlManager) DiffCommits(ctx context.Context, diskID uuid.UUID, opts engine.DiffCommitsOptions) (*engine.DiffResult, error) {
	var res *engine.DiffResult
	err := m.withEntry(diskID, false, func(entry *diskEntry) error {
		var dErr error
		res, dErr = entry.store.DiffCommits(ctx, opts)
		return dErr
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *ControlManager) DiffWork(ctx context.Context, diskID uuid.UUID, branch string, opts engine.DiffWorkOptions) (*engine.DiffResult, error) {
	var res *engine.DiffResult
	err := m.withEntry(diskID, false, func(entry *diskEntry) error {
		var dErr error
		res, dErr = entry.store.DiffWork(ctx, branch, opts)
		return dErr
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *ControlManager) ReadCommitNode(ctx context.Context, diskID uuid.UUID, commitID, ino uint64, offset int64, size int) (engine.CommitNodeResult, error) {
	var res engine.CommitNodeResult
	err := m.withEntry(diskID, false, func(entry *diskEntry) error {
		var rErr error
		res, rErr = entry.store.ReadCommitNode(ctx, commitID, ino, offset, size)
		return rErr
	})
	if err != nil {
		return engine.CommitNodeResult{}, err
	}
	return res, nil
}

func (m *ControlManager) MountLeaseTTL() time.Duration {
	return m.mountLeaseTTL
}

func (m *ControlManager) WithDisk(ctx context.Context, diskID uuid.UUID, fn func(*engine.DiskStore) error) error {
	if diskID == uuid.Nil {
		return engine.ErrInvalid
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	slot := m.getSlot(diskID)
	slot.opMu.RLock()
	defer slot.opMu.RUnlock()

	entry, err := m.acquireOpenEntry(slot, diskID)
	if err != nil {
		return err
	}
	err = fn(entry.store)
	m.touchLastUsed(entry)
	return err
}

func (m *ControlManager) Disk(ctx context.Context, diskID uuid.UUID) (*engine.DiskStore, error) {
	if diskID == uuid.Nil {
		return nil, engine.ErrInvalid
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	var ds *engine.DiskStore
	err := m.withEntry(diskID, false, func(entry *diskEntry) error {
		ds = entry.store
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func (m *ControlManager) ListDisks(ctx context.Context, q ListDisksQuery) (ListDisksPage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.store == nil {
		return ListDisksPage{}, ErrUnavailable
	}
	dirtyCatalog := m.queue.TakeDirtyCatalogDisks()
	if len(dirtyCatalog) > 0 {
		syncCtx, cancel := context.WithTimeout(ctx, m.syncWriteTimeout)
		defer cancel()
		for diskID := range dirtyCatalog {
			if err := m.reconcileDisk(syncCtx, diskID, true, true); err != nil {
				if ctx.Err() != nil {
					return ListDisksPage{}, ctx.Err()
				}
				return ListDisksPage{}, ErrUnavailable
			}
		}
	}
	page, err := m.store.ListDisks(ctx, q)
	if err != nil {
		if ctx.Err() == nil && IsUnavailable(err) && !errors.Is(err, ErrUnavailable) {
			return ListDisksPage{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		return ListDisksPage{}, err
	}
	for i := range page.Disks {
		if used, ok := m.openDiskUsedBytes(page.Disks[i].DiskID); ok {
			page.Disks[i].UsedBytes = used
		}
	}
	return page, nil
}

func (m *ControlManager) shouldFallbackToPebble(diskID uuid.UUID) bool {
	return m.store == nil || m.queue.IsDirty(diskID)
}

func readWithFallback[T any](
	ctx context.Context,
	m *ControlManager,
	diskID uuid.UUID,
	pgRead func(context.Context) (T, error),
	pebbleRead func(*engine.DiskStore) (T, error),
) (T, error) {
	runFallback := func() (T, error) {
		var zero, out T
		err := m.withDiskReadOnly(diskID, func(ds *engine.DiskStore) error {
			var rErr error
			out, rErr = pebbleRead(ds)
			return rErr
		})
		if err != nil {
			if errors.Is(err, engine.ErrNotFound) {
				return zero, ErrNotFound
			}
			return zero, err
		}
		return out, nil
	}

	if m.shouldFallbackToPebble(diskID) {
		return runFallback()
	}
	res, err := pgRead(ctx)
	if err != nil {
		if ctx.Err() == nil && IsUnavailable(err) {
			return runFallback()
		}
		var zero T
		return zero, err
	}
	return res, nil
}

func (m *ControlManager) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	return readWithFallback(
		ctx,
		m,
		diskID,
		func(pgCtx context.Context) (DiskRecord, error) {
			rec, err := m.store.GetDisk(pgCtx, diskID)
			if err != nil {
				return DiskRecord{}, err
			}
			if used, ok := m.openDiskUsedBytes(diskID); ok {
				rec.UsedBytes = used
			}
			return rec, nil
		},
		func(ds *engine.DiskStore) (DiskRecord, error) {
			return diskMetaToRecord(diskID, ds.DiskMeta(), ds.UsedBytes()), nil
		},
	)
}

func (m *ControlManager) ListBranches(ctx context.Context, diskID uuid.UUID) ([]BranchRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	return readWithFallback(
		ctx,
		m,
		diskID,
		func(pgCtx context.Context) ([]BranchRecord, error) {
			return m.store.ListBranches(pgCtx, diskID)
		},
		func(ds *engine.DiskStore) ([]BranchRecord, error) {
			raw, err := ds.ListBranches()
			if err != nil {
				return nil, err
			}
			out := make([]BranchRecord, 0, len(raw))
			for _, b := range raw {
				out = append(out, branchMetaToRecord(diskID, b))
			}
			return out, nil
		},
	)
}

func (m *ControlManager) ListCommits(ctx context.Context, q ListCommitsQuery) (ListCommitsPage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
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

	return readWithFallback(
		ctx,
		m,
		q.DiskID,
		func(pgCtx context.Context) (ListCommitsPage, error) {
			return m.store.ListCommits(pgCtx, q)
		},
		func(ds *engine.DiskStore) (ListCommitsPage, error) {
			raw, lErr := ds.ListCommits()
			if lErr != nil {
				return ListCommitsPage{}, lErr
			}
			filtered := make([]CommitRecord, 0, min(len(raw), int(limit)+1))
			for _, c := range raw {
				rec := commitMetaToRecord(q.DiskID, c)
				if q.Kind != "" && rec.Kind != q.Kind {
					continue
				}
				if hasCursor && rec.CommitID >= cursorID {
					continue
				}
				filtered = append(filtered, rec)
				if len(filtered) > int(limit) {
					break
				}
			}
			var page ListCommitsPage
			if len(filtered) > int(limit) {
				filtered = filtered[:limit]
				page.NextPageToken = EncodeCommitPageCursor(filtered[len(filtered)-1].CommitID)
			}
			page.Commits = filtered
			return page, nil
		},
	)
}

func (m *ControlManager) ListMounts(ctx context.Context, diskID uuid.UUID, branchName string) ([]MountRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	now := m.now()
	cutoff := normalizeTimestamp(now)
	return readWithFallback(
		ctx,
		m,
		diskID,
		func(pgCtx context.Context) ([]MountRecord, error) {
			return m.store.ListMounts(pgCtx, diskID, branchName, now)
		},
		func(ds *engine.DiskStore) ([]MountRecord, error) {
			raw, lErr := ds.ListMounts(branchName, now)
			if lErr != nil {
				return nil, lErr
			}
			out := make([]MountRecord, 0, len(raw))
			for _, mm := range raw {
				rec, convErr := mountMetaToRecord(diskID, mm)
				if convErr != nil {
					continue
				}
				if !rec.ExpiresAt.After(cutoff) {
					continue
				}
				out = append(out, rec)
			}
			slices.SortFunc(out, func(a, b MountRecord) int {
				return compareUUID(a.MountID, b.MountID)
			})
			return out, nil
		},
	)
}

func nanoToTime(ns int64) time.Time {
	return time.Unix(0, ns).UTC().Truncate(time.Microsecond)
}

func engineCommitKindToString(k engine.CommitKind) string {
	switch k {
	case engine.CommitKindInit:
		return CommitKindInit
	case engine.CommitKindManual:
		return CommitKindManual
	case engine.CommitKindFork:
		return CommitKindFork
	case engine.CommitKindMerge:
		return CommitKindMerge
	default:
		return ""
	}
}

func diskMetaToRecord(diskID uuid.UUID, meta engine.DiskMeta, usedBytes uint64) DiskRecord {
	return DiskRecord{
		DiskID:        diskID,
		Name:          meta.Name,
		DefaultBranch: meta.DefaultBranch,
		MaxBytes:      meta.MaxBytes,
		UsedBytes:     usedBytes,
		CreatedAt:     nanoToTime(meta.CreatedAt),
		UpdatedAt:     nanoToTime(meta.UpdatedAt),
	}
}

func branchMetaToRecord(diskID uuid.UUID, meta engine.BranchMeta) BranchRecord {
	return BranchRecord{
		DiskID:       diskID,
		Name:         meta.Name,
		HeadCommitID: meta.HeadCommitID,
		WriterEpoch:  meta.WriterEpoch,
		CreatedAt:    nanoToTime(meta.CreatedAt),
		UpdatedAt:    nanoToTime(meta.UpdatedAt),
	}
}

func commitMetaToRecord(diskID uuid.UUID, meta engine.CommitMeta) CommitRecord {
	return CommitRecord{
		DiskID:        diskID,
		CommitID:      meta.ID,
		ParentID:      meta.ParentID,
		MergeParentID: meta.MergeParentID,
		Kind:          engineCommitKindToString(meta.Kind),
		Message:       meta.Message,
		CreatedAt:     nanoToTime(meta.CreatedAt),
	}
}

func mountMetaToRecord(diskID uuid.UUID, meta engine.MountMeta) (MountRecord, error) {
	mountID, err := uuid.Parse(meta.ID)
	if err != nil || mountID == uuid.Nil {
		return MountRecord{}, fmt.Errorf("%w: invalid mount id %q", ErrInvalidArgument, meta.ID)
	}
	return MountRecord{
		DiskID:      diskID,
		MountID:     mountID,
		BranchName:  meta.BranchName,
		ReadOnly:    meta.ReadOnly,
		WriterEpoch: meta.WriterEpoch,
		MountedAt:   nanoToTime(meta.MountedAt),
		ExpiresAt:   nanoToTime(meta.ExpiresAt),
	}, nil
}

func controlSnapshotToDiskSnapshot(diskID uuid.UUID, raw engine.ControlSnapshot) DiskSnapshot {
	branches := make([]BranchRecord, 0, len(raw.Branches))
	for _, b := range raw.Branches {
		branches = append(branches, branchMetaToRecord(diskID, b))
	}
	commits := make([]CommitRecord, 0, len(raw.Commits))
	for _, c := range raw.Commits {
		commits = append(commits, commitMetaToRecord(diskID, c))
	}
	mounts := make([]MountRecord, 0, len(raw.Mounts))
	for _, mm := range raw.Mounts {
		if rec, err := mountMetaToRecord(diskID, mm); err == nil {
			mounts = append(mounts, rec)
		}
	}
	return DiskSnapshot{
		Disk:     diskMetaToRecord(diskID, raw.Disk, raw.UsedBytes),
		Branches: branches,
		Commits:  commits,
		Mounts:   mounts,
	}
}
