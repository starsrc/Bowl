package projection

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

const (
	DefaultMountLeaseTTL     = 30 * time.Second
	DefaultBarrierTimeout    = 200 * time.Millisecond
	DefaultReconcileInterval = 5 * time.Second
	DefaultSharedCacheBytes  = 64 << 20
)

type ManagerOptions struct {
	StorageRoot             string
	Store                   Store
	Now                     func() time.Time
	MountLeaseTTL           time.Duration
	BarrierTimeout          time.Duration
	FlushInterval           time.Duration
	ReconcileInterval       time.Duration
	ActorIdleTTL            time.Duration
	DiskQueueSize           int
	OpTimeout               time.Duration
	SharedCacheBytes        int64
	DisableStartupReconcile bool
}

type diskSlot struct {
	opMu   sync.RWMutex
	openMu sync.Mutex
}

type diskEntry struct {
	store           *engine.DiskStore
	lastUsedNano    atomic.Int64
	inFlight        atomic.Int64
	lastSampledUsed atomic.Uint64
	hasSampled      atomic.Bool
}

type ControlManager struct {
	storageRoot       string
	disksDir          string
	store             Store
	queue             *Queue
	cache             *pebble.Cache
	now               func() time.Time
	mountLeaseTTL     time.Duration
	barrierTimeout    time.Duration
	reconcileInterval time.Duration
	actorIdleTTL      time.Duration
	opTimeout         time.Duration

	slotsMu sync.Mutex
	slots   map[uuid.UUID]*diskSlot

	poolMu sync.RWMutex
	closed bool
	disks  map[uuid.UUID]*diskEntry

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
	barrierTimeout := opts.BarrierTimeout
	if barrierTimeout <= 0 {
		barrierTimeout = DefaultBarrierTimeout
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

	cache := pebble.NewCache(cacheBytes)
	q := NewQueue(opts.Store, QueueConfig{
		DiskQueueSize: opts.DiskQueueSize,
		FlushInterval: flushInterval,
		OpTimeout:     opTimeout,
	})

	bgCtx, bgCancel := context.WithCancel(context.Background())
	m := &ControlManager{
		storageRoot:       opts.StorageRoot,
		disksDir:          disksDir,
		store:             opts.Store,
		queue:             q,
		cache:             cache,
		now:               nowFn,
		mountLeaseTTL:     leaseTTL,
		barrierTimeout:    barrierTimeout,
		reconcileInterval: reconcileInterval,
		actorIdleTTL:      opts.ActorIdleTTL,
		opTimeout:         opTimeout,
		slots:             make(map[uuid.UUID]*diskSlot),
		disks:             make(map[uuid.UUID]*diskEntry),
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

	m.bgWG.Go(m.runReconcileLoop)

	if m.actorIdleTTL > 0 {
		m.bgWG.Go(m.runEvictLoop)
	}

	return m, nil
}

func (m *ControlManager) Queue() *Queue {
	return m.queue
}

func (m *ControlManager) OpenCount() int64 {
	return m.openCount.Load()
}

func (m *ControlManager) IsOpen(diskID uuid.UUID) bool {
	m.poolMu.RLock()
	_, ok := m.disks[diskID]
	m.poolMu.RUnlock()
	return ok
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

		m.poolMu.Lock()
		m.closed = true
		openEntries := make(map[uuid.UUID]*diskEntry, len(m.disks))
		maps.Copy(openEntries, m.disks)
		clear(m.disks)
		m.poolMu.Unlock()

		for id, entry := range openEntries {
			if st, stErr := entry.store.StatFS(ctx); stErr == nil {
				m.queue.RecordUsage(id, st.UsedBytes)
			}
		}

		var errs []error
		if qErr := m.queue.Close(ctx); qErr != nil {
			errs = append(errs, qErr)
		}
		for _, entry := range openEntries {
			if cErr := entry.store.Close(); cErr != nil {
				errs = append(errs, cErr)
			}
		}
		m.cache.Unref()
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

func (m *ControlManager) getSlot(diskID uuid.UUID) *diskSlot {
	m.slotsMu.Lock()
	slot, ok := m.slots[diskID]
	if !ok {
		slot = &diskSlot{}
		m.slots[diskID] = slot
	}
	m.slotsMu.Unlock()
	return slot
}

func (m *ControlManager) lockDisk(diskID uuid.UUID) func() {
	slot := m.getSlot(diskID)
	slot.opMu.Lock()
	return slot.opMu.Unlock
}

func (m *ControlManager) rlockDisk(diskID uuid.UUID) func() {
	slot := m.getSlot(diskID)
	slot.opMu.RLock()
	return slot.opMu.RUnlock
}

func (m *ControlManager) isClosed() bool {
	m.poolMu.RLock()
	closed := m.closed
	m.poolMu.RUnlock()
	return closed
}

func (m *ControlManager) openDiskRaw(diskID uuid.UUID, name string, maxBytes uint64) (*engine.DiskStore, error) {
	m.openCount.Add(1)
	return engine.OpenDisk(engine.DiskOptions{
		Dir:          m.diskDir(diskID),
		DiskID:       diskID.String(),
		Name:         name,
		MaxBytes:     maxBytes,
		Cache:        m.cache,
		Now:          m.now,
		ActorIdleTTL: m.actorIdleTTL,
	})
}

func (m *ControlManager) acquireDisk(diskID uuid.UUID) (*diskEntry, func(), error) {
	nowNano := m.now().UnixNano()
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, nil, engine.ErrClosed
	}
	if entry, ok := m.disks[diskID]; ok {
		entry.inFlight.Add(1)
		entry.lastUsedNano.Store(nowNano)
		m.poolMu.RUnlock()
		return entry, func() {
			entry.lastUsedNano.Store(m.now().UnixNano())
			entry.inFlight.Add(-1)
		}, nil
	}
	m.poolMu.RUnlock()

	slot := m.getSlot(diskID)
	slot.openMu.Lock()
	defer slot.openMu.Unlock()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, nil, engine.ErrClosed
	}
	if entry, ok := m.disks[diskID]; ok {
		entry.inFlight.Add(1)
		entry.lastUsedNano.Store(nowNano)
		m.poolMu.RUnlock()
		return entry, func() {
			entry.lastUsedNano.Store(m.now().UnixNano())
			entry.inFlight.Add(-1)
		}, nil
	}
	m.poolMu.RUnlock()

	if !diskExistsOnFS(m.diskDir(diskID)) {
		return nil, nil, engine.ErrNotFound
	}

	ds, err := m.openDiskRaw(diskID, "", 0)
	if err != nil {
		return nil, nil, err
	}

	entry := &diskEntry{store: ds}
	entry.lastUsedNano.Store(m.now().UnixNano())
	entry.inFlight.Add(1)

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		_ = ds.Close()
		return nil, nil, engine.ErrClosed
	}
	m.disks[diskID] = entry
	m.poolMu.Unlock()

	return entry, func() {
		entry.lastUsedNano.Store(m.now().UnixNano())
		entry.inFlight.Add(-1)
	}, nil
}

func (m *ControlManager) withDiskReadOnly(diskID uuid.UUID, fn func(*engine.DiskStore) error) error {
	slot := m.getSlot(diskID)
	slot.opMu.RLock()
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		slot.opMu.RUnlock()
		return engine.ErrClosed
	}
	if entry, ok := m.disks[diskID]; ok {
		entry.inFlight.Add(1)
		entry.lastUsedNano.Store(m.now().UnixNano())
		m.poolMu.RUnlock()
		err := fn(entry.store)
		entry.lastUsedNano.Store(m.now().UnixNano())
		entry.inFlight.Add(-1)
		slot.opMu.RUnlock()
		return err
	}
	m.poolMu.RUnlock()
	slot.opMu.RUnlock()

	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return engine.ErrClosed
	}
	if entry, ok := m.disks[diskID]; ok {
		entry.inFlight.Add(1)
		entry.lastUsedNano.Store(m.now().UnixNano())
		m.poolMu.RUnlock()
		err := fn(entry.store)
		entry.lastUsedNano.Store(m.now().UnixNano())
		entry.inFlight.Add(-1)
		return err
	}
	m.poolMu.RUnlock()

	if !diskExistsOnFS(m.diskDir(diskID)) {
		return ErrNotFound
	}
	ds, err := m.openDiskRaw(diskID, "", 0)
	if err != nil {
		return err
	}
	defer func() { _ = ds.Close() }()
	return fn(ds)
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

	unlock := m.lockDisk(diskID)
	defer unlock()

	if m.isClosed() {
		return DiskRecord{}, engine.ErrClosed
	}

	ds, err := m.openDiskRaw(diskID, name, maxBytes)
	if err != nil {
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, err
	}

	meta := ds.DiskMeta()
	mainBranch, brErr := ds.GetBranch(meta.DefaultBranch)
	if brErr != nil {
		_ = ds.Close()
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, brErr
	}
	initCommit, cmtErr := ds.GetCommit(mainBranch.HeadCommitID)
	if cmtErr != nil {
		_ = ds.Close()
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, cmtErr
	}
	st, stErr := ds.StatFS(ctx)
	if stErr != nil {
		_ = ds.Close()
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, stErr
	}

	entry := &diskEntry{store: ds}
	entry.lastUsedNano.Store(m.now().UnixNano())
	entry.lastSampledUsed.Store(st.UsedBytes)
	entry.hasSampled.Store(true)

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		_ = ds.Close()
		_ = os.RemoveAll(m.diskDir(diskID))
		return DiskRecord{}, engine.ErrClosed
	}
	m.disks[diskID] = entry
	m.poolMu.Unlock()

	diskRec := diskMetaToRecord(diskID, meta, st.UsedBytes)
	branchRec := branchMetaToRecord(diskID, mainBranch)
	commitRec := commitMetaToRecord(diskID, initCommit)
	m.queue.EnqueueUpsertDisk(diskRec, &branchRec, &commitRec)
	return diskRec, nil
}

func (m *ControlManager) UpdateDisk(ctx context.Context, diskID uuid.UUID, name *string, maxBytes *uint64) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return DiskRecord{}, err
	}
	defer release()

	meta, err := entry.store.UpdateDiskMeta(ctx, name, maxBytes)
	if err != nil {
		return DiskRecord{}, err
	}
	var usedBytes uint64
	if st, stErr := entry.store.StatFS(ctx); stErr == nil {
		usedBytes = st.UsedBytes
	}
	diskRec := diskMetaToRecord(diskID, meta, usedBytes)
	m.queue.EnqueueUpdateDisk(diskRec)
	return diskRec, nil
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

	unlock := m.lockDisk(diskID)
	defer unlock()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return engine.ErrClosed
	}
	entry, isOpen := m.disks[diskID]
	m.poolMu.RUnlock()

	now := m.now()
	if isOpen {
		mounted, err := entry.store.HasActiveMounts("", now)
		if err != nil {
			return err
		}
		if mounted {
			return engine.ErrDiskMounted
		}
		m.poolMu.Lock()
		delete(m.disks, diskID)
		m.poolMu.Unlock()
		if closeErr := entry.store.Close(); closeErr != nil {
			return closeErr
		}
	} else {
		if !diskExistsOnFS(m.diskDir(diskID)) {
			return engine.ErrNotFound
		}
		ds, err := m.openDiskRaw(diskID, "", 0)
		if err != nil {
			return err
		}
		mounted, mErr := ds.HasActiveMounts("", now)
		closeErr := ds.Close()
		if mErr != nil {
			return mErr
		}
		if closeErr != nil {
			return closeErr
		}
		if mounted {
			return engine.ErrDiskMounted
		}
	}

	if err := os.RemoveAll(m.diskDir(diskID)); err != nil {
		return err
	}
	m.queue.EnqueueDeleteDisk(diskID)
	return nil
}

func (m *ControlManager) ForkBranch(ctx context.Context, diskID uuid.UUID, srcBranch, newBranch, message string) (CommitRecord, BranchRecord, error) {
	if diskID == uuid.Nil {
		return CommitRecord{}, BranchRecord{}, engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return CommitRecord{}, BranchRecord{}, err
	}
	defer release()

	cmt, newBr, err := entry.store.Fork(ctx, srcBranch, newBranch, message)
	if err != nil {
		return CommitRecord{}, BranchRecord{}, err
	}
	cmtRec := commitMetaToRecord(diskID, cmt)
	newBrRec := branchMetaToRecord(diskID, newBr)

	srcBr, srcErr := entry.store.GetBranch(srcBranch)
	if srcErr != nil {
		m.queue.MarkDirty(diskID)
		return cmtRec, newBrRec, nil
	}
	m.queue.EnqueueUpsertBranchAndCommit(
		diskID,
		[]BranchRecord{branchMetaToRecord(diskID, srcBr), newBrRec},
		[]CommitRecord{cmtRec},
		nil,
	)
	return cmtRec, newBrRec, nil
}

func (m *ControlManager) DeleteBranch(ctx context.Context, diskID uuid.UUID, branch string) error {
	if diskID == uuid.Nil {
		return engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return err
	}
	defer release()

	if delErr := entry.store.DeleteBranch(ctx, branch); delErr != nil {
		return delErr
	}
	m.queue.EnqueueDeleteBranch(diskID, branch)
	return nil
}

func (m *ControlManager) Commit(ctx context.Context, diskID uuid.UUID, branch, message string) (CommitRecord, error) {
	if diskID == uuid.Nil {
		return CommitRecord{}, engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return CommitRecord{}, err
	}
	defer release()

	cmt, err := entry.store.Commit(ctx, branch, message)
	if err != nil {
		return CommitRecord{}, err
	}
	cmtRec := commitMetaToRecord(diskID, cmt)
	br, brErr := entry.store.GetBranch(branch)
	if brErr != nil {
		m.queue.MarkDirty(diskID)
		return cmtRec, nil
	}
	m.queue.EnqueueUpsertBranchAndCommit(
		diskID,
		[]BranchRecord{branchMetaToRecord(diskID, br)},
		[]CommitRecord{cmtRec},
		nil,
	)
	return cmtRec, nil
}

func (m *ControlManager) ResetBranch(ctx context.Context, diskID uuid.UUID, branch string, targetCommitID uint64) (BranchRecord, error) {
	if diskID == uuid.Nil {
		return BranchRecord{}, engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return BranchRecord{}, err
	}
	defer release()

	br, err := entry.store.Reset(ctx, branch, targetCommitID)
	if err != nil {
		return BranchRecord{}, err
	}
	brRec := branchMetaToRecord(diskID, br)
	m.queue.EnqueueUpsertBranchAndCommit(diskID, []BranchRecord{brRec}, nil, nil)
	return brRec, nil
}

func (m *ControlManager) MergeBranch(ctx context.Context, diskID uuid.UUID, opts engine.MergeOptions) (*engine.MergeResult, error) {
	if diskID == uuid.Nil {
		return nil, engine.ErrInvalid
	}
	if opts.DryRun {
		unlock := m.rlockDisk(diskID)
		defer unlock()
		entry, release, err := m.acquireDisk(diskID)
		if err != nil {
			return nil, err
		}
		defer release()
		return entry.store.Merge(ctx, opts)
	}

	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return nil, err
	}
	defer release()

	res, err := entry.store.Merge(ctx, opts)
	if err != nil {
		return nil, err
	}
	if !res.UpToDate {
		tgtBr, brErr := entry.store.GetBranch(opts.TargetBranch)
		if brErr != nil {
			m.queue.MarkDirty(diskID)
			return res, nil
		}
		branches := []BranchRecord{branchMetaToRecord(diskID, tgtBr)}
		var commits []CommitRecord
		if !res.FastForward && res.CommitID != 0 {
			cmt, cmtErr := entry.store.GetCommit(res.CommitID)
			if cmtErr != nil {
				m.queue.MarkDirty(diskID)
				return res, nil
			}
			commits = []CommitRecord{commitMetaToRecord(diskID, cmt)}
		}
		m.queue.EnqueueUpsertBranchAndCommit(diskID, branches, commits, nil)
	}
	return res, nil
}

func (m *ControlManager) DeleteCommit(ctx context.Context, diskID uuid.UUID, commitID uint64) error {
	if diskID == uuid.Nil {
		return engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return err
	}
	defer release()

	if delErr := entry.store.DeleteCommit(ctx, commitID); delErr != nil {
		return delErr
	}
	m.queue.EnqueueDeleteCommits(diskID, []uint64{commitID})
	return nil
}

func (m *ControlManager) PruneCommits(ctx context.Context, diskID uuid.UUID) (int, error) {
	if diskID == uuid.Nil {
		return 0, engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return 0, err
	}
	defer release()

	pruned, err := entry.store.PruneCommits(ctx)
	if err != nil {
		return 0, err
	}
	if pruned > 0 {
		rawSnap, snapErr := entry.store.SnapshotControlMeta(m.now())
		if snapErr != nil {
			m.queue.MarkDirty(diskID)
			return pruned, nil
		}
		m.queue.EnqueueReplaceDiskSnapshot(controlSnapshotToDiskSnapshot(diskID, rawSnap))
	}
	return pruned, nil
}

func (m *ControlManager) MountBranch(ctx context.Context, diskID uuid.UUID, branch string, readOnly, force bool) (MountRecord, error) {
	if diskID == uuid.Nil {
		return MountRecord{}, engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return MountRecord{}, err
	}
	defer release()

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

	mm, err := entry.store.MountBranch(ctx, branch, readOnly, force, m.mountLeaseTTL)
	if err != nil {
		return MountRecord{}, err
	}
	mountRec, convErr := mountMetaToRecord(diskID, mm)
	if convErr != nil {
		m.queue.MarkDirty(diskID)
		return MountRecord{}, convErr
	}

	if !readOnly {
		br, brErr := entry.store.GetBranch(branch)
		if brErr != nil {
			m.queue.MarkDirty(diskID)
			return mountRec, nil
		}
		brRec := branchMetaToRecord(diskID, br)
		m.queue.EnqueueUpsertMount(mountRec, &brRec, deletedRWIDs)
	} else {
		m.queue.EnqueueUpsertMount(mountRec, nil, nil)
	}
	return mountRec, nil
}

func (m *ControlManager) RenewMount(ctx context.Context, diskID uuid.UUID, branch string, mountID uuid.UUID, readOnly bool, epoch uint64) (MountRecord, error) {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return MountRecord{}, engine.ErrInvalid
	}
	unlock := m.rlockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return MountRecord{}, err
	}
	defer release()

	mm, err := entry.store.RenewMount(ctx, branch, mountID.String(), readOnly, epoch, m.mountLeaseTTL)
	if err != nil {
		return MountRecord{}, err
	}
	mountRec, convErr := mountMetaToRecord(diskID, mm)
	if convErr != nil {
		return MountRecord{}, convErr
	}
	m.queue.RecordMountRenew(diskID, mountRec.MountID, mountRec.ExpiresAt)
	return mountRec, nil
}

func (m *ControlManager) UnmountBranch(ctx context.Context, diskID, mountID uuid.UUID) error {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return err
	}
	defer release()

	if umErr := entry.store.Unmount(ctx, mountID.String()); umErr != nil {
		return umErr
	}
	m.queue.EnqueueDeleteMounts(diskID, []uuid.UUID{mountID}, nil)
	return nil
}

func (m *ControlManager) EvictMounts(ctx context.Context, diskID uuid.UUID, branch string, mountID uuid.UUID) (engine.EvictMountsResult, error) {
	if diskID == uuid.Nil {
		return engine.EvictMountsResult{}, engine.ErrInvalid
	}
	unlock := m.lockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return engine.EvictMountsResult{}, err
	}
	defer release()

	mountIDStr := ""
	if mountID != uuid.Nil {
		mountIDStr = mountID.String()
	}
	res, err := entry.store.EvictMounts(ctx, branch, mountIDStr)
	if err != nil {
		return engine.EvictMountsResult{}, err
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
		m.queue.EnqueueDeleteMounts(diskID, evictedUUIDs, updatedBranches)
	}
	return res, nil
}

func (m *ControlManager) Diff(ctx context.Context, diskID uuid.UUID, opts engine.DiffOptions) (*engine.DiffResult, error) {
	if diskID == uuid.Nil {
		return nil, engine.ErrInvalid
	}
	unlock := m.rlockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return nil, err
	}
	defer release()
	return entry.store.Diff(ctx, opts)
}

func (m *ControlManager) DiffCommits(ctx context.Context, diskID uuid.UUID, opts engine.DiffCommitsOptions) (*engine.DiffResult, error) {
	if diskID == uuid.Nil {
		return nil, engine.ErrInvalid
	}
	unlock := m.rlockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return nil, err
	}
	defer release()
	return entry.store.DiffCommits(ctx, opts)
}

func (m *ControlManager) DiffWork(ctx context.Context, diskID uuid.UUID, branch string, opts engine.DiffWorkOptions) (*engine.DiffResult, error) {
	if diskID == uuid.Nil {
		return nil, engine.ErrInvalid
	}
	unlock := m.rlockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return nil, err
	}
	defer release()
	return entry.store.DiffWork(ctx, branch, opts)
}

func (m *ControlManager) ReadCommitNode(ctx context.Context, diskID uuid.UUID, commitID, ino uint64, offset int64, size int) (engine.CommitNodeResult, error) {
	if diskID == uuid.Nil {
		return engine.CommitNodeResult{}, engine.ErrInvalid
	}
	unlock := m.rlockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return engine.CommitNodeResult{}, err
	}
	defer release()
	return entry.store.ReadCommitNode(ctx, commitID, ino, offset, size)
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
	unlock := m.rlockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return err
	}
	defer release()
	return fn(entry.store)
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
	unlock := m.rlockDisk(diskID)
	defer unlock()

	entry, release, err := m.acquireDisk(diskID)
	if err != nil {
		return nil, err
	}
	defer release()
	return entry.store, nil
}

func (m *ControlManager) ListDisks(ctx context.Context, q ListDisksQuery) (ListDisksPage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.store == nil {
		return ListDisksPage{}, ErrUnavailable
	}
	if err := m.queue.WaitCatalog(ctx, m.barrierTimeout); err != nil {
		if errors.Is(err, ErrBarrierTimeout) {
			return ListDisksPage{}, ErrUnavailable
		}
		return ListDisksPage{}, err
	}
	page, err := m.store.ListDisks(ctx, q)
	if err != nil {
		if ctx.Err() == nil && IsUnavailable(err) && !errors.Is(err, ErrUnavailable) {
			return ListDisksPage{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		return ListDisksPage{}, err
	}
	return page, nil
}

func (m *ControlManager) shouldFallbackOnWait(ctx context.Context, diskID uuid.UUID) (fallback bool, err error) {
	if m.store == nil || m.queue.IsDirty(diskID) {
		return true, nil
	}
	waitErr := m.queue.WaitDisk(ctx, diskID, m.barrierTimeout)
	if waitErr == nil {
		return false, nil
	}
	if errors.Is(waitErr, ErrDiskDirty) || errors.Is(waitErr, ErrBarrierTimeout) {
		return true, nil
	}
	return false, waitErr
}

func (m *ControlManager) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	fallback, waitErr := m.shouldFallbackOnWait(ctx, diskID)
	if waitErr != nil {
		return DiskRecord{}, waitErr
	}
	if fallback {
		return m.fallbackGetDisk(ctx, diskID)
	}
	rec, err := m.store.GetDisk(ctx, diskID)
	if err != nil {
		if ctx.Err() == nil && IsUnavailable(err) {
			return m.fallbackGetDisk(ctx, diskID)
		}
		return DiskRecord{}, err
	}
	return rec, nil
}

func (m *ControlManager) fallbackGetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	var rec DiskRecord
	err := m.withDiskReadOnly(diskID, func(ds *engine.DiskStore) error {
		st, stErr := ds.StatFS(ctx)
		if stErr != nil {
			return stErr
		}
		rec = diskMetaToRecord(diskID, ds.DiskMeta(), st.UsedBytes)
		return nil
	})
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return DiskRecord{}, ErrNotFound
		}
		return DiskRecord{}, err
	}
	return rec, nil
}

func (m *ControlManager) ListBranches(ctx context.Context, diskID uuid.UUID) ([]BranchRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	fallback, waitErr := m.shouldFallbackOnWait(ctx, diskID)
	if waitErr != nil {
		return nil, waitErr
	}
	if fallback {
		return m.fallbackListBranches(diskID)
	}
	branches, err := m.store.ListBranches(ctx, diskID)
	if err != nil {
		if ctx.Err() == nil && IsUnavailable(err) {
			return m.fallbackListBranches(diskID)
		}
		return nil, err
	}
	return branches, nil
}

func (m *ControlManager) fallbackListBranches(diskID uuid.UUID) ([]BranchRecord, error) {
	var out []BranchRecord
	err := m.withDiskReadOnly(diskID, func(ds *engine.DiskStore) error {
		raw, lErr := ds.ListBranches()
		if lErr != nil {
			return lErr
		}
		out = make([]BranchRecord, 0, len(raw))
		for _, b := range raw {
			out = append(out, branchMetaToRecord(diskID, b))
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return out, nil
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

	fallback, waitErr := m.shouldFallbackOnWait(ctx, q.DiskID)
	if waitErr != nil {
		return ListCommitsPage{}, waitErr
	}
	if fallback {
		return m.fallbackListCommits(q, limit, hasCursor, cursorID)
	}
	page, err := m.store.ListCommits(ctx, q)
	if err != nil {
		if ctx.Err() == nil && IsUnavailable(err) {
			return m.fallbackListCommits(q, limit, hasCursor, cursorID)
		}
		return ListCommitsPage{}, err
	}
	return page, nil
}

func (m *ControlManager) fallbackListCommits(q ListCommitsQuery, limit uint32, hasCursor bool, cursorID uint64) (ListCommitsPage, error) {
	var page ListCommitsPage
	err := m.withDiskReadOnly(q.DiskID, func(ds *engine.DiskStore) error {
		raw, lErr := ds.ListCommits()
		if lErr != nil {
			return lErr
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
		if len(filtered) > int(limit) {
			filtered = filtered[:limit]
			page.NextPageToken = EncodeCommitPageCursor(filtered[len(filtered)-1].CommitID)
		}
		page.Commits = filtered
		return nil
	})
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return ListCommitsPage{}, ErrNotFound
		}
		return ListCommitsPage{}, err
	}
	return page, nil
}

func (m *ControlManager) ListMounts(ctx context.Context, diskID uuid.UUID, branchName string) ([]MountRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	now := m.now()
	fallback, waitErr := m.shouldFallbackOnWait(ctx, diskID)
	if waitErr != nil {
		return nil, waitErr
	}
	if fallback {
		return m.fallbackListMounts(diskID, branchName, now)
	}
	mounts, err := m.store.ListMounts(ctx, diskID, branchName, now)
	if err != nil {
		if ctx.Err() == nil && IsUnavailable(err) {
			return m.fallbackListMounts(diskID, branchName, now)
		}
		return nil, err
	}
	return mounts, nil
}

func (m *ControlManager) fallbackListMounts(diskID uuid.UUID, branchName string, now time.Time) ([]MountRecord, error) {
	cutoff := normalizeTimestamp(now)
	var out []MountRecord
	err := m.withDiskReadOnly(diskID, func(ds *engine.DiskStore) error {
		raw, lErr := ds.ListMounts(branchName, now)
		if lErr != nil {
			return lErr
		}
		out = make([]MountRecord, 0, len(raw))
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
		return nil
	})
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return out, nil
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
