package engine

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/google/uuid"
)

const (
	defaultMemTableSize     = 16 << 20
	DefaultSharedCacheBytes = 64 << 20
	defaultMaxOpenFiles     = 128
	defaultWALBytesPerSync  = 4 << 20
	defaultBloomBitsPerKey  = 10
	pebbleLevelCount        = 7
	DefaultMountLeaseTTL    = 30 * time.Second
	DefaultDiskIdleTTL      = 5 * time.Minute
	DefaultGCInterval       = 30 * time.Second
	nodeSlotStripes         = 1024
	nodeSlotCacheLine       = 64
	defaultMaintOpTimeout   = 30 * time.Second
)

type GCMode int

const (
	GCModePeriodic GCMode = iota
	GCModeWakeOnly
	GCModeDisabled
)

type NodeOptions struct {
	DataDir        string
	BlockSize      uint32
	MaxPackSize    uint32
	CacheSizeBytes int64
	Now            func() time.Time
	MountLeaseTTL  time.Duration
	DiskIdleTTL    time.Duration
	GCMode         GCMode
	GCInterval     time.Duration
	Logger         *slog.Logger
}

type DiskInfo struct {
	DiskMeta
	UsedBytes uint64
}

type ListDisksQuery struct {
	PageSize  uint32
	PageToken string
}

type ListDisksPage struct {
	Disks         []DiskInfo
	NextPageToken string
}

type ListCommitsQuery struct {
	DiskID    uuid.UUID
	Kind      CommitKind
	PageSize  uint32
	PageToken string
}

type ListCommitsPage struct {
	Commits       []CommitMeta
	NextPageToken string
}

type MergeBranchResult struct {
	BaseCommitID   uint64
	TargetCommitID uint64
	SourceCommitID uint64
	CommitID       uint64
	FastForward    bool
	UpToDate       bool
	WriterEpoch    uint64
	Summary        DiffSummary
	Branch         *BranchMeta
	Commit         *CommitMeta
}

type NodeMetricsSnapshot struct {
	DisksLoaded       int
	LogicalBytes      uint64
	PackPhysicalBytes uint64
	Inodes            uint64
	Pebble            *pebble.Metrics
	BlockCache        pebble.CacheMetrics
}

type nodeStripeDisk struct {
	id    uuid.UUID
	entry *nodeDiskEntry
}

type nodeDiskSlotCore struct {
	opMu   sync.RWMutex
	openMu sync.Mutex
	disks  atomic.Pointer[[]nodeStripeDisk]
}

type nodeDiskSlot struct {
	nodeDiskSlotCore
	_ [(nodeSlotCacheLine - unsafe.Sizeof(nodeDiskSlotCore{})%nodeSlotCacheLine) % nodeSlotCacheLine]byte
}

type nodeDiskEntry struct {
	store        *DiskStore
	lastUsedNano atomic.Int64
}

type dirtyPackEntry struct {
	store *DiskStore
	gen   uint64
}

type walSyncWave struct {
	done      chan struct{}
	syncedGen uint64
	err       error
}

type NodeStore struct {
	dataDir           string
	disksDir          string
	db                *pebble.DB
	cache             *pebble.Cache
	logger            *slog.Logger
	now               func() time.Time
	blockSize         uint32
	maxPackSize       uint32
	mountLeaseTTL     time.Duration
	diskIdleTTL       time.Duration
	touchIntervalNano int64
	gcMode            GCMode
	gcInterval        time.Duration

	slots [nodeSlotStripes]nodeDiskSlot

	syncGate       sync.RWMutex
	walWriteGen    atomic.Uint64
	walSyncedGen   atomic.Uint64
	walSyncMu      sync.Mutex
	activeSyncWave *walSyncWave
	syncWaveWG     sync.WaitGroup
	dirtyPacksMu   sync.Mutex
	dirtyPacks     map[uuid.UUID]*dirtyPackEntry
	walCommitHook  func(sync bool)

	idleEvictions atomic.Uint64

	bgCtx     context.Context
	bgCancel  context.CancelFunc
	bgWG      sync.WaitGroup
	closeMu   sync.RWMutex
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func newNodePebbleOptions(cache *pebble.Cache) *pebble.Options {
	cmp := *pebble.DefaultComparer
	cmp.Name = "bowl.engine.node.comparer"
	cmp.Split = NodeSplitKey

	var levels [pebbleLevelCount]pebble.LevelOptions
	policy := bloom.FilterPolicy(defaultBloomBitsPerKey)
	for i := range levels {
		levels[i].FilterPolicy = policy
	}

	return &pebble.Options{
		Comparer:           &cmp,
		Cache:              cache,
		MemTableSize:       defaultMemTableSize,
		MaxOpenFiles:       defaultMaxOpenFiles,
		WALBytesPerSync:    defaultWALBytesPerSync,
		FormatMajorVersion: pebble.FormatNewest,
		Levels:             levels,
	}
}

func OpenNode(opts NodeOptions) (*NodeStore, error) {
	if opts.DataDir == "" {
		return nil, ErrInvalid
	}
	if opts.CacheSizeBytes < 0 {
		return nil, ErrInvalid
	}
	if opts.MountLeaseTTL < 0 {
		return nil, ErrInvalid
	}
	if opts.DiskIdleTTL < 0 {
		return nil, ErrInvalid
	}
	if opts.GCMode < GCModePeriodic || opts.GCMode > GCModeDisabled {
		return nil, ErrInvalid
	}
	if opts.GCInterval < 0 {
		return nil, ErrInvalid
	}
	if opts.GCInterval > 0 && opts.GCMode != GCModePeriodic {
		return nil, ErrInvalid
	}

	blockSize := opts.BlockSize
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	} else if blockSize < MinBlockSize || blockSize > MaxBlockSize || blockSize&(blockSize-1) != 0 {
		return nil, ErrInvalid
	}

	maxPackSize := opts.MaxPackSize
	if maxPackSize == 0 {
		maxPackSize = DefaultMaxPackSize
	}
	if maxPackSize > DefaultMaxPackSize || maxPackSize < blockSize {
		return nil, ErrInvalid
	}

	cacheSize := opts.CacheSizeBytes
	if cacheSize == 0 {
		cacheSize = DefaultSharedCacheBytes
	}

	leaseTTL := opts.MountLeaseTTL
	if leaseTTL == 0 {
		leaseTTL = DefaultMountLeaseTTL
	}

	gcInterval := opts.GCInterval
	if opts.GCMode == GCModePeriodic && gcInterval == 0 {
		gcInterval = DefaultGCInterval
	}

	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	var touchIntervalNano int64
	if opts.DiskIdleTTL > 0 {
		touchIntervalNano = max(opts.DiskIdleTTL.Nanoseconds()/8, int64(1))
	}

	pebbleDir := filepath.Join(opts.DataDir, "pebble")
	disksDir := filepath.Join(opts.DataDir, "disks")
	if err := os.MkdirAll(pebbleDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(disksDir, 0o755); err != nil {
		return nil, err
	}

	cache := pebble.NewCache(cacheSize)
	db, err := pebble.Open(pebbleDir, newNodePebbleOptions(cache))
	if err != nil {
		cache.Unref()
		return nil, err
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	n := &NodeStore{
		dataDir:           opts.DataDir,
		disksDir:          disksDir,
		db:                db,
		cache:             cache,
		logger:            logger,
		now:               nowFn,
		blockSize:         blockSize,
		maxPackSize:       maxPackSize,
		mountLeaseTTL:     leaseTTL,
		diskIdleTTL:       opts.DiskIdleTTL,
		touchIntervalNano: touchIntervalNano,
		gcMode:            opts.GCMode,
		gcInterval:        gcInterval,
		dirtyPacks:        make(map[uuid.UUID]*dirtyPackEntry),
		bgCtx:             bgCtx,
		bgCancel:          bgCancel,
	}

	if n.diskIdleTTL > 0 {
		n.bgWG.Go(n.runMaintenanceLoop)
	}

	return n, nil
}

func (n *NodeStore) Ready(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if n.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (n *NodeStore) MountLeaseTTL() time.Duration {
	return n.mountLeaseTTL
}

func (n *NodeStore) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	n.closeOnce.Do(func() {
		n.bgCancel()
		n.bgWG.Wait()

		n.closeMu.Lock()
		defer n.closeMu.Unlock()

		n.walSyncMu.Lock()
		n.closed.Store(true)
		n.walSyncMu.Unlock()
		n.syncWaveWG.Wait()

		var detached []nodeStripeDisk
		for i := range n.slots {
			n.slots[i].opMu.Lock()
			if entries := n.slots[i].disks.Swap(nil); entries != nil {
				detached = append(detached, *entries...)
			}
			n.slots[i].opMu.Unlock()
		}

		var (
			errs      []error
			packErr   error
			diskHooks []func(sync bool)
		)
		for _, item := range detached {
			if item.entry == nil || item.entry.store == nil {
				continue
			}
			if item.entry.store.walCommitHook != nil {
				diskHooks = append(diskHooks, item.entry.store.walCommitHook)
			}
			if err := item.entry.store.closeResources(nil); err != nil {
				packErr = errors.Join(packErr, err)
				errs = append(errs, err)
			}
		}

		if err := n.flushDirtyPacks(ctx); err != nil {
			packErr = errors.Join(packErr, err)
			errs = append(errs, err)
		}

		if n.db != nil {
			if packErr == nil {
				n.syncGate.Lock()
				for _, hook := range diskHooks {
					hook(true)
				}
				if n.walCommitHook != nil {
					n.walCommitHook(true)
				}
				if logErr := n.db.LogData(nil, pebble.Sync); logErr != nil {
					errs = append(errs, logErr)
				}
				n.syncGate.Unlock()
			}
			if closeErr := n.db.Close(); closeErr != nil {
				errs = append(errs, closeErr)
			}
		}
		if n.cache != nil {
			n.cache.Unref()
		}
		n.closeErr = errors.Join(errs...)
	})
	return n.closeErr
}

func (n *NodeStore) diskDir(diskID uuid.UUID) string {
	return filepath.Join(n.disksDir, diskID.String())
}

func (n *NodeStore) packsDir(diskID uuid.UUID) string {
	return filepath.Join(n.diskDir(diskID), "packs")
}

func nodeSlotIndex(diskID uuid.UUID) uint64 {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	for _, b := range diskID {
		h ^= uint64(b)
		h *= prime64
	}
	return h & (nodeSlotStripes - 1)
}

func (n *NodeStore) getSlot(diskID uuid.UUID) *nodeDiskSlot {
	return &n.slots[nodeSlotIndex(diskID)]
}

func lookupLoadedEntry(slot *nodeDiskSlot, diskID uuid.UUID) *nodeDiskEntry {
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

func (s *nodeDiskSlot) upsert(diskID uuid.UUID, entry *nodeDiskEntry) {
	cur := s.disks.Load()
	var next []nodeStripeDisk
	if cur != nil {
		next = make([]nodeStripeDisk, len(*cur), len(*cur)+1)
		copy(next, *cur)
		for i := range next {
			if next[i].id == diskID {
				next[i].entry = entry
				s.disks.Store(&next)
				return
			}
		}
	} else {
		next = make([]nodeStripeDisk, 0, 1)
	}
	next = append(next, nodeStripeDisk{id: diskID, entry: entry})
	s.disks.Store(&next)
}

func (s *nodeDiskSlot) remove(diskID uuid.UUID) (*nodeDiskEntry, bool) {
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
	next := make([]nodeStripeDisk, 0, len(old)-1)
	next = append(next, old[:idx]...)
	next = append(next, old[idx+1:]...)
	s.disks.Store(&next)
	return removed, true
}

func (n *NodeStore) newDiskEntry(ds *DiskStore) *nodeDiskEntry {
	entry := &nodeDiskEntry{store: ds}
	if n.diskIdleTTL > 0 {
		entry.lastUsedNano.Store(n.now().UnixNano())
	}
	return entry
}

func (n *NodeStore) touchLastUsed(entry *nodeDiskEntry) {
	if n.diskIdleTTL <= 0 {
		return
	}
	nowNano := n.now().UnixNano()
	if nowNano-entry.lastUsedNano.Load() >= n.touchIntervalNano {
		entry.lastUsedNano.Store(nowNano)
	}
}

func (n *NodeStore) markPackDirty(diskID uuid.UUID, ds *DiskStore) {
	n.dirtyPacksMu.Lock()
	if cur, ok := n.dirtyPacks[diskID]; ok {
		cur.store = ds
		cur.gen++
	} else {
		n.dirtyPacks[diskID] = &dirtyPackEntry{
			store: ds,
			gen:   1,
		}
	}
	n.dirtyPacksMu.Unlock()
}

func (n *NodeStore) clearPackDirty(diskID uuid.UUID) {
	n.dirtyPacksMu.Lock()
	delete(n.dirtyPacks, diskID)
	n.dirtyPacksMu.Unlock()
}

func (n *NodeStore) hasDirtyPack(diskID uuid.UUID) bool {
	n.dirtyPacksMu.Lock()
	_, ok := n.dirtyPacks[diskID]
	n.dirtyPacksMu.Unlock()
	return ok
}

func (n *NodeStore) hasAnyDirtyPack() bool {
	n.dirtyPacksMu.Lock()
	has := len(n.dirtyPacks) > 0
	n.dirtyPacksMu.Unlock()
	return has
}

func (n *NodeStore) advanceWalSyncedGen(target uint64) {
	for {
		cur := n.walSyncedGen.Load()
		if cur >= target {
			return
		}
		if n.walSyncedGen.CompareAndSwap(cur, target) {
			return
		}
	}
}

func (n *NodeStore) flushDirtyPacks(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	type snapItem struct {
		diskID uuid.UUID
		store  *DiskStore
		gen    uint64
	}
	n.dirtyPacksMu.Lock()
	if len(n.dirtyPacks) == 0 {
		n.dirtyPacksMu.Unlock()
		return nil
	}
	items := make([]snapItem, 0, len(n.dirtyPacks))
	for id, entry := range n.dirtyPacks {
		items = append(items, snapItem{
			diskID: id,
			store:  entry.store,
			gen:    entry.gen,
		})
	}
	n.dirtyPacksMu.Unlock()

	for _, item := range items {
		if item.store == nil || item.store.packs == nil {
			n.dirtyPacksMu.Lock()
			if cur, ok := n.dirtyPacks[item.diskID]; ok && cur.gen == item.gen {
				delete(n.dirtyPacks, item.diskID)
			}
			n.dirtyPacksMu.Unlock()
			continue
		}
		if err := item.store.packs.SyncActive(ctx); err != nil {
			return err
		}
		n.dirtyPacksMu.Lock()
		if cur, ok := n.dirtyPacks[item.diskID]; ok && cur.gen == item.gen {
			delete(n.dirtyPacks, item.diskID)
		}
		n.dirtyPacksMu.Unlock()
	}
	return nil
}

func (n *NodeStore) commitBatchNoSync(batch *pebble.Batch, diskHook func(sync bool)) error {
	n.syncGate.RLock()
	defer n.syncGate.RUnlock()
	if diskHook != nil {
		diskHook(false)
	}
	if n.walCommitHook != nil {
		n.walCommitHook(false)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	n.walWriteGen.Add(1)
	return nil
}

func (n *NodeStore) runWithSyncBarrier(ctx context.Context, diskHook func(sync bool), op func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := n.flushDirtyPacks(ctx); err != nil {
		return err
	}

	n.syncGate.Lock()
	defer n.syncGate.Unlock()

	if err := n.flushDirtyPacks(ctx); err != nil {
		return err
	}
	if diskHook != nil {
		diskHook(true)
	}
	if n.walCommitHook != nil {
		n.walCommitHook(true)
	}
	if err := op(); err != nil {
		return err
	}
	gen := n.walWriteGen.Add(1)
	n.advanceWalSyncedGen(gen)
	return nil
}

func (n *NodeStore) SyncWAL(ctx context.Context, targetGen uint64, diskHook func(sync bool)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.closed.Load() {
		return ErrClosed
	}
	if n.walSyncedGen.Load() >= targetGen && !n.hasAnyDirtyPack() {
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n.walSyncMu.Lock()
		if n.closed.Load() {
			n.walSyncMu.Unlock()
			return ErrClosed
		}
		if n.walSyncedGen.Load() >= targetGen && !n.hasAnyDirtyPack() {
			n.walSyncMu.Unlock()
			return nil
		}
		w := n.activeSyncWave
		if w == nil {
			w = &walSyncWave{done: make(chan struct{})}
			n.activeSyncWave = w
			n.syncWaveWG.Add(1)
			go n.runSyncWave(w, diskHook)
		}
		n.walSyncMu.Unlock()

		select {
		case <-w.done:
			if w.err != nil {
				return w.err
			}
			if w.syncedGen >= targetGen {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (n *NodeStore) runSyncWave(w *walSyncWave, diskHook func(sync bool)) {
	defer n.syncWaveWG.Done()

	waveCtx := n.bgCtx
	if waveCtx == nil {
		waveCtx = context.Background()
	}

	var (
		waveErr       error
		syncTargetGen uint64
	)
	if err := n.flushDirtyPacks(waveCtx); err != nil {
		waveErr = err
	} else {
		n.syncGate.Lock()
		syncTargetGen = n.walWriteGen.Load()
		if err := n.flushDirtyPacks(waveCtx); err != nil {
			waveErr = err
		} else {
			if diskHook != nil {
				diskHook(true)
			}
			if n.walCommitHook != nil {
				n.walCommitHook(true)
			}
			if err := n.db.LogData(nil, pebble.Sync); err != nil {
				waveErr = err
			} else {
				n.advanceWalSyncedGen(syncTargetGen)
			}
		}
		n.syncGate.Unlock()
	}

	n.walSyncMu.Lock()
	w.err = waveErr
	w.syncedGen = syncTargetGen
	if n.activeSyncWave == w {
		n.activeSyncWave = nil
	}
	close(w.done)
	n.walSyncMu.Unlock()
}

func (n *NodeStore) commitBatchSync(ctx context.Context, batch *pebble.Batch, diskHook func(sync bool)) error {
	return n.runWithSyncBarrier(ctx, diskHook, func() error {
		return batch.Commit(pebble.Sync)
	})
}

func (n *NodeStore) setKeySync(ctx context.Context, key, val []byte, diskHook func(sync bool)) error {
	return n.runWithSyncBarrier(ctx, diskHook, func() error {
		return n.db.Set(key, val, pebble.Sync)
	})
}

func (n *NodeStore) deleteKeySync(ctx context.Context, key []byte, diskHook func(sync bool)) error {
	return n.runWithSyncBarrier(ctx, diskHook, func() error {
		return n.db.Delete(key, pebble.Sync)
	})
}

func (n *NodeStore) instantiateDiskStore(diskID uuid.UUID, meta DiskMeta, fresh bool) (*DiskStore, error) {
	if n.maxPackSize < meta.BlockSize {
		return nil, ErrInvalid
	}
	ds := &DiskStore{
		node:        n,
		diskID:      diskID,
		keys:        newDiskKeys(diskID),
		dir:         n.diskDir(diskID),
		db:          n.db,
		now:         n.now,
		blockSize:   meta.BlockSize,
		meta:        meta,
		branchLocks: make(map[string]*branchLockEntry),
		commitCache: make(map[uint64]commitLink),
	}

	if fresh {
		ds.usedBytes.Store(0)
		ds.nextIno.Store(RootIno + 1)
		ds.resIno.Store(meta.ReservedIno)
		ds.nextLayerID.Store(3)
		ds.resLayerID.Store(meta.ReservedLayer)
		ds.nextCmtID.Store(2)
		ds.resCmtID.Store(meta.ReservedCmt)
	} else {
		ds.initAllocatorsFromMeta(meta)
		if err := ds.recoverUsedBytes(); err != nil {
			return nil, err
		}
	}

	packs, err := newPackStore(n.packsDir(diskID), n.maxPackSize, func() {
		n.markPackDirty(diskID, ds)
	})
	if err != nil {
		return nil, err
	}
	ds.packs = packs

	if n.gcMode != GCModeDisabled {
		gcCtx, gcCancel := context.WithCancel(context.Background())
		ds.gcCancel = gcCancel
		ds.gcDone = make(chan struct{})
		ds.gcWake = make(chan struct{}, 1)
		go ds.runBackgroundGC(gcCtx, n.gcInterval)
	}
	return ds, nil
}

func (n *NodeStore) acquireLoadedEntry(slot *nodeDiskSlot, diskID uuid.UUID) (*nodeDiskEntry, error) {
	if n.closed.Load() {
		return nil, ErrClosed
	}
	if entry := lookupLoadedEntry(slot, diskID); entry != nil {
		return entry, nil
	}
	return n.loadEntrySlow(slot, diskID)
}

func (n *NodeStore) loadEntrySlow(slot *nodeDiskSlot, diskID uuid.UUID) (*nodeDiskEntry, error) {
	slot.openMu.Lock()
	defer slot.openMu.Unlock()

	if n.closed.Load() {
		return nil, ErrClosed
	}
	if entry := lookupLoadedEntry(slot, diskID); entry != nil {
		return entry, nil
	}

	dk := newDiskKeys(diskID)
	meta, err := readDiskMeta(n.db, dk)
	if err != nil {
		return nil, err
	}

	ds, err := n.instantiateDiskStore(diskID, meta, false)
	if err != nil {
		return nil, err
	}
	if n.closed.Load() {
		_ = ds.closeResources(nil)
		return nil, ErrClosed
	}

	entry := n.newDiskEntry(ds)
	slot.upsert(diskID, entry)
	return entry, nil
}

func (n *NodeStore) withEntry(ctx context.Context, diskID uuid.UUID, exclusive bool, fn func(*nodeDiskEntry) error) error {
	if diskID == uuid.Nil {
		return ErrInvalid
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	slot := n.getSlot(diskID)
	if exclusive {
		slot.opMu.Lock()
		defer slot.opMu.Unlock()
	} else {
		slot.opMu.RLock()
		defer slot.opMu.RUnlock()
	}
	entry, err := n.acquireLoadedEntry(slot, diskID)
	if err != nil {
		return err
	}
	err = fn(entry)
	n.touchLastUsed(entry)
	return err
}

func (n *NodeStore) WithDisk(ctx context.Context, diskID uuid.UUID, fn func(*DiskStore) error) error {
	return n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		return fn(entry.store)
	})
}

func (n *NodeStore) loadedDiskUsedBytes(diskID uuid.UUID) (uint64, bool) {
	if n.closed.Load() {
		return 0, false
	}
	slot := n.getSlot(diskID)
	slot.opMu.RLock()
	defer slot.opMu.RUnlock()
	if n.closed.Load() {
		return 0, false
	}
	entry := lookupLoadedEntry(slot, diskID)
	if entry == nil || entry.store == nil {
		return 0, false
	}
	return entry.store.UsedBytes(), true
}

func (n *NodeStore) CreateDisk(ctx context.Context, name string, maxBytes uint64) (DiskInfo, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return DiskInfo{}, err
		}
	}
	if n.closed.Load() {
		return DiskInfo{}, ErrClosed
	}
	if err := validateDiskName(name); err != nil {
		return DiskInfo{}, err
	}
	if maxBytes == 0 || maxBytes > math.MaxInt64 {
		return DiskInfo{}, ErrInvalid
	}

	diskID, err := uuid.NewV7()
	if err != nil {
		return DiskInfo{}, err
	}
	return n.createDiskWithID(ctx, diskID, name, maxBytes)
}

func (n *NodeStore) createDiskWithID(ctx context.Context, diskID uuid.UUID, name string, maxBytes uint64) (DiskInfo, error) {
	slot := n.getSlot(diskID)
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if n.closed.Load() {
		return DiskInfo{}, ErrClosed
	}

	packsDir := n.packsDir(diskID)
	if err := os.MkdirAll(packsDir, 0o755); err != nil {
		return DiskInfo{}, err
	}

	const (
		initCommitID uint64 = 1
		initLayerID  uint64 = 1
		mainWorkID   uint64 = 2
	)

	nowNano := n.now().UnixNano()
	meta := DiskMeta{
		DiskID:        diskID.String(),
		Name:          name,
		DefaultBranch: DefaultBranchName,
		MaxBytes:      maxBytes,
		BlockSize:     n.blockSize,
		ReservedIno:   RootIno + IDSegmentSize,
		ReservedLayer: mainWorkID + IDSegmentSize,
		ReservedCmt:   initCommitID + IDSegmentSize,
		CreatedAt:     nowNano,
		UpdatedAt:     nowNano,
	}

	rootInode := Inode{
		Ino:        RootIno,
		ParentIno:  RootIno,
		Mode:       ModeDir | 0o755,
		Nlink:      2,
		Size:       DefaultDirSize,
		Atime:      nowNano,
		Mtime:      nowNano,
		Ctime:      nowNano,
		Generation: 1,
	}
	encodedRoot, encErr := EncodeInode(rootInode)
	if encErr != nil {
		_ = os.RemoveAll(n.diskDir(diskID))
		return DiskInfo{}, encErr
	}

	initCommit := CommitMeta{
		ID:        initCommitID,
		ParentID:  0,
		LayerID:   initLayerID,
		Kind:      CommitKindInit,
		Message:   "init",
		CreatedAt: nowNano,
	}
	mainBranch := BranchMeta{
		Name:         DefaultBranchName,
		HeadCommitID: initCommitID,
		WorkLayerID:  mainWorkID,
		WriterEpoch:  1,
		CreatedAt:    nowNano,
		UpdatedAt:    nowNano,
	}

	dk := newDiskKeys(diskID)
	catKey := CatalogIdxKey(createdAtMicroFromNano(meta.CreatedAt), diskID)

	batch := n.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if setErr := batch.Set(catKey, nil, nil); setErr != nil {
		_ = os.RemoveAll(n.diskDir(diskID))
		return DiskInfo{}, setErr
	}
	if setErr := batch.Set(dk.MetaDiskKey(), EncodeDiskMeta(meta), nil); setErr != nil {
		_ = os.RemoveAll(n.diskDir(diskID))
		return DiskInfo{}, setErr
	}
	if setErr := batch.Set(dk.MetaCommitKey(initCommitID), EncodeCommitMeta(initCommit), nil); setErr != nil {
		_ = os.RemoveAll(n.diskDir(diskID))
		return DiskInfo{}, setErr
	}
	if setErr := batch.Set(dk.LayerInodeKey(initLayerID, RootIno), EncodeLiveEnvelope(encodedRoot), nil); setErr != nil {
		_ = os.RemoveAll(n.diskDir(diskID))
		return DiskInfo{}, setErr
	}
	if setErr := batch.Set(dk.MetaBranchKey(DefaultBranchName), EncodeBranchMeta(mainBranch), nil); setErr != nil {
		_ = os.RemoveAll(n.diskDir(diskID))
		return DiskInfo{}, setErr
	}
	if commitErr := n.commitBatchSync(ctx, batch, nil); commitErr != nil {
		_ = os.RemoveAll(n.diskDir(diskID))
		return DiskInfo{}, commitErr
	}

	ds, err := n.instantiateDiskStore(diskID, meta, true)
	if err != nil {
		rb := n.db.NewBatch()
		start, end := dk.RangeBounds()
		_ = rb.Delete(catKey, nil)
		_ = rb.DeleteRange(start, end, nil)
		_ = n.commitBatchSync(context.Background(), rb, nil)
		_ = rb.Close()
		_ = os.RemoveAll(n.diskDir(diskID))
		return DiskInfo{}, err
	}

	slot.upsert(diskID, n.newDiskEntry(ds))
	return DiskInfo{
		DiskMeta:  meta,
		UsedBytes: 0,
	}, nil
}

func (n *NodeStore) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskInfo, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return DiskInfo{}, err
		}
	}
	if diskID == uuid.Nil {
		return DiskInfo{}, ErrInvalid
	}
	n.closeMu.RLock()
	defer n.closeMu.RUnlock()
	if n.closed.Load() {
		return DiskInfo{}, ErrClosed
	}

	snap := n.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := newDiskKeys(diskID)
	meta, err := readDiskMeta(snap, dk)
	if err != nil {
		return DiskInfo{}, err
	}

	used, ok := n.loadedDiskUsedBytes(diskID)
	if !ok {
		used, err = computeColdUsedBytes(snap, dk)
		if err != nil {
			return DiskInfo{}, err
		}
	}
	return DiskInfo{
		DiskMeta:  meta,
		UsedBytes: used,
	}, nil
}

func (n *NodeStore) ListDisks(ctx context.Context, q ListDisksQuery) (ListDisksPage, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return ListDisksPage{}, err
		}
	}
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

	n.closeMu.RLock()
	defer n.closeMu.RUnlock()
	if n.closed.Load() {
		return ListDisksPage{}, ErrClosed
	}

	snap := n.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	prefix := CatalogIdxPrefix()
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return ListDisksPage{}, err
	}

	var valid bool
	if hasCursor {
		cursorKey := CatalogIdxKey(cursorTime.UnixMicro(), cursorID)
		valid = iter.SeekGE(cursorKey)
		if valid && bytes.Equal(iter.Key(), cursorKey) {
			valid = iter.Next()
		}
	} else {
		valid = iter.First()
	}

	disks := make([]DiskInfo, 0, min(int(limit), 64))
	var (
		hasMore       bool
		lastDiskID    uuid.UUID
		lastCreatedAt time.Time
	)
	for ; valid; valid = iter.Next() {
		if ctx != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				_ = iter.Close()
				return ListDisksPage{}, ctxErr
			}
		}
		us, diskID, decErr := DecodeCatalogIdxKey(iter.Key())
		if decErr != nil {
			_ = iter.Close()
			return ListDisksPage{}, decErr
		}
		if len(disks) == int(limit) {
			hasMore = true
			break
		}
		dk := newDiskKeys(diskID)
		meta, metaErr := readDiskMeta(snap, dk)
		if errors.Is(metaErr, ErrNotFound) {
			continue
		}
		if metaErr != nil {
			_ = iter.Close()
			return ListDisksPage{}, metaErr
		}
		used, ok := n.loadedDiskUsedBytes(diskID)
		if !ok {
			used, metaErr = computeColdUsedBytes(snap, dk)
			if metaErr != nil {
				_ = iter.Close()
				return ListDisksPage{}, metaErr
			}
		}
		disks = append(disks, DiskInfo{
			DiskMeta:  meta,
			UsedBytes: used,
		})
		lastDiskID = diskID
		lastCreatedAt = time.UnixMicro(us).UTC()
	}
	if closeErr := iter.Close(); closeErr != nil {
		return ListDisksPage{}, closeErr
	}

	page := ListDisksPage{
		Disks: disks,
	}
	if hasMore {
		page.NextPageToken = EncodeDiskPageCursor(lastCreatedAt, lastDiskID)
	}
	return page, nil
}

func (n *NodeStore) UpdateDisk(ctx context.Context, diskID uuid.UUID, name *string, maxBytes *uint64) (DiskInfo, error) {
	if diskID == uuid.Nil {
		return DiskInfo{}, ErrInvalid
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return DiskInfo{}, err
		}
	}
	if name == nil && maxBytes == nil {
		return DiskInfo{}, ErrInvalid
	}
	if name != nil {
		if err := validateDiskName(*name); err != nil {
			return DiskInfo{}, err
		}
	}
	if maxBytes != nil {
		if *maxBytes == 0 || *maxBytes > math.MaxInt64 {
			return DiskInfo{}, ErrInvalid
		}
	}

	slot := n.getSlot(diskID)
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if n.closed.Load() {
		return DiskInfo{}, ErrClosed
	}

	if entry := lookupLoadedEntry(slot, diskID); entry != nil {
		meta, err := entry.store.UpdateDiskMeta(ctx, name, maxBytes)
		if err != nil {
			return DiskInfo{}, err
		}
		n.touchLastUsed(entry)
		return DiskInfo{
			DiskMeta:  meta,
			UsedBytes: entry.store.UsedBytes(),
		}, nil
	}

	dk := newDiskKeys(diskID)
	snap := n.db.NewSnapshot()
	meta, err := readDiskMeta(snap, dk)
	if err != nil {
		_ = snap.Close()
		return DiskInfo{}, err
	}
	used, err := computeColdUsedBytes(snap, dk)
	closeErr := snap.Close()
	if err != nil {
		return DiskInfo{}, err
	}
	if closeErr != nil {
		return DiskInfo{}, closeErr
	}
	if maxBytes != nil && *maxBytes < used {
		return DiskInfo{}, ErrQuotaShrinkBelowUsed
	}

	if name != nil {
		meta.Name = *name
	}
	if maxBytes != nil {
		meta.MaxBytes = *maxBytes
	}
	meta.UpdatedAt = n.now().UnixNano()

	if err := n.setKeySync(ctx, dk.MetaDiskKey(), EncodeDiskMeta(meta), nil); err != nil {
		return DiskInfo{}, err
	}
	return DiskInfo{
		DiskMeta:  meta,
		UsedBytes: used,
	}, nil
}

func (n *NodeStore) DeleteDisk(ctx context.Context, diskID uuid.UUID) error {
	if diskID == uuid.Nil {
		return ErrInvalid
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	slot := n.getSlot(diskID)
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if n.closed.Load() {
		return ErrClosed
	}

	dk := newDiskKeys(diskID)
	meta, err := readDiskMeta(n.db, dk)
	if err != nil {
		return err
	}

	mounted, err := hasActiveMountOnReader(n.db, dk, "", n.now().UnixNano(), false)
	if err != nil {
		return err
	}
	if mounted {
		return ErrDiskMounted
	}

	var packCloseErr error
	if entry := lookupLoadedEntry(slot, diskID); entry != nil {
		packCloseErr = entry.store.closeResources(func() {
			slot.remove(diskID)
		})
	} else {
		n.clearPackDirty(diskID)
	}

	catKey := CatalogIdxKey(createdAtMicroFromNano(meta.CreatedAt), diskID)
	start, end := dk.RangeBounds()

	batch := n.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if delErr := batch.Delete(catKey, nil); delErr != nil {
		return errors.Join(packCloseErr, delErr)
	}
	if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
		return errors.Join(packCloseErr, delErr)
	}
	if commitErr := n.commitBatchSync(ctx, batch, nil); commitErr != nil {
		return errors.Join(packCloseErr, commitErr)
	}
	if rmErr := os.RemoveAll(n.diskDir(diskID)); rmErr != nil {
		return errors.Join(packCloseErr, rmErr)
	}
	return packCloseErr
}

func (n *NodeStore) ForkBranch(ctx context.Context, diskID uuid.UUID, srcBranch, newBranch, message string) (CommitMeta, BranchMeta, error) {
	var (
		cmt CommitMeta
		br  BranchMeta
	)
	err := n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		var fErr error
		cmt, br, fErr = entry.store.Fork(ctx, srcBranch, newBranch, message)
		return fErr
	})
	if err != nil {
		return CommitMeta{}, BranchMeta{}, err
	}
	return cmt, br, nil
}

func (n *NodeStore) ListBranches(ctx context.Context, diskID uuid.UUID) ([]BranchMeta, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if diskID == uuid.Nil {
		return nil, ErrInvalid
	}
	n.closeMu.RLock()
	defer n.closeMu.RUnlock()
	if n.closed.Load() {
		return nil, ErrClosed
	}

	snap := n.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := newDiskKeys(diskID)
	if _, err := readDiskMeta(snap, dk); err != nil {
		return nil, err
	}
	return listBranchesFromReader(snap, dk)
}

func (n *NodeStore) DeleteBranch(ctx context.Context, diskID uuid.UUID, branch string) error {
	return n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		return entry.store.DeleteBranch(ctx, branch)
	})
}

func (n *NodeStore) ResetBranch(ctx context.Context, diskID uuid.UUID, branch string, targetCommitID uint64) (BranchMeta, error) {
	var br BranchMeta
	err := n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		var rErr error
		br, rErr = entry.store.Reset(ctx, branch, targetCommitID)
		return rErr
	})
	if err != nil {
		return BranchMeta{}, err
	}
	return br, nil
}

func (n *NodeStore) Commit(ctx context.Context, diskID uuid.UUID, branch, message string) (CommitMeta, error) {
	var cmt CommitMeta
	err := n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		var cErr error
		cmt, cErr = entry.store.Commit(ctx, branch, message)
		return cErr
	})
	if err != nil {
		return CommitMeta{}, err
	}
	return cmt, nil
}

func (n *NodeStore) ListCommits(ctx context.Context, q ListCommitsQuery) (ListCommitsPage, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return ListCommitsPage{}, err
		}
	}
	if q.DiskID == uuid.Nil {
		return ListCommitsPage{}, ErrInvalid
	}
	limit, err := NormalizePageSize(q.PageSize)
	if err != nil {
		return ListCommitsPage{}, err
	}
	if !isValidCommitKindFilter(q.Kind) {
		return ListCommitsPage{}, ErrInvalid
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

	n.closeMu.RLock()
	defer n.closeMu.RUnlock()
	if n.closed.Load() {
		return ListCommitsPage{}, ErrClosed
	}

	snap := n.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := newDiskKeys(q.DiskID)
	if _, err = readDiskMeta(snap, dk); err != nil {
		return ListCommitsPage{}, err
	}

	prefix := dk.MetaCommitPrefix()
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return ListCommitsPage{}, err
	}

	var valid bool
	if hasCursor {
		valid = iter.SeekLT(dk.MetaCommitKey(cursorID))
	} else {
		valid = iter.Last()
	}

	commits := make([]CommitMeta, 0, min(int(limit), 64))
	hasMore := false
	for ; valid; valid = iter.Prev() {
		cmt, decErr := DecodeCommitMeta(iter.Value())
		if decErr != nil {
			_ = iter.Close()
			return ListCommitsPage{}, decErr
		}
		if q.Kind != CommitKindUnspecified && cmt.Kind != q.Kind {
			continue
		}
		if len(commits) == int(limit) {
			hasMore = true
			break
		}
		commits = append(commits, cmt)
	}
	if closeErr := iter.Close(); closeErr != nil {
		return ListCommitsPage{}, closeErr
	}

	page := ListCommitsPage{
		Commits: commits,
	}
	if hasMore {
		page.NextPageToken = EncodeCommitPageCursor(commits[len(commits)-1].ID)
	}
	return page, nil
}

func (n *NodeStore) DeleteCommit(ctx context.Context, diskID uuid.UUID, commitID uint64) error {
	return n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		return entry.store.DeleteCommit(ctx, commitID)
	})
}

func (n *NodeStore) PruneCommits(ctx context.Context, diskID uuid.UUID) (int, error) {
	var pruned int
	err := n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		var pErr error
		pruned, pErr = entry.store.PruneCommits(ctx)
		return pErr
	})
	if err != nil {
		return 0, err
	}
	return pruned, nil
}

func (n *NodeStore) MergeBranch(ctx context.Context, diskID uuid.UUID, opts MergeOptions) (MergeBranchResult, error) {
	var out MergeBranchResult
	err := n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		res, mErr := entry.store.Merge(ctx, opts)
		if mErr != nil {
			return mErr
		}
		out = MergeBranchResult{
			BaseCommitID:   res.BaseCommitID,
			TargetCommitID: res.TargetCommitID,
			SourceCommitID: res.SourceCommitID,
			CommitID:       res.CommitID,
			FastForward:    res.FastForward,
			UpToDate:       res.UpToDate,
			WriterEpoch:    res.WriterEpoch,
			Summary:        res.Summary,
		}
		if opts.DryRun {
			return nil
		}
		br, brErr := entry.store.GetBranch(opts.TargetBranch)
		if brErr != nil {
			return brErr
		}
		out.Branch = &br
		if res.CommitID != 0 {
			cmt, cmtErr := entry.store.GetCommit(res.CommitID)
			if cmtErr != nil {
				return cmtErr
			}
			out.Commit = &cmt
		}
		return nil
	})
	if err != nil {
		return MergeBranchResult{}, err
	}
	return out, nil
}

func (n *NodeStore) MountBranch(ctx context.Context, diskID uuid.UUID, branch string, readOnly, force bool) (MountBranchResult, error) {
	var out MountBranchResult
	err := n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		var mErr error
		out, mErr = entry.store.MountBranch(ctx, branch, readOnly, force, n.mountLeaseTTL)
		return mErr
	})
	if err != nil {
		return MountBranchResult{}, err
	}
	return out, nil
}

func (n *NodeStore) RenewMount(ctx context.Context, diskID uuid.UUID, branch string, mountID uuid.UUID, readOnly bool, epoch uint64) (MountMeta, error) {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return MountMeta{}, ErrInvalid
	}
	var out MountMeta
	err := n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		var rErr error
		out, rErr = entry.store.RenewMount(ctx, branch, mountID.String(), readOnly, epoch, n.mountLeaseTTL)
		return rErr
	})
	if err != nil {
		return MountMeta{}, err
	}
	return out, nil
}

func (n *NodeStore) UnmountBranch(ctx context.Context, diskID, mountID uuid.UUID) error {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return ErrInvalid
	}
	return n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		return entry.store.Unmount(ctx, mountID.String())
	})
}

func (n *NodeStore) ListMounts(ctx context.Context, diskID uuid.UUID, branch string) ([]MountMeta, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if diskID == uuid.Nil {
		return nil, ErrInvalid
	}
	if branch != "" {
		if err := validateBranchName(branch); err != nil {
			return nil, err
		}
	}

	n.closeMu.RLock()
	defer n.closeMu.RUnlock()
	if n.closed.Load() {
		return nil, ErrClosed
	}

	snap := n.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := newDiskKeys(diskID)
	if _, err := readDiskMeta(snap, dk); err != nil {
		return nil, err
	}
	return listActiveMountsFromReader(snap, dk, branch, n.now().UnixNano())
}

func (n *NodeStore) EvictMounts(ctx context.Context, diskID uuid.UUID, branch string, mountID uuid.UUID) (EvictMountsResult, error) {
	var out EvictMountsResult
	err := n.withEntry(ctx, diskID, false, func(entry *nodeDiskEntry) error {
		mountIDStr := ""
		if mountID != uuid.Nil {
			mountIDStr = mountID.String()
		}
		var eErr error
		out, eErr = entry.store.EvictMounts(ctx, branch, mountIDStr)
		return eErr
	})
	if err != nil {
		return EvictMountsResult{}, err
	}
	return out, nil
}

func (n *NodeStore) MetricsSnapshot() NodeMetricsSnapshot {
	n.closeMu.RLock()
	defer n.closeMu.RUnlock()
	if n.closed.Load() {
		return NodeMetricsSnapshot{}
	}

	var snap NodeMetricsSnapshot
	for i := range n.slots {
		if entries := n.slots[i].disks.Load(); entries == nil || len(*entries) == 0 {
			continue
		}
		n.slots[i].opMu.RLock()
		if !n.closed.Load() {
			if entries := n.slots[i].disks.Load(); entries != nil {
				for _, item := range *entries {
					if item.entry == nil || item.entry.store == nil {
						continue
					}
					ds := item.entry.store
					snap.DisksLoaded++
					snap.LogicalBytes += ds.UsedBytes()
					snap.PackPhysicalBytes += ds.PackPhysicalBytes()
					snap.Inodes += ds.allocatedInodes()
				}
			}
		}
		n.slots[i].opMu.RUnlock()
	}
	if n.db != nil {
		snap.Pebble = n.db.Metrics()
	}
	if n.cache != nil {
		snap.BlockCache = n.cache.Metrics()
	}
	return snap
}

func (n *NodeStore) runMaintenanceLoop() {
	interval := min(max(n.diskIdleTTL/2, 5*time.Millisecond), time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-n.bgCtx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(n.bgCtx, defaultMaintOpTimeout)
			n.evictIdleDisks(opCtx)
			cancel()
		}
	}
}

func (n *NodeStore) evictIdleDisks(ctx context.Context) int {
	if n.diskIdleTTL <= 0 {
		return 0
	}
	if ctx == nil {
		ctx = context.Background()
	}

	evicted := 0
	ttlNano := n.diskIdleTTL.Nanoseconds()
	var (
		hasNow  bool
		nowNano int64
	)
	for i := range n.slots {
		if ctx.Err() != nil {
			break
		}
		entries := n.slots[i].disks.Load()
		if entries == nil || len(*entries) == 0 {
			continue
		}
		if !hasNow {
			nowNano = n.now().UnixNano()
			hasNow = true
		}
		for _, item := range *entries {
			if ctx.Err() != nil {
				break
			}
			if nowNano-item.entry.lastUsedNano.Load() < ttlNano {
				continue
			}
			if n.tryEvictIdleDisk(&n.slots[i], item.id, ttlNano) {
				evicted++
			}
		}
	}
	return evicted
}

func (n *NodeStore) tryEvictIdleDisk(slot *nodeDiskSlot, diskID uuid.UUID, ttlNano int64) bool {
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if n.closed.Load() {
		return false
	}
	entry := lookupLoadedEntry(slot, diskID)
	if entry == nil {
		return false
	}

	now := n.now()
	if now.UnixNano()-entry.lastUsedNano.Load() < ttlNano {
		return false
	}

	mounted, err := hasActiveMountOnReader(n.db, entry.store.keys, "", now.UnixNano(), false)
	if err != nil || mounted {
		return false
	}

	if closeErr := entry.store.closeResources(func() {
		slot.remove(diskID)
	}); closeErr != nil {
		n.logger.Warn("node idle eviction disk close failed",
			"op", "evict_idle_disk",
			"disk_id", diskID.String(),
			"error", closeErr,
		)
	}
	n.idleEvictions.Add(1)
	return true
}
