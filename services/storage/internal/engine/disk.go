package engine

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
)

type DiskStore struct {
	node         *NodeStore
	diskID       uuid.UUID
	keys         diskKeys
	dir          string
	db           *pebble.DB
	packs        *PackActor
	now          func() time.Time
	blockSize    uint32
	actorIdleTTL time.Duration

	metaMu    sync.RWMutex
	meta      DiskMeta
	usedBytes atomic.Uint64

	allocMu     sync.Mutex
	nextIno     atomic.Uint64
	resIno      atomic.Uint64
	nextLayerID atomic.Uint64
	resLayerID  atomic.Uint64
	nextCmtID   atomic.Uint64
	resCmtID    atomic.Uint64

	actorsMu     sync.Mutex
	branchActors map[string]*BranchActor
	commitActors map[uint64]*BranchActor

	branchRegMu sync.RWMutex
	gcGate      gcGate

	beforeGCRelocateHook   func(packID uint64, loc BlockRefLocation) error
	beforeGCSealActiveHook func(packID uint64) error
	beforeMergeExecHook    func(sourceBranch, targetBranch string) error
	beforeCommitExecHook   func(branch string) error
	beforeForkExecHook     func(srcBranch, newBranch string) error
	onDirVisitedHook       func(dirIno uint64)
	walCommitHook          func(sync bool)
	beforeTickHook         func()

	gcCancel   context.CancelFunc
	gcDone     chan struct{}
	gcWake     chan struct{}
	gcStopOnce sync.Once
	closing    atomic.Bool
}

func validateDiskName(name string) error {
	if len(name) > MaxFileNameBytes || strings.ContainsRune(name, '\x00') {
		return ErrInvalid
	}
	return nil
}

func (d *DiskStore) stopGC() {
	d.gcStopOnce.Do(func() {
		if d.gcCancel != nil {
			d.gcCancel()
			<-d.gcDone
		}
	})
}

func (d *DiskStore) closeResources(onDetached func()) error {
	d.stopGC()
	_ = d.gcGate.lockExclusive(context.Background())
	d.branchRegMu.Lock()
	d.closing.Store(true)
	d.stopAllActors()
	var packErr error
	if d.packs != nil {
		packErr = d.packs.Close()
	}
	if onDetached != nil {
		onDetached()
	}
	if d.node != nil {
		d.node.clearPackDirty(d.diskID)
	}
	d.branchRegMu.Unlock()
	d.gcGate.unlockExclusive()
	return packErr
}

func (d *DiskStore) recoverUsedBytes() error {
	total, err := computeColdUsedBytes(d.db, d.keys)
	if err != nil {
		return err
	}
	d.usedBytes.Store(total)
	return nil
}

func computeColdUsedBytes(r pebbleReader, dk diskKeys) (uint64, error) {
	branches, err := listBranchesFromReader(r, dk)
	if err != nil {
		return 0, err
	}
	commits, err := listCommitsFromReader(r, dk)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, b := range branches {
		total += b.WorkOwnedBytes
	}
	for _, c := range commits {
		total += c.OwnedBytes
	}
	return total, nil
}

func (d *DiskStore) UsedBytes() uint64 {
	return d.usedBytes.Load()
}

func (d *DiskStore) PackPhysicalBytes() uint64 {
	if d.packs == nil {
		return 0
	}
	return d.packs.PhysicalBytes()
}

func (d *DiskStore) reserveQuota(delta uint64) error {
	if delta == 0 {
		return nil
	}
	d.metaMu.RLock()
	maxB := d.meta.MaxBytes
	for {
		cur := d.usedBytes.Load()
		if maxB > 0 && (cur > maxB || delta > maxB-cur) {
			d.metaMu.RUnlock()
			return ErrNoSpace
		}
		if d.usedBytes.CompareAndSwap(cur, cur+delta) {
			d.metaMu.RUnlock()
			return nil
		}
	}
}

func (d *DiskStore) releaseQuota(delta uint64) {
	if delta == 0 {
		return
	}
	for {
		cur := d.usedBytes.Load()
		next := saturatingSub(cur, delta)
		if d.usedBytes.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (d *DiskStore) initAllocatorsFromMeta(meta DiskMeta) {
	d.nextIno.Store(meta.ReservedIno + 1)
	d.resIno.Store(meta.ReservedIno)
	d.nextLayerID.Store(meta.ReservedLayer + 1)
	d.resLayerID.Store(meta.ReservedLayer)
	d.nextCmtID.Store(meta.ReservedCmt + 1)
	d.resCmtID.Store(meta.ReservedCmt)
}

func (d *DiskStore) allocIno() (uint64, error) {
	return d.allocFromSegment(&d.nextIno, &d.resIno, func(m *DiskMeta, newRes uint64) {
		m.ReservedIno = newRes
	})
}

func (d *DiskStore) allocLayerID() (uint64, error) {
	return d.allocFromSegment(&d.nextLayerID, &d.resLayerID, func(m *DiskMeta, newRes uint64) {
		m.ReservedLayer = newRes
	})
}

func (d *DiskStore) allocCommitID() (uint64, error) {
	return d.allocFromSegment(&d.nextCmtID, &d.resCmtID, func(m *DiskMeta, newRes uint64) {
		m.ReservedCmt = newRes
	})
}

func (d *DiskStore) allocFromSegment(nextAtom, resAtom *atomic.Uint64, updateMeta func(*DiskMeta, uint64)) (uint64, error) {
	for {
		cur := nextAtom.Load()
		res := resAtom.Load()
		if cur <= res {
			if nextAtom.CompareAndSwap(cur, cur+1) {
				return cur, nil
			}
			continue
		}
		d.allocMu.Lock()
		cur = nextAtom.Load()
		res = resAtom.Load()
		if cur <= res {
			d.allocMu.Unlock()
			continue
		}
		newRes := res + IDSegmentSize
		d.metaMu.Lock()
		metaCopy := d.meta
		updateMeta(&metaCopy, newRes)
		err := d.setKeySync(context.Background(), d.keys.MetaDiskKey(), EncodeDiskMeta(metaCopy))
		if err != nil {
			d.metaMu.Unlock()
			d.allocMu.Unlock()
			return 0, err
		}
		d.meta = metaCopy
		d.metaMu.Unlock()

		resAtom.Store(newRes)
		d.allocMu.Unlock()
	}
}

func (d *DiskStore) commitTickNoSync(batch *pebble.Batch) error {
	return d.node.commitBatchNoSync(batch, d.walCommitHook)
}

func (d *DiskStore) commitTickSync(ctx context.Context, batch *pebble.Batch) error {
	return d.node.commitTickSync(ctx, batch, d.walCommitHook)
}

func (d *DiskStore) commitBatchSync(ctx context.Context, batch *pebble.Batch) error {
	return d.node.commitBatchSync(ctx, batch, d.walCommitHook)
}

func (d *DiskStore) setKeySync(ctx context.Context, key, val []byte) error {
	return d.node.setKeySync(ctx, key, val, d.walCommitHook)
}

func (d *DiskStore) deleteKeySync(ctx context.Context, key []byte) error {
	return d.node.deleteKeySync(ctx, key, d.walCommitHook)
}

func (d *DiskStore) DiskMeta() DiskMeta {
	d.metaMu.RLock()
	defer d.metaMu.RUnlock()
	return d.meta
}

func (d *DiskStore) UpdateDiskMeta(ctx context.Context, name *string, maxBytes *uint64) (DiskMeta, error) {
	if d.closing.Load() {
		return DiskMeta{}, ErrClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return DiskMeta{}, err
		}
	}
	if name == nil && maxBytes == nil {
		return DiskMeta{}, ErrInvalid
	}
	if name != nil {
		if err := validateDiskName(*name); err != nil {
			return DiskMeta{}, err
		}
	}
	if maxBytes != nil {
		if *maxBytes == 0 || *maxBytes > math.MaxInt64 {
			return DiskMeta{}, ErrInvalid
		}
	}

	d.metaMu.Lock()
	defer d.metaMu.Unlock()

	if d.closing.Load() {
		return DiskMeta{}, ErrClosed
	}
	if maxBytes != nil && *maxBytes < d.UsedBytes() {
		return DiskMeta{}, ErrQuotaShrinkBelowUsed
	}

	metaCopy := d.meta
	if name != nil {
		metaCopy.Name = *name
	}
	if maxBytes != nil {
		metaCopy.MaxBytes = *maxBytes
	}
	metaCopy.UpdatedAt = d.now().UnixNano()

	if err := d.setKeySync(ctx, d.keys.MetaDiskKey(), EncodeDiskMeta(metaCopy)); err != nil {
		return DiskMeta{}, err
	}
	d.meta = metaCopy
	return metaCopy, nil
}

func (d *DiskStore) GetBranch(name string) (BranchMeta, error) {
	if d.closing.Load() {
		return BranchMeta{}, ErrClosed
	}
	if name == "" {
		return BranchMeta{}, ErrInvalid
	}
	val, closer, err := d.db.Get(d.keys.MetaBranchKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return BranchMeta{}, ErrNotFound
	}
	if err != nil {
		return BranchMeta{}, err
	}
	meta, decErr := DecodeBranchMeta(val)
	closeErr := closer.Close()
	if decErr != nil || closeErr != nil {
		return BranchMeta{}, errors.Join(decErr, closeErr)
	}
	return meta, nil
}

func (d *DiskStore) ListBranches() ([]BranchMeta, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	return listBranchesFromReader(d.db, d.keys)
}

func listBranchesFromReader(r pebbleReader, dk diskKeys) ([]BranchMeta, error) {
	prefix := dk.MetaBranchPrefix()
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	var out []BranchMeta
	for iter.First(); iter.Valid(); iter.Next() {
		meta, decErr := DecodeBranchMeta(iter.Value())
		if decErr != nil {
			_ = iter.Close()
			return nil, decErr
		}
		out = append(out, meta)
	}
	if closeErr := iter.Close(); closeErr != nil {
		return nil, closeErr
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (d *DiskStore) ListCommits() ([]CommitMeta, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	return listCommitsFromReader(d.db, d.keys)
}

func listCommitsFromReader(r pebbleReader, dk diskKeys) ([]CommitMeta, error) {
	prefix := dk.MetaCommitPrefix()
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	var out []CommitMeta
	for iter.First(); iter.Valid(); iter.Next() {
		meta, decErr := DecodeCommitMeta(iter.Value())
		if decErr != nil {
			_ = iter.Close()
			return nil, decErr
		}
		out = append(out, meta)
	}
	if closeErr := iter.Close(); closeErr != nil {
		return nil, closeErr
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID > out[j].ID
	})
	return out, nil
}

func readDiskMeta(r pebbleReader, dk diskKeys) (DiskMeta, error) {
	val, closer, err := r.Get(dk.MetaDiskKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return DiskMeta{}, ErrNotFound
	}
	if err != nil {
		return DiskMeta{}, err
	}
	meta, decErr := DecodeDiskMeta(val)
	closeErr := closer.Close()
	if decErr != nil || closeErr != nil {
		return DiskMeta{}, errors.Join(decErr, closeErr)
	}
	return meta, nil
}

func (d *DiskStore) SnapshotControlMeta(now time.Time) (ControlSnapshot, error) {
	if d.closing.Load() {
		return ControlSnapshot{}, ErrClosed
	}
	if now.IsZero() {
		now = d.now()
	}

	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	usedBytes := d.UsedBytes()
	diskMeta, err := readDiskMeta(snap, dk)
	if err != nil {
		return ControlSnapshot{}, err
	}
	branches, err := listBranchesFromReader(snap, dk)
	if err != nil {
		return ControlSnapshot{}, err
	}
	commits, err := listCommitsFromReader(snap, dk)
	if err != nil {
		return ControlSnapshot{}, err
	}
	mounts, err := listActiveMountsFromReader(snap, dk, "", now.UnixNano())
	if err != nil {
		return ControlSnapshot{}, err
	}
	return ControlSnapshot{
		Disk:      diskMeta,
		UsedBytes: usedBytes,
		Branches:  branches,
		Commits:   commits,
		Mounts:    mounts,
	}, nil
}

func (d *DiskStore) GetCommit(commitID uint64) (CommitMeta, error) {
	if d.closing.Load() {
		return CommitMeta{}, ErrClosed
	}
	if commitID == 0 {
		return CommitMeta{}, ErrInvalid
	}
	val, closer, err := d.db.Get(d.keys.MetaCommitKey(commitID))
	if errors.Is(err, pebble.ErrNotFound) {
		return CommitMeta{}, ErrNotFound
	}
	if err != nil {
		return CommitMeta{}, err
	}
	meta, decErr := DecodeCommitMeta(val)
	closeErr := closer.Close()
	if decErr != nil || closeErr != nil {
		return CommitMeta{}, errors.Join(decErr, closeErr)
	}
	return meta, nil
}

type DiskMetricsSnapshot struct {
	UsedBytes          uint64
	PackPhysicalBytes  uint64
	Inodes             uint64
	ActiveBranchActors int
	Pebble             *pebble.Metrics
}

func (d *DiskStore) allocatedInodes() uint64 {
	cur := d.nextIno.Load()
	if cur <= RootIno {
		return 1
	}
	return cur - 1
}

func (d *DiskStore) activeBranchActorCount() int {
	d.actorsMu.Lock()
	n := len(d.branchActors)
	d.actorsMu.Unlock()
	return n
}

func (d *DiskStore) MetricsSnapshot() DiskMetricsSnapshot {
	if d.closing.Load() {
		return DiskMetricsSnapshot{}
	}
	branchCount := d.activeBranchActorCount()

	var pm *pebble.Metrics
	if d.db != nil && !d.closing.Load() {
		pm = d.db.Metrics()
	}
	return DiskMetricsSnapshot{
		UsedBytes:          d.UsedBytes(),
		PackPhysicalBytes:  d.PackPhysicalBytes(),
		Inodes:             d.allocatedInodes(),
		ActiveBranchActors: branchCount,
		Pebble:             pm,
	}
}

func (d *DiskStore) StatFS(_ context.Context) (StatFSResult, error) {
	used := d.UsedBytes()
	return d.buildStatFS(d.allocatedInodes(), used, used)
}

func (d *DiskStore) StatFSBranch(_ context.Context, branch string) (StatFSResult, error) {
	if d.closing.Load() {
		return StatFSResult{}, ErrClosed
	}
	if err := validateBranchName(branch); err != nil {
		return StatFSResult{}, err
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	branchMeta, layers, err := resolveBranchLayers(snap, dk, branch)
	if err != nil {
		return StatFSResult{}, err
	}
	liveInodes, err := countLiveInodes(snap, dk, layers)
	if err != nil {
		return StatFSResult{}, err
	}
	return d.buildStatFS(liveInodes, branchMeta.LogicalBytes, d.UsedBytes())
}

func countLiveInodes(r pebbleReader, dk diskKeys, layers []uint64) (uint64, error) {
	seen := make(map[uint64]struct{})
	var liveCount uint64
	for _, layerID := range layers {
		prefix := dk.LayerInodePrefix(layerID)
		iter, err := r.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: PrefixUpperBound(prefix),
		})
		if err != nil {
			return 0, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			_, ino, decErr := dk.DecodeLayerInodeKey(iter.Key())
			if decErr != nil {
				_ = iter.Close()
				return 0, decErr
			}
			if _, ok := seen[ino]; ok {
				continue
			}
			seen[ino] = struct{}{}
			tag, _, envErr := DecodeEnvelope(iter.Value())
			if envErr != nil {
				_ = iter.Close()
				return 0, envErr
			}
			if tag == TagLive {
				liveCount++
			}
		}
		if closeErr := iter.Close(); closeErr != nil {
			return 0, closeErr
		}
	}
	return liveCount, nil
}

func (d *DiskStore) buildStatFS(files, usedBytes, diskUsedBytes uint64) (StatFSResult, error) {
	if d.closing.Load() {
		return StatFSResult{}, ErrClosed
	}
	total := d.DiskMeta().MaxBytes
	branchFreeBytes := saturatingSub(total, usedBytes)
	diskAvailBytes := saturatingSub(total, diskUsedBytes)
	avail := min(branchFreeBytes, diskAvailBytes)

	const blockSz = uint64(DefaultStatFSBlockSize)
	blocks := total / blockSz
	if blocks == 0 && total > 0 {
		blocks = 1
	}
	var usedBlocks uint64
	if usedBytes > 0 {
		usedBlocks = (usedBytes-1)/blockSz + 1
	}
	bfree := saturatingSub(blocks, usedBlocks)
	bavail := min(bfree, avail/blockSz)

	return StatFSResult{
		TotalBytes: total,
		UsedBytes:  usedBytes,
		AvailBytes: avail,
		Bsize:      DefaultStatFSBlockSize,
		Frsize:     DefaultStatFSBlockSize,
		Blocks:     blocks,
		Bfree:      bfree,
		Bavail:     bavail,
		Files:      files,
		Ffree:      0,
		Namelen:    MaxFileNameBytes,
	}, nil
}
