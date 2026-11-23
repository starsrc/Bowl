package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/google/uuid"
)

const (
	defaultMemTableSize    = 16 << 20
	defaultCacheSize       = 32 << 20
	defaultMaxOpenFiles    = 128
	defaultWALBytesPerSync = 4 << 20
	defaultBloomBitsPerKey = 10
	pebbleLevelCount       = 7
)

type DiskOptions struct {
	Dir          string
	DiskID       string
	MaxBytes     uint64
	BlockSize    uint32
	MaxPackSize  uint32
	Cache        *pebble.Cache
	Now          func() time.Time
	ActorIdleTTL time.Duration
	GCInterval   time.Duration
}

type DiskStore struct {
	dir          string
	db           *pebble.DB
	ownedCache   *pebble.Cache
	packs        *PackActor
	now          func() time.Time
	blockSize    uint32
	actorIdleTTL time.Duration

	metaMu sync.RWMutex
	meta   DiskMeta

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
	gcMu        sync.Mutex

	beforeGCRelocateHook func(packID uint64, loc BlockRefLocation) error

	gcCancel  context.CancelFunc
	gcDone    chan struct{}
	closing   atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func newPebbleOptions(cache *pebble.Cache) *pebble.Options {
	cmp := *pebble.DefaultComparer
	cmp.Name = "bowl.engine.comparer"
	cmp.Split = SplitKey

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

func OpenDisk(opts DiskOptions) (*DiskStore, error) {
	if opts.Dir == "" {
		return nil, ErrInvalid
	}
	blockSize := opts.BlockSize
	if blockSize != 0 {
		if blockSize < MinBlockSize || blockSize > MaxBlockSize || blockSize&(blockSize-1) != 0 {
			return nil, ErrInvalid
		}
	}
	maxPackSize := opts.MaxPackSize
	if maxPackSize == 0 {
		maxPackSize = DefaultMaxPackSize
	}
	if maxPackSize > DefaultMaxPackSize {
		return nil, ErrInvalid
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	pebbleDir := filepath.Join(opts.Dir, "pebble")
	packsDir := filepath.Join(opts.Dir, "packs")
	if err := os.MkdirAll(pebbleDir, 0o755); err != nil {
		return nil, err
	}

	var ownedCache *pebble.Cache
	cache := opts.Cache
	if cache == nil {
		cache = pebble.NewCache(defaultCacheSize)
		ownedCache = cache
	}

	db, err := pebble.Open(pebbleDir, newPebbleOptions(cache))
	if err != nil {
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}

	defaultBlockSize := blockSize
	if defaultBlockSize == 0 {
		defaultBlockSize = DefaultBlockSize
	}
	d := &DiskStore{
		dir:          opts.Dir,
		db:           db,
		ownedCache:   ownedCache,
		now:          nowFn,
		blockSize:    defaultBlockSize,
		actorIdleTTL: opts.ActorIdleTTL,
		branchActors: make(map[string]*BranchActor),
		commitActors: make(map[uint64]*BranchActor),
	}

	if err = d.loadOrBootstrapMeta(opts, defaultBlockSize); err != nil {
		_ = db.Close()
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}
	if maxPackSize < d.blockSize {
		_ = db.Close()
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, ErrInvalid
	}

	packs, err := newPackActor(packsDir, maxPackSize, d.meta.MaxBytes)
	if err != nil {
		_ = db.Close()
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}
	d.packs = packs
	if opts.GCInterval > 0 {
		gcCtx, gcCancel := context.WithCancel(context.Background())
		d.gcCancel = gcCancel
		d.gcDone = make(chan struct{})
		go d.runBackgroundGC(gcCtx, opts.GCInterval)
	}
	return d, nil
}

func (d *DiskStore) loadOrBootstrapMeta(opts DiskOptions, blockSize uint32) error {
	val, closer, err := d.db.Get(MetaDiskKey())
	if err == nil {
		meta, decErr := DecodeDiskMeta(val)
		closeErr := closer.Close()
		if decErr != nil || closeErr != nil {
			return errors.Join(decErr, closeErr)
		}
		if opts.MaxBytes > 0 && opts.MaxBytes != meta.MaxBytes {
			meta.MaxBytes = opts.MaxBytes
			if setErr := d.db.Set(MetaDiskKey(), EncodeDiskMeta(meta), pebble.Sync); setErr != nil {
				return setErr
			}
		}
		d.meta = meta
		d.blockSize = meta.BlockSize
		d.initAllocatorsFromMeta(meta)
		return nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}

	diskID := opts.DiskID
	if diskID == "" {
		diskID = uuid.NewString()
	}
	nowNano := d.now().UnixNano()

	const (
		initCommitID uint64 = 1
		initLayerID  uint64 = 1
		mainWorkID   uint64 = 2
	)

	meta := DiskMeta{
		DiskID:        diskID,
		DefaultBranch: DefaultBranchName,
		MaxBytes:      opts.MaxBytes,
		BlockSize:     blockSize,
		ReservedIno:   RootIno + IDSegmentSize,
		ReservedLayer: mainWorkID + IDSegmentSize,
		ReservedCmt:   initCommitID + IDSegmentSize,
		CreatedAt:     nowNano,
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
		return encErr
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

	batch := d.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if setErr := batch.Set(MetaDiskKey(), EncodeDiskMeta(meta), nil); setErr != nil {
		return setErr
	}
	if setErr := batch.Set(MetaCommitKey(initCommitID), EncodeCommitMeta(initCommit), nil); setErr != nil {
		return setErr
	}
	if setErr := batch.Set(LayerInodeKey(initLayerID, RootIno), EncodeLiveEnvelope(encodedRoot), nil); setErr != nil {
		return setErr
	}
	if setErr := batch.Set(MetaBranchKey(DefaultBranchName), EncodeBranchMeta(mainBranch), nil); setErr != nil {
		return setErr
	}
	if commitErr := batch.Commit(pebble.Sync); commitErr != nil {
		return commitErr
	}

	d.meta = meta
	d.nextIno.Store(RootIno + 1)
	d.resIno.Store(meta.ReservedIno)
	d.nextLayerID.Store(mainWorkID + 1)
	d.resLayerID.Store(meta.ReservedLayer)
	d.nextCmtID.Store(initCommitID + 1)
	d.resCmtID.Store(meta.ReservedCmt)
	return nil
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
		err := d.db.Set(MetaDiskKey(), EncodeDiskMeta(metaCopy), pebble.Sync)
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

func (d *DiskStore) Close() error {
	d.closeOnce.Do(func() {
		d.closing.Store(true)
		if d.gcCancel != nil {
			d.gcCancel()
			<-d.gcDone
		}
		d.stopAllActors()
		var packErr, dbErr error
		if d.packs != nil {
			packErr = d.packs.Close()
		}
		if d.db != nil {
			dbErr = d.db.Close()
		}
		if d.ownedCache != nil {
			d.ownedCache.Unref()
		}
		d.closeErr = errors.Join(packErr, dbErr)
	})
	return d.closeErr
}

func (d *DiskStore) DiskMeta() DiskMeta {
	d.metaMu.RLock()
	defer d.metaMu.RUnlock()
	return d.meta
}

func (d *DiskStore) GetBranch(name string) (BranchMeta, error) {
	if d.closing.Load() {
		return BranchMeta{}, ErrClosed
	}
	if name == "" {
		return BranchMeta{}, ErrInvalid
	}
	val, closer, err := d.db.Get(MetaBranchKey(name))
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
	prefix := MetaBranchPrefix()
	iter, err := d.db.NewIter(&pebble.IterOptions{
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

func (d *DiskStore) GetCommit(commitID uint64) (CommitMeta, error) {
	if d.closing.Load() {
		return CommitMeta{}, ErrClosed
	}
	if commitID == 0 {
		return CommitMeta{}, ErrInvalid
	}
	val, closer, err := d.db.Get(MetaCommitKey(commitID))
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

func (d *DiskStore) StatFS(_ context.Context) (StatFSResult, error) {
	if d.closing.Load() {
		return StatFSResult{}, ErrClosed
	}
	used := d.packs.PhysicalBytes()
	total := d.packs.MaxBytes()
	var avail uint64
	if total > used {
		avail = total - used
	}
	const blockSz = uint64(DefaultStatFSBlockSize)
	blocks := total / blockSz
	if blocks == 0 && total > 0 {
		blocks = 1
	}
	bavail := avail / blockSz
	return StatFSResult{
		TotalBytes: total,
		UsedBytes:  used,
		AvailBytes: avail,
		Bsize:      DefaultStatFSBlockSize,
		Frsize:     DefaultStatFSBlockSize,
		Blocks:     blocks,
		Bfree:      bavail,
		Bavail:     bavail,
		Namelen:    MaxFileNameBytes,
	}, nil
}
