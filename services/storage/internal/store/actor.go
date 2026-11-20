package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	DefaultMemTableSize    = 16 << 20
	DefaultCacheSize       = 64 << 20
	DefaultMaxOpenFiles    = 128
	DefaultWALBytesPerSync = 4 << 20
	DefaultBloomBitsPerKey = 10
	PebbleLevelCount       = 7

	BatchResetMaxReprBytes = 1 << 20
	BatchResetMaxCount     = 8192

	defaultSlowTickThreshold    = 20 * time.Millisecond
	staleEpochLogRateLimitNanos = int64(time.Second)
)

type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

type opKind uint8

const (
	opNoop opKind = iota
	opMknod
	opMkdir
	opCreate
	opCreateTmpfile
	opSymlink
	opLink
	opUnlink
	opRmdir
	opRename
	opSetAttr
	opSetXattr
	opRemoveXattr
	opWrite
	opFallocate
	opFsync
	opFsyncDir
	opPurgeOrphan
	opCleanOrphans
	opAdvanceEpoch
	opUpdateMaxBytes
	opCompactSelect
	opCompactRelocate
	opCompactReap
	opCheckpoint
)

func (op opKind) String() string {
	switch op {
	case opNoop:
		return "noop"
	case opMknod:
		return "mknod"
	case opMkdir:
		return "mkdir"
	case opCreate:
		return "create"
	case opCreateTmpfile:
		return "create_tmpfile"
	case opSymlink:
		return "symlink"
	case opLink:
		return "link"
	case opUnlink:
		return "unlink"
	case opRmdir:
		return "rmdir"
	case opRename:
		return "rename"
	case opSetAttr:
		return "setattr"
	case opSetXattr:
		return "setxattr"
	case opRemoveXattr:
		return "removexattr"
	case opWrite:
		return "write"
	case opFallocate:
		return "fallocate"
	case opFsync:
		return "fsync"
	case opFsyncDir:
		return "fsyncdir"
	case opPurgeOrphan:
		return "purge_orphan"
	case opCleanOrphans:
		return "clean_orphans"
	case opAdvanceEpoch:
		return "advance_epoch"
	case opUpdateMaxBytes:
		return "update_max_bytes"
	case opCompactSelect:
		return "compact_select"
	case opCompactRelocate:
		return "compact_relocate"
	case opCompactReap:
		return "compact_reap"
	case opCheckpoint:
		return "checkpoint"
	default:
		return "unknown"
	}
}

type writeCmd struct {
	op   opKind
	req  any
	err  error
	done chan struct{}

	enqueueAt   time.Time
	dequeueAt   time.Time
	spanCtx     trace.SpanContext
	epoch       uint64
	syncBarrier bool
	syncIno     uint64
	stagedBlobs []BlobRef
}

type Options struct {
	Dir               string
	DiskID            string
	ReadOnly          bool
	MaxBytes          uint64
	MaxInodes         uint64
	AtimeMode         AtimeMode
	Cache             *pebble.Cache
	Now               func() time.Time
	Logger            *slog.Logger
	Metrics           *Metrics
	InoAllocator      InodeSegmentAllocator
	PackAllocator     PackSegmentAllocator
	ResetEpoch        bool
	MaxPackSize       uint32
	SlowTickThreshold time.Duration

	pebbleFS                  vfs.FS
	diskSlowThreshold         time.Duration
	beforeTickHook            func()
	beforeCompactRelocateHook func()
	syncFileHook              func(packID uint64)
	removePackHook            func(packID uint64)
}

func NewPebbleOptions(cache *pebble.Cache, readOnly bool) *pebble.Options {
	if cache == nil {
		cache = pebble.NewCache(DefaultCacheSize)
	}
	cmp := *pebble.DefaultComparer
	cmp.Name = "bowl.store.comparer"
	cmp.Split = SplitKey

	var levels [PebbleLevelCount]pebble.LevelOptions
	policy := bloom.FilterPolicy(DefaultBloomBitsPerKey)
	for i := range levels {
		levels[i].FilterPolicy = policy
	}

	return &pebble.Options{
		Comparer:           &cmp,
		Cache:              cache,
		MemTableSize:       DefaultMemTableSize,
		MaxOpenFiles:       DefaultMaxOpenFiles,
		WALBytesPerSync:    DefaultWALBytesPerSync,
		FormatMajorVersion: pebble.FormatNewest,
		Levels:             levels,
		ReadOnly:           readOnly,
		ErrorIfNotExists:   readOnly,
	}
}

type StoreActor struct {
	dir            string
	diskID         string
	readOnly       bool
	atimeMode      AtimeMode
	now            func() time.Time
	logger         *slog.Logger
	metrics        *Metrics
	db             *pebble.DB
	pebbleListener *pebble.EventListener
	fsCloser       io.Closer
	ownedCache     *pebble.Cache

	maxBytes  atomic.Uint64
	maxInodes atomic.Uint64

	mb struct {
		mu       sync.Mutex
		active   []*writeCmd
		draining []*writeCmd
		notify   chan struct{}
		closed   bool
	}
	cmdPool sync.Pool

	batch                  *pebble.Batch
	stat                   BranchStat
	packStats              map[uint64]PackMeta
	dirtyPackStats         map[uint64]struct{}
	totalPackPhysicalBytes uint64
	totalPackLiveBytes     uint64
	lowWatermarkCount      int
	lastActivePackID       uint64
	nextIno                uint64
	reservedIno            uint64
	maxExistingIno         uint64
	inoAllocator           InodeSegmentAllocator
	walDirty               atomic.Bool
	tickCount              atomic.Uint64
	batchResetCnt          atomic.Uint64

	atomicLogicalBytes      atomic.Uint64
	atomicInodes            atomic.Uint64
	atomicWriterEpoch       atomic.Uint64
	atomicPackPhysicalBytes atomic.Uint64
	atomicPackLiveBytes     atomic.Uint64
	atomicLowWatermarkPacks atomic.Int64
	atomicPendingDeadPacks  atomic.Int64
	pebbleStallCount        atomic.Int64
	pendingEpochAdvances    atomic.Int32

	lastStaleEpochLogNano   atomic.Int64
	suppressedStaleEpochLog atomic.Uint64
	slowTickThreshold       time.Duration

	atimeMu    sync.Mutex
	dirtyAtime map[uint64]int64

	lifecycleMu    sync.RWMutex
	closing        atomic.Bool
	discardOnClose atomic.Bool
	loopDone       chan struct{}
	closeOnce      sync.Once
	closeErr       error

	inodeLocks                [4096]sync.Mutex
	blobs                     *blobStore
	beforeTickHook            func()
	beforeCompactRelocateHook func()
	inFlightMu                sync.Mutex
	inFlightPacks             map[uint64]int
	deadPacksMu               sync.Mutex
	pendingDeadPacks          []uint64
}

func Open(opts Options) (*StoreActor, error) {
	if opts.Dir == "" {
		return nil, ErrInvalid
	}
	maxPackSize := opts.MaxPackSize
	if maxPackSize == 0 {
		maxPackSize = PackMaxSize
	} else if maxPackSize < BlobSize || maxPackSize > PackMaxSize {
		return nil, ErrInvalid
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	baseLogger := opts.Logger
	if baseLogger == nil {
		baseLogger = slog.Default()
	}
	slowTickThresh := opts.SlowTickThreshold
	if slowTickThresh <= 0 {
		slowTickThresh = defaultSlowTickThreshold
	}

	var ownedCache *pebble.Cache
	cache := opts.Cache
	if cache == nil {
		ownedCache = pebble.NewCache(DefaultCacheSize)
		cache = ownedCache
	}

	if !opts.ReadOnly {
		if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
			if ownedCache != nil {
				ownedCache.Unref()
			}
			return nil, err
		}
	}

	s := &StoreActor{
		dir:                       opts.Dir,
		diskID:                    opts.DiskID,
		readOnly:                  opts.ReadOnly,
		atimeMode:                 opts.AtimeMode,
		now:                       nowFn,
		logger:                    baseLogger.With("component", "store", "disk_id", opts.DiskID),
		metrics:                   opts.Metrics,
		ownedCache:                ownedCache,
		packStats:                 make(map[uint64]PackMeta),
		dirtyPackStats:            make(map[uint64]struct{}),
		dirtyAtime:                make(map[uint64]int64),
		loopDone:                  make(chan struct{}),
		inFlightPacks:             make(map[uint64]int),
		inoAllocator:              opts.InoAllocator,
		slowTickThreshold:         slowTickThresh,
		beforeTickHook:            opts.beforeTickHook,
		beforeCompactRelocateHook: opts.beforeCompactRelocateHook,
	}
	s.maxBytes.Store(opts.MaxBytes)
	s.maxInodes.Store(opts.MaxInodes)
	s.mb.notify = make(chan struct{}, 1)
	s.cmdPool.New = func() any {
		return &writeCmd{
			done: make(chan struct{}, 1),
		}
	}

	pebbleOpts := NewPebbleOptions(cache, opts.ReadOnly)
	if opts.pebbleFS != nil {
		pebbleOpts.FS = opts.pebbleFS
	}
	fsCloser := s.configurePebbleObservability(pebbleOpts, baseLogger, opts.diskSlowThreshold)

	db, err := pebble.Open(opts.Dir, pebbleOpts)
	if err != nil {
		if fsCloser != nil {
			_ = fsCloser.Close()
		}
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}
	s.db = db
	s.fsCloser = fsCloser

	if err := s.initOrLoadState(&opts); err != nil {
		_ = db.Close()
		if fsCloser != nil {
			_ = fsCloser.Close()
		}
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}

	s.blobs = newBlobStore(
		s.dir,
		maxPackSize,
		baseLogger.With("component", "blobstore", "disk_id", opts.DiskID),
		opts.Metrics,
		opts.PackAllocator,
		opts.syncFileHook,
		opts.removePackHook,
	)

	if err := s.syncPackStatsWithBlobs(); err != nil {
		_ = s.blobs.close()
		_ = db.Close()
		if fsCloser != nil {
			_ = fsCloser.Close()
		}
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}

	s.publishAtomicStats()

	if s.readOnly {
		close(s.loopDone)
	} else {
		s.batch = s.db.NewIndexedBatch()
		go pprof.Do(context.Background(), pprof.Labels("component", "store_actor", "disk_id", s.diskID), func(context.Context) {
			s.runLoop()
		})
	}

	return s, nil
}

func (s *StoreActor) initOrLoadState(opts *Options) error {
	val, closer, err := s.db.Get(StatKey())
	if errors.Is(err, pebble.ErrNotFound) {
		if s.readOnly {
			return ErrNotFound
		}
		nowNano := s.now().UnixNano()
		root := Inode{
			Ino:        RootIno,
			ParentIno:  RootIno,
			Mode:       S_IFDIR | 0o755,
			Nlink:      2,
			Size:       0,
			Atime:      nowNano,
			Mtime:      nowNano,
			Ctime:      nowNano,
			Generation: 1,
			WriteSeq:   1,
		}
		stat := BranchStat{
			LogicalBytes: 0,
			Inodes:       1,
			NextWriteSeq: 1,
			WriterEpoch:  0,
			MaxBytes:     s.maxBytes.Load(),
		}
		initBatch := s.db.NewBatch()
		defer func() { _ = initBatch.Close() }()
		err = BatchSetInodeDeferred(initBatch, root)
		if err != nil {
			return err
		}
		BatchSetStatDeferred(initBatch, stat)
		err = s.db.Apply(initBatch, pebble.Sync)
		if err != nil {
			return err
		}
		s.stat = stat
		s.atomicLogicalBytes.Store(stat.LogicalBytes)
		s.atomicInodes.Store(stat.Inodes)
		s.atomicWriterEpoch.Store(stat.WriterEpoch)
		s.maxExistingIno = RootIno
		if s.inoAllocator != nil {
			s.nextIno = 0
			s.reservedIno = 0
		} else {
			s.reservedIno = RootIno
		}
		return nil
	}
	if err != nil {
		return err
	}
	stat, decErr := DecodeBranchStat(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return errors.Join(decErr, closeErr)
	}
	if !s.readOnly && opts.ResetEpoch {
		if opts.MaxBytes > 0 {
			if opts.MaxBytes < stat.LogicalBytes {
				return ErrQuotaShrinkBelowUsed
			}
			stat.MaxBytes = opts.MaxBytes
			s.maxBytes.Store(opts.MaxBytes)
		} else if stat.MaxBytes > 0 {
			s.maxBytes.Store(stat.MaxBytes)
		}
		stat.WriterEpoch = 0
		resetBatch := s.db.NewBatch()
		BatchSetStatDeferred(resetBatch, stat)
		applyErr := s.db.Apply(resetBatch, pebble.Sync)
		closeBatchErr := resetBatch.Close()
		if applyErr != nil || closeBatchErr != nil {
			return errors.Join(applyErr, closeBatchErr)
		}
	} else if !s.readOnly && opts.MaxBytes > 0 && opts.MaxBytes != stat.MaxBytes {
		stat.MaxBytes = opts.MaxBytes
		syncBatch := s.db.NewBatch()
		BatchSetStatDeferred(syncBatch, stat)
		applyErr := s.db.Apply(syncBatch, pebble.Sync)
		closeBatchErr := syncBatch.Close()
		if applyErr != nil || closeBatchErr != nil {
			return errors.Join(applyErr, closeBatchErr)
		}
		s.maxBytes.Store(opts.MaxBytes)
	} else if stat.MaxBytes > 0 {
		s.maxBytes.Store(stat.MaxBytes)
	} else if maxB := s.maxBytes.Load(); maxB > 0 {
		stat.MaxBytes = maxB
	}
	s.stat = stat
	s.atomicLogicalBytes.Store(stat.LogicalBytes)
	s.atomicInodes.Store(stat.Inodes)
	s.atomicWriterEpoch.Store(stat.WriterEpoch)

	if !s.readOnly {
		maxIno, scanErr := s.scanMaxInode()
		if scanErr != nil {
			return scanErr
		}
		s.maxExistingIno = max(maxIno, RootIno)
		if s.inoAllocator != nil {
			s.nextIno = 0
			s.reservedIno = 0
		} else {
			s.reservedIno = s.maxExistingIno
		}
	}
	return s.loadPackStats()
}

func (s *StoreActor) scanMaxInode() (uint64, error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	if iter.SeekLT([]byte("inode;")) && bytes.HasPrefix(iter.Key(), inodePrefixBytes) {
		ino, err := DecodeInodeKey(iter.Key())
		if err != nil {
			return 0, err
		}
		return ino, iter.Error()
	}
	return RootIno, iter.Error()
}

func (s *StoreActor) enter() (func(), error) {
	if s.closing.Load() {
		return nil, ErrClosed
	}
	s.lifecycleMu.RLock()
	if s.closing.Load() {
		s.lifecycleMu.RUnlock()
		return nil, ErrClosed
	}
	return s.lifecycleMu.RUnlock, nil
}

func (s *StoreActor) Dir() string { return s.dir }

func (s *StoreActor) ReadOnly() bool { return s.readOnly }

func (s *StoreActor) SetMaxBytes(maxBytes uint64) { s.maxBytes.Store(maxBytes) }

func (s *StoreActor) MaxBytes() uint64 { return s.maxBytes.Load() }

func (s *StoreActor) WriterEpoch() uint64 { return s.atomicWriterEpoch.Load() }

type advanceEpochOp struct {
	plans    []inodeReclaimPlan
	root     Inode
	oldEpoch uint64
	epoch    uint64
	rootAttr Attribute
}

func (s *StoreActor) AdvanceEpoch(ctx context.Context) (uint64, Attribute, error) {
	if s.readOnly {
		return 0, Attribute{}, ErrReadOnly
	}
	s.pendingEpochAdvances.Add(1)
	defer s.pendingEpochAdvances.Add(-1)

	op := &advanceEpochOp{}
	cmd := s.acquireCmd()
	cmd.op = opAdvanceEpoch
	cmd.req = op
	if err := s.submit(ctx, cmd); err != nil {
		return 0, Attribute{}, err
	}
	if s.logger != nil {
		s.logger.Info("writer epoch advanced",
			"old_epoch", op.oldEpoch,
			"new_epoch", op.epoch,
			"reclaimed_orphans", len(op.plans),
		)
	}
	return op.epoch, op.rootAttr, nil
}

func (s *StoreActor) validateAdvanceEpochCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*advanceEpochOp)
	if s.stat.WriterEpoch == math.MaxUint64 {
		return ErrNoSpace
	}
	root, err := s.readInode(s.batch, RootIno)
	if err != nil {
		return err
	}
	plans, err := s.planCleanOrphans(s.batch)
	if err != nil {
		return err
	}
	op.root = root
	op.plans = plans
	return nil
}

func (s *StoreActor) mutateAdvanceEpochCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*advanceEpochOp)
	op.oldEpoch = s.stat.WriterEpoch
	s.stat.WriterEpoch++
	if len(op.plans) > 0 {
		s.stat.NextWriteSeq++
		for i := range op.plans {
			s.applyInodeReclaimPlan(&op.plans[i])
		}
	}
	s.overlayAtime(&op.root)
	op.epoch = s.stat.WriterEpoch
	op.rootAttr = op.root.ToAttribute()
	cmd.syncBarrier = true
}

type updateMaxBytesOp struct {
	maxBytes uint64
}

func (s *StoreActor) UpdateMaxBytes(ctx context.Context, maxBytes uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	if maxBytes == 0 {
		return ErrInvalid
	}
	cmd := s.acquireCmd()
	cmd.op = opUpdateMaxBytes
	cmd.req = &updateMaxBytesOp{maxBytes: maxBytes}
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateUpdateMaxBytesCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*updateMaxBytesOp)
	if op.maxBytes == 0 {
		return ErrInvalid
	}
	if op.maxBytes < s.stat.LogicalBytes {
		return ErrQuotaShrinkBelowUsed
	}
	return nil
}

func (s *StoreActor) mutateUpdateMaxBytesCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*updateMaxBytesOp)
	s.stat.MaxBytes = op.maxBytes
	s.maxBytes.Store(op.maxBytes)
	cmd.syncBarrier = true
}

func (s *StoreActor) SetMaxInodes(maxInodes uint64) { s.maxInodes.Store(maxInodes) }

func (s *StoreActor) MaxInodes() uint64 { return s.maxInodes.Load() }

func (s *StoreActor) TickCount() uint64 { return s.tickCount.Load() }

func (s *StoreActor) BatchReallocCount() uint64 { return s.batchResetCnt.Load() }

func (s *StoreActor) WALDirty() bool { return s.walDirty.Load() }

func (s *StoreActor) StatFS(_ context.Context) (StatFSResult, error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpStatFS, time.Since(opStart))
		}()
	}
	if s.closing.Load() {
		return StatFSResult{}, ErrClosed
	}
	usedBytes := s.atomicLogicalBytes.Load()
	usedInodes := s.atomicInodes.Load()
	totalBytes := s.maxBytes.Load()

	var availBytes uint64
	if totalBytes > usedBytes {
		availBytes = totalBytes - usedBytes
	}

	const blockSize = uint64(DefaultStatFSBlockSize)
	blocks := totalBytes / blockSize
	if blocks == 0 && totalBytes > 0 {
		blocks = 1
	}
	bavail := availBytes / blockSize

	return StatFSResult{
		TotalBytes: totalBytes,
		UsedBytes:  usedBytes,
		AvailBytes: availBytes,
		Bsize:      DefaultStatFSBlockSize,
		Frsize:     DefaultStatFSBlockSize,
		Blocks:     blocks,
		Bfree:      bavail,
		Bavail:     bavail,
		Files:      usedInodes,
		Ffree:      0,
		Namelen:    MaxFileNameBytes,
	}, nil
}

func (s *StoreActor) GetAttr(ctx context.Context, ino uint64) (Attribute, error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpGetAttr, time.Since(opStart))
		}()
	}
	if ino == 0 || ino > math.MaxInt64 {
		return Attribute{}, ErrInvalid
	}
	unlock, err := s.enter()
	if err != nil {
		return Attribute{}, err
	}
	defer unlock()

	inode, err := s.readInodeFromDB(ctx, ino)
	if err != nil {
		return Attribute{}, err
	}
	if err := s.checkInodeLiveFromDB(ctx, &inode); err != nil {
		return Attribute{}, err
	}
	s.overlayAtime(&inode)
	return inode.ToAttribute(), nil
}

func (s *StoreActor) dbGetPoint(ctx context.Context, key []byte) ([]byte, io.Closer, error) {
	var span trace.Span
	if isSpanRecording(ctx) {
		_, span = startChildSpan(ctx, "store.KVGet")
	}
	var start time.Time
	if s.metrics != nil {
		start = time.Now()
	}
	val, closer, err := s.db.Get(key)
	if s.metrics != nil {
		s.metrics.observeKVPointGet(time.Since(start))
	}
	if span != nil {
		span.SetAttributes(attribute.Bool("kv.found", err == nil))
		var spanErr error
		if err != nil && !errors.Is(err, pebble.ErrNotFound) {
			spanErr = err
		}
		finishSpan(span, spanErr)
	}
	return val, closer, err
}

func (s *StoreActor) readInodeFromDB(ctx context.Context, ino uint64) (Inode, error) {
	var keyBuf [InodeKeyLen]byte
	putInodeKey(keyBuf[:], ino)
	val, closer, err := s.dbGetPoint(ctx, keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return Inode{}, ErrNotFound
	}
	if err != nil {
		return Inode{}, err
	}
	inode, decErr := DecodeInode(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return Inode{}, errors.Join(decErr, closeErr)
	}
	if inode.Ino != ino {
		return Inode{}, ErrInvalidValue
	}
	return inode, nil
}

func (s *StoreActor) checkInodeLiveFromDB(ctx context.Context, inode *Inode) error {
	if inode.Nlink != 0 {
		return nil
	}
	if s.readOnly {
		return ErrNotFound
	}
	var keyBuf [OrphanKeyLen]byte
	putOrphanKey(keyBuf[:], inode.Ino)
	_, closer, err := s.dbGetPoint(ctx, keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return closer.Close()
}

func (s *StoreActor) readInode(reader pebbleReader, ino uint64) (Inode, error) {
	var keyBuf [InodeKeyLen]byte
	putInodeKey(keyBuf[:], ino)
	val, closer, err := reader.Get(keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return Inode{}, ErrNotFound
	}
	if err != nil {
		return Inode{}, err
	}
	inode, decErr := DecodeInode(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return Inode{}, errors.Join(decErr, closeErr)
	}
	if inode.Ino != ino {
		return Inode{}, ErrInvalidValue
	}
	return inode, nil
}

func (s *StoreActor) checkInodeLive(reader pebbleReader, inode *Inode) error {
	if inode.Nlink != 0 {
		return nil
	}
	if s.readOnly {
		return ErrNotFound
	}
	var keyBuf [OrphanKeyLen]byte
	putOrphanKey(keyBuf[:], inode.Ino)
	_, closer, err := reader.Get(keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return closer.Close()
}

func (s *StoreActor) MarkAtimeDirty(ino uint64, atime time.Time) {
	s.markAtimeDirty(ino, atime.UnixNano())
}

func (s *StoreActor) markAtimeDirty(ino uint64, unixNano int64) {
	if ino == 0 || s.readOnly || s.closing.Load() {
		return
	}
	s.atimeMu.Lock()
	if prev, ok := s.dirtyAtime[ino]; !ok || unixNano > prev {
		s.dirtyAtime[ino] = unixNano
	}
	s.atimeMu.Unlock()
}

func (s *StoreActor) overlayAtime(inode *Inode) {
	if inode == nil {
		return
	}
	s.atimeMu.Lock()
	if ns, ok := s.dirtyAtime[inode.Ino]; ok && ns > inode.Atime {
		inode.Atime = ns
	}
	s.atimeMu.Unlock()
}

func (s *StoreActor) discardDirtyAtime(ino uint64) {
	if ino == 0 {
		return
	}
	s.atimeMu.Lock()
	delete(s.dirtyAtime, ino)
	s.atimeMu.Unlock()
}

func (s *StoreActor) FlushWAL() error {
	if s.readOnly {
		return nil
	}
	unlock, err := s.enter()
	if err != nil {
		return err
	}
	defer unlock()

	return s.syncWALAndReclaimDeadPacks()
}

func (s *StoreActor) acquireCmd() *writeCmd {
	cmd, _ := s.cmdPool.Get().(*writeCmd)
	return cmd
}

func (s *StoreActor) releaseCmd(cmd *writeCmd) {
	cmd.op = 0
	cmd.req = nil
	cmd.err = nil
	cmd.enqueueAt = time.Time{}
	cmd.dequeueAt = time.Time{}
	cmd.spanCtx = trace.SpanContext{}
	cmd.epoch = 0
	cmd.syncBarrier = false
	cmd.syncIno = 0
	cmd.stagedBlobs = cmd.stagedBlobs[:0]
	s.cmdPool.Put(cmd)
}

func (s *StoreActor) lockInode(ctx context.Context, ino uint64) *sync.Mutex {
	mu := &s.inodeLocks[ino&0xfff]
	if mu.TryLock() {
		return mu
	}
	recording := isSpanRecording(ctx)
	if s.metrics != nil || recording {
		waitStart := time.Now()
		mu.Lock()
		waitEnd := time.Now()
		s.metrics.observeInodeLockWait(waitEnd.Sub(waitStart))
		if recording {
			recordWindowSpan(ctx, "store.InodeLockWait", waitStart, waitEnd, nil, attribute.Int64("inode.ino", int64(ino)))
		}
		return mu
	}
	mu.Lock()
	return mu
}

func resolveEpoch(epoch []uint64) uint64 {
	if len(epoch) > 0 {
		return epoch[0]
	}
	return 0
}

func (s *StoreActor) isDefinitelyStaleEpoch(epoch uint64) bool {
	cur := s.atomicWriterEpoch.Load()
	if epoch < cur {
		return true
	}
	if epoch > cur && s.pendingEpochAdvances.Load() == 0 {
		return true
	}
	return false
}

func (s *StoreActor) submitStaleEpoch(ctx context.Context, op opKind, epoch uint64) error {
	cmd := s.acquireCmd()
	cmd.op = op
	cmd.epoch = epoch
	return s.submit(ctx, cmd)
}

func (s *StoreActor) submit(ctx context.Context, cmd *writeCmd) error {
	if s.readOnly {
		s.unpinStagedBlobs(cmd.stagedBlobs)
		s.releaseCmd(cmd)
		return ErrReadOnly
	}
	unlock, err := s.enter()
	if err != nil {
		s.unpinStagedBlobs(cmd.stagedBlobs)
		s.releaseCmd(cmd)
		return err
	}
	defer unlock()
	return s.enqueueAndWait(ctx, cmd)
}

func (s *StoreActor) enqueueAndWait(ctx context.Context, cmd *writeCmd) error {
	recording := isSpanRecording(ctx)
	if s.metrics != nil || recording {
		cmd.enqueueAt = time.Now()
	}
	if recording {
		cmd.spanCtx = sampledSpanContext(ctx)
	}
	s.mb.mu.Lock()
	if s.mb.closed {
		s.mb.mu.Unlock()
		s.unpinStagedBlobs(cmd.stagedBlobs)
		s.releaseCmd(cmd)
		return ErrClosed
	}
	s.mb.active = append(s.mb.active, cmd)
	select {
	case s.mb.notify <- struct{}{}:
	default:
	}
	s.mb.mu.Unlock()

	<-cmd.done
	if recording && !cmd.enqueueAt.IsZero() && !cmd.dequeueAt.IsZero() {
		recordWindowSpan(ctx, "store.QueueWait", cmd.enqueueAt, cmd.dequeueAt, nil, attribute.String("store.op", cmd.op.String()))
	}
	cmdErr := cmd.err
	s.releaseCmd(cmd)
	return cmdErr
}

func (s *StoreActor) prepareIno(parentSC trace.SpanContext) (uint64, error) {
	if s.nextIno == 0 || s.nextIno > s.reservedIno {
		if err := s.reserveInoSegment(parentSC); err != nil {
			return 0, err
		}
	}
	if s.nextIno > uint64(math.MaxInt64) {
		return 0, ErrInodeExhausted
	}
	return s.nextIno, nil
}

func (s *StoreActor) reserveInoSegment(parentSC trace.SpanContext) error {
	last := max(s.maxExistingIno, s.reservedIno, RootIno)
	if last >= uint64(math.MaxInt64) {
		return ErrInodeExhausted
	}
	if s.inoAllocator != nil {
		allocCtx := context.Background()
		if parentSC.IsValid() {
			allocCtx = trace.ContextWithRemoteSpanContext(allocCtx, parentSC)
		}
		first, reserved, err := s.inoAllocator(allocCtx, last+1)
		if err != nil {
			return err
		}
		if first > uint64(math.MaxInt64) {
			return ErrInodeExhausted
		}
		if first <= last || reserved < first {
			return ErrInvalidValue
		}
		if reserved > uint64(math.MaxInt64) {
			reserved = uint64(math.MaxInt64)
		}
		s.nextIno = first
		s.reservedIno = reserved
		s.maxExistingIno = reserved
		return nil
	}
	first := last + 1
	var reserved uint64
	if uint64(math.MaxInt64)-last < InodeSegment {
		reserved = uint64(math.MaxInt64)
	} else {
		reserved = last + InodeSegment
	}
	s.nextIno = first
	s.reservedIno = reserved
	s.maxExistingIno = reserved
	return nil
}

func (s *StoreActor) consumeIno() uint64 {
	ino := s.nextIno
	s.nextIno++
	return ino
}

func (s *StoreActor) checkQuota(bytesDelta, inodesDelta uint64) error {
	if maxB := s.maxBytes.Load(); maxB > 0 && bytesDelta > 0 {
		if s.stat.LogicalBytes > maxB || bytesDelta > maxB-s.stat.LogicalBytes {
			return ErrNoSpace
		}
	}
	if maxI := s.maxInodes.Load(); maxI > 0 && inodesDelta > 0 {
		if s.stat.Inodes > maxI || inodesDelta > maxI-s.stat.Inodes {
			return ErrNoSpace
		}
	}
	return nil
}

func (s *StoreActor) runLoop() {
	defer close(s.loopDone)
	for {
		<-s.mb.notify
		for {

			s.mb.mu.Lock()
			if len(s.mb.active) == 0 {
				closed := s.mb.closed
				s.mb.mu.Unlock()
				if closed {
					if !s.discardOnClose.Load() {
						s.closeErr = s.flushOnClose()
					}
					return
				}
				break
			}
			s.mb.active, s.mb.draining = s.mb.draining[:0], s.mb.active
			s.mb.mu.Unlock()

			s.processTick()
		}
	}
}

func (s *StoreActor) processTick() {
	if s.beforeTickHook != nil {
		s.beforeTickHook()
	}
	tickStart := time.Now()
	cmds := s.mb.draining
	batchSize := len(cmds)
	if s.metrics != nil {
		s.metrics.observeTickBatchSize(batchSize)
	}

	var sampledCount int
	for _, cmd := range cmds {
		cmd.dequeueAt = tickStart
		if s.metrics != nil && !cmd.enqueueAt.IsZero() {
			s.metrics.observeQueueWait(tickStart.Sub(cmd.enqueueAt))
		}
		if cmd.spanCtx.IsValid() {
			sampledCount++
		}
	}

	var (
		tickCtx  context.Context
		tickSpan trace.Span
	)
	if sampledCount > 0 {
		links := make([]trace.Link, 0, sampledCount)
		var parentSC trace.SpanContext
		for _, cmd := range cmds {
			if !cmd.spanCtx.IsValid() {
				continue
			}
			if !parentSC.IsValid() {
				parentSC = cmd.spanCtx
			}
			links = append(links, trace.Link{
				SpanContext: cmd.spanCtx,
				Attributes:  []attribute.KeyValue{attribute.String("store.op", cmd.op.String())},
			})
		}
		parentCtx := trace.ContextWithRemoteSpanContext(context.Background(), parentSC)
		tickCtx, tickSpan = currentStoreTracer().Start(parentCtx, "store.Tick",
			trace.WithTimestamp(tickStart),
			trace.WithLinks(links...),
			trace.WithAttributes(attribute.Int("batch_size", batchSize)),
		)
	}

	var (
		hasSyncBarrier  bool
		hasCheckpoint   bool
		fsyncFileInodes []uint64
		mutated         bool
		phaseADur       time.Duration
		phaseBDur       time.Duration
	)

	for _, cmd := range cmds {

		aStart := time.Now()
		err := s.validateCmd(cmd)
		phaseADur += time.Since(aStart)
		if err != nil {
			bStart := time.Now()
			for _, ref := range cmd.stagedBlobs {
				s.recordDeadBlobWrite(ref)
			}
			s.unpinStagedBlobs(cmd.stagedBlobs)
			cmd.stagedBlobs = cmd.stagedBlobs[:0]
			cmd.err = err
			phaseBDur += time.Since(bStart)
			continue
		}

		bStart := time.Now()
		prevCount := s.batch.Count()
		prevStat := s.stat
		s.mutateCmd(cmd)
		for _, ref := range cmd.stagedBlobs {
			if meta, ok := s.packStats[ref.PackID]; ok && meta.LiveBytes == 0 {
				s.dirtyPackStats[ref.PackID] = struct{}{}
			}
		}
		s.unpinStagedBlobs(cmd.stagedBlobs)
		cmd.stagedBlobs = cmd.stagedBlobs[:0]
		if s.batch.Count() > prevCount || s.stat != prevStat || len(s.dirtyPackStats) > 0 || cmd.op == opUpdateMaxBytes {
			mutated = true
		}
		if cmd.op == opCheckpoint {
			hasCheckpoint = true
		}
		if cmd.syncBarrier || cmd.op == opFsync || cmd.op == opFsyncDir {
			hasSyncBarrier = true
			if cmd.syncIno != 0 && !slices.Contains(fsyncFileInodes, cmd.syncIno) {
				fsyncFileInodes = append(fsyncFileInodes, cmd.syncIno)
			}
		}
		phaseBDur += time.Since(bStart)
	}

	var (
		commitErr      error
		deadPacks      []uint64
		packSyncDur    time.Duration
		walApplyDur    time.Duration
		deadRemoveDur  time.Duration
		reclaimedPacks int
	)
	sealStart := time.Now()
	if mutated || s.batch.Count() > 0 || hasSyncBarrier || len(s.dirtyPackStats) > 0 {

		s.flushDirtyAtimeToBatch(s.batch)
		var wrotePacks bool
		deadPacks, wrotePacks = s.flushDirtyPackStatsToBatch(s.batch)
		if wrotePacks {
			mutated = true
		}
	}
	if mutated {

		BatchSetStatDeferred(s.batch, s.stat)
	}
	phaseBDur += time.Since(sealStart)
	s.metrics.observePhaseAValidate(phaseADur)
	s.metrics.observePhaseBMutate(phaseBDur)

	if tickSpan != nil {
		phaseAEnd := tickStart.Add(phaseADur)
		phaseBEnd := phaseAEnd.Add(phaseBDur)
		recordWindowSpan(tickCtx, "store.PhaseA", tickStart, phaseAEnd, nil, attribute.Int("batch_size", batchSize))
		recordWindowSpan(tickCtx, "store.PhaseB", phaseAEnd, phaseBEnd, nil, attribute.Int("batch_counts", int(s.batch.Count())))
	}

	if s.batch.Count() > 0 {
		if hasSyncBarrier {
			var syncSpan trace.Span
			if tickSpan != nil {
				_, syncSpan = startChildSpan(tickCtx, "store.SyncBarrier",
					attribute.Bool("has_batch", true),
					attribute.Int("fsync_inodes", len(fsyncFileInodes)),
				)
			}
			packSyncDur, walApplyDur, deadRemoveDur, reclaimedPacks, commitErr = s.commitWithSyncBarrier(fsyncFileInodes, deadPacks)
			finishSpan(syncSpan, commitErr)
		} else {
			applyStart := time.Now()
			commitErr = s.db.Apply(s.batch, pebble.NoSync)
			walApplyDur = time.Since(applyStart)
			s.metrics.observePhaseWALApplyNoSync(walApplyDur)
			if commitErr == nil {
				s.addPendingDeadPacks(deadPacks)
				s.walDirty.Store(true)
			}
		}
		if commitErr == nil {
			s.publishAtomicStats()
		}
	} else if hasSyncBarrier {
		var syncSpan trace.Span
		if tickSpan != nil {
			_, syncSpan = startChildSpan(tickCtx, "store.SyncBarrier",
				attribute.Bool("has_batch", false),
				attribute.Int("fsync_inodes", len(fsyncFileInodes)),
			)
		}
		packSyncDur, walApplyDur, deadRemoveDur, reclaimedPacks, commitErr = s.syncBarrierWithoutBatch(fsyncFileInodes)
		finishSpan(syncSpan, commitErr)
	}

	if len(s.batch.Repr()) > BatchResetMaxReprBytes || s.batch.Count() > BatchResetMaxCount {
		s.batch = s.db.NewIndexedBatch()
		s.batchResetCnt.Add(1)
	} else {
		s.batch.Reset()
	}

	tickErr := commitErr
	var checkpointDur time.Duration
	if hasCheckpoint && commitErr == nil {
		ckStart := time.Now()
		ckCtx := context.Background()
		var ckSpan trace.Span
		if tickSpan != nil {
			ckCtx, ckSpan = startChildSpan(tickCtx, "store.Checkpoint")
		}
		var (
			firstCkErr error
			livePacks  int
			hasOrphans bool
		)
		for _, cmd := range cmds {
			if cmd.op == opCheckpoint && cmd.err == nil {
				cmd.err = s.execCheckpointCmd(ckCtx, cmd)
				if op, ok := cmd.req.(*checkpointOp); ok && op != nil {
					livePacks = op.stats.LivePacks
					hasOrphans = op.stats.HasOrphans
				}
				if cmd.err != nil && firstCkErr == nil {
					firstCkErr = cmd.err
				}
			}
		}
		checkpointDur = time.Since(ckStart)
		s.metrics.observePhaseCheckpoint(checkpointDur, sampledSpanContext(ckCtx))
		if ckSpan != nil {
			ckSpan.SetAttributes(
				attribute.Int("checkpoint.live_packs", livePacks),
				attribute.Bool("checkpoint.has_orphans", hasOrphans),
			)
			finishSpan(ckSpan, firstCkErr)
		}
		tickErr = firstCkErr
	}

	if tickSpan != nil {
		tickSpan.SetAttributes(
			attribute.Bool("has_sync_barrier", hasSyncBarrier),
			attribute.Bool("has_checkpoint", hasCheckpoint),
			attribute.Int("reclaimed_packs", reclaimedPacks),
		)
		finishSpan(tickSpan, tickErr)
	}

	s.tickCount.Add(1)

	for i, cmd := range cmds {
		if cmd.err == nil && commitErr != nil {
			cmd.err = commitErr
		}
		cmds[i] = nil
		cmd.done <- struct{}{}
	}

	totalDur := time.Since(tickStart)
	if s.slowTickThreshold > 0 && totalDur > s.slowTickThreshold && s.logger != nil {
		logCtx := tickCtx
		if logCtx == nil {
			logCtx = context.Background()
		}
		s.logger.WarnContext(logCtx, "slow actor tick",
			"batch_size", batchSize,
			"has_sync_barrier", hasSyncBarrier,
			"has_checkpoint", hasCheckpoint,
			"fsync_inodes", len(fsyncFileInodes),
			"reclaimed_packs", reclaimedPacks,
			"total_ms", totalDur.Milliseconds(),
			"phase_a_ms", phaseADur.Milliseconds(),
			"phase_b_ms", phaseBDur.Milliseconds(),
			"pack_sync_ms", packSyncDur.Milliseconds(),
			"wal_apply_ms", walApplyDur.Milliseconds(),
			"dead_pack_remove_ms", deadRemoveDur.Milliseconds(),
			"checkpoint_ms", checkpointDur.Milliseconds(),
		)
	}
}

func isExternalWriteOp(op opKind) bool {
	switch op {
	case opMknod, opMkdir, opCreate, opCreateTmpfile, opSymlink, opLink,
		opUnlink, opRmdir, opRename,
		opSetAttr, opSetXattr, opRemoveXattr,
		opWrite, opFallocate,
		opFsync, opFsyncDir,
		opPurgeOrphan:
		return true
	default:
		return false
	}
}

func (s *StoreActor) logStaleEpochFenced(op opKind, reqEpoch, curEpoch uint64) {
	if s.logger == nil {
		return
	}
	nowNano := time.Now().UnixNano()
	prev := s.lastStaleEpochLogNano.Load()
	if prev == 0 || nowNano-prev >= staleEpochLogRateLimitNanos {
		if s.lastStaleEpochLogNano.CompareAndSwap(prev, nowNano) {
			suppressed := s.suppressedStaleEpochLog.Swap(0)
			s.logger.Warn("stale writer epoch fenced",
				"op", op.String(),
				"req_epoch", reqEpoch,
				"current_epoch", curEpoch,
				"suppressed", suppressed,
			)
			return
		}
	}
	s.suppressedStaleEpochLog.Add(1)
}

func (s *StoreActor) validateCmd(cmd *writeCmd) error {
	if isExternalWriteOp(cmd.op) && cmd.epoch != s.stat.WriterEpoch {
		s.metrics.incEpochFenced()
		s.logStaleEpochFenced(cmd.op, cmd.epoch, s.stat.WriterEpoch)
		return ErrStaleEpoch
	}
	switch cmd.op {
	case opMknod, opMkdir, opCreate, opSymlink:
		return s.validateCreateNodeCmd(cmd)
	case opCreateTmpfile:
		return s.validateCreateTmpfileCmd(cmd)
	case opLink:
		return s.validateLinkCmd(cmd)
	case opUnlink:
		return s.validateUnlinkCmd(cmd)
	case opRmdir:
		return s.validateRmdirCmd(cmd)
	case opRename:
		return s.validateRenameCmd(cmd)
	case opSetAttr:
		return s.validateSetAttrCmd(cmd)
	case opSetXattr:
		return s.validateSetXattrCmd(cmd)
	case opRemoveXattr:
		return s.validateRemoveXattrCmd(cmd)
	case opWrite:
		return s.validateWriteCmd(cmd)
	case opFallocate:
		return s.validateFallocateCmd(cmd)
	case opFsync:
		return s.validateFsyncCmd(cmd)
	case opFsyncDir:
		return s.validateFsyncDirCmd(cmd)
	case opPurgeOrphan:
		return s.validatePurgeOrphanCmd(cmd)
	case opCleanOrphans:
		return s.validateCleanOrphansCmd(cmd)
	case opAdvanceEpoch:
		return s.validateAdvanceEpochCmd(cmd)
	case opUpdateMaxBytes:
		return s.validateUpdateMaxBytesCmd(cmd)
	case opCompactSelect:
		return s.validateCompactSelectCmd(cmd)
	case opCompactRelocate:
		return s.validateCompactRelocateCmd(cmd)
	case opCheckpoint:
		return s.validateCheckpointCmd(cmd)
	default:
		return nil
	}
}

func (s *StoreActor) mutateCmd(cmd *writeCmd) {
	switch cmd.op {
	case opMknod, opMkdir, opCreate, opSymlink:
		s.mutateCreateNodeCmd(cmd)
	case opCreateTmpfile:
		s.mutateCreateTmpfileCmd(cmd)
	case opLink:
		s.mutateLinkCmd(cmd)
	case opUnlink:
		s.mutateUnlinkCmd(cmd)
	case opRmdir:
		s.mutateRmdirCmd(cmd)
	case opRename:
		s.mutateRenameCmd(cmd)
	case opSetAttr:
		s.mutateSetAttrCmd(cmd)
	case opSetXattr:
		s.mutateSetXattrCmd(cmd)
	case opRemoveXattr:
		s.mutateRemoveXattrCmd(cmd)
	case opWrite:
		s.mutateWriteCmd(cmd)
	case opFallocate:
		s.mutateFallocateCmd(cmd)
	case opFsync:
		s.mutateFsyncCmd(cmd)
	case opFsyncDir:
		s.mutateFsyncDirCmd(cmd)
	case opPurgeOrphan:
		s.mutatePurgeOrphanCmd(cmd)
	case opCleanOrphans:
		s.mutateCleanOrphansCmd(cmd)
	case opAdvanceEpoch:
		s.mutateAdvanceEpochCmd(cmd)
	case opUpdateMaxBytes:
		s.mutateUpdateMaxBytesCmd(cmd)
	case opCompactRelocate:
		s.mutateCompactRelocateCmd(cmd)
	case opCompactReap:
		s.mutateCompactReapCmd(cmd)
	case opCheckpoint:
		s.mutateCheckpointCmd(cmd)
	}
}

func (s *StoreActor) commitWithSyncBarrier(fsyncFileInodes, deadPacks []uint64) (packSyncDur, walSyncDur, deadRemoveDur time.Duration, reclaimed int, err error) {
	toReclaim := s.takePendingDeadPacks()
	applyStart := time.Now()
	if applyErr := s.db.ApplyNoSyncWait(s.batch, pebble.Sync); applyErr != nil {
		walSyncDur = time.Since(applyStart)
		s.metrics.observePhaseWALApplySync(walSyncDur)
		s.restorePendingDeadPacks(toReclaim)
		return 0, walSyncDur, 0, 0, applyErr
	}

	toReclaim = append(toReclaim, deadPacks...)
	var dataErr error
	if len(fsyncFileInodes) > 0 {
		packStart := time.Now()
		dataErr = s.syncFileInodes(fsyncFileInodes)
		packSyncDur = time.Since(packStart)
		s.metrics.observePhasePackSyncBarrier(packSyncDur)
	}

	walErr := s.batch.SyncWait()

	walSyncDur = time.Since(applyStart)
	s.metrics.observePhaseWALApplySync(walSyncDur)

	if walErr != nil {
		s.restorePendingDeadPacks(toReclaim)
		s.walDirty.Store(true)
	} else {
		if dataErr == nil {
			s.walDirty.Store(false)
		}
		if len(toReclaim) > 0 {
			remStart := time.Now()
			s.enqueueRemovePacks(toReclaim)
			deadRemoveDur = time.Since(remStart)
			reclaimed = len(toReclaim)
		}
	}
	return packSyncDur, walSyncDur, deadRemoveDur, reclaimed, errors.Join(dataErr, walErr)
}

func (s *StoreActor) syncBarrierWithoutBatch(fsyncFileInodes []uint64) (packSyncDur, walSyncDur, deadRemoveDur time.Duration, reclaimed int, err error) {
	toReclaim := s.takePendingDeadPacks()
	wasDirty := s.walDirty.Swap(false)
	needPackSync := len(fsyncFileInodes) > 0
	needWALSync := wasDirty || len(toReclaim) > 0

	var (
		dataErr error
		walErr  error
	)
	switch {
	case needPackSync && needWALSync:
		var wg sync.WaitGroup
		wg.Go(func() {
			packStart := time.Now()
			dataErr = s.syncFileInodes(fsyncFileInodes)
			packSyncDur = time.Since(packStart)
		})
		walStart := time.Now()
		walErr = s.db.LogData(nil, pebble.Sync)
		walSyncDur = time.Since(walStart)
		wg.Wait()
	case needPackSync:
		packStart := time.Now()
		dataErr = s.syncFileInodes(fsyncFileInodes)
		packSyncDur = time.Since(packStart)
	case needWALSync:
		walStart := time.Now()
		walErr = s.db.LogData(nil, pebble.Sync)
		walSyncDur = time.Since(walStart)
	}

	if needPackSync {
		s.metrics.observePhasePackSyncBarrier(packSyncDur)
	}
	if needWALSync {
		s.metrics.observePhaseWALApplySync(walSyncDur)
		if walErr != nil {
			s.restorePendingDeadPacks(toReclaim)
			s.walDirty.Store(true)
		} else if len(toReclaim) > 0 {
			remStart := time.Now()
			s.enqueueRemovePacks(toReclaim)
			deadRemoveDur = time.Since(remStart)
			reclaimed = len(toReclaim)
		}
	}
	return packSyncDur, walSyncDur, deadRemoveDur, reclaimed, errors.Join(dataErr, walErr)
}

func (s *StoreActor) takeDirtyAtime() map[uint64]int64 {
	s.atimeMu.Lock()
	defer s.atimeMu.Unlock()
	if len(s.dirtyAtime) == 0 {
		return nil
	}
	pending := s.dirtyAtime
	s.dirtyAtime = make(map[uint64]int64)
	return pending
}

func (s *StoreActor) flushDirtyAtimeToBatch(batch *pebble.Batch) bool {
	pending := s.takeDirtyAtime()
	if len(pending) == 0 {
		return false
	}
	wrote := false
	for ino, ns := range pending {
		inode, err := s.readInode(batch, ino)
		if err != nil {
			continue
		}
		if ns <= inode.Atime {
			continue
		}
		inode.Atime = ns
		if err := BatchSetInodeDeferred(batch, inode); err != nil {
			continue
		}
		wrote = true
	}
	return wrote
}

func (s *StoreActor) flushOnClose() error {
	var applyErr error
	wroteAtime := s.flushDirtyAtimeToBatch(s.batch)
	deadPacks, wrotePacks := s.flushDirtyPackStatsToBatch(s.batch)
	if wroteAtime || wrotePacks {
		applyErr = s.db.Apply(s.batch, pebble.NoSync)
		s.batch.Reset()
		if applyErr == nil {
			s.addPendingDeadPacks(deadPacks)
			s.walDirty.Store(true)
			s.publishAtomicStats()
		}
	}
	if applyErr != nil {
		return applyErr
	}
	return s.syncWALAndReclaimDeadPacks()
}

func (s *StoreActor) Close() error {
	return s.closeInternal(false)
}

func (s *StoreActor) CloseDiscard() error {
	return s.closeInternal(true)
}

func (s *StoreActor) closeInternal(discard bool) error {
	s.closeOnce.Do(func() {

		s.discardOnClose.Store(discard)
		s.closing.Store(true)

		s.mb.mu.Lock()
		s.mb.closed = true
		select {
		case s.mb.notify <- struct{}{}:
		default:
		}
		s.mb.mu.Unlock()

		<-s.loopDone

		s.lifecycleMu.Lock()
		if s.batch != nil {
			_ = s.batch.Close()
			s.batch = nil
		}
		dbErr := s.db.Close()
		var fsErr error
		if s.fsCloser != nil {
			fsErr = s.fsCloser.Close()
			s.fsCloser = nil
		}
		if s.ownedCache != nil {
			s.ownedCache.Unref()
			s.ownedCache = nil
		}
		var blobErr error
		if s.blobs != nil {
			blobErr = s.blobs.close()
		}
		s.lifecycleMu.Unlock()

		s.closeErr = errors.Join(s.closeErr, dbErr, fsErr, blobErr)
	})
	return s.closeErr
}
