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

// Pebble 初始化与批次重置阈值常量。
const (
	DefaultMemTableSize    = 16 << 20 // 16 MiB
	DefaultCacheSize       = 64 << 20 // 64 MiB
	DefaultMaxOpenFiles    = 128
	DefaultWALBytesPerSync = 4 << 20 // 4 MiB
	DefaultBloomBitsPerKey = 10
	PebbleLevelCount       = 7

	BatchResetMaxReprBytes = 1 << 20 // 1 MiB
	BatchResetMaxCount     = 8192

	defaultSlowTickThreshold    = 20 * time.Millisecond
	staleEpochLogRateLimitNanos = int64(time.Second)
)

// pebbleReader 抽象 *pebble.DB 与 *pebble.Batch 的共享只读接口。
type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

const (
	// DefaultWriterLeaseTTL 定义单写者租约的默认有效时长（30 秒）。
	DefaultWriterLeaseTTL = 30 * time.Second
)

var (
	minUnixNanoTime = time.Unix(0, math.MinInt64).UTC()
	maxUnixNanoTime = time.Unix(0, math.MaxInt64).UTC()
)

type writerLeaseSnapshot struct {
	epoch     uint64
	expiresAt int64
}

// opKind 标识投递到 StoreActor 双缓冲队列的操作类型。
type opKind uint8

const (
	opNoop opKind = iota
	opMknod
	opMkdir
	opCreate
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
	opAcquireWriterLease
	opRenewWriterLease
	opReleaseWriterLease
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
	case opAcquireWriterLease:
		return "acquire_writer_lease"
	case opRenewWriterLease:
		return "renew_writer_lease"
	case opReleaseWriterLease:
		return "release_writer_lease"
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

// writeCmd 是投递至 StoreActor 双缓冲 MPSC 队列的命令载体（由 sync.Pool 复用）。
type writeCmd struct {
	op   opKind
	req  any
	err  error
	done chan struct{} // 容量为 1 的完成信号

	enqueueAt   time.Time
	dequeueAt   time.Time
	spanCtx     trace.SpanContext
	epoch       uint64
	forceStale  bool
	syncBarrier bool
	syncIno     uint64
	stagedBlobs []BlobRef
}

// Options 配置 StoreActor 实例打开参数。
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

	// 包内单测注入钩子（生产路径留零值）。
	pebbleFS                  vfs.FS
	diskSlowThreshold         time.Duration
	beforeTickHook            func()
	beforeCompactRelocateHook func()
	syncFileHook              func(packID uint64)
	removePackHook            func(packID uint64)
}

// NewPebbleOptions 构建*pebble.Options：
//   - 自定义 Comparer.Split（entry:/xattr: -> 15，data: -> 14，其余 -> len(key)）；
//   - 全部 7 层启用 bloom.FilterPolicy(10)；
//   - MemTableSize=16MiB, MaxOpenFiles=128, WALBytesPerSync=4MiB, FormatMajorVersion=pebble.FormatNewest。
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

// StoreActor 是单盘（或只读快照）存储引擎实例。
//
//nolint:revive // exported: StoreActor 为存储引擎锁定的核心实体名称。
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

	// 1. 双缓冲 MPSC 写命令队列（RPC 协程向 active 追加，Actor 协程交换到 draining 批量处理）
	mb struct {
		mu       sync.Mutex
		active   []*writeCmd
		draining []*writeCmd
		notify   chan struct{}
		closed   bool
	}
	cmdPool sync.Pool

	// 2. Actor 写协程私有状态（仅在单协程 Loop 内无锁读写）
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

	// 3. 并发读路径与监控快照无锁镜像（每次 Batch 提交后刷新）
	atomicLogicalBytes      atomic.Uint64
	atomicInodes            atomic.Uint64
	leaseSnap               atomic.Pointer[writerLeaseSnapshot]
	atomicPackPhysicalBytes atomic.Uint64
	atomicPackLiveBytes     atomic.Uint64
	atomicLowWatermarkPacks atomic.Int64
	atomicPendingDeadPacks  atomic.Int64
	pebbleStallCount        atomic.Int64
	pendingLeaseUpdates     atomic.Int32

	lastStaleEpochLogNano   atomic.Int64
	suppressedStaleEpochLog atomic.Uint64
	slowTickThreshold       time.Duration

	// 4. 延迟 Atime 内存脏表（读路径登记/叠加，写循环或关闭时合并刷盘）
	atimeMu    sync.Mutex
	dirtyAtime map[uint64]int64 // ino -> unixNano

	// 5. 生命周期守卫（保护 s.db 不在在途直读或写阶段 1 预读期间被 Close 关闭）
	lifecycleMu    sync.RWMutex
	closing        atomic.Bool
	discardOnClose atomic.Bool
	loopDone       chan struct{}
	closeOnce      sync.Once
	closeErr       error

	// 6. Data Layer 协同锁与钩子
	inodeLocks                [4096]sync.Mutex
	blobs                     *blobStore
	beforeTickHook            func()
	beforeCompactRelocateHook func()
	inFlightMu                sync.Mutex
	inFlightPacks             map[uint64]int
	deadPacksMu               sync.Mutex
	pendingDeadPacks          []uint64
}

// Open 打开或初始化一个 StoreActor 实例。
// 空库首次可写打开时通过单次 Batch 初始化根目录 Inode(1)（S_IFDIR | 0755, Nlink = 2）与 "sys:stat"（Inodes = 1, LogicalBytes = 0）。
//
//nolint:gocritic // hugeParam: Open 公开构造契约按值接收 Options。
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
		s.publishLeaseSnapshot()
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
		stat.WriterExpiresAt = 0
		resetBatch := s.db.NewBatch()
		BatchSetStatDeferred(resetBatch, stat)
		applyErr := s.db.Apply(resetBatch, pebble.Sync)
		closeBatchErr := resetBatch.Close()
		if applyErr != nil || closeBatchErr != nil {
			return errors.Join(applyErr, closeBatchErr)
		}
	} else if stat.MaxBytes > 0 {
		s.maxBytes.Store(stat.MaxBytes)
	} else if maxB := s.maxBytes.Load(); maxB > 0 {
		stat.MaxBytes = maxB
	}
	s.stat = stat
	s.atomicLogicalBytes.Store(stat.LogicalBytes)
	s.atomicInodes.Store(stat.Inodes)
	s.publishLeaseSnapshot()

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

	// "inode;" 紧邻 "inode:" 之后（':' = 0x3a, ';' = 0x3b），SeekLT 直接定位最大 InodeKey。
	if iter.SeekLT([]byte("inode;")) && bytes.HasPrefix(iter.Key(), inodePrefixBytes) {
		ino, err := DecodeInodeKey(iter.Key())
		if err != nil {
			return 0, err
		}
		return ino, iter.Error()
	}
	return RootIno, iter.Error()
}

// enter 是所有直读路径与写阶段 1 预读的统一生命周期读守卫。
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

// Dir 返回当前分支工作区目录路径。
func (s *StoreActor) Dir() string { return s.dir }

// ReadOnly 报告实例是否为只读模式。
func (s *StoreActor) ReadOnly() bool { return s.readOnly }

// SetMaxBytes 动态更新磁盘逻辑容量配额上限。
func (s *StoreActor) SetMaxBytes(maxBytes uint64) { s.maxBytes.Store(maxBytes) }

// MaxBytes 返回当前逻辑容量配额上限。
func (s *StoreActor) MaxBytes() uint64 { return s.maxBytes.Load() }

// WriterEpoch 返回当前已持久化生效的写世代号。
func (s *StoreActor) publishLeaseSnapshot() {
	s.leaseSnap.Store(&writerLeaseSnapshot{
		epoch:     s.stat.WriterEpoch,
		expiresAt: s.stat.WriterExpiresAt,
	})
}

func (s *StoreActor) currentUTCUnixNano() (int64, error) {
	nowUTC := s.now().UTC()
	if nowUTC.Before(minUnixNanoTime) || nowUTC.After(maxUnixNanoTime) {
		return 0, ErrInvalid
	}
	return nowUTC.UnixNano(), nil
}

func (s *StoreActor) calcLeaseExpiresAt(ttl time.Duration) (nowNano, expiresAt int64, err error) {
	if ttl <= 0 {
		return 0, 0, ErrInvalid
	}
	nowUTC := s.now().UTC()
	if nowUTC.Before(minUnixNanoTime) || nowUTC.After(maxUnixNanoTime) {
		return 0, 0, ErrInvalid
	}
	nowNano = nowUTC.UnixNano()
	exp := nowUTC.Add(ttl)
	if !exp.After(nowUTC) || exp.After(maxUnixNanoTime) {
		return 0, 0, ErrInvalid
	}
	expiresAt = exp.UnixNano()
	ttlNano := ttl.Nanoseconds()
	if expiresAt <= 0 || expiresAt <= nowNano || nowNano > math.MaxInt64-ttlNano {
		return 0, 0, ErrInvalid
	}
	return nowNano, expiresAt, nil
}

func isWriterLeaseActive(st BranchStat, nowNano int64) bool {
	return st.WriterEpoch > 0 && st.WriterExpiresAt > 0 && nowNano < st.WriterExpiresAt
}

// WriterEpoch 返回当前写世代号的原子只读快照。
func (s *StoreActor) WriterEpoch() uint64 {
	if snap := s.leaseSnap.Load(); snap != nil {
		return snap.epoch
	}
	return 0
}

// WriterExpiresAt 返回当前写租约过期时间（UTC UnixNano）的原子只读快照。
func (s *StoreActor) WriterExpiresAt() int64 {
	if snap := s.leaseSnap.Load(); snap != nil {
		return snap.expiresAt
	}
	return 0
}

// HasActiveWriterLease 只读检查磁盘当前是否持有未过期的写租约。
func (s *StoreActor) HasActiveWriterLease(_ context.Context) bool {
	if s == nil {
		return false
	}
	snap := s.leaseSnap.Load()
	if snap == nil || snap.epoch == 0 || snap.expiresAt <= 0 {
		return false
	}
	nowNano, err := s.currentUTCUnixNano()
	if err != nil {
		return false
	}
	return nowNano < snap.expiresAt
}

type acquireWriterLeaseOp struct {
	ttl       time.Duration
	force     bool
	plans     []inodeReclaimPlan
	root      Inode
	oldEpoch  uint64
	epoch     uint64
	expiresAt int64
	rootAttr  Attribute
}

type renewWriterLeaseOp struct {
	epoch     uint64
	ttl       time.Duration
	expiresAt int64
}

type releaseWriterLeaseOp struct {
	epoch uint64
	noop  bool
}

// AdvanceEpoch 委托 AcquireWriterLease(ctx, DefaultWriterLeaseTTL, true) 获取一个默认时长的抢占式写租约，
// 并保持历史调用方的返回契约不变。
func (s *StoreActor) AdvanceEpoch(ctx context.Context) (uint64, Attribute, error) {
	return s.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, true)
}

// AcquireWriterLease 通过 Actor 队列获取或抢占单写者租约并推进 WriterEpoch，
// 清扫所有残留 Orphan Inode，以 pebble.Sync 持久化 49 字节 BranchStat 至 "sys:stat"。
func (s *StoreActor) AcquireWriterLease(ctx context.Context, ttl time.Duration, force bool) (uint64, Attribute, error) {
	if s.readOnly {
		return 0, Attribute{}, ErrReadOnly
	}
	if ttl <= 0 {
		return 0, Attribute{}, ErrInvalid
	}
	s.pendingLeaseUpdates.Add(1)
	defer s.pendingLeaseUpdates.Add(-1)

	op := &acquireWriterLeaseOp{
		ttl:   ttl,
		force: force,
	}
	cmd := s.acquireCmd()
	cmd.op = opAcquireWriterLease
	cmd.req = op
	if err := s.submit(ctx, cmd); err != nil {
		return 0, Attribute{}, err
	}
	if s.logger != nil {
		s.logger.Info("writer lease acquired",
			"old_epoch", op.oldEpoch,
			"new_epoch", op.epoch,
			"expires_at", op.expiresAt,
			"force", force,
			"reclaimed_orphans", len(op.plans),
		)
	}
	return op.epoch, op.rootAttr, nil
}

func (s *StoreActor) validateAcquireWriterLeaseCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*acquireWriterLeaseOp)
	nowNano, expNano, err := s.calcLeaseExpiresAt(op.ttl)
	if err != nil {
		return err
	}
	if !op.force && isWriterLeaseActive(s.stat, nowNano) {
		return ErrWriterLeaseActive
	}
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
	if len(plans) > 0 && s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	op.root = root
	op.plans = plans
	op.expiresAt = expNano
	return nil
}

func (s *StoreActor) mutateAcquireWriterLeaseCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*acquireWriterLeaseOp)
	op.oldEpoch = s.stat.WriterEpoch
	s.stat.WriterEpoch++
	s.stat.WriterExpiresAt = op.expiresAt
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

// RenewWriterLease 在写世代仍活跃且未过期时续延当前 WriterExpiresAt，不递增 WriterEpoch 或 NextWriteSeq。
func (s *StoreActor) RenewWriterLease(ctx context.Context, epoch uint64, ttl time.Duration) error {
	if s.readOnly {
		return ErrReadOnly
	}
	if ttl <= 0 {
		return ErrInvalid
	}
	s.pendingLeaseUpdates.Add(1)
	defer s.pendingLeaseUpdates.Add(-1)

	op := &renewWriterLeaseOp{
		epoch: epoch,
		ttl:   ttl,
	}
	cmd := s.acquireCmd()
	cmd.op = opRenewWriterLease
	cmd.req = op
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateRenewWriterLeaseCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*renewWriterLeaseOp)
	nowNano, expNano, err := s.calcLeaseExpiresAt(op.ttl)
	if err != nil {
		return err
	}
	if op.epoch == 0 || op.epoch != s.stat.WriterEpoch || !isWriterLeaseActive(s.stat, nowNano) {
		return ErrStaleEpoch
	}
	op.expiresAt = expNano
	return nil
}

func (s *StoreActor) mutateRenewWriterLeaseCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*renewWriterLeaseOp)
	s.stat.WriterExpiresAt = op.expiresAt
	cmd.syncBarrier = true
}

// ReleaseWriterLease 幂等清零匹配写世代的 WriterExpiresAt，保持 WriterEpoch 单调不回退。
func (s *StoreActor) ReleaseWriterLease(ctx context.Context, epoch uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	s.pendingLeaseUpdates.Add(1)
	defer s.pendingLeaseUpdates.Add(-1)

	op := &releaseWriterLeaseOp{epoch: epoch}
	cmd := s.acquireCmd()
	cmd.op = opReleaseWriterLease
	cmd.req = op
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateReleaseWriterLeaseCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*releaseWriterLeaseOp)
	if op.epoch == 0 || op.epoch != s.stat.WriterEpoch || s.stat.WriterExpiresAt == 0 {
		op.noop = true
	}
	return nil
}

func (s *StoreActor) mutateReleaseWriterLeaseCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*releaseWriterLeaseOp)
	if op.noop {
		return
	}
	s.stat.WriterExpiresAt = 0
	cmd.syncBarrier = true
}

type updateMaxBytesOp struct {
	maxBytes uint64
}

// UpdateMaxBytes 通过 Actor 队列在线扩容或安全缩容单盘逻辑容量配额上限，并以 pebble.Sync 持久化至 "sys:stat"。
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

// SetMaxInodes 动态更新节点数量配额上限。
func (s *StoreActor) SetMaxInodes(maxInodes uint64) { s.maxInodes.Store(maxInodes) }

// MaxInodes 返回当前节点数量配额上限。
func (s *StoreActor) MaxInodes() uint64 { return s.maxInodes.Load() }

// TickCount 返回 Actor 后台已执行的写批次轮数（用于并发合并测试观测）。
func (s *StoreActor) TickCount() uint64 { return s.tickCount.Load() }

// BatchReallocCount 返回 IndexedBatch 触发防膨胀重建的次数（用于测试观测）。
func (s *StoreActor) BatchReallocCount() uint64 { return s.batchResetCnt.Load() }

// WALDirty 报告是否存在尚未通过 pebble.Sync 刷盘的 NoSync WAL 写入（用于测试观测）。
func (s *StoreActor) WALDirty() bool { return s.walDirty.Load() }

// StatFS 直接从内存原子镜像读取逻辑已用字节数与节点数（0 锁、0 I/O）。
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

// GetAttr 在 lifecycleMu.RLock() 守卫下直接调用 db.Get(InodeKey) 原地解码并叠加 dirtyAtime（严禁创建 Snapshot）。
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

// MarkAtimeDirty 非阻塞登记节点最新访问时间至内存 dirtyAtime 表。
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

// FlushWAL 在存在未落盘 NoSync 写入时执行一次同步 WAL 刷盘（db.LogData(nil, pebble.Sync)），
// 并在 WAL 落盘确认后物理清理挂起的全死封口 .pack 文件。
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
	cmd.forceStale = false
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
	if s.pendingLeaseUpdates.Load() != 0 {
		return false
	}
	snap := s.leaseSnap.Load()
	if snap == nil {
		return false
	}
	if epoch != snap.epoch {
		return true
	}
	if snap.epoch == 0 {
		return false
	}
	if snap.expiresAt <= 0 {
		return true
	}
	nowNano, err := s.currentUTCUnixNano()
	if err != nil {
		return false
	}
	return nowNano >= snap.expiresAt
}

func (s *StoreActor) submitStaleEpoch(ctx context.Context, op opKind, epoch uint64) error {
	cmd := s.acquireCmd()
	cmd.op = op
	cmd.epoch = epoch
	cmd.forceStale = true
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

// prepareIno 在 Phase A 检查本地 Inode 号段余量；若已耗尽则按 4096 步长向分配器预留新号段。
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

// consumeIno 在 Phase B 消费当前已预留的 Inode 编号并推进游标。
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

// runLoop 是 StoreActor 单协程写循环。
func (s *StoreActor) runLoop() {
	defer close(s.loopDone)
	for {
		<-s.mb.notify
		for {
			// Step 1：单锁指针交换排空队列（Drain）
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

// processTick 严格执行 Group Commit 的 Step 2 ~ Step 4。
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

	tickStartStat := s.stat

	// Step 2：逐条两阶段串行演进（Phase A Validate -> Phase B Mutate）
	for _, cmd := range cmds {
		// Phase A — 纯内存只读预校验（绝不触碰 s.batch 写接口）
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

		// Phase B — 内存记账与原地写入（必定成功）
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
		if s.batch.Count() > prevCount || s.stat != prevStat || len(s.dirtyPackStats) > 0 || cmd.op == opUpdateMaxBytes || cmd.op == opAcquireWriterLease || cmd.op == opRenewWriterLease {
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

	// Step 3：批次收敛与合并提交（Seal & Commit）
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
		// 顺带将内存 dirtyAtime 与 dirtyPackStats 合入当前批次
		s.flushDirtyAtimeToBatch(s.batch)
		var wrotePacks bool
		deadPacks, wrotePacks = s.flushDirtyPackStatsToBatch(s.batch)
		if wrotePacks {
			mutated = true
		}
	}
	if mutated {
		// 批次末尾仅写 1 次 "sys:stat"
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
		} else {
			s.stat = tickStartStat
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
		if commitErr != nil {
			s.stat = tickStartStat
		}
	}

	// Step 4：防膨胀重置与执行切面固化（仍在 tickSpan 生命周期内）、唤醒调用方（Reset & Wake）
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
	case opMknod, opMkdir, opCreate, opSymlink, opLink,
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
	if isExternalWriteOp(cmd.op) {
		if cmd.forceStale || cmd.req == nil {
			s.metrics.incEpochFenced()
			s.logStaleEpochFenced(cmd.op, cmd.epoch, s.stat.WriterEpoch)
			return ErrStaleEpoch
		}
		if s.stat.WriterEpoch == 0 {
			if cmd.epoch != 0 {
				s.metrics.incEpochFenced()
				s.logStaleEpochFenced(cmd.op, cmd.epoch, s.stat.WriterEpoch)
				return ErrStaleEpoch
			}
		} else {
			nowNano, err := s.currentUTCUnixNano()
			if err != nil {
				return err
			}
			if cmd.epoch != s.stat.WriterEpoch || !isWriterLeaseActive(s.stat, nowNano) {
				s.metrics.incEpochFenced()
				s.logStaleEpochFenced(cmd.op, cmd.epoch, s.stat.WriterEpoch)
				return ErrStaleEpoch
			}
		}
	}
	switch cmd.op {
	case opMknod, opMkdir, opCreate, opSymlink:
		return s.validateCreateNodeCmd(cmd)
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
	case opAcquireWriterLease:
		return s.validateAcquireWriterLeaseCmd(cmd)
	case opRenewWriterLease:
		return s.validateRenewWriterLeaseCmd(cmd)
	case opReleaseWriterLease:
		return s.validateReleaseWriterLeaseCmd(cmd)
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
	case opAcquireWriterLease:
		s.mutateAcquireWriterLeaseCmd(cmd)
	case opRenewWriterLease:
		s.mutateRenewWriterLeaseCmd(cmd)
	case opReleaseWriterLease:
		s.mutateReleaseWriterLeaseCmd(cmd)
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
	var allReclaim []uint64
	if len(toReclaim)+len(deadPacks) > 0 {
		allReclaim = make([]uint64, 0, len(toReclaim)+len(deadPacks))
		allReclaim = append(allReclaim, toReclaim...)
		allReclaim = append(allReclaim, deadPacks...)
	}
	needFullBlobSync := s.blobs != nil && (len(allReclaim) > 0 || s.blobs.hasDirty())
	var dataErr error
	if len(fsyncFileInodes) > 0 || needFullBlobSync {
		packStart := time.Now()
		if len(fsyncFileInodes) > 0 {
			dataErr = s.syncFileInodes(fsyncFileInodes)
		}
		if needFullBlobSync && s.blobs.hasDirty() {
			dataErr = errors.Join(dataErr, s.blobs.sync())
		}
		packSyncDur = time.Since(packStart)
		s.metrics.observePhasePackSyncBarrier(packSyncDur)
		if dataErr != nil {
			s.restorePendingDeadPacks(allReclaim)
			return packSyncDur, 0, 0, 0, dataErr
		}
	}

	applyStart := time.Now()
	if applyErr := s.db.ApplyNoSyncWait(s.batch, pebble.Sync); applyErr != nil {
		walSyncDur = time.Since(applyStart)
		s.metrics.observePhaseWALApplySync(walSyncDur)
		s.restorePendingDeadPacks(allReclaim)
		return packSyncDur, walSyncDur, 0, 0, applyErr
	}

	walErr := s.batch.SyncWait()
	// 记录从 ApplyNoSyncWait 提交至 SyncWait 返回的完整 WAL 同步墙钟耗时。
	walSyncDur = time.Since(applyStart)
	s.metrics.observePhaseWALApplySync(walSyncDur)

	if walErr != nil {
		s.restorePendingDeadPacks(allReclaim)
		s.walDirty.Store(true)
	} else {
		s.walDirty.Store(false)
		if len(allReclaim) > 0 {
			remStart := time.Now()
			s.enqueueRemovePacks(allReclaim)
			deadRemoveDur = time.Since(remStart)
			reclaimed = len(allReclaim)
		}
	}
	return packSyncDur, walSyncDur, deadRemoveDur, reclaimed, walErr
}

func (s *StoreActor) syncBarrierWithoutBatch(fsyncFileInodes []uint64) (packSyncDur, walSyncDur, deadRemoveDur time.Duration, reclaimed int, err error) {
	toReclaim := s.takePendingDeadPacks()
	wasDirty := s.walDirty.Load()
	needFullBlobSync := s.blobs != nil && (len(toReclaim) > 0 || s.blobs.hasDirty())
	needPackSync := len(fsyncFileInodes) > 0 || needFullBlobSync
	needWALSync := wasDirty || len(toReclaim) > 0

	var dataErr error
	if needPackSync {
		packStart := time.Now()
		if len(fsyncFileInodes) > 0 {
			dataErr = s.syncFileInodes(fsyncFileInodes)
		}
		if needFullBlobSync && s.blobs.hasDirty() {
			dataErr = errors.Join(dataErr, s.blobs.sync())
		}
		packSyncDur = time.Since(packStart)
		s.metrics.observePhasePackSyncBarrier(packSyncDur)
		if dataErr != nil {
			s.restorePendingDeadPacks(toReclaim)
			return packSyncDur, 0, 0, 0, dataErr
		}
	}

	var walErr error
	if needWALSync {
		walStart := time.Now()
		walErr = s.db.LogData(nil, pebble.Sync)
		walSyncDur = time.Since(walStart)
		s.metrics.observePhaseWALApplySync(walSyncDur)
		if walErr != nil {
			s.restorePendingDeadPacks(toReclaim)
			return packSyncDur, walSyncDur, 0, 0, walErr
		}
		s.walDirty.Store(false)
		if len(toReclaim) > 0 {
			remStart := time.Now()
			s.enqueueRemovePacks(toReclaim)
			deadRemoveDur = time.Since(remStart)
			reclaimed = len(toReclaim)
		}
	}
	return packSyncDur, walSyncDur, deadRemoveDur, reclaimed, nil
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

// Close 执行三步无死锁优雅关闭：
//   - Step 1：置 closing=true 拒新，封闭 mb 队列并唤醒 Actor 协程；
//   - Step 2：等待 <-loopDone 排空已入队写批次，刷入残留 dirtyAtime 与 WAL Sync；
//   - Step 3：获取 lifecycleMu.Lock() 排空在途直读后安全关闭底层 *pebble.DB。
func (s *StoreActor) Close() error {
	return s.closeInternal(false)
}

// CloseDiscard 执行销毁型三步关闭（用于 Rollback / DeleteBranch / DeleteDisk）：
// 排空已入队批次后直接跳过尾部 dirtyAtime 与 WAL Sync 刷盘，省去无意义磁盘 fsync。
func (s *StoreActor) CloseDiscard() error {
	return s.closeInternal(true)
}

func (s *StoreActor) closeInternal(discard bool) error {
	s.closeOnce.Do(func() {
		// Step 1 — 拉闸拒新并封闭写队列
		s.discardOnClose.Store(discard)
		s.closing.Store(true)

		s.mb.mu.Lock()
		s.mb.closed = true
		select {
		case s.mb.notify <- struct{}{}:
		default:
		}
		s.mb.mu.Unlock()

		// Step 2 — 排空已入队写批次并等待 Actor 协程退出
		<-s.loopDone

		// Step 3 — 排空在途直读并安全关闭 Pebble、健康检测包装与常驻 .pack 文件句柄
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
