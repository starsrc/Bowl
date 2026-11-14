// Package control 实现单盘 StoreActor 控制面管理、MountDisk 世代号颁发与实例池路由。
package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/handoff"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

const controlInstrumentationName = "github.com/starsrc/bowl/services/storage/internal/control"

// 控制面常量定义。
const (
	RootIno                       uint64  = store.RootIno
	MaxDiskNameBytes                      = 255
	DefaultListDisksPageSize      uint32  = 50
	MaxListDisksPageSize          uint32  = 1000
	DefaultEntryTimeoutSeconds    float64 = 1.0
	DefaultAttrTimeoutSeconds     float64 = 1.0
	DefaultDirEntryTimeoutSeconds float64 = 1.0
	DefaultSharedCacheSize                = store.DefaultCacheSize // 64 MiB
	DefaultMountLeaseTTL                  = 30 * time.Second

	segmentReserveTimeout = 5 * time.Second
)

// 领域哨兵错误（与 internal/store 哨兵错误对齐，支持双向 errors.Is 判定）。
var (
	ErrInvalidArgument = store.ErrInvalid
	ErrNameTooLong     = store.ErrNameTooLong
	ErrNotFound        = store.ErrNotFound
	ErrInvalidShrink   = store.ErrQuotaShrinkBelowUsed
	ErrStaleEpoch      = store.ErrStaleEpoch
	ErrClosed          = store.ErrClosed
	ErrAlreadyExists   = store.ErrExist
	ErrNotDir          = store.ErrNotDir
	ErrIsDir           = store.ErrIsDir
	ErrNotEmpty        = store.ErrNotEmpty
	ErrPerm            = store.ErrPerm
	ErrNoData          = store.ErrNoData
	ErrNoSpace         = store.ErrNoSpace
	ErrNotSupported    = store.ErrNotSupported
	ErrRange           = store.ErrRange
	ErrTooManyLinks    = store.ErrTooManyLinks
	ErrFileTooLarge    = store.ErrFileTooLarge
	ErrIO              = store.ErrIO
	ErrCrossLineage    = db.ErrCrossLineage
	ErrStaleBaseCommit = db.ErrStaleBaseCommit
	ErrAmbiguousLCA    = errors.New("ambiguous lowest common ancestor (criss-cross merge)")
	ErrNoBaseCommit    = errors.New("disk has no base commit in lineage")
	ErrConcurrentMerge = errors.New("concurrent write or epoch change on target disk during merge")
	ErrDiskMounted     = errors.New("disk is currently mounted")
)

// AtimeMode 复用 store.AtimeMode。
type AtimeMode = store.AtimeMode

// 访问时间更新策略常量。
const (
	AtimeModeNoAtime     = store.AtimeModeNoAtime
	AtimeModeRelAtime    = store.AtimeModeRelAtime
	AtimeModeStrictAtime = store.AtimeModeStrictAtime
)

// Disk 描述单盘控制面领域实体。
type Disk struct {
	DiskID       string
	ParentDiskID string
	Name         string
	RootIno      uint64
	MaxBytes     uint64
	UsedBytes    uint64
	UsedInodes   uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CreateDiskParams 描述创建虚拟磁盘入参。
type CreateDiskParams struct {
	Name     string
	MaxBytes uint64
}

// ForkDiskParams 描述基于已有虚拟磁盘零拷贝派生新盘的入参。
type ForkDiskParams struct {
	SourceDiskID string
	Name         string
	MaxBytes     *uint64
}

// ListDisksParams 描述分页查询虚拟磁盘列表入参。
type ListDisksParams struct {
	PageSize  uint32
	PageToken string
}

// ListDisksResult 描述分页查询虚拟磁盘列表返回结果。
type ListDisksResult struct {
	Disks         []*Disk
	NextPageToken string
}

// UpdateDiskParams 描述更新虚拟磁盘入参。
type UpdateDiskParams struct {
	DiskID   string
	Name     *string
	MaxBytes *uint64
}

// MountDiskParams 描述挂载虚拟磁盘握手入参。
type MountDiskParams struct {
	DiskID   string
	ReadOnly bool
}

// MountDiskResult 描述挂载虚拟磁盘握手返回结果。
type MountDiskResult struct {
	Epoch               uint64
	RootAttr            store.Attribute
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
	MountID             string
	LeaseTTLSeconds     uint32
}

// RenewMountParams 描述在线挂载软租约续期入参。
type RenewMountParams struct {
	DiskID   string
	MountID  string
	ReadOnly bool
	Epoch    uint64
}

// RenewMountResult 描述在线挂载软租约续期返回结果。
type RenewMountResult struct {
	LeaseTTLSeconds uint32
}

// UnmountDiskParams 描述释放挂载软租约入参。
type UnmountDiskParams struct {
	DiskID  string
	MountID string
}

// Manager 定义单盘控制面与 StoreActor 实例池管理接口。
type Manager interface {
	CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error)
	ForkDisk(ctx context.Context, params ForkDiskParams) (*Disk, error)
	GetDisk(ctx context.Context, diskID string) (*Disk, error)
	ListDisks(ctx context.Context, params ListDisksParams) (*ListDisksResult, error)
	UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error)
	DeleteDisk(ctx context.Context, diskID string) error
	MountDisk(ctx context.Context, params MountDiskParams) (*MountDiskResult, error)
	RenewMount(ctx context.Context, params RenewMountParams) (*RenewMountResult, error)
	UnmountDisk(ctx context.Context, params UnmountDiskParams) error
	DiffDisk(ctx context.Context, params *DiffDiskParams) (*DiffDiskResult, error)
	ReadCommitNode(ctx context.Context, params *ReadCommitNodeParams) (*store.ReadNodeResult, error)
	MergeDisk(ctx context.Context, params *MergeDiskParams) (*MergeDiskResult, error)
	Actor(ctx context.Context, diskID string) (*store.StoreActor, error)
	Collector() prometheus.Collector

	EntryTimeoutSeconds() float64
	AttrTimeoutSeconds() float64
	DirEntryTimeoutSeconds() float64
	EntryTimeoutForMode(mode uint32) float64
	OpenKeepCache() bool
	OpenDirCache() bool
	AtimeMode() AtimeMode
	StorageRoot() string
	Close() error
}

type diskLockEntry struct {
	mu   sync.Mutex
	refs int
}

type manager struct {
	store                                      db.Store
	storageRoot                                string
	entryTimeout, attrTimeout, dirEntryTimeout float64
	openKeepCache                              bool
	openDirCache                               bool
	atimeMode                                  AtimeMode
	maxPackSize                                uint32

	baseLogger   *slog.Logger
	logger       *slog.Logger
	storeMetrics *store.Metrics
	collector    *storageEngineCollector
	removeAll    func(string) error

	cache *pebble.Cache

	diskLocksMu sync.Mutex
	diskLocks   map[string]*diskLockEntry

	lineageCommitMu         sync.RWMutex
	beforePhase2CutoverHook func()

	poolMu sync.RWMutex
	closed bool
	actors map[string]*store.StoreActor

	now           func() time.Time
	mountLeaseTTL time.Duration
	mountsMu      sync.Mutex
	activeMounts  map[string]map[string]mountLease

	// 交接（handoff）：远端为 nil 时整套能力关闭；handoffReleasing 受 mountsMu 保护。
	handoffRemote    handoff.Remote
	nodeID           string
	handoffReleasing map[string]bool
	// handoffDirty 记录自上次后台同步以来有过可写租约活动的盘，受 mountsMu 保护。
	handoffDirty        map[string]bool
	handoffSyncInterval time.Duration
	handoffStop         chan struct{}
	handoffDone         chan struct{}
	handoffStopOnce     sync.Once
	handoffGCInterval   time.Duration
	handoffGC           handoff.GCOptions
	commitHydrateMu     sync.Mutex
}

// Option 配置单盘控制面管理器。
type Option func(*manager)

// WithStorageRoot 配置磁盘存储根目录。
func WithStorageRoot(root string) Option {
	return func(m *manager) {
		if root != "" {
			m.storageRoot = root
		}
	}
}

// WithEntryTimeout 配置非目录条目缓存时效（秒）。
func WithEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.entryTimeout = d
		}
	}
}

// WithAttrTimeout 配置节点属性缓存时效（秒）。
func WithAttrTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.attrTimeout = d
		}
	}
}

// WithDirEntryTimeout 配置目录条目缓存时效（秒）。
func WithDirEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.dirEntryTimeout = d
		}
	}
}

// WithOpenKeepCache 配置打开文件时是否下发 FOPEN_KEEP_CACHE。
func WithOpenKeepCache(enable bool) Option {
	return func(m *manager) { m.openKeepCache = enable }
}

// WithOpenDirCache 配置打开目录时是否保留内核目录缓存。
func WithOpenDirCache(enable bool) Option {
	return func(m *manager) { m.openDirCache = enable }
}

// WithAtimeMode 配置访问时间更新策略。
func WithAtimeMode(mode AtimeMode) Option {
	return func(m *manager) { m.atimeMode = mode }
}

// WithLogger 配置控制面与下属 StoreActor 共享的结构化日志器（nil 时保持默认 slog.Default()）。
func WithLogger(logger *slog.Logger) Option {
	return func(m *manager) {
		if logger != nil {
			m.baseLogger = logger
			m.logger = logger.With("component", "control")
		}
	}
}

// WithStoreMetrics 配置下属 StoreActor 共享的热路径指标收集器（nil 时保持默认隔离实例）。
func WithStoreMetrics(metrics *store.Metrics) Option {
	return func(m *manager) {
		if metrics != nil {
			m.storeMetrics = metrics
		}
	}
}

func withEngineCollector(collector *storageEngineCollector) Option {
	return func(m *manager) {
		if collector != nil {
			m.collector = collector
		}
	}
}

func withMaxPackSize(size uint32) Option {
	return func(m *manager) { m.maxPackSize = size }
}

// WithClock 配置控制面租约时效判定的时钟源（nil 时回退为 time.Now）。
func WithClock(now func() time.Time) Option {
	return func(m *manager) {
		if now != nil {
			m.now = now
		}
	}
}

// WithMountLeaseTTL 配置在线挂载软租约有效时长（ttl <= 0 时保持默认值）。
func WithMountLeaseTTL(ttl time.Duration) Option {
	return func(m *manager) {
		if ttl > 0 {
			m.mountLeaseTTL = ttl
		}
	}
}

// New 创建单盘控制面管理器。
func New(dbStore db.Store, opts ...Option) Manager {
	return newManager(dbStore, opts...)
}

func newManager(dbStore db.Store, opts ...Option) *manager {
	baseLogger := slog.Default()
	m := &manager{
		store:           dbStore,
		storageRoot:     config.DefaultDataDir,
		entryTimeout:    DefaultEntryTimeoutSeconds,
		attrTimeout:     DefaultAttrTimeoutSeconds,
		dirEntryTimeout: DefaultDirEntryTimeoutSeconds,
		baseLogger:      baseLogger,
		logger:          baseLogger.With("component", "control"),
		removeAll:       os.RemoveAll,
		cache:           pebble.NewCache(DefaultSharedCacheSize),
		diskLocks:       make(map[string]*diskLockEntry),
		actors:          make(map[string]*store.StoreActor),
		now:             time.Now,
		mountLeaseTTL:   DefaultMountLeaseTTL,
		activeMounts:    make(map[string]map[string]mountLease),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	if m.storeMetrics == nil {
		m.storeMetrics = store.NewMetrics()
	}
	if m.collector == nil {
		m.collector = newStorageEngineCollector()
	}
	m.collector.bind(m)
	m.startHandoffSync()
	return m
}

func (m *manager) Collector() prometheus.Collector {
	if m == nil || m.collector == nil {
		return newStorageEngineCollector()
	}
	return m.collector
}

func (m *manager) EntryTimeoutSeconds() float64    { return m.entryTimeout }
func (m *manager) AttrTimeoutSeconds() float64     { return m.attrTimeout }
func (m *manager) DirEntryTimeoutSeconds() float64 { return m.dirEntryTimeout }
func (m *manager) EntryTimeoutForMode(mode uint32) float64 {
	if mode&store.S_IFMT == store.S_IFDIR {
		return m.dirEntryTimeout
	}
	return m.entryTimeout
}
func (m *manager) OpenKeepCache() bool { return m.openKeepCache }
func (m *manager) OpenDirCache() bool  { return m.openDirCache }
func (m *manager) AtimeMode() AtimeMode {
	return m.atimeMode
}
func (m *manager) StorageRoot() string { return m.storageRoot }

func (m *manager) diskDir(diskID string) string {
	return filepath.Join(m.storageRoot, "disks", diskID)
}

func (m *manager) commitDir(commitID string) string {
	return filepath.Join(m.storageRoot, "commits", commitID)
}

func (m *manager) newSegmentAllocators(lineageID uuid.UUID) (store.InodeSegmentAllocator, store.PackSegmentAllocator) {
	if m.store == nil || lineageID == uuid.Nil {
		return nil, nil
	}
	inoAlloc := m.newSegmentAllocator(db.SegmentKindInode, lineageID, 2, store.InodeSegment, store.ErrInodeExhausted, "inode", "预留 Inode 号段")
	packAlloc := m.newSegmentAllocator(db.SegmentKindPack, lineageID, 1, store.PackSegment, store.ErrNoSpace, "pack", "预留 Pack 号段")
	return inoAlloc, packAlloc
}

func (m *manager) newSegmentAllocator(
	kind db.SegmentKind,
	lineageID uuid.UUID,
	minFloor, segmentSize uint64,
	exhaustedErr error,
	kindLabel, errPrefix string,
) func(context.Context, uint64) (uint64, uint64, error) {
	lineageStr := lineageID.String()
	return func(reqCtx context.Context, minNext uint64) (uint64, uint64, error) {
		if minNext < minFloor {
			minNext = minFloor
		}
		if minNext > uint64(math.MaxInt64) {
			return 0, 0, exhaustedErr
		}
		count := segmentSize
		if rem := uint64(math.MaxInt64) - minNext; rem < count {
			if rem == 0 {
				return 0, 0, exhaustedErr
			}
			count = rem
		}

		baseCtx := context.Background()
		if reqCtx != nil {
			baseCtx = context.WithoutCancel(reqCtx)
		}
		allocCtx, cancel := context.WithTimeout(baseCtx, segmentReserveTimeout)
		defer cancel()

		var span trace.Span
		if trace.SpanFromContext(allocCtx).IsRecording() {
			allocCtx, span = otel.GetTracerProvider().Tracer(controlInstrumentationName).Start(allocCtx, "control.ReserveSegment",
				trace.WithAttributes(
					attribute.String("segment.kind", kindLabel),
					attribute.String("lineage.id", lineageStr),
					attribute.Int64("segment.min_next", int64(minNext)),
				),
			)
		}

		start := time.Now()
		seg, err := m.store.ReserveSegment(allocCtx, kind, lineageID, int64(minNext), int64(count))
		if err != nil {
			m.collector.observeSegmentReserve(allocCtx, kindLabel, false, time.Since(start))
			if errors.Is(err, db.ErrNotFound) {
				err = ErrNotFound
			} else {
				err = fmt.Errorf("%s: %w", errPrefix, mapDBErr(err))
			}
			if span != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				span.End()
			}
			return 0, 0, err
		}
		if seg.First < int64(minFloor) || seg.Last < seg.First {
			m.collector.observeSegmentReserve(allocCtx, kindLabel, false, time.Since(start))
			if span != nil {
				span.RecordError(exhaustedErr)
				span.SetStatus(codes.Error, exhaustedErr.Error())
				span.End()
			}
			return 0, 0, exhaustedErr
		}
		m.collector.observeSegmentReserve(allocCtx, kindLabel, true, time.Since(start))
		if span != nil {
			span.SetAttributes(
				attribute.Int64("segment.first", seg.First),
				attribute.Int64("segment.last", seg.Last),
			)
			span.End()
		}
		return uint64(seg.First), uint64(seg.Last), nil
	}
}

func (m *manager) lockDisk(ctx context.Context, diskID string) func() {
	m.diskLocksMu.Lock()
	entry := m.diskLocks[diskID]
	if entry == nil {
		entry = &diskLockEntry{}
		m.diskLocks[diskID] = entry
	}
	entry.refs++
	m.diskLocksMu.Unlock()

	if !entry.mu.TryLock() {
		var span trace.Span
		if ctx != nil && trace.SpanFromContext(ctx).IsRecording() {
			_, span = otel.GetTracerProvider().Tracer(controlInstrumentationName).Start(ctx, "control.LockDiskWait",
				trace.WithAttributes(attribute.String("disk.id", diskID)),
			)
		}
		entry.mu.Lock()
		if span != nil {
			span.End()
		}
	}
	return func() {
		entry.mu.Unlock()
		m.diskLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(m.diskLocks, diskID)
		}
		m.diskLocksMu.Unlock()
	}
}

func mapDBErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, db.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, db.ErrInvalidArgument):
		return ErrInvalidArgument
	default:
		return err
	}
}

// validateDiskName 校验磁盘名称合法性（优先校验超长 > 255B，后校验空字节 '\x00'）。
func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("%w: disk name length %d exceeds max %d", ErrNameTooLong, len(name), MaxDiskNameBytes)
	}
	if strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: disk name contains null byte", ErrInvalidArgument)
	}
	return nil
}

// parseDiskID 解析字符串形式的 UUID，并拦截非法格式或全零 Nil UUID。
func parseDiskID(diskID string) (uuid.UUID, error) {
	id, err := uuid.Parse(diskID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: invalid disk id %q", ErrInvalidArgument, diskID)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	return id, nil
}

func assembleDisk(row *db.DiskRecord, stat *store.StatFSResult) *Disk {
	var parentDiskID string
	if row.ParentDiskID != uuid.Nil {
		parentDiskID = row.ParentDiskID.String()
	}
	return &Disk{
		DiskID:       row.DiskID.String(),
		ParentDiskID: parentDiskID,
		Name:         row.Name,
		RootIno:      RootIno,
		MaxBytes:     stat.TotalBytes,
		UsedBytes:    stat.UsedBytes,
		UsedInodes:   stat.Files,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

//nolint:gocritic // hugeParam: store.Options 按值传递保持与 store.Open 构造契约一致。
func (m *manager) openDiskActor(ctx context.Context, opts store.Options) (*store.StoreActor, error) {
	if !opts.ReadOnly && !strings.HasPrefix(opts.DiskID, "commit:") && m.store != nil {
		if reconcileErr := m.reconcilePendingMergeLocked(ctx, opts.DiskID); reconcileErr != nil {
			return nil, reconcileErr
		}
	}
	var span trace.Span
	if ctx != nil && trace.SpanFromContext(ctx).IsRecording() {
		_, span = otel.GetTracerProvider().Tracer(controlInstrumentationName).Start(ctx, "control.OpenDiskActor",
			trace.WithAttributes(
				attribute.String("disk.id", opts.DiskID),
				attribute.Bool("disk.read_only", opts.ReadOnly),
			),
		)
	}
	if opts.PackSource == nil {
		if src := m.packSource(); src != nil {
			opts.PackSource = src
			// 临时打开的提交切面不做后台补齐，只有常驻的可写盘才值得把数据逐步拉回本地。
			opts.PackPrefetch = !opts.ReadOnly && !strings.HasPrefix(opts.DiskID, "commit:")
		}
	}
	openStart := time.Now()
	actor, err := store.Open(opts)
	m.collector.observeOpenActor(ctx, err == nil, time.Since(openStart))
	if span != nil {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
	return actor, err
}

func (m *manager) getOrOpenActorWithLineageLocked(ctx context.Context, diskID string, lineageID uuid.UUID) (*store.StoreActor, error) {
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	cache := m.cache
	m.poolMu.RUnlock()

	inoAlloc, packAlloc := m.newSegmentAllocators(lineageID)
	actor, err := m.openDiskActor(ctx, store.Options{
		Dir:           m.diskDir(diskID),
		DiskID:        diskID,
		AtimeMode:     m.atimeMode,
		MaxPackSize:   m.maxPackSize,
		Cache:         cache,
		Logger:        m.baseLogger,
		Metrics:       m.storeMetrics,
		InoAllocator:  inoAlloc,
		PackAllocator: packAlloc,
	})
	if err != nil {
		return nil, err
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		if closeErr := actor.Close(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "actor close failed after manager closed",
				"disk_id", diskID,
				"mode", actorModeLabel(actor.ReadOnly()),
				"err", closeErr,
			)
		}
		return nil, ErrClosed
	}
	if existing := m.actors[diskID]; existing != nil {
		m.poolMu.Unlock()
		if closeErr := actor.CloseDiscard(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "actor race loser cleanup failed",
				"disk_id", diskID,
				"mode", actorModeLabel(actor.ReadOnly()),
				"err", closeErr,
			)
		}
		return existing, nil
	}
	m.actors[diskID] = actor
	m.poolMu.Unlock()

	mode := actorModeLabel(actor.ReadOnly())
	m.collector.incActorLoad(actor.ReadOnly())
	m.logger.InfoContext(ctx, "actor loaded",
		"disk_id", diskID,
		"epoch", actor.WriterEpoch(),
		"mode", mode,
	)
	return actor, nil
}

func (m *manager) CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error) {
	if params.MaxBytes == 0 || params.MaxBytes > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateDiskName(params.Name); err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	diskID := id.String()

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	cache := m.cache
	m.poolMu.RUnlock()

	dir := m.diskDir(diskID)
	inoAlloc, packAlloc := m.newSegmentAllocators(id)
	actor, err := m.openDiskActor(ctx, store.Options{
		Dir:           dir,
		DiskID:        diskID,
		MaxBytes:      params.MaxBytes,
		AtimeMode:     m.atimeMode,
		MaxPackSize:   m.maxPackSize,
		Cache:         cache,
		Logger:        m.baseLogger,
		Metrics:       m.storeMetrics,
		InoAllocator:  inoAlloc,
		PackAllocator: packAlloc,
	})
	if err != nil {
		return nil, fmt.Errorf("初始化磁盘存储: %w", err)
	}

	row, createErr := m.store.CreateDisk(ctx, id, params.Name)
	if createErr != nil {
		if closeErr := actor.CloseDiscard(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "create disk rollback close failed",
				"disk_id", diskID,
				"err", closeErr,
			)
		}
		if removeErr := m.removeAll(dir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "create disk rollback remove dir failed",
				"disk_id", diskID,
				"dir", dir,
				"err", removeErr,
			)
		}
		return nil, fmt.Errorf("创建磁盘: %w", mapDBErr(createErr))
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		if closeErr := actor.Close(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "create disk close failed after manager closed",
				"disk_id", diskID,
				"err", closeErr,
			)
		}
		return nil, ErrClosed
	}
	m.actors[diskID] = actor
	m.poolMu.Unlock()

	mode := actorModeLabel(actor.ReadOnly())
	m.collector.incActorLoad(actor.ReadOnly())

	stat, err := actor.StatFS(ctx)
	if err != nil {
		return nil, err
	}
	m.logger.InfoContext(ctx, "disk created",
		"disk_id", diskID,
		"name", params.Name,
		"max_bytes", params.MaxBytes,
		"epoch", actor.WriterEpoch(),
		"mode", mode,
	)
	return assembleDisk(&row, &stat), nil
}

func (m *manager) ForkDisk(ctx context.Context, params ForkDiskParams) (_ *Disk, retErr error) {
	defer func() {
		m.collector.incForkResult(retErr == nil)
	}()

	sourceUUID, err := parseDiskID(params.SourceDiskID)
	if err != nil {
		return nil, err
	}
	sourceDiskID := sourceUUID.String()

	err = validateDiskName(params.Name)
	if err != nil {
		return nil, err
	}
	if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		return nil, ErrInvalidArgument
	}

	unlockSource := m.lockDisk(ctx, sourceDiskID)
	defer unlockSource()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	cache := m.cache
	m.poolMu.RUnlock()

	sourceRow, err := m.store.GetDisk(ctx, sourceUUID)
	if err != nil {
		return nil, mapDBErr(err)
	}

	sourceActor, err := m.getOrOpenActorWithLineageLocked(ctx, sourceDiskID, sourceRow.LineageID)
	if err != nil {
		return nil, err
	}

	if params.MaxBytes != nil {
		hasOrphans, orphanErr := sourceActor.HasOrphans()
		if orphanErr != nil {
			return nil, orphanErr
		}
		if !hasOrphans {
			srcStat, statErr := sourceActor.StatFS(ctx)
			if statErr != nil {
				return nil, statErr
			}
			if *params.MaxBytes < srcStat.UsedBytes {
				return nil, ErrInvalidShrink
			}
		}
	}

	commitUUID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	childUUID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	commitID := commitUUID.String()
	childDiskID := childUUID.String()
	if childDiskID == sourceDiskID {
		return nil, ErrIO
	}

	unlockChild := m.lockDisk(ctx, childDiskID)
	defer unlockChild()

	commitDir := m.commitDir(commitID)
	ckStart := time.Now()
	ckStats, err := sourceActor.Checkpoint(ctx, commitDir)
	m.collector.observeForkStage(ctx, "checkpoint", time.Since(ckStart))
	if err != nil {
		return nil, fmt.Errorf("固化源盘基准切面: %w", err)
	}

	removeCommitDirOnRollback := func(stage string) {
		if removeErr := m.removeAll(commitDir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk rollback remove commit dir failed",
				"stage", stage,
				"source_disk_id", sourceDiskID,
				"commit_id", commitID,
				"dir", commitDir,
				"err", removeErr,
			)
		}
	}

	effectiveUsedBytes := ckStats.LogicalBytes
	if ckStats.HasOrphans {
		sanStart := time.Now()
		sanitizedStat, sanErr := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{
			Dir:       commitDir,
			CommitID:  commitID,
			Cache:     cache,
			Logger:    m.baseLogger,
			Metrics:   m.storeMetrics,
			OpenActor: m.openDiskActor,
		})
		m.collector.observeForkStage(ctx, "sanitize", time.Since(sanStart))
		if sanErr != nil {
			removeCommitDirOnRollback("sanitize")
			return nil, fmt.Errorf("净化基准切面孤儿节点: %w", sanErr)
		}
		effectiveUsedBytes = sanitizedStat.UsedBytes
	}

	targetMaxBytes := ckStats.MaxBytes
	if params.MaxBytes != nil {
		if *params.MaxBytes < effectiveUsedBytes {
			removeCommitDirOnRollback("quota_check")
			return nil, ErrInvalidShrink
		}
		targetMaxBytes = *params.MaxBytes
	}

	childDir := m.diskDir(childDiskID)
	cloneStart := time.Now()
	err = store.CloneCheckpointDir(ctx, commitDir, childDir)
	m.collector.observeForkStage(ctx, "clone_dir", time.Since(cloneStart))
	if err != nil {
		removeCommitDirOnRollback("clone")
		return nil, fmt.Errorf("克隆派生子盘目录: %w", err)
	}

	removeChildDirOnRollback := func(stage string) {
		if removeErr := m.removeAll(childDir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk rollback remove child dir failed",
				"stage", stage,
				"disk_id", childDiskID,
				"dir", childDir,
				"err", removeErr,
			)
		}
	}

	inoAlloc, packAlloc := m.newSegmentAllocators(sourceRow.LineageID)
	openChildStart := time.Now()
	childActor, err := m.openDiskActor(ctx, store.Options{
		Dir:           childDir,
		DiskID:        childDiskID,
		MaxBytes:      targetMaxBytes,
		AtimeMode:     m.atimeMode,
		MaxPackSize:   m.maxPackSize,
		Cache:         cache,
		Logger:        m.baseLogger,
		Metrics:       m.storeMetrics,
		InoAllocator:  inoAlloc,
		PackAllocator: packAlloc,
		ResetEpoch:    true,
	})
	m.collector.observeForkStage(ctx, "open_child", time.Since(openChildStart))
	if err != nil {
		removeChildDirOnRollback("open_child")
		removeCommitDirOnRollback("open_child")
		return nil, fmt.Errorf("打开派生子盘存储实例: %w", err)
	}

	var (
		txSpan    trace.Span
		txCtxWrap = ctx
	)
	if ctx != nil && trace.SpanFromContext(ctx).IsRecording() {
		txCtxWrap, txSpan = otel.GetTracerProvider().Tracer(controlInstrumentationName).Start(ctx, "control.ForkCommitTx",
			trace.WithAttributes(
				attribute.String("source_disk_id", sourceDiskID),
				attribute.String("disk.id", childDiskID),
				attribute.String("bowl.commit_id", commitID),
			),
		)
	}
	commitTxStart := time.Now()
	childRow, err := m.store.ForkDisk(txCtxWrap, db.ForkDiskInput{
		SourceDiskID: sourceUUID,
		ChildDiskID:  childUUID,
		CommitID:     commitUUID,
		Name:         params.Name,
	})
	m.collector.observeForkStage(ctx, "commit_tx", time.Since(commitTxStart))
	if err != nil {
		err = mapDBErr(err)
	}
	if txSpan != nil {
		if err != nil {
			txSpan.RecordError(err)
			txSpan.SetStatus(codes.Error, err.Error())
		}
		txSpan.End()
	}
	if err != nil {
		if closeErr := childActor.CloseDiscard(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk rollback close child actor failed",
				"disk_id", childDiskID,
				"err", closeErr,
			)
		}
		removeChildDirOnRollback("tx")
		removeCommitDirOnRollback("tx")
		return nil, fmt.Errorf("派生磁盘: %w", err)
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		if closeErr := childActor.Close(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk close failed after manager closed",
				"disk_id", childDiskID,
				"err", closeErr,
			)
		}
		return nil, ErrClosed
	}
	m.actors[childDiskID] = childActor
	m.poolMu.Unlock()

	mode := actorModeLabel(childActor.ReadOnly())
	m.collector.incActorLoad(childActor.ReadOnly())

	stat, err := childActor.StatFS(ctx)
	if err != nil {
		return nil, err
	}
	m.logger.InfoContext(ctx, "disk forked",
		"disk_id", childDiskID,
		"source_disk_id", sourceDiskID,
		"commit_id", commitID,
		"name", params.Name,
		"max_bytes", targetMaxBytes,
		"epoch", childActor.WriterEpoch(),
		"mode", mode,
	)
	return assembleDisk(&childRow, &stat), nil
}

func (m *manager) ensureOpen() error {
	m.poolMu.RLock()
	closed := m.closed
	m.poolMu.RUnlock()
	if closed {
		return ErrClosed
	}
	return nil
}

func (m *manager) GetDisk(ctx context.Context, diskID string) (*Disk, error) {
	id, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}
	diskID = id.String()
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	row, err := m.store.GetDisk(ctx, id)
	if err != nil {
		return nil, mapDBErr(err)
	}

	actor, err := m.getOrOpenActorWithLineageLocked(ctx, diskID, row.LineageID)
	if err != nil {
		return nil, err
	}
	stat, err := actor.StatFS(ctx)
	if err != nil {
		return nil, err
	}
	return assembleDisk(&row, &stat), nil
}

func (m *manager) ListDisks(ctx context.Context, params ListDisksParams) (*ListDisksResult, error) {
	if params.PageSize > MaxListDisksPageSize {
		return nil, fmt.Errorf("%w: page_size %d exceeds max %d", ErrInvalidArgument, params.PageSize, MaxListDisksPageSize)
	}
	if err := m.ensureOpen(); err != nil {
		return nil, err
	}
	pageSize := params.PageSize
	if pageSize == 0 {
		pageSize = DefaultListDisksPageSize
	}

	page, err := m.store.ListDisks(ctx, db.ListDisksQuery{
		PageSize:  pageSize,
		PageToken: params.PageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("list disks: %w", mapDBErr(err))
	}

	disks := make([]*Disk, 0, len(page.Disks))
	for i := range page.Disks {
		row := page.Disks[i]
		diskID := row.DiskID.String()
		actor, actorErr := m.actorForDisk(ctx, diskID, row.LineageID)
		if actorErr != nil {
			return nil, actorErr
		}
		stat, statErr := actor.StatFS(ctx)
		if statErr != nil {
			return nil, statErr
		}
		disks = append(disks, assembleDisk(&row, &stat))
	}

	return &ListDisksResult{
		Disks:         disks,
		NextPageToken: page.NextPageToken,
	}, nil
}

func (m *manager) actorForDisk(ctx context.Context, diskID string, lineageID uuid.UUID) (*store.StoreActor, error) {
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	m.poolMu.RUnlock()

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()
	return m.getOrOpenActorWithLineageLocked(ctx, diskID, lineageID)
}

func (m *manager) UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error) {
	id, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	diskID := id.String()

	if params.Name == nil && params.MaxBytes == nil {
		return nil, ErrInvalidArgument
	}
	if params.Name != nil {
		if nameErr := validateDiskName(*params.Name); nameErr != nil {
			return nil, nameErr
		}
	}
	if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		return nil, ErrInvalidArgument
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	row, err := m.store.GetDisk(ctx, id)
	if err != nil {
		return nil, mapDBErr(err)
	}

	actor, err := m.getOrOpenActorWithLineageLocked(ctx, diskID, row.LineageID)
	if err != nil {
		return nil, err
	}

	if params.MaxBytes != nil {
		if maxErr := actor.UpdateMaxBytes(ctx, *params.MaxBytes); maxErr != nil {
			return nil, maxErr
		}
	}

	if params.Name != nil {
		row, err = m.store.UpdateDiskName(ctx, id, *params.Name)
		if err != nil {
			return nil, mapDBErr(err)
		}
	}

	stat, err := actor.StatFS(ctx)
	if err != nil {
		return nil, err
	}
	m.logger.InfoContext(ctx, "disk updated",
		"disk_id", diskID,
		"name", row.Name,
		"max_bytes", stat.TotalBytes,
	)
	return assembleDisk(&row, &stat), nil
}

func (m *manager) DeleteDisk(ctx context.Context, diskID string) error {
	id, err := parseDiskID(diskID)
	if err != nil {
		return err
	}
	diskID = id.String()
	if openErr := m.ensureOpen(); openErr != nil {
		return openErr
	}

	// 删盘触发 ClearLineageBaseCommits 前，先协助血缘中崩溃未重启的他盘完成挂起合并的前滚/回滚。
	if recErr := m.reconcileOtherPendingMergesBeforeDelete(ctx, diskID); recErr != nil {
		return recErr
	}

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	if openErr := m.ensureOpen(); openErr != nil {
		return openErr
	}
	if recErr := m.reconcilePendingMergeLocked(ctx, diskID); recErr != nil {
		return recErr
	}

	commitsToClean, err := m.store.DeleteDisk(ctx, id)
	if err != nil {
		return mapDBErr(err)
	}

	m.poolMu.Lock()
	actor := m.actors[diskID]
	delete(m.actors, diskID)
	m.poolMu.Unlock()

	var (
		epoch       uint64
		mode        = actorModeRW
		cleanupErrs []error
	)
	if actor != nil {
		epoch = actor.WriterEpoch()
		mode = actorModeLabel(actor.ReadOnly())
		if closeErr := actor.CloseDiscard(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "delete disk actor close failed",
				"disk_id", diskID,
				"epoch", epoch,
				"mode", mode,
				"err", closeErr,
			)
			cleanupErrs = append(cleanupErrs, fmt.Errorf("关闭磁盘存储实例: %w", closeErr))
		}
	}

	dir := m.diskDir(diskID)
	if removeErr := m.removeAll(dir); removeErr != nil {
		m.collector.incCleanupError()
		m.logger.WarnContext(ctx, "delete disk directory cleanup failed",
			"disk_id", diskID,
			"dir", dir,
			"epoch", epoch,
			"mode", mode,
			"err", removeErr,
		)
		cleanupErrs = append(cleanupErrs, fmt.Errorf("清理磁盘存储目录: %w", removeErr))
	}
	m.cleanupPendingMergeArtifactsForDisk(ctx, diskID)

	if len(commitsToClean) > 0 {
		var reclaimSpan trace.Span
		if ctx != nil && trace.SpanFromContext(ctx).IsRecording() {
			_, reclaimSpan = otel.GetTracerProvider().Tracer(controlInstrumentationName).Start(ctx, "control.ReclaimCommits",
				trace.WithAttributes(
					attribute.String("disk.id", diskID),
					attribute.Int("reclaimed_commits", len(commitsToClean)),
				),
			)
		}
		var reclaimErrs []error
		m.lineageCommitMu.Lock()
		for _, cID := range commitsToClean {
			if cID == uuid.Nil {
				continue
			}
			cidStr := cID.String()
			cDir := m.commitDir(cidStr)
			if removeErr := m.removeAll(cDir); removeErr != nil {
				m.collector.incCleanupError()
				m.logger.WarnContext(ctx, "delete disk commit directory cleanup failed",
					"disk_id", diskID,
					"commit_id", cidStr,
					"dir", cDir,
					"err", removeErr,
				)
				rErr := fmt.Errorf("清理切面快照目录 %s: %w", cidStr, removeErr)
				cleanupErrs = append(cleanupErrs, rErr)
				reclaimErrs = append(reclaimErrs, rErr)
			} else {
				m.collector.addCommitsReclaimed(1)
			}
		}
		m.lineageCommitMu.Unlock()
		if reclaimSpan != nil {
			if errJoined := errors.Join(reclaimErrs...); errJoined != nil {
				reclaimSpan.RecordError(errJoined)
				reclaimSpan.SetStatus(codes.Error, errJoined.Error())
			}
			reclaimSpan.End()
		}
	}

	if len(cleanupErrs) > 0 {
		return errors.Join(cleanupErrs...)
	}
	m.clearDiskMounts(diskID)
	_ = os.Remove(m.syncMarkPath(diskID))
	m.dropRemoteDisk(ctx, diskID)

	m.logger.InfoContext(ctx, "disk deleted",
		"disk_id", diskID,
		"reclaimed_commits", len(commitsToClean),
		"epoch", epoch,
		"mode", mode,
	)
	return nil
}

func (m *manager) MountDisk(ctx context.Context, params MountDiskParams) (*MountDiskResult, error) {
	id, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	diskID := id.String()

	if syncErr := m.syncBeforeMount(ctx, diskID, params.ReadOnly); syncErr != nil {
		return nil, syncErr
	}
	actor, err := m.Actor(ctx, diskID)
	if err != nil {
		return nil, err
	}
	if params.ReadOnly {
		rootAttr, attrErr := actor.GetAttr(ctx, store.RootIno)
		if attrErr != nil {
			return nil, attrErr
		}
		mountID, ttlSeconds, leaseErr := m.issueMountLease(diskID, true, 0)
		if leaseErr != nil {
			return nil, leaseErr
		}
		m.logger.InfoContext(ctx, "disk mounted",
			"disk_id", diskID,
			"mount_id", mountID,
			"epoch", uint64(0),
			"mode", actorModeRO,
		)
		return &MountDiskResult{
			Epoch:               0,
			RootAttr:            rootAttr,
			EntryTimeoutSeconds: m.entryTimeout,
			AttrTimeoutSeconds:  m.attrTimeout,
			MountID:             mountID,
			LeaseTTLSeconds:     ttlSeconds,
		}, nil
	}

	if ownerErr := m.checkHandoffOwner(ctx, diskID); ownerErr != nil {
		return nil, ownerErr
	}
	prevEpoch := actor.WriterEpoch()
	epoch, rootAttr, err := actor.AdvanceEpoch(ctx)
	if err != nil {
		return nil, err
	}
	mountID, ttlSeconds, leaseErr := m.issueMountLease(diskID, false, epoch)
	if leaseErr != nil {
		return nil, leaseErr
	}
	m.logger.InfoContext(ctx, "disk epoch fenced",
		"disk_id", diskID,
		"mount_id", mountID,
		"prev_epoch", prevEpoch,
		"epoch", epoch,
		"mode", actorModeRW,
	)
	return &MountDiskResult{
		Epoch:               epoch,
		RootAttr:            rootAttr,
		EntryTimeoutSeconds: m.entryTimeout,
		AttrTimeoutSeconds:  m.attrTimeout,
		MountID:             mountID,
		LeaseTTLSeconds:     ttlSeconds,
	}, nil
}

func (m *manager) Actor(ctx context.Context, diskID string) (*store.StoreActor, error) {
	// 热路径零分配快查：m.actors 仅登记规范化小写非零 UUID 键，已打开的磁盘可直接命中返回。
	m.poolMu.RLock()
	if !m.closed {
		if actor := m.actors[diskID]; actor != nil {
			m.poolMu.RUnlock()
			return actor, nil
		}
	}
	m.poolMu.RUnlock()

	id, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}
	diskID = id.String()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	m.poolMu.RUnlock()

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	m.poolMu.RUnlock()

	row, err := m.store.GetDisk(ctx, id)
	if err != nil {
		return nil, mapDBErr(err)
	}
	return m.getOrOpenActorWithLineageLocked(ctx, diskID, row.LineageID)
}

func (m *manager) Close() error {
	// 先停后台同步并等它退出：它会拿着 Actor 做 Checkpoint，不能和下面的关闭并发。
	m.stopHandoffSync()
	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		return nil
	}
	m.closed = true
	actors := m.actors
	m.actors = make(map[string]*store.StoreActor)
	cache := m.cache
	m.cache = nil
	m.poolMu.Unlock()

	var errs []error
	for diskID, actor := range actors {
		if closeErr := actor.Close(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.Warn("manager shutdown actor close failed",
				"disk_id", diskID,
				"err", closeErr,
			)
			errs = append(errs, closeErr)
		}
	}
	if cache != nil {
		cache.Unref()
	}
	return errors.Join(errs...)
}
