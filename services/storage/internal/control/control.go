package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	"github.com/starsrc/bowl/services/storage/internal/store"
)

const controlInstrumentationName = "github.com/starsrc/bowl/services/storage/internal/control"

const (
	RootIno                       uint64  = store.RootIno
	MaxDiskNameBytes                      = 255
	DefaultListDisksPageSize      uint32  = 50
	MaxListDisksPageSize          uint32  = 1000
	DefaultEntryTimeoutSeconds    float64 = 1.0
	DefaultAttrTimeoutSeconds     float64 = 1.0
	DefaultDirEntryTimeoutSeconds float64 = 1.0
	DefaultSharedCacheSize                = store.DefaultCacheSize
	DefaultMountLeaseTTL                  = 30 * time.Second
	DefaultActorIdleTTL                   = config.DefaultActorIdleTTL

	segmentReserveTimeout = 5 * time.Second
)

var (
	ErrInvalidArgument   = store.ErrInvalid
	ErrNameTooLong       = store.ErrNameTooLong
	ErrNotFound          = store.ErrNotFound
	ErrInvalidShrink     = store.ErrQuotaShrinkBelowUsed
	ErrStaleEpoch        = store.ErrStaleEpoch
	ErrClosed            = store.ErrClosed
	ErrAlreadyExists     = store.ErrExist
	ErrNotDir            = store.ErrNotDir
	ErrIsDir             = store.ErrIsDir
	ErrNotEmpty          = store.ErrNotEmpty
	ErrPerm              = store.ErrPerm
	ErrNoData            = store.ErrNoData
	ErrNoSpace           = store.ErrNoSpace
	ErrNotSupported      = store.ErrNotSupported
	ErrRange             = store.ErrRange
	ErrTooManyLinks      = store.ErrTooManyLinks
	ErrFileTooLarge      = store.ErrFileTooLarge
	ErrIO                = store.ErrIO
	ErrCrossLineage      = db.ErrCrossLineage
	ErrStaleBaseSnapshot = db.ErrStaleBaseSnapshot
	ErrAmbiguousLCA      = errors.New("ambiguous lowest common ancestor (criss-cross merge)")
	ErrNoBaseSnapshot    = errors.New("disk has no base snapshot in lineage")
	ErrConcurrentMerge   = errors.New("concurrent write or epoch change on target disk during merge")
	ErrDiskMounted       = errors.New("disk is currently mounted")
)

type AtimeMode = store.AtimeMode

const (
	AtimeModeNoAtime     = store.AtimeModeNoAtime
	AtimeModeRelAtime    = store.AtimeModeRelAtime
	AtimeModeStrictAtime = store.AtimeModeStrictAtime
)

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

type CreateDiskParams struct {
	Name     string
	MaxBytes uint64
}

type ForkDiskParams struct {
	SourceDiskID string
	Name         string
	MaxBytes     *uint64
}

type ListDisksParams struct {
	PageSize  uint32
	PageToken string
}

type ListDisksResult struct {
	Disks         []*Disk
	NextPageToken string
}

type UpdateDiskParams struct {
	DiskID   string
	Name     *string
	MaxBytes *uint64
}

type DeleteDiskParams struct {
	DiskID    string
	Recursive bool
}

type MountDiskParams struct {
	DiskID   string
	ReadOnly bool
	Force    bool
}

type MountDiskResult struct {
	Epoch               uint64
	RootAttr            store.Attribute
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
	MountID             string
	LeaseTTLSeconds     uint32
}

type RenewMountParams struct {
	DiskID   string
	MountID  string
	ReadOnly bool
	Epoch    uint64
}

type RenewMountResult struct {
	LeaseTTLSeconds uint32
}

type UnmountDiskParams struct {
	DiskID  string
	MountID string
}

type MountLeaseInfo struct {
	MountID   string
	ReadOnly  bool
	Epoch     uint64
	ExpiresAt time.Time
}

type EvictMountsParams struct {
	DiskID  string
	MountID string
}

type EvictMountsResult struct {
	EvictedCount uint32
	WriterEpoch  uint64
}

type Manager interface {
	CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error)
	ForkDisk(ctx context.Context, params ForkDiskParams) (*Disk, error)
	GetDisk(ctx context.Context, diskID string) (*Disk, error)
	ListDisks(ctx context.Context, params ListDisksParams) (*ListDisksResult, error)
	UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error)
	DeleteDisk(ctx context.Context, params DeleteDiskParams) error
	MountDisk(ctx context.Context, params MountDiskParams) (*MountDiskResult, error)
	RenewMount(ctx context.Context, params RenewMountParams) (*RenewMountResult, error)
	UnmountDisk(ctx context.Context, params UnmountDiskParams) error
	ListMounts(ctx context.Context, diskID string) ([]MountLeaseInfo, error)
	EvictMounts(ctx context.Context, params EvictMountsParams) (*EvictMountsResult, error)
	DiffDisk(ctx context.Context, params *DiffDiskParams) (*DiffDiskResult, error)
	ReadSnapshotNode(ctx context.Context, params *ReadSnapshotNodeParams) (*store.ReadNodeResult, error)
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

	lineageSnapshotMu       sync.RWMutex
	beforePhase2CutoverHook func()

	poolMu sync.RWMutex
	closed bool
	actors map[string]*actorEntry

	now               func() time.Time
	mountLeaseTTL     time.Duration
	actorIdleTTL      time.Duration
	usageSyncInterval time.Duration

	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWG     sync.WaitGroup
}

type Option func(*manager)

func WithStorageRoot(root string) Option {
	return func(m *manager) {
		if root != "" {
			m.storageRoot = root
		}
	}
}

func WithEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.entryTimeout = d
		}
	}
}

func WithAttrTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.attrTimeout = d
		}
	}
}

func WithDirEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.dirEntryTimeout = d
		}
	}
}

func WithOpenKeepCache(enable bool) Option {
	return func(m *manager) { m.openKeepCache = enable }
}

func WithOpenDirCache(enable bool) Option {
	return func(m *manager) { m.openDirCache = enable }
}

func WithAtimeMode(mode AtimeMode) Option {
	return func(m *manager) { m.atimeMode = mode }
}

func WithLogger(logger *slog.Logger) Option {
	return func(m *manager) {
		if logger != nil {
			m.baseLogger = logger
			m.logger = logger.With("component", "control")
		}
	}
}

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

func WithClock(now func() time.Time) Option {
	return func(m *manager) {
		if now != nil {
			m.now = now
		}
	}
}

func WithMountLeaseTTL(ttl time.Duration) Option {
	return func(m *manager) {
		if ttl > 0 {
			m.mountLeaseTTL = ttl
		}
	}
}

func WithActorIdleTTL(ttl time.Duration) Option {
	return func(m *manager) {
		if ttl >= 0 {
			m.actorIdleTTL = ttl
		}
	}
}

func WithUsageSyncInterval(interval time.Duration) Option {
	return func(m *manager) {
		if interval >= 0 {
			m.usageSyncInterval = interval
		}
	}
}

func New(dbStore db.Store, opts ...Option) Manager {
	return newManager(dbStore, opts...)
}

func newManager(dbStore db.Store, opts ...Option) *manager {
	baseLogger := slog.Default()
	m := &manager{
		store:             dbStore,
		storageRoot:       config.DefaultDataDir,
		entryTimeout:      DefaultEntryTimeoutSeconds,
		attrTimeout:       DefaultAttrTimeoutSeconds,
		dirEntryTimeout:   DefaultDirEntryTimeoutSeconds,
		baseLogger:        baseLogger,
		logger:            baseLogger.With("component", "control"),
		removeAll:         os.RemoveAll,
		cache:             pebble.NewCache(DefaultSharedCacheSize),
		diskLocks:         make(map[string]*diskLockEntry),
		actors:            make(map[string]*actorEntry),
		now:               time.Now,
		mountLeaseTTL:     DefaultMountLeaseTTL,
		actorIdleTTL:      DefaultActorIdleTTL,
		usageSyncInterval: DefaultUsageSyncInterval,
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
	m.startBackgroundLoops()
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

func (m *manager) snapshotDir(snapshotID string) string {
	return filepath.Join(m.storageRoot, "snapshots", snapshotID)
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
	case errors.Is(err, db.ErrStaleEpoch):
		return ErrStaleEpoch
	default:
		return err
	}
}

func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("%w: disk name length %d exceeds max %d", ErrNameTooLong, len(name), MaxDiskNameBytes)
	}
	if strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: disk name contains null byte", ErrInvalidArgument)
	}
	return nil
}

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

func assembleDiskFromRecord(row *db.DiskRecord) *Disk {
	var parentDiskID string
	if row.ParentDiskID != uuid.Nil {
		parentDiskID = row.ParentDiskID.String()
	}
	return &Disk{
		DiskID:       row.DiskID.String(),
		ParentDiskID: parentDiskID,
		Name:         row.Name,
		RootIno:      RootIno,
		MaxBytes:     row.MaxBytes,
		UsedBytes:    row.UsedBytes,
		UsedInodes:   row.UsedInodes,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func (m *manager) openDiskActor(ctx context.Context, opts store.Options) (*store.StoreActor, error) {
	if !opts.ReadOnly && !strings.HasPrefix(opts.DiskID, "snapshot:") && m.store != nil {
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

func (m *manager) getOrOpenActorWithRecordLocked(ctx context.Context, row *db.DiskRecord) (*actorEntry, error) {
	diskID := row.DiskID.String()
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if entry := m.actors[diskID]; entry != nil {
		entry.touch(m.currentTime().UnixNano())
		m.poolMu.RUnlock()
		return entry, nil
	}
	cache := m.cache
	m.poolMu.RUnlock()

	inoAlloc, packAlloc := m.newSegmentAllocators(row.LineageID)
	actor, err := m.openDiskActor(ctx, store.Options{
		Dir:           m.diskDir(diskID),
		DiskID:        diskID,
		MaxBytes:      row.MaxBytes,
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

	entry := newActorEntry(row.DiskID, actor, m.currentTime().UnixNano(), row.UsedBytes, row.UsedInodes)

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
		existing.touch(m.currentTime().UnixNano())
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
	m.actors[diskID] = entry
	m.poolMu.Unlock()

	mode := actorModeLabel(actor.ReadOnly())
	m.collector.incActorLoad(actor.ReadOnly())
	m.logger.InfoContext(ctx, "actor loaded",
		"disk_id", diskID,
		"epoch", actor.WriterEpoch(),
		"mode", mode,
	)
	return entry, nil
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

	row, createErr := m.store.CreateDisk(ctx, id, params.Name, params.MaxBytes)
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

	entry := newActorEntry(row.DiskID, actor, m.currentTime().UnixNano(), row.UsedBytes, row.UsedInodes)

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
	m.actors[diskID] = entry
	m.poolMu.Unlock()

	mode := actorModeLabel(actor.ReadOnly())
	m.collector.incActorLoad(actor.ReadOnly())

	if flushErr := m.flushEntryUsage(ctx, entry); flushErr != nil {
		m.logger.WarnContext(ctx, "create disk usage flush failed",
			"disk_id", diskID,
			"err", flushErr,
		)
	}

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

	sourceEntry, err := m.getOrOpenActorWithRecordLocked(ctx, &sourceRow)
	if err != nil {
		return nil, err
	}
	sourceActor := sourceEntry.actor

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

	snapshotUUID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	childUUID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	snapshotID := snapshotUUID.String()
	childDiskID := childUUID.String()
	if childDiskID == sourceDiskID {
		return nil, ErrIO
	}

	unlockChild := m.lockDisk(ctx, childDiskID)
	defer unlockChild()

	snapDir := m.snapshotDir(snapshotID)
	ckStart := time.Now()
	ckStats, err := sourceActor.Checkpoint(ctx, snapDir)
	m.collector.observeForkStage(ctx, "checkpoint", time.Since(ckStart))
	if err != nil {
		return nil, fmt.Errorf("固化源盘基准快照: %w", err)
	}

	removeSnapshotDirOnRollback := func(stage string) {
		if removeErr := m.removeAll(snapDir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk rollback remove snapshot dir failed",
				"stage", stage,
				"source_disk_id", sourceDiskID,
				"snapshot_id", snapshotID,
				"dir", snapDir,
				"err", removeErr,
			)
		}
	}

	effectiveUsedBytes := ckStats.LogicalBytes
	if ckStats.HasOrphans {
		sanStart := time.Now()
		sanitizedStat, sanErr := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{
			Dir:        snapDir,
			SnapshotID: snapshotID,
			Cache:      cache,
			Logger:     m.baseLogger,
			Metrics:    m.storeMetrics,
			OpenActor:  m.openDiskActor,
		})
		m.collector.observeForkStage(ctx, "sanitize", time.Since(sanStart))
		if sanErr != nil {
			removeSnapshotDirOnRollback("sanitize")
			return nil, fmt.Errorf("净化基准快照孤儿节点: %w", sanErr)
		}
		effectiveUsedBytes = sanitizedStat.UsedBytes
	}

	targetMaxBytes := ckStats.MaxBytes
	if params.MaxBytes != nil {
		if *params.MaxBytes < effectiveUsedBytes {
			removeSnapshotDirOnRollback("quota_check")
			return nil, ErrInvalidShrink
		}
		targetMaxBytes = *params.MaxBytes
	}

	childDir := m.diskDir(childDiskID)
	cloneStart := time.Now()
	err = store.CloneCheckpointDir(ctx, snapDir, childDir)
	m.collector.observeForkStage(ctx, "clone_dir", time.Since(cloneStart))
	if err != nil {
		removeSnapshotDirOnRollback("clone")
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
		removeSnapshotDirOnRollback("open_child")
		return nil, fmt.Errorf("打开派生子盘存储实例: %w", err)
	}

	childStat, err := childActor.StatFS(ctx)
	if err != nil {
		_ = childActor.CloseDiscard()
		removeChildDirOnRollback("statfs")
		removeSnapshotDirOnRollback("statfs")
		return nil, err
	}

	var (
		txSpan    trace.Span
		txCtxWrap = ctx
	)
	if ctx != nil && trace.SpanFromContext(ctx).IsRecording() {
		txCtxWrap, txSpan = otel.GetTracerProvider().Tracer(controlInstrumentationName).Start(ctx, "control.ForkSnapshotTx",
			trace.WithAttributes(
				attribute.String("source_disk_id", sourceDiskID),
				attribute.String("disk.id", childDiskID),
				attribute.String("bowl.snapshot_id", snapshotID),
			),
		)
	}
	commitTxStart := time.Now()
	childRow, err := m.store.ForkDisk(txCtxWrap, db.ForkDiskInput{
		SourceDiskID: sourceUUID,
		ChildDiskID:  childUUID,
		SnapshotID:   snapshotUUID,
		Name:         params.Name,
		MaxBytes:     targetMaxBytes,
		UsedBytes:    childStat.UsedBytes,
		UsedInodes:   childStat.Files,
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
		removeSnapshotDirOnRollback("tx")
		return nil, fmt.Errorf("派生磁盘: %w", err)
	}

	childEntry := newActorEntry(childRow.DiskID, childActor, m.currentTime().UnixNano(), childRow.UsedBytes, childRow.UsedInodes)

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
	m.actors[childDiskID] = childEntry
	m.poolMu.Unlock()

	mode := actorModeLabel(childActor.ReadOnly())
	m.collector.incActorLoad(childActor.ReadOnly())

	if flushErr := m.flushEntryUsage(ctx, sourceEntry); flushErr != nil {
		m.logger.WarnContext(ctx, "fork disk source usage flush failed",
			"source_disk_id", sourceDiskID,
			"err", flushErr,
		)
	}

	m.logger.InfoContext(ctx, "disk forked",
		"disk_id", childDiskID,
		"source_disk_id", sourceDiskID,
		"snapshot_id", snapshotID,
		"name", params.Name,
		"max_bytes", targetMaxBytes,
		"epoch", childActor.WriterEpoch(),
		"mode", mode,
	)
	return assembleDisk(&childRow, &childStat), nil
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
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	row, err := m.store.GetDisk(ctx, id)
	if err != nil {
		return nil, mapDBErr(err)
	}
	return assembleDiskFromRecord(&row), nil
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
		disks = append(disks, assembleDiskFromRecord(&page.Disks[i]))
	}

	return &ListDisksResult{
		Disks:         disks,
		NextPageToken: page.NextPageToken,
	}, nil
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

	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	row, err := m.store.GetDisk(ctx, id)
	if err != nil {
		return nil, mapDBErr(err)
	}

	entry := m.getWarmEntry(diskID)
	if entry != nil {
		if params.MaxBytes != nil {
			liveStat, statErr := entry.actor.StatFS(ctx)
			if statErr != nil {
				return nil, statErr
			}
			if *params.MaxBytes < liveStat.UsedBytes {
				return nil, ErrInvalidShrink
			}
		}

		updatedRow, updateErr := m.store.UpdateDisk(ctx, db.UpdateDiskInput{
			DiskID:   id,
			Name:     params.Name,
			MaxBytes: params.MaxBytes,
		})
		if updateErr != nil {
			return nil, mapDBErr(updateErr)
		}

		if params.MaxBytes != nil {
			if maxErr := entry.actor.UpdateMaxBytes(ctx, *params.MaxBytes); maxErr != nil {
				rollback := db.UpdateDiskInput{
					DiskID:   id,
					MaxBytes: &row.MaxBytes,
				}
				if params.Name != nil {
					rollback.Name = &row.Name
				}
				if _, rbErr := m.store.UpdateDisk(context.WithoutCancel(ctx), rollback); rbErr != nil {
					m.logger.WarnContext(ctx, "update disk rollback pg failed",
						"disk_id", diskID,
						"err", rbErr,
					)
				}
				return nil, maxErr
			}
		}

		stat, statErr := entry.actor.StatFS(ctx)
		if statErr != nil {
			return nil, statErr
		}
		m.logger.InfoContext(ctx, "disk updated",
			"disk_id", diskID,
			"name", updatedRow.Name,
			"max_bytes", stat.TotalBytes,
		)
		return assembleDisk(&updatedRow, &stat), nil
	}

	if params.MaxBytes != nil && *params.MaxBytes < row.UsedBytes {
		return nil, ErrInvalidShrink
	}
	updatedRow, updateErr := m.store.UpdateDisk(ctx, db.UpdateDiskInput{
		DiskID:   id,
		Name:     params.Name,
		MaxBytes: params.MaxBytes,
	})
	if updateErr != nil {
		return nil, mapDBErr(updateErr)
	}
	m.logger.InfoContext(ctx, "disk updated",
		"disk_id", diskID,
		"name", updatedRow.Name,
		"max_bytes", updatedRow.MaxBytes,
	)
	return assembleDiskFromRecord(&updatedRow), nil
}

func (m *manager) lockDisks(ctx context.Context, diskIDs []string) func() {
	if len(diskIDs) == 0 {
		return func() {}
	}
	sorted := slices.Clone(diskIDs)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	unlocks := make([]func(), 0, len(sorted))
	for _, id := range sorted {
		unlocks = append(unlocks, m.lockDisk(ctx, id))
	}
	return func() {
		for _, unlock := range slices.Backward(unlocks) {
			unlock()
		}
	}
}

func collectSubtreeUUIDs(disks []db.DiskRecord, rootID uuid.UUID) ([]uuid.UUID, bool) {
	foundRoot := false
	childrenByParent := make(map[uuid.UUID][]uuid.UUID)
	for i := range disks {
		d := disks[i]
		if d.DiskID == rootID {
			foundRoot = true
		}
		if d.ParentDiskID != uuid.Nil {
			childrenByParent[d.ParentDiskID] = append(childrenByParent[d.ParentDiskID], d.DiskID)
		}
	}
	if !foundRoot {
		return nil, false
	}
	visited := map[uuid.UUID]struct{}{rootID: {}}
	queue := []uuid.UUID{rootID}
	out := []uuid.UUID{rootID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, childID := range childrenByParent[cur] {
			if _, seen := visited[childID]; seen {
				continue
			}
			visited[childID] = struct{}{}
			out = append(out, childID)
			queue = append(queue, childID)
		}
	}
	slices.SortFunc(out, func(a, b uuid.UUID) int {
		return strings.Compare(a.String(), b.String())
	})
	return out, true
}

func (m *manager) lockDeleteTargets(ctx context.Context, rootID uuid.UUID, recursive bool) ([]uuid.UUID, func(), error) {
	if !recursive {
		unlock := m.lockDisk(ctx, rootID.String())
		return []uuid.UUID{rootID}, unlock, nil
	}

	for {
		row, getErr := m.store.GetDisk(ctx, rootID)
		if getErr != nil {
			return nil, nil, mapDBErr(getErr)
		}
		lineageDisks, listErr := m.store.ListLineageDisks(ctx, row.LineageID)
		if listErr != nil {
			return nil, nil, mapDBErr(listErr)
		}
		subtree, found := collectSubtreeUUIDs(lineageDisks, rootID)
		if !found {
			return nil, nil, ErrNotFound
		}
		keys := make([]string, len(subtree))
		for i, tid := range subtree {
			keys[i] = tid.String()
		}

		unlock := m.lockDisks(ctx, keys)
		if openErr := m.ensureOpen(); openErr != nil {
			unlock()
			return nil, nil, openErr
		}
		verifyDisks, verifyErr := m.store.ListLineageDisks(ctx, row.LineageID)
		if verifyErr != nil {
			unlock()
			return nil, nil, mapDBErr(verifyErr)
		}
		verifiedSubtree, stillFound := collectSubtreeUUIDs(verifyDisks, rootID)
		if !stillFound {
			unlock()
			return nil, nil, ErrNotFound
		}
		if slices.Equal(subtree, verifiedSubtree) {
			return verifiedSubtree, unlock, nil
		}
		unlock()
	}
}

func (m *manager) DeleteDisk(ctx context.Context, params DeleteDiskParams) error {
	id, err := parseDiskID(params.DiskID)
	if err != nil {
		return err
	}
	diskID := id.String()
	if openErr := m.ensureOpen(); openErr != nil {
		return openErr
	}

	if recErr := m.reconcileOtherPendingMergesBeforeDelete(ctx, diskID); recErr != nil {
		return recErr
	}

	targetIDs, unlockTargets, err := m.lockDeleteTargets(ctx, id, params.Recursive)
	if err != nil {
		return err
	}
	defer unlockTargets()

	if openErr := m.ensureOpen(); openErr != nil {
		return openErr
	}
	for _, tid := range targetIDs {
		if gateErr := m.checkDeleteMountGate(ctx, tid); gateErr != nil {
			return gateErr
		}
	}
	for _, tid := range targetIDs {
		if recErr := m.reconcilePendingMergeLocked(ctx, tid.String()); recErr != nil {
			return recErr
		}
	}

	delRes, err := m.store.DeleteDisk(ctx, id, params.Recursive)
	if err != nil {
		return mapDBErr(err)
	}

	deletedKeys := make([]string, 0, len(delRes.DeletedDiskIDs))
	for _, did := range delRes.DeletedDiskIDs {
		if did != uuid.Nil {
			deletedKeys = append(deletedKeys, did.String())
		}
	}

	evicted := make(map[string]*actorEntry, len(deletedKeys))
	m.poolMu.Lock()
	for _, dk := range deletedKeys {
		if entry := m.actors[dk]; entry != nil {
			evicted[dk] = entry
			delete(m.actors, dk)
		}
	}
	m.poolMu.Unlock()

	var (
		epoch       uint64
		mode        = actorModeRW
		cleanupErrs []error
	)
	for _, dk := range deletedKeys {
		if entry := evicted[dk]; entry != nil && entry.actor != nil {
			actor := entry.actor
			if dk == diskID {
				epoch = actor.WriterEpoch()
				mode = actorModeLabel(actor.ReadOnly())
			}
			if closeErr := actor.CloseDiscard(); closeErr != nil {
				m.collector.incCleanupError()
				m.logger.WarnContext(ctx, "delete disk actor close failed",
					"disk_id", dk,
					"epoch", actor.WriterEpoch(),
					"mode", actorModeLabel(actor.ReadOnly()),
					"err", closeErr,
				)
				cleanupErrs = append(cleanupErrs, fmt.Errorf("关闭磁盘存储实例 %s: %w", dk, closeErr))
			}
		}

		dir := m.diskDir(dk)
		if removeErr := m.removeAll(dir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "delete disk directory cleanup failed",
				"disk_id", dk,
				"dir", dir,
				"err", removeErr,
			)
			cleanupErrs = append(cleanupErrs, fmt.Errorf("清理磁盘存储目录 %s: %w", dk, removeErr))
		}
		m.cleanupPendingMergeArtifactsForDisk(ctx, dk)
	}

	snapshotsToClean := delRes.ReclaimedSnapshotIDs
	if len(snapshotsToClean) > 0 {
		var reclaimSpan trace.Span
		if ctx != nil && trace.SpanFromContext(ctx).IsRecording() {
			_, reclaimSpan = otel.GetTracerProvider().Tracer(controlInstrumentationName).Start(ctx, "control.ReclaimSnapshots",
				trace.WithAttributes(
					attribute.String("disk.id", diskID),
					attribute.Int("reclaimed_snapshots", len(snapshotsToClean)),
				),
			)
		}
		var reclaimErrs []error
		m.lineageSnapshotMu.Lock()
		for _, sID := range snapshotsToClean {
			if sID == uuid.Nil {
				continue
			}
			sidStr := sID.String()
			sDir := m.snapshotDir(sidStr)
			if removeErr := m.removeAll(sDir); removeErr != nil {
				m.collector.incCleanupError()
				m.logger.WarnContext(ctx, "delete disk snapshot directory cleanup failed",
					"disk_id", diskID,
					"snapshot_id", sidStr,
					"dir", sDir,
					"err", removeErr,
				)
				rErr := fmt.Errorf("清理快照目录 %s: %w", sidStr, removeErr)
				cleanupErrs = append(cleanupErrs, rErr)
				reclaimErrs = append(reclaimErrs, rErr)
			} else {
				m.collector.addSnapshotsReclaimed(1)
			}
		}
		m.lineageSnapshotMu.Unlock()
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

	m.logger.InfoContext(ctx, "disk deleted",
		"disk_id", diskID,
		"recursive", params.Recursive,
		"deleted_disks", len(deletedKeys),
		"reclaimed_snapshots", len(snapshotsToClean),
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

	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	entry, err := m.actorEntryLocked(ctx, id)
	if err != nil {
		return nil, err
	}
	actor := entry.actor

	if params.ReadOnly {
		rootAttr, attrErr := actor.GetAttr(ctx, store.RootIno)
		if attrErr != nil {
			return nil, attrErr
		}
		mountID, ttlSeconds, leaseErr := m.issueMountLease(ctx, id, true, 0)
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

	if !params.Force {
		if gateErr := m.checkRWMountGate(ctx, id); gateErr != nil {
			return nil, gateErr
		}
	}

	prevEpoch := actor.WriterEpoch()
	epoch, rootAttr, err := actor.AdvanceEpoch(ctx)
	if err != nil {
		return nil, err
	}
	if flushErr := m.flushEntryUsage(ctx, entry); flushErr != nil {
		m.logger.WarnContext(ctx, "mount disk post-orphan-reclaim usage flush failed",
			"disk_id", diskID,
			"err", flushErr,
		)
	}
	mountID, ttlSeconds, leaseErr := m.issueMountLease(ctx, id, false, epoch)
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

func (m *manager) actorEntryLocked(ctx context.Context, id uuid.UUID) (*actorEntry, error) {
	diskID := id.String()
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if entry := m.actors[diskID]; entry != nil {
		entry.touch(m.currentTime().UnixNano())
		m.poolMu.RUnlock()
		return entry, nil
	}
	m.poolMu.RUnlock()

	row, err := m.store.GetDisk(ctx, id)
	if err != nil {
		return nil, mapDBErr(err)
	}
	return m.getOrOpenActorWithRecordLocked(ctx, &row)
}

func (m *manager) Actor(ctx context.Context, diskID string) (*store.StoreActor, error) {

	m.poolMu.RLock()
	if !m.closed {
		if entry := m.actors[diskID]; entry != nil {
			entry.lastUsedAtNano.Store(m.currentTime().UnixNano())
			actor := entry.actor
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
	if entry := m.actors[diskID]; entry != nil {
		entry.lastUsedAtNano.Store(m.currentTime().UnixNano())
		actor := entry.actor
		m.poolMu.RUnlock()
		return actor, nil
	}
	m.poolMu.RUnlock()

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	entry, err := m.actorEntryLocked(ctx, id)
	if err != nil {
		return nil, err
	}
	return entry.actor, nil
}

func (m *manager) Close() error {
	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		return nil
	}
	m.closed = true
	bgCancel := m.bgCancel
	m.poolMu.Unlock()

	if bgCancel != nil {
		bgCancel()
	}
	m.bgWG.Wait()

	m.poolMu.Lock()
	actors := m.actors
	m.actors = make(map[string]*actorEntry)
	cache := m.cache
	m.cache = nil
	m.poolMu.Unlock()

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), closeUsageFlushTimeout)
	defer cancelFlush()
	for diskID, entry := range actors {
		if flushErr := m.flushEntryUsage(flushCtx, entry); flushErr != nil {
			m.logger.Warn("manager shutdown usage flush failed",
				"disk_id", diskID,
				"err", flushErr,
			)
		}
	}

	var errs []error
	for diskID, entry := range actors {
		if entry == nil || entry.actor == nil {
			continue
		}
		if closeErr := entry.actor.Close(); closeErr != nil {
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
