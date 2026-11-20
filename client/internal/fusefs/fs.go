package fusefs

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const DefaultReleaseRetryInterval = 200 * time.Millisecond

const DefaultRPCTimeout = 30 * time.Second

const DefaultWriteAttrTimeoutSeconds = 1.0

const DefaultOrphanBacklogThreshold = 16

const (
	DefaultUnmountTimeout           = 5 * time.Minute
	DefaultUnmountStageTimeout      = 4 * time.Minute
	DefaultUnmountStageStallTimeout = 30 * time.Second
	DefaultUnmountFsyncTimeout      = 45 * time.Second
	DefaultUnmountReleaseTimeout    = 2 * time.Second
)

const DefaultUnmountFsyncConcurrency = 64

const orphanWarnMinInterval = time.Second

var (
	noopSpanInstance = noop.Span{}
	noopEndFuseOp    = func(fuse.Status) {}
	fuseSpanNames    = func() [OpCount]string {
		var out [OpCount]string
		for op := range FuseOp(OpCount) {
			out[op] = "fuse." + op.String()
		}
		return out
	}()
)

var _ fuse.RawFileSystem = (*RawFS)(nil)

type Config struct {
	DiskID                   string
	Epoch                    uint64
	RootIno                  uint64
	RootInode                *storagev1.Inode
	EntryTimeoutSeconds      float64
	AttrTimeoutSeconds       float64
	Mode                     string
	DirectIO                 bool
	AllowOther               bool
	Debug                    bool
	EnableStatsFile          bool
	OrphanBacklogThreshold   int
	StageSize                uint64
	StageDir                 string
	StageSession             *StageDirSession
	UnmountTimeout           time.Duration
	UnmountStageTimeout      time.Duration
	UnmountStageStallTimeout time.Duration
	UnmountFsyncTimeout      time.Duration
	OnFence                  func(status fuse.Status, reason string)
	Logger                   *slog.Logger
	Metrics                  *Metrics
	TracerProvider           trace.TracerProvider
}

func normalizeMountMode(mode string) string {
	if m := strings.ToLower(strings.TrimSpace(mode)); m != "" {
		return m
	}
	return MountModeStaged
}

func (c *Config) EffectiveMode() string {
	if c == nil {
		return MountModeStaged
	}
	return normalizeMountMode(c.Mode)
}

func (c *Config) IsReadOnly() bool {
	return c.EffectiveMode() == MountModeRO
}

func (c *Config) IsStaged() bool {
	return c.EffectiveMode() == MountModeStaged
}

type reclaimRetryItem struct {
	ino      uint64
	spanCtx  trace.SpanContext
	attempts int
}

type UnmountProgress struct {
	Active    bool   `json:"active"`
	Completed uint64 `json:"completed"`
	Total     uint64 `json:"total"`
}

type unmountProgressState struct {
	active        bool
	completedOnce bool
	phase         uint8
	stageTotal    uint64
	completed     uint64
	total         uint64
}

type RawFS struct {
	fuse.RawFileSystem

	client  storagev1connect.StorageServiceClient
	handles *HandleTable
	cache   *NamespaceCache
	caps    *capabilityCache

	lifecycleMu sync.RWMutex

	stateMu        sync.Mutex
	dirtyInodes    map[uint64]struct{}
	orphanInodes   map[uint64]struct{}
	orphanSpans    map[uint64]trace.SpanContext
	statsSnapshots map[uint64][]byte

	inodeLocks inodeLocks

	topologySeq   atomic.Uint64
	metaDirtySeq  atomic.Uint64
	metaSyncedSeq atomic.Uint64
	tmpMgr        *TmpFileManager
	cfg           Config
	metrics       *Metrics
	tracer        trace.Tracer
	nextReqID     atomic.Uint64

	releaseMu       sync.Mutex
	releaseQueue    []reclaimRetryItem
	releaseCtx      context.Context
	releaseCancel   context.CancelFunc
	releaseLoopDone chan struct{}
	releaseStarted  bool
	releaseClosed   bool

	lastOrphanWarnUnixNano atomic.Int64
	fuseSysfsDir           atomic.Pointer[string]

	readAhead          *ReadAheadCoordinator
	stage              *StageCoordinator
	stageSession       *StageDirSession
	ephemeralStageRoot string
	specialMu          sync.RWMutex
	specialInodes      map[uint64]bool
	unmountMu          sync.Mutex
	unmountState       unmountProgressState
	unmountDeadline    time.Time

	fencedStatus atomic.Uint32
	fenceMu      sync.Mutex
	fenceReason  string
	fenceCh      chan struct{}
	onFence      func(status fuse.Status, reason string)
}

func NewRawFS(client storagev1connect.StorageServiceClient, cfg Config) *RawFS {
	return newRawFS(client, cfg, nil)
}

func newRawFS(client storagev1connect.StorageServiceClient, cfg Config, nowFn func() time.Time) *RawFS {
	cfg.Mode = cfg.EffectiveMode()
	if cfg.UnmountTimeout <= 0 {
		cfg.UnmountTimeout = DefaultUnmountTimeout
	}
	if cfg.UnmountStageTimeout <= 0 {
		cfg.UnmountStageTimeout = DefaultUnmountStageTimeout
	}
	if cfg.UnmountStageTimeout > cfg.UnmountTimeout {
		cfg.UnmountStageTimeout = cfg.UnmountTimeout
	}
	if cfg.UnmountStageStallTimeout <= 0 {
		cfg.UnmountStageStallTimeout = DefaultUnmountStageStallTimeout
	}
	if cfg.UnmountStageStallTimeout > cfg.UnmountStageTimeout {
		cfg.UnmountStageStallTimeout = cfg.UnmountStageTimeout
	}
	if cfg.UnmountFsyncTimeout <= 0 {
		cfg.UnmountFsyncTimeout = DefaultUnmountFsyncTimeout
	}
	if cfg.UnmountFsyncTimeout > cfg.UnmountTimeout {
		cfg.UnmountFsyncTimeout = cfg.UnmountTimeout
	}
	if cfg.RootIno == 0 && cfg.RootInode != nil {
		cfg.RootIno = cfg.RootInode.GetIno()
	}
	if cfg.TracerProvider != nil {
		if rv := reflect.ValueOf(cfg.TracerProvider); rv.Kind() == reflect.Pointer && rv.IsNil() {
			cfg.TracerProvider = nil
		}
	}
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics()
	}

	var (
		session       *StageDirSession
		ephemeralRoot string
		instanceDir   string
	)
	if cfg.IsStaged() {
		if cfg.StageSize == 0 {
			cfg.StageSize = DefaultStageSize
		}
		session = cfg.StageSession
		if session == nil {
			if cfg.StageDir != "" {
				if s, err := OpenStageDirSession(cfg.StageDir); err == nil {
					session = s
				}
			}
			if session == nil {
				if tmpRoot, err := os.MkdirTemp("", "bowl-stage-*"); err == nil {
					if s, sErr := OpenStageDirSession(tmpRoot); sErr == nil {
						session = s
						ephemeralRoot = tmpRoot
					} else {
						_ = os.RemoveAll(tmpRoot)
					}
				}
			}
		}
		if session != nil {
			instanceDir = session.InstanceDir()
		}
	}

	releaseCtx, releaseCancel := context.WithCancel(context.Background())
	fs := &RawFS{
		RawFileSystem:      fuse.NewDefaultRawFileSystem(),
		client:             client,
		cache:              NewNamespaceCache(DefaultNamespaceCacheCapacity),
		caps:               newCapabilityCache(),
		dirtyInodes:        make(map[uint64]struct{}),
		orphanInodes:       make(map[uint64]struct{}),
		orphanSpans:        make(map[uint64]trace.SpanContext),
		statsSnapshots:     make(map[uint64][]byte),
		specialInodes:      make(map[uint64]bool),
		tmpMgr:             NewTmpFileManager(cfg.RootIno, instanceDir),
		cfg:                cfg,
		metrics:            metrics,
		releaseCtx:         releaseCtx,
		releaseCancel:      releaseCancel,
		readAhead:          NewReadAheadCoordinator(),
		stageSession:       session,
		ephemeralStageRoot: ephemeralRoot,
		fenceCh:            make(chan struct{}),
		onFence:            cfg.OnFence,
	}
	fs.cache.configure(fuse.FUSE_ROOT_ID, cfg.EntryTimeoutSeconds, cfg.AttrTimeoutSeconds)
	if cfg.IsReadOnly() {
		fs.cache.configureReadOnly(nowFn)
		fs.readAhead.configureReadOnly(cfg.AttrTimeoutSeconds, nowFn)
	}
	if cfg.RootInode != nil && cfg.RootInode.GetAttr() != nil {
		fs.cache.putInode(fuse.FUSE_ROOT_ID, cfg.RootInode, cfg.EntryTimeoutSeconds, cfg.AttrTimeoutSeconds)
	}
	if cfg.IsStaged() {
		var onStage func(nodeID, cacheEpoch, endOffset uint64, ts *timestamppb.Timestamp)
		if cfg.AttrTimeoutSeconds > 0 {
			onStage = func(nodeID, cacheEpoch, endOffset uint64, ts *timestamppb.Timestamp) {
				if fs.IsFenced() {
					return
				}
				fs.cache.stageWriteInodeAt(cacheEpoch, nodeID, endOffset, ts)
			}
		}
		fs.stage = NewStageCoordinator(
			instanceDir,
			cfg.StageSize,
			func(ctx context.Context, ino, offset uint64, data []byte) (uint32, *storagev1.Inode, error) {
				if fs.client == nil {
					return 0, nil, syscall.EIO
				}
				reqCtx := rpc.WithRequestID(ctx, fs.NextRequestID())
				resp, err := fs.client.Write(reqCtx, connect.NewRequest(&storagev1.WriteRequest{
					Ino:    ino,
					Fh:     0,
					Offset: offset,
					Data:   data,
					DiskId: fs.cfg.DiskID,
					Epoch:  fs.cfg.Epoch,
				}))
				if err != nil {
					fs.fenceIfStaleEpoch(err)
					return 0, nil, err
				}
				fs.markMetaDirty()
				return resp.Msg.GetSize(), resp.Msg.GetInode(), nil
			},
			func(ctx context.Context, ino uint64, meta StagePromotedMeta) (*storagev1.Inode, error) {
				if fs.client == nil {
					return nil, syscall.EIO
				}
				size := meta.Size
				mode := meta.Mode
				gid := meta.Gid
				reqCtx := rpc.WithRequestID(ctx, fs.NextRequestID())
				resp, err := fs.client.SetAttr(reqCtx, connect.NewRequest(&storagev1.SetAttrRequest{
					Ino:    ino,
					Size:   &size,
					Atime:  meta.Atime,
					Mtime:  meta.Mtime,
					Mode:   &mode,
					Gid:    &gid,
					DiskId: fs.cfg.DiskID,
					Epoch:  fs.cfg.Epoch,
				}))
				if err != nil {
					fs.fenceIfStaleEpoch(err)
					return nil, err
				}
				fs.markMetaDirty()
				finalInode := resp.Msg.GetInode()

				for _, xa := range meta.Xattrs {
					xReqCtx := rpc.WithRequestID(ctx, fs.NextRequestID())
					if _, xErr := fs.client.SetXattr(xReqCtx, connect.NewRequest(&storagev1.SetXattrRequest{
						Ino:    ino,
						Name:   []byte(xa.Name),
						Value:  xa.Value,
						Flags:  0,
						DiskId: fs.cfg.DiskID,
						Epoch:  fs.cfg.Epoch,
					})); xErr != nil {
						fs.fenceIfStaleEpoch(xErr)
						return nil, xErr
					}
					fs.markMetaDirty()
				}

				if meta.Flags != 0 {
					flags := meta.Flags
					fReqCtx := rpc.WithRequestID(ctx, fs.NextRequestID())
					fResp, fErr := fs.client.SetAttr(fReqCtx, connect.NewRequest(&storagev1.SetAttrRequest{
						Ino:    ino,
						Flags:  &flags,
						DiskId: fs.cfg.DiskID,
						Epoch:  fs.cfg.Epoch,
					}))
					if fErr != nil {
						fs.fenceIfStaleEpoch(fErr)
						return nil, fErr
					}
					fs.markMetaDirty()
					if fResp.Msg.GetInode() != nil {
						finalInode = fResp.Msg.GetInode()
					}
				}
				return finalInode, nil
			},
			StageHooks{
				OnStage: onStage,
				OnCommit: func(nodeID, ino, cacheEpoch uint64, inode *storagev1.Inode, minSize uint64, refreshAttr, preserveMeta, capPresent bool) {
					if fs.IsFenced() {
						return
					}
					fs.markMetaDirty()
					fs.markDirty(ino)
					if preserveMeta {
						return
					}
					if inode != nil {
						fs.recordInodeSpecial(nodeID, inode)
					}
					if refreshAttr && inode != nil {
						fs.cache.refreshInodeFromWriteAt(cacheEpoch, nodeID, inode, minSize, DefaultWriteAttrTimeoutSeconds)
					}
					if capPresent {
						fs.caps.markPresent(nodeID)
					} else {
						fs.caps.markAbsent(nodeID)
					}
				},
				OnError: func(nodeID uint64) {
					fs.cache.invalidateInode(nodeID)
				},
			},
		)
		fs.stage.bindLogger(fs.logger())
	}
	fs.handles = NewHandleTableWithContext(fs.reclaimOrphan)
	fs.bindMetrics(metrics)
	fs.bindTracerProvider(fs.tracerProvider())
	return fs
}

func (fs *RawFS) bindMetrics(m *Metrics) {
	if m == nil {
		return
	}
	fs.metrics = m
	m.bindProvider(fs)
	fs.cache.bindMetrics(m)
	fs.caps.bindMetrics(m)
	fs.tmpMgr.bindMetrics(m)
	fs.readAhead.bindMetrics(m)
	fs.stage.bindMetrics(m)
	fs.inodeLocks.bindMetrics(m)
}

func (fs *RawFS) tracerProvider() trace.TracerProvider {
	if fs.cfg.TracerProvider != nil {
		return fs.cfg.TracerProvider
	}
	return noop.NewTracerProvider()
}

func (fs *RawFS) bindTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		fs.tracer = nil
	} else if _, isNoop := tp.(noop.TracerProvider); isNoop {
		fs.tracer = nil
	} else {
		fs.tracer = tp.Tracer("bowl/client/fusefs")
	}
	fs.readAhead.bindTracerProvider(tp)
	fs.stage.bindTracerProvider(tp)
}

func isSpecialAttr(inode *storagev1.Inode) bool {
	if inode == nil || inode.GetAttr() == nil {
		return true
	}
	attr := inode.GetAttr()
	return attr.GetFlags()&(ProtoFlagImmutable|ProtoFlagAppend) != 0 ||
		attr.GetMode()&(syscall.S_ISUID|syscall.S_ISGID) != 0
}

func (fs *RawFS) recordInodeSpecial(nodeID uint64, inode *storagev1.Inode) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	special := isSpecialAttr(inode)
	fs.specialMu.Lock()
	if fs.specialInodes == nil {
		fs.specialInodes = make(map[uint64]bool)
	}
	fs.specialInodes[nodeID] = special
	fs.specialMu.Unlock()
}

func (fs *RawFS) setInodeSpecialFlag(nodeID uint64, special bool) {
	fs.specialMu.Lock()
	if fs.specialInodes == nil {
		fs.specialInodes = make(map[uint64]bool)
	}
	fs.specialInodes[nodeID] = special
	fs.specialMu.Unlock()
}

func (fs *RawFS) forgetInodeSpecial(nodeID uint64) {
	fs.specialMu.Lock()
	delete(fs.specialInodes, nodeID)
	fs.specialMu.Unlock()
}

func (fs *RawFS) isInodeSpecial(nodeID uint64) bool {
	fs.specialMu.RLock()
	special, ok := fs.specialInodes[nodeID]
	fs.specialMu.RUnlock()
	if ok {
		return special
	}
	if cached, hit := fs.cache.inodeReadOnly(nodeID); hit && cached.inode != nil && cached.inode.GetAttr() != nil {
		special = isSpecialAttr(cached.inode)
		fs.specialMu.Lock()
		if fs.specialInodes == nil {
			fs.specialInodes = make(map[uint64]bool)
		}
		fs.specialInodes[nodeID] = special
		fs.specialMu.Unlock()
		return special
	}
	return true
}

func (fs *RawFS) stageErrToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	if errno, ok := errors.AsType[syscall.Errno](err); ok {
		return fuse.Status(errno)
	}
	return fs.ToStatus(err)
}

func (fs *RawFS) stageBarrierContext(opCtx context.Context, cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	if opCtx == nil {
		opCtx = context.Background()
	}
	return rpc.ContextWithCancelChan(opCtx, cancel)
}

func (fs *RawFS) barrierInode(opCtx context.Context, cancel <-chan struct{}, ino uint64) fuse.Status {
	if fs.stage == nil {
		return fuse.OK
	}
	ctx, done := fs.stageBarrierContext(opCtx, cancel)
	err := fs.stage.BarrierInode(ctx, ino)
	done()
	return fs.stageErrToStatus(err)
}

func (fs *RawFS) flushAndResetInodeForHandle(opCtx context.Context, cancel <-chan struct{}, fh, ino uint64) error {
	if fs.stage == nil {
		return nil
	}
	ctx, done := fs.stageBarrierContext(opCtx, cancel)
	err := fs.stage.FlushAndResetInodeForHandle(ctx, fh, ino)
	done()
	return err
}

func (fs *RawFS) BindMountpoint(mountpoint string) {
	if fs == nil || mountpoint == "" {
		return
	}
	if sysfsDir := resolveFuseSysfsConnectionDir(mountpoint); sysfsDir != "" {
		fs.setFuseSysfsDir(sysfsDir)
	}
}

func (fs *RawFS) setFuseSysfsDir(dir string) {
	if dir == "" {
		fs.fuseSysfsDir.Store(nil)
		return
	}
	copied := dir
	fs.fuseSysfsDir.Store(&copied)
}

func (fs *RawFS) Metrics() *Metrics {
	if fs == nil {
		return nil
	}
	return fs.metrics
}

func (fs *RawFS) Collector() prometheus.Collector {
	if fs == nil || fs.metrics == nil {
		return nil
	}
	return fs.metrics
}

func (fs *RawFS) Snapshot() Snapshot {
	if fs == nil {
		return Snapshot{}
	}
	return fs.metrics.Snapshot()
}

func (fs *RawFS) collectSubsystemGauges() subsystemGauges {
	nsEntries, nsUsed, nsCap := fs.cache.stats()
	openFiles, openDirs := fs.handles.stats()
	tmpActive := fs.tmpMgr.activeLocalFiles()
	tmpResBytes := uint64(max(int64(0), fs.metrics.tmpResidentBytes.Load()))
	stagePending, stageUsed, stageCap := fs.stage.gauges()

	fs.stateMu.Lock()
	dirtyCount := len(fs.dirtyInodes)
	orphanCount := len(fs.orphanInodes)
	fs.stateMu.Unlock()

	var fuseWaiting, fuseCongestion int
	if dirPtr := fs.fuseSysfsDir.Load(); dirPtr != nil && *dirPtr != "" {
		fuseWaiting, fuseCongestion = readFuseConnectionGauges(*dirPtr)
	}

	return subsystemGauges{
		nsEntries:               nsEntries,
		nsUsedWeight:            nsUsed,
		nsCapacity:              nsCap,
		capEntries:              fs.caps.count(),
		stagePendingChunks:      stagePending,
		stageUsedBytes:          stageUsed,
		stageCapacityBytes:      stageCap,
		tmpActiveFiles:          tmpActive,
		tmpResidentByte:         tmpResBytes,
		openFiles:               openFiles,
		openDirs:                openDirs,
		inodeLockActive:         fs.inodeLocks.activeEntries(),
		dirtyInodes:             dirtyCount,
		orphanInodes:            orphanCount,
		releaseQueue:            fs.releaseQueueDepth(),
		fuseWaiting:             fuseWaiting,
		fuseCongestionThreshold: fuseCongestion,
	}
}

func (fs *RawFS) logger() *slog.Logger {
	if fs.cfg.Logger != nil {
		return fs.cfg.Logger
	}
	return slog.Default()
}

func (fs *RawFS) startFuseOp(op FuseOp) (opCtx context.Context, endSpan func(fuse.Status)) {
	if fs.tracer == nil {
		return context.Background(), noopEndFuseOp
	}
	ctx, span := fs.tracer.Start(context.Background(), fuseSpanNames[op])
	if !span.IsRecording() {
		return ctx, noopEndFuseOp
	}
	return ctx, func(st fuse.Status) {
		span.SetAttributes(attribute.String("fuse.op", op.String()))
		if st != fuse.OK {
			errStr := errnoName(st)
			span.SetAttributes(
				attribute.Int("fuse.status_code", int(st)),
				attribute.String("fuse.errno", errStr),
			)
			if st != fuse.ENOENT && st != fuse.Status(syscall.ENODATA) {
				span.SetStatus(codes.Error, errStr)
			}
		}
		span.End()
	}
}

func (fs *RawFS) Config() Config {
	return fs.cfg
}

func (fs *RawFS) ToServerIno(kernelIno uint64) (uint64, fuse.Status) {
	return fs.tmpMgr.ToServerIno(kernelIno)
}

func (fs *RawFS) ToKernelIno(serverIno uint64) uint64 {
	return fs.tmpMgr.ToKernelIno(serverIno)
}

func (fs *RawFS) CheckWritable() fuse.Status {
	if st := fs.FenceStatus(); st != fuse.OK {
		return st
	}
	if fs.cfg.IsReadOnly() {
		return fuse.Status(syscall.EROFS)
	}
	return fuse.OK
}

func (fs *RawFS) RPCContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return fs.RPCContextFrom(context.Background(), cancel)
}

func (fs *RawFS) RPCContextFrom(parent context.Context, cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	reqCtx := rpc.WithRequestID(parent, fs.NextRequestID())
	baseCtx, cancelTimeout := context.WithTimeout(reqCtx, DefaultRPCTimeout)
	ctx, cancelChan := rpc.ContextWithCancelChan(baseCtx, cancel)
	return ctx, func() {
		cancelChan()
		cancelTimeout()
	}
}

func (fs *RawFS) ToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	fs.fenceIfStaleEpoch(err)
	errno := rpc.MapError(err)
	return fuse.Status(errno)
}

func (fs *RawFS) checkedHandle(ino, fh uint64, dir bool) (FileHandle, fuse.Status) {
	h, ok := fs.handles.Get(fh)
	if !ok || h.Ino != ino || h.IsDir != dir {
		return FileHandle{}, fuse.EBADF
	}
	return h, fuse.OK
}

func (fs *RawFS) openHandle(ino uint64, flags uint32, dir bool) (uint64, fuse.Status) {
	fh, err := fs.handles.Open(ino, flags, dir)
	if err != nil {
		if errno, ok := errors.AsType[syscall.Errno](err); ok {
			return 0, fuse.Status(errno)
		}
		return 0, fuse.EIO
	}
	if !dir && ino != VirtualStatsIno {
		fs.stage.RegisterHandle(fh)
	}
	return fh, fuse.OK
}

func (fs *RawFS) NextRequestID() uint64 {
	for {
		curr := fs.nextReqID.Add(1)
		id := curr & math.MaxInt64
		if id != 0 {
			return id
		}
	}
}

func (fs *RawFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	start := fs.metrics.beginOp(OpAccess)
	status := fs.RawFileSystem.Access(cancel, input)
	fs.metrics.endOp(OpAccess, start, status, 0)
	return status
}

func (fs *RawFS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	start := fs.metrics.beginOp(OpLseek)
	status := fs.RawFileSystem.Lseek(cancel, in, out)
	fs.metrics.endOp(OpLseek, start, status, 0)
	return status
}

func (fs *RawFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	start := fs.metrics.beginOp(OpCopyFileRange)
	written, status := fs.RawFileSystem.CopyFileRange(cancel, input)
	var n uint64
	if status == fuse.OK {
		n = uint64(written)
	}
	fs.metrics.endOp(OpCopyFileRange, start, status, n)
	return written, status
}

func (fs *RawFS) Statx(cancel <-chan struct{}, input *fuse.StatxIn, out *fuse.StatxOut) fuse.Status {
	start := fs.metrics.beginOp(OpStatx)
	status := fs.RawFileSystem.Statx(cancel, input, out)
	fs.metrics.endOp(OpStatx, start, status, 0)
	return status
}

func (fs *RawFS) UnmountRemainingBudget() time.Duration {
	if fs == nil {
		return 0
	}
	fs.unmountMu.Lock()
	deadline := fs.unmountDeadline
	fs.unmountMu.Unlock()
	if deadline.IsZero() {
		return fs.cfg.UnmountTimeout
	}
	rem := time.Until(deadline)
	if rem <= 0 {
		return 0
	}
	return rem
}

func (fs *RawFS) UnmountProgress() UnmountProgress {
	if fs == nil || fs.IsFenced() {
		return UnmountProgress{}
	}
	fs.unmountMu.Lock()
	defer fs.unmountMu.Unlock()
	if fs.unmountState.active && fs.unmountState.phase == 1 {
		livePending := uint64(max(int64(0), fs.stage.pendingChunkCount()))
		var stageDone uint64
		if livePending < fs.unmountState.stageTotal {
			stageDone = fs.unmountState.stageTotal - livePending
		}
		if stageDone > fs.unmountState.completed {
			fs.unmountState.completed = stageDone
		}
	}
	return UnmountProgress{
		Active:    fs.unmountState.active,
		Completed: fs.unmountState.completed,
		Total:     fs.unmountState.total,
	}
}

func (fs *RawFS) countDirtyAndStagedInodes(stagedInodes []uint64) int {
	fs.stateMu.Lock()
	defer fs.stateMu.Unlock()
	if len(stagedInodes) == 0 {
		return len(fs.dirtyInodes)
	}
	seen := make(map[uint64]struct{}, len(fs.dirtyInodes)+len(stagedInodes))
	for ino := range fs.dirtyInodes {
		seen[ino] = struct{}{}
	}
	for _, ino := range stagedInodes {
		seen[ino] = struct{}{}
	}
	return len(seen)
}

func (fs *RawFS) OnUnmount() {
	start := fs.metrics.beginOp(OpOnUnmount)
	opCtx, endSpan := fs.startFuseOp(OpOnUnmount)
	defer func() {
		endSpan(fuse.OK)
		fs.metrics.endOp(OpOnUnmount, start, fuse.OK, 0)
	}()

	fs.lifecycleMu.Lock()
	if fs.IsFenced() {
		fs.unmountFencedLocked(opCtx)
		fs.lifecycleMu.Unlock()
		fs.readAhead.Clear()
		return
	}

	pendingChunks, stagedInodes := fs.stage.unmountSnapshot()
	stageTotal := uint64(max(int64(0), pendingChunks))
	dirtyUnionCount := fs.countDirtyAndStagedInodes(stagedInodes)
	needMetaBarrier := dirtyUnionCount == 0 && (fs.hasUnsyncedMeta() || fs.releaseQueueDepth() > 0)
	phase2Units := uint64(dirtyUnionCount)
	if needMetaBarrier {
		phase2Units = 1
	}
	total := stageTotal + phase2Units

	fs.unmountMu.Lock()
	totalDeadline := fs.unmountDeadline
	if totalDeadline.IsZero() {
		totalDeadline = time.Now().Add(fs.cfg.UnmountTimeout)
		fs.unmountDeadline = totalDeadline
	}
	repeatNoop := fs.unmountState.completedOnce && total == 0
	if repeatNoop {
		fs.unmountState.active = false
	} else {
		fs.unmountState.active = true
		fs.unmountState.phase = 1
		fs.unmountState.stageTotal = stageTotal
		fs.unmountState.completed = 0
		fs.unmountState.total = total
	}
	fs.unmountMu.Unlock()

	totalCtx, totalCancel := context.WithDeadline(opCtx, totalDeadline)
	defer totalCancel()

	stageCtx, stageCancel := context.WithTimeout(totalCtx, fs.cfg.UnmountStageTimeout)
	fs.stage.Clear(stageCtx, fs.cfg.UnmountStageStallTimeout)
	stageCancel()
	if fs.IsFenced() {
		fs.stopAndDiscardReleaseQueue()
		_ = fs.takeDirtyInodes()
		fs.advanceMetaSyncedSeq(fs.metaDirtySeq.Load())
		fs.cleanupLocalStateOnUnmountLocked()
		fs.resetFencedUnmountProgress()
		fs.lifecycleMu.Unlock()
		fs.readAhead.Clear()
		return
	}

	fs.stopAndDrainReleaseQueue(totalCtx)
	unmountSeq := fs.metaDirtySeq.Load()
	dirtyInodes := fs.takeDirtyInodes()

	phase2Remaining := uint64(len(dirtyInodes))
	if phase2Remaining == 0 && fs.hasUnsyncedMeta() && fs.client != nil {
		phase2Remaining = 1
	}

	fs.unmountMu.Lock()
	if !repeatNoop {
		baseDone := fs.unmountState.completed
		if fs.unmountState.total >= phase2Remaining {
			if candidate := fs.unmountState.total - phase2Remaining; candidate > baseDone {
				baseDone = candidate
			}
		}
		fs.unmountState.phase = 2
		fs.unmountState.completed = baseDone
	}
	fs.unmountMu.Unlock()

	if !fs.IsFenced() && totalCtx.Err() == nil {
		fsyncCtx, fsyncCancel := context.WithTimeout(totalCtx, fs.cfg.UnmountFsyncTimeout)
		if len(dirtyInodes) > 0 {
			fs.syncDirtyInodesOnUnmount(fsyncCtx, dirtyInodes, repeatNoop)
		} else if fs.hasUnsyncedMeta() {
			fs.syncRootDirOnUnmount(fsyncCtx, repeatNoop)
		}
		fsyncCancel()
	}
	if !repeatNoop {
		fs.advanceMetaSyncedSeq(unmountSeq)
	}
	fs.cleanupLocalStateOnUnmountLocked()
	fs.unmountMu.Lock()
	if !repeatNoop {
		fs.unmountState.completed = fs.unmountState.total
		fs.unmountState.completedOnce = true
	}
	fs.unmountState.active = false
	fs.unmountState.phase = 2
	fs.unmountMu.Unlock()
	fs.lifecycleMu.Unlock()
	fs.readAhead.Clear()
}

func (fs *RawFS) unmountFencedLocked(opCtx context.Context) {
	fs.resetFencedUnmountProgress()
	fs.stage.AbortAll(fs.fenceErrToErrno())
	fs.stage.Clear(opCtx, 0)
	fs.stopAndDiscardReleaseQueue()
	_ = fs.takeDirtyInodes()
	fs.advanceMetaSyncedSeq(fs.metaDirtySeq.Load())
	fs.cleanupLocalStateOnUnmountLocked()
}

func (fs *RawFS) resetFencedUnmountProgress() {
	fs.unmountMu.Lock()
	fs.unmountState.active = false
	fs.unmountState.phase = 2
	fs.unmountState.stageTotal = 0
	fs.unmountState.completed = 0
	fs.unmountState.total = 0
	fs.unmountState.completedOnce = true
	fs.unmountMu.Unlock()
}

func (fs *RawFS) cleanupLocalStateOnUnmountLocked() {
	fs.handles.clear()
	fs.cache.clear()
	fs.caps.reset()
	fs.tmpMgr.Clear()
	fs.specialMu.Lock()
	clear(fs.specialInodes)
	fs.specialMu.Unlock()
	fs.stateMu.Lock()
	clear(fs.orphanInodes)
	clear(fs.orphanSpans)
	clear(fs.statsSnapshots)
	fs.stateMu.Unlock()
	fs.releaseMu.Lock()
	fs.releaseQueue = nil
	fs.releaseMu.Unlock()
	if fs.stageSession != nil {
		_ = fs.stageSession.Close()
	}
	if fs.ephemeralStageRoot != "" {
		_ = os.RemoveAll(fs.ephemeralStageRoot)
	}
}

func (fs *RawFS) syncDirtyInodesOnUnmount(opCtx context.Context, dirtyInodes []uint64, repeatNoop bool) {
	switch len(dirtyInodes) {
	case 0:
		return
	case 1:
		if opCtx.Err() != nil {
			fs.incrementUnmountCompleted(repeatNoop)
			return
		}
		fs.syncDirtyInodeOnUnmount(opCtx, dirtyInodes[0], repeatNoop)
		return
	}

	workers := min(DefaultUnmountFsyncConcurrency, len(dirtyInodes))
	var next atomic.Uint64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				if opCtx.Err() != nil {
					return
				}
				idx := int(next.Add(1) - 1)
				if idx >= len(dirtyInodes) {
					return
				}
				fs.syncDirtyInodeOnUnmount(opCtx, dirtyInodes[idx], repeatNoop)
			}
		})
	}
	wg.Wait()
}

func (fs *RawFS) syncDirtyInodeOnUnmount(opCtx context.Context, ino uint64, repeatNoop bool) {
	if fs.client == nil || fs.IsFenced() || opCtx.Err() != nil {
		fs.incrementUnmountCompleted(repeatNoop)
		return
	}
	seq := fs.metaDirtySeq.Load()
	ctx, done := fs.RPCContextFrom(opCtx, nil)
	_, err := fs.client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		Ino:    ino,
		DiskId: fs.cfg.DiskID,
		Epoch:  fs.cfg.Epoch,
	}))
	done()
	fs.recordUnmountSyncOutcome(ctx, ino, seq, err, repeatNoop)
}

func (fs *RawFS) syncRootDirOnUnmount(opCtx context.Context, repeatNoop bool) {
	if fs.client == nil || opCtx.Err() != nil {
		return
	}
	fs.unmountMu.Lock()
	if !repeatNoop && fs.unmountState.total <= fs.unmountState.completed {
		fs.unmountState.total = fs.unmountState.completed + 1
	}
	fs.unmountMu.Unlock()

	rootIno := fs.cfg.RootIno
	if rootIno == 0 {
		rootIno = fuse.FUSE_ROOT_ID
	}
	seq := fs.metaDirtySeq.Load()
	ctx, done := fs.RPCContextFrom(opCtx, nil)
	_, err := fs.client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
		Ino:    rootIno,
		DiskId: fs.cfg.DiskID,
		Epoch:  fs.cfg.Epoch,
	}))
	done()
	fs.recordUnmountSyncOutcome(ctx, rootIno, seq, err, repeatNoop)
}

func (fs *RawFS) recordUnmountSyncOutcome(ctx context.Context, ino, seq uint64, err error, repeatNoop bool) {
	if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
		fs.fenceIfStaleEpoch(err)
		fs.metrics.incDirtyFlushError()
		fs.logger().WarnContext(ctx, "unmount_fsync_failed", "ino", ino, "error", err)
	} else {
		fs.advanceMetaSyncedSeq(seq)
		fs.metrics.incDirtyFlushOK()
	}
	fs.incrementUnmountCompleted(repeatNoop)
}

func (fs *RawFS) incrementUnmountCompleted(repeatNoop bool) {
	fs.unmountMu.Lock()
	if !repeatNoop && fs.unmountState.completed < fs.unmountState.total {
		fs.unmountState.completed++
	}
	fs.unmountMu.Unlock()
}

func (fs *RawFS) markMetaDirty() {
	fs.metaDirtySeq.Add(1)
}

func (fs *RawFS) advanceMetaSyncedSeq(seq uint64) {
	for {
		cur := fs.metaSyncedSeq.Load()
		if cur >= seq {
			return
		}
		if fs.metaSyncedSeq.CompareAndSwap(cur, seq) {
			return
		}
	}
}

func (fs *RawFS) hasUnsyncedMeta() bool {
	return fs.metaDirtySeq.Load() > fs.metaSyncedSeq.Load()
}

func shouldRetryReclaim(err error) bool {
	if err == nil {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeResourceExhausted, connect.CodeAborted, connect.CodeInternal:
		return true
	default:
		return errors.Is(err, context.DeadlineExceeded)
	}
}

func (fs *RawFS) markDirty(ino uint64) {
	fs.stateMu.Lock()
	fs.dirtyInodes[ino] = struct{}{}
	fs.stateMu.Unlock()
}

func (fs *RawFS) clearDirty(ino uint64) {
	fs.stateMu.Lock()
	delete(fs.dirtyInodes, ino)
	fs.stateMu.Unlock()
}

func (fs *RawFS) hasDirtyInodes() bool {
	fs.stateMu.Lock()
	has := len(fs.dirtyInodes) > 0
	fs.stateMu.Unlock()
	return has
}

func (fs *RawFS) takeDirtyInodes() []uint64 {
	fs.stateMu.Lock()
	defer fs.stateMu.Unlock()
	inodes := make([]uint64, 0, len(fs.dirtyInodes))
	for ino := range fs.dirtyInodes {
		inodes = append(inodes, ino)
	}
	clear(fs.dirtyInodes)
	return inodes
}

func (fs *RawFS) orphanBacklogThreshold() int {
	if fs.cfg.OrphanBacklogThreshold > 0 {
		return fs.cfg.OrphanBacklogThreshold
	}
	return DefaultOrphanBacklogThreshold
}

func (fs *RawFS) releaseQueueDepth() int {
	fs.releaseMu.Lock()
	defer fs.releaseMu.Unlock()
	return len(fs.releaseQueue)
}

func (fs *RawFS) warnOrphanBacklog(ctx context.Context, ino uint64, attempts, queueDepth int, err error) {
	now := time.Now().UnixNano()
	last := fs.lastOrphanWarnUnixNano.Load()
	if last != 0 && now-last < int64(orphanWarnMinInterval) {
		return
	}
	if !fs.lastOrphanWarnUnixNano.CompareAndSwap(last, now) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fs.logger().WarnContext(ctx, "orphan_reclaim_backlogged",
		"ino", ino,
		"attempts", attempts,
		"queue_depth", queueDepth,
		"error", err,
	)
}

func (fs *RawFS) markOrphanWithContext(ctx context.Context, ino uint64) {
	sc := trace.SpanContextFromContext(ctx)
	fs.stateMu.Lock()
	fs.orphanInodes[ino] = struct{}{}
	if sc.IsValid() {
		fs.orphanSpans[ino] = sc
	}
	fs.stateMu.Unlock()
	fs.handles.MarkOrphanContext(ctx, ino)
}

func (fs *RawFS) clearOrphan(ino uint64) bool {
	wasOrphan := fs.handles.IsOrphan(ino)
	fs.stateMu.Lock()
	if _, ok := fs.orphanInodes[ino]; ok {
		wasOrphan = true
		delete(fs.orphanInodes, ino)
		delete(fs.orphanSpans, ino)
	}
	fs.stateMu.Unlock()
	fs.handles.ClearOrphan(ino)
	return wasOrphan
}

func (fs *RawFS) reclaimOrphan(ctx context.Context, ino uint64) {
	fs.stateMu.Lock()
	_, exists := fs.orphanInodes[ino]
	storedSC := fs.orphanSpans[ino]
	delete(fs.orphanInodes, ino)
	delete(fs.orphanSpans, ino)
	fs.stateMu.Unlock()
	if !exists {
		return
	}
	serverIno, status := fs.ToServerIno(ino)
	if status != fuse.OK {
		return
	}
	fs.clearDirty(serverIno)
	if fs.client == nil || fs.IsFenced() {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		sc = storedSC
	}
	item := reclaimRetryItem{ino: serverIno, spanCtx: sc}
	if fs.tryReclaim(ctx, &item, false) {
		return
	}
	item.attempts = 1
	fs.releaseMu.Lock()
	if fs.releaseClosed {
		fs.releaseMu.Unlock()
		return
	}
	fs.releaseQueue = append(fs.releaseQueue, item)
	qDepth := len(fs.releaseQueue)
	if !fs.releaseStarted {
		fs.releaseStarted = true
		fs.releaseLoopDone = make(chan struct{})
		go fs.releaseRetryLoop()
	}
	fs.releaseMu.Unlock()
	if qDepth >= fs.orphanBacklogThreshold() {
		logCtx := ctx
		if !trace.SpanContextFromContext(logCtx).IsValid() && sc.IsValid() {
			logCtx = trace.ContextWithSpanContext(context.Background(), sc)
		}
		fs.warnOrphanBacklog(logCtx, serverIno, item.attempts, qDepth, nil)
	}
}

func (fs *RawFS) tryReclaim(parent context.Context, item *reclaimRetryItem, fromQueue bool) bool {
	if fs.IsFenced() {
		return true
	}
	if parent == nil {
		parent = context.Background()
	}
	if !trace.SpanContextFromContext(parent).IsValid() && item.spanCtx.IsValid() {
		parent = trace.ContextWithSpanContext(parent, item.spanCtx)
	}
	ctx, cancel := fs.RPCContextFrom(parent, nil)
	defer cancel()
	req := &storagev1.ReclaimOrphanRequest{
		DiskId: fs.cfg.DiskID,
		Epoch:  fs.cfg.Epoch,
		Ino:    item.ino,
	}
	_, err := fs.client.ReclaimOrphan(ctx, connect.NewRequest(req))
	if err == nil {
		fs.markMetaDirty()
		fs.metrics.incOrphanReclaimOK()
		if fromQueue {
			fs.metrics.incAsyncReleaseOK()
		}
		return true
	}
	fs.fenceIfStaleEpoch(err)
	fs.metrics.incOrphanReclaimError()
	if fromQueue {
		fs.metrics.incAsyncReleaseError()
	}
	fs.warnOrphanBacklog(ctx, item.ino, item.attempts+1, fs.releaseQueueDepth(), err)
	if fs.IsFenced() {
		return true
	}
	return !shouldRetryReclaim(err)
}

func (fs *RawFS) releaseRetryLoop() {
	defer close(fs.releaseLoopDone)
	ticker := time.NewTicker(DefaultReleaseRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-fs.releaseCtx.Done():
			return
		case <-ticker.C:
			fs.drainReleaseQueueOnce(fs.releaseCtx)
		}
	}
}

func (fs *RawFS) drainReleaseQueueOnce(ctx context.Context) {
	fs.releaseMu.Lock()
	queue := fs.releaseQueue
	fs.releaseQueue = nil
	fs.releaseMu.Unlock()
	for _, item := range queue {
		if ctx.Err() != nil {
			fs.releaseMu.Lock()
			fs.releaseQueue = append(fs.releaseQueue, item)
			fs.releaseMu.Unlock()
			continue
		}
		if !fs.tryReclaim(ctx, &item, true) {
			item.attempts++
			fs.releaseMu.Lock()
			fs.releaseQueue = append(fs.releaseQueue, item)
			qDepth := len(fs.releaseQueue)
			fs.releaseMu.Unlock()
			if qDepth >= fs.orphanBacklogThreshold() {
				logCtx := ctx
				if !trace.SpanContextFromContext(logCtx).IsValid() && item.spanCtx.IsValid() {
					logCtx = trace.ContextWithSpanContext(context.Background(), item.spanCtx)
				}
				fs.warnOrphanBacklog(logCtx, item.ino, item.attempts, qDepth, nil)
			}
		}
	}
}

func (fs *RawFS) stopAndDiscardReleaseQueue() {
	fs.releaseMu.Lock()
	fs.releaseClosed = true
	fs.releaseQueue = nil
	started, done := fs.releaseStarted, fs.releaseLoopDone
	fs.releaseMu.Unlock()
	fs.releaseCancel()
	if started {
		<-done
	}
}

func (fs *RawFS) stopAndDrainReleaseQueue(parentCtx context.Context) {
	fs.releaseMu.Lock()
	fs.releaseClosed = true
	started, done := fs.releaseStarted, fs.releaseLoopDone
	fs.releaseMu.Unlock()
	fs.releaseCancel()
	if started {
		<-done
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	if parentCtx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(parentCtx, DefaultUnmountReleaseTimeout)
	defer cancel()
	fs.drainReleaseQueueOnce(ctx)
}

func (fs *RawFS) FillAttr(out *fuse.Attr, ino *storagev1.Inode) {
	if out == nil || ino == nil {
		return
	}
	out.Ino = fs.ToKernelIno(ino.GetIno())

	attr := ino.GetAttr()
	if attr == nil {
		return
	}

	out.Size = attr.GetSize()
	out.Blocks = attr.GetBlocks()
	out.Mode = attr.GetMode()
	out.Nlink = attr.GetNlink()
	out.Uid = attr.GetUid()
	out.Gid = attr.GetGid()
	out.Rdev = attr.GetRdev()
	out.Blksize = attr.GetBlksize()

	if atime := attr.GetAtime(); atime != nil {
		out.Atime = uint64(atime.GetSeconds())
		out.Atimensec = uint32(atime.GetNanos())
	}
	if mtime := attr.GetMtime(); mtime != nil {
		out.Mtime = uint64(mtime.GetSeconds())
		out.Mtimensec = uint32(mtime.GetNanos())
	}
	if ctime := attr.GetCtime(); ctime != nil {
		out.Ctime = uint64(ctime.GetSeconds())
		out.Ctimensec = uint32(ctime.GetNanos())
	}
}

func (fs *RawFS) FillEntryOut(out *fuse.EntryOut, ino *storagev1.Inode, entryTimeoutSec, attrTimeoutSec float64) {
	if out == nil || ino == nil {
		return
	}
	out.NodeId = fs.ToKernelIno(ino.GetIno())
	out.Generation = ino.GetGeneration()
	out.SetEntryTimeout(time.Duration(entryTimeoutSec * float64(time.Second)))
	out.SetAttrTimeout(time.Duration(attrTimeoutSec * float64(time.Second)))
	fs.FillAttr(&out.Attr, ino)
}

func (fs *RawFS) FillAttrOut(out *fuse.AttrOut, ino *storagev1.Inode, attrTimeoutSec float64) {
	if out == nil || ino == nil {
		return
	}
	out.SetTimeout(time.Duration(attrTimeoutSec * float64(time.Second)))
	fs.FillAttr(&out.Attr, ino)
}

func BuildMountOptions(cfg Config) *fuse.MountOptions {
	options := []string{"default_permissions"}

	disabledCaps := uint64(fuse.CAP_WRITEBACK_CACHE | fuse.CAP_AUTO_INVAL_DATA)
	if cfg.IsReadOnly() {
		options = append(options, "ro")
		disabledCaps = fuse.CAP_WRITEBACK_CACHE
	}

	return &fuse.MountOptions{
		FsName:        "bowl:" + cfg.DiskID,
		Name:          "bowl",
		Options:       options,
		AllowOther:    cfg.AllowOther,
		EnableLocks:   false,
		DisableXAttrs: false,
		MaxWrite:      1 << 20,
		MaxReadAhead:  1 << 20,

		MaxBackground:       64,
		CongestionThreshold: 48,

		ExtraCapabilities:    fuse.CAP_HANDLE_KILLPRIV,
		DisabledCapabilities: disabledCaps,
		DirectMount:          true,
		Debug:                cfg.Debug,
	}
}
