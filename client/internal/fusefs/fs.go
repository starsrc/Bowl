// Package fusefs 实现基于 go-fuse RawFileSystem 的 Bowl 用户态文件系统适配层。
package fusefs

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"reflect"
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

// DefaultReleaseRetryInterval 是孤儿回收异步重试的默认轮询周期。
const DefaultReleaseRetryInterval = 200 * time.Millisecond

// DefaultRPCTimeout 是 FUSE 常规请求（不含阻塞锁 SetLkw）的默认单请求超时预算。
const DefaultRPCTimeout = 30 * time.Second

// DefaultWriteAttrTimeoutSeconds 是写操作成功后回填内核属性缓存的有效期。
// 严禁置 0：置 0 会使内核在连续 write() 前重新发起 GetAttr RPC 核对文件尺寸。
const DefaultWriteAttrTimeoutSeconds = 1.0

// DefaultOrphanBacklogThreshold 是触发孤儿回收积压告警的默认队列深度阈值。
const DefaultOrphanBacklogThreshold = 16

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

// 确保 RawFS 完整实现 fuse.RawFileSystem 接口。
var _ fuse.RawFileSystem = (*RawFS)(nil)

// Config 包含 FUSE 文件系统运行所需的所有静态与动态配置。
type Config struct {
	DiskID                 string
	Epoch                  uint64
	RootIno                uint64
	RootInode              *storagev1.Inode
	EntryTimeoutSeconds    float64
	AttrTimeoutSeconds     float64
	ReadOnly               bool
	DirectIO               bool
	AllowOther             bool
	Debug                  bool
	EnableStatsFile        bool
	OrphanBacklogThreshold int
	StageSize              uint64
	StageDir               string
	StageSession           *StageDirSession
	Logger                 *slog.Logger
	Metrics                *Metrics
	TracerProvider         trace.TracerProvider
}

type reclaimRetryItem struct {
	ino      uint64
	spanCtx  trace.SpanContext
	attempts int
}

// RawFS 封装底层 FUSE 请求处理，桥接 Linux 内核 VFS 与 StorageService RPC。
type RawFS struct {
	fuse.RawFileSystem

	client  storagev1connect.StorageServiceClient
	locks   *LockTable
	handles *HandleTable
	cache   *NamespaceCache
	caps    *capabilityCache
	// lifecycleMu 保证 OnUnmount 等待所有在途 FUSE 请求排空；常规请求只持读锁。
	lifecycleMu sync.RWMutex
	// stateMu 仅保护本地内存态的微秒级读写，绝不包裹跨机 RPC。
	stateMu        sync.Mutex
	dirtyInodes    map[uint64]struct{}
	orphanInodes   map[uint64]struct{}
	orphanSpans    map[uint64]trace.SpanContext
	statsSnapshots map[uint64][]byte
	// inodeLocks 按 Inode 粒度串行化 O_APPEND 定序与 Unlink/Release 孤儿标记。
	inodeLocks inodeLocks
	tmpMgr     *TmpFileManager
	cfg        Config
	metrics    *Metrics
	tracer     trace.Tracer
	nextReqID  atomic.Uint64

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
}

// NewRawFS 创建并初始化 RawFS 实例。
//
//nolint:gocritic // Config 按值传入，RawFS 持有独立副本，避免调用方后续突变
func NewRawFS(client storagev1connect.StorageServiceClient, cfg Config) *RawFS {
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
	if !cfg.ReadOnly {
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
		locks:              NewLockTable(),
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
	}
	fs.cache.configure(fuse.FUSE_ROOT_ID, cfg.EntryTimeoutSeconds, cfg.AttrTimeoutSeconds)
	if cfg.RootInode != nil && cfg.RootInode.GetAttr() != nil {
		fs.cache.putInode(fuse.FUSE_ROOT_ID, cfg.RootInode, cfg.EntryTimeoutSeconds, cfg.AttrTimeoutSeconds)
	}
	if !cfg.ReadOnly {
		var onStage func(nodeID, cacheEpoch, endOffset uint64, ts *timestamppb.Timestamp)
		if cfg.AttrTimeoutSeconds > 0 {
			onStage = func(nodeID, cacheEpoch, endOffset uint64, ts *timestamppb.Timestamp) {
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
					return 0, nil, err
				}
				return resp.Msg.GetSize(), resp.Msg.GetInode(), nil
			},
			func(ctx context.Context, ino, size uint64, atime, mtime *timestamppb.Timestamp) (*storagev1.Inode, error) {
				if fs.client == nil {
					return nil, syscall.EIO
				}
				reqCtx := rpc.WithRequestID(ctx, fs.NextRequestID())
				resp, err := fs.client.SetAttr(reqCtx, connect.NewRequest(&storagev1.SetAttrRequest{
					Ino:    ino,
					Size:   &size,
					Atime:  atime,
					Mtime:  mtime,
					DiskId: fs.cfg.DiskID,
					Epoch:  fs.cfg.Epoch,
				}))
				if err != nil {
					return nil, err
				}
				return resp.Msg.GetInode(), nil
			},
			StageHooks{
				OnStage: onStage,
				OnCommit: func(nodeID, ino, cacheEpoch uint64, inode *storagev1.Inode, minSize uint64, refreshAttr bool) {
					fs.markDirty(ino)
					if inode != nil {
						fs.recordInodeSpecial(nodeID, inode)
					}
					if refreshAttr && inode != nil {
						fs.cache.refreshInodeFromWriteAt(cacheEpoch, nodeID, inode, minSize, DefaultWriteAttrTimeoutSeconds)
					}
					fs.caps.markAbsent(nodeID)
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
	fs.locks.bindMetrics(m)
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
	return fs.ToStatus(err, nil)
}

// BindMountpoint 在 FUSE 挂载完成后解析并绑定对应的 /sys/fs/fuse/connections/<dev> 目录。
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

// Metrics 返回绑定的统一指标数据源。
func (fs *RawFS) Metrics() *Metrics {
	if fs == nil {
		return nil
	}
	return fs.metrics
}

// Collector 返回可直接注册至自定义 prometheus.Registerer 的指标收集器。
func (fs *RawFS) Collector() prometheus.Collector {
	if fs == nil || fs.metrics == nil {
		return nil
	}
	return fs.metrics
}

// Snapshot 抓取当前时刻客户端 FUSE 41 操作与 9 大子系统的结构化一致性快照。
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
		activeLocks:             fs.locks.activeLocks(),
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

// Config 返回当前 RawFS 绑定的静态与握手配置副本。
func (fs *RawFS) Config() Config {
	return fs.cfg
}

// ToServerIno 将内核传入的 Inode 映射为服务端持久化 Inode。
// 严防内核越过根映射以非 1 且等于 RootIno 的编号直接访问，返回 ENOENT。
func (fs *RawFS) ToServerIno(kernelIno uint64) (uint64, fuse.Status) {
	return fs.tmpMgr.ToServerIno(kernelIno)
}

// ToKernelIno 将服务端持久化 Inode 映射为 Linux 内核可见的 Inode。
// 当 serverIno 等于目标 Disk.RootIno 时，强制映射为 1 (fuse.FUSE_ROOT_ID)。
func (fs *RawFS) ToKernelIno(serverIno uint64) uint64 {
	return fs.tmpMgr.ToKernelIno(serverIno)
}

// CheckWritable 检查文件系统当前是否允许写操作。只读挂载时返回 EROFS。
func (fs *RawFS) CheckWritable() fuse.Status {
	if fs.cfg.ReadOnly {
		return fuse.Status(syscall.EROFS)
	}
	return fuse.OK
}

// RPCContext 包装内核 cancel 信号通道、单调递增 RequestID 与请求超时为派生 context，支持 FUSE_INTERRUPT 及时取消与 30s 超时保护。
func (fs *RawFS) RPCContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return fs.RPCContextFrom(context.Background(), cancel)
}

// RPCContextFrom 基于指定父 Context 注入唯一 RequestID、30s 超时预算与可选 FUSE cancel 通道，确保跨阶段 RPC 不断链。
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

// ToStatus 将 ConnectRPC 错误解码为对应的 Linux fuse.Status。
func (fs *RawFS) ToStatus(err error, cancel <-chan struct{}) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	errno := rpc.MapErrorWithCancel(err, cancel)
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
	fs.locks.RegisterHandle(fh, flags)
	if !dir && ino != VirtualStatsIno {
		fs.stage.RegisterHandle(fh)
	}
	return fh, fuse.OK
}

// NextRequestID 原子单调递增生成在 [1, math.MaxInt64] 范围内的唯一请求 ID。
func (fs *RawFS) NextRequestID() uint64 {
	for {
		curr := fs.nextReqID.Add(1)
		id := curr & math.MaxInt64
		if id != 0 {
			return id
		}
	}
}

// Access 记录操作指标后委托内嵌 fuse.RawFileSystem 默认实现处理。
func (fs *RawFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	start := fs.metrics.beginOp(OpAccess)
	status := fs.RawFileSystem.Access(cancel, input)
	fs.metrics.endOp(OpAccess, start, status, 0)
	return status
}

// Lseek 记录操作指标后委托内嵌 fuse.RawFileSystem 默认实现处理。
func (fs *RawFS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	start := fs.metrics.beginOp(OpLseek)
	status := fs.RawFileSystem.Lseek(cancel, in, out)
	fs.metrics.endOp(OpLseek, start, status, 0)
	return status
}

// CopyFileRange 记录操作指标及传输字节数后委托内嵌 fuse.RawFileSystem 默认实现处理。
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

// Statx 记录操作指标后委托内嵌 fuse.RawFileSystem 默认实现处理。
func (fs *RawFS) Statx(cancel <-chan struct{}, input *fuse.StatxIn, out *fuse.StatxOut) fuse.Status {
	start := fs.metrics.beginOp(OpStatx)
	status := fs.RawFileSystem.Statx(cancel, input, out)
	fs.metrics.endOp(OpStatx, start, status, 0)
	return status
}

// OnUnmount 在文件系统卸载完成时被调用。
func (fs *RawFS) OnUnmount() {
	start := fs.metrics.beginOp(OpOnUnmount)
	opCtx, endSpan := fs.startFuseOp(OpOnUnmount)
	defer func() {
		endSpan(fuse.OK)
		fs.metrics.endOp(OpOnUnmount, start, fuse.OK, 0)
	}()

	fs.locks.Close()
	// 排他锁等待所有在途请求释放读锁，先排空本地暂存上传队列确保 dirtyInodes 完整，再逐一同步落盘，最后关闭 StageSession。
	fs.lifecycleMu.Lock()
	fs.stage.Clear(opCtx)
	for _, ino := range fs.takeDirtyInodes() {
		if fs.client == nil {
			continue
		}
		ctx, done := fs.RPCContextFrom(opCtx, nil)
		_, err := fs.client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
			Ino:    ino,
			DiskId: fs.cfg.DiskID,
			Epoch:  fs.cfg.Epoch,
		}))
		done()
		if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
			fs.metrics.incDirtyFlushError()
			fs.logger().WarnContext(ctx, "unmount_fsync_failed", "ino", ino, "error", err)
		} else {
			fs.metrics.incDirtyFlushOK()
		}
	}
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
	if fs.stageSession != nil {
		_ = fs.stageSession.Close()
	}
	if fs.ephemeralStageRoot != "" {
		_ = os.RemoveAll(fs.ephemeralStageRoot)
	}
	fs.lifecycleMu.Unlock()
	fs.readAhead.Clear()
	fs.stopAndDrainReleaseQueue()
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

// markDirty 记录尚未显式 Fsync 的节点。
func (fs *RawFS) markDirty(ino uint64) {
	fs.stateMu.Lock()
	fs.dirtyInodes[ino] = struct{}{}
	fs.stateMu.Unlock()
}

// clearDirty 在服务端确认落盘或节点已彻底删除后清除脏标记。
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

// takeDirtyInodes 原子取走并清空全部脏节点，供卸载时排空。
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

// markOrphanWithContext 记录待延迟回收的孤儿节点及其来源 SpanContext。
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
	if fs.client == nil {
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

// tryReclaim 返回 true 表示回收完成或错误不可重试。
func (fs *RawFS) tryReclaim(parent context.Context, item *reclaimRetryItem, fromQueue bool) bool {
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
		fs.metrics.incOrphanReclaimOK()
		if fromQueue {
			fs.metrics.incAsyncReleaseOK()
		}
		return true
	}
	fs.metrics.incOrphanReclaimError()
	if fromQueue {
		fs.metrics.incAsyncReleaseError()
	}
	fs.warnOrphanBacklog(ctx, item.ino, item.attempts+1, fs.releaseQueueDepth(), err)
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

func (fs *RawFS) stopAndDrainReleaseQueue() {
	fs.releaseMu.Lock()
	fs.releaseClosed = true
	started, done := fs.releaseStarted, fs.releaseLoopDone
	fs.releaseMu.Unlock()
	fs.releaseCancel()
	if started {
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fs.drainReleaseQueueOnce(ctx)
}

// FillAttr 精确转换 Inode 属性并填入 fuse.Attr。
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

// FillEntryOut 精确填充 Lookup 等操作所需的 fuse.EntryOut 结构。
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

// FillAttrOut 精确填充 GetAttr 等操作所需的 fuse.AttrOut 结构。
func (fs *RawFS) FillAttrOut(out *fuse.AttrOut, ino *storagev1.Inode, attrTimeoutSec float64) {
	if out == nil || ino == nil {
		return
	}
	out.SetTimeout(time.Duration(attrTimeoutSec * float64(time.Second)))
	fs.FillAttr(&out.Attr, ino)
}

// BuildMountOptions 根据 Config 构造标准化的 fuse.MountOptions。
//
//nolint:gocritic // 既有公开签名按值接收 Config，本任务不变更对外契约
func BuildMountOptions(cfg Config) *fuse.MountOptions {
	options := []string{"default_permissions"}
	if cfg.ReadOnly {
		options = append(options, "ro")
	}

	return &fuse.MountOptions{
		FsName:        "bowl:" + cfg.DiskID,
		Name:          "bowl",
		Options:       options,
		AllowOther:    cfg.AllowOther,
		EnableLocks:   true,
		DisableXAttrs: false,
		MaxWrite:      1 << 20,
		MaxReadAhead:  1 << 20,
		// 64 个后台异步请求配合 3/4 拥塞阈值，释放多线程大块顺序写与小文件并发吞吐。
		MaxBackground:       64,
		CongestionThreshold: 48,
		// 由文件系统自身在写批次中清除 security.capability，避免内核写前 GetXattr 探测。
		ExtraCapabilities: fuse.CAP_HANDLE_KILLPRIV,
		// 关闭内核 writeback 缓存，保持 1 MiB 直通写，避免 4 KiB 脏页回写与写前尺寸核对。
		// 同时禁用 CAP_AUTO_INVAL_DATA：Bowl 是单分支 SWMR（同一 (disk,branch) 同时最多 1 个可写会话，
		// 只读会话只能挂不可变 Commit），同一挂载视图内不存在第三方写入，内核页缓存只会被本地 write()
		// 更新，不会被“外部修改”。而服务端 mtime 来自 Go 时钟、内核 i_mtime 来自 file_update_time()，
		// 两者在非 fsync 路径上通常不等；保留 auto-inval 会让写后第一次 read() 先发一次 GetAttr，
		// 并因 mtime 不等整文件清空页缓存。这里只关闭 AUTO_INVAL，不开启 EXPLICIT_INVAL：
		// 尺寸变化仍由 fuse_change_attributes 的 oldsize != attr->size 分支做
		// truncate_pagecache + invalidate_inode_pages2 兜底。
		DisabledCapabilities: fuse.CAP_WRITEBACK_CACHE | fuse.CAP_AUTO_INVAL_DATA,
		DirectMount:          true,
		Debug:                cfg.Debug,
	}
}
