package fusefs

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// DefaultReleaseRetryInterval 是待释放句柄异步重试队列的默认轮询周期。
const DefaultReleaseRetryInterval = 200 * time.Millisecond

// 确保 RawFS 完整实现 fuse.RawFileSystem 接口。
var _ fuse.RawFileSystem = (*RawFS)(nil)

// Config 包含 FUSE 文件系统运行所需的所有静态与动态配置。
type Config struct {
	DiskID               string
	RootIno              uint64
	ReadOnly             bool
	DirectIO             bool
	AllowOther           bool
	Debug                bool
	ReleaseRetryInterval time.Duration
	OnUnmount            func()
}

type releaseRetryKey struct {
	isDir     bool
	fh        uint64
	sessionID uint64
}

type releaseRetryItem struct {
	key     releaseRetryKey
	fileReq *storagev1.ReleaseRequest
	dirReq  *storagev1.ReleaseDirRequest
}

// RawFS 封装底层 FUSE 请求处理，桥接 Linux 内核 VFS 与 StorageService RPC。
type RawFS struct {
	fuse.RawFileSystem

	client    storagev1connect.StorageServiceClient
	sessions  *session.Manager
	cfg       Config
	nextReqID atomic.Uint64

	releaseMu       sync.Mutex
	releaseQueue    []releaseRetryItem
	releaseWake     chan struct{}
	releaseCtx      context.Context
	releaseCancel   context.CancelFunc
	releaseLoopDone chan struct{}
	releaseStarted  bool
	releaseClosed   bool
}

// NewRawFS 创建并初始化 RawFS 实例。
func NewRawFS(client storagev1connect.StorageServiceClient, sessions *session.Manager, cfg Config) *RawFS {
	if cfg.ReleaseRetryInterval <= 0 {
		cfg.ReleaseRetryInterval = DefaultReleaseRetryInterval
	}
	releaseCtx, releaseCancel := context.WithCancel(context.Background())
	return &RawFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		client:        client,
		sessions:      sessions,
		cfg:           cfg,
		releaseWake:   make(chan struct{}, 1),
		releaseCtx:    releaseCtx,
		releaseCancel: releaseCancel,
	}
}

// ToServerIno 将内核传入的 Inode 映射为服务端持久化 Inode。
// 严防内核越过根映射以非 1 且等于 RootIno 的编号直接访问，返回 ENOENT。
func (fs *RawFS) ToServerIno(kernelIno uint64) (uint64, fuse.Status) {
	if kernelIno == fuse.FUSE_ROOT_ID {
		return fs.cfg.RootIno, fuse.OK
	}
	if fs.cfg.RootIno != fuse.FUSE_ROOT_ID && kernelIno == fs.cfg.RootIno {
		return 0, fuse.ENOENT
	}
	return kernelIno, fuse.OK
}

// ToKernelIno 将服务端持久化 Inode 映射为 Linux 内核可见的 Inode。
// 当 serverIno 等于目标 Disk.RootIno 时，强制映射为 1 (fuse.FUSE_ROOT_ID)。
func (fs *RawFS) ToKernelIno(serverIno uint64) uint64 {
	if serverIno == fs.cfg.RootIno {
		return fuse.FUSE_ROOT_ID
	}
	return serverIno
}

// CheckWritable 检查文件系统当前是否允许写操作。只读挂载时返回 EROFS。
func (fs *RawFS) CheckWritable() fuse.Status {
	if fs.cfg.ReadOnly {
		return fuse.Status(syscall.EROFS)
	}
	return fuse.OK
}

// RPCContext 包装内核 cancel 信号通道为派生 context，支持 FUSE_INTERRUPT 及时取消。
func (fs *RawFS) RPCContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return rpc.ContextWithCancelChan(context.Background(), cancel)
}

// ToStatus 将 ConnectRPC 错误解码为对应的 Linux fuse.Status。
func (fs *RawFS) ToStatus(err error, cancel <-chan struct{}) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	errno := rpc.MapErrorWithCancel(err, cancel)
	return fuse.Status(errno)
}

// ResolveHandle 查询并校验文件句柄 fh 所属的会话 ID。
// 若句柄不存在或其所属会话已被淘汰，返回 (0, fuse.EBADF)。
func (fs *RawFS) ResolveHandle(fh uint64) (uint64, fuse.Status) {
	if fs.sessions == nil {
		return 0, fuse.EBADF
	}
	sid, err := fs.sessions.ResolveHandleSession(fh)
	if err != nil {
		return 0, fuse.EBADF
	}
	return sid, fuse.OK
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

// OnUnmount 在文件系统卸载完成时被调用。
func (fs *RawFS) OnUnmount() {
	fs.stopAndDrainReleaseQueue()
	if fs.cfg.OnUnmount != nil {
		fs.cfg.OnUnmount()
	}
}

// shouldEnqueueReleaseRetry 判断 Release / ReleaseDir 失败后是否应入队异步重试。
// 当服务端返回 EBADF / ENOENT / EINVAL 时，说明句柄或节点已不存在（已完成销毁或不可恢复），视为已完成销毁。
func shouldEnqueueReleaseRetry(err error) bool {
	if err == nil {
		return false
	}
	errno := rpc.MapError(err)
	switch errno {
	case 0, syscall.EBADF, syscall.ENOENT, syscall.EINVAL:
		return false
	default:
		return true
	}
}

func (fs *RawFS) enqueueReleaseFileRetry(req *storagev1.ReleaseRequest, sessionID uint64) {
	if req == nil {
		return
	}
	var lockOwnerPtr *uint64
	if req.LockOwner != nil {
		lo := *req.LockOwner
		lockOwnerPtr = &lo
	}
	var sidPtr *uint64
	if req.SessionId != nil {
		sid := *req.SessionId
		sidPtr = &sid
	}
	cloned := &storagev1.ReleaseRequest{
		Ino:          req.GetIno(),
		Fh:           req.GetFh(),
		LockOwner:    lockOwnerPtr,
		ReleaseFlags: req.GetReleaseFlags(),
		SessionId:    sidPtr,
	}
	fs.enqueueReleaseRetryItem(releaseRetryItem{
		key: releaseRetryKey{
			isDir:     false,
			fh:        req.GetFh(),
			sessionID: sessionID,
		},
		fileReq: cloned,
	})
}

func (fs *RawFS) enqueueReleaseDirRetry(req *storagev1.ReleaseDirRequest, sessionID uint64) {
	if req == nil {
		return
	}
	var sidPtr *uint64
	if req.SessionId != nil {
		sid := *req.SessionId
		sidPtr = &sid
	}
	cloned := &storagev1.ReleaseDirRequest{
		Ino:       req.GetIno(),
		Fh:        req.GetFh(),
		SessionId: sidPtr,
	}
	fs.enqueueReleaseRetryItem(releaseRetryItem{
		key: releaseRetryKey{
			isDir:     true,
			fh:        req.GetFh(),
			sessionID: sessionID,
		},
		dirReq: cloned,
	})
}

func (fs *RawFS) enqueueReleaseRetryItem(item releaseRetryItem) {
	fs.releaseMu.Lock()
	if fs.releaseClosed {
		fs.releaseMu.Unlock()
		return
	}
	for i := range fs.releaseQueue {
		if fs.releaseQueue[i].key == item.key {
			fs.releaseQueue[i] = item
			fs.releaseMu.Unlock()
			return
		}
	}
	fs.releaseQueue = append(fs.releaseQueue, item)
	if !fs.releaseStarted {
		fs.releaseStarted = true
		fs.releaseLoopDone = make(chan struct{})
		go fs.releaseRetryLoop()
	}
	fs.releaseMu.Unlock()

	select {
	case fs.releaseWake <- struct{}{}:
	default:
	}
}

func (fs *RawFS) releaseRetryLoop() {
	defer close(fs.releaseLoopDone)

	interval := fs.cfg.ReleaseRetryInterval
	if interval <= 0 {
		interval = DefaultReleaseRetryInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-fs.releaseCtx.Done():
			return
		case <-fs.releaseWake:
			select {
			case <-fs.releaseCtx.Done():
				return
			case <-ticker.C:
			}
		case <-ticker.C:
		}

		fs.drainReleaseQueueOnce(fs.releaseCtx)
	}
}

func (fs *RawFS) drainReleaseQueueOnce(parentCtx context.Context) {
	fs.releaseMu.Lock()
	if len(fs.releaseQueue) == 0 {
		fs.releaseMu.Unlock()
		return
	}
	snapshot := make([]releaseRetryItem, len(fs.releaseQueue))
	copy(snapshot, fs.releaseQueue)
	fs.releaseMu.Unlock()

	completed := make(map[releaseRetryKey]struct{})
	for _, item := range snapshot {
		if parentCtx.Err() != nil {
			break
		}
		if fs.sessions != nil && item.key.sessionID != 0 {
			if fs.sessions.CurrentSessionID() != item.key.sessionID {
				completed[item.key] = struct{}{}
				continue
			}
		}
		if fs.client == nil {
			completed[item.key] = struct{}{}
			continue
		}

		reqCtx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
		var err error
		if item.key.isDir {
			_, err = fs.client.ReleaseDir(reqCtx, connect.NewRequest(item.dirReq))
		} else {
			_, err = fs.client.Release(reqCtx, connect.NewRequest(item.fileReq))
		}
		cancel()

		if !shouldEnqueueReleaseRetry(err) {
			completed[item.key] = struct{}{}
		}
	}

	if len(completed) == 0 {
		return
	}

	fs.releaseMu.Lock()
	remaining := fs.releaseQueue[:0]
	for _, item := range fs.releaseQueue {
		if _, done := completed[item.key]; !done {
			remaining = append(remaining, item)
		}
	}
	fs.releaseQueue = remaining
	fs.releaseMu.Unlock()
}

func (fs *RawFS) stopAndDrainReleaseQueue() {
	fs.releaseMu.Lock()
	alreadyClosed := fs.releaseClosed
	fs.releaseClosed = true
	started := fs.releaseStarted
	loopDone := fs.releaseLoopDone
	fs.releaseMu.Unlock()

	if !alreadyClosed && fs.releaseCancel != nil {
		fs.releaseCancel()
	}
	if started && loopDone != nil {
		<-loopDone
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
func BuildMountOptions(cfg Config) *fuse.MountOptions {
	options := []string{"default_permissions"}
	if cfg.ReadOnly {
		options = append(options, "ro")
	}

	return &fuse.MountOptions{
		FsName:               "bowl:" + cfg.DiskID,
		Name:                 "bowl",
		Options:              options,
		AllowOther:           cfg.AllowOther,
		EnableLocks:          true,
		DisableXAttrs:        false,
		DisabledCapabilities: fuse.CAP_WRITEBACK_CACHE,
		MaxWrite:             1 << 20,
		MaxReadAhead:         1 << 20,
		DirectMount:          true,
		Debug:                cfg.Debug,
	}
}
