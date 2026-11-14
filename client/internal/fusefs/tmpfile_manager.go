package fusefs

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// tmpLocalInoBit 是客户端本地匿名文件 Inode 的最高位标识 (1 << 63)。
	tmpLocalInoBit = uint64(1) << 63
	// tmpDefaultBlksize 是匿名文件属性返回的块尺寸 (4 KiB)。
	tmpDefaultBlksize = 4 * 1024
)

// TmpMaterializer 定义本地匿名文件通过 linkat 具名化转正时的服务端建项与后台接管端口。
type TmpMaterializer interface {
	CreateTmpInode(ctx context.Context, parentIno uint64, name string, mode, uid, gid uint32) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status)
	PromoteTmpfile(ctx context.Context, serverIno, nodeID uint64, file *os.File, size uint64, atime, mtime *timestamppb.Timestamp, extents []stageExtent)
}

type managedTmpFile struct {
	mu            sync.Mutex
	inode         *storagev1.Inode
	file          *os.File
	residentBytes uint64
	exclusive     bool
	named         bool
	parent        uint64
	lookups       uint64
	serverIno     uint64
	uploaded      atomic.Bool
}

// TmpFileManager 封装宿主机暂存盘匿名文件（O_TMPFILE / CreateTemp+unlink）、
// 高位本地 Inode (1 << 63) 与服务端 Inode 的双向别名映射，以及 nlookup 与句柄双重归零回收逻辑。
type TmpFileManager struct {
	rootIno       uint64
	stageDir      string
	mu            sync.RWMutex
	files         map[uint64]*managedTmpFile
	serverToLocal map[uint64]uint64
	nextTmp       atomic.Uint64
	metrics       atomic.Pointer[Metrics]
}

// NewTmpFileManager 创建并初始化基于宿主机磁盘的 TmpFileManager。
func NewTmpFileManager(rootIno uint64, stageDir string) *TmpFileManager {
	return &TmpFileManager{
		rootIno:       rootIno,
		stageDir:      stageDir,
		files:         make(map[uint64]*managedTmpFile),
		serverToLocal: make(map[uint64]uint64),
	}
}

func (m *TmpFileManager) bindMetrics(metrics *Metrics) {
	m.metrics.Store(metrics)
}

func hostErrToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	if errno, ok := errors.AsType[syscall.Errno](err); ok {
		return fuse.Status(errno)
	}
	return fuse.EIO
}

func (m *TmpFileManager) activeLocalFiles() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var count int
	for _, f := range m.files {
		if !f.uploaded.Load() {
			count++
		}
	}
	return count
}

// IsUnpromoted 报告该内核 Inode 是否为尚未具名化的本地匿名文件。
func (m *TmpFileManager) IsUnpromoted(ino uint64) bool {
	tmp := m.getFile(ino)
	return tmp != nil && !tmp.uploaded.Load()
}

func (m *TmpFileManager) getFile(ino uint64) *managedTmpFile {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.files[ino]
}

func (m *TmpFileManager) refreshResidentBytesLocked(tmp *managedTmpFile) {
	if tmp == nil {
		return
	}
	newBytes := hostFileAllocatedBytes(tmp.file)
	delta := int64(newBytes) - int64(tmp.residentBytes)
	tmp.residentBytes = newBytes
	if delta != 0 {
		m.metrics.Load().addTmpResidentBytes(delta)
	}
}

func (m *TmpFileManager) releaseResidentBytesLocked(tmp *managedTmpFile) {
	if tmp == nil || tmp.residentBytes == 0 {
		return
	}
	m.metrics.Load().addTmpResidentBytes(-int64(tmp.residentBytes))
	tmp.residentBytes = 0
}

// fillAttr 将 storagev1.Inode 转换为 fuse.Attr。
func (m *TmpFileManager) fillAttr(out *fuse.Attr, ino *storagev1.Inode) {
	if out == nil || ino == nil {
		return
	}
	out.Ino = m.ToKernelIno(ino.GetIno())
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

func (m *TmpFileManager) fillEntryOut(out *fuse.EntryOut, ino *storagev1.Inode, entryTimeoutSec, attrTimeoutSec float64) {
	if out == nil || ino == nil {
		return
	}
	out.NodeId = m.ToKernelIno(ino.GetIno())
	out.Generation = ino.GetGeneration()
	out.SetEntryTimeout(time.Duration(entryTimeoutSec * float64(time.Second)))
	out.SetAttrTimeout(time.Duration(attrTimeoutSec * float64(time.Second)))
	m.fillAttr(&out.Attr, ino)
}

// Create 在宿主机暂存目录创建匿名磁盘文件并分配高位 Inode (1<<63 | seq)，登记初始 nlookup=1。
func (m *TmpFileManager) Create(parent uint64, uid, gid, mode, flags uint32) (uint64, *storagev1.Inode, fuse.Status) {
	f, err := openHostTmpfile(m.stageDir)
	if err != nil {
		return 0, nil, hostErrToStatus(err)
	}
	ino := tmpLocalInoBit | m.nextTmp.Add(1)
	now := timestamppb.Now()
	inode := &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    syscall.S_IFREG | (mode & 0o7777),
			Uid:     uid,
			Gid:     gid,
			Blksize: tmpDefaultBlksize,
			Atime:   now,
			Mtime:   now,
			Ctime:   now,
		},
	}
	file := &managedTmpFile{
		inode:     inode,
		file:      f,
		exclusive: flags&uint32(syscall.O_EXCL) != 0,
		parent:    parent,
		lookups:   1,
	}
	m.mu.Lock()
	m.files[ino] = file
	m.mu.Unlock()
	return ino, cloneInode(inode), fuse.OK
}

// ToServerIno 将内核传入的 Inode 映射为服务端持久化 Inode（未具名匿名文件返回自身高位 Inode）。
func (m *TmpFileManager) ToServerIno(kernelIno uint64) (uint64, fuse.Status) {
	tmp := m.getFile(kernelIno)
	if tmp != nil {
		tmp.mu.Lock()
		serverIno := tmp.serverIno
		tmp.mu.Unlock()
		if serverIno != 0 {
			return serverIno, fuse.OK
		}
		return kernelIno, fuse.OK
	}
	if kernelIno&tmpLocalInoBit != 0 {
		return 0, fuse.ENOENT
	}
	if kernelIno == fuse.FUSE_ROOT_ID {
		return m.rootIno, fuse.OK
	}
	if m.rootIno != 0 && m.rootIno != fuse.FUSE_ROOT_ID && kernelIno == m.rootIno {
		return 0, fuse.ENOENT
	}
	return kernelIno, fuse.OK
}

// ToKernelIno 将服务端持久化 Inode 映射为 Linux 内核可见的 Inode（含已具名化匿名文件别名与 RootIno 映射）。
func (m *TmpFileManager) ToKernelIno(serverIno uint64) uint64 {
	m.mu.RLock()
	localIno := m.serverToLocal[serverIno]
	m.mu.RUnlock()
	if localIno != 0 {
		return localIno
	}
	if m.rootIno != 0 && serverIno == m.rootIno {
		return fuse.FUSE_ROOT_ID
	}
	return serverIno
}

// ObserveKernelLookup 原子完成服务端或内核 Inode 向内核可见 Inode 的转换，并在命中匿名节点时递增其内核 nlookup 引用计数。
func (m *TmpFileManager) ObserveKernelLookup(ino uint64) uint64 {
	kernelIno := m.ToKernelIno(ino)
	m.AddLookup(kernelIno)
	return kernelIno
}

// AddLookup 在目标内核 Inode 是受管匿名文件时递增其 nlookup 引用计数。
func (m *TmpFileManager) AddLookup(kernelIno uint64) {
	tmp := m.getFile(kernelIno)
	if tmp == nil {
		return
	}
	tmp.mu.Lock()
	tmp.lookups++
	tmp.mu.Unlock()
}

// LocalInode 若目标节点是尚未具名化的本地匿名文件，返回其深拷贝 Inode 与 true。
func (m *TmpFileManager) LocalInode(ino uint64) (*storagev1.Inode, bool) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return nil, false
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return nil, false
	}
	return cloneInode(tmp.inode), true
}

// GetAttr 读取尚未具名化的本地匿名文件属性并填入 fuse.Attr。
func (m *TmpFileManager) GetAttr(ino uint64, out *fuse.Attr) (bool, fuse.Status) {
	inode, ok := m.LocalInode(ino)
	if !ok {
		return false, fuse.OK
	}
	m.fillAttr(out, inode)
	return true, fuse.OK
}

// Read 从尚未具名化的宿主机匿名文件读取数据，超出 EOF 自动截断。
func (m *TmpFileManager) Read(ino, offset uint64, dest []byte) (n int, handled bool, status fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return 0, false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return 0, false, fuse.OK
	}
	if len(dest) == 0 || offset >= tmp.inode.Attr.Size {
		return 0, true, fuse.OK
	}
	length := min(uint64(len(dest)), tmp.inode.Attr.Size-offset)
	clear(dest[:length])
	if tmp.file != nil {
		if _, err := readAtFull(tmp.file, dest[:length], int64(offset)); err != nil {
			return 0, true, hostErrToStatus(err)
		}
	}
	return int(length), true, fuse.OK
}

// Write 将数据写入尚未具名化的宿主机匿名文件，不占用 --stage-size 配额。
func (m *TmpFileManager) Write(ino, offset uint64, data []byte, appendMode bool) (written uint32, handled bool, status fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return 0, false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return 0, false, fuse.OK
	}
	if appendMode {
		offset = tmp.inode.Attr.Size
	}
	if offset > math.MaxInt64 || uint64(len(data)) > math.MaxInt64-offset {
		return 0, true, fuse.Status(syscall.EFBIG)
	}
	if len(data) == 0 {
		return 0, true, fuse.OK
	}
	if tmp.file == nil {
		return 0, true, fuse.EIO
	}
	if err := writeAtFull(tmp.file, data, int64(offset)); err != nil {
		return 0, true, hostErrToStatus(err)
	}
	m.refreshResidentBytesLocked(tmp)
	tmp.inode.Attr.Size = max(tmp.inode.Attr.Size, offset+uint64(len(data)))
	tmp.inode.Attr.Blocks = tmp.residentBytes / 512
	now := timestamppb.Now()
	tmp.inode.Attr.Mtime = now
	tmp.inode.Attr.Ctime = now
	return uint32(len(data)), true, fuse.OK
}

// SetAttr 更新尚未具名化的本地匿名文件属性或截断宿主机磁盘文件尺寸。
func (m *TmpFileManager) SetAttr(ino uint64, input *fuse.SetAttrIn, checkHandle func(fh uint64, needWrite bool) fuse.Status, out *fuse.Attr) (*storagev1.Inode, bool, fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return nil, false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return nil, false, fuse.OK
	}
	if input.Valid&fuse.FATTR_FH != 0 && checkHandle != nil {
		if status := checkHandle(input.Fh, input.Valid&fuse.FATTR_SIZE != 0); status != fuse.OK {
			return nil, true, status
		}
	}
	a := tmp.inode.Attr
	if input.Valid&fuse.FATTR_SIZE != 0 {
		if input.Size > math.MaxInt64 {
			return nil, true, fuse.Status(syscall.EFBIG)
		}
		if tmp.file == nil {
			return nil, true, fuse.EIO
		}
		if err := tmp.file.Truncate(int64(input.Size)); err != nil {
			return nil, true, hostErrToStatus(err)
		}
		m.refreshResidentBytesLocked(tmp)
		a.Size = input.Size
		a.Blocks = tmp.residentBytes / 512
		a.Mtime = timestamppb.Now()
	}
	if input.Valid&fuse.FATTR_MODE != 0 {
		a.Mode = (a.Mode & syscall.S_IFMT) | (input.Mode & 0o7777)
	}
	if input.Valid&fuse.FATTR_UID != 0 {
		a.Uid = input.Uid
	}
	if input.Valid&fuse.FATTR_GID != 0 {
		a.Gid = input.Gid
	}
	if input.Valid&fuse.FATTR_ATIME_NOW != 0 {
		a.Atime = timestamppb.Now()
	} else if input.Valid&fuse.FATTR_ATIME != 0 {
		a.Atime = timestamppb.New(time.Unix(int64(input.Atime), int64(input.Atimensec)))
	}
	if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
		a.Mtime = timestamppb.Now()
	} else if input.Valid&fuse.FATTR_MTIME != 0 {
		a.Mtime = timestamppb.New(time.Unix(int64(input.Mtime), int64(input.Mtimensec)))
	}
	if input.Valid&(fuse.FATTR_SIZE|fuse.FATTR_MODE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW|fuse.FATTR_CTIME) != 0 {
		a.Ctime = timestamppb.Now()
	}
	cloned := cloneInode(tmp.inode)
	m.fillAttr(out, cloned)
	return cloned, true, fuse.OK
}

// Allocate 在尚未具名化的宿主机匿名文件上执行预分配或打洞（支持 EOPNOTSUPP 写零回退）。
func (m *TmpFileManager) Allocate(ino, offset, length uint64, mode uint32) (bool, fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return false, fuse.OK
	}
	if mode != 0 &&
		mode != fallocFlKeepSize &&
		mode != (fallocFlPunchHole|fallocFlKeepSize) {
		return true, fuse.Status(syscall.EOPNOTSUPP)
	}
	if length == 0 || offset > math.MaxInt64 || length > math.MaxInt64-offset {
		return true, fuse.EINVAL
	}
	if tmp.file == nil {
		return true, fuse.EIO
	}
	a := tmp.inode.Attr
	oldSize := a.Size
	if err := fallocateHostFile(tmp.file, mode, int64(offset), int64(length), oldSize); err != nil {
		return true, hostErrToStatus(err)
	}
	end := offset + length
	if mode&fallocFlKeepSize == 0 && end > a.Size {
		a.Size = end
	}
	punch := mode&fallocFlPunchHole != 0
	if punch {
		a.Mode &^= syscall.S_ISUID | syscall.S_ISGID
	}
	m.refreshResidentBytesLocked(tmp)
	a.Blocks = tmp.residentBytes / 512
	now := timestamppb.Now()
	if punch || a.Size > oldSize {
		a.Mtime = now
	}
	a.Ctime = now
	return true, fuse.OK
}

// Fsync 对尚未具名化的宿主机匿名文件执行本地 f.Sync()，零服务端 RPC。
func (m *TmpFileManager) Fsync(ino uint64) (bool, fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return false, fuse.OK
	}
	if tmp.file == nil {
		return true, fuse.EIO
	}
	if err := tmp.file.Sync(); err != nil {
		return true, hostErrToStatus(err)
	}
	return true, fuse.OK
}

func (m *TmpFileManager) installAliasLocked(localIno uint64, tmp *managedTmpFile, serverIno uint64) {
	m.releaseResidentBytesLocked(tmp)
	tmp.serverIno = serverIno
	tmp.file = nil
	tmp.uploaded.Store(true)
	m.mu.Lock()
	m.serverToLocal[serverIno] = localIno
	m.mu.Unlock()
}

// TmpLinkResult 承载匿名文件具名化成功后的服务端属性与超时信息。
type TmpLinkResult struct {
	ServerIno    uint64
	Inode        *storagev1.Inode
	EntryTimeout float64
	AttrTimeout  float64
}

// Link 通过同步 Mknod 在服务端分配 serverIno 与目录项，并将宿主机 *os.File 所有权移交至 StageCoordinator 后台上传。
// 已具名化或携带 O_EXCL 的匿名节点拒绝再次 Link。
func (m *TmpFileManager) Link(ctx context.Context, ino, newParentKernelIno uint64, name string, isOpen bool, mat TmpMaterializer, out *fuse.EntryOut) (res TmpLinkResult, handled bool, status fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return TmpLinkResult{}, false, fuse.OK
	}
	parentServerIno, st := m.ToServerIno(newParentKernelIno)
	if st != fuse.OK {
		return TmpLinkResult{}, true, st
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.exclusive || tmp.named || !isOpen || tmp.serverIno != 0 {
		return TmpLinkResult{}, true, fuse.ENOENT
	}
	if tmp.file == nil {
		return TmpLinkResult{}, true, fuse.EIO
	}

	attr := tmp.inode.Attr
	extents, extErr := findHostDataExtents(tmp.file, attr.Size)
	if extErr != nil {
		return TmpLinkResult{}, true, hostErrToStatus(extErr)
	}

	inode, entryTimeout, attrTimeout, status := mat.CreateTmpInode(ctx, parentServerIno, name, attr.Mode, attr.Uid, attr.Gid)
	if status != fuse.OK {
		return TmpLinkResult{}, true, status
	}
	if inode == nil || inode.GetAttr() == nil {
		return TmpLinkResult{}, true, fuse.EIO
	}

	serverIno := inode.GetIno()
	mergedInode := cloneInode(inode)
	mergedInode.Attr.Size = attr.Size
	mergedInode.Attr.Blocks = max(mergedInode.Attr.Blocks, (attr.Size+511)/512)
	if attr.Atime != nil {
		mergedInode.Attr.Atime = cloneTimestamp(attr.Atime)
	}
	if attr.Mtime != nil {
		mergedInode.Attr.Mtime = cloneTimestamp(attr.Mtime)
	}
	if attr.Ctime != nil {
		mergedInode.Attr.Ctime = cloneTimestamp(attr.Ctime)
	}
	mergedInode.Attr.Nlink = 1

	hostFile := tmp.file
	m.installAliasLocked(ino, tmp, serverIno)
	tmp.named = true
	tmp.lookups++
	mat.PromoteTmpfile(ctx, serverIno, ino, hostFile, attr.Size, attr.Atime, attr.Mtime, extents)
	m.metrics.Load().incTmpPromote()

	m.fillEntryOut(out, mergedInode, entryTimeout, attrTimeout)
	return TmpLinkResult{
		ServerIno:    serverIno,
		Inode:        mergedInode,
		EntryTimeout: entryTimeout,
		AttrTimeout:  attrTimeout,
	}, true, fuse.OK
}

func (m *TmpFileManager) dropIfUnused(ino uint64, orphan bool, hasOpenHandles func(uint64) bool) bool {
	if hasOpenHandles != nil && hasOpenHandles(ino) {
		return false
	}
	tmp := m.getFile(ino)
	if tmp == nil {
		return false
	}
	tmp.mu.Lock()
	serverIno, lookups := tmp.serverIno, tmp.lookups
	if !orphan && lookups != 0 {
		tmp.mu.Unlock()
		return false
	}
	m.mu.Lock()
	_, existed := m.files[ino]
	if existed {
		delete(m.files, ino)
	}
	if serverIno != 0 && m.serverToLocal[serverIno] == ino {
		delete(m.serverToLocal, serverIno)
	}
	m.mu.Unlock()

	var f *os.File
	if existed {
		m.releaseResidentBytesLocked(tmp)
		f = tmp.file
		tmp.file = nil
	}
	tmp.mu.Unlock()
	if f != nil {
		_ = f.Close()
	}
	return existed
}

// OnForget 递减内核 nlookup 引用计数，并在 nlookup 与打开句柄双重归零时关闭宿主机匿名文件并清理别名。
func (m *TmpFileManager) OnForget(ino, nlookup uint64, hasOpenHandles func(uint64) bool) bool {
	tmp := m.getFile(ino)
	if tmp == nil {
		return false
	}
	tmp.mu.Lock()
	if nlookup >= tmp.lookups {
		tmp.lookups = 0
	} else {
		tmp.lookups -= nlookup
	}
	tmp.mu.Unlock()
	return m.dropIfUnused(ino, false, hasOpenHandles)
}

// OnHandleReleased 在文件句柄释放后检查该节点是否满足双重归零（或孤儿无句柄）回收条件。
func (m *TmpFileManager) OnHandleReleased(ino uint64, orphan bool, hasOpenHandles func(uint64) bool) bool {
	return m.dropIfUnused(ino, orphan, hasOpenHandles)
}

// Clear 关闭所有尚未转正的宿主机匿名文件并清空双向别名映射（供卸载时调用）。
func (m *TmpFileManager) Clear() {
	m.mu.Lock()
	detached := make([]*managedTmpFile, 0, len(m.files))
	for _, f := range m.files {
		detached = append(detached, f)
	}
	clear(m.files)
	clear(m.serverToLocal)
	m.mu.Unlock()

	var filesToClose []*os.File
	for _, tmp := range detached {
		tmp.mu.Lock()
		m.releaseResidentBytesLocked(tmp)
		if tmp.file != nil {
			filesToClose = append(filesToClose, tmp.file)
			tmp.file = nil
		}
		tmp.mu.Unlock()
	}
	for _, f := range filesToClose {
		_ = f.Close()
	}
}
