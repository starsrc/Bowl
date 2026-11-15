package fusefs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math"
	"slices"
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
	// tmpSparsePageSize 是本地匿名文件稀疏页缓冲的单页尺寸 (4 KiB)。
	tmpSparsePageSize = 4 * 1024
	// tmpUploadChunkSize 是本地匿名文件具名化/同步上云时的最大聚合写入分片尺寸 (1 MiB)。
	tmpUploadChunkSize = 1024 * 1024
)

// TmpUploadRange 表示待上云的一段连续字节区间。
type TmpUploadRange struct {
	Offset uint64
	Data   []byte
}

// TmpMaterializer 定义本地匿名文件上云具名化（Link）与匿名落盘（Sync）的窄端口适配器。
type TmpMaterializer interface {
	CreateTmpInode(ctx context.Context, parentIno uint64, name string, mode, uid, gid uint32) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status)
	WriteChunk(ctx context.Context, serverIno, offset uint64, data []byte) fuse.Status
	CommitTmpAttr(ctx context.Context, serverIno, size uint64, atime, mtime *timestamppb.Timestamp) (*storagev1.Inode, fuse.Status)
	RollbackTmpInode(ctx context.Context, parentIno uint64, name string)
	UnlinkOpenTmpInode(ctx context.Context, parentIno uint64, name string) fuse.Status
	LinkInode(ctx context.Context, serverIno, newParentIno uint64, name string) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status)
}

type managedTmpFile struct {
	mu        sync.Mutex
	inode     *storagev1.Inode
	pages     map[uint64][]byte
	exclusive bool
	named     bool
	parent    uint64
	lookups   uint64
	serverIno uint64
	uploaded  atomic.Bool
}

// TmpFileManager 封装本地匿名文件（TmpFile）的 4 KiB 稀疏页缓冲、
// 高位本地 Inode (1 << 63) 与服务端 Inode 的双向别名映射、内部双层锁协议以及
// 内核 nlookup 与打开句柄双重归零回收逻辑。
type TmpFileManager struct {
	rootIno       uint64
	mu            sync.RWMutex
	files         map[uint64]*managedTmpFile
	serverToLocal map[uint64]uint64
	nextTmp       atomic.Uint64
	metrics       atomic.Pointer[Metrics]
}

// NewTmpFileManager 创建并初始化 TmpFileManager。
func NewTmpFileManager(rootIno uint64) *TmpFileManager {
	return &TmpFileManager{
		rootIno:       rootIno,
		files:         make(map[uint64]*managedTmpFile),
		serverToLocal: make(map[uint64]uint64),
	}
}

func (m *TmpFileManager) bindMetrics(metrics *Metrics) {
	m.metrics.Store(metrics)
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

// HasPendingSync 报告该内核 Inode 是否为尚未上云的本地匿名文件，避免常规文件 Flush 无谓构造 RPCContext。
func (m *TmpFileManager) HasPendingSync(ino uint64) bool {
	tmp := m.getFile(ino)
	return tmp != nil && !tmp.uploaded.Load()
}

func (m *TmpFileManager) getFile(ino uint64) *managedTmpFile {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.files[ino]
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

// Create 分配高位匿名文件 Inode (1<<63 | seq)，登记初始 nlookup=1 并返回克隆的 Inode 描述。
func (m *TmpFileManager) Create(parent uint64, uid, gid, mode, flags uint32) (uint64, *storagev1.Inode) {
	ino := tmpLocalInoBit | m.nextTmp.Add(1)
	now := timestamppb.Now()
	inode := &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    syscall.S_IFREG | (mode & 0o7777),
			Uid:     uid,
			Gid:     gid,
			Blksize: tmpSparsePageSize,
			Atime:   now,
			Mtime:   now,
			Ctime:   now,
		},
	}
	file := &managedTmpFile{
		inode:     inode,
		pages:     make(map[uint64][]byte),
		exclusive: flags&uint32(syscall.O_EXCL) != 0,
		parent:    parent,
		lookups:   1,
	}
	m.mu.Lock()
	m.files[ino] = file
	m.mu.Unlock()
	return ino, cloneInode(inode)
}

// ToServerIno 将内核传入的 Inode 映射为服务端持久化 Inode（未上云匿名文件返回自身高位 Inode）。
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

// ToKernelIno 将服务端持久化 Inode 映射为 Linux 内核可见的 Inode（含已具名化的匿名文件别名与 RootIno 映射）。
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

// LocalInode 若目标节点是尚未上云的本地匿名文件，返回其深拷贝 Inode 与 true。
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

// GetAttr 读取尚未上云的本地匿名文件属性并填入 fuse.Attr。
func (m *TmpFileManager) GetAttr(ino uint64, out *fuse.Attr) (bool, fuse.Status) {
	inode, ok := m.LocalInode(ino)
	if !ok {
		return false, fuse.OK
	}
	m.fillAttr(out, inode)
	return true, fuse.OK
}

// Read 从尚未上云的本地匿名文件稀疏页缓冲中读取数据，未分配的空洞区间自动零填充，超出 EOF 自动截断。
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
	for pos := uint64(0); pos < length; {
		at := offset + pos
		within := at % tmpSparsePageSize
		n := min(length-pos, tmpSparsePageSize-within)
		if page := tmp.pages[at/tmpSparsePageSize]; page != nil {
			copy(dest[pos:pos+n], page[within:within+n])
		}
		pos += n
	}
	return int(length), true, fuse.OK
}

// Write 将数据按 4 KiB 稀疏页写入尚未上云的本地匿名文件缓冲。
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
	var addedPages int64
	for pos := 0; pos < len(data); {
		at := offset + uint64(pos)
		index, within := at/tmpSparsePageSize, at%tmpSparsePageSize
		n := min(len(data)-pos, tmpSparsePageSize-int(within))
		page := tmp.pages[index]
		if page == nil {
			page = make([]byte, tmpSparsePageSize)
			tmp.pages[index] = page
			addedPages++
		}
		copy(page[within:within+uint64(n)], data[pos:pos+n])
		pos += n
	}
	if addedPages > 0 {
		m.metrics.Load().addTmpResidentPages(addedPages)
	}
	if len(data) != 0 {
		tmp.inode.Attr.Size = max(tmp.inode.Attr.Size, offset+uint64(len(data)))
		tmp.inode.Attr.Blocks = uint64(len(tmp.pages)) * (tmpSparsePageSize / 512)
		now := timestamppb.Now()
		tmp.inode.Attr.Mtime = now
		tmp.inode.Attr.Ctime = now
	}
	return uint32(len(data)), true, fuse.OK
}

// isAllZeroPage 判断单页是否全部为零字节。
func isAllZeroPage(page []byte) bool {
	for _, b := range page {
		if b != 0 {
			return false
		}
	}
	return true
}

// SetAttr 按需部分更新尚未上云的本地匿名文件属性或截断文件大小（缩小释放多余页并清理截断尾页，扩大零扩展）。
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
		if input.Size < a.Size {
			before := len(tmp.pages)
			for index, page := range tmp.pages {
				start := index * tmpSparsePageSize
				if start >= input.Size {
					delete(tmp.pages, index)
				} else if input.Size-start < tmpSparsePageSize {
					clear(page[input.Size-start:])
					if isAllZeroPage(page) {
						delete(tmp.pages, index)
					}
				}
			}
			if removed := before - len(tmp.pages); removed > 0 {
				m.metrics.Load().addTmpResidentPages(-int64(removed))
			}
		}
		a.Size = input.Size
		a.Blocks = uint64(len(tmp.pages)) * (tmpSparsePageSize / 512)
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

// Allocate 在尚未上云的本地匿名文件上执行预分配、打洞与零范围清零。
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
	if mode == fallocFlPunchHole {
		return true, fuse.EINVAL
	}
	switch mode {
	case 0,
		fallocFlKeepSize,
		fallocFlPunchHole | fallocFlKeepSize,
		fallocFlZeroRange,
		fallocFlZeroRange | fallocFlKeepSize:
	default:
		return true, fuse.Status(syscall.EOPNOTSUPP)
	}
	if length == 0 || offset > math.MaxInt64 || length > math.MaxInt64-offset {
		return true, fuse.EINVAL
	}
	a := tmp.inode.Attr
	oldSize := a.Size
	end := offset + length
	if mode&fallocFlKeepSize == 0 && end > a.Size {
		a.Size = end
	}
	zeroRange := mode&(fallocFlPunchHole|fallocFlZeroRange) != 0
	if zeroRange {
		before := len(tmp.pages)
		holeEnd := min(end, a.Size)
		for pos := offset; pos < holeEnd; {
			index := pos / tmpSparsePageSize
			within := pos % tmpSparsePageSize
			n := min(holeEnd-pos, tmpSparsePageSize-within)
			if within == 0 && n == tmpSparsePageSize {
				delete(tmp.pages, index)
			} else if page := tmp.pages[index]; page != nil {
				clear(page[within : within+n])
				if isAllZeroPage(page) {
					delete(tmp.pages, index)
				}
			}
			pos += n
		}
		if removed := before - len(tmp.pages); removed > 0 {
			m.metrics.Load().addTmpResidentPages(-int64(removed))
		}
		a.Mode &^= syscall.S_ISUID | syscall.S_ISGID
	}
	a.Blocks = uint64(len(tmp.pages)) * (tmpSparsePageSize / 512)
	now := timestamppb.Now()
	if zeroRange || a.Size > oldSize {
		a.Mtime = now
	}
	a.Ctime = now
	return true, fuse.OK
}

func (m *TmpFileManager) installAliasLocked(localIno uint64, tmp *managedTmpFile, serverIno uint64) {
	if pages := len(tmp.pages); pages > 0 {
		m.metrics.Load().addTmpResidentPages(-int64(pages))
	}
	tmp.serverIno = serverIno
	tmp.pages = nil
	tmp.uploaded.Store(true)
	m.mu.Lock()
	m.serverToLocal[serverIno] = localIno
	m.mu.Unlock()
}

// uploadTmpLocked 创建目录项、按最大 1 MiB 聚合连续 4 KiB 稀疏页写入服务端并提交属性，失败时回滚新目录项且不破坏本地缓冲。
// 调用方须持有 tmp.mu。
func (m *TmpFileManager) uploadTmpLocked(ctx context.Context, tmp *managedTmpFile, parentServerIno uint64, filename string, mat TmpMaterializer) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status) {
	attr := tmp.inode.Attr
	inode, entryTimeout, attrTimeout, status = mat.CreateTmpInode(ctx, parentServerIno, filename, attr.Mode, attr.Uid, attr.Gid)
	if status != fuse.OK {
		return nil, 0, 0, status
	}
	if inode == nil || inode.GetAttr() == nil {
		mat.RollbackTmpInode(ctx, parentServerIno, filename)
		return nil, 0, 0, fuse.EIO
	}
	serverIno := inode.GetIno()
	indices := make([]uint64, 0, len(tmp.pages))
	for index := range tmp.pages {
		if index*tmpSparsePageSize < attr.Size {
			indices = append(indices, index)
		}
	}
	slices.Sort(indices)

	for i := 0; i < len(indices); {
		startIdx := indices[i]
		startOffset := startIdx * tmpSparsePageSize
		endIdx := startIdx
		totalLen := min(uint64(tmpSparsePageSize), attr.Size-startOffset)
		j := i + 1
		for j < len(indices) && indices[j] == endIdx+1 {
			nextOffset := indices[j] * tmpSparsePageSize
			nextLen := min(uint64(tmpSparsePageSize), attr.Size-nextOffset)
			if totalLen+nextLen > tmpUploadChunkSize {
				break
			}
			totalLen += nextLen
			endIdx = indices[j]
			j++
		}
		var chunk []byte
		if j == i+1 {
			chunk = tmp.pages[startIdx][:totalLen]
		} else {
			chunk = make([]byte, 0, totalLen)
			for k := i; k < j; k++ {
				idx := indices[k]
				off := idx * tmpSparsePageSize
				n := min(uint64(tmpSparsePageSize), attr.Size-off)
				chunk = append(chunk, tmp.pages[idx][:n]...)
			}
		}
		if st := mat.WriteChunk(ctx, serverIno, startOffset, chunk); st != fuse.OK {
			mat.RollbackTmpInode(ctx, parentServerIno, filename)
			return nil, 0, 0, st
		}
		i = j
	}

	updated, st := mat.CommitTmpAttr(ctx, serverIno, attr.Size, attr.Atime, attr.Mtime)
	if st != fuse.OK {
		mat.RollbackTmpInode(ctx, parentServerIno, filename)
		return nil, 0, 0, st
	}
	if updated != nil {
		inode = updated
	}
	return inode, entryTimeout, attrTimeout, fuse.OK
}

// TmpLinkResult 承载匿名文件具名化成功后的服务端属性与超时信息。
type TmpLinkResult struct {
	ServerIno    uint64
	FirstUpload  bool
	Inode        *storagev1.Inode
	EntryTimeout float64
	AttrTimeout  float64
}

// Link 将匿名文件具名化（首次上云或将已通过 Sync 上云的孤儿节点链接到目标目录），已具名化或 O_EXCL 的匿名节点拒绝再次 Link。
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
	if tmp.exclusive || tmp.named || !isOpen {
		return TmpLinkResult{}, true, fuse.ENOENT
	}
	var (
		inode                     *storagev1.Inode
		entryTimeout, attrTimeout float64
		serverIno                 uint64
		firstUpload               bool
	)
	if tmp.serverIno == 0 {
		inode, entryTimeout, attrTimeout, st = m.uploadTmpLocked(ctx, tmp, parentServerIno, name, mat)
		if st != fuse.OK {
			return TmpLinkResult{}, true, st
		}
		serverIno = inode.GetIno()
		m.installAliasLocked(ino, tmp, serverIno)
		firstUpload = true
	} else {
		inode, entryTimeout, attrTimeout, st = mat.LinkInode(ctx, tmp.serverIno, parentServerIno, name)
		if st != fuse.OK {
			return TmpLinkResult{}, true, st
		}
		if inode == nil || inode.GetAttr() == nil {
			return TmpLinkResult{}, true, fuse.EIO
		}
		serverIno = tmp.serverIno
		firstUpload = false
	}
	tmp.named = true
	tmp.lookups++
	m.metrics.Load().incTmpPromote()
	m.fillEntryOut(out, inode, entryTimeout, attrTimeout)
	return TmpLinkResult{
		ServerIno:    serverIno,
		FirstUpload:  firstUpload,
		Inode:        inode,
		EntryTimeout: entryTimeout,
		AttrTimeout:  attrTimeout,
	}, true, fuse.OK
}

// Sync 将尚未上云的匿名文件以临时隐藏名称上云并立即解链为服务端打开孤儿节点，供 Flush / Fsync / Fallocate 完成持久化。
func (m *TmpFileManager) Sync(ctx context.Context, ino uint64, mat TmpMaterializer) (parentKernelIno uint64, tempName string, serverIno uint64, uploaded bool, status fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return 0, "", 0, false, fuse.OK
	}
	parentServerIno, st := m.ToServerIno(tmp.parent)
	if st != fuse.OK {
		return tmp.parent, "", 0, false, st
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return tmp.parent, "", tmp.serverIno, false, fuse.OK
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return tmp.parent, "", 0, false, fuse.EIO
	}
	tempName = ".bowl-tmp-" + hex.EncodeToString(random[:])
	inode, _, _, st := m.uploadTmpLocked(ctx, tmp, parentServerIno, tempName, mat)
	if st != fuse.OK {
		return tmp.parent, tempName, 0, false, st
	}
	if st := mat.UnlinkOpenTmpInode(ctx, parentServerIno, tempName); st != fuse.OK {
		mat.RollbackTmpInode(ctx, parentServerIno, tempName)
		return tmp.parent, tempName, 0, false, st
	}
	serverIno = inode.GetIno()
	m.installAliasLocked(ino, tmp, serverIno)
	return tmp.parent, tempName, serverIno, true, fuse.OK
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
	serverIno, lookups, pageCount := tmp.serverIno, tmp.lookups, len(tmp.pages)
	if !orphan && lookups != 0 {
		tmp.mu.Unlock()
		return false
	}
	m.mu.Lock()
	_, existed := m.files[ino]
	if existed {
		delete(m.files, ino)
		tmp.pages = nil
	}
	if serverIno != 0 && m.serverToLocal[serverIno] == ino {
		delete(m.serverToLocal, serverIno)
	}
	m.mu.Unlock()
	tmp.mu.Unlock()
	if existed && pageCount > 0 {
		m.metrics.Load().addTmpResidentPages(-int64(pageCount))
	}
	return existed
}

// OnForget 递减内核 nlookup 引用计数，并在 nlookup 与打开句柄双重归零时回收内存页与双向 Inode 别名。
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

// Clear 清空全部匿名文件页缓冲与双向 Inode 别名映射（供卸载时调用）。
func (m *TmpFileManager) Clear() {
	m.mu.Lock()
	detached := make([]*managedTmpFile, 0, len(m.files))
	for _, f := range m.files {
		detached = append(detached, f)
	}
	clear(m.files)
	clear(m.serverToLocal)
	m.mu.Unlock()

	var releasedPages int64
	for _, f := range detached {
		f.mu.Lock()
		releasedPages += int64(len(f.pages))
		f.pages = nil
		f.mu.Unlock()
	}
	if releasedPages > 0 {
		m.metrics.Load().addTmpResidentPages(-releasedPages)
	}
}
