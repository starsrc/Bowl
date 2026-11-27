package fusefs

import (
	"context"
	"errors"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	tmpLocalInoBit = uint64(1) << 63

	tmpDefaultBlksize = 4 * 1024

	xattrCreateFlag       uint32 = 0x1
	xattrReplaceFlag      uint32 = 0x2
	maxXattrNameBytes            = 255
	maxXattrValueBytes           = 65536
	maxXattrListBytes            = 65536
	tmpExtentScanPageSize        = 4096
)

type tmpExtent struct {
	offset uint64
	length uint64
}

type TmpXattr struct {
	Name  string
	Value []byte
}

type TmpPromotedMeta struct {
	Size   uint64
	Atime  *timestamppb.Timestamp
	Mtime  *timestamppb.Timestamp
	Mode   uint32
	Gid    uint32
	Flags  uint32
	Xattrs []TmpXattr
}

type TmpMaterializer interface {
	CreateTmpInode(ctx context.Context, parentIno uint64, name string, mode, uid, gid uint32) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status)
	WriteTmpExtent(ctx context.Context, serverIno, offset uint64, data []byte) fuse.Status
	CommitTmpMeta(ctx context.Context, serverIno uint64, meta TmpPromotedMeta) (*storagev1.Inode, fuse.Status)
	RollbackTmpInode(ctx context.Context, parentIno uint64, name string)
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
	xattrs        map[string][]byte
}

type TmpFileManager struct {
	rootIno       uint64
	stageDir      string
	mu            sync.RWMutex
	files         map[uint64]*managedTmpFile
	serverToLocal map[uint64]uint64
	activeCount   atomic.Int64
	aliasCount    atomic.Int64
	nextTmp       atomic.Uint64
	metrics       atomic.Pointer[Metrics]
}

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
	if m.activeCount.Load() == 0 {
		return 0
	}
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

func (m *TmpFileManager) IsUnpromoted(ino uint64) bool {
	tmp := m.getFile(ino)
	return tmp != nil && !tmp.uploaded.Load()
}

func (m *TmpFileManager) getFile(ino uint64) *managedTmpFile {
	if ino&tmpLocalInoBit == 0 || m.activeCount.Load() == 0 {
		return nil
	}
	m.mu.RLock()
	tmp := m.files[ino]
	m.mu.RUnlock()
	return tmp
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
	m.activeCount.Store(int64(len(m.files)))
	m.mu.Unlock()
	return ino, proto.CloneOf(inode), fuse.OK
}

func (m *TmpFileManager) ToServerIno(kernelIno uint64) (uint64, fuse.Status) {
	if kernelIno&tmpLocalInoBit != 0 {
		tmp := m.getFile(kernelIno)
		if tmp == nil {
			return 0, fuse.ENOENT
		}
		tmp.mu.Lock()
		serverIno := tmp.serverIno
		tmp.mu.Unlock()
		if serverIno != 0 {
			return serverIno, fuse.OK
		}
		return kernelIno, fuse.OK
	}
	if kernelIno == fuse.FUSE_ROOT_ID {
		return m.rootIno, fuse.OK
	}
	if m.rootIno != 0 && m.rootIno != fuse.FUSE_ROOT_ID && kernelIno == m.rootIno {
		return 0, fuse.ENOENT
	}
	return kernelIno, fuse.OK
}

func (m *TmpFileManager) ToKernelIno(serverIno uint64) uint64 {
	if m.aliasCount.Load() != 0 {
		m.mu.RLock()
		localIno := m.serverToLocal[serverIno]
		m.mu.RUnlock()
		if localIno != 0 {
			return localIno
		}
	}
	if m.rootIno != 0 && serverIno == m.rootIno {
		return fuse.FUSE_ROOT_ID
	}
	return serverIno
}

func (m *TmpFileManager) ObserveKernelLookup(ino uint64) uint64 {
	kernelIno := m.ToKernelIno(ino)
	m.AddLookup(kernelIno)
	return kernelIno
}

func (m *TmpFileManager) AddLookup(kernelIno uint64) {
	tmp := m.getFile(kernelIno)
	if tmp == nil {
		return
	}
	tmp.mu.Lock()
	tmp.lookups++
	tmp.mu.Unlock()
}

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
	return proto.CloneOf(tmp.inode), true
}

func (m *TmpFileManager) GetAttr(ino uint64, out *fuse.Attr) (bool, fuse.Status) {
	inode, ok := m.LocalInode(ino)
	if !ok {
		return false, fuse.OK
	}
	fillFuseAttr(out, inode, m.ToKernelIno(inode.GetIno()))
	return true, fuse.OK
}

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

func (m *TmpFileManager) Write(ino, offset uint64, data []byte, appendMode bool) (written uint32, inode *storagev1.Inode, handled bool, status fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return 0, nil, false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return 0, nil, false, fuse.OK
	}
	if tmp.inode.Attr.Flags&ProtoFlagImmutable != 0 {
		return 0, nil, true, fuse.EPERM
	}
	if appendMode {
		offset = tmp.inode.Attr.Size
	}
	if len(data) > 0 && tmp.inode.Attr.Flags&ProtoFlagAppend != 0 && offset != tmp.inode.Attr.Size {
		return 0, nil, true, fuse.EPERM
	}
	if offset > math.MaxInt64 || uint64(len(data)) > math.MaxInt64-offset {
		return 0, nil, true, fuse.Status(syscall.EFBIG)
	}
	if len(data) == 0 {
		return 0, nil, true, fuse.OK
	}
	if tmp.file == nil {
		return 0, nil, true, fuse.EIO
	}
	if err := writeAtFull(tmp.file, data, int64(offset)); err != nil {
		return 0, nil, true, hostErrToStatus(err)
	}
	m.refreshResidentBytesLocked(tmp)
	tmp.inode.Attr.Size = max(tmp.inode.Attr.Size, offset+uint64(len(data)))
	tmp.inode.Attr.Blocks = tmp.residentBytes / 512
	tmp.inode.Attr.Mode &^= syscall.S_ISUID | syscall.S_ISGID
	delete(tmp.xattrs, securityCapabilityXattr)
	now := timestamppb.Now()
	tmp.inode.Attr.Mtime = now
	tmp.inode.Attr.Ctime = now
	return uint32(len(data)), proto.CloneOf(tmp.inode), true, fuse.OK
}

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
	const attrUpdateMask = fuse.FATTR_SIZE | fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID |
		fuse.FATTR_ATIME | fuse.FATTR_ATIME_NOW | fuse.FATTR_MTIME | fuse.FATTR_MTIME_NOW | fuse.FATTR_CTIME
	if input.Valid&attrUpdateMask != 0 {
		if a.Flags&ProtoFlagImmutable != 0 {
			return nil, true, fuse.EPERM
		}
		if a.Flags&ProtoFlagAppend != 0 && input.Valid&fuse.FATTR_SIZE != 0 {
			return nil, true, fuse.EPERM
		}
	}
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
	} else if input.Valid&(fuse.FATTR_SIZE|fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		a.Mode &^= syscall.S_ISUID | syscall.S_ISGID
	}
	if input.Valid&fuse.FATTR_UID != 0 {
		a.Uid = input.Uid
	}
	if input.Valid&fuse.FATTR_GID != 0 {
		a.Gid = input.Gid
	}
	if input.Valid&(fuse.FATTR_SIZE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_MODE) != 0 {
		delete(tmp.xattrs, securityCapabilityXattr)
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
	cloned := proto.CloneOf(tmp.inode)
	fillFuseAttr(out, cloned, m.ToKernelIno(cloned.GetIno()))
	return cloned, true, fuse.OK
}

func (m *TmpFileManager) Allocate(ino, offset, length uint64, mode uint32) (*storagev1.Inode, bool, fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return nil, false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return nil, false, fuse.OK
	}
	if mode != 0 &&
		mode != fallocFlKeepSize &&
		mode != (fallocFlPunchHole|fallocFlKeepSize) {
		return nil, true, fuse.Status(syscall.EOPNOTSUPP)
	}
	if length == 0 || offset > math.MaxInt64 || length > math.MaxInt64-offset {
		return nil, true, fuse.EINVAL
	}
	a := tmp.inode.Attr
	if a.Flags&(ProtoFlagImmutable|ProtoFlagAppend) != 0 {
		return nil, true, fuse.EPERM
	}
	if tmp.file == nil {
		return nil, true, fuse.EIO
	}
	oldSize := a.Size
	if err := fallocateHostFile(tmp.file, mode, int64(offset), int64(length), oldSize); err != nil {
		return nil, true, hostErrToStatus(err)
	}
	end := offset + length
	if mode&fallocFlKeepSize == 0 && end > a.Size {
		a.Size = end
	}
	punch := mode&fallocFlPunchHole != 0
	if punch {
		a.Mode &^= syscall.S_ISUID | syscall.S_ISGID
	}
	delete(tmp.xattrs, securityCapabilityXattr)
	m.refreshResidentBytesLocked(tmp)
	a.Blocks = tmp.residentBytes / 512
	now := timestamppb.Now()
	if punch || a.Size > oldSize {
		a.Mtime = now
	}
	a.Ctime = now
	return proto.CloneOf(tmp.inode), true, fuse.OK
}

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

func validateXattrName(name string) fuse.Status {
	if name == "" || strings.IndexByte(name, 0) >= 0 {
		return fuse.EINVAL
	}
	if len(name) > maxXattrNameBytes {
		return fuse.Status(syscall.ERANGE)
	}
	return fuse.OK
}

func (m *TmpFileManager) SetXAttr(ino uint64, name string, value []byte, flags uint32) (bool, fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return false, fuse.OK
	}
	if flags&^(xattrCreateFlag|xattrReplaceFlag) != 0 {
		return true, fuse.EINVAL
	}
	if st := validateXattrName(name); st != fuse.OK {
		return true, st
	}
	if len(value) > maxXattrValueBytes {
		return true, fuse.Status(syscall.ERANGE)
	}
	_, exists := tmp.xattrs[name]
	if exists {
		if flags&xattrCreateFlag != 0 {
			return true, fuse.Status(syscall.EEXIST)
		}
	} else {
		if flags&xattrReplaceFlag != 0 {
			return true, fuse.Status(syscall.ENODATA)
		}
		total := len(name) + 1
		for k := range tmp.xattrs {
			total += len(k) + 1
		}
		if total > maxXattrListBytes {
			return true, fuse.Status(syscall.ENOSPC)
		}
	}
	if tmp.xattrs == nil {
		tmp.xattrs = make(map[string][]byte)
	}
	tmp.xattrs[name] = slices.Clone(value)
	tmp.inode.Attr.Ctime = timestamppb.Now()
	return true, fuse.OK
}

func (m *TmpFileManager) GetXAttr(ino uint64, name string, dest []byte) (sz uint32, handled bool, status fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return 0, false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return 0, false, fuse.OK
	}
	if st := validateXattrName(name); st != fuse.OK {
		return 0, true, st
	}
	val, exists := tmp.xattrs[name]
	if !exists {
		return 0, true, fuse.Status(syscall.ENODATA)
	}
	sz, st := copyXattrValue(val, dest)
	return sz, true, st
}

func (m *TmpFileManager) ListXAttr(ino uint64, dest []byte) (sz uint32, handled bool, status fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return 0, false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return 0, false, fuse.OK
	}
	names := make([][]byte, 0, len(tmp.xattrs))
	for k := range tmp.xattrs {
		names = append(names, []byte(k))
	}
	slices.SortFunc(names, func(a, b []byte) int {
		return strings.Compare(string(a), string(b))
	})
	sz, st := formatXattrList(names, dest)
	return sz, true, st
}

func (m *TmpFileManager) RemoveXAttr(ino uint64, name string) (bool, fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return false, fuse.OK
	}
	if st := validateXattrName(name); st != fuse.OK {
		return true, st
	}
	if _, exists := tmp.xattrs[name]; !exists {
		return true, fuse.Status(syscall.ENODATA)
	}
	delete(tmp.xattrs, name)
	tmp.inode.Attr.Ctime = timestamppb.Now()
	return true, fuse.OK
}

func (m *TmpFileManager) GetFlags(ino uint64) (uint32, bool) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return 0, false
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return 0, false
	}
	return tmp.inode.Attr.Flags, true
}

func (m *TmpFileManager) SetFlags(ino uint64, flags uint32) (*storagev1.Inode, bool, fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return nil, false, fuse.OK
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return nil, false, fuse.OK
	}
	if tmp.inode.Attr.Flags&ProtoFlagImmutable != 0 && flags&ProtoFlagImmutable != 0 {
		return nil, true, fuse.EPERM
	}
	tmp.inode.Attr.Flags = flags
	tmp.inode.Attr.Ctime = timestamppb.Now()
	return proto.CloneOf(tmp.inode), true, fuse.OK
}

func (m *TmpFileManager) installAliasLocked(localIno uint64, tmp *managedTmpFile, serverIno uint64) {
	m.releaseResidentBytesLocked(tmp)
	tmp.serverIno = serverIno
	tmp.file = nil
	tmp.xattrs = nil
	tmp.uploaded.Store(true)
	m.mu.Lock()
	m.serverToLocal[serverIno] = localIno
	m.aliasCount.Store(int64(len(m.serverToLocal)))
	m.mu.Unlock()
}

type TmpLinkResult struct {
	ServerIno      uint64
	Inode          *storagev1.Inode
	EntryTimeout   float64
	AttrTimeout    float64
	HasSecurityCap bool
}

func uploadTmpExtents(ctx context.Context, file *os.File, serverIno uint64, extents []tmpExtent, mat TmpMaterializer) fuse.Status {
	if len(extents) == 0 {
		return fuse.OK
	}
	uploadOne := func(uploadCtx context.Context, ext tmpExtent) fuse.Status {
		buf := acquireStageBuf()
		defer releaseStageBuf(buf)
		n, err := readAtFull(file, buf[:ext.length], int64(ext.offset))
		if err != nil {
			return hostErrToStatus(err)
		}
		if uint64(n) < ext.length {
			return fuse.EIO
		}
		return mat.WriteTmpExtent(uploadCtx, serverIno, ext.offset, buf[:ext.length:ext.length])
	}
	if len(extents) == 1 {
		return uploadOne(ctx, extents[0])
	}

	uploadCtx, cancelUpload := context.WithCancel(ctx)
	defer cancelUpload()

	workers := min(len(extents), StageWorkers)
	var (
		nextIdx     atomic.Int64
		firstStatus atomic.Uint32
		wg          sync.WaitGroup
	)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				if uploadCtx.Err() != nil {
					return
				}
				idx := int(nextIdx.Add(1) - 1)
				if idx >= len(extents) {
					return
				}
				if st := uploadOne(uploadCtx, extents[idx]); st != fuse.OK {
					if firstStatus.CompareAndSwap(0, uint32(st)) {
						cancelUpload()
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	return fuse.Status(firstStatus.Load())
}

func (m *TmpFileManager) Link(ctx context.Context, ino, newParentKernelIno uint64, name string, isOpen bool, mat TmpMaterializer, out *fuse.EntryOut) (res TmpLinkResult, handled bool, status fuse.Status) {
	tmp := m.getFile(ino)
	if tmp == nil {
		return TmpLinkResult{}, false, fuse.OK
	}
	parentServerIno, st := m.ToServerIno(newParentKernelIno)
	if st != fuse.OK {
		return TmpLinkResult{}, true, st
	}

	var fileToClose *os.File
	defer func() {
		if fileToClose != nil {
			_ = fileToClose.Close()
		}
	}()

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

	baseMode := uint32(syscall.S_IFREG) | (attr.Mode & 0o777)
	inode, entryTimeout, attrTimeout, status := mat.CreateTmpInode(ctx, parentServerIno, name, baseMode, attr.Uid, attr.Gid)
	if status != fuse.OK {
		return TmpLinkResult{}, true, status
	}
	if inode == nil || inode.GetAttr() == nil {
		return TmpLinkResult{}, true, fuse.EIO
	}

	serverIno := inode.GetIno()
	if extStatus := uploadTmpExtents(ctx, tmp.file, serverIno, extents, mat); extStatus != fuse.OK {
		mat.RollbackTmpInode(ctx, parentServerIno, name)
		return TmpLinkResult{}, true, extStatus
	}

	var stagedXattrs []TmpXattr
	hasSecurityCap := false
	if len(tmp.xattrs) > 0 {
		keys := make([]string, 0, len(tmp.xattrs))
		for k := range tmp.xattrs {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		stagedXattrs = make([]TmpXattr, 0, len(keys))
		for _, k := range keys {
			if k == securityCapabilityXattr {
				hasSecurityCap = true
			}
			stagedXattrs = append(stagedXattrs, TmpXattr{
				Name:  k,
				Value: slices.Clone(tmp.xattrs[k]),
			})
		}
	}

	meta := TmpPromotedMeta{
		Size:   attr.Size,
		Atime:  proto.CloneOf(attr.Atime),
		Mtime:  proto.CloneOf(attr.Mtime),
		Mode:   attr.Mode,
		Gid:    attr.Gid,
		Flags:  attr.Flags,
		Xattrs: stagedXattrs,
	}
	committedInode, commitStatus := mat.CommitTmpMeta(ctx, serverIno, meta)
	if commitStatus != fuse.OK {
		mat.RollbackTmpInode(ctx, parentServerIno, name)
		return TmpLinkResult{}, true, commitStatus
	}

	var mergedInode *storagev1.Inode
	if committedInode != nil && committedInode.GetAttr() != nil {
		mergedInode = proto.CloneOf(committedInode)
	} else {
		mergedInode = proto.CloneOf(inode)
		mergedInode.Attr.Size = attr.Size
		mergedInode.Attr.Blocks = max(mergedInode.Attr.Blocks, (attr.Size+511)/512)
		mergedInode.Attr.Mode = attr.Mode
		mergedInode.Attr.Uid = attr.Uid
		mergedInode.Attr.Gid = attr.Gid
		mergedInode.Attr.Flags = attr.Flags
		if attr.Atime != nil {
			mergedInode.Attr.Atime = proto.CloneOf(attr.Atime)
		}
		if attr.Mtime != nil {
			mergedInode.Attr.Mtime = proto.CloneOf(attr.Mtime)
		}
		if attr.Ctime != nil {
			mergedInode.Attr.Ctime = proto.CloneOf(attr.Ctime)
		}
		mergedInode.Attr.Nlink = 1
	}

	fileToClose = tmp.file
	m.installAliasLocked(ino, tmp, serverIno)
	tmp.named = true
	tmp.lookups++
	m.metrics.Load().incTmpPromote()

	fillFuseEntryOut(out, mergedInode, entryTimeout, attrTimeout, ino)
	return TmpLinkResult{
		ServerIno:      serverIno,
		Inode:          mergedInode,
		EntryTimeout:   entryTimeout,
		AttrTimeout:    attrTimeout,
		HasSecurityCap: hasSecurityCap,
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
		m.activeCount.Store(int64(len(m.files)))
	}
	if serverIno != 0 && m.serverToLocal[serverIno] == ino {
		delete(m.serverToLocal, serverIno)
		m.aliasCount.Store(int64(len(m.serverToLocal)))
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

func (m *TmpFileManager) OnHandleReleased(ino uint64, orphan bool, hasOpenHandles func(uint64) bool) bool {
	return m.dropIfUnused(ino, orphan, hasOpenHandles)
}

func (m *TmpFileManager) Clear() {
	m.mu.Lock()
	detached := make([]*managedTmpFile, 0, len(m.files))
	for _, f := range m.files {
		detached = append(detached, f)
	}
	clear(m.files)
	clear(m.serverToLocal)
	m.activeCount.Store(0)
	m.aliasCount.Store(0)
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

func createAndUnlinkTempFile(dir string) (*os.File, error) {
	f, err := os.CreateTemp(dir, "tmpfile-*")
	if err != nil {
		return nil, err
	}
	if rmErr := os.Remove(f.Name()); rmErr != nil {
		_ = f.Close()
		return nil, rmErr
	}
	return f, nil
}

func fallocateHostFileFallback(f *os.File, mode uint32, offset, length int64, curSize uint64) error {
	switch mode {
	case 0:
		end := uint64(offset + length)
		if end > curSize {
			return f.Truncate(int64(end))
		}
		return nil
	case fallocFlKeepSize:
		return nil
	case fallocFlPunchHole | fallocFlKeepSize:
		if uint64(offset) >= curSize {
			return nil
		}
		punchEnd := min(uint64(offset+length), curSize)
		return writeZerosAt(f, offset, int64(punchEnd-uint64(offset)))
	default:
		return syscall.EOPNOTSUPP
	}
}

func writeZerosAt(f *os.File, offset, length int64) error {
	if length <= 0 {
		return nil
	}
	var zeroBuf [32 * 1024]byte
	pos := offset
	rem := length
	for rem > 0 {
		n := min(rem, int64(len(zeroBuf)))
		if err := writeAtFull(f, zeroBuf[:n], pos); err != nil {
			return err
		}
		pos += n
		rem -= n
	}
	return nil
}

func appendSplitExtents(extents []tmpExtent, start, end uint64) []tmpExtent {
	for pos := start; pos < end; {
		n := min(end-pos, StageChunkSize)
		extents = append(extents, tmpExtent{offset: pos, length: n})
		pos += n
	}
	return extents
}

func isAllZeroBytes(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func scanNonZeroExtents(f *os.File, startOff, size uint64) ([]tmpExtent, error) {
	if startOff >= size {
		return nil, nil
	}
	var extents []tmpExtent
	var page [tmpExtentScanPageSize]byte
	hasCur := false
	var curStart, curEnd uint64

	flushExtent := func() {
		if hasCur && curEnd > curStart {
			extents = appendSplitExtents(extents, curStart, curEnd)
		}
		hasCur = false
	}

	for pos := startOff; pos < size; {
		want := int(min(uint64(len(page)), size-pos))
		n, err := readAtFull(f, page[:want], int64(pos))
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		if !isAllZeroBytes(page[:n]) {
			switch {
			case !hasCur:
				curStart = pos
				curEnd = pos + uint64(n)
				hasCur = true
			case pos == curEnd && curEnd-curStart+uint64(n) <= StageChunkSize:
				curEnd += uint64(n)
			default:
				flushExtent()
				curStart = pos
				curEnd = pos + uint64(n)
				hasCur = true
			}
		} else if hasCur {
			flushExtent()
		}
		pos += uint64(n)
	}
	flushExtent()
	return extents, nil
}
