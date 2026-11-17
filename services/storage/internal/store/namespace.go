package store

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// MknodReq 定义 Mknod 节点创建参数。
type MknodReq struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Rdev      uint32
	Umask     uint32
	Epoch     uint64
}

// MkdirReq 定义 Mkdir 目录创建参数。
type MkdirReq struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Umask     uint32
	Epoch     uint64
}

// CreateReq 定义 Create 常规文件创建参数。
type CreateReq struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Umask     uint32
	Flags     uint32
	Epoch     uint64
}

// CreateTmpfileReq defines parameters for creating an anonymous temporary regular file.
type CreateTmpfileReq struct {
	ParentIno uint64
	Mode      uint32
	UID       uint32
	GID       uint32
	Umask     uint32
	Exclusive bool
	Epoch     uint64
}

// SymlinkReq 定义 Symlink 符号链接创建参数。
type SymlinkReq struct {
	ParentIno uint64
	Name      []byte
	Target    []byte
	UID       uint32
	GID       uint32
	Epoch     uint64
}

// LinkReq 定义 Link 硬链接创建参数。
type LinkReq struct {
	Ino          uint64
	NewParentIno uint64
	NewName      []byte
	Epoch        uint64
}

// DirEntry 表示 ReadDir 返回的单条目录项。
type DirEntry struct {
	Ino        uint64
	Name       []byte
	Mode       uint32
	NextOffset uint64
}

// DirEntryPlus 表示 ReadDirPlus 返回的单条扩展目录项（内联目标节点 Attribute）。
type DirEntryPlus struct {
	Ino        uint64
	Name       []byte
	Mode       uint32
	NextOffset uint64
	Attr       Attribute
}

type createNodeOp struct {
	parentIno uint64
	name      []byte
	mode      uint32
	uid       uint32
	gid       uint32
	rdev      uint32
	target    []byte

	// Phase A 校验缓存至 Phase B
	parent Inode
	attr   Attribute
}

type createTmpfileOp struct {
	parentIno uint64
	mode      uint32
	uid       uint32
	gid       uint32
	exclusive bool

	// Phase A validation cached for Phase B.
	parent Inode
	attr   Attribute
}

type linkNodeOp struct {
	ino          uint64
	newParentIno uint64
	newName      []byte

	// Phase A 校验缓存至 Phase B
	parent Inode
	target Inode
	attr   Attribute
}

func validateNodeIno(ino uint64) error {
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalid
	}
	return nil
}

func validateEntryName(name []byte) error {
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	if len(name) == 0 || bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalid
	}
	return nil
}

func isDotOrDotDot(name []byte) bool {
	return bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte(".."))
}

// Mknod 在 Actor 双阶段循环内创建非目录节点（S_IFREG / S_IFCHR / S_IFBLK / S_IFIFO / S_IFSOCK）。
func (s *StoreActor) Mknod(ctx context.Context, req MknodReq) (Attribute, error) {
	if s.readOnly {
		return Attribute{}, ErrReadOnly
	}
	if s.isDefinitelyStaleEpoch(req.Epoch) {
		return Attribute{}, s.submitStaleEpoch(ctx, opMknod, req.Epoch)
	}
	if err := validateNodeIno(req.ParentIno); err != nil {
		return Attribute{}, err
	}
	if err := validateEntryName(req.Name); err != nil {
		return Attribute{}, err
	}
	if req.Mode > 0o177777 {
		return Attribute{}, ErrInvalid
	}
	modeType := req.Mode & S_IFMT
	switch modeType {
	case S_IFREG, S_IFCHR, S_IFBLK, S_IFIFO, S_IFSOCK:
	default:
		return Attribute{}, ErrInvalid
	}

	effMode := modeType | ((req.Mode & PermMask) &^ (req.Umask & 0o777))
	return s.submitCreateNode(ctx, opMknod, req.Epoch, &createNodeOp{
		parentIno: req.ParentIno,
		name:      req.Name,
		mode:      effMode,
		uid:       req.UID,
		gid:       req.GID,
		rdev:      req.Rdev,
	})
}

// Mkdir 在 Actor 双阶段循环内创建子目录并递增父目录 Nlink。
func (s *StoreActor) Mkdir(ctx context.Context, req MkdirReq) (Attribute, error) {
	if s.readOnly {
		return Attribute{}, ErrReadOnly
	}
	if s.isDefinitelyStaleEpoch(req.Epoch) {
		return Attribute{}, s.submitStaleEpoch(ctx, opMkdir, req.Epoch)
	}
	if err := validateNodeIno(req.ParentIno); err != nil {
		return Attribute{}, err
	}
	if err := validateEntryName(req.Name); err != nil {
		return Attribute{}, err
	}
	if req.Mode > 0o177777 {
		return Attribute{}, ErrInvalid
	}
	modeType := req.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFDIR {
		return Attribute{}, ErrInvalid
	}

	effMode := S_IFDIR | ((req.Mode & PermMask) &^ (req.Umask & 0o777))
	return s.submitCreateNode(ctx, opMkdir, req.Epoch, &createNodeOp{
		parentIno: req.ParentIno,
		name:      req.Name,
		mode:      effMode,
		uid:       req.UID,
		gid:       req.GID,
	})
}

func resolveRegCreateMode(mode, umask uint32) (uint32, error) {
	if mode > 0o177777 {
		return 0, ErrInvalid
	}
	modeType := mode & S_IFMT
	if modeType != 0 && modeType != S_IFREG {
		return 0, ErrInvalid
	}
	return S_IFREG | ((mode & PermMask) &^ (umask & 0o777)), nil
}

// Create 在 Actor 双阶段循环内创建常规文件节点。
func (s *StoreActor) Create(ctx context.Context, req CreateReq) (Attribute, error) {
	if s.readOnly {
		return Attribute{}, ErrReadOnly
	}
	if s.isDefinitelyStaleEpoch(req.Epoch) {
		return Attribute{}, s.submitStaleEpoch(ctx, opCreate, req.Epoch)
	}
	if err := validateNodeIno(req.ParentIno); err != nil {
		return Attribute{}, err
	}
	if err := validateEntryName(req.Name); err != nil {
		return Attribute{}, err
	}
	if (req.Flags & O_TMPFILE) != 0 {
		return Attribute{}, ErrInvalid
	}
	effMode, err := resolveRegCreateMode(req.Mode, req.Umask)
	if err != nil {
		return Attribute{}, err
	}

	return s.submitCreateNode(ctx, opCreate, req.Epoch, &createNodeOp{
		parentIno: req.ParentIno,
		name:      req.Name,
		mode:      effMode,
		uid:       req.UID,
		gid:       req.GID,
	})
}

// CreateTmpfile allocates an unlinked regular-file inode and writes its orphan marker.
func (s *StoreActor) CreateTmpfile(ctx context.Context, req CreateTmpfileReq) (Attribute, error) {
	if s.readOnly {
		return Attribute{}, ErrReadOnly
	}
	if s.isDefinitelyStaleEpoch(req.Epoch) {
		return Attribute{}, s.submitStaleEpoch(ctx, opCreateTmpfile, req.Epoch)
	}
	if err := validateNodeIno(req.ParentIno); err != nil {
		return Attribute{}, err
	}
	effMode, err := resolveRegCreateMode(req.Mode, req.Umask)
	if err != nil {
		return Attribute{}, err
	}

	op := &createTmpfileOp{
		parentIno: req.ParentIno,
		mode:      effMode,
		uid:       req.UID,
		gid:       req.GID,
		exclusive: req.Exclusive,
	}
	cmd := s.acquireCmd()
	cmd.op = opCreateTmpfile
	cmd.epoch = req.Epoch
	cmd.req = op
	if err := s.submit(ctx, cmd); err != nil {
		return Attribute{}, err
	}
	return op.attr, nil
}

// Symlink 在 Actor 双阶段循环内创建符号链接节点。
func (s *StoreActor) Symlink(ctx context.Context, req SymlinkReq) (Attribute, error) {
	if s.readOnly {
		return Attribute{}, ErrReadOnly
	}
	if s.isDefinitelyStaleEpoch(req.Epoch) {
		return Attribute{}, s.submitStaleEpoch(ctx, opSymlink, req.Epoch)
	}
	if err := validateNodeIno(req.ParentIno); err != nil {
		return Attribute{}, err
	}
	if err := validateEntryName(req.Name); err != nil {
		return Attribute{}, err
	}
	if len(req.Target) == 0 || bytes.IndexByte(req.Target, 0) >= 0 {
		return Attribute{}, ErrInvalid
	}
	if len(req.Target) > MaxSymlinkTargetBytes {
		return Attribute{}, ErrNameTooLong
	}

	return s.submitCreateNode(ctx, opSymlink, req.Epoch, &createNodeOp{
		parentIno: req.ParentIno,
		name:      req.Name,
		mode:      S_IFLNK | 0o777,
		uid:       req.UID,
		gid:       req.GID,
		target:    append([]byte(nil), req.Target...),
	})
}

// Link 在 Actor 双阶段循环内为已存在的非目录节点创建硬链接并递增 Nlink。
func (s *StoreActor) Link(ctx context.Context, req LinkReq) (Attribute, error) {
	if s.readOnly {
		return Attribute{}, ErrReadOnly
	}
	if s.isDefinitelyStaleEpoch(req.Epoch) {
		return Attribute{}, s.submitStaleEpoch(ctx, opLink, req.Epoch)
	}
	if err := validateNodeIno(req.Ino); err != nil {
		return Attribute{}, err
	}
	if err := validateNodeIno(req.NewParentIno); err != nil {
		return Attribute{}, err
	}
	if err := validateEntryName(req.NewName); err != nil {
		return Attribute{}, err
	}

	op := &linkNodeOp{
		ino:          req.Ino,
		newParentIno: req.NewParentIno,
		newName:      req.NewName,
	}
	cmd := s.acquireCmd()
	cmd.op = opLink
	cmd.epoch = req.Epoch
	cmd.req = op
	if err := s.submit(ctx, cmd); err != nil {
		return Attribute{}, err
	}
	return op.attr, nil
}

func (s *StoreActor) submitCreateNode(ctx context.Context, kind opKind, epoch uint64, op *createNodeOp) (Attribute, error) {
	cmd := s.acquireCmd()
	cmd.op = kind
	cmd.epoch = epoch
	cmd.req = op
	if err := s.submit(ctx, cmd); err != nil {
		return Attribute{}, err
	}
	return op.attr, nil
}

func (s *StoreActor) readWritableParent(reader pebbleReader, parentIno uint64, name []byte) (Inode, error) {
	parent, err := s.readInode(reader, parentIno)
	if err != nil {
		return Inode{}, err
	}
	if parent.Mode&S_IFMT != S_IFDIR {
		return Inode{}, ErrNotDir
	}
	if parent.Nlink == 0 {
		return Inode{}, ErrNotFound
	}
	if isDotOrDotDot(name) {
		return Inode{}, ErrExist
	}
	return parent, nil
}

func (s *StoreActor) checkEntryNotExists(reader pebbleReader, parentIno uint64, name []byte) error {
	_, closer, err := reader.Get(EntryKey(parentIno, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if closeErr := closer.Close(); closeErr != nil {
		return closeErr
	}
	return ErrExist
}

func (s *StoreActor) validateCreateNodeCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*createNodeOp)
	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	parent, err := s.readWritableParent(s.batch, op.parentIno, op.name)
	if err != nil {
		return err
	}
	if err := s.checkEntryNotExists(s.batch, op.parentIno, op.name); err != nil {
		return err
	}
	isDir := op.mode&S_IFMT == S_IFDIR
	if isDir && parent.Nlink >= MaxNlink {
		return ErrTooManyLinks
	}
	size := uint64(len(op.target))
	if s.stat.Inodes == math.MaxUint64 || size > math.MaxUint64-s.stat.LogicalBytes {
		return ErrNoSpace
	}
	if err := s.checkQuota(size, 1); err != nil {
		return err
	}
	if _, err := s.prepareIno(cmd.spanCtx); err != nil {
		return err
	}
	op.parent = parent
	return nil
}

func (s *StoreActor) mutateCreateNodeCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*createNodeOp)
	ino := s.consumeIno()
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	isDir := op.mode&S_IFMT == S_IFDIR
	gid := op.gid
	mode := op.mode
	if op.parent.Mode&S_ISGID != 0 {
		gid = op.parent.GID
		if isDir {
			mode |= S_ISGID
		}
	}
	var rdev uint32
	if mode&S_IFMT == S_IFCHR || mode&S_IFMT == S_IFBLK {
		rdev = op.rdev
	}

	nlink := uint32(1)
	parentIno := uint64(0)
	if isDir {
		nlink = 2
		parentIno = op.parentIno
		op.parent.Nlink++
	}

	size := uint64(len(op.target))
	child := Inode{
		Ino:           ino,
		ParentIno:     parentIno,
		Mode:          mode,
		Nlink:         nlink,
		UID:           op.uid,
		GID:           gid,
		Rdev:          rdev,
		Size:          size,
		Atime:         nowNano,
		Mtime:         nowNano,
		Ctime:         nowNano,
		Generation:    1,
		WriteSeq:      seq,
		SymlinkTarget: op.target,
	}

	op.parent.Mtime = nowNano
	op.parent.Ctime = nowNano
	op.parent.WriteSeq = seq

	s.stat.Inodes++
	s.stat.LogicalBytes += size

	_ = BatchSetInodeDeferred(s.batch, child)
	_ = BatchSetInodeDeferred(s.batch, op.parent)
	BatchSetEntryDeferred(s.batch, op.parentIno, op.name, Entry{
		Ino:  ino,
		Mode: mode,
	})

	op.attr = child.ToAttribute()
}

func (s *StoreActor) validateCreateTmpfileCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*createTmpfileOp)
	if s.stat.NextWriteSeq == math.MaxUint64 || s.stat.Inodes == math.MaxUint64 {
		return ErrNoSpace
	}
	parent, err := s.readLiveDir(s.batch, op.parentIno)
	if err != nil {
		return err
	}
	if err := s.checkQuota(0, 1); err != nil {
		return err
	}
	if _, err := s.prepareIno(cmd.spanCtx); err != nil {
		return err
	}
	op.parent = parent
	return nil
}

func (s *StoreActor) mutateCreateTmpfileCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*createTmpfileOp)
	ino := s.consumeIno()
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	gid := op.gid
	if op.parent.Mode&S_ISGID != 0 {
		gid = op.parent.GID
	}

	child := Inode{
		Ino:        ino,
		ParentIno:  0,
		Mode:       op.mode,
		Nlink:      0,
		UID:        op.uid,
		GID:        gid,
		Size:       0,
		Atime:      nowNano,
		Mtime:      nowNano,
		Ctime:      nowNano,
		Flags:      0,
		Generation: 1,
		WriteSeq:   seq,
	}

	s.stat.Inodes++
	_ = BatchSetInodeDeferred(s.batch, child)

	orphanKind := OrphanKindTmpfileLinkable
	if op.exclusive {
		orphanKind = OrphanKindTmpfileExclusive
	}
	BatchSetOrphanKindDeferred(s.batch, ino, orphanKind)

	op.attr = child.ToAttribute()
}

func (s *StoreActor) validateLinkCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*linkNodeOp)
	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	parent, err := s.readWritableParent(s.batch, op.newParentIno, op.newName)
	if err != nil {
		return err
	}
	target, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	if target.Mode&S_IFMT == S_IFDIR {
		return ErrPerm
	}
	if target.Nlink >= MaxNlink {
		return ErrTooManyLinks
	}
	if target.Nlink == 0 {
		if target.Mode&S_IFMT != S_IFREG {
			return ErrNotFound
		}
		kind, hasOrphan, orphanErr := s.readOrphanKind(s.batch, op.ino)
		if orphanErr != nil {
			return orphanErr
		}
		if !hasOrphan || kind != OrphanKindTmpfileLinkable {
			return ErrNotFound
		}
	}
	if err := s.checkEntryNotExists(s.batch, op.newParentIno, op.newName); err != nil {
		return err
	}
	op.parent = parent
	op.target = target
	return nil
}

func (s *StoreActor) mutateLinkCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*linkNodeOp)
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	promoteOrphan := op.target.Nlink == 0
	op.target.Nlink++
	op.target.Ctime = nowNano
	op.target.WriteSeq = seq

	op.parent.Mtime = nowNano
	op.parent.Ctime = nowNano
	op.parent.WriteSeq = seq

	_ = BatchSetInodeDeferred(s.batch, op.target)
	_ = BatchSetInodeDeferred(s.batch, op.parent)
	BatchSetEntryDeferred(s.batch, op.newParentIno, op.newName, Entry{
		Ino:  op.target.Ino,
		Mode: op.target.Mode,
	})
	if promoteOrphan {
		BatchDeleteOrphanDeferred(s.batch, op.target.Ino)
	}

	s.overlayAtime(&op.target)
	op.attr = op.target.ToAttribute()
}

// Lookup 在并发直读快路径（lifecycleMu.RLock()）解析父目录下指定名称，返回 Entry 与叠加 dirtyAtime 后的 Attribute。
func (s *StoreActor) Lookup(ctx context.Context, parentIno uint64, name []byte) (Entry, Attribute, error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpLookup, time.Since(opStart))
		}()
	}
	if err := validateNodeIno(parentIno); err != nil {
		return Entry{}, Attribute{}, err
	}
	if len(name) > MaxFileNameBytes {
		return Entry{}, Attribute{}, ErrNameTooLong
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return Entry{}, Attribute{}, ErrInvalid
	}

	unlock, err := s.enter()
	if err != nil {
		return Entry{}, Attribute{}, err
	}
	defer unlock()

	parent, err := s.readInodeFromDB(ctx, parentIno)
	if err != nil {
		return Entry{}, Attribute{}, err
	}
	if parent.Mode&S_IFMT != S_IFDIR {
		return Entry{}, Attribute{}, ErrNotDir
	}

	if bytes.Equal(name, []byte("..")) {
		err = s.checkInodeLiveFromDB(ctx, &parent)
		if err != nil {
			return Entry{}, Attribute{}, err
		}
		if parentIno == RootIno {
			s.overlayAtime(&parent)
			return Entry{Ino: parent.Ino, Mode: parent.Mode}, parent.ToAttribute(), nil
		}
		if parent.ParentIno == 0 || parent.ParentIno == parentIno {
			return Entry{}, Attribute{}, ErrNotFound
		}
		var grandparent Inode
		grandparent, err = s.readInodeFromDB(ctx, parent.ParentIno)
		if err != nil {
			return Entry{}, Attribute{}, err
		}
		err = s.checkInodeLiveFromDB(ctx, &grandparent)
		if err != nil {
			return Entry{}, Attribute{}, err
		}
		s.overlayAtime(&grandparent)
		return Entry{Ino: grandparent.Ino, Mode: grandparent.Mode}, grandparent.ToAttribute(), nil
	}

	if parent.Nlink == 0 {
		return Entry{}, Attribute{}, ErrNotFound
	}
	if bytes.Equal(name, []byte(".")) {
		s.overlayAtime(&parent)
		return Entry{Ino: parent.Ino, Mode: parent.Mode}, parent.ToAttribute(), nil
	}
	if len(name) == 0 {
		return Entry{}, Attribute{}, ErrNotFound
	}

	entry, err := s.readEntryFromDB(ctx, parentIno, name)
	if err != nil {
		return Entry{}, Attribute{}, err
	}
	child, err := s.readInodeFromDB(ctx, entry.Ino)
	if err != nil {
		return Entry{}, Attribute{}, err
	}
	if child.Nlink == 0 {
		return Entry{}, Attribute{}, ErrNotFound
	}
	s.overlayAtime(&child)
	return entry, child.ToAttribute(), nil
}

func (s *StoreActor) readEntry(reader pebbleReader, parentIno uint64, name []byte) (Entry, error) {
	val, closer, err := reader.Get(EntryKey(parentIno, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, err
	}
	entry, decErr := DecodeEntry(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return Entry{}, errors.Join(decErr, closeErr)
	}
	return entry, nil
}

func (s *StoreActor) readEntryFromDB(ctx context.Context, parentIno uint64, name []byte) (Entry, error) {
	val, closer, err := s.dbGetPoint(ctx, EntryKey(parentIno, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, err
	}
	entry, decErr := DecodeEntry(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return Entry{}, errors.Join(decErr, closeErr)
	}
	return entry, nil
}

// Readlink 在并发直读快路径（lifecycleMu.RLock()）读取符号链接目标路径并按策略非阻塞登记 dirtyAtime。
func (s *StoreActor) Readlink(ctx context.Context, ino uint64) ([]byte, error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpReadlink, time.Since(opStart))
		}()
	}
	if err := validateNodeIno(ino); err != nil {
		return nil, err
	}
	unlock, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer unlock()

	inode, err := s.readInodeFromDB(ctx, ino)
	if err != nil {
		return nil, err
	}
	if err := s.checkInodeLiveFromDB(ctx, &inode); err != nil {
		return nil, err
	}
	if inode.Mode&S_IFMT != S_IFLNK {
		return nil, ErrInvalid
	}
	if len(inode.SymlinkTarget) == 0 {
		return nil, ErrIO
	}
	s.touchReadAtime(&inode)
	return append([]byte(nil), inode.SymlinkTarget...), nil
}

func (s *StoreActor) touchReadAtime(inode *Inode) {
	if s.readOnly || s.closing.Load() {
		return
	}
	nowNano := s.now().UnixNano()
	switch s.atimeMode {
	case AtimeModeNoAtime:
		if nowNano > inode.Atime {
			s.markAtimeDirty(inode.Ino, nowNano)
		}
	case AtimeModeStrictAtime:
		s.markAtimeDirty(inode.Ino, nowNano)
	case AtimeModeRelAtime:
		curAtime := inode.Atime
		s.atimeMu.Lock()
		if pending, ok := s.dirtyAtime[inode.Ino]; ok && pending > curAtime {
			curAtime = pending
		}
		s.atimeMu.Unlock()
		if curAtime <= inode.Mtime || curAtime <= inode.Ctime || nowNano-curAtime >= int64(24*time.Hour) {
			s.markAtimeDirty(inode.Ino, nowNano)
		}
	}
}

// ReadDir 在并发直读快路径（lifecycleMu.RLock()）通过 db.NewIter(nil) + iter.SeekPrefixGE(EntryPrefix(ino))
// 触发 15B 前缀布隆过滤器扫描目录项，合成 "." 与 ".." 并按游标 offset 分页返回。
func (s *StoreActor) ReadDir(ctx context.Context, ino, offset uint64, maxEntries uint32) (entries []DirEntry, err error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpReadDir, time.Since(opStart))
		}()
	}
	err = validateNodeIno(ino)
	if err != nil {
		return nil, err
	}
	if maxEntries == 0 || maxEntries > MaxReadDirLimit || offset > math.MaxInt64 {
		return nil, ErrInvalid
	}
	unlock, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer unlock()

	dir, err := s.readInodeFromDB(ctx, ino)
	if err != nil {
		return nil, err
	}
	if dir.Mode&S_IFMT != S_IFDIR {
		return nil, ErrNotDir
	}
	if dir.Nlink == 0 {
		err = s.checkInodeLiveFromDB(ctx, &dir)
		if err != nil {
			return nil, err
		}
	}

	limit := int(maxEntries)
	entries = make([]DirEntry, 0, min(limit, 16))
	if offset < 1 && len(entries) < limit {
		entries = append(entries, DirEntry{
			Ino:        dir.Ino,
			Name:       []byte("."),
			Mode:       dir.Mode,
			NextOffset: 1,
		})
	}
	if offset < 2 && len(entries) < limit {
		dotDotIno := dir.Ino
		dotDotMode := dir.Mode
		if ino != RootIno && dir.ParentIno > 0 {
			parent, readErr := s.readInodeFromDB(ctx, dir.ParentIno)
			if readErr == nil {
				dotDotIno = parent.Ino
				dotDotMode = parent.Mode
			} else if !errors.Is(readErr, ErrNotFound) {
				return nil, readErr
			}
		}
		entries = append(entries, DirEntry{
			Ino:        dotDotIno,
			Name:       []byte(".."),
			Mode:       dotDotMode,
			NextOffset: 2,
		})
	}
	if len(entries) == limit {
		return entries, nil
	}

	var scanSpan trace.Span
	if isSpanRecording(ctx) {
		_, scanSpan = startChildSpan(ctx, "store.KVScan",
			attribute.String("scan.kind", "dir_entries"),
			attribute.Int64("inode.ino", int64(ino)),
		)
	}
	var scanStart time.Time
	if s.metrics != nil {
		scanStart = time.Now()
	}
	defer func() {
		if s.metrics != nil {
			s.metrics.observeKVPrefixScan(time.Since(scanStart))
		}
		finishSpan(scanSpan, err)
	}()

	// 严格使用 db.NewIter(nil) + iter.SeekPrefixGE(EntryPrefix(ino)) 激活 15B Prefix Bloom Filter。
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	var skip uint64
	if offset > 2 {
		skip = offset - 2
	}
	next := max(offset, uint64(2))

	prefix := EntryPrefix(ino)
	for valid := iter.SeekPrefixGE(prefix); valid && len(entries) < limit; valid = iter.Next() {
		_, name, decErr := DecodeEntryKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		entry, decErr := DecodeEntry(iter.Value())
		if decErr != nil {
			return nil, decErr
		}
		if skip > 0 {
			skip--
			continue
		}
		if next == math.MaxInt64 {
			return nil, ErrInvalid
		}
		next++
		entries = append(entries, DirEntry{
			Ino:        entry.Ino,
			Name:       name,
			Mode:       entry.Mode,
			NextOffset: next,
		})
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, iterErr
	}
	return entries, nil
}

// ReadDirPlus 在并发直读快路径（lifecycleMu.RLock()）扫出当前页目录项后，
// 将子项按 Ino 升序排序并复用同一个 iter 循环调用 iter.SeekPrefixGE(InodeKey(ino))
// 激活 Pebble TrySeekUsingNext 批量获取子项 Attribute。
func (s *StoreActor) ReadDirPlus(ctx context.Context, ino, offset uint64, maxEntries uint32) (entries []DirEntryPlus, err error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpReadDirPlus, time.Since(opStart))
		}()
	}
	err = validateNodeIno(ino)
	if err != nil {
		return nil, err
	}
	if maxEntries == 0 || maxEntries > MaxReadDirLimit || offset > math.MaxInt64 {
		return nil, ErrInvalid
	}
	unlock, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer unlock()

	dir, err := s.readInodeFromDB(ctx, ino)
	if err != nil {
		return nil, err
	}
	if dir.Mode&S_IFMT != S_IFDIR {
		return nil, ErrNotDir
	}
	if dir.Nlink == 0 {
		err = s.checkInodeLiveFromDB(ctx, &dir)
		if err != nil {
			return nil, err
		}
	}
	s.overlayAtime(&dir)
	dirAttr := dir.ToAttribute()

	limit := int(maxEntries)
	entries = make([]DirEntryPlus, 0, min(limit, 16))
	if offset < 1 && len(entries) < limit {
		entries = append(entries, DirEntryPlus{
			Ino:        dir.Ino,
			Name:       []byte("."),
			Mode:       dir.Mode,
			NextOffset: 1,
			Attr:       dirAttr,
		})
	}
	if offset < 2 && len(entries) < limit {
		parentAttr := dirAttr
		parentIno := dir.Ino
		parentMode := dir.Mode
		if ino != RootIno && dir.ParentIno > 0 {
			parent, readErr := s.readInodeFromDB(ctx, dir.ParentIno)
			if readErr == nil {
				s.overlayAtime(&parent)
				parentAttr = parent.ToAttribute()
				parentIno = parent.Ino
				parentMode = parent.Mode
			} else if !errors.Is(readErr, ErrNotFound) {
				return nil, readErr
			}
		}
		entries = append(entries, DirEntryPlus{
			Ino:        parentIno,
			Name:       []byte(".."),
			Mode:       parentMode,
			NextOffset: 2,
			Attr:       parentAttr,
		})
	}
	if len(entries) == limit {
		return entries, nil
	}

	var scanSpan trace.Span
	if isSpanRecording(ctx) {
		_, scanSpan = startChildSpan(ctx, "store.KVScan",
			attribute.String("scan.kind", "dir_entries_plus"),
			attribute.Int64("inode.ino", int64(ino)),
		)
	}
	var scanStart time.Time
	if s.metrics != nil {
		scanStart = time.Now()
	}
	defer func() {
		if s.metrics != nil {
			s.metrics.observeKVPrefixScan(time.Since(scanStart))
		}
		finishSpan(scanSpan, err)
	}()

	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	var skip uint64
	if offset > 2 {
		skip = offset - 2
	}
	next := max(offset, uint64(2))

	type scannedChild struct {
		entry      Entry
		name       []byte
		nextOffset uint64
	}
	children := make([]scannedChild, 0, min(limit-len(entries), 16))

	prefix := EntryPrefix(ino)
	for valid := iter.SeekPrefixGE(prefix); valid && len(entries)+len(children) < limit; valid = iter.Next() {
		_, name, decErr := DecodeEntryKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		entry, decErr := DecodeEntry(iter.Value())
		if decErr != nil {
			return nil, decErr
		}
		if skip > 0 {
			skip--
			continue
		}
		if next == math.MaxInt64 {
			return nil, ErrInvalid
		}
		next++
		children = append(children, scannedChild{
			entry:      entry,
			name:       name,
			nextOffset: next,
		})
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, iterErr
	}
	if len(children) == 0 {
		return entries, nil
	}

	// 将子项按 Ino 升序去重排列，复用同一个 iter 循环调用 iter.SeekPrefixGE(InodeKey(ino))
	// 使搜索键单调递增，自动触发 Pebble TrySeekUsingNext 块内步进优化。
	sortedInos := make([]uint64, len(children))
	for i, c := range children {
		sortedInos[i] = c.entry.Ino
	}
	slices.Sort(sortedInos)
	sortedInos = slices.Compact(sortedInos)

	attrByIno := make(map[uint64]Attribute, len(sortedInos))
	var keyBuf [InodeKeyLen]byte
	for _, childIno := range sortedInos {
		putInodeKey(keyBuf[:], childIno)
		if !iter.SeekPrefixGE(keyBuf[:]) || !bytes.Equal(iter.Key(), keyBuf[:]) {
			continue
		}
		inode, decErr := DecodeInode(iter.Value())
		if decErr != nil {
			return nil, decErr
		}
		if inode.Ino != childIno {
			return nil, ErrInvalidValue
		}
		if inode.Nlink == 0 {
			continue
		}
		s.overlayAtime(&inode)
		attrByIno[childIno] = inode.ToAttribute()
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, iterErr
	}

	for _, c := range children {
		attr, ok := attrByIno[c.entry.Ino]
		if !ok {
			continue
		}
		entries = append(entries, DirEntryPlus{
			Ino:        c.entry.Ino,
			Name:       c.name,
			Mode:       attr.Mode,
			NextOffset: c.nextOffset,
			Attr:       attr,
		})
	}
	return entries, nil
}
