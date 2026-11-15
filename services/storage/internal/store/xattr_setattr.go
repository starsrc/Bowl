package store

import (
	"bytes"
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	userXattrPrefix         = []byte("user.")
	securityCapabilityXattr = []byte("security.capability")
)

// SetAttrReq 定义 SetAttr 属性变更请求参数。
type SetAttrReq struct {
	Mode     *uint32
	UID      *uint32
	GID      *uint32
	Size     *uint64
	Atime    *time.Time
	Mtime    *time.Time
	AtimeNow bool
	MtimeNow bool
	CtimeNow bool
	Flags    *uint32
	Epoch    uint64
}

type setXattrOp struct {
	ino   uint64
	name  []byte
	value []byte
	flags uint32

	// Phase A 校验缓存至 Phase B
	inode Inode
}

type removeXattrOp struct {
	ino  uint64
	name []byte

	// Phase A 校验缓存至 Phase B
	inode Inode
}

type setAttrOp struct {
	ino       uint64
	req       SetAttrReq
	tailSpecs []writeBlockSpec

	// Phase A 校验缓存至 Phase B
	inode      Inode
	deleteKeys []dataKeyEntry
	attr       Attribute
}

func validateXattrName(name []byte) error {
	if len(name) == 0 || bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalid
	}
	if len(name) > MaxXattrNameBytes {
		return ErrRange
	}
	return nil
}

// isAllowedUserXattrFileType 判定文件模式是否允许访问 user.* 扩展属性命名空间（仅常规文件、目录与套接字）。
func isAllowedUserXattrFileType(mode uint32) bool {
	fileType := mode & S_IFMT
	return fileType == S_IFREG || fileType == S_IFDIR || fileType == S_IFSOCK
}

func xattrExists(reader pebbleReader, ino uint64, name []byte) (bool, error) {
	_, closer, err := reader.Get(XattrKey(ino, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, wrapIOErr(err)
	}
	if closeErr := wrapIOErr(closer.Close()); closeErr != nil {
		return false, closeErr
	}
	return true, nil
}

func checkXattrListQuota(reader pebbleReader, ino uint64, newName []byte) (err error) {
	iter, openErr := reader.NewIter(nil)
	if openErr != nil {
		return wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
	}()

	total := len(newName) + 1
	prefix := XattrPrefix(ino)
	for valid := iter.SeekPrefixGE(prefix); valid; valid = iter.Next() {
		key := iter.Key()
		if len(key) < XattrPrefixLen {
			return ErrInvalidKey
		}
		total += (len(key) - XattrPrefixLen) + 1
		if total > MaxXattrListBytes {
			return ErrNoSpace
		}
	}
	if total > MaxXattrListBytes {
		return ErrNoSpace
	}
	return nil
}

// SetXattr 在 Actor 双阶段循环内设置或覆盖节点扩展属性。
func (s *StoreActor) SetXattr(ctx context.Context, ino uint64, name, value []byte, flags uint32, epoch ...uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return s.submitStaleEpoch(ctx, opSetXattr, ep)
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}
	if flags&^(XattrCreate|XattrReplace) != 0 {
		return ErrInvalid
	}
	if err := validateXattrName(name); err != nil {
		return err
	}
	if len(value) > MaxXattrValueBytes {
		return ErrRange
	}

	cmd := s.acquireCmd()
	cmd.op = opSetXattr
	cmd.epoch = ep
	cmd.req = &setXattrOp{
		ino:   ino,
		name:  name,
		value: value,
		flags: flags,
	}
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateSetXattrCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*setXattrOp)
	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	err = s.checkInodeLive(s.batch, &inode)
	if err != nil {
		return err
	}
	if bytes.HasPrefix(op.name, userXattrPrefix) && !isAllowedUserXattrFileType(inode.Mode) {
		return ErrPerm
	}
	exists, err := xattrExists(s.batch, op.ino, op.name)
	if err != nil {
		return err
	}
	if exists {
		if op.flags&XattrCreate != 0 {
			return ErrExist
		}
	} else {
		if op.flags&XattrReplace != 0 {
			return ErrNoData
		}
		if err := checkXattrListQuota(s.batch, op.ino, op.name); err != nil {
			return err
		}
	}
	op.inode = inode
	return nil
}

func (s *StoreActor) mutateSetXattrCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*setXattrOp)
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	op.inode.Ctime = nowNano
	op.inode.WriteSeq = seq

	BatchSetXattrDeferred(s.batch, op.ino, op.name, op.value)
	_ = BatchSetInodeDeferred(s.batch, op.inode)
}

// GetXattr 在并发直读快路径（lifecycleMu.RLock()）直接调用 db.Get(XattrKey) 读取节点扩展属性值。
func (s *StoreActor) GetXattr(ctx context.Context, ino uint64, name []byte) ([]byte, error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpGetXattr, time.Since(opStart))
		}()
	}
	if err := validateNodeIno(ino); err != nil {
		return nil, err
	}
	if err := validateXattrName(name); err != nil {
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
	err = s.checkInodeLiveFromDB(ctx, &inode)
	if err != nil {
		return nil, err
	}
	if bytes.HasPrefix(name, userXattrPrefix) && !isAllowedUserXattrFileType(inode.Mode) {
		return nil, ErrNoData
	}

	val, closer, err := s.dbGetPoint(ctx, XattrKey(ino, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNoData
	}
	if err != nil {
		return nil, wrapIOErr(err)
	}
	out := append([]byte{}, val...)
	if closeErr := wrapIOErr(closer.Close()); closeErr != nil {
		return nil, closeErr
	}
	return out, nil
}

// ListXattr 在并发直读快路径（lifecycleMu.RLock()）通过 db.NewIter(nil) + iter.SeekPrefixGE(XattrPrefix(ino))
// 触发 15B 前缀布隆过滤器，按字节字典序升序返回节点扩展属性名列表。
func (s *StoreActor) ListXattr(ctx context.Context, ino uint64) (names [][]byte, err error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpListXattr, time.Since(opStart))
		}()
	}
	err = validateNodeIno(ino)
	if err != nil {
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
	err = s.checkInodeLiveFromDB(ctx, &inode)
	if err != nil {
		return nil, err
	}

	var scanSpan trace.Span
	if isSpanRecording(ctx) {
		_, scanSpan = startChildSpan(ctx, "store.KVScan",
			attribute.String("scan.kind", "xattrs"),
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

	iter, openErr := s.db.NewIter(nil)
	if openErr != nil {
		return nil, wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
		if err != nil {
			names = nil
		}
	}()

	names = [][]byte{}
	prefix := XattrPrefix(ino)
	for valid := iter.SeekPrefixGE(prefix); valid; valid = iter.Next() {
		_, name, decErr := DecodeXattrKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		names = append(names, name)
	}
	return names, nil
}

// RemoveXattr 在 Actor 双阶段循环内删除节点指定扩展属性。
func (s *StoreActor) RemoveXattr(ctx context.Context, ino uint64, name []byte, epoch ...uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return s.submitStaleEpoch(ctx, opRemoveXattr, ep)
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}
	if err := validateXattrName(name); err != nil {
		return err
	}

	cmd := s.acquireCmd()
	cmd.op = opRemoveXattr
	cmd.epoch = ep
	cmd.req = &removeXattrOp{
		ino:  ino,
		name: name,
	}
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateRemoveXattrCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*removeXattrOp)
	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	err = s.checkInodeLive(s.batch, &inode)
	if err != nil {
		return err
	}
	if bytes.HasPrefix(op.name, userXattrPrefix) && !isAllowedUserXattrFileType(inode.Mode) {
		return ErrPerm
	}
	exists, err := xattrExists(s.batch, op.ino, op.name)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNoData
	}
	op.inode = inode
	return nil
}

func (s *StoreActor) mutateRemoveXattrCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*removeXattrOp)
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	op.inode.Ctime = nowNano
	op.inode.WriteSeq = seq

	BatchDeleteXattrDeferred(s.batch, op.ino, op.name)
	_ = BatchSetInodeDeferred(s.batch, op.inode)
}

func validateSetAttrReq(req SetAttrReq) error {
	if req.Mode == nil && req.UID == nil && req.GID == nil && req.Size == nil &&
		req.Atime == nil && req.Mtime == nil && !req.AtimeNow && !req.MtimeNow &&
		!req.CtimeNow && req.Flags == nil {
		return ErrInvalid
	}
	if (req.Atime != nil && req.AtimeNow) || (req.Mtime != nil && req.MtimeNow) {
		return ErrInvalid
	}
	if req.Mode != nil && *req.Mode > 0o177777 {
		return ErrInvalid
	}
	if req.UID != nil && *req.UID > math.MaxInt32 {
		return ErrInvalid
	}
	if req.GID != nil && *req.GID > math.MaxInt32 {
		return ErrInvalid
	}
	if req.Size != nil && *req.Size > MaxFileSize {
		return ErrInvalid
	}
	if req.Flags != nil && *req.Flags > math.MaxInt32 {
		return ErrInvalid
	}
	return nil
}

// validateSetAttrInode 在 Phase A 校验目标 Inode 是否允许执行 req 指定的属性变更（供 SetAttr 及 setAttrSizeHook 复用）。
func (s *StoreActor) validateSetAttrInode(inode *Inode, req SetAttrReq) error {
	fileType := inode.Mode & S_IFMT
	if req.Size != nil {
		if fileType == S_IFDIR {
			return ErrIsDir
		}
		if fileType != S_IFREG {
			return ErrInvalid
		}
	}
	if inode.Flags&FlagImmutable != 0 && (req.Flags == nil || *req.Flags&FlagImmutable != 0) {
		return ErrPerm
	}
	if inode.Flags&FlagAppend != 0 && req.Size != nil {
		return ErrPerm
	}
	if req.Mode != nil {
		inputType := *req.Mode & S_IFMT
		if fileType == S_IFLNK || (inputType != 0 && inputType != fileType) {
			return ErrInvalid
		}
	}
	if req.Size != nil && *req.Size > inode.Size {
		delta := *req.Size - inode.Size
		if delta > math.MaxUint64-s.stat.LogicalBytes {
			return ErrNoSpace
		}
		if err := s.checkQuota(delta, 0); err != nil {
			return err
		}
	}
	return nil
}

// applySetAttrFields 在 Phase B 将 req 属性更新应用至 inode，并同步更新分支逻辑容量、dirtyAtime 及 security.capability。
func (s *StoreActor) applySetAttrFields(inode *Inode, req SetAttrReq, nowNano int64, seq uint64) {
	fileType := inode.Mode & S_IFMT
	if req.Mode != nil {
		inode.Mode = fileType | (*req.Mode & PermMask)
	} else if fileType != S_IFDIR && (req.UID != nil || req.GID != nil || req.Size != nil) {
		inode.Mode &^= S_ISUID | S_ISGID
	}
	if req.UID != nil {
		inode.UID = *req.UID
	}
	if req.GID != nil {
		inode.GID = *req.GID
	}
	if req.Flags != nil {
		inode.Flags = *req.Flags
	}
	if req.Size != nil {
		oldSize := inode.Size
		newSize := *req.Size
		inode.Size = newSize
		if newSize > oldSize {
			s.stat.LogicalBytes += newSize - oldSize
		} else if oldSize > newSize {
			diff := oldSize - newSize
			if diff <= s.stat.LogicalBytes {
				s.stat.LogicalBytes -= diff
			} else {
				s.stat.LogicalBytes = 0
			}
		}
	}
	switch {
	case req.AtimeNow:
		inode.Atime = nowNano
		s.discardDirtyAtime(inode.Ino)
	case req.Atime != nil:
		inode.Atime = req.Atime.UnixNano()
		s.discardDirtyAtime(inode.Ino)
	default:
		s.overlayAtime(inode)
	}
	if req.MtimeNow || (req.Size != nil && req.Mtime == nil) {
		inode.Mtime = nowNano
	} else if req.Mtime != nil {
		inode.Mtime = req.Mtime.UnixNano()
	}
	inode.Ctime = nowNano
	inode.WriteSeq = seq

	if req.Size != nil || req.UID != nil || req.GID != nil || req.Mode != nil {
		BatchDeleteXattrDeferred(s.batch, inode.Ino, securityCapabilityXattr)
	}
}

// SetAttr 在 Actor 双阶段循环内修改节点属性。
func (s *StoreActor) SetAttr(ctx context.Context, ino uint64, req SetAttrReq, epoch ...uint64) (Attribute, error) {
	if s.readOnly {
		return Attribute{}, ErrReadOnly
	}
	if len(epoch) > 0 {
		req.Epoch = epoch[0]
	}
	if s.isDefinitelyStaleEpoch(req.Epoch) {
		return Attribute{}, s.submitStaleEpoch(ctx, opSetAttr, req.Epoch)
	}
	if err := validateNodeIno(ino); err != nil {
		return Attribute{}, err
	}
	if err := validateSetAttrReq(req); err != nil {
		return Attribute{}, err
	}
	if req.Size != nil {
		return s.setAttrWithSize(ctx, ino, req)
	}

	op := &setAttrOp{
		ino: ino,
		req: req,
	}
	cmd := s.acquireCmd()
	cmd.op = opSetAttr
	cmd.epoch = req.Epoch
	cmd.req = op
	if err := s.submit(ctx, cmd); err != nil {
		return Attribute{}, err
	}
	return op.attr, nil
}

func (s *StoreActor) validateSetAttrCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*setAttrOp)
	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	if err := s.checkInodeLive(s.batch, &inode); err != nil {
		return err
	}
	if err := s.validateSetAttrInode(&inode, op.req); err != nil {
		return err
	}
	if op.req.Size != nil {
		newSize := *op.req.Size
		if newSize < inode.Size {
			var startDelBlock uint64
			if newSize > 0 {
				startDelBlock = ((newSize - 1) / BlobSize) + 1
			}
			keys, err := s.collectDataKeysFrom(s.batch, op.ino, startDelBlock)
			if err != nil {
				return err
			}
			op.deleteKeys = keys

			if err := s.resolveWriteBlockSpecs(s.batch, op.ino, inode.Size, op.tailSpecs); err != nil {
				return err
			}
		} else {
			op.tailSpecs = nil
		}
	}
	op.inode = inode
	return nil
}

func (s *StoreActor) mutateSetAttrCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*setAttrOp)
	oldSize := op.inode.Size
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	s.applySetAttrFields(&op.inode, op.req, nowNano, seq)

	if op.req.Size != nil && *op.req.Size < oldSize {
		s.applyBlockMutations(op.ino, op.tailSpecs, op.deleteKeys)
	}

	_ = BatchSetInodeDeferred(s.batch, op.inode)
	op.attr = op.inode.ToAttribute()
}
