package store

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

type RenameReq struct {
	OldParentIno     uint64
	OldName          []byte
	NewParentIno     uint64
	NewName          []byte
	Flags            uint32
	TargetOpenHandle bool
	Epoch            uint64
}

type rmdirNodeOp struct {
	parentIno uint64
	name      []byte

	parent    Inode
	target    Inode
	xattrKeys [][]byte
}

func validateEntryNameAllowEmpty(name []byte) error {
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalid
	}
	return nil
}

func (s *StoreActor) readLiveDir(reader pebbleReader, ino uint64) (Inode, error) {
	dir, err := s.readInode(reader, ino)
	if err != nil {
		return Inode{}, err
	}
	if dir.Mode&S_IFMT != S_IFDIR {
		return Inode{}, ErrNotDir
	}
	if dir.Nlink == 0 {
		return Inode{}, ErrNotFound
	}
	return dir, nil
}

func (s *StoreActor) readLiveChildByEntry(reader pebbleReader, parentIno uint64, name []byte) (Inode, error) {
	entry, err := s.readEntry(reader, parentIno, name)
	if err != nil {
		return Inode{}, err
	}
	child, err := s.readInode(reader, entry.Ino)
	if err != nil {
		return Inode{}, err
	}
	if child.Nlink == 0 {
		return Inode{}, ErrNotFound
	}
	return child, nil
}

func (s *StoreActor) isDirEmpty(reader pebbleReader, ino uint64) (empty bool, err error) {
	iter, err := reader.NewIter(nil)
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	valid := iter.SeekPrefixGE(EntryPrefix(ino))
	if iterErr := iter.Error(); iterErr != nil {
		return false, iterErr
	}
	return !valid, nil
}

func (s *StoreActor) collectXattrKeys(reader pebbleReader, ino uint64) (keys [][]byte, err error) {
	iter, err := reader.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	prefix := XattrPrefix(ino)
	for valid := iter.SeekPrefixGE(prefix); valid; valid = iter.Next() {
		if _, _, decErr := DecodeXattrKey(iter.Key()); decErr != nil {
			return nil, decErr
		}
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, iterErr
	}
	return keys, nil
}

type inodeReclaimPlan struct {
	ino          uint64
	inode        Inode
	hasInode     bool
	hasOrphan    bool
	staleOnAlive bool
	xattrKeys    [][]byte
	dataKeys     []dataKeyEntry
}

func (p *inodeReclaimPlan) isNoop() bool {
	if p.staleOnAlive {
		return !p.hasOrphan
	}
	return !p.hasInode && !p.hasOrphan && len(p.xattrKeys) == 0 && len(p.dataKeys) == 0
}

func (s *StoreActor) hasOrphanMarker(reader pebbleReader, ino uint64) (bool, error) {
	var keyBuf [OrphanKeyLen]byte
	putOrphanKey(keyBuf[:], ino)
	_, closer, err := reader.Get(keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}

func (s *StoreActor) readOrphanKind(reader pebbleReader, ino uint64) (kind byte, ok bool, err error) {
	var keyBuf [OrphanKeyLen]byte
	putOrphanKey(keyBuf[:], ino)
	val, closer, getErr := reader.Get(keyBuf[:])
	if errors.Is(getErr, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if getErr != nil {
		return 0, false, getErr
	}
	kind, decErr := DecodeOrphanKind(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return 0, false, errors.Join(decErr, closeErr)
	}
	return kind, true, nil
}

func (s *StoreActor) buildInodeReclaimPlan(reader pebbleReader, ino uint64, target *Inode, hasInode bool) (inodeReclaimPlan, error) {
	xattrKeys, err := s.collectXattrKeys(reader, ino)
	if err != nil {
		return inodeReclaimPlan{}, err
	}
	dataKeys, err := s.collectDataKeys(reader, ino)
	if err != nil {
		return inodeReclaimPlan{}, err
	}
	return inodeReclaimPlan{
		ino:       ino,
		inode:     *target,
		hasInode:  hasInode,
		xattrKeys: xattrKeys,
		dataKeys:  dataKeys,
	}, nil
}

func (s *StoreActor) applyInodeReclaimPlan(plan *inodeReclaimPlan) {
	if plan.staleOnAlive {
		if plan.hasOrphan {
			BatchDeleteOrphanDeferred(s.batch, plan.ino)
		}
		return
	}
	BatchDeleteInodeDeferred(s.batch, plan.ino)
	BatchDeleteOrphanDeferred(s.batch, plan.ino)
	for _, k := range plan.xattrKeys {
		BatchDeleteDeferred(s.batch, k)
	}
	s.deleteDataKeyEntries(plan.dataKeys)
	s.discardDirtyAtime(plan.ino)
	if plan.hasInode {
		s.stat.Inodes -= min(s.stat.Inodes, 1)
		s.stat.LogicalBytes -= min(s.stat.LogicalBytes, plan.inode.Size)
	}
}

func (s *StoreActor) Rmdir(ctx context.Context, parentIno uint64, name []byte, epoch ...uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return s.submitStaleEpoch(ctx, opRmdir, ep)
	}
	if err := validateNodeIno(parentIno); err != nil {
		return err
	}
	if err := validateEntryNameAllowEmpty(name); err != nil {
		return err
	}

	op := &rmdirNodeOp{
		parentIno: parentIno,
		name:      name,
	}
	cmd := s.acquireCmd()
	cmd.op = opRmdir
	cmd.epoch = ep
	cmd.req = op
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateRmdirCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*rmdirNodeOp)
	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	parent, err := s.readLiveDir(s.batch, op.parentIno)
	if err != nil {
		return err
	}
	if len(op.name) == 0 {
		return ErrNotFound
	}
	if bytes.Equal(op.name, []byte("..")) {
		return ErrNotEmpty
	}
	if bytes.Equal(op.name, []byte(".")) {
		return ErrInvalid
	}

	target, err := s.readLiveChildByEntry(s.batch, op.parentIno, op.name)
	if err != nil {
		return err
	}
	if target.Ino == RootIno {
		return ErrInvalid
	}
	if target.Flags&FlagImmutable != 0 {
		return ErrPerm
	}
	if target.Mode&S_IFMT != S_IFDIR {
		return ErrNotDir
	}

	empty, err := s.isDirEmpty(s.batch, target.Ino)
	if err != nil {
		return err
	}
	if !empty {
		return ErrNotEmpty
	}

	xattrKeys, err := s.collectXattrKeys(s.batch, target.Ino)
	if err != nil {
		return err
	}

	op.parent = parent
	op.target = target
	op.xattrKeys = xattrKeys
	return nil
}

func (s *StoreActor) mutateRmdirCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*rmdirNodeOp)
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	if op.parent.Nlink > 0 {
		op.parent.Nlink--
	}
	op.parent.Mtime = nowNano
	op.parent.Ctime = nowNano
	op.parent.WriteSeq = seq

	BatchDeleteEntryDeferred(s.batch, op.parentIno, op.name)
	BatchDeleteInodeDeferred(s.batch, op.target.Ino)
	BatchDeleteOrphanDeferred(s.batch, op.target.Ino)
	for _, k := range op.xattrKeys {
		BatchDeleteDeferred(s.batch, k)
	}
	_ = BatchSetInodeDeferred(s.batch, op.parent)

	s.stat.Inodes -= min(s.stat.Inodes, 1)
	s.stat.LogicalBytes -= min(s.stat.LogicalBytes, op.target.Size)
	s.discardDirtyAtime(op.target.Ino)
}

type unlinkNodeOp struct {
	parentIno  uint64
	name       []byte
	openHandle bool

	parent  Inode
	target  Inode
	reclaim inodeReclaimPlan
}

type purgeOrphanOp struct {
	ino  uint64
	plan inodeReclaimPlan
}

type cleanOrphansOp struct {
	plans []inodeReclaimPlan
}

func (s *StoreActor) Unlink(ctx context.Context, parentIno uint64, name []byte, openHandle bool, epoch ...uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return s.submitStaleEpoch(ctx, opUnlink, ep)
	}
	if err := validateNodeIno(parentIno); err != nil {
		return err
	}
	if err := validateEntryNameAllowEmpty(name); err != nil {
		return err
	}

	op := &unlinkNodeOp{
		parentIno:  parentIno,
		name:       name,
		openHandle: openHandle,
	}
	cmd := s.acquireCmd()
	cmd.op = opUnlink
	cmd.epoch = ep
	cmd.req = op
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateUnlinkCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*unlinkNodeOp)
	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	parent, err := s.readLiveDir(s.batch, op.parentIno)
	if err != nil {
		return err
	}
	if len(op.name) == 0 {
		return ErrNotFound
	}
	if isDotOrDotDot(op.name) {
		return ErrInvalid
	}

	target, err := s.readLiveChildByEntry(s.batch, op.parentIno, op.name)
	if err != nil {
		return err
	}
	if target.Ino == RootIno {
		return ErrInvalid
	}
	if target.Flags&FlagImmutable != 0 {
		return ErrPerm
	}
	if target.Mode&S_IFMT == S_IFDIR {
		return ErrIsDir
	}

	var reclaim inodeReclaimPlan
	if target.Nlink <= 1 && !op.openHandle {
		reclaim, err = s.buildInodeReclaimPlan(s.batch, target.Ino, &target, true)
		if err != nil {
			return err
		}
	}

	op.parent = parent
	op.target = target
	op.reclaim = reclaim
	return nil
}

func (s *StoreActor) mutateUnlinkCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*unlinkNodeOp)
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	op.parent.Mtime = nowNano
	op.parent.Ctime = nowNano
	op.parent.WriteSeq = seq

	BatchDeleteEntryDeferred(s.batch, op.parentIno, op.name)
	_ = BatchSetInodeDeferred(s.batch, op.parent)

	if op.target.Nlink > 1 {
		op.target.Nlink--
		op.target.Ctime = nowNano
		op.target.WriteSeq = seq
		_ = BatchSetInodeDeferred(s.batch, op.target)
		BatchDeleteOrphanDeferred(s.batch, op.target.Ino)
		return
	}

	if op.openHandle {
		op.target.Nlink = 0
		op.target.Ctime = nowNano
		op.target.WriteSeq = seq
		_ = BatchSetInodeDeferred(s.batch, op.target)
		BatchSetOrphanDeferred(s.batch, op.target.Ino)
		return
	}

	s.applyInodeReclaimPlan(&op.reclaim)
}

func (s *StoreActor) PurgeOrphanInode(ctx context.Context, ino uint64, epoch ...uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return s.submitStaleEpoch(ctx, opPurgeOrphan, ep)
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}

	op := &purgeOrphanOp{ino: ino}
	cmd := s.acquireCmd()
	cmd.op = opPurgeOrphan
	cmd.epoch = ep
	cmd.req = op
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validatePurgeOrphanCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*purgeOrphanOp)
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	hasInode := err == nil
	if hasInode && inode.Nlink > 0 {
		return ErrInvalid
	}

	hasOrphan, err := s.hasOrphanMarker(s.batch, op.ino)
	if err != nil {
		return err
	}
	plan, err := s.buildInodeReclaimPlan(s.batch, op.ino, &inode, hasInode)
	if err != nil {
		return err
	}
	plan.hasOrphan = hasOrphan
	if !plan.isNoop() && s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	op.plan = plan
	return nil
}

func (s *StoreActor) mutatePurgeOrphanCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*purgeOrphanOp)
	if op.plan.isNoop() {
		return
	}
	s.stat.NextWriteSeq++
	s.applyInodeReclaimPlan(&op.plan)
}

func (s *StoreActor) CleanOrphans(ctx context.Context) error {
	if s.readOnly {
		return ErrReadOnly
	}

	op := &cleanOrphansOp{}
	cmd := s.acquireCmd()
	cmd.op = opCleanOrphans
	cmd.req = op
	return s.submit(ctx, cmd)
}

func (s *StoreActor) collectOrphanInodes(reader pebbleReader) (inos []uint64, err error) {
	iter, err := reader.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	prefix := OrphanPrefix()
	for valid := iter.SeekGE(prefix); valid && bytes.HasPrefix(iter.Key(), prefix); valid = iter.Next() {
		ino, decErr := DecodeOrphanKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		inos = append(inos, ino)
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, iterErr
	}
	return inos, nil
}

func (s *StoreActor) planCleanOrphans(reader pebbleReader) ([]inodeReclaimPlan, error) {
	orphanInos, err := s.collectOrphanInodes(reader)
	if err != nil {
		return nil, err
	}
	if len(orphanInos) == 0 {
		return nil, nil
	}

	plans := make([]inodeReclaimPlan, 0, len(orphanInos))
	hasMutation := false
	for _, ino := range orphanInos {
		if ino == RootIno {
			plans = append(plans, inodeReclaimPlan{
				ino:          ino,
				hasOrphan:    true,
				staleOnAlive: true,
			})
			hasMutation = true
			continue
		}
		inode, err := s.readInode(reader, ino)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		hasInode := err == nil
		if hasInode && inode.Nlink > 0 {
			plans = append(plans, inodeReclaimPlan{
				ino:          ino,
				hasOrphan:    true,
				staleOnAlive: true,
			})
			hasMutation = true
			continue
		}
		plan, err := s.buildInodeReclaimPlan(reader, ino, &inode, hasInode)
		if err != nil {
			return nil, err
		}
		plan.hasOrphan = true
		plans = append(plans, plan)
		if !plan.isNoop() {
			hasMutation = true
		}
	}
	if hasMutation && s.stat.NextWriteSeq == math.MaxUint64 {
		return nil, ErrNoSpace
	}
	return plans, nil
}

func (s *StoreActor) validateCleanOrphansCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*cleanOrphansOp)
	plans, err := s.planCleanOrphans(s.batch)
	if err != nil {
		return err
	}
	op.plans = plans
	return nil
}

func (s *StoreActor) mutateCleanOrphansCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*cleanOrphansOp)
	if len(op.plans) == 0 {
		return
	}
	s.stat.NextWriteSeq++
	for i := range op.plans {
		s.applyInodeReclaimPlan(&op.plans[i])
	}
}

type renameNodeOp struct {
	req RenameReq

	noop          bool
	crossParent   bool
	oldIsDir      bool
	newIsDir      bool
	hasNew        bool
	reclaimTarget bool
	oldParent     Inode
	newParent     Inode
	oldInode      Inode
	newInode      Inode
	reclaim       inodeReclaimPlan
}

func (s *StoreActor) isSubdirectory(reader pebbleReader, childIno, ancestorIno uint64) (bool, error) {
	seen := make(map[uint64]struct{})
	for curr := childIno; ; {
		if curr == ancestorIno {
			return true, nil
		}
		if curr == RootIno {
			return false, nil
		}
		if curr == 0 {
			return false, ErrNotFound
		}
		if _, ok := seen[curr]; ok {
			return false, ErrInvalid
		}
		seen[curr] = struct{}{}
		inode, err := s.readLiveDir(reader, curr)
		if err != nil {
			return false, err
		}
		curr = inode.ParentIno
	}
}

func (s *StoreActor) Rename(ctx context.Context, req RenameReq) error {
	if s.readOnly {
		return ErrReadOnly
	}
	if s.isDefinitelyStaleEpoch(req.Epoch) {
		return s.submitStaleEpoch(ctx, opRename, req.Epoch)
	}
	if req.Flags&RenameWhiteout != 0 {
		return ErrNotSupported
	}
	if req.Flags&^(RenameNoreplace|RenameExchange) != 0 {
		return ErrInvalid
	}
	if req.Flags&RenameNoreplace != 0 && req.Flags&RenameExchange != 0 {
		return ErrInvalid
	}
	if err := validateNodeIno(req.OldParentIno); err != nil {
		return err
	}
	if err := validateNodeIno(req.NewParentIno); err != nil {
		return err
	}
	if err := validateEntryNameAllowEmpty(req.OldName); err != nil {
		return err
	}
	if err := validateEntryNameAllowEmpty(req.NewName); err != nil {
		return err
	}

	op := &renameNodeOp{req: req}
	cmd := s.acquireCmd()
	cmd.op = opRename
	cmd.epoch = req.Epoch
	cmd.req = op
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateRenameCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*renameNodeOp)
	req := op.req

	oldParent, err := s.readLiveDir(s.batch, req.OldParentIno)
	if err != nil {
		return err
	}
	newParent := oldParent
	crossParent := req.OldParentIno != req.NewParentIno
	if crossParent {
		newParent, err = s.readLiveDir(s.batch, req.NewParentIno)
		if err != nil {
			return err
		}
	}
	if len(req.OldName) == 0 || len(req.NewName) == 0 {
		return ErrNotFound
	}
	if isDotOrDotDot(req.OldName) || isDotOrDotDot(req.NewName) {
		return ErrInvalid
	}

	oldInode, err := s.readLiveChildByEntry(s.batch, req.OldParentIno, req.OldName)
	if err != nil {
		return err
	}
	if oldInode.Ino == RootIno {
		return ErrInvalid
	}
	if !crossParent && bytes.Equal(req.OldName, req.NewName) {
		if req.Flags == RenameNoreplace {
			return ErrExist
		}
		op.noop = true
		return nil
	}
	if oldInode.Flags&FlagImmutable != 0 {
		return ErrPerm
	}

	newInode, err := s.readLiveChildByEntry(s.batch, req.NewParentIno, req.NewName)
	hasNew := err == nil
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if hasNew && newInode.Ino == RootIno {
		return ErrInvalid
	}
	if hasNew && newInode.Flags&FlagImmutable != 0 {
		return ErrPerm
	}
	if !hasNew && req.Flags == RenameExchange {
		return ErrNotFound
	}
	if hasNew && req.Flags == RenameNoreplace {
		return ErrExist
	}
	if hasNew && oldInode.Ino == newInode.Ino {
		op.noop = true
		return nil
	}
	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}

	oldIsDir := oldInode.Mode&S_IFMT == S_IFDIR
	newIsDir := hasNew && newInode.Mode&S_IFMT == S_IFDIR

	if oldIsDir && crossParent {
		var isSub bool
		isSub, err = s.isSubdirectory(s.batch, req.NewParentIno, oldInode.Ino)
		if err != nil {
			return err
		}
		if isSub {
			return ErrInvalid
		}
	}

	var reclaim inodeReclaimPlan
	var reclaimTarget bool

	if req.Flags == RenameExchange {
		if newIsDir && crossParent {
			var isSub bool
			isSub, err = s.isSubdirectory(s.batch, req.OldParentIno, newInode.Ino)
			if err != nil {
				return err
			}
			if isSub {
				return ErrInvalid
			}
		}
		if crossParent && oldIsDir && !newIsDir && newParent.Nlink >= MaxNlink ||
			crossParent && !oldIsDir && newIsDir && oldParent.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}
	} else {
		if hasNew {
			if oldIsDir && !newIsDir {
				return ErrNotDir
			}
			if !oldIsDir && newIsDir {
				return ErrIsDir
			}
			if newIsDir {
				var empty bool
				empty, err = s.isDirEmpty(s.batch, newInode.Ino)
				if err != nil {
					return err
				}
				if !empty {
					return ErrNotEmpty
				}
			}
			if (newIsDir || newInode.Nlink <= 1) && !req.TargetOpenHandle {
				reclaim, err = s.buildInodeReclaimPlan(s.batch, newInode.Ino, &newInode, true)
				if err != nil {
					return err
				}
				reclaimTarget = true
			}
		}
		if oldIsDir && crossParent && !hasNew && newParent.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}
	}

	op.noop = false
	op.crossParent = crossParent
	op.oldIsDir = oldIsDir
	op.newIsDir = newIsDir
	op.hasNew = hasNew
	op.reclaimTarget = reclaimTarget
	op.oldParent = oldParent
	op.newParent = newParent
	op.oldInode = oldInode
	op.newInode = newInode
	op.reclaim = reclaim
	return nil
}

func (s *StoreActor) mutateRenameCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*renameNodeOp)
	if op.noop {
		return
	}
	req := op.req
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	if req.Flags == RenameExchange {
		BatchSetEntryDeferred(s.batch, req.OldParentIno, req.OldName, Entry{
			Ino:  op.newInode.Ino,
			Mode: op.newInode.Mode,
		})
		BatchSetEntryDeferred(s.batch, req.NewParentIno, req.NewName, Entry{
			Ino:  op.oldInode.Ino,
			Mode: op.oldInode.Mode,
		})
		if op.crossParent {
			if op.oldIsDir || op.oldInode.Nlink == 1 {
				op.oldInode.ParentIno = req.NewParentIno
			}
			if op.newIsDir || op.newInode.Nlink == 1 {
				op.newInode.ParentIno = req.OldParentIno
			}
			if op.oldIsDir != op.newIsDir {
				if op.oldIsDir {
					if op.oldParent.Nlink > 0 {
						op.oldParent.Nlink--
					}
					op.newParent.Nlink++
				} else {
					op.oldParent.Nlink++
					if op.newParent.Nlink > 0 {
						op.newParent.Nlink--
					}
				}
			}
		}
		op.oldInode.Ctime = nowNano
		op.oldInode.WriteSeq = seq
		op.newInode.Ctime = nowNano
		op.newInode.WriteSeq = seq
		_ = BatchSetInodeDeferred(s.batch, op.oldInode)
		_ = BatchSetInodeDeferred(s.batch, op.newInode)
	} else {
		BatchDeleteEntryDeferred(s.batch, req.OldParentIno, req.OldName)
		BatchSetEntryDeferred(s.batch, req.NewParentIno, req.NewName, Entry{
			Ino:  op.oldInode.Ino,
			Mode: op.oldInode.Mode,
		})
		if op.crossParent && (op.oldIsDir || op.oldInode.Nlink == 1) {
			op.oldInode.ParentIno = req.NewParentIno
		}
		op.oldInode.Ctime = nowNano
		op.oldInode.WriteSeq = seq
		_ = BatchSetInodeDeferred(s.batch, op.oldInode)

		if op.oldIsDir {
			if op.crossParent {
				if op.oldParent.Nlink > 0 {
					op.oldParent.Nlink--
				}
				if !op.hasNew {
					op.newParent.Nlink++
				}
			} else if op.hasNew {
				if op.oldParent.Nlink > 0 {
					op.oldParent.Nlink--
				}
			}
		}

		if op.hasNew {
			switch {
			case op.reclaimTarget:
				s.applyInodeReclaimPlan(&op.reclaim)
			case !op.newIsDir && op.newInode.Nlink > 1:
				op.newInode.Nlink--
				op.newInode.Ctime = nowNano
				op.newInode.WriteSeq = seq
				_ = BatchSetInodeDeferred(s.batch, op.newInode)
				BatchDeleteOrphanDeferred(s.batch, op.newInode.Ino)
			default:
				op.newInode.Nlink = 0
				op.newInode.Ctime = nowNano
				op.newInode.WriteSeq = seq
				_ = BatchSetInodeDeferred(s.batch, op.newInode)
				BatchSetOrphanDeferred(s.batch, op.newInode.Ino)
			}
		}
	}

	op.oldParent.Mtime = nowNano
	op.oldParent.Ctime = nowNano
	op.oldParent.WriteSeq = seq
	_ = BatchSetInodeDeferred(s.batch, op.oldParent)

	if op.crossParent {
		op.newParent.Mtime = nowNano
		op.newParent.Ctime = nowNano
		op.newParent.WriteSeq = seq
		_ = BatchSetInodeDeferred(s.batch, op.newParent)
	}
}
