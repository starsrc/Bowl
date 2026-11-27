package engine

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

type epochCtxKey struct{}

func WithWriterEpoch(ctx context.Context, epoch uint64) context.Context {
	return context.WithValue(ctx, epochCtxKey{}, epoch)
}

func writerEpochFromContext(ctx context.Context) uint64 {
	if ctx == nil {
		return 0
	}
	v, ok := ctx.Value(epochCtxKey{}).(uint64)
	if !ok {
		return 0
	}
	return v
}

type SetAttrReq struct {
	Mode     *uint32
	UID      *uint32
	GID      *uint32
	Size     *uint64
	Atime    *int64
	Mtime    *int64
	AtimeNow bool
	MtimeNow bool
	CtimeNow bool
	Flags    *uint32
}

type RenameReq struct {
	OldParentIno       uint64
	OldName            string
	NewParentIno       uint64
	NewName            string
	Flags              uint32
	TargetOpenInClient bool
}

type opQuotaTracker struct {
	disk     *DiskStore
	reserved uint64
	released uint64
}

func (q *opQuotaTracker) reserve(delta uint64) error {
	if delta == 0 {
		return nil
	}
	if delta <= q.released {
		q.released -= delta
		return nil
	}
	extra := delta - q.released
	if err := q.disk.reserveQuota(extra); err != nil {
		return err
	}
	q.released = 0
	q.reserved += extra
	return nil
}

func (q *opQuotaTracker) release(delta uint64) {
	q.released += delta
}

func (q *opQuotaTracker) onCommit() {
	if q.released > 0 {
		q.disk.releaseQuota(q.released)
		q.released = 0
	}
}

func (q *opQuotaTracker) onRollback() {
	if q.reserved > 0 {
		q.disk.releaseQuota(q.reserved)
		q.reserved = 0
	}
	q.released = 0
}

func (d *DiskStore) execBranchOp(
	ctx context.Context,
	branch string,
	syncCommit bool,
	checkEpoch bool,
	fn func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error,
) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	mu, err := d.acquireBranchLock(branch)
	if err != nil {
		return err
	}
	defer mu.Unlock()

	batchGen := d.commitCacheGen.Load()
	batch := d.db.NewIndexedBatch()
	defer func() { _ = batch.Close() }()

	bm, err := readBranchMeta(batch, d.keys, branch)
	if err != nil {
		return err
	}
	if checkEpoch {
		if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != bm.WriterEpoch {
			return ErrStaleEpoch
		}
	}

	var cursor branchLayerCursor
	d.initBranchCursor(&cursor, batch, bm, batchGen)
	quota := opQuotaTracker{disk: d}

	if err = fn(batch, &quota, bm, &cursor); err != nil {
		quota.onRollback()
		return err
	}

	var commitErr error
	if batch.Count() > 0 {
		if syncCommit {
			commitErr = d.commitBatchSync(ctx, batch)
		} else {
			commitErr = d.commitBatchNoSync(batch)
		}
	}
	if commitErr != nil {
		quota.onRollback()
		return commitErr
	}
	quota.onCommit()
	return nil
}

func (d *DiskStore) execBranchWrite(
	ctx context.Context,
	branch string,
	fn func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error,
) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if d.closing.Load() {
		return ErrClosed
	}
	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()
	return d.execBranchOp(ctx, branch, false, true, fn)
}

func validateName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') || strings.ContainsRune(name, '\x00') {
		return ErrInvalid
	}
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	return nil
}

func validateXattrName(name string) error {
	if name == "" || strings.ContainsRune(name, '\x00') {
		return ErrInvalid
	}
	if len(name) > MaxXattrNameBytes {
		return ErrNameTooLong
	}
	return nil
}

func (d *DiskStore) Mkdir(ctx context.Context, branch string, parentIno uint64, name string, mode, uid, gid uint32) (Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return Inode{}, ErrInvalid
	}
	if err := validateName(name); err != nil {
		return Inode{}, err
	}
	if mode > 0o177777 || uid > math.MaxInt32 || gid > math.MaxInt32 {
		return Inode{}, ErrInvalid
	}
	modeType := mode & ModeTypeMask
	if modeType != 0 && modeType != ModeDir {
		return Inode{}, ErrInvalid
	}
	normMode := ModeDir | (mode & ModePermMask)

	var child Inode
	err := d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		parent, err := cursor.getInode(parentIno)
		if err != nil {
			return err
		}
		if parent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		if parent.Nlink == 0 {
			return ErrNotFound
		}
		if _, entryErr := cursor.getEntry(parentIno, []byte(name)); entryErr == nil {
			return ErrExist
		} else if !errors.Is(entryErr, ErrNotFound) {
			return entryErr
		}
		if parent.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}

		inoNum, err := d.allocIno()
		if err != nil {
			return err
		}
		childMode := normMode
		childGID := gid
		if parent.Mode&ModeSetGID != 0 {
			childGID = parent.GID
			childMode |= ModeSetGID
		}
		nowNano := d.now().UnixNano()
		child = Inode{
			Ino:        inoNum,
			ParentIno:  parentIno,
			Mode:       childMode,
			Nlink:      2,
			UID:        uid,
			GID:        childGID,
			Size:       DefaultDirSize,
			Atime:      nowNano,
			Mtime:      nowNano,
			Ctime:      nowNano,
			Generation: 1,
		}
		parent.Nlink++
		parent.Mtime = nowNano
		parent.Ctime = nowNano

		childRaw, err := EncodeInode(child)
		if err != nil {
			return err
		}
		parentRaw, err := EncodeInode(parent)
		if err != nil {
			return err
		}

		work := bm.WorkLayerID
		if setErr := batch.Set(dk.LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
			return setErr
		}
		ent := Entry{Ino: child.Ino, Mode: child.Mode}
		return batch.Set(dk.LayerEntryKey(work, parentIno, []byte(name)), EncodeLiveEnvelope(EncodeEntry(ent)), nil)
	})
	if err != nil {
		return Inode{}, err
	}
	return child, nil
}

func (d *DiskStore) Create(ctx context.Context, branch string, parentIno uint64, name string, mode, uid, gid uint32) (Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return Inode{}, ErrInvalid
	}
	if err := validateName(name); err != nil {
		return Inode{}, err
	}
	if mode > 0o177777 || uid > math.MaxInt32 || gid > math.MaxInt32 {
		return Inode{}, ErrInvalid
	}
	modeType := mode & ModeTypeMask
	if modeType != 0 && modeType != ModeRegular {
		return Inode{}, ErrInvalid
	}
	normMode := ModeRegular | (mode & ModePermMask)

	var child Inode
	err := d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		parent, err := cursor.getInode(parentIno)
		if err != nil {
			return err
		}
		if parent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		if parent.Nlink == 0 {
			return ErrNotFound
		}
		if _, entryErr := cursor.getEntry(parentIno, []byte(name)); entryErr == nil {
			return ErrExist
		} else if !errors.Is(entryErr, ErrNotFound) {
			return entryErr
		}

		inoNum, err := d.allocIno()
		if err != nil {
			return err
		}
		childGID := gid
		if parent.Mode&ModeSetGID != 0 {
			childGID = parent.GID
		}
		nowNano := d.now().UnixNano()
		child = Inode{
			Ino:        inoNum,
			ParentIno:  parentIno,
			Mode:       normMode,
			Nlink:      1,
			UID:        uid,
			GID:        childGID,
			Atime:      nowNano,
			Mtime:      nowNano,
			Ctime:      nowNano,
			Generation: 1,
		}
		parent.Mtime = nowNano
		parent.Ctime = nowNano

		childRaw, err := EncodeInode(child)
		if err != nil {
			return err
		}
		parentRaw, err := EncodeInode(parent)
		if err != nil {
			return err
		}

		work := bm.WorkLayerID
		if setErr := batch.Set(dk.LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
			return setErr
		}
		ent := Entry{Ino: child.Ino, Mode: child.Mode}
		return batch.Set(dk.LayerEntryKey(work, parentIno, []byte(name)), EncodeLiveEnvelope(EncodeEntry(ent)), nil)
	})
	if err != nil {
		return Inode{}, err
	}
	return child, nil
}

func (d *DiskStore) Symlink(ctx context.Context, branch string, parentIno uint64, name, target string, uid, gid uint32) (Inode, error) {
	if err := validateName(name); err != nil {
		return Inode{}, err
	}
	if target == "" || len(target) > MaxSymlinkTargetBytes || strings.ContainsRune(target, '\x00') {
		return Inode{}, ErrInvalid
	}

	var child Inode
	err := d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		parent, err := cursor.getInode(parentIno)
		if err != nil {
			return err
		}
		if parent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		if parent.Nlink == 0 {
			return ErrNotFound
		}
		if _, entryErr := cursor.getEntry(parentIno, []byte(name)); entryErr == nil {
			return ErrExist
		} else if !errors.Is(entryErr, ErrNotFound) {
			return entryErr
		}

		inoNum, err := d.allocIno()
		if err != nil {
			return err
		}
		childGID := gid
		if parent.Mode&ModeSetGID != 0 {
			childGID = parent.GID
		}
		nowNano := d.now().UnixNano()
		child = Inode{
			Ino:           inoNum,
			ParentIno:     parentIno,
			Mode:          ModeSymlink | 0o777,
			Nlink:         1,
			UID:           uid,
			GID:           childGID,
			Size:          uint64(len(target)),
			Atime:         nowNano,
			Mtime:         nowNano,
			Ctime:         nowNano,
			Generation:    1,
			SymlinkTarget: []byte(target),
		}
		parent.Mtime = nowNano
		parent.Ctime = nowNano

		childRaw, err := EncodeInode(child)
		if err != nil {
			return err
		}
		parentRaw, err := EncodeInode(parent)
		if err != nil {
			return err
		}

		work := bm.WorkLayerID
		if setErr := batch.Set(dk.LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
			return setErr
		}
		ent := Entry{Ino: child.Ino, Mode: child.Mode}
		return batch.Set(dk.LayerEntryKey(work, parentIno, []byte(name)), EncodeLiveEnvelope(EncodeEntry(ent)), nil)
	})
	if err != nil {
		return Inode{}, err
	}
	return child, nil
}

func (d *DiskStore) Link(ctx context.Context, branch string, ino, newParentIno uint64, newName string) (Inode, error) {
	if err := validateName(newName); err != nil {
		return Inode{}, err
	}

	var out Inode
	err := d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		parent, err := cursor.getInode(newParentIno)
		if err != nil {
			return err
		}
		if parent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		if parent.Nlink == 0 {
			return ErrNotFound
		}
		target, err := cursor.getInode(ino)
		if err != nil {
			return err
		}
		if target.Mode&ModeTypeMask == ModeDir {
			return ErrPerm
		}
		if target.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}

		work := bm.WorkLayerID
		promoteOrphan := false
		if target.Nlink == 0 {
			if target.Mode&ModeTypeMask != ModeRegular {
				return ErrNotFound
			}
			kind, hasOrphan, orphanErr := readLayerOrphanKind(batch, dk, work, target.Ino)
			if orphanErr != nil {
				return orphanErr
			}
			if !hasOrphan || kind != OrphanTmpfileLinkable {
				return ErrNotFound
			}
			promoteOrphan = true
		}

		if _, entryErr := cursor.getEntry(newParentIno, []byte(newName)); entryErr == nil {
			return ErrExist
		} else if !errors.Is(entryErr, ErrNotFound) {
			return entryErr
		}

		wasEverLinked := target.Flags&InodeFlagEverLinked != 0
		nowNano := d.now().UnixNano()
		if promoteOrphan {
			target.Nlink = 1
			target.ParentIno = newParentIno
		} else {
			target.Nlink++
			target.Flags |= InodeFlagEverLinked
		}
		target.Ctime = nowNano
		target.WriteSeq++
		parent.Mtime = nowNano
		parent.Ctime = nowNano

		targetRaw, err := EncodeInode(target)
		if err != nil {
			return err
		}
		parentRaw, err := EncodeInode(parent)
		if err != nil {
			return err
		}

		if setErr := batch.Set(dk.LayerInodeKey(work, target.Ino), EncodeLiveEnvelope(targetRaw), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
			return setErr
		}
		ent := Entry{Ino: target.Ino, Mode: target.Mode}
		if setErr := batch.Set(dk.LayerEntryKey(work, newParentIno, []byte(newName)), EncodeLiveEnvelope(EncodeEntry(ent)), nil); setErr != nil {
			return setErr
		}
		out = target
		if promoteOrphan {
			return batch.Delete(dk.LayerOrphanKey(work, target.Ino), nil)
		}
		if !wasEverLinked && target.ParentIno != 0 {
			if setErr := batch.Set(dk.LayerParentKey(work, target.Ino, target.ParentIno), EncodeLiveEnvelope(nil), nil); setErr != nil {
				return setErr
			}
		}
		return batch.Set(dk.LayerParentKey(work, target.Ino, newParentIno), EncodeLiveEnvelope(nil), nil)
	})
	if err != nil {
		return Inode{}, err
	}
	return out, nil
}

func (d *DiskStore) Unlink(ctx context.Context, branch string, parentIno uint64, name string, openInClient bool) error {
	if err := validateName(name); err != nil {
		return err
	}
	return d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		parent, err := cursor.getInode(parentIno)
		if err != nil {
			return err
		}
		if parent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		if parent.Nlink == 0 {
			return ErrNotFound
		}
		ent, err := cursor.getEntry(parentIno, []byte(name))
		if err != nil {
			return err
		}
		target, err := cursor.getInode(ent.Ino)
		if err != nil {
			return err
		}
		if target.Nlink == 0 {
			return ErrNotFound
		}
		if target.Ino == RootIno {
			return ErrInvalid
		}
		if target.Flags&FlagImmutable != 0 {
			return ErrPerm
		}
		if target.Mode&ModeTypeMask == ModeDir {
			return ErrIsDir
		}

		work := bm.WorkLayerID
		nowNano := d.now().UnixNano()
		parent.Mtime = nowNano
		parent.Ctime = nowNano
		parentRaw, err := EncodeInode(parent)
		if err != nil {
			return err
		}

		if setErr := batch.Set(dk.LayerEntryKey(work, parentIno, []byte(name)), EncodeTombstone(), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
			return setErr
		}

		if target.Nlink <= 1 && openInClient {
			target.Nlink = 0
			target.Ctime = nowNano
			target.WriteSeq++
			rawTarget, encErr := EncodeInode(target)
			if encErr != nil {
				return encErr
			}
			if setErr := batch.Set(dk.LayerInodeKey(work, target.Ino), EncodeLiveEnvelope(rawTarget), nil); setErr != nil {
				return setErr
			}
			return batch.Set(dk.LayerOrphanKey(work, target.Ino), []byte{OrphanUnlinked}, nil)
		}

		freedWork, freedLogical, tbErr := tombstoneOrDecInode(batch, dk, work, cursor, target, nowNano)
		if tbErr != nil {
			return tbErr
		}
		if freedWork > 0 || freedLogical > 0 {
			bm.WorkOwnedBytes = saturatingSub(bm.WorkOwnedBytes, freedWork)
			bm.LogicalBytes = saturatingSub(bm.LogicalBytes, freedLogical)
			if setErr := batch.Set(dk.MetaBranchKey(bm.Name), EncodeBranchMeta(bm), nil); setErr != nil {
				return setErr
			}
			quota.release(freedWork)
		}
		return nil
	})
}

func tombstoneOrDecInode(batch *pebble.Batch, dk diskKeys, work uint64, cursor *branchLayerCursor, target Inode, nowNano int64) (freedWork, freedLogical uint64, err error) {
	if target.Nlink > 1 {
		target.Nlink--
		target.Ctime = nowNano
		raw, encErr := EncodeInode(target)
		if encErr != nil {
			return 0, 0, encErr
		}
		return 0, 0, batch.Set(dk.LayerInodeKey(work, target.Ino), EncodeLiveEnvelope(raw), nil)
	}

	layers, err := cursor.allLayers()
	if err != nil {
		return 0, 0, err
	}
	if setErr := batch.Set(dk.LayerInodeKey(work, target.Ino), EncodeTombstone(), nil); setErr != nil {
		return 0, 0, setErr
	}
	xnames, err := overlayListXattr(batch, dk, layers, target.Ino)
	if err != nil {
		return 0, 0, err
	}
	for _, xname := range xnames {
		if setErr := batch.Set(dk.LayerXattrKey(work, target.Ino, []byte(xname)), EncodeTombstone(), nil); setErr != nil {
			return 0, 0, setErr
		}
	}
	liveBlks, dropWork, dropEff, err := overlayCollectLiveBlocksRange(batch, dk, layers, target.Ino, 0, math.MaxUint64)
	if err != nil {
		return 0, 0, err
	}
	for _, blkIdx := range liveBlks {
		if setErr := batch.Set(dk.LayerDataKey(work, target.Ino, blkIdx), EncodeTombstone(), nil); setErr != nil {
			return 0, 0, setErr
		}
	}
	return dropWork, dropEff, nil
}

func (d *DiskStore) Rmdir(ctx context.Context, branch string, parentIno uint64, name string) error {
	if name == ".." {
		return ErrNotEmpty
	}
	if err := validateName(name); err != nil {
		return err
	}
	return d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		parent, err := cursor.getInode(parentIno)
		if err != nil {
			return err
		}
		if parent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		if parent.Nlink == 0 {
			return ErrNotFound
		}
		ent, err := cursor.getEntry(parentIno, []byte(name))
		if err != nil {
			return err
		}
		target, err := cursor.getInode(ent.Ino)
		if err != nil {
			return err
		}
		if target.Ino == RootIno {
			return ErrInvalid
		}
		if target.Flags&FlagImmutable != 0 {
			return ErrPerm
		}
		if target.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		layers, err := cursor.allLayers()
		if err != nil {
			return err
		}
		children, err := overlayReadDir(batch, dk, layers, target.Ino)
		if err != nil {
			return err
		}
		if len(children) > 0 {
			return ErrNotEmpty
		}

		work := bm.WorkLayerID
		nowNano := d.now().UnixNano()
		if parent.Nlink > 2 {
			parent.Nlink--
		}
		parent.Mtime = nowNano
		parent.Ctime = nowNano
		parentRaw, err := EncodeInode(parent)
		if err != nil {
			return err
		}
		if setErr := batch.Set(dk.LayerEntryKey(work, parentIno, []byte(name)), EncodeTombstone(), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, target.Ino), EncodeTombstone(), nil); setErr != nil {
			return setErr
		}
		xnames, err := overlayListXattr(batch, dk, layers, target.Ino)
		if err != nil {
			return err
		}
		for _, xname := range xnames {
			if setErr := batch.Set(dk.LayerXattrKey(work, target.Ino, []byte(xname)), EncodeTombstone(), nil); setErr != nil {
				return setErr
			}
		}
		return nil
	})
}

func isOverlaySubdirectory(cursor *branchLayerCursor, childIno, ancestorIno uint64) (bool, error) {
	seen := make(map[uint64]struct{})
	for cur := childIno; ; {
		if cur == ancestorIno {
			return true, nil
		}
		if cur == RootIno || cur == 0 {
			return false, nil
		}
		if _, ok := seen[cur]; ok {
			return false, ErrInvalid
		}
		seen[cur] = struct{}{}
		anc, err := cursor.getInode(cur)
		if err != nil {
			return false, err
		}
		if anc.ParentIno == cur {
			return false, nil
		}
		cur = anc.ParentIno
	}
}

func recordEverLinkedParent(batch *pebble.Batch, dk diskKeys, work uint64, ino *Inode, newParentIno uint64) error {
	if ino.Mode&ModeTypeMask == ModeDir {
		return nil
	}
	if ino.Flags&InodeFlagEverLinked == 0 && ino.Nlink <= 1 {
		return nil
	}
	if ino.Flags&InodeFlagEverLinked == 0 {
		ino.Flags |= InodeFlagEverLinked
		if ino.ParentIno != 0 && ino.ParentIno != newParentIno {
			if setErr := batch.Set(dk.LayerParentKey(work, ino.Ino, ino.ParentIno), EncodeLiveEnvelope(nil), nil); setErr != nil {
				return setErr
			}
		}
	}
	return batch.Set(dk.LayerParentKey(work, ino.Ino, newParentIno), EncodeLiveEnvelope(nil), nil)
}

func (d *DiskStore) Rename(ctx context.Context, branch string, req RenameReq) error {
	if req.OldParentIno == 0 || req.OldParentIno > math.MaxInt64 || req.NewParentIno == 0 || req.NewParentIno > math.MaxInt64 {
		return ErrInvalid
	}
	if req.Flags&^(RenameNoReplace|RenameExchange) != 0 || req.Flags == (RenameNoReplace|RenameExchange) {
		return ErrInvalid
	}
	if err := validateName(req.OldName); err != nil {
		return err
	}
	if err := validateName(req.NewName); err != nil {
		return err
	}
	return d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		oldParent, err := cursor.getInode(req.OldParentIno)
		if err != nil {
			return err
		}
		if oldParent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		if oldParent.Nlink == 0 {
			return ErrNotFound
		}
		crossParent := req.OldParentIno != req.NewParentIno
		newParent := oldParent
		if crossParent {
			newParent, err = cursor.getInode(req.NewParentIno)
			if err != nil {
				return err
			}
			if newParent.Mode&ModeTypeMask != ModeDir {
				return ErrNotDir
			}
			if newParent.Nlink == 0 {
				return ErrNotFound
			}
		}

		srcEnt, err := cursor.getEntry(req.OldParentIno, []byte(req.OldName))
		if err != nil {
			return err
		}
		srcIno, err := cursor.getInode(srcEnt.Ino)
		if err != nil {
			return err
		}
		if srcIno.Nlink == 0 {
			return ErrNotFound
		}
		if srcIno.Ino == RootIno {
			return ErrInvalid
		}
		if !crossParent && req.OldName == req.NewName {
			if req.Flags == RenameNoReplace {
				return ErrExist
			}
			return nil
		}
		if srcIno.Flags&FlagImmutable != 0 {
			return ErrPerm
		}

		var dstEnt Entry
		var dstIno Inode
		hasDst := false
		dstEnt, dstErr := cursor.getEntry(req.NewParentIno, []byte(req.NewName))
		if dstErr == nil {
			dstIno, err = cursor.getInode(dstEnt.Ino)
			if err != nil {
				return err
			}
			if dstIno.Nlink == 0 {
				return ErrNotFound
			}
			if dstIno.Ino == RootIno {
				return ErrInvalid
			}
			if dstIno.Flags&FlagImmutable != 0 {
				return ErrPerm
			}
			if req.Flags == RenameNoReplace {
				return ErrExist
			}
			if dstIno.Ino == srcIno.Ino {
				return nil
			}
			hasDst = true
		} else if !errors.Is(dstErr, ErrNotFound) {
			return dstErr
		}

		if !hasDst && req.Flags == RenameExchange {
			return ErrNotFound
		}

		srcIsDir := srcIno.Mode&ModeTypeMask == ModeDir
		dstIsDir := hasDst && dstIno.Mode&ModeTypeMask == ModeDir

		if srcIsDir && crossParent {
			isSub, subErr := isOverlaySubdirectory(cursor, req.NewParentIno, srcIno.Ino)
			if subErr != nil {
				return subErr
			}
			if isSub {
				return ErrInvalid
			}
		}

		work := bm.WorkLayerID
		nowNano := d.now().UnixNano()

		if req.Flags == RenameExchange {
			if dstIsDir && crossParent {
				isSub, subErr := isOverlaySubdirectory(cursor, req.OldParentIno, dstIno.Ino)
				if subErr != nil {
					return subErr
				}
				if isSub {
					return ErrInvalid
				}
			}
			if crossParent && srcIsDir && !dstIsDir && newParent.Nlink >= MaxNlink {
				return ErrTooManyLinks
			}
			if crossParent && !srcIsDir && dstIsDir && oldParent.Nlink >= MaxNlink {
				return ErrTooManyLinks
			}

			if setErr := batch.Set(dk.LayerEntryKey(work, req.OldParentIno, []byte(req.OldName)), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: dstIno.Ino, Mode: dstIno.Mode})), nil); setErr != nil {
				return setErr
			}
			if setErr := batch.Set(dk.LayerEntryKey(work, req.NewParentIno, []byte(req.NewName)), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: srcIno.Ino, Mode: srcIno.Mode})), nil); setErr != nil {
				return setErr
			}
			if recErr := recordEverLinkedParent(batch, dk, work, &srcIno, req.NewParentIno); recErr != nil {
				return recErr
			}
			if recErr := recordEverLinkedParent(batch, dk, work, &dstIno, req.OldParentIno); recErr != nil {
				return recErr
			}
			if crossParent {
				if srcIsDir || srcIno.Nlink == 1 {
					srcIno.ParentIno = req.NewParentIno
				}
				if dstIsDir || dstIno.Nlink == 1 {
					dstIno.ParentIno = req.OldParentIno
				}
				if srcIsDir != dstIsDir {
					if srcIsDir {
						if oldParent.Nlink > 2 {
							oldParent.Nlink--
						}
						newParent.Nlink++
					} else {
						oldParent.Nlink++
						if newParent.Nlink > 2 {
							newParent.Nlink--
						}
					}
				}
			}
			srcIno.Ctime = nowNano
			srcIno.WriteSeq++
			dstIno.Ctime = nowNano
			dstIno.WriteSeq++
			srcRaw, encErr := EncodeInode(srcIno)
			if encErr != nil {
				return encErr
			}
			dstRaw, encErr := EncodeInode(dstIno)
			if encErr != nil {
				return encErr
			}
			if setErr := batch.Set(dk.LayerInodeKey(work, srcIno.Ino), EncodeLiveEnvelope(srcRaw), nil); setErr != nil {
				return setErr
			}
			if setErr := batch.Set(dk.LayerInodeKey(work, dstIno.Ino), EncodeLiveEnvelope(dstRaw), nil); setErr != nil {
				return setErr
			}
			oldParent.Mtime = nowNano
			oldParent.Ctime = nowNano
			oldParentRaw, encErr := EncodeInode(oldParent)
			if encErr != nil {
				return encErr
			}
			if setErr := batch.Set(dk.LayerInodeKey(work, oldParent.Ino), EncodeLiveEnvelope(oldParentRaw), nil); setErr != nil {
				return setErr
			}
			if crossParent {
				newParent.Mtime = nowNano
				newParent.Ctime = nowNano
				newParentRaw, newEncErr := EncodeInode(newParent)
				if newEncErr != nil {
					return newEncErr
				}
				return batch.Set(dk.LayerInodeKey(work, newParent.Ino), EncodeLiveEnvelope(newParentRaw), nil)
			}
			return nil
		}

		var dstLayers []uint64
		if hasDst {
			if srcIsDir && !dstIsDir {
				return ErrNotDir
			}
			if !srcIsDir && dstIsDir {
				return ErrIsDir
			}
			if dstIsDir {
				var lErr error
				dstLayers, lErr = cursor.allLayers()
				if lErr != nil {
					return lErr
				}
				children, dirErr := overlayReadDir(batch, dk, dstLayers, dstIno.Ino)
				if dirErr != nil {
					return dirErr
				}
				if len(children) > 0 {
					return ErrNotEmpty
				}
			}
		}
		if srcIsDir && crossParent && !hasDst && newParent.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}

		if setErr := batch.Set(dk.LayerEntryKey(work, req.OldParentIno, []byte(req.OldName)), EncodeTombstone(), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.LayerEntryKey(work, req.NewParentIno, []byte(req.NewName)), EncodeLiveEnvelope(EncodeEntry(srcEnt)), nil); setErr != nil {
			return setErr
		}

		if hasDst {
			switch {
			case dstIsDir:
				if setErr := batch.Set(dk.LayerInodeKey(work, dstIno.Ino), EncodeTombstone(), nil); setErr != nil {
					return setErr
				}
				xnames, xErr := overlayListXattr(batch, dk, dstLayers, dstIno.Ino)
				if xErr != nil {
					return xErr
				}
				for _, xname := range xnames {
					if setErr := batch.Set(dk.LayerXattrKey(work, dstIno.Ino, []byte(xname)), EncodeTombstone(), nil); setErr != nil {
						return setErr
					}
				}
			case dstIno.Nlink <= 1 && req.TargetOpenInClient:
				dstIno.Nlink = 0
				dstIno.Ctime = nowNano
				dstIno.WriteSeq++
				dstRaw, encErr := EncodeInode(dstIno)
				if encErr != nil {
					return encErr
				}
				if setErr := batch.Set(dk.LayerInodeKey(work, dstIno.Ino), EncodeLiveEnvelope(dstRaw), nil); setErr != nil {
					return setErr
				}
				if setErr := batch.Set(dk.LayerOrphanKey(work, dstIno.Ino), []byte{OrphanUnlinked}, nil); setErr != nil {
					return setErr
				}
			default:
				freedWork, freedLogical, tbErr := tombstoneOrDecInode(batch, dk, work, cursor, dstIno, nowNano)
				if tbErr != nil {
					return tbErr
				}
				if freedWork > 0 || freedLogical > 0 {
					bm.WorkOwnedBytes = saturatingSub(bm.WorkOwnedBytes, freedWork)
					bm.LogicalBytes = saturatingSub(bm.LogicalBytes, freedLogical)
					if setErr := batch.Set(dk.MetaBranchKey(bm.Name), EncodeBranchMeta(bm), nil); setErr != nil {
						return setErr
					}
					quota.release(freedWork)
				}
			}
		}

		if recErr := recordEverLinkedParent(batch, dk, work, &srcIno, req.NewParentIno); recErr != nil {
			return recErr
		}

		srcIno.ParentIno = req.NewParentIno
		srcIno.Ctime = nowNano
		srcIno.WriteSeq++
		srcRaw, err := EncodeInode(srcIno)
		if err != nil {
			return err
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, srcIno.Ino), EncodeLiveEnvelope(srcRaw), nil); setErr != nil {
			return setErr
		}

		if !crossParent {
			if hasDst && srcIsDir && oldParent.Nlink > 2 {
				oldParent.Nlink--
			}
			oldParent.Mtime = nowNano
			oldParent.Ctime = nowNano
			pRaw, encErr := EncodeInode(oldParent)
			if encErr != nil {
				return encErr
			}
			return batch.Set(dk.LayerInodeKey(work, oldParent.Ino), EncodeLiveEnvelope(pRaw), nil)
		}

		if srcIsDir {
			if oldParent.Nlink > 2 {
				oldParent.Nlink--
			}
			if !hasDst {
				newParent.Nlink++
			}
		}
		oldParent.Mtime = nowNano
		oldParent.Ctime = nowNano
		newParent.Mtime = nowNano
		newParent.Ctime = nowNano

		oldParentRaw, err := EncodeInode(oldParent)
		if err != nil {
			return err
		}
		newParentRaw, err := EncodeInode(newParent)
		if err != nil {
			return err
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, oldParent.Ino), EncodeLiveEnvelope(oldParentRaw), nil); setErr != nil {
			return setErr
		}
		return batch.Set(dk.LayerInodeKey(work, newParent.Ino), EncodeLiveEnvelope(newParentRaw), nil)
	})
}

type setAttrTailUpdate struct {
	hasTailRef    bool
	tailTombstone bool
	tailBlkIdx    uint64
	tailRef       BlobRef
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
		return ErrFileTooLarge
	}
	if req.Flags != nil && *req.Flags > math.MaxInt32 {
		return ErrInvalid
	}
	return nil
}

func validateSetAttrInode(inode Inode, req SetAttrReq) error {
	fileType := inode.Mode & ModeTypeMask
	if req.Size != nil {
		if fileType == ModeDir {
			return ErrIsDir
		}
		if fileType != ModeRegular {
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
		inputType := *req.Mode & ModeTypeMask
		if fileType == ModeSymlink || (inputType != 0 && inputType != fileType) {
			return ErrInvalid
		}
	}
	return nil
}

func (d *DiskStore) SetAttr(ctx context.Context, branch string, ino uint64, req SetAttrReq) (Inode, error) {
	if d.closing.Load() {
		return Inode{}, ErrClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return Inode{}, err
		}
	}
	if ino == 0 || ino > math.MaxInt64 {
		return Inode{}, ErrInvalid
	}
	if err := validateSetAttrReq(req); err != nil {
		return Inode{}, err
	}

	var tailUpd setAttrTailUpdate
	if req.Size != nil {
		mu := d.inodeLock(branch, ino)
		mu.Lock()
		defer mu.Unlock()

		newSize := *req.Size
		rv := d.openReadView()
		branchMeta, bmErr := readBranchMeta(rv.snap, d.keys, branch)
		if bmErr != nil {
			_ = rv.Close()
			return Inode{}, bmErr
		}
		if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != branchMeta.WriterEpoch {
			_ = rv.Close()
			return Inode{}, ErrStaleEpoch
		}
		var snapCursor branchLayerCursor
		d.initBranchCursor(&snapCursor, rv.snap, branchMeta, rv.snapGen)
		curInode, inoErr := snapCursor.getInode(ino)
		if inoErr != nil {
			_ = rv.Close()
			return Inode{}, inoErr
		}
		if valErr := validateSetAttrInode(curInode, req); valErr != nil {
			_ = rv.Close()
			return Inode{}, valErr
		}
		blkSize := uint64(d.blockSize)
		var (
			trimmedTail []byte
			tailBlk     uint64
		)
		if newSize < curInode.Size && newSize%blkSize != 0 {
			tailBlk = newSize / blkSize
			tailLen := uint32(newSize % blkSize)
			existingRef, exists, refErr := snapCursor.getDataRef(ino, tailBlk)
			if refErr != nil {
				_ = rv.Close()
				return Inode{}, refErr
			}
			if exists && existingRef.Length > tailLen {
				oldData, readErr := rv.readBlock(existingRef)
				if readErr != nil {
					_ = rv.Close()
					return Inode{}, readErr
				}
				trimmedTail = make([]byte, tailLen)
				copy(trimmedTail, oldData[:tailLen])
			}
		}
		_ = rv.Close()

		if trimmedTail != nil {
			if isAllZero(trimmedTail) {
				tailUpd.hasTailRef = true
				tailUpd.tailTombstone = true
				tailUpd.tailBlkIdx = tailBlk
			} else {
				refs, appErr := d.packs.AppendBlocks(ctx, [][]byte{trimmedTail})
				if appErr != nil {
					return Inode{}, appErr
				}
				defer d.packs.ReleaseInFlight(refs)
				tailUpd.hasTailRef = true
				tailUpd.tailBlkIdx = tailBlk
				tailUpd.tailRef = refs[0]
			}
		}
	}

	var out Inode
	err := d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		inode, err := cursor.getInode(ino)
		if err != nil {
			return err
		}
		if valErr := validateSetAttrInode(inode, req); valErr != nil {
			return valErr
		}
		nowNano := d.now().UnixNano()
		work := bm.WorkLayerID
		fileType := inode.Mode & ModeTypeMask

		if req.Mode != nil {
			inode.Mode = fileType | (*req.Mode & ModePermMask)
		} else if fileType != ModeDir && (req.UID != nil || req.GID != nil || req.Size != nil) {
			inode.Mode &^= ModeSetUID | ModeSetGID
		}
		if req.UID != nil {
			inode.UID = *req.UID
		}
		if req.GID != nil {
			inode.GID = *req.GID
		}
		if req.Flags != nil {
			inode.Flags = (*req.Flags) | (inode.Flags & InodeFlagEverLinked)
		}
		if req.AtimeNow {
			inode.Atime = nowNano
		} else if req.Atime != nil {
			inode.Atime = *req.Atime
		}
		if req.MtimeNow || (req.Size != nil && req.Mtime == nil) {
			inode.Mtime = nowNano
		} else if req.Mtime != nil {
			inode.Mtime = *req.Mtime
		}

		var (
			dropBlks       []uint64
			workOwnedDelta int64
			logicalDelta   int64
		)
		if req.Size != nil {
			newSize := *req.Size
			blkSize := uint64(d.blockSize)
			if newSize < inode.Size {
				layers, lErr := cursor.allLayers()
				if lErr != nil {
					return lErr
				}
				firstDropBlk := (newSize + blkSize - 1) / blkSize
				liveBlks, dropWork, dropEff, listErr := overlayCollectLiveBlocksRange(batch, dk, layers, inode.Ino, firstDropBlk, math.MaxUint64)
				if listErr != nil {
					return listErr
				}
				dropBlks = liveBlks
				workOwnedDelta -= int64(dropWork)
				logicalDelta -= int64(dropEff)
				if tailUpd.hasTailRef {
					oldWorkLen, workErr := readLayerDataRefLen(batch, dk, work, inode.Ino, tailUpd.tailBlkIdx)
					if workErr != nil {
						return workErr
					}
					oldEffLen, effErr := cursor.getDataRefLen(inode.Ino, tailUpd.tailBlkIdx)
					if effErr != nil {
						return effErr
					}
					if tailUpd.tailTombstone {
						workOwnedDelta -= int64(oldWorkLen)
						logicalDelta -= int64(oldEffLen)
					} else {
						newLen := uint64(tailUpd.tailRef.Length)
						workOwnedDelta += int64(newLen) - int64(oldWorkLen)
						logicalDelta += int64(newLen) - int64(oldEffLen)
					}
				}
			}
			inode.Size = newSize
		}

		if workOwnedDelta > 0 {
			if resErr := quota.reserve(uint64(workOwnedDelta)); resErr != nil {
				return resErr
			}
		}

		for _, blkIdx := range dropBlks {
			if setErr := batch.Set(dk.LayerDataKey(work, inode.Ino, blkIdx), EncodeTombstone(), nil); setErr != nil {
				return setErr
			}
		}
		if req.Size != nil && tailUpd.hasTailRef {
			if tailUpd.tailTombstone {
				if setErr := batch.Set(dk.LayerDataKey(work, inode.Ino, tailUpd.tailBlkIdx), EncodeTombstone(), nil); setErr != nil {
					return setErr
				}
			} else {
				if setErr := batch.Set(dk.LayerDataKey(work, inode.Ino, tailUpd.tailBlkIdx), EncodeLiveEnvelope(EncodeBlobRef(tailUpd.tailRef)), nil); setErr != nil {
					return setErr
				}
			}
		}
		if req.Size != nil || req.UID != nil || req.GID != nil || req.Mode != nil {
			if capErr := clearSecurityCapability(batch, dk, work, cursor, inode.Ino); capErr != nil {
				return capErr
			}
		}
		inode.Ctime = nowNano
		inode.WriteSeq++

		encoded, err := EncodeInode(inode)
		if err != nil {
			return err
		}
		if setErr := batch.Set(dk.LayerInodeKey(work, inode.Ino), EncodeLiveEnvelope(encoded), nil); setErr != nil {
			return setErr
		}
		if workOwnedDelta != 0 || logicalDelta != 0 {
			applyBranchByteDeltas(&bm, workOwnedDelta, logicalDelta)
			if setErr := batch.Set(dk.MetaBranchKey(bm.Name), EncodeBranchMeta(bm), nil); setErr != nil {
				return setErr
			}
		}
		if workOwnedDelta < 0 {
			quota.release(uint64(-workOwnedDelta))
		}
		out = inode
		return nil
	})
	if err != nil {
		return Inode{}, err
	}
	return out, nil
}

func (d *DiskStore) SetXattr(ctx context.Context, branch string, ino uint64, name string, value []byte, flags uint32) error {
	if ino == 0 {
		return ErrInvalid
	}
	if err := validateXattrName(name); err != nil {
		return err
	}
	if len(value) > MaxXattrValueBytes {
		return ErrInvalid
	}
	if flags&^(XattrCreate|XattrReplace) != 0 || flags == (XattrCreate|XattrReplace) {
		return ErrInvalid
	}
	valCopy := append([]byte(nil), value...)
	return d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		inode, err := cursor.getInode(ino)
		if err != nil {
			return err
		}
		if strings.HasPrefix(name, "user.") && !isAllowedUserXattrFileType(inode.Mode) {
			return ErrPerm
		}
		_, curErr := cursor.getXattr(ino, []byte(name))
		exists := curErr == nil
		if curErr != nil && !errors.Is(curErr, ErrNoData) {
			return curErr
		}
		if flags&XattrCreate != 0 && exists {
			return ErrExist
		}
		if flags&XattrReplace != 0 && !exists {
			return ErrNoData
		}
		if !exists {
			layers, lErr := cursor.allLayers()
			if lErr != nil {
				return lErr
			}
			existingNames, listErr := overlayListXattr(batch, dk, layers, ino)
			if listErr != nil {
				return listErr
			}
			total := len(name) + 1
			for _, existingName := range existingNames {
				total += len(existingName) + 1
				if total > MaxXattrListBytes {
					return ErrNoSpace
				}
			}
			if total > MaxXattrListBytes {
				return ErrNoSpace
			}
		}

		work := bm.WorkLayerID
		if setErr := batch.Set(dk.LayerXattrKey(work, ino, []byte(name)), EncodeLiveEnvelope(valCopy), nil); setErr != nil {
			return setErr
		}
		inode.Ctime = d.now().UnixNano()
		inode.WriteSeq++
		encoded, err := EncodeInode(inode)
		if err != nil {
			return err
		}
		return batch.Set(dk.LayerInodeKey(work, inode.Ino), EncodeLiveEnvelope(encoded), nil)
	})
}

func (d *DiskStore) RemoveXattr(ctx context.Context, branch string, ino uint64, name string) error {
	if ino == 0 {
		return ErrInvalid
	}
	if err := validateXattrName(name); err != nil {
		return err
	}
	return d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		inode, err := cursor.getInode(ino)
		if err != nil {
			return err
		}
		if strings.HasPrefix(name, "user.") && !isAllowedUserXattrFileType(inode.Mode) {
			return ErrPerm
		}
		if _, curErr := cursor.getXattr(ino, []byte(name)); curErr != nil {
			return curErr
		}
		work := bm.WorkLayerID
		if setErr := batch.Set(dk.LayerXattrKey(work, ino, []byte(name)), EncodeTombstone(), nil); setErr != nil {
			return setErr
		}
		inode.Ctime = d.now().UnixNano()
		inode.WriteSeq++
		encoded, err := EncodeInode(inode)
		if err != nil {
			return err
		}
		return batch.Set(dk.LayerInodeKey(work, inode.Ino), EncodeLiveEnvelope(encoded), nil)
	})
}

type writeBlockUpdate struct {
	blockIdx uint64
	ref      BlobRef
}

func validateWritableRegularInode(inode Inode, offset, dataLen uint64) error {
	switch inode.Mode & ModeTypeMask {
	case ModeDir:
		return ErrIsDir
	case ModeRegular:
	default:
		return ErrInvalid
	}
	if inode.Flags&FlagImmutable != 0 {
		return ErrPerm
	}
	if dataLen > 0 && inode.Flags&FlagAppend != 0 && offset != inode.Size {
		return ErrPerm
	}
	return nil
}

func (d *DiskStore) Write(ctx context.Context, branch string, ino uint64, offset int64, data []byte) (int, error) {
	n, _, err := d.WriteWithAttr(ctx, branch, ino, offset, data)
	return n, err
}

func (d *DiskStore) WriteWithAttr(ctx context.Context, branch string, ino uint64, offset int64, data []byte) (int, Inode, error) {
	if d.closing.Load() {
		return 0, Inode{}, ErrClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return 0, Inode{}, err
		}
	}
	if ino == 0 || ino > math.MaxInt64 || offset < 0 {
		return 0, Inode{}, ErrInvalid
	}
	if uint64(offset) > MaxFileSize || uint64(len(data)) > MaxFileSize-uint64(offset) {
		return 0, Inode{}, ErrFileTooLarge
	}

	startOff := uint64(offset)
	if len(data) == 0 {
		var out Inode
		err := d.execBranchWrite(ctx, branch, func(_ *pebble.Batch, _ *opQuotaTracker, _ BranchMeta, cursor *branchLayerCursor) error {
			inode, err := cursor.getInode(ino)
			if err != nil {
				return err
			}
			if valErr := validateWritableRegularInode(inode, startOff, 0); valErr != nil {
				return valErr
			}
			out = inode
			return nil
		})
		if err != nil {
			return 0, Inode{}, err
		}
		return 0, out, nil
	}

	mu := d.inodeLock(branch, ino)
	mu.Lock()
	defer mu.Unlock()

	rv := d.openReadView()
	branchMeta, bmErr := readBranchMeta(rv.snap, d.keys, branch)
	if bmErr != nil {
		_ = rv.Close()
		return 0, Inode{}, bmErr
	}
	if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != branchMeta.WriterEpoch {
		_ = rv.Close()
		return 0, Inode{}, ErrStaleEpoch
	}
	var snapCursor branchLayerCursor
	d.initBranchCursor(&snapCursor, rv.snap, branchMeta, rv.snapGen)
	curInode, inoErr := snapCursor.getInode(ino)
	if inoErr != nil {
		_ = rv.Close()
		return 0, Inode{}, inoErr
	}
	if valErr := validateWritableRegularInode(curInode, startOff, uint64(len(data))); valErr != nil {
		_ = rv.Close()
		return 0, Inode{}, valErr
	}

	blkSize := uint64(d.blockSize)
	endOff := startOff + uint64(len(data))
	startBlk := startOff / blkSize
	endBlk := (endOff - 1) / blkSize

	numBlks := int(endBlk - startBlk + 1)
	payloads := make([][]byte, 0, numBlks)
	blkIndices := make([]uint64, 0, numBlks)

	for blkIdx := startBlk; blkIdx <= endBlk; blkIdx++ {
		blkStart := blkIdx * blkSize
		inStart := uint64(0)
		if startOff > blkStart {
			inStart = startOff - blkStart
		}
		inEnd := blkSize
		if endOff < blkStart+blkSize {
			inEnd = endOff - blkStart
		}
		srcStart := blkStart + inStart - startOff
		srcEnd := blkStart + inEnd - startOff

		var existingData []byte
		if inStart > 0 || inEnd < blkSize {
			oldRef, exists, refErr := snapCursor.getDataRef(ino, blkIdx)
			if refErr != nil {
				_ = rv.Close()
				return 0, Inode{}, refErr
			}
			if exists {
				existingData, refErr = rv.readBlock(oldRef)
				if refErr != nil {
					_ = rv.Close()
					return 0, Inode{}, refErr
				}
			}
		}

		newLen := max(inEnd, uint64(len(existingData)))
		buf := make([]byte, newLen)
		copy(buf, existingData)
		copy(buf[inStart:inEnd], data[srcStart:srcEnd])

		payloads = append(payloads, buf)
		blkIndices = append(blkIndices, blkIdx)
	}
	_ = rv.Close()

	refs, err := d.packs.AppendBlocks(ctx, payloads)
	if err != nil {
		return 0, Inode{}, err
	}
	defer d.packs.ReleaseInFlight(refs)

	updates := make([]writeBlockUpdate, len(refs))
	for i := range refs {
		updates[i] = writeBlockUpdate{
			blockIdx: blkIndices[i],
			ref:      refs[i],
		}
	}

	var outInode Inode
	err = d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		inode, getErr := cursor.getInode(ino)
		if getErr != nil {
			return getErr
		}
		if valErr := validateWritableRegularInode(inode, startOff, uint64(len(data))); valErr != nil {
			return valErr
		}

		work := bm.WorkLayerID
		var (
			workOwnedDelta int64
			logicalDelta   int64
		)
		for _, upd := range updates {
			oldWorkLen, workErr := readLayerDataRefLen(batch, dk, work, ino, upd.blockIdx)
			if workErr != nil {
				return workErr
			}
			oldEffLen, effErr := cursor.getDataRefLen(ino, upd.blockIdx)
			if effErr != nil {
				return effErr
			}
			newLen := uint64(upd.ref.Length)
			workOwnedDelta += int64(newLen) - int64(oldWorkLen)
			logicalDelta += int64(newLen) - int64(oldEffLen)
		}

		nowNano := d.now().UnixNano()
		if endOff > inode.Size {
			inode.Size = endOff
		}
		inode.Mode &^= ModeSetUID | ModeSetGID
		inode.Mtime = nowNano
		inode.Ctime = nowNano
		inode.WriteSeq++

		encoded, encErr := EncodeInode(inode)
		if encErr != nil {
			return encErr
		}

		if workOwnedDelta > 0 {
			if resErr := quota.reserve(uint64(workOwnedDelta)); resErr != nil {
				return resErr
			}
		}

		for _, upd := range updates {
			if setErr := batch.Set(dk.LayerDataKey(work, ino, upd.blockIdx), EncodeLiveEnvelope(EncodeBlobRef(upd.ref)), nil); setErr != nil {
				return setErr
			}
		}

		if capErr := clearSecurityCapability(batch, dk, work, cursor, inode.Ino); capErr != nil {
			return capErr
		}

		if setErr := batch.Set(dk.LayerInodeKey(work, inode.Ino), EncodeLiveEnvelope(encoded), nil); setErr != nil {
			return setErr
		}
		if workOwnedDelta != 0 || logicalDelta != 0 {
			applyBranchByteDeltas(&bm, workOwnedDelta, logicalDelta)
			if setErr := batch.Set(dk.MetaBranchKey(bm.Name), EncodeBranchMeta(bm), nil); setErr != nil {
				return setErr
			}
		}
		if workOwnedDelta < 0 {
			quota.release(uint64(-workOwnedDelta))
		}
		outInode = inode
		return nil
	})
	if err != nil {
		return 0, Inode{}, err
	}
	return len(data), outInode, nil
}
