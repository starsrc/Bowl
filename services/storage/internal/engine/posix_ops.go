package engine

import (
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

var securityCapabilityXattr = []byte("security.capability")

func clearSecurityCapability(batch *pebble.Batch, dk diskKeys, work uint64, cursor *branchLayerCursor, ino uint64) error {
	_, err := cursor.getXattr(ino, securityCapabilityXattr)
	if errors.Is(err, ErrNoData) {
		return nil
	}
	if err != nil {
		return err
	}
	return batch.Set(dk.LayerXattrKey(work, ino, securityCapabilityXattr), EncodeTombstone(), nil)
}

func (d *DiskStore) Mknod(ctx context.Context, branch string, parentIno uint64, name string, mode, uid, gid, rdev uint32) (Inode, error) {
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
	switch modeType {
	case ModeRegular, ModeFIFO, ModeChar, ModeBlock, ModeSocket:
	default:
		return Inode{}, ErrInvalid
	}
	normMode := modeType | (mode & ModePermMask)

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
		var childRdev uint32
		if modeType == ModeChar || modeType == ModeBlock {
			childRdev = rdev
		}

		nowNano := d.now().UnixNano()
		child = Inode{
			Ino:        inoNum,
			ParentIno:  parentIno,
			Mode:       normMode,
			Nlink:      1,
			UID:        uid,
			GID:        childGID,
			Rdev:       childRdev,
			Atime:      nowNano,
			Mtime:      nowNano,
			Ctime:      nowNano,
			Generation: 1,
			WriteSeq:   1,
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

func (d *DiskStore) validateFsyncTarget(ctx context.Context, branch string, ino uint64, wantDir bool) error {
	snap, snapGen := d.newReadSnapshot()
	defer func() { _ = snap.Close() }()

	bm, err := readBranchMeta(snap, d.keys, branch)
	if err != nil {
		return err
	}
	if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != bm.WriterEpoch {
		return ErrStaleEpoch
	}
	var cursor branchLayerCursor
	d.initBranchCursor(&cursor, snap, bm, snapGen)
	inode, err := cursor.getInode(ino)
	if err != nil {
		return err
	}
	isDir := inode.Mode&ModeTypeMask == ModeDir
	if wantDir && !isDir {
		return ErrNotDir
	}
	if !wantDir && isDir {
		return ErrIsDir
	}
	return nil
}

func (d *DiskStore) Fsync(ctx context.Context, branch string, ino uint64) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalid
	}
	if err := d.validateFsyncTarget(ctx, branch, ino, false); err != nil {
		return err
	}
	targetGen := d.node.walWriteGen.Load()
	return d.node.SyncWAL(ctx, targetGen, d.walCommitHook)
}

func (d *DiskStore) FsyncDir(ctx context.Context, branch string, ino uint64) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalid
	}
	if err := d.validateFsyncTarget(ctx, branch, ino, true); err != nil {
		return err
	}
	targetGen := d.node.walWriteGen.Load()
	return d.node.SyncWAL(ctx, targetGen, d.walCommitHook)
}

type fallocateBlockUpdate struct {
	blockIdx  uint64
	tombstone bool
	ref       BlobRef
}

func (d *DiskStore) Fallocate(ctx context.Context, branch string, ino, offset, length uint64, mode uint32) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalid
	}
	if length == 0 || offset > MaxFileSize || length > MaxFileSize-offset {
		return ErrInvalid
	}
	if mode == FallocPunchHole {
		return ErrInvalid
	}
	switch mode {
	case 0, FallocKeepSize, FallocPunchHole | FallocKeepSize, FallocZeroRange, FallocZeroRange | FallocKeepSize:
	default:
		return ErrNotSupported
	}

	mu := d.inodeLock(branch, ino)
	mu.Lock()
	defer mu.Unlock()

	zeroRange := mode&FallocPunchHole != 0 || mode&FallocZeroRange != 0
	var partialUpdates []fallocateBlockUpdate

	if zeroRange {
		rv := d.openReadView()
		branchMeta, bmErr := readBranchMeta(rv.snap, d.keys, branch)
		if bmErr != nil {
			_ = rv.Close()
			return bmErr
		}
		if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != branchMeta.WriterEpoch {
			_ = rv.Close()
			return ErrStaleEpoch
		}
		var snapCursor branchLayerCursor
		d.initBranchCursor(&snapCursor, rv.snap, branchMeta, rv.snapGen)
		curInode, inoErr := snapCursor.getInode(ino)
		if inoErr != nil {
			_ = rv.Close()
			return inoErr
		}
		switch curInode.Mode & ModeTypeMask {
		case ModeDir:
			_ = rv.Close()
			return ErrIsDir
		case ModeRegular:
		default:
			_ = rv.Close()
			return ErrNotSupported
		}
		if curInode.Flags&(FlagImmutable|FlagAppend) != 0 {
			_ = rv.Close()
			return ErrPerm
		}

		blkSize := uint64(d.blockSize)
		end := offset + length
		firstBlk := offset / blkSize
		lastBlk := (end - 1) / blkSize
		candidates := []uint64{firstBlk}
		if lastBlk != firstBlk {
			candidates = append(candidates, lastBlk)
		}

		var toAppend [][]byte
		var toAppendIndices []uint64

		for _, blkIdx := range candidates {
			blkStart := blkIdx * blkSize
			blkEnd := blkStart + blkSize
			rStart := max(offset, blkStart)
			rEnd := min(end, blkEnd)
			inBlock := int(rStart - blkStart)
			clearLen := int(rEnd - rStart)
			if inBlock == 0 && uint64(clearLen) == blkSize {
				continue
			}

			oldRef, exists, refErr := snapCursor.getDataRef(ino, blkIdx)
			if refErr != nil {
				_ = rv.Close()
				return refErr
			}
			if !exists || inBlock >= int(oldRef.Length) {
				continue
			}
			oldData, readErr := rv.readBlock(oldRef)
			if readErr != nil {
				_ = rv.Close()
				return readErr
			}
			clearEnd := min(inBlock+clearLen, len(oldData))
			if isAllZero(oldData[inBlock:clearEnd]) {
				continue
			}
			clear(oldData[inBlock:clearEnd])
			if isAllZero(oldData) {
				partialUpdates = append(partialUpdates, fallocateBlockUpdate{
					blockIdx:  blkIdx,
					tombstone: true,
				})
				continue
			}
			toAppend = append(toAppend, oldData)
			toAppendIndices = append(toAppendIndices, blkIdx)
		}
		_ = rv.Close()

		if len(toAppend) > 0 {
			refs, appErr := d.packs.AppendBlocks(ctx, toAppend)
			if appErr != nil {
				return appErr
			}
			defer d.packs.ReleaseInFlight(refs)
			for i, ref := range refs {
				partialUpdates = append(partialUpdates, fallocateBlockUpdate{
					blockIdx: toAppendIndices[i],
					ref:      ref,
				})
			}
		}
	}

	return d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		dk := d.keys
		inode, err := cursor.getInode(ino)
		if err != nil {
			return err
		}
		switch inode.Mode & ModeTypeMask {
		case ModeDir:
			return ErrIsDir
		case ModeRegular:
		default:
			return ErrNotSupported
		}
		if inode.Flags&(FlagImmutable|FlagAppend) != 0 {
			return ErrPerm
		}

		work := bm.WorkLayerID
		blkSize := uint64(d.blockSize)
		end := offset + length
		oldSize := inode.Size
		newSize := oldSize
		if mode&FallocKeepSize == 0 && end > newSize {
			newSize = end
		}

		var (
			fullDropBlks   []uint64
			workOwnedDelta int64
			logicalDelta   int64
		)
		if zeroRange {
			fullStart := (offset + blkSize - 1) / blkSize
			if end >= blkSize {
				fullEnd := (end / blkSize) - 1
				if fullStart <= fullEnd {
					layers, lErr := cursor.allLayers()
					if lErr != nil {
						return lErr
					}
					liveBlks, dropWork, dropEff, listErr := overlayCollectLiveBlocksRange(batch, dk, layers, inode.Ino, fullStart, fullEnd)
					if listErr != nil {
						return listErr
					}
					fullDropBlks = liveBlks
					workOwnedDelta -= int64(dropWork)
					logicalDelta -= int64(dropEff)
				}
			}
			for _, upd := range partialUpdates {
				oldWorkLen, workErr := readLayerDataRefLen(batch, dk, work, inode.Ino, upd.blockIdx)
				if workErr != nil {
					return workErr
				}
				oldEffLen, effErr := cursor.getDataRefLen(inode.Ino, upd.blockIdx)
				if effErr != nil {
					return effErr
				}
				if upd.tombstone {
					workOwnedDelta -= int64(oldWorkLen)
					logicalDelta -= int64(oldEffLen)
				} else {
					newLen := uint64(upd.ref.Length)
					workOwnedDelta += int64(newLen) - int64(oldWorkLen)
					logicalDelta += int64(newLen) - int64(oldEffLen)
				}
			}
		}

		if workOwnedDelta > 0 {
			if resErr := quota.reserve(uint64(workOwnedDelta)); resErr != nil {
				return resErr
			}
		}

		if zeroRange {
			for _, blkIdx := range fullDropBlks {
				if setErr := batch.Set(dk.LayerDataKey(work, inode.Ino, blkIdx), EncodeTombstone(), nil); setErr != nil {
					return setErr
				}
			}
			for _, upd := range partialUpdates {
				if upd.tombstone {
					if setErr := batch.Set(dk.LayerDataKey(work, inode.Ino, upd.blockIdx), EncodeTombstone(), nil); setErr != nil {
						return setErr
					}
				} else {
					if setErr := batch.Set(dk.LayerDataKey(work, inode.Ino, upd.blockIdx), EncodeLiveEnvelope(EncodeBlobRef(upd.ref)), nil); setErr != nil {
						return setErr
					}
				}
			}
			inode.Mode &^= ModeSetUID | ModeSetGID
		}

		if capErr := clearSecurityCapability(batch, dk, work, cursor, inode.Ino); capErr != nil {
			return capErr
		}

		nowNano := d.now().UnixNano()
		inode.Size = newSize
		if zeroRange || newSize > oldSize {
			inode.Mtime = nowNano
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
		return nil
	})
}
