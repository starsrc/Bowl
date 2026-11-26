package engine

import (
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

var securityCapabilityXattr = []byte("security.capability")

func clearSecurityCapability(batch *pebble.Batch, dk diskKeys, work uint64, layers []uint64, ino uint64) error {
	_, err := overlayGetXattr(batch, dk, layers, ino, securityCapabilityXattr)
	if errors.Is(err, ErrNoData) {
		return nil
	}
	if err != nil {
		return err
	}
	return batch.Set(dk.LayerXattrKey(work, ino, securityCapabilityXattr), EncodeTombstone(), nil)
}

type mknodReq struct {
	parentIno uint64
	name      string
	mode      uint32
	uid       uint32
	gid       uint32
	rdev      uint32
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

	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return Inode{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpMknod, &mknodReq{
		parentIno: parentIno,
		name:      name,
		mode:      modeType | (mode & ModePermMask),
		uid:       uid,
		gid:       gid,
		rdev:      rdev,
	})
	if err != nil {
		return Inode{}, err
	}
	ino, ok := resp.(Inode)
	if !ok {
		return Inode{}, ErrInvalidValue
	}
	return ino, nil
}

func (a *BranchActor) execMknod(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) (Inode, error) {
	req, ok := raw.(*mknodReq)
	if !ok {
		return Inode{}, ErrInvalid
	}
	dk := a.disk.keys
	parent, err := overlayGetInode(batch, dk, layers, req.parentIno)
	if err != nil {
		return Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Inode{}, ErrNotDir
	}
	if parent.Nlink == 0 {
		return Inode{}, ErrNotFound
	}
	if _, entryErr := overlayGetEntry(batch, dk, layers, req.parentIno, []byte(req.name)); entryErr == nil {
		return Inode{}, ErrExist
	} else if !errors.Is(entryErr, ErrNotFound) {
		return Inode{}, entryErr
	}

	inoNum, err := a.disk.allocIno()
	if err != nil {
		return Inode{}, err
	}
	gid := req.gid
	if parent.Mode&ModeSetGID != 0 {
		gid = parent.GID
	}
	var rdev uint32
	ft := req.mode & ModeTypeMask
	if ft == ModeChar || ft == ModeBlock {
		rdev = req.rdev
	}

	nowNano := a.disk.now().UnixNano()
	child := Inode{
		Ino:        inoNum,
		ParentIno:  req.parentIno,
		Mode:       req.mode,
		Nlink:      1,
		UID:        req.uid,
		GID:        gid,
		Rdev:       rdev,
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
		return Inode{}, err
	}
	parentRaw, err := EncodeInode(parent)
	if err != nil {
		return Inode{}, err
	}

	work := branch.WorkLayerID
	if setErr := batch.Set(dk.LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	if setErr := batch.Set(dk.LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	ent := Entry{Ino: child.Ino, Mode: child.Mode}
	if setErr := batch.Set(dk.LayerEntryKey(work, req.parentIno, []byte(req.name)), EncodeLiveEnvelope(EncodeEntry(ent)), nil); setErr != nil {
		return Inode{}, setErr
	}
	return child, nil
}

type fsyncReq struct {
	ino uint64
}

func (d *DiskStore) Fsync(ctx context.Context, branch string, ino uint64) error {
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalid
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpFsync, &fsyncReq{ino: ino})
	return err
}

func (d *DiskStore) FsyncDir(ctx context.Context, branch string, ino uint64) error {
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalid
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpFsyncDir, &fsyncReq{ino: ino})
	return err
}

func (a *BranchActor) execFsync(batch *pebble.Batch, layers []uint64, raw any) error {
	req, ok := raw.(*fsyncReq)
	if !ok {
		return ErrInvalid
	}
	inode, err := overlayGetInode(batch, a.disk.keys, layers, req.ino)
	if err != nil {
		return err
	}
	if inode.Mode&ModeTypeMask == ModeDir {
		return ErrIsDir
	}
	return nil
}

func (a *BranchActor) execFsyncDir(batch *pebble.Batch, layers []uint64, raw any) error {
	req, ok := raw.(*fsyncReq)
	if !ok {
		return ErrInvalid
	}
	inode, err := overlayGetInode(batch, a.disk.keys, layers, req.ino)
	if err != nil {
		return err
	}
	if inode.Mode&ModeTypeMask != ModeDir {
		return ErrNotDir
	}
	return nil
}

type fallocateBlockUpdate struct {
	blockIdx  uint64
	tombstone bool
	ref       BlobRef
}

type fallocateReq struct {
	ino            uint64
	offset         uint64
	length         uint64
	mode           uint32
	zeroRange      bool
	partialUpdates []fallocateBlockUpdate
}

func (d *DiskStore) Fallocate(ctx context.Context, branch string, ino, offset, length uint64, mode uint32) error {
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

	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	mu := &actor.inodeLocks[ino&0xff]
	mu.Lock()
	defer mu.Unlock()

	zeroRange := mode&FallocPunchHole != 0 || mode&FallocZeroRange != 0
	var (
		partialUpdates []fallocateBlockUpdate
		stagedRefs     []BlobRef
	)

	if zeroRange {
		dk := d.keys
		snap := d.db.NewSnapshot()
		branchMeta, layers, layerErr := resolveBranchLayers(snap, dk, branch)
		if layerErr != nil {
			_ = snap.Close()
			return layerErr
		}
		if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != branchMeta.WriterEpoch {
			_ = snap.Close()
			return ErrStaleEpoch
		}
		curInode, inoErr := overlayGetInode(snap, dk, layers, ino)
		if inoErr != nil {
			_ = snap.Close()
			return inoErr
		}
		switch curInode.Mode & ModeTypeMask {
		case ModeDir:
			_ = snap.Close()
			return ErrIsDir
		case ModeRegular:
		default:
			_ = snap.Close()
			return ErrNotSupported
		}
		if curInode.Flags&(FlagImmutable|FlagAppend) != 0 {
			_ = snap.Close()
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

			oldRef, exists, refErr := overlayGetDataRef(snap, dk, layers, ino, blkIdx)
			if refErr != nil {
				_ = snap.Close()
				return refErr
			}
			if !exists || inBlock >= int(oldRef.Length) {
				continue
			}
			oldData, readErr := d.packs.ReadBlock(oldRef)
			if readErr != nil {
				_ = snap.Close()
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
		_ = snap.Close()

		if len(toAppend) > 0 {
			refs, appErr := d.packs.AppendBlocks(ctx, toAppend)
			if appErr != nil {
				return appErr
			}
			stagedRefs = refs
			defer d.packs.ReleaseInFlight(stagedRefs)
			for i, ref := range refs {
				partialUpdates = append(partialUpdates, fallocateBlockUpdate{
					blockIdx: toAppendIndices[i],
					ref:      ref,
				})
			}
		}
	}

	_, err = actor.submit(ctx, branchOpFallocate, &fallocateReq{
		ino:            ino,
		offset:         offset,
		length:         length,
		mode:           mode,
		zeroRange:      zeroRange,
		partialUpdates: partialUpdates,
	})
	return err
}

func (a *BranchActor) execFallocate(batch *pebble.Batch, quota *tickQuotaTracker, branch BranchMeta, layers []uint64, raw any) error {
	req, ok := raw.(*fallocateReq)
	if !ok {
		return ErrInvalid
	}
	dk := a.disk.keys
	inode, err := overlayGetInode(batch, dk, layers, req.ino)
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

	work := branch.WorkLayerID
	blkSize := uint64(a.disk.blockSize)
	end := req.offset + req.length
	oldSize := inode.Size
	newSize := oldSize
	if req.mode&FallocKeepSize == 0 && end > newSize {
		newSize = end
	}

	var (
		fullDropBlks   []uint64
		workOwnedDelta int64
		logicalDelta   int64
	)
	if req.zeroRange {
		fullStart := (req.offset + blkSize - 1) / blkSize
		if end >= blkSize {
			fullEnd := (end / blkSize) - 1
			if fullStart <= fullEnd {
				liveBlks, dropWork, dropEff, listErr := overlayCollectLiveBlocksRange(batch, dk, layers, inode.Ino, fullStart, fullEnd)
				if listErr != nil {
					return listErr
				}
				fullDropBlks = liveBlks
				workOwnedDelta -= int64(dropWork)
				logicalDelta -= int64(dropEff)
			}
		}
		for _, upd := range req.partialUpdates {
			oldWorkLen, workErr := readLayerDataRefLen(batch, dk, work, inode.Ino, upd.blockIdx)
			if workErr != nil {
				return workErr
			}
			oldEffLen, effErr := overlayGetDataRefLen(batch, dk, layers, inode.Ino, upd.blockIdx)
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

	if req.zeroRange {
		for _, blkIdx := range fullDropBlks {
			if setErr := batch.Set(dk.LayerDataKey(work, inode.Ino, blkIdx), EncodeTombstone(), nil); setErr != nil {
				return setErr
			}
		}
		for _, upd := range req.partialUpdates {
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

	if capErr := clearSecurityCapability(batch, dk, work, layers, inode.Ino); capErr != nil {
		return capErr
	}

	nowNano := a.disk.now().UnixNano()
	inode.Size = newSize
	if req.zeroRange || newSize > oldSize {
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
		applyBranchByteDeltas(&branch, workOwnedDelta, logicalDelta)
		if setErr := batch.Set(dk.MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
			return setErr
		}
	}
	if workOwnedDelta < 0 {
		quota.release(uint64(-workOwnedDelta))
	}
	return nil
}
