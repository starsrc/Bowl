package engine

import (
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

type orphanRecord struct {
	ino  uint64
	kind byte
}

type rawKV struct {
	key []byte
	val []byte
}

func isValidOrphanKind(kind byte) bool {
	return kind == OrphanUnlinked || kind == OrphanTmpfileExcl || kind == OrphanTmpfileLinkable
}

func readLayerOrphanKind(r pebbleReader, dk diskKeys, layerID, ino uint64) (kind byte, ok bool, err error) {
	val, closer, getErr := r.Get(dk.LayerOrphanKey(layerID, ino))
	if errors.Is(getErr, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if getErr != nil {
		return 0, false, getErr
	}
	var decErr error
	if len(val) != 1 || !isValidOrphanKind(val[0]) {
		decErr = ErrInvalidValue
	} else {
		kind = val[0]
	}
	closeErr := closer.Close()
	if decErr != nil || closeErr != nil {
		return 0, false, errors.Join(decErr, closeErr)
	}
	return kind, true, nil
}

func collectLayerOrphans(r pebbleReader, dk diskKeys, layerID uint64) ([]orphanRecord, error) {
	prefix := dk.LayerOrphanPrefix(layerID)
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	var out []orphanRecord
	for iter.First(); iter.Valid(); iter.Next() {
		_, ino, keyErr := dk.DecodeLayerOrphanKey(iter.Key())
		if keyErr != nil {
			_ = iter.Close()
			return nil, keyErr
		}
		val := iter.Value()
		if len(val) != 1 || !isValidOrphanKind(val[0]) {
			_ = iter.Close()
			return nil, ErrInvalidValue
		}
		out = append(out, orphanRecord{ino: ino, kind: val[0]})
	}
	if closeErr := iter.Close(); closeErr != nil {
		return nil, closeErr
	}
	return out, nil
}

func collectRawKVByPrefix(r pebbleReader, prefix []byte) ([]rawKV, error) {
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	var items []rawKV
	for iter.First(); iter.Valid(); iter.Next() {
		items = append(items, rawKV{
			key: append([]byte(nil), iter.Key()...),
			val: append([]byte(nil), iter.Value()...),
		})
	}
	if closeErr := iter.Close(); closeErr != nil {
		return nil, closeErr
	}
	return items, nil
}

func deletePrefixRange(batch *pebble.Batch, prefix []byte) error {
	return batch.DeleteRange(prefix, PrefixUpperBound(prefix), nil)
}

func purgeOrphanFromLayer(batch *pebble.Batch, dk diskKeys, workLayerID uint64, ancestorLayers []uint64, ino uint64) error {
	if delErr := batch.Delete(dk.LayerOrphanKey(workLayerID, ino), nil); delErr != nil {
		return delErr
	}
	if delErr := deletePrefixRange(batch, dk.LayerXattrPrefix(workLayerID, ino)); delErr != nil {
		return delErr
	}
	if delErr := deletePrefixRange(batch, dk.LayerDataPrefix(workLayerID, ino)); delErr != nil {
		return delErr
	}
	if delErr := deletePrefixRange(batch, dk.LayerParentPrefix(workLayerID, ino)); delErr != nil {
		return delErr
	}

	if len(ancestorLayers) > 0 {
		_, ancErr := overlayGetInode(batch, dk, ancestorLayers, ino)
		if ancErr == nil {
			return batch.Set(dk.LayerInodeKey(workLayerID, ino), EncodeTombstone(), nil)
		}
		if !errors.Is(ancErr, ErrNotFound) {
			return ancErr
		}
	}
	return batch.Delete(dk.LayerInodeKey(workLayerID, ino), nil)
}

func (d *DiskStore) migrateOrphansAcrossLayerSwap(batch *pebble.Batch, oldWorkLayer, newWorkLayer, headCommitID uint64) (migratedOwned, orphanEffective uint64, err error) {
	dk := d.keys
	orphans, err := collectLayerOrphans(batch, dk, oldWorkLayer)
	if err != nil || len(orphans) == 0 {
		return 0, 0, err
	}

	var ancestorLayers []uint64
	if headCommitID != 0 {
		_, ancestorLayers, err = d.resolveCommitLayers(batch, headCommitID)
		if err != nil {
			return 0, 0, err
		}
	}
	allLayers := make([]uint64, 0, 1+len(ancestorLayers))
	allLayers = append(allLayers, oldWorkLayer)
	allLayers = append(allLayers, ancestorLayers...)

	for _, rec := range orphans {
		inoRaw, closer, getErr := batch.Get(dk.LayerInodeKey(oldWorkLayer, rec.ino))
		if errors.Is(getErr, pebble.ErrNotFound) {
			if delErr := batch.Delete(dk.LayerOrphanKey(oldWorkLayer, rec.ino), nil); delErr != nil {
				return 0, 0, delErr
			}
			continue
		}
		if getErr != nil {
			return 0, 0, getErr
		}
		inoRawCopy := append([]byte(nil), inoRaw...)
		if closeErr := closer.Close(); closeErr != nil {
			return 0, 0, closeErr
		}

		tag, payload, decErr := DecodeEnvelope(inoRawCopy)
		if decErr != nil {
			return 0, 0, decErr
		}
		if tag != TagLive {
			if delErr := batch.Delete(dk.LayerOrphanKey(oldWorkLayer, rec.ino), nil); delErr != nil {
				return 0, 0, delErr
			}
			continue
		}
		inode, inoDecErr := DecodeInode(payload)
		if inoDecErr != nil {
			return 0, 0, inoDecErr
		}
		if inode.Nlink > 0 {
			if delErr := batch.Delete(dk.LayerOrphanKey(oldWorkLayer, rec.ino), nil); delErr != nil {
				return 0, 0, delErr
			}
			continue
		}

		_, ownBytes, effBytes, blkErr := overlayCollectLiveBlocksRange(batch, dk, allLayers, rec.ino, 0, math.MaxUint64)
		if blkErr != nil {
			return 0, 0, blkErr
		}

		xattrs, err := collectRawKVByPrefix(batch, dk.LayerXattrPrefix(oldWorkLayer, rec.ino))
		if err != nil {
			return 0, 0, err
		}
		dataEntries, err := collectRawKVByPrefix(batch, dk.LayerDataPrefix(oldWorkLayer, rec.ino))
		if err != nil {
			return 0, 0, err
		}

		if setErr := batch.Set(dk.LayerOrphanKey(newWorkLayer, rec.ino), []byte{rec.kind}, nil); setErr != nil {
			return 0, 0, setErr
		}
		if setErr := batch.Set(dk.LayerInodeKey(newWorkLayer, rec.ino), inoRawCopy, nil); setErr != nil {
			return 0, 0, setErr
		}
		for _, kv := range xattrs {
			_, _, xname, keyErr := dk.DecodeLayerXattrKey(kv.key)
			if keyErr != nil {
				return 0, 0, keyErr
			}
			if setErr := batch.Set(dk.LayerXattrKey(newWorkLayer, rec.ino, xname), kv.val, nil); setErr != nil {
				return 0, 0, setErr
			}
		}
		for _, kv := range dataEntries {
			_, _, blkIdx, keyErr := dk.DecodeLayerDataKey(kv.key)
			if keyErr != nil {
				return 0, 0, keyErr
			}
			if setErr := batch.Set(dk.LayerDataKey(newWorkLayer, rec.ino, blkIdx), kv.val, nil); setErr != nil {
				return 0, 0, setErr
			}
		}

		if purgeErr := purgeOrphanFromLayer(batch, dk, oldWorkLayer, ancestorLayers, rec.ino); purgeErr != nil {
			return 0, 0, purgeErr
		}
		migratedOwned += ownBytes
		orphanEffective += effBytes
	}
	return migratedOwned, orphanEffective, nil
}

func (d *DiskStore) sweepOrphansInWorkLayer(batch *pebble.Batch, workLayerID, headCommitID uint64) (freedWork, freedLogical uint64, err error) {
	dk := d.keys
	orphans, err := collectLayerOrphans(batch, dk, workLayerID)
	if err != nil || len(orphans) == 0 {
		return 0, 0, err
	}

	var ancestorLayers []uint64
	if headCommitID != 0 {
		_, ancestorLayers, err = d.resolveCommitLayers(batch, headCommitID)
		if err != nil {
			return 0, 0, err
		}
	}
	allLayers := make([]uint64, 0, 1+len(ancestorLayers))
	allLayers = append(allLayers, workLayerID)
	allLayers = append(allLayers, ancestorLayers...)

	for _, rec := range orphans {
		inode, getErr := overlayGetInode(batch, dk, allLayers, rec.ino)
		if getErr == nil && inode.Nlink > 0 {
			if delErr := batch.Delete(dk.LayerOrphanKey(workLayerID, rec.ino), nil); delErr != nil {
				return 0, 0, delErr
			}
			continue
		}
		if getErr != nil && !errors.Is(getErr, ErrNotFound) {
			return 0, 0, getErr
		}
		var ownBytes, effBytes uint64
		if getErr == nil {
			_, ownBytes, effBytes, err = overlayCollectLiveBlocksRange(batch, dk, allLayers, rec.ino, 0, math.MaxUint64)
			if err != nil {
				return 0, 0, err
			}
		}
		if purgeErr := purgeOrphanFromLayer(batch, dk, workLayerID, ancestorLayers, rec.ino); purgeErr != nil {
			return 0, 0, purgeErr
		}
		freedWork += ownBytes
		freedLogical += effBytes
	}
	return freedWork, freedLogical, nil
}

func (d *DiskStore) CreateTmpfile(ctx context.Context, branch string, parentIno uint64, mode, uid, gid uint32, exclusive bool) (Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return Inode{}, ErrInvalid
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
			ParentIno:  0,
			Mode:       normMode,
			Nlink:      0,
			UID:        uid,
			GID:        childGID,
			Atime:      nowNano,
			Mtime:      nowNano,
			Ctime:      nowNano,
			Generation: 1,
			WriteSeq:   1,
		}
		childRaw, err := EncodeInode(child)
		if err != nil {
			return err
		}
		kind := OrphanTmpfileLinkable
		if exclusive {
			kind = OrphanTmpfileExcl
		}

		work := bm.WorkLayerID
		dk := d.keys
		if setErr := batch.Set(dk.LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
			return setErr
		}
		return batch.Set(dk.LayerOrphanKey(work, child.Ino), []byte{kind}, nil)
	})
	if err != nil {
		return Inode{}, err
	}
	return child, nil
}

func (d *DiskStore) PurgeOrphanInode(ctx context.Context, branch string, ino uint64) error {
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalid
	}
	return d.execBranchWrite(ctx, branch, func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, cursor *branchLayerCursor) error {
		work := bm.WorkLayerID
		dk := d.keys
		inode, err := cursor.getInode(ino)
		if errors.Is(err, ErrNotFound) {
			return batch.Delete(dk.LayerOrphanKey(work, ino), nil)
		}
		if err != nil {
			return err
		}
		if inode.Nlink > 0 {
			return nil
		}
		_, hasOrphan, err := readLayerOrphanKind(batch, dk, work, ino)
		if err != nil {
			return err
		}
		if !hasOrphan {
			return nil
		}
		layers, err := cursor.allLayers()
		if err != nil {
			return err
		}
		_, dropWorkBytes, dropEffBytes, err := overlayCollectLiveBlocksRange(batch, dk, layers, ino, 0, math.MaxUint64)
		if err != nil {
			return err
		}
		var ancestorLayers []uint64
		if len(layers) > 1 {
			ancestorLayers = layers[1:]
		}
		if purgeErr := purgeOrphanFromLayer(batch, dk, work, ancestorLayers, ino); purgeErr != nil {
			return purgeErr
		}
		if dropWorkBytes > 0 || dropEffBytes > 0 {
			bm.WorkOwnedBytes = saturatingSub(bm.WorkOwnedBytes, dropWorkBytes)
			bm.LogicalBytes = saturatingSub(bm.LogicalBytes, dropEffBytes)
			if setErr := batch.Set(dk.MetaBranchKey(bm.Name), EncodeBranchMeta(bm), nil); setErr != nil {
				return setErr
			}
			quota.release(dropWorkBytes)
		}
		return nil
	})
}
