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

func readLayerOrphanKind(r pebbleReader, layerID, ino uint64) (kind byte, ok bool, err error) {
	val, closer, getErr := r.Get(LayerOrphanKey(layerID, ino))
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

func collectLayerOrphans(r pebbleReader, layerID uint64) ([]orphanRecord, error) {
	prefix := LayerOrphanPrefix(layerID)
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	var out []orphanRecord
	for iter.First(); iter.Valid(); iter.Next() {
		_, ino, keyErr := DecodeLayerOrphanKey(iter.Key())
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

func purgeOrphanFromLayer(batch *pebble.Batch, workLayerID uint64, ancestorLayers []uint64, ino uint64) error {
	if delErr := batch.Delete(LayerOrphanKey(workLayerID, ino), nil); delErr != nil {
		return delErr
	}
	if delErr := deletePrefixRange(batch, LayerXattrPrefix(workLayerID, ino)); delErr != nil {
		return delErr
	}
	if delErr := deletePrefixRange(batch, LayerDataPrefix(workLayerID, ino)); delErr != nil {
		return delErr
	}
	if delErr := deletePrefixRange(batch, LayerParentPrefix(workLayerID, ino)); delErr != nil {
		return delErr
	}

	if len(ancestorLayers) > 0 {
		_, ancErr := overlayGetInode(batch, ancestorLayers, ino)
		if ancErr == nil {
			return batch.Set(LayerInodeKey(workLayerID, ino), EncodeTombstone(), nil)
		}
		if !errors.Is(ancErr, ErrNotFound) {
			return ancErr
		}
	}
	return batch.Delete(LayerInodeKey(workLayerID, ino), nil)
}

func migrateOrphansAcrossLayerSwap(batch *pebble.Batch, oldWorkLayer, newWorkLayer, headCommitID uint64) error {
	orphans, err := collectLayerOrphans(batch, oldWorkLayer)
	if err != nil || len(orphans) == 0 {
		return err
	}

	var ancestorLayers []uint64
	if headCommitID != 0 {
		_, ancestorLayers, err = resolveCommitLayers(batch, headCommitID)
		if err != nil {
			return err
		}
	}

	for _, rec := range orphans {
		inoRaw, closer, getErr := batch.Get(LayerInodeKey(oldWorkLayer, rec.ino))
		if errors.Is(getErr, pebble.ErrNotFound) {
			if delErr := batch.Delete(LayerOrphanKey(oldWorkLayer, rec.ino), nil); delErr != nil {
				return delErr
			}
			continue
		}
		if getErr != nil {
			return getErr
		}
		inoRawCopy := append([]byte(nil), inoRaw...)
		if closeErr := closer.Close(); closeErr != nil {
			return closeErr
		}

		tag, payload, decErr := DecodeEnvelope(inoRawCopy)
		if decErr != nil {
			return decErr
		}
		if tag != TagLive {
			if delErr := batch.Delete(LayerOrphanKey(oldWorkLayer, rec.ino), nil); delErr != nil {
				return delErr
			}
			continue
		}
		inode, inoDecErr := DecodeInode(payload)
		if inoDecErr != nil {
			return inoDecErr
		}
		if inode.Nlink > 0 {
			if delErr := batch.Delete(LayerOrphanKey(oldWorkLayer, rec.ino), nil); delErr != nil {
				return delErr
			}
			continue
		}

		xattrs, err := collectRawKVByPrefix(batch, LayerXattrPrefix(oldWorkLayer, rec.ino))
		if err != nil {
			return err
		}
		dataEntries, err := collectRawKVByPrefix(batch, LayerDataPrefix(oldWorkLayer, rec.ino))
		if err != nil {
			return err
		}

		if setErr := batch.Set(LayerOrphanKey(newWorkLayer, rec.ino), []byte{rec.kind}, nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(LayerInodeKey(newWorkLayer, rec.ino), inoRawCopy, nil); setErr != nil {
			return setErr
		}
		for _, kv := range xattrs {
			_, _, xname, keyErr := DecodeLayerXattrKey(kv.key)
			if keyErr != nil {
				return keyErr
			}
			if setErr := batch.Set(LayerXattrKey(newWorkLayer, rec.ino, xname), kv.val, nil); setErr != nil {
				return setErr
			}
		}
		for _, kv := range dataEntries {
			_, _, blkIdx, keyErr := DecodeLayerDataKey(kv.key)
			if keyErr != nil {
				return keyErr
			}
			if setErr := batch.Set(LayerDataKey(newWorkLayer, rec.ino, blkIdx), kv.val, nil); setErr != nil {
				return setErr
			}
		}

		if purgeErr := purgeOrphanFromLayer(batch, oldWorkLayer, ancestorLayers, rec.ino); purgeErr != nil {
			return purgeErr
		}
	}
	return nil
}

func sweepOrphansInWorkLayer(batch *pebble.Batch, workLayerID, headCommitID uint64) error {
	orphans, err := collectLayerOrphans(batch, workLayerID)
	if err != nil || len(orphans) == 0 {
		return err
	}

	var ancestorLayers []uint64
	if headCommitID != 0 {
		_, ancestorLayers, err = resolveCommitLayers(batch, headCommitID)
		if err != nil {
			return err
		}
	}
	allLayers := make([]uint64, 0, 1+len(ancestorLayers))
	allLayers = append(allLayers, workLayerID)
	allLayers = append(allLayers, ancestorLayers...)

	for _, rec := range orphans {
		inode, getErr := overlayGetInode(batch, allLayers, rec.ino)
		if getErr == nil && inode.Nlink > 0 {
			if delErr := batch.Delete(LayerOrphanKey(workLayerID, rec.ino), nil); delErr != nil {
				return delErr
			}
			continue
		}
		if getErr != nil && !errors.Is(getErr, ErrNotFound) {
			return getErr
		}
		if purgeErr := purgeOrphanFromLayer(batch, workLayerID, ancestorLayers, rec.ino); purgeErr != nil {
			return purgeErr
		}
	}
	return nil
}

type createTmpfileReq struct {
	parentIno uint64
	mode      uint32
	uid       uint32
	gid       uint32
	exclusive bool
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
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return Inode{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpCreateTmpfile, &createTmpfileReq{
		parentIno: parentIno,
		mode:      ModeRegular | (mode & ModePermMask),
		uid:       uid,
		gid:       gid,
		exclusive: exclusive,
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

func (a *BranchActor) execCreateTmpfile(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) (Inode, error) {
	req, ok := raw.(*createTmpfileReq)
	if !ok {
		return Inode{}, ErrInvalid
	}
	parent, err := overlayGetInode(batch, layers, req.parentIno)
	if err != nil {
		return Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Inode{}, ErrNotDir
	}
	if parent.Nlink == 0 {
		return Inode{}, ErrNotFound
	}

	inoNum, err := a.disk.allocIno()
	if err != nil {
		return Inode{}, err
	}
	gid := req.gid
	if parent.Mode&ModeSetGID != 0 {
		gid = parent.GID
	}
	nowNano := a.disk.now().UnixNano()
	child := Inode{
		Ino:        inoNum,
		ParentIno:  0,
		Mode:       req.mode,
		Nlink:      0,
		UID:        req.uid,
		GID:        gid,
		Atime:      nowNano,
		Mtime:      nowNano,
		Ctime:      nowNano,
		Generation: 1,
		WriteSeq:   1,
	}
	childRaw, err := EncodeInode(child)
	if err != nil {
		return Inode{}, err
	}
	kind := OrphanTmpfileLinkable
	if req.exclusive {
		kind = OrphanTmpfileExcl
	}

	work := branch.WorkLayerID
	if setErr := batch.Set(LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	if setErr := batch.Set(LayerOrphanKey(work, child.Ino), []byte{kind}, nil); setErr != nil {
		return Inode{}, setErr
	}
	return child, nil
}

type purgeOrphanReq struct {
	ino uint64
}

func (d *DiskStore) PurgeOrphanInode(ctx context.Context, branch string, ino uint64) error {
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalid
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpPurgeOrphan, &purgeOrphanReq{ino: ino})
	return err
}

func (a *BranchActor) execPurgeOrphan(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) error {
	req, ok := raw.(*purgeOrphanReq)
	if !ok {
		return ErrInvalid
	}
	work := branch.WorkLayerID
	inode, err := overlayGetInode(batch, layers, req.ino)
	if errors.Is(err, ErrNotFound) {
		return batch.Delete(LayerOrphanKey(work, req.ino), nil)
	}
	if err != nil {
		return err
	}
	if inode.Nlink > 0 {
		return nil
	}
	_, hasOrphan, err := readLayerOrphanKind(batch, work, req.ino)
	if err != nil {
		return err
	}
	if !hasOrphan {
		return nil
	}
	var ancestorLayers []uint64
	if len(layers) > 1 {
		ancestorLayers = layers[1:]
	}
	return purgeOrphanFromLayer(batch, work, ancestorLayers, req.ino)
}
