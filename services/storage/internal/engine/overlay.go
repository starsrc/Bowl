package engine

import (
	"context"
	"errors"
	"io"
	"sort"

	"github.com/cockroachdb/pebble/v2"
)

type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

func readBranchMeta(r pebbleReader, branchName string) (BranchMeta, error) {
	if branchName == "" {
		return BranchMeta{}, ErrInvalid
	}
	val, closer, err := r.Get(MetaBranchKey(branchName))
	if errors.Is(err, pebble.ErrNotFound) {
		return BranchMeta{}, ErrNotFound
	}
	if err != nil {
		return BranchMeta{}, err
	}
	meta, decErr := DecodeBranchMeta(val)
	closeErr := closer.Close()
	if decErr != nil || closeErr != nil {
		return BranchMeta{}, errors.Join(decErr, closeErr)
	}
	return meta, nil
}

func readCommitMeta(r pebbleReader, commitID uint64) (CommitMeta, error) {
	if commitID == 0 {
		return CommitMeta{}, ErrInvalid
	}
	val, closer, err := r.Get(MetaCommitKey(commitID))
	if errors.Is(err, pebble.ErrNotFound) {
		return CommitMeta{}, ErrNotFound
	}
	if err != nil {
		return CommitMeta{}, err
	}
	meta, decErr := DecodeCommitMeta(val)
	closeErr := closer.Close()
	if decErr != nil || closeErr != nil {
		return CommitMeta{}, errors.Join(decErr, closeErr)
	}
	return meta, nil
}

func resolveBranchLayers(r pebbleReader, branchName string) (BranchMeta, []uint64, error) {
	branch, err := readBranchMeta(r, branchName)
	if err != nil {
		return BranchMeta{}, nil, err
	}
	layers := make([]uint64, 0, 8)
	if branch.WorkLayerID != 0 {
		layers = append(layers, branch.WorkLayerID)
	}
	commitID := branch.HeadCommitID
	for commitID != 0 {
		cmt, cmtErr := readCommitMeta(r, commitID)
		if cmtErr != nil {
			return BranchMeta{}, nil, cmtErr
		}
		if cmt.LayerID != 0 {
			layers = append(layers, cmt.LayerID)
		}
		commitID = cmt.ParentID
	}
	return branch, layers, nil
}

func resolveCommitLayers(r pebbleReader, commitID uint64) (CommitMeta, []uint64, error) {
	head, err := readCommitMeta(r, commitID)
	if err != nil {
		return CommitMeta{}, nil, err
	}
	layers := make([]uint64, 0, 8)
	cur := head
	for {
		if cur.LayerID != 0 {
			layers = append(layers, cur.LayerID)
		}
		if cur.ParentID == 0 {
			break
		}
		next, nextErr := readCommitMeta(r, cur.ParentID)
		if nextErr != nil {
			return CommitMeta{}, nil, nextErr
		}
		cur = next
	}
	return head, layers, nil
}

func overlayGetEnvelope(r pebbleReader, layers []uint64, keyFn func(layerID uint64) []byte) (tag byte, payload []byte, found bool, err error) {
	for _, layerID := range layers {
		val, closer, getErr := r.Get(keyFn(layerID))
		if errors.Is(getErr, pebble.ErrNotFound) {
			continue
		}
		if getErr != nil {
			return 0, nil, false, getErr
		}
		decTag, decPayload, decErr := DecodeEnvelope(val)
		var payloadCopy []byte
		if decErr == nil && len(decPayload) > 0 {
			payloadCopy = append([]byte(nil), decPayload...)
		}
		closeErr := closer.Close()
		if decErr != nil || closeErr != nil {
			return 0, nil, false, errors.Join(decErr, closeErr)
		}
		return decTag, payloadCopy, true, nil
	}
	return 0, nil, false, nil
}

func overlayGetInode(r pebbleReader, layers []uint64, ino uint64) (Inode, error) {
	if ino == 0 {
		return Inode{}, ErrInvalid
	}
	tag, payload, found, err := overlayGetEnvelope(r, layers, func(layerID uint64) []byte {
		return LayerInodeKey(layerID, ino)
	})
	if err != nil {
		return Inode{}, err
	}
	if !found || tag == TagTombstone {
		return Inode{}, ErrNotFound
	}
	return DecodeInode(payload)
}

func overlayGetEntry(r pebbleReader, layers []uint64, parent uint64, name []byte) (Entry, error) {
	tag, payload, found, err := overlayGetEnvelope(r, layers, func(layerID uint64) []byte {
		return LayerEntryKey(layerID, parent, name)
	})
	if err != nil {
		return Entry{}, err
	}
	if !found || tag == TagTombstone {
		return Entry{}, ErrNotFound
	}
	return DecodeEntry(payload)
}

func overlayGetXattr(r pebbleReader, layers []uint64, ino uint64, name []byte) ([]byte, error) {
	tag, payload, found, err := overlayGetEnvelope(r, layers, func(layerID uint64) []byte {
		return LayerXattrKey(layerID, ino, name)
	})
	if err != nil {
		return nil, err
	}
	if !found || tag == TagTombstone {
		return nil, ErrNoData
	}
	if payload == nil {
		return []byte{}, nil
	}
	return payload, nil
}

func overlayGetDataRef(r pebbleReader, layers []uint64, ino, blockIdx uint64) (BlobRef, bool, error) {
	tag, payload, found, err := overlayGetEnvelope(r, layers, func(layerID uint64) []byte {
		return LayerDataKey(layerID, ino, blockIdx)
	})
	if err != nil {
		return BlobRef{}, false, err
	}
	if !found || tag == TagTombstone {
		return BlobRef{}, false, nil
	}
	ref, err := DecodeBlobRef(payload)
	if err != nil {
		return BlobRef{}, false, err
	}
	return ref, true, nil
}

func overlayReadDir(r pebbleReader, layers []uint64, parent uint64) ([]DirEntry, error) {
	seen := make(map[string]struct{})
	var entries []DirEntry
	for _, layerID := range layers {
		prefix := LayerEntryPrefix(layerID, parent)
		iter, err := r.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: PrefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			_, _, nameBytes, decKeyErr := DecodeLayerEntryKey(iter.Key())
			if decKeyErr != nil {
				_ = iter.Close()
				return nil, decKeyErr
			}
			name := string(nameBytes)
			if _, exists := seen[name]; exists {
				continue
			}
			seen[name] = struct{}{}
			tag, payload, decErr := DecodeEnvelope(iter.Value())
			if decErr != nil {
				_ = iter.Close()
				return nil, decErr
			}
			if tag == TagTombstone {
				continue
			}
			ent, entErr := DecodeEntry(payload)
			if entErr != nil {
				_ = iter.Close()
				return nil, entErr
			}
			entries = append(entries, DirEntry{
				Name: name,
				Ino:  ent.Ino,
				Mode: ent.Mode,
			})
		}
		if closeErr := iter.Close(); closeErr != nil {
			return nil, closeErr
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
	return entries, nil
}

func overlayListXattr(r pebbleReader, layers []uint64, ino uint64) ([]string, error) {
	seen := make(map[string]struct{})
	var names []string
	for _, layerID := range layers {
		prefix := LayerXattrPrefix(layerID, ino)
		iter, err := r.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: PrefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			_, _, nameBytes, decKeyErr := DecodeLayerXattrKey(iter.Key())
			if decKeyErr != nil {
				_ = iter.Close()
				return nil, decKeyErr
			}
			name := string(nameBytes)
			if _, exists := seen[name]; exists {
				continue
			}
			seen[name] = struct{}{}
			tag, _, decErr := DecodeEnvelope(iter.Value())
			if decErr != nil {
				_ = iter.Close()
				return nil, decErr
			}
			if tag == TagTombstone {
				continue
			}
			names = append(names, name)
		}
		if closeErr := iter.Close(); closeErr != nil {
			return nil, closeErr
		}
	}
	sort.Strings(names)
	return names, nil
}

func overlayListLiveBlocks(r pebbleReader, layers []uint64, ino, minBlockIdx uint64) ([]uint64, error) {
	seen := make(map[uint64]struct{})
	var live []uint64
	for _, layerID := range layers {
		prefix := LayerDataPrefix(layerID, ino)
		iter, err := r.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: PrefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			_, _, blkIdx, decKeyErr := DecodeLayerDataKey(iter.Key())
			if decKeyErr != nil {
				_ = iter.Close()
				return nil, decKeyErr
			}
			if blkIdx < minBlockIdx {
				continue
			}
			if _, exists := seen[blkIdx]; exists {
				continue
			}
			seen[blkIdx] = struct{}{}
			tag, _, decErr := DecodeEnvelope(iter.Value())
			if decErr != nil {
				_ = iter.Close()
				return nil, decErr
			}
			if tag == TagLive {
				live = append(live, blkIdx)
			}
		}
		if closeErr := iter.Close(); closeErr != nil {
			return nil, closeErr
		}
	}
	return live, nil
}

func layerHasAnyKey(r pebbleReader, layerID uint64) (bool, error) {
	start, end := LayerRangeBounds(layerID)
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return false, err
	}
	has := iter.First()
	if closeErr := iter.Close(); closeErr != nil {
		return false, closeErr
	}
	return has, nil
}

func (d *DiskStore) readInodeFromLayers(r pebbleReader, layers []uint64, ino uint64) (Inode, error) {
	return overlayGetInode(r, layers, ino)
}

func (d *DiskStore) readDataFromLayers(r pebbleReader, layers []uint64, ino uint64, offset int64, size int) ([]byte, error) {
	if offset < 0 || size < 0 {
		return nil, ErrInvalid
	}
	inode, err := overlayGetInode(r, layers, ino)
	if err != nil {
		return nil, err
	}
	if inode.Mode&ModeTypeMask != ModeRegular {
		return nil, ErrIsDir
	}
	if size == 0 || uint64(offset) >= inode.Size {
		return []byte{}, nil
	}
	end := min(uint64(offset)+uint64(size), inode.Size)
	outLen := int(end - uint64(offset))
	out := make([]byte, outLen)

	blkSize := uint64(d.blockSize)
	startBlk := uint64(offset) / blkSize
	endBlk := (end - 1) / blkSize

	for blkIdx := startBlk; blkIdx <= endBlk; blkIdx++ {
		ref, ok, refErr := overlayGetDataRef(r, layers, ino, blkIdx)
		if refErr != nil {
			return nil, refErr
		}
		if !ok {
			continue
		}
		blkData, readErr := d.packs.ReadBlock(ref)
		if readErr != nil {
			return nil, readErr
		}
		blkStart := blkIdx * blkSize
		copyStart := max(uint64(offset), blkStart)
		copyEnd := min(end, blkStart+uint64(len(blkData)))
		if copyEnd <= copyStart {
			continue
		}
		srcOff := copyStart - blkStart
		dstOff := copyStart - uint64(offset)
		copy(out[dstOff:dstOff+(copyEnd-copyStart)], blkData[srcOff:srcOff+(copyEnd-copyStart)])
	}
	return out, nil
}

func (d *DiskStore) GetAttr(_ context.Context, branch string, ino uint64) (Inode, error) {
	if d.closing.Load() {
		return Inode{}, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveBranchLayers(snap, branch)
	if err != nil {
		return Inode{}, err
	}
	return d.readInodeFromLayers(snap, layers, ino)
}

func (d *DiskStore) Lookup(_ context.Context, branch string, parentIno uint64, name string) (Entry, Inode, error) {
	if d.closing.Load() {
		return Entry{}, Inode{}, ErrClosed
	}
	if err := validateName(name); err != nil {
		return Entry{}, Inode{}, err
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveBranchLayers(snap, branch)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	parent, err := overlayGetInode(snap, layers, parentIno)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Entry{}, Inode{}, ErrNotDir
	}
	ent, err := overlayGetEntry(snap, layers, parentIno, []byte(name))
	if err != nil {
		return Entry{}, Inode{}, err
	}
	inode, err := overlayGetInode(snap, layers, ent.Ino)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	return ent, inode, nil
}

func (d *DiskStore) ReadDir(_ context.Context, branch string, parentIno uint64) ([]DirEntry, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveBranchLayers(snap, branch)
	if err != nil {
		return nil, err
	}
	parent, err := overlayGetInode(snap, layers, parentIno)
	if err != nil {
		return nil, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return nil, ErrNotDir
	}
	return overlayReadDir(snap, layers, parentIno)
}

func (d *DiskStore) GetXattr(_ context.Context, branch string, ino uint64, name string) ([]byte, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if err := validateXattrName(name); err != nil {
		return nil, err
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveBranchLayers(snap, branch)
	if err != nil {
		return nil, err
	}
	if _, err = overlayGetInode(snap, layers, ino); err != nil {
		return nil, err
	}
	return overlayGetXattr(snap, layers, ino, []byte(name))
}

func (d *DiskStore) ListXattr(_ context.Context, branch string, ino uint64) ([]string, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveBranchLayers(snap, branch)
	if err != nil {
		return nil, err
	}
	if _, err = overlayGetInode(snap, layers, ino); err != nil {
		return nil, err
	}
	return overlayListXattr(snap, layers, ino)
}

func (d *DiskStore) Read(_ context.Context, branch string, ino uint64, offset int64, size int) ([]byte, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveBranchLayers(snap, branch)
	if err != nil {
		return nil, err
	}
	return d.readDataFromLayers(snap, layers, ino, offset, size)
}

func (d *DiskStore) GetAttrAtCommit(_ context.Context, commitID, ino uint64) (Inode, error) {
	if d.closing.Load() {
		return Inode{}, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveCommitLayers(snap, commitID)
	if err != nil {
		return Inode{}, err
	}
	return overlayGetInode(snap, layers, ino)
}

func (d *DiskStore) LookupAtCommit(_ context.Context, commitID, parentIno uint64, name string) (Entry, Inode, error) {
	if d.closing.Load() {
		return Entry{}, Inode{}, ErrClosed
	}
	if err := validateName(name); err != nil {
		return Entry{}, Inode{}, err
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveCommitLayers(snap, commitID)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	ent, err := overlayGetEntry(snap, layers, parentIno, []byte(name))
	if err != nil {
		return Entry{}, Inode{}, err
	}
	inode, err := overlayGetInode(snap, layers, ent.Ino)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	return ent, inode, nil
}

func (d *DiskStore) ReadDirAtCommit(_ context.Context, commitID, parentIno uint64) ([]DirEntry, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveCommitLayers(snap, commitID)
	if err != nil {
		return nil, err
	}
	return overlayReadDir(snap, layers, parentIno)
}

func (d *DiskStore) ReadAtCommit(_ context.Context, commitID, ino uint64, offset int64, size int) ([]byte, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveCommitLayers(snap, commitID)
	if err != nil {
		return nil, err
	}
	return d.readDataFromLayers(snap, layers, ino, offset, size)
}
