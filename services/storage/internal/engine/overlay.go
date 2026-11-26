package engine

import (
	"context"
	"errors"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

func readBranchMeta(r pebbleReader, dk diskKeys, branchName string) (BranchMeta, error) {
	if branchName == "" {
		return BranchMeta{}, ErrInvalid
	}
	val, closer, err := r.Get(dk.MetaBranchKey(branchName))
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

func readCommitMeta(r pebbleReader, dk diskKeys, commitID uint64) (CommitMeta, error) {
	if commitID == 0 {
		return CommitMeta{}, ErrInvalid
	}
	val, closer, err := r.Get(dk.MetaCommitKey(commitID))
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

func resolveBranchLayers(r pebbleReader, dk diskKeys, branchName string) (BranchMeta, []uint64, error) {
	branch, err := readBranchMeta(r, dk, branchName)
	if err != nil {
		return BranchMeta{}, nil, err
	}
	layers := make([]uint64, 0, 8)
	if branch.WorkLayerID != 0 {
		layers = append(layers, branch.WorkLayerID)
	}
	commitID := branch.HeadCommitID
	for commitID != 0 {
		cmt, cmtErr := readCommitMeta(r, dk, commitID)
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

func resolveCommitLayers(r pebbleReader, dk diskKeys, commitID uint64) (CommitMeta, []uint64, error) {
	head, err := readCommitMeta(r, dk, commitID)
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
		next, nextErr := readCommitMeta(r, dk, cur.ParentID)
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

func overlayGetInode(r pebbleReader, dk diskKeys, layers []uint64, ino uint64) (Inode, error) {
	if ino == 0 {
		return Inode{}, ErrInvalid
	}
	tag, payload, found, err := overlayGetEnvelope(r, layers, func(layerID uint64) []byte {
		return dk.LayerInodeKey(layerID, ino)
	})
	if err != nil {
		return Inode{}, err
	}
	if !found || tag == TagTombstone {
		return Inode{}, ErrNotFound
	}
	return DecodeInode(payload)
}

func overlayGetEntry(r pebbleReader, dk diskKeys, layers []uint64, parent uint64, name []byte) (Entry, error) {
	tag, payload, found, err := overlayGetEnvelope(r, layers, func(layerID uint64) []byte {
		return dk.LayerEntryKey(layerID, parent, name)
	})
	if err != nil {
		return Entry{}, err
	}
	if !found || tag == TagTombstone {
		return Entry{}, ErrNotFound
	}
	return DecodeEntry(payload)
}

func overlayGetXattr(r pebbleReader, dk diskKeys, layers []uint64, ino uint64, name []byte) ([]byte, error) {
	tag, payload, found, err := overlayGetEnvelope(r, layers, func(layerID uint64) []byte {
		return dk.LayerXattrKey(layerID, ino, name)
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

func overlayGetDataRef(r pebbleReader, dk diskKeys, layers []uint64, ino, blockIdx uint64) (BlobRef, bool, error) {
	tag, payload, found, err := overlayGetEnvelope(r, layers, func(layerID uint64) []byte {
		return dk.LayerDataKey(layerID, ino, blockIdx)
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

func overlayReadDir(r pebbleReader, dk diskKeys, layers []uint64, parent uint64) ([]DirEntry, error) {
	seen := make(map[string]struct{})
	var entries []DirEntry
	for _, layerID := range layers {
		prefix := dk.LayerEntryPrefix(layerID, parent)
		iter, err := r.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: PrefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			_, _, nameBytes, decKeyErr := dk.DecodeLayerEntryKey(iter.Key())
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

func overlayListXattr(r pebbleReader, dk diskKeys, layers []uint64, ino uint64) ([]string, error) {
	seen := make(map[string]struct{})
	var names []string
	for _, layerID := range layers {
		prefix := dk.LayerXattrPrefix(layerID, ino)
		iter, err := r.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: PrefixUpperBound(prefix),
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			_, _, nameBytes, decKeyErr := dk.DecodeLayerXattrKey(iter.Key())
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

func readLayerDataRefLen(r pebbleReader, dk diskKeys, layerID, ino, blockIdx uint64) (uint64, error) {
	val, closer, err := r.Get(dk.LayerDataKey(layerID, ino, blockIdx))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	tag, payload, decErr := DecodeEnvelope(val)
	var ref BlobRef
	if decErr == nil && tag == TagLive {
		ref, decErr = DecodeBlobRef(payload)
	}
	closeErr := closer.Close()
	if decErr != nil || closeErr != nil {
		return 0, errors.Join(decErr, closeErr)
	}
	if tag != TagLive {
		return 0, nil
	}
	return uint64(ref.Length), nil
}

func overlayGetDataRefLen(r pebbleReader, dk diskKeys, layers []uint64, ino, blockIdx uint64) (uint64, error) {
	ref, exists, err := overlayGetDataRef(r, dk, layers, ino, blockIdx)
	if err != nil || !exists {
		return 0, err
	}
	return uint64(ref.Length), nil
}

func saturatingSub(a, b uint64) uint64 {
	if a <= b {
		return 0
	}
	return a - b
}

func applySignedDelta(base uint64, delta int64) uint64 {
	if delta >= 0 {
		return base + uint64(delta)
	}
	return saturatingSub(base, uint64(-delta))
}

func applyBranchByteDeltas(branch *BranchMeta, workOwnedDelta, logicalDelta int64) {
	branch.WorkOwnedBytes = applySignedDelta(branch.WorkOwnedBytes, workOwnedDelta)
	branch.LogicalBytes = applySignedDelta(branch.LogicalBytes, logicalDelta)
}

func overlayCollectLiveBlocksRange(r pebbleReader, dk diskKeys, layers []uint64, ino, minBlockIdx, maxBlockIdx uint64) (live []uint64, workBytes, effBytes uint64, err error) {
	seen := make(map[uint64]struct{})
	for i, layerID := range layers {
		prefix := dk.LayerDataPrefix(layerID, ino)
		iter, iterErr := r.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: PrefixUpperBound(prefix),
		})
		if iterErr != nil {
			return nil, 0, 0, iterErr
		}
		for iter.First(); iter.Valid(); iter.Next() {
			_, _, blkIdx, decKeyErr := dk.DecodeLayerDataKey(iter.Key())
			if decKeyErr != nil {
				_ = iter.Close()
				return nil, 0, 0, decKeyErr
			}
			if blkIdx < minBlockIdx || blkIdx > maxBlockIdx {
				continue
			}
			if _, exists := seen[blkIdx]; exists {
				continue
			}
			seen[blkIdx] = struct{}{}
			tag, payload, decErr := DecodeEnvelope(iter.Value())
			if decErr != nil {
				_ = iter.Close()
				return nil, 0, 0, decErr
			}
			if tag == TagLive {
				ref, refErr := DecodeBlobRef(payload)
				if refErr != nil {
					_ = iter.Close()
					return nil, 0, 0, refErr
				}
				blen := uint64(ref.Length)
				if i == 0 {
					workBytes += blen
				}
				effBytes += blen
				live = append(live, blkIdx)
			}
		}
		if closeErr := iter.Close(); closeErr != nil {
			return nil, 0, 0, closeErr
		}
	}
	return live, workBytes, effBytes, nil
}

func layerHasAnyKey(r pebbleReader, dk diskKeys, layerID uint64) (bool, error) {
	start, end := dk.LayerRangeBounds(layerID)
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
	return overlayGetInode(r, d.keys, layers, ino)
}

func (d *DiskStore) readDataFromLayers(r pebbleReader, layers []uint64, ino uint64, offset int64, size int) ([]byte, error) {
	return d.readDataFromLayersInto(r, layers, ino, offset, size, nil)
}

func (d *DiskStore) readDataFromLayersInto(r pebbleReader, layers []uint64, ino uint64, offset int64, size int, dst []byte) ([]byte, error) {
	if offset < 0 || size < 0 {
		return nil, ErrInvalid
	}
	inode, err := overlayGetInode(r, d.keys, layers, ino)
	if err != nil {
		return nil, err
	}
	switch inode.Mode & ModeTypeMask {
	case ModeDir:
		return nil, ErrIsDir
	case ModeRegular:
	default:
		return nil, ErrInvalid
	}
	return d.readRegularDataInto(r, layers, inode, offset, size, dst)
}

func (d *DiskStore) readRegularDataInto(r pebbleReader, layers []uint64, inode Inode, offset int64, size int, dst []byte) ([]byte, error) {
	if size == 0 || uint64(offset) >= inode.Size {
		return []byte{}, nil
	}
	end := min(uint64(offset)+uint64(size), inode.Size)
	outLen := int(end - uint64(offset))
	var out []byte
	if cap(dst) >= outLen {
		out = dst[:outLen]
		clear(out)
	} else {
		out = make([]byte, outLen)
	}

	blkSize := uint64(d.blockSize)
	startBlk := uint64(offset) / blkSize
	endBlk := (end - 1) / blkSize

	for blkIdx := startBlk; blkIdx <= endBlk; blkIdx++ {
		ref, ok, refErr := overlayGetDataRef(r, d.keys, layers, inode.Ino, blkIdx)
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

	_, layers, err := resolveBranchLayers(snap, d.keys, branch)
	if err != nil {
		return Inode{}, err
	}
	return d.readInodeFromLayers(snap, layers, ino)
}

func (d *DiskStore) Lookup(_ context.Context, branch string, parentIno uint64, name string) (Entry, Inode, error) {
	if d.closing.Load() {
		return Entry{}, Inode{}, ErrClosed
	}
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return Entry{}, Inode{}, ErrInvalid
	}
	if len(name) > MaxFileNameBytes {
		return Entry{}, Inode{}, ErrNameTooLong
	}
	if strings.ContainsRune(name, '/') || strings.ContainsRune(name, '\x00') {
		return Entry{}, Inode{}, ErrInvalid
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	_, layers, err := resolveBranchLayers(snap, dk, branch)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	parent, err := overlayGetInode(snap, dk, layers, parentIno)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Entry{}, Inode{}, ErrNotDir
	}
	if parent.Nlink == 0 {
		return Entry{}, Inode{}, ErrNotFound
	}
	if name == "" {
		return Entry{}, Inode{}, ErrNotFound
	}
	if name == "." {
		return Entry{Ino: parent.Ino, Mode: parent.Mode}, parent, nil
	}
	if name == ".." {
		if parentIno == RootIno {
			return Entry{Ino: parent.Ino, Mode: parent.Mode}, parent, nil
		}
		if parent.ParentIno == 0 || parent.ParentIno == parentIno {
			return Entry{}, Inode{}, ErrNotFound
		}
		grandparent, gpErr := overlayGetInode(snap, dk, layers, parent.ParentIno)
		if gpErr != nil {
			return Entry{}, Inode{}, gpErr
		}
		if grandparent.Nlink == 0 {
			return Entry{}, Inode{}, ErrNotFound
		}
		return Entry{Ino: grandparent.Ino, Mode: grandparent.Mode}, grandparent, nil
	}
	ent, err := overlayGetEntry(snap, dk, layers, parentIno, []byte(name))
	if err != nil {
		return Entry{}, Inode{}, err
	}
	inode, err := overlayGetInode(snap, dk, layers, ent.Ino)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	if inode.Nlink == 0 {
		return Entry{}, Inode{}, ErrNotFound
	}
	return ent, inode, nil
}

func (d *DiskStore) ReadDir(_ context.Context, branch string, parentIno uint64) ([]DirEntry, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	_, layers, err := resolveBranchLayers(snap, dk, branch)
	if err != nil {
		return nil, err
	}
	parent, err := overlayGetInode(snap, dk, layers, parentIno)
	if err != nil {
		return nil, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return nil, ErrNotDir
	}
	return overlayReadDir(snap, dk, layers, parentIno)
}

func (d *DiskStore) Readlink(_ context.Context, branch string, ino uint64) ([]byte, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalid
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	_, layers, err := resolveBranchLayers(snap, dk, branch)
	if err != nil {
		return nil, err
	}
	inode, err := overlayGetInode(snap, dk, layers, ino)
	if err != nil {
		return nil, err
	}
	if inode.Mode&ModeTypeMask != ModeSymlink {
		return nil, ErrInvalid
	}
	return append([]byte(nil), inode.SymlinkTarget...), nil
}

func (d *DiskStore) ReadDirPage(_ context.Context, branch string, parentIno, offset uint64, limit uint32) ([]DirEntry, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if parentIno == 0 || parentIno > math.MaxInt64 || limit == 0 || limit > MaxReadDirLimit || offset > math.MaxInt64 {
		return nil, ErrInvalid
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	_, layers, err := resolveBranchLayers(snap, dk, branch)
	if err != nil {
		return nil, err
	}
	dir, err := overlayGetInode(snap, dk, layers, parentIno)
	if err != nil {
		return nil, err
	}
	if dir.Mode&ModeTypeMask != ModeDir {
		return nil, ErrNotDir
	}
	if dir.Nlink == 0 {
		return []DirEntry{}, nil
	}

	maxCount := int(limit)
	entries := make([]DirEntry, 0, min(maxCount, 16))
	if offset < 1 && len(entries) < maxCount {
		entries = append(entries, DirEntry{
			Name:       ".",
			Ino:        dir.Ino,
			Mode:       dir.Mode,
			NextOffset: 1,
		})
	}
	if offset < 2 && len(entries) < maxCount {
		dotDotIno := dir.Ino
		dotDotMode := dir.Mode
		if parentIno != RootIno && dir.ParentIno > 0 {
			parent, readErr := overlayGetInode(snap, dk, layers, dir.ParentIno)
			if readErr == nil {
				dotDotIno = parent.Ino
				dotDotMode = parent.Mode
			} else if !errors.Is(readErr, ErrNotFound) {
				return nil, readErr
			}
		}
		entries = append(entries, DirEntry{
			Name:       "..",
			Ino:        dotDotIno,
			Mode:       dotDotMode,
			NextOffset: 2,
		})
	}
	if len(entries) == maxCount {
		return entries, nil
	}

	children, err := overlayReadDir(snap, dk, layers, parentIno)
	if err != nil {
		return nil, err
	}
	var skip uint64
	if offset > 2 {
		skip = offset - 2
	}
	if skip >= uint64(len(children)) {
		return entries, nil
	}
	next := max(offset, uint64(2))
	for _, ch := range children[skip:] {
		if len(entries) >= maxCount {
			break
		}
		next++
		entries = append(entries, DirEntry{
			Name:       ch.Name,
			Ino:        ch.Ino,
			Mode:       ch.Mode,
			NextOffset: next,
		})
	}
	return entries, nil
}

func (d *DiskStore) ReadDirPlus(_ context.Context, branch string, parentIno, offset uint64, limit uint32) ([]DirEntryPlus, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if parentIno == 0 || parentIno > math.MaxInt64 || limit == 0 || limit > MaxReadDirLimit || offset > math.MaxInt64 {
		return nil, ErrInvalid
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	_, layers, err := resolveBranchLayers(snap, dk, branch)
	if err != nil {
		return nil, err
	}
	dir, err := overlayGetInode(snap, dk, layers, parentIno)
	if err != nil {
		return nil, err
	}
	if dir.Mode&ModeTypeMask != ModeDir {
		return nil, ErrNotDir
	}
	if dir.Nlink == 0 {
		return []DirEntryPlus{}, nil
	}

	maxCount := int(limit)
	entries := make([]DirEntryPlus, 0, min(maxCount, 16))
	if offset < 1 && len(entries) < maxCount {
		entries = append(entries, DirEntryPlus{
			Name:       ".",
			Ino:        dir.Ino,
			Mode:       dir.Mode,
			NextOffset: 1,
			Inode:      dir,
		})
	}
	if offset < 2 && len(entries) < maxCount {
		dotDot := dir
		if parentIno != RootIno && dir.ParentIno > 0 {
			parent, readErr := overlayGetInode(snap, dk, layers, dir.ParentIno)
			if readErr == nil {
				dotDot = parent
			} else if !errors.Is(readErr, ErrNotFound) {
				return nil, readErr
			}
		}
		entries = append(entries, DirEntryPlus{
			Name:       "..",
			Ino:        dotDot.Ino,
			Mode:       dotDot.Mode,
			NextOffset: 2,
			Inode:      dotDot,
		})
	}
	if len(entries) == maxCount {
		return entries, nil
	}

	children, err := overlayReadDir(snap, dk, layers, parentIno)
	if err != nil {
		return nil, err
	}
	var skip uint64
	if offset > 2 {
		skip = offset - 2
	}
	if skip >= uint64(len(children)) {
		return entries, nil
	}
	next := max(offset, uint64(2))
	for _, ch := range children[skip:] {
		if len(entries) >= maxCount {
			break
		}
		next++
		childInode, getErr := overlayGetInode(snap, dk, layers, ch.Ino)
		if errors.Is(getErr, ErrNotFound) {
			continue
		}
		if getErr != nil {
			return nil, getErr
		}
		if childInode.Nlink == 0 {
			continue
		}
		entries = append(entries, DirEntryPlus{
			Name:       ch.Name,
			Ino:        childInode.Ino,
			Mode:       childInode.Mode,
			NextOffset: next,
			Inode:      childInode,
		})
	}
	return entries, nil
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

	dk := d.keys
	_, layers, err := resolveBranchLayers(snap, dk, branch)
	if err != nil {
		return nil, err
	}
	inode, err := overlayGetInode(snap, dk, layers, ino)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(name, "user.") && !isAllowedUserXattrFileType(inode.Mode) {
		return nil, ErrNoData
	}
	return overlayGetXattr(snap, dk, layers, ino, []byte(name))
}

func (d *DiskStore) ListXattr(_ context.Context, branch string, ino uint64) ([]string, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	_, layers, err := resolveBranchLayers(snap, dk, branch)
	if err != nil {
		return nil, err
	}
	if _, err = overlayGetInode(snap, dk, layers, ino); err != nil {
		return nil, err
	}
	return overlayListXattr(snap, dk, layers, ino)
}

func (d *DiskStore) Read(ctx context.Context, branch string, ino uint64, offset int64, size int) ([]byte, error) {
	return d.ReadInto(ctx, branch, ino, offset, size, nil)
}

func (d *DiskStore) ReadInto(_ context.Context, branch string, ino uint64, offset int64, size int, dst []byte) ([]byte, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveBranchLayers(snap, d.keys, branch)
	if err != nil {
		return nil, err
	}
	return d.readDataFromLayersInto(snap, layers, ino, offset, size, dst)
}

func (d *DiskStore) GetAttrAtCommit(_ context.Context, commitID, ino uint64) (Inode, error) {
	if d.closing.Load() {
		return Inode{}, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	_, layers, err := resolveCommitLayers(snap, dk, commitID)
	if err != nil {
		return Inode{}, err
	}
	return overlayGetInode(snap, dk, layers, ino)
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

	dk := d.keys
	_, layers, err := resolveCommitLayers(snap, dk, commitID)
	if err != nil {
		return Entry{}, Inode{}, err
	}
	ent, err := overlayGetEntry(snap, dk, layers, parentIno, []byte(name))
	if err != nil {
		return Entry{}, Inode{}, err
	}
	inode, err := overlayGetInode(snap, dk, layers, ent.Ino)
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

	dk := d.keys
	_, layers, err := resolveCommitLayers(snap, dk, commitID)
	if err != nil {
		return nil, err
	}
	return overlayReadDir(snap, dk, layers, parentIno)
}

func (d *DiskStore) ReadAtCommit(_ context.Context, commitID, ino uint64, offset int64, size int) ([]byte, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	_, layers, err := resolveCommitLayers(snap, d.keys, commitID)
	if err != nil {
		return nil, err
	}
	return d.readDataFromLayers(snap, layers, ino, offset, size)
}

func (d *DiskStore) ReadCommitNode(_ context.Context, commitID, ino uint64, offset int64, size int) (CommitNodeResult, error) {
	if d.closing.Load() {
		return CommitNodeResult{}, ErrClosed
	}
	if commitID == 0 || ino == 0 || ino > math.MaxInt64 || offset < 0 || size < 0 {
		return CommitNodeResult{}, ErrInvalid
	}
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	_, layers, err := resolveCommitLayers(snap, dk, commitID)
	if err != nil {
		return CommitNodeResult{}, err
	}
	inode, err := overlayGetInode(snap, dk, layers, ino)
	if err != nil {
		return CommitNodeResult{}, err
	}

	switch inode.Mode & ModeTypeMask {
	case ModeDir:
		return CommitNodeResult{}, ErrIsDir
	case ModeSymlink:
		return CommitNodeResult{
			Mode:          inode.Mode,
			FileSize:      inode.Size,
			EOF:           true,
			SymlinkTarget: append([]byte(nil), inode.SymlinkTarget...),
		}, nil
	case ModeRegular:
		data, readErr := d.readRegularDataInto(snap, layers, inode, offset, size, nil)
		if readErr != nil {
			return CommitNodeResult{}, readErr
		}
		eof := uint64(offset) >= inode.Size || uint64(offset)+uint64(len(data)) >= inode.Size
		return CommitNodeResult{
			Mode:     inode.Mode,
			FileSize: inode.Size,
			Data:     data,
			EOF:      eof,
		}, nil
	default:
		return CommitNodeResult{}, ErrInvalid
	}
}
