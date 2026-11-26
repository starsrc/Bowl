package engine

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

type layerView struct {
	r      pebbleReader
	keys   diskKeys
	layers []uint64
}

func (v layerView) getInode(ino uint64) (Inode, bool, error) {
	if v.r == nil || len(v.layers) == 0 {
		return Inode{}, false, nil
	}
	inode, err := overlayGetInode(v.r, v.keys, v.layers, ino)
	if errors.Is(err, ErrNotFound) {
		return Inode{}, false, nil
	}
	if err != nil {
		return Inode{}, false, err
	}
	return inode, true, nil
}

func (v layerView) readDir(parent uint64) ([]DirEntry, error) {
	if v.r == nil || len(v.layers) == 0 {
		return nil, nil
	}
	return overlayReadDir(v.r, v.keys, v.layers, parent)
}

func (v layerView) readAllXattrs(ino uint64) (map[string][]byte, error) {
	out := make(map[string][]byte)
	if v.r == nil || len(v.layers) == 0 {
		return out, nil
	}
	err := overlayScanFirstWins(
		v.r,
		v.layers,
		func(layerID uint64) []byte { return v.keys.LayerXattrPrefix(layerID, ino) },
		func(rawKey []byte) (string, bool, error) {
			_, _, nameBytes, decKeyErr := v.keys.DecodeLayerXattrKey(rawKey)
			if decKeyErr != nil {
				return "", false, decKeyErr
			}
			return string(nameBytes), true, nil
		},
		func(_ int, name string, rawVal []byte) error {
			tag, payload, decErr := DecodeEnvelope(rawVal)
			if decErr != nil {
				return decErr
			}
			if tag == TagTombstone {
				return nil
			}
			if payload == nil {
				out[name] = []byte{}
			} else {
				out[name] = append([]byte(nil), payload...)
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (v layerView) readAllDataRefs(ino uint64) (map[uint64]BlobRef, error) {
	out := make(map[uint64]BlobRef)
	if v.r == nil || len(v.layers) == 0 {
		return out, nil
	}
	err := overlayScanFirstWins(
		v.r,
		v.layers,
		func(layerID uint64) []byte { return v.keys.LayerDataPrefix(layerID, ino) },
		func(rawKey []byte) (uint64, bool, error) {
			_, _, blkIdx, decKeyErr := v.keys.DecodeLayerDataKey(rawKey)
			return blkIdx, true, decKeyErr
		},
		func(_ int, blkIdx uint64, rawVal []byte) error {
			tag, payload, decErr := DecodeEnvelope(rawVal)
			if decErr != nil {
				return decErr
			}
			if tag == TagTombstone {
				return nil
			}
			ref, refErr := DecodeBlobRef(payload)
			if refErr != nil {
				return refErr
			}
			out[blkIdx] = ref
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (v layerView) scanParentDirs(ino uint64) ([]uint64, error) {
	if v.r == nil || len(v.layers) == 0 {
		return nil, nil
	}
	var parents []uint64
	err := overlayScanFirstWins(
		v.r,
		v.layers,
		func(layerID uint64) []byte { return v.keys.LayerParentPrefix(layerID, ino) },
		func(rawKey []byte) (uint64, bool, error) {
			_, _, parentDirIno, decKeyErr := v.keys.DecodeLayerParentKey(rawKey)
			return parentDirIno, true, decKeyErr
		},
		func(_ int, parentDirIno uint64, _ []byte) error {
			parents = append(parents, parentDirIno)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return parents, nil
}

type layerSource struct {
	layerID    uint64
	r          pebbleReader
	fromSource bool
	fromTarget bool
}

type dirtySet struct {
	dirtyDirs             map[uint64]struct{}
	dirtyInos             map[uint64]struct{}
	targetHardlinkDirs    map[uint64]struct{}
	sourceDeltaParentKeys map[uint64]map[uint64]struct{}
}

func commonSuffixLen(a, b []uint64) int {
	ia := len(a) - 1
	ib := len(b) - 1
	n := 0
	for ia >= 0 && ib >= 0 && a[ia] == b[ib] {
		n++
		ia--
		ib--
	}
	return n
}

func collectDeltaLayers(base, target, source layerView, targetIsBase bool) []layerSource {
	sOS := commonSuffixLen(base.layers, source.layers)
	common := sOS
	sOT := len(base.layers)
	if !targetIsBase {
		sOT = commonSuffixLen(base.layers, target.layers)
		common = min(common, sOT)
	}

	byID := make(map[uint64]*layerSource)
	var ordered []uint64
	addLayer := func(lid uint64, r pebbleReader, fromSrc, fromTgt bool) {
		if lid == 0 {
			return
		}
		if existing, ok := byID[lid]; ok {
			existing.fromSource = existing.fromSource || fromSrc
			existing.fromTarget = existing.fromTarget || fromTgt
			return
		}
		byID[lid] = &layerSource{
			layerID:    lid,
			r:          r,
			fromSource: fromSrc,
			fromTarget: fromTgt,
		}
		ordered = append(ordered, lid)
	}

	for _, lid := range base.layers[:len(base.layers)-common] {
		addLayer(lid, base.r, false, false)
	}
	srcDeltaCut := len(source.layers) - sOS
	for idx, lid := range source.layers[:len(source.layers)-common] {
		addLayer(lid, source.r, idx < srcDeltaCut, false)
	}
	if !targetIsBase {
		tgtDeltaCut := len(target.layers) - sOT
		for idx, lid := range target.layers[:len(target.layers)-common] {
			addLayer(lid, target.r, false, idx < tgtDeltaCut)
		}
	}

	out := make([]layerSource, 0, len(ordered))
	for _, lid := range ordered {
		out = append(out, *byID[lid])
	}
	return out
}

func discoverDirtySet(base, target, source layerView, targetIsBase bool) (*dirtySet, error) {
	ds := &dirtySet{
		dirtyDirs:             make(map[uint64]struct{}),
		dirtyInos:             make(map[uint64]struct{}),
		targetHardlinkDirs:    make(map[uint64]struct{}),
		sourceDeltaParentKeys: make(map[uint64]map[uint64]struct{}),
	}

	deltaLayers := collectDeltaLayers(base, target, source, targetIsBase)
	if len(deltaLayers) == 0 {
		return ds, nil
	}

	targetParentSeedDirs := make(map[uint64]struct{})

	dk := base.keys
	for _, ls := range deltaLayers {
		start, end := dk.LayerRangeBounds(ls.layerID)
		iter, err := ls.r.NewIter(&pebble.IterOptions{
			LowerBound: start,
			UpperBound: end,
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			k := iter.Key()
			sub, ok := dk.stripPrefix(k)
			if !ok || len(sub) < 16 {
				continue
			}
			switch {
			case len(sub) >= LayerEntryPrefixLen && bytes.Equal(sub[10:17], layerEntryTagBytes):
				_, parentIno, _, decErr := dk.DecodeLayerEntryKey(k)
				if decErr != nil {
					_ = iter.Close()
					return nil, decErr
				}
				ds.dirtyDirs[parentIno] = struct{}{}
			case len(sub) == LayerInodeKeyLen && bytes.Equal(sub[10:17], layerInodeTagBytes):
				_, ino, decErr := dk.DecodeLayerInodeKey(k)
				if decErr != nil {
					_ = iter.Close()
					return nil, decErr
				}
				ds.dirtyInos[ino] = struct{}{}
			case len(sub) >= LayerXattrPrefixLen && bytes.Equal(sub[10:17], layerXattrTagBytes):
				_, ino, _, decErr := dk.DecodeLayerXattrKey(k)
				if decErr != nil {
					_ = iter.Close()
					return nil, decErr
				}
				ds.dirtyInos[ino] = struct{}{}
			case len(sub) == LayerDataKeyLen && bytes.Equal(sub[10:16], layerDataTagBytes):
				_, ino, _, decErr := dk.DecodeLayerDataKey(k)
				if decErr != nil {
					_ = iter.Close()
					return nil, decErr
				}
				ds.dirtyInos[ino] = struct{}{}
			case len(sub) == LayerParentKeyLen && bytes.Equal(sub[10:18], layerParentTagBytes):
				_, ino, parentDirIno, decErr := dk.DecodeLayerParentKey(k)
				if decErr != nil {
					_ = iter.Close()
					return nil, decErr
				}
				ds.dirtyInos[ino] = struct{}{}
				ds.dirtyDirs[parentDirIno] = struct{}{}
				if ls.fromSource {
					pm := ds.sourceDeltaParentKeys[ino]
					if pm == nil {
						pm = make(map[uint64]struct{})
						ds.sourceDeltaParentKeys[ino] = pm
					}
					pm[parentDirIno] = struct{}{}
				}
				if ls.fromTarget {
					targetParentSeedDirs[parentDirIno] = struct{}{}
				}
			}
		}
		if closeErr := iter.Close(); closeErr != nil {
			return nil, closeErr
		}
	}

	views := []layerView{base, source}
	if !targetIsBase {
		views = append(views, target)
	}

	walkedDirs := make(map[uint64]struct{}, len(ds.dirtyDirs))
	dirQueue := make([]uint64, 0, len(ds.dirtyDirs))
	enqueueDir := func(dirIno uint64) {
		if dirIno == 0 {
			return
		}
		ds.dirtyDirs[dirIno] = struct{}{}
		if _, done := walkedDirs[dirIno]; !done {
			walkedDirs[dirIno] = struct{}{}
			dirQueue = append(dirQueue, dirIno)
		}
	}

	for dIno := range ds.dirtyDirs {
		enqueueDir(dIno)
	}

	for ino := range ds.dirtyInos {
		everLinked := false
		for _, vw := range views {
			node, ok, err := vw.getInode(ino)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			if ino != RootIno && node.ParentIno != 0 {
				enqueueDir(node.ParentIno)
			}
			if node.Mode&ModeTypeMask != ModeDir && (node.Nlink > 1 || node.Flags&InodeFlagEverLinked != 0) {
				everLinked = true
			}
		}
		if everLinked {
			for _, vw := range views {
				pDirs, err := vw.scanParentDirs(ino)
				if err != nil {
					return nil, err
				}
				for _, pDir := range pDirs {
					enqueueDir(pDir)
				}
			}
		}
	}

	for len(dirQueue) > 0 {
		dIno := dirQueue[len(dirQueue)-1]
		dirQueue = dirQueue[:len(dirQueue)-1]
		if dIno == RootIno {
			continue
		}
		for _, vw := range views {
			node, ok, err := vw.getInode(dIno)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			if node.ParentIno != 0 && node.ParentIno != dIno {
				enqueueDir(node.ParentIno)
			}
		}
	}

	if !targetIsBase && len(targetParentSeedDirs) > 0 {
		hlQueue := make([]uint64, 0, len(targetParentSeedDirs))
		for dIno := range targetParentSeedDirs {
			if _, seen := ds.targetHardlinkDirs[dIno]; !seen {
				ds.targetHardlinkDirs[dIno] = struct{}{}
				hlQueue = append(hlQueue, dIno)
			}
		}
		for len(hlQueue) > 0 {
			dIno := hlQueue[len(hlQueue)-1]
			hlQueue = hlQueue[:len(hlQueue)-1]
			if dIno == RootIno {
				continue
			}
			node, ok, err := target.getInode(dIno)
			if err != nil {
				return nil, err
			}
			if ok && node.ParentIno != 0 && node.ParentIno != dIno {
				if _, seen := ds.targetHardlinkDirs[node.ParentIno]; !seen {
					ds.targetHardlinkDirs[node.ParentIno] = struct{}{}
					hlQueue = append(hlQueue, node.ParentIno)
				}
			}
		}
	}

	return ds, nil
}

type dirSliceFrame struct {
	dirPath    []byte
	bEntries   []DirEntry
	tEntries   []DirEntry
	sEntries   []DirEntry
	bIdx       int
	tIdx       int
	sIdx       int
	targetSame bool

	nextName string
	nextPath []byte
	bEntry   *Entry
	tEntry   *Entry
	sEntry   *Entry
}

func (f *dirSliceFrame) peekNext() bool {
	hasMin := false
	var minName string
	consider := func(entries []DirEntry, idx int) {
		if idx < len(entries) {
			if !hasMin || entries[idx].Name < minName {
				minName = entries[idx].Name
				hasMin = true
			}
		}
	}
	consider(f.bEntries, f.bIdx)
	if !f.targetSame {
		consider(f.tEntries, f.tIdx)
	}
	consider(f.sEntries, f.sIdx)
	if !hasMin {
		f.nextName = ""
		f.nextPath = nil
		f.bEntry = nil
		f.tEntry = nil
		f.sEntry = nil
		return false
	}
	f.nextName = minName
	f.nextPath = joinRelPath(f.dirPath, []byte(minName))

	f.bEntry = nil
	if f.bIdx < len(f.bEntries) && f.bEntries[f.bIdx].Name == minName {
		e := Entry{Ino: f.bEntries[f.bIdx].Ino, Mode: f.bEntries[f.bIdx].Mode}
		f.bEntry = &e
	}
	f.tEntry = nil
	if f.targetSame {
		if f.bEntry != nil {
			e := *f.bEntry
			f.tEntry = &e
		}
	} else if f.tIdx < len(f.tEntries) && f.tEntries[f.tIdx].Name == minName {
		e := Entry{Ino: f.tEntries[f.tIdx].Ino, Mode: f.tEntries[f.tIdx].Mode}
		f.tEntry = &e
	}
	f.sEntry = nil
	if f.sIdx < len(f.sEntries) && f.sEntries[f.sIdx].Name == minName {
		e := Entry{Ino: f.sEntries[f.sIdx].Ino, Mode: f.sEntries[f.sIdx].Mode}
		f.sEntry = &e
	}
	return true
}

func (f *dirSliceFrame) consumeAndPeekNext() bool {
	if f.bEntry != nil {
		f.bIdx++
	}
	if !f.targetSame && f.tEntry != nil {
		f.tIdx++
	}
	if f.sEntry != nil {
		f.sIdx++
	}
	return f.peekNext()
}

type dirSliceHeap []*dirSliceFrame

func (h dirSliceHeap) Len() int           { return len(h) }
func (h dirSliceHeap) Less(i, j int) bool { return bytes.Compare(h[i].nextPath, h[j].nextPath) < 0 }
func (h dirSliceHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *dirSliceHeap) Push(x any) {
	if f, ok := x.(*dirSliceFrame); ok {
		*h = append(*h, f)
	}
}
func (h *dirSliceHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

type treeCompareCtx struct {
	ctx          context.Context
	disk         *DiskStore
	base         layerView
	target       layerView
	source       layerView
	targetIsBase bool
	dirty        *dirtySet
}

func newTreeCompareCtx(ctx context.Context, disk *DiskStore, base, target, source layerView, targetIsBase bool, dirty *dirtySet) *treeCompareCtx {
	if ctx == nil {
		ctx = context.Background()
	}
	return &treeCompareCtx{
		ctx:          ctx,
		disk:         disk,
		base:         base,
		target:       target,
		source:       source,
		targetIsBase: targetIsBase,
		dirty:        dirty,
	}
}

func userInodeFlags(flags uint32) uint32 {
	return flags &^ InodeFlagEverLinked
}

func metaEqual(a, b *Inode) bool {
	return (a.Mode&ModePermMask) == (b.Mode&ModePermMask) &&
		a.UID == b.UID &&
		a.GID == b.GID &&
		userInodeFlags(a.Flags) == userInodeFlags(b.Flags)
}

func xattrsEqual(xa, xb map[string][]byte) bool {
	if len(xa) != len(xb) {
		return false
	}
	for k, va := range xa {
		vb, ok := xb[k]
		if !ok || !bytes.Equal(va, vb) {
			return false
		}
	}
	return true
}

func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func blockLogicalLen(fileSize, blockIdx uint64, blockSize uint32) uint64 {
	blkSz := uint64(blockSize)
	start := blockIdx * blkSz
	if start >= fileSize {
		return 0
	}
	return min(blkSz, fileSize-start)
}

func (tc *treeCompareCtx) singleBlockIsZero(fileSize, blockIdx uint64, ref BlobRef) (bool, error) {
	logicalLen := blockLogicalLen(fileSize, blockIdx, tc.disk.blockSize)
	if logicalLen == 0 {
		return true, nil
	}
	readLen := min(uint64(ref.Length), logicalLen)
	if readLen == 0 {
		return true, nil
	}
	data, err := tc.disk.packs.ReadBlock(ref)
	if err != nil {
		return false, err
	}
	return isAllZero(data[:readLen]), nil
}

func (tc *treeCompareCtx) blocksEqual(fileSize, blockIdx uint64, refA, refB BlobRef) (bool, error) {
	if refA == refB {
		return true, nil
	}
	logicalLen := blockLogicalLen(fileSize, blockIdx, tc.disk.blockSize)
	if logicalLen == 0 {
		return true, nil
	}
	dataA, err := tc.disk.packs.ReadBlock(refA)
	if err != nil {
		return false, err
	}
	dataB, err := tc.disk.packs.ReadBlock(refB)
	if err != nil {
		return false, err
	}
	lenA := min(uint64(len(dataA)), logicalLen)
	lenB := min(uint64(len(dataB)), logicalLen)
	common := min(lenA, lenB)
	if !bytes.Equal(dataA[:common], dataB[:common]) {
		return false, nil
	}
	if lenA > common && !isAllZero(dataA[common:lenA]) {
		return false, nil
	}
	if lenB > common && !isAllZero(dataB[common:lenB]) {
		return false, nil
	}
	return true, nil
}

func (tc *treeCompareCtx) regDataEqual(viewA layerView, a *Inode, viewB layerView, b *Inode) (bool, error) {
	if a.Ino == b.Ino {
		if _, dirty := tc.dirty.dirtyInos[a.Ino]; !dirty {
			return true, nil
		}
	}
	refsA, err := viewA.readAllDataRefs(a.Ino)
	if err != nil {
		return false, err
	}
	refsB, err := viewB.readAllDataRefs(b.Ino)
	if err != nil {
		return false, err
	}
	allIndices := make([]uint64, 0, len(refsA)+len(refsB))
	seen := make(map[uint64]struct{}, len(refsA)+len(refsB))
	for idx := range refsA {
		seen[idx] = struct{}{}
		allIndices = append(allIndices, idx)
	}
	for idx := range refsB {
		if _, ok := seen[idx]; !ok {
			allIndices = append(allIndices, idx)
		}
	}
	slices.Sort(allIndices)

	for _, idx := range allIndices {
		refA, hasA := refsA[idx]
		refB, hasB := refsB[idx]
		switch {
		case hasA && !hasB:
			eq, zErr := tc.singleBlockIsZero(a.Size, idx, refA)
			if zErr != nil || !eq {
				return false, zErr
			}
		case !hasA && hasB:
			eq, zErr := tc.singleBlockIsZero(b.Size, idx, refB)
			if zErr != nil || !eq {
				return false, zErr
			}
		default:
			eq, bErr := tc.blocksEqual(a.Size, idx, refA, refB)
			if bErr != nil || !eq {
				return false, bErr
			}
		}
	}
	return true, nil
}

func (tc *treeCompareCtx) payloadEqual(viewA layerView, a *Inode, viewB layerView, b *Inode) (bool, error) {
	if (a.Mode & ModeTypeMask) != (b.Mode & ModeTypeMask) {
		return false, nil
	}
	switch a.Mode & ModeTypeMask {
	case ModeDir, ModeFIFO, ModeSocket:
		return true, nil
	case ModeChar, ModeBlock:
		return a.Rdev == b.Rdev, nil
	case ModeSymlink:
		return bytes.Equal(a.SymlinkTarget, b.SymlinkTarget), nil
	case ModeRegular:
		if a.Size != b.Size {
			return false, nil
		}
		if a.Size == 0 {
			return true, nil
		}
		return tc.regDataEqual(viewA, a, viewB, b)
	default:
		return false, nil
	}
}

func (tc *treeCompareCtx) nodesEqual(viewA layerView, a *Inode, viewB layerView, b *Inode) (bool, error) {
	if a.Ino == b.Ino {
		if _, dirty := tc.dirty.dirtyInos[a.Ino]; !dirty {
			return true, nil
		}
	}
	if (a.Mode & ModeTypeMask) != (b.Mode & ModeTypeMask) {
		return false, nil
	}
	if !metaEqual(a, b) {
		return false, nil
	}
	if (a.Mode&ModeTypeMask) == ModeRegular && a.Size != b.Size {
		return false, nil
	}
	xa, err := viewA.readAllXattrs(a.Ino)
	if err != nil {
		return false, err
	}
	xb, err := viewB.readAllXattrs(b.Ino)
	if err != nil {
		return false, err
	}
	if !xattrsEqual(xa, xb) {
		return false, nil
	}
	return tc.payloadEqual(viewA, a, viewB, b)
}

func (tc *treeCompareCtx) dirSelfEqual(viewA layerView, a *Inode, viewB layerView, b *Inode) (bool, error) {
	if a.Ino == b.Ino {
		if _, dirty := tc.dirty.dirtyInos[a.Ino]; !dirty {
			return true, nil
		}
	}
	if !metaEqual(a, b) {
		return false, nil
	}
	xa, err := viewA.readAllXattrs(a.Ino)
	if err != nil {
		return false, err
	}
	xb, err := viewB.readAllXattrs(b.Ino)
	if err != nil {
		return false, err
	}
	return xattrsEqual(xa, xb), nil
}

func (tc *treeCompareCtx) isNodeUnchangedInView(oNode, xNode *Inode) bool {
	if oNode == nil || xNode == nil || oNode.Ino != xNode.Ino {
		return false
	}
	_, dirty := tc.dirty.dirtyInos[oNode.Ino]
	return !dirty
}

func (tc *treeCompareCtx) isSubtreeModifiedVsBase(baseIno uint64, branchView layerView, branchIno uint64) (bool, error) {
	if baseIno == branchIno {
		_, dirDirty := tc.dirty.dirtyDirs[baseIno]
		_, inoDirty := tc.dirty.dirtyInos[baseIno]
		if !dirDirty && !inoDirty {
			return false, nil
		}
	}
	bNode, bOK, err := tc.base.getInode(baseIno)
	if err != nil || !bOK {
		return true, err
	}
	xNode, xOK, err := branchView.getInode(branchIno)
	if err != nil || !xOK {
		return true, err
	}
	if (xNode.Mode & ModeTypeMask) != (bNode.Mode & ModeTypeMask) {
		return true, nil
	}
	selfEq, eqErr := tc.dirSelfEqual(tc.base, &bNode, branchView, &xNode)
	if eqErr != nil || !selfEq {
		return true, eqErr
	}

	bEntries, err := tc.base.readDir(baseIno)
	if err != nil {
		return false, err
	}
	xEntries, err := branchView.readDir(branchIno)
	if err != nil {
		return false, err
	}

	bi, xi := 0, 0
	for bi < len(bEntries) || xi < len(xEntries) {
		switch {
		case xi < len(xEntries) && (bi >= len(bEntries) || xEntries[xi].Name < bEntries[bi].Name):
			return true, nil
		case bi < len(bEntries) && (xi >= len(xEntries) || bEntries[bi].Name < xEntries[xi].Name):
			bi++
		default:
			bc := bEntries[bi]
			xc := xEntries[xi]
			if (bc.Mode & ModeTypeMask) != (xc.Mode & ModeTypeMask) {
				return true, nil
			}
			if (bc.Mode & ModeTypeMask) == ModeDir {
				subMod, subErr := tc.isSubtreeModifiedVsBase(bc.Ino, branchView, xc.Ino)
				if subErr != nil || subMod {
					return subMod, subErr
				}
			} else {
				bn, bnOK, rErr := tc.base.getInode(bc.Ino)
				if rErr != nil || !bnOK {
					return true, rErr
				}
				xn, xnOK, rErr := branchView.getInode(xc.Ino)
				if rErr != nil || !xnOK {
					return true, rErr
				}
				eq, nErr := tc.nodesEqual(tc.base, &bn, branchView, &xn)
				if nErr != nil || !eq {
					return true, nErr
				}
			}
			bi++
			xi++
		}
	}
	return false, nil
}

func isSecurityMetaChanged(a, b *Inode) bool {
	if a.UID != b.UID || a.GID != b.GID {
		return true
	}
	const secBits = ModeSetUID | ModeSetGID
	return (a.Mode & secBits) != (b.Mode & secBits)
}

func hasSecurityXattrChange(xa, xb map[string][]byte) bool {
	for k, va := range xa {
		if strings.HasPrefix(k, securityXattrPrefixStr) {
			if vb, ok := xb[k]; !ok || !bytes.Equal(va, vb) {
				return true
			}
		}
	}
	for k := range xb {
		if strings.HasPrefix(k, securityXattrPrefixStr) {
			if _, ok := xa[k]; !ok {
				return true
			}
		}
	}
	return false
}

func isAllowedUserXattrFileType(mode uint32) bool {
	ft := mode & ModeTypeMask
	return ft == ModeRegular || ft == ModeDir || ft == ModeSocket
}

func classifyXattrKeyConflict(key string) ConflictKind {
	if strings.HasPrefix(key, securityXattrPrefixStr) {
		return ConflictSecurityAttr
	}
	if key == systemQuotaXattrName {
		return ConflictXattrQuota
	}
	return ConflictXattr
}

func mergeNodeAttributesAndXattrs(o, t, s *Inode, xo, xt, xs map[string][]byte, tPayloadChanged, sPayloadChanged bool) (mergedMeta Inode, mergedXattrs map[string][]byte, conflict ConflictKind) {
	mergedMeta = *t
	everLinkedBit := (t.Flags | s.Flags) & InodeFlagEverLinked
	if o != nil {
		everLinkedBit |= o.Flags & InodeFlagEverLinked
		tSecChanged := isSecurityMetaChanged(o, t) || hasSecurityXattrChange(xo, xt)
		sSecChanged := isSecurityMetaChanged(o, s) || hasSecurityXattrChange(xo, xs)
		if (tPayloadChanged && sSecChanged) || (sPayloadChanged && tSecChanged) {
			return mergedMeta, nil, ConflictSecurityAttr
		}
	}

	if o == nil {
		if !metaEqual(t, s) {
			return mergedMeta, nil, ConflictAttr
		}
	} else {
		var attrConflict bool
		mergeU32 := func(ov, tv, sv uint32) uint32 {
			if tv == sv {
				return tv
			}
			if tv == ov {
				return sv
			}
			if sv == ov {
				return tv
			}
			attrConflict = true
			return tv
		}
		permO := o.Mode & ModePermMask
		permT := t.Mode & ModePermMask
		permS := s.Mode & ModePermMask
		mergedPerm := mergeU32(permO, permT, permS)
		mergedMeta.Mode = (t.Mode & ModeTypeMask) | mergedPerm
		mergedMeta.UID = mergeU32(o.UID, t.UID, s.UID)
		mergedMeta.GID = mergeU32(o.GID, t.GID, s.GID)
		mergedMeta.Flags = mergeU32(userInodeFlags(o.Flags), userInodeFlags(t.Flags), userInodeFlags(s.Flags)) | everLinkedBit
		if attrConflict {
			return mergedMeta, nil, ConflictAttr
		}
	}
	mergedMeta.Flags |= everLinkedBit

	mergedXattrs = make(map[string][]byte)
	allKeys := make(map[string]struct{}, len(xt)+len(xs))
	for k := range xt {
		allKeys[k] = struct{}{}
	}
	for k := range xs {
		allKeys[k] = struct{}{}
	}
	for k := range xo {
		allKeys[k] = struct{}{}
	}

	for k := range allKeys {
		vo, okO := xo[k]
		vt, okT := xt[k]
		vs, okS := xs[k]

		sameTS := okT == okS && (!okT || bytes.Equal(vt, vs))
		var chosen []byte
		var hasChosen bool

		switch {
		case sameTS:
			chosen, hasChosen = vt, okT
		case o != nil:
			sameOT := okO == okT && (!okO || bytes.Equal(vo, vt))
			sameOS := okO == okS && (!okO || bytes.Equal(vo, vs))
			switch {
			case sameOT:
				chosen, hasChosen = vs, okS
			case sameOS:
				chosen, hasChosen = vt, okT
			default:
				return mergedMeta, nil, classifyXattrKeyConflict(k)
			}
		default:
			return mergedMeta, nil, classifyXattrKeyConflict(k)
		}

		if hasChosen {
			if strings.HasPrefix(k, userXattrPrefixStr) && !isAllowedUserXattrFileType(mergedMeta.Mode) {
				return mergedMeta, nil, ConflictXattr
			}
			mergedXattrs[k] = append([]byte(nil), chosen...)
		}
	}

	totalListBytes := 0
	for k := range mergedXattrs {
		totalListBytes += len(k) + 1
	}
	if totalListBytes > MaxXattrListBytes {
		return mergedMeta, nil, ConflictXattrQuota
	}

	return mergedMeta, mergedXattrs, ConflictNone
}

type nodeDecisionKind uint8

const (
	nodeDecisionSkip nodeDecisionKind = iota
	nodeDecisionKeepTarget
	nodeDecisionAdoptSource
	nodeDecisionDeleteTarget
	nodeDecisionFuse
	nodeDecisionConflict
)

type analyzedNode struct {
	path           []byte
	kind           DiffKind
	conflict       ConflictKind
	emitDiff       bool
	decision       nodeDecisionKind
	descend        bool
	isDir          bool
	oNode          *Inode
	tNode          *Inode
	sNode          *Inode
	mergedMeta     Inode
	mergedXattrs   map[string][]byte
	payloadFromSrc bool
	payloadFromTgt bool
}

func (tc *treeCompareCtx) analyzeEntry(relPath []byte, bEntry, tEntry, sEntry *Entry) (*analyzedNode, error) {
	an := &analyzedNode{path: relPath}

	var (
		oNode, tNode, sNode Inode
		hasO, hasT, hasS    bool
		err                 error
	)
	if bEntry != nil {
		oNode, hasO, err = tc.base.getInode(bEntry.Ino)
		if err != nil {
			return nil, err
		}
		if hasO {
			an.oNode = &oNode
		}
	}
	if tEntry != nil {
		if tc.targetIsBase && bEntry != nil && tEntry.Ino == bEntry.Ino && hasO {
			tNode = oNode
			hasT = true
		} else {
			tNode, hasT, err = tc.target.getInode(tEntry.Ino)
			if err != nil {
				return nil, err
			}
		}
		if hasT {
			an.tNode = &tNode
		}
	}
	if sEntry != nil {
		sNode, hasS, err = tc.source.getInode(sEntry.Ino)
		if err != nil {
			return nil, err
		}
		if hasS {
			an.sNode = &sNode
		}
	}

	switch {
	case !hasO && !hasT && !hasS:
		an.decision = nodeDecisionSkip
		return an, nil

	case !hasO && !hasT && hasS:
		an.kind = DiffKindAdded
		an.emitDiff = true
		an.decision = nodeDecisionAdoptSource
		an.isDir = (sNode.Mode & ModeTypeMask) == ModeDir
		an.descend = an.isDir

		if !tc.targetIsBase && (sNode.Mode&ModeTypeMask) != ModeDir {
			if baseOrig, ok, rErr := tc.base.getInode(sNode.Ino); rErr != nil {
				return nil, rErr
			} else if ok {
				tgtOrig, tOK, tErr := tc.target.getInode(sNode.Ino)
				if tErr != nil {
					return nil, tErr
				}
				if !tOK {
					an.conflict = ConflictHardlink
					an.decision = nodeDecisionConflict
				} else {
					eq, eqErr := tc.nodesEqual(tc.base, &baseOrig, tc.target, &tgtOrig)
					if eqErr != nil {
						return nil, eqErr
					}
					if !eq {
						an.conflict = ConflictHardlink
						an.decision = nodeDecisionConflict
					}
				}
			}
		}
		return an, nil

	case !hasO && hasT && !hasS:
		an.decision = nodeDecisionKeepTarget
		an.isDir = (tNode.Mode & ModeTypeMask) == ModeDir
		an.descend = an.isDir
		return an, nil

	case !hasO && hasT && hasS:
		tIsDir := (tNode.Mode & ModeTypeMask) == ModeDir
		sIsDir := (sNode.Mode & ModeTypeMask) == ModeDir
		if tIsDir != sIsDir {
			an.kind = DiffKindAdded
			an.conflict = ConflictDirFile
			an.emitDiff = true
			an.decision = nodeDecisionConflict
			return an, nil
		}
		if tIsDir && sIsDir {
			an.isDir = true
			an.descend = true
			selfEq, eqErr := tc.dirSelfEqual(tc.target, &tNode, tc.source, &sNode)
			if eqErr != nil {
				return nil, eqErr
			}
			if selfEq {
				an.decision = nodeDecisionFuse
				an.mergedMeta = tNode
				return an, nil
			}
			xt, xErr := tc.target.readAllXattrs(tNode.Ino)
			if xErr != nil {
				return nil, xErr
			}
			xs, xErr := tc.source.readAllXattrs(sNode.Ino)
			if xErr != nil {
				return nil, xErr
			}
			meta, xattrs, cKind := mergeNodeAttributesAndXattrs(nil, &tNode, &sNode, nil, xt, xs, false, false)
			if cKind != ConflictNone {
				an.kind = DiffKindAdded
				an.conflict = cKind
				an.emitDiff = true
				an.decision = nodeDecisionConflict
				return an, nil
			}
			an.decision = nodeDecisionFuse
			an.mergedMeta = meta
			an.mergedXattrs = xattrs
			return an, nil
		}

		if (tNode.Mode & ModeTypeMask) != (sNode.Mode & ModeTypeMask) {
			an.kind = DiffKindAdded
			an.conflict = ConflictType
			an.emitDiff = true
			an.decision = nodeDecisionConflict
			return an, nil
		}
		if (tNode.Nlink > 1 || sNode.Nlink > 1) && tNode.Ino != sNode.Ino {
			an.kind = DiffKindAdded
			an.conflict = ConflictHardlink
			an.emitDiff = true
			an.decision = nodeDecisionConflict
			return an, nil
		}
		eq, eqErr := tc.nodesEqual(tc.target, &tNode, tc.source, &sNode)
		if eqErr != nil {
			return nil, eqErr
		}
		if eq {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		an.kind = DiffKindAdded
		an.conflict = ConflictAddAdd
		an.emitDiff = true
		an.decision = nodeDecisionConflict
		return an, nil

	case hasO && !hasT && !hasS:
		an.decision = nodeDecisionSkip
		return an, nil

	case hasO && hasT && !hasS:
		oIsDir := (oNode.Mode & ModeTypeMask) == ModeDir
		tIsDir := (tNode.Mode & ModeTypeMask) == ModeDir
		if oIsDir {
			if !tIsDir {
				an.kind = DiffKindDeleted
				an.conflict = ConflictDirFile
				an.emitDiff = true
				an.decision = nodeDecisionConflict
				return an, nil
			}
			mod, mErr := tc.isSubtreeModifiedVsBase(oNode.Ino, tc.target, tNode.Ino)
			if mErr != nil {
				return nil, mErr
			}
			an.kind = DiffKindDeleted
			an.emitDiff = true
			if mod {
				an.conflict = ConflictTreeDeleteModify
				an.decision = nodeDecisionConflict
			} else {
				an.decision = nodeDecisionDeleteTarget
			}
			return an, nil
		}
		if tIsDir {
			an.kind = DiffKindDeleted
			an.conflict = ConflictDirFile
			an.emitDiff = true
			an.decision = nodeDecisionConflict
			return an, nil
		}
		tUnchanged := tc.targetIsBase || tc.isNodeUnchangedInView(&oNode, &tNode)
		if !tUnchanged {
			eq, eqErr := tc.nodesEqual(tc.base, &oNode, tc.target, &tNode)
			if eqErr != nil {
				return nil, eqErr
			}
			tUnchanged = eq
		}
		an.kind = DiffKindDeleted
		an.emitDiff = true
		if tUnchanged {
			an.decision = nodeDecisionDeleteTarget
		} else {
			an.conflict = ConflictModifyDelete
			an.decision = nodeDecisionConflict
		}
		return an, nil

	case hasO && !hasT && hasS:
		oIsDir := (oNode.Mode & ModeTypeMask) == ModeDir
		sIsDir := (sNode.Mode & ModeTypeMask) == ModeDir
		if oIsDir {
			if !sIsDir {
				an.kind = DiffKindModified
				an.conflict = ConflictDirFile
				an.emitDiff = true
				an.decision = nodeDecisionConflict
				return an, nil
			}
			mod, mErr := tc.isSubtreeModifiedVsBase(oNode.Ino, tc.source, sNode.Ino)
			if mErr != nil {
				return nil, mErr
			}
			if mod {
				an.kind = DiffKindModified
				an.conflict = ConflictTreeDeleteModify
				an.emitDiff = true
				an.decision = nodeDecisionConflict
				return an, nil
			}
			an.decision = nodeDecisionSkip
			return an, nil
		}
		if sIsDir {
			an.kind = DiffKindModified
			an.conflict = ConflictDirFile
			an.emitDiff = true
			an.decision = nodeDecisionConflict
			return an, nil
		}
		sUnchanged := tc.isNodeUnchangedInView(&oNode, &sNode)
		if !sUnchanged {
			eq, eqErr := tc.nodesEqual(tc.base, &oNode, tc.source, &sNode)
			if eqErr != nil {
				return nil, eqErr
			}
			sUnchanged = eq
		}
		if sUnchanged {
			an.decision = nodeDecisionSkip
			return an, nil
		}
		an.kind = DiffKindModified
		an.conflict = ConflictDeleteModify
		an.emitDiff = true
		an.decision = nodeDecisionConflict
		return an, nil

	default:
		return tc.analyzeAllThreePresent(an, &oNode, &tNode, &sNode)
	}
}

func (tc *treeCompareCtx) analyzeAllThreePresent(an *analyzedNode, oNode, tNode, sNode *Inode) (*analyzedNode, error) {
	oType := oNode.Mode & ModeTypeMask
	tType := tNode.Mode & ModeTypeMask
	sType := sNode.Mode & ModeTypeMask

	tUnchangedHint := tc.targetIsBase || tc.isNodeUnchangedInView(oNode, tNode)
	sUnchangedHint := tc.isNodeUnchangedInView(oNode, sNode)

	if tType == ModeDir && sType == ModeDir && oType == ModeDir {
		an.isDir = true
		an.descend = true
		if tUnchangedHint && sUnchangedHint {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		if sUnchangedHint {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		if tUnchangedHint {
			selfEq, eqErr := tc.dirSelfEqual(tc.base, oNode, tc.source, sNode)
			if eqErr != nil {
				return nil, eqErr
			}
			if selfEq {
				an.decision = nodeDecisionKeepTarget
				return an, nil
			}
			an.kind = DiffKindModified
			an.emitDiff = true
			an.decision = nodeDecisionAdoptSource
			return an, nil
		}

		xo, err := tc.base.readAllXattrs(oNode.Ino)
		if err != nil {
			return nil, err
		}
		xt, err := tc.target.readAllXattrs(tNode.Ino)
		if err != nil {
			return nil, err
		}
		xs, err := tc.source.readAllXattrs(sNode.Ino)
		if err != nil {
			return nil, err
		}
		tSelfEq := metaEqual(oNode, tNode) && xattrsEqual(xo, xt)
		sSelfEq := metaEqual(oNode, sNode) && xattrsEqual(xo, xs)
		if sSelfEq {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		if tSelfEq {
			an.kind = DiffKindModified
			an.emitDiff = true
			an.decision = nodeDecisionAdoptSource
			return an, nil
		}
		if metaEqual(tNode, sNode) && xattrsEqual(xt, xs) {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		meta, xattrs, cKind := mergeNodeAttributesAndXattrs(oNode, tNode, sNode, xo, xt, xs, false, false)
		if cKind != ConflictNone {
			an.kind = DiffKindModified
			an.conflict = cKind
			an.emitDiff = true
			an.decision = nodeDecisionConflict
			return an, nil
		}
		an.kind = DiffKindModified
		an.emitDiff = true
		an.decision = nodeDecisionFuse
		an.mergedMeta = meta
		an.mergedXattrs = xattrs
		return an, nil
	}

	if tType == ModeDir && sType == ModeDir {
		an.isDir = true
		an.descend = true
		selfEq, eqErr := tc.dirSelfEqual(tc.target, tNode, tc.source, sNode)
		if eqErr != nil {
			return nil, eqErr
		}
		if selfEq {
			an.decision = nodeDecisionFuse
			an.mergedMeta = *tNode
			return an, nil
		}
		xt, xErr := tc.target.readAllXattrs(tNode.Ino)
		if xErr != nil {
			return nil, xErr
		}
		xs, xErr := tc.source.readAllXattrs(sNode.Ino)
		if xErr != nil {
			return nil, xErr
		}
		meta, xattrs, cKind := mergeNodeAttributesAndXattrs(nil, tNode, sNode, nil, xt, xs, false, false)
		if cKind != ConflictNone {
			an.kind = DiffKindModified
			an.conflict = cKind
			an.emitDiff = true
			an.decision = nodeDecisionConflict
			return an, nil
		}
		an.kind = DiffKindModified
		an.emitDiff = true
		an.decision = nodeDecisionFuse
		an.mergedMeta = meta
		an.mergedXattrs = xattrs
		return an, nil
	}

	if tType != sType {
		dirOrTypeConflict := func(aType, bType uint32) ConflictKind {
			if (aType == ModeDir) != (bType == ModeDir) {
				return ConflictDirFile
			}
			return ConflictType
		}

		if sType == oType {
			mod, err := tc.branchNodeOrSubtreeModifiedVsBase(oNode, sNode, tc.source, sUnchangedHint)
			if err != nil {
				return nil, err
			}
			if mod {
				an.kind = DiffKindModified
				an.conflict = dirOrTypeConflict(tType, sType)
				an.emitDiff = true
				an.decision = nodeDecisionConflict
				return an, nil
			}
			an.decision = nodeDecisionKeepTarget
			an.isDir = tType == ModeDir
			an.descend = an.isDir
			return an, nil
		}
		if tType == oType {
			mod, err := tc.branchNodeOrSubtreeModifiedVsBase(oNode, tNode, tc.target, tUnchangedHint)
			if err != nil {
				return nil, err
			}
			if mod {
				an.kind = DiffKindModified
				an.conflict = dirOrTypeConflict(tType, sType)
				an.emitDiff = true
				an.decision = nodeDecisionConflict
				return an, nil
			}
			an.kind = DiffKindModified
			an.emitDiff = true
			an.decision = nodeDecisionAdoptSource
			an.isDir = sType == ModeDir
			an.descend = an.isDir
			return an, nil
		}

		an.kind = DiffKindModified
		an.conflict = dirOrTypeConflict(tType, sType)
		an.emitDiff = true
		an.decision = nodeDecisionConflict
		return an, nil
	}

	if sUnchangedHint {
		an.decision = nodeDecisionKeepTarget
		return an, nil
	}
	if tUnchangedHint {
		eq, err := tc.nodesEqual(tc.base, oNode, tc.source, sNode)
		if err != nil {
			return nil, err
		}
		if eq {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		an.kind = DiffKindModified
		an.emitDiff = true
		an.decision = nodeDecisionAdoptSource
		return an, nil
	}

	if oType != tType {
		eq, err := tc.nodesEqual(tc.target, tNode, tc.source, sNode)
		if err != nil {
			return nil, err
		}
		if eq {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		an.kind = DiffKindModified
		an.conflict = ConflictContent
		an.emitDiff = true
		an.decision = nodeDecisionConflict
		return an, nil
	}

	tPayloadEq, err := tc.payloadEqual(tc.base, oNode, tc.target, tNode)
	if err != nil {
		return nil, err
	}
	sPayloadEq, err := tc.payloadEqual(tc.base, oNode, tc.source, sNode)
	if err != nil {
		return nil, err
	}
	tPayloadChanged := !tPayloadEq
	sPayloadChanged := !sPayloadEq

	if tPayloadChanged && sPayloadChanged {
		tsPayloadEq, pErr := tc.payloadEqual(tc.target, tNode, tc.source, sNode)
		if pErr != nil {
			return nil, pErr
		}
		if !tsPayloadEq {
			xo, xErr := tc.base.readAllXattrs(oNode.Ino)
			if xErr != nil {
				return nil, xErr
			}
			xt, xErr := tc.target.readAllXattrs(tNode.Ino)
			if xErr != nil {
				return nil, xErr
			}
			xs, xErr := tc.source.readAllXattrs(sNode.Ino)
			if xErr != nil {
				return nil, xErr
			}
			_, _, cKind := mergeNodeAttributesAndXattrs(oNode, tNode, sNode, xo, xt, xs, tPayloadChanged, sPayloadChanged)
			an.kind = DiffKindModified
			if cKind == ConflictSecurityAttr {
				an.conflict = ConflictSecurityAttr
			} else {
				an.conflict = ConflictContent
			}
			an.emitDiff = true
			an.decision = nodeDecisionConflict
			return an, nil
		}
	}

	xo, err := tc.base.readAllXattrs(oNode.Ino)
	if err != nil {
		return nil, err
	}
	xt, err := tc.target.readAllXattrs(tNode.Ino)
	if err != nil {
		return nil, err
	}
	xs, err := tc.source.readAllXattrs(sNode.Ino)
	if err != nil {
		return nil, err
	}

	mergedMeta, mergedXattrs, cKind := mergeNodeAttributesAndXattrs(oNode, tNode, sNode, xo, xt, xs, tPayloadChanged, sPayloadChanged)
	if cKind != ConflictNone {
		an.kind = DiffKindModified
		an.conflict = cKind
		an.emitDiff = true
		an.decision = nodeDecisionConflict
		return an, nil
	}

	tEqualFinal := metaEqual(tNode, &mergedMeta) && xattrsEqual(xt, mergedXattrs) && !sPayloadChanged
	if !tEqualFinal && sPayloadChanged && tPayloadChanged {
		tEqualFinal = metaEqual(tNode, &mergedMeta) && xattrsEqual(xt, mergedXattrs)
	}
	if tEqualFinal {
		an.decision = nodeDecisionKeepTarget
		return an, nil
	}

	an.kind = DiffKindModified
	an.emitDiff = true
	if !sPayloadChanged && !tPayloadChanged && metaEqual(sNode, &mergedMeta) && xattrsEqual(xs, mergedXattrs) && tNode.Ino == sNode.Ino {
		an.decision = nodeDecisionAdoptSource
		return an, nil
	}
	an.decision = nodeDecisionFuse
	an.mergedMeta = mergedMeta
	an.mergedXattrs = mergedXattrs
	an.payloadFromSrc = sPayloadChanged && !tPayloadChanged
	an.payloadFromTgt = !an.payloadFromSrc
	return an, nil
}

func (tc *treeCompareCtx) branchNodeOrSubtreeModifiedVsBase(oNode, branchNode *Inode, branchView layerView, unchangedHint bool) (bool, error) {
	if (oNode.Mode & ModeTypeMask) == ModeDir {
		return tc.isSubtreeModifiedVsBase(oNode.Ino, branchView, branchNode.Ino)
	}
	if unchangedHint {
		return false, nil
	}
	eq, err := tc.nodesEqual(tc.base, oNode, branchView, branchNode)
	if err != nil {
		return false, err
	}
	return !eq, nil
}

func (tc *treeCompareCtx) buildDirFrame(dirPath []byte, bDirIno, tDirIno, sDirIno uint64) (*dirSliceFrame, error) {
	if tc.disk.onDirVisitedHook != nil {
		visitedIno := tDirIno
		if visitedIno == 0 {
			visitedIno = sDirIno
		}
		if visitedIno == 0 {
			visitedIno = bDirIno
		}
		if visitedIno != 0 {
			tc.disk.onDirVisitedHook(visitedIno)
		}
	}

	var (
		bEntries, tEntries, sEntries []DirEntry
		err                          error
	)
	if bDirIno != 0 {
		bEntries, err = tc.base.readDir(bDirIno)
		if err != nil {
			return nil, err
		}
	}
	targetSame := tc.targetIsBase && bDirIno == tDirIno
	if !targetSame && tDirIno != 0 {
		tEntries, err = tc.target.readDir(tDirIno)
		if err != nil {
			return nil, err
		}
	}
	if sDirIno != 0 {
		sEntries, err = tc.source.readDir(sDirIno)
		if err != nil {
			return nil, err
		}
	}
	return &dirSliceFrame{
		dirPath:    dirPath,
		bEntries:   bEntries,
		tEntries:   tEntries,
		sEntries:   sEntries,
		targetSame: targetSame,
	}, nil
}

func (tc *treeCompareCtx) shouldSkipSubtree(an *analyzedNode) bool {
	if an.oNode != nil && an.tNode != nil && an.sNode != nil {
		if (an.oNode.Mode&ModeTypeMask) == ModeDir &&
			(an.tNode.Mode&ModeTypeMask) == ModeDir &&
			(an.sNode.Mode&ModeTypeMask) == ModeDir &&
			an.oNode.Ino == an.tNode.Ino && an.tNode.Ino == an.sNode.Ino {
			if _, dirty := tc.dirty.dirtyDirs[an.oNode.Ino]; !dirty {
				return true
			}
		}
	}
	if an.oNode == nil && an.tNode != nil && an.sNode == nil && (an.tNode.Mode&ModeTypeMask) == ModeDir {
		if _, hasHL := tc.dirty.targetHardlinkDirs[an.tNode.Ino]; !hasHL {
			return true
		}
	}
	return false
}

func (tc *treeCompareCtx) walkAnalyzedTree(normPrefix []byte, pruneToPrefix bool, fn func(an *analyzedNode) error) error {
	if _, rootDirty := tc.dirty.dirtyDirs[RootIno]; !rootDirty {
		return nil
	}

	rootFrame, err := tc.buildDirFrame(nil, RootIno, RootIno, RootIno)
	if err != nil {
		return err
	}
	h := &dirSliceHeap{}
	heap.Init(h)
	if rootFrame.peekNext() {
		heap.Push(h, rootFrame)
	}

	for h.Len() > 0 {
		if ctxErr := tc.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		frame, okPop := heap.Pop(h).(*dirSliceFrame)
		if !okPop {
			continue
		}

		childPath := frame.nextPath
		bEntry := frame.bEntry
		tEntry := frame.tEntry
		sEntry := frame.sEntry

		if frame.consumeAndPeekNext() {
			heap.Push(h, frame)
		}

		if pruneToPrefix && len(normPrefix) > 0 && !matchPathPrefix(childPath, normPrefix) && !dirCanContainPathPrefix(childPath, normPrefix) {
			continue
		}

		an, anErr := tc.analyzeEntry(childPath, bEntry, tEntry, sEntry)
		if anErr != nil {
			return anErr
		}

		if cbErr := fn(an); cbErr != nil {
			return cbErr
		}

		shouldDescend := an.descend
		if !shouldDescend && len(normPrefix) > 0 && an.decision != nodeDecisionSkip && !matchPathPrefix(childPath, normPrefix) && dirCanContainPathPrefix(childPath, normPrefix) {
			if (an.oNode != nil && (an.oNode.Mode&ModeTypeMask) == ModeDir) ||
				(an.tNode != nil && (an.tNode.Mode&ModeTypeMask) == ModeDir) ||
				(an.sNode != nil && (an.sNode.Mode&ModeTypeMask) == ModeDir) {
				shouldDescend = true
			}
		}

		if shouldDescend && !tc.shouldSkipSubtree(an) && (!pruneToPrefix || dirCanContainPathPrefix(childPath, normPrefix)) {
			var bDirIno, tDirIno, sDirIno uint64
			if an.oNode != nil && (an.oNode.Mode&ModeTypeMask) == ModeDir {
				bDirIno = an.oNode.Ino
			}
			if an.tNode != nil && (an.tNode.Mode&ModeTypeMask) == ModeDir {
				tDirIno = an.tNode.Ino
			}
			if an.sNode != nil && (an.sNode.Mode&ModeTypeMask) == ModeDir {
				sDirIno = an.sNode.Ino
			}
			childFrame, frameErr := tc.buildDirFrame(childPath, bDirIno, tDirIno, sDirIno)
			if frameErr != nil {
				return frameErr
			}
			if childFrame.peekNext() {
				heap.Push(h, childFrame)
			}
		}
	}
	return nil
}

type targetAddedHardlink struct {
	path  []byte
	tNode *Inode
}

type targetHardlinkTracker struct {
	addedByIno    map[uint64][]targetAddedHardlink
	sourceTouched map[uint64]struct{}
}

func newTargetHardlinkTracker() *targetHardlinkTracker {
	return &targetHardlinkTracker{
		addedByIno:    make(map[uint64][]targetAddedHardlink),
		sourceTouched: make(map[uint64]struct{}),
	}
}

func (tr *targetHardlinkTracker) observe(tc *treeCompareCtx, an *analyzedNode) error {
	if tc.targetIsBase {
		return nil
	}
	if an.oNode == nil && an.tNode != nil && an.sNode == nil && (an.tNode.Mode&ModeTypeMask) != ModeDir {
		if _, ok, rErr := tc.base.getInode(an.tNode.Ino); rErr != nil {
			return rErr
		} else if ok {
			tr.addedByIno[an.tNode.Ino] = append(tr.addedByIno[an.tNode.Ino], targetAddedHardlink{
				path:  an.path,
				tNode: an.tNode,
			})
		}
	}
	if an.oNode != nil && (an.oNode.Mode&ModeTypeMask) != ModeDir {
		if an.sNode == nil {
			tr.sourceTouched[an.oNode.Ino] = struct{}{}
		} else if !tc.isNodeUnchangedInView(an.oNode, an.sNode) {
			eq, eqErr := tc.nodesEqual(tc.base, an.oNode, tc.source, an.sNode)
			if eqErr != nil {
				return eqErr
			}
			if !eq {
				tr.sourceTouched[an.oNode.Ino] = struct{}{}
			}
		}
	}
	return nil
}

func (tr *targetHardlinkTracker) hasCandidates() bool {
	return len(tr.addedByIno) > 0
}

func (tr *targetHardlinkTracker) isConflicting(ino uint64) bool {
	if ino == 0 {
		return false
	}
	if _, hasTLink := tr.addedByIno[ino]; !hasTLink {
		return false
	}
	_, hasSTouch := tr.sourceTouched[ino]
	return hasSTouch
}

func (tc *treeCompareCtx) analyzeRootInode(requirePresent bool) (*analyzedNode, error) {
	if _, rootInoDirty := tc.dirty.dirtyInos[RootIno]; !rootInoDirty {
		return nil, nil
	}
	bRoot, bOK, err := tc.base.getInode(RootIno)
	if err != nil {
		return nil, err
	}
	tRoot, tOK, err := tc.target.getInode(RootIno)
	if err != nil {
		return nil, err
	}
	sRoot, sOK, err := tc.source.getInode(RootIno)
	if err != nil {
		return nil, err
	}
	if !bOK || !tOK || !sOK {
		if requirePresent {
			return nil, ErrNotFound
		}
		return nil, nil
	}
	rootAN := &analyzedNode{
		path:  []byte("/"),
		oNode: &bRoot,
		tNode: &tRoot,
		sNode: &sRoot,
	}
	return tc.analyzeAllThreePresent(rootAN, &bRoot, &tRoot, &sRoot)
}

type diffRunParams struct {
	baseCommitID         uint64
	targetCommitID       uint64
	sourceCommitID       uint64
	baseView             layerView
	targetView           layerView
	sourceView           layerView
	targetIsBase         bool
	includeTargetSummary bool
	pathPrefix           []byte
	conflictsOnly        bool
	pageSize             uint32
	pageToken            string
}

func (d *DiskStore) diffViews(ctx context.Context, p diffRunParams) (*DiffResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if p.pageSize > MaxDiffPageSize {
		return nil, ErrInvalid
	}
	pageSize := p.pageSize
	if pageSize == 0 {
		pageSize = DefaultDiffPageSize
	}

	normPrefix, err := normalizePathPrefix(p.pathPrefix)
	if err != nil {
		return nil, err
	}

	var afterPath []byte
	if p.pageToken != "" {
		afterPath, err = decodeDiffPageCursor(p.pageToken)
		if err != nil {
			return nil, err
		}
	}

	dirty, err := discoverDirtySet(p.baseView, p.targetView, p.sourceView, p.targetIsBase)
	if err != nil {
		return nil, err
	}

	res := &DiffResult{
		BaseCommitID:   p.baseCommitID,
		TargetCommitID: p.targetCommitID,
		SourceCommitID: p.sourceCommitID,
	}

	if len(dirty.dirtyDirs) == 0 && len(dirty.dirtyInos) == 0 {
		return res, nil
	}

	tc := newTreeCompareCtx(ctx, d, p.baseView, p.targetView, p.sourceView, p.targetIsBase, dirty)

	var allMatched []*DiffEntry
	hlTracker := newTargetHardlinkTracker()

	if len(normPrefix) == 0 {
		rootAN, rootErr := tc.analyzeRootInode(false)
		if rootErr != nil {
			return nil, rootErr
		}
		if rootAN != nil && rootAN.emitDiff {
			var tSum *DiffNodeSummary
			if p.includeTargetSummary {
				tSum = makeNodeSummary(rootAN.tNode)
			}
			allMatched = append(allMatched, &DiffEntry{
				Path:     rootAN.path,
				Kind:     rootAN.kind,
				Conflict: rootAN.conflict,
				Base:     makeNodeSummary(rootAN.oNode),
				Target:   tSum,
				Source:   makeNodeSummary(rootAN.sNode),
			})
		}
	}

	walkErr := tc.walkAnalyzedTree(normPrefix, p.targetIsBase, func(an *analyzedNode) error {
		if obsErr := hlTracker.observe(tc, an); obsErr != nil {
			return obsErr
		}

		if !an.emitDiff {
			return nil
		}
		if len(normPrefix) > 0 && !matchPathPrefix(an.path, normPrefix) {
			return nil
		}

		var tSum *DiffNodeSummary
		if p.includeTargetSummary {
			tSum = makeNodeSummary(an.tNode)
		}
		allMatched = append(allMatched, &DiffEntry{
			Path:     an.path,
			Kind:     an.kind,
			Conflict: an.conflict,
			Base:     makeNodeSummary(an.oNode),
			Target:   tSum,
			Source:   makeNodeSummary(an.sNode),
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	if !p.targetIsBase && hlTracker.hasCandidates() {
		for baseIno, items := range hlTracker.addedByIno {
			if !hlTracker.isConflicting(baseIno) {
				continue
			}
			for _, e := range allMatched {
				if e.Base != nil && e.Base.Ino == baseIno && e.Conflict == ConflictNone {
					e.Conflict = ConflictHardlink
				}
			}
			for _, item := range items {
				if len(normPrefix) > 0 && !matchPathPrefix(item.path, normPrefix) {
					continue
				}
				var tSum *DiffNodeSummary
				if p.includeTargetSummary {
					tSum = makeNodeSummary(item.tNode)
				}
				allMatched = append(allMatched, &DiffEntry{
					Path:     item.path,
					Kind:     DiffKindAdded,
					Conflict: ConflictHardlink,
					Target:   tSum,
				})
			}
		}
	}

	sort.Slice(allMatched, func(i, j int) bool {
		return compareDiffPath(allMatched[i].Path, allMatched[j].Path) < 0
	})

	var hasMore bool
	for _, e := range allMatched {
		switch e.Kind {
		case DiffKindAdded:
			res.Summary.Added++
		case DiffKindModified:
			res.Summary.Modified++
		case DiffKindDeleted:
			res.Summary.Deleted++
		}
		if e.Conflict != ConflictNone {
			res.Summary.Conflicts++
		}
		if p.conflictsOnly && e.Conflict == ConflictNone {
			continue
		}
		if len(afterPath) > 0 && compareDiffPath(e.Path, afterPath) <= 0 {
			continue
		}
		if uint32(len(res.Entries)) < pageSize {
			res.Entries = append(res.Entries, *e)
		} else {
			hasMore = true
		}
	}
	if hasMore && len(res.Entries) > 0 {
		res.NextPageToken = encodeDiffPageCursor(res.Entries[len(res.Entries)-1].Path)
	}
	return res, nil
}

func (d *DiskStore) Diff(ctx context.Context, opts DiffOptions) (*DiffResult, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if err := validateBranchName(opts.SourceBranch); err != nil {
		return nil, err
	}
	if err := validateBranchName(opts.TargetBranch); err != nil {
		return nil, err
	}

	dk := d.keys
	snap, snapGen := d.newReadSnapshot()
	defer func() { _ = snap.Close() }()

	srcBranch, err := readBranchMeta(snap, dk, opts.SourceBranch)
	if err != nil {
		return nil, err
	}
	tgtBranch, err := readBranchMeta(snap, dk, opts.TargetBranch)
	if err != nil {
		return nil, err
	}

	lca, err := findLCA(snap, dk, srcBranch.HeadCommitID, tgtBranch.HeadCommitID)
	if err != nil {
		return nil, err
	}

	_, baseLayers, err := d.resolveCommitLayersWithGen(snap, lca, snapGen)
	if err != nil {
		return nil, err
	}
	targetIsBase := tgtBranch.HeadCommitID == lca
	targetLayers := baseLayers
	if !targetIsBase {
		_, targetLayers, err = d.resolveCommitLayersWithGen(snap, tgtBranch.HeadCommitID, snapGen)
		if err != nil {
			return nil, err
		}
	}
	_, sourceLayers, err := d.resolveCommitLayersWithGen(snap, srcBranch.HeadCommitID, snapGen)
	if err != nil {
		return nil, err
	}

	return d.diffViews(ctx, diffRunParams{
		baseCommitID:         lca,
		targetCommitID:       tgtBranch.HeadCommitID,
		sourceCommitID:       srcBranch.HeadCommitID,
		baseView:             layerView{r: snap, keys: dk, layers: baseLayers},
		targetView:           layerView{r: snap, keys: dk, layers: targetLayers},
		sourceView:           layerView{r: snap, keys: dk, layers: sourceLayers},
		targetIsBase:         targetIsBase,
		includeTargetSummary: true,
		pathPrefix:           opts.PathPrefix,
		conflictsOnly:        opts.ConflictsOnly,
		pageSize:             opts.PageSize,
		pageToken:            opts.PageToken,
	})
}

func (d *DiskStore) DiffCommits(ctx context.Context, opts DiffCommitsOptions) (*DiffResult, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if opts.BaseCommitID == 0 || opts.SourceCommitID == 0 {
		return nil, ErrInvalid
	}

	dk := d.keys
	snap, snapGen := d.newReadSnapshot()
	defer func() { _ = snap.Close() }()

	_, baseLayers, err := d.resolveCommitLayersWithGen(snap, opts.BaseCommitID, snapGen)
	if err != nil {
		return nil, err
	}
	_, sourceLayers, err := d.resolveCommitLayersWithGen(snap, opts.SourceCommitID, snapGen)
	if err != nil {
		return nil, err
	}

	baseView := layerView{r: snap, keys: dk, layers: baseLayers}
	return d.diffViews(ctx, diffRunParams{
		baseCommitID:         opts.BaseCommitID,
		targetCommitID:       opts.BaseCommitID,
		sourceCommitID:       opts.SourceCommitID,
		baseView:             baseView,
		targetView:           baseView,
		sourceView:           layerView{r: snap, keys: dk, layers: sourceLayers},
		targetIsBase:         true,
		includeTargetSummary: false,
		pathPrefix:           opts.PathPrefix,
		conflictsOnly:        opts.ConflictsOnly,
		pageSize:             opts.PageSize,
		pageToken:            opts.PageToken,
	})
}

func (d *DiskStore) DiffWork(ctx context.Context, branch string, opts DiffWorkOptions) (*DiffResult, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if err := validateBranchName(branch); err != nil {
		return nil, err
	}

	dk := d.keys
	snap, snapGen := d.newReadSnapshot()
	defer func() { _ = snap.Close() }()

	bm, err := readBranchMeta(snap, dk, branch)
	if err != nil {
		return nil, err
	}
	_, headLayers, err := d.resolveCommitLayersWithGen(snap, bm.HeadCommitID, snapGen)
	if err != nil {
		return nil, err
	}

	sourceLayers := headLayers
	if bm.WorkLayerID != 0 {
		sourceLayers = make([]uint64, 0, len(headLayers)+1)
		sourceLayers = append(sourceLayers, bm.WorkLayerID)
		sourceLayers = append(sourceLayers, headLayers...)
	}

	baseView := layerView{r: snap, keys: dk, layers: headLayers}
	return d.diffViews(ctx, diffRunParams{
		baseCommitID:         bm.HeadCommitID,
		targetCommitID:       bm.HeadCommitID,
		sourceCommitID:       0,
		baseView:             baseView,
		targetView:           baseView,
		sourceView:           layerView{r: snap, keys: dk, layers: sourceLayers},
		targetIsBase:         true,
		includeTargetSummary: false,
		pathPrefix:           opts.PathPrefix,
		conflictsOnly:        opts.ConflictsOnly,
		pageSize:             opts.PageSize,
		pageToken:            opts.PageToken,
	})
}
