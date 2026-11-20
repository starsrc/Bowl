package store

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

const (
	systemQuotaXattrName   = "system.bowl.max_bytes"
	securityXattrPrefixStr = "security."
	diffCursorVersion      = 1
)

type dirChildEntry struct {
	name  []byte
	entry Entry
}

type sideIter struct {
	iter   *pebble.Iterator
	prefix []byte
	valid  bool
	cur    dirChildEntry
}

func newSideIter(actor *StoreActor, dirIno uint64) (*sideIter, error) {
	if actor == nil {
		return nil, nil
	}
	iter, err := actor.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	prefix := EntryPrefix(dirIno)
	si := &sideIter{
		iter:   iter,
		prefix: prefix,
	}
	if iter.SeekGE(prefix) {
		if err := si.decodeCurrent(); err != nil {
			_ = iter.Close()
			return nil, err
		}
	} else if err := iter.Error(); err != nil {
		_ = iter.Close()
		return nil, err
	}
	return si, nil
}

func (si *sideIter) decodeCurrent() error {
	if !si.iter.Valid() || !bytes.HasPrefix(si.iter.Key(), si.prefix) {
		si.valid = false
		si.cur = dirChildEntry{}
		return si.iter.Error()
	}
	_, name, err := DecodeEntryKey(si.iter.Key())
	if err != nil {
		return err
	}
	entry, err := DecodeEntry(si.iter.Value())
	if err != nil {
		return err
	}
	si.cur = dirChildEntry{name: name, entry: entry}
	si.valid = true
	return nil
}

func (si *sideIter) advance() error {
	if si == nil || !si.valid {
		return nil
	}
	if !si.iter.Next() {
		si.valid = false
		si.cur = dirChildEntry{}
		return si.iter.Error()
	}
	return si.decodeCurrent()
}

func (si *sideIter) close() error {
	if si == nil || si.iter == nil {
		return nil
	}
	err := si.iter.Close()
	si.iter = nil
	si.valid = false
	return err
}

type dirMergeFrame struct {
	dirPath    []byte
	baseIt     *sideIter
	targetIt   *sideIter
	sourceIt   *sideIter
	targetSame bool

	nextName []byte
	nextPath []byte
	bEntry   *Entry
	tEntry   *Entry
	sEntry   *Entry
}

func (f *dirMergeFrame) close() error {
	var errs []error
	if f.baseIt != nil {
		errs = append(errs, f.baseIt.close())
	}
	if f.targetIt != nil {
		errs = append(errs, f.targetIt.close())
	}
	if f.sourceIt != nil {
		errs = append(errs, f.sourceIt.close())
	}
	return errors.Join(errs...)
}

func (f *dirMergeFrame) peekNext() (bool, error) {
	var minName []byte
	consider := func(si *sideIter) {
		if si != nil && si.valid {
			if minName == nil || bytes.Compare(si.cur.name, minName) < 0 {
				minName = si.cur.name
			}
		}
	}
	consider(f.baseIt)
	if !f.targetSame {
		consider(f.targetIt)
	}
	consider(f.sourceIt)
	if minName == nil {
		f.nextName = nil
		f.nextPath = nil
		f.bEntry = nil
		f.tEntry = nil
		f.sEntry = nil
		return false, nil
	}
	f.nextName = append([]byte(nil), minName...)
	f.nextPath = joinRelPath(f.dirPath, minName)

	f.bEntry = nil
	if f.baseIt != nil && f.baseIt.valid && bytes.Equal(f.baseIt.cur.name, minName) {
		e := f.baseIt.cur.entry
		f.bEntry = &e
	}
	f.tEntry = nil
	if f.targetSame {
		if f.bEntry != nil {
			e := *f.bEntry
			f.tEntry = &e
		}
	} else if f.targetIt != nil && f.targetIt.valid && bytes.Equal(f.targetIt.cur.name, minName) {
		e := f.targetIt.cur.entry
		f.tEntry = &e
	}
	f.sEntry = nil
	if f.sourceIt != nil && f.sourceIt.valid && bytes.Equal(f.sourceIt.cur.name, minName) {
		e := f.sourceIt.cur.entry
		f.sEntry = &e
	}
	return true, nil
}

func (f *dirMergeFrame) consumeAndPeekNext() (bool, error) {
	if f.bEntry != nil && f.baseIt != nil {
		if err := f.baseIt.advance(); err != nil {
			return false, err
		}
	}
	if !f.targetSame && f.tEntry != nil && f.targetIt != nil {
		if err := f.targetIt.advance(); err != nil {
			return false, err
		}
	}
	if f.sEntry != nil && f.sourceIt != nil {
		if err := f.sourceIt.advance(); err != nil {
			return false, err
		}
	}
	return f.peekNext()
}

type dirFrameHeap []*dirMergeFrame

func (h dirFrameHeap) Len() int           { return len(h) }
func (h dirFrameHeap) Less(i, j int) bool { return bytes.Compare(h[i].nextPath, h[j].nextPath) < 0 }
func (h dirFrameHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *dirFrameHeap) Push(x any) {
	if f, ok := x.(*dirMergeFrame); ok {
		*h = append(*h, f)
	}
}
func (h *dirFrameHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

func joinRelPath(parent, name []byte) []byte {
	if len(parent) == 0 {
		return append([]byte(nil), name...)
	}
	out := make([]byte, len(parent)+1+len(name))
	copy(out, parent)
	out[len(parent)] = '/'
	copy(out[len(parent)+1:], name)
	return out
}

func normalizePathPrefix(raw []byte) ([]byte, error) {
	if bytes.IndexByte(raw, 0) >= 0 {
		return nil, ErrInvalid
	}
	trimmed := bytes.TrimLeft(raw, "/")
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte(".")) {
		return nil, nil
	}
	return append([]byte(nil), trimmed...), nil
}

func normalizeStrategyPath(raw string) (string, error) {
	if strings.ContainsRune(raw, 0) {
		return "", ErrInvalid
	}
	trimmed := strings.Trim(raw, "/")
	if trimmed == "." {
		return "", nil
	}
	return path.Clean("/" + trimmed)[1:], nil
}

func isRootDiffPath(p []byte) bool {
	return len(p) == 1 && p[0] == '/'
}

func compareDiffPath(a, b []byte) int {
	aRoot := isRootDiffPath(a)
	bRoot := isRootDiffPath(b)
	switch {
	case aRoot && bRoot:
		return 0
	case aRoot:
		return -1
	case bRoot:
		return 1
	default:
		return bytes.Compare(a, b)
	}
}

func matchPathPrefix(relPath, normPrefix []byte) bool {
	if len(normPrefix) == 0 {
		return true
	}
	if !bytes.HasPrefix(relPath, normPrefix) {
		return false
	}
	if normPrefix[len(normPrefix)-1] == '/' {
		return true
	}
	return len(relPath) == len(normPrefix) || relPath[len(normPrefix)] == '/'
}

func dirCanContainPathPrefix(dirPath, normPrefix []byte) bool {
	if len(normPrefix) == 0 || matchPathPrefix(dirPath, normPrefix) {
		return true
	}
	if len(normPrefix) <= len(dirPath) {
		return false
	}
	return bytes.HasPrefix(normPrefix, dirPath) && normPrefix[len(dirPath)] == '/'
}

type diffPageCursor struct {
	Version int    `json:"v"`
	Path    []byte `json:"p"`
}

func encodeDiffPageCursor(lastPath []byte) string {
	raw, err := json.Marshal(diffPageCursor{
		Version: diffCursorVersion,
		Path:    lastPath,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeDiffPageCursor(token string) ([]byte, error) {
	if strings.ContainsRune(token, 0) {
		return nil, ErrInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrInvalid
	}
	var cursor diffPageCursor
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cursor); err != nil || dec.More() || cursor.Version != diffCursorVersion || len(cursor.Path) == 0 {
		return nil, ErrInvalid
	}
	if bytes.IndexByte(cursor.Path, 0) >= 0 {
		return nil, ErrInvalid
	}
	return cursor.Path, nil
}

type treeCompareCtx struct {
	ctx           context.Context
	base          *StoreActor
	target        *StoreActor
	source        *StoreActor
	targetIsBase  bool
	samePackCache map[[2]*StoreActor]map[uint64]bool
	bufA          []byte
	bufB          []byte
}

func newTreeCompareCtx(ctx context.Context, base, target, source *StoreActor, targetIsBase bool) *treeCompareCtx {
	if ctx == nil {
		ctx = context.Background()
	}
	return &treeCompareCtx{
		ctx:           ctx,
		base:          base,
		target:        target,
		source:        source,
		targetIsBase:  targetIsBase,
		samePackCache: make(map[[2]*StoreActor]map[uint64]bool),
		bufA:          make([]byte, BlobSize),
		bufB:          make([]byte, BlobSize),
	}
}

func (tc *treeCompareCtx) samePackFile(a, b *StoreActor, packID uint64) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil || a.blobs == nil || b.blobs == nil {
		return false
	}
	pair := [2]*StoreActor{a, b}
	m, ok := tc.samePackCache[pair]
	if !ok {
		m = make(map[uint64]bool)
		tc.samePackCache[pair] = m
	}
	if cached, hit := m[packID]; hit {
		return cached
	}
	infoA, errA := os.Stat(packFilePath(a.blobs.dir, packID))
	infoB, errB := os.Stat(packFilePath(b.blobs.dir, packID))
	same := errA == nil && errB == nil && os.SameFile(infoA, infoB)
	m[packID] = same
	return same
}

func (tc *treeCompareCtx) readLiveInode(actor *StoreActor, ino uint64) (Inode, bool, error) {
	if actor == nil {
		return Inode{}, false, nil
	}
	inode, err := actor.readInodeFromDB(tc.ctx, ino)
	if errors.Is(err, ErrNotFound) {
		return Inode{}, false, nil
	}
	if err != nil {
		return Inode{}, false, err
	}
	if err := actor.checkInodeLiveFromDB(tc.ctx, &inode); err != nil {
		if errors.Is(err, ErrNotFound) {
			return Inode{}, false, nil
		}
		return Inode{}, false, err
	}
	return inode, true, nil
}

func readAllXattrs(actor *StoreActor, ino uint64) (map[string][]byte, error) {
	iter, err := actor.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	out := make(map[string][]byte)
	prefix := XattrPrefix(ino)
	for valid := iter.SeekGE(prefix); valid && bytes.HasPrefix(iter.Key(), prefix); valid = iter.Next() {
		keyIno, name, decErr := DecodeXattrKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		if keyIno != ino {
			break
		}
		out[string(name)] = append([]byte(nil), iter.Value()...)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
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

func metaEqual(a, b *Inode) bool {
	return (a.Mode&PermMask) == (b.Mode&PermMask) &&
		a.UID == b.UID &&
		a.GID == b.GID &&
		a.Flags == b.Flags
}

func isUnchangedVsBase(branch, base *Inode) bool {
	return branch != nil && base != nil && branch.Ino == base.Ino && branch.WriteSeq == base.WriteSeq
}

func (tc *treeCompareCtx) payloadEqual(actorA *StoreActor, a *Inode, actorB *StoreActor, b *Inode) (bool, error) {
	if (a.Mode & S_IFMT) != (b.Mode & S_IFMT) {
		return false, nil
	}
	switch a.Mode & S_IFMT {
	case S_IFDIR, S_IFIFO, S_IFSOCK:
		return true, nil
	case S_IFCHR, S_IFBLK:
		return a.Rdev == b.Rdev, nil
	case S_IFLNK:
		return bytes.Equal(a.SymlinkTarget, b.SymlinkTarget), nil
	case S_IFREG:
		if a.Size != b.Size {
			return false, nil
		}
		if a.Size == 0 {
			return true, nil
		}
		return tc.regDataEqual(actorA, a, actorB, b)
	default:
		return false, nil
	}
}

type dataBlockCursor struct {
	iter   *pebble.Iterator
	prefix []byte
	ino    uint64
	valid  bool
	idx    uint64
	ref    BlobRef
}

func newDataBlockCursor(actor *StoreActor, ino uint64) (*dataBlockCursor, error) {
	iter, err := actor.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	prefix := DataPrefix(ino)
	dc := &dataBlockCursor{iter: iter, prefix: prefix, ino: ino}
	var startKey [DataKeyLen]byte
	putDataKey(startKey[:], ino, 0)
	if iter.SeekGE(startKey[:]) {
		if err := dc.decode(); err != nil {
			_ = iter.Close()
			return nil, err
		}
	} else if err := iter.Error(); err != nil {
		_ = iter.Close()
		return nil, err
	}
	return dc, nil
}

func (dc *dataBlockCursor) decode() error {
	if !dc.iter.Valid() || !bytes.HasPrefix(dc.iter.Key(), dc.prefix) {
		dc.valid = false
		return dc.iter.Error()
	}
	keyIno, idx, err := DecodeDataKey(dc.iter.Key())
	if err != nil {
		return err
	}
	if keyIno != dc.ino {
		dc.valid = false
		return dc.iter.Error()
	}
	ref, err := DecodeBlobRef(dc.iter.Value())
	if err != nil {
		return err
	}
	dc.idx = idx
	dc.ref = ref
	dc.valid = true
	return nil
}

func (dc *dataBlockCursor) advance() error {
	if !dc.valid {
		return nil
	}
	if !dc.iter.Next() {
		dc.valid = false
		return dc.iter.Error()
	}
	return dc.decode()
}

func (dc *dataBlockCursor) close() error {
	if dc.iter == nil {
		return nil
	}
	err := dc.iter.Close()
	dc.iter = nil
	return err
}

func (tc *treeCompareCtx) regDataEqual(actorA *StoreActor, a *Inode, actorB *StoreActor, b *Inode) (bool, error) {
	curA, err := newDataBlockCursor(actorA, a.Ino)
	if err != nil {
		return false, err
	}
	defer func() { _ = curA.close() }()

	curB, err := newDataBlockCursor(actorB, b.Ino)
	if err != nil {
		return false, err
	}
	defer func() { _ = curB.close() }()

	for curA.valid || curB.valid {
		switch {
		case curA.valid && (!curB.valid || curA.idx < curB.idx):

			eq, err := tc.singleBlockIsZero(actorA, a.Size, curA.idx, curA.ref)
			if err != nil || !eq {
				return false, err
			}
			if err := curA.advance(); err != nil {
				return false, err
			}
		case curB.valid && (!curA.valid || curB.idx < curA.idx):
			eq, err := tc.singleBlockIsZero(actorB, b.Size, curB.idx, curB.ref)
			if err != nil || !eq {
				return false, err
			}
			if err := curB.advance(); err != nil {
				return false, err
			}
		default:

			if curA.ref == curB.ref && tc.samePackFile(actorA, actorB, curA.ref.PackID) {
				if err := curA.advance(); err != nil {
					return false, err
				}
				if err := curB.advance(); err != nil {
					return false, err
				}
				continue
			}
			eq, err := tc.blocksEqual(actorA, a.Size, curA.idx, curA.ref, actorB, curB.ref)
			if err != nil || !eq {
				return false, err
			}
			if err := curA.advance(); err != nil {
				return false, err
			}
			if err := curB.advance(); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func blockLogicalLen(fileSize, blockIdx uint64) uint64 {
	start := blockIdx * BlobSize
	if start >= fileSize {
		return 0
	}
	return min(uint64(BlobSize), fileSize-start)
}

func (tc *treeCompareCtx) singleBlockIsZero(actor *StoreActor, fileSize, blockIdx uint64, ref BlobRef) (bool, error) {
	logicalLen := blockLogicalLen(fileSize, blockIdx)
	if logicalLen == 0 {
		return true, nil
	}
	readLen := min(uint64(ref.Length), logicalLen)
	if readLen == 0 {
		return true, nil
	}
	buf := tc.bufA[:readLen]
	if err := actor.blobs.readAtContext(tc.ctx, ref, 0, buf); err != nil {
		return false, err
	}
	return isAllZero(buf), nil
}

func (tc *treeCompareCtx) blocksEqual(actorA *StoreActor, fileSize, blockIdx uint64, refA BlobRef, actorB *StoreActor, refB BlobRef) (bool, error) {
	logicalLen := blockLogicalLen(fileSize, blockIdx)
	if logicalLen == 0 {
		return true, nil
	}
	lenA := min(uint64(refA.Length), logicalLen)
	lenB := min(uint64(refB.Length), logicalLen)
	if err := actorA.blobs.readAtContext(tc.ctx, refA, 0, tc.bufA[:lenA]); err != nil {
		return false, err
	}
	if err := actorB.blobs.readAtContext(tc.ctx, refB, 0, tc.bufB[:lenB]); err != nil {
		return false, err
	}
	common := min(lenA, lenB)
	if !bytes.Equal(tc.bufA[:common], tc.bufB[:common]) {
		return false, nil
	}
	if lenA > common && !isAllZero(tc.bufA[common:lenA]) {
		return false, nil
	}
	if lenB > common && !isAllZero(tc.bufB[common:lenB]) {
		return false, nil
	}
	return true, nil
}

func (tc *treeCompareCtx) nodesEqual(actorA *StoreActor, a *Inode, actorB *StoreActor, b *Inode) (bool, error) {
	if (a.Mode & S_IFMT) != (b.Mode & S_IFMT) {
		return false, nil
	}
	if !metaEqual(a, b) {
		return false, nil
	}
	if (a.Mode&S_IFMT) == S_IFREG && a.Size != b.Size {
		return false, nil
	}
	xa, err := readAllXattrs(actorA, a.Ino)
	if err != nil {
		return false, err
	}
	xb, err := readAllXattrs(actorB, b.Ino)
	if err != nil {
		return false, err
	}
	if !xattrsEqual(xa, xb) {
		return false, nil
	}
	return tc.payloadEqual(actorA, a, actorB, b)
}

func (tc *treeCompareCtx) dirSelfEqual(actorA *StoreActor, a *Inode, actorB *StoreActor, b *Inode) (bool, error) {
	if !metaEqual(a, b) {
		return false, nil
	}
	xa, err := readAllXattrs(actorA, a.Ino)
	if err != nil {
		return false, err
	}
	xb, err := readAllXattrs(actorB, b.Ino)
	if err != nil {
		return false, err
	}
	return xattrsEqual(xa, xb), nil
}

func (tc *treeCompareCtx) isSubtreeModifiedVsBase(baseIno uint64, branchActor *StoreActor, branchIno uint64) (bool, error) {
	bNode, bOK, err := tc.readLiveInode(tc.base, baseIno)
	if err != nil || !bOK {
		return true, err
	}
	xNode, xOK, err := tc.readLiveInode(branchActor, branchIno)
	if err != nil || !xOK {
		return true, err
	}
	if (xNode.Mode & S_IFMT) != (bNode.Mode & S_IFMT) {
		return true, nil
	}
	if !isUnchangedVsBase(&xNode, &bNode) {
		selfEq, eqErr := tc.dirSelfEqual(tc.base, &bNode, branchActor, &xNode)
		if eqErr != nil || !selfEq {
			return true, eqErr
		}
	}

	bIt, err := newSideIter(tc.base, baseIno)
	if err != nil {
		return false, err
	}
	defer func() { _ = bIt.close() }()

	xIt, err := newSideIter(branchActor, branchIno)
	if err != nil {
		return false, err
	}
	defer func() { _ = xIt.close() }()

	for bIt.valid || xIt.valid {
		switch {
		case xIt.valid && (!bIt.valid || bytes.Compare(xIt.cur.name, bIt.cur.name) < 0):

			return true, nil
		case bIt.valid && (!xIt.valid || bytes.Compare(bIt.cur.name, xIt.cur.name) < 0):

			if err := bIt.advance(); err != nil {
				return false, err
			}
		default:
			bc := bIt.cur.entry
			xc := xIt.cur.entry
			if (bc.Mode & S_IFMT) != (xc.Mode & S_IFMT) {
				return true, nil
			}
			if (bc.Mode & S_IFMT) == S_IFDIR {
				subMod, subErr := tc.isSubtreeModifiedVsBase(bc.Ino, branchActor, xc.Ino)
				if subErr != nil || subMod {
					return subMod, subErr
				}
			} else {
				bn, bnOK, rErr := tc.readLiveInode(tc.base, bc.Ino)
				if rErr != nil || !bnOK {
					return true, rErr
				}
				xn, xnOK, rErr := tc.readLiveInode(branchActor, xc.Ino)
				if rErr != nil || !xnOK {
					return true, rErr
				}
				if !isUnchangedVsBase(&xn, &bn) {
					eq, eqErr := tc.nodesEqual(tc.base, &bn, branchActor, &xn)
					if eqErr != nil || !eq {
						return true, eqErr
					}
				}
			}
			if err := bIt.advance(); err != nil {
				return false, err
			}
			if err := xIt.advance(); err != nil {
				return false, err
			}
		}
	}
	return false, nil
}

func makeNodeSummary(n *Inode) *DiffNodeSummary {
	if n == nil {
		return nil
	}
	return &DiffNodeSummary{
		Ino:  n.Ino,
		Mode: n.Mode,
		Size: n.Size,
	}
}

func isSecurityMetaChanged(a, b *Inode) bool {
	if a.UID != b.UID || a.GID != b.GID {
		return true
	}
	const secBits = S_ISUID | S_ISGID
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

func mergeNodeAttributesAndXattrs(o, t, s *Inode, xo, xt, xs map[string][]byte, tPayloadChanged, sPayloadChanged bool) (mergedMeta Inode, mergedXattrs map[string][]byte, conflict ConflictKind) {
	mergedMeta = *t

	if o != nil {
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
		permO := o.Mode & PermMask
		permT := t.Mode & PermMask
		permS := s.Mode & PermMask
		mergedPerm := mergeU32(permO, permT, permS)
		mergedMeta.Mode = (t.Mode & S_IFMT) | mergedPerm
		mergedMeta.UID = mergeU32(o.UID, t.UID, s.UID)
		mergedMeta.GID = mergeU32(o.GID, t.GID, s.GID)
		mergedMeta.Flags = mergeU32(o.Flags, t.Flags, s.Flags)
		if attrConflict {
			return mergedMeta, nil, ConflictAttr
		}
	}

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
			if strings.HasPrefix(k, string(userXattrPrefix)) && !isAllowedUserXattrFileType(mergedMeta.Mode) {
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

func classifyXattrKeyConflict(key string) ConflictKind {
	if strings.HasPrefix(key, securityXattrPrefixStr) {
		return ConflictSecurityAttr
	}
	if key == systemQuotaXattrName {
		return ConflictXattrQuota
	}
	return ConflictXattr
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
		oNode, hasO, err = tc.readLiveInode(tc.base, bEntry.Ino)
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
			tNode, hasT, err = tc.readLiveInode(tc.target, tEntry.Ino)
			if err != nil {
				return nil, err
			}
		}
		if hasT {
			an.tNode = &tNode
		}
	}
	if sEntry != nil {
		sNode, hasS, err = tc.readLiveInode(tc.source, sEntry.Ino)
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
		an.isDir = (sNode.Mode & S_IFMT) == S_IFDIR
		an.descend = an.isDir

		if !tc.targetIsBase && (sNode.Mode&S_IFMT) != S_IFDIR {
			if baseOrig, ok, rErr := tc.readLiveInode(tc.base, sNode.Ino); rErr != nil {
				return nil, rErr
			} else if ok {
				tgtOrig, tOK, tErr := tc.readLiveInode(tc.target, sNode.Ino)
				if tErr != nil {
					return nil, tErr
				}
				if !tOK {
					an.conflict = ConflictHardlink
					an.decision = nodeDecisionConflict
				} else if !isUnchangedVsBase(&tgtOrig, &baseOrig) {
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
		an.isDir = (tNode.Mode & S_IFMT) == S_IFDIR
		an.descend = an.isDir
		return an, nil

	case !hasO && hasT && hasS:

		tIsDir := (tNode.Mode & S_IFMT) == S_IFDIR
		sIsDir := (sNode.Mode & S_IFMT) == S_IFDIR
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
			xt, xErr := readAllXattrs(tc.target, tNode.Ino)
			if xErr != nil {
				return nil, xErr
			}
			xs, xErr := readAllXattrs(tc.source, sNode.Ino)
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

		if (tNode.Mode & S_IFMT) != (sNode.Mode & S_IFMT) {
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

		oIsDir := (oNode.Mode & S_IFMT) == S_IFDIR
		tIsDir := (tNode.Mode & S_IFMT) == S_IFDIR
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
		tUnchanged := isUnchangedVsBase(&tNode, &oNode)
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

		oIsDir := (oNode.Mode & S_IFMT) == S_IFDIR
		sIsDir := (sNode.Mode & S_IFMT) == S_IFDIR
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
		sUnchanged := isUnchangedVsBase(&sNode, &oNode)
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
	oType := oNode.Mode & S_IFMT
	tType := tNode.Mode & S_IFMT
	sType := sNode.Mode & S_IFMT

	tUnchanged := isUnchangedVsBase(tNode, oNode)
	sUnchanged := isUnchangedVsBase(sNode, oNode)

	if tType == S_IFDIR && sType == S_IFDIR && oType == S_IFDIR {
		an.isDir = true
		an.descend = true
		if tUnchanged && sUnchanged {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		if sUnchanged {
			an.decision = nodeDecisionKeepTarget
			return an, nil
		}
		if tUnchanged {
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

		xo, err := readAllXattrs(tc.base, oNode.Ino)
		if err != nil {
			return nil, err
		}
		xt, err := readAllXattrs(tc.target, tNode.Ino)
		if err != nil {
			return nil, err
		}
		xs, err := readAllXattrs(tc.source, sNode.Ino)
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

	if tType == S_IFDIR && sType == S_IFDIR {

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
		xt, xErr := readAllXattrs(tc.target, tNode.Ino)
		if xErr != nil {
			return nil, xErr
		}
		xs, xErr := readAllXattrs(tc.source, sNode.Ino)
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
			if (aType == S_IFDIR) != (bType == S_IFDIR) {
				return ConflictDirFile
			}
			return ConflictType
		}

		if sType == oType {
			mod, err := tc.branchNodeOrSubtreeModifiedVsBase(oNode, sNode, tc.source, sUnchanged)
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
			an.isDir = tType == S_IFDIR
			an.descend = an.isDir
			return an, nil
		}
		if tType == oType {
			mod, err := tc.branchNodeOrSubtreeModifiedVsBase(oNode, tNode, tc.target, tUnchanged)
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
			an.isDir = sType == S_IFDIR
			an.descend = an.isDir
			return an, nil
		}

		an.kind = DiffKindModified
		an.conflict = dirOrTypeConflict(tType, sType)
		an.emitDiff = true
		an.decision = nodeDecisionConflict
		return an, nil
	}

	if sUnchanged {
		an.decision = nodeDecisionKeepTarget
		return an, nil
	}
	if tUnchanged {
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

			xo, xErr := readAllXattrs(tc.base, oNode.Ino)
			if xErr != nil {
				return nil, xErr
			}
			xt, xErr := readAllXattrs(tc.target, tNode.Ino)
			if xErr != nil {
				return nil, xErr
			}
			xs, xErr := readAllXattrs(tc.source, sNode.Ino)
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

	xo, err := readAllXattrs(tc.base, oNode.Ino)
	if err != nil {
		return nil, err
	}
	xt, err := readAllXattrs(tc.target, tNode.Ino)
	if err != nil {
		return nil, err
	}
	xs, err := readAllXattrs(tc.source, sNode.Ino)
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

func (tc *treeCompareCtx) branchNodeOrSubtreeModifiedVsBase(oNode, branchNode *Inode, branchActor *StoreActor, unchangedHint bool) (bool, error) {
	if (oNode.Mode & S_IFMT) == S_IFDIR {
		return tc.isSubtreeModifiedVsBase(oNode.Ino, branchActor, branchNode.Ino)
	}
	if unchangedHint {
		return false, nil
	}
	eq, err := tc.nodesEqual(tc.base, oNode, branchActor, branchNode)
	if err != nil {
		return false, err
	}
	return !eq, nil
}

func (tc *treeCompareCtx) walkAnalyzedTree(normPrefix []byte, pruneToPrefix bool, fn func(an *analyzedNode) error) (retErr error) {
	rootB, err := newSideIter(tc.base, RootIno)
	if err != nil {
		return err
	}
	var rootT *sideIter
	if !tc.targetIsBase {
		rootT, err = newSideIter(tc.target, RootIno)
		if err != nil {
			_ = rootB.close()
			return err
		}
	}
	rootS, err := newSideIter(tc.source, RootIno)
	if err != nil {
		_ = rootB.close()
		_ = rootT.close()
		return err
	}

	rootFrame := &dirMergeFrame{
		baseIt:     rootB,
		targetIt:   rootT,
		sourceIt:   rootS,
		targetSame: tc.targetIsBase,
	}
	h := &dirFrameHeap{}
	heap.Init(h)
	defer func() {
		for h.Len() > 0 {
			if f, okPop := heap.Pop(h).(*dirMergeFrame); okPop {
				retErr = errors.Join(retErr, f.close())
			}
		}
	}()

	ok, err := rootFrame.peekNext()
	if err != nil {
		_ = rootFrame.close()
		return err
	}
	if ok {
		heap.Push(h, rootFrame)
	} else {
		if cErr := rootFrame.close(); cErr != nil {
			return cErr
		}
	}

	for h.Len() > 0 {
		if ctxErr := tc.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		frame, okPop := heap.Pop(h).(*dirMergeFrame)
		if !okPop {
			continue
		}

		childPath := frame.nextPath
		bEntry := frame.bEntry
		tEntry := frame.tEntry
		sEntry := frame.sEntry

		hasMore, advErr := frame.consumeAndPeekNext()
		if advErr != nil {
			_ = frame.close()
			return advErr
		}
		if hasMore {
			heap.Push(h, frame)
		} else if cErr := frame.close(); cErr != nil {
			return cErr
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
			if (an.oNode != nil && (an.oNode.Mode&S_IFMT) == S_IFDIR) ||
				(an.tNode != nil && (an.tNode.Mode&S_IFMT) == S_IFDIR) ||
				(an.sNode != nil && (an.sNode.Mode&S_IFMT) == S_IFDIR) {
				shouldDescend = true
			}
		}

		if shouldDescend && (!pruneToPrefix || dirCanContainPathPrefix(childPath, normPrefix)) {
			var bDirIno, tDirIno, sDirIno uint64
			if an.oNode != nil && (an.oNode.Mode&S_IFMT) == S_IFDIR {
				bDirIno = an.oNode.Ino
			}
			if an.tNode != nil && (an.tNode.Mode&S_IFMT) == S_IFDIR {
				tDirIno = an.tNode.Ino
			}
			if an.sNode != nil && (an.sNode.Mode&S_IFMT) == S_IFDIR {
				sDirIno = an.sNode.Ino
			}
			var bIt, tIt, sIt *sideIter
			if bDirIno != 0 {
				bIt, err = newSideIter(tc.base, bDirIno)
				if err != nil {
					return err
				}
			}
			if !tc.targetIsBase && tDirIno != 0 {
				tIt, err = newSideIter(tc.target, tDirIno)
				if err != nil {
					_ = bIt.close()
					return err
				}
			}
			if sDirIno != 0 {
				sIt, err = newSideIter(tc.source, sDirIno)
				if err != nil {
					_ = bIt.close()
					_ = tIt.close()
					return err
				}
			}
			childFrame := &dirMergeFrame{
				dirPath:    childPath,
				baseIt:     bIt,
				targetIt:   tIt,
				sourceIt:   sIt,
				targetSame: tc.targetIsBase && bDirIno == tDirIno,
			}
			cOK, pErr := childFrame.peekNext()
			if pErr != nil {
				_ = childFrame.close()
				return pErr
			}
			if cOK {
				heap.Push(h, childFrame)
			} else if cErr := childFrame.close(); cErr != nil {
				return cErr
			}
		}
	}
	return nil
}

func DiffTrees(ctx context.Context, opts *DiffOptions) (*DiffResult, error) {
	if opts == nil || opts.BaseRoot == "" || opts.SourceRoot == "" {
		return nil, ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.PageSize > MaxDiffPageSize {
		return nil, ErrInvalid
	}
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = DefaultDiffPageSize
	}

	normPrefix, err := normalizePathPrefix(opts.PathPrefix)
	if err != nil {
		return nil, err
	}

	var afterPath []byte
	if opts.PageToken != "" {
		afterPath, err = decodeDiffPageCursor(opts.PageToken)
		if err != nil {
			return nil, err
		}
	}

	var sharedCache *pebble.Cache
	cache := opts.Cache
	if cache == nil {
		sharedCache = pebble.NewCache(inspectReadOnlyCacheSize)
		cache = sharedCache
		defer sharedCache.Unref()
	}

	baseClean := filepath.Clean(opts.BaseRoot)
	baseActor, baseCleanup, err := openReadOnlyForInspect(baseClean, cache)
	if err != nil {
		return nil, err
	}
	defer baseCleanup()

	targetClean := baseClean
	if opts.TargetRoot != "" {
		targetClean = filepath.Clean(opts.TargetRoot)
	}
	targetIsBase := targetClean == baseClean
	var targetActor *StoreActor
	if targetIsBase {
		targetActor = baseActor
	} else {
		var targetCleanup func()
		targetActor, targetCleanup, err = openReadOnlyForInspect(targetClean, cache)
		if err != nil {
			return nil, err
		}
		defer targetCleanup()
	}

	sourceClean := filepath.Clean(opts.SourceRoot)
	var sourceActor *StoreActor
	switch sourceClean {
	case baseClean:
		sourceActor = baseActor
	case targetClean:
		sourceActor = targetActor
	default:
		var sourceCleanup func()
		sourceActor, sourceCleanup, err = openReadOnlyForInspect(sourceClean, cache)
		if err != nil {
			return nil, err
		}
		defer sourceCleanup()
	}

	tc := newTreeCompareCtx(ctx, baseActor, targetActor, sourceActor, targetIsBase)

	res := &DiffResult{}
	var hasMore bool

	type tAddedHLCandidate struct {
		path      []byte
		targetSum *DiffNodeSummary
	}

	var allMatched []*DiffEntry
	tAddedHLs := make(map[uint64][]tAddedHLCandidate)
	sTouchedBaseInos := make(map[uint64]struct{})

	if len(normPrefix) == 0 {
		bRoot, bOK, rErr := tc.readLiveInode(tc.base, RootIno)
		if rErr != nil {
			return nil, rErr
		}
		tRoot, tOK, rErr := tc.readLiveInode(tc.target, RootIno)
		if rErr != nil {
			return nil, rErr
		}
		sRoot, sOK, rErr := tc.readLiveInode(tc.source, RootIno)
		if rErr != nil {
			return nil, rErr
		}
		if bOK && tOK && sOK {
			rootAN, rErr := tc.analyzeAllThreePresent(&analyzedNode{path: []byte("/")}, &bRoot, &tRoot, &sRoot)
			if rErr != nil {
				return nil, rErr
			}
			if rootAN.emitDiff {
				var tSum *DiffNodeSummary
				if opts.TargetRoot != "" {
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
	}

	walkErr := tc.walkAnalyzedTree(normPrefix, targetIsBase, func(an *analyzedNode) error {

		if !targetIsBase && an.oNode == nil && an.tNode != nil && an.sNode == nil && (an.tNode.Mode&S_IFMT) != S_IFDIR {
			if _, ok, rErr := tc.readLiveInode(tc.base, an.tNode.Ino); rErr != nil {
				return rErr
			} else if ok {
				var tSum *DiffNodeSummary
				if opts.TargetRoot != "" {
					tSum = makeNodeSummary(an.tNode)
				}
				tAddedHLs[an.tNode.Ino] = append(tAddedHLs[an.tNode.Ino], tAddedHLCandidate{
					path:      an.path,
					targetSum: tSum,
				})
			}
		}

		if !targetIsBase && an.oNode != nil && (an.oNode.Mode&S_IFMT) != S_IFDIR {
			if an.sNode == nil {
				sTouchedBaseInos[an.oNode.Ino] = struct{}{}
			} else if !isUnchangedVsBase(an.sNode, an.oNode) {
				eq, eqErr := tc.nodesEqual(tc.base, an.oNode, tc.source, an.sNode)
				if eqErr != nil {
					return eqErr
				}
				if !eq {
					sTouchedBaseInos[an.oNode.Ino] = struct{}{}
				}
			}
		}

		if !an.emitDiff {
			return nil
		}
		if len(normPrefix) > 0 && !matchPathPrefix(an.path, normPrefix) {
			return nil
		}

		var tSum *DiffNodeSummary
		if opts.TargetRoot != "" {
			tSum = makeNodeSummary(an.tNode)
		}
		entry := &DiffEntry{
			Path:     an.path,
			Kind:     an.kind,
			Conflict: an.conflict,
			Base:     makeNodeSummary(an.oNode),
			Target:   tSum,
			Source:   makeNodeSummary(an.sNode),
		}
		allMatched = append(allMatched, entry)
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	if !targetIsBase && len(tAddedHLs) > 0 {
		for baseIno, items := range tAddedHLs {
			if _, touched := sTouchedBaseInos[baseIno]; !touched {
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
				allMatched = append(allMatched, &DiffEntry{
					Path:     item.path,
					Kind:     DiffKindAdded,
					Conflict: ConflictHardlink,
					Target:   item.targetSum,
				})
			}
		}
	}

	sort.Slice(allMatched, func(i, j int) bool {
		return compareDiffPath(allMatched[i].Path, allMatched[j].Path) < 0
	})

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
		if opts.ConflictsOnly && e.Conflict == ConflictNone {
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
