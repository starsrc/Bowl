package store

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

type inodeOriginKind uint8

const (
	originTarget inodeOriginKind = iota + 1
	originSource
	originFused
)

type inodeOriginKey struct {
	kind     inodeOriginKind
	origIno  uint64
	srcIno   uint64
	splitSeq uint64
}

type xattrMode uint8

const (
	xattrKeepTarget xattrMode = iota
	xattrCopySource
	xattrReplaceMap
)

type plannedInodeSpec struct {
	key           inodeOriginKey
	tgtOrigIno    uint64
	targetIno     uint64
	isDir         bool
	baseInode     Inode
	srcInoForData uint64
	payloadFrom   inodeOriginKind
	xattrMode     xattrMode
	mergedXattrs  map[string][]byte
	forceMergeSeq bool
	reallocated   bool
	nlink         uint32
	parentIno     uint64
}

type plannedChild struct {
	parentPath string
	name       []byte
	relPath    string
	spec       *plannedInodeSpec
}

type mergeWalkRecord struct {
	an        *analyzedNode
	parentDir string
	name      []byte
}

type mergeIDAllocator struct {
	next     uint64
	reserved uint64
	maxSeen  uint64
	allocFn  func(ctx context.Context, minNext uint64) (first, last uint64, err error)
}

func newMergeIDAllocator(maxExisting uint64, fn func(ctx context.Context, minNext uint64) (uint64, uint64, error)) *mergeIDAllocator {
	return &mergeIDAllocator{
		maxSeen: maxExisting,
		allocFn: fn,
	}
}

func (a *mergeIDAllocator) nextID(ctx context.Context) (uint64, error) {
	if a.next == 0 || a.next > a.reserved {
		last := max(a.maxSeen, a.reserved)
		if last >= uint64(math.MaxInt64) {
			return 0, ErrInodeExhausted
		}
		if a.allocFn != nil {
			first, reserved, err := a.allocFn(ctx, last+1)
			if err != nil {
				return 0, err
			}
			if first <= last || reserved < first || first > uint64(math.MaxInt64) {
				return 0, ErrInvalidValue
			}
			if reserved > uint64(math.MaxInt64) {
				reserved = uint64(math.MaxInt64)
			}
			a.next = first
			a.reserved = reserved
		} else {
			a.next = last + 1
			a.reserved = last + 1
		}
	}
	id := a.next
	a.next++
	if id > a.maxSeen {
		a.maxSeen = id
	}
	return id, nil
}

type packOffsetKey struct {
	packID uint64
	offset uint32
}

type splitPackKey struct {
	targetIno uint64
	packID    uint64
}

func MergeTrees(ctx context.Context, opts *MergeOptions) (*MergeResult, error) {
	if opts == nil || opts.BaseRoot == "" || opts.TargetRoot == "" || opts.SourceRoot == "" {
		return nil, ErrInvalid
	}
	var stagingClean string
	if !opts.DryRun {
		if opts.StagingDir == "" {
			return nil, ErrInvalid
		}
		stagingClean = filepath.Clean(opts.StagingDir)
		if _, statErr := os.Lstat(stagingClean); statErr == nil {
			return nil, ErrExist
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, statErr
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	normStrategies, err := normalizePathStrategyMap(opts.PathStrategies)
	if err != nil {
		return nil, err
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

	targetClean := filepath.Clean(opts.TargetRoot)
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

	tStat, err := readBranchStatFromDB(targetActor.db)
	if err != nil {
		return nil, err
	}
	if tStat.WriterEpoch != opts.ExpectedWriterEpoch {
		return nil, ErrStaleEpoch
	}
	sStat, err := readBranchStatFromDB(sourceActor.db)
	if err != nil {
		return nil, err
	}
	seqMerge := max(tStat.NextWriteSeq, sStat.NextWriteSeq) + 1

	tc := newTreeCompareCtx(ctx, baseActor, targetActor, sourceActor, targetIsBase)

	planner, err := buildMergePlan(ctx, tc, opts, normStrategies, seqMerge)
	if err != nil {
		return nil, err
	}

	if len(planner.conflicts) > 0 {
		total := uint64(len(planner.conflicts))
		samples := planner.conflicts
		truncated := false
		if len(samples) > MaxMergeConflictSamples {
			samples = samples[:MaxMergeConflictSamples]
			truncated = true
		}
		return nil, &ErrMergeConflict{
			Report: MergeConflictReport{
				TotalConflicts:     total,
				Conflicts:          samples,
				ConflictsTruncated: truncated,
			},
		}
	}

	if err := planner.finalizeInodesAndLedger(ctx, targetActor, opts); err != nil {
		return nil, err
	}

	targetMaxBytes := tStat.MaxBytes
	if opts.OverrideMaxBytes > 0 {
		targetMaxBytes = opts.OverrideMaxBytes
		if planner.logicalBytes > targetMaxBytes {
			return nil, ErrQuotaShrinkBelowUsed
		}
	} else if targetMaxBytes > 0 && planner.logicalBytes > targetMaxBytes {
		return nil, ErrNoSpace
	}

	result := &MergeResult{
		UsedBytes:      planner.logicalBytes,
		Inodes:         planner.inodesCount,
		TargetMaxBytes: targetMaxBytes,
		WriterEpoch:    opts.ExpectedWriterEpoch + 1,
		NextWriteSeq:   seqMerge,
	}
	if opts.DryRun {
		return result, nil
	}

	if err := planner.commitToStaging(ctx, targetClean, sourceClean, opts, targetActor, sourceActor, result, cache); err != nil {
		if !errors.Is(err, ErrExist) && !errors.Is(err, os.ErrExist) {
			_ = os.RemoveAll(stagingClean)
		}
		return nil, err
	}
	return result, nil
}

func normalizePathStrategyMap(raw map[string]MergeStrategy) (map[string]MergeStrategy, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]MergeStrategy, len(raw))
	for k, v := range raw {
		norm, err := normalizeStrategyPath(k)
		if err != nil {
			return nil, err
		}
		if v < MergeStrategyUnspecified || v > MergeStrategyTheirs {
			return nil, ErrInvalid
		}
		out[norm] = v
	}
	return out, nil
}

func resolveMergeStrategy(global MergeStrategy, byPath map[string]MergeStrategy, relPath []byte) MergeStrategy {
	if len(byPath) > 0 {
		cur := string(relPath)
		if cur == "/" {
			cur = ""
		}
		for {
			if s, ok := byPath[cur]; ok && s != MergeStrategyUnspecified {
				return s
			}
			if cur == "" {
				break
			}
			idx := strings.LastIndexByte(cur, '/')
			if idx < 0 {
				cur = ""
			} else {
				cur = cur[:idx]
			}
		}
	}
	if global == MergeStrategyUnspecified {
		return MergeStrategyFail
	}
	return global
}

func readBranchStatFromDB(db *pebble.DB) (BranchStat, error) {
	val, closer, err := db.Get(StatKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return BranchStat{}, ErrNotFound
		}
		return BranchStat{}, err
	}
	st, decErr := DecodeBranchStat(val)
	closeErr := closer.Close()
	if decErr != nil {
		return BranchStat{}, decErr
	}
	if closeErr != nil {
		return BranchStat{}, closeErr
	}
	return st, nil
}

type mergePlanner struct {
	tc                 *treeCompareCtx
	seqMerge           uint64
	specsByOrigin      map[inodeOriginKey]*plannedInodeSpec
	rootSpec           *plannedInodeSpec
	childrenByParent   map[string][]*plannedChild
	dirSpecByPath      map[string]*plannedInodeSpec
	dirChildrenChanged map[string]bool
	targetLiveInos     map[uint64]Inode
	targetOrphans      []uint64
	maxExistingTgtIno  uint64
	conflicts          []MergeConflict
	orderedSpecs       []*plannedInodeSpec
	logicalBytes       uint64
	inodesCount        uint64
}

func buildMergePlan(
	ctx context.Context,
	tc *treeCompareCtx,
	opts *MergeOptions,
	normStrategies map[string]MergeStrategy,
	seqMerge uint64,
) (*mergePlanner, error) {
	mp := &mergePlanner{
		tc:                 tc,
		seqMerge:           seqMerge,
		specsByOrigin:      make(map[inodeOriginKey]*plannedInodeSpec),
		childrenByParent:   make(map[string][]*plannedChild),
		dirSpecByPath:      make(map[string]*plannedInodeSpec),
		dirChildrenChanged: make(map[string]bool),
		targetLiveInos:     make(map[uint64]Inode),
	}

	if err := mp.scanTargetInodesAndOrphans(); err != nil {
		return nil, err
	}

	if err := mp.analyzeRootNode(opts.Strategy, normStrategies); err != nil {
		return nil, err
	}

	var records []*mergeWalkRecord
	tAddedHLPaths := make(map[uint64][][]byte)
	sTouchedBaseInos := make(map[uint64]struct{})

	walkErr := tc.walkAnalyzedTree(nil, false, func(an *analyzedNode) error {
		if !tc.targetIsBase && an.oNode == nil && an.tNode != nil && an.sNode == nil && (an.tNode.Mode&S_IFMT) != S_IFDIR {
			if _, ok, rErr := tc.readLiveInode(tc.base, an.tNode.Ino); rErr != nil {
				return rErr
			} else if ok {
				tAddedHLPaths[an.tNode.Ino] = append(tAddedHLPaths[an.tNode.Ino], an.path)
			}
		}
		if !tc.targetIsBase && an.oNode != nil && (an.oNode.Mode&S_IFMT) != S_IFDIR {
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

		parentStr, name := splitParentAndName(an.path)
		records = append(records, &mergeWalkRecord{
			an:        an,
			parentDir: parentStr,
			name:      name,
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	if !tc.targetIsBase && len(tAddedHLPaths) > 0 {
		for _, rec := range records {
			an := rec.an
			var checkIno uint64
			if an.oNode != nil {
				checkIno = an.oNode.Ino
			} else if an.tNode != nil && an.sNode == nil {
				checkIno = an.tNode.Ino
			}
			if checkIno == 0 {
				continue
			}
			if _, hasTLink := tAddedHLPaths[checkIno]; !hasTLink {
				continue
			}
			if _, hasSTouch := sTouchedBaseInos[checkIno]; !hasSTouch {
				continue
			}
			if an.conflict == ConflictNone {
				an.conflict = ConflictHardlink
				an.decision = nodeDecisionConflict
			}
		}
	}

	skippedSubtreePrefixes := make([]string, 0)
	for _, rec := range records {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		pathStr := string(rec.an.path)
		if isUnderAnyPrefix(pathStr, skippedSubtreePrefixes) {
			continue
		}
		if err := mp.applyRecord(rec, opts.Strategy, normStrategies, &skippedSubtreePrefixes); err != nil {
			return nil, err
		}
	}

	if len(mp.conflicts) > 1 {
		sort.Slice(mp.conflicts, func(i, j int) bool {
			return compareDiffPath(mp.conflicts[i].Path, mp.conflicts[j].Path) < 0
		})
	}

	return mp, nil
}

func (mp *mergePlanner) scanTargetInodesAndOrphans() error {
	iter, err := mp.tc.target.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	inoPrefix := inodePrefixBytes
	for iter.SeekGE(inoPrefix); iter.Valid() && bytes.HasPrefix(iter.Key(), inoPrefix); iter.Next() {
		ino, decErr := DecodeInodeKey(iter.Key())
		if decErr != nil {
			return decErr
		}
		if ino > mp.maxExistingTgtIno {
			mp.maxExistingTgtIno = ino
		}
		inode, valErr := DecodeInode(iter.Value())
		if valErr != nil {
			return valErr
		}
		if inode.Nlink > 0 {
			mp.targetLiveInos[ino] = inode
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}

	orphPrefix := OrphanPrefix()
	for iter.SeekGE(orphPrefix); iter.Valid() && bytes.HasPrefix(iter.Key(), orphPrefix); iter.Next() {
		ino, decErr := DecodeOrphanKey(iter.Key())
		if decErr != nil {
			return decErr
		}
		mp.targetOrphans = append(mp.targetOrphans, ino)
	}
	return iter.Error()
}

func (mp *mergePlanner) analyzeRootNode(global MergeStrategy, byPath map[string]MergeStrategy) error {
	bRoot, hasB, err := mp.tc.readLiveInode(mp.tc.base, RootIno)
	if err != nil {
		return err
	}
	tRoot, hasT, err := mp.tc.readLiveInode(mp.tc.target, RootIno)
	if err != nil {
		return err
	}
	sRoot, hasS, err := mp.tc.readLiveInode(mp.tc.source, RootIno)
	if err != nil {
		return err
	}
	if !hasB || !hasT || !hasS {
		return ErrNotFound
	}

	if isUnchangedVsBase(&sRoot, &bRoot) {
		mp.rootSpec = mp.createRootSpec(originTarget, &tRoot, xattrKeepTarget, nil, false)
		return nil
	}
	if isUnchangedVsBase(&tRoot, &bRoot) {
		selfEq, eqErr := mp.tc.dirSelfEqual(mp.tc.base, &bRoot, mp.tc.source, &sRoot)
		if eqErr != nil {
			return eqErr
		}
		if selfEq {
			mp.rootSpec = mp.createRootSpec(originTarget, &tRoot, xattrKeepTarget, nil, false)
			return nil
		}
		meta := tRoot
		copyMeta(&meta, &sRoot)
		mp.rootSpec = mp.createRootSpec(originFused, &meta, xattrCopySource, nil, true)
		return nil
	}

	xo, err := readAllXattrs(mp.tc.base, RootIno)
	if err != nil {
		return err
	}
	xt, err := readAllXattrs(mp.tc.target, RootIno)
	if err != nil {
		return err
	}
	xs, err := readAllXattrs(mp.tc.source, RootIno)
	if err != nil {
		return err
	}
	meta, xattrs, cKind := mergeNodeAttributesAndXattrs(&bRoot, &tRoot, &sRoot, xo, xt, xs, false, false)
	if cKind != ConflictNone {
		strategy := resolveMergeStrategy(global, byPath, []byte("/"))
		switch strategy {
		case MergeStrategyFail:
			mp.conflicts = append(mp.conflicts, MergeConflict{
				Path:   []byte("/"),
				Kind:   cKind,
				Base:   makeNodeSummary(&bRoot),
				Target: makeNodeSummary(&tRoot),
				Source: makeNodeSummary(&sRoot),
			})
			return nil
		case MergeStrategyOurs:
			mp.rootSpec = mp.createRootSpec(originTarget, &tRoot, xattrKeepTarget, nil, false)
			return nil
		case MergeStrategyTheirs:
			adopted := tRoot
			copyMeta(&adopted, &sRoot)
			mp.rootSpec = mp.createRootSpec(originFused, &adopted, xattrCopySource, nil, true)
			return nil
		}
	}
	changed := !metaEqual(&tRoot, &meta) || !xattrsEqual(xt, xattrs)
	mp.rootSpec = mp.createRootSpec(originFused, &meta, xattrReplaceMap, xattrs, changed)
	return nil
}

func copyMeta(dst, src *Inode) {
	dst.Mode = (dst.Mode & S_IFMT) | (src.Mode & PermMask)
	dst.UID = src.UID
	dst.GID = src.GID
	dst.Flags = src.Flags
	dst.Mtime = src.Mtime
	dst.Ctime = src.Ctime
}

func (mp *mergePlanner) createRootSpec(kind inodeOriginKind, inode *Inode, xm xattrMode, mx map[string][]byte, changed bool) *plannedInodeSpec {
	spec := &plannedInodeSpec{
		key:           inodeOriginKey{kind: kind, origIno: RootIno, srcIno: RootIno},
		tgtOrigIno:    RootIno,
		targetIno:     RootIno,
		isDir:         true,
		baseInode:     *inode,
		srcInoForData: RootIno,
		xattrMode:     xm,
		mergedXattrs:  mx,
		forceMergeSeq: changed,
		parentIno:     RootIno,
	}
	mp.specsByOrigin[spec.key] = spec
	mp.dirSpecByPath[""] = spec
	return spec
}

func (mp *mergePlanner) applyRecord(
	rec *mergeWalkRecord,
	global MergeStrategy,
	byPath map[string]MergeStrategy,
	skippedSubtreePrefixes *[]string,
) error {
	an := rec.an
	pathStr := string(an.path)

	switch an.decision {
	case nodeDecisionSkip:
		if an.tNode != nil {
			mp.dirChildrenChanged[rec.parentDir] = true
		}
		return nil

	case nodeDecisionDeleteTarget:
		mp.dirChildrenChanged[rec.parentDir] = true
		return nil

	case nodeDecisionKeepTarget:
		spec := mp.getOrCreateOriginSpec(originTarget, an.tNode, nil)
		mp.recordPlannedChild(rec.parentDir, rec.name, pathStr, spec)
		if spec.isDir {
			mp.dirSpecByPath[pathStr] = spec
		}
		return nil

	case nodeDecisionAdoptSource:
		mp.dirChildrenChanged[rec.parentDir] = true
		spec := mp.getOrCreateOriginSpec(originSource, an.sNode, nil)
		mp.recordPlannedChild(rec.parentDir, rec.name, pathStr, spec)
		if spec.isDir {
			mp.dirSpecByPath[pathStr] = spec
		}
		return nil

	case nodeDecisionFuse:
		return mp.applyFusedRecord(rec, pathStr)

	case nodeDecisionConflict:
		strategy := resolveMergeStrategy(global, byPath, an.path)
		if strategy == MergeStrategyFail {
			var tSum *DiffNodeSummary
			if !mp.tc.targetIsBase {
				tSum = makeNodeSummary(an.tNode)
			} else {
				tSum = makeNodeSummary(an.oNode)
			}
			mp.conflicts = append(mp.conflicts, MergeConflict{
				Path:   append([]byte(nil), an.path...),
				Kind:   an.conflict,
				Base:   makeNodeSummary(an.oNode),
				Target: tSum,
				Source: makeNodeSummary(an.sNode),
			})
			return nil
		}
		if strategy == MergeStrategyOurs {
			if an.tNode == nil {
				return nil
			}
			spec := mp.getOrCreateOriginSpec(originTarget, an.tNode, nil)
			mp.recordPlannedChild(rec.parentDir, rec.name, pathStr, spec)
			if spec.isDir {
				mp.dirSpecByPath[pathStr] = spec
				if !an.descend {

					return mp.collectSingleSideSubtree(mp.tc.target, an.tNode.Ino, pathStr, originTarget)
				}
			}
			return nil
		}

		mp.dirChildrenChanged[rec.parentDir] = true
		if an.sNode == nil {
			if an.isDir && an.descend {
				*skippedSubtreePrefixes = append(*skippedSubtreePrefixes, pathStr)
			}
			return nil
		}
		if an.tNode != nil && (an.tNode.Mode&S_IFMT) == S_IFDIR && an.descend && (an.sNode.Mode&S_IFMT) == S_IFDIR {

			meta := *an.tNode
			copyMeta(&meta, an.sNode)
			key := inodeOriginKey{kind: originFused, origIno: an.tNode.Ino, srcIno: an.sNode.Ino}
			spec, exists := mp.specsByOrigin[key]
			if !exists {
				xs, xErr := readAllXattrs(mp.tc.source, an.sNode.Ino)
				if xErr != nil {
					return xErr
				}
				spec = &plannedInodeSpec{
					key:           key,
					tgtOrigIno:    an.tNode.Ino,
					isDir:         true,
					baseInode:     meta,
					srcInoForData: an.sNode.Ino,
					xattrMode:     xattrReplaceMap,
					mergedXattrs:  xs,
					forceMergeSeq: true,
				}
				mp.specsByOrigin[key] = spec
			}
			mp.recordPlannedChild(rec.parentDir, rec.name, pathStr, spec)
			mp.dirSpecByPath[pathStr] = spec
			return nil
		}

		spec := mp.getOrCreateOriginSpec(originSource, an.sNode, nil)
		mp.recordPlannedChild(rec.parentDir, rec.name, pathStr, spec)
		if spec.isDir {
			mp.dirSpecByPath[pathStr] = spec
			if !an.descend {
				return mp.collectSingleSideSubtree(mp.tc.source, an.sNode.Ino, pathStr, originSource)
			}
		}
		return nil
	}
	return nil
}

func fusedSpecMatches(existing *plannedInodeSpec, meta *Inode, payloadFrom inodeOriginKind, xm xattrMode, mx map[string][]byte) bool {
	if existing.payloadFrom != payloadFrom || existing.xattrMode != xm {
		return false
	}
	if !metaEqual(&existing.baseInode, meta) || existing.baseInode.Size != meta.Size || !bytes.Equal(existing.baseInode.SymlinkTarget, meta.SymlinkTarget) {
		return false
	}
	return xattrsEqual(existing.mergedXattrs, mx)
}

func (mp *mergePlanner) applyFusedRecord(rec *mergeWalkRecord, pathStr string) error {
	an := rec.an
	tIno := an.tNode.Ino
	var sIno uint64
	if an.sNode != nil {
		sIno = an.sNode.Ino
	}

	meta := an.mergedMeta
	payloadFrom := originTarget
	var srcInoForData uint64
	if an.payloadFromSrc && an.sNode != nil {
		payloadFrom = originSource
		srcInoForData = an.sNode.Ino
		meta.Size = an.sNode.Size
		meta.SymlinkTarget = append([]byte(nil), an.sNode.SymlinkTarget...)
	}
	xm := xattrKeepTarget
	if an.mergedXattrs != nil {
		xm = xattrReplaceMap
	}

	var spec *plannedInodeSpec
	for splitSeq := uint64(0); ; splitSeq++ {
		key := inodeOriginKey{kind: originFused, origIno: tIno, srcIno: sIno, splitSeq: splitSeq}
		existing, exists := mp.specsByOrigin[key]
		if !exists {
			spec = &plannedInodeSpec{
				key:           key,
				tgtOrigIno:    tIno,
				isDir:         an.isDir,
				baseInode:     meta,
				srcInoForData: srcInoForData,
				payloadFrom:   payloadFrom,
				xattrMode:     xm,
				mergedXattrs:  an.mergedXattrs,
				forceMergeSeq: true,
			}
			mp.specsByOrigin[key] = spec
			break
		}
		if fusedSpecMatches(existing, &meta, payloadFrom, xm, an.mergedXattrs) {
			spec = existing
			break
		}
	}

	mp.recordPlannedChild(rec.parentDir, rec.name, pathStr, spec)
	if spec.isDir {
		mp.dirSpecByPath[pathStr] = spec
	}
	return nil
}

func (mp *mergePlanner) getOrCreateOriginSpec(kind inodeOriginKind, inode *Inode, extraXattrs map[string][]byte) *plannedInodeSpec {
	key := inodeOriginKey{kind: kind, origIno: inode.Ino}
	if existing, ok := mp.specsByOrigin[key]; ok {
		return existing
	}
	xm := xattrKeepTarget
	if kind == originSource {
		xm = xattrCopySource
	}
	if extraXattrs != nil {
		xm = xattrReplaceMap
	}
	copyInode := *inode
	if copyInode.SymlinkTarget != nil {
		copyInode.SymlinkTarget = append([]byte(nil), copyInode.SymlinkTarget...)
	}
	var tgtOrig uint64
	if kind == originTarget {
		tgtOrig = inode.Ino
	}
	spec := &plannedInodeSpec{
		key:           key,
		tgtOrigIno:    tgtOrig,
		isDir:         (inode.Mode & S_IFMT) == S_IFDIR,
		baseInode:     copyInode,
		srcInoForData: inode.Ino,
		payloadFrom:   kind,
		xattrMode:     xm,
		mergedXattrs:  extraXattrs,
	}
	mp.specsByOrigin[key] = spec
	return spec
}

func (mp *mergePlanner) recordPlannedChild(parentDir string, name []byte, relPath string, spec *plannedInodeSpec) {
	child := &plannedChild{
		parentPath: parentDir,
		name:       append([]byte(nil), name...),
		relPath:    relPath,
		spec:       spec,
	}
	mp.childrenByParent[parentDir] = append(mp.childrenByParent[parentDir], child)
}

func (mp *mergePlanner) collectSingleSideSubtree(actor *StoreActor, dirIno uint64, dirPath string, origin inodeOriginKind) error {
	iter, err := newSideIter(actor, dirIno)
	if err != nil {
		return err
	}
	defer func() { _ = iter.close() }()

	type subDirWork struct {
		ino  uint64
		path string
	}
	var subDirs []subDirWork

	for iter != nil && iter.valid {
		childEntry := iter.cur
		childInode, ok, rErr := mp.tc.readLiveInode(actor, childEntry.entry.Ino)
		if rErr != nil {
			return rErr
		}
		if ok {
			childPath := joinRelPath([]byte(dirPath), childEntry.name)
			childPathStr := string(childPath)
			spec := mp.getOrCreateOriginSpec(origin, &childInode, nil)
			mp.recordPlannedChild(dirPath, childEntry.name, childPathStr, spec)
			if spec.isDir {
				mp.dirSpecByPath[childPathStr] = spec
				subDirs = append(subDirs, subDirWork{ino: childInode.Ino, path: childPathStr})
			}
		}
		if err := iter.advance(); err != nil {
			return err
		}
	}

	for _, sd := range subDirs {
		if err := mp.collectSingleSideSubtree(actor, sd.ino, sd.path, origin); err != nil {
			return err
		}
	}
	return nil
}

func (mp *mergePlanner) finalizeInodesAndLedger(ctx context.Context, _ *StoreActor, opts *MergeOptions) error {

	seenSpecs := make(map[*plannedInodeSpec]struct{})
	var allSpecs []*plannedInodeSpec
	addSpec := func(s *plannedInodeSpec) {
		if _, ok := seenSpecs[s]; ok {
			return
		}
		seenSpecs[s] = struct{}{}
		allSpecs = append(allSpecs, s)
	}
	addSpec(mp.rootSpec)

	for _, children := range mp.childrenByParent {
		for _, ch := range children {
			addSpec(ch.spec)
			if !ch.spec.isDir {
				ch.spec.nlink++
			}
		}
	}

	dirSubdirs := make(map[*plannedInodeSpec]uint32, len(mp.dirSpecByPath))
	for parentPath, children := range mp.childrenByParent {
		parentSpec := mp.dirSpecByPath[parentPath]
		if parentSpec == nil {
			continue
		}
		for _, ch := range children {
			if ch.spec.isDir {
				dirSubdirs[parentSpec]++
			}
		}
	}
	for _, spec := range allSpecs {
		if !spec.isDir {
			continue
		}
		nlink := uint64(2) + uint64(dirSubdirs[spec])
		if nlink > uint64(MaxNlink) {
			return ErrTooManyLinks
		}
		spec.nlink = uint32(nlink)
		if spec != mp.rootSpec && spec.baseInode.Nlink != spec.nlink {
			spec.forceMergeSeq = true
		}
	}
	if mp.rootSpec.baseInode.Nlink != mp.rootSpec.nlink || mp.dirChildrenChanged[""] {
		mp.rootSpec.forceMergeSeq = true
	}

	claimedInos := make(map[uint64]struct{}, len(allSpecs))
	maxIno := max(mp.maxExistingTgtIno, RootIno)
	for _, spec := range allSpecs {
		if spec == mp.rootSpec {
			spec.targetIno = RootIno
			claimedInos[RootIno] = struct{}{}
			continue
		}
		if spec.tgtOrigIno > maxIno {
			maxIno = spec.tgtOrigIno
		}
		if spec.key.origIno > maxIno {
			maxIno = spec.key.origIno
		}
		if spec.key.kind == originTarget {
			spec.targetIno = spec.tgtOrigIno
			claimedInos[spec.targetIno] = struct{}{}
		}
	}

	var allocFn func(context.Context, uint64) (uint64, uint64, error)
	if !opts.DryRun {
		allocFn = opts.InoAllocator
	}
	inoAlloc := newMergeIDAllocator(maxIno, allocFn)

	for _, spec := range allSpecs {
		if spec == mp.rootSpec || spec.key.kind != originFused {
			continue
		}
		origIno := spec.tgtOrigIno
		if _, alreadyClaimed := claimedInos[origIno]; !alreadyClaimed {
			spec.targetIno = origIno
			claimedInos[origIno] = struct{}{}
			continue
		}
		newIno, err := inoAlloc.nextID(ctx)
		if err != nil {
			return err
		}
		spec.targetIno = newIno
		spec.reallocated = true
		spec.forceMergeSeq = true
		claimedInos[newIno] = struct{}{}
	}

	for _, spec := range allSpecs {
		if spec.key.kind != originSource {
			continue
		}
		origIno := spec.key.origIno
		_, alreadyClaimed := claimedInos[origIno]
		_, wasLiveInTgt := mp.targetLiveInos[origIno]

		if !alreadyClaimed && !wasLiveInTgt {
			spec.targetIno = origIno
			claimedInos[origIno] = struct{}{}
			continue
		}

		if !alreadyClaimed {
			spec.targetIno = origIno
			claimedInos[origIno] = struct{}{}
			continue
		}
		newIno, err := inoAlloc.nextID(ctx)
		if err != nil {
			return err
		}
		spec.targetIno = newIno
		spec.forceMergeSeq = true
		claimedInos[newIno] = struct{}{}
	}

	for dirPath, spec := range mp.dirSpecByPath {
		if dirPath == "" {
			spec.parentIno = RootIno
			continue
		}
		parentPath, _ := splitParentAndName([]byte(dirPath))
		parentSpec := mp.dirSpecByPath[parentPath]
		if parentSpec == nil {
			return ErrInvalidValue
		}
		spec.parentIno = parentSpec.targetIno
		if spec.baseInode.ParentIno != spec.parentIno || mp.dirChildrenChanged[dirPath] {
			spec.forceMergeSeq = true
		}
	}

	var totalLogical uint64
	for _, spec := range allSpecs {
		if spec.baseInode.Size > math.MaxUint64-totalLogical {
			return ErrNoSpace
		}
		totalLogical += spec.baseInode.Size
		if !spec.isDir {
			spec.parentIno = 0
		}
	}

	mp.orderedSpecs = allSpecs
	mp.logicalBytes = totalLogical
	mp.inodesCount = uint64(len(allSpecs))
	return nil
}

func (mp *mergePlanner) commitToStaging(
	ctx context.Context,
	targetDir, sourceDir string,
	opts *MergeOptions,
	targetActor, sourceActor *StoreActor,
	result *MergeResult,
	cache *pebble.Cache,
) error {
	if err := CloneCheckpointDir(ctx, targetDir, opts.StagingDir); err != nil {
		return err
	}
	stagingDB, err := pebble.Open(opts.StagingDir, NewPebbleOptions(cache, false))
	if err != nil {
		return err
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = stagingDB.Close()
		}
	}()

	tgtPackStats, maxPackID, err := loadPackStatsMap(targetActor.db)
	if err != nil {
		return err
	}
	srcPackStats, _, err := loadPackStatsMap(sourceActor.db)
	if err != nil {
		return err
	}
	for _, pid := range existingPackIDsFromDir(opts.StagingDir) {
		if pid > maxPackID {
			maxPackID = pid
		}
	}
	for _, pid := range existingPackIDsFromDir(sourceDir) {
		if pid > maxPackID {
			maxPackID = pid
		}
	}
	for pid := range srcPackStats {
		if pid > maxPackID {
			maxPackID = pid
		}
	}
	packAlloc := newMergeIDAllocator(maxPackID, opts.PackAllocator)

	packLiveBytes := make(map[uint64]uint64, len(tgtPackStats)+len(srcPackStats))
	packTotalBytes := make(map[uint64]uint64, len(tgtPackStats)+len(srcPackStats))
	for pid, meta := range tgtPackStats {
		packLiveBytes[pid] = meta.LiveBytes
		packTotalBytes[pid] = meta.TotalBytes
	}

	batch := stagingDB.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, orphIno := range mp.targetOrphans {
		BatchDeleteOrphanDeferred(batch, orphIno)
		if err := reclaimTargetInodeKV(stagingDB, batch, orphIno, packLiveBytes); err != nil {
			return err
		}
	}

	survivingTargetInos := make(map[uint64]*plannedInodeSpec)
	for _, spec := range mp.orderedSpecs {
		if spec.key.kind == originTarget || spec.key.kind == originFused {
			survivingTargetInos[spec.targetIno] = spec
		}
	}
	for oldTgtIno := range mp.targetLiveInos {
		if _, keep := survivingTargetInos[oldTgtIno]; !keep {
			if err := reclaimTargetInodeKV(stagingDB, batch, oldTgtIno, packLiveBytes); err != nil {
				return err
			}
		}
	}

	usedPackOffsets := make(map[packOffsetKey]bool)
	for _, spec := range mp.orderedSpecs {
		if spec.isDir || (spec.baseInode.Mode&S_IFMT) != S_IFREG || spec.reallocated {
			continue
		}
		if spec.key.kind == originTarget || (spec.key.kind == originFused && spec.payloadFrom == originTarget) {
			if err := scanExistingDataOffsets(stagingDB, spec.targetIno, usedPackOffsets); err != nil {
				return err
			}
		} else if spec.key.kind == originFused && spec.payloadFrom == originSource {

			if err := reclaimDataPrefix(stagingDB, batch, spec.targetIno, packLiveBytes); err != nil {
				return err
			}
		}
	}

	srcPacksDir := filepath.Join(sourceDir, "packs")
	tgtPacksDir := filepath.Join(targetDir, "packs")
	stagingPacksDir := filepath.Join(opts.StagingDir, "packs")
	srcDefaultPackMap := make(map[uint64]uint64)
	tgtDefaultPackMap := make(map[uint64]uint64)
	splitPackMap := make(map[splitPackKey]uint64)
	linksToCreate := make(map[uint64]string)

	importSpecData := func(srcDB *pebble.DB, srcPacksDir string, srcStats map[uint64]PackMeta, defaultPackMap map[uint64]uint64, srcIno, targetIno uint64) error {
		return forEachDataKey(srcDB, srcIno, func(blockIdx uint64, ref BlobRef) error {
			srcPackPath := packFilePath(srcPacksDir, ref.PackID)
			mappedPackID, ok := defaultPackMap[ref.PackID]
			if !ok {
				tgtPackPath := packFilePath(stagingPacksDir, ref.PackID)
				tgtInfo, tStatErr := os.Stat(tgtPackPath)
				srcInfo, sStatErr := os.Stat(srcPackPath)
				if sStatErr != nil {
					return sStatErr
				}
				srcTotal := uint64(srcInfo.Size())
				if meta, hasMeta := srcStats[ref.PackID]; hasMeta && meta.TotalBytes > srcTotal {
					srcTotal = meta.TotalBytes
				}

				switch {
				case tStatErr == nil:
					if os.SameFile(tgtInfo, srcInfo) {
						mappedPackID = ref.PackID
					} else {
						newID, aErr := packAlloc.nextID(ctx)
						if aErr != nil {
							return aErr
						}
						mappedPackID = newID
						linksToCreate[newID] = srcPackPath
						packTotalBytes[newID] = srcTotal
					}
				case errors.Is(tStatErr, os.ErrNotExist):
					mappedPackID = ref.PackID
					linksToCreate[mappedPackID] = srcPackPath
					packTotalBytes[mappedPackID] = srcTotal
				default:
					return tStatErr
				}
				defaultPackMap[ref.PackID] = mappedPackID
			}

			finalPackID := mappedPackID
			if usedPackOffsets[packOffsetKey{packID: finalPackID, offset: ref.Offset}] {
				sk := splitPackKey{targetIno: targetIno, packID: ref.PackID}
				splitID, hasSplit := splitPackMap[sk]
				if !hasSplit {
					newID, aErr := packAlloc.nextID(ctx)
					if aErr != nil {
						return aErr
					}
					splitID = newID
					splitPackMap[sk] = splitID
					linksToCreate[splitID] = srcPackPath
					srcInfo, sStatErr := os.Stat(srcPackPath)
					if sStatErr != nil {
						return sStatErr
					}
					srcTotal := uint64(srcInfo.Size())
					if meta, hasMeta := srcStats[ref.PackID]; hasMeta && meta.TotalBytes > srcTotal {
						srcTotal = meta.TotalBytes
					}
					packTotalBytes[splitID] = srcTotal
				}
				finalPackID = splitID
			}

			usedPackOffsets[packOffsetKey{packID: finalPackID, offset: ref.Offset}] = true
			packLiveBytes[finalPackID] += uint64(ref.Length)
			BatchSetDataDeferred(batch, targetIno, blockIdx, BlobRef{
				PackID: finalPackID,
				Offset: ref.Offset,
				Length: ref.Length,
			})
			return nil
		})
	}

	for _, spec := range mp.orderedSpecs {
		if spec.isDir || (spec.baseInode.Mode&S_IFMT) != S_IFREG {
			continue
		}
		if spec.payloadFrom == originSource {
			if err := importSpecData(sourceActor.db, srcPacksDir, srcPackStats, srcDefaultPackMap, spec.srcInoForData, spec.targetIno); err != nil {
				return err
			}
		} else if spec.reallocated && spec.payloadFrom == originTarget {
			if err := importSpecData(targetActor.db, tgtPacksDir, tgtPackStats, tgtDefaultPackMap, spec.tgtOrigIno, spec.targetIno); err != nil {
				return err
			}
		}
	}

	if len(linksToCreate) > 0 {
		if mkErr := os.MkdirAll(stagingPacksDir, 0o755); mkErr != nil {
			return mkErr
		}
	}
	for dstPackID, srcPackPath := range linksToCreate {
		dstPackPath := packFilePath(stagingPacksDir, dstPackID)
		if linkErr := os.Link(srcPackPath, dstPackPath); linkErr != nil {
			if !errors.Is(linkErr, os.ErrExist) {
				return linkErr
			}
			srcInfo, sErr := os.Stat(srcPackPath)
			dstInfo, dErr := os.Stat(dstPackPath)
			if sErr != nil || dErr != nil || !os.SameFile(srcInfo, dstInfo) {
				return linkErr
			}
		}
	}
	if len(linksToCreate) > 0 {
		if err := syncDirPath(stagingPacksDir); err != nil {
			return err
		}
	}

	for _, spec := range mp.orderedSpecs {
		inode := spec.baseInode
		inode.Ino = spec.targetIno
		inode.Nlink = spec.nlink
		inode.ParentIno = spec.parentIno
		if spec.forceMergeSeq {
			inode.WriteSeq = mp.seqMerge
		}
		if err := BatchSetInodeDeferred(batch, inode); err != nil {
			return err
		}

		switch spec.xattrMode {
		case xattrKeepTarget:
			if spec.reallocated {
				tx, xErr := readAllXattrs(targetActor, spec.tgtOrigIno)
				if xErr != nil {
					return xErr
				}
				for k, v := range tx {
					BatchSetXattrDeferred(batch, spec.targetIno, []byte(k), v)
				}
			}
		case xattrCopySource:
			if !spec.reallocated && spec.key.kind == originFused {
				if err := deleteKeyPrefix(stagingDB, batch, XattrPrefix(spec.targetIno)); err != nil {
					return err
				}
			}
			sx, err := readAllXattrs(sourceActor, spec.srcInoForData)
			if err != nil {
				return err
			}
			for k, v := range sx {
				BatchSetXattrDeferred(batch, spec.targetIno, []byte(k), v)
			}
		case xattrReplaceMap:
			if !spec.reallocated && (spec.key.kind == originTarget || spec.key.kind == originFused) {
				if err := deleteKeyPrefix(stagingDB, batch, XattrPrefix(spec.targetIno)); err != nil {
					return err
				}
			}
			for k, v := range spec.mergedXattrs {
				BatchSetXattrDeferred(batch, spec.targetIno, []byte(k), v)
			}
		}

		if spec.isDir {

			if !spec.reallocated && (spec.key.kind == originTarget || spec.key.kind == originFused) {
				if err := deleteKeyPrefix(stagingDB, batch, EntryPrefix(spec.targetIno)); err != nil {
					return err
				}
			}
		}
	}

	for dirPath, children := range mp.childrenByParent {
		parentSpec := mp.dirSpecByPath[dirPath]
		if parentSpec == nil {
			return ErrInvalidValue
		}
		for _, ch := range children {
			BatchSetEntryDeferred(batch, parentSpec.targetIno, ch.name, Entry{
				Ino:  ch.spec.targetIno,
				Mode: ch.spec.baseInode.Mode,
			})
		}
	}

	var deadPackIDs []uint64
	for pid, live := range packLiveBytes {
		if live == 0 {
			BatchDeletePackMetaDeferred(batch, pid)
			deadPackIDs = append(deadPackIDs, pid)
			continue
		}
		total := max(packTotalBytes[pid], live)
		BatchSetPackMetaDeferred(batch, pid, PackMeta{
			TotalBytes: total,
			LiveBytes:  live,
		})
	}

	BatchSetStatDeferred(batch, BranchStat{
		LogicalBytes: result.UsedBytes,
		Inodes:       result.Inodes,
		NextWriteSeq: result.NextWriteSeq,
		MaxBytes:     result.TargetMaxBytes,
		WriterEpoch:  result.WriterEpoch,
	})

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	if err := stagingDB.Flush(); err != nil {
		return err
	}
	if err := stagingDB.Close(); err != nil {
		return err
	}
	dbClosed = true

	for _, pid := range deadPackIDs {
		p := packFilePath(stagingPacksDir, pid)
		if rmErr := os.Remove(p); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			return rmErr
		}
	}
	if len(deadPackIDs) > 0 {
		if err := syncDirPath(stagingPacksDir); err != nil {
			return err
		}
	}
	return syncDirPath(opts.StagingDir)
}

func splitParentAndName(relPath []byte) (parent string, name []byte) {
	idx := bytes.LastIndexByte(relPath, '/')
	if idx < 0 {
		return "", relPath
	}
	return string(relPath[:idx]), relPath[idx+1:]
}

func isUnderAnyPrefix(pathStr string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(pathStr, p) && len(pathStr) > len(p) && pathStr[len(p)] == '/' {
			return true
		}
	}
	return false
}

func loadPackStatsMap(db *pebble.DB) (stats map[uint64]PackMeta, maxPID uint64, err error) {
	iter, err := db.NewIter(nil)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = iter.Close() }()

	stats = make(map[uint64]PackMeta)
	prefix := PackStatPrefix()
	for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		pid, decErr := DecodePackStatKey(iter.Key())
		if decErr != nil {
			return nil, 0, decErr
		}
		meta, valErr := DecodePackMeta(iter.Value())
		if valErr != nil {
			return nil, 0, valErr
		}
		stats[pid] = meta
		if pid > maxPID {
			maxPID = pid
		}
	}
	return stats, maxPID, iter.Error()
}

func existingPackIDsFromDir(baseDir string) []uint64 {
	entries, err := os.ReadDir(filepath.Join(baseDir, "packs"))
	if err != nil {
		return nil
	}
	var ids []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".pack") {
			continue
		}
		numStr := strings.TrimSuffix(name, ".pack")
		if id, pErr := strconv.ParseUint(numStr, 16, 64); pErr == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

func reclaimTargetInodeKV(db *pebble.DB, batch *pebble.Batch, ino uint64, packLiveBytes map[uint64]uint64) error {
	BatchDeleteInodeDeferred(batch, ino)
	if err := deleteKeyPrefix(db, batch, XattrPrefix(ino)); err != nil {
		return err
	}
	if err := deleteKeyPrefix(db, batch, EntryPrefix(ino)); err != nil {
		return err
	}
	return reclaimDataPrefix(db, batch, ino, packLiveBytes)
}

func reclaimDataPrefix(db *pebble.DB, batch *pebble.Batch, ino uint64, packLiveBytes map[uint64]uint64) error {
	prefix := DataPrefix(ino)
	iter, err := db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		ref, decErr := DecodeBlobRef(iter.Value())
		if decErr != nil {
			return decErr
		}
		if cur := packLiveBytes[ref.PackID]; cur >= uint64(ref.Length) {
			packLiveBytes[ref.PackID] = cur - uint64(ref.Length)
		} else {
			packLiveBytes[ref.PackID] = 0
		}
		BatchDeleteDeferred(batch, iter.Key())
	}
	return iter.Error()
}

func deleteKeyPrefix(db *pebble.DB, batch *pebble.Batch, prefix []byte) error {
	iter, err := db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		BatchDeleteDeferred(batch, iter.Key())
	}
	return iter.Error()
}

func scanExistingDataOffsets(db *pebble.DB, ino uint64, used map[packOffsetKey]bool) error {
	return forEachDataKey(db, ino, func(_ uint64, ref BlobRef) error {
		used[packOffsetKey{packID: ref.PackID, offset: ref.Offset}] = true
		return nil
	})
}

func forEachDataKey(db *pebble.DB, ino uint64, fn func(blockIdx uint64, ref BlobRef) error) error {
	prefix := DataPrefix(ino)
	iter, err := db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		_, idx, decKeyErr := DecodeDataKey(iter.Key())
		if decKeyErr != nil {
			return decKeyErr
		}
		ref, decValErr := DecodeBlobRef(iter.Value())
		if decValErr != nil {
			return decValErr
		}
		if err := fn(idx, ref); err != nil {
			return err
		}
	}
	return iter.Error()
}
