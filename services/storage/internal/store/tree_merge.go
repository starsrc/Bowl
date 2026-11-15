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
	payloadFrom   inodeOriginKind // originTarget or originSource (valid for regular files)
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

// MergeTrees 执行三路/两路只读分析并将零拷贝合并结果落盘至 opts.StagingDir（或 DryRun 仅校验并估算结果）。
// 原 BaseRoot、TargetRoot、SourceRoot 仅以 ReadOnly 模式打开且绝不被修改。
func MergeTrees(ctx context.Context, opts *MergeOptions) (retRes *MergeResult, retErr error) {
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
			return nil, wrapIOErr(statErr)
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
	defer func() {
		retErr = errors.Join(retErr, baseCleanup())
		if retErr != nil {
			retRes = nil
		}
	}()

	targetClean := filepath.Clean(opts.TargetRoot)
	targetIsBase := targetClean == baseClean
	var targetActor *StoreActor
	if targetIsBase {
		targetActor = baseActor
	} else {
		var targetCleanup func() error
		targetActor, targetCleanup, err = openReadOnlyForInspect(targetClean, cache)
		if err != nil {
			return nil, err
		}
		defer func() {
			retErr = errors.Join(retErr, targetCleanup())
			if retErr != nil {
				retRes = nil
			}
		}()
	}

	sourceClean := filepath.Clean(opts.SourceRoot)
	var sourceActor *StoreActor
	switch sourceClean {
	case baseClean:
		sourceActor = baseActor
	case targetClean:
		sourceActor = targetActor
	default:
		var sourceCleanup func() error
		sourceActor, sourceCleanup, err = openReadOnlyForInspect(sourceClean, cache)
		if err != nil {
			return nil, err
		}
		defer func() {
			retErr = errors.Join(retErr, sourceCleanup())
			if retErr != nil {
				retRes = nil
			}
		}()
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

	// 1. 扫描 RootIno 与收集增量树记录，同时侦测双向硬链接冲突
	planner, err := buildMergePlan(ctx, tc, opts, normStrategies, seqMerge)
	if err != nil {
		return nil, err
	}

	// 若仍有未决冲突，返回有序样本报告，零副作用
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

	// 2. 分配目标 Ino 并重算目录树 Nlink / ParentIno / LogicalBytes / Inodes
	if err := planner.finalizeInodesAndLedger(ctx, targetActor, opts); err != nil {
		return nil, err
	}

	// 3. 配额检查（在创建 StagingDir 之前完成，失败零残留）
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

	// 4. 在独立 StagingDir 构建合并后分叉快照与硬链接复用 Pack
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
		return BranchStat{}, wrapIOErr(err)
	}
	st, decErr := DecodeBranchStat(val)
	closeErr := wrapIOErr(closer.Close())
	if decErr != nil || closeErr != nil {
		return BranchStat{}, errors.Join(decErr, closeErr)
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

	// 分析 RootIno 自身属性/Xattr
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

	// 补全双向硬链接冲突标记
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

	// 逐条决议并构造合并树拓扑
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

func (mp *mergePlanner) scanTargetInodesAndOrphans() (err error) {
	iter, openErr := mp.tc.target.db.NewIter(nil)
	if openErr != nil {
		return wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
	}()

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
	if iterErr := iter.Error(); iterErr != nil {
		return wrapIOErr(iterErr)
	}

	orphPrefix := OrphanPrefix()
	for iter.SeekGE(orphPrefix); iter.Valid() && bytes.HasPrefix(iter.Key(), orphPrefix); iter.Next() {
		ino, decErr := DecodeOrphanKey(iter.Key())
		if decErr != nil {
			return decErr
		}
		mp.targetOrphans = append(mp.targetOrphans, ino)
	}
	return nil
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
					// ConflictTreeDeleteModify 或 ConflictDirFile 下 Target 目录胜出：整棵导入 Target 子树
					return mp.collectSingleSideSubtree(mp.tc.target, an.tNode.Ino, pathStr, originTarget)
				}
			}
			return nil
		}
		// MergeStrategyTheirs
		mp.dirChildrenChanged[rec.parentDir] = true
		if an.sNode == nil {
			if an.isDir && an.descend {
				*skippedSubtreePrefixes = append(*skippedSubtreePrefixes, pathStr)
			}
			return nil
		}
		if an.tNode != nil && (an.tNode.Mode&S_IFMT) == S_IFDIR && an.descend && (an.sNode.Mode&S_IFMT) == S_IFDIR {
			// 双边目录因 Dir Attr/Xattr 冲突裁决为 Theirs，但子项继续正常三路遍历
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
		// 非目录或需要整棵导入 Source 子树的情况
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

func (mp *mergePlanner) collectSingleSideSubtree(actor *StoreActor, dirIno uint64, dirPath string, origin inodeOriginKind) (err error) {
	iter, openErr := newSideIter(actor, dirIno)
	if openErr != nil {
		return openErr
	}
	defer func() {
		err = errors.Join(err, iter.close())
	}()

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
		if advErr := iter.advance(); advErr != nil {
			return advErr
		}
	}

	for _, sd := range subDirs {
		if subErr := mp.collectSingleSideSubtree(actor, sd.ino, sd.path, origin); subErr != nil {
			return subErr
		}
	}
	return nil
}

func (mp *mergePlanner) finalizeInodesAndLedger(ctx context.Context, _ *StoreActor, opts *MergeOptions) error {
	// 收集所有被实际目录项引用的唯一节点与重算 Nlink
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

	// 计算目录 Nlink = 2 + liveSubdirs 及目录父子关系
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

	// 为存活节点分配 Target Ino：
	// 1) RootIno 与 originTarget 优先保留原 Target Ino；
	// 2) originFused 优先保留原 tgtOrigIno，若已被同一 Target 硬链接组中的其他存活分支声明（分裂），则分配新 Target Ino；
	// 3) originSource 在未冲突时保留原 Ino，否则重分配；
	// 4) DryRun 时不调用外部 InoAllocator（零副作用）。
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
		// 若 origIno 在 Target 原先存在但不属于本次保留，仅当它未被 claimed 时？
		// 按契约：!targetHasLiveIno(srcIno) 时保留 srcIno；若原 Target 已存在且与此次来源路径不同（异源冲突或分裂），则重新分配。
		if !alreadyClaimed && !wasLiveInTgt {
			spec.targetIno = origIno
			claimedInos[origIno] = struct{}{}
			continue
		}
		// 若 origIno 原本也是 Base 同源 Ino（即它没有被任何 originTarget/originFused 占用，且在 Target 中本属于同一路径但已被本次 Source 覆盖），保留之
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

	// 设置所有存活节点的 ParentIno（非目录统一置 0，非根目录置真实合并后父目录 Ino）与 WriteSeq
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
) (retErr error) {
	if err := CloneCheckpointDir(ctx, targetDir, opts.StagingDir); err != nil {
		return err
	}
	stagingDB, err := pebble.Open(opts.StagingDir, NewPebbleOptions(cache, false))
	if err != nil {
		return wrapIOErr(err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			retErr = errors.Join(retErr, wrapIOErr(stagingDB.Close()))
		}
	}()

	// 加载 Target 和 Source 的 PackMeta，以及扫描 StagingDir 现有 pack 文件
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
	defer func() {
		if batch != nil {
			retErr = errors.Join(retErr, wrapIOErr(batch.Close()))
		}
	}()

	// 清理全部 Target 孤儿与死亡（被删除或替换）的 Target Inode 及其 DataKey/XattrKey/EntryKey
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

	// 对需要保留/重用的 Target 存活数据块：先登记其 (PackID, Offset)，并对 payload 被 Source 覆盖的节点回收旧 Target DataKey
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
			// payload 切换为 Source：回收原 Target DataKey 并扣减 LiveBytes
			if err := reclaimDataPrefix(stagingDB, batch, spec.targetIno, packLiveBytes); err != nil {
				return err
			}
		}
	}

	// 导入数据块（Source 节点及硬链接分裂重分配 Target Ino 的节点）：处理 os.SameFile 复用、异源 PackID 碰撞重映射及分裂多重引用重映射
	srcPacksDir := filepath.Join(sourceDir, "packs")
	tgtPacksDir := filepath.Join(targetDir, "packs")
	stagingPacksDir := filepath.Join(opts.StagingDir, "packs")
	srcDefaultPackMap := make(map[uint64]uint64)
	tgtDefaultPackMap := make(map[uint64]uint64)
	splitPackMap := make(map[splitPackKey]uint64)
	linksToCreate := make(map[uint64]string) // dstPackID -> srcPackPath

	importSpecData := func(srcDB *pebble.DB, srcPacksDir string, srcStats map[uint64]PackMeta, defaultPackMap map[uint64]uint64, srcIno, targetIno uint64) error {
		return forEachDataKey(srcDB, srcIno, func(blockIdx uint64, ref BlobRef) error {
			srcPackPath := packFilePath(srcPacksDir, ref.PackID)
			mappedPackID, ok := defaultPackMap[ref.PackID]
			if !ok {
				tgtPackPath := packFilePath(stagingPacksDir, ref.PackID)
				tgtInfo, tStatErr := os.Stat(tgtPackPath)
				srcInfo, sStatErr := os.Stat(srcPackPath)
				if sStatErr != nil {
					return wrapIOErr(sStatErr)
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
					return wrapIOErr(tStatErr)
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
						return wrapIOErr(sStatErr)
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

	// 硬链接导入新/重映射 Pack 文件并 fsync packs 目录
	if len(linksToCreate) > 0 {
		if mkErr := os.MkdirAll(stagingPacksDir, 0o755); mkErr != nil {
			return wrapIOErr(mkErr)
		}
	}
	for dstPackID, srcPackPath := range linksToCreate {
		dstPackPath := packFilePath(stagingPacksDir, dstPackID)
		if linkErr := os.Link(srcPackPath, dstPackPath); linkErr != nil {
			if !errors.Is(linkErr, os.ErrExist) {
				return wrapIOErr(linkErr)
			}
			srcInfo, sErr := os.Stat(srcPackPath)
			if sErr != nil {
				return wrapIOErr(sErr)
			}
			dstInfo, dErr := os.Stat(dstPackPath)
			if dErr != nil {
				return wrapIOErr(dErr)
			}
			if !os.SameFile(srcInfo, dstInfo) {
				return wrapIOErr(linkErr)
			}
		}
	}
	if len(linksToCreate) > 0 {
		if err := syncDirPath(stagingPacksDir); err != nil {
			return err
		}
	}

	// 写入所有存活 Inode、目录项与 Xattr
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
			// 重建该存活目录的直接子项表（先清理现有 EntryPrefix，再写入合并后子项）
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

	// 更新 sys:pack 账本并删除 0 活跃 Pack 文件
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

	// 持久化 sys:stat（WriterEpoch = ExpectedWriterEpoch + 1）
	BatchSetStatDeferred(batch, BranchStat{
		LogicalBytes: result.UsedBytes,
		Inodes:       result.Inodes,
		NextWriteSeq: result.NextWriteSeq,
		MaxBytes:     result.TargetMaxBytes,
		WriterEpoch:  result.WriterEpoch,
	})

	if commitErr := batch.Commit(pebble.Sync); commitErr != nil {
		return wrapIOErr(commitErr)
	}
	closeBatchErr := wrapIOErr(batch.Close())
	batch = nil
	if closeBatchErr != nil {
		return closeBatchErr
	}
	if flushErr := stagingDB.Flush(); flushErr != nil {
		return wrapIOErr(flushErr)
	}
	dbClosed = true
	if closeDBErr := stagingDB.Close(); closeDBErr != nil {
		return wrapIOErr(closeDBErr)
	}

	for _, pid := range deadPackIDs {
		p := packFilePath(stagingPacksDir, pid)
		if rmErr := os.Remove(p); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			return wrapIOErr(rmErr)
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
	iter, openErr := db.NewIter(nil)
	if openErr != nil {
		return nil, 0, wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
		if err != nil {
			stats = nil
			maxPID = 0
		}
	}()

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
	return stats, maxPID, nil
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

func reclaimDataPrefix(db *pebble.DB, batch *pebble.Batch, ino uint64, packLiveBytes map[uint64]uint64) (err error) {
	prefix := DataPrefix(ino)
	iter, openErr := db.NewIter(nil)
	if openErr != nil {
		return wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
	}()

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
	return nil
}

func deleteKeyPrefix(db *pebble.DB, batch *pebble.Batch, prefix []byte) (err error) {
	iter, openErr := db.NewIter(nil)
	if openErr != nil {
		return wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
	}()

	for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		BatchDeleteDeferred(batch, iter.Key())
	}
	return nil
}

func scanExistingDataOffsets(db *pebble.DB, ino uint64, used map[packOffsetKey]bool) error {
	return forEachDataKey(db, ino, func(_ uint64, ref BlobRef) error {
		used[packOffsetKey{packID: ref.PackID, offset: ref.Offset}] = true
		return nil
	})
}

func forEachDataKey(db *pebble.DB, ino uint64, fn func(blockIdx uint64, ref BlobRef) error) (err error) {
	prefix := DataPrefix(ino)
	iter, openErr := db.NewIter(nil)
	if openErr != nil {
		return wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
	}()

	for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		_, idx, decKeyErr := DecodeDataKey(iter.Key())
		if decKeyErr != nil {
			return decKeyErr
		}
		ref, decValErr := DecodeBlobRef(iter.Value())
		if decValErr != nil {
			return decValErr
		}
		if fnErr := fn(idx, ref); fnErr != nil {
			return fnErr
		}
	}
	return nil
}
