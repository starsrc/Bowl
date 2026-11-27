package engine

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

func (d *DiskStore) Merge(ctx context.Context, opts MergeOptions) (*MergeResult, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateBranchName(opts.SourceBranch); err != nil {
		return nil, err
	}
	if err := validateBranchName(opts.TargetBranch); err != nil {
		return nil, err
	}
	if opts.Strategy < MergeStrategyUnspecified || opts.Strategy > MergeStrategyTheirs {
		return nil, ErrInvalid
	}
	normStrategies, err := normalizePathStrategyMap(opts.PathStrategies)
	if err != nil {
		return nil, err
	}

	if opts.DryRun {
		return d.mergeDryRun(ctx, opts, normStrategies)
	}

	if lockErr := d.lockGCShared(ctx); lockErr != nil {
		return nil, lockErr
	}
	defer d.unlockGCShared()

	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()

	var res *MergeResult
	err = d.execBranchOp(ctx, opts.TargetBranch, true, true, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, _ *branchLayerCursor) error {
		var mErr error
		res, mErr = d.execMerge(batch, bm, opts, normStrategies)
		return mErr
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *DiskStore) mergeDryRun(ctx context.Context, opts MergeOptions, normStrategies map[string]MergeStrategy) (*MergeResult, error) {
	dk := d.keys
	rv := d.openReadView()
	defer func() { _ = rv.Close() }()

	tgtBranch, err := readBranchMeta(rv.snap, dk, opts.TargetBranch)
	if err != nil {
		return nil, err
	}
	tgtDirty, err := layerHasAnyKey(rv.snap, dk, tgtBranch.WorkLayerID)
	if err != nil {
		return nil, err
	}
	if tgtDirty {
		return nil, ErrDirtyWorkLayer
	}

	srcBranch := tgtBranch
	if opts.SourceBranch != opts.TargetBranch {
		srcBranch, err = readBranchMeta(rv.snap, dk, opts.SourceBranch)
		if err != nil {
			return nil, err
		}
		srcDirty, sErr := layerHasAnyKey(rv.snap, dk, srcBranch.WorkLayerID)
		if sErr != nil {
			return nil, sErr
		}
		if srcDirty {
			return nil, ErrDirtyWorkLayer
		}
	}

	lca, err := findLCA(rv.snap, dk, srcBranch.HeadCommitID, tgtBranch.HeadCommitID)
	if err != nil {
		return nil, err
	}

	if lca == srcBranch.HeadCommitID {
		return &MergeResult{
			CommitID:       tgtBranch.HeadCommitID,
			BaseCommitID:   lca,
			TargetCommitID: tgtBranch.HeadCommitID,
			SourceCommitID: srcBranch.HeadCommitID,
			UpToDate:       true,
			WriterEpoch:    tgtBranch.WriterEpoch,
		}, nil
	}

	if lca == tgtBranch.HeadCommitID && !opts.NoFastForward {
		return &MergeResult{
			CommitID:       srcBranch.HeadCommitID,
			BaseCommitID:   lca,
			TargetCommitID: tgtBranch.HeadCommitID,
			SourceCommitID: srcBranch.HeadCommitID,
			FastForward:    true,
			WriterEpoch:    tgtBranch.WriterEpoch + 1,
		}, nil
	}

	_, baseLayers, err := d.resolveCommitLayersWithGen(rv.snap, lca, rv.snapGen)
	if err != nil {
		return nil, err
	}
	targetIsBase := tgtBranch.HeadCommitID == lca
	targetLayers := baseLayers
	if !targetIsBase {
		_, targetLayers, err = d.resolveCommitLayersWithGen(rv.snap, tgtBranch.HeadCommitID, rv.snapGen)
		if err != nil {
			return nil, err
		}
	}
	_, sourceLayers, err := d.resolveCommitLayersWithGen(rv.snap, srcBranch.HeadCommitID, rv.snapGen)
	if err != nil {
		return nil, err
	}

	plan, err := d.planDeltaMerge(
		ctx,
		rv,
		lca,
		layerView{r: rv.snap, keys: dk, layers: baseLayers},
		layerView{r: rv.snap, keys: dk, layers: targetLayers},
		layerView{r: rv.snap, keys: dk, layers: sourceLayers},
		targetIsBase,
		opts,
		normStrategies,
		true,
	)
	if err != nil {
		return nil, err
	}
	return &MergeResult{
		BaseCommitID:   lca,
		TargetCommitID: tgtBranch.HeadCommitID,
		SourceCommitID: srcBranch.HeadCommitID,
		WriterEpoch:    tgtBranch.WriterEpoch + 1,
		Summary:        plan.summary,
	}, nil
}

func (d *DiskStore) execMerge(batch *pebble.Batch, branch BranchMeta, opts MergeOptions, normStrategies map[string]MergeStrategy) (*MergeResult, error) {
	dk := d.keys
	gateNow := d.now().UnixNano()
	tgtMounted, err := hasActiveMountOnReader(batch, dk, branch.Name, gateNow, false)
	if err != nil {
		return nil, err
	}
	if tgtMounted {
		return nil, ErrDiskMounted
	}
	if opts.SourceBranch != branch.Name {
		srcMounted, srcMountErr := hasActiveMountOnReader(batch, dk, opts.SourceBranch, gateNow, true)
		if srcMountErr != nil {
			return nil, srcMountErr
		}
		if srcMounted {
			return nil, ErrDiskMounted
		}
	}

	if d.beforeMergeExecHook != nil {
		if hookErr := d.beforeMergeExecHook(opts.SourceBranch, branch.Name); hookErr != nil {
			return nil, hookErr
		}
	}

	rv := d.openReadView()
	defer func() { _ = rv.Close() }()

	tgtDirty, err := layerHasAnyKey(batch, dk, branch.WorkLayerID)
	if err != nil {
		return nil, err
	}
	if tgtDirty {
		return nil, ErrDirtyWorkLayer
	}

	srcBranch := branch
	srcReader := pebbleReader(rv.snap)
	if opts.SourceBranch == branch.Name {
		srcReader = batch
	} else {
		srcBranch, err = readBranchMeta(rv.snap, dk, opts.SourceBranch)
		if err != nil {
			return nil, err
		}
		srcDirty, sErr := layerHasAnyKey(rv.snap, dk, srcBranch.WorkLayerID)
		if sErr != nil {
			return nil, sErr
		}
		if srcDirty {
			return nil, ErrDirtyWorkLayer
		}
	}

	lca, err := findLCA(batch, dk, srcBranch.HeadCommitID, branch.HeadCommitID)
	if err != nil {
		return nil, err
	}

	oldTargetHead := branch.HeadCommitID
	if lca == srcBranch.HeadCommitID {
		return &MergeResult{
			CommitID:       oldTargetHead,
			BaseCommitID:   lca,
			TargetCommitID: oldTargetHead,
			SourceCommitID: srcBranch.HeadCommitID,
			UpToDate:       true,
			WriterEpoch:    branch.WriterEpoch,
		}, nil
	}

	nowNano := d.now().UnixNano()
	if lca == oldTargetHead && !opts.NoFastForward {
		srcHeadCmt, cmtErr := readCommitMeta(srcReader, dk, srcBranch.HeadCommitID)
		if cmtErr != nil {
			return nil, cmtErr
		}
		branch.HeadCommitID = srcBranch.HeadCommitID
		branch.WriterEpoch++
		branch.WorkOwnedBytes = 0
		branch.LogicalBytes = srcHeadCmt.LogicalBytes
		branch.UpdatedAt = nowNano
		if setErr := batch.Set(dk.MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
			return nil, setErr
		}
		return &MergeResult{
			CommitID:       srcBranch.HeadCommitID,
			BaseCommitID:   lca,
			TargetCommitID: oldTargetHead,
			SourceCommitID: srcBranch.HeadCommitID,
			FastForward:    true,
			WriterEpoch:    branch.WriterEpoch,
		}, nil
	}

	targetHeadCmt, targetLayers, err := d.resolveCommitLayersWithGen(batch, oldTargetHead, rv.snapGen)
	if err != nil {
		return nil, err
	}
	targetIsBase := oldTargetHead == lca
	var baseView layerView
	targetView := layerView{r: batch, keys: dk, layers: targetLayers}
	if targetIsBase {
		baseView = targetView
	} else {
		_, baseLayers, bErr := d.resolveCommitLayersWithGen(rv.snap, lca, rv.snapGen)
		if bErr != nil {
			return nil, bErr
		}
		baseView = layerView{r: rv.snap, keys: dk, layers: baseLayers}
	}
	_, sourceLayers, err := d.resolveCommitLayersWithGen(srcReader, srcBranch.HeadCommitID, rv.snapGen)
	if err != nil {
		return nil, err
	}
	sourceView := layerView{r: srcReader, keys: dk, layers: sourceLayers}

	plan, err := d.planDeltaMerge(
		context.Background(),
		rv,
		lca,
		baseView,
		targetView,
		sourceView,
		targetIsBase,
		opts,
		normStrategies,
		false,
	)
	if err != nil {
		return nil, err
	}

	commitID, err := d.allocCommitID()
	if err != nil {
		return nil, err
	}

	var mergeLayerID uint64
	if len(plan.deltaKV) > 0 {
		mergeLayerID, err = d.allocLayerID()
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(plan.deltaKV))
		for k := range plan.deltaKV {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			rawKey := []byte(k)
			dk.setLayerID(rawKey, mergeLayerID)
			if setErr := batch.Set(rawKey, plan.deltaKV[k], nil); setErr != nil {
				return nil, setErr
			}
		}
	}

	msg := opts.Message
	if msg == "" {
		msg = "merge " + opts.SourceBranch + " into " + branch.Name
	}
	mergeLogical := applySignedDelta(targetHeadCmt.LogicalBytes, plan.logicalDelta)
	cmt := CommitMeta{
		ID:            commitID,
		ParentID:      oldTargetHead,
		MergeParentID: srcBranch.HeadCommitID,
		LayerID:       mergeLayerID,
		Kind:          CommitKindMerge,
		OwnedBytes:    0,
		LogicalBytes:  mergeLogical,
		Message:       msg,
		CreatedAt:     nowNano,
	}
	branch.HeadCommitID = commitID
	branch.WriterEpoch++
	branch.WorkOwnedBytes = 0
	branch.LogicalBytes = mergeLogical
	branch.UpdatedAt = nowNano

	if setErr := batch.Set(dk.MetaCommitKey(commitID), EncodeCommitMeta(cmt), nil); setErr != nil {
		return nil, setErr
	}
	if setErr := batch.Set(dk.MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
		return nil, setErr
	}

	return &MergeResult{
		CommitID:       commitID,
		BaseCommitID:   lca,
		TargetCommitID: oldTargetHead,
		SourceCommitID: srcBranch.HeadCommitID,
		WriterEpoch:    branch.WriterEpoch,
		Summary:        plan.summary,
	}, nil
}

type mergeWalkRecord struct {
	an        *analyzedNode
	parentDir string
	name      []byte
}

type resolvedAction uint8

const (
	actionKeepTarget resolvedAction = iota
	actionDeleteTarget
	actionAdoptSource
	actionFuse
)

type resolvedRecord struct {
	rec    *mergeWalkRecord
	action resolvedAction
}

type xattrDeltaMode uint8

const (
	xattrDeltaKeepTarget xattrDeltaMode = iota
	xattrDeltaCopySource
	xattrDeltaReplaceMap
	xattrDeltaCopyTarget
)

type plannedInodeWrite struct {
	finalIno            uint64
	isDir               bool
	existedInTarget     bool
	targetOldInode      Inode
	meta                Inode
	srcIno              uint64
	tgtOrigIno          uint64
	xattrMode           xattrDeltaMode
	mergedXattrs        map[string][]byte
	writeDataFromSource bool
	writeDataFromTarget bool
	payloadChanged      bool
	srcWriteSeq         uint64
	everLinked          bool
	parentIno           uint64
}

type deltaMergePlan struct {
	summary      DiffSummary
	deltaKV      map[string][]byte
	logicalDelta int64
}

type deltaMergeBuilder struct {
	ctx             context.Context
	disk            *DiskStore
	tc              *treeCompareCtx
	dryRun          bool
	nowNano         int64
	conflicts       []MergeConflict
	summary         DiffSummary
	deltaKV         map[string][]byte
	logicalDelta    int64
	dirInoByPath    map[string]uint64
	keptTargetInos  map[uint64]bool
	claimedInos     map[uint64]bool
	adoptedSrcIno   map[uint64]uint64
	splitSrcIno     map[uint64]uint64
	plannedInodes   map[uint64]*plannedInodeWrite
	nlinkDelta      map[uint64]int64
	dirSubdirDelta  map[uint64]int64
	dirtyParentMeta map[uint64]bool
	tombstonedInos  map[uint64]bool
	entryParentDirs map[uint64]map[uint64]struct{}
	dryRunNextIno   uint64
}

func (d *DiskStore) planDeltaMerge(
	ctx context.Context,
	rv *readView,
	lcaCommitID uint64,
	baseView, targetView, sourceView layerView,
	targetIsBase bool,
	opts MergeOptions,
	normStrategies map[string]MergeStrategy,
	dryRun bool,
) (*deltaMergePlan, error) {
	dirty, err := discoverDirtySet(baseView, targetView, sourceView, targetIsBase)
	if err != nil {
		return nil, err
	}
	if len(dirty.dirtyDirs) == 0 && len(dirty.dirtyInos) == 0 {
		return &deltaMergePlan{deltaKV: make(map[string][]byte)}, nil
	}

	tc := newTreeCompareCtx(ctx, d, rv, baseView, targetView, sourceView, targetIsBase, dirty)
	b := &deltaMergeBuilder{
		ctx:             ctx,
		disk:            d,
		tc:              tc,
		dryRun:          dryRun,
		nowNano:         d.now().UnixNano(),
		deltaKV:         make(map[string][]byte),
		dirInoByPath:    map[string]uint64{"": RootIno},
		keptTargetInos:  make(map[uint64]bool),
		claimedInos:     map[uint64]bool{RootIno: true},
		adoptedSrcIno:   make(map[uint64]uint64),
		splitSrcIno:     make(map[uint64]uint64),
		plannedInodes:   make(map[uint64]*plannedInodeWrite),
		nlinkDelta:      make(map[uint64]int64),
		dirSubdirDelta:  make(map[uint64]int64),
		dirtyParentMeta: make(map[uint64]bool),
		tombstonedInos:  make(map[uint64]bool),
		entryParentDirs: make(map[uint64]map[uint64]struct{}),
		dryRunNextIno:   1 << 60,
	}

	if rootErr := b.analyzeRoot(opts.Strategy, normStrategies); rootErr != nil {
		return nil, rootErr
	}

	var records []*mergeWalkRecord
	hlTracker := newTargetHardlinkTracker()

	walkErr := tc.walkAnalyzedTree(nil, false, func(an *analyzedNode) error {
		if obsErr := hlTracker.observe(tc, an); obsErr != nil {
			return obsErr
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

	if !targetIsBase && hlTracker.hasCandidates() {
		for _, rec := range records {
			an := rec.an
			var checkIno uint64
			if an.oNode != nil {
				checkIno = an.oNode.Ino
			} else if an.tNode != nil && an.sNode == nil {
				checkIno = an.tNode.Ino
			}
			if !hlTracker.isConflicting(checkIno) {
				continue
			}
			if an.conflict == ConflictNone {
				an.conflict = ConflictHardlink
				an.decision = nodeDecisionConflict
				an.emitDiff = true
				if an.kind == DiffKindUnspecified {
					an.kind = DiffKindAdded
				}
			}
		}
	}

	resolved := make([]resolvedRecord, 0, len(records))
	var skippedSubtreePrefixes []string

	for _, rec := range records {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		an := rec.an
		pathStr := string(an.path)
		if isUnderAnyPrefix(pathStr, skippedSubtreePrefixes) {
			continue
		}

		if an.emitDiff {
			switch an.kind {
			case DiffKindAdded:
				b.summary.Added++
			case DiffKindModified:
				b.summary.Modified++
			case DiffKindDeleted:
				b.summary.Deleted++
			}
			if an.conflict != ConflictNone {
				b.summary.Conflicts++
			}
		}

		switch an.decision {
		case nodeDecisionSkip:
			continue

		case nodeDecisionKeepTarget:
			if an.tNode != nil {
				if (an.tNode.Mode & ModeTypeMask) == ModeDir {
					b.dirInoByPath[pathStr] = an.tNode.Ino
				} else {
					b.keptTargetInos[an.tNode.Ino] = true
				}
				b.claimedInos[an.tNode.Ino] = true
			}
			resolved = append(resolved, resolvedRecord{rec: rec, action: actionKeepTarget})

		case nodeDecisionDeleteTarget:
			if an.tNode != nil && (an.tNode.Mode&ModeTypeMask) == ModeDir {
				skippedSubtreePrefixes = append(skippedSubtreePrefixes, pathStr)
			}
			resolved = append(resolved, resolvedRecord{rec: rec, action: actionDeleteTarget})

		case nodeDecisionAdoptSource:
			if an.tNode != nil && (an.tNode.Mode&ModeTypeMask) == ModeDir && an.sNode != nil && (an.sNode.Mode&ModeTypeMask) != ModeDir {
				skippedSubtreePrefixes = append(skippedSubtreePrefixes, pathStr)
			}
			resolved = append(resolved, resolvedRecord{rec: rec, action: actionAdoptSource})

		case nodeDecisionFuse:
			if an.tNode != nil && an.isDir {
				b.dirInoByPath[pathStr] = an.tNode.Ino
				b.claimedInos[an.tNode.Ino] = true
			}
			resolved = append(resolved, resolvedRecord{rec: rec, action: actionFuse})

		case nodeDecisionConflict:
			strategy := resolveMergeStrategy(opts.Strategy, normStrategies, an.path)
			switch strategy {
			case MergeStrategyFail:
				b.conflicts = append(b.conflicts, MergeConflict{
					Path:   append([]byte(nil), an.path...),
					Kind:   an.conflict,
					Base:   makeNodeSummary(an.oNode),
					Target: makeNodeSummary(an.tNode),
					Source: makeNodeSummary(an.sNode),
				})
			case MergeStrategyOurs:
				if an.tNode != nil {
					if (an.tNode.Mode & ModeTypeMask) == ModeDir {
						b.dirInoByPath[pathStr] = an.tNode.Ino
					} else {
						b.keptTargetInos[an.tNode.Ino] = true
					}
					b.claimedInos[an.tNode.Ino] = true
				}
				resolved = append(resolved, resolvedRecord{rec: rec, action: actionKeepTarget})
			case MergeStrategyTheirs:
				if an.sNode == nil {
					if an.tNode != nil && (an.tNode.Mode&ModeTypeMask) == ModeDir {
						skippedSubtreePrefixes = append(skippedSubtreePrefixes, pathStr)
					}
					resolved = append(resolved, resolvedRecord{rec: rec, action: actionDeleteTarget})
				} else {
					if an.tNode != nil && (an.tNode.Mode&ModeTypeMask) == ModeDir && (an.sNode.Mode&ModeTypeMask) != ModeDir {
						skippedSubtreePrefixes = append(skippedSubtreePrefixes, pathStr)
					}
					resolved = append(resolved, resolvedRecord{rec: rec, action: actionAdoptSource})
				}
			}
		}
	}

	if len(b.conflicts) > 0 {
		sort.Slice(b.conflicts, func(i, j int) bool {
			return compareDiffPath(b.conflicts[i].Path, b.conflicts[j].Path) < 0
		})
		total := uint64(len(b.conflicts))
		samples := b.conflicts
		truncated := false
		if len(samples) > MaxMergeConflictSamples {
			samples = samples[:MaxMergeConflictSamples]
			truncated = true
		}
		return nil, &ErrMergeConflict{
			Report: MergeConflictReport{
				LCACommitID:        lcaCommitID,
				TotalConflicts:     total,
				Conflicts:          samples,
				ConflictsTruncated: truncated,
			},
		}
	}

	if dryRun {
		return &deltaMergePlan{
			summary: b.summary,
			deltaKV: b.deltaKV,
		}, nil
	}

	for _, rr := range resolved {
		if applyErr := b.applyResolvedRecord(rr); applyErr != nil {
			return nil, applyErr
		}
	}

	if matErr := b.materializeDeltaKV(); matErr != nil {
		return nil, matErr
	}

	return &deltaMergePlan{
		summary:      b.summary,
		deltaKV:      b.deltaKV,
		logicalDelta: b.logicalDelta,
	}, nil
}

func (b *deltaMergeBuilder) allocIno() (uint64, error) {
	if b.dryRun {
		b.dryRunNextIno++
		return b.dryRunNextIno, nil
	}
	return b.disk.allocIno()
}

func (b *deltaMergeBuilder) putDelta(key, val []byte) {
	b.deltaKV[string(key)] = val
}

func (b *deltaMergeBuilder) recordEntryParent(ino, parentIno uint64) {
	if ino == 0 || parentIno == 0 {
		return
	}
	pm := b.entryParentDirs[ino]
	if pm == nil {
		pm = make(map[uint64]struct{})
		b.entryParentDirs[ino] = pm
	}
	pm[parentIno] = struct{}{}
}

func copyInodeMeta(dst, src *Inode) {
	everLinked := (dst.Flags | src.Flags) & InodeFlagEverLinked
	dst.Mode = (dst.Mode & ModeTypeMask) | (src.Mode & ModePermMask)
	dst.UID = src.UID
	dst.GID = src.GID
	dst.Flags = userInodeFlags(src.Flags) | everLinked
	dst.Mtime = src.Mtime
	dst.Ctime = src.Ctime
}

func (b *deltaMergeBuilder) analyzeRoot(global MergeStrategy, byPath map[string]MergeStrategy) error {
	rootAN, err := b.tc.analyzeRootInode(true)
	if err != nil {
		return err
	}
	if rootAN == nil || !rootAN.emitDiff {
		return nil
	}
	bRoot := *rootAN.oNode
	tRoot := *rootAN.tNode
	sRoot := *rootAN.sNode
	b.summary.Modified++
	if rootAN.conflict != ConflictNone {
		b.summary.Conflicts++
	}

	switch rootAN.decision {
	case nodeDecisionAdoptSource:
		meta := tRoot
		copyInodeMeta(&meta, &sRoot)
		b.plannedInodes[RootIno] = &plannedInodeWrite{
			finalIno:        RootIno,
			isDir:           true,
			existedInTarget: true,
			targetOldInode:  tRoot,
			meta:            meta,
			srcIno:          RootIno,
			xattrMode:       xattrDeltaCopySource,
			parentIno:       RootIno,
		}
	case nodeDecisionFuse:
		b.plannedInodes[RootIno] = &plannedInodeWrite{
			finalIno:        RootIno,
			isDir:           true,
			existedInTarget: true,
			targetOldInode:  tRoot,
			meta:            rootAN.mergedMeta,
			srcIno:          RootIno,
			xattrMode:       xattrDeltaReplaceMap,
			mergedXattrs:    rootAN.mergedXattrs,
			parentIno:       RootIno,
		}
	case nodeDecisionConflict:
		strategy := resolveMergeStrategy(global, byPath, []byte("/"))
		switch strategy {
		case MergeStrategyFail:
			b.conflicts = append(b.conflicts, MergeConflict{
				Path:   []byte("/"),
				Kind:   rootAN.conflict,
				Base:   makeNodeSummary(&bRoot),
				Target: makeNodeSummary(&tRoot),
				Source: makeNodeSummary(&sRoot),
			})
		case MergeStrategyOurs:
		case MergeStrategyTheirs:
			meta := tRoot
			copyInodeMeta(&meta, &sRoot)
			b.plannedInodes[RootIno] = &plannedInodeWrite{
				finalIno:        RootIno,
				isDir:           true,
				existedInTarget: true,
				targetOldInode:  tRoot,
				meta:            meta,
				srcIno:          RootIno,
				xattrMode:       xattrDeltaCopySource,
				parentIno:       RootIno,
			}
		}
	}
	return nil
}

func (b *deltaMergeBuilder) resolveParentDirIno(parentPath string) (uint64, error) {
	if ino, ok := b.dirInoByPath[parentPath]; ok {
		return ino, nil
	}
	if parentPath == "" {
		return RootIno, nil
	}
	cur := RootIno
	for seg := range strings.SplitSeq(parentPath, "/") {
		ent, err := overlayGetEntry(b.tc.target.r, b.tc.target.keys, b.tc.target.layers, cur, []byte(seg))
		if err != nil {
			return 0, err
		}
		cur = ent.Ino
	}
	b.dirInoByPath[parentPath] = cur
	return cur, nil
}

func (b *deltaMergeBuilder) applyResolvedRecord(rr resolvedRecord) error {
	rec := rr.rec
	an := rec.an
	pathStr := string(an.path)
	dk := b.disk.keys

	switch rr.action {
	case actionKeepTarget:
		return nil

	case actionDeleteTarget:
		if an.tNode == nil {
			return nil
		}
		parentIno, err := b.resolveParentDirIno(rec.parentDir)
		if err != nil {
			return err
		}
		b.putDelta(dk.LayerEntryKey(0, parentIno, rec.name), EncodeTombstone())
		b.dirtyParentMeta[parentIno] = true
		if (an.tNode.Mode & ModeTypeMask) == ModeDir {
			b.dirSubdirDelta[parentIno]--
			return b.deleteTargetSubtree(an.tNode.Ino)
		}
		b.nlinkDelta[an.tNode.Ino]--
		return nil

	case actionAdoptSource:
		if an.sNode == nil {
			return nil
		}
		parentIno, err := b.resolveParentDirIno(rec.parentDir)
		if err != nil {
			return err
		}
		sIsDir := (an.sNode.Mode & ModeTypeMask) == ModeDir
		tIsDir := an.tNode != nil && (an.tNode.Mode&ModeTypeMask) == ModeDir

		if sIsDir && tIsDir && an.descend {
			b.dirInoByPath[pathStr] = an.tNode.Ino
			b.claimedInos[an.tNode.Ino] = true
			meta := *an.tNode
			copyInodeMeta(&meta, an.sNode)
			b.plannedInodes[an.tNode.Ino] = &plannedInodeWrite{
				finalIno:        an.tNode.Ino,
				isDir:           true,
				existedInTarget: true,
				targetOldInode:  *an.tNode,
				meta:            meta,
				srcIno:          an.sNode.Ino,
				xattrMode:       xattrDeltaCopySource,
				parentIno:       parentIno,
			}
			if an.tNode.Mode != meta.Mode {
				b.putDelta(dk.LayerEntryKey(0, parentIno, rec.name), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: an.tNode.Ino, Mode: meta.Mode})))
			}
			return nil
		}

		if sIsDir {
			if an.tNode != nil {
				if tIsDir {
					b.dirSubdirDelta[parentIno]--
					if delErr := b.deleteTargetSubtree(an.tNode.Ino); delErr != nil {
						return delErr
					}
				} else {
					b.nlinkDelta[an.tNode.Ino]--
				}
			}
			finalDirIno, allocErr := b.pickDirInoForSource(an.sNode.Ino)
			if allocErr != nil {
				return allocErr
			}
			b.dirInoByPath[pathStr] = finalDirIno
			b.claimedInos[finalDirIno] = true
			b.putDelta(dk.LayerEntryKey(0, parentIno, rec.name), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: finalDirIno, Mode: an.sNode.Mode})))
			b.dirSubdirDelta[parentIno]++
			b.dirtyParentMeta[parentIno] = true
			meta := *an.sNode
			meta.Ino = finalDirIno
			meta.ParentIno = parentIno
			b.plannedInodes[finalDirIno] = &plannedInodeWrite{
				finalIno:        finalDirIno,
				isDir:           true,
				existedInTarget: false,
				meta:            meta,
				srcIno:          an.sNode.Ino,
				xattrMode:       xattrDeltaCopySource,
				parentIno:       parentIno,
			}
			if !an.descend {
				return b.adoptSourceSubtree(an.sNode.Ino, finalDirIno, pathStr)
			}
			return nil
		}

		if tIsDir {
			b.dirSubdirDelta[parentIno]--
			if delErr := b.deleteTargetSubtree(an.tNode.Ino); delErr != nil {
				return delErr
			}
		}

		return b.adoptSourceNonDir(parentIno, rec.name, an.tNode, an.sNode, tIsDir)

	case actionFuse:
		parentIno, err := b.resolveParentDirIno(rec.parentDir)
		if err != nil {
			return err
		}
		if an.isDir {
			b.dirInoByPath[pathStr] = an.tNode.Ino
			b.claimedInos[an.tNode.Ino] = true
			xm := xattrDeltaKeepTarget
			if an.mergedXattrs != nil {
				xm = xattrDeltaReplaceMap
			}
			if !metaEqual(an.tNode, &an.mergedMeta) || xm == xattrDeltaReplaceMap {
				b.plannedInodes[an.tNode.Ino] = &plannedInodeWrite{
					finalIno:        an.tNode.Ino,
					isDir:           true,
					existedInTarget: true,
					targetOldInode:  *an.tNode,
					meta:            an.mergedMeta,
					srcIno:          an.sNode.Ino,
					xattrMode:       xm,
					mergedXattrs:    an.mergedXattrs,
					parentIno:       parentIno,
				}
				if an.tNode.Mode != an.mergedMeta.Mode {
					b.putDelta(dk.LayerEntryKey(0, parentIno, rec.name), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: an.tNode.Ino, Mode: an.mergedMeta.Mode})))
				}
			}
			return nil
		}
		return b.fuseNonDir(parentIno, rec.name, an)
	}
	return nil
}

func (b *deltaMergeBuilder) pickDirInoForSource(srcDirIno uint64) (uint64, error) {
	if !b.claimedInos[srcDirIno] {
		_, live, err := b.tc.target.getInode(srcDirIno)
		if err != nil {
			return 0, err
		}
		if !live {
			return srcDirIno, nil
		}
	}
	return b.allocIno()
}

func (b *deltaMergeBuilder) adoptSourceNonDir(parentIno uint64, name []byte, tNode, sNode *Inode, tWasDir bool) error {
	var oldTgtIno uint64
	if tNode != nil && !tWasDir {
		oldTgtIno = tNode.Ino
	}

	finalIno, existedInTarget, tgtOldInode, needContentWrite, err := b.resolveAdoptedNonDirIno(tNode, sNode, tWasDir)
	if err != nil {
		return err
	}

	b.claimedInos[finalIno] = true
	b.recordEntryParent(finalIno, parentIno)

	if oldTgtIno != finalIno || tNode == nil || tNode.Mode != sNode.Mode {
		b.putDelta(b.disk.keys.LayerEntryKey(0, parentIno, name), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: finalIno, Mode: sNode.Mode})))
		b.dirtyParentMeta[parentIno] = true
	}
	if oldTgtIno != 0 && oldTgtIno != finalIno {
		b.nlinkDelta[oldTgtIno]--
	}
	if oldTgtIno != finalIno {
		b.nlinkDelta[finalIno]++
	}

	if existing, ok := b.plannedInodes[finalIno]; ok {
		if sNode.Flags&InodeFlagEverLinked != 0 || sNode.Nlink > 1 {
			existing.everLinked = true
		}
		return nil
	}

	if !needContentWrite {
		return nil
	}

	meta := *sNode
	if sNode.SymlinkTarget != nil {
		meta.SymlinkTarget = append([]byte(nil), sNode.SymlinkTarget...)
	}
	if existedInTarget {
		meta.Flags |= tgtOldInode.Flags & InodeFlagEverLinked
	}
	meta.ParentIno = parentIno
	b.plannedInodes[finalIno] = &plannedInodeWrite{
		finalIno:            finalIno,
		isDir:               false,
		existedInTarget:     existedInTarget,
		targetOldInode:      tgtOldInode,
		meta:                meta,
		srcIno:              sNode.Ino,
		xattrMode:           xattrDeltaCopySource,
		writeDataFromSource: (sNode.Mode & ModeTypeMask) == ModeRegular,
		payloadChanged:      true,
		srcWriteSeq:         sNode.WriteSeq,
		everLinked:          sNode.Flags&InodeFlagEverLinked != 0 || sNode.Nlink > 1 || (existedInTarget && (tgtOldInode.Flags&InodeFlagEverLinked != 0 || tgtOldInode.Nlink > 1)),
		parentIno:           parentIno,
	}
	return nil
}

func (b *deltaMergeBuilder) resolveAdoptedNonDirIno(tNode, sNode *Inode, tWasDir bool) (finalIno uint64, existedInTarget bool, tgtOld Inode, needWrite bool, err error) {
	if tNode != nil && !tWasDir && tNode.Ino == sNode.Ino && !b.keptTargetInos[tNode.Ino] {
		return tNode.Ino, true, *tNode, true, nil
	}

	if mapped, ok := b.adoptedSrcIno[sNode.Ino]; ok {
		if existing, hasPlan := b.plannedInodes[mapped]; hasPlan {
			return mapped, existing.existedInTarget, existing.targetOldInode, false, nil
		}
		ti, live, tErr := b.tc.target.getInode(mapped)
		if tErr != nil {
			return 0, false, Inode{}, false, tErr
		}
		return mapped, live, ti, false, nil
	}

	if split, ok := b.splitSrcIno[sNode.Ino]; ok {
		return split, false, Inode{}, false, nil
	}

	tgtExisting, tgtLive, tErr := b.tc.target.getInode(sNode.Ino)
	if tErr != nil {
		return 0, false, Inode{}, false, tErr
	}
	if !tgtLive && !b.claimedInos[sNode.Ino] {
		b.adoptedSrcIno[sNode.Ino] = sNode.Ino
		return sNode.Ino, false, Inode{}, true, nil
	}
	if tgtLive {
		if !b.keptTargetInos[sNode.Ino] {
			b.adoptedSrcIno[sNode.Ino] = sNode.Ino
			return sNode.Ino, true, tgtExisting, true, nil
		}
		eq, eqErr := b.tc.nodesEqual(b.tc.target, &tgtExisting, b.tc.source, sNode)
		if eqErr != nil {
			return 0, false, Inode{}, false, eqErr
		}
		if eq {
			b.adoptedSrcIno[sNode.Ino] = sNode.Ino
			return sNode.Ino, true, tgtExisting, false, nil
		}
	}

	newIno, aErr := b.allocIno()
	if aErr != nil {
		return 0, false, Inode{}, false, aErr
	}
	b.splitSrcIno[sNode.Ino] = newIno
	return newIno, false, Inode{}, true, nil
}

func (b *deltaMergeBuilder) fuseNonDir(parentIno uint64, name []byte, an *analyzedNode) error {
	tNode := an.tNode
	sNode := an.sNode
	finalIno := tNode.Ino
	existedInTarget := true
	reallocated := false

	if b.keptTargetInos[tNode.Ino] {
		newIno, err := b.allocIno()
		if err != nil {
			return err
		}
		finalIno = newIno
		existedInTarget = false
		reallocated = true
	}

	b.claimedInos[finalIno] = true
	b.recordEntryParent(finalIno, parentIno)

	meta := an.mergedMeta
	if an.payloadFromSrc && sNode != nil {
		meta.Size = sNode.Size
		if sNode.SymlinkTarget != nil {
			meta.SymlinkTarget = append([]byte(nil), sNode.SymlinkTarget...)
		} else {
			meta.SymlinkTarget = nil
		}
	}
	if reallocated {
		meta.ParentIno = parentIno
		b.putDelta(b.disk.keys.LayerEntryKey(0, parentIno, name), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: finalIno, Mode: meta.Mode})))
		b.dirtyParentMeta[parentIno] = true
		b.nlinkDelta[tNode.Ino]--
		b.nlinkDelta[finalIno]++
	} else if tNode.Mode != meta.Mode {
		b.putDelta(b.disk.keys.LayerEntryKey(0, parentIno, name), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: finalIno, Mode: meta.Mode})))
		b.dirtyParentMeta[parentIno] = true
	}

	xm := xattrDeltaKeepTarget
	if an.mergedXattrs != nil {
		xm = xattrDeltaReplaceMap
	} else if reallocated {
		xm = xattrDeltaCopyTarget
	}

	var srcIno, srcWriteSeq uint64
	if sNode != nil {
		srcIno = sNode.Ino
		srcWriteSeq = sNode.WriteSeq
	}

	b.plannedInodes[finalIno] = &plannedInodeWrite{
		finalIno:            finalIno,
		isDir:               false,
		existedInTarget:     existedInTarget,
		targetOldInode:      *tNode,
		meta:                meta,
		srcIno:              srcIno,
		tgtOrigIno:          tNode.Ino,
		xattrMode:           xm,
		mergedXattrs:        an.mergedXattrs,
		writeDataFromSource: an.payloadFromSrc && (meta.Mode&ModeTypeMask) == ModeRegular,
		writeDataFromTarget: reallocated && !an.payloadFromSrc && (meta.Mode&ModeTypeMask) == ModeRegular,
		payloadChanged:      an.payloadFromSrc,
		srcWriteSeq:         srcWriteSeq,
		everLinked:          meta.Flags&InodeFlagEverLinked != 0 || tNode.Nlink > 1 || (sNode != nil && sNode.Nlink > 1),
		parentIno:           parentIno,
	}
	return nil
}

func (b *deltaMergeBuilder) adoptSourceSubtree(srcDirIno, dstDirIno uint64, dirPath string) error {
	entries, err := b.tc.source.readDir(srcDirIno)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		sNode, ok, rErr := b.tc.source.getInode(ent.Ino)
		if rErr != nil {
			return rErr
		}
		if !ok {
			continue
		}
		childPath := string(joinRelPath([]byte(dirPath), []byte(ent.Name)))
		if (sNode.Mode & ModeTypeMask) == ModeDir {
			subDstIno, aErr := b.pickDirInoForSource(sNode.Ino)
			if aErr != nil {
				return aErr
			}
			b.dirInoByPath[childPath] = subDstIno
			b.claimedInos[subDstIno] = true
			b.putDelta(b.disk.keys.LayerEntryKey(0, dstDirIno, []byte(ent.Name)), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: subDstIno, Mode: sNode.Mode})))
			b.dirSubdirDelta[dstDirIno]++
			meta := sNode
			meta.Ino = subDstIno
			meta.ParentIno = dstDirIno
			b.plannedInodes[subDstIno] = &plannedInodeWrite{
				finalIno:        subDstIno,
				isDir:           true,
				existedInTarget: false,
				meta:            meta,
				srcIno:          sNode.Ino,
				xattrMode:       xattrDeltaCopySource,
				parentIno:       dstDirIno,
			}
			if subErr := b.adoptSourceSubtree(sNode.Ino, subDstIno, childPath); subErr != nil {
				return subErr
			}
		} else {
			if fErr := b.adoptSourceNonDir(dstDirIno, []byte(ent.Name), nil, &sNode, false); fErr != nil {
				return fErr
			}
		}
	}
	return nil
}

func (b *deltaMergeBuilder) deleteTargetSubtree(dirIno uint64) error {
	if b.tombstonedInos[dirIno] {
		return nil
	}
	b.tombstonedInos[dirIno] = true
	dk := b.disk.keys

	entries, err := b.tc.target.readDir(dirIno)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		b.putDelta(dk.LayerEntryKey(0, dirIno, []byte(ent.Name)), EncodeTombstone())
		if (ent.Mode & ModeTypeMask) == ModeDir {
			if subErr := b.deleteTargetSubtree(ent.Ino); subErr != nil {
				return subErr
			}
		} else {
			b.nlinkDelta[ent.Ino]--
		}
	}

	b.putDelta(dk.LayerInodeKey(0, dirIno), EncodeTombstone())
	xattrs, err := b.tc.target.readAllXattrs(dirIno)
	if err != nil {
		return err
	}
	for k := range xattrs {
		b.putDelta(dk.LayerXattrKey(0, dirIno, []byte(k)), EncodeTombstone())
	}
	return nil
}

func (b *deltaMergeBuilder) materializeDeltaKV() error {
	dk := b.disk.keys
	for ino, delta := range b.nlinkDelta {
		if delta == 0 {
			continue
		}
		if _, planned := b.plannedInodes[ino]; planned {
			continue
		}
		tNode, ok, err := b.tc.target.getInode(ino)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		newNlink := int64(tNode.Nlink) + delta
		if newNlink <= 0 {
			if tbErr := b.tombstoneTargetNonDir(ino); tbErr != nil {
				return tbErr
			}
			continue
		}
		if newNlink > int64(MaxNlink) {
			return ErrTooManyLinks
		}
		tNode.Nlink = uint32(newNlink)
		if delta > 0 || tNode.Nlink > 1 {
			tNode.Flags |= InodeFlagEverLinked
		}
		tNode.Ctime = b.nowNano
		raw, err := EncodeInode(tNode)
		if err != nil {
			return err
		}
		b.putDelta(dk.LayerInodeKey(0, ino), EncodeLiveEnvelope(raw))
		if tNode.Flags&InodeFlagEverLinked != 0 {
			if tNode.ParentIno != 0 {
				b.putDelta(dk.LayerParentKey(0, ino, tNode.ParentIno), EncodeLiveEnvelope(nil))
			}
			for pDir := range b.entryParentDirs[ino] {
				b.putDelta(dk.LayerParentKey(0, ino, pDir), EncodeLiveEnvelope(nil))
			}
		}
	}

	for finalIno, w := range b.plannedInodes {
		if err := b.writePlannedInode(finalIno, w); err != nil {
			return err
		}
	}

	for dirIno := range b.dirtyParentMeta {
		if b.tombstonedInos[dirIno] {
			continue
		}
		if _, planned := b.plannedInodes[dirIno]; planned {
			continue
		}
		dNode, ok, err := b.tc.target.getInode(dirIno)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		newNlink := max(int64(dNode.Nlink)+b.dirSubdirDelta[dirIno], 2)
		if newNlink > int64(MaxNlink) {
			return ErrTooManyLinks
		}
		dNode.Nlink = uint32(newNlink)
		dNode.Mtime = b.nowNano
		dNode.Ctime = b.nowNano
		raw, err := EncodeInode(dNode)
		if err != nil {
			return err
		}
		b.putDelta(dk.LayerInodeKey(0, dirIno), EncodeLiveEnvelope(raw))
	}

	for srcIno, pSet := range b.tc.dirty.sourceDeltaParentKeys {
		targetIno := srcIno
		if splitIno, isSplit := b.splitSrcIno[srcIno]; isSplit {
			targetIno = splitIno
		} else if mappedIno, isMapped := b.adoptedSrcIno[srcIno]; isMapped {
			targetIno = mappedIno
		}
		if b.tombstonedInos[targetIno] {
			continue
		}
		_, planned := b.plannedInodes[targetIno]
		if !planned {
			_, live, err := b.tc.target.getInode(targetIno)
			if err != nil {
				return err
			}
			if !live {
				continue
			}
		}
		for pDir := range pSet {
			b.putDelta(dk.LayerParentKey(0, targetIno, pDir), EncodeLiveEnvelope(nil))
		}
	}

	return nil
}

func (b *deltaMergeBuilder) tombstoneTargetNonDir(ino uint64) error {
	if b.tombstonedInos[ino] {
		return nil
	}
	b.tombstonedInos[ino] = true
	dk := b.disk.keys
	b.putDelta(dk.LayerInodeKey(0, ino), EncodeTombstone())
	xattrs, err := b.tc.target.readAllXattrs(ino)
	if err != nil {
		return err
	}
	for k := range xattrs {
		b.putDelta(dk.LayerXattrKey(0, ino, []byte(k)), EncodeTombstone())
	}
	refs, err := b.tc.target.readAllDataRefs(ino)
	if err != nil {
		return err
	}
	for blkIdx, ref := range refs {
		b.putDelta(dk.LayerDataKey(0, ino, blkIdx), EncodeTombstone())
		b.logicalDelta -= int64(ref.Length)
	}
	return nil
}

func (b *deltaMergeBuilder) writePlannedInode(finalIno uint64, w *plannedInodeWrite) error {
	inode := w.meta
	inode.Ino = finalIno
	dk := b.disk.keys

	if w.isDir {
		var baseNlink int64 = 2
		if w.existedInTarget {
			baseNlink = int64(w.targetOldInode.Nlink)
		}
		newNlink := max(baseNlink+b.dirSubdirDelta[finalIno], 2)
		if newNlink > int64(MaxNlink) {
			return ErrTooManyLinks
		}
		inode.Nlink = uint32(newNlink)
		inode.ParentIno = w.parentIno
		if !w.existedInTarget {
			inode.Size = DefaultDirSize
		}
		if b.dirtyParentMeta[finalIno] {
			inode.Mtime = b.nowNano
		}
		inode.Ctime = b.nowNano
	} else {
		var newNlink int64
		if w.existedInTarget {
			newNlink = int64(w.targetOldInode.Nlink) + b.nlinkDelta[finalIno]
		} else {
			newNlink = b.nlinkDelta[finalIno]
		}
		if newNlink <= 0 {
			return b.tombstoneTargetNonDir(finalIno)
		}
		if newNlink > int64(MaxNlink) {
			return ErrTooManyLinks
		}
		inode.Nlink = uint32(newNlink)
		if inode.Nlink > 1 || w.everLinked {
			inode.Flags |= InodeFlagEverLinked
		}
		if w.payloadChanged {
			inode.WriteSeq = max(w.targetOldInode.WriteSeq, max(inode.WriteSeq, w.srcWriteSeq)) + 1
		}
		inode.Ctime = b.nowNano
	}

	raw, err := EncodeInode(inode)
	if err != nil {
		return err
	}
	b.putDelta(dk.LayerInodeKey(0, finalIno), EncodeLiveEnvelope(raw))

	if xErr := b.writePlannedXattrs(finalIno, w); xErr != nil {
		return xErr
	}

	if !w.isDir && (inode.Mode&ModeTypeMask) == ModeRegular {
		if dErr := b.writePlannedDataRefs(finalIno, w); dErr != nil {
			return dErr
		}
	}

	if !w.isDir && (inode.Flags&InodeFlagEverLinked != 0 || inode.Nlink > 1) {
		if inode.ParentIno != 0 {
			b.putDelta(dk.LayerParentKey(0, finalIno, inode.ParentIno), EncodeLiveEnvelope(nil))
		}
		for pDir := range b.entryParentDirs[finalIno] {
			b.putDelta(dk.LayerParentKey(0, finalIno, pDir), EncodeLiveEnvelope(nil))
		}
		if w.srcIno != 0 {
			srcParents, pErr := b.tc.source.scanParentDirs(w.srcIno)
			if pErr != nil {
				return pErr
			}
			for _, pDir := range srcParents {
				b.putDelta(dk.LayerParentKey(0, finalIno, pDir), EncodeLiveEnvelope(nil))
			}
		}
	}

	return nil
}

func (b *deltaMergeBuilder) writePlannedXattrs(finalIno uint64, w *plannedInodeWrite) error {
	if w.xattrMode == xattrDeltaKeepTarget {
		return nil
	}
	dk := b.disk.keys
	var desired map[string][]byte
	var err error
	switch w.xattrMode {
	case xattrDeltaCopySource:
		desired, err = b.tc.source.readAllXattrs(w.srcIno)
		if err != nil {
			return err
		}
	case xattrDeltaReplaceMap:
		desired = w.mergedXattrs
	case xattrDeltaCopyTarget:
		desired, err = b.tc.target.readAllXattrs(w.tgtOrigIno)
		if err != nil {
			return err
		}
	}

	var existing map[string][]byte
	if w.existedInTarget {
		existing, err = b.tc.target.readAllXattrs(finalIno)
		if err != nil {
			return err
		}
	}

	for k, v := range desired {
		if ev, ok := existing[k]; !ok || !bytes.Equal(ev, v) {
			b.putDelta(dk.LayerXattrKey(0, finalIno, []byte(k)), EncodeLiveEnvelope(v))
		}
	}
	for k := range existing {
		if _, ok := desired[k]; !ok {
			b.putDelta(dk.LayerXattrKey(0, finalIno, []byte(k)), EncodeTombstone())
		}
	}
	return nil
}

func (b *deltaMergeBuilder) writePlannedDataRefs(finalIno uint64, w *plannedInodeWrite) error {
	if !w.writeDataFromSource && !w.writeDataFromTarget {
		return nil
	}
	dk := b.disk.keys
	var desired map[uint64]BlobRef
	var err error
	if w.writeDataFromSource {
		desired, err = b.tc.source.readAllDataRefs(w.srcIno)
		if err != nil {
			return err
		}
	} else {
		desired, err = b.tc.target.readAllDataRefs(w.tgtOrigIno)
		if err != nil {
			return err
		}
	}

	var existing map[uint64]BlobRef
	if w.existedInTarget {
		existing, err = b.tc.target.readAllDataRefs(finalIno)
		if err != nil {
			return err
		}
	}

	for blkIdx, ref := range desired {
		if eRef, ok := existing[blkIdx]; !ok || eRef != ref {
			b.putDelta(dk.LayerDataKey(0, finalIno, blkIdx), EncodeLiveEnvelope(EncodeBlobRef(ref)))
			var oldLen uint64
			if ok {
				oldLen = uint64(eRef.Length)
			}
			b.logicalDelta += int64(ref.Length) - int64(oldLen)
		}
	}
	for blkIdx, eRef := range existing {
		if _, ok := desired[blkIdx]; !ok {
			b.putDelta(dk.LayerDataKey(0, finalIno, blkIdx), EncodeTombstone())
			b.logicalDelta -= int64(eRef.Length)
		}
	}
	return nil
}

func isUnderAnyPrefix(pathStr string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(pathStr, p) && len(pathStr) > len(p) && pathStr[len(p)] == '/' {
			return true
		}
	}
	return false
}
