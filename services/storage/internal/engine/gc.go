package engine

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type relocateItem struct {
	loc    BlockRefLocation
	oldRef BlobRef
	newRef BlobRef
}

type activeLayerInfo struct {
	layerID    uint64
	branchName string
	commitID   uint64
	isCommit   bool
}

func (d *DiskStore) TriggerGC() {
	if d.closing.Load() || d.gcWake == nil {
		return
	}
	select {
	case d.gcWake <- struct{}{}:
	default:
	}
}

func (d *DiskStore) runBackgroundGC(ctx context.Context, interval time.Duration) {
	defer close(d.gcDone)
	var tickCh <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		tickCh = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickCh:
			_, _ = d.RunGC(ctx)
		case <-d.gcWake:
			_, _ = d.RunGC(ctx)
		}
	}
}

func sumLiveRefBytes(refMap map[BlobRef][]BlockRefLocation) uint64 {
	var liveBytes uint64
	for ref := range refMap {
		liveBytes += uint64(ref.Length)
	}
	return liveBytes
}

func (d *DiskStore) sweepSealedPack(ctx context.Context, packID, totalBytes uint64, refMap map[BlobRef][]BlockRefLocation, stats *GCStats) error {
	liveBytes := sumLiveRefBytes(refMap)
	if liveBytes == 0 {
		if delErr := d.packs.DeletePack(ctx, packID); delErr != nil {
			return delErr
		}
		stats.PacksDeleted++
		stats.BytesReclaimed += totalBytes
		return nil
	}
	if liveBytes*2 > totalBytes {
		return nil
	}
	relocated, skipped, relocErr := d.relocatePack(ctx, packID, refMap)
	stats.BlocksRelocated += relocated
	stats.BlocksSkipped += skipped
	if relocErr != nil {
		return relocErr
	}
	if delErr := d.packs.DeletePack(ctx, packID); delErr != nil {
		return delErr
	}
	stats.PacksDeleted++
	stats.BytesReclaimed += totalBytes
	return nil
}

type gcGate struct {
	mu               sync.Mutex
	activeShared     int
	exclusiveHeld    bool
	waitingExclusive int
	notify           chan struct{}
}

func (g *gcGate) waitChLocked() <-chan struct{} {
	if g.notify == nil {
		g.notify = make(chan struct{})
	}
	return g.notify
}

func (g *gcGate) broadcastLocked() {
	if g.notify != nil {
		close(g.notify)
		g.notify = nil
	}
}

func (g *gcGate) lockShared(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		g.mu.Lock()
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()
			return err
		}
		if !g.exclusiveHeld && g.waitingExclusive == 0 {
			g.activeShared++
			g.mu.Unlock()
			return nil
		}
		waitCh := g.waitChLocked()
		g.mu.Unlock()

		select {
		case <-waitCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *gcGate) unlockShared() {
	g.mu.Lock()
	g.activeShared--
	if g.activeShared == 0 && g.waitingExclusive > 0 {
		g.broadcastLocked()
	}
	g.mu.Unlock()
}

func (g *gcGate) lockExclusive(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}
	if !g.exclusiveHeld && g.activeShared == 0 {
		g.exclusiveHeld = true
		g.mu.Unlock()
		return nil
	}
	g.waitingExclusive++
	for {
		if err := ctx.Err(); err != nil {
			g.waitingExclusive--
			if !g.exclusiveHeld && g.waitingExclusive == 0 {
				g.broadcastLocked()
			}
			g.mu.Unlock()
			return err
		}
		if !g.exclusiveHeld && g.activeShared == 0 {
			g.waitingExclusive--
			g.exclusiveHeld = true
			g.mu.Unlock()
			return nil
		}
		waitCh := g.waitChLocked()
		g.mu.Unlock()

		select {
		case <-waitCh:
			g.mu.Lock()
		case <-ctx.Done():
			g.mu.Lock()
			g.waitingExclusive--
			if !g.exclusiveHeld && g.waitingExclusive == 0 {
				g.broadcastLocked()
			}
			g.mu.Unlock()
			return ctx.Err()
		}
	}
}

func (g *gcGate) unlockExclusive() {
	g.mu.Lock()
	g.exclusiveHeld = false
	g.broadcastLocked()
	g.mu.Unlock()
}

func (d *DiskStore) lockGCShared(ctx context.Context) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.gcGate.lockShared(ctx); err != nil {
		return err
	}
	if d.closing.Load() {
		d.unlockGCShared()
		return ErrClosed
	}
	return nil
}

func (d *DiskStore) unlockGCShared() {
	d.gcGate.unlockShared()
}

func (d *DiskStore) lockGCExclusive(ctx context.Context) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.gcGate.lockExclusive(ctx); err != nil {
		return err
	}
	if d.closing.Load() {
		d.unlockGCExclusive()
		return ErrClosed
	}
	return nil
}

func (d *DiskStore) unlockGCExclusive() {
	d.gcGate.unlockExclusive()
}

func (d *DiskStore) RunGC(ctx context.Context) (GCStats, error) {
	if d.closing.Load() {
		return GCStats{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if err := d.lockGCExclusive(ctx); err != nil {
		return GCStats{}, err
	}
	defer d.unlockGCExclusive()

	insp, err := d.packs.GCInspectionSnapshot(ctx)
	if err != nil {
		return GCStats{}, err
	}
	watermark := insp.Watermark
	packSizes := insp.SealedSizes
	activePackID := insp.ActivePackID
	activePackSize := insp.ActivePackSize
	inspectActive := activePackID != 0 && activePackSize > 0 && !insp.ActiveInFlight
	if (watermark == 0 || len(packSizes) == 0) && !inspectActive {
		return GCStats{}, nil
	}

	candidatePacks := make(map[uint64]uint64, len(packSizes)+1)
	maps.Copy(candidatePacks, packSizes)
	if inspectActive {
		candidatePacks[activePackID] = activePackSize
	}

	packLiveRefs, err := d.scanLiveRefsAtSnapshot(candidatePacks)
	if err != nil {
		return GCStats{}, err
	}

	packIDs := make([]uint64, 0, len(packSizes))
	for packID := range packSizes {
		packIDs = append(packIDs, packID)
	}
	slices.Sort(packIDs)

	var (
		stats            GCStats
		firstErr         error
		sealedToRelocate []uint64
	)

	for _, packID := range packIDs {
		if ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			break
		}

		totalBytes := packSizes[packID]
		refMap := packLiveRefs[packID]
		liveBytes := sumLiveRefBytes(refMap)
		stats.PacksScanned++

		if liveBytes == 0 {
			if delErr := d.packs.DeletePack(ctx, packID); delErr != nil {
				if firstErr == nil {
					firstErr = delErr
				}
				continue
			}
			stats.PacksDeleted++
			stats.BytesReclaimed += totalBytes
			continue
		}

		if liveBytes*2 <= totalBytes {
			sealedToRelocate = append(sealedToRelocate, packID)
		}
	}

	if inspectActive && ctx.Err() == nil {
		stats.PacksScanned++
		activeLiveBytes := sumLiveRefBytes(packLiveRefs[activePackID])
		if activeLiveBytes*2 <= activePackSize {
			if d.beforeGCSealActiveHook != nil {
				if hookErr := d.beforeGCSealActiveHook(activePackID); hookErr != nil && firstErr == nil {
					firstErr = hookErr
				}
			}
			if firstErr == nil {
				sealed, sealErr := d.packs.SealActiveIf(ctx, activePackID)
				if sealErr != nil {
					firstErr = sealErr
				} else if sealed {
					postWatermark, postSizes, postErr := d.packs.SealedWatermarkAndSizes(ctx)
					if postErr != nil {
						firstErr = postErr
					} else if postSize, ok := postSizes[activePackID]; ok && activePackID <= postWatermark {
						postRefs, scanErr := d.scanLiveRefsAtSnapshot(map[uint64]uint64{activePackID: postSize})
						if scanErr != nil {
							firstErr = scanErr
						} else if sweepErr := d.sweepSealedPack(ctx, activePackID, postSize, postRefs[activePackID], &stats); sweepErr != nil {
							firstErr = sweepErr
						}
					}
				}
			}
		}
	}

	for _, packID := range sealedToRelocate {
		if ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			break
		}
		if sweepErr := d.sweepSealedPack(ctx, packID, packSizes[packID], packLiveRefs[packID], &stats); sweepErr != nil && firstErr == nil {
			firstErr = sweepErr
		}
	}

	return stats, firstErr
}

func (d *DiskStore) scanLiveRefsAtSnapshot(packSizes map[uint64]uint64) (map[uint64]map[BlobRef][]BlockRefLocation, error) {
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dk := d.keys
	layers, err := collectActiveLayers(snap, dk)
	if err != nil {
		return nil, err
	}

	packLiveRefs := make(map[uint64]map[BlobRef][]BlockRefLocation, len(packSizes))
	for _, layer := range layers {
		prefix := dk.LayerDataAllPrefix(layer.layerID)
		iter, iterErr := snap.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: PrefixUpperBound(prefix),
		})
		if iterErr != nil {
			return nil, iterErr
		}
		for iter.First(); iter.Valid(); iter.Next() {
			tag, payload, decErr := DecodeEnvelope(iter.Value())
			if decErr != nil {
				_ = iter.Close()
				return nil, decErr
			}
			if tag != TagLive {
				continue
			}
			ref, refErr := DecodeBlobRef(payload)
			if refErr != nil {
				_ = iter.Close()
				return nil, refErr
			}
			if _, candidate := packSizes[ref.PackID]; !candidate {
				continue
			}
			_, ino, blkIdx, keyErr := dk.DecodeLayerDataKey(iter.Key())
			if keyErr != nil {
				_ = iter.Close()
				return nil, keyErr
			}
			byRef := packLiveRefs[ref.PackID]
			if byRef == nil {
				byRef = make(map[BlobRef][]BlockRefLocation)
				packLiveRefs[ref.PackID] = byRef
			}
			byRef[ref] = append(byRef[ref], BlockRefLocation{
				LayerID:    layer.layerID,
				BranchName: layer.branchName,
				CommitID:   layer.commitID,
				IsCommit:   layer.isCommit,
				Ino:        ino,
				BlockIdx:   blkIdx,
			})
		}
		if closeErr := iter.Close(); closeErr != nil {
			return nil, closeErr
		}
	}
	return packLiveRefs, nil
}

func collectActiveLayers(r pebbleReader, dk diskKeys) ([]activeLayerInfo, error) {
	var layers []activeLayerInfo

	bPrefix := dk.MetaBranchPrefix()
	bIter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: bPrefix,
		UpperBound: PrefixUpperBound(bPrefix),
	})
	if err != nil {
		return nil, err
	}
	for bIter.First(); bIter.Valid(); bIter.Next() {
		bm, decErr := DecodeBranchMeta(bIter.Value())
		if decErr != nil {
			_ = bIter.Close()
			return nil, decErr
		}
		if bm.WorkLayerID != 0 {
			layers = append(layers, activeLayerInfo{
				layerID:    bm.WorkLayerID,
				branchName: bm.Name,
				isCommit:   false,
			})
		}
	}
	if closeErr := bIter.Close(); closeErr != nil {
		return nil, closeErr
	}

	cPrefix := dk.MetaCommitPrefix()
	cIter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: cPrefix,
		UpperBound: PrefixUpperBound(cPrefix),
	})
	if err != nil {
		return nil, err
	}
	for cIter.First(); cIter.Valid(); cIter.Next() {
		cm, decErr := DecodeCommitMeta(cIter.Value())
		if decErr != nil {
			_ = cIter.Close()
			return nil, decErr
		}
		if cm.LayerID != 0 {
			layers = append(layers, activeLayerInfo{
				layerID:  cm.LayerID,
				commitID: cm.ID,
				isCommit: true,
			})
		}
	}
	if closeErr := cIter.Close(); closeErr != nil {
		return nil, closeErr
	}

	return layers, nil
}

func (d *DiskStore) relocatePack(ctx context.Context, packID uint64, refMap map[BlobRef][]BlockRefLocation) (relocated, skipped int, err error) {
	orderedRefs := make([]BlobRef, 0, len(refMap))
	for ref := range refMap {
		orderedRefs = append(orderedRefs, ref)
	}
	sort.Slice(orderedRefs, func(i, j int) bool {
		return orderedRefs[i].Offset < orderedRefs[j].Offset
	})

	payloads := make([][]byte, 0, len(orderedRefs))
	for _, ref := range orderedRefs {
		data, readErr := d.packs.ReadBlock(ref)
		if readErr != nil {
			return 0, 0, readErr
		}
		payloads = append(payloads, data)
	}

	newRefs, err := d.packs.AppendBlocks(ctx, payloads)
	if err != nil {
		return 0, 0, err
	}
	defer d.packs.ReleaseInFlight(newRefs)

	byLayer := make(map[uint64][]relocateItem)
	layerMeta := make(map[uint64]BlockRefLocation)
	for i, oldRef := range orderedRefs {
		newRef := newRefs[i]
		for _, loc := range refMap[oldRef] {
			byLayer[loc.LayerID] = append(byLayer[loc.LayerID], relocateItem{
				loc:    loc,
				oldRef: oldRef,
				newRef: newRef,
			})
			layerMeta[loc.LayerID] = loc
		}
	}

	layerIDs := make([]uint64, 0, len(byLayer))
	for lid := range byLayer {
		layerIDs = append(layerIDs, lid)
	}
	slices.Sort(layerIDs)

	for _, lid := range layerIDs {
		items := byLayer[lid]
		if d.beforeGCRelocateHook != nil {
			for _, item := range items {
				if hookErr := d.beforeGCRelocateHook(packID, item.loc); hookErr != nil {
					return relocated, skipped, hookErr
				}
			}
		}

		target := layerMeta[lid]
		rel, skp, relErr := d.relocateLayer(ctx, target, items)
		if relErr != nil {
			return relocated, skipped, relErr
		}
		relocated += rel
		skipped += skp
	}

	return relocated, skipped, nil
}

func (d *DiskStore) relocateLayer(ctx context.Context, target BlockRefLocation, items []relocateItem) (relocated, skipped int, err error) {
	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()

	if !target.IsCommit {
		bm, bmErr := readBranchMeta(d.db, d.keys, target.BranchName)
		if errors.Is(bmErr, ErrNotFound) {
			return 0, len(items), nil
		}
		if bmErr != nil {
			return 0, 0, bmErr
		}
		if bm.WorkLayerID != target.LayerID {
			return 0, len(items), nil
		}
		mu, lockErr := d.acquireBranchLock(target.BranchName)
		if errors.Is(lockErr, ErrNotFound) {
			return 0, len(items), nil
		}
		if lockErr != nil {
			return 0, 0, lockErr
		}
		defer mu.Unlock()

		batch := d.db.NewIndexedBatch()
		defer func() { _ = batch.Close() }()

		curBM, reErr := readBranchMeta(batch, d.keys, target.BranchName)
		if errors.Is(reErr, ErrNotFound) {
			return 0, len(items), nil
		}
		if reErr != nil {
			return 0, 0, reErr
		}
		if curBM.WorkLayerID != target.LayerID {
			return 0, len(items), nil
		}
		return d.relocateLayerBatch(ctx, batch, items)
	}

	cmt, cmtErr := readCommitMeta(d.db, d.keys, target.CommitID)
	if errors.Is(cmtErr, ErrNotFound) {
		return 0, len(items), nil
	}
	if cmtErr != nil {
		return 0, 0, cmtErr
	}
	if cmt.LayerID != target.LayerID {
		return 0, len(items), nil
	}

	batch := d.db.NewIndexedBatch()
	defer func() { _ = batch.Close() }()
	return d.relocateLayerBatch(ctx, batch, items)
}

func (d *DiskStore) relocateLayerBatch(ctx context.Context, batch *pebble.Batch, items []relocateItem) (relocated, skipped int, err error) {
	dk := d.keys
	for _, item := range items {
		key := dk.LayerDataKey(item.loc.LayerID, item.loc.Ino, item.loc.BlockIdx)
		val, closer, getErr := batch.Get(key)
		if errors.Is(getErr, pebble.ErrNotFound) {
			skipped++
			continue
		}
		if getErr != nil {
			return 0, 0, getErr
		}
		tag, payload, decErr := DecodeEnvelope(val)
		var curRef BlobRef
		if decErr == nil && tag == TagLive {
			curRef, decErr = DecodeBlobRef(payload)
		}
		closeErr := closer.Close()
		if decErr != nil || closeErr != nil {
			return 0, 0, errors.Join(decErr, closeErr)
		}
		if tag != TagLive || curRef != item.oldRef {
			skipped++
			continue
		}
		if setErr := batch.Set(key, EncodeLiveEnvelope(EncodeBlobRef(item.newRef)), nil); setErr != nil {
			return 0, 0, setErr
		}
		relocated++
	}
	if batch.Count() > 0 {
		if commitErr := d.commitBatchSync(ctx, batch); commitErr != nil {
			return 0, 0, commitErr
		}
	}
	return relocated, skipped, nil
}
