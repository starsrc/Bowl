package engine

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type relocateItem struct {
	loc    BlockRefLocation
	oldRef BlobRef
	newRef BlobRef
}

type relocateReq struct {
	items []relocateItem
}

type relocateResp struct {
	relocated int
	skipped   int
}

type activeLayerInfo struct {
	layerID    uint64
	branchName string
	commitID   uint64
	isCommit   bool
}

func (d *DiskStore) runBackgroundGC(ctx context.Context, interval time.Duration) {
	defer close(d.gcDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = d.RunGC(ctx)
		}
	}
}

func (d *DiskStore) RunGC(ctx context.Context) (GCStats, error) {
	if d.closing.Load() {
		return GCStats{}, ErrClosed
	}

	d.gcMu.Lock()
	defer d.gcMu.Unlock()

	if d.closing.Load() {
		return GCStats{}, ErrClosed
	}

	watermark, packSizes, err := d.packs.SealedWatermarkAndSizes(ctx)
	if err != nil {
		return GCStats{}, err
	}
	if watermark == 0 || len(packSizes) == 0 {
		return GCStats{}, nil
	}

	packLiveRefs, err := d.scanLiveRefsAtSnapshot(watermark, packSizes)
	if err != nil {
		return GCStats{}, err
	}

	packIDs := make([]uint64, 0, len(packSizes))
	for packID := range packSizes {
		packIDs = append(packIDs, packID)
	}
	slices.Sort(packIDs)

	var (
		stats    GCStats
		firstErr error
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
		var liveBytes uint64
		for ref := range refMap {
			liveBytes += uint64(ref.Length)
		}
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

		if liveBytes*2 > totalBytes {
			continue
		}

		relocated, skipped, relocErr := d.relocatePack(ctx, packID, refMap)
		stats.BlocksRelocated += relocated
		stats.BlocksSkipped += skipped
		if relocErr != nil {
			if firstErr == nil {
				firstErr = relocErr
			}
			continue
		}

		if delErr := d.packs.DeletePack(ctx, packID); delErr != nil {
			if firstErr == nil {
				firstErr = delErr
			}
			continue
		}
		stats.PacksDeleted++
		stats.BytesReclaimed += totalBytes
	}

	return stats, firstErr
}

func (d *DiskStore) scanLiveRefsAtSnapshot(watermark uint64, packSizes map[uint64]uint64) (map[uint64]map[BlobRef][]BlockRefLocation, error) {
	snap := d.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	layers, err := collectActiveLayers(snap)
	if err != nil {
		return nil, err
	}

	packLiveRefs := make(map[uint64]map[BlobRef][]BlockRefLocation, len(packSizes))
	for _, layer := range layers {
		prefix := LayerDataAllPrefix(layer.layerID)
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
			if ref.PackID > watermark {
				continue
			}
			if _, candidate := packSizes[ref.PackID]; !candidate {
				continue
			}
			_, ino, blkIdx, keyErr := DecodeLayerDataKey(iter.Key())
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

func collectActiveLayers(r pebbleReader) ([]activeLayerInfo, error) {
	var layers []activeLayerInfo

	bPrefix := MetaBranchPrefix()
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

	cPrefix := MetaCommitPrefix()
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

	newRefs, err := d.packs.AppendBlocks(ctx, payloads, true)
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
		var (
			actor   *BranchActor
			release func()
			acqErr  error
		)
		if target.IsCommit {
			actor, release, acqErr = d.acquireCommitActor(target.LayerID)
		} else {
			actor, release, acqErr = d.acquireBranchActor(target.BranchName)
			if errors.Is(acqErr, ErrNotFound) {
				actor, release, acqErr = d.acquireCommitActor(target.LayerID)
			}
		}
		if acqErr != nil {
			return relocated, skipped, acqErr
		}

		resp, subErr := actor.submit(ctx, branchOpRelocate, &relocateReq{items: items})
		release()
		if subErr != nil {
			return relocated, skipped, subErr
		}
		rr, ok := resp.(relocateResp)
		if !ok {
			return relocated, skipped, ErrInvalidValue
		}
		relocated += rr.relocated
		skipped += rr.skipped
	}

	return relocated, skipped, nil
}

func (a *BranchActor) execRelocate(batch *pebble.Batch, raw any) (any, error) {
	req, ok := raw.(*relocateReq)
	if !ok {
		return nil, ErrInvalid
	}

	var resp relocateResp
	for _, item := range req.items {
		key := LayerDataKey(item.loc.LayerID, item.loc.Ino, item.loc.BlockIdx)
		val, closer, err := batch.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			resp.skipped++
			continue
		}
		if err != nil {
			return nil, err
		}
		tag, payload, decErr := DecodeEnvelope(val)
		var curRef BlobRef
		if decErr == nil && tag == TagLive {
			curRef, decErr = DecodeBlobRef(payload)
		}
		closeErr := closer.Close()
		if decErr != nil || closeErr != nil {
			return nil, errors.Join(decErr, closeErr)
		}
		if tag != TagLive || curRef != item.oldRef {
			resp.skipped++
			continue
		}
		if setErr := batch.Set(key, EncodeLiveEnvelope(EncodeBlobRef(item.newRef)), nil); setErr != nil {
			return nil, setErr
		}
		resp.relocated++
	}
	return resp, nil
}
