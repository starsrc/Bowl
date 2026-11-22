package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func isLowWatermarkMeta(meta PackMeta) bool {
	return meta.LiveBytes > 0 && meta.LiveBytes*2 <= meta.TotalBytes
}

func (s *StoreActor) recordPackUndo(packID uint64) {
	if !s.recordingPackUndo {
		return
	}
	if s.tickPackUndo == nil {
		s.tickPackUndo = make(map[uint64]packMetaUndo)
	}
	if _, recorded := s.tickPackUndo[packID]; recorded {
		return
	}
	prev, existed := s.packStats[packID]
	s.tickPackUndo[packID] = packMetaUndo{
		existed: existed,
		meta:    prev,
	}
}

func (s *StoreActor) setPackMeta(packID uint64, next PackMeta) {
	s.recordPackUndo(packID)
	if prev, ok := s.packStats[packID]; ok {
		s.totalPackPhysicalBytes -= prev.TotalBytes
		s.totalPackLiveBytes -= prev.LiveBytes
		if isLowWatermarkMeta(prev) {
			s.lowWatermarkCount--
		}
	}
	s.totalPackPhysicalBytes += next.TotalBytes
	s.totalPackLiveBytes += next.LiveBytes
	if isLowWatermarkMeta(next) {
		s.lowWatermarkCount++
	}
	s.packStats[packID] = next
}

func (s *StoreActor) deletePackMeta(packID uint64) {
	s.recordPackUndo(packID)
	if prev, ok := s.packStats[packID]; ok {
		s.totalPackPhysicalBytes -= prev.TotalBytes
		s.totalPackLiveBytes -= prev.LiveBytes
		if isLowWatermarkMeta(prev) {
			s.lowWatermarkCount--
		}
		delete(s.packStats, packID)
	}
}

func (s *StoreActor) publishAtomicStats() {
	s.atomicLogicalBytes.Store(s.stat.LogicalBytes)
	s.atomicInodes.Store(s.stat.Inodes)
	s.atomicWriterEpoch.Store(s.stat.WriterEpoch)
	s.atomicPackPhysicalBytes.Store(s.totalPackPhysicalBytes)
	s.atomicPackLiveBytes.Store(s.totalPackLiveBytes)

	low := s.lowWatermarkCount
	if activeID := s.activePackID(); activeID != 0 {
		if meta, ok := s.packStats[activeID]; ok && isLowWatermarkMeta(meta) {
			low--
		}
	}
	s.atomicLowWatermarkPacks.Store(int64(max(0, low)))
}

func (s *StoreActor) loadPackStats() error {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	prefix := PackStatPrefix()
	for valid := iter.SeekGE(prefix); valid && bytes.HasPrefix(iter.Key(), prefix); valid = iter.Next() {
		packID, decErr := DecodePackStatKey(iter.Key())
		if decErr != nil {
			return decErr
		}
		meta, decErr := DecodePackMeta(iter.Value())
		if decErr != nil {
			return decErr
		}
		s.setPackMeta(packID, meta)
	}
	return iter.Error()
}

func (s *StoreActor) syncPackStatsWithBlobs() error {
	bs := s.blobs
	if bs == nil {
		return nil
	}
	var maxPackID uint64
	var zeroLiveMetaPacks []uint64
	for packID, meta := range s.packStats {
		if packID > maxPackID {
			maxPackID = packID
		}
		if meta.LiveBytes == 0 {
			zeroLiveMetaPacks = append(zeroLiveMetaPacks, packID)
		}
	}

	var deadDiskPacks []uint64
	for _, packID := range bs.existingPackIDs() {
		if packID > maxPackID {
			maxPackID = packID
		}
		if meta, ok := s.packStats[packID]; !ok || meta.LiveBytes == 0 {
			deadDiskPacks = append(deadDiskPacks, packID)
		}
	}

	if maxPackID > 0 {
		bs.advanceMinPackID(maxPackID)
	}
	if s.readOnly {
		return nil
	}

	if len(zeroLiveMetaPacks) > 0 {
		cleanBatch := s.db.NewBatch()
		defer func() { _ = cleanBatch.Close() }()
		for _, packID := range zeroLiveMetaPacks {
			s.deletePackMeta(packID)
			BatchDeletePackMetaDeferred(cleanBatch, packID)
			if !slices.Contains(deadDiskPacks, packID) {
				deadDiskPacks = append(deadDiskPacks, packID)
			}
		}
		if err := s.db.Apply(cleanBatch, pebble.Sync); err != nil {
			return err
		}
	} else if len(deadDiskPacks) > 0 {
		if err := s.db.LogData(nil, pebble.Sync); err != nil {
			return err
		}
	}
	for _, packID := range deadDiskPacks {
		if err := bs.removePack(packID); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.metrics.incPackRemoveErrors()
			if s.logger != nil {
				s.logger.Warn("startup dead pack remove failed", "pack_id", packID, "err", err)
			}
		}
	}
	return nil
}

func (s *StoreActor) addBlobRef(ref BlobRef) {
	if ref.PackID == 0 || ref.Length == 0 {
		return
	}
	meta := s.packStats[ref.PackID]
	meta.LiveBytes += uint64(ref.Length)
	endOffset := uint64(ref.Offset) + uint64(ref.Length)
	if endOffset > meta.TotalBytes {
		meta.TotalBytes = endOffset
	}
	if meta.LiveBytes > meta.TotalBytes {
		meta.TotalBytes = meta.LiveBytes
	}
	s.setPackMeta(ref.PackID, meta)
	s.dirtyPackStats[ref.PackID] = struct{}{}
}

func (s *StoreActor) removeBlobRef(ref BlobRef) {
	if ref.PackID == 0 || ref.Length == 0 {
		return
	}
	meta, ok := s.packStats[ref.PackID]
	if !ok {
		return
	}
	dec := uint64(ref.Length)
	if dec >= meta.LiveBytes {
		meta.LiveBytes = 0
	} else {
		meta.LiveBytes -= dec
	}
	s.setPackMeta(ref.PackID, meta)
	s.dirtyPackStats[ref.PackID] = struct{}{}
}

func (s *StoreActor) recordDeadBlobWrite(ref BlobRef) {
	if ref.PackID == 0 || ref.Length == 0 {
		return
	}
	meta := s.packStats[ref.PackID]
	endOffset := uint64(ref.Offset) + uint64(ref.Length)
	if endOffset > meta.TotalBytes {
		meta.TotalBytes = endOffset
	}
	s.setPackMeta(ref.PackID, meta)
	s.dirtyPackStats[ref.PackID] = struct{}{}
}

func (s *StoreActor) activePackID() uint64 {
	if s.blobs != nil {
		return s.blobs.activePackID()
	}
	return 0
}

func (s *StoreActor) stageAppend(ctx context.Context, blobs *blobStore, data []byte, staged *[]BlobRef) (BlobRef, error) {
	ref, err := blobs.append(ctx, data)
	if err != nil {
		return BlobRef{}, err
	}
	s.pinPackID(ref.PackID)
	if staged != nil {
		*staged = append(*staged, ref)
	}
	return ref, nil
}

func (s *StoreActor) stageAppendBlocks(ctx context.Context, blobs *blobStore, data []byte, staged *[]BlobRef) ([]BlobRef, error) {
	refs, err := blobs.appendBlocks(ctx, data)
	if err != nil {
		return nil, err
	}
	s.inFlightMu.Lock()
	for _, ref := range refs {
		if ref.PackID != 0 {
			s.inFlightPacks[ref.PackID]++
		}
	}
	s.inFlightMu.Unlock()
	if staged != nil {
		*staged = append(*staged, refs...)
	}
	return refs, nil
}

func (s *StoreActor) stageExtendTail(blobs *blobStore, oldRef BlobRef, patchOffset uint32, data []byte, newLen uint32, staged *[]BlobRef) (BlobRef, bool, error) {
	extRef, ok, err := blobs.extendTail(oldRef, patchOffset, data, newLen)
	if err != nil || !ok {
		return BlobRef{}, false, err
	}
	s.pinPackID(extRef.PackID)
	if staged != nil {
		*staged = append(*staged, extRef)
	}
	return extRef, true, nil
}

func (s *StoreActor) pinPackID(packID uint64) {
	if packID == 0 {
		return
	}
	s.inFlightMu.Lock()
	s.inFlightPacks[packID]++
	s.inFlightMu.Unlock()
}

func (s *StoreActor) pinPackIDs(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	s.inFlightMu.Lock()
	for _, id := range packIDs {
		if id != 0 {
			s.inFlightPacks[id]++
		}
	}
	s.inFlightMu.Unlock()
}

func (s *StoreActor) unpinPackIDs(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	s.inFlightMu.Lock()
	for _, id := range packIDs {
		if c := s.inFlightPacks[id]; c <= 1 {
			delete(s.inFlightPacks, id)
		} else {
			s.inFlightPacks[id] = c - 1
		}
	}
	s.inFlightMu.Unlock()
}

func (s *StoreActor) unpinStagedBlobs(staged []BlobRef) {
	if len(staged) == 0 {
		return
	}
	s.inFlightMu.Lock()
	for _, ref := range staged {
		if ref.PackID == 0 {
			continue
		}
		if c := s.inFlightPacks[ref.PackID]; c <= 1 {
			delete(s.inFlightPacks, ref.PackID)
		} else {
			s.inFlightPacks[ref.PackID] = c - 1
		}
	}
	s.inFlightMu.Unlock()
}

func (s *StoreActor) isPackPinned(packID uint64) bool {
	s.inFlightMu.Lock()
	pinned := s.inFlightPacks[packID] > 0
	s.inFlightMu.Unlock()
	return pinned
}

func (s *StoreActor) flushDirtyPackStatsToBatch(batch *pebble.Batch) (deadPacks []uint64, wrote bool) {
	activeID := s.activePackID()
	if s.lastActivePackID != 0 && s.lastActivePackID != activeID {
		if meta, ok := s.packStats[s.lastActivePackID]; ok && meta.LiveBytes == 0 {
			s.dirtyPackStats[s.lastActivePackID] = struct{}{}
		}
	}
	s.lastActivePackID = activeID

	for packID := range s.dirtyPackStats {
		meta, ok := s.packStats[packID]
		if !ok {
			continue
		}
		if meta.LiveBytes == 0 && packID != activeID && !s.isPackPinned(packID) {
			s.deletePackMeta(packID)
			BatchDeletePackMetaDeferred(batch, packID)
			deadPacks = append(deadPacks, packID)
			wrote = true
			continue
		}
		BatchSetPackMetaDeferred(batch, packID, meta)
		wrote = true
	}
	clear(s.dirtyPackStats)
	return deadPacks, wrote
}

func (s *StoreActor) addPendingDeadPacks(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	s.deadPacksMu.Lock()
	for _, id := range packIDs {
		if id != 0 && !slices.Contains(s.pendingDeadPacks, id) {
			s.pendingDeadPacks = append(s.pendingDeadPacks, id)
		}
	}
	s.atomicPendingDeadPacks.Store(int64(len(s.pendingDeadPacks)))
	s.deadPacksMu.Unlock()
}

func (s *StoreActor) takePendingDeadPacks() []uint64 {
	s.deadPacksMu.Lock()
	defer s.deadPacksMu.Unlock()
	if len(s.pendingDeadPacks) == 0 {
		return nil
	}
	out := s.pendingDeadPacks
	s.pendingDeadPacks = nil
	s.atomicPendingDeadPacks.Store(0)
	return out
}

func (s *StoreActor) restorePendingDeadPacks(packIDs []uint64) {
	s.addPendingDeadPacks(packIDs)
}

func (s *StoreActor) enqueueRemovePacks(packIDs []uint64) {
	if len(packIDs) == 0 || s.blobs == nil {
		return
	}
	s.blobs.enqueueRemovePacks(packIDs)
}

func (s *StoreActor) syncWALAndReclaimDeadPacks() error {
	_, _, _, _, err := s.syncWALAndReclaimDeadPacksTimed()
	return err
}

func (s *StoreActor) syncWALAndReclaimDeadPacksTimed() (didWALSync bool, walDur time.Duration, reclaimed int, removeDur time.Duration, err error) {
	toReclaim := s.takePendingDeadPacks()
	wasDirty := s.walDirty.Swap(false)
	blobsDirty := s.blobs != nil && s.blobs.hasDirty()
	if !wasDirty && len(toReclaim) == 0 && !blobsDirty {
		if s.blobs != nil {
			remStart := time.Now()
			s.blobs.waitPendingReclaims()
			removeDur = time.Since(remStart)
		}
		return false, 0, 0, removeDur, nil
	}
	if s.blobs != nil {
		if syncErr := s.blobs.sync(); syncErr != nil {
			s.restorePendingDeadPacks(toReclaim)
			if wasDirty {
				s.walDirty.Store(true)
			}
			return false, 0, 0, 0, syncErr
		}
	}
	if s.beforeWALSyncHook != nil {
		s.beforeWALSyncHook()
	}
	walStart := time.Now()
	var syncErr error
	if s.walApplyErrHook != nil {
		syncErr = s.walApplyErrHook()
	}
	if syncErr == nil {
		syncErr = s.db.LogData(nil, pebble.Sync)
	}
	if syncErr != nil {
		walDur = time.Since(walStart)
		s.restorePendingDeadPacks(toReclaim)
		if wasDirty {
			s.walDirty.Store(true)
		}
		return true, walDur, 0, 0, syncErr
	}
	walDur = time.Since(walStart)
	remStart := time.Now()
	if len(toReclaim) > 0 {
		s.enqueueRemovePacks(toReclaim)
		reclaimed = len(toReclaim)
	}
	if s.blobs != nil {
		s.blobs.waitPendingReclaims()
	}
	if reclaimed > 0 {
		removeDur = time.Since(remStart)
	}
	return true, walDur, reclaimed, removeDur, nil
}
