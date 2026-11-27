package engine

import (
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const defaultMaxOpenReadPacks = 64

type packReadHandle struct {
	file     *os.File
	refs     atomic.Int32
	accessed atomic.Bool
}

func (h *packReadHandle) acquire() bool {
	for {
		cur := h.refs.Load()
		if cur <= 0 {
			return false
		}
		if h.refs.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (h *packReadHandle) release() {
	_ = h.releaseWithErr()
}

func (h *packReadHandle) releaseWithErr() error {
	if h.refs.Add(-1) == 0 {
		return h.file.Close()
	}
	return nil
}

type dirtyPackSnap struct {
	packID uint64
	handle *packReadHandle
}

type PackGCInspection struct {
	Watermark      uint64
	SealedSizes    map[uint64]uint64
	ActivePackID   uint64
	ActivePackSize uint64
	ActiveInFlight bool
}

type PackStore struct {
	dir              string
	maxPackSize      uint32
	maxOpenReadPacks int

	syncMu sync.Mutex

	packMu             sync.Mutex
	closed             bool
	nextPackID         uint64
	activePackID       uint64
	activeHandle       *packReadHandle
	activeOffset       uint32
	dirtyGen           uint64
	syncedGen          uint64
	dirtyPacks         map[uint64]uint64
	dirtyDirGen        uint64
	dirtyAtomic        atomic.Bool
	packSizes          map[uint64]uint64
	retiredPacks       map[uint64]struct{}
	totalPhysicalBytes uint64
	atomicPhysical     atomic.Uint64

	syncFileHook   func(packID uint64) error
	syncDirHook    func(dir string) error
	removePackHook func(packID uint64) error
	onDirty        func()

	inFlightMu    sync.Mutex
	inFlightPacks map[uint64]int

	filesMu          sync.RWMutex
	activeReadID     uint64
	activeReadHandle *packReadHandle
	readFiles        map[uint64]*packReadHandle
	readClock        []uint64
	readClockIdx     map[uint64]int
	readClockHand    int
	filesDeleteGen   uint64
	closedFiles      bool

	closeDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newPackStore(dir string, maxPackSize uint32, onDirty func()) (*PackStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &PackStore{
		dir:           dir,
		maxPackSize:   maxPackSize,
		onDirty:       onDirty,
		dirtyPacks:    make(map[uint64]uint64),
		packSizes:     make(map[uint64]uint64),
		retiredPacks:  make(map[uint64]struct{}),
		inFlightPacks: make(map[uint64]int),
		readFiles:     make(map[uint64]*packReadHandle),
		readClockIdx:  make(map[uint64]int),
		closeDone:     make(chan struct{}),
	}
	if err := p.recoverExistingPacks(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *PackStore) recoverExistingPacks() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	var maxID uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}
		packID, parseErr := parsePackID(entry.Name())
		if parseErr != nil || packID == 0 {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		sz := uint64(max(info.Size(), 0))
		p.packSizes[packID] = sz
		p.totalPhysicalBytes += sz
		if packID > maxID {
			maxID = packID
		}
	}
	p.nextPackID = maxID + 1
	p.atomicPhysical.Store(p.totalPhysicalBytes)
	return nil
}

func (p *PackStore) Close() error {
	p.closeOnce.Do(func() {
		defer close(p.closeDone)

		p.packMu.Lock()
		p.closed = true
		p.packMu.Unlock()

		flushErr := p.flushAllOnClose()

		p.packMu.Lock()
		closeErr := p.closeActivePack()
		p.packMu.Unlock()

		p.filesMu.Lock()
		p.closedFiles = true
		for id := range p.readFiles {
			p.removeReadFileLocked(id)
		}
		p.filesMu.Unlock()

		p.closeErr = errors.Join(flushErr, closeErr)
	})
	return p.closeErr
}

func (p *PackStore) openReadPackLimit() int {
	if p.maxOpenReadPacks > 0 {
		return p.maxOpenReadPacks
	}
	return defaultMaxOpenReadPacks
}

func (p *PackStore) PhysicalBytes() uint64 {
	return p.atomicPhysical.Load()
}

func (p *PackStore) AppendBlocks(ctx context.Context, blocks [][]byte) ([]BlobRef, error) {
	if len(blocks) == 0 {
		return nil, ErrInvalidValue
	}
	for _, blk := range blocks {
		if len(blk) == 0 || uint32(len(blk)) > MaxBlockSize || uint32(len(blk)) > p.maxPackSize {
			return nil, ErrInvalidValue
		}
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	p.packMu.Lock()
	defer p.packMu.Unlock()

	if p.closed {
		return nil, ErrClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	refs := make([]BlobRef, 0, len(blocks))
	createdDir := false

	for _, blk := range blocks {
		blkLen := uint32(len(blk))
		if p.activeHandle == nil || uint64(p.activeOffset)+uint64(blkLen) > uint64(p.maxPackSize) {
			if openErr := p.openNextActivePack(); openErr != nil {
				p.recordDirtyLocked(refs, createdDir)
				return nil, openErr
			}
			createdDir = true
		}

		packID := p.activePackID
		offset := p.activeOffset
		if _, writeErr := p.activeHandle.file.WriteAt(blk, int64(offset)); writeErr != nil {
			p.recordDirtyLocked(refs, createdDir)
			return nil, writeErr
		}
		p.activeOffset = offset + blkLen
		p.packSizes[packID] = uint64(p.activeOffset)
		p.totalPhysicalBytes += uint64(blkLen)
		p.atomicPhysical.Store(p.totalPhysicalBytes)

		refs = append(refs, BlobRef{
			PackID: packID,
			Offset: offset,
			Length: blkLen,
		})
	}

	p.recordDirtyLocked(refs, createdDir)
	if p.onDirty != nil {
		p.onDirty()
	}
	p.pinInFlight(refs)
	return refs, nil
}

func (p *PackStore) recordDirtyLocked(refs []BlobRef, createdDir bool) {
	if len(refs) == 0 && !createdDir {
		return
	}
	p.dirtyGen++
	gen := p.dirtyGen
	for _, ref := range refs {
		p.dirtyPacks[ref.PackID] = gen
	}
	if createdDir {
		p.dirtyDirGen = gen
	}
	p.updateDirtyAtomic()
}

func (p *PackStore) ReleaseInFlight(refs []BlobRef) {
	if len(refs) == 0 {
		return
	}
	p.inFlightMu.Lock()
	for _, ref := range refs {
		if cur := p.inFlightPacks[ref.PackID]; cur <= 1 {
			delete(p.inFlightPacks, ref.PackID)
		} else {
			p.inFlightPacks[ref.PackID] = cur - 1
		}
	}
	p.inFlightMu.Unlock()
}

func (p *PackStore) pinInFlight(refs []BlobRef) {
	if len(refs) == 0 {
		return
	}
	p.inFlightMu.Lock()
	for _, ref := range refs {
		p.inFlightPacks[ref.PackID]++
	}
	p.inFlightMu.Unlock()
}

func (p *PackStore) RetirePack(ctx context.Context, packID uint64) error {
	if packID == 0 {
		return ErrInvalidValue
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	p.packMu.Lock()
	defer p.packMu.Unlock()

	if p.closed {
		return ErrClosed
	}
	if packID == p.activePackID {
		return ErrInvalid
	}
	p.inFlightMu.Lock()
	inFlight := p.inFlightPacks[packID] > 0
	p.inFlightMu.Unlock()
	if inFlight {
		return ErrInvalid
	}

	p.retiredPacks[packID] = struct{}{}
	delete(p.dirtyPacks, packID)
	p.updateDirtyAtomic()
	return nil
}

func (p *PackStore) removePackFile(packID uint64) error {
	if p.removePackHook != nil {
		if err := p.removePackHook(packID); err != nil {
			return err
		}
	}
	return os.Remove(packFilePath(p.dir, packID))
}

func (p *PackStore) unlinkRetiredPacks(packIDs []uint64) ([]uint64, error) {
	if len(packIDs) == 0 {
		return nil, nil
	}
	var (
		removed []uint64
		failed  []uint64
		errs    []error
	)
	for _, packID := range packIDs {
		if packID == 0 {
			continue
		}
		if rmErr := p.removePackFile(packID); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			failed = append(failed, packID)
			errs = append(errs, rmErr)
			continue
		}
		removed = append(removed, packID)
	}

	if len(removed) > 0 {
		p.packMu.Lock()
		p.filesMu.Lock()
		p.filesDeleteGen++
		for _, packID := range removed {
			p.removeReadFileLocked(packID)
		}
		p.filesMu.Unlock()

		for _, packID := range removed {
			delete(p.retiredPacks, packID)
			if sz, ok := p.packSizes[packID]; ok {
				delete(p.packSizes, packID)
				p.totalPhysicalBytes = saturatingSub(p.totalPhysicalBytes, sz)
			}
		}
		p.atomicPhysical.Store(p.totalPhysicalBytes)
		p.packMu.Unlock()
	}

	return failed, errors.Join(errs...)
}

func (p *PackStore) SealedWatermark(ctx context.Context) (uint64, error) {
	wm, _, err := p.SealedWatermarkAndSizes(ctx)
	return wm, err
}

func (p *PackStore) SealedWatermarkAndSizes(ctx context.Context) (watermark uint64, sizes map[uint64]uint64, err error) {
	if flushErr := p.flushDirty(ctx, false); flushErr != nil {
		return 0, nil, flushErr
	}

	p.packMu.Lock()
	defer p.packMu.Unlock()

	if p.closed {
		return 0, nil, ErrClosed
	}
	wm, sz := p.computeWatermarkAndSizes()
	return wm, sz, nil
}

func (p *PackStore) GCInspectionSnapshot(ctx context.Context) (PackGCInspection, error) {
	if flushErr := p.flushDirty(ctx, false); flushErr != nil {
		return PackGCInspection{}, flushErr
	}

	p.packMu.Lock()
	defer p.packMu.Unlock()

	if p.closed {
		return PackGCInspection{}, ErrClosed
	}
	wm, sz := p.computeWatermarkAndSizes()
	insp := PackGCInspection{
		Watermark:    wm,
		SealedSizes:  sz,
		ActivePackID: p.activePackID,
	}
	if p.activePackID != 0 {
		insp.ActivePackSize = p.packSizes[p.activePackID]
		p.inFlightMu.Lock()
		insp.ActiveInFlight = p.inFlightPacks[p.activePackID] > 0
		p.inFlightMu.Unlock()
	}
	return insp, nil
}

func (p *PackStore) SealActive(ctx context.Context) error {
	if flushErr := p.flushDirty(ctx, false); flushErr != nil {
		return flushErr
	}

	p.packMu.Lock()
	defer p.packMu.Unlock()

	if p.closed {
		return ErrClosed
	}
	return p.closeActivePack()
}

func (p *PackStore) SealActiveIf(ctx context.Context, expectedPackID uint64) (bool, error) {
	if expectedPackID == 0 {
		return false, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return false, err
		}
	}

	p.packMu.Lock()
	if p.closed {
		p.packMu.Unlock()
		return false, ErrClosed
	}
	if p.activePackID != expectedPackID {
		p.packMu.Unlock()
		return false, nil
	}
	p.packMu.Unlock()

	if flushErr := p.flushDirty(ctx, false); flushErr != nil {
		return false, flushErr
	}

	p.packMu.Lock()
	defer p.packMu.Unlock()

	if p.closed {
		return false, ErrClosed
	}
	if p.activePackID != expectedPackID {
		return false, nil
	}
	if closeErr := p.closeActivePack(); closeErr != nil {
		return false, closeErr
	}
	return true, nil
}

func (p *PackStore) SyncActive(ctx context.Context) error {
	return p.flushDirty(ctx, true)
}

func (p *PackStore) hasDirty() bool {
	return p.dirtyAtomic.Load()
}

func (p *PackStore) updateDirtyAtomic() {
	p.dirtyAtomic.Store(len(p.dirtyPacks) > 0 || p.dirtyDirGen > 0)
}

func (p *PackStore) syncPackFile(packID uint64, f *os.File) error {
	if p.syncFileHook != nil {
		if err := p.syncFileHook(packID); err != nil {
			return err
		}
	}
	return syncFileData(f)
}

func (p *PackStore) syncPacksDir() error {
	if p.syncDirHook != nil {
		if err := p.syncDirHook(p.dir); err != nil {
			return err
		}
	}
	return syncDir(p.dir)
}

func (p *PackStore) closedFlushResult(ctx context.Context, waitOnClose bool) error {
	if !waitOnClose {
		return ErrClosed
	}
	select {
	case <-p.closeDone:
		return p.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PackStore) snapshotDirtyLocked() (uint64, []dirtyPackSnap, bool) {
	flushGen := p.dirtyGen
	needDir := p.dirtyDirGen > 0
	if len(p.dirtyPacks) == 0 {
		return flushGen, nil, needDir
	}
	ids := make([]uint64, 0, len(p.dirtyPacks))
	for id := range p.dirtyPacks {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	snaps := make([]dirtyPackSnap, len(ids))
	for i, id := range ids {
		snaps[i].packID = id
		if id == p.activePackID && p.activeHandle != nil && p.activeHandle.acquire() {
			snaps[i].handle = p.activeHandle
		}
	}
	return flushGen, snaps, needDir
}

func releaseDirtySnaps(snaps []dirtyPackSnap) {
	for i := range snaps {
		if snaps[i].handle != nil {
			snaps[i].handle.release()
			snaps[i].handle = nil
		}
	}
}

func (p *PackStore) syncSnapshot(snaps []dirtyPackSnap, needDir bool) error {
	defer releaseDirtySnaps(snaps)
	for i := range snaps {
		h := snaps[i].handle
		snaps[i].handle = nil
		if h == nil {
			var acqErr error
			h, acqErr = p.acquireReadHandle(snaps[i].packID)
			if errors.Is(acqErr, ErrNotFound) {
				continue
			}
			if acqErr != nil {
				return acqErr
			}
		}
		syncErr := p.syncPackFile(snaps[i].packID, h.file)
		h.release()
		if syncErr != nil {
			return syncErr
		}
	}
	if needDir {
		if err := p.syncPacksDir(); err != nil {
			return err
		}
	}
	return nil
}

func (p *PackStore) markSnapshotSyncedLocked(flushGen uint64, snaps []dirtyPackSnap, needDir bool) {
	for _, snap := range snaps {
		if curGen, ok := p.dirtyPacks[snap.packID]; ok && curGen <= flushGen {
			delete(p.dirtyPacks, snap.packID)
		}
	}
	if needDir && p.dirtyDirGen <= flushGen {
		p.dirtyDirGen = 0
	}
	if flushGen > p.syncedGen {
		p.syncedGen = flushGen
	}
	p.updateDirtyAtomic()
}

func (p *PackStore) flushDirty(ctx context.Context, waitOnClose bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	p.packMu.Lock()
	if p.closed {
		p.packMu.Unlock()
		return p.closedFlushResult(ctx, waitOnClose)
	}
	if len(p.dirtyPacks) == 0 && p.dirtyDirGen == 0 {
		p.packMu.Unlock()
		return nil
	}
	targetGen := p.dirtyGen
	p.packMu.Unlock()

	p.syncMu.Lock()

	p.packMu.Lock()
	if p.closed {
		p.packMu.Unlock()
		p.syncMu.Unlock()
		return p.closedFlushResult(ctx, waitOnClose)
	}
	if p.syncedGen >= targetGen {
		p.packMu.Unlock()
		p.syncMu.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		p.packMu.Unlock()
		p.syncMu.Unlock()
		return err
	}
	flushGen, snaps, needDir := p.snapshotDirtyLocked()
	if len(snaps) == 0 && !needDir {
		if flushGen > p.syncedGen {
			p.syncedGen = flushGen
		}
		p.updateDirtyAtomic()
		p.packMu.Unlock()
		p.syncMu.Unlock()
		return nil
	}
	p.packMu.Unlock()

	if syncErr := p.syncSnapshot(snaps, needDir); syncErr != nil {
		p.syncMu.Unlock()
		return syncErr
	}

	p.packMu.Lock()
	p.markSnapshotSyncedLocked(flushGen, snaps, needDir)
	p.packMu.Unlock()
	p.syncMu.Unlock()
	return nil
}

func (p *PackStore) flushAllOnClose() error {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()

	p.packMu.Lock()
	flushGen, snaps, needDir := p.snapshotDirtyLocked()
	p.packMu.Unlock()

	if len(snaps) == 0 && !needDir {
		return nil
	}
	if err := p.syncSnapshot(snaps, needDir); err != nil {
		return err
	}

	p.packMu.Lock()
	p.markSnapshotSyncedLocked(flushGen, snaps, needDir)
	p.packMu.Unlock()
	return nil
}

func (p *PackStore) openNextActivePack() error {
	if err := p.closeActivePack(); err != nil {
		return err
	}

	packID := p.nextPackID
	p.nextPackID++
	path := packFilePath(p.dir, packID)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	handle := &packReadHandle{file: file}
	handle.refs.Store(1)

	p.activePackID = packID
	p.activeHandle = handle
	p.activeOffset = 0
	p.packSizes[packID] = 0

	p.filesMu.Lock()
	p.activeReadID = packID
	p.activeReadHandle = handle
	p.filesMu.Unlock()
	return nil
}

func (p *PackStore) closeActivePack() error {
	if p.activeHandle == nil {
		return nil
	}
	old := p.activeHandle
	p.filesMu.Lock()
	p.activeReadID = 0
	p.activeReadHandle = nil
	p.filesMu.Unlock()

	p.activeHandle = nil
	p.activePackID = 0
	p.activeOffset = 0
	p.updateDirtyAtomic()
	return old.releaseWithErr()
}

func (p *PackStore) computeWatermarkAndSizes() (watermark uint64, sizes map[uint64]uint64) {
	if p.activePackID > 0 {
		watermark = p.activePackID - 1
	} else if p.nextPackID > 1 {
		watermark = p.nextPackID - 1
	}

	p.inFlightMu.Lock()
	for packID, cnt := range p.inFlightPacks {
		if cnt > 0 && packID > 0 && packID-1 < watermark {
			watermark = packID - 1
		}
	}
	p.inFlightMu.Unlock()

	if watermark == 0 {
		return 0, map[uint64]uint64{}
	}
	sizes = make(map[uint64]uint64, len(p.packSizes))
	for packID, sz := range p.packSizes {
		if packID <= watermark && packID != p.activePackID {
			if _, retired := p.retiredPacks[packID]; retired {
				continue
			}
			sizes[packID] = sz
		}
	}
	return watermark, sizes
}

func (p *PackStore) readBlockInternal(ref BlobRef) ([]byte, error) {
	if ref.PackID == 0 || ref.Length == 0 || ref.Length > MaxBlockSize {
		return nil, ErrInvalidValue
	}

	handle, err := p.acquireReadHandle(ref.PackID)
	if err != nil {
		return nil, err
	}
	defer handle.release()

	buf := make([]byte, ref.Length)
	n, err := handle.file.ReadAt(buf, int64(ref.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if uint32(n) != ref.Length {
		return nil, ErrInvalidValue
	}
	return buf, nil
}

func (p *PackStore) readBlockPinned(pin *readEpoch, ref BlobRef) ([]byte, error) {
	if pin == nil {
		return nil, ErrInvalid
	}
	return p.readBlockInternal(ref)
}

func (p *PackStore) readBlockForGC(ref BlobRef) ([]byte, error) {
	return p.readBlockInternal(ref)
}

func (p *PackStore) removeReadFileLocked(packID uint64) {
	handle, ok := p.readFiles[packID]
	if !ok {
		return
	}
	delete(p.readFiles, packID)
	if idx, hasIdx := p.readClockIdx[packID]; hasIdx {
		delete(p.readClockIdx, packID)
		last := len(p.readClock) - 1
		if idx != last {
			moved := p.readClock[last]
			p.readClock[idx] = moved
			p.readClockIdx[moved] = idx
		}
		p.readClock = p.readClock[:last]
		if len(p.readClock) == 0 || p.readClockHand >= len(p.readClock) {
			p.readClockHand = 0
		}
	}
	handle.release()
}

func (p *PackStore) evictReadFileForInsertLocked(limit int) {
	for len(p.readFiles) >= limit && len(p.readClock) > 0 {
		if p.readClockHand >= len(p.readClock) {
			p.readClockHand = 0
		}
		packID := p.readClock[p.readClockHand]
		h := p.readFiles[packID]
		if h != nil && h.accessed.Swap(false) {
			p.readClockHand++
			if p.readClockHand >= len(p.readClock) {
				p.readClockHand = 0
			}
			continue
		}
		p.removeReadFileLocked(packID)
	}
}

func (p *PackStore) acquireReadHandle(packID uint64) (*packReadHandle, error) {
	p.filesMu.RLock()
	if p.activeReadID == packID && p.activeReadHandle != nil && p.activeReadHandle.acquire() {
		h := p.activeReadHandle
		p.filesMu.RUnlock()
		return h, nil
	}
	if h := p.readFiles[packID]; h != nil && h.acquire() {
		if !h.accessed.Load() {
			h.accessed.Store(true)
		}
		p.filesMu.RUnlock()
		return h, nil
	}
	deleteGen := p.filesDeleteGen
	p.filesMu.RUnlock()

	file, err := os.Open(packFilePath(p.dir, packID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	p.filesMu.Lock()
	if p.activeReadID == packID && p.activeReadHandle != nil && p.activeReadHandle.acquire() {
		h := p.activeReadHandle
		p.filesMu.Unlock()
		_ = file.Close()
		return h, nil
	}
	if existing := p.readFiles[packID]; existing != nil && existing.acquire() {
		if !existing.accessed.Load() {
			existing.accessed.Store(true)
		}
		p.filesMu.Unlock()
		_ = file.Close()
		return existing, nil
	}
	nh := &packReadHandle{file: file}
	if p.closedFiles || p.filesDeleteGen != deleteGen {
		nh.refs.Store(1)
		p.filesMu.Unlock()
		return nh, nil
	}
	p.evictReadFileForInsertLocked(p.openReadPackLimit())
	nh.refs.Store(2)
	p.readFiles[packID] = nh
	p.readClockIdx[packID] = len(p.readClock)
	p.readClock = append(p.readClock, packID)
	p.filesMu.Unlock()
	return nh, nil
}

func (p *PackStore) PackSizesSnapshot() map[uint64]uint64 {
	_, sizes, err := p.SealedWatermarkAndSizes(context.Background())
	if err != nil {
		return nil
	}
	return maps.Clone(sizes)
}

func packFileName(packID uint64) string {
	var buf [21]byte
	const hexDigits = "0123456789abcdef"
	for i := 15; i >= 0; i-- {
		buf[i] = hexDigits[packID&0x0f]
		packID >>= 4
	}
	copy(buf[16:], ".pack")
	return string(buf[:])
}

func packFilePath(dir string, packID uint64) string {
	return filepath.Join(dir, packFileName(packID))
}

func parsePackID(name string) (uint64, error) {
	if !strings.HasSuffix(name, ".pack") {
		return 0, ErrInvalidValue
	}
	hexPart := strings.TrimSuffix(name, ".pack")
	if hexPart == "" || len(hexPart) > 16 {
		return 0, ErrInvalidValue
	}
	return strconv.ParseUint(hexPart, 16, 64)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	return errors.Join(syncErr, closeErr)
}
