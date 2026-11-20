package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const PackSegment uint64 = 64

type PackSegmentAllocator func(ctx context.Context, minNext uint64) (first, last uint64, err error)

const (
	BlobIDSegment           uint64 = 4096
	hotBlobShards                  = 16
	hotBlobSlots                   = 64
	hotBlobTotalBudget             = 64 << 20
	hotBlobShardBudget             = hotBlobTotalBudget / hotBlobShards
	hotExtendMinCap                = 16 << 10
	maxSyncBatchConcurrency        = 8
	packWritebackWindow     uint32 = 8 << 20
	writebackHintQueueCap          = 32
)

type writebackHint struct {
	packID uint64
	offset uint32
	length uint32
}

type packSyncFlight struct {
	targetSeq uint64
	done      chan struct{}
	err       error
}

type dirSyncFlight struct {
	targetSeq uint64
	done      chan struct{}
	err       error
}

type hotBlob struct {
	packID uint64
	offset uint32
	data   []byte
}

type hotShard struct {
	mu    sync.Mutex
	slots [hotBlobSlots]hotBlob
	head  int
	count int
	bytes int
}

type blobStore struct {
	dir         string
	logger      *slog.Logger
	metrics     *Metrics
	dirReady    atomic.Bool
	maxPackSize uint32

	nextPackID     atomic.Uint64
	reservedPackID atomic.Uint64
	allocMu        sync.Mutex
	minPackID      uint64
	lastReserved   uint64
	packAlloc      PackSegmentAllocator

	hot [hotBlobShards]hotShard

	atomicActivePackID atomic.Uint64
	mu                 sync.Mutex
	closed             bool
	curActivePackID    uint64
	activeFile         *os.File
	activeOffset       uint32
	activeFlushedTo    uint32
	writeSeq           uint64
	dirtyPacks         map[uint64]uint64
	unownedDirty       map[uint64]uint64
	inoPacks           map[uint64]map[uint64]uint64
	dirtyNew           map[uint64]struct{}
	dirDirty           bool
	dirDirtySeq        uint64
	syncFlights        map[uint64]*packSyncFlight
	dirFlight          *dirSyncFlight

	bgStarted    bool
	bgQueue      []uint64
	bgNotify     chan struct{}
	bgDone       chan struct{}
	syncFileHook func(packID uint64)

	wbStarted bool
	wbCh      chan writebackHint
	wbStop    chan struct{}
	wbDone    chan struct{}

	reclaimStarted  bool
	reclaimQueue    []uint64
	reclaimInFlight int
	reclaimNotify   chan struct{}
	reclaimIdleCh   chan struct{}
	reclaimDone     chan struct{}
	removePackHook  func(packID uint64)

	filesMu   sync.RWMutex
	packFiles map[uint64]*os.File

	writebackHintCount atomic.Uint64
}

func newBlobStore(
	base string,
	maxPackSize uint32,
	logger *slog.Logger,
	metrics *Metrics,
	packAlloc PackSegmentAllocator,
	syncFileHook, removePackHook func(uint64),
) *blobStore {
	if logger == nil {
		logger = slog.Default()
	}
	if maxPackSize == 0 {
		maxPackSize = PackMaxSize
	}
	bs := &blobStore{
		dir:            filepath.Join(base, "packs"),
		logger:         logger,
		metrics:        metrics,
		maxPackSize:    maxPackSize,
		packAlloc:      packAlloc,
		dirtyPacks:     make(map[uint64]uint64),
		unownedDirty:   make(map[uint64]uint64),
		inoPacks:       make(map[uint64]map[uint64]uint64),
		dirtyNew:       make(map[uint64]struct{}),
		syncFlights:    make(map[uint64]*packSyncFlight),
		packFiles:      make(map[uint64]*os.File),
		syncFileHook:   syncFileHook,
		removePackHook: removePackHook,
	}
	maxID := bs.scanExistingPacks()
	bs.minPackID = maxID
	bs.lastReserved = maxID
	return bs
}

func (s *blobStore) snapshotStats() (openFDs int, hotBytes, hotBlocks uint64) {
	s.filesMu.RLock()
	for _, f := range s.packFiles {
		if f != nil {
			openFDs++
		}
	}
	s.filesMu.RUnlock()

	for i := range s.hot {
		sh := &s.hot[i]
		sh.mu.Lock()
		if sh.bytes > 0 {
			hotBytes += uint64(sh.bytes)
		}
		if sh.count > 0 {
			hotBlocks += uint64(sh.count)
		}
		sh.mu.Unlock()
	}
	return openFDs, hotBytes, hotBlocks
}

func (s *blobStore) lockActiveMu() {
	if s.metrics != nil {
		start := time.Now()
		s.mu.Lock()
		s.metrics.observeBlobMuWait(time.Since(start))
		return
	}
	s.mu.Lock()
}

func (s *blobStore) writeAtTimed(file *os.File, data []byte, off int64) (int, error) {
	if s.metrics != nil {
		start := time.Now()
		n, err := file.WriteAt(data, off)
		s.metrics.observeBlobPwrite(time.Since(start))
		return n, err
	}
	return file.WriteAt(data, off)
}

func (s *blobStore) advanceMinPackID(minPackID uint64) {
	if minPackID == 0 {
		return
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()
	if minPackID > s.minPackID {
		s.minPackID = minPackID
	}
	if s.minPackID > s.lastReserved {
		s.lastReserved = s.minPackID
	}
	if next := s.nextPackID.Load(); next != 0 && next <= s.minPackID {
		s.nextPackID.Store(s.minPackID + 1)
	}
}

func (s *blobStore) activePackID() uint64 {
	return s.atomicActivePackID.Load()
}

func (s *blobStore) scanExistingPacks() uint64 {
	var maxID uint64
	for _, id := range s.existingPackIDs() {
		if id > maxID {
			maxID = id
		}
	}
	return maxID
}

func (s *blobStore) existingPackIDs() []uint64 {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	s.dirReady.Store(true)
	var ids []uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}
		if id, err := parsePackID(entry.Name()); err == nil && id > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

func (s *blobStore) allocPackID(ctx context.Context) (uint64, error) {
	for {
		next := s.nextPackID.Load()
		reserved := s.reservedPackID.Load()
		if next != 0 && next <= reserved {
			if s.nextPackID.CompareAndSwap(next, next+1) {
				return next, nil
			}
			continue
		}
		s.allocMu.Lock()
		next = s.nextPackID.Load()
		reserved = s.reservedPackID.Load()
		if next != 0 && next <= reserved {
			s.allocMu.Unlock()
			continue
		}
		if s.minPackID > s.lastReserved {
			s.lastReserved = s.minPackID
		}
		if s.lastReserved == math.MaxUint64 {
			s.allocMu.Unlock()
			return 0, ErrNoSpace
		}
		var first, last uint64
		if s.packAlloc != nil {
			allocCtx := ctx
			if allocCtx == nil {
				allocCtx = context.Background()
			}
			var err error
			first, last, err = s.packAlloc(allocCtx, s.lastReserved+1)
			if err != nil {
				s.allocMu.Unlock()
				return 0, err
			}
			if first <= s.lastReserved || last < first {
				s.allocMu.Unlock()
				return 0, ErrInvalidValue
			}
		} else {
			first = s.lastReserved + 1
			if math.MaxUint64-s.lastReserved < BlobIDSegment {
				last = math.MaxUint64
			} else {
				last = s.lastReserved + BlobIDSegment
			}
		}
		s.minPackID = max(s.minPackID, last)
		s.lastReserved = last
		s.reservedPackID.Store(last)
		if first == math.MaxUint64 {
			s.nextPackID.Store(0)
		} else {
			s.nextPackID.Store(first + 1)
		}
		s.allocMu.Unlock()
		return first, nil
	}
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
	var buf [22]byte
	const hexDigits = "0123456789abcdef"
	buf[0] = filepath.Separator
	for i := 16; i >= 1; i-- {
		buf[i] = hexDigits[packID&0x0f]
		packID >>= 4
	}
	copy(buf[17:], ".pack")
	return dir + string(buf[:])
}

func (s *blobStore) append(ctx context.Context, ino uint64, data []byte) (BlobRef, error) {
	if len(data) == 0 || len(data) > BlobSize {
		return BlobRef{}, ErrInvalidValue
	}
	maxCap := s.maxPackSize
	if uint32(len(data)) > maxCap {
		return BlobRef{}, ErrInvalidValue
	}

	s.lockActiveMu()
	if s.closed {
		s.mu.Unlock()
		return BlobRef{}, ErrClosed
	}

	if s.activeFile == nil || uint64(s.activeOffset)+uint64(len(data)) > uint64(maxCap) {
		if err := s.openNextActivePackLocked(ctx); err != nil {
			s.mu.Unlock()
			return BlobRef{}, err
		}
	}

	packID := s.curActivePackID
	offset := s.activeOffset
	file := s.activeFile

	if _, err := s.writeAtTimed(file, data, int64(offset)); err != nil {
		s.mu.Unlock()
		return BlobRef{}, err
	}

	s.activeOffset = offset + uint32(len(data))
	wbHint, wbCh := s.takeWritebackHintLocked(packID, s.activeOffset)
	s.markPackDirtyLocked(packID, ino)
	s.mu.Unlock()

	s.dispatchWritebackHint(wbCh, wbHint)

	ref := BlobRef{
		PackID: packID,
		Offset: offset,
		Length: uint32(len(data)),
	}
	s.cacheHotCopy(ref.PackID, ref.Offset, data)
	return ref, nil
}

func (s *blobStore) appendBlocks(ctx context.Context, ino uint64, data []byte) ([]BlobRef, error) {
	if len(data) == 0 || len(data)%BlobSize != 0 {
		return nil, ErrInvalidValue
	}
	maxCap := s.maxPackSize
	if maxCap < BlobSize {
		return nil, ErrInvalidValue
	}

	numBlocks := len(data) / BlobSize
	refs := make([]BlobRef, 0, numBlocks)

	s.lockActiveMu()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}

	var (
		pendingHints [4]writebackHint
		hintCount    int
		wbCh         chan writebackHint
	)
	var overflowHints []writebackHint

	for consumed := 0; consumed < len(data); {
		if s.activeFile == nil || uint64(s.activeOffset)+BlobSize > uint64(maxCap) {
			if err := s.openNextActivePackLocked(ctx); err != nil {
				s.mu.Unlock()
				return nil, err
			}
		}

		availBlocks := int((uint64(maxCap) - uint64(s.activeOffset)) / BlobSize)
		remBlocks := (len(data) - consumed) / BlobSize
		batchBlocks := min(availBlocks, remBlocks)
		batchBytes := batchBlocks * BlobSize

		packID := s.curActivePackID
		baseOffset := s.activeOffset
		file := s.activeFile

		if _, err := s.writeAtTimed(file, data[consumed:consumed+batchBytes], int64(baseOffset)); err != nil {
			s.mu.Unlock()
			return nil, err
		}

		s.activeOffset = baseOffset + uint32(batchBytes)
		if h, ch := s.takeWritebackHintLocked(packID, s.activeOffset); ch != nil {
			wbCh = ch
			if hintCount < len(pendingHints) {
				pendingHints[hintCount] = h
				hintCount++
			} else {
				overflowHints = append(overflowHints, h)
			}
		}
		s.markPackDirtyLocked(packID, ino)

		for b := range batchBlocks {
			refs = append(refs, BlobRef{
				PackID: packID,
				Offset: baseOffset + uint32(b*BlobSize),
				Length: BlobSize,
			})
		}
		consumed += batchBytes
	}
	s.mu.Unlock()

	for i := range hintCount {
		s.dispatchWritebackHint(wbCh, pendingHints[i])
	}
	for _, h := range overflowHints {
		s.dispatchWritebackHint(wbCh, h)
	}

	return refs, nil
}

func (s *blobStore) extendTail(ino uint64, oldRef BlobRef, patchOffset uint32, patchData []byte, newLength uint32) (BlobRef, bool, error) {
	if oldRef.PackID == 0 || oldRef.Length == 0 || newLength < oldRef.Length || newLength == 0 || newLength > BlobSize {
		return BlobRef{}, false, ErrInvalidValue
	}
	if uint64(patchOffset)+uint64(len(patchData)) > uint64(newLength) {
		return BlobRef{}, false, ErrInvalidValue
	}
	maxCap := s.maxPackSize

	s.lockActiveMu()
	if s.closed {
		s.mu.Unlock()
		return BlobRef{}, false, ErrClosed
	}
	if s.activeFile == nil ||
		oldRef.PackID != s.curActivePackID ||
		oldRef.Offset+oldRef.Length != s.activeOffset ||
		uint64(oldRef.Offset)+uint64(newLength) > uint64(maxCap) {
		s.mu.Unlock()
		return BlobRef{}, false, nil
	}

	file := s.activeFile
	packID := s.curActivePackID

	writeStart := patchOffset
	writePayload := patchData
	if patchOffset > oldRef.Length {
		holeLen := patchOffset - oldRef.Length
		writeStart = oldRef.Length
		writePayload = make([]byte, int(holeLen)+len(patchData))
		copy(writePayload[holeLen:], patchData)
	}
	if uint64(writeStart)+uint64(len(writePayload)) < uint64(newLength) && newLength > oldRef.Length {
		tailZeroStart := max(oldRef.Length, writeStart+uint32(len(writePayload)))
		if len(writePayload) == 0 {
			writeStart = tailZeroStart
			writePayload = make([]byte, newLength-tailZeroStart)
		} else {
			ext := make([]byte, newLength-writeStart)
			copy(ext, writePayload)
			writePayload = ext
		}
	}

	if len(writePayload) > 0 {
		if _, err := s.writeAtTimed(file, writePayload, int64(oldRef.Offset)+int64(writeStart)); err != nil {
			s.mu.Unlock()
			return BlobRef{}, false, err
		}
	}

	s.activeOffset = oldRef.Offset + newLength
	wbHint, wbCh := s.takeWritebackHintLocked(packID, s.activeOffset)
	s.markPackDirtyLocked(packID, ino)
	s.mu.Unlock()

	s.dispatchWritebackHint(wbCh, wbHint)

	newRef := BlobRef{
		PackID: oldRef.PackID,
		Offset: oldRef.Offset,
		Length: newLength,
	}
	s.extendHotShard(oldRef, patchOffset, patchData, newLength)
	return newRef, true, nil
}

func (s *blobStore) openNextActivePackLocked(ctx context.Context) error {
	if !s.dirReady.Load() {
		if err := os.MkdirAll(s.dir, 0o755); err != nil {
			return err
		}
		s.dirReady.Store(true)
	}
	packID, err := s.allocPackID(ctx)
	if err != nil {
		return err
	}
	path := packFilePath(s.dir, packID)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	prevPackID := s.curActivePackID

	s.filesMu.Lock()
	s.packFiles[packID] = file
	s.filesMu.Unlock()

	s.curActivePackID = packID
	s.atomicActivePackID.Store(packID)
	s.activeFile = file
	s.activeOffset = 0
	s.activeFlushedTo = 0
	s.dirtyNew[packID] = struct{}{}

	if prevPackID != 0 {
		if _, dirty := s.dirtyPacks[prevPackID]; dirty {
			s.enqueueBgSyncLocked(prevPackID)
		}
	}
	return nil
}

func (s *blobStore) takeWritebackHintLocked(packID uint64, newOffset uint32) (hint writebackHint, ch chan writebackHint) {
	if s.closed || packID == 0 || newOffset < packWritebackWindow {
		return writebackHint{}, nil
	}
	alignedEnd := (newOffset / packWritebackWindow) * packWritebackWindow
	if alignedEnd <= s.activeFlushedTo {
		return writebackHint{}, nil
	}
	hint = writebackHint{
		packID: packID,
		offset: s.activeFlushedTo,
		length: alignedEnd - s.activeFlushedTo,
	}
	s.activeFlushedTo = alignedEnd
	if !s.wbStarted {
		s.wbStarted = true
		s.wbCh = make(chan writebackHint, writebackHintQueueCap)
		s.wbStop = make(chan struct{})
		s.wbDone = make(chan struct{})
		go s.wbLoop(s.wbCh, s.wbStop, s.wbDone)
	}
	s.writebackHintCount.Add(1)
	return hint, s.wbCh
}

func (s *blobStore) dispatchWritebackHint(ch chan writebackHint, hint writebackHint) {
	if ch == nil || hint.packID == 0 || hint.length == 0 {
		return
	}
	select {
	case ch <- hint:
	default:
	}
}

func (s *blobStore) wbLoop(ch <-chan writebackHint, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-stop:
			return
		case h := <-ch:
			s.filesMu.RLock()
			f := s.packFiles[h.packID]
			if f != nil {
				_ = startAsyncWriteback(f, int64(h.offset), int64(h.length))
			}
			s.filesMu.RUnlock()
		}
	}
}

func (s *blobStore) enqueueBgSyncLocked(packID uint64) {
	if s.closed || packID == 0 {
		return
	}
	if !s.bgStarted {
		s.bgStarted = true
		s.bgNotify = make(chan struct{}, 1)
		s.bgDone = make(chan struct{})
		go s.bgSyncLoop()
	}
	if !slices.Contains(s.bgQueue, packID) {
		s.bgQueue = append(s.bgQueue, packID)
	}
	select {
	case s.bgNotify <- struct{}{}:
	default:
	}
}

func (s *blobStore) bgSyncLoop() {
	defer close(s.bgDone)
	for {
		s.mu.Lock()
		for len(s.bgQueue) == 0 && !s.closed {
			s.mu.Unlock()
			<-s.bgNotify
			s.mu.Lock()
		}
		if s.closed {
			s.bgQueue = nil
			s.mu.Unlock()
			return
		}
		packID := s.bgQueue[0]
		s.bgQueue = s.bgQueue[1:]
		s.mu.Unlock()

		if err := s.syncPackWithTrigger(packID, syncTriggerBgRotate); err != nil && !errors.Is(err, ErrClosed) {
			s.metrics.incBgSyncErrors()
			if s.logger != nil {
				s.logger.Error("background pack sync failed", "pack_id", packID, "err", err)
			}
		}
	}
}

func (s *blobStore) markPackDirtyLocked(packID, ino uint64) {
	s.writeSeq++
	seq := s.writeSeq
	s.dirtyPacks[packID] = seq
	if ino == 0 {
		s.unownedDirty[packID] = seq
		return
	}
	set := s.inoPacks[ino]
	if set == nil {
		set = make(map[uint64]uint64)
		s.inoPacks[ino] = set
	}
	set[packID] = seq
}

func (s *blobStore) markDirDirtyLocked() {
	s.dirDirtySeq++
	s.dirDirty = true
}

func (s *blobStore) tryReadHot(ref BlobRef, offset int64, dst []byte) bool {
	if len(dst) == 0 {
		return true
	}
	if offset < 0 || uint64(offset)+uint64(len(dst)) > uint64(ref.Length) {
		return false
	}
	if s.hotReadAt(ref.PackID, ref.Offset, offset, dst) {
		s.metrics.incReadHotCache()
		return true
	}
	return false
}

func (s *blobStore) readAtContext(ctx context.Context, ref BlobRef, offset int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if ref.PackID == 0 || ref.Length == 0 || ref.Length > BlobSize || uint64(ref.Offset)+uint64(ref.Length) > PackMaxSize {
		return ErrInvalidValue
	}
	if offset < 0 || uint64(offset)+uint64(len(dst)) > uint64(ref.Length) {
		return io.ErrUnexpectedEOF
	}
	if s.tryReadHot(ref, offset, dst) {
		return nil
	}
	return s.readRangeAtContext(ctx, ref.PackID, int64(ref.Offset)+offset, dst, 1)
}

func (s *blobStore) readRangeAtContext(ctx context.Context, packID uint64, packOffset int64, dst []byte, blockCount uint64) (err error) {
	if len(dst) == 0 {
		return nil
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if packID == 0 || packOffset < 0 || uint64(packOffset)+uint64(len(dst)) > PackMaxSize {
		return ErrInvalidValue
	}

	s.metrics.addReadDiskPack(max(1, blockCount))
	var span trace.Span
	if isSpanRecording(ctx) {
		_, span = startChildSpan(ctx, "store.BlobPread",
			attribute.Int64("pack.id", int64(packID)),
			attribute.Int64("pack.offset", packOffset),
			attribute.Int("read.bytes", len(dst)),
			attribute.Int64("read.blocks", int64(max(1, blockCount))),
		)
		defer func() { finishSpan(span, err) }()
	}

	file, err := s.getOrOpenPackFile(packID)
	if err != nil {
		return err
	}
	var preadStart time.Time
	if s.metrics != nil {
		preadStart = time.Now()
	}
	n, err := file.ReadAt(dst, packOffset)
	if s.metrics != nil {
		s.metrics.observeBlobPread(time.Since(preadStart))
	}
	if errors.Is(err, io.EOF) {
		if n == len(dst) {
			return nil
		}
		return io.ErrUnexpectedEOF
	}
	return err
}

func (s *blobStore) getOrOpenPackFile(packID uint64) (*os.File, error) {
	s.filesMu.RLock()
	if f, ok := s.packFiles[packID]; ok && f != nil {
		s.filesMu.RUnlock()
		return f, nil
	}
	s.filesMu.RUnlock()

	s.filesMu.Lock()
	defer s.filesMu.Unlock()
	if f, ok := s.packFiles[packID]; ok && f != nil {
		return f, nil
	}
	f, err := os.Open(packFilePath(s.dir, packID))
	if err != nil {
		return nil, err
	}
	s.packFiles[packID] = f
	return f, nil
}

func (s *blobStore) hotReadAt(packID uint64, blockOffset uint32, offset int64, dst []byte) bool {
	if offset < 0 {
		return false
	}
	sh := s.hotShardFor(packID, blockOffset)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		slot := &sh.slots[idx]
		if slot.packID == packID && slot.offset == blockOffset && slot.data != nil {
			end := offset + int64(len(dst))
			if end <= int64(len(slot.data)) {
				copy(dst, slot.data[offset:end])
				return true
			}
			return false
		}
	}
	return false
}

func (s *blobStore) hotShardFor(packID uint64, offset uint32) *hotShard {
	shardIdx := (packID + uint64(offset/BlobSize)) % hotBlobShards
	return &s.hot[shardIdx]
}

func (s *blobStore) cacheHotCopy(packID uint64, offset uint32, src []byte) {
	if len(src) > hotBlobShardBudget {
		return
	}
	sh := s.hotShardFor(packID, offset)
	sh.mu.Lock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].packID == packID && sh.slots[idx].offset == offset {
			oldData := sh.slots[idx].data
			var dst []byte
			if cap(oldData) >= len(src) {
				dst = oldData[:len(src)]
			} else {
				dst = make([]byte, len(src))
			}
			copy(dst, src)
			sh.bytes += len(src) - len(oldData)
			sh.slots[idx].data = dst
			evicted := sh.trimLocked()
			sh.mu.Unlock()
			s.metrics.addHotCacheEvictions(evicted)
			return
		}
	}

	var reused []byte
	var evictedCount uint64
	for (sh.count == len(sh.slots) || sh.bytes+len(src) > hotBlobShardBudget) && sh.count > 0 {
		evicted := sh.evictOldestTakeBufLocked()
		evictedCount++
		if cap(evicted) >= len(src) && cap(evicted) > cap(reused) {
			reused = evicted
		}
	}
	var dst []byte
	if cap(reused) >= len(src) {
		dst = reused[:len(src)]
	} else {
		dst = make([]byte, len(src))
	}
	copy(dst, src)

	idx := (sh.head + sh.count) % len(sh.slots)
	sh.slots[idx] = hotBlob{packID: packID, offset: offset, data: dst}
	sh.count++
	sh.bytes += len(dst)
	evictedCount += sh.trimLocked()
	sh.mu.Unlock()
	s.metrics.addHotCacheEvictions(evictedCount)
}

func (s *blobStore) extendHotShard(oldRef BlobRef, patchOffset uint32, patchData []byte, newLength uint32) {
	sh := s.hotShardFor(oldRef.PackID, oldRef.Offset)
	sh.mu.Lock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		slot := &sh.slots[idx]
		if slot.packID != oldRef.PackID || slot.offset != oldRef.Offset || slot.data == nil {
			continue
		}
		oldData := slot.data
		newLen := int(newLength)
		if newLen > hotBlobShardBudget {
			sh.evictBlockLocked(oldRef.PackID, oldRef.Offset)
			sh.mu.Unlock()
			s.metrics.addHotCacheEvictions(1)
			return
		}
		var newData []byte
		if newLen <= cap(oldData) {
			newData = oldData[:newLen]
			if newLen > len(oldData) {
				clear(newData[len(oldData):newLen])
			}
		} else {
			nextCap := max(hotExtendMinCap, cap(oldData)*2)
			for nextCap < newLen {
				nextCap *= 2
			}
			nextCap = min(BlobSize, max(nextCap, newLen))
			newData = make([]byte, newLen, nextCap)
			copy(newData, oldData)
		}
		if len(patchData) > 0 && int(patchOffset)+len(patchData) <= newLen {
			copy(newData[patchOffset:], patchData)
		}
		sh.bytes += newLen - len(oldData)
		slot.data = newData
		evicted := sh.trimLocked()
		sh.mu.Unlock()
		s.metrics.addHotCacheEvictions(evicted)
		return
	}
	sh.mu.Unlock()
}

func (sh *hotShard) trimLocked() uint64 {
	var evicted uint64
	for sh.bytes > hotBlobShardBudget && sh.count > 0 {
		sh.evictOldestLocked()
		evicted++
	}
	if sh.count == 0 {
		sh.bytes = 0
	}
	return evicted
}

func (sh *hotShard) evictOldestLocked() {
	_ = sh.evictOldestTakeBufLocked()
}

func (sh *hotShard) evictOldestTakeBufLocked() []byte {
	slot := &sh.slots[sh.head]
	buf := slot.data
	sh.bytes -= len(buf)
	*slot = hotBlob{}
	sh.head = (sh.head + 1) % len(sh.slots)
	sh.count--
	return buf
}

func (sh *hotShard) evictBlockLocked(packID uint64, offset uint32) {
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].packID != packID || sh.slots[idx].offset != offset {
			continue
		}
		sh.bytes -= len(sh.slots[idx].data)
		for j := i; j < sh.count-1; j++ {
			from := (sh.head + j + 1) % len(sh.slots)
			to := (sh.head + j) % len(sh.slots)
			sh.slots[to] = sh.slots[from]
		}
		tail := (sh.head + sh.count - 1) % len(sh.slots)
		sh.slots[tail] = hotBlob{}
		sh.count--
		return
	}
}

func (s *blobStore) evictHotPack(packID uint64) {
	for i := range s.hot {
		sh := &s.hot[i]
		sh.mu.Lock()
		for j := 0; j < sh.count; {
			idx := (sh.head + j) % len(sh.slots)
			if sh.slots[idx].packID != packID {
				j++
				continue
			}
			sh.bytes -= len(sh.slots[idx].data)
			for k := j; k < sh.count-1; k++ {
				from := (sh.head + k + 1) % len(sh.slots)
				to := (sh.head + k) % len(sh.slots)
				sh.slots[to] = sh.slots[from]
			}
			tail := (sh.head + sh.count - 1) % len(sh.slots)
			sh.slots[tail] = hotBlob{}
			sh.count--
		}
		if sh.count == 0 {
			sh.bytes = 0
		}
		sh.mu.Unlock()
	}
}

func (s *blobStore) sync() error {
	s.mu.Lock()
	if len(s.dirtyPacks) == 0 && !s.dirDirty {
		s.mu.Unlock()
		return nil
	}
	packs := make([]uint64, 0, len(s.dirtyPacks))
	for packID := range s.dirtyPacks {
		packs = append(packs, packID)
	}
	s.mu.Unlock()

	return s.syncBatch(packs, syncTriggerFlushWAL)
}

func (s *blobStore) sealAndSync(ctx context.Context, livePackIDs []uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.waitPendingReclaims()
	if err := s.sync(); err != nil {
		return err
	}

	s.lockActiveMu()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.activeFile != nil {
		packID := s.curActivePackID
		file := s.activeFile
		offset := s.activeOffset
		if offset > 0 {
			var syncStart time.Time
			if s.metrics != nil {
				syncStart = time.Now()
			}
			syncErr := file.Sync()
			if s.metrics != nil {
				s.metrics.observeBlobSync(syncTargetPackData, syncTriggerFlushWAL, time.Since(syncStart))
			}
			if syncErr != nil {
				s.mu.Unlock()
				return syncErr
			}
			s.filesMu.Lock()
			s.packFiles[packID] = file
			s.filesMu.Unlock()
			s.clearDirtyTrackingOnSealLocked(packID)
		} else {
			s.clearDirtyStateForPackLocked(packID)
			s.filesMu.Lock()
			delete(s.packFiles, packID)
			s.filesMu.Unlock()
			_ = file.Close()
			_ = os.Remove(packFilePath(s.dir, packID))
		}
		s.curActivePackID = 0
		s.atomicActivePackID.Store(0)
		s.activeFile = nil
		s.activeOffset = 0
		s.activeFlushedTo = 0
	}

	var residualDirty []uint64
	for _, id := range livePackIDs {
		if _, ok := s.dirtyPacks[id]; ok {
			residualDirty = append(residualDirty, id)
		}
	}
	needDirSync := s.dirDirty
	s.mu.Unlock()

	if len(residualDirty) > 0 || needDirSync {
		if err := s.syncBatch(residualDirty, syncTriggerFlushWAL); err != nil {
			return err
		}
	}
	return nil
}

func (s *blobStore) clearDirtyTrackingOnSealLocked(packID uint64) {
	delete(s.dirtyPacks, packID)
	delete(s.unownedDirty, packID)
	for owner, set := range s.inoPacks {
		if _, ok := set[packID]; ok {
			delete(set, packID)
			if len(set) == 0 {
				delete(s.inoPacks, owner)
			}
		}
	}
	if _, isNew := s.dirtyNew[packID]; isNew {
		delete(s.dirtyNew, packID)
		s.markDirDirtyLocked()
	}
	s.bgQueue = slices.DeleteFunc(s.bgQueue, func(id uint64) bool { return id == packID })
}

func (s *blobStore) syncInode(ino uint64) error {
	s.mu.Lock()
	var packs []uint64
	set := s.inoPacks[ino]
	if len(set) > 0 {
		packs = make([]uint64, 0, len(set)+len(s.unownedDirty))
		for packID := range set {
			packs = append(packs, packID)
		}
	}
	for packID := range s.unownedDirty {
		if _, exists := set[packID]; !exists {
			packs = append(packs, packID)
		}
	}
	if len(packs) == 0 && !s.dirDirty {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	return s.syncBatch(packs, syncTriggerFsyncBarrier)
}

func (s *blobStore) syncPackWithTrigger(packID uint64, trigger syncTrigger) error {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return ErrClosed
		}
		reqSeq, dirty := s.dirtyPacks[packID]
		if !dirty {
			s.mu.Unlock()
			return nil
		}
		if flight, ok := s.syncFlights[packID]; ok {
			s.mu.Unlock()
			<-flight.done
			if flight.err != nil {
				return flight.err
			}
			if flight.targetSeq >= reqSeq {
				return nil
			}
			continue
		}
		flight := &packSyncFlight{
			targetSeq: reqSeq,
			done:      make(chan struct{}),
		}
		s.syncFlights[packID] = flight
		hook := s.syncFileHook
		s.mu.Unlock()

		if hook != nil {
			hook(packID)
		}

		var syncErr error
		file, err := s.getOrOpenPackFile(packID)
		switch {
		case errors.Is(err, os.ErrNotExist):
			syncErr = nil
		case err != nil:
			syncErr = err
		default:
			var syncStart time.Time
			if s.metrics != nil {
				syncStart = time.Now()
			}
			err = syncFileData(file)
			if s.metrics != nil {
				s.metrics.observeBlobSync(syncTargetPackData, trigger, time.Since(syncStart))
			}
			if err != nil {
				syncErr = err
			}
		}

		s.mu.Lock()
		delete(s.syncFlights, packID)
		if syncErr == nil {
			s.completePackSyncLocked(packID, reqSeq)
		}
		flight.err = syncErr
		close(flight.done)
		s.mu.Unlock()

		return syncErr
	}
}

func (s *blobStore) completePackSyncLocked(packID, syncedSeq uint64) {
	if curSeq, ok := s.dirtyPacks[packID]; ok && curSeq <= syncedSeq {
		delete(s.dirtyPacks, packID)
	}
	if curSeq, ok := s.unownedDirty[packID]; ok && curSeq <= syncedSeq {
		delete(s.unownedDirty, packID)
	}
	for owner, set := range s.inoPacks {
		if curSeq, ok := set[packID]; ok && curSeq <= syncedSeq {
			delete(set, packID)
			if len(set) == 0 {
				delete(s.inoPacks, owner)
			}
		}
	}
	if _, isNew := s.dirtyNew[packID]; isNew {
		delete(s.dirtyNew, packID)
		s.markDirDirtyLocked()
	}
}

func (s *blobStore) syncPacksDirIfNeeded(trigger syncTrigger) error {
	for {
		s.mu.Lock()
		if !s.dirDirty {
			s.mu.Unlock()
			return nil
		}
		reqSeq := s.dirDirtySeq
		if flight := s.dirFlight; flight != nil {
			s.mu.Unlock()
			<-flight.done
			if flight.err != nil {
				return flight.err
			}
			if flight.targetSeq >= reqSeq {
				return nil
			}
			continue
		}
		flight := &dirSyncFlight{
			targetSeq: reqSeq,
			done:      make(chan struct{}),
		}
		s.dirFlight = flight
		s.mu.Unlock()

		syncErr := s.syncPacksDir(trigger)

		s.mu.Lock()
		s.dirFlight = nil
		if syncErr == nil && s.dirDirtySeq <= reqSeq {
			s.dirDirty = false
		}
		flight.err = syncErr
		close(flight.done)
		s.mu.Unlock()

		return syncErr
	}
}

func (s *blobStore) syncPacksDir(trigger syncTrigger) error {
	handle, err := os.Open(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var syncStart time.Time
	if s.metrics != nil {
		syncStart = time.Now()
	}
	syncErr := errors.Join(handle.Sync(), handle.Close())
	if s.metrics != nil {
		s.metrics.observeBlobSync(syncTargetPacksDir, trigger, time.Since(syncStart))
	}
	return syncErr
}

func (s *blobStore) syncBatch(packs []uint64, trigger syncTrigger) error {
	var errs []error
	switch len(packs) {
	case 0:
	case 1:
		if err := s.syncPackWithTrigger(packs[0], trigger); err != nil {
			errs = append(errs, err)
		}
	default:
		workers := min(len(packs), maxSyncBatchConcurrency)
		errList := make([]error, len(packs))
		var nextIdx atomic.Uint32
		var wg sync.WaitGroup
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				for {
					i := int(nextIdx.Add(1) - 1)
					if i >= len(packs) {
						return
					}
					errList[i] = s.syncPackWithTrigger(packs[i], trigger)
				}
			}()
		}
		wg.Wait()
		for _, err := range errList {
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := s.syncPacksDirIfNeeded(trigger); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *blobStore) enqueueRemovePacks(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		for _, packID := range packIDs {
			if packID != 0 {
				s.execReclaimPack(packID, nil)
			}
		}
		return
	}

	enqueued := false
	for _, packID := range packIDs {
		if packID == 0 {
			continue
		}
		s.clearDirtyStateForPackLocked(packID)
		if !slices.Contains(s.reclaimQueue, packID) {
			s.reclaimQueue = append(s.reclaimQueue, packID)
			enqueued = true
		}
	}
	if !enqueued {
		s.mu.Unlock()
		return
	}
	if !s.reclaimStarted {
		s.reclaimStarted = true
		s.reclaimNotify = make(chan struct{}, 1)
		s.reclaimDone = make(chan struct{})
		go s.reclaimLoop()
	}
	select {
	case s.reclaimNotify <- struct{}{}:
	default:
	}
	s.mu.Unlock()
}

func (s *blobStore) waitPendingReclaims() {
	for {
		s.mu.Lock()
		if len(s.reclaimQueue) == 0 && s.reclaimInFlight == 0 {
			s.mu.Unlock()
			return
		}
		if s.reclaimIdleCh == nil {
			s.reclaimIdleCh = make(chan struct{})
		}
		ch := s.reclaimIdleCh
		s.mu.Unlock()
		<-ch
	}
}

func (s *blobStore) reclaimLoop() {
	defer close(s.reclaimDone)
	for {
		s.mu.Lock()
		for len(s.reclaimQueue) == 0 && !s.closed {
			if s.reclaimInFlight == 0 && s.reclaimIdleCh != nil {
				close(s.reclaimIdleCh)
				s.reclaimIdleCh = nil
			}
			s.mu.Unlock()
			<-s.reclaimNotify
			s.mu.Lock()
		}
		if len(s.reclaimQueue) == 0 && s.closed {
			if s.reclaimInFlight == 0 && s.reclaimIdleCh != nil {
				close(s.reclaimIdleCh)
				s.reclaimIdleCh = nil
			}
			s.mu.Unlock()
			return
		}
		packID := s.reclaimQueue[0]
		s.reclaimQueue = s.reclaimQueue[1:]
		s.reclaimInFlight++
		hook := s.removePackHook
		s.mu.Unlock()

		s.execReclaimPack(packID, hook)

		s.mu.Lock()
		s.reclaimInFlight--
		if len(s.reclaimQueue) == 0 && s.reclaimInFlight == 0 && s.reclaimIdleCh != nil {
			close(s.reclaimIdleCh)
			s.reclaimIdleCh = nil
		}
		s.mu.Unlock()
	}
}

func (s *blobStore) execReclaimPack(packID uint64, hook func(uint64)) {
	if hook != nil {
		hook(packID)
	}
	remStart := time.Now()
	err := s.removePack(packID)
	s.metrics.observePhaseDeadPackRemove(time.Since(remStart))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.metrics.incPackRemoveErrors()
		if s.logger != nil {
			s.logger.Warn("dead pack remove failed", "pack_id", packID, "err", err)
		}
	}
}

func (s *blobStore) clearDirtyStateForPackLocked(packID uint64) {
	if s.curActivePackID == packID {
		s.curActivePackID = 0
		s.atomicActivePackID.Store(0)
		s.activeFile = nil
		s.activeOffset = 0
		s.activeFlushedTo = 0
	}
	delete(s.dirtyPacks, packID)
	delete(s.unownedDirty, packID)
	delete(s.dirtyNew, packID)
	for owner, set := range s.inoPacks {
		delete(set, packID)
		if len(set) == 0 {
			delete(s.inoPacks, owner)
		}
	}
	s.bgQueue = slices.DeleteFunc(s.bgQueue, func(id uint64) bool { return id == packID })
	s.markDirDirtyLocked()
}

func (s *blobStore) removePack(packID uint64) error {
	if packID == 0 {
		return nil
	}
	s.evictHotPack(packID)

	s.mu.Lock()
	s.clearDirtyStateForPackLocked(packID)
	flight := s.syncFlights[packID]
	s.mu.Unlock()

	if flight != nil {
		<-flight.done
	}

	s.filesMu.Lock()
	if f, ok := s.packFiles[packID]; ok {
		delete(s.packFiles, packID)
		if f != nil {
			_ = f.Close()
		}
	}
	s.filesMu.Unlock()

	err := os.Remove(packFilePath(s.dir, packID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err == nil {
		s.mu.Lock()
		s.markDirDirtyLocked()
		s.mu.Unlock()
	}
	return err
}

func (s *blobStore) close() error {
	s.mu.Lock()
	wasClosed := s.closed
	s.closed = true
	s.curActivePackID = 0
	s.atomicActivePackID.Store(0)
	s.activeFile = nil
	s.activeOffset = 0
	s.activeFlushedTo = 0
	s.bgQueue = nil
	bgStarted := s.bgStarted
	bgNotify := s.bgNotify
	bgDone := s.bgDone
	wbStarted := s.wbStarted
	wbStop := s.wbStop
	wbDone := s.wbDone
	reclaimStarted := s.reclaimStarted
	reclaimNotify := s.reclaimNotify
	reclaimDone := s.reclaimDone
	flights := make([]*packSyncFlight, 0, len(s.syncFlights))
	for _, f := range s.syncFlights {
		flights = append(flights, f)
	}
	dirFlight := s.dirFlight
	s.mu.Unlock()

	if wbStarted {
		if !wasClosed {
			close(wbStop)
		}
		<-wbDone
	}
	if bgStarted {
		select {
		case bgNotify <- struct{}{}:
		default:
		}
		<-bgDone
	}
	for _, f := range flights {
		<-f.done
	}
	if dirFlight != nil {
		<-dirFlight.done
	}
	if reclaimStarted {
		select {
		case reclaimNotify <- struct{}{}:
		default:
		}
		<-reclaimDone
	}

	s.filesMu.Lock()
	defer s.filesMu.Unlock()
	var errs []error
	for id, f := range s.packFiles {
		delete(s.packFiles, id)
		if f != nil {
			if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func parsePackID(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(name, ".pack"), 16, 64)
}
