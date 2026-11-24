package engine

import (
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	hotBlobShards      = 16
	hotBlobSlots       = 64
	hotBlobTotalBudget = 64 << 20
	hotBlobShardBudget = hotBlobTotalBudget / hotBlobShards
)

type packOpKind uint8

const (
	packOpAppend packOpKind = iota + 1
	packOpDelete
	packOpWatermark
	packOpSealActive
)

type packCmd struct {
	op          packOpKind
	blocks      [][]byte
	quotaExempt bool
	packID      uint64
	refs        []BlobRef
	watermark   uint64
	sizes       map[uint64]uint64
	err         error
	done        chan struct{}
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

type packReadHandle struct {
	file *os.File
	refs atomic.Int32
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
	if h.refs.Add(-1) == 0 {
		_ = h.file.Close()
	}
}

type PackActor struct {
	dir         string
	maxPackSize uint32
	maxBytes    atomic.Uint64

	mb struct {
		mu       sync.Mutex
		active   []*packCmd
		draining []*packCmd
		notify   chan struct{}
		closed   bool
	}

	nextPackID         uint64
	activePackID       uint64
	activeFile         *os.File
	activeOffset       uint32
	packSizes          map[uint64]uint64
	totalPhysicalBytes uint64
	atomicPhysical     atomic.Uint64

	inFlightMu    sync.Mutex
	inFlightPacks map[uint64]int

	hot [hotBlobShards]hotShard

	filesMu   sync.RWMutex
	readFiles map[uint64]*packReadHandle

	loopDone  chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newPackActor(dir string, maxPackSize uint32, maxBytes uint64) (*PackActor, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &PackActor{
		dir:           dir,
		maxPackSize:   maxPackSize,
		packSizes:     make(map[uint64]uint64),
		inFlightPacks: make(map[uint64]int),
		readFiles:     make(map[uint64]*packReadHandle),
		loopDone:      make(chan struct{}),
	}
	p.maxBytes.Store(maxBytes)
	p.mb.notify = make(chan struct{}, 1)

	if err := p.recoverExistingPacks(); err != nil {
		return nil, err
	}
	go p.runLoop()
	return p, nil
}

func (p *PackActor) recoverExistingPacks() error {
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

func (p *PackActor) Close() error {
	p.closeOnce.Do(func() {
		p.mb.mu.Lock()
		p.mb.closed = true
		select {
		case p.mb.notify <- struct{}{}:
		default:
		}
		p.mb.mu.Unlock()

		<-p.loopDone

		p.filesMu.Lock()
		for id, handle := range p.readFiles {
			delete(p.readFiles, id)
			handle.release()
		}
		p.filesMu.Unlock()
	})
	return p.closeErr
}

func (p *PackActor) PhysicalBytes() uint64 {
	return p.atomicPhysical.Load()
}

func (p *PackActor) MaxBytes() uint64 {
	return p.maxBytes.Load()
}

func (p *PackActor) SetMaxBytes(maxBytes uint64) {
	p.maxBytes.Store(maxBytes)
}

func (p *PackActor) submit(ctx context.Context, cmd *packCmd) error {
	p.mb.mu.Lock()
	if p.mb.closed {
		p.mb.mu.Unlock()
		return ErrClosed
	}
	p.mb.active = append(p.mb.active, cmd)
	select {
	case p.mb.notify <- struct{}{}:
	default:
	}
	p.mb.mu.Unlock()

	select {
	case <-cmd.done:
		return cmd.err
	case <-ctx.Done():
		<-cmd.done
		if cmd.op == packOpAppend && cmd.err == nil && len(cmd.refs) > 0 {
			p.ReleaseInFlight(cmd.refs)
		}
		return ctx.Err()
	}
}

func (p *PackActor) AppendBlocks(ctx context.Context, blocks [][]byte, quotaExempt bool) ([]BlobRef, error) {
	if len(blocks) == 0 {
		return nil, ErrInvalidValue
	}
	for _, blk := range blocks {
		if len(blk) == 0 || uint32(len(blk)) > MaxBlockSize || uint32(len(blk)) > p.maxPackSize {
			return nil, ErrInvalidValue
		}
	}
	cmd := &packCmd{
		op:          packOpAppend,
		blocks:      blocks,
		quotaExempt: quotaExempt,
		done:        make(chan struct{}),
	}
	if err := p.submit(ctx, cmd); err != nil {
		return nil, err
	}
	return cmd.refs, nil
}

func (p *PackActor) ReleaseInFlight(refs []BlobRef) {
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

func (p *PackActor) pinInFlight(refs []BlobRef) {
	if len(refs) == 0 {
		return
	}
	p.inFlightMu.Lock()
	for _, ref := range refs {
		p.inFlightPacks[ref.PackID]++
	}
	p.inFlightMu.Unlock()
}

func (p *PackActor) DeletePack(ctx context.Context, packID uint64) error {
	if packID == 0 {
		return ErrInvalidValue
	}
	cmd := &packCmd{
		op:     packOpDelete,
		packID: packID,
		done:   make(chan struct{}),
	}
	return p.submit(ctx, cmd)
}

func (p *PackActor) SealedWatermark(ctx context.Context) (uint64, error) {
	wm, _, err := p.SealedWatermarkAndSizes(ctx)
	return wm, err
}

func (p *PackActor) SealedWatermarkAndSizes(ctx context.Context) (watermark uint64, sizes map[uint64]uint64, err error) {
	cmd := &packCmd{
		op:   packOpWatermark,
		done: make(chan struct{}),
	}
	if err := p.submit(ctx, cmd); err != nil {
		return 0, nil, err
	}
	return cmd.watermark, cmd.sizes, nil
}

func (p *PackActor) SealActive(ctx context.Context) error {
	cmd := &packCmd{
		op:   packOpSealActive,
		done: make(chan struct{}),
	}
	return p.submit(ctx, cmd)
}

func (p *PackActor) runLoop() {
	defer close(p.loopDone)
	for {
		<-p.mb.notify
		for {
			p.mb.mu.Lock()
			if len(p.mb.active) == 0 {
				closed := p.mb.closed
				p.mb.mu.Unlock()
				if closed {
					p.closeErr = p.closeActivePack()
					return
				}
				break
			}
			p.mb.active, p.mb.draining = p.mb.draining[:0], p.mb.active
			p.mb.mu.Unlock()

			p.processTick(p.mb.draining)
		}
	}
}

func (p *PackActor) processTick(cmds []*packCmd) {
	activeDirty := false
	createdDir := false
	pendingReceipts := make([]*packCmd, 0, len(cmds))

	flushDirty := func() error {
		if !activeDirty && !createdDir {
			return nil
		}
		var syncErr error
		if activeDirty && p.activeFile != nil {
			if err := syncFileData(p.activeFile); err != nil {
				syncErr = err
			}
		}
		if createdDir {
			if err := syncDir(p.dir); err != nil && syncErr == nil {
				syncErr = err
			}
		}
		activeDirty = false
		createdDir = false
		return syncErr
	}

	finishPending := func(syncErr error) {
		for _, c := range pendingReceipts {
			if syncErr != nil {
				if c.op == packOpAppend && len(c.refs) > 0 {
					p.ReleaseInFlight(c.refs)
					c.refs = nil
				}
				c.err = syncErr
			}
			close(c.done)
		}
		pendingReceipts = pendingReceipts[:0]
	}

	for _, cmd := range cmds {
		switch cmd.op {
		case packOpAppend:
			refs, created, err := p.execAppend(cmd.blocks, cmd.quotaExempt)
			if err != nil {
				cmd.err = err
				close(cmd.done)
				continue
			}
			activeDirty = true
			if created {
				createdDir = true
			}
			p.pinInFlight(refs)
			cmd.refs = refs
			pendingReceipts = append(pendingReceipts, cmd)

		case packOpSealActive:
			syncErr := flushDirty()
			finishPending(syncErr)
			_ = p.closeActivePack()
			close(cmd.done)

		case packOpWatermark:
			syncErr := flushDirty()
			finishPending(syncErr)
			cmd.watermark, cmd.sizes = p.computeWatermarkAndSizes()
			close(cmd.done)

		case packOpDelete:
			syncErr := flushDirty()
			finishPending(syncErr)
			cmd.err = p.execDeletePack(cmd.packID)
			close(cmd.done)
		}
	}

	syncErr := flushDirty()
	finishPending(syncErr)
}

func (p *PackActor) execAppend(blocks [][]byte, quotaExempt bool) ([]BlobRef, bool, error) {
	var totalDelta uint64
	for _, blk := range blocks {
		totalDelta += uint64(len(blk))
	}
	maxB := p.maxBytes.Load()
	if !quotaExempt && maxB > 0 {
		if p.totalPhysicalBytes > maxB || totalDelta > maxB-p.totalPhysicalBytes {
			return nil, false, ErrNoSpace
		}
	}

	refs := make([]BlobRef, 0, len(blocks))
	createdDir := false

	for _, blk := range blocks {
		blkLen := uint32(len(blk))
		if p.activeFile == nil || uint64(p.activeOffset)+uint64(blkLen) > uint64(p.maxPackSize) {
			if err := p.openNextActivePack(); err != nil {
				return nil, createdDir, err
			}
			createdDir = true
		}

		packID := p.activePackID
		offset := p.activeOffset
		if _, err := p.activeFile.WriteAt(blk, int64(offset)); err != nil {
			return nil, createdDir, err
		}
		p.activeOffset = offset + blkLen
		p.packSizes[packID] = uint64(p.activeOffset)
		p.totalPhysicalBytes += uint64(blkLen)
		p.atomicPhysical.Store(p.totalPhysicalBytes)

		ref := BlobRef{
			PackID: packID,
			Offset: offset,
			Length: blkLen,
		}
		p.cacheHotCopy(ref.PackID, ref.Offset, blk)
		refs = append(refs, ref)
	}

	return refs, createdDir, nil
}

func (p *PackActor) openNextActivePack() error {
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
	p.activePackID = packID
	p.activeFile = file
	p.activeOffset = 0
	p.packSizes[packID] = 0
	return nil
}

func (p *PackActor) closeActivePack() error {
	if p.activeFile == nil {
		return nil
	}
	syncErr := syncFileData(p.activeFile)
	closeErr := p.activeFile.Close()
	p.activeFile = nil
	p.activePackID = 0
	p.activeOffset = 0
	return errors.Join(syncErr, closeErr)
}

func (p *PackActor) computeWatermarkAndSizes() (watermark uint64, sizes map[uint64]uint64) {
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
			sizes[packID] = sz
		}
	}
	return watermark, sizes
}

func (p *PackActor) execDeletePack(packID uint64) error {
	if packID == p.activePackID {
		return ErrInvalid
	}
	p.inFlightMu.Lock()
	inFlight := p.inFlightPacks[packID] > 0
	p.inFlightMu.Unlock()
	if inFlight {
		return ErrInvalid
	}

	p.evictHotPack(packID)

	p.filesMu.Lock()
	if handle, ok := p.readFiles[packID]; ok {
		delete(p.readFiles, packID)
		handle.release()
	}
	p.filesMu.Unlock()

	path := packFilePath(p.dir, packID)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if sz, ok := p.packSizes[packID]; ok {
		delete(p.packSizes, packID)
		if p.totalPhysicalBytes >= sz {
			p.totalPhysicalBytes -= sz
		} else {
			p.totalPhysicalBytes = 0
		}
		p.atomicPhysical.Store(p.totalPhysicalBytes)
	}
	return nil
}

func (p *PackActor) ReadBlock(ref BlobRef) ([]byte, error) {
	if ref.PackID == 0 || ref.Length == 0 || ref.Length > MaxBlockSize {
		return nil, ErrInvalidValue
	}
	if data, ok := p.lookupHot(ref.PackID, ref.Offset, ref.Length); ok {
		return data, nil
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

func (p *PackActor) acquireReadHandle(packID uint64) (*packReadHandle, error) {
	p.filesMu.RLock()
	h := p.readFiles[packID]
	if h != nil && h.acquire() {
		p.filesMu.RUnlock()
		return h, nil
	}
	p.filesMu.RUnlock()

	file, err := os.Open(packFilePath(p.dir, packID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	p.filesMu.Lock()
	if existing := p.readFiles[packID]; existing != nil && existing.acquire() {
		p.filesMu.Unlock()
		_ = file.Close()
		return existing, nil
	}
	nh := &packReadHandle{file: file}
	nh.refs.Store(2)
	p.readFiles[packID] = nh
	p.filesMu.Unlock()
	return nh, nil
}

func hotShardIdx(packID uint64, offset uint32) uint64 {
	return (packID*11400714819323198485 ^ uint64(offset)*2654435761) & (hotBlobShards - 1)
}

func (p *PackActor) cacheHotCopy(packID uint64, offset uint32, data []byte) {
	if len(data) == 0 || len(data) > hotBlobShardBudget {
		return
	}
	cp := append([]byte(nil), data...)
	sh := &p.hot[hotShardIdx(packID, offset)]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	for sh.count > 0 && (sh.count >= hotBlobSlots || sh.bytes+len(cp) > hotBlobShardBudget) {
		oldestIdx := (sh.head - sh.count + hotBlobSlots) % hotBlobSlots
		sh.bytes -= len(sh.slots[oldestIdx].data)
		sh.slots[oldestIdx] = hotBlob{}
		sh.count--
	}
	sh.slots[sh.head] = hotBlob{
		packID: packID,
		offset: offset,
		data:   cp,
	}
	sh.head = (sh.head + 1) % hotBlobSlots
	sh.count++
	sh.bytes += len(cp)
}

func (p *PackActor) lookupHot(packID uint64, offset, length uint32) ([]byte, bool) {
	sh := &p.hot[hotShardIdx(packID, offset)]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	for i := range sh.count {
		idx := (sh.head - 1 - i + hotBlobSlots) % hotBlobSlots
		slot := sh.slots[idx]
		if slot.packID == packID && slot.offset == offset && uint32(len(slot.data)) == length {
			out := make([]byte, len(slot.data))
			copy(out, slot.data)
			return out, true
		}
	}
	return nil, false
}

func (p *PackActor) evictHotPack(packID uint64) {
	for i := range p.hot {
		sh := &p.hot[i]
		sh.mu.Lock()
		for j := range sh.slots {
			if sh.slots[j].packID == packID {
				sh.bytes -= len(sh.slots[j].data)
				sh.slots[j] = hotBlob{}
			}
		}
		sh.mu.Unlock()
	}
}

func (p *PackActor) PackSizesSnapshot() map[uint64]uint64 {
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
