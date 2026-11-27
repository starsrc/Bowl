package fusefs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/starsrc/pkg/serverx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	MountModeStaged = "staged"

	MountModeSync = "sync"

	MountModeRO = "ro"

	DefaultStageSize uint64 = 512 * 1024 * 1024

	MinStageSize uint64 = MaxRPCChunkSize

	DefaultStageDir = "/var/tmp/bowl-stage"

	StageChunkSize = MaxRPCChunkSize

	StageBlockAlign = 256 * 1024

	StageWorkers = 8

	stageBackoffInitial = 100 * time.Millisecond
	stageBackoffMax     = 2 * time.Second
	maxStageSpanLinks   = 32
)

var (
	stageFreeCh  = make(chan *[StageChunkSize]byte, StageWorkers)
	stageBufPool = sync.Pool{
		New: func() any {
			return new([StageChunkSize]byte)
		},
	}
)

func acquireStageBuf() *[StageChunkSize]byte {
	select {
	case buf := <-stageFreeCh:
		return buf
	default:
	}
	buf, _ := stageBufPool.Get().(*[StageChunkSize]byte)
	if buf == nil {
		return new([StageChunkSize]byte)
	}
	return buf
}

func releaseStageBuf(buf *[StageChunkSize]byte) {
	if buf == nil {
		return
	}
	select {
	case stageFreeCh <- buf:
	default:
		stageBufPool.Put(buf)
	}
}

type StageDirSession struct {
	mu           sync.Mutex
	stageDir     string
	instanceDir  string
	lockFile     *os.File
	isTmpfs      bool
	cleanedCount int
	closed       bool
}

func (s *StageDirSession) InstanceDir() string {
	if s == nil {
		return ""
	}
	return s.instanceDir
}

func (s *StageDirSession) IsTmpfs() bool {
	if s == nil {
		return false
	}
	return s.isTmpfs
}

func (s *StageDirSession) CleanedCount() int {
	if s == nil {
		return 0
	}
	return s.cleanedCount
}

func (s *StageDirSession) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	lf := s.lockFile
	s.lockFile = nil
	instDir := s.instanceDir
	s.mu.Unlock()

	var firstErr error
	if lf != nil {
		if err := lf.Close(); err != nil {
			firstErr = err
		}
	}
	if instDir != "" {
		if err := os.RemoveAll(instDir); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func OpenStageDirSession(stageDir string) (*StageDirSession, error) {
	if strings.TrimSpace(stageDir) == "" {
		return nil, errors.New("stage dir must not be empty")
	}
	cleanDir := filepath.Clean(stageDir)
	if err := ensureStageRootDir(cleanDir); err != nil {
		return nil, err
	}

	cleaned := cleanupDefunctStageEntries(cleanDir)

	var randBytes [4]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		return nil, fmt.Errorf("generate stage instance id: %w", err)
	}
	suffix := fmt.Sprintf("%d-%s", os.Getpid(), hex.EncodeToString(randBytes[:]))
	initDir := filepath.Join(cleanDir, ".init-"+suffix)
	instDir := filepath.Join(cleanDir, "inst-"+suffix)
	if err := os.Mkdir(initDir, 0o700); err != nil {
		return nil, fmt.Errorf("create stage init dir %q: %w", initDir, err)
	}

	lockPath := filepath.Join(initDir, ".lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		_ = os.RemoveAll(initDir)
		return nil, fmt.Errorf("open stage lock file %q: %w", lockPath, err)
	}
	if flockErr := flockExNB(lockFile); flockErr != nil {
		_ = lockFile.Close()
		_ = os.RemoveAll(initDir)
		return nil, fmt.Errorf("lock stage instance %q: %w", lockPath, flockErr)
	}
	if renameErr := os.Rename(initDir, instDir); renameErr != nil {
		_ = lockFile.Close()
		_ = os.RemoveAll(initDir)
		return nil, fmt.Errorf("publish stage instance dir %q: %w", instDir, renameErr)
	}

	isTmpfs, err := CheckTmpfs(cleanDir)
	if err != nil {
		_ = lockFile.Close()
		_ = os.RemoveAll(instDir)
		return nil, fmt.Errorf("check tmpfs for %q: %w", cleanDir, err)
	}

	return &StageDirSession{
		stageDir:     cleanDir,
		instanceDir:  instDir,
		lockFile:     lockFile,
		isTmpfs:      isTmpfs,
		cleanedCount: cleaned,
	}, nil
}

func ensureStageRootDir(cleanDir string) error {
	parent := filepath.Dir(cleanDir)
	if parent != "" && parent != cleanDir {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("create parent dir for %q: %w", cleanDir, err)
		}
	}
	err := os.Mkdir(cleanDir, 0o755)
	if err == nil {
		if cleanDir == DefaultStageDir {
			if chmodErr := os.Chmod(cleanDir, os.ModeSticky|0o777); chmodErr != nil {
				return fmt.Errorf("chmod default stage dir %q: %w", cleanDir, chmodErr)
			}
		}
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create stage dir %q: %w", cleanDir, err)
	}
	info, statErr := os.Stat(cleanDir)
	if statErr != nil {
		return fmt.Errorf("stat stage dir %q: %w", cleanDir, statErr)
	}
	if !info.IsDir() {
		return fmt.Errorf("stage path %q exists and is not a directory", cleanDir)
	}
	return nil
}

func cleanupDefunctStageEntries(cleanDir string) int {
	entries, err := os.ReadDir(cleanDir)
	if err != nil {
		return 0
	}
	cleaned := 0
	for _, entry := range entries {
		name := entry.Name()
		fullPath := filepath.Join(cleanDir, name)
		if !entry.IsDir() {
			if rmErr := os.Remove(fullPath); rmErr == nil {
				cleaned++
			}
			continue
		}
		if strings.HasPrefix(name, ".init-") {
			if pid, ok := parseStageDirPID(name, ".init-"); !ok || !isPIDAlive(pid) {
				if rmErr := os.RemoveAll(fullPath); rmErr == nil {
					cleaned++
				}
			}
			continue
		}
		if !strings.HasPrefix(name, "inst-") {
			continue
		}
		lockPath := filepath.Join(fullPath, ".lock")
		lf, openErr := os.OpenFile(lockPath, os.O_RDWR|unix.O_CLOEXEC, 0)
		if openErr == nil {
			flockErr := flockExNB(lf)
			_ = lf.Close()
			if flockErr == nil {
				if rmErr := os.RemoveAll(fullPath); rmErr == nil {
					cleaned++
				}
			}
			continue
		}

		if errors.Is(openErr, os.ErrNotExist) {
			if pid, ok := parseStageDirPID(name, "inst-"); !ok || !isPIDAlive(pid) {
				if rmErr := os.RemoveAll(fullPath); rmErr == nil {
					cleaned++
				}
			}
		}
	}
	return cleaned
}

func flockExNB(f *os.File) error {
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err
	}
}

func parseStageDirPID(name, prefix string) (int, bool) {
	rest, ok := strings.CutPrefix(name, prefix)
	if !ok {
		return 0, false
	}
	pidStr, _, ok := strings.Cut(rest, "-")
	if !ok {
		return 0, false
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

func isPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := unix.Kill(pid, 0)
	if err == nil || errors.Is(err, unix.EPERM) {
		return true
	}
	return !errors.Is(err, unix.ESRCH)
}

func writeAtFull(f *os.File, data []byte, offset int64) error {
	written := 0
	for written < len(data) {
		n, err := f.WriteAt(data[written:], offset+int64(written))
		if n > 0 {
			written += n
		}
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func readAtFull(f *os.File, buf []byte, offset int64) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := f.ReadAt(buf[total:], offset+int64(total))
		if n > 0 {
			total += n
		}
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			return total, nil
		}
	}
	return total, nil
}

type StageChunkWriter func(ctx context.Context, ino, offset uint64, data []byte) (uint32, *storagev1.Inode, error)

type StageChunkReader func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error)

type StageHooks struct {
	OnCommit func(nodeID, ino uint64, inode *storagev1.Inode)
}

type stageItemState uint8

const (
	stageStateActive stageItemState = iota
	stageStateQueued
	stageStateInflight
	stageStateDone
)

type stageItem struct {
	seq        uint64
	state      stageItemState
	ino        uint64
	nodeID     uint64
	offset     uint64
	length     uint64
	path       string
	gen        uint64
	inodeState *stageInode
	links      []trace.Link

	readers   int
	uploaded  bool
	discarded atomic.Bool
	unlinked  bool
}

func (it *stageItem) isAlignedFullChunk() bool {
	return it.length == StageChunkSize && it.offset%StageBlockAlign == 0
}

func (it *stageItem) blockRange() (startBlock, endBlock uint64) {
	startBlock = it.offset / StageBlockAlign
	if it.length == 0 {
		return startBlock, startBlock
	}
	endBlock = (it.offset + it.length + StageBlockAlign - 1) / StageBlockAlign
	return startBlock, endBlock
}

func (it *stageItem) blocksOverlap(other *stageItem) bool {
	aStart, aEnd := it.blockRange()
	bStart, bEnd := other.blockRange()
	return aStart < bEnd && bStart < aEnd
}

func (it *stageItem) recordSpanLinkLocked(ctx context.Context) {
	if ctx == nil || len(it.links) >= maxStageSpanLinks {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() || !sc.IsSampled() {
		return
	}
	for i := range it.links {
		if it.links[i].SpanContext.SpanID() == sc.SpanID() {
			return
		}
	}
	it.links = append(it.links, trace.Link{SpanContext: sc})
}

type stageInode struct {
	ino               uint64
	nodeID            uint64
	active            *stageItem
	pending           []*stageItem
	activeWriteBusy   bool
	discarding        bool
	lastWriteUnixNano int64
	streamGen         uint64
	errGen            uint64
	errSeq            uint64
	lastErr           error
	defaultSeenSeq    uint64
	discardCh         chan struct{}
}

func newStageInode(ino uint64) *stageInode {
	return &stageInode{
		ino:       ino,
		streamGen: 1,
		discardCh: make(chan struct{}),
	}
}

func (st *stageInode) hasPendingLocked() bool {
	return st.active != nil || len(st.pending) > 0
}

func (st *stageInode) hasInflightLocked() bool {
	for _, it := range st.pending {
		if it.state == stageStateInflight {
			return true
		}
	}
	return false
}

func (st *stageInode) dirtyEndLocked() (end uint64, hasDirty bool) {
	if st.active != nil && st.active.length > 0 && !st.active.discarded.Load() && !st.active.uploaded {
		hasDirty = true
		if chunkEnd := st.active.offset + st.active.length; chunkEnd > end {
			end = chunkEnd
		}
	}
	for _, it := range st.pending {
		if it == nil || it.length == 0 || it.discarded.Load() || it.uploaded {
			continue
		}
		hasDirty = true
		if chunkEnd := it.offset + it.length; chunkEnd > end {
			end = chunkEnd
		}
	}
	return end, hasDirty
}

func (st *stageInode) resetStreamLocked() {
	st.streamGen++
}

func (st *stageInode) hasUnseenErrLocked(c *StageCoordinator, fh uint64) bool {
	if st.errSeq == 0 {
		return false
	}
	if fh == 0 {
		return st.defaultSeenSeq < st.errSeq
	}
	return c.handleSeenSeq(fh) < st.errSeq
}

func (st *stageInode) consumeErrLocked(c *StageCoordinator, fh uint64) error {
	if st.errSeq == 0 {
		return nil
	}
	if fh == 0 {
		if st.defaultSeenSeq >= st.errSeq {
			return nil
		}
		st.defaultSeenSeq = st.errSeq
		st.resetStreamLocked()
		return st.lastErr
	}
	if !c.advanceHandleSeenSeq(fh, st.errSeq) {
		return nil
	}
	st.resetStreamLocked()
	return st.lastErr
}

type StageCoordinator struct {
	instanceDir string
	stageSize   uint64
	writer      StageChunkWriter
	hooks       StageHooks

	nextSeq          atomic.Uint64
	totalActiveItems atomic.Int64
	pendingChunks    atomic.Int64
	errSeqGen        atomic.Uint64
	aborted          atomic.Bool

	seqMu     sync.Mutex
	handleSeq map[uint64]uint64

	clearMu         sync.Mutex
	mu              sync.Mutex
	inodes          map[uint64]*stageInode
	queue           []*stageItem
	usedBytes       uint64
	inflightCount   int
	inflightBytes   uint64
	abortErr        error
	abortErrSeq     uint64
	abortCtx        context.Context
	abortCancel     context.CancelFunc
	closed          bool
	stopWorkers     bool
	workersDone     bool
	bpWakeCh        chan struct{}
	progressCh      chan struct{}
	stateCond       *sync.Cond
	workerCond      *sync.Cond
	stopRetryCh     chan struct{}
	stopRetryOnce   sync.Once
	workerWG        sync.WaitGroup
	metrics         atomic.Pointer[Metrics]
	logger          atomic.Pointer[slog.Logger]
	tracerProv      atomic.Pointer[trace.TracerProvider]
	backoffInit     time.Duration
	backoffLimit    time.Duration
	writeChunkPiece func(path string, curOff, oldOffset uint64, piece []byte) error
}

func NewStageCoordinator(
	instanceDir string,
	stageSize uint64,
	writer StageChunkWriter,
	hooks StageHooks,
) *StageCoordinator {
	if stageSize == 0 {
		stageSize = DefaultStageSize
	}
	abortCtx, abortCancel := context.WithCancel(context.Background())
	c := &StageCoordinator{
		instanceDir:  instanceDir,
		stageSize:    stageSize,
		writer:       writer,
		hooks:        hooks,
		handleSeq:    make(map[uint64]uint64),
		inodes:       make(map[uint64]*stageInode),
		abortCtx:     abortCtx,
		abortCancel:  abortCancel,
		progressCh:   make(chan struct{}, 1),
		stopRetryCh:  make(chan struct{}),
		backoffInit:  stageBackoffInitial,
		backoffLimit: stageBackoffMax,
	}
	c.stateCond = sync.NewCond(&c.mu)
	c.workerCond = sync.NewCond(&c.mu)
	for range StageWorkers {
		c.workerWG.Add(1)
		go c.uploadWorker()
	}
	return c
}

func (c *StageCoordinator) bindMetrics(m *Metrics) {
	if c == nil {
		return
	}
	c.metrics.Store(m)
}

func (c *StageCoordinator) bindLogger(l *slog.Logger) {
	if c == nil {
		return
	}
	c.logger.Store(l)
}

func (c *StageCoordinator) activeLogger() *slog.Logger {
	if c != nil {
		if l := c.logger.Load(); l != nil {
			return l
		}
	}
	return slog.Default()
}

func (c *StageCoordinator) bindTracerProvider(tp trace.TracerProvider) {
	if c == nil {
		return
	}
	if tp == nil {
		c.tracerProv.Store(nil)
		return
	}
	c.tracerProv.Store(&tp)
}

func (c *StageCoordinator) activeTracer() (trace.Tracer, bool) {
	if c == nil {
		return nil, false
	}
	ptr := c.tracerProv.Load()
	if ptr == nil || *ptr == nil {
		return nil, false
	}
	tp := *ptr
	if _, isNoop := tp.(noop.TracerProvider); isNoop {
		return nil, false
	}
	return tp.Tracer("bowl/client/fusefs"), true
}

func (c *StageCoordinator) gauges() (pendingChunks int64, usedBytes, capacityBytes uint64) {
	if c == nil {
		return 0, 0, 0
	}
	c.mu.Lock()
	used := c.usedBytes
	capBytes := c.stageSize
	c.mu.Unlock()
	if c.aborted.Load() {
		return 0, 0, capBytes
	}
	return max(int64(0), c.pendingChunks.Load()), used, capBytes
}

func (c *StageCoordinator) unmountSnapshot() (pendingChunks int64, stagedInodes []uint64) {
	if c == nil || c.aborted.Load() {
		return 0, nil
	}
	c.mu.Lock()
	for ino, st := range c.inodes {
		if st != nil && (st.hasPendingLocked() || st.activeWriteBusy || st.active != nil) {
			stagedInodes = append(stagedInodes, ino)
		}
	}
	pendingChunks = max(int64(0), c.pendingChunks.Load())
	c.mu.Unlock()
	return pendingChunks, stagedInodes
}

func (c *StageCoordinator) pendingChunkCount() int64 {
	if c == nil || c.aborted.Load() {
		return 0
	}
	return max(int64(0), c.pendingChunks.Load())
}

func (c *StageCoordinator) RegisterHandle(fh uint64) {
	if c == nil || fh == 0 {
		return
	}
	seq := c.errSeqGen.Load()
	if seq == 0 {
		return
	}
	c.seqMu.Lock()
	if c.handleSeq == nil {
		c.handleSeq = make(map[uint64]uint64)
	}
	c.handleSeq[fh] = seq
	c.seqMu.Unlock()
}

func (c *StageCoordinator) ForgetHandle(fh uint64) {
	if c == nil || fh == 0 || c.errSeqGen.Load() == 0 {
		return
	}
	c.seqMu.Lock()
	delete(c.handleSeq, fh)
	c.seqMu.Unlock()
}

func (c *StageCoordinator) handleSeenSeq(fh uint64) uint64 {
	c.seqMu.Lock()
	seen := c.handleSeq[fh]
	c.seqMu.Unlock()
	return seen
}

func (c *StageCoordinator) advanceHandleSeenSeq(fh, target uint64) bool {
	c.seqMu.Lock()
	if c.handleSeq == nil {
		c.handleSeq = make(map[uint64]uint64)
	}
	if c.handleSeq[fh] >= target {
		c.seqMu.Unlock()
		return false
	}
	c.handleSeq[fh] = target
	c.seqMu.Unlock()
	return true
}

func (c *StageCoordinator) getOrCreateInodeLocked(ino uint64) *stageInode {
	st := c.inodes[ino]
	if st == nil {
		st = newStageInode(ino)
		c.inodes[ino] = st
	}
	return st
}

func (c *StageCoordinator) wakeBackpressureLocked() {
	if c.bpWakeCh != nil {
		close(c.bpWakeCh)
		c.bpWakeCh = nil
	}
}

func (c *StageCoordinator) drainingBytesLocked() uint64 {
	draining := c.inflightBytes
	for _, qIt := range c.queue {
		if qIt != nil {
			draining += qIt.length
		}
	}
	return draining
}

func (c *StageCoordinator) pruneIdleInodeLocked(st *stageInode) {
	if st == nil || st.hasPendingLocked() || st.activeWriteBusy || st.discarding || st.errSeq != 0 {
		return
	}
	if c.inodes[st.ino] == st {
		delete(c.inodes, st.ino)
	}
}

func (c *StageCoordinator) sealActiveLocked(st *stageInode) bool {
	if st == nil || st.active == nil || st.activeWriteBusy || c.stopWorkers || c.aborted.Load() {
		return false
	}
	it := st.active
	st.active = nil
	if it.length == 0 {
		it.state = stageStateDone
		c.pendingChunks.Add(-1)
		c.totalActiveItems.Add(-1)
		if !st.hasPendingLocked() {
			st.lastWriteUnixNano = 0
			c.pruneIdleInodeLocked(st)
		}
		return false
	}
	it.state = stageStateQueued
	st.pending = append(st.pending, it)
	c.queue = append(c.queue, it)
	c.workerCond.Broadcast()
	return true
}

func (c *StageCoordinator) sealAllIdleActiveLocked() bool {
	sealedAny := false
	for _, st := range c.inodes {
		if c.sealActiveLocked(st) {
			sealedAny = true
		}
	}
	return sealedAny
}

func (c *StageCoordinator) HasAnyPending() bool {
	return c != nil && !c.aborted.Load() && c.totalActiveItems.Load() > 0
}

func (c *StageCoordinator) HasPending(ino uint64) bool {
	if c == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return false
	}
	c.mu.Lock()
	st := c.inodes[ino]
	pending := st != nil && st.hasPendingLocked()
	c.mu.Unlock()
	return pending
}

func (c *StageCoordinator) OverlayDirty(ino uint64, attr *fuse.Attr) bool {
	if c == nil || attr == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return false
	}
	c.mu.Lock()
	if c.aborted.Load() {
		c.mu.Unlock()
		return false
	}
	st := c.inodes[ino]
	if st == nil || st.discarding {
		c.mu.Unlock()
		return false
	}
	dirtyEnd, hasDirty := st.dirtyEndLocked()
	if !hasDirty {
		c.mu.Unlock()
		return false
	}
	if dirtyEnd > attr.Size {
		attr.Size = dirtyEnd
		minBlocks := (attr.Size + 511) / 512
		if minBlocks > attr.Blocks {
			attr.Blocks = minBlocks
		}
	}
	if st.lastWriteUnixNano > 0 {
		sec := uint64(st.lastWriteUnixNano / int64(time.Second))
		nsec := uint32(st.lastWriteUnixNano % int64(time.Second))
		if sec > attr.Mtime || (sec == attr.Mtime && nsec > attr.Mtimensec) {
			attr.Mtime = sec
			attr.Mtimensec = nsec
		}
		if sec > attr.Ctime || (sec == attr.Ctime && nsec > attr.Ctimensec) {
			attr.Ctime = sec
			attr.Ctimensec = nsec
		}
	}
	c.mu.Unlock()
	return true
}

func (c *StageCoordinator) Write(ctx context.Context, ino, nodeID, offset uint64, data []byte) (written uint32, handled bool, err error) {
	return c.WriteForHandle(ctx, nil, 0, ino, nodeID, offset, data)
}

func (c *StageCoordinator) WriteForHandle(
	ctx context.Context,
	cancel <-chan struct{},
	fh, ino, nodeID, offset uint64,
	data []byte,
) (written uint32, handled bool, err error) {
	if c == nil || len(data) == 0 || len(data) > StageChunkSize || c.writer == nil {
		return 0, false, nil
	}

	rem := data
	curOff := offset

	for len(rem) > 0 {
		step, earlyDone, earlyErr := c.prepareWriteStep(ctx, cancel, fh, ino, nodeID, curOff, rem)
		if earlyDone {
			if written > 0 {
				return written, true, nil
			}
			return 0, true, earlyErr
		}

		piece := rem[:step.take]
		writePieceFn := c.writeChunkPiece
		if writePieceFn == nil {
			writePieceFn = writeChunkFilePiece
		}
		ioErr := writePieceFn(step.chunkPath, curOff, step.oldOffset, piece)
		if ioErr != nil && step.oldLength > 0 {
			_ = os.Truncate(step.chunkPath, int64(step.oldLength))
		}

		c.mu.Lock()
		if c.aborted.Load() {
			abortErr := c.abortErr
			pathToRemove := c.abortActiveWriteStepLocked(step.st, step.activeChunk, step.oldLength, step.deltaBytes)
			c.mu.Unlock()
			if pathToRemove != "" {
				_ = os.Remove(pathToRemove)
			}
			return 0, true, abortErr
		}
		if ioErr != nil {
			c.rollbackWriteStepLocked(step.st, step.activeChunk, step.oldLength, step.deltaBytes)
			c.mu.Unlock()
			if written > 0 {
				return written, true, nil
			}
			return 0, true, ioErr
		}

		step.activeChunk.length = step.oldLength + uint64(step.take)
		step.activeChunk.recordSpanLinkLocked(ctx)
		step.st.nodeID = nodeID
		step.st.lastWriteUnixNano = time.Now().UnixNano()
		step.st.activeWriteBusy = false
		c.stateCond.Broadcast()
		c.wakeBackpressureLocked()
		if step.activeChunk.length == StageChunkSize {
			c.sealActiveLocked(step.st)
		}
		c.mu.Unlock()

		rem = rem[step.take:]
		curOff += uint64(step.take)
		written += uint32(step.take)
	}

	c.metrics.Load().addStageStaged(uint64(written))
	return written, true, nil
}

type stageWriteStep struct {
	take        int
	chunkPath   string
	oldOffset   uint64
	oldLength   uint64
	deltaBytes  uint64
	activeChunk *stageItem
	st          *stageInode
}

func (c *StageCoordinator) prepareWriteStep(
	ctx context.Context,
	cancel <-chan struct{},
	fh, ino, nodeID, curOff uint64,
	rem []byte,
) (step stageWriteStep, earlyDone bool, earlyErr error) {
	var bpStart time.Time
	var bpSpan trace.Span = noopSpanInstance
	bpWaiting := false
	finishBP := func() {
		if bpWaiting {
			bpSpan.End()
			c.metrics.Load().observeStageBackpressureWait(time.Since(bpStart))
			bpWaiting = false
		}
	}

	var st *stageInode
	var take int
	var deltaBytes uint64
	var needSealFirst bool
	c.mu.Lock()
	for {
		if c.aborted.Load() {
			err := c.abortErr
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, err
		}
		if c.closed {
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, syscall.EIO
		}
		st = c.getOrCreateInodeLocked(ino)
		if st.discarding {
			c.stateCond.Wait()
			continue
		}
		st.nodeID = nodeID

		for st.activeWriteBusy {
			c.stateCond.Wait()
		}
		if c.aborted.Load() {
			err := c.abortErr
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, err
		}
		if st.discarding || c.inodes[ino] != st {
			continue
		}
		if st.hasUnseenErrLocked(c, fh) {
			unseenErr := st.consumeErrLocked(c, fh)
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, unseenErr
		}

		take, deltaBytes, needSealFirst = planWriteStepLocked(st, curOff, rem)
		if needSealFirst {
			c.sealActiveLocked(st)
			continue
		}
		if deltaBytes > c.stageSize {
			c.pruneIdleInodeLocked(st)
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, syscall.ENOSPC
		}
		if c.usedBytes+deltaBytes <= c.stageSize {
			finishBP()
			break
		}

		draining := c.drainingBytesLocked()
		var nonDraining uint64
		if c.usedBytes > draining {
			nonDraining = c.usedBytes - draining
		}
		if nonDraining+deltaBytes > c.stageSize {
			if c.sealAllIdleActiveLocked() {
				continue
			}
		}

		if cancel != nil {
			select {
			case <-cancel:
				c.pruneIdleInodeLocked(st)
				finishBP()
				c.mu.Unlock()
				return stageWriteStep{}, true, rpc.ErrKernelInterrupted
			default:
			}
		}

		if !bpWaiting {
			bpWaiting = true
			bpStart = time.Now()
			if tracer, ok := c.activeTracer(); ok {
				parent := ctx
				if parent == nil {
					parent = context.Background()
				}
				_, bpSpan = tracer.Start(parent, "stage.BackpressureWait",
					trace.WithAttributes(
						attribute.Int64("stage.ino", int64(ino)),
						attribute.Int64("stage.used_bytes", int64(c.usedBytes)),
						attribute.Int64("stage.capacity_bytes", int64(c.stageSize)),
					),
				)
			}
		}

		wakeCh := c.bpWakeCh
		if wakeCh == nil {
			wakeCh = make(chan struct{})
			c.bpWakeCh = wakeCh
		}
		c.mu.Unlock()
		select {
		case <-wakeCh:
		case <-cancel:
			c.mu.Lock()
			c.pruneIdleInodeLocked(st)
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, rpc.ErrKernelInterrupted
		}
		c.mu.Lock()
	}

	c.usedBytes += deltaBytes
	if st.active == nil {
		seq := c.nextSeq.Add(1)
		st.active = &stageItem{
			seq:        seq,
			state:      stageStateActive,
			ino:        ino,
			nodeID:     nodeID,
			offset:     curOff,
			path:       filepath.Join(c.instanceDir, fmt.Sprintf("chunk-%d.stage", seq)),
			gen:        st.streamGen,
			inodeState: st,
		}
		c.pendingChunks.Add(1)
		c.totalActiveItems.Add(1)
	}
	activeChunk := st.active
	oldOffset := activeChunk.offset
	oldLength := activeChunk.length
	chunkPath := activeChunk.path
	st.activeWriteBusy = true
	c.mu.Unlock()

	return stageWriteStep{
		take:        take,
		chunkPath:   chunkPath,
		oldOffset:   oldOffset,
		oldLength:   oldLength,
		deltaBytes:  deltaBytes,
		activeChunk: activeChunk,
		st:          st,
	}, false, nil
}

func planWriteStepLocked(st *stageInode, curOff uint64, rem []byte) (take int, deltaBytes uint64, needSealFirst bool) {
	act := st.active
	if act == nil {
		return len(rem), uint64(len(rem)), false
	}
	stageEnd := act.offset + act.length
	if curOff == stageEnd && act.length < StageChunkSize {
		part := min(len(rem), int(StageChunkSize-act.length))
		return part, uint64(part), false
	}
	return 0, 0, true
}

func writeChunkFilePiece(path string, curOff, oldOffset uint64, piece []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	relOff := int64(curOff - oldOffset)
	return writeAtFull(f, piece, relOff)
}

func (c *StageCoordinator) rollbackWriteStepLocked(st *stageInode, activeChunk *stageItem, oldLength, deltaBytes uint64) {
	if c.usedBytes >= deltaBytes {
		c.usedBytes -= deltaBytes
	} else {
		c.usedBytes = 0
	}
	if oldLength == 0 && st.active == activeChunk {
		st.active = nil
		activeChunk.state = stageStateDone
		c.pendingChunks.Add(-1)
		c.totalActiveItems.Add(-1)
		_ = os.Remove(activeChunk.path)
	}
	st.activeWriteBusy = false
	if !st.hasPendingLocked() {
		st.lastWriteUnixNano = 0
		c.pruneIdleInodeLocked(st)
	}
	c.stateCond.Broadcast()
	c.wakeBackpressureLocked()
}

func (c *StageCoordinator) abortActiveWriteStepLocked(st *stageInode, activeChunk *stageItem, oldLength, deltaBytes uint64) string {
	totalBytes := oldLength + deltaBytes
	if c.usedBytes >= totalBytes {
		c.usedBytes -= totalBytes
	} else {
		c.usedBytes = 0
	}
	var pathToRemove string
	if st.active == activeChunk {
		st.active = nil
		activeChunk.state = stageStateDone
		activeChunk.discarded.Store(true)
		c.pendingChunks.Add(-1)
		c.totalActiveItems.Add(-1)
		if activeChunk.readers == 0 && !activeChunk.unlinked && activeChunk.path != "" {
			activeChunk.unlinked = true
			pathToRemove = activeChunk.path
		}
	}
	st.activeWriteBusy = false
	if !st.hasPendingLocked() {
		st.lastWriteUnixNano = 0
		c.pruneIdleInodeLocked(st)
	}
	c.stateCond.Broadcast()
	c.wakeBackpressureLocked()
	return pathToRemove
}

type readChunkSnapshot struct {
	item   *stageItem
	offset uint64
	length uint64
	path   string
}

type byteRange struct {
	start uint64
	end   uint64
}

func (c *StageCoordinator) Read(
	ctx context.Context,
	ino, offset uint64,
	dest []byte,
	baseSize uint64,
	hasBaseSize bool,
	mutatedFn func() bool,
	serverRead StageChunkReader,
) (n int, handled bool, err error) {
	if c == nil || len(dest) == 0 || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return 0, false, nil
	}

	c.mu.Lock()
	if c.aborted.Load() {
		c.mu.Unlock()
		return 0, false, nil
	}
	st := c.inodes[ino]
	if st == nil || st.discarding {
		c.mu.Unlock()
		return 0, false, nil
	}
	for st.activeWriteBusy {
		c.stateCond.Wait()
	}
	if c.aborted.Load() || st.discarding || !st.hasPendingLocked() {
		c.mu.Unlock()
		return 0, false, nil
	}

	var snapshots []readChunkSnapshot
	var dirtyEnd uint64
	collect := func(it *stageItem) {
		if it == nil || it.length == 0 || it.discarded.Load() || it.uploaded {
			return
		}
		it.readers++
		snapshots = append(snapshots, readChunkSnapshot{
			item:   it,
			offset: it.offset,
			length: it.length,
			path:   it.path,
		})
		if chunkEnd := it.offset + it.length; chunkEnd > dirtyEnd {
			dirtyEnd = chunkEnd
		}
	}
	for _, it := range st.pending {
		collect(it)
	}
	collect(st.active)
	c.mu.Unlock()

	if len(snapshots) == 0 {
		return 0, false, nil
	}

	defer func() {
		c.releaseReadSnapshots(snapshots)
	}()

	if hasBaseSize && mutatedFn != nil && mutatedFn() {
		hasBaseSize = false
		baseSize = 0
	}

	logicalSize := dirtyEnd
	if hasBaseSize && baseSize > logicalSize {
		logicalSize = baseSize
	}

	reqStart := offset
	reqEnd := offset + uint64(len(dest))

	if hasBaseSize && baseSize == 0 {
		if reqStart >= dirtyEnd {
			return 0, true, nil
		}
		readLen := min(uint64(len(dest)), dirtyEnd-reqStart)
		clear(dest[:readLen])
		if overlayErr := overlaySnapshotChunks(dest[:readLen], reqStart, snapshots); overlayErr != nil {
			return 0, true, overlayErr
		}
		return int(readLen), true, nil
	}

	targetEnd := reqEnd
	if hasBaseSize {
		if reqStart >= logicalSize {
			return 0, true, nil
		}
		targetEnd = min(reqEnd, logicalSize)
	}

	if isRangeCoveredLocally(snapshots, reqStart, targetEnd) {
		readLen := targetEnd - reqStart
		if overlayErr := overlaySnapshotChunks(dest[:readLen], reqStart, snapshots); overlayErr != nil {
			return 0, true, overlayErr
		}
		return int(readLen), true, nil
	}

	firstUncovered, lastUncovered := uncoveredBounds(snapshots, reqStart, reqEnd)
	clear(dest)
	var serverGot uint64
	if lastUncovered > firstUncovered && serverRead != nil {
		var readErr error
		serverGot, readErr = readUncoveredFromServer(ctx, serverRead, firstUncovered, dest[firstUncovered-reqStart:lastUncovered-reqStart])
		if readErr != nil {
			return 0, true, readErr
		}
	}

	actualEnd := logicalSize
	if serverGot > 0 && firstUncovered+serverGot > actualEnd {
		actualEnd = firstUncovered + serverGot
	}
	if actualEnd <= reqStart {
		return 0, true, nil
	}
	readLen := min(uint64(len(dest)), actualEnd-reqStart)
	if overlayErr := overlaySnapshotChunks(dest[:readLen], reqStart, snapshots); overlayErr != nil {
		return 0, true, overlayErr
	}
	return int(readLen), true, nil
}

func (c *StageCoordinator) releaseReadSnapshots(snapshots []readChunkSnapshot) {
	var toRemove []string

	c.mu.Lock()
	for _, snap := range snapshots {
		it := snap.item
		it.readers--
		if it.readers == 0 && (it.uploaded || it.discarded.Load()) && !it.unlinked && it.path != "" {
			it.unlinked = true
			toRemove = append(toRemove, it.path)
		}
	}
	c.stateCond.Broadcast()
	c.mu.Unlock()

	for _, p := range toRemove {
		_ = os.Remove(p)
	}
}

func overlaySnapshotChunks(dest []byte, reqStart uint64, snapshots []readChunkSnapshot) error {
	reqEnd := reqStart + uint64(len(dest))
	for _, snap := range snapshots {
		chunkEnd := snap.offset + snap.length
		if chunkEnd <= reqStart || snap.offset >= reqEnd {
			continue
		}
		ovStart := max(reqStart, snap.offset)
		ovEnd := min(reqEnd, chunkEnd)
		if ovEnd <= ovStart {
			continue
		}
		f, err := os.OpenFile(snap.path, os.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		_, readErr := readAtFull(f, dest[ovStart-reqStart:ovEnd-reqStart], int64(ovStart-snap.offset))
		_ = f.Close()
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func sortedIntervalsInRange(snapshots []readChunkSnapshot, start, end uint64) []byteRange {
	if end <= start || len(snapshots) == 0 {
		return nil
	}
	ivs := make([]byteRange, 0, len(snapshots))
	for _, snap := range snapshots {
		cEnd := snap.offset + snap.length
		if cEnd <= start || snap.offset >= end {
			continue
		}
		ivs = append(ivs, byteRange{
			start: max(start, snap.offset),
			end:   min(end, cEnd),
		})
	}
	slices.SortFunc(ivs, func(a, b byteRange) int {
		if a.start < b.start {
			return -1
		}
		if a.start > b.start {
			return 1
		}
		if a.end < b.end {
			return -1
		}
		if a.end > b.end {
			return 1
		}
		return 0
	})
	return ivs
}

func isRangeCoveredLocally(snapshots []readChunkSnapshot, start, end uint64) bool {
	if end <= start {
		return true
	}
	ivs := sortedIntervalsInRange(snapshots, start, end)
	coveredTo := start
	for _, iv := range ivs {
		if iv.start > coveredTo {
			return false
		}
		if iv.end > coveredTo {
			coveredTo = iv.end
			if coveredTo >= end {
				return true
			}
		}
	}
	return coveredTo >= end
}

func uncoveredBounds(snapshots []readChunkSnapshot, start, end uint64) (firstUncovered, lastUncovered uint64) {
	ivs := sortedIntervalsInRange(snapshots, start, end)
	firstUncovered = start
	for _, iv := range ivs {
		if iv.start > firstUncovered {
			break
		}
		if iv.end > firstUncovered {
			firstUncovered = iv.end
		}
	}
	if firstUncovered >= end {
		return end, end
	}
	lastUncovered = end
	for _, iv := range slices.Backward(ivs) {
		if iv.end < lastUncovered {
			break
		}
		if iv.start < lastUncovered {
			lastUncovered = iv.start
		}
	}
	if lastUncovered < firstUncovered {
		return firstUncovered, firstUncovered
	}
	return firstUncovered, lastUncovered
}

func readUncoveredFromServer(ctx context.Context, reader StageChunkReader, offset uint64, dest []byte) (uint64, error) {
	var total uint64
	for total < uint64(len(dest)) {
		reqLen := uint32(min(uint64(len(dest))-total, MaxRPCChunkSize))
		payload, err := reader(ctx, offset+total, reqLen)
		if err != nil {
			if total > 0 {
				return total, nil
			}
			return 0, err
		}
		n := min(int(reqLen), len(payload.Data))
		if n > 0 {
			copy(dest[total:total+uint64(n)], payload.Data[:n])
			total += uint64(n)
		}
		payload.Release()
		if n < int(reqLen) {
			break
		}
	}
	return total, nil
}

func (c *StageCoordinator) FlushHandle(fh, ino uint64) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.aborted.Load() {
		err := c.abortErr
		if st := c.inodes[ino]; st != nil {
			_ = st.consumeErrLocked(c, fh)
		}
		c.mu.Unlock()
		return err
	}
	st := c.inodes[ino]
	if st == nil {
		c.mu.Unlock()
		return nil
	}
	for st.activeWriteBusy {
		c.stateCond.Wait()
	}
	if c.aborted.Load() {
		err := c.abortErr
		_ = st.consumeErrLocked(c, fh)
		c.mu.Unlock()
		return err
	}
	c.sealActiveLocked(st)
	err := st.consumeErrLocked(c, fh)
	c.mu.Unlock()
	return err
}

func (c *StageCoordinator) SealInode(ino uint64) {
	if c == nil || c.aborted.Load() {
		return
	}
	c.mu.Lock()
	st := c.inodes[ino]
	if st != nil && !st.discarding {
		for st.activeWriteBusy {
			c.stateCond.Wait()
		}
		if !c.aborted.Load() && !st.discarding {
			c.sealActiveLocked(st)
		}
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) watchContextBroadcast(ctx context.Context) func() bool {
	if ctx == nil || ctx.Done() == nil {
		return nil
	}
	return context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.stateCond.Broadcast()
		c.mu.Unlock()
	})
}

func (c *StageCoordinator) barrierInodeLocked(ctx context.Context, st *stageInode) error {
	if c.aborted.Load() {
		return nil
	}
	if !st.activeWriteBusy && !st.hasPendingLocked() {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if !st.activeWriteBusy && !st.discarding {
		c.sealActiveLocked(st)
		if len(st.pending) == 0 {
			return nil
		}
	}
	stopWatch := c.watchContextBroadcast(ctx)
	if stopWatch != nil {
		defer stopWatch()
	}
	waitStart := time.Now()
	var waitSpan trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		parent := ctx
		if parent == nil {
			parent = context.Background()
		}
		_, waitSpan = tracer.Start(parent, "stage.BarrierWait",
			trace.WithAttributes(
				attribute.Int64("stage.ino", int64(st.ino)),
				attribute.Int("stage.pending_chunks", len(st.pending)),
			),
		)
	}
	defer func() {
		waitSpan.End()
		c.metrics.Load().observeStageBarrierWait(time.Since(waitStart))
	}()
	for !c.aborted.Load() {
		if ctx != nil && ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if st.activeWriteBusy {
			c.stateCond.Wait()
			continue
		}
		if !st.discarding {
			c.sealActiveLocked(st)
		}
		if len(st.pending) == 0 {
			return nil
		}
		c.stateCond.Wait()
	}
	return nil
}

func (c *StageCoordinator) BarrierInode(ctx context.Context, ino uint64) error {
	if c == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.inodes[ino]
	if st == nil {
		return nil
	}
	return c.barrierInodeLocked(ctx, st)
}

func (c *StageCoordinator) FlushInodeForHandle(ctx context.Context, fh, ino uint64) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.aborted.Load() {
		err := c.abortErr
		if st := c.inodes[ino]; st != nil {
			_ = st.consumeErrLocked(c, fh)
		}
		c.mu.Unlock()
		return err
	}
	st := c.inodes[ino]
	if st == nil {
		c.mu.Unlock()
		return nil
	}
	if err := c.barrierInodeLocked(ctx, st); err != nil {
		c.mu.Unlock()
		return err
	}
	err := st.consumeErrLocked(c, fh)
	if c.aborted.Load() && err == nil {
		err = c.abortErr
	}
	c.mu.Unlock()
	return err
}

func (c *StageCoordinator) FlushAndResetInodeForHandle(ctx context.Context, fh, ino uint64) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.aborted.Load() {
		err := c.abortErr
		if st := c.inodes[ino]; st != nil {
			_ = st.consumeErrLocked(c, fh)
			st.resetStreamLocked()
		}
		c.mu.Unlock()
		return err
	}
	st := c.inodes[ino]
	if st == nil {
		c.mu.Unlock()
		return nil
	}
	if err := c.barrierInodeLocked(ctx, st); err != nil {
		c.mu.Unlock()
		return err
	}
	err := st.consumeErrLocked(c, fh)
	if c.aborted.Load() && err == nil {
		err = c.abortErr
	}
	st.resetStreamLocked()
	c.mu.Unlock()
	return err
}

func (c *StageCoordinator) ResetInodeStream(ctx context.Context, ino uint64) error {
	if c == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.inodes[ino]
	if st != nil {
		if err := c.barrierInodeLocked(ctx, st); err != nil {
			return err
		}
		st.resetStreamLocked()
		c.pruneIdleInodeLocked(st)
	}
	return nil
}

func (c *StageCoordinator) BarrierAll(ctx context.Context) error {
	if c == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, st := range c.inodes {
		if c.aborted.Load() {
			break
		}
		if st.hasPendingLocked() || st.activeWriteBusy {
			if err := c.barrierInodeLocked(ctx, st); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *StageCoordinator) DiscardInode(ino uint64) {
	if c == nil {
		return
	}
	var pathsToRemove []string

	c.mu.Lock()
	st := c.inodes[ino]
	if st == nil {
		c.mu.Unlock()
		return
	}
	for st.activeWriteBusy || st.discarding {
		if st.discarding {
			for c.inodes[ino] == st && !c.aborted.Load() {
				c.stateCond.Wait()
			}
			c.mu.Unlock()
			return
		}
		c.stateCond.Wait()
		if c.inodes[ino] != st {
			c.mu.Unlock()
			return
		}
	}
	st.discarding = true

	close(st.discardCh)
	st.discardCh = make(chan struct{})

	if st.active != nil {
		it := st.active
		st.active = nil
		it.state = stageStateDone
		it.discarded.Store(true)
		c.pendingChunks.Add(-1)
		c.totalActiveItems.Add(-1)
		if c.usedBytes >= it.length {
			c.usedBytes -= it.length
		} else {
			c.usedBytes = 0
		}
		if it.readers == 0 && !it.unlinked && it.path != "" {
			it.unlinked = true
			pathsToRemove = append(pathsToRemove, it.path)
		}
	}

	var freedBytes uint64
	var droppedChunks int64
	keepQueue := 0
	for _, qIt := range c.queue {
		if qIt.ino == ino && qIt.state == stageStateQueued {
			qIt.state = stageStateDone
			qIt.discarded.Store(true)
			droppedChunks++
			freedBytes += qIt.length
			if qIt.readers == 0 && !qIt.unlinked && qIt.path != "" {
				qIt.unlinked = true
				pathsToRemove = append(pathsToRemove, qIt.path)
			}
			continue
		}
		c.queue[keepQueue] = qIt
		keepQueue++
	}
	clear(c.queue[keepQueue:])
	c.queue = c.queue[:keepQueue]

	keepPending := 0
	for _, pIt := range st.pending {
		if pIt.state == stageStateInflight {
			pIt.discarded.Store(true)
			st.pending[keepPending] = pIt
			keepPending++
		}
	}
	clear(st.pending[keepPending:])
	st.pending = st.pending[:keepPending]

	if droppedChunks > 0 {
		c.pendingChunks.Add(-droppedChunks)
		c.totalActiveItems.Add(-droppedChunks)
	}
	if freedBytes > 0 {
		if c.usedBytes >= freedBytes {
			c.usedBytes -= freedBytes
		} else {
			c.usedBytes = 0
		}
	}
	c.wakeBackpressureLocked()

	for st.hasInflightLocked() && !c.aborted.Load() {
		c.stateCond.Wait()
	}
	st.lastWriteUnixNano = 0
	st.discarding = false
	st.resetStreamLocked()
	if c.inodes[ino] == st {
		delete(c.inodes, ino)
	}
	c.stateCond.Broadcast()
	c.mu.Unlock()

	for _, p := range pathsToRemove {
		_ = os.Remove(p)
	}
}

func (c *StageCoordinator) AbortAll(err error) {
	if c == nil {
		return
	}
	if err == nil {
		err = syscall.EBUSY
	}

	var pathsToRemove []string

	c.mu.Lock()
	if c.aborted.Load() {
		c.mu.Unlock()
		return
	}
	c.aborted.Store(true)
	c.abortErr = err
	seq := c.errSeqGen.Add(1)
	c.abortErrSeq = seq

	var freedBytes uint64
	var droppedChunks int64
	for i, qIt := range c.queue {
		c.queue[i] = nil
		if qIt != nil && qIt.state == stageStateQueued {
			qIt.state = stageStateDone
			qIt.discarded.Store(true)
			droppedChunks++
			freedBytes += qIt.length
			if qIt.readers == 0 && !qIt.unlinked && qIt.path != "" {
				qIt.unlinked = true
				pathsToRemove = append(pathsToRemove, qIt.path)
			}
		}
	}
	c.queue = c.queue[:0]

	for _, st := range c.inodes {
		if st == nil {
			continue
		}
		close(st.discardCh)
		st.discardCh = make(chan struct{})

		st.lastErr = err
		st.errSeq = seq
		st.errGen = st.streamGen
		st.resetStreamLocked()

		if st.active != nil {
			it := st.active
			it.discarded.Store(true)
			if !st.activeWriteBusy {
				st.active = nil
				it.state = stageStateDone
				droppedChunks++
				freedBytes += it.length
				if it.readers == 0 && !it.unlinked && it.path != "" {
					it.unlinked = true
					pathsToRemove = append(pathsToRemove, it.path)
				}
			}
		}

		keepPending := 0
		for _, pIt := range st.pending {
			if pIt.state == stageStateInflight {
				pIt.discarded.Store(true)
				st.pending[keepPending] = pIt
				keepPending++
			}
		}
		clear(st.pending[keepPending:])
		st.pending = st.pending[:keepPending]

		st.lastWriteUnixNano = 0
	}

	if droppedChunks > 0 {
		c.pendingChunks.Add(-droppedChunks)
		c.totalActiveItems.Add(-droppedChunks)
	}
	if freedBytes > 0 {
		if c.usedBytes >= freedBytes {
			c.usedBytes -= freedBytes
		} else {
			c.usedBytes = 0
		}
	}

	c.wakeBackpressureLocked()
	c.stateCond.Broadcast()
	c.workerCond.Broadcast()
	c.mu.Unlock()

	c.stopRetryOnce.Do(func() { close(c.stopRetryCh) })
	if c.abortCancel != nil {
		c.abortCancel()
	}

	for _, p := range pathsToRemove {
		_ = os.Remove(p)
	}
}

func (c *StageCoordinator) countUnuploadedChunksLocked() int64 {
	var dropped int64
	for _, st := range c.inodes {
		if st == nil {
			continue
		}
		if st.active != nil && !st.active.uploaded {
			dropped++
		}
		for _, it := range st.pending {
			if it != nil && !it.uploaded {
				dropped++
			}
		}
	}
	return dropped
}

func (c *StageCoordinator) Clear(ctx context.Context, stallTimeout time.Duration) {
	if c == nil {
		return
	}
	c.clearMu.Lock()
	defer c.clearMu.Unlock()

	c.mu.Lock()
	if c.workersDone {
		c.mu.Unlock()
		return
	}
	c.closed = true
	aborted := c.aborted.Load()
	c.wakeBackpressureLocked()
	c.mu.Unlock()

	parentCtx := ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	if !aborted {
		select {
		case <-c.progressCh:
		default:
		}

		drainCtx, drainCancel := context.WithCancel(parentCtx)
		drainAbortedCh := make(chan bool, 1)
		go func() {
			drainAbortedCh <- c.BarrierAll(drainCtx) != nil
		}()

		var stallTimer *time.Timer
		var stallC <-chan time.Time
		if stallTimeout > 0 {
			stallTimer = time.NewTimer(stallTimeout)
			defer stallTimer.Stop()
			stallC = stallTimer.C
		}

		var abortReason string
		drainFinished := false
		draining := true
		for draining {
			select {
			case drainAborted := <-drainAbortedCh:
				drainFinished = true
				draining = false
				if drainAborted {
					abortReason = "stage_timeout"
				}
			case <-c.progressCh:
				if stallTimer != nil {
					if !stallTimer.Stop() {
						select {
						case <-stallTimer.C:
						default:
						}
					}
					stallTimer.Reset(stallTimeout)
				}
			case <-stallC:
				abortReason = "stall_timeout"
				draining = false
			case <-drainCtx.Done():
				abortReason = "stage_timeout"
				draining = false
			}
		}

		if abortReason != "" {
			drainCancel()
			c.stopRetryOnce.Do(func() { close(c.stopRetryCh) })

			c.mu.Lock()
			droppedChunks := c.countUnuploadedChunksLocked()
			c.stopWorkers = true
			for i, qIt := range c.queue {
				c.queue[i] = nil
				if qIt != nil && qIt.state == stageStateQueued {
					qIt.state = stageStateDone
					qIt.discarded.Store(true)
				}
			}
			c.queue = c.queue[:0]
			for _, st := range c.inodes {
				if st == nil {
					continue
				}
				if st.active != nil {
					st.active.discarded.Store(true)
				}
				for _, pIt := range st.pending {
					if pIt != nil {
						pIt.discarded.Store(true)
					}
				}
			}
			if c.abortCancel != nil {
				c.abortCancel()
			}
			c.workerCond.Broadcast()
			c.stateCond.Broadcast()
			c.mu.Unlock()

			if !drainFinished {
				<-drainAbortedCh
			}
			if droppedChunks > 0 {
				c.activeLogger().ErrorContext(parentCtx, "stage_unmount_drain_aborted",
					"reason", abortReason,
					"dropped_chunks", droppedChunks,
				)
			}
		} else {
			drainCancel()
		}
	}

	c.mu.Lock()
	c.stopWorkers = true
	c.workerCond.Broadcast()
	c.mu.Unlock()

	c.workerWG.Wait()
	if c.abortCancel != nil {
		c.abortCancel()
	}

	var leftoverPaths []string
	c.mu.Lock()
	c.workersDone = true
	for _, st := range c.inodes {
		if st.active != nil && st.active.path != "" && !st.active.unlinked {
			st.active.unlinked = true
			leftoverPaths = append(leftoverPaths, st.active.path)
		}
		for _, it := range st.pending {
			if it.path != "" && !it.unlinked {
				it.unlinked = true
				leftoverPaths = append(leftoverPaths, it.path)
			}
		}
	}
	clear(c.inodes)
	c.queue = nil
	c.usedBytes = 0
	c.inflightCount = 0
	c.inflightBytes = 0
	c.pendingChunks.Store(0)
	c.totalActiveItems.Store(0)
	c.mu.Unlock()

	for _, p := range leftoverPaths {
		_ = os.Remove(p)
	}

	c.seqMu.Lock()
	clear(c.handleSeq)
	c.seqMu.Unlock()
}

func (c *StageCoordinator) canRunItemLocked(it *stageItem) bool {
	st := it.inodeState
	if st == nil {
		return true
	}
	alignFull := it.isAlignedFullChunk()
	for _, earlier := range st.pending {
		if earlier.seq >= it.seq {
			break
		}
		if earlier.state == stageStateDone {
			continue
		}
		if !alignFull {
			return false
		}
		if !earlier.isAlignedFullChunk() || it.blocksOverlap(earlier) {
			return false
		}
	}
	return true
}

func (c *StageCoordinator) nextRunnableItemLocked() *stageItem {
	if c.aborted.Load() {
		return nil
	}
	for i, it := range c.queue {
		if !c.canRunItemLocked(it) {
			continue
		}
		rem := copy(c.queue[i:], c.queue[i+1:])
		c.queue[i+rem] = nil
		c.queue = c.queue[:i+rem]
		it.state = stageStateInflight
		return it
	}
	return nil
}

func (c *StageCoordinator) uploadWorker() {
	defer c.workerWG.Done()
	for {
		c.mu.Lock()
		var item *stageItem
		for {
			item = c.nextRunnableItemLocked()
			if item != nil {
				break
			}
			if c.stopWorkers && (len(c.queue) == 0 || c.aborted.Load()) {
				c.mu.Unlock()
				return
			}
			c.workerCond.Wait()
		}
		discardCh := item.inodeState.discardCh
		c.inflightCount++
		c.inflightBytes += item.length
		c.mu.Unlock()

		c.metrics.Load().addStageInflight(1)
		c.processUploadItem(item, discardCh)
		c.metrics.Load().addStageInflight(-1)
	}
}

func (c *StageCoordinator) processUploadItem(item *stageItem, initialDiscardCh chan struct{}) {
	uploadBaseCtx := c.abortCtx
	if uploadBaseCtx == nil {
		uploadBaseCtx = context.Background()
	}
	var uploadSpan trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		opts := make([]trace.SpanStartOption, 0, 3)
		opts = append(opts,
			trace.WithNewRoot(),
			trace.WithAttributes(
				attribute.Int64("stage.ino", int64(item.ino)),
				attribute.Int64("stage.offset", int64(item.offset)),
				attribute.Int("stage.length", int(item.length)),
			),
		)
		if len(item.links) > 0 {
			opts = append(opts, trace.WithLinks(item.links...))
		}
		uploadBaseCtx, uploadSpan = tracer.Start(uploadBaseCtx, "stage.UploadChunk", opts...)
	}

	var (
		respInode     *storagev1.Inode
		writtenBytes  uint32
		finalErr      error
		discardedMid  bool
		notFoundAbort bool
	)

	backoff := c.backoffInit
	if backoff <= 0 {
		backoff = stageBackoffInitial
	}
	maxBackoff := max(c.backoffLimit, backoff)

	discardCh := initialDiscardCh
	for {
		if c.aborted.Load() || uploadBaseCtx.Err() != nil {
			discardedMid = true
			break
		}
		buf := acquireStageBuf()
		dataLen := int(item.length)
		readErr := readChunkBytesForUpload(item.path, buf[:dataLen])
		if readErr != nil {
			releaseStageBuf(buf)
			finalErr = readErr
			break
		}

		rpcCtx, cancel := context.WithTimeout(uploadBaseCtx, DefaultRPCTimeout)
		writtenBytes, respInode, finalErr = c.writer(rpcCtx, item.ino, item.offset, buf[:dataLen:dataLen])
		cancel()
		releaseStageBuf(buf)

		if finalErr == nil && writtenBytes < uint32(dataLen) {
			finalErr = syscall.EIO
		}

		if finalErr == nil {
			break
		}
		if c.aborted.Load() || uploadBaseCtx.Err() != nil {
			discardedMid = true
			break
		}

		action := classifyStageError(finalErr)
		if action == stageErrNotFound {
			notFoundAbort = true
			break
		}
		if action != stageErrTransient {
			break
		}

		c.mu.Lock()
		st := item.inodeState
		if st != nil {
			discardCh = st.discardCh
		}
		aborted := c.aborted.Load() || item.discarded.Load() || (st == nil)
		c.mu.Unlock()
		if aborted {
			discardedMid = true
			break
		}

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-discardCh:
			timer.Stop()
			discardedMid = true
		case <-uploadBaseCtx.Done():
			timer.Stop()
			discardedMid = true
		case <-c.stopRetryCh:
			timer.Stop()
			discardedMid = true
		}
		if discardedMid {
			break
		}
		backoff = min(backoff*2, maxBackoff)
	}

	if finalErr != nil && !discardedMid && !notFoundAbort {
		uploadSpan.RecordError(finalErr)
		uploadSpan.SetStatus(codes.Error, finalErr.Error())
		c.metrics.Load().incStageUploadRPC(true)
		c.activeLogger().ErrorContext(uploadBaseCtx, "stage_upload_failed",
			"ino", item.ino,
			"offset", item.offset,
			"length", item.length,
			"error", finalErr,
		)
	} else if !discardedMid {
		c.metrics.Load().incStageUploadRPC(false)
		uploadSpan.SetAttributes(attribute.Int("stage.written_bytes", int(writtenBytes)))
	}
	uploadSpan.End()

	c.finishUploadItem(item, respInode, finalErr, discardedMid, notFoundAbort)
}

func readChunkBytesForUpload(path string, dst []byte) error {
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	n, readErr := readAtFull(f, dst, 0)
	if readErr != nil {
		return readErr
	}
	if n < len(dst) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (c *StageCoordinator) finishUploadItem(
	item *stageItem,
	respInode *storagev1.Inode,
	finalErr error,
	discardedMid bool,
	notFoundAbort bool,
) {
	if finalErr == nil && !discardedMid && !c.aborted.Load() && !item.discarded.Load() && c.hooks.OnCommit != nil {
		c.hooks.OnCommit(item.nodeID, item.ino, respInode)
	}

	var pathToRemove string

	c.mu.Lock()
	st := item.inodeState
	item.state = stageStateDone
	item.uploaded = true
	if c.inflightCount > 0 {
		c.inflightCount--
	}
	if c.inflightBytes >= item.length {
		c.inflightBytes -= item.length
	} else {
		c.inflightBytes = 0
	}

	if item.readers == 0 && !item.unlinked && item.path != "" {
		item.unlinked = true
		pathToRemove = item.path
	}

	c.pendingChunks.Add(-1)
	c.totalActiveItems.Add(-1)
	if c.usedBytes >= item.length {
		c.usedBytes -= item.length
	} else {
		c.usedBytes = 0
	}

	if st != nil {
		for i, p := range st.pending {
			if p == item {
				rem := copy(st.pending[i:], st.pending[i+1:])
				st.pending[i+rem] = nil
				st.pending = st.pending[:i+rem]
				break
			}
		}

		if !c.aborted.Load() {
			if finalErr != nil && !discardedMid {
				if !notFoundAbort {
					if st.errGen != item.gen {
						st.errGen = item.gen
						st.lastErr = finalErr
						st.errSeq = c.errSeqGen.Add(1)
					}
					if st.streamGen == item.gen {
						st.resetStreamLocked()
					}
				}
			} else if finalErr == nil && !discardedMid && !item.discarded.Load() {
				select {
				case c.progressCh <- struct{}{}:
				default:
				}
			}
		}

		if !st.hasPendingLocked() {
			st.lastWriteUnixNano = 0
			c.pruneIdleInodeLocked(st)
		}
	}

	c.wakeBackpressureLocked()
	c.workerCond.Broadcast()
	c.stateCond.Broadcast()
	c.mu.Unlock()

	if pathToRemove != "" {
		_ = os.Remove(pathToRemove)
	}
}

type stageErrAction uint8

const (
	stageErrTerminal stageErrAction = iota
	stageErrTransient
	stageErrNotFound
)

func classifyStageError(err error) stageErrAction {
	if err == nil {
		return stageErrTerminal
	}
	if info := serverx.ExtractErrorInfo(err); info != nil && info.Reason != "" {
		if info.Reason == "ENOENT" {
			return stageErrNotFound
		}
		return stageErrTerminal
	}
	if errors.Is(err, syscall.ENOENT) {
		return stageErrNotFound
	}
	if connectErr, ok := errors.AsType[*connect.Error](err); ok {
		switch connectErr.Code() {
		case connect.CodeNotFound:
			return stageErrNotFound
		case connect.CodeUnavailable, connect.CodeDeadlineExceeded:
			return stageErrTransient
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || isStageTransportError(err) {
		return stageErrTransient
	}
	return stageErrTerminal
}

func isStageTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTUNREACH) {
		return true
	}
	if _, ok := errors.AsType[*net.OpError](err); ok {
		return true
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	return false
}
