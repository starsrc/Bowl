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
	"google.golang.org/protobuf/types/known/timestamppb"

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

	StageWorkers = rpc.HTTP2DataConnectionShards

	stageExtentScanPageSize = 4096
	stageBackoffInitial     = 100 * time.Millisecond
	stageBackoffMax         = 2 * time.Second
	maxStageSpanLinks       = 32
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
	instName := fmt.Sprintf("inst-%d-%s", os.Getpid(), hex.EncodeToString(randBytes[:]))
	instDir := filepath.Join(cleanDir, instName)
	if err := os.Mkdir(instDir, 0o700); err != nil {
		return nil, fmt.Errorf("create stage instance dir %q: %w", instDir, err)
	}

	lockPath := filepath.Join(instDir, ".lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		_ = os.RemoveAll(instDir)
		return nil, fmt.Errorf("open stage lock file %q: %w", lockPath, err)
	}
	if flockErr := flockExNB(lockFile); flockErr != nil {
		_ = lockFile.Close()
		_ = os.RemoveAll(instDir)
		return nil, fmt.Errorf("lock stage instance %q: %w", lockPath, flockErr)
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

		if pid, ok := parseInstDirPID(name); !ok || !isPIDAlive(pid) {
			if rmErr := os.RemoveAll(fullPath); rmErr == nil {
				cleaned++
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

func parseInstDirPID(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, "inst-")
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

type stageExtent struct {
	offset uint64
	length uint64
}

func createAndUnlinkTempFile(dir string) (*os.File, error) {
	f, err := os.CreateTemp(dir, "tmpfile-*")
	if err != nil {
		return nil, err
	}
	if rmErr := os.Remove(f.Name()); rmErr != nil {
		_ = f.Close()
		return nil, rmErr
	}
	return f, nil
}

func fallocateHostFileFallback(f *os.File, mode uint32, offset, length int64, curSize uint64) error {
	switch mode {
	case 0:
		end := uint64(offset + length)
		if end > curSize {
			return f.Truncate(int64(end))
		}
		return nil
	case fallocFlKeepSize:
		return nil
	case fallocFlPunchHole | fallocFlKeepSize:
		if uint64(offset) >= curSize {
			return nil
		}
		punchEnd := min(uint64(offset+length), curSize)
		return writeZerosAt(f, offset, int64(punchEnd-uint64(offset)))
	default:
		return syscall.EOPNOTSUPP
	}
}

func writeZerosAt(f *os.File, offset, length int64) error {
	if length <= 0 {
		return nil
	}
	var zeroBuf [32 * 1024]byte
	pos := offset
	rem := length
	for rem > 0 {
		n := min(rem, int64(len(zeroBuf)))
		if err := writeAtFull(f, zeroBuf[:n], pos); err != nil {
			return err
		}
		pos += n
		rem -= n
	}
	return nil
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

func appendSplitExtents(extents []stageExtent, start, end uint64) []stageExtent {
	for pos := start; pos < end; {
		n := min(end-pos, StageChunkSize)
		extents = append(extents, stageExtent{offset: pos, length: n})
		pos += n
	}
	return extents
}

func isAllZeroBytes(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func scanNonZeroExtents(f *os.File, startOff, size uint64) ([]stageExtent, error) {
	if startOff >= size {
		return nil, nil
	}
	var extents []stageExtent
	var page [stageExtentScanPageSize]byte
	hasCur := false
	var curStart, curEnd uint64

	flushExtent := func() {
		if hasCur && curEnd > curStart {
			extents = appendSplitExtents(extents, curStart, curEnd)
		}
		hasCur = false
	}

	for pos := startOff; pos < size; {
		want := int(min(uint64(len(page)), size-pos))
		n, err := readAtFull(f, page[:want], int64(pos))
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		if !isAllZeroBytes(page[:n]) {
			switch {
			case !hasCur:
				curStart = pos
				curEnd = pos + uint64(n)
				hasCur = true
			case pos == curEnd && curEnd-curStart+uint64(n) <= StageChunkSize:
				curEnd += uint64(n)
			default:
				flushExtent()
				curStart = pos
				curEnd = pos + uint64(n)
				hasCur = true
			}
		} else if hasCur {
			flushExtent()
		}
		pos += uint64(n)
	}
	flushExtent()
	return extents, nil
}

type StageChunkWriter func(ctx context.Context, ino, offset uint64, data []byte) (uint32, *storagev1.Inode, error)

type StageChunkReader func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error)

type StagedXattr struct {
	Name  string
	Value []byte
}

type StagePromotedMeta struct {
	Size   uint64
	Atime  *timestamppb.Timestamp
	Mtime  *timestamppb.Timestamp
	Mode   uint32
	Gid    uint32
	Flags  uint32
	Xattrs []StagedXattr
}

type StageAttrCommitter func(ctx context.Context, ino uint64, meta StagePromotedMeta) (*storagev1.Inode, error)

type StageHooks struct {
	OnStage  func(nodeID, cacheEpoch, endOffset uint64, ts *timestamppb.Timestamp)
	OnCommit func(nodeID, ino, cacheEpoch uint64, inode *storagev1.Inode, minSize uint64, refreshAttr, preserveMeta, capPresent bool)
	OnError  func(nodeID uint64)
}

type stageItemKind uint8

const (
	stageItemChunkFile stageItemKind = iota
	stageItemPromotedExtent
	stageItemPromotedCommit
)

type stageItemState uint8

const (
	stageStateActive stageItemState = iota
	stageStateQueued
	stageStateInflight
	stageStateDone
)

type promotedTmpState struct {
	file     *os.File
	size     uint64
	atime    *timestamppb.Timestamp
	mtime    *timestamppb.Timestamp
	mode     uint32
	gid      uint32
	flags    uint32
	xattrs   []StagedXattr
	hasCap   bool
	readers  int
	finished bool
	hasErr   bool
}

func (p *promotedTmpState) maybeDetachFileLocked() *os.File {
	if p == nil || !p.finished || p.readers > 0 || p.file == nil {
		return nil
	}
	f := p.file
	p.file = nil
	return f
}

type stageItem struct {
	seq        uint64
	kind       stageItemKind
	state      stageItemState
	ino        uint64
	nodeID     uint64
	cacheEpoch uint64
	offset     uint64
	length     uint64
	path       string
	gen        uint64
	inodeState *stageInode
	promoted   *promotedTmpState
	links      []trace.Link

	readers   int
	uploaded  bool
	discarded bool
	unlinked  bool
}

func (it *stageItem) isAlignedFullChunk() bool {
	return it.kind != stageItemPromotedCommit &&
		it.length == StageChunkSize &&
		it.offset%StageBlockAlign == 0
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
	ino              uint64
	nodeID           uint64
	cacheEpoch       uint64
	active           *stageItem
	pending          []*stageItem
	promoted         *promotedTmpState
	activeWriteBusy  bool
	inflightHooks    int
	createdInSession bool
	hasKnownSize     bool
	logicalSize      uint64
	lastWriteTS      *timestamppb.Timestamp
	lastCommitted    *storagev1.Inode
	streamGen        uint64
	errGen           uint64
	errSeq           uint64
	lastErr          error
	defaultSeenSeq   uint64
	discardCh        chan struct{}
}

func newStageInode(ino uint64) *stageInode {
	return &stageInode{
		ino:       ino,
		streamGen: 1,
		discardCh: make(chan struct{}),
	}
}

func (st *stageInode) hasPendingLocked() bool {
	return st.active != nil || len(st.pending) > 0 || st.promoted != nil || st.inflightHooks > 0
}

func (st *stageInode) hasInflightLocked() bool {
	if st.inflightHooks > 0 {
		return true
	}
	for _, it := range st.pending {
		if it.state == stageStateInflight {
			return true
		}
	}
	return false
}

func (st *stageInode) resetStreamLocked() {
	st.streamGen++
	st.lastCommitted = nil
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
	committer   StageAttrCommitter
	hooks       StageHooks

	nextSeq          atomic.Uint64
	totalActiveItems atomic.Int64
	pendingChunks    atomic.Int64
	inflight         atomic.Int64
	errSeqGen        atomic.Uint64
	aborted          atomic.Bool

	seqMu     sync.Mutex
	handleSeq map[uint64]uint64

	clearMu       sync.Mutex
	mu            sync.Mutex
	inodes        map[uint64]*stageInode
	queue         []*stageItem
	usedBytes     uint64
	abortErr      error
	abortErrSeq   uint64
	abortCtx      context.Context
	abortCancel   context.CancelFunc
	closed        bool
	stopWorkers   bool
	workersDone   bool
	bpWakeCh      chan struct{}
	stateCond     *sync.Cond
	workerCond    *sync.Cond
	stopRetryCh   chan struct{}
	stopRetryOnce sync.Once
	workerWG      sync.WaitGroup
	metrics       atomic.Pointer[Metrics]
	logger        atomic.Pointer[slog.Logger]
	tracerProv    atomic.Pointer[trace.TracerProvider]
	backoffInit   time.Duration
	backoffLimit  time.Duration
}

func NewStageCoordinator(
	instanceDir string,
	stageSize uint64,
	writer StageChunkWriter,
	committer StageAttrCommitter,
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
		committer:    committer,
		hooks:        hooks,
		handleSeq:    make(map[uint64]uint64),
		inodes:       make(map[uint64]*stageInode),
		abortCtx:     abortCtx,
		abortCancel:  abortCancel,
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

func (c *StageCoordinator) sealActiveLocked(st *stageInode) bool {
	if st == nil || st.active == nil || st.activeWriteBusy {
		return false
	}
	it := st.active
	st.active = nil
	if it.length == 0 {
		it.state = stageStateDone
		c.pendingChunks.Add(-1)
		c.totalActiveItems.Add(-1)
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

func (c *StageCoordinator) SeedCreatedInode(ino uint64) {
	if c == nil || ino == 0 {
		return
	}
	c.mu.Lock()
	st := c.getOrCreateInodeLocked(ino)
	if !st.hasPendingLocked() {
		st.createdInSession = true
		st.hasKnownSize = true
		st.logicalSize = 0
		st.lastWriteTS = nil
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) ResetTruncatedInode(ino uint64) {
	if c == nil || ino == 0 {
		return
	}
	c.mu.Lock()
	st := c.getOrCreateInodeLocked(ino)
	st.createdInSession = true
	st.hasKnownSize = true
	st.logicalSize = 0
	st.lastWriteTS = nil
	st.resetStreamLocked()
	c.mu.Unlock()
}

func (c *StageCoordinator) ObserveServerInode(ino uint64, inode *storagev1.Inode, truncatedZero bool) {
	if c == nil || ino == 0 || inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	st := c.getOrCreateInodeLocked(ino)
	if !st.hasPendingLocked() {
		st.hasKnownSize = true
		st.logicalSize = inode.GetAttr().GetSize()
		st.createdInSession = truncatedZero
		if truncatedZero {
			st.lastWriteTS = nil
		}
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) InvalidateKnownSize(ino uint64) {
	if c == nil || ino == 0 {
		return
	}
	c.mu.Lock()
	if st := c.inodes[ino]; st != nil && !st.hasPendingLocked() {
		st.hasKnownSize = false
		st.createdInSession = false
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) RemoveIdleInode(ino uint64) {
	if c == nil || ino == 0 {
		return
	}
	c.mu.Lock()
	if st := c.inodes[ino]; st != nil && !st.hasPendingLocked() && !st.activeWriteBusy && st.errSeq == 0 {
		delete(c.inodes, ino)
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) OverlayAttr(ino uint64, attr *fuse.Attr) {
	if c == nil || attr == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return
	}
	c.mu.Lock()
	st := c.inodes[ino]
	if st == nil || !st.hasPendingLocked() {
		c.mu.Unlock()
		return
	}
	if st.logicalSize > attr.Size {
		attr.Size = st.logicalSize
		minBlocks := (attr.Size + 511) / 512
		if minBlocks > attr.Blocks {
			attr.Blocks = minBlocks
		}
	}
	if st.lastWriteTS != nil {
		sec := uint64(max(int64(0), st.lastWriteTS.GetSeconds()))
		nsec := uint32(max(0, st.lastWriteTS.GetNanos()))
		if sec > attr.Mtime || (sec == attr.Mtime && nsec > attr.Mtimensec) {
			attr.Mtime = sec
			attr.Mtimensec = nsec
		}
		if sec > attr.Ctime || (sec == attr.Ctime && nsec > attr.Ctimensec) {
			attr.Ctime = sec
			attr.Ctimensec = nsec
		}
	}
	if st.promoted != nil && !st.promoted.finished {
		attr.Mode = st.promoted.mode
		attr.Gid = st.promoted.gid
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) OverlayInode(ino uint64, inode *storagev1.Inode) {
	if c == nil || inode == nil || inode.GetAttr() == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return
	}
	c.mu.Lock()
	st := c.inodes[ino]
	if st == nil || !st.hasPendingLocked() {
		c.mu.Unlock()
		return
	}
	a := inode.Attr
	if st.logicalSize > a.Size {
		a.Size = st.logicalSize
		minBlocks := (a.Size + 511) / 512
		if minBlocks > a.Blocks {
			a.Blocks = minBlocks
		}
	}
	if st.lastWriteTS != nil {
		if compareProtoTimestamp(st.lastWriteTS, a.Mtime) > 0 {
			a.Mtime = cloneTimestamp(st.lastWriteTS)
		}
		if compareProtoTimestamp(st.lastWriteTS, a.Ctime) > 0 {
			a.Ctime = cloneTimestamp(st.lastWriteTS)
		}
	}
	if st.promoted != nil && !st.promoted.finished {
		a.Mode = st.promoted.mode
		a.Gid = st.promoted.gid
		a.Flags = st.promoted.flags
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) Write(ctx context.Context, ino, nodeID, offset uint64, data []byte, cacheEpoch uint64) (written uint32, handled bool, err error) {
	return c.WriteForHandle(ctx, nil, 0, ino, nodeID, offset, data, cacheEpoch)
}

func (c *StageCoordinator) WriteForHandle(
	ctx context.Context,
	cancel <-chan struct{},
	fh, ino, nodeID, offset uint64,
	data []byte,
	cacheEpoch uint64,
) (written uint32, handled bool, err error) {
	if c == nil || len(data) == 0 || len(data) > StageChunkSize || c.writer == nil {
		return 0, false, nil
	}

	rem := data
	curOff := offset

	for len(rem) > 0 {
		step, earlyDone, earlyErr := c.prepareWriteStep(ctx, cancel, fh, ino, nodeID, curOff, rem, cacheEpoch)
		if earlyDone {
			if written > 0 {
				return written, true, nil
			}
			return 0, true, earlyErr
		}

		piece := rem[:step.take]
		ioErr := writeChunkFilePiece(step.chunkPath, step.isPrepend, curOff, step.oldOffset, step.oldLength, piece)

		var onStageEnd uint64
		var onStageTS *timestamppb.Timestamp
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

		if step.isPrepend {
			step.activeChunk.offset = curOff
		}
		newEnd := max(step.oldOffset+step.oldLength, curOff+uint64(step.take))
		step.activeChunk.length = newEnd - step.activeChunk.offset
		step.activeChunk.recordSpanLinkLocked(ctx)
		step.st.nodeID = nodeID
		step.st.cacheEpoch = cacheEpoch
		if curOff+uint64(step.take) > step.st.logicalSize {
			step.st.logicalSize = curOff + uint64(step.take)
		}
		nowTS := timestamppb.Now()
		step.st.lastWriteTS = nowTS
		onStageEnd = step.st.logicalSize
		onStageTS = nowTS
		step.st.activeWriteBusy = false
		c.stateCond.Broadcast()
		if step.activeChunk.length == StageChunkSize {
			c.sealActiveLocked(step.st)
		}
		c.mu.Unlock()

		if c.hooks.OnStage != nil {
			c.hooks.OnStage(nodeID, cacheEpoch, onStageEnd, onStageTS)
		}

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
	isPrepend   bool
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
	cacheEpoch uint64,
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
	var isPrepend bool
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
		st.nodeID = nodeID
		st.cacheEpoch = cacheEpoch

		for st.activeWriteBusy {
			c.stateCond.Wait()
		}
		if c.aborted.Load() {
			err := c.abortErr
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, err
		}
		if st.hasUnseenErrLocked(c, fh) {
			unseenErr := st.consumeErrLocked(c, fh)
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, unseenErr
		}

		take, isPrepend, deltaBytes, needSealFirst = planWriteStepLocked(st, curOff, rem)
		if needSealFirst {
			c.sealActiveLocked(st)
			continue
		}
		if deltaBytes > c.stageSize {
			finishBP()
			c.mu.Unlock()
			return stageWriteStep{}, true, syscall.ENOSPC
		}
		if c.usedBytes+deltaBytes <= c.stageSize {
			finishBP()
			break
		}

		if len(c.queue) == 0 && c.inflight.Load() == 0 {
			if c.sealAllIdleActiveLocked() {
				continue
			}
		}

		if cancel != nil {
			select {
			case <-cancel:
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
			kind:       stageItemChunkFile,
			state:      stageStateActive,
			ino:        ino,
			nodeID:     nodeID,
			cacheEpoch: cacheEpoch,
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
		isPrepend:   isPrepend,
		oldOffset:   oldOffset,
		oldLength:   oldLength,
		deltaBytes:  deltaBytes,
		activeChunk: activeChunk,
		st:          st,
	}, false, nil
}

func planWriteStepLocked(st *stageInode, curOff uint64, rem []byte) (take int, isPrepend bool, deltaBytes uint64, needSealFirst bool) {
	act := st.active
	if act == nil {
		return len(rem), false, uint64(len(rem)), false
	}
	stageStart := act.offset
	stageEnd := act.offset + act.length
	writeEnd := curOff + uint64(len(rem))

	if act.readers > 0 && curOff < stageEnd {
		return 0, false, 0, true
	}

	switch {
	case curOff >= stageStart && curOff <= stageEnd && writeEnd-stageStart <= StageChunkSize:
		newEnd := max(stageEnd, writeEnd)
		return len(rem), false, newEnd - stageEnd, false
	case curOff == stageEnd && act.length < StageChunkSize:
		part := min(len(rem), int(StageChunkSize-act.length))
		return part, false, uint64(part), false
	case curOff < stageStart && writeEnd >= stageStart && max(stageEnd, writeEnd)-curOff <= StageChunkSize:
		newLen := max(stageEnd, writeEnd) - curOff
		return len(rem), true, newLen - act.length, false
	default:
		return 0, false, 0, true
	}
}

func writeChunkFilePiece(path string, isPrepend bool, curOff, oldOffset, oldLength uint64, piece []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if !isPrepend {
		relOff := int64(curOff - oldOffset)
		return writeAtFull(f, piece, relOff)
	}

	buf := acquireStageBuf()
	defer releaseStageBuf(buf)

	oldLen := int(oldLength)
	if oldLen > 0 {
		if _, err := readAtFull(f, buf[:oldLen], 0); err != nil {
			return err
		}
	}
	shift := int(oldOffset - curOff)
	copy(buf[shift:shift+oldLen], buf[:oldLen])
	copy(buf[:len(piece)], piece)
	totalLen := max(len(piece), shift+oldLen)
	return writeAtFull(f, buf[:totalLen], 0)
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
		activeChunk.discarded = true
		c.pendingChunks.Add(-1)
		c.totalActiveItems.Add(-1)
		if activeChunk.readers == 0 && !activeChunk.unlinked && activeChunk.path != "" {
			activeChunk.unlinked = true
			pathToRemove = activeChunk.path
		}
	}
	st.activeWriteBusy = false
	c.stateCond.Broadcast()
	c.wakeBackpressureLocked()
	return pathToRemove
}

func (c *StageCoordinator) PromoteTmpfile(
	ctx context.Context,
	serverIno, nodeID, cacheEpoch uint64,
	file *os.File,
	meta StagePromotedMeta,
	extents []stageExtent,
) {
	if c == nil {
		if file != nil {
			_ = file.Close()
		}
		return
	}

	var xattrsCopy []StagedXattr
	hasCap := false
	if len(meta.Xattrs) > 0 {
		xattrsCopy = make([]StagedXattr, len(meta.Xattrs))
		for i, xa := range meta.Xattrs {
			if xa.Name == securityCapabilityXattr {
				hasCap = true
			}
			xattrsCopy[i] = StagedXattr{
				Name:  xa.Name,
				Value: slices.Clone(xa.Value),
			}
		}
	}

	p := &promotedTmpState{
		file:   file,
		size:   meta.Size,
		atime:  cloneTimestamp(meta.Atime),
		mtime:  cloneTimestamp(meta.Mtime),
		mode:   meta.Mode,
		gid:    meta.Gid,
		flags:  meta.Flags,
		xattrs: xattrsCopy,
		hasCap: hasCap,
	}

	var totalExtentBytes uint64
	for _, ext := range extents {
		totalExtentBytes += ext.length
	}

	c.mu.Lock()
	if c.aborted.Load() {
		c.mu.Unlock()
		if file != nil {
			_ = file.Close()
		}
		return
	}
	st := c.getOrCreateInodeLocked(serverIno)
	st.nodeID = nodeID
	st.cacheEpoch = cacheEpoch
	st.promoted = p
	st.createdInSession = true
	st.hasKnownSize = true
	if meta.Size > st.logicalSize {
		st.logicalSize = meta.Size
	}
	if meta.Mtime != nil {
		st.lastWriteTS = cloneTimestamp(meta.Mtime)
	}

	for _, ext := range extents {
		it := &stageItem{
			seq:        c.nextSeq.Add(1),
			kind:       stageItemPromotedExtent,
			state:      stageStateQueued,
			ino:        serverIno,
			nodeID:     nodeID,
			cacheEpoch: cacheEpoch,
			offset:     ext.offset,
			length:     ext.length,
			gen:        st.streamGen,
			inodeState: st,
			promoted:   p,
		}
		it.recordSpanLinkLocked(ctx)
		st.pending = append(st.pending, it)
		c.queue = append(c.queue, it)
		c.pendingChunks.Add(1)
		c.totalActiveItems.Add(1)
	}

	commitItem := &stageItem{
		seq:        c.nextSeq.Add(1),
		kind:       stageItemPromotedCommit,
		state:      stageStateQueued,
		ino:        serverIno,
		nodeID:     nodeID,
		cacheEpoch: cacheEpoch,
		gen:        st.streamGen,
		inodeState: st,
		promoted:   p,
	}
	commitItem.recordSpanLinkLocked(ctx)
	st.pending = append(st.pending, commitItem)
	c.queue = append(c.queue, commitItem)
	c.totalActiveItems.Add(1)

	c.usedBytes += totalExtentBytes
	c.workerCond.Broadcast()
	c.mu.Unlock()
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
	if st == nil {
		c.mu.Unlock()
		return 0, false, nil
	}
	for st.activeWriteBusy {
		c.stateCond.Wait()
	}
	if c.aborted.Load() || !st.hasPendingLocked() {
		c.mu.Unlock()
		return 0, false, nil
	}

	createdInSession := st.createdInSession
	hasKnownSize := st.hasKnownSize
	logicalSize := st.logicalSize
	promoted := st.promoted
	if promoted != nil {
		promoted.readers++
	}

	var snapshots []readChunkSnapshot
	collect := func(it *stageItem) {
		if it == nil || it.kind != stageItemChunkFile || it.length == 0 || it.discarded || it.uploaded {
			return
		}
		it.readers++
		snapshots = append(snapshots, readChunkSnapshot{
			item:   it,
			offset: it.offset,
			length: it.length,
			path:   it.path,
		})
	}
	for _, it := range st.pending {
		collect(it)
	}
	collect(st.active)
	c.mu.Unlock()

	defer func() {
		c.releaseReadSnapshots(st, promoted, snapshots)
	}()

	reqStart := offset
	reqEnd := offset + uint64(len(dest))

	if createdInSession {
		if reqStart >= logicalSize {
			return 0, true, nil
		}
		readLen := min(uint64(len(dest)), logicalSize-reqStart)
		clear(dest[:readLen])
		if promoted != nil && promoted.file != nil && reqStart < promoted.size {
			pLen := min(readLen, promoted.size-reqStart)
			if _, readErr := readAtFull(promoted.file, dest[:pLen], int64(reqStart)); readErr != nil {
				return 0, true, readErr
			}
		}
		if overlayErr := overlaySnapshotChunks(dest[:readLen], reqStart, snapshots); overlayErr != nil {
			return 0, true, overlayErr
		}
		return int(readLen), true, nil
	}

	targetEnd := reqEnd
	if hasKnownSize {
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

func (c *StageCoordinator) releaseReadSnapshots(st *stageInode, promoted *promotedTmpState, snapshots []readChunkSnapshot) {
	var toRemove []string
	var fileToClose *os.File

	c.mu.Lock()
	for _, snap := range snapshots {
		it := snap.item
		it.readers--
		if it.readers == 0 && (it.uploaded || it.discarded) && !it.unlinked && it.path != "" {
			it.unlinked = true
			toRemove = append(toRemove, it.path)
		}
	}
	if promoted != nil {
		promoted.readers--
		fileToClose = promoted.maybeDetachFileLocked()
	}
	c.stateCond.Broadcast()
	c.mu.Unlock()

	for _, p := range toRemove {
		_ = os.Remove(p)
	}
	if fileToClose != nil {
		_ = fileToClose.Close()
	}
	_ = st
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
	if st != nil {
		for st.activeWriteBusy {
			c.stateCond.Wait()
		}
		if !c.aborted.Load() {
			c.sealActiveLocked(st)
		}
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) barrierInodeLocked(ctx context.Context, st *stageInode) {
	if c.aborted.Load() {
		return
	}
	for st.activeWriteBusy {
		c.stateCond.Wait()
	}
	if c.aborted.Load() {
		return
	}
	c.sealActiveLocked(st)
	if len(st.pending) == 0 && st.inflightHooks == 0 {
		return
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
	for !c.aborted.Load() {
		for st.activeWriteBusy {
			c.stateCond.Wait()
		}
		if c.aborted.Load() {
			break
		}
		c.sealActiveLocked(st)
		if len(st.pending) == 0 && st.inflightHooks == 0 {
			break
		}
		c.stateCond.Wait()
	}
	waitSpan.End()
	c.metrics.Load().observeStageBarrierWait(time.Since(waitStart))
}

func (c *StageCoordinator) BarrierInode(ctx context.Context, ino uint64) {
	if c == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return
	}
	c.mu.Lock()
	st := c.inodes[ino]
	if st != nil {
		c.barrierInodeLocked(ctx, st)
	}
	c.mu.Unlock()
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
	c.barrierInodeLocked(ctx, st)
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
	c.barrierInodeLocked(ctx, st)
	err := st.consumeErrLocked(c, fh)
	if c.aborted.Load() && err == nil {
		err = c.abortErr
	}
	st.resetStreamLocked()
	c.mu.Unlock()
	return err
}

func (c *StageCoordinator) ResetInodeStream(ctx context.Context, ino uint64) {
	if c == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return
	}
	c.mu.Lock()
	st := c.inodes[ino]
	if st != nil {
		c.barrierInodeLocked(ctx, st)
		st.resetStreamLocked()
		st.createdInSession = false
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) HasPromotedPending(ino uint64) bool {
	if c == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return false
	}
	c.mu.Lock()
	st := c.inodes[ino]
	pending := st != nil && st.promoted != nil && !st.promoted.finished
	c.mu.Unlock()
	return pending
}

func (c *StageCoordinator) BarrierAll(ctx context.Context) {
	if c == nil || c.aborted.Load() || c.totalActiveItems.Load() == 0 {
		return
	}
	c.mu.Lock()
	for _, st := range c.inodes {
		if c.aborted.Load() {
			break
		}
		if st.hasPendingLocked() || st.activeWriteBusy {
			c.barrierInodeLocked(ctx, st)
		}
	}
	c.mu.Unlock()
}

func (c *StageCoordinator) DiscardInode(ino uint64) {
	if c == nil {
		return
	}
	var pathsToRemove []string
	var fileToClose *os.File

	c.mu.Lock()
	st := c.inodes[ino]
	if st == nil {
		c.mu.Unlock()
		return
	}
	for st.activeWriteBusy {
		c.stateCond.Wait()
	}

	close(st.discardCh)
	st.discardCh = make(chan struct{})

	if st.active != nil {
		it := st.active
		st.active = nil
		it.state = stageStateDone
		it.discarded = true
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
	var droppedTotal int64
	keepQueue := 0
	for _, qIt := range c.queue {
		if qIt.ino == ino && qIt.state == stageStateQueued {
			qIt.state = stageStateDone
			qIt.discarded = true
			droppedTotal++
			if qIt.kind != stageItemPromotedCommit {
				droppedChunks++
				freedBytes += qIt.length
			}
			if qIt.kind == stageItemChunkFile && qIt.readers == 0 && !qIt.unlinked && qIt.path != "" {
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
			pIt.discarded = true
			st.pending[keepPending] = pIt
			keepPending++
		}
	}
	clear(st.pending[keepPending:])
	st.pending = st.pending[:keepPending]

	if droppedChunks > 0 {
		c.pendingChunks.Add(-droppedChunks)
	}
	if droppedTotal > 0 {
		c.totalActiveItems.Add(-droppedTotal)
	}
	if freedBytes > 0 {
		if c.usedBytes >= freedBytes {
			c.usedBytes -= freedBytes
		} else {
			c.usedBytes = 0
		}
	}
	if len(st.pending) == 0 && st.promoted != nil {
		st.promoted.finished = true
		fileToClose = st.promoted.maybeDetachFileLocked()
		st.promoted = nil
	}
	c.wakeBackpressureLocked()

	for st.hasInflightLocked() && !c.aborted.Load() {
		c.stateCond.Wait()
	}
	if st.promoted != nil && len(st.pending) == 0 {
		st.promoted.finished = true
		if f := st.promoted.maybeDetachFileLocked(); f != nil {
			fileToClose = f
		}
		st.promoted = nil
	}
	st.createdInSession = false
	st.hasKnownSize = false
	st.logicalSize = 0
	st.lastWriteTS = nil
	st.resetStreamLocked()
	delete(c.inodes, ino)
	c.stateCond.Broadcast()
	c.mu.Unlock()

	for _, p := range pathsToRemove {
		_ = os.Remove(p)
	}
	if fileToClose != nil {
		_ = fileToClose.Close()
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
	var filesToClose []*os.File

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
	var droppedTotal int64
	for i, qIt := range c.queue {
		c.queue[i] = nil
		if qIt != nil && qIt.state == stageStateQueued {
			qIt.state = stageStateDone
			qIt.discarded = true
			droppedTotal++
			if qIt.kind != stageItemPromotedCommit {
				droppedChunks++
				freedBytes += qIt.length
			}
			if qIt.kind == stageItemChunkFile && qIt.readers == 0 && !qIt.unlinked && qIt.path != "" {
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
			it.discarded = true
			if !st.activeWriteBusy {
				st.active = nil
				it.state = stageStateDone
				droppedChunks++
				droppedTotal++
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
				pIt.discarded = true
				st.pending[keepPending] = pIt
				keepPending++
			}
		}
		clear(st.pending[keepPending:])
		st.pending = st.pending[:keepPending]

		if st.promoted != nil {
			st.promoted.hasErr = true
			if len(st.pending) == 0 {
				st.promoted.finished = true
				if f := st.promoted.maybeDetachFileLocked(); f != nil {
					filesToClose = append(filesToClose, f)
				}
				st.promoted = nil
			}
		}

		st.createdInSession = false
		st.hasKnownSize = false
		st.logicalSize = 0
		st.lastWriteTS = nil
	}

	if droppedChunks > 0 {
		c.pendingChunks.Add(-droppedChunks)
	}
	if droppedTotal > 0 {
		c.totalActiveItems.Add(-droppedTotal)
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
	for _, f := range filesToClose {
		_ = f.Close()
	}
}

func (c *StageCoordinator) Clear(ctx context.Context) {
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

	if !aborted {
		drainDone := make(chan struct{})
		go func() {
			c.BarrierAll(ctx)
			close(drainDone)
		}()

		if ctx != nil {
			select {
			case <-drainDone:
			case <-ctx.Done():
				c.stopRetryOnce.Do(func() { close(c.stopRetryCh) })
				<-drainDone
			}
		} else {
			<-drainDone
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

	var leftoverFiles []*os.File
	var leftoverPaths []string
	c.mu.Lock()
	c.workersDone = true
	for _, st := range c.inodes {
		if st.promoted != nil {
			st.promoted.finished = true
			if f := st.promoted.maybeDetachFileLocked(); f != nil {
				leftoverFiles = append(leftoverFiles, f)
			}
			st.promoted = nil
		}
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
	c.mu.Unlock()

	for _, f := range leftoverFiles {
		_ = f.Close()
	}
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
		c.mu.Unlock()

		c.inflight.Add(1)
		c.metrics.Load().addStageInflight(1)
		c.processUploadItem(item, discardCh)
		c.inflight.Add(-1)
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
		if c.aborted.Load() {
			discardedMid = true
			break
		}
		if item.kind == stageItemPromotedCommit {
			c.mu.Lock()
			skipCommit := item.promoted != nil && item.promoted.hasErr
			c.mu.Unlock()
			if skipCommit {
				break
			}
			rpcCtx, cancel := context.WithTimeout(uploadBaseCtx, DefaultRPCTimeout)
			if c.committer != nil && item.promoted != nil {
				meta := StagePromotedMeta{
					Size:   item.promoted.size,
					Atime:  cloneTimestamp(item.promoted.atime),
					Mtime:  cloneTimestamp(item.promoted.mtime),
					Mode:   item.promoted.mode,
					Gid:    item.promoted.gid,
					Flags:  item.promoted.flags,
					Xattrs: item.promoted.xattrs,
				}
				respInode, finalErr = c.committer(rpcCtx, item.ino, meta)
			} else {
				finalErr = syscall.EIO
			}
			cancel()
		} else {
			buf := acquireStageBuf()
			dataLen := int(item.length)
			var readErr error
			switch {
			case item.kind == stageItemChunkFile:
				readErr = readChunkBytesForUpload(item.path, buf[:dataLen])
			case item.promoted != nil && item.promoted.file != nil:
				_, readErr = readAtFull(item.promoted.file, buf[:dataLen], int64(item.offset))
			default:
				readErr = syscall.EIO
			}
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
		}

		if finalErr == nil {
			break
		}
		if c.aborted.Load() {
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
		aborted := c.aborted.Load() || item.discarded || (st == nil)
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
		if item.kind != stageItemPromotedCommit {
			c.metrics.Load().incStageUploadRPC(true)
		}
		c.activeLogger().ErrorContext(uploadBaseCtx, "stage_upload_failed",
			"ino", item.ino,
			"offset", item.offset,
			"length", item.length,
			"error", finalErr,
		)
	} else if item.kind != stageItemPromotedCommit && !discardedMid {
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
	var pathToRemove string
	var fileToClose *os.File
	var onCommitNodeID uint64
	var onCommitIno uint64
	var onCommitEpoch uint64
	var onCommitMinSize uint64
	var onCommitRefresh bool
	var onCommitPreserveMeta bool
	var onCommitCapPresent bool
	var callOnCommit bool
	var callOnError bool
	var onErrorNodeID uint64

	c.mu.Lock()
	st := item.inodeState
	item.state = stageStateDone
	item.uploaded = true

	if item.kind == stageItemChunkFile {
		if item.readers == 0 && !item.unlinked && item.path != "" {
			item.unlinked = true
			pathToRemove = item.path
		}
	}

	if item.kind != stageItemPromotedCommit {
		c.pendingChunks.Add(-1)
		if c.usedBytes >= item.length {
			c.usedBytes -= item.length
		} else {
			c.usedBytes = 0
		}
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

		if item.kind == stageItemChunkFile || item.kind == stageItemPromotedCommit {

			st.createdInSession = false
		}

		if item.kind == stageItemPromotedCommit && item.promoted != nil {
			item.promoted.finished = true
			fileToClose = item.promoted.maybeDetachFileLocked()
			if st.promoted == item.promoted {
				st.promoted = nil
			}
		} else if c.aborted.Load() && st.promoted != nil && len(st.pending) == 0 {
			st.promoted.finished = true
			fileToClose = st.promoted.maybeDetachFileLocked()
			st.promoted = nil
		}

		if !c.aborted.Load() {
			if finalErr != nil && !discardedMid {
				if notFoundAbort {
					if item.promoted != nil {
						item.promoted.hasErr = true
					}
				} else {
					if item.promoted != nil {
						item.promoted.hasErr = true
					}
					if st.errGen != item.gen {
						st.errGen = item.gen
						st.lastErr = finalErr
						st.errSeq = c.errSeqGen.Add(1)
					}
					if st.streamGen == item.gen {
						st.resetStreamLocked()
					}
					callOnError = true
					onErrorNodeID = item.nodeID
				}
			} else if finalErr == nil && !discardedMid && !item.discarded {
				if respInode != nil && respInode.GetAttr() != nil {
					if respInode.GetAttr().GetSize() > st.logicalSize {
						st.logicalSize = respInode.GetAttr().GetSize()
					}
					st.hasKnownSize = true
				}
				if item.kind == stageItemChunkFile || (item.kind == stageItemPromotedCommit && respInode != nil) {
					callOnCommit = true
					onCommitNodeID = item.nodeID
					onCommitIno = item.ino
					onCommitEpoch = item.cacheEpoch
					if item.kind == stageItemPromotedCommit && item.promoted != nil {
						onCommitCapPresent = item.promoted.hasCap
					}
					if item.gen == st.streamGen {
						onCommitMinSize = st.logicalSize
						if isNotOlderCommittedInode(st.lastCommitted, respInode) {
							st.lastCommitted = cloneInode(respInode)
							onCommitRefresh = true
						}
					}
				} else if item.kind == stageItemPromotedExtent {
					callOnCommit = true
					onCommitNodeID = item.nodeID
					onCommitIno = item.ino
					onCommitEpoch = item.cacheEpoch
					onCommitPreserveMeta = true
				}
			}
		}
	}

	waitHooks := st != nil && (callOnCommit || callOnError)
	if waitHooks {
		st.inflightHooks++
	} else {
		c.totalActiveItems.Add(-1)
	}
	c.wakeBackpressureLocked()
	c.workerCond.Broadcast()
	c.stateCond.Broadcast()
	c.mu.Unlock()

	if pathToRemove != "" {
		_ = os.Remove(pathToRemove)
	}
	if fileToClose != nil {
		_ = fileToClose.Close()
	}
	if callOnError && c.hooks.OnError != nil {
		c.hooks.OnError(onErrorNodeID)
	}
	if callOnCommit && c.hooks.OnCommit != nil {
		c.hooks.OnCommit(onCommitNodeID, onCommitIno, onCommitEpoch, respInode, onCommitMinSize, onCommitRefresh, onCommitPreserveMeta, onCommitCapPresent)
	}
	if waitHooks {
		c.mu.Lock()
		st.inflightHooks--
		c.totalActiveItems.Add(-1)
		c.stateCond.Broadcast()
		c.mu.Unlock()
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

func compareProtoTimestamp(a, b *timestamppb.Timestamp) int {
	var aSec, bSec int64
	var aNano, bNano int32
	if a != nil {
		aSec, aNano = a.GetSeconds(), a.GetNanos()
	}
	if b != nil {
		bSec, bNano = b.GetSeconds(), b.GetNanos()
	}
	if aSec != bSec {
		if aSec > bSec {
			return 1
		}
		return -1
	}
	if aNano != bNano {
		if aNano > bNano {
			return 1
		}
		return -1
	}
	return 0
}

func isNotOlderCommittedInode(prev, cand *storagev1.Inode) bool {
	if cand == nil || cand.GetAttr() == nil {
		return false
	}
	if prev == nil || prev.GetAttr() == nil {
		return true
	}
	candAttr := cand.GetAttr()
	prevAttr := prev.GetAttr()
	if candAttr.GetSize() != prevAttr.GetSize() {
		return candAttr.GetSize() > prevAttr.GetSize()
	}
	if cmp := compareProtoTimestamp(candAttr.GetCtime(), prevAttr.GetCtime()); cmp != 0 {
		return cmp > 0
	}
	return compareProtoTimestamp(candAttr.GetMtime(), prevAttr.GetMtime()) >= 0
}
