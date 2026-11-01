package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// validateEntryName 校验目录项名称合法性。
// len(name) > MaxFileNameBytes 时返回 ErrNameTooLong；
// len(name) == 0 或包含 '/' 或 '\x00' 时返回 ErrInvalidArgument。
func validateEntryName(name []byte) error {
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	if len(name) == 0 || bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}

// isDotOrDotDot 判断目录项名称是否为 "." 或 ".."。
func isDotOrDotDot(name []byte) bool {
	return bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte(".."))
}

// Manager 定义元数据领域层管理器接口。
type Manager interface {
	Lookup(ctx context.Context, parentIno uint64, name []byte) (*Inode, error)
	GetAttr(ctx context.Context, ino uint64) (*Inode, error)
	SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error)
	Mknod(ctx context.Context, params MknodParams) (*Inode, error)
	Mkdir(ctx context.Context, params MkdirParams) (*Inode, error)
	Symlink(ctx context.Context, params SymlinkParams) (*Inode, error)
	Readlink(ctx context.Context, ino uint64) ([]byte, error)
	Link(ctx context.Context, params LinkParams) (*Inode, error)
	Unlink(ctx context.Context, parentIno uint64, name []byte) error
	Rmdir(ctx context.Context, parentIno uint64, name []byte) error
	Rename(ctx context.Context, params RenameParams) error
	Create(ctx context.Context, params CreateParams) (*CreateResult, error)
	TmpFile(ctx context.Context, params TmpFileParams) (*CreateResult, error)
	Open(ctx context.Context, params OpenParams) (*OpenResult, error)
	Flush(ctx context.Context, params FlushParams) error
	Fsync(ctx context.Context, params FsyncParams) error
	Release(ctx context.Context, params ReleaseParams) error
	OpenDir(ctx context.Context, params OpenDirParams) (*OpenDirResult, error)
	FsyncDir(ctx context.Context, params FsyncDirParams) error
	ReleaseDir(ctx context.Context, params ReleaseDirParams) error
	ReadDir(ctx context.Context, params ReadDirParams) ([]DirEntry, error)
	ReadDirPlus(ctx context.Context, params ReadDirPlusParams) ([]DirEntryPlus, error)
	Fallocate(ctx context.Context, params FallocateParams) error
	SetXattr(ctx context.Context, params SetXattrParams) error
	GetXattr(ctx context.Context, ino uint64, name []byte) ([]byte, error)
	ListXattr(ctx context.Context, ino uint64) ([][]byte, error)
	RemoveXattr(ctx context.Context, ino uint64, name []byte) error
	GetLk(ctx context.Context, params GetLkParams) (*FileLock, error)
	SetLk(ctx context.Context, params SetLkParams) error
	SetLkw(ctx context.Context, params SetLkwParams) error
	RenewSession(ctx context.Context, sessionID uint64) (time.Time, error)
	RenewSessionWithStatus(ctx context.Context, sessionID uint64) (RenewSessionResult, error)
	RenewSessionWithDisk(ctx context.Context, sessionID uint64, diskID *string) (RenewSessionResult, error)
	CloseSession(ctx context.Context, sessionID uint64) error
	WatchHub() *SessionWatchHub
	ValidateWatchSession(sessionID uint64, diskID *string) error
	GetParents(ctx context.Context, ino uint64) (map[uint64]uint32, error)
	CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error)
	GetDisk(ctx context.Context, diskID string) (*Disk, error)
	ListDisks(ctx context.Context, params ListDisksParams) (*ListDisksResult, error)
	UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error)
	DeleteDisk(ctx context.Context, diskID string) error
	StatFS(ctx context.Context, ino uint64) (*StatFSResult, error)
	Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error)
	Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error)
	CompactChunk(ctx context.Context, ino uint64, indx int64) error
	CollectGarbage(ctx context.Context, limit int32) (int, error)
	StartGCWorker(ctx context.Context, interval time.Duration, batchLimit int32)
}

type chunkKey struct {
	ino  uint64
	indx int64
}

type dirCursorKey struct {
	ino    uint64
	fh     uint64
	offset uint64
}

type dirHandleKey struct {
	ino uint64
	fh  uint64
}

type inFlightDirRead struct {
	ino                 uint64
	fh                  uint64
	invalidatedAll      bool
	invalidatedSessions map[uint64]struct{}
}

type dirCursorAnchor struct {
	lastName        []byte
	sqlOffset       int64
	dirMutated      bool
	sessionID       uint64
	handleCreatedAt time.Time
}

type writeSessionTouchState struct {
	lastTouchedAt    time.Time
	sessionCreatedAt pgtype.Timestamptz
}

type manager struct {
	store                       db.Store
	blockStore                  BlockStore
	entryTimeout                float64
	attrTimeout                 float64
	dirEntryTimeout             float64
	openKeepCache               bool
	openDirCache                bool
	atimeMode                   AtimeMode
	sessionLeaseTTL             time.Duration
	sessionProbeTimeout         time.Duration
	gcInterval                  time.Duration
	gcBatchLimit                int32
	gcOrphanGracePeriod         time.Duration
	compactSliceThreshold       int
	sliceIDBatchSize            int
	sliceIDMu                   sync.Mutex
	sliceIDQueue                []int64
	lastAllocatedSliceID        int64
	writeSessionTouchInterval   time.Duration
	writeSessionTouchDisabled   bool
	writeSessionTouchConfigured bool
	writeSessionTouchMu         sync.Mutex
	writeSessionTouches         map[uint64]writeSessionTouchState
	writeGroupMaxBatchSize      int
	writeInFlightStage1         atomic.Int64
	writeGroupMu                sync.Mutex
	writeLeaderActive           bool
	writePendingQueue           []*writeBatchTask
	writeCoalesceCh             chan struct{}
	inFlightCompactsMu          sync.Mutex
	inFlightCompacts            map[chunkKey]bool
	lockNotifyMu                sync.Mutex
	lockNotifyChans             map[uint64]*lockNotifyEntry
	crossDirRenameMu            sync.Mutex
	dirCursorMu                 sync.Mutex
	dirCursors                  map[dirCursorKey]dirCursorAnchor
	dirMutationSeq              map[uint64]uint64
	inFlightDirMutations        map[uint64]int
	activeDirReads              map[uint64]*inFlightDirRead
	nextDirReadID               uint64
	inFlightGrantMu             sync.Mutex
	inFlightGrants              map[inFlightGrantKey]*inFlightGrant
	nextInFlightGrantID         uint64
	deferredOrphansMu           sync.Mutex
	deferredOrphanInos          map[int64]struct{}
	quotaMu                     sync.Mutex
	diskQuotas                  map[int64]*diskQuotaState
	watchHubMu                  sync.Mutex
	watchHub                    *SessionWatchHub
}

func (m *manager) markDeferredOrphan(ino int64) {
	if ino <= 0 {
		return
	}
	m.deferredOrphansMu.Lock()
	if m.deferredOrphanInos == nil {
		m.deferredOrphanInos = make(map[int64]struct{})
	}
	m.deferredOrphanInos[ino] = struct{}{}
	m.deferredOrphansMu.Unlock()
}

func (m *manager) unmarkDeferredOrphan(inos ...int64) {
	if len(inos) == 0 {
		return
	}
	m.deferredOrphansMu.Lock()
	for _, ino := range inos {
		delete(m.deferredOrphanInos, ino)
	}
	m.deferredOrphansMu.Unlock()
}

func (m *manager) hasDeferredOrphans() bool {
	m.deferredOrphansMu.Lock()
	n := len(m.deferredOrphanInos)
	m.deferredOrphansMu.Unlock()
	return n > 0
}

type poolProvider interface {
	Pool() *pgxpool.Pool
}

func queryDBTX(q db.Querier, pp poolProvider) db.DBTX {
	if r, ok := q.(interface{ RawDBTX() db.DBTX }); ok && r != nil && r.RawDBTX() != nil {
		return r.RawDBTX()
	}
	if pp != nil && pp.Pool() != nil {
		return pp.Pool()
	}
	return nil
}

func (m *manager) shouldThrottleSessionTouch(ctx context.Context) (poolProvider, bool) {
	if m.writeSessionTouchDisabled || m.writeSessionTouchInterval <= 0 {
		return nil, false
	}
	if _, hasLockSync := db.LockSyncInoFromContext(ctx); hasLockSync {
		return nil, false
	}
	if pp, ok := m.store.(poolProvider); ok && pp != nil && pp.Pool() != nil {
		return pp, true
	}
	if m.writeSessionTouchConfigured {
		return nil, true
	}
	return nil, false
}

var _ Manager = (*manager)(nil)

// Option 定义 Manager 的配置选项。
type Option func(*manager)

// WithBlockStore 配置底层物理块存储引擎。
func WithBlockStore(bs BlockStore) Option {
	return func(m *manager) {
		m.blockStore = bs
	}
}

// BlockStore 返回底层物理块存储引擎（未配置时可能为 nil）。
func (m *manager) BlockStore() BlockStore {
	return m.blockStore
}

// getBlockStore 返回底层物理块存储引擎；若未配置则兜底使用基于临时目录的 LocalFSBlockStore。
func (m *manager) getBlockStore() BlockStore {
	if m.blockStore != nil {
		return m.blockStore
	}
	return NewLocalFSBlockStore(filepath.Join(os.TempDir(), "bowl_data"))
}

// WithSessionLeaseTTL 配置客户端会话租约的续期时长（TTL <= 0 时忽略并保持默认值）。
func WithSessionLeaseTTL(ttl time.Duration) Option {
	return func(m *manager) {
		if ttl > 0 {
			m.sessionLeaseTTL = ttl
		}
	}
}

// WithSessionProbeTimeout 配置删盘时对活跃客户端会话流发送 Ping-Pong 探活的超时窗口（d > 0 时生效，默认 DefaultSessionProbeTimeout）。
func WithSessionProbeTimeout(d time.Duration) Option {
	return func(m *manager) {
		if d > 0 {
			m.sessionProbeTimeout = d
		}
	}
}

// WithEntryTimeout 配置非目录节点内核条目缓存有效时间（秒）。
func WithEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.entryTimeout = d
		}
	}
}

// WithAttrTimeout 配置节点内核属性缓存有效时间（秒）。
func WithAttrTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.attrTimeout = d
		}
	}
}

// WithDirEntryTimeout 配置目录节点内核条目缓存有效时间（秒）。
func WithDirEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.dirEntryTimeout = d
		}
	}
}

// WithOpenKeepCache 配置打开文件是否保留内核页缓存。
func WithOpenKeepCache(enable bool) Option {
	return func(m *manager) {
		m.openKeepCache = enable
	}
}

// WithOpenDirCache 配置打开目录是否保留内核目录缓存。
func WithOpenDirCache(enable bool) Option {
	return func(m *manager) {
		m.openDirCache = enable
	}
}

// WithAtimeMode 配置访问时间更新策略。
func WithAtimeMode(mode AtimeMode) Option {
	return func(m *manager) {
		m.atimeMode = mode
	}
}

// WithCompactSliceThreshold 配置单 Chunk 累积写入 Slice 达到触发 Compaction 的阈值。
func WithCompactSliceThreshold(threshold int) Option {
	return func(m *manager) {
		if threshold > 0 {
			m.compactSliceThreshold = threshold
		}
	}
}

// WithGCInterval 配置后台 GC Worker 扫描轮询周期（d > 0 时生效，默认 DefaultGCInterval）。
func WithGCInterval(d time.Duration) Option {
	return func(m *manager) {
		if d > 0 {
			m.gcInterval = d
		}
	}
}

// WithGCBatchLimit 配置后台 GC Worker 单批次扫描回收上限（limit > 0 时生效，默认 DefaultGCBatchLimit）。
func WithGCBatchLimit(limit int32) Option {
	return func(m *manager) {
		if limit > 0 {
			m.gcBatchLimit = limit
		}
	}
}

// WithGCOrphanGracePeriod 配置后台 GC 扫描未在 slice_refs 登记的孤儿物理块文件的安全宽限期（d > 0 时生效，默认 DefaultGCOrphanGracePeriod）。
func WithGCOrphanGracePeriod(d time.Duration) Option {
	return func(m *manager) {
		if d > 0 {
			m.gcOrphanGracePeriod = d
		}
	}
}

// WithSliceIDBatchSize 配置服务端分配 slice_id 的内存号段预取步长（batchSize > 0 时生效，默认 DefaultSliceIDBatchSize）。
func WithSliceIDBatchSize(batchSize int) Option {
	return func(m *manager) {
		if batchSize > 0 {
			m.sliceIDBatchSize = batchSize
		}
	}
}

// WithWriteSessionTouchInterval 配置对同一活跃客户端会话执行租约续期的最小节流时间窗口（d >= 0 时生效，默认 DefaultWriteSessionTouchInterval）。
func WithWriteSessionTouchInterval(d time.Duration) Option {
	return func(m *manager) {
		if d == 0 {
			m.writeSessionTouchInterval = 0
			m.writeSessionTouchDisabled = true
			m.writeSessionTouchConfigured = false
		} else if d > 0 {
			m.writeSessionTouchInterval = d
			m.writeSessionTouchDisabled = false
			m.writeSessionTouchConfigured = true
		}
	}
}

// WithWriteGroupMaxBatchSize 配置 Write 阶段 2 Group Commit 单批次最大聚合请求数（batchSize > 0 时生效，默认 DefaultWriteGroupMaxBatchSize）。
func WithWriteGroupMaxBatchSize(batchSize int) Option {
	return func(m *manager) {
		if batchSize > 0 {
			m.writeGroupMaxBatchSize = batchSize
		}
	}
}

// New 创建 Manager 实例。
func New(store db.Store, opts ...Option) Manager {
	m := &manager{
		store:                     store,
		entryTimeout:              DefaultEntryTimeoutSeconds,
		attrTimeout:               DefaultAttrTimeoutSeconds,
		dirEntryTimeout:           DefaultEntryTimeoutSeconds,
		sessionLeaseTTL:           DefaultSessionLeaseTTL,
		sessionProbeTimeout:       DefaultSessionProbeTimeout,
		gcInterval:                DefaultGCInterval,
		gcBatchLimit:              DefaultGCBatchLimit,
		gcOrphanGracePeriod:       DefaultGCOrphanGracePeriod,
		compactSliceThreshold:     CompactSliceThreshold,
		sliceIDBatchSize:          DefaultSliceIDBatchSize,
		writeSessionTouchInterval: DefaultWriteSessionTouchInterval,
		writeSessionTouches:       make(map[uint64]writeSessionTouchState),
		writeGroupMaxBatchSize:    DefaultWriteGroupMaxBatchSize,
		writeCoalesceCh:           make(chan struct{}, 1),
		inFlightCompacts:          make(map[chunkKey]bool),
		lockNotifyChans:           make(map[uint64]*lockNotifyEntry),
		dirCursors:                make(map[dirCursorKey]dirCursorAnchor),
		dirMutationSeq:            make(map[uint64]uint64),
		inFlightDirMutations:      make(map[uint64]int),
		activeDirReads:            make(map[uint64]*inFlightDirRead),
		inFlightGrants:            make(map[inFlightGrantKey]*inFlightGrant),
		watchHub:                  NewSessionWatchHub(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	if !m.writeSessionTouchConfigured && !m.writeSessionTouchDisabled && m.sessionLeaseTTL > 0 && m.writeSessionTouchInterval > m.sessionLeaseTTL/3 {
		m.writeSessionTouchInterval = m.sessionLeaseTTL / 3
	}
	return m
}

// allocSliceID 从内存号段分配器领取一个全局唯一 slice_id；当缓存号段耗尽时按步长向底层 Store 批量预取。
func (m *manager) allocSliceID(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	m.sliceIDMu.Lock()
	defer m.sliceIDMu.Unlock()

	if len(m.sliceIDQueue) > 0 {
		id := m.sliceIDQueue[0]
		m.sliceIDQueue = m.sliceIDQueue[1:]
		return id, nil
	}

	batchSize := m.sliceIDBatchSize
	if batchSize <= 0 {
		batchSize = DefaultSliceIDBatchSize
	}

	firstID, err := m.store.NextSliceID(ctx)
	if err != nil {
		return 0, err
	}
	if batchSize <= 1 {
		if firstID > m.lastAllocatedSliceID {
			m.lastAllocatedSliceID = firstID
		}
		return firstID, nil
	}

	type poolProvider interface {
		Pool() *pgxpool.Pool
	}
	if pp, ok := m.store.(poolProvider); ok && pp != nil && pp.Pool() != nil {
		rows, qErr := pp.Pool().Query(ctx, `SELECT nextval('slice_id_seq')::bigint FROM generate_series(1, $1)`, batchSize-1)
		if qErr != nil {
			if firstID > m.lastAllocatedSliceID {
				m.lastAllocatedSliceID = firstID
			}
			return firstID, nil
		}
		if cap(m.sliceIDQueue) < batchSize-1 {
			m.sliceIDQueue = make([]int64, 0, batchSize-1)
		} else {
			m.sliceIDQueue = m.sliceIDQueue[:0]
		}
		for rows.Next() {
			var nextID int64
			if scanErr := rows.Scan(&nextID); scanErr != nil {
				break
			}
			m.sliceIDQueue = append(m.sliceIDQueue, nextID)
			if nextID > m.lastAllocatedSliceID {
				m.lastAllocatedSliceID = nextID
			}
		}
		rows.Close()
		if firstID > m.lastAllocatedSliceID {
			m.lastAllocatedSliceID = firstID
		}
		return firstID, nil
	}

	if firstID <= m.lastAllocatedSliceID {
		firstID = m.lastAllocatedSliceID + 1
	}
	if cap(m.sliceIDQueue) < batchSize-1 {
		m.sliceIDQueue = make([]int64, 0, batchSize-1)
	} else {
		m.sliceIDQueue = m.sliceIDQueue[:0]
	}
	for i := 1; i < batchSize; i++ {
		m.sliceIDQueue = append(m.sliceIDQueue, firstID+int64(i))
	}
	m.lastAllocatedSliceID = firstID + int64(batchSize-1)
	return firstID, nil
}

func (m *manager) getDirMutationSeq(ino uint64) uint64 {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.dirMutationSeq == nil {
		return 0
	}
	return m.dirMutationSeq[ino]
}

func (m *manager) getDirCursorOffset(ino, fh, offset uint64) int64 {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.dirCursors == nil {
		return int64(offset - 2)
	}
	if anchor, ok := m.dirCursors[dirCursorKey{ino: ino, fh: fh, offset: offset}]; ok {
		return anchor.sqlOffset
	}
	return int64(offset - 2)
}

func (m *manager) lookupDirCursorAnchor(ino, fh, offset uint64) (dirCursorAnchor, bool) {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.dirCursors == nil {
		return dirCursorAnchor{}, false
	}
	anchor, ok := m.dirCursors[dirCursorKey{ino: ino, fh: fh, offset: offset}]
	return anchor, ok
}

func (m *manager) beginDirCursorRead(ino, fh uint64) uint64 {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.activeDirReads == nil {
		m.activeDirReads = make(map[uint64]*inFlightDirRead)
	}
	m.nextDirReadID++
	readID := m.nextDirReadID
	m.activeDirReads[readID] = &inFlightDirRead{
		ino: ino,
		fh:  fh,
	}
	return readID
}

func (m *manager) endDirCursorRead(readID uint64) {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	delete(m.activeDirReads, readID)
}

func (m *manager) recordDirCursors(readID uint64, cursors []pendingDirCursor) {
	if len(cursors) == 0 {
		return
	}
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	r, ok := m.activeDirReads[readID]
	if !ok || r.invalidatedAll {
		return
	}
	for _, pc := range cursors {
		if r.invalidatedSessions != nil {
			if _, bad := r.invalidatedSessions[pc.sessionID]; bad {
				continue
			}
		}
		if m.dirCursors == nil {
			m.dirCursors = make(map[dirCursorKey]dirCursorAnchor)
		}
		dirMutated := pc.dirMutated
		if (m.dirMutationSeq != nil && m.dirMutationSeq[pc.ino] != pc.startMutationSeq) ||
			(m.inFlightDirMutations != nil && m.inFlightDirMutations[pc.ino] > 0) {
			dirMutated = true
		}
		m.dirCursors[dirCursorKey{ino: pc.ino, fh: pc.fh, offset: pc.nextOffset}] = dirCursorAnchor{
			lastName:        bytes.Clone(pc.lastName),
			sqlOffset:       pc.sqlOffset,
			dirMutated:      dirMutated,
			sessionID:       pc.sessionID,
			handleCreatedAt: pc.handleCreatedAt,
		}
	}
}

func (m *manager) recordDirCursor(ino, fh, nextOffset uint64, lastName []byte, sqlOffset int64, dirMutated bool, sessionID uint64, handleCreatedAt time.Time, startMutationSeq uint64) {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.dirCursors == nil {
		m.dirCursors = make(map[dirCursorKey]dirCursorAnchor)
	}
	if (m.dirMutationSeq != nil && m.dirMutationSeq[ino] != startMutationSeq) ||
		(m.inFlightDirMutations != nil && m.inFlightDirMutations[ino] > 0) {
		dirMutated = true
	}
	m.dirCursors[dirCursorKey{ino: ino, fh: fh, offset: nextOffset}] = dirCursorAnchor{
		lastName:        bytes.Clone(lastName),
		sqlOffset:       sqlOffset,
		dirMutated:      dirMutated,
		sessionID:       sessionID,
		handleCreatedAt: handleCreatedAt,
	}
}

func (m *manager) onDirEntryRemoved(parentIno uint64, name []byte) {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.dirMutationSeq == nil {
		m.dirMutationSeq = make(map[uint64]uint64)
	}
	m.dirMutationSeq[parentIno]++
	for k, anchor := range m.dirCursors {
		if k.ino == parentIno {
			anchor.dirMutated = true
			if len(name) > 0 && bytes.Compare(name, anchor.lastName) <= 0 && anchor.sqlOffset > 0 {
				anchor.sqlOffset--
			}
			m.dirCursors[k] = anchor
		}
	}
}

func (m *manager) onDirEntryMutated(parentIno uint64) {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.dirMutationSeq == nil {
		m.dirMutationSeq = make(map[uint64]uint64)
	}
	m.dirMutationSeq[parentIno]++
	for k, anchor := range m.dirCursors {
		if k.ino == parentIno {
			anchor.dirMutated = true
			m.dirCursors[k] = anchor
		}
	}
}

func (m *manager) clearDirCursors(ino, fh uint64) {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	for _, r := range m.activeDirReads {
		if r.ino == ino && r.fh == fh {
			r.invalidatedAll = true
		}
	}
	for k := range m.dirCursors {
		if k.ino == ino && k.fh == fh {
			delete(m.dirCursors, k)
		}
	}
}

func (m *manager) clearDirCursorsForSession(ino, fh, sessionID uint64) {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	for _, r := range m.activeDirReads {
		if r.ino == ino && r.fh == fh {
			if r.invalidatedSessions == nil {
				r.invalidatedSessions = make(map[uint64]struct{})
			}
			r.invalidatedSessions[sessionID] = struct{}{}
		}
	}
	for k, anchor := range m.dirCursors {
		if k.ino == ino && k.fh == fh && anchor.sessionID == sessionID {
			delete(m.dirCursors, k)
		}
	}
}

func (m *manager) clearDirCursorsForInos(inos ...uint64) {
	if len(inos) == 0 {
		return
	}
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()

	inoSet := make(map[uint64]struct{}, len(inos))
	for _, ino := range inos {
		inoSet[ino] = struct{}{}
	}
	for _, r := range m.activeDirReads {
		if _, ok := inoSet[r.ino]; ok {
			r.invalidatedAll = true
		}
	}

	if len(m.dirCursors) == 0 && len(m.dirMutationSeq) == 0 {
		return
	}

	for k := range m.dirCursors {
		if _, ok := inoSet[k.ino]; ok {
			delete(m.dirCursors, k)
		}
	}
	if m.dirMutationSeq != nil {
		for _, ino := range inos {
			delete(m.dirMutationSeq, ino)
		}
	}
	if m.inFlightDirMutations != nil {
		for _, ino := range inos {
			delete(m.inFlightDirMutations, ino)
		}
	}
}

// beginDirMutation 递增指定目录节点的在途变更计数。
func (m *manager) beginDirMutation(ino uint64) {
	if ino == 0 {
		return
	}
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.inFlightDirMutations == nil {
		m.inFlightDirMutations = make(map[uint64]int)
	}
	m.inFlightDirMutations[ino]++
}

// endDirMutation 递减指定目录节点的在途变更计数，归零后删除条目。
func (m *manager) endDirMutation(ino uint64) {
	if ino == 0 {
		return
	}
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	if m.inFlightDirMutations == nil {
		return
	}
	m.inFlightDirMutations[ino]--
	if m.inFlightDirMutations[ino] <= 0 {
		delete(m.inFlightDirMutations, ino)
	}
}

// dirMutationTracker 追踪单次写操作中已登记的在途目录变更，防止重试重复递增并在操作退出时可靠归零。
type dirMutationTracker struct {
	m    *manager
	inos map[uint64]struct{}
}

func (m *manager) newDirMutationTracker() *dirMutationTracker {
	return &dirMutationTracker{m: m}
}

func (t *dirMutationTracker) track(inos ...uint64) {
	if t == nil || t.m == nil {
		return
	}
	for _, ino := range inos {
		if ino == 0 {
			continue
		}
		if t.inos == nil {
			t.inos = make(map[uint64]struct{})
		}
		if _, ok := t.inos[ino]; !ok {
			t.inos[ino] = struct{}{}
			t.m.beginDirMutation(ino)
		}
	}
}

func (t *dirMutationTracker) done() {
	if t == nil || t.m == nil {
		return
	}
	for ino := range t.inos {
		t.m.endDirMutation(ino)
	}
	t.inos = nil
}

// ManagerParams 定义 Manager 的 Fx 依赖注入参数。
type ManagerParams struct {
	fx.In

	Lifecycle  fx.Lifecycle `optional:"true"`
	Store      db.Store
	BlockStore BlockStore         `optional:"true"`
	MetaCfg    *config.MetaConfig `optional:"true"`
	MetaVal    config.MetaConfig  `optional:"true"`
}

func newManagerFromParams(params ManagerParams) Manager {
	var metaCfg *config.MetaConfig
	if params.MetaCfg != nil {
		metaCfg = params.MetaCfg
	} else if params.MetaVal != (config.MetaConfig{}) {
		metaCfg = &params.MetaVal
	}

	opts := make([]Option, 0, 10)

	// BlockStore 依赖解析：优先使用注入的 BlockStore，否则根据 MetaConfig.DataDir 构造默认 LocalFSBlockStore
	if params.BlockStore != nil {
		opts = append(opts, WithBlockStore(params.BlockStore))
	} else if metaCfg != nil && metaCfg.DataDir() != "" {
		opts = append(opts, WithBlockStore(NewLocalFSBlockStore(metaCfg.DataDir())))
	} else {
		opts = append(opts, WithBlockStore(NewLocalFSBlockStore(config.DefaultDataDir)))
	}

	if metaCfg != nil {
		var atimeMode AtimeMode
		switch metaCfg.AtimeMode() {
		case "relatime":
			atimeMode = AtimeModeRelAtime
		case "strictatime":
			atimeMode = AtimeModeStrictAtime
		default:
			atimeMode = AtimeModeNoAtime
		}

		opts = append(opts,
			WithSessionLeaseTTL(metaCfg.SessionLeaseTTL()),
			WithAtimeMode(atimeMode),
			WithEntryTimeout(metaCfg.EntryTimeoutSeconds()),
			WithAttrTimeout(metaCfg.AttrTimeoutSeconds()),
			WithDirEntryTimeout(metaCfg.DirEntryTimeoutSeconds()),
			WithOpenKeepCache(metaCfg.OpenKeepCache()),
			WithOpenDirCache(metaCfg.OpenDirCache()),
			WithGCInterval(metaCfg.GCInterval()),
		)
	}

	mgr := New(params.Store, opts...).(*manager)

	if params.Lifecycle != nil {
		var (
			gcCancel context.CancelFunc
			doneCh   <-chan struct{}
		)
		params.Lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				var gcCtx context.Context
				gcCtx, gcCancel = context.WithCancel(context.Background())
				doneCh = mgr.startGCWorkerWithDone(gcCtx, mgr.gcInterval, mgr.gcBatchLimit)
				return nil
			},
			OnStop: func(stopCtx context.Context) error {
				if gcCancel != nil {
					gcCancel()
				}
				if doneCh != nil {
					select {
					case <-doneCh:
						return nil
					case <-stopCtx.Done():
						return stopCtx.Err()
					}
				}
				return nil
			},
		})
	}

	return mgr
}

// WatchHub 返回会话流探活中心实例（懒初始化兜底，防止测试直接构造 &manager{} 时返回 nil）。
func (m *manager) WatchHub() *SessionWatchHub {
	m.watchHubMu.Lock()
	defer m.watchHubMu.Unlock()
	if m.watchHub == nil {
		m.watchHub = NewSessionWatchHub()
	}
	return m.watchHub
}

// Module 提供 meta 依赖注入模块。
var Module = fx.Module("meta",
	fx.Provide(newManagerFromParams),
)

func convertDBInodeToEntity(target db.Inode, entryTimeout, attrTimeout float64) *Inode {
	mode := uint32(target.Mode)
	fileType := mode & S_IFMT

	var size uint64
	var blocks uint64
	var rdev uint32

	switch fileType {
	case S_IFDIR:
		size = uint64(target.Size)
		if size == 0 {
			size = DefaultDirSize
		}
		blocks = (size + 511) / 512
	case S_IFREG, S_IFLNK:
		size = uint64(target.Size)
		if size > 0 {
			blocks = (size + 511) / 512
		}
	case S_IFCHR, S_IFBLK:
		size = 0
		blocks = 0
		rdev = uint32(target.Rdev)
	default:
		// FIFO, SOCK 等特殊文件
		size = 0
		blocks = 0
		rdev = 0
	}

	return &Inode{
		Ino:                 uint64(target.Ino),
		Generation:          uint64(target.Generation),
		EntryTimeoutSeconds: entryTimeout,
		AttrTimeoutSeconds:  attrTimeout,
		Attr: Attribute{
			Mode:      mode,
			Size:      size,
			Nlink:     uint32(target.Nlink),
			UID:       uint32(target.Uid),
			GID:       uint32(target.Gid),
			Rdev:      rdev,
			Blksize:   DefaultBlockSize,
			Blocks:    blocks,
			Atime:     target.Atime.Time,
			Mtime:     target.Mtime.Time,
			Ctime:     target.Ctime.Time,
			Flags:     uint32(target.Flags),
			ParentIno: uint64(target.ParentIno),
		},
	}
}

func (m *manager) newInode(target db.Inode) *Inode {
	entryTimeout := m.entryTimeout
	if (uint32(target.Mode) & S_IFMT) == S_IFDIR {
		entryTimeout = m.dirEntryTimeout
	}
	return convertDBInodeToEntity(target, entryTimeout, m.attrTimeout)
}

func (m *manager) newInodeFromListDirRow(row db.ListDirEntriesRow) *Inode {
	entryTimeout := m.entryTimeout
	if (uint32(row.Mode) & S_IFMT) == S_IFDIR {
		entryTimeout = m.dirEntryTimeout
	}
	return convertDBInodeToEntity(db.Inode{
		Ino:           row.Ino,
		Generation:    row.Generation,
		Mode:          row.Mode,
		Size:          row.Size,
		Nlink:         row.Nlink,
		Uid:           row.Uid,
		Gid:           row.Gid,
		Rdev:          row.Rdev,
		Atime:         row.Atime,
		Mtime:         row.Mtime,
		Ctime:         row.Ctime,
		SymlinkTarget: row.SymlinkTarget,
		Flags:         row.Flags,
		ParentIno:     row.ParentIno,
		RootIno:       row.RootIno,
	}, entryTimeout, m.attrTimeout)
}

func (m *manager) newInodeFromListDirAfterNameRow(row db.ListDirEntriesAfterNameRow) *Inode {
	entryTimeout := m.entryTimeout
	if (uint32(row.Mode) & S_IFMT) == S_IFDIR {
		entryTimeout = m.dirEntryTimeout
	}
	return convertDBInodeToEntity(db.Inode{
		Ino:           row.Ino,
		Generation:    row.Generation,
		Mode:          row.Mode,
		Size:          row.Size,
		Nlink:         row.Nlink,
		Uid:           row.Uid,
		Gid:           row.Gid,
		Rdev:          row.Rdev,
		Atime:         row.Atime,
		Mtime:         row.Mtime,
		Ctime:         row.Ctime,
		SymlinkTarget: row.SymlinkTarget,
		Flags:         row.Flags,
		ParentIno:     row.ParentIno,
		RootIno:       row.RootIno,
	}, entryTimeout, m.attrTimeout)
}

// errRetryTx 表示事务预读的目录项映射在加锁前发生并发漂移，需回滚并重试当前事务。
var errRetryTx = errors.New("retry transaction due to concurrent entry drift")

func isRetryableTxError(err error) bool {
	return tx.IsRetryable(err, errRetryTx)
}

func (m *manager) execWriteTxWithRetry(ctx context.Context, fn func(q db.Querier) error) error {
	var buffered *quotaBufferedQuerier
	err := tx.Retry(ctx, func(attemptCtx context.Context) error {
		if buffered != nil {
			buffered.rollback()
			buffered = nil
		}
		return m.store.ExecTxWithOptions(attemptCtx, func(q db.Querier) error {
			buffered = &quotaBufferedQuerier{
				Querier: q,
				m:       m,
			}
			return fn(buffered)
		})
	}, tx.WithDefaultRetry(errRetryTx))
	if err != nil {
		if buffered != nil {
			buffered.rollback()
		}
		return err
	}
	if buffered != nil {
		buffered.commit()
	}
	m.cleanExpiredDirCursors(ctx)
	return nil
}

// uniqueSortedInos 将一组 Inode 号按全局升序排列并去重，确保所有多 Inode 事务遵循单一全局加锁顺序。
func uniqueSortedInos(inos ...int64) []int64 {
	slices.Sort(inos)
	return slices.Compact(inos)
}

func lockAndValidateParents(ctx context.Context, q db.Querier, oldParentIno, newParentIno uint64) (db.Inode, db.Inode, error) {
	var oldParent, newParent db.Inode
	for _, ino := range uniqueSortedInos(int64(oldParentIno), int64(newParentIno)) {
		row, err := q.GetInodeForUpdate(ctx, ino)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return db.Inode{}, db.Inode{}, ErrNotFound
			}
			return db.Inode{}, db.Inode{}, fmt.Errorf("get parent inode %d for update: %w", ino, err)
		}
		if (uint32(row.Mode) & S_IFMT) != S_IFDIR {
			return db.Inode{}, db.Inode{}, ErrNotDir
		}
		if row.Nlink == 0 {
			return db.Inode{}, db.Inode{}, ErrNotFound
		}
		if ino == int64(oldParentIno) {
			oldParent = row
		}
		if ino == int64(newParentIno) {
			newParent = row
		}
	}
	return oldParent, newParent, nil
}

func validateOptionalSessionID(sessionID *uint64) error {
	if sessionID != nil && (*sessionID == 0 || *sessionID > OffsetMax) {
		return ErrInvalidArgument
	}
	return nil
}

func toPgSessionID(sessionID *uint64) pgtype.Int8 {
	if sessionID == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{
		Int64: int64(*sessionID),
		Valid: true,
	}
}

// renewOptionalClientSession 若传入了有效的客户端会话标识，则在当前事务中按内存时间窗口节流执行会话租约续期并清理相关孤儿节点。
func (m *manager) renewOptionalClientSession(ctx context.Context, q db.Querier, sessionID *uint64) error {
	if sessionID == nil {
		return nil
	}
	sid := *sessionID
	if pp, throttle := m.shouldThrottleSessionTouch(ctx); throttle && (m.writeSessionTouchConfigured || isNativeTxQuerier(q)) {
		interval := m.writeSessionTouchInterval
		if interval <= 0 {
			interval = DefaultWriteSessionTouchInterval
		}
		now := time.Now()
		m.writeSessionTouchMu.Lock()
		if m.writeSessionTouches == nil {
			m.writeSessionTouches = make(map[uint64]writeSessionTouchState)
		}
		prev, hadPrev := m.writeSessionTouches[sid]
		inWindow := interval > 0 && hadPrev && !prev.lastTouchedAt.IsZero() && now.Sub(prev.lastTouchedAt) < interval
		m.writeSessionTouchMu.Unlock()

		if inWindow {
			if !m.hasDeferredOrphans() {
				return nil
			}
			dbtx := queryDBTX(q, pp)
			if dbtx == nil {
				return nil
			}
			var sessionCreatedAt pgtype.Timestamptz
			var alive, hasPendingCleanup bool
			err := dbtx.QueryRow(ctx, `
				SELECT
					cs.created_at,
					(cs.expires_at > clock_timestamp()) AS alive,
					(
						EXISTS (SELECT 1 FROM client_sessions e WHERE e.expires_at <= clock_timestamp())
						OR EXISTS (
							SELECT 1 FROM inodes i
							WHERE i.nlink = 0
							  AND NOT EXISTS (SELECT 1 FROM file_handles fh WHERE fh.ino = i.ino)
						)
					) AS has_pending_cleanup
				FROM client_sessions cs
				WHERE cs.session_id = $1
			`, int64(sid)).Scan(&sessionCreatedAt, &alive, &hasPendingCleanup)
			if err == nil && alive && !hasPendingCleanup {
				m.recordWriteSessionTouch(sid, time.Time{}, sessionCreatedAt)
				return nil
			}
			if err != nil || !alive {
				m.clearWriteSessionTouch(sid)
			}
		}
	}
	_, err := m.renewClientSession(ctx, q, sid)
	return err
}

// GetParents 返回指定 Inode 挂载在各个父目录下的目录项数量统计。
func (m *manager) GetParents(ctx context.Context, ino uint64) (map[uint64]uint32, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	var result map[uint64]uint32
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		rows, err := q.ListParentEntryCountsByIno(ctx, int64(ino))
		if err != nil {
			return fmt.Errorf("list parent entry counts for ino %d: %w", ino, err)
		}
		result = make(map[uint64]uint32, len(rows))
		for _, row := range rows {
			result[uint64(row.ParentIno)] = uint32(row.EntryCount)
		}
		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RenewSessionWithStatus 续期客户端会话并返回详细状态（包括是否为新创建或过期重建）。
func (m *manager) RenewSessionWithStatus(ctx context.Context, sessionID uint64) (RenewSessionResult, error) {
	return m.RenewSessionWithDisk(ctx, sessionID, nil)
}

// RenewSessionWithDisk 续期客户端会话并可选绑定指定虚拟磁盘，返回详细状态（包括是否为新创建或过期重建）。
func (m *manager) RenewSessionWithDisk(ctx context.Context, sessionID uint64, diskID *string) (RenewSessionResult, error) {
	if sessionID == 0 || sessionID > math.MaxInt64 {
		return RenewSessionResult{}, ErrInvalidArgument
	}
	var pgDiskID pgtype.UUID
	if diskID != nil {
		parsed, err := parseDiskID(*diskID)
		if err != nil {
			return RenewSessionResult{}, err
		}
		pgDiskID = parsed
	}
	var res RenewSessionResult
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		leaseTTL := m.sessionLeaseTTL
		if leaseTTL <= 0 {
			leaseTTL = DefaultSessionLeaseTTL
		}

		sid := int64(sessionID)
		deletedCount, err := q.DeleteExpiredClientSessions(ctx)
		if err != nil {
			return fmt.Errorf("delete expired client sessions: %w", err)
		}
		if deletedCount > 0 {
			m.notifyAllLockWaiters()
		}

		session, err := q.UpsertClientSession(db.WithLeaseTTL(ctx, leaseTTL), db.UpsertClientSessionParams{
			SessionID: sid,
			ExpiresAt: pgtype.Timestamptz{
				Time:  time.Now().Add(leaseTTL),
				Valid: true,
			},
			DiskID: pgDiskID,
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23503" {
				return ErrNotFound
			}
			return fmt.Errorf("upsert client session %d: %w", sessionID, err)
		}
		if err := m.reclaimUnreferencedOrphans(ctx, q); err != nil {
			return err
		}
		m.recordWriteSessionTouch(sessionID, time.Now(), session.CreatedAt)
		res.ExpiresAt = session.ExpiresAt.Time
		res.SessionRecreated = session.CreatedAt.Time.Equal(session.UpdatedAt.Time)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrInvalidArgument) {
			return RenewSessionResult{}, ErrInvalidArgument
		}
		if errors.Is(err, ErrNotFound) {
			return RenewSessionResult{}, ErrNotFound
		}
		return RenewSessionResult{}, fmt.Errorf("renew session with status tx: %w", err)
	}
	return res, nil
}

var errSessionHasActiveStream = errors.New("meta: session regained active watch stream before close")

// CloseSession 立即注销指定客户端会话租约，级联释放关联的文件句柄、文件锁并回收孤儿节点。
func (m *manager) CloseSession(ctx context.Context, sessionID uint64) error {
	if streamID, ok := disconnectedStreamIDFromContext(ctx); ok {
		_, err := m.closeSessionAfterStream(ctx, sessionID, true, streamID)
		return err
	}
	return m.closeSession(ctx, sessionID, false)
}

func (m *manager) closeSessionIfNoActiveStream(ctx context.Context, sessionID uint64, evictedStreamID uint64) (abortedByActiveStream bool, err error) {
	return m.closeSessionAfterStream(ctx, sessionID, true, evictedStreamID)
}

func (m *manager) closeSession(ctx context.Context, sessionID uint64, abortIfActiveStream bool) error {
	_, err := m.closeSessionAfterStream(ctx, sessionID, abortIfActiveStream, 0)
	return err
}

func (m *manager) closeSessionAfterStream(ctx context.Context, sessionID uint64, abortIfActiveStream bool, expectedMaxStreamID uint64) (abortedByActiveStream bool, err error) {
	if sessionID == 0 || sessionID > math.MaxInt64 {
		return false, ErrInvalidArgument
	}

	var (
		shouldNotify   bool
		reclaimedInos  []int64
		autoCloseGate  func()
		releaseGateNow = func() {
			if autoCloseGate != nil {
				autoCloseGate()
				autoCloseGate = nil
			}
		}
	)
	defer func() {
		releaseGateNow()
	}()

	var buffered *quotaBufferedQuerier
	err = tx.Retry(ctx, func(attemptCtx context.Context) error {
		defer releaseGateNow()
		if buffered != nil {
			buffered.rollback()
			buffered = nil
		}
		return m.store.ExecTxWithOptions(attemptCtx, func(rawQ db.Querier) error {
			buffered = &quotaBufferedQuerier{
				Querier: rawQ,
				m:       m,
			}
			q := buffered
			shouldNotify = false
			reclaimedInos = nil
			expiredCount, err := q.DeleteExpiredClientSessions(ctx)
			if err != nil {
				return fmt.Errorf("delete expired client sessions: %w", err)
			}
			deletedCount, err := q.DeleteClientSessionByID(ctx, int64(sessionID))
			if err != nil {
				return fmt.Errorf("delete client session %d: %w", sessionID, err)
			}
			if abortIfActiveStream && m.WatchHub().HasActiveStream(sessionID) {
				return errSessionHasActiveStream
			}
			if expiredCount > 0 || deletedCount > 0 {
				shouldNotify = true
			}
			reclaimedInos, err = m.reclaimUnreferencedOrphansInTx(ctx, q)
			if err != nil {
				return err
			}
			if abortIfActiveStream {
				allowed, unlock := m.WatchHub().AcquireAutoCloseGate(sessionID, expectedMaxStreamID)
				if !allowed {
					return errSessionHasActiveStream
				}
				autoCloseGate = unlock
			}
			return nil
		})
	}, tx.WithDefaultRetry(errRetryTx))
	releaseGateNow()

	if err != nil {
		if buffered != nil {
			buffered.rollback()
		}
		if errors.Is(err, errSessionHasActiveStream) {
			return true, nil
		}
		if errors.Is(err, ErrInvalidArgument) {
			return false, ErrInvalidArgument
		}
		return false, fmt.Errorf("close session tx: %w", err)
	}

	if buffered != nil {
		buffered.commit()
	}
	m.finalizeReclaimedOrphans(reclaimedInos)
	m.cleanExpiredDirCursors(ctx)
	if shouldNotify {
		m.notifyAllLockWaiters()
	}
	m.invalidateDirCursorsBySession(sessionID)
	m.clearWriteSessionTouch(sessionID)
	return false, nil
}

func (m *manager) invalidateDirCursorsBySession(sessionID uint64) {
	if sessionID == 0 {
		return
	}
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	for _, r := range m.activeDirReads {
		if r.invalidatedSessions == nil {
			r.invalidatedSessions = make(map[uint64]struct{})
		}
		r.invalidatedSessions[sessionID] = struct{}{}
	}
	for k, anchor := range m.dirCursors {
		if anchor.sessionID == sessionID {
			delete(m.dirCursors, k)
		}
	}
}

// reclaimUnreferencedOrphans 批量清理无句柄引用的超期孤儿节点，并按 root_ino 汇总退还磁盘配额。
func (m *manager) reclaimUnreferencedOrphans(ctx context.Context, q db.Querier) error {
	deletedInos, err := m.reclaimUnreferencedOrphansInTx(ctx, q)
	if err != nil {
		return err
	}
	m.finalizeReclaimedOrphans(deletedInos)
	return nil
}

func (m *manager) reclaimUnreferencedOrphansInTx(ctx context.Context, q db.Querier) ([]int64, error) {
	deletedOrphans, err := q.DeleteUnreferencedOrphanInodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("delete unreferenced orphan inodes: %w", err)
	}
	if len(deletedOrphans) == 0 {
		return nil, nil
	}

	type quotaDelta struct {
		deltaBytes  int64
		deltaInodes int64
	}
	quotaDeltas := make(map[int64]*quotaDelta)
	deletedInos := make([]int64, 0, len(deletedOrphans))

	for _, row := range deletedOrphans {
		deletedInos = append(deletedInos, row.Ino)
		if row.RootIno <= 0 {
			continue
		}
		agg, ok := quotaDeltas[row.RootIno]
		if !ok {
			agg = &quotaDelta{}
			quotaDeltas[row.RootIno] = agg
		}
		agg.deltaInodes--
		if (uint32(row.Mode)&S_IFMT) == S_IFREG && row.Size > 0 {
			agg.deltaBytes -= row.Size
		}
	}

	if len(deletedInos) > 0 {
		if err := deleteAllChunksByInos(ctx, q, deletedInos); err != nil {
			return nil, fmt.Errorf("delete chunks for unreferenced orphan inodes: %w", err)
		}
	}

	rootInos := make([]int64, 0, len(quotaDeltas))
	for rootIno := range quotaDeltas {
		rootInos = append(rootInos, rootIno)
	}
	slices.Sort(rootInos)

	for _, rootIno := range rootInos {
		agg := quotaDeltas[rootIno]
		if err := m.recordQuotaDelta(ctx, q, rootIno, agg.deltaBytes, agg.deltaInodes); err != nil {
			return nil, fmt.Errorf("reclaim disk quota for root_ino %d: %w", rootIno, err)
		}
	}

	return deletedInos, nil
}

func (m *manager) finalizeReclaimedOrphans(deletedInos []int64) {
	if len(deletedInos) == 0 {
		return
	}
	m.unmarkDeferredOrphan(deletedInos...)
	m.cleanLockWaitersForInos(deletedInos)
	inos := make([]uint64, len(deletedInos))
	for i, ino := range deletedInos {
		inos[i] = uint64(ino)
	}
	m.clearDirCursorsForInos(inos...)
}

// reclaimOrphanIfNoHandles 检查孤儿节点是否存在未释放句柄，若无任何句柄则执行物理删除并唤醒清理锁等待通道。
// 若节点所属 RootIno > 0，在物理删除后联动退还 used_bytes 与 used_inodes 配额。
func (m *manager) reclaimOrphanIfNoHandles(ctx context.Context, q db.Querier, inode db.Inode) error {
	hasRemaining, err := q.HasFileHandlesByIno(ctx, inode.Ino)
	if err != nil {
		return fmt.Errorf("check remaining file handles for inode %d: %w", inode.Ino, err)
	}
	if hasRemaining {
		m.markDeferredOrphan(inode.Ino)
		return nil
	}

	// 若为常规文件，在同一数据库事务内物理删除其全部 chunks 行并批量扣减关联物理切片的引用计数
	if (uint32(inode.Mode) & S_IFMT) == S_IFREG {
		if err := deleteAllChunksByIno(ctx, q, inode.Ino); err != nil {
			return err
		}
	}

	if err := q.DeleteInode(ctx, inode.Ino); err != nil {
		return fmt.Errorf("delete orphan inode %d: %w", inode.Ino, err)
	}
	m.unmarkDeferredOrphan(inode.Ino)
	m.notifyLockWaiters(uint64(inode.Ino))
	m.clearDirCursorsForInos(uint64(inode.Ino))

	if inode.RootIno > 0 {
		var deltaBytes int64
		if (uint32(inode.Mode)&S_IFMT) == S_IFREG && inode.Size > 0 {
			deltaBytes = -inode.Size
		}
		if err := m.recordQuotaDelta(ctx, q, inode.RootIno, deltaBytes, -1); err != nil {
			return fmt.Errorf("reclaim orphan disk quota for root_ino %d: %w", inode.RootIno, err)
		}
	}
	return nil
}

// shouldTouchAtime 根据当前配置的 AtimeMode 判断是否需要刷新节点的访问时间 atime。
func (m *manager) shouldTouchAtime(atime, mtime, ctime, now time.Time) bool {
	switch m.atimeMode {
	case AtimeModeStrictAtime:
		return now.Sub(atime) >= time.Second
	case AtimeModeRelAtime:
		return atime.Before(mtime) || atime.Before(ctime) || now.Sub(atime) >= 24*time.Hour
	default:
		return false
	}
}

// isClientSessionAlive 检查指定会话在真实数据库中是否存在且未过期。
func (m *manager) isClientSessionAlive(ctx context.Context, sessionID uint64) bool {
	type poolProvider interface {
		Pool() *pgxpool.Pool
	}
	pp, ok := m.store.(poolProvider)
	if !ok || pp == nil || pp.Pool() == nil {
		return true // Mock 环境默认兜底视为存活
	}
	var alive bool
	err := pp.Pool().QueryRow(ctx, `SELECT expires_at > clock_timestamp() FROM client_sessions WHERE session_id = $1`, int64(sessionID)).Scan(&alive)
	if err != nil {
		return false
	}
	return alive
}

// cleanExpiredDirCursors 检查并清理关联会话已过期、不存在或已被重建，以及句柄已不存在的目录游标缓存。
func (m *manager) cleanExpiredDirCursors(ctx context.Context) {
	type poolProvider interface {
		Pool() *pgxpool.Pool
	}
	pp, ok := m.store.(poolProvider)
	if !ok || pp == nil || pp.Pool() == nil {
		return
	}

	m.dirCursorMu.Lock()
	if len(m.dirCursors) == 0 {
		m.dirCursorMu.Unlock()
		return
	}

	type handleInfo struct {
		sessionID uint64
	}

	handleMap := make(map[dirHandleKey]handleInfo)
	sessionIDsMap := make(map[uint64]struct{})
	for k, anchor := range m.dirCursors {
		handleMap[dirHandleKey{ino: k.ino, fh: k.fh}] = handleInfo{sessionID: anchor.sessionID}
		if anchor.sessionID > 0 {
			sessionIDsMap[anchor.sessionID] = struct{}{}
		}
	}
	m.dirCursorMu.Unlock()

	type sessionStatus struct {
		createdAt time.Time
		alive     bool
	}
	statuses := make(map[uint64]sessionStatus, len(sessionIDsMap))
	if len(sessionIDsMap) > 0 {
		sessionIDs := make([]int64, 0, len(sessionIDsMap))
		for sid := range sessionIDsMap {
			sessionIDs = append(sessionIDs, int64(sid))
		}

		rows, err := pp.Pool().Query(ctx, `
			SELECT session_id, created_at, (expires_at > clock_timestamp()) AS alive
			FROM client_sessions
			WHERE session_id = ANY($1)
		`, sessionIDs)
		if err != nil {
			return
		}

		for rows.Next() {
			var sid int64
			var createdAt time.Time
			var alive bool
			if err := rows.Scan(&sid, &createdAt, &alive); err != nil {
				rows.Close()
				return
			}
			statuses[uint64(sid)] = sessionStatus{
				createdAt: createdAt,
				alive:     alive,
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return
		}
	}

	deadHandles := make(map[dirHandleKey]struct{})
	for h, info := range handleMap {
		var sidPtr *uint64
		if info.sessionID > 0 {
			sidPtr = &info.sessionID
		}
		_, err := m.store.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:        int64(h.fh),
			Ino:       int64(h.ino),
			SessionID: toPgSessionID(sidPtr),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrBadFileHandle) {
				deadHandles[h] = struct{}{}
			}
		}
	}

	expiredSessions := make(map[uint64]struct{})
	for sid := range sessionIDsMap {
		st, ok := statuses[sid]
		if !ok || !st.alive {
			expiredSessions[sid] = struct{}{}
		}
	}

	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	for _, r := range m.activeDirReads {
		hk := dirHandleKey{ino: r.ino, fh: r.fh}
		if _, isDead := deadHandles[hk]; isDead {
			r.invalidatedAll = true
			continue
		}
		for sid := range expiredSessions {
			if r.invalidatedSessions == nil {
				r.invalidatedSessions = make(map[uint64]struct{})
			}
			r.invalidatedSessions[sid] = struct{}{}
		}
	}
	for k, anchor := range m.dirCursors {
		hk := dirHandleKey{ino: k.ino, fh: k.fh}
		if _, isDead := deadHandles[hk]; isDead {
			delete(m.dirCursors, k)
			continue
		}
		if anchor.sessionID > 0 {
			if _, queried := sessionIDsMap[anchor.sessionID]; !queried {
				continue
			}
			st, ok := statuses[anchor.sessionID]
			// 若 session 在表中不存在，或已过期 (alive == false)，或已被重建 (st.createdAt > anchor.handleCreatedAt)
			if !ok || !st.alive || (!anchor.handleCreatedAt.IsZero() && st.createdAt.After(anchor.handleCreatedAt)) {
				delete(m.dirCursors, k)
			}
		}
	}
}
