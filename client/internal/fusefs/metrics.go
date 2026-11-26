package fusefs

import (
	"encoding/json"
	"fmt"
	"math/bits"
	rmetrics "runtime/metrics"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
)

type FuseOp uint8

const (
	OpLookup FuseOp = iota
	OpGetAttr
	OpStatFs
	OpMkdir
	OpMknod
	OpRmdir
	OpUnlink
	OpRename
	OpSymlink
	OpReadlink
	OpLink
	OpCreate
	OpOpen
	OpFlush
	OpFsync
	OpRelease
	OpOpenDir
	OpReadDir
	OpReadDirPlus
	OpFsyncDir
	OpRead
	OpWrite
	OpSetAttr
	OpFallocate
	OpSetXAttr
	OpGetXAttr
	OpListXAttr
	OpRemoveXAttr
	OpIoctl
	OpOnUnmount
	OpReleaseDir
	OpTmpfile
	OpForget
	OpAccess
	OpLseek
	OpWritev
	OpCopyFileRange
	OpStatx

	OpCount = 38
)

const (
	opHistBuckets = 26

	errnoBucketCount = 24

	waitHistBuckets = 18
)

var fuseOpNames = [OpCount]string{
	OpLookup:        "Lookup",
	OpGetAttr:       "GetAttr",
	OpStatFs:        "StatFs",
	OpMkdir:         "Mkdir",
	OpMknod:         "Mknod",
	OpRmdir:         "Rmdir",
	OpUnlink:        "Unlink",
	OpRename:        "Rename",
	OpSymlink:       "Symlink",
	OpReadlink:      "Readlink",
	OpLink:          "Link",
	OpCreate:        "Create",
	OpOpen:          "Open",
	OpFlush:         "Flush",
	OpFsync:         "Fsync",
	OpRelease:       "Release",
	OpOpenDir:       "OpenDir",
	OpReadDir:       "ReadDir",
	OpReadDirPlus:   "ReadDirPlus",
	OpFsyncDir:      "FsyncDir",
	OpRead:          "Read",
	OpWrite:         "Write",
	OpSetAttr:       "SetAttr",
	OpFallocate:     "Fallocate",
	OpSetXAttr:      "SetXAttr",
	OpGetXAttr:      "GetXAttr",
	OpListXAttr:     "ListXAttr",
	OpRemoveXAttr:   "RemoveXAttr",
	OpIoctl:         "Ioctl",
	OpOnUnmount:     "OnUnmount",
	OpReleaseDir:    "ReleaseDir",
	OpTmpfile:       "Tmpfile",
	OpForget:        "Forget",
	OpAccess:        "Access",
	OpLseek:         "Lseek",
	OpWritev:        "Writev",
	OpCopyFileRange: "CopyFileRange",
	OpStatx:         "Statx",
}

func (op FuseOp) String() string {
	if int(op) < len(fuseOpNames) {
		return fuseOpNames[op]
	}
	return "Unknown"
}

const (
	errnoIdxENOENT = iota
	errnoIdxENODATA
	errnoIdxEEXIST
	errnoIdxEINVAL
	errnoIdxEPERM
	errnoIdxEACCES
	errnoIdxENOTDIR
	errnoIdxEISDIR
	errnoIdxENOTEMPTY
	errnoIdxEBADF
	errnoIdxENOSPC
	errnoIdxEAGAIN
	errnoIdxEDEADLK
	errnoIdxEINTR
	errnoIdxEOPNOTSUPP
	errnoIdxENOTTY
	errnoIdxESTALE
	errnoIdxEROFS
	errnoIdxEBUSY
	errnoIdxEIO
	errnoIdxERANGE
	errnoIdxENAMETOOLONG
	errnoIdxEFBIG
	errnoIdxOther
)

var errnoBucketNames = [errnoBucketCount]string{
	errnoIdxENOENT:       "ENOENT",
	errnoIdxENODATA:      "ENODATA",
	errnoIdxEEXIST:       "EEXIST",
	errnoIdxEINVAL:       "EINVAL",
	errnoIdxEPERM:        "EPERM",
	errnoIdxEACCES:       "EACCES",
	errnoIdxENOTDIR:      "ENOTDIR",
	errnoIdxEISDIR:       "EISDIR",
	errnoIdxENOTEMPTY:    "ENOTEMPTY",
	errnoIdxEBADF:        "EBADF",
	errnoIdxENOSPC:       "ENOSPC",
	errnoIdxEAGAIN:       "EAGAIN",
	errnoIdxEDEADLK:      "EDEADLK",
	errnoIdxEINTR:        "EINTR",
	errnoIdxEOPNOTSUPP:   "EOPNOTSUPP",
	errnoIdxENOTTY:       "ENOTTY",
	errnoIdxESTALE:       "ESTALE",
	errnoIdxEROFS:        "EROFS",
	errnoIdxEBUSY:        "EBUSY",
	errnoIdxEIO:          "EIO",
	errnoIdxERANGE:       "ERANGE",
	errnoIdxENAMETOOLONG: "ENAMETOOLONG",
	errnoIdxEFBIG:        "EFBIG",
	errnoIdxOther:        "OTHER",
}

var (
	errnoLookupTable [256]uint8
	opHistUpperSec   [opHistBuckets]float64

	waitHistUpperSec = [waitHistBuckets]float64{
		0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
		0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
	}
	waitHistUpperNanos = [waitHistBuckets]int64{
		25_000, 50_000, 100_000, 250_000, 500_000,
		1_000_000, 2_500_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000,
		100_000_000, 250_000_000, 500_000_000, 1_000_000_000, 2_500_000_000, 5_000_000_000, 10_000_000_000,
	}
)

func init() {
	for i := range errnoLookupTable {
		errnoLookupTable[i] = errnoIdxOther
	}
	registerErrno := func(errno syscall.Errno, idx uint8) {
		if int(errno) > 0 && int(errno) < len(errnoLookupTable) {
			errnoLookupTable[int(errno)] = idx
		}
	}
	registerErrno(syscall.ENOENT, errnoIdxENOENT)
	registerErrno(syscall.ENODATA, errnoIdxENODATA)
	registerErrno(syscall.EEXIST, errnoIdxEEXIST)
	registerErrno(syscall.EINVAL, errnoIdxEINVAL)
	registerErrno(syscall.EPERM, errnoIdxEPERM)
	registerErrno(syscall.EACCES, errnoIdxEACCES)
	registerErrno(syscall.ENOTDIR, errnoIdxENOTDIR)
	registerErrno(syscall.EISDIR, errnoIdxEISDIR)
	registerErrno(syscall.ENOTEMPTY, errnoIdxENOTEMPTY)
	registerErrno(syscall.EBADF, errnoIdxEBADF)
	registerErrno(syscall.ENOSPC, errnoIdxENOSPC)
	registerErrno(syscall.EAGAIN, errnoIdxEAGAIN)
	registerErrno(syscall.EDEADLK, errnoIdxEDEADLK)
	registerErrno(syscall.EINTR, errnoIdxEINTR)
	registerErrno(syscall.EOPNOTSUPP, errnoIdxEOPNOTSUPP)
	registerErrno(syscall.ENOTTY, errnoIdxENOTTY)
	registerErrno(syscall.ESTALE, errnoIdxESTALE)
	registerErrno(syscall.EROFS, errnoIdxEROFS)
	registerErrno(syscall.EBUSY, errnoIdxEBUSY)
	registerErrno(syscall.EIO, errnoIdxEIO)
	registerErrno(syscall.ERANGE, errnoIdxERANGE)
	registerErrno(syscall.ENAMETOOLONG, errnoIdxENAMETOOLONG)
	registerErrno(syscall.EFBIG, errnoIdxEFBIG)

	for i := range opHistBuckets {
		opHistUpperSec[i] = float64(uint64(1)<<i) * 1e-6
	}
}

func errnoIndex(status fuse.Status) uint8 {
	code := int(status)
	if code < 0 {
		code = -code
	}
	if code <= 0 || code >= len(errnoLookupTable) {
		return errnoIdxOther
	}
	return errnoLookupTable[code]
}

func errnoName(status fuse.Status) string {
	return errnoBucketNames[errnoIndex(status)]
}

type opSlot struct {
	inflight atomic.Int64
	calls    atomic.Uint64
	errors   atomic.Uint64
	bytes    atomic.Uint64
	latNanos atomic.Uint64
	buckets  [opHistBuckets]atomic.Uint64
	_        [8]byte
}

var _ [0]struct{} = [unsafe.Sizeof(opSlot{}) % 64]struct{}{}

type waitHist struct {
	count    atomic.Uint64
	sumNanos atomic.Uint64
	buckets  [waitHistBuckets]atomic.Uint64
}

func (h *waitHist) observe(d time.Duration) {
	ns := max(int64(0), d.Nanoseconds())
	for i, bound := range waitHistUpperNanos {
		if ns <= bound {
			h.buckets[i].Add(1)
			break
		}
	}
	if ns > 0 {
		h.sumNanos.Add(uint64(ns))
	}
	h.count.Add(1)
}

func (h *waitHist) constHistogram(desc *prometheus.Desc) prometheus.Metric {
	buckets := make(map[float64]uint64, waitHistBuckets)
	var cum uint64
	for i := range waitHistBuckets {
		cum += h.buckets[i].Load()
		buckets[waitHistUpperSec[i]] = cum
	}
	cnt := max(h.count.Load(), cum)
	sumSec := float64(h.sumNanos.Load()) * 1e-9
	return prometheus.MustNewConstHistogram(desc, cnt, sumSec, buckets)
}

type nsCacheKind uint8

const (
	nsCacheAttr nsCacheKind = iota
	nsCacheEntry
	nsCacheNegativeEntry
	nsCacheReadDir
	nsCacheReadlink
	nsCacheKindCount
)

var nsCacheKindNames = [nsCacheKindCount]string{
	nsCacheAttr:          "attr",
	nsCacheEntry:         "entry",
	nsCacheNegativeEntry: "negative_entry",
	nsCacheReadDir:       "readdir",
	nsCacheReadlink:      "readlink",
}

type stateSnapshotProvider interface {
	collectSubsystemGauges() subsystemGauges
}

type subsystemGauges struct {
	nsEntries               int
	nsUsedWeight            int
	nsCapacity              int
	capEntries              int
	tmpActiveFiles          int
	tmpResidentByte         uint64
	openFiles               int
	openDirs                int
	inodeLockActive         int
	orphanInodes            int
	releaseQueue            int
	stagePendingChunks      int64
	stageUsedBytes          uint64
	stageCapacityBytes      uint64
	fuseWaiting             int
	fuseCongestionThreshold int
}

type Metrics struct {
	ops        [OpCount]opSlot
	errnoTable [OpCount][errnoBucketCount]atomic.Uint64

	nsHits          [nsCacheKindCount]atomic.Uint64
	nsMisses        [nsCacheKindCount]atomic.Uint64
	nsInvalidations atomic.Uint64
	nsEvictions     atomic.Uint64

	capHits               atomic.Uint64
	capMisses             atomic.Uint64
	capGrants             atomic.Uint64
	capEpochInvalidations atomic.Uint64

	readAheadTriggers        atomic.Uint64
	readAheadPrefetchedBytes atomic.Uint64
	readAheadHits            atomic.Uint64
	readAheadHitBytes        atomic.Uint64
	readAheadDiscardedBytes  atomic.Uint64
	readAheadInflight        atomic.Int64
	readAheadWaitHist        waitHist

	stageStagedCalls          atomic.Uint64
	stageStagedBytes          atomic.Uint64
	stagePassthroughCalls     atomic.Uint64
	stagePassthroughBytes     atomic.Uint64
	stageUploadRPCOK          atomic.Uint64
	stageUploadRPCErrors      atomic.Uint64
	stageInflight             atomic.Int64
	stageBackpressureWaitHist waitHist
	stageBarrierWaitHist      waitHist

	tmpPromotes      atomic.Uint64
	tmpResidentBytes atomic.Int64

	inodeLockFast      atomic.Uint64
	inodeLockContended atomic.Uint64
	inodeLockWaitNanos atomic.Uint64
	inodeLockWaitHist  waitHist

	dirtyFlushOK        atomic.Uint64
	dirtyFlushErrors    atomic.Uint64
	orphanReclaimOK     atomic.Uint64
	orphanReclaimErrors atomic.Uint64
	asyncReleaseOK      atomic.Uint64
	asyncReleaseErrors  atomic.Uint64

	startedAtUnixNano int64
	provider          atomic.Pointer[stateSnapshotProvider]

	fuseOpDurationDesc *prometheus.Desc
	fuseOpsTotalDesc   *prometheus.Desc
	fuseInflightDesc   *prometheus.Desc
	fuseOpErrorsDesc   *prometheus.Desc
	fuseIOBytesDesc    *prometheus.Desc

	nsHitsDesc          *prometheus.Desc
	nsMissesDesc        *prometheus.Desc
	nsInvalidationsDesc *prometheus.Desc
	nsEvictionsDesc     *prometheus.Desc
	nsEntriesDesc       *prometheus.Desc
	nsWeightDesc        *prometheus.Desc

	capHitsDesc               *prometheus.Desc
	capMissesDesc             *prometheus.Desc
	capGrantsDesc             *prometheus.Desc
	capEpochInvalidationsDesc *prometheus.Desc
	capEntriesDesc            *prometheus.Desc

	raTriggersDesc     *prometheus.Desc
	raPrefetchedDesc   *prometheus.Desc
	raHitBytesDesc     *prometheus.Desc
	raDiscardedDesc    *prometheus.Desc
	raInflightDesc     *prometheus.Desc
	raWaitDurationDesc *prometheus.Desc

	stageWritesTotalDesc      *prometheus.Desc
	stageBytesTotalDesc       *prometheus.Desc
	stageUploadRPCsTotalDesc  *prometheus.Desc
	stageInflightDesc         *prometheus.Desc
	stagePendingChunksDesc    *prometheus.Desc
	stageUsedBytesDesc        *prometheus.Desc
	stageCapacityBytesDesc    *prometheus.Desc
	stageBackpressureWaitDesc *prometheus.Desc
	stageBarrierWaitDesc      *prometheus.Desc

	tmpActiveFilesDesc   *prometheus.Desc
	tmpResidentBytesDesc *prometheus.Desc
	tmpPromotesDesc      *prometheus.Desc

	openHandlesDesc *prometheus.Desc

	inodeLockAcqDesc          *prometheus.Desc
	inodeLockWaitSecDesc      *prometheus.Desc
	inodeLockWaitDurationDesc *prometheus.Desc
	inodeLockActiveDesc       *prometheus.Desc

	queueDepthDesc             *prometheus.Desc
	queueOpsDesc               *prometheus.Desc
	fuseConnWaitingDesc        *prometheus.Desc
	fuseConnCongestionThrsDesc *prometheus.Desc
}

var _ prometheus.Collector = (*Metrics)(nil)

func NewMetrics() *Metrics {
	return &Metrics{
		startedAtUnixNano: time.Now().UnixNano(),

		fuseOpDurationDesc: prometheus.NewDesc(
			"bowl_client_fuse_op_duration_seconds",
			"Latency distribution of client FUSE operations in seconds.",
			[]string{"op"},
			nil,
		),
		fuseOpsTotalDesc: prometheus.NewDesc(
			"bowl_client_fuse_ops_total",
			"Total number of completed client FUSE operations.",
			[]string{"op"},
			nil,
		),
		fuseInflightDesc: prometheus.NewDesc(
			"bowl_client_fuse_inflight_ops",
			"Current number of in-flight client FUSE operations.",
			[]string{"op"},
			nil,
		),
		fuseOpErrorsDesc: prometheus.NewDesc(
			"bowl_client_fuse_op_errors_total",
			"Total number of non-OK client FUSE operation responses by POSIX errno.",
			[]string{"op", "errno"},
			nil,
		),
		fuseIOBytesDesc: prometheus.NewDesc(
			"bowl_client_fuse_io_bytes_total",
			"Total payload bytes transferred through FUSE Read and Write operations.",
			[]string{"dir"},
			nil,
		),

		nsHitsDesc: prometheus.NewDesc(
			"bowl_client_nscache_hits_total",
			"Total number of client NamespaceCache hits by lookup kind.",
			[]string{"kind"},
			nil,
		),
		nsMissesDesc: prometheus.NewDesc(
			"bowl_client_nscache_misses_total",
			"Total number of client NamespaceCache misses by lookup kind.",
			[]string{"kind"},
			nil,
		),
		nsInvalidationsDesc: prometheus.NewDesc(
			"bowl_client_nscache_invalidations_total",
			"Total number of client NamespaceCache invalidation operations.",
			nil,
			nil,
		),
		nsEvictionsDesc: prometheus.NewDesc(
			"bowl_client_nscache_evictions_total",
			"Total number of client NamespaceCache LRU capacity evictions.",
			nil,
			nil,
		),
		nsEntriesDesc: prometheus.NewDesc(
			"bowl_client_nscache_entries",
			"Current number of cached items in client NamespaceCache.",
			nil,
			nil,
		),
		nsWeightDesc: prometheus.NewDesc(
			"bowl_client_nscache_weight",
			"Current weighted usage and capacity of client NamespaceCache.",
			[]string{"state"},
			nil,
		),

		capHitsDesc: prometheus.NewDesc(
			"bowl_client_capcache_hits_total",
			"Total number of security.capability negative-cache short-circuit hits.",
			nil,
			nil,
		),
		capMissesDesc: prometheus.NewDesc(
			"bowl_client_capcache_misses_total",
			"Total number of security.capability cache misses.",
			nil,
			nil,
		),
		capGrantsDesc: prometheus.NewDesc(
			"bowl_client_capcache_grants_total",
			"Total number of local capability cache state acquisitions and refreshes.",
			nil,
			nil,
		),
		capEpochInvalidationsDesc: prometheus.NewDesc(
			"bowl_client_capcache_epoch_invalidations_total",
			"Total number of capability cache resets triggered by session or epoch invalidation.",
			nil,
			nil,
		),
		capEntriesDesc: prometheus.NewDesc(
			"bowl_client_capcache_entries",
			"Current number of tracked inodes in client capabilityCache.",
			nil,
			nil,
		),

		raTriggersDesc: prometheus.NewDesc(
			"bowl_client_readahead_triggers_total",
			"Total number of asynchronous read-ahead window prefetches triggered.",
			nil,
			nil,
		),
		raPrefetchedDesc: prometheus.NewDesc(
			"bowl_client_readahead_prefetched_bytes_total",
			"Total bytes fetched by read-ahead windows and background prefetches.",
			nil,
			nil,
		),
		raHitBytesDesc: prometheus.NewDesc(
			"bowl_client_readahead_hit_bytes_total",
			"Total bytes served directly from client read-ahead windows.",
			nil,
			nil,
		),
		raDiscardedDesc: prometheus.NewDesc(
			"bowl_client_readahead_discarded_bytes_total",
			"Total unconsumed read-ahead bytes discarded due to window reset, invalidation, or handle close.",
			nil,
			nil,
		),
		raInflightDesc: prometheus.NewDesc(
			"bowl_client_readahead_inflight",
			"Current number of in-flight asynchronous read-ahead prefetches.",
			nil,
			nil,
		),
		raWaitDurationDesc: prometheus.NewDesc(
			"bowl_client_read_ahead_wait_duration_seconds",
			"Blocking wait duration in seconds when foreground Read waits for an in-flight read-ahead window.",
			nil,
			nil,
		),

		stageWritesTotalDesc: prometheus.NewDesc(
			"bowl_client_stage_writes_total",
			"Total number of client write calls routed through local-disk staging vs synchronous passthrough.",
			[]string{"mode"},
			nil,
		),
		stageBytesTotalDesc: prometheus.NewDesc(
			"bowl_client_stage_bytes_total",
			"Total payload bytes routed through local-disk staging vs synchronous passthrough.",
			[]string{"mode"},
			nil,
		),
		stageUploadRPCsTotalDesc: prometheus.NewDesc(
			"bowl_client_stage_upload_rpcs_total",
			"Total number of background staged chunk Write RPC uploads by result.",
			[]string{"result"},
			nil,
		),
		stageInflightDesc: prometheus.NewDesc(
			"bowl_client_stage_inflight",
			"Current number of in-flight background staged chunk upload RPCs.",
			nil,
			nil,
		),
		stagePendingChunksDesc: prometheus.NewDesc(
			"bowl_client_stage_pending_chunks",
			"Current number of active, queued, and in-flight staged chunk files.",
			nil,
			nil,
		),
		stageUsedBytesDesc: prometheus.NewDesc(
			"bowl_client_stage_used_bytes",
			"Current host-disk staging quota bytes occupied by pending chunks.",
			nil,
			nil,
		),
		stageCapacityBytesDesc: prometheus.NewDesc(
			"bowl_client_stage_capacity_bytes",
			"Configured local-disk staging capacity limit in bytes.",
			nil,
			nil,
		),
		stageBackpressureWaitDesc: prometheus.NewDesc(
			"bowl_client_stage_backpressure_wait_duration_seconds",
			"Blocking wait duration in seconds when foreground writes wait for local-disk stage quota.",
			nil,
			nil,
		),
		stageBarrierWaitDesc: prometheus.NewDesc(
			"bowl_client_stage_barrier_wait_duration_seconds",
			"Blocking drain duration in seconds when FUSE barrier operations wait for staged uploads.",
			nil,
			nil,
		),

		tmpActiveFilesDesc: prometheus.NewDesc(
			"bowl_client_tmpfile_active_files",
			"Current number of active anonymous temporary files managed on host disk.",
			nil,
			nil,
		),
		tmpResidentBytesDesc: prometheus.NewDesc(
			"bowl_client_tmpfile_resident_bytes",
			"Current host-disk bytes occupied by unpromoted temporary files.",
			nil,
			nil,
		),
		tmpPromotesDesc: prometheus.NewDesc(
			"bowl_client_tmpfile_promotes_total",
			"Total number of anonymous temporary files promoted to named entries via Link.",
			nil,
			nil,
		),

		openHandlesDesc: prometheus.NewDesc(
			"bowl_client_open_handles",
			"Current number of open client handles by kind.",
			[]string{"kind"},
			nil,
		),

		inodeLockAcqDesc: prometheus.NewDesc(
			"bowl_client_inodelock_acquisitions_total",
			"Total number of per-inode serialization lock acquisitions by contention mode.",
			[]string{"mode"},
			nil,
		),
		inodeLockWaitSecDesc: prometheus.NewDesc(
			"bowl_client_inodelock_wait_seconds_total",
			"Total contended wait time in seconds spent acquiring per-inode serialization locks.",
			nil,
			nil,
		),
		inodeLockWaitDurationDesc: prometheus.NewDesc(
			"bowl_client_inodelock_wait_duration_seconds",
			"Contended wait duration distribution in seconds for per-inode serialization locks.",
			nil,
			nil,
		),
		inodeLockActiveDesc: prometheus.NewDesc(
			"bowl_client_inodelock_active",
			"Current number of active per-inode serialization lock entries.",
			nil,
			nil,
		),

		queueDepthDesc: prometheus.NewDesc(
			"bowl_client_queue_depth",
			"Current depth of client asynchronous state queues.",
			[]string{"queue"},
			nil,
		),
		queueOpsDesc: prometheus.NewDesc(
			"bowl_client_queue_ops_total",
			"Total number of client asynchronous flush, orphan reclaim, and release queue operations.",
			[]string{"queue", "result"},
			nil,
		),
		fuseConnWaitingDesc: prometheus.NewDesc(
			"bowl_client_fuse_connection_waiting",
			"Current number of waiting requests reported by Linux /sys/fs/fuse/connections/<dev>/waiting.",
			nil,
			nil,
		),
		fuseConnCongestionThrsDesc: prometheus.NewDesc(
			"bowl_client_fuse_connection_congestion_threshold",
			"Congestion threshold reported by Linux /sys/fs/fuse/connections/<dev>/congestion_threshold.",
			nil,
			nil,
		),
	}
}

func (m *Metrics) bindProvider(p stateSnapshotProvider) {
	if m == nil || p == nil {
		return
	}
	m.provider.Store(&p)
}

func (m *Metrics) collectGauges() subsystemGauges {
	if m == nil {
		return subsystemGauges{}
	}
	if ptr := m.provider.Load(); ptr != nil && *ptr != nil {
		return (*ptr).collectSubsystemGauges()
	}
	return subsystemGauges{
		tmpResidentByte: uint64(max(int64(0), m.tmpResidentBytes.Load())),
	}
}

func (m *Metrics) beginOp(op FuseOp) time.Time {
	if m != nil && op < OpCount {
		m.ops[op].inflight.Add(1)
	}
	return time.Now()
}

func (m *Metrics) endOp(op FuseOp, start time.Time, status fuse.Status, nBytes uint64) {
	if m == nil || op >= OpCount {
		return
	}
	durNs := max(int64(0), time.Since(start).Nanoseconds())
	slot := &m.ops[op]
	slot.inflight.Add(-1)
	if nBytes > 0 {
		slot.bytes.Add(nBytes)
	}
	us := uint64(durNs) / 1000
	if idx := bits.Len64(us); idx < opHistBuckets {
		slot.buckets[idx].Add(1)
	}
	slot.latNanos.Add(uint64(durNs))
	slot.calls.Add(1)
	if status != fuse.OK {
		slot.errors.Add(1)
		m.errnoTable[op][errnoIndex(status)].Add(1)
	}
}

func (m *Metrics) incNSCacheHit(kind nsCacheKind) {
	if m != nil && kind < nsCacheKindCount {
		m.nsHits[kind].Add(1)
	}
}

func (m *Metrics) incNSCacheMiss(kind nsCacheKind) {
	if m != nil && kind < nsCacheKindCount {
		m.nsMisses[kind].Add(1)
	}
}

func (m *Metrics) incNSCacheInvalidation() {
	if m != nil {
		m.nsInvalidations.Add(1)
	}
}

func (m *Metrics) incNSCacheEviction() {
	if m != nil {
		m.nsEvictions.Add(1)
	}
}

func (m *Metrics) incCapHit() {
	if m != nil {
		m.capHits.Add(1)
	}
}

func (m *Metrics) incCapMiss() {
	if m != nil {
		m.capMisses.Add(1)
	}
}

func (m *Metrics) incCapGrant() {
	if m != nil {
		m.capGrants.Add(1)
	}
}

func (m *Metrics) incCapEpochInvalidation() {
	if m != nil {
		m.capEpochInvalidations.Add(1)
	}
}

func (m *Metrics) incReadAheadTrigger() {
	if m != nil {
		m.readAheadTriggers.Add(1)
	}
}

func (m *Metrics) addReadAheadPrefetchedBytes(n uint64) {
	if m != nil && n > 0 {
		m.readAheadPrefetchedBytes.Add(n)
	}
}

func (m *Metrics) addReadAheadHit(n uint64) {
	if m != nil {
		m.readAheadHits.Add(1)
		if n > 0 {
			m.readAheadHitBytes.Add(n)
		}
	}
}

func (m *Metrics) addReadAheadDiscardedBytes(n uint64) {
	if m != nil && n > 0 {
		m.readAheadDiscardedBytes.Add(n)
	}
}

func (m *Metrics) addReadAheadInflight(delta int64) {
	if m != nil && delta != 0 {
		m.readAheadInflight.Add(delta)
	}
}

func (m *Metrics) observeReadAheadWait(d time.Duration) {
	if m != nil {
		m.readAheadWaitHist.observe(d)
	}
}

func (m *Metrics) addStageStaged(n uint64) {
	if m != nil {
		m.stageStagedCalls.Add(1)
		if n > 0 {
			m.stageStagedBytes.Add(n)
		}
	}
}

func (m *Metrics) addStagePassthrough(n uint64) {
	if m != nil {
		m.stagePassthroughCalls.Add(1)
		if n > 0 {
			m.stagePassthroughBytes.Add(n)
		}
	}
}

func (m *Metrics) incStageUploadRPC(isErr bool) {
	if m == nil {
		return
	}
	if isErr {
		m.stageUploadRPCErrors.Add(1)
	} else {
		m.stageUploadRPCOK.Add(1)
	}
}

func (m *Metrics) addStageInflight(delta int64) {
	if m != nil && delta != 0 {
		m.stageInflight.Add(delta)
	}
}

func (m *Metrics) observeStageBackpressureWait(d time.Duration) {
	if m != nil {
		m.stageBackpressureWaitHist.observe(d)
	}
}

func (m *Metrics) observeStageBarrierWait(d time.Duration) {
	if m != nil {
		m.stageBarrierWaitHist.observe(d)
	}
}

func (m *Metrics) addTmpResidentBytes(delta int64) {
	if m != nil && delta != 0 {
		m.tmpResidentBytes.Add(delta)
	}
}

func (m *Metrics) incTmpPromote() {
	if m != nil {
		m.tmpPromotes.Add(1)
	}
}

func (m *Metrics) incInodeLockFast() {
	if m != nil {
		m.inodeLockFast.Add(1)
	}
}

func (m *Metrics) observeInodeLockContended(d time.Duration) {
	if m != nil {
		m.inodeLockContended.Add(1)
		if ns := d.Nanoseconds(); ns > 0 {
			m.inodeLockWaitNanos.Add(uint64(ns))
		}
		m.inodeLockWaitHist.observe(d)
	}
}

func (m *Metrics) incDirtyFlushOK() {
	if m != nil {
		m.dirtyFlushOK.Add(1)
	}
}

func (m *Metrics) incDirtyFlushError() {
	if m != nil {
		m.dirtyFlushErrors.Add(1)
	}
}

func (m *Metrics) incOrphanReclaimOK() {
	if m != nil {
		m.orphanReclaimOK.Add(1)
	}
}

func (m *Metrics) incOrphanReclaimError() {
	if m != nil {
		m.orphanReclaimErrors.Add(1)
	}
}

func (m *Metrics) incAsyncReleaseOK() {
	if m != nil {
		m.asyncReleaseOK.Add(1)
	}
}

func (m *Metrics) incAsyncReleaseError() {
	if m != nil {
		m.asyncReleaseErrors.Add(1)
	}
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if m == nil {
		return
	}
	ch <- m.fuseOpDurationDesc
	ch <- m.fuseOpsTotalDesc
	ch <- m.fuseInflightDesc
	ch <- m.fuseOpErrorsDesc
	ch <- m.fuseIOBytesDesc

	ch <- m.nsHitsDesc
	ch <- m.nsMissesDesc
	ch <- m.nsInvalidationsDesc
	ch <- m.nsEvictionsDesc
	ch <- m.nsEntriesDesc
	ch <- m.nsWeightDesc

	ch <- m.capHitsDesc
	ch <- m.capMissesDesc
	ch <- m.capGrantsDesc
	ch <- m.capEpochInvalidationsDesc
	ch <- m.capEntriesDesc

	ch <- m.raTriggersDesc
	ch <- m.raPrefetchedDesc
	ch <- m.raHitBytesDesc
	ch <- m.raDiscardedDesc
	ch <- m.raInflightDesc
	ch <- m.raWaitDurationDesc

	ch <- m.stageWritesTotalDesc
	ch <- m.stageBytesTotalDesc
	ch <- m.stageUploadRPCsTotalDesc
	ch <- m.stageInflightDesc
	ch <- m.stagePendingChunksDesc
	ch <- m.stageUsedBytesDesc
	ch <- m.stageCapacityBytesDesc
	ch <- m.stageBackpressureWaitDesc
	ch <- m.stageBarrierWaitDesc

	ch <- m.tmpActiveFilesDesc
	ch <- m.tmpResidentBytesDesc
	ch <- m.tmpPromotesDesc

	ch <- m.openHandlesDesc

	ch <- m.inodeLockAcqDesc
	ch <- m.inodeLockWaitSecDesc
	ch <- m.inodeLockWaitDurationDesc
	ch <- m.inodeLockActiveDesc

	ch <- m.queueDepthDesc
	ch <- m.queueOpsDesc
	ch <- m.fuseConnWaitingDesc
	ch <- m.fuseConnCongestionThrsDesc
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m == nil {
		return
	}

	for op := range FuseOp(OpCount) {
		slot := &m.ops[op]
		opName := fuseOpNames[op]

		buckets := make(map[float64]uint64, opHistBuckets)
		var cum uint64
		for i := range opHistBuckets {
			cum += slot.buckets[i].Load()
			buckets[opHistUpperSec[i]] = cum
		}
		latNs := slot.latNanos.Load()
		calls := max(slot.calls.Load(), cum)
		inflight := slot.inflight.Load()

		ch <- prometheus.MustNewConstHistogram(
			m.fuseOpDurationDesc,
			calls,
			float64(latNs)*1e-9,
			buckets,
			opName,
		)
		ch <- prometheus.MustNewConstMetric(
			m.fuseOpsTotalDesc,
			prometheus.CounterValue,
			float64(calls),
			opName,
		)
		ch <- prometheus.MustNewConstMetric(
			m.fuseInflightDesc,
			prometheus.GaugeValue,
			float64(inflight),
			opName,
		)

		for errIdx := range errnoBucketCount {
			if cnt := m.errnoTable[op][errIdx].Load(); cnt > 0 {
				ch <- prometheus.MustNewConstMetric(
					m.fuseOpErrorsDesc,
					prometheus.CounterValue,
					float64(cnt),
					opName,
					errnoBucketNames[errIdx],
				)
			}
		}
	}

	writeBytes := m.ops[OpWrite].bytes.Load() + m.ops[OpWritev].bytes.Load() + m.ops[OpCopyFileRange].bytes.Load()
	ch <- prometheus.MustNewConstMetric(
		m.fuseIOBytesDesc,
		prometheus.CounterValue,
		float64(m.ops[OpRead].bytes.Load()),
		"read",
	)
	ch <- prometheus.MustNewConstMetric(
		m.fuseIOBytesDesc,
		prometheus.CounterValue,
		float64(writeBytes),
		"write",
	)

	gauges := m.collectGauges()

	for k := range nsCacheKindCount {
		label := nsCacheKindNames[k]
		ch <- prometheus.MustNewConstMetric(m.nsHitsDesc, prometheus.CounterValue, float64(m.nsHits[k].Load()), label)
		ch <- prometheus.MustNewConstMetric(m.nsMissesDesc, prometheus.CounterValue, float64(m.nsMisses[k].Load()), label)
	}
	ch <- prometheus.MustNewConstMetric(m.nsInvalidationsDesc, prometheus.CounterValue, float64(m.nsInvalidations.Load()))
	ch <- prometheus.MustNewConstMetric(m.nsEvictionsDesc, prometheus.CounterValue, float64(m.nsEvictions.Load()))
	ch <- prometheus.MustNewConstMetric(m.nsEntriesDesc, prometheus.GaugeValue, float64(gauges.nsEntries))
	ch <- prometheus.MustNewConstMetric(m.nsWeightDesc, prometheus.GaugeValue, float64(gauges.nsUsedWeight), "used")
	ch <- prometheus.MustNewConstMetric(m.nsWeightDesc, prometheus.GaugeValue, float64(gauges.nsCapacity), "capacity")

	ch <- prometheus.MustNewConstMetric(m.capHitsDesc, prometheus.CounterValue, float64(m.capHits.Load()))
	ch <- prometheus.MustNewConstMetric(m.capMissesDesc, prometheus.CounterValue, float64(m.capMisses.Load()))
	ch <- prometheus.MustNewConstMetric(m.capGrantsDesc, prometheus.CounterValue, float64(m.capGrants.Load()))
	ch <- prometheus.MustNewConstMetric(m.capEpochInvalidationsDesc, prometheus.CounterValue, float64(m.capEpochInvalidations.Load()))
	ch <- prometheus.MustNewConstMetric(m.capEntriesDesc, prometheus.GaugeValue, float64(gauges.capEntries))

	ch <- prometheus.MustNewConstMetric(m.raTriggersDesc, prometheus.CounterValue, float64(m.readAheadTriggers.Load()))
	ch <- prometheus.MustNewConstMetric(m.raPrefetchedDesc, prometheus.CounterValue, float64(m.readAheadPrefetchedBytes.Load()))
	ch <- prometheus.MustNewConstMetric(m.raHitBytesDesc, prometheus.CounterValue, float64(m.readAheadHitBytes.Load()))
	ch <- prometheus.MustNewConstMetric(m.raDiscardedDesc, prometheus.CounterValue, float64(m.readAheadDiscardedBytes.Load()))
	ch <- prometheus.MustNewConstMetric(m.raInflightDesc, prometheus.GaugeValue, float64(m.readAheadInflight.Load()))
	ch <- m.readAheadWaitHist.constHistogram(m.raWaitDurationDesc)

	ch <- prometheus.MustNewConstMetric(m.stageWritesTotalDesc, prometheus.CounterValue, float64(m.stageStagedCalls.Load()), "staged")
	ch <- prometheus.MustNewConstMetric(m.stageWritesTotalDesc, prometheus.CounterValue, float64(m.stagePassthroughCalls.Load()), "passthrough")
	ch <- prometheus.MustNewConstMetric(m.stageBytesTotalDesc, prometheus.CounterValue, float64(m.stageStagedBytes.Load()), "staged")
	ch <- prometheus.MustNewConstMetric(m.stageBytesTotalDesc, prometheus.CounterValue, float64(m.stagePassthroughBytes.Load()), "passthrough")
	ch <- prometheus.MustNewConstMetric(m.stageUploadRPCsTotalDesc, prometheus.CounterValue, float64(m.stageUploadRPCOK.Load()), "ok")
	ch <- prometheus.MustNewConstMetric(m.stageUploadRPCsTotalDesc, prometheus.CounterValue, float64(m.stageUploadRPCErrors.Load()), "error")
	ch <- prometheus.MustNewConstMetric(m.stageInflightDesc, prometheus.GaugeValue, float64(m.stageInflight.Load()))
	ch <- prometheus.MustNewConstMetric(m.stagePendingChunksDesc, prometheus.GaugeValue, float64(gauges.stagePendingChunks))
	ch <- prometheus.MustNewConstMetric(m.stageUsedBytesDesc, prometheus.GaugeValue, float64(gauges.stageUsedBytes))
	ch <- prometheus.MustNewConstMetric(m.stageCapacityBytesDesc, prometheus.GaugeValue, float64(gauges.stageCapacityBytes))
	ch <- m.stageBackpressureWaitHist.constHistogram(m.stageBackpressureWaitDesc)
	ch <- m.stageBarrierWaitHist.constHistogram(m.stageBarrierWaitDesc)

	ch <- prometheus.MustNewConstMetric(m.tmpActiveFilesDesc, prometheus.GaugeValue, float64(gauges.tmpActiveFiles))
	ch <- prometheus.MustNewConstMetric(m.tmpResidentBytesDesc, prometheus.GaugeValue, float64(gauges.tmpResidentByte))
	ch <- prometheus.MustNewConstMetric(m.tmpPromotesDesc, prometheus.CounterValue, float64(m.tmpPromotes.Load()))

	ch <- prometheus.MustNewConstMetric(m.openHandlesDesc, prometheus.GaugeValue, float64(gauges.openFiles), "file")
	ch <- prometheus.MustNewConstMetric(m.openHandlesDesc, prometheus.GaugeValue, float64(gauges.openDirs), "dir")

	ch <- prometheus.MustNewConstMetric(m.inodeLockAcqDesc, prometheus.CounterValue, float64(m.inodeLockFast.Load()), "fast")
	ch <- prometheus.MustNewConstMetric(m.inodeLockAcqDesc, prometheus.CounterValue, float64(m.inodeLockContended.Load()), "contended")
	ch <- prometheus.MustNewConstMetric(m.inodeLockWaitSecDesc, prometheus.CounterValue, float64(m.inodeLockWaitNanos.Load())*1e-9)
	ch <- m.inodeLockWaitHist.constHistogram(m.inodeLockWaitDurationDesc)
	ch <- prometheus.MustNewConstMetric(m.inodeLockActiveDesc, prometheus.GaugeValue, float64(gauges.inodeLockActive))

	ch <- prometheus.MustNewConstMetric(m.queueDepthDesc, prometheus.GaugeValue, float64(gauges.orphanInodes), "orphan_inodes")
	ch <- prometheus.MustNewConstMetric(m.queueDepthDesc, prometheus.GaugeValue, float64(gauges.releaseQueue), "release_queue")

	ch <- prometheus.MustNewConstMetric(m.queueOpsDesc, prometheus.CounterValue, float64(m.dirtyFlushOK.Load()), "dirty_flush", "ok")
	ch <- prometheus.MustNewConstMetric(m.queueOpsDesc, prometheus.CounterValue, float64(m.dirtyFlushErrors.Load()), "dirty_flush", "error")
	ch <- prometheus.MustNewConstMetric(m.queueOpsDesc, prometheus.CounterValue, float64(m.orphanReclaimOK.Load()), "orphan_reclaim", "ok")
	ch <- prometheus.MustNewConstMetric(m.queueOpsDesc, prometheus.CounterValue, float64(m.orphanReclaimErrors.Load()), "orphan_reclaim", "error")
	ch <- prometheus.MustNewConstMetric(m.queueOpsDesc, prometheus.CounterValue, float64(m.asyncReleaseOK.Load()), "async_release", "ok")
	ch <- prometheus.MustNewConstMetric(m.queueOpsDesc, prometheus.CounterValue, float64(m.asyncReleaseErrors.Load()), "async_release", "error")

	ch <- prometheus.MustNewConstMetric(m.fuseConnWaitingDesc, prometheus.GaugeValue, float64(gauges.fuseWaiting))
	ch <- prometheus.MustNewConstMetric(m.fuseConnCongestionThrsDesc, prometheus.GaugeValue, float64(gauges.fuseCongestionThreshold))
}

type OpSnapshot struct {
	Op            string                `json:"op"`
	Inflight      int64                 `json:"inflight"`
	Calls         uint64                `json:"calls"`
	Errors        uint64                `json:"errors"`
	Bytes         uint64                `json:"bytes"`
	DurationNanos uint64                `json:"duration_nanos"`
	Buckets       [opHistBuckets]uint64 `json:"buckets"`
	Errnos        map[string]uint64     `json:"errnos,omitempty"`
}

type FUSESnapshot struct {
	TotalCalls           uint64       `json:"total_calls"`
	TotalErrors          uint64       `json:"total_errors"`
	TotalInflight        int64        `json:"total_inflight"`
	TotalDurationSeconds float64      `json:"total_duration_seconds"`
	ReadBytes            uint64       `json:"read_bytes"`
	WriteBytes           uint64       `json:"write_bytes"`
	Ops                  []OpSnapshot `json:"ops"`
}

type NamespaceCacheStats struct {
	AttrHits            uint64 `json:"attr_hits"`
	AttrMisses          uint64 `json:"attr_misses"`
	EntryHits           uint64 `json:"entry_hits"`
	EntryMisses         uint64 `json:"entry_misses"`
	NegativeEntryHits   uint64 `json:"negative_entry_hits"`
	NegativeEntryMisses uint64 `json:"negative_entry_misses"`
	ReadDirHits         uint64 `json:"readdir_hits"`
	ReadDirMisses       uint64 `json:"readdir_misses"`
	ReadlinkHits        uint64 `json:"readlink_hits"`
	ReadlinkMisses      uint64 `json:"readlink_misses"`
	Invalidations       uint64 `json:"invalidations"`
	Evictions           uint64 `json:"evictions"`
	Entries             int    `json:"entries"`
	UsedWeight          int    `json:"used_weight"`
	Capacity            int    `json:"capacity"`
}

type CapabilityCacheStats struct {
	Hits               uint64 `json:"hits"`
	Misses             uint64 `json:"misses"`
	Grants             uint64 `json:"grants"`
	EpochInvalidations uint64 `json:"epoch_invalidations"`
	Entries            int    `json:"entries"`
}

type ReadAheadStats struct {
	Triggers        uint64  `json:"triggers"`
	PrefetchedBytes uint64  `json:"prefetched_bytes"`
	Hits            uint64  `json:"hits"`
	HitBytes        uint64  `json:"hit_bytes"`
	DiscardedBytes  uint64  `json:"discarded_bytes"`
	Inflight        int64   `json:"inflight"`
	Waits           uint64  `json:"waits"`
	WaitSeconds     float64 `json:"wait_seconds"`
}

type StageStats struct {
	StagedCalls             uint64  `json:"staged_calls"`
	StagedBytes             uint64  `json:"staged_bytes"`
	PassthroughCalls        uint64  `json:"passthrough_calls"`
	PassthroughBytes        uint64  `json:"passthrough_bytes"`
	UploadRPCs              uint64  `json:"upload_rpcs"`
	UploadErrors            uint64  `json:"upload_errors"`
	Inflight                int64   `json:"inflight"`
	PendingChunks           int64   `json:"pending_chunks"`
	UsedBytes               uint64  `json:"used_bytes"`
	CapacityBytes           uint64  `json:"capacity_bytes"`
	BackpressureWaits       uint64  `json:"backpressure_waits"`
	BackpressureWaitSeconds float64 `json:"backpressure_wait_seconds"`
	BarrierWaits            uint64  `json:"barrier_waits"`
	BarrierWaitSeconds      float64 `json:"barrier_wait_seconds"`
}

type TmpFileStats struct {
	ActiveFiles   int    `json:"active_files"`
	ResidentBytes uint64 `json:"resident_bytes"`
	Promotes      uint64 `json:"promotes"`
}

type HandleStats struct {
	OpenFiles int `json:"open_files"`
	OpenDirs  int `json:"open_dirs"`
}

type InodeLockStats struct {
	FastAcquisitions      uint64  `json:"fast_acquisitions"`
	ContendedAcquisitions uint64  `json:"contended_acquisitions"`
	WaitSeconds           float64 `json:"wait_seconds"`
	ActiveEntries         int     `json:"active_entries"`
}

type AsyncQueueStats struct {
	OrphanInodes            int    `json:"orphan_inodes"`
	ReleaseQueueDepth       int    `json:"release_queue_depth"`
	DirtyFlushOK            uint64 `json:"dirty_flush_ok"`
	DirtyFlushErrors        uint64 `json:"dirty_flush_errors"`
	OrphanReclaimOK         uint64 `json:"orphan_reclaim_ok"`
	OrphanReclaimErrors     uint64 `json:"orphan_reclaim_errors"`
	AsyncReleaseOK          uint64 `json:"async_release_ok"`
	AsyncReleaseErrors      uint64 `json:"async_release_errors"`
	FuseWaiting             int    `json:"fuse_waiting"`
	FuseCongestionThreshold int    `json:"fuse_congestion_threshold"`
}

type Snapshot struct {
	TimestampUnixNano int64                `json:"timestamp_unix_nano"`
	UptimeSeconds     float64              `json:"uptime_seconds"`
	CPUUsageSeconds   float64              `json:"cpu_usage_seconds"`
	MemoryBytes       uint64               `json:"memory_bytes"`
	FUSE              FUSESnapshot         `json:"fuse"`
	NamespaceCache    NamespaceCacheStats  `json:"namespace_cache"`
	CapabilityCache   CapabilityCacheStats `json:"capability_cache"`
	ReadAhead         ReadAheadStats       `json:"read_ahead"`
	Stage             StageStats           `json:"stage"`
	TmpFile           TmpFileStats         `json:"tmp_file"`
	Handles           HandleStats          `json:"handles"`
	InodeLocks        InodeLockStats       `json:"inode_locks"`
	Queues            AsyncQueueStats      `json:"queues"`
}

func readRuntimeProcessStats() (cpuSec float64, memBytes uint64) {
	samples := [3]rmetrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
		{Name: "/memory/classes/total:bytes"},
	}
	rmetrics.Read(samples[:])
	var totalCPU, idleCPU float64
	if samples[0].Value.Kind() == rmetrics.KindFloat64 {
		totalCPU = samples[0].Value.Float64()
	}
	if samples[1].Value.Kind() == rmetrics.KindFloat64 {
		idleCPU = samples[1].Value.Float64()
	}
	if samples[2].Value.Kind() == rmetrics.KindUint64 {
		memBytes = samples[2].Value.Uint64()
	}
	cpuSec = max(0, totalCPU-idleCPU)
	return cpuSec, memBytes
}

func (m *Metrics) Snapshot() Snapshot {
	nowNs := time.Now().UnixNano()
	cpuSec, memBytes := readRuntimeProcessStats()
	if m == nil {
		return Snapshot{
			TimestampUnixNano: nowNs,
			CPUUsageSeconds:   cpuSec,
			MemoryBytes:       memBytes,
		}
	}

	uptimeSec := max(0, float64(nowNs-m.startedAtUnixNano)*1e-9)
	gauges := m.collectGauges()

	ops := make([]OpSnapshot, OpCount)
	var totalCalls, totalErrors, totalDurNs uint64
	var totalInflight int64
	for op := range FuseOp(OpCount) {
		slot := &m.ops[op]
		var buckets [opHistBuckets]uint64
		var cum uint64
		for i := range opHistBuckets {
			v := slot.buckets[i].Load()
			buckets[i] = v
			cum += v
		}
		calls := max(slot.calls.Load(), cum)
		errs := slot.errors.Load()
		b := slot.bytes.Load()
		durNs := slot.latNanos.Load()
		inf := slot.inflight.Load()

		var errMap map[string]uint64
		for errIdx := range errnoBucketCount {
			if cnt := m.errnoTable[op][errIdx].Load(); cnt > 0 {
				if errMap == nil {
					errMap = make(map[string]uint64)
				}
				errMap[errnoBucketNames[errIdx]] = cnt
			}
		}

		ops[op] = OpSnapshot{
			Op:            fuseOpNames[op],
			Inflight:      inf,
			Calls:         calls,
			Errors:        errs,
			Bytes:         b,
			DurationNanos: durNs,
			Buckets:       buckets,
			Errnos:        errMap,
		}
		totalCalls += calls
		totalErrors += errs
		totalDurNs += durNs
		totalInflight += inf
	}

	uploadOK := m.stageUploadRPCOK.Load()
	uploadErr := m.stageUploadRPCErrors.Load()

	return Snapshot{
		TimestampUnixNano: nowNs,
		UptimeSeconds:     uptimeSec,
		CPUUsageSeconds:   cpuSec,
		MemoryBytes:       memBytes,
		FUSE: FUSESnapshot{
			TotalCalls:           totalCalls,
			TotalErrors:          totalErrors,
			TotalInflight:        totalInflight,
			TotalDurationSeconds: float64(totalDurNs) * 1e-9,
			ReadBytes:            ops[OpRead].Bytes,
			WriteBytes:           ops[OpWrite].Bytes + ops[OpWritev].Bytes + ops[OpCopyFileRange].Bytes,
			Ops:                  ops,
		},
		NamespaceCache: NamespaceCacheStats{
			AttrHits:            m.nsHits[nsCacheAttr].Load(),
			AttrMisses:          m.nsMisses[nsCacheAttr].Load(),
			EntryHits:           m.nsHits[nsCacheEntry].Load(),
			EntryMisses:         m.nsMisses[nsCacheEntry].Load(),
			NegativeEntryHits:   m.nsHits[nsCacheNegativeEntry].Load(),
			NegativeEntryMisses: m.nsMisses[nsCacheNegativeEntry].Load(),
			ReadDirHits:         m.nsHits[nsCacheReadDir].Load(),
			ReadDirMisses:       m.nsMisses[nsCacheReadDir].Load(),
			ReadlinkHits:        m.nsHits[nsCacheReadlink].Load(),
			ReadlinkMisses:      m.nsMisses[nsCacheReadlink].Load(),
			Invalidations:       m.nsInvalidations.Load(),
			Evictions:           m.nsEvictions.Load(),
			Entries:             gauges.nsEntries,
			UsedWeight:          gauges.nsUsedWeight,
			Capacity:            gauges.nsCapacity,
		},
		CapabilityCache: CapabilityCacheStats{
			Hits:               m.capHits.Load(),
			Misses:             m.capMisses.Load(),
			Grants:             m.capGrants.Load(),
			EpochInvalidations: m.capEpochInvalidations.Load(),
			Entries:            gauges.capEntries,
		},
		ReadAhead: ReadAheadStats{
			Triggers:        m.readAheadTriggers.Load(),
			PrefetchedBytes: m.readAheadPrefetchedBytes.Load(),
			Hits:            m.readAheadHits.Load(),
			HitBytes:        m.readAheadHitBytes.Load(),
			DiscardedBytes:  m.readAheadDiscardedBytes.Load(),
			Inflight:        m.readAheadInflight.Load(),
			Waits:           m.readAheadWaitHist.count.Load(),
			WaitSeconds:     float64(m.readAheadWaitHist.sumNanos.Load()) * 1e-9,
		},
		Stage: StageStats{
			StagedCalls:             m.stageStagedCalls.Load(),
			StagedBytes:             m.stageStagedBytes.Load(),
			PassthroughCalls:        m.stagePassthroughCalls.Load(),
			PassthroughBytes:        m.stagePassthroughBytes.Load(),
			UploadRPCs:              uploadOK + uploadErr,
			UploadErrors:            uploadErr,
			Inflight:                m.stageInflight.Load(),
			PendingChunks:           gauges.stagePendingChunks,
			UsedBytes:               gauges.stageUsedBytes,
			CapacityBytes:           gauges.stageCapacityBytes,
			BackpressureWaits:       m.stageBackpressureWaitHist.count.Load(),
			BackpressureWaitSeconds: float64(m.stageBackpressureWaitHist.sumNanos.Load()) * 1e-9,
			BarrierWaits:            m.stageBarrierWaitHist.count.Load(),
			BarrierWaitSeconds:      float64(m.stageBarrierWaitHist.sumNanos.Load()) * 1e-9,
		},
		TmpFile: TmpFileStats{
			ActiveFiles:   gauges.tmpActiveFiles,
			ResidentBytes: gauges.tmpResidentByte,
			Promotes:      m.tmpPromotes.Load(),
		},
		Handles: HandleStats{
			OpenFiles: gauges.openFiles,
			OpenDirs:  gauges.openDirs,
		},
		InodeLocks: InodeLockStats{
			FastAcquisitions:      m.inodeLockFast.Load(),
			ContendedAcquisitions: m.inodeLockContended.Load(),
			WaitSeconds:           float64(m.inodeLockWaitNanos.Load()) * 1e-9,
			ActiveEntries:         gauges.inodeLockActive,
		},
		Queues: AsyncQueueStats{
			OrphanInodes:            gauges.orphanInodes,
			ReleaseQueueDepth:       gauges.releaseQueue,
			DirtyFlushOK:            m.dirtyFlushOK.Load(),
			DirtyFlushErrors:        m.dirtyFlushErrors.Load(),
			OrphanReclaimOK:         m.orphanReclaimOK.Load(),
			OrphanReclaimErrors:     m.orphanReclaimErrors.Load(),
			AsyncReleaseOK:          m.asyncReleaseOK.Load(),
			AsyncReleaseErrors:      m.asyncReleaseErrors.Load(),
			FuseWaiting:             gauges.fuseWaiting,
			FuseCongestionThreshold: gauges.fuseCongestionThreshold,
		},
	}
}

func (s *Snapshot) FormatJSON() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}

func (s *Snapshot) FormatText() string {
	var b strings.Builder
	b.Grow(1280)

	fmt.Fprintf(&b, "Uptime: %.3fs  CPU: %.3fs  Memory: %d B\n", s.UptimeSeconds, s.CPUUsageSeconds, s.MemoryBytes)
	fmt.Fprintf(&b, "FUSE: calls=%d errors=%d inflight=%d duration=%.6fs read_bytes=%d write_bytes=%d\n",
		s.FUSE.TotalCalls, s.FUSE.TotalErrors, s.FUSE.TotalInflight,
		s.FUSE.TotalDurationSeconds, s.FUSE.ReadBytes, s.FUSE.WriteBytes,
	)
	fmt.Fprintf(&b, "NamespaceCache: entries=%d weight=%d/%d attr=%d/%d entry=%d/%d neg=%d/%d readdir=%d/%d readlink=%d/%d inv=%d evict=%d\n",
		s.NamespaceCache.Entries, s.NamespaceCache.UsedWeight, s.NamespaceCache.Capacity,
		s.NamespaceCache.AttrHits, s.NamespaceCache.AttrMisses,
		s.NamespaceCache.EntryHits, s.NamespaceCache.EntryMisses,
		s.NamespaceCache.NegativeEntryHits, s.NamespaceCache.NegativeEntryMisses,
		s.NamespaceCache.ReadDirHits, s.NamespaceCache.ReadDirMisses,
		s.NamespaceCache.ReadlinkHits, s.NamespaceCache.ReadlinkMisses,
		s.NamespaceCache.Invalidations, s.NamespaceCache.Evictions,
	)
	fmt.Fprintf(&b, "CapabilityCache: entries=%d hits=%d misses=%d grants=%d epoch_inv=%d\n",
		s.CapabilityCache.Entries, s.CapabilityCache.Hits, s.CapabilityCache.Misses,
		s.CapabilityCache.Grants, s.CapabilityCache.EpochInvalidations,
	)
	fmt.Fprintf(&b, "ReadAhead: triggers=%d prefetched_bytes=%d hits=%d hit_bytes=%d discarded_bytes=%d inflight=%d waits=%d wait_sec=%.6f\n",
		s.ReadAhead.Triggers, s.ReadAhead.PrefetchedBytes, s.ReadAhead.Hits,
		s.ReadAhead.HitBytes, s.ReadAhead.DiscardedBytes, s.ReadAhead.Inflight,
		s.ReadAhead.Waits, s.ReadAhead.WaitSeconds,
	)
	fmt.Fprintf(&b, "Stage: staged=%d/%dB passthrough=%d/%dB upload_rpcs=%d upload_err=%d inflight=%d chunks=%d used=%d/%dB bp_waits=%d/%.6fs barrier_waits=%d/%.6fs\n",
		s.Stage.StagedCalls, s.Stage.StagedBytes,
		s.Stage.PassthroughCalls, s.Stage.PassthroughBytes,
		s.Stage.UploadRPCs, s.Stage.UploadErrors,
		s.Stage.Inflight, s.Stage.PendingChunks,
		s.Stage.UsedBytes, s.Stage.CapacityBytes,
		s.Stage.BackpressureWaits, s.Stage.BackpressureWaitSeconds,
		s.Stage.BarrierWaits, s.Stage.BarrierWaitSeconds,
	)
	fmt.Fprintf(&b, "TmpFile: active=%d resident_bytes=%d promotes=%d\n",
		s.TmpFile.ActiveFiles, s.TmpFile.ResidentBytes, s.TmpFile.Promotes,
	)
	fmt.Fprintf(&b, "Handles: open_files=%d open_dirs=%d\n", s.Handles.OpenFiles, s.Handles.OpenDirs)
	fmt.Fprintf(&b, "InodeLocks: active=%d fast=%d contended=%d wait_sec=%.6f\n",
		s.InodeLocks.ActiveEntries, s.InodeLocks.FastAcquisitions,
		s.InodeLocks.ContendedAcquisitions, s.InodeLocks.WaitSeconds,
	)
	fmt.Fprintf(&b, "Queues: orphan=%d release=%d flush_ok=%d flush_err=%d reclaim_ok=%d reclaim_err=%d release_ok=%d release_err=%d fuse_waiting=%d fuse_congestion=%d\n",
		s.Queues.OrphanInodes, s.Queues.ReleaseQueueDepth,
		s.Queues.DirtyFlushOK, s.Queues.DirtyFlushErrors,
		s.Queues.OrphanReclaimOK, s.Queues.OrphanReclaimErrors,
		s.Queues.AsyncReleaseOK, s.Queues.AsyncReleaseErrors,
		s.Queues.FuseWaiting, s.Queues.FuseCongestionThreshold,
	)
	return b.String()
}

var metaMutationOps = [...]FuseOp{
	OpMkdir, OpMknod, OpRmdir, OpUnlink, OpRename,
	OpSymlink, OpLink, OpCreate, OpSetAttr, OpFallocate,
	OpSetXAttr, OpRemoveXAttr, OpFsync, OpFsyncDir,
}

func (s *Snapshot) FormatStatsFile() string {
	var b strings.Builder
	b.Grow(4096)

	writeU64 := func(k string, v uint64) {
		b.WriteString(k)
		b.WriteByte(' ')
		b.WriteString(strconv.FormatUint(v, 10))
		b.WriteByte('\n')
	}
	writeI64 := func(k string, v int64) {
		b.WriteString(k)
		b.WriteByte(' ')
		b.WriteString(strconv.FormatInt(v, 10))
		b.WriteByte('\n')
	}
	writeF64 := func(k string, v float64) {
		b.WriteString(k)
		b.WriteByte(' ')
		b.WriteString(strconv.FormatFloat(v, 'f', 6, 64))
		b.WriteByte('\n')
	}

	var metaCalls, metaDurNs uint64
	var readCalls, readDurNs uint64
	var writeCalls, writeDurNs uint64
	var deleteCalls, deleteDurNs uint64

	if len(s.FUSE.Ops) == OpCount {
		for _, op := range metaMutationOps {
			metaCalls += s.FUSE.Ops[op].Calls
			metaDurNs += s.FUSE.Ops[op].DurationNanos
		}
		readCalls = s.FUSE.Ops[OpRead].Calls
		readDurNs = s.FUSE.Ops[OpRead].DurationNanos
		writeCalls = s.FUSE.Ops[OpWrite].Calls + s.FUSE.Ops[OpWritev].Calls + s.FUSE.Ops[OpCopyFileRange].Calls
		writeDurNs = s.FUSE.Ops[OpWrite].DurationNanos + s.FUSE.Ops[OpWritev].DurationNanos + s.FUSE.Ops[OpCopyFileRange].DurationNanos
		deleteCalls = s.FUSE.Ops[OpUnlink].Calls + s.FUSE.Ops[OpRmdir].Calls
		deleteDurNs = s.FUSE.Ops[OpUnlink].DurationNanos + s.FUSE.Ops[OpRmdir].DurationNanos
	}

	writeF64("juicefs_uptime", s.UptimeSeconds)
	writeF64("juicefs_cpu_usage", s.CPUUsageSeconds)
	writeU64("juicefs_memory", s.MemoryBytes)
	writeU64("juicefs_fuse_ops_durations_histogram_seconds_total", s.FUSE.TotalCalls)
	writeF64("juicefs_fuse_ops_durations_histogram_seconds_sum", s.FUSE.TotalDurationSeconds)
	writeU64("juicefs_transaction_durations_histogram_seconds_total", metaCalls)
	writeF64("juicefs_transaction_durations_histogram_seconds_sum", float64(metaDurNs)*1e-9)
	writeU64("juicefs_object_request_durations_histogram_seconds_PUT_total", writeCalls)
	writeF64("juicefs_object_request_durations_histogram_seconds_PUT_sum", float64(writeDurNs)*1e-9)
	writeU64("juicefs_object_request_durations_histogram_seconds_GET_total", readCalls)
	writeF64("juicefs_object_request_durations_histogram_seconds_GET_sum", float64(readDurNs)*1e-9)
	writeU64("juicefs_object_request_durations_histogram_seconds_DELETE_total", deleteCalls)
	writeF64("juicefs_object_request_durations_histogram_seconds_DELETE_sum", float64(deleteDurNs)*1e-9)
	writeU64("juicefs_blockcache_write_hist_seconds_total", s.Stage.StagedCalls)
	writeF64("juicefs_blockcache_write_hist_seconds_sum", s.Stage.BackpressureWaitSeconds)
	writeU64("juicefs_blockcache_read_hist_seconds_total", s.ReadAhead.Hits)
	writeF64("juicefs_blockcache_read_hist_seconds_sum", s.ReadAhead.WaitSeconds)

	writeF64("bowl_client_uptime_seconds", s.UptimeSeconds)
	writeF64("bowl_client_cpu_usage_seconds", s.CPUUsageSeconds)
	writeU64("bowl_client_memory_bytes", s.MemoryBytes)

	writeU64("bowl_client_fuse_ops_total", s.FUSE.TotalCalls)
	writeU64("bowl_client_fuse_op_errors_total", s.FUSE.TotalErrors)
	writeI64("bowl_client_fuse_inflight_ops", s.FUSE.TotalInflight)
	writeF64("bowl_client_fuse_op_duration_seconds_sum", s.FUSE.TotalDurationSeconds)
	writeU64("bowl_client_fuse_read_bytes_total", s.FUSE.ReadBytes)
	writeU64("bowl_client_fuse_write_bytes_total", s.FUSE.WriteBytes)

	for i := range s.FUSE.Ops {
		op := &s.FUSE.Ops[i]
		if op.Calls == 0 && op.Inflight == 0 {
			continue
		}
		writeU64("bowl_client_fuse_op_calls_"+op.Op, op.Calls)
		writeF64("bowl_client_fuse_op_duration_seconds_sum_"+op.Op, float64(op.DurationNanos)*1e-9)
		if op.Errors > 0 {
			writeU64("bowl_client_fuse_op_errors_"+op.Op, op.Errors)
		}
	}

	writeU64("bowl_client_nscache_attr_hits_total", s.NamespaceCache.AttrHits)
	writeU64("bowl_client_nscache_attr_misses_total", s.NamespaceCache.AttrMisses)
	writeU64("bowl_client_nscache_entry_hits_total", s.NamespaceCache.EntryHits)
	writeU64("bowl_client_nscache_entry_misses_total", s.NamespaceCache.EntryMisses)
	writeU64("bowl_client_nscache_negative_entry_hits_total", s.NamespaceCache.NegativeEntryHits)
	writeU64("bowl_client_nscache_negative_entry_misses_total", s.NamespaceCache.NegativeEntryMisses)
	writeU64("bowl_client_nscache_readdir_hits_total", s.NamespaceCache.ReadDirHits)
	writeU64("bowl_client_nscache_readdir_misses_total", s.NamespaceCache.ReadDirMisses)
	writeU64("bowl_client_nscache_readlink_hits_total", s.NamespaceCache.ReadlinkHits)
	writeU64("bowl_client_nscache_readlink_misses_total", s.NamespaceCache.ReadlinkMisses)
	writeU64("bowl_client_nscache_invalidations_total", s.NamespaceCache.Invalidations)
	writeU64("bowl_client_nscache_evictions_total", s.NamespaceCache.Evictions)
	writeI64("bowl_client_nscache_entries", int64(s.NamespaceCache.Entries))

	writeU64("bowl_client_capcache_hits_total", s.CapabilityCache.Hits)
	writeU64("bowl_client_capcache_misses_total", s.CapabilityCache.Misses)
	writeU64("bowl_client_capcache_grants_total", s.CapabilityCache.Grants)
	writeU64("bowl_client_capcache_epoch_invalidations_total", s.CapabilityCache.EpochInvalidations)
	writeI64("bowl_client_capcache_entries", int64(s.CapabilityCache.Entries))

	writeU64("bowl_client_readahead_triggers_total", s.ReadAhead.Triggers)
	writeU64("bowl_client_readahead_prefetched_bytes_total", s.ReadAhead.PrefetchedBytes)
	writeU64("bowl_client_readahead_hits_total", s.ReadAhead.Hits)
	writeU64("bowl_client_readahead_hit_bytes_total", s.ReadAhead.HitBytes)
	writeU64("bowl_client_readahead_discarded_bytes_total", s.ReadAhead.DiscardedBytes)
	writeI64("bowl_client_readahead_inflight", s.ReadAhead.Inflight)
	writeU64("bowl_client_readahead_waits_total", s.ReadAhead.Waits)
	writeF64("bowl_client_readahead_wait_seconds_total", s.ReadAhead.WaitSeconds)

	writeU64("bowl_client_stage_staged_calls_total", s.Stage.StagedCalls)
	writeU64("bowl_client_stage_staged_bytes_total", s.Stage.StagedBytes)
	writeU64("bowl_client_stage_passthrough_calls_total", s.Stage.PassthroughCalls)
	writeU64("bowl_client_stage_passthrough_bytes_total", s.Stage.PassthroughBytes)
	writeU64("bowl_client_stage_upload_rpcs_total", s.Stage.UploadRPCs)
	writeU64("bowl_client_stage_upload_errors_total", s.Stage.UploadErrors)
	writeI64("bowl_client_stage_inflight", s.Stage.Inflight)
	writeI64("bowl_client_stage_pending_chunks", s.Stage.PendingChunks)
	writeU64("bowl_client_stage_used_bytes", s.Stage.UsedBytes)
	writeU64("bowl_client_stage_capacity_bytes", s.Stage.CapacityBytes)
	writeU64("bowl_client_stage_backpressure_waits_total", s.Stage.BackpressureWaits)
	writeF64("bowl_client_stage_backpressure_wait_seconds_total", s.Stage.BackpressureWaitSeconds)
	writeU64("bowl_client_stage_barrier_waits_total", s.Stage.BarrierWaits)
	writeF64("bowl_client_stage_barrier_wait_seconds_total", s.Stage.BarrierWaitSeconds)

	writeI64("bowl_client_tmpfile_active_files", int64(s.TmpFile.ActiveFiles))
	writeU64("bowl_client_tmpfile_resident_bytes", s.TmpFile.ResidentBytes)
	writeU64("bowl_client_tmpfile_promotes_total", s.TmpFile.Promotes)

	writeI64("bowl_client_open_handles_file", int64(s.Handles.OpenFiles))
	writeI64("bowl_client_open_handles_dir", int64(s.Handles.OpenDirs))

	writeU64("bowl_client_inodelock_fast_total", s.InodeLocks.FastAcquisitions)
	writeU64("bowl_client_inodelock_contended_total", s.InodeLocks.ContendedAcquisitions)
	writeF64("bowl_client_inodelock_wait_seconds_total", s.InodeLocks.WaitSeconds)
	writeI64("bowl_client_inodelock_active", int64(s.InodeLocks.ActiveEntries))

	writeI64("bowl_client_queue_orphan_inodes", int64(s.Queues.OrphanInodes))
	writeI64("bowl_client_queue_release_depth", int64(s.Queues.ReleaseQueueDepth))
	writeU64("bowl_client_queue_dirty_flush_ok_total", s.Queues.DirtyFlushOK)
	writeU64("bowl_client_queue_dirty_flush_errors_total", s.Queues.DirtyFlushErrors)
	writeU64("bowl_client_queue_orphan_reclaim_ok_total", s.Queues.OrphanReclaimOK)
	writeU64("bowl_client_queue_orphan_reclaim_errors_total", s.Queues.OrphanReclaimErrors)
	writeU64("bowl_client_queue_async_release_ok_total", s.Queues.AsyncReleaseOK)
	writeU64("bowl_client_queue_async_release_errors_total", s.Queues.AsyncReleaseErrors)
	writeI64("bowl_client_fuse_connection_waiting", int64(s.Queues.FuseWaiting))
	writeI64("bowl_client_fuse_connection_congestion_threshold", int64(s.Queues.FuseCongestionThreshold))

	return b.String()
}

func (m *Metrics) FormatStatsFile() []byte {
	snap := m.Snapshot()
	return []byte(snap.FormatStatsFile())
}
