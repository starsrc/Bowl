package store

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

var _ prometheus.Collector = (*Metrics)(nil)

var FineDurationBuckets = []float64{
	0.000005, 0.00001, 0.000025, 0.00005, 0.0001,
	0.00025, 0.0005, 0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05, 0.1, 0.25,
	0.5, 1.0, 2.5, 5.0, 10.0,
}

type blobWriteMode int

const (
	writeModeFullBlock blobWriteMode = iota
	writeModeExtendTail
	writeModeRMW
	writeModeZeroElide
	writeModeFallocZero
	writeModeCount
)

var blobWriteModeNames = [writeModeCount]string{
	writeModeFullBlock:  "full_block",
	writeModeExtendTail: "extend_tail",
	writeModeRMW:        "rmw",
	writeModeZeroElide:  "zero_elide",
	writeModeFallocZero: "falloc_zero",
}

type syncTarget int

const (
	syncTargetPackData syncTarget = iota
	syncTargetPacksDir
	syncTargetCount
)

var syncTargetNames = [syncTargetCount]string{
	syncTargetPackData: "pack_data",
	syncTargetPacksDir: "packs_dir",
}

type syncTrigger int

const (
	syncTriggerFsyncBarrier syncTrigger = iota
	syncTriggerBgRotate
	syncTriggerFlushWAL
	syncTriggerCount
)

var syncTriggerNames = [syncTriggerCount]string{
	syncTriggerFsyncBarrier: "fsync_barrier",
	syncTriggerBgRotate:     "bg_rotate",
	syncTriggerFlushWAL:     "flush_wal",
}

type readOpKind int

const (
	readOpRead readOpKind = iota
	readOpLookup
	readOpGetAttr
	readOpReadDir
	readOpReadDirPlus
	readOpReadlink
	readOpGetXattr
	readOpListXattr
	readOpStatFS
	readOpCount
)

var readOpNames = [readOpCount]string{
	readOpRead:        "read",
	readOpLookup:      "lookup",
	readOpGetAttr:     "getattr",
	readOpReadDir:     "readdir",
	readOpReadDirPlus: "readdirplus",
	readOpReadlink:    "readlink",
	readOpGetXattr:    "getxattr",
	readOpListXattr:   "listxattr",
	readOpStatFS:      "statfs",
}

type ActorMetricsSnapshot struct {
	DiskID            string
	ReadOnly          bool
	LogicalBytes      uint64
	Inodes            uint64
	MaxBytes          uint64
	MaxInodes         uint64
	PackPhysicalBytes uint64
	PackLiveBytes     uint64
	LowWatermarkPacks int
	PendingDeadPacks  int
	OpenPackFDs       int
	HotCacheBytes     uint64
	HotCacheBlocks    uint64
	PebbleStallActive bool
	Pebble            *pebble.Metrics
}

func (s *StoreActor) MetricsSnapshot() ActorMetricsSnapshot {
	if s == nil {
		return ActorMetricsSnapshot{}
	}
	snap := ActorMetricsSnapshot{
		DiskID:            s.diskID,
		ReadOnly:          s.readOnly,
		LogicalBytes:      s.atomicLogicalBytes.Load(),
		Inodes:            s.atomicInodes.Load(),
		MaxBytes:          s.maxBytes.Load(),
		MaxInodes:         s.maxInodes.Load(),
		PackPhysicalBytes: s.atomicPackPhysicalBytes.Load(),
		PackLiveBytes:     s.atomicPackLiveBytes.Load(),
		LowWatermarkPacks: int(s.atomicLowWatermarkPacks.Load()),
		PendingDeadPacks:  int(s.atomicPendingDeadPacks.Load()),
		PebbleStallActive: s.pebbleStallCount.Load() > 0,
	}

	unlock, err := s.enter()
	if err != nil {
		return snap
	}
	defer unlock()

	if s.blobs != nil {
		snap.OpenPackFDs, snap.HotCacheBytes, snap.HotCacheBlocks = s.blobs.snapshotStats()
	}
	if s.db != nil {
		snap.Pebble = s.db.Metrics()
	}
	return snap
}

type Metrics struct {
	actorQueueWait     prometheus.Histogram
	actorTickBatchSize prometheus.Histogram
	actorTickPhaseVec  *prometheus.HistogramVec
	inodeLockWait      prometheus.Histogram
	blobRMWReadDur     prometheus.Histogram
	blobMuWait         prometheus.Histogram
	blobPwriteDur      prometheus.Histogram
	blobPreadDur       prometheus.Histogram
	blobSyncDurVec     *prometheus.HistogramVec
	readOpDurVec       *prometheus.HistogramVec
	kvReadDurVec       *prometheus.HistogramVec

	phaseAValidate       prometheus.Observer
	phaseBMutate         prometheus.Observer
	phasePackSyncBarrier prometheus.Observer
	phaseWALApplySync    prometheus.Observer
	phaseWALApplyNoSync  prometheus.Observer
	phaseDeadPackRemove  prometheus.Observer
	phaseCheckpoint      prometheus.Observer
	blobSyncObservers    [syncTargetCount][syncTriggerCount]prometheus.Observer
	readOpObservers      [readOpCount]prometheus.Observer
	kvPointGetObserver   prometheus.Observer
	kvPrefixScanObserver prometheus.Observer

	writeBlocksDesc       *prometheus.Desc
	writeBytesDesc        *prometheus.Desc
	readSourceDesc        *prometheus.Desc
	hotCacheEvictionsDesc *prometheus.Desc
	epochFencedDesc       *prometheus.Desc
	pebbleStallEventsDesc *prometheus.Desc
	pebbleDiskSlowDesc    *prometheus.Desc
	bgSyncErrorsDesc      *prometheus.Desc
	packRemoveErrorsDesc  *prometheus.Desc

	writeBlocks [writeModeCount]atomic.Uint64
	writeBytes  [writeModeCount]atomic.Uint64

	readHotCache      atomic.Uint64
	readDiskPack      atomic.Uint64
	hotCacheEvictions atomic.Uint64
	epochFenced       atomic.Uint64
	pebbleStallEvents atomic.Uint64
	pebbleDiskSlow    atomic.Uint64
	bgSyncErrors      atomic.Uint64
	packRemoveErrors  atomic.Uint64
}

func NewMetrics() *Metrics {
	phaseVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bowl_storage_actor_tick_phase_duration_seconds",
		Help:    "Duration of StoreActor group-commit tick phases in seconds.",
		Buckets: FineDurationBuckets,
	}, []string{"phase"})

	syncDurVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bowl_storage_blob_sync_duration_seconds",
		Help:    "Duration of pack file fdatasync and packs directory fsync operations in seconds.",
		Buckets: FineDurationBuckets,
	}, []string{"target", "trigger"})

	readOpVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bowl_storage_read_op_duration_seconds",
		Help:    "Duration of read-only StoreActor operations in seconds by operation.",
		Buckets: FineDurationBuckets,
	}, []string{"op"})

	kvReadVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bowl_storage_kv_read_duration_seconds",
		Help:    "Duration of Pebble KV point lookups and prefix scans on read paths in seconds.",
		Buckets: FineDurationBuckets,
	}, []string{"op"})

	m := &Metrics{
		actorQueueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "bowl_storage_actor_queue_wait_seconds",
			Help:    "Time commands spend waiting in the StoreActor MPSC queue before tick processing.",
			Buckets: FineDurationBuckets,
		}),
		actorTickBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "bowl_storage_actor_tick_batch_size",
			Help:    "Number of commands drained and processed per StoreActor group-commit tick.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
		}),
		actorTickPhaseVec: phaseVec,
		inodeLockWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "bowl_storage_inode_lock_wait_seconds",
			Help:    "Contended wait duration when acquiring per-inode serialization locks.",
			Buckets: FineDurationBuckets,
		}),
		blobRMWReadDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "bowl_storage_blob_rmw_read_duration_seconds",
			Help:    "Duration of reading existing blocks during read-modify-write partial updates.",
			Buckets: FineDurationBuckets,
		}),
		blobMuWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "bowl_storage_blob_mu_wait_seconds",
			Help:    "Wait duration to acquire BlobStore active pack mutex.",
			Buckets: FineDurationBuckets,
		}),
		blobPwriteDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "bowl_storage_blob_pwrite_duration_seconds",
			Help:    "Duration of pwrite (WriteAt) system calls to active pack files.",
			Buckets: FineDurationBuckets,
		}),
		blobPreadDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "bowl_storage_blob_pread_duration_seconds",
			Help:    "Duration of pread (ReadAt) system calls to pack files on cold block reads.",
			Buckets: FineDurationBuckets,
		}),
		blobSyncDurVec: syncDurVec,
		readOpDurVec:   readOpVec,
		kvReadDurVec:   kvReadVec,

		phaseAValidate:       phaseVec.WithLabelValues("phase_a_validate"),
		phaseBMutate:         phaseVec.WithLabelValues("phase_b_mutate"),
		phasePackSyncBarrier: phaseVec.WithLabelValues("pack_sync_barrier"),
		phaseWALApplySync:    phaseVec.WithLabelValues("wal_apply_sync"),
		phaseWALApplyNoSync:  phaseVec.WithLabelValues("wal_apply_nosync"),
		phaseDeadPackRemove:  phaseVec.WithLabelValues("dead_pack_remove"),
		phaseCheckpoint:      phaseVec.WithLabelValues("checkpoint"),

		kvPointGetObserver:   kvReadVec.WithLabelValues("point_get"),
		kvPrefixScanObserver: kvReadVec.WithLabelValues("prefix_scan"),

		writeBlocksDesc: prometheus.NewDesc(
			"bowl_storage_blob_write_blocks_total",
			"Total number of 256 KiB grid logical blocks processed on write paths by mode.",
			[]string{"mode"},
			nil,
		),
		writeBytesDesc: prometheus.NewDesc(
			"bowl_storage_blob_write_bytes_total",
			"Total bytes written or elided across 256 KiB grid blocks on write paths by mode.",
			[]string{"mode"},
			nil,
		),
		readSourceDesc: prometheus.NewDesc(
			"bowl_storage_blob_read_source_total",
			"Total number of BlobStore block slice reads served by source.",
			[]string{"source"},
			nil,
		),
		hotCacheEvictionsDesc: prometheus.NewDesc(
			"bowl_storage_hot_cache_evictions_total",
			"Total number of blocks evicted from the sharded 64 MiB hot block cache.",
			nil,
			nil,
		),
		epochFencedDesc: prometheus.NewDesc(
			"bowl_storage_epoch_fenced_total",
			"Total number of write commands rejected due to stale writer epoch fencing.",
			nil,
			nil,
		),
		pebbleStallEventsDesc: prometheus.NewDesc(
			"bowl_storage_pebble_stall_events_total",
			"Total number of Pebble write stall begin events across loaded disks.",
			nil,
			nil,
		),
		pebbleDiskSlowDesc: prometheus.NewDesc(
			"bowl_storage_pebble_disk_slow_total",
			"Total number of slow disk write operations detected by Pebble VFS health checks.",
			nil,
			nil,
		),
		bgSyncErrorsDesc: prometheus.NewDesc(
			"bowl_storage_bg_sync_errors_total",
			"Total number of background rotated pack fdatasync errors.",
			nil,
			nil,
		),
		packRemoveErrorsDesc: prometheus.NewDesc(
			"bowl_storage_pack_remove_errors_total",
			"Total number of dead pack file removal errors (excluding ENOENT).",
			nil,
			nil,
		),
	}

	for target := range syncTargetCount {
		for trigger := range syncTriggerCount {
			m.blobSyncObservers[target][trigger] = syncDurVec.WithLabelValues(
				syncTargetNames[target],
				syncTriggerNames[trigger],
			)
		}
	}
	for op := range readOpCount {
		m.readOpObservers[op] = readOpVec.WithLabelValues(readOpNames[op])
	}

	return m
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if m == nil {
		return
	}
	m.actorQueueWait.Describe(ch)
	m.actorTickBatchSize.Describe(ch)
	m.actorTickPhaseVec.Describe(ch)
	m.inodeLockWait.Describe(ch)
	m.blobRMWReadDur.Describe(ch)
	m.blobMuWait.Describe(ch)
	m.blobPwriteDur.Describe(ch)
	m.blobPreadDur.Describe(ch)
	m.blobSyncDurVec.Describe(ch)
	m.readOpDurVec.Describe(ch)
	m.kvReadDurVec.Describe(ch)

	ch <- m.writeBlocksDesc
	ch <- m.writeBytesDesc
	ch <- m.readSourceDesc
	ch <- m.hotCacheEvictionsDesc
	ch <- m.epochFencedDesc
	ch <- m.pebbleStallEventsDesc
	ch <- m.pebbleDiskSlowDesc
	ch <- m.bgSyncErrorsDesc
	ch <- m.packRemoveErrorsDesc
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m == nil {
		return
	}
	m.actorQueueWait.Collect(ch)
	m.actorTickBatchSize.Collect(ch)
	m.actorTickPhaseVec.Collect(ch)
	m.inodeLockWait.Collect(ch)
	m.blobRMWReadDur.Collect(ch)
	m.blobMuWait.Collect(ch)
	m.blobPwriteDur.Collect(ch)
	m.blobPreadDur.Collect(ch)
	m.blobSyncDurVec.Collect(ch)
	m.readOpDurVec.Collect(ch)
	m.kvReadDurVec.Collect(ch)

	for mode := range writeModeCount {
		label := blobWriteModeNames[mode]
		ch <- prometheus.MustNewConstMetric(
			m.writeBlocksDesc,
			prometheus.CounterValue,
			float64(m.writeBlocks[mode].Load()),
			label,
		)
		ch <- prometheus.MustNewConstMetric(
			m.writeBytesDesc,
			prometheus.CounterValue,
			float64(m.writeBytes[mode].Load()),
			label,
		)
	}

	ch <- prometheus.MustNewConstMetric(
		m.readSourceDesc,
		prometheus.CounterValue,
		float64(m.readHotCache.Load()),
		"hot_cache",
	)
	ch <- prometheus.MustNewConstMetric(
		m.readSourceDesc,
		prometheus.CounterValue,
		float64(m.readDiskPack.Load()),
		"disk_pack",
	)
	ch <- prometheus.MustNewConstMetric(
		m.hotCacheEvictionsDesc,
		prometheus.CounterValue,
		float64(m.hotCacheEvictions.Load()),
	)
	ch <- prometheus.MustNewConstMetric(
		m.epochFencedDesc,
		prometheus.CounterValue,
		float64(m.epochFenced.Load()),
	)
	ch <- prometheus.MustNewConstMetric(
		m.pebbleStallEventsDesc,
		prometheus.CounterValue,
		float64(m.pebbleStallEvents.Load()),
	)
	ch <- prometheus.MustNewConstMetric(
		m.pebbleDiskSlowDesc,
		prometheus.CounterValue,
		float64(m.pebbleDiskSlow.Load()),
	)
	ch <- prometheus.MustNewConstMetric(
		m.bgSyncErrorsDesc,
		prometheus.CounterValue,
		float64(m.bgSyncErrors.Load()),
	)
	ch <- prometheus.MustNewConstMetric(
		m.packRemoveErrorsDesc,
		prometheus.CounterValue,
		float64(m.packRemoveErrors.Load()),
	)
}

func (m *Metrics) observeQueueWait(d time.Duration) {
	if m == nil {
		return
	}
	m.actorQueueWait.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeTickBatchSize(n int) {
	if m == nil {
		return
	}
	m.actorTickBatchSize.Observe(float64(n))
}

func (m *Metrics) observePhaseAValidate(d time.Duration) {
	if m == nil {
		return
	}
	m.phaseAValidate.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observePhaseBMutate(d time.Duration) {
	if m == nil {
		return
	}
	m.phaseBMutate.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observePhasePackSyncBarrier(d time.Duration) {
	if m == nil {
		return
	}
	m.phasePackSyncBarrier.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observePhaseWALApplySync(d time.Duration) {
	if m == nil {
		return
	}
	m.phaseWALApplySync.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observePhaseWALApplyNoSync(d time.Duration) {
	if m == nil {
		return
	}
	m.phaseWALApplyNoSync.Observe(max(0, d.Seconds()))
}

func observeWithExemplar(obs prometheus.Observer, d time.Duration, sc trace.SpanContext) {
	v := max(0, d.Seconds())
	if sc.IsSampled() {
		if ex, ok := obs.(prometheus.ExemplarObserver); ok {
			ex.ObserveWithExemplar(v, prometheus.Labels{"trace_id": sc.TraceID().String()})
			return
		}
	}
	obs.Observe(v)
}

func (m *Metrics) observePhaseCheckpoint(d time.Duration, sc trace.SpanContext) {
	if m == nil {
		return
	}
	observeWithExemplar(m.phaseCheckpoint, d, sc)
}

func (m *Metrics) observePhaseDeadPackRemove(d time.Duration) {
	if m == nil {
		return
	}
	m.phaseDeadPackRemove.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeInodeLockWait(d time.Duration) {
	if m == nil {
		return
	}
	m.inodeLockWait.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeReadOp(op readOpKind, d time.Duration) {
	if m == nil || op < 0 || op >= readOpCount {
		return
	}
	m.readOpObservers[op].Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeKVPointGet(d time.Duration) {
	if m == nil {
		return
	}
	m.kvPointGetObserver.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeKVPrefixScan(d time.Duration) {
	if m == nil {
		return
	}
	m.kvPrefixScanObserver.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeBlobPread(d time.Duration) {
	if m == nil {
		return
	}
	m.blobPreadDur.Observe(max(0, d.Seconds()))
}

func (m *Metrics) recordBlobWrite(mode blobWriteMode, blocks, bytes uint64) {
	if m == nil || mode < 0 || mode >= writeModeCount {
		return
	}
	if blocks > 0 {
		m.writeBlocks[mode].Add(blocks)
	}
	if bytes > 0 {
		m.writeBytes[mode].Add(bytes)
	}
}

func (m *Metrics) observeBlobRMWRead(d time.Duration) {
	if m == nil {
		return
	}
	m.blobRMWReadDur.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeBlobMuWait(d time.Duration) {
	if m == nil {
		return
	}
	m.blobMuWait.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeBlobPwrite(d time.Duration) {
	if m == nil {
		return
	}
	m.blobPwriteDur.Observe(max(0, d.Seconds()))
}

func (m *Metrics) observeBlobSync(target syncTarget, trigger syncTrigger, d time.Duration) {
	if m == nil || target < 0 || target >= syncTargetCount || trigger < 0 || trigger >= syncTriggerCount {
		return
	}
	m.blobSyncObservers[target][trigger].Observe(max(0, d.Seconds()))
}

func (m *Metrics) incReadHotCache() {
	if m == nil {
		return
	}
	m.readHotCache.Add(1)
}

func (m *Metrics) addReadDiskPack(n uint64) {
	if m == nil || n == 0 {
		return
	}
	m.readDiskPack.Add(n)
}

func (m *Metrics) addHotCacheEvictions(n uint64) {
	if m == nil || n == 0 {
		return
	}
	m.hotCacheEvictions.Add(n)
}

func (m *Metrics) incEpochFenced() {
	if m == nil {
		return
	}
	m.epochFenced.Add(1)
}

func (m *Metrics) incPebbleStallEvents() {
	if m == nil {
		return
	}
	m.pebbleStallEvents.Add(1)
}

func (m *Metrics) incPebbleDiskSlow() {
	if m == nil {
		return
	}
	m.pebbleDiskSlow.Add(1)
}

func (m *Metrics) incBgSyncErrors() {
	if m == nil {
		return
	}
	m.bgSyncErrors.Add(1)
}

func (m *Metrics) incPackRemoveErrors() {
	if m == nil {
		return
	}
	m.packRemoveErrors.Add(1)
}
