package control

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

var _ prometheus.Collector = (*storageEngineCollector)(nil)

const (
	actorModeRW = "rw"
	actorModeRO = "ro"
)

func actorModeLabel(readOnly bool) string {
	if readOnly {
		return actorModeRO
	}
	return actorModeRW
}

var walFsyncBucketBoundsSeconds = buildWALFsyncBucketBoundsSeconds()

var controlOpenActorBuckets = []float64{
	0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025,
	0.05, 0.1, 0.25, 0.5, 1.0,
	2.5, 5.0, 10.0,
}

func buildWALFsyncBucketBoundsSeconds() []float64 {
	bounds := make([]float64, 0, len(pebble.FsyncLatencyBuckets))
	for _, ns := range pebble.FsyncLatencyBuckets {
		if math.IsInf(ns, 1) || math.IsNaN(ns) {
			continue
		}
		bounds = append(bounds, ns/float64(time.Second))
	}
	return bounds
}

type storageEngineCollector struct {
	mgr atomic.Pointer[manager]

	disksLoadedDesc *prometheus.Desc

	logicalBytesDesc         *prometheus.Desc
	inodesDesc               *prometheus.Desc
	packPhysicalBytesDesc    *prometheus.Desc
	packReferencedBytesDesc  *prometheus.Desc
	packLiveBytesDesc        *prometheus.Desc
	packLowWatermarkDesc     *prometheus.Desc
	packPendingDeadDesc      *prometheus.Desc
	packOpenFDsDesc          *prometheus.Desc
	hotCacheBytesDesc        *prometheus.Desc
	hotCacheBlocksDesc       *prometheus.Desc
	pebbleStallActiveDesc    *prometheus.Desc
	dataDirPhysicalBytesDesc *prometheus.Desc
	commitsActiveDesc        *prometheus.Desc
	commitsPhysicalBytesDesc *prometheus.Desc
	volumeTotalBytesDesc     *prometheus.Desc
	volumeAvailableBytesDesc *prometheus.Desc

	pebbleWALBytesInDesc          *prometheus.Desc
	pebbleWALBytesWrittenDesc     *prometheus.Desc
	pebbleWALFsyncDurationDesc    *prometheus.Desc
	pebbleL0SublevelsDesc         *prometheus.Desc
	pebbleL0TablesDesc            *prometheus.Desc
	pebbleReadAmpDesc             *prometheus.Desc
	pebbleCompactionDebtBytesDesc *prometheus.Desc
	pebbleCompactionsInProgDesc   *prometheus.Desc
	pebbleFlushesInProgDesc       *prometheus.Desc
	pebbleFilterHitsDesc          *prometheus.Desc
	pebbleFilterMissesDesc        *prometheus.Desc
	pebbleOpenTableItersDesc      *prometheus.Desc

	pebbleBlockCacheBytesDesc  *prometheus.Desc
	pebbleBlockCacheCountDesc  *prometheus.Desc
	pebbleBlockCacheHitsDesc   *prometheus.Desc
	pebbleBlockCacheMissesDesc *prometheus.Desc

	controlActorLoadsDesc       *prometheus.Desc
	controlActorEvictionsDesc   *prometheus.Desc
	controlCleanupErrorsDesc    *prometheus.Desc
	controlForksDesc            *prometheus.Desc
	controlCommitsReclaimedDesc *prometheus.Desc
	openActorDurVec             *prometheus.HistogramVec
	openActorOK                 prometheus.Observer
	openActorErr                prometheus.Observer
	forkStageDurVec             *prometheus.HistogramVec
	forkStageCheckpoint         prometheus.Observer
	forkStageSanitize           prometheus.Observer
	forkStageCloneDir           prometheus.Observer
	forkStageOpenChild          prometheus.Observer
	forkStageCommitTx           prometheus.Observer
	segmentReserveDurVec        *prometheus.HistogramVec
	segmentInodeOK              prometheus.Observer
	segmentInodeErr             prometheus.Observer
	segmentPackOK               prometheus.Observer
	segmentPackErr              prometheus.Observer

	actorLoadsRW     atomic.Uint64
	actorLoadsRO     atomic.Uint64
	actorEvictions   atomic.Uint64
	cleanupErrors    atomic.Uint64
	forksOK          atomic.Uint64
	forksErr         atomic.Uint64
	commitsReclaimed atomic.Uint64
}

func newStorageEngineCollector() *storageEngineCollector {
	openDurVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bowl_storage_control_open_actor_duration_seconds",
		Help:    "Duration of opening and recovering StoreActor instances on cold paths in seconds.",
		Buckets: controlOpenActorBuckets,
	}, []string{"result"})

	forkStageVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bowl_storage_control_fork_stage_duration_seconds",
		Help:    "Duration of each ForkDisk stage (checkpoint, sanitize, clone_dir, open_child, commit_tx) in seconds.",
		Buckets: controlOpenActorBuckets,
	}, []string{"stage"})

	segReserveVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bowl_storage_control_segment_reserve_duration_seconds",
		Help:    "Duration of lineage ID segment reservation RPC/DB transactions by kind and result in seconds.",
		Buckets: controlOpenActorBuckets,
	}, []string{"kind", "result"})

	return &storageEngineCollector{
		disksLoadedDesc: prometheus.NewDesc(
			"bowl_storage_disks_loaded",
			"Current number of StoreActor instances loaded in the control pool by mode.",
			[]string{"mode"},
			nil,
		),
		logicalBytesDesc: prometheus.NewDesc(
			"bowl_storage_logical_bytes",
			"Total logical file bytes across all loaded disks.",
			nil,
			nil,
		),
		inodesDesc: prometheus.NewDesc(
			"bowl_storage_inodes",
			"Total allocated inodes across all loaded disks.",
			nil,
			nil,
		),
		packPhysicalBytesDesc: prometheus.NewDesc(
			"bowl_storage_pack_physical_bytes",
			"Total deduplicated physical bytes occupied by pack files across disks and commits directories.",
			nil,
			nil,
		),
		packReferencedBytesDesc: prometheus.NewDesc(
			"bowl_storage_pack_referenced_bytes",
			"Sum of referenced pack file bytes reported across all loaded StoreActor instances (includes hardlink sharing).",
			nil,
			nil,
		),
		packLiveBytesDesc: prometheus.NewDesc(
			"bowl_storage_pack_live_bytes",
			"Total live referenced bytes inside pack files across all loaded disks.",
			nil,
			nil,
		),
		packLowWatermarkDesc: prometheus.NewDesc(
			"bowl_storage_pack_low_watermark_files",
			"Total number of sealed pack files eligible for compaction (live <= 50%) across loaded disks.",
			nil,
			nil,
		),
		packPendingDeadDesc: prometheus.NewDesc(
			"bowl_storage_pack_pending_dead_files",
			"Total number of dead pack files awaiting WAL sync barrier before removal across loaded disks.",
			nil,
			nil,
		),
		packOpenFDsDesc: prometheus.NewDesc(
			"bowl_storage_pack_open_fds",
			"Total number of open pack file descriptors held by BlobStore instances across loaded disks.",
			nil,
			nil,
		),
		hotCacheBytesDesc: prometheus.NewDesc(
			"bowl_storage_hot_cache_bytes",
			"Total bytes resident in BlobStore hot block caches across loaded disks.",
			nil,
			nil,
		),
		hotCacheBlocksDesc: prometheus.NewDesc(
			"bowl_storage_hot_cache_blocks",
			"Total block entries resident in BlobStore hot block caches across loaded disks.",
			nil,
			nil,
		),
		pebbleStallActiveDesc: prometheus.NewDesc(
			"bowl_storage_pebble_stall_active",
			"Number of loaded disks currently experiencing a Pebble write stall.",
			nil,
			nil,
		),
		dataDirPhysicalBytesDesc: prometheus.NewDesc(
			"bowl_storage_data_dir_physical_bytes",
			"Total deduplicated physical bytes across the entire storage data directory (disks and commits).",
			nil,
			nil,
		),
		commitsActiveDesc: prometheus.NewDesc(
			"bowl_storage_commits_active",
			"Current number of active fork base commit directories under commits/.",
			nil,
			nil,
		),
		commitsPhysicalBytesDesc: prometheus.NewDesc(
			"bowl_storage_commits_physical_bytes",
			"Total deduplicated physical bytes across files retained inside commits/ directories.",
			nil,
			nil,
		),
		volumeTotalBytesDesc: prometheus.NewDesc(
			"bowl_storage_volume_total_bytes",
			"Total capacity in bytes of the filesystem hosting the storage data directory.",
			nil,
			nil,
		),
		volumeAvailableBytesDesc: prometheus.NewDesc(
			"bowl_storage_volume_available_bytes",
			"Available unprivileged bytes of the filesystem hosting the storage data directory.",
			nil,
			nil,
		),
		pebbleWALBytesInDesc: prometheus.NewDesc(
			"bowl_storage_pebble_wal_bytes_in_total",
			"Total logical bytes written into Pebble WAL across loaded disks.",
			nil,
			nil,
		),
		pebbleWALBytesWrittenDesc: prometheus.NewDesc(
			"bowl_storage_pebble_wal_bytes_written_total",
			"Total physical bytes written to Pebble WAL files across loaded disks.",
			nil,
			nil,
		),
		pebbleWALFsyncDurationDesc: prometheus.NewDesc(
			"bowl_storage_pebble_wal_fsync_duration_seconds",
			"Pebble WAL fsync latency distribution in seconds aggregated across loaded disks.",
			nil,
			nil,
		),
		pebbleL0SublevelsDesc: prometheus.NewDesc(
			"bowl_storage_pebble_l0_sublevels",
			"Maximum number of Pebble L0 sublevels across loaded disks.",
			nil,
			nil,
		),
		pebbleL0TablesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_l0_tables",
			"Total number of Pebble L0 sstables across loaded disks.",
			nil,
			nil,
		),
		pebbleReadAmpDesc: prometheus.NewDesc(
			"bowl_storage_pebble_read_amp",
			"Maximum Pebble LSM read amplification factor across loaded disks.",
			nil,
			nil,
		),
		pebbleCompactionDebtBytesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_compaction_debt_bytes",
			"Total estimated Pebble compaction debt in bytes across loaded disks.",
			nil,
			nil,
		),
		pebbleCompactionsInProgDesc: prometheus.NewDesc(
			"bowl_storage_pebble_compactions_in_progress",
			"Total number of in-progress Pebble compactions across loaded disks.",
			nil,
			nil,
		),
		pebbleFlushesInProgDesc: prometheus.NewDesc(
			"bowl_storage_pebble_flushes_in_progress",
			"Total number of in-progress Pebble memtable flushes across loaded disks.",
			nil,
			nil,
		),
		pebbleFilterHitsDesc: prometheus.NewDesc(
			"bowl_storage_pebble_filter_hits_total",
			"Total number of Pebble bloom filter hits avoiding block reads across loaded disks.",
			nil,
			nil,
		),
		pebbleFilterMissesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_filter_misses_total",
			"Total number of Pebble bloom filter misses across loaded disks.",
			nil,
			nil,
		),
		pebbleOpenTableItersDesc: prometheus.NewDesc(
			"bowl_storage_pebble_open_table_iters",
			"Total number of currently open Pebble sstable iterators across loaded disks.",
			nil,
			nil,
		),
		pebbleBlockCacheBytesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_block_cache_bytes",
			"Bytes currently in use by the process-wide shared Pebble block cache.",
			nil,
			nil,
		),
		pebbleBlockCacheCountDesc: prometheus.NewDesc(
			"bowl_storage_pebble_block_cache_count",
			"Number of blocks currently cached in the process-wide shared Pebble block cache.",
			nil,
			nil,
		),
		pebbleBlockCacheHitsDesc: prometheus.NewDesc(
			"bowl_storage_pebble_block_cache_hits_total",
			"Total hits in the process-wide shared Pebble block cache.",
			nil,
			nil,
		),
		pebbleBlockCacheMissesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_block_cache_misses_total",
			"Total misses in the process-wide shared Pebble block cache.",
			nil,
			nil,
		),
		controlActorLoadsDesc: prometheus.NewDesc(
			"bowl_storage_control_actor_loads_total",
			"Total number of StoreActor instances opened and loaded into the control pool by mode.",
			[]string{"mode"},
			nil,
		),
		controlActorEvictionsDesc: prometheus.NewDesc(
			"bowl_storage_control_actor_evictions_total",
			"Total number of idle StoreActor instances evicted from the control pool.",
			nil,
			nil,
		),
		controlCleanupErrorsDesc: prometheus.NewDesc(
			"bowl_storage_control_cleanup_errors_total",
			"Total number of errors encountered while closing actors or removing disk directories during rollback, eviction, or deletion.",
			nil,
			nil,
		),
		controlForksDesc: prometheus.NewDesc(
			"bowl_storage_control_forks_total",
			"Total number of ForkDisk control operations by result.",
			[]string{"result"},
			nil,
		),
		controlCommitsReclaimedDesc: prometheus.NewDesc(
			"bowl_storage_control_commits_reclaimed_total",
			"Total number of unreferenced fork commit directories physically reclaimed by DeleteDisk.",
			nil,
			nil,
		),
		openActorDurVec:      openDurVec,
		openActorOK:          openDurVec.WithLabelValues("ok"),
		openActorErr:         openDurVec.WithLabelValues("error"),
		forkStageDurVec:      forkStageVec,
		forkStageCheckpoint:  forkStageVec.WithLabelValues("checkpoint"),
		forkStageSanitize:    forkStageVec.WithLabelValues("sanitize"),
		forkStageCloneDir:    forkStageVec.WithLabelValues("clone_dir"),
		forkStageOpenChild:   forkStageVec.WithLabelValues("open_child"),
		forkStageCommitTx:    forkStageVec.WithLabelValues("commit_tx"),
		segmentReserveDurVec: segReserveVec,
		segmentInodeOK:       segReserveVec.WithLabelValues("inode", "ok"),
		segmentInodeErr:      segReserveVec.WithLabelValues("inode", "error"),
		segmentPackOK:        segReserveVec.WithLabelValues("pack", "ok"),
		segmentPackErr:       segReserveVec.WithLabelValues("pack", "error"),
	}
}

func (c *storageEngineCollector) bind(m *manager) {
	if c == nil {
		return
	}
	c.mgr.Store(m)
}

func (c *storageEngineCollector) incActorLoad(readOnly bool) {
	if c == nil {
		return
	}
	if readOnly {
		c.actorLoadsRO.Add(1)
		return
	}
	c.actorLoadsRW.Add(1)
}

func (c *storageEngineCollector) incActorEviction() {
	if c == nil {
		return
	}
	c.actorEvictions.Add(1)
}

func (c *storageEngineCollector) incCleanupError() {
	if c == nil {
		return
	}
	c.cleanupErrors.Add(1)
}

func observeWithExemplar(ctx context.Context, obs prometheus.Observer, d time.Duration) {
	secs := max(0, d.Seconds())
	if ctx != nil {
		if sc := trace.SpanContextFromContext(ctx); sc.IsSampled() {
			if ex, ok := obs.(prometheus.ExemplarObserver); ok {
				ex.ObserveWithExemplar(secs, prometheus.Labels{"trace_id": sc.TraceID().String()})
				return
			}
		}
	}
	obs.Observe(secs)
}

func (c *storageEngineCollector) observeOpenActor(ctx context.Context, ok bool, d time.Duration) {
	if c == nil {
		return
	}
	if ok {
		observeWithExemplar(ctx, c.openActorOK, d)
		return
	}
	observeWithExemplar(ctx, c.openActorErr, d)
}

func (c *storageEngineCollector) observeForkStage(ctx context.Context, stage string, d time.Duration) {
	if c == nil {
		return
	}
	var obs prometheus.Observer
	switch stage {
	case "checkpoint":
		obs = c.forkStageCheckpoint
	case "sanitize":
		obs = c.forkStageSanitize
	case "clone_dir":
		obs = c.forkStageCloneDir
	case "open_child":
		obs = c.forkStageOpenChild
	case "commit_tx":
		obs = c.forkStageCommitTx
	default:
		return
	}
	observeWithExemplar(ctx, obs, d)
}

func (c *storageEngineCollector) observeSegmentReserve(ctx context.Context, kind string, ok bool, d time.Duration) {
	if c == nil {
		return
	}
	var obs prometheus.Observer
	switch kind {
	case "inode":
		if ok {
			obs = c.segmentInodeOK
		} else {
			obs = c.segmentInodeErr
		}
	case "pack":
		if ok {
			obs = c.segmentPackOK
		} else {
			obs = c.segmentPackErr
		}
	default:
		return
	}
	observeWithExemplar(ctx, obs, d)
}

func (c *storageEngineCollector) incForkResult(ok bool) {
	if c == nil {
		return
	}
	if ok {
		c.forksOK.Add(1)
		return
	}
	c.forksErr.Add(1)
}

func (c *storageEngineCollector) addCommitsReclaimed(n uint64) {
	if c == nil || n == 0 {
		return
	}
	c.commitsReclaimed.Add(n)
}

func (c *storageEngineCollector) Describe(ch chan<- *prometheus.Desc) {
	if c == nil {
		return
	}
	ch <- c.disksLoadedDesc

	ch <- c.logicalBytesDesc
	ch <- c.inodesDesc
	ch <- c.packPhysicalBytesDesc
	ch <- c.packReferencedBytesDesc
	ch <- c.packLiveBytesDesc
	ch <- c.packLowWatermarkDesc
	ch <- c.packPendingDeadDesc
	ch <- c.packOpenFDsDesc
	ch <- c.hotCacheBytesDesc
	ch <- c.hotCacheBlocksDesc
	ch <- c.pebbleStallActiveDesc
	ch <- c.dataDirPhysicalBytesDesc
	ch <- c.commitsActiveDesc
	ch <- c.commitsPhysicalBytesDesc
	ch <- c.volumeTotalBytesDesc
	ch <- c.volumeAvailableBytesDesc

	ch <- c.pebbleWALBytesInDesc
	ch <- c.pebbleWALBytesWrittenDesc
	ch <- c.pebbleWALFsyncDurationDesc
	ch <- c.pebbleL0SublevelsDesc
	ch <- c.pebbleL0TablesDesc
	ch <- c.pebbleReadAmpDesc
	ch <- c.pebbleCompactionDebtBytesDesc
	ch <- c.pebbleCompactionsInProgDesc
	ch <- c.pebbleFlushesInProgDesc
	ch <- c.pebbleFilterHitsDesc
	ch <- c.pebbleFilterMissesDesc
	ch <- c.pebbleOpenTableItersDesc

	ch <- c.pebbleBlockCacheBytesDesc
	ch <- c.pebbleBlockCacheCountDesc
	ch <- c.pebbleBlockCacheHitsDesc
	ch <- c.pebbleBlockCacheMissesDesc

	ch <- c.controlActorLoadsDesc
	ch <- c.controlActorEvictionsDesc
	ch <- c.controlCleanupErrorsDesc
	ch <- c.controlForksDesc
	ch <- c.controlCommitsReclaimedDesc
	c.openActorDurVec.Describe(ch)
	c.forkStageDurVec.Describe(ch)
	c.segmentReserveDurVec.Describe(ch)
}

func (c *storageEngineCollector) Collect(ch chan<- prometheus.Metric) {
	if c == nil {
		return
	}

	var (
		actors      []*store.StoreActor
		rwLoaded    int
		roLoaded    int
		cache       *pebble.Cache
		storageRoot string
	)
	if m := c.mgr.Load(); m != nil {
		m.poolMu.RLock()
		if !m.closed {
			storageRoot = m.storageRoot
			if len(m.actors) > 0 {
				actors = make([]*store.StoreActor, 0, len(m.actors))
				for _, entry := range m.actors {
					if entry == nil || entry.actor == nil {
						continue
					}
					actors = append(actors, entry.actor)
					if entry.actor.ReadOnly() {
						roLoaded++
					} else {
						rwLoaded++
					}
				}
			}
			if m.cache != nil {
				m.cache.Ref()
				cache = m.cache
			}
		}
		m.poolMu.RUnlock()
	}

	var cacheMetrics pebble.CacheMetrics
	if cache != nil {
		cacheMetrics = cache.Metrics()
		cache.Unref()
	}

	var (
		logicalBytes      uint64
		inodes            uint64
		packPhysicalBytes uint64
		packLiveBytes     uint64
		lowWatermarkPacks int
		pendingDeadPacks  int
		openPackFDs       int
		hotCacheBytes     uint64
		hotCacheBlocks    uint64
		pebbleStallActive int

		walBytesIn          uint64
		walBytesWritten     uint64
		maxL0Sublevels      int32
		l0Tables            int64
		maxReadAmp          int
		compactionDebtBytes uint64
		compactionsInProg   int64
		flushesInProg       int64
		filterHits          int64
		filterMisses        int64
		openTableIters      int64

		walFsyncCount      uint64
		walFsyncSumSeconds float64
	)

	walFsyncBuckets := make(map[float64]uint64, len(walFsyncBucketBoundsSeconds))
	for _, bound := range walFsyncBucketBoundsSeconds {
		walFsyncBuckets[bound] = 0
	}

	var dtoMetric dto.Metric
	for _, actor := range actors {
		snap := actor.MetricsSnapshot()
		if snap.Pebble == nil {

			continue
		}

		logicalBytes += snap.LogicalBytes
		inodes += snap.Inodes
		packPhysicalBytes += snap.PackPhysicalBytes
		packLiveBytes += snap.PackLiveBytes
		lowWatermarkPacks += snap.LowWatermarkPacks
		pendingDeadPacks += snap.PendingDeadPacks
		openPackFDs += snap.OpenPackFDs
		hotCacheBytes += snap.HotCacheBytes
		hotCacheBlocks += snap.HotCacheBlocks
		if snap.PebbleStallActive {
			pebbleStallActive++
		}

		pm := snap.Pebble
		walBytesIn += pm.WAL.BytesIn
		walBytesWritten += pm.WAL.BytesWritten
		if pm.Levels[0].Sublevels > maxL0Sublevels {
			maxL0Sublevels = pm.Levels[0].Sublevels
		}
		l0Tables += pm.Levels[0].TablesCount
		if ra := pm.ReadAmp(); ra > maxReadAmp {
			maxReadAmp = ra
		}
		compactionDebtBytes += pm.Compact.EstimatedDebt
		compactionsInProg += pm.Compact.NumInProgress
		flushesInProg += pm.Flush.NumInProgress
		filterHits += pm.Filter.Hits
		filterMisses += pm.Filter.Misses
		openTableIters += pm.TableIters

		if pm.LogWriter.FsyncLatency != nil {
			dtoMetric.Reset()
			if err := pm.LogWriter.FsyncLatency.Write(&dtoMetric); err == nil {
				if h := dtoMetric.GetHistogram(); h != nil {
					walFsyncCount += h.GetSampleCount()

					walFsyncSumSeconds += h.GetSampleSum() / float64(time.Second)
					for _, b := range h.GetBucket() {
						ub := b.GetUpperBound()
						if math.IsInf(ub, 1) || math.IsNaN(ub) {
							continue
						}
						secBound := ub / float64(time.Second)
						walFsyncBuckets[secBound] += b.GetCumulativeCount()
					}
				}
			}
		}
	}

	usage := scanStorageRootUsage(storageRoot)

	ch <- prometheus.MustNewConstMetric(c.disksLoadedDesc, prometheus.GaugeValue, float64(rwLoaded), actorModeRW)
	ch <- prometheus.MustNewConstMetric(c.disksLoadedDesc, prometheus.GaugeValue, float64(roLoaded), actorModeRO)

	ch <- prometheus.MustNewConstMetric(c.logicalBytesDesc, prometheus.GaugeValue, float64(logicalBytes))
	ch <- prometheus.MustNewConstMetric(c.inodesDesc, prometheus.GaugeValue, float64(inodes))
	ch <- prometheus.MustNewConstMetric(c.packPhysicalBytesDesc, prometheus.GaugeValue, float64(usage.PackPhysicalBytes))
	ch <- prometheus.MustNewConstMetric(c.packReferencedBytesDesc, prometheus.GaugeValue, float64(packPhysicalBytes))
	ch <- prometheus.MustNewConstMetric(c.packLiveBytesDesc, prometheus.GaugeValue, float64(packLiveBytes))
	ch <- prometheus.MustNewConstMetric(c.packLowWatermarkDesc, prometheus.GaugeValue, float64(lowWatermarkPacks))
	ch <- prometheus.MustNewConstMetric(c.packPendingDeadDesc, prometheus.GaugeValue, float64(pendingDeadPacks))
	ch <- prometheus.MustNewConstMetric(c.packOpenFDsDesc, prometheus.GaugeValue, float64(openPackFDs))
	ch <- prometheus.MustNewConstMetric(c.hotCacheBytesDesc, prometheus.GaugeValue, float64(hotCacheBytes))
	ch <- prometheus.MustNewConstMetric(c.hotCacheBlocksDesc, prometheus.GaugeValue, float64(hotCacheBlocks))
	ch <- prometheus.MustNewConstMetric(c.pebbleStallActiveDesc, prometheus.GaugeValue, float64(pebbleStallActive))
	ch <- prometheus.MustNewConstMetric(c.dataDirPhysicalBytesDesc, prometheus.GaugeValue, float64(usage.DataDirPhysicalBytes))
	ch <- prometheus.MustNewConstMetric(c.commitsActiveDesc, prometheus.GaugeValue, float64(usage.CommitsActive))
	ch <- prometheus.MustNewConstMetric(c.commitsPhysicalBytesDesc, prometheus.GaugeValue, float64(usage.CommitsPhysicalBytes))
	ch <- prometheus.MustNewConstMetric(c.volumeTotalBytesDesc, prometheus.GaugeValue, float64(usage.VolumeTotalBytes))
	ch <- prometheus.MustNewConstMetric(c.volumeAvailableBytesDesc, prometheus.GaugeValue, float64(usage.VolumeAvailableBytes))

	ch <- prometheus.MustNewConstMetric(c.pebbleWALBytesInDesc, prometheus.CounterValue, float64(walBytesIn))
	ch <- prometheus.MustNewConstMetric(c.pebbleWALBytesWrittenDesc, prometheus.CounterValue, float64(walBytesWritten))
	ch <- prometheus.MustNewConstHistogram(c.pebbleWALFsyncDurationDesc, walFsyncCount, walFsyncSumSeconds, walFsyncBuckets)
	ch <- prometheus.MustNewConstMetric(c.pebbleL0SublevelsDesc, prometheus.GaugeValue, float64(maxL0Sublevels))
	ch <- prometheus.MustNewConstMetric(c.pebbleL0TablesDesc, prometheus.GaugeValue, float64(l0Tables))
	ch <- prometheus.MustNewConstMetric(c.pebbleReadAmpDesc, prometheus.GaugeValue, float64(maxReadAmp))
	ch <- prometheus.MustNewConstMetric(c.pebbleCompactionDebtBytesDesc, prometheus.GaugeValue, float64(compactionDebtBytes))
	ch <- prometheus.MustNewConstMetric(c.pebbleCompactionsInProgDesc, prometheus.GaugeValue, float64(compactionsInProg))
	ch <- prometheus.MustNewConstMetric(c.pebbleFlushesInProgDesc, prometheus.GaugeValue, float64(flushesInProg))
	ch <- prometheus.MustNewConstMetric(c.pebbleFilterHitsDesc, prometheus.CounterValue, float64(max(0, filterHits)))
	ch <- prometheus.MustNewConstMetric(c.pebbleFilterMissesDesc, prometheus.CounterValue, float64(max(0, filterMisses)))
	ch <- prometheus.MustNewConstMetric(c.pebbleOpenTableItersDesc, prometheus.GaugeValue, float64(openTableIters))

	ch <- prometheus.MustNewConstMetric(c.pebbleBlockCacheBytesDesc, prometheus.GaugeValue, float64(max(0, cacheMetrics.Size)))
	ch <- prometheus.MustNewConstMetric(c.pebbleBlockCacheCountDesc, prometheus.GaugeValue, float64(max(0, cacheMetrics.Count)))
	ch <- prometheus.MustNewConstMetric(c.pebbleBlockCacheHitsDesc, prometheus.CounterValue, float64(max(0, cacheMetrics.Hits)))
	ch <- prometheus.MustNewConstMetric(c.pebbleBlockCacheMissesDesc, prometheus.CounterValue, float64(max(0, cacheMetrics.Misses)))

	ch <- prometheus.MustNewConstMetric(c.controlActorLoadsDesc, prometheus.CounterValue, float64(c.actorLoadsRW.Load()), actorModeRW)
	ch <- prometheus.MustNewConstMetric(c.controlActorLoadsDesc, prometheus.CounterValue, float64(c.actorLoadsRO.Load()), actorModeRO)
	ch <- prometheus.MustNewConstMetric(c.controlActorEvictionsDesc, prometheus.CounterValue, float64(c.actorEvictions.Load()))
	ch <- prometheus.MustNewConstMetric(c.controlCleanupErrorsDesc, prometheus.CounterValue, float64(c.cleanupErrors.Load()))
	ch <- prometheus.MustNewConstMetric(c.controlForksDesc, prometheus.CounterValue, float64(c.forksOK.Load()), "ok")
	ch <- prometheus.MustNewConstMetric(c.controlForksDesc, prometheus.CounterValue, float64(c.forksErr.Load()), "error")
	ch <- prometheus.MustNewConstMetric(c.controlCommitsReclaimedDesc, prometheus.CounterValue, float64(c.commitsReclaimed.Load()))
	c.openActorDurVec.Collect(ch)
	c.forkStageDurVec.Collect(ch)
	c.segmentReserveDurVec.Collect(ch)
}
