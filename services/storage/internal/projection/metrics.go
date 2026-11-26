package projection

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
)

var _ prometheus.Collector = (*StorageMetricsCollector)(nil)

type StorageMetricsCollector struct {
	mgr *ControlManager

	disksLoadedDesc               *prometheus.Desc
	logicalBytesDesc              *prometheus.Desc
	packPhysicalBytesDesc         *prometheus.Desc
	inodesDesc                    *prometheus.Desc
	branchActorsLoadedDesc        *prometheus.Desc
	pebbleWALBytesInDesc          *prometheus.Desc
	pebbleWALBytesWrittenDesc     *prometheus.Desc
	pebbleL0SublevelsDesc         *prometheus.Desc
	pebbleL0TablesDesc            *prometheus.Desc
	pebbleReadAmpDesc             *prometheus.Desc
	pebbleCompactionDebtBytesDesc *prometheus.Desc
	pebbleCompactionsInProgDesc   *prometheus.Desc
	pebbleFlushesInProgDesc       *prometheus.Desc
	pebbleFilterHitsDesc          *prometheus.Desc
	pebbleFilterMissesDesc        *prometheus.Desc
	pebbleOpenTableItersDesc      *prometheus.Desc
	pebbleBlockCacheBytesDesc     *prometheus.Desc
	pebbleBlockCacheCountDesc     *prometheus.Desc
	pebbleBlockCacheHitsDesc      *prometheus.Desc
	pebbleBlockCacheMissesDesc    *prometheus.Desc

	projectionDirtyDisksDesc        *prometheus.Desc
	projectionDirtyCatalogDisksDesc *prometheus.Desc
	projectionSyncWritesDesc        *prometheus.Desc
	projectionSyncWriteErrorsDesc   *prometheus.Desc
	projectionSyncWriteSkipsDesc    *prometheus.Desc
	projectionReadFallbacksDesc     *prometheus.Desc
	projectionReconcileDisksDesc    *prometheus.Desc
	projectionReconcileErrorsDesc   *prometheus.Desc
	projectionBatchFlushErrorsDesc  *prometheus.Desc
	projectionIdleEvictionsDesc     *prometheus.Desc
	projectionDiskOpensDesc         *prometheus.Desc
}

func NewStorageMetricsCollector(mgr *ControlManager) *StorageMetricsCollector {
	return &StorageMetricsCollector{
		mgr: mgr,
		disksLoadedDesc: prometheus.NewDesc(
			"bowl_storage_disks_loaded",
			"Current number of open DiskStore instances loaded in the projection control manager.",
			nil,
			nil,
		),
		logicalBytesDesc: prometheus.NewDesc(
			"bowl_storage_logical_bytes",
			"Total used bytes across all loaded disks.",
			nil,
			nil,
		),
		packPhysicalBytesDesc: prometheus.NewDesc(
			"bowl_storage_pack_physical_bytes",
			"Total physical pack file bytes across all loaded disks.",
			nil,
			nil,
		),
		inodesDesc: prometheus.NewDesc(
			"bowl_storage_inodes",
			"Total allocated inodes across all loaded disks.",
			nil,
			nil,
		),
		branchActorsLoadedDesc: prometheus.NewDesc(
			"bowl_storage_branch_actors_loaded",
			"Total active branch actors across all loaded disks.",
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
			"Total number of Pebble bloom filter hits across loaded disks.",
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
		projectionDirtyDisksDesc: prometheus.NewDesc(
			"bowl_storage_projection_dirty_disks",
			"Current number of dirty disks awaiting projection reconciliation.",
			nil,
			nil,
		),
		projectionDirtyCatalogDisksDesc: prometheus.NewDesc(
			"bowl_storage_projection_dirty_catalog_disks",
			"Current number of dirty catalog disks awaiting projection reconciliation.",
			nil,
			nil,
		),
		projectionSyncWritesDesc: prometheus.NewDesc(
			"bowl_storage_projection_sync_writes_total",
			"Total synchronous projection writes that succeeded.",
			nil,
			nil,
		),
		projectionSyncWriteErrorsDesc: prometheus.NewDesc(
			"bowl_storage_projection_sync_write_errors_total",
			"Total synchronous projection writes that failed due to context or store errors.",
			nil,
			nil,
		),
		projectionSyncWriteSkipsDesc: prometheus.NewDesc(
			"bowl_storage_projection_sync_write_skips_total",
			"Total synchronous projection writes skipped because store is nil or disk is already dirty.",
			nil,
			nil,
		),
		projectionReadFallbacksDesc: prometheus.NewDesc(
			"bowl_storage_projection_read_fallbacks_total",
			"Total projection control-plane reads served via Pebble fallback.",
			nil,
			nil,
		),
		projectionReconcileDisksDesc: prometheus.NewDesc(
			"bowl_storage_projection_reconcile_disks_total",
			"Total disk projection reconciliations executed and succeeded.",
			nil,
			nil,
		),
		projectionReconcileErrorsDesc: prometheus.NewDesc(
			"bowl_storage_projection_reconcile_errors_total",
			"Total disk projection reconciliation failures excluding standalone mode and shutdown cancellation.",
			nil,
			nil,
		),
		projectionBatchFlushErrorsDesc: prometheus.NewDesc(
			"bowl_storage_projection_batch_flush_errors_total",
			"Total failed usage or renew projection batch flushes excluding standalone mode and shutdown cancellation.",
			nil,
			nil,
		),
		projectionIdleEvictionsDesc: prometheus.NewDesc(
			"bowl_storage_projection_idle_evictions_total",
			"Total idle disks evicted from the control manager open pool.",
			nil,
			nil,
		),
		projectionDiskOpensDesc: prometheus.NewDesc(
			"bowl_storage_projection_disk_opens_total",
			"Total Pebble DiskStore opens performed by the control manager.",
			nil,
			nil,
		),
	}
}

func (c *StorageMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	if c == nil {
		return
	}
	ch <- c.disksLoadedDesc
	ch <- c.logicalBytesDesc
	ch <- c.packPhysicalBytesDesc
	ch <- c.inodesDesc
	ch <- c.branchActorsLoadedDesc
	ch <- c.pebbleWALBytesInDesc
	ch <- c.pebbleWALBytesWrittenDesc
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
	ch <- c.projectionDirtyDisksDesc
	ch <- c.projectionDirtyCatalogDisksDesc
	ch <- c.projectionSyncWritesDesc
	ch <- c.projectionSyncWriteErrorsDesc
	ch <- c.projectionSyncWriteSkipsDesc
	ch <- c.projectionReadFallbacksDesc
	ch <- c.projectionReconcileDisksDesc
	ch <- c.projectionReconcileErrorsDesc
	ch <- c.projectionBatchFlushErrorsDesc
	ch <- c.projectionIdleEvictionsDesc
	ch <- c.projectionDiskOpensDesc
}

func (c *StorageMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	if c == nil {
		return
	}

	var (
		cache               *pebble.Cache
		cacheMetrics        pebble.CacheMetrics
		disksLoaded         int
		logicalBytes        uint64
		packPhysicalBytes   uint64
		inodes              uint64
		branchActors        int
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
		dirtyDisks          int
		dirtyCatalogDisks   int
		syncWrites          uint64
		syncWriteErrors     uint64
		syncWriteSkips      uint64
		readFallbacks       uint64
		reconcileDisks      uint64
		reconcileErrors     uint64
		batchFlushErrors    uint64
		idleEvictions       uint64
		diskOpens           int64
	)

	if m := c.mgr; m != nil {
		if m.queue != nil {
			dirtyDisks, dirtyCatalogDisks = m.queue.dirtyCounts()
			batchFlushErrors = m.queue.batchFlushErrors.Load()
		}
		syncWrites = m.syncWrites.Load()
		syncWriteErrors = m.syncWriteErrors.Load()
		syncWriteSkips = m.syncWriteSkips.Load()
		readFallbacks = m.readFallbacks.Load()
		reconcileDisks = m.reconcileDisks.Load()
		reconcileErrors = m.reconcileErrors.Load()
		idleEvictions = m.idleEvictions.Load()
		diskOpens = m.openCount.Load()
		m.slots[0].opMu.RLock()
		if !m.closed.Load() && m.cache != nil {
			m.cache.Ref()
			cache = m.cache
		}
		m.slots[0].opMu.RUnlock()

		if cache != nil {
			defer cache.Unref()
		}

		for i := range m.slots {
			if entries := m.slots[i].disks.Load(); entries == nil || len(*entries) == 0 {
				continue
			}
			m.slots[i].opMu.RLock()
			if !m.closed.Load() {
				if entries := m.slots[i].disks.Load(); entries != nil {
					for _, item := range *entries {
						if item.entry == nil || item.entry.store == nil {
							continue
						}
						disksLoaded++
						snap := item.entry.store.MetricsSnapshot()
						logicalBytes += snap.UsedBytes
						packPhysicalBytes += snap.PackPhysicalBytes
						inodes += snap.Inodes
						branchActors += snap.ActiveBranchActors
						if pm := snap.Pebble; pm != nil {
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
						}
					}
				}
			}
			m.slots[i].opMu.RUnlock()
		}

		if cache != nil {
			cacheMetrics = cache.Metrics()
		}
	}

	ch <- prometheus.MustNewConstMetric(c.disksLoadedDesc, prometheus.GaugeValue, float64(disksLoaded))
	ch <- prometheus.MustNewConstMetric(c.logicalBytesDesc, prometheus.GaugeValue, float64(logicalBytes))
	ch <- prometheus.MustNewConstMetric(c.packPhysicalBytesDesc, prometheus.GaugeValue, float64(packPhysicalBytes))
	ch <- prometheus.MustNewConstMetric(c.inodesDesc, prometheus.GaugeValue, float64(inodes))
	ch <- prometheus.MustNewConstMetric(c.branchActorsLoadedDesc, prometheus.GaugeValue, float64(branchActors))
	ch <- prometheus.MustNewConstMetric(c.pebbleWALBytesInDesc, prometheus.CounterValue, float64(walBytesIn))
	ch <- prometheus.MustNewConstMetric(c.pebbleWALBytesWrittenDesc, prometheus.CounterValue, float64(walBytesWritten))
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
	ch <- prometheus.MustNewConstMetric(c.projectionDirtyDisksDesc, prometheus.GaugeValue, float64(dirtyDisks))
	ch <- prometheus.MustNewConstMetric(c.projectionDirtyCatalogDisksDesc, prometheus.GaugeValue, float64(dirtyCatalogDisks))
	ch <- prometheus.MustNewConstMetric(c.projectionSyncWritesDesc, prometheus.CounterValue, float64(syncWrites))
	ch <- prometheus.MustNewConstMetric(c.projectionSyncWriteErrorsDesc, prometheus.CounterValue, float64(syncWriteErrors))
	ch <- prometheus.MustNewConstMetric(c.projectionSyncWriteSkipsDesc, prometheus.CounterValue, float64(syncWriteSkips))
	ch <- prometheus.MustNewConstMetric(c.projectionReadFallbacksDesc, prometheus.CounterValue, float64(readFallbacks))
	ch <- prometheus.MustNewConstMetric(c.projectionReconcileDisksDesc, prometheus.CounterValue, float64(reconcileDisks))
	ch <- prometheus.MustNewConstMetric(c.projectionReconcileErrorsDesc, prometheus.CounterValue, float64(reconcileErrors))
	ch <- prometheus.MustNewConstMetric(c.projectionBatchFlushErrorsDesc, prometheus.CounterValue, float64(batchFlushErrors))
	ch <- prometheus.MustNewConstMetric(c.projectionIdleEvictionsDesc, prometheus.CounterValue, float64(idleEvictions))
	ch <- prometheus.MustNewConstMetric(c.projectionDiskOpensDesc, prometheus.CounterValue, float64(max(0, diskOpens)))
}
