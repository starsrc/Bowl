package projection

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/starsrc/bowl/services/storage/internal/engine"
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
}

func (c *StorageMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	if c == nil {
		return
	}

	var (
		stores []*engine.DiskStore
		cache  *pebble.Cache
	)
	if m := c.mgr; m != nil {
		m.poolMu.RLock()
		if !m.closed {
			if len(m.disks) > 0 {
				stores = make([]*engine.DiskStore, 0, len(m.disks))
				for _, entry := range m.disks {
					if entry != nil && entry.store != nil {
						stores = append(stores, entry.store)
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
	)

	for _, ds := range stores {
		snap := ds.MetricsSnapshot()
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

	ch <- prometheus.MustNewConstMetric(c.disksLoadedDesc, prometheus.GaugeValue, float64(len(stores)))
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
}
