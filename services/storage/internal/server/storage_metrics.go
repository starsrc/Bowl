package server

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

var _ prometheus.Collector = (*StorageMetricsCollector)(nil)

type StorageMetricsCollector struct {
	node *engine.NodeStore

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

func NewStorageMetricsCollector(node *engine.NodeStore) *StorageMetricsCollector {
	return &StorageMetricsCollector{
		node: node,
		disksLoadedDesc: prometheus.NewDesc(
			"bowl_storage_disks_loaded",
			"Current number of open DiskStore instances loaded in the node store.",
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
			"Total logical bytes written into the shared Pebble WAL.",
			nil,
			nil,
		),
		pebbleWALBytesWrittenDesc: prometheus.NewDesc(
			"bowl_storage_pebble_wal_bytes_written_total",
			"Total physical bytes written to the shared Pebble WAL files.",
			nil,
			nil,
		),
		pebbleL0SublevelsDesc: prometheus.NewDesc(
			"bowl_storage_pebble_l0_sublevels",
			"Current number of Pebble L0 sublevels in the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleL0TablesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_l0_tables",
			"Current number of Pebble L0 sstables in the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleReadAmpDesc: prometheus.NewDesc(
			"bowl_storage_pebble_read_amp",
			"Current Pebble LSM read amplification factor in the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleCompactionDebtBytesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_compaction_debt_bytes",
			"Estimated Pebble compaction debt in bytes for the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleCompactionsInProgDesc: prometheus.NewDesc(
			"bowl_storage_pebble_compactions_in_progress",
			"Current number of in-progress Pebble compactions in the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleFlushesInProgDesc: prometheus.NewDesc(
			"bowl_storage_pebble_flushes_in_progress",
			"Current number of in-progress Pebble memtable flushes in the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleFilterHitsDesc: prometheus.NewDesc(
			"bowl_storage_pebble_filter_hits_total",
			"Total number of Pebble bloom filter hits in the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleFilterMissesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_filter_misses_total",
			"Total number of Pebble bloom filter misses in the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleOpenTableItersDesc: prometheus.NewDesc(
			"bowl_storage_pebble_open_table_iters",
			"Current number of open Pebble sstable iterators in the shared Pebble DB.",
			nil,
			nil,
		),
		pebbleBlockCacheBytesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_block_cache_bytes",
			"Bytes currently in use by the shared Pebble block cache.",
			nil,
			nil,
		),
		pebbleBlockCacheCountDesc: prometheus.NewDesc(
			"bowl_storage_pebble_block_cache_count",
			"Number of blocks currently cached in the shared Pebble block cache.",
			nil,
			nil,
		),
		pebbleBlockCacheHitsDesc: prometheus.NewDesc(
			"bowl_storage_pebble_block_cache_hits_total",
			"Total hits in the shared Pebble block cache.",
			nil,
			nil,
		),
		pebbleBlockCacheMissesDesc: prometheus.NewDesc(
			"bowl_storage_pebble_block_cache_misses_total",
			"Total misses in the shared Pebble block cache.",
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
		snap                engine.NodeMetricsSnapshot
		walBytesIn          uint64
		walBytesWritten     uint64
		l0Sublevels         int32
		l0Tables            int64
		readAmp             int
		compactionDebtBytes uint64
		compactionsInProg   int64
		flushesInProg       int64
		filterHits          int64
		filterMisses        int64
		openTableIters      int64
	)

	if c.node != nil {
		snap = c.node.MetricsSnapshot()
		if pm := snap.Pebble; pm != nil {
			walBytesIn = pm.WAL.BytesIn
			walBytesWritten = pm.WAL.BytesWritten
			l0Sublevels = pm.Levels[0].Sublevels
			l0Tables = pm.Levels[0].TablesCount
			readAmp = pm.ReadAmp()
			compactionDebtBytes = pm.Compact.EstimatedDebt
			compactionsInProg = pm.Compact.NumInProgress
			flushesInProg = pm.Flush.NumInProgress
			filterHits = pm.Filter.Hits
			filterMisses = pm.Filter.Misses
			openTableIters = pm.TableIters
		}
	}

	ch <- prometheus.MustNewConstMetric(c.disksLoadedDesc, prometheus.GaugeValue, float64(snap.DisksLoaded))
	ch <- prometheus.MustNewConstMetric(c.logicalBytesDesc, prometheus.GaugeValue, float64(snap.LogicalBytes))
	ch <- prometheus.MustNewConstMetric(c.packPhysicalBytesDesc, prometheus.GaugeValue, float64(snap.PackPhysicalBytes))
	ch <- prometheus.MustNewConstMetric(c.inodesDesc, prometheus.GaugeValue, float64(snap.Inodes))
	ch <- prometheus.MustNewConstMetric(c.branchActorsLoadedDesc, prometheus.GaugeValue, float64(snap.ActiveBranchActors))
	ch <- prometheus.MustNewConstMetric(c.pebbleWALBytesInDesc, prometheus.CounterValue, float64(walBytesIn))
	ch <- prometheus.MustNewConstMetric(c.pebbleWALBytesWrittenDesc, prometheus.CounterValue, float64(walBytesWritten))
	ch <- prometheus.MustNewConstMetric(c.pebbleL0SublevelsDesc, prometheus.GaugeValue, float64(l0Sublevels))
	ch <- prometheus.MustNewConstMetric(c.pebbleL0TablesDesc, prometheus.GaugeValue, float64(l0Tables))
	ch <- prometheus.MustNewConstMetric(c.pebbleReadAmpDesc, prometheus.GaugeValue, float64(readAmp))
	ch <- prometheus.MustNewConstMetric(c.pebbleCompactionDebtBytesDesc, prometheus.GaugeValue, float64(compactionDebtBytes))
	ch <- prometheus.MustNewConstMetric(c.pebbleCompactionsInProgDesc, prometheus.GaugeValue, float64(compactionsInProg))
	ch <- prometheus.MustNewConstMetric(c.pebbleFlushesInProgDesc, prometheus.GaugeValue, float64(flushesInProg))
	ch <- prometheus.MustNewConstMetric(c.pebbleFilterHitsDesc, prometheus.CounterValue, float64(max(0, filterHits)))
	ch <- prometheus.MustNewConstMetric(c.pebbleFilterMissesDesc, prometheus.CounterValue, float64(max(0, filterMisses)))
	ch <- prometheus.MustNewConstMetric(c.pebbleOpenTableItersDesc, prometheus.GaugeValue, float64(openTableIters))
	ch <- prometheus.MustNewConstMetric(c.pebbleBlockCacheBytesDesc, prometheus.GaugeValue, float64(max(0, snap.BlockCache.Size)))
	ch <- prometheus.MustNewConstMetric(c.pebbleBlockCacheCountDesc, prometheus.GaugeValue, float64(max(0, snap.BlockCache.Count)))
	ch <- prometheus.MustNewConstMetric(c.pebbleBlockCacheHitsDesc, prometheus.CounterValue, float64(max(0, snap.BlockCache.Hits)))
	ch <- prometheus.MustNewConstMetric(c.pebbleBlockCacheMissesDesc, prometheus.CounterValue, float64(max(0, snap.BlockCache.Misses)))
}
