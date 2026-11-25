package projection

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func silenceDefaultLog(b *testing.B) {
	b.Helper()
	orig := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() {
		log.SetOutput(orig)
	})
}

func benchReadUsedBytes(ds *engine.DiskStore) error {
	if ds.UsedBytes() == ^uint64(0) {
		return engine.ErrInvalid
	}
	return nil
}

func findCollidingUUIDsBench(b *testing.B) (diskA, diskB uuid.UUID) {
	b.Helper()
	seen := make(map[uint64]uuid.UUID, diskSlotStripes)
	for i := range uint64(4096) {
		var id uuid.UUID
		binary.BigEndian.PutUint64(id[:8], 0x0190000000007000)
		binary.BigEndian.PutUint64(id[8:], 0x8000000000000000|i)
		stripe := diskSlotIndex(id)
		if prev, ok := seen[stripe]; ok {
			return prev, id
		}
		seen[stripe] = id
	}
	b.Fatalf("expected two colliding UUIDs within 4096 probes")
	return uuid.Nil, uuid.Nil
}

func seedColdDiskBench(b *testing.B, root string, diskID uuid.UUID, name string) {
	b.Helper()
	ds, err := engine.OpenDisk(engine.DiskOptions{
		Dir:      filepath.Join(root, "disks", diskID.String()),
		DiskID:   diskID.String(),
		Name:     name,
		MaxBytes: 16 << 20,
	})
	if err != nil {
		b.Fatalf("open seed disk: %v", err)
	}
	if err := ds.Close(); err != nil {
		b.Fatalf("close seed disk: %v", err)
	}
}

func seedSyntheticOpenEntries(mgr *ControlManager, ids []uuid.UUID, ds *engine.DiskStore, nowNano int64) {
	for _, id := range ids {
		entry := &diskEntry{store: ds}
		entry.lastUsedNano.Store(nowNano)
		entry.lastSampledUsed.Store(-1)
		mgr.getSlot(id).upsert(id, entry)
	}
}

func clearSyntheticOpenEntries(mgr *ControlManager, ids []uuid.UUID) {
	for _, id := range ids {
		mgr.getSlot(id).remove(id)
	}
}

func BenchmarkWithDisk_ParallelMultiDisk(b *testing.B) {
	silenceDefaultLog(b)
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{name: "TTLDisabled", ttl: 0},
		{name: "TTLEnabled", ttl: 30 * time.Second},
	} {
		ctx := context.Background()
		mgr, err := NewControlManager(ManagerOptions{
			StorageRoot:             b.TempDir(),
			Store:                   NewMemStore(),
			FlushInterval:           time.Hour,
			ReconcileInterval:       time.Hour,
			ActorIdleTTL:            tc.ttl,
			DisableStartupReconcile: true,
			GCMode:                  GCModeDisabled,
		})
		if err != nil {
			b.Fatalf("NewControlManager: %v", err)
		}

		const diskCount = 64
		diskIDs := make([]uuid.UUID, diskCount)
		for i := range diskCount {
			rec, cErr := mgr.CreateDisk(ctx, fmt.Sprintf("bench-multi-%d", i), 16<<20)
			if cErr != nil {
				b.Fatalf("CreateDisk %d: %v", i, cErr)
			}
			diskIDs[i] = rec.DiskID
		}

		b.Run(tc.name, func(b *testing.B) {
			var workerSeq atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				idx := int(workerSeq.Add(1)-1) * 7
				for pb.Next() {
					if wErr := mgr.WithDisk(ctx, diskIDs[idx&(diskCount-1)], benchReadUsedBytes); wErr != nil {
						b.Fatalf("WithDisk: %v", wErr)
					}
					idx++
				}
			})
			b.StopTimer()
		})

		if cErr := mgr.Close(ctx); cErr != nil {
			b.Fatalf("Close: %v", cErr)
		}
	}
}

func BenchmarkWithDisk_SingleHotDisk(b *testing.B) {
	silenceDefaultLog(b)
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{name: "TTLDisabled", ttl: 0},
		{name: "TTLEnabled", ttl: 30 * time.Second},
	} {
		ctx := context.Background()
		mgr, err := NewControlManager(ManagerOptions{
			StorageRoot:             b.TempDir(),
			Store:                   NewMemStore(),
			FlushInterval:           time.Hour,
			ReconcileInterval:       time.Hour,
			ActorIdleTTL:            tc.ttl,
			DisableStartupReconcile: true,
			GCMode:                  GCModeDisabled,
		})
		if err != nil {
			b.Fatalf("NewControlManager: %v", err)
		}

		rec, err := mgr.CreateDisk(ctx, "bench-single", 16<<20)
		if err != nil {
			b.Fatalf("CreateDisk: %v", err)
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if wErr := mgr.WithDisk(ctx, rec.DiskID, benchReadUsedBytes); wErr != nil {
						b.Fatalf("WithDisk: %v", wErr)
					}
				}
			})
			b.StopTimer()
		})

		if cErr := mgr.Close(ctx); cErr != nil {
			b.Fatalf("Close: %v", cErr)
		}
	}
}

func BenchmarkPoolScan(b *testing.B) {
	silenceDefaultLog(b)
	for _, openDisks := range []int{0, 100, 10000} {
		b.Run(fmt.Sprintf("Open=%d", openDisks), func(b *testing.B) {
			ctx := context.Background()
			fixedNow := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
			mgr, err := NewControlManager(ManagerOptions{
				StorageRoot:             b.TempDir(),
				Store:                   NewMemStore(),
				Now:                     func() time.Time { return fixedNow },
				FlushInterval:           time.Hour,
				ReconcileInterval:       time.Hour,
				ActorIdleTTL:            30 * time.Second,
				DisableStartupReconcile: true,
				GCMode:                  GCModeDisabled,
			})
			if err != nil {
				b.Fatalf("NewControlManager: %v", err)
			}
			defer func() { _ = mgr.Close(ctx) }()

			var syntheticIDs []uuid.UUID
			if openDisks > 0 {
				base, cErr := mgr.CreateDisk(ctx, "bench-scan-base", 16<<20)
				if cErr != nil {
					b.Fatalf("CreateDisk: %v", cErr)
				}
				baseEntry := lookupOpenEntry(mgr.getSlot(base.DiskID), base.DiskID)
				if baseEntry == nil {
					b.Fatalf("expected base disk to be open")
				}
				baseDS := baseEntry.store
				if openDisks > 1 {
					syntheticIDs = make([]uuid.UUID, openDisks-1)
					for i := range syntheticIDs {
						var id uuid.UUID
						binary.BigEndian.PutUint64(id[:8], 0x0190000000007000)
						binary.BigEndian.PutUint64(id[8:], 0x8000000000000000|uint64(i+1))
						syntheticIDs[i] = id
					}
					seedSyntheticOpenEntries(mgr, syntheticIDs, baseDS, fixedNow.UnixNano())
				}
			}
			defer clearSyntheticOpenEntries(mgr, syntheticIDs)

			b.Run("EvictIdleDisks", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if evicted := mgr.evictIdleDisks(ctx); evicted != 0 {
						b.Fatalf("expected 0 evicted, got %d", evicted)
					}
				}
			})

			b.Run("Collect", func(b *testing.B) {
				collector := NewStorageMetricsCollector(mgr)
				ch := make(chan prometheus.Metric, 32)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					collector.Collect(ch)
					count := 0
					for len(ch) > 0 {
						<-ch
						count++
					}
					if count != 20 {
						b.Fatalf("expected 20 metrics, got %d", count)
					}
				}
			})
		})
	}
}

func BenchmarkColdFallback_CollidingHotDisk(b *testing.B) {
	silenceDefaultLog(b)
	ctx := context.Background()
	root := b.TempDir()
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		GCMode:                  GCModeDisabled,
	})
	if err != nil {
		b.Fatalf("NewControlManager: %v", err)
	}
	defer func() { _ = mgr.Close(ctx) }()

	coldDisk, hotDisk := findCollidingUUIDsBench(b)
	seedColdDiskBench(b, root, coldDisk, "bench-cold")
	seedColdDiskBench(b, root, hotDisk, "bench-hot")

	if err := mgr.WithDisk(ctx, hotDisk, func(*engine.DiskStore) error { return nil }); err != nil {
		b.Fatalf("warm hotDisk: %v", err)
	}
	store.SetHooks(MemStoreHooks{
		BeforeRead: func(context.Context, string) error {
			return ErrUnavailable
		},
	})

	stopCold := make(chan struct{})
	coldStarted := make(chan struct{})
	coldDone := make(chan struct{})
	go func() {
		defer close(coldDone)
		first := true
		for {
			select {
			case <-stopCold:
				return
			default:
				if _, gErr := mgr.GetDisk(ctx, coldDisk); gErr != nil {
					return
				}
				if first {
					close(coldStarted)
					first = false
				}
			}
		}
	}()
	<-coldStarted

	latencies := make([]int64, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		start := time.Now()
		if wErr := mgr.WithDisk(ctx, hotDisk, benchReadUsedBytes); wErr != nil {
			b.Fatalf("hot WithDisk: %v", wErr)
		}
		latencies = append(latencies, time.Since(start).Nanoseconds())
	}
	b.StopTimer()

	close(stopCold)
	<-coldDone

	if len(latencies) > 0 {
		slices.Sort(latencies)
		p50 := latencies[(len(latencies)-1)*50/100]
		p95 := latencies[(len(latencies)-1)*95/100]
		p99 := latencies[(len(latencies)-1)*99/100]
		pMax := latencies[len(latencies)-1]
		b.ReportMetric(float64(p50), "p50-ns/op")
		b.ReportMetric(float64(p95), "p95-ns/op")
		b.ReportMetric(float64(p99), "p99-ns/op")
		b.ReportMetric(float64(pMax), "max-ns/op")
	}
}
