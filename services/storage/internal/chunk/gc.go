package chunk

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/minio/minio-go/v7"
)

// GCDBExecutor abstracts PostgreSQL execution required by GarbageCollector.
type GCDBExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// FormatDiskSchemaName returns the schema name for a given virtual disk identifier.
func FormatDiskSchemaName(diskID string) string {
	if strings.HasPrefix(diskID, "disk_") {
		return diskID
	}
	parsed, err := uuid.Parse(diskID)
	if err == nil {
		return "disk_" + strings.ReplaceAll(parsed.String(), "-", "_")
	}
	return "disk_" + strings.ReplaceAll(diskID, "-", "_")
}

// ParseDiskSchemaName converts a PostgreSQL schema name (format "disk_<uuid_with_underscores>")
// back into the canonical disk UUID string format.
func ParseDiskSchemaName(schema string) (string, error) {
	raw := strings.TrimPrefix(schema, "disk_")
	uuidStr := strings.ReplaceAll(raw, "_", "-")
	parsed, err := uuid.Parse(uuidStr)
	if err != nil {
		return raw, fmt.Errorf("invalid disk schema name %q: %w", schema, err)
	}
	return parsed.String(), nil
}

// GarbageCollector removes stale local staging files, S3 physical blocks, and database references
// for slices whose reference count has dropped to zero or below (refs <= 0).
type GarbageCollector struct {
	db        GCDBExecutor
	staging   *StagingManager
	client    ObjectStorageClient
	bucket    string
	batcher   *BlockBatcher
	interval  time.Duration
	batchSize int

	mu       sync.Mutex
	disks    map[string]struct{}
	stopCh   chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

// GCOption configures a GarbageCollector instance.
type GCOption func(*GarbageCollector)

// WithGCInterval sets the periodic GC scanning interval.
func WithGCInterval(d time.Duration) GCOption {
	return func(gc *GarbageCollector) {
		if d > 0 {
			gc.interval = d
		}
	}
}

// WithGCBatchSize sets the maximum number of expired slice records to process per run.
func WithGCBatchSize(n int) GCOption {
	return func(gc *GarbageCollector) {
		if n > 0 {
			gc.batchSize = n
		}
	}
}

// WithGCClient configures the ObjectStorageClient and bucket for S3 block removal.
func WithGCClient(client ObjectStorageClient, bucket string) GCOption {
	return func(gc *GarbageCollector) {
		gc.client = client
		gc.bucket = bucket
	}
}

// WithGCBatcher configures the BlockBatcher.
func WithGCBatcher(batcher *BlockBatcher) GCOption {
	return func(gc *GarbageCollector) {
		gc.batcher = batcher
	}
}

// NewGarbageCollector initializes a new GarbageCollector.
func NewGarbageCollector(db GCDBExecutor, staging *StagingManager, opts ...GCOption) *GarbageCollector {
	gc := &GarbageCollector{
		db:        db,
		staging:   staging,
		batcher:   NewBlockBatcher(),
		interval:  1 * time.Minute,
		batchSize: 1000,
		disks:     make(map[string]struct{}),
		stopCh:    make(chan struct{}),
		stopped:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(gc)
	}
	return gc
}

// SetClient updates the ObjectStorageClient and bucket for S3 block deletion.
func (gc *GarbageCollector) SetClient(client ObjectStorageClient, bucket string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.client = client
	gc.bucket = bucket
}

// RegisterDisk registers a virtual disk to be periodically garbage collected.
func (gc *GarbageCollector) RegisterDisk(diskID string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.disks[diskID] = struct{}{}
}

// UnregisterDisk removes a virtual disk from periodic GC monitoring.
func (gc *GarbageCollector) UnregisterDisk(diskID string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	delete(gc.disks, diskID)
}

type expiredSlice struct {
	id        uint64
	chunkIndx uint32
	size      uint32
}

// RunGC performs a single garbage collection cycle on the specified virtual disk.
// It retrieves slices with refs <= 0, deletes their NVMe staging files and S3 blocks,
// and removes the corresponding rows from slice_refs.
func (gc *GarbageCollector) RunGC(ctx context.Context, diskID string) (int, error) {
	if gc.db == nil {
		return 0, nil
	}

	schemaName := FormatDiskSchemaName(diskID)

	// 1. Query expired slices from slice_refs
	querySQL := fmt.Sprintf(`
SELECT id, chunk_indx, size
FROM "%s".slice_refs
WHERE refs <= 0
LIMIT $1;
`, schemaName)

	rows, err := gc.db.Query(ctx, querySQL, gc.batchSize)
	if err != nil {
		return 0, fmt.Errorf("query expired slices: %w", err)
	}
	defer rows.Close()

	var expired []expiredSlice
	for rows.Next() {
		var sID int64
		var cIndx int32
		var sz int32
		if err := rows.Scan(&sID, &cIndx, &sz); err != nil {
			return 0, fmt.Errorf("scan expired slice: %w", err)
		}
		expired = append(expired, expiredSlice{
			id:        uint64(sID),
			chunkIndx: uint32(cIndx),
			size:      uint32(sz),
		})
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate expired slices: %w", err)
	}
	rows.Close()

	if len(expired) == 0 {
		return 0, nil
	}

	gc.mu.Lock()
	client := gc.client
	bucket := gc.bucket
	gc.mu.Unlock()

	blockSize := uint32(gc.batcher.BlockSize())

	// 2. For each expired slice: remove staging file, remove S3 blocks, delete DB row
	reclaimedCount := 0
	for _, s := range expired {
		// 2.1. Remove local NVMe staging cache file
		if gc.staging != nil {
			if err := gc.staging.DeleteSlice(diskID, s.id); err != nil && !os.IsNotExist(err) {
				// Ignore non-fatal deletion failure
			}
		}

		// 2.2. Remove physical S3 blocks if S3 client is configured
		if client != nil && bucket != "" {
			numBlocks := (s.size + blockSize - 1) / blockSize
			if numBlocks == 0 {
				numBlocks = 1
			}
			for b := uint32(0); b < numBlocks; b++ {
				key := FormatObjectKey(diskID, s.chunkIndx, s.id, b)
				_ = client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
			}
		}

		// 2.3. Delete slice_refs row
		deleteSQL := fmt.Sprintf(`DELETE FROM "%s".slice_refs WHERE id = $1;`, schemaName)
		if _, err := gc.db.Exec(ctx, deleteSQL, int64(s.id)); err != nil {
			return reclaimedCount, fmt.Errorf("delete slice_refs row %d: %w", s.id, err)
		}
		reclaimedCount++
	}

	return reclaimedCount, nil
}

// Start launches the background periodic garbage collection loop.
func (gc *GarbageCollector) Start(ctx context.Context) {
	go func() {
		defer close(gc.stopped)
		ticker := time.NewTicker(gc.interval)
		defer ticker.Stop()

		for {
			select {
			case <-gc.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				gc.tick(ctx)
			}
		}
	}()
}

func (gc *GarbageCollector) tick(ctx context.Context) {
	gc.mu.Lock()
	disks := make([]string, 0, len(gc.disks))
	for d := range gc.disks {
		disks = append(disks, d)
	}
	gc.mu.Unlock()

	// If no disks were explicitly registered, discover disks from database schemata
	if len(disks) == 0 && gc.db != nil {
		rows, err := gc.db.Query(ctx, "SELECT table_schema FROM information_schema.tables WHERE table_name = 'slice_refs';")
		if err == nil {
			for rows.Next() {
				var schema string
				if err := rows.Scan(&schema); err == nil {
					diskUUID, parseErr := ParseDiskSchemaName(schema)
					if parseErr == nil {
						disks = append(disks, diskUUID)
					} else {
						disks = append(disks, schema)
					}
				}
			}
			rows.Close()
		}
	}

	for _, d := range disks {
		_, _ = gc.RunGC(ctx, d)
	}
}

// Stop cleanly terminates the background periodic GC goroutine.
func (gc *GarbageCollector) Stop() {
	gc.stopOnce.Do(func() {
		close(gc.stopCh)
		<-gc.stopped
	})
}
