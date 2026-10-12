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

// GCDBExecutor 抽象垃圾回收器所需的 PostgreSQL 数据库操作接口
type GCDBExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// FormatDiskSchemaName 格式化虚拟磁盘独立的 PostgreSQL Schema 名称
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

// ParseDiskSchemaName 将 PostgreSQL Schema 名称转换为标准磁盘 UUID 字符串
func ParseDiskSchemaName(schema string) (string, error) {
	raw := strings.TrimPrefix(schema, "disk_")
	uuidStr := strings.ReplaceAll(raw, "_", "-")
	parsed, err := uuid.Parse(uuidStr)
	if err != nil {
		return raw, fmt.Errorf("非法磁盘 Schema 名称 %q: %w", schema, err)
	}
	return parsed.String(), nil
}

// GarbageCollector 清理引用计数归零 (refs <= 0) 的废弃本地暂存文件、S3 物理对象及数据库元数据
type GarbageCollector struct {
	db        GCDBExecutor
	staging   *StagingManager
	client    ObjectStorageClient
	bucket    string
	batcher   *BlockBatcher
	interval  time.Duration
	batchSize int

	mu           sync.Mutex
	disks        map[string]struct{}
	orphanSlices map[string][]uint64 // 按虚拟磁盘 diskID 记录的待物理回收孤儿切片编号列表
	stopCh       chan struct{}
	stopped      chan struct{}
	stopOnce     sync.Once
}

// GCOption 配置 GarbageCollector 实例的可选函数
type GCOption func(*GarbageCollector)

// WithGCInterval 配置后台周期性垃圾回收扫描间隔
func WithGCInterval(d time.Duration) GCOption {
	return func(gc *GarbageCollector) {
		if d > 0 {
			gc.interval = d
		}
	}
}

// WithGCBatchSize 设置单次 GC 周期处理的最大过期切片数量
func WithGCBatchSize(n int) GCOption {
	return func(gc *GarbageCollector) {
		if n > 0 {
			gc.batchSize = n
		}
	}
}

// WithGCClient 配置用于物理删除 S3 分块对象的客户端及存储桶
func WithGCClient(client ObjectStorageClient, bucket string) GCOption {
	return func(gc *GarbageCollector) {
		gc.client = client
		gc.bucket = bucket
	}
}

// WithGCBatcher 配置分块批处理器 BlockBatcher
func WithGCBatcher(batcher *BlockBatcher) GCOption {
	return func(gc *GarbageCollector) {
		gc.batcher = batcher
	}
}

// NewGarbageCollector 构造 GarbageCollector 实例
func NewGarbageCollector(db GCDBExecutor, staging *StagingManager, opts ...GCOption) *GarbageCollector {
	gc := &GarbageCollector{
		db:           db,
		staging:      staging,
		batcher:      NewBlockBatcher(),
		interval:     1 * time.Minute,
		batchSize:    1000,
		disks:        make(map[string]struct{}),
		orphanSlices: make(map[string][]uint64),
		stopCh:       make(chan struct{}),
		stopped:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(gc)
	}
	return gc
}

// SetClient 更新 S3 物理删除所使用的客户端与目标存储桶
func (gc *GarbageCollector) SetClient(client ObjectStorageClient, bucket string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.client = client
	gc.bucket = bucket
}

// RegisterDisk 登记虚拟磁盘以纳入周期性垃圾回收调度范围
func (gc *GarbageCollector) RegisterDisk(diskID string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.disks[diskID] = struct{}{}
}

// UnregisterDisk 从周期性垃圾回收监控列表中移除指定虚拟磁盘
func (gc *GarbageCollector) UnregisterDisk(diskID string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	delete(gc.disks, diskID)
	delete(gc.orphanSlices, diskID)
}

// TrackOrphans 登记并跟踪待物理回收的孤儿切片编号
func (gc *GarbageCollector) TrackOrphans(diskID string, sliceIDs ...uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.disks[diskID] = struct{}{}
	if len(sliceIDs) > 0 {
		gc.orphanSlices[diskID] = append(gc.orphanSlices[diskID], sliceIDs...)
	}
}

// TrackedOrphans 返回指定虚拟磁盘当前已跟踪的物理孤儿切片编号副本
func (gc *GarbageCollector) TrackedOrphans(diskID string) []uint64 {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	slices := gc.orphanSlices[diskID]
	res := make([]uint64, len(slices))
	copy(res, slices)
	return res
}

type expiredSlice struct {
	id        uint64
	chunkIndx uint32
	size      uint32
}

// RunGC 对指定虚拟磁盘执行单个周期的垃圾回收操作：
// 查询 refs <= 0 的切片，物理删除对应的本地 NVMe Staging 文件与 S3 对象，并从 slice_refs 移除元数据行。
func (gc *GarbageCollector) RunGC(ctx context.Context, diskID string) (int, error) {
	if gc.db == nil {
		return 0, nil
	}

	schemaName := FormatDiskSchemaName(diskID)

	// 1. 查询 slice_refs 中已过期切片 (refs <= 0)
	querySQL := fmt.Sprintf(`
SELECT id, chunk_indx, size
FROM "%s".slice_refs
WHERE refs <= 0
LIMIT $1;
`, schemaName)

	rows, err := gc.db.Query(ctx, querySQL, gc.batchSize)
	if err != nil {
		return 0, fmt.Errorf("查询过期切片失败: %w", err)
	}
	defer rows.Close()

	var expired []expiredSlice
	for rows.Next() {
		var sID int64
		var cIndx int32
		var sz int32
		if err := rows.Scan(&sID, &cIndx, &sz); err != nil {
			return 0, fmt.Errorf("扫描过期切片失败: %w", err)
		}
		expired = append(expired, expiredSlice{
			id:        uint64(sID),
			chunkIndx: uint32(cIndx),
			size:      uint32(sz),
		})
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("遍历过期切片失败: %w", err)
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

	// 2. 针对每个过期切片：清理本地暂存文件、物理删除 S3 分块、删除数据库行
	reclaimedCount := 0
	for _, s := range expired {
		// 2.1. 移除本地 NVMe Staging 缓存文件
		if gc.staging != nil {
			if err := gc.staging.DeleteSlice(diskID, s.id); err != nil && !os.IsNotExist(err) {
				// 忽略非致命删除错误
			}
		}

		// 2.2. 若配置了 S3 客户端，删除 S3 物理对象
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

		// 2.3. 删除 slice_refs 行
		deleteSQL := fmt.Sprintf(`DELETE FROM "%s".slice_refs WHERE id = $1;`, schemaName)
		if _, err := gc.db.Exec(ctx, deleteSQL, int64(s.id)); err != nil {
			return reclaimedCount, fmt.Errorf("删除 slice_refs 行 %d 失败: %w", s.id, err)
		}
		reclaimedCount++
	}

	gc.mu.Lock()
	delete(gc.orphanSlices, diskID)
	gc.mu.Unlock()

	return reclaimedCount, nil
}

// Start 启动后台周期性垃圾回收工作协程
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

	// 若未显式登记磁盘，自动从数据库模式中发现
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

// Stop 安全停止后台周期性垃圾回收协程
func (gc *GarbageCollector) Stop() {
	gc.stopOnce.Do(func() {
		close(gc.stopCh)
		<-gc.stopped
	})
}
