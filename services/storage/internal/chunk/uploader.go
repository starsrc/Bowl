package chunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/juicefs/object"
)

// Block 表示待上传至 S3 的 4MB 物理分块
type Block struct {
	DiskID    string
	ChunkIndx uint32
	SliceID   uint64
	BlockIndx uint32
	Offset    uint32 // 切片载荷内的起始偏移量
	Data      []byte
	Key       string
}

// BlockBatcher 将切片数据划分为 4MB 物理分块并计算 S3 对象键
type BlockBatcher struct {
	blockSize int
}

// NewBlockBatcher 构造 BlockBatcher 实例
func NewBlockBatcher(blockSize ...int) *BlockBatcher {
	bs := BlockSize
	if len(blockSize) > 0 && blockSize[0] > 0 {
		bs = blockSize[0]
	}
	return &BlockBatcher{blockSize: bs}
}

// BlockSize 返回当前配置的物理分块字节大小
func (b *BlockBatcher) BlockSize() int {
	return b.blockSize
}

// SplitSlice 将切片数据切分为 4MB 物理分块并生成对应的 S3 对象键
func (b *BlockBatcher) SplitSlice(diskID string, chunkIndx uint32, sliceID uint64, data []byte) []Block {
	if len(data) == 0 {
		return nil
	}

	numBlocks := (len(data) + b.blockSize - 1) / b.blockSize
	blocks := make([]Block, 0, numBlocks)

	for i := 0; i < numBlocks; i++ {
		start := i * b.blockSize
		end := start + b.blockSize
		if end > len(data) {
			end = len(data)
		}
		blockIndx := uint32(i)
		key := FormatObjectKey(diskID, chunkIndx, sliceID, blockIndx)
		blocks = append(blocks, Block{
			DiskID:    diskID,
			ChunkIndx: chunkIndx,
			SliceID:   sliceID,
			BlockIndx: blockIndx,
			Offset:    uint32(start),
			Data:      data[start:end],
			Key:       key,
		})
	}

	return blocks
}

// SplitSliceFromStaging 从本地 staging 目录读取切片数据并切分为物理分块
func (b *BlockBatcher) SplitSliceFromStaging(staging *StagingManager, diskID string, chunkIndx uint32, sliceID uint64) ([]Block, error) {
	data, err := staging.ReadSlice(diskID, sliceID)
	if err != nil {
		return nil, fmt.Errorf("读取 staging 切片失败: %w", err)
	}
	return b.SplitSlice(diskID, chunkIndx, sliceID, data), nil
}

// inodeBarrier 表示针对指定 Inode 的在途切片持久化同步屏障
type inodeBarrier struct {
	remaining map[uint64]struct{}
	ch        chan error
	err       error
}

// ObjectStorageClient 抽象标准 S3/MinIO 存储操作接口契约
type ObjectStorageClient interface {
	PutObject(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	StatObject(ctx context.Context, bucket, objectKey string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	GetObject(ctx context.Context, bucket, objectKey string, opts minio.GetObjectOptions) (*minio.Object, error)
	RemoveObject(ctx context.Context, bucket, objectKey string, opts minio.RemoveObjectOptions) error
	BucketExists(ctx context.Context, bucket string) (bool, error)
	MakeBucket(ctx context.Context, bucket string, opts minio.MakeBucketOptions) error
}

// SliceStatus 表示暂存切片的异步 S3 持久化状态
type SliceStatus string

const (
	SliceStatusUnknown   SliceStatus = "unknown"
	SliceStatusPending   SliceStatus = "pending"
	SliceStatusUploading SliceStatus = "uploading"
	SliceStatusSynced    SliceStatus = "synced"
	SliceStatusFailed    SliceStatus = "failed"
)

// UploadTask 表示排队等待 S3 持久化的切片上传任务
type UploadTask struct {
	DiskID    string
	Ino       uint64
	ChunkIndx uint32
	SliceID   uint64
	Data      []byte // 可选载荷；若为空则从 staging 目录读取
}

type sliceKey struct {
	diskID  string
	sliceID uint64
}

type inoKey struct {
	diskID string
	ino    uint64
}

type sliceTrack struct {
	status SliceStatus
	err    error
	done   chan struct{}
}

// UploaderOption 配置 S3Uploader 实例的可选函数
type UploaderOption func(*S3Uploader)

// WithMaxRetries 设置针对瞬时错误的最大重试次数
func WithMaxRetries(n int) UploaderOption {
	return func(u *S3Uploader) {
		u.maxRetries = n
	}
}

// WithInitialBackoff 设置首次重试前的初始退避时间
func WithInitialBackoff(d time.Duration) UploaderOption {
	return func(u *S3Uploader) {
		u.initialBackoff = d
	}
}

// WithMaxBackoff 设置重试之间的最大退避等待上限
func WithMaxBackoff(d time.Duration) UploaderOption {
	return func(u *S3Uploader) {
		u.maxBackoff = d
	}
}

// WithBackoffFactor 设置指数退避乘数因子
func WithBackoffFactor(f float64) UploaderOption {
	return func(u *S3Uploader) {
		u.backoffFactor = f
	}
}

// WithWorkers 设置并发上传 worker 协程数量
func WithWorkers(n int) UploaderOption {
	return func(u *S3Uploader) {
		if n > 0 {
			u.workers = n
		}
	}
}

// WithQueueSize 设置上传任务队列缓冲通道容量
func WithQueueSize(n int) UploaderOption {
	return func(u *S3Uploader) {
		if n > 0 {
			u.queueSize = n
		}
	}
}

// WithStaging 配置读取切片载荷数据的 StagingManager 实例
func WithStaging(s *StagingManager) UploaderOption {
	return func(u *S3Uploader) {
		u.staging = s
	}
}

// WithBatcher 配置分块批处理器 BlockBatcher
func WithBatcher(b *BlockBatcher) UploaderOption {
	return func(u *S3Uploader) {
		u.batcher = b
	}
}

// WithObjectStorage 配置 JuiceFS 内联对象存储驱动
func WithObjectStorage(objStore object.ObjectStorage) UploaderOption {
	return func(u *S3Uploader) {
		u.objStorage = objStore
	}
}

// S3Uploader 管理分块聚合、瞬时错误重试以及后台异步多协程 S3 推流落盘
type S3Uploader struct {
	client         ObjectStorageClient
	objStorage     object.ObjectStorage
	bucket         string
	staging        *StagingManager
	batcher        *BlockBatcher
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	backoffFactor  float64
	workers        int
	queueSize      int

	queue    chan *UploadTask
	workerWg sync.WaitGroup
	stopOnce sync.Once
	ctx      context.Context
	cancel   context.CancelFunc

	mu          sync.Mutex
	slices      map[sliceKey]*sliceTrack
	inoTasks    map[inoKey]map[uint64]struct{}
	inoErrors   map[inoKey]error
	inoBarriers map[inoKey][]*inodeBarrier
}

// NewMinIOClient 基于 S3Config 初始化 MinIO 客户端实例
func NewMinIOClient(cfg config.S3Config) (*minio.Client, error) {
	endpoint := cfg.Endpoint
	u, err := url.Parse(endpoint)
	var host string
	var secure bool
	if err == nil && u.Host != "" {
		host = u.Host
		secure = u.Scheme == "https" || cfg.UseSSL
	} else {
		host = strings.TrimPrefix(endpoint, "http://")
		host = strings.TrimPrefix(host, "https://")
		secure = cfg.UseSSL
	}

	return minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: secure,
		Region: cfg.Region,
	})
}

// NewS3Uploader 基于 S3Config 创建并连接 S3/MinIO 的 S3Uploader 实例
func NewS3Uploader(cfg config.S3Config, staging *StagingManager, opts ...UploaderOption) (*S3Uploader, error) {
	client, err := NewMinIOClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("初始化 minio 客户端失败: %w", err)
	}

	objStore := object.NewS3WithClient(client, cfg.Bucket)
	allOpts := append([]UploaderOption{WithStaging(staging), WithObjectStorage(objStore)}, opts...)
	return NewS3UploaderWithClient(client, cfg.Bucket, allOpts...), nil
}

// NewS3UploaderWithClient 基于已有 ObjectStorageClient 构造 S3Uploader 实例
func NewS3UploaderWithClient(client ObjectStorageClient, bucket string, opts ...UploaderOption) *S3Uploader {
	ctx, cancel := context.WithCancel(context.Background())
	u := &S3Uploader{
		client:         client,
		bucket:         bucket,
		batcher:        NewBlockBatcher(),
		maxRetries:     3,
		initialBackoff: 50 * time.Millisecond,
		maxBackoff:     2 * time.Second,
		backoffFactor:  2.0,
		workers:        4,
		queueSize:      1000,
		ctx:            ctx,
		cancel:         cancel,
		slices:         make(map[sliceKey]*sliceTrack),
		inoTasks:       make(map[inoKey]map[uint64]struct{}),
		inoErrors:      make(map[inoKey]error),
		inoBarriers:    make(map[inoKey][]*inodeBarrier),
	}

	for _, opt := range opts {
		opt(u)
	}

	// 若未显式传入 objStorage，且 client 实际为 *minio.Client，自动构造内联 JuiceFS S3 驱动
	if u.objStorage == nil && client != nil {
		if mc, ok := client.(*minio.Client); ok {
			u.objStorage = object.NewS3WithClient(mc, bucket)
		}
	}

	u.queue = make(chan *UploadTask, u.queueSize)
	return u
}

// ObjectStorage 返回当前配置的 JuiceFS 内联对象存储驱动实例
func (u *S3Uploader) ObjectStorage() object.ObjectStorage {
	return u.objStorage
}

// Client 返回底层绑定的 ObjectStorageClient 实例
func (u *S3Uploader) Client() ObjectStorageClient {
	return u.client
}

// Bucket 返回目标 S3 存储桶名称
func (u *S3Uploader) Bucket() string {
	return u.bucket
}

// isTransientError 检查 S3/MinIO 返回的错误是否属于可通过重试恢复的瞬时错误
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == 500 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504 || resp.StatusCode == 429 {
		return true
	}

	switch resp.Code {
	case "SlowDown", "RequestTimeout", "ServiceUnavailable", "InternalError":
		return true
	}

	return false
}

// UploadBlock 将单个 4MB 物理分块上传至 S3，并在遇到瞬时错误时执行指数退避重试
func (u *S3Uploader) UploadBlock(ctx context.Context, block Block) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var lastErr error
	maxAttempts := 1 + u.maxRetries

	for attempt := 0; attempt < maxAttempts; attempt++ {
		reader := bytes.NewReader(block.Data)
		var err error
		if u.objStorage != nil {
			err = u.objStorage.Put(ctx, block.Key, reader)
		} else if u.client != nil {
			_, err = u.client.PutObject(ctx, u.bucket, block.Key, reader, int64(len(block.Data)), minio.PutObjectOptions{
				ContentType: "application/octet-stream",
			})
		} else {
			return errors.New("对象存储客户端未初始化")
		}

		if err == nil {
			return nil
		}

		lastErr = err

		// 不可恢复错误或上下文已取消：立即终止退出
		if !isTransientError(err) || ctx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("s3 put %s 遇到不可恢复错误: %w", block.Key, err)
		}

		// 若为最后一次尝试，则无需等待
		if attempt == maxAttempts-1 {
			break
		}

		// 计算指数退避延迟时间
		backoff := float64(u.initialBackoff) * math.Pow(u.backoffFactor, float64(attempt))
		delay := time.Duration(backoff)
		if delay > u.maxBackoff {
			delay = u.maxBackoff
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("s3 put %s 超过最大重试次数 (%d): %w", block.Key, u.maxRetries, lastErr)
}

// UploadSlice 同步将切片载荷数据划分为 4MB 物理分块并上传至 S3
func (u *S3Uploader) UploadSlice(ctx context.Context, diskID string, chunkIndx uint32, sliceID uint64, data []byte) error {
	blocks := u.batcher.SplitSlice(diskID, chunkIndx, sliceID, data)
	for _, block := range blocks {
		if err := u.UploadBlock(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

// Start 启动后台 worker 协程池以并发消费并处理推流任务
func (u *S3Uploader) Start(ctx context.Context) {
	for i := 0; i < u.workers; i++ {
		u.workerWg.Add(1)
		go u.worker(ctx)
	}
}

// Stop 优雅关闭上传队列并等待所有后台 worker 协程退出完成
func (u *S3Uploader) Stop() {
	u.stopOnce.Do(func() {
		u.cancel()
		close(u.queue)
		u.workerWg.Wait()
	})
}

// Enqueue 登记异步上传任务并投递入后台排队通道
func (u *S3Uploader) Enqueue(ctx context.Context, task *UploadTask) error {
	if task == nil {
		return errors.New("不能入队空上传任务")
	}

	sKey := sliceKey{diskID: task.DiskID, sliceID: task.SliceID}
	iKey := inoKey{diskID: task.DiskID, ino: task.Ino}

	u.mu.Lock()
	track, exists := u.slices[sKey]
	if !exists {
		track = &sliceTrack{
			status: SliceStatusPending,
			done:   make(chan struct{}),
		}
		u.slices[sKey] = track
	}

	if task.Ino > 0 {
		if _, ok := u.inoTasks[iKey]; !ok {
			u.inoTasks[iKey] = make(map[uint64]struct{})
		}
		u.inoTasks[iKey][task.SliceID] = struct{}{}
	}
	u.mu.Unlock()

	select {
	case u.queue <- task:
		return nil
	case <-ctx.Done():
		u.failTaskOnEnqueueError(task, ctx.Err())
		return ctx.Err()
	case <-u.ctx.Done():
		err := errors.New("上传器已关闭")
		u.failTaskOnEnqueueError(task, err)
		return err
	}
}

// failTaskOnEnqueueError 在入队遭遇上下文取消或通道关闭时清理在途状态并通知屏障，避免死锁
func (u *S3Uploader) failTaskOnEnqueueError(task *UploadTask, err error) {
	sKey := sliceKey{diskID: task.DiskID, sliceID: task.SliceID}
	iKey := inoKey{diskID: task.DiskID, ino: task.Ino}

	u.mu.Lock()
	defer u.mu.Unlock()

	if track := u.slices[sKey]; track != nil {
		track.status = SliceStatusFailed
		track.err = err
		close(track.done)
	}

	if task.Ino > 0 {
		u.inoErrors[iKey] = err
		if set, ok := u.inoTasks[iKey]; ok {
			delete(set, task.SliceID)
			if len(set) == 0 {
				delete(u.inoTasks, iKey)
			}
		}
		u.notifyBarriersLocked(iKey, task.SliceID, err)
	}
}

func (u *S3Uploader) worker(parentCtx context.Context) {
	defer u.workerWg.Done()

	for {
		select {
		case <-parentCtx.Done():
			return
		case <-u.ctx.Done():
			return
		case task, ok := <-u.queue:
			if !ok {
				return
			}
			u.processTask(parentCtx, task)
		}
	}
}

func (u *S3Uploader) processTask(ctx context.Context, task *UploadTask) {
	sKey := sliceKey{diskID: task.DiskID, sliceID: task.SliceID}
	iKey := inoKey{diskID: task.DiskID, ino: task.Ino}

	u.mu.Lock()
	track := u.slices[sKey]
	if track != nil {
		track.status = SliceStatusUploading
	}
	u.mu.Unlock()

	// 1. 获取有效切片载荷字节
	data := task.Data
	var err error
	if len(data) == 0 && u.staging != nil {
		data, err = u.staging.ReadSlice(task.DiskID, task.SliceID)
	}

	if err == nil {
		// 2. 上传 4MB 物理分块
		err = u.UploadSlice(ctx, task.DiskID, task.ChunkIndx, task.SliceID, data)
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if track != nil {
		if err != nil {
			track.status = SliceStatusFailed
			track.err = err
		} else {
			track.status = SliceStatusSynced
		}
		close(track.done)
	}

	if task.Ino > 0 {
		if err != nil {
			u.inoErrors[iKey] = err
		}
		if set, ok := u.inoTasks[iKey]; ok {
			delete(set, task.SliceID)
			if len(set) == 0 {
				delete(u.inoTasks, iKey)
			}
		}
		// 通知所有等待此 Inode 的同步屏障
		u.notifyBarriersLocked(iKey, task.SliceID, err)
	}
}

// notifyBarriersLocked 递减等待指定切片的同步屏障并触发达成通知（必须持有 u.mu 锁）
func (u *S3Uploader) notifyBarriersLocked(iKey inoKey, sliceID uint64, err error) {
	barriers := u.inoBarriers[iKey]
	var activeBarriers []*inodeBarrier

	for _, b := range barriers {
		if _, ok := b.remaining[sliceID]; ok {
			delete(b.remaining, sliceID)
			if err != nil && b.err == nil {
				b.err = err
			}
			if len(b.remaining) == 0 {
				b.ch <- b.err
				continue
			}
		}
		activeBarriers = append(activeBarriers, b)
	}

	if len(activeBarriers) == 0 {
		delete(u.inoBarriers, iKey)
	} else {
		u.inoBarriers[iKey] = activeBarriers
	}
}

// GetSliceStatus 查询指定切片的 S3 异步推流持久化状态
func (u *S3Uploader) GetSliceStatus(diskID string, sliceID uint64) SliceStatus {
	u.mu.Lock()
	defer u.mu.Unlock()

	track := u.slices[sliceKey{diskID: diskID, sliceID: sliceID}]
	if track == nil {
		return SliceStatusUnknown
	}
	return track.status
}

// WaitForSlice 阻塞等待指定切片上传成功或失败
func (u *S3Uploader) WaitForSlice(ctx context.Context, diskID string, sliceID uint64) error {
	u.mu.Lock()
	track := u.slices[sliceKey{diskID: diskID, sliceID: sliceID}]
	u.mu.Unlock()

	if track == nil {
		return fmt.Errorf("切片 %d 未被跟踪", sliceID)
	}

	select {
	case <-track.done:
		return track.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Barrier 为指定 Inode 创建持久化同步屏障信道。
// 当该 Inode 当前所有的在途 4MB 物理分块推流完成时，该信道将接收处理结果（nil 或不可恢复错误）。
func (u *S3Uploader) Barrier(diskID string, ino uint64) <-chan error {
	ch := make(chan error, 1)
	if u == nil || diskID == "" || ino == 0 {
		ch <- nil
		return ch
	}

	iKey := inoKey{diskID: diskID, ino: ino}

	u.mu.Lock()
	defer u.mu.Unlock()

	set := u.inoTasks[iKey]
	if len(set) == 0 {
		if recordedErr, ok := u.inoErrors[iKey]; ok {
			delete(u.inoErrors, iKey)
			ch <- recordedErr
			return ch
		}
		ch <- nil
		return ch
	}

	b := &inodeBarrier{
		remaining: make(map[uint64]struct{}, len(set)),
		ch:        ch,
		err:       u.inoErrors[iKey],
	}
	for sliceID := range set {
		b.remaining[sliceID] = struct{}{}
	}
	u.inoBarriers[iKey] = append(u.inoBarriers[iKey], b)
	return ch
}

// WaitForIno 阻塞等待指定 Inode 当前所有的在途 4MB 物理分块持久化至 S3/MinIO 完成
func (u *S3Uploader) WaitForIno(ctx context.Context, diskID string, ino uint64) error {
	if u == nil || diskID == "" || ino == 0 {
		return nil
	}

	barrierCh := u.Barrier(diskID, ino)

	select {
	case err := <-barrierCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-u.ctx.Done():
		return errors.New("上传器已关闭")
	}
}

// WaitForAll 阻塞等待直到当前队列清空且所有在途推流任务全部执行完成
func (u *S3Uploader) WaitForAll(ctx context.Context) error {
	for {
		u.mu.Lock()
		pendingCount := len(u.inoTasks)
		queueLen := len(u.queue)
		u.mu.Unlock()

		if pendingCount == 0 && queueLen == 0 {
			return nil
		}

		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
