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
)

// ObjectStorageClient represents the contract for standard S3/MinIO operations.
type ObjectStorageClient interface {
	PutObject(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	StatObject(ctx context.Context, bucket, objectKey string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	GetObject(ctx context.Context, bucket, objectKey string, opts minio.GetObjectOptions) (*minio.Object, error)
	RemoveObject(ctx context.Context, bucket, objectKey string, opts minio.RemoveObjectOptions) error
	BucketExists(ctx context.Context, bucket string) (bool, error)
	MakeBucket(ctx context.Context, bucket string, opts minio.MakeBucketOptions) error
}

// SliceStatus represents the asynchronous S3 persistence state of a staged slice.
type SliceStatus string

const (
	SliceStatusUnknown   SliceStatus = "unknown"
	SliceStatusPending   SliceStatus = "pending"
	SliceStatusUploading SliceStatus = "uploading"
	SliceStatusSynced    SliceStatus = "synced"
	SliceStatusFailed    SliceStatus = "failed"
)

// UploadTask represents a staged slice queued for S3 persistence.
type UploadTask struct {
	DiskID    string
	Ino       uint64
	ChunkIndx uint32
	SliceID   uint64
	Data      []byte // optional payload; if empty, read from staging
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

// UploaderOption configures an S3Uploader instance.
type UploaderOption func(*S3Uploader)

// WithMaxRetries sets the maximum number of retry attempts for transient errors.
func WithMaxRetries(n int) UploaderOption {
	return func(u *S3Uploader) {
		u.maxRetries = n
	}
}

// WithInitialBackoff sets the base delay before the first retry attempt.
func WithInitialBackoff(d time.Duration) UploaderOption {
	return func(u *S3Uploader) {
		u.initialBackoff = d
	}
}

// WithMaxBackoff sets the maximum backoff delay between retry attempts.
func WithMaxBackoff(d time.Duration) UploaderOption {
	return func(u *S3Uploader) {
		u.maxBackoff = d
	}
}

// WithBackoffFactor sets the multiplier for exponential backoff.
func WithBackoffFactor(f float64) UploaderOption {
	return func(u *S3Uploader) {
		u.backoffFactor = f
	}
}

// WithWorkers sets the number of concurrent upload worker goroutines.
func WithWorkers(n int) UploaderOption {
	return func(u *S3Uploader) {
		if n > 0 {
			u.workers = n
		}
	}
}

// WithQueueSize sets the upload queue channel capacity.
func WithQueueSize(n int) UploaderOption {
	return func(u *S3Uploader) {
		if n > 0 {
			u.queueSize = n
		}
	}
}

// WithStaging configures the StagingManager for reading slice payloads.
func WithStaging(s *StagingManager) UploaderOption {
	return func(u *S3Uploader) {
		u.staging = s
	}
}

// WithBatcher configures the BlockBatcher.
func WithBatcher(b *BlockBatcher) UploaderOption {
	return func(u *S3Uploader) {
		u.batcher = b
	}
}

// S3Uploader manages block aggregation, transient error retry, and background asynchronous S3 uploads.
type S3Uploader struct {
	client         ObjectStorageClient
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

	mu        sync.Mutex
	slices    map[sliceKey]*sliceTrack
	inoTasks  map[inoKey]map[uint64]struct{}
	inoErrors map[inoKey]error
}

// NewMinIOClient initializes a MinIO client from S3Config.
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

// NewS3Uploader creates a new S3Uploader connected to S3/MinIO using S3Config.
func NewS3Uploader(cfg config.S3Config, staging *StagingManager, opts ...UploaderOption) (*S3Uploader, error) {
	client, err := NewMinIOClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("init minio client: %w", err)
	}

	allOpts := append([]UploaderOption{WithStaging(staging)}, opts...)
	return NewS3UploaderWithClient(client, cfg.Bucket, allOpts...), nil
}

// NewS3UploaderWithClient creates an S3Uploader using a provided ObjectStorageClient.
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
	}

	for _, opt := range opts {
		opt(u)
	}

	u.queue = make(chan *UploadTask, u.queueSize)
	return u
}

// Client returns the underlying ObjectStorageClient.
func (u *S3Uploader) Client() ObjectStorageClient {
	return u.client
}

// Bucket returns the target bucket name.
func (u *S3Uploader) Bucket() string {
	return u.bucket
}

// IsTransientError checks if an error returned by S3/MinIO is recoverable via retry.
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

// UploadBlock uploads a single 4MB block to S3, with retry and exponential backoff on transient errors.
func (u *S3Uploader) UploadBlock(ctx context.Context, block Block) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var lastErr error
	maxAttempts := 1 + u.maxRetries

	for attempt := 0; attempt < maxAttempts; attempt++ {
		reader := bytes.NewReader(block.Data)
		_, err := u.client.PutObject(ctx, u.bucket, block.Key, reader, int64(len(block.Data)), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		if err == nil {
			return nil
		}

		lastErr = err

		// Permanent error or context cancellation: stop immediately
		if !isTransientError(err) || ctx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("s3 put %s permanent failure: %w", block.Key, err)
		}

		// If this was the last attempt, don't sleep
		if attempt == maxAttempts-1 {
			break
		}

		// Calculate exponential backoff delay
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

	return fmt.Errorf("s3 put %s exceeded max retries (%d): %w", block.Key, u.maxRetries, lastErr)
}

// UploadSlice synchronously partitions a slice payload into 4MB blocks and uploads them to S3.
func (u *S3Uploader) UploadSlice(ctx context.Context, diskID string, chunkIndx uint32, sliceID uint64, data []byte) error {
	blocks := u.batcher.SplitSlice(diskID, chunkIndx, sliceID, data)
	for _, block := range blocks {
		if err := u.UploadBlock(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

// Start launches the background worker pool to consume and process upload tasks.
func (u *S3Uploader) Start(ctx context.Context) {
	for i := 0; i < u.workers; i++ {
		u.workerWg.Add(1)
		go u.worker(ctx)
	}
}

// Stop gracefully stops the upload queue and waits for all workers to finish.
func (u *S3Uploader) Stop() {
	u.stopOnce.Do(func() {
		u.cancel()
		close(u.queue)
		u.workerWg.Wait()
	})
}

// Enqueue registers an asynchronous upload task and schedules it for background persistence.
func (u *S3Uploader) Enqueue(ctx context.Context, task *UploadTask) error {
	if task == nil {
		return errors.New("cannot enqueue nil upload task")
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
		return ctx.Err()
	case <-u.ctx.Done():
		return errors.New("uploader is closed")
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

	// 1. Resolve payload bytes
	data := task.Data
	var err error
	if len(data) == 0 && u.staging != nil {
		data, err = u.staging.ReadSlice(task.DiskID, task.SliceID)
	}

	if err == nil {
		// 2. Upload partitioned 4MB blocks
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
	}
}

// GetSliceStatus queries the upload status of a slice.
func (u *S3Uploader) GetSliceStatus(diskID string, sliceID uint64) SliceStatus {
	u.mu.Lock()
	defer u.mu.Unlock()

	track := u.slices[sliceKey{diskID: diskID, sliceID: sliceID}]
	if track == nil {
		return SliceStatusUnknown
	}
	return track.status
}

// WaitForSlice blocks until the specified slice is uploaded or fails.
func (u *S3Uploader) WaitForSlice(ctx context.Context, diskID string, sliceID uint64) error {
	u.mu.Lock()
	track := u.slices[sliceKey{diskID: diskID, sliceID: sliceID}]
	u.mu.Unlock()

	if track == nil {
		return fmt.Errorf("slice %d not tracked", sliceID)
	}

	select {
	case <-track.done:
		return track.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForIno blocks until all in-flight S3 upload tasks for a given Inode complete.
func (u *S3Uploader) WaitForIno(ctx context.Context, diskID string, ino uint64) error {
	if u == nil {
		return nil
	}

	iKey := inoKey{diskID: diskID, ino: ino}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		u.mu.Lock()
		set := u.inoTasks[iKey]
		if len(set) == 0 {
			if recordedErr, ok := u.inoErrors[iKey]; ok {
				delete(u.inoErrors, iKey)
				u.mu.Unlock()
				return recordedErr
			}
			u.mu.Unlock()
			return nil
		}
		var waitChans []chan struct{}
		var tracks []*sliceTrack
		for sliceID := range set {
			if tr := u.slices[sliceKey{diskID: diskID, sliceID: sliceID}]; tr != nil {
				waitChans = append(waitChans, tr.done)
				tracks = append(tracks, tr)
			}
		}
		u.mu.Unlock()

		for _, ch := range waitChans {
			select {
			case <-ch:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		u.mu.Lock()
		for _, tr := range tracks {
			if tr.err != nil {
				if recordedErr, ok := u.inoErrors[iKey]; ok {
					delete(u.inoErrors, iKey)
					u.mu.Unlock()
					return recordedErr
				}
				u.mu.Unlock()
				return tr.err
			}
		}
		if recordedErr, ok := u.inoErrors[iKey]; ok {
			delete(u.inoErrors, iKey)
			u.mu.Unlock()
			return recordedErr
		}
		u.mu.Unlock()
	}
}

// WaitForAll blocks until the current queue is empty and all in-flight tasks have finished.
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
