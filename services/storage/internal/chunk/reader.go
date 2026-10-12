package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

// ReaderOption configures an S3Reader instance.
type ReaderOption func(*S3Reader)

// WithReaderMaxRetries sets the maximum number of retry attempts for transient errors.
func WithReaderMaxRetries(n int) ReaderOption {
	return func(r *S3Reader) {
		r.maxRetries = n
	}
}

// WithReaderInitialBackoff sets the base delay before the first retry attempt.
func WithReaderInitialBackoff(d time.Duration) ReaderOption {
	return func(r *S3Reader) {
		r.initialBackoff = d
	}
}

// WithReaderMaxBackoff sets the maximum backoff delay between retry attempts.
func WithReaderMaxBackoff(d time.Duration) ReaderOption {
	return func(r *S3Reader) {
		r.maxBackoff = d
	}
}

// WithReaderBackoffFactor sets the multiplier for exponential backoff.
func WithReaderBackoffFactor(f float64) ReaderOption {
	return func(r *S3Reader) {
		r.backoffFactor = f
	}
}

// WithReaderBatcher configures the BlockBatcher.
func WithReaderBatcher(b *BlockBatcher) ReaderOption {
	return func(r *S3Reader) {
		r.batcher = b
	}
}

// S3Reader provides transparent read penetration for slices:
// reading first from local NVMe staging, and falling back to S3/MinIO.
type S3Reader struct {
	client         ObjectStorageClient
	bucket         string
	staging        *StagingManager
	batcher        *BlockBatcher
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	backoffFactor  float64
}

// NewS3Reader creates a new S3Reader.
func NewS3Reader(client ObjectStorageClient, bucket string, staging *StagingManager, opts ...ReaderOption) *S3Reader {
	r := &S3Reader{
		client:         client,
		bucket:         bucket,
		staging:        staging,
		batcher:        NewBlockBatcher(),
		maxRetries:     3,
		initialBackoff: 50 * time.Millisecond,
		maxBackoff:     2 * time.Second,
		backoffFactor:  2.0,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NewS3ReaderFromConfig creates a new S3Reader connected to S3/MinIO using S3Config.
func NewS3ReaderFromConfig(cfg config.S3Config, staging *StagingManager, opts ...ReaderOption) (*S3Reader, error) {
	client, err := NewMinIOClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("init minio client: %w", err)
	}
	return NewS3Reader(client, cfg.Bucket, staging, opts...), nil
}

// Client returns the underlying ObjectStorageClient.
func (r *S3Reader) Client() ObjectStorageClient {
	return r.client
}

// Bucket returns the target bucket name.
func (r *S3Reader) Bucket() string {
	return r.bucket
}

// Staging returns the configured StagingManager.
func (r *S3Reader) Staging() *StagingManager {
	return r.staging
}

// isS3NotFoundError checks if an error indicates the object was not found in S3/MinIO.
func isS3NotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" || resp.Code == "ResourceNotFound" {
		return true
	}
	return false
}

// ReadBlock reads an object block from S3 with retry on transient errors.
func (r *S3Reader) ReadBlock(ctx context.Context, key string, opts minio.GetObjectOptions) ([]byte, error) {
	if r.client == nil {
		return nil, fmt.Errorf("s3 client is nil: %w", os.ErrNotExist)
	}

	maxAttempts := 1 + r.maxRetries
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		obj, err := r.client.GetObject(ctx, r.bucket, key, opts)
		if err != nil {
			lastErr = err
			if isS3NotFoundError(err) || !isTransientError(err) {
				return nil, err
			}
		} else {
			data, readErr := io.ReadAll(obj)
			_ = obj.Close()
			if readErr == nil {
				return data, nil
			}

			lastErr = readErr
			if isS3NotFoundError(readErr) {
				return nil, readErr
			}
			if !isTransientError(readErr) {
				return nil, readErr
			}
		}

		if attempt == maxAttempts-1 {
			break
		}

		backoff := float64(r.initialBackoff) * math.Pow(r.backoffFactor, float64(attempt))
		delay := time.Duration(backoff)
		if delay > r.maxBackoff {
			delay = r.maxBackoff
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("s3 get %s exceeded max retries (%d): %w", key, r.maxRetries, lastErr)
}

// ReadSlice attempts to read a slice payload from local NVMe staging first,
// and falls back to S3/MinIO transparently if not found in staging.
func (r *S3Reader) ReadSlice(ctx context.Context, diskID string, chunkIndx uint32, sliceID uint64) ([]byte, error) {
	// 1. Hot path: NVMe Staging cache
	if r.staging != nil {
		data, err := r.staging.ReadSlice(diskID, sliceID)
		if err == nil {
			return data, nil
		}
	}

	// 2. Cold path: S3 / MinIO transparent fallback
	if r.client == nil {
		return nil, fmt.Errorf("slice %d not found in staging and s3 client is not configured: %w", sliceID, os.ErrNotExist)
	}

	var totalData []byte
	blockSize := r.batcher.BlockSize()

	for blockIndx := uint32(0); ; blockIndx++ {
		key := FormatObjectKey(diskID, chunkIndx, sliceID, blockIndx)
		data, err := r.ReadBlock(ctx, key, minio.GetObjectOptions{})
		if err != nil {
			if isS3NotFoundError(err) {
				if blockIndx == 0 {
					return nil, fmt.Errorf("slice %d (key: %s) not found: %w", sliceID, key, os.ErrNotExist)
				}
				// Reached end of multi-block slice
				break
			}
			return nil, fmt.Errorf("failed to fetch slice block %s: %w", key, err)
		}

		totalData = append(totalData, data...)

		// If block size is less than 4MB, it must be the final block of the slice
		if len(data) < blockSize {
			break
		}
	}

	return totalData, nil
}

// ReadSliceRange reads a sub-range [offset, offset+length) of a slice payload.
// It checks local NVMe staging first, and falls back to S3 HTTP Range requests if not found.
func (r *S3Reader) ReadSliceRange(ctx context.Context, diskID string, chunkIndx uint32, sliceID uint64, offset uint32, length uint32) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	// 1. Hot path: Read from local NVMe staging file
	if r.staging != nil {
		path := r.staging.SlicePath(diskID, sliceID)
		f, err := os.Open(path)
		if err == nil {
			defer f.Close()
			buf := make([]byte, length)
			n, readErr := f.ReadAt(buf, int64(offset))
			if readErr == nil || (errors.Is(readErr, io.EOF) && n > 0) {
				return buf[:n], nil
			}
		}
	}

	// 2. Cold path: S3 HTTP Range requests
	if r.client == nil {
		return nil, fmt.Errorf("slice %d not found in staging and s3 client is not configured: %w", sliceID, os.ErrNotExist)
	}

	blockSize := uint32(r.batcher.BlockSize())
	startBlock := offset / blockSize
	endBlock := (offset + length - 1) / blockSize

	result := make([]byte, 0, length)

	for b := startBlock; b <= endBlock; b++ {
		key := FormatObjectKey(diskID, chunkIndx, sliceID, b)

		blockStart := int64(0)
		if b == startBlock {
			blockStart = int64(offset % blockSize)
		}

		blockEnd := int64(blockSize - 1)
		if b == endBlock {
			blockEnd = int64((offset + length - 1) % blockSize)
		}

		opts := minio.GetObjectOptions{}
		if err := opts.SetRange(blockStart, blockEnd); err != nil {
			return nil, fmt.Errorf("invalid range [%d, %d]: %w", blockStart, blockEnd, err)
		}

		data, err := r.ReadBlock(ctx, key, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch slice block range %s [%d-%d]: %w", key, blockStart, blockEnd, err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ForChunk returns a chunkReaderWrapper bound to a specific chunk index using context.Background().
func (r *S3Reader) ForChunk(chunkIndx uint32) *chunkReaderWrapper {
	return r.ForChunkWithContext(context.Background(), chunkIndx)
}

// ForChunkWithContext returns a chunkReaderWrapper bound to a specific chunk index and context.
func (r *S3Reader) ForChunkWithContext(ctx context.Context, chunkIndx uint32) *chunkReaderWrapper {
	return &chunkReaderWrapper{
		reader:    r,
		ctx:       ctx,
		chunkIndx: chunkIndx,
	}
}

// chunkReaderWrapper adapts S3Reader to SliceReader and RangeSliceReader for a specific chunk.
type chunkReaderWrapper struct {
	reader    *S3Reader
	ctx       context.Context
	chunkIndx uint32
}

func (w *chunkReaderWrapper) ReadSlice(diskID string, sliceID uint64) ([]byte, error) {
	return w.reader.ReadSlice(w.ctx, diskID, w.chunkIndx, sliceID)
}

func (w *chunkReaderWrapper) ReadSliceRange(diskID string, sliceID uint64, off uint32, length uint32) ([]byte, error) {
	return w.reader.ReadSliceRange(w.ctx, diskID, w.chunkIndx, sliceID, off, length)
}

// chunkSliceReaderAdapter is maintained for backward compatibility.
type chunkSliceReaderAdapter = chunkReaderWrapper

// Ensure chunkReaderWrapper implements SliceReader and RangeSliceReader.
var _ SliceReader = (*chunkReaderWrapper)(nil)
var _ RangeSliceReader = (*chunkReaderWrapper)(nil)
