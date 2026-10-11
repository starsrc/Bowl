package service

import (
	"context"
	"fmt"
	"sync"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/services/storage/internal/chunk"
)

// chunkFileHandle represents open file context for ChunkDriver.
type chunkFileHandle struct {
	mu       sync.Mutex
	diskID   string
	ino      uint64
	openMode bowlv1.OpenMode
	flags    uint32
}

// ChunkDriver implements FileStorageDriver using NVMe staging slices and PostgreSQL chunk metadata.
type ChunkDriver struct {
	staging  *chunk.StagingManager
	db       DBExecutor
	uploader *chunk.S3Uploader
}

var _ FileStorageDriver = (*ChunkDriver)(nil)

// NewChunkDriver constructs a new ChunkDriver backed by a StagingManager and database executor.
// An optional S3Uploader can be provided for background S3 block persistence.
func NewChunkDriver(staging *chunk.StagingManager, db DBExecutor, uploader ...*chunk.S3Uploader) *ChunkDriver {
	var up *chunk.S3Uploader
	if len(uploader) > 0 {
		up = uploader[0]
	}
	return &ChunkDriver{
		staging:  staging,
		db:       db,
		uploader: up,
	}
}

// Uploader returns the configured S3Uploader, if any.
func (d *ChunkDriver) Uploader() *chunk.S3Uploader {
	return d.uploader
}

// SetUploader configures or updates the S3Uploader.
func (d *ChunkDriver) SetUploader(uploader *chunk.S3Uploader) {
	d.uploader = uploader
}


// StagingManager returns the underlying StagingManager.
func (d *ChunkDriver) StagingManager() *chunk.StagingManager {
	return d.staging
}

// DB returns the underlying DBExecutor.
func (d *ChunkDriver) DB() DBExecutor {
	return d.db
}

// Open opens an existing file or creates a driver handle.
func (d *ChunkDriver) Open(ctx context.Context, diskID string, ino uint64, openMode bowlv1.OpenMode, flags uint32) (HandleContext, error) {
	return &chunkFileHandle{
		diskID:   diskID,
		ino:      ino,
		openMode: openMode,
		flags:    flags,
	}, nil
}

// Read reads file data at a given offset and size.
// Note: Chunk read from NVMe staging cache and S3 will be implemented in subsequent tickets (STARSRC-118/120/122).
func (d *ChunkDriver) Read(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, size uint32) ([]byte, bool, error) {
	if size == 0 {
		return []byte{}, false, nil
	}
	return []byte{}, true, nil
}

// Write writes file data at a given offset by creating an NVMe staging slice and appending its 24-byte metadata to chunks table.
func (d *ChunkDriver) Write(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, nil
	}

	chunkIndx := uint32(offset / chunk.ChunkSize)
	chunkPos := uint32(offset % chunk.ChunkSize)
	dataLen := uint32(len(data))

	// 1. Generate unique slice ID
	sliceID := d.staging.NextSliceID()

	// 2. Write payload to local NVMe staging file
	if err := d.staging.WriteSlice(diskID, sliceID, data); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to write staging slice: %s", err)
	}

	// 3. Encode 24-byte big-endian slice
	s := chunk.Slice{
		Pos:  chunkPos,
		ID:   sliceID,
		Size: dataLen,
		Off:  0,
		Len:  dataLen,
	}
	if err := s.Validate(); err != nil {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid slice: %s", err)
	}
	encoded := chunk.EncodeSlice(s)

	// 4. Resolve schema name from diskID
	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	// 5. Atomically append the 24-byte slice to "<schema>".chunks table
	query := fmt.Sprintf(`
INSERT INTO "%s".chunks (ino, indx, slices)
VALUES ($1, $2, $3)
ON CONFLICT (ino, indx)
DO UPDATE SET slices = chunks.slices || EXCLUDED.slices;
`, schemaName)

	if _, err := d.db.Exec(ctx, query, int64(ino), int32(chunkIndx), encoded); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to append chunk slice metadata: %s", err)
	}

	// 6. Asynchronously enqueue slice to S3 uploader if configured
	if d.uploader != nil {
		_ = d.uploader.Enqueue(ctx, &chunk.UploadTask{
			DiskID:    diskID,
			Ino:       ino,
			ChunkIndx: chunkIndx,
			SliceID:   sliceID,
		})
	}

	return dataLen, nil
}


// Flush flushes dirty data for an open handle (no-op in STARSRC-117, implemented in STARSRC-123).
func (d *ChunkDriver) Flush(ctx context.Context, handle HandleContext, diskID string, ino uint64) error {
	return nil
}

// Fsync synchronizes file data to persistent storage (no-op in STARSRC-117, implemented in STARSRC-123).
func (d *ChunkDriver) Fsync(ctx context.Context, handle HandleContext, diskID string, ino uint64, datasync bool) error {
	return nil
}

// Truncate truncates or extends a file to the specified size (implemented in STARSRC-125).
func (d *ChunkDriver) Truncate(ctx context.Context, diskID string, ino uint64, size uint64) error {
	return nil
}

// Fallocate preallocates space or punches a hole in the file.
func (d *ChunkDriver) Fallocate(ctx context.Context, handle HandleContext, diskID string, ino uint64, mode uint32, offset uint64, length uint64) error {
	return nil
}

// CopyFileRange copies a range of data between files or within the same file (implemented in STARSRC-124).
func (d *ChunkDriver) CopyFileRange(ctx context.Context, srcHandle HandleContext, srcDiskID string, srcIno uint64, srcOffset uint64, dstHandle HandleContext, dstDiskID string, dstIno uint64, dstOffset uint64, length uint64) (uint64, error) {
	return 0, nil
}

// Lseek seeks for data or holes within the file.
func (d *ChunkDriver) Lseek(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, whence uint32) (uint64, error) {
	return offset, nil
}

// Close closes the driver handle and frees resources.
func (d *ChunkDriver) Close(ctx context.Context, handle HandleContext) error {
	return nil
}
