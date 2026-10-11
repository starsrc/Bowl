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
	staging *chunk.StagingManager
	db      DBExecutor
}

var _ FileStorageDriver = (*ChunkDriver)(nil)

// NewChunkDriver constructs a new ChunkDriver backed by a StagingManager and database executor.
func NewChunkDriver(staging *chunk.StagingManager, db DBExecutor) *ChunkDriver {
	return &ChunkDriver{
		staging: staging,
		db:      db,
	}
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

// Write writes file data at a given offset by creating NVMe staging slices and appending their 24-byte metadata to chunks table.
// If the write interval [offset, offset+len(data)) crosses 64MB Chunk boundaries, it is automatically split into multiple slices.
func (d *ChunkDriver) Write(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// 1. Resolve schema name from diskID
	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	query := fmt.Sprintf(`
INSERT INTO "%s".chunks (ino, indx, slices)
VALUES ($1, $2, $3)
ON CONFLICT (ino, indx)
DO UPDATE SET slices = chunks.slices || EXCLUDED.slices;
`, schemaName)

	curOffset := offset
	remaining := data
	endOffset := offset + uint64(len(data))

	// 2. Iterate and split data across 64MB chunk boundaries
	for curOffset < endOffset {
		curChunk := uint32(curOffset / chunk.ChunkSize)
		chunkEnd := uint64(curChunk+1) * chunk.ChunkSize
		chunkRemaining := chunkEnd - curOffset

		toWrite := uint64(len(remaining))
		if toWrite > chunkRemaining {
			toWrite = chunkRemaining
		}
		chunkPos := uint32(curOffset % chunk.ChunkSize)
		chunkData := remaining[:toWrite]

		// 2.1. Generate unique slice ID
		sliceID := d.staging.NextSliceID()

		// 2.2. Write payload to local NVMe staging file
		if err := d.staging.WriteSlice(diskID, sliceID, chunkData); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to write staging slice: %s", err)
		}

		// 2.3. Encode 24-byte big-endian slice
		s := chunk.Slice{
			Pos:  chunkPos,
			ID:   sliceID,
			Size: uint32(toWrite),
			Off:  0,
			Len:  uint32(toWrite),
		}
		if err := s.Validate(); err != nil {
			return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid slice: %s", err)
		}
		encoded := chunk.EncodeSlice(s)

		// 2.4. Atomically append the 24-byte slice to "<schema>".chunks table
		if _, err := d.db.Exec(ctx, query, int64(ino), int32(curChunk), encoded); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to append chunk slice metadata: %s", err)
		}

		curOffset += toWrite
		remaining = remaining[toWrite:]
	}

	return uint32(len(data)), nil
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
