package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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

// Read reads file data at a given offset and size from NVMe staging slices or sparse holes.
func (d *ChunkDriver) Read(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, size uint32) ([]byte, bool, error) {
	if size == 0 {
		return []byte{}, false, nil
	}

	if handle != nil {
		if ch, ok := handle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if diskID == "" {
				diskID = ch.diskID
			}
			if ino == 0 {
				ino = ch.ino
			}
			ch.mu.Unlock()
		}
	}

	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return nil, false, connect.Errorf(connect.CodeInvalidArgument, "invalid disk id: %s", err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	// 1. Query Inode size from PostgreSQL
	var fileSize int64
	queryInodeSize := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, queryInodeSize, int64(ino)).Scan(&fileSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, connect.Errorf(connect.CodeNotFound, "inode %d not found", ino)
		}
		return nil, false, connect.Errorf(connect.CodeInternal, "failed to query inode size: %s", err)
	}

	// 2. Boundary check: if offset >= fileSize, return EOF
	if offset >= uint64(fileSize) {
		return []byte{}, true, nil
	}

	available := uint64(fileSize) - offset
	toRead := uint64(size)
	if toRead > available {
		toRead = available
	}

	// 3. Calculate chunk coordinates
	chunkIndx := uint32(offset / chunk.ChunkSize)
	chunkPos := uint32(offset % chunk.ChunkSize)

	// 4. Query PostgreSQL chunks table for (ino, chunkIndx)
	var rawSlices []byte
	queryChunks := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
	err = d.db.QueryRow(ctx, queryChunks, int64(ino), int32(chunkIndx)).Scan(&rawSlices)
	if errors.Is(err, pgx.ErrNoRows) {
		// Row does not exist: sparse hole filled with zeros
		isEOF := (offset + toRead) >= uint64(fileSize)
		return make([]byte, toRead), isEOF, nil
	} else if err != nil {
		return nil, false, connect.Errorf(connect.CodeInternal, "failed to query chunk slices: %s", err)
	}

	// 5. Decode slices
	slices, err := chunk.DecodeSlices(rawSlices)
	if err != nil {
		return nil, false, connect.Errorf(connect.CodeInternal, "failed to decode chunk slices: %s", err)
	}

	// 6. Locate the slice containing chunkPos (scan backwards to find newest overlapping slice)
	var matchedSlice *chunk.Slice
	for i := len(slices) - 1; i >= 0; i-- {
		s := slices[i]
		if chunkPos >= s.Pos && chunkPos < s.Pos+s.Len {
			matchedSlice = &slices[i]
			break
		}
	}

	if matchedSlice == nil {
		// No slice covers chunkPos: sparse hole filled with zeros
		isEOF := (offset + toRead) >= uint64(fileSize)
		return make([]byte, toRead), isEOF, nil
	}

	// 7. Read slice bytes from StagingManager
	sliceData, err := d.staging.ReadSlice(diskID, matchedSlice.ID)
	if err != nil {
		return nil, false, connect.Errorf(connect.CodeInternal, "failed to read staging slice: %s", err)
	}

	offsetInSlice := chunkPos - matchedSlice.Pos
	bytesInSlice := matchedSlice.Len - offsetInSlice
	readLen := uint32(toRead)
	if readLen > bytesInSlice {
		readLen = bytesInSlice
	}

	start := matchedSlice.Off + offsetInSlice
	end := start + readLen
	if int(end) > len(sliceData) {
		return nil, false, connect.Errorf(connect.CodeInternal, "corrupted staging slice: length %d is less than required %d", len(sliceData), end)
	}

	result := sliceData[start:end]
	isEOF := (offset + uint64(len(result))) >= uint64(fileSize)
	return result, isEOF, nil
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

		// 2.5. Asynchronously enqueue slice to S3 uploader if configured
		if d.uploader != nil {
			_ = d.uploader.Enqueue(ctx, &chunk.UploadTask{
				DiskID:    diskID,
				Ino:       ino,
				ChunkIndx: curChunk,
				SliceID:   sliceID,
			})
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
