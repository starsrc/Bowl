package service

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	reader   *chunk.S3Reader
}

var _ FileStorageDriver = (*ChunkDriver)(nil)

// NewChunkDriver constructs a new ChunkDriver backed by a StagingManager and database executor.
// An optional S3Uploader can be provided for background S3 block persistence.
func NewChunkDriver(staging *chunk.StagingManager, db DBExecutor, uploader ...*chunk.S3Uploader) *ChunkDriver {
	var up *chunk.S3Uploader
	if len(uploader) > 0 {
		up = uploader[0]
	}
	var r *chunk.S3Reader
	if up != nil {
		r = chunk.NewS3Reader(up.Client(), up.Bucket(), staging)
	}
	return &ChunkDriver{
		staging:  staging,
		db:       db,
		uploader: up,
		reader:   r,
	}
}

// Uploader returns the configured S3Uploader, if any.
func (d *ChunkDriver) Uploader() *chunk.S3Uploader {
	return d.uploader
}

// SetUploader configures or updates the S3Uploader and initializes the S3Reader fallback if not set.
func (d *ChunkDriver) SetUploader(uploader *chunk.S3Uploader) {
	d.uploader = uploader
	if uploader != nil {
		d.reader = chunk.NewS3Reader(uploader.Client(), uploader.Bucket(), d.staging)
	}
}

// Reader returns the configured S3Reader, if any.
func (d *ChunkDriver) Reader() *chunk.S3Reader {
	return d.reader
}

// SetReader configures or updates the S3Reader.
func (d *ChunkDriver) SetReader(reader *chunk.S3Reader) {
	d.reader = reader
}


// StagingManager returns the underlying StagingManager.
func (d *ChunkDriver) StagingManager() *chunk.StagingManager {
	return d.staging
}

// DB returns the underlying DBExecutor.
func (d *ChunkDriver) DB() DBExecutor {
	return d.db
}

// LocateSlice queries the PostgreSQL chunks table for the given diskID to find the (ino, chunk_indx)
// referencing the given sliceID.
func (d *ChunkDriver) LocateSlice(ctx context.Context, diskID string, sliceID uint64) (uint64, uint32, bool, error) {
	parsedDiskUUID, err := uuid.Parse(diskID)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid disk id %q: %w", diskID, err)
	}
	schemaName := FormatDiskSchema(parsedDiskUUID)

	sliceIDBytes := binary.BigEndian.AppendUint64(nil, sliceID)
	query := fmt.Sprintf(`SELECT ino, indx, slices FROM "%s".chunks WHERE position($1 IN slices) > 0;`, schemaName)

	rows, err := d.db.Query(ctx, query, sliceIDBytes)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "3F000" || pgErr.Code == "42P01") {
			return 0, 0, false, nil
		}
		return 0, 0, false, fmt.Errorf("query chunk slices for disk %s: %w", diskID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var ino int64
		var indx int32
		var rawSlices []byte
		if err := rows.Scan(&ino, &indx, &rawSlices); err != nil {
			return 0, 0, false, fmt.Errorf("scan chunk row: %w", err)
		}

		slices, err := chunk.DecodeSlices(rawSlices)
		if err != nil {
			continue
		}
		for _, s := range slices {
			if s.ID == sliceID {
				return uint64(ino), uint32(indx), true, nil
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, false, fmt.Errorf("iterate chunk rows: %w", err)
	}

	return 0, 0, false, nil
}

// RecoverStaging triggers the recovery pipeline, scanning local NVMe staging for unflushed slices,
// resolving metadata from PostgreSQL, and re-enqueueing unpersisted slices to S3.
func (d *ChunkDriver) RecoverStaging(ctx context.Context) (*chunk.RecoveryReport, error) {
	if d.staging == nil {
		return &chunk.RecoveryReport{}, nil
	}
	return chunk.RecoverStagingPipeline(ctx, d.staging, d.uploader, d)
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

// Read reads file data at a given offset and size from NVMe staging slices or sparse holes,
// reconstructing overlapping slices and spanning across 64MB Chunk boundaries.
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

	isEOF := (offset + uint64(size)) >= uint64(fileSize)

	result := make([]byte, toRead)
	var resultOffset uint64 = 0
	curOffset := offset
	endOffset := offset + toRead

	queryChunks := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)

	for curOffset < endOffset {
		curChunk := uint32(curOffset / chunk.ChunkSize)
		chunkEnd := uint64(curChunk+1) * chunk.ChunkSize
		chunkRemaining := chunkEnd - curOffset

		chunkReadLen := endOffset - curOffset
		if chunkReadLen > chunkRemaining {
			chunkReadLen = chunkRemaining
		}
		chunkPos := uint32(curOffset % chunk.ChunkSize)

		var rawSlices []byte
		err := d.db.QueryRow(ctx, queryChunks, int64(ino), int32(curChunk)).Scan(&rawSlices)
		if errors.Is(err, pgx.ErrNoRows) {
			// Chunk row does not exist: entire range is sparse hole filled with 0x00.
			// Since result is already initialized with 0x00, no copy is necessary.
		} else if err != nil {
			return nil, false, connect.Errorf(connect.CodeInternal, "failed to query chunk slices: %s", err)
		} else if len(rawSlices) > 0 {
			slices, err := chunk.DecodeSlices(rawSlices)
			if err != nil {
				return nil, false, connect.Errorf(connect.CodeInternal, "failed to decode chunk slices: %s", err)
			}

			dst := result[resultOffset : resultOffset+chunkReadLen]
			var sliceReader chunk.SliceReader = d.staging
			if d.reader != nil {
				sliceReader = d.reader.ForChunkWithContext(ctx, curChunk)
			}
			if err := chunk.ReconstituteRangeTo(dst, diskID, slices, chunkPos, uint32(chunkReadLen), sliceReader); err != nil {
				return nil, false, connect.Errorf(connect.CodeInternal, "failed to reconstitute chunk slice: %s", err)
			}
		}

		resultOffset += chunkReadLen
		curOffset += chunkReadLen
	}

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

		// 2.5. Record slice reference in slice_refs table
		insertRefQuery := fmt.Sprintf(`
INSERT INTO "%s".slice_refs (id, size, refs)
VALUES ($1, $2, 1)
ON CONFLICT (id) DO NOTHING;
`, schemaName)
		if _, err := d.db.Exec(ctx, insertRefQuery, int64(sliceID), int32(toWrite)); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to record slice reference: %s", err)
		}

		// 2.6. Asynchronously enqueue slice to S3 uploader if configured
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



// Flush flushes dirty data for an open handle (implemented in STARSRC-123).
func (d *ChunkDriver) Flush(ctx context.Context, handle HandleContext, diskID string, ino uint64) error {
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

	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return connect.NewError(connect.CodeDeadlineExceeded, err.Error())
		}
		if errors.Is(err, context.Canceled) {
			return connect.NewError(connect.CodeCanceled, err.Error())
		}
		return err
	}

	if d.staging != nil && diskID != "" {
		if err := d.staging.SyncDisk(diskID); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to sync staging disk: %s", err)
		}
	}

	if d.uploader != nil && diskID != "" && ino > 0 {
		if err := d.uploader.WaitForIno(ctx, diskID, ino); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return connect.NewError(connect.CodeDeadlineExceeded, err.Error())
			}
			if errors.Is(err, context.Canceled) {
				return connect.NewError(connect.CodeCanceled, err.Error())
			}
			return connect.Errorf(connect.CodeInternal, "s3 persistence failure for ino %d: %s", ino, err)
		}
	}

	return nil
}

// Fsync synchronizes file data to persistent storage (implemented in STARSRC-123).
func (d *ChunkDriver) Fsync(ctx context.Context, handle HandleContext, diskID string, ino uint64, datasync bool) error {
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

	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return connect.NewError(connect.CodeDeadlineExceeded, err.Error())
		}
		if errors.Is(err, context.Canceled) {
			return connect.NewError(connect.CodeCanceled, err.Error())
		}
		return err
	}

	if d.staging != nil && diskID != "" {
		if err := d.staging.SyncDisk(diskID); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to sync staging disk: %s", err)
		}
	}

	if d.uploader != nil && diskID != "" && ino > 0 {
		if err := d.uploader.WaitForIno(ctx, diskID, ino); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return connect.NewError(connect.CodeDeadlineExceeded, err.Error())
			}
			if errors.Is(err, context.Canceled) {
				return connect.NewError(connect.CodeCanceled, err.Error())
			}
			return connect.Errorf(connect.CodeInternal, "s3 persistence failure for ino %d: %s", ino, err)
		}
	}

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

// CopyFileRange copies a range of data between files or within the same file via metadata-only zero-copy clone.
func (d *ChunkDriver) CopyFileRange(ctx context.Context, srcHandle HandleContext, srcDiskID string, srcIno uint64, srcOffset uint64, dstHandle HandleContext, dstDiskID string, dstIno uint64, dstOffset uint64, length uint64) (uint64, error) {
	if srcHandle != nil {
		if ch, ok := srcHandle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if srcDiskID == "" {
				srcDiskID = ch.diskID
			}
			if srcIno == 0 {
				srcIno = ch.ino
			}
			ch.mu.Unlock()
		}
	}
	if dstHandle != nil {
		if ch, ok := dstHandle.(*chunkFileHandle); ok {
			ch.mu.Lock()
			if dstDiskID == "" {
				dstDiskID = ch.diskID
			}
			if dstIno == 0 {
				dstIno = ch.ino
			}
			ch.mu.Unlock()
		}
	}

	if length == 0 {
		return 0, nil
	}

	srcDiskUUID, err := uuid.Parse(srcDiskID)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid src disk id: %s", err)
	}
	dstDiskUUID, err := uuid.Parse(dstDiskID)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "invalid dst disk id: %s", err)
	}
	if srcDiskUUID != dstDiskUUID {
		return 0, connect.Errorf(connect.CodeInvalidArgument, "cross-disk copy_file_range is not supported")
	}
	schemaName := FormatDiskSchema(srcDiskUUID)

	// 1. Query source Inode size and destination Inode existence
	var srcSize int64
	querySrcInode := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := d.db.QueryRow(ctx, querySrcInode, int64(srcIno)).Scan(&srcSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, connect.Errorf(connect.CodeNotFound, "src inode %d not found", srcIno)
		}
		return 0, connect.Errorf(connect.CodeInternal, "failed to query src inode: %s", err)
	}

	var dstExists bool
	checkDstInode := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM "%s".inodes WHERE ino = $1)`, schemaName)
	if err := d.db.QueryRow(ctx, checkDstInode, int64(dstIno)).Scan(&dstExists); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to query dst inode: %s", err)
	}
	if !dstExists {
		return 0, connect.Errorf(connect.CodeNotFound, "dst inode %d not found", dstIno)
	}

	if srcOffset >= uint64(srcSize) {
		return 0, nil
	}
	toCopy := length
	available := uint64(srcSize) - srcOffset
	if toCopy > available {
		toCopy = available
	}
	if toCopy == 0 {
		return 0, nil
	}

	srcStart := srcOffset
	srcEnd := srcOffset + toCopy

	startChunkIndx := int32(srcStart / chunk.ChunkSize)
	endChunkIndx := int32((srcEnd - 1) / chunk.ChunkSize)

	// 2. Query source Inode chunks intersecting [srcOffset, srcOffset+toCopy)
	queryChunks := fmt.Sprintf(`
SELECT indx, slices FROM "%s".chunks
WHERE ino = $1 AND indx >= $2 AND indx <= $3
ORDER BY indx ASC;
`, schemaName)

	rows, err := d.db.Query(ctx, queryChunks, int64(srcIno), startChunkIndx, endChunkIndx)
	if err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to query src chunks: %s", err)
	}
	defer rows.Close()

	type chunkRow struct {
		indx   uint32
		slices []chunk.Slice
	}
	var srcRows []chunkRow

	for rows.Next() {
		var cIndx int32
		var rawSlices []byte
		if err := rows.Scan(&cIndx, &rawSlices); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to scan src chunk: %s", err)
		}
		slices, err := chunk.DecodeSlices(rawSlices)
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to decode src chunk slices: %s", err)
		}
		srcRows = append(srcRows, chunkRow{
			indx:   uint32(cIndx),
			slices: slices,
		})
	}
	if err := rows.Err(); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to iterate src chunks: %s", err)
	}
	rows.Close()

	// 3. Map intersecting source slices to destination chunk intervals
	dstChunks := make(map[uint32][]byte)
	var dstChunkOrder []uint32

	type sliceRefUpdate struct {
		id   uint64
		size uint32
	}
	var sliceRefsToInc []sliceRefUpdate

	for _, cr := range srcRows {
		chunkBase := uint64(cr.indx) * uint64(chunk.ChunkSize)
		for _, s := range cr.slices {
			sliceStart := chunkBase + uint64(s.Pos)
			sliceEnd := sliceStart + uint64(s.Len)

			overlapStart := max(sliceStart, srcStart)
			overlapEnd := min(sliceEnd, srcEnd)
			if overlapStart >= overlapEnd {
				continue
			}

			subLen := overlapEnd - overlapStart
			subOff := s.Off + uint32(overlapStart-sliceStart)
			dstFileStart := dstOffset + (overlapStart - srcStart)

			curDstPos := dstFileStart
			remainingLen := subLen
			curPayloadOff := subOff

			for remainingLen > 0 {
				dstChunkIndx := uint32(curDstPos / chunk.ChunkSize)
				dstChunkOffset := uint32(curDstPos % chunk.ChunkSize)
				dstChunkRemaining := uint64(chunk.ChunkSize - dstChunkOffset)

				pieceLen := remainingLen
				if pieceLen > dstChunkRemaining {
					pieceLen = dstChunkRemaining
				}

				clonedSlice := chunk.Slice{
					Pos:  dstChunkOffset,
					ID:   s.ID,
					Size: s.Size,
					Off:  curPayloadOff,
					Len:  uint32(pieceLen),
				}
				if err := clonedSlice.Validate(); err != nil {
					return 0, connect.Errorf(connect.CodeInternal, "cloned slice invalid: %s", err)
				}

				encoded := chunk.EncodeSlice(clonedSlice)
				if _, exists := dstChunks[dstChunkIndx]; !exists {
					dstChunkOrder = append(dstChunkOrder, dstChunkIndx)
				}
				dstChunks[dstChunkIndx] = append(dstChunks[dstChunkIndx], encoded...)

				curDstPos += pieceLen
				curPayloadOff += uint32(pieceLen)
				remainingLen -= pieceLen
			}

			sliceRefsToInc = append(sliceRefsToInc, sliceRefUpdate{
				id:   s.ID,
				size: s.Size,
			})
		}
	}

	// 4. Increment reference count in slice_refs for each mapped source slice
	if len(sliceRefsToInc) > 0 {
		refCounts := make(map[uint64]int)
		refSizes := make(map[uint64]uint32)
		for _, ref := range sliceRefsToInc {
			refCounts[ref.id]++
			refSizes[ref.id] = ref.size
		}

		countsMap := make(map[int][]int64)
		for id, cnt := range refCounts {
			countsMap[cnt] = append(countsMap[cnt], int64(id))
		}

		for cnt, ids := range countsMap {
			updateRefSQL := fmt.Sprintf(`UPDATE "%s".slice_refs SET refs = refs + $1 WHERE id = ANY($2);`, schemaName)
			res, err := d.db.Exec(ctx, updateRefSQL, int32(cnt), ids)
			if err != nil {
				return 0, connect.Errorf(connect.CodeInternal, "failed to increment slice reference count: %s", err)
			}
			if res.RowsAffected() < int64(len(ids)) {
				for _, id := range ids {
					insertRefSQL := fmt.Sprintf(`
INSERT INTO "%s".slice_refs (id, size, refs)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET refs = "%s".slice_refs.refs + EXCLUDED.refs;
`, schemaName, schemaName)
					if _, err := d.db.Exec(ctx, insertRefSQL, id, int32(refSizes[uint64(id)]), int32(cnt+1)); err != nil {
						return 0, connect.Errorf(connect.CodeInternal, "failed to insert slice reference count: %s", err)
					}
				}
			}
		}
	}

	// 5. Append cloned slice metadata to destination Inode chunks
	if len(dstChunkOrder) > 0 {
		valuePlaceholders := make([]string, len(dstChunkOrder))
		args := make([]any, 0, len(dstChunkOrder)*3)
		for i, indx := range dstChunkOrder {
			p1 := i*3 + 1
			p2 := i*3 + 2
			p3 := i*3 + 3
			valuePlaceholders[i] = fmt.Sprintf("($%d, $%d, $%d)", p1, p2, p3)
			args = append(args, int64(dstIno), int32(indx), dstChunks[indx])
		}

		insertChunksSQL := fmt.Sprintf(`
INSERT INTO "%s".chunks (ino, indx, slices)
VALUES %s
ON CONFLICT (ino, indx)
DO UPDATE SET slices = chunks.slices || EXCLUDED.slices;
`, schemaName, strings.Join(valuePlaceholders, ", "))

		if _, err := d.db.Exec(ctx, insertChunksSQL, args...); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to append dst chunk metadata: %s", err)
		}
	}

	// 6. Update destination Inode size/mtime/ctime in PostgreSQL
	now := time.Now()
	newEnd := int64(dstOffset + toCopy)
	updateInodeSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = GREATEST(size, $1::bigint),
    blocks = (GREATEST(size, $1::bigint) + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
	if _, err := d.db.Exec(ctx, updateInodeSQL, newEnd, now, int64(dstIno)); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to update dst inode metadata: %s", err)
	}

	return toCopy, nil
}

// Lseek seeks for data or holes within the file.
func (d *ChunkDriver) Lseek(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, whence uint32) (uint64, error) {
	return offset, nil
}

// Close closes the driver handle and frees resources.
func (d *ChunkDriver) Close(ctx context.Context, handle HandleContext) error {
	return nil
}
