package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/services/storage/internal/chunk"
	"github.com/starsrc/bowl/services/storage/internal/config"

	"github.com/minio/minio-go/v7"
)


func setupChunkHarness(t *testing.T) (*testHarness, *chunk.StagingManager, *ChunkDriver) {
	t.Helper()
	h := setupTestHarness(t)

	stagingDir := filepath.Join(t.TempDir(), "staging")
	stagingMgr := chunk.NewStagingManager(stagingDir)
	chunkDriver := NewChunkDriver(stagingMgr, h.dbPool)

	// Reconfigure storage service with ChunkDriver
	h.storageSvc.driver = chunkDriver

	return h, stagingMgr, chunkDriver
}

func TestChunkDriver_Write_EndToEnd_SingleSlice(t *testing.T) {
	h, stagingMgr, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-test-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. Mount virtual disk
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. Create file under root directory (ino 1)
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test-chunk-file.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 3. Write 4096 bytes at offset 0
	payload := make([]byte, 4096)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	start := time.Now()
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload)), writeResp.GetWritten())

	// Assert sub-10ms write latency
	assert.Less(t, elapsed, 10*time.Millisecond, "write latency must be under 10ms, was %v", elapsed)

	// 4. Assert NVMe Staging physical file exists and contains correct bytes
	expectedSlicePath := stagingMgr.SlicePath(diskID.String(), 1)
	fi, err := os.Stat(expectedSlicePath)
	require.NoError(t, err, "staging slice file must exist at %s", expectedSlicePath)
	assert.Equal(t, int64(len(payload)), fi.Size())

	sliceBytes, err := stagingMgr.ReadSlice(diskID.String(), 1)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(payload, sliceBytes), "staging slice file bytes must match written payload")

	// 5. Assert chunks table row exists and contains the appended 24-byte slice
	var rawSlices []byte
	querySQL := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
	err = h.dbPool.QueryRow(ctx, querySQL, int64(ino), int32(0)).Scan(&rawSlices)
	require.NoError(t, err)
	assert.Len(t, rawSlices, chunk.SliceBytes, "single write must produce exactly 24 bytes in chunks table")

	slices, err := chunk.DecodeSlices(rawSlices)
	require.NoError(t, err)
	require.Len(t, slices, 1)

	assert.Equal(t, uint32(0), slices[0].Pos)
	assert.Equal(t, uint64(1), slices[0].ID)
	assert.Equal(t, uint32(4096), slices[0].Size)
	assert.Equal(t, uint32(0), slices[0].Off)
	assert.Equal(t, uint32(4096), slices[0].Len)

	// 6. Assert Inode metadata in PostgreSQL
	var (
		inodeSize int64
		mtime     time.Time
	)
	inodeSQL := fmt.Sprintf(`SELECT size, mtime FROM "%s".inodes WHERE ino = $1`, schemaName)
	err = h.dbPool.QueryRow(ctx, inodeSQL, int64(ino)).Scan(&inodeSize, &mtime)
	require.NoError(t, err)
	assert.Equal(t, int64(4096), inodeSize)
	assert.False(t, mtime.IsZero())
}

func TestChunkDriver_Write_EndToEnd_MultipleAppendsSameChunk(t *testing.T) {
	h, stagingMgr, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-multi-append-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "multi-append.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// First write: 1024 bytes at offset 0
	payload1 := bytes.Repeat([]byte("A"), 1024)
	write1, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload1,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(1024), write1.GetWritten())

	// Second write: 2048 bytes at offset 1024
	payload2 := bytes.Repeat([]byte("B"), 2048)
	write2, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1024,
		Data:      payload2,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2048), write2.GetWritten())

	// Verify both staging files
	slice1Data, err := stagingMgr.ReadSlice(diskID.String(), 1)
	require.NoError(t, err)
	assert.Equal(t, payload1, slice1Data)

	slice2Data, err := stagingMgr.ReadSlice(diskID.String(), 2)
	require.NoError(t, err)
	assert.Equal(t, payload2, slice2Data)

	// Verify chunks table has 48 bytes (2 slices)
	var rawSlices []byte
	querySQL := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
	err = h.dbPool.QueryRow(ctx, querySQL, int64(ino), int32(0)).Scan(&rawSlices)
	require.NoError(t, err)
	assert.Len(t, rawSlices, 2*chunk.SliceBytes)

	slices, err := chunk.DecodeSlices(rawSlices)
	require.NoError(t, err)
	require.Len(t, slices, 2)

	assert.Equal(t, chunk.Slice{Pos: 0, ID: 1, Size: 1024, Off: 0, Len: 1024}, slices[0])
	assert.Equal(t, chunk.Slice{Pos: 1024, ID: 2, Size: 2048, Off: 0, Len: 2048}, slices[1])

	// Inode size should be 3072
	var inodeSize int64
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName), int64(ino)).Scan(&inodeSize)
	require.NoError(t, err)
	assert.Equal(t, int64(3072), inodeSize)
}

func TestChunkDriver_Write_EndToEnd_AcrossChunks(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-across-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "across-chunks.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write at chunk 0 (offset 0)
	payload0 := []byte("chunk 0 data")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload0,
	})
	require.NoError(t, err)

	// Write at chunk 1 (offset 64 MiB = 67108864)
	payload1 := []byte("chunk 1 data")
	offset1 := uint64(chunk.ChunkSize)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset1,
		Data:      payload1,
	})
	require.NoError(t, err)

	// Verify two distinct rows in chunks table
	rows, err := h.dbPool.Query(ctx, fmt.Sprintf(`SELECT indx, slices FROM "%s".chunks WHERE ino = $1 ORDER BY indx`, schemaName), int64(ino))
	require.NoError(t, err)
	defer rows.Close()

	type chunkRow struct {
		indx   int32
		slices []byte
	}
	var chunkRows []chunkRow
	for rows.Next() {
		var r chunkRow
		err := rows.Scan(&r.indx, &r.slices)
		require.NoError(t, err)
		chunkRows = append(chunkRows, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, chunkRows, 2)
	assert.Equal(t, int32(0), chunkRows[0].indx)
	assert.Equal(t, int32(1), chunkRows[1].indx)

	// Verify inode size
	var inodeSize int64
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName), int64(ino)).Scan(&inodeSize)
	require.NoError(t, err)
	assert.Equal(t, int64(offset1)+int64(len(payload1)), inodeSize)
}

func TestChunkDriver_Write_Latency_Sub10ms(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-latency-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "latency-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	payload := make([]byte, 16*1024) // 16 KB
	_, _ = rand.Read(payload)

	// Warm-up write to ensure connection pool and caches are warm
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// Perform 30 sequential writes and record latency
	const iterations = 30
	var totalDuration time.Duration
	var maxDuration time.Duration

	for i := 1; i <= iterations; i++ {
		offset := uint64(i * len(payload))
		start := time.Now()
		_, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    offset,
			Data:      payload,
		})
		elapsed := time.Since(start)
		require.NoError(t, err)

		totalDuration += elapsed
		if elapsed > maxDuration {
			maxDuration = elapsed
		}

		assert.Less(t, elapsed, 10*time.Millisecond, "write #%d latency (%v) exceeded 10ms SLA", i, elapsed)
	}

	avgLatency := totalDuration / iterations
	t.Logf("Write latency stats: average=%v, max=%v over %d writes", avgLatency, maxDuration, iterations)
	assert.Less(t, avgLatency, 5*time.Millisecond, "average write latency should be under 5ms")
}

func TestChunkDriver_DirectOperations(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// Ensure schema exists
	_, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "direct-op-client",
		ReadOnly: false,
	})
	require.NoError(t, err)

	// 1. Open
	handle, err := driver.Open(ctx, diskID.String(), 100, bowlv1.OpenMode_OPEN_MODE_READ_WRITE, 0)
	require.NoError(t, err)
	require.NotNil(t, handle)

	// 2. Direct Write
	payload := []byte("direct chunk write payload")
	written, err := driver.Write(ctx, handle, diskID.String(), 100, 0, payload)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload)), written)

	// Verify slice in staging
	sliceBytes, err := stagingMgr.ReadSlice(diskID.String(), 1)
	require.NoError(t, err)
	assert.Equal(t, payload, sliceBytes)

	// Verify chunk in DB
	var rawSlices []byte
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName), int64(100), int32(0)).Scan(&rawSlices)
	require.NoError(t, err)
	assert.Len(t, rawSlices, chunk.SliceBytes)

	// Insert inode 100 into inodes table to allow driver.Read
	insertInodeSQL := fmt.Sprintf(`
INSERT INTO "%s".inodes (ino, parent, name, type, mode, nlink, size, atime, mtime, ctime)
VALUES (100, 1, 'direct-file', 1, 0644, 1, $1, NOW(), NOW(), NOW())
`, schemaName)
	_, err = h.dbPool.Exec(ctx, insertInodeSQL, int64(len(payload)))
	require.NoError(t, err)

	// 3. Direct Read
	readData, eof, err := driver.Read(ctx, handle, diskID.String(), 100, 0, uint32(len(payload)))
	require.NoError(t, err)
	assert.Equal(t, payload, readData)
	assert.True(t, eof)

	// 4. Close
	err = driver.Close(ctx, handle)
	require.NoError(t, err)
}

func TestChunkDriver_Read_EndToEnd_RoundTrip(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-read-roundtrip-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "read-roundtrip.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 8192 bytes
	payload := make([]byte, 8192)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload)), writeResp.GetWritten())

	// Read 8192 bytes back
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      8192,
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp.GetData())
	assert.True(t, readResp.GetEof())
}


func TestChunkDriver_Read_EndToEnd_BoundaryCases(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-read-boundary-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "read-boundary.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 4096 bytes
	payload := make([]byte, 4096)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// 1. Zero size read
	respZero, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      0,
	})
	require.NoError(t, err)
	assert.Empty(t, respZero.GetData())
	assert.False(t, respZero.GetEof())

	// 2. Read exactly at EOF (offset == 4096)
	respAtEOF, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    4096,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.Empty(t, respAtEOF.GetData())
	assert.True(t, respAtEOF.GetEof())

	// 3. Read past EOF (offset > 4096)
	respPastEOF, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    5000,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.Empty(t, respPastEOF.GetData())
	assert.True(t, respPastEOF.GetEof())

	// 4. Partial read from beginning: offset 0, size 1024
	respPart1, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.Equal(t, payload[:1024], respPart1.GetData())
	assert.False(t, respPart1.GetEof())

	// 5. Partial read inside: offset 1000, size 500
	respPart2, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1000,
		Size:      500,
	})
	require.NoError(t, err)
	assert.Equal(t, payload[1000:1500], respPart2.GetData())
	assert.False(t, respPart2.GetEof())

	// 6. Partial read reaching EOF: offset 2048, size 2048
	respPart3, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    2048,
		Size:      2048,
	})
	require.NoError(t, err)
	assert.Equal(t, payload[2048:4096], respPart3.GetData())
	assert.True(t, respPart3.GetEof())

	// 7. Oversized read reaching EOF: offset 3000, size 2000 (only 1096 available)
	respOversized, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    3000,
		Size:      2000,
	})
	require.NoError(t, err)
	assert.Equal(t, payload[3000:4096], respOversized.GetData())
	assert.Equal(t, 1096, len(respOversized.GetData()))
	assert.True(t, respOversized.GetEof())
}

func TestChunkDriver_Read_SparseHole(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-sparse-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "sparse-file.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Update inode size directly in DB to create a sparse file of 4096 bytes with no chunk rows
	updateSQL := fmt.Sprintf(`UPDATE "%s".inodes SET size = 4096 WHERE ino = $1`, schemaName)
	_, err = h.dbPool.Exec(ctx, updateSQL, int64(ino))
	require.NoError(t, err)

	// Read 4096 bytes from the hole
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      4096,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 4096), readResp.GetData())
	assert.True(t, readResp.GetEof())
}

func TestChunkDriver_Read_InodeNotFound(t *testing.T) {
	h, _, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	_, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "read-not-found-client",
		ReadOnly: false,
	})
	require.NoError(t, err)

	// Inode 99999 does not exist
	data, eof, err := driver.Read(ctx, nil, diskID.String(), 99999, 0, 1024)
	assert.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	assert.Nil(t, data)
	assert.False(t, eof)
}

func TestChunkDriver_DirectWrite_SpanningBoundary(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	_, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "direct-spanning-client",
		ReadOnly: false,
	})
	require.NoError(t, err)

	handle, err := driver.Open(ctx, diskID.String(), 200, bowlv1.OpenMode_OPEN_MODE_READ_WRITE, 0)
	require.NoError(t, err)
	defer func() { _ = driver.Close(ctx, handle) }()

	// Write spanning from Chunk 2 to Chunk 3:
	// Chunk 2 starts at 2 * ChunkSize = 134217728.
	// Offset: Chunk 2 at 63 MiB offset = 2 * ChunkSize + 63 MiB
	// Length: 2 MiB (1 MiB in Chunk 2, 1 MiB in Chunk 3)
	offset := uint64(2*chunk.ChunkSize + 63*1024*1024)
	payload := []byte("first-half-chunk2-data" + string(bytes.Repeat([]byte("A"), 1024*1024-22)) +
		"second-half-chunk3-data" + string(bytes.Repeat([]byte("B"), 1024*1024-23)))

	written, err := driver.Write(ctx, handle, diskID.String(), 200, offset, payload)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload)), written)

	// Verify Chunk 2
	var rawSlices2 []byte
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = 200 AND indx = 2`, schemaName)).Scan(&rawSlices2)
	require.NoError(t, err)
	slices2, err := chunk.DecodeSlices(rawSlices2)
	require.NoError(t, err)
	require.Len(t, slices2, 1)
	assert.Equal(t, uint32(63*1024*1024), slices2[0].Pos)
	assert.Equal(t, uint32(1024*1024), slices2[0].Len)

	// Verify Chunk 3
	var rawSlices3 []byte
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = 200 AND indx = 3`, schemaName)).Scan(&rawSlices3)
	require.NoError(t, err)
	slices3, err := chunk.DecodeSlices(rawSlices3)
	require.NoError(t, err)
	require.Len(t, slices3, 1)
	assert.Equal(t, uint32(0), slices3[0].Pos)
	assert.Equal(t, uint32(1024*1024), slices3[0].Len)

	// Verify staging slice data
	slice2Data, err := stagingMgr.ReadSlice(diskID.String(), slices2[0].ID)
	require.NoError(t, err)
	assert.Equal(t, payload[:1024*1024], slice2Data)

	slice3Data, err := stagingMgr.ReadSlice(diskID.String(), slices3[0].ID)
	require.NoError(t, err)
	assert.Equal(t, payload[1024*1024:], slice3Data)
}

func TestChunkDriver_Write_EndToEnd_SpanningBoundary(t *testing.T) {
	h, stagingMgr, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-spanning-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "spanning-boundary.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 2 MiB spanning across Chunk 0 and Chunk 1:
	// Start at 63 MiB (63 * 1024 * 1024), write 2 MiB (2 * 1024 * 1024)
	offset := uint64(63 * 1024 * 1024)
	size := 2 * 1024 * 1024
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset,
		Data:      payload,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(size), writeResp.GetWritten())

	// Verify Chunk 0 (indx 0)
	var rawSlices0 []byte
	query0 := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName)
	err = h.dbPool.QueryRow(ctx, query0, int64(ino)).Scan(&rawSlices0)
	require.NoError(t, err)
	slices0, err := chunk.DecodeSlices(rawSlices0)
	require.NoError(t, err)
	require.Len(t, slices0, 1)

	expectedChunk0Len := uint32(1 * 1024 * 1024)
	assert.Equal(t, uint32(63*1024*1024), slices0[0].Pos)
	assert.Equal(t, expectedChunk0Len, slices0[0].Size)
	assert.Equal(t, uint32(0), slices0[0].Off)
	assert.Equal(t, expectedChunk0Len, slices0[0].Len)

	// Verify Chunk 1 (indx 1)
	var rawSlices1 []byte
	query1 := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 1`, schemaName)
	err = h.dbPool.QueryRow(ctx, query1, int64(ino)).Scan(&rawSlices1)
	require.NoError(t, err)
	slices1, err := chunk.DecodeSlices(rawSlices1)
	require.NoError(t, err)
	require.Len(t, slices1, 1)

	expectedChunk1Len := uint32(1 * 1024 * 1024)
	assert.Equal(t, uint32(0), slices1[0].Pos)
	assert.Equal(t, expectedChunk1Len, slices1[0].Size)
	assert.Equal(t, uint32(0), slices1[0].Off)
	assert.Equal(t, expectedChunk1Len, slices1[0].Len)

	// Verify staging slice data
	slice0Data, err := stagingMgr.ReadSlice(diskID.String(), slices0[0].ID)
	require.NoError(t, err)
	assert.Equal(t, payload[:expectedChunk0Len], slice0Data)

	slice1Data, err := stagingMgr.ReadSlice(diskID.String(), slices1[0].ID)
	require.NoError(t, err)
	assert.Equal(t, payload[expectedChunk0Len:], slice1Data)

	// Verify inode metadata
	var inodeSize int64
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName), int64(ino)).Scan(&inodeSize)
	require.NoError(t, err)
	assert.Equal(t, int64(offset)+int64(size), inodeSize)
}

func TestChunkDriver_Write_EndToEnd_MultiChunk_ThreeChunks(t *testing.T) {
	h, stagingMgr, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-3chunks-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "three-chunks.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 130 MiB write starting at offset 0:
	// Chunk 0: 64 MiB
	// Chunk 1: 64 MiB
	// Chunk 2: 2 MiB
	totalSize := 130 * 1024 * 1024
	payload := make([]byte, totalSize)
	// Fill with deterministic pattern
	for i := 0; i < totalSize; i += 1024 * 1024 {
		pattern := byte(i / (1024 * 1024))
		for j := 0; j < 1024*1024 && i+j < totalSize; j++ {
			payload[i+j] = pattern
		}
	}

	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(totalSize), writeResp.GetWritten())

	// Query all chunk rows
	rows, err := h.dbPool.Query(ctx, fmt.Sprintf(`SELECT indx, slices FROM "%s".chunks WHERE ino = $1 ORDER BY indx`, schemaName), int64(ino))
	require.NoError(t, err)
	defer rows.Close()

	type chunkInfo struct {
		indx   int32
		slices []chunk.Slice
	}
	var chunks []chunkInfo
	for rows.Next() {
		var indx int32
		var raw []byte
		err := rows.Scan(&indx, &raw)
		require.NoError(t, err)
		decoded, err := chunk.DecodeSlices(raw)
		require.NoError(t, err)
		chunks = append(chunks, chunkInfo{indx: indx, slices: decoded})
	}
	require.NoError(t, rows.Err())

	require.Len(t, chunks, 3)

	// Chunk 0: 64 MiB
	assert.Equal(t, int32(0), chunks[0].indx)
	require.Len(t, chunks[0].slices, 1)
	assert.Equal(t, uint32(0), chunks[0].slices[0].Pos)
	assert.Equal(t, uint32(chunk.ChunkSize), chunks[0].slices[0].Size)
	assert.Equal(t, uint32(chunk.ChunkSize), chunks[0].slices[0].Len)

	// Chunk 1: 64 MiB
	assert.Equal(t, int32(1), chunks[1].indx)
	require.Len(t, chunks[1].slices, 1)
	assert.Equal(t, uint32(0), chunks[1].slices[0].Pos)
	assert.Equal(t, uint32(chunk.ChunkSize), chunks[1].slices[0].Size)
	assert.Equal(t, uint32(chunk.ChunkSize), chunks[1].slices[0].Len)

	// Chunk 2: 2 MiB
	expectedChunk2Len := uint32(2 * 1024 * 1024)
	assert.Equal(t, int32(2), chunks[2].indx)
	require.Len(t, chunks[2].slices, 1)
	assert.Equal(t, uint32(0), chunks[2].slices[0].Pos)
	assert.Equal(t, expectedChunk2Len, chunks[2].slices[0].Size)
	assert.Equal(t, expectedChunk2Len, chunks[2].slices[0].Len)

	// Verify staging slice data for Chunk 2
	slice2Data, err := stagingMgr.ReadSlice(diskID.String(), chunks[2].slices[0].ID)
	require.NoError(t, err)
	assert.Equal(t, payload[128*1024*1024:], slice2Data)

	// Verify inode metadata size
	var inodeSize int64
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName), int64(ino)).Scan(&inodeSize)
	require.NoError(t, err)
	assert.Equal(t, int64(totalSize), inodeSize)
}

func TestChunkDriver_Write_S3AsyncUploadPipeline(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "pipeline-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. Mount disk
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. Setup S3Uploader (using MinIO or mock)
	cfg := config.S3Config{
		Enabled:        true,
		Endpoint:       "http://127.0.0.1:9000",
		Bucket:         "bowl-data",
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
		Region:         "us-east-1",
		UseSSL:         false,
		ForcePathStyle: true,
	}

	uploader, err := chunk.NewS3Uploader(cfg, stagingMgr,
		chunk.WithWorkers(2),
		chunk.WithInitialBackoff(10*time.Millisecond),
	)
	require.NoError(t, err)

	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	// 3. Create file
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "pipeline-test.dat",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 4. Write 1MB payload via Connect-RPC client (FUSE standard chunk write)
	payloadSize := 1024 * 1024
	payload := bytes.Repeat([]byte("X"), payloadSize)
	start := time.Now()
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, uint32(payloadSize), writeResp.GetWritten())
	assert.Less(t, elapsed, 100*time.Millisecond, "NVMe staging fast path must ACK rapidly")

	// 5. Wait for Inode upload barrier
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()

	err = uploader.WaitForIno(waitCtx, diskID.String(), ino)
	require.NoError(t, err)

	// 6. Verify object exists in MinIO
	expectedKey := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)

	defer func() {
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, expectedKey, minio.RemoveObjectOptions{})
	}()

	stat, err := minioClient.StatObject(context.Background(), cfg.Bucket, expectedKey, minio.StatObjectOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(payloadSize), stat.Size)
}

func TestChunkDriver_Read_OverlappingOverwrites(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-overwrite-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "overwrite-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 1. Base write: 100 bytes of 'A' at offset 0
	dataA := bytes.Repeat([]byte("A"), 100)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      dataA,
	})
	require.NoError(t, err)

	// 2. Overwrite: 20 bytes of 'B' at offset 10
	dataB := bytes.Repeat([]byte("B"), 20)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    10,
		Data:      dataB,
	})
	require.NoError(t, err)

	// 3. Overwrite: 10 bytes of 'C' at offset 50
	dataC := bytes.Repeat([]byte("C"), 10)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    50,
		Data:      dataC,
	})
	require.NoError(t, err)

	// Expected content for 0..100:
	// 0..9: 'A'
	// 10..29: 'B'
	// 30..49: 'A'
	// 50..59: 'C'
	// 60..99: 'A'
	expected := make([]byte, 100)
	copy(expected[0:100], dataA)
	copy(expected[10:30], dataB)
	copy(expected[50:60], dataC)

	// Read full range 0..100
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      100,
	})
	require.NoError(t, err)
	assert.Equal(t, expected, readResp.GetData())
	assert.True(t, readResp.GetEof())

	// Read sub-range 5..35 (covering partial A and all B and partial A)
	readSub1, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    5,
		Size:      30,
	})
	require.NoError(t, err)
	assert.Equal(t, expected[5:35], readSub1.GetData())
	assert.False(t, readSub1.GetEof())

	// Read sub-range 45..65 (covering partial A, all C, partial A)
	readSub2, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    45,
		Size:      20,
	})
	require.NoError(t, err)
	assert.Equal(t, expected[45:65], readSub2.GetData())
	assert.False(t, readSub2.GetEof())
}

func TestChunkDriver_Read_SparseHolesWithData(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-sparse-data-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "sparse-data-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 10 bytes at offset 0 ('X')
	dataX := bytes.Repeat([]byte("X"), 10)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      dataX,
	})
	require.NoError(t, err)

	// Write 10 bytes at offset 50 ('Y')
	dataY := bytes.Repeat([]byte("Y"), 10)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    50,
		Data:      dataY,
	})
	require.NoError(t, err)

	// Read range 0..60
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      60,
	})
	require.NoError(t, err)
	data := readResp.GetData()
	require.Len(t, data, 60)
	assert.Equal(t, dataX, data[0:10])
	assert.Equal(t, make([]byte, 40), data[10:50], "bytes 10..49 must be zero sparse hole")
	assert.Equal(t, dataY, data[50:60])
	assert.True(t, readResp.GetEof())
}

func TestChunkDriver_Read_MultiChunk_SpanningBoundary(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunk-driver-multichunk-read-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "multichunk-read.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 2 MiB spanning across Chunk 0 and Chunk 1:
	// Chunk 0: from 63 MiB to 64 MiB (1 MiB)
	// Chunk 1: from 0 to 1 MiB (1 MiB)
	offset := uint64(63 * 1024 * 1024)
	size := 2 * 1024 * 1024
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset,
		Data:      payload,
	})
	require.NoError(t, err)

	// Read spanning Chunk 0 and Chunk 1:
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset,
		Size:      uint32(size),
	})
	require.NoError(t, err)
	require.Equal(t, len(payload), len(readResp.GetData()), "read length should match payload length")
	assert.True(t, bytes.Equal(payload, readResp.GetData()), "read data should match payload")
	assert.True(t, readResp.GetEof())

	// Read spanning boundary with sparse holes before it:
	// Read from 62 MiB (sparse hole 1 MiB) through 65 MiB (1 MiB data in chunk 0 + 1 MiB data in chunk 1)
	// Total requested size = 3 MiB
	readRespSpanningHole, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    uint64(62 * 1024 * 1024),
		Size:      uint32(3 * 1024 * 1024),
	})
	require.NoError(t, err)
	dataSpanning := readRespSpanningHole.GetData()
	require.Equal(t, 3*1024*1024, len(dataSpanning))
	assert.True(t, bytes.Equal(make([]byte, 1024*1024), dataSpanning[:1024*1024]), "62MiB..63MiB must be zeros")
	assert.True(t, bytes.Equal(payload, dataSpanning[1024*1024:]), "63MiB..65MiB must match payload")
	assert.True(t, readRespSpanningHole.GetEof())
}

func TestChunkDriver_DirectRead_MultiChunkAndEOF(t *testing.T) {
	h, _, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	_, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "direct-multichunk-client",
		ReadOnly: false,
	})
	require.NoError(t, err)

	handle, err := driver.Open(ctx, diskID.String(), 300, bowlv1.OpenMode_OPEN_MODE_READ_WRITE, 0)
	require.NoError(t, err)
	defer func() { _ = driver.Close(ctx, handle) }()

	// Write data at Chunk 0 (offset 63.5 MiB, size 1 MiB -> ends at 64.5 MiB in Chunk 1)
	startOffset := uint64(63*1024*1024 + 512*1024)
	payload := bytes.Repeat([]byte("M"), 1024*1024)
	written, err := driver.Write(ctx, handle, diskID.String(), 300, startOffset, payload)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload)), written)

	fileSize := startOffset + uint64(len(payload))

	// Insert inode with fileSize
	insertInodeSQL := fmt.Sprintf(`
INSERT INTO "%s".inodes (ino, parent, name, type, mode, nlink, size, atime, mtime, ctime)
VALUES (300, 1, 'direct-multichunk', 1, 0644, 1, $1, NOW(), NOW(), NOW())
`, schemaName)
	_, err = h.dbPool.Exec(ctx, insertInodeSQL, int64(fileSize))
	require.NoError(t, err)

	// Case 1: Read exactly within data spanning Chunk 0 and Chunk 1 (not hitting EOF)
	// Read 512 KiB spanning the 64 MiB boundary: 256 KiB in Chunk 0, 256 KiB in Chunk 1
	subOffset := uint64(chunk.ChunkSize - 256*1024)
	subSize := uint32(512 * 1024)
	data, eof, err := driver.Read(ctx, handle, diskID.String(), 300, subOffset, subSize)
	require.NoError(t, err)
	require.Equal(t, int(subSize), len(data))
	assert.False(t, eof)
	assert.True(t, bytes.Equal(bytes.Repeat([]byte("M"), int(subSize)), data))

	// Case 2: Read reaching past EOF (offset + size > fileSize)
	// Reading from subOffset with size = 2 MiB (file ends at 64.5 MiB, so available is only 768 KiB)
	oversized := uint32(2 * 1024 * 1024)
	expectedAvailable := fileSize - subOffset
	dataEOF, eof, err := driver.Read(ctx, handle, diskID.String(), 300, subOffset, oversized)
	require.NoError(t, err)
	assert.True(t, eof, "eof must be true when requested range reaches or exceeds fileSize")
	require.Equal(t, int(expectedAvailable), len(dataEOF), "data must be truncated at fileSize")
	assert.True(t, bytes.Equal(bytes.Repeat([]byte("M"), int(expectedAvailable)), dataEOF))
}
