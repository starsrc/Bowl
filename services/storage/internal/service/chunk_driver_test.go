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
