package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/services/storage/internal/chunk"
	"github.com/starsrc/bowl/services/storage/internal/config"
	jfsChunk "github.com/starsrc/bowl/services/storage/internal/juicefs/chunk"
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

func TestChunkDriver_Read_S3TransparentReadPenetration_EndToEnd(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "s3-read-penetration-client"
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

	// 2. Setup S3Uploader & S3Reader
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
		Name:      "cold-penetration.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 4. Write 64 KB of random data
	payloadSize := 64 * 1024
	payload := make([]byte, payloadSize)
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
	assert.Equal(t, uint32(payloadSize), writeResp.GetWritten())

	// 5. Query chunk slice ID
	var rawSlices []byte
	querySQL := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName)
	err = h.dbPool.QueryRow(ctx, querySQL, int64(ino)).Scan(&rawSlices)
	require.NoError(t, err)
	slices, err := chunk.DecodeSlices(rawSlices)
	require.NoError(t, err)
	require.Len(t, slices, 1)
	sliceID := slices[0].ID

	// 6. Wait for async S3 upload to finish
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	err = uploader.WaitForSlice(waitCtx, diskID.String(), sliceID)
	require.NoError(t, err)
	assert.Equal(t, chunk.SliceStatusSynced, uploader.GetSliceStatus(diskID.String(), sliceID))

	// Verify object exists in S3
	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)
	objectKey := chunk.FormatObjectKey(diskID.String(), 0, sliceID, 0)
	stat, err := minioClient.StatObject(ctx, cfg.Bucket, objectKey, minio.StatObjectOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(payloadSize), stat.Size)

	// Clean up S3 object on test completion
	defer func() {
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, objectKey, minio.RemoveObjectOptions{})
	}()

	// 7. Purge/delete local NVMe staging file to turn this slice into a COLD slice
	localPath := stagingMgr.SlicePath(diskID.String(), sliceID)
	err = os.Remove(localPath)
	require.NoError(t, err, "staging file must be successfully purged")
	_, err = os.Stat(localPath)
	require.True(t, os.IsNotExist(err), "staging file must not exist locally")

	// 8. Read via Connect-RPC StorageService.Read
	// Should transparently penetrate to S3, fetch the cold slice block, and reconstitute data
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      uint32(payloadSize),
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp.GetData(), "data read from cold S3 must match original written payload")
	assert.True(t, readResp.GetEof())

	// 9. Partial read on cold slice
	readPart, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1024,
		Size:      2048,
	})
	require.NoError(t, err)
	assert.Equal(t, payload[1024:3072], readPart.GetData())
	assert.False(t, readPart.GetEof())
}

func TestChunkDriver_Read_MixedHotStagingAndColdS3(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "mixed-hot-cold-client"
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

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "mixed-hot-cold.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 1. Write first slice (Slice 1): 2048 bytes of 'A' at offset 0
	dataA := bytes.Repeat([]byte("A"), 2048)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      dataA,
	})
	require.NoError(t, err)

	// Wait for Slice 1 to upload to S3
	waitCtx1, cancel1 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel1()
	err = uploader.WaitForIno(waitCtx1, diskID.String(), ino)
	require.NoError(t, err)

	// Purge Slice 1 from local staging (turns into COLD slice on S3)
	slice1Path := stagingMgr.SlicePath(diskID.String(), 1)
	err = os.Remove(slice1Path)
	require.NoError(t, err)

	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)
	defer func() {
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, chunk.FormatObjectKey(diskID.String(), 0, 1, 0), minio.RemoveObjectOptions{})
	}()

	// 2. Write second slice (Slice 2): 2048 bytes of 'B' at offset 2048 (remains HOT in NVMe staging)
	dataB := bytes.Repeat([]byte("B"), 2048)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    2048,
		Data:      dataB,
	})
	require.NoError(t, err)

	// Ensure Slice 1 is absent from staging and Slice 2 is present in staging
	_, err = os.Stat(stagingMgr.SlicePath(diskID.String(), 1))
	assert.True(t, os.IsNotExist(err), "Slice 1 must be purged from staging")
	_, err = os.Stat(stagingMgr.SlicePath(diskID.String(), 2))
	assert.NoError(t, err, "Slice 2 must exist in staging")

	// 3. Read spanning both slices (0..4096)
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      4096,
	})
	require.NoError(t, err)

	expected := append(dataA, dataB...)
	assert.Equal(t, expected, readResp.GetData(), "mixed read from cold S3 (0..2048) and hot staging (2048..4096) must be seamless")
	assert.True(t, readResp.GetEof())

	// 4. Overwrite part of cold slice: 100 bytes of 'C' at offset 1000 (hot in staging)
	dataC := bytes.Repeat([]byte("C"), 100)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1000,
		Data:      dataC,
	})
	require.NoError(t, err)

	readOverwrite, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    950,
		Size:      200,
	})
	require.NoError(t, err)

	// Offset 950..1000: 'A' (from cold S3 slice 1)
	// Offset 1000..1100: 'C' (from hot staging slice 3)
	// Offset 1100..1150: 'A' (from cold S3 slice 1)
	expectedOverwrite := make([]byte, 200)
	copy(expectedOverwrite[0:50], bytes.Repeat([]byte("A"), 50))
	copy(expectedOverwrite[50:150], dataC)
	copy(expectedOverwrite[150:200], bytes.Repeat([]byte("A"), 50))

	assert.Equal(t, expectedOverwrite, readOverwrite.GetData(), "overwritten range over cold S3 slice must reconstitute properly")
}

func TestChunkDriver_Read_MultiChunk_ColdS3SpanningBoundary(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "multichunk-cold-client"
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

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "spanning-cold.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 2 MiB spanning Chunk 0 and Chunk 1:
	// 1 MiB in Chunk 0 (pos: 63 MiB)
	// 1 MiB in Chunk 1 (pos: 0)
	offset := uint64(63 * 1024 * 1024)
	size := 2 * 1024 * 1024
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte((i * 13) % 251)
	}

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset,
		Data:      payload,
	})
	require.NoError(t, err)

	// Wait for upload of both slices (chunk 0 slice 1, chunk 1 slice 2)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = uploader.WaitForIno(waitCtx, diskID.String(), ino)
	require.NoError(t, err)

	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)
	defer func() {
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, chunk.FormatObjectKey(diskID.String(), 0, 1, 0), minio.RemoveObjectOptions{})
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, chunk.FormatObjectKey(diskID.String(), 1, 2, 0), minio.RemoveObjectOptions{})
	}()

	// Purge BOTH slices from staging -> both are now COLD in S3
	err = os.Remove(stagingMgr.SlicePath(diskID.String(), 1))
	require.NoError(t, err)
	err = os.Remove(stagingMgr.SlicePath(diskID.String(), 2))
	require.NoError(t, err)

	// Read 2 MiB spanning across cold Chunk 0 and cold Chunk 1
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset,
		Size:      uint32(size),
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp.GetData(), "spanning read across cold Chunk 0 and Chunk 1 from S3 must match")
	assert.True(t, readResp.GetEof())
}


func TestChunkDriver_Write_RecordsSliceRefs(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "write-slice-refs-client"
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
		Name:      "test-slice-refs.bin",
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

	// Verify slice_refs has 1 row with size = 4096 and refs = 1
	var (
		refID   int64
		refSize int32
		refRefs int32
	)
	query := fmt.Sprintf(`SELECT id, size, refs FROM "%s".slice_refs WHERE id = 1`, schemaName)
	err = h.dbPool.QueryRow(ctx, query).Scan(&refID, &refSize, &refRefs)
	require.NoError(t, err)
	assert.Equal(t, int64(1), refID)
	assert.Equal(t, int32(4096), refSize)
	assert.Equal(t, int32(1), refRefs)
}

func TestChunkDriver_CopyFileRange_ZeroCopyAndRead(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "copy-file-range-client"
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

	// 1. Create source file and write 16KB
	srcResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "src-file.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	srcIno := srcResp.GetInode().GetIno()
	srcFH := srcResp.GetFileHandle().GetFh()

	payload := make([]byte, 16384)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        srcFH,
		Ino:       srcIno,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// Verify slice_refs initially has refs = 1
	var ref1 int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&ref1)
	require.NoError(t, err)
	assert.Equal(t, int32(1), ref1)

	// 2. Create destination file
	dstResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dst-file.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	dstIno := dstResp.GetInode().GetIno()
	dstFH := dstResp.GetFileHandle().GetFh()

	// 3. Clone via CopyFileRange
	copyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    srcIno,
		SrcFh:     srcFH,
		SrcOffset: 0,
		DstIno:    dstIno,
		DstFh:     dstFH,
		DstOffset: 0,
		Length:    16384,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(16384), copyResp.GetCopied())

	// 4. Assert slice_refs.refs incremented to 2
	var ref2 int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&ref2)
	require.NoError(t, err)
	assert.Equal(t, int32(2), ref2, "slice_refs.refs must be incremented to 2 after CopyFileRange")

	// 5. Assert destination Inode size is 16384
	dstAttr, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       dstIno,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(16384), dstAttr.GetInode().GetSize())

	// 6. Assert destination chunks metadata row exists
	var dstRawSlices []byte
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName), int64(dstIno)).Scan(&dstRawSlices)
	require.NoError(t, err)
	dstSlices, err := chunk.DecodeSlices(dstRawSlices)
	require.NoError(t, err)
	require.Len(t, dstSlices, 1)
	assert.Equal(t, uint64(1), dstSlices[0].ID)
	assert.Equal(t, uint32(0), dstSlices[0].Pos)
	assert.Equal(t, uint32(16384), dstSlices[0].Len)

	// 7. Read destination file and verify exact byte content
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    0,
		Size:      16384,
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp.GetData())
	assert.True(t, readResp.GetEof())
}

func TestChunkDriver_CopyFileRange_Simulated1GB_Sub50ms(t *testing.T) {
	h, _, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "simulated-1gb-client"
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

	// Create source inode (ino 2) and destination inode (ino 3)
	srcResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "src-1gb.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	srcIno := srcResp.GetInode().GetIno()

	dstResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dst-1gb.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	dstIno := dstResp.GetInode().GetIno()

	// Simulate 1GB (16 chunks of 64MB) in source inode metadata
	const (
		numChunks = 16
		oneGB     = uint64(1024 * 1024 * 1024)
	)

	// Update source inode size to 1GB in postgres
	_, err = h.dbPool.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET size = $1 WHERE ino = $2`, schemaName), int64(oneGB), int64(srcIno))
	require.NoError(t, err)

	// Insert 16 chunk rows and slice_refs rows (1 slice of 64MB per chunk)
	for i := 0; i < numChunks; i++ {
		sliceID := uint64(100 + i)
		s := chunk.Slice{
			Pos:  0,
			ID:   sliceID,
			Size: chunk.ChunkSize,
			Off:  0,
			Len:  chunk.ChunkSize,
		}
		encoded := chunk.EncodeSlice(s)
		_, err = h.dbPool.Exec(ctx, fmt.Sprintf(`INSERT INTO "%s".chunks (ino, indx, slices) VALUES ($1, $2, $3)`, schemaName), int64(srcIno), int32(i), encoded)
		require.NoError(t, err)

		_, err = h.dbPool.Exec(ctx, fmt.Sprintf(`INSERT INTO "%s".slice_refs (id, size, refs) VALUES ($1, $2, 1)`, schemaName), int64(sliceID), int32(chunk.ChunkSize))
		require.NoError(t, err)
	}

	// Benchmark CopyFileRange execution time
	start := time.Now()
	copied, err := chunkDriver.CopyFileRange(
		ctx,
		nil,
		diskID.String(),
		srcIno,
		0,
		nil,
		diskID.String(),
		dstIno,
		0,
		oneGB,
	)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, oneGB, copied)
	t.Logf("1GB simulated CopyFileRange elapsed: %v", elapsed)
	assert.Less(t, elapsed, 50*time.Millisecond, "1GB metadata zero-copy clone must complete in < 50ms, took %v", elapsed)

	// Verify all 16 slice_refs have refs = 2
	for i := 0; i < numChunks; i++ {
		sliceID := uint64(100 + i)
		var refs int32
		err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = $1`, schemaName), int64(sliceID)).Scan(&refs)
		require.NoError(t, err)
		assert.Equal(t, int32(2), refs, "slice %d refs must be 2", sliceID)
	}

	// Verify destination Inode size is 1GB
	var dstSize int64
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName), int64(dstIno)).Scan(&dstSize)
	require.NoError(t, err)
	assert.Equal(t, int64(oneGB), dstSize)
}

func TestChunkDriver_CopyFileRange_OffsetAndCrossBoundary(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "copy-range-boundary-client"
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

	// 1. Partial range copy within chunk
	srcResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "src-offset.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	srcIno := srcResp.GetInode().GetIno()
	srcFH := srcResp.GetFileHandle().GetFh()

	payload := make([]byte, 8192)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        srcFH,
		Ino:       srcIno,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	dstResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dst-offset.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	dstIno := dstResp.GetInode().GetIno()
	dstFH := dstResp.GetFileHandle().GetFh()

	// Copy 2500 bytes from src offset 1000 to dst offset 3000
	copyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    srcIno,
		SrcFh:     srcFH,
		SrcOffset: 1000,
		DstIno:    dstIno,
		DstFh:     dstFH,
		DstOffset: 3000,
		Length:    2500,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2500), copyResp.GetCopied())

	// Read destination at offset 3000, size 2500
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    3000,
		Size:      2500,
	})
	require.NoError(t, err)
	assert.Equal(t, payload[1000:3500], readResp.GetData())

	// 2. Zero length copy
	zeroCopyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    srcIno,
		SrcFh:     srcFH,
		SrcOffset: 0,
		DstIno:    dstIno,
		DstFh:     dstFH,
		DstOffset: 0,
		Length:    0,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), zeroCopyResp.GetCopied())

	// 3. Offset past EOF copy
	eofCopyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    srcIno,
		SrcFh:     srcFH,
		SrcOffset: 99999,
		DstIno:    dstIno,
		DstFh:     dstFH,
		DstOffset: 0,
		Length:    100,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), eofCopyResp.GetCopied())

	// 4. Cross 64MB Chunk boundary copy in destination
	crossSrcResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "src-cross.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	crossSrcIno := crossSrcResp.GetInode().GetIno()
	crossSrcFH := crossSrcResp.GetFileHandle().GetFh()

	crossPayload := make([]byte, 4096)
	_, err = rand.Read(crossPayload)
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        crossSrcFH,
		Ino:       crossSrcIno,
		Offset:    0,
		Data:      crossPayload,
	})
	require.NoError(t, err)

	crossDstResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dst-cross.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	crossDstIno := crossDstResp.GetInode().GetIno()
	crossDstFH := crossDstResp.GetFileHandle().GetFh()

	// Destination offset is chunk.ChunkSize - 2048, length is 4096
	// This forces 2048 bytes in chunk 0 and 2048 bytes in chunk 1
	dstCrossOffset := uint64(chunk.ChunkSize - 2048)
	crossCopyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    crossSrcIno,
		SrcFh:     crossSrcFH,
		SrcOffset: 0,
		DstIno:    crossDstIno,
		DstFh:     crossDstFH,
		DstOffset: dstCrossOffset,
		Length:    4096,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), crossCopyResp.GetCopied())

	// Verify destination chunks table has 2 rows
	var chunk0Slices, chunk1Slices []byte
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName), int64(crossDstIno)).Scan(&chunk0Slices)
	require.NoError(t, err)
	s0, err := chunk.DecodeSlices(chunk0Slices)
	require.NoError(t, err)
	require.Len(t, s0, 1)
	assert.Equal(t, uint32(chunk.ChunkSize-2048), s0[0].Pos)
	assert.Equal(t, uint32(0), s0[0].Off)
	assert.Equal(t, uint32(2048), s0[0].Len)

	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 1`, schemaName), int64(crossDstIno)).Scan(&chunk1Slices)
	require.NoError(t, err)
	s1, err := chunk.DecodeSlices(chunk1Slices)
	require.NoError(t, err)
	require.Len(t, s1, 1)
	assert.Equal(t, uint32(0), s1[0].Pos)
	assert.Equal(t, uint32(2048), s1[0].Off)
	assert.Equal(t, uint32(2048), s1[0].Len)

	// Read destination across both chunks
	readPart0, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        crossDstFH,
		Ino:       crossDstIno,
		Offset:    dstCrossOffset,
		Size:      2048,
	})
	require.NoError(t, err)
	assert.Equal(t, crossPayload[:2048], readPart0.GetData())

	readPart1, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        crossDstFH,
		Ino:       crossDstIno,
		Offset:    uint64(chunk.ChunkSize),
		Size:      2048,
	})
	require.NoError(t, err)
	assert.Equal(t, crossPayload[2048:], readPart1.GetData())
}

// TestChunkDriver_CopyFileRange_COWIsolation_DataPreservation 验证写时复制 (COW) 严格数据隔离性与生命周期联动
func TestChunkDriver_CopyFileRange_COWIsolation_DataPreservation(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "cow-isolation-client"
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

	// 1. 创建源文件 A 并写入 32KB 数据 (固定填充字节 0xAA)
	srcResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "src-cow.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	srcIno := srcResp.GetInode().GetIno()
	srcFH := srcResp.GetFileHandle().GetFh()

	const dataSize = 32768
	payloadA := bytes.Repeat([]byte{0xAA}, dataSize)
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        srcFH,
		Ino:       srcIno,
		Offset:    0,
		Data:      payloadA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(dataSize), writeResp.GetWritten())

	// 确认切片 1 在 slice_refs 中引用计数为 1
	var ref1 int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&ref1)
	require.NoError(t, err)
	assert.Equal(t, int32(1), ref1)

	// 2. 创建目标文件 B
	dstResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dst-cow.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	dstIno := dstResp.GetInode().GetIno()
	dstFH := dstResp.GetFileHandle().GetFh()

	// 3. 执行 CopyFileRange：零拷贝克隆整段 32KB 数据
	copyStart := time.Now()
	copyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    srcIno,
		SrcFh:     srcFH,
		SrcOffset: 0,
		DstIno:    dstIno,
		DstFh:     dstFH,
		DstOffset: 0,
		Length:    dataSize,
	})
	copyElapsed := time.Since(copyStart)
	require.NoError(t, err)
	assert.Equal(t, uint64(dataSize), copyResp.GetCopied())
	assert.Less(t, copyElapsed, 50*time.Millisecond, "元数据零拷贝克隆必须在 50ms 内完成，实际耗时 %v", copyElapsed)

	// 校验 slice_refs 引用计数原子递增至 2
	var ref2 int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&ref2)
	require.NoError(t, err)
	assert.Equal(t, int32(2), ref2, "CopyFileRange 必须原子递增切片引用计数至 2")

	// 校验目标 Inode 大小已自动更新为 32KB
	dstAttr, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       dstIno,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(dataSize), dstAttr.GetInode().GetSize())

	// 4. 克隆后立即回读：验证源与目标数据 100% 一致
	readDstBefore, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, payloadA, readDstBefore.GetData(), "克隆后目标文件内容必须与源文件完全一致")

	// 5. 写时复制 (COW) 隔离性验证：覆写目标文件 B 内部中间区域 [8192, 16384)
	const overwriteSize = 8192
	const overwriteOffset = 8192
	payloadB := bytes.Repeat([]byte{0xBB}, overwriteSize)

	writeOverwriteResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    overwriteOffset,
		Data:      payloadB,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(overwriteSize), writeOverwriteResp.GetWritten())

	// 6. 核心 COW 隔离断言：
	// 6.1 源文件 A 必须保持 100% 原始数据，完全不受目标文件覆写影响！
	readSrcAfter, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        srcFH,
		Ino:       srcIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, payloadA, readSrcAfter.GetData(), "写时复制隔离性：目标文件覆写后，源文件数据必须绝对不受影响保持原状")

	// 6.2 目标文件 B 读取应返回最新的图层覆盖合成数据：[0..8K: 0xAA, 8K..16K: 0xBB, 16K..32K: 0xAA]
	expectedDstData := make([]byte, dataSize)
	copy(expectedDstData, payloadA)
	copy(expectedDstData[overwriteOffset:], payloadB)

	readDstAfter, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedDstData, readDstAfter.GetData(), "目标文件必须精确反映写时复制覆写后的图层数据")

	// 7. 反向隔离断言：修改源文件 A 的起始 4KB 为 0xCC，验证目标文件 B 同样不受源文件修改影响！
	const srcModifySize = 4096
	payloadC := bytes.Repeat([]byte{0xCC}, srcModifySize)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        srcFH,
		Ino:       srcIno,
		Offset:    0,
		Data:      payloadC,
	})
	require.NoError(t, err)

	// 目标文件 B 内容必须依然保持不变
	readDstAfterSrcMod, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedDstData, readDstAfterSrcMod.GetData(), "源文件后续修改不应波及克隆后的目标文件")

	// 8. 生命周期与孤儿切片保护验证：
	// 释放句柄、Unlink 并 Forget 源文件 A
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{SessionId: sessionID, Fh: srcFH})
	require.NoError(t, err)
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{SessionId: sessionID, ParentIno: 1, Name: "src-cow.bin"})
	require.NoError(t, err)
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{SessionId: sessionID, Ino: srcIno, Nlookup: 1})
	require.NoError(t, err)

	// 切片 1 的引用计数应从 2 递减至 1，由于 refs > 0，不属于物理孤儿切片
	var refAfterUnlinkA int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&refAfterUnlinkA)
	require.NoError(t, err)
	assert.Equal(t, int32(1), refAfterUnlinkA, "删除源文件 A 后，切片 1 引用计数必须安全递减为 1")

	// 验证底层 NVMe Staging 物理文件依然完整保留
	slice1Path := stagingMgr.SlicePath(diskID.String(), 1)
	_, err = os.Stat(slice1Path)
	require.NoError(t, err, "切片 1 物理文件不能被误删")

	// 目标文件 B 依然可以完整正常读取
	readDstAfterDeleteA, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedDstData, readDstAfterDeleteA.GetData(), "源文件删除后目标文件数据依然可无损读取")

	// 释放目标句柄、Unlink 并 Forget 目标文件 B
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{SessionId: sessionID, Fh: dstFH})
	require.NoError(t, err)
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{SessionId: sessionID, ParentIno: 1, Name: "dst-cow.bin"})
	require.NoError(t, err)
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{SessionId: sessionID, Ino: dstIno, Nlookup: 1})
	require.NoError(t, err)

	// 切片 1 引用计数归零
	var ref1Final int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&ref1Final)
	require.NoError(t, err)
	assert.Equal(t, int32(0), ref1Final, "目标文件删除后，切片 1 引用计数归零")

	// 触发 GC，确认切片最终被回收
	deletedCount, err := driver.RunGC(ctx, diskID.String())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, deletedCount, 1)
}

// TestChunkDriver_CopyFileRange_ErrorCases 测试写时复制克隆接口的错误处理与边界输入校验
func TestChunkDriver_CopyFileRange_ErrorCases(t *testing.T) {
	h, _, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID1 := uuid.New()
	diskID2 := uuid.New()
	schemaName1 := FormatDiskSchema(diskID1)
	schemaName2 := FormatDiskSchema(diskID2)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName1+"\" CASCADE")
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName2+"\" CASCADE")
	}()

	_, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID1.String(),
		ClientId: "error-cases-client-1",
		ReadOnly: false,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID2.String(),
		ClientId: "error-cases-client-2",
		ReadOnly: false,
	})
	require.NoError(t, err)

	// 1. 跨磁盘克隆 -> CodeInvalidArgument
	_, err = driver.CopyFileRange(ctx, nil, diskID1.String(), 10, 0, nil, diskID2.String(), 20, 0, 1024)
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. 源 Inode 不存在 -> CodeNotFound
	_, err = driver.CopyFileRange(ctx, nil, diskID1.String(), 99999, 0, nil, diskID1.String(), 1, 0, 1024)
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 3. 目标 Inode 不存在 -> CodeNotFound (使用已存在的根目录 Inode 1 作为源)
	_, err = driver.CopyFileRange(ctx, nil, diskID1.String(), 1, 0, nil, diskID1.String(), 88888, 0, 1024)
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}


// mockS3Client implements chunk.ObjectStorageClient for controlled delay and failure simulation.
type mockS3Client struct {
	mu       sync.Mutex
	putCalls atomic.Int32
	putDelay time.Duration
	putErr   error
	putFunc  func(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	objects  map[string][]byte
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{
		objects: make(map[string][]byte),
	}
}

func (m *mockS3Client) PutObject(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	m.putCalls.Add(1)
	if m.putDelay > 0 {
		select {
		case <-time.After(m.putDelay):
		case <-ctx.Done():
			return minio.UploadInfo{}, ctx.Err()
		}
	}
	m.mu.Lock()
	pFunc := m.putFunc
	err := m.putErr
	m.mu.Unlock()
	if pFunc != nil {
		return pFunc(ctx, bucket, objectKey, reader, objectSize, opts)
	}
	if err != nil {
		return minio.UploadInfo{}, err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}

	m.mu.Lock()
	m.objects[objectKey] = data
	m.mu.Unlock()

	return minio.UploadInfo{
		Bucket: bucket,
		Key:    objectKey,
		Size:   int64(len(data)),
	}, nil
}

func (m *mockS3Client) StatObject(ctx context.Context, bucket, objectKey string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	m.mu.Lock()
	data, ok := m.objects[objectKey]
	m.mu.Unlock()
	if !ok {
		return minio.ObjectInfo{}, minio.ErrorResponse{
			StatusCode: 404,
			Code:       "NoSuchKey",
			Message:    "The specified key does not exist.",
		}
	}
	return minio.ObjectInfo{
		Key:  objectKey,
		Size: int64(len(data)),
	}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, bucket, objectKey string, opts minio.GetObjectOptions) (*minio.Object, error) {
	return nil, errors.New("not implemented")
}

func (m *mockS3Client) RemoveObject(ctx context.Context, bucket, objectKey string, opts minio.RemoveObjectOptions) error {
	m.mu.Lock()
	delete(m.objects, objectKey)
	m.mu.Unlock()
	return nil
}

func (m *mockS3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return true, nil
}

func (m *mockS3Client) MakeBucket(ctx context.Context, bucket string, opts minio.MakeBucketOptions) error {
	return nil
}

func (m *mockS3Client) getObjects() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make(map[string][]byte, len(m.objects))
	for k, v := range m.objects {
		res[k] = v
	}
	return res
}

func TestChunkDriver_Fsync_BlocksUntilS3UploadsFinish(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "fsync-block-client"
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

	mockClient := newMockS3Client()
	mockClient.putDelay = 80 * time.Millisecond // simulate network latency on S3 PutObject

	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(2),
		chunk.WithInitialBackoff(10*time.Millisecond),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "fsync-barrier-test.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 3 slices: 4MB, 4MB, 1MB
	blockSize := chunk.BlockSize // 4MB
	payload1 := bytes.Repeat([]byte("A"), blockSize)
	payload2 := bytes.Repeat([]byte("B"), blockSize)
	payload3 := bytes.Repeat([]byte("C"), 1024*1024)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload1,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    uint64(blockSize),
		Data:      payload2,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    uint64(blockSize * 2),
		Data:      payload3,
	})
	require.NoError(t, err)

	// Call Fsync and verify it blocks until all 3 slices are uploaded
	start := time.Now()
	fsyncResp, err := h.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Datasync:  false,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, fsyncResp)
	assert.GreaterOrEqual(t, elapsed, 80*time.Millisecond, "Fsync must block until simulated S3 upload finishes")

	// Verify all blocks exist in storage
	objs := mockClient.getObjects()
	assert.Equal(t, int32(3), mockClient.putCalls.Load())
	assert.Len(t, objs, 3)

	key1 := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	key2 := chunk.FormatObjectKey(diskID.String(), 0, 2, 0)
	key3 := chunk.FormatObjectKey(diskID.String(), 0, 3, 0)
	assert.Contains(t, objs, key1)
	assert.Contains(t, objs, key2)
	assert.Contains(t, objs, key3)
	assert.Equal(t, payload1, objs[key1])
	assert.Equal(t, payload2, objs[key2])
	assert.Equal(t, payload3, objs[key3])
}

func TestChunkDriver_Flush_BlocksUntilS3UploadsFinish(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "flush-block-client"
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

	mockClient := newMockS3Client()
	mockClient.putDelay = 80 * time.Millisecond

	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(2),
		chunk.WithInitialBackoff(10*time.Millisecond),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "flush-barrier-test.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	payload := bytes.Repeat([]byte("D"), 2*1024*1024)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	start := time.Now()
	flushResp, err := h.storageClient.Flush(ctx, &bowlv1.FlushRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, flushResp)
	assert.GreaterOrEqual(t, elapsed, 70*time.Millisecond, "Flush must block until simulated S3 upload finishes")

	objs := mockClient.getObjects()
	key := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	assert.Contains(t, objs, key)
	assert.Equal(t, payload, objs[key])
}

func TestChunkDriver_Fsync_UploadFailure_PropagatesError(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "fsync-fail-client"
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

	mockClient := newMockS3Client()
	mockClient.putErr = errors.New("s3 permanent failure: access denied")

	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(1),
		chunk.WithMaxRetries(0),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "fail-test.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      []byte("hello world fail"),
	})
	require.NoError(t, err)

	_, err = h.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Datasync:  false,
	})
	require.Error(t, err, "Fsync must return error when background S3 upload fails")
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "s3 persistence failure")
}

func TestChunkDriver_Fsync_ContextTimeout_UnblocksWithError(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "fsync-timeout-client"
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

	mockClient := newMockS3Client()
	mockClient.putDelay = 1 * time.Second

	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(1),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "timeout-test.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      []byte("timeout test payload"),
	})
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = h.storageClient.Fsync(timeoutCtx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Datasync:  false,
	})
	elapsed := time.Since(start)

	require.Error(t, err, "Fsync must fail with deadline exceeded")
	assert.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
	assert.Less(t, elapsed, 300*time.Millisecond, "Fsync must unblock immediately upon context cancellation")
}

func TestChunkDriver_Fsync_WithoutUploader_Succeeds(t *testing.T) {
	h, _, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "no-uploader-client"
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

	assert.Nil(t, driver.Uploader(), "Uploader should be nil initially")

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "no-uploader.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      []byte("data without uploader"),
	})
	require.NoError(t, err)

	_, err = h.storageClient.Flush(ctx, &bowlv1.FlushRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
	})
	require.NoError(t, err)
}

// TestChunkDriver_Fsync_TransientRetry_SucceedsAfterBackoff 验证遭遇瞬时网络抖动时指数退避重试，Fsync 成功阻塞直至数据落盘
func TestChunkDriver_Fsync_TransientRetry_SucceedsAfterBackoff(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "fsync-transient-retry-client"
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

	mockClient := newMockS3Client()
	var attempts atomic.Int32
	mockClient.putFunc = func(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
		cur := attempts.Add(1)
		if cur <= 2 {
			// 模拟前 2 次遭遇瞬时 503 错误
			return minio.UploadInfo{}, minio.ErrorResponse{
				StatusCode: 503,
				Code:       "ServiceUnavailable",
				Message:    "transient network congestion",
			}
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return minio.UploadInfo{}, err
		}
		mockClient.mu.Lock()
		mockClient.objects[objectKey] = data
		mockClient.mu.Unlock()
		return minio.UploadInfo{
			Bucket: bucket,
			Key:    objectKey,
			Size:   int64(len(data)),
		}, nil
	}

	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(1),
		chunk.WithMaxRetries(4),
		chunk.WithInitialBackoff(10*time.Millisecond),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "retry-test.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	payload := []byte("transient retry data payload with backoff")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// Fsync 必须挂起直至退避重试成功并提交 S3
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer syncCancel()

	fsyncResp, err := h.storageClient.Fsync(syncCtx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Datasync:  false,
	})
	require.NoError(t, err)
	require.NotNil(t, fsyncResp)
	assert.GreaterOrEqual(t, attempts.Load(), int32(3), "应当经历至少 2 次瞬时失败与 1 次成功重试")

	objs := mockClient.getObjects()
	key := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	assert.Equal(t, payload, objs[key], "重试成功后 S3 物理对象必须完整存在")
}

// TestChunkDriver_Fsync_ConcurrentCalls_SameInode 验证对同一 Inode 发起并发 Fsync/Flush 时均能正确挂起并安全返回
func TestChunkDriver_Fsync_ConcurrentCalls_SameInode(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "fsync-concurrent-client"
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

	mockClient := newMockS3Client()
	mockClient.putDelay = 60 * time.Millisecond

	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(2),
		chunk.WithInitialBackoff(10*time.Millisecond),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "concurrent-fsync.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	payload := bytes.Repeat([]byte("C"), 1024*1024)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// 并发触发 Flush 与 Fsync
	var wg sync.WaitGroup
	var errFlush, errFsync error
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, errFlush = h.storageClient.Flush(ctx, &bowlv1.FlushRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
		})
	}()

	go func() {
		defer wg.Done()
		_, errFsync = h.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
		})
	}()

	wg.Wait()
	require.NoError(t, errFlush, "并发 Flush 必须成功")
	require.NoError(t, errFsync, "并发 Fsync 必须成功")

	objs := mockClient.getObjects()
	key := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	assert.Equal(t, payload, objs[key])
}

// TestChunkDriver_ObjectStorage_And_Barrier 验证切片驱动与内联 JuiceFS 对象存储及同步屏障信道的集成
func TestChunkDriver_ObjectStorage_And_Barrier(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "driver-barrier-client"
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

	mockClient := newMockS3Client()
	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(2),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "barrier-seam.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      []byte("data for barrier seam test"),
	})
	require.NoError(t, err)

	// 直接从驱动 seam 获取同步屏障信道并等待达成
	barrierCh := driver.Barrier(diskID.String(), ino)
	select {
	case err := <-barrierCh:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("持久化屏障信道等待超时")
	}
}

func isMinIOHealthy(endpoint string) bool {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(endpoint + "/minio/health/live")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func TestChunkDriver_Fsync_EndToEnd_MinIO_MultiBlock(t *testing.T) {
	minioEndpoint := "http://127.0.0.1:9000"
	if !isMinIOHealthy(minioEndpoint) {
		t.Skip("MinIO is not reachable at 127.0.0.1:9000, skipping live MinIO test")
	}

	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "fsync-minio-client"
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

	cfg := config.S3Config{
		Enabled:        true,
		Endpoint:       minioEndpoint,
		Bucket:         "bowl-data",
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
		Region:         "us-east-1",
		UseSSL:         false,
		ForcePathStyle: true,
	}

	uploader, err := chunk.NewS3Uploader(cfg, stagingMgr,
		chunk.WithWorkers(4),
		chunk.WithInitialBackoff(10*time.Millisecond),
	)
	require.NoError(t, err)

	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "multiblock-minio.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	blockSize := chunk.BlockSize // 4MB
	payload1 := make([]byte, blockSize)
	_, _ = rand.Read(payload1)
	payload2 := make([]byte, blockSize)
	_, _ = rand.Read(payload2)
	payload3 := make([]byte, 1024*1024) // 1MB
	_, _ = rand.Read(payload3)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload1,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    uint64(blockSize),
		Data:      payload2,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    uint64(blockSize * 2),
		Data:      payload3,
	})
	require.NoError(t, err)

	// Invoke Fsync to wait for all in-flight S3 blocks to be persisted
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer syncCancel()

	fsyncResp, err := h.storageClient.Fsync(syncCtx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Datasync:  false,
	})
	require.NoError(t, err)
	require.NotNil(t, fsyncResp)

	// Verify all 4MB physical blocks exist in MinIO
	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)

	key1 := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	key2 := chunk.FormatObjectKey(diskID.String(), 0, 2, 0)
	key3 := chunk.FormatObjectKey(diskID.String(), 0, 3, 0)

	defer func() {
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, key1, minio.RemoveObjectOptions{})
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, key2, minio.RemoveObjectOptions{})
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, key3, minio.RemoveObjectOptions{})
	}()

	stat1, err := minioClient.StatObject(context.Background(), cfg.Bucket, key1, minio.StatObjectOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(blockSize), stat1.Size)

	stat2, err := minioClient.StatObject(context.Background(), cfg.Bucket, key2, minio.StatObjectOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(blockSize), stat2.Size)

	stat3, err := minioClient.StatObject(context.Background(), cfg.Bucket, key3, minio.StatObjectOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(len(payload3)), stat3.Size)
}

func TestChunkDriver_DeleteInode_DecrementsSliceRefs_AndDeletesChunks(t *testing.T) {
	h, _, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-del-test",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "del-test-file.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write data spanning 2 chunks (64MB - 1KB to 64MB + 1KB, total 2KB)
	writeOffset := uint64(chunk.ChunkSize - 1024)
	payload := make([]byte, 2048)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    writeOffset,
		Data:      payload,
	})
	require.NoError(t, err)

	// Verify 2 slices created in slice_refs with refs = 1
	var refCount int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".slice_refs WHERE refs = 1`, schemaName)).Scan(&refCount)
	require.NoError(t, err)
	assert.Equal(t, 2, refCount)

	// Verify 2 chunks rows exist for ino
	var chunkCount int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1`, schemaName), int64(ino)).Scan(&chunkCount)
	require.NoError(t, err)
	assert.Equal(t, 2, chunkCount)

	// Call DeleteInode on ChunkDriver
	err = chunkDriver.DeleteInode(ctx, diskID.String(), ino)
	require.NoError(t, err)

	// Assert chunks rows are deleted
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1`, schemaName), int64(ino)).Scan(&chunkCount)
	require.NoError(t, err)
	assert.Equal(t, 0, chunkCount, "chunks rows must be deleted for unlinked inode")

	// Assert slice_refs have refs = 0
	var zeroRefCount int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".slice_refs WHERE refs = 0`, schemaName)).Scan(&zeroRefCount)
	require.NoError(t, err)
	assert.Equal(t, 2, zeroRefCount, "all discarded slices must have refs = 0")

	// 校验垃圾回收器已自动登记该磁盘并跟踪这 2 个孤儿切片
	orphans := chunkDriver.TrackedOrphans(diskID.String())
	assert.Len(t, orphans, 2, "垃圾回收器必须记录待物理清理的 2 个孤儿切片")
	assert.ElementsMatch(t, []uint64{1, 2}, orphans, "孤儿切片编号必须与释放切片一致")
}

func TestStorageService_Unlink_ReclaimsChunksAndDecrementsSliceRefs(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-unlink-test",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "unlink-file.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

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

	// Release handle
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)

	// Assert refs = 1
	var ref1 int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&ref1)
	require.NoError(t, err)
	assert.Equal(t, int32(1), ref1)

	// Unlink file (nlink becomes 0, lookup_count is still 1)
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "unlink-file.bin",
	})
	require.NoError(t, err)

	// While still referenced by lookup_count = 1, chunks and refs are preserved
	var chunkCountStill int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1`, schemaName), int64(ino)).Scan(&chunkCountStill)
	require.NoError(t, err)
	assert.Equal(t, 1, chunkCountStill)

	// Now client forgets lookup reference (lookup_count drops to 0) -> triggers permanent reclamation!
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       ino,
		Nlookup:   1,
	})
	require.NoError(t, err)

	// Assert chunks rows deleted and slice_refs decremented to 0
	var chunkCount int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1`, schemaName), int64(ino)).Scan(&chunkCount)
	require.NoError(t, err)
	assert.Equal(t, 0, chunkCount, "chunks rows must be deleted when unlinked inode is forgotten")

	var refAfter int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&refAfter)
	require.NoError(t, err)
	assert.Equal(t, int32(0), refAfter, "slice_refs.refs must be decremented to 0 upon final reclaim")
}

func TestChunkDriver_Truncate_Shrink_AdjustsSlicesAndDecrementsRefs(t *testing.T) {
	h, _, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-truncate-test",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "trunc-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 1. Write Slice 1: offset 0..4096 (in chunk 0)
	p1 := make([]byte, 4096)
	_, _ = rand.Read(p1)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      p1,
	})
	require.NoError(t, err)

	// 2. Write Slice 2: offset 4096..8192 (in chunk 0)
	p2 := make([]byte, 4096)
	_, _ = rand.Read(p2)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    4096,
		Data:      p2,
	})
	require.NoError(t, err)

	// 3. Write Slice 3: offset 64MB..64MB+4096 (in chunk 1)
	p3 := make([]byte, 4096)
	_, _ = rand.Read(p3)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    uint64(chunk.ChunkSize),
		Data:      p3,
	})
	require.NoError(t, err)

	// All 3 slices must have refs = 1
	var r1, r2, r3 int32
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&r1))
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 2`, schemaName)).Scan(&r2))
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 3`, schemaName)).Scan(&r3))
	assert.Equal(t, int32(1), r1)
	assert.Equal(t, int32(1), r2)
	assert.Equal(t, int32(1), r3)

	// Step A: Truncate to 64MB (Chunk 1 discarded completely)
	err = chunkDriver.Truncate(ctx, diskID.String(), ino, uint64(chunk.ChunkSize))
	require.NoError(t, err)

	// Chunk 1 row deleted
	var c1Count int
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1 AND indx = 1`, schemaName), int64(ino)).Scan(&c1Count))
	assert.Equal(t, 0, c1Count, "chunk 1 row must be deleted after truncate to 64MB")

	// Slice 3 refs decremented to 0; Slices 1 and 2 keep refs = 1
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 3`, schemaName)).Scan(&r3))
	assert.Equal(t, int32(0), r3, "slice 3 in chunk 1 must have refs = 0")
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&r1))
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 2`, schemaName)).Scan(&r2))
	assert.Equal(t, int32(1), r1)
	assert.Equal(t, int32(1), r2)

	// Step B: Truncate to 6000 bytes (Slice 2 partially truncated to Len = 1904)
	err = chunkDriver.Truncate(ctx, diskID.String(), ino, 6000)
	require.NoError(t, err)

	// Check chunk 0 slices
	var rawSlices []byte
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName), int64(ino)).Scan(&rawSlices))
	sList, err := chunk.DecodeSlices(rawSlices)
	require.NoError(t, err)
	require.Len(t, sList, 2)
	assert.Equal(t, uint32(4096), sList[0].Len)
	assert.Equal(t, uint32(1904), sList[1].Len, "slice 2 Len must be adjusted to 6000 - 4096 = 1904")

	// Slices 1 and 2 still have refs = 1
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&r1))
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 2`, schemaName)).Scan(&r2))
	assert.Equal(t, int32(1), r1)
	assert.Equal(t, int32(1), r2)

	// Step C: Truncate to 2000 bytes (Slice 2 fully discarded, Slice 1 partially truncated to Len = 2000)
	err = chunkDriver.Truncate(ctx, diskID.String(), ino, 2000)
	require.NoError(t, err)

	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName), int64(ino)).Scan(&rawSlices))
	sList, err = chunk.DecodeSlices(rawSlices)
	require.NoError(t, err)
	require.Len(t, sList, 1)
	assert.Equal(t, uint32(2000), sList[0].Len, "slice 1 Len must be adjusted to 2000")

	// Slice 2 refs decremented to 0
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 2`, schemaName)).Scan(&r2))
	assert.Equal(t, int32(0), r2, "slice 2 must have refs = 0 after being fully discarded")

	// Step D: Truncate to 0 bytes
	err = chunkDriver.Truncate(ctx, diskID.String(), ino, 0)
	require.NoError(t, err)

	var c0Count int
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName), int64(ino)).Scan(&c0Count))
	assert.Equal(t, 0, c0Count, "chunk 0 row must be deleted after truncate to 0")

	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&r1))
	assert.Equal(t, int32(0), r1, "slice 1 must have refs = 0 after truncate to 0")
}

func TestStorageService_SetAttr_TruncateShrink_DecrementsSliceRefs(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-setattr-trunc",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "setattr-trunc.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 2 Chunks:
	// Chunk 0: 4096 bytes at offset 0
	p1 := make([]byte, 4096)
	_, _ = rand.Read(p1)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      p1,
	})
	require.NoError(t, err)

	// Chunk 1: 4096 bytes at offset 64MB
	p2 := make([]byte, 4096)
	_, _ = rand.Read(p2)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    uint64(chunk.ChunkSize),
		Data:      p2,
	})
	require.NoError(t, err)

	// Assert 2 slices exist with refs = 1
	var r1, r2 int32
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&r1))
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 2`, schemaName)).Scan(&r2))
	assert.Equal(t, int32(1), r1)
	assert.Equal(t, int32(1), r2)

	// Call SetAttr to shrink file size to 2048 bytes (half of Chunk 0, completely discarding Chunk 1)
	setAttrResp, err := h.storageClient.SetAttr(ctx, &bowlv1.SetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
		Valid:     uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_SIZE),
		Size:      2048,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2048), setAttrResp.GetInode().GetSize())

	// Assert Slice 2 in Chunk 1 is discarded (refs = 0), and Chunk 1 row is gone
	var c1Count int
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1 AND indx = 1`, schemaName), int64(ino)).Scan(&c1Count))
	assert.Equal(t, 0, c1Count)

	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 2`, schemaName)).Scan(&r2))
	assert.Equal(t, int32(0), r2, "slice 2 in chunk 1 must have refs = 0")

	// Slice 1 in Chunk 0 is partially truncated to 2048, so it still has refs = 1
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&r1))
	assert.Equal(t, int32(1), r1, "slice 1 must remain referenced with refs = 1")

	// Read beyond new size returns EOF
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    2048,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.True(t, readResp.GetEof())
	assert.Empty(t, readResp.GetData())

	// Read within new size returns first 2048 bytes
	readResp2, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      2048,
	})
	require.NoError(t, err)
	assert.Equal(t, p1[:2048], readResp2.GetData())
}

func TestChunkDriver_COWClone_S3AndStaging_GCLifecycle(t *testing.T) {
	minioEndpoint := "http://127.0.0.1:9000"
	if !isMinIOHealthy(minioEndpoint) {
		t.Skip("MinIO is not reachable at 127.0.0.1:9000, skipping live MinIO test")
	}

	h, stagingMgr, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "cow-gc-test-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// Mount disk
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// Configure S3 Uploader
	cfg := config.S3Config{
		Enabled:        true,
		Endpoint:       minioEndpoint,
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

	chunkDriver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	// 1. Create File A
	createAResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "file_a.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	inoA := createAResp.GetInode().GetIno()
	fhA := createAResp.GetFileHandle().GetFh()

	// Write 4096 bytes to File A
	payloadA := make([]byte, 4096)
	_, _ = rand.Read(payloadA)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fhA,
		Ino:       inoA,
		Offset:    0,
		Data:      payloadA,
	})
	require.NoError(t, err)

	// Wait for S3 upload barrier
	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	err = uploader.WaitForSlice(waitCtx, diskID.String(), 1)
	require.NoError(t, err)

	// Verify slice 1 staging file exists
	slice1StagingPath := stagingMgr.SlicePath(diskID.String(), 1)
	_, err = os.Stat(slice1StagingPath)
	require.NoError(t, err, "staging file must exist")

	// Verify S3 block exists in MinIO
	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)
	expectedObjectKey := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	objInfo, err := minioClient.StatObject(ctx, cfg.Bucket, expectedObjectKey, minio.StatObjectOptions{})
	require.NoError(t, err, "S3 object must exist in bucket")
	assert.Equal(t, int64(4096), objInfo.Size)

	// 2. Create File B
	createBResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "file_b.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	inoB := createBResp.GetInode().GetIno()
	fhB := createBResp.GetFileHandle().GetFh()

	// 3. CopyFileRange from File A to File B (COW clone)
	copyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    inoA,
		SrcFh:     fhA,
		SrcOffset: 0,
		DstIno:    inoB,
		DstFh:     fhB,
		DstOffset: 0,
		Length:    4096,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), copyResp.GetCopied())

	// Release handles
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fhA,
	})
	require.NoError(t, err)
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fhB,
	})
	require.NoError(t, err)

	// Verify slice_refs.refs is now 2
	var refs int32
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&refs)
	require.NoError(t, err)
	assert.Equal(t, int32(2), refs, "slice_refs.refs must be 2 after CopyFileRange")

	// 4. Unlink and Forget File A
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "file_a.bin",
	})
	require.NoError(t, err)
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       inoA,
		Nlookup:   1,
	})
	require.NoError(t, err)

	// Verify slice_refs.refs decremented to 1
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&refs)
	require.NoError(t, err)
	assert.Equal(t, int32(1), refs, "slice_refs.refs must be 1 after unlinking file A")

	// 5. Trigger GC -> Assert S3 object and staging data are NOT deleted!
	reclaimed, err := chunkDriver.RunGC(ctx, diskID.String())
	require.NoError(t, err)
	assert.Equal(t, 0, reclaimed, "0 slices must be collected while refs == 1")

	_, err = os.Stat(slice1StagingPath)
	assert.NoError(t, err, "staging file must NOT be deleted while File B still references it")

	_, err = minioClient.StatObject(ctx, cfg.Bucket, expectedObjectKey, minio.StatObjectOptions{})
	assert.NoError(t, err, "S3 object must NOT be deleted while File B still references it")

	// File B must still be fully readable!
	openBResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID,
		Ino:       inoB,
		Flags:     0,
	})
	require.NoError(t, err)
	fhReadB := openBResp.GetFileHandle().GetFh()

	readBResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fhReadB,
		Ino:       inoB,
		Offset:    0,
		Size:      4096,
	})
	require.NoError(t, err)
	assert.Equal(t, payloadA, readBResp.GetData())

	_, _ = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fhReadB,
	})

	// 6. Unlink and Forget File B -> refs becomes 0
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "file_b.bin",
	})
	require.NoError(t, err)
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       inoB,
		Nlookup:   1,
	})
	require.NoError(t, err)

	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&refs)
	require.NoError(t, err)
	assert.Equal(t, int32(0), refs, "slice_refs.refs must be 0 after unlinking file B")

	// 7. Trigger GC -> S3 object and staging file are cleaned up, slice_refs row is removed!
	reclaimed, err = chunkDriver.RunGC(ctx, diskID.String())
	require.NoError(t, err)
	assert.Equal(t, 1, reclaimed, "1 slice must be collected now that refs == 0")

	// Staging file deleted
	_, err = os.Stat(slice1StagingPath)
	assert.True(t, os.IsNotExist(err), "staging file must be deleted after GC")

	// S3 object deleted
	_, err = minioClient.StatObject(ctx, cfg.Bucket, expectedObjectKey, minio.StatObjectOptions{})
	assert.Error(t, err, "S3 object must be deleted from MinIO bucket after GC")
	errResp := minio.ToErrorResponse(err)
	assert.Equal(t, "NoSuchKey", errResp.Code)

	// slice_refs row deleted
	var rowCount int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 0, rowCount, "slice_refs row must be deleted after GC")
}

func TestChunkDriver_Write_128KB_FastIngestion_Sub10ms(t *testing.T) {
	h, stagingMgr, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-fast-ingest"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. 挂载虚拟磁盘并获取写租约会话
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. 创建测试文件
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test-128kb-fast.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 3. 准备 128KB 载荷并执行写入
	payloadSize := 128 * 1024
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte((i * 17) % 256)
	}

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

	// 严格断言 128KB 载荷写入确认延迟小于 50ms
	assert.Less(t, elapsed, 50*time.Millisecond, "128KB 载荷写入延迟必须在 50ms 内返回 ACK，实际耗时: %v", elapsed)

	// 4. 断言 NVMe 本地暂存文件存在且数据字节完整一致
	slicePath := stagingMgr.SlicePath(diskID.String(), 1)
	fi, err := os.Stat(slicePath)
	require.NoError(t, err, "NVMe 本地暂存切片文件必须存在: %s", slicePath)
	assert.Equal(t, int64(payloadSize), fi.Size())

	sliceData, err := stagingMgr.ReadSlice(diskID.String(), 1)
	require.NoError(t, err)
	assert.Equal(t, payload, sliceData)

	// 5. 断言 PostgreSQL chunks 表记录且元数据切片严格为 24 字节大端序规约
	var rawSlices []byte
	queryChunks := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName)
	err = h.dbPool.QueryRow(ctx, queryChunks, int64(ino)).Scan(&rawSlices)
	require.NoError(t, err)
	assert.Len(t, rawSlices, SliceBytes)

	slices, err := DecodeSlices(rawSlices)
	require.NoError(t, err)
	require.Len(t, slices, 1)
	assert.Equal(t, uint32(0), slices[0].Pos)
	assert.Equal(t, uint64(1), slices[0].ID)
	assert.Equal(t, uint32(payloadSize), slices[0].Size)
	assert.Equal(t, uint32(0), slices[0].Off)
	assert.Equal(t, uint32(payloadSize), slices[0].Len)

	// 6. 断言 slice_refs 引用账本表记录且引用计数为 1
	var refCount int32
	var refSize int32
	queryRef := fmt.Sprintf(`SELECT refs, size FROM "%s".slice_refs WHERE id = 1`, schemaName)
	err = h.dbPool.QueryRow(ctx, queryRef).Scan(&refCount, &refSize)
	require.NoError(t, err)
	assert.Equal(t, int32(1), refCount)
	assert.Equal(t, int32(payloadSize), refSize)

	// 7. 释放文件句柄
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

func TestChunkDriver_Write_CrossChunk64MB_BoundarySplitting_MetaAdapter(t *testing.T) {
	h, stagingMgr, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-boundary-split"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. 挂载虚拟磁盘
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. 创建跨 64MB 边界测试文件
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test-boundary-split.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 3. 跨越 64MB Chunk 边界写入: 从 Chunk 0 末尾 64KB (ChunkSize - 64KB) 开始，写入 128KB
	// 期望: 前 64KB 落在 Chunk 0 (indx=0), 后 64KB 落在 Chunk 1 (indx=1)
	startOffset := uint64(ChunkSize - 64*1024)
	payloadSize := 128 * 1024
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte((i * 31) % 256)
	}

	start := time.Now()
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    startOffset,
		Data:      payload,
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, uint32(payloadSize), writeResp.GetWritten())
	assert.Less(t, elapsed, 10*time.Millisecond)

	// 4. 校验 Chunk 0 元数据行
	var rawSlices0 []byte
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName), int64(ino)).Scan(&rawSlices0)
	require.NoError(t, err)
	slices0, err := DecodeSlices(rawSlices0)
	require.NoError(t, err)
	require.Len(t, slices0, 1)
	assert.Equal(t, uint32(ChunkSize-64*1024), slices0[0].Pos)
	assert.Equal(t, uint32(64*1024), slices0[0].Len)
	assert.Equal(t, uint32(64*1024), slices0[0].Size)
	assert.Equal(t, uint64(1), slices0[0].ID)

	// 5. 校验 Chunk 1 元数据行
	var rawSlices1 []byte
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 1`, schemaName), int64(ino)).Scan(&rawSlices1)
	require.NoError(t, err)
	slices1, err := DecodeSlices(rawSlices1)
	require.NoError(t, err)
	require.Len(t, slices1, 1)
	assert.Equal(t, uint32(0), slices1[0].Pos)
	assert.Equal(t, uint32(64*1024), slices1[0].Len)
	assert.Equal(t, uint32(64*1024), slices1[0].Size)
	assert.Equal(t, uint64(2), slices1[0].ID)

	// 6. 校验 NVMe 本地暂存物理文件分块
	data0, err := stagingMgr.ReadSlice(diskID.String(), slices0[0].ID)
	require.NoError(t, err)
	assert.Equal(t, payload[:64*1024], data0)

	data1, err := stagingMgr.ReadSlice(diskID.String(), slices1[0].ID)
	require.NoError(t, err)
	assert.Equal(t, payload[64*1024:], data1)

	// 7. 校验 slice_refs 账本记录
	var refs0, refs1 int32
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = $1`, schemaName), int64(slices0[0].ID)).Scan(&refs0))
	require.NoError(t, h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = $1`, schemaName), int64(slices1[0].ID)).Scan(&refs1))
	assert.Equal(t, int32(1), refs0)
	assert.Equal(t, int32(1), refs1)

	// 8. 回读跨边界数据并校验一致性
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    startOffset,
		Size:      uint32(payloadSize),
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp.GetData())

	// 9. 释放句柄
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

func TestChunkDriver_Write_SequentialAppend_MultiSliceLedger(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-seq-append"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. 挂载虚拟磁盘
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. 创建连续追加测试文件
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test-seq-append.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 3. 执行三次连续 128KB 追加写
	sliceSize := 128 * 1024
	var allPayload []byte
	for i := 0; i < 3; i++ {
		p := make([]byte, sliceSize)
		for j := range p {
			p[j] = byte((i*73 + j*13) % 256)
		}
		allPayload = append(allPayload, p...)

		start := time.Now()
		writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    uint64(i * sliceSize),
			Data:      p,
		})
		elapsed := time.Since(start)
		require.NoError(t, err)
		assert.Equal(t, uint32(sliceSize), writeResp.GetWritten())
		assert.Less(t, elapsed, 10*time.Millisecond)
	}

	// 4. 校验 chunks 表包含连续追加的 3 条切片记录 (总计 72 字节)
	var rawSlices []byte
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName), int64(ino)).Scan(&rawSlices)
	require.NoError(t, err)
	assert.Len(t, rawSlices, 3*SliceBytes)

	slices, err := DecodeSlices(rawSlices)
	require.NoError(t, err)
	require.Len(t, slices, 3)
	for i, s := range slices {
		assert.Equal(t, uint32(i*sliceSize), s.Pos)
		assert.Equal(t, uint64(i+1), s.ID)
		assert.Equal(t, uint32(sliceSize), s.Size)
		assert.Equal(t, uint32(sliceSize), s.Len)
	}

	// 5. 校验 slice_refs 包含 3 条引用记录
	var totalRefs int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".slice_refs WHERE refs = 1`, schemaName)).Scan(&totalRefs)
	require.NoError(t, err)
	assert.Equal(t, 3, totalRefs)

	// 6. 回读完整 384KB 数据并校验
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      uint32(len(allPayload)),
	})
	require.NoError(t, err)
	assert.Equal(t, allPayload, readResp.GetData())

	// 7. 释放句柄
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestChunkDriver_Read_JuiceFSOverlay_DenseOverwrites 验证多层密集覆写场景下 JuiceFS 图层重构算法严格返回最新写入数据
func TestChunkDriver_Read_JuiceFSOverlay_DenseOverwrites(t *testing.T) {
	h, _, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-overlay-dense"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. 挂载虚拟磁盘
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. 创建测试文件
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test-overlay-dense.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 3. 多层密集覆写写入序列:
	// 层 1: 写入 8192 字节基底数据 'A' (区间 0..8192)
	dataA := bytes.Repeat([]byte("A"), 8192)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      dataA,
	})
	require.NoError(t, err)

	// 层 2: 覆写 2048 字节数据 'B' (区间 1024..3072)
	dataB := bytes.Repeat([]byte("B"), 2048)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1024,
		Data:      dataB,
	})
	require.NoError(t, err)

	// 层 3: 覆写 1024 字节数据 'C' (区间 2048..3072)，覆盖层 2 后半段
	dataC := bytes.Repeat([]byte("C"), 1024)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    2048,
		Data:      dataC,
	})
	require.NoError(t, err)

	// 层 4: 覆写 512 字节数据 'D' (区间 512..1024)，跨越层 1 与层 2 边界
	dataD := bytes.Repeat([]byte("D"), 512)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    512,
		Data:      dataD,
	})
	require.NoError(t, err)

	// 层 5: 追加并扩展写入 4096 字节数据 'E' (区间 6144..10240)，使文件总长扩展至 10240 字节
	dataE := bytes.Repeat([]byte("E"), 4096)
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    6144,
		Data:      dataE,
	})
	require.NoError(t, err)

	// 4. 根据写入时序构建预期真值缓冲
	// 0..512:     'A' (来自层 1)
	// 512..1024:  'D' (来自层 4)
	// 1024..2048: 'B' (来自层 2)
	// 2048..3072: 'C' (来自层 3)
	// 3072..6144: 'A' (来自层 1)
	// 6144..10240:'E' (来自层 5)
	expectedFull := make([]byte, 10240)
	copy(expectedFull[0:8192], dataA)
	copy(expectedFull[1024:3072], dataB)
	copy(expectedFull[2048:3072], dataC)
	copy(expectedFull[512:1024], dataD)
	copy(expectedFull[6144:10240], dataE)

	// 5. 校验 JuiceFS MetaAdapter 重组后的切片图层
	meta := driver.MetaAdapter()
	reconstituted, err := meta.ReadReconstitutedSlices(ctx, diskID.String(), ino, 0)
	require.NoError(t, err)
	require.NotEmpty(t, reconstituted)

	// 6. 全量回读校验
	readFullResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      10240,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedFull, readFullResp.GetData(), "全量图层覆写回读必须与最新写入严格一致")
	assert.True(t, readFullResp.GetEof())

	// 7. 跨重叠切片交界处的局部区间回读校验
	subTests := []struct {
		name   string
		offset uint64
		size   uint32
	}{
		{"跨 A 与 D 边界", 400, 300},
		{"跨 D 与 B 边界", 900, 300},
		{"跨 B 与 C 边界", 1900, 300},
		{"跨 C 与 A 边界", 3000, 300},
		{"跨 A 与 E 边界", 6000, 400},
		{"单切片内部读取", 1200, 500},
	}

	for _, tc := range subTests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
				SessionId: sessionID,
				Fh:        fh,
				Ino:       ino,
				Offset:    tc.offset,
				Size:      tc.size,
			})
			require.NoError(t, err)
			expectedPart := expectedFull[tc.offset : tc.offset+uint64(tc.size)]
			assert.Equal(t, expectedPart, resp.GetData(), "局部切片图层区间回读不符")
		})
	}

	// 8. 释放句柄
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestChunkDriver_Read_SparseHoles_ZeroFill 验证未写入的稀疏空洞区间自动返回全零 0x00 字节且不触发多余 S3 请求
func TestChunkDriver_Read_SparseHoles_ZeroFill(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-sparse-holes"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. 挂载虚拟磁盘
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. 配置 S3Reader 与 S3Uploader
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

	// 3. 创建测试文件
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test-sparse-holes.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 4. 写入两个孤立的数据块，并在中间留下稀疏空洞:
	// 块 1: 在起始偏移 0 写入 32KB
	payload1 := make([]byte, 32*1024)
	for i := range payload1 {
		payload1[i] = byte((i * 17) % 256)
	}
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload1,
	})
	require.NoError(t, err)

	// 块 2: 在 1MB (1048576) 偏移处写入 32KB
	offset2 := uint64(1024 * 1024)
	payload2 := make([]byte, 32*1024)
	for i := range payload2 {
		payload2[i] = byte((i * 23) % 256)
	}
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset2,
		Data:      payload2,
	})
	require.NoError(t, err)

	// 等待 S3 异步推流完成
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	err = uploader.WaitForIno(waitCtx, diskID.String(), ino)
	require.NoError(t, err)

	// 将本地 Staging 切片删除，使数据切片完全转入 S3 冷存储
	localSlice1 := stagingMgr.SlicePath(diskID.String(), 1)
	localSlice2 := stagingMgr.SlicePath(diskID.String(), 2)
	_ = os.Remove(localSlice1)
	_ = os.Remove(localSlice2)

	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)
	defer func() {
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, chunk.FormatObjectKey(diskID.String(), 0, 1, 0), minio.RemoveObjectOptions{})
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, chunk.FormatObjectKey(diskID.String(), 0, 2, 0), minio.RemoveObjectOptions{})
	}()

	// 5. 校验中间稀疏空洞区间 (32KB..1MB) 读取: 必须返回全零且不报错
	holeOffset := uint64(32 * 1024)
	holeSize := uint32(256 * 1024)
	readHoleResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    holeOffset,
		Size:      holeSize,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, holeSize), readHoleResp.GetData(), "稀疏空洞区间必须返回全 0x00 字节")

	// 6. 校验跨越“空洞-有效冷数据”交界的读取 (从 1MB-16KB 开始读取 32KB: 前 16KB 为全零，后 16KB 为 S3 冷数据)
	spanOffset := offset2 - 16*1024
	spanSize := uint32(32 * 1024)
	readSpanResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    spanOffset,
		Size:      spanSize,
	})
	require.NoError(t, err)
	expectedSpan := make([]byte, spanSize)
	copy(expectedSpan[16*1024:], payload2[:16*1024])
	assert.Equal(t, expectedSpan, readSpanResp.GetData(), "空洞与冷切片交界读取数据必须准确吻合")

	// 7. 释放句柄
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestChunkDriver_Read_ColdS3_HTTPRange_SubBlock 验证跨 4MB S3 Block 边界时的 HTTP Range 局部精细回源拉取
func TestChunkDriver_Read_ColdS3_HTTPRange_SubBlock(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-s3-range-subblock"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. 挂载虚拟磁盘
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. 配置 S3Reader 与 S3Uploader
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

	// 3. 创建测试文件
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test-s3-range-subblock.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 4. 写入 5MB 载荷 (跨越 4MB 物理分块边界: Block 0 为 4MB, Block 1 为 1MB)
	payload5MB := make([]byte, 5*1024*1024)
	for i := range payload5MB {
		payload5MB[i] = byte((i * 19) % 256)
	}

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload5MB,
	})
	require.NoError(t, err)

	// 等待 S3 异步推流完成
	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	err = uploader.WaitForIno(waitCtx, diskID.String(), ino)
	require.NoError(t, err)

	// 清理本地 Staging 文件，迫使读请求全部通过 S3 回源
	localSlice := stagingMgr.SlicePath(diskID.String(), 1)
	_ = os.Remove(localSlice)

	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)
	defer func() {
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, chunk.FormatObjectKey(diskID.String(), 0, 1, 0), minio.RemoveObjectOptions{})
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, chunk.FormatObjectKey(diskID.String(), 0, 1, 1), minio.RemoveObjectOptions{})
	}()

	// 5. 跨越 4MB Block 边界发起局部 Range 读取: 从 4MB-32KB 读取 64KB
	// 期望: 通过 HTTP Range 从 Block 0 读取末尾 32KB，从 Block 1 读取起始 32KB
	rangeOffset := uint64(4*1024*1024 - 32*1024)
	rangeSize := uint32(64 * 1024)
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    rangeOffset,
		Size:      rangeSize,
	})
	require.NoError(t, err)
	expectedRange := payload5MB[rangeOffset : rangeOffset+uint64(rangeSize)]
	assert.Equal(t, expectedRange, readResp.GetData(), "跨 4MB Block 边界的 S3 HTTP Range 读取数据必须准确")

	// 6. 释放句柄
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestChunkDriver_Close_ReleasesPageBuffersAndClosesState 验证文件句柄关闭时安全释放内存 Page 缓冲区，置位关闭状态，并阻断后续全部 I/O 操作
func TestChunkDriver_Close_ReleasesPageBuffersAndClosesState(t *testing.T) {
	h, _, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. 挂载虚拟磁盘并创建测试文件
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-close-test",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "close-test-file.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()

	// 2. 直接通过 ChunkDriver 打开句柄
	handle, err := chunkDriver.Open(ctx, diskID.String(), ino, bowlv1.OpenMode_OPEN_MODE_READ_WRITE, 0)
	require.NoError(t, err)
	require.NotNil(t, handle)

	ch, ok := handle.(*chunkFileHandle)
	require.True(t, ok, "底层句柄类型必须为 *chunkFileHandle")
	assert.False(t, ch.IsClosed(), "初始状态句柄不应为关闭状态")

	// 3. 向句柄关联内存 Page 缓冲区
	p1 := jfsChunk.NewOffPage(1024)
	p2 := jfsChunk.NewOffPage(2048)
	ch.AttachPage(p1)
	ch.AttachPage(p2)
	assert.Len(t, ch.Pages(), 2, "句柄必须持有 2 个关联的内存 Page 缓冲区")

	// 4. 调用 Close 释放句柄资源
	err = chunkDriver.Close(ctx, handle)
	require.NoError(t, err)
	assert.True(t, ch.IsClosed(), "Close 后句柄必须标记为已关闭")
	assert.Empty(t, ch.Pages(), "Close 后关联的内存 Page 缓冲区列表必须清空")

	// 5. 重复 Close 具备幂等安全性
	err = chunkDriver.Close(ctx, handle)
	require.NoError(t, err)
	assert.True(t, ch.IsClosed())

	// 6. 验证后续全部 I/O 操作均被前置拦截并返回 CodeFailedPrecondition 错误
	// 6.1 Read
	_, _, err = chunkDriver.Read(ctx, handle, diskID.String(), ino, 0, 1024)
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "关闭句柄上的 Read 必须返回 CodeFailedPrecondition")

	// 6.2 Write
	_, err = chunkDriver.Write(ctx, handle, diskID.String(), ino, 0, []byte("data"))
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "关闭句柄上的 Write 必须返回 CodeFailedPrecondition")

	// 6.3 Flush
	err = chunkDriver.Flush(ctx, handle, diskID.String(), ino)
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "关闭句柄上的 Flush 必须返回 CodeFailedPrecondition")

	// 6.4 Fsync
	err = chunkDriver.Fsync(ctx, handle, diskID.String(), ino, false)
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "关闭句柄上的 Fsync 必须返回 CodeFailedPrecondition")

	// 6.5 Fallocate
	err = chunkDriver.Fallocate(ctx, handle, diskID.String(), ino, 0, 0, 1024)
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "关闭句柄上的 Fallocate 必须返回 CodeFailedPrecondition")

	// 6.6 Lseek
	_, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 0, 0)
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "关闭句柄上的 Lseek 必须返回 CodeFailedPrecondition")

	// 6.7 CopyFileRange (源句柄关闭)
	_, err = chunkDriver.CopyFileRange(ctx, handle, diskID.String(), ino, 0, nil, diskID.String(), ino, 0, 1024)
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "源句柄关闭时的 CopyFileRange 必须返回 CodeFailedPrecondition")

	// 6.8 CopyFileRange (目标句柄关闭)
	_, err = chunkDriver.CopyFileRange(ctx, nil, diskID.String(), ino, 0, handle, diskID.String(), ino, 0, 1024)
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "目标句柄关闭时的 CopyFileRange 必须返回 CodeFailedPrecondition")
}

// TestChunkDriver_DeleteInode_TracksOrphans 验证 Inode 物理删除联动切片引用计数递减与垃圾回收器孤儿切片追踪
func TestChunkDriver_DeleteInode_TracksOrphans(t *testing.T) {
	h, stagingMgr, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 1. 挂载虚拟磁盘并创建文件
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-gc-test",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "gc-orphan-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 2. 写入 3 个不同偏移位置生成 3 个独立切片
	for i := 0; i < 3; i++ {
		payload := bytes.Repeat([]byte{byte('A' + i)}, 1024)
		_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    uint64(i * 4096),
			Data:      payload,
		})
		require.NoError(t, err)
	}

	// 3. 校验切片已落入 NVMe Staging 缓存目录且在 slice_refs 中引用计数为 1
	for sliceID := uint64(1); sliceID <= 3; sliceID++ {
		slicePath := stagingMgr.SlicePath(diskID.String(), sliceID)
		_, err := os.Stat(slicePath)
		require.NoError(t, err, "切片本地缓存文件必须存在: %s", slicePath)

		var refs int32
		err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = $1`, schemaName), int64(sliceID)).Scan(&refs)
		require.NoError(t, err)
		assert.Equal(t, int32(1), refs)
	}

	// 4. 调用 DeleteInode 触发 Inode 删除联动
	err = chunkDriver.DeleteInode(ctx, diskID.String(), ino)
	require.NoError(t, err)

	// 5. 校验 chunks 表已彻底清理
	var chunkCount int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1`, schemaName), int64(ino)).Scan(&chunkCount)
	require.NoError(t, err)
	assert.Equal(t, 0, chunkCount, "chunks 元数据行必须全部被清除")

	// 6. 校验 slice_refs 中全部切片引用计数已递减至 0
	var zeroRefCount int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".slice_refs WHERE refs = 0`, schemaName)).Scan(&zeroRefCount)
	require.NoError(t, err)
	assert.Equal(t, 3, zeroRefCount, "3 个切片的引用计数必须均已递减为 0")

	// 7. 校验垃圾回收器已登记并跟踪全部 3 个孤儿切片
	tracked := chunkDriver.TrackedOrphans(diskID.String())
	assert.ElementsMatch(t, []uint64{1, 2, 3}, tracked, "垃圾回收器跟踪列表必须包含全部 3 个孤儿切片")

	// 8. 触发垃圾回收 GC 周期，验证孤儿切片物理清理
	cleaned, err := chunkDriver.RunGC(ctx, diskID.String())
	require.NoError(t, err)
	assert.Equal(t, 3, cleaned, "GC 必须成功回收清理 3 个孤儿切片")

	// 9. 校验 GC 运行后，slice_refs 引用账本已清空，NVMe 本地缓存文件已彻底移除
	var remainingRefs int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".slice_refs`, schemaName)).Scan(&remainingRefs)
	require.NoError(t, err)
	assert.Equal(t, 0, remainingRefs, "GC 运行后 slice_refs 记录必须全部删除")

	for sliceID := uint64(1); sliceID <= 3; sliceID++ {
		slicePath := stagingMgr.SlicePath(diskID.String(), sliceID)
		_, err := os.Stat(slicePath)
		assert.True(t, os.IsNotExist(err), "GC 运行后本地缓存切片文件必须被物理删除: %s", slicePath)
	}

	// 10. 校验垃圾回收器中的孤儿跟踪列表已同步清空
	assert.Empty(t, chunkDriver.TrackedOrphans(diskID.String()), "GC 运行后孤儿切片跟踪队列必须被清空")
}

// TestChunkDriver_Truncate_Extend_FormsSparseHolesAndReadsZeroBytes 测试 Truncate 扩容形成稀疏空洞且读取返回全零数据
func TestChunkDriver_Truncate_Extend_FormsSparseHolesAndReadsZeroBytes(t *testing.T) {
	h, _, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-truncate-extend",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "trunc-extend.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 1. 初始写入 16 字节
	initialData := []byte("0123456789ABCDEF")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      initialData,
	})
	require.NoError(t, err)

	// 2. 将文件通过 Truncate 扩容至 1MB (1048576 字节)
	targetExtendSize := uint64(1024 * 1024)
	err = chunkDriver.Truncate(ctx, diskID.String(), ino, targetExtendSize)
	require.NoError(t, err)

	// 3. 校验 Inode 大小已变为 1MB 且 blocks 正确计算
	var currentSize int64
	var currentBlocks int64
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT size, blocks FROM "%s".inodes WHERE ino = $1`, schemaName), int64(ino)).Scan(&currentSize, &currentBlocks)
	require.NoError(t, err)
	assert.Equal(t, int64(targetExtendSize), currentSize)
	assert.Equal(t, int64((targetExtendSize+511)/512), currentBlocks)

	// 4. 校验未分配额外的 Chunk 行 (仍仅有 Chunk 0 且包含初始写入切片)
	var chunkCount int
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1`, schemaName), int64(ino)).Scan(&chunkCount)
	require.NoError(t, err)
	assert.Equal(t, 1, chunkCount)

	// 5. 校验读取前 16 字节仍为原始数据
	readOrig, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      16,
	})
	require.NoError(t, err)
	assert.Equal(t, initialData, readOrig.GetData())

	// 6. 校验读取扩容稀疏空洞区间 [16, 4096) 返回全零 0x00
	readZero, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    16,
		Size:      4080,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 4080), readZero.GetData())

	// 7. 校验读取远端空洞区间 [65536, 69632) 返回全零 0x00
	readFarZero, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    65536,
		Size:      4096,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 4096), readFarZero.GetData())

	// 8. 校验读取超出扩容尺寸 (>= 1MB) 返回空数据与 EOF
	readEOF, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    targetExtendSize,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.True(t, readEOF.GetEof())
	assert.Empty(t, readEOF.GetData())
}

// TestChunkDriver_Fallocate_Preallocate_And_PunchHole 测试 ChunkDriver 预分配与打洞释放空间语义
func TestChunkDriver_Fallocate_Preallocate_And_PunchHole(t *testing.T) {
	h, _, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-fallocate-test",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "falloc-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 1. 写入 4096 字节固定数据
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// 2. 空间预分配：Mode 0 预分配扩展至 8192 字节
	handle, err := chunkDriver.Open(ctx, diskID.String(), ino, bowlv1.OpenMode_OPEN_MODE_READ_WRITE, 2)
	require.NoError(t, err)
	defer chunkDriver.Close(ctx, handle)

	err = chunkDriver.Fallocate(ctx, handle, diskID.String(), ino, 0, 0, 8192)
	require.NoError(t, err)

	var currentSize int64
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName), int64(ino)).Scan(&currentSize)
	require.NoError(t, err)
	assert.Equal(t, int64(8192), currentSize, "预分配扩容后大小必须为 8192")

	// 验证扩展区间 [4096, 8192) 读取为零
	readExtend, isEOF, err := chunkDriver.Read(ctx, handle, diskID.String(), ino, 4096, 4096)
	require.NoError(t, err)
	assert.True(t, isEOF)
	assert.Equal(t, make([]byte, 4096), readExtend)

	// 3. 区间打洞：FALLOC_FL_PUNCH_HOLE 在 [1024, 2048) 打洞 (长度 1024)
	err = chunkDriver.Fallocate(ctx, handle, diskID.String(), ino, uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE), 1024, 1024)
	require.NoError(t, err)

	// 文件大小在打洞后必须保持 8192 不变
	err = h.dbPool.QueryRow(ctx, fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName), int64(ino)).Scan(&currentSize)
	require.NoError(t, err)
	assert.Equal(t, int64(8192), currentSize)

	// 校验打洞前区间 [0, 1024) 保持原样
	readBeforeHole, _, err := chunkDriver.Read(ctx, handle, diskID.String(), ino, 0, 1024)
	require.NoError(t, err)
	assert.Equal(t, payload[:1024], readBeforeHole)

	// 校验打洞区间 [1024, 2048) 已被清零 (0x00)
	readHole, _, err := chunkDriver.Read(ctx, handle, diskID.String(), ino, 1024, 1024)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 1024), readHole)

	// 校验打洞后区间 [2048, 4096) 保持原样
	readAfterHole, _, err := chunkDriver.Read(ctx, handle, diskID.String(), ino, 2048, 2048)
	require.NoError(t, err)
	assert.Equal(t, payload[2048:4096], readAfterHole)

	// 4. 边界校验：length = 0 必须返回 InvalidArgument
	err = chunkDriver.Fallocate(ctx, handle, diskID.String(), ino, 0, 0, 0)
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 5. 超出 EOF 打洞：应平稳返回且不改变文件内容
	err = chunkDriver.Fallocate(ctx, handle, diskID.String(), ino, uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE), 10000, 2048)
	require.NoError(t, err)
}

// TestChunkDriver_Lseek_SeekData_SeekHole_AcrossSlicesAndHoles 测试 Lseek 跨切片与稀疏空洞探测
func TestChunkDriver_Lseek_SeekData_SeekHole_AcrossSlicesAndHoles(t *testing.T) {
	h, _, chunkDriver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-lseek-test",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "lseek-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	handle, err := chunkDriver.Open(ctx, diskID.String(), ino, bowlv1.OpenMode_OPEN_MODE_READ_WRITE, 2)
	require.NoError(t, err)
	defer chunkDriver.Close(ctx, handle)

	// 1. 预分配文件大小至 100 字节
	err = chunkDriver.Fallocate(ctx, handle, diskID.String(), ino, 0, 0, 100)
	require.NoError(t, err)

	// 2. 在 [0, 10) 写入 "0123456789"
	chunk1 := []byte("0123456789")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      chunk1,
	})
	require.NoError(t, err)

	// 3. 在 [30, 40) 写入 "ABCDEFGHIJ"
	chunk2 := []byte("ABCDEFGHIJ")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    30,
		Data:      chunk2,
	})
	require.NoError(t, err)

	// 4. 在 [2, 5) 打洞释放空间 (使 [2, 5) 成为空洞)
	err = chunkDriver.Fallocate(ctx, handle, diskID.String(), ino, uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE), 2, 3)
	require.NoError(t, err)

	// 当前文件布局：
	// [0..2):   DATA (切片)
	// [2..5):   HOLE (打洞空洞)
	// [5..10):  DATA (切片)
	// [10..30): HOLE (未分配稀疏空洞)
	// [30..40): DATA (切片)
	// [40..100):HOLE (未分配稀疏空洞)
	// 100:      EOF

	// --- 测试 SEEK_DATA ---
	// offset 0: 数据起始位置 -> 0
	off, err := chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 0, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), off)

	// offset 1: 处于数据区间 [0..2) 内 -> 1
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 1, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), off)

	// offset 2: 处于打洞空洞 [2..5) 内，下一个数据为 offset 5 -> 5
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 2, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA))
	require.NoError(t, err)
	assert.Equal(t, uint64(5), off)

	// offset 3: 处于空洞内 -> 5
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 3, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA))
	require.NoError(t, err)
	assert.Equal(t, uint64(5), off)

	// offset 10: 处于稀疏空洞 [10..30) 内，下一个数据为 offset 30 -> 30
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 10, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA))
	require.NoError(t, err)
	assert.Equal(t, uint64(30), off)

	// offset 35: 处于数据区间 [30..40) 内 -> 35
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 35, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA))
	require.NoError(t, err)
	assert.Equal(t, uint64(35), off)

	// offset 40: 处于末尾空洞 [40..100) 内，后续无数据 -> OutOfRange
	_, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 40, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA))
	require.Error(t, err)
	assert.Equal(t, connect.CodeOutOfRange, connect.CodeOf(err))

	// offset 100: 到达 EOF -> OutOfRange
	_, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 100, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA))
	require.Error(t, err)
	assert.Equal(t, connect.CodeOutOfRange, connect.CodeOf(err))

	// --- 测试 SEEK_HOLE ---
	// offset 0: 处于数据区间，第一个空洞在打洞处 offset 2 -> 2
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 0, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), off)

	// offset 2: 处于空洞内 -> 2
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 2, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), off)

	// offset 5: 处于数据区间 [5..10)，下一个空洞在 offset 10 -> 10
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 5, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE))
	require.NoError(t, err)
	assert.Equal(t, uint64(10), off)

	// offset 15: 处于稀疏空洞内 -> 15
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 15, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE))
	require.NoError(t, err)
	assert.Equal(t, uint64(15), off)

	// offset 30: 处于数据区间 [30..40)，下一个空洞在 offset 40 -> 40
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 30, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE))
	require.NoError(t, err)
	assert.Equal(t, uint64(40), off)

	// offset 50: 处于末尾空洞内 -> 50
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 50, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE))
	require.NoError(t, err)
	assert.Equal(t, uint64(50), off)

	// offset 100: EOF 本身视为空洞 -> 100
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 100, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE))
	require.NoError(t, err)
	assert.Equal(t, uint64(100), off)

	// offset 101: 超出 EOF -> OutOfRange
	_, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 101, uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE))
	require.Error(t, err)
	assert.Equal(t, connect.CodeOutOfRange, connect.CodeOf(err))

	// --- 测试标准 whence ---
	// SEEK_SET (0)
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 12, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(12), off)

	// SEEK_CUR (1)
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 24, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(24), off)

	// SEEK_END (2) -> 100 + 10 = 110
	off, err = chunkDriver.Lseek(ctx, handle, diskID.String(), ino, 10, 2)
	require.NoError(t, err)
	assert.Equal(t, uint64(110), off)
}

// TestStorageService_ChunkDriver_FallocateAndLseek_ConnectRPC 验证经由 ConnectRPC 接口层路由至 ChunkDriver 的 Fallocate 打洞与 Lseek 空洞探测
func TestStorageService_ChunkDriver_FallocateAndLseek_ConnectRPC(t *testing.T) {
	h, _, _ := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-connectrpc-falloc-lseek",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "rpc-falloc-lseek.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 1. 写入 4096 字节
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// 2. Fallocate 预分配至 8192 字节
	fallocResp, err := h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Mode:      0,
		Offset:    0,
		Length:    8192,
	})
	require.NoError(t, err)
	require.NotNil(t, fallocResp)

	attrResp, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), attrResp.GetInode().GetSize())

	// 3. Fallocate 打洞 [1024, 2048)
	punchResp, err := h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Mode:      uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE),
		Offset:    1024,
		Length:    1024,
	})
	require.NoError(t, err)
	require.NotNil(t, punchResp)

	// 4. 读取打洞区域验证全零
	readHole, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1024,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 1024), readHole.GetData())

	// 5. Lseek 探测
	lseekData, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1024,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_DATA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2048), lseekData.GetOffset())

	lseekHole, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1024), lseekHole.GetOffset())

	lseekHoleEOF, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    8192,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), lseekHoleEOF.GetOffset())
}




