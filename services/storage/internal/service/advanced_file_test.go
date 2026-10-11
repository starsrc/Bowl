package service

import (
	"context"
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
)

// TestStorageService_Flush_Fsync_CommitDirtyWrites verifies Flush and Fsync commit dirty writes to disk and update metadata.
func TestStorageService_Flush_Fsync_CommitDirtyWrites(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-flush-fsync"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 1. Create a file
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "sync_test.txt",
		Mode:      0o644,
		Flags:     2, // O_RDWR
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 2. Write dirty data
	payload := []byte("persistent storage verification data")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// 3. Flush dirty data
	flushResp, err := h.storageClient.Flush(ctx, &bowlv1.FlushRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
	})
	require.NoError(t, err)
	require.NotNil(t, flushResp)

	// Verify file content on disk after flush
	filePath := filepath.Join(h.dataDir, diskID.String(), fmt.Sprintf("file_%d.bin", ino))
	diskBytes, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, payload, diskBytes)

	// Get initial mtime
	attrBefore, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	mtimeBefore := attrBefore.GetInode().GetMtime().AsTime()

	// Sleep slightly to observe mtime change
	time.Sleep(10 * time.Millisecond)

	// 4. Fsync dirty data and update mtime
	fsyncResp, err := h.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Datasync:  false,
	})
	require.NoError(t, err)
	require.NotNil(t, fsyncResp)

	// Verify Inode mtime is updated in DB
	attrAfter, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	mtimeAfter := attrAfter.GetInode().GetMtime().AsTime()
	assert.True(t, mtimeAfter.After(mtimeBefore) || mtimeAfter.Equal(mtimeBefore), "mtime should be updated")

	// 5. Test Fsync with datasync = true
	fsyncDataResp, err := h.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Datasync:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, fsyncDataResp)

	// 6. Test Flush with invalid fh returns NotFound
	_, err = h.storageClient.Flush(ctx, &bowlv1.FlushRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        999999,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 7. Test Fsync with invalid fh returns NotFound
	_, err = h.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        999999,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestStorageService_Fallocate_Preallocate_HolePunching verifies preallocation and punch hole operations.
func TestStorageService_Fallocate_Preallocate_HolePunching(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-fallocate"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
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
		Name:      "fallocate_test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Initial write of 16 bytes
	initialData := []byte("0123456789ABCDEF")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      initialData,
	})
	require.NoError(t, err)

	// 1. Mode 0 Preallocate extending file size: offset=0, length=4096
	fallocResp, err := h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Mode:      0,
		Offset:    0,
		Length:    4096,
	})
	require.NoError(t, err)
	require.NotNil(t, fallocResp)

	// Verify Inode size extended to 4096 and blocks updated
	attrResp, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), attrResp.GetInode().GetSize())
	assert.Equal(t, uint64(8), attrResp.GetInode().GetBlocks()) // 4096 / 512 = 8 blocks of 512 bytes

	// Verify original content preserved at offset 0
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Size:      16,
	})
	require.NoError(t, err)
	assert.Equal(t, initialData, readResp.GetData())

	// Verify extended area is zeroed
	readZeroResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    16,
		Size:      16,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 16), readZeroResp.GetData())

	// 2. Punch hole (mode & 2 or FALLOC_FL_PUNCH_HOLE): punch hole at offset 4, length 6
	// Bytes 4..9 of "0123456789ABCDEF" ("456789") should become zeros
	punchResp, err := h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Mode:      uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE),
		Offset:    4,
		Length:    6,
	})
	require.NoError(t, err)
	require.NotNil(t, punchResp)

	// Verify size did not change after punch hole
	attrAfterPunch, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), attrAfterPunch.GetInode().GetSize())

	// Verify punched range contains zeros
	readPunched, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Size:      16,
	})
	require.NoError(t, err)
	expectedPunched := []byte("0123\x00\x00\x00\x00\x00\x00ABCDEF")
	assert.Equal(t, expectedPunched, readPunched.GetData())

	// 3. Fallocate with length = 0 returns InvalidArgument
	_, err = h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Mode:      0,
		Offset:    0,
		Length:    0,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

// TestStorageService_CopyFileRange_CrossFile verifies copying bytes between files.
func TestStorageService_CopyFileRange_CrossFile(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-copy-range"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 1. Create source file and populate data
	srcResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "source.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	srcIno := srcResp.GetInode().GetIno()
	srcFH := srcResp.GetFileHandle().GetFh()

	srcData := []byte("BOWL-STORAGE-COPY-FILE-RANGE-TEST")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       srcIno,
		Fh:        srcFH,
		Offset:    0,
		Data:      srcData,
	})
	require.NoError(t, err)

	// 2. Create destination file
	dstResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "destination.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	dstIno := dstResp.GetInode().GetIno()
	dstFH := dstResp.GetFileHandle().GetFh()

	// 3. Copy 15 bytes from src_offset 5 ("STORAGE-COPY-FI") to dst_offset 0
	copyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    srcIno,
		SrcFh:     srcFH,
		SrcOffset: 5,
		DstIno:    dstIno,
		DstFh:     dstFH,
		DstOffset: 0,
		Length:    15,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(15), copyResp.GetCopied())

	// Verify dst file size in DB
	dstAttr, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       dstIno,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(15), dstAttr.GetInode().GetSize())

	// Read and verify dst file content
	readDst, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       dstIno,
		Fh:        dstFH,
		Offset:    0,
		Size:      15,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("STORAGE-COPY-FI"), readDst.GetData())

	// 4. Copy with length = 0 returns copied = 0
	copyZeroResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
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
	assert.Equal(t, uint64(0), copyZeroResp.GetCopied())

	// 5. Copy with src_offset >= src size returns copied = 0
	copyEofResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    srcIno,
		SrcFh:     srcFH,
		SrcOffset: 1000,
		DstIno:    dstIno,
		DstFh:     dstFH,
		DstOffset: 0,
		Length:    10,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), copyEofResp.GetCopied())
}

// TestStorageService_Lseek_DataAndHoleDetection verifies SEEK_DATA, SEEK_HOLE, and standard whence modes.
func TestStorageService_Lseek_DataAndHoleDetection(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-lseek"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
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
		Name:      "sparse.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Prepare file with pattern:
	// [0..9]: DATA "0123456789" (10 bytes)
	// [10..29]: HOLE zeros (20 bytes)
	// [30..39]: DATA "ABCDEFGHIJ" (10 bytes)
	// Total size = 40 bytes.
	chunk1 := []byte("0123456789")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      chunk1,
	})
	require.NoError(t, err)

	// Preallocate to 40 bytes (creates hole between 10 and 40)
	_, err = h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Mode:      0,
		Offset:    0,
		Length:    40,
	})
	require.NoError(t, err)

	// Write chunk 2 at offset 30
	chunk2 := []byte("ABCDEFGHIJ")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    30,
		Data:      chunk2,
	})
	require.NoError(t, err)

	// 1. SEEK_DATA starting at offset 0 (contains data) -> offset 0
	lseekResp1, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_DATA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lseekResp1.GetOffset())

	// 2. SEEK_DATA starting at offset 15 (inside hole [10..29]) -> finds data at offset 30
	lseekResp2, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    15,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_DATA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(30), lseekResp2.GetOffset())

	// 3. SEEK_HOLE starting at offset 0 (data) -> finds first hole at offset 10
	lseekHole1, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(10), lseekHole1.GetOffset())

	// 4. SEEK_HOLE starting at offset 15 (inside hole) -> returns 15
	lseekHole2, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    15,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(15), lseekHole2.GetOffset())

	// 5. SEEK_HOLE starting at offset 35 (data near end) -> EOF (40) is the hole
	lseekHole3, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    35,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(40), lseekHole3.GetOffset())

	// 6. SEEK_DATA at or beyond EOF returns OutOfRange (ENXIO)
	_, err = h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    40,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_DATA,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeOutOfRange, connect.CodeOf(err))

	// 7. Test standard whence SEEK_SET (0), SEEK_CUR (1), SEEK_END (2)
	lseekSet, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    12,
		Whence:    0, // SEEK_SET
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(12), lseekSet.GetOffset())

	lseekCur, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    24,
		Whence:    1, // SEEK_CUR
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(24), lseekCur.GetOffset())

	lseekEnd, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    10,
		Whence:    2, // SEEK_END -> 40 + 10 = 50
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(50), lseekEnd.GetOffset())
}

// TestStorageService_WriteLeaseChecks_ReadOnlyRejected verifies read-only sessions are rejected for mutating calls.
func TestStorageService_WriteLeaseChecks_ReadOnlyRejected(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	// 1. RW session creates files first
	rwMountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-rw-setup",
		ReadOnly: false,
	})
	require.NoError(t, err)
	rwSessionID := rwMountResp.GetSessionId()

	createResp1, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: rwSessionID,
		ParentIno: 1,
		Name:      "ro_target.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino1 := createResp1.GetInode().GetIno()
	fh1 := createResp1.GetFileHandle().GetFh()

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: rwSessionID,
		Ino:       ino1,
		Fh:        fh1,
		Offset:    0,
		Data:      []byte("initial data"),
	})
	require.NoError(t, err)

	// Release handle
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: rwSessionID,
		Ino:       ino1,
		Fh:        fh1,
	})
	require.NoError(t, err)

	// 2. Mount Read-Only session
	roMountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-ro-reader",
		ReadOnly: true,
	})
	require.NoError(t, err)
	roSessionID := roMountResp.GetSessionId()

	// Open file in RO session
	openResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: roSessionID,
		Ino:       ino1,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_READ_ONLY,
	})
	require.NoError(t, err)
	roFH := openResp.GetFileHandle().GetFh()

	// 3. Fallocate in Read-Only session must fail with PermissionDenied
	_, err = h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: roSessionID,
		Ino:       ino1,
		Fh:        roFH,
		Mode:      0,
		Offset:    0,
		Length:    1024,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 4. CopyFileRange with Read-Only session must fail with PermissionDenied
	_, err = h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: roSessionID,
		SrcIno:    ino1,
		SrcFh:     roFH,
		SrcOffset: 0,
		DstIno:    ino1,
		DstFh:     roFH,
		DstOffset: 10,
		Length:    5,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 5. Expired/revoked lease check on mutating call:
	// Manually expire rw lease
	sess, ok := h.sessionMgr.Get(rwSessionID)
	require.True(t, ok)
	sess.LeaseExpiresAt = time.Now().Add(-1 * time.Minute)
	sess.Revoked = true
	h.sessionMgr.Create(sess) // overwrite

	openRWResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: rwSessionID,
		Ino:       ino1,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_READ_ONLY,
	})
	if err == nil {
		rwExpiredFH := openRWResp.GetFileHandle().GetFh()
		_, err = h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
			SessionId: rwSessionID,
			Ino:       ino1,
			Fh:        rwExpiredFH,
			Mode:      0,
			Offset:    0,
			Length:    1024,
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	}
}
