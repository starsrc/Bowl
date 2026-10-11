package service

import (
	"context"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

func TestStorageService_FileIO_Roundtrip(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-io-roundtrip"

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

	// 1. Create a regular file
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test_io.txt",
		Mode:      0o644,
		Flags:     2, // O_RDWR
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	createFh := createResp.GetFileHandle().GetFh()
	require.Greater(t, ino, uint64(1))
	require.Greater(t, createFh, uint64(0))

	// 2. Write data using createFh
	payload := []byte("Hello Bowl File Storage!")
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        createFh,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload)), writeResp.GetWritten())

	// 3. Verify Inode size updated in DB
	attrResp, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(len(payload)), attrResp.GetInode().GetSize())

	// 4. Read data using createFh
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        createFh,
		Offset:    0,
		Size:      uint32(len(payload) + 10),
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp.GetData())
	assert.True(t, readResp.GetEof())

	// 5. Open the existing file with OpenRequest
	openResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID,
		Ino:       ino,
		Flags:     0, // O_RDONLY
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_READ_ONLY,
	})
	require.NoError(t, err)
	require.NotNil(t, openResp.GetFileHandle())
	openFh := openResp.GetFileHandle().GetFh()
	assert.NotEqual(t, createFh, openFh)
	assert.Equal(t, ino, openResp.GetFileHandle().GetIno())

	// 6. Read slice from offset 6, size 4 ("Bowl") using openFh
	readSliceResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        openFh,
		Offset:    6,
		Size:      4,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("Bowl"), readSliceResp.GetData())
	assert.False(t, readSliceResp.GetEof())

	// 7. Release handles
	rel1, err := h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        createFh,
	})
	require.NoError(t, err)
	require.NotNil(t, rel1)

	rel2, err := h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        openFh,
	})
	require.NoError(t, err)
	require.NotNil(t, rel2)
}

func TestStorageService_Write_PermissionDenied_ReadOnlySession(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-ro"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	// First mount read-write to create the file
	rwMountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "writer-setup",
		ReadOnly: false,
	})
	require.NoError(t, err)

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: rwMountResp.GetSessionId(),
		ParentIno: 1,
		Name:      "ro_test.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()

	// Mount as read-only session
	roMountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: true,
	})
	require.NoError(t, err)
	roSessionID := roMountResp.GetSessionId()

	// 1. Attempt to open in write mode on read-only session -> PermissionDenied
	_, err = h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: roSessionID,
		Ino:       ino,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_WRITE_ONLY,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 2. Open in read-only mode -> succeeds
	openResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: roSessionID,
		Ino:       ino,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_READ_ONLY,
	})
	require.NoError(t, err)
	roFh := openResp.GetFileHandle().GetFh()

	// 3. Attempt Write using this handle -> PermissionDenied
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: roSessionID,
		Ino:       ino,
		Fh:        roFh,
		Offset:    0,
		Data:      []byte("Illegal Write"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

func TestStorageService_Write_PermissionDenied_ExpiredLease(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-lease-expire"

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
		Name:      "lease_test.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Artificially expire the lease in memory manager
	sess, found := h.sessionMgr.Get(sessionID)
	require.True(t, found)
	sess.LeaseExpiresAt = time.Now().Add(-1 * time.Second)
	sess.IsWriteLease = false
	sess.Revoked = true
	h.sessionMgr.Create(sess)

	// Attempt Write after lease expired -> PermissionDenied
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      []byte("Expired write"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

func TestStorageService_Read_OffsetsAndEOF(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-offsets"

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
		Name:      "pattern.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 100 bytes of patterned data: 0, 1, 2, ..., 99
	pattern := make([]byte, 100)
	for i := 0; i < 100; i++ {
		pattern[i] = byte(i)
	}

	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      pattern,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(100), writeResp.GetWritten())

	// Read offset 0, size 20 -> bytes 0..19, EOF = false
	r1, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Size:      20,
	})
	require.NoError(t, err)
	assert.Equal(t, pattern[0:20], r1.GetData())
	assert.False(t, r1.GetEof())

	// Read offset 40, size 30 -> bytes 40..69, EOF = false
	r2, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    40,
		Size:      30,
	})
	require.NoError(t, err)
	assert.Equal(t, pattern[40:70], r2.GetData())
	assert.False(t, r2.GetEof())

	// Read offset 80, size 30 -> only 20 bytes available (80..99), EOF = true
	r3, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    80,
		Size:      30,
	})
	require.NoError(t, err)
	assert.Equal(t, pattern[80:100], r3.GetData())
	assert.True(t, r3.GetEof())

	// Read offset 100, size 20 -> 0 bytes, EOF = true
	r4, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    100,
		Size:      20,
	})
	require.NoError(t, err)
	assert.Empty(t, r4.GetData())
	assert.True(t, r4.GetEof())

	// Read offset 150, size 10 -> 0 bytes, EOF = true
	r5, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    150,
		Size:      10,
	})
	require.NoError(t, err)
	assert.Empty(t, r5.GetData())
	assert.True(t, r5.GetEof())

	// Read offset 0, size 200 -> all 100 bytes, EOF = true
	r6, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Size:      200,
	})
	require.NoError(t, err)
	assert.Equal(t, pattern, r6.GetData())
	assert.True(t, r6.GetEof())

	// Read size 0 -> empty data, EOF = false
	r0, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    10,
		Size:      0,
	})
	require.NoError(t, err)
	assert.Empty(t, r0.GetData())
	assert.False(t, r0.GetEof())
}

func TestStorageService_Release_ClosesHandle_SubsequentFails(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-release-test"

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
		Name:      "test_release.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Release handle
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
	})
	require.NoError(t, err)

	// Subsequent Read with closed fh must fail with CodeNotFound
	_, err = h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Size:      10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Subsequent Write with closed fh must fail with CodeNotFound
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      []byte("Write after release"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Subsequent Release with closed fh must fail with CodeNotFound
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_Open_Validation(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-validation"

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

	// 1. Empty session_id -> CodeInvalidArgument
	_, err = h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: "",
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. Non-existent session_id -> CodeNotFound
	_, err = h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: "non-existent-session",
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 3. ino == 0 -> CodeInvalidArgument
	_, err = h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID,
		Ino:       0,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 4. Non-existent ino -> CodeNotFound
	_, err = h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID,
		Ino:       99999,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 5. Open directory inode (root ino = 1) -> CodeInvalidArgument
	_, err = h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestStorageService_SparseWriteAndRead(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-sparse"

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

	// Write at offset 1024 with 10 bytes
	data := []byte("SPARSEDATA")
	offset := uint64(1024)
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    offset,
		Data:      data,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(len(data)), writeResp.GetWritten())

	// Check Inode size in DB -> 1024 + 10 = 1034
	attrResp, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1034), attrResp.GetInode().GetSize())

	// Read hole at offset 0, size 16 -> should be 16 zeros
	readHole, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Size:      16,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 16), readHole.GetData())
	assert.False(t, readHole.GetEof())

	// Read written data at offset 1024, size 10 -> "SPARSEDATA", EOF = true
	readData, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        fh,
		Offset:    offset,
		Size:      10,
	})
	require.NoError(t, err)
	assert.Equal(t, data, readData.GetData())
	assert.True(t, readData.GetEof())
}

func TestStorageService_HandleAccessModeValidation(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-access-mode"

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
		Name:      "mode_check.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()

	// 1. Open read-only handle
	roOpenResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID,
		Ino:       ino,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_READ_ONLY,
	})
	require.NoError(t, err)
	roFh := roOpenResp.GetFileHandle().GetFh()

	// Attempt Write on read-only handle -> PermissionDenied
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        roFh,
		Offset:    0,
		Data:      []byte("should fail"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 2. Open write-only handle
	woOpenResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID,
		Ino:       ino,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_WRITE_ONLY,
	})
	require.NoError(t, err)
	woFh := woOpenResp.GetFileHandle().GetFh()

	// Write succeeds on write-only handle
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        woFh,
		Offset:    0,
		Data:      []byte("write ok"),
	})
	require.NoError(t, err)

	// Attempt Read on write-only handle -> PermissionDenied
	_, err = h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       ino,
		Fh:        woFh,
		Offset:    0,
		Size:      10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

func TestStorageService_SessionMismatch(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	m1, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-sess-1",
		ReadOnly: false,
	})
	require.NoError(t, err)

	m2, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-sess-2",
		ReadOnly: true,
	})
	require.NoError(t, err)

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: m1.GetSessionId(),
		ParentIno: 1,
		Name:      "session_test.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Use m2 session to read/write/release handle created by m1 -> PermissionDenied
	_, err = h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: m2.GetSessionId(),
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Size:      10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: m2.GetSessionId(),
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      []byte("mismatch"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: m2.GetSessionId(),
		Ino:       ino,
		Fh:        fh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}
