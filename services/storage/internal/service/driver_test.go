package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

// mockStorageDriver implements FileStorageDriver and records all invoked methods.
type mockStorageDriver struct {
	openCalled          bool
	readCalled          bool
	writeCalled         bool
	flushCalled         bool
	fsyncCalled         bool
	truncateCalled      bool
	fallocateCalled     bool
	copyFileRangeCalled bool
	lseekCalled         bool
	closeCalled         bool

	lastDiskID    string
	lastIno       uint64
	lastOffset    uint64
	lastWriteData []byte
	lastFsyncSync bool
}

func (m *mockStorageDriver) Open(ctx context.Context, diskID string, ino uint64, openMode bowlv1.OpenMode, flags uint32) (HandleContext, error) {
	m.openCalled = true
	m.lastDiskID = diskID
	m.lastIno = ino
	return "mock-handle-ctx", nil
}

func (m *mockStorageDriver) Read(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, size uint32) ([]byte, bool, error) {
	m.readCalled = true
	m.lastDiskID = diskID
	m.lastIno = ino
	m.lastOffset = offset
	return []byte("mock-data"), true, nil
}

func (m *mockStorageDriver) Write(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, data []byte) (uint32, error) {
	m.writeCalled = true
	m.lastDiskID = diskID
	m.lastIno = ino
	m.lastOffset = offset
	m.lastWriteData = data
	return uint32(len(data)), nil
}

func (m *mockStorageDriver) Flush(ctx context.Context, handle HandleContext, diskID string, ino uint64) error {
	m.flushCalled = true
	m.lastDiskID = diskID
	m.lastIno = ino
	return nil
}

func (m *mockStorageDriver) Fsync(ctx context.Context, handle HandleContext, diskID string, ino uint64, datasync bool) error {
	m.fsyncCalled = true
	m.lastDiskID = diskID
	m.lastIno = ino
	m.lastFsyncSync = datasync
	return nil
}

func (m *mockStorageDriver) Truncate(ctx context.Context, diskID string, ino uint64, size uint64) error {
	m.truncateCalled = true
	m.lastDiskID = diskID
	m.lastIno = ino
	return nil
}

func (m *mockStorageDriver) Fallocate(ctx context.Context, handle HandleContext, diskID string, ino uint64, mode uint32, offset uint64, length uint64) error {
	m.fallocateCalled = true
	m.lastDiskID = diskID
	m.lastIno = ino
	return nil
}

func (m *mockStorageDriver) CopyFileRange(ctx context.Context, srcHandle HandleContext, srcDiskID string, srcIno uint64, srcOffset uint64, dstHandle HandleContext, dstDiskID string, dstIno uint64, dstOffset uint64, length uint64) (uint64, error) {
	m.copyFileRangeCalled = true
	return length, nil
}

func (m *mockStorageDriver) Lseek(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, whence uint32) (uint64, error) {
	m.lseekCalled = true
	m.lastDiskID = diskID
	m.lastIno = ino
	return offset, nil
}

func (m *mockStorageDriver) Close(ctx context.Context, handle HandleContext) error {
	m.closeCalled = true
	return nil
}

func TestStorageService_FileStorageDriver_InterfaceAndDefault(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	// 1. Verify default driver is *LegacyFileDriver implementing FileStorageDriver
	require.NotNil(t, h.storageSvc.driver)
	var _ FileStorageDriver = h.storageSvc.driver
	_, isLegacy := h.storageSvc.driver.(*LegacyFileDriver)
	assert.True(t, isLegacy, "default driver must be *LegacyFileDriver")
}

func TestStorageService_FileStorageDriver_Delegation(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-driver-delegation"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	mockDriver := &mockStorageDriver{}
	// Reconfigure storageSvc with injected mock driver
	WithFileStorageDriver(mockDriver)(h.storageSvc)

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// Insert inode 2 for testing
	now := time.Now()
	_, err = h.dbPool.Exec(ctx, `
INSERT INTO "`+schemaName+`".inodes (ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime)
VALUES (2, 1, 'test.bin', 1, 420, 1, 0, 0, 100, 1, 0, $1, $1, $1);
`, now)
	require.NoError(t, err)

	// 1. Open -> driver.Open
	openResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID,
		Ino:       2,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_READ_WRITE,
	})
	require.NoError(t, err)
	require.True(t, mockDriver.openCalled)
	assert.Equal(t, diskID.String(), mockDriver.lastDiskID)
	assert.Equal(t, uint64(2), mockDriver.lastIno)
	fh := openResp.GetFileHandle().GetFh()

	// 2. Write -> driver.Write
	writePayload := []byte("custom-driver-write")
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       2,
		Fh:        fh,
		Offset:    10,
		Data:      writePayload,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(len(writePayload)), writeResp.GetWritten())
	assert.True(t, mockDriver.writeCalled)
	assert.Equal(t, uint64(10), mockDriver.lastOffset)
	assert.Equal(t, writePayload, mockDriver.lastWriteData)

	// 3. Read -> driver.Read
	readResp, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       2,
		Fh:        fh,
		Offset:    5,
		Size:      20,
	})
	require.NoError(t, err)
	assert.True(t, mockDriver.readCalled)
	assert.Equal(t, []byte("mock-data"), readResp.GetData())
	assert.True(t, readResp.GetEof())

	// 4. Flush -> driver.Flush
	flushResp, err := h.storageClient.Flush(ctx, &bowlv1.FlushRequest{
		SessionId: sessionID,
		Ino:       2,
		Fh:        fh,
	})
	require.NoError(t, err)
	require.NotNil(t, flushResp)
	assert.True(t, mockDriver.flushCalled)

	// 5. Fsync -> driver.Fsync
	fsyncResp, err := h.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Ino:       2,
		Fh:        fh,
		Datasync:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, fsyncResp)
	assert.True(t, mockDriver.fsyncCalled)
	assert.True(t, mockDriver.lastFsyncSync)

	// 6. Fallocate -> driver.Fallocate
	fallocateResp, err := h.storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: sessionID,
		Ino:       2,
		Fh:        fh,
		Mode:      0,
		Offset:    0,
		Length:    500,
	})
	require.NoError(t, err)
	require.NotNil(t, fallocateResp)
	assert.True(t, mockDriver.fallocateCalled)

	// 7. CopyFileRange -> driver.CopyFileRange
	copyResp, err := h.storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    2,
		SrcFh:     fh,
		SrcOffset: 0,
		DstIno:    2,
		DstFh:     fh,
		DstOffset: 200,
		Length:    50,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(50), copyResp.GetCopied())
	assert.True(t, mockDriver.copyFileRangeCalled)

	// 8. Lseek -> driver.Lseek
	lseekResp, err := h.storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Ino:       2,
		Fh:        fh,
		Offset:    15,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_DATA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(15), lseekResp.GetOffset())
	assert.True(t, mockDriver.lseekCalled)

	// 9. SetAttr size -> driver.Truncate
	setAttrResp, err := h.storageClient.SetAttr(ctx, &bowlv1.SetAttrRequest{
		SessionId: sessionID,
		Ino:       2,
		Valid:     uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_SIZE),
		Size:      1024,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1024), setAttrResp.GetInode().GetSize())
	assert.True(t, mockDriver.truncateCalled)

	// 10. Release -> driver.Close
	releaseResp, err := h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Ino:       2,
		Fh:        fh,
	})
	require.NoError(t, err)
	require.NotNil(t, releaseResp)
	assert.True(t, mockDriver.closeCalled)
}
