package service

import (
	"context"
	"testing"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

func TestStorageService_OpenDir_ReadDir_Streaming(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-dir-stream"

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

	// 1. Create items in root directory: 3 files and 1 directory
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "alpha.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "beta.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "subdir",
		Mode:      0o755,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "gamma.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)

	// 2. OpenDir on root directory
	openResp, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Flags:     0,
	})
	require.NoError(t, err)
	require.NotNil(t, openResp)
	fh := openResp.GetFileHandle()
	require.NotNil(t, fh)
	assert.Greater(t, fh.GetFh(), uint64(0))
	assert.Equal(t, uint64(1), fh.GetIno())
	assert.Equal(t, clientID, fh.GetClientId())

	dirFH := fh.GetFh()

	// Total entries expected: "." (offset 1), ".." (offset 2), and 4 children (offsets 3, 4, 5, 6) = 6 entries.

	// 3. Batch 1: offset 0, limit 2
	b1, err := h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dirFH,
		Offset:    0,
		Limit:     2,
	})
	require.NoError(t, err)
	require.Len(t, b1.GetEntries(), 2)

	assert.Equal(t, ".", b1.GetEntries()[0].GetName())
	assert.Equal(t, uint64(1), b1.GetEntries()[0].GetIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_DIRECTORY, b1.GetEntries()[0].GetType())
	assert.Equal(t, uint64(1), b1.GetEntries()[0].GetOffset())

	assert.Equal(t, "..", b1.GetEntries()[1].GetName())
	assert.Equal(t, uint64(1), b1.GetEntries()[1].GetIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_DIRECTORY, b1.GetEntries()[1].GetType())
	assert.Equal(t, uint64(2), b1.GetEntries()[1].GetOffset())

	// 4. Batch 2: offset 2, limit 2
	b2, err := h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dirFH,
		Offset:    2,
		Limit:     2,
	})
	require.NoError(t, err)
	require.Len(t, b2.GetEntries(), 2)
	assert.Equal(t, uint64(3), b2.GetEntries()[0].GetOffset())
	assert.Equal(t, uint64(4), b2.GetEntries()[1].GetOffset())

	// 5. Batch 3: offset 4, limit 2
	b3, err := h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dirFH,
		Offset:    4,
		Limit:     2,
	})
	require.NoError(t, err)
	require.Len(t, b3.GetEntries(), 2)
	assert.Equal(t, uint64(5), b3.GetEntries()[0].GetOffset())
	assert.Equal(t, uint64(6), b3.GetEntries()[1].GetOffset())

	// 6. Batch 4: offset 6, limit 2 (end of directory stream)
	b4, err := h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dirFH,
		Offset:    6,
		Limit:     2,
	})
	require.NoError(t, err)
	assert.Empty(t, b4.GetEntries())

	// 7. Full stream verification (offset 0, limit 100)
	fullResp, err := h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dirFH,
		Offset:    0,
		Limit:     100,
	})
	require.NoError(t, err)
	require.Len(t, fullResp.GetEntries(), 6)

	expectedNames := []string{".", "..", "alpha.txt", "beta.txt", "subdir", "gamma.txt"}
	receivedNames := make([]string, len(fullResp.GetEntries()))
	for i, entry := range fullResp.GetEntries() {
		receivedNames[i] = entry.GetName()
		assert.Equal(t, uint64(i+1), entry.GetOffset())
	}
	assert.ElementsMatch(t, expectedNames, receivedNames)
}

func TestStorageService_ReadDirPlus_FullMetadataAndTimeouts(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-dir-plus"

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

	// 1. Create a child directory and a file
	mkdirResp, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "testdir",
		Mode:      0o755,
	})
	require.NoError(t, err)
	childDirIno := mkdirResp.GetInode().GetIno()

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: childDirIno,
		Name:      "child.txt",
		Mode:      0o640,
	})
	require.NoError(t, err)
	childFileIno := createResp.GetInode().GetIno()

	// 2. Open childdir
	openResp, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       childDirIno,
	})
	require.NoError(t, err)
	dirFH := openResp.GetFileHandle().GetFh()

	// 3. ReadDirPlus on childdir
	plusResp, err := h.storageClient.ReadDirPlus(ctx, &bowlv1.ReadDirPlusRequest{
		SessionId: sessionID,
		Ino:       childDirIno,
		Fh:        dirFH,
		Offset:    0,
		Limit:     10,
	})
	require.NoError(t, err)
	require.NotNil(t, plusResp)
	entries := plusResp.GetEntries()
	require.Len(t, entries, 3) // ".", "..", "child.txt"

	// Verify entries and full Inode attributes + timeouts
	for _, entry := range entries {
		require.NotNil(t, entry.GetAttr(), "attr must be populated for ReadDirPlus")
		require.NotNil(t, entry.GetEntryTimeout(), "entry_timeout must be populated for ReadDirPlus")
		assert.Equal(t, int64(1), entry.GetEntryTimeout().GetSeconds())
		assert.Equal(t, int64(1), entry.GetAttr().GetAttrTimeout().GetSeconds())
		assert.Equal(t, entry.GetIno(), entry.GetAttr().GetIno())
		assert.Equal(t, entry.GetType(), entry.GetAttr().GetType())
	}

	// Verify "."
	dot := entries[0]
	assert.Equal(t, ".", dot.GetName())
	assert.Equal(t, childDirIno, dot.GetIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_DIRECTORY, dot.GetType())
	assert.Equal(t, uint64(1), dot.GetOffset())
	assert.Equal(t, uint64(1), dot.GetAttr().GetParentIno()) // parent is root

	// Verify ".."
	dotDot := entries[1]
	assert.Equal(t, "..", dotDot.GetName())
	assert.Equal(t, uint64(1), dotDot.GetIno()) // root inode is 1
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_DIRECTORY, dotDot.GetType())
	assert.Equal(t, uint64(2), dotDot.GetOffset())

	// Verify "child.txt"
	fileEntry := entries[2]
	assert.Equal(t, "child.txt", fileEntry.GetName())
	assert.Equal(t, childFileIno, fileEntry.GetIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_FILE, fileEntry.GetType())
	assert.Equal(t, uint64(3), fileEntry.GetOffset())
	assert.Equal(t, uint32(0o640), fileEntry.GetAttr().GetMode())
	assert.Equal(t, childDirIno, fileEntry.GetAttr().GetParentIno())
}

func TestStorageService_ReleaseDir_ClosesHandle(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-dir-release"

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

	// 1. OpenDir
	openResp, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	dh := openResp.GetFileHandle().GetFh()

	// 2. ReadDir succeeds while handle is open
	readResp, err := h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
		Offset:    0,
		Limit:     10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, readResp.GetEntries())

	// 3. ReleaseDir
	releaseResp, err := h.storageClient.ReleaseDir(ctx, &bowlv1.ReleaseDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
	})
	require.NoError(t, err)
	require.NotNil(t, releaseResp)

	// 4. Subsequent ReadDir with closed dh must fail with NotFound
	_, err = h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
		Offset:    0,
		Limit:     10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 5. Subsequent ReadDirPlus with closed dh must also fail with NotFound
	_, err = h.storageClient.ReadDirPlus(ctx, &bowlv1.ReadDirPlusRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
		Offset:    0,
		Limit:     10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 6. Second ReleaseDir on same dh must fail with NotFound
	_, err = h.storageClient.ReleaseDir(ctx, &bowlv1.ReleaseDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_FsyncDir_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-dir-fsync"

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

	// 1. OpenDir
	openResp, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	dh := openResp.GetFileHandle().GetFh()

	// 2. FsyncDir with datasync = false
	fsyncResp1, err := h.storageClient.FsyncDir(ctx, &bowlv1.FsyncDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
		Datasync:  false,
	})
	require.NoError(t, err)
	require.NotNil(t, fsyncResp1)

	// 3. FsyncDir with datasync = true
	fsyncResp2, err := h.storageClient.FsyncDir(ctx, &bowlv1.FsyncDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
		Datasync:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, fsyncResp2)

	// 4. FsyncDir with non-existent dh fails with NotFound
	_, err = h.storageClient.FsyncDir(ctx, &bowlv1.FsyncDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        999999,
		Datasync:  false,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_Dir_Validation_And_NotFound(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-dir-validation"

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

	// 1. OpenDir on non-existent Inode returns NotFound
	_, err = h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       999999,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 2. Create regular file, OpenDir on file returns FailedPrecondition
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "regular.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	fileIno := createResp.GetInode().GetIno()

	_, err = h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       fileIno,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 3. ReadDir with non-existent dh returns NotFound
	_, err = h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        999999,
		Offset:    0,
		Limit:     10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 4. Invalid session ID returns NotFound
	_, err = h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: "invalid-session-id",
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 5. OpenDir with ino 0 returns InvalidArgument
	_, err = h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       0,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestStorageService_Unmount_ClosesOrphanDirHandles(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-dir-unmount-orphan"

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

	openResp, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	dh := openResp.GetFileHandle().GetFh()

	// Unmount session
	_, err = h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: sessionID,
	})
	require.NoError(t, err)

	// Subsequent ReadDir with dh fails with NotFound
	_, err = h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
