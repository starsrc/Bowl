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

func TestStorageService_Create_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-create-1"

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
		Name:      "test.txt",
		Mode:      0o644,
		Umask:     0o022,
		Flags:     2, // O_RDWR
	})
	require.NoError(t, err)
	require.NotNil(t, createResp)

	// Check Inode
	inode := createResp.GetInode()
	require.NotNil(t, inode)
	assert.Greater(t, inode.GetIno(), uint64(1))
	assert.Equal(t, uint64(1), inode.GetParentIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_FILE, inode.GetType())
	assert.Equal(t, uint32(0o644), inode.GetMode())
	assert.Equal(t, uint32(1), inode.GetNlink())
	assert.Equal(t, uint64(0), inode.GetSize())
	assert.Equal(t, int64(1), inode.GetAttrTimeout().GetSeconds())
	assert.Equal(t, int64(1), createResp.GetEntryTimeout().GetSeconds())

	// Check FileHandle
	fh := createResp.GetFileHandle()
	require.NotNil(t, fh)
	assert.Greater(t, fh.GetFh(), uint64(0))
	assert.Equal(t, inode.GetIno(), fh.GetIno())
	assert.Equal(t, bowlv1.OpenMode_OPEN_MODE_READ_WRITE, fh.GetOpenMode())
	assert.Equal(t, clientID, fh.GetClientId())
	assert.True(t, fh.GetIsWriteLease())

	// 2. Lookup the newly created file
	lookupResp, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, inode.GetIno(), lookupResp.GetInode().GetIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_FILE, lookupResp.GetInode().GetType())

	// 3. Duplicate creation must return CodeAlreadyExists
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test.txt",
		Mode:      0o644,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

func TestStorageService_Mkdir_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-mkdir-1"

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

	// Query initial root nlink
	rootAttr, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	initialRootNlink := rootAttr.GetInode().GetNlink()

	// 1. Mkdir "docs" under root
	mkdirResp, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "docs",
		Mode:      0o755,
		Umask:     0o022,
	})
	require.NoError(t, err)
	require.NotNil(t, mkdirResp)

	newDir := mkdirResp.GetInode()
	require.NotNil(t, newDir)
	assert.Greater(t, newDir.GetIno(), uint64(1))
	assert.Equal(t, uint64(1), newDir.GetParentIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_DIRECTORY, newDir.GetType())
	assert.Equal(t, uint32(0o755), newDir.GetMode())
	assert.Equal(t, uint32(2), newDir.GetNlink())
	assert.Equal(t, uint64(4096), newDir.GetSize())
	assert.Equal(t, int64(1), mkdirResp.GetEntryTimeout().GetSeconds())

	// 2. Parent directory nlink must be incremented by 1
	rootAttrAfter, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	assert.Equal(t, initialRootNlink+1, rootAttrAfter.GetInode().GetNlink())

	// 3. Duplicate mkdir must return CodeAlreadyExists
	_, err = h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "docs",
		Mode:      0o755,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

func TestStorageService_Rmdir_SuccessAndNonEmpty(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-rmdir-1"

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

	// 1. Create a directory "testdir"
	mkdirResp, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "testdir",
		Mode:      0o755,
	})
	require.NoError(t, err)
	dirIno := mkdirResp.GetInode().GetIno()

	// 2. Create a child file inside "testdir"
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: dirIno,
		Name:      "child.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)

	// 3. Attempt to Rmdir "testdir" when non-empty -> CodeFailedPrecondition
	_, err = h.storageClient.Rmdir(ctx, &bowlv1.RmdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "testdir",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 4. Unlink child file
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: dirIno,
		Name:      "child.txt",
	})
	require.NoError(t, err)

	// 5. Rmdir "testdir" should now succeed
	rmdirResp, err := h.storageClient.Rmdir(ctx, &bowlv1.RmdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "testdir",
	})
	require.NoError(t, err)
	require.NotNil(t, rmdirResp)

	// 6. Lookup "testdir" must return CodeNotFound
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "testdir",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 7. Verify parent (root) nlink is back to 2
	rootAttr, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), rootAttr.GetInode().GetNlink())

	// 8. Rmdir on non-existent directory -> CodeNotFound
	_, err = h.storageClient.Rmdir(ctx, &bowlv1.RmdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "nonexistent",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_Unlink_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-unlink-1"

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

	// 1. Create file to unlink
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "to_unlink.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)

	// 2. Unlink the file
	unlinkResp, err := h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "to_unlink.txt",
	})
	require.NoError(t, err)
	require.NotNil(t, unlinkResp)

	// 3. Lookup should return CodeNotFound
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "to_unlink.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 4. Unlink non-existent file -> CodeNotFound
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "to_unlink.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 5. Unlink a directory -> CodeInvalidArgument
	_, err = h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "somedir",
		Mode:      0o755,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "somedir",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestStorageService_Mknod_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-mknod-1"

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

	// 1. Create FIFO node
	mknodResp, err := h.storageClient.Mknod(ctx, &bowlv1.MknodRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test_fifo",
		Type:      bowlv1.InodeType_INODE_TYPE_FIFO,
		Mode:      0o644,
	})
	require.NoError(t, err)
	require.NotNil(t, mknodResp)
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_FIFO, mknodResp.GetInode().GetType())
	assert.Equal(t, uint32(0o644), mknodResp.GetInode().GetMode())
	assert.Equal(t, uint32(1), mknodResp.GetInode().GetNlink())

	// 2. Create Character Device node with rdev
	chrResp, err := h.storageClient.Mknod(ctx, &bowlv1.MknodRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test_chr",
		Type:      bowlv1.InodeType_INODE_TYPE_CHAR_DEV,
		Mode:      0o660,
		Rdev:      12345,
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_CHAR_DEV, chrResp.GetInode().GetType())
	assert.Equal(t, uint32(12345), chrResp.GetInode().GetRdev())

	// 3. Create Block Device node with rdev
	blkResp, err := h.storageClient.Mknod(ctx, &bowlv1.MknodRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test_blk",
		Type:      bowlv1.InodeType_INODE_TYPE_BLOCK_DEV,
		Mode:      0o660,
		Rdev:      67890,
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_BLOCK_DEV, blkResp.GetInode().GetType())
	assert.Equal(t, uint32(67890), blkResp.GetInode().GetRdev())

	// 4. Create Socket node
	sockResp, err := h.storageClient.Mknod(ctx, &bowlv1.MknodRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test_sock",
		Type:      bowlv1.InodeType_INODE_TYPE_SOCKET,
		Mode:      0o777,
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_SOCKET, sockResp.GetInode().GetType())

	// 5. Duplicate mknod name -> CodeAlreadyExists
	_, err = h.storageClient.Mknod(ctx, &bowlv1.MknodRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test_fifo",
		Type:      bowlv1.InodeType_INODE_TYPE_FIFO,
		Mode:      0o644,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

func TestStorageService_Namespace_WriteLeaseProtection_ReadOnlySession(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-ro-1"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: true,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 1. Create on read-only session must return CodePermissionDenied
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "file.txt",
		Mode:      0o644,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 2. Mkdir on read-only session must return CodePermissionDenied
	_, err = h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "folder",
		Mode:      0o755,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 3. Rmdir on read-only session must return CodePermissionDenied
	_, err = h.storageClient.Rmdir(ctx, &bowlv1.RmdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "folder",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 4. Unlink on read-only session must return CodePermissionDenied
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "file.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 5. Mknod on read-only session must return CodePermissionDenied
	_, err = h.storageClient.Mknod(ctx, &bowlv1.MknodRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "fifo",
		Type:      bowlv1.InodeType_INODE_TYPE_FIFO,
		Mode:      0o644,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

func TestStorageService_Namespace_WriteLeaseProtection_ExpiredSession(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-expired-1"

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

	// Expire the lease in session manager
	sess, found := h.sessionMgr.Get(sessionID)
	require.True(t, found)
	sess.IsWriteLease = false
	sess.Revoked = true
	sess.LeaseExpiresAt = time.Now().Add(-1 * time.Minute)
	h.sessionMgr.Create(sess)

	// All mutating operations must return CodePermissionDenied
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "file.txt",
		Mode:      0o644,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "folder",
		Mode:      0o755,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.Rmdir(ctx, &bowlv1.RmdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "folder",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "file.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.Mknod(ctx, &bowlv1.MknodRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "fifo",
		Type:      bowlv1.InodeType_INODE_TYPE_FIFO,
		Mode:      0o644,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

func TestStorageService_Namespace_ValidationErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-val-1"

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

	// 1. Empty session_id
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: "",
		ParentIno: 1,
		Name:      "file.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. ParentIno = 0
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 0,
		Name:      "file.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. Empty name
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 4. Invalid names ("." or "..")
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      ".",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "..",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 5. Slash in name
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "sub/file.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 6. Parent does not exist -> CodeNotFound
	_, err = h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 99999,
		Name:      "file.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
