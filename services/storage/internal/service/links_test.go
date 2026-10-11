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

func TestStorageService_Link_SuccessAndSharedInode(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-link-1"

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

	// 1. Create source file
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "original.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	srcIno := createResp.GetInode().GetIno()
	assert.Equal(t, uint32(1), createResp.GetInode().GetNlink())

	// 2. Create hard link in same directory
	linkResp, err := h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       srcIno,
		NewParentIno: 1,
		NewName:      "hardlink1.txt",
	})
	require.NoError(t, err)
	require.NotNil(t, linkResp)
	assert.Equal(t, srcIno, linkResp.GetInode().GetIno())
	assert.Equal(t, uint32(2), linkResp.GetInode().GetNlink())
	assert.Equal(t, int64(1), linkResp.GetEntryTimeout().GetSeconds())

	// 3. Verify both entries lookup the same Inode with nlink = 2
	lookup1, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "original.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, srcIno, lookup1.GetInode().GetIno())
	assert.Equal(t, uint32(2), lookup1.GetInode().GetNlink())

	lookup2, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "hardlink1.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, srcIno, lookup2.GetInode().GetIno())
	assert.Equal(t, uint32(2), lookup2.GetInode().GetNlink())

	// 4. Create directory and hard link into that subdirectory
	mkdirResp, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "sub",
		Mode:      0o755,
	})
	require.NoError(t, err)
	subDirIno := mkdirResp.GetInode().GetIno()

	linkSubResp, err := h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       srcIno,
		NewParentIno: subDirIno,
		NewName:      "sub_hardlink.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, srcIno, linkSubResp.GetInode().GetIno())
	assert.Equal(t, uint32(3), linkSubResp.GetInode().GetNlink())

	// Lookup all 3 entries: nlink must be 3
	lookupSub, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: subDirIno,
		Name:      "sub_hardlink.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, srcIno, lookupSub.GetInode().GetIno())
	assert.Equal(t, uint32(3), lookupSub.GetInode().GetNlink())

	// 5. Unlink original.txt: nlink should drop to 2, hardlink1 and sub_hardlink remain
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "original.txt",
	})
	require.NoError(t, err)

	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "original.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	lookup1After, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "hardlink1.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), lookup1After.GetInode().GetNlink())

	// 6. Unlink hardlink1.txt: nlink drops to 1, sub_hardlink remains
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "hardlink1.txt",
	})
	require.NoError(t, err)

	lookupSubAfter, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: subDirIno,
		Name:      "sub_hardlink.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(1), lookupSubAfter.GetInode().GetNlink())

	// 7. Unlink last reference: inode is unlinked and after forgetting references is fully deleted
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: subDirIno,
		Name:      "sub_hardlink.txt",
	})
	require.NoError(t, err)

	// Release kernel lookup references (FUSE lifecycle per STARSRC-95)
	_, _ = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       srcIno,
		Nlookup:   100,
	})

	_, err = h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       srcIno,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_Link_ValidationAndErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-link-val"

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
		Name:      "file.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	fileIno := createResp.GetInode().GetIno()

	mkdirResp, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dir",
		Mode:      0o755,
	})
	require.NoError(t, err)
	dirIno := mkdirResp.GetInode().GetIno()

	// 1. Empty session_id -> InvalidArgument
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    "",
		SrcIno:       fileIno,
		NewParentIno: 1,
		NewName:      "link1",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. SrcIno = 0 -> InvalidArgument
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       0,
		NewParentIno: 1,
		NewName:      "link1",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. NewParentIno = 0 -> InvalidArgument
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       fileIno,
		NewParentIno: 0,
		NewName:      "link1",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 4. Invalid name -> InvalidArgument
	for _, badName := range []string{"", ".", "..", "foo/bar"} {
		_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
			SessionId:    sessionID,
			SrcIno:       fileIno,
			NewParentIno: 1,
			NewName:      badName,
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}

	// 5. Source Inode not found -> NotFound
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       99999,
		NewParentIno: 1,
		NewName:      "link_missing",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 6. Hardlinking directory must be rejected -> InvalidArgument
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       dirIno,
		NewParentIno: 1,
		NewName:      "dir_hardlink",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 7. New parent not found -> NotFound
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       fileIno,
		NewParentIno: 88888,
		NewName:      "link_parent_missing",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 8. New parent is not a directory -> FailedPrecondition
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       fileIno,
		NewParentIno: fileIno,
		NewName:      "link_into_file",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 9. Target name already exists under new parent -> AlreadyExists
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       fileIno,
		NewParentIno: 1,
		NewName:      "file.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

func TestStorageService_Symlink_And_Readlink_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-symlink-1"

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

	targetPath := "/var/log/app/output.log"

	// 1. Create symlink
	symResp, err := h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "log_symlink",
		Target:    targetPath,
	})
	require.NoError(t, err)
	require.NotNil(t, symResp)

	inode := symResp.GetInode()
	require.NotNil(t, inode)
	assert.Greater(t, inode.GetIno(), uint64(1))
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_SYMLINK, inode.GetType())
	assert.Equal(t, uint32(0o777), inode.GetMode())
	assert.Equal(t, uint32(1), inode.GetNlink())
	assert.Equal(t, uint64(len(targetPath)), inode.GetSize())
	assert.Equal(t, targetPath, inode.GetTarget())
	assert.Equal(t, int64(1), symResp.GetEntryTimeout().GetSeconds())

	// 2. Readlink
	readResp, err := h.storageClient.Readlink(ctx, &bowlv1.ReadlinkRequest{
		SessionId: sessionID,
		Ino:       inode.GetIno(),
	})
	require.NoError(t, err)
	require.NotNil(t, readResp)
	assert.Equal(t, targetPath, readResp.GetTarget())

	// 3. Lookup symlink
	lookupResp, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "log_symlink",
	})
	require.NoError(t, err)
	assert.Equal(t, inode.GetIno(), lookupResp.GetInode().GetIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_SYMLINK, lookupResp.GetInode().GetType())
	assert.Equal(t, targetPath, lookupResp.GetInode().GetTarget())

	// 4. Duplicate symlink name -> AlreadyExists
	_, err = h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "log_symlink",
		Target:    "/other/path",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))

	// 5. Readlink on non-existent ino -> NotFound
	_, err = h.storageClient.Readlink(ctx, &bowlv1.ReadlinkRequest{
		SessionId: sessionID,
		Ino:       99999,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 6. Readlink on a regular file -> InvalidArgument
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "plain.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	_, err = h.storageClient.Readlink(ctx, &bowlv1.ReadlinkRequest{
		SessionId: sessionID,
		Ino:       createResp.GetInode().GetIno(),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestStorageService_Symlink_ValidationAndErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-symlink-val"

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

	// 1. Empty session_id -> InvalidArgument
	_, err = h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId: "",
		ParentIno: 1,
		Name:      "sym",
		Target:    "dest",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. ParentIno = 0 -> InvalidArgument
	_, err = h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId: sessionID,
		ParentIno: 0,
		Name:      "sym",
		Target:    "dest",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. Invalid name -> InvalidArgument
	for _, bad := range []string{"", ".", "..", "a/b"} {
		_, err = h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
			SessionId: sessionID,
			ParentIno: 1,
			Name:      bad,
			Target:    "dest",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}

	// 4. Empty target -> InvalidArgument
	_, err = h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "sym",
		Target:    "",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 5. Parent not found -> NotFound
	_, err = h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId: sessionID,
		ParentIno: 99999,
		Name:      "sym",
		Target:    "dest",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 6. Readlink validation: empty session_id or ino = 0
	_, err = h.storageClient.Readlink(ctx, &bowlv1.ReadlinkRequest{
		SessionId: "",
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.Readlink(ctx, &bowlv1.ReadlinkRequest{
		SessionId: sessionID,
		Ino:       0,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestStorageService_Rename_SameDirectory_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-rename-same"

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

	// 1. Create file
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "old_name.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	fileIno := createResp.GetInode().GetIno()

	// 2. Rename in same parent
	renameResp, err := h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "old_name.txt",
		ParentDst: 1,
		NameDst:   "new_name.txt",
		Flags:     0,
	})
	require.NoError(t, err)
	require.NotNil(t, renameResp)

	// 3. Lookup old should return NotFound
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "old_name.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 4. Lookup new should return original ino
	lookupNew, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "new_name.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, fileIno, lookupNew.GetInode().GetIno())

	// 5. Rename to itself is idempotent success
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "new_name.txt",
		ParentDst: 1,
		NameDst:   "new_name.txt",
		Flags:     0,
	})
	require.NoError(t, err)
}

func TestStorageService_Rename_AcrossDirectories_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-rename-across"

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

	// Create dir1, dir2
	d1Resp, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dir1",
		Mode:      0o755,
	})
	require.NoError(t, err)
	dir1Ino := d1Resp.GetInode().GetIno()

	d2Resp, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dir2",
		Mode:      0o755,
	})
	require.NoError(t, err)
	dir2Ino := d2Resp.GetInode().GetIno()

	// Create file in dir1
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: dir1Ino,
		Name:      "file.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	fileIno := createResp.GetInode().GetIno()

	// Move file from dir1 to dir2
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: dir1Ino,
		NameSrc:   "file.txt",
		ParentDst: dir2Ino,
		NameDst:   "moved.txt",
		Flags:     0,
	})
	require.NoError(t, err)

	// Old location not found
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: dir1Ino,
		Name:      "file.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// New location found
	lookupResp, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: dir2Ino,
		Name:      "moved.txt",
	})
	require.NoError(t, err)
	assert.Equal(t, fileIno, lookupResp.GetInode().GetIno())
}

func TestStorageService_Rename_Directory_AcrossDirectories_NlinkUpdated(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-rename-dir"

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

	d1, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "d1",
		Mode:      0o755,
	})
	require.NoError(t, err)
	d1Ino := d1.GetInode().GetIno()

	d2, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "d2",
		Mode:      0o755,
	})
	require.NoError(t, err)
	d2Ino := d2.GetInode().GetIno()

	sub, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: d1Ino,
		Name:      "sub",
		Mode:      0o755,
	})
	require.NoError(t, err)
	subIno := sub.GetInode().GetIno()

	// d1 should have nlink = 3 (root, ., sub)
	d1AttrBefore, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       d1Ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(3), d1AttrBefore.GetInode().GetNlink())

	// d2 should have nlink = 2 (root, .)
	d2AttrBefore, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       d2Ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), d2AttrBefore.GetInode().GetNlink())

	// Move d1/sub to d2/sub
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: d1Ino,
		NameSrc:   "sub",
		ParentDst: d2Ino,
		NameDst:   "sub",
		Flags:     0,
	})
	require.NoError(t, err)

	// Verify nlink adjustments: d1 nlink should decrease to 2, d2 nlink should increase to 3
	d1AttrAfter, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       d1Ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), d1AttrAfter.GetInode().GetNlink())

	d2AttrAfter, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       d2Ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(3), d2AttrAfter.GetInode().GetNlink())

	// Verify sub exists under d2
	lookupSub, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: d2Ino,
		Name:      "sub",
	})
	require.NoError(t, err)
	assert.Equal(t, subIno, lookupSub.GetInode().GetIno())
}

func TestStorageService_Rename_Overwrite_And_NoReplace(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-rename-overwrite"

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

	srcFile, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "src.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	srcIno := srcFile.GetInode().GetIno()

	dstFile, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dst.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	dstIno := dstFile.GetInode().GetIno()

	// 1. With RENAME_NOREPLACE (1): destination exists -> CodeAlreadyExists
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "src.txt",
		ParentDst: 1,
		NameDst:   "dst.txt",
		Flags:     uint32(bowlv1.RenameFlag_RENAME_FLAG_NO_REPLACE),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))

	// Both files still intact
	lSrc, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{SessionId: sessionID, ParentIno: 1, Name: "src.txt"})
	require.NoError(t, err)
	assert.Equal(t, srcIno, lSrc.GetInode().GetIno())

	lDst, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{SessionId: sessionID, ParentIno: 1, Name: "dst.txt"})
	require.NoError(t, err)
	assert.Equal(t, dstIno, lDst.GetInode().GetIno())

	// 2. Normal overwrite rename (flags = 0)
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "src.txt",
		ParentDst: 1,
		NameDst:   "dst.txt",
		Flags:     0,
	})
	require.NoError(t, err)

	// src.txt is gone
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{SessionId: sessionID, ParentIno: 1, Name: "src.txt"})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// dst.txt is now pointing to srcIno
	lDstAfter, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{SessionId: sessionID, ParentIno: 1, Name: "dst.txt"})
	require.NoError(t, err)
	assert.Equal(t, srcIno, lDstAfter.GetInode().GetIno())

	// Old dstIno is deleted
	_, err = h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{SessionId: sessionID, Ino: dstIno})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_Rename_Exchange(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-rename-exchange"

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

	fileA, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "alpha.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	inoA := fileA.GetInode().GetIno()

	fileB, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "beta.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	inoB := fileB.GetInode().GetIno()

	// 1. RENAME_EXCHANGE with missing destination -> CodeNotFound
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "alpha.txt",
		ParentDst: 1,
		NameDst:   "nonexistent.txt",
		Flags:     uint32(bowlv1.RenameFlag_RENAME_FLAG_EXCHANGE),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 2. Atomic exchange between alpha and beta
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "alpha.txt",
		ParentDst: 1,
		NameDst:   "beta.txt",
		Flags:     uint32(bowlv1.RenameFlag_RENAME_FLAG_EXCHANGE),
	})
	require.NoError(t, err)

	// alpha.txt must now have inoB
	lookupA, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{SessionId: sessionID, ParentIno: 1, Name: "alpha.txt"})
	require.NoError(t, err)
	assert.Equal(t, inoB, lookupA.GetInode().GetIno())

	// beta.txt must now have inoA
	lookupB, err := h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{SessionId: sessionID, ParentIno: 1, Name: "beta.txt"})
	require.NoError(t, err)
	assert.Equal(t, inoA, lookupB.GetInode().GetIno())
}

func TestStorageService_Rename_ValidationAndErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-rename-errs"

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

	f, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "f.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)

	d, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "d",
		Mode:      0o755,
	})
	require.NoError(t, err)

	// 1. Missing session_id -> InvalidArgument
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: "",
		ParentSrc: 1,
		NameSrc:   "f.txt",
		ParentDst: 1,
		NameDst:   "f2.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. ParentSrc = 0 -> InvalidArgument
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 0,
		NameSrc:   "f.txt",
		ParentDst: 1,
		NameDst:   "f2.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. ParentDst = 0 -> InvalidArgument
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "f.txt",
		ParentDst: 0,
		NameDst:   "f2.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 4. Invalid NameSrc -> InvalidArgument
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "..",
		ParentDst: 1,
		NameDst:   "f2.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 5. Source entry does not exist -> NotFound
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "nonexistent.txt",
		ParentDst: 1,
		NameDst:   "target.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 6. Overwrite directory with file -> InvalidArgument
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "f.txt",
		ParentDst: 1,
		NameDst:   "d",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 7. Overwrite file with directory -> InvalidArgument
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "d",
		ParentDst: 1,
		NameDst:   "f.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 8. Cannot move directory into a subdirectory of itself -> InvalidArgument
	sub, err := h.storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: d.GetInode().GetIno(),
		Name:      "sub",
		Mode:      0o755,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "d",
		ParentDst: sub.GetInode().GetIno(),
		NameDst:   "loop",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	_ = f
}

func TestStorageService_Links_WriteLeaseProtection_ReadOnlySession(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-ro-links"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	// 1. Mount read-write to seed data
	rwMount, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-rw-seed",
		ReadOnly: false,
	})
	require.NoError(t, err)
	rwSession := rwMount.GetSessionId()

	cf, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: rwSession,
		ParentIno: 1,
		Name:      "seed.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	seedIno := cf.GetInode().GetIno()

	sym, err := h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId: rwSession,
		ParentIno: 1,
		Name:      "seed_sym",
		Target:    "/target/path",
	})
	require.NoError(t, err)
	symIno := sym.GetInode().GetIno()

	// 2. Mount read-only session
	roMount, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: true,
	})
	require.NoError(t, err)
	roSession := roMount.GetSessionId()

	// Link rejected on read-only session
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    roSession,
		SrcIno:       seedIno,
		NewParentIno: 1,
		NewName:      "link_ro",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// Symlink rejected on read-only session
	_, err = h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId:    roSession,
		ParentIno:    1,
		Name:         "sym_ro",
		Target:       "/target",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// Rename rejected on read-only session
	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: roSession,
		ParentSrc: 1,
		NameSrc:   "seed.txt",
		ParentDst: 1,
		NameDst:   "seed_moved.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// Readlink is a read operation -> SUCCEEDS on read-only session!
	readlinkResp, err := h.storageClient.Readlink(ctx, &bowlv1.ReadlinkRequest{
		SessionId: roSession,
		Ino:       symIno,
	})
	require.NoError(t, err)
	assert.Equal(t, "/target/path", readlinkResp.GetTarget())
}

func TestStorageService_Links_WriteLeaseProtection_ExpiredSession(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-expired-links"

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

	cf, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "f.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	fIno := cf.GetInode().GetIno()

	// Expire session
	sess, found := h.sessionMgr.Get(sessionID)
	require.True(t, found)
	sess.IsWriteLease = false
	sess.Revoked = true
	sess.LeaseExpiresAt = time.Now().Add(-1 * time.Minute)
	h.sessionMgr.Create(sess)

	// All mutating link operations return CodePermissionDenied
	_, err = h.storageClient.Link(ctx, &bowlv1.LinkRequest{
		SessionId:    sessionID,
		SrcIno:       fIno,
		NewParentIno: 1,
		NewName:      "link_exp",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.Symlink(ctx, &bowlv1.SymlinkRequest{
		SessionId:    sessionID,
		ParentIno:    1,
		Name:         "sym_exp",
		Target:       "/target",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.Rename(ctx, &bowlv1.RenameRequest{
		SessionId: sessionID,
		ParentSrc: 1,
		NameSrc:   "f.txt",
		ParentDst: 1,
		NameDst:   "f_exp.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}
