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

func TestStorageService_XAttr_Roundtrip(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "xattr-client-roundtrip"
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

	// 1. Initial list of xattrs on root ino=1 should be empty
	listResp, err := h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	assert.Empty(t, listResp.GetNames())

	// 2. SetXAttr on root ino=1
	attr1Name := "user.comment"
	attr1Val := []byte("sample extended attribute value")
	setResp, err := h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attr1Name,
		Value:     attr1Val,
		Flags:     bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_UNSPECIFIED,
	})
	require.NoError(t, err)
	require.NotNil(t, setResp)

	// 3. GetXAttr on attr1
	getResp, err := h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attr1Name,
	})
	require.NoError(t, err)
	assert.Equal(t, attr1Val, getResp.GetValue())

	// 4. Set another xattr
	attr2Name := "user.security.level"
	attr2Val := []byte("high")
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attr2Name,
		Value:     attr2Val,
		Flags:     bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_UNSPECIFIED,
	})
	require.NoError(t, err)

	// 5. ListXAttr should return both attributes
	listResp, err = h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{attr1Name, attr2Name}, listResp.GetNames())

	// 6. Remove attr1
	removeResp, err := h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attr1Name,
	})
	require.NoError(t, err)
	require.NotNil(t, removeResp)

	// 7. GetXAttr on removed attr1 must return NotFound
	_, err = h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attr1Name,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 8. ListXAttr should now contain only attr2
	listResp, err = h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{attr2Name}, listResp.GetNames())

	// 9. Remove attr2
	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attr2Name,
	})
	require.NoError(t, err)

	// 10. ListXAttr should now be empty
	listResp, err = h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	assert.Empty(t, listResp.GetNames())

	// 11. Removing already removed attribute should return NotFound
	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attr2Name,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_SetXAttr_Flags(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "xattr-client-flags"
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

	attrName := "user.checksum"
	initialVal := []byte("sha256:abcd")

	// 1. XATTR_CREATE on non-existent attribute should succeed
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attrName,
		Value:     initialVal,
		Flags:     bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_CREATE,
	})
	require.NoError(t, err)

	// 2. XATTR_CREATE on already existing attribute should return AlreadyExists
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attrName,
		Value:     []byte("sha256:1111"),
		Flags:     bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_CREATE,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))

	// 3. XATTR_REPLACE on non-existent attribute should return NotFound
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      "user.nonexistent",
		Value:     []byte("value"),
		Flags:     bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_REPLACE,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 4. XATTR_REPLACE on existing attribute should succeed
	replacedVal := []byte("sha256:replaced-val")
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attrName,
		Value:     replacedVal,
		Flags:     bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_REPLACE,
	})
	require.NoError(t, err)

	// Verify updated value via GetXAttr
	getResp, err := h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attrName,
	})
	require.NoError(t, err)
	assert.Equal(t, replacedVal, getResp.GetValue())

	// 5. Default/unspecified flag should upsert on existing attribute
	upsertVal := []byte("sha256:upserted-val")
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attrName,
		Value:     upsertVal,
		Flags:     bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_UNSPECIFIED,
	})
	require.NoError(t, err)

	getResp, err = h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{
		SessionId: sessionID,
		Ino:       1,
		Name:      attrName,
	})
	require.NoError(t, err)
	assert.Equal(t, upsertVal, getResp.GetValue())
}

func TestStorageService_XAttr_ReadOnlySession_PermissionDenied(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "xattr-client-ro"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: true,
	})
	require.NoError(t, err)
	roSessionID := mountResp.GetSessionId()

	// 1. Read-only session cannot SetXAttr -> PermissionDenied
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: roSessionID,
		Ino:       1,
		Name:      "user.ro.test",
		Value:     []byte("ro-value"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 2. Read-only session cannot RemoveXAttr -> PermissionDenied
	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{
		SessionId: roSessionID,
		Ino:       1,
		Name:      "user.ro.test",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// 3. Read-only session CAN read attributes (ListXAttr and GetXAttr)
	listResp, err := h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{
		SessionId: roSessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	assert.Empty(t, listResp.GetNames())

	// 4. Test expired writer lease
	rwClientID := "xattr-client-expired"
	rwMountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: rwClientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	rwSessionID := rwMountResp.GetSessionId()

	sess, found := h.sessionMgr.Get(rwSessionID)
	require.True(t, found)
	sess.IsWriteLease = false
	sess.Revoked = true
	h.sessionMgr.Create(sess)

	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: rwSessionID,
		Ino:       1,
		Name:      "user.expired.test",
		Value:     []byte("test"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{
		SessionId: rwSessionID,
		Ino:       1,
		Name:      "user.expired.test",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

func TestStorageService_XAttr_NonExistentInode_NotFound(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "xattr-client-notfound-ino"
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

	nonExistentIno := uint64(999999)

	// 1. SetXAttr on non-existent Inode -> NotFound
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       nonExistentIno,
		Name:      "user.test",
		Value:     []byte("val"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 2. GetXAttr on non-existent Inode -> NotFound
	_, err = h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{
		SessionId: sessionID,
		Ino:       nonExistentIno,
		Name:      "user.test",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 3. ListXAttr on non-existent Inode -> NotFound
	_, err = h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{
		SessionId: sessionID,
		Ino:       nonExistentIno,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 4. RemoveXAttr on non-existent Inode -> NotFound
	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{
		SessionId: sessionID,
		Ino:       nonExistentIno,
		Name:      "user.test",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_XAttr_ValidationErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "xattr-client-val"
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

	// 1. Empty session_id -> InvalidArgument
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{SessionId: "", Ino: 1, Name: "k", Value: []byte("v")})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{SessionId: "", Ino: 1, Name: "k"})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{SessionId: "", Ino: 1})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{SessionId: "", Ino: 1, Name: "k"})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. Ino = 0 -> InvalidArgument
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{SessionId: sessionID, Ino: 0, Name: "k", Value: []byte("v")})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{SessionId: sessionID, Ino: 0, Name: "k"})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{SessionId: sessionID, Ino: 0})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{SessionId: sessionID, Ino: 0, Name: "k"})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. Empty name -> InvalidArgument
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{SessionId: sessionID, Ino: 1, Name: "", Value: []byte("v")})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{SessionId: sessionID, Ino: 1, Name: "  "})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{SessionId: sessionID, Ino: 1, Name: ""})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 4. Non-existent session_id -> NotFound
	unknownSessionID := uuid.New().String()
	_, err = h.storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{SessionId: unknownSessionID, Ino: 1, Name: "k", Value: []byte("v")})
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = h.storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{SessionId: unknownSessionID, Ino: 1, Name: "k"})
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = h.storageClient.ListXAttr(ctx, &bowlv1.ListXAttrRequest{SessionId: unknownSessionID, Ino: 1})
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = h.storageClient.RemoveXAttr(ctx, &bowlv1.RemoveXAttrRequest{SessionId: unknownSessionID, Ino: 1, Name: "k"})
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
