package service

import (
	"context"
	"fmt"
	"testing"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

func TestStorageService_Forget_DecrementsLookupCount(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "forget-client-1"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schemaName))
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 1. Initial root inode (ino=1) lookup count is 1
	var rootLookupCount int64
	queryRoot := fmt.Sprintf(`SELECT lookup_count FROM "%s".inodes WHERE ino = 1;`, schemaName)
	err = h.dbPool.QueryRow(ctx, queryRoot).Scan(&rootLookupCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rootLookupCount)

	// 2. Perform Lookup on "." twice -> each lookup increments lookup_count by 1
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      ".",
	})
	require.NoError(t, err)

	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      ".",
	})
	require.NoError(t, err)

	err = h.dbPool.QueryRow(ctx, queryRoot).Scan(&rootLookupCount)
	require.NoError(t, err)
	assert.Equal(t, int64(3), rootLookupCount)

	// 3. Forget 1 lookup reference -> lookup_count decreases to 2
	forgetResp, err := h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       1,
		Nlookup:   1,
	})
	require.NoError(t, err)
	require.NotNil(t, forgetResp)

	err = h.dbPool.QueryRow(ctx, queryRoot).Scan(&rootLookupCount)
	require.NoError(t, err)
	assert.Equal(t, int64(2), rootLookupCount)

	// 4. Forget 2 lookup references -> lookup_count decreases to 0
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       1,
		Nlookup:   2,
	})
	require.NoError(t, err)

	err = h.dbPool.QueryRow(ctx, queryRoot).Scan(&rootLookupCount)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rootLookupCount)

	// 5. Root inode has nlink=2 > 0, so it must NOT be deleted even when lookup_count is 0
	getAttrResp, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), getAttrResp.GetInode().GetIno())

	// 6. Forget with nlookup > current count -> floored at 0 (max(0, 0-5) = 0)
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       1,
		Nlookup:   5,
	})
	require.NoError(t, err)

	err = h.dbPool.QueryRow(ctx, queryRoot).Scan(&rootLookupCount)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rootLookupCount)
}

func TestStorageService_BatchForget_DecrementsAcrossMultipleInodes(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "batch-forget-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schemaName))
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 1. Create 3 files: f1, f2, f3 (each initial lookup_count = 1)
	create1, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "f1.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	ino1 := create1.GetInode().GetIno()

	create2, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "f2.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	ino2 := create2.GetInode().GetIno()

	create3, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "f3.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	ino3 := create3.GetInode().GetIno()

	// 2. Lookup f1 twice -> lookup_count = 1 + 2 = 3
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "f1.txt",
	})
	require.NoError(t, err)
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "f1.txt",
	})
	require.NoError(t, err)

	// 3. Lookup f2 once -> lookup_count = 1 + 1 = 2
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "f2.txt",
	})
	require.NoError(t, err)

	// 4. BatchForget:
	// - f1: nlookup = 2 -> expected 1
	// - f2: nlookup = 1 -> expected 1
	// - f3: nlookup = 1 -> expected 0
	batchResp, err := h.storageClient.BatchForget(ctx, &bowlv1.BatchForgetRequest{
		SessionId: sessionID,
		Nodes: []*bowlv1.ForgetNode{
			{Ino: ino1, Nlookup: 2},
			{Ino: ino2, Nlookup: 1},
			{Ino: ino3, Nlookup: 1},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, batchResp)

	// 5. Verify counts in DB
	var c1, c2, c3 int64
	queryIno := fmt.Sprintf(`SELECT lookup_count FROM "%s".inodes WHERE ino = $1;`, schemaName)
	require.NoError(t, h.dbPool.QueryRow(ctx, queryIno, int64(ino1)).Scan(&c1))
	require.NoError(t, h.dbPool.QueryRow(ctx, queryIno, int64(ino2)).Scan(&c2))
	require.NoError(t, h.dbPool.QueryRow(ctx, queryIno, int64(ino3)).Scan(&c3))

	assert.Equal(t, int64(1), c1)
	assert.Equal(t, int64(1), c2)
	assert.Equal(t, int64(0), c3)

	// 6. All files retain nlink=1 > 0, so none are deleted
	for _, ino := range []uint64{ino1, ino2, ino3} {
		attr, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
			SessionId: sessionID,
			Ino:       ino,
		})
		require.NoError(t, err)
		assert.Equal(t, ino, attr.GetInode().GetIno())
	}
}

func TestStorageService_Forget_ReclaimUnlinkedInode(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "reclaim-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schemaName))
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 1. Create file (lookup_count = 1, nlink = 1)
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "to_reclaim.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()

	// 2. Lookup file once -> lookup_count = 2
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "to_reclaim.txt",
	})
	require.NoError(t, err)

	// 3. Unlink file:
	// Since lookup_count is 2 > 0, inode is unlinked (nlink=0), but not yet deleted!
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "to_reclaim.txt",
	})
	require.NoError(t, err)

	// Directory entry is gone: Lookup returns CodeNotFound
	_, err = h.storageClient.Lookup(ctx, &bowlv1.LookupRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "to_reclaim.txt",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Inode metadata still accessible by ino: nlink is 0, lookup_count is 2
	attrResp, err := h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(0), attrResp.GetInode().GetNlink())

	// 4. Forget 1 lookup reference -> lookup_count = 1 > 0 -> still NOT reclaimed
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       ino,
		Nlookup:   1,
	})
	require.NoError(t, err)

	attrResp, err = h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	assert.Equal(t, ino, attrResp.GetInode().GetIno())

	// 5. Forget remaining lookup reference -> lookup_count reaches 0 with nlink == 0
	// Inode must be reclaimed and deleted!
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       ino,
		Nlookup:   1,
	})
	require.NoError(t, err)

	// Inode is gone: GetAttr returns CodeNotFound
	_, err = h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 6. Test BatchForget inode reclamation on another unlinked node
	createBatch, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "batch_reclaim.txt",
		Mode:      0o644,
	})
	require.NoError(t, err)
	batchIno := createBatch.GetInode().GetIno()

	// Unlink with initial lookup_count = 1
	_, err = h.storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "batch_reclaim.txt",
	})
	require.NoError(t, err)

	// BatchForget with nlookup = 1
	_, err = h.storageClient.BatchForget(ctx, &bowlv1.BatchForgetRequest{
		SessionId: sessionID,
		Nodes: []*bowlv1.ForgetNode{
			{Ino: batchIno, Nlookup: 1},
		},
	})
	require.NoError(t, err)

	// Verify batchIno is deleted
	_, err = h.storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       batchIno,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_Forget_ValidationErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "val-client"
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schemaName))
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 1. Forget: Empty session_id -> CodeInvalidArgument
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: "",
		Ino:       1,
		Nlookup:   1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. Forget: Whitespace session_id -> CodeInvalidArgument
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: "   ",
		Ino:       1,
		Nlookup:   1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. Forget: Non-existent session_id -> CodeNotFound
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: uuid.New().String(),
		Ino:       1,
		Nlookup:   1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 4. Forget: Ino = 0 -> CodeInvalidArgument
	_, err = h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       0,
		Nlookup:   1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 5. BatchForget: Empty session_id -> CodeInvalidArgument
	_, err = h.storageClient.BatchForget(ctx, &bowlv1.BatchForgetRequest{
		SessionId: "",
		Nodes: []*bowlv1.ForgetNode{
			{Ino: 1, Nlookup: 1},
		},
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 6. BatchForget: Non-existent session_id -> CodeNotFound
	_, err = h.storageClient.BatchForget(ctx, &bowlv1.BatchForgetRequest{
		SessionId: uuid.New().String(),
		Nodes: []*bowlv1.ForgetNode{
			{Ino: 1, Nlookup: 1},
		},
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 7. BatchForget: Empty nodes list -> Success (no-op)
	batchResp, err := h.storageClient.BatchForget(ctx, &bowlv1.BatchForgetRequest{
		SessionId: sessionID,
		Nodes:     []*bowlv1.ForgetNode{},
	})
	require.NoError(t, err)
	require.NotNil(t, batchResp)

	// 8. Forget on non-existent ino -> Idempotent Success (FUSE semantics: silent no-op)
	forgetNonExistent, err := h.storageClient.Forget(ctx, &bowlv1.ForgetRequest{
		SessionId: sessionID,
		Ino:       999999,
		Nlookup:   1,
	})
	require.NoError(t, err)
	require.NotNil(t, forgetNonExistent)
}
