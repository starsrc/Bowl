package db_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/disk/internal/db"
)

func setupTestStore(t *testing.T) (db.Store, func()) {
	t.Helper()
	dsn := getTestDSN()

	err := db.MigrateUp(dsn)
	require.NoError(t, err)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	store := db.NewStore(pool)

	cleanup := func() {
		_, _ = pool.Exec(ctx, "TRUNCATE TABLE disks CASCADE")
		store.Close()
	}

	return store, cleanup
}

func TestStore_CreateAndGetDisk(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	name := "test-volume-01"
	capacity := int64(10 * 1024 * 1024 * 1024) // 10 GB
	inodesQuota := int64(1000000)
	status := int16(2) // READY

	created, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          name,
		CapacityBytes: capacity,
		InodesQuota:   inodesQuota,
		Status:        status,
	})
	require.NoError(t, err)
	assert.Equal(t, diskID, uuid.UUID(created.DiskID.Bytes))
	assert.Equal(t, name, created.Name)
	assert.Equal(t, capacity, created.CapacityBytes)
	assert.Equal(t, int64(0), created.UsedBytes)
	assert.Equal(t, inodesQuota, created.InodesQuota)
	assert.Equal(t, int64(0), created.InodesUsed)
	assert.Equal(t, status, created.Status)
	assert.WithinDuration(t, time.Now(), created.CreatedAt.Time, 5*time.Second)
	assert.WithinDuration(t, time.Now(), created.UpdatedAt.Time, 5*time.Second)

	// Query via GetDisk
	retrieved, err := store.GetDisk(ctx, pgtype.UUID{Bytes: diskID, Valid: true})
	require.NoError(t, err)
	assert.Equal(t, created.DiskID, retrieved.DiskID)
	assert.Equal(t, created.Name, retrieved.Name)
	assert.Equal(t, created.CapacityBytes, retrieved.CapacityBytes)
	assert.Equal(t, created.Status, retrieved.Status)

	// Query non-existent disk
	_, err = store.GetDisk(ctx, pgtype.UUID{Bytes: uuid.New(), Valid: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_UpdateDiskStatus(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	_, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          "status-test-volume",
		CapacityBytes: 1024,
		InodesQuota:   100,
		Status:        2, // READY
	})
	require.NoError(t, err)

	updated, err := store.UpdateDiskStatus(ctx, db.UpdateDiskStatusParams{
		DiskID: pgtype.UUID{Bytes: diskID, Valid: true},
		Status: 3, // ATTACHED
	})
	require.NoError(t, err)
	assert.Equal(t, int16(3), updated.Status)
}

func TestStore_DiskAttachments_OWMR(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	_, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          "owmr-test-volume",
		CapacityBytes: 1024,
		InodesQuota:   100,
		Status:        2,
	})
	require.NoError(t, err)

	diskUUID := pgtype.UUID{Bytes: diskID, Valid: true}

	// 1. Attach writer client-1 (read_only = false)
	writerAtt, err := store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "writer-client-1",
		ReadOnly: false,
	})
	require.NoError(t, err)
	assert.Equal(t, "writer-client-1", writerAtt.ClientID)
	assert.False(t, writerAtt.ReadOnly)

	// 2. Attach reader client-2 (read_only = true) -> succeeds
	readerAtt, err := store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "reader-client-2",
		ReadOnly: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "reader-client-2", readerAtt.ClientID)
	assert.True(t, readerAtt.ReadOnly)

	// 3. Attach another writer client-3 (read_only = false) -> fails due to OWMR partial unique index idx_disk_attachments_writer
	_, err = store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "writer-client-3",
		ReadOnly: false,
	})
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "23505", pgErr.Code)
	assert.Equal(t, "idx_disk_attachments_writer", pgErr.ConstraintName)

	// 4. Attach same client reader-client-2 again -> fails due to idx_disk_attachments_client
	_, err = store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "reader-client-2",
		ReadOnly: true,
	})
	require.Error(t, err)
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "23505", pgErr.Code)
	assert.Equal(t, "idx_disk_attachments_client", pgErr.ConstraintName)

	// 5. Count attachments
	count, err := store.CountDiskAttachmentsByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// 6. List attachments
	attachments, err := store.ListDiskAttachmentsByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Len(t, attachments, 2)

	// 7. Delete writer attachment
	deletedRows, err := store.DeleteDiskAttachment(ctx, db.DeleteDiskAttachmentParams{
		DiskID:   diskUUID,
		ClientID: "writer-client-1",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedRows)

	// 8. Now client-3 can attach as writer!
	writerAtt3, err := store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "writer-client-3",
		ReadOnly: false,
	})
	require.NoError(t, err)
	assert.Equal(t, "writer-client-3", writerAtt3.ClientID)

	// 9. Delete all attachments for disk
	deletedAll, err := store.DeleteDiskAttachmentsByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deletedAll) // client-2 and client-3

	countAfter, err := store.CountDiskAttachmentsByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), countAfter)
}

