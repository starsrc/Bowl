package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
