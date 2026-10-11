package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

func TestEnsureDiskSchema_ChunksTableInitialized(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "chunks-schema-test-client"
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
	require.NotEmpty(t, mountResp.GetSessionId())

	// Verify chunks table exists in the schema
	var tableExists bool
	checkTableSQL := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = 'chunks'
		);
	`
	err = h.dbPool.QueryRow(ctx, checkTableSQL, schemaName).Scan(&tableExists)
	require.NoError(t, err)
	assert.True(t, tableExists, "table %s.chunks must exist", schemaName)

	// Verify columns: ino (bigint), indx (integer), slices (bytea)
	type columnInfo struct {
		columnName string
		dataType   string
		isNullable string
	}
	colsQuery := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'chunks'
		ORDER BY column_name;
	`
	rows, err := h.dbPool.Query(ctx, colsQuery, schemaName)
	require.NoError(t, err)
	defer rows.Close()

	cols := make(map[string]columnInfo)
	for rows.Next() {
		var c columnInfo
		err := rows.Scan(&c.columnName, &c.dataType, &c.isNullable)
		require.NoError(t, err)
		cols[c.columnName] = c
	}
	require.NoError(t, rows.Err())

	require.Contains(t, cols, "ino")
	assert.Equal(t, "bigint", cols["ino"].dataType)
	assert.Equal(t, "NO", cols["ino"].isNullable)

	require.Contains(t, cols, "indx")
	assert.Equal(t, "integer", cols["indx"].dataType)
	assert.Equal(t, "NO", cols["indx"].isNullable)

	require.Contains(t, cols, "slices")
	assert.Equal(t, "bytea", cols["slices"].dataType)
	assert.Equal(t, "NO", cols["slices"].isNullable)

	// Verify primary key constraint is (ino, indx)
	var pkColumns []string
	pkQuery := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = $1
		  AND tc.table_name = 'chunks'
		  AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position;
	`
	pkRows, err := h.dbPool.Query(ctx, pkQuery, schemaName)
	require.NoError(t, err)
	defer pkRows.Close()

	for pkRows.Next() {
		var col string
		err := pkRows.Scan(&col)
		require.NoError(t, err)
		pkColumns = append(pkColumns, col)
	}
	require.NoError(t, pkRows.Err())

	assert.Equal(t, []string{"ino", "indx"}, pkColumns)

	// Verify we can insert and query a chunks row
	insertSQL := fmt.Sprintf(`
		INSERT INTO "%s".chunks (ino, indx, slices)
		VALUES ($1, $2, $3)
		RETURNING ino, indx, slices;
	`, schemaName)
	var (
		resIno    int64
		resIndx   int32
		resSlices []byte
	)
	testPayload := []byte{0x01, 0x02, 0x03, 0x04}
	err = h.dbPool.QueryRow(ctx, insertSQL, int64(100), int32(0), testPayload).Scan(&resIno, &resIndx, &resSlices)
	require.NoError(t, err)
	assert.Equal(t, int64(100), resIno)
	assert.Equal(t, int32(0), resIndx)
	assert.Equal(t, testPayload, resSlices)
}

func TestEnsureDiskSchema_SliceRefsTableInitialized(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "slice-refs-schema-test-client"
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
	require.NotEmpty(t, mountResp.GetSessionId())

	// Verify slice_refs table exists in the schema
	var tableExists bool
	checkTableSQL := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = 'slice_refs'
		);
	`
	err = h.dbPool.QueryRow(ctx, checkTableSQL, schemaName).Scan(&tableExists)
	require.NoError(t, err)
	assert.True(t, tableExists, "table %s.slice_refs must exist", schemaName)

	// Verify columns: id (bigint), size (integer), refs (integer)
	type columnInfo struct {
		columnName string
		dataType   string
		isNullable string
	}
	colsQuery := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'slice_refs'
		ORDER BY column_name;
	`
	rows, err := h.dbPool.Query(ctx, colsQuery, schemaName)
	require.NoError(t, err)
	defer rows.Close()

	cols := make(map[string]columnInfo)
	for rows.Next() {
		var c columnInfo
		err := rows.Scan(&c.columnName, &c.dataType, &c.isNullable)
		require.NoError(t, err)
		cols[c.columnName] = c
	}
	require.NoError(t, rows.Err())

	require.Contains(t, cols, "id")
	assert.Equal(t, "bigint", cols["id"].dataType)
	assert.Equal(t, "NO", cols["id"].isNullable)

	require.Contains(t, cols, "size")
	assert.Equal(t, "integer", cols["size"].dataType)
	assert.Equal(t, "NO", cols["size"].isNullable)

	require.Contains(t, cols, "refs")
	assert.Equal(t, "integer", cols["refs"].dataType)
	assert.Equal(t, "NO", cols["refs"].isNullable)

	// Verify primary key constraint is (id)
	var pkColumns []string
	pkQuery := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = $1
		  AND tc.table_name = 'slice_refs'
		  AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position;
	`
	pkRows, err := h.dbPool.Query(ctx, pkQuery, schemaName)
	require.NoError(t, err)
	defer pkRows.Close()

	for pkRows.Next() {
		var col string
		err := pkRows.Scan(&col)
		require.NoError(t, err)
		pkColumns = append(pkColumns, col)
	}
	require.NoError(t, pkRows.Err())

	assert.Equal(t, []string{"id"}, pkColumns)
}
