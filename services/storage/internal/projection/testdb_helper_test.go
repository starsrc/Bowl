package projection_test

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/projection"
)

const defaultProjectionTestDSN = "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"

var projectionTestSchemaSeq atomic.Int64

func resolveProjectionTestDSN() string {
	return cmp.Or(
		os.Getenv("STORAGE_TEST_DSN"),
		os.Getenv("STORAGE_DB_DSN"),
		os.Getenv("DATABASE_URL"),
		os.Getenv("BOWL_TEST_POSTGRES_DSN"),
		os.Getenv("TEST_DB_DSN"),
		defaultProjectionTestDSN,
	)
}

func setupProjectionTestDB(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	dsn := resolveProjectionTestDSN()
	adminCfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)

	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	defer pingCancel()

	adminPool, err := pgxpool.NewWithConfig(pingCtx, adminCfg)
	if err != nil {
		t.Skipf("postgres unavailable at %s: %v", dsn, err)
	}
	if pingErr := adminPool.Ping(pingCtx); pingErr != nil {
		adminPool.Close()
		t.Skipf("postgres unreachable at %s: %v", dsn, pingErr)
	}
	t.Cleanup(adminPool.Close)

	schema := fmt.Sprintf("test_proj_%d_%d", time.Now().UnixNano(), projectionTestSchemaSeq.Add(1))
	_, err = adminPool.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dropCancel()
		_, dropErr := adminPool.Exec(dropCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		require.NoError(t, dropErr)
	})

	isoCfg := adminPool.Config().Copy()
	if isoCfg.ConnConfig.RuntimeParams == nil {
		isoCfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	isoCfg.ConnConfig.RuntimeParams["search_path"] = schema

	isoPool, err := pgxpool.NewWithConfig(ctx, isoCfg)
	require.NoError(t, err)
	t.Cleanup(isoPool.Close)
	require.NoError(t, isoPool.Ping(ctx))

	require.NoError(t, projection.EnsureSchema(ctx, isoPool))
	require.NoError(t, projection.EnsureSchema(ctx, isoPool))

	return isoPool, ctx
}
