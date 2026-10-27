package db

import (
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/config"
)

// Store 组合 Querier 与通用事务执行器接口。
type Store interface {
	Querier
	tx.Transactor[Querier]
}

// SQLStore 提供基于 pgxpool.Pool 的 Store 实现。
type SQLStore struct {
	*Queries
	tx.Transactor[Querier]
	pool *pgxpool.Pool
}

var _ Store = (*SQLStore)(nil)

// NewStore 基于已有 pgxpool.Pool 创建 Store 实例。
func NewStore(pool *pgxpool.Pool) Store {
	return &SQLStore{
		Queries: New(pool),
		Transactor: tx.NewPGXTransactor(pool, func(rawTx pgx.Tx) Querier {
			return New(rawTx)
		}),
		pool: pool,
	}
}

// Close 关闭底层数据库连接池。
func (s *SQLStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// Pool 返回底层 pgxpool.Pool 实例。
func (s *SQLStore) Pool() *pgxpool.Pool {
	return s.pool
}

// OpenPool 根据 DBPoolConfig 初始化底层 pgxpool.Pool。
func OpenPool(cfg config.DBPoolConfig) (*pgxpool.Pool, error) {
	pool, err := tx.OpenPool(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dbpool config: %w", err)
	}
	return pool, nil
}

// PoolParams 封装 ProvidePool 依赖注入参数。
type PoolParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    config.DBPoolConfig
	Ready     telemetry.Ready `optional:"true"`
}

// ProvidePool 根据 PoolParams 初始化底层连接池并托管其生命周期至 Fx。
func ProvidePool(params PoolParams) (*pgxpool.Pool, error) {
	pool, err := OpenPool(params.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dbpool: %w", err)
	}

	tx.RegisterPoolLifecycle(params.Lifecycle, pool)
	return pool, nil
}

// Module 提供 db 依赖注入模块。
var Module = fx.Module("db",
	fx.Provide(
		ProvidePool,
		NewStore,
	),
)
