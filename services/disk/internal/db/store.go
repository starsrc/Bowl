package db

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/tx"
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
}

var _ Store = (*SQLStore)(nil)

// NewStore 基于已有 pgxpool.Pool 创建 Store 实例。
func NewStore(pool *pgxpool.Pool) Store {
	return &SQLStore{
		Queries: New(pool),
		Transactor: tx.NewPGXTransactor(pool, func(rawTx pgx.Tx) Querier {
			return New(rawTx)
		}),
	}
}

// Module 提供 db 依赖注入模块。
var Module = fx.Module("db",
	fx.Provide(NewStore),
)
