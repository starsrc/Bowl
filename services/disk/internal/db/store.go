package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store combines Querier with transaction execution capabilities.
type Store interface {
	Querier
	ExecTx(ctx context.Context, fn func(Querier) error) error
	Close()
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// SQLStore provides Store implementation backed by pgxpool.Pool.
type SQLStore struct {
	*Queries
	pool *pgxpool.Pool
}

// NewStore initializes a Store instance from a pgxpool.Pool.
func NewStore(pool *pgxpool.Pool) Store {
	return &SQLStore{
		Queries: New(pool),
		pool:    pool,
	}
}

// Exec executes a raw SQL query on the connection pool.
func (s *SQLStore) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return s.pool.Exec(ctx, sql, arguments...)
}

// Close closes the underlying connection pool.
func (s *SQLStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// ExecTx executes a function within a database transaction.
func (s *SQLStore) ExecTx(ctx context.Context, fn func(Querier) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := fn(s.WithTx(tx)); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
