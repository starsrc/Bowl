package db

import (
	"context"
	"errors"
	"fmt"
	"time"

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

type leaseTTLContextKey struct{}

// WithLeaseTTL 将客户端会话租约 TTL 注入上下文，供 UpsertClientSession 在行锁持有后基于数据库物理时钟原子续期。
func WithLeaseTTL(ctx context.Context, ttl time.Duration) context.Context {
	return context.WithValue(ctx, leaseTTLContextKey{}, ttl)
}

func leaseTTLFromContext(ctx context.Context) (time.Duration, bool) {
	if ctx == nil {
		return 0, false
	}
	ttl, ok := ctx.Value(leaseTTLContextKey{}).(time.Duration)
	return ttl, ok
}

type lockSyncInoContextKey struct{}

// WithLockSyncIno 将锁操作目标 Ino 注入上下文，供 UpsertClientSession 锁定目标会话及关联会话行，消除并发授锁与未提交续期交错。
func WithLockSyncIno(ctx context.Context, ino int64) context.Context {
	return context.WithValue(ctx, lockSyncInoContextKey{}, ino)
}

func lockSyncInoFromContext(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	ino, ok := ctx.Value(lockSyncInoContextKey{}).(int64)
	return ino, ok
}

const checkClientSessionExists = `-- name: checkClientSessionExists
SELECT EXISTS (
    SELECT 1 FROM client_sessions WHERE session_id = $1
)
`

const selectSessionAndInoRelatedSessionsForUpdate = `-- name: selectSessionAndInoRelatedSessionsForUpdate
SELECT s.session_id
FROM client_sessions s
WHERE s.session_id = $1
   OR EXISTS (SELECT 1 FROM file_locks fl WHERE fl.ino = $2 AND fl.session_id = s.session_id)
   OR EXISTS (SELECT 1 FROM file_lock_waiters fw WHERE fw.ino = $2 AND fw.session_id = s.session_id)
ORDER BY s.session_id ASC
FOR UPDATE
`

const deleteExpiredClientSessionsByIDs = `-- name: deleteExpiredClientSessionsByIDs
DELETE FROM client_sessions
WHERE session_id = ANY($1::bigint[])
  AND expires_at <= clock_timestamp()
`

const deleteExpiredClientSessionsOrdered = `-- name: deleteExpiredClientSessionsOrdered :execrows
DELETE FROM client_sessions
WHERE session_id IN (
    SELECT session_id
    FROM client_sessions
    WHERE expires_at <= clock_timestamp()
    ORDER BY session_id ASC
    FOR UPDATE
)
`

const selectClientSessionForUpdate = `-- name: selectClientSessionForUpdate
SELECT session_id
FROM client_sessions
WHERE session_id = $1
FOR UPDATE
`

const insertPlaceholderClientSession = `-- name: insertPlaceholderClientSession
INSERT INTO client_sessions (session_id, expires_at, updated_at, created_at)
VALUES ($1, '-infinity'::timestamptz, clock_timestamp(), clock_timestamp())
ON CONFLICT (session_id) DO NOTHING
`

const upsertClientSessionWithTTL = `-- name: upsertClientSessionWithTTL :one
WITH ts AS (
    SELECT clock_timestamp() AS now
),
target AS (
    SELECT session_id, (expires_at <= (SELECT now FROM ts)) AS is_expired
    FROM client_sessions
    WHERE session_id = $1
),
purged_locks AS (
    DELETE FROM file_locks
    WHERE session_id IN (SELECT session_id FROM target WHERE is_expired)
),
purged_waiters AS (
    DELETE FROM file_lock_waiters
    WHERE session_id IN (SELECT session_id FROM target WHERE is_expired)
),
purged_handles AS (
    DELETE FROM file_handles
    WHERE session_id IN (SELECT session_id FROM target WHERE is_expired)
)
UPDATE client_sessions s
SET expires_at = (SELECT now FROM ts) + ($2::bigint * interval '1 microsecond'),
    created_at = CASE
        WHEN t.is_expired THEN (SELECT now FROM ts)
        ELSE s.created_at
    END,
    updated_at = CASE
        WHEN t.is_expired THEN (SELECT now FROM ts)
        ELSE GREATEST((SELECT now FROM ts), s.created_at + interval '1 microsecond')
    END,
    disk_id = CASE
        WHEN t.is_expired THEN $3
        ELSE COALESCE($3, s.disk_id)
    END
FROM ts, target t
WHERE s.session_id = $1
RETURNING s.session_id, s.expires_at, s.updated_at, s.created_at, s.disk_id
`

const upsertClientSessionExplicitExpiresAt = `-- name: upsertClientSessionExplicitExpiresAt :one
WITH ts AS (
    SELECT clock_timestamp() AS now
),
target AS (
    SELECT session_id, (expires_at <= (SELECT now FROM ts)) AS is_expired
    FROM client_sessions
    WHERE session_id = $1
),
purged_locks AS (
    DELETE FROM file_locks
    WHERE session_id IN (SELECT session_id FROM target WHERE is_expired)
),
purged_waiters AS (
    DELETE FROM file_lock_waiters
    WHERE session_id IN (SELECT session_id FROM target WHERE is_expired)
),
purged_handles AS (
    DELETE FROM file_handles
    WHERE session_id IN (SELECT session_id FROM target WHERE is_expired)
)
UPDATE client_sessions s
SET expires_at = $2,
    created_at = CASE
        WHEN t.is_expired THEN (SELECT now FROM ts)
        ELSE s.created_at
    END,
    updated_at = CASE
        WHEN t.is_expired THEN (SELECT now FROM ts)
        ELSE GREATEST((SELECT now FROM ts), s.created_at + interval '1 microsecond')
    END,
    disk_id = CASE
        WHEN t.is_expired THEN $3
        ELSE COALESCE($3, s.disk_id)
    END
FROM ts, target t
WHERE s.session_id = $1
RETURNING s.session_id, s.expires_at, s.updated_at, s.created_at, s.disk_id
`

func upsertClientSessionTx(ctx context.Context, dbtx DBTX, arg UpsertClientSessionParams) (ClientSession, error) {
	lockSyncIno, hasSyncIno := lockSyncInoFromContext(ctx)
	if hasSyncIno && lockSyncIno > 0 {
		var lockedIDs []int64
		var hasTarget bool
		for attempt := 0; attempt < 5; attempt++ {
			var exists bool
			err := dbtx.QueryRow(ctx, checkClientSessionExists, arg.SessionID).Scan(&exists)
			if err != nil {
				return ClientSession{}, err
			}
			if !exists {
				if _, err := dbtx.Exec(ctx, insertPlaceholderClientSession, arg.SessionID); err != nil {
					return ClientSession{}, err
				}
			}

			rows, err := dbtx.Query(ctx, selectSessionAndInoRelatedSessionsForUpdate, arg.SessionID, lockSyncIno)
			if err != nil {
				return ClientSession{}, err
			}
			lockedIDs = lockedIDs[:0]
			hasTarget = false
			for rows.Next() {
				var sid int64
				if err := rows.Scan(&sid); err != nil {
					rows.Close()
					return ClientSession{}, err
				}
				lockedIDs = append(lockedIDs, sid)
				if sid == arg.SessionID {
					hasTarget = true
				}
			}
			if err := rows.Err(); err != nil {
				return ClientSession{}, err
			}
			rows.Close()

			if hasTarget {
				break
			}
		}
		if !hasTarget {
			return ClientSession{}, fmt.Errorf("failed to lock target session %d with lock sync ino %d", arg.SessionID, lockSyncIno)
		}

		// 仅按 session_id ASC 顺序对相关会话行加排他锁以实现跨会话互斥，
		// 过期会话的级联清理严格遵循 ADR-0016 由 RenewSession / Sweeper 触发。
	} else {
		var locked bool
		for i := 0; i < 5; i++ {
			var id int64
			err := dbtx.QueryRow(ctx, selectClientSessionForUpdate, arg.SessionID).Scan(&id)
			if err == nil {
				locked = true
				break
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return ClientSession{}, err
			}
			if _, err := dbtx.Exec(ctx, insertPlaceholderClientSession, arg.SessionID); err != nil {
				return ClientSession{}, err
			}
		}
		if !locked {
			var id int64
			if err := dbtx.QueryRow(ctx, selectClientSessionForUpdate, arg.SessionID).Scan(&id); err != nil {
				return ClientSession{}, err
			}
		}
	}

	var row pgx.Row
	if ttl, ok := leaseTTLFromContext(ctx); ok && ttl > 0 {
		row = dbtx.QueryRow(ctx, upsertClientSessionWithTTL, arg.SessionID, ttl.Microseconds(), arg.DiskID)
	} else {
		row = dbtx.QueryRow(ctx, upsertClientSessionExplicitExpiresAt, arg.SessionID, arg.ExpiresAt, arg.DiskID)
	}

	var i ClientSession
	if err := row.Scan(
		&i.SessionID,
		&i.ExpiresAt,
		&i.UpdatedAt,
		&i.CreatedAt,
		&i.DiskID,
	); err != nil {
		return ClientSession{}, err
	}
	return i, nil
}

const selectOrphanInodesForUpdate = `-- name: SelectOrphanInodesForUpdate :many
SELECT i.ino
FROM inodes i
WHERE i.nlink = 0
  AND NOT EXISTS (
      SELECT 1
      FROM file_handles fh
      WHERE fh.ino = i.ino
  )
ORDER BY i.ino ASC
FOR UPDATE SKIP LOCKED
`

const deleteLockedOrphanInodes = `-- name: DeleteLockedOrphanInodes :many
DELETE FROM inodes i
WHERE i.ino = ANY($1::bigint[])
  AND i.nlink = 0
  AND NOT EXISTS (
      SELECT 1
      FROM file_handles fh
      WHERE fh.ino = i.ino
  )
RETURNING i.ino, i.root_ino, i.mode, i.size
`

func deleteUnreferencedOrphanInodesTwoPhase(ctx context.Context, dbtx DBTX) ([]DeleteUnreferencedOrphanInodesRow, error) {
	rows, err := dbtx.Query(ctx, selectOrphanInodesForUpdate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lockedInos []int64
	for rows.Next() {
		var ino int64
		if err := rows.Scan(&ino); err != nil {
			return nil, err
		}
		lockedInos = append(lockedInos, ino)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	if len(lockedInos) == 0 {
		return []DeleteUnreferencedOrphanInodesRow{}, nil
	}

	deleteRows, err := dbtx.Query(ctx, deleteLockedOrphanInodes, lockedInos)
	if err != nil {
		return nil, err
	}
	defer deleteRows.Close()

	var items []DeleteUnreferencedOrphanInodesRow
	for deleteRows.Next() {
		var i DeleteUnreferencedOrphanInodesRow
		if err := deleteRows.Scan(
			&i.Ino,
			&i.RootIno,
			&i.Mode,
			&i.Size,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := deleteRows.Err(); err != nil {
		return nil, err
	}
	if items == nil {
		items = []DeleteUnreferencedOrphanInodesRow{}
	}
	return items, nil
}

// txQueries 包装 Queries 并持有底层 DBTX，覆盖特定事务内复杂查询实现。
type txQueries struct {
	*Queries
	db DBTX
}

var _ Querier = (*txQueries)(nil)

// DeleteExpiredClientSessions 在事务内按 session_id ASC 有序加锁清扫过期会话，避免并发操作产生行锁顺序反转。
func (q *txQueries) DeleteExpiredClientSessions(ctx context.Context) (int64, error) {
	res, err := q.db.Exec(ctx, deleteExpiredClientSessionsOrdered)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

// DeleteUnreferencedOrphanInodes 在事务内分两阶段执行孤儿 Inode 清理（行级加锁 + 新快照复核删除）。
func (q *txQueries) DeleteUnreferencedOrphanInodes(ctx context.Context) ([]DeleteUnreferencedOrphanInodesRow, error) {
	return deleteUnreferencedOrphanInodesTwoPhase(ctx, q.db)
}

// UpsertClientSession 在事务内先稳妥获取行排他锁，随后基于获锁后物理时钟原子裁决过期与清理旧状态。
func (q *txQueries) UpsertClientSession(ctx context.Context, arg UpsertClientSessionParams) (ClientSession, error) {
	return upsertClientSessionTx(ctx, q.db, arg)
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
			return &txQueries{
				Queries: New(rawTx),
				db:      rawTx,
			}
		}),
		pool: pool,
	}
}

// DeleteExpiredClientSessions 覆写顶层非事务调用，确保按 session_id ASC 有序加锁清扫在独立事务内执行。
func (s *SQLStore) DeleteExpiredClientSessions(ctx context.Context) (int64, error) {
	var affected int64
	err := s.ExecTx(ctx, func(q Querier) error {
		var err error
		affected, err = q.DeleteExpiredClientSessions(ctx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return affected, nil
}

// DeleteUnreferencedOrphanInodes 覆写顶层非事务调用，确保两步清理在同一独立事务内执行。
func (s *SQLStore) DeleteUnreferencedOrphanInodes(ctx context.Context) ([]DeleteUnreferencedOrphanInodesRow, error) {
	var rows []DeleteUnreferencedOrphanInodesRow
	err := s.ExecTx(ctx, func(q Querier) error {
		var err error
		rows, err = q.DeleteUnreferencedOrphanInodes(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// UpsertClientSession 覆写顶层非事务调用，确保行排他锁与原子续期裁决在同一独立读写事务内执行。
func (s *SQLStore) UpsertClientSession(ctx context.Context, arg UpsertClientSessionParams) (ClientSession, error) {
	var session ClientSession
	err := s.ExecTx(ctx, func(q Querier) error {
		var err error
		session, err = q.UpsertClientSession(ctx, arg)
		return err
	})
	if err != nil {
		return ClientSession{}, err
	}
	return session, nil
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
