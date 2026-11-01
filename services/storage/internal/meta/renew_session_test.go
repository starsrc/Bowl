package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// TestRenewSession_InputValidation 验证 RenewSession 的入参防御逻辑。
func TestRenewSession_InputValidation(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		sessionID uint64
	}{
		{
			name:      "sessionID 为 0",
			sessionID: 0,
		},
		{
			name:      "sessionID 超过 math.MaxInt64",
			sessionID: uint64(math.MaxInt64) + 1,
		},
		{
			name:      "sessionID 为 uint64 最大值",
			sessionID: math.MaxUint64,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			expiresAt, err := mgr.RenewSession(ctx, tc.sessionID)
			require.Error(t, err)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
			assert.True(t, expiresAt.IsZero())
		})
	}
}

// TestRenewSession_Unit 验证 RenewSession 领域层在 Mock Querier 下的正常流与异常流。
func TestRenewSession_Unit(t *testing.T) {
	ctx := context.Background()
	sessID := uint64(10086)
	expectedExpires := time.Now().Add(10 * time.Minute).Truncate(time.Microsecond)

	t.Run("正常流: 成功清理过期会话、续期并回收孤儿节点", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sessID) && arg.ExpiresAt.Valid
			})).
			Return(db.ClientSession{
				SessionID: int64(sessID),
				ExpiresAt: pgtype.Timestamptz{Time: expectedExpires, Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)
		assert.Equal(t, expectedExpires, expiresAt)
	})

	t.Run("异常流: DeleteExpiredClientSessions 失败向上传播错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		expectedErr := errors.New("delete expired sessions failed")
		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), expectedErr).
			Once()

		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.True(t, expiresAt.IsZero())
	})

	t.Run("异常流: UpsertClientSession 失败向上传播错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		expectedErr := errors.New("upsert session failed")
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{}, expectedErr).
			Once()

		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.True(t, expiresAt.IsZero())
	})

	t.Run("异常流: DeleteUnreferencedOrphanInodes 失败向上传播错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{
				SessionID: int64(sessID),
				ExpiresAt: pgtype.Timestamptz{Time: expectedExpires, Valid: true},
			}, nil).
			Once()

		expectedErr := errors.New("delete orphan inodes failed")
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), expectedErr).
			Once()

		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.True(t, expiresAt.IsZero())
	})

	t.Run("批量清理分属不同 root_ino 的孤儿节点按 root_ino 升序汇总退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sessID) && arg.ExpiresAt.Valid
			})).
			Return(db.ClientSession{
				SessionID: int64(sessID),
				ExpiresAt: pgtype.Timestamptz{Time: expectedExpires, Valid: true},
			}, nil).
			Once()

		orphanRows := []db.DeleteUnreferencedOrphanInodesRow{
			{Ino: 101, RootIno: 20, Mode: int32(meta.S_IFREG | 0o644), Size: 4096}, // disk 20: reg, 4096B
			{Ino: 102, RootIno: 10, Mode: int32(meta.S_IFREG | 0o644), Size: 1024}, // disk 10: reg, 1024B
			{Ino: 103, RootIno: 20, Mode: int32(meta.S_IFDIR | 0o755), Size: 4096}, // disk 20: dir, 0B (目录不退还 size)
			{Ino: 104, RootIno: 10, Mode: int32(meta.S_IFREG | 0o644), Size: 0},    // disk 10: reg, 0B (空文件)
			{Ino: 105, RootIno: 0, Mode: int32(meta.S_IFREG | 0o644), Size: 2048},  // root_ino=0: 不退还配额
			{Ino: 106, RootIno: 10, Mode: int32(meta.S_IFREG | 0o644), Size: 2048}, // disk 10: reg, 2048B
		}

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return(orphanRows, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, []int64{101, 102, 103, 104, 105, 106}).
			Return([][]byte(nil), nil).
			Once()

		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)
		assert.Equal(t, expectedExpires, expiresAt)
	})
}

// TestRenewSessionWithStatus_Unit 验证 RenewSessionWithStatus 的单元测试与异常错误传播。
func TestRenewSessionWithStatus_Unit(t *testing.T) {
	ctx := context.Background()
	sessID := uint64(10086)
	expectedExpires := time.Now().Add(10 * time.Minute).Truncate(time.Microsecond)

	t.Run("首次创建或过期重建会话返回 SessionRecreated: true 且 ExpiresAt 与 DB 返回值一致", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		ts := time.Now().Truncate(time.Microsecond)
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sessID) && arg.ExpiresAt.Valid
			})).
			Return(db.ClientSession{
				SessionID: int64(sessID),
				ExpiresAt: pgtype.Timestamptz{Time: expectedExpires, Valid: true},
				CreatedAt: pgtype.Timestamptz{Time: ts, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: ts, Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		res, err := mgr.RenewSessionWithStatus(ctx, sessID)
		require.NoError(t, err)
		assert.True(t, res.SessionRecreated, "CreatedAt == UpdatedAt 时 SessionRecreated 必须为 true")
		assert.Equal(t, expectedExpires, res.ExpiresAt)
	})

	t.Run("续期已有活跃会话返回 SessionRecreated: false 且 ExpiresAt 与 DB 返回值一致", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		createdTS := time.Now().Add(-5 * time.Minute).Truncate(time.Microsecond)
		updatedTS := time.Now().Truncate(time.Microsecond)
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sessID) && arg.ExpiresAt.Valid
			})).
			Return(db.ClientSession{
				SessionID: int64(sessID),
				ExpiresAt: pgtype.Timestamptz{Time: expectedExpires, Valid: true},
				CreatedAt: pgtype.Timestamptz{Time: createdTS, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: updatedTS, Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		res, err := mgr.RenewSessionWithStatus(ctx, sessID)
		require.NoError(t, err)
		assert.False(t, res.SessionRecreated, "CreatedAt < UpdatedAt 时 SessionRecreated 必须为 false")
		assert.Equal(t, expectedExpires, res.ExpiresAt)
	})

	t.Run("配置自定义 WithSessionLeaseTTL 时传入 UpsertClientSession 的 expiresAt 处于合理窗口", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		customTTL := 45 * time.Minute
		mgr := meta.New(mockStore, meta.WithSessionLeaseTTL(customTTL))

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(1), nil). // 模拟清理了1个过期会话触发 notifyAllLockWaiters
			Once()

		before := time.Now()
		var capturedExpiresAt time.Time
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				if arg.SessionID != int64(sessID) || !arg.ExpiresAt.Valid {
					return false
				}
				capturedExpiresAt = arg.ExpiresAt.Time
				return true
			})).
			Return(db.ClientSession{
				SessionID: int64(sessID),
				ExpiresAt: pgtype.Timestamptz{Time: expectedExpires, Valid: true},
				CreatedAt: pgtype.Timestamptz{Time: before, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: before, Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		res, err := mgr.RenewSessionWithStatus(ctx, sessID)
		after := time.Now()
		require.NoError(t, err)
		assert.True(t, res.SessionRecreated)
		assert.Equal(t, expectedExpires, res.ExpiresAt)

		// 验证传入 DB 的 expiresAt 处于 [before+customTTL, after+customTTL] 窗口内
		assert.False(t, capturedExpiresAt.Before(before.Add(customTTL)), "传入 expiresAt 不得早于 before + customTTL")
		assert.False(t, capturedExpiresAt.After(after.Add(customTTL)), "传入 expiresAt 不得晚于 after + customTTL")
	})

	t.Run("参数校验防御: sessionID 为 0 或超出 math.MaxInt64 返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		for _, invalidID := range []uint64{0, uint64(math.MaxInt64) + 1, math.MaxUint64} {
			res, err := mgr.RenewSessionWithStatus(ctx, invalidID)
			require.Error(t, err)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
			assert.Empty(t, res)
		}
	})

	t.Run("异常流: DeleteExpiredClientSessions 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		expectedErr := errors.New("delete expired failed")
		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), expectedErr).
			Once()

		res, err := mgr.RenewSessionWithStatus(ctx, sessID)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Empty(t, res)
	})

	t.Run("异常流: UpsertClientSession 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		expectedErr := errors.New("upsert failed")
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{}, expectedErr).
			Once()

		res, err := mgr.RenewSessionWithStatus(ctx, sessID)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Empty(t, res)
	})

	t.Run("异常流: DeleteUnreferencedOrphanInodes 失败透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{
				SessionID: int64(sessID),
				ExpiresAt: pgtype.Timestamptz{Time: expectedExpires, Valid: true},
			}, nil).
			Once()

		expectedErr := errors.New("delete orphans failed")
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), expectedErr).
			Once()

		res, err := mgr.RenewSessionWithStatus(ctx, sessID)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Empty(t, res)
	})
}

// TestRenewSession_PostgresIntegration 验证 RenewSession 在真实 PostgreSQL 环境下的功能与只读客户端保活特性。
func TestRenewSession_PostgresIntegration(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过 PostgreSQL 测试 (连接初始化失败): %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("跳过 PostgreSQL 测试 (ping 失败): %v", err)
	}

	store := db.NewStore(pool)

	t.Run("AC1_AC2: 正常续期与租约刷新", func(t *testing.T) {
		leaseTTL := 3 * time.Second
		mgr := meta.New(store, meta.WithSessionLeaseTTL(leaseTTL))
		sessID := uint64(88001)

		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
		}()

		nowBefore := time.Now()
		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)
		assert.True(t, expiresAt.After(nowBefore))
		assert.True(t, expiresAt.After(nowBefore.Add(leaseTTL-500*time.Millisecond)))

		// 验证数据库持久化记录
		var dbExpiresAt time.Time
		err = pool.QueryRow(ctx, "SELECT expires_at FROM client_sessions WHERE session_id = $1", int64(sessID)).Scan(&dbExpiresAt)
		require.NoError(t, err)
		assert.WithinDuration(t, expiresAt, dbExpiresAt, time.Millisecond*50)

		// 再次续期，验证时间戳被推进延展
		time.Sleep(200 * time.Millisecond)
		secondExpiresAt, err := mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)
		assert.True(t, secondExpiresAt.After(expiresAt), "二次续期租约截止时间必须单调推进")

		// 验证 RenewSessionWithStatus 返回的 ExpiresAt 处于合理 TTL 窗口内
		statusBefore := time.Now()
		statusRes, err := mgr.RenewSessionWithStatus(ctx, sessID)
		require.NoError(t, err)
		assert.False(t, statusRes.SessionRecreated, "续期已有活跃会话时 session_recreated 必须为 false")
		assert.True(t, statusRes.ExpiresAt.After(statusBefore))
		assert.True(t, statusRes.ExpiresAt.After(statusBefore.Add(leaseTTL-500*time.Millisecond)))
		assert.True(t, statusRes.ExpiresAt.Before(time.Now().Add(leaseTTL+500*time.Millisecond)))

		// 验证 RenewSessionWithStatus 首次创建会话返回 session_recreated = true 且 ExpiresAt 处于合理 TTL 窗口内
		newSessID := uint64(88009)
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(newSessID))
		}()
		newBefore := time.Now()
		newRes, err := mgr.RenewSessionWithStatus(ctx, newSessID)
		require.NoError(t, err)
		assert.True(t, newRes.SessionRecreated, "首次创建会话 session_recreated 必须为 true")
		assert.True(t, newRes.ExpiresAt.After(newBefore))
		assert.True(t, newRes.ExpiresAt.After(newBefore.Add(leaseTTL-500*time.Millisecond)))
		assert.True(t, newRes.ExpiresAt.Before(time.Now().Add(leaseTTL+500*time.Millisecond)))
	})

	t.Run("AC2: 顺带清理超期会话与孤儿 Inode", func(t *testing.T) {
		mgr := meta.New(store)
		activeSess := uint64(88002)
		expiredSess := uint64(88003)

		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(activeSess), int64(expiredSess))
		}()

		// 预置已超期的会话（10 秒前过期）
		pastTime := time.Now().Add(-10 * time.Second)
		_, err := pool.Exec(ctx, `
			INSERT INTO client_sessions (session_id, expires_at, updated_at)
			VALUES ($1, $2, clock_timestamp())
			ON CONFLICT (session_id) DO UPDATE SET expires_at = EXCLUDED.expires_at
		`, int64(expiredSess), pastTime)
		require.NoError(t, err)

		// 预置孤儿 Inode (nlink = 0，无任何有效句柄)
		orphanInode, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 0,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		orphanIno := orphanInode.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), orphanIno)
		}()

		// 调用 RenewSession 续期活跃会话
		_, err = mgr.RenewSession(ctx, activeSess)
		require.NoError(t, err)

		// 验证超期会话被自动清理
		var cnt int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(expiredSess)).Scan(&cnt)
		require.NoError(t, err)
		assert.Equal(t, 0, cnt, "超期会话应被自动级联清理")

		// 验证孤儿 Inode 被非阻塞物理回收
		_, err = store.GetInode(ctx, orphanIno)
		assert.ErrorIs(t, err, pgx.ErrNoRows, "无句柄引用的孤儿 Inode 应被物理回收")
	})

	t.Run("AC3: 只读客户端长连接保活效果验证", func(t *testing.T) {
		// 设置较短的租约 TTL (1500 毫秒)
		leaseTTL := 1500 * time.Millisecond
		mgr := meta.New(store, meta.WithSessionLeaseTTL(leaseTTL))

		sessKeepAlive := uint64(88004)
		sessExpired := uint64(88005)

		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sessKeepAlive), int64(sessExpired))
		}()

		rootDir, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		rootIno := uint64(rootDir.Ino)
		defer func() {
			_ = store.DeleteInode(context.Background(), rootDir.Ino)
		}()

		// 创建测试目录与常规文件
		testDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("keepalive_dir"),
			Mode:      meta.S_IFDIR | 0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(testDir.Ino))
		}()

		testFile, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("keepalive_file.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &sessKeepAlive,
		})
		require.NoError(t, err)
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(testFile.Inode.Ino))
		}()

		// 客户端 A (sessKeepAlive) 打开目录与文件获得句柄
		dirResA, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       testDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessKeepAlive,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.ReleaseDir(context.Background(), meta.ReleaseDirParams{
				Ino:       testDir.Ino,
				Fh:        dirResA.Fh,
				SessionID: &sessKeepAlive,
			})
		}()

		fileResA, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       testFile.Inode.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessKeepAlive,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{
				Ino:       testFile.Inode.Ino,
				Fh:        fileResA.Fh,
				SessionID: &sessKeepAlive,
			})
		}()

		// 客户端 B (sessExpired) 对照组：打开目录与文件获得句柄，但不续期
		dirResB, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       testDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessExpired,
		})
		require.NoError(t, err)

		fileResB, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       testFile.Inode.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessExpired,
		})
		require.NoError(t, err)

		// 初始状态：只读操作均可用
		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{Ino: testDir.Ino, Fh: dirResA.Fh, Limit: 10, SessionID: &sessKeepAlive})
		require.NoError(t, err)
		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: testDir.Ino, Fh: dirResA.Fh, Limit: 10, SessionID: &sessKeepAlive})
		require.NoError(t, err)
		require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: testDir.Ino, Fh: dirResA.Fh, SessionID: &sessKeepAlive}))
		require.NoError(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: testFile.Inode.Ino, Fh: fileResA.Fh, SessionID: &sessKeepAlive}))
		_, err = mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       testFile.Inode.Ino,
			Fh:        fileResA.Fh,
			LockOwner: 100,
			SessionID: sessKeepAlive,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK},
		})
		require.NoError(t, err)

		// 睡眠 800ms（小于 leaseTTL 1500ms），客户端 A 调用 RenewSession 显式保活
		time.Sleep(800 * time.Millisecond)
		newExpires, err := mgr.RenewSession(ctx, sessKeepAlive)
		require.NoError(t, err)
		assert.True(t, newExpires.After(time.Now().Add(1000*time.Millisecond)))

		// 再次睡眠 900ms（此时累计流逝 1700ms，已经超过初始 1500ms TTL）
		time.Sleep(900 * time.Millisecond)

		// 验证客户端 A (sessKeepAlive) 的句柄依然有效可用（ReadDir, ReadDirPlus, Fsync, FsyncDir, GetLk）
		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{Ino: testDir.Ino, Fh: dirResA.Fh, Limit: 10, SessionID: &sessKeepAlive})
		assert.NoError(t, err, "续期后的客户端 ReadDir 必须成功")

		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: testDir.Ino, Fh: dirResA.Fh, Limit: 10, SessionID: &sessKeepAlive})
		assert.NoError(t, err, "续期后的客户端 ReadDirPlus 必须成功")

		assert.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: testDir.Ino, Fh: dirResA.Fh, SessionID: &sessKeepAlive}), "续期后的客户端 FsyncDir 必须成功")
		assert.NoError(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: testFile.Inode.Ino, Fh: fileResA.Fh, SessionID: &sessKeepAlive}), "续期后的客户端 Fsync 必须成功")

		_, err = mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       testFile.Inode.Ino,
			Fh:        fileResA.Fh,
			LockOwner: 100,
			SessionID: sessKeepAlive,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK},
		})
		assert.NoError(t, err, "续期后的客户端 GetLk 必须成功")

		// 对照组：验证未续期的客户端 B (sessExpired) 句柄已经超期失效，返回 ErrBadFileHandle
		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{Ino: testDir.Ino, Fh: dirResB.Fh, Limit: 10, SessionID: &sessExpired})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "未续期的超期会话 ReadDir 应返回 ErrBadFileHandle")

		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: testDir.Ino, Fh: dirResB.Fh, Limit: 10, SessionID: &sessExpired})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "未续期的超期会话 ReadDirPlus 应返回 ErrBadFileHandle")

		assert.ErrorIs(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: testDir.Ino, Fh: dirResB.Fh, SessionID: &sessExpired}), meta.ErrBadFileHandle, "未续期的超期会话 FsyncDir 应返回 ErrBadFileHandle")
		assert.ErrorIs(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: testFile.Inode.Ino, Fh: fileResB.Fh, SessionID: &sessExpired}), meta.ErrBadFileHandle, "未续期的超期会话 Fsync 应返回 ErrBadFileHandle")

		_, err = mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       testFile.Inode.Ino,
			Fh:        fileResB.Fh,
			LockOwner: 100,
			SessionID: sessExpired,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK},
		})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "未续期的超期会话 GetLk 应返回 ErrBadFileHandle")
	})
}

// rowLockExpirySkipDeleteStore 允许在特定并发测试阶段跳过 DeleteExpiredClientSessions，
// 模拟客户端在未执行全局清扫或清扫刚过时的交错场景，防止其被测试持有的特定会话行锁阻塞。
type rowLockExpirySkipDeleteStore struct {
	db.Store
	skipDeleteExpired bool
}

type rowLockExpirySkipDeleteQuerier struct {
	db.Querier
}

func (q *rowLockExpirySkipDeleteQuerier) DeleteExpiredClientSessions(ctx context.Context) (int64, error) {
	return 0, nil
}

func (s *rowLockExpirySkipDeleteStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.ExecTxWithOptions(ctx, fn)
}

func (s *rowLockExpirySkipDeleteStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		if s.skipDeleteExpired {
			return fn(&rowLockExpirySkipDeleteQuerier{Querier: q})
		}
		return fn(q)
	}, opts...)
}

// TestRenewSession_RowLockWaitAcrossExpiry_MustNotResurrectExpiredLocksAndMustUsePostLockClock 验证 P2 / Ch1-R2：
// 当 UpsertClientSession 阻塞在 client_sessions 行锁上直至跨越租约过期线时：
// 1. 锁等待跨期恢复执行时不得死灰复燃过期锁，句柄操作必须返回 ErrBadFileHandle，底表同区间排他锁计数必须严格为 1；
// 2. 租约续期截止时间必须基于获锁后的数据库最新时钟（而非等锁前宿主机陈旧时间戳）计算。
func TestRenewSession_RowLockWaitAcrossExpiry_MustNotResurrectExpiredLocksAndMustUsePostLockClock(t *testing.T) {
	pool := setupFormalPostgresPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	baseStore := db.NewStore(pool)
	storeShort := &rowLockExpirySkipDeleteStore{Store: baseStore}
	storeLong := &rowLockExpirySkipDeleteStore{Store: baseStore}

	leaseTTL := 400 * time.Millisecond
	mgrShort := meta.New(storeShort, meta.WithSessionLeaseTTL(leaseTTL))
	mgrLong := meta.New(storeLong, meta.WithSessionLeaseTTL(5*time.Second))

	// 1. 创建独立测试虚拟磁盘
	diskName := fmt.Sprintf("disk-wait-expiry-%d", time.Now().UnixNano())
	disk, err := mgrShort.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgrShort.DeleteDisk(context.Background(), disk.DiskID)
	})

	sessS := uint64(77001)
	sessC := uint64(77002)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sessS), int64(sessC))
	})

	// 2. 会话 sessS 打开两个文件 fileI 和 fileJ
	fileIRes, err := mgrShort.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("file_i.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessS,
	})
	require.NoError(t, err)
	fileI := fileIRes.Inode.Ino
	fhI := fileIRes.Fh

	fileJRes, err := mgrShort.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("file_j.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessS,
	})
	require.NoError(t, err)
	fileJ := fileJRes.Inode.Ino
	fhJ := fileJRes.Fh

	// sessS 在 fileI 区间 [0, 100] 获取排他写锁 F_WRLCK
	err = mgrShort.SetLk(ctx, meta.SetLkParams{
		Ino:       fileI,
		Fh:        fhI,
		LockOwner: 101,
		SessionID: sessS,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   1,
		},
	})
	require.NoError(t, err)

	// 活跃会话 sessC 打开 fileI
	openResC, err := mgrLong.Open(ctx, meta.OpenParams{
		Ino:       fileI,
		Flags:     meta.O_RDWR,
		SessionID: &sessC,
	})
	require.NoError(t, err)
	fhC := openResC.Fh

	// 调整 sessS 的过期时间为即将到期（80ms 后），模拟语句启动瞬间尚未过期但即将在等锁期间过期的临界状态
	_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() + interval '80 milliseconds' WHERE session_id = $1", int64(sessS))
	require.NoError(t, err)

	// 3. 开启独立数据库连接事务 txHold 显式锁定 sessS 会话行
	holdConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer holdConn.Release()

	txHold, err := holdConn.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		_ = txHold.Rollback(context.Background())
	}()

	var heldID int64
	err = txHold.QueryRow(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sessS)).Scan(&heldID)
	require.NoError(t, err)

	// 4. 在 sessS 尚未过期前，启动协程 F 调用 Flush 尝试续期
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- mgrShort.Flush(ctx, meta.FlushParams{
			Ino:       fileJ,
			Fh:        fhJ,
			SessionID: &sessS,
		})
	}()

	// 轮询确认协程 F 已真实进入 client_sessions 行锁等待
	require.Eventually(t, func() bool {
		var count int64
		err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_locks l
			JOIN pg_stat_activity a ON l.pid = a.pid
			WHERE a.pid != pg_backend_pid()
			  AND NOT l.granted
			  AND (a.query ILIKE '%client_sessions%' OR a.wait_event_type = 'Lock')
		`).Scan(&count)
		return err == nil && count > 0
	}, 3*time.Second, 10*time.Millisecond, "协程 F 的 UpsertClientSession 必须真实进入 client_sessions 行锁等待")

	// 5. 轮询确认 sessS 在物理时钟上已真实过期
	require.Eventually(t, func() bool {
		var expired bool
		err := pool.QueryRow(ctx, "SELECT clock_timestamp() > expires_at FROM client_sessions WHERE session_id = $1", int64(sessS)).Scan(&expired)
		return err == nil && expired
	}, 3*time.Second, 10*time.Millisecond, "sessS 必须在物理时钟上真实过期")

	// sessC 在 fileI 上调用 SetLk 申请区间 [0, 100] 排他写锁
	// 跳过 DeleteExpiredClientSessions 防止其等待 txHold 锁
	storeLong.skipDeleteExpired = true
	setLkDone := make(chan error, 1)
	go func() {
		setLkDone <- mgrLong.SetLk(ctx, meta.SetLkParams{
			Ino:       fileI,
			Fh:        fhC,
			LockOwner: 303,
			SessionID: sessC,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   3,
			},
		})
	}()

	// 轮询确认 sessC 的 SetLk 已真实进入 client_sessions 行锁等待（等待 txHold 释放 sessS 的行锁）
	require.Eventually(t, func() bool {
		var count int64
		err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_locks l
			JOIN pg_stat_activity a ON l.pid = a.pid
			WHERE a.pid != pg_backend_pid()
			  AND NOT l.granted
			  AND (a.query ILIKE '%client_sessions%' OR a.wait_event_type = 'Lock')
		`).Scan(&count)
		return err == nil && count >= 2
	}, 3*time.Second, 10*time.Millisecond, "sessC 的 SetLk 与协程 F 必须真实进入 client_sessions 行锁等待")

	// 释放 txHold 行锁，让协程 F 与 sessC 的 SetLk 恢复执行
	err = txHold.Rollback(ctx)
	require.NoError(t, err)

	var setLkErr error
	select {
	case setLkErr = <-setLkDone:
	case <-time.After(5 * time.Second):
		t.Fatal("sessC SetLk 超时未返回")
	}
	storeLong.skipDeleteExpired = false
	require.NoError(t, setLkErr, "活跃会话 sessC 发现 sessS 已过期，必须成功获取排他写锁")

	var flushErr error
	select {
	case flushErr = <-flushDone:
	case <-time.After(5 * time.Second):
		t.Fatal("协程 F Flush 超时未返回")
	}

	// 6. 核心断言 ①：协程 F 的 Flush 在等锁跨期后恢复执行，必须返回 ErrBadFileHandle（拒绝续命已过期句柄）
	assert.ErrorIs(t, flushErr, meta.ErrBadFileHandle, "Flush 在等锁跨期后恢复执行时必须返回 ErrBadFileHandle")

	// 核心断言 ②：底表有效排他锁计数必须严格等于 1（仅 sessC 持有锁，绝不允许 sessS 死灰复燃造成冲突排他锁共存）
	var activeExclusiveLocks int64
	err = pool.QueryRow(ctx, `
		SELECT count(*)
		FROM file_locks l
		JOIN client_sessions s ON l.session_id = s.session_id
		WHERE l.ino = $1 AND l.lock_type = $2 AND s.expires_at > clock_timestamp()
	`, int64(fileI), int16(meta.F_WRLCK)).Scan(&activeExclusiveLocks)
	require.NoError(t, err)
	assert.Equal(t, int64(1), activeExclusiveLocks, "底表有效排他锁计数必须严格等于 1，绝不允许过期锁死灰复燃造成冲突排他锁共存")

	// 核心断言 ③（子测试）：新会话行锁等待耗时后，续期截止时间必须基于获锁后的数据库实时时钟计算
	t.Run("SubTest_RenewSessionMustUsePostLockClock", func(t *testing.T) {
		sessR := uint64(77003)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessR))
		})

		mgrTight := meta.New(storeShort, meta.WithSessionLeaseTTL(250*time.Millisecond))

		// 先激活新会话 sessR
		_, err := mgrTight.RenewSession(ctx, sessR)
		require.NoError(t, err)

		holdConn2, err := pool.Acquire(ctx)
		require.NoError(t, err)
		defer holdConn2.Release()

		txHold2, err := holdConn2.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = txHold2.Rollback(context.Background())
		}()

		var heldID2 int64
		err = txHold2.QueryRow(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sessR)).Scan(&heldID2)
		require.NoError(t, err)

		renewDone := make(chan error, 1)
		go func() {
			_, rErr := mgrTight.RenewSession(ctx, sessR)
			renewDone <- rErr
		}()

		require.Eventually(t, func() bool {
			var count int64
			err := pool.QueryRow(ctx, `
				SELECT count(*)
				FROM pg_locks l
				JOIN pg_stat_activity a ON l.pid = a.pid
				WHERE a.pid != pg_backend_pid()
				  AND NOT l.granted
				  AND (a.query ILIKE '%client_sessions%' OR a.wait_event_type = 'Lock')
			`).Scan(&count)
			return err == nil && count > 0
		}, 3*time.Second, 10*time.Millisecond, "RenewSession 必须真实进入 client_sessions 行锁等待")

		// 阻塞等待 180ms（总 leaseTTL 为 250ms，若在获锁前计算宿主机时间，恢复落库时剩余租约被吞噬至不足 70ms）
		time.Sleep(180 * time.Millisecond)

		err = txHold2.Rollback(ctx)
		require.NoError(t, err)

		select {
		case rErr := <-renewDone:
			require.NoError(t, rErr)
		case <-time.After(5 * time.Second):
			t.Fatal("RenewSession 超时未返回")
		}

		var hasAdequateLease bool
		err = pool.QueryRow(ctx, "SELECT expires_at > clock_timestamp() + interval '150 milliseconds' FROM client_sessions WHERE session_id = $1", int64(sessR)).Scan(&hasAdequateLease)
		require.NoError(t, err)
		assert.True(t, hasAdequateLease, "续期截止时间必须基于获锁后的数据库实时时钟计算，剩余有效租约必须大于 150ms")
	})
}

type lockLeaseSyncBarrierStore struct {
	db.Store
	onDeleteExpiredClientSessions    func(ctx context.Context, q db.Querier) error
	onDeleteUnreferencedOrphanInodes func(ctx context.Context, q db.Querier) ([]db.DeleteUnreferencedOrphanInodesRow, error)
}

func (s *lockLeaseSyncBarrierStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.ExecTxWithOptions(ctx, fn)
}

func (s *lockLeaseSyncBarrierStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		wrapped := &lockLeaseSyncBarrierQuerier{
			Querier: q,
			store:   s,
		}
		return fn(wrapped)
	}, opts...)
}

type lockLeaseSyncBarrierQuerier struct {
	db.Querier
	store *lockLeaseSyncBarrierStore
}

func (q *lockLeaseSyncBarrierQuerier) DeleteExpiredClientSessions(ctx context.Context) (int64, error) {
	if q.store.onDeleteExpiredClientSessions != nil {
		if err := q.store.onDeleteExpiredClientSessions(ctx, q.Querier); err != nil {
			return 0, err
		}
	}
	return q.Querier.DeleteExpiredClientSessions(ctx)
}

func (q *lockLeaseSyncBarrierQuerier) DeleteUnreferencedOrphanInodes(ctx context.Context) ([]db.DeleteUnreferencedOrphanInodesRow, error) {
	if q.store.onDeleteUnreferencedOrphanInodes != nil {
		return q.store.onDeleteUnreferencedOrphanInodes(ctx, q.Querier)
	}
	return q.Querier.DeleteUnreferencedOrphanInodes(ctx)
}

// TestSetLk_UncommittedRenewalAcrossOldExpiry_MustNotGrantConflictingExclusiveLock 验证 Wave 0 / Issue #1 / Ch1-1：
// 在 PostgreSQL READ COMMITTED 隔离级别下：
//  1. 会话 S1 持有常规文件 fileIno 的 [0, 100] 排他写锁 F_WRLCK，当前底表已提交的 client_sessions(S1).expires_at = E；
//  2. 事务 T1（RenewSession(S1)）在物理时钟 t1 < E 时执行 UpsertClientSession(S1)，更新为新租约 E' = t1 + TTL 并持有 S1 行锁，但 T1 尚未提交；
//  3. 并发事务 T2（活跃会话 S2 申请同区间 [0, 100] 排他写锁）在 t2 < E 时执行完 DeleteExpiredClientSessions（跳过 S1）；
//     随后物理时钟跨过旧截止时间 E（t3 >= E），T2 执行 ListActiveFileLocksByInoAndFlags 时因普通 SELECT 快照只能看到 T1 未提交前的旧 expires_at = E，
//     误判 S1 已过期而将其写锁静默过滤，向 S2 授予同区间排他写锁并提交；紧接着 T1 提交新租约 E'，导致底表中 S1 与 S2 同时持有同一文件同区间的冲突排他写锁。
//
// 本测试编排确定性时序屏障，断言在未修复前稳定捕获此并发写偏序（真红：SetLk(S2) 返回 nil 且底表有效排他锁数为 2）。
func TestSetLk_UncommittedRenewalAcrossOldExpiry_MustNotGrantConflictingExclusiveLock(t *testing.T) {
	pool := setupFormalPostgresPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	type lockLeaseSyncTagKey struct{}
	const (
		tagT1Renew = "T1_Renew"
		tagT2SetLk = "T2_SetLk"
	)

	t1UpsertedCh := make(chan struct{})
	t1UpsertedOnce := sync.OnceFunc(func() {
		close(t1UpsertedCh)
	})
	defer t1UpsertedOnce()

	t1AllowCommitCh := make(chan struct{})
	t1AllowCommitOnce := sync.OnceFunc(func() {
		close(t1AllowCommitCh)
	})
	defer t1AllowCommitOnce()

	t2PastOldExpiryCh := make(chan struct{})
	t2PastOldExpiryOnce := sync.OnceFunc(func() {
		close(t2PastOldExpiryCh)
	})
	defer t2PastOldExpiryOnce()

	var t2DeleteExpiredDone atomic.Bool
	var oldExpiresAt time.Time

	baseStore := db.NewStore(pool)
	barrierStore := &lockLeaseSyncBarrierStore{
		Store: baseStore,
		onDeleteExpiredClientSessions: func(ctx context.Context, q db.Querier) error {
			tag, _ := ctx.Value(lockLeaseSyncTagKey{}).(string)
			if tag == tagT2SetLk {
				t2DeleteExpiredDone.Store(true)
			}
			return nil
		},
		onDeleteUnreferencedOrphanInodes: func(ctx context.Context, q db.Querier) ([]db.DeleteUnreferencedOrphanInodesRow, error) {
			tag, _ := ctx.Value(lockLeaseSyncTagKey{}).(string)
			switch tag {
			case tagT1Renew:
				// T1: 在 UpsertClientSession(S1) 返回后、执行 DeleteUnreferencedOrphanInodes 时
				// 关闭 t1UpsertedCh 发出信号，并阻塞等待 <-t1AllowCommitCh
				t1UpsertedOnce()
				select {
				case <-t1AllowCommitCh:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			case tagT2SetLk:
				// T2: 在 DeleteExpiredClientSessions 执行完毕后，轮询 pool 等待数据库物理时钟跨过 oldExpiresAt
				for {
					var isPast bool
					err := pool.QueryRow(ctx, "SELECT clock_timestamp() >= $1", oldExpiresAt).Scan(&isPast)
					if err == nil && isPast {
						break
					}
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(2 * time.Millisecond):
					}
				}
				t2PastOldExpiryOnce()
			}
			return q.DeleteUnreferencedOrphanInodes(ctx)
		},
	}

	leaseTTL := 5 * time.Second
	mgr := meta.New(barrierStore, meta.WithSessionLeaseTTL(leaseTTL))

	// 1. 创建独立测试虚拟磁盘
	diskName := fmt.Sprintf("disk-uncommitted-renewal-%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	sess1 := uint64(77101)
	sess2 := uint64(77102)

	t.Cleanup(func() {
		t1UpsertedOnce()
		t1AllowCommitOnce()
		t2PastOldExpiryOnce()
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sess1), int64(sess2))
	})

	// 会话 S1 创建常规文件 fileIno 并获取 [0, 100] 排他写锁 F_WRLCK
	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("conflict_test.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh1 := createRes.Fh

	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 801,
		SessionID: sess1,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   1,
		},
	})
	require.NoError(t, err)

	// 会话 S2 打开同一 fileIno 获得 fh2
	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sess2,
	})
	require.NoError(t, err)
	fh2 := openRes.Fh

	// 2. 将 S1 在底表 client_sessions 的 expires_at 更新为 clock_timestamp() + interval '150 milliseconds'
	err = pool.QueryRow(ctx, `
		UPDATE client_sessions
		SET expires_at = clock_timestamp() + interval '150 milliseconds'
		WHERE session_id = $1
		RETURNING expires_at
	`, int64(sess1)).Scan(&oldExpiresAt)
	require.NoError(t, err)
	require.False(t, oldExpiresAt.IsZero())

	// 3. 编排确定性屏障
	type renewResult struct {
		expiresAt time.Time
		err       error
	}
	renewDoneCh := make(chan renewResult, 1)

	// 启动协程 1 执行 mgr.RenewSession(ctx, S1)
	ctxT1 := context.WithValue(ctx, lockLeaseSyncTagKey{}, tagT1Renew)
	go func() {
		exp, rErr := mgr.RenewSession(ctxT1, sess1)
		renewDoneCh <- renewResult{expiresAt: exp, err: rErr}
	}()

	// 等待协程 1 完成 UpsertClientSession 并在 DeleteUnreferencedOrphanInodes 处阻塞
	select {
	case <-t1UpsertedCh:
	case <-ctx.Done():
		t.Fatal("等待 T1 完成 UpsertClientSession 超时")
	}

	// 确认当前数据库物理时钟仍在 oldExpiresAt 之前
	var stillBefore bool
	err = pool.QueryRow(ctx, "SELECT clock_timestamp() < $1", oldExpiresAt).Scan(&stillBefore)
	require.NoError(t, err)
	require.True(t, stillBefore, "T1 upsert 必须在 oldExpiresAt 到期前完成")

	// 启动后台监控协程：兼容 Step B 修复后 SetLk 在进入 DeleteUnreferencedOrphanInodes 前即阻塞在 S1 行锁上的场景
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t2PastOldExpiryCh:
				return
			case <-ticker.C:
				if !t2DeleteExpiredDone.Load() {
					continue
				}
				var isPast bool
				if err := pool.QueryRow(ctx, "SELECT clock_timestamp() >= $1", oldExpiresAt).Scan(&isPast); err != nil || !isPast {
					continue
				}
				var blockedCount int64
				err := pool.QueryRow(ctx, `
					SELECT count(*)
					FROM pg_locks l
					JOIN pg_stat_activity a ON l.pid = a.pid
					WHERE a.pid != pg_backend_pid()
					  AND NOT l.granted
					  AND (a.query ILIKE '%client_sessions%' OR a.wait_event_type = 'Lock')
				`).Scan(&blockedCount)
				if err == nil && blockedCount > 0 {
					t2PastOldExpiryOnce()
					return
				}
			}
		}
	}()

	// 启动协程 2 执行 SetLk(S2)
	type setLkResult struct {
		err error
	}
	setLkDoneCh := make(chan setLkResult, 1)
	ctxT2 := context.WithValue(ctx, lockLeaseSyncTagKey{}, tagT2SetLk)
	go func() {
		sErr := mgr.SetLk(ctxT2, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 802,
			SessionID: sess2,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   2,
			},
		})
		setLkDoneCh <- setLkResult{err: sErr}
	}()

	// 等待协程 2 跨过旧过期时间
	select {
	case <-t2PastOldExpiryCh:
	case <-ctx.Done():
		t.Fatal("等待 T2 跨过旧过期时间超时")
	}

	// 主协程等待协程 2 返回，或确认其已阻塞在 client_sessions 行锁上
	var setLkRes setLkResult
	var setLkReceived bool
	select {
	case setLkRes = <-setLkDoneCh:
		setLkReceived = true
	case <-time.After(50 * time.Millisecond):
		require.Eventually(t, func() bool {
			var count int64
			err := pool.QueryRow(ctx, `
				SELECT count(*)
				FROM pg_locks l
				JOIN pg_stat_activity a ON l.pid = a.pid
				WHERE a.pid != pg_backend_pid()
				  AND NOT l.granted
				  AND (a.query ILIKE '%client_sessions%' OR a.wait_event_type = 'Lock')
			`).Scan(&count)
			return err == nil && count > 0
		}, 2*time.Second, 10*time.Millisecond, "协程 2 必须已返回或阻塞在 client_sessions 行锁")
	}

	// 放行协程 1 提交
	t1AllowCommitOnce()

	// 收集协程 1 续期结果
	var renewRes renewResult
	select {
	case renewRes = <-renewDoneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("RenewSession(S1) 超时未返回")
	}

	// 若协程 2 之前在等锁，等待其在协程 1 提交后恢复并返回
	if !setLkReceived {
		select {
		case setLkRes = <-setLkDoneCh:
			setLkReceived = true
		case <-time.After(5 * time.Second):
			t.Fatal("SetLk(S2) 等锁唤醒后超时未返回")
		}
	}

	// 4. 核心断言
	// 断言 ①：RenewSession(S1) 必须成功
	require.NoError(t, renewRes.err, "RenewSession(S1) 必须成功")
	require.False(t, renewRes.expiresAt.IsZero(), "RenewSession(S1) 返回的新过期时间不得为零")

	// 查询底表有效排他锁数量
	var activeExclusiveLocks int64
	err = pool.QueryRow(ctx, `
		SELECT count(*)
		FROM file_locks l
		JOIN client_sessions s ON l.session_id = s.session_id
		WHERE l.ino = $1 AND s.expires_at > clock_timestamp()
	`, int64(fileIno)).Scan(&activeExclusiveLocks)
	require.NoError(t, err)

	t.Logf("【实证观测】RenewSession(S1) err: %v, SetLk(S2) err: %v, 底表有效排他锁数: %d", renewRes.err, setLkRes.err, activeExclusiveLocks)

	// 断言 ②：SetLk(S2) 必须返回 meta.ErrAgain
	assert.ErrorIs(t, setLkRes.err, meta.ErrAgain, "SetLk(S2) 必须因与未提交已续期的 S1 排他写锁冲突而返回 ErrAgain")

	// 断言 ③：底表有效排他写锁数量必须严格等于 1
	assert.Equal(t, int64(1), activeExclusiveLocks, "底表有效排他锁数量必须严格等于 1，绝不允许 S1 与 S2 同时持有冲突排他锁")

	// 刚性断言：满足 prompt 要求的 require.ErrorIs(t, setLkErr, meta.ErrAgain)
	require.ErrorIs(t, setLkRes.err, meta.ErrAgain, "SetLk(S2) 必须返回 ErrAgain")
}

// TestRenewSession_RowLockWaitAcrossExpiry_PurgesStaleLocksWaitersAndHandlesOnCommit 验证 Wave 1 / Issue #3 / Ch2-2：
// 当客户端在会话 sessTarget 过期前调用 RenewSessionWithStatus(ctx, sessTarget) 时：
//  1. 首步 DeleteExpiredClientSessions 因 sessTarget 尚未过期不会将其删除；
//  2. 随后的 UpsertClientSession 因并发事务持有 client_sessions(sessTarget) 行锁而阻塞；
//  3. 在阻塞期间物理时钟跨过了 sessTarget 的旧 expires_at；
//  4. 行锁释放获锁恢复执行后，UpsertClientSession 判定 is_expired = true，原地重建会话并成功提交事务（不回滚）；
//  5. 核心验证：此时全靠 UpsertClientSession 内部的 purged_locks、purged_waiters、purged_handles 三个 CTE
//     原子清除该会话残留的旧锁、旧等待者与旧句柄，同时对照会话 sessOther 的全部状态完好保留！
func TestRenewSession_RowLockWaitAcrossExpiry_PurgesStaleLocksWaitersAndHandlesOnCommit(t *testing.T) {
	pool := setupFormalPostgresPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	baseStore := db.NewStore(pool)
	leaseTTL := 10 * time.Second
	mgr := meta.New(baseStore, meta.WithSessionLeaseTTL(leaseTTL))

	// 1. 创建独立测试虚拟磁盘
	diskName := fmt.Sprintf("disk-wait-expiry-purge-%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	sessTarget := uint64(77201)
	sessOther := uint64(77202)

	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_lock_waiters WHERE session_id IN ($1, $2)", int64(sessTarget), int64(sessOther))
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_locks WHERE session_id IN ($1, $2)", int64(sessTarget), int64(sessOther))
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM file_handles WHERE session_id IN ($1, $2)", int64(sessTarget), int64(sessOther))
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sessTarget), int64(sessOther))
	})

	// 激活目标会话与对照活跃会话（长租约 10s）
	_, err = mgr.RenewSessionWithStatus(ctx, sessTarget)
	require.NoError(t, err)
	_, err = mgr.RenewSessionWithStatus(ctx, sessOther)
	require.NoError(t, err)

	// 2. 为 sessTarget 真实建立并持久化全部三类状态，同时为 sessOther 建立对照状态：
	// (a) 句柄 (file_handles)：sessTarget 打开 fileA 与 fileB；sessOther 打开 fileA 与 fileB
	createResA, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("file_a.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessTarget,
	})
	require.NoError(t, err)
	fileA := createResA.Inode.Ino
	fhTargetA := createResA.Fh

	createResB, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("file_b.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessOther,
	})
	require.NoError(t, err)
	fileB := createResB.Inode.Ino
	fhOtherB := createResB.Fh

	openResTargetB, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileB,
		Flags:     meta.O_RDWR,
		SessionID: &sessTarget,
	})
	require.NoError(t, err)
	fhTargetB := openResTargetB.Fh

	openResOtherA, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileA,
		Flags:     meta.O_RDWR,
		SessionID: &sessOther,
	})
	require.NoError(t, err)
	fhOtherA := openResOtherA.Fh

	// (b) 已持有锁 (file_locks)：sessTarget 在 fileA [0, 100] 获取排他写锁；sessOther 在 fileB [0, 100] 获取排他写锁
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileA,
		Fh:        fhTargetA,
		LockOwner: 801,
		SessionID: sessTarget,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   1,
		},
	})
	require.NoError(t, err)

	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileB,
		Fh:        fhOtherB,
		LockOwner: 802,
		SessionID: sessOther,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   2,
		},
	})
	require.NoError(t, err)

	// (c) 等待记录 (file_lock_waiters)：让 sessTarget 在 fileB [0, 100] 上拥有一条真实 PENDING 等待记录；
	// 同时为 sessOther 在 fileA 上插入一条对照 PENDING 等待记录
	reqTarget := int64(9001)
	_, err = pool.Exec(ctx, `
		INSERT INTO file_lock_waiters (session_id, request_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid, status)
		VALUES ($1, $2, $3, $4, 801, 0, 0, 100, $5, 1, $6)
	`, int64(sessTarget), reqTarget, int64(fileB), int64(fhTargetB), int16(meta.F_WRLCK), int16(meta.WaiterStatusPending))
	require.NoError(t, err)

	reqOther := int64(9002)
	_, err = pool.Exec(ctx, `
		INSERT INTO file_lock_waiters (session_id, request_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid, status)
		VALUES ($1, $2, $3, $4, 802, 0, 0, 100, $5, 2, $6)
	`, int64(sessOther), reqOther, int64(fileA), int64(fhOtherA), int16(meta.F_WRLCK), int16(meta.WaiterStatusPending))
	require.NoError(t, err)

	// 前置断言：确认 sessTarget 在 file_handles、file_locks、file_lock_waiters 三张表中的记录数均 >= 1
	var (
		preTargetHandlesCnt int64
		preTargetLocksCnt   int64
		preTargetWaitersCnt int64
	)
	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE session_id = $1", int64(sessTarget)).Scan(&preTargetHandlesCnt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, preTargetHandlesCnt, int64(1), "前置断言: sessTarget 的 file_handles 记录数必须 >= 1")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1", int64(sessTarget)).Scan(&preTargetLocksCnt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, preTargetLocksCnt, int64(1), "前置断言: sessTarget 的 file_locks 记录数必须 >= 1")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1", int64(sessTarget)).Scan(&preTargetWaitersCnt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, preTargetWaitersCnt, int64(1), "前置断言: sessTarget 的 file_lock_waiters 记录数必须 >= 1")

	// 对照组前置断言：确认 sessOther 在三张表中的记录数均 >= 1
	var (
		preOtherHandlesCnt int64
		preOtherLocksCnt   int64
		preOtherWaitersCnt int64
	)
	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE session_id = $1", int64(sessOther)).Scan(&preOtherHandlesCnt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, preOtherHandlesCnt, int64(1), "前置断言: sessOther 的 file_handles 记录数必须 >= 1")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1", int64(sessOther)).Scan(&preOtherLocksCnt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, preOtherLocksCnt, int64(1), "前置断言: sessOther 的 file_locks 记录数必须 >= 1")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1", int64(sessOther)).Scan(&preOtherWaitersCnt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, preOtherWaitersCnt, int64(1), "前置断言: sessOther 的 file_lock_waiters 记录数必须 >= 1")

	// 3. 将 sessTarget 的 expires_at 更新为 clock_timestamp() + interval '150 milliseconds'（此时尚未过期）
	var oldExpiresAt time.Time
	err = pool.QueryRow(ctx, `
		UPDATE client_sessions
		SET expires_at = clock_timestamp() + interval '150 milliseconds'
		WHERE session_id = $1
		RETURNING expires_at
	`, int64(sessTarget)).Scan(&oldExpiresAt)
	require.NoError(t, err)
	require.False(t, oldExpiresAt.IsZero())

	// 立即开启独立连接事务 txHold 显式锁定 sessTarget 会话行
	holdConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer holdConn.Release()

	txHold, err := holdConn.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		_ = txHold.Rollback(context.Background())
	}()

	var heldID int64
	err = txHold.QueryRow(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sessTarget)).Scan(&heldID)
	require.NoError(t, err)
	require.Equal(t, int64(sessTarget), heldID)

	// 4. 在 sessTarget 过期前，启动后台协程调用 mgr.RenewSessionWithStatus(ctx, sessTarget)
	var stillBefore bool
	err = pool.QueryRow(ctx, "SELECT clock_timestamp() < $1", oldExpiresAt).Scan(&stillBefore)
	require.NoError(t, err)
	require.True(t, stillBefore, "RenewSessionWithStatus 必须在 oldExpiresAt 到期前启动")

	type renewStatusResult struct {
		res meta.RenewSessionResult
		err error
	}
	renewDone := make(chan renewStatusResult, 1)
	go func() {
		rRes, rErr := mgr.RenewSessionWithStatus(ctx, sessTarget)
		renewDone <- renewStatusResult{res: rRes, err: rErr}
	}()

	// 轮询 pg_locks 确认该协程已走过 DeleteExpiredClientSessions 并真实阻塞在 client_sessions 行锁等待上
	require.Eventually(t, func() bool {
		var count int64
		err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_locks l
			JOIN pg_stat_activity a ON l.pid = a.pid
			WHERE a.pid != pg_backend_pid()
			  AND NOT l.granted
			  AND (a.query ILIKE '%client_sessions%' OR a.wait_event_type = 'Lock')
		`).Scan(&count)
		return err == nil && count > 0
	}, 3*time.Second, 10*time.Millisecond, "RenewSessionWithStatus 必须真实进入 client_sessions 行锁等待")

	// 5. 轮询数据库确认 clock_timestamp() 已跨过 sessTarget 原 expires_at（真实过期）
	require.Eventually(t, func() bool {
		var expired bool
		err := pool.QueryRow(ctx, "SELECT clock_timestamp() >= $1", oldExpiresAt).Scan(&expired)
		return err == nil && expired
	}, 3*time.Second, 10*time.Millisecond, "物理时钟必须已跨过 sessTarget 的旧 expires_at")

	// 随即执行 txHold.Rollback(ctx) 释放行锁，等待 RenewSessionWithStatus 成功提交返回
	err = txHold.Rollback(ctx)
	require.NoError(t, err)

	var renewRes renewStatusResult
	select {
	case renewRes = <-renewDone:
	case <-time.After(5 * time.Second):
		t.Fatal("RenewSessionWithStatus 超时未返回")
	}

	// 6. 核心断言：
	// 断言 ①：RenewSessionWithStatus 返回 NoError，res.SessionRecreated == true，且底表新 expires_at > clock_timestamp()
	require.NoError(t, renewRes.err, "RenewSessionWithStatus 必须成功提交")
	assert.True(t, renewRes.res.SessionRecreated, "等锁跨期后原地重建会话，SessionRecreated 必须为 true")
	assert.True(t, renewRes.res.ExpiresAt.After(oldExpiresAt), "新 expires_at 必须更新推进")

	var (
		newExpiresAt time.Time
		isFuture     bool
	)
	err = pool.QueryRow(ctx, "SELECT expires_at, expires_at > clock_timestamp() FROM client_sessions WHERE session_id = $1", int64(sessTarget)).Scan(&newExpiresAt, &isFuture)
	require.NoError(t, err)
	assert.True(t, isFuture, "底表新 expires_at 必须大于当前物理时钟")
	assert.WithinDuration(t, renewRes.res.ExpiresAt, newExpiresAt, 50*time.Millisecond)

	// 断言 ②：直接查询底表 file_locks、file_lock_waiters、file_handles 中 WHERE session_id = sessTarget 的记录数，
	// 三张表计数均必须严格等于 0（证明 purged_locks、purged_waiters、purged_handles 三个 CTE 生效）
	var (
		targetLocksCnt   int64
		targetWaitersCnt int64
		targetHandlesCnt int64
	)
	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1", int64(sessTarget)).Scan(&targetLocksCnt)
	require.NoError(t, err)
	assert.Zero(t, targetLocksCnt, "sessTarget 的残留旧锁必须被 purged_locks 原子清除")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1", int64(sessTarget)).Scan(&targetWaitersCnt)
	require.NoError(t, err)
	assert.Zero(t, targetWaitersCnt, "sessTarget 的残留旧等待者必须被 purged_waiters 原子清除")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE session_id = $1", int64(sessTarget)).Scan(&targetHandlesCnt)
	require.NoError(t, err)
	assert.Zero(t, targetHandlesCnt, "sessTarget 的残留旧句柄必须被 purged_handles 原子清除")

	// 断言 ③：直接查询底表验证对照组 sessOther 在 client_sessions、file_handles、file_locks、file_lock_waiters 中的活跃记录均完整保留未被误删
	var (
		otherSessionsCnt int64
		otherHandlesCnt  int64
		otherLocksCnt    int64
		otherWaitersCnt  int64
	)
	err = pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1 AND expires_at > clock_timestamp()", int64(sessOther)).Scan(&otherSessionsCnt)
	require.NoError(t, err)
	assert.Equal(t, int64(1), otherSessionsCnt, "对照会话 sessOther 必须在 client_sessions 中完好活跃")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE session_id = $1", int64(sessOther)).Scan(&otherHandlesCnt)
	require.NoError(t, err)
	assert.Equal(t, int64(2), otherHandlesCnt, "对照会话 sessOther 的 2 个句柄必须完整保留")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1", int64(sessOther)).Scan(&otherLocksCnt)
	require.NoError(t, err)
	assert.Equal(t, int64(1), otherLocksCnt, "对照会话 sessOther 的已持有锁必须完整保留")

	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1", int64(sessOther)).Scan(&otherWaitersCnt)
	require.NoError(t, err)
	assert.Equal(t, int64(1), otherWaitersCnt, "对照会话 sessOther 的等待者记录必须完整保留")
}

// TestRenewSessionWithDisk_And_CloseSession_Unit 验证 RenewSessionWithDisk 与 CloseSession 的静态校验、外键错误映射与事务异常传播。
func TestRenewSessionWithDisk_And_CloseSession_Unit(t *testing.T) {
	ctx := context.Background()
	sessID := uint64(20086)
	validDiskID := "11111111-1111-7111-8111-111111111111"

	t.Run("RenewSessionWithDisk 入参静态校验拒绝非法 session_id 或非法/Nil disk_id", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		_, err := mgr.RenewSessionWithDisk(ctx, 0, &validDiskID)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		_, err = mgr.RenewSessionWithDisk(ctx, uint64(math.MaxInt64)+1, &validDiskID)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		emptyStr := ""
		_, err = mgr.RenewSessionWithDisk(ctx, sessID, &emptyStr)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		badUUID := "not-a-uuid"
		_, err = mgr.RenewSessionWithDisk(ctx, sessID, &badUUID)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		nilUUID := "00000000-0000-0000-0000-000000000000"
		_, err = mgr.RenewSessionWithDisk(ctx, sessID, &nilUUID)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("RenewSessionWithDisk 遇到 PG 23503 外键冲突映射为 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sessID) && arg.DiskID.Valid
			})).
			Return(db.ClientSession{}, &pgconn.PgError{Code: "23503"}).
			Once()

		_, err := mgr.RenewSessionWithDisk(ctx, sessID, &validDiskID)
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("CloseSession 入参静态校验与幂等/异常传播", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		require.ErrorIs(t, mgr.CloseSession(ctx, 0), meta.ErrInvalidArgument)
		require.ErrorIs(t, mgr.CloseSession(ctx, uint64(math.MaxInt64)+1), meta.ErrInvalidArgument)

		mockQuerier := dbmock.NewMockQuerier(t)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().DeleteExpiredClientSessions(mock.Anything).Return(int64(0), nil).Once()
		mockQuerier.EXPECT().DeleteClientSessionByID(mock.Anything, int64(sessID)).Return(int64(1), nil).Once()
		mockQuerier.EXPECT().DeleteUnreferencedOrphanInodes(mock.Anything).Return(nil, nil).Once()

		require.NoError(t, mgr.CloseSession(ctx, sessID))
	})
}

// TestRenewSessionWithDisk_And_CloseSession_PostgresIntegration 在真实 PostgreSQL 上验证：
// 1. RenewSessionWithDisk 首次绑定、活跃续租保留、跨期重建覆盖及不存在磁盘返回 ErrNotFound；
// 2. CloseSession 即时删除目标会话、级联清理关联 file_handles / file_locks / file_lock_waiters、回收孤儿 Inode 并退还配额、唤醒阻塞 SetLkw 锁等待者，且不影响其他活跃会话。
func TestRenewSessionWithDisk_And_CloseSession_PostgresIntegration(t *testing.T) {
	pool := setupFormalPostgresPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	baseStore := db.NewStore(pool)
	mgr := meta.New(baseStore, meta.WithSessionLeaseTTL(10*time.Second))

	diskA, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-a-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	diskB, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-b-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	sess1 := uint64(88501)
	sess2 := uint64(88502)

	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_ = mgr.DeleteDisk(cleanCtx, diskA.DiskID)
		_ = mgr.DeleteDisk(cleanCtx, diskB.DiskID)
		_, _ = pool.Exec(cleanCtx, "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sess1), int64(sess2))
	})

	// 1. 不存在的磁盘 UUID 触发外键冲突并映射为 meta.ErrNotFound
	nonExistentDisk := "99999999-9999-7999-8999-999999999999"
	_, err = mgr.RenewSessionWithDisk(ctx, sess1, &nonExistentDisk)
	require.ErrorIs(t, err, meta.ErrNotFound)

	// 2. 绑定 diskA 激活 sess1，绑定 diskB 激活 sess2
	res1, err := mgr.RenewSessionWithDisk(ctx, sess1, &diskA.DiskID)
	require.NoError(t, err)
	assert.True(t, res1.SessionRecreated)

	var gotDiskID string
	err = pool.QueryRow(ctx, "SELECT disk_id::text FROM client_sessions WHERE session_id = $1", int64(sess1)).Scan(&gotDiskID)
	require.NoError(t, err)
	assert.Equal(t, diskA.DiskID, gotDiskID)

	// 活跃状态下不传 disk_id 调用 RenewSessionWithStatus，保留原 diskA
	res1Keep, err := mgr.RenewSessionWithStatus(ctx, sess1)
	require.NoError(t, err)
	assert.False(t, res1Keep.SessionRecreated)
	err = pool.QueryRow(ctx, "SELECT disk_id::text FROM client_sessions WHERE session_id = $1", int64(sess1)).Scan(&gotDiskID)
	require.NoError(t, err)
	assert.Equal(t, diskA.DiskID, gotDiskID)

	// 激活对照会话 sess2
	_, err = mgr.RenewSessionWithDisk(ctx, sess2, &diskB.DiskID)
	require.NoError(t, err)

	// 3. 在 diskA 下由 sess1 创建常规文件并 Unlink 形成孤儿节点（但仍被 sess1 的句柄引用）
	orphanCreate, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: diskA.RootIno,
		Name:      []byte("orphan_on_close.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	orphanIno := orphanCreate.Inode.Ino
	require.NoError(t, mgr.Unlink(ctx, diskA.RootIno, []byte("orphan_on_close.bin")))

	// 4. 在 diskA 下创建共享文件 lock_target.bin，由 sess1 持有排他写锁，sess2 发起阻塞 SetLkw 等待
	lockFile, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: diskA.RootIno,
		Name:      []byte("lock_target.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	lockIno := lockFile.Inode.Ino
	fh1 := lockFile.Fh

	open2, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       lockIno,
		Flags:     meta.O_RDWR,
		SessionID: &sess2,
	})
	require.NoError(t, err)
	fh2 := open2.Fh

	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       lockIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionID: sess1,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   1,
		},
	}))

	setLkwErrCh := make(chan error, 1)
	go func() {
		setLkwErrCh <- mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       lockIno,
			Fh:        fh2,
			LockOwner: 202,
			SessionID: sess2,
			RequestID: 5001,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
				PID:   2,
			},
		})
	}()

	// 等待 sess2 进入 file_lock_waiters 排队
	require.Eventually(t, func() bool {
		var cnt int64
		_ = pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = 5001", int64(sess2)).Scan(&cnt)
		return cnt == 1
	}, 3*time.Second, 10*time.Millisecond)

	// 5. 调用 CloseSession(sess1)：必须即时删除 sess1、释放其句柄与排他锁、回收孤儿节点 orphanIno，并唤醒 sess2 获得锁
	require.NoError(t, mgr.CloseSession(ctx, sess1))

	select {
	case lkwErr := <-setLkwErrCh:
		require.NoError(t, lkwErr, "CloseSession(sess1) 释放锁后必须立即唤醒阻塞的 SetLkw(sess2) 并成功获锁")
	case <-time.After(3 * time.Second):
		t.Fatal("CloseSession 后阻塞的 SetLkw 未在预期时间内被唤醒")
	}

	// 验证底表状态：sess1 及其句柄、锁已全部清空；orphanIno 已物理回收；sess2 仍然存活且持有新锁
	var s1Cnt, s1FhCnt, s1LockCnt, orphanCnt, s2Cnt, s2LockCnt int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sess1)).Scan(&s1Cnt))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE session_id = $1", int64(sess1)).Scan(&s1FhCnt))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1", int64(sess1)).Scan(&s1LockCnt))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM inodes WHERE ino = $1", int64(orphanIno)).Scan(&orphanCnt))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sess2)).Scan(&s2Cnt))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1 AND ino = $2", int64(sess2), int64(lockIno)).Scan(&s2LockCnt))

	assert.Zero(t, s1Cnt, "sess1 必须已从 client_sessions 中物理删除")
	assert.Zero(t, s1FhCnt, "sess1 的 file_handles 必须已全部级联删除")
	assert.Zero(t, s1LockCnt, "sess1 的 file_locks 必须已全部级联删除")
	assert.Zero(t, orphanCnt, "孤儿节点 orphanIno 必须在 CloseSession 中被物理回收")
	assert.Equal(t, int64(1), s2Cnt, "对照会话 sess2 必须完好保留")
	assert.Equal(t, int64(1), s2LockCnt, "sess2 必须已成功获得 lockIno 的排他锁")

	// 6. 重复调用 CloseSession(sess1) 必须幂等成功
	require.NoError(t, mgr.CloseSession(ctx, sess1))
}

// TestSessionTouchThrottling_AllPaths_Unit 验证 STARSRC-536 (ADR-0023 Decision 1)：
// 1. DefaultWriteSessionTouchInterval 默认为 5s，且在传入较短 SessionLeaseTTL 时自动按 ttl/3 钳制节流窗口；
// 2. Create、TmpFile、Open、OpenDir 在节流窗口内仅首次请求执行 DeleteExpiredClientSessions + UpsertClientSession + DeleteUnreferencedOrphanInodes，后续请求跳过写库续期与全表扫描；
// 3. Flush、Release、ReleaseDir、SetAttr、Fallocate、Read 在会话已处于节流窗口内时跳过 DeleteExpiredClientSessions + UpsertClientSession + DeleteUnreferencedOrphanInodes。
func TestSessionTouchThrottling_AllPaths_Unit(t *testing.T) {
	require.Equal(t, 5*time.Second, meta.DefaultWriteSessionTouchInterval)

	ctx := context.Background()
	sessID := uint64(30086)
	now := time.Now().Truncate(time.Microsecond)
	sessionCreatedAt := pgtype.Timestamptz{Time: now.Add(-10 * time.Second), Valid: true}
	sessionUpdatedAt := pgtype.Timestamptz{Time: now, Valid: true}
	sessionExpiresAt := pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true}
	handleCreatedAt := pgtype.Timestamptz{Time: now.Add(-5 * time.Second), Valid: true}

	t.Run("Create/TmpFile/Open/OpenDir 节流窗口内跳过重复 UpsertClientSession 与孤儿扫描", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithWriteSessionTouchInterval(5*time.Second))

		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.AnythingOfType("func(db.Querier) error")).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				return fn(mockQuerier)
			}).
			Times(4)

		parentDir := db.Inode{
			Ino:     1,
			Mode:    int32(meta.S_IFDIR | 0o755),
			Nlink:   2,
			RootIno: 1,
		}
		regInode := db.Inode{
			Ino:     100,
			Mode:    int32(meta.S_IFREG | 0o644),
			Nlink:   1,
			RootIno: 1,
		}

		// 仅首次 Create 触发 1 次会话续期与孤儿扫描；后续 TmpFile、Open、OpenDir 在 5s 窗口内均不再调用！
		mockQuerier.EXPECT().DeleteExpiredClientSessions(mock.Anything).Return(int64(0), nil).Once()
		mockQuerier.EXPECT().UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
			return arg.SessionID == int64(sessID)
		})).Return(db.ClientSession{
			SessionID: int64(sessID),
			CreatedAt: sessionCreatedAt,
			UpdatedAt: sessionUpdatedAt,
			ExpiresAt: sessionExpiresAt,
		}, nil).Once()
		mockQuerier.EXPECT().DeleteUnreferencedOrphanInodes(mock.Anything).Return(nil, nil).Once()

		// 1. Create
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(1)).Return(parentDir, nil).Once()
		mockQuerier.EXPECT().CreateInode(mock.Anything, mock.Anything).Return(regInode, nil).Once()
		mockQuerier.EXPECT().CreateEntry(mock.Anything, mock.Anything).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(1)).Return(nil).Once()
		mockQuerier.EXPECT().CreateFileHandle(mock.Anything, mock.Anything).Return(db.FileHandle{Fh: 10, Ino: 100}, nil).Once()

		_, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("a.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 2. TmpFile（窗口内跳过续期）
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(1)).Return(parentDir, nil).Once()
		mockQuerier.EXPECT().CreateInode(mock.Anything, mock.Anything).Return(db.Inode{Ino: 101, Mode: int32(meta.S_IFREG | 0o644), Nlink: 0, RootIno: 1}, nil).Once()
		mockQuerier.EXPECT().CreateFileHandle(mock.Anything, mock.Anything).Return(db.FileHandle{Fh: 11, Ino: 101}, nil).Once()

		_, err = mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: 1,
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 3. Open（窗口内跳过续期）
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(100)).Return(regInode, nil).Once()
		mockQuerier.EXPECT().CreateFileHandle(mock.Anything, mock.Anything).Return(db.FileHandle{Fh: 12, Ino: 100}, nil).Once()

		_, err = mgr.Open(ctx, meta.OpenParams{
			Ino:       100,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 4. OpenDir（窗口内跳过续期）
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(1)).Return(parentDir, nil).Once()
		mockQuerier.EXPECT().CreateFileHandle(mock.Anything, mock.Anything).Return(db.FileHandle{Fh: 13, Ino: 1}, nil).Once()

		_, err = mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       1,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)
	})

	t.Run("Flush/SetAttr/Fallocate/Read/Release/ReleaseDir 节流窗口内跳过重复续期写库与孤儿扫描，且拦截旧代际句柄", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithWriteSessionTouchInterval(5*time.Second))

		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.AnythingOfType("func(db.Querier) error")).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				return fn(mockQuerier)
			}).
			Times(7)

		regInode := db.Inode{
			Ino:   200,
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
			Size:  0,
		}
		dirInode := db.Inode{
			Ino:   201,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}
		validFh := db.FileHandle{
			Fh:        20,
			Ino:       200,
			Flags:     int32(meta.O_RDWR),
			SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
			CreatedAt: handleCreatedAt,
		}

		// 首次 Flush 触发慢路径续期并写入内存节流状态；随后 SetAttr、Fallocate、Read、Release、ReleaseDir 均命中节流窗口！
		mockQuerier.EXPECT().DeleteExpiredClientSessions(mock.Anything).Return(int64(0), nil).Once()
		mockQuerier.EXPECT().UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
			return arg.SessionID == int64(sessID)
		})).Return(db.ClientSession{
			SessionID: int64(sessID),
			CreatedAt: sessionCreatedAt,
			UpdatedAt: sessionUpdatedAt,
			ExpiresAt: sessionExpiresAt,
		}, nil).Once()
		mockQuerier.EXPECT().DeleteUnreferencedOrphanInodes(mock.Anything).Return(nil, nil).Once()

		// 1. Flush (触发首次慢路径)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(200)).Return(regInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, mock.Anything).Return(validFh, nil).Once()
		require.NoError(t, mgr.Flush(ctx, meta.FlushParams{Ino: 200, Fh: 20, SessionID: &sessID}))

		// 2. SetAttr (窗口内跳过续期)
		newMode := uint32(0o600)
		fhVal := uint64(20)
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(200)).Return(regInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, mock.Anything).Return(validFh, nil).Once()
		mockQuerier.EXPECT().UpdateInodeAttr(mock.Anything, mock.Anything).Return(regInode, nil).Once()
		_, err := mgr.SetAttr(ctx, 200, meta.SetAttrInput{Mode: &newMode, Fh: &fhVal, SessionID: &sessID})
		require.NoError(t, err)

		// 3. Fallocate (窗口内跳过续期)
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(200)).Return(regInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, mock.Anything).Return(validFh, nil).Once()
		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, int64(200)).Return(nil).Once()
		require.NoError(t, mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       200,
			Fh:        20,
			Offset:    0,
			Length:    64,
			Mode:      meta.FallocFlKeepSize,
			SessionID: &sessID,
		}))

		// 4. Read (窗口内跳过续期)
		mockQuerier.EXPECT().GetInodeSizeAndChunksInRange(mock.Anything, mock.Anything).Return([]db.GetInodeSizeAndChunksInRangeRow{
			{Ino: 200, Mode: int32(meta.S_IFREG | 0o644), Size: 0},
		}, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, mock.Anything).Return(validFh, nil).Once()
		readResp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       200,
			Fh:        20,
			Offset:    0,
			Size:      64,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.Empty(t, readResp.Data)

		// 5. 窗口内持旧代际句柄（handle.CreatedAt < session.CreatedAt）调用 Flush 必须立即返回 ErrBadFileHandle
		staleFh := validFh
		staleFh.CreatedAt = pgtype.Timestamptz{Time: sessionCreatedAt.Time.Add(-time.Second), Valid: true}
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(200)).Return(regInode, nil).Once()
		mockQuerier.EXPECT().GetFileHandle(mock.Anything, mock.Anything).Return(staleFh, nil).Once()
		require.ErrorIs(t, mgr.Flush(ctx, meta.FlushParams{Ino: 200, Fh: 20, SessionID: &sessID}), meta.ErrBadFileHandle)

		// 6. Release (窗口内跳过续期)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(200)).Return(regInode, nil).Once()
		mockQuerier.EXPECT().DeleteFileHandle(mock.Anything, mock.Anything).Return(int64(1), nil).Once()
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: 200, Fh: 20, SessionID: &sessID}))

		// 7. ReleaseDir (窗口内跳过续期)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(201)).Return(dirInode, nil).Once()
		mockQuerier.EXPECT().DeleteFileHandle(mock.Anything, mock.Anything).Return(int64(1), nil).Once()
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: 201, Fh: 21, SessionID: &sessID}))
	})
}

// TestSessionTouchThrottling_PostgresIntegration 在真实 PostgreSQL 上验证 STARSRC-536 全部验收标准：
// 1. Create、TmpFile、Open、OpenDir、Read、Flush、Release、ReleaseDir、SetAttr、Fallocate 在 5s 节流窗口内不改写 client_sessions.updated_at；
// 2. 节流窗口内数据库中的 ClientSession 一旦过期或不存在，操作已有 FileHandle 的接口（Read、Flush、Release、ReleaseDir、SetAttr、Fallocate）立即拒绝并返回 ErrBadFileHandle，清除内存节流状态且绝不复活过期会话；
// 3. 会话过期后被相同 session_id 重建（且再次续期使 updated_at > created_at）时，旧代际句柄（handle.CreatedAt < session.CreatedAt）在节流窗口内外发起的 Read、Flush、Release、ReleaseDir、SetAttr、Fallocate 均严格返回 ErrBadFileHandle，且绝不误删新重建的活跃会话；
// 4. 短 SessionLeaseTTL（如 150ms）自动钳制节流窗口为 ttl/3，使高频操作在短租约下仍能按时续期保活。
func TestSessionTouchThrottling_PostgresIntegration(t *testing.T) {
	pool := setupFormalPostgresPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	baseStore := db.NewStore(pool)
	mgr := meta.New(baseStore)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-536-throttle-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_ = mgr.DeleteDisk(cleanCtx, disk.DiskID)
	})

	t.Run("AC1_AC2_全路径5s节流窗口内不执行UpsertClientSession写库", func(t *testing.T) {
		sessID := uint64(536001)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
		})

		// 确保无遗留过期会话或孤儿节点，使测试处于稳态
		_, err := mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)

		var initialUpdatedAt time.Time
		require.NoError(t, pool.QueryRow(ctx, "SELECT updated_at FROM client_sessions WHERE session_id = $1", int64(sessID)).Scan(&initialUpdatedAt))

		time.Sleep(10 * time.Millisecond)

		// 依次在 5s 窗口内执行 Create、TmpFile、Open、OpenDir、SetAttr、Fallocate、Read、Flush、Release、ReleaseDir
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("throttled_file.bin"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: disk.RootIno,
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       createRes.Inode.Ino,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       disk.RootIno,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		newMode := uint32(0o600)
		_, err = mgr.SetAttr(ctx, createRes.Inode.Ino, meta.SetAttrInput{
			Mode:      &newMode,
			Fh:        &createRes.Fh,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		require.NoError(t, mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       createRes.Inode.Ino,
			Fh:        createRes.Fh,
			Offset:    0,
			Length:    128,
			Mode:      0,
			SessionID: &sessID,
		}))

		readResp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       createRes.Inode.Ino,
			Fh:        createRes.Fh,
			Offset:    0,
			Size:      64,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.Len(t, readResp.Data, 64)

		require.NoError(t, mgr.Flush(ctx, meta.FlushParams{
			Ino:       createRes.Inode.Ino,
			Fh:        createRes.Fh,
			SessionID: &sessID,
		}))

		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       tmpRes.Inode.Ino,
			Fh:        tmpRes.Fh,
			SessionID: &sessID,
		}))
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       createRes.Inode.Ino,
			Fh:        openRes.Fh,
			SessionID: &sessID,
		}))
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       createRes.Inode.Ino,
			Fh:        createRes.Fh,
			SessionID: &sessID,
		}))
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       disk.RootIno,
			Fh:        openDirRes.Fh,
			SessionID: &sessID,
		}))

		var afterUpdatedAt time.Time
		require.NoError(t, pool.QueryRow(ctx, "SELECT updated_at FROM client_sessions WHERE session_id = $1", int64(sessID)).Scan(&afterUpdatedAt))
		assert.True(t, initialUpdatedAt.Equal(afterUpdatedAt), "5s 节流窗口内的全路径操作不得执行 UpsertClientSession 推进 client_sessions.updated_at")
	})

	t.Run("AC3_节流窗口内会话超期后各句柄接口立即拒绝并清理过期会话绝不复活", func(t *testing.T) {
		for idx, opName := range []string{"Read", "Flush", "Release", "ReleaseDir", "SetAttr", "Fallocate"} {
			t.Run(opName, func(t *testing.T) {
				sessID := uint64(536100 + idx)
				t.Cleanup(func() {
					_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
				})

				_, err := mgr.RenewSession(ctx, sessID)
				require.NoError(t, err)

				createRes, err := mgr.Create(ctx, meta.CreateParams{
					ParentIno: disk.RootIno,
					Name:      []byte(fmt.Sprintf("ac3_%s_%d.bin", opName, time.Now().UnixNano())),
					Mode:      meta.S_IFREG | 0o644,
					Flags:     meta.O_RDWR,
					SessionID: &sessID,
				})
				require.NoError(t, err)

				openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
					Ino:       disk.RootIno,
					Flags:     meta.O_RDONLY,
					SessionID: &sessID,
				})
				require.NoError(t, err)

				// 此时 sessID 处于 5s 内存节流窗口内，将底表会话设为已超期
				_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(sessID))
				require.NoError(t, err)

				var opErr error
				switch opName {
				case "Read":
					_, opErr = mgr.Read(ctx, &meta.ReadRequest{
						Ino:       createRes.Inode.Ino,
						Fh:        createRes.Fh,
						Offset:    0,
						Size:      16,
						SessionID: &sessID,
					})
				case "Flush":
					opErr = mgr.Flush(ctx, meta.FlushParams{
						Ino:       createRes.Inode.Ino,
						Fh:        createRes.Fh,
						SessionID: &sessID,
					})
				case "Release":
					opErr = mgr.Release(ctx, meta.ReleaseParams{
						Ino:       createRes.Inode.Ino,
						Fh:        createRes.Fh,
						SessionID: &sessID,
					})
				case "ReleaseDir":
					opErr = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
						Ino:       disk.RootIno,
						Fh:        openDirRes.Fh,
						SessionID: &sessID,
					})
				case "SetAttr":
					newMode := uint32(0o600)
					_, opErr = mgr.SetAttr(ctx, createRes.Inode.Ino, meta.SetAttrInput{
						Mode:      &newMode,
						Fh:        &createRes.Fh,
						SessionID: &sessID,
					})
				case "Fallocate":
					opErr = mgr.Fallocate(ctx, meta.FallocateParams{
						Ino:       createRes.Inode.Ino,
						Fh:        createRes.Fh,
						Offset:    0,
						Length:    64,
						Mode:      0,
						SessionID: &sessID,
					})
				}
				require.ErrorIs(t, opErr, meta.ErrBadFileHandle)

				var sessCount int64
				require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sessID)).Scan(&sessCount))
				assert.Zero(t, sessCount, "%s 遇到超期会话后必须清理过期会话且绝不复活", opName)
			})
		}
	})

	t.Run("AC4_会话过期被同名session_id重建且续期后旧代际句柄在节流窗口内外均返回ErrBadFileHandle且不误删活跃会话", func(t *testing.T) {
		sessID := uint64(536200)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
		})

		_, err := mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("ac4_stale_%d.bin", time.Now().UnixNano())),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		oldFileFh := createRes.Fh

		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       disk.RootIno,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		oldDirFh := openDirRes.Fh

		// 模拟会话被重建且再次续期（created_at 晚于旧句柄，且 updated_at > created_at），同时保留底表旧句柄以测试代际时间戳拦截
		_, err = pool.Exec(ctx, `
			UPDATE client_sessions
			SET created_at = clock_timestamp() + interval '2 seconds',
			    updated_at = clock_timestamp() + interval '3 seconds',
			    expires_at = clock_timestamp() + interval '60 seconds'
			WHERE session_id = $1
		`, int64(sessID))
		require.NoError(t, err)

		// 在节流窗口内验证 Read、Flush、SetAttr、Fallocate、Release、ReleaseDir 均拦截旧代际句柄
		_, err = mgr.Read(ctx, &meta.ReadRequest{
			Ino:       createRes.Inode.Ino,
			Fh:        oldFileFh,
			Offset:    0,
			Size:      16,
			SessionID: &sessID,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)

		require.ErrorIs(t, mgr.Flush(ctx, meta.FlushParams{
			Ino:       createRes.Inode.Ino,
			Fh:        oldFileFh,
			SessionID: &sessID,
		}), meta.ErrBadFileHandle)

		newMode := uint32(0o600)
		_, err = mgr.SetAttr(ctx, createRes.Inode.Ino, meta.SetAttrInput{
			Mode:      &newMode,
			Fh:        &oldFileFh,
			SessionID: &sessID,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)

		require.ErrorIs(t, mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       createRes.Inode.Ino,
			Fh:        oldFileFh,
			Offset:    0,
			Length:    64,
			Mode:      0,
			SessionID: &sessID,
		}), meta.ErrBadFileHandle)

		require.ErrorIs(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       createRes.Inode.Ino,
			Fh:        oldFileFh,
			SessionID: &sessID,
		}), meta.ErrBadFileHandle)

		require.ErrorIs(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       disk.RootIno,
			Fh:        oldDirFh,
			SessionID: &sessID,
		}), meta.ErrBadFileHandle)

		// 验证新重建的活跃会话绝未被误删
		var alive bool
		require.NoError(t, pool.QueryRow(ctx, "SELECT expires_at > clock_timestamp() FROM client_sessions WHERE session_id = $1", int64(sessID)).Scan(&alive))
		assert.True(t, alive, "新重建的活跃会话绝不能被旧代际句柄操作误删")
	})

	t.Run("短SessionLeaseTTL自动按三分之一钳制节流窗口确保高频请求持续保活", func(t *testing.T) {
		shortTTL := 150 * time.Millisecond
		mgrShort := meta.New(baseStore, meta.WithSessionLeaseTTL(shortTTL))

		sessID := uint64(536300)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
		})

		_, err := mgrShort.RenewSession(ctx, sessID)
		require.NoError(t, err)

		createRes, err := mgrShort.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("short_ttl_%d.bin", time.Now().UnixNano())),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 每隔 60ms（大于 ttl/3 = 50ms，小于 ttl = 150ms）调用 Flush，持续 4 轮共 240ms > 150ms，会话必须始终存活
		for i := 0; i < 4; i++ {
			time.Sleep(60 * time.Millisecond)
			require.NoError(t, mgrShort.Flush(ctx, meta.FlushParams{
				Ino:       createRes.Inode.Ino,
				Fh:        createRes.Fh,
				SessionID: &sessID,
			}))
		}
	})
}
