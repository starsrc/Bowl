package meta_test

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

		var callOrder []int64
		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     10,
				DeltaBytes:  -3072,
				DeltaInodes: -3,
			}).
			Run(func(_ context.Context, arg db.AddDiskQuotaByRootInoParams) {
				callOrder = append(callOrder, arg.RootIno)
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     20,
				DeltaBytes:  -4096,
				DeltaInodes: -2,
			}).
			Run(func(_ context.Context, arg db.AddDiskQuotaByRootInoParams) {
				callOrder = append(callOrder, arg.RootIno)
			}).
			Return(nil).
			Once()

		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)
		assert.Equal(t, expectedExpires, expiresAt)
		assert.Equal(t, []int64{10, 20}, callOrder, "必须按 root_ino 升序依次退还配额以保证锁顺序确定性")
	})

	t.Run("异常流: AddDiskQuotaByRootIno 失败中止事务并向上传播错误", func(t *testing.T) {
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

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow{
				{Ino: 201, RootIno: 10, Mode: int32(meta.S_IFREG | 0o644), Size: 4096},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, []int64{201}).
			Return([][]byte(nil), nil).
			Once()

		expectedErr := errors.New("update disk quota failed")
		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     10,
				DeltaBytes:  -4096,
				DeltaInodes: -1,
			}).
			Return(expectedErr).
			Once()

		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.True(t, expiresAt.IsZero())
	})
}

// TestRenewSession_PostgresIntegration 验证 RenewSession 在真实 PostgreSQL 环境下的功能与只读客户端保活特性。
func TestRenewSession_PostgresIntegration(t *testing.T) {
	dsn := os.Getenv("STORAGE_DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

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
