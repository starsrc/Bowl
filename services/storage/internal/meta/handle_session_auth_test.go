package meta_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// TestHandleSessionAuth_PostgresRegression 针对 STARSRC-237 规范进行全链路回归验证：
// 1. 跨会话隔离：S2 访问 S1 句柄，全 11 个接口均返回 ErrBadFileHandle；
// 2. 有会话访问无会话句柄隔离：会话请求访问无会话句柄返回 ErrBadFileHandle；
// 3. 无会话访问有会话句柄隔离：无会话请求访问有会话句柄返回 ErrBadFileHandle；
// 4. 会话过期防“诈尸自救”：过期会话句柄写操作直接被拒，且会话不会被重新激活续期；
// 5. Release / ReleaseDir 严格会话匹配，绝不误删其他会话合法句柄。
func TestHandleSessionAuth_PostgresRegression(t *testing.T) {
	dsn := os.Getenv("BOWL_TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	// 创建测试根目录与测试文件/子目录
	rootDir, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	defer func() {
		_ = store.DeleteInode(context.Background(), rootDir.Ino)
	}()

	s1 := uint64(960001)
	s2 := uint64(960002)
	sExpired := uint64(960099)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2, $3)", int64(s1), int64(s2), int64(sExpired))
	}()

	// 1. 创建属于 s1 的文件句柄与目录句柄
	createRes1, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: uint64(rootDir.Ino),
		Name:      []byte("auth_test_file.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &s1,
	})
	require.NoError(t, err)
	fileIno := createRes1.Inode.Ino
	fhS1 := createRes1.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(fileIno))
	}()

	mkdirRes, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: uint64(rootDir.Ino),
		Name:      []byte("auth_test_dir"),
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	dirIno := mkdirRes.Ino
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(dirIno))
	}()

	openDirS1, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       dirIno,
		Flags:     meta.O_RDONLY,
		SessionID: &s1,
	})
	require.NoError(t, err)
	dirFhS1 := openDirS1.Fh

	// 2. 创建无会话的文件句柄与目录句柄
	openNoSess, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   fileIno,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fhNoSess := openNoSess.Fh

	openDirNoSess, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:   dirIno,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)
	dirFhNoSess := openDirNoSess.Fh

	t.Run("跨会话隔离: S2 访问 S1 句柄全 11 个接口均返回 ErrBadFileHandle", func(t *testing.T) {
		// 1. Flush
		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       fileIno,
			Fh:        fhS1,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "Flush with cross-session handle must return ErrBadFileHandle")

		// 2. Fsync
		err = mgr.Fsync(ctx, meta.FsyncParams{
			Ino:       fileIno,
			Fh:        fhS1,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "Fsync with cross-session handle must return ErrBadFileHandle")

		// 3. FsyncDir
		err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:       dirIno,
			Fh:        dirFhS1,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "FsyncDir with cross-session handle must return ErrBadFileHandle")

		// 4. ReadDir
		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:       dirIno,
			Fh:        dirFhS1,
			Limit:     10,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "ReadDir with cross-session handle must return ErrBadFileHandle")

		// 5. ReadDirPlus
		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:       dirIno,
			Fh:        dirFhS1,
			Limit:     10,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "ReadDirPlus with cross-session handle must return ErrBadFileHandle")

		// 6. Fallocate
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       fileIno,
			Fh:        fhS1,
			Length:    128,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "Fallocate with cross-session handle must return ErrBadFileHandle")

		// 7. Release
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        fhS1,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "Release with cross-session handle must return ErrBadFileHandle")

		// 8. ReleaseDir
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       dirIno,
			Fh:        dirFhS1,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "ReleaseDir with cross-session handle must return ErrBadFileHandle")

		// 9. GetLk
		_, err = mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       fileIno,
			Fh:        fhS1,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK},
			SessionID: s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "GetLk with cross-session handle must return ErrBadFileHandle")

		// 10. SetLk
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhS1,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
			SessionID: s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "SetLk with cross-session handle must return ErrBadFileHandle")

		// 11. SetLkw
		err = mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhS1,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
			SessionID: s2,
			RequestID: 7001,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle), "SetLkw with cross-session handle must return ErrBadFileHandle")
	})

	t.Run("有会话请求访问无会话句柄隔离: 全 11 个接口均返回 ErrBadFileHandle", func(t *testing.T) {
		// 1. Flush
		err := mgr.Flush(ctx, meta.FlushParams{Ino: fileIno, Fh: fhNoSess, SessionID: &s1})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 2. Fsync
		err = mgr.Fsync(ctx, meta.FsyncParams{Ino: fileIno, Fh: fhNoSess, SessionID: &s1})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 3. FsyncDir
		err = mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: dirIno, Fh: dirFhNoSess, SessionID: &s1})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 4. ReadDir
		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{Ino: dirIno, Fh: dirFhNoSess, Limit: 10, SessionID: &s1})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 5. ReadDirPlus
		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: dirIno, Fh: dirFhNoSess, Limit: 10, SessionID: &s1})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 6. Fallocate
		err = mgr.Fallocate(ctx, meta.FallocateParams{Ino: fileIno, Fh: fhNoSess, Length: 10, SessionID: &s1})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 7. Release
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: fileIno, Fh: fhNoSess, SessionID: &s1})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 8. ReleaseDir
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: dirIno, Fh: dirFhNoSess, SessionID: &s1})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 9. GetLk
		_, err = mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       fileIno,
			Fh:        fhNoSess,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK},
			SessionID: s1,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 10. SetLk
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhNoSess,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
			SessionID: s1,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 11. SetLkw
		err = mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhNoSess,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
			SessionID: s1,
			RequestID: 7002,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("无会话请求访问有会话句柄隔离: 支持无会话的 8 个接口均返回 ErrBadFileHandle", func(t *testing.T) {
		// 1. Flush
		err := mgr.Flush(ctx, meta.FlushParams{Ino: fileIno, Fh: fhS1, SessionID: nil})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 2. Fsync
		err = mgr.Fsync(ctx, meta.FsyncParams{Ino: fileIno, Fh: fhS1, SessionID: nil})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 3. FsyncDir
		err = mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: dirIno, Fh: dirFhS1, SessionID: nil})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 4. ReadDir
		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{Ino: dirIno, Fh: dirFhS1, Limit: 10, SessionID: nil})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 5. ReadDirPlus
		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: dirIno, Fh: dirFhS1, Limit: 10, SessionID: nil})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 6. Fallocate
		err = mgr.Fallocate(ctx, meta.FallocateParams{Ino: fileIno, Fh: fhS1, Length: 10, SessionID: nil})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 7. Release
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: fileIno, Fh: fhS1, SessionID: nil})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 8. ReleaseDir
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: dirIno, Fh: dirFhS1, SessionID: nil})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("会话过期后不可诈尸自救: 写操作先校验未过期，已过期直接返回 ErrBadFileHandle 且不续期", func(t *testing.T) {
		pastTime := time.Now().Add(-1 * time.Hour)
		_, err := store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
			SessionID: int64(sExpired),
			ExpiresAt: pgtype.Timestamptz{Time: pastTime, Valid: true},
		})
		require.NoError(t, err)

		// 直接插入属于 sExpired 的句柄
		fhExpiredRow, err := store.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       int64(fileIno),
			Flags:     int32(meta.O_RDWR),
			SessionID: pgtype.Int8{Int64: int64(sExpired), Valid: true},
		})
		require.NoError(t, err)
		fhExpired := uint64(fhExpiredRow.Fh)

		dirFhExpiredRow, err := store.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       int64(dirIno),
			Flags:     int32(meta.O_RDONLY),
			SessionID: pgtype.Int8{Int64: int64(sExpired), Valid: true},
		})
		require.NoError(t, err)
		dirFhExpired := uint64(dirFhExpiredRow.Fh)

		// 1. Flush 写操作拦截
		err = mgr.Flush(ctx, meta.FlushParams{
			Ino:       fileIno,
			Fh:        fhExpired,
			SessionID: &sExpired,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 2. Fallocate 写操作拦截
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       fileIno,
			Fh:        fhExpired,
			Length:    64,
			SessionID: &sExpired,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 3. SetLk 写操作拦截
		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fhExpired,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
			SessionID: sExpired,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 4. SetLkw 写操作拦截
		err = mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fhExpired,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 1001},
			SessionID: sExpired,
			RequestID: 7003,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 5. Release / ReleaseDir 写操作拦截
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        fhExpired,
			SessionID: &sExpired,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       dirIno,
			Fh:        dirFhExpired,
			SessionID: &sExpired,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 6. 读操作也同样拦截
		err = mgr.Fsync(ctx, meta.FsyncParams{Ino: fileIno, Fh: fhExpired, SessionID: &sExpired})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		err = mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: dirIno, Fh: dirFhExpired, SessionID: &sExpired})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{Ino: dirIno, Fh: dirFhExpired, Limit: 10, SessionID: &sExpired})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: dirIno, Fh: dirFhExpired, Limit: 10, SessionID: &sExpired})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		_, err = mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       fileIno,
			Fh:        fhExpired,
			LockOwner: 1001,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK},
			SessionID: sExpired,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 7. 严防诈尸：校验 client_sessions 中 sExpired 绝未被更新为未来时间（或者已被惰性清理）
		var expTimestamptz pgtype.Timestamptz
		qErr := pool.QueryRow(ctx, "SELECT expires_at FROM client_sessions WHERE session_id = $1", int64(sExpired)).Scan(&expTimestamptz)
		if qErr == nil {
			assert.True(t, expTimestamptz.Time.Before(time.Now()), "expired session must not be renewed into the future")
		} else {
			assert.True(t, errors.Is(qErr, pgx.ErrNoRows), "expired session may be evicted, but must not be revived")
		}
	})

	t.Run("Release / ReleaseDir 绝不误删其他会话句柄", func(t *testing.T) {
		// S2 尝试 Release S1 的文件句柄
		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        fhS1,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 验证 fhS1 依然存在于数据库中
		var count int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_handles WHERE fh = $1", int64(fhS1)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "fhS1 must not be deleted by cross-session release")

		// S2 尝试 ReleaseDir S1 的目录句柄
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       dirIno,
			Fh:        dirFhS1,
			SessionID: &s2,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))

		// 验证 dirFhS1 依然存在于数据库中
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_handles WHERE fh = $1", int64(dirFhS1)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "dirFhS1 must not be deleted by cross-session release_dir")

		// 拥有者 S1 正确释放句柄
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        fhS1,
			SessionID: &s1,
		})
		require.NoError(t, err)

		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_handles WHERE fh = $1", int64(fhS1)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "fhS1 must be deleted by its rightful owner")

		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       dirIno,
			Fh:        dirFhS1,
			SessionID: &s1,
		})
		require.NoError(t, err)

		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_handles WHERE fh = $1", int64(dirFhS1)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "dirFhS1 must be deleted by its rightful owner")
	})
}
