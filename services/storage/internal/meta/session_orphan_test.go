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

func TestSessionID_InputValidation_AllHandleOps(t *testing.T) {
	ctx := context.Background()
	zeroSession := uint64(0)
	overflowSession := uint64(math.MaxInt64) + 1

	for _, badSession := range []*uint64{&zeroSession, &overflowSession} {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		_, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("a.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		_, err = mgr.Open(ctx, meta.OpenParams{
			Ino:       2,
			Flags:     meta.O_RDONLY,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		err = mgr.Fsync(ctx, meta.FsyncParams{
			Ino:       2,
			Fh:        10,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		_, err = mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       1,
			Flags:     meta.O_RDONLY,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:       1,
			Fh:        10,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       1,
			Fh:        10,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:       1,
			Fh:        10,
			Limit:     10,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:       1,
			Fh:        10,
			Limit:     10,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       2,
			Fh:        10,
			Offset:    0,
			Length:    4096,
			Mode:      0,
			SessionID: badSession,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	}
}

func TestSessionBindingAndRenewal_Unit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	sessID := uint64(42)

	dirInode := db.Inode{
		Ino:        1,
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Nlink:      2,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}
	fileInode := db.Inode{
		Ino:        100,
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	expectRenewSession := func(mockQuerier *dbmock.MockQuerier, sid uint64) {
		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(nil).
			Once()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sid) && arg.ExpiresAt.Valid
			})).
			Return(db.ClientSession{SessionID: int64(sid)}, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return(nil).
			Once()
	}

	t.Run("Create 传入 SessionID 时续期会话并写入 file_handles.session_id", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(dirInode, nil).
			Once()
		mockQuerier.EXPECT().
			CreateInode(mock.Anything, mock.Anything).
			Return(fileInode, nil).
			Once()
		mockQuerier.EXPECT().
			CreateEntry(mock.Anything, db.CreateEntryParams{
				ParentIno: 1,
				Name:      []byte("created.txt"),
				Ino:       100,
			}).
			Return(nil).
			Once()
		mockQuerier.EXPECT().
			TouchInodeTimestamps(mock.Anything, int64(1)).
			Return(nil).
			Once()
		expectRenewSession(mockQuerier, sessID)
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:       100,
				Flags:     int32(meta.O_RDWR),
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
			}).
			Return(db.FileHandle{
				Fh:        501,
				Ino:       100,
				Flags:     int32(meta.O_RDWR),
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
			}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("created.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(501), res.Fh)
	})

	t.Run("Open 传入 SessionID 时续期会话并写入 file_handles.session_id", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(fileInode, nil).
			Once()
		expectRenewSession(mockQuerier, sessID)
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:       100,
				Flags:     int32(meta.O_RDONLY),
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
			}).
			Return(db.FileHandle{
				Fh:        502,
				Ino:       100,
				Flags:     int32(meta.O_RDONLY),
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       100,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(502), res.Fh)
	})

	t.Run("OpenDir 传入 SessionID 时续期会话并写入 file_handles.session_id", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(dirInode, nil).
			Once()
		expectRenewSession(mockQuerier, sessID)
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:       1,
				Flags:     int32(meta.O_RDONLY),
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
			}).
			Return(db.FileHandle{
				Fh:        503,
				Ino:       1,
				Flags:     int32(meta.O_RDONLY),
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
			}, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       1,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(503), res.Fh)
	})

	t.Run("Flush 仅传 SessionID 未传 LockOwner 时走写事务完成校验与会话续期", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(fileInode, nil).
			Once()
		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  502,
				Ino: 100,
			}).
			Return(db.FileHandle{Fh: 502, Ino: 100}, nil).
			Once()
		expectRenewSession(mockQuerier, sessID)

		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       100,
			Fh:        502,
			SessionID: &sessID,
		})
		require.NoError(t, err)
	})

	t.Run("Release 未传 FuseReleaseFlockUnlock 但传 SessionID 时仍续期会话", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(fileInode, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  502,
				Ino: 100,
			}).
			Return(int64(502), nil).
			Once()
		expectRenewSession(mockQuerier, sessID)

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:       100,
			Fh:        502,
			SessionID: &sessID,
		})
		require.NoError(t, err)
	})

	t.Run("ReleaseDir 传入 SessionID 时续期会话并触发孤儿目录回收检查", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(dirInode, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  503,
				Ino: 1,
			}).
			Return(int64(503), nil).
			Once()
		expectRenewSession(mockQuerier, sessID)

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       1,
			Fh:        503,
			SessionID: &sessID,
		})
		require.NoError(t, err)
	})

	t.Run("Fallocate 传入 SessionID 时校验句柄有效后续期会话", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(fileInode, nil).
			Once()
		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  501,
				Ino: 100,
			}).
			Return(db.FileHandle{
				Fh:    501,
				Ino:   100,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()
		expectRenewSession(mockQuerier, sessID)
		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(100)).
			Return(nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       100,
			Fh:        501,
			Offset:    0,
			Length:    512,
			Mode:      meta.FallocFlKeepSize,
			SessionID: &sessID,
		})
		require.NoError(t, err)
	})

	t.Run("renewClientSession 中 DeleteUnreferencedOrphanInodes 失败时向上传播错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(fileInode, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(nil).
			Once()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: int64(sessID)}, nil).
			Once()
		orphanErr := errors.New("orphan gc failed")
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return(orphanErr).
			Once()

		_, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       100,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, orphanErr)
	})
}

func TestSessionHandleAndOrphanGC_PostgresIntegration(t *testing.T) {
	dsn := os.Getenv("STORAGE_DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	s1 := uint64(980001)
	s2 := uint64(980002)
	s3 := uint64(980003)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2, $3)", int64(s1), int64(s2), int64(s3))
	}()

	// AC1: 向后兼容无会话模式（SessionID == nil）
	t.Run("AC1_无会话模式句柄永不过期且兼容全量操作", func(t *testing.T) {
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("ac1_legacy.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		ino := createRes.Inode.Ino
		fh := createRes.Fh
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(ino))
		}()

		fhRow, err := store.GetFileHandle(ctx, db.GetFileHandleParams{Fh: int64(fh), Ino: int64(ino)})
		require.NoError(t, err)
		assert.False(t, fhRow.SessionID.Valid, "未传 SessionID 时 session_id 必须为 NULL")

		require.NoError(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: fh, Datasync: true}))
		require.NoError(t, mgr.Fallocate(ctx, meta.FallocateParams{Ino: ino, Fh: fh, Offset: 0, Length: 128, Mode: 0}))
		require.NoError(t, mgr.Flush(ctx, meta.FlushParams{Ino: ino, Fh: fh}))
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: ino, Fh: fh}))
		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("ac1_legacy.txt")))
	})

	// AC2: 会话过期后句柄自动失效，且孤儿节点仅剩过期句柄时拒绝再次 Open / OpenDir
	t.Run("AC2_会话过期后读写与释放操作返回ErrBadFileHandle且孤儿禁止重新打开", func(t *testing.T) {
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("ac2_file.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &s1,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		fileFh := createRes.Fh
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(fileIno))
		}()

		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("ac2_dir"),
			Mode:      meta.S_IFDIR | 0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		dirIno := subDir.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(dirIno))
		}()

		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       dirIno,
			Flags:     meta.O_RDONLY,
			SessionID: &s1,
		})
		require.NoError(t, err)
		dirFh := openDirRes.Fh

		// 将文件和目录均变为 nlink == 0 的 Orphan Inode
		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("ac2_file.txt")))
		require.NoError(t, mgr.Rmdir(ctx, rootIno, []byte("ac2_dir")))

		// 会话 S1 尚未过期时，Orphan Inode 仍具有活跃句柄，允许通过 /proc/self/fd 语义重新 Open / OpenDir
		reopenRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDONLY,
			SessionID: &s1,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        reopenRes.Fh,
			SessionID: &s1,
		}))

		// 令会话 S1 过期（还未物理删除 client_sessions 行）
		_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(s1))
		require.NoError(t, err)

		// 1. 持已过期会话句柄执行只读/读写操作均返回 ErrBadFileHandle (EBADF)
		assert.ErrorIs(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: fileIno, Fh: fileFh, SessionID: &s1}), meta.ErrBadFileHandle)
		assert.ErrorIs(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: dirIno, Fh: dirFh, SessionID: &s1}), meta.ErrBadFileHandle)
		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{Ino: dirIno, Fh: dirFh, Limit: 10, SessionID: &s1})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: dirIno, Fh: dirFh, Limit: 10, SessionID: &s1})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = mgr.GetLk(ctx, meta.GetLkParams{
			Ino:       fileIno,
			Fh:        fileFh,
			SessionID: s1,
			LockOwner: 1,
			LkFlags:   0,
			Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 10},
		})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle)
		assert.ErrorIs(t, mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       fileIno,
			Fh:        fileFh,
			Offset:    0,
			Length:    64,
			Mode:      0,
			SessionID: &s1,
		}), meta.ErrBadFileHandle)
		assert.ErrorIs(t, mgr.Flush(ctx, meta.FlushParams{Ino: fileIno, Fh: fileFh, SessionID: &s1}), meta.ErrBadFileHandle)
		assert.ErrorIs(t, mgr.Release(ctx, meta.ReleaseParams{Ino: fileIno, Fh: fileFh, SessionID: &s1}), meta.ErrBadFileHandle)
		assert.ErrorIs(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: dirIno, Fh: dirFh, SessionID: &s1}), meta.ErrBadFileHandle)

		// 2. 尝试对仅剩过期句柄的 Orphan Inode 执行 Open / OpenDir 必须返回 ErrNotFound (ENOENT)
		_, err = mgr.Open(ctx, meta.OpenParams{Ino: fileIno, Flags: meta.O_RDONLY})
		assert.ErrorIs(t, err, meta.ErrNotFound)
		_, err = mgr.OpenDir(ctx, meta.OpenDirParams{Ino: dirIno, Flags: meta.O_RDONLY})
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	// AC3: 客户端崩溃后 Orphan Inode 物理回收（文件与目录）
	t.Run("AC3_客户端S1租约超期后由S2写请求级联清理句柄并物理回收孤儿文件与目录", func(t *testing.T) {
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("ac3_orphan_file.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &s1,
		})
		require.NoError(t, err)
		orphanFileIno := createRes.Inode.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(orphanFileIno))
		}()

		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("ac3_orphan_dir"),
			Mode:      meta.S_IFDIR | 0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		orphanDirIno := subDir.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(orphanDirIno))
		}()

		_, err = mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       orphanDirIno,
			Flags:     meta.O_RDONLY,
			SessionID: &s1,
		})
		require.NoError(t, err)

		// Unlink 文件与 Rmdir 目录，使其成为仅被 S1 持有的 Orphan Inode
		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("ac3_orphan_file.txt")))
		require.NoError(t, mgr.Rmdir(ctx, rootIno, []byte("ac3_orphan_dir")))

		// S1 崩溃导致租约过期
		_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(s1))
		require.NoError(t, err)

		// S2 发起携带 SessionID 的写操作（例如 Open 根目录下的新文件或 OpenDir 根目录）
		s2DirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       rootIno,
			Flags:     meta.O_RDONLY,
			SessionID: &s2,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.ReleaseDir(context.Background(), meta.ReleaseDirParams{Ino: rootIno, Fh: s2DirRes.Fh, SessionID: &s2})
		}()

		// 断言孤儿文件与孤儿目录均已被物理删除
		_, err = store.GetInode(ctx, int64(orphanFileIno))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "孤儿文件 Inode 应被物理回收")
		_, err = store.GetInode(ctx, int64(orphanDirIno))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "孤儿目录 Inode 应被物理回收")
	})

	// AC4: 多会话共享 Orphan Inode 的引用计数保护
	t.Run("AC4_多会话共享孤儿节点时仅清理超期会话S1不误删活跃会话S2或NULL句柄持有的孤儿节点", func(t *testing.T) {
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("ac4_shared_orphan.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &s1,
		})
		require.NoError(t, err)
		sharedIno := createRes.Inode.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(sharedIno))
		}()

		// 会话 S2 也打开该文件
		openS2, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       sharedIno,
			Flags:     meta.O_RDONLY,
			SessionID: &s2,
		})
		require.NoError(t, err)

		// 同时创建一个無会话（session_id IS NULL）句柄
		openNull, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   sharedIno,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err)

		// Unlink 使其变为 nlink == 0 的 Orphan Inode
		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("ac4_shared_orphan.txt")))

		// S1 租约过期，S2 发起 Flush 触发清理
		_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(s1))
		require.NoError(t, err)

		require.NoError(t, mgr.Flush(ctx, meta.FlushParams{
			Ino:       sharedIno,
			Fh:        openS2.Fh,
			SessionID: &s2,
		}))

		// S2 仍在活跃租约内，且存在 NULL 会话句柄，Orphan Inode 必须保留
		inodeAttr, err := mgr.GetAttr(ctx, sharedIno)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), inodeAttr.Attr.Nlink)

		// S2 正常释放句柄，由于 NULL 会话句柄仍在，Orphan Inode 仍保留
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       sharedIno,
			Fh:        openS2.Fh,
			SessionID: &s2,
		}))
		_, err = mgr.GetAttr(ctx, sharedIno)
		require.NoError(t, err)

		// 释放最后一个 NULL 会话句柄后，Orphan Inode 被立即物理回收
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino: sharedIno,
			Fh:  openNull.Fh,
		}))
		_, err = mgr.GetAttr(ctx, sharedIno)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	// AC5: 并发锁竞争下 SKIP LOCKED 无死锁与非阻塞保证
	t.Run("AC5_并发事务持有孤儿Inode排他锁时renewClientSession通过SKIP_LOCKED非阻塞跳过", func(t *testing.T) {
		// 创建两个 Orphan Inode：lockedOrphanIno 将被并发事务锁定，freeOrphanIno 未被锁定
		lockedRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("ac5_locked_orphan.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &s1,
		})
		require.NoError(t, err)
		lockedOrphanIno := lockedRes.Inode.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(lockedOrphanIno))
		}()

		freeRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("ac5_free_orphan.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &s1,
		})
		require.NoError(t, err)
		freeOrphanIno := freeRes.Inode.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(freeOrphanIno))
		}()

		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("ac5_locked_orphan.txt")))
		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("ac5_free_orphan.txt")))

		// 令 S1 过期
		_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(s1))
		require.NoError(t, err)

		// 开启独立事务 txA 对 lockedOrphanIno 持有 FOR UPDATE 行级排他锁
		txA, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = txA.Rollback(context.Background())
		}()

		var lockedIno int64
		err = txA.QueryRow(ctx, "SELECT ino FROM inodes WHERE ino = $1 FOR UPDATE", int64(lockedOrphanIno)).Scan(&lockedIno)
		require.NoError(t, err)

		// 在 txA 未提交期间，客户端 S3 执行带 SessionID 的 OpenDir，触发 renewClientSession -> DeleteUnreferencedOrphanInodes
		doneCh := make(chan error, 1)
		go func() {
			dirRes, oErr := mgr.OpenDir(ctx, meta.OpenDirParams{
				Ino:       rootIno,
				Flags:     meta.O_RDONLY,
				SessionID: &s3,
			})
			if oErr != nil {
				doneCh <- oErr
				return
			}
			_ = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
				Ino:       rootIno,
				Fh:        dirRes.Fh,
				SessionID: &s3,
			})
			doneCh <- nil
		}()

		select {
		case opErr := <-doneCh:
			require.NoError(t, opErr, "SKIP LOCKED 应保证并发孤儿回收不被 txA 阻塞或触发死锁")
		case <-time.After(2 * time.Second):
			t.Fatal("renewClientSession 被并发事务持有的孤儿 Inode 行锁阻塞超时")
		}

		// 未被锁定的 freeOrphanIno 已被立即物理回收
		_, err = store.GetInode(ctx, int64(freeOrphanIno))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "未加锁的孤儿节点应在同一轮收集中被物理回收")

		// 释放 txA 锁后，lockedOrphanIno 仍在，下一次会话续期即可将其回收
		require.NoError(t, txA.Rollback(ctx))
		_, err = store.GetInode(ctx, int64(lockedOrphanIno))
		require.NoError(t, err, "被 txA 锁定的孤儿节点在跳过后仍暂留")

		dirRes2, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       rootIno,
			Flags:     meta.O_RDONLY,
			SessionID: &s3,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       rootIno,
			Fh:        dirRes2.Fh,
			SessionID: &s3,
		}))

		_, err = store.GetInode(ctx, int64(lockedOrphanIno))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "txA 释放锁后下一轮会话续期应回收该孤儿节点")
	})
}
