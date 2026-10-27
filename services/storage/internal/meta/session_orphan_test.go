package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
		now := time.Now()
		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sid) && arg.ExpiresAt.Valid
			})).
			Return(db.ClientSession{
				SessionID: int64(sid),
				ExpiresAt: pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true},
			}, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
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
				Fh:        502,
				Ino:       100,
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
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
				Fh:        502,
				Ino:       100,
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
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
				Fh:        503,
				Ino:       1,
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
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
				Fh:        501,
				Ino:       100,
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
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
			Return(int64(0), nil).
			Once()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: int64(sessID)}, nil).
			Once()
		orphanErr := errors.New("orphan gc failed")
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), orphanErr).
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
	dsn := getStorageTestDSN()

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

	// AC6: 客户端会话过期后，由另一会话续期触发批量清理超期孤儿节点并按 root_ino 汇总退还配额
	t.Run("AC6_会话超时批量回收孤儿节点按root_ino汇总退还磁盘配额", func(t *testing.T) {
		diskName := fmt.Sprintf("test_orphan_quota_reclaim_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		// 初始配额校验：仅含根目录 Inode
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		sess1 := uint64(981001)
		sess2 := uint64(981002)
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sess1), int64(sess2))
		}()

		// 1. 客户端会话 1 在磁盘下创建 TmpFile 并 SetAttr(size=8192)
		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: disk.RootIno,
			Mode:      0o644,
			Flags:     meta.O_RDWR,
			UID:       1000,
			GID:       1000,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		tmpIno := tmpRes.Inode.Ino
		tmpFh := tmpRes.Fh

		size8192 := uint64(8192)
		_, err = mgr.SetAttr(ctx, tmpIno, meta.SetAttrInput{
			Size:      &size8192,
			Fh:        &tmpFh,
			SessionID: &sess1,
		})
		require.NoError(t, err)

		// 2. 客户端会话 1 创建常规文件 Open -> SetAttr(size=4096) -> Unlink
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan_file.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino
		// 释放 Create 默认分配的无会话句柄
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: fileIno, Fh: createRes.Fh}))

		openFileRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		fileFh := openFileRes.Fh

		size4096 := uint64(4096)
		_, err = mgr.SetAttr(ctx, fileIno, meta.SetAttrInput{
			Size:      &size4096,
			Fh:        &fileFh,
			SessionID: &sess1,
		})
		require.NoError(t, err)

		err = mgr.Unlink(ctx, disk.RootIno, []byte("orphan_file.txt"))
		require.NoError(t, err)

		// 3. 客户端会话 1 创建目录 OpenDir -> Rmdir
		dirInode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		dirIno := dirInode.Ino

		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       dirIno,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		_ = openDirRes.Fh

		err = mgr.Rmdir(ctx, disk.RootIno, []byte("orphan_dir"))
		require.NoError(t, err)

		// 验证当前磁盘配额：used_bytes == 12288, used_inodes == 4 (含根目录)
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(12288), d.UsedBytes, "当前 used_bytes 应为 8192 + 4096 == 12288")
		assert.Equal(t, uint64(4), d.UsedInodes, "当前 used_inodes 应为 4 (根目录 + TmpFile + 常规文件 + 目录)")

		// 4. 模拟客户端会话 1 过期
		_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = $1 WHERE session_id = $2", time.Now().Add(-10*time.Second), int64(sess1))
		require.NoError(t, err)

		// 5. 客户端会话 2 调用 RenewSession 触发超期会话清理与孤儿节点批量物理回收
		_, err = mgr.RenewSession(ctx, sess2)
		require.NoError(t, err)

		// 6. 验证所属磁盘配额自动恢复为 used_bytes == 0, used_inodes == 1 (仅剩根目录)
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), d.UsedBytes, "孤儿节点批量物理回收后 used_bytes 必须自动恢复为 0")
		assert.Equal(t, uint64(1), d.UsedInodes, "孤儿节点批量物理回收后 used_inodes 必须自动恢复为 1 (仅剩根目录)")

		// 7. 验证 3 个孤儿节点均已被物理删除
		_, err = mgr.GetAttr(ctx, tmpIno)
		assert.ErrorIs(t, err, meta.ErrNotFound, "TmpFile 应被物理删除")
		_, err = mgr.GetAttr(ctx, fileIno)
		assert.ErrorIs(t, err, meta.ErrNotFound, "孤儿常规文件应被物理删除")
		_, err = mgr.GetAttr(ctx, dirIno)
		assert.ErrorIs(t, err, meta.ErrNotFound, "孤儿目录应被物理删除")
	})
}

// TestSession_ExpiredLockRevivedByRenewSession 验证 G-02：
// 会话过期后锁被忽略并授予新客户端，原会话续期/重建绝不得复活旧锁导致双写锁并存。
func TestSession_ExpiredLockRevivedByRenewSession(t *testing.T) {
	dsn := getStorageTestDSN()

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

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-g02-lock-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sessA := uint64(888001)
	sessB := uint64(888002)
	_, err = mgr.RenewSession(ctx, sessA)
	require.NoError(t, err)
	_, err = mgr.RenewSession(ctx, sessB)
	require.NoError(t, err)

	createResA, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("revived_lock.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	fileIno := createResA.Inode.Ino
	fhA := createResA.Fh

	openResB, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sessB,
	})
	require.NoError(t, err)
	fhB := openResB.Fh

	// 1. 会话 A 加上排他写锁 [0, 100]
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: 601,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   601,
		},
		LkFlags:   0,
		SessionID: sessA,
	})
	require.NoError(t, err)

	// 2. 模拟会话 A 过期 (将 expires_at 改为过去 10 秒)
	_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = $1 WHERE session_id = $2", time.Now().Add(-10*time.Second), int64(sessA))
	require.NoError(t, err)

	// 3. 会话 B 申请同一区间排他写锁：由于会话 A 已过期，会话 B 成功获锁
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fhB,
		LockOwner: 602,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   602,
		},
		LkFlags:   0,
		SessionID: sessB,
	})
	require.NoError(t, err, "会话 A 过期后会话 B 应能成功获锁")

	// 4. 会话 A 尝试续期/重建
	_, err = mgr.RenewSession(ctx, sessA)
	require.NoError(t, err)

	// 5. 核心断言：会话 A 续期后，绝不能使之前未被级联删除的旧锁复活，导致同一区间同时存在两把有效排他锁！
	var totalLocks int64
	err = pool.QueryRow(ctx, `
		SELECT count(*)
		FROM file_locks l
		JOIN client_sessions s ON l.session_id = s.session_id
		WHERE l.ino = $1 AND s.expires_at > clock_timestamp() AND l.lock_type = $2
	`, fileIno, meta.F_WRLCK).Scan(&totalLocks)
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalLocks, "G-02: 会话 A 续期后绝不能复活已失效旧锁，有效排他写锁必须唯一")
}

// TestSession_ExpiredHandleOperationsReturnBadFileHandle 验证 G-02 与 G-11：
// 为每个被测操作（Release、ReleaseDir、Flush、Fallocate、SetLk、SetLkw）分别创建独立会话与对应句柄，
// 消除前序操作清理会话与级联句柄的状态污染；
// 等待租约过期后，断言调用前过期会话与句柄在表中存在，调用时返回 ErrBadFileHandle，
// 调用后确认该过期会话与句柄已被清理，且绝未被重新插入复活为活跃会话。
func TestSession_ExpiredHandleOperationsReturnBadFileHandle(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-g02-handles-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	// 辅助函数：断言调用前过期会话与句柄在数据库表中仍然存在（尚未被清理）
	assertSessionAndHandleExistBefore := func(t *testing.T, sessID uint64, ino, fh uint64) {
		t.Helper()
		var sessExists, fhExists bool
		err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM client_sessions WHERE session_id = $1)", int64(sessID)).Scan(&sessExists)
		require.NoError(t, err)
		require.True(t, sessExists, "调用前该过期会话在表中应仍然存在（尚未被清理）")

		err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM file_handles WHERE fh = $1 AND ino = $2)", int64(fh), int64(ino)).Scan(&fhExists)
		require.NoError(t, err)
		require.True(t, fhExists, "调用前该句柄在表中应仍然存在（尚未被清理）")
	}

	// 辅助函数：断言调用后过期会话与句柄已被清除（或绝未被重新插入复活为活跃会话）
	assertSessionAndHandleCleanedAfter := func(t *testing.T, sessID uint64, ino, fh uint64, opName string) {
		t.Helper()
		if opName == "Release" || opName == "ReleaseDir" {
			var sessExists, fhExists bool
			err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM client_sessions WHERE session_id = $1)", int64(sessID)).Scan(&sessExists)
			require.NoError(t, err)
			assert.False(t, sessExists, "持已过期句柄调用 %s 绝不能复活会话，过期会话应已被彻底清理", opName)

			err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM file_handles WHERE fh = $1 AND ino = $2)", int64(fh), int64(ino)).Scan(&fhExists)
			require.NoError(t, err)
			assert.False(t, fhExists, "持已过期句柄调用 %s 后，该句柄应已被彻底清除（级联删除）", opName)
		} else {
			var exp pgtype.Timestamptz
			err := pool.QueryRow(ctx, "SELECT expires_at FROM client_sessions WHERE session_id = $1", int64(sessID)).Scan(&exp)
			if err == nil {
				assert.True(t, exp.Valid && exp.Time.Before(time.Now()), "持已过期句柄调用 %s 绝不能被重新插入复活为活跃会话", opName)
			} else {
				assert.ErrorIs(t, err, pgx.ErrNoRows)
			}

			var fhActive bool
			err = pool.QueryRow(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM file_handles fh
					JOIN client_sessions s ON fh.session_id = s.session_id
					WHERE fh.fh = $1 AND fh.ino = $2 AND s.expires_at > clock_timestamp()
				)`, int64(fh), int64(ino)).Scan(&fhActive)
			require.NoError(t, err)
			assert.False(t, fhActive, "持已过期句柄调用 %s 后，该句柄绝不得被复活为活跃句柄", opName)
		}
	}

	// 辅助函数：令指定会话过期
	expireSession := func(t *testing.T, sessID uint64) {
		t.Helper()
		_, err := pool.Exec(ctx, "UPDATE client_sessions SET expires_at = $1 WHERE session_id = $2", time.Now().Add(-10*time.Second), int64(sessID))
		require.NoError(t, err)
	}

	// 1. Release
	t.Run("Release", func(t *testing.T) {
		sess1 := uint64(888101)
		_, err := mgr.RenewSession(ctx, sess1)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("expired_rel_%d.bin", time.Now().UnixNano())),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess1,
		})
		require.NoError(t, err)
		fileIno, fh := createRes.Inode.Ino, createRes.Fh

		expireSession(t, sess1)
		assertSessionAndHandleExistBefore(t, sess1, fileIno, fh)

		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        fh,
			SessionID: &sess1,
		})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "G-02: 持已过期会话句柄调用 Release 必须返回 ErrBadFileHandle")

		assertSessionAndHandleCleanedAfter(t, sess1, fileIno, fh, "Release")
	})

	// 2. ReleaseDir
	t.Run("ReleaseDir", func(t *testing.T) {
		sess2 := uint64(888102)
		_, err := mgr.RenewSession(ctx, sess2)
		require.NoError(t, err)

		mkdirRes, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("expired_dir_%d", time.Now().UnixNano())),
			Mode:      0o755,
		})
		require.NoError(t, err)
		dirIno := mkdirRes.Ino

		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       dirIno,
			Flags:     meta.O_RDONLY,
			SessionID: &sess2,
		})
		require.NoError(t, err)
		dfh := openDirRes.Fh

		expireSession(t, sess2)
		assertSessionAndHandleExistBefore(t, sess2, dirIno, dfh)

		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       dirIno,
			Fh:        dfh,
			SessionID: &sess2,
		})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "G-02: 持已过期会话句柄调用 ReleaseDir 必须返回 ErrBadFileHandle")

		assertSessionAndHandleCleanedAfter(t, sess2, dirIno, dfh, "ReleaseDir")
	})

	// 3. Flush
	t.Run("Flush", func(t *testing.T) {
		sess3 := uint64(888103)
		_, err := mgr.RenewSession(ctx, sess3)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("expired_flush_%d.bin", time.Now().UnixNano())),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess3,
		})
		require.NoError(t, err)
		fileIno, fh := createRes.Inode.Ino, createRes.Fh

		expireSession(t, sess3)
		assertSessionAndHandleExistBefore(t, sess3, fileIno, fh)

		err = mgr.Flush(ctx, meta.FlushParams{
			Ino:       fileIno,
			Fh:        fh,
			SessionID: &sess3,
		})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "G-02: 持已过期会话句柄调用 Flush 必须返回 ErrBadFileHandle")

		assertSessionAndHandleCleanedAfter(t, sess3, fileIno, fh, "Flush")
	})

	// 4. Fallocate
	t.Run("Fallocate", func(t *testing.T) {
		sess4 := uint64(888104)
		_, err := mgr.RenewSession(ctx, sess4)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("expired_falloc_%d.bin", time.Now().UnixNano())),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess4,
		})
		require.NoError(t, err)
		fileIno, fh := createRes.Inode.Ino, createRes.Fh

		expireSession(t, sess4)
		assertSessionAndHandleExistBefore(t, sess4, fileIno, fh)

		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       fileIno,
			Fh:        fh,
			Offset:    0,
			Length:    1024,
			Mode:      meta.FallocFlKeepSize,
			SessionID: &sess4,
		})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "G-02: 持已过期会话句柄调用 Fallocate 必须返回 ErrBadFileHandle")

		assertSessionAndHandleCleanedAfter(t, sess4, fileIno, fh, "Fallocate")
	})

	// 5. SetLk
	t.Run("SetLk", func(t *testing.T) {
		sess5 := uint64(888105)
		_, err := mgr.RenewSession(ctx, sess5)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("expired_setlk_%d.bin", time.Now().UnixNano())),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess5,
		})
		require.NoError(t, err)
		fileIno, fh := createRes.Inode.Ino, createRes.Fh

		expireSession(t, sess5)
		assertSessionAndHandleExistBefore(t, sess5, fileIno, fh)

		err = mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fileIno,
			Fh:        fh,
			LockOwner: 5001,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
			},
			SessionID: sess5,
		})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "G-02: 持已过期会话句柄调用 SetLk 必须返回 ErrBadFileHandle")

		assertSessionAndHandleCleanedAfter(t, sess5, fileIno, fh, "SetLk")
	})

	// 6. SetLkw
	t.Run("SetLkw", func(t *testing.T) {
		sess6 := uint64(888106)
		_, err := mgr.RenewSession(ctx, sess6)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("expired_setlkw_%d.bin", time.Now().UnixNano())),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sess6,
		})
		require.NoError(t, err)
		fileIno, fh := createRes.Inode.Ino, createRes.Fh

		expireSession(t, sess6)
		assertSessionAndHandleExistBefore(t, sess6, fileIno, fh)

		err = mgr.SetLkw(ctx, meta.SetLkwParams{
			Ino:       fileIno,
			Fh:        fh,
			LockOwner: 6001,
			Lk: meta.FileLock{
				Start: 0,
				End:   100,
				Type:  meta.F_WRLCK,
			},
			SessionID: sess6,
			RequestID: 1,
		})
		assert.ErrorIs(t, err, meta.ErrBadFileHandle, "G-02: 持已过期会话句柄调用 SetLkw 必须返回 ErrBadFileHandle")

		assertSessionAndHandleCleanedAfter(t, sess6, fileIno, fh, "SetLkw")
	})
}

type staleHandleHookQuerier struct {
	db.Querier
	afterGetFileHandle func()
}

func (q *staleHandleHookQuerier) GetFileHandle(ctx context.Context, arg db.GetFileHandleParams) (db.FileHandle, error) {
	h, err := q.Querier.GetFileHandle(ctx, arg)
	if err == nil && q.afterGetFileHandle != nil {
		q.afterGetFileHandle()
	}
	return h, err
}

type staleHandleHookStore struct {
	db.Store
	afterGetFileHandle func()
	hookActive         bool
}

func (s *staleHandleHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		if s.hookActive && s.afterGetFileHandle != nil {
			wrapped := &staleHandleHookQuerier{
				Querier:            q,
				afterGetFileHandle: s.afterGetFileHandle,
			}
			return fn(wrapped)
		}
		return fn(q)
	}, opts...)
}

// TestSession_ExpiredAndRecreatedSameSessionID_RejectsStaleFileHandle 验证 G-02：
// 会话过期并被相同 session_id 重建后，写操作预检通过的旧代际句柄绝不得逃逸 CreatedAt == UpdatedAt 检查，
// 必须严格返回 ErrBadFileHandle，底表 file_locks 绝不能写入已失效句柄的锁记录，且新重建的活跃会话不能被误删。
func TestSession_ExpiredAndRecreatedSameSessionID_RejectsStaleFileHandle(t *testing.T) {
	dsn := getStorageTestDSN()

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

	baseStore := db.NewStore(pool)
	hookStore := &staleHandleHookStore{Store: baseStore}
	mgr := meta.New(hookStore)
	otherMgr := meta.New(baseStore)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-g02-stale-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	}()

	sessID := uint64(888009)
	_, err = mgr.RenewSession(ctx, sessID)
	require.NoError(t, err)

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("stale_handle_test.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh := createRes.Fh

	// 确认句柄已在底表持久化
	var fhExists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM file_handles WHERE fh = $1 AND ino = $2)", int64(fh), int64(fileIno)).Scan(&fhExists)
	require.NoError(t, err)
	require.True(t, fhExists)

	// 配置钩子：在 T1 事务内通过 GetFileHandle 预检之后、即将执行 touchActiveClientSession 之前触发
	var hookTriggered bool
	hookStore.afterGetFileHandle = func() {
		if hookTriggered {
			return
		}
		hookTriggered = true

		// 1. 令会话 S1 过期 (将 expires_at 改为过去 10 秒)
		_, errExec := pool.Exec(ctx, "UPDATE client_sessions SET expires_at = $1 WHERE session_id = $2", time.Now().Add(-10*time.Second), int64(sessID))
		require.NoError(t, errExec)

		// 2. 由另一独立连接调用 otherMgr.RenewSession(S1)
		// 这会触发清除旧会话 S1（级联删除 file_handles 中的 fh），并以相同 session_id 重建新会话
		_, errRenew := otherMgr.RenewSession(ctx, sessID)
		require.NoError(t, errRenew)

		// 3. 再次更新或者续期，令新会话 updated_at > created_at，从而避开旧有的 CreatedAt == UpdatedAt 检查
		time.Sleep(10 * time.Millisecond)
		_, errRenew2 := otherMgr.RenewSession(ctx, sessID)
		require.NoError(t, errRenew2)
	}
	hookStore.hookActive = true

	// 此时 T1 执行 SetLk 写操作：在事务内先完成 GetFileHandle，随后触发 hook 使会话过期并以相同 session_id 重建
	err = mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       fileIno,
		Fh:        fh,
		LockOwner: 901,
		Lk: meta.FileLock{
			Start: 0,
			End:   100,
			Type:  meta.F_WRLCK,
			PID:   901,
		},
		LkFlags:   0,
		SessionID: sessID,
	})

	hookStore.hookActive = false
	require.True(t, hookTriggered, "hook 必须被正常触发")

	// 核心断言 1：T1 的写操作必须返回 ErrBadFileHandle，绝不能因为同名 session_id 重建而放行旧句柄
	assert.ErrorIs(t, err, meta.ErrBadFileHandle, "G-02: 会话过期重建后，旧代际句柄执行写操作必须返回 ErrBadFileHandle")

	// 核心断言 2：底表 file_locks 中绝不能写入绑定已级联删除句柄 fh 的锁记录
	var lockCount int64
	err = pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE fh = $1", int64(fh)).Scan(&lockCount)
	require.NoError(t, err)
	assert.Equal(t, int64(0), lockCount, "G-02: 底表 file_locks 绝不能写入绑定已删除句柄的锁记录")

	// 核心断言 3：新重建的活跃会话 S1 绝不能被误删
	var sessExists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM client_sessions WHERE session_id = $1)", int64(sessID)).Scan(&sessExists)
	require.NoError(t, err)
	assert.True(t, sessExists, "G-02: 新重建的活跃会话 S1 绝不能被写操作事务误删")
}
