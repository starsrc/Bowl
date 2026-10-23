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
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestTmpFile_InputValidation(t *testing.T) {
	ctx := context.Background()
	zeroSession := uint64(0)
	overflowSession := uint64(math.MaxInt64) + 1

	testCases := []struct {
		name        string
		params      meta.TmpFileParams
		expectedErr error
	}{
		{
			name: "parent_ino 为 0",
			params: meta.TmpFileParams{
				ParentIno: 0,
				Mode:      meta.S_IFREG | 0o600,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "parent_ino 超过 math.MaxInt64",
			params: meta.TmpFileParams{
				ParentIno: uint64(math.MaxInt64) + 1,
				Mode:      meta.S_IFREG | 0o600,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "mode 超过 0xffff",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      0x10000,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "mode 文件类型为目录 S_IFDIR",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      meta.S_IFDIR | 0o755,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "mode 文件类型为符号链接 S_IFLNK",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      meta.S_IFLNK | 0o777,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "mode 文件类型为命名管道 S_IFIFO",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      meta.S_IFIFO | 0o644,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "mode 文件类型为字符设备 S_IFCHR",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      meta.S_IFCHR | 0o644,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "mode 文件类型为块设备 S_IFBLK",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      meta.S_IFBLK | 0o644,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "mode 文件类型为套接字 S_IFSOCK",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      meta.S_IFSOCK | 0o644,
				Flags:     meta.O_RDWR,
			},
		},
		{
			name: "flags 访问模式为 O_RDONLY (0)",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      0o600,
				Flags:     meta.O_RDONLY,
			},
		},
		{
			name: "flags 携带 O_TMPFILE 但访问模式为 O_RDONLY",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      0o600,
				Flags:     meta.O_TMPFILE | meta.O_RDONLY,
			},
		},
		{
			name: "flags 访问模式为非法值 3 (O_ACCMODE)",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      0o600,
				Flags:     meta.O_ACCMODE,
			},
		},
		{
			name: "session_id 为 0",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      0o600,
				Flags:     meta.O_RDWR,
				SessionID: &zeroSession,
			},
		},
		{
			name: "session_id 超过 math.MaxInt64",
			params: meta.TmpFileParams{
				ParentIno: 1,
				Mode:      0o600,
				Flags:     meta.O_RDWR,
				SessionID: &overflowSession,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			res, err := mgr.TmpFile(ctx, tc.params)
			assert.Nil(t, res)
			exp := tc.expectedErr
			if exp == nil {
				exp = meta.ErrInvalidArgument
			}
			assert.ErrorIs(t, err, exp)
		})
	}
}

func TestTmpFile_PositiveAndParentInvariants(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	newIno := uint64(200)
	expectedFh := int64(51)

	baseParentRow := db.Inode{
		Ino:        int64(parentIno),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      2,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("成功创建 nlink = 0 匿名常规文件并自动置位 O_TMPFILE（不写 Entry 且不修改父目录时间戳）", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRow, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       int32(meta.S_IFREG | 0o600),
			Size:       0,
			Nlink:      0,
			Uid:        1001,
			Gid:        1002,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFREG | 0o600),
				Nlink: 0,
				Uid:   1001,
				Gid:   1002,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		createdHandleRow := db.FileHandle{
			Fh:        expectedFh,
			Ino:       int64(newIno),
			Flags:     int32(meta.O_RDWR | meta.O_TMPFILE),
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(newIno),
				Flags: int32(meta.O_RDWR | meta.O_TMPFILE),
			}).
			Return(createdHandleRow, nil).
			Once()

		res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: parentIno,
			Mode:      0o600,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(expectedFh), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
		require.NotNil(t, res.Inode)
		assert.Equal(t, newIno, res.Inode.Ino)
		assert.Equal(t, meta.S_IFREG|0o600, res.Inode.Attr.Mode)
		assert.Equal(t, uint32(0), res.Inode.Attr.Nlink)
		assert.Equal(t, uint32(1001), res.Inode.Attr.UID)
		assert.Equal(t, uint32(1002), res.Inode.Attr.GID)
	})

	t.Run("父目录含 S_ISGID 位且传入 SessionID 时继承父目录 GID 并续期会话", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		parentWithSGID := baseParentRow
		parentWithSGID.Mode = int32(meta.S_IFDIR | meta.S_ISGID | 0o755)
		parentWithSGID.Gid = 9999

		sessID := uint64(777)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentWithSGID, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:        int64(newIno),
			Generation: 0,
			Mode:       int32(meta.S_IFREG | 0o640),
			Size:       0,
			Nlink:      0,
			Uid:        1001,
			Gid:        9999,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:  int32(meta.S_IFREG | 0o640),
				Nlink: 0,
				Uid:   1001,
				Gid:   9999,
				Rdev:  0,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				return arg.SessionID == int64(sessID) && arg.ExpiresAt.Valid
			})).
			Return(db.ClientSession{SessionID: int64(sessID)}, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:       int64(newIno),
				Flags:     int32(meta.O_WRONLY | meta.O_EXCL | meta.O_TMPFILE),
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
			}).
			Return(db.FileHandle{
				Fh:        expectedFh,
				Ino:       int64(newIno),
				Flags:     int32(meta.O_WRONLY | meta.O_EXCL | meta.O_TMPFILE),
				SessionID: pgtype.Int8{Int64: int64(sessID), Valid: true},
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: parentIno,
			Mode:      meta.S_IFREG | 0o640,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_WRONLY | meta.O_EXCL,
			SessionID: &sessID,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint32(9999), res.Inode.Attr.GID)
		assert.Equal(t, uint32(0), res.Inode.Attr.Nlink)
	})

	t.Run("父节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: parentIno,
			Mode:      0o600,
			Flags:     meta.O_RDWR,
		})
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("父节点非目录 (S_IFREG) 返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileParent := baseParentRow
		fileParent.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(fileParent, nil).
			Once()

		res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: parentIno,
			Mode:      0o600,
			Flags:     meta.O_RDWR,
		})
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("父目录为已解链孤儿目录 (nlink == 0) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanParent := baseParentRow
		orphanParent.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(orphanParent, nil).
			Once()

		res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: parentIno,
			Mode:      0o600,
			Flags:     meta.O_RDWR,
		})
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("底层数据库错误透传包装", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("disk error")
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, dbErr).
			Once()

		res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: parentIno,
			Mode:      0o600,
			Flags:     meta.O_RDWR,
		})
		assert.Nil(t, res)
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestTmpFileAndLinkMaterialization_PostgresIntegration(t *testing.T) {
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

	s1 := uint64(990001)
	s2 := uint64(990002)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(s1), int64(s2))
	}()

	// AC2: TmpFile 成功创建 nlink == 0 的常规文件 Inode 与携带 O_TMPFILE 的 FileHandle，entries 表无记录，父目录 mtime/ctime 未被修改，且 S_ISGID 继承父目录 gid
	t.Run("AC2_TmpFile创建nlink0匿名文件且不修改父目录时间戳并继承S_ISGID属组", func(t *testing.T) {
		sgidDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("sgid_parent"),
			Mode:      meta.S_IFDIR | meta.S_ISGID | 0o755,
			UID:       1000,
			GID:       7777,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.Rmdir(context.Background(), rootIno, []byte("sgid_parent"))
			_ = store.DeleteInode(context.Background(), int64(sgidDir.Ino))
		}()

		parentBefore, err := mgr.GetAttr(ctx, sgidDir.Ino)
		require.NoError(t, err)

		time.Sleep(5 * time.Millisecond)

		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: sgidDir.Ino,
			Mode:      0o600,
			UID:       1001,
			GID:       2002,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		require.NotNil(t, tmpRes)
		tmpIno := tmpRes.Inode.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(tmpIno))
		}()

		assert.Equal(t, uint32(0), tmpRes.Inode.Attr.Nlink)
		assert.Equal(t, meta.S_IFREG|0o600, tmpRes.Inode.Attr.Mode)
		assert.Equal(t, uint32(7777), tmpRes.Inode.Attr.GID, "应继承带有 S_ISGID 父目录的 GID")

		// 验证 entries 表中无任何指向 tmpIno 的记录
		var entryCount int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM entries WHERE ino = $1", int64(tmpIno)).Scan(&entryCount)
		require.NoError(t, err)
		assert.Equal(t, 0, entryCount, "TmpFile 严禁写入 entries 表")

		// 验证父目录 mtime 与 ctime 未被修改
		parentAfter, err := mgr.GetAttr(ctx, sgidDir.Ino)
		require.NoError(t, err)
		assert.True(t, parentBefore.Attr.Mtime.Equal(parentAfter.Attr.Mtime), "TmpFile 不得修改父目录 mtime")
		assert.True(t, parentBefore.Attr.Ctime.Equal(parentAfter.Attr.Ctime), "TmpFile 不得修改父目录 ctime")

		// 验证 file_handles.flags 强制置位 O_TMPFILE
		fhRow, err := store.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:  int64(tmpRes.Fh),
			Ino: int64(tmpIno),
		})
		require.NoError(t, err)
		assert.NotZero(t, uint32(fhRow.Flags)&meta.O_TMPFILE, "file_handles.flags 必须置位 O_TMPFILE")

		// AC3 (a): 未具名化前调用 Release 关闭最后一个句柄，物理删除该 Inode
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino: tmpIno,
			Fh:  tmpRes.Fh,
		}))
		_, err = store.GetInode(ctx, int64(tmpIno))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "未具名化的 TmpFile 在 Release 后应被物理删除")
	})

	// AC3 (b): TmpFile 绑定会话后客户端崩溃过期，由 renewClientSession 物理回收
	t.Run("AC3_TmpFile绑定会话过期后由后续写操作物理回收", func(t *testing.T) {
		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: rootIno,
			Mode:      0o600,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_WRONLY,
			SessionID: &s1,
		})
		require.NoError(t, err)
		tmpIno := tmpRes.Inode.Ino
		defer func() {
			_ = store.DeleteInode(context.Background(), int64(tmpIno))
		}()

		// 令会话 s1 过期
		_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(s1))
		require.NoError(t, err)

		// 会话 s1 过期但尚未清理时，尝试 Link 该 TmpFile 必须返回 ErrNotFound（AC5b）
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          tmpIno,
			NewParentIno: rootIno,
			NewName:      []byte("expired_tmpfile.txt"),
		})
		assert.ErrorIs(t, err, meta.ErrNotFound, "所属会话已过期的 TmpFile 严禁被 Link 具名化")

		// 会话 s2 执行写操作触发 renewClientSession 物理回收该孤儿 TmpFile
		dirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       rootIno,
			Flags:     meta.O_RDONLY,
			SessionID: &s2,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       rootIno,
			Fh:        dirRes.Fh,
			SessionID: &s2,
		}))

		_, err = store.GetInode(ctx, int64(tmpIno))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "会话过期的未具名化 TmpFile 应被 renewClientSession 物理删除")
	})

	// AC4 & AC5(d): 非 O_EXCL TmpFile 成功 Link 具名化（nlink 0->1 且清除 O_TMPFILE），具名化后再次 Unlink 严禁二次 Link 复活
	t.Run("AC4_AC5d_非O_EXCL_TmpFile一次性具名化成功且擦除O_TMPFILE防Unlink后二次复活", func(t *testing.T) {
		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: rootIno,
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &s2,
		})
		require.NoError(t, err)
		tmpIno := tmpRes.Inode.Ino
		defer func() {
			_ = mgr.Unlink(context.Background(), rootIno, []byte("published.txt"))
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: tmpIno, Fh: tmpRes.Fh, SessionID: &s2})
			_ = store.DeleteInode(context.Background(), int64(tmpIno))
		}()

		// 首次 Link 具名化成功
		linkedInode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          tmpIno,
			NewParentIno: rootIno,
			NewName:      []byte("published.txt"),
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(1), linkedInode.Attr.Nlink)

		// 验证句柄上的 O_TMPFILE 位已被原子清除
		fhRow, err := store.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:        int64(tmpRes.Fh),
			Ino:       int64(tmpIno),
			SessionID: pgtype.Int8{Int64: int64(s2), Valid: true},
		})
		require.NoError(t, err)
		assert.Zero(t, uint32(fhRow.Flags)&meta.O_TMPFILE, "具名化后 file_handles.flags 的 O_TMPFILE 位必须被清除")

		// Lookup 可正常查到具名化后的文件
		lookedUp, err := mgr.Lookup(ctx, rootIno, []byte("published.txt"))
		require.NoError(t, err)
		assert.Equal(t, tmpIno, lookedUp.Ino)
		assert.Equal(t, uint32(1), lookedUp.Attr.Nlink)

		// 执行 Unlink 使其降回 nlink == 0（此时初始句柄 tmpRes.Fh 仍然打开）
		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("published.txt")))

		attrAfterUnlink, err := mgr.GetAttr(ctx, tmpIno)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), attrAfterUnlink.Attr.Nlink)

		// AC5(d): 尝试第二次 Link 复活必须被拒绝并返回 ErrNotFound
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          tmpIno,
			NewParentIno: rootIno,
			NewName:      []byte("resurrected_again.txt"),
		})
		assert.ErrorIs(t, err, meta.ErrNotFound, "具名化后被 Unlink 的文件严禁二次 Link 复活")

		// 关闭句柄后物理删除
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       tmpIno,
			Fh:        tmpRes.Fh,
			SessionID: &s2,
		}))
		_, err = store.GetInode(ctx, int64(tmpIno))
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})

	// AC5(a) & AC5(c): 拒绝携带 O_EXCL 的 TmpFile 具名化，以及拒绝普通 Create->Unlink 孤儿文件复活
	t.Run("AC5a_AC5c_拒绝O_EXCL_TmpFile具名化与普通Unlink孤儿文件复活", func(t *testing.T) {
		// (a) 创建时携带 O_EXCL 的 TmpFile
		exclRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: rootIno,
			Mode:      0o600,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR | meta.O_EXCL,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: exclRes.Inode.Ino, Fh: exclRes.Fh})
			_ = store.DeleteInode(context.Background(), int64(exclRes.Inode.Ino))
		}()

		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          exclRes.Inode.Ino,
			NewParentIno: rootIno,
			NewName:      []byte("excl_should_fail.txt"),
		})
		assert.ErrorIs(t, err, meta.ErrNotFound, "携带 O_EXCL 的 TmpFile 严禁 Link 具名化")

		// (c) 普通文件经 Create -> Unlink 形成的 nlink == 0 孤儿文件
		normRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("normal_temp.txt"),
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: normRes.Inode.Ino, Fh: normRes.Fh})
			_ = store.DeleteInode(context.Background(), int64(normRes.Inode.Ino))
		}()

		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("normal_temp.txt")))

		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          normRes.Inode.Ino,
			NewParentIno: rootIno,
			NewName:      []byte("normal_resurrect_should_fail.txt"),
		})
		assert.ErrorIs(t, err, meta.ErrNotFound, "普通 Unlink 孤儿文件严禁 Link 复活")
	})

	// AC3: 普通文件拦截 Create 与 Open 伪造 O_TMPFILE 标志位，严防 Unlink 降为 nlink == 0 后被 Link 非法复活
	t.Run("AC3_普通文件拦截Create与Open伪造O_TMPFILE标志位严防Unlink后Link非法复活", func(t *testing.T) {
		// 1. 尝试通过 Create(..., O_TMPFILE) 伪造标志位，静态入参校验必须拒绝且不落库任何实体
		createForgedRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("forged_via_create.txt"),
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_CREAT | meta.O_RDWR | meta.O_TMPFILE,
		})
		assert.Nil(t, createForgedRes)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		stCreate := meta.ToStatus(err)
		require.NotNil(t, stCreate)
		require.NotEmpty(t, stCreate.Details())
		infoCreate, ok := stCreate.Details()[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, "EINVAL", infoCreate.Reason)

		// 验证目录项未被创建
		_, err = mgr.Lookup(ctx, rootIno, []byte("forged_via_create.txt"))
		assert.ErrorIs(t, err, meta.ErrNotFound)

		// 2. 正常创建常规文件
		normalRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("legit_file.txt"),
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})
		require.NoError(t, err)
		normalIno := normalRes.Inode.Ino
		defer func() {
			_ = mgr.Unlink(context.Background(), rootIno, []byte("legit_file.txt"))
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: normalIno, Fh: normalRes.Fh})
			_ = store.DeleteInode(context.Background(), int64(normalIno))
		}()

		// 3. 尝试对已有常规文件调用 Open(..., O_TMPFILE) 伪造注入 O_TMPFILE 标志位
		openForgedRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   normalIno,
			Flags: meta.O_RDWR | meta.O_TMPFILE,
		})
		assert.Nil(t, openForgedRes)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		stOpen := meta.ToStatus(err)
		require.NotNil(t, stOpen)
		require.NotEmpty(t, stOpen.Details())
		infoOpen, ok := stOpen.Details()[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, "EINVAL", infoOpen.Reason)

		// 4. 正常二次打开该文件，验证句柄正常分配且库中 flags 不含 O_TMPFILE
		openLegitRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   normalIno,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: normalIno, Fh: openLegitRes.Fh})
		}()
		fhRow, err := store.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:  int64(openLegitRes.Fh),
			Ino: int64(normalIno),
		})
		require.NoError(t, err)
		assert.Zero(t, uint32(fhRow.Flags)&meta.O_TMPFILE, "普通 Open 句柄严禁携带 O_TMPFILE 标志位")

		// 5. 对该文件执行 Unlink，使其硬链接降为 nlink == 0（但仍持有活跃句柄处于孤儿状态）
		require.NoError(t, mgr.Unlink(ctx, rootIno, []byte("legit_file.txt")))
		attrAfterUnlink, err := mgr.GetAttr(ctx, normalIno)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), attrAfterUnlink.Attr.Nlink)

		// 6. 尝试调用 Link 试图使该孤儿文件复活，必须继续返回 ErrNotFound (ENOENT)，杜绝非法复活
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          normalIno,
			NewParentIno: rootIno,
			NewName:      []byte("resurrect_forged_should_fail.txt"),
		})
		assert.ErrorIs(t, err, meta.ErrNotFound, "普通文件 Unlink 后严禁通过 Link 非法复活")
	})

	// AC4: 由 TmpFile(..., O_EXCL) 创建的独占匿名文件无法通过二次调用 Open(..., O_TMPFILE) 绕过独占限制并在 Link 中具名化
	t.Run("AC4_独占TmpFile严禁通过Open伪造O_TMPFILE绕过O_EXCL并在Link中具名化", func(t *testing.T) {
		// 1. 创建携带 O_EXCL 的独占匿名临时文件
		exclRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: rootIno,
			Mode:      0o600,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR | meta.O_EXCL,
		})
		require.NoError(t, err)
		exclIno := exclRes.Inode.Ino
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: exclIno, Fh: exclRes.Fh})
			_ = store.DeleteInode(context.Background(), int64(exclIno))
		}()

		// 验证初始句柄携带 O_EXCL 与 O_TMPFILE
		fhRow1, err := store.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:  int64(exclRes.Fh),
			Ino: int64(exclIno),
		})
		require.NoError(t, err)
		assert.NotZero(t, uint32(fhRow1.Flags)&meta.O_EXCL)
		assert.NotZero(t, uint32(fhRow1.Flags)&meta.O_TMPFILE)

		// 2. 此时调用 Link 具名化必定失败（因携带 O_EXCL）
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          exclIno,
			NewParentIno: rootIno,
			NewName:      []byte("excl_materialize_1.txt"),
		})
		assert.ErrorIs(t, err, meta.ErrNotFound, "携带 O_EXCL 的独占临时文件首次 Link 必须返回 ErrNotFound")

		// 3. 尝试通过二次调用 Open(..., O_TMPFILE)（未带 O_EXCL）伪造分配可具名化句柄
		openForgedRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   exclIno,
			Flags: meta.O_RDWR | meta.O_TMPFILE,
		})
		assert.Nil(t, openForgedRes)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		st := meta.ToStatus(err)
		require.NotNil(t, st)
		require.NotEmpty(t, st.Details())
		info, ok := st.Details()[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, "EINVAL", info.Reason)

		// 4. 调用合法普通 Open(..., O_RDWR) 获取第二句柄，验证其 flags 严禁包含 O_TMPFILE
		openLegitRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   exclIno,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: exclIno, Fh: openLegitRes.Fh})
		}()
		fhRow2, err := store.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:  int64(openLegitRes.Fh),
			Ino: int64(exclIno),
		})
		require.NoError(t, err)
		assert.Zero(t, uint32(fhRow2.Flags)&meta.O_TMPFILE, "二次 Open 句柄严禁携带 O_TMPFILE 标志位")

		// 5. 再次尝试调用 Link 试图具名化该独占临时文件：
		// 句柄 1 携带 O_EXCL，句柄 2 不含 O_TMPFILE，两者均不符合具名化条件，必须继续返回 ErrNotFound (ENOENT)
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          exclIno,
			NewParentIno: rootIno,
			NewName:      []byte("excl_materialize_bypassed.txt"),
		})
		assert.ErrorIs(t, err, meta.ErrNotFound, "独占 TmpFile 严禁通过二次 Open 绕过独占限制并在 Link 中具名化")
	})
}

// TestTmpFile_OrphanPreventionAndExclBypassWithMock 通过单元 Mock 隔离验证 AC3 与 AC4 的安全不变量。
func TestTmpFile_OrphanPreventionAndExclBypassWithMock(t *testing.T) {
	ctx := context.Background()

	t.Run("Create 拦截 O_TMPFILE 确保普通文件无法注入 O_TMPFILE 凭证", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("mock_forged.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR | meta.O_TMPFILE,
		})
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("Open 拦截 O_TMPFILE 确保独占 TmpFile 无法通过二次 Open 获得可具名化句柄", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   100,
			Flags: meta.O_RDWR | meta.O_TMPFILE,
		})
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})
}

// TestTmpFile_InheritRootInoAndQuotaIncrement 验证 TmpFile 联动继承父目录 root_ino 并递增 used_inodes。
func TestTmpFile_InheritRootInoAndQuotaIncrement(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)
	rootIno := int64(2002)
	newIno := uint64(300)
	expectedFh := int64(10)

	baseParentRowWithRoot := db.Inode{
		Ino:     int64(parentIno),
		Mode:    int32(meta.S_IFDIR | 0o755),
		Nlink:   2,
		RootIno: rootIno,
	}

	t.Run("当 parent.RootIno > 0 时继承 root_ino 并在同一事务内调用 AddDiskQuotaByRootIno 递增 used_inodes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRowWithRoot, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:     int64(newIno),
			Mode:    int32(meta.S_IFREG | 0o600),
			Nlink:   0,
			Uid:     1001,
			Gid:     1002,
			RootIno: rootIno,
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:    int32(meta.S_IFREG | 0o600),
				Nlink:   0,
				Uid:     1001,
				Gid:     1002,
				Rdev:    0,
				RootIno: rootIno,
			}).
			Return(createdInodeRow, nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}).
			Return(nil).
			Once()

		createdHandleRow := db.FileHandle{
			Fh:    expectedFh,
			Ino:   int64(newIno),
			Flags: int32(meta.O_RDWR | meta.O_TMPFILE),
		}

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(newIno),
				Flags: int32(meta.O_RDWR | meta.O_TMPFILE),
			}).
			Return(createdHandleRow, nil).
			Once()

		res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: parentIno,
			Mode:      0o600,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(expectedFh), res.Fh)
		require.NotNil(t, res.Inode)
		assert.Equal(t, newIno, res.Inode.Ino)
	})

	t.Run("AddDiskQuotaByRootIno 失败时回滚事务并向上传播错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(baseParentRowWithRoot, nil).
			Once()

		createdInodeRow := db.Inode{
			Ino:     int64(newIno),
			Mode:    int32(meta.S_IFREG | 0o600),
			Nlink:   0,
			Uid:     1001,
			Gid:     1002,
			RootIno: rootIno,
		}

		mockQuerier.EXPECT().
			CreateInode(mock.Anything, db.CreateInodeParams{
				Mode:    int32(meta.S_IFREG | 0o600),
				Nlink:   0,
				Uid:     1001,
				Gid:     1002,
				Rdev:    0,
				RootIno: rootIno,
			}).
			Return(createdInodeRow, nil).
			Once()

		quotaErr := errors.New("failed to increment disk quota inodes for tmpfile")
		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     rootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}).
			Return(quotaErr).
			Once()

		res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: parentIno,
			Mode:      0o600,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_RDWR,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.ErrorIs(t, err, quotaErr)
		assert.Contains(t, err.Error(), "increment disk used_inodes")
	})
}

