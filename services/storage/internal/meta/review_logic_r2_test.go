package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// 在旧流被驱逐、会话删除等待数据库行锁期间重连，新流的存活证据必须阻止删盘。
func TestReviewLogicR2_DeleteDiskReconnectAfterEviction(t *testing.T) {
	pool, parent := setupMetaTestDB(t)
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	mgr := meta.New(db.NewStore(pool), meta.WithSessionProbeTimeout(50*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		mgr.WatchHub().Evict(sid)
		require.NoError(t, mgr.CloseSession(cleanCtx, sid))
		_, cleanErr := pool.Exec(cleanCtx, "DELETE FROM disks WHERE disk_id = $1", disk.DiskID)
		require.NoError(t, cleanErr)
		_, cleanErr = pool.Exec(cleanCtx, "DELETE FROM inodes WHERE root_ino = $1", int64(disk.RootIno))
		require.NoError(t, cleanErr)
	})
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	_, unregisterOld := mgr.WatchHub().Register(sid, func(uint64) error { return nil })
	defer unregisterOld()

	// 真实 PostgreSQL 行锁只延迟 CloseSession，不修改会话状态或领域契约。
	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sid))
	require.NoError(t, err)
	deleteDone := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID)
	}()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("删盘协程未响应取消")
		}
	}()
	require.Eventually(t, func() bool { return !mgr.WatchHub().HasActiveStream(sid) }, time.Second, time.Millisecond, "旧流应在探活超时后被驱逐")

	var streamID uint64
	streamID, unregisterNew := mgr.WatchHub().Register(sid, func(seq uint64) error {
		mgr.WatchHub().HandlePong(sid, streamID, seq)
		return nil
	})
	defer unregisterNew()
	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "放行数据库回收前，新流已完成 Ping-Pong")
	var sessions int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.Equal(t, 1, sessions, "重连时租约仍有效")
	require.NoError(t, blocker.Commit(ctx))

	select {
	case deleteErr := <-deleteDone:
		assert.ErrorIs(t, deleteErr, meta.ErrBusy, "已确认重连客户端存活，必须拒绝删盘")
	case <-ctx.Done():
		t.Fatal("删盘未在期限内结束")
	}
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	assert.NoError(t, err, "存活挂载的磁盘必须保留")
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	assert.Equal(t, 1, sessions, "新流的租约必须保留")
	assert.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "误删后新流仍能应答探活")
}
