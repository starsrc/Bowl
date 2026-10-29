package meta_test

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

// 最终删盘事务等待级联会话行锁时，已完成真实 Ping-Pong 的重连流必须撤销删除资格。
func TestReviewLogicR4_DeleteDiskReconnectDuringFinalCascade(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "review_logic_r4_delete")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	mgr := meta.New(db.NewStore(pool))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	file, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno, Name: []byte("保留文件"), Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)
	srv, err := server.New(server.Params{
		Config: config.ServerConfig{ServerHost: "127.0.0.1", ServerReadMaxBytes: config.DefaultServerReadMaxBytes}, MetaManager: mgr,
	})
	require.NoError(t, err)
	ts := httptest.NewUnstartedServer(srv.Handler())
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer func() { ts.CloseClientConnections(); ts.Close() }()
	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL, connect.WithGRPC())
	old := client.WatchSession(ctx)
	require.NoError(t, old.Send(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID}))
	_, err = old.Receive()
	require.NoError(t, err)

	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sid))
	require.NoError(t, err)
	require.NoError(t, old.CloseRequest())
	require.Eventually(t, func() bool { return !mgr.WatchHub().HasActiveStream(sid) }, time.Second, time.Millisecond)
	deleteDone := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID) }()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("删盘协程未退出")
		}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND cardinality(pg_blocking_pids(pid)) > 0
			AND query LIKE '%name: DeleteDisk :execrows%'
		)`).Scan(&waiting)
		require.NoError(t, err)
		return waiting
	}, time.Second, time.Millisecond, "删盘应已越过流检查并等待级联会话行锁")

	stream := client.WatchSession(ctx)
	require.NoError(t, stream.Send(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID}))
	ack, err := stream.Receive()
	require.NoError(t, err)
	require.Zero(t, ack.GetPingSeq())
	probeDone := make(chan bool, 1)
	go func() { probeDone <- mgr.WatchHub().Probe(ctx, sid, 300*time.Millisecond) }()
	ping, err := stream.Receive()
	require.NoError(t, err)
	require.Positive(t, ping.GetPingSeq())
	require.NoError(t, stream.Send(&storagev1.WatchSessionRequest{SessionId: sid, PongSeq: ping.GetPingSeq()}))
	require.True(t, <-probeDone, "放行删除前，新流已完成真实 Ping-Pong")
	require.NoError(t, blocker.Commit(ctx))

	select {
	case deleteErr := <-deleteDone:
		assert.ErrorIs(t, deleteErr, meta.ErrBusy, "重连流已确认存活，必须撤销最终删盘事务")
	case <-ctx.Done():
		t.Fatal("删盘未在期限内结束")
	}
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	assert.NoError(t, err, "存活挂载的磁盘必须保留")
	var sessions, handles, inodes int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(file.Fh)).Scan(&handles))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM inodes WHERE ino = $1", int64(file.Inode.Ino)).Scan(&inodes))
	assert.Equal(t, 1, sessions, "重连会话租约必须保留")
	assert.Equal(t, 1, handles, "重连会话句柄必须保留")
	assert.Equal(t, 1, inodes, "磁盘内文件必须保留")
}

// DeleteDisk 在完成全部级联删除 SQL 并获取 AcquireSessionsCommitGate 之后、真实数据库事务 Commit 之前，
// 同 sessionID 的新流登记必须严格被批量提交门控阻塞至删盘事务提交结束，
// 严禁在删盘事务未提交前完成新流登记与 Ping-Pong 探活后又被随后的 DeleteDisk 提交删空会话租约、打开句柄与磁盘内 Inode。
func TestReviewLogicR4_DeleteDiskCommitGateBlocksRegisterUntilCommit(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "review_logic_r4_delete_commit_gate")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()

	atCommitBoundary := make(chan struct{})
	releaseCommit := make(chan struct{})
	var (
		interceptMu sync.Mutex
		intercept   bool
		barrierOnce sync.Once
	)
	store := &commitBarrierStore{
		Store: db.NewStore(pool),
		beforeCommit: func(txCtx context.Context) error {
			interceptMu.Lock()
			enabled := intercept
			interceptMu.Unlock()
			if !enabled {
				return nil
			}
			barrierOnce.Do(func() {
				close(atCommitBoundary)
				select {
				case <-releaseCommit:
				case <-txCtx.Done():
				}
			})
			return nil
		},
	}

	mgr := meta.New(store, meta.WithSessionProbeTimeout(300*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	file, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno, Name: []byte("delete-commit-boundary"), Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)

	// 仅开启拦截 DeleteDisk 最终级联事务的提交边界
	interceptMu.Lock()
	intercept = true
	interceptMu.Unlock()

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID)
	}()

	select {
	case <-atCommitBoundary:
	case <-ctx.Done():
		t.Fatal("DeleteDisk 未到达事务提交边界")
	}

	// 此时 DeleteDisk 事务已完成全部级联 SQL 且已调用 AcquireSessionsCommitGate，但真实 PG 事务尚未 Commit。
	// 并发发起同 sessionID 的新流 Register，验证其必须阻塞在批量提交门控上，不得越过提交边界提前完成登记与探活。
	regEntered := make(chan struct{})
	regDone := make(chan struct{})
	var (
		newStreamID   uint64
		unregisterNew func() bool
	)
	go func() {
		close(regEntered)
		newStreamID, unregisterNew = mgr.WatchHub().Register(sid, func(seq uint64) error {
			mgr.WatchHub().HandlePong(sid, newStreamID, seq)
			return nil
		})
		close(regDone)
	}()
	<-regEntered

	var leakedBeforeCommit bool
	select {
	case <-regDone:
		leakedBeforeCommit = true
		require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "提前越过批量门控的新流已完成 Ping-Pong")
	case <-time.After(50 * time.Millisecond):
	}
	assert.False(t, leakedBeforeCommit, "DeleteDisk 事务未 Commit 前，同会话的新流 Register 必须被批量提交门控阻塞")

	// 放行 DeleteDisk 事务完成真实 Commit
	close(releaseCommit)
	require.NoError(t, <-deleteDone)

	// 事务 Commit 完成后，批量提交门控释放，新流 Register 立即完成
	select {
	case <-regDone:
	case <-ctx.Done():
		t.Fatal("DeleteDisk 事务提交后新流 Register 仍未完成")
	}
	defer unregisterNew()

	// 若未泄漏（批量门控正常保持至 Commit 结束），旧磁盘与旧会话已在新流登记前正规提交删除；
	// 客户端在新流完成登记后绑定新盘续期并创建打开文件，其会话租约、打开句柄与磁盘内 Inode 数量均必须保留。
	// 若发生泄漏（新流在 Commit 前已登记并完成 Ping-Pong），随后的 DeleteDisk Commit 会将该存活会话的租约、句柄与 Inode 全部删空（0）。
	targetFh := file.Fh
	targetIno := file.Inode.Ino
	if !leakedBeforeCommit {
		newDisk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name() + "_recreated", MaxBytes: 1024})
		require.NoError(t, err)
		_, err = mgr.RenewSessionWithDisk(ctx, sid, &newDisk.DiskID)
		require.NoError(t, err)
		newFile, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: newDisk.RootIno, Name: []byte("recreated-after-commit"), Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
		})
		require.NoError(t, err)
		targetFh = newFile.Fh
		targetIno = newFile.Inode.Ino
	}

	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "新流必须可正常响应 Ping-Pong")
	var sessions, handles, inodes int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(targetFh)).Scan(&handles))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM inodes WHERE ino = $1", int64(targetIno)).Scan(&inodes))
	assert.Equal(t, 1, sessions, "已完成登记与探活的会话租约不得被滞后的 DeleteDisk 提交删空")
	assert.Equal(t, 1, handles, "已完成登记与探活的打开句柄不得被滞后的 DeleteDisk 提交删空")
	assert.Equal(t, 1, inodes, "已完成登记与探活的磁盘内 Inode 不得被滞后的 DeleteDisk 提交删空")
}
