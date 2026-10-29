package meta_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// 会话删除在孤儿回收退还配额时等待磁盘行锁，等待期间重连的存活会话必须保留。
func TestReviewLogicR3_DeleteDiskReconnectDuringOrphanQuotaReclaim(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "review_logic_r3")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	mgr := meta.New(db.NewStore(pool), meta.WithSessionProbeTimeout(300*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	tmp, err := mgr.TmpFile(ctx, meta.TmpFileParams{
		ParentIno: disk.RootIno, Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)

	oldPing := make(chan struct{}, 1)
	_, unregisterOld := mgr.WatchHub().Register(sid, func(uint64) error {
		oldPing <- struct{}{}
		return nil
	})
	defer unregisterOld()
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
	select {
	case <-oldPing:
	case <-ctx.Done():
		t.Fatal("未收到旧流探活")
	}

	// DeleteDisk 已释放磁盘锁开始探活。并发磁盘事务仅持锁，不修改任何业务状态。
	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "SELECT disk_id FROM disks WHERE disk_id = $1 FOR UPDATE", disk.DiskID)
	require.NoError(t, err)
	var blockerPID int32
	require.NoError(t, blocker.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID))
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE $1 = ANY(pg_blocking_pids(pid))
			AND query LIKE '%AddDiskQuotaByRootIno%'
		)`, blockerPID).Scan(&waiting)
		require.NoError(t, err)
		return waiting
	}, 2*time.Second, time.Millisecond, "会话回收应在流复核之后等待配额行锁")
	require.False(t, mgr.WatchHub().HasActiveStream(sid))

	var streamID uint64
	streamID, unregisterNew := mgr.WatchHub().Register(sid, func(seq uint64) error {
		mgr.WatchHub().HandlePong(sid, streamID, seq)
		return nil
	})
	defer unregisterNew()
	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "事务提交前，新流已完成 Ping-Pong")
	var sessions int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.Equal(t, 1, sessions, "重连时租约尚未提交删除")
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
	assert.Equal(t, 1, sessions, "存活会话必须保留")
	var handles int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(tmp.Fh)).Scan(&handles))
	assert.Equal(t, 1, handles, "重连会话的已打开匿名文件句柄必须保留")
	assert.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "新流仍可响应探活")
}

type commitBarrierStore struct {
	db.Store
	beforeCommit func(ctx context.Context) error
}

func (s *commitBarrierStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		if err := fn(q); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			return s.beforeCommit(ctx)
		}
		return nil
	}, opts...)
}

// 会话自动回收事务在完成全部 SQL 并获取 AcquireAutoCloseGate 后、真实数据库事务 Commit 前，
// 同 sessionID 的新流登记必须严格被门控阻塞至回收事务提交结束，严禁越过提交边界被随后的 Commit 删空会话租约与句柄。
func TestReviewLogicR3_AutoCloseGateBlocksRegisterUntilCommit(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "review_logic_r3_commit_gate")
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
		ParentIno: disk.RootIno, Name: []byte("commit-boundary"), Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)

	oldStreamID, unregisterOld := mgr.WatchHub().Register(sid, func(uint64) error { return nil })
	unregisterOld()

	// 仅开启拦截断连自动回收事务的提交边界
	interceptMu.Lock()
	intercept = true
	interceptMu.Unlock()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- mgr.CloseSession(meta.WithDisconnectedStreamID(ctx, oldStreamID), sid)
	}()

	select {
	case <-atCommitBoundary:
	case <-ctx.Done():
		t.Fatal("断连自动回收未到达事务提交边界")
	}

	// 此时自动回收事务已完成全部 SQL 且已调用 AcquireAutoCloseGate，但真实 PG 事务尚未 Commit。
	// 并发发起同 sessionID 的新流 Register，验证其必须阻塞在门控上，不得越过提交边界提前完成登记与探活。
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

	// 给新流 Register 留出充足调度窗口：若门控在事务 Commit 前被提前释放，Register 将在此窗口内完成并可通过 Probe
	var leakedBeforeCommit bool
	select {
	case <-regDone:
		leakedBeforeCommit = true
		// 即使提前登记，也验证其完成了真实 Ping-Pong，随后事务提交会误删该存活会话
		require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "提前越过门控的新流已完成 Ping-Pong")
	case <-time.After(50 * time.Millisecond):
	}
	assert.False(t, leakedBeforeCommit, "自动回收事务未 Commit 前，同会话的新流 Register 必须被门控阻塞")

	// 放行自动回收事务完成真实 Commit
	close(releaseCommit)
	require.NoError(t, <-closeDone)

	// 事务 Commit 完成后，门控释放，新流 Register 立即完成
	select {
	case <-regDone:
	case <-ctx.Done():
		t.Fatal("回收事务提交后新流 Register 仍未完成")
	}
	defer unregisterNew()

	// 若未泄漏（门控正常保持至 Commit 结束），此时旧会话已在新流登记前正规提交回收；
	// 为验证「提前释放门控会导致已确认存活的新流会话与句柄被随后的 Commit 删空」，
	// 当新流在 Commit 后才完成登记时，客户端随后正常续期并打开句柄，会话租约与句柄数量必须为 1。
	if !leakedBeforeCommit {
		_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
		require.NoError(t, err)
		_, err = mgr.Open(ctx, meta.OpenParams{Ino: file.Inode.Ino, Flags: meta.O_RDWR, SessionID: &sid})
		require.NoError(t, err)
	}

	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond), "新流必须可正常响应 Ping-Pong")
	var sessions, handles int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE session_id = $1", int64(sid)).Scan(&handles))
	assert.Equal(t, 1, sessions, "新流完成登记与探活后，所属会话租约不得被滞后的回收事务提交删空")
	assert.Equal(t, 1, handles, "新流完成登记与探活后，所属打开句柄不得被滞后的回收事务提交删空")
}
