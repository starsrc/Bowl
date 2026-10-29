package meta_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

// 旧 HTTP/2 流断连后会话清理等待行锁，新流重连并应答探活必须使旧流清理失效。
func TestReviewLogicR3_DisconnectCleanupPreservesReconnectedSession(t *testing.T) {
	pool, parent := setupIsolatedMetaTestDB(t, "review_logic_r3_disconnect")
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	mgr := meta.New(db.NewStore(pool))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano())
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)
	tmp, err := mgr.TmpFile(ctx, meta.TmpFileParams{
		ParentIno: disk.RootIno, Mode: 0o600, Flags: meta.O_RDWR, SessionID: &sid,
	})
	require.NoError(t, err)
	srv, err := server.New(server.Params{
		Config: config.ServerConfig{ServerHost: "127.0.0.1", ServerReadMaxBytes: config.DefaultServerReadMaxBytes}, MetaManager: mgr,
	})
	require.NoError(t, err)
	oldHandlerDone := make(chan struct{})
	var requests atomic.Int32
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			defer close(oldHandlerDone)
		}
		srv.Handler().ServeHTTP(w, r)
	}))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer func() {
		ts.CloseClientConnections()
		ts.Close()
	}()
	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL, connect.WithGRPC())
	oldCtx, oldCancel := context.WithCancel(ctx)
	defer oldCancel()
	oldStream := client.WatchSession(oldCtx)
	require.NoError(t, oldStream.Send(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID}))
	ack, err := oldStream.Receive()
	require.NoError(t, err)
	require.Zero(t, ack.GetPingSeq())

	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "SELECT session_id FROM client_sessions WHERE session_id = $1 FOR UPDATE", int64(sid))
	require.NoError(t, err)
	var blockerPID int32
	require.NoError(t, blocker.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID))
	require.NoError(t, oldStream.CloseRequest())
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE $1 = ANY(pg_blocking_pids(pid))
			AND query LIKE '%DeleteClientSessionByID%'
		)`, blockerPID).Scan(&waiting)
		require.NoError(t, err)
		return waiting
	}, time.Second, time.Millisecond, "旧流清理应等待会话行锁")
	require.False(t, mgr.WatchHub().HasActiveStream(sid))

	newCtx, newCancel := context.WithCancel(ctx)
	defer newCancel()
	newStream := client.WatchSession(newCtx)
	require.NoError(t, newStream.Send(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID}))
	ack, err = newStream.Receive()
	require.NoError(t, err)
	require.Zero(t, ack.GetPingSeq())
	probeNew := func() {
		probeDone := make(chan bool, 1)
		go func() { probeDone <- mgr.WatchHub().Probe(ctx, sid, 300*time.Millisecond) }()
		ping, recvErr := newStream.Receive()
		require.NoError(t, recvErr)
		require.Positive(t, ping.GetPingSeq())
		require.NoError(t, newStream.Send(&storagev1.WatchSessionRequest{SessionId: sid, PongSeq: ping.GetPingSeq()}))
		require.True(t, <-probeDone, "真实重连流必须完成 Ping-Pong")
	}
	probeNew()
	require.NoError(t, blocker.Commit(ctx))
	select {
	case <-oldHandlerDone:
	case <-ctx.Done():
		t.Fatal("旧流清理未结束")
	}
	var sessions, handles int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	assert.Equal(t, 1, sessions, "旧流退出不得注销已重连的存活会话")
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(tmp.Fh)).Scan(&handles))
	assert.Equal(t, 1, handles, "旧流退出不得销毁已重连会话的文件句柄")
	probeNew()
}
