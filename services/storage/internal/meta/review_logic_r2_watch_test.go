package meta_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

// 客户端合法地将 HTTP/2 接收窗口设为零，流发送阻塞后探活退出必须释放发送协程。
func TestReviewLogicR2_ProbeEvictionDrainsBlockedHTTPSenders(t *testing.T) {
	pool, testCtx := setupMetaTestDB(t)
	mgr := meta.New(db.NewStore(pool))
	disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
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
	_, err = mgr.RenewSessionWithDisk(testCtx, sid, &disk.DiskID)
	require.NoError(t, err)
	hub := mgr.WatchHub()
	srv, err := server.New(server.Params{
		Config:      config.ServerConfig{ServerHost: "127.0.0.1", ServerReadMaxBytes: config.DefaultServerReadMaxBytes},
		MetaManager: mgr,
	})
	require.NoError(t, err)
	ts := httptest.NewUnstartedServer(srv.Handler())
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	ts.Config.Protocols = &protocols
	ts.Start()
	defer ts.Close()
	conn, err := net.DialTimeout("tcp", ts.Listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	framer := http2.NewFramer(conn, conn)
	_, err = conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)
	require.NoError(t, framer.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}))
	var headers bytes.Buffer
	encoder := hpack.NewEncoder(&headers)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: ts.Listener.Addr().String()},
		{Name: ":path", Value: storagev1connect.StorageServiceWatchSessionProcedure},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
	} {
		require.NoError(t, encoder.WriteField(field))
	}
	require.NoError(t, framer.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: headers.Bytes(), EndHeaders: true}))
	payload, err := proto.Marshal(&storagev1.WatchSessionRequest{SessionId: sid, DiskId: &disk.DiskID})
	require.NoError(t, err)
	envelope := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(envelope[1:5], uint32(len(payload)))
	copy(envelope[5:], payload)
	require.NoError(t, framer.WriteData(1, false, envelope))
	readerDone := make(chan struct{})
	var writeMu sync.Mutex
	go func() {
		defer close(readerDone)
		for {
			frame, readErr := framer.ReadFrame()
			if readErr != nil {
				return
			}
			if settings, ok := frame.(*http2.SettingsFrame); ok && !settings.IsAck() {
				writeMu.Lock()
				ackErr := framer.WriteSettingsAck()
				writeMu.Unlock()
				if ackErr != nil {
					return
				}
			}
		}
	}()
	defer func() {
		require.NoError(t, conn.Close())
		select {
		case <-readerDone:
		case <-time.After(time.Second):
			t.Error("HTTP/2 读协程未退出")
		}
	}()
	require.Eventually(t, func() bool { return hub.HasActiveStream(sid) }, time.Second, time.Millisecond)

	countSenders := func() int {
		var stacks bytes.Buffer
		require.NoError(t, pprof.Lookup("goroutine").WriteTo(&stacks, 2))
		count := 0
		for _, stack := range strings.Split(stacks.String(), "\n\n") {
			if strings.Contains(stack, "meta.(*SessionWatchHub).ProbeStream") {
				count++
			}
		}
		return count
	}
	require.Zero(t, countSenders())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 8; i++ {
		require.False(t, hub.Probe(ctx, sid, 10*time.Millisecond))
	}
	hub.Evict(sid)
	require.False(t, hub.HasActiveStream(sid))
	// 对端继续保持连接，但探活和流驱逐均已结束，不应仍保留发送任务。
	assert.Eventually(t, func() bool { return countSenders() == 0 }, 200*time.Millisecond, 5*time.Millisecond,
		"流驱逐后仍遗留探活发送协程；当前数量=%d", countSenders())
	// 先恢复接收窗口并排空遗留发送，再关闭连接，避免清理阶段访问已结束的 HTTP 处理器。
	writeMu.Lock()
	windowErr := framer.WriteWindowUpdate(1, 1024)
	writeMu.Unlock()
	require.NoError(t, windowErr)
	require.Eventually(t, func() bool { return countSenders() == 0 }, time.Second, time.Millisecond)
}
