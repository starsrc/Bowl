package meta

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionWatchHub_RegisterUnregisterAndHasActiveStream(t *testing.T) {
	hub := NewSessionWatchHub()
	const sid = uint64(1001)

	require.False(t, hub.HasActiveStream(sid))

	streamID1, unreg1 := hub.Register(sid, func(pingSeq uint64) error {
		return nil
	})
	require.GreaterOrEqual(t, streamID1, uint64(1))
	require.True(t, hub.HasActiveStream(sid))

	replaced := unreg1()
	require.False(t, replaced)
	require.False(t, hub.HasActiveStream(sid))
}

func TestSessionWatchHub_RegisterReplacesOldStreamAndWakesPendingProbe(t *testing.T) {
	hub := NewSessionWatchHub()
	const sid = uint64(1002)

	pingSent1 := make(chan uint64, 1)
	streamID1, unreg1 := hub.Register(sid, func(pingSeq uint64) error {
		pingSent1 <- pingSeq
		return nil
	})
	require.GreaterOrEqual(t, streamID1, uint64(1))

	probeDone := make(chan bool, 1)
	go func() {
		probeDone <- hub.Probe(context.Background(), sid, 5*time.Second)
	}()

	select {
	case <-pingSent1:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for pingSent1")
	}

	// 新流注册覆盖旧流，应立即关闭旧流的 doneCh 使正在阻塞的 Probe 返回 false
	streamID2, unreg2 := hub.Register(sid, func(pingSeq uint64) error {
		return nil
	})
	require.Greater(t, streamID2, streamID1)

	select {
	case alive := <-probeDone:
		require.False(t, alive, "旧流被覆盖时阻塞中的 Probe 必须立即返回 false")
	case <-time.After(time.Second):
		t.Fatal("probe did not return immediately after stream replacement")
	}

	// 旧流调用 unregister 应返回 replaced = true 且不影响新流槽位
	require.True(t, unreg1())
	require.True(t, hub.HasActiveStream(sid))

	// 新流调用 unregister 应返回 replaced = false 并清理槽位
	require.False(t, unreg2())
	require.False(t, hub.HasActiveStream(sid))
}

func TestSessionWatchHub_ProbeScenarios(t *testing.T) {
	t.Run("无活跃流时 Probe 立即 0ms 返回 false", func(t *testing.T) {
		hub := NewSessionWatchHub()
		start := time.Now()
		alive := hub.Probe(context.Background(), 9999, 5*time.Second)
		require.False(t, alive)
		require.Less(t, time.Since(start), 100*time.Millisecond)
	})

	t.Run("收到匹配的 HandlePong 时 Probe 返回 true", func(t *testing.T) {
		hub := NewSessionWatchHub()
		const sid = uint64(2001)

		var streamID uint64
		var unreg func() bool
		streamID, unreg = hub.Register(sid, func(pingSeq uint64) error {
			go func() {
				// 先发一个不匹配的 pongSeq 或零值不应误唤醒
				hub.HandlePong(sid, streamID, 0)
				hub.HandlePong(sid, streamID, pingSeq+999)
				hub.HandlePong(sid, streamID+999, pingSeq)
				time.Sleep(10 * time.Millisecond)
				hub.HandlePong(sid, streamID, pingSeq)
			}()
			return nil
		})
		defer unreg()

		alive := hub.Probe(context.Background(), sid, time.Second)
		require.True(t, alive)
	})

	t.Run("sendPing 返回错误时 Probe 立即返回 false", func(t *testing.T) {
		hub := NewSessionWatchHub()
		const sid = uint64(2002)

		_, unreg := hub.Register(sid, func(pingSeq uint64) error {
			return errors.New("stream broken")
		})
		defer unreg()

		start := time.Now()
		alive := hub.Probe(context.Background(), sid, 5*time.Second)
		require.False(t, alive)
		require.Less(t, time.Since(start), 100*time.Millisecond)
	})

	t.Run("探测期间流断开 unregister 时 Probe 立即返回 false", func(t *testing.T) {
		hub := NewSessionWatchHub()
		const sid = uint64(2003)

		pingReceived := make(chan struct{})
		_, unreg := hub.Register(sid, func(pingSeq uint64) error {
			close(pingReceived)
			return nil
		})

		go func() {
			<-pingReceived
			time.Sleep(15 * time.Millisecond)
			unreg()
		}()

		start := time.Now()
		alive := hub.Probe(context.Background(), sid, 5*time.Second)
		require.False(t, alive)
		require.Less(t, time.Since(start), 500*time.Millisecond)
	})

	t.Run("超时未收到 Pong 时 Probe 返回 false", func(t *testing.T) {
		hub := NewSessionWatchHub()
		const sid = uint64(2004)

		_, unreg := hub.Register(sid, func(pingSeq uint64) error {
			return nil
		})
		defer unreg()

		alive := hub.Probe(context.Background(), sid, 30*time.Millisecond)
		require.False(t, alive)
	})

	t.Run("ctx 取消时 Probe 立即返回 false", func(t *testing.T) {
		hub := NewSessionWatchHub()
		const sid = uint64(2005)

		pingReceived := make(chan struct{})
		_, unreg := hub.Register(sid, func(pingSeq uint64) error {
			close(pingReceived)
			return nil
		})
		defer unreg()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-pingReceived
			time.Sleep(15 * time.Millisecond)
			cancel()
		}()

		start := time.Now()
		alive := hub.Probe(ctx, sid, 5*time.Second)
		require.False(t, alive)
		require.Less(t, time.Since(start), 500*time.Millisecond)
	})

	t.Run("并发多次 Probe 各自独立匹配 pingSeq", func(t *testing.T) {
		hub := NewSessionWatchHub()
		const sid = uint64(2006)

		var streamID uint64
		var sendMu sync.Mutex
		streamID, unreg := hub.Register(sid, func(pingSeq uint64) error {
			sendMu.Lock()
			defer sendMu.Unlock()
			go func(seq uint64) {
				time.Sleep(10 * time.Millisecond)
				hub.HandlePong(sid, streamID, seq)
			}(pingSeq)
			return nil
		})
		defer unreg()

		const workers = 10
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				assert.True(t, hub.Probe(context.Background(), sid, time.Second))
			}()
		}
		wg.Wait()
	})
}

func TestManager_WatchHubAndValidateWatchSession(t *testing.T) {
	// 1. 通过 New 创建的 manager 拥有非 nil WatchHub
	m := New(nil)
	require.NotNil(t, m.WatchHub())
	require.Same(t, m.WatchHub(), m.WatchHub())

	// 2. 直接零值构造 &manager{} 调用 WatchHub() 也能懒初始化返回非 nil 实例
	raw := &manager{}
	require.NotNil(t, raw.WatchHub())
	require.Same(t, raw.WatchHub(), raw.WatchHub())

	// 3. ValidateWatchSession 校验边界
	validUUID := "11111111-1111-7111-8111-111111111111"
	emptyUUID := ""
	invalidUUID := "not-a-uuid"
	nilUUID := "00000000-0000-0000-0000-000000000000"

	require.NoError(t, m.ValidateWatchSession(1, nil))
	require.NoError(t, m.ValidateWatchSession(math.MaxInt64, &validUUID))

	require.ErrorIs(t, m.ValidateWatchSession(0, nil), ErrInvalidArgument)
	require.ErrorIs(t, m.ValidateWatchSession(uint64(math.MaxInt64)+1, nil), ErrInvalidArgument)
	require.ErrorIs(t, m.ValidateWatchSession(1, &emptyUUID), ErrInvalidArgument)
	require.ErrorIs(t, m.ValidateWatchSession(1, &invalidUUID), ErrInvalidArgument)
	require.ErrorIs(t, m.ValidateWatchSession(1, &nilUUID), ErrInvalidArgument)
}
