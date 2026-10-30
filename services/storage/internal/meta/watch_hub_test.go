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

type selectBarrierContext struct {
	context.Context
	once          sync.Once
	atSelect      chan struct{}
	releaseSelect chan struct{}
}

func (c *selectBarrierContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.atSelect)
		<-c.releaseSelect
	})
	return c.Context.Done()
}

// TestSessionWatchHub_ProbeCancellationDuringBlockedSend 验证探活在发送端阻塞时独立响应探活预算到期、调用上下文取消与流驱逐。
func TestSessionWatchHub_ProbeCancellationDuringBlockedSend(t *testing.T) {
	t.Run("探活预算到期", func(t *testing.T) {
		hub := NewSessionWatchHub()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		entered := make(chan struct{})
		release := make(chan struct{})
		_, unregister := hub.Register(71, func(seq uint64) error {
			close(entered)
			<-release
			return nil
		})
		defer unregister()

		done := make(chan bool, 1)
		go func() { done <- hub.Probe(ctx, 71, 20*time.Millisecond) }()
		defer func() {
			close(release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("释放发送屏障后探活协程仍未退出")
			}
		}()

		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("未进入发送回调")
		}

		select {
		case alive := <-done:
			require.False(t, alive)
			require.NoError(t, ctx.Err(), "探活预算到期时调用上下文仍应未取消")
			done <- alive
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Probe 在 20ms 探活预算到期后仍阻塞于发送回调")
		}
	})

	t.Run("上下文取消", func(t *testing.T) {
		hub := NewSessionWatchHub()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		entered := make(chan struct{})
		release := make(chan struct{})
		_, unregister := hub.Register(72, func(seq uint64) error {
			close(entered)
			<-release
			return nil
		})
		defer unregister()

		done := make(chan bool, 1)
		go func() { done <- hub.Probe(ctx, 72, 2*time.Second) }()
		defer func() {
			close(release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("释放发送屏障后探活协程仍未退出")
			}
		}()

		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("未进入发送回调")
		}

		select {
		case alive := <-done:
			require.False(t, alive)
			done <- alive
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Probe 在 50ms 上下文取消后仍阻塞于发送回调")
		}
	})

	t.Run("排队等待发送令牌期间上下文取消", func(t *testing.T) {
		hub := NewSessionWatchHub()
		firstCtx, firstCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer firstCancel()

		entered := make(chan struct{})
		release := make(chan struct{})
		_, unregister := hub.Register(73, func(seq uint64) error {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return nil
		})
		defer unregister()

		firstDone := make(chan bool, 1)
		go func() { firstDone <- hub.Probe(firstCtx, 73, 2*time.Second) }()
		defer func() {
			firstCancel()
			close(release)
			select {
			case <-firstDone:
			case <-time.After(time.Second):
				t.Error("释放发送屏障后首个探活协程仍未退出")
			}
		}()

		select {
		case <-entered:
		case <-firstCtx.Done():
			t.Fatal("首个 Probe 未进入发送回调")
		}

		queuedCtx, queuedCancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer queuedCancel()
		queuedDone := make(chan bool, 1)
		go func() { queuedDone <- hub.Probe(queuedCtx, 73, 2*time.Second) }()

		select {
		case alive := <-queuedDone:
			require.False(t, alive)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("排队等待 sendSem 的 Probe 在上下文取消后未在 200ms 内返回 false")
		}
	})

	t.Run("排队等待发送令牌期间流被驱逐", func(t *testing.T) {
		hub := NewSessionWatchHub()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		entered := make(chan struct{})
		release := make(chan struct{})
		_, unregister := hub.Register(74, func(seq uint64) error {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return nil
		})
		defer unregister()

		firstDone := make(chan bool, 1)
		go func() { firstDone <- hub.Probe(ctx, 74, 2*time.Second) }()
		defer func() {
			close(release)
			select {
			case <-firstDone:
			case <-time.After(time.Second):
				t.Error("释放发送屏障后首个探活协程仍未退出")
			}
		}()

		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("首个 Probe 未进入发送回调")
		}

		queuedDone := make(chan bool, 1)
		go func() { queuedDone <- hub.Probe(ctx, 74, 2*time.Second) }()

		time.Sleep(20 * time.Millisecond)
		hub.Evict(74)

		select {
		case alive := <-queuedDone:
			require.False(t, alive)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("排队等待 sendSem 的 Probe 在流被驱逐后未在 200ms 内返回 false")
		}
	})
}

// TestSessionWatchHub_CommitGatesPreserveUnrelatedProbeBounds 验证会话 A 的自动回收提交门控或批量删盘提交门控不会使会话 B 的 Probe 绕过预算或上下文取消。
func TestSessionWatchHub_CommitGatesPreserveUnrelatedProbeBounds(t *testing.T) {
	for _, gateType := range []struct {
		name    string
		acquire func(t *testing.T, hub *SessionWatchHub, sid uint64, expectedStreamID uint64) func()
	}{
		{
			name: "AcquireAutoCloseGate",
			acquire: func(t *testing.T, hub *SessionWatchHub, sid uint64, expectedStreamID uint64) func() {
				allowed, unlock := hub.AcquireAutoCloseGate(sid, expectedStreamID)
				require.True(t, allowed)
				return unlock
			},
		},
		{
			name: "AcquireSessionsCommitGate",
			acquire: func(t *testing.T, hub *SessionWatchHub, sid uint64, expectedStreamID uint64) func() {
				allowed, activeMap, unlock := hub.AcquireSessionsCommitGate(map[uint64]uint64{sid: expectedStreamID})
				require.True(t, allowed)
				require.Nil(t, activeMap)
				return unlock
			},
		},
	} {
		t.Run(gateType.name, func(t *testing.T) {
			for _, scenario := range []struct {
				name   string
				budget time.Duration
				ctxTTL time.Duration
			}{
				{name: "探活预算", budget: 20 * time.Millisecond, ctxTTL: 2 * time.Second},
				{name: "上下文取消", budget: 2 * time.Second, ctxTTL: 20 * time.Millisecond},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					hub := NewSessionWatchHub()
					streamA, unregisterA := hub.Register(1, func(uint64) error { return nil })
					unregisterA()
					_, unregisterB := hub.Register(2, func(uint64) error { return nil })
					defer unregisterB()
					unlock := gateType.acquire(t, hub, 1, streamA)
					defer unlock()

					ctx, cancel := context.WithTimeout(context.Background(), scenario.ctxTTL)
					defer cancel()
					probeDone := make(chan bool, 1)
					started := make(chan struct{})
					go func() {
						close(started)
						probeDone <- hub.Probe(ctx, 2, scenario.budget)
					}()
					<-started
					select {
					case alive := <-probeDone:
						assert.False(t, alive)
					case <-time.After(200 * time.Millisecond):
						if scenario.ctxTTL < scenario.budget {
							require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
						} else {
							require.NoError(t, ctx.Err(), "探活预算到期测试的上下文必须仍有效")
						}
						t.Errorf("会话 A 持有提交门控期间，会话 B 的 Probe 未在 200ms 内返回（探活预算=%v，上下文期限=%v）", scenario.budget, scenario.ctxTTL)
						unlock()
						select {
						case alive := <-probeDone:
							assert.False(t, alive)
						case <-time.After(3 * time.Second):
							t.Fatal("释放门控后探活协程仍未退出")
						}
					}
				})
			}
		})
	}
}

// TestSessionWatchHub_AcquireSessionsCommitGateAndCancelRecheck 验证批量提交门控排序获取、并发门控等待阻塞、流代数推进否决以及门控唤醒重入循环时的确定性上下文取消复核。
func TestSessionWatchHub_AcquireSessionsCommitGateAndCancelRecheck(t *testing.T) {
	hub := NewSessionWatchHub()

	// 1. 空基线或全零 sessionID 立即放行
	allowed, activeMap, unlock := hub.AcquireSessionsCommitGate(nil)
	require.True(t, allowed)
	require.Nil(t, activeMap)
	unlock()

	allowed, activeMap, unlock = hub.AcquireSessionsCommitGate(map[uint64]uint64{0: 0})
	require.True(t, allowed)
	require.Nil(t, activeMap)
	unlock()

	// 2. HasVerifiedPong 边界覆盖（streamID=0、未注册、已注册但未 Pong、已 Pong）与入口取消守卫
	assert.False(t, hub.HasVerifiedPong(1, 0))
	assert.False(t, hub.HasVerifiedPong(1, 999))
	s1, unreg1 := hub.Register(1, func(uint64) error { return nil })
	assert.False(t, hub.HasVerifiedPong(1, s1))
	hub.HandlePong(1, s1, 1)
	assert.True(t, hub.HasVerifiedPong(1, s1))

	s1Done := hub.StreamDone(1, s1)
	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	canceledStreamID, canceledUnreg, canceledErr := hub.RegisterContext(canceledCtx, 1, func(uint64) error { return nil })
	require.ErrorIs(t, canceledErr, context.Canceled)
	assert.Zero(t, canceledStreamID)
	assert.Nil(t, canceledUnreg)
	activeID, hasActive := hub.ActiveStreamID(1)
	assert.True(t, hasActive)
	assert.Equal(t, s1, activeID)
	assert.Equal(t, s1, hub.LatestStreamID(1))
	select {
	case <-s1Done:
		t.Fatal("已取消的 RegisterContext 调用不得关闭或覆盖当前活跃流")
	default:
	}
	unreg1()

	// 3. 单会话门控并发等待：先持有会话 1 的门控，并发再次调用 AcquireAutoCloseGate 与 AcquireSessionsCommitGate
	allowed1, unlock1 := hub.AcquireAutoCloseGate(1, s1)
	require.True(t, allowed1)

	waitAutoStarted := make(chan struct{})
	waitAutoDone := make(chan struct{})
	go func() {
		close(waitAutoStarted)
		defer close(waitAutoDone)
		ok, u := hub.AcquireAutoCloseGate(1, s1)
		assert.True(t, ok)
		if u != nil {
			u()
		}
	}()
	<-waitAutoStarted
	prematureAutoDone := false
	select {
	case <-waitAutoDone:
		prematureAutoDone = true
	case <-time.After(30 * time.Millisecond):
	}
	assert.False(t, prematureAutoDone, "首门控未释放前，二次调用 AcquireAutoCloseGate 不得提前返回")
	probeRegCtx1, cancelProbeReg1 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelProbeReg1()
	regID1, regUnreg1, regErr1 := hub.RegisterContext(probeRegCtx1, 1, func(uint64) error { return nil })
	if regUnreg1 != nil {
		regUnreg1()
	}
	assert.ErrorIs(t, regErr1, context.DeadlineExceeded)
	assert.Zero(t, regID1)
	assert.Nil(t, regUnreg1)
	unlock1()
	if !prematureAutoDone {
		select {
		case <-waitAutoDone:
		case <-time.After(time.Second):
			t.Fatal("释放首门控后，等待中的 AcquireAutoCloseGate 应立即完成")
		}
	}

	// 4. 批量会话门控并发等待已有门控释放后成功获取，并在持有批量门控期间阻塞批量内全部会话 Register 直至 unlock() 调用
	allowed1, unlock1 = hub.AcquireAutoCloseGate(1, s1)
	require.True(t, allowed1)
	batchStarted := make(chan struct{})
	batchAcquired := make(chan func(), 1)
	go func() {
		close(batchStarted)
		ok, act, u := hub.AcquireSessionsCommitGate(map[uint64]uint64{2: 0, 1: s1})
		assert.True(t, ok)
		assert.Nil(t, act)
		batchAcquired <- u
	}()
	<-batchStarted
	var prematureBatchUnlock func()
	prematureBatchDone := false
	select {
	case prematureBatchUnlock = <-batchAcquired:
		prematureBatchDone = true
		if prematureBatchUnlock != nil {
			prematureBatchUnlock()
		}
	case <-time.After(30 * time.Millisecond):
	}
	assert.False(t, prematureBatchDone, "首门控未释放前，AcquireSessionsCommitGate 不得提前返回")
	probeRegCtx2, cancelProbeReg2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelProbeReg2()
	regID2, regUnreg2, regErr2 := hub.RegisterContext(probeRegCtx2, 1, func(uint64) error { return nil })
	if regUnreg2 != nil {
		regUnreg2()
	}
	assert.ErrorIs(t, regErr2, context.DeadlineExceeded)
	assert.Zero(t, regID2)
	assert.Nil(t, regUnreg2)
	unlock1()
	if prematureBatchDone {
		t.FailNow()
	}
	var batchUnlock func()
	select {
	case batchUnlock = <-batchAcquired:
	case <-time.After(time.Second):
		t.Fatal("释放首门控后，等待中的 AcquireSessionsCommitGate 应立即完成")
	}
	require.NotNil(t, batchUnlock)

	type batchRegResult struct {
		sid      uint64
		started  chan struct{}
		done     chan struct{}
		streamID uint64
		unreg    func() bool
	}
	batchRegs := []*batchRegResult{
		{sid: 1, started: make(chan struct{}), done: make(chan struct{})},
		{sid: 2, started: make(chan struct{}), done: make(chan struct{})},
	}
	for _, r := range batchRegs {
		go func(r *batchRegResult) {
			close(r.started)
			r.streamID, r.unreg = hub.Register(r.sid, func(uint64) error { return nil })
			close(r.done)
		}(r)
	}
	for _, r := range batchRegs {
		<-r.started
		select {
		case <-r.done:
			t.Fatalf("持有批量会话提交门控期间，会话 %d 的 Register 必须被阻塞", r.sid)
		case <-time.After(30 * time.Millisecond):
		}
	}
	batchUnlock()
	for _, r := range batchRegs {
		select {
		case <-r.done:
		case <-time.After(time.Second):
			t.Fatalf("释放批量会话提交门控后，会话 %d 的 Register 应立即完成", r.sid)
		}
		require.NotZero(t, r.streamID)
		require.NotNil(t, r.unreg)
		r.unreg()
	}
	s1New := batchRegs[0].streamID
	s2GateAfter := batchRegs[1].streamID

	// 5. 会话 1 已推进流代数且当前无活跃流，会话 2 保持活跃流
	s2New, unreg2New := hub.Register(2, func(uint64) error { return nil })
	defer unreg2New()
	assert.Greater(t, s2New, s2GateAfter)
	hub.HandlePong(2, s2New, 1)

	// 5a. 独立覆盖：会话 1 无活跃流但流代数已推进（s1New > s1）时，单会话自动回收门控与批量门控均必须拒绝旧基线
	assert.False(t, hub.HasActiveStream(1))
	assert.Greater(t, s1New, s1)
	allowedStaleAuto, unlockStaleAuto := hub.AcquireAutoCloseGate(1, s1)
	if unlockStaleAuto != nil {
		unlockStaleAuto()
	}
	assert.False(t, allowedStaleAuto)
	assert.Nil(t, unlockStaleAuto)

	allowedStaleBatch, activeStaleBatch, unlockStaleBatch := hub.AcquireSessionsCommitGate(map[uint64]uint64{1: s1})
	if unlockStaleBatch != nil {
		unlockStaleBatch()
	}
	assert.False(t, allowedStaleBatch)
	assert.Nil(t, unlockStaleBatch)
	assert.Equal(t, map[uint64]uint64{1: 0}, activeStaleBatch)

	// 5b. 独立覆盖：会话 2 存在活跃流且流代数与基线相等时，批量门控必须拒绝并返回活跃流映射
	allowedEqualActive, activeEqualBatch, unlockEqualActive := hub.AcquireSessionsCommitGate(map[uint64]uint64{2: s2New})
	if unlockEqualActive != nil {
		unlockEqualActive()
	}
	assert.False(t, allowedEqualActive)
	assert.Nil(t, unlockEqualActive)
	assert.Equal(t, map[uint64]uint64{2: s2New}, activeEqualBatch)
	assert.True(t, hub.HasActiveStream(2))

	// 5c. 组合覆盖：会话 1 流代数已推进且无活跃流，会话 2 存在活跃流
	allowedBatch, activeBatch, unlockBatch := hub.AcquireSessionsCommitGate(map[uint64]uint64{1: s1, 2: s2GateAfter})
	assert.False(t, allowedBatch)
	assert.Nil(t, unlockBatch)
	assert.Equal(t, map[uint64]uint64{1: 0, 2: s2New}, activeBatch)
	assert.Equal(t, s1New, hub.LatestStreamID(1))

	// 6. 门控释放唤醒后重入循环的持锁上下文取消复核（基于 Context.Done() 确定性同步屏障）：
	// 当 RegisterContext 通过首轮 ctx.Err() 检查并进入门控等待 select 求值 ctx.Done() 时，
	// 通知主测试协程同时完成 unlockGate() 与 cancelReg()，确保 select 开始选择时 gateCh 与 ctx.Done() 同时就绪且 ctx.Err() 已为 context.Canceled。
	const sid = uint64(99)
	for i := 0; i < 64; i++ {
		okGate, unlockGate := hub.AcquireAutoCloseGate(sid, 0)
		require.True(t, okGate)
		require.NotNil(t, unlockGate)

		rawCtx, cancelReg := context.WithCancel(context.Background())
		barrierCtx := &selectBarrierContext{
			Context:       rawCtx,
			atSelect:      make(chan struct{}),
			releaseSelect: make(chan struct{}),
		}
		type regResult struct {
			streamID uint64
			unreg    func() bool
			err      error
		}
		resCh := make(chan regResult, 1)
		go func() {
			id, u, err := hub.RegisterContext(barrierCtx, sid, func(uint64) error { return nil })
			resCh <- regResult{streamID: id, unreg: u, err: err}
		}()

		<-barrierCtx.atSelect
		unlockGate()
		cancelReg()
		close(barrierCtx.releaseSelect)

		res := <-resCh
		if res.unreg != nil {
			res.unreg()
		}
		require.ErrorIs(t, res.err, context.Canceled, "第 %d 轮：门控释放与取消同轮就绪时 RegisterContext 必须在重入循环时复核并返回 context.Canceled", i)
		require.Zero(t, res.streamID)
		require.Nil(t, res.unreg)
		require.False(t, hub.HasActiveStream(sid))
		require.Zero(t, hub.LatestStreamID(sid))
	}
}
