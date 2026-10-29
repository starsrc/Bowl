package meta_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// 会话 A 的自动回收提交门控或批量删盘提交门控不得使会话 B 的 Probe 绕过预算或上下文取消。
func TestReviewLogicR4_AutoCloseGatePreservesUnrelatedProbeBounds(t *testing.T) {
	for _, gateType := range []struct {
		name    string
		acquire func(t *testing.T, hub *meta.SessionWatchHub, sid uint64, expectedStreamID uint64) func()
	}{
		{
			name: "AcquireAutoCloseGate",
			acquire: func(t *testing.T, hub *meta.SessionWatchHub, sid uint64, expectedStreamID uint64) func() {
				allowed, unlock := hub.AcquireAutoCloseGate(sid, expectedStreamID)
				require.True(t, allowed)
				return unlock
			},
		},
		{
			name: "AcquireSessionsCommitGate",
			acquire: func(t *testing.T, hub *meta.SessionWatchHub, sid uint64, expectedStreamID uint64) func() {
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
					hub := meta.NewSessionWatchHub()
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

// AcquireSessionsCommitGate 必须支持按排序批量获取会话门控、并发等待已有门控释放，
// 且在持有批量门控期间阻塞同会话 Register 直至 unlock() 调用，并在发现新流或代数推进时准确返回需要探活的会话映射。
func TestReviewLogicR4_AcquireSessionsCommitGateBranches(t *testing.T) {
	hub := meta.NewSessionWatchHub()

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

	// 6. 门控释放唤醒后重入循环的持锁上下文取消复核：
	// 当 RegisterContext 从 <-gateCh 唤醒并 continue 重入循环时，若此时 ctx 也已取消，必须在持锁复核时立即拒绝，不得登记新流或推进代数。
	func() {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
		const sid = uint64(99)
		for i := 0; i < 64; i++ {
			okGate, unlockGate := hub.AcquireAutoCloseGate(sid, 0)
			require.True(t, okGate)
			require.NotNil(t, unlockGate)

			regCtx, cancelReg := context.WithCancel(context.Background())
			started := make(chan struct{})
			type regResult struct {
				streamID uint64
				unreg    func() bool
				err      error
			}
			resCh := make(chan regResult, 1)
			go func() {
				close(started)
				id, u, err := hub.RegisterContext(regCtx, sid, func(uint64) error { return nil })
				resCh <- regResult{streamID: id, unreg: u, err: err}
			}()
			<-started
			runtime.Gosched()

			unlockGate()
			cancelReg()

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
	}()
}
