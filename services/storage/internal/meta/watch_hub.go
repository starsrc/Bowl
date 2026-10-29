package meta

import (
	"context"
	"math"
	"slices"
	"sync"
	"time"
)

type disconnectedStreamIDKey struct{}

// WithDisconnectedStreamID 在上下文中记录触发断连清理的旧流代数 streamID，
// 供 CloseSession 在事务提交前校验该会话是否已被新流接管。
func WithDisconnectedStreamID(ctx context.Context, streamID uint64) context.Context {
	return context.WithValue(ctx, disconnectedStreamIDKey{}, streamID)
}

func disconnectedStreamIDFromContext(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	id, ok := ctx.Value(disconnectedStreamIDKey{}).(uint64)
	return id, ok && id > 0
}

type watchStreamEntry struct {
	streamID     uint64
	sendSem      chan struct{}
	sendPing     func(pingSeq uint64) error
	doneCh       chan struct{}
	waitMu       sync.Mutex
	waiters      map[uint64]chan struct{}
	lastPongTime time.Time
}

// WatchHub 负责管理客户端会话的 WatchSession 流注册与按需探活。
type WatchHub = SessionWatchHub

// SessionWatchHub 维护客户端活跃的 WatchSession 双向流并提供按需 Ping-Pong 探活能力。
type SessionWatchHub struct {
	mu           sync.Mutex
	nextStreamID uint64
	nextPingSeq  uint64
	streams      map[uint64]*watchStreamEntry
	latestStream map[uint64]uint64
	commitGates  map[uint64]chan struct{}
}

// NewSessionWatchHub 创建并发安全的会话流探活中心。
func NewSessionWatchHub() *SessionWatchHub {
	return &SessionWatchHub{
		streams:      make(map[uint64]*watchStreamEntry),
		latestStream: make(map[uint64]uint64),
		commitGates:  make(map[uint64]chan struct{}),
	}
}

// Register 注册指定 sessionID 的活跃 WatchSession 流。
// 若该 sessionID 先前已有旧流注册，关闭旧流的 doneCh 并用新流覆盖。
func (h *SessionWatchHub) Register(sessionID uint64, sendPing func(pingSeq uint64) error) (streamID uint64, unregister func() (replaced bool)) {
	streamID, unregister, _ = h.RegisterContext(context.Background(), sessionID, sendPing)
	return streamID, unregister
}

// RegisterContext 注册指定 sessionID 的活跃 WatchSession 流，并在等待同会话提交门控期间响应 ctx 取消。
// 若在获取流注册前 ctx 已取消，立即返回 0, nil, ctx.Err()，不修改流代数与活跃流表。
func (h *SessionWatchHub) RegisterContext(ctx context.Context, sessionID uint64, sendPing func(pingSeq uint64) error) (streamID uint64, unregister func() (replaced bool), err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		h.mu.Lock()
		if err := ctx.Err(); err != nil {
			h.mu.Unlock()
			return 0, nil, err
		}
		if gateCh, gated := h.commitGates[sessionID]; gated {
			h.mu.Unlock()
			select {
			case <-gateCh:
				continue
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			}
		}
		if h.streams == nil {
			h.streams = make(map[uint64]*watchStreamEntry)
		}
		if h.latestStream == nil {
			h.latestStream = make(map[uint64]uint64)
		}
		h.nextStreamID++
		streamID = h.nextStreamID
		h.latestStream[sessionID] = streamID

		if old, ok := h.streams[sessionID]; ok {
			close(old.doneCh)
		}

		sendSem := make(chan struct{}, 1)
		sendSem <- struct{}{}

		entry := &watchStreamEntry{
			streamID: streamID,
			sendSem:  sendSem,
			sendPing: sendPing,
			doneCh:   make(chan struct{}),
			waiters:  make(map[uint64]chan struct{}),
		}
		h.streams[sessionID] = entry
		h.mu.Unlock()

		var once sync.Once
		var wasReplaced bool
		unregister = func() bool {
			once.Do(func() {
				h.mu.Lock()
				cur, ok := h.streams[sessionID]
				if ok && cur.streamID == streamID {
					delete(h.streams, sessionID)
					close(entry.doneCh)
					wasReplaced = false
				} else {
					wasReplaced = true
				}
				h.mu.Unlock()
			})
			return wasReplaced
		}
		return streamID, unregister, nil
	}
}

// LatestStreamID 返回指定 sessionID 最新注册过的 WatchSession 流代数（若从未注册则返回 0）。
func (h *SessionWatchHub) LatestStreamID(sessionID uint64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.latestStream[sessionID]
}

// HasVerifiedPong 返回指定 sessionID 与 streamID 的活跃流是否已成功收到过客户端回复的 Pong 帧。
func (h *SessionWatchHub) HasVerifiedPong(sessionID uint64, streamID uint64) bool {
	if streamID == 0 {
		return false
	}
	h.mu.Lock()
	entry, ok := h.streams[sessionID]
	if !ok || entry.streamID != streamID {
		h.mu.Unlock()
		return false
	}
	h.mu.Unlock()

	entry.waitMu.Lock()
	verified := !entry.lastPongTime.IsZero()
	entry.waitMu.Unlock()
	return verified
}

// AcquireAutoCloseGate 在会话自动回收事务提交前获取与该 sessionID 的 Register 互斥的细粒度门控锁，
// 原子校验该 sessionID 当前既无活跃流、且在 expectedMaxStreamID > 0 时最新注册流代数未超过 expectedMaxStreamID。
// 若校验通过返回 true 及 unlock 释放函数（调用方必须在事务提交结束后调用 unlock）；若已被新流接管则返回 false, nil。
func (h *SessionWatchHub) AcquireAutoCloseGate(sessionID uint64, expectedMaxStreamID uint64) (allowed bool, unlock func()) {
	for {
		h.mu.Lock()
		if gateCh, gated := h.commitGates[sessionID]; gated {
			h.mu.Unlock()
			<-gateCh
			continue
		}
		if _, active := h.streams[sessionID]; active {
			h.mu.Unlock()
			return false, nil
		}
		if expectedMaxStreamID > 0 && h.latestStream[sessionID] != expectedMaxStreamID {
			h.mu.Unlock()
			return false, nil
		}
		if h.commitGates == nil {
			h.commitGates = make(map[uint64]chan struct{})
		}
		gateCh := make(chan struct{})
		h.commitGates[sessionID] = gateCh
		h.mu.Unlock()

		var once sync.Once
		return true, func() {
			once.Do(func() {
				h.mu.Lock()
				if cur, ok := h.commitGates[sessionID]; ok && cur == gateCh {
					delete(h.commitGates, sessionID)
				}
				close(gateCh)
				h.mu.Unlock()
			})
		}
	}
}

// AcquireSessionsCommitGate 在级联删盘等批量会话事务提交前按 sessionID 升序原子获取细粒度提交门控，
// 校验所有目标会话当前均无活跃流且最新流代数仍等于 expectedLatestStreams 基线。
// 若全部会话校验通过，返回 true, nil 及 unlock 释放函数；若任一会话已出现活跃流或流代数已推进，
// 立即释放已获取的门控并返回 false 与当前需要探活的 (sessionID -> streamID) 映射。
func (h *SessionWatchHub) AcquireSessionsCommitGate(expectedLatestStreams map[uint64]uint64) (allowed bool, activeStreams map[uint64]uint64, unlock func()) {
	if len(expectedLatestStreams) == 0 {
		return true, nil, func() {}
	}
	sids := make([]uint64, 0, len(expectedLatestStreams))
	for sid := range expectedLatestStreams {
		if sid > 0 {
			sids = append(sids, sid)
		}
	}
	if len(sids) == 0 {
		return true, nil, func() {}
	}
	slices.Sort(sids)
	sids = slices.Compact(sids)

	for {
		h.mu.Lock()
		var blockingGate chan struct{}
		for _, sid := range sids {
			if gateCh, gated := h.commitGates[sid]; gated {
				blockingGate = gateCh
				break
			}
		}
		if blockingGate != nil {
			h.mu.Unlock()
			<-blockingGate
			continue
		}

		var failed bool
		for _, sid := range sids {
			_, active := h.streams[sid]
			if active || h.latestStream[sid] != expectedLatestStreams[sid] {
				failed = true
				break
			}
		}
		if failed {
			activeStreams = make(map[uint64]uint64)
			for _, sid := range sids {
				if entry, active := h.streams[sid]; active {
					activeStreams[sid] = entry.streamID
				} else if h.latestStream[sid] != expectedLatestStreams[sid] {
					activeStreams[sid] = 0
				}
			}
			h.mu.Unlock()
			return false, activeStreams, nil
		}

		if h.commitGates == nil {
			h.commitGates = make(map[uint64]chan struct{})
		}
		acquired := make(map[uint64]chan struct{}, len(sids))
		for _, sid := range sids {
			gateCh := make(chan struct{})
			h.commitGates[sid] = gateCh
			acquired[sid] = gateCh
		}
		h.mu.Unlock()

		var once sync.Once
		return true, nil, func() {
			once.Do(func() {
				h.mu.Lock()
				for sid, gateCh := range acquired {
					if cur, ok := h.commitGates[sid]; ok && cur == gateCh {
						delete(h.commitGates, sid)
					}
					close(gateCh)
				}
				h.mu.Unlock()
			})
		}
	}
}

// HandlePong 处理客户端在 WatchSession 流上回复的 Pong 序列号。
func (h *SessionWatchHub) HandlePong(sessionID uint64, streamID uint64, pongSeq uint64) {
	if pongSeq == 0 {
		return
	}
	h.mu.Lock()
	entry, ok := h.streams[sessionID]
	if !ok || entry.streamID != streamID {
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()

	entry.waitMu.Lock()
	entry.lastPongTime = time.Now()
	ch, waiting := entry.waiters[pongSeq]
	if waiting {
		delete(entry.waiters, pongSeq)
		close(ch)
	}
	entry.waitMu.Unlock()
}

// ActiveStreamID 返回指定 sessionID 当前活跃 WatchSession 流的 streamID。
func (h *SessionWatchHub) ActiveStreamID(sessionID uint64) (uint64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.streams[sessionID]
	if !ok {
		return 0, false
	}
	return entry.streamID, true
}

// StreamDone 返回指定 sessionID 与 expectedStreamID 匹配的活跃流的关闭信号通道。
// 若该流已被注销或替换，返回已关闭的通道。
func (h *SessionWatchHub) StreamDone(sessionID uint64, expectedStreamID uint64) <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.streams[sessionID]
	if !ok || (expectedStreamID > 0 && entry.streamID != expectedStreamID) {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return entry.doneCh
}

// HasActiveStream 返回指定 sessionID 当前是否存在活跃的 WatchSession 流。
func (h *SessionWatchHub) HasActiveStream(sessionID uint64) bool {
	_, ok := h.ActiveStreamID(sessionID)
	return ok
}

// Evict 强制移除指定 sessionID 的活跃流注册并关闭其完成通道。
func (h *SessionWatchHub) Evict(sessionID uint64) {
	h.mu.Lock()
	if entry, ok := h.streams[sessionID]; ok {
		delete(h.streams, sessionID)
		close(entry.doneCh)
	}
	h.mu.Unlock()
}

// EvictIfStreamID 仅当指定 sessionID 当前关联的活跃流代数仍为 expectedStreamID 时将其移除，返回是否成功驱逐。
func (h *SessionWatchHub) EvictIfStreamID(sessionID uint64, expectedStreamID uint64) bool {
	if expectedStreamID == 0 {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.streams[sessionID]
	if !ok || entry.streamID != expectedStreamID {
		return false
	}
	delete(h.streams, sessionID)
	close(entry.doneCh)
	return true
}

// Probe 向指定 sessionID 的活跃流发送 Ping 探活帧并等待客户端在 timeout 内回复 Pong。
// 若不存在活跃流，立即返回 false（0ms 等待）。
func (h *SessionWatchHub) Probe(ctx context.Context, sessionID uint64, timeout time.Duration) bool {
	alive, _ := h.ProbeStream(ctx, sessionID, 0, timeout)
	return alive
}

// ProbeStream 向指定 sessionID 的活跃流（若 expectedStreamID > 0 则要求流代数严格匹配）发送 Ping 探活帧并等待客户端在 timeout 内回复 Pong。
// 返回该流是否存活，以及本次实际被探测的 streamID（若未找到匹配流则返回 0）。
func (h *SessionWatchHub) ProbeStream(ctx context.Context, sessionID uint64, expectedStreamID uint64, timeout time.Duration) (alive bool, probedStreamID uint64) {
	if err := ctx.Err(); err != nil {
		return false, 0
	}

	h.mu.Lock()
	entry, ok := h.streams[sessionID]
	if !ok || (expectedStreamID > 0 && entry.streamID != expectedStreamID) {
		h.mu.Unlock()
		return false, 0
	}
	probedStreamID = entry.streamID
	h.nextPingSeq++
	pingSeq := h.nextPingSeq
	h.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	pongCh := make(chan struct{})
	entry.waitMu.Lock()
	entry.waiters[pingSeq] = pongCh
	entry.waitMu.Unlock()

	defer func() {
		entry.waitMu.Lock()
		delete(entry.waiters, pingSeq)
		entry.waitMu.Unlock()
	}()

	select {
	case <-entry.sendSem:
	case <-pongCh:
		return true, probedStreamID
	case <-entry.doneCh:
		return false, probedStreamID
	case <-timer.C:
		return false, probedStreamID
	case <-ctx.Done():
		return false, probedStreamID
	}

	sendErrCh := make(chan error, 1)
	go func() {
		defer func() {
			entry.sendSem <- struct{}{}
		}()
		var sendErr error
		if entry.sendPing != nil {
			sendErr = entry.sendPing(pingSeq)
		}
		sendErrCh <- sendErr
	}()

	select {
	case sendErr := <-sendErrCh:
		if sendErr != nil {
			return false, probedStreamID
		}
	case <-pongCh:
		return true, probedStreamID
	case <-entry.doneCh:
		return false, probedStreamID
	case <-timer.C:
		return false, probedStreamID
	case <-ctx.Done():
		return false, probedStreamID
	}

	select {
	case <-pongCh:
		return true, probedStreamID
	case <-entry.doneCh:
		return false, probedStreamID
	case <-timer.C:
		return false, probedStreamID
	case <-ctx.Done():
		return false, probedStreamID
	}
}

// ValidateWatchSession 校验 WatchSession 首帧声明的 sessionID 与可选 diskID。
func ValidateWatchSession(sessionID uint64, diskID *string) error {
	if sessionID == 0 || sessionID > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if diskID != nil {
		if _, err := parseDiskID(*diskID); err != nil {
			return ErrInvalidArgument
		}
	}
	return nil
}

// ValidateWatchSession 校验 WatchSession 首帧声明的 sessionID 与可选 diskID。
func (m *manager) ValidateWatchSession(sessionID uint64, diskID *string) error {
	return ValidateWatchSession(sessionID, diskID)
}
