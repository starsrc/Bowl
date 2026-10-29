package meta

import (
	"context"
	"math"
	"sync"
	"time"
)

type watchStreamEntry struct {
	streamID uint64
	sendMu   sync.Mutex
	sendPing func(pingSeq uint64) error
	doneCh   chan struct{}
	waitMu   sync.Mutex
	waiters  map[uint64]chan struct{}
}

// SessionWatchHub 维护客户端活跃的 WatchSession 双向流并提供按需 Ping-Pong 探活能力。
type SessionWatchHub struct {
	mu           sync.Mutex
	nextStreamID uint64
	nextPingSeq  uint64
	streams      map[uint64]*watchStreamEntry
}

// NewSessionWatchHub 创建并发安全的会话流探活中心。
func NewSessionWatchHub() *SessionWatchHub {
	return &SessionWatchHub{
		streams: make(map[uint64]*watchStreamEntry),
	}
}

// Register 注册指定 sessionID 的活跃 WatchSession 流。
// 若该 sessionID 先前已有旧流注册，关闭旧流的 doneCh 并用新流覆盖。
func (h *SessionWatchHub) Register(sessionID uint64, sendPing func(pingSeq uint64) error) (streamID uint64, unregister func() (replaced bool)) {
	h.mu.Lock()
	if h.streams == nil {
		h.streams = make(map[uint64]*watchStreamEntry)
	}
	h.nextStreamID++
	streamID = h.nextStreamID

	if old, ok := h.streams[sessionID]; ok {
		close(old.doneCh)
	}

	entry := &watchStreamEntry{
		streamID: streamID,
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
	return streamID, unregister
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
	ch, waiting := entry.waiters[pongSeq]
	if waiting {
		delete(entry.waiters, pongSeq)
		close(ch)
	}
	entry.waitMu.Unlock()
}

// HasActiveStream 返回指定 sessionID 当前是否存在活跃的 WatchSession 流。
func (h *SessionWatchHub) HasActiveStream(sessionID uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.streams[sessionID]
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

// Probe 向指定 sessionID 的活跃流发送 Ping 探活帧并等待客户端在 timeout 内回复 Pong。
// 若不存在活跃流，立即返回 false（0ms 等待）。
func (h *SessionWatchHub) Probe(ctx context.Context, sessionID uint64, timeout time.Duration) bool {
	h.mu.Lock()
	entry, ok := h.streams[sessionID]
	if !ok {
		h.mu.Unlock()
		return false
	}
	h.nextPingSeq++
	pingSeq := h.nextPingSeq
	h.mu.Unlock()

	pongCh := make(chan struct{})
	entry.waitMu.Lock()
	entry.waiters[pingSeq] = pongCh
	entry.waitMu.Unlock()

	defer func() {
		entry.waitMu.Lock()
		delete(entry.waiters, pingSeq)
		entry.waitMu.Unlock()
	}()

	entry.sendMu.Lock()
	var sendErr error
	if entry.sendPing != nil {
		sendErr = entry.sendPing(pingSeq)
	}
	entry.sendMu.Unlock()
	if sendErr != nil {
		return false
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-pongCh:
		return true
	case <-entry.doneCh:
		return false
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
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
