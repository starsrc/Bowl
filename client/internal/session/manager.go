package session

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// ErrBadFileHandle 表示文件句柄无效、未绑定或其所属会话已淘汰失效（对齐 POSIX EBADF 语义）。
var ErrBadFileHandle = syscall.EBADF

const (
	DefaultMinInterval         = 200 * time.Millisecond
	DefaultMaxInterval         = 20 * time.Second
	DefaultInitialRetryBackoff = 200 * time.Millisecond
	DefaultMaxRetryBackoff     = 2 * time.Second
)

// StorageClient 抽象 StorageService 的 RenewSession、CloseSession 与 WatchSession RPC 契约。
type StorageClient interface {
	RenewSession(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
	CloseSession(context.Context, *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error)
	WatchSession(context.Context) *connect.BidiStreamForClient[storagev1.WatchSessionRequest, storagev1.WatchSessionResponse]
}

// Config 定义 ClientSession 生命周期的参数配置。
type Config struct {
	DiskID              string
	MinInterval         time.Duration
	MaxInterval         time.Duration
	InitialRetryBackoff time.Duration
	MaxRetryBackoff     time.Duration
	Logger              *slog.Logger
}

// Manager 负责 ClientSession 自适应保活、超期自愈轮换及句柄血缘绑定。
type Manager struct {
	client StorageClient
	cfg    Config
	logger *slog.Logger

	// 当前活跃会话 ID
	currentSessionID atomic.Uint64

	// 句柄血缘映射与已淘汰会话集合
	handleMu        sync.RWMutex
	handleSessions  map[uint64]uint64
	retiredSessions map[uint64]struct{}

	// 后台续期与监听协程生命周期控制
	loopCtx     context.Context
	cancelLoop  context.CancelFunc
	loopDone    chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	watchMu     sync.Mutex
	watchCancel context.CancelFunc

	// 最新租约截止时间
	expiresAtMu sync.RWMutex
	expiresAt   time.Time
}

// New 生成 63 位随机正整数并调用 RenewSession 激活初始 ClientSession。
// 若初始激活失败则返回错误，阻止挂载进入就绪态。
func New(ctx context.Context, client StorageClient, cfg Config) (*Manager, error) {
	if client == nil {
		return nil, errors.New("storage client is nil")
	}

	normalizeConfig(&cfg)

	initialSID, err := generateSessionID(0)
	if err != nil {
		return nil, fmt.Errorf("generate initial session id: %w", err)
	}

	resp, err := client.RenewSession(ctx, newRenewSessionRequest(initialSID, cfg.DiskID))
	if err != nil {
		return nil, fmt.Errorf("activate initial session: %w", err)
	}

	var expiresAt time.Time
	if expTs := resp.Msg.GetExpiresAt(); expTs != nil {
		expiresAt = expTs.AsTime()
	} else {
		expiresAt = time.Now().Add(60 * time.Second)
	}

	loopCtx, cancelLoop := context.WithCancel(context.Background())

	m := &Manager{
		client:          client,
		cfg:             cfg,
		logger:          cfg.Logger,
		handleSessions:  make(map[uint64]uint64),
		retiredSessions: make(map[uint64]struct{}),
		loopCtx:         loopCtx,
		cancelLoop:      cancelLoop,
		loopDone:        make(chan struct{}),
		expiresAt:       expiresAt,
	}
	m.currentSessionID.Store(initialSID)

	return m, nil
}

// Start 启动后台自适应租约续期保活与 WatchSession 双向流监听协程。
// 协程会按 (expiresAt - now) / 3 动态心跳保活；收到 Close 信号时干净终止。
func (m *Manager) Start(ctx context.Context) error {
	var alreadyStarted bool
	m.startOnce.Do(func() {
		alreadyStarted = true
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			m.renewLoop(m.loopCtx)
		}()
		go func() {
			defer wg.Done()
			m.watchLoop(m.loopCtx)
		}()
		go func() {
			wg.Wait()
			close(m.loopDone)
		}()
	})
	if !alreadyStarted {
		return errors.New("session manager renew loop already started")
	}
	return nil
}

func (m *Manager) renewLoop(ctx context.Context) {

	currentBackoff := m.cfg.InitialRetryBackoff

	for {
		m.expiresAtMu.RLock()
		exp := m.expiresAt
		m.expiresAtMu.RUnlock()

		now := time.Now()
		var interval time.Duration
		rem := exp.Sub(now)
		if rem <= 0 {
			interval = m.cfg.MinInterval
		} else {
			interval = rem / 3
			if interval < m.cfg.MinInterval {
				interval = m.cfg.MinInterval
			} else if interval > m.cfg.MaxInterval {
				interval = m.cfg.MaxInterval
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}

		sid := m.currentSessionID.Load()
		resp, err := m.client.RenewSession(ctx, newRenewSessionRequest(sid, m.cfg.DiskID))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Warn("renew session failed, retrying with backoff",
				"session_id", sid,
				"error", err,
				"backoff", currentBackoff,
			)

			select {
			case <-ctx.Done():
				return
			case <-time.After(currentBackoff):
			}

			currentBackoff = min(currentBackoff*2, m.cfg.MaxRetryBackoff)
			continue
		}

		currentBackoff = m.cfg.InitialRetryBackoff

		if resp.Msg.GetSessionRecreated() {
			m.handleSessionExpired(ctx, sid)
			continue
		}

		if expTs := resp.Msg.GetExpiresAt(); expTs != nil {
			m.expiresAtMu.Lock()
			m.expiresAt = expTs.AsTime()
			m.expiresAtMu.Unlock()
		}
	}
}

func (m *Manager) handleSessionExpired(ctx context.Context, oldSID uint64) {
	newSID, err := generateSessionID(oldSID)
	if err != nil {
		m.logger.Error("failed to generate new session id on session expiry", "error", err)
		return
	}

	m.logger.Warn("client session expired on server; rotating session_id",
		"old_session_id", oldSID,
		"new_session_id", newSID,
	)

	// 将旧会话登记到已退役集合中，使后续对旧会话句柄的访问立即失效
	m.handleMu.Lock()
	m.retiredSessions[oldSID] = struct{}{}
	m.handleMu.Unlock()

	// 激活新会话，遇到临时网络错误时以指数退避重试保活
	backoff := m.cfg.InitialRetryBackoff
	for {
		resp, err := m.client.RenewSession(ctx, newRenewSessionRequest(newSID, m.cfg.DiskID))
		if err == nil {
			// 新会话激活成功，原子切换当前活跃会话 ID
			m.currentSessionID.Store(newSID)
			m.cancelCurrentWatch()

			var newExp time.Time
			if expTs := resp.Msg.GetExpiresAt(); expTs != nil {
				newExp = expTs.AsTime()
			} else {
				newExp = time.Now().Add(60 * time.Second)
			}
			m.expiresAtMu.Lock()
			m.expiresAt = newExp
			m.expiresAtMu.Unlock()
			return
		}

		if ctx.Err() != nil {
			return
		}

		m.logger.Warn("activate rotated session failed, retrying with backoff",
			"new_session_id", newSID,
			"error", err,
			"backoff", backoff,
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, m.cfg.MaxRetryBackoff)
	}
}

// CurrentSessionID 返回当前活跃会话 ID。
func (m *Manager) CurrentSessionID() uint64 {
	return m.currentSessionID.Load()
}

// BindHandle 在 Open/Create/OpenDir 成功时登记句柄与打开时刻使用的 sessionID。
// 仅记录 fh -> session_id 整数映射，绝不缓存文件属性或数据。
func (m *Manager) BindHandle(fh uint64, sessionID uint64) {
	m.handleMu.Lock()
	m.handleSessions[fh] = sessionID
	m.handleMu.Unlock()
}

// LookupHandleSession 查询指定 fh 打开时登记的 sessionID。
// 若句柄未注册则返回 (0, false)。
func (m *Manager) LookupHandleSession(fh uint64) (uint64, bool) {
	m.handleMu.RLock()
	sid, ok := m.handleSessions[fh]
	m.handleMu.RUnlock()
	return sid, ok
}

// ResolveHandleSession 校验并获取指定 fh 绑定的有效 sessionID。
// 若句柄不存在或其绑定的会话已被判定淘汰/退役，返回 (0, ErrBadFileHandle) (syscall.EBADF)。
// 严格杜绝借用新 CurrentSessionID 伪造续命老句柄。
func (m *Manager) ResolveHandleSession(fh uint64) (uint64, error) {
	m.handleMu.RLock()
	defer m.handleMu.RUnlock()

	sid, ok := m.handleSessions[fh]
	if !ok {
		return 0, ErrBadFileHandle
	}

	if _, retired := m.retiredSessions[sid]; retired {
		return sid, ErrBadFileHandle
	}
	if sid != m.currentSessionID.Load() {
		return sid, ErrBadFileHandle
	}

	return sid, nil
}

// UnbindHandle 在 Release/ReleaseDir 完成时清理句柄映射条目，避免内存泄漏。
func (m *Manager) UnbindHandle(fh uint64) {
	m.handleMu.Lock()
	delete(m.handleSessions, fh)
	m.handleMu.Unlock()
}

// Close 优雅停止后台续期协程并尽力注销当前活跃会话。
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.cancelLoop()
		m.cancelCurrentWatch()
		// 若后台协程未启动过，直接关闭 loopDone 避免死锁
		m.startOnce.Do(func() {
			close(m.loopDone)
		})
		<-m.loopDone

		if sid := m.currentSessionID.Load(); sid > 0 {
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := m.client.CloseSession(closeCtx, connect.NewRequest(&storagev1.CloseSessionRequest{
				SessionId: sid,
			})); err != nil {
				m.logger.Warn("close session failed on shutdown",
					"session_id", sid,
					"error", err,
				)
			}
		}
	})
	return nil
}

func (m *Manager) setWatchCancel(cancel context.CancelFunc) {
	m.watchMu.Lock()
	m.watchCancel = cancel
	m.watchMu.Unlock()
}

func (m *Manager) cancelCurrentWatch() {
	m.watchMu.Lock()
	cancel := m.watchCancel
	m.watchCancel = nil
	m.watchMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (m *Manager) watchLoop(ctx context.Context) {
	currentBackoff := m.cfg.InitialRetryBackoff

	for {
		if ctx.Err() != nil {
			return
		}

		sid := m.currentSessionID.Load()
		streamCtx, streamCancel := context.WithCancel(ctx)
		m.setWatchCancel(streamCancel)

		stream := m.client.WatchSession(streamCtx)
		doneClose := make(chan struct{})
		go func() {
			select {
			case <-streamCtx.Done():
				_ = stream.CloseRequest()
				_ = stream.CloseResponse()
			case <-doneClose:
			}
		}()

		sendErr := stream.Send(&storagev1.WatchSessionRequest{
			SessionId: sid,
			DiskId:    optionalDiskID(m.cfg.DiskID),
			PongSeq:   0,
		})
		if sendErr == nil {
			for {
				resp, recvErr := stream.Receive()
				if recvErr != nil {
					break
				}
				currentBackoff = m.cfg.InitialRetryBackoff
				if pingSeq := resp.GetPingSeq(); pingSeq > 0 {
					if err := stream.Send(&storagev1.WatchSessionRequest{
						SessionId: sid,
						PongSeq:   pingSeq,
					}); err != nil {
						break
					}
				}
			}
		}

		close(doneClose)
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
		streamCancel()

		if ctx.Err() != nil {
			return
		}

		// 若会话已发生轮换，无需退避等待，立即使用新 session_id 建立新流
		if m.currentSessionID.Load() != sid {
			currentBackoff = m.cfg.InitialRetryBackoff
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(currentBackoff):
		}
		currentBackoff = min(currentBackoff*2, m.cfg.MaxRetryBackoff)
	}
}

func optionalDiskID(diskID string) *string {
	if diskID == "" {
		return nil
	}
	return &diskID
}

func newRenewSessionRequest(sessionID uint64, diskID string) *connect.Request[storagev1.RenewSessionRequest] {
	req := &storagev1.RenewSessionRequest{
		SessionId: sessionID,
	}
	if diskID != "" {
		req.DiskId = &diskID
	}
	return connect.NewRequest(req)
}

func normalizeConfig(cfg *Config) {
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = DefaultMinInterval
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = DefaultMaxInterval
	}
	if cfg.MinInterval > cfg.MaxInterval {
		cfg.MinInterval, cfg.MaxInterval = cfg.MaxInterval, cfg.MinInterval
	}
	if cfg.InitialRetryBackoff <= 0 {
		cfg.InitialRetryBackoff = DefaultInitialRetryBackoff
	}
	if cfg.MaxRetryBackoff <= 0 {
		cfg.MaxRetryBackoff = DefaultMaxRetryBackoff
	}
	if cfg.InitialRetryBackoff > cfg.MaxRetryBackoff {
		cfg.InitialRetryBackoff, cfg.MaxRetryBackoff = cfg.MaxRetryBackoff, cfg.InitialRetryBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
}

// generateSessionID 生成在 [1, math.MaxInt64] 范围内的加密安全随机 63 位正整数。
func generateSessionID(exclude uint64) (uint64, error) {
	for {
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err != nil {
			return 0, fmt.Errorf("read crypto random: %w", err)
		}
		// 掩码 0x7FFFFFFFFFFFFFFF 确保结果落在 [0, math.MaxInt64] 范围内
		sid := binary.BigEndian.Uint64(buf[:]) & 0x7FFFFFFFFFFFFFFF
		if sid > 0 && sid != exclude {
			return sid, nil
		}
	}
}
