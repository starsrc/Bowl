package session

import (
	"errors"
	"sync"
	"time"
)

// Session represents an active FUSE client mount session.
type Session struct {
	ID             string
	DiskID         string
	ClientID       string
	ReadOnly       bool
	IsWriteLease   bool
	LeaseExpiresAt time.Time
	Revoked        bool
	CreatedAt      time.Time
}

// DefaultLeaseTimeout defines the default write lease duration (10 seconds).
const DefaultLeaseTimeout = 10 * time.Second

// ErrNotFound is returned when a requested session is not found.
var ErrNotFound = errors.New("session not found")

// Manager defines the interface for managing active mount sessions in memory.
type Manager interface {
	Create(sess *Session)
	Get(sessionID string) (*Session, bool)
	Delete(sessionID string) (*Session, bool)
	ListByDiskID(diskID string) []*Session
	RenewLease(sessionID string) (*Session, error)
	CheckExpiredLeases(now time.Time) []*Session
}

// MemoryManagerOption configures MemoryManager parameters.
type MemoryManagerOption func(*MemoryManager)

// WithLeaseTimeout overrides the default lease duration for MemoryManager.
func WithLeaseTimeout(d time.Duration) MemoryManagerOption {
	return func(m *MemoryManager) {
		m.leaseTimeout = d
	}
}

// MemoryManager provides thread-safe in-memory session management.
type MemoryManager struct {
	mu           sync.RWMutex
	sessions     map[string]*Session
	leaseTimeout time.Duration
}

// NewMemoryManager initializes an empty MemoryManager.
func NewMemoryManager(opts ...MemoryManagerOption) *MemoryManager {
	m := &MemoryManager{
		sessions:     make(map[string]*Session),
		leaseTimeout: DefaultLeaseTimeout,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Create stores a new session into memory.
func (m *MemoryManager) Create(sess *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *sess
	m.sessions[sess.ID] = &cp
}

// Get retrieves a session by its ID.
func (m *MemoryManager) Get(sessionID string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sess, found := m.sessions[sessionID]
	if !found {
		return nil, false
	}
	cp := *sess
	return &cp, true
}

// Delete removes a session by its ID and returns the removed session.
func (m *MemoryManager) Delete(sessionID string) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess, found := m.sessions[sessionID]
	if !found {
		return nil, false
	}
	delete(m.sessions, sessionID)
	cp := *sess
	return &cp, true
}

// ListByDiskID lists all sessions associated with a specific disk ID.
func (m *MemoryManager) ListByDiskID(diskID string) []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*Session
	for _, sess := range m.sessions {
		if sess.DiskID == diskID {
			cp := *sess
			result = append(result, &cp)
		}
	}
	return result
}

// RenewLease renews the write lease for the specified session.
// If the session is not found, ErrNotFound is returned.
// If the session is read-only or not holding a write lease, it returns the session with IsWriteLease: false.
// If the session has already expired, the write lease is revoked (IsWriteLease: false, Revoked: true).
// Otherwise, LeaseExpiresAt is extended by DefaultLeaseTimeout.
func (m *MemoryManager) RenewLease(sessionID string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sess, found := m.sessions[sessionID]
	if !found {
		return nil, ErrNotFound
	}

	// Read-only session
	if sess.ReadOnly {
		cp := *sess
		cp.IsWriteLease = false
		return &cp, nil
	}

	// Already revoked session
	if sess.Revoked {
		sess.IsWriteLease = false
		cp := *sess
		return &cp, nil
	}

	// If session does not hold write lease
	if !sess.IsWriteLease {
		cp := *sess
		return &cp, nil
	}

	// Check if already expired
	now := time.Now()
	if now.After(sess.LeaseExpiresAt) {
		sess.IsWriteLease = false
		sess.Revoked = true
		cp := *sess
		return &cp, nil
	}

	// Active write lease: extend expiration
	timeout := m.leaseTimeout
	if timeout <= 0 {
		timeout = DefaultLeaseTimeout
	}
	sess.LeaseExpiresAt = now.Add(timeout)
	cp := *sess
	return &cp, nil
}

// CheckExpiredLeases scans all active sessions and revokes write leases whose LeaseExpiresAt is before now.
// It returns a slice of sessions whose write leases were revoked during this check.
func (m *MemoryManager) CheckExpiredLeases(now time.Time) []*Session {
	m.mu.Lock()
	defer m.mu.Unlock()

	var revoked []*Session
	for _, sess := range m.sessions {
		if sess.IsWriteLease && now.After(sess.LeaseExpiresAt) {
			sess.IsWriteLease = false
			sess.Revoked = true
			cp := *sess
			revoked = append(revoked, &cp)
		}
	}
	return revoked
}
