package session

import (
	"sync"
	"time"
)

// Session represents an active FUSE client mount session.
type Session struct {
	ID              string
	DiskID          string
	ClientID        string
	ReadOnly        bool
	IsWriteLease    bool
	LeaseExpireTime time.Time
	CreatedAt       time.Time
}

// Manager defines the interface for managing active mount sessions in memory.
type Manager interface {
	Create(sess *Session)
	Get(sessionID string) (*Session, bool)
	Delete(sessionID string) (*Session, bool)
	ListByDiskID(diskID string) []*Session
}

// MemoryManager provides thread-safe in-memory session management.
type MemoryManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewMemoryManager initializes an empty MemoryManager.
func NewMemoryManager() *MemoryManager {
	return &MemoryManager{
		sessions: make(map[string]*Session),
	}
}

// Create stores a new session into memory.
func (m *MemoryManager) Create(sess *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[sess.ID] = sess
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
