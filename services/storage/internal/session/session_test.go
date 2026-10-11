package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryManager_CRUD(t *testing.T) {
	mgr := NewMemoryManager()

	sess1 := &Session{
		ID:             "sess-1",
		DiskID:         "disk-100",
		ClientID:       "client-a",
		ReadOnly:       false,
		IsWriteLease:   true,
		LeaseExpiresAt: time.Now().Add(10 * time.Second),
		CreatedAt:      time.Now(),
	}

	sess2 := &Session{
		ID:             "sess-2",
		DiskID:         "disk-100",
		ClientID:       "client-b",
		ReadOnly:       true,
		IsWriteLease:   false,
		LeaseExpiresAt: time.Time{},
		CreatedAt:      time.Now(),
	}

	mgr.Create(sess1)
	mgr.Create(sess2)

	// Get sess1
	got1, found := mgr.Get("sess-1")
	require.True(t, found)
	assert.Equal(t, "sess-1", got1.ID)
	assert.Equal(t, "disk-100", got1.DiskID)
	assert.Equal(t, "client-a", got1.ClientID)
	assert.True(t, got1.IsWriteLease)

	// List by disk ID
	list := mgr.ListByDiskID("disk-100")
	assert.Len(t, list, 2)

	// Non-existent get
	_, found = mgr.Get("non-existent")
	assert.False(t, found)

	// Delete sess1
	deleted, found := mgr.Delete("sess-1")
	assert.True(t, found)
	assert.Equal(t, "sess-1", deleted.ID)

	_, found = mgr.Get("sess-1")
	assert.False(t, found)

	listAfter := mgr.ListByDiskID("disk-100")
	assert.Len(t, listAfter, 1)

	// Delete non-existent
	_, found = mgr.Delete("non-existent")
	assert.False(t, found)
}

func TestMemoryManager_RenewLease_ActiveWriter(t *testing.T) {
	mgr := NewMemoryManager()
	initialExpiry := time.Now().Add(5 * time.Second)

	sess := &Session{
		ID:             "writer-1",
		DiskID:         "disk-1",
		ClientID:       "client-1",
		ReadOnly:       false,
		IsWriteLease:   true,
		LeaseExpiresAt: initialExpiry,
		CreatedAt:      time.Now(),
	}
	mgr.Create(sess)

	time.Sleep(10 * time.Millisecond)

	renewed, err := mgr.RenewLease("writer-1")
	require.NoError(t, err)
	require.NotNil(t, renewed)
	assert.True(t, renewed.IsWriteLease)
	assert.False(t, renewed.Revoked)
	assert.True(t, renewed.LeaseExpiresAt.After(initialExpiry))

	// Verify persistence in manager
	stored, found := mgr.Get("writer-1")
	require.True(t, found)
	assert.True(t, stored.IsWriteLease)
	assert.False(t, stored.Revoked)
	assert.Equal(t, renewed.LeaseExpiresAt, stored.LeaseExpiresAt)
}

func TestMemoryManager_RenewLease_ReadOnly(t *testing.T) {
	mgr := NewMemoryManager()

	sess := &Session{
		ID:             "reader-1",
		DiskID:         "disk-1",
		ClientID:       "client-reader",
		ReadOnly:       true,
		IsWriteLease:   false,
		LeaseExpiresAt: time.Time{},
		CreatedAt:      time.Now(),
	}
	mgr.Create(sess)

	renewed, err := mgr.RenewLease("reader-1")
	require.NoError(t, err)
	require.NotNil(t, renewed)
	assert.False(t, renewed.IsWriteLease)
	assert.False(t, renewed.Revoked)
}

func TestMemoryManager_RenewLease_Expired_TriggersRevocation(t *testing.T) {
	mgr := NewMemoryManager()

	// Session with lease expired in the past
	sess := &Session{
		ID:             "expired-writer",
		DiskID:         "disk-1",
		ClientID:       "client-expired",
		ReadOnly:       false,
		IsWriteLease:   true,
		LeaseExpiresAt: time.Now().Add(-1 * time.Second),
		CreatedAt:      time.Now().Add(-20 * time.Second),
	}
	mgr.Create(sess)

	// RenewLease after timeout should revoke lease
	renewed, err := mgr.RenewLease("expired-writer")
	require.NoError(t, err)
	require.NotNil(t, renewed)
	assert.False(t, renewed.IsWriteLease)
	assert.True(t, renewed.Revoked)

	// Check manager state
	stored, found := mgr.Get("expired-writer")
	require.True(t, found)
	assert.False(t, stored.IsWriteLease)
	assert.True(t, stored.Revoked)

	// Subsequent renewal stays revoked
	renewedAgain, err := mgr.RenewLease("expired-writer")
	require.NoError(t, err)
	assert.False(t, renewedAgain.IsWriteLease)
	assert.True(t, renewedAgain.Revoked)
}

func TestMemoryManager_RenewLease_NotFound(t *testing.T) {
	mgr := NewMemoryManager()

	sess, err := mgr.RenewLease("non-existent-session")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, sess)
}

func TestMemoryManager_CheckExpiredLeases(t *testing.T) {
	mgr := NewMemoryManager()
	baseTime := time.Now()

	sessExpired := &Session{
		ID:             "writer-expired",
		DiskID:         "disk-1",
		ClientID:       "client-1",
		ReadOnly:       false,
		IsWriteLease:   true,
		LeaseExpiresAt: baseTime.Add(-5 * time.Second),
		CreatedAt:      baseTime.Add(-20 * time.Second),
	}
	sessActive := &Session{
		ID:             "writer-active",
		DiskID:         "disk-2",
		ClientID:       "client-2",
		ReadOnly:       false,
		IsWriteLease:   true,
		LeaseExpiresAt: baseTime.Add(10 * time.Second),
		CreatedAt:      baseTime,
	}
	sessReader := &Session{
		ID:             "reader-only",
		DiskID:         "disk-3",
		ClientID:       "client-3",
		ReadOnly:       true,
		IsWriteLease:   false,
		LeaseExpiresAt: time.Time{},
		CreatedAt:      baseTime,
	}

	mgr.Create(sessExpired)
	mgr.Create(sessActive)
	mgr.Create(sessReader)

	// Sweep at baseTime
	revoked := mgr.CheckExpiredLeases(baseTime)
	require.Len(t, revoked, 1)
	assert.Equal(t, "writer-expired", revoked[0].ID)
	assert.False(t, revoked[0].IsWriteLease)
	assert.True(t, revoked[0].Revoked)

	// Verify manager state: active writer untouched
	activeStored, found := mgr.Get("writer-active")
	require.True(t, found)
	assert.True(t, activeStored.IsWriteLease)
	assert.False(t, activeStored.Revoked)

	// Sweep later at baseTime + 15s: now writer-active should also expire
	revokedLater := mgr.CheckExpiredLeases(baseTime.Add(15 * time.Second))
	require.Len(t, revokedLater, 1)
	assert.Equal(t, "writer-active", revokedLater[0].ID)
	assert.False(t, revokedLater[0].IsWriteLease)
	assert.True(t, revokedLater[0].Revoked)
}
