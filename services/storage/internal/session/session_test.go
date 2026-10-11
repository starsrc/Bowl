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
		ID:              "sess-1",
		DiskID:          "disk-100",
		ClientID:        "client-a",
		ReadOnly:        false,
		IsWriteLease:    true,
		LeaseExpireTime: time.Now().Add(10 * time.Second),
		CreatedAt:       time.Now(),
	}

	sess2 := &Session{
		ID:              "sess-2",
		DiskID:          "disk-100",
		ClientID:        "client-b",
		ReadOnly:        true,
		IsWriteLease:    false,
		LeaseExpireTime: time.Time{},
		CreatedAt:       time.Now(),
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
