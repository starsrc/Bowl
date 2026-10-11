package service

import (
	"context"
	"sync"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

// Helper to setup two mounted sessions and open handles on the same regular test file.
func setupTwoSessionsOnFile(t *testing.T) (h *testHarness, sess1ID, sess2ID string, ino, fh1, fh2 uint64, diskID uuid.UUID, cleanup func()) {
	t.Helper()
	h = setupTestHarness(t)
	ctx := context.Background()
	diskID = uuid.New()
	schemaName := FormatDiskSchema(diskID)

	// Mount session 1
	mount1, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-1",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sess1ID = mount1.GetSessionId()

	// Mount session 2
	mount2, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "client-2",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sess2ID = mount2.GetSessionId()

	// Create regular file in session 1
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sess1ID,
		ParentIno: 1,
		Name:      "lock_test.txt",
		Mode:      0o644,
		Flags:     2, // O_RDWR
	})
	require.NoError(t, err)
	ino = createResp.GetInode().GetIno()
	fh1 = createResp.GetFileHandle().GetFh()
	require.Greater(t, ino, uint64(1))
	require.Greater(t, fh1, uint64(0))

	// Open handle for session 2 on the newly created regular file
	open2, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sess2ID,
		Ino:       ino,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_READ_WRITE,
	})
	require.NoError(t, err)
	fh2 = open2.GetFileHandle().GetFh()
	require.Greater(t, fh2, uint64(0))

	cleanup = func() {
		_, _ = h.dbPool.Exec(context.Background(), "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
		h.cleanup()
	}

	return h, sess1ID, sess2ID, ino, fh1, fh2, diskID, cleanup
}

func TestStorageService_Lock_SharedReadLocksConcurrent(t *testing.T) {
	h, sess1ID, sess2ID, ino, fh1, fh2, _, cleanup := setupTwoSessionsOnFile(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Session 1 acquires shared read lock on [0, 100]
	owner1 := uint64(1001)
	_, err := h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
			Pid:   101,
		},
	})
	require.NoError(t, err)

	// 2. Session 2 concurrently acquires shared read lock on overlapping [50, 150]
	owner2 := uint64(1002)
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 50,
			End:   150,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
			Pid:   102,
		},
	})
	require.NoError(t, err)

	// 3. GetLk queries from both owners should report NO CONFLICT (LOCK_TYPE_UNLOCK)
	get1, err := h.storageClient.GetLk(ctx, &bowlv1.GetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.LockType_LOCK_TYPE_UNLOCK, get1.GetLk().GetType())

	get2, err := h.storageClient.GetLk(ctx, &bowlv1.GetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 50,
			End:   150,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.LockType_LOCK_TYPE_UNLOCK, get2.GetLk().GetType())
}

func TestStorageService_Lock_ExclusiveWriteLockConflict(t *testing.T) {
	h, sess1ID, sess2ID, ino, fh1, fh2, _, cleanup := setupTwoSessionsOnFile(t)
	defer cleanup()

	ctx := context.Background()
	owner1 := uint64(2001)
	owner2 := uint64(2002)

	// 1. Session 1 acquires shared read lock on [0, 100]
	_, err := h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
			Pid:   201,
		},
	})
	require.NoError(t, err)

	// 2. Session 2 tries to acquire exclusive write lock on overlapping [50, 150] -> conflicts!
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 50,
			End:   150,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
			Pid:   202,
		},
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 3. Session 2 GetLk queries write lock on [50, 150] -> reports conflicting lock from Session 1
	getResp, err := h.storageClient.GetLk(ctx, &bowlv1.GetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 50,
			End:   150,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.GetLk())
	assert.Equal(t, bowlv1.LockType_LOCK_TYPE_SHARED, getResp.GetLk().GetType())
	assert.Equal(t, uint64(0), getResp.GetLk().GetStart())
	assert.Equal(t, uint64(100), getResp.GetLk().GetEnd())
	assert.Equal(t, uint32(201), getResp.GetLk().GetPid())

	// 4. Session 1 unlocks [0, 100]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_UNLOCK,
		},
	})
	require.NoError(t, err)

	// 5. Session 2 can now acquire exclusive write lock on [50, 150]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 50,
			End:   150,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
			Pid:   202,
		},
	})
	require.NoError(t, err)

	// 6. Session 1 now conflicts for write lock on [100, 200]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 100,
			End:   200,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 7. Session 1 also conflicts for read lock on [100, 200]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 100,
			End:   200,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
		},
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 8. Session 1 non-overlapping read lock on [200, 300] succeeds
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 200,
			End:   300,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
		},
	})
	require.NoError(t, err)
}

func TestStorageService_Flock_SharedVsExclusiveMutualExclusion(t *testing.T) {
	h, sess1ID, sess2ID, ino, fh1, fh2, _, cleanup := setupTwoSessionsOnFile(t)
	defer cleanup()

	ctx := context.Background()
	owner1 := uint64(3001)
	owner2 := uint64(3002)

	// 1. Session 1 acquires Flock SHARED (LOCK_SH = 1)
	_, err := h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Type:      bowlv1.LockType_LOCK_TYPE_SHARED,
	})
	require.NoError(t, err)

	// 2. Session 2 acquires Flock SHARED concurrently -> succeeds
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Type:      bowlv1.LockType_LOCK_TYPE_SHARED,
	})
	require.NoError(t, err)

	// 3. Session 2 tries to acquire Flock EXCLUSIVE non-blocking (flags = LOCK_NB = 4) -> fails
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		Flags:     FlockOpNB,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 4. Session 1 unlocks (LOCK_UN)
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Type:      bowlv1.LockType_LOCK_TYPE_UNLOCK,
	})
	require.NoError(t, err)

	// 5. Session 2 upgrades to EXCLUSIVE
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
	})
	require.NoError(t, err)

	// 6. Session 1 tries to acquire SHARED with LOCK_NB -> fails
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Type:      bowlv1.LockType_LOCK_TYPE_SHARED,
		Flags:     FlockOpNB,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 7. Session 2 unlocks
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Type:      bowlv1.LockType_LOCK_TYPE_UNLOCK,
	})
	require.NoError(t, err)

	// 8. Session 1 acquires EXCLUSIVE
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
	})
	require.NoError(t, err)
}

func TestStorageService_Lock_UnlockReleasesLock(t *testing.T) {
	h, sess1ID, sess2ID, ino, fh1, fh2, _, cleanup := setupTwoSessionsOnFile(t)
	defer cleanup()

	ctx := context.Background()
	owner1 := uint64(4001)
	owner2 := uint64(4002)

	// 1. Session 1 acquires write lock on [0, 100]
	_, err := h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)

	// 2. Session 2 cannot acquire [0, 100]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 3. Session 1 unlocks entire range [0, 100]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_UNLOCK,
		},
	})
	require.NoError(t, err)

	// 4. Session 2 acquires write lock on [0, 100]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)

	// 5. Session 2 partially unlocks [30, 60] (middle hole)
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 30,
			End:   60,
			Type:  bowlv1.LockType_LOCK_TYPE_UNLOCK,
		},
	})
	require.NoError(t, err)

	// 6. Session 1 acquires write lock in the freed hole [30, 60] -> succeeds!
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 30,
			End:   60,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)

	// 7. Session 1 trying to acquire [0, 50] conflicts because Session 2 still holds [0, 29]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   50,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

func TestStorageService_Lock_SessionTerminationReleasesLocks(t *testing.T) {
	h, sess1ID, sess2ID, ino, fh1, fh2, _, cleanup := setupTwoSessionsOnFile(t)
	defer cleanup()

	ctx := context.Background()
	owner1 := uint64(5001)
	owner2 := uint64(5002)

	// 1. Session 1 acquires WRLCK on [0, 500] and Flock EXCLUSIVE
	_, err := h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   500,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)

	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
	})
	require.NoError(t, err)

	// 2. Session 2 conflicts on both
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   500,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		Flags:     FlockOpNB,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 3. Unmount Session 1 (simulating session termination / disconnect)
	_, err = h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: sess1ID,
	})
	require.NoError(t, err)

	// 4. Session 2 can now acquire the WRLCK and Flock EXCLUSIVE
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   500,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)

	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		Flags:     FlockOpNB,
	})
	require.NoError(t, err)
}

func TestStorageService_Lock_BlockingSetLkwAndFlock(t *testing.T) {
	h, sess1ID, sess2ID, ino, fh1, fh2, _, cleanup := setupTwoSessionsOnFile(t)
	defer cleanup()

	ctx := context.Background()
	owner1 := uint64(6001)
	owner2 := uint64(6002)

	// 1. Session 1 acquires WRLCK on [0, 100]
	_, err := h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)

	// 2. Session 2 blocks on SetLkw in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	var setLkwErr error

	go func() {
		defer wg.Done()
		_, setLkwErr = h.storageClient.SetLkw(ctx, &bowlv1.SetLkwRequest{
			SessionId: sess2ID,
			Ino:       ino,
			Fh:        fh2,
			LockOwner: owner2,
			Lk: &bowlv1.PlockRecord{
				Start: 0,
				End:   100,
				Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
			},
		})
	}()

	// Give the goroutine time to enter blocking wait
	time.Sleep(100 * time.Millisecond)

	// 3. Session 1 unlocks [0, 100]
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_UNLOCK,
		},
	})
	require.NoError(t, err)

	// 4. Session 2 should unblock and succeed
	wg.Wait()
	require.NoError(t, setLkwErr)

	// 5. Blocking Flock test: Session 2 currently holds WRLCK, let's test Flock blocking
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
	})
	require.NoError(t, err)

	var flockWg sync.WaitGroup
	flockWg.Add(1)
	var flockErr error

	go func() {
		defer flockWg.Done()
		_, flockErr = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
			SessionId: sess1ID,
			Ino:       ino,
			Fh:        fh1,
			LockOwner: owner1,
			Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
			Flags:     0, // Blocking
		})
	}()

	time.Sleep(100 * time.Millisecond)

	// Session 2 unlocks Flock
	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Type:      bowlv1.LockType_LOCK_TYPE_UNLOCK,
	})
	require.NoError(t, err)

	flockWg.Wait()
	require.NoError(t, flockErr)
}

func TestStorageService_Lock_HandleCloseReleasesLocks(t *testing.T) {
	h, sess1ID, sess2ID, ino, fh1, fh2, _, cleanup := setupTwoSessionsOnFile(t)
	defer cleanup()

	ctx := context.Background()
	owner1 := uint64(7001)
	owner2 := uint64(7002)

	// 1. Session 1 acquires WRLCK on fh1
	_, err := h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)

	// 2. Session 2 cannot acquire
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.Error(t, err)

	// 3. Release handle fh1
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sess1ID,
		Ino:       ino,
		Fh:        fh1,
		LockOwner: owner1,
	})
	require.NoError(t, err)

	// 4. Session 2 can now acquire lock
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess2ID,
		Ino:       ino,
		Fh:        fh2,
		LockOwner: owner2,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
		},
	})
	require.NoError(t, err)
}

func TestStorageService_Lock_ValidationErrors(t *testing.T) {
	h, sess1ID, _, ino, fh1, _, _, cleanup := setupTwoSessionsOnFile(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Missing session_id -> InvalidArgument
	_, err := h.storageClient.GetLk(ctx, &bowlv1.GetLkRequest{SessionId: "", Fh: fh1, Ino: ino, Lk: &bowlv1.PlockRecord{}})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{SessionId: "", Fh: fh1, Ino: ino, Lk: &bowlv1.PlockRecord{}})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = h.storageClient.Flock(ctx, &bowlv1.FlockRequest{SessionId: "", Fh: fh1, Ino: ino})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. Fh = 0 -> InvalidArgument
	_, err = h.storageClient.GetLk(ctx, &bowlv1.GetLkRequest{SessionId: sess1ID, Fh: 0, Ino: ino, Lk: &bowlv1.PlockRecord{}})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. Nil Lk -> InvalidArgument
	_, err = h.storageClient.GetLk(ctx, &bowlv1.GetLkRequest{SessionId: sess1ID, Fh: fh1, Ino: ino, Lk: nil})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 4. Invalid range: end > 0 && end < start -> InvalidArgument
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Fh:        fh1,
		Ino:       ino,
		Lk: &bowlv1.PlockRecord{
			Start: 100,
			End:   50,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
		},
	})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 5. Unknown session_id -> NotFound
	unknownSessionID := uuid.New().String()
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: unknownSessionID,
		Fh:        fh1,
		Ino:       ino,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
		},
	})
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 6. Unknown fh -> NotFound
	_, err = h.storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sess1ID,
		Fh:        999999,
		Ino:       ino,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   100,
			Type:  bowlv1.LockType_LOCK_TYPE_SHARED,
		},
	})
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
