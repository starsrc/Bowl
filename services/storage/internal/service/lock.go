package service

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	connect "connectrpc.com/connect/v2"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/services/storage/internal/session"
)

// DefaultLockTimeout specifies the default timeout for blocking lock acquisitions (5s).
const DefaultLockTimeout = 5 * time.Second

// Flock operation flags matching BSD flock(2)
const (
	FlockOpShared    = 1 // LOCK_SH
	FlockOpExclusive = 2 // LOCK_EX
	FlockOpNB        = 4 // LOCK_NB
	FlockOpUnlock    = 8 // LOCK_UN
)

// ByteRangeLock represents a POSIX byte-range record lock.
type ByteRangeLock struct {
	Start     uint64
	End       uint64
	Type      bowlv1.LockType
	Owner     uint64
	Pid       uint32
	SessionID string
	Fh        uint64
}

// FlockLock represents a BSD whole-file flock.
type FlockLock struct {
	Ino         uint64
	Owner       uint64
	IsExclusive bool
	SessionID   string
	Fh          uint64
}

// fileLockState maintains all locks and waiter notifications for a single Inode.
type fileLockState struct {
	mu         sync.Mutex
	rangeLocks []ByteRangeLock
	flocks     []FlockLock
	notifyCh   chan struct{}
}

func newFileLockState() *fileLockState {
	return &fileLockState{
		notifyCh: make(chan struct{}),
	}
}

func (fl *fileLockState) notifyWaitersLocked() {
	if fl.notifyCh != nil {
		close(fl.notifyCh)
	}
	fl.notifyCh = make(chan struct{})
}

// LockManager manages POSIX byte-range record locks and BSD flocks in memory.
type LockManager struct {
	mu       sync.Mutex
	files    map[uint64]*fileLockState
	sessions session.Manager
}

// NewLockManager initializes an in-memory lock manager.
func NewLockManager(sessions session.Manager) *LockManager {
	return &LockManager{
		files:    make(map[uint64]*fileLockState),
		sessions: sessions,
	}
}

func (lm *LockManager) getFileLockState(ino uint64) *fileLockState {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	st, exists := lm.files[ino]
	if !exists {
		st = newFileLockState()
		lm.files[ino] = st
	}
	return st
}

// purgeTerminatedSessionsLocked removes locks held by terminated or non-existent sessions.
func (lm *LockManager) purgeTerminatedSessionsLocked(fl *fileLockState) bool {
	if lm.sessions == nil {
		return false
	}
	changed := false

	newRanges := make([]ByteRangeLock, 0, len(fl.rangeLocks))
	for _, l := range fl.rangeLocks {
		if l.SessionID != "" {
			if _, found := lm.sessions.Get(l.SessionID); !found {
				changed = true
				continue
			}
		}
		newRanges = append(newRanges, l)
	}
	fl.rangeLocks = newRanges

	newFlocks := make([]FlockLock, 0, len(fl.flocks))
	for _, f := range fl.flocks {
		if f.SessionID != "" {
			if _, found := lm.sessions.Get(f.SessionID); !found {
				changed = true
				continue
			}
		}
		newFlocks = append(newFlocks, f)
	}
	fl.flocks = newFlocks

	if changed {
		fl.notifyWaitersLocked()
	}
	return changed
}

// checkRangeConflictLocked checks whether [start, end] conflicts with any existing lock by another owner.
func checkRangeConflictLocked(fl *fileLockState, owner uint64, start, end uint64, lockType bowlv1.LockType) *ByteRangeLock {
	for i := range fl.rangeLocks {
		existing := &fl.rangeLocks[i]
		if existing.Owner == owner {
			continue // Locks by the same owner never conflict
		}
		// Check range overlap
		if existing.Start <= end && start <= existing.End {
			// Conflict if either is exclusive
			if lockType == bowlv1.LockType_LOCK_TYPE_EXCLUSIVE || existing.Type == bowlv1.LockType_LOCK_TYPE_EXCLUSIVE {
				return existing
			}
		}
	}
	return nil
}

// carveOutOwnerRangeLocked removes [start, end] from all locks of 'owner' in fl.rangeLocks.
func carveOutOwnerRangeLocked(fl *fileLockState, owner uint64, start, end uint64) {
	var result []ByteRangeLock
	for _, l := range fl.rangeLocks {
		if l.Owner != owner {
			result = append(result, l)
			continue
		}
		if !(l.Start <= end && start <= l.End) {
			result = append(result, l)
			continue
		}
		// Case 1: [start, end] completely encloses l -> remove
		if start <= l.Start && end >= l.End {
			continue
		}
		// Case 2: [start, end] strictly inside l -> split into two
		if start > l.Start && end < l.End {
			left := l
			left.End = start - 1
			right := l
			right.Start = end + 1
			result = append(result, left, right)
			continue
		}
		// Case 3: [start, end] overlaps left of l -> truncate left
		if start <= l.Start && end < l.End {
			rem := l
			rem.Start = end + 1
			result = append(result, rem)
			continue
		}
		// Case 4: [start, end] overlaps right of l -> truncate right
		if start > l.Start && end >= l.End {
			rem := l
			rem.End = start - 1
			result = append(result, rem)
			continue
		}
	}
	fl.rangeLocks = result
}

// mergeOwnerRangeLocksLocked merges adjacent or touching locks for 'owner' with same LockType.
func mergeOwnerRangeLocksLocked(fl *fileLockState, owner uint64) {
	var ownerLocks []ByteRangeLock
	var otherLocks []ByteRangeLock

	for _, l := range fl.rangeLocks {
		if l.Owner == owner {
			ownerLocks = append(ownerLocks, l)
		} else {
			otherLocks = append(otherLocks, l)
		}
	}

	if len(ownerLocks) <= 1 {
		return
	}

	sort.Slice(ownerLocks, func(i, j int) bool {
		return ownerLocks[i].Start < ownerLocks[j].Start
	})

	merged := make([]ByteRangeLock, 0, len(ownerLocks))
	cur := ownerLocks[0]

	for i := 1; i < len(ownerLocks); i++ {
		next := ownerLocks[i]
		if cur.Type == next.Type && cur.End != math.MaxUint64 && next.Start <= cur.End+1 {
			if next.End > cur.End {
				cur.End = next.End
			}
		} else {
			merged = append(merged, cur)
			cur = next
		}
	}
	merged = append(merged, cur)

	fl.rangeLocks = append(otherLocks, merged...)
}

// GetRangeConflict queries any conflicting range lock for testing.
func (lm *LockManager) GetRangeConflict(ino uint64, owner uint64, start, end uint64, lockType bowlv1.LockType) *ByteRangeLock {
	fl := lm.getFileLockState(ino)
	fl.mu.Lock()
	defer fl.mu.Unlock()

	lm.purgeTerminatedSessionsLocked(fl)
	return checkRangeConflictLocked(fl, owner, start, end, lockType)
}

// TryAcquireRange attempts to acquire a byte-range lock without blocking.
func (lm *LockManager) TryAcquireRange(
	ino uint64,
	owner uint64,
	start, end uint64,
	lockType bowlv1.LockType,
	pid uint32,
	sessionID string,
	fh uint64,
) (bool, *ByteRangeLock, <-chan struct{}) {
	fl := lm.getFileLockState(ino)
	fl.mu.Lock()
	defer fl.mu.Unlock()

	lm.purgeTerminatedSessionsLocked(fl)

	conflict := checkRangeConflictLocked(fl, owner, start, end, lockType)
	if conflict != nil {
		cp := *conflict
		return false, &cp, fl.notifyCh
	}

	carveOutOwnerRangeLocked(fl, owner, start, end)
	fl.rangeLocks = append(fl.rangeLocks, ByteRangeLock{
		Start:     start,
		End:       end,
		Type:      lockType,
		Owner:     owner,
		Pid:       pid,
		SessionID: sessionID,
		Fh:        fh,
	})
	mergeOwnerRangeLocksLocked(fl, owner)
	fl.notifyWaitersLocked()
	return true, nil, nil
}

// UnlockRange releases the specified range for 'owner'.
func (lm *LockManager) UnlockRange(ino uint64, owner uint64, start, end uint64) {
	fl := lm.getFileLockState(ino)
	fl.mu.Lock()
	defer fl.mu.Unlock()

	lm.purgeTerminatedSessionsLocked(fl)
	carveOutOwnerRangeLocked(fl, owner, start, end)
	fl.notifyWaitersLocked()
}

func checkFlockConflictLocked(fl *fileLockState, owner uint64, isExclusive bool) bool {
	for _, f := range fl.flocks {
		if f.Owner == owner {
			continue // Same owner
		}
		if isExclusive {
			return true // Exclusive conflicts with ANY lock
		}
		if f.IsExclusive {
			return true // Shared conflicts with exclusive lock
		}
	}
	return false
}

func applyFlockLocked(fl *fileLockState, ino uint64, owner uint64, isExclusive bool, isUnlock bool, sessionID string, fh uint64) {
	newFlocks := make([]FlockLock, 0, len(fl.flocks))
	for _, f := range fl.flocks {
		if f.Owner == owner {
			continue
		}
		newFlocks = append(newFlocks, f)
	}

	if !isUnlock {
		newFlocks = append(newFlocks, FlockLock{
			Ino:         ino,
			Owner:       owner,
			IsExclusive: isExclusive,
			SessionID:   sessionID,
			Fh:          fh,
		})
	}
	fl.flocks = newFlocks
	fl.notifyWaitersLocked()
}

// TryAcquireFlock attempts to acquire a BSD flock without blocking.
func (lm *LockManager) TryAcquireFlock(
	ino uint64,
	owner uint64,
	isExclusive bool,
	sessionID string,
	fh uint64,
) (bool, <-chan struct{}) {
	fl := lm.getFileLockState(ino)
	fl.mu.Lock()
	defer fl.mu.Unlock()

	lm.purgeTerminatedSessionsLocked(fl)

	if checkFlockConflictLocked(fl, owner, isExclusive) {
		return false, fl.notifyCh
	}

	applyFlockLocked(fl, ino, owner, isExclusive, false, sessionID, fh)
	return true, nil
}

// UnlockFlock releases the BSD flock for 'owner' on 'ino'.
func (lm *LockManager) UnlockFlock(ino uint64, owner uint64) {
	fl := lm.getFileLockState(ino)
	fl.mu.Lock()
	defer fl.mu.Unlock()

	applyFlockLocked(fl, ino, owner, false, true, "", 0)
}

// ReleaseHandleLocks releases all byte-range and flock locks associated with an open handle or owner.
func (lm *LockManager) ReleaseHandleLocks(sessionID string, ino uint64, fh uint64, lockOwner uint64) {
	fl := lm.getFileLockState(ino)
	fl.mu.Lock()
	defer fl.mu.Unlock()

	changed := false
	newRanges := make([]ByteRangeLock, 0, len(fl.rangeLocks))
	for _, l := range fl.rangeLocks {
		if (fh != 0 && l.Fh == fh) || (lockOwner != 0 && l.Owner == lockOwner && l.SessionID == sessionID) {
			changed = true
			continue
		}
		newRanges = append(newRanges, l)
	}
	fl.rangeLocks = newRanges

	newFlocks := make([]FlockLock, 0, len(fl.flocks))
	for _, f := range fl.flocks {
		if (fh != 0 && f.Fh == fh) || (lockOwner != 0 && f.Owner == lockOwner && f.SessionID == sessionID) {
			changed = true
			continue
		}
		newFlocks = append(newFlocks, f)
	}
	fl.flocks = newFlocks

	if changed {
		fl.notifyWaitersLocked()
	}
}

// ReleaseSessionLocks releases all locks held by a session across all files.
func (lm *LockManager) ReleaseSessionLocks(sessionID string) {
	if sessionID == "" {
		return
	}
	lm.mu.Lock()
	fileStates := make([]*fileLockState, 0, len(lm.files))
	for _, fl := range lm.files {
		fileStates = append(fileStates, fl)
	}
	lm.mu.Unlock()

	for _, fl := range fileStates {
		fl.mu.Lock()
		changed := false

		newRanges := make([]ByteRangeLock, 0, len(fl.rangeLocks))
		for _, l := range fl.rangeLocks {
			if l.SessionID == sessionID {
				changed = true
				continue
			}
			newRanges = append(newRanges, l)
		}
		fl.rangeLocks = newRanges

		newFlocks := make([]FlockLock, 0, len(fl.flocks))
		for _, f := range fl.flocks {
			if f.SessionID == sessionID {
				changed = true
				continue
			}
			newFlocks = append(newFlocks, f)
		}
		fl.flocks = newFlocks

		if changed {
			fl.notifyWaitersLocked()
		}
		fl.mu.Unlock()
	}
}

// =========================================================================
// StorageService RPC Handlers
// =========================================================================

// validateSessionAndFH validates session existence, file handle existence, ownership, and Inode matching.
func (s *StorageService) validateSessionAndFH(sessionID string, fh uint64, reqIno uint64) (*session.Session, *OpenFileHandle, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if fh == 0 {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, "fh must be greater than 0")
	}

	sess, found := s.sessions.Get(sessionID)
	if !found {
		return nil, nil, connect.Errorf(connect.CodeNotFound, "session not found: %s", sessionID)
	}

	val, ok := s.openHandles.Load(fh)
	if !ok {
		return nil, nil, connect.Errorf(connect.CodeNotFound, "file handle %d not found", fh)
	}
	handle := val.(*OpenFileHandle)
	if handle.SessionID != sessionID {
		return nil, nil, connect.Errorf(connect.CodePermissionDenied, "file handle %d does not belong to session %s", fh, sessionID)
	}
	if reqIno != 0 && reqIno != handle.Handle.GetIno() {
		return nil, nil, connect.Errorf(connect.CodeInvalidArgument, "ino %d does not match handle ino %d", reqIno, handle.Handle.GetIno())
	}
	return sess, handle, nil
}

// normalizeLockRange validates start and end offsets and normalizes end=0 to math.MaxUint64.
func normalizeLockRange(lk *bowlv1.PlockRecord) (uint64, uint64, error) {
	if lk == nil {
		return 0, 0, connect.NewError(connect.CodeInvalidArgument, "lk record is required")
	}
	start := lk.GetStart()
	end := lk.GetEnd()
	var normEnd uint64 = math.MaxUint64
	if end != 0 {
		if end < start {
			return 0, 0, connect.NewError(connect.CodeInvalidArgument, "invalid lock range: end must be >= start")
		}
		normEnd = end
	}
	return start, normEnd, nil
}

// GetLk tests whether a requested POSIX byte-range record lock conflicts with an existing lock (FUSE_GETLK).
func (s *StorageService) GetLk(ctx context.Context, req *bowlv1.GetLkRequest) (*bowlv1.GetLkResponse, error) {
	_, handle, err := s.validateSessionAndFH(req.GetSessionId(), req.GetFh(), req.GetIno())
	if err != nil {
		return nil, err
	}
	ino := handle.Handle.GetIno()

	start, end, err := normalizeLockRange(req.GetLk())
	if err != nil {
		return nil, err
	}

	owner := req.GetLockOwner()
	if owner == 0 {
		owner = req.GetFh()
	}

	lockType := req.GetLk().GetType()
	if lockType == bowlv1.LockType_LOCK_TYPE_UNSPECIFIED {
		lockType = bowlv1.LockType_LOCK_TYPE_SHARED
	}

	conflict := s.lockMgr.GetRangeConflict(ino, owner, start, end, lockType)
	if conflict != nil {
		return &bowlv1.GetLkResponse{
			Lk: &bowlv1.PlockRecord{
				Start: conflict.Start,
				End:   conflict.End,
				Type:  conflict.Type,
				Pid:   conflict.Pid,
			},
		}, nil
	}

	return &bowlv1.GetLkResponse{
		Lk: &bowlv1.PlockRecord{
			Type: bowlv1.LockType_LOCK_TYPE_UNLOCK,
		},
	}, nil
}

// SetLk non-blockingly acquires or releases a POSIX byte-range record lock (FUSE_SETLK).
func (s *StorageService) SetLk(ctx context.Context, req *bowlv1.SetLkRequest) (*bowlv1.SetLkResponse, error) {
	sess, handle, err := s.validateSessionAndFH(req.GetSessionId(), req.GetFh(), req.GetIno())
	if err != nil {
		return nil, err
	}
	ino := handle.Handle.GetIno()

	start, end, err := normalizeLockRange(req.GetLk())
	if err != nil {
		return nil, err
	}

	owner := req.GetLockOwner()
	if owner == 0 {
		owner = req.GetFh()
	}

	switch req.GetLk().GetType() {
	case bowlv1.LockType_LOCK_TYPE_UNLOCK:
		s.lockMgr.UnlockRange(ino, owner, start, end)
		return &bowlv1.SetLkResponse{}, nil

	case bowlv1.LockType_LOCK_TYPE_SHARED, bowlv1.LockType_LOCK_TYPE_EXCLUSIVE:
		acquired, _, _ := s.lockMgr.TryAcquireRange(
			ino,
			owner,
			start,
			end,
			req.GetLk().GetType(),
			req.GetLk().GetPid(),
			sess.ID,
			req.GetFh(),
		)
		if !acquired {
			return nil, connect.NewError(connect.CodeFailedPrecondition, "lock conflict")
		}
		return &bowlv1.SetLkResponse{}, nil

	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, "invalid lock type")
	}
}

// SetLkw blocking acquires a POSIX byte-range record lock (FUSE_SETLKW).
func (s *StorageService) SetLkw(ctx context.Context, req *bowlv1.SetLkwRequest) (*bowlv1.SetLkwResponse, error) {
	sess, handle, err := s.validateSessionAndFH(req.GetSessionId(), req.GetFh(), req.GetIno())
	if err != nil {
		return nil, err
	}
	ino := handle.Handle.GetIno()

	start, end, err := normalizeLockRange(req.GetLk())
	if err != nil {
		return nil, err
	}

	owner := req.GetLockOwner()
	if owner == 0 {
		owner = req.GetFh()
	}

	switch req.GetLk().GetType() {
	case bowlv1.LockType_LOCK_TYPE_UNLOCK:
		s.lockMgr.UnlockRange(ino, owner, start, end)
		return &bowlv1.SetLkwResponse{}, nil

	case bowlv1.LockType_LOCK_TYPE_SHARED, bowlv1.LockType_LOCK_TYPE_EXCLUSIVE:
		timeout := s.lockTimeout
		if timeout <= 0 {
			timeout = DefaultLockTimeout
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		for {
			acquired, _, ch := s.lockMgr.TryAcquireRange(
				ino,
				owner,
				start,
				end,
				req.GetLk().GetType(),
				req.GetLk().GetPid(),
				sess.ID,
				req.GetFh(),
			)
			if acquired {
				return &bowlv1.SetLkwResponse{}, nil
			}

			select {
			case <-ctx.Done():
				return nil, connect.Errorf(connect.CodeCanceled, "canceled waiting for lock: %s", ctx.Err())
			case <-timer.C:
				return nil, connect.NewError(connect.CodeDeadlineExceeded, "timed out waiting for lock")
			case <-ch:
				// File lock state changed, retry acquisition
			}
		}

	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, "invalid lock type")
	}
}

// Flock acquires or releases a BSD whole-file advisory lock.
func (s *StorageService) Flock(ctx context.Context, req *bowlv1.FlockRequest) (*bowlv1.FlockResponse, error) {
	sess, handle, err := s.validateSessionAndFH(req.GetSessionId(), req.GetFh(), req.GetIno())
	if err != nil {
		return nil, err
	}
	ino := handle.Handle.GetIno()

	owner := req.GetLockOwner()
	if owner == 0 {
		owner = req.GetFh()
	}

	flags := req.GetFlags()
	isNB := (flags & FlockOpNB) != 0
	var isExclusive, isUnlock bool

	switch req.GetType() {
	case bowlv1.LockType_LOCK_TYPE_SHARED:
		// Shared lock: isExclusive = false, isUnlock = false
	case bowlv1.LockType_LOCK_TYPE_EXCLUSIVE:
		isExclusive = true
	case bowlv1.LockType_LOCK_TYPE_UNLOCK:
		isUnlock = true
	default:
		if flags&FlockOpUnlock != 0 {
			isUnlock = true
		} else if flags&FlockOpExclusive != 0 {
			isExclusive = true
		} else if flags&FlockOpShared != 0 {
			// Shared lock
		} else {
			return nil, connect.NewError(connect.CodeInvalidArgument, "invalid or missing lock type / op")
		}
	}

	if isUnlock {
		s.lockMgr.UnlockFlock(ino, owner)
		return &bowlv1.FlockResponse{}, nil
	}

	if isNB {
		acquired, _ := s.lockMgr.TryAcquireFlock(ino, owner, isExclusive, sess.ID, req.GetFh())
		if !acquired {
			return nil, connect.NewError(connect.CodeFailedPrecondition, "flock conflict")
		}
		return &bowlv1.FlockResponse{}, nil
	}

	// Blocking flock
	timeout := s.lockTimeout
	if timeout <= 0 {
		timeout = DefaultLockTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		acquired, ch := s.lockMgr.TryAcquireFlock(ino, owner, isExclusive, sess.ID, req.GetFh())
		if acquired {
			return &bowlv1.FlockResponse{}, nil
		}

		select {
		case <-ctx.Done():
			return nil, connect.Errorf(connect.CodeCanceled, "canceled waiting for flock: %s", ctx.Err())
		case <-timer.C:
			return nil, connect.NewError(connect.CodeDeadlineExceeded, "timed out waiting for flock")
		case <-ch:
			// Lock state changed, retry acquisition
		}
	}
}
