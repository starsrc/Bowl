package fusefs

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type lockHolder struct {
	id uint64
	fh bool
}

type localFileLock struct {
	holder lockHolder
	lock   fuse.FileLock
}

type lockNode struct {
	posix   []localFileLock
	flock   []localFileLock
	waiters []*lockWaiter
	changed chan struct{}
}

type lockWaiter struct {
	request  fuse.LkIn
	canceled bool
}

type LockTable struct {
	mu      sync.Mutex
	nodes   map[uint64]*lockNode
	handles map[uint64]uint32
	closed  bool
	metrics atomic.Pointer[Metrics]
}

func NewLockTable() *LockTable {
	return &LockTable{nodes: make(map[uint64]*lockNode), handles: make(map[uint64]uint32)}
}

func (t *LockTable) bindMetrics(m *Metrics) {
	t.metrics.Store(m)
}

func (t *LockTable) activeLocks() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total int
	for _, node := range t.nodes {
		total += len(node.posix) + len(node.flock)
	}
	return total
}

func (t *LockTable) RegisterHandle(fh uint64, flags uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handles[fh] = flags
}

func (t *LockTable) ForgetHandle(fh uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handles, fh)
}

func (t *LockTable) ReleasePOSIXByOwner(ino, owner uint64) {
	t.release(ino, false, func(holder lockHolder) bool { return holder == (lockHolder{id: owner}) })
}

func (t *LockTable) ReleaseFlockByFH(ino, fh, owner uint64) {
	t.release(ino, true, func(holder lockHolder) bool {
		return (fh != 0 && holder == (lockHolder{id: fh, fh: true})) || holder == (lockHolder{id: owner})
	})
}

func (t *LockTable) release(ino uint64, flock bool, match func(lockHolder) bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.nodes[ino]
	if node == nil {
		return
	}
	locks := &node.posix
	if flock {
		locks = &node.flock
	}
	kept := (*locks)[:0]
	for _, entry := range *locks {
		if !match(entry.holder) {
			kept = append(kept, entry)
		}
	}
	*locks = kept
	waiters := node.waiters[:0]
	for _, waiter := range node.waiters {
		if (waiter.request.LkFlags&fuse.FUSE_LK_FLOCK != 0) == flock && match(lockIdentity(&waiter.request)) {
			waiter.canceled = true
		} else {
			waiters = append(waiters, waiter)
		}
	}
	node.waiters = waiters
	node.notify()
	t.prune(ino, node)
}

func (t *LockTable) prune(ino uint64, node *lockNode) {
	if len(node.posix) == 0 && len(node.flock) == 0 && len(node.waiters) == 0 && t.nodes[ino] == node {
		delete(t.nodes, ino)
	}
}

func (t *LockTable) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for _, node := range t.nodes {
		for _, waiter := range node.waiters {
			waiter.canceled = true
		}
		node.notify()
	}
	clear(t.nodes)
	clear(t.handles)
}

func lockIdentity(in *fuse.LkIn) lockHolder {
	if in.LkFlags&fuse.FUSE_LK_FLOCK != 0 && in.Fh != 0 {
		return lockHolder{id: in.Fh, fh: true}
	}
	return lockHolder{id: in.Owner}
}

func (n *lockNode) locks(in *fuse.LkIn) *[]localFileLock {
	if in.LkFlags&fuse.FUSE_LK_FLOCK != 0 {
		return &n.flock
	}
	return &n.posix
}

func lockConflict(a, b fuse.FileLock) bool {
	return a.Start <= b.End && b.Start <= a.End &&
		(a.Typ == uint32(syscall.F_WRLCK) || b.Typ == uint32(syscall.F_WRLCK))
}

func findConflict(locks []localFileLock, holder lockHolder, requested fuse.FileLock) *localFileLock {
	for i := range locks {
		if locks[i].holder != holder && lockConflict(locks[i].lock, requested) {
			return &locks[i]
		}
	}
	return nil
}

func (t *LockTable) GetLk(in *fuse.LkIn) fuse.FileLock {
	t.mu.Lock()
	defer t.mu.Unlock()

	if node := t.nodes[in.NodeId]; node != nil {
		if conflict := findConflict(*node.locks(in), lockIdentity(in), in.Lk); conflict != nil {
			return conflict.lock
		}
	}
	return fuse.FileLock{Start: in.Lk.Start, End: in.Lk.End, Typ: uint32(syscall.F_UNLCK)}
}

func (t *LockTable) SetLk(in *fuse.LkIn) fuse.Status {
	return t.set(context.Background(), nil, in, false)
}

func (t *LockTable) SetLkw(ctx context.Context, cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return t.set(ctx, cancel, in, true)
}

func (n *lockNode) notify() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *lockNode) removeWaiter(waiter *lockWaiter) {
	if waiter == nil {
		return
	}
	for i, candidate := range n.waiters {
		if candidate == waiter {
			n.waiters = append(n.waiters[:i], n.waiters[i+1:]...)
			n.notify()
			return
		}
	}
}

func (n *lockNode) hasEarlierConflict(in *fuse.LkIn, current *lockWaiter) bool {
	for _, waiter := range n.waiters {
		if waiter == current {
			break
		}
		other := &waiter.request
		if (other.LkFlags&fuse.FUSE_LK_FLOCK) == (in.LkFlags&fuse.FUSE_LK_FLOCK) &&
			lockIdentity(other) != lockIdentity(in) && lockConflict(other.Lk, in.Lk) {
			return true
		}
	}
	return false
}

func coveredByOwner(locks []localFileLock, holder lockHolder, requested fuse.FileLock) bool {
	next := requested.Start
	for _, entry := range locks {
		if entry.holder != holder || entry.lock.End < next || entry.lock.Start > next ||
			(requested.Typ == uint32(syscall.F_WRLCK) && entry.lock.Typ != requested.Typ) {
			continue
		}
		if entry.lock.End >= requested.End {
			return true
		}
		next = entry.lock.End + 1
	}
	return false
}

func (n *lockNode) deadlocked(target *lockWaiter) bool {
	graph := make(map[lockHolder]map[lockHolder]struct{})
	for _, waiter := range n.waiters {
		in := &waiter.request
		if (in.LkFlags & fuse.FUSE_LK_FLOCK) != (target.request.LkFlags & fuse.FUSE_LK_FLOCK) {
			continue
		}
		from := lockIdentity(in)
		add := func(to lockHolder) {
			if from == to {
				return
			}
			if graph[from] == nil {
				graph[from] = make(map[lockHolder]struct{})
			}
			graph[from][to] = struct{}{}
		}
		locks := *n.locks(in)
		for _, held := range locks {
			if lockConflict(held.lock, in.Lk) {
				add(held.holder)
			}
		}
		if !coveredByOwner(locks, from, in.Lk) {
			for _, earlier := range n.waiters {
				if earlier == waiter {
					break
				}
				other := &earlier.request
				if (other.LkFlags&fuse.FUSE_LK_FLOCK) == (in.LkFlags&fuse.FUSE_LK_FLOCK) &&
					lockConflict(other.Lk, in.Lk) {
					add(lockIdentity(other))
				}
			}
		}
	}
	start := lockIdentity(&target.request)
	seen := make(map[lockHolder]bool)
	var reachesStart func(lockHolder) bool
	reachesStart = func(holder lockHolder) bool {
		if seen[holder] {
			return false
		}
		seen[holder] = true
		for next := range graph[holder] {
			if next == start || reachesStart(next) {
				return true
			}
		}
		return false
	}
	return reachesStart(start)
}

func lockInterrupted(ctx context.Context, cancel <-chan struct{}) bool {
	if ctx.Err() != nil {
		return true
	}
	select {
	case <-cancel:
		return true
	default:
		return false
	}
}

func (t *LockTable) set(ctx context.Context, cancel <-chan struct{}, in *fuse.LkIn, wait bool) fuse.Status {
	t.mu.Lock()
	if in.LkFlags&fuse.FUSE_LK_FLOCK == 0 {
		if flags, known := t.handles[in.Fh]; known {
			mode := flags & uint32(syscall.O_ACCMODE)
			if (in.Lk.Typ == uint32(syscall.F_RDLCK) && mode != uint32(syscall.O_RDONLY) && mode != uint32(syscall.O_RDWR)) ||
				(in.Lk.Typ == uint32(syscall.F_WRLCK) && mode != uint32(syscall.O_WRONLY) && mode != uint32(syscall.O_RDWR)) {
				t.mu.Unlock()
				return fuse.EBADF
			}
		}
	}
	node := t.nodes[in.NodeId]
	if node == nil {
		node = &lockNode{changed: make(chan struct{})}
		t.nodes[in.NodeId] = node
	}
	var waiter *lockWaiter
	var waitStart time.Time
	for {
		if t.closed || (waiter != nil && waiter.canceled) || lockInterrupted(ctx, cancel) {
			node.removeWaiter(waiter)
			t.prune(in.NodeId, node)
			t.mu.Unlock()
			if waiter != nil {
				t.metrics.Load().observeLockWait(time.Since(waitStart))
			}
			return fuse.EINTR
		}
		locks := node.locks(in)
		holder := lockIdentity(in)
		if in.Lk.Typ == uint32(syscall.F_UNLCK) ||
			(findConflict(*locks, holder, in.Lk) == nil &&
				(!node.hasEarlierConflict(in, waiter) || coveredByOwner(*locks, holder, in.Lk))) {
			*locks = updateLocks(*locks, holder, in.Lk)
			node.removeWaiter(waiter)
			node.notify()
			t.prune(in.NodeId, node)
			t.mu.Unlock()
			if waiter != nil {
				t.metrics.Load().observeLockWait(time.Since(waitStart))
			}
			return fuse.OK
		}
		if in.LkFlags&fuse.FUSE_LK_FLOCK != 0 {
			for _, held := range *locks {
				if held.holder == holder && held.lock.Typ != in.Lk.Typ {
					*locks = updateLocks(*locks, holder, fuse.FileLock{Start: 0, End: ^uint64(0), Typ: uint32(syscall.F_UNLCK)})
					node.notify()
					break
				}
			}
		}
		if !wait {
			t.prune(in.NodeId, node)
			t.mu.Unlock()
			t.metrics.Load().incLockConflict()
			return fuse.EAGAIN
		}
		if waiter == nil {
			waiter = &lockWaiter{request: *in}
			node.waiters = append(node.waiters, waiter)
			if node.deadlocked(waiter) {
				node.removeWaiter(waiter)
				t.prune(in.NodeId, node)
				t.mu.Unlock()
				t.metrics.Load().incLockConflict()
				return fuse.Status(syscall.EDEADLK)
			}
			waitStart = time.Now()
			t.metrics.Load().incLockConflict()
		}
		changed := node.changed
		t.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-cancel:
		case <-changed:
		}
		t.mu.Lock()
	}
}

func updateLocks(locks []localFileLock, holder lockHolder, requested fuse.FileLock) []localFileLock {
	updated := make([]localFileLock, 0, len(locks)+1)
	for _, entry := range locks {
		if entry.holder != holder || entry.lock.End < requested.Start || entry.lock.Start > requested.End {
			updated = append(updated, entry)
			continue
		}
		if entry.lock.Start < requested.Start {
			left := entry
			left.lock.End = requested.Start - 1
			updated = append(updated, left)
		}
		if entry.lock.End > requested.End {
			right := entry
			right.lock.Start = requested.End + 1
			updated = append(updated, right)
		}
	}
	if requested.Typ != uint32(syscall.F_UNLCK) {
		updated = append(updated, localFileLock{holder: holder, lock: requested})
	}
	sort.Slice(updated, func(i, j int) bool {
		if updated[i].holder != updated[j].holder {
			if updated[i].holder.fh != updated[j].holder.fh {
				return !updated[i].holder.fh
			}
			return updated[i].holder.id < updated[j].holder.id
		}
		return updated[i].lock.Start < updated[j].lock.Start
	})
	merged := updated[:0]
	for _, entry := range updated {
		if len(merged) > 0 {
			previous := &merged[len(merged)-1]
			if previous.holder == entry.holder && previous.lock.Typ == entry.lock.Typ &&
				(previous.lock.End == ^uint64(0) || previous.lock.End+1 >= entry.lock.Start) {
				if entry.lock.End > previous.lock.End {
					previous.lock.End = entry.lock.End
				}
				continue
			}
		}
		merged = append(merged, entry)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].lock.Start < merged[j].lock.Start })
	return merged
}
