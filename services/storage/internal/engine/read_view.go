package engine

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
)

type retiredPack struct {
	packID      uint64
	retireEpoch uint64
}

type readEpoch struct {
	id   uint64
	refs atomic.Int64
	next *readEpoch
	ebr  *epochTracker
}

type epochTracker struct {
	cur atomic.Pointer[readEpoch]

	mu       sync.Mutex
	oldest   *readEpoch
	retired  []retiredPack
	unlinkFn func(packIDs []uint64) ([]uint64, error)
}

func (t *epochTracker) init(unlinkFn func(packIDs []uint64) ([]uint64, error)) {
	first := &readEpoch{id: 1, ebr: t}
	first.refs.Store(1)
	t.oldest = first
	t.cur.Store(first)
	t.unlinkFn = unlinkFn
}

func (t *epochTracker) pin() *readEpoch {
	for {
		e := t.cur.Load()
		for {
			cur := e.refs.Load()
			if cur <= 0 {
				break
			}
			if e.refs.CompareAndSwap(cur, cur+1) {
				return e
			}
		}
	}
}

func (e *readEpoch) unpin() {
	if e == nil {
		return
	}
	if e.refs.Add(-1) == 0 {
		_ = e.ebr.drainReady()
	}
}

func (t *epochTracker) retire(packID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	old := t.cur.Load()
	next := &readEpoch{
		id:  old.id + 1,
		ebr: t,
	}
	next.refs.Store(1)
	old.next = next
	t.cur.Store(next)
	t.retired = append(t.retired, retiredPack{
		packID:      packID,
		retireEpoch: old.id,
	})
	old.refs.Add(-1)
	return t.drainReadyLocked()
}

func (t *epochTracker) drainReady() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.drainReadyLocked()
}

func (t *epochTracker) drainReadyLocked() error {
	cur := t.cur.Load()
	for t.oldest != cur && t.oldest.refs.Load() == 0 {
		prev := t.oldest
		t.oldest = prev.next
		prev.next = nil
	}
	if len(t.retired) == 0 {
		return nil
	}
	oldestID := t.oldest.id
	var (
		ready     []uint64
		remaining []retiredPack
	)
	for _, item := range t.retired {
		if item.retireEpoch < oldestID {
			ready = append(ready, item.packID)
		} else {
			remaining = append(remaining, item)
		}
	}
	if len(ready) == 0 {
		return nil
	}
	t.retired = remaining
	if t.unlinkFn == nil {
		return nil
	}
	failed, err := t.unlinkFn(ready)
	for _, packID := range failed {
		t.retired = append(t.retired, retiredPack{
			packID:      packID,
			retireEpoch: 0,
		})
	}
	return err
}

func (t *epochTracker) drainAll() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.retired) == 0 {
		return nil
	}
	ready := make([]uint64, len(t.retired))
	for i, item := range t.retired {
		ready[i] = item.packID
	}
	t.retired = nil
	if t.unlinkFn == nil {
		return nil
	}
	failed, err := t.unlinkFn(ready)
	for _, packID := range failed {
		t.retired = append(t.retired, retiredPack{
			packID:      packID,
			retireEpoch: 0,
		})
	}
	return err
}

type readView struct {
	disk    *DiskStore
	snap    *pebble.Snapshot
	snapGen uint64
	pin     *readEpoch
	closed  bool
}

func (d *DiskStore) openReadView() *readView {
	pin := d.ebr.pin()
	snapGen := d.commitCacheGen.Load()
	snap := d.db.NewSnapshot()
	return &readView{
		disk:    d,
		snap:    snap,
		snapGen: snapGen,
		pin:     pin,
	}
}

func (v *readView) readBlock(ref BlobRef) ([]byte, error) {
	if v == nil || v.closed || v.pin == nil {
		return nil, ErrClosed
	}
	return v.disk.packs.readBlockPinned(v.pin, ref)
}

func (v *readView) Close() error {
	if v == nil || v.closed {
		return nil
	}
	v.closed = true
	var err error
	if v.snap != nil {
		err = v.snap.Close()
		v.snap = nil
	}
	pin := v.pin
	v.pin = nil
	pin.unpin()
	return err
}
