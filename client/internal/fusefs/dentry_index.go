package fusefs

import "sync"

type dentryKey struct {
	parent uint64
	name   string
}

type dentryIndex struct {
	mu      sync.RWMutex
	byName  map[dentryKey]uint64
	byChild map[uint64]map[dentryKey]struct{}
}

func newDentryIndex() *dentryIndex {
	return &dentryIndex{
		byName:  make(map[dentryKey]uint64),
		byChild: make(map[uint64]map[dentryKey]struct{}),
	}
}

func (idx *dentryIndex) get(parent uint64, name string) (uint64, bool) {
	if idx == nil {
		return 0, false
	}
	idx.mu.RLock()
	child, ok := idx.byName[dentryKey{parent: parent, name: name}]
	idx.mu.RUnlock()
	return child, ok
}

func (idx *dentryIndex) put(parent uint64, name string, child uint64) {
	if idx == nil || parent == 0 || name == "" || child == 0 {
		return
	}
	key := dentryKey{parent: parent, name: name}
	idx.mu.Lock()
	if prev, ok := idx.byName[key]; ok {
		if prev == child {
			idx.mu.Unlock()
			return
		}
		idx.removeReverseLocked(prev, key)
	}
	idx.byName[key] = child
	set := idx.byChild[child]
	if set == nil {
		set = make(map[dentryKey]struct{})
		idx.byChild[child] = set
	}
	set[key] = struct{}{}
	idx.mu.Unlock()
}

func (idx *dentryIndex) deleteName(parent uint64, name string) {
	if idx == nil {
		return
	}
	key := dentryKey{parent: parent, name: name}
	idx.mu.Lock()
	if prev, ok := idx.byName[key]; ok {
		delete(idx.byName, key)
		idx.removeReverseLocked(prev, key)
	}
	idx.mu.Unlock()
}

func (idx *dentryIndex) forgetChild(child uint64) {
	if idx == nil || child == 0 {
		return
	}
	idx.mu.Lock()
	if keys, ok := idx.byChild[child]; ok {
		for key := range keys {
			delete(idx.byName, key)
		}
		delete(idx.byChild, child)
	}
	idx.mu.Unlock()
}

func (idx *dentryIndex) clear() {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	clear(idx.byName)
	clear(idx.byChild)
	idx.mu.Unlock()
}

func (idx *dentryIndex) removeReverseLocked(child uint64, key dentryKey) {
	if keys, ok := idx.byChild[child]; ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(idx.byChild, child)
		}
	}
}
