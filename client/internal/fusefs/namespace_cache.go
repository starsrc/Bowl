package fusefs

import (
	"container/list"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const DefaultNamespaceCacheCapacity = 65536

const MaxFilenameBytes = 255

type cacheKey struct {
	kind byte
	ino  uint64
	name string
}

type cachedInode struct {
	inode        *storagev1.Inode
	entryTimeout float64
	attrTimeout  float64
}

type cacheItem struct {
	key             cacheKey
	inode           cachedInode
	entry           uint64
	dir             []*storagev1.DirEntryPlus
	dirEntryExpires []int64
	dirAttrExpires  []int64
	linkTarget      []byte
	expiresAtNano   int64
	weight          int
}

func (item *cacheItem) dirEntryExpiryAt(idx int) int64 {
	if idx >= 0 && idx < len(item.dirEntryExpires) {
		return item.dirEntryExpires[idx]
	}
	return item.expiresAtNano
}

func (item *cacheItem) dirAttrExpiryAt(idx int) int64 {
	if idx >= 0 && idx < len(item.dirAttrExpires) {
		return item.dirAttrExpires[idx]
	}
	return item.expiresAtNano
}

type cachedDir struct {
	entries         []*storagev1.DirEntryPlus
	fromCache       bool
	nowNano         int64
	expiresAtNano   int64
	dirEntryExpires []int64
	dirAttrExpires  []int64
}

func (d *cachedDir) entryExpiryAt(idx int) int64 {
	if idx >= 0 && idx < len(d.dirEntryExpires) {
		return d.dirEntryExpires[idx]
	}
	return d.expiresAtNano
}

func (d *cachedDir) attrExpiryAt(idx int) int64 {
	if idx >= 0 && idx < len(d.dirAttrExpires) {
		return d.dirAttrExpires[idx]
	}
	return d.expiresAtNano
}

func itemOf(el *list.Element) *cacheItem {
	item, _ := el.Value.(*cacheItem)
	return item
}

type NamespaceCache struct {
	mu          sync.Mutex
	capacity    int
	used        int
	epoch       uint64
	mutations   uint64
	rootIno     uint64
	entryTTL    float64
	attrTTL     float64
	readOnly    bool
	nowFn       func() time.Time
	items       map[cacheKey]*list.Element
	lru         list.List
	byParent    map[uint64]map[string]struct{}
	dirsByChild map[uint64]map[uint64]struct{}
	inodeDirs   map[uint64]map[uint64]struct{}

	completeDirs map[uint64]int64

	parentByDir map[uint64]uint64
	metrics     atomic.Pointer[Metrics]
}

func NewNamespaceCache(capacity int) *NamespaceCache {
	if capacity <= 0 {
		capacity = DefaultNamespaceCacheCapacity
	}
	return &NamespaceCache{
		capacity:     capacity,
		rootIno:      1,
		items:        make(map[cacheKey]*list.Element),
		byParent:     make(map[uint64]map[string]struct{}),
		dirsByChild:  make(map[uint64]map[uint64]struct{}),
		inodeDirs:    make(map[uint64]map[uint64]struct{}),
		completeDirs: make(map[uint64]int64),
		parentByDir:  make(map[uint64]uint64),
	}
}

func (c *NamespaceCache) configure(rootIno uint64, entryTTL, attrTTL float64) {
	c.mu.Lock()
	if rootIno > 0 {
		c.rootIno = rootIno
	}
	c.entryTTL = entryTTL
	c.attrTTL = attrTTL
	c.mu.Unlock()
}

func (c *NamespaceCache) configureReadOnly(nowFn func() time.Time) {
	c.mu.Lock()
	c.readOnly = true
	c.nowFn = nowFn
	c.mu.Unlock()
}

func (c *NamespaceCache) currentTime() time.Time {
	if c.nowFn != nil {
		return c.nowFn()
	}
	return time.Now()
}

func ttlDuration(ttlSeconds float64) time.Duration {
	if ttlSeconds <= 0 || math.IsNaN(ttlSeconds) {
		return 0
	}
	if ttlSeconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ttlSeconds * float64(time.Second))
}

func remainingSeconds(expiresAtNano, nowNano int64) float64 {
	if expiresAtNano <= nowNano {
		return 0
	}
	return max(0, float64(expiresAtNano-nowNano)/1e9)
}

func (c *NamespaceCache) bindMetrics(m *Metrics) {
	c.metrics.Store(m)
}

func (c *NamespaceCache) stats() (entries, used, capacity int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items), c.used, c.capacity
}

func (c *NamespaceCache) remove(el *list.Element) {
	if el == nil {
		return
	}
	item := itemOf(el)
	delete(c.items, item.key)
	c.used -= item.weight
	c.lru.Remove(el)
	c.unindex(item)
}

func (c *NamespaceCache) removeKey(key cacheKey) {
	if el := c.items[key]; el != nil {
		c.remove(el)
	}
}

func (c *NamespaceCache) pruneOrphanDirMetaLocked(ino uint64) {
	if c.items[cacheKey{kind: 'i', ino: ino}] == nil && c.items[cacheKey{kind: 'd', ino: ino}] == nil {
		delete(c.completeDirs, ino)
		delete(c.parentByDir, ino)
	}
}

func (c *NamespaceCache) index(item *cacheItem) {
	switch item.key.kind {
	case 'e':
		names := c.byParent[item.key.ino]
		if names == nil {
			names = make(map[string]struct{})
			c.byParent[item.key.ino] = names
		}
		names[item.key.name] = struct{}{}
	case 'd':
		dir := item.key.ino
		children := make(map[uint64]struct{})
		for _, entry := range item.dir {
			child := entry.GetIno()
			if child == 0 {
				continue
			}
			children[child] = struct{}{}
			dirs := c.dirsByChild[child]
			if dirs == nil {
				dirs = make(map[uint64]struct{})
				c.dirsByChild[child] = dirs
			}
			dirs[dir] = struct{}{}
		}
		c.inodeDirs[dir] = children
	}
}

func (c *NamespaceCache) unindex(item *cacheItem) {
	switch item.key.kind {
	case 'e':
		names := c.byParent[item.key.ino]
		if names == nil {
			return
		}
		delete(names, item.key.name)
		if len(names) == 0 {
			delete(c.byParent, item.key.ino)
		}
	case 'd':
		dir := item.key.ino

		delete(c.completeDirs, dir)
		for child := range c.inodeDirs[dir] {
			dirs := c.dirsByChild[child]
			if dirs == nil {
				continue
			}
			delete(dirs, dir)
			if len(dirs) == 0 {
				delete(c.dirsByChild, child)
			}
		}
		delete(c.inodeDirs, dir)
	}
}

func (c *NamespaceCache) putItem(item *cacheItem) {
	key := item.key
	weight := item.weight
	if old := c.items[key]; old != nil {
		c.remove(old)
	}
	if weight > c.capacity {

		switch key.kind {
		case 'e':
			delete(c.completeDirs, key.ino)
		case 'i', 'd':
			c.pruneOrphanDirMetaLocked(key.ino)
		}
		return
	}
	for c.used+weight > c.capacity {
		back := c.lru.Back()
		if back == nil {
			break
		}
		evicted := itemOf(back)

		if evicted.key.kind == 'e' {
			delete(c.completeDirs, evicted.key.ino)
		}
		c.remove(back)
		if evicted.key.kind == 'i' || evicted.key.kind == 'd' {
			c.pruneOrphanDirMetaLocked(evicted.key.ino)
		}
		c.metrics.Load().incNSCacheEviction()
	}
	c.items[key] = c.lru.PushFront(item)
	c.used += weight
	c.index(item)
}

func (c *NamespaceCache) getValidLocked(key cacheKey, nowNano int64) (*cacheItem, bool) {
	el := c.items[key]
	if el == nil {
		return nil, false
	}
	item := itemOf(el)
	if c.readOnly && nowNano >= item.expiresAtNano {
		c.remove(el)
		if key.kind == 'i' || key.kind == 'd' {
			c.pruneOrphanDirMetaLocked(key.ino)
		}
		return nil, false
	}
	c.lru.MoveToFront(el)
	return item, true
}

func cloneTimestamp(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return &timestamppb.Timestamp{Seconds: ts.GetSeconds(), Nanos: ts.GetNanos()}
}

func cloneInode(inode *storagev1.Inode) *storagev1.Inode {
	if inode == nil {
		return nil
	}
	out := &storagev1.Inode{Ino: inode.GetIno(), Generation: inode.GetGeneration()}
	if attr := inode.GetAttr(); attr != nil {
		out.Attr = &storagev1.InodeAttribute{
			Mode:    attr.GetMode(),
			Size:    attr.GetSize(),
			Nlink:   attr.GetNlink(),
			Uid:     attr.GetUid(),
			Gid:     attr.GetGid(),
			Rdev:    attr.GetRdev(),
			Blksize: attr.GetBlksize(),
			Blocks:  attr.GetBlocks(),
			Atime:   cloneTimestamp(attr.GetAtime()),
			Mtime:   cloneTimestamp(attr.GetMtime()),
			Ctime:   cloneTimestamp(attr.GetCtime()),
			Flags:   attr.GetFlags(),
		}
	}
	return out
}

func cloneDirEntry(entry *storagev1.DirEntryPlus) *storagev1.DirEntryPlus {
	if entry == nil {
		return nil
	}
	out := &storagev1.DirEntryPlus{
		Ino:                 entry.GetIno(),
		NextOffset:          entry.GetNextOffset(),
		Inode:               cloneInode(entry.GetInode()),
		EntryTimeoutSeconds: entry.GetEntryTimeoutSeconds(),
		AttrTimeoutSeconds:  entry.GetAttrTimeoutSeconds(),
	}
	if name := entry.GetName(); len(name) > 0 {
		out.Name = append([]byte(nil), name...)
	}
	return out
}

func cloneDir(entries []*storagev1.DirEntryPlus) []*storagev1.DirEntryPlus {
	out := make([]*storagev1.DirEntryPlus, len(entries))
	for i, entry := range entries {
		out[i] = cloneDirEntry(entry)
	}
	return out
}

func (c *NamespaceCache) isCompleteDirValidLocked(dirIno uint64, nowNano int64) (int64, bool) {
	exp, ok := c.completeDirs[dirIno]
	if !ok {
		return 0, false
	}
	if c.readOnly && nowNano >= exp {
		delete(c.completeDirs, dirIno)
		return 0, false
	}
	return exp, true
}

func (c *NamespaceCache) readCachedInodeAtLocked(ino uint64, nowNano int64, touchLRU bool) (cachedInode, int64, bool) {
	el := c.items[cacheKey{kind: 'i', ino: ino}]
	if el != nil {
		item := itemOf(el)
		if c.readOnly && nowNano >= item.expiresAtNano {
			c.remove(el)
			c.pruneOrphanDirMetaLocked(ino)
		} else {
			if touchLRU {
				c.lru.MoveToFront(el)
			}
			v := item.inode
			if c.readOnly {
				v.attrTimeout = min(v.attrTimeout, remainingSeconds(item.expiresAtNano, nowNano))
			}
			return v, item.expiresAtNano, true
		}
	}

	for dir := range c.dirsByChild[ino] {
		dirEl := c.items[cacheKey{kind: 'd', ino: dir}]
		if dirEl == nil {
			continue
		}
		dItem := itemOf(dirEl)
		if c.readOnly && nowNano >= dItem.expiresAtNano {
			c.remove(dirEl)
			c.pruneOrphanDirMetaLocked(dir)
			continue
		}
		for i, entry := range dItem.dir {
			if entry.GetIno() != ino || entry.GetInode() == nil || entry.GetInode().GetAttr() == nil {
				continue
			}
			v := cachedInode{entry.GetInode(), entry.GetEntryTimeoutSeconds(), entry.GetAttrTimeoutSeconds()}
			var attrExp int64
			if c.readOnly {
				attrExp = dItem.dirAttrExpiryAt(i)
				if nowNano >= attrExp {
					continue
				}
				v.attrTimeout = min(v.attrTimeout, remainingSeconds(attrExp, nowNano))
				if entryExp := dItem.dirEntryExpiryAt(i); entryExp > nowNano {
					v.entryTimeout = min(v.entryTimeout, remainingSeconds(entryExp, nowNano))
				} else {
					v.entryTimeout = 0
				}
			}
			if touchLRU {
				c.lru.MoveToFront(dirEl)
			}
			return v, attrExp, true
		}
	}
	return cachedInode{}, 0, false
}

func (c *NamespaceCache) peekCachedInodeLocked(ino uint64) (cachedInode, bool) {
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}
	v, _, ok := c.readCachedInodeAtLocked(ino, nowNano, false)
	return v, ok
}

func (c *NamespaceCache) getCachedInodeLocked(ino uint64) (cachedInode, bool) {
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}
	v, _, ok := c.readCachedInodeAtLocked(ino, nowNano, true)
	return v, ok
}

func (c *NamespaceCache) getCachedInodeAtLocked(ino uint64, nowNano int64) (cachedInode, bool) {
	v, _, ok := c.readCachedInodeAtLocked(ino, nowNano, true)
	return v, ok
}

func (c *NamespaceCache) inode(ino uint64) (cachedInode, bool) {
	c.mu.Lock()
	v, ok := c.getCachedInodeLocked(ino)
	if ok {
		v.inode = cloneInode(v.inode)
		c.mu.Unlock()
		c.metrics.Load().incNSCacheHit(nsCacheAttr)
		return v, true
	}
	c.mu.Unlock()
	c.metrics.Load().incNSCacheMiss(nsCacheAttr)
	return cachedInode{}, false
}

func (c *NamespaceCache) inodeReadOnly(ino uint64) (cachedInode, bool) {
	c.mu.Lock()
	v, ok := c.getCachedInodeLocked(ino)
	c.mu.Unlock()
	if ok {
		c.metrics.Load().incNSCacheHit(nsCacheAttr)
		return v, true
	}
	c.metrics.Load().incNSCacheMiss(nsCacheAttr)
	return cachedInode{}, false
}

func (c *NamespaceCache) lookupReadOnly(parent uint64, name string) (cached cachedInode, childIno uint64, negative, ok bool) {
	if len(name) > MaxFilenameBytes {
		return cachedInode{}, 0, false, false
	}
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}
	c.mu.Lock()
	if item, hit := c.getValidLocked(cacheKey{kind: 'e', ino: parent, name: name}, nowNano); hit {
		ino := item.entry
		if ino == 0 {
			c.mu.Unlock()
			c.metrics.Load().incNSCacheHit(nsCacheNegativeEntry)
			return cachedInode{}, 0, true, true
		}
		v, attrHit := c.getCachedInodeAtLocked(ino, nowNano)
		if attrHit && c.readOnly {
			v.entryTimeout = min(v.entryTimeout, remainingSeconds(item.expiresAtNano, nowNano))
		}
		c.mu.Unlock()
		m := c.metrics.Load()
		m.incNSCacheHit(nsCacheEntry)
		if attrHit {
			m.incNSCacheHit(nsCacheAttr)
			return v, ino, false, true
		}
		m.incNSCacheMiss(nsCacheAttr)
		return cachedInode{}, ino, false, false
	}
	if item, hit := c.getValidLocked(cacheKey{kind: 'd', ino: parent}, nowNano); hit {
		for i, entry := range item.dir {
			if string(entry.GetName()) != name {
				continue
			}
			ino := entry.GetIno()
			if ino == 0 {
				c.mu.Unlock()
				c.metrics.Load().incNSCacheHit(nsCacheNegativeEntry)
				return cachedInode{}, 0, true, true
			}
			var entryExp int64
			if c.readOnly {
				entryExp = item.dirEntryExpiryAt(i)
				if nowNano >= entryExp {
					c.mu.Unlock()
					c.metrics.Load().incNSCacheMiss(nsCacheEntry)
					return cachedInode{}, 0, false, false
				}
			}
			if attrItem, attrHit := c.getValidLocked(cacheKey{kind: 'i', ino: ino}, nowNano); attrHit {
				v := attrItem.inode
				if c.readOnly {
					v.entryTimeout = min(entry.GetEntryTimeoutSeconds(), remainingSeconds(entryExp, nowNano))
					v.attrTimeout = min(v.attrTimeout, remainingSeconds(attrItem.expiresAtNano, nowNano))
				}
				c.mu.Unlock()
				m := c.metrics.Load()
				m.incNSCacheHit(nsCacheEntry)
				m.incNSCacheHit(nsCacheAttr)
				return v, ino, false, true
			}
			if entry.GetInode() != nil && entry.GetInode().GetAttr() != nil {
				v := cachedInode{entry.GetInode(), entry.GetEntryTimeoutSeconds(), entry.GetAttrTimeoutSeconds()}
				if c.readOnly {
					attrExp := item.dirAttrExpiryAt(i)
					if nowNano >= attrExp {
						c.mu.Unlock()
						m := c.metrics.Load()
						m.incNSCacheHit(nsCacheEntry)
						m.incNSCacheMiss(nsCacheAttr)
						return cachedInode{}, ino, false, false
					}
					v.entryTimeout = min(v.entryTimeout, remainingSeconds(entryExp, nowNano))
					v.attrTimeout = min(v.attrTimeout, remainingSeconds(attrExp, nowNano))
				}
				c.mu.Unlock()
				m := c.metrics.Load()
				m.incNSCacheHit(nsCacheEntry)
				m.incNSCacheHit(nsCacheAttr)
				return v, ino, false, true
			}
			c.mu.Unlock()
			m := c.metrics.Load()
			m.incNSCacheHit(nsCacheEntry)
			m.incNSCacheMiss(nsCacheAttr)
			return cachedInode{}, ino, false, false
		}
		c.mu.Unlock()
		c.metrics.Load().incNSCacheHit(nsCacheNegativeEntry)
		return cachedInode{}, 0, true, true
	}
	if _, complete := c.isCompleteDirValidLocked(parent, nowNano); complete {
		c.mu.Unlock()
		c.metrics.Load().incNSCacheHit(nsCacheNegativeEntry)
		return cachedInode{}, 0, true, true
	}
	c.mu.Unlock()
	c.metrics.Load().incNSCacheMiss(nsCacheEntry)
	return cachedInode{}, 0, false, false
}

func (c *NamespaceCache) putInode(ino uint64, inode *storagev1.Inode, entryTimeout, attrTimeout float64) {
	c.putInodeAt(^uint64(0), ino, inode, entryTimeout, attrTimeout)
}

func (c *NamespaceCache) putInodeAt(epoch, ino uint64, inode *storagev1.Inode, entryTimeout, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.mutations++
	c.putInodeLocked(ino, inode, entryTimeout, attrTimeout)
}

func (c *NamespaceCache) putInodeAtIf(generation, epoch, ino uint64, inode *storagev1.Inode, entryTimeout, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.putInodeLocked(ino, inode, entryTimeout, attrTimeout)
}

func (c *NamespaceCache) putInodeLocked(ino uint64, inode *storagev1.Inode, entryTimeout, attrTimeout float64) {
	var expiresAtNano int64
	var nowNano int64
	if c.readOnly {
		ttlDur := ttlDuration(attrTimeout)
		if ttlDur <= 0 {
			c.removeKey(cacheKey{kind: 'i', ino: ino})
			c.pruneOrphanDirMetaLocked(ino)
			if len(c.dirsByChild[ino]) > 0 {
				c.updateDirChildInodesLocked(ino, inode, attrTimeout, c.currentTime().UnixNano(), 0)
			}
			return
		}
		now := c.currentTime()
		nowNano = now.UnixNano()
		expiresAtNano = now.Add(ttlDur).UnixNano()
	}
	c.updateDirChildInodesLocked(ino, inode, attrTimeout, nowNano, expiresAtNano)
	c.putItem(&cacheItem{
		key:           cacheKey{kind: 'i', ino: ino},
		inode:         cachedInode{cloneInode(inode), entryTimeout, attrTimeout},
		expiresAtNano: expiresAtNano,
		weight:        1,
	})
}

func (c *NamespaceCache) updateDirChildInodesLocked(ino uint64, inode *storagev1.Inode, attrTimeout float64, nowNano, expiresAtNano int64) {
	for dir := range c.dirsByChild[ino] {
		el := c.items[cacheKey{kind: 'd', ino: dir}]
		if el == nil {
			continue
		}
		dItem := itemOf(el)
		if c.readOnly && nowNano >= dItem.expiresAtNano {
			c.remove(el)
			c.pruneOrphanDirMetaLocked(dir)
			continue
		}
		entries := dItem.dir
		for i, entry := range entries {
			if entry.GetIno() != ino {
				continue
			}

			updated := cloneDirEntry(entry)
			updated.Inode = cloneInode(inode)
			updated.AttrTimeoutSeconds = attrTimeout
			entries[i] = updated
			if c.readOnly && i < len(dItem.dirAttrExpires) {
				nextAttrExp := slices.Clone(dItem.dirAttrExpires)
				if expiresAtNano <= 0 {
					nextAttrExp[i] = 0
				} else {
					nextAttrExp[i] = min(dItem.expiresAtNano, expiresAtNano)
				}
				dItem.dirAttrExpires = nextAttrExp
			}
		}
	}
}

func (c *NamespaceCache) refreshExistingInodesFromReadDirAt(epoch uint64, entries []*storagev1.DirEntryPlus) {
	if !c.readOnly || len(entries) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	now := c.currentTime()
	nowNano := now.UnixNano()
	for _, entry := range entries {
		inode := entry.GetInode()
		if inode == nil || inode.GetAttr() == nil {
			continue
		}
		kernelIno := entry.GetIno()
		if kernelIno == 0 {
			continue
		}
		attrTimeout := entry.GetAttrTimeoutSeconds()
		el := c.items[cacheKey{kind: 'i', ino: kernelIno}]
		if el != nil && nowNano >= itemOf(el).expiresAtNano {
			c.remove(el)
			c.pruneOrphanDirMetaLocked(kernelIno)
			el = nil
		}
		ttlDur := ttlDuration(attrTimeout)
		if ttlDur <= 0 {
			if el != nil {
				c.remove(el)
				c.pruneOrphanDirMetaLocked(kernelIno)
			}
			if len(c.dirsByChild[kernelIno]) > 0 {
				c.updateDirChildInodesLocked(kernelIno, inode, attrTimeout, nowNano, 0)
			}
			continue
		}
		expiresAtNano := now.Add(ttlDur).UnixNano()
		if el != nil {
			entryTimeout := entry.GetEntryTimeoutSeconds()
			if entryTimeout <= 0 {
				entryTimeout = itemOf(el).inode.entryTimeout
			}
			c.putItem(&cacheItem{
				key:           cacheKey{kind: 'i', ino: kernelIno},
				inode:         cachedInode{cloneInode(inode), entryTimeout, attrTimeout},
				expiresAtNano: expiresAtNano,
				weight:        1,
			})
			if len(c.dirsByChild[kernelIno]) > 0 {
				c.updateDirChildInodesLocked(kernelIno, inode, attrTimeout, nowNano, expiresAtNano)
			}
			continue
		}
		if len(c.dirsByChild[kernelIno]) > 0 {
			c.updateDirChildInodesLocked(kernelIno, inode, attrTimeout, nowNano, expiresAtNano)
		}
	}
}

func (c *NamespaceCache) refreshInodeAt(epoch, ino uint64, inode *storagev1.Inode, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.refreshInodeLocked(ino, inode, attrTimeout)
}

func (c *NamespaceCache) refreshInodeAtIf(generation, epoch, ino uint64, inode *storagev1.Inode, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.refreshInodeLocked(ino, inode, attrTimeout)
}

func (c *NamespaceCache) refreshInodeLocked(ino uint64, inode *storagev1.Inode, attrTimeout float64) {
	entryTimeout := c.entryTimeoutLocked(ino, attrTimeout)
	c.removeKey(cacheKey{kind: 'i', ino: ino})
	if !c.readOnly {
		c.mutations++
	}
	c.putInodeLocked(ino, inode, entryTimeout, attrTimeout)
}

func (c *NamespaceCache) stageWriteInodeAt(epoch, ino, endOffset uint64, ts *timestamppb.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.attrTTL <= 0 || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	cur, ok := c.peekCachedInodeLocked(ino)
	if !ok || cur.inode == nil || cur.inode.GetAttr() == nil {
		return
	}
	updated := cloneInode(cur.inode)
	if endOffset > updated.Attr.Size {
		updated.Attr.Size = endOffset
		minBlocks := (endOffset + 511) / 512
		if minBlocks > updated.Attr.Blocks {
			updated.Attr.Blocks = minBlocks
		}
	}
	if ts != nil {
		updated.Attr.Mtime = cloneTimestamp(ts)
		updated.Attr.Ctime = cloneTimestamp(ts)
	}
	at := cur.attrTimeout
	if at <= 0 {
		at = DefaultWriteAttrTimeoutSeconds
	}
	c.removeKey(cacheKey{kind: 'i', ino: ino})
	c.mutations++
	c.putInodeLocked(ino, updated, cur.entryTimeout, at)
}

func (c *NamespaceCache) refreshInodeFromWriteAt(epoch, ino uint64, inode *storagev1.Inode, minSize uint64, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	target := inode
	if minSize > inode.GetAttr().GetSize() {
		cloned := cloneInode(inode)
		cloned.Attr.Size = minSize
		minBlocks := (minSize + 511) / 512
		if minBlocks > cloned.Attr.Blocks {
			cloned.Attr.Blocks = minBlocks
		}
		target = cloned
	}
	c.refreshInodeLocked(ino, target, attrTimeout)
}

func (c *NamespaceCache) entryTimeoutLocked(ino uint64, fallback float64) float64 {
	if c.readOnly {
		if v, ok := c.peekCachedInodeLocked(ino); ok {
			return v.entryTimeout
		}
		return fallback
	}
	if el := c.items[cacheKey{kind: 'i', ino: ino}]; el != nil {
		return itemOf(el).inode.entryTimeout
	}
	for dir := range c.dirsByChild[ino] {
		el := c.items[cacheKey{kind: 'd', ino: dir}]
		if el == nil {
			continue
		}
		for _, entry := range itemOf(el).dir {
			if entry.GetIno() == ino {
				return entry.GetEntryTimeoutSeconds()
			}
		}
	}
	return fallback
}

func (c *NamespaceCache) peekInode(ino uint64) (cachedInode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.peekCachedInodeLocked(ino)
	if !ok {
		return cachedInode{}, false
	}
	v.inode = cloneInode(v.inode)
	return v, true
}

func (c *NamespaceCache) inodeAttrTimeout(ino uint64, fallback float64) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readOnly {
		if v, ok := c.peekCachedInodeLocked(ino); ok {
			return v.attrTimeout
		}
		return fallback
	}
	if el := c.items[cacheKey{kind: 'i', ino: ino}]; el != nil {
		return itemOf(el).inode.attrTimeout
	}
	for dir := range c.dirsByChild[ino] {
		el := c.items[cacheKey{kind: 'd', ino: dir}]
		if el == nil {
			continue
		}
		for _, entry := range itemOf(el).dir {
			if entry.GetIno() == ino {
				return entry.GetAttrTimeoutSeconds()
			}
		}
	}
	return fallback
}

func (c *NamespaceCache) readlink(ino uint64) ([]byte, bool) {
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}
	c.mu.Lock()
	el := c.items[cacheKey{kind: 'l', ino: ino}]
	if el == nil {
		c.mu.Unlock()
		c.metrics.Load().incNSCacheMiss(nsCacheReadlink)
		return nil, false
	}
	item := itemOf(el)
	if c.readOnly && nowNano >= item.expiresAtNano {
		c.remove(el)
		c.mu.Unlock()
		c.metrics.Load().incNSCacheMiss(nsCacheReadlink)
		return nil, false
	}
	c.lru.MoveToFront(el)
	out := append([]byte(nil), item.linkTarget...)
	c.mu.Unlock()
	c.metrics.Load().incNSCacheHit(nsCacheReadlink)
	return out, true
}

func (c *NamespaceCache) putReadlinkAt(epoch, ino uint64, target []byte, ttlSeconds float64) {
	if ttlDuration(ttlSeconds) <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.putReadlinkLocked(ino, target, ttlSeconds)
}

func (c *NamespaceCache) putReadlinkAtIf(generation, epoch, ino uint64, target []byte, ttlSeconds float64) {
	if ttlDuration(ttlSeconds) <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.putReadlinkLocked(ino, target, ttlSeconds)
}

func (c *NamespaceCache) putReadlinkLocked(ino uint64, target []byte, ttlSeconds float64) {
	ttlDur := ttlDuration(ttlSeconds)
	if ttlDur <= 0 {
		return
	}
	var expiresAtNano int64
	if c.readOnly {
		expiresAtNano = c.currentTime().Add(ttlDur).UnixNano()
	}
	c.putItem(&cacheItem{
		key:           cacheKey{kind: 'l', ino: ino},
		linkTarget:    append([]byte(nil), target...),
		expiresAtNano: expiresAtNano,
		weight:        1,
	})
}

func (c *NamespaceCache) invalidateInode(ino uint64) {
	c.mu.Lock()
	delete(c.parentByDir, ino)
	c.removeKey(cacheKey{kind: 'i', ino: ino})
	c.removeKey(cacheKey{kind: 'l', ino: ino})
	for dir := range copyInoSet(c.dirsByChild[ino]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
		c.pruneOrphanDirMetaLocked(dir)
	}
	c.pruneOrphanDirMetaLocked(ino)
	c.mutations++
	c.mu.Unlock()
	c.metrics.Load().incNSCacheInvalidation()
}

func (c *NamespaceCache) entry(parent uint64, name string) (uint64, bool) {
	if len(name) > MaxFilenameBytes {
		return 0, false
	}
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}
	c.mu.Lock()
	if item, ok := c.getValidLocked(cacheKey{kind: 'e', ino: parent, name: name}, nowNano); ok {
		ino := item.entry
		c.mu.Unlock()
		if ino == 0 {
			c.metrics.Load().incNSCacheHit(nsCacheNegativeEntry)
		} else {
			c.metrics.Load().incNSCacheHit(nsCacheEntry)
		}
		return ino, true
	}
	if item, ok := c.getValidLocked(cacheKey{kind: 'd', ino: parent}, nowNano); ok {
		for i, entry := range item.dir {
			if string(entry.GetName()) != name {
				continue
			}
			if c.readOnly && nowNano >= item.dirEntryExpiryAt(i) {
				c.mu.Unlock()
				c.metrics.Load().incNSCacheMiss(nsCacheEntry)
				return 0, false
			}
			ino := entry.GetIno()
			c.mu.Unlock()
			c.metrics.Load().incNSCacheHit(nsCacheEntry)
			return ino, true
		}
		c.mu.Unlock()
		c.metrics.Load().incNSCacheHit(nsCacheNegativeEntry)
		return 0, true
	}

	if _, ok := c.isCompleteDirValidLocked(parent, nowNano); ok {
		c.mu.Unlock()
		c.metrics.Load().incNSCacheHit(nsCacheNegativeEntry)
		return 0, true
	}
	c.mu.Unlock()
	c.metrics.Load().incNSCacheMiss(nsCacheEntry)
	return 0, false
}

func (c *NamespaceCache) putEntryAt(epoch, parent uint64, name string, ino uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.putEntryLocked(parent, name, ino, c.entryTTL)
	c.mutations++
}

func (c *NamespaceCache) putEntryAtIf(generation, epoch, parent uint64, name string, ino uint64, entryTimeout float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.putEntryLocked(parent, name, ino, entryTimeout)
}

func (c *NamespaceCache) putEntryLocked(parent uint64, name string, ino uint64, entryTimeout float64) {
	if len(name) > MaxFilenameBytes {
		return
	}
	var expiresAtNano int64
	if c.readOnly {
		ttl := entryTimeout
		if ino == 0 {
			ttl = c.entryTTL
		}
		ttlDur := ttlDuration(ttl)
		if ttlDur <= 0 {
			c.removeKey(cacheKey{kind: 'e', ino: parent, name: name})
			return
		}
		expiresAtNano = c.currentTime().Add(ttlDur).UnixNano()
	}

	c.demoteDirStreamToEntriesLocked(parent)
	c.putItem(&cacheItem{
		key:           cacheKey{kind: 'e', ino: parent, name: name},
		entry:         ino,
		expiresAtNano: expiresAtNano,
		weight:        1,
	})
}

func (c *NamespaceCache) invalidateDir(parent uint64) {
	c.mu.Lock()
	delete(c.completeDirs, parent)
	delete(c.parentByDir, parent)
	c.removeKey(cacheKey{kind: 'd', ino: parent})
	c.removeKey(cacheKey{kind: 'i', ino: parent})
	for _, name := range copyNames(c.byParent[parent]) {
		c.removeKey(cacheKey{kind: 'e', ino: parent, name: name})
	}

	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
		c.pruneOrphanDirMetaLocked(dir)
	}
	c.mutations++
	c.mu.Unlock()
	c.metrics.Load().incNSCacheInvalidation()
}

func (c *NamespaceCache) demoteDirStreamToEntriesLocked(parent uint64) {
	el := c.items[cacheKey{kind: 'd', ino: parent}]
	if el == nil {
		return
	}
	dItem := itemOf(el)
	entries := dItem.dir
	dExpires := dItem.expiresAtNano
	dEntryExpires := dItem.dirEntryExpires
	dAttrExpires := dItem.dirAttrExpires
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}
	c.remove(el)
	if c.readOnly && nowNano >= dExpires {
		c.pruneOrphanDirMetaLocked(parent)
		return
	}
	if len(entries) > c.capacity/4 {
		c.pruneOrphanDirMetaLocked(parent)
		return
	}
	var parentIno uint64
	evictedSameParent := false
	for idx, entry := range entries {
		name := string(entry.GetName())
		if name == ".." && entry.GetIno() != 0 {
			parentIno = entry.GetIno()
		}
		if name == "" || name == "." || name == ".." || entry.GetIno() == 0 {
			continue
		}
		var entryExp int64
		if c.readOnly {
			if idx < len(dEntryExpires) {
				entryExp = dEntryExpires[idx]
			} else {
				entryExp = dExpires
			}
			if nowNano >= entryExp {
				evictedSameParent = true
				continue
			}
		}
		key := cacheKey{kind: 'e', ino: parent, name: name}

		if c.items[key] == nil && c.used+1 > c.capacity {
			if back := c.lru.Back(); back != nil {
				if item := itemOf(back); item.key.kind == 'e' && item.key.ino == parent {
					evictedSameParent = true
				}
			}
		}
		c.putItem(&cacheItem{key: key, entry: entry.GetIno(), expiresAtNano: entryExp, weight: 1})
	}
	if c.items[cacheKey{kind: 'i', ino: parent}] != nil {
		if parentIno != 0 {
			c.parentByDir[parent] = parentIno
		}
		if !evictedSameParent {
			c.completeDirs[parent] = dExpires
		}
	} else {
		c.pruneOrphanDirMetaLocked(parent)
	}

	for idx, entry := range entries {
		childIno := entry.GetIno()
		if childIno == 0 || childIno == parent {
			continue
		}
		inode := entry.GetInode()
		at := entry.GetAttrTimeoutSeconds()
		if inode == nil || inode.GetAttr() == nil || at <= 0 {
			continue
		}
		var attrExp int64
		if c.readOnly {
			if idx < len(dAttrExpires) {
				attrExp = dAttrExpires[idx]
			} else {
				attrExp = dExpires
			}
			if nowNano >= attrExp {
				continue
			}
		}
		key := cacheKey{kind: 'i', ino: childIno}
		if c.items[key] != nil {
			continue
		}
		et := entry.GetEntryTimeoutSeconds()
		if string(entry.GetName()) == ".." && et <= 0 {
			et = c.entryTimeoutLocked(childIno, c.entryTTL)
		}
		c.putItem(&cacheItem{
			key:           key,
			inode:         cachedInode{cloneInode(inode), et, at},
			expiresAtNano: attrExp,
			weight:        1,
		})
	}
}

func (c *NamespaceCache) markNewDirWithParentAt(epoch, dirIno, parentIno uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	if c.items[cacheKey{kind: 'i', ino: dirIno}] == nil && c.items[cacheKey{kind: 'd', ino: dirIno}] == nil {
		return
	}
	c.completeDirs[dirIno] = 0
	if parentIno != 0 {
		c.parentByDir[dirIno] = parentIno
	}
}

func (c *NamespaceCache) invalidateDirListingTouchParentAt(epoch, parent uint64, child *storagev1.Inode, nlinkDelta int32, attrTimeout float64) {
	c.metrics.Load().incNSCacheInvalidation()
	c.mu.Lock()
	defer c.mu.Unlock()
	var parentEntry cachedInode
	var hadParent bool
	if el := c.items[cacheKey{kind: 'i', ino: parent}]; el != nil {
		parentEntry = itemOf(el).inode
		hadParent = true
	}
	c.demoteDirStreamToEntriesLocked(parent)
	c.removeKey(cacheKey{kind: 'i', ino: parent})
	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
		c.pruneOrphanDirMetaLocked(dir)
	}
	c.mutations++
	if !hadParent || (epoch != ^uint64(0) && epoch != c.epoch) ||
		parentEntry.inode == nil || parentEntry.inode.GetAttr() == nil ||
		child == nil || child.GetAttr() == nil {
		c.pruneOrphanDirMetaLocked(parent)
		return
	}
	ts := child.GetAttr().GetCtime()
	if ts == nil {
		ts = child.GetAttr().GetMtime()
	}
	if ts == nil {
		c.pruneOrphanDirMetaLocked(parent)
		return
	}
	updatedParent := cloneInode(parentEntry.inode)
	updatedParent.Attr.Mtime = cloneTimestamp(ts)
	updatedParent.Attr.Ctime = cloneTimestamp(ts)
	if nlinkDelta > 0 {
		updatedParent.Attr.Nlink += uint32(nlinkDelta)
	} else if nlinkDelta < 0 && updatedParent.Attr.Nlink > 0 {
		updatedParent.Attr.Nlink--
	}
	at := attrTimeout
	if at <= 0 {
		at = parentEntry.attrTimeout
	}
	c.putInodeLocked(parent, updatedParent, parentEntry.entryTimeout, at)
}

func (c *NamespaceCache) invalidateDirListingTouchParentNowAt(epoch, parent uint64, nlinkDelta int32) {
	c.metrics.Load().incNSCacheInvalidation()
	c.mu.Lock()
	defer c.mu.Unlock()
	var parentEntry cachedInode
	var hadParent bool
	if el := c.items[cacheKey{kind: 'i', ino: parent}]; el != nil {
		parentEntry = itemOf(el).inode
		hadParent = true
	}
	c.demoteDirStreamToEntriesLocked(parent)
	c.removeKey(cacheKey{kind: 'i', ino: parent})
	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
		c.pruneOrphanDirMetaLocked(dir)
	}
	c.mutations++
	if !hadParent || (epoch != ^uint64(0) && epoch != c.epoch) ||
		parentEntry.inode == nil || parentEntry.inode.GetAttr() == nil {
		c.pruneOrphanDirMetaLocked(parent)
		return
	}
	ts := timestamppb.Now()
	updated := cloneInode(parentEntry.inode)
	updated.Attr.Mtime = cloneTimestamp(ts)
	updated.Attr.Ctime = cloneTimestamp(ts)
	if nlinkDelta > 0 {
		updated.Attr.Nlink += uint32(nlinkDelta)
	} else if nlinkDelta < 0 && updated.Attr.Nlink > 0 {
		updated.Attr.Nlink--
	}
	c.putInodeLocked(parent, updated, parentEntry.entryTimeout, parentEntry.attrTimeout)
}

func (c *NamespaceCache) invalidateDirListingDropParent(parent uint64) {
	c.metrics.Load().incNSCacheInvalidation()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.demoteDirStreamToEntriesLocked(parent)
	c.removeKey(cacheKey{kind: 'i', ino: parent})
	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
		c.pruneOrphanDirMetaLocked(dir)
	}
	c.pruneOrphanDirMetaLocked(parent)
	c.mutations++
}

func (c *NamespaceCache) invalidateChildParents(parent uint64) {
	c.mu.Lock()
	for dir, p := range c.parentByDir {
		if p == parent && dir != parent {
			delete(c.parentByDir, dir)
			c.removeKey(cacheKey{kind: 'd', ino: dir})
		}
	}
	c.mutations++
	c.mu.Unlock()
}

func (c *NamespaceCache) removeEntry(parent uint64, name string) {
	c.mu.Lock()

	delete(c.completeDirs, parent)
	c.removeKey(cacheKey{kind: 'e', ino: parent, name: name})
	c.mutations++
	c.mu.Unlock()
	c.metrics.Load().incNSCacheInvalidation()
}

func (c *NamespaceCache) dir(ino uint64) (cachedDir, bool) {
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}
	c.mu.Lock()
	if item, ok := c.getValidLocked(cacheKey{kind: 'd', ino: ino}, nowNano); ok {

		res := cachedDir{
			entries:         append([]*storagev1.DirEntryPlus(nil), item.dir...),
			fromCache:       true,
			nowNano:         nowNano,
			expiresAtNano:   item.expiresAtNano,
			dirEntryExpires: item.dirEntryExpires,
			dirAttrExpires:  item.dirAttrExpires,
		}
		c.mu.Unlock()
		c.metrics.Load().incNSCacheHit(nsCacheReadDir)
		return res, true
	}
	if synth, ok := c.synthesizeCompleteDirAtLocked(ino, nowNano); ok {
		res := cachedDir{
			entries:   append([]*storagev1.DirEntryPlus(nil), synth...),
			fromCache: true,
			nowNano:   nowNano,
		}
		if el := c.items[cacheKey{kind: 'd', ino: ino}]; el != nil {
			item := itemOf(el)
			res.expiresAtNano = item.expiresAtNano
			res.dirEntryExpires = item.dirEntryExpires
			res.dirAttrExpires = item.dirAttrExpires
		}
		c.mu.Unlock()
		c.metrics.Load().incNSCacheHit(nsCacheReadDir)
		return res, true
	}
	c.mu.Unlock()
	c.metrics.Load().incNSCacheMiss(nsCacheReadDir)
	return cachedDir{}, false
}

type synthChildEntry struct {
	name   string
	ino    uint64
	cached cachedInode
}

func (c *NamespaceCache) synthesizeCompleteDirLocked(dirIno uint64) ([]*storagev1.DirEntryPlus, bool) {
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}
	return c.synthesizeCompleteDirAtLocked(dirIno, nowNano)
}

func (c *NamespaceCache) synthesizeCompleteDirAtLocked(dirIno uint64, nowNano int64) ([]*storagev1.DirEntryPlus, bool) {
	if c.entryTTL <= 0 || c.attrTTL <= 0 {
		return nil, false
	}
	completeExp, complete := c.isCompleteDirValidLocked(dirIno, nowNano)
	if !complete {
		return nil, false
	}
	minExp := completeExp
	var parentIno uint64
	if c.rootIno != 0 && dirIno == c.rootIno {
		parentIno = dirIno
	} else {
		parentIno = c.parentByDir[dirIno]
		if parentIno == 0 {
			return nil, false
		}
	}
	dirCached, dirExp, ok := c.readCachedInodeAtLocked(dirIno, nowNano, false)
	if !ok || dirCached.inode == nil || dirCached.inode.GetAttr() == nil || dirCached.attrTimeout <= 0 {
		return nil, false
	}
	if c.readOnly && dirExp < minExp {
		minExp = dirExp
	}
	parentCached := dirCached
	if parentIno != dirIno {
		var parentExp int64
		parentCached, parentExp, ok = c.readCachedInodeAtLocked(parentIno, nowNano, false)
		if !ok || parentCached.inode == nil || parentCached.inode.GetAttr() == nil || parentCached.attrTimeout <= 0 {
			return nil, false
		}
		if c.readOnly && parentExp < minExp {
			minExp = parentExp
		}
	}

	namesMap := c.byParent[dirIno]
	children := make([]synthChildEntry, 0, len(namesMap))
	for name := range namesMap {
		el := c.items[cacheKey{kind: 'e', ino: dirIno, name: name}]
		if el == nil {
			return nil, false
		}
		eItem := itemOf(el)
		if c.readOnly && nowNano >= eItem.expiresAtNano {
			c.remove(el)
			delete(c.completeDirs, dirIno)
			return nil, false
		}
		if c.readOnly && eItem.expiresAtNano < minExp {
			minExp = eItem.expiresAtNano
		}
		childIno := eItem.entry
		if childIno == 0 {
			continue
		}
		childCached, childExp, hit := c.readCachedInodeAtLocked(childIno, nowNano, false)
		if !hit || childCached.inode == nil || childCached.inode.GetAttr() == nil ||
			childCached.attrTimeout <= 0 || childCached.entryTimeout <= 0 {
			return nil, false
		}
		if c.readOnly && childExp < minExp {
			minExp = childExp
		}
		children = append(children, synthChildEntry{
			name:   name,
			ino:    childIno,
			cached: childCached,
		})
	}
	if c.readOnly && nowNano >= minExp {
		delete(c.completeDirs, dirIno)
		return nil, false
	}

	total := 2 + len(children)
	if total+1 > c.capacity {
		return nil, false
	}

	slices.SortFunc(children, func(a, b synthChildEntry) int {
		if a.name < b.name {
			return -1
		}
		if a.name > b.name {
			return 1
		}
		return 0
	})

	entries := make([]*storagev1.DirEntryPlus, 0, total)
	entries = append(entries,
		&storagev1.DirEntryPlus{
			Ino:                 dirIno,
			Name:                []byte("."),
			NextOffset:          1,
			Inode:               dirCached.inode,
			EntryTimeoutSeconds: 0,
			AttrTimeoutSeconds:  dirCached.attrTimeout,
		},
		&storagev1.DirEntryPlus{
			Ino:                 parentIno,
			Name:                []byte(".."),
			NextOffset:          2,
			Inode:               parentCached.inode,
			EntryTimeoutSeconds: 0,
			AttrTimeoutSeconds:  parentCached.attrTimeout,
		},
	)
	for i, ch := range children {
		entries = append(entries, &storagev1.DirEntryPlus{
			Ino:                 ch.ino,
			Name:                []byte(ch.name),
			NextOffset:          uint64(3 + i),
			Inode:               ch.cached.inode,
			EntryTimeoutSeconds: ch.cached.entryTimeout,
			AttrTimeoutSeconds:  ch.cached.attrTimeout,
		})
	}

	c.putDirWithMaxExpiryLocked(dirIno, entries, minExp)
	if el := c.items[cacheKey{kind: 'd', ino: dirIno}]; el != nil {
		return itemOf(el).dir, true
	}
	return cloneDir(entries), true
}

func (c *NamespaceCache) putDirAtIf(generation, epoch, ino uint64, entries []*storagev1.DirEntryPlus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.putDirLocked(ino, entries)
}

func (c *NamespaceCache) putDirLocked(ino uint64, entries []*storagev1.DirEntryPlus) {
	c.putDirWithMaxExpiryLocked(ino, entries, 0)
}

func (c *NamespaceCache) putDirWithMaxExpiryLocked(ino uint64, entries []*storagev1.DirEntryPlus, maxExpNano int64) {
	var expiresAtNano int64
	var dirEntryExpires []int64
	var dirAttrExpires []int64
	if c.readOnly {
		if c.entryTTL <= 0 || c.attrTTL <= 0 {
			c.removeKey(cacheKey{kind: 'd', ino: ino})
			delete(c.completeDirs, ino)
			c.pruneOrphanDirMetaLocked(ino)
			return
		}
		dirDur := ttlDuration(min(c.entryTTL, c.attrTTL))
		if dirDur <= 0 {
			c.removeKey(cacheKey{kind: 'd', ino: ino})
			delete(c.completeDirs, ino)
			c.pruneOrphanDirMetaLocked(ino)
			return
		}
		now := c.currentTime()
		expiresAtNano = now.Add(dirDur).UnixNano()
		if maxExpNano > 0 && maxExpNano < expiresAtNano {
			expiresAtNano = maxExpNano
		}
		if now.UnixNano() >= expiresAtNano {
			c.removeKey(cacheKey{kind: 'd', ino: ino})
			delete(c.completeDirs, ino)
			c.pruneOrphanDirMetaLocked(ino)
			return
		}
		dirEntryExpires = make([]int64, len(entries))
		dirAttrExpires = make([]int64, len(entries))
		for i, entry := range entries {
			if eDur := ttlDuration(entry.GetEntryTimeoutSeconds()); eDur > 0 {
				dirEntryExpires[i] = min(expiresAtNano, now.Add(eDur).UnixNano())
			}
			if aDur := ttlDuration(entry.GetAttrTimeoutSeconds()); aDur > 0 {
				dirAttrExpires[i] = min(expiresAtNano, now.Add(aDur).UnixNano())
			}
		}
	}

	for _, name := range copyNames(c.byParent[ino]) {
		c.removeKey(cacheKey{kind: 'e', ino: ino, name: name})
	}
	c.putItem(&cacheItem{
		key:             cacheKey{kind: 'd', ino: ino},
		dir:             cloneDir(entries),
		dirEntryExpires: dirEntryExpires,
		dirAttrExpires:  dirAttrExpires,
		expiresAtNano:   expiresAtNano,
		weight:          len(entries) + 1,
	})

	if c.items[cacheKey{kind: 'd', ino: ino}] != nil {
		c.completeDirs[ino] = expiresAtNano
		if c.rootIno != 0 && ino == c.rootIno {
			c.parentByDir[ino] = ino
		} else {
			for _, entry := range entries {
				if string(entry.GetName()) == ".." && entry.GetIno() != 0 {
					c.parentByDir[ino] = entry.GetIno()
					break
				}
			}
		}
	} else {
		delete(c.completeDirs, ino)
		c.pruneOrphanDirMetaLocked(ino)
	}
}

func (c *NamespaceCache) clear() {
	c.mu.Lock()
	c.epoch++
	c.mutations++
	c.items = make(map[cacheKey]*list.Element)
	c.lru.Init()
	c.used = 0
	c.byParent = make(map[uint64]map[string]struct{})
	c.dirsByChild = make(map[uint64]map[uint64]struct{})
	c.inodeDirs = make(map[uint64]map[uint64]struct{})
	c.completeDirs = make(map[uint64]int64)
	c.parentByDir = make(map[uint64]uint64)
	c.mu.Unlock()
	c.metrics.Load().incNSCacheInvalidation()
}

func (c *NamespaceCache) version() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

func (c *NamespaceCache) snapshot() (epoch, generation uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch, c.mutations
}

func copyNames(names map[string]struct{}) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	return out
}

func copyInoSet(inos map[uint64]struct{}) map[uint64]struct{} {
	if len(inos) == 0 {
		return nil
	}
	out := make(map[uint64]struct{}, len(inos))
	for ino := range inos {
		out[ino] = struct{}{}
	}
	return out
}
