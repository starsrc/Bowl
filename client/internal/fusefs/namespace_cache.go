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

// DefaultNamespaceCacheCapacity 限制缓存的节点、目录项和目录流所占条目数。
// mdtest 1proc 会创建约 1.2 万个节点，16K 容量会在创建阶段持续 LRU 抖动，
// 64K 可容纳整棵工作集，同时常驻内存增量仍在数十 MB 量级。
const DefaultNamespaceCacheCapacity = 65536

// MaxFilenameBytes 定义单个目录项名称的最大字节数（对齐 POSIX NAME_MAX 与服务端契约）。
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

// NamespaceCache 按最近使用顺序淘汰属性、目录项和完整目录流。
// 目录流按包含的目录项数计费，避免单个目录绕过容量限制。
//
// 除 LRU 链表外维护三组二级索引，把失效与回填从 O(N) 全链表扫描降为 O(1)/O(K) 定向查找：
//   - byParent：父目录 Inode -> 其独立目录项名称集合；
//   - dirsByChild：子 Inode -> 引用它的目录流父 Inode 集合；
//   - inodeDirs：目录流父 Inode -> 它引用的子 Inode 集合（用于定向清理前两者）。
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
	// completeDirs 记录“目录内容已被完整掌握”的父节点：目录流已加载，或目录刚由 Mkdir 创建且为空。
	// 命中时未找到的名字可以确定返回 ENOENT，无需再发 Lookup RPC。
	// 任一目录项被淘汰或目录发生未知变更时必须同步撤销该标记，避免把已存在名字误判为不存在。
	// 在只读挂载模式下，值保存目录完整性的过期纳秒时间戳；可写挂载模式下保持为 0（永不过期）。
	completeDirs map[uint64]int64
	// parentByDir 记录已知目录的权威父目录内核 Inode（来自 Mkdir 或 ReadDirPlus 的 ".."），
	// 供 completeDirs 在目录流缺失时本地合成包含 "." 与 ".." 的完整目录流。
	parentByDir map[uint64]uint64
	metrics     atomic.Pointer[Metrics]
}

// NewNamespaceCache 创建容量有界的缓存。
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

// index 登记缓存项对应的二级索引。
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

// unindex 在缓存项移除前撤销其二级索引。
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
		// 目录流一旦移除，就不再掌握该目录的完整内容：必须撤销权威完整标记，
		// 否则后续缺失名字会被误判为 ENOENT（目录流本身已不再持有目录项）。
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
		// 无法容纳的目录项意味着该父目录的完整内容不再可信。
		if key.kind == 'e' {
			delete(c.completeDirs, key.ino)
		}
		return
	}
	for c.used+weight > c.capacity {
		back := c.lru.Back()
		if back == nil {
			break
		}
		// 目录项被容量淘汰后，该父目录的完整内容不再可信；属性与目录流的淘汰不影响完整标记。
		if evicted := itemOf(back); evicted.key.kind == 'e' {
			delete(c.completeDirs, evicted.key.ino)
		}
		c.remove(back)
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

// cloneInode 手写复制 Inode，避免 proto.Clone 的反射深拷贝开销。
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

// cloneDirEntry 手写复制目录项，名称字节与内嵌 Inode 均独立持有。
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
	// 完整目录流本身包含属性，即使独立属性项被淘汰也可定向命中。
	for dir := range c.dirsByChild[ino] {
		dirEl := c.items[cacheKey{kind: 'd', ino: dir}]
		if dirEl == nil {
			continue
		}
		dItem := itemOf(dirEl)
		if c.readOnly && nowNano >= dItem.expiresAtNano {
			c.remove(dirEl)
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

// inode 返回深拷贝后的节点属性，供外部调用方安全读写。
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

// inodeReadOnly 单次加锁零分配读取节点属性快照；缓存内 Inode 一经发布严禁原地修改，调用方仅可只读访问返回值。
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

// lookupReadOnly 在单把锁内同时解析目录项与目标节点属性（零堆分配只读快路径）：
//   - negative == true && ok == true：权威负缓存命中（ENOENT）；
//   - negative == false && ok == true：目录项与节点属性同时命中，返回只读 cachedInode；
//   - ok == false：未完整命中；若 childIno > 0 表示目录项已命中正子节点但缺少缓存属性。
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

// putInodeAtIf 仅在缓存未被任何目录变更改写时回填，避免在途读响应覆盖更新的属性。
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
			continue
		}
		entries := dItem.dir
		for i, entry := range entries {
			if entry.GetIno() != ino {
				continue
			}
			// 替换而非原地改写：目录流一旦发布就不再被修改，
			// dir() 只需做指针快照即可安全并发读取。
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

// refreshExistingInodesFromReadDirAt updates already-cached inode items and parent directory
// entries using fresh attributes returned by ReadDirPlus in read-only mode without inserting
// uncached child inodes or bumping mutations.
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
			el = nil
		}
		ttlDur := ttlDuration(attrTimeout)
		if ttlDur <= 0 {
			if el != nil {
				c.remove(el)
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

// refreshInodeAt 用写操作返回的最新属性刷新缓存项，但保留既有目录项有效期。
// Write / SetAttr / Fallocate / Open(O_TRUNC) 的响应不携带 entry timeout；
// 若沿用 putInodeAt(..., 0, ...) 把 0 写回缓存，后续 Lookup 命中缓存时会把
// EntryValid=0 交给内核，内核据此拒绝缓存 dentry，使该文件每次 stat 都穿透到 FUSE Lookup。
// 同时原地刷新所有引用该 Inode 的目录流，避免无谓丢弃整条目录流。
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

// refreshInodeAtIf 是 refreshInodeAt 的代数校验版本，供 GetAttr 回填使用。
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

// stageWriteInodeAt 在 Stage 写暂存落盘成功后，以写时复制方式单调推进本地缓存的 Size 与 Mtime/Ctime，
// 使 AttrTimeout > 0 时随后的本地 fstat/stat 零 RPC 即可观测最新文件大小，且绝不原地修改已发布指针。
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

// refreshInodeFromWriteAt 在异步/聚合写提交完成时刷新缓存，并保护同一顺序写世代内已被后续缓冲推进的最小文件尺寸不倒退。
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

// entryTimeoutLocked 取回该 Inode 已知的目录项有效期：优先独立属性项，
// 其次仍引用它的目录流，都没有时才回落到 fallback。
// fallback 由调用方提供（GetAttr / SetAttr 用服务端返回的 attr timeout，Write 用默认写回填值），
// 保证 STORAGE_ENTRY_TIMEOUT_SECONDS=0 的 pjdfstest 场景不会产生非预期的非零 EntryValid。
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

// peekInode 在不污染属性命中率统计的前提下探查缓存中的节点属性（供 Unlink/Rename 判断 nlink 使用）。
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

// inodeAttrTimeout 读取缓存中已知的节点属性 TTL；未缓存时回退至 fallback。
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

func (c *NamespaceCache) readlink(ino uint64, now time.Time) ([]byte, bool) {
	c.mu.Lock()
	el := c.items[cacheKey{kind: 'l', ino: ino}]
	if el == nil {
		c.mu.Unlock()
		c.metrics.Load().incNSCacheMiss(nsCacheReadlink)
		return nil, false
	}
	item := itemOf(el)
	if now.UnixNano() >= item.expiresAtNano {
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

func (c *NamespaceCache) putReadlinkAt(epoch, ino uint64, target []byte, ttlSeconds float64, now time.Time) {
	if ttlSeconds <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.putReadlinkLocked(ino, target, ttlSeconds, now)
}

func (c *NamespaceCache) putReadlinkAtIf(generation, epoch, ino uint64, target []byte, ttlSeconds float64, now time.Time) {
	if ttlSeconds <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.putReadlinkLocked(ino, target, ttlSeconds, now)
}

func (c *NamespaceCache) putReadlinkLocked(ino uint64, target []byte, ttlSeconds float64, now time.Time) {
	ttlDur := time.Duration(ttlSeconds * float64(time.Second))
	if ttlDur <= 0 {
		return
	}
	c.putItem(&cacheItem{
		key:           cacheKey{kind: 'l', ino: ino},
		linkTarget:    append([]byte(nil), target...),
		expiresAtNano: now.Add(ttlDur).UnixNano(),
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
	}
	c.mutations++
	c.mu.Unlock()
	c.metrics.Load().incNSCacheInvalidation()
}

// entry 返回子节点号；0 表示已缓存的 ENOENT。
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
	// 目录内容已被完整掌握（目录流已降级为独立目录项，或目录刚创建且为空）：
	// 未命中即权威 ENOENT，可直接返回 0 而不发网络 Lookup。
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

// putEntryAtIf 仅在缓存未被任何目录变更改写时回填，避免在途 Lookup 的过期结果覆盖新目录项。
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
	// 独立目录项与旧目录流冲突：先把目录流按条目降级，保留未变动兄弟项的权威命中，
	// 再写入本次变更的目录项。
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
	// 父节点属性变化会使其出现在其它目录流的目录项中，一并定向失效。
	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
	}
	c.mutations++
	c.mu.Unlock()
	c.metrics.Load().incNSCacheInvalidation()
}

// invalidateDirListing 只失效父目录的目录流、父目录属性以及引用了父目录的祖先目录流，
// demoteDirStreamToEntriesLocked 把父目录的完整目录流拆成独立目录项，并保留尚无独立属性项的子节点属性。
// 只在目录流条目数不超过容量四分之一时降级，避免小容量缓存被单个大目录挤爆；
// 若降级过程中同父目录的目录项被 LRU 淘汰，则不能声称内容完整，撤销权威标记。
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
		return
	}
	if len(entries) > c.capacity/4 {
		return
	}
	evictedSameParent := false
	for idx, entry := range entries {
		name := string(entry.GetName())
		if name == ".." && entry.GetIno() != 0 && len(c.parentByDir) < c.capacity {
			c.parentByDir[parent] = entry.GetIno()
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
		// put 每个目录项最多触发一次容量淘汰，预先判断被淘汰项是否属于同一父目录。
		if c.items[key] == nil && c.used+1 > c.capacity {
			if back := c.lru.Back(); back != nil {
				if item := itemOf(back); item.key.kind == 'e' && item.key.ino == parent {
					evictedSameParent = true
				}
			}
		}
		c.putItem(&cacheItem{key: key, entry: entry.GetIno(), expiresAtNano: entryExp, weight: 1})
	}
	if !evictedSameParent {
		c.completeDirs[parent] = dExpires
	}
	// 当目录流携带有效属性（AttrTimeoutSeconds > 0）时，为尚无独立 'i' 项的子节点及父目录（".."）保留属性，
	// 消除祖先目录流因局部目录变更降级后后续 OpenDir/GetAttr 对未变动子目录的穿透 RPC。
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

// markNewDirWithParentAt 把刚由 Mkdir 创建的空目录登记为权威完整目录并记录其权威父目录。
func (c *NamespaceCache) markNewDirWithParentAt(epoch, dirIno, parentIno uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	// 标记不参与 LRU 计费，用容量作为上界，避免长生命周期客户端创建海量目录后无界增长。
	if len(c.completeDirs) < c.capacity {
		c.completeDirs[dirIno] = 0
		if parentIno != 0 && len(c.parentByDir) < c.capacity {
			c.parentByDir[dirIno] = parentIno
		}
	}
}

// invalidateDirListingTouchParentAt 在目录项变更后失效父目录的目录流与属性，
// 并用服务端返回的子节点时间戳就地刷新父目录属性（mtime/ctime，Mkdir 时 nlink+1）。
// 内核在 Create/Mkdir/Mknod/Symlink/Link 后会主动失效父目录属性并触发 FUSE_GETATTR；
// 保留一份已更新的父属性可消除这次网络 GetAttr RPC。
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
	}
	c.mutations++
	if !hadParent || (epoch != ^uint64(0) && epoch != c.epoch) ||
		parentEntry.inode == nil || parentEntry.inode.GetAttr() == nil ||
		child == nil || child.GetAttr() == nil {
		return
	}
	ts := child.GetAttr().GetCtime()
	if ts == nil {
		ts = child.GetAttr().GetMtime()
	}
	if ts == nil {
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

// invalidateDirListingTouchParentNowAt 在 Unlink/Rmdir/Rename 成功后失效父目录的
// 目录流与引用它的祖先流，但保留父目录 Inode 缓存并用当前时间刷新 mtime/ctime。
// 内核在每次目录变更后会 fuse_invalidate_attr(dir)，下一次 unlink/rename 前的
// inode_permission(MAY_EXEC) 会下发 FUSE_GETATTR；保留一份刚更新的父属性可消除
// 这次每操作一次的跨机 GetAttr RPC（cold unlink 500 次即 500 次 RPC）。
// nlinkDelta 仅在目录被删除/移出/移入父目录时非零。
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
	}
	c.mutations++
	if !hadParent || (epoch != ^uint64(0) && epoch != c.epoch) ||
		parentEntry.inode == nil || parentEntry.inode.GetAttr() == nil {
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

// removeEntry 定向删除单个目录项，用于 Unlink/Rmdir/Rename 的旧名。
func (c *NamespaceCache) removeEntry(parent uint64, name string) {
	c.mu.Lock()
	// 删除单个名字后无法再断言目录内容完整，撤销权威标记，避免误判其它名字。
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
		// 只做指针快照：缓存项一经发布就不再原地修改（putInodeLocked 以替换元素的方式更新），
		// 因此调用方可以安全并发读取。500 项目录从 2,001 次分配 / 65~97µs 降到 1 次分配 / ~0.3µs。
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

// synthesizeCompleteDirLocked 在目录处于权威完整状态（completeDirs）且自身、父目录及全部正子项属性均在有效缓存中时，
// 直接按服务端 ReadDirPlus 契约（"."、".." 及按字节序升序排列的正子项与连续 NextOffset）本地合成完整目录流并回填 'd'。
// 任一子项或父项缺失有效属性缓存时立即返回 false，由调用方安全回退远端 ReadDirPlus RPC。
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

// putDirAtIf 仅在缓存未被任何目录变更改写时回填，避免在途目录流覆盖新目录项。
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
			return
		}
		dirDur := ttlDuration(min(c.entryTTL, c.attrTTL))
		if dirDur <= 0 {
			c.removeKey(cacheKey{kind: 'd', ino: ino})
			delete(c.completeDirs, ino)
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
	// 旧的独立目录项可能与新目录流冲突，按父目录定向清理。
	for _, name := range copyNames(c.byParent[ino]) {
		c.removeKey(cacheKey{kind: 'e', ino: ino, name: name})
	}
	if c.rootIno != 0 && ino == c.rootIno && len(c.parentByDir) < c.capacity {
		c.parentByDir[ino] = ino
	} else {
		for _, entry := range entries {
			if string(entry.GetName()) == ".." && entry.GetIno() != 0 {
				if len(c.parentByDir) < c.capacity {
					c.parentByDir[ino] = entry.GetIno()
				}
				break
			}
		}
	}
	c.putItem(&cacheItem{
		key:             cacheKey{kind: 'd', ino: ino},
		dir:             cloneDir(entries),
		dirEntryExpires: dirEntryExpires,
		dirAttrExpires:  dirAttrExpires,
		expiresAtNano:   expiresAtNano,
		weight:          len(entries) + 1,
	})
	// 目录流已完整加载：未命中的名字可权威判定为 ENOENT。
	if len(entries)+1 <= c.capacity {
		c.completeDirs[ino] = expiresAtNano
	} else {
		delete(c.completeDirs, ino)
	}
}

// clear 在会话失效后丢弃旧会话期间获得的全部缓存。
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

// snapshot 同时返回会话代数与目录变更代数，供在途 RPC 回填前校验。
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
