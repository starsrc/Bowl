package fusefs

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func makeTestProtoInode(ino, size uint64, ts time.Time) *storagev1.Inode {
	pbTS := timestamppb.New(ts)
	return &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:   uint32(syscall.S_IFREG | 0o644),
			Size:   size,
			Nlink:  1,
			Blocks: (size + 511) / 512,
			Atime:  pbTS,
			Mtime:  pbTS,
			Ctime:  pbTS,
		},
	}
}

func makeTestProtoDirInode(ino uint64, ts time.Time) *storagev1.Inode {
	pbTS := timestamppb.New(ts)
	return &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  uint32(syscall.S_IFDIR | 0o755),
			Nlink: 2,
			Atime: pbTS,
			Mtime: pbTS,
			Ctime: pbTS,
		},
	}
}

func TestRO_NamespaceCache_InternalPeekAndSynthesizeCompleteDirRespectExpiration(t *testing.T) {
	t.Run("peekCachedInodeLocked expires embedded child attrs in dir stream", func(t *testing.T) {
		clk := newFakeClockInternal(time.Unix(1_700_000_000, 0))
		cache := NewNamespaceCache(64)
		cache.configure(1, 2.0, 1.0)
		cache.configureReadOnly(clk.Now)

		rootInode := makeTestProtoDirInode(1, clk.Now())
		childInode := makeTestProtoInode(200, 12, clk.Now())
		cache.putDirAtIf(0, 0, 1, []*storagev1.DirEntryPlus{
			{Ino: 1, Name: []byte("."), NextOffset: 1, Inode: rootInode, AttrTimeoutSeconds: 1.0},
			{Ino: 1, Name: []byte(".."), NextOffset: 2, Inode: rootInode, AttrTimeoutSeconds: 1.0},
			{Ino: 200, Name: []byte("f.txt"), NextOffset: 3, Inode: childInode, EntryTimeoutSeconds: 2.0, AttrTimeoutSeconds: 1.0},
		})

		clk.Advance(400 * time.Millisecond)
		cache.mu.Lock()
		got, ok := cache.peekCachedInodeLocked(200)
		cache.mu.Unlock()
		require.True(t, ok)
		require.InDelta(t, 0.6, got.attrTimeout, 1e-6)

		clk.Advance(600 * time.Millisecond)
		cache.mu.Lock()
		_, expiredOK := cache.peekCachedInodeLocked(200)
		cache.mu.Unlock()
		require.False(t, expiredOK, "embedded child attr in 'd' must expire at min(entryTTL, attrTTL)")
	})

	t.Run("synthesizeCompleteDirLocked rejects expired completeDirs or expired child entries and caps remaining TTL", func(t *testing.T) {
		clk := newFakeClockInternal(time.Unix(1_700_000_000, 0))
		cache := NewNamespaceCache(64)
		cache.configure(1, 2.0, 1.0)
		cache.configureReadOnly(clk.Now)

		rootInode := makeTestProtoDirInode(1, clk.Now())
		childInode := makeTestProtoInode(300, 64, clk.Now())
		cache.putInodeAt(0, 1, rootInode, 2.0, 1.0)
		epoch, gen := cache.snapshot()
		cache.putDirAtIf(gen, epoch, 1, []*storagev1.DirEntryPlus{
			{Ino: 1, Name: []byte("."), NextOffset: 1, Inode: rootInode, AttrTimeoutSeconds: 1.0},
			{Ino: 1, Name: []byte(".."), NextOffset: 2, Inode: rootInode, AttrTimeoutSeconds: 1.0},
			{Ino: 300, Name: []byte("c.bin"), NextOffset: 3, Inode: childInode, EntryTimeoutSeconds: 2.0, AttrTimeoutSeconds: 1.0},
		})

		clk.Advance(200 * time.Millisecond)
		cache.mu.Lock()
		cache.demoteDirStreamToEntriesLocked(1)
		synth, ok := cache.synthesizeCompleteDirLocked(1)
		cache.mu.Unlock()
		require.True(t, ok)
		require.Len(t, synth, 3)

		clk.Advance(800 * time.Millisecond)
		cache.mu.Lock()
		_, stillValid := cache.synthesizeCompleteDirLocked(1)
		cache.mu.Unlock()
		require.False(t, stillValid, "synthesized complete dir must expire at original TTL deadline")
		_, dirHit := cache.dir(1)
		require.False(t, dirHit, "synthesized 'd' stream must expire at original TTL deadline")
	})

	t.Run("synthesizeCompleteDirLocked evicts expired child entry and drops completeDirs", func(t *testing.T) {
		clk := newFakeClockInternal(time.Unix(1_700_000_000, 0))
		cache := NewNamespaceCache(64)
		cache.configure(1, 2.0, 2.0)
		cache.configureReadOnly(clk.Now)

		rootInode := makeTestProtoDirInode(1, clk.Now())
		childInode := makeTestProtoInode(301, 64, clk.Now())
		cache.putInodeAt(0, 1, rootInode, 2.0, 2.0)
		epoch, gen := cache.snapshot()
		cache.putEntryAtIf(gen, epoch, 1, "short.bin", 301, 0.5)
		cache.putInodeAt(0, 301, childInode, 0.5, 2.0)
		cache.mu.Lock()
		cache.completeDirs[1] = clk.Now().Add(2 * time.Second).UnixNano()
		cache.mu.Unlock()

		clk.Advance(500 * time.Millisecond)
		cache.mu.Lock()
		_, ok := cache.synthesizeCompleteDirLocked(1)
		_, stillComplete := cache.completeDirs[1]
		cache.mu.Unlock()
		require.False(t, ok)
		require.False(t, stillComplete)
	})

	t.Run("ReadAheadCoordinator TrackOpenInode UntrackOpenInode and RemoveHandle prune idle tracks", func(t *testing.T) {
		clk := newFakeClockInternal(time.Unix(1_700_000_000, 0))
		coord := NewReadAheadCoordinator()
		coord.configureReadOnly(1.0, clk.Now)

		inode := makeTestProtoInode(555, 1024, clk.Now())
		coord.TrackOpenInode(555, inode)
		coord.mu.Lock()
		require.Len(t, coord.inodeTracks, 1)
		coord.mu.Unlock()

		_ = coord.getOrCreateHandle(77, 555)
		coord.UntrackOpenInode(555)
		coord.mu.Lock()
		require.Len(t, coord.inodeTracks, 1, "track remains while handleReadAhead is active")
		coord.mu.Unlock()

		coord.RemoveHandle(77)
		coord.mu.Lock()
		require.Empty(t, coord.inodeTracks, "track must be pruned once openCount and byInode drop to 0")
		coord.mu.Unlock()

		coord.ObserveInode(999, inode)
		coord.mu.Lock()
		require.Empty(t, coord.inodeTracks, "unopened inode ObserveInode must not allocate track")
		coord.mu.Unlock()
	})

	t.Run("TrackOpenInode with stale cached inode does not roll back newer stamp or invalidate fresh read-ahead", func(t *testing.T) {
		t0 := time.Unix(1_700_000_000, 0)
		clk := newFakeClockInternal(t0)
		coord := NewReadAheadCoordinator()
		coord.configureReadOnly(5.0, clk.Now)

		const ino uint64 = 777
		const fh1 uint64 = 10
		v1Inode := makeTestProtoInode(ino, 1<<20, t0)
		v2Inode := makeTestProtoInode(ino, 1<<20, t0.Add(time.Second))

		coord.TrackOpenInode(ino, v1Inode)
		coord.ObserveInode(ino, v2Inode)

		var fetchCalls atomic.Int32
		v2Payload := makeFilledChunk(1<<20, 0x22)
		fetch := func(_ context.Context, off uint64, size uint32) (rpc.ReadPayload, error) {
			fetchCalls.Add(1)
			end := min(len(v2Payload), int(off)+int(size))
			return rpc.ReadPayload{Data: v2Payload[off:end]}, nil
		}

		buf := make([]byte, 256<<10)
		_, err := coord.Read(context.Background(), fh1, ino, 0, buf, fetch)
		require.NoError(t, err)
		_, err = coord.Read(context.Background(), fh1, ino, 256<<10, buf, fetch)
		require.NoError(t, err)
		callsAfterWarm := fetchCalls.Load()

		coord.TrackOpenInode(ino, v1Inode)

		coord.mu.Lock()
		tr := coord.inodeTracks[ino]
		require.NotNil(t, tr)
		require.Equal(t, stampFromInodeMust(v2Inode), tr.stamp, "TrackOpenInode must not roll back newer observed stamp")
		coord.mu.Unlock()

		n, err := coord.Read(context.Background(), fh1, ino, 512<<10, buf, fetch)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, byte(0x22), buf[0])
		require.Equal(t, callsAfterWarm, fetchCalls.Load(), "stale TrackOpenInode must not invalidate valid v2 window")
	})
}

func stampFromInodeMust(inode *storagev1.Inode) inodeStamp {
	s, ok := stampFromInode(inode)
	if !ok {
		panic("invalid test inode")
	}
	return s
}

type fakeClockInternal struct {
	now time.Time
}

func newFakeClockInternal(t0 time.Time) *fakeClockInternal {
	return &fakeClockInternal{now: t0}
}

func (c *fakeClockInternal) Now() time.Time {
	return c.now
}

func (c *fakeClockInternal) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func assertDirMetaSubsetInvariant(t *testing.T, cache *NamespaceCache) {
	t.Helper()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	require.LessOrEqual(t, len(cache.completeDirs), cache.capacity, "completeDirs must never exceed cache capacity")
	require.LessOrEqual(t, len(cache.parentByDir), cache.capacity, "parentByDir must never exceed cache capacity")
	for ino := range cache.completeDirs {
		hasI := cache.items[cacheKey{kind: 'i', ino: ino}] != nil
		hasD := cache.items[cacheKey{kind: 'd', ino: ino}] != nil
		require.True(t, hasI || hasD, "completeDirs[%d] outlived both 'i' and 'd' LRU entries", ino)
	}
	for ino := range cache.parentByDir {
		hasI := cache.items[cacheKey{kind: 'i', ino: ino}] != nil
		hasD := cache.items[cacheKey{kind: 'd', ino: ino}] != nil
		require.True(t, hasI || hasD, "parentByDir[%d] outlived both 'i' and 'd' LRU entries", ino)
	}
}

func TestNamespaceCache_CompleteDirsAndParentByDirCoupledToLRUEviction(t *testing.T) {
	t.Run("GivenSmallCapacity_WhenCreatingOver2xCapacityDirs_ThenCompleteDirsAndParentByDirStayBoundedAndTrackNewestDirs", func(t *testing.T) {

		const capacity = 8
		cache := NewNamespaceCache(capacity)
		cache.configure(1, 2.0, 2.0)
		now := time.Unix(1_700_000_000, 0)

		for i := uint64(10); i < 10+3*capacity; i++ {
			dirInode := makeTestProtoDirInode(i, now)
			cache.putInodeAt(0, i, dirInode, 2.0, 2.0)
			cache.markNewDirWithParentAt(0, i, 1)

			assertDirMetaSubsetInvariant(t, cache)
			cache.mu.Lock()
			_, hasComplete := cache.completeDirs[i]
			parent, hasParent := cache.parentByDir[i]
			cache.mu.Unlock()
			require.True(t, hasComplete, "newest dir %d must be recorded in completeDirs after LRU eviction", i)
			require.True(t, hasParent, "newest dir %d must be recorded in parentByDir after LRU eviction", i)
			require.Equal(t, uint64(1), parent)
		}

		cache.mu.Lock()
		_, oldestComplete := cache.completeDirs[10]
		_, oldestParent := cache.parentByDir[10]
		cache.mu.Unlock()
		require.False(t, oldestComplete, "evicted oldest dir must be purged from completeDirs")
		require.False(t, oldestParent, "evicted oldest dir must be purged from parentByDir")
	})

	t.Run("GivenROCache_WhenDirInodeAndStreamExpire_ThenCompleteDirsAndParentByDirPruned", func(t *testing.T) {
		clk := newFakeClockInternal(time.Unix(1_700_000_000, 0))
		cache := NewNamespaceCache(16)
		cache.configure(1, 1.0, 1.0)
		cache.configureReadOnly(clk.Now)

		parentInode := makeTestProtoDirInode(1, clk.Now())
		dirInode := makeTestProtoDirInode(50, clk.Now())
		cache.putInodeAt(0, 50, dirInode, 1.0, 1.0)
		epoch, gen := cache.snapshot()
		cache.putDirAtIf(gen, epoch, 50, []*storagev1.DirEntryPlus{
			{Ino: 50, Name: []byte("."), NextOffset: 1, Inode: dirInode, AttrTimeoutSeconds: 1.0},
			{Ino: 1, Name: []byte(".."), NextOffset: 2, Inode: parentInode, AttrTimeoutSeconds: 1.0},
		})
		assertDirMetaSubsetInvariant(t, cache)

		clk.Advance(1500 * time.Millisecond)
		_, _ = cache.dir(50)
		_, _ = cache.inode(50)

		assertDirMetaSubsetInvariant(t, cache)
		cache.mu.Lock()
		_, hasComplete := cache.completeDirs[50]
		_, hasParent := cache.parentByDir[50]
		cache.mu.Unlock()
		require.False(t, hasComplete)
		require.False(t, hasParent)
	})

	t.Run("GivenDemotedDirWithoutParentInode_WhenDemoteRuns_ThenCompleteDirsAndParentByDirNotLeaked", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		cache := NewNamespaceCache(16)
		cache.configure(1, 2.0, 2.0)

		parentInode := makeTestProtoDirInode(1, now)
		dirInode := makeTestProtoDirInode(60, now)
		childInode := makeTestProtoInode(61, 10, now)
		epoch, gen := cache.snapshot()

		cache.putDirAtIf(gen, epoch, 60, []*storagev1.DirEntryPlus{
			{Ino: 60, Name: []byte("."), NextOffset: 1, Inode: dirInode, AttrTimeoutSeconds: 2.0},
			{Ino: 1, Name: []byte(".."), NextOffset: 2, Inode: parentInode, AttrTimeoutSeconds: 2.0},
			{Ino: 61, Name: []byte("a.txt"), NextOffset: 3, Inode: childInode, EntryTimeoutSeconds: 2.0, AttrTimeoutSeconds: 2.0},
		})

		cache.mu.Lock()
		cache.demoteDirStreamToEntriesLocked(60)
		_, hasComplete := cache.completeDirs[60]
		_, hasParent := cache.parentByDir[60]
		cache.mu.Unlock()

		require.False(t, hasComplete, "demote without live 'i' must not leave orphan completeDirs")
		require.False(t, hasParent, "demote without live 'i' must not leave orphan parentByDir")
		assertDirMetaSubsetInvariant(t, cache)
	})

	t.Run("GivenOversizedDirStream_WhenPutDirExceedsCapacity_ThenCompleteDirsAndParentByDirNotRecorded", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		cache := NewNamespaceCache(4)
		cache.configure(1, 2.0, 2.0)

		parentInode := makeTestProtoDirInode(1, now)
		dirInode := makeTestProtoDirInode(70, now)
		entries := []*storagev1.DirEntryPlus{
			{Ino: 70, Name: []byte("."), NextOffset: 1, Inode: dirInode, AttrTimeoutSeconds: 2.0},
			{Ino: 1, Name: []byte(".."), NextOffset: 2, Inode: parentInode, AttrTimeoutSeconds: 2.0},
			{Ino: 71, Name: []byte("f1"), NextOffset: 3, Inode: makeTestProtoInode(71, 1, now), EntryTimeoutSeconds: 2.0, AttrTimeoutSeconds: 2.0},
			{Ino: 72, Name: []byte("f2"), NextOffset: 4, Inode: makeTestProtoInode(72, 1, now), EntryTimeoutSeconds: 2.0, AttrTimeoutSeconds: 2.0},
		}
		epoch, gen := cache.snapshot()
		cache.putDirAtIf(gen, epoch, 70, entries)

		cache.mu.Lock()
		_, hasComplete := cache.completeDirs[70]
		_, hasParent := cache.parentByDir[70]
		cache.mu.Unlock()
		require.False(t, hasComplete)
		require.False(t, hasParent)
		assertDirMetaSubsetInvariant(t, cache)
	})

	t.Run("GivenInPlaceInodeRefresh_WhenParentInodeTouched_ThenCompleteDirsAndParentByDirPreserved", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		cache := NewNamespaceCache(16)
		cache.configure(1, 2.0, 2.0)

		dirInode := makeTestProtoDirInode(80, now)
		childInode := makeTestProtoInode(81, 8, now.Add(time.Second))
		cache.putInodeAt(0, 80, dirInode, 2.0, 2.0)
		cache.markNewDirWithParentAt(0, 80, 1)

		cache.refreshInodeAt(0, 80, dirInode, 2.0)
		cache.invalidateDirListingTouchParentAt(0, 80, childInode, 0, 2.0)
		cache.invalidateDirListingTouchParentNowAt(0, 80, 0)

		cache.mu.Lock()
		_, hasComplete := cache.completeDirs[80]
		parent := cache.parentByDir[80]
		cache.mu.Unlock()
		require.True(t, hasComplete, "in-place inode refresh must preserve completeDirs")
		require.Equal(t, uint64(1), parent, "in-place inode refresh must preserve parentByDir")
		assertDirMetaSubsetInvariant(t, cache)

		cache.invalidateInode(80)
		cache.mu.Lock()
		_, completeAfterInv := cache.completeDirs[80]
		_, parentAfterInv := cache.parentByDir[80]
		cache.mu.Unlock()
		require.False(t, completeAfterInv)
		require.False(t, parentAfterInv)
		assertDirMetaSubsetInvariant(t, cache)
	})
}
