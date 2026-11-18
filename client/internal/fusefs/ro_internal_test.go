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

		// Demote dir stream into entries + completeDirs + child 'i' at t + 200ms.
		clk.Advance(200 * time.Millisecond)
		cache.mu.Lock()
		cache.demoteDirStreamToEntriesLocked(1)
		synth, ok := cache.synthesizeCompleteDirLocked(1)
		cache.mu.Unlock()
		require.True(t, ok)
		require.Len(t, synth, 3)

		// Synthesized 'd' must NOT outlive the original 1.0s TTL (expires at t0 + 1.0s, not t0 + 1.2s).
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

		// Warm fh1's read-ahead window with v2 data.
		buf := make([]byte, 256<<10)
		_, err := coord.Read(context.Background(), fh1, ino, 0, buf, fetch)
		require.NoError(t, err)
		_, err = coord.Read(context.Background(), fh1, ino, 256<<10, buf, fetch)
		require.NoError(t, err)
		callsAfterWarm := fetchCalls.Load()

		// Cached Open(fh2) observes stale v1 from NamespaceCache and calls TrackOpenInode.
		coord.TrackOpenInode(ino, v1Inode)

		coord.mu.Lock()
		tr := coord.inodeTracks[ino]
		require.NotNil(t, tr)
		require.Equal(t, stampFromInodeMust(v2Inode), tr.stamp, "TrackOpenInode must not roll back newer observed stamp")
		coord.mu.Unlock()

		// fh1 must keep its valid v2 read-ahead window with zero extra fetch calls.
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
