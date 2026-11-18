package fusefs_test

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestRawFSWithClock(t *testing.T, s *fakeStorage, clk *fakeClock, opts ...rawFSOption) *fusefs.RawFS {
	t.Helper()
	cfg := fusefs.Config{
		DiskID:              s.diskID,
		Epoch:               s.epoch,
		RootIno:             s.rootIno,
		RootInode:           s.rootInodeClone(),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	fs := fusefs.NewRawFSWithClockForTest(s, cfg, clk.Now)
	t.Cleanup(fs.OnUnmount)
	return fs
}

func TestRO_NamespaceCache_InodeAttrTTLExpirationAndClamping(t *testing.T) {
	t.Run("RootInode seeded at mount clamps remaining AttrValid and expires at AttrTimeoutSeconds", func(t *testing.T) {
		// Given
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		// When: advance 500ms before expiry and read RootInode via GetAttr.
		clk.Advance(500 * time.Millisecond)
		hitOut := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)

		// Then: cache hit clamps AttrValid to remaining 1.5s with zero RPCs.
		require.Zero(t, srv.getAttrCalls.Load())
		require.Equal(t, uint64(1), hitOut.AttrValid)
		require.Equal(t, uint32(500_000_000), hitOut.AttrValidNsec)

		// When: advance to exact expiration boundary (t0 + 2.0s) and read RootInode again.
		clk.Advance(1500 * time.Millisecond)
		missOut := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)

		// Then: expired RootInode lazily evicts and refreshes from server with full 2.0s TTL.
		require.Equal(t, int64(1), srv.getAttrCalls.Load())
		require.Equal(t, uint64(2), missOut.AttrValid)
		require.Zero(t, missOut.AttrValidNsec)
	})

	t.Run("File inode attr expires lazily on read and reflects updated server metadata", func(t *testing.T) {
		// Given
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 1.5))
		ino := srv.seedFile(1, "data.bin", 0o644, []byte("v1"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		first := mustGetAttr(t, fs, ino)
		require.Equal(t, uint64(2), first.Size)
		require.Equal(t, int64(1), srv.getAttrCalls.Load())

		// When: writer updates size on server, then RO calls GetAttr before and after 1.5s TTL.
		srv.mu.Lock()
		srv.nodes[ino].data = []byte("v2-longer")
		srv.nodes[ino].inode.Attr.Size = 9
		srv.mu.Unlock()

		clk.Advance(600 * time.Millisecond)
		cachedHit := mustGetAttr(t, fs, ino)
		require.Equal(t, int64(1), srv.getAttrCalls.Load())
		require.Equal(t, uint64(2), cachedHit.Size)
		require.Zero(t, cachedHit.AttrValid)
		require.Equal(t, uint32(900_000_000), cachedHit.AttrValidNsec)

		clk.Advance(900 * time.Millisecond)
		refreshed := mustGetAttr(t, fs, ino)
		require.Equal(t, int64(2), srv.getAttrCalls.Load())
		require.Equal(t, uint64(9), refreshed.Size)
		require.Equal(t, uint64(1), refreshed.AttrValid)
		require.Equal(t, uint32(500_000_000), refreshed.AttrValidNsec)
	})

	t.Run("AttrTimeoutSeconds <= 0 skips caching RootInode and file attrs on RO", func(t *testing.T) {
		// Given
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 0))
		ino := srv.seedFile(1, "nocache.bin", 0o644, []byte("a"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		// When
		root1 := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
		root2 := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
		f1 := mustGetAttr(t, fs, ino)
		f2 := mustGetAttr(t, fs, ino)

		// Then: zero TTL skips caching, hitting server every time.
		require.Zero(t, root1.AttrValid)
		require.Zero(t, root2.AttrValid)
		require.Zero(t, f1.AttrValid)
		require.Zero(t, f2.AttrValid)
		require.Equal(t, int64(4), srv.getAttrCalls.Load())
	})

	t.Run("Writable staged and sync mounts remain TTL-free even as time advances", func(t *testing.T) {
		for _, mode := range []string{fusefs.MountModeStaged, fusefs.MountModeSync} {
			t.Run(mode, func(t *testing.T) {
				clk := newFakeClock(time.Unix(1_700_000_000, 0))
				srv := newFakeStorage(t, withTTL(1.0, 1.0))
				ino := srv.seedFile(1, "rw.bin", 0o644, []byte("ok"))
				fs := newTestRawFSWithClock(t, srv, clk, withMode(mode))

				_ = mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
				_ = mustGetAttr(t, fs, ino)
				clk.Advance(60 * time.Second)
				rootAfter := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
				fileAfter := mustGetAttr(t, fs, ino)

				require.Equal(t, int64(1), srv.getAttrCalls.Load())
				require.Equal(t, uint64(1), rootAfter.AttrValid)
				require.Equal(t, uint64(1), fileAfter.AttrValid)
			})
		}
	})
}

func TestRO_NamespaceCache_EntryPositiveAndNegativeTTLExpiration(t *testing.T) {
	t.Run("Positive entry expires after its EntryTimeoutSeconds and clamps EntryValid and AttrValid independently", func(t *testing.T) {
		// Given: config EntryTTL=2.0s, AttrTTL=3.0s, but Lookup response returns EntryTimeoutSeconds=0.8s.
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 3.0))
		ino := srv.seedFile(1, "item.txt", 0o644, []byte("v1"))
		customEntryTTL := 0.8
		srv.lookupHook = func(_ context.Context, req *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			if string(req.GetName()) != "item.txt" {
				return nil, nil
			}
			srv.mu.Lock()
			curIno := srv.entries[1]["item.txt"]
			node := srv.nodes[curIno]
			clone := cloneProtoInode(node.inode)
			srv.mu.Unlock()
			return &storagev1.LookupResponse{
				Inode:               clone,
				EntryTimeoutSeconds: customEntryTTL,
				AttrTimeoutSeconds:  3.0,
			}, nil
		}
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		// When: initial Lookup caches 'e' (0.8s) and 'i' (3.0s), then advance 500ms.
		first := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "item.txt")
		require.Equal(t, ino, first.NodeId)
		require.Equal(t, int64(1), srv.lookupCalls.Load())

		clk.Advance(500 * time.Millisecond)
		hit := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "item.txt")
		require.Equal(t, int64(1), srv.lookupCalls.Load())
		require.Zero(t, hit.EntryValid)
		require.Equal(t, uint32(300_000_000), hit.EntryValidNsec)
		require.Equal(t, uint64(2), hit.AttrValid)
		require.Equal(t, uint32(500_000_000), hit.AttrValidNsec)

		// When: writer replaces item.txt with a new file inode and clock reaches 0.8s.
		newIno := srv.seedFile(1, "item.txt", 0o644, []byte("v2"))
		clk.Advance(300 * time.Millisecond)
		refreshed := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "item.txt")

		// Then: expired 'e' lazily evicts at 0.8s (before cfg.EntryTimeoutSeconds=2.0s and AttrTTL=3.0s).
		require.Equal(t, int64(2), srv.lookupCalls.Load())
		require.Equal(t, newIno, refreshed.NodeId)
	})

	t.Run("Positive entry skips caching when EntryTimeoutSeconds <= 0 while GetAttr still caches attr", func(t *testing.T) {
		// Given
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(0, 2.0))
		ino := srv.seedFile(1, "no-entry-cache.txt", 0o644, []byte("a"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		// When
		out1 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "no-entry-cache.txt")
		out2 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "no-entry-cache.txt")
		attr := mustGetAttr(t, fs, ino)

		// Then
		require.Zero(t, out1.EntryValid)
		require.Zero(t, out2.EntryValid)
		require.Equal(t, int64(2), srv.lookupCalls.Load())
		require.Equal(t, ino, attr.Ino)
		require.Zero(t, srv.getAttrCalls.Load())
	})

	t.Run("Negative entry expires after EntryTimeoutSeconds and skips caching when <= 0", func(t *testing.T) {
		// Given: TTL = 1.2s
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(1.2, 2.0))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		var out fuse.EntryOut
		st1 := fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "late.txt", &out)
		requireStatus(t, "initial negative lookup", fuse.ENOENT, st1)
		require.Equal(t, int64(1), srv.lookupCalls.Load())

		// Writer creates late.txt on server, RO checks before and at 1.2s expiry.
		lateIno := srv.seedFile(1, "late.txt", 0o644, []byte("born"))
		clk.Advance(1100 * time.Millisecond)
		st2 := fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "late.txt", &out)
		requireStatus(t, "cached negative lookup before expiry", fuse.ENOENT, st2)
		require.Equal(t, int64(1), srv.lookupCalls.Load())

		clk.Advance(100 * time.Millisecond)
		st3 := fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "late.txt", &out)
		requireStatus(t, "expired negative lookup discovers new file", fuse.OK, st3)
		require.Equal(t, lateIno, out.NodeId)
		require.Equal(t, int64(2), srv.lookupCalls.Load())

		// Sub-case: EntryTimeoutSeconds <= 0 skips negative entry caching.
		srvZero := newFakeStorage(t, withTTL(0, 2.0))
		fsZero := newTestRawFSWithClock(t, srvZero, clk, withMode(fusefs.MountModeRO))
		requireStatus(t, "zero-TTL miss 1", fuse.ENOENT, fsZero.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "z.txt", &out))
		zIno := srvZero.seedFile(1, "z.txt", 0o644, []byte("z"))
		requireStatus(t, "zero-TTL miss 2 sees file", fuse.OK, fsZero.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "z.txt", &out))
		require.Equal(t, zIno, out.NodeId)
		require.Equal(t, int64(2), srvZero.lookupCalls.Load())
	})

	t.Run("Lazy eviction never bumps epoch so concurrent in-flight RPCs never fail with ESTALE", func(t *testing.T) {
		// Given: file A cached at t0, Lookup for file B blocks in flight while file C expires lazily.
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		inoA := srv.seedFile(1, "a.txt", 0o644, []byte("a"))
		inoB := srv.seedFile(1, "b.txt", 0o644, []byte("b"))
		enteredB := make(chan struct{})
		releaseB := make(chan struct{})
		srv.lookupHook = func(_ context.Context, req *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			if string(req.GetName()) == "b.txt" {
				close(enteredB)
				<-releaseB
			}
			return nil, nil
		}
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))
		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "a.txt")

		// When: advance past TTL, start Lookup(b.txt), lazily evict expired a.txt while b.txt RPC is in flight.
		clk.Advance(1500 * time.Millisecond)
		var outB fuse.EntryOut
		errCh := make(chan fuse.Status, 1)
		go func() {
			errCh <- fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "b.txt", &outB)
		}()
		<-enteredB
		_ = mustGetAttr(t, fs, inoA)
		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "a.txt")
		close(releaseB)

		// Then: in-flight Lookup(b.txt) succeeds without spurious ESTALE.
		stB := <-errCh
		require.NotEqual(t, fuse.Status(syscall.ESTALE), stB)
		requireStatus(t, "in-flight lookup after concurrent lazy eviction", fuse.OK, stB)
		require.Equal(t, inoB, outB.NodeId)
	})
}

type decodedDirEntryPlus struct {
	name     string
	entryOut fuse.EntryOut
}

func readDirPlusDecoded(t *testing.T, fs *fusefs.RawFS, dirIno uint64) []decodedDirEntryPlus {
	t.Helper()
	var openOut fuse.OpenOut
	requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dirIno}}, &openOut))
	defer fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirIno}, Fh: openOut.Fh})

	buf := make([]byte, 64<<10)
	list := fuse.NewDirEntryList(buf, 0)
	requireStatus(t, "ReadDirPlus", fuse.OK, fs.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: dirIno},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, list))

	raw := list.Bytes()
	const entryOutSize = int(unsafe.Sizeof(fuse.EntryOut{}))
	const direntSize = 24
	var out []decodedDirEntryPlus
	for pos := 0; pos < len(raw); {
		require.LessOrEqual(t, pos+entryOutSize+direntSize, len(raw))
		eo := *(*fuse.EntryOut)(unsafe.Pointer(&raw[pos]))
		pos += entryOutSize
		nameLen := int(*(*uint32)(unsafe.Pointer(&raw[pos+16])))
		pos += direntSize
		require.LessOrEqual(t, pos+nameLen, len(raw))
		name := string(raw[pos : pos+nameLen])
		pos += nameLen
		padding := (8 - nameLen&7) & 7
		pos += padding
		out = append(out, decodedDirEntryPlus{name: name, entryOut: eo})
	}
	return out
}

func findDecodedEntry(t *testing.T, entries []decodedDirEntryPlus, name string) decodedDirEntryPlus {
	t.Helper()
	for i := range entries {
		if entries[i].name == name {
			return entries[i]
		}
	}
	t.Fatalf("missing directory entry %q", name)
	return decodedDirEntryPlus{}
}

func TestRO_NamespaceCache_DirStreamAndCompleteDirTTLExpiration(t *testing.T) {
	t.Run("Dir stream and completeDirs expire after min(entryTTL, attrTTL) and ReadDirPlus clamps timeouts", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			entryTTL float64
			attrTTL  float64
			wantTTL  time.Duration
		}{
			{name: "entry_smaller", entryTTL: 1.0, attrTTL: 3.0, wantTTL: 1000 * time.Millisecond},
			{name: "attr_smaller", entryTTL: 3.0, attrTTL: 1.0, wantTTL: 1000 * time.Millisecond},
		} {
			t.Run(tc.name, func(t *testing.T) {
				clk := newFakeClock(time.Unix(1_700_000_000, 0))
				srv := newFakeStorage(t, withTTL(tc.entryTTL, tc.attrTTL))
				_ = srv.seedFile(1, "alpha.txt", 0o644, []byte("v1"))
				fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

				// Initial ReadDirPlus caches 'd' and marks completeDirs[1].
				entries1 := readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)
				require.Equal(t, int64(1), srv.readDirPlusCalls.Load())
				_ = findDecodedEntry(t, entries1, "alpha.txt")

				// Writer adds beta.txt; before min(entryTTL, attrTTL), ReadDirPlus & negative Lookup stay cached.
				betaIno := srv.seedFile(1, "beta.txt", 0o644, []byte("v2"))
				clk.Advance(400 * time.Millisecond)

				entries2 := readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)
				require.Equal(t, int64(1), srv.readDirPlusCalls.Load())
				alphaHit := findDecodedEntry(t, entries2, "alpha.txt")
				wantRem := tc.wantTTL - 400*time.Millisecond
				require.Equal(t, uint64(wantRem/time.Second), alphaHit.entryOut.EntryValid)
				require.Equal(t, uint32(wantRem%time.Second), alphaHit.entryOut.EntryValidNsec)
				require.Equal(t, uint64(wantRem/time.Second), alphaHit.entryOut.AttrValid)
				require.Equal(t, uint32(wantRem%time.Second), alphaHit.entryOut.AttrValidNsec)

				var missingOut fuse.EntryOut
				requireStatus(t, "negative hit from cached dir", fuse.ENOENT, fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "beta.txt", &missingOut))
				require.Zero(t, srv.lookupCalls.Load())

				// Advance to exact expiration boundary min(entryTTL, attrTTL).
				clk.Advance(tc.wantTTL - 400*time.Millisecond)

				// Lookup misses expired dir/completeDirs and discovers beta.txt.
				betaOut := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "beta.txt")
				require.Equal(t, betaIno, betaOut.NodeId)
				require.Equal(t, int64(1), srv.lookupCalls.Load())

				// ReadDirPlus misses expired dir stream and fetches updated listing including beta.txt.
				entries3 := readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)
				require.Equal(t, int64(2), srv.readDirPlusCalls.Load())
				_ = findDecodedEntry(t, entries3, "beta.txt")
			})
		}
	})

	t.Run("Skip caching dir stream and completeDirs when entryTTL <= 0 or attrTTL <= 0", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			entryTTL float64
			attrTTL  float64
		}{
			{name: "zero_entry_ttl", entryTTL: 0, attrTTL: 2.0},
			{name: "zero_attr_ttl", entryTTL: 2.0, attrTTL: 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				clk := newFakeClock(time.Unix(1_700_000_000, 0))
				srv := newFakeStorage(t, withTTL(tc.entryTTL, tc.attrTTL))
				_ = srv.seedFile(1, "a.txt", 0o644, []byte("a"))
				fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

				_ = readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)
				bIno := srv.seedFile(1, "b.txt", 0o644, []byte("b"))
				entries2 := readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)

				require.Equal(t, int64(2), srv.readDirPlusCalls.Load())
				_ = findDecodedEntry(t, entries2, "b.txt")

				cIno := srv.seedFile(1, "c.txt", 0o644, []byte("c"))
				gotC := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "c.txt")
				require.Equal(t, cIno, gotC.NodeId)
				require.Equal(t, int64(1), srv.lookupCalls.Load())
				_ = bIno
			})
		}
	})

	t.Run("Embedded child attr from dir stream respects expiration on GetAttr and Lookup", func(t *testing.T) {
		// Given: ReadDirPlus warms 'd' (min(2.0, 1.0) = 1.0s) with embedded child attr for child.txt (no separate 'i' item).
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 1.0))
		childIno := srv.seedFile(1, "child.txt", 0o644, []byte("old"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		_ = readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)
		require.Equal(t, int64(1), srv.readDirPlusCalls.Load())

		// Before expiry, GetAttr hits embedded child attr inside 'd' and clamps AttrValid.
		clk.Advance(300 * time.Millisecond)
		attrHit := mustGetAttr(t, fs, childIno)
		require.Zero(t, srv.getAttrCalls.Load())
		require.Zero(t, attrHit.AttrValid)
		require.Equal(t, uint32(700_000_000), attrHit.AttrValidNsec)

		// Writer mutates child.txt and clock crosses 1.0s expiry; GetAttr must NOT read stale embedded child attr.
		srv.mu.Lock()
		srv.nodes[childIno].data = []byte("updated")
		srv.nodes[childIno].inode.Attr.Size = 7
		srv.mu.Unlock()
		clk.Advance(700 * time.Millisecond)

		attrFresh := mustGetAttr(t, fs, childIno)
		require.Equal(t, int64(1), srv.getAttrCalls.Load())
		require.Equal(t, uint64(7), attrFresh.Size)
	})
}

func TestRO_KernelPageCacheFlags_RetainsKeepCacheWhenNotDirectIO(t *testing.T) {
	t.Run("RO buffered Open sets FOPEN_KEEP_CACHE", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ino := srv.seedFile(1, "keep.bin", 0o644, []byte("hello"))
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeRO), withDirectIO(false))

		opened := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})

		require.NotZero(t, opened.OpenFlags&fuse.FOPEN_KEEP_CACHE)
		require.Zero(t, opened.OpenFlags&fuse.FOPEN_DIRECT_IO)
	})

	t.Run("RO DirectIO Open sets FOPEN_DIRECT_IO and clears FOPEN_KEEP_CACHE", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ino := srv.seedFile(1, "direct.bin", 0o644, []byte("hello"))
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeRO), withDirectIO(true))

		opened := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})

		require.NotZero(t, opened.OpenFlags&fuse.FOPEN_DIRECT_IO)
		require.Zero(t, opened.OpenFlags&fuse.FOPEN_KEEP_CACHE)
	})
}

func mutateServerFile(srv *fakeStorage, ino uint64, data []byte, genDelta uint64, mtimeDelta, ctimeDelta time.Duration) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	node := srv.nodes[ino]
	node.data = append([]byte(nil), data...)
	node.inode.Generation += genDelta
	node.inode.Attr.Size = uint64(len(data))
	node.inode.Attr.Blocks = (uint64(len(data)) + 511) / 512
	if mtimeDelta != 0 {
		mt := node.inode.Attr.Mtime.AsTime().Add(mtimeDelta)
		node.inode.Attr.Mtime = timestamppb.New(mt)
	}
	if ctimeDelta != 0 {
		ct := node.inode.Attr.Ctime.AsTime().Add(ctimeDelta)
		node.inode.Attr.Ctime = timestamppb.New(ct)
	}
}

func TestRO_ReadAhead_StampChangeInvalidatesOpenHandleOnMetadataRPC(t *testing.T) {
	t.Run("Lookup observing changed mtime invalidates open handle read-ahead before windowTTL expires", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(0.2, 10.0))
		v1 := buildRepeatingBytes(1<<20, 'A')
		v2 := buildRepeatingBytes(1<<20, 'B')
		ino := srv.seedFile(1, "big.bin", 0o644, v1)
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		fh := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})
		requireBytesMatch(t, "initial seq read 1", v1[:256<<10], mustReadAt(t, fs, ino, fh.Fh, 0, 256<<10))
		requireBytesMatch(t, "initial seq read 2", v1[256<<10:512<<10], mustReadAt(t, fs, ino, fh.Fh, 256<<10, 256<<10))

		mutateServerFile(srv, ino, v2, 0, time.Second, 0)
		clk.Advance(250 * time.Millisecond)
		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "big.bin")

		got := mustReadAt(t, fs, ino, fh.Fh, 0, 256<<10)
		requireBytesMatch(t, "Lookup stamp change must invalidate read-ahead", v2[:256<<10], got)
	})

	t.Run("GetAttr observing changed ctime nsec invalidates open handle read-ahead before windowTTL expires", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(10.0, 1.0))
		v1 := buildRepeatingBytes(1<<20, 'A')
		v2 := buildRepeatingBytes(1<<20, 'B')
		ino := srv.seedFile(1, "big.bin", 0o644, v1)
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		fh := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})
		clk.Advance(800 * time.Millisecond)
		requireBytesMatch(t, "seq read 1", v1[:256<<10], mustReadAt(t, fs, ino, fh.Fh, 0, 256<<10))
		requireBytesMatch(t, "seq read 2", v1[256<<10:512<<10], mustReadAt(t, fs, ino, fh.Fh, 256<<10, 256<<10))

		mutateServerFile(srv, ino, v2, 0, 0, time.Nanosecond)
		clk.Advance(250 * time.Millisecond)
		_ = mustGetAttr(t, fs, ino)

		got := mustReadAt(t, fs, ino, fh.Fh, 0, 256<<10)
		requireBytesMatch(t, "GetAttr ctime change must invalidate read-ahead", v2[:256<<10], got)
	})

	t.Run("Open observing changed generation invalidates existing open handle read-ahead before windowTTL expires", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(10.0, 1.0))
		v1 := buildRepeatingBytes(1<<20, 'A')
		v2 := buildRepeatingBytes(1<<20, 'B')
		ino := srv.seedFile(1, "big.bin", 0o644, v1)
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		fh1 := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh1.Fh})
		clk.Advance(800 * time.Millisecond)
		requireBytesMatch(t, "seq read 1", v1[:256<<10], mustReadAt(t, fs, ino, fh1.Fh, 0, 256<<10))
		requireBytesMatch(t, "seq read 2", v1[256<<10:512<<10], mustReadAt(t, fs, ino, fh1.Fh, 256<<10, 256<<10))

		mutateServerFile(srv, ino, v2, 1, 0, 0)
		clk.Advance(250 * time.Millisecond)
		fh2 := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh2.Fh})

		got := mustReadAt(t, fs, ino, fh1.Fh, 0, 256<<10)
		requireBytesMatch(t, "Open generation change must invalidate read-ahead", v2[:256<<10], got)
	})

	t.Run("ReadDirPlus observing changed size invalidates open handle read-ahead before windowTTL expires", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(0.2, 10.0))
		v1 := buildRepeatingBytes(1<<20, 'A')
		v2 := buildRepeatingBytes((1<<20)+4096, 'B')
		ino := srv.seedFile(1, "big.bin", 0o644, v1)
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		fh := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})
		requireBytesMatch(t, "seq read 1", v1[:256<<10], mustReadAt(t, fs, ino, fh.Fh, 0, 256<<10))
		requireBytesMatch(t, "seq read 2", v1[256<<10:512<<10], mustReadAt(t, fs, ino, fh.Fh, 256<<10, 256<<10))

		mutateServerFile(srv, ino, v2, 0, 0, 0)
		clk.Advance(250 * time.Millisecond)
		_ = readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)

		got := mustReadAt(t, fs, ino, fh.Fh, 0, 256<<10)
		requireBytesMatch(t, "ReadDirPlus size change must invalidate read-ahead", v2[:256<<10], got)
	})

	t.Run("Unchanged stamp preserves cached read-ahead window across metadata refreshes", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(0.5, 1.0))
		v1 := buildRepeatingBytes(1<<20, 'A')
		ino := srv.seedFile(1, "big.bin", 0o644, v1)
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		fh := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})
		clk.Advance(800 * time.Millisecond)
		requireBytesMatch(t, "seq read 1", v1[:256<<10], mustReadAt(t, fs, ino, fh.Fh, 0, 256<<10))
		requireBytesMatch(t, "seq read 2", v1[256<<10:512<<10], mustReadAt(t, fs, ino, fh.Fh, 256<<10, 256<<10))
		readsBefore := srv.readCalls.Load()

		clk.Advance(250 * time.Millisecond)
		_ = mustGetAttr(t, fs, ino)
		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "big.bin")
		_ = readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)
		fh2 := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh2.Fh})

		got := mustReadAt(t, fs, ino, fh.Fh, 512<<10, 256<<10)
		requireBytesMatch(t, "unchanged stamp keeps cached window", v1[512<<10:768<<10], got)
		require.Equal(t, readsBefore, srv.readCalls.Load())
	})
}

func TestRO_ReadAhead_WindowAndSlotTTLExpiration(t *testing.T) {
	t.Run("Sequential W_read=4 read-ahead stays active on RO within AttrTimeoutSeconds", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		truth := buildSeededBytes(16<<20, 0x33)
		srv := newFakeStorage(t, withTTL(1.0, 2.0))
		ino := srv.seedFile(1, "seq-ro.bin", 0o644, truth)
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		fh := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})

		got := performSequentialReads(t, fs, ino, fh.Fh, 16<<20, 512<<10)
		requireBytesMatch(t, "RO 16 MiB sequential read-ahead content", truth, got)
		require.Less(t, srv.readCalls.Load(), int64(12), "RO sequential read-ahead must keep W_read=4 batching active")
	})

	t.Run("Expired winBuf, winEOF, prevWinBuf, and completed prefetch slots are discarded on open DirectIO handle", func(t *testing.T) {
		// 1. Small/EOF window (winBuf + winEOF) refresh after overwrite + append.
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		v1Small := buildRepeatingBytes(1<<20, 'A')
		inoSmall := srv.seedFile(1, "small-eof.bin", 0o644, v1Small)
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO), withDirectIO(true))

		fhSmall := mustOpenFile(t, fs, inoSmall, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: inoSmall}, Fh: fhSmall.Fh})
		requireBytesMatch(t, "small read 1", v1Small[:512<<10], mustReadAt(t, fs, inoSmall, fhSmall.Fh, 0, 512<<10))
		requireBytesMatch(t, "small read 2", v1Small[512<<10:], mustReadAt(t, fs, inoSmall, fhSmall.Fh, 512<<10, 512<<10))

		v2Appended := append(buildRepeatingBytes(1<<20, 'B'), buildRepeatingBytes(64<<10, 'C')...)
		mutateServerFile(srv, inoSmall, v2Appended, 0, time.Second, time.Second)

		clk.Advance(500 * time.Millisecond)
		requireBytesMatch(t, "before TTL serves cached v1", v1Small[:512<<10], mustReadAt(t, fs, inoSmall, fhSmall.Fh, 0, 512<<10))

		clk.Advance(500 * time.Millisecond)
		requireBytesMatch(t, "after TTL discards winBuf and refreshes v2 head", v2Appended[:512<<10], mustReadAt(t, fs, inoSmall, fhSmall.Fh, 0, 512<<10))
		requireBytesMatch(t, "after TTL resets winEOF and reads appended tail", v2Appended[512<<10:], mustReadAt(t, fs, inoSmall, fhSmall.Fh, 512<<10, (512<<10)+(64<<10)))

		// 2. Multi-window prevWinBuf and prefetched slots expire after AttrTimeoutSeconds.
		v1Large := buildRepeatingBytes(12<<20, 'X')
		v2Large := buildRepeatingBytes(12<<20, 'Y')
		inoLarge := srv.seedFile(1, "large-ra.bin", 0o644, v1Large)

		// Advance sequentially across 4 MiB boundary to populate prevWinBuf=[0..4MiB), winBuf=[4MiB..8MiB), and prefetch slot=[8MiB..12MiB).
		fhSlot := mustOpenFile(t, fs, inoLarge, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: inoLarge}, Fh: fhSlot.Fh})
		for off := 0; off < 8<<20; off += 512 << 10 {
			got := mustReadAt(t, fs, inoLarge, fhSlot.Fh, uint64(off), 512<<10)
			requireBytesMatch(t, "initial large sequential read", v1Large[off:off+(512<<10)], got)
		}
		require.Eventually(t, func() bool {
			return fs.Snapshot().ReadAhead.Inflight == 0
		}, 3*time.Second, 5*time.Millisecond, "background prefetch slots must finish before advancing clock")

		mutateServerFile(srv, inoLarge, v2Large, 0, time.Second, time.Second)
		clk.Advance(1000 * time.Millisecond)

		// Reading into 8 MiB on the same sequential stream must discard expired prefetched slot[8MiB].
		gotSlot := mustReadAt(t, fs, inoLarge, fhSlot.Fh, 8<<20, 512<<10)
		requireBytesMatch(t, "expired prefetch slot must be discarded", v2Large[8<<20:(8<<20)+(512<<10)], gotSlot)

		// Separate handle testing prevWinBuf expiration after crossing 4 MiB boundary.
		srvPrev := newFakeStorage(t, withTTL(1.0, 1.0))
		inoPrev := srvPrev.seedFile(1, "prev-ra.bin", 0o644, v1Large)
		fsPrev := newTestRawFSWithClock(t, srvPrev, clk, withMode(fusefs.MountModeRO), withDirectIO(true))
		fhPrev := mustOpenFile(t, fsPrev, inoPrev, syscall.O_RDONLY)
		defer fsPrev.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: inoPrev}, Fh: fhPrev.Fh})

		for off := 0; off < (4<<20)+(512<<10); off += 512 << 10 {
			_ = mustReadAt(t, fsPrev, inoPrev, fhPrev.Fh, uint64(off), 512<<10)
		}
		require.Eventually(t, func() bool {
			return fsPrev.Snapshot().ReadAhead.Inflight == 0
		}, 3*time.Second, 5*time.Millisecond)

		mutateServerFile(srvPrev, inoPrev, v2Large, 0, time.Second, time.Second)
		clk.Advance(1000 * time.Millisecond)

		gotPrev := mustReadAt(t, fsPrev, inoPrev, fhPrev.Fh, 1<<20, 512<<10)
		requireBytesMatch(t, "expired prevWinBuf must be discarded", v2Large[1<<20:(1<<20)+(512<<10)], gotPrev)
	})

	t.Run("AttrTimeoutSeconds <= 0 skips cross-request window retention and speculative prefetch on RO", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(1.0, 0))
		v1 := buildRepeatingBytes(12<<20, 'M')
		v2 := buildRepeatingBytes(12<<20, 'N')
		ino := srv.seedFile(1, "zero-ra.bin", 0o644, v1)
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		fh := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})

		requireBytesMatch(t, "zero-TTL seq read 1", v1[:512<<10], mustReadAt(t, fs, ino, fh.Fh, 0, 512<<10))
		requireBytesMatch(t, "zero-TTL seq read 2", v1[512<<10:1<<20], mustReadAt(t, fs, ino, fh.Fh, 512<<10, 512<<10))
		require.Zero(t, fs.Snapshot().ReadAhead.Triggers, "AttrTimeoutSeconds <= 0 must not launch speculative prefetch")

		mutateServerFile(srv, ino, v2, 0, time.Second, time.Second)
		requireBytesMatch(t, "zero-TTL re-read 1 reflects v2 immediately", v2[:512<<10], mustReadAt(t, fs, ino, fh.Fh, 0, 512<<10))
		requireBytesMatch(t, "zero-TTL re-read 2 reflects v2 immediately", v2[512<<10:1<<20], mustReadAt(t, fs, ino, fh.Fh, 512<<10, 512<<10))
	})

	t.Run("Writable staged and sync mounts keep read-ahead windows without TTL expiry", func(t *testing.T) {
		for _, mode := range []string{fusefs.MountModeStaged, fusefs.MountModeSync} {
			t.Run(mode, func(t *testing.T) {
				clk := newFakeClock(time.Unix(1_700_000_000, 0))
				srv := newFakeStorage(t, withTTL(1.0, 1.0))
				truth := buildSeededBytes(8<<20, 0x44)
				ino := srv.seedFile(1, "rw-ra.bin", 0o644, truth)
				fs := newTestRawFSWithClock(t, srv, clk, withMode(mode))

				fh := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
				defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})

				_ = mustReadAt(t, fs, ino, fh.Fh, 0, 512<<10)
				_ = mustReadAt(t, fs, ino, fh.Fh, 512<<10, 512<<10)
				_ = mustReadAt(t, fs, ino, fh.Fh, 1<<20, 512<<10)
				require.Eventually(t, func() bool {
					return fs.Snapshot().ReadAhead.Inflight == 0
				}, 3*time.Second, 5*time.Millisecond)
				readsBefore := srv.readCalls.Load()

				clk.Advance(60 * time.Second)
				got := mustReadAt(t, fs, ino, fh.Fh, (1<<20)+(512<<10), 512<<10)
				requireBytesMatch(t, "writable read-ahead remains cached", truth[(1<<20)+(512<<10):(2<<20)], got)
				require.Equal(t, readsBefore, srv.readCalls.Load())
			})
		}
	})
}
