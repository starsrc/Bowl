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

		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		clk.Advance(500 * time.Millisecond)
		hitOut := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)

		require.Zero(t, srv.getAttrCalls.Load())
		require.Equal(t, uint64(1), hitOut.AttrValid)
		require.Equal(t, uint32(500_000_000), hitOut.AttrValidNsec)

		clk.Advance(1500 * time.Millisecond)
		missOut := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)

		require.Equal(t, int64(1), srv.getAttrCalls.Load())
		require.Equal(t, uint64(2), missOut.AttrValid)
		require.Zero(t, missOut.AttrValidNsec)
	})

	t.Run("File inode attr expires lazily on read and reflects updated server metadata", func(t *testing.T) {

		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 1.5))
		ino := srv.seedFile(1, "data.bin", 0o644, []byte("v1"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		first := mustGetAttr(t, fs, ino)
		require.Equal(t, uint64(2), first.Size)
		require.Equal(t, int64(1), srv.getAttrCalls.Load())

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

		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 0))
		ino := srv.seedFile(1, "nocache.bin", 0o644, []byte("a"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		root1 := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
		root2 := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
		f1 := mustGetAttr(t, fs, ino)
		f2 := mustGetAttr(t, fs, ino)

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

		newIno := srv.seedFile(1, "item.txt", 0o644, []byte("v2"))
		clk.Advance(300 * time.Millisecond)
		refreshed := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "item.txt")

		require.Equal(t, int64(2), srv.lookupCalls.Load())
		require.Equal(t, newIno, refreshed.NodeId)
	})

	t.Run("Positive entry skips caching when EntryTimeoutSeconds <= 0 while GetAttr still caches attr", func(t *testing.T) {

		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(0, 2.0))
		ino := srv.seedFile(1, "no-entry-cache.txt", 0o644, []byte("a"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		out1 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "no-entry-cache.txt")
		out2 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "no-entry-cache.txt")
		attr := mustGetAttr(t, fs, ino)

		require.Zero(t, out1.EntryValid)
		require.Zero(t, out2.EntryValid)
		require.Equal(t, int64(2), srv.lookupCalls.Load())
		require.Equal(t, ino, attr.Ino)
		require.Zero(t, srv.getAttrCalls.Load())
	})

	t.Run("Negative entry expires after EntryTimeoutSeconds and skips caching when <= 0", func(t *testing.T) {

		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(1.2, 2.0))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		var out fuse.EntryOut
		st1 := fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "late.txt", &out)
		requireStatus(t, "initial negative lookup", fuse.ENOENT, st1)
		require.Equal(t, int64(1), srv.lookupCalls.Load())

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

		srvZero := newFakeStorage(t, withTTL(0, 2.0))
		fsZero := newTestRawFSWithClock(t, srvZero, clk, withMode(fusefs.MountModeRO))
		requireStatus(t, "zero-TTL miss 1", fuse.ENOENT, fsZero.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "z.txt", &out))
		zIno := srvZero.seedFile(1, "z.txt", 0o644, []byte("z"))
		requireStatus(t, "zero-TTL miss 2 sees file", fuse.OK, fsZero.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "z.txt", &out))
		require.Equal(t, zIno, out.NodeId)
		require.Equal(t, int64(2), srvZero.lookupCalls.Load())
	})

	t.Run("Lazy eviction never bumps epoch so concurrent in-flight RPCs never fail with ESTALE", func(t *testing.T) {

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

				entries1 := readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)
				require.Equal(t, int64(1), srv.readDirPlusCalls.Load())
				_ = findDecodedEntry(t, entries1, "alpha.txt")

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

				clk.Advance(tc.wantTTL - 400*time.Millisecond)

				betaOut := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "beta.txt")
				require.Equal(t, betaIno, betaOut.NodeId)
				require.Equal(t, int64(1), srv.lookupCalls.Load())

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

		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 1.0))
		childIno := srv.seedFile(1, "child.txt", 0o644, []byte("old"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		_ = readDirPlusDecoded(t, fs, fuse.FUSE_ROOT_ID)
		require.Equal(t, int64(1), srv.readDirPlusCalls.Load())

		clk.Advance(300 * time.Millisecond)
		attrHit := mustGetAttr(t, fs, childIno)
		require.Zero(t, srv.getAttrCalls.Load())
		require.Zero(t, attrHit.AttrValid)
		require.Equal(t, uint32(700_000_000), attrHit.AttrValidNsec)

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

		v1Large := buildRepeatingBytes(12<<20, 'X')
		v2Large := buildRepeatingBytes(12<<20, 'Y')
		inoLarge := srv.seedFile(1, "large-ra.bin", 0o644, v1Large)

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

		gotSlot := mustReadAt(t, fs, inoLarge, fhSlot.Fh, 8<<20, 512<<10)
		requireBytesMatch(t, "expired prefetch slot must be discarded", v2Large[8<<20:(8<<20)+(512<<10)], gotSlot)

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

func readDirPlusAtOffset(t *testing.T, fs *fusefs.RawFS, dirIno, offset uint64) []decodedDirEntryPlus {
	t.Helper()
	var openOut fuse.OpenOut
	requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dirIno}}, &openOut))
	defer fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirIno}, Fh: openOut.Fh})

	buf := make([]byte, 64<<10)
	list := fuse.NewDirEntryList(buf, 0)
	requireStatus(t, "ReadDirPlus", fuse.OK, fs.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: dirIno},
		Fh:       openOut.Fh,
		Offset:   offset,
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

func TestRO_NamespaceCache_ReadDirPlusRefreshesExistingInodeAndConcurrentMisses(t *testing.T) {
	t.Run("ReadDirPlus updates existing cached inode attrs and avoids inserting uncached siblings", func(t *testing.T) {

		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(0.2, 5.0))
		trackedIno := srv.seedFile(1, "tracked.txt", 0o644, []byte("v1"))
		uncachedIno := srv.seedFile(1, "uncached.txt", 0o644, []byte("sib"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		initialLookup := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "tracked.txt")
		require.Equal(t, uint64(2), initialLookup.Size)

		mutateServerFile(srv, trackedIno, []byte("v2-updated"), 1, time.Second, time.Second)
		clk.Advance(300 * time.Millisecond)

		entries := readDirPlusAtOffset(t, fs, fuse.FUSE_ROOT_ID, 1)
		trackedEntry := findDecodedEntry(t, entries, "tracked.txt")
		require.Equal(t, uint64(10), trackedEntry.entryOut.Size)

		getAttrBefore := srv.getAttrCalls.Load()
		gotAttr := mustGetAttr(t, fs, trackedIno)
		require.Equal(t, getAttrBefore, srv.getAttrCalls.Load(), "updated existing 'i' entry must serve v2 without GetAttr RPC")
		require.Equal(t, uint64(10), gotAttr.Size)

		_ = mustGetAttr(t, fs, uncachedIno)
		require.Equal(t, getAttrBefore+1, srv.getAttrCalls.Load(), "uncached sibling must still miss to server GetAttr")
	})

	t.Run("ReadDirPlus preserves entryTimeout when entry EntryTimeoutSeconds <= 0 and evicts cached inode when AttrTimeoutSeconds <= 0", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 5.0))
		inoA := srv.seedFile(1, "keep-entry-ttl.txt", 0o644, []byte("v1"))
		inoB := srv.seedFile(1, "zero-attr-ttl.txt", 0o644, []byte("b1"))
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "keep-entry-ttl.txt")
		_ = mustGetAttr(t, fs, inoB)

		mutateServerFile(srv, inoA, []byte("v2-longer"), 1, time.Second, time.Second)
		srv.readDirPlusHook = func(_ context.Context, _ *storagev1.ReadDirPlusRequest) ([]*storagev1.DirEntryPlus, error) {
			srv.mu.Lock()
			nodeA := cloneProtoInode(srv.nodes[inoA].inode)
			nodeB := cloneProtoInode(srv.nodes[inoB].inode)
			srv.mu.Unlock()
			return []*storagev1.DirEntryPlus{
				{Ino: inoA, Name: []byte("keep-entry-ttl.txt"), NextOffset: 3, Inode: nodeA, EntryTimeoutSeconds: 0, AttrTimeoutSeconds: 5.0},
				{Ino: inoB, Name: []byte("zero-attr-ttl.txt"), NextOffset: 4, Inode: nodeB, EntryTimeoutSeconds: 2.0, AttrTimeoutSeconds: 0},
			}, nil
		}

		_ = readDirPlusAtOffset(t, fs, fuse.FUSE_ROOT_ID, 2)

		lookupA := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "keep-entry-ttl.txt")
		require.Equal(t, uint64(9), lookupA.Size)
		require.Equal(t, int64(1), srv.lookupCalls.Load())
		require.Positive(t, lookupA.EntryValid)

		beforeB := srv.getAttrCalls.Load()
		_ = mustGetAttr(t, fs, inoB)
		require.Equal(t, beforeB+1, srv.getAttrCalls.Load())
	})

	t.Run("Concurrent GetAttr miss does not increment mutations or drop concurrent Lookup backfill on RO", func(t *testing.T) {
		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		_ = srv.seedFile(1, "foo.bin", 0o644, []byte("foo"))
		barIno := srv.seedFile(1, "bar.bin", 0o644, []byte("bar"))

		lookupEntered := make(chan struct{})
		releaseLookup := make(chan struct{})
		srv.lookupHook = func(_ context.Context, req *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			if string(req.GetName()) == "foo.bin" {
				close(lookupEntered)
				<-releaseLookup
			}
			return nil, nil
		}
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		var fooOut fuse.EntryOut
		done := make(chan fuse.Status, 1)
		go func() {
			done <- fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "foo.bin", &fooOut)
		}()
		<-lookupEntered

		_ = mustGetAttr(t, fs, barIno)
		close(releaseLookup)
		requireStatus(t, "concurrent Lookup(foo.bin)", fuse.OK, <-done)

		lookup2 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "foo.bin")
		require.Equal(t, uint64(3), lookup2.Size)
		require.Equal(t, int64(1), srv.lookupCalls.Load(), "second Lookup(foo.bin) must hit cache backfilled by first Lookup")
	})
}

func TestRO_NamespaceCache_ReadlinkClockAndRWROExpiration(t *testing.T) {
	t.Run("GivenROMountWithInjectedClock_WhenClockReachesAttrTTL_ThenReadlinkExpiresAndRefreshes", func(t *testing.T) {

		clk := newFakeClock(time.Unix(1_700_000_000, 0))
		srv := newFakeStorage(t, withTTL(2.0, 1.0))
		symIno := srv.seedSymlink(1, "link-ro", "/target/v1")
		fs := newTestRawFSWithClock(t, srv, clk, withMode(fusefs.MountModeRO))

		got1, st1 := fs.Readlink(nil, &fuse.InHeader{NodeId: symIno})
		requireStatus(t, "initial RO Readlink", fuse.OK, st1)
		require.Equal(t, "/target/v1", string(got1))
		require.Equal(t, int64(1), srv.readlinkCalls.Load())

		srv.mu.Lock()
		srv.nodes[symIno].link = []byte("/target/v2")
		srv.mu.Unlock()
		clk.Advance(600 * time.Millisecond)

		got2, st2 := fs.Readlink(nil, &fuse.InHeader{NodeId: symIno})
		requireStatus(t, "cached RO Readlink before expiry", fuse.OK, st2)
		require.Equal(t, "/target/v1", string(got2))
		require.Equal(t, int64(1), srv.readlinkCalls.Load())

		clk.Advance(400 * time.Millisecond)
		got3, st3 := fs.Readlink(nil, &fuse.InHeader{NodeId: symIno})

		requireStatus(t, "expired RO Readlink refresh", fuse.OK, st3)
		require.Equal(t, "/target/v2", string(got3))
		require.Equal(t, int64(2), srv.readlinkCalls.Load())
	})

	t.Run("GivenWritableMount_WhenPositiveTinyAttrTTLConfigured_ThenReadlinkRemainsCachedWithoutTTLExpiryUntilUnlink", func(t *testing.T) {
		for _, mode := range []string{fusefs.MountModeStaged, fusefs.MountModeSync} {
			t.Run(mode, func(t *testing.T) {

				clk := newFakeClock(time.Unix(1_700_000_000, 0))
				srv := newFakeStorage(t, withTTL(1.0, 1e-9))
				fs := newTestRawFSWithClock(t, srv, clk, withMode(mode))
				var symOut fuse.EntryOut
				requireStatus(t, "Symlink", fuse.OK, fs.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "/target/rw", "rw-link", &symOut))

				clk.Advance(60 * time.Second)
				got1, st1 := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})
				got2, st2 := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})

				requireStatus(t, "writable Readlink#1", fuse.OK, st1)
				requireStatus(t, "writable Readlink#2", fuse.OK, st2)
				require.Equal(t, "/target/rw", string(got1))
				require.Equal(t, "/target/rw", string(got2))
				require.Zero(t, srv.readlinkCalls.Load(), "writable mount readlink must not expire by TTL")

				requireStatus(t, "Unlink symlink", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "rw-link"))
				_, stAfter := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})
				requireStatus(t, "Readlink after Unlink", fuse.ENOENT, stAfter)
				require.Equal(t, int64(1), srv.readlinkCalls.Load())
			})
		}
	})

	t.Run("GivenROAndWritableMountsWithZeroAttrTTL_WhenReadlinkRuns_ThenCacheIsBypassed", func(t *testing.T) {
		for _, mode := range []string{fusefs.MountModeRO, fusefs.MountModeStaged} {
			t.Run(mode, func(t *testing.T) {
				clk := newFakeClock(time.Unix(1_700_000_000, 0))
				srv := newFakeStorage(t, withTTL(2.0, 0))
				symIno := srv.seedSymlink(1, "zero-link", "/target/zero")
				fs := newTestRawFSWithClock(t, srv, clk, withMode(mode))

				got1, st1 := fs.Readlink(nil, &fuse.InHeader{NodeId: symIno})
				got2, st2 := fs.Readlink(nil, &fuse.InHeader{NodeId: symIno})

				requireStatus(t, "zero-TTL Readlink#1", fuse.OK, st1)
				requireStatus(t, "zero-TTL Readlink#2", fuse.OK, st2)
				require.Equal(t, "/target/zero", string(got1))
				require.Equal(t, "/target/zero", string(got2))
				require.Equal(t, int64(2), srv.readlinkCalls.Load())
				require.Zero(t, fs.Snapshot().NamespaceCache.ReadlinkHits)
			})
		}
	})
}
