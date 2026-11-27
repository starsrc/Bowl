package fusefs

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type slice1StubClient struct {
	storagev1connect.StorageServiceClient

	mu             sync.Mutex
	inodes         map[uint64]*storagev1.Inode
	files          map[uint64][]byte
	names          map[string]uint64
	nextIno        uint64
	writeHook      func(context.Context, *storagev1.WriteRequest) (*storagev1.Inode, error)
	getAttrHook    func(context.Context, *storagev1.GetAttrRequest) (*storagev1.Inode, error)
	setAttrHook    func(context.Context, *storagev1.SetAttrRequest) error
	fallocateHook  func(context.Context, *storagev1.FallocateRequest) error
	readCalls      atomic.Int64
	writeCalls     atomic.Int64
	getAttrCalls   atomic.Int64
	setAttrCalls   atomic.Int64
	fallocateCalls atomic.Int64
}

func newSlice1StubClient() *slice1StubClient {
	now := timestamppb.Now()
	root := &storagev1.Inode{
		Ino:        1,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFDIR | 0o755,
			Nlink: 2,
			Atime: now,
			Mtime: now,
			Ctime: now,
		},
	}
	return &slice1StubClient{
		inodes:  map[uint64]*storagev1.Inode{1: root},
		files:   make(map[uint64][]byte),
		names:   make(map[string]uint64),
		nextIno: 100,
	}
}

func (c *slice1StubClient) Create(_ context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextIno++
	ino := c.nextIno
	now := timestamppb.Now()
	node := &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFREG | (req.Msg.GetMode() & 0o7777),
			Nlink: 1,
			Uid:   req.Msg.GetUid(),
			Gid:   req.Msg.GetGid(),
			Size:  0,
			Atime: now,
			Mtime: now,
			Ctime: now,
		},
	}
	c.inodes[ino] = node
	c.files[ino] = nil
	c.names[string(req.Msg.GetName())] = ino
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               node,
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (c *slice1StubClient) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	c.getAttrCalls.Add(1)
	if c.getAttrHook != nil {
		override, err := c.getAttrHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		if override != nil {
			return connect.NewResponse(&storagev1.GetAttrResponse{
				Inode:              override,
				AttrTimeoutSeconds: 1.0,
			}), nil
		}
	}
	c.mu.Lock()
	node, ok := c.inodes[req.Msg.GetIno()]
	c.mu.Unlock()
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              node,
		AttrTimeoutSeconds: 1.0,
	}), nil
}

func (c *slice1StubClient) Lookup(_ context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	c.mu.Lock()
	ino, ok := c.names[string(req.Msg.GetName())]
	var node *storagev1.Inode
	if ok {
		node = c.inodes[ino]
	}
	c.mu.Unlock()
	if !ok || node == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode:               node,
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (c *slice1StubClient) ReadDirPlus(_ context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	if req.Msg.GetOffset() > 0 {
		return connect.NewResponse(&storagev1.ReadDirPlusResponse{}), nil
	}
	c.mu.Lock()
	entries := make([]*storagev1.DirEntryPlus, 0, len(c.names))
	var next uint64 = 1
	for name, ino := range c.names {
		node := c.inodes[ino]
		entries = append(entries, &storagev1.DirEntryPlus{
			Ino:                 ino,
			Name:                []byte(name),
			NextOffset:          next,
			Inode:               node,
			EntryTimeoutSeconds: 1.0,
			AttrTimeoutSeconds:  1.0,
		})
		next++
	}
	c.mu.Unlock()
	return connect.NewResponse(&storagev1.ReadDirPlusResponse{Entries: entries}), nil
}

func (c *slice1StubClient) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	c.writeCalls.Add(1)
	if c.writeHook != nil {
		override, err := c.writeHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		if override != nil {
			return connect.NewResponse(&storagev1.WriteResponse{
				Size:  uint32(len(req.Msg.GetData())),
				Inode: override,
			}), nil
		}
	}
	c.mu.Lock()
	cur := c.files[req.Msg.GetIno()]
	off := int(req.Msg.GetOffset())
	payload := req.Msg.GetData()
	end := off + len(payload)
	if end > len(cur) {
		grown := make([]byte, end)
		copy(grown, cur)
		cur = grown
	}
	copy(cur[off:end], payload)
	c.files[req.Msg.GetIno()] = cur
	size := uint64(len(cur))
	now := timestamppb.Now()
	node := &storagev1.Inode{
		Ino:        req.Msg.GetIno(),
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFREG | 0o644,
			Nlink: 1,
			Size:  size,
			Atime: now,
			Mtime: now,
			Ctime: now,
		},
	}
	c.inodes[req.Msg.GetIno()] = node
	c.mu.Unlock()
	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  uint32(len(payload)),
		Inode: node,
	}), nil
}

func (c *slice1StubClient) Read(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	c.readCalls.Add(1)
	c.mu.Lock()
	data := c.files[req.Msg.GetIno()]
	off := int(req.Msg.GetOffset())
	size := int(req.Msg.GetSize())
	var out []byte
	if off < len(data) {
		end := min(len(data), off+size)
		out = append([]byte(nil), data[off:end]...)
	}
	c.mu.Unlock()
	return connect.NewResponse(&storagev1.ReadResponse{Data: out}), nil
}

func (c *slice1StubClient) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	c.setAttrCalls.Add(1)
	if c.setAttrHook != nil {
		if err := c.setAttrHook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	cur := c.files[req.Msg.GetIno()]
	if req.Msg.Size != nil {
		target := int(*req.Msg.Size)
		if target < len(cur) {
			cur = cur[:target]
		} else if target > len(cur) {
			grown := make([]byte, target)
			copy(grown, cur)
			cur = grown
		}
		c.files[req.Msg.GetIno()] = cur
	}
	size := uint64(len(cur))
	now := timestamppb.Now()
	node := &storagev1.Inode{
		Ino:        req.Msg.GetIno(),
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFREG | 0o644,
			Nlink: 1,
			Size:  size,
			Atime: now,
			Mtime: now,
			Ctime: now,
		},
	}
	c.inodes[req.Msg.GetIno()] = node
	c.mu.Unlock()
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              node,
		AttrTimeoutSeconds: 1.0,
	}), nil
}

func (c *slice1StubClient) Fallocate(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	c.fallocateCalls.Add(1)
	if c.fallocateHook != nil {
		if err := c.fallocateHook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	cur := c.files[req.Msg.GetIno()]
	end := int(req.Msg.GetOffset() + req.Msg.GetLength())
	if req.Msg.GetMode() == 0 && end > len(cur) {
		grown := make([]byte, end)
		copy(grown, cur)
		c.files[req.Msg.GetIno()] = grown
		if node := c.inodes[req.Msg.GetIno()]; node != nil && node.Attr != nil {
			node.Attr.Size = uint64(len(grown))
		}
	}
	c.mu.Unlock()
	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

func (c *slice1StubClient) Fsync(_ context.Context, _ *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (c *slice1StubClient) FsyncDir(_ context.Context, _ *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

func newSlice1RawFS(t *testing.T, client storagev1connect.StorageServiceClient) *RawFS {
	t.Helper()
	fs := NewRawFS(client, Config{
		DiskID:              "disk-slice1",
		Epoch:               1,
		RootIno:             1,
		Mode:                MountModeStaged,
		StageDir:            t.TempDir(),
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	})
	t.Cleanup(fs.OnUnmount)
	return fs
}

func TestStep1_OutOfOrder4MiBOnCommit_KeepsMonotonicServerSizeAndRespectsAuthoritativeMutations(t *testing.T) {
	client := newSlice1StubClient()
	const chunkLen = StageChunkSize

	var inflight atomic.Int32
	bothInflight := make(chan struct{})
	var bothOnce sync.Once
	releaseChunk0 := make(chan struct{})
	chunk1Returned := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseChunk0:
		default:
			close(releaseChunk0)
		}
	})

	client.writeHook = func(_ context.Context, req *storagev1.WriteRequest) (*storagev1.Inode, error) {
		if inflight.Add(1) == 2 {
			bothOnce.Do(func() { close(bothInflight) })
		}
		select {
		case <-bothInflight:
		case <-time.After(2 * time.Second):
			return nil, errors.New("parallel 4MiB chunks did not overlap")
		}
		now := timestamppb.Now()
		if req.GetOffset() == 0 {
			<-releaseChunk0
			return &storagev1.Inode{
				Ino:        req.GetIno(),
				Generation: 1,
				Attr: &storagev1.InodeAttribute{
					Mode:  syscall.S_IFREG | 0o644,
					Nlink: 1,
					Size:  chunkLen,
					Atime: now,
					Mtime: now,
					Ctime: now,
				},
			}, nil
		}
		defer close(chunk1Returned)
		return &storagev1.Inode{
			Ino:        req.GetIno(),
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:  syscall.S_IFREG | 0o644,
				Nlink: 1,
				Size:  2 * chunkLen,
				Atime: now,
				Mtime: now,
				Ctime: now,
			},
		}, nil
	}

	fs := newSlice1RawFS(t, client)
	var created fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "parallel-4m.bin", &created))

	chunk0 := bytes.Repeat([]byte{'A'}, chunkLen)
	chunk1 := bytes.Repeat([]byte{'B'}, chunkLen)

	_, st0 := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
		Size:     chunkLen,
	}, chunk0)
	require.Equal(t, fuse.OK, st0)

	_, st1 := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   chunkLen,
		Size:     chunkLen,
	}, chunk1)
	require.Equal(t, fuse.OK, st1)

	select {
	case <-chunk1Returned:
	case <-time.After(2 * time.Second):
		t.Fatal("chunk 1 did not finish upload")
	}
	require.Eventually(t, func() bool {
		info, ok := fs.lookupKnownInode(created.NodeId)
		return ok && info.hasServerSize && info.serverSize == 2*chunkLen
	}, 2*time.Second, 2*time.Millisecond)

	close(releaseChunk0)
	require.NoError(t, fs.stage.BarrierInode(context.Background(), created.NodeId))

	infoAfterCommit, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.True(t, infoAfterCommit.hasServerSize)
	require.Equal(t, uint64(2*chunkLen), infoAfterCommit.serverSize, "out-of-order OnCommit with smaller respSize must not regress knownInodes.serverSize")

	staleEpoch := infoAfterCommit.metaEpoch - 1
	fs.recordKnownInode(created.NodeId, &storagev1.Inode{
		Ino: created.NodeId,
		Attr: &storagev1.InodeAttribute{
			Mode: syscall.S_IFREG | 0o644,
			Size: 123,
		},
	}, staleEpoch)
	infoAfterStaleRead, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.Equal(t, uint64(2*chunkLen), infoAfterStaleRead.serverSize, "stale read-path recordKnownInode must not overwrite newer metaEpoch")

	var setOut fuse.AttrOut
	require.Equal(t, fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Valid:    fuse.FATTR_SIZE,
			Size:     2 << 20,
		},
	}, &setOut))
	infoAfterTrunc, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.True(t, infoAfterTrunc.hasServerSize)
	require.Equal(t, uint64(2<<20), infoAfterTrunc.serverSize, "serialized SetAttr must overwrite serverSize downward")
	require.Greater(t, infoAfterTrunc.metaEpoch, infoAfterCommit.metaEpoch)

	var openTruncOut fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Flags:    uint32(syscall.O_RDWR | syscall.O_TRUNC),
	}, &openTruncOut))
	infoAfterOpenTrunc, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.True(t, infoAfterOpenTrunc.hasServerSize)
	require.Equal(t, uint64(0), infoAfterOpenTrunc.serverSize, "serialized Open(O_TRUNC) must overwrite serverSize downward to 0")
	require.Greater(t, infoAfterOpenTrunc.metaEpoch, infoAfterTrunc.metaEpoch)

	require.Equal(t, fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
		Length:   8192,
		Mode:     0,
	}))
	infoAfterFalloc, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.False(t, infoAfterFalloc.hasServerSize, "Fallocate must clear hasServerSize=false")
	require.Greater(t, infoAfterFalloc.metaEpoch, infoAfterOpenTrunc.metaEpoch, "Fallocate must bump metaEpoch")

	fs.recordKnownInode(created.NodeId, &storagev1.Inode{
		Ino: created.NodeId,
		Attr: &storagev1.InodeAttribute{
			Mode: syscall.S_IFREG | 0o644,
			Size: 0,
		},
	}, infoAfterOpenTrunc.metaEpoch)
	infoStillCleared, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.False(t, infoStillCleared.hasServerSize, "stale read-path recordKnownInode before Fallocate must not restore hasServerSize")

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: openTruncOut.Fh})
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
}

func TestStep2_OnCommitAfterForget_ClosedFileSkipsKnownInodesAndCapsLeak(t *testing.T) {
	client := newSlice1StubClient()
	uploadEntered := make(chan struct{})
	uploadRelease := make(chan struct{})
	var enterOnce sync.Once
	client.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (*storagev1.Inode, error) {
		enterOnce.Do(func() { close(uploadEntered) })
		<-uploadRelease
		return nil, nil
	}
	t.Cleanup(func() {
		select {
		case <-uploadRelease:
		default:
			close(uploadRelease)
		}
	})

	fs := newSlice1RawFS(t, client)
	var created fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "forgotten.bin", &created))

	_, wrSt := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
		Size:     10,
	}, []byte("0123456789"))
	require.Equal(t, fuse.OK, wrSt)

	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	})

	select {
	case <-uploadEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("staged upload did not start after Release")
	}

	fs.Forget(created.NodeId, 1)
	_, knownBefore := fs.lookupKnownInode(created.NodeId)
	require.False(t, knownBefore)
	require.False(t, fs.caps.knownAbsent(created.NodeId))

	g0 := fs.metaClock.Load()
	metaDirtyBefore := fs.metaDirtySeq.Load()
	close(uploadRelease)
	require.NoError(t, fs.stage.BarrierInode(context.Background(), created.NodeId))

	_, knownAfter := fs.lookupKnownInode(created.NodeId)
	require.False(t, knownAfter, "OnCommit after Forget on closed file must not re-insert into knownInodes")
	require.False(t, fs.caps.knownAbsent(created.NodeId), "OnCommit after Forget on closed file must not re-insert into caps")
	require.True(t, fs.inodeMutatedSince(created.NodeId, g0), "OnCommit after Forget must still run markServerMutated")
	require.Greater(t, fs.metaDirtySeq.Load(), metaDirtyBefore, "OnCommit after Forget must still mark metadata dirty")
}

func TestStep2_OnCommitAfterForget_OpenOrphanUpdatesKnownInodesAndCaps(t *testing.T) {
	client := newSlice1StubClient()
	uploadEntered := make(chan struct{})
	uploadRelease := make(chan struct{})
	var enterOnce sync.Once
	client.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (*storagev1.Inode, error) {
		enterOnce.Do(func() { close(uploadEntered) })
		<-uploadRelease
		return nil, nil
	}
	t.Cleanup(func() {
		select {
		case <-uploadRelease:
		default:
			close(uploadRelease)
		}
	})

	fs := newSlice1RawFS(t, client)
	var created fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "orphan-open.bin", &created))

	_, wrSt := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
		Size:     8,
	}, []byte("abcdefgh"))
	require.Equal(t, fuse.OK, wrSt)
	fs.stage.SealInode(created.NodeId)

	select {
	case <-uploadEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("staged upload did not start")
	}

	fs.Forget(created.NodeId, 1)
	_, knownBefore := fs.lookupKnownInode(created.NodeId)
	require.False(t, knownBefore)

	g0 := fs.metaClock.Load()
	close(uploadRelease)
	require.NoError(t, fs.stage.BarrierInode(context.Background(), created.NodeId))

	infoAfter, knownAfter := fs.lookupKnownInode(created.NodeId)
	require.True(t, knownAfter, "OnCommit on open unlinked orphan must keep knownInodes updated")
	require.True(t, infoAfter.hasServerSize)
	require.Equal(t, uint64(8), infoAfter.serverSize)
	require.True(t, fs.caps.knownAbsent(created.NodeId), "OnCommit on open unlinked orphan must keep caps marked absent")
	require.True(t, fs.inodeMutatedSince(created.NodeId, g0), "OnCommit on open unlinked orphan must run markServerMutated")

	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	})
}

func TestStep3_FailedLargeChunkAndPendingSmallChunk_DynamicDirtyEndAndIdlePruning(t *testing.T) {
	client := newSlice1StubClient()
	largeFailed := make(chan struct{})
	var failOnce sync.Once
	client.writeHook = func(_ context.Context, req *storagev1.WriteRequest) (*storagev1.Inode, error) {
		if req.GetOffset() == 1<<20 {
			failOnce.Do(func() { close(largeFailed) })
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("no space left on device"))
		}
		return nil, nil
	}

	fs := newSlice1RawFS(t, client)
	var created fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "dynamic-dirty-end.bin", &created))

	_, st1 := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   1 << 20,
		Size:     4096,
	}, make([]byte, 4096))
	require.Equal(t, fuse.OK, st1)

	_, st2 := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
		Size:     128,
	}, make([]byte, 128))
	require.Equal(t, fuse.OK, st2)

	select {
	case <-largeFailed:
	case <-time.After(2 * time.Second):
		t.Fatal("large chunk upload did not fail as expected")
	}

	require.Eventually(t, func() bool {
		fs.stage.mu.Lock()
		st := fs.stage.inodes[created.NodeId]
		done := st != nil && len(st.pending) == 0 && st.active != nil
		fs.stage.mu.Unlock()
		return done
	}, 2*time.Second, 5*time.Millisecond)

	var attrOut fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
	}, &attrOut))
	require.Equal(t, uint64(128), attrOut.Size, "dirty end must reflect live pending small chunk (128), not failed large chunk (1MiB+4096)")

	require.NoError(t, fs.stage.BarrierInode(context.Background(), created.NodeId))
	fs.stage.mu.Lock()
	erroredSt := fs.stage.inodes[created.NodeId]
	var lastWriteAfterIdle int64
	if erroredSt != nil {
		lastWriteAfterIdle = erroredSt.lastWriteUnixNano
	}
	fs.stage.mu.Unlock()
	require.NotNil(t, erroredSt, "errored stageInode with errSeq!=0 must stay until handle consumes or DiscardInode")
	require.Zero(t, lastWriteAfterIdle, "lastWriteUnixNano must reset to 0 when stageInode transitions to idle")

	var clean fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "idle-prune.bin", &clean))
	_, cleanSt := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: clean.NodeId},
		Fh:       clean.Fh,
		Offset:   0,
		Size:     32,
	}, make([]byte, 32))
	require.Equal(t, fuse.OK, cleanSt)
	require.NoError(t, fs.stage.BarrierInode(context.Background(), clean.NodeId))

	fs.stage.mu.Lock()
	_, stillPresent := fs.stage.inodes[clean.NodeId]
	fs.stage.mu.Unlock()
	require.False(t, stillPresent, "idle stageInode with no active/pending chunks and errSeq==0 must be pruned from c.inodes")

	fs.stage.writeChunkPiece = func(string, uint64, uint64, []byte) error {
		return syscall.EIO
	}
	var rollbackIno uint64 = 999
	_, _, rbErr := fs.stage.WriteForHandle(context.Background(), nil, 1, rollbackIno, rollbackIno, 0, []byte("fail"))
	fs.stage.writeChunkPiece = nil
	require.ErrorIs(t, rbErr, syscall.EIO)
	fs.stage.mu.Lock()
	_, rbPresent := fs.stage.inodes[rollbackIno]
	fs.stage.mu.Unlock()
	require.False(t, rbPresent, "write rollback leaving an empty idle stageInode must prune it from c.inodes")
}

func TestStep4_GetAttrLookupReadDirPlus_LeavesServerInodeAndKnownServerSizeUnpolluted(t *testing.T) {
	client := newSlice1StubClient()
	fs := newSlice1RawFS(t, client)

	var created fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "unpolluted.bin", &created))

	client.mu.Lock()
	serverInodePtr := client.inodes[created.NodeId]
	client.mu.Unlock()
	require.Equal(t, uint64(0), serverInodePtr.GetAttr().GetSize())

	_, wrSt := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
		Size:     17,
	}, []byte("0123456789abcdefg"))
	require.Equal(t, fuse.OK, wrSt)

	var attrOut fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
	}, &attrOut))
	require.Equal(t, uint64(17), attrOut.Size)
	require.Equal(t, uint64(1), attrOut.Blocks)
	require.Equal(t, uint64(0), serverInodePtr.GetAttr().GetSize(), "GetAttr must not mutate server *storagev1.Inode size")
	infoAfterGetAttr, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.True(t, infoAfterGetAttr.hasServerSize)
	require.Equal(t, uint64(0), infoAfterGetAttr.serverSize, "GetAttr must not pollute knownInodes.serverSize with uncommitted dirty size")

	var entryOut fuse.EntryOut
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "unpolluted.bin", &entryOut))
	require.Equal(t, uint64(17), entryOut.Size)
	require.Equal(t, uint64(0), serverInodePtr.GetAttr().GetSize(), "Lookup must not mutate server *storagev1.Inode size")
	infoAfterLookup, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.Equal(t, uint64(0), infoAfterLookup.serverSize, "Lookup must not pollute knownInodes.serverSize with uncommitted dirty size")

	var dirOut fuse.OpenOut
	require.Equal(t, fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDONLY),
	}, &dirOut))
	dirBuf := make([]byte, 8192)
	dirList := fuse.NewDirEntryList(dirBuf, 0)
	require.Equal(t, fuse.OK, fs.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Fh:       dirOut.Fh,
		Offset:   0,
		Size:     uint32(len(dirBuf)),
	}, dirList))
	require.Equal(t, uint64(0), serverInodePtr.GetAttr().GetSize(), "ReadDirPlus must not mutate server *storagev1.Inode size")
	infoAfterRDP, ok := fs.lookupKnownInode(created.NodeId)
	require.True(t, ok)
	require.Equal(t, uint64(0), infoAfterRDP.serverSize, "ReadDirPlus must not pollute knownInodes.serverSize with uncommitted dirty size")
	fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOut.Fh})

	directAttr := fuse.Attr{Size: 0, Blocks: 0, Mtime: 0, Ctime: 0}
	require.True(t, fs.stage.OverlayDirty(created.NodeId, &directAttr))
	require.Equal(t, uint64(17), directAttr.Size)
	require.Equal(t, uint64(1), directAttr.Blocks)
	require.Positive(t, directAttr.Mtime, "OverlayDirty must overlay Mtime from lastWriteUnixNano")
	require.Positive(t, directAttr.Ctime, "OverlayDirty must overlay Ctime from lastWriteUnixNano")

	fs.stage.AbortAll(syscall.EBUSY)
	abortedAttr := fuse.Attr{Size: 5, Blocks: 1}
	require.False(t, fs.stage.OverlayDirty(created.NodeId, &abortedAttr), "OverlayDirty after AbortAll must return false")
	require.Equal(t, uint64(5), abortedAttr.Size, "OverlayDirty after AbortAll must not overlay aborted dirty size")
}

func TestStageCoordinator_Read_MutatedFnDropsBaseSizeAndPreventsZeroHole(t *testing.T) {
	stageDir := t.TempDir()
	coord := NewStageCoordinator(stageDir, DefaultStageSize, func(_ context.Context, _ uint64, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		return uint32(len(data)), nil, nil
	}, StageHooks{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	const ino uint64 = 707
	_, handledWrite, writeErr := coord.WriteForHandle(context.Background(), nil, 1, ino, ino, 12, []byte("world"))
	require.NoError(t, writeErr)
	require.True(t, handledWrite)

	unmutatedBuf := make([]byte, 17)
	nUnmutated, handledUnmutated, errUnmutated := coord.Read(
		context.Background(),
		ino,
		0,
		unmutatedBuf,
		0,
		true,
		func() bool { return false },
		func(context.Context, uint64, uint32) (rpc.ReadPayload, error) {
			t.Fatal("zero-RPC RYOW read with baseSize=0 and !mutated must not call serverRead")
			return rpc.ReadPayload{}, nil
		},
	)
	require.NoError(t, errUnmutated)
	require.True(t, handledUnmutated)
	require.Equal(t, 17, nUnmutated)
	require.Equal(t, []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00world"), unmutatedBuf)

	var serverReads atomic.Int32
	mutatedBuf := make([]byte, 17)
	nMutated, handledMutated, errMutated := coord.Read(
		context.Background(),
		ino,
		0,
		mutatedBuf,
		0,
		true,
		func() bool { return true },
		func(_ context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
			serverReads.Add(1)
			require.Equal(t, uint64(0), offset)
			require.Equal(t, uint32(12), length)
			return rpc.ReadPayload{Data: []byte("hello-go\x00\x00\x00\x00")}, nil
		},
	)
	require.NoError(t, errMutated)
	require.True(t, handledMutated)
	require.Equal(t, 17, nMutated)
	require.Equal(t, int32(1), serverReads.Load(), "mutatedFn returning true must drop hasBaseSize and fetch uncovered prefix from server")
	require.Equal(t, []byte("hello-go\x00\x00\x00\x00world"), mutatedBuf)
}
