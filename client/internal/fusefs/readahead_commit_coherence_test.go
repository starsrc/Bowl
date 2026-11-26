package fusefs

import (
	"bytes"
	"context"
	"os"
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

type step69StubClient struct {
	storagev1connect.StorageServiceClient
	mu          sync.Mutex
	files       map[uint64][]byte
	writeHook   func(context.Context, *storagev1.WriteRequest) error
	setAttrHook func(context.Context, *storagev1.SetAttrRequest) error
	readCalls   atomic.Int64
}

func newStep69StubClient() *step69StubClient {
	return &step69StubClient{
		files: make(map[uint64][]byte),
	}
}

func (c *step69StubClient) setFile(ino uint64, data []byte) {
	c.mu.Lock()
	c.files[ino] = append([]byte(nil), data...)
	c.mu.Unlock()
}

func (c *step69StubClient) Read(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
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

func (c *step69StubClient) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	if c.writeHook != nil {
		if err := c.writeHook(ctx, req.Msg); err != nil {
			return nil, err
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
	c.mu.Unlock()
	now := timestamppb.Now()
	return connect.NewResponse(&storagev1.WriteResponse{
		Size: uint32(len(payload)),
		Inode: &storagev1.Inode{
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
		},
	}), nil
}

func (c *step69StubClient) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
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
	c.mu.Unlock()
	now := timestamppb.Now()
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode: &storagev1.Inode{
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
		},
		AttrTimeoutSeconds: 1.0,
	}), nil
}

func (c *step69StubClient) Fsync(_ context.Context, _ *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (c *step69StubClient) FsyncDir(_ context.Context, _ *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

func TestStep69_StageOnCommit_InvalidatesReadAheadWindowAndPrefetchSlots(t *testing.T) {
	t.Run("RegularStagedCommit_EvictsPreCommitReadAheadWindowAndDiscardsInFlightPrefetch", func(t *testing.T) {
		client := newStep69StubClient()
		const ino uint64 = 501
		const chunkLen = 128 << 10
		oldData := bytes.Repeat([]byte{'A'}, 2*MaxRPCChunkSize)
		newData := bytes.Repeat([]byte{'B'}, chunkLen)
		client.setFile(ino, oldData)

		writeGate := make(chan struct{})
		writeEntered := make(chan struct{})
		var writeOnce sync.Once
		client.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) error {
			writeOnce.Do(func() { close(writeEntered) })
			<-writeGate
			return nil
		}

		fs := NewRawFS(client, Config{
			DiskID:              "disk-step69",
			Epoch:               1,
			RootIno:             1,
			Mode:                MountModeStaged,
			StageDir:            t.TempDir(),
			EntryTimeoutSeconds: 1.0,
			AttrTimeoutSeconds:  1.0,
		})
		t.Cleanup(func() {
			select {
			case <-writeGate:
			default:
				close(writeGate)
			}
			fs.OnUnmount()
		})

		rdFh, st := fs.openHandle(ino, syscall.O_RDONLY, false)
		require.Equal(t, fuse.OK, st)
		wrFh, st := fs.openHandle(ino, syscall.O_RDWR, false)
		require.Equal(t, fuse.OK, st)

		_, handled, err := fs.stage.WriteForHandle(context.Background(), nil, wrFh, ino, ino, 0, newData, 0)
		require.NoError(t, err)
		require.True(t, handled)
		fs.stage.SealInode(ino)

		select {
		case <-writeEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("staged Write did not enter background upload")
		}

		prefetchStarted := make(chan struct{})
		prefetchCanceled := make(chan struct{})
		var prefetchStartOnce sync.Once
		var prefetchCancelOnce sync.Once
		fetch := func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
			if offset >= MaxRPCChunkSize {
				prefetchStartOnce.Do(func() { close(prefetchStarted) })
				<-ctx.Done()
				prefetchCancelOnce.Do(func() { close(prefetchCanceled) })
				return rpc.ReadPayload{}, ctx.Err()
			}
			resp, rErr := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
				Ino:    ino,
				Offset: offset,
				Size:   length,
				DiskId: "disk-step69",
			}))
			if rErr != nil {
				return rpc.ReadPayload{}, rErr
			}
			return rpc.TakeReadPayload(resp.Msg), nil
		}

		buf := make([]byte, chunkLen)
		for seqOff := uint64(0); seqOff < 3*chunkLen; seqOff += chunkLen {
			n, rErr := fs.readAhead.Read(context.Background(), rdFh, ino, seqOff, buf, fetch)
			require.NoError(t, rErr)
			require.Equal(t, chunkLen, n)
			require.Equal(t, byte('A'), buf[0])
		}

		select {
		case <-prefetchStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("readAhead did not launch in-flight prefetch slot")
		}

		close(writeGate)
		require.NoError(t, fs.stage.BarrierInode(context.Background(), ino))

		select {
		case <-prefetchCanceled:
		case <-time.After(2 * time.Second):
			t.Fatal("StageHooks.OnCommit did not cancel in-flight ReadAhead prefetch slot")
		}

		res, readSt := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Fh:       rdFh,
			Offset:   0,
			Size:     uint32(chunkLen),
		}, make([]byte, chunkLen))
		require.Equal(t, fuse.OK, readSt)
		got, byteSt := res.Bytes(make([]byte, chunkLen))
		require.Equal(t, fuse.OK, byteSt)
		require.Equal(t, newData, got, "scene=step69_regular_commit_evicts_readahead want='B' got=%q", got[:8])
	})

	t.Run("PromotedTmpfileExtentCommit_PreserveMetaTrue_StillEvictsReadAheadWindow", func(t *testing.T) {
		client := newStep69StubClient()
		const ino uint64 = 502
		const chunkLen = 128 << 10
		const fileLen = 4 * chunkLen
		oldData := bytes.Repeat([]byte{'A'}, fileLen)
		newData := bytes.Repeat([]byte{'P'}, fileLen)
		client.setFile(ino, oldData)

		extentWritten := make(chan struct{})
		var writeOnce sync.Once
		client.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) error {
			writeOnce.Do(func() { close(extentWritten) })
			return nil
		}
		commitEntered := make(chan struct{})
		commitGate := make(chan struct{})
		var commitOnce sync.Once
		client.setAttrHook = func(_ context.Context, _ *storagev1.SetAttrRequest) error {
			commitOnce.Do(func() { close(commitEntered) })
			<-commitGate
			return nil
		}

		fs := NewRawFS(client, Config{
			DiskID:              "disk-step69-promo",
			Epoch:               1,
			RootIno:             1,
			Mode:                MountModeStaged,
			StageDir:            t.TempDir(),
			EntryTimeoutSeconds: 1.0,
			AttrTimeoutSeconds:  1.0,
		})
		t.Cleanup(func() {
			select {
			case <-commitGate:
			default:
				close(commitGate)
			}
			fs.OnUnmount()
		})

		rdFh, st := fs.openHandle(ino, syscall.O_RDONLY, false)
		require.Equal(t, fuse.OK, st)

		fetch := func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
			resp, rErr := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
				Ino:    ino,
				Offset: offset,
				Size:   length,
				DiskId: "disk-step69-promo",
			}))
			if rErr != nil {
				return rpc.ReadPayload{}, rErr
			}
			return rpc.TakeReadPayload(resp.Msg), nil
		}
		warmBuf := make([]byte, chunkLen)
		for seqOff := uint64(0); seqOff < 2*chunkLen; seqOff += chunkLen {
			n, err := fs.readAhead.Read(context.Background(), rdFh, ino, seqOff, warmBuf, fetch)
			require.NoError(t, err)
			require.Equal(t, chunkLen, n)
			require.Equal(t, byte('A'), warmBuf[0])
		}

		tmpFile, err := os.CreateTemp(t.TempDir(), "promo-*")
		require.NoError(t, err)
		_, err = tmpFile.Write(newData)
		require.NoError(t, err)

		err = fs.stage.PromoteTmpfile(
			context.Background(),
			ino,
			ino,
			0,
			tmpFile,
			StagePromotedMeta{Size: fileLen, Mode: syscall.S_IFREG | 0o644},
			[]stageExtent{{offset: 0, length: fileLen}},
		)
		require.NoError(t, err)

		select {
		case <-extentWritten:
		case <-time.After(2 * time.Second):
			t.Fatal("promoted tmpfile extent did not upload")
		}
		select {
		case <-commitEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("promoted tmpfile SetAttr commit did not start")
		}

		afterExtentBuf := make([]byte, chunkLen)
		n, err := fs.readAhead.Read(context.Background(), rdFh, ino, 2*chunkLen, afterExtentBuf, fetch)
		require.NoError(t, err)
		require.Equal(t, chunkLen, n)
		require.Equal(t, byte('P'), afterExtentBuf[0], "scene=step69_promoted_extent_preserve_meta_evicts_readahead want='P' got=%q", afterExtentBuf[:8])

		client.setFile(ino, oldData)
		fs.readAhead.InvalidateInode(ino)
		for seqOff := uint64(0); seqOff < 2*chunkLen; seqOff += chunkLen {
			n, err = fs.readAhead.Read(context.Background(), rdFh, ino, seqOff, warmBuf, fetch)
			require.NoError(t, err)
			require.Equal(t, chunkLen, n)
			require.Equal(t, byte('A'), warmBuf[0])
		}
		client.setFile(ino, newData)

		close(commitGate)
		require.NoError(t, fs.stage.BarrierInode(context.Background(), ino))

		afterCommitBuf := make([]byte, chunkLen)
		n, err = fs.readAhead.Read(context.Background(), rdFh, ino, 2*chunkLen, afterCommitBuf, fetch)
		require.NoError(t, err)
		require.Equal(t, chunkLen, n)
		require.Equal(t, byte('P'), afterCommitBuf[0], "scene=step69_promoted_commit_evicts_readahead want='P' got=%q", afterCommitBuf[:8])
	})

	t.Run("RawFSRead_RemainsLockFreeUnderExclusiveInodeLock", func(t *testing.T) {
		client := newStep69StubClient()
		const ino uint64 = 503
		want := []byte("lock-free-read-data")
		client.setFile(ino, want)

		fs := NewRawFS(client, Config{
			DiskID:              "disk-step69-lockfree",
			Epoch:               1,
			RootIno:             1,
			Mode:                MountModeStaged,
			StageDir:            t.TempDir(),
			EntryTimeoutSeconds: 1.0,
			AttrTimeoutSeconds:  1.0,
		})
		t.Cleanup(fs.OnUnmount)

		rdFh, st := fs.openHandle(ino, syscall.O_RDONLY, false)
		require.Equal(t, fuse.OK, st)

		unlock := fs.inodeLocks.lock(ino)
		defer unlock()

		done := make(chan []byte, 1)
		go func() {
			buf := make([]byte, len(want))
			res, readSt := fs.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: ino},
				Fh:       rdFh,
				Offset:   0,
				Size:     uint32(len(want)),
			}, buf)
			if readSt != fuse.OK {
				done <- nil
				return
			}
			got, _ := res.Bytes(buf)
			done <- append([]byte(nil), got...)
		}()

		select {
		case got := <-done:
			require.Equal(t, want, got)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("RawFS.Read blocked on exclusive inodeLocks.lock")
		}
	})
}
