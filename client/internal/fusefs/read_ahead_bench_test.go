package fusefs_test

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
)

func BenchmarkReadAhead_Sequential64MiB(b *testing.B) {
	const (
		totalSize = 64 << 20
		step      = 128 << 10
	)
	chunk := make([]byte, fusefs.MaxRPCChunkSize)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	fetch := func(_ context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
		if offset >= totalSize {
			return rpc.ReadPayload{}, nil
		}
		n := min(int(length), int(totalSize-offset))
		return rpc.ReadPayload{Data: chunk[:n]}, nil
	}
	buf := make([]byte, step)
	b.SetBytes(totalSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ra := fusefs.NewReadAheadCoordinator()
		fh := uint64(i + 1)
		for off := uint64(0); off < totalSize; off += step {
			n, err := ra.Read(context.Background(), fh, 100, off, buf, fetch)
			if err != nil || n != step {
				b.Fatalf("Read off=%d n=%d err=%v", off, n, err)
			}
		}
		ra.RemoveHandle(fh)
	}
}

func BenchmarkReadAhead_RandomReadZeroAmp(b *testing.B) {
	const (
		totalSize = 64 << 20
		readSize  = 4096
	)
	chunk := make([]byte, readSize)
	offsets := [16]uint64{
		12 << 20, 4 << 20, 20 << 20, 8 << 20,
		28 << 20, 0, 36 << 20, 16 << 20,
		44 << 20, 24 << 20, 52 << 20, 32 << 20,
		60 << 20, 40 << 20, 48 << 20, 56 << 20,
	}
	var fetches atomic.Int64
	fetch := func(_ context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
		fetches.Add(1)
		if length != readSize {
			return rpc.ReadPayload{}, syscall.EINVAL
		}
		if offset >= totalSize {
			return rpc.ReadPayload{}, nil
		}
		return rpc.ReadPayload{Data: chunk}, nil
	}
	ra := fusefs.NewReadAheadCoordinator()
	buf := make([]byte, readSize)
	b.SetBytes(int64(len(offsets) * readSize))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, off := range offsets {
			n, err := ra.Read(context.Background(), 1, 200, off, buf, fetch)
			if err != nil || n != readSize {
				b.Fatalf("Read off=%d n=%d err=%v", off, n, err)
			}
		}
	}
	b.StopTimer()
	ra.RemoveHandle(1)
	if fetches.Load() != int64(b.N*len(offsets)) {
		b.Fatalf("expected 0 read amplification: fetches=%d want=%d", fetches.Load(), b.N*len(offsets))
	}
}

func TestStep17_ReadAhead_ROTTLExpirationAndZeroTTLBypass(t *testing.T) {
	t.Run("ROWindowExpiresAfterAttrTimeout", func(t *testing.T) {
		var (
			mu  sync.Mutex
			now = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
		)
		clock := func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return now
		}
		advance := func(d time.Duration) {
			mu.Lock()
			now = now.Add(d)
			mu.Unlock()
		}

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "ro.bin", 0o644, buildRepeatingBytes(512*1024, 'A'))
		fs := fusefs.NewRawFSWithClockForTest(srv, fusefs.Config{
			DiskID:              "disk-test",
			RootIno:             1,
			Mode:                fusefs.MountModeRO,
			EntryTimeoutSeconds: 2.0,
			AttrTimeoutSeconds:  2.0,
		}, clock)
		t.Cleanup(fs.OnUnmount)

		var openOut fuse.OpenOut
		requireStatus(t, "Open RO", fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Flags:    syscall.O_RDONLY,
		}, &openOut))

		_ = mustReadAt(t, fs, ino, openOut.Fh, 0, 128*1024)
		_ = mustReadAt(t, fs, ino, openOut.Fh, 128*1024, 128*1024)

		callsBeforeHit := srv.readCalls.Load()
		got := mustReadAt(t, fs, ino, openOut.Fh, 256*1024, 64*1024)
		require.Equal(t, byte('A'), got[0])
		require.Equal(t, callsBeforeHit, srv.readCalls.Load())

		srv.mu.Lock()
		srv.nodes[ino].data = buildRepeatingBytes(512*1024, 'B')
		srv.mu.Unlock()

		advance(2 * time.Second)
		gotAfterTTL := mustReadAt(t, fs, ino, openOut.Fh, 320*1024, 64*1024)
		require.Equal(t, byte('B'), gotAfterTTL[0])
		require.Greater(t, srv.readCalls.Load(), callsBeforeHit)
	})

	t.Run("ROZeroAttrTimeoutBypassesBufferingAndPrefetch", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(0, 0))
		ino := srv.seedFile(1, "ro-zero.bin", 0o644, buildRepeatingBytes(8<<20, 'C'))
		fs := fusefs.NewRawFS(srv, fusefs.Config{
			DiskID:              "disk-test",
			RootIno:             1,
			Mode:                fusefs.MountModeRO,
			EntryTimeoutSeconds: 0,
			AttrTimeoutSeconds:  0,
		})
		t.Cleanup(fs.OnUnmount)

		var openOut fuse.OpenOut
		requireStatus(t, "Open RO0", fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Flags:    syscall.O_RDONLY,
		}, &openOut))

		readsBefore := srv.readCalls.Load()
		_ = mustReadAt(t, fs, ino, openOut.Fh, 0, 64*1024)
		_ = mustReadAt(t, fs, ino, openOut.Fh, 64*1024, 64*1024)
		_ = mustReadAt(t, fs, ino, openOut.Fh, 128*1024, 64*1024)
		require.Equal(t, readsBefore+3, srv.readCalls.Load())
		require.Zero(t, fs.Snapshot().ReadAhead.Triggers)
	})
}
