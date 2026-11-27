package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func TestGCReadRace_ConcurrentReadFailsDuringBackgroundPackGC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	node, err := engine.OpenNode(engine.NodeOptions{
		DataDir:       t.TempDir(),
		BlockSize:     4096,
		MaxPackSize:   8192,
		MountLeaseTTL: 30 * time.Second,
		GCMode:        engine.GCModeWakeOnly,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close(context.Background()))
	})

	srv, err := New(Params{
		Config: config.ServerConfig{},
		Node:   node,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)

	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "gc-read-race",
		MaxBytes: 64 << 20,
	}))
	require.NoError(t, err)
	diskID := createDiskResp.Msg.GetDisk().GetDiskId()

	mntResp, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: defaultBranch(),
	}))
	require.NoError(t, err)
	epoch := mntResp.Msg.GetEpoch()

	liveCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    defaultBranch(),
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("live.bin"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	liveIno := liveCreate.Msg.GetInode().GetIno()

	deadCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    defaultBranch(),
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("dead.bin"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	deadIno := deadCreate.Msg.GetInode().GetIno()

	const (
		blockSize  = 4096
		blockCount = 256
		fileSize   = blockSize * blockCount
	)

	expected := make([]byte, fileSize)
	for i := range blockCount {
		blk := bytes.Repeat([]byte{byte(i%251 + 1)}, blockSize)
		copy(expected[i*blockSize:], blk)

		_, wErr := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID,
			Branch: defaultBranch(),
			Epoch:  epoch,
			Ino:    liveIno,
			Offset: uint64(i * blockSize),
			Data:   blk,
		}))
		require.NoError(t, wErr)

		_, dErr := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID,
			Branch: defaultBranch(),
			Epoch:  epoch,
			Ino:    deadIno,
			Offset: uint64(i * blockSize),
			Data:   bytes.Repeat([]byte{0xEE}, blockSize),
		}))
		require.NoError(t, dErr)
	}

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:    diskID,
		Branch:    defaultBranch(),
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("dead.bin"),
	}))
	require.NoError(t, err)

	sealCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    defaultBranch(),
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("seal.bin"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: defaultBranch(),
		Epoch:  epoch,
		Ino:    sealCreate.Msg.GetInode().GetIno(),
		Offset: 0,
		Data:   bytes.Repeat([]byte{0xCC}, blockSize),
	}))
	require.NoError(t, err)

	_, err = client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: defaultBranch(),
		NewBranch:    "gc-trigger",
		Message:      "branch whose deletion triggers background pack GC",
	}))
	require.NoError(t, err)

	var (
		stopReaders atomic.Bool
		errMu       sync.Mutex
		firstErr    error
		readerWG    sync.WaitGroup
		readyWG     sync.WaitGroup
	)

	const numReaders = 4
	readyWG.Add(numReaders)
	for range numReaders {
		readerWG.Go(func() {
			readyWG.Done()
			for !stopReaders.Load() {
				resp, rErr := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
					DiskId: diskID,
					Branch: defaultBranch(),
					Ino:    liveIno,
					Offset: 0,
					Size:   fileSize,
				}))
				if rErr != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = rErr
					}
					errMu.Unlock()
					stopReaders.Store(true)
					return
				}
				if !bytes.Equal(resp.Msg.GetData(), expected) {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("read payload mismatch: got %d bytes", len(resp.Msg.GetData()))
					}
					errMu.Unlock()
					stopReaders.Store(true)
					return
				}
			}
		})
	}

	readyWG.Wait()
	_, err = client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: diskID,
		Branch: "gc-trigger",
	}))
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !stopReaders.Load() {
		if node.MetricsSnapshot().PackPhysicalBytes <= fileSize+2*blockSize {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	stopReaders.Store(true)
	readerWG.Wait()

	require.NoError(t, firstErr)
}
