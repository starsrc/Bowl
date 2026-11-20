package fusefs

import (
	"context"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestStageCoordinator_PartialAppendWriteFailure_TruncatesBackToOldLength(t *testing.T) {
	stageDir := t.TempDir()
	var uploaded []byte
	writer := func(_ context.Context, _ uint64, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		uploaded = append([]byte(nil), data...)
		return uint32(len(data)), nil, nil
	}
	coord := NewStageCoordinator(stageDir, DefaultStageSize, writer, nil, StageHooks{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	w1, handled1, err1 := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, []byte("ABCD"), 1)
	require.NoError(t, err1)
	require.True(t, handled1)
	require.Equal(t, uint32(4), w1)

	coord.mu.Lock()
	chunkPath := coord.inodes[100].active.path
	coord.mu.Unlock()

	coord.writeChunkPiece = func(path string, curOff, oldOffset uint64, piece []byte) error {
		require.NoError(t, writeChunkFilePiece(path, curOff, oldOffset, piece[:2]))
		return syscall.ENOSPC
	}

	_, handled2, err2 := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 4, []byte("EFGH"), 1)
	require.True(t, handled2)
	require.ErrorIs(t, err2, syscall.ENOSPC)

	info, statErr := os.Stat(chunkPath)
	require.NoError(t, statErr)
	require.Equal(t, int64(4), info.Size(), "partial append failure must truncate chunk back to oldLength=4, got=%d", info.Size())

	diskBytes, readErr := os.ReadFile(chunkPath)
	require.NoError(t, readErr)
	require.Equal(t, []byte("ABCD"), diskBytes)

	coord.mu.Lock()
	usedAfterRollback := coord.usedBytes
	coord.mu.Unlock()
	require.Equal(t, uint64(4), usedAfterRollback)

	coord.writeChunkPiece = nil
	require.NoError(t, coord.FlushInodeForHandle(context.Background(), 1, 100))
	require.Equal(t, []byte("ABCD"), uploaded)
}

func TestStageCoordinator_BusyActiveWriterCompleting_WakesBackpressureWaiter(t *testing.T) {
	stageDir := t.TempDir()
	writer := func(_ context.Context, _ uint64, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		return uint32(len(data)), nil, nil
	}
	coord := NewStageCoordinator(stageDir, StageChunkSize, writer, nil, StageHooks{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	writerAEntered := make(chan struct{})
	writerARelease := make(chan struct{})
	coord.writeChunkPiece = func(path string, curOff, oldOffset uint64, piece []byte) error {
		if len(piece) == 2<<20 {
			close(writerAEntered)
			<-writerARelease
		}
		return writeChunkFilePiece(path, curOff, oldOffset, piece)
	}

	writerADone := make(chan error, 1)
	go func() {
		_, _, err := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, make([]byte, 2<<20), 1)
		writerADone <- err
	}()
	<-writerAEntered

	writerBDone := make(chan error, 1)
	go func() {
		_, _, err := coord.WriteForHandle(context.Background(), nil, 2, 200, 200, 0, make([]byte, 3<<20), 1)
		writerBDone <- err
	}()

	require.Eventually(t, func() bool {
		coord.mu.Lock()
		sleeping := coord.bpWakeCh != nil
		coord.mu.Unlock()
		return sleeping
	}, 2*time.Second, 2*time.Millisecond, "writer B did not enter backpressure wait")

	close(writerARelease)
	require.NoError(t, <-writerADone)

	select {
	case errB := <-writerBDone:
		require.NoError(t, errB)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writer B deadlocked in backpressure after busy writer A became idle")
	}
}

func TestStageCoordinator_ProactiveSealIdleActiveChunks_WhenDrainingAloneCannotFreeQuota(t *testing.T) {
	stageDir := t.TempDir()
	ino200Entered := make(chan struct{})
	ino200Gate := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-ino200Gate:
		default:
			close(ino200Gate)
		}
	})
	writer := func(ctx context.Context, ino uint64, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		if ino == 200 {
			close(ino200Entered)
			select {
			case <-ino200Gate:
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			}
		}
		return uint32(len(data)), nil, nil
	}
	coord := NewStageCoordinator(stageDir, StageChunkSize, writer, nil, StageHooks{})
	t.Cleanup(func() {
		select {
		case <-ino200Gate:
		default:
			close(ino200Gate)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	_, _, err1 := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, make([]byte, 2<<20), 1)
	require.NoError(t, err1)

	_, _, err2 := coord.WriteForHandle(context.Background(), nil, 2, 200, 200, 0, make([]byte, 1<<20), 1)
	require.NoError(t, err2)
	coord.SealInode(200)
	<-ino200Entered

	writer3Done := make(chan error, 1)
	go func() {
		_, _, err := coord.WriteForHandle(context.Background(), nil, 3, 300, 300, 0, make([]byte, (2<<20)+(512<<10)), 1)
		writer3Done <- err
	}()

	select {
	case err3 := <-writer3Done:
		require.NoError(t, err3)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writer 3 blocked because idle active chunk on inode 100 was not proactively sealed while inode 200 was inflight")
	}
}

func TestStageCoordinator_ConcurrentDiscardInodeAndWrite_NeverDetachesNewWrite(t *testing.T) {
	stageDir := t.TempDir()
	inflightEntered := make(chan struct{})
	inflightRelease := make(chan struct{})
	var firstOnce sync.Once
	var mu sync.Mutex
	var uploaded [][]byte

	writer := func(_ context.Context, _ uint64, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		if string(data) == "old-inflight" {
			firstOnce.Do(func() { close(inflightEntered) })
			<-inflightRelease
		}
		mu.Lock()
		uploaded = append(uploaded, append([]byte(nil), data...))
		mu.Unlock()
		return uint32(len(data)), nil, nil
	}
	coord := NewStageCoordinator(stageDir, DefaultStageSize, writer, nil, StageHooks{})
	t.Cleanup(func() {
		select {
		case <-inflightRelease:
		default:
			close(inflightRelease)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	_, _, err := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, []byte("old-inflight"), 1)
	require.NoError(t, err)
	coord.SealInode(100)
	<-inflightEntered

	coord.mu.Lock()
	oldDiscardCh := coord.inodes[100].discardCh
	coord.mu.Unlock()

	discardDone := make(chan struct{})
	go func() {
		coord.DiscardInode(100)
		close(discardDone)
	}()

	<-oldDiscardCh

	newWriteDone := make(chan error, 1)
	go func() {
		_, _, wErr := coord.WriteForHandle(context.Background(), nil, 2, 100, 100, 0, []byte("new-after-discard"), 2)
		newWriteDone <- wErr
	}()

	time.Sleep(25 * time.Millisecond)
	close(inflightRelease)

	<-discardDone
	require.NoError(t, <-newWriteDone)

	require.True(t, coord.HasPending(100), "new write racing with DiscardInode must remain attached to c.inodes[100]")
	require.NoError(t, coord.FlushInodeForHandle(context.Background(), 2, 100))

	mu.Lock()
	defer mu.Unlock()
	foundNew := false
	for _, buf := range uploaded {
		if string(buf) == "new-after-discard" {
			foundNew = true
		}
	}
	require.True(t, foundNew, "new write after concurrent DiscardInode must be flushed and uploaded")
}
