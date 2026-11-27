package fusefs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestStageCoordinator_PartialAppendWriteFailure_TruncatesBackToOldLength(t *testing.T) {
	stageDir := t.TempDir()
	var uploaded []byte
	writer := func(_ context.Context, _ uint64, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		uploaded = append([]byte(nil), data...)
		return uint32(len(data)), nil, nil
	}
	coord := NewStageCoordinator(stageDir, DefaultStageSize, writer, StageHooks{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	w1, handled1, err1 := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, []byte("ABCD"))
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

	_, handled2, err2 := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 4, []byte("EFGH"))
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
	coord := NewStageCoordinator(stageDir, StageChunkSize, writer, StageHooks{})
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
		_, _, err := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, make([]byte, 2<<20))
		writerADone <- err
	}()
	<-writerAEntered

	writerBDone := make(chan error, 1)
	go func() {
		_, _, err := coord.WriteForHandle(context.Background(), nil, 2, 200, 200, 0, make([]byte, 3<<20))
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
	coord := NewStageCoordinator(stageDir, StageChunkSize, writer, StageHooks{})
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

	_, _, err1 := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, make([]byte, 2<<20))
	require.NoError(t, err1)

	_, _, err2 := coord.WriteForHandle(context.Background(), nil, 2, 200, 200, 0, make([]byte, 1<<20))
	require.NoError(t, err2)
	coord.SealInode(200)
	<-ino200Entered

	writer3Done := make(chan error, 1)
	go func() {
		_, _, err := coord.WriteForHandle(context.Background(), nil, 3, 300, 300, 0, make([]byte, (2<<20)+(512<<10)))
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
	coord := NewStageCoordinator(stageDir, DefaultStageSize, writer, StageHooks{})
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

	_, _, err := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, []byte("old-inflight"))
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
		_, _, wErr := coord.WriteForHandle(context.Background(), nil, 2, 100, 100, 0, []byte("new-after-discard"))
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

func TestStageCoordinator_ClearWithExpiredContext_DispatchesZeroWritesAndCleansUp(t *testing.T) {
	stageDir := t.TempDir()
	var writeCalls atomic.Int32
	writer := func(_ context.Context, _ uint64, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		writeCalls.Add(1)
		return uint32(len(data)), nil, nil
	}
	coord := NewStageCoordinator(stageDir, DefaultStageSize, writer, StageHooks{})

	var logBuf bytes.Buffer
	coord.bindLogger(slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	_, _, err := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, []byte("expired-clear"))
	require.NoError(t, err)

	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()

	coord.Clear(expiredCtx, 0)

	require.Zero(t, writeCalls.Load(), "Clear with expired context must not seal or dispatch Write RPCs: got=%d", writeCalls.Load())
	matches, globErr := filepath.Glob(filepath.Join(stageDir, "chunk-*.stage"))
	require.NoError(t, globErr)
	require.Empty(t, matches, "leftover stage chunk files must be removed after expired Clear")

	foundAbortLog := false
	for line := range strings.SplitSeq(strings.TrimSpace(logBuf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil && rec["msg"] == "stage_unmount_drain_aborted" {
			if dropped, ok := rec["dropped_chunks"].(float64); ok && int(dropped) == 1 {
				foundAbortLog = true
			}
		}
	}
	require.True(t, foundAbortLog, "expired Clear must log stage_unmount_drain_aborted with dropped_chunks=1, got=%s", logBuf.String())
}

func TestStageCoordinator_BarrierInodeWithPreCanceledContext_ReturnsCauseWithoutSealingActiveChunk(t *testing.T) {
	stageDir := t.TempDir()
	var writeCalls atomic.Int32
	writer := func(_ context.Context, _ uint64, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		writeCalls.Add(1)
		return uint32(len(data)), nil, nil
	}
	coord := NewStageCoordinator(stageDir, DefaultStageSize, writer, StageHooks{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	_, _, err := coord.WriteForHandle(context.Background(), nil, 1, 100, 100, 0, []byte("pre-canceled"))
	require.NoError(t, err)

	preCanceledCtx, cancelCause := context.WithCancelCause(context.Background())
	cancelCause(rpc.ErrKernelInterrupted)

	barrierErr := coord.BarrierInode(preCanceledCtx, 100)
	require.ErrorIs(t, barrierErr, rpc.ErrKernelInterrupted)
	require.Zero(t, writeCalls.Load(), "pre-canceled BarrierInode must not seal or upload active chunk: got=%d", writeCalls.Load())

	coord.mu.Lock()
	st := coord.inodes[100]
	activeStillPresent := st != nil && st.active != nil && len(coord.queue) == 0
	coord.mu.Unlock()
	require.True(t, activeStillPresent, "active chunk must remain unsealed after pre-canceled BarrierInode")
}

type stubTmpMaterializer struct {
	serverIno uint64
}

func (s *stubTmpMaterializer) CreateTmpInode(_ context.Context, _ uint64, _ string, mode, uid, gid uint32) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status) {
	return &storagev1.Inode{
		Ino:        s.serverIno,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  mode,
			Nlink: 1,
			Uid:   uid,
			Gid:   gid,
		},
	}, 1.0, 1.0, fuse.OK
}

func (s *stubTmpMaterializer) WriteTmpExtent(_ context.Context, _, _ uint64, _ []byte) fuse.Status {
	return fuse.OK
}

func (s *stubTmpMaterializer) CommitTmpMeta(_ context.Context, serverIno uint64, meta TmpPromotedMeta) (*storagev1.Inode, fuse.Status) {
	return &storagev1.Inode{
		Ino:        serverIno,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Size:  meta.Size,
			Mode:  meta.Mode,
			Nlink: 1,
			Gid:   meta.Gid,
			Flags: meta.Flags,
		},
	}, fuse.OK
}

func (s *stubTmpMaterializer) RollbackTmpInode(_ context.Context, _ uint64, _ string) {}

func TestTmpFileManager_LockFreeFastPath_ToServerInoAndToKernelIno(t *testing.T) {
	mgr := NewTmpFileManager(42, t.TempDir())
	t.Cleanup(mgr.Clear)

	mgr.mu.Lock()
	gotRoot, stRoot := mgr.ToServerIno(fuse.FUSE_ROOT_ID)
	gotNormal, stNormal := mgr.ToServerIno(100)
	_, stRawRoot := mgr.ToServerIno(42)
	_, stMissingTmp := mgr.ToServerIno(tmpLocalInoBit | 99)
	gotKernelRoot := mgr.ToKernelIno(42)
	gotKernelNormal := mgr.ToKernelIno(100)
	mgr.mu.Unlock()

	require.Equal(t, fuse.OK, stRoot)
	require.Equal(t, uint64(42), gotRoot)
	require.Equal(t, fuse.OK, stNormal)
	require.Equal(t, uint64(100), gotNormal)
	require.Equal(t, fuse.ENOENT, stRawRoot)
	require.Equal(t, fuse.ENOENT, stMissingTmp)
	require.Equal(t, uint64(fuse.FUSE_ROOT_ID), gotKernelRoot)
	require.Equal(t, uint64(100), gotKernelNormal)

	localIno, _, createSt := mgr.Create(fuse.FUSE_ROOT_ID, 1000, 1000, 0o600, uint32(syscall.O_RDWR))
	require.Equal(t, fuse.OK, createSt)
	require.Equal(t, int64(1), mgr.activeCount.Load())
	require.Zero(t, mgr.aliasCount.Load())

	mgr.mu.Lock()
	gotNormalWithTmp, stNormalWithTmp := mgr.ToServerIno(100)
	gotKernelNoAlias := mgr.ToKernelIno(777)
	mgr.mu.Unlock()
	require.Equal(t, fuse.OK, stNormalWithTmp)
	require.Equal(t, uint64(100), gotNormalWithTmp)
	require.Equal(t, uint64(777), gotKernelNoAlias)

	unpromotedSrv, stUnpromoted := mgr.ToServerIno(localIno)
	require.Equal(t, fuse.OK, stUnpromoted)
	require.Equal(t, localIno, unpromotedSrv)

	var entry fuse.EntryOut
	res, handled, linkSt := mgr.Link(context.Background(), localIno, fuse.FUSE_ROOT_ID, "promoted.bin", true, &stubTmpMaterializer{serverIno: 777}, &entry)
	require.True(t, handled)
	require.Equal(t, fuse.OK, linkSt)
	require.Equal(t, uint64(777), res.ServerIno)
	require.Equal(t, int64(1), mgr.activeCount.Load())
	require.Equal(t, int64(1), mgr.aliasCount.Load())

	aliasedSrv, stAliased := mgr.ToServerIno(localIno)
	require.Equal(t, fuse.OK, stAliased)
	require.Equal(t, uint64(777), aliasedSrv)
	require.Equal(t, localIno, mgr.ToKernelIno(777))

	mgr.mu.Lock()
	gotNormalWithAlias, stNormalWithAlias := mgr.ToServerIno(100)
	mgr.mu.Unlock()
	require.Equal(t, fuse.OK, stNormalWithAlias)
	require.Equal(t, uint64(100), gotNormalWithAlias)

	require.True(t, mgr.OnForget(localIno, 2, nil))
	require.Zero(t, mgr.activeCount.Load())
	require.Zero(t, mgr.aliasCount.Load())

	mgr.mu.Lock()
	_, stAfterForget := mgr.ToServerIno(localIno)
	gotKernelAfterForget := mgr.ToKernelIno(777)
	mgr.mu.Unlock()
	require.Equal(t, fuse.ENOENT, stAfterForget)
	require.Equal(t, uint64(777), gotKernelAfterForget)

	localIno2, _, createSt2 := mgr.Create(fuse.FUSE_ROOT_ID, 1000, 1000, 0o600, uint32(syscall.O_RDWR))
	require.Equal(t, fuse.OK, createSt2)
	_, _, linkSt2 := mgr.Link(context.Background(), localIno2, fuse.FUSE_ROOT_ID, "promoted2.bin", true, &stubTmpMaterializer{serverIno: 778}, &entry)
	require.Equal(t, fuse.OK, linkSt2)
	mgr.Clear()
	require.Zero(t, mgr.activeCount.Load())
	require.Zero(t, mgr.aliasCount.Load())
}

func TestStageCoordinator_CleanChunkLifecycle_CoalesceRYOWOnCommitAndZeroLeftovers(t *testing.T) {
	stageDir := t.TempDir()
	var (
		writeCalls atomic.Int32
		uploaded   []byte
		commitMu   sync.Mutex
		commitNode uint64
		commitIno  uint64
		commitSize uint64
	)
	writer := func(_ context.Context, ino, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
		writeCalls.Add(1)
		uploaded = append([]byte(nil), data...)
		return uint32(len(data)), &storagev1.Inode{
			Ino: ino,
			Attr: &storagev1.InodeAttribute{
				Mode: syscall.S_IFREG | 0o644,
				Size: uint64(len(data)),
			},
		}, nil
	}
	coord := NewStageCoordinator(stageDir, DefaultStageSize, writer, StageHooks{
		OnCommit: func(nodeID, ino uint64, inode *storagev1.Inode) {
			commitMu.Lock()
			commitNode = nodeID
			commitIno = ino
			if inode != nil && inode.GetAttr() != nil {
				commitSize = inode.GetAttr().GetSize()
			}
			commitMu.Unlock()
		},
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	coord.SeedCreatedInode(100)
	w1, h1, err1 := coord.WriteForHandle(context.Background(), nil, 7, 100, 500, 0, []byte("hello "))
	require.NoError(t, err1)
	require.True(t, h1)
	require.Equal(t, uint32(6), w1)

	w2, h2, err2 := coord.WriteForHandle(context.Background(), nil, 7, 100, 500, 6, []byte("world!"))
	require.NoError(t, err2)
	require.True(t, h2)
	require.Equal(t, uint32(6), w2)

	matchesBefore, err := filepath.Glob(filepath.Join(stageDir, "chunk-*.stage"))
	require.NoError(t, err)
	require.Len(t, matchesBefore, 1, "contiguous writes must coalesce into one stage chunk file")

	var attr fuse.Attr
	coord.OverlayAttr(100, &attr)
	require.Equal(t, uint64(12), attr.Size)

	readBuf := make([]byte, 12)
	n, handled, readErr := coord.Read(context.Background(), 100, 0, readBuf, func(context.Context, uint64, uint32) (rpc.ReadPayload, error) {
		t.Fatal("RYOW read on staged created inode must not call serverRead")
		return rpc.ReadPayload{}, nil
	})
	require.NoError(t, readErr)
	require.True(t, handled)
	require.Equal(t, 12, n)
	require.Equal(t, []byte("hello world!"), readBuf)

	require.NoError(t, coord.FlushInodeForHandle(context.Background(), 7, 100))
	require.Equal(t, int32(1), writeCalls.Load())
	require.Equal(t, []byte("hello world!"), uploaded)
	require.False(t, coord.HasPending(100))

	commitMu.Lock()
	require.Equal(t, uint64(500), commitNode)
	require.Equal(t, uint64(100), commitIno)
	require.Equal(t, uint64(12), commitSize)
	commitMu.Unlock()

	matchesAfter, err := filepath.Glob(filepath.Join(stageDir, "chunk-*.stage"))
	require.NoError(t, err)
	require.Empty(t, matchesAfter, "uploaded stage chunk file must be unlinked after commit")
}
