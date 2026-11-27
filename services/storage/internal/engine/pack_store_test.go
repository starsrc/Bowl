package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackStore_ConcurrentAppendReadAndRotation_NoEBADF(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store, err := newPackStore(t.TempDir(), 8192, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	type writtenBlock struct {
		ref     BlobRef
		payload []byte
	}

	var (
		mu      sync.RWMutex
		written []writtenBlock
	)

	const (
		writers         = 6
		blocksPerWriter = 20
		readers         = 6
		blockLen        = 4096
	)

	var (
		writerWG   sync.WaitGroup
		readerWG   sync.WaitGroup
		syncWG     sync.WaitGroup
		writeErrs  = make([]error, writers)
		readErrs   = make([]error, readers)
		syncErr    error
		writesDone atomic.Bool
	)

	var ebr epochTracker
	ebr.init(store.unlinkRetiredPacks)

	for r := range readers {
		readerIdx := r
		readerWG.Go(func() {
			for iter := 0; !writesDone.Load() || iter < 20; iter++ {
				mu.RLock()
				n := len(written)
				var sample writtenBlock
				if n > 0 {
					sample = written[(iter*7+readerIdx)%n]
				}
				mu.RUnlock()
				if n == 0 {
					continue
				}
				pin := ebr.pin()
				got, readErr := store.readBlockPinned(pin, sample.ref)
				pin.unpin()
				if readErr != nil {
					readErrs[readerIdx] = readErr
					return
				}
				if !bytes.Equal(got, sample.payload) {
					readErrs[readerIdx] = errors.New("payload mismatch during concurrent read")
					return
				}
			}
		})
	}

	syncWG.Go(func() {
		for !writesDone.Load() {
			if err := store.SyncActive(ctx); err != nil {
				syncErr = err
				return
			}
		}
	})

	for w := range writers {
		writerIdx := w
		writerWG.Go(func() {
			for i := range blocksPerWriter {
				fill := byte((writerIdx*blocksPerWriter+i)%251 + 1)
				payload := bytes.Repeat([]byte{fill}, blockLen)
				refs, appErr := store.AppendBlocks(ctx, [][]byte{payload})
				if appErr != nil {
					writeErrs[writerIdx] = appErr
					return
				}
				mu.Lock()
				written = append(written, writtenBlock{ref: refs[0], payload: payload})
				mu.Unlock()
				store.ReleaseInFlight(refs)
			}
		})
	}

	writerWG.Wait()
	writesDone.Store(true)
	readerWG.Wait()
	syncWG.Wait()

	for _, wErr := range writeErrs {
		require.NoError(t, wErr)
	}
	for _, rErr := range readErrs {
		require.NoError(t, rErr)
	}
	require.NoError(t, syncErr)
	require.Len(t, written, writers*blocksPerWriter)
	require.Equal(t, uint64(writers*blocksPerWriter*blockLen), store.PhysicalBytes())

	for _, item := range written {
		pin := ebr.pin()
		got, readErr := store.readBlockPinned(pin, item.ref)
		pin.unpin()
		require.NoError(t, readErr)
		require.Equal(t, item.payload, got)
	}
}

func TestPackStore_SyncActive_CoalescesConcurrentGenerations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store, err := newPackStore(t.TempDir(), 64<<10, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	var syncCalls atomic.Int32

	store.syncFileHook = func(_ uint64) error {
		if syncCalls.Add(1) == 1 {
			close(firstEntered)
			<-firstRelease
		}
		return nil
	}

	refs1, err := store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0x11}, 4096)})
	require.NoError(t, err)
	store.ReleaseInFlight(refs1)

	var firstErr error
	var firstWG sync.WaitGroup
	firstWG.Go(func() {
		firstErr = store.SyncActive(ctx)
	})

	<-firstEntered

	refs2, err := store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0x22}, 4096)})
	require.NoError(t, err)
	store.ReleaseInFlight(refs2)

	refs3, err := store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0x33}, 4096)})
	require.NoError(t, err)
	store.ReleaseInFlight(refs3)

	const waiters = 8
	waitErrs := make([]error, waiters)
	var readyWG sync.WaitGroup
	var waitWG sync.WaitGroup
	readyWG.Add(waiters)

	for i := range waiters {
		idx := i
		waitWG.Go(func() {
			readyWG.Done()
			waitErrs[idx] = store.SyncActive(ctx)
		})
	}
	readyWG.Wait()

	close(firstRelease)
	firstWG.Wait()
	waitWG.Wait()

	require.NoError(t, firstErr)
	for _, wErr := range waitErrs {
		require.NoError(t, wErr)
	}
	require.Equal(t, int32(2), syncCalls.Load())
	require.False(t, store.hasDirty())
}

func TestPackStore_SyncActive_ErrorPreservesDirtyStateAndRetries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store, err := newPackStore(t.TempDir(), 4096, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	refs1, err := store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0xaa}, 4096)})
	require.NoError(t, err)
	store.ReleaseInFlight(refs1)

	refs2, err := store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0xbb}, 4096)})
	require.NoError(t, err)
	store.ReleaseInFlight(refs2)
	require.Equal(t, uint64(1), refs1[0].PackID)
	require.Equal(t, uint64(2), refs2[0].PackID)
	require.True(t, store.hasDirty())

	fileErr := errors.New("injected file sync error")
	store.syncFileHook = func(packID uint64) error {
		if packID == 2 {
			return fileErr
		}
		return nil
	}

	require.ErrorIs(t, store.SyncActive(ctx), fileErr)
	require.True(t, store.hasDirty())

	dirErr := errors.New("injected dir sync error")
	store.syncFileHook = nil
	store.syncDirHook = func(_ string) error {
		return dirErr
	}

	require.ErrorIs(t, store.SyncActive(ctx), dirErr)
	require.True(t, store.hasDirty())

	var retryFileCalls atomic.Int32
	var retryDirCalls atomic.Int32
	store.syncFileHook = func(_ uint64) error {
		retryFileCalls.Add(1)
		return nil
	}
	store.syncDirHook = func(_ string) error {
		retryDirCalls.Add(1)
		return nil
	}

	require.NoError(t, store.SyncActive(ctx))
	require.False(t, store.hasDirty())
	require.Equal(t, int32(2), retryFileCalls.Load())
	require.Equal(t, int32(1), retryDirCalls.Load())

	require.NoError(t, store.SyncActive(ctx))
	require.Equal(t, int32(2), retryFileCalls.Load())
	require.Equal(t, int32(1), retryDirCalls.Load())
}

func TestPackStore_RetirePack_GuardsAndExcludesFromGCScan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store, err := newPackStore(t.TempDir(), 4096, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	payload1 := bytes.Repeat([]byte{0x41}, 4096)
	payload2 := bytes.Repeat([]byte{0x42}, 4096)

	refs1, err := store.AppendBlocks(ctx, [][]byte{payload1})
	require.NoError(t, err)
	refs2, err := store.AppendBlocks(ctx, [][]byte{payload2})
	require.NoError(t, err)
	store.ReleaseInFlight(refs2)

	require.ErrorIs(t, store.RetirePack(ctx, 0), ErrInvalidValue)
	require.ErrorIs(t, store.RetirePack(ctx, refs2[0].PackID), ErrInvalid)
	require.ErrorIs(t, store.RetirePack(ctx, refs1[0].PackID), ErrInvalid)

	store.ReleaseInFlight(refs1)

	inspBefore, err := store.GCInspectionSnapshot(ctx)
	require.NoError(t, err)
	require.Contains(t, inspBefore.SealedSizes, refs1[0].PackID)

	require.NoError(t, store.RetirePack(ctx, refs1[0].PackID))

	inspAfter, err := store.GCInspectionSnapshot(ctx)
	require.NoError(t, err)
	require.NotContains(t, inspAfter.SealedSizes, refs1[0].PackID)

	_, sealedSizes, err := store.SealedWatermarkAndSizes(ctx)
	require.NoError(t, err)
	require.NotContains(t, sealedSizes, refs1[0].PackID)

	require.Equal(t, uint64(8192), store.PhysicalBytes())

	var ebr epochTracker
	ebr.init(store.unlinkRetiredPacks)
	pin := ebr.pin()
	gotRetired, err := store.readBlockPinned(pin, refs1[0])
	require.NoError(t, err)
	require.Equal(t, payload1, gotRetired)
	pin.unpin()

	failed, err := store.unlinkRetiredPacks([]uint64{refs1[0].PackID})
	require.NoError(t, err)
	require.Empty(t, failed)
	require.Equal(t, uint64(4096), store.PhysicalBytes())

	failed2, err := store.unlinkRetiredPacks([]uint64{refs1[0].PackID})
	require.NoError(t, err)
	require.Empty(t, failed2)
	require.Equal(t, uint64(4096), store.PhysicalBytes())

	_, statErr := os.Stat(packFilePath(store.dir, refs1[0].PackID))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestEBR_RetireAndDrain_UnlinksImmediatelyWhenUnpinnedAndDefersUntilLastReaderExits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store, err := newPackStore(t.TempDir(), 4096, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	var ebr epochTracker
	ebr.init(store.unlinkRetiredPacks)

	p1 := bytes.Repeat([]byte{0x11}, 4096)
	p2 := bytes.Repeat([]byte{0x22}, 4096)
	p3 := bytes.Repeat([]byte{0x33}, 4096)
	p4 := bytes.Repeat([]byte{0x44}, 4096)

	refs1, err := store.AppendBlocks(ctx, [][]byte{p1})
	require.NoError(t, err)
	store.ReleaseInFlight(refs1)
	refs2, err := store.AppendBlocks(ctx, [][]byte{p2})
	require.NoError(t, err)
	store.ReleaseInFlight(refs2)
	refs3, err := store.AppendBlocks(ctx, [][]byte{p3})
	require.NoError(t, err)
	store.ReleaseInFlight(refs3)
	refs4, err := store.AppendBlocks(ctx, [][]byte{p4})
	require.NoError(t, err)
	store.ReleaseInFlight(refs4)

	require.NoError(t, store.RetirePack(ctx, refs1[0].PackID))
	require.NoError(t, ebr.retire(refs1[0].PackID))
	_, statErr := os.Stat(packFilePath(store.dir, refs1[0].PackID))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	require.Equal(t, uint64(3*4096), store.PhysicalBytes())

	pinOld := ebr.pin()
	require.NoError(t, store.RetirePack(ctx, refs2[0].PackID))
	require.NoError(t, ebr.retire(refs2[0].PackID))

	pinMid := ebr.pin()
	require.NoError(t, store.RetirePack(ctx, refs3[0].PackID))
	require.NoError(t, ebr.retire(refs3[0].PackID))

	got2, err := store.readBlockPinned(pinOld, refs2[0])
	require.NoError(t, err)
	require.Equal(t, p2, got2)

	got3, err := store.readBlockPinned(pinMid, refs3[0])
	require.NoError(t, err)
	require.Equal(t, p3, got3)

	require.Equal(t, uint64(3*4096), store.PhysicalBytes())

	pinMid.unpin()
	_, statErr = os.Stat(packFilePath(store.dir, refs2[0].PackID))
	require.NoError(t, statErr)
	_, statErr = os.Stat(packFilePath(store.dir, refs3[0].PackID))
	require.NoError(t, statErr)
	require.Equal(t, uint64(3*4096), store.PhysicalBytes())

	got2Again, err := store.readBlockPinned(pinOld, refs2[0])
	require.NoError(t, err)
	require.Equal(t, p2, got2Again)

	pinOld.unpin()
	_, statErr = os.Stat(packFilePath(store.dir, refs2[0].PackID))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	_, statErr = os.Stat(packFilePath(store.dir, refs3[0].PackID))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	require.Equal(t, uint64(4096), store.PhysicalBytes())

	pinAfter := ebr.pin()
	_, err = store.readBlockPinned(pinAfter, refs2[0])
	require.ErrorIs(t, err, ErrNotFound)
	got4, err := store.readBlockPinned(pinAfter, refs4[0])
	require.NoError(t, err)
	require.Equal(t, p4, got4)
	pinAfter.unpin()
}

func TestPackStore_Close_FlushesDirtyPacksAndRejectsSubsequentOps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("clean close", func(t *testing.T) {
		t.Parallel()
		store, err := newPackStore(t.TempDir(), 4096, nil)
		require.NoError(t, err)

		refs1, err := store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0x01}, 4096)})
		require.NoError(t, err)
		store.ReleaseInFlight(refs1)

		refs2, err := store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0x02}, 4096)})
		require.NoError(t, err)
		store.ReleaseInFlight(refs2)

		var syncedPacks []uint64
		var syncedDir bool
		store.syncFileHook = func(packID uint64) error {
			syncedPacks = append(syncedPacks, packID)
			return nil
		}
		store.syncDirHook = func(_ string) error {
			syncedDir = true
			return nil
		}

		require.NoError(t, store.Close())
		require.Equal(t, []uint64{1, 2}, syncedPacks)
		require.True(t, syncedDir)
		require.False(t, store.hasDirty())
		require.NoError(t, store.Close())
		require.NoError(t, store.SyncActive(ctx))

		_, err = store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0x03}, 4096)})
		require.ErrorIs(t, err, ErrClosed)
		require.ErrorIs(t, store.RetirePack(ctx, 1), ErrClosed)
		require.ErrorIs(t, store.SealActive(ctx), ErrClosed)
		_, err = store.SealActiveIf(ctx, 2)
		require.ErrorIs(t, err, ErrClosed)
		_, err = store.SealedWatermark(ctx)
		require.ErrorIs(t, err, ErrClosed)
		_, err = store.GCInspectionSnapshot(ctx)
		require.ErrorIs(t, err, ErrClosed)
	})

	t.Run("close flush error propagates to SyncActive", func(t *testing.T) {
		t.Parallel()
		store, err := newPackStore(t.TempDir(), 4096, nil)
		require.NoError(t, err)

		refs, err := store.AppendBlocks(ctx, [][]byte{bytes.Repeat([]byte{0x09}, 4096)})
		require.NoError(t, err)
		store.ReleaseInFlight(refs)

		closeSyncErr := errors.New("injected close flush error")
		store.syncFileHook = func(_ uint64) error {
			return closeSyncErr
		}

		require.ErrorIs(t, store.Close(), closeSyncErr)
		require.True(t, store.hasDirty())
		require.ErrorIs(t, store.SyncActive(ctx), closeSyncErr)
	})
}
