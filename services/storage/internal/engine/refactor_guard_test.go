package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingCommitReader struct {
	pebbleReader
	commitPrefix []byte
	commitGets   atomic.Int64
}

func (r *countingCommitReader) Get(key []byte) ([]byte, io.Closer, error) {
	if bytes.HasPrefix(key, r.commitPrefix) {
		r.commitGets.Add(1)
	}
	return r.pebbleReader.Get(key)
}

func TestStep11_SyncWAL_ConcurrentFsyncCoalescing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         t.TempDir(),
		DiskID:      "step11-coalesce",
		BlockSize:   4096,
		MaxPackSize: 1 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "f.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("A"), 4096))
	require.NoError(t, err)

	waveEntered := make(chan struct{})
	releaseWave := make(chan struct{})
	var enterOnce sync.Once
	var walSyncs atomic.Int32

	disk.packs.syncFileHook = func(_ uint64) error {
		enterOnce.Do(func() {
			close(waveEntered)
			<-releaseWave
		})
		return nil
	}
	disk.walCommitHook = func(isSync bool) {
		if isSync {
			walSyncs.Add(1)
		}
	}

	const callers = 32
	errs := make([]error, callers)
	var wg sync.WaitGroup

	wg.Go(func() {
		errs[0] = disk.Fsync(ctx, DefaultBranchName, f.Ino)
	})
	<-waveEntered

	for i := 1; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				errs[idx] = disk.Fsync(ctx, DefaultBranchName, f.Ino)
			} else {
				errs[idx] = disk.FsyncDir(ctx, DefaultBranchName, RootIno)
			}
		}(i)
	}

	time.Sleep(25 * time.Millisecond)
	close(releaseWave)
	wg.Wait()

	for i, e := range errs {
		require.NoError(t, e, "caller %d failed", i)
	}
	require.Equal(t, int32(1), walSyncs.Load())
	require.GreaterOrEqual(t, disk.node.walSyncedGen.Load(), disk.node.walWriteGen.Load())
	require.False(t, disk.packs.hasDirty())
}

func TestStep11_SyncWAL_PackSyncErrorDoesNotAdvanceSyncedGen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step11-sync-err",
		BlockSize: 4096,
	})
	require.NoError(t, err)

	injectedErr := errors.New("injected pack sync failure")
	var failSync atomic.Bool
	t.Cleanup(func() {
		failSync.Store(false)
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "err.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("E"), 4096))
	require.NoError(t, err)

	var walSyncs atomic.Int32
	failSync.Store(true)
	disk.packs.syncFileHook = func(_ uint64) error {
		if failSync.Load() {
			return injectedErr
		}
		return nil
	}
	disk.walCommitHook = func(isSync bool) {
		if isSync {
			walSyncs.Add(1)
		}
	}

	syncedBefore := disk.node.walSyncedGen.Load()
	writeGen := disk.node.walWriteGen.Load()
	require.Less(t, syncedBefore, writeGen)

	const callers = 8
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = disk.Fsync(ctx, DefaultBranchName, f.Ino)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		require.ErrorIs(t, e, injectedErr, "caller %d expected injected error", i)
	}
	require.Equal(t, syncedBefore, disk.node.walSyncedGen.Load())
	require.Zero(t, walSyncs.Load())
	require.True(t, disk.packs.hasDirty())

	failSync.Store(false)
	require.NoError(t, disk.Fsync(ctx, DefaultBranchName, f.Ino))
	require.Equal(t, int32(1), walSyncs.Load())
	require.GreaterOrEqual(t, disk.node.walSyncedGen.Load(), writeGen)
	require.False(t, disk.packs.hasDirty())
}

func TestStep11_SyncWAL_CallerCancelDoesNotAbortPeerWaiters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step11-cancel-isolation",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "cancel.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("C"), 4096))
	require.NoError(t, err)

	waveEntered := make(chan struct{})
	releaseWave := make(chan struct{})
	var enterOnce sync.Once
	disk.packs.syncFileHook = func(_ uint64) error {
		enterOnce.Do(func() {
			close(waveEntered)
			<-releaseWave
		})
		return nil
	}

	leaderCtx, cancelLeader := context.WithCancel(ctx)
	leaderErrCh := make(chan error, 1)
	go func() {
		leaderErrCh <- disk.Fsync(leaderCtx, DefaultBranchName, f.Ino)
	}()
	<-waveEntered

	const peers = 4
	peerErrs := make([]error, peers)
	var peerWG sync.WaitGroup
	for i := range peers {
		peerWG.Add(1)
		go func(idx int) {
			defer peerWG.Done()
			peerErrs[idx] = disk.Fsync(ctx, DefaultBranchName, f.Ino)
		}(i)
	}

	time.Sleep(15 * time.Millisecond)
	cancelLeader()

	select {
	case lErr := <-leaderErrCh:
		require.ErrorIs(t, lErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not return promptly after context cancel")
	}

	close(releaseWave)
	peerWG.Wait()
	for i, e := range peerErrs {
		require.NoError(t, e, "peer %d must succeed despite leader cancel", i)
	}
	require.GreaterOrEqual(t, disk.node.walSyncedGen.Load(), disk.node.walWriteGen.Load())
}

func TestStep11_Fsync_LockFreeAndCleanSkipsSync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step11-lockfree-clean",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "lockfree.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("L"), 4096))
	require.NoError(t, err)

	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	disk.beforeCommitExecHook = func(_ string) error {
		close(commitEntered)
		<-releaseCommit
		return nil
	}

	commitDone := make(chan error, 1)
	go func() {
		_, cErr := disk.Commit(ctx, DefaultBranchName, "hold-branch-mu")
		commitDone <- cErr
	}()
	<-commitEntered

	fsyncDone := make(chan error, 1)
	go func() {
		if sErr := disk.Fsync(ctx, DefaultBranchName, f.Ino); sErr != nil {
			fsyncDone <- sErr
			return
		}
		fsyncDone <- disk.FsyncDir(ctx, DefaultBranchName, RootIno)
	}()

	select {
	case sErr := <-fsyncDone:
		require.NoError(t, sErr)
	case <-time.After(2 * time.Second):
		close(releaseCommit)
		t.Fatal("Fsync/FsyncDir blocked while branch.mu was held")
	}

	close(releaseCommit)
	require.NoError(t, <-commitDone)

	var walSyncs atomic.Int32
	var packSyncs atomic.Int32
	disk.walCommitHook = func(isSync bool) {
		if isSync {
			walSyncs.Add(1)
		}
	}
	disk.packs.syncFileHook = func(_ uint64) error {
		packSyncs.Add(1)
		return nil
	}

	for range 10 {
		require.NoError(t, disk.Fsync(ctx, DefaultBranchName, f.Ino))
		require.NoError(t, disk.FsyncDir(ctx, DefaultBranchName, RootIno))
	}
	require.Zero(t, walSyncs.Load())
	require.Zero(t, packSyncs.Load())
}

func TestStep13_WorkLayerFirstHit_ZeroCommitReadsAndCommitCacheHits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step13-lazy-cache",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	baseFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "base.bin", 0o644, 0, 0)
	require.NoError(t, err)
	basePayload := bytes.Repeat([]byte("B"), 4096)
	_, err = disk.Write(ctx, DefaultBranchName, baseFile.Ino, 0, basePayload)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "base")
	require.NoError(t, err)

	const chainDepth = 48
	for i := range chainDepth {
		name := fmt.Sprintf("c-%02d.txt", i)
		f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, name, 0o644, 0, 0)
		require.NoError(t, cErr)
		_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte(name))
		require.NoError(t, wErr)
		_, cmtErr := disk.Commit(ctx, DefaultBranchName, name)
		require.NoError(t, cmtErr)
	}

	hotFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "hot.bin", 0o644, 0, 0)
	require.NoError(t, err)
	hotPayload := bytes.Repeat([]byte("H"), 4096)
	_, err = disk.Write(ctx, DefaultBranchName, hotFile.Ino, 0, hotPayload)
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, hotFile.Ino, "user.hot", []byte("yes"), 0))
	hotSym, err := disk.Symlink(ctx, DefaultBranchName, RootIno, "hot-link", "/hot.bin", 0, 0)
	require.NoError(t, err)

	disk.commitCacheMu.Lock()
	clear(disk.commitCache)
	disk.commitCacheMu.Unlock()

	_, err = disk.GetAttr(ctx, DefaultBranchName, hotFile.Ino)
	require.NoError(t, err)
	_, _, err = disk.Lookup(ctx, DefaultBranchName, RootIno, "hot.bin")
	require.NoError(t, err)
	gotHot, err := disk.Read(ctx, DefaultBranchName, hotFile.Ino, 0, len(hotPayload))
	require.NoError(t, err)
	require.Equal(t, hotPayload, gotHot)
	gotXattr, err := disk.GetXattr(ctx, DefaultBranchName, hotFile.Ino, "user.hot")
	require.NoError(t, err)
	require.Equal(t, []byte("yes"), gotXattr)
	gotTarget, err := disk.Readlink(ctx, DefaultBranchName, hotSym.Ino)
	require.NoError(t, err)
	require.Equal(t, []byte("/hot.bin"), gotTarget)
	require.NoError(t, disk.Fsync(ctx, DefaultBranchName, hotFile.Ino))
	require.NoError(t, disk.FsyncDir(ctx, DefaultBranchName, RootIno))

	disk.commitCacheMu.RLock()
	cachedAfterWorkHits := len(disk.commitCache)
	disk.commitCacheMu.RUnlock()
	require.Zero(t, cachedAfterWorkHits)

	gotBase, err := disk.Read(ctx, DefaultBranchName, baseFile.Ino, 0, len(basePayload))
	require.NoError(t, err)
	require.Equal(t, basePayload, gotBase)

	disk.commitCacheMu.RLock()
	cachedAfterBaseHit := len(disk.commitCache)
	_, initCached := disk.commitCache[1]
	disk.commitCacheMu.RUnlock()
	require.Equal(t, chainDepth+1, cachedAfterBaseHit)
	require.False(t, initCached)

	_, err = disk.ReadDir(ctx, DefaultBranchName, RootIno)
	require.NoError(t, err)

	snap, snapGen := disk.newReadSnapshot()
	defer func() { _ = snap.Close() }()
	cr := &countingCommitReader{
		pebbleReader: snap,
		commitPrefix: disk.keys.MetaCommitPrefix(),
	}
	_, layers, err := disk.resolveBranchLayersWithGen(cr, DefaultBranchName, snapGen)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(layers), chainDepth+2)
	require.Zero(t, cr.commitGets.Load())
}

func TestStep13_CommitCacheInvalidation_StaleSnapshotAndConcurrentPrune_Race(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step13-invalidation",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "v.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("v1"))
	require.NoError(t, err)
	cBase, err := disk.Commit(ctx, DefaultBranchName, "base")
	require.NoError(t, err)

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("v2"))
	require.NoError(t, err)
	cDelParent, err := disk.Commit(ctx, DefaultBranchName, "del-parent")
	require.NoError(t, err)

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("v3"))
	require.NoError(t, err)
	cDelChild, err := disk.Commit(ctx, DefaultBranchName, "del-child")
	require.NoError(t, err)

	_, err = disk.Reset(ctx, DefaultBranchName, cBase.ID)
	require.NoError(t, err)

	gotV3, err := disk.ReadAtCommit(ctx, cDelChild.ID, f.Ino, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("v3"), gotV3)

	disk.commitCacheMu.Lock()
	clear(disk.commitCache)
	disk.commitCacheMu.Unlock()

	staleSnap, staleGen := disk.newReadSnapshot()
	defer func() { _ = staleSnap.Close() }()

	require.NoError(t, disk.DeleteCommit(ctx, cDelChild.ID))
	require.NoError(t, disk.DeleteCommit(ctx, cDelParent.ID))

	_, err = disk.GetAttrAtCommit(ctx, cDelChild.ID, f.Ino)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = disk.GetAttrAtCommit(ctx, cDelParent.ID, f.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	_, _, err = disk.resolveCommitLayersWithGen(staleSnap, cDelChild.ID, staleGen)
	require.NoError(t, err)

	disk.commitCacheMu.RLock()
	_, childBackfilled := disk.commitCache[cDelChild.ID]
	_, parentBackfilled := disk.commitCache[cDelParent.ID]
	disk.commitCacheMu.RUnlock()
	require.False(t, childBackfilled)
	require.False(t, parentBackfilled)

	var createdIDs []uint64
	for i := range 20 {
		_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte(fmt.Sprintf("r-%d", i)))
		require.NoError(t, wErr)
		c, cErr := disk.Commit(ctx, DefaultBranchName, fmt.Sprintf("r-%d", i))
		require.NoError(t, cErr)
		createdIDs = append(createdIDs, c.ID)
	}
	_, err = disk.Reset(ctx, DefaultBranchName, cBase.ID)
	require.NoError(t, err)

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	readerErrs := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				for _, id := range createdIDs {
					_, gErr := disk.GetAttrAtCommit(ctx, id, f.Ino)
					if gErr != nil && !errors.Is(gErr, ErrNotFound) {
						readerErrs <- gErr
						return
					}
				}
			}
		})
	}

	time.Sleep(10 * time.Millisecond)
	pruned, err := disk.PruneCommits(ctx)
	require.NoError(t, err)
	require.Equal(t, len(createdIDs), pruned)
	close(stopReaders)
	wg.Wait()
	close(readerErrs)
	for rErr := range readerErrs {
		require.NoError(t, rErr)
	}

	disk.commitCacheMu.RLock()
	for _, id := range createdIDs {
		_, present := disk.commitCache[id]
		require.False(t, present, "pruned commit %d must not remain in commitCache", id)
	}
	disk.commitCacheMu.RUnlock()

	for _, id := range createdIDs {
		_, gErr := disk.GetAttrAtCommit(ctx, id, f.Ino)
		require.ErrorIs(t, gErr, ErrNotFound)
	}
}

func TestReadView_Guard_RejectsNilPinAndClosedView(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "readview-guard",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	payload := bytes.Repeat([]byte{0x5c}, 4096)
	refs, err := disk.packs.AppendBlocks(ctx, [][]byte{payload})
	require.NoError(t, err)
	disk.packs.ReleaseInFlight(refs)

	_, err = disk.packs.readBlockPinned(nil, refs[0])
	require.ErrorIs(t, err, ErrInvalid)

	rv := disk.openReadView()
	got, err := rv.readBlock(refs[0])
	require.NoError(t, err)
	require.Equal(t, payload, got)

	require.NoError(t, rv.Close())
	require.NoError(t, rv.Close())

	_, err = rv.readBlock(refs[0])
	require.ErrorIs(t, err, ErrClosed)
}

func isReadViewReadBlockMethod(decl *ast.FuncDecl) bool {
	if decl == nil || decl.Name == nil || decl.Name.Name != "readBlock" || decl.Recv == nil || len(decl.Recv.List) != 1 {
		return false
	}
	star, ok := decl.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	ident, ok := star.X.(*ast.Ident)
	return ok && ident.Name == "readView"
}

func TestReadView_ASTGuard_ForbidsDirectPackBlockReadsOutsideReadViewAndGC(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var (
		pinnedCalls   int
		gcCalls       int
		internalCalls int
		violations    []string
	)

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, parseErr)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			inReadViewReadBlock := name == "read_view.go" && isReadViewReadBlockMethod(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				sel, isSel := call.Fun.(*ast.SelectorExpr)
				if !isSel || sel.Sel == nil {
					return true
				}
				pos := fset.Position(sel.Sel.Pos())
				switch sel.Sel.Name {
				case "ReadBlock":
					violations = append(violations, fmt.Sprintf("%s:%d calls forbidden ReadBlock", name, pos.Line))
				case "readBlockPinned":
					if !inReadViewReadBlock {
						violations = append(violations, fmt.Sprintf("%s:%d calls readBlockPinned outside (*readView).readBlock", name, pos.Line))
					} else {
						pinnedCalls++
					}
				case "readBlockForGC":
					if name != "gc.go" {
						violations = append(violations, fmt.Sprintf("%s:%d calls readBlockForGC outside gc.go", name, pos.Line))
					} else {
						gcCalls++
					}
				case "readBlockInternal":
					if name != "pack_store.go" {
						violations = append(violations, fmt.Sprintf("%s:%d calls readBlockInternal outside pack_store.go", name, pos.Line))
					} else {
						internalCalls++
					}
				}
				return true
			})
		}
	}

	require.Empty(t, violations)
	require.Equal(t, 1, pinnedCalls)
	require.Equal(t, 1, gcCalls)
	require.Equal(t, 2, internalCalls)
}
