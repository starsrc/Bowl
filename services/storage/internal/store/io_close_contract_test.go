package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

func TestIOCloseTruncatedPackColdReadReturnsErrIOAndUnexpectedEOF(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	actor, err := Open(Options{
		Dir:    dir,
		DiskID: "disk-trunc-pack",
	})
	require.NoError(t, err)

	attr, err := actor.Create(ctx, CreateReq{
		ParentIno: RootIno,
		Name:      []byte("payload.bin"),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)

	payload := bytes.Repeat([]byte{0x5a}, 4096)
	written, _, err := actor.Write(ctx, attr.Ino, 0, payload)
	require.NoError(t, err)
	require.Equal(t, uint32(len(payload)), written)
	require.NoError(t, actor.FlushWAL())
	require.NoError(t, actor.Close())

	packFiles, err := filepath.Glob(filepath.Join(dir, "packs", "*.pack"))
	require.NoError(t, err)
	require.Len(t, packFiles, 1)
	require.NoError(t, os.Truncate(packFiles[0], 1))

	reopened, err := Open(Options{
		Dir:    dir,
		DiskID: "disk-trunc-pack",
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reopened.Close())
	}()

	_, readErr := reopened.Read(ctx, attr.Ino, 0, uint32(len(payload)))
	require.Error(t, readErr)
	require.ErrorIs(t, readErr, ErrIO)
	require.ErrorIs(t, readErr, io.ErrUnexpectedEOF)

	newSize := uint64(2048)
	_, truncErr := reopened.SetAttr(ctx, attr.Ino, SetAttrReq{Size: &newSize})
	require.Error(t, truncErr)
	require.ErrorIs(t, truncErr, ErrIO)
	require.ErrorIs(t, truncErr, io.ErrUnexpectedEOF)
}

func TestIOCloseOpenCloneAndInspectOSErrorsReturnErrIO(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("missing directory preserves os.ErrNotExist and ErrIO", func(t *testing.T) {
		t.Parallel()
		missingDir := filepath.Join(t.TempDir(), "missing-branch")

		_, openErr := Open(Options{
			Dir:      missingDir,
			DiskID:   "disk-missing",
			ReadOnly: true,
		})
		require.Error(t, openErr)
		require.ErrorIs(t, openErr, ErrIO)
		require.ErrorIs(t, openErr, os.ErrNotExist)

		cloneErr := CloneCheckpointDir(ctx, missingDir, filepath.Join(t.TempDir(), "dst"))
		require.Error(t, cloneErr)
		require.ErrorIs(t, cloneErr, ErrIO)
		require.ErrorIs(t, cloneErr, os.ErrNotExist)

		_, _, _, inspectErr := InspectBranchStat(missingDir)
		require.Error(t, inspectErr)
		require.ErrorIs(t, inspectErr, ErrIO)
		require.ErrorIs(t, inspectErr, os.ErrNotExist)
	})

	t.Run("non-directory parent or target triggers OS IO wrapped as ErrIO", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()
		blocker := filepath.Join(baseDir, "regular-file")
		require.NoError(t, os.WriteFile(blocker, []byte("not a dir"), 0o600))

		_, openErr := Open(Options{
			Dir:    filepath.Join(blocker, "child"),
			DiskID: "disk-enotdir",
		})
		require.Error(t, openErr)
		require.ErrorIs(t, openErr, ErrIO)

		srcDir := filepath.Join(baseDir, "src")
		srcActor, err := Open(Options{
			Dir:    srcDir,
			DiskID: "disk-src",
		})
		require.NoError(t, err)
		require.NoError(t, srcActor.Close())

		cloneErr := CloneCheckpointDir(ctx, srcDir, filepath.Join(blocker, "dst"))
		require.Error(t, cloneErr)
		require.ErrorIs(t, cloneErr, ErrIO)

		_, _, _, inspectParentErr := InspectBranchStat(filepath.Join(blocker, "child"))
		require.Error(t, inspectParentErr)
		require.ErrorIs(t, inspectParentErr, ErrIO)

		_, _, _, inspectFileErr := InspectBranchStat(blocker)
		require.Error(t, inspectFileErr)
		require.ErrorIs(t, inspectFileErr, ErrIO)
	})
}

func TestIOCloseWrapIOErrContract(t *testing.T) {
	t.Parallel()

	require.NoError(t, wrapIOErr(nil))

	for _, ctxErr := range []error{context.Canceled, context.DeadlineExceeded} {
		got := wrapIOErr(ctxErr)
		require.ErrorIs(t, got, ctxErr)
		require.NotErrorIs(t, got, ErrIO)

		wrapped := fmt.Errorf("rpc ctx: %w", ctxErr)
		gotWrapped := wrapIOErr(wrapped)
		require.ErrorIs(t, gotWrapped, ctxErr)
		require.NotErrorIs(t, gotWrapped, ErrIO)
	}

	sentinels := []error{
		ErrClosed,
		ErrReadOnly,
		ErrNotFound,
		ErrExist,
		ErrNotDir,
		ErrIsDir,
		ErrNotEmpty,
		ErrInvalid,
		ErrInvalidKey,
		ErrInvalidValue,
		ErrPerm,
		ErrNoData,
		ErrNoSpace,
		ErrInodeExhausted,
		ErrNameTooLong,
		ErrTooManyLinks,
		ErrFileTooLarge,
		ErrNotSupported,
		ErrRange,
		ErrIO,
		ErrStaleEpoch,
		ErrQuotaShrinkBelowUsed,
	}
	require.Len(t, sentinels, 22)

	for _, sentinel := range sentinels {
		got := wrapIOErr(sentinel)
		require.ErrorIs(t, got, sentinel)
		if !errors.Is(sentinel, ErrIO) {
			require.NotErrorIs(t, got, ErrIO)
		}

		wrapped := fmt.Errorf("domain context: %w", sentinel)
		gotWrapped := wrapIOErr(wrapped)
		require.ErrorIs(t, gotWrapped, sentinel)
		if !errors.Is(sentinel, ErrIO) {
			require.NotErrorIs(t, gotWrapped, ErrIO)
		}
	}

	conflictErr := &ErrMergeConflict{
		Report: MergeConflictReport{TotalConflicts: 1},
	}
	require.NotErrorIs(t, wrapIOErr(conflictErr), ErrIO)
	var targetConflict *ErrMergeConflict
	require.ErrorAs(t, wrapIOErr(conflictErr), &targetConflict)

	wrappedConflict := fmt.Errorf("merge aborted: %w", conflictErr)
	require.NotErrorIs(t, wrapIOErr(wrappedConflict), ErrIO)
	targetConflict = nil
	require.ErrorAs(t, wrapIOErr(wrappedConflict), &targetConflict)

	for _, rawErr := range []error{
		io.ErrUnexpectedEOF,
		io.EOF,
		os.ErrNotExist,
		os.ErrPermission,
		pebble.ErrNotFound,
		errors.New("raw hardware error"),
	} {
		got := wrapIOErr(rawErr)
		require.ErrorIs(t, got, ErrIO)
		require.ErrorIs(t, got, rawErr)
	}
}

func TestIOCloseExternalAllocatorsAndOpenFnNotWrappedAsErrIO(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("InoAllocator external error passes through untouched", func(t *testing.T) {
		t.Parallel()
		extErr := errors.New("external inode allocator RPC error")
		actor, err := Open(Options{
			Dir:    t.TempDir(),
			DiskID: "disk-ino-alloc",
			InoAllocator: func(_ context.Context, _ uint64) (uint64, uint64, error) {
				return 0, 0, extErr
			},
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, actor.Close())
		}()

		_, createErr := actor.Create(ctx, CreateReq{
			ParentIno: RootIno,
			Name:      []byte("a.txt"),
			Mode:      S_IFREG | 0o644,
		})
		require.ErrorIs(t, createErr, extErr)
		require.NotErrorIs(t, createErr, ErrIO)
	})

	t.Run("PackAllocator external error passes through untouched", func(t *testing.T) {
		t.Parallel()
		extErr := errors.New("external pack allocator RPC error")
		actor, err := Open(Options{
			Dir:    t.TempDir(),
			DiskID: "disk-pack-alloc",
			PackAllocator: func(_ context.Context, _ uint64) (uint64, uint64, error) {
				return 0, 0, extErr
			},
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, actor.Close())
		}()

		attr, createErr := actor.Create(ctx, CreateReq{
			ParentIno: RootIno,
			Name:      []byte("b.txt"),
			Mode:      S_IFREG | 0o644,
		})
		require.NoError(t, createErr)

		_, _, writeErr := actor.Write(ctx, attr.Ino, 0, []byte("hello pack alloc"))
		require.ErrorIs(t, writeErr, extErr)
		require.NotErrorIs(t, writeErr, ErrIO)
	})

	t.Run("SanitizeCheckpoint OpenActor external error passes through untouched", func(t *testing.T) {
		t.Parallel()
		extErr := errors.New("custom open actor failure")
		_, sanitizeErr := SanitizeCheckpoint(ctx, SanitizeOptions{
			Dir:      t.TempDir(),
			CommitID: "commit-openfn",
			OpenActor: func(_ context.Context, _ Options) (*StoreActor, error) {
				return nil, extErr
			},
		})
		require.ErrorIs(t, sanitizeErr, extErr)
		require.NotErrorIs(t, sanitizeErr, ErrIO)
	})

	t.Run("forEachDataKey external callback error passes through untouched", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		actor, err := Open(Options{
			Dir:    dir,
			DiskID: "disk-foreach-cb",
		})
		require.NoError(t, err)
		attr, createErr := actor.Create(ctx, CreateReq{
			ParentIno: RootIno,
			Name:      []byte("c.bin"),
			Mode:      S_IFREG | 0o644,
		})
		require.NoError(t, createErr)
		_, _, writeErr := actor.Write(ctx, attr.Ino, 0, []byte("block-payload"))
		require.NoError(t, writeErr)
		require.NoError(t, actor.FlushWAL())
		defer func() {
			require.NoError(t, actor.Close())
		}()

		cbErr := errors.New("external callback abort")
		gotErr := forEachDataKey(actor.db, attr.Ino, func(_ uint64, _ BlobRef) error {
			return cbErr
		})
		require.ErrorIs(t, gotErr, cbErr)
		require.NotErrorIs(t, gotErr, ErrIO)
	})
}

type failingCloser struct {
	io.Closer
	closeErr error
}

func (c *failingCloser) Close() error {
	origErr := c.Closer.Close()
	if c.closeErr != nil {
		return c.closeErr
	}
	return origErr
}

type failingCloseFile struct {
	vfs.File
	closeErr error
}

func (f *failingCloseFile) Close() error {
	origErr := f.File.Close()
	if f.closeErr != nil {
		return f.closeErr
	}
	return origErr
}

type closeFailFS struct {
	vfs.FS
	matchSubstr string
	closeErr    error
}

func (fs *closeFailFS) Lock(name string) (io.Closer, error) {
	c, err := fs.FS.Lock(name)
	if err != nil {
		return nil, err
	}
	if fs.matchSubstr != "" && bytes.Contains([]byte(name), []byte(fs.matchSubstr)) {
		return &failingCloser{Closer: c, closeErr: fs.closeErr}, nil
	}
	return c, nil
}

func (fs *closeFailFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	if fs.matchSubstr != "" && bytes.Contains([]byte(name), []byte(fs.matchSubstr)) {
		return &failingCloseFile{File: f, closeErr: fs.closeErr}, nil
	}
	return f, nil
}

func (fs *closeFailFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	if fs.matchSubstr != "" && bytes.Contains([]byte(name), []byte(fs.matchSubstr)) {
		return &failingCloseFile{File: f, closeErr: fs.closeErr}, nil
	}
	return f, nil
}

func (fs *closeFailFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	if fs.matchSubstr != "" && bytes.Contains([]byte(newname), []byte(fs.matchSubstr)) {
		return &failingCloseFile{File: f, closeErr: fs.closeErr}, nil
	}
	return f, nil
}

func TestIOCloseActorCloseAggregatesPebbleCloseErrorAsErrIO(t *testing.T) {
	t.Parallel()
	injectErr := errors.New("injected lock file close failure")
	actor, err := Open(Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-close-err",
		pebbleFS: &closeFailFS{FS: vfs.Default, matchSubstr: "LOCK", closeErr: injectErr},
	})
	require.NoError(t, err)

	closeErr := actor.Close()
	require.Error(t, closeErr)
	require.ErrorIs(t, closeErr, ErrIO)
	require.ErrorIs(t, closeErr, injectErr)
}
