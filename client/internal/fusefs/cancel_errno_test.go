package fusefs_test

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func newStorageErrorInfo(t *testing.T, code connect.Code, reason, msg string) *connect.Error {
	t.Helper()
	err := connect.NewError(code, errors.New(msg))
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: "bowl.storage",
		Reason: reason,
	})
	require.NoError(t, detailErr)
	err.AddDetail(detail)
	return err
}

func TestRawFS_GivenCancelClosingDuringFastRPCWithinGrace_PreservesRealErrnoAndOK(t *testing.T) {
	t.Run("ENOENT preserved when cancel closes during fast Lookup", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		cancelCh := make(chan struct{})
		srv.lookupHook = func(_ context.Context, _ *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			close(cancelCh)
			return nil, newStorageErrorInfo(t, connect.CodeNotFound, "ENOENT", "missing entry")
		}
		fs := newTestRawFS(t, srv)

		var out fuse.EntryOut
		st := fs.Lookup(cancelCh, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "absent", &out)

		requireStatus(t, "fast Lookup ENOENT under cancel", fuse.ENOENT, st)
	})

	t.Run("ENOSPC preserved when cancel closes during fast Mknod", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		cancelCh := make(chan struct{})
		srv.mknodHook = func(_ context.Context, _ *storagev1.MknodRequest) error {
			close(cancelCh)
			return newStorageErrorInfo(t, connect.CodeResourceExhausted, "ENOSPC", "disk full")
		}
		fs := newTestRawFS(t, srv)

		var out fuse.EntryOut
		st := fs.Mknod(cancelCh, &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     syscall.S_IFREG | 0o644,
		}, "full.bin", &out)

		requireStatus(t, "fast Mknod ENOSPC under cancel", fuse.Status(syscall.ENOSPC), st)
	})

	t.Run("stale-epoch EBUSY preserved and fences RawFS when cancel closes during fast SetAttr", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		ino := srv.seedFile(1, "fenced.txt", 0o644, []byte("abc"))
		cancelCh := make(chan struct{})
		srv.setAttrHook = func(_ context.Context, _ *storagev1.SetAttrRequest) error {
			close(cancelCh)
			return newStorageErrorInfo(t, connect.CodeFailedPrecondition, "EBUSY", "stale writer epoch")
		}
		fs := newTestRawFS(t, srv)

		var out fuse.AttrOut
		st := fs.SetAttr(cancelCh, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: ino},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o600,
			},
		}, &out)

		requireStatus(t, "fast SetAttr stale-epoch EBUSY under cancel", fuse.EBUSY, st)
		require.True(t, fs.IsFenced())
		require.Equal(t, fuse.EBUSY, fs.FenceStatus())
	})

	t.Run("nil error returns OK when cancel closes during fast Lookup", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		ino := srv.seedFile(1, "fast.txt", 0o644, []byte("ok"))
		cancelCh := make(chan struct{})
		srv.lookupHook = func(_ context.Context, _ *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			close(cancelCh)
			return nil, nil
		}
		fs := newTestRawFS(t, srv)

		var out fuse.EntryOut
		st := fs.Lookup(cancelCh, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "fast.txt", &out)

		requireStatus(t, "fast Lookup OK under cancel", fuse.OK, st)
		require.Equal(t, ino, out.NodeId)
	})
}

func TestRawFS_GivenPreClosedOrGraceExpiredCancel_ReturnsEINTR(t *testing.T) {
	t.Run("pre-closed cancel immediately cancels context with ErrKernelInterrupted and returns EINTR", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		fs := newTestRawFS(t, srv)
		preClosed := newClosedCancelChan()

		ctx, cancelFunc := fs.RPCContext(preClosed)
		defer cancelFunc()
		var out fuse.EntryOut
		st := fs.Lookup(preClosed, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "any.txt", &out)

		require.ErrorIs(t, context.Cause(ctx), rpc.ErrKernelInterrupted)
		require.Equal(t, fuse.EINTR, fs.ToStatus(ctx.Err()))
		requireStatus(t, "pre-closed cancel Lookup", fuse.EINTR, st)
	})

	t.Run("cancel exceeding 15ms grace period cancels context with ErrKernelInterrupted and returns EINTR", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		cancelCh := make(chan struct{})
		var sawCause error
		srv.lookupHook = func(ctx context.Context, _ *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			err := awaitBeyondInterruptGrace(ctx, cancelCh)
			sawCause = context.Cause(ctx)
			return nil, err
		}
		fs := newTestRawFS(t, srv)

		var out fuse.EntryOut
		st := fs.Lookup(cancelCh, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "slow.txt", &out)

		requireStatus(t, "grace-expired cancel Lookup", fuse.EINTR, st)
		require.ErrorIs(t, sawCause, rpc.ErrKernelInterrupted)
	})
}

func TestLinkTmp_GivenPreClosedOrMidMknodCancel_CompletesWithoutEINTRAndPromotesFullPayload(t *testing.T) {
	t.Run("GivenStagedTmpfileWithPayload_WhenLinkHasPreClosedCancel_ThenMknodAndPromoteSucceedWithFullPayload", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpOut))
		payload := []byte("staged-tmpfile-preclosed-cancel-payload")
		mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, payload)

		var linkOut fuse.EntryOut
		st := fs.Link(newClosedCancelChan(), &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "preclosed-tmp.bin", &linkOut)

		requireStatus(t, "linkTmp with pre-closed cancel", fuse.OK, st)
		requireStatus(t, "Fsync promoted tmpfile", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
		}))
		require.Equal(t, int64(1), srv.mknodCalls.Load())
		requireBytesMatch(t, "promoted payload after pre-closed cancel", payload, mustReadAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, len(payload)))
		serverIno, mapSt := fs.ToServerIno(tmpOut.NodeId)
		requireStatus(t, "ToServerIno", fuse.OK, mapSt)
		gotBytes, ok := srv.fileBytes(serverIno)
		require.True(t, ok)
		requireBytesMatch(t, "server persisted payload", payload, gotBytes)

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 2)
	})

	t.Run("GivenStagedTmpfileWithPayload_WhenCancelClosesMidMknodBeyondGrace_ThenMknodCompletesAndPromotesFullPayload", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		cancelCh := make(chan struct{})
		var mknodCtxErr error
		srv.mknodHook = func(ctx context.Context, _ *storagev1.MknodRequest) error {
			close(cancelCh)
			time.Sleep(30 * time.Millisecond)
			mknodCtxErr = ctx.Err()
			return mknodCtxErr
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpOut))
		payload := []byte("staged-tmpfile-mid-mknod-cancel-payload")
		mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, payload)

		var linkOut fuse.EntryOut
		st := fs.Link(cancelCh, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "mid-cancel-tmp.bin", &linkOut)

		require.NoError(t, mknodCtxErr, "scene=linktmp_mid_mknod_cancel_detached want=nil got=%v", mknodCtxErr)
		requireStatus(t, "linkTmp with mid-Mknod cancel >15ms", fuse.OK, st)
		requireStatus(t, "Fsync promoted tmpfile", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
		}))
		serverIno, mapSt := fs.ToServerIno(tmpOut.NodeId)
		requireStatus(t, "ToServerIno", fuse.OK, mapSt)
		gotBytes, ok := srv.fileBytes(serverIno)
		require.True(t, ok)
		requireBytesMatch(t, "server persisted payload after mid-Mknod cancel", payload, gotBytes)

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 2)
	})
}
