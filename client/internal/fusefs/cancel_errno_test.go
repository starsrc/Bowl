package fusefs_test

import (
	"context"
	"errors"
	"syscall"
	"testing"

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
