package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func newTestErrorInfo(t *testing.T, domain, reason string) *connect.Error {
	t.Helper()
	err := connect.NewError(connect.CodeUnavailable, errors.New("storage failure"))
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: domain,
		Reason: reason,
	})
	require.NoError(t, detailErr)
	err.AddDetail(detail)
	return err
}

func TestIsIdempotentProcedure_GivenWhitelistAndMutatingProcedures_ReturnsExpectedClassification(t *testing.T) {

	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceGetDiskProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceListDisksProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceLookupProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceGetAttrProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadlinkProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceFsyncProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceFsyncDirProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadDirProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadDirPlusProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceGetXattrProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceListXattrProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceStatFSProcedure))

	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceDiffDiskProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadCommitNodeProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceRenewMountProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceUnmountDiskProcedure))

	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceCreateDiskProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceForkDiskProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceMergeDiskProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceCreateProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceCreateTmpfileProcedure), "CreateTmpfile is non-idempotent")
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceWriteProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceUnlinkProcedure))
}

func TestRetry_GivenDisabledConfig_WhenUnavailableOccurs_DoesNotRetry(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("unreachable")}), nil
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			Disable: true,
		},
	}, clk)
	require.NoError(t, err)

	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	require.Error(t, err)
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenIdempotentRead_WhenTransientFailuresRecover_RetriesWithBoundedBackoffAndSucceeds(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient-1"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient-2"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient-3"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("recovered")}), nil
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     250 * time.Millisecond,
			MaxElapsedTime:  5 * time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	resp, err := client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 9}))

	require.NoError(t, err)
	payload := TakeReadPayload(resp.Msg)
	require.Equal(t, []byte("recovered"), payload.Data)
	payload.Release()
	require.Equal(t, 4, handler.calls)
	require.Equal(t, []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		250 * time.Millisecond,
	}, clk.Sleeps())
}

func TestRetry_GivenJitterRandZero_WhenRetrying_AppliesHalfIntervalFloor(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     1 * time.Second,
			MaxElapsedTime:  2 * time.Second,
			RandFloat64:     func() float64 { return 0.0 },
		},
	}, clk)
	require.NoError(t, err)

	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	require.NoError(t, err)
	require.Equal(t, []time.Duration{50 * time.Millisecond}, clk.Sleeps())
}

func TestRetry_GivenIdempotentRead_WhenBudgetExhausted_WrapsDeadlineExceededAndMapsEIO(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	reg := prometheus.NewRegistry()
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("still down"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		Metrics:    rpcMetrics,
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     250 * time.Millisecond,
			MaxElapsedTime:  350 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	require.Error(t, err)
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
	require.Equal(t, syscall.EIO, MapError(err))
	require.Equal(t, 3, handler.calls)
	require.Equal(t, []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
	}, clk.Sleeps())

	mfs, gatherErr := reg.Gather()
	require.NoError(t, gatherErr)
	proc := storagev1connect.StorageServiceReadProcedure
	require.InDelta(t, 2.0, counterValueForProcedure(mfs, "bowl_client_rpc_retries_total", proc), 1e-9)
	require.InDelta(t, 1.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9)
}

func TestRetry_GivenBusinessErrorInfo_WhenUnavailableCodeSet_NeverRetries(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, newTestErrorInfo(t, "bowl.storage", "ENOENT")
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
	}, clk)
	require.NoError(t, err)

	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	require.Error(t, err)
	require.Equal(t, syscall.ENOENT, MapError(err))
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenEmptyReasonErrorInfo_WhenUnavailable_FallsBackToRetry(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, newTestErrorInfo(t, "bowl.storage", "")
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			RandFloat64: func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	resp, err := client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	require.NoError(t, err)
	payload := TakeReadPayload(resp.Msg)
	require.Equal(t, []byte("ok"), payload.Data)
	payload.Release()
	require.Equal(t, 2, handler.calls)
}

func TestRetry_GivenTerminalConnectCode_WhenIdempotentRead_NeverRetries(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("denied"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
	}, clk)
	require.NoError(t, err)

	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	require.Error(t, err)
	require.Equal(t, syscall.EACCES, MapError(err))
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenUnauthenticatedCode_WhenIdempotentRead_NeverRetriesAndMapsEACCES(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing token"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
	}, clk)
	require.NoError(t, err)

	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
	require.Equal(t, syscall.EACCES, MapError(err))
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenNonIdempotentWrite_WhenUnavailableOrTransportEOF_DoesNotRetry(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("mid-flight drop"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
	}, clk)
	require.NoError(t, err)

	_, err = client.Write(context.Background(), connect.NewRequest(&storagev1.WriteRequest{Ino: 1, Data: []byte("x")}))

	require.Error(t, err)
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenNonIdempotentWrite_WhenDialConnectionRefused_RetriesAndSucceeds(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{}
	innerClient := newInMemoryHTTPClient(handler)
	var attempt int
	dialErrClient := &httpClientWithFirstErr{
		firstErr: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		next:     innerClient.Transport,
		attempts: &attempt,
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: dialErrClient.HTTPClient(),
		RetryConfig: &RetryConfig{
			InitialInterval: 80 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	resp, err := client.Write(context.Background(), connect.NewRequest(&storagev1.WriteRequest{Ino: 1, Data: []byte("abc")}))

	require.NoError(t, err)
	require.Equal(t, uint32(3), resp.Msg.GetSize())
	require.Equal(t, 2, attempt)
	require.Equal(t, []time.Duration{80 * time.Millisecond}, clk.Sleeps())
}

func TestRetry_GivenIdempotentRead_WhenTransportEOFAndReset_RetriesAndSucceeds(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{}
	innerClient := newInMemoryHTTPClient(handler)
	transport := &scriptedTransport{
		errs: []error{
			io.ErrUnexpectedEOF,
			&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
		},
		next: innerClient.Transport,
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: transport.HTTPClient(),
		RetryConfig: &RetryConfig{
			InitialInterval: 50 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     500 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	resp, err := client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	require.NoError(t, err)
	payload := TakeReadPayload(resp.Msg)
	require.Equal(t, []byte("ok"), payload.Data)
	payload.Release()
	require.Equal(t, 3, transport.calls)
	require.Equal(t, []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}, clk.Sleeps())
}

func TestRetry_GivenParentContextCanceledDuringBackoff_StopsImmediatelyWithCanceled(t *testing.T) {

	clk := newManualClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxElapsedTime:  10 * time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	parentCtx, cancelParent := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		_, callErr := client.Read(parentCtx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))
		errCh <- callErr
	}()

	armedDuration := <-clk.TimerArmed()
	require.Equal(t, 100*time.Millisecond, armedDuration)
	cancelParent()
	gotErr := <-errCh

	require.ErrorIs(t, gotErr, context.Canceled)
	require.Equal(t, syscall.EINTR, MapError(gotErr))
	require.Equal(t, 1, handler.calls)
}

func TestRetry_GivenSyscallConnectError_ShouldRetryEvenForNonIdempotentProcedure(t *testing.T) {

	connectErr := fmt.Errorf("connect failed: %w", &os.SyscallError{Syscall: "connect", Err: errors.New("refused")})
	require.True(t, shouldRetry(storagev1connect.StorageServiceCreateDiskProcedure, connectErr))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeInvalidArgument, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeNotFound, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeAlreadyExists, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeFailedPrecondition, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeResourceExhausted, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeAborted, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeUnimplemented, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeUnauthenticated, io.ErrUnexpectedEOF)))
	require.True(t, isNeverRetryCode(connect.CodeUnauthenticated))
}

type scriptedDiffMergeHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	diffCalls   int
	diffErrs    []error
	commitCalls int
	commitErrs  []error
	mergeCalls  int
	mergeErrs   []error
	mergeStep   func(context.Context, *connect.Request[storagev1.MergeDiskRequest]) (*connect.Response[storagev1.MergeDiskResponse], error)
}

func (h *scriptedDiffMergeHandler) DiffDisk(_ context.Context, _ *connect.Request[storagev1.DiffDiskRequest]) (*connect.Response[storagev1.DiffDiskResponse], error) {
	idx := h.diffCalls
	h.diffCalls++
	if idx < len(h.diffErrs) && h.diffErrs[idx] != nil {
		return nil, h.diffErrs[idx]
	}
	return connect.NewResponse(&storagev1.DiffDiskResponse{LcaCommitId: "01950000-0000-7000-8000-000000000099"}), nil
}

func (h *scriptedDiffMergeHandler) ReadCommitNode(_ context.Context, _ *connect.Request[storagev1.ReadCommitNodeRequest]) (*connect.Response[storagev1.ReadCommitNodeResponse], error) {
	idx := h.commitCalls
	h.commitCalls++
	if idx < len(h.commitErrs) && h.commitErrs[idx] != nil {
		return nil, h.commitErrs[idx]
	}
	return connect.NewResponse(&storagev1.ReadCommitNodeResponse{Data: []byte("commit-data"), Eof: true}), nil
}

func (h *scriptedDiffMergeHandler) MergeDisk(ctx context.Context, req *connect.Request[storagev1.MergeDiskRequest]) (*connect.Response[storagev1.MergeDiskResponse], error) {
	idx := h.mergeCalls
	h.mergeCalls++
	if h.mergeStep != nil {
		return h.mergeStep(ctx, req)
	}
	if idx < len(h.mergeErrs) && h.mergeErrs[idx] != nil {
		return nil, h.mergeErrs[idx]
	}
	return connect.NewResponse(&storagev1.MergeDiskResponse{MergeCommitId: "01950000-0000-7000-8000-000000000088"}), nil
}

func TestRetry_GivenDiffAndReadCommitNode_WhenUnavailableRecovers_RetriesAndSucceeds(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedDiffMergeHandler{
		diffErrs: []error{
			connect.NewError(connect.CodeUnavailable, errors.New("diff-transient")),
		},
		commitErrs: []error{
			connect.NewError(connect.CodeUnavailable, errors.New("commit-transient")),
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			InitialInterval: 50 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	diffResp, err := client.DiffDisk(context.Background(), connect.NewRequest(&storagev1.DiffDiskRequest{}))
	require.NoError(t, err)
	require.Equal(t, "01950000-0000-7000-8000-000000000099", diffResp.Msg.GetLcaCommitId())
	require.Equal(t, 2, handler.diffCalls)

	commitResp, err := client.ReadCommitNode(context.Background(), connect.NewRequest(&storagev1.ReadCommitNodeRequest{Ino: 1}))
	require.NoError(t, err)
	require.Equal(t, []byte("commit-data"), commitResp.Msg.GetData())
	require.Equal(t, 2, handler.commitCalls)
	require.Equal(t, []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}, clk.Sleeps())
}

func TestRetry_GivenMergeDisk_WhenUnavailableOrTransportEOF_DoesNotRetryButRetriesDialError(t *testing.T) {

	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedDiffMergeHandler{
		mergeErrs: []error{
			connect.NewError(connect.CodeUnavailable, errors.New("mid-flight")),
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
	}, clk)
	require.NoError(t, err)

	_, err = client.MergeDisk(context.Background(), connect.NewRequest(&storagev1.MergeDiskRequest{}))

	require.Error(t, err)
	require.Equal(t, 1, handler.mergeCalls)
	require.Empty(t, clk.Sleeps())

	innerClient := newInMemoryHTTPClient(&scriptedDiffMergeHandler{})
	transport := &scriptedTransport{
		errs: []error{io.ErrUnexpectedEOF},
		next: innerClient.Transport,
	}
	client2, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: transport.HTTPClient(),
	}, clk)
	require.NoError(t, err)

	_, err = client2.MergeDisk(context.Background(), connect.NewRequest(&storagev1.MergeDiskRequest{}))
	require.Error(t, err)
	require.Equal(t, 1, transport.calls)

	var dialAttempts int
	dialErrClient := &httpClientWithFirstErr{
		firstErr: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		next:     innerClient.Transport,
		attempts: &dialAttempts,
	}
	client3, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: dialErrClient.HTTPClient(),
		RetryConfig: &RetryConfig{
			InitialInterval: 40 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	mergeResp, err := client3.MergeDisk(context.Background(), connect.NewRequest(&storagev1.MergeDiskRequest{}))
	require.NoError(t, err)
	require.Equal(t, "01950000-0000-7000-8000-000000000088", mergeResp.Msg.GetMergeCommitId())
	require.Equal(t, 2, dialAttempts)
	require.Equal(t, []time.Duration{40 * time.Millisecond}, clk.Sleeps())
}

func TestRetry_GivenCallerDeadlineLongerThanMaxElapsedTime_WhenFirstAttemptTakes35s_SucceedsWithoutRetryOrExhaustedMetrics(t *testing.T) {

	start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	clk := newAutoAdvanceClock(start)
	reg := prometheus.NewRegistry()
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	handler := &scriptedDiffMergeHandler{
		mergeStep: func(ctx context.Context, _ *connect.Request[storagev1.MergeDiskRequest]) (*connect.Response[storagev1.MergeDiskResponse], error) {
			clk.Advance(35 * time.Second)
			if err := ctx.Err(); err != nil {
				return nil, connect.NewError(connect.CodeDeadlineExceeded, err)
			}
			return connect.NewResponse(&storagev1.MergeDiskResponse{
				MergeCommitId: "01950000-0000-7000-8000-000000000088",
			}), nil
		},
	}

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		Logger:     logger,
		Metrics:    rpcMetrics,
		RetryConfig: &RetryConfig{
			MaxElapsedTime: 30 * time.Second,
		},
	}, clk)
	require.NoError(t, err)

	callerCtx, cancel := clk.WithDeadline(context.Background(), start.Add(60*time.Second))
	defer cancel()

	resp, err := client.MergeDisk(callerCtx, connect.NewRequest(&storagev1.MergeDiskRequest{}))

	require.NoError(t, err, "scene=caller_60s_deadline_35s_merge want=nil got=%v", err)
	require.Equal(t, "01950000-0000-7000-8000-000000000088", resp.Msg.GetMergeCommitId())
	require.Equal(t, 1, handler.mergeCalls)
	require.Empty(t, clk.Sleeps())

	mfs, gatherErr := reg.Gather()
	require.NoError(t, gatherErr)
	proc := storagev1connect.StorageServiceMergeDiskProcedure
	require.InDelta(t, 0.0, counterValueForProcedure(mfs, "bowl_client_rpc_retries_total", proc), 1e-9)
	require.InDelta(t, 0.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9)
	require.Equal(t, uint64(1), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "ok"))
	require.NotContains(t, logBuf.String(), "rpc_retry_exhausted_eio")
}

func TestRetry_GivenSlowFirstAttemptWithoutRetryableErrors_WhenDeadlineExpires_ReturnsTimeoutWithoutRetryExhaustedMetrics(t *testing.T) {
	t.Run("default MaxElapsedTime slow first attempt", func(t *testing.T) {

		start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
		clk := newAutoAdvanceClock(start)
		reg := prometheus.NewRegistry()
		rpcMetrics := NewMetrics()
		require.NoError(t, reg.Register(rpcMetrics))

		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		handler := &scriptedReadHandler{
			steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
				func(ctx context.Context, _ *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
					clk.Advance(31 * time.Second)
					return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())
				},
			},
		}

		client, err := newClientWithClock(ClientConfig{
			Endpoint:   "http://127.0.0.1:9000",
			HTTPClient: newInMemoryHTTPClient(handler),
			Logger:     logger,
			Metrics:    rpcMetrics,
			RetryConfig: &RetryConfig{
				MaxElapsedTime: 30 * time.Second,
			},
		}, clk)
		require.NoError(t, err)

		_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

		require.Error(t, err)
		require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
		require.Equal(t, syscall.EIO, MapError(err))
		require.Equal(t, 1, handler.calls)
		require.Empty(t, clk.Sleeps())

		mfs, gatherErr := reg.Gather()
		require.NoError(t, gatherErr)
		proc := storagev1connect.StorageServiceReadProcedure
		require.InDelta(t, 0.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9,
			"scene=slow_first_attempt_default_deadline want=0_retry_exhausted")
		require.Equal(t, uint64(1), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "deadline_exceeded"))
		require.Equal(t, uint64(0), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "unknown"))
		require.NotContains(t, logBuf.String(), "rpc_retry_exhausted_eio")
	})

	t.Run("caller 60s deadline slow first MergeDisk attempt", func(t *testing.T) {

		start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
		clk := newAutoAdvanceClock(start)
		reg := prometheus.NewRegistry()
		rpcMetrics := NewMetrics()
		require.NoError(t, reg.Register(rpcMetrics))

		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		handler := &scriptedDiffMergeHandler{
			mergeStep: func(ctx context.Context, _ *connect.Request[storagev1.MergeDiskRequest]) (*connect.Response[storagev1.MergeDiskResponse], error) {
				clk.Advance(61 * time.Second)
				return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())
			},
		}

		client, err := newClientWithClock(ClientConfig{
			Endpoint:   "http://127.0.0.1:9000",
			HTTPClient: newInMemoryHTTPClient(handler),
			Logger:     logger,
			Metrics:    rpcMetrics,
			RetryConfig: &RetryConfig{
				MaxElapsedTime: 30 * time.Second,
			},
		}, clk)
		require.NoError(t, err)

		callerCtx, cancel := clk.WithDeadline(context.Background(), start.Add(60*time.Second))
		defer cancel()

		_, err = client.MergeDisk(callerCtx, connect.NewRequest(&storagev1.MergeDiskRequest{}))

		require.Error(t, err)
		require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
		require.Equal(t, syscall.EIO, MapError(err))
		require.Equal(t, 1, handler.mergeCalls)

		mfs, gatherErr := reg.Gather()
		require.NoError(t, gatherErr)
		proc := storagev1connect.StorageServiceMergeDiskProcedure
		require.InDelta(t, 0.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9)
		require.Equal(t, uint64(1), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "deadline_exceeded"))
		require.NotContains(t, logBuf.String(), "rpc_retry_exhausted_eio")
	})

	t.Run("already expired caller deadline before first attempt", func(t *testing.T) {

		start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
		clk := newAutoAdvanceClock(start)
		reg := prometheus.NewRegistry()
		rpcMetrics := NewMetrics()
		require.NoError(t, reg.Register(rpcMetrics))

		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		handler := &scriptedReadHandler{}

		client, err := newClientWithClock(ClientConfig{
			Endpoint:   "http://127.0.0.1:9000",
			HTTPClient: newInMemoryHTTPClient(handler),
			Logger:     logger,
			Metrics:    rpcMetrics,
		}, clk)
		require.NoError(t, err)

		callerCtx, cancel := clk.WithDeadline(context.Background(), start.Add(-1*time.Second))
		defer cancel()

		_, err = client.Read(callerCtx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, syscall.EIO, MapError(err))
		require.Equal(t, 0, handler.calls)

		mfs, gatherErr := reg.Gather()
		require.NoError(t, gatherErr)
		proc := storagev1connect.StorageServiceReadProcedure
		require.InDelta(t, 0.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9)
		require.Equal(t, uint64(1), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "deadline_exceeded"))
		require.Equal(t, uint64(0), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "unknown"))
		require.NotContains(t, logBuf.String(), "rpc_retry_exhausted_eio")
	})
}

func TestRetry_GivenParentDeadlineAndRetryableFailure_WhenDeadlineExpiresDuringNextAttempt_ExhaustsBudgetWithDeadlineExceededCode(t *testing.T) {

	start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	clk := newAutoAdvanceClock(start)
	reg := prometheus.NewRegistry()
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("backend-down"))
			},
			func(ctx context.Context, _ *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				clk.Advance(30 * time.Second)
				return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())
			},
		},
	}

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		Logger:     logger,
		Metrics:    rpcMetrics,
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxElapsedTime:  30 * time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	parentCtx, cancel := clk.WithDeadline(context.Background(), start.Add(30*time.Second))
	defer cancel()

	_, err = client.Read(parentCtx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	require.Error(t, err)
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
	require.Equal(t, syscall.EIO, MapError(err))
	require.Contains(t, err.Error(), "backend-down")
	require.Equal(t, 2, handler.calls)

	mfs, gatherErr := reg.Gather()
	require.NoError(t, gatherErr)
	proc := storagev1connect.StorageServiceReadProcedure
	require.InDelta(t, 1.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9,
		"scene=parent_deadline_during_next want=1_retry_exhausted")
	require.Equal(t, uint64(1), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "deadline_exceeded"))
	require.Equal(t, uint64(0), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "unknown"))
	require.Contains(t, logBuf.String(), `"msg":"rpc_retry_exhausted_eio"`)
	require.Contains(t, logBuf.String(), "backend-down")
}

func TestRetry_GivenParentDeadlineAndRetryableFailure_WhenDeadlineExpiresDuringBackoffSleep_ExhaustsBudgetWithDeadlineExceededCode(t *testing.T) {

	start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	clk := newManualClock(start)
	reg := prometheus.NewRegistry()
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("backoff-root-cause"))
			},
		},
	}

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		Logger:     logger,
		Metrics:    rpcMetrics,
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxElapsedTime:  30 * time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	baseCtx, cancelCause := context.WithCancelCause(context.Background())
	defer cancelCause(nil)
	parentCtx := &fakeDeadlineContext{
		Context:  baseCtx,
		deadline: start.Add(200 * time.Millisecond),
	}

	errCh := make(chan error, 1)

	go func() {
		_, callErr := client.Read(parentCtx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))
		errCh <- callErr
	}()

	require.Equal(t, 100*time.Millisecond, <-clk.TimerArmed())
	cancelCause(context.DeadlineExceeded)
	gotErr := <-errCh

	require.Error(t, gotErr)
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(gotErr))
	require.Equal(t, syscall.EIO, MapError(gotErr))
	require.Contains(t, gotErr.Error(), "backoff-root-cause")
	require.Equal(t, 1, handler.calls)

	mfs, gatherErr := reg.Gather()
	require.NoError(t, gatherErr)
	proc := storagev1connect.StorageServiceReadProcedure
	require.InDelta(t, 1.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9,
		"scene=parent_deadline_during_backoff want=1_retry_exhausted")
	require.Equal(t, uint64(1), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "deadline_exceeded"))
	require.Equal(t, uint64(0), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "unknown"))
	require.Contains(t, logBuf.String(), `"msg":"rpc_retry_exhausted_eio"`)
	require.Contains(t, logBuf.String(), "backoff-root-cause")
}

func TestRetry_GivenKernelInterruptedCancelAfterRetryableFailure_DoesNotFireRetryExhausted(t *testing.T) {

	start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	clk := newManualClock(start)
	reg := prometheus.NewRegistry()
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient"))
			},
		},
	}

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		Logger:     logger,
		Metrics:    rpcMetrics,
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxElapsedTime:  30 * time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	deadlineCtx, cancelDeadline := clk.WithDeadline(context.Background(), start.Add(30*time.Second))
	defer cancelDeadline()
	parentCtx, cancelCause := context.WithCancelCause(deadlineCtx)
	defer cancelCause(nil)

	errCh := make(chan error, 1)

	go func() {
		_, callErr := client.Read(parentCtx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))
		errCh <- callErr
	}()

	require.Equal(t, 100*time.Millisecond, <-clk.TimerArmed())
	cancelCause(ErrKernelInterrupted)
	gotErr := <-errCh

	require.ErrorIs(t, gotErr, context.Canceled)
	require.Equal(t, syscall.EINTR, MapError(gotErr))

	mfs, gatherErr := reg.Gather()
	require.NoError(t, gatherErr)
	proc := storagev1connect.StorageServiceReadProcedure
	require.InDelta(t, 0.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9)
	require.Equal(t, uint64(1), histogramCountForProcedureCode(mfs, "bowl_client_rpc_logical_duration_seconds", proc, "canceled"))
	require.NotContains(t, logBuf.String(), "rpc_retry_exhausted_eio")
}

func TestRetry_GivenBackoffExceedingRemainingBudget_WhenRetryableFailureOccurs_ExitsImmediatelyWithoutDeadSleepOrRetryMetric(t *testing.T) {
	t.Run("sleepDuration equals remaining", func(t *testing.T) {

		start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
		clk := newAutoAdvanceClock(start)
		reg := prometheus.NewRegistry()
		rpcMetrics := NewMetrics()
		require.NoError(t, reg.Register(rpcMetrics))

		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		handler := &scriptedReadHandler{
			steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
				func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
					return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable-at-boundary"))
				},
			},
		}

		client, err := newClientWithClock(ClientConfig{
			Endpoint:   "http://127.0.0.1:9000",
			HTTPClient: newInMemoryHTTPClient(handler),
			Logger:     logger,
			Metrics:    rpcMetrics,
			RetryConfig: &RetryConfig{
				InitialInterval: 100 * time.Millisecond,
				MaxElapsedTime:  100 * time.Millisecond,
				RandFloat64:     func() float64 { return 1.0 },
			},
		}, clk)
		require.NoError(t, err)

		_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

		require.Error(t, err)
		require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
		require.Equal(t, syscall.EIO, MapError(err))
		require.Equal(t, 1, handler.calls)
		require.Empty(t, clk.Sleeps(), "must not arm dead sleep when sleepDuration == remaining")

		mfs, gatherErr := reg.Gather()
		require.NoError(t, gatherErr)
		proc := storagev1connect.StorageServiceReadProcedure
		require.InDelta(t, 0.0, counterValueForProcedure(mfs, "bowl_client_rpc_retries_total", proc), 1e-9)
		require.InDelta(t, 1.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9)
		require.NotContains(t, logBuf.String(), "rpc_retry_triggered")
		require.Contains(t, logBuf.String(), `"msg":"rpc_retry_exhausted_eio"`)
	})

	t.Run("caller 60s deadline with 50ms remaining after 59.95s attempt", func(t *testing.T) {

		start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
		clk := newAutoAdvanceClock(start)
		reg := prometheus.NewRegistry()
		rpcMetrics := NewMetrics()
		require.NoError(t, reg.Register(rpcMetrics))

		handler := &scriptedReadHandler{
			steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
				func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
					clk.Advance(59950 * time.Millisecond)
					return nil, connect.NewError(connect.CodeUnavailable, errors.New("late-unavailable"))
				},
			},
		}

		client, err := newClientWithClock(ClientConfig{
			Endpoint:   "http://127.0.0.1:9000",
			HTTPClient: newInMemoryHTTPClient(handler),
			Metrics:    rpcMetrics,
			RetryConfig: &RetryConfig{
				InitialInterval: 100 * time.Millisecond,
				MaxElapsedTime:  30 * time.Second,
				RandFloat64:     func() float64 { return 1.0 },
			},
		}, clk)
		require.NoError(t, err)

		callerCtx, cancel := clk.WithDeadline(context.Background(), start.Add(60*time.Second))
		defer cancel()

		_, err = client.Read(callerCtx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

		require.Error(t, err)
		require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
		require.Equal(t, 1, handler.calls)
		require.Empty(t, clk.Sleeps())

		mfs, gatherErr := reg.Gather()
		require.NoError(t, gatherErr)
		proc := storagev1connect.StorageServiceReadProcedure
		require.InDelta(t, 0.0, counterValueForProcedure(mfs, "bowl_client_rpc_retries_total", proc), 1e-9)
		require.InDelta(t, 1.0, counterValueForProcedure(mfs, "bowl_client_rpc_retry_exhausted_total", proc), 1e-9)
	})

	t.Run("caller 60s deadline continues retrying past 30s MaxElapsedTime when budget remains", func(t *testing.T) {

		start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
		clk := newAutoAdvanceClock(start)
		handler := &scriptedReadHandler{
			steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
				func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
					clk.Advance(35 * time.Second)
					return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient-at-35s"))
				},
				func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
					return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
				},
			},
		}

		client, err := newClientWithClock(ClientConfig{
			Endpoint:   "http://127.0.0.1:9000",
			HTTPClient: newInMemoryHTTPClient(handler),
			RetryConfig: &RetryConfig{
				InitialInterval: 100 * time.Millisecond,
				MaxElapsedTime:  30 * time.Second,
				RandFloat64:     func() float64 { return 1.0 },
			},
		}, clk)
		require.NoError(t, err)

		callerCtx, cancel := clk.WithDeadline(context.Background(), start.Add(60*time.Second))
		defer cancel()

		resp, err := client.Read(callerCtx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

		require.NoError(t, err)
		payload := TakeReadPayload(resp.Msg)
		require.Equal(t, []byte("ok"), payload.Data)
		payload.Release()
		require.Equal(t, 2, handler.calls)
		require.Equal(t, []time.Duration{100 * time.Millisecond}, clk.Sleeps())
	})
}
