package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type customGRPCStatusErr struct {
	st *status.Status
}

func (e customGRPCStatusErr) Error() string {
	return e.st.Message()
}

func (e customGRPCStatusErr) GRPCStatus() *status.Status {
	return e.st
}

func TestStatusToConnectError_MapsAll17GRPCCodesAndPreservesDetails(t *testing.T) {
	require.NoError(t, StatusToConnectError(nil))
	require.NoError(t, StatusToConnectError(status.New(codes.OK, "ok")))

	cases := []struct {
		grpcCode    codes.Code
		connectCode connect.Code
	}{
		{grpcCode: codes.Canceled, connectCode: connect.CodeCanceled},
		{grpcCode: codes.Unknown, connectCode: connect.CodeUnknown},
		{grpcCode: codes.InvalidArgument, connectCode: connect.CodeInvalidArgument},
		{grpcCode: codes.DeadlineExceeded, connectCode: connect.CodeDeadlineExceeded},
		{grpcCode: codes.NotFound, connectCode: connect.CodeNotFound},
		{grpcCode: codes.AlreadyExists, connectCode: connect.CodeAlreadyExists},
		{grpcCode: codes.PermissionDenied, connectCode: connect.CodePermissionDenied},
		{grpcCode: codes.ResourceExhausted, connectCode: connect.CodeResourceExhausted},
		{grpcCode: codes.FailedPrecondition, connectCode: connect.CodeFailedPrecondition},
		{grpcCode: codes.Aborted, connectCode: connect.CodeAborted},
		{grpcCode: codes.OutOfRange, connectCode: connect.CodeOutOfRange},
		{grpcCode: codes.Unimplemented, connectCode: connect.CodeUnimplemented},
		{grpcCode: codes.Internal, connectCode: connect.CodeInternal},
		{grpcCode: codes.Unavailable, connectCode: connect.CodeUnavailable},
		{grpcCode: codes.DataLoss, connectCode: connect.CodeDataLoss},
		{grpcCode: codes.Unauthenticated, connectCode: connect.CodeUnauthenticated},
	}

	for _, tc := range cases {
		t.Run(tc.grpcCode.String(), func(t *testing.T) {
			st, err := status.New(tc.grpcCode, "boom").WithDetails(&errdetails.ErrorInfo{
				Reason: "EBUSY",
				Domain: ErrorDomain,
			})
			require.NoError(t, err)

			cErr := StatusToConnectError(st)
			requireConnectErr(t, cErr, tc.connectCode, "EBUSY")
		})
	}
}

func TestExtractErrorInfo_SupportsConnectGRPCChainAndCustomGRPCStatus(t *testing.T) {
	require.Nil(t, ExtractErrorInfo(nil))
	require.Nil(t, ExtractErrorInfo(errors.New("plain")))

	st, err := status.New(codes.NotFound, "missing").WithDetails(&errdetails.ErrorInfo{
		Reason: "ENOENT",
		Domain: ErrorDomain,
	})
	require.NoError(t, err)

	wrappedConnect := fmt.Errorf("wrap connect: %w", StatusToConnectError(st))
	gotConnect := ExtractErrorInfo(wrappedConnect)
	require.NotNil(t, gotConnect)
	require.Equal(t, "ENOENT", gotConnect.GetReason())

	wrappedGRPC := fmt.Errorf("wrap grpc: %w", st.Err())
	gotGRPC := ExtractErrorInfo(wrappedGRPC)
	require.NotNil(t, gotGRPC)
	require.Equal(t, "ENOENT", gotGRPC.GetReason())

	customWrapped := fmt.Errorf("wrap custom: %w", customGRPCStatusErr{st: st})
	gotCustom := ExtractErrorInfo(customWrapped)
	require.NotNil(t, gotCustom)
	require.Equal(t, "ENOENT", gotCustom.GetReason())
}

func TestErrorReasonInterceptor_RecordsSpanAttributeAndInjectsTraceparent(t *testing.T) {
	installOTelCleanup(t)
	spanRec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(spanRec),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	st, err := status.New(codes.FailedPrecondition, "stale").WithDetails(&errdetails.ErrorInfo{
		Reason: "EBUSY",
		Domain: ErrorDomain,
	})
	require.NoError(t, err)

	ctx, span := tp.Tracer("test").Start(context.Background(), "test-rpc")
	wrapped := (errorReasonInterceptor{}).WrapUnary(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return nil, StatusToConnectError(st)
	})
	_, callErr := wrapped(ctx, connect.NewRequest(&storagev1.GetAttrRequest{}))
	span.End()

	var cErr *connect.Error
	require.ErrorAs(t, callErr, &cErr)
	require.NotEmpty(t, cErr.Meta().Get("traceparent"))

	ended := spanRec.Ended()
	require.Len(t, ended, 1)
	require.Contains(t, ended[0].Attributes(), attribute.String(attrKeyRPCErrorReason, "EBUSY"))
}
