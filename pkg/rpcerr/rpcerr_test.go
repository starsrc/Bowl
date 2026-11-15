package rpcerr_test

import (
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/pkg/rpcerr"
)

func newConnectErrorWithInfo(t *testing.T, domain, reason string) error {
	t.Helper()
	connErr := connect.NewError(connect.CodeInternal, errors.New("connect failure"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: domain,
		Reason: reason,
	})
	require.NoError(t, err)
	connErr.AddDetail(detail)
	return connErr
}

func newGRPCErrorWithInfo(t *testing.T, domain, reason string) error {
	t.Helper()
	st := status.New(codes.Internal, "grpc failure")
	stWithDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Domain: domain,
		Reason: reason,
	})
	require.NoError(t, err)
	return stWithDetails.Err()
}

func TestExtractReason_GivenInvalidOrForeignErrors_ThenReturnsFalse(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{
			name: "nil_error",
			err:  nil,
		},
		{
			name: "connect_error_without_error_info",
			err:  connect.NewError(connect.CodeNotFound, errors.New("missing")),
		},
		{
			name: "empty_reason",
			err:  newConnectErrorWithInfo(t, rpcerr.Domain, ""),
		},
		{
			name: "empty_domain",
			err:  newConnectErrorWithInfo(t, "", rpcerr.ReasonNotFound),
		},
		{
			name: "foreign_domain",
			err:  newConnectErrorWithInfo(t, "custom.domain", rpcerr.ReasonAlreadyExists),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, ok := rpcerr.ExtractReason(tc.err)
			require.False(t, ok)
			require.Empty(t, reason)
		})
	}
}

func TestExtractReason_GivenMatchingDomain_ThenReturnsReason(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantReason string
	}{
		{
			name:       "connect_error",
			err:        newConnectErrorWithInfo(t, rpcerr.Domain, rpcerr.ReasonReadOnly),
			wantReason: rpcerr.ReasonReadOnly,
		},
		{
			name:       "wrapped_connect_error",
			err:        fmt.Errorf("wrap: %w", newConnectErrorWithInfo(t, rpcerr.Domain, rpcerr.ReasonConflict)),
			wantReason: rpcerr.ReasonConflict,
		},
		{
			name:       "grpc_status_error",
			err:        newGRPCErrorWithInfo(t, rpcerr.Domain, rpcerr.ReasonBusy),
			wantReason: rpcerr.ReasonBusy,
		},
		{
			name:       "wrapped_grpc_status_error",
			err:        fmt.Errorf("wrap: %w", newGRPCErrorWithInfo(t, rpcerr.Domain, rpcerr.ReasonNotFound)),
			wantReason: rpcerr.ReasonNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, ok := rpcerr.ExtractReason(tc.err)
			require.True(t, ok)
			require.Equal(t, tc.wantReason, reason)
		})
	}
}
