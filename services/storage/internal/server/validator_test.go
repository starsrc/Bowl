package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

const validTestDiskUUID = "019440e0-5b9a-7a31-a8d0-1b2c3d4e5f60"
const validTestMountUUID = "019440e0-5b9a-7a31-a8d0-1b2c3d4e5f61"

func TestValidationInterceptor_GivenReadWriteBranchVariants_WhenValidatedUnconditionally_ThenMatchesProtoContract(t *testing.T) {
	v, err := NewStorageValidator()
	require.NoError(t, err)
	interceptor := NewValidationInterceptor(v)
	noopNext := func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}
	wrapped := interceptor(noopNext)

	longBranch := strings.Repeat("a", 256)

	writeCases := []struct {
		name      string
		branch    string
		wantValid bool
	}{
		{name: "valid main branch", branch: "main", wantValid: true},
		{name: "valid max 255 byte branch", branch: strings.Repeat("b", 255), wantValid: true},
		{name: "empty branch rejected", branch: "", wantValid: false},
		{name: "too long branch rejected", branch: longBranch, wantValid: false},
		{name: "NUL byte branch rejected", branch: "feat\x00x", wantValid: false},
	}

	for _, tc := range writeCases {
		t.Run("WriteRequest/"+tc.name, func(t *testing.T) {
			msg := &storagev1.WriteRequest{
				Ino:    2,
				Fh:     1,
				Offset: 0,
				Data:   []byte("abc"),
				DiskId: validTestDiskUUID,
				Epoch:  2,
				Branch: tc.branch,
			}
			_, callErr := wrapped(context.Background(), connect.NewRequest(msg))
			if tc.wantValid {
				require.NoError(t, callErr)
			} else {
				requireConnectErr(t, callErr, connect.CodeInvalidArgument, ReasonInvalidArgument)
			}
		})

		t.Run("ReadRequest/"+tc.name, func(t *testing.T) {
			msg := &storagev1.ReadRequest{
				Ino:    2,
				Fh:     1,
				Offset: 0,
				Size:   64,
				DiskId: validTestDiskUUID,
				Branch: tc.branch,
			}
			_, callErr := wrapped(context.Background(), connect.NewRequest(msg))
			if tc.wantValid {
				require.NoError(t, callErr)
			} else {
				requireConnectErr(t, callErr, connect.CodeInvalidArgument, ReasonInvalidArgument)
			}
		})
	}
}

func TestValidator_Step23AndStep24RPCRequests(t *testing.T) {
	v, err := NewStorageValidator()
	require.NoError(t, err)
	interceptor := NewValidationInterceptor(v)
	noopNext := func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return connect.NewResponse(&storagev1.LookupResponse{}), nil
	}
	wrapped := interceptor(noopNext)

	invalidReqs := []struct {
		name string
		req  connect.AnyRequest
	}{
		{
			name: "Lookup empty branch",
			req:  connect.NewRequest(&storagev1.LookupRequest{DiskId: validTestDiskUUID, ParentIno: 1, Name: []byte("f")}),
		},
		{
			name: "Lookup parent_ino over max int64",
			req:  connect.NewRequest(&storagev1.LookupRequest{DiskId: validTestDiskUUID, Branch: "main", ParentIno: (1 << 63), Name: []byte("f")}),
		},
		{
			name: "GetAttr empty branch",
			req:  connect.NewRequest(&storagev1.GetAttrRequest{DiskId: validTestDiskUUID, Ino: 1}),
		},
		{
			name: "Create empty branch",
			req:  connect.NewRequest(&storagev1.CreateRequest{DiskId: validTestDiskUUID, ParentIno: 1, Name: []byte("f"), Mode: 0o644, Epoch: 1}),
		},
		{
			name: "MountBranch empty branch",
			req:  connect.NewRequest(&storagev1.MountBranchRequest{DiskId: validTestDiskUUID}),
		},
		{
			name: "RenewMount empty branch",
			req:  connect.NewRequest(&storagev1.RenewMountRequest{DiskId: validTestDiskUUID, MountId: validTestMountUUID, ReadOnly: true}),
		},
		{
			name: "RenewMount RO with non-zero epoch",
			req:  connect.NewRequest(&storagev1.RenewMountRequest{DiskId: validTestDiskUUID, Branch: "main", MountId: validTestMountUUID, ReadOnly: true, Epoch: 2}),
		},
		{
			name: "RenewMount RW with zero epoch",
			req:  connect.NewRequest(&storagev1.RenewMountRequest{DiskId: validTestDiskUUID, Branch: "main", MountId: validTestMountUUID, ReadOnly: false, Epoch: 0}),
		},
		{
			name: "ForkBranch same source and new",
			req:  connect.NewRequest(&storagev1.ForkBranchRequest{DiskId: validTestDiskUUID, SourceBranch: "main", NewBranch: "main"}),
		},
		{
			name: "DeleteBranch empty branch",
			req:  connect.NewRequest(&storagev1.DeleteBranchRequest{DiskId: validTestDiskUUID}),
		},
		{
			name: "ResetBranch empty branch",
			req:  connect.NewRequest(&storagev1.ResetBranchRequest{DiskId: validTestDiskUUID, TargetCommitId: 1}),
		},
		{
			name: "CreateCommit empty branch",
			req:  connect.NewRequest(&storagev1.CreateCommitRequest{DiskId: validTestDiskUUID}),
		},
		{
			name: "MergeBranch same source and target",
			req:  connect.NewRequest(&storagev1.MergeBranchRequest{DiskId: validTestDiskUUID, SourceBranch: "main", TargetBranch: "main"}),
		},
		{
			name: "DiffBranches same source and target",
			req:  connect.NewRequest(&storagev1.DiffBranchesRequest{DiskId: validTestDiskUUID, SourceBranch: "main", TargetBranch: "main"}),
		},
		{
			name: "DiffCommits same base and source",
			req:  connect.NewRequest(&storagev1.DiffCommitsRequest{DiskId: validTestDiskUUID, BaseCommitId: 2, SourceCommitId: 2}),
		},
		{
			name: "DiffWork empty branch",
			req:  connect.NewRequest(&storagev1.DiffWorkRequest{DiskId: validTestDiskUUID}),
		},
		{
			name: "ReadCommitNode zero commit_id",
			req:  connect.NewRequest(&storagev1.ReadCommitNodeRequest{DiskId: validTestDiskUUID, CommitId: 0, Ino: 2, Size: 64}),
		},
		{
			name: "ListMounts NUL branch",
			req:  connect.NewRequest(&storagev1.ListMountsRequest{DiskId: validTestDiskUUID, Branch: "a\x00b"}),
		},
		{
			name: "EvictMounts too long branch",
			req:  connect.NewRequest(&storagev1.EvictMountsRequest{DiskId: validTestDiskUUID, Branch: strings.Repeat("x", 256)}),
		},
	}

	for _, tc := range invalidReqs {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			_, callErr := wrapped(context.Background(), tc.req)
			requireConnectErr(t, callErr, connect.CodeInvalidArgument, "")
		})
	}

	validReqs := []struct {
		name string
		req  connect.AnyRequest
	}{
		{
			name: "ListMounts optional empty branch",
			req:  connect.NewRequest(&storagev1.ListMountsRequest{DiskId: validTestDiskUUID}),
		},
		{
			name: "ListMounts explicit branch",
			req:  connect.NewRequest(&storagev1.ListMountsRequest{DiskId: validTestDiskUUID, Branch: "main"}),
		},
		{
			name: "EvictMounts optional empty branch",
			req:  connect.NewRequest(&storagev1.EvictMountsRequest{DiskId: validTestDiskUUID}),
		},
		{
			name: "EvictMounts explicit branch",
			req:  connect.NewRequest(&storagev1.EvictMountsRequest{DiskId: validTestDiskUUID, Branch: "feat"}),
		},
		{
			name: "DiffCommits distinct commits",
			req:  connect.NewRequest(&storagev1.DiffCommitsRequest{DiskId: validTestDiskUUID, BaseCommitId: 1, SourceCommitId: 2}),
		},
		{
			name: "DiffWork valid branch",
			req:  connect.NewRequest(&storagev1.DiffWorkRequest{DiskId: validTestDiskUUID, Branch: "main"}),
		},
		{
			name: "ReadCommitNode valid",
			req:  connect.NewRequest(&storagev1.ReadCommitNodeRequest{DiskId: validTestDiskUUID, CommitId: 1, Ino: 2, Size: 64}),
		},
	}

	for _, tc := range validReqs {
		t.Run("valid/"+tc.name, func(t *testing.T) {
			_, callErr := wrapped(context.Background(), tc.req)
			require.NoError(t, callErr)
		})
	}
}

func TestToConnectError_MapsAllEngineSentinels(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantCode   connect.Code
		wantReason string
	}{
		{name: "ErrQuotaShrinkBelowUsed", err: engine.ErrQuotaShrinkBelowUsed, wantCode: connect.CodeFailedPrecondition, wantReason: "EINVAL"},
		{name: "ErrStaleEpoch", err: engine.ErrStaleEpoch, wantCode: connect.CodeFailedPrecondition, wantReason: "EBUSY"},
		{name: "ErrDirtyWorkLayer", err: engine.ErrDirtyWorkLayer, wantCode: connect.CodeFailedPrecondition, wantReason: "EDIRTY"},
		{name: "ErrDefaultBranch", err: engine.ErrDefaultBranch, wantCode: connect.CodeFailedPrecondition, wantReason: "EPERM"},
		{name: "ErrCommitInUse", err: engine.ErrCommitInUse, wantCode: connect.CodeFailedPrecondition, wantReason: "EBUSY"},
		{name: "ErrAmbiguousLCA", err: engine.ErrAmbiguousLCA, wantCode: connect.CodeFailedPrecondition, wantReason: "EINVAL"},
		{name: "ErrDiskMounted", err: engine.ErrDiskMounted, wantCode: connect.CodeAborted, wantReason: "EBUSY"},
		{name: "engine.ErrNotFound", err: engine.ErrNotFound, wantCode: connect.CodeNotFound, wantReason: "ENOENT"},
		{name: "ErrNoData", err: engine.ErrNoData, wantCode: connect.CodeNotFound, wantReason: "ENODATA"},
		{name: "ErrExist", err: engine.ErrExist, wantCode: connect.CodeAlreadyExists, wantReason: "EEXIST"},
		{name: "ErrPerm", err: engine.ErrPerm, wantCode: connect.CodePermissionDenied, wantReason: "EPERM"},
		{name: "ErrNotDir", err: engine.ErrNotDir, wantCode: connect.CodeFailedPrecondition, wantReason: "ENOTDIR"},
		{name: "ErrIsDir", err: engine.ErrIsDir, wantCode: connect.CodeFailedPrecondition, wantReason: "EISDIR"},
		{name: "ErrNotEmpty", err: engine.ErrNotEmpty, wantCode: connect.CodeFailedPrecondition, wantReason: "ENOTEMPTY"},
		{name: "ErrNoSpace", err: engine.ErrNoSpace, wantCode: connect.CodeResourceExhausted, wantReason: "ENOSPC"},
		{name: "ErrInodeExhausted", err: engine.ErrInodeExhausted, wantCode: connect.CodeResourceExhausted, wantReason: "ENOSPC"},
		{name: "ErrTooManyLinks", err: engine.ErrTooManyLinks, wantCode: connect.CodeResourceExhausted, wantReason: "EMLINK"},
		{name: "ErrFileTooLarge", err: engine.ErrFileTooLarge, wantCode: connect.CodeInvalidArgument, wantReason: "EFBIG"},
		{name: "ErrNameTooLong", err: engine.ErrNameTooLong, wantCode: connect.CodeInvalidArgument, wantReason: "ENAMETOOLONG"},
		{name: "ErrNotSupported", err: engine.ErrNotSupported, wantCode: connect.CodeUnimplemented, wantReason: "EOPNOTSUPP"},
		{name: "ErrInvalid", err: engine.ErrInvalid, wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL"},
		{name: "ErrInvalidKey", err: engine.ErrInvalidKey, wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL"},
		{name: "ErrInvalidValue", err: engine.ErrInvalidValue, wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL"},
		{name: "ErrClosed", err: engine.ErrClosed, wantCode: connect.CodeInternal, wantReason: "EIO"},
		{name: "wrapped ErrStaleEpoch", err: fmt.Errorf("wrap: %w", engine.ErrStaleEpoch), wantCode: connect.CodeFailedPrecondition, wantReason: "EBUSY"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireConnectErr(t, ToConnectError(tc.err), tc.wantCode, tc.wantReason)
		})
	}

	t.Run("ErrMergeConflict includes ErrorInfo and MergeConflictReport with lca_commit_id", func(t *testing.T) {
		conflictErr := &engine.ErrMergeConflict{
			Report: engine.MergeConflictReport{
				LCACommitID:    42,
				TotalConflicts: 1,
				Conflicts: []engine.MergeConflict{
					{
						Path: []byte("a.txt"),
						Kind: engine.ConflictContent,
						Base: &engine.DiffNodeSummary{Ino: 2, Mode: 0o100644, Size: 5},
					},
				},
			},
		}
		cErr := ToConnectError(conflictErr)
		requireConnectErr(t, cErr, connect.CodeAborted, "ECONFLICT")

		var connectErr *connect.Error
		require.ErrorAs(t, cErr, &connectErr)
		var report *storagev1.MergeConflictReport
		for _, d := range connectErr.Details() {
			if val, vErr := d.Value(); vErr == nil {
				if r, ok := val.(*storagev1.MergeConflictReport); ok {
					report = r
				}
			}
		}
		require.NotNil(t, report)
		require.Equal(t, uint64(42), report.GetLcaCommitId())
		require.Equal(t, uint64(1), report.GetTotalConflicts())
		require.Len(t, report.GetConflicts(), 1)
		require.Equal(t, []byte("a.txt"), report.GetConflicts()[0].GetPath())
		require.Equal(t, storagev1.ConflictKind_CONFLICT_KIND_CONTENT, report.GetConflicts()[0].GetKind())
	})
}
