package validator_test

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpccodec"
	"github.com/starsrc/bowl/pkg/validator"
)

const (
	testValidDiskID   = "11111111-1111-7111-8111-111111111111"
	testValidMountID  = "22222222-2222-7222-8222-222222222222"
	testNilUUID       = "00000000-0000-0000-0000-000000000000"
	testBranchMain    = "main"
	testBranchFeat    = "feat"
	testOverMaxInt64  = uint64(math.MaxInt64) + 1
	testMaxReadWrite  = 4 << 20
	testMaxNameBytes  = 255
	testMaxXattrValue = 65536
	testMaxTargetLen  = 4095
)

func requireErrorInfo(t *testing.T, st *status.Status, wantReason string) {
	t.Helper()
	require.NotNil(t, st, "gRPC status must not be nil")
	require.Equal(t, codes.InvalidArgument, st.Code(), "gRPC code want=%s, got=%s", codes.InvalidArgument, st.Code())
	details := st.Details()
	require.Len(t, details, 1, "ErrorInfo details len want=1, got=%d", len(details))
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok, "detail type want=*errdetails.ErrorInfo, got=%T", details[0])
	require.Equal(t, validator.ErrorDomain, info.GetDomain(), "ErrorInfo.Domain want=%s, got=%s", validator.ErrorDomain, info.GetDomain())
	require.Equal(t, wantReason, info.GetReason(), "ErrorInfo.Reason want=%s, got=%s", wantReason, info.GetReason())
}

func runSubtests[T any](t *testing.T, cases []T, nameFn func(T) string, runFn func(*testing.T, T)) {
	t.Helper()
	for _, tc := range cases {
		t.Run(nameFn(tc), func(t *testing.T) {
			runFn(t, tc)
		})
	}
}

func assertValidationReason(t *testing.T, err error, wantReason string) {
	t.Helper()
	require.Error(t, err, "expected validation failure with reason %s, got nil", wantReason)

	var pvErr *protovalidate.ValidationError
	require.ErrorAs(t, err, &pvErr, "errors.As must unwrap *protovalidate.ValidationError")

	reason, ok := validator.ErrnoReason(err)
	require.True(t, ok, "validator.ErrnoReason must recognize validation error: %v", err)
	require.Equal(t, wantReason, reason, "ErrnoReason want=%s, got=%s", wantReason, reason)

	requireErrorInfo(t, validator.ToStatus(err), wantReason)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok, "validation error must implement GRPCStatus(): %v", err)
	requireErrorInfo(t, grpcStatus, wantReason)

	grpcErr := validator.ToGRPCError(err)
	convertedStatus, ok := status.FromError(grpcErr)
	require.True(t, ok, "ToGRPCError output must parse with status.FromError")
	requireErrorInfo(t, convertedStatus, wantReason)
}

func TestStorageValidator_GivenValidExtremeRequests_WhenValidated_ThenSucceeds(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	now := timestamppb.Now()
	maxInt := uint64(math.MaxInt64)
	renamed := "renamed"
	maxBytes := uint64(1 << 30)

	validDisk := &storagev1.Disk{
		DiskId:        testValidDiskID,
		Name:          "root-disk",
		DefaultBranch: testBranchMain,
		MaxBytes:      maxBytes,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	validBranch := &storagev1.Branch{
		DiskId:       testValidDiskID,
		Name:         testBranchFeat,
		HeadCommitId: 2,
		WriterEpoch:  1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	validCommit := &storagev1.Commit{
		DiskId:    testValidDiskID,
		CommitId:  2,
		ParentId:  1,
		Kind:      storagev1.CommitKind_COMMIT_KIND_FORK,
		Message:   "fork feat",
		CreatedAt: now,
	}

	type validCase struct {
		name string
		msg  proto.Message
	}
	cases := []validCase{
		{
			name: "lookup_non_utf8_binary_name",
			msg:  &storagev1.LookupRequest{DiskId: testValidDiskID, Branch: testBranchMain, ParentIno: 1, Name: []byte{0xff, 0xfe, 0x80, 0x81}},
		},
		{
			name: "lookup_max_255_bytes_name",
			msg:  &storagev1.LookupRequest{DiskId: testValidDiskID, Branch: testBranchMain, ParentIno: 1, Name: bytes.Repeat([]byte("a"), testMaxNameBytes)},
		},
		{
			name: "setxattr_flags_3_max_name_and_value",
			msg: &storagev1.SetXattrRequest{
				DiskId: testValidDiskID,
				Branch: testBranchMain,
				Epoch:  1,
				Ino:    1,
				Name:   bytes.Repeat([]byte("k"), testMaxNameBytes),
				Value:  bytes.Repeat([]byte("v"), testMaxXattrValue),
				Flags:  3,
			},
		},
		{
			name: "create_max_int64_ino_and_max_int32_ids",
			msg:  &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: maxInt, ParentIno: maxInt, Name: []byte("created"), Mode: 0o644, Uid: math.MaxInt32, Gid: math.MaxInt32},
		},
		{
			name: "read_4mib_and_max_int64_fields",
			msg:  &storagev1.ReadRequest{DiskId: testValidDiskID, Branch: testBranchMain, Ino: maxInt, Fh: maxInt, Offset: maxInt, Size: testMaxReadWrite},
		},
		{
			name: "write_4mib_payload",
			msg:  &storagev1.WriteRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Offset: 0, Data: bytes.Repeat([]byte("w"), testMaxReadWrite)},
		},
		{
			name: "write_empty_payload_at_max_int64_offset",
			msg:  &storagev1.WriteRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: maxInt, Ino: maxInt, Fh: maxInt, Offset: maxInt},
		},
		{
			name: "fallocate_max_int64_fields",
			msg:  &storagev1.FallocateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: maxInt, Ino: maxInt, Fh: maxInt, Offset: maxInt, Length: maxInt, Mode: 0},
		},
		{
			name: "symlink_max_4095_target",
			msg:  &storagev1.SymlinkRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("sym"), Target: bytes.Repeat([]byte("t"), testMaxTargetLen)},
		},
		{
			name: "mount_branch_ro",
			msg:  &storagev1.MountBranchRequest{DiskId: testValidDiskID, Branch: testBranchMain, ReadOnly: true},
		},
		{
			name: "renew_mount_ro",
			msg:  &storagev1.RenewMountRequest{DiskId: testValidDiskID, Branch: testBranchMain, MountId: testValidMountID, ReadOnly: true, Epoch: 0},
		},
		{
			name: "renew_mount_rw",
			msg:  &storagev1.RenewMountRequest{DiskId: testValidDiskID, Branch: testBranchMain, MountId: testValidMountID, ReadOnly: false, Epoch: 1},
		},
		{
			name: "unmount_branch_valid",
			msg:  &storagev1.UnmountBranchRequest{DiskId: testValidDiskID, MountId: testValidMountID},
		},
		{
			name: "list_mounts_whole_disk_valid",
			msg:  &storagev1.ListMountsRequest{DiskId: testValidDiskID},
		},
		{
			name: "list_mounts_branch_valid",
			msg:  &storagev1.ListMountsRequest{DiskId: testValidDiskID, Branch: testBranchMain},
		},
		{
			name: "evict_mounts_all_valid",
			msg:  &storagev1.EvictMountsRequest{DiskId: testValidDiskID},
		},
		{
			name: "evict_mounts_single_valid",
			msg:  &storagev1.EvictMountsRequest{DiskId: testValidDiskID, Branch: testBranchMain, MountId: new(testValidMountID)},
		},
		{
			name: "create_disk_valid",
			msg:  &storagev1.CreateDiskRequest{Name: "disk", MaxBytes: maxBytes},
		},
		{
			name: "get_disk_valid",
			msg:  &storagev1.GetDiskRequest{DiskId: testValidDiskID},
		},
		{
			name: "list_disks_max_page_size",
			msg:  &storagev1.ListDisksRequest{PageSize: 1000, PageToken: "next"},
		},
		{
			name: "update_disk_name_only",
			msg:  &storagev1.UpdateDiskRequest{DiskId: testValidDiskID, Name: &renamed},
		},
		{
			name: "update_disk_max_bytes_only",
			msg:  &storagev1.UpdateDiskRequest{DiskId: testValidDiskID, MaxBytes: &maxBytes},
		},
		{
			name: "delete_disk_valid",
			msg:  &storagev1.DeleteDiskRequest{DiskId: testValidDiskID},
		},
		{
			name: "disk_entity",
			msg:  validDisk,
		},
		{
			name: "fork_branch_valid",
			msg:  &storagev1.ForkBranchRequest{DiskId: testValidDiskID, SourceBranch: testBranchMain, NewBranch: testBranchFeat, Message: "fork"},
		},
		{
			name: "fork_branch_response",
			msg:  &storagev1.ForkBranchResponse{Commit: validCommit, Branch: validBranch},
		},
		{
			name: "list_branches_valid",
			msg:  &storagev1.ListBranchesRequest{DiskId: testValidDiskID},
		},
		{
			name: "delete_branch_valid",
			msg:  &storagev1.DeleteBranchRequest{DiskId: testValidDiskID, Branch: testBranchFeat},
		},
		{
			name: "reset_branch_valid",
			msg:  &storagev1.ResetBranchRequest{DiskId: testValidDiskID, Branch: testBranchFeat, TargetCommitId: 1},
		},
		{
			name: "merge_branch_valid",
			msg: &storagev1.MergeBranchRequest{
				DiskId:         testValidDiskID,
				SourceBranch:   testBranchFeat,
				TargetBranch:   testBranchMain,
				Strategy:       storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
				PathStrategies: map[string]storagev1.MergeStrategy{"a/b": storagev1.MergeStrategy_MERGE_STRATEGY_OURS},
				NoFastForward:  true,
				Message:        "merge feat",
			},
		},
		{
			name: "create_commit_valid",
			msg:  &storagev1.CreateCommitRequest{DiskId: testValidDiskID, Branch: testBranchMain, Message: strings.Repeat("m", 4096)},
		},
		{
			name: "list_commits_valid",
			msg:  &storagev1.ListCommitsRequest{DiskId: testValidDiskID, Kind: storagev1.CommitKind_COMMIT_KIND_MANUAL, PageSize: 1000},
		},
		{
			name: "delete_commit_valid",
			msg:  &storagev1.DeleteCommitRequest{DiskId: testValidDiskID, CommitId: 2},
		},
		{
			name: "prune_commits_valid",
			msg:  &storagev1.PruneCommitsRequest{DiskId: testValidDiskID},
		},
		{
			name: "read_commit_node_valid",
			msg:  &storagev1.ReadCommitNodeRequest{DiskId: testValidDiskID, CommitId: 1, Ino: 2, Offset: 0, Size: testMaxReadWrite},
		},
		{
			name: "diff_branches_valid",
			msg:  &storagev1.DiffBranchesRequest{DiskId: testValidDiskID, SourceBranch: testBranchFeat, TargetBranch: testBranchMain, PageSize: 1000},
		},
		{
			name: "diff_commits_valid",
			msg:  &storagev1.DiffCommitsRequest{DiskId: testValidDiskID, BaseCommitId: 1, SourceCommitId: 2, PageSize: 1000},
		},
		{
			name: "diff_work_valid",
			msg:  &storagev1.DiffWorkRequest{DiskId: testValidDiskID, Branch: testBranchMain, PageSize: 1000},
		},
	}

	runSubtests(t, cases, func(tc validCase) string { return tc.name }, func(t *testing.T, tc validCase) {
		require.NoError(t, v.Validate(tc.msg), "valid extreme request %s must pass", tc.name)
	})
}

func TestStorageValidator_GivenInvalidReadDataPlaneDiskIDOrBranch_WhenValidated_ThenReturnsEINVAL(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	type readBuilder struct {
		name  string
		build func(diskID, branch string) proto.Message
	}
	builders := []readBuilder{
		{name: "LookupRequest", build: func(id, br string) proto.Message {
			return &storagev1.LookupRequest{DiskId: id, Branch: br, ParentIno: 1, Name: []byte("f")}
		}},
		{name: "GetAttrRequest", build: func(id, br string) proto.Message {
			return &storagev1.GetAttrRequest{DiskId: id, Branch: br, Ino: 1}
		}},
		{name: "ReadlinkRequest", build: func(id, br string) proto.Message {
			return &storagev1.ReadlinkRequest{DiskId: id, Branch: br, Ino: 1}
		}},
		{name: "ReadRequest", build: func(id, br string) proto.Message {
			return &storagev1.ReadRequest{DiskId: id, Branch: br, Ino: 1, Size: 64}
		}},
		{name: "ReadDirRequest", build: func(id, br string) proto.Message {
			return &storagev1.ReadDirRequest{DiskId: id, Branch: br, Ino: 1, Limit: 16}
		}},
		{name: "ReadDirPlusRequest", build: func(id, br string) proto.Message {
			return &storagev1.ReadDirPlusRequest{DiskId: id, Branch: br, Ino: 1, Limit: 16}
		}},
		{name: "GetXattrRequest", build: func(id, br string) proto.Message {
			return &storagev1.GetXattrRequest{DiskId: id, Branch: br, Ino: 1, Name: []byte("user.k")}
		}},
		{name: "ListXattrRequest", build: func(id, br string) proto.Message {
			return &storagev1.ListXattrRequest{DiskId: id, Branch: br, Ino: 1}
		}},
		{name: "StatFSRequest", build: func(id, br string) proto.Message {
			return &storagev1.StatFSRequest{DiskId: id, Branch: br, Ino: 1}
		}},
	}

	runSubtests(t, builders, func(b readBuilder) string { return b.name }, func(t *testing.T, b readBuilder) {
		require.NoError(t, v.Validate(b.build(testValidDiskID, testBranchMain)))
		assertValidationReason(t, v.Validate(b.build("", testBranchMain)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build("not-a-uuid", testBranchMain)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testNilUUID, testBranchMain)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testValidDiskID, "")), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testValidDiskID, "bad\x00br")), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testValidDiskID, strings.Repeat("b", 256))), validator.ReasonInvalidArgument)
	})
}

func TestStorageValidator_GivenInvalidWriteDataPlaneDiskIDBranchOrEpoch_WhenValidated_ThenReturnsEINVAL(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	mode := uint32(0o644)

	type writeBuilder struct {
		name  string
		build func(diskID, branch string, epoch uint64) proto.Message
	}
	builders := []writeBuilder{
		{name: "SetAttrRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.SetAttrRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 1, Mode: &mode}
		}},
		{name: "MknodRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.MknodRequest{DiskId: id, Branch: br, Epoch: ep, ParentIno: 1, Name: []byte("n"), Mode: 0o644}
		}},
		{name: "MkdirRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.MkdirRequest{DiskId: id, Branch: br, Epoch: ep, ParentIno: 1, Name: []byte("d"), Mode: 0o755}
		}},
		{name: "SymlinkRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.SymlinkRequest{DiskId: id, Branch: br, Epoch: ep, ParentIno: 1, Name: []byte("s"), Target: []byte("t")}
		}},
		{name: "LinkRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.LinkRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 2, NewParentIno: 1, NewName: []byte("h")}
		}},
		{name: "UnlinkRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.UnlinkRequest{DiskId: id, Branch: br, Epoch: ep, ParentIno: 1, Name: []byte("f")}
		}},
		{name: "RmdirRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.RmdirRequest{DiskId: id, Branch: br, Epoch: ep, ParentIno: 1, Name: []byte("d")}
		}},
		{name: "RenameRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.RenameRequest{DiskId: id, Branch: br, Epoch: ep, OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("b")}
		}},
		{name: "CreateRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.CreateRequest{DiskId: id, Branch: br, Epoch: ep, ParentIno: 1, Name: []byte("f"), Mode: 0o644}
		}},
		{name: "CreateTmpfileRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.CreateTmpfileRequest{DiskId: id, Branch: br, Epoch: ep, ParentIno: 1, Mode: 0o600}
		}},
		{name: "WriteRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.WriteRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 1, Data: []byte("hello")}
		}},
		{name: "FsyncRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.FsyncRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 1}
		}},
		{name: "FsyncDirRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.FsyncDirRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 1}
		}},
		{name: "FallocateRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.FallocateRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 1, Length: 4096}
		}},
		{name: "SetXattrRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.SetXattrRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 1, Name: []byte("user.k"), Value: []byte("v")}
		}},
		{name: "RemoveXattrRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.RemoveXattrRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 1, Name: []byte("user.k")}
		}},
		{name: "ReclaimOrphanRequest", build: func(id, br string, ep uint64) proto.Message {
			return &storagev1.ReclaimOrphanRequest{DiskId: id, Branch: br, Epoch: ep, Ino: 1}
		}},
	}

	runSubtests(t, builders, func(b writeBuilder) string { return b.name }, func(t *testing.T, b writeBuilder) {
		require.NoError(t, v.Validate(b.build(testValidDiskID, testBranchMain, 1)))
		require.NoError(t, v.Validate(b.build(testValidDiskID, testBranchMain, math.MaxInt64)))
		assertValidationReason(t, v.Validate(b.build("", testBranchMain, 1)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build("not-a-uuid", testBranchMain, 1)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testNilUUID, testBranchMain, 1)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testValidDiskID, "", 1)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testValidDiskID, "bad\x00br", 1)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testValidDiskID, strings.Repeat("b", 256), 1)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testValidDiskID, testBranchMain, 0)), validator.ReasonInvalidArgument)
		assertValidationReason(t, v.Validate(b.build(testValidDiskID, testBranchMain, testOverMaxInt64)), validator.ReasonInvalidArgument)
	})
}

func TestStorageValidator_GivenInvalidNamesXattrsOrTargets_WhenValidated_ThenMapsErrnoReason(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	long256Bytes := bytes.Repeat([]byte("a"), 256)
	long256Str := strings.Repeat("a", 256)

	type reasonCase struct {
		name       string
		msg        proto.Message
		wantReason string
	}
	cases := []reasonCase{
		{name: "lookup_name_too_long", msg: &storagev1.LookupRequest{DiskId: testValidDiskID, Branch: testBranchMain, ParentIno: 1, Name: long256Bytes}, wantReason: validator.ReasonNameTooLong},
		{name: "lookup_name_has_nul", msg: &storagev1.LookupRequest{DiskId: testValidDiskID, Branch: testBranchMain, ParentIno: 1, Name: []byte("foo\x00bar")}, wantReason: validator.ReasonInvalidArgument},
		{name: "lookup_name_has_slash", msg: &storagev1.LookupRequest{DiskId: testValidDiskID, Branch: testBranchMain, ParentIno: 1, Name: []byte("foo/bar")}, wantReason: validator.ReasonInvalidArgument},
		{name: "mkdir_256_slashes_too_long", msg: &storagev1.MkdirRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: bytes.Repeat([]byte("/"), 256), Mode: 0o755}, wantReason: validator.ReasonNameTooLong},
		{name: "create_empty_name", msg: &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte{}}, wantReason: validator.ReasonInvalidArgument},
		{name: "create_name_too_long", msg: &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: long256Bytes}, wantReason: validator.ReasonNameTooLong},
		{name: "create_name_has_slash", msg: &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("bad/name")}, wantReason: validator.ReasonInvalidArgument},
		{name: "create_name_has_nul", msg: &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("bad\x00name")}, wantReason: validator.ReasonInvalidArgument},
		{name: "rename_new_name_too_long", msg: &storagev1.RenameRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: long256Bytes}, wantReason: validator.ReasonNameTooLong},
		{name: "create_disk_name_too_long", msg: &storagev1.CreateDiskRequest{Name: long256Str, MaxBytes: 1}, wantReason: validator.ReasonNameTooLong},
		{name: "create_disk_name_has_nul", msg: &storagev1.CreateDiskRequest{Name: "a\x00b", MaxBytes: 1}, wantReason: validator.ReasonInvalidArgument},
		{name: "update_disk_name_too_long", msg: &storagev1.UpdateDiskRequest{DiskId: testValidDiskID, Name: &long256Str}, wantReason: validator.ReasonNameTooLong},
		{name: "update_disk_name_has_nul", msg: &storagev1.UpdateDiskRequest{DiskId: testValidDiskID, Name: new("a\x00b")}, wantReason: validator.ReasonInvalidArgument},
		{name: "setxattr_empty_name", msg: &storagev1.SetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Name: []byte{}, Value: []byte("val")}, wantReason: validator.ReasonInvalidArgument},
		{name: "setxattr_invalid_flags", msg: &storagev1.SetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Name: []byte("user.k"), Value: []byte("v"), Flags: 4}, wantReason: validator.ReasonInvalidArgument},
		{name: "setxattr_name_too_long", msg: &storagev1.SetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Name: long256Bytes, Value: []byte("val")}, wantReason: validator.ReasonRange},
		{name: "setxattr_name_has_nul", msg: &storagev1.SetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Name: []byte("user.a\x00b"), Value: []byte("val")}, wantReason: validator.ReasonInvalidArgument},
		{name: "setxattr_value_too_long", msg: &storagev1.SetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Name: []byte("user.test"), Value: bytes.Repeat([]byte("v"), 65537)}, wantReason: validator.ReasonRange},
		{name: "getxattr_nil_name", msg: &storagev1.GetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Ino: 1, Name: nil}, wantReason: validator.ReasonInvalidArgument},
		{name: "removexattr_name_too_long", msg: &storagev1.RemoveXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Name: long256Bytes}, wantReason: validator.ReasonRange},
		{name: "listxattr_resp_empty_item", msg: &storagev1.ListXattrResponse{Names: [][]byte{[]byte("user.ok"), {}}}, wantReason: validator.ReasonInvalidArgument},
		{name: "listxattr_resp_long_item", msg: &storagev1.ListXattrResponse{Names: [][]byte{long256Bytes}}, wantReason: validator.ReasonRange},
		{name: "getxattr_resp_value_too_long", msg: &storagev1.GetXattrResponse{Value: bytes.Repeat([]byte("v"), 65537)}, wantReason: validator.ReasonRange},
		{name: "symlink_target_too_long", msg: &storagev1.SymlinkRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("link"), Target: bytes.Repeat([]byte("t"), 4096)}, wantReason: validator.ReasonNameTooLong},
		{name: "symlink_target_empty", msg: &storagev1.SymlinkRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("link"), Target: []byte{}}, wantReason: validator.ReasonInvalidArgument},
		{name: "symlink_target_4095_plus_nul", msg: &storagev1.SymlinkRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("link"), Target: append(bytes.Repeat([]byte("t"), 4095), 0)}, wantReason: validator.ReasonInvalidArgument},
		{name: "readlink_resp_target_too_long", msg: &storagev1.ReadlinkResponse{Target: bytes.Repeat([]byte("t"), 4096)}, wantReason: validator.ReasonNameTooLong},
		{name: "readlink_resp_target_4095_plus_nul", msg: &storagev1.ReadlinkResponse{Target: append(bytes.Repeat([]byte("t"), 4095), 0)}, wantReason: validator.ReasonInvalidArgument},
	}

	runSubtests(t, cases, func(tc reasonCase) string { return tc.name }, func(t *testing.T, tc reasonCase) {
		assertValidationReason(t, v.Validate(tc.msg), tc.wantReason)
	})
}

func TestStorageValidator_GivenOutOfRangeNumericOrControlFields_WhenValidated_ThenReturnsEINVAL(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	now := timestamppb.Now()
	type invalidCase struct {
		name string
		msg  proto.Message
	}
	cases := []invalidCase{
		{name: "lookup_parent_ino_zero", msg: &storagev1.LookupRequest{DiskId: testValidDiskID, Branch: testBranchMain, ParentIno: 0, Name: []byte("test.txt")}},
		{name: "create_parent_ino_zero", msg: &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 0, Name: []byte("c")}},
		{name: "create_parent_ino_over_max_int64", msg: &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: testOverMaxInt64, Name: []byte("c")}},
		{name: "create_uid_over_max_int32", msg: &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("c"), Uid: math.MaxInt32 + 1}},
		{name: "create_gid_over_max_int32", msg: &storagev1.CreateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("c"), Gid: math.MaxInt32 + 1}},
		{name: "read_ino_zero", msg: &storagev1.ReadRequest{DiskId: testValidDiskID, Branch: testBranchMain, Ino: 0, Size: 1}},
		{name: "read_size_over_4mib", msg: &storagev1.ReadRequest{DiskId: testValidDiskID, Branch: testBranchMain, Ino: 1, Size: testMaxReadWrite + 1}},
		{name: "read_ino_over_max_int64", msg: &storagev1.ReadRequest{DiskId: testValidDiskID, Branch: testBranchMain, Ino: testOverMaxInt64, Size: 1}},
		{name: "read_fh_over_max_int64", msg: &storagev1.ReadRequest{DiskId: testValidDiskID, Branch: testBranchMain, Ino: 1, Fh: testOverMaxInt64, Size: 1}},
		{name: "read_offset_over_max_int64", msg: &storagev1.ReadRequest{DiskId: testValidDiskID, Branch: testBranchMain, Ino: 1, Offset: testOverMaxInt64, Size: 1}},
		{name: "write_ino_zero", msg: &storagev1.WriteRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 0, Data: []byte("x")}},
		{name: "write_data_over_4mib", msg: &storagev1.WriteRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Data: bytes.Repeat([]byte("w"), testMaxReadWrite+1)}},
		{name: "write_offset_len_overflow", msg: &storagev1.WriteRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Offset: math.MaxInt64, Data: []byte("x")}},
		{name: "write_ino_over_max_int64", msg: &storagev1.WriteRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: testOverMaxInt64, Data: []byte("x")}},
		{name: "write_fh_over_max_int64", msg: &storagev1.WriteRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Fh: testOverMaxInt64, Data: []byte("x")}},
		{name: "write_offset_over_max_int64", msg: &storagev1.WriteRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Offset: testOverMaxInt64}},
		{name: "fallocate_ino_zero", msg: &storagev1.FallocateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 0, Length: 1}},
		{name: "fallocate_length_zero", msg: &storagev1.FallocateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Length: 0}},
		{name: "fallocate_ino_over_max_int64", msg: &storagev1.FallocateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: testOverMaxInt64, Length: 1}},
		{name: "fallocate_fh_over_max_int64", msg: &storagev1.FallocateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Fh: testOverMaxInt64, Length: 1}},
		{name: "fallocate_offset_over_max_int64", msg: &storagev1.FallocateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Offset: testOverMaxInt64, Length: 1}},
		{name: "fallocate_length_over_max_int64", msg: &storagev1.FallocateRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Length: testOverMaxInt64}},
		{name: "setattr_no_updates", msg: &storagev1.SetAttrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1}},
		{name: "setattr_atime_conflict", msg: &storagev1.SetAttrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Atime: now, AtimeNow: true}},
		{name: "reclaim_orphan_ino_zero", msg: &storagev1.ReclaimOrphanRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 0}},
		{name: "reclaim_orphan_ino_over_max_int64", msg: &storagev1.ReclaimOrphanRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: testOverMaxInt64}},
		{name: "mount_branch_empty_id", msg: &storagev1.MountBranchRequest{Branch: testBranchMain}},
		{name: "mount_branch_bad_id", msg: &storagev1.MountBranchRequest{DiskId: "bad", Branch: testBranchMain}},
		{name: "mount_branch_nil_uuid", msg: &storagev1.MountBranchRequest{DiskId: testNilUUID, Branch: testBranchMain}},
		{name: "mount_branch_empty_branch", msg: &storagev1.MountBranchRequest{DiskId: testValidDiskID}},
		{name: "renew_mount_empty_branch", msg: &storagev1.RenewMountRequest{DiskId: testValidDiskID, MountId: testValidMountID, ReadOnly: true}},
		{name: "renew_mount_ro_nonzero_epoch", msg: &storagev1.RenewMountRequest{DiskId: testValidDiskID, Branch: testBranchMain, MountId: testValidMountID, ReadOnly: true, Epoch: 1}},
		{name: "renew_mount_rw_zero_epoch", msg: &storagev1.RenewMountRequest{DiskId: testValidDiskID, Branch: testBranchMain, MountId: testValidMountID, ReadOnly: false, Epoch: 0}},
		{name: "unmount_branch_nil_mount", msg: &storagev1.UnmountBranchRequest{DiskId: testValidDiskID, MountId: testNilUUID}},
		{name: "list_mounts_empty_id", msg: &storagev1.ListMountsRequest{}},
		{name: "list_mounts_bad_id", msg: &storagev1.ListMountsRequest{DiskId: "bad"}},
		{name: "list_mounts_nil_uuid", msg: &storagev1.ListMountsRequest{DiskId: testNilUUID}},
		{name: "list_mounts_nul_branch", msg: &storagev1.ListMountsRequest{DiskId: testValidDiskID, Branch: "a\x00b"}},
		{name: "evict_mounts_empty_id", msg: &storagev1.EvictMountsRequest{}},
		{name: "evict_mounts_bad_id", msg: &storagev1.EvictMountsRequest{DiskId: "bad"}},
		{name: "evict_mounts_nil_uuid", msg: &storagev1.EvictMountsRequest{DiskId: testNilUUID}},
		{name: "evict_mounts_empty_mount_id", msg: &storagev1.EvictMountsRequest{DiskId: testValidDiskID, MountId: new("")}},
		{name: "evict_mounts_bad_mount_id", msg: &storagev1.EvictMountsRequest{DiskId: testValidDiskID, MountId: new("bad")}},
		{name: "evict_mounts_nil_mount_id", msg: &storagev1.EvictMountsRequest{DiskId: testValidDiskID, MountId: new(testNilUUID)}},
		{name: "create_disk_zero_max_bytes", msg: &storagev1.CreateDiskRequest{Name: "disk", MaxBytes: 0}},
		{name: "create_disk_over_max_int64_bytes", msg: &storagev1.CreateDiskRequest{Name: "disk", MaxBytes: testOverMaxInt64}},
		{name: "get_disk_bad_id", msg: &storagev1.GetDiskRequest{DiskId: "bad"}},
		{name: "get_disk_nil_uuid", msg: &storagev1.GetDiskRequest{DiskId: testNilUUID}},
		{name: "list_disks_page_size_over_1000", msg: &storagev1.ListDisksRequest{PageSize: 1001}},
		{name: "update_disk_no_updates", msg: &storagev1.UpdateDiskRequest{DiskId: testValidDiskID}},
		{name: "update_disk_bad_id", msg: &storagev1.UpdateDiskRequest{DiskId: "bad", Name: new("renamed")}},
		{name: "update_disk_zero_max_bytes", msg: &storagev1.UpdateDiskRequest{DiskId: testValidDiskID, MaxBytes: new(uint64(0))}},
		{name: "update_disk_over_max_int64_bytes", msg: &storagev1.UpdateDiskRequest{DiskId: testValidDiskID, MaxBytes: new(testOverMaxInt64)}},
		{name: "delete_disk_nil_uuid", msg: &storagev1.DeleteDiskRequest{DiskId: testNilUUID}},
		{name: "disk_missing_default_branch", msg: &storagev1.Disk{DiskId: testValidDiskID, MaxBytes: 1024, CreatedAt: now, UpdatedAt: now}},
		{name: "fork_branch_same_name", msg: &storagev1.ForkBranchRequest{DiskId: testValidDiskID, SourceBranch: testBranchMain, NewBranch: testBranchMain}},
		{name: "fork_branch_empty_source", msg: &storagev1.ForkBranchRequest{DiskId: testValidDiskID, SourceBranch: "", NewBranch: testBranchFeat}},
		{name: "fork_branch_empty_new", msg: &storagev1.ForkBranchRequest{DiskId: testValidDiskID, SourceBranch: testBranchMain, NewBranch: ""}},
		{name: "fork_branch_response_missing_fields", msg: &storagev1.ForkBranchResponse{}},
		{name: "list_branches_nil_disk", msg: &storagev1.ListBranchesRequest{DiskId: testNilUUID}},
		{name: "delete_branch_empty_branch", msg: &storagev1.DeleteBranchRequest{DiskId: testValidDiskID}},
		{name: "reset_branch_zero_commit", msg: &storagev1.ResetBranchRequest{DiskId: testValidDiskID, Branch: testBranchMain, TargetCommitId: 0}},
		{name: "reset_branch_over_max_commit", msg: &storagev1.ResetBranchRequest{DiskId: testValidDiskID, Branch: testBranchMain, TargetCommitId: testOverMaxInt64}},
		{name: "merge_branch_same_branch", msg: &storagev1.MergeBranchRequest{DiskId: testValidDiskID, SourceBranch: testBranchMain, TargetBranch: testBranchMain}},
		{name: "merge_branch_invalid_strategy", msg: &storagev1.MergeBranchRequest{DiskId: testValidDiskID, SourceBranch: testBranchFeat, TargetBranch: testBranchMain, Strategy: storagev1.MergeStrategy(99)}},
		{name: "merge_branch_unspecified_path_strategy", msg: &storagev1.MergeBranchRequest{DiskId: testValidDiskID, SourceBranch: testBranchFeat, TargetBranch: testBranchMain, PathStrategies: map[string]storagev1.MergeStrategy{"a": storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED}}},
		{name: "create_commit_empty_branch", msg: &storagev1.CreateCommitRequest{DiskId: testValidDiskID}},
		{name: "create_commit_overlong_message", msg: &storagev1.CreateCommitRequest{DiskId: testValidDiskID, Branch: testBranchMain, Message: strings.Repeat("m", 4097)}},
		{name: "create_commit_nul_message", msg: &storagev1.CreateCommitRequest{DiskId: testValidDiskID, Branch: testBranchMain, Message: "a\x00b"}},
		{name: "list_commits_invalid_kind", msg: &storagev1.ListCommitsRequest{DiskId: testValidDiskID, Kind: storagev1.CommitKind(99)}},
		{name: "list_commits_page_size_over_1000", msg: &storagev1.ListCommitsRequest{DiskId: testValidDiskID, PageSize: 1001}},
		{name: "delete_commit_zero_id", msg: &storagev1.DeleteCommitRequest{DiskId: testValidDiskID, CommitId: 0}},
		{name: "prune_commits_nil_disk", msg: &storagev1.PruneCommitsRequest{DiskId: testNilUUID}},
		{name: "read_commit_node_zero_commit", msg: &storagev1.ReadCommitNodeRequest{DiskId: testValidDiskID, CommitId: 0, Ino: 1, Size: 64}},
		{name: "read_commit_node_zero_ino", msg: &storagev1.ReadCommitNodeRequest{DiskId: testValidDiskID, CommitId: 1, Ino: 0, Size: 64}},
		{name: "read_commit_node_size_over_4mib", msg: &storagev1.ReadCommitNodeRequest{DiskId: testValidDiskID, CommitId: 1, Ino: 1, Size: testMaxReadWrite + 1}},
		{name: "diff_branches_same_branch", msg: &storagev1.DiffBranchesRequest{DiskId: testValidDiskID, SourceBranch: testBranchMain, TargetBranch: testBranchMain}},
		{name: "diff_commits_same_commit", msg: &storagev1.DiffCommitsRequest{DiskId: testValidDiskID, BaseCommitId: 1, SourceCommitId: 1}},
		{name: "diff_commits_zero_base", msg: &storagev1.DiffCommitsRequest{DiskId: testValidDiskID, BaseCommitId: 0, SourceCommitId: 2}},
		{name: "diff_work_empty_branch", msg: &storagev1.DiffWorkRequest{DiskId: testValidDiskID}},
	}

	runSubtests(t, cases, func(tc invalidCase) string { return tc.name }, func(t *testing.T, tc invalidCase) {
		assertValidationReason(t, v.Validate(tc.msg), validator.ReasonInvalidArgument)
	})
}

func TestStorageValidator_GivenMultipleViolations_WhenValidated_ThenSelectsHighestPriorityReason(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	now := timestamppb.Now()
	long256Bytes := bytes.Repeat([]byte("a"), 256)
	long256Str := strings.Repeat("a", 256)

	type reasonCase struct {
		name       string
		msg        proto.Message
		wantReason string
	}
	cases := []reasonCase{
		{
			name:       "lookup_bad_disk_id_beats_long_name",
			msg:        &storagev1.LookupRequest{DiskId: "bad", Branch: testBranchMain, ParentIno: 1, Name: long256Bytes},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "lookup_empty_branch_beats_long_name",
			msg:        &storagev1.LookupRequest{DiskId: testValidDiskID, Branch: "", ParentIno: 1, Name: long256Bytes},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "setxattr_bad_disk_id_beats_long_name",
			msg:        &storagev1.SetXattrRequest{DiskId: "bad", Branch: testBranchMain, Epoch: 1, Ino: 1, Name: long256Bytes, Value: []byte("val")},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "setxattr_zero_epoch_beats_long_name",
			msg:        &storagev1.SetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 0, Ino: 1, Name: long256Bytes, Value: []byte("val")},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "setxattr_name_max_len_beats_nul_byte",
			msg:        &storagev1.SetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Name: append(bytes.Repeat([]byte("a"), 255), 0), Value: []byte("val")},
			wantReason: validator.ReasonRange,
		},
		{
			name:       "setxattr_name_nul_beats_value_max_len",
			msg:        &storagev1.SetXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, Ino: 1, Name: []byte("bad\x00name"), Value: bytes.Repeat([]byte("v"), 65537)},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "getxattr_nil_uuid_beats_long_name",
			msg:        &storagev1.GetXattrRequest{DiskId: testNilUUID, Branch: testBranchMain, Ino: 1, Name: long256Bytes},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "removexattr_zero_epoch_beats_long_name",
			msg:        &storagev1.RemoveXattrRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 0, Ino: 1, Name: long256Bytes},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "mkdir_zero_epoch_beats_long_name",
			msg:        &storagev1.MkdirRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 0, ParentIno: 1, Name: long256Bytes, Mode: 0o755},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "rename_nil_uuid_beats_long_new_name",
			msg:        &storagev1.RenameRequest{DiskId: testNilUUID, Branch: testBranchMain, Epoch: 1, OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: long256Bytes},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "rename_old_name_too_long_beats_empty_new_name",
			msg:        &storagev1.RenameRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, OldParentIno: 1, OldName: long256Bytes, NewParentIno: 1, NewName: []byte{}},
			wantReason: validator.ReasonNameTooLong,
		},
		{
			name:       "rename_old_name_slash_beats_long_new_name",
			msg:        &storagev1.RenameRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, OldParentIno: 1, OldName: []byte("a/b"), NewParentIno: 1, NewName: long256Bytes},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "symlink_zero_epoch_beats_long_target",
			msg:        &storagev1.SymlinkRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 0, ParentIno: 1, Name: []byte("link"), Target: bytes.Repeat([]byte("t"), 4096)},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "symlink_long_name_beats_empty_target",
			msg:        &storagev1.SymlinkRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: long256Bytes, Target: []byte{}},
			wantReason: validator.ReasonNameTooLong,
		},
		{
			name:       "symlink_name_slash_beats_long_target",
			msg:        &storagev1.SymlinkRequest{DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1, ParentIno: 1, Name: []byte("a/b"), Target: bytes.Repeat([]byte("t"), 4096)},
			wantReason: validator.ReasonInvalidArgument,
		},
		{
			name:       "disk_empty_default_branch_beats_long_name",
			msg:        &storagev1.Disk{DiskId: testValidDiskID, Name: long256Str, DefaultBranch: "", MaxBytes: 1024, CreatedAt: now, UpdatedAt: now},
			wantReason: validator.ReasonInvalidArgument,
		},
	}

	runSubtests(t, cases, func(tc reasonCase) string { return tc.name }, func(t *testing.T, tc reasonCase) {
		assertValidationReason(t, v.Validate(tc.msg), tc.wantReason)
	})
}

func TestStorageValidator_GivenExternalProtovalidateError_WhenTranslated_ThenMapsReason(t *testing.T) {
	rawValidator, err := protovalidate.New()
	require.NoError(t, err)

	rawErr := rawValidator.Validate(&storagev1.SetXattrRequest{
		DiskId: testValidDiskID,
		Branch: testBranchMain,
		Epoch:  1,
		Ino:    1,
		Name:   bytes.Repeat([]byte("a"), 256),
		Value:  []byte("v"),
	})
	require.Error(t, rawErr)
	requireErrorInfo(t, validator.ToStatus(rawErr), validator.ReasonRange)

	rawReason, ok := validator.ErrnoReason(rawErr)
	require.True(t, ok)
	require.Equal(t, validator.ReasonRange, rawReason)

	wrappedRaw := fmt.Errorf("wrap: %w", rawErr)
	requireErrorInfo(t, validator.ToStatus(wrappedRaw), validator.ReasonRange)
}

func TestStorageValidator_GivenNilOrNonValidationError_WhenTranslated_ThenReturnsEmptyOrUnknown(t *testing.T) {
	plainErr := errors.New("plain non-validation failure")
	plainReason, ok := validator.ErrnoReason(plainErr)
	require.False(t, ok)
	require.Empty(t, plainReason)
	unknownStatus := validator.ToStatus(plainErr)
	require.Equal(t, codes.Unknown, unknownStatus.Code(), "non-validation error code want=%s, got=%s", codes.Unknown, unknownStatus.Code())

	nilReason, ok := validator.ErrnoReason(nil)
	require.False(t, ok)
	require.Empty(t, nilReason)
	require.Nil(t, validator.ToStatus(nil))
	require.NoError(t, validator.ToGRPCError(nil))
}

func TestStorageValidator_GivenCodecRoundTrippedRequest_WhenValidated_ThenPreservesPriorityError(t *testing.T) {
	codec := rpccodec.NewProtoBinaryCodec(rpccodec.CodecHooks{})
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	req := &storagev1.LookupRequest{
		DiskId:    "bad-uuid",
		Branch:    testBranchMain,
		ParentIno: 1,
		Name:      bytes.Repeat([]byte("n"), 256),
	}
	wire, err := codec.Marshal(req)
	require.NoError(t, err)

	var decoded storagev1.LookupRequest
	require.NoError(t, codec.Unmarshal(wire, &decoded))

	valErr := v.Validate(&decoded)
	assertValidationReason(t, valErr, validator.ReasonInvalidArgument)

	var typedValErr *validator.ValidationError
	require.ErrorAs(t, valErr, &typedValErr)
	require.Equal(t, validator.ReasonInvalidArgument, typedValErr.Reason())
	require.Equal(t, validator.ErrorDomain, typedValErr.Domain())
	require.NotEmpty(t, typedValErr.Error())
}

func TestStorageValidator_GivenValidCreateTmpfileMessages_WhenValidated_ThenSucceeds(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err, "create storage validator: want nil error, got=%v", err)

	now := timestamppb.Now()
	validInode := &storagev1.Inode{
		Ino:        42,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  0o100600,
			Nlink: 0,
			Uid:   1000,
			Gid:   1000,
			Atime: now,
			Mtime: now,
			Ctime: now,
		},
	}

	type validCase struct {
		name string
		msg  proto.Message
	}
	cases := []validCase{
		{
			name: "request_mode_zero_non_exclusive",
			msg: &storagev1.CreateTmpfileRequest{
				ParentIno: 1,
				Mode:      0,
				Uid:       0,
				Gid:       0,
				Exclusive: false,
				DiskId:    testValidDiskID,
				Branch:    testBranchMain,
				Epoch:     1,
			},
		},
		{
			name: "request_s_ifreg_0600_exclusive",
			msg: &storagev1.CreateTmpfileRequest{
				ParentIno: 1,
				Mode:      0o100600,
				Uid:       1000,
				Gid:       1000,
				Exclusive: true,
				DiskId:    testValidDiskID,
				Branch:    testBranchMain,
				Epoch:     1,
			},
		},
		{
			name: "request_max_int64_and_max_int32_boundaries",
			msg: &storagev1.CreateTmpfileRequest{
				ParentIno: math.MaxInt64,
				Mode:      0o100777,
				Uid:       math.MaxInt32,
				Gid:       math.MaxInt32,
				Exclusive: false,
				DiskId:    testValidDiskID,
				Branch:    testBranchMain,
				Epoch:     math.MaxInt64,
			},
		},
		{
			name: "response_nlink_zero_zero_timeouts",
			msg: &storagev1.CreateTmpfileResponse{
				Inode:               validInode,
				EntryTimeoutSeconds: 0,
				AttrTimeoutSeconds:  0,
			},
		},
		{
			name: "response_nlink_zero_positive_timeouts",
			msg: &storagev1.CreateTmpfileResponse{
				Inode:               validInode,
				EntryTimeoutSeconds: 1.5,
				AttrTimeoutSeconds:  2.5,
			},
		},
	}

	runSubtests(t, cases, func(tc validCase) string { return tc.name }, func(t *testing.T, tc validCase) {
		gotErr := v.Validate(tc.msg)
		require.NoError(t, gotErr, "case=%s: want validation success, got=%v", tc.name, gotErr)
	})
}

func TestStorageValidator_GivenInvalidCreateTmpfileMessages_WhenValidated_ThenReturnsEINVAL(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err, "create storage validator: want nil error, got=%v", err)

	now := timestamppb.Now()
	validInode := &storagev1.Inode{
		Ino:        42,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  0o100600,
			Nlink: 0,
			Atime: now,
			Mtime: now,
			Ctime: now,
		},
	}

	type invalidCase struct {
		name string
		msg  proto.Message
	}
	cases := []invalidCase{
		{
			name: "request_empty_disk_id",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 1, Mode: 0o600, DiskId: "", Branch: testBranchMain, Epoch: 1},
		},
		{
			name: "request_malformed_disk_id",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 1, Mode: 0o600, DiskId: "not-a-uuid", Branch: testBranchMain, Epoch: 1},
		},
		{
			name: "request_nil_disk_id",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 1, Mode: 0o600, DiskId: testNilUUID, Branch: testBranchMain, Epoch: 1},
		},
		{
			name: "request_empty_branch",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 1, Mode: 0o600, DiskId: testValidDiskID, Branch: "", Epoch: 1},
		},
		{
			name: "request_epoch_zero",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 1, Mode: 0o600, DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 0},
		},
		{
			name: "request_epoch_over_max_int64",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 1, Mode: 0o600, DiskId: testValidDiskID, Branch: testBranchMain, Epoch: testOverMaxInt64},
		},
		{
			name: "request_parent_ino_zero",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 0, Mode: 0o600, DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1},
		},
		{
			name: "request_parent_ino_over_max_int64",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: testOverMaxInt64, Mode: 0o600, DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1},
		},
		{
			name: "request_uid_over_max_int32",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 1, Mode: 0o600, Uid: math.MaxInt32 + 1, DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1},
		},
		{
			name: "request_gid_over_max_int32",
			msg:  &storagev1.CreateTmpfileRequest{ParentIno: 1, Mode: 0o600, Gid: math.MaxInt32 + 1, DiskId: testValidDiskID, Branch: testBranchMain, Epoch: 1},
		},
		{
			name: "response_missing_inode",
			msg:  &storagev1.CreateTmpfileResponse{EntryTimeoutSeconds: 1.0, AttrTimeoutSeconds: 1.0},
		},
		{
			name: "response_negative_entry_timeout",
			msg:  &storagev1.CreateTmpfileResponse{Inode: validInode, EntryTimeoutSeconds: -0.1, AttrTimeoutSeconds: 1.0},
		},
		{
			name: "response_negative_attr_timeout",
			msg:  &storagev1.CreateTmpfileResponse{Inode: validInode, EntryTimeoutSeconds: 1.0, AttrTimeoutSeconds: -0.1},
		},
	}

	runSubtests(t, cases, func(tc invalidCase) string { return tc.name }, func(t *testing.T, tc invalidCase) {
		gotErr := v.Validate(tc.msg)
		require.Error(t, gotErr, "case=%s: want validation failure with reason %s, got nil", tc.name, validator.ReasonInvalidArgument)
		assertValidationReason(t, gotErr, validator.ReasonInvalidArgument)
	})
}
