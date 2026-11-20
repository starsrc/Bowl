package validator

import (
	"errors"
	"strings"

	"buf.build/go/protovalidate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const ErrorDomain = "bowl.storage"

const (
	ReasonInvalidArgument = "EINVAL"
	ReasonNameTooLong     = "ENAMETOOLONG"
	ReasonRange           = "ERANGE"
	ReasonTooBig          = "E2BIG"
)

var storageMessages = []proto.Message{

	&storagev1.MountDiskRequest{},
	&storagev1.RenewMountRequest{},
	&storagev1.UnmountDiskRequest{},
	&storagev1.ListMountsRequest{},
	&storagev1.EvictMountsRequest{},
	&storagev1.ReclaimOrphanRequest{},
	&storagev1.LookupRequest{},
	&storagev1.GetAttrRequest{},
	&storagev1.SetAttrRequest{},
	&storagev1.MknodRequest{},
	&storagev1.MkdirRequest{},
	&storagev1.SymlinkRequest{},
	&storagev1.ReadlinkRequest{},
	&storagev1.LinkRequest{},
	&storagev1.UnlinkRequest{},
	&storagev1.RmdirRequest{},
	&storagev1.RenameRequest{},
	&storagev1.CreateRequest{},
	&storagev1.CreateTmpfileRequest{},
	&storagev1.ReadRequest{},
	&storagev1.WriteRequest{},
	&storagev1.FsyncRequest{},
	&storagev1.FsyncDirRequest{},
	&storagev1.ReadDirRequest{},
	&storagev1.ReadDirPlusRequest{},
	&storagev1.FallocateRequest{},
	&storagev1.SetXattrRequest{},
	&storagev1.GetXattrRequest{},
	&storagev1.ListXattrRequest{},
	&storagev1.RemoveXattrRequest{},
	&storagev1.CreateDiskRequest{},
	&storagev1.GetDiskRequest{},
	&storagev1.ListDisksRequest{},
	&storagev1.UpdateDiskRequest{},
	&storagev1.DeleteDiskRequest{},
	&storagev1.ForkDiskRequest{},
	&storagev1.StatFSRequest{},
	&storagev1.DiffDiskRequest{},
	&storagev1.ReadCommitNodeRequest{},
	&storagev1.MergeDiskRequest{},

	&storagev1.MountDiskResponse{},
	&storagev1.RenewMountResponse{},
	&storagev1.UnmountDiskResponse{},
	&storagev1.ListMountsResponse{},
	&storagev1.EvictMountsResponse{},
	&storagev1.ReclaimOrphanResponse{},
	&storagev1.LookupResponse{},
	&storagev1.GetAttrResponse{},
	&storagev1.SetAttrResponse{},
	&storagev1.MknodResponse{},
	&storagev1.MkdirResponse{},
	&storagev1.SymlinkResponse{},
	&storagev1.ReadlinkResponse{},
	&storagev1.LinkResponse{},
	&storagev1.UnlinkResponse{},
	&storagev1.RmdirResponse{},
	&storagev1.RenameResponse{},
	&storagev1.CreateResponse{},
	&storagev1.CreateTmpfileResponse{},
	&storagev1.ReadResponse{},
	&storagev1.WriteResponse{},
	&storagev1.FsyncResponse{},
	&storagev1.FsyncDirResponse{},
	&storagev1.ReadDirResponse{},
	&storagev1.ReadDirPlusResponse{},
	&storagev1.FallocateResponse{},
	&storagev1.SetXattrResponse{},
	&storagev1.GetXattrResponse{},
	&storagev1.ListXattrResponse{},
	&storagev1.RemoveXattrResponse{},
	&storagev1.CreateDiskResponse{},
	&storagev1.GetDiskResponse{},
	&storagev1.ListDisksResponse{},
	&storagev1.UpdateDiskResponse{},
	&storagev1.DeleteDiskResponse{},
	&storagev1.ForkDiskResponse{},
	&storagev1.StatFSResponse{},
	&storagev1.DiffDiskResponse{},
	&storagev1.ReadCommitNodeResponse{},
	&storagev1.MergeDiskResponse{},

	&storagev1.DirEntry{},
	&storagev1.DirEntryPlus{},
	&storagev1.Inode{},
	&storagev1.InodeAttribute{},
	&storagev1.Disk{},
	&storagev1.MountLease{},
	&storagev1.DiffNodeSummary{},
	&storagev1.DiffEntry{},
	&storagev1.DiffSummary{},
	&storagev1.MergeConflict{},
	&storagev1.MergeConflictReport{},
}

type ValidationError struct {
	err    *protovalidate.ValidationError
	reason string
	domain string
}

func (e *ValidationError) Error() string {
	return e.err.Error()
}

func (e *ValidationError) Unwrap() error {
	return e.err
}

func (e *ValidationError) Reason() string {
	return e.reason
}

func (e *ValidationError) Domain() string {
	if e.domain != "" {
		return e.domain
	}
	return ErrorDomain
}

func (e *ValidationError) GRPCStatus() *status.Status {
	return buildInvalidArgumentStatus(e.err.Error(), e.reason, e.Domain())
}

type storageValidator struct {
	inner protovalidate.Validator
}

func (v *storageValidator) Validate(msg proto.Message, options ...protovalidate.ValidationOption) error {
	err := v.inner.Validate(msg, options...)
	if err == nil {
		return nil
	}
	if valErr, ok := errors.AsType[*protovalidate.ValidationError](err); ok {
		reason, domain := translateValidation(valErr, ErrorDomain)
		return &ValidationError{
			err:    valErr,
			reason: reason,
			domain: domain,
		}
	}
	return err
}

func NewStorageValidator() (protovalidate.Validator, error) {
	inner, err := protovalidate.New(protovalidate.WithMessages(storageMessages...))
	if err != nil {
		return nil, err
	}
	return &storageValidator{inner: inner}, nil
}

func ErrnoReason(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if storageValErr, ok := errors.AsType[*ValidationError](err); ok {
		return storageValErr.Reason(), true
	}
	if protoValErr, ok := errors.AsType[*protovalidate.ValidationError](err); ok {
		return translateValidationReason(protoValErr), true
	}
	return "", false
}

func ToStatus(err error) *status.Status {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok {
		return s
	}
	if storageValErr, ok := errors.AsType[*ValidationError](err); ok {
		return buildInvalidArgumentStatus(err.Error(), storageValErr.Reason(), storageValErr.Domain())
	}
	if protoValErr, ok := errors.AsType[*protovalidate.ValidationError](err); ok {
		reason, domain := translateValidation(protoValErr, ErrorDomain)
		return buildInvalidArgumentStatus(err.Error(), reason, domain)
	}
	return status.New(codes.Unknown, err.Error())
}

func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return ToStatus(err).Err()
}

func buildInvalidArgumentStatus(msg, reason, domain string) *status.Status {
	if domain == "" {
		domain = ErrorDomain
	}
	st := status.New(codes.InvalidArgument, msg)
	stWithDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: domain,
	})
	if detailErr == nil {
		return stWithDetails
	}
	return st
}

func translateValidationReason(valErr *protovalidate.ValidationError) string {
	reason, _ := translateValidation(valErr, ErrorDomain)
	return reason
}

func translateValidation(valErr *protovalidate.ValidationError, defaultDomain string) (reason, domain string) {
	bestPriority := 100
	bestReason := ReasonInvalidArgument
	bestDomain := defaultDomain
	if bestDomain == "" {
		bestDomain = ErrorDomain
	}

	for _, v := range valErr.Violations {
		priority, vReason, vDomain := classifyViolation(v)
		if priority < bestPriority {
			bestPriority = priority
			bestReason = vReason
			if vDomain != "" {
				bestDomain = vDomain
			}
		}
	}

	return bestReason, bestDomain
}

func classifyViolation(v *protovalidate.Violation) (priority int, reason, domain string) {
	priority, reason = classifyViolationInternal(v)
	fullName := ""
	if v != nil && v.FieldDescriptor != nil {
		fullName = string(v.FieldDescriptor.FullName())
	}

	domain = ""
	if strings.HasPrefix(fullName, "storage.v1.") {
		domain = ErrorDomain
	} else if fullName == "" {

		domain = ""
	}
	return priority, reason, domain
}

func classifyViolationInternal(v *protovalidate.Violation) (priority int, reason string) {
	if v == nil || v.Proto == nil {
		return 60, ReasonInvalidArgument
	}

	ruleID := v.Proto.GetRuleId()
	isMaxLen := ruleID == "bytes.max_len" || strings.HasSuffix(ruleID, ".bytes.max_len") ||
		ruleID == "string.max_bytes" || strings.HasSuffix(ruleID, ".string.max_bytes")

	if ruleID == "update_disk_has_update" || ruleID == "write_request_offset_len_overflow" || ruleID == "renew_mount_epoch_matches_mode" {
		return 10, ReasonInvalidArgument
	}

	fullName := ""
	if v.FieldDescriptor != nil {
		fullName = string(v.FieldDescriptor.FullName())
	} else if elems := v.Proto.GetField().GetElements(); len(elems) > 0 {
		lastField := elems[len(elems)-1].GetFieldName()
		if lastField == "names" {
			fullName = "storage.v1.ListXattrResponse.names"
		}
	}

	switch fullName {

	case "storage.v1.MountDiskRequest.disk_id",
		"storage.v1.RenewMountRequest.disk_id",
		"storage.v1.RenewMountRequest.mount_id",
		"storage.v1.RenewMountRequest.epoch",
		"storage.v1.UnmountDiskRequest.disk_id",
		"storage.v1.UnmountDiskRequest.mount_id",
		"storage.v1.ListMountsRequest.disk_id",
		"storage.v1.EvictMountsRequest.disk_id",
		"storage.v1.EvictMountsRequest.mount_id",
		"storage.v1.LookupRequest.disk_id",
		"storage.v1.GetAttrRequest.disk_id",
		"storage.v1.SetAttrRequest.disk_id",
		"storage.v1.SetAttrRequest.epoch",
		"storage.v1.MknodRequest.disk_id",
		"storage.v1.MknodRequest.epoch",
		"storage.v1.MkdirRequest.disk_id",
		"storage.v1.MkdirRequest.epoch",
		"storage.v1.SymlinkRequest.disk_id",
		"storage.v1.SymlinkRequest.epoch",
		"storage.v1.ReadlinkRequest.disk_id",
		"storage.v1.LinkRequest.disk_id",
		"storage.v1.LinkRequest.epoch",
		"storage.v1.UnlinkRequest.disk_id",
		"storage.v1.UnlinkRequest.epoch",
		"storage.v1.RmdirRequest.disk_id",
		"storage.v1.RmdirRequest.epoch",
		"storage.v1.RenameRequest.disk_id",
		"storage.v1.RenameRequest.epoch",
		"storage.v1.CreateRequest.disk_id",
		"storage.v1.CreateRequest.epoch",
		"storage.v1.CreateTmpfileRequest.disk_id",
		"storage.v1.CreateTmpfileRequest.epoch",
		"storage.v1.ReadRequest.disk_id",
		"storage.v1.WriteRequest.disk_id",
		"storage.v1.WriteRequest.epoch",
		"storage.v1.FsyncRequest.disk_id",
		"storage.v1.FsyncRequest.epoch",
		"storage.v1.FsyncDirRequest.disk_id",
		"storage.v1.FsyncDirRequest.epoch",
		"storage.v1.ReadDirRequest.disk_id",
		"storage.v1.ReadDirPlusRequest.disk_id",
		"storage.v1.FallocateRequest.disk_id",
		"storage.v1.FallocateRequest.epoch",
		"storage.v1.SetXattrRequest.disk_id",
		"storage.v1.SetXattrRequest.epoch",
		"storage.v1.GetXattrRequest.disk_id",
		"storage.v1.ListXattrRequest.disk_id",
		"storage.v1.RemoveXattrRequest.disk_id",
		"storage.v1.RemoveXattrRequest.epoch",
		"storage.v1.StatFSRequest.disk_id",
		"storage.v1.ReclaimOrphanRequest.disk_id",
		"storage.v1.ReclaimOrphanRequest.epoch",
		"storage.v1.ReclaimOrphanRequest.ino",
		"storage.v1.LookupRequest.parent_ino",
		"storage.v1.GetAttrRequest.ino",
		"storage.v1.SetAttrRequest.ino",
		"storage.v1.SetAttrRequest.mode",
		"storage.v1.SetAttrRequest.uid",
		"storage.v1.SetAttrRequest.gid",
		"storage.v1.SetAttrRequest.size",
		"storage.v1.SetAttrRequest.fh",
		"storage.v1.SetAttrRequest.flags",
		"storage.v1.MknodRequest.parent_ino",
		"storage.v1.MknodRequest.mode",
		"storage.v1.MknodRequest.uid",
		"storage.v1.MknodRequest.gid",
		"storage.v1.MknodRequest.rdev",
		"storage.v1.MkdirRequest.parent_ino",
		"storage.v1.MkdirRequest.mode",
		"storage.v1.MkdirRequest.uid",
		"storage.v1.MkdirRequest.gid",
		"storage.v1.SymlinkRequest.parent_ino",
		"storage.v1.SymlinkRequest.uid",
		"storage.v1.SymlinkRequest.gid",
		"storage.v1.ReadlinkRequest.ino",
		"storage.v1.LinkRequest.ino",
		"storage.v1.LinkRequest.new_parent_ino",
		"storage.v1.UnlinkRequest.parent_ino",
		"storage.v1.RmdirRequest.parent_ino",
		"storage.v1.RenameRequest.old_parent_ino",
		"storage.v1.RenameRequest.new_parent_ino",
		"storage.v1.RenameRequest.flags",
		"storage.v1.CreateRequest.parent_ino",
		"storage.v1.CreateRequest.mode",
		"storage.v1.CreateRequest.uid",
		"storage.v1.CreateRequest.gid",
		"storage.v1.CreateRequest.flags",
		"storage.v1.CreateTmpfileRequest.parent_ino",
		"storage.v1.CreateTmpfileRequest.mode",
		"storage.v1.CreateTmpfileRequest.uid",
		"storage.v1.CreateTmpfileRequest.gid",
		"storage.v1.CreateTmpfileRequest.exclusive",
		"storage.v1.ReadRequest.ino",
		"storage.v1.ReadRequest.fh",
		"storage.v1.ReadRequest.offset",
		"storage.v1.ReadRequest.size",
		"storage.v1.WriteRequest.ino",
		"storage.v1.WriteRequest.fh",
		"storage.v1.WriteRequest.offset",
		"storage.v1.WriteRequest.data",
		"storage.v1.WriteResponse.size",
		"storage.v1.WriteResponse.inode",
		"storage.v1.FsyncRequest.ino",
		"storage.v1.FsyncRequest.fh",
		"storage.v1.FsyncRequest.datasync",
		"storage.v1.FsyncDirRequest.ino",
		"storage.v1.FsyncDirRequest.fh",
		"storage.v1.FsyncDirRequest.datasync",
		"storage.v1.ReadDirRequest.ino",
		"storage.v1.ReadDirRequest.fh",
		"storage.v1.ReadDirRequest.offset",
		"storage.v1.ReadDirRequest.limit",
		"storage.v1.ReadDirPlusRequest.ino",
		"storage.v1.ReadDirPlusRequest.fh",
		"storage.v1.ReadDirPlusRequest.offset",
		"storage.v1.ReadDirPlusRequest.limit",
		"storage.v1.FallocateRequest.ino",
		"storage.v1.FallocateRequest.fh",
		"storage.v1.FallocateRequest.offset",
		"storage.v1.FallocateRequest.length",
		"storage.v1.FallocateRequest.mode",
		"storage.v1.SetXattrRequest.ino",
		"storage.v1.SetXattrRequest.flags",
		"storage.v1.GetXattrRequest.ino",
		"storage.v1.ListXattrRequest.ino",
		"storage.v1.RemoveXattrRequest.ino",
		"storage.v1.DirEntry.ino",
		"storage.v1.DirEntryPlus.ino",
		"storage.v1.Inode.ino",
		"storage.v1.InodeAttribute.mode",
		"storage.v1.InodeAttribute.uid",
		"storage.v1.InodeAttribute.gid",
		"storage.v1.InodeAttribute.rdev",
		"storage.v1.InodeAttribute.size",
		"storage.v1.Disk.disk_id",
		"storage.v1.Disk.root_ino",
		"storage.v1.Disk.max_bytes",
		"storage.v1.Disk.used_bytes",
		"storage.v1.Disk.used_inodes",
		"storage.v1.Disk.created_at",
		"storage.v1.Disk.updated_at",
		"storage.v1.Disk.parent_disk_id",
		"storage.v1.CreateDiskRequest.max_bytes",
		"storage.v1.CreateDiskResponse.disk",
		"storage.v1.GetDiskRequest.disk_id",
		"storage.v1.GetDiskResponse.disk",
		"storage.v1.ListDisksRequest.page_size",
		"storage.v1.UpdateDiskRequest.disk_id",
		"storage.v1.UpdateDiskRequest.max_bytes",
		"storage.v1.UpdateDiskResponse.disk",
		"storage.v1.DeleteDiskRequest.disk_id",
		"storage.v1.ForkDiskRequest.source_disk_id",
		"storage.v1.ForkDiskRequest.max_bytes",
		"storage.v1.ForkDiskResponse.disk",
		"storage.v1.StatFSRequest.ino":
		return 10, ReasonInvalidArgument

	case "storage.v1.SetXattrRequest.name",
		"storage.v1.GetXattrRequest.name",
		"storage.v1.RemoveXattrRequest.name",
		"storage.v1.ListXattrResponse.names":
		if isMaxLen {
			return 20, ReasonRange
		}
		return 30, ReasonInvalidArgument

	case "storage.v1.LookupRequest.name",
		"storage.v1.MknodRequest.name",
		"storage.v1.MkdirRequest.name",
		"storage.v1.SymlinkRequest.name",
		"storage.v1.LinkRequest.new_name",
		"storage.v1.UnlinkRequest.name",
		"storage.v1.RmdirRequest.name",
		"storage.v1.RenameRequest.old_name",
		"storage.v1.CreateRequest.name",
		"storage.v1.DirEntry.name",
		"storage.v1.DirEntryPlus.name",
		"storage.v1.Disk.name",
		"storage.v1.CreateDiskRequest.name",
		"storage.v1.UpdateDiskRequest.name",
		"storage.v1.ForkDiskRequest.name":
		if isMaxLen {
			return 20, ReasonNameTooLong
		}
		return 30, ReasonInvalidArgument

	case "storage.v1.RenameRequest.new_name":
		if isMaxLen {
			return 40, ReasonNameTooLong
		}
		return 50, ReasonInvalidArgument

	case "storage.v1.SymlinkRequest.target",
		"storage.v1.ReadlinkResponse.target":
		if isMaxLen {
			return 50, ReasonNameTooLong
		}
		return 40, ReasonInvalidArgument

	case "storage.v1.SetXattrRequest.value",
		"storage.v1.GetXattrResponse.value":
		if isMaxLen {
			return 40, ReasonRange
		}
		return 45, ReasonInvalidArgument
	}

	return 60, ReasonInvalidArgument
}
