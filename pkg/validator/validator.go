// Package validator 提供 Storage RPC 请求参数的业务校验器。
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

// ErrorDomain 定义与 storage 领域层一致的 ErrorInfo 错误领域标识符。
const ErrorDomain = "bowl.storage"

// POSIX errno reason 常量，与 meta.ToStatus 映射契约保持严格一致。
const (
	ReasonInvalidArgument = "EINVAL"
	ReasonNameTooLong     = "ENAMETOOLONG"
	ReasonRange           = "ERANGE"
	ReasonTooBig          = "E2BIG"
)

// storageMessages 汇总 StorageService 全部现有的请求与响应消息，用于启动期预编译校验规则。
var storageMessages = []proto.Message{
	// 请求消息 (Requests)
	&storagev1.MountDiskRequest{},
	&storagev1.RenewMountRequest{},
	&storagev1.UnmountDiskRequest{},
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

	// 响应消息 (Responses)
	&storagev1.MountDiskResponse{},
	&storagev1.RenewMountResponse{},
	&storagev1.UnmountDiskResponse{},
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

	// 嵌套与共享消息
	&storagev1.DirEntry{},
	&storagev1.DirEntryPlus{},
	&storagev1.Inode{},
	&storagev1.InodeAttribute{},
	&storagev1.Disk{},
	&storagev1.DiffNodeSummary{},
	&storagev1.DiffEntry{},
	&storagev1.DiffSummary{},
	&storagev1.MergeConflict{},
	&storagev1.MergeConflictReport{},
}

// ValidationError 包装 protovalidate.ValidationError，并将其边界违规映射为携带 google.rpc.ErrorInfo 的 gRPC Status。
// 同时实现 Unwrap() error 与 GRPCStatus() *status.Status，确保 errors.As(..., **protovalidate.ValidationError)、
// status.FromError 及 meta.ToStatus 均可无缝提取底层违规详情与对应的 POSIX errno reason。
type ValidationError struct {
	err    *protovalidate.ValidationError
	reason string
	domain string
}

// Error 返回底层 protovalidate 校验错误信息。
func (e *ValidationError) Error() string {
	return e.err.Error()
}

// Unwrap 返回底层 *protovalidate.ValidationError。
func (e *ValidationError) Unwrap() error {
	return e.err
}

// Reason 返回映射后的标准 POSIX errno 名称（如 "EINVAL"、"ERANGE"、"E2BIG"、"ENAMETOOLONG"）。
func (e *ValidationError) Reason() string {
	return e.reason
}

// Domain 返回错误所属的服务领域标识符（如 "bowl.storage"）。
func (e *ValidationError) Domain() string {
	if e.domain != "" {
		return e.domain
	}
	return ErrorDomain
}

// GRPCStatus 构造携带 google.rpc.ErrorInfo 的 InvalidArgument 状态。
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

// NewStorageValidator 创建并初始化一个预编译好 StorageService 全部请求、响应与实体消息的 protovalidate.Validator。
// 通过 protovalidate.WithMessages 选项在启动阶段编译 CEL 约束规则，避免在首次运行时产生编译延迟；
// 同时将字段边界校验违规自动关联至对应的 POSIX errno reason（ERANGE、E2BIG、ENAMETOOLONG、EINVAL）。
func NewStorageValidator() (protovalidate.Validator, error) {
	inner, err := protovalidate.New(protovalidate.WithMessages(storageMessages...))
	if err != nil {
		return nil, err
	}
	return &storageValidator{inner: inner}, nil
}

// ErrnoReason 从校验错误中提取对应的 POSIX errno reason。
// 支持 NewStorageValidator 返回的 *ValidationError 以及原始 *protovalidate.ValidationError。
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

// ToStatus 将校验错误转换为携带 google.rpc.ErrorInfo 的 gRPC *status.Status。
// 若 err 为 protovalidate 校验违规（含原始 *protovalidate.ValidationError），
// 按字段与规则优先级映射为 codes.InvalidArgument 及对应的 ERANGE / E2BIG / ENAMETOOLONG / EINVAL。
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

// ToGRPCError 将校验错误转换为携带 google.rpc.ErrorInfo 的 gRPC error。
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

// translateValidationReason 按照与 meta.Manager 静态校验完全一致的求值优先级，
// 将 protovalidate 违规项还原为对应的 POSIX errno reason。
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
		// 消息级约束（如 update_disk_has_update）无 field descriptor，返回空 domain，保留调用方的 defaultDomain
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

	if ruleID == "update_disk_has_update" || ruleID == "write_request_offset_len_overflow" || ruleID == "mount_disk_force_requires_rw" {
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
	// 第 1 优先级：基础整型/标识符与模式/标志字段校验 -> EINVAL
	case "storage.v1.MountDiskRequest.disk_id",
		"storage.v1.RenewMountRequest.disk_id",
		"storage.v1.RenewMountRequest.epoch",
		"storage.v1.UnmountDiskRequest.disk_id",
		"storage.v1.UnmountDiskRequest.epoch",
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

	// 第 2/3 优先级：Xattr 属性名称长度边界 (仅 >255 为 ERANGE，空名称或含 NUL 为 EINVAL)
	case "storage.v1.SetXattrRequest.name",
		"storage.v1.GetXattrRequest.name",
		"storage.v1.RemoveXattrRequest.name",
		"storage.v1.ListXattrResponse.names":
		if isMaxLen {
			return 20, ReasonRange
		}
		return 30, ReasonInvalidArgument

	// 第 2/3 优先级：主目录项名称超长 (>255) -> ENAMETOOLONG；为空或包含 '/' / NUL -> EINVAL
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

	// 第 4/5 优先级：Rename 目标新名称超长 (>255) -> ENAMETOOLONG；为空或包含 '/' / NUL -> EINVAL
	case "storage.v1.RenameRequest.new_name":
		if isMaxLen {
			return 40, ReasonNameTooLong
		}
		return 50, ReasonInvalidArgument

	// 第 4/5 优先级：Symlink / Readlink 目标路径为空或包含 NUL -> EINVAL；超长 (>4095) -> ENAMETOOLONG
	case "storage.v1.SymlinkRequest.target",
		"storage.v1.ReadlinkResponse.target":
		if isMaxLen {
			return 50, ReasonNameTooLong
		}
		return 40, ReasonInvalidArgument

	// 第 4 优先级：Xattr 属性值超长 (>65536) -> ERANGE (对齐 JuiceFS Linux 行为)
	case "storage.v1.SetXattrRequest.value",
		"storage.v1.GetXattrResponse.value":
		if isMaxLen {
			return 40, ReasonRange
		}
		return 45, ReasonInvalidArgument
	}

	return 60, ReasonInvalidArgument
}
