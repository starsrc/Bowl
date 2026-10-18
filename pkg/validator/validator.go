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

// ErrorDomain 定义与领域层一致的 ErrorInfo 错误领域标识符。
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
	&storagev1.OpenRequest{},
	&storagev1.FlushRequest{},
	&storagev1.FsyncRequest{},
	&storagev1.ReleaseRequest{},
	&storagev1.OpenDirRequest{},
	&storagev1.ReleaseDirRequest{},
	&storagev1.ReadDirRequest{},
	&storagev1.ReadDirPlusRequest{},
	&storagev1.FallocateRequest{},
	&storagev1.SetXattrRequest{},
	&storagev1.GetXattrRequest{},
	&storagev1.ListXattrRequest{},
	&storagev1.RemoveXattrRequest{},
	&storagev1.GetLkRequest{},
	&storagev1.SetLkRequest{},
	&storagev1.SetLkwRequest{},

	// 响应消息 (Responses)
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
	&storagev1.OpenResponse{},
	&storagev1.FlushResponse{},
	&storagev1.FsyncResponse{},
	&storagev1.ReleaseResponse{},
	&storagev1.OpenDirResponse{},
	&storagev1.ReleaseDirResponse{},
	&storagev1.ReadDirResponse{},
	&storagev1.ReadDirPlusResponse{},
	&storagev1.FallocateResponse{},
	&storagev1.SetXattrResponse{},
	&storagev1.GetXattrResponse{},
	&storagev1.ListXattrResponse{},
	&storagev1.RemoveXattrResponse{},
	&storagev1.GetLkResponse{},
	&storagev1.SetLkResponse{},
	&storagev1.SetLkwResponse{},

	// 嵌套与共享消息
	&storagev1.DirEntry{},
	&storagev1.DirEntryPlus{},
	&storagev1.FileLock{},
	&storagev1.Inode{},
	&storagev1.InodeAttribute{},
}

// ValidationError 包装 protovalidate.ValidationError，并将其边界违规映射为携带 google.rpc.ErrorInfo 的 gRPC Status。
// 同时实现 Unwrap() error 与 GRPCStatus() *status.Status，确保 errors.As(..., **protovalidate.ValidationError)、
// status.FromError 及 meta.ToStatus 均可无缝提取底层违规详情与对应的 POSIX errno reason。
type ValidationError struct {
	err    *protovalidate.ValidationError
	reason string
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

// GRPCStatus 构造携带 google.rpc.ErrorInfo (domain = "bowl.storage") 的 InvalidArgument 状态。
func (e *ValidationError) GRPCStatus() *status.Status {
	return buildInvalidArgumentStatus(e.err.Error(), e.reason)
}

type storageValidator struct {
	inner protovalidate.Validator
}

func (v *storageValidator) Validate(msg proto.Message, options ...protovalidate.ValidationOption) error {
	err := v.inner.Validate(msg, options...)
	if err == nil {
		return nil
	}
	var valErr *protovalidate.ValidationError
	if errors.As(err, &valErr) {
		return &ValidationError{
			err:    valErr,
			reason: translateValidationReason(valErr),
		}
	}
	return err
}

// NewStorageValidator 创建并初始化一个预编译好 StorageService 全部请求与响应消息的 protovalidate.Validator。
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
	var storageValErr *ValidationError
	if errors.As(err, &storageValErr) {
		return storageValErr.Reason(), true
	}
	var protoValErr *protovalidate.ValidationError
	if errors.As(err, &protoValErr) {
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
	if reason, ok := ErrnoReason(err); ok {
		return buildInvalidArgumentStatus(err.Error(), reason)
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

func buildInvalidArgumentStatus(msg, reason string) *status.Status {
	st := status.New(codes.InvalidArgument, msg)
	stWithDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: ErrorDomain,
	})
	if detailErr == nil {
		return stWithDetails
	}
	return st
}

// translateValidationReason 按照与 meta.Manager 静态校验完全一致的求值优先级，
// 将 protovalidate 违规项还原为对应的 POSIX errno reason。
func translateValidationReason(valErr *protovalidate.ValidationError) string {
	bestPriority := 100
	bestReason := ReasonInvalidArgument

	for _, v := range valErr.Violations {
		priority, reason := classifyViolation(v)
		if priority < bestPriority {
			bestPriority = priority
			bestReason = reason
		}
	}

	return bestReason
}

func classifyViolation(v *protovalidate.Violation) (int, string) {
	if v == nil || v.Proto == nil {
		return 60, ReasonInvalidArgument
	}

	ruleID := v.Proto.GetRuleId()
	isMinLen := ruleID == "bytes.min_len" || strings.HasSuffix(ruleID, ".bytes.min_len")
	isMaxLen := ruleID == "bytes.max_len" || strings.HasSuffix(ruleID, ".bytes.max_len")

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
	// 第 1 优先级：Inode 编号与 SetXattr.flags 基础整型范围校验 -> EINVAL
	case "storage.v1.LookupRequest.parent_ino",
		"storage.v1.GetAttrRequest.ino",
		"storage.v1.SetAttrRequest.ino",
		"storage.v1.MknodRequest.parent_ino",
		"storage.v1.MkdirRequest.parent_ino",
		"storage.v1.SymlinkRequest.parent_ino",
		"storage.v1.ReadlinkRequest.ino",
		"storage.v1.LinkRequest.ino",
		"storage.v1.LinkRequest.new_parent_ino",
		"storage.v1.UnlinkRequest.parent_ino",
		"storage.v1.RmdirRequest.parent_ino",
		"storage.v1.RenameRequest.old_parent_ino",
		"storage.v1.RenameRequest.new_parent_ino",
		"storage.v1.CreateRequest.parent_ino",
		"storage.v1.OpenRequest.ino",
		"storage.v1.FlushRequest.ino",
		"storage.v1.FsyncRequest.ino",
		"storage.v1.ReleaseRequest.ino",
		"storage.v1.OpenDirRequest.ino",
		"storage.v1.ReleaseDirRequest.ino",
		"storage.v1.ReadDirRequest.ino",
		"storage.v1.ReadDirPlusRequest.ino",
		"storage.v1.FallocateRequest.ino",
		"storage.v1.SetXattrRequest.ino",
		"storage.v1.SetXattrRequest.flags",
		"storage.v1.GetXattrRequest.ino",
		"storage.v1.ListXattrRequest.ino",
		"storage.v1.RemoveXattrRequest.ino",
		"storage.v1.GetLkRequest.ino",
		"storage.v1.SetLkRequest.ino",
		"storage.v1.SetLkwRequest.ino",
		"storage.v1.DirEntry.ino",
		"storage.v1.DirEntryPlus.ino",
		"storage.v1.Inode.ino":
		return 10, ReasonInvalidArgument

	// 第 2/3 优先级：Xattr 属性名称长度边界 (0 或 >255) -> ERANGE；包含 NUL -> EINVAL
	case "storage.v1.SetXattrRequest.name",
		"storage.v1.GetXattrRequest.name",
		"storage.v1.RemoveXattrRequest.name",
		"storage.v1.ListXattrResponse.names":
		if isMinLen || isMaxLen {
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
		"storage.v1.DirEntryPlus.name":
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

	// 第 4 优先级：Xattr 属性值超长 (>65536) -> E2BIG
	case "storage.v1.SetXattrRequest.value",
		"storage.v1.GetXattrResponse.value":
		if isMaxLen {
			return 40, ReasonTooBig
		}
		return 45, ReasonInvalidArgument
	}

	return 60, ReasonInvalidArgument
}
