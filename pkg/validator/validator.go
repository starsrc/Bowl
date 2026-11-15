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

// POSIX errno reason 常量，与服务端 status 映射契约保持一致。
const (
	ReasonInvalidArgument = "EINVAL"
	ReasonNameTooLong     = "ENAMETOOLONG"
	ReasonRange           = "ERANGE"
)

// storageMessages 汇总 StorageService 全部现有的请求、响应与嵌套消息，用于启动期预编译校验规则。
var storageMessages = []proto.Message{
	// 请求消息
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

	// 响应消息
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

type fieldRule struct {
	priority       int
	reason         string
	hasMaxLen      bool
	maxLenPriority int
	maxLenReason   string
}

var messageRulePriorities = map[string]fieldRule{
	"update_disk_has_update":             {priority: 10, reason: ReasonInvalidArgument},
	"write_request_offset_len_overflow":  {priority: 10, reason: ReasonInvalidArgument},
	"renew_mount_epoch_matches_mode":     {priority: 10, reason: ReasonInvalidArgument},
	"setattr_atime_conflict":             {priority: 10, reason: ReasonInvalidArgument},
	"setattr_mtime_conflict":             {priority: 10, reason: ReasonInvalidArgument},
	"setattr_has_update":                 {priority: 10, reason: ReasonInvalidArgument},
	"diff_disk_source_not_equal_target":  {priority: 10, reason: ReasonInvalidArgument},
	"merge_disk_source_not_equal_target": {priority: 10, reason: ReasonInvalidArgument},
}

var fieldClassifications = buildFieldClassifications()

func registerFieldRules(dst map[string]fieldRule, rule fieldRule, names ...string) {
	for _, name := range names {
		dst[name] = rule
	}
}

func buildFieldClassifications() map[string]fieldRule {
	dst := make(map[string]fieldRule, 160)

	// 第 1 优先级：标识符、整型边界、布尔与时间戳等基础约束，映射为 EINVAL
	registerFieldRules(
		dst,
		fieldRule{priority: 10, reason: ReasonInvalidArgument},
		"storage.v1.MountDiskRequest.disk_id",
		"storage.v1.RenewMountRequest.disk_id",
		"storage.v1.RenewMountRequest.mount_id",
		"storage.v1.RenewMountRequest.epoch",
		"storage.v1.UnmountDiskRequest.disk_id",
		"storage.v1.UnmountDiskRequest.mount_id",
		"storage.v1.LookupRequest.parent_ino",
		"storage.v1.LookupRequest.disk_id",
		"storage.v1.LookupResponse.inode",
		"storage.v1.LookupResponse.entry_timeout_seconds",
		"storage.v1.LookupResponse.attr_timeout_seconds",
		"storage.v1.GetAttrRequest.ino",
		"storage.v1.GetAttrRequest.disk_id",
		"storage.v1.GetAttrResponse.inode",
		"storage.v1.GetAttrResponse.attr_timeout_seconds",
		"storage.v1.SetAttrRequest.ino",
		"storage.v1.SetAttrRequest.uid",
		"storage.v1.SetAttrRequest.gid",
		"storage.v1.SetAttrRequest.size",
		"storage.v1.SetAttrRequest.fh",
		"storage.v1.SetAttrRequest.disk_id",
		"storage.v1.SetAttrRequest.flags",
		"storage.v1.SetAttrRequest.epoch",
		"storage.v1.SetAttrResponse.inode",
		"storage.v1.SetAttrResponse.attr_timeout_seconds",
		"storage.v1.MknodRequest.parent_ino",
		"storage.v1.MknodRequest.uid",
		"storage.v1.MknodRequest.gid",
		"storage.v1.MknodRequest.disk_id",
		"storage.v1.MknodRequest.epoch",
		"storage.v1.MknodResponse.inode",
		"storage.v1.MknodResponse.entry_timeout_seconds",
		"storage.v1.MknodResponse.attr_timeout_seconds",
		"storage.v1.MkdirRequest.parent_ino",
		"storage.v1.MkdirRequest.uid",
		"storage.v1.MkdirRequest.gid",
		"storage.v1.MkdirRequest.disk_id",
		"storage.v1.MkdirRequest.epoch",
		"storage.v1.MkdirResponse.inode",
		"storage.v1.MkdirResponse.entry_timeout_seconds",
		"storage.v1.MkdirResponse.attr_timeout_seconds",
		"storage.v1.SymlinkRequest.parent_ino",
		"storage.v1.SymlinkRequest.uid",
		"storage.v1.SymlinkRequest.gid",
		"storage.v1.SymlinkRequest.disk_id",
		"storage.v1.SymlinkRequest.epoch",
		"storage.v1.SymlinkResponse.inode",
		"storage.v1.SymlinkResponse.entry_timeout_seconds",
		"storage.v1.SymlinkResponse.attr_timeout_seconds",
		"storage.v1.ReadlinkRequest.ino",
		"storage.v1.ReadlinkRequest.disk_id",
		"storage.v1.LinkRequest.ino",
		"storage.v1.LinkRequest.new_parent_ino",
		"storage.v1.LinkRequest.disk_id",
		"storage.v1.LinkRequest.epoch",
		"storage.v1.LinkResponse.inode",
		"storage.v1.LinkResponse.entry_timeout_seconds",
		"storage.v1.LinkResponse.attr_timeout_seconds",
		"storage.v1.UnlinkRequest.parent_ino",
		"storage.v1.UnlinkRequest.disk_id",
		"storage.v1.UnlinkRequest.epoch",
		"storage.v1.RmdirRequest.parent_ino",
		"storage.v1.RmdirRequest.disk_id",
		"storage.v1.RmdirRequest.epoch",
		"storage.v1.RenameRequest.old_parent_ino",
		"storage.v1.RenameRequest.new_parent_ino",
		"storage.v1.RenameRequest.disk_id",
		"storage.v1.RenameRequest.epoch",
		"storage.v1.CreateRequest.parent_ino",
		"storage.v1.CreateRequest.uid",
		"storage.v1.CreateRequest.gid",
		"storage.v1.CreateRequest.disk_id",
		"storage.v1.CreateRequest.epoch",
		"storage.v1.CreateResponse.inode",
		"storage.v1.CreateResponse.fh",
		"storage.v1.CreateResponse.entry_timeout_seconds",
		"storage.v1.CreateResponse.attr_timeout_seconds",
		"storage.v1.ReadRequest.ino",
		"storage.v1.ReadRequest.fh",
		"storage.v1.ReadRequest.offset",
		"storage.v1.ReadRequest.size",
		"storage.v1.ReadRequest.disk_id",
		"storage.v1.WriteRequest.ino",
		"storage.v1.WriteRequest.fh",
		"storage.v1.WriteRequest.offset",
		"storage.v1.WriteRequest.data",
		"storage.v1.WriteRequest.disk_id",
		"storage.v1.WriteRequest.epoch",
		"storage.v1.WriteResponse.size",
		"storage.v1.WriteResponse.inode",
		"storage.v1.FsyncRequest.ino",
		"storage.v1.FsyncRequest.fh",
		"storage.v1.FsyncRequest.disk_id",
		"storage.v1.FsyncRequest.epoch",
		"storage.v1.FsyncDirRequest.ino",
		"storage.v1.FsyncDirRequest.fh",
		"storage.v1.FsyncDirRequest.disk_id",
		"storage.v1.FsyncDirRequest.epoch",
		"storage.v1.DirEntry.ino",
		"storage.v1.DirEntry.mode",
		"storage.v1.DirEntry.next_offset",
		"storage.v1.ReadDirRequest.ino",
		"storage.v1.ReadDirRequest.fh",
		"storage.v1.ReadDirRequest.offset",
		"storage.v1.ReadDirRequest.limit",
		"storage.v1.ReadDirRequest.disk_id",
		"storage.v1.DirEntryPlus.ino",
		"storage.v1.DirEntryPlus.next_offset",
		"storage.v1.DirEntryPlus.inode",
		"storage.v1.DirEntryPlus.entry_timeout_seconds",
		"storage.v1.DirEntryPlus.attr_timeout_seconds",
		"storage.v1.ReadDirPlusRequest.ino",
		"storage.v1.ReadDirPlusRequest.fh",
		"storage.v1.ReadDirPlusRequest.offset",
		"storage.v1.ReadDirPlusRequest.limit",
		"storage.v1.ReadDirPlusRequest.disk_id",
		"storage.v1.FallocateRequest.ino",
		"storage.v1.FallocateRequest.fh",
		"storage.v1.FallocateRequest.offset",
		"storage.v1.FallocateRequest.length",
		"storage.v1.FallocateRequest.disk_id",
		"storage.v1.FallocateRequest.epoch",
		"storage.v1.SetXattrRequest.ino",
		"storage.v1.SetXattrRequest.flags",
		"storage.v1.SetXattrRequest.disk_id",
		"storage.v1.SetXattrRequest.epoch",
		"storage.v1.GetXattrRequest.ino",
		"storage.v1.GetXattrRequest.disk_id",
		"storage.v1.ListXattrRequest.ino",
		"storage.v1.ListXattrRequest.disk_id",
		"storage.v1.RemoveXattrRequest.ino",
		"storage.v1.RemoveXattrRequest.disk_id",
		"storage.v1.RemoveXattrRequest.epoch",
		"storage.v1.Inode.ino",
		"storage.v1.Inode.attr",
		"storage.v1.InodeAttribute.mode",
		"storage.v1.InodeAttribute.uid",
		"storage.v1.InodeAttribute.gid",
		"storage.v1.InodeAttribute.atime",
		"storage.v1.InodeAttribute.mtime",
		"storage.v1.InodeAttribute.ctime",
		"storage.v1.InodeAttribute.flags",
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
		"storage.v1.StatFSRequest.ino",
		"storage.v1.StatFSRequest.disk_id",
		"storage.v1.ReclaimOrphanRequest.disk_id",
		"storage.v1.ReclaimOrphanRequest.epoch",
		"storage.v1.ReclaimOrphanRequest.ino",
		"storage.v1.DiffDiskRequest.source_disk_id",
		"storage.v1.DiffDiskRequest.target_disk_id",
		"storage.v1.DiffDiskRequest.page_size",
		"storage.v1.ReadCommitNodeRequest.disk_id",
		"storage.v1.ReadCommitNodeRequest.commit_id",
		"storage.v1.ReadCommitNodeRequest.ino",
		"storage.v1.ReadCommitNodeRequest.offset",
		"storage.v1.ReadCommitNodeRequest.size",
		"storage.v1.MergeDiskRequest.source_disk_id",
		"storage.v1.MergeDiskRequest.target_disk_id",
		"storage.v1.MergeDiskRequest.strategy",
		"storage.v1.MergeDiskRequest.max_bytes",
	)

	// 第 2/3 优先级：Xattr 属性名称长度超 255 为 ERANGE，空名称或含 NUL 为 EINVAL
	registerFieldRules(
		dst,
		fieldRule{priority: 30, reason: ReasonInvalidArgument, hasMaxLen: true, maxLenPriority: 20, maxLenReason: ReasonRange},
		"storage.v1.SetXattrRequest.name",
		"storage.v1.GetXattrRequest.name",
		"storage.v1.RemoveXattrRequest.name",
		"storage.v1.ListXattrResponse.names",
	)

	// 第 2/3 优先级：主目录项与磁盘名称超 255 为 ENAMETOOLONG，为空或含非法字符为 EINVAL
	registerFieldRules(
		dst,
		fieldRule{priority: 30, reason: ReasonInvalidArgument, hasMaxLen: true, maxLenPriority: 20, maxLenReason: ReasonNameTooLong},
		"storage.v1.LookupRequest.name",
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
		"storage.v1.ForkDiskRequest.name",
	)

	// 第 3 优先级：Diff / Merge 的路径过滤、分页令牌与合并路径策略统一映射为 EINVAL
	registerFieldRules(
		dst,
		fieldRule{priority: 30, reason: ReasonInvalidArgument},
		"storage.v1.DiffDiskRequest.path_prefix",
		"storage.v1.DiffDiskRequest.page_token",
		"storage.v1.MergeDiskRequest.path_strategies",
	)

	// 第 4/5 优先级：Rename 目标新名称超 255 为 ENAMETOOLONG，为空或含非法字符为 EINVAL
	registerFieldRules(
		dst,
		fieldRule{priority: 50, reason: ReasonInvalidArgument, hasMaxLen: true, maxLenPriority: 40, maxLenReason: ReasonNameTooLong},
		"storage.v1.RenameRequest.new_name",
	)

	// 第 4/5 优先级：符号链接目标为空或含 NUL 为 EINVAL，超 4095 为 ENAMETOOLONG
	registerFieldRules(
		dst,
		fieldRule{priority: 40, reason: ReasonInvalidArgument, hasMaxLen: true, maxLenPriority: 50, maxLenReason: ReasonNameTooLong},
		"storage.v1.SymlinkRequest.target",
		"storage.v1.ReadlinkResponse.target",
	)

	// 第 4 优先级：Xattr 属性值超 65536 映射为 ERANGE
	registerFieldRules(
		dst,
		fieldRule{priority: 40, reason: ReasonRange},
		"storage.v1.SetXattrRequest.value",
		"storage.v1.GetXattrResponse.value",
	)

	return dst
}

// ValidationError 包装 protovalidate.ValidationError，并将边界违规映射为携带 google.rpc.ErrorInfo 的 gRPC Status。
// 该类型同时实现 Unwrap 与 GRPCStatus 方法，供 errors.As、status.FromError 与下游状态转换提取底层违规详情与 POSIX errno reason。
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

// Reason 返回映射后的标准 POSIX errno 名称，例如 "EINVAL"、"ERANGE" 或 "ENAMETOOLONG"。
func (e *ValidationError) Reason() string {
	return e.reason
}

// Domain 返回错误所属的服务领域标识符，默认为 "bowl.storage"。
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
// 通过 protovalidate.WithMessages 选项在启动阶段编译 CEL 约束规则，避免首次运行时产生编译延迟，
// 并将字段边界校验违规关联至对应的 POSIX errno reason，包括 ERANGE、ENAMETOOLONG 与 EINVAL。
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
// 若 err 为 protovalidate 校验违规，包括原始 *protovalidate.ValidationError，
// 按字段与规则优先级映射为 codes.InvalidArgument 及对应的 ERANGE、ENAMETOOLONG 或 EINVAL。
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

// translateValidationReason 按照统一字段与规则求值优先级，
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

func violationFieldFullName(v *protovalidate.Violation) string {
	if v == nil {
		return ""
	}
	if v.FieldDescriptor != nil {
		fullName := string(v.FieldDescriptor.FullName())
		if strings.HasPrefix(fullName, "storage.v1.MergeDiskRequest.PathStrategiesEntry.") {
			return "storage.v1.MergeDiskRequest.path_strategies"
		}
		return fullName
	}
	if v.Proto == nil {
		return ""
	}
	for _, elem := range v.Proto.GetField().GetElements() {
		switch elem.GetFieldName() {
		case "names":
			return "storage.v1.ListXattrResponse.names"
		case "path_strategies":
			return "storage.v1.MergeDiskRequest.path_strategies"
		}
	}
	return ""
}

func classifyViolation(v *protovalidate.Violation) (priority int, reason, domain string) {
	priority, reason = classifyViolationInternal(v)
	fullName := violationFieldFullName(v)

	domain = ""
	if strings.HasPrefix(fullName, "storage.v1.") {
		domain = ErrorDomain
	}
	// 消息级约束无 field descriptor，返回空 domain，保留调用方的 defaultDomain。
	return priority, reason, domain
}

func classifyViolationInternal(v *protovalidate.Violation) (priority int, reason string) {
	if v == nil || v.Proto == nil {
		return 60, ReasonInvalidArgument
	}

	ruleID := v.Proto.GetRuleId()
	if rule, ok := messageRulePriorities[ruleID]; ok {
		return rule.priority, rule.reason
	}

	isMaxLen := ruleID == "bytes.max_len" || strings.HasSuffix(ruleID, ".bytes.max_len") ||
		ruleID == "string.max_bytes" || strings.HasSuffix(ruleID, ".string.max_bytes")

	fullName := violationFieldFullName(v)
	if rule, ok := fieldClassifications[fullName]; ok {
		if rule.hasMaxLen && isMaxLen {
			return rule.maxLenPriority, rule.maxLenReason
		}
		return rule.priority, rule.reason
	}

	return 60, ReasonInvalidArgument
}
