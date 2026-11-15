// Package rpcerr 定义跨服务端与客户端共享的 google.rpc.ErrorInfo 领域标识、原因码契约与提取方法。
package rpcerr

import "github.com/starsrc/pkg/serverx"

// Domain 为 StorageService 返回的 google.rpc.ErrorInfo 统一错误域标识。
const Domain = "bowl.storage"

// 以下常量定义服务端通过 ErrorInfo.Reason 传递的 19 类契约原因码，包括 18 类 POSIX errno 与合并冲突标识。
const (
	ReasonInvalidArgument  = "EINVAL"
	ReasonNameTooLong      = "ENAMETOOLONG"
	ReasonRange            = "ERANGE"
	ReasonNotFound         = "ENOENT"
	ReasonNoData           = "ENODATA"
	ReasonAlreadyExists    = "EEXIST"
	ReasonPermissionDenied = "EPERM"
	ReasonReadOnly         = "EROFS"
	ReasonNotDir           = "ENOTDIR"
	ReasonIsDir            = "EISDIR"
	ReasonNotEmpty         = "ENOTEMPTY"
	ReasonNoSpace          = "ENOSPC"
	ReasonNotSupported     = "EOPNOTSUPP"
	ReasonTooManyLinks     = "EMLINK"
	ReasonFileTooLarge     = "EFBIG"
	ReasonCrossDevice      = "EXDEV"
	ReasonIO               = "EIO"
	ReasonBusy             = "EBUSY"
	ReasonConflict         = "ECONFLICT"
)

// ExtractReason 从 err 中提取 google.rpc.ErrorInfo 并校验其 Domain 与 Reason。
// 仅当 ErrorInfo 非空、Domain 等于 Domain 且 Reason 非空时返回对应的 Reason 与 true，否则返回空字符串与 false。
func ExtractReason(err error) (string, bool) {
	info := serverx.ExtractErrorInfo(err)
	if info == nil || info.GetDomain() != Domain || info.GetReason() == "" {
		return "", false
	}
	return info.GetReason(), true
}
