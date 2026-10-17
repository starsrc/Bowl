package validator

import (
	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
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

	// 嵌套与共享消息
	&storagev1.DirEntry{},
	&storagev1.DirEntryPlus{},
	&storagev1.Inode{},
	&storagev1.InodeAttribute{},
}

// NewStorageValidator 创建并初始化一个预编译好 StorageService 全部请求与响应消息的 protovalidate.Validator。
// 通过 protovalidate.WithMessages 选项在启动阶段编译 CEL 约束规则，避免在首次运行时产生编译延迟。
func NewStorageValidator() (protovalidate.Validator, error) {
	return protovalidate.New(protovalidate.WithMessages(storageMessages...))
}
