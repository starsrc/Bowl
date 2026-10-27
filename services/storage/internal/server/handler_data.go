package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Read 读取指定文件的数据块内容（对接 Linux FUSE FUSE_READ 协议）。
// 当前阶段（STARSRC-486）作为桩方法返回 Unimplemented，后续工单（STARSRC-493）接入 meta.Manager 读实现。
func (s *Server) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("storage.v1.StorageService.Read is not implemented"))
}

// Write 向指定文件写入数据块内容（对接 Linux FUSE FUSE_WRITE 协议）。
// 当前阶段（STARSRC-486）作为桩方法返回 Unimplemented，后续工单（STARSRC-490）接入 meta.Manager 写实现。
func (s *Server) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("storage.v1.StorageService.Write is not implemented"))
}
