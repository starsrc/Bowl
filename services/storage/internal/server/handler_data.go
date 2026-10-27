package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// Read 读取指定文件的数据块内容（对接 Linux FUSE FUSE_READ 协议）。
func (s *Server) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.Read(ctx, &meta.ReadRequest{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Offset:    msg.Offset,
		Size:      msg.Size,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.ReadResponse{
		Data: res.Data,
	}), nil
}

// Write 向指定文件写入数据块内容（对接 Linux FUSE FUSE_WRITE 协议）。
func (s *Server) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.Write(ctx, &meta.WriteRequest{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Offset:    msg.Offset,
		Data:      msg.Data,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  res.Size,
		Inode: toProtoInode(res.Inode),
	}), nil
}
