package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// toDomainFileLock 将 Protobuf FileLock 转换为领域层 meta.FileLock。
func toDomainFileLock(lk *storagev1.FileLock) meta.FileLock {
	if lk == nil {
		return meta.FileLock{}
	}
	return meta.FileLock{
		Start: lk.GetStart(),
		End:   lk.GetEnd(),
		Type:  lk.GetType(),
		PID:   lk.GetPid(),
	}
}

// toProtoFileLock 将领域层 meta.FileLock 转换为 Protobuf FileLock。
func toProtoFileLock(lk *meta.FileLock) *storagev1.FileLock {
	if lk == nil {
		return nil
	}
	return &storagev1.FileLock{
		Start: lk.Start,
		End:   lk.End,
		Type:  lk.Type,
		Pid:   lk.PID,
	}
}

// GetLk 探测目标 Inode 在指定锁命名空间（POSIX 记录锁或 BSD flock）下是否存在冲突锁（对接 Linux FUSE FUSE_GETLK = 31 协议）。
func (s *Server) GetLk(ctx context.Context, req *connect.Request[storagev1.GetLkRequest]) (*connect.Response[storagev1.GetLkResponse], error) {
	msg := req.Msg
	conflict, err := s.metaManager.GetLk(ctx, meta.GetLkParams{
		Ino:       msg.GetIno(),
		Fh:        msg.GetFh(),
		LockOwner: msg.GetLockOwner(),
		Lk:        toDomainFileLock(msg.GetLk()),
		LkFlags:   msg.GetLkFlags(),
		SessionID: msg.GetSessionId(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.GetLkResponse{
		Lk: toProtoFileLock(conflict),
	}), nil
}

// SetLk 非阻塞地申请、转换、裂解合并或释放文件锁（对接 Linux FUSE FUSE_SETLK = 32 协议）。
func (s *Server) SetLk(ctx context.Context, req *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error) {
	msg := req.Msg
	err := s.metaManager.SetLk(ctx, meta.SetLkParams{
		Ino:       msg.GetIno(),
		Fh:        msg.GetFh(),
		LockOwner: msg.GetLockOwner(),
		Lk:        toDomainFileLock(msg.GetLk()),
		LkFlags:   msg.GetLkFlags(),
		SessionID: msg.GetSessionId(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.SetLkResponse{}), nil
}

// SetLkw 阻塞式申请、转换、裂解合并或释放文件锁（对接 Linux FUSE FUSE_SETLKW = 33 协议）。
func (s *Server) SetLkw(ctx context.Context, req *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error) {
	msg := req.Msg
	err := s.metaManager.SetLkw(ctx, meta.SetLkwParams{
		Ino:       msg.GetIno(),
		Fh:        msg.GetFh(),
		LockOwner: msg.GetLockOwner(),
		Lk:        toDomainFileLock(msg.GetLk()),
		LkFlags:   msg.GetLkFlags(),
		SessionID: msg.GetSessionId(),
		RequestID: msg.GetRequestId(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.SetLkwResponse{}), nil
}

// RenewSession 显式延长客户端会话租约有效期并顺带回收孤儿节点。
func (s *Server) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	var (
		res meta.RenewSessionResult
		err error
	)
	if req.Msg.DiskId == nil {
		res, err = s.metaManager.RenewSessionWithStatus(ctx, req.Msg.GetSessionId())
	} else {
		res, err = s.metaManager.RenewSessionWithDisk(ctx, req.Msg.GetSessionId(), req.Msg.DiskId)
	}
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt:        timestamppb.New(res.ExpiresAt),
		SessionRecreated: res.SessionRecreated,
	}), nil
}

// CloseSession 在客户端优雅卸载时即时注销指定会话租约。
func (s *Server) CloseSession(ctx context.Context, req *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error) {
	if err := s.metaManager.CloseSession(ctx, req.Msg.GetSessionId()); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.CloseSessionResponse{}), nil
}
