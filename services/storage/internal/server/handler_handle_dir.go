package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// Create 在指定父目录下原子创建常规文件并分配初始文件句柄 (FileHandle)。
func (s *Server) Create(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.Create(ctx, meta.CreateParams{
		ParentIno: msg.ParentIno,
		Name:      msg.Name,
		Mode:      msg.Mode,
		UID:       msg.Uid,
		GID:       msg.Gid,
		Flags:     msg.Flags,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               inodeToProtoForHandle(res.Inode),
		Fh:                  res.Fh,
		OpenFlags:           res.OpenFlags,
		EntryTimeoutSeconds: res.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  res.AttrTimeoutSeconds,
	}), nil
}

// TmpFile 在指定父目录下原子创建未绑定 Entry (nlink = 0) 的匿名临时常规文件并分配初始文件句柄 (FileHandle)。
func (s *Server) TmpFile(ctx context.Context, req *connect.Request[storagev1.TmpFileRequest]) (*connect.Response[storagev1.TmpFileResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.TmpFile(ctx, meta.TmpFileParams{
		ParentIno: msg.ParentIno,
		Mode:      msg.Mode,
		UID:       msg.Uid,
		GID:       msg.Gid,
		Flags:     msg.Flags,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.TmpFileResponse{
		Inode:               inodeToProtoForHandle(res.Inode),
		Fh:                  res.Fh,
		OpenFlags:           res.OpenFlags,
		EntryTimeoutSeconds: res.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  res.AttrTimeoutSeconds,
	}), nil
}

// Open 打开指定 Inode 节点并分配文件句柄 (FileHandle)。
func (s *Server) Open(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.Open(ctx, meta.OpenParams{
		Ino:       msg.Ino,
		Flags:     msg.Flags,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.OpenResponse{
		Fh:        res.Fh,
		OpenFlags: res.OpenFlags,
	}), nil
}

// Flush 在单次文件关闭时触发，冲刷并校验文件句柄的合法性。
func (s *Server) Flush(ctx context.Context, req *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error) {
	msg := req.Msg
	err := s.metaManager.Flush(ctx, meta.FlushParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		LockOwner: msg.LockOwner,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FlushResponse{}), nil
}

// Fsync 同步并持久化文件节点与句柄关联的状态。
func (s *Server) Fsync(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	msg := req.Msg
	err := s.metaManager.Fsync(ctx, meta.FsyncParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Datasync:  msg.Datasync,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

// Release 关闭并销毁文件句柄 (FileHandle)。
func (s *Server) Release(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
	msg := req.Msg
	err := s.metaManager.Release(ctx, meta.ReleaseParams{
		Ino:          msg.Ino,
		Fh:           msg.Fh,
		LockOwner:    msg.LockOwner,
		ReleaseFlags: msg.ReleaseFlags,
		SessionID:    msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
}

// OpenDir 打开指定目录 Inode 节点并分配目录 FileHandle。
func (s *Server) OpenDir(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.OpenDir(ctx, meta.OpenDirParams{
		Ino:       msg.Ino,
		Flags:     msg.Flags,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.OpenDirResponse{
		Fh:        res.Fh,
		OpenFlags: res.OpenFlags,
	}), nil
}

// FsyncDir 同步并持久化目录节点与目录句柄关联的状态。
func (s *Server) FsyncDir(ctx context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	msg := req.Msg
	err := s.metaManager.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Datasync:  msg.Datasync,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

// ReleaseDir 关闭并销毁目录 FileHandle。
func (s *Server) ReleaseDir(ctx context.Context, req *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error) {
	msg := req.Msg
	err := s.metaManager.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.ReleaseDirResponse{}), nil
}

// ReadDir 读取指定目录的子项流（基本条目）。
func (s *Server) ReadDir(ctx context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.ReadDir(ctx, meta.ReadDirParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Offset:    msg.Offset,
		Limit:     msg.Limit,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	entries := make([]*storagev1.DirEntry, len(res))
	for i, e := range res {
		entries[i] = toProtoDirEntry(e)
	}

	return connect.NewResponse(&storagev1.ReadDirResponse{
		Entries: entries,
	}), nil
}

// ReadDirPlus 读取指定目录的子项流并附带完整 Inode 属性与缓存时效（扩展条目）。
func (s *Server) ReadDirPlus(ctx context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.ReadDirPlus(ctx, meta.ReadDirPlusParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Offset:    msg.Offset,
		Limit:     msg.Limit,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	entries := make([]*storagev1.DirEntryPlus, len(res))
	for i, e := range res {
		entries[i] = toProtoDirEntryPlus(e)
	}

	return connect.NewResponse(&storagev1.ReadDirPlusResponse{
		Entries: entries,
	}), nil
}

// Fallocate 为常规文件预分配物理空间或打洞去分配空间。
func (s *Server) Fallocate(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	msg := req.Msg
	err := s.metaManager.Fallocate(ctx, meta.FallocateParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Offset:    msg.Offset,
		Length:    msg.Length,
		Mode:      msg.Mode,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

// inodeToProtoForHandle 将 meta.Inode 转换为 proto storagev1.Inode。
// 命名隔离：确保不与并行工单定义的 toProtoInode 冲突。
func inodeToProtoForHandle(in *meta.Inode) *storagev1.Inode {
	if in == nil {
		return nil
	}

	return &storagev1.Inode{
		Ino:        in.Ino,
		Generation: in.Generation,
		Attr: &storagev1.InodeAttribute{
			Mode:    in.Attr.Mode,
			Size:    in.Attr.Size,
			Nlink:   in.Attr.Nlink,
			Uid:     in.Attr.UID,
			Gid:     in.Attr.GID,
			Rdev:    in.Attr.Rdev,
			Blksize: in.Attr.Blksize,
			Blocks:  in.Attr.Blocks,
			Atime:   timestamppb.New(in.Attr.Atime),
			Mtime:   timestamppb.New(in.Attr.Mtime),
			Ctime:   timestamppb.New(in.Attr.Ctime),
			Flags:   in.Attr.Flags,
		},
	}
}

// toProtoDirEntry 将 meta.DirEntry 转换为 proto storagev1.DirEntry。
func toProtoDirEntry(e meta.DirEntry) *storagev1.DirEntry {
	return &storagev1.DirEntry{
		Ino:        e.Ino,
		Name:       e.Name,
		Mode:       e.Mode,
		NextOffset: e.NextOffset,
	}
}

// toProtoDirEntryPlus 将 meta.DirEntryPlus 转换为 proto storagev1.DirEntryPlus。
func toProtoDirEntryPlus(e meta.DirEntryPlus) *storagev1.DirEntryPlus {
	return &storagev1.DirEntryPlus{
		Ino:                 e.Ino,
		Name:                e.Name,
		NextOffset:          e.NextOffset,
		Inode:               inodeToProtoForHandle(e.Inode),
		EntryTimeoutSeconds: e.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  e.AttrTimeoutSeconds,
	}
}
