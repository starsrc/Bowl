package server

import (
	"bytes"
	"context"
	"math"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

const fopenKeepCache uint32 = 1 << 1

func (s *Server) Create(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	msg := req.Msg
	if err := validateWriteEpoch(msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if msg.Uid > math.MaxInt32 || msg.Gid > math.MaxInt32 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	attr, err := actor.Create(ctx, store.CreateReq{
		ParentIno: msg.ParentIno,
		Name:      msg.Name,
		Mode:      msg.Mode,
		UID:       msg.Uid,
		GID:       msg.Gid,
		Flags:     msg.Flags,
		Epoch:     msg.Epoch,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	var openFlags uint32
	if s.control.OpenKeepCache() {
		openFlags = fopenKeepCache
	}

	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               toProtoInode(&attr),
		Fh:                  1,
		OpenFlags:           openFlags,
		EntryTimeoutSeconds: s.control.EntryTimeoutForMode(attr.Mode),
		AttrTimeoutSeconds:  s.control.AttrTimeoutSeconds(),
	}), nil
}

func (s *Server) CreateTmpfile(ctx context.Context, req *connect.Request[storagev1.CreateTmpfileRequest]) (*connect.Response[storagev1.CreateTmpfileResponse], error) {
	msg := req.Msg
	if err := validateWriteEpoch(msg.GetEpoch()); err != nil {
		return nil, toConnectError(err)
	}
	if msg.GetUid() > math.MaxInt32 || msg.GetGid() > math.MaxInt32 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	attr, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: msg.GetParentIno(),
		Mode:      msg.GetMode(),
		UID:       msg.GetUid(),
		GID:       msg.GetGid(),
		Exclusive: msg.GetExclusive(),
		Epoch:     msg.GetEpoch(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.CreateTmpfileResponse{
		Inode:               toProtoInode(&attr),
		EntryTimeoutSeconds: s.control.EntryTimeoutForMode(attr.Mode),
		AttrTimeoutSeconds:  s.control.AttrTimeoutSeconds(),
	}), nil
}

func (s *Server) Fsync(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	msg := req.Msg
	if err := validateWriteEpoch(msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if msg.Fh > math.MaxInt64 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if err := actor.Fsync(ctx, msg.Ino, msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (s *Server) FsyncDir(ctx context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	msg := req.Msg
	if err := validateWriteEpoch(msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if msg.Fh > math.MaxInt64 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if err := actor.FsyncDir(ctx, msg.Ino, msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

func (s *Server) ReadDir(ctx context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
	msg := req.Msg
	if msg.Fh > math.MaxInt64 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	res, err := actor.ReadDir(ctx, msg.Ino, msg.Offset, msg.Limit)
	if err != nil {
		return nil, toConnectError(err)
	}

	entries := make([]*storagev1.DirEntry, len(res))
	for i, e := range res {
		entries[i] = &storagev1.DirEntry{
			Ino:        e.Ino,
			Name:       e.Name,
			Mode:       e.Mode,
			NextOffset: e.NextOffset,
		}
	}

	return connect.NewResponse(&storagev1.ReadDirResponse{
		Entries: entries,
	}), nil
}

func (s *Server) ReadDirPlus(ctx context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	msg := req.Msg
	if msg.Fh > math.MaxInt64 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	res, err := actor.ReadDirPlus(ctx, msg.Ino, msg.Offset, msg.Limit)
	if err != nil {
		return nil, toConnectError(err)
	}

	attrTimeout := s.control.AttrTimeoutSeconds()
	entries := make([]*storagev1.DirEntryPlus, len(res))
	for i := range res {
		e := &res[i]
		entryTimeout := s.control.EntryTimeoutForMode(e.Attr.Mode)
		if bytes.Equal(e.Name, []byte(".")) || bytes.Equal(e.Name, []byte("..")) {
			entryTimeout = 0
		}
		entries[i] = &storagev1.DirEntryPlus{
			Ino:                 e.Ino,
			Name:                e.Name,
			NextOffset:          e.NextOffset,
			Inode:               toProtoInode(&e.Attr),
			EntryTimeoutSeconds: entryTimeout,
			AttrTimeoutSeconds:  attrTimeout,
		}
	}

	return connect.NewResponse(&storagev1.ReadDirPlusResponse{
		Entries: entries,
	}), nil
}

func (s *Server) Fallocate(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	msg := req.Msg
	if err := validateWriteEpoch(msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if msg.Fh > math.MaxInt64 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if err := actor.Fallocate(ctx, msg.Ino, msg.Offset, msg.Length, msg.Mode, msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}
