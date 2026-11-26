package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

const (
	fopenKeepCache uint32 = 1 << 1
	oTmpfileFlag   uint32 = 0o20000000
)

func (s *Server) Create(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	msg := req.Msg
	if msg.Flags&oTmpfileFlag != 0 {
		return nil, toConnectError(engine.ErrInvalid)
	}
	diskID, err := parseRequiredUUID(msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, msg.Epoch)
	var inode engine.Inode
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var cErr error
		inode, cErr = ds.Create(epochCtx, msg.Branch, msg.ParentIno, string(msg.Name), msg.Mode, msg.Uid, msg.Gid)
		return cErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	var openFlags uint32
	if s.metaCfg.OpenKeepCache() {
		openFlags = fopenKeepCache
	}

	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               toProtoInode(inode),
		Fh:                  1,
		OpenFlags:           openFlags,
		EntryTimeoutSeconds: s.entryTimeoutForMode(inode.Mode),
		AttrTimeoutSeconds:  s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) CreateTmpfile(ctx context.Context, req *connect.Request[storagev1.CreateTmpfileRequest]) (*connect.Response[storagev1.CreateTmpfileResponse], error) {
	msg := req.Msg
	diskID, err := parseRequiredUUID(msg.GetDiskId())
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, msg.GetEpoch())
	var inode engine.Inode
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var cErr error
		inode, cErr = ds.CreateTmpfile(epochCtx, msg.GetBranch(), msg.GetParentIno(), msg.GetMode(), msg.GetUid(), msg.GetGid(), msg.GetExclusive())
		return cErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.CreateTmpfileResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: s.entryTimeoutForMode(inode.Mode),
		AttrTimeoutSeconds:  s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) Fsync(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	msg := req.Msg
	diskID, err := parseRequiredUUID(msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, msg.Epoch)
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.Fsync(epochCtx, msg.Branch, msg.Ino)
	}); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (s *Server) FsyncDir(ctx context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	msg := req.Msg
	diskID, err := parseRequiredUUID(msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, msg.Epoch)
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.FsyncDir(epochCtx, msg.Branch, msg.Ino)
	}); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

func (s *Server) ReadDir(ctx context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
	msg := req.Msg
	diskID, err := parseRequiredUUID(msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var res []engine.DirEntry
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var rErr error
		res, rErr = ds.ReadDirPage(ctx, msg.Branch, msg.Ino, msg.Offset, msg.Limit)
		return rErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	entries := make([]*storagev1.DirEntry, len(res))
	for i, e := range res {
		entries[i] = &storagev1.DirEntry{
			Ino:        e.Ino,
			Name:       []byte(e.Name),
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
	diskID, err := parseRequiredUUID(msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var res []engine.DirEntryPlus
	err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var rErr error
		res, rErr = ds.ReadDirPlus(ctx, msg.Branch, msg.Ino, msg.Offset, msg.Limit)
		return rErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	attrTimeout := s.attrTimeoutSeconds()
	entries := make([]*storagev1.DirEntryPlus, len(res))
	for i := range res {
		e := &res[i]
		entryTimeout := s.entryTimeoutForMode(e.Inode.Mode)
		if e.Name == "." || e.Name == ".." {
			entryTimeout = 0
		}
		entries[i] = &storagev1.DirEntryPlus{
			Ino:                 e.Ino,
			Name:                []byte(e.Name),
			NextOffset:          e.NextOffset,
			Inode:               toProtoInode(e.Inode),
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
	diskID, err := parseRequiredUUID(msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, msg.Epoch)
	if err = s.manager.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		return ds.Fallocate(epochCtx, msg.Branch, msg.Ino, msg.Offset, msg.Length, msg.Mode)
	}); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}
