package server

import (
	"context"
	"math"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func validateWriteEpoch(epoch uint64) error {
	if epoch == 0 || epoch > math.MaxInt64 {
		return store.ErrInvalid
	}
	return nil
}

func toProtoAttribute(attr *store.Attribute) *storagev1.InodeAttribute {
	return &storagev1.InodeAttribute{
		Mode:    attr.Mode,
		Size:    attr.Size,
		Nlink:   attr.Nlink,
		Uid:     attr.UID,
		Gid:     attr.GID,
		Rdev:    attr.Rdev,
		Blksize: attr.Blksize,
		Blocks:  attr.Blocks,
		Atime:   timestamppb.New(time.Unix(0, attr.Atime).UTC()),
		Mtime:   timestamppb.New(time.Unix(0, attr.Mtime).UTC()),
		Ctime:   timestamppb.New(time.Unix(0, attr.Ctime).UTC()),
		Flags:   attr.Flags,
	}
}

func toProtoInode(attr *store.Attribute) *storagev1.Inode {
	if attr == nil {
		return nil
	}
	return &storagev1.Inode{
		Ino:        attr.Ino,
		Generation: attr.Generation,
		Attr:       toProtoAttribute(attr),
	}
}

func toProtoDisk(d *control.Disk) *storagev1.Disk {
	if d == nil {
		return nil
	}
	pb := &storagev1.Disk{
		DiskId:     d.DiskID,
		Name:       d.Name,
		RootIno:    d.RootIno,
		MaxBytes:   d.MaxBytes,
		UsedBytes:  d.UsedBytes,
		UsedInodes: d.UsedInodes,
		CreatedAt:  timestamppb.New(d.CreatedAt),
		UpdatedAt:  timestamppb.New(d.UpdatedAt),
	}
	if d.ParentDiskID != "" {
		pb.ParentDiskId = &d.ParentDiskID
	}
	return pb
}

func (s *Server) CreateDisk(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	disk, err := s.control.CreateDisk(ctx, control.CreateDiskParams{
		Name:     req.Msg.Name,
		MaxBytes: req.Msg.MaxBytes,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.CreateDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

func (s *Server) ForkDisk(ctx context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
	disk, err := s.control.ForkDisk(ctx, control.ForkDiskParams{
		SourceDiskID: req.Msg.SourceDiskId,
		Name:         req.Msg.Name,
		MaxBytes:     req.Msg.MaxBytes,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ForkDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

func (s *Server) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	disk, err := s.control.GetDisk(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.GetDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

func (s *Server) ListDisks(ctx context.Context, req *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
	res, err := s.control.ListDisks(ctx, control.ListDisksParams{
		PageSize:  req.Msg.PageSize,
		PageToken: req.Msg.PageToken,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	protoDisks := make([]*storagev1.Disk, 0, len(res.Disks))
	for _, d := range res.Disks {
		protoDisks = append(protoDisks, toProtoDisk(d))
	}
	return connect.NewResponse(&storagev1.ListDisksResponse{
		Disks:         protoDisks,
		NextPageToken: res.NextPageToken,
	}), nil
}

func (s *Server) UpdateDisk(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
	disk, err := s.control.UpdateDisk(ctx, control.UpdateDiskParams{
		DiskID:   req.Msg.DiskId,
		Name:     req.Msg.Name,
		MaxBytes: req.Msg.MaxBytes,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.UpdateDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

func (s *Server) DeleteDisk(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	if err := s.control.DeleteDisk(ctx, req.Msg.DiskId); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
}

func (s *Server) MountDisk(ctx context.Context, req *connect.Request[storagev1.MountDiskRequest]) (*connect.Response[storagev1.MountDiskResponse], error) {
	res, err := s.control.MountDisk(ctx, control.MountDiskParams{
		DiskID:   req.Msg.DiskId,
		ReadOnly: req.Msg.ReadOnly,
		Force:    req.Msg.Force,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.MountDiskResponse{
		Epoch:               res.Epoch,
		RootInode:           toProtoInode(&res.RootAttr),
		EntryTimeoutSeconds: res.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  res.AttrTimeoutSeconds,
		MountId:             res.MountID,
		LeaseTtlSeconds:     res.LeaseTTLSeconds,
	}), nil
}

func (s *Server) RenewMount(ctx context.Context, req *connect.Request[storagev1.RenewMountRequest]) (*connect.Response[storagev1.RenewMountResponse], error) {
	res, err := s.control.RenewMount(ctx, control.RenewMountParams{
		DiskID:   req.Msg.DiskId,
		MountID:  req.Msg.MountId,
		ReadOnly: req.Msg.ReadOnly,
		Epoch:    req.Msg.Epoch,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.RenewMountResponse{
		LeaseTtlSeconds: res.LeaseTTLSeconds,
	}), nil
}

func (s *Server) UnmountDisk(ctx context.Context, req *connect.Request[storagev1.UnmountDiskRequest]) (*connect.Response[storagev1.UnmountDiskResponse], error) {
	if err := s.control.UnmountDisk(ctx, control.UnmountDiskParams{
		DiskID:  req.Msg.DiskId,
		MountID: req.Msg.MountId,
	}); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.UnmountDiskResponse{}), nil
}

func (s *Server) StatFS(ctx context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if _, err = actor.GetAttr(ctx, req.Msg.Ino); err != nil {
		return nil, toConnectError(err)
	}
	res, err := actor.StatFS(ctx)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.StatFSResponse{
		TotalBytes: res.TotalBytes,
		UsedBytes:  res.UsedBytes,
		AvailBytes: res.AvailBytes,
		Bsize:      res.Bsize,
		Frsize:     res.Frsize,
		Blocks:     res.Blocks,
		Bfree:      res.Bfree,
		Bavail:     res.Bavail,
		Files:      res.Files,
		Ffree:      res.Ffree,
		Namelen:    res.Namelen,
	}), nil
}

func (s *Server) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	_, attr, err := actor.Lookup(ctx, req.Msg.ParentIno, req.Msg.Name)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode:               toProtoInode(&attr),
		EntryTimeoutSeconds: s.control.EntryTimeoutForMode(attr.Mode),
		AttrTimeoutSeconds:  s.control.AttrTimeoutSeconds(),
	}), nil
}

func (s *Server) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	attr, err := actor.GetAttr(ctx, req.Msg.Ino)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              toProtoInode(&attr),
		AttrTimeoutSeconds: s.control.AttrTimeoutSeconds(),
	}), nil
}

func (s *Server) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	if err := validateWriteEpoch(req.Msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if req.Msg.Fh != nil && (*req.Msg.Fh == 0 || *req.Msg.Fh > math.MaxInt64) {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	input := store.SetAttrReq{
		Mode:     req.Msg.Mode,
		UID:      req.Msg.Uid,
		GID:      req.Msg.Gid,
		Size:     req.Msg.Size,
		AtimeNow: req.Msg.AtimeNow,
		MtimeNow: req.Msg.MtimeNow,
		CtimeNow: req.Msg.CtimeNow,
		Flags:    req.Msg.Flags,
		Epoch:    req.Msg.Epoch,
	}
	if req.Msg.Atime != nil {
		atime := req.Msg.Atime.AsTime()
		input.Atime = &atime
	}
	if req.Msg.Mtime != nil {
		mtime := req.Msg.Mtime.AsTime()
		input.Mtime = &mtime
	}

	attr, err := actor.SetAttr(ctx, req.Msg.Ino, input, req.Msg.Epoch)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              toProtoInode(&attr),
		AttrTimeoutSeconds: s.control.AttrTimeoutSeconds(),
	}), nil
}

func (s *Server) Mknod(ctx context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	if err := validateWriteEpoch(req.Msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if req.Msg.Uid > math.MaxInt32 || req.Msg.Gid > math.MaxInt32 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	attr, err := actor.Mknod(ctx, store.MknodReq{
		ParentIno: req.Msg.ParentIno,
		Name:      req.Msg.Name,
		Mode:      req.Msg.Mode,
		UID:       req.Msg.Uid,
		GID:       req.Msg.Gid,
		Rdev:      req.Msg.Rdev,
		Epoch:     req.Msg.Epoch,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.MknodResponse{
		Inode:               toProtoInode(&attr),
		EntryTimeoutSeconds: s.control.EntryTimeoutForMode(attr.Mode),
		AttrTimeoutSeconds:  s.control.AttrTimeoutSeconds(),
	}), nil
}

func (s *Server) Mkdir(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	if err := validateWriteEpoch(req.Msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if req.Msg.Uid > math.MaxInt32 || req.Msg.Gid > math.MaxInt32 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	attr, err := actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: req.Msg.ParentIno,
		Name:      req.Msg.Name,
		Mode:      req.Msg.Mode,
		UID:       req.Msg.Uid,
		GID:       req.Msg.Gid,
		Epoch:     req.Msg.Epoch,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode:               toProtoInode(&attr),
		EntryTimeoutSeconds: s.control.EntryTimeoutForMode(attr.Mode),
		AttrTimeoutSeconds:  s.control.AttrTimeoutSeconds(),
	}), nil
}

func (s *Server) Symlink(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
	if err := validateWriteEpoch(req.Msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if req.Msg.Uid > math.MaxInt32 || req.Msg.Gid > math.MaxInt32 {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	attr, err := actor.Symlink(ctx, store.SymlinkReq{
		ParentIno: req.Msg.ParentIno,
		Name:      req.Msg.Name,
		Target:    req.Msg.Target,
		UID:       req.Msg.Uid,
		GID:       req.Msg.Gid,
		Epoch:     req.Msg.Epoch,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.SymlinkResponse{
		Inode:               toProtoInode(&attr),
		EntryTimeoutSeconds: s.control.EntryTimeoutForMode(attr.Mode),
		AttrTimeoutSeconds:  s.control.AttrTimeoutSeconds(),
	}), nil
}

func (s *Server) Readlink(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	actor, err := s.control.Actor(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	target, err := actor.Readlink(ctx, req.Msg.Ino)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ReadlinkResponse{
		Target: target,
	}), nil
}
