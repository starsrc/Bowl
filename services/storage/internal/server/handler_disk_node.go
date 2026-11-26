package server

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func parseRequiredUUID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, engine.ErrInvalid
	}
	return id, nil
}

func parseOptionalUUID(raw *string) (uuid.UUID, error) {
	if raw == nil || *raw == "" {
		return uuid.Nil, nil
	}
	return parseRequiredUUID(*raw)
}

func toProtoTimeFromNano(ns int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, ns).UTC().Truncate(time.Microsecond))
}

func toProtoAttribute(inode engine.Inode) *storagev1.InodeAttribute {
	size := inode.Size
	fileType := inode.Mode & engine.ModeTypeMask
	if fileType == engine.ModeDir && size == 0 {
		size = engine.DefaultDirSize
	}
	var blocks uint64
	switch fileType {
	case engine.ModeDir, engine.ModeRegular, engine.ModeSymlink:
		blocks = (size + 511) / 512
	}
	var rdev uint32
	if fileType == engine.ModeChar || fileType == engine.ModeBlock {
		rdev = inode.Rdev
	}
	return &storagev1.InodeAttribute{
		Mode:    inode.Mode,
		Size:    size,
		Nlink:   inode.Nlink,
		Uid:     inode.UID,
		Gid:     inode.GID,
		Rdev:    rdev,
		Blksize: engine.DefaultBlockSize,
		Blocks:  blocks,
		Atime:   timestamppb.New(time.Unix(0, inode.Atime).UTC()),
		Mtime:   timestamppb.New(time.Unix(0, inode.Mtime).UTC()),
		Ctime:   timestamppb.New(time.Unix(0, inode.Ctime).UTC()),
		Flags:   inode.Flags &^ engine.InodeFlagEverLinked,
	}
}

func toProtoInode(inode engine.Inode) *storagev1.Inode {
	return &storagev1.Inode{
		Ino:        inode.Ino,
		Generation: inode.Generation,
		Attr:       toProtoAttribute(inode),
	}
}

func toProtoDisk(d engine.DiskInfo) *storagev1.Disk {
	return &storagev1.Disk{
		DiskId:        d.DiskID,
		Name:          d.Name,
		DefaultBranch: d.DefaultBranch,
		MaxBytes:      d.MaxBytes,
		UsedBytes:     d.UsedBytes,
		CreatedAt:     toProtoTimeFromNano(d.CreatedAt),
		UpdatedAt:     toProtoTimeFromNano(d.UpdatedAt),
	}
}

func toProtoBranch(diskID uuid.UUID, b engine.BranchMeta) *storagev1.Branch {
	return &storagev1.Branch{
		DiskId:       diskID.String(),
		Name:         b.Name,
		HeadCommitId: b.HeadCommitID,
		WriterEpoch:  b.WriterEpoch,
		CreatedAt:    toProtoTimeFromNano(b.CreatedAt),
		UpdatedAt:    toProtoTimeFromNano(b.UpdatedAt),
	}
}

func toProtoCommitKind(kind engine.CommitKind) storagev1.CommitKind {
	switch kind {
	case engine.CommitKindInit:
		return storagev1.CommitKind_COMMIT_KIND_INIT
	case engine.CommitKindManual:
		return storagev1.CommitKind_COMMIT_KIND_MANUAL
	case engine.CommitKindFork:
		return storagev1.CommitKind_COMMIT_KIND_FORK
	case engine.CommitKindMerge:
		return storagev1.CommitKind_COMMIT_KIND_MERGE
	default:
		return storagev1.CommitKind_COMMIT_KIND_UNSPECIFIED
	}
}

func toEngineCommitKind(kind storagev1.CommitKind) (engine.CommitKind, error) {
	switch kind {
	case storagev1.CommitKind_COMMIT_KIND_UNSPECIFIED:
		return engine.CommitKindUnspecified, nil
	case storagev1.CommitKind_COMMIT_KIND_INIT:
		return engine.CommitKindInit, nil
	case storagev1.CommitKind_COMMIT_KIND_MANUAL:
		return engine.CommitKindManual, nil
	case storagev1.CommitKind_COMMIT_KIND_FORK:
		return engine.CommitKindFork, nil
	case storagev1.CommitKind_COMMIT_KIND_MERGE:
		return engine.CommitKindMerge, nil
	default:
		return engine.CommitKindUnspecified, engine.ErrInvalid
	}
}

func toProtoCommit(diskID uuid.UUID, c engine.CommitMeta) *storagev1.Commit {
	return &storagev1.Commit{
		DiskId:        diskID.String(),
		CommitId:      c.ID,
		ParentId:      c.ParentID,
		MergeParentId: c.MergeParentID,
		Kind:          toProtoCommitKind(c.Kind),
		Message:       c.Message,
		CreatedAt:     toProtoTimeFromNano(c.CreatedAt),
	}
}

func toProtoMountLease(m engine.MountMeta) *storagev1.MountLease {
	return &storagev1.MountLease{
		MountId:   m.ID,
		Branch:    m.BranchName,
		ReadOnly:  m.ReadOnly,
		Epoch:     m.WriterEpoch,
		MountedAt: toProtoTimeFromNano(m.MountedAt),
		ExpiresAt: toProtoTimeFromNano(m.ExpiresAt),
	}
}

func (s *Server) CreateDisk(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	disk, err := s.node.CreateDisk(ctx, req.Msg.Name, req.Msg.MaxBytes)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.CreateDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

func (s *Server) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	disk, err := s.node.GetDisk(ctx, diskID)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.GetDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

func (s *Server) ListDisks(ctx context.Context, req *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
	page, err := s.node.ListDisks(ctx, engine.ListDisksQuery{
		PageSize:  req.Msg.PageSize,
		PageToken: req.Msg.PageToken,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	disks := make([]*storagev1.Disk, 0, len(page.Disks))
	for _, d := range page.Disks {
		disks = append(disks, toProtoDisk(d))
	}
	return connect.NewResponse(&storagev1.ListDisksResponse{
		Disks:         disks,
		NextPageToken: page.NextPageToken,
	}), nil
}

func (s *Server) UpdateDisk(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	disk, err := s.node.UpdateDisk(ctx, diskID, req.Msg.Name, req.Msg.MaxBytes)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.UpdateDiskResponse{
		Disk: toProtoDisk(disk),
	}), nil
}

func (s *Server) DeleteDisk(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if err = s.node.DeleteDisk(ctx, diskID); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
}

func (s *Server) MountBranch(ctx context.Context, req *connect.Request[storagev1.MountBranchRequest]) (*connect.Response[storagev1.MountBranchResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	res, err := s.node.MountBranch(ctx, diskID, req.Msg.Branch, req.Msg.ReadOnly, req.Msg.Force)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.MountBranchResponse{
		Epoch:               res.Mount.WriterEpoch,
		RootInode:           toProtoInode(res.RootInode),
		EntryTimeoutSeconds: s.entryTimeoutForMode(res.RootInode.Mode),
		AttrTimeoutSeconds:  s.attrTimeoutSeconds(),
		MountId:             res.Mount.ID,
		LeaseTtlSeconds:     s.leaseTTLSeconds(),
	}), nil
}

func (s *Server) RenewMount(ctx context.Context, req *connect.Request[storagev1.RenewMountRequest]) (*connect.Response[storagev1.RenewMountResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	mountID, err := parseRequiredUUID(req.Msg.MountId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if _, err = s.node.RenewMount(ctx, diskID, req.Msg.Branch, mountID, req.Msg.ReadOnly, req.Msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.RenewMountResponse{
		LeaseTtlSeconds: s.leaseTTLSeconds(),
	}), nil
}

func (s *Server) UnmountBranch(ctx context.Context, req *connect.Request[storagev1.UnmountBranchRequest]) (*connect.Response[storagev1.UnmountBranchResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	mountID, err := parseRequiredUUID(req.Msg.MountId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if err = s.node.UnmountBranch(ctx, diskID, mountID); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.UnmountBranchResponse{}), nil
}

func (s *Server) ListMounts(ctx context.Context, req *connect.Request[storagev1.ListMountsRequest]) (*connect.Response[storagev1.ListMountsResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	leases, err := s.node.ListMounts(ctx, diskID, req.Msg.Branch)
	if err != nil {
		return nil, toConnectError(err)
	}
	mounts := make([]*storagev1.MountLease, 0, len(leases))
	for _, lease := range leases {
		mounts = append(mounts, toProtoMountLease(lease))
	}
	return connect.NewResponse(&storagev1.ListMountsResponse{
		Mounts: mounts,
	}), nil
}

func (s *Server) EvictMounts(ctx context.Context, req *connect.Request[storagev1.EvictMountsRequest]) (*connect.Response[storagev1.EvictMountsResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	mountID, err := parseOptionalUUID(req.Msg.MountId)
	if err != nil {
		return nil, toConnectError(err)
	}
	res, err := s.node.EvictMounts(ctx, diskID, req.Msg.Branch, mountID)
	if err != nil {
		return nil, toConnectError(err)
	}
	updatedBranches := make([]*storagev1.Branch, 0, len(res.UpdatedBranches))
	for _, br := range res.UpdatedBranches {
		updatedBranches = append(updatedBranches, toProtoBranch(diskID, br))
	}
	return connect.NewResponse(&storagev1.EvictMountsResponse{
		EvictedCount:    res.EvictedCount,
		UpdatedBranches: updatedBranches,
	}), nil
}

func (s *Server) StatFS(ctx context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var res engine.StatFSResult
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		if _, attrErr := ds.GetAttr(ctx, req.Msg.Branch, req.Msg.Ino); attrErr != nil {
			return attrErr
		}
		var stErr error
		res, stErr = ds.StatFSBranch(ctx, req.Msg.Branch)
		return stErr
	})
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
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var inode engine.Inode
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var lErr error
		_, inode, lErr = ds.Lookup(ctx, req.Msg.Branch, req.Msg.ParentIno, string(req.Msg.Name))
		return lErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: s.entryTimeoutForMode(inode.Mode),
		AttrTimeoutSeconds:  s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var inode engine.Inode
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var gErr error
		inode, gErr = ds.GetAttr(ctx, req.Msg.Branch, req.Msg.Ino)
		return gErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              toProtoInode(inode),
		AttrTimeoutSeconds: s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	input := engine.SetAttrReq{
		Mode:     req.Msg.Mode,
		UID:      req.Msg.Uid,
		GID:      req.Msg.Gid,
		Size:     req.Msg.Size,
		AtimeNow: req.Msg.AtimeNow,
		MtimeNow: req.Msg.MtimeNow,
		CtimeNow: req.Msg.CtimeNow,
		Flags:    req.Msg.Flags,
	}
	if req.Msg.Atime != nil {
		ns := req.Msg.Atime.AsTime().UnixNano()
		input.Atime = &ns
	}
	if req.Msg.Mtime != nil {
		ns := req.Msg.Mtime.AsTime().UnixNano()
		input.Mtime = &ns
	}

	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.Epoch)
	var inode engine.Inode
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var sErr error
		inode, sErr = ds.SetAttr(epochCtx, req.Msg.Branch, req.Msg.Ino, input)
		return sErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              toProtoInode(inode),
		AttrTimeoutSeconds: s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) Mknod(ctx context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.Epoch)
	var inode engine.Inode
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var mErr error
		inode, mErr = ds.Mknod(epochCtx, req.Msg.Branch, req.Msg.ParentIno, string(req.Msg.Name), req.Msg.Mode, req.Msg.Uid, req.Msg.Gid, req.Msg.Rdev)
		return mErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.MknodResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: s.entryTimeoutForMode(inode.Mode),
		AttrTimeoutSeconds:  s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) Mkdir(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.Epoch)
	var inode engine.Inode
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var mErr error
		inode, mErr = ds.Mkdir(epochCtx, req.Msg.Branch, req.Msg.ParentIno, string(req.Msg.Name), req.Msg.Mode, req.Msg.Uid, req.Msg.Gid)
		return mErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: s.entryTimeoutForMode(inode.Mode),
		AttrTimeoutSeconds:  s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) Symlink(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, req.Msg.Epoch)
	var inode engine.Inode
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var sErr error
		inode, sErr = ds.Symlink(epochCtx, req.Msg.Branch, req.Msg.ParentIno, string(req.Msg.Name), string(req.Msg.Target), req.Msg.Uid, req.Msg.Gid)
		return sErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.SymlinkResponse{
		Inode:               toProtoInode(inode),
		EntryTimeoutSeconds: s.entryTimeoutForMode(inode.Mode),
		AttrTimeoutSeconds:  s.attrTimeoutSeconds(),
	}), nil
}

func (s *Server) Readlink(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	diskID, err := parseRequiredUUID(req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	var target []byte
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var rErr error
		target, rErr = ds.Readlink(ctx, req.Msg.Branch, req.Msg.Ino)
		return rErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ReadlinkResponse{
		Target: target,
	}), nil
}
