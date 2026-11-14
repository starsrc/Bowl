package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/handoff"
)

// HandoffErrorDomain 标识交接接口附带的第二个 ErrorInfo。
// 通用的 bowl.storage ErrorInfo 把几种交接失败都归成 EBUSY，调用方分不出该怎么处理；
// 这个领域下的 Reason 是可按值分支的具体原因（见 handoffReason）。
const HandoffErrorDomain = "bowl.storage.handoff"

// handoffControl 取出控制面的交接能力；节点没配置交接远端时返回 Unimplemented。
func (s *Server) handoffControl() (control.Handoff, error) {
	h, ok := s.control.(control.Handoff)
	if !ok {
		return nil, toHandoffError(control.ErrHandoffDisabled)
	}
	return h, nil
}

// PushDisk 把磁盘当前状态同步到交接远端，可同时交出写权限。
func (s *Server) PushDisk(ctx context.Context, req *connect.Request[storagev1.PushDiskRequest]) (*connect.Response[storagev1.PushDiskResponse], error) {
	h, err := s.handoffControl()
	if err != nil {
		return nil, err
	}
	res, pushErr := h.PushDisk(ctx, control.PushDiskParams{DiskID: req.Msg.DiskId, Release: req.Msg.Release})
	if pushErr != nil {
		return nil, toHandoffError(pushErr)
	}
	return connect.NewResponse(&storagev1.PushDiskResponse{
		Head:              toProtoDiskHead(&res.Head),
		Packs:             nonNegative(res.Stats.Packs),
		PacksUploaded:     nonNegative(res.Stats.PacksUploaded),
		PackBytesUploaded: nonNegative(res.Stats.PackBytes),
		MetaBytes:         nonNegative(res.Stats.MetaBytes),
		Unchanged:         res.Unchanged,
	}), nil
}

// PullDisk 从交接远端拉取磁盘最新快照并把本节点登记为写者。
func (s *Server) PullDisk(ctx context.Context, req *connect.Request[storagev1.PullDiskRequest]) (*connect.Response[storagev1.PullDiskResponse], error) {
	h, err := s.handoffControl()
	if err != nil {
		return nil, err
	}
	res, pullErr := h.PullDisk(ctx, control.PullDiskParams{DiskID: req.Msg.DiskId, Force: req.Msg.Force, KeepLocal: req.Msg.KeepLocal})
	if pullErr != nil {
		return nil, toHandoffError(pullErr)
	}
	return connect.NewResponse(&storagev1.PullDiskResponse{
		Head:                toProtoDiskHead(&res.Head),
		Installed:           res.Installed,
		Packs:               nonNegative(res.Pulled.Packs),
		PacksReused:         nonNegative(res.Pulled.PacksReused),
		PacksDownloaded:     nonNegative(res.Pulled.PacksDownloaded),
		PackBytesDownloaded: nonNegative(res.Pulled.PackBytes),
		PacksDeferred:       nonNegative(res.Pulled.PacksDeferred),
		PackBytesDeferred:   nonNegative(res.Pulled.DeferredBytes),
		MetaBytes:           nonNegative(res.Pulled.MetaBytes),
		KeptLocalDiskId:     res.KeptLocalDiskID,
	}), nil
}

// GetDiskHead 读取交接远端记录的当前写者与最新快照。
func (s *Server) GetDiskHead(ctx context.Context, req *connect.Request[storagev1.GetDiskHeadRequest]) (*connect.Response[storagev1.GetDiskHeadResponse], error) {
	h, err := s.handoffControl()
	if err != nil {
		return nil, err
	}
	head, headErr := h.HeadDisk(ctx, req.Msg.DiskId)
	if headErr != nil {
		return nil, toHandoffError(headErr)
	}
	return connect.NewResponse(&storagev1.GetDiskHeadResponse{Head: toProtoDiskHead(head)}), nil
}

// GetDiskHandoffStatus 查询磁盘在本节点视角下的交接状态。
func (s *Server) GetDiskHandoffStatus(ctx context.Context, req *connect.Request[storagev1.GetDiskHandoffStatusRequest]) (*connect.Response[storagev1.GetDiskHandoffStatusResponse], error) {
	h, err := s.handoffControl()
	if err != nil {
		return nil, err
	}
	st, statusErr := h.StatusDisk(ctx, req.Msg.DiskId)
	if statusErr != nil {
		return nil, toHandoffError(statusErr)
	}
	return connect.NewResponse(&storagev1.GetDiskHandoffStatusResponse{
		DiskId:        st.DiskID,
		NodeId:        st.NodeID,
		Role:          toProtoHandoffRole(st.Role),
		Head:          toProtoDiskHead(st.Head),
		LocalSnapshot: st.LocalSnapshot,
		PendingPacks:  nonNegative(st.PendingPacks),
	}), nil
}

func toProtoDiskHead(head *handoff.Head) *storagev1.DiskHead {
	if head == nil {
		return nil
	}
	return &storagev1.DiskHead{
		DiskId:      head.DiskID,
		Name:        head.Name,
		Generation:  head.Generation,
		Owner:       head.Owner,
		Snapshot:    head.Snapshot,
		WriterEpoch: head.WriterEpoch,
		WriteSeq:    head.WriteSeq,
		UpdatedAt:   timestamppb.New(head.UpdatedAt),
		UpdatedBy:   head.UpdatedBy,
	}
}

func toProtoHandoffRole(role string) storagev1.DiskHandoffRole {
	switch role {
	case "writer":
		return storagev1.DiskHandoffRole_DISK_HANDOFF_ROLE_WRITER
	case "replica":
		return storagev1.DiskHandoffRole_DISK_HANDOFF_ROLE_REPLICA
	case "released":
		return storagev1.DiskHandoffRole_DISK_HANDOFF_ROLE_RELEASED
	case "unbound":
		return storagev1.DiskHandoffRole_DISK_HANDOFF_ROLE_UNBOUND
	case "absent":
		return storagev1.DiskHandoffRole_DISK_HANDOFF_ROLE_ABSENT
	default:
		return storagev1.DiskHandoffRole_DISK_HANDOFF_ROLE_UNSPECIFIED
	}
}

// nonNegative 把计数与字节数转成协议里的无符号数。
func nonNegative[T int | int64](v T) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// toHandoffError 在通用错误映射之上再附一个交接专用的 ErrorInfo。
func toHandoffError(err error) error {
	out := toConnectError(err)
	connectErr, ok := errors.AsType[*connect.Error](out)
	if !ok {
		return out
	}
	if detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: handoffReason(err),
		Domain: HandoffErrorDomain,
	}); detailErr == nil {
		connectErr.AddDetail(detail)
	}
	return connectErr
}

// handoffReason 给调用方一个稳定的、可按值分支的失败原因。
func handoffReason(err error) string {
	switch {
	case errors.Is(err, control.ErrNotOwner):
		return "NOT_OWNER"
	case errors.Is(err, control.ErrDiskMounted):
		return "MOUNTED"
	case errors.Is(err, control.ErrLocalDiverged):
		return "LOCAL_DIVERGED"
	case errors.Is(err, control.ErrHandoffConflict):
		return "CONFLICT"
	case errors.Is(err, control.ErrHandoffInProgress):
		return "IN_PROGRESS"
	case errors.Is(err, control.ErrHandoffDisabled):
		return "DISABLED"
	case errors.Is(err, control.ErrNotFound):
		return "NOT_FOUND"
	case errors.Is(err, control.ErrInvalidArgument):
		return "INVALID"
	default:
		return "INTERNAL"
	}
}
