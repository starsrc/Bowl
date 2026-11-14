package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"

	"github.com/starsrc/bowl/services/storage/internal/control"
)

// 交接（handoff）的管理接口。
//
// ponytail: 原型阶段用三个普通 JSON 路由暴露，不进 storage.proto —— 协议还在变，
// 进了公开契约就要背兼容。等交接语义定下来再提升为正式 RPC。
// 这三个路由和现有 RPC 一样没有服务端鉴权，不要暴露到不可信网络。

type handoffRequest struct {
	DiskID  string `json:"disk_id"`
	Release bool   `json:"release"`
	Force   bool   `json:"force"`
	// KeepLocal 仅对 pull 有效：本地有未同步写入时另存为一块新盘，而不是拒绝。
	KeepLocal bool `json:"keep_local"`
}

type handoffErrorBody struct {
	Error  string `json:"error"`
	Reason string `json:"reason"`
}

// registerHandoffRoutes 在控制面支持交接时挂上 push / pull / head 三个路由。
func registerHandoffRoutes(mux *http.ServeMux, ctrl control.Manager) {
	h, ok := ctrl.(control.Handoff)
	if !ok {
		return
	}
	mux.HandleFunc("POST /handoff/v1/push", func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeHandoffRequest(w, r)
		if !ok {
			return
		}
		res, err := h.PushDisk(r.Context(), control.PushDiskParams{DiskID: req.DiskID, Release: req.Release})
		writeHandoffResult(w, res, err)
	})
	mux.HandleFunc("POST /handoff/v1/pull", func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeHandoffRequest(w, r)
		if !ok {
			return
		}
		res, err := h.PullDisk(r.Context(), control.PullDiskParams{DiskID: req.DiskID, Force: req.Force, KeepLocal: req.KeepLocal})
		writeHandoffResult(w, res, err)
	})
	mux.HandleFunc("GET /handoff/v1/head", func(w http.ResponseWriter, r *http.Request) {
		head, err := h.HeadDisk(r.Context(), r.URL.Query().Get("disk_id"))
		writeHandoffResult(w, head, err)
	})
}

func decodeHandoffRequest(w http.ResponseWriter, r *http.Request) (handoffRequest, bool) {
	var req handoffRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeHandoffError(w, http.StatusBadRequest, "EINVAL", errors.New("invalid JSON body: "+err.Error()))
		return handoffRequest{}, false
	}
	return req, true
}

func writeHandoffResult(w http.ResponseWriter, body any, err error) {
	if err != nil {
		st := control.ToStatus(err)
		writeHandoffError(w, httpStatusFromCode(st.Code()), handoffReason(err), err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func writeHandoffError(w http.ResponseWriter, status int, reason string, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(handoffErrorBody{Error: err.Error(), Reason: reason})
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

func httpStatusFromCode(code codes.Code) int {
	switch code {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.FailedPrecondition, codes.Aborted, codes.AlreadyExists:
		return http.StatusConflict
	case codes.Unimplemented:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}
