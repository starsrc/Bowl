package server

import (
	"context"
	"math"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

const maxReadWriteSize = 4 << 20

func (s *Server) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	msg := req.Msg
	if msg.Fh > math.MaxInt64 || msg.Size > maxReadWriteSize {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	if msg.Size == 0 {
		data, rErr := actor.Read(ctx, msg.Ino, msg.Offset, 0)
		if rErr != nil {
			return nil, toConnectError(rErr)
		}
		return connect.NewResponse(&storagev1.ReadResponse{Data: data}), nil
	}

	leased := acquireReadPayloadBuf()
	data, err := actor.ReadInto(ctx, msg.Ino, msg.Offset, msg.Size, leased[:0])
	if err != nil {
		recycleReadPayloadBuf(leased)
		return nil, toConnectError(err)
	}
	if len(data) == 0 {
		recycleReadPayloadBuf(leased)
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}

	respMsg := &storagev1.ReadResponse{
		Data: data,
	}
	if &data[0] == &leased[0] {
		registerLeasedReadPayload(respMsg, leased)
	} else {
		recycleReadPayloadBuf(leased)
	}
	return connect.NewResponse(respMsg), nil
}

func (s *Server) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	msg := req.Msg
	if err := validateWriteEpoch(msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if msg.Fh > math.MaxInt64 || len(msg.Data) > maxReadWriteSize {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	written, attr, err := actor.Write(ctx, msg.Ino, msg.Offset, msg.Data, msg.Epoch)
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  written,
		Inode: toProtoInode(&attr),
	}), nil
}
