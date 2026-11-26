package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

const maxReadWriteSize = 4 << 20

func (s *Server) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	msg := req.Msg
	diskID, err := parseRequiredUUID(msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if msg.Size == 0 {
		var data []byte
		err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
			var rErr error
			data, rErr = ds.Read(ctx, msg.Branch, msg.Ino, int64(msg.Offset), 0)
			return rErr
		})
		if err != nil {
			return nil, toConnectError(err)
		}
		return connect.NewResponse(&storagev1.ReadResponse{Data: data}), nil
	}

	leased := acquireReadPayloadBuf()
	var data []byte
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var rErr error
		data, rErr = ds.ReadInto(ctx, msg.Branch, msg.Ino, int64(msg.Offset), int(msg.Size), leased[:0])
		return rErr
	})
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
	diskID, err := parseRequiredUUID(msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	epochCtx := engine.WithWriterEpoch(ctx, msg.Epoch)
	var (
		written int
		inode   engine.Inode
	)
	err = s.node.WithDisk(ctx, diskID, func(ds *engine.DiskStore) error {
		var wErr error
		written, inode, wErr = ds.WriteWithAttr(epochCtx, msg.Branch, msg.Ino, int64(msg.Offset), msg.Data)
		return wErr
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  uint32(max(0, written)),
		Inode: toProtoInode(inode),
	}), nil
}
