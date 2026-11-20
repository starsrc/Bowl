package server

import (
	"context"
	"fmt"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpccodec"
)

const (
	writePayloadRetainedCap = 8
	readPayloadRetainedCap  = 8
)

var (
	writePayloadLeases = rpccodec.NewPayloadLeasePool[storagev1.WriteRequest](
		writePayloadRetainedCap,
		rpccodec.OrphanRecycleBuffer,
	)
	readPayloadLeases = rpccodec.NewPayloadLeasePool[storagev1.ReadResponse](
		readPayloadRetainedCap,
		rpccodec.OrphanRecycleBuffer,
	)
)

func clearWriteRequestData(msg *storagev1.WriteRequest) {
	msg.Data = nil
}

func clearReadResponseData(msg *storagev1.ReadResponse) {
	msg.Data = nil
}

func releaseWritePayload(msg *storagev1.WriteRequest) {
	writePayloadLeases.Release(msg, clearWriteRequestData)
}

func leasedWritePayloadCount() int {
	return writePayloadLeases.LeasedCount()
}

func acquireReadPayloadBuf() *[maxReadWriteSize]byte {
	return readPayloadLeases.AcquireBuf()
}

func recycleReadPayloadBuf(buf *[maxReadWriteSize]byte) {
	readPayloadLeases.RecycleBuf(buf)
}

func registerLeasedReadPayload(msg *storagev1.ReadResponse, buf *[maxReadWriteSize]byte) {
	readPayloadLeases.Register(msg, buf)
}

func releaseReadPayload(msg *storagev1.ReadResponse) {
	readPayloadLeases.Release(msg, clearReadResponseData)
}

func leasedReadPayloadCount() int {
	return readPayloadLeases.LeasedCount()
}

func newWritePayloadReleaseInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req != nil {
				if msg, ok := req.Any().(*storagev1.WriteRequest); ok {
					defer releaseWritePayload(msg)
				}
			}
			return next(ctx, req)
		}
	}
}

func newProtoBinaryCodec() *rpccodec.ProtoBinaryCodec {
	return rpccodec.NewProtoBinaryCodec(rpccodec.CodecHooks{
		MarshalAppend: marshalReadResponseHook,
		AfterMarshal:  afterMarshalReadResponseHook,
		Unmarshal:     unmarshalWriteRequestHook,
	})
}

func afterMarshalReadResponseHook(message any) {
	if readResp, ok := message.(*storagev1.ReadResponse); ok && readResp != nil {
		releaseReadPayload(readResp)
	}
}

func marshalReadResponseHook(dst []byte, message any) (out []byte, handled bool, err error) {
	readResp, ok := message.(*storagev1.ReadResponse)
	if !ok {
		return nil, false, nil
	}
	if readResp == nil {
		return dst, true, nil
	}
	data := readResp.Data
	if len(data) == 0 {
		releaseReadPayload(readResp)
		return dst, true, nil
	}

	need := 1 + protowire.SizeVarint(uint64(len(data))) + len(data)
	if cap(dst)-len(dst) < need {
		grown := make([]byte, len(dst), len(dst)+need)
		copy(grown, dst)
		dst = grown
	}
	dst = append(dst, 0x0a)
	dst = protowire.AppendVarint(dst, uint64(len(data)))
	dst = append(dst, data...)
	releaseReadPayload(readResp)
	return dst, true, nil
}

func unmarshalWriteRequestHook(data []byte, message any) (bool, error) {
	writeReq, ok := message.(*storagev1.WriteRequest)
	if !ok {
		return false, nil
	}
	return true, unmarshalWriteRequest(data, writeReq)
}

func unmarshalWriteRequest(raw []byte, msg *storagev1.WriteRequest) error {
	if msg == nil {
		return fmt.Errorf("unmarshal into %T: nil message", msg)
	}
	releaseWritePayload(msg)
	proto.Reset(msg)
	var leased *[maxReadWriteSize]byte
	fallback := func() error {
		if leased != nil {
			writePayloadLeases.RecycleBuf(leased)
			leased = nil
		}
		proto.Reset(msg)
		if err := proto.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("unmarshal into %T: %w", msg, err)
		}
		return nil
	}

	b := raw
	for len(b) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return fallback()
		}
		b = b[tagLen:]

		switch num {
		case 1:
			if typ != protowire.VarintType {
				return fallback()
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fallback()
			}
			msg.Ino = v
			b = b[n:]
		case 2:
			if typ != protowire.VarintType {
				return fallback()
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fallback()
			}
			msg.Fh = v
			b = b[n:]
		case 3:
			if typ != protowire.VarintType {
				return fallback()
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fallback()
			}
			msg.Offset = v
			b = b[n:]
		case 4:
			if typ != protowire.BytesType {
				return fallback()
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return fallback()
			}
			switch {
			case len(v) == 0:
				if leased != nil {
					writePayloadLeases.RecycleBuf(leased)
					leased = nil
				}
				msg.Data = []byte{}
			case len(v) <= maxReadWriteSize:
				if leased == nil {
					leased = writePayloadLeases.AcquireBuf()
				}
				copy(leased[:len(v)], v)
				msg.Data = leased[:len(v):len(v)]
			default:
				if leased != nil {
					writePayloadLeases.RecycleBuf(leased)
					leased = nil
				}
				msg.Data = append([]byte(nil), v...)
			}
			b = b[n:]
		case 5:
			if typ != protowire.BytesType {
				return fallback()
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 || !utf8.Valid(v) {
				return fallback()
			}
			msg.DiskId = string(v)
			b = b[n:]
		case 6:
			if typ != protowire.VarintType {
				return fallback()
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fallback()
			}
			msg.Epoch = v
			b = b[n:]
		default:
			return fallback()
		}
	}

	if leased != nil {
		writePayloadLeases.Register(msg, leased)
	}
	return nil
}
