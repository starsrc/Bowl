package rpc

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpccodec"
)

const (
	MaxReadPayloadSize = rpccodec.MaxPayloadSize

	SmallReadPayloadCutoff = 256 << 10

	readPayloadRetainedCap = 16
)

var readPayloadLeases = rpccodec.NewPayloadLeasePool[storagev1.ReadResponse](
	readPayloadRetainedCap,
	rpccodec.OrphanDropEntryOnly,
)

type ReadPayload struct {
	Data  []byte
	guard *rpccodec.Guard
}

func (p *ReadPayload) Release() {
	if p == nil {
		return
	}
	if p.guard != nil {
		p.guard.Release()
		p.guard = nil
	}
	p.Data = nil
}

func clearReadResponseData(msg *storagev1.ReadResponse) {
	msg.Data = nil
}

func TakeReadPayload(msg *storagev1.ReadResponse) ReadPayload {
	if msg == nil {
		return ReadPayload{}
	}
	data := msg.Data
	msg.Data = nil
	if len(data) <= SmallReadPayloadCutoff || len(data) > MaxReadPayloadSize {
		return ReadPayload{Data: data}
	}
	return ReadPayload{
		Data:  data,
		guard: readPayloadLeases.Claim(msg),
	}
}

func leasedReadPayloadCount() int {
	return readPayloadLeases.LeasedCount()
}

func newProtoBinaryCodec() connect.Codec {
	return rpccodec.NewProtoBinaryCodec(rpccodec.CodecHooks{
		MarshalAppend: marshalWriteRequestHook,
		Unmarshal:     unmarshalReadResponseHook,
	})
}

func marshalWriteRequestHook(dst []byte, message any) (out []byte, handled bool, err error) {
	writeReq, ok := message.(*storagev1.WriteRequest)
	if !ok {
		return nil, false, nil
	}
	out, err = marshalAppendWriteRequest(dst, writeReq)
	return out, true, err
}

func unmarshalReadResponseHook(data []byte, message any) (bool, error) {
	readResp, ok := message.(*storagev1.ReadResponse)
	if !ok {
		return false, nil
	}
	return true, unmarshalReadResponse(data, readResp)
}

func writeRequestWireSize(msg *storagev1.WriteRequest) int {
	var n int
	if msg.Ino != 0 {
		n += 1 + protowire.SizeVarint(msg.Ino)
	}
	if msg.Fh != 0 {
		n += 1 + protowire.SizeVarint(msg.Fh)
	}
	if msg.Offset != 0 {
		n += 1 + protowire.SizeVarint(msg.Offset)
	}
	if len(msg.Data) > 0 {
		n += 1 + protowire.SizeBytes(len(msg.Data))
	}
	if msg.DiskId != "" {
		n += 1 + protowire.SizeBytes(len(msg.DiskId))
	}
	if msg.Epoch != 0 {
		n += 1 + protowire.SizeVarint(msg.Epoch)
	}
	if msg.Branch != "" {
		n += 1 + protowire.SizeBytes(len(msg.Branch))
	}
	return n
}

func marshalAppendWriteRequest(dst []byte, msg *storagev1.WriteRequest) ([]byte, error) {
	if msg == nil {
		if dst == nil {
			return []byte{}, nil
		}
		return dst, nil
	}
	if !utf8.ValidString(msg.DiskId) || !utf8.ValidString(msg.Branch) {
		return proto.MarshalOptions{}.MarshalAppend(dst, msg)
	}

	wireSize := writeRequestWireSize(msg)
	if wireSize == 0 {
		if dst == nil {
			return []byte{}, nil
		}
		return dst, nil
	}
	if cap(dst)-len(dst) < wireSize {
		grown := make([]byte, len(dst), len(dst)+wireSize)
		copy(grown, dst)
		dst = grown
	}

	if msg.Ino != 0 {
		dst = protowire.AppendTag(dst, 1, protowire.VarintType)
		dst = protowire.AppendVarint(dst, msg.Ino)
	}
	if msg.Fh != 0 {
		dst = protowire.AppendTag(dst, 2, protowire.VarintType)
		dst = protowire.AppendVarint(dst, msg.Fh)
	}
	if msg.Offset != 0 {
		dst = protowire.AppendTag(dst, 3, protowire.VarintType)
		dst = protowire.AppendVarint(dst, msg.Offset)
	}
	if len(msg.Data) > 0 {
		dst = protowire.AppendTag(dst, 4, protowire.BytesType)
		dst = protowire.AppendBytes(dst, msg.Data)
	}
	if msg.DiskId != "" {
		dst = protowire.AppendTag(dst, 5, protowire.BytesType)
		dst = protowire.AppendString(dst, msg.DiskId)
	}
	if msg.Epoch != 0 {
		dst = protowire.AppendTag(dst, 6, protowire.VarintType)
		dst = protowire.AppendVarint(dst, msg.Epoch)
	}
	if msg.Branch != "" {
		dst = protowire.AppendTag(dst, 7, protowire.BytesType)
		dst = protowire.AppendString(dst, msg.Branch)
	}
	return dst, nil
}

func unmarshalReadResponse(raw []byte, msg *storagev1.ReadResponse) error {
	if msg == nil {
		return fmt.Errorf("unmarshal into %T: nil message", msg)
	}
	readPayloadLeases.Release(msg, clearReadResponseData)
	proto.Reset(msg)

	var leased *[MaxReadPayloadSize]byte
	fallback := func() error {
		if leased != nil {
			readPayloadLeases.RecycleBuf(leased)
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
					readPayloadLeases.RecycleBuf(leased)
					leased = nil
				}
				msg.Data = []byte{}
			case len(v) <= SmallReadPayloadCutoff:
				if leased != nil {
					readPayloadLeases.RecycleBuf(leased)
					leased = nil
				}
				msg.Data = bytes.Clone(v)
			case len(v) <= MaxReadPayloadSize:
				if leased == nil {
					leased = readPayloadLeases.AcquireBuf()
				}
				copy(leased[:len(v)], v)
				msg.Data = leased[:len(v):len(v)]
			default:
				if leased != nil {
					readPayloadLeases.RecycleBuf(leased)
					leased = nil
				}
				msg.Data = bytes.Clone(v)
			}
			b = b[n:]
		default:
			return fallback()
		}
	}

	if leased != nil {
		readPayloadLeases.Register(msg, leased)
	}
	return nil
}
