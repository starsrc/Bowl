package rpccodec

import (
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

const (
	CodecNameProto = "proto"
)

type CodecHooks struct {
	MarshalAppend func(dst []byte, msg any) (out []byte, handled bool, err error)
	AfterMarshal  func(msg any)
	Unmarshal     func(data []byte, msg any) (handled bool, err error)
}

type ProtoBinaryCodec struct {
	hooks CodecHooks
}

var _ connect.Codec = (*ProtoBinaryCodec)(nil)

func NewProtoBinaryCodec(hooks CodecHooks) *ProtoBinaryCodec {
	return &ProtoBinaryCodec{hooks: hooks}
}

func (c *ProtoBinaryCodec) Name() string {
	return CodecNameProto
}

func (c *ProtoBinaryCodec) IsBinary() bool {
	return true
}

func (c *ProtoBinaryCodec) Marshal(message any) ([]byte, error) {
	if c.hooks.MarshalAppend != nil {
		if out, handled, err := c.hooks.MarshalAppend(nil, message); handled {
			return out, err
		}
	}
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if c.hooks.AfterMarshal != nil {
		defer c.hooks.AfterMarshal(message)
	}
	return proto.Marshal(protoMsg)
}

func (c *ProtoBinaryCodec) MarshalAppend(dst []byte, message any) ([]byte, error) {
	if c.hooks.MarshalAppend != nil {
		if out, handled, err := c.hooks.MarshalAppend(dst, message); handled {
			return out, err
		}
	}
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if c.hooks.AfterMarshal != nil {
		defer c.hooks.AfterMarshal(message)
	}
	return proto.MarshalOptions{}.MarshalAppend(dst, protoMsg)
}

func (c *ProtoBinaryCodec) MarshalStable(message any) ([]byte, error) {
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if c.hooks.AfterMarshal != nil {
		defer c.hooks.AfterMarshal(message)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(protoMsg)
}

func (c *ProtoBinaryCodec) Unmarshal(data []byte, message any) error {
	if c.hooks.Unmarshal != nil {
		if handled, err := c.hooks.Unmarshal(data, message); handled {
			return err
		}
	}
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return fmt.Errorf("%T does not implement proto.Message", message)
	}
	if err := proto.Unmarshal(data, protoMsg); err != nil {
		return fmt.Errorf("unmarshal into %T: %w", message, err)
	}
	return nil
}
