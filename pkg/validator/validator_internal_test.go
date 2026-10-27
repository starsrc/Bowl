package validator

import (
	"reflect"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	bufvalidate "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestStorageMessagesRegistered(t *testing.T) {
	requiredTypes := []reflect.Type{
		reflect.TypeOf(&storagev1.ReadRequest{}),
		reflect.TypeOf(&storagev1.ReadResponse{}),
		reflect.TypeOf(&storagev1.WriteRequest{}),
		reflect.TypeOf(&storagev1.WriteResponse{}),
	}

	registeredMap := make(map[reflect.Type]bool)
	for _, msg := range storageMessages {
		registeredMap[reflect.TypeOf(msg)] = true
	}

	for _, reqType := range requiredTypes {
		assert.Truef(t, registeredMap[reqType], "storageMessages 必须包含 %v 以支持服务启动期预编译 CEL 规则", reqType)
	}
}

func TestClassifyViolationPriority(t *testing.T) {
	testFields := []string{
		"storage.v1.ReadRequest.ino",
		"storage.v1.ReadRequest.fh",
		"storage.v1.ReadRequest.offset",
		"storage.v1.ReadRequest.size",
		"storage.v1.ReadRequest.session_id",
		"storage.v1.WriteRequest.ino",
		"storage.v1.WriteRequest.fh",
		"storage.v1.WriteRequest.offset",
		"storage.v1.WriteRequest.data",
		"storage.v1.WriteRequest.session_id",
		"storage.v1.WriteResponse.size",
		"storage.v1.WriteResponse.inode",
	}

	for _, f := range testFields {
		t.Run(f, func(t *testing.T) {
			fd := mockFieldDescriptor(protoreflect.FullName(f))
			v := &protovalidate.Violation{
				FieldDescriptor: fd,
				Proto: &bufvalidate.Violation{
					RuleId: proto.String("test_rule"),
				},
			}
			priority, reason := classifyViolationInternal(v)
			assert.Equal(t, 10, priority, "字段 %s 应具备第 1 优先级 (10)", f)
			assert.Equal(t, ReasonInvalidArgument, reason, "字段 %s 应映射为 EINVAL", f)
		})
	}

	t.Run("CEL: write_request_offset_len_overflow", func(t *testing.T) {
		v := &protovalidate.Violation{
			Proto: &bufvalidate.Violation{
				RuleId: proto.String("write_request_offset_len_overflow"),
			},
		}
		priority, reason := classifyViolationInternal(v)
		assert.Equal(t, 10, priority, "CEL write_request_offset_len_overflow 应具备第 1 优先级 (10)")
		assert.Equal(t, ReasonInvalidArgument, reason, "CEL write_request_offset_len_overflow 应映射为 EINVAL")
	})
}

type fakeFieldDescriptor struct {
	protoreflect.FieldDescriptor
	fullName protoreflect.FullName
}

func (f *fakeFieldDescriptor) FullName() protoreflect.FullName {
	return f.fullName
}

func mockFieldDescriptor(name protoreflect.FullName) protoreflect.FieldDescriptor {
	return &fakeFieldDescriptor{fullName: name}
}
