package server

import (
	"context"
	"errors"
	"strings"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	ReasonInvalidArgument = "EINVAL"
	ReasonNameTooLong     = "ENAMETOOLONG"
	ReasonRange           = "ERANGE"
	ReasonTooBig          = "E2BIG"
)

type ValidationError struct {
	err    *protovalidate.ValidationError
	reason string
	domain string
}

func (e *ValidationError) Error() string {
	return e.err.Error()
}

func (e *ValidationError) Unwrap() error {
	return e.err
}

func (e *ValidationError) Reason() string {
	return e.reason
}

func (e *ValidationError) Domain() string {
	if e.domain != "" {
		return e.domain
	}
	return ErrorDomain
}

func (e *ValidationError) GRPCStatus() *status.Status {
	return buildInvalidArgumentStatus(e.err.Error(), e.reason, e.Domain())
}

type storageValidator struct {
	inner protovalidate.Validator
}

func (v *storageValidator) Validate(msg proto.Message, options ...protovalidate.ValidationOption) error {
	err := v.inner.Validate(msg, options...)
	if err == nil {
		return nil
	}
	if valErr, ok := errors.AsType[*protovalidate.ValidationError](err); ok {
		reason, domain := translateValidation(valErr, ErrorDomain)
		return &ValidationError{
			err:    valErr,
			reason: reason,
			domain: domain,
		}
	}
	return err
}

func storageMessageDescriptors() []protoreflect.MessageDescriptor {
	msgs := storagev1.File_storage_v1_storage_proto.Messages()
	descriptors := make([]protoreflect.MessageDescriptor, msgs.Len())
	for i := range msgs.Len() {
		descriptors[i] = msgs.Get(i)
	}
	return descriptors
}

func NewStorageValidator() (protovalidate.Validator, error) {
	inner, err := protovalidate.New(
		protovalidate.WithMessageDescriptors(storageMessageDescriptors()...),
	)
	if err != nil {
		return nil, err
	}
	return &storageValidator{inner: inner}, nil
}

func ErrnoReason(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if storageValErr, ok := errors.AsType[*ValidationError](err); ok {
		return storageValErr.Reason(), true
	}
	if protoValErr, ok := errors.AsType[*protovalidate.ValidationError](err); ok {
		return translateValidationReason(protoValErr), true
	}
	return "", false
}

func buildInvalidArgumentStatus(msg, reason, domain string) *status.Status {
	if domain == "" {
		domain = ErrorDomain
	}
	st := status.New(codes.InvalidArgument, msg)
	stWithDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: domain,
	})
	if detailErr == nil {
		return stWithDetails
	}
	return st
}

func translateValidationReason(valErr *protovalidate.ValidationError) string {
	reason, _ := translateValidation(valErr, ErrorDomain)
	return reason
}

func translateValidation(valErr *protovalidate.ValidationError, defaultDomain string) (reason, domain string) {
	bestPriority := 100
	bestReason := ReasonInvalidArgument
	bestDomain := defaultDomain
	if bestDomain == "" {
		bestDomain = ErrorDomain
	}

	for _, v := range valErr.Violations {
		priority, vReason, vDomain := classifyViolation(v)
		if priority < bestPriority {
			bestPriority = priority
			bestReason = vReason
			if vDomain != "" {
				bestDomain = vDomain
			}
		}
	}

	return bestReason, bestDomain
}

func classifyViolation(v *protovalidate.Violation) (priority int, reason, domain string) {
	priority, reason = classifyViolationInternal(v)
	fullName := ""
	if v != nil && v.FieldDescriptor != nil {
		fullName = string(v.FieldDescriptor.FullName())
	}

	domain = ""
	if strings.HasPrefix(fullName, "storage.v1.") {
		domain = ErrorDomain
	}
	return priority, reason, domain
}

func classifyViolationInternal(v *protovalidate.Violation) (priority int, reason string) {
	if v == nil || v.Proto == nil {
		return 60, ReasonInvalidArgument
	}

	ruleID := v.Proto.GetRuleId()
	isMaxLen := ruleID == "bytes.max_len" || strings.HasSuffix(ruleID, ".bytes.max_len") ||
		ruleID == "string.max_bytes" || strings.HasSuffix(ruleID, ".string.max_bytes")

	fullName := ""
	if v.FieldDescriptor != nil {
		fullName = string(v.FieldDescriptor.FullName())
	} else if elems := v.Proto.GetField().GetElements(); len(elems) > 0 {
		lastField := elems[len(elems)-1].GetFieldName()
		if lastField == "names" {
			fullName = "storage.v1.ListXattrResponse.names"
		}
	}

	switch fullName {
	case "storage.v1.SetXattrRequest.name",
		"storage.v1.GetXattrRequest.name",
		"storage.v1.RemoveXattrRequest.name",
		"storage.v1.ListXattrResponse.names":
		if isMaxLen {
			return 20, ReasonRange
		}
		return 30, ReasonInvalidArgument

	case "storage.v1.LookupRequest.name",
		"storage.v1.MknodRequest.name",
		"storage.v1.MkdirRequest.name",
		"storage.v1.SymlinkRequest.name",
		"storage.v1.LinkRequest.new_name",
		"storage.v1.UnlinkRequest.name",
		"storage.v1.RmdirRequest.name",
		"storage.v1.RenameRequest.old_name",
		"storage.v1.CreateRequest.name",
		"storage.v1.DirEntry.name",
		"storage.v1.DirEntryPlus.name",
		"storage.v1.Disk.name",
		"storage.v1.CreateDiskRequest.name",
		"storage.v1.UpdateDiskRequest.name":
		if isMaxLen {
			return 20, ReasonNameTooLong
		}
		return 30, ReasonInvalidArgument

	case "storage.v1.RenameRequest.new_name":
		if isMaxLen {
			return 40, ReasonNameTooLong
		}
		return 50, ReasonInvalidArgument

	case "storage.v1.SymlinkRequest.target",
		"storage.v1.ReadlinkResponse.target":
		if isMaxLen {
			return 50, ReasonNameTooLong
		}
		return 40, ReasonInvalidArgument

	case "storage.v1.SetXattrRequest.value",
		"storage.v1.GetXattrResponse.value":
		if isMaxLen {
			return 40, ReasonRange
		}
		return 45, ReasonInvalidArgument
	}

	return 10, ReasonInvalidArgument
}

func NewValidationInterceptor(v protovalidate.Validator) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req != nil {
				if msg, ok := req.Any().(proto.Message); ok {
					if err := v.Validate(msg); err != nil {
						return nil, statusToConnectError(ToStatus(err))
					}
				}
			}
			return next(ctx, req)
		}
	}
}
