package contract_test

import (
	"strings"
	"testing"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

func TestNewStorageValidator(t *testing.T) {
	v, err := validator.NewStorageValidator()
	if err != nil {
		t.Fatalf("NewStorageValidator() returned error: %v", err)
	}
	if v == nil {
		t.Fatal("NewStorageValidator() returned nil validator")
	}

	t.Run("有效请求校验通过", func(t *testing.T) {
		validLookup := &storagev1.LookupRequest{
			ParentIno: 1,
			Name:      "test.txt",
		}
		if err := v.Validate(validLookup); err != nil {
			t.Errorf("expected valid LookupRequest, got error: %v", err)
		}

		validMkdir := &storagev1.MkdirRequest{
			ParentIno: 1,
			Name:      "subdir",
			Mode:      0o755,
			Uid:       1000,
			Gid:       1000,
		}
		if err := v.Validate(validMkdir); err != nil {
			t.Errorf("expected valid MkdirRequest, got error: %v", err)
		}
	})

	t.Run("非法请求校验失败", func(t *testing.T) {
		invalidLookupZeroParent := &storagev1.LookupRequest{
			ParentIno: 0,
			Name:      "test.txt",
		}
		if err := v.Validate(invalidLookupZeroParent); err == nil {
			t.Error("expected validation error for zero parent_ino, got nil")
		}

		invalidLookupNameTooLong := &storagev1.LookupRequest{
			ParentIno: 1,
			Name:      strings.Repeat("a", 256),
		}
		if err := v.Validate(invalidLookupNameTooLong); err == nil {
			t.Error("expected validation error for name exceeding 255 bytes, got nil")
		}

		invalidLookupEmbeddedNul := &storagev1.LookupRequest{
			ParentIno: 1,
			Name:      "foo\x00bar",
		}
		if err := v.Validate(invalidLookupEmbeddedNul); err == nil {
			t.Error("expected validation error for embedded NUL in name, got nil")
		}
	})
}
