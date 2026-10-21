package contract_test

import (
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestLookupRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		parent uint64
		entry  []byte
		valid  bool
	}{
		{"ordinary", 1, []byte("file.txt"), true},
		{"non-utf8 bytes", 1, []byte{0xff, 0xfe, 0x80, 0x81}, true},
		{"255 bytes", 1, []byte(strings.Repeat("a", 255)), true},
		{"multibyte 255 bytes", 1, []byte(strings.Repeat("界", 85)), true},
		{"newline allowed", 1, []byte("a\nb"), true},
		{"empty allowed to reach domain layer", 1, []byte(""), true},
		{"zero parent", 0, []byte("file"), false},
		{"256 bytes", 1, []byte(strings.Repeat("a", 256)), false},
		{"multibyte too long", 1, []byte(strings.Repeat("界", 86)), false},
		{"slash", 1, []byte("a/b"), false},
		{"embedded NUL", 1, []byte("a\x00b"), false},
		{"NUL only", 1, []byte("\x00"), false},
		{"leading NUL", 1, []byte("\x00file"), false},
		{"trailing NUL", 1, []byte("file\x00"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(&storagev1.LookupRequest{ParentIno: tc.parent, Name: tc.entry})
			if (err == nil) != tc.valid {
				t.Fatalf("valid = %v, validation error = %v", tc.valid, err)
			}
		})
	}
}
