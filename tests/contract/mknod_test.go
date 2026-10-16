package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestMknodRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name      string
		parentIno uint64
		entryName string
		mode      uint32
		uid       uint32
		gid       uint32
		rdev      uint32
		valid     bool
	}{
		// 正向边界用例
		{
			name:      "ordinary regular file",
			parentIno: 1,
			entryName: "file.txt",
			mode:      0o100644,
			uid:       1000,
			gid:       1000,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "name exactly 255 bytes",
			parentIno: 1,
			entryName: strings.Repeat("a", 255),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "multibyte name exactly 255 bytes",
			parentIno: 1,
			entryName: strings.Repeat("界", 85),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "newline allowed in name",
			parentIno: 1,
			entryName: "a\nb",
			mode:      0o100644,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "parent_ino upper boundary math.MaxInt64",
			parentIno: math.MaxInt64,
			entryName: "file.txt",
			mode:      0o100644,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "mode lower boundary 1",
			parentIno: 1,
			entryName: "file.txt",
			mode:      1,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "mode upper boundary 0o177777 (65535)",
			parentIno: 1,
			entryName: "file.txt",
			mode:      65535,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "device file with rdev",
			parentIno: 1,
			entryName: "null",
			mode:      0o020666,
			uid:       0,
			gid:       0,
			rdev:      12345,
			valid:     true,
		},

		// 负向边界用例 - parent_ino
		{
			name:      "zero parent_ino invalid",
			parentIno: 0,
			entryName: "file.txt",
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "parent_ino exceeding math.MaxInt64 invalid",
			parentIno: uint64(math.MaxInt64) + 1,
			entryName: "file.txt",
			mode:      0o100644,
			valid:     false,
		},

		// 负向边界用例 - name
		{
			name:      "empty name invalid",
			parentIno: 1,
			entryName: "",
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "name 256 bytes invalid",
			parentIno: 1,
			entryName: strings.Repeat("a", 256),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "multibyte name too long invalid",
			parentIno: 1,
			entryName: strings.Repeat("界", 86),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "slash in name invalid",
			parentIno: 1,
			entryName: "a/b",
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "leading slash in name invalid",
			parentIno: 1,
			entryName: "/file",
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "embedded NUL in name invalid",
			parentIno: 1,
			entryName: "a\x00b",
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "NUL only in name invalid",
			parentIno: 1,
			entryName: "\x00",
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "leading NUL in name invalid",
			parentIno: 1,
			entryName: "\x00file",
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "trailing NUL in name invalid",
			parentIno: 1,
			entryName: "file\x00",
			mode:      0o100644,
			valid:     false,
		},

		// 负向边界用例 - mode
		{
			name:      "zero mode invalid",
			parentIno: 1,
			entryName: "file.txt",
			mode:      0,
			valid:     false,
		},
		{
			name:      "mode exceeding 65535 invalid",
			parentIno: 1,
			entryName: "file.txt",
			mode:      65536,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.MknodRequest{
				ParentIno: tc.parentIno,
				Name:      tc.entryName,
				Mode:      tc.mode,
				Uid:       tc.uid,
				Gid:       tc.gid,
				Rdev:      tc.rdev,
			}
			err := validator.Validate(req)
			if (err == nil) != tc.valid {
				t.Fatalf("valid = %v, validation error = %v", tc.valid, err)
			}
		})
	}
}

func TestMknodResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}

	validInode := &storagev1.Inode{
		Ino:        100,
		Generation: 0,
		Attr: &storagev1.InodeAttribute{
			Mode:  0o100644,
			Nlink: 1,
		},
	}

	for _, tc := range []struct {
		name         string
		inode        *storagev1.Inode
		entryTimeout doubleEntryTimeout
		attrTimeout  doubleAttrTimeout
		valid        bool
	}{
		{
			name:         "valid response with positive timeouts",
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        true,
		},
		{
			name:         "valid response with zero timeouts",
			inode:        validInode,
			entryTimeout: 0.0,
			attrTimeout:  0.0,
			valid:        true,
		},
		{
			name:         "nil inode invalid",
			inode:        nil,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "negative entry timeout invalid",
			inode:        validInode,
			entryTimeout: -0.1,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "negative attr timeout invalid",
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  -0.1,
			valid:        false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.MknodResponse{
				Inode:               tc.inode,
				EntryTimeoutSeconds: float64(tc.entryTimeout),
				AttrTimeoutSeconds:  float64(tc.attrTimeout),
			}
			err := validator.Validate(resp)
			if (err == nil) != tc.valid {
				t.Fatalf("valid = %v, validation error = %v", tc.valid, err)
			}
		})
	}
}

type (
	doubleEntryTimeout = float64
	doubleAttrTimeout  = float64
)
