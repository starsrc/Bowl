package utils

import (
	"testing"
)

func TestAlloc(t *testing.T) {
	old := AllocMemory()
	b := Alloc(10)
	if AllocMemory()-old != 16 {
		t.Fatalf("预期分配 16 字节，实际为 %d", AllocMemory()-old)
	}
	Free(b)
	if AllocMemory()-old != 0 {
		t.Fatalf("释放后内存计数不为 0，实际为 %d", AllocMemory()-old)
	}
}

func TestPowerOf2(t *testing.T) {
	if p := PowerOf2(0); p != 0 {
		t.Fatalf("PowerOf2(0) 应当为 0，实际为 %d", p)
	}
	if p := PowerOf2(1); p != 0 {
		t.Fatalf("PowerOf2(1) 应当为 0，实际为 %d", p)
	}
	if p := PowerOf2(2); p != 1 {
		t.Fatalf("PowerOf2(2) 应当为 1，实际为 %d", p)
	}
	if p := PowerOf2(10); p != 4 {
		t.Fatalf("PowerOf2(10) 应当为 4，实际为 %d", p)
	}
	if p := PowerOf2(16); p != 4 {
		t.Fatalf("PowerOf2(16) 应当为 4，实际为 %d", p)
	}
}
