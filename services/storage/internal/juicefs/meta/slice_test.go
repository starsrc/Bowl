package meta

import (
	"reflect"
	"testing"
)

func TestSliceMarshalAndRead(t *testing.T) {
	raw := MarshalSlice(100, 1001, 4096, 0, 4096)
	if len(raw) != SliceBytes {
		t.Fatalf("预期切片字节大小为 %d，实际为 %d", SliceBytes, len(raw))
	}

	ss := ReadSliceBuf(raw)
	if len(ss) != 1 {
		t.Fatalf("反序列化切片数量应为 1，实际为 %d", len(ss))
	}
	s := ss[0]
	if s.pos != 100 || s.id != 1001 || s.size != 4096 || s.off != 0 || s.len != 4096 {
		t.Fatalf("切片字段解析不符: %+v", s)
	}
}

func TestBuildSliceOverlay(t *testing.T) {
	// 场景：初始写入 0~4096，后续在 2048~3072 进行覆盖覆写
	s1Raw := MarshalSlice(0, 1, 4096, 0, 4096)
	s2Raw := MarshalSlice(2048, 2, 1024, 0, 1024)

	buf := append(s1Raw, s2Raw...)
	ss := ReadSliceBuf(buf)

	reconstituted := BuildSlice(ss)
	expected := []Slice{
		{Id: 1, Size: 4096, Off: 0, Len: 2048},
		{Id: 2, Size: 1024, Off: 0, Len: 1024},
		{Id: 1, Size: 4096, Off: 3072, Len: 1024},
	}

	if !reflect.DeepEqual(reconstituted, expected) {
		t.Fatalf("切片重构覆盖图层不匹配:\n期望: %+v\n实际: %+v", expected, reconstituted)
	}
}

func TestBuildSliceHole(t *testing.T) {
	// 场景：存在稀疏空洞
	s1Raw := MarshalSlice(100, 1, 100, 0, 100)
	ss := ReadSliceBuf(s1Raw)

	reconstituted := BuildSlice(ss)
	expected := []Slice{
		{Id: 0, Size: 100, Off: 0, Len: 100}, // 0~100 的空洞
		{Id: 1, Size: 100, Off: 0, Len: 100}, // 100~200 的切片
	}
	if !reflect.DeepEqual(reconstituted, expected) {
		t.Fatalf("稀疏空洞重组不匹配:\n期望: %+v\n实际: %+v", expected, reconstituted)
	}
}

func TestCompactChunk(t *testing.T) {
	s1Raw := MarshalSlice(0, 1, 1024, 0, 1024)
	s2Raw := MarshalSlice(1024, 2, 1024, 0, 1024)
	buf := append(s1Raw, s2Raw...)
	ss := ReadSliceBuf(buf)

	pos, size, slices := CompactChunk(ss)
	if pos != 0 || size != 2048 || len(slices) != 2 {
		t.Fatalf("CompactChunk 结果不符: pos=%d, size=%d, slices=%+v", pos, size, slices)
	}
}
