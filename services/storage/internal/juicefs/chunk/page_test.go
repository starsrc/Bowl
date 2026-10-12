package chunk

import (
	"io"
	"testing"
)

func TestPage(t *testing.T) {
	p1 := NewOffPage(1)
	if len(p1.Data) != 1 {
		t.Fail()
	}
	if cap(p1.Data) != 1 {
		t.Fail()
	}
	p1.Acquire()
	p1.Release()
	if p1.Data == nil {
		t.Fail()
	}

	p2 := p1.Slice(0, 1)
	p1.Release()
	if p1.Data == nil {
		t.Fail()
	}

	p2.Release()
	if p2.Data != nil {
		t.Fail()
	}
	if p1.Data != nil {
		t.Fail()
	}
}

func TestPageReader(t *testing.T) {
	data := []byte("hello")
	p := NewPage(data)
	r := NewPageReader(p)

	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Fatalf("空读取应当返回 0")
	}
	buf := make([]byte, 3)
	if n, err := r.Read(buf); n != 3 || err != nil {
		t.Fatalf("应当读取 3 字节，实际为 %d", n)
	}
	if n, err := r.Read(buf); n != 2 || (err != nil && err != io.EOF) {
		t.Fatalf("应当读取 2 字节，实际为 %d", n)
	}
	if n, err := r.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("应当读取 0 字节并返回 EOF")
	}
	if n, err := r.ReadAt(buf, 4); n != 1 || (err != nil && err != io.EOF) {
		t.Fatalf("ReadAt 偏移 4 应当读取 1 字节")
	}
	if n, err := r.ReadAt(buf, 5); n != 0 || err != io.EOF {
		t.Fatalf("ReadAt 偏移 5 应当返回 EOF")
	}
	_ = r.Close()
	if n, err := r.ReadAt(buf, 5); n != 0 || err == nil {
		t.Fatalf("关闭后读取应当失败")
	}
}
