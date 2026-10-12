package utils

import (
	"reflect"
	"testing"
)

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	t.Helper()
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("值不匹配: %v != %v", a, b)
	}
}

func TestBuffer(t *testing.T) {
	b := NewBuffer(20)
	b.Put8(1)
	b.Put16(2)
	b.Put32(3)
	b.Put64(4)
	b.Put([]byte("hello"))
	assertEqual(t, b.Len(), 20)

	r := ReadBuffer(b.Bytes())
	assertEqual(t, r.Get8(), uint8(1))
	assertEqual(t, r.Get16(), uint16(2))
	assertEqual(t, r.Get32(), uint32(3))
	assertEqual(t, r.Get64(), uint64(4))
	assertEqual(t, r.HasMore(), true)
	assertEqual(t, r.Left(), 5)
	if len(r.Buffer()) != 5 {
		t.Fatal("剩余缓冲应当为 5 字节")
	}
	assertEqual(t, string(r.Get(5)), "hello")
	r.Seek(10)
	assertEqual(t, r.Left(), 10)
}

func TestSetBytes(t *testing.T) {
	var w Buffer
	w.SetBytes(make([]byte, 3))
	w.Put8(1)
	w.Put16(2)
	r := ReadBuffer(w.Bytes())
	assertEqual(t, r.Get8(), uint8(1))
	assertEqual(t, r.Get16(), uint16(2))
}

func TestNativeBuffer(t *testing.T) {
	b := NewNativeBuffer(make([]byte, 20))
	b.Put8(1)
	b.Put16(2)
	b.Put32(3)
	b.Put64(4)
	b.Put([]byte("hello"))

	r := NewNativeBuffer(b.Bytes())
	assertEqual(t, r.Get8(), uint8(1))
	assertEqual(t, r.Get16(), uint16(2))
	assertEqual(t, r.Get32(), uint32(3))
	assertEqual(t, r.Get64(), uint64(4))
	assertEqual(t, string(r.Get(5)), "hello")
}
