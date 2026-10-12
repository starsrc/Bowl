package compress

import (
	"testing"
)

// 测试各压缩算法的基本压缩与解压缩行为
func testCompress(t *testing.T, c Compressor) {
	src := []byte(c.Name())
	testIt := func(src []byte) {
		if len(src) > 1 {
			_, err := c.Compress(make([]byte, 1), src)
			if err == nil {
				t.Fatal("期望短缓冲错误，但返回了 nil")
			}
		}
		dst := make([]byte, c.CompressBound(len(src)))
		n, err := c.Compress(dst, src)
		if err != nil {
			t.Fatalf("压缩失败: %s", err)
		}
		if len(src) > 1 {
			_, err = c.Decompress(make([]byte, 1), dst[:n])
			if err == nil {
				t.Fatalf("期望短缓冲错误，但返回了 nil")
			}
		}
		src2 := make([]byte, len(src))
		n, err = c.Decompress(src2, dst[:n])
		if err != nil {
			t.Fatalf("解压缩失败: %s", err)
		}
		if string(src2[:n]) != string(src) {
			t.Fatalf("期望 %s 但实际得到 %s", string(src), string(src2))
		}
	}

	testIt(src)
	testIt(nil)

	if c.CompressBound(0) > 0 {
		n, err := c.Decompress(make([]byte, 100), src[:0])
		if err == nil || n > 0 {
			t.Fatalf("空输入解压应当失败，但返回了 %d", n)
		}
	}
}

func TestUncompressed(t *testing.T) {
	testCompress(t, NewCompressor("none"))
}

func TestZstd(t *testing.T) {
	testCompress(t, NewCompressor("zstd"))
}

func TestLZ4(t *testing.T) {
	testCompress(t, NewCompressor("lz4"))
}

func TestInvalidCompressor(t *testing.T) {
	if c := NewCompressor("unknown"); c != nil {
		t.Fatalf("预期未知算法返回 nil，实际返回: %v", c)
	}
}
