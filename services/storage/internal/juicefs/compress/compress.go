package compress

import (
	"fmt"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Compressor 定义统一的纯 Go 压缩驱动接口
type Compressor interface {
	Name() string
	CompressBound(int) int
	Compress(dst, src []byte) (int, error)
	Decompress(dst, src []byte) (int, error)
}

// NewCompressor 根据算法名称构造对应的纯 Go 压缩实现
func NewCompressor(algr string) Compressor {
	algr = strings.ToLower(algr)
	if algr == "zstd" {
		return ZStandard{}
	} else if algr == "lz4" {
		return LZ4{}
	} else if algr == "none" || algr == "" {
		return noOp{}
	}
	return nil
}

// noOp 提供直通无压缩实现
type noOp struct{}

func (n noOp) Name() string            { return "Noop" }
func (n noOp) CompressBound(l int) int { return l }

func (n noOp) Compress(dst, src []byte) (int, error) {
	if len(dst) < len(src) {
		return 0, fmt.Errorf("缓冲区过小: %d < %d", len(dst), len(src))
	}
	copy(dst, src)
	return len(src), nil
}

func (n noOp) Decompress(dst, src []byte) (int, error) {
	if len(dst) < len(src) {
		return 0, fmt.Errorf("缓冲区过小: %d < %d", len(dst), len(src))
	}
	copy(dst, src)
	return len(src), nil
}

var (
	zstdFastEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithZeroFrames(true))
	zstdDecoder, _     = zstd.NewReader(nil)
)

// ZStandard 基于 klauspost/compress/zstd 的纯 Go Zstd 实现
type ZStandard struct{}

func (z ZStandard) Name() string { return "Zstd" }

func (z ZStandard) CompressBound(l int) int {
	bound := l + (l >> 8)
	if l < (128 << 10) {
		bound += ((128 << 10) - l) >> 11
	}
	if bound < 64 {
		bound = 64
	}
	return bound
}

func (z ZStandard) Compress(dst, src []byte) (int, error) {
	bound := z.CompressBound(len(src))
	if cap(dst) < bound {
		return 0, fmt.Errorf("缓冲区过小: %d < %d", cap(dst), bound)
	}
	d := zstdFastEncoder.EncodeAll(src, dst[:0])
	if len(d) > cap(dst) {
		return 0, fmt.Errorf("缓冲区过小: %d < %d", cap(dst), len(d))
	}
	return len(d), nil
}

func (z ZStandard) Decompress(dst, src []byte) (int, error) {
	if len(src) == 0 {
		return 0, fmt.Errorf("解压空输入")
	}
	d, err := zstdDecoder.DecodeAll(src, dst[:0])
	if err != nil {
		return 0, err
	}
	if len(d) > 0 && len(dst) > 0 && &d[0] != &dst[0] {
		return 0, fmt.Errorf("缓冲区过小: %d < %d", len(dst), len(d))
	}
	if len(d) > len(dst) {
		return 0, fmt.Errorf("缓冲区过小: %d < %d", len(dst), len(d))
	}
	return len(d), nil
}

// LZ4 基于 pierrec/lz4/v4 的纯 Go LZ4 块压缩实现
type LZ4 struct{}

func (l LZ4) Name() string { return "LZ4" }

func (l LZ4) CompressBound(size int) int {
	return lz4.CompressBlockBound(size)
}

func (l LZ4) Compress(dst, src []byte) (int, error) {
	if len(src) == 0 {
		if len(dst) < 1 {
			return 0, fmt.Errorf("缓冲区过小: %d < 1", len(dst))
		}
		dst[0] = 0
		return 1, nil
	}
	bound := l.CompressBound(len(src))
	if len(dst) < bound {
		return 0, fmt.Errorf("缓冲区过小: %d < %d", len(dst), bound)
	}
	n, err := lz4.CompressBlock(src, dst, nil)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		// 未能压缩，以不可压缩块格式回退或处理
		if len(dst) < len(src) {
			return 0, fmt.Errorf("缓冲区过小: %d < %d", len(dst), len(src))
		}
		copy(dst, src)
		return len(src), nil
	}
	return n, nil
}

func (l LZ4) Decompress(dst, src []byte) (int, error) {
	if len(src) == 0 {
		return 0, fmt.Errorf("解压空输入")
	}
	if len(src) == 1 && src[0] == 0 {
		return 0, nil
	}
	if len(dst) == 0 {
		return 0, fmt.Errorf("缓冲区过小: 0")
	}
	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return 0, err
	}
	return n, nil
}
