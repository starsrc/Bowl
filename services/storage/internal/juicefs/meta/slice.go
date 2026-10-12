package meta

import (
	"github.com/starsrc/bowl/services/storage/internal/juicefs/utils"
)

const (
	// SliceBytes 切片紧凑二进制编码的标准字节长度（24 字节大端序）
	SliceBytes = 24
	// ChunkBits Chunk 大小的幂次数（26 位对应 64MB）
	ChunkBits = 26
	// ChunkSize 单个 Chunk 的最大逻辑长度（64 MiB）
	ChunkSize = 1 << ChunkBits
	// BlockSize 单个物理块的标准大小（4 MiB）
	BlockSize = 4 << 20
)

var logger = utils.GetLogger("juicefs")

// Slice 对应 Chunk 内部连续区间的切片载荷视图
type Slice struct {
	Id   uint64 // 切片全局唯一递增编号（为 0 时表示稀疏空洞）
	Size uint32 // 切片实际在对象存储中的物理尺寸
	Off  uint32 // 切片在所属物理对象中的起始有效偏移
	Len  uint32 // 该区间在当前视图下的有效字节长度
}

// slice 内部树形节点，用于多切片写入时的覆盖重叠计算
type slice struct {
	id    uint64
	size  uint32
	off   uint32
	len   uint32
	pos   uint32
	left  *slice
	right *slice
}

// NewSlice 构造内部切片树节点
func NewSlice(pos uint32, id uint64, cleng, off, len uint32) *slice {
	if len == 0 {
		return nil
	}
	s := &slice{}
	s.pos = pos
	s.id = id
	s.size = cleng
	s.off = off
	s.len = len
	s.left = nil
	s.right = nil
	return s
}

func (s *slice) read(buf []byte) {
	rb := utils.ReadBuffer(buf)
	s.pos = rb.Get32()
	s.id = rb.Get64()
	s.size = rb.Get32()
	s.off = rb.Get32()
	s.len = rb.Get32()
}

func (s *slice) cut(pos uint32) (left, right *slice) {
	if s == nil {
		return nil, nil
	}
	if pos <= s.pos {
		if s.left == nil {
			s.left = NewSlice(pos, 0, 0, 0, s.pos-pos)
		}
		left, s.left = s.left.cut(pos)
		return left, s
	} else if pos < s.pos+s.len {
		l := pos - s.pos
		right = NewSlice(pos, s.id, s.size, s.off+l, s.len-l)
		right.right = s.right
		s.len = l
		s.right = nil
		return s, right
	} else {
		if s.right == nil {
			s.right = NewSlice(s.pos+s.len, 0, 0, 0, pos-s.pos-s.len)
		}
		s.right, right = s.right.cut(pos)
		return s, right
	}
}

func (s *slice) visit(f func(*slice)) {
	if s == nil {
		return
	}
	s.left.visit(f)
	right := s.right
	f(s)
	right.visit(f)
}

// MarshalSlice 序列化单切片为 24 字节大端序二进制切片
func MarshalSlice(pos uint32, id uint64, size, off, len uint32) []byte {
	w := utils.NewBuffer(SliceBytes)
	w.Put32(pos)
	w.Put64(id)
	w.Put32(size)
	w.Put32(off)
	w.Put32(len)
	return w.Bytes()
}

// ReadSlices 从多个字符串切片中反序列化内部 slice 节点数组
func ReadSlices(vals []string) []*slice {
	slices := make([]slice, len(vals))
	ss := make([]*slice, len(vals))
	for i, val := range vals {
		if len(val) != SliceBytes {
			logger.Errorf("无效的切片字节长度: len=%d", len(val))
			return nil
		}
		s := &slices[i]
		s.read([]byte(val))
		ss[i] = s
	}
	return ss
}

// ReadSliceBuf 从连续字节流反序列化内部切片列表
func ReadSliceBuf(buf []byte) []*slice {
	if len(buf)%SliceBytes != 0 {
		logger.Errorf("无效的切片缓冲区总长度: len=%d", len(buf))
		return nil
	}
	nSlices := len(buf) / SliceBytes
	slices := make([]slice, nSlices)
	ss := make([]*slice, nSlices)
	for i := 0; i < len(buf); i += SliceBytes {
		s := &slices[i/SliceBytes]
		s.read(buf[i:])
		ss[i/SliceBytes] = s
	}
	return ss
}

// BuildSlice 依据时序图层将多重切片重组裁切为互不重叠的顺序逻辑切片
func BuildSlice(ss []*slice) []Slice {
	var root *slice
	for i := range ss {
		s := new(slice)
		*s = *ss[i]
		var right *slice
		s.left, right = root.cut(s.pos)
		_, s.right = right.cut(s.pos + s.len)
		root = s
	}
	var pos uint32
	var chunk []Slice
	root.visit(func(s *slice) {
		if s.pos > pos {
			chunk = append(chunk, Slice{Size: s.pos - pos, Len: s.pos - pos})
			pos = s.pos
		}
		chunk = append(chunk, Slice{Id: s.id, Size: s.size, Off: s.off, Len: s.len})
		pos += s.len
	})
	return chunk
}

// CompactChunk 压实 Chunk 内部切片，剔除头尾空洞并计算物理起始位置与总跨度
func CompactChunk(ss []*slice) (uint32, uint32, []Slice) {
	var chunk = BuildSlice(ss)
	var pos uint32
	n := len(chunk)
	for n > 1 {
		if chunk[0].Id == 0 {
			pos += chunk[0].Len
			chunk = chunk[1:]
			n--
		} else if chunk[n-1].Id == 0 {
			chunk = chunk[:n-1]
			n--
		} else {
			break
		}
	}
	if n == 1 && chunk[0].Id == 0 {
		chunk[0].Len = 1
	}
	var size uint32
	for _, c := range chunk {
		size += c.Len
	}
	return pos, size, chunk
}
