package meta

import (
	"encoding/binary"
	"fmt"
)

// 切片编解码领域哨兵错误。
var (
	ErrInvalidSliceBytes = fmt.Errorf("%w: invalid slice bytes", ErrInvalidArgument)
	ErrInvalidSliceData  = fmt.Errorf("%w: invalid slice data", ErrInvalidArgument)
)

// Slice 表示 64 MiB Chunk 内的一个 24 字节大端序二进制切片描述符。
type Slice struct {
	Pos  uint32 // 在 64 MiB Chunk 内的起始逻辑偏移 (0 <= Pos < ChunkSize)
	ID   uint64 // 物理切片唯一编号 (0 表示稀疏空洞 Hole)
	Size uint32 // 该物理切片的总大小 (ID > 0 时 Size > 0)
	Off  uint32 // 在该物理切片内的起始偏移
	Len  uint32 // 当前引用的有效长度 (Len > 0)
}

// Validate 校验单个切片的字段合法性。
func (s Slice) Validate() error {
	if s.Len == 0 {
		return fmt.Errorf("%w: slice len must be greater than 0", ErrInvalidSliceData)
	}
	if uint64(s.Pos)+uint64(s.Len) > ChunkSize {
		return fmt.Errorf("%w: slice range [%d, %d) exceeds chunk size %d", ErrInvalidSliceData, s.Pos, uint64(s.Pos)+uint64(s.Len), ChunkSize)
	}
	if s.ID > 0 {
		if s.Size == 0 {
			return fmt.Errorf("%w: physical slice (ID=%d) size must be greater than 0", ErrInvalidSliceData, s.ID)
		}
		if uint64(s.Off)+uint64(s.Len) > uint64(s.Size) {
			return fmt.Errorf("%w: slice range in physical block [%d, %d) exceeds slice size %d", ErrInvalidSliceData, s.Off, uint64(s.Off)+uint64(s.Len), s.Size)
		}
	}
	return nil
}

// EncodeSlice 将单个切片描述符编码为 24 字节大端序字节切片。
func EncodeSlice(s Slice) []byte {
	buf := make([]byte, SliceBytes)
	binary.BigEndian.PutUint32(buf[0:4], s.Pos)
	binary.BigEndian.PutUint64(buf[4:12], s.ID)
	binary.BigEndian.PutUint32(buf[12:16], s.Size)
	binary.BigEndian.PutUint32(buf[16:20], s.Off)
	binary.BigEndian.PutUint32(buf[20:24], s.Len)
	return buf
}

// AppendEncodedSlice 将单个切片描述符以 24 字节大端序追加到 dst 中。
func AppendEncodedSlice(dst []byte, s Slice) []byte {
	var buf [SliceBytes]byte
	binary.BigEndian.PutUint32(buf[0:4], s.Pos)
	binary.BigEndian.PutUint64(buf[4:12], s.ID)
	binary.BigEndian.PutUint32(buf[12:16], s.Size)
	binary.BigEndian.PutUint32(buf[16:20], s.Off)
	binary.BigEndian.PutUint32(buf[20:24], s.Len)
	return append(dst, buf[:]...)
}

// EncodeSlices 将多个切片描述符连续编码为 24 字节大端序字节流。
func EncodeSlices(slices []Slice) []byte {
	if len(slices) == 0 {
		return nil
	}
	buf := make([]byte, 0, len(slices)*SliceBytes)
	for _, s := range slices {
		buf = AppendEncodedSlice(buf, s)
	}
	return buf
}

// DecodeSlice 从 24 字节大端序缓冲区解码并校验切片描述符。
func DecodeSlice(buf []byte) (Slice, error) {
	if len(buf) != SliceBytes {
		return Slice{}, fmt.Errorf("%w: slice buffer length must be %d, got %d", ErrInvalidSliceBytes, SliceBytes, len(buf))
	}
	s := Slice{
		Pos:  binary.BigEndian.Uint32(buf[0:4]),
		ID:   binary.BigEndian.Uint64(buf[4:12]),
		Size: binary.BigEndian.Uint32(buf[12:16]),
		Off:  binary.BigEndian.Uint32(buf[16:20]),
		Len:  binary.BigEndian.Uint32(buf[20:24]),
	}
	if err := s.Validate(); err != nil {
		return Slice{}, err
	}
	return s, nil
}

// DecodeSlices 从 24 字节整数倍大端序缓冲区解码并校验多个切片描述符。
func DecodeSlices(buf []byte) ([]Slice, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf)%SliceBytes != 0 {
		return nil, fmt.Errorf("%w: slices buffer length %d is not a multiple of %d", ErrInvalidSliceBytes, len(buf), SliceBytes)
	}
	n := len(buf) / SliceBytes
	slices := make([]Slice, n)
	for i := 0; i < n; i++ {
		s, err := DecodeSlice(buf[i*SliceBytes : (i+1)*SliceBytes])
		if err != nil {
			return nil, err
		}
		slices[i] = s
	}
	return slices, nil
}

type sliceNode struct {
	pos   uint32
	id    uint64
	size  uint32
	off   uint32
	len   uint32
	left  *sliceNode
	right *sliceNode
}

func newSliceNode(pos uint32, id uint64, size, off, length uint32) *sliceNode {
	if length == 0 {
		return nil
	}
	if id == 0 {
		size = 0
		off = 0
	}
	return &sliceNode{
		pos:  pos,
		id:   id,
		size: size,
		off:  off,
		len:  length,
	}
}

func (n *sliceNode) cut(pos uint32) (left, right *sliceNode) {
	if n == nil {
		return nil, nil
	}
	if pos <= n.pos {
		if n.left == nil {
			n.left = newSliceNode(pos, 0, 0, 0, n.pos-pos)
		}
		left, n.left = n.left.cut(pos)
		return left, n
	} else if pos < n.pos+n.len {
		l := pos - n.pos
		var off uint32
		var size uint32
		if n.id > 0 {
			off = n.off + l
			size = n.size
		}
		right = newSliceNode(pos, n.id, size, off, n.len-l)
		right.right = n.right
		n.len = l
		n.right = nil
		return n, right
	} else {
		if n.right == nil {
			n.right = newSliceNode(n.pos+n.len, 0, 0, 0, pos-n.pos-n.len)
		}
		n.right, right = n.right.cut(pos)
		return n, right
	}
}

func (n *sliceNode) visit(f func(*sliceNode)) {
	if n == nil {
		return
	}
	n.left.visit(f)
	right := n.right
	f(n)
	right.visit(f)
}

func appendHoleOrExtend(res []Slice, pos uint32, length uint32) []Slice {
	if length == 0 {
		return res
	}
	if len(res) > 0 {
		last := &res[len(res)-1]
		if last.ID == 0 && last.Pos+last.Len == pos {
			last.Len += length
			return res
		}
	}
	return append(res, Slice{
		Pos:  pos,
		ID:   0,
		Size: 0,
		Off:  0,
		Len:  length,
	})
}

// BuildSlice 依据一维区间二叉裁剪算法合并切片序列。
// 1. 后追加的切片覆盖先追加的历史切片区间；
// 2. 当新切片落入旧切片中间时，旧切片自动分裂为左右两段，右段的 Pos 与 Off 精确递增对应偏移量；
// 3. 中序遍历输出单调升序切片序列，对首部间隙或相邻切片之间的未写入逻辑间隙自动合成 ID=0 的空洞切片；
// 4. 最新覆盖的空洞切片（ID=0，如打洞追加）同样作为裁剪树中的旧切片被后续切片裁剪覆盖。
func BuildSlice(slices []Slice) []Slice {
	if len(slices) == 0 {
		return nil
	}
	var root *sliceNode
	for _, s := range slices {
		if s.Len == 0 {
			continue
		}
		node := newSliceNode(s.Pos, s.ID, s.Size, s.Off, s.Len)
		var right *sliceNode
		node.left, right = root.cut(node.pos)
		_, node.right = right.cut(node.pos + node.len)
		root = node
	}
	if root == nil {
		return nil
	}

	var pos uint32
	var result []Slice
	root.visit(func(n *sliceNode) {
		if n.pos > pos {
			result = appendHoleOrExtend(result, pos, n.pos-pos)
			pos = n.pos
		}
		if n.id == 0 {
			result = appendHoleOrExtend(result, n.pos, n.len)
		} else {
			result = append(result, Slice{
				Pos:  n.pos,
				ID:   n.id,
				Size: n.size,
				Off:  n.off,
				Len:  n.len,
			})
		}
		pos += n.len
	})
	return result
}

// CountSliceRefs 从一段或多段原始 slices 字节流中统计各有效 slice_id (ID > 0) 出现频次。
func CountSliceRefs(rawSlices ...[]byte) (map[uint64]int32, error) {
	counts := make(map[uint64]int32)
	for _, raw := range rawSlices {
		if len(raw) == 0 {
			continue
		}
		if len(raw)%SliceBytes != 0 {
			return nil, fmt.Errorf("%w: raw slice buffer length %d is not a multiple of %d", ErrInvalidSliceBytes, len(raw), SliceBytes)
		}
		n := len(raw) / SliceBytes
		for i := 0; i < n; i++ {
			s, err := DecodeSlice(raw[i*SliceBytes : (i+1)*SliceBytes])
			if err != nil {
				return nil, err
			}
			if s.ID > 0 {
				counts[s.ID]++
			}
		}
	}
	return counts, nil
}
