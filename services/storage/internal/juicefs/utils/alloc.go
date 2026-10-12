package utils

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
)

var used int64

// Alloc 从内存池中分配 size 字节的切片，并记录已分配内存量
func Alloc(size int) []byte {
	b := Alloc0(size)
	atomic.AddInt64(&used, int64(cap(b)))
	return b
}

// Alloc0 从 2 的幂次对象池中获取底层切片
func Alloc0(size int) []byte {
	zeros := PowerOf2(size)
	b := *pools[zeros].Get().(*[]byte)
	if cap(b) < size {
		panic(fmt.Sprintf("%d < %d", cap(b), size))
	}
	return b[:size]
}

// Free 将切片归还内存池并递减已分配内存计数
func Free(b []byte) {
	atomic.AddInt64(&used, -int64(cap(b)))
	Free0(b)
}

// Free0 将切片放回对应的 2 的幂次内存池
func Free0(b []byte) {
	pools[PowerOf2(cap(b))].Put(&b)
}

// AllocMemory 返回当前已分配内存字节数
func AllocMemory() int64 {
	return atomic.LoadInt64(&used)
}

var pools []*sync.Pool

// PowerOf2 返回大于等于 s 的最小 2 的幂次指数（例如 10 -> 4，因 2^4 = 16 >= 10）
func PowerOf2(s int) int {
	if s <= 0 {
		return 0
	}
	return bits.Len(uint(s - 1))
}

func init() {
	pools = make([]*sync.Pool, 34) // 支持 1 字节至 8GB
	for i := 0; i < 34; i++ {
		func(bit int) {
			pools[i] = &sync.Pool{
				New: func() interface{} {
					b := make([]byte, 1<<bit)
					return &b
				},
			}
		}(i)
	}
}
