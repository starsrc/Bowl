package chunk

import (
	"errors"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"

	"github.com/starsrc/bowl/services/storage/internal/juicefs/utils"
)

var pageStack = os.Getenv("JFS_PAGE_STACK") != ""

// Page 带有引用计数管理的内存分页
type Page struct {
	refs    int32
	offheap bool
	dep     *Page
	Data    []byte
	stack   []byte
}

// NewPage 根据给定字节切片创建引用计数为 1 的 Page
func NewPage(data []byte) *Page {
	return &Page{refs: 1, Data: data}
}

// NewOffPage 从内存池中分配指定大小的高速缓存页
func NewOffPage(size int) *Page {
	if size <= 0 {
		panic("Page 大小必须大于 0")
	}
	p := utils.Alloc(size)
	page := &Page{refs: 1, offheap: true, Data: p}
	if pageStack {
		page.stack = debug.Stack()
	}
	runtime.SetFinalizer(page, func(p *Page) {
		refcnt := atomic.LoadInt32(&p.refs)
		if refcnt != 0 {
			logger.Errorf("Page %p (%d 字节) 终结时引用计数不为 0: %d, 创建堆栈: %s", p, cap(p.Data), refcnt, string(p.stack))
			if refcnt > 0 {
				p.Release()
			}
		}
	})
	return page
}

// Slice 从现有 Page 中切取子切片并建立依赖引用
func (p *Page) Slice(off, len int) *Page {
	p.Acquire()
	np := NewPage(p.Data[off : off+len])
	np.dep = p
	return np
}

// Acquire 递增 Page 引用计数
func (p *Page) Acquire() {
	if pageStack {
		p.stack = append(p.stack, debug.Stack()...)
	}
	atomic.AddInt32(&p.refs, 1)
}

// Release 递减 Page 引用计数，并在归零时归还底层内存池
func (p *Page) Release() {
	if pageStack {
		p.stack = append(p.stack, debug.Stack()...)
	}
	if atomic.AddInt32(&p.refs, -1) == 0 {
		if p.offheap {
			utils.Free(p.Data)
		}
		if p.dep != nil {
			p.dep.Release()
			p.dep = nil
		}
		p.Data = nil
	}
}

type pageReader struct {
	p   *Page
	off int
}

// NewPageReader 构造 Page 读取流包装器
func NewPageReader(p *Page) *pageReader {
	p.Acquire()
	return &pageReader{p, 0}
}

func (r *pageReader) Read(buf []byte) (int, error) {
	n, err := r.ReadAt(buf, int64(r.off))
	r.off += n
	return n, err
}

func (r *pageReader) ReadAt(buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if r.p == nil {
		return 0, errors.New("Page 已经提前释放")
	}
	if int(off) == len(r.p.Data) {
		return 0, io.EOF
	}
	n := copy(buf, r.p.Data[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (r *pageReader) Close() error {
	if r.p != nil {
		r.p.Release()
		r.p = nil
	}
	return nil
}
