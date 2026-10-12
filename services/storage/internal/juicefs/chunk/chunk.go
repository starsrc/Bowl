package chunk

import (
	"context"
	"io"

	"github.com/starsrc/bowl/services/storage/internal/juicefs/object"
)

// Reader 切片数据范围读取器接口
type Reader interface {
	ReadAt(ctx context.Context, p *Page, off int) (int, error)
}

// Writer 切片写入器接口
type Writer interface {
	io.WriterAt
	ID() uint64
	SetID(id uint64)
	SetWriteback(enabled bool)
	FlushTo(offset int) error
	Finish(length int) error
	Abort()
}

// ChunkStore 统一 Chunk 存储引擎接口
type ChunkStore interface {
	NewReader(id uint64, length int) Reader
	NewWriter(id uint64, tierID uint8) Writer
	Remove(id uint64, length int) error
	FillCache(id uint64, length uint32) error
	EvictCache(id uint64, length uint32) error
	CheckCache(id uint64, length uint32, handler func(exists bool, loc string, size int)) error
	UsedMemory() int64
	UpdateLimit(upload, download int64)
	BlobStorage() object.ObjectStorage
}
