package service

import (
	"github.com/starsrc/bowl/services/storage/internal/juicefs/adapter"
)

// MetaAdapter 为 JuiceFS 元数据适配层的类型别名
type MetaAdapter = adapter.MetaAdapter

// Slice 为 JuiceFS 元数据适配层切片结构的类型别名
type Slice = adapter.Slice

// NewMetaAdapter 构造绑定指定数据库执行器的 JuiceFS 元数据切片账本适配器
func NewMetaAdapter(db DBExecutor) MetaAdapter {
	return adapter.NewMetaAdapter(db)
}
