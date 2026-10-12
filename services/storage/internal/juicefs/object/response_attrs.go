package object

// DefaultStorageClass 默认存储类型
const DefaultStorageClass = "STANDARD"

// ResponseAttrs 从底层对象存储客户端获取返回属性的通用结构
type ResponseAttrs struct {
	storageClass *string
	requestID    *string
	requestSize  *int64
}

// SetRequestID 记录底层请求 ID
func (r *ResponseAttrs) SetRequestID(id string) *ResponseAttrs {
	if r.requestID != nil {
		*r.requestID = id
	}
	return r
}

// SetStorageClass 记录存储类别
func (r *ResponseAttrs) SetStorageClass(sc string) *ResponseAttrs {
	if r.storageClass != nil && sc != "" {
		*r.storageClass = sc
	}
	return r
}

// GetRequestSize 获取请求字节大小
func (r *ResponseAttrs) GetRequestSize() int64 {
	if r.requestSize != nil {
		return *r.requestSize
	}
	return -1
}

// AttrGetter 属性读取器回调函数
type AttrGetter func(attrs *ResponseAttrs)

// WithRequestID 捕获请求 ID 回调
func WithRequestID(id *string) AttrGetter {
	return func(attrs *ResponseAttrs) {
		attrs.requestID = id
	}
}

// WithStorageClass 捕获存储类别回调
func WithStorageClass(sc *string) AttrGetter {
	return func(attrs *ResponseAttrs) {
		attrs.storageClass = sc
	}
}

// WithRequestSize 捕获请求大小回调
func WithRequestSize(size *int64) AttrGetter {
	return func(attrs *ResponseAttrs) {
		attrs.requestSize = size
	}
}

// ApplyGetters 执行所有属性读取回调并返回聚合属性
func ApplyGetters(getters ...AttrGetter) ResponseAttrs {
	var attrs ResponseAttrs
	for _, getter := range getters {
		getter(&attrs)
	}
	return attrs
}
