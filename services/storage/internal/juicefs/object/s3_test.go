package object

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// s3MockServer 内存模拟 S3 HTTP 服务器，支持基础 Put, Range Get, Delete, Batch Delete 和 List
type s3MockServer struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newS3MockServer() *httptest.Server {
	mock := &s3MockServer{
		objects: make(map[string][]byte),
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		bucket := parts[0]
		key := ""
		if len(parts) > 1 {
			key = parts[1]
		}

		switch r.Method {
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mock.mu.Lock()
			mock.objects[bucket+"/"+key] = data
			mock.mu.Unlock()
			w.Header().Set("ETag", `"mock-etag"`)
			w.WriteHeader(http.StatusOK)

		case http.MethodHead:
			mock.mu.RLock()
			data, ok := mock.objects[bucket+"/"+key]
			mock.mu.RUnlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			// 处理列举对象
			if key == "" || r.URL.Query().Has("list-type") || r.URL.Query().Has("prefix") {
				prefix := r.URL.Query().Get("prefix")
				type ObjectXML struct {
					Key          string    `xml:"Key"`
					LastModified time.Time `xml:"LastModified"`
					ETag         string    `xml:"ETag"`
					Size         int64     `xml:"Size"`
					StorageClass string    `xml:"StorageClass"`
				}
				type ListBucketResult struct {
					XMLName  xml.Name    `xml:"ListBucketResult"`
					Name     string      `xml:"Name"`
					Prefix   string      `xml:"Prefix"`
					Contents []ObjectXML `xml:"Contents"`
				}

				res := ListBucketResult{
					Name:   bucket,
					Prefix: prefix,
				}

				mock.mu.RLock()
				for k, v := range mock.objects {
					if strings.HasPrefix(k, bucket+"/") {
						objKey := strings.TrimPrefix(k, bucket+"/")
						if strings.HasPrefix(objKey, prefix) {
							res.Contents = append(res.Contents, ObjectXML{
								Key:          objKey,
								LastModified: time.Now(),
								ETag:         `"mock-etag"`,
								Size:         int64(len(v)),
								StorageClass: "STANDARD",
							})
						}
					}
				}
				mock.mu.RUnlock()

				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusOK)
				_ = xml.NewEncoder(w).Encode(res)
				return
			}

			// 普通对象读取与 Range 请求
			mock.mu.RLock()
			data, ok := mock.objects[bucket+"/"+key]
			mock.mu.RUnlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			rangeHeader := r.Header.Get("Range")
			if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
				rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
				parts := strings.Split(rangeSpec, "-")
				start, _ := strconv.ParseInt(parts[0], 10, 64)
				end := int64(len(data) - 1)
				if len(parts) > 1 && parts[1] != "" {
					end, _ = strconv.ParseInt(parts[1], 10, 64)
				}
				if start < 0 {
					start = 0
				}
				if end >= int64(len(data)) {
					end = int64(len(data) - 1)
				}
				if start <= end && start < int64(len(data)) {
					partial := data[start : end+1]
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
					w.Header().Set("Content-Length", strconv.Itoa(len(partial)))
					w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(partial)
					return
				}
			}

			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)

		case http.MethodDelete:
			mock.mu.Lock()
			delete(mock.objects, bucket+"/"+key)
			mock.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)

		case http.MethodPost:
			// S3 批量删除 XML 请求 (?delete)
			if r.URL.Query().Has("delete") {
				type DeleteKey struct {
					Key string `xml:"Key"`
				}
				type DeleteRequest struct {
					Objects []DeleteKey `xml:"Object"`
				}
				var req DeleteRequest
				_ = xml.NewDecoder(r.Body).Decode(&req)

				mock.mu.Lock()
				for _, obj := range req.Objects {
					delete(mock.objects, bucket+"/"+obj.Key)
				}
				mock.mu.Unlock()

				type DeletedObj struct {
					Key string `xml:"Key"`
				}
				type DeleteResult struct {
					XMLName xml.Name     `xml:"DeleteResult"`
					Deleted []DeletedObj `xml:"Deleted"`
				}
				res := DeleteResult{}
				for _, obj := range req.Objects {
					res.Deleted = append(res.Deleted, DeletedObj{Key: obj.Key})
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusOK)
				_ = xml.NewEncoder(w).Encode(res)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	return httptest.NewServer(handler)
}

// TestS3_Mock_FullLifecycle 测试 S3 驱动的完整生命周期：4MB 写入、Range 读取、批量删除与多租户隔离
func TestS3_Mock_FullLifecycle(t *testing.T) {
	ts := newS3MockServer()
	defer ts.Close()

	endpoint := strings.TrimPrefix(ts.URL, "http://")
	bucket := "test-bucket"

	// 1. 初始化 S3 驱动
	store, err := NewS3(endpoint, bucket, "testaccess", "testsecret", "", false)
	require.NoError(t, err, "初始化 S3 驱动必须成功")
	require.NotNil(t, store)

	ctx := context.Background()

	// 2. 测试 4MB 物理数据块上传
	diskID := "disk-tenant-alpha"
	key4MB := FormatChunkKey(diskID, 0, 100, 0)
	fourMB := 4 * 1024 * 1024
	payload4MB := make([]byte, fourMB)
	for i := range payload4MB {
		payload4MB[i] = byte(i % 251)
	}

	err = store.Put(ctx, key4MB, bytes.NewReader(payload4MB))
	require.NoError(t, err, "4MB 物理分块 Put 上传必须成功")

	// 3. 元数据 Head 查询
	obj, err := store.Head(ctx, key4MB)
	require.NoError(t, err)
	assert.Equal(t, int64(fourMB), obj.Size(), "对象大小必须为 4MB")

	// 4. HTTP Range 局部读取 (如读取 64KB 跨度)
	offset := int64(1024 * 1024)     // 1MB 偏移
	length := int64(64 * 1024)        // 64KB 长度
	rc, err := store.Get(ctx, key4MB, offset, length)
	require.NoError(t, err, "HTTP Range 部分读取请求必须成功")
	defer rc.Close()

	readBuf := make([]byte, length)
	n, err := io.ReadFull(rc, readBuf)
	require.NoError(t, err)
	assert.Equal(t, int(length), n)
	assert.True(t, bytes.Equal(readBuf, payload4MB[offset:offset+length]), "Range 读取的数据必须与原始 4MB 块相应区间完全匹配")

	// 5. 多租户前缀适配与隔离校验
	tenantStoreA := WithDiskPrefix(store, "disk-A")
	tenantStoreB := WithDiskPrefix(store, "disk-B")

	relKey := FormatRelativeChunkKey(0, 1, 0)
	err = tenantStoreA.Put(ctx, relKey, bytes.NewReader([]byte("data-disk-A")))
	require.NoError(t, err)
	err = tenantStoreB.Put(ctx, relKey, bytes.NewReader([]byte("data-disk-B")))
	require.NoError(t, err)

	rcA, err := tenantStoreA.Get(ctx, relKey, 0, -1)
	require.NoError(t, err)
	dataA, _ := io.ReadAll(rcA)
	rcA.Close()
	assert.Equal(t, "data-disk-A", string(dataA))

	rcB, err := tenantStoreB.Get(ctx, relKey, 0, -1)
	require.NoError(t, err)
	dataB, _ := io.ReadAll(rcB)
	rcB.Close()
	assert.Equal(t, "data-disk-B", string(dataB))

	// 6. 批量删除 (DeleteBatch)
	batchKeys := []string{
		FormatChunkKey(diskID, 0, 100, 1),
		FormatChunkKey(diskID, 0, 100, 2),
		FormatChunkKey(diskID, 0, 100, 3),
	}
	for _, k := range batchKeys {
		require.NoError(t, store.Put(ctx, k, bytes.NewReader([]byte("batch-data"))))
	}

	err = store.DeleteBatch(ctx, batchKeys)
	require.NoError(t, err, "DeleteBatch 批量删除必须成功")

	for _, k := range batchKeys {
		_, err := store.Head(ctx, k)
		assert.Error(t, err, "批量删除后对象应当不存在: %s", k)
	}

	// 7. 按前缀删除 (DeletePrefix)
	err = store.DeletePrefix(ctx, "chunks/disk-A/")
	require.NoError(t, err)

	_, err = tenantStoreA.Head(ctx, relKey)
	assert.Error(t, err, "disk-A 下的对象应已被前缀删除")

	_, err = tenantStoreB.Head(ctx, relKey)
	assert.NoError(t, err, "disk-B 下的对象应完好保留，不受 disk-A 删除影响")
}

func isLiveMinIOAvailable(endpoint string) bool {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(endpoint + "/minio/health/live")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// TestS3_LiveMinIO_Integration 当外部 MinIO 存活时执行真实环境集成测试
func TestS3_LiveMinIO_Integration(t *testing.T) {
	endpoint := "127.0.0.1:9000"
	httpEndpoint := "http://" + endpoint
	if !isLiveMinIOAvailable(httpEndpoint) {
		t.Skip("MinIO 未运行在 127.0.0.1:9000，跳过外部集成测试")
	}

	bucket := fmt.Sprintf("bowl-s3-test-%d", time.Now().UnixNano()%100000)
	store, err := NewS3(endpoint, bucket, "minioadmin", "minioadmin", "", false)
	require.NoError(t, err)

	ctx := context.Background()

	// 1. 创建测试桶
	err = store.Create(ctx)
	require.NoError(t, err, "MinIO 桶创建必须成功")

	// 2. 写入 4MB 物理数据块
	diskID := fmt.Sprintf("disk-%d", time.Now().UnixNano())
	key := FormatChunkKey(diskID, 0, 888, 0)
	fourMB := 4 * 1024 * 1024
	blockData := make([]byte, fourMB)
	_, _ = rand.Read(blockData)

	err = store.Put(ctx, key, bytes.NewReader(blockData))
	require.NoError(t, err, "真实 MinIO 写入 4MB 数据块必须成功")

	// 3. 元数据验证
	obj, err := store.Head(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(fourMB), obj.Size())

	// 4. HTTP Range 局部读取验证
	off := int64(512 * 1024)
	limit := int64(128 * 1024)
	rc, err := store.Get(ctx, key, off, limit)
	require.NoError(t, err)
	rangeBytes, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	assert.Equal(t, int(limit), len(rangeBytes))
	assert.True(t, bytes.Equal(rangeBytes, blockData[off:off+limit]))

	// 5. 租户隔离前缀测试
	tenantStore := WithDiskPrefix(store, diskID)
	relKey := FormatRelativeChunkKey(0, 888, 0)
	relObj, err := tenantStore.Head(ctx, relKey)
	require.NoError(t, err)
	assert.Equal(t, int64(fourMB), relObj.Size())

	// 6. 批量删除与前缀级联清理
	err = tenantStore.DeleteBatch(ctx, []string{relKey})
	require.NoError(t, err)

	_, err = tenantStore.Head(ctx, relKey)
	assert.Error(t, err, "批量删除后对象不应再被查询到")
}
