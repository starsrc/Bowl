package chunk_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/chunk"
)

func setupTestMinIOClient(t *testing.T, handler http.Handler) (*minio.Client, *httptest.Server) {
	t.Helper()
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		handler.ServeHTTP(w, r)
	})

	server := httptest.NewServer(wrappedHandler)
	u, err := url.Parse(server.URL)
	require.NoError(t, err)

	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4("mockaccess", "mocksecret", ""),
		Secure: false,
		Region: "us-east-1",
	})
	require.NoError(t, err)

	return client, server
}

func TestS3Reader_StagingHit_DoesNotCallS3(t *testing.T) {
	ctx := context.Background()
	stagingDir := filepath.Join(t.TempDir(), "staging")
	stagingMgr := chunk.NewStagingManager(stagingDir)

	diskID := "disk-test-1"
	chunkIndx := uint32(0)
	sliceID := uint64(10)
	payload := []byte("hot nvme staging data")

	// Write payload into staging
	err := stagingMgr.WriteSlice(diskID, sliceID, payload)
	require.NoError(t, err)

	s3Called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s3Called = true
		w.WriteHeader(http.StatusInternalServerError)
	})
	minioClient, server := setupTestMinIOClient(t, handler)
	defer server.Close()

	reader := chunk.NewS3Reader(minioClient, "bowl-data", stagingMgr)

	data, err := reader.ReadSlice(ctx, diskID, chunkIndx, sliceID)
	require.NoError(t, err)
	assert.Equal(t, payload, data)
	assert.False(t, s3Called, "S3 must not be queried when slice is present in hot staging")
}

func TestS3Reader_StagingMiss_ColdS3Fallback_SingleBlock(t *testing.T) {
	ctx := context.Background()
	stagingDir := filepath.Join(t.TempDir(), "staging")
	stagingMgr := chunk.NewStagingManager(stagingDir)

	diskID := "disk-test-1"
	chunkIndx := uint32(42)
	sliceID := uint64(101)
	payload := []byte("cold s3 payload bytes")

	expectedKey := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 0)
	assert.Equal(t, fmt.Sprintf("chunks/%s/000/%d/%d_0", diskID, chunkIndx, sliceID), expectedKey)

	s3RequestedKey := ""
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("handler request: method=%s, path=%s, query=%s", r.Method, r.URL.Path, r.URL.RawQuery)
		s3RequestedKey = r.URL.Path
		if r.URL.Path == "/bowl-data/"+expectedKey {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	minioClient, server := setupTestMinIOClient(t, handler)
	defer server.Close()

	reader := chunk.NewS3Reader(minioClient, "bowl-data", stagingMgr)

	// Ensure slice does NOT exist in staging
	_, err := os.Stat(stagingMgr.SlicePath(diskID, sliceID))
	require.True(t, os.IsNotExist(err))

	// Read should transparently penetrate through to cold S3
	data, err := reader.ReadSlice(ctx, diskID, chunkIndx, sliceID)
	require.NoError(t, err)
	assert.Equal(t, payload, data)
	assert.Equal(t, "/bowl-data/"+expectedKey, s3RequestedKey)
}

func TestS3Reader_NotFoundInBothStagingAndS3(t *testing.T) {
	ctx := context.Background()
	stagingDir := filepath.Join(t.TempDir(), "staging")
	stagingMgr := chunk.NewStagingManager(stagingDir)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	minioClient, server := setupTestMinIOClient(t, handler)
	defer server.Close()

	reader := chunk.NewS3Reader(minioClient, "bowl-data", stagingMgr)

	_, err := reader.ReadSlice(ctx, "disk-test-1", 0, 9999)
	require.Error(t, err)
}

func TestS3Reader_MultiBlock_ColdS3(t *testing.T) {
	ctx := context.Background()
	stagingDir := filepath.Join(t.TempDir(), "staging")
	stagingMgr := chunk.NewStagingManager(stagingDir)

	diskID := "disk-multi-1"
	chunkIndx := uint32(2)
	sliceID := uint64(500)

	// Custom batcher with small block size (1024 bytes) for test speed
	testBlockSize := 1024
	batcher := chunk.NewBlockBatcher(testBlockSize)

	// 2500 bytes payload -> 3 blocks: 1024, 1024, 452
	payload := make([]byte, 2500)
	for i := range payload {
		payload[i] = byte(i % 127)
	}

	block0 := payload[:1024]
	block1 := payload[1024:2048]
	block2 := payload[2048:]

	key0 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 0)
	key1 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 1)
	key2 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 2)

	objects := map[string][]byte{
		"/bowl-data/" + key0: block0,
		"/bowl-data/" + key1: block1,
		"/bowl-data/" + key2: block2,
	}

	requestedKeys := make([]string, 0)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedKeys = append(requestedKeys, r.URL.Path)
		if data, ok := objects[r.URL.Path]; ok {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	minioClient, server := setupTestMinIOClient(t, handler)
	defer server.Close()

	reader := chunk.NewS3Reader(minioClient, "bowl-data", stagingMgr,
		chunk.WithReaderBatcher(batcher),
	)

	data, err := reader.ReadSlice(ctx, diskID, chunkIndx, sliceID)
	require.NoError(t, err)
	assert.Equal(t, payload, data)
	assert.Equal(t, []string{"/bowl-data/" + key0, "/bowl-data/" + key1, "/bowl-data/" + key2}, requestedKeys)
}

func TestS3Reader_ReadSliceRange_HTTPRange(t *testing.T) {
	ctx := context.Background()
	stagingDir := filepath.Join(t.TempDir(), "staging")
	stagingMgr := chunk.NewStagingManager(stagingDir)

	diskID := "disk-range-1"
	chunkIndx := uint32(0)
	sliceID := uint64(600)

	testBlockSize := 1024
	batcher := chunk.NewBlockBatcher(testBlockSize)

	payload := make([]byte, 2048)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	key0 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 0)
	key1 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 1)

	var receivedRangeHeader string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRangeHeader = r.Header.Get("Range")
		var fullData []byte
		if r.URL.Path == "/bowl-data/"+key0 {
			fullData = payload[:1024]
		} else if r.URL.Path == "/bowl-data/"+key1 {
			fullData = payload[1024:]
		} else {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// minio-go range handling in mock HTTP server
		if receivedRangeHeader != "" {
			var start, end int64
			_, err := fmt.Sscanf(receivedRangeHeader, "bytes=%d-%d", &start, &end)
			if err == nil && start <= end && end < int64(len(fullData)) {
				subData := fullData[start : end+1]
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(fullData)))
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(subData)))
				w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(subData)
				return
			}
		}

		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fullData)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fullData)
	})
	minioClient, server := setupTestMinIOClient(t, handler)
	defer server.Close()

	reader := chunk.NewS3Reader(minioClient, "bowl-data", stagingMgr,
		chunk.WithReaderBatcher(batcher),
	)

	// Read range inside block 0: offset 100, length 50
	rangeData, err := reader.ReadSliceRange(ctx, diskID, chunkIndx, sliceID, 100, 50)
	require.NoError(t, err)
	assert.Equal(t, payload[100:150], rangeData)
	assert.Equal(t, "bytes=100-149", receivedRangeHeader)

	// Read range from staging when present
	err = stagingMgr.WriteSlice(diskID, 700, payload)
	require.NoError(t, err)
	stagingRangeData, err := reader.ReadSliceRange(ctx, diskID, chunkIndx, 700, 200, 80)
	require.NoError(t, err)
	assert.Equal(t, payload[200:280], stagingRangeData)

	// 验证分块读取包装器直接调用 ReadSliceRange
	wrapper := reader.ForChunkWithContext(ctx, chunkIndx)
	subData, err := wrapper.ReadSliceRange(diskID, sliceID, 100, 50)
	require.NoError(t, err)
	assert.Equal(t, payload[100:150], subData)
}


