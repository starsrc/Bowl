package object

import (
	"fmt"
	"testing"
)

// TestFormatChunkKey 测试多租户对象键格式化规则
func TestFormatChunkKey(t *testing.T) {
	diskID := "d4b4a11b-1234-4567-89ab-cdef01234567"

	tests := []struct {
		name        string
		chunkIndx   uint32
		sliceID     uint64
		blockIndx   uint32
		expectedKey string
	}{
		{
			name:        "chunk 0",
			chunkIndx:   0,
			sliceID:     1,
			blockIndx:   0,
			expectedKey: fmt.Sprintf("chunks/%s/000/0/1_0", diskID),
		},
		{
			name:        "chunk 999",
			chunkIndx:   999,
			sliceID:     10,
			blockIndx:   1,
			expectedKey: fmt.Sprintf("chunks/%s/000/999/10_1", diskID),
		},
		{
			name:        "chunk 1000",
			chunkIndx:   1000,
			sliceID:     100,
			blockIndx:   2,
			expectedKey: fmt.Sprintf("chunks/%s/001/1000/100_2", diskID),
		},
		{
			name:        "chunk 2542",
			chunkIndx:   2542,
			sliceID:     9999,
			blockIndx:   3,
			expectedKey: fmt.Sprintf("chunks/%s/002/2542/9999_3", diskID),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := FormatChunkKey(diskID, tc.chunkIndx, tc.sliceID, tc.blockIndx)
			if key != tc.expectedKey {
				t.Fatalf("FormatChunkKey 不匹配: 期望 %s, 实际 %s", tc.expectedKey, key)
			}

			// 测试双向对称解析
			pDiskID, pChunkIndx, pSliceID, pBlockIndx, err := ParseChunkKey(key)
			if err != nil {
				t.Fatalf("ParseChunkKey 解析失败: %v", err)
			}
			if pDiskID != diskID || pChunkIndx != tc.chunkIndx || pSliceID != tc.sliceID || pBlockIndx != tc.blockIndx {
				t.Fatalf("ParseChunkKey 结果不匹配: %s, %d, %d, %d", pDiskID, pChunkIndx, pSliceID, pBlockIndx)
			}
		})
	}
}

// TestParseChunkKey_Invalid 测试非法键解析失败场景
func TestParseChunkKey_Invalid(t *testing.T) {
	invalidKeys := []string{
		"",
		"chunk/disk1/000/0/1_0",
		"chunks//000/0/1_0",
		"chunks/disk1/000/0",
		"chunks/disk1/000/0/1_0/extra",
		"chunks/disk1/000/notanumber/1_0",
		"chunks/disk1/000/0/invalid",
		"chunks/disk1/000/0/abc_0",
		"chunks/disk1/000/0/1_abc",
		"chunks/disk1/999/0/1_0", // chunk_prefix 000 与 chunk 0 不匹配
	}

	for _, key := range invalidKeys {
		t.Run(key, func(t *testing.T) {
			_, _, _, _, err := ParseChunkKey(key)
			if err == nil {
				t.Fatalf("期望解析 key %q 失败，但未报错", key)
			}
		})
	}
}

// TestPrefixHelpers 测试前缀与相对键辅助函数
func TestPrefixHelpers(t *testing.T) {
	diskID := "disk-uuid-1"
	if exp, act := "chunks/disk-uuid-1/", DiskPrefix(diskID); exp != act {
		t.Fatalf("DiskPrefix 期望 %s, 实际 %s", exp, act)
	}

	if exp, act := "chunks/disk-uuid-1/002/", ChunkPrefix(diskID, 2500); exp != act {
		t.Fatalf("ChunkPrefix 期望 %s, 实际 %s", exp, act)
	}

	relKey := FormatRelativeChunkKey(2500, 42, 1)
	if exp := "002/2500/42_1"; exp != relKey {
		t.Fatalf("FormatRelativeChunkKey 期望 %s, 实际 %s", exp, relKey)
	}

	cIdx, sID, bIdx, err := ParseRelativeChunkKey(relKey)
	if err != nil {
		t.Fatalf("ParseRelativeChunkKey 失败: %v", err)
	}
	if cIdx != 2500 || sID != 42 || bIdx != 1 {
		t.Fatalf("ParseRelativeChunkKey 结果不匹配: %d, %d, %d", cIdx, sID, bIdx)
	}
}
