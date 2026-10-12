package chunk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
)

// MetadataLocator 解析并定位暂存切片所归属的 Inode 和 ChunkIndx
type MetadataLocator interface {
	LocateSlice(ctx context.Context, diskID string, sliceID uint64) (ino uint64, chunkIndx uint32, found bool, err error)
}

// MetadataLocatorFunc 支持普通函数作为 MetadataLocator 接口实现
type MetadataLocatorFunc func(ctx context.Context, diskID string, sliceID uint64) (ino uint64, chunkIndx uint32, found bool, err error)

// LocateSlice 委托给底层函数实现
func (f MetadataLocatorFunc) LocateSlice(ctx context.Context, diskID string, sliceID uint64) (ino uint64, chunkIndx uint32, found bool, err error) {
	return f(ctx, diskID, sliceID)
}

// RecoveryReport 记录 Staging 暂存恢复重放管道的统计指标
type RecoveryReport struct {
	ScannedDisks   int
	ScannedSlices  int
	RequeuedSlices int
	SkippedSlices  int
	OrphanedSlices int
	MaxSliceID     uint64
}

// RecoverStagingPipeline 扫描本地暂存目录 <staging_dir>/<disk_id>/slice_<slice_id>.bin，
// 查验 S3 持久化状态，定位所属 (ino, chunk_indx)，并将未落盘切片重新入队至 S3Uploader 进行持久化推流。
func RecoverStagingPipeline(
	ctx context.Context,
	staging *StagingManager,
	uploader *S3Uploader,
	locator MetadataLocator,
) (*RecoveryReport, error) {
	report := &RecoveryReport{}
	if staging == nil {
		return report, nil
	}

	entries, err := os.ReadDir(staging.Dir())
	if err != nil {
		if os.IsNotExist(err) {
			return report, nil
		}
		return nil, fmt.Errorf("读取 staging 目录 %s 失败: %w", staging.Dir(), err)
	}

	var maxSliceID uint64

	for _, diskEntry := range entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if !diskEntry.IsDir() {
			continue
		}

		diskID := diskEntry.Name()
		diskDir := filepath.Join(staging.Dir(), diskID)

		sliceEntries, err := os.ReadDir(diskDir)
		if err != nil {
			continue
		}
		report.ScannedDisks++

		for _, fileEntry := range sliceEntries {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			if fileEntry.IsDir() {
				continue
			}

			fileName := fileEntry.Name()
			if !strings.HasPrefix(fileName, "slice_") || !strings.HasSuffix(fileName, ".bin") {
				continue
			}

			idStr := strings.TrimSuffix(strings.TrimPrefix(fileName, "slice_"), ".bin")
			sliceID, err := strconv.ParseUint(idStr, 10, 64)
			if err != nil {
				continue
			}

			report.ScannedSlices++
			if sliceID > maxSliceID {
				maxSliceID = sliceID
			}

			if locator == nil || uploader == nil {
				continue
			}

			// 1. 定位 (ino, chunk_indx)
			ino, chunkIndx, found, err := locator.LocateSlice(ctx, diskID, sliceID)
			if err != nil {
				return report, fmt.Errorf("定位切片元数据失败 (disk=%s, slice=%d): %w", diskID, sliceID, err)
			}
			if !found {
				report.OrphanedSlices++
				continue
			}

			// 2. 通过 S3 StatObject 查验该切片是否已全部持久化至对象存储
			alreadyPersisted, err := isSlicePersistedToS3(ctx, uploader, staging, diskID, chunkIndx, sliceID)
			if err != nil {
				return report, fmt.Errorf("查验 S3 持久化状态失败 (disk=%s, slice=%d): %w", diskID, sliceID, err)
			}
			if alreadyPersisted {
				report.SkippedSlices++
				continue
			}

			// 3. 将未完全落盘的切片作为 UploadTask 重新入队到 S3Uploader
			task := &UploadTask{
				DiskID:    diskID,
				Ino:       ino,
				ChunkIndx: chunkIndx,
				SliceID:   sliceID,
			}
			if err := uploader.Enqueue(ctx, task); err != nil {
				return report, fmt.Errorf("重放切片入队失败 (disk=%s, slice=%d): %w", diskID, sliceID, err)
			}
			report.RequeuedSlices++
		}
	}

	staging.UpdateMaxSliceID(maxSliceID)
	report.MaxSliceID = maxSliceID

	return report, nil
}

func isSlicePersistedToS3(
	ctx context.Context,
	uploader *S3Uploader,
	staging *StagingManager,
	diskID string,
	chunkIndx uint32,
	sliceID uint64,
) (bool, error) {
	if uploader == nil {
		return false, nil
	}
	client := uploader.Client()
	if client == nil {
		return false, nil
	}

	path := staging.SlicePath(diskID, sliceID)
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		return false, nil
	}

	blockSize := int64(BlockSize)
	if uploader.batcher != nil && uploader.batcher.BlockSize() > 0 {
		blockSize = int64(uploader.batcher.BlockSize())
	}

	numBlocks := (fileSize + blockSize - 1) / blockSize

	for b := int64(0); b < numBlocks; b++ {
		key := FormatObjectKey(diskID, chunkIndx, sliceID, uint32(b))
		_, err := client.StatObject(ctx, uploader.Bucket(), key, minio.StatObjectOptions{})
		if err != nil {
			if isS3NotFoundError(err) {
				return false, nil
			}
			var minioErr minio.ErrorResponse
			if errors.As(err, &minioErr) && minioErr.StatusCode == 404 {
				return false, nil
			}
			return false, err
		}
	}

	return true, nil
}
