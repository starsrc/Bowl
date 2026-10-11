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

// MetadataLocator resolves the file Inode and ChunkIndx associated with a staged slice.
type MetadataLocator interface {
	LocateSlice(ctx context.Context, diskID string, sliceID uint64) (ino uint64, chunkIndx uint32, found bool, err error)
}

// MetadataLocatorFunc allows a plain function to act as a MetadataLocator.
type MetadataLocatorFunc func(ctx context.Context, diskID string, sliceID uint64) (ino uint64, chunkIndx uint32, found bool, err error)

// LocateSlice delegates to the underlying function.
func (f MetadataLocatorFunc) LocateSlice(ctx context.Context, diskID string, sliceID uint64) (ino uint64, chunkIndx uint32, found bool, err error) {
	return f(ctx, diskID, sliceID)
}

// RecoveryReport contains metrics and statistics from a staging recovery replay run.
type RecoveryReport struct {
	ScannedDisks   int
	ScannedSlices  int
	RequeuedSlices int
	SkippedSlices  int
	OrphanedSlices int
	MaxSliceID     uint64
}

// RecoverStagingPipeline scans local staging directory `<staging_dir>/<disk_id>/slice_<slice_id>.bin`,
// verifies S3 persistence status, locates associated (ino, chunk_indx), and re-enqueues unflushed slices into S3Uploader.
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
		return nil, fmt.Errorf("read staging directory %s: %w", staging.Dir(), err)
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

			// 1. Locate (ino, chunk_indx)
			ino, chunkIndx, found, err := locator.LocateSlice(ctx, diskID, sliceID)
			if err != nil {
				return report, fmt.Errorf("locate slice (disk=%s, slice=%d): %w", diskID, sliceID, err)
			}
			if !found {
				report.OrphanedSlices++
				continue
			}

			// 2. Query S3 StatObject to check if slice is fully persisted
			alreadyPersisted, err := isSlicePersistedToS3(ctx, uploader, staging, diskID, chunkIndx, sliceID)
			if err != nil {
				return report, fmt.Errorf("check s3 persistence (disk=%s, slice=%d): %w", diskID, sliceID, err)
			}
			if alreadyPersisted {
				report.SkippedSlices++
				continue
			}

			// 3. Re-register slice as an UploadTask into S3Uploader
			task := &UploadTask{
				DiskID:    diskID,
				Ino:       ino,
				ChunkIndx: chunkIndx,
				SliceID:   sliceID,
			}
			if err := uploader.Enqueue(ctx, task); err != nil {
				return report, fmt.Errorf("enqueue upload task (disk=%s, slice=%d): %w", diskID, sliceID, err)
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
