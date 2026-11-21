//go:build unix

package control

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type devInoKey struct {
	dev uint64
	ino uint64
}

type storageRootUsage struct {
	PackPhysicalBytes      uint64
	SnapshotsActive        int
	SnapshotsPhysicalBytes uint64
	DataDirPhysicalBytes   uint64
	VolumeTotalBytes       uint64
	VolumeAvailableBytes   uint64
}

func scanStorageRootUsage(root string) storageRootUsage {
	var usage storageRootUsage
	if root == "" {
		return usage
	}
	cleanRoot := filepath.Clean(root)

	usage.VolumeTotalBytes, usage.VolumeAvailableBytes = readVolumeSpace(cleanRoot)

	if _, err := os.Stat(cleanRoot); err != nil {
		return usage
	}

	allSeen := make(map[devInoKey]struct{})
	packSeen := make(map[devInoKey]struct{})
	snapshotSeen := make(map[devInoKey]struct{})

	_ = filepath.WalkDir(cleanRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() {
			rel, relErr := filepath.Rel(cleanRoot, path)
			if relErr == nil && isDirectSnapshotSubdir(rel) {
				usage.SnapshotsActive++
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil || !info.Mode().IsRegular() {
			return nil
		}
		size := info.Size()
		if size <= 0 {
			return nil
		}
		fileBytes := uint64(size)

		rel, relErr := filepath.Rel(cleanRoot, path)
		if relErr != nil {
			return nil
		}
		slashRel := filepath.ToSlash(rel)

		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st == nil {
			usage.DataDirPhysicalBytes += fileBytes
			if isSnapshotTreePath(slashRel) {
				usage.SnapshotsPhysicalBytes += fileBytes
			}
			if isManagedPackPath(slashRel) {
				usage.PackPhysicalBytes += fileBytes
			}
			return nil
		}

		key := devInoKey{
			dev: uint64(st.Dev),
			ino: st.Ino,
		}
		if _, exists := allSeen[key]; !exists {
			allSeen[key] = struct{}{}
			usage.DataDirPhysicalBytes += fileBytes
		}
		if isSnapshotTreePath(slashRel) {
			if _, exists := snapshotSeen[key]; !exists {
				snapshotSeen[key] = struct{}{}
				usage.SnapshotsPhysicalBytes += fileBytes
			}
		}
		if isManagedPackPath(slashRel) {
			if _, exists := packSeen[key]; !exists {
				packSeen[key] = struct{}{}
				usage.PackPhysicalBytes += fileBytes
			}
		}
		return nil
	})

	return usage
}

func isDirectSnapshotSubdir(rel string) bool {
	slash := filepath.ToSlash(rel)
	if !strings.HasPrefix(slash, "snapshots/") {
		return false
	}
	rest := strings.TrimPrefix(slash, "snapshots/")
	return rest != "" && !strings.Contains(rest, "/")
}

func isSnapshotTreePath(slashRel string) bool {
	return strings.HasPrefix(slashRel, "snapshots/")
}

func isManagedPackPath(slashRel string) bool {
	if !strings.HasSuffix(slashRel, ".pack") {
		return false
	}
	if !strings.HasPrefix(slashRel, "disks/") && !strings.HasPrefix(slashRel, "snapshots/") {
		return false
	}
	return filepath.Base(filepath.Dir(slashRel)) == "packs"
}

func readVolumeSpace(dir string) (totalBytes, availBytes uint64) {
	cur := filepath.Clean(dir)
	for {
		if _, err := os.Stat(cur); err == nil {
			var fsStat syscall.Statfs_t
			if statErr := syscall.Statfs(cur, &fsStat); statErr == nil && fsStat.Bsize > 0 {
				blockSize := uint64(fsStat.Bsize)
				return fsStat.Blocks * blockSize, fsStat.Bavail * blockSize
			}
			return 0, 0
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return 0, 0
		}
		cur = parent
	}
}
