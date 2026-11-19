package handoff

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"
)

// GCOptions 控制远端回收的保留策略。
type GCOptions struct {
	// KeepSnapshots 是每块盘除 HEAD 指向的快照外，额外保留的最近快照个数。
	// 保留旧快照是为了让掉线的节点回来后还能找到它最后一次同步的快照，作为合并的共同祖先。
	KeepSnapshots int
	// MinAge 以内的快照和对象一律不删：既是旧快照的保留期，也是给"对象已上传、
	// 清单还没写"的在途推送留出的安全窗口。
	MinAge time.Duration
	// Now 用于测试注入时钟。
	Now func() time.Time
}

// GCStats 汇总一次回收删除了什么。
type GCStats struct {
	Disks            int   `json:"disks"`
	SnapshotsKept    int   `json:"snapshots_kept"`
	SnapshotsDeleted int   `json:"snapshots_deleted"`
	ObjectsKept      int   `json:"objects_kept"`
	ObjectsDeleted   int   `json:"objects_deleted"`
	BytesFreed       int64 `json:"bytes_freed"`
}

// DeleteDisk 删除一块盘在远端的 HEAD 与全部快照。它引用的数据包对象由之后的 GC 回收。
// 先删 HEAD：从这一刻起其他节点就不再把远端当作这块盘的权威。
func DeleteDisk(ctx context.Context, r Remote, diskID string) error {
	if err := r.Delete(ctx, headKey(diskID)); err != nil {
		return err
	}
	objects, err := r.List(ctx, diskPrefix(diskID)+"/")
	if err != nil {
		return err
	}
	for _, obj := range objects {
		if err := r.Delete(ctx, obj.Key); err != nil {
			return err
		}
	}
	return nil
}

// GC 删除远端不再需要的旧快照，以及没有任何保留快照引用的数据包对象。
//
// 做法是标记-清除：先定出每块盘要保留的快照，读出它们引用的对象，再删掉其余的。
// 任何一个保留快照的清单读不出来，本轮就只删快照、不删对象——宁可少删。
//
// ponytail: 清除与并发推送之间仍有一个极窄的窗口——推送复用了一个"旧的、当时没人引用"的
// 对象，而本轮在它写出清单之前就完成了标记。MinAge 与清除前的二次标记把窗口压到
// "二次标记之后、删除之前"这一小段；要彻底消除需要对象存储的条件删除或版本化。
func GC(ctx context.Context, r Remote, opts GCOptions) (GCStats, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	cutoff := opts.Now().Add(-opts.MinAge)
	var stats GCStats

	listing, err := r.List(ctx, "disks/")
	if err != nil {
		return stats, err
	}
	type snapshotInfo struct {
		keys     []string
		complete bool
		newest   time.Time
	}
	disks := make(map[string]map[string]*snapshotInfo)
	for _, obj := range listing {
		parts := strings.SplitN(obj.Key, "/", 5) // disks/<id>/snapshots/<name>/...
		if len(parts) < 5 || parts[2] != "snapshots" {
			continue
		}
		snaps := disks[parts[1]]
		if snaps == nil {
			snaps = make(map[string]*snapshotInfo)
			disks[parts[1]] = snaps
		}
		info := snaps[parts[3]]
		if info == nil {
			info = &snapshotInfo{}
			snaps[parts[3]] = info
		}
		info.keys = append(info.keys, obj.Key)
		if parts[4] == "manifest.json" {
			info.complete = true
		}
		if obj.ModTime.After(info.newest) {
			info.newest = obj.ModTime
		}
	}

	var retained []string // 保留快照的 manifest 键
	for diskID, snaps := range disks {
		stats.Disks++
		headSnapshot := ""
		if head, _, headErr := ReadHead(ctx, r, diskID); headErr == nil {
			headSnapshot = head.Snapshot
		} else if !errors.Is(headErr, ErrNotFound) {
			return stats, fmt.Errorf("handoff gc: read head of %s: %w", diskID, headErr)
		}
		names := make([]string, 0, len(snaps))
		for name := range snaps {
			names = append(names, name)
		}
		slices.Sort(names)
		slices.Reverse(names) // 快照名定宽，字典序即时间序：最新的在前
		extra := 0
		for _, name := range names {
			info := snaps[name]
			keep := name == headSnapshot || info.newest.After(cutoff)
			if !keep && info.complete && extra < opts.KeepSnapshots {
				keep = true
			}
			if keep {
				if info.complete {
					if name != headSnapshot {
						extra++
					}
					retained = append(retained, path.Join(snapshotPrefix(diskID, name), "manifest.json"))
				}
				stats.SnapshotsKept++
				continue
			}
			for _, key := range info.keys {
				if delErr := r.Delete(ctx, key); delErr != nil {
					return stats, delErr
				}
			}
			stats.SnapshotsDeleted++
		}
	}

	referenced, err := referencedObjects(ctx, r, retained)
	if err != nil {
		return stats, nil //nolint:nilerr // 有清单读不出来时放弃本轮对象清除，避免误删；快照已按策略处理
	}
	objects, err := r.List(ctx, "objects/")
	if err != nil {
		return stats, err
	}
	var candidates []ObjectInfo
	for _, obj := range objects {
		if _, ok := referenced[obj.Key]; ok || obj.ModTime.After(cutoff) {
			stats.ObjectsKept++
			continue
		}
		candidates = append(candidates, obj)
	}
	if len(candidates) == 0 {
		return stats, nil
	}
	// 二次标记：把标记期间新写出来的清单也算进去，再动手删。
	fresh, err := r.List(ctx, "disks/")
	if err != nil {
		return stats, err
	}
	var freshManifests []string
	for _, obj := range fresh {
		if strings.HasSuffix(obj.Key, "/manifest.json") && !slices.Contains(retained, obj.Key) {
			freshManifests = append(freshManifests, obj.Key)
		}
	}
	late, err := referencedObjects(ctx, r, freshManifests)
	if err != nil {
		return stats, nil //nolint:nilerr // 同上：读不全就不删对象
	}
	for _, obj := range candidates {
		if _, ok := late[obj.Key]; ok {
			stats.ObjectsKept++
			continue
		}
		if delErr := r.Delete(ctx, obj.Key); delErr != nil {
			return stats, delErr
		}
		stats.ObjectsDeleted++
		stats.BytesFreed += obj.Size
	}
	return stats, nil
}

// referencedObjects 读出一组清单引用的全部数据包对象键。清单在读取前被并发删除的忽略。
func referencedObjects(ctx context.Context, r Remote, manifestKeys []string) (map[string]struct{}, error) {
	refs := make(map[string]struct{})
	for _, key := range manifestKeys {
		rc, err := r.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		raw, readErr := io.ReadAll(rc)
		if err := errors.Join(readErr, rc.Close()); err != nil {
			return nil, err
		}
		var mf manifest
		if err := json.Unmarshal(raw, &mf); err != nil {
			return nil, fmt.Errorf("handoff gc: decode %s: %w", key, err)
		}
		for _, p := range mf.Packs {
			if len(p.SHA256) < 2 {
				return nil, fmt.Errorf("handoff gc: %s lists a pack without content hash", key)
			}
			refs[objectKey(p.SHA256)] = struct{}{}
		}
	}
	return refs, nil
}
