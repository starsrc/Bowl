package db

import (
	"bytes"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

func ComputeActiveLCAHull(disks []DiskRecord, snapshots []SnapshotRecord) map[uuid.UUID]struct{} {
	hull := make(map[uuid.UUID]struct{})
	if len(disks) == 0 {
		return hull
	}

	bases := make([]uuid.UUID, 0, len(disks))
	for i := range disks {
		baseID := disks[i].BaseSnapshotID
		if baseID == uuid.Nil {
			continue
		}
		if _, exists := hull[baseID]; exists {
			continue
		}
		hull[baseID] = struct{}{}
		bases = append(bases, baseID)
	}
	if len(bases) <= 1 || len(snapshots) == 0 {
		return hull
	}

	byID := make(map[uuid.UUID]SnapshotRecord, len(snapshots))
	for i := range snapshots {
		byID[snapshots[i].SnapshotID] = snapshots[i]
	}

	ancByBase := make([]map[uuid.UUID]struct{}, len(bases))
	ancOfBases := make(map[uuid.UUID]struct{})
	for i, baseID := range bases {
		anc := collectSnapshotAncestors(baseID, byID)
		ancByBase[i] = anc
		for id := range anc {
			ancOfBases[id] = struct{}{}
		}
	}

	allMCAs := make(map[uuid.UUID]struct{})
	for i := 0; i < len(bases); i++ {
		for j := i + 1; j < len(bases); j++ {
			collectPairwiseMCAs(ancByBase[i], ancByBase[j], byID, allMCAs)
		}
	}
	if len(allMCAs) == 0 {
		return hull
	}

	childrenInAnc := make(map[uuid.UUID][]uuid.UUID, len(ancOfBases))
	for id := range ancOfBases {
		rec, ok := byID[id]
		if !ok {
			continue
		}
		if p := rec.ParentSnapshotID; p != uuid.Nil && p != id {
			if _, inAnc := ancOfBases[p]; inAnc {
				childrenInAnc[p] = append(childrenInAnc[p], id)
			}
		}
		if mp := rec.MergeParentSnapshotID; mp != uuid.Nil && mp != id && mp != rec.ParentSnapshotID {
			if _, inAnc := ancOfBases[mp]; inAnc {
				childrenInAnc[mp] = append(childrenInAnc[mp], id)
			}
		}
	}

	visited := make(map[uuid.UUID]struct{}, len(allMCAs))
	queue := make([]uuid.UUID, 0, len(allMCAs))
	for id := range allMCAs {
		visited[id] = struct{}{}
		hull[id] = struct{}{}
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, child := range childrenInAnc[cur] {
			if _, seen := visited[child]; seen {
				continue
			}
			visited[child] = struct{}{}
			hull[child] = struct{}{}
			queue = append(queue, child)
		}
	}
	return hull
}

func collectSnapshotAncestors(startID uuid.UUID, byID map[uuid.UUID]SnapshotRecord) map[uuid.UUID]struct{} {
	anc := make(map[uuid.UUID]struct{})
	if _, ok := byID[startID]; !ok {
		return anc
	}
	queue := []uuid.UUID{startID}
	anc[startID] = struct{}{}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		rec := byID[cur]
		for _, parentID := range [2]uuid.UUID{rec.ParentSnapshotID, rec.MergeParentSnapshotID} {
			if parentID == uuid.Nil {
				continue
			}
			if _, seen := anc[parentID]; seen {
				continue
			}
			if _, ok := byID[parentID]; !ok {
				continue
			}
			anc[parentID] = struct{}{}
			queue = append(queue, parentID)
		}
	}
	return anc
}

func collectPairwiseMCAs(ancA, ancB map[uuid.UUID]struct{}, byID map[uuid.UUID]SnapshotRecord, out map[uuid.UUID]struct{}) {
	smaller, larger := ancA, ancB
	if len(ancB) < len(ancA) {
		smaller, larger = ancB, ancA
	}
	common := make(map[uuid.UUID]struct{})
	for id := range smaller {
		if _, ok := larger[id]; ok {
			common[id] = struct{}{}
		}
	}
	if len(common) == 0 {
		return
	}
	nonMaximal := make(map[uuid.UUID]struct{})
	for id := range common {
		rec, ok := byID[id]
		if !ok {
			continue
		}
		if p := rec.ParentSnapshotID; p != uuid.Nil && p != id {
			nonMaximal[p] = struct{}{}
		}
		if mp := rec.MergeParentSnapshotID; mp != uuid.Nil && mp != id {
			nonMaximal[mp] = struct{}{}
		}
	}
	for id := range common {
		if _, dominated := nonMaximal[id]; !dominated {
			out[id] = struct{}{}
		}
	}
}

func validateSnapshotKindFilter(kind SnapshotKind) error {
	switch kind {
	case "", SnapshotKindFork, SnapshotKindMerge, SnapshotKindManual:
		return nil
	default:
		return fmt.Errorf("%w: invalid snapshot kind %q", ErrInvalidArgument, kind)
	}
}

func sortSnapshotsDesc(snaps []SnapshotRecord) {
	slices.SortFunc(snaps, func(a, b SnapshotRecord) int {
		if cmp := b.CreatedAt.Compare(a.CreatedAt); cmp != 0 {
			return cmp
		}
		return bytes.Compare(b.SnapshotID[:], a.SnapshotID[:])
	})
}

func buildListSnapshotsPage(
	disks []DiskRecord,
	snapshots []SnapshotRecord,
	kind SnapshotKind,
	pageSize uint32,
	hasCursor bool,
	cursorTime time.Time,
	cursorSnapID uuid.UUID,
) ListSnapshotsPage {
	hull := ComputeActiveLCAHull(disks, snapshots)
	filtered := make([]SnapshotRecord, 0, len(snapshots))
	for i := range snapshots {
		snap := snapshots[i]
		if kind != "" && snap.Kind != kind {
			continue
		}
		if hasCursor {
			beforeTime := snap.CreatedAt.Before(cursorTime)
			sameTimeSmallerID := snap.CreatedAt.Equal(cursorTime) && bytes.Compare(snap.SnapshotID[:], cursorSnapID[:]) < 0
			if !beforeTime && !sameTimeSmallerID {
				continue
			}
		}
		_, snap.Protected = hull[snap.SnapshotID]
		filtered = append(filtered, snap)
	}
	sortSnapshotsDesc(filtered)

	var nextPageToken string
	if len(filtered) > int(pageSize) {
		filtered = filtered[:pageSize]
		last := filtered[pageSize-1]
		nextPageToken = encodeSnapshotPageCursor(last.CreatedAt, last.SnapshotID)
	}
	return ListSnapshotsPage{
		Snapshots:     filtered,
		NextPageToken: nextPageToken,
	}
}

func selectPrunableSnapshots(disks []DiskRecord, snapshots []SnapshotRecord) []SnapshotRecord {
	if len(snapshots) == 0 {
		return nil
	}
	hull := ComputeActiveLCAHull(disks, snapshots)
	candidates := make([]SnapshotRecord, 0)
	for i := range snapshots {
		snap := snapshots[i]
		if snap.Kind == SnapshotKindManual {
			continue
		}
		if _, protected := hull[snap.SnapshotID]; protected {
			continue
		}
		snap.Protected = false
		candidates = append(candidates, snap)
	}
	sortSnapshotsDesc(candidates)
	return candidates
}
