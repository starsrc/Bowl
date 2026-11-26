package engine

func collectAncestors(r pebbleReader, dk diskKeys, startCommitID uint64) (map[uint64]CommitMeta, error) {
	if startCommitID == 0 {
		return nil, ErrInvalid
	}
	anc := make(map[uint64]CommitMeta)
	stack := []uint64{startCommitID}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == 0 {
			continue
		}
		if _, seen := anc[cur]; seen {
			continue
		}
		cmt, err := readCommitMeta(r, dk, cur)
		if err != nil {
			return nil, err
		}
		anc[cur] = cmt
		if cmt.ParentID != 0 {
			stack = append(stack, cmt.ParentID)
		}
		if cmt.MergeParentID != 0 {
			stack = append(stack, cmt.MergeParentID)
		}
	}
	return anc, nil
}

func findLCA(r pebbleReader, dk diskKeys, commitA, commitB uint64) (uint64, error) {
	if commitA == 0 || commitB == 0 {
		return 0, ErrInvalid
	}
	if commitA == commitB {
		if _, err := readCommitMeta(r, dk, commitA); err != nil {
			return 0, err
		}
		return commitA, nil
	}

	ancA, err := collectAncestors(r, dk, commitA)
	if err != nil {
		return 0, err
	}
	ancB, err := collectAncestors(r, dk, commitB)
	if err != nil {
		return 0, err
	}

	common := make(map[uint64]CommitMeta)
	for id, cmt := range ancA {
		if _, ok := ancB[id]; ok {
			common[id] = cmt
		}
	}
	if len(common) == 0 {
		return 0, ErrInvalid
	}
	if len(common) == 1 {
		for id := range common {
			return id, nil
		}
	}

	notMaximal := make(map[uint64]struct{}, len(common))
	stack := make([]uint64, 0, len(common))
	for _, cmt := range common {
		if cmt.ParentID != 0 {
			stack = append(stack, cmt.ParentID)
		}
		if cmt.MergeParentID != 0 {
			stack = append(stack, cmt.MergeParentID)
		}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if cur == 0 {
				continue
			}
			if _, already := notMaximal[cur]; already {
				continue
			}
			notMaximal[cur] = struct{}{}
			if curMeta, ok := common[cur]; ok {
				if curMeta.ParentID != 0 {
					stack = append(stack, curMeta.ParentID)
				}
				if curMeta.MergeParentID != 0 {
					stack = append(stack, curMeta.MergeParentID)
				}
			}
		}
	}

	var mca []uint64
	for id := range common {
		if _, nonMax := notMaximal[id]; !nonMax {
			mca = append(mca, id)
		}
	}
	if len(mca) > 1 {
		return 0, ErrAmbiguousLCA
	}
	if len(mca) == 0 {
		return 0, ErrInvalid
	}
	return mca[0], nil
}
