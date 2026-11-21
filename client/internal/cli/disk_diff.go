package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	diskDiffTimeout       = 60 * time.Second
	maxDiffPathBytes      = 4096
	maxDiffPageTokenBytes = 4096
	readNodeChunkSize     = 4 << 20
	maxPatchFileBytes     = 32 << 20
	maxMyersEditDist      = 2000

	posixIFMT = 0o170000
	posixIFDR = 0o040000
	posixIFRG = 0o100000
	posixIFLN = 0o120000
)

type diffNodeSummaryJSON struct {
	Ino  uint64 `json:"ino"`
	Mode uint32 `json:"mode"`
	Size uint64 `json:"size"`
}

type diffEntryJSON struct {
	Path     string               `json:"path"`
	Kind     string               `json:"kind"`
	Conflict string               `json:"conflict,omitempty"`
	Base     *diffNodeSummaryJSON `json:"base,omitempty"`
	Target   *diffNodeSummaryJSON `json:"target,omitempty"`
	Source   *diffNodeSummaryJSON `json:"source,omitempty"`
	Patch    string               `json:"patch,omitempty"`
}

type diffSummaryJSON struct {
	Added     uint64 `json:"added"`
	Modified  uint64 `json:"modified"`
	Deleted   uint64 `json:"deleted"`
	Conflicts uint64 `json:"conflicts"`
}

type diskDiffJSON struct {
	SourceDiskID  string          `json:"source_disk_id"`
	TargetDiskID  string          `json:"target_disk_id,omitempty"`
	LCASnapshotID string          `json:"lca_snapshot_id,omitempty"`
	Summary       diffSummaryJSON `json:"summary"`
	Entries       []diffEntryJSON `json:"entries"`
	NextPageToken string          `json:"next_page_token"`
}

var diskDiffSpec = diskSubcommandSpec{
	use:       "diff <source-disk-id>",
	short:     "Show differences and conflicts between disks or against the common ancestor",
	args:      cobra.ExactArgs(1),
	uuidLabel: "source-disk-id",
	spanName:  "cli.disk.diff",
	timeout:   diskDiffTimeout,
	buildAdapter: func(cmd *cobra.Command) diskRequestAdapter {
		a := &diskDiffAdapter{}
		flags := cmd.Flags()
		flags.StringVar(&a.targetRaw, "target", "", "Target virtual disk UUID; omit to compare with the lowest common ancestor")
		flags.StringVar(&a.prefix, "prefix", "", "Path prefix filter up to 4096 bytes")
		flags.BoolVar(&a.conflictsOnly, "conflicts-only", false, "Return only entries with conflicts")
		flags.BoolVarP(&a.patch, "patch", "p", false, "Render unified line diffs, symlink changes, and binary notices")
		flags.IntVar(&a.limit, "limit", 0, "Maximum number of entries per page in [1, 1000]")
		flags.StringVar(&a.pageToken, "page-token", "", "Pagination cursor token")
		return a
	},
}

type diskDiffAdapter struct {
	sourceDiskID  string
	targetRaw     string
	targetDiskID  string
	prefix        string
	conflictsOnly bool
	patch         bool
	limit         int
	pageSize      uint32
	pageToken     string
}

func (a *diskDiffAdapter) finalize(cmd *cobra.Command, posArgs []string) (bool, error) {
	a.sourceDiskID = posArgs[0]
	flags := cmd.Flags()
	if flags.Changed("target") {
		if _, err := parseNonNilUUID(a.targetRaw, "target-disk-id"); err != nil {
			return false, err
		}
		if a.targetRaw == a.sourceDiskID {
			return false, errors.New("target disk cannot be the same as source disk")
		}
		a.targetDiskID = a.targetRaw
	}
	if len(a.prefix) > maxDiffPathBytes {
		return false, fmt.Errorf("--prefix cannot exceed %d bytes", maxDiffPathBytes)
	}
	if strings.Contains(a.prefix, "\x00") {
		return false, errors.New("--prefix cannot contain NUL bytes")
	}
	if len(a.pageToken) > maxDiffPageTokenBytes {
		return false, fmt.Errorf("--page-token cannot exceed %d bytes", maxDiffPageTokenBytes)
	}
	if strings.Contains(a.pageToken, "\x00") {
		return false, errors.New("--page-token cannot contain NUL bytes")
	}
	if flags.Changed("limit") {
		if a.limit < 1 || a.limit > 1000 {
			return false, fmt.Errorf("invalid --limit %d: must be an integer in [1, 1000]", a.limit)
		}
		a.pageSize = uint32(a.limit)
	}
	return false, nil
}

func (a *diskDiffAdapter) spanAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("bowl.source_disk_id", a.sourceDiskID),
	}
	if a.targetDiskID != "" {
		attrs = append(attrs, attribute.String("bowl.target_disk_id", a.targetDiskID))
	}
	return attrs
}

func (a *diskDiffAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskDiffAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	var targetPtr *string
	if a.targetDiskID != "" {
		targetPtr = &a.targetDiskID
	}
	var prefixBytes []byte
	if a.prefix != "" {
		prefixBytes = []byte(a.prefix)
	}

	resp, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId:  a.sourceDiskID,
		TargetDiskId:  targetPtr,
		PathPrefix:    prefixBytes,
		ConflictsOnly: a.conflictsOnly,
		PageSize:      a.pageSize,
		PageToken:     a.pageToken,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			defaultOp:       "diff disks failed",
			checkInvalidArg: true,
		})
	}

	msg := resp.Msg
	lcaSnapshotID := msg.GetLcaSnapshotId()
	entries := msg.GetEntries()
	summary := msg.GetSummary()

	patches := make([]string, len(entries))
	if a.patch {
		for idx, entry := range entries {
			patchStr, patchErr := a.renderEntryPatch(ctx, client, lcaSnapshotID, entry)
			if patchErr != nil {
				return diskOutcome{}, mapDiskRPCError(patchErr, diskRPCErrorPolicy{
					defaultOp: fmt.Sprintf("read diff content for %s failed", string(entry.GetPath())),
				})
			}
			patches[idx] = patchStr
		}
	}

	jsonEntries := make([]diffEntryJSON, 0, len(entries))
	for idx, entry := range entries {
		jsonEntries = append(jsonEntries, diffEntryJSON{
			Path:     string(entry.GetPath()),
			Kind:     diffKindLabel(entry.GetKind()),
			Conflict: conflictKindLabel(entry.GetConflict()),
			Base:     toDiffNodeSummaryJSON(entry.GetBase()),
			Target:   toDiffNodeSummaryJSON(entry.GetTarget()),
			Source:   toDiffNodeSummaryJSON(entry.GetSource()),
			Patch:    patches[idx],
		})
	}

	payload := diskDiffJSON{
		SourceDiskID:  a.sourceDiskID,
		TargetDiskID:  a.targetDiskID,
		LCASnapshotID: lcaSnapshotID,
		Summary: diffSummaryJSON{
			Added:     summary.GetAdded(),
			Modified:  summary.GetModified(),
			Deleted:   summary.GetDeleted(),
			Conflicts: summary.GetConflicts(),
		},
		Entries:       jsonEntries,
		NextPageToken: msg.GetNextPageToken(),
	}

	return diskOutcome{
		jsonPayload: payload,
		renderHuman: func(w io.Writer) {
			renderDiskDiffHuman(w, &payload, a.patch)
		},
	}, nil
}

func toDiffNodeSummaryJSON(s *storagev1.DiffNodeSummary) *diffNodeSummaryJSON {
	if s == nil {
		return nil
	}
	return &diffNodeSummaryJSON{
		Ino:  s.GetIno(),
		Mode: s.GetMode(),
		Size: s.GetSize(),
	}
}

func diffKindLabel(k storagev1.DiffKind) string {
	switch k {
	case storagev1.DiffKind_DIFF_KIND_ADDED:
		return "added"
	case storagev1.DiffKind_DIFF_KIND_MODIFIED:
		return "modified"
	case storagev1.DiffKind_DIFF_KIND_DELETED:
		return "deleted"
	default:
		return "unspecified"
	}
}

func diffKindCode(k string) string {
	switch k {
	case "added":
		return "A"
	case "modified":
		return "M"
	case "deleted":
		return "D"
	default:
		return "?"
	}
}

func conflictKindLabel(c storagev1.ConflictKind) string {
	switch c {
	case storagev1.ConflictKind_CONFLICT_KIND_ADD_ADD:
		return "ADD_ADD"
	case storagev1.ConflictKind_CONFLICT_KIND_CONTENT:
		return "CONTENT"
	case storagev1.ConflictKind_CONFLICT_KIND_MODIFY_DELETE:
		return "MODIFY_DELETE"
	case storagev1.ConflictKind_CONFLICT_KIND_DELETE_MODIFY:
		return "DELETE_MODIFY"
	case storagev1.ConflictKind_CONFLICT_KIND_TREE_DELETE_MODIFY:
		return "TREE_DELETE_MODIFY"
	case storagev1.ConflictKind_CONFLICT_KIND_TYPE:
		return "TYPE"
	case storagev1.ConflictKind_CONFLICT_KIND_DIR_FILE:
		return "DIR_FILE"
	case storagev1.ConflictKind_CONFLICT_KIND_ATTR:
		return "ATTR"
	case storagev1.ConflictKind_CONFLICT_KIND_XATTR:
		return "XATTR"
	case storagev1.ConflictKind_CONFLICT_KIND_XATTR_QUOTA:
		return "XATTR_QUOTA"
	case storagev1.ConflictKind_CONFLICT_KIND_SECURITY_ATTR:
		return "SECURITY_ATTR"
	case storagev1.ConflictKind_CONFLICT_KIND_HARDLINK:
		return "HARDLINK"
	default:
		return ""
	}
}

func renderDiskDiffHuman(w io.Writer, res *diskDiffJSON, withPatch bool) {
	if res.LCASnapshotID != "" {
		fmt.Fprintf(w, "LCA Snapshot: %s\n", res.LCASnapshotID)
	}
	fmt.Fprintf(
		w,
		"Summary:    +%d ~%d -%d (conflicts: %d)\n",
		res.Summary.Added,
		res.Summary.Modified,
		res.Summary.Deleted,
		res.Summary.Conflicts,
	)

	if len(res.Entries) > 0 {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "KIND\tCONFLICT\tPATH")
		for _, entry := range res.Entries {
			conflictCol := entry.Conflict
			if conflictCol == "" {
				conflictCol = "-"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\n", diffKindCode(entry.Kind), conflictCol, entry.Path)
		}
		_ = tw.Flush()
	}

	if withPatch {
		for _, entry := range res.Entries {
			if entry.Patch != "" {
				fmt.Fprintf(w, "\n%s", entry.Patch)
				if !strings.HasSuffix(entry.Patch, "\n") {
					fmt.Fprintln(w)
				}
			}
		}
	}

	if res.NextPageToken != "" {
		fmt.Fprintf(w, "\nNext Page Token: %s\n", res.NextPageToken)
	}
}

type nodeContentSnapshot struct {
	present       bool
	isDir         bool
	isSymlink     bool
	isRegular     bool
	fileType      uint32
	tooLarge      bool
	data          []byte
	symlinkTarget string
}

func (a *diskDiffAdapter) renderEntryPatch(
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	lcaSnapshotID string,
	entry *storagev1.DiffEntry,
) (string, error) {
	relPath := strings.TrimPrefix(string(entry.GetPath()), "/")
	if relPath == "" {
		relPath = "."
	}

	var (
		left  nodeContentSnapshot
		right nodeContentSnapshot
		err   error
	)

	if a.targetDiskID != "" {
		left, err = fetchLiveDiskNode(ctx, client, a.targetDiskID, entry.GetTarget())
		if err != nil {
			return "", err
		}
	} else if entry.GetBase() != nil {
		if lcaSnapshotID == "" {
			return fmt.Sprintf("--- a/%s\n+++ b/%s\nmissing lca_snapshot_id: cannot read ancestor snapshot content\n", relPath, relPath), nil
		}
		left, err = fetchSnapshotNode(ctx, client, a.sourceDiskID, lcaSnapshotID, entry.GetBase())
		if err != nil {
			return "", err
		}
	}

	right, err = fetchLiveDiskNode(ctx, client, a.sourceDiskID, entry.GetSource())
	if err != nil {
		return "", err
	}

	if !left.present && !right.present {
		return "", nil
	}

	if (left.present && left.isDir) || (right.present && right.isDir) {
		if left.isDir != right.isDir {
			return fmt.Sprintf("diff a/%s b/%s\nFile type changed (directory vs non-directory)\n", relPath, relPath), nil
		}
		return "", nil
	}
	if (left.present && !left.isDir && !left.isSymlink && !left.isRegular) ||
		(right.present && !right.isDir && !right.isSymlink && !right.isRegular) {
		if left.present && right.present && left.fileType != right.fileType {
			return fmt.Sprintf("diff a/%s b/%s\nFile type changed\n", relPath, relPath), nil
		}
		return "", nil
	}

	oldLabel := "a/" + relPath
	newLabel := "b/" + relPath
	if !left.present {
		oldLabel = "/dev/null"
	}
	if !right.present {
		newLabel = "/dev/null"
	}

	if left.isSymlink || right.isSymlink {
		if left.present && right.present && left.isSymlink != right.isSymlink {
			return fmt.Sprintf("diff %s %s\nFile type changed (symlink vs regular file)\n", oldLabel, newLabel), nil
		}
		oldLink := left.symlinkTarget
		newLink := right.symlinkTarget
		if oldLink == newLink && left.present == right.present {
			return "", nil
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "--- %s (symlink)\n+++ %s (symlink)\n", oldLabel, newLabel)
		switch {
		case !left.present:
			fmt.Fprintf(&sb, "@@ -0,0 +1,1 @@\n+%s\n", newLink)
		case !right.present:
			fmt.Fprintf(&sb, "@@ -1,1 +0,0 @@\n-%s\n", oldLink)
		default:
			fmt.Fprintf(&sb, "@@ -1,1 +1,1 @@\n-%s\n+%s\n", oldLink, newLink)
		}
		return sb.String(), nil
	}

	if left.tooLarge || right.tooLarge {
		return fmt.Sprintf("Binary files %s and %s differ (exceeds %s patch limit)\n", oldLabel, newLabel, FormatSize(maxPatchFileBytes)), nil
	}
	if bytes.IndexByte(left.data, 0) >= 0 || bytes.IndexByte(right.data, 0) >= 0 {
		return fmt.Sprintf("Binary files %s and %s differ\n", oldLabel, newLabel), nil
	}

	return formatUnifiedDiff(oldLabel, newLabel, left.data, right.data), nil
}

func normalizeFileType(mode uint32) uint32 {
	ft := mode & posixIFMT
	if ft == 0 {
		return posixIFRG
	}
	return ft
}

func isModeDir(mode uint32) bool {
	return mode&posixIFMT == posixIFDR
}

func isModeSymlink(mode uint32) bool {
	return mode&posixIFMT == posixIFLN
}

func isModeRegularOrDefault(mode uint32) bool {
	return normalizeFileType(mode) == posixIFRG
}

func fetchSnapshotNode(
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID, snapshotID string,
	summary *storagev1.DiffNodeSummary,
) (nodeContentSnapshot, error) {
	if summary == nil {
		return nodeContentSnapshot{}, nil
	}
	ft := normalizeFileType(summary.GetMode())
	if isModeDir(summary.GetMode()) {
		return nodeContentSnapshot{present: true, isDir: true, fileType: posixIFDR}, nil
	}
	if !isModeSymlink(summary.GetMode()) && !isModeRegularOrDefault(summary.GetMode()) {
		return nodeContentSnapshot{present: true, fileType: ft}, nil
	}
	if summary.GetSize() > maxPatchFileBytes && !isModeSymlink(summary.GetMode()) {
		return nodeContentSnapshot{present: true, isRegular: true, fileType: posixIFRG, tooLarge: true}, nil
	}

	var buf []byte
	var offset uint64
	for {
		resp, err := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{
			DiskId:     diskID,
			SnapshotId: snapshotID,
			Ino:        summary.GetIno(),
			Offset:     offset,
			Size:       readNodeChunkSize,
		}))
		if err != nil {
			return nodeContentSnapshot{}, err
		}
		msg := resp.Msg
		if isModeSymlink(msg.GetMode()) || isModeSymlink(summary.GetMode()) || len(msg.GetSymlinkTarget()) > 0 {
			return nodeContentSnapshot{
				present:       true,
				isSymlink:     true,
				fileType:      posixIFLN,
				symlinkTarget: string(msg.GetSymlinkTarget()),
			}, nil
		}
		if msg.GetFileSize() > maxPatchFileBytes {
			return nodeContentSnapshot{present: true, isRegular: true, fileType: posixIFRG, tooLarge: true}, nil
		}
		chunk := msg.GetData()
		if len(chunk) > 0 {
			buf = append(buf, chunk...)
			offset += uint64(len(chunk))
		}
		if msg.GetEof() || len(chunk) == 0 || offset >= msg.GetFileSize() {
			break
		}
	}
	return nodeContentSnapshot{
		present:   true,
		isRegular: true,
		fileType:  posixIFRG,
		data:      buf,
	}, nil
}

func fetchLiveDiskNode(
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID string,
	summary *storagev1.DiffNodeSummary,
) (nodeContentSnapshot, error) {
	if summary == nil {
		return nodeContentSnapshot{}, nil
	}
	ft := normalizeFileType(summary.GetMode())
	if isModeDir(summary.GetMode()) {
		return nodeContentSnapshot{present: true, isDir: true, fileType: posixIFDR}, nil
	}
	if isModeSymlink(summary.GetMode()) {
		resp, err := client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
			DiskId: diskID,
			Ino:    summary.GetIno(),
		}))
		if err != nil {
			return nodeContentSnapshot{}, err
		}
		return nodeContentSnapshot{
			present:       true,
			isSymlink:     true,
			fileType:      posixIFLN,
			symlinkTarget: string(resp.Msg.GetTarget()),
		}, nil
	}
	if !isModeRegularOrDefault(summary.GetMode()) {
		return nodeContentSnapshot{present: true, fileType: ft}, nil
	}
	totalSize := summary.GetSize()
	if totalSize > maxPatchFileBytes {
		return nodeContentSnapshot{present: true, isRegular: true, fileType: posixIFRG, tooLarge: true}, nil
	}
	if totalSize == 0 {
		return nodeContentSnapshot{present: true, isRegular: true, fileType: posixIFRG}, nil
	}

	buf := make([]byte, 0, totalSize)
	var offset uint64
	for offset < totalSize {
		want := uint32(min(uint64(readNodeChunkSize), totalSize-offset))
		resp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			DiskId: diskID,
			Ino:    summary.GetIno(),
			Offset: offset,
			Size:   want,
		}))
		if err != nil {
			return nodeContentSnapshot{}, err
		}
		payload := rpc.TakeReadPayload(resp.Msg)
		n := len(payload.Data)
		if n > 0 {
			buf = append(buf, payload.Data...)
			offset += uint64(n)
		}
		payload.Release()
		if n == 0 {
			break
		}
	}
	return nodeContentSnapshot{
		present:   true,
		isRegular: true,
		fileType:  posixIFRG,
		data:      buf,
	}, nil
}

type diffLineOpKind uint8

const (
	diffOpEqual diffLineOpKind = iota
	diffOpDelete
	diffOpInsert
)

type diffLineOp struct {
	kind   diffLineOpKind
	text   string
	oldIdx int
	newIdx int
}

type diffLines struct {
	lines      []string
	endsWithNL bool
}

func splitDiffLines(data []byte) diffLines {
	if len(data) == 0 {
		return diffLines{endsWithNL: true}
	}
	endsWithNL := data[len(data)-1] == '\n'
	trimmed := data
	if endsWithNL {
		trimmed = data[:len(data)-1]
	}
	if len(trimmed) == 0 && endsWithNL {
		return diffLines{lines: []string{""}, endsWithNL: true}
	}
	return diffLines{
		lines:      strings.Split(string(trimmed), "\n"),
		endsWithNL: endsWithNL,
	}
}

func computeLineOps(oldLines, newLines []string) []diffLineOp {
	n := len(oldLines)
	m := len(newLines)

	prefix := 0
	for prefix < n && prefix < m && oldLines[prefix] == newLines[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < n-prefix && suffix < m-prefix && oldLines[n-1-suffix] == newLines[m-1-suffix] {
		suffix++
	}

	var ops []diffLineOp
	for i := range prefix {
		ops = append(ops, diffLineOp{kind: diffOpEqual, text: oldLines[i], oldIdx: i, newIdx: i})
	}

	midOld := oldLines[prefix : n-suffix]
	midNew := newLines[prefix : m-suffix]
	midOps := computeMyersMiddle(midOld, midNew, prefix)
	ops = append(ops, midOps...)

	for i := range suffix {
		oi := n - suffix + i
		ni := m - suffix + i
		ops = append(ops, diffLineOp{kind: diffOpEqual, text: oldLines[oi], oldIdx: oi, newIdx: ni})
	}
	return ops
}

func computeMyersMiddle(a, b []string, baseOffset int) []diffLineOp {
	n := len(a)
	m := len(b)
	if n == 0 && m == 0 {
		return nil
	}
	if n == 0 {
		ops := make([]diffLineOp, m)
		for j, line := range b {
			ops[j] = diffLineOp{kind: diffOpInsert, text: line, oldIdx: baseOffset, newIdx: baseOffset + j}
		}
		return ops
	}
	if m == 0 {
		ops := make([]diffLineOp, n)
		for i, line := range a {
			ops[i] = diffLineOp{kind: diffOpDelete, text: line, oldIdx: baseOffset + i, newIdx: baseOffset}
		}
		return ops
	}

	maxD := min(n+m, maxMyersEditDist)
	v := make([]int, 2*maxD+1)
	offset := maxD
	trace := make([][]int, 0, min(maxD+1, 64))

	found := false
	for d := 0; d <= maxD; d++ {
		snap := make([]int, len(v))
		copy(snap, v)
		trace = append(trace, snap)

		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && v[offset+k-1] < v[offset+k+1]) {
				x = v[offset+k+1]
			} else {
				x = v[offset+k-1] + 1
			}
			y := x - k
			for x < n && y < m && a[x] == b[y] {
				x++
				y++
			}
			v[offset+k] = x
			if x >= n && y >= m {
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	if !found {
		ops := make([]diffLineOp, 0, n+m)
		for i, line := range a {
			ops = append(ops, diffLineOp{kind: diffOpDelete, text: line, oldIdx: baseOffset + i, newIdx: baseOffset})
		}
		for j, line := range b {
			ops = append(ops, diffLineOp{kind: diffOpInsert, text: line, oldIdx: baseOffset + n, newIdx: baseOffset + j})
		}
		return ops
	}

	var revOps []diffLineOp
	x, y := n, m
	for d := len(trace) - 1; d > 0; d-- {
		prevV := trace[d]
		k := x - y
		var prevK int
		if k == -d || (k != d && prevV[offset+k-1] < prevV[offset+k+1]) {
			prevK = k + 1
		} else {
			prevK = k - 1
		}
		prevX := prevV[offset+prevK]
		prevY := prevX - prevK

		for x > prevX && y > prevY {
			x--
			y--
			revOps = append(revOps, diffLineOp{
				kind:   diffOpEqual,
				text:   a[x],
				oldIdx: baseOffset + x,
				newIdx: baseOffset + y,
			})
		}
		if x > prevX {
			x--
			revOps = append(revOps, diffLineOp{
				kind:   diffOpDelete,
				text:   a[x],
				oldIdx: baseOffset + x,
				newIdx: baseOffset + y,
			})
		} else if y > prevY {
			y--
			revOps = append(revOps, diffLineOp{
				kind:   diffOpInsert,
				text:   b[y],
				oldIdx: baseOffset + x,
				newIdx: baseOffset + y,
			})
		}
	}
	for x > 0 && y > 0 {
		x--
		y--
		revOps = append(revOps, diffLineOp{
			kind:   diffOpEqual,
			text:   a[x],
			oldIdx: baseOffset + x,
			newIdx: baseOffset + y,
		})
	}

	for l, r := 0, len(revOps)-1; l < r; l, r = l+1, r-1 {
		revOps[l], revOps[r] = revOps[r], revOps[l]
	}
	return revOps
}

func formatUnifiedDiff(oldLabel, newLabel string, oldData, newData []byte) string {
	if bytes.Equal(oldData, newData) {
		return ""
	}
	oldParsed := splitDiffLines(oldData)
	newParsed := splitDiffLines(newData)
	ops := computeLineOps(oldParsed.lines, newParsed.lines)
	if len(ops) == 0 {
		return ""
	}

	const contextLines = 3
	type hunkRange struct {
		start int
		end   int
	}
	var hunks []hunkRange
	for i := 0; i < len(ops); i++ {
		if ops[i].kind == diffOpEqual {
			continue
		}
		hStart := max(0, i-contextLines)
		hEnd := i + 1
		for hEnd < len(ops) {
			if ops[hEnd].kind != diffOpEqual {
				hEnd++
				continue
			}

			nextChange := -1
			limit := min(len(ops), hEnd+2*contextLines+1)
			for j := hEnd; j < limit; j++ {
				if ops[j].kind != diffOpEqual {
					nextChange = j
					break
				}
			}
			if nextChange >= 0 {
				hEnd = nextChange + 1
			} else {
				hEnd = min(len(ops), hEnd+contextLines)
				break
			}
		}
		hunks = append(hunks, hunkRange{start: hStart, end: hEnd})
		i = hEnd - 1
	}
	if len(hunks) == 0 {
		return ""
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "--- %s\n+++ %s\n", oldLabel, newLabel)
	for _, hr := range hunks {
		hOps := ops[hr.start:hr.end]
		oldCount := 0
		newCount := 0
		oldStart := 0
		newStart := 0
		for _, op := range hOps {
			switch op.kind {
			case diffOpEqual:
				if oldCount == 0 {
					oldStart = op.oldIdx + 1
				}
				if newCount == 0 {
					newStart = op.newIdx + 1
				}
				oldCount++
				newCount++
			case diffOpDelete:
				if oldCount == 0 {
					oldStart = op.oldIdx + 1
				}
				if newCount == 0 {
					newStart = op.newIdx
				}
				oldCount++
			case diffOpInsert:
				if oldCount == 0 {
					oldStart = op.oldIdx
				}
				if newCount == 0 {
					newStart = op.newIdx + 1
				}
				newCount++
			}
		}

		fmt.Fprintf(&sb, "@@ -%d,%d +%d,%d @@\n", oldStart, oldCount, newStart, newCount)
		for _, op := range hOps {
			switch op.kind {
			case diffOpEqual:
				fmt.Fprintf(&sb, " %s\n", op.text)
			case diffOpDelete:
				fmt.Fprintf(&sb, "-%s\n", op.text)
				if !oldParsed.endsWithNL && op.oldIdx == len(oldParsed.lines)-1 {
					sb.WriteString("\\ No newline at end of file\n")
				}
			case diffOpInsert:
				fmt.Fprintf(&sb, "+%s\n", op.text)
				if !newParsed.endsWithNL && op.newIdx == len(newParsed.lines)-1 {
					sb.WriteString("\\ No newline at end of file\n")
				}
			}
		}
	}
	return sb.String()
}
