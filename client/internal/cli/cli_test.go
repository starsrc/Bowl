package cli

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/version"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	testDiskID1 = "01950000-0000-7000-8000-000000000001"
	testDiskID2 = "01950000-0000-7000-8000-000000000002"
	testNilUUID = "00000000-0000-0000-0000-000000000000"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func runCLI(args ...string) (exitCode int, stdout, stderr string) {
	var outBuf, errBuf bytes.Buffer
	code := Execute(args, &outBuf, &errBuf)
	return code, outBuf.String(), errBuf.String()
}

func withMountInfoContent(t *testing.T, content string) {
	t.Helper()
	prev := mountInfoPath
	path := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	mountInfoPath = path
	t.Cleanup(func() { mountInfoPath = prev })
}

func withMissingMountInfo(t *testing.T) {
	t.Helper()
	prev := mountInfoPath
	mountInfoPath = filepath.Join(t.TempDir(), "nonexistent-mountinfo")
	t.Cleanup(func() { mountInfoPath = prev })
}

func withUnreadableMountInfo(t *testing.T) {
	t.Helper()
	prev := mountInfoPath
	mountInfoPath = t.TempDir()
	t.Cleanup(func() { mountInfoPath = prev })
}

func withDiskStdin(t *testing.T, r io.Reader) {
	t.Helper()
	prev := diskStdin
	diskStdin = r
	t.Cleanup(func() { diskStdin = prev })
}

func withTempMountMetaDirs(t *testing.T, dirs ...string) {
	t.Helper()
	prev := mountMetaDirs
	mountMetaDirs = func() []string { return dirs }
	t.Cleanup(func() { mountMetaDirs = prev })
}

type errReader struct{ err error }

func (r errReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

type fakeStorage struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu                  sync.Mutex
	disks               map[string]*storagev1.Disk
	branches            map[string]map[string]*storagev1.Branch
	commits             map[string]map[uint64]*storagev1.Commit
	mounted             map[string]bool
	mountLeases         map[string]map[string]*storagev1.MountLease
	nextSeq             int
	nextCommitID        uint64
	epochSeq            uint64
	lastAuth            string
	lastTraceparent     string
	lastRequestID       string
	deleteCalls         int
	updateCalls         int
	forkBranchCalls     int
	listBranchesCalls   int
	deleteBranchCalls   int
	resetBranchCalls    int
	mergeBranchCalls    int
	createCommitCalls   int
	listCommitsCalls    int
	deleteCommitCalls   int
	pruneCommitsCalls   int
	diffBranchesCalls   int
	diffCommitsCalls    int
	diffWorkCalls       int
	readCommitNodeCalls int
	readCalls           int
	readlinkCalls       int
	listMountsCalls     int
	evictCalls          int
	lastDeleteReq       *storagev1.DeleteDiskRequest
	lastForkBranchReq   *storagev1.ForkBranchRequest
	lastDeleteBranchReq *storagev1.DeleteBranchRequest
	lastResetBranchReq  *storagev1.ResetBranchRequest
	lastMergeBranchReq  *storagev1.MergeBranchRequest
	lastCreateCommitReq *storagev1.CreateCommitRequest
	lastListCommitsReq  *storagev1.ListCommitsRequest
	lastDeleteCommitReq *storagev1.DeleteCommitRequest
	lastPruneCommitsReq *storagev1.PruneCommitsRequest
	lastDiffBranchesReq *storagev1.DiffBranchesRequest
	lastDiffCommitsReq  *storagev1.DiffCommitsRequest
	lastDiffWorkReq     *storagev1.DiffWorkRequest
	lastListMountsReq   *storagev1.ListMountsRequest
	lastEvictReq        *storagev1.EvictMountsRequest
	diffResponse        *storagev1.DiffResponse
	commitNodes         map[string]*storagev1.ReadCommitNodeResponse
	liveFiles           map[string][]byte
	liveSymlinks        map[string][]byte
	mergeResponse       *storagev1.MergeBranchResponse
	forcedErr           map[string]error
	returnNilDisk       map[string]bool
	returnNilCommit     bool
	returnNilBranch     bool
	mountZeroRoot       bool
	mountZeroEpoch      bool
	mountIDSeq          int
	mountLeaseTTL       uint32
	mountReqs           []*storagev1.MountBranchRequest
	renewReqs           []*storagev1.RenewMountRequest
	unmountReqs         []*storagev1.UnmountBranchRequest
	renewErr            error
	unmountErr          error
	onMountBranch       func(context.Context, *storagev1.MountBranchRequest) error
	onRenewMount        func(*storagev1.RenewMountRequest) (*storagev1.RenewMountResponse, error)
	onUnmountBranch     func(*storagev1.UnmountBranchRequest)
	pruneReclaimedCount uint32
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{
		disks:         make(map[string]*storagev1.Disk),
		branches:      make(map[string]map[string]*storagev1.Branch),
		commits:       make(map[string]map[uint64]*storagev1.Commit),
		mounted:       make(map[string]bool),
		mountLeases:   make(map[string]map[string]*storagev1.MountLease),
		commitNodes:   make(map[string]*storagev1.ReadCommitNodeResponse),
		liveFiles:     make(map[string][]byte),
		liveSymlinks:  make(map[string][]byte),
		forcedErr:     make(map[string]error),
		returnNilDisk: make(map[string]bool),
	}
}

func (f *fakeStorage) seedMountLease(diskID, branch, mountID string, readOnly bool, epoch uint64, expiresAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if branch == "" {
		branch = fusefs.DefaultBranch
	}
	if f.mountLeases[diskID] == nil {
		f.mountLeases[diskID] = make(map[string]*storagev1.MountLease)
	}
	f.mountLeases[diskID][mountID] = &storagev1.MountLease{
		MountId:   mountID,
		Branch:    branch,
		ReadOnly:  readOnly,
		Epoch:     epoch,
		MountedAt: timestamppb.New(expiresAt.Add(-30 * time.Second).UTC()),
		ExpiresAt: timestamppb.New(expiresAt.UTC()),
	}
}

func (f *fakeStorage) seedDisk(id, name string, maxBytes, usedBytes uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	f.disks[id] = &storagev1.Disk{
		DiskId:        id,
		Name:          name,
		DefaultBranch: fusefs.DefaultBranch,
		MaxBytes:      maxBytes,
		UsedBytes:     usedBytes,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if f.branches[id] == nil {
		f.branches[id] = make(map[string]*storagev1.Branch)
	}
	if _, exists := f.branches[id][fusefs.DefaultBranch]; !exists {
		f.branches[id][fusefs.DefaultBranch] = &storagev1.Branch{
			DiskId:       id,
			Name:         fusefs.DefaultBranch,
			HeadCommitId: 1,
			WriterEpoch:  0,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
	}
	if f.commits[id] == nil {
		f.commits[id] = make(map[uint64]*storagev1.Commit)
	}
	if _, exists := f.commits[id][1]; !exists {
		f.commits[id][1] = &storagev1.Commit{
			DiskId:    id,
			CommitId:  1,
			Kind:      storagev1.CommitKind_COMMIT_KIND_INIT,
			Message:   "init",
			CreatedAt: now,
		}
		if f.nextCommitID < 1 {
			f.nextCommitID = 1
		}
	}
}

func (f *fakeStorage) seedBranch(diskID, name string, headCommitID, writerEpoch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 5, 0, 0, time.UTC))
	if f.branches[diskID] == nil {
		f.branches[diskID] = make(map[string]*storagev1.Branch)
	}
	f.branches[diskID][name] = &storagev1.Branch{
		DiskId:       diskID,
		Name:         name,
		HeadCommitId: headCommitID,
		WriterEpoch:  writerEpoch,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

func (f *fakeStorage) seedCommit(c *storagev1.Commit) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commits[c.GetDiskId()] == nil {
		f.commits[c.GetDiskId()] = make(map[uint64]*storagev1.Commit)
	}
	f.commits[c.GetDiskId()][c.GetCommitId()] = c
	if c.GetCommitId() > f.nextCommitID {
		f.nextCommitID = c.GetCommitId()
	}
}

func (f *fakeStorage) recordHeaders(header http.Header) {
	f.lastAuth = header.Get("Authorization")
	f.lastTraceparent = header.Get("Traceparent")
	f.lastRequestID = header.Get("X-Request-Id")
}

func (f *fakeStorage) CreateDisk(_ context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["CreateDisk"]; err != nil {
		return nil, err
	}
	if f.returnNilDisk["CreateDisk"] {
		return connect.NewResponse(&storagev1.CreateDiskResponse{}), nil
	}
	f.nextSeq++
	id := fmt.Sprintf("01950000-0000-7000-8000-%012d", f.nextSeq)
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	d := &storagev1.Disk{
		DiskId:        id,
		Name:          req.Msg.GetName(),
		DefaultBranch: fusefs.DefaultBranch,
		MaxBytes:      req.Msg.GetMaxBytes(),
		UsedBytes:     0,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	f.disks[id] = d
	f.branches[id] = map[string]*storagev1.Branch{
		fusefs.DefaultBranch: {
			DiskId:       id,
			Name:         fusefs.DefaultBranch,
			HeadCommitId: 1,
			CreatedAt:    now,
			UpdatedAt:    now,
		},
	}
	return connect.NewResponse(&storagev1.CreateDiskResponse{Disk: d}), nil
}

func (f *fakeStorage) GetDisk(_ context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["GetDisk"]; err != nil {
		return nil, err
	}
	if f.returnNilDisk["GetDisk"] {
		return connect.NewResponse(&storagev1.GetDiskResponse{}), nil
	}
	d, ok := f.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	return connect.NewResponse(&storagev1.GetDiskResponse{Disk: d}), nil
}

func (f *fakeStorage) ListDisks(_ context.Context, req *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ListDisks"]; err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(f.disks))
	for k := range f.disks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	start := 0
	if req.Msg.GetPageToken() != "" {
		for idx, k := range keys {
			if k == req.Msg.GetPageToken() {
				start = idx
				break
			}
		}
	}
	limit := int(req.Msg.GetPageSize())
	if limit <= 0 {
		limit = 50
	}
	end := min(start+limit, len(keys))
	out := make([]*storagev1.Disk, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, f.disks[k])
	}
	var nextToken string
	if end < len(keys) {
		nextToken = keys[end]
	}
	return connect.NewResponse(&storagev1.ListDisksResponse{
		Disks:         out,
		NextPageToken: nextToken,
	}), nil
}

func (f *fakeStorage) UpdateDisk(_ context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls++
	f.recordHeaders(req.Header())
	if err := f.forcedErr["UpdateDisk"]; err != nil {
		return nil, err
	}
	if f.returnNilDisk["UpdateDisk"] {
		return connect.NewResponse(&storagev1.UpdateDiskResponse{}), nil
	}
	d, ok := f.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	if req.Msg.MaxBytes != nil && req.Msg.GetMaxBytes() < d.GetUsedBytes() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("shrink below used"))
	}
	updated := &storagev1.Disk{
		DiskId:        d.GetDiskId(),
		Name:          d.GetName(),
		DefaultBranch: d.GetDefaultBranch(),
		MaxBytes:      d.GetMaxBytes(),
		UsedBytes:     d.GetUsedBytes(),
		CreatedAt:     d.GetCreatedAt(),
		UpdatedAt:     d.GetUpdatedAt(),
	}
	if req.Msg.Name != nil {
		updated.Name = req.Msg.GetName()
	}
	if req.Msg.MaxBytes != nil {
		updated.MaxBytes = req.Msg.GetMaxBytes()
	}
	f.disks[d.GetDiskId()] = updated
	return connect.NewResponse(&storagev1.UpdateDiskResponse{Disk: updated}), nil
}

func (f *fakeStorage) DeleteDisk(_ context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	f.lastDeleteReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["DeleteDisk"]; err != nil {
		return nil, err
	}
	id := req.Msg.GetDiskId()
	if f.mounted[id] {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("active mount session"))
	}
	if _, ok := f.disks[id]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	delete(f.disks, id)
	delete(f.branches, id)
	delete(f.commits, id)
	return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
}

func (f *fakeStorage) MountBranch(ctx context.Context, req *connect.Request[storagev1.MountBranchRequest]) (*connect.Response[storagev1.MountBranchResponse], error) {
	f.mu.Lock()
	f.recordHeaders(req.Header())
	f.mountReqs = append(f.mountReqs, req.Msg)
	hook := f.onMountBranch
	f.mu.Unlock()

	if hook != nil {
		if err := hook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.forcedErr["MountBranch"]; err != nil {
		return nil, err
	}
	if _, ok := f.disks[req.Msg.GetDiskId()]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	if f.mountZeroRoot {
		return connect.NewResponse(&storagev1.MountBranchResponse{}), nil
	}
	var epoch uint64
	if !f.mountZeroEpoch && !req.Msg.GetReadOnly() {
		f.epochSeq++
		epoch = f.epochSeq
	}
	f.mountIDSeq++
	mountID := fmt.Sprintf("01950000-0000-7000-9000-%012d", f.mountIDSeq)
	ttl := f.mountLeaseTTL
	if ttl == 0 {
		ttl = 30
	}
	return connect.NewResponse(&storagev1.MountBranchResponse{
		Epoch: epoch,
		RootInode: &storagev1.Inode{
			Ino:  1,
			Attr: &storagev1.InodeAttribute{Mode: 0o040755},
		},
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
		MountId:             mountID,
		LeaseTtlSeconds:     ttl,
	}), nil
}

func (f *fakeStorage) RenewMount(_ context.Context, req *connect.Request[storagev1.RenewMountRequest]) (*connect.Response[storagev1.RenewMountResponse], error) {
	f.mu.Lock()
	f.recordHeaders(req.Header())
	f.renewReqs = append(f.renewReqs, req.Msg)
	hook := f.onRenewMount
	renewErr := f.renewErr
	forced := f.forcedErr["RenewMount"]
	ttl := f.mountLeaseTTL
	f.mu.Unlock()

	if hook != nil {
		resp, err := hook(req.Msg)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(resp), nil
	}
	if renewErr != nil {
		return nil, renewErr
	}
	if forced != nil {
		return nil, forced
	}
	if ttl == 0 {
		ttl = 30
	}
	return connect.NewResponse(&storagev1.RenewMountResponse{
		LeaseTtlSeconds: ttl,
	}), nil
}

func (f *fakeStorage) UnmountBranch(_ context.Context, req *connect.Request[storagev1.UnmountBranchRequest]) (*connect.Response[storagev1.UnmountBranchResponse], error) {
	f.mu.Lock()
	f.recordHeaders(req.Header())
	f.unmountReqs = append(f.unmountReqs, req.Msg)
	hook := f.onUnmountBranch
	unmountErr := f.unmountErr
	if unmountErr == nil {
		unmountErr = f.forcedErr["UnmountBranch"]
	}
	f.mu.Unlock()

	if hook != nil {
		hook(req.Msg)
	}
	if unmountErr != nil {
		return nil, unmountErr
	}
	return connect.NewResponse(&storagev1.UnmountBranchResponse{}), nil
}

func (f *fakeStorage) ListMounts(_ context.Context, req *connect.Request[storagev1.ListMountsRequest]) (*connect.Response[storagev1.ListMountsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listMountsCalls++
	f.lastListMountsReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ListMounts"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	leases := f.mountLeases[diskID]
	ids := make([]string, 0, len(leases))
	for id, l := range leases {
		if req.Msg.GetBranch() != "" && l.GetBranch() != req.Msg.GetBranch() {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*storagev1.MountLease, 0, len(ids))
	for _, id := range ids {
		out = append(out, leases[id])
	}
	return connect.NewResponse(&storagev1.ListMountsResponse{
		Mounts: out,
	}), nil
}

func (f *fakeStorage) EvictMounts(_ context.Context, req *connect.Request[storagev1.EvictMountsRequest]) (*connect.Response[storagev1.EvictMountsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictCalls++
	f.recordHeaders(req.Header())
	f.lastEvictReq = req.Msg
	if err := f.forcedErr["EvictMounts"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	leases := f.mountLeases[diskID]
	branchFilter := req.Msg.GetBranch()
	var evicted uint32
	updatedBranchNames := make(map[string]struct{})
	if req.Msg.MountId != nil {
		mountID := req.Msg.GetMountId()
		lease, ok := leases[mountID]
		if ok && (branchFilter == "" || lease.GetBranch() == branchFilter) {
			delete(leases, mountID)
			evicted = 1
			if !lease.GetReadOnly() {
				updatedBranchNames[lease.GetBranch()] = struct{}{}
			}
		}
	} else {
		for id, lease := range leases {
			if branchFilter != "" && lease.GetBranch() != branchFilter {
				continue
			}
			delete(leases, id)
			evicted++
			if !lease.GetReadOnly() {
				updatedBranchNames[lease.GetBranch()] = struct{}{}
			}
		}
	}
	if len(leases) == 0 {
		delete(f.mountLeases, diskID)
	}
	var updated []*storagev1.Branch
	names := make([]string, 0, len(updatedBranchNames))
	for name := range updatedBranchNames {
		names = append(names, name)
	}
	sort.Strings(names)
	now := timestamppb.New(time.Date(2026, 11, 20, 12, 1, 0, 0, time.UTC))
	for _, name := range names {
		f.epochSeq++
		if b := f.branches[diskID][name]; b != nil {
			b.WriterEpoch = f.epochSeq
			b.UpdatedAt = now
			updated = append(updated, b)
		} else {
			updated = append(updated, &storagev1.Branch{
				DiskId:      diskID,
				Name:        name,
				WriterEpoch: f.epochSeq,
				CreatedAt:   now,
				UpdatedAt:   now,
			})
		}
	}
	return connect.NewResponse(&storagev1.EvictMountsResponse{
		EvictedCount:    evicted,
		UpdatedBranches: updated,
	}), nil
}

func (f *fakeStorage) ForkBranch(_ context.Context, req *connect.Request[storagev1.ForkBranchRequest]) (*connect.Response[storagev1.ForkBranchResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forkBranchCalls++
	f.lastForkBranchReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ForkBranch"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	src, ok := f.branches[diskID][req.Msg.GetSourceBranch()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source branch not found"))
	}
	if _, exists := f.branches[diskID][req.Msg.GetNewBranch()]; exists {
		return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("branch already exists"))
	}
	if f.returnNilBranch {
		return connect.NewResponse(&storagev1.ForkBranchResponse{}), nil
	}
	f.nextCommitID++
	now := timestamppb.New(time.Date(2026, 11, 22, 10, 0, int(f.nextCommitID), 0, time.UTC))
	commit := &storagev1.Commit{
		DiskId:    diskID,
		CommitId:  f.nextCommitID,
		ParentId:  src.GetHeadCommitId(),
		Kind:      storagev1.CommitKind_COMMIT_KIND_FORK,
		Message:   req.Msg.GetMessage(),
		CreatedAt: now,
	}
	branch := &storagev1.Branch{
		DiskId:       diskID,
		Name:         req.Msg.GetNewBranch(),
		HeadCommitId: commit.GetCommitId(),
		WriterEpoch:  0,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	f.branches[diskID][branch.GetName()] = branch
	if f.commits[diskID] == nil {
		f.commits[diskID] = make(map[uint64]*storagev1.Commit)
	}
	f.commits[diskID][commit.GetCommitId()] = commit
	return connect.NewResponse(&storagev1.ForkBranchResponse{
		Commit: commit,
		Branch: branch,
	}), nil
}

func (f *fakeStorage) ListBranches(_ context.Context, req *connect.Request[storagev1.ListBranchesRequest]) (*connect.Response[storagev1.ListBranchesResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listBranchesCalls++
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ListBranches"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	branchMap := f.branches[diskID]
	names := make([]string, 0, len(branchMap))
	for name := range branchMap {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*storagev1.Branch, 0, len(names))
	for _, name := range names {
		out = append(out, branchMap[name])
	}
	return connect.NewResponse(&storagev1.ListBranchesResponse{
		Branches: out,
	}), nil
}

func (f *fakeStorage) DeleteBranch(_ context.Context, req *connect.Request[storagev1.DeleteBranchRequest]) (*connect.Response[storagev1.DeleteBranchResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteBranchCalls++
	f.lastDeleteBranchReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["DeleteBranch"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	d, ok := f.disks[diskID]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	if req.Msg.GetBranch() == d.GetDefaultBranch() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("cannot delete default branch"))
	}
	if _, exists := f.branches[diskID][req.Msg.GetBranch()]; !exists {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("branch not found"))
	}
	delete(f.branches[diskID], req.Msg.GetBranch())
	return connect.NewResponse(&storagev1.DeleteBranchResponse{}), nil
}

func (f *fakeStorage) ResetBranch(_ context.Context, req *connect.Request[storagev1.ResetBranchRequest]) (*connect.Response[storagev1.ResetBranchResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resetBranchCalls++
	f.lastResetBranchReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ResetBranch"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	b, ok := f.branches[diskID][req.Msg.GetBranch()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("branch not found"))
	}
	if _, ok := f.commits[diskID][req.Msg.GetTargetCommitId()]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("target commit not found"))
	}
	b.HeadCommitId = req.Msg.GetTargetCommitId()
	b.UpdatedAt = timestamppb.New(time.Date(2026, 11, 22, 11, 0, 0, 0, time.UTC))
	return connect.NewResponse(&storagev1.ResetBranchResponse{Branch: b}), nil
}

func (f *fakeStorage) MergeBranch(_ context.Context, req *connect.Request[storagev1.MergeBranchRequest]) (*connect.Response[storagev1.MergeBranchResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mergeBranchCalls++
	f.lastMergeBranchReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["MergeBranch"]; err != nil {
		return nil, err
	}
	if f.mergeResponse != nil {
		return connect.NewResponse(f.mergeResponse), nil
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	target, ok := f.branches[diskID][req.Msg.GetTargetBranch()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("target branch not found"))
	}
	src, ok := f.branches[diskID][req.Msg.GetSourceBranch()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source branch not found"))
	}
	if req.Msg.GetDryRun() {
		return connect.NewResponse(&storagev1.MergeBranchResponse{
			LcaCommitId: 1,
		}), nil
	}
	f.nextCommitID++
	now := timestamppb.New(time.Date(2026, 11, 22, 12, 0, int(f.nextCommitID), 0, time.UTC))
	commit := &storagev1.Commit{
		DiskId:        diskID,
		CommitId:      f.nextCommitID,
		ParentId:      target.GetHeadCommitId(),
		MergeParentId: src.GetHeadCommitId(),
		Kind:          storagev1.CommitKind_COMMIT_KIND_MERGE,
		Message:       req.Msg.GetMessage(),
		CreatedAt:     now,
	}
	target.HeadCommitId = commit.GetCommitId()
	target.UpdatedAt = now
	if f.commits[diskID] == nil {
		f.commits[diskID] = make(map[uint64]*storagev1.Commit)
	}
	f.commits[diskID][commit.GetCommitId()] = commit
	return connect.NewResponse(&storagev1.MergeBranchResponse{
		LcaCommitId: 1,
		CommitId:    commit.GetCommitId(),
		WriterEpoch: target.GetWriterEpoch(),
		Branch:      target,
		Commit:      commit,
	}), nil
}

func (f *fakeStorage) CreateCommit(_ context.Context, req *connect.Request[storagev1.CreateCommitRequest]) (*connect.Response[storagev1.CreateCommitResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCommitCalls++
	f.lastCreateCommitReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["CreateCommit"]; err != nil {
		return nil, err
	}
	if f.returnNilCommit {
		return connect.NewResponse(&storagev1.CreateCommitResponse{}), nil
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	branch, ok := f.branches[diskID][req.Msg.GetBranch()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("branch not found"))
	}
	f.nextCommitID++
	now := timestamppb.New(time.Date(2026, 11, 21, 10, 0, int(f.nextCommitID), 0, time.UTC))
	commit := &storagev1.Commit{
		DiskId:    diskID,
		CommitId:  f.nextCommitID,
		ParentId:  branch.GetHeadCommitId(),
		Kind:      storagev1.CommitKind_COMMIT_KIND_MANUAL,
		Message:   req.Msg.GetMessage(),
		CreatedAt: now,
	}
	branch.HeadCommitId = commit.GetCommitId()
	if f.commits[diskID] == nil {
		f.commits[diskID] = make(map[uint64]*storagev1.Commit)
	}
	f.commits[diskID][commit.GetCommitId()] = commit
	return connect.NewResponse(&storagev1.CreateCommitResponse{Commit: commit}), nil
}

func (f *fakeStorage) ListCommits(_ context.Context, req *connect.Request[storagev1.ListCommitsRequest]) (*connect.Response[storagev1.ListCommitsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCommitsCalls++
	f.lastListCommitsReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ListCommits"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	var ids []uint64
	for id, c := range f.commits[diskID] {
		if req.Msg.GetKind() != storagev1.CommitKind_COMMIT_KIND_UNSPECIFIED && c.GetKind() != req.Msg.GetKind() {
			continue
		}
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b uint64) int {
		return cmp.Compare(b, a)
	})

	start := 0
	if req.Msg.GetPageToken() != "" {
		tokID, parseErr := strconv.ParseUint(req.Msg.GetPageToken(), 10, 64)
		if parseErr == nil {
			for idx, id := range ids {
				if id == tokID {
					start = idx
					break
				}
			}
		}
	}
	limit := int(req.Msg.GetPageSize())
	if limit <= 0 {
		limit = 50
	}
	end := min(start+limit, len(ids))
	out := make([]*storagev1.Commit, 0, end-start)
	for _, id := range ids[start:end] {
		out = append(out, f.commits[diskID][id])
	}
	var nextToken string
	if end < len(ids) {
		nextToken = strconv.FormatUint(ids[end], 10)
	}
	return connect.NewResponse(&storagev1.ListCommitsResponse{
		Commits:       out,
		NextPageToken: nextToken,
	}), nil
}

func (f *fakeStorage) DeleteCommit(_ context.Context, req *connect.Request[storagev1.DeleteCommitRequest]) (*connect.Response[storagev1.DeleteCommitResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCommitCalls++
	f.lastDeleteCommitReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["DeleteCommit"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	if _, ok := f.commits[diskID][req.Msg.GetCommitId()]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("commit not found"))
	}
	delete(f.commits[diskID], req.Msg.GetCommitId())
	return connect.NewResponse(&storagev1.DeleteCommitResponse{}), nil
}

func (f *fakeStorage) PruneCommits(_ context.Context, req *connect.Request[storagev1.PruneCommitsRequest]) (*connect.Response[storagev1.PruneCommitsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruneCommitsCalls++
	f.lastPruneCommitsReq = req.Msg
	f.recordHeaders(req.Header())
	if err := f.forcedErr["PruneCommits"]; err != nil {
		return nil, err
	}
	diskID := req.Msg.GetDiskId()
	if _, ok := f.disks[diskID]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	return connect.NewResponse(&storagev1.PruneCommitsResponse{
		ReclaimedCount: f.pruneReclaimedCount,
	}), nil
}

func (f *fakeStorage) getMountReqs() []*storagev1.MountBranchRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*storagev1.MountBranchRequest, len(f.mountReqs))
	copy(out, f.mountReqs)
	return out
}

func (f *fakeStorage) getRenewReqs() []*storagev1.RenewMountRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*storagev1.RenewMountRequest, len(f.renewReqs))
	copy(out, f.renewReqs)
	return out
}

func (f *fakeStorage) getUnmountReqs() []*storagev1.UnmountBranchRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*storagev1.UnmountBranchRequest, len(f.unmountReqs))
	copy(out, f.unmountReqs)
	return out
}

func fakeCommitNodeKey(diskID string, commitID, ino uint64) string {
	return fmt.Sprintf("%s|%d|%d", diskID, commitID, ino)
}

func fakeLiveBranchNodeKey(diskID, branch string, ino uint64) string {
	return fmt.Sprintf("%s|%s|%d", diskID, branch, ino)
}

func (f *fakeStorage) seedCommitFile(diskID string, commitID, ino uint64, mode uint32, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitNodes[fakeCommitNodeKey(diskID, commitID, ino)] = &storagev1.ReadCommitNodeResponse{
		Mode:     mode,
		FileSize: uint64(len(data)),
		Data:     append([]byte(nil), data...),
	}
}

func (f *fakeStorage) seedCommitSymlink(diskID string, commitID, ino uint64, target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitNodes[fakeCommitNodeKey(diskID, commitID, ino)] = &storagev1.ReadCommitNodeResponse{
		Mode:          0o120777,
		FileSize:      uint64(len(target)),
		SymlinkTarget: []byte(target),
		Eof:           true,
	}
}

func (f *fakeStorage) seedLiveFile(diskID, branch string, ino uint64, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.liveFiles[fakeLiveBranchNodeKey(diskID, branch, ino)] = append([]byte(nil), data...)
}

func (f *fakeStorage) seedLiveSymlink(diskID, branch string, ino uint64, target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.liveSymlinks[fakeLiveBranchNodeKey(diskID, branch, ino)] = []byte(target)
}

func (f *fakeStorage) DiffBranches(_ context.Context, req *connect.Request[storagev1.DiffBranchesRequest]) (*connect.Response[storagev1.DiffResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diffBranchesCalls++
	f.recordHeaders(req.Header())
	f.lastDiffBranchesReq = req.Msg
	if err := f.forcedErr["DiffBranches"]; err != nil {
		return nil, err
	}
	if f.diffResponse == nil {
		return connect.NewResponse(&storagev1.DiffResponse{Summary: &storagev1.DiffSummary{}}), nil
	}
	return connect.NewResponse(f.diffResponse), nil
}

func (f *fakeStorage) DiffCommits(_ context.Context, req *connect.Request[storagev1.DiffCommitsRequest]) (*connect.Response[storagev1.DiffResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diffCommitsCalls++
	f.recordHeaders(req.Header())
	f.lastDiffCommitsReq = req.Msg
	if err := f.forcedErr["DiffCommits"]; err != nil {
		return nil, err
	}
	if f.diffResponse == nil {
		return connect.NewResponse(&storagev1.DiffResponse{Summary: &storagev1.DiffSummary{}}), nil
	}
	return connect.NewResponse(f.diffResponse), nil
}

func (f *fakeStorage) DiffWork(_ context.Context, req *connect.Request[storagev1.DiffWorkRequest]) (*connect.Response[storagev1.DiffResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diffWorkCalls++
	f.recordHeaders(req.Header())
	f.lastDiffWorkReq = req.Msg
	if err := f.forcedErr["DiffWork"]; err != nil {
		return nil, err
	}
	if f.diffResponse == nil {
		return connect.NewResponse(&storagev1.DiffResponse{Summary: &storagev1.DiffSummary{}}), nil
	}
	return connect.NewResponse(f.diffResponse), nil
}

func (f *fakeStorage) ReadCommitNode(_ context.Context, req *connect.Request[storagev1.ReadCommitNodeRequest]) (*connect.Response[storagev1.ReadCommitNodeResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCommitNodeCalls++
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ReadCommitNode"]; err != nil {
		return nil, err
	}
	node, ok := f.commitNodes[fakeCommitNodeKey(req.Msg.GetDiskId(), req.Msg.GetCommitId(), req.Msg.GetIno())]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("commit node not found"))
	}
	if len(node.GetSymlinkTarget()) > 0 || node.GetMode()&0o170000 == 0o120000 {
		return connect.NewResponse(&storagev1.ReadCommitNodeResponse{
			Mode:          node.GetMode(),
			FileSize:      node.GetFileSize(),
			Eof:           true,
			SymlinkTarget: append([]byte(nil), node.GetSymlinkTarget()...),
		}), nil
	}
	data := node.GetData()
	off := int(req.Msg.GetOffset())
	if off >= len(data) {
		return connect.NewResponse(&storagev1.ReadCommitNodeResponse{
			Mode:     node.GetMode(),
			FileSize: uint64(len(data)),
			Eof:      true,
		}), nil
	}
	end := len(data)
	if sz := int(req.Msg.GetSize()); sz > 0 && off+sz < end {
		end = off + sz
	}
	return connect.NewResponse(&storagev1.ReadCommitNodeResponse{
		Mode:     node.GetMode(),
		FileSize: uint64(len(data)),
		Data:     append([]byte(nil), data[off:end]...),
		Eof:      end >= len(data),
	}), nil
}

func (f *fakeStorage) Read(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCalls++
	f.recordHeaders(req.Header())
	if err := f.forcedErr["Read"]; err != nil {
		return nil, err
	}
	data, ok := f.liveFiles[fakeLiveBranchNodeKey(req.Msg.GetDiskId(), req.Msg.GetBranch(), req.Msg.GetIno())]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("live file not found"))
	}
	off := int(req.Msg.GetOffset())
	if off >= len(data) {
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}
	end := len(data)
	if sz := int(req.Msg.GetSize()); sz > 0 && off+sz < end {
		end = off + sz
	}
	return connect.NewResponse(&storagev1.ReadResponse{
		Data: append([]byte(nil), data[off:end]...),
	}), nil
}

func (f *fakeStorage) Readlink(_ context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readlinkCalls++
	f.recordHeaders(req.Header())
	if err := f.forcedErr["Readlink"]; err != nil {
		return nil, err
	}
	target, ok := f.liveSymlinks[fakeLiveBranchNodeKey(req.Msg.GetDiskId(), req.Msg.GetBranch(), req.Msg.GetIno())]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("live symlink not found"))
	}
	return connect.NewResponse(&storagev1.ReadlinkResponse{
		Target: append([]byte(nil), target...),
	}), nil
}

func newFakeStorageHTTPClient(f *fakeStorage) *http.Client {
	path, connectHandler := storagev1connect.NewStorageServiceHandler(f)
	mux := http.NewServeMux()
	mux.Handle(path, connectHandler)
	return &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			resp := rec.Result()
			resp.Request = req
			return resp, nil
		}),
	}
}

func withDiskStorageFake(t *testing.T, f *fakeStorage) {
	t.Helper()
	prev := diskHTTPClient
	diskHTTPClient = newFakeStorageHTTPClient(f)
	t.Cleanup(func() { diskHTTPClient = prev })
}

func withMountStorageFake(t *testing.T, f *fakeStorage) {
	t.Helper()
	prev := mountHTTPClient
	mountHTTPClient = newFakeStorageHTTPClient(f)
	t.Cleanup(func() { mountHTTPClient = prev })
}

func requireSpanByName(t *testing.T, spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	t.Helper()
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	t.Fatalf("expected span %q not captured", name)
	return nil
}

func TestExecute_GivenNoArgs_PrintsUsageToStderrAndReturns2(t *testing.T) {
	code, out, errOut := runCLI()

	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Usage:")
	require.Contains(t, errOut, "bowl [command]")
}

func TestExecute_GivenUnknownCommand_PrintsErrorAndReturns2(t *testing.T) {
	for _, cmdName := range []string{"unknown-cmd", "sandbox", "snapshot"} {
		code, out, errOut := runCLI(cmdName)
		require.Equal(t, 2, code, "cmd=%s", cmdName)
		require.Empty(t, out, "cmd=%s", cmdName)
		require.Contains(t, errOut, fmt.Sprintf(`Error: unknown command %q for "bowl"`, cmdName))
		require.Contains(t, errOut, "Available Commands:")
	}
}

func TestExecute_GivenVersionFlags_PrintsVersionAndReturns0(t *testing.T) {
	want := "bowl version " + version.Version + "\n"

	code, out, errOut := runCLI("version")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, want, out)

	code, out, errOut = runCLI("--version")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, want, out)

	code, out, errOut = runCLI("-v")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, want, out)
}

func TestExecute_GivenVersionExtraArgsOrInvalidFlag_Returns2(t *testing.T) {
	for _, args := range [][]string{
		{"version", "extra"},
		{"version", "--bogus"},
		{"--version=bad"},
		{"-v=bad"},
	} {
		code, out, errOut := runCLI(args...)
		require.Equal(t, 2, code, "args=%v stderr=%s", args, errOut)
		require.Empty(t, out, "args=%v", args)
		require.Contains(t, errOut, "Error:", "args=%v", args)
	}
}

func TestExecute_GivenHelpFlags_PrintsNativeScopeAndReturns0(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"--help"}, {"-h"}} {
		code, out, errOut := runCLI(args...)
		require.Equal(t, 0, code, "args=%v stderr=%s", args, errOut)
		require.Empty(t, errOut)
		require.Contains(t, out, "Available Commands:")
		for _, wantCmd := range []string{"disk", "branch", "commit", "lease", "diff", "mount", "umount", "stats", "version", "help", "completion"} {
			require.Contains(t, out, wantCmd, "args=%v", args)
		}
		require.NotContains(t, out, "snapshot", "args=%v", args)
		require.NotContains(t, out, "sandbox", "args=%v", args)
	}
}

func TestExecute_GivenHelpSubcommand_PrintsTargetHelpAndReturns0(t *testing.T) {
	cases := []struct {
		args       []string
		wantSubstr string
	}{
		{args: []string{"help", "disk"}, wantSubstr: "bowl disk [command]"},
		{args: []string{"help", "disk", "create"}, wantSubstr: "bowl disk create"},
		{args: []string{"help", "disk", "list"}, wantSubstr: "list, ls"},
		{args: []string{"help", "disk", "get"}, wantSubstr: "get, view"},
		{args: []string{"help", "disk", "update"}, wantSubstr: "update, edit"},
		{args: []string{"help", "disk", "delete"}, wantSubstr: "delete, rm"},
		{args: []string{"help", "branch"}, wantSubstr: "bowl branch [command]"},
		{args: []string{"help", "branch", "list"}, wantSubstr: "list, ls"},
		{args: []string{"help", "branch", "create"}, wantSubstr: "create, fork"},
		{args: []string{"help", "branch", "delete"}, wantSubstr: "delete, rm"},
		{args: []string{"help", "branch", "reset"}, wantSubstr: "bowl branch reset <branch>"},
		{args: []string{"help", "branch", "merge"}, wantSubstr: "bowl branch merge <source-branch>"},
		{args: []string{"help", "commit"}, wantSubstr: "bowl commit [command]"},
		{args: []string{"help", "commit", "create"}, wantSubstr: "bowl commit create"},
		{args: []string{"help", "commit", "list"}, wantSubstr: "list, ls"},
		{args: []string{"help", "commit", "delete"}, wantSubstr: "delete, rm"},
		{args: []string{"help", "commit", "prune"}, wantSubstr: "bowl commit prune"},
		{args: []string{"help", "lease"}, wantSubstr: "bowl lease [command]"},
		{args: []string{"help", "lease", "list"}, wantSubstr: "list, ls"},
		{args: []string{"help", "lease", "evict"}, wantSubstr: "bowl lease evict"},
		{args: []string{"help", "diff"}, wantSubstr: "bowl diff [<target>..<source>]"},
		{args: []string{"help", "mount"}, wantSubstr: "bowl mount <disk-id> <mountpoint>"},
		{args: []string{"help", "umount"}, wantSubstr: "umount, unmount"},
		{args: []string{"help", "stats"}, wantSubstr: "bowl stats [mountpoint]"},
		{args: []string{"help", "version"}, wantSubstr: "bowl version"},
		{args: []string{"help", "completion"}, wantSubstr: "bowl completion"},
	}

	for _, tc := range cases {
		code, out, errOut := runCLI(tc.args...)
		require.Equal(t, 0, code, "args=%v stderr=%s", tc.args, errOut)
		require.Empty(t, errOut, "args=%v", tc.args)
		require.Contains(t, out, tc.wantSubstr, "args=%v", tc.args)
	}
}

func TestExecute_GivenCompletionCommand_GeneratesScriptsAndRejectsExtraArgsOrBadFlags(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		code, out, errOut := runCLI("completion", shell)
		require.Equal(t, 0, code, "shell=%s stderr=%s", shell, errOut)
		require.Empty(t, errOut, "shell=%s", shell)
		require.NotEmpty(t, out, "shell=%s", shell)
		require.Contains(t, out, "bowl", "shell=%s", shell)
	}

	for _, badArgs := range [][]string{
		{"completion", "bash", "extra"},
		{"completion", "--bogus"},
	} {
		code, out, errOut := runCLI(badArgs...)
		require.Equal(t, 2, code, "args=%v stderr=%s", badArgs, errOut)
		require.Empty(t, out, "args=%v", badArgs)
		require.Contains(t, errOut, "Error:", "args=%v", badArgs)
	}
}
