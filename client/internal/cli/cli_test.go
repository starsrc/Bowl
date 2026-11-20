package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	mu              sync.Mutex
	disks           map[string]*storagev1.Disk
	mounted         map[string]bool
	nextSeq         int
	epochSeq        uint64
	lastAuth        string
	lastTraceparent string
	lastRequestID   string
	deleteCalls     int
	updateCalls     int
	diffCalls       int
	readCommitCalls int
	readCalls       int
	readlinkCalls   int
	mergeCalls      int
	lastDiffReq     *storagev1.DiffDiskRequest
	lastMergeReq    *storagev1.MergeDiskRequest
	diffResponse    *storagev1.DiffDiskResponse
	commitNodes     map[string]*storagev1.ReadCommitNodeResponse
	liveFiles       map[string][]byte
	liveSymlinks    map[string][]byte
	mergeResponse   *storagev1.MergeDiskResponse
	forcedErr       map[string]error
	returnNilDisk   map[string]bool
	mountZeroRoot   bool
	mountZeroEpoch  bool
	mountIDSeq      int
	mountLeaseTTL   uint32
	mountReqs       []*storagev1.MountDiskRequest
	renewReqs       []*storagev1.RenewMountRequest
	unmountReqs     []*storagev1.UnmountDiskRequest
	renewErr        error
	unmountErr      error
	onRenewMount    func(*storagev1.RenewMountRequest) (*storagev1.RenewMountResponse, error)
	onUnmountDisk   func(*storagev1.UnmountDiskRequest)
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{
		disks:         make(map[string]*storagev1.Disk),
		mounted:       make(map[string]bool),
		commitNodes:   make(map[string]*storagev1.ReadCommitNodeResponse),
		liveFiles:     make(map[string][]byte),
		liveSymlinks:  make(map[string][]byte),
		forcedErr:     make(map[string]error),
		returnNilDisk: make(map[string]bool),
	}
}

func (f *fakeStorage) seedDisk(id, name string, maxBytes, usedBytes uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	f.disks[id] = &storagev1.Disk{
		DiskId:     id,
		Name:       name,
		RootIno:    1,
		MaxBytes:   maxBytes,
		UsedBytes:  usedBytes,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
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
		DiskId:     id,
		Name:       req.Msg.GetName(),
		RootIno:    1,
		MaxBytes:   req.Msg.GetMaxBytes(),
		UsedBytes:  0,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	f.disks[id] = d
	return connect.NewResponse(&storagev1.CreateDiskResponse{Disk: d}), nil
}

func (f *fakeStorage) ForkDisk(_ context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ForkDisk"]; err != nil {
		return nil, err
	}
	if f.returnNilDisk["ForkDisk"] {
		return connect.NewResponse(&storagev1.ForkDiskResponse{}), nil
	}
	src, ok := f.disks[req.Msg.GetSourceDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source disk not found"))
	}
	maxBytes := src.GetMaxBytes()
	if req.Msg.MaxBytes != nil {
		maxBytes = req.Msg.GetMaxBytes()
	}
	if maxBytes < src.GetUsedBytes() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("quota shrink below used"))
	}
	f.nextSeq++
	id := fmt.Sprintf("01950000-0000-7000-8000-%012d", f.nextSeq)
	parentID := src.GetDiskId()
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	child := &storagev1.Disk{
		DiskId:       id,
		ParentDiskId: &parentID,
		Name:         req.Msg.GetName(),
		RootIno:      1,
		MaxBytes:     maxBytes,
		UsedBytes:    src.GetUsedBytes(),
		UsedInodes:   src.GetUsedInodes(),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	f.disks[id] = child
	return connect.NewResponse(&storagev1.ForkDiskResponse{Disk: child}), nil
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
		DiskId:       d.GetDiskId(),
		ParentDiskId: d.ParentDiskId,
		Name:         d.GetName(),
		RootIno:      d.GetRootIno(),
		MaxBytes:     d.GetMaxBytes(),
		UsedBytes:    d.GetUsedBytes(),
		UsedInodes:   d.GetUsedInodes(),
		CreatedAt:    d.GetCreatedAt(),
		UpdatedAt:    d.GetUpdatedAt(),
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
	return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
}

func (f *fakeStorage) MountDisk(_ context.Context, req *connect.Request[storagev1.MountDiskRequest]) (*connect.Response[storagev1.MountDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	f.mountReqs = append(f.mountReqs, req.Msg)
	if err := f.forcedErr["MountDisk"]; err != nil {
		return nil, err
	}
	d, ok := f.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	if f.mountZeroRoot {
		return connect.NewResponse(&storagev1.MountDiskResponse{}), nil
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
	return connect.NewResponse(&storagev1.MountDiskResponse{
		Epoch: epoch,
		RootInode: &storagev1.Inode{
			Ino:  d.GetRootIno(),
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

func (f *fakeStorage) UnmountDisk(_ context.Context, req *connect.Request[storagev1.UnmountDiskRequest]) (*connect.Response[storagev1.UnmountDiskResponse], error) {
	f.mu.Lock()
	f.recordHeaders(req.Header())
	f.unmountReqs = append(f.unmountReqs, req.Msg)
	hook := f.onUnmountDisk
	unmountErr := f.unmountErr
	if unmountErr == nil {
		unmountErr = f.forcedErr["UnmountDisk"]
	}
	f.mu.Unlock()

	if hook != nil {
		hook(req.Msg)
	}
	if unmountErr != nil {
		return nil, unmountErr
	}
	return connect.NewResponse(&storagev1.UnmountDiskResponse{}), nil
}

func (f *fakeStorage) getMountReqs() []*storagev1.MountDiskRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*storagev1.MountDiskRequest, len(f.mountReqs))
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

func (f *fakeStorage) getUnmountReqs() []*storagev1.UnmountDiskRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*storagev1.UnmountDiskRequest, len(f.unmountReqs))
	copy(out, f.unmountReqs)
	return out
}

func fakeCommitNodeKey(diskID, commitID string, ino uint64) string {
	return fmt.Sprintf("%s|%s|%d", diskID, commitID, ino)
}

func fakeLiveNodeKey(diskID string, ino uint64) string {
	return fmt.Sprintf("%s|%d", diskID, ino)
}

func (f *fakeStorage) seedCommitFile(diskID, commitID string, ino uint64, mode uint32, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitNodes[fakeCommitNodeKey(diskID, commitID, ino)] = &storagev1.ReadCommitNodeResponse{
		Mode:     mode,
		FileSize: uint64(len(data)),
		Data:     append([]byte(nil), data...),
	}
}

func (f *fakeStorage) seedCommitSymlink(diskID, commitID string, ino uint64, target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitNodes[fakeCommitNodeKey(diskID, commitID, ino)] = &storagev1.ReadCommitNodeResponse{
		Mode:          0o120777,
		FileSize:      uint64(len(target)),
		SymlinkTarget: []byte(target),
		Eof:           true,
	}
}

func (f *fakeStorage) seedLiveFile(diskID string, ino uint64, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.liveFiles[fakeLiveNodeKey(diskID, ino)] = append([]byte(nil), data...)
}

func (f *fakeStorage) seedLiveSymlink(diskID string, ino uint64, target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.liveSymlinks[fakeLiveNodeKey(diskID, ino)] = []byte(target)
}

func (f *fakeStorage) DiffDisk(_ context.Context, req *connect.Request[storagev1.DiffDiskRequest]) (*connect.Response[storagev1.DiffDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diffCalls++
	f.recordHeaders(req.Header())
	f.lastDiffReq = req.Msg
	if err := f.forcedErr["DiffDisk"]; err != nil {
		return nil, err
	}
	if f.diffResponse == nil {
		return connect.NewResponse(&storagev1.DiffDiskResponse{
			Summary: &storagev1.DiffSummary{},
		}), nil
	}
	return connect.NewResponse(f.diffResponse), nil
}

func (f *fakeStorage) ReadCommitNode(_ context.Context, req *connect.Request[storagev1.ReadCommitNodeRequest]) (*connect.Response[storagev1.ReadCommitNodeResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCommitCalls++
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
	data, ok := f.liveFiles[fakeLiveNodeKey(req.Msg.GetDiskId(), req.Msg.GetIno())]
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
	target, ok := f.liveSymlinks[fakeLiveNodeKey(req.Msg.GetDiskId(), req.Msg.GetIno())]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("live symlink not found"))
	}
	return connect.NewResponse(&storagev1.ReadlinkResponse{
		Target: append([]byte(nil), target...),
	}), nil
}

func (f *fakeStorage) MergeDisk(_ context.Context, req *connect.Request[storagev1.MergeDiskRequest]) (*connect.Response[storagev1.MergeDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mergeCalls++
	f.recordHeaders(req.Header())
	f.lastMergeReq = req.Msg
	if err := f.forcedErr["MergeDisk"]; err != nil {
		return nil, err
	}
	if f.mergeResponse != nil {
		return connect.NewResponse(f.mergeResponse), nil
	}
	target, ok := f.disks[req.Msg.GetTargetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("target disk not found"))
	}
	if _, srcOK := f.disks[req.Msg.GetSourceDiskId()]; !srcOK {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source disk not found"))
	}
	if req.Msg.GetDryRun() {
		maxB := target.GetMaxBytes()
		if req.Msg.MaxBytes != nil {
			maxB = req.Msg.GetMaxBytes()
		}
		return connect.NewResponse(&storagev1.MergeDiskResponse{
			LcaCommitId:        "01950000-0000-7000-8000-000000000090",
			EstimatedUsedBytes: target.GetUsedBytes() + 4096,
			TargetMaxBytes:     maxB,
		}), nil
	}
	return connect.NewResponse(&storagev1.MergeDiskResponse{
		Disk:               target,
		LcaCommitId:        "01950000-0000-7000-8000-000000000090",
		MergeCommitId:      "01950000-0000-7000-8000-000000000091",
		EstimatedUsedBytes: target.GetUsedBytes(),
		TargetMaxBytes:     target.GetMaxBytes(),
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
	t.Fatalf("未采集到期望 Span %q", name)
	return nil
}

func requireSpanStringAttr(t *testing.T, sp *tracetest.SpanStub, key, want string) {
	t.Helper()
	for _, attr := range sp.Attributes {
		if string(attr.Key) == key {
			require.Equal(t, want, attr.Value.AsString())
			return
		}
	}
	t.Fatalf("Span %s 缺少属性 %q", sp.Name, key)
}

func TestExecute_GivenNoArgs_PrintsUsageToStderrAndReturns2(t *testing.T) {

	code, out, errOut := runCLI()

	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Usage:")
	require.Contains(t, errOut, "bowl [command]")
}

func TestExecute_GivenUnknownCommand_PrintsErrorAndReturns2(t *testing.T) {

	for _, cmdName := range []string{"unknown-cmd", "sandbox"} {
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
		for _, wantCmd := range []string{"disk", "mount", "umount", "stats", "version", "help", "completion"} {
			require.Contains(t, out, wantCmd, "args=%v", args)
		}
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
		{args: []string{"help", "disk", "update"}, wantSubstr: "update, resize"},
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
