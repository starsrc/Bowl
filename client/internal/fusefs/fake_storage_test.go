package fusefs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

var _ storagev1connect.StorageServiceClient = (*fakeStorage)(nil)

type fakeNode struct {
	inode  *storagev1.Inode
	data   []byte
	link   []byte
	xattrs map[string][]byte
}

type fakeStorageOption func(*fakeStorage)

func withRootIno(rootIno uint64) fakeStorageOption {
	return func(s *fakeStorage) {
		s.rootIno = rootIno
	}
}

func withTTL(entryTTL, attrTTL float64) fakeStorageOption {
	return func(s *fakeStorage) {
		s.entryTTL = entryTTL
		s.attrTTL = attrTTL
	}
}

// fakeStorage 是面向 StorageServiceClient 接缝的内存存储适配器，
// 默认对未覆盖控制面接口执行 Fail Fast 契约拦截。
type fakeStorage struct {
	tb       testing.TB
	mu       sync.Mutex
	rootIno  uint64
	nextIno  uint64
	diskID   string
	epoch    uint64
	entryTTL float64
	attrTTL  float64
	statFS   storagev1.StatFSResponse

	nodes   map[uint64]*fakeNode
	entries map[uint64]map[string]uint64

	// 各外部存储调用次数计数器
	lookupCalls        atomic.Int64
	getAttrCalls       atomic.Int64
	setAttrCalls       atomic.Int64
	mknodCalls         atomic.Int64
	mkdirCalls         atomic.Int64
	symlinkCalls       atomic.Int64
	readlinkCalls      atomic.Int64
	linkCalls          atomic.Int64
	unlinkCalls        atomic.Int64
	rmdirCalls         atomic.Int64
	renameCalls        atomic.Int64
	createCalls        atomic.Int64
	readCalls          atomic.Int64
	writeCalls         atomic.Int64
	fsyncCalls         atomic.Int64
	fsyncDirCalls      atomic.Int64
	readDirPlusCalls   atomic.Int64
	fallocateCalls     atomic.Int64
	setXattrCalls      atomic.Int64
	getXattrCalls      atomic.Int64
	listXattrCalls     atomic.Int64
	removeXattrCalls   atomic.Int64
	statFSCalls        atomic.Int64
	reclaimOrphanCalls atomic.Int64

	// 观测到的上下文元数据
	lastReqID       atomic.Uint64
	capturedTraceMu sync.Mutex
	capturedSpans   map[string][]trace.SpanContext

	// 故障注入与自定义钩子
	lookupHook        func(context.Context, *storagev1.LookupRequest) (*storagev1.LookupResponse, error)
	getAttrHook       func(context.Context, *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error)
	setAttrHook       func(context.Context, *storagev1.SetAttrRequest) error
	readHook          func(context.Context, *storagev1.ReadRequest) ([]byte, error)
	writeHook         func(context.Context, *storagev1.WriteRequest) (uint32, error)
	fsyncHook         func(context.Context, *storagev1.FsyncRequest) error
	unlinkHook        func(context.Context, *storagev1.UnlinkRequest) error
	linkHook          func(context.Context, *storagev1.LinkRequest) error
	readDirPlusHook   func(context.Context, *storagev1.ReadDirPlusRequest) ([]*storagev1.DirEntryPlus, error)
	reclaimOrphanHook func(context.Context, *storagev1.ReclaimOrphanRequest) error
}

func newFakeStorage(tb testing.TB, opts ...fakeStorageOption) *fakeStorage {
	tb.Helper()
	s := &fakeStorage{
		tb:       tb,
		rootIno:  1,
		nextIno:  100,
		diskID:   "disk-test",
		epoch:    1,
		entryTTL: 1.0,
		attrTTL:  1.0,
		statFS: storagev1.StatFSResponse{
			Blocks:  1024 * 1024,
			Bfree:   512 * 1024,
			Bavail:  512 * 1024,
			Files:   65536,
			Ffree:   65000,
			Bsize:   4096,
			Frsize:  4096,
			Namelen: fusefs.MaxFilenameBytes,
		},
		nodes:         make(map[uint64]*fakeNode),
		entries:       make(map[uint64]map[string]uint64),
		capturedSpans: make(map[string][]trace.SpanContext),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.nextIno <= s.rootIno {
		s.nextIno = s.rootIno + 100
	}
	now := timestamppb.Now()
	s.nodes[s.rootIno] = &fakeNode{
		inode: &storagev1.Inode{
			Ino:        s.rootIno,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    syscall.S_IFDIR | 0o755,
				Nlink:   2,
				Blksize: 4096,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		},
		xattrs: make(map[string][]byte),
	}
	s.entries[s.rootIno] = make(map[string]uint64)
	return s
}

func (s *fakeStorage) rootInodeClone() *storagev1.Inode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneProtoInode(s.nodes[s.rootIno].inode)
}

func (s *fakeStorage) failFast(op string) error {
	s.tb.Errorf("契约违例：fakeStorage 收到未实现的控制面调用 %s", op)
	return connect.NewError(connect.CodeUnimplemented, fmt.Errorf("unimplemented RPC: %s", op))
}

func (s *fakeStorage) recordCtx(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return connect.NewError(connect.CodeDeadlineExceeded, err)
		}
		return connect.NewError(connect.CodeCanceled, err)
	}
	if reqID, ok := rpc.RequestIDFromContext(ctx); ok && reqID != 0 {
		s.lastReqID.Store(reqID)
	}
	sc := trace.SpanFromContext(ctx).SpanContext()
	if sc.IsValid() {
		s.capturedTraceMu.Lock()
		s.capturedSpans[op] = append(s.capturedSpans[op], sc)
		s.capturedTraceMu.Unlock()
	}
	return nil
}

func (s *fakeStorage) spansFor(op string) []trace.SpanContext {
	s.capturedTraceMu.Lock()
	defer s.capturedTraceMu.Unlock()
	return slices.Clone(s.capturedSpans[op])
}

func (s *fakeStorage) checkDiskID(op, diskID string) error {
	if s.diskID != "" && diskID != s.diskID {
		s.tb.Errorf("契约违例：调用 %s 携带非法 DiskId，want=%q got=%q", op, s.diskID, diskID)
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unexpected disk_id %q", diskID))
	}
	return nil
}

func (s *fakeStorage) checkEpoch(op string, epoch uint64) error {
	if s.epoch != 0 && epoch != s.epoch {
		s.tb.Errorf("契约违例：写调用 %s 携带非法 Epoch，want=%d got=%d", op, s.epoch, epoch)
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("unexpected epoch %d", epoch))
	}
	return nil
}

func cloneProtoInode(src *storagev1.Inode) *storagev1.Inode {
	if src == nil {
		return nil
	}
	cloned, _ := proto.Clone(src).(*storagev1.Inode)
	return cloned
}

func errnoError(code connect.Code, reason string) error {
	ce := connect.NewError(code, errors.New(reason))
	if detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "bowl.storage",
	}); err == nil {
		ce.AddDetail(detail)
	}
	return ce
}

func (s *fakeStorage) allocInoLocked() uint64 {
	s.nextIno++
	return s.nextIno
}

func (s *fakeStorage) seedFile(parentIno uint64, name string, mode uint32, data []byte) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ino := s.allocInoLocked()
	now := timestamppb.Now()
	node := &fakeNode{
		inode: &storagev1.Inode{
			Ino:        ino,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    syscall.S_IFREG | (mode & 0o7777),
				Nlink:   1,
				Size:    uint64(len(data)),
				Blocks:  (uint64(len(data)) + 511) / 512,
				Blksize: 4096,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		},
		data:   slices.Clone(data),
		xattrs: make(map[string][]byte),
	}
	s.nodes[ino] = node
	if s.entries[parentIno] == nil {
		s.entries[parentIno] = make(map[string]uint64)
	}
	s.entries[parentIno][name] = ino
	return ino
}

func (s *fakeStorage) seedDir(parentIno uint64, name string, mode uint32) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ino := s.allocInoLocked()
	now := timestamppb.Now()
	node := &fakeNode{
		inode: &storagev1.Inode{
			Ino:        ino,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    syscall.S_IFDIR | (mode & 0o7777),
				Nlink:   2,
				Blksize: 4096,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		},
		xattrs: make(map[string][]byte),
	}
	s.nodes[ino] = node
	s.entries[ino] = make(map[string]uint64)
	if s.entries[parentIno] == nil {
		s.entries[parentIno] = make(map[string]uint64)
	}
	s.entries[parentIno][name] = ino
	if p := s.nodes[parentIno]; p != nil {
		p.inode.Attr.Nlink++
	}
	return ino
}

func (s *fakeStorage) fileBytes(ino uint64) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.nodes[ino]
	if n == nil {
		return nil, false
	}
	return slices.Clone(n.data), true
}

func (s *fakeStorage) hasNode(ino uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[ino] != nil
}

func (s *fakeStorage) nodeNlink(ino uint64) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.nodes[ino]; n != nil && n.inode.Attr != nil {
		return n.inode.Attr.Nlink
	}
	return 0
}

func (s *fakeStorage) isAncestorLocked(candidateAncestor, node uint64) bool {
	if candidateAncestor == node {
		return true
	}
	visited := make(map[uint64]bool)
	queue := []uint64{candidateAncestor}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if visited[cur] {
			continue
		}
		visited[cur] = true
		for _, child := range s.entries[cur] {
			if child == node {
				return true
			}
			if cn := s.nodes[child]; cn != nil && cn.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR {
				queue = append(queue, child)
			}
		}
	}
	return false
}

// Lookup 实现单级目录项查找。
func (s *fakeStorage) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	s.lookupCalls.Add(1)
	if err := s.recordCtx(ctx, "Lookup"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Lookup", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if s.lookupHook != nil {
		resp, err := s.lookupHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		if resp != nil {
			return connect.NewResponse(resp), nil
		}
	}
	name := string(req.Msg.GetName())
	if len(name) > fusefs.MaxFilenameBytes {
		return nil, errnoError(connect.CodeInvalidArgument, "ENAMETOOLONG")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.nodes[req.Msg.GetParentIno()]
	if parent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if parent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	ino := s.entries[req.Msg.GetParentIno()][name]
	node := s.nodes[ino]
	if ino == 0 || node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode:               cloneProtoInode(node.inode),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

// GetAttr 获取节点属性。
func (s *fakeStorage) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	s.getAttrCalls.Add(1)
	if err := s.recordCtx(ctx, "GetAttr"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("GetAttr", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if s.getAttrHook != nil {
		resp, err := s.getAttrHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		if resp != nil {
			return connect.NewResponse(resp), nil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              cloneProtoInode(node.inode),
		AttrTimeoutSeconds: s.attrTTL,
	}), nil
}

// SetAttr 按需部分更新节点属性。
func (s *fakeStorage) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	s.setAttrCalls.Add(1)
	if err := s.recordCtx(ctx, "SetAttr"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("SetAttr", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("SetAttr", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	if s.setAttrHook != nil {
		if err := s.setAttrHook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	if req.Msg.GetAtimeNow() && req.Msg.GetAtime() != nil {
		s.tb.Errorf("契约违例：SetAttrRequest 同时设置 AtimeNow 与 Atime")
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	if req.Msg.GetMtimeNow() && req.Msg.GetMtime() != nil {
		s.tb.Errorf("契约违例：SetAttrRequest 同时设置 MtimeNow 与 Mtime")
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	attr := node.inode.Attr
	if req.Msg.Mode != nil {
		ftype := attr.Mode & syscall.S_IFMT
		attr.Mode = ftype | (req.Msg.GetMode() & 0o7777)
	}
	if req.Msg.Uid != nil {
		attr.Uid = req.Msg.GetUid()
	}
	if req.Msg.Gid != nil {
		attr.Gid = req.Msg.GetGid()
	}
	if req.Msg.Size != nil {
		if attr.Mode&syscall.S_IFMT == syscall.S_IFDIR {
			return nil, errnoError(connect.CodeFailedPrecondition, "EISDIR")
		}
		target := int(req.Msg.GetSize())
		if target < len(node.data) {
			node.data = node.data[:target]
		} else if target > len(node.data) {
			grown := make([]byte, target)
			copy(grown, node.data)
			node.data = grown
		}
		attr.Size = uint64(target)
		attr.Blocks = (attr.Size + 511) / 512
	}
	if req.Msg.Flags != nil {
		attr.Flags = req.Msg.GetFlags()
	}
	now := timestamppb.Now()
	if req.Msg.GetAtimeNow() {
		attr.Atime = now
	} else if req.Msg.GetAtime() != nil {
		attr.Atime = req.Msg.GetAtime()
	}
	if req.Msg.GetMtimeNow() {
		attr.Mtime = now
	} else if req.Msg.GetMtime() != nil {
		attr.Mtime = req.Msg.GetMtime()
	}
	attr.Ctime = now
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              cloneProtoInode(node.inode),
		AttrTimeoutSeconds: s.attrTTL,
	}), nil
}

// Mknod 创建常规或特殊文件节点。
func (s *fakeStorage) Mknod(ctx context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	s.mknodCalls.Add(1)
	if err := s.recordCtx(ctx, "Mknod"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Mknod", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Mknod", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	name := string(req.Msg.GetName())
	if len(name) > fusefs.MaxFilenameBytes {
		return nil, errnoError(connect.CodeInvalidArgument, "ENAMETOOLONG")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.nodes[req.Msg.GetParentIno()]
	if parent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if parent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	if s.entries[req.Msg.GetParentIno()][name] != 0 {
		return nil, errnoError(connect.CodeAlreadyExists, "EEXIST")
	}
	mode := req.Msg.GetMode()
	if mode&syscall.S_IFMT == 0 {
		mode |= syscall.S_IFREG
	}
	ino := s.allocInoLocked()
	now := timestamppb.Now()
	node := &fakeNode{
		inode: &storagev1.Inode{
			Ino:        ino,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    mode,
				Nlink:   1,
				Uid:     req.Msg.GetUid(),
				Gid:     req.Msg.GetGid(),
				Rdev:    req.Msg.GetRdev(),
				Blksize: 4096,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		},
		xattrs: make(map[string][]byte),
	}
	s.nodes[ino] = node
	s.entries[req.Msg.GetParentIno()][name] = ino
	return connect.NewResponse(&storagev1.MknodResponse{
		Inode:               cloneProtoInode(node.inode),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

// Mkdir 创建子目录。
func (s *fakeStorage) Mkdir(ctx context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	s.mkdirCalls.Add(1)
	if err := s.recordCtx(ctx, "Mkdir"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Mkdir", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Mkdir", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	name := string(req.Msg.GetName())
	if len(name) > fusefs.MaxFilenameBytes {
		return nil, errnoError(connect.CodeInvalidArgument, "ENAMETOOLONG")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.nodes[req.Msg.GetParentIno()]
	if parent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if parent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	if s.entries[req.Msg.GetParentIno()][name] != 0 {
		return nil, errnoError(connect.CodeAlreadyExists, "EEXIST")
	}
	ino := s.allocInoLocked()
	now := timestamppb.Now()
	node := &fakeNode{
		inode: &storagev1.Inode{
			Ino:        ino,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    syscall.S_IFDIR | (req.Msg.GetMode() & 0o7777),
				Nlink:   2,
				Uid:     req.Msg.GetUid(),
				Gid:     req.Msg.GetGid(),
				Blksize: 4096,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		},
		xattrs: make(map[string][]byte),
	}
	s.nodes[ino] = node
	s.entries[ino] = make(map[string]uint64)
	s.entries[req.Msg.GetParentIno()][name] = ino
	parent.inode.Attr.Nlink++
	parent.inode.Attr.Mtime = now
	parent.inode.Attr.Ctime = now
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode:               cloneProtoInode(node.inode),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

// Symlink 创建符号链接。
func (s *fakeStorage) Symlink(ctx context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
	s.symlinkCalls.Add(1)
	if err := s.recordCtx(ctx, "Symlink"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Symlink", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Symlink", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	name := string(req.Msg.GetName())
	if len(name) > fusefs.MaxFilenameBytes {
		return nil, errnoError(connect.CodeInvalidArgument, "ENAMETOOLONG")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.nodes[req.Msg.GetParentIno()]
	if parent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if parent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	if s.entries[req.Msg.GetParentIno()][name] != 0 {
		return nil, errnoError(connect.CodeAlreadyExists, "EEXIST")
	}
	ino := s.allocInoLocked()
	now := timestamppb.Now()
	target := slices.Clone(req.Msg.GetTarget())
	node := &fakeNode{
		inode: &storagev1.Inode{
			Ino:        ino,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    syscall.S_IFLNK | 0o777,
				Nlink:   1,
				Size:    uint64(len(target)),
				Uid:     req.Msg.GetUid(),
				Gid:     req.Msg.GetGid(),
				Blksize: 4096,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		},
		link:   target,
		xattrs: make(map[string][]byte),
	}
	s.nodes[ino] = node
	s.entries[req.Msg.GetParentIno()][name] = ino
	parent.inode.Attr.Mtime = now
	parent.inode.Attr.Ctime = now
	return connect.NewResponse(&storagev1.SymlinkResponse{
		Inode:               cloneProtoInode(node.inode),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

// Readlink 读取符号链接目标路径。
func (s *fakeStorage) Readlink(ctx context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	s.readlinkCalls.Add(1)
	if err := s.recordCtx(ctx, "Readlink"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Readlink", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if node.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFLNK {
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	return connect.NewResponse(&storagev1.ReadlinkResponse{
		Target: slices.Clone(node.link),
	}), nil
}

// Link 创建硬链接。
func (s *fakeStorage) Link(ctx context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	s.linkCalls.Add(1)
	if err := s.recordCtx(ctx, "Link"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Link", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Link", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	if s.linkHook != nil {
		if err := s.linkHook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	name := string(req.Msg.GetNewName())
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.nodes[req.Msg.GetIno()]
	if src == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if src.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR {
		return nil, errnoError(connect.CodePermissionDenied, "EPERM")
	}
	parent := s.nodes[req.Msg.GetNewParentIno()]
	if parent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if parent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	if s.entries[req.Msg.GetNewParentIno()][name] != 0 {
		return nil, errnoError(connect.CodeAlreadyExists, "EEXIST")
	}
	now := timestamppb.Now()
	src.inode.Attr.Nlink++
	src.inode.Attr.Ctime = now
	s.entries[req.Msg.GetNewParentIno()][name] = req.Msg.GetIno()
	parent.inode.Attr.Mtime = now
	parent.inode.Attr.Ctime = now
	return connect.NewResponse(&storagev1.LinkResponse{
		Inode:               cloneProtoInode(src.inode),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

// Unlink 删除目录项并维护孤儿或硬链接计数。
func (s *fakeStorage) Unlink(ctx context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	s.unlinkCalls.Add(1)
	if err := s.recordCtx(ctx, "Unlink"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Unlink", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Unlink", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	if s.unlinkHook != nil {
		if err := s.unlinkHook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	name := string(req.Msg.GetName())
	if name == "." || name == ".." {
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.nodes[req.Msg.GetParentIno()]
	if parent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if parent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	ino := s.entries[req.Msg.GetParentIno()][name]
	if ino == 0 {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	node := s.nodes[ino]
	if node != nil && node.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "EISDIR")
	}
	delete(s.entries[req.Msg.GetParentIno()], name)
	now := timestamppb.Now()
	parent.inode.Attr.Mtime = now
	parent.inode.Attr.Ctime = now
	if node != nil {
		if node.inode.Attr.Nlink > 0 {
			node.inode.Attr.Nlink--
		}
		node.inode.Attr.Ctime = now
		if node.inode.Attr.Nlink == 0 && !req.Msg.GetOpenInClient() {
			delete(s.nodes, ino)
		}
	}
	return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
}

// Rmdir 移除空目录。
func (s *fakeStorage) Rmdir(ctx context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
	s.rmdirCalls.Add(1)
	if err := s.recordCtx(ctx, "Rmdir"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Rmdir", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Rmdir", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	name := string(req.Msg.GetName())
	if name == "." {
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	if name == ".." {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTEMPTY")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.nodes[req.Msg.GetParentIno()]
	if parent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if parent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	ino := s.entries[req.Msg.GetParentIno()][name]
	if ino == 0 {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	node := s.nodes[ino]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if node.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	if len(s.entries[ino]) > 0 {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTEMPTY")
	}
	delete(s.entries[req.Msg.GetParentIno()], name)
	delete(s.entries, ino)
	delete(s.nodes, ino)
	if parent.inode.Attr.Nlink > 0 {
		parent.inode.Attr.Nlink--
	}
	now := timestamppb.Now()
	parent.inode.Attr.Mtime = now
	parent.inode.Attr.Ctime = now
	return connect.NewResponse(&storagev1.RmdirResponse{}), nil
}

// Rename 执行原子重命名、覆盖或交换。
func (s *fakeStorage) Rename(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	s.renameCalls.Add(1)
	if err := s.recordCtx(ctx, "Rename"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Rename", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Rename", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	oldName := string(req.Msg.GetOldName())
	newName := string(req.Msg.GetNewName())
	if oldName == "." || oldName == ".." || newName == "." || newName == ".." {
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	flags := req.Msg.GetFlags()
	if flags&^(uint32(0x1|0x2)) != 0 || (flags&0x1 != 0 && flags&0x2 != 0) {
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	oldParent := s.nodes[req.Msg.GetOldParentIno()]
	newParent := s.nodes[req.Msg.GetNewParentIno()]
	if oldParent == nil || newParent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if oldParent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR ||
		newParent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	srcIno := s.entries[req.Msg.GetOldParentIno()][oldName]
	if srcIno == 0 || s.nodes[srcIno] == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	srcNode := s.nodes[srcIno]
	srcIsDir := srcNode.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
	dstIno := s.entries[req.Msg.GetNewParentIno()][newName]
	dstNode := s.nodes[dstIno]

	if flags&0x2 != 0 {
		if dstIno == 0 || dstNode == nil {
			return nil, errnoError(connect.CodeNotFound, "ENOENT")
		}
		dstIsDir := dstNode.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
		if (srcIsDir && s.isAncestorLocked(srcIno, req.Msg.GetNewParentIno())) ||
			(dstIsDir && s.isAncestorLocked(dstIno, req.Msg.GetOldParentIno())) {
			return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
		}
		s.entries[req.Msg.GetOldParentIno()][oldName] = dstIno
		s.entries[req.Msg.GetNewParentIno()][newName] = srcIno
		if req.Msg.GetOldParentIno() != req.Msg.GetNewParentIno() && srcIsDir != dstIsDir {
			if srcIsDir {
				oldParent.inode.Attr.Nlink--
				newParent.inode.Attr.Nlink++
			} else {
				oldParent.inode.Attr.Nlink++
				newParent.inode.Attr.Nlink--
			}
		}
		return connect.NewResponse(&storagev1.RenameResponse{}), nil
	}

	if flags&0x1 != 0 && dstIno != 0 {
		return nil, errnoError(connect.CodeAlreadyExists, "EEXIST")
	}
	if srcIsDir && s.isAncestorLocked(srcIno, req.Msg.GetNewParentIno()) {
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	if req.Msg.GetOldParentIno() == req.Msg.GetNewParentIno() && oldName == newName {
		return connect.NewResponse(&storagev1.RenameResponse{}), nil
	}
	if dstIno != 0 && dstNode != nil {
		if dstIno == srcIno {
			delete(s.entries[req.Msg.GetOldParentIno()], oldName)
			return connect.NewResponse(&storagev1.RenameResponse{}), nil
		}
		dstIsDir := dstNode.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR
		if srcIsDir && !dstIsDir {
			return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
		}
		if !srcIsDir && dstIsDir {
			return nil, errnoError(connect.CodeFailedPrecondition, "EISDIR")
		}
		if dstIsDir {
			if len(s.entries[dstIno]) > 0 {
				return nil, errnoError(connect.CodeFailedPrecondition, "ENOTEMPTY")
			}
			delete(s.entries, dstIno)
			delete(s.nodes, dstIno)
			newParent.inode.Attr.Nlink--
		} else {
			if dstNode.inode.Attr.Nlink > 0 {
				dstNode.inode.Attr.Nlink--
			}
			if dstNode.inode.Attr.Nlink == 0 && !req.Msg.GetTargetOpenInClient() {
				delete(s.nodes, dstIno)
			}
		}
	}
	delete(s.entries[req.Msg.GetOldParentIno()], oldName)
	s.entries[req.Msg.GetNewParentIno()][newName] = srcIno
	if srcIsDir && req.Msg.GetOldParentIno() != req.Msg.GetNewParentIno() {
		oldParent.inode.Attr.Nlink--
		newParent.inode.Attr.Nlink++
	}
	return connect.NewResponse(&storagev1.RenameResponse{}), nil
}

// Create 创建常规文件。
func (s *fakeStorage) Create(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	s.createCalls.Add(1)
	if err := s.recordCtx(ctx, "Create"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Create", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Create", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	name := string(req.Msg.GetName())
	if len(name) > fusefs.MaxFilenameBytes {
		return nil, errnoError(connect.CodeInvalidArgument, "ENAMETOOLONG")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.nodes[req.Msg.GetParentIno()]
	if parent == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if parent.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	if existing := s.entries[req.Msg.GetParentIno()][name]; existing != 0 {
		if req.Msg.GetFlags()&0o200 != 0 {
			return nil, errnoError(connect.CodeAlreadyExists, "EEXIST")
		}
		node := s.nodes[existing]
		return connect.NewResponse(&storagev1.CreateResponse{
			Inode:               cloneProtoInode(node.inode),
			EntryTimeoutSeconds: s.entryTTL,
			AttrTimeoutSeconds:  s.attrTTL,
		}), nil
	}
	ino := s.allocInoLocked()
	now := timestamppb.Now()
	node := &fakeNode{
		inode: &storagev1.Inode{
			Ino:        ino,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    syscall.S_IFREG | (req.Msg.GetMode() & 0o7777),
				Nlink:   1,
				Uid:     req.Msg.GetUid(),
				Gid:     req.Msg.GetGid(),
				Blksize: 4096,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		},
		xattrs: make(map[string][]byte),
	}
	s.nodes[ino] = node
	s.entries[req.Msg.GetParentIno()][name] = ino
	parent.inode.Attr.Mtime = now
	parent.inode.Attr.Ctime = now
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               cloneProtoInode(node.inode),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

// Read 读取文件数据块。
func (s *fakeStorage) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	s.readCalls.Add(1)
	if err := s.recordCtx(ctx, "Read"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Read", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if s.readHook != nil {
		data, err := s.readHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		if data != nil {
			return connect.NewResponse(&storagev1.ReadResponse{Data: data}), nil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if node.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "EISDIR")
	}
	off := req.Msg.GetOffset()
	if off >= uint64(len(node.data)) {
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}
	end := min(off+uint64(req.Msg.GetSize()), uint64(len(node.data)))
	return connect.NewResponse(&storagev1.ReadResponse{
		Data: slices.Clone(node.data[off:end]),
	}), nil
}

// Write 写入文件数据块并同步更新长度与时间戳。
func (s *fakeStorage) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	s.writeCalls.Add(1)
	if err := s.recordCtx(ctx, "Write"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Write", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Write", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	if s.writeHook != nil {
		shortLen, err := s.writeHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		if shortLen > 0 && shortLen < uint32(len(req.Msg.GetData())) {
			s.mu.Lock()
			defer s.mu.Unlock()
			node := s.nodes[req.Msg.GetIno()]
			if node == nil {
				return nil, errnoError(connect.CodeNotFound, "ENOENT")
			}
			end := int(req.Msg.GetOffset()) + int(shortLen)
			if end > len(node.data) {
				grown := make([]byte, end)
				copy(grown, node.data)
				node.data = grown
			}
			copy(node.data[req.Msg.GetOffset():end], req.Msg.GetData()[:shortLen])
			node.inode.Attr.Size = uint64(len(node.data))
			node.inode.Attr.Blocks = (node.inode.Attr.Size + 511) / 512
			now := timestamppb.Now()
			node.inode.Attr.Mtime = now
			node.inode.Attr.Ctime = now
			return connect.NewResponse(&storagev1.WriteResponse{
				Size:  shortLen,
				Inode: cloneProtoInode(node.inode),
			}), nil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if node.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "EISDIR")
	}
	end := int(req.Msg.GetOffset()) + len(req.Msg.GetData())
	if end > len(node.data) {
		grown := make([]byte, end)
		copy(grown, node.data)
		node.data = grown
	}
	copy(node.data[req.Msg.GetOffset():end], req.Msg.GetData())
	node.inode.Attr.Size = uint64(len(node.data))
	node.inode.Attr.Blocks = (node.inode.Attr.Size + 511) / 512
	now := timestamppb.Now()
	node.inode.Attr.Mtime = now
	node.inode.Attr.Ctime = now
	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  uint32(len(req.Msg.GetData())),
		Inode: cloneProtoInode(node.inode),
	}), nil
}

// Fsync 同步持久化文件节点。
func (s *fakeStorage) Fsync(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	s.fsyncCalls.Add(1)
	if err := s.recordCtx(ctx, "Fsync"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Fsync", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Fsync", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	if s.fsyncHook != nil {
		if err := s.fsyncHook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nodes[req.Msg.GetIno()] == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

// FsyncDir 同步持久化目录节点。
func (s *fakeStorage) FsyncDir(ctx context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	s.fsyncDirCalls.Add(1)
	if err := s.recordCtx(ctx, "FsyncDir"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("FsyncDir", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("FsyncDir", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if node.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

// ReadDirPlus 读取目录流与子项属性。
func (s *fakeStorage) ReadDirPlus(ctx context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	s.readDirPlusCalls.Add(1)
	if err := s.recordCtx(ctx, "ReadDirPlus"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("ReadDirPlus", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if s.readDirPlusHook != nil {
		entries, err := s.readDirPlusHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		if entries != nil {
			return connect.NewResponse(&storagev1.ReadDirPlusResponse{Entries: entries}), nil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dirNode := s.nodes[req.Msg.GetIno()]
	if dirNode == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	if dirNode.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, errnoError(connect.CodeFailedPrecondition, "ENOTDIR")
	}
	names := make([]string, 0, len(s.entries[req.Msg.GetIno()]))
	for name := range s.entries[req.Msg.GetIno()] {
		names = append(names, name)
	}
	slices.Sort(names)

	all := make([]*storagev1.DirEntryPlus, 0, len(names)+2)
	all = append(all,
		&storagev1.DirEntryPlus{
			Ino:                 req.Msg.GetIno(),
			Name:                []byte("."),
			NextOffset:          1,
			Inode:               cloneProtoInode(dirNode.inode),
			EntryTimeoutSeconds: s.entryTTL,
			AttrTimeoutSeconds:  s.attrTTL,
		},
		&storagev1.DirEntryPlus{
			Ino:                 s.rootIno,
			Name:                []byte(".."),
			NextOffset:          2,
			Inode:               cloneProtoInode(s.nodes[s.rootIno].inode),
			EntryTimeoutSeconds: s.entryTTL,
			AttrTimeoutSeconds:  s.attrTTL,
		},
	)
	for i, name := range names {
		childIno := s.entries[req.Msg.GetIno()][name]
		childNode := s.nodes[childIno]
		if childNode == nil {
			continue
		}
		all = append(all, &storagev1.DirEntryPlus{
			Ino:                 childIno,
			Name:                []byte(name),
			NextOffset:          uint64(3 + i),
			Inode:               cloneProtoInode(childNode.inode),
			EntryTimeoutSeconds: s.entryTTL,
			AttrTimeoutSeconds:  s.attrTTL,
		})
	}
	offset := req.Msg.GetOffset()
	limit := int(req.Msg.GetLimit())
	if limit <= 0 {
		limit = 512
	}
	var page []*storagev1.DirEntryPlus
	for _, entry := range all {
		if entry.GetNextOffset() <= offset {
			continue
		}
		page = append(page, entry)
		if len(page) >= limit {
			break
		}
	}
	return connect.NewResponse(&storagev1.ReadDirPlusResponse{Entries: page}), nil
}

// Fallocate 执行空间预分配或打洞去分配。
func (s *fakeStorage) Fallocate(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	s.fallocateCalls.Add(1)
	if err := s.recordCtx(ctx, "Fallocate"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("Fallocate", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("Fallocate", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	if req.Msg.GetLength() == 0 || req.Msg.GetOffset() > math.MaxInt64 || req.Msg.GetLength() > math.MaxInt64-req.Msg.GetOffset() {
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	offset := int(req.Msg.GetOffset())
	end := int(req.Msg.GetOffset() + req.Msg.GetLength())
	const (
		keepSizeFlag  = 0x01
		punchHoleFlag = 0x02
		zeroRangeFlag = 0x10
		punchAndKeep  = punchHoleFlag | keepSizeFlag
		zeroAndKeep   = zeroRangeFlag | keepSizeFlag
	)
	switch req.Msg.GetMode() {
	case 0:
		if end > len(node.data) {
			grown := make([]byte, end)
			copy(grown, node.data)
			node.data = grown
			node.inode.Attr.Size = uint64(end)
		}
	case keepSizeFlag:
		// 仅保留空间，不改变可见长度。
	case punchHoleFlag:
		return nil, errnoError(connect.CodeInvalidArgument, "EINVAL")
	case punchAndKeep, zeroAndKeep:
		if offset < len(node.data) {
			clearEnd := min(end, len(node.data))
			clear(node.data[offset:clearEnd])
		}
		node.inode.Attr.Mode &^= syscall.S_ISUID | syscall.S_ISGID
	case zeroRangeFlag:
		if end > len(node.data) {
			grown := make([]byte, end)
			copy(grown, node.data)
			node.data = grown
			node.inode.Attr.Size = uint64(end)
		}
		clear(node.data[offset:end])
		node.inode.Attr.Mode &^= syscall.S_ISUID | syscall.S_ISGID
	default:
		return nil, errnoError(connect.CodeUnimplemented, "EOPNOTSUPP")
	}
	now := timestamppb.Now()
	node.inode.Attr.Mtime = now
	node.inode.Attr.Ctime = now
	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

// SetXattr 设置扩展属性。
func (s *fakeStorage) SetXattr(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	s.setXattrCalls.Add(1)
	if err := s.recordCtx(ctx, "SetXattr"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("SetXattr", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("SetXattr", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	name := string(req.Msg.GetName())
	_, exists := node.xattrs[name]
	switch req.Msg.GetFlags() {
	case 0x1: // XATTR_CREATE
		if exists {
			return nil, errnoError(connect.CodeAlreadyExists, "EEXIST")
		}
	case 0x2: // XATTR_REPLACE
		if !exists {
			return nil, errnoError(connect.CodeNotFound, "ENODATA")
		}
	}
	node.xattrs[name] = slices.Clone(req.Msg.GetValue())
	return connect.NewResponse(&storagev1.SetXattrResponse{}), nil
}

// GetXattr 读取扩展属性。
func (s *fakeStorage) GetXattr(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
	s.getXattrCalls.Add(1)
	if err := s.recordCtx(ctx, "GetXattr"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("GetXattr", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	val, exists := node.xattrs[string(req.Msg.GetName())]
	if !exists {
		return nil, errnoError(connect.CodeNotFound, "ENODATA")
	}
	return connect.NewResponse(&storagev1.GetXattrResponse{
		Value: slices.Clone(val),
	}), nil
}

// ListXattr 按字典序返回扩展属性名称列表。
func (s *fakeStorage) ListXattr(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
	s.listXattrCalls.Add(1)
	if err := s.recordCtx(ctx, "ListXattr"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("ListXattr", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	names := make([]string, 0, len(node.xattrs))
	for name := range node.xattrs {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make([][]byte, 0, len(names))
	for _, name := range names {
		out = append(out, []byte(name))
	}
	return connect.NewResponse(&storagev1.ListXattrResponse{Names: out}), nil
}

// RemoveXattr 移除扩展属性。
func (s *fakeStorage) RemoveXattr(ctx context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	s.removeXattrCalls.Add(1)
	if err := s.recordCtx(ctx, "RemoveXattr"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("RemoveXattr", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("RemoveXattr", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[req.Msg.GetIno()]
	if node == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	name := string(req.Msg.GetName())
	if _, exists := node.xattrs[name]; !exists {
		return nil, errnoError(connect.CodeNotFound, "ENODATA")
	}
	delete(node.xattrs, name)
	return connect.NewResponse(&storagev1.RemoveXattrResponse{}), nil
}

// StatFS 返回文件系统配额与节点统计。
func (s *fakeStorage) StatFS(ctx context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	s.statFSCalls.Add(1)
	if err := s.recordCtx(ctx, "StatFS"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("StatFS", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nodes[req.Msg.GetIno()] == nil {
		return nil, errnoError(connect.CodeNotFound, "ENOENT")
	}
	resp, _ := proto.Clone(&s.statFS).(*storagev1.StatFSResponse)
	return connect.NewResponse(resp), nil
}

// ReclaimOrphan 回收孤儿节点。
func (s *fakeStorage) ReclaimOrphan(ctx context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	s.reclaimOrphanCalls.Add(1)
	if err := s.recordCtx(ctx, "ReclaimOrphan"); err != nil {
		return nil, err
	}
	if err := s.checkDiskID("ReclaimOrphan", req.Msg.GetDiskId()); err != nil {
		return nil, err
	}
	if err := s.checkEpoch("ReclaimOrphan", req.Msg.GetEpoch()); err != nil {
		return nil, err
	}
	if s.reclaimOrphanHook != nil {
		if err := s.reclaimOrphanHook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	delete(s.nodes, req.Msg.GetIno())
	s.mu.Unlock()
	return connect.NewResponse(&storagev1.ReclaimOrphanResponse{}), nil
}

// 以下控制面与旧版目录接口在 fusefs 数据面调用中属于 Fail Fast 违例。

func (s *fakeStorage) ReadDir(context.Context, *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
	return nil, s.failFast("ReadDir")
}

func (s *fakeStorage) MountDisk(context.Context, *connect.Request[storagev1.MountDiskRequest]) (*connect.Response[storagev1.MountDiskResponse], error) {
	return nil, s.failFast("MountDisk")
}

func (s *fakeStorage) RenewMount(context.Context, *connect.Request[storagev1.RenewMountRequest]) (*connect.Response[storagev1.RenewMountResponse], error) {
	return nil, s.failFast("RenewMount")
}

func (s *fakeStorage) UnmountDisk(context.Context, *connect.Request[storagev1.UnmountDiskRequest]) (*connect.Response[storagev1.UnmountDiskResponse], error) {
	return nil, s.failFast("UnmountDisk")
}

func (s *fakeStorage) CreateDisk(context.Context, *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	return nil, s.failFast("CreateDisk")
}

func (s *fakeStorage) GetDisk(context.Context, *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	return nil, s.failFast("GetDisk")
}

func (s *fakeStorage) ListDisks(context.Context, *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
	return nil, s.failFast("ListDisks")
}

func (s *fakeStorage) UpdateDisk(context.Context, *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
	return nil, s.failFast("UpdateDisk")
}

func (s *fakeStorage) DeleteDisk(context.Context, *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	return nil, s.failFast("DeleteDisk")
}

func (s *fakeStorage) ForkDisk(context.Context, *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
	return nil, s.failFast("ForkDisk")
}

func (s *fakeStorage) DiffDisk(context.Context, *connect.Request[storagev1.DiffDiskRequest]) (*connect.Response[storagev1.DiffDiskResponse], error) {
	return nil, s.failFast("DiffDisk")
}

func (s *fakeStorage) ReadCommitNode(context.Context, *connect.Request[storagev1.ReadCommitNodeRequest]) (*connect.Response[storagev1.ReadCommitNodeResponse], error) {
	return nil, s.failFast("ReadCommitNode")
}

func (s *fakeStorage) MergeDisk(context.Context, *connect.Request[storagev1.MergeDiskRequest]) (*connect.Response[storagev1.MergeDiskResponse], error) {
	return nil, s.failFast("MergeDisk")
}

// ---------------------------------------------------------------------------
// 测试通用装配与自诊断辅助函数（置于接缝外侧，零业务分支）
// ---------------------------------------------------------------------------

type rawFSOption func(*fusefs.Config)

func withReadOnly(ro bool) rawFSOption {
	return func(c *fusefs.Config) {
		c.ReadOnly = ro
	}
}

func withDirectIO(dio bool) rawFSOption {
	return func(c *fusefs.Config) {
		c.DirectIO = dio
	}
}

func withStatsFile(enabled bool) rawFSOption {
	return func(c *fusefs.Config) {
		c.EnableStatsFile = enabled
	}
}

func withTracerProvider(tp trace.TracerProvider) rawFSOption {
	return func(c *fusefs.Config) {
		c.TracerProvider = tp
	}
}

func withOrphanThreshold(n int) rawFSOption {
	return func(c *fusefs.Config) {
		c.OrphanBacklogThreshold = n
	}
}

func withLogger(l *slog.Logger) rawFSOption {
	return func(c *fusefs.Config) {
		c.Logger = l
	}
}

func withMetrics(m *fusefs.Metrics) rawFSOption {
	return func(c *fusefs.Config) {
		c.Metrics = m
	}
}

func newTestRawFS(t *testing.T, s *fakeStorage, opts ...rawFSOption) *fusefs.RawFS {
	t.Helper()
	cfg := fusefs.Config{
		DiskID:              s.diskID,
		Epoch:               s.epoch,
		RootIno:             s.rootIno,
		RootInode:           s.rootInodeClone(),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	fs := fusefs.NewRawFS(s, cfg)
	t.Cleanup(fs.OnUnmount)
	return fs
}

func requireStatus(t *testing.T, contextLabel string, want, got fuse.Status) {
	t.Helper()
	if want != got {
		t.Fatalf("%s：状态码不符，want=%v (%d) got=%v (%d)", contextLabel, want, want, got, got)
	}
}

func mustCreateFile(t *testing.T, fs *fusefs.RawFS, parent uint64, name string, flags, mode uint32) fuse.CreateOut {
	t.Helper()
	var out fuse.CreateOut
	st := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: parent},
		Flags:    flags,
		Mode:     mode,
	}, name, &out)
	requireStatus(t, "Create("+name+")", fuse.OK, st)
	return out
}

func mustOpenFile(t *testing.T, fs *fusefs.RawFS, ino uint64, flags uint32) fuse.OpenOut {
	t.Helper()
	var out fuse.OpenOut
	st := fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: ino},
		Flags:    flags,
	}, &out)
	requireStatus(t, fmt.Sprintf("Open(ino=%d)", ino), fuse.OK, st)
	return out
}

func mustMkdir(t *testing.T, fs *fusefs.RawFS, parent uint64, name string, mode uint32) fuse.EntryOut {
	t.Helper()
	var out fuse.EntryOut
	st := fs.Mkdir(nil, &fuse.MkdirIn{
		InHeader: fuse.InHeader{NodeId: parent},
		Mode:     mode,
	}, name, &out)
	requireStatus(t, "Mkdir("+name+")", fuse.OK, st)
	return out
}

func mustLookup(t *testing.T, fs *fusefs.RawFS, parent uint64, name string) fuse.EntryOut {
	t.Helper()
	var out fuse.EntryOut
	st := fs.Lookup(nil, &fuse.InHeader{NodeId: parent}, name, &out)
	requireStatus(t, "Lookup("+name+")", fuse.OK, st)
	return out
}

func mustGetAttr(t *testing.T, fs *fusefs.RawFS, ino uint64) fuse.AttrOut {
	t.Helper()
	var out fuse.AttrOut
	st := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: ino}}, &out)
	requireStatus(t, fmt.Sprintf("GetAttr(ino=%d)", ino), fuse.OK, st)
	return out
}

func mustReadAt(t *testing.T, fs *fusefs.RawFS, ino, fh, offset uint64, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	res, st := fs.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: ino},
		Fh:       fh,
		Offset:   offset,
		Size:     uint32(size),
	}, buf)
	requireStatus(t, fmt.Sprintf("Read(ino=%d,off=%d)", ino, offset), fuse.OK, st)
	data, byteSt := res.Bytes(buf)
	requireStatus(t, "ReadResult.Bytes", fuse.OK, byteSt)
	return slices.Clone(data)
}

func mustWriteAt(t *testing.T, fs *fusefs.RawFS, ino, fh, offset uint64, payload []byte) {
	t.Helper()
	written, st := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: ino},
		Fh:       fh,
		Offset:   offset,
		Size:     uint32(len(payload)),
	}, payload)
	requireStatus(t, fmt.Sprintf("Write(ino=%d,off=%d)", ino, offset), fuse.OK, st)
	require.Equal(t, uint32(len(payload)), written, "Write 写入字节数不符")
}

func buildRepeatingBytes(size int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, size)
}

func buildSeededBytes(size int, seed byte) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte((int(seed) + i*31) & 0xff)
	}
	return out
}

func requireBytesMatch(t *testing.T, contextLabel string, want, got []byte) {
	t.Helper()
	if !bytes.Equal(want, got) {
		t.Fatalf("%s：字节序列不一致，wantLen=%d gotLen=%d", contextLabel, len(want), len(got))
	}
}
