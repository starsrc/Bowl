package server

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func createAndMountTestDisk(t *testing.T, client storagev1connect.StorageServiceClient) (diskID string, epoch uint64) {
	t.Helper()
	ctx := context.Background()
	createResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "fs-test-disk",
		MaxBytes: 32 << 20,
	}))
	require.NoError(t, err, "CreateDisk 失败")
	diskID = createResp.Msg.GetDisk().GetDiskId()

	mountResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID}))
	require.NoError(t, err, "MountDisk 失败")
	return diskID, mountResp.Msg.GetEpoch()
}

func findDirEntryPlusByName(entries []*storagev1.DirEntryPlus, name string) *storagev1.DirEntryPlus {
	for _, e := range entries {
		if string(e.GetName()) == name {
			return e
		}
	}
	return nil
}

func findDirEntryByName(entries []*storagev1.DirEntry, name string) *storagev1.DirEntry {
	for _, e := range entries {
		if string(e.GetName()) == name {
			return e
		}
	}
	return nil
}

func TestServerFSRPC_FileLifecycleReadWriteFsyncAndStatFS_Succeeds(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(
		t,
		control.WithOpenKeepCache(true),
		control.WithEntryTimeout(2.0),
		control.WithAttrTimeout(4.0),
	)
	diskID, epoch := createAndMountTestDisk(t, client)

	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("blob.bin"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err, "Create RPC 失败")
	require.Equal(t, fopenKeepCache, createResp.Msg.GetOpenFlags(), "OpenKeepCache=true 时应返回 FOPEN_KEEP_CACHE")
	require.InDelta(t, 2.0, createResp.Msg.GetEntryTimeoutSeconds(), 1e-9)
	require.InDelta(t, 4.0, createResp.Msg.GetAttrTimeoutSeconds(), 1e-9)
	fileIno := createResp.Msg.GetInode().GetIno()

	payload := bytes.Repeat([]byte{0x5A, 0xA5, 0x3C, 0xC3}, 128<<10)
	writeResp, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
		Offset: 0,
		Data:   payload,
	}))
	require.NoError(t, err, "Write RPC 失败")
	require.Equal(t, uint32(len(payload)), writeResp.Msg.GetSize())

	_, err = client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
	}))
	require.NoError(t, err, "Fsync RPC 失败")

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Offset: 0,
		Size:   uint32(len(payload)),
	}))
	require.NoError(t, err, "Read RPC 失败")
	require.Equal(t, payload, readResp.Msg.GetData(), "Read 返回载荷与写入不一致")

	emptyRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Offset: 0,
		Size:   0,
	}))
	require.NoError(t, err, "0 字节 Read 失败")
	require.Empty(t, emptyRead.Msg.GetData())

	eofRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Offset: uint64(len(payload)) + 4096,
		Size:   4096,
	}))
	require.NoError(t, err, "EOF 之后 Read 失败")
	require.Empty(t, eofRead.Msg.GetData())

	newMode := store.S_IFREG | 0o600
	setResp, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
		Mode:   &newMode,
	}))
	require.NoError(t, err, "SetAttr RPC 失败")
	require.Equal(t, newMode, setResp.Msg.GetInode().GetAttr().GetMode())

	getResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{DiskId: diskID, Ino: fileIno}))
	require.NoError(t, err, "GetAttr RPC 失败")
	require.Equal(t, uint64(len(payload)), getResp.Msg.GetInode().GetAttr().GetSize())

	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: 1,
		Name:      []byte("blob.bin"),
	}))
	require.NoError(t, err, "Lookup RPC 失败")
	require.Equal(t, fileIno, lookupResp.Msg.GetInode().GetIno())

	statResp, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{DiskId: diskID, Ino: fileIno}))
	require.NoError(t, err, "StatFS RPC 失败")

	require.Equal(t, uint64(32<<20), statResp.Msg.GetTotalBytes())
	require.Equal(t, uint64(len(payload)), statResp.Msg.GetUsedBytes())
	require.Equal(t, uint64(2), statResp.Msg.GetFiles())
	require.Zero(t, leasedWritePayloadCount(), "Write 缓冲池必须零泄漏")
	require.Zero(t, leasedReadPayloadCount(), "Read 缓冲池必须零泄漏")
}

func TestServerFSRPC_DirectoryAndLinkOps_EnforcesTimeoutsAndPOSIXSemantics(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(
		t,
		control.WithEntryTimeout(1.5),
		control.WithDirEntryTimeout(5.0),
		control.WithAttrTimeout(3.0),
	)
	diskID, epoch := createAndMountTestDisk(t, client)

	dirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("subdir"),
		Mode:      store.S_IFDIR | 0o755,
	}))
	require.NoError(t, err, "Mkdir RPC 失败")
	require.InDelta(t, 5.0, dirResp.Msg.GetEntryTimeoutSeconds(), 1e-9, "目录 EntryTimeout 应使用 DirEntryTimeoutSeconds")
	subDirIno := dirResp.Msg.GetInode().GetIno()

	mknodResp, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: subDirIno,
		Name:      []byte("node.dat"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err, "Mknod RPC 失败")
	require.InDelta(t, 1.5, mknodResp.Msg.GetEntryTimeoutSeconds(), 1e-9, "文件 EntryTimeout 应使用 EntryTimeoutSeconds")
	nodeIno := mknodResp.Msg.GetInode().GetIno()

	symResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: subDirIno,
		Name:      []byte("sym"),
		Target:    []byte("/subdir/node.dat"),
	}))
	require.NoError(t, err, "Symlink RPC 失败")
	readlinkResp, err := client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
		DiskId: diskID,
		Ino:    symResp.Msg.GetInode().GetIno(),
	}))
	require.NoError(t, err, "Readlink RPC 失败")
	require.Equal(t, []byte("/subdir/node.dat"), readlinkResp.Msg.GetTarget())

	_, err = client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId:       diskID,
		Epoch:        epoch,
		Ino:          nodeIno,
		NewParentIno: 1,
		NewName:      []byte("hard.dat"),
	}))
	require.NoError(t, err, "Link RPC 失败")

	readDirResp, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		DiskId: diskID,
		Ino:    1,
		Offset: 0,
		Limit:  64,
	}))
	require.NoError(t, err, "ReadDir RPC 失败")
	require.NotNil(t, findDirEntryByName(readDirResp.Msg.GetEntries(), "subdir"))
	require.NotNil(t, findDirEntryByName(readDirResp.Msg.GetEntries(), "hard.dat"))

	readDirPlusResp, err := client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
		DiskId: diskID,
		Ino:    1,
		Offset: 0,
		Limit:  64,
	}))
	require.NoError(t, err, "ReadDirPlus RPC 失败")

	dotEntry := findDirEntryPlusByName(readDirPlusResp.Msg.GetEntries(), ".")
	dotDotEntry := findDirEntryPlusByName(readDirPlusResp.Msg.GetEntries(), "..")
	subEntry := findDirEntryPlusByName(readDirPlusResp.Msg.GetEntries(), "subdir")
	hardEntry := findDirEntryPlusByName(readDirPlusResp.Msg.GetEntries(), "hard.dat")
	require.NotNil(t, dotEntry)
	require.NotNil(t, dotDotEntry)
	require.NotNil(t, subEntry)
	require.NotNil(t, hardEntry)
	require.InDelta(t, 0.0, dotEntry.GetEntryTimeoutSeconds(), 1e-9, "'.' 条目不得设置正缓存时长")
	require.InDelta(t, 0.0, dotDotEntry.GetEntryTimeoutSeconds(), 1e-9, "'..' 条目不得设置正缓存时长")
	require.InDelta(t, 5.0, subEntry.GetEntryTimeoutSeconds(), 1e-9, "子目录条目应返回 DirEntryTimeoutSeconds")
	require.InDelta(t, 1.5, hardEntry.GetEntryTimeoutSeconds(), 1e-9, "常规文件条目应返回 EntryTimeoutSeconds")

	_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		DiskId:       diskID,
		Epoch:        epoch,
		OldParentIno: 1,
		OldName:      []byte("hard.dat"),
		NewParentIno: 1,
		NewName:      []byte("hard-renamed.dat"),
	}))
	require.NoError(t, err, "Rename RPC 失败")

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("hard-renamed.dat"),
	}))
	require.NoError(t, err, "Unlink hard-renamed.dat 失败")

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: subDirIno,
		Name:      []byte("node.dat"),
	}))
	require.NoError(t, err, "Unlink node.dat 失败")

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: subDirIno,
		Name:      []byte("sym"),
	}))
	require.NoError(t, err, "Unlink sym 失败")

	_, err = client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("subdir"),
	}))
	require.NoError(t, err, "Rmdir RPC 失败")

	_, err = client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    1,
	}))
	require.NoError(t, err, "FsyncDir RPC 失败")
}

func TestServerFSRPC_FallocateAndOrphanReclaim_UpdatesAndReclaimsSpace(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t)
	diskID, epoch := createAndMountTestDisk(t, client)

	cResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("alloc.bin"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err, "Create 失败")
	ino := cResp.Msg.GetInode().GetIno()

	_, err = client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    ino,
		Offset: 0,
		Length: 16384,
	}))
	require.NoError(t, err, "Fallocate RPC 失败")

	attrResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{DiskId: diskID, Ino: ino}))
	require.NoError(t, err, "GetAttr 失败")
	require.Equal(t, uint64(16384), attrResp.Msg.GetInode().GetAttr().GetSize())

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:       diskID,
		Epoch:        epoch,
		ParentIno:    1,
		Name:         []byte("alloc.bin"),
		OpenInClient: new(true),
	}))
	require.NoError(t, err, "Unlink 孤儿保留失败")

	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    ino,
	}))
	require.NoError(t, err, "ReclaimOrphan RPC 失败")

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{DiskId: diskID, Ino: ino}))
	requireConnectErrorCodeAndReason(t, err, connect.CodeNotFound, "ENOENT")
}

func TestServerFSRPC_XattrLifecycle_SetsGetsListsAndRemoves(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t)
	diskID, epoch := createAndMountTestDisk(t, client)

	_, err := client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    1,
		Name:   []byte("user.meta"),
		Value:  []byte("v1"),
	}))
	require.NoError(t, err, "SetXattr RPC 失败")

	getResp, err := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		DiskId: diskID,
		Ino:    1,
		Name:   []byte("user.meta"),
	}))
	require.NoError(t, err, "GetXattr RPC 失败")
	require.Equal(t, []byte("v1"), getResp.Msg.GetValue())

	listResp, err := client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err, "ListXattr RPC 失败")
	require.Equal(t, [][]byte{[]byte("user.meta")}, listResp.Msg.GetNames())

	_, err = client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    1,
		Name:   []byte("user.meta"),
	}))
	require.NoError(t, err, "RemoveXattr RPC 失败")

	_, err = client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		DiskId: diskID,
		Ino:    1,
		Name:   []byte("user.meta"),
	}))
	requireConnectErrorCodeAndReason(t, err, connect.CodeNotFound, "ENODATA")
}

func TestServerFSRPC_ValidationFastPathAndProtovalidate_RejectsWithExpectedReason(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t)
	validUUID := "018f0000-0000-7000-8000-000000000001"
	nilUUID := "00000000-0000-0000-0000-000000000000"

	cases := []struct {
		name       string
		invoke     func() error
		wantReason string
	}{
		{
			name: "WriteRequest Epoch 为 0",
			invoke: func() error {
				_, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					DiskId: validUUID, Ino: 1, Epoch: 0, Data: []byte("a"),
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "WriteRequest Offset 加 Data 溢出 MaxInt64",
			invoke: func() error {
				_, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					DiskId: validUUID, Ino: 1, Epoch: 1, Offset: math.MaxInt64, Data: []byte("a"),
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "WriteRequest Data 超过 4MiB",
			invoke: func() error {
				_, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					DiskId: validUUID, Ino: 1, Epoch: 1, Data: make([]byte, maxReadWriteSize+1),
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "WriteRequest 全零 Nil UUID",
			invoke: func() error {
				_, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					DiskId: nilUUID, Ino: 1, Epoch: 1, Data: []byte("a"),
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "ReadRequest Ino 为 0",
			invoke: func() error {
				_, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
					DiskId: validUUID, Ino: 0, Size: 16,
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "ReadRequest Size 超过 4MiB",
			invoke: func() error {
				_, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
					DiskId: validUUID, Ino: 1, Size: maxReadWriteSize + 1,
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "ReadRequest 非法 UUID 长度",
			invoke: func() error {
				_, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
					DiskId: "short", Ino: 1, Size: 16,
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "Lookup 名称超过 255 字节",
			invoke: func() error {
				_, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
					DiskId: validUUID, ParentIno: 1, Name: []byte(strings.Repeat("a", 256)),
				}))
				return err
			},
			wantReason: "ENAMETOOLONG",
		},
		{
			name: "Lookup 名称包含斜杠",
			invoke: func() error {
				_, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
					DiskId: validUUID, ParentIno: 1, Name: []byte("a/b"),
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "Symlink 目标路径超长",
			invoke: func() error {
				_, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
					DiskId: validUUID, Epoch: 1, ParentIno: 1, Name: []byte("s"), Target: []byte(strings.Repeat("t", 4096)),
				}))
				return err
			},
			wantReason: "ENAMETOOLONG",
		},
		{
			name: "SetXattr 名称超长",
			invoke: func() error {
				_, err := client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
					DiskId: validUUID, Epoch: 1, Ino: 1, Name: []byte(strings.Repeat("x", 256)), Value: []byte("v"),
				}))
				return err
			},
			wantReason: "ERANGE",
		},
		{
			name: "SetXattr 值超长",
			invoke: func() error {
				_, err := client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
					DiskId: validUUID, Epoch: 1, Ino: 1, Name: []byte("user.big"), Value: make([]byte, 65537),
				}))
				return err
			},
			wantReason: "ERANGE",
		},
		{
			name: "SetXattr 非法 flags",
			invoke: func() error {
				_, err := client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
					DiskId: validUUID, Epoch: 1, Ino: 1, Name: []byte("user.k"), Value: []byte("v"), Flags: 4,
				}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "ReadDir limit 为 0",
			invoke: func() error {
				_, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
					DiskId: validUUID, Ino: 1, Limit: 0,
				}))
				return err
			},
			wantReason: "EINVAL",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireConnectErrorCodeAndReason(t, tc.invoke(), connect.CodeInvalidArgument, tc.wantReason)
		})
	}
}

func TestServerFSRPC_BusinessInterception_DirectHandlerGuardsAndDomainErrors(t *testing.T) {

	ctx := context.Background()
	srv, client := newTestServerAndClient(t)
	diskID, epoch := createAndMountTestDisk(t, client)

	_, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		DiskId: diskID, Epoch: epoch, ParentIno: 1, Name: []byte("nonempty"), Mode: store.S_IFDIR | 0o755,
	}))
	require.NoError(t, err, "Mkdir 失败")
	_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId: diskID, ParentIno: 1, Name: []byte("nonempty"),
	}))
	require.NoError(t, err, "Lookup nonempty 失败")
	nonEmptyResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId: diskID, ParentIno: 1, Name: []byte("nonempty"),
	}))
	require.NoError(t, err)
	_, err = client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId: diskID, Epoch: epoch, ParentIno: nonEmptyResp.Msg.GetInode().GetIno(), Name: []byte("child"), Mode: store.S_IFREG | 0o644,
	}))
	require.NoError(t, err)

	_, err = client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID, Force: true}))
	require.NoError(t, err, "推进 epoch 失败")

	zeroFh := uint64(0)
	cases := []struct {
		name       string
		invoke     func() error
		wantCode   connect.Code
		wantReason string
	}{
		{
			name: "SetAttr 直接调用 Epoch=0 拦截",
			invoke: func() error {
				_, hErr := srv.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{DiskId: diskID, Ino: 1, Epoch: 0}))
				return hErr
			},
			wantCode:   connect.CodeInvalidArgument,
			wantReason: "EINVAL",
		},
		{
			name: "SetAttr 直接调用 Fh=0 拦截",
			invoke: func() error {
				_, hErr := srv.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{DiskId: diskID, Ino: 1, Epoch: 2, Fh: &zeroFh}))
				return hErr
			},
			wantCode:   connect.CodeInvalidArgument,
			wantReason: "EINVAL",
		},
		{
			name: "Create 直接调用 Uid 超过 MaxInt32 拦截",
			invoke: func() error {
				_, hErr := srv.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
					DiskId: diskID, Epoch: 2, ParentIno: 1, Name: []byte("baduid"), Uid: math.MaxInt32 + 1,
				}))
				return hErr
			},
			wantCode:   connect.CodeInvalidArgument,
			wantReason: "EINVAL",
		},
		{
			name: "Read 直接调用 Size 超过 4MiB 拦截",
			invoke: func() error {
				_, hErr := srv.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
					DiskId: diskID, Ino: 1, Size: maxReadWriteSize + 1,
				}))
				return hErr
			},
			wantCode:   connect.CodeInvalidArgument,
			wantReason: "EINVAL",
		},
		{
			name: "Write 直接调用 Fh 超过 MaxInt64 拦截",
			invoke: func() error {
				_, hErr := srv.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					DiskId: diskID, Epoch: 2, Ino: 1, Fh: uint64(math.MaxInt64) + 1,
				}))
				return hErr
			},
			wantCode:   connect.CodeInvalidArgument,
			wantReason: "EINVAL",
		},
		{
			name: "Write 陈旧 Epoch 拦截",
			invoke: func() error {
				_, hErr := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					DiskId: diskID, Epoch: 1, Ino: 1, Data: []byte("stale"),
				}))
				return hErr
			},
			wantCode:   connect.CodeFailedPrecondition,
			wantReason: "EBUSY",
		},
		{
			name: "Rmdir 非空目录拦截",
			invoke: func() error {
				_, hErr := client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
					DiskId: diskID, Epoch: 2, ParentIno: 1, Name: []byte("nonempty"),
				}))
				return hErr
			},
			wantCode:   connect.CodeFailedPrecondition,
			wantReason: "ENOTEMPTY",
		},
		{
			name: "Lookup 目标不存在",
			invoke: func() error {
				_, hErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
					DiskId: diskID, ParentIno: 1, Name: []byte("missing"),
				}))
				return hErr
			},
			wantCode:   connect.CodeNotFound,
			wantReason: "ENOENT",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireConnectErrorCodeAndReason(t, tc.invoke(), tc.wantCode, tc.wantReason)
		})
	}

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, cErr := srv.GetAttr(canceledCtx, connect.NewRequest(&storagev1.GetAttrRequest{DiskId: diskID, Ino: 1}))
	require.Equal(t, connect.CodeCanceled, connect.CodeOf(cErr), "已取消上下文应返回 CodeCanceled: got=%v", cErr)
}

func TestServerFSRPC_CreateTmpfile_ProcedureRegisteredInMetrics(t *testing.T) {

	proc := storagev1connect.StorageServiceCreateTmpfileProcedure

	got := normalizeProcedure(proc)

	require.Equal(t, proc, got, "CreateTmpfile procedure must be registered in knownProcedures: want=%q got=%q", proc, got)
}

func TestServerFSRPC_CreateTmpfile_LifecyclePromotionAndOrphanReclaim(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(
		t,
		control.WithEntryTimeout(1.5),
		control.WithAttrTimeout(3.5),
	)
	diskID, epoch1 := createAndMountTestDisk(t, client)

	linkableResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Mode:      store.S_IFREG | 0o640,
		Uid:       1000,
		Gid:       2000,
		Exclusive: false,
	}))
	require.NoError(t, err, "CreateTmpfile(exclusive=false) RPC failed")
	linkableIno := linkableResp.Msg.GetInode().GetIno()
	require.NotZero(t, linkableIno, "CreateTmpfile must allocate a positive inode number")
	require.Zero(t, linkableResp.Msg.GetInode().GetAttr().GetNlink(), "unpromoted tmpfile must start with nlink=0")
	require.Equal(t, store.S_IFREG|0o640, linkableResp.Msg.GetInode().GetAttr().GetMode(), "tmpfile mode mismatch")
	require.Equal(t, uint32(1000), linkableResp.Msg.GetInode().GetAttr().GetUid(), "tmpfile uid mismatch")
	require.Equal(t, uint32(2000), linkableResp.Msg.GetInode().GetAttr().GetGid(), "tmpfile gid mismatch")
	require.InDelta(t, 1.5, linkableResp.Msg.GetEntryTimeoutSeconds(), 1e-9, "entry timeout mismatch")
	require.InDelta(t, 3.5, linkableResp.Msg.GetAttrTimeoutSeconds(), 1e-9, "attr timeout mismatch")

	dirBeforeLink, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		DiskId: diskID,
		Ino:    1,
		Offset: 0,
		Limit:  16,
	}))
	require.NoError(t, err, "ReadDir before Link failed")
	require.Len(t, dirBeforeLink.Msg.GetEntries(), 2, "unpromoted tmpfile must not add a directory entry")

	payload := bytes.Repeat([]byte{0xA5}, 8192)
	writeResp, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    linkableIno,
		Offset: 0,
		Data:   payload,
	}))
	require.NoError(t, err, "Write to linkable tmpfile failed")
	require.Equal(t, uint32(8192), writeResp.Msg.GetSize(), "Write byte count mismatch")

	_, err = client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    linkableIno,
		Offset: 1024,
		Length: 2048,
		Mode:   store.FallocPunchHole | store.FallocKeepSize,
	}))
	require.NoError(t, err, "Fallocate punch-hole on tmpfile failed")

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    linkableIno,
		Offset: 0,
		Size:   8192,
	}))
	require.NoError(t, err, "Read from tmpfile failed")
	wantData := bytes.Repeat([]byte{0xA5}, 8192)
	clear(wantData[1024:3072])
	require.Equal(t, wantData, readResp.Msg.GetData(), "punched hole data mismatch on tmpfile")

	linkResp, err := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId:       diskID,
		Epoch:        epoch1,
		Ino:          linkableIno,
		NewParentIno: 1,
		NewName:      []byte("promoted.bin"),
	}))
	require.NoError(t, err, "Link promotion of linkable tmpfile failed")
	require.Equal(t, linkableIno, linkResp.Msg.GetInode().GetIno(), "promoted inode number mismatch")
	require.Equal(t, uint32(1), linkResp.Msg.GetInode().GetAttr().GetNlink(), "promoted inode nlink mismatch")

	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: 1,
		Name:      []byte("promoted.bin"),
	}))
	require.NoError(t, err, "Lookup promoted tmpfile failed")
	require.Equal(t, linkableIno, lookupResp.Msg.GetInode().GetIno(), "Lookup inode mismatch")
	require.Equal(t, uint32(1), lookupResp.Msg.GetInode().GetAttr().GetNlink(), "Lookup nlink mismatch")
	require.Equal(t, uint64(8192), lookupResp.Msg.GetInode().GetAttr().GetSize(), "Lookup size mismatch")

	statAfterPromote, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err, "StatFS after promotion failed")
	require.Equal(t, uint64(8192), statAfterPromote.Msg.GetUsedBytes(), "StatFS UsedBytes after promotion mismatch")
	require.Equal(t, uint64(2), statAfterPromote.Msg.GetFiles(), "StatFS Files after promotion mismatch")

	exclResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Mode:      store.S_IFREG | 0o600,
		Exclusive: true,
	}))
	require.NoError(t, err, "CreateTmpfile(exclusive=true) RPC failed")
	exclIno := exclResp.Msg.GetInode().GetIno()
	require.Zero(t, exclResp.Msg.GetInode().GetAttr().GetNlink(), "exclusive tmpfile must start with nlink=0")

	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    exclIno,
		Offset: 0,
		Data:   bytes.Repeat([]byte{0x5B}, 4096),
	}))
	require.NoError(t, err, "Write to exclusive tmpfile failed")

	statWithExcl, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err, "StatFS with exclusive tmpfile failed")
	require.Equal(t, uint64(12288), statWithExcl.Msg.GetUsedBytes(), "StatFS UsedBytes with exclusive tmpfile mismatch")
	require.Equal(t, uint64(3), statWithExcl.Msg.GetFiles(), "StatFS Files with exclusive tmpfile mismatch")

	_, linkExclErr := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId:       diskID,
		Epoch:        epoch1,
		Ino:          exclIno,
		NewParentIno: 1,
		NewName:      []byte("exclusive.bin"),
	}))
	requireConnectErrorCodeAndReason(t, linkExclErr, connect.CodeNotFound, "ENOENT")

	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    exclIno,
	}))
	require.NoError(t, err, "ReclaimOrphan on exclusive tmpfile failed")

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Ino:    exclIno,
	}))
	requireConnectErrorCodeAndReason(t, err, connect.CodeNotFound, "ENOENT")

	statAfterReclaim, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err, "StatFS after ReclaimOrphan failed")
	require.Equal(t, uint64(8192), statAfterReclaim.Msg.GetUsedBytes(), "StatFS UsedBytes after ReclaimOrphan mismatch")
	require.Equal(t, uint64(2), statAfterReclaim.Msg.GetFiles(), "StatFS Files after ReclaimOrphan mismatch")

	lingeringResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Mode:      store.S_IFREG | 0o600,
		Exclusive: false,
	}))
	require.NoError(t, err, "CreateTmpfile for lingering orphan failed")
	lingeringIno := lingeringResp.Msg.GetInode().GetIno()

	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    lingeringIno,
		Offset: 0,
		Data:   bytes.Repeat([]byte{0x7C}, 2048),
	}))
	require.NoError(t, err, "Write to lingering tmpfile failed")

	mountResp2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: diskID,
		Force:  true,
	}))
	require.NoError(t, err, "MountDisk epoch advance failed")
	require.Equal(t, uint64(2), mountResp2.Msg.GetEpoch(), "epoch after remount mismatch")

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Ino:    lingeringIno,
	}))
	requireConnectErrorCodeAndReason(t, err, connect.CodeNotFound, "ENOENT")

	statAfterMount, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err, "StatFS after remount cleanup failed")
	require.Equal(t, uint64(8192), statAfterMount.Msg.GetUsedBytes(), "StatFS UsedBytes after remount cleanup mismatch")
	require.Equal(t, uint64(2), statAfterMount.Msg.GetFiles(), "StatFS Files after remount cleanup mismatch")

	readAfterMount, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    linkableIno,
		Offset: 0,
		Size:   8192,
	}))
	require.NoError(t, err, "Read promoted file after remount failed")
	require.Equal(t, wantData, readAfterMount.Msg.GetData(), "promoted file data must survive remount orphan cleanup")
}

type readOnlyActorManager struct {
	control.Manager
	diskID  string
	roActor *store.StoreActor
}

func (m *readOnlyActorManager) Actor(ctx context.Context, diskID string) (*store.StoreActor, error) {
	if diskID == m.diskID {
		return m.roActor, nil
	}
	return m.Manager.Actor(ctx, diskID)
}

func TestServerFSRPC_CreateTmpfile_StaleEpoch_RejectsWithEBUSY(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t)
	diskID, _ := createAndMountTestDisk(t, client)

	remountResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: diskID,
		Force:  true,
	}))
	require.NoError(t, err, "second MountDisk failed")
	require.Equal(t, uint64(2), remountResp.Msg.GetEpoch(), "second mount must advance epoch to 2")

	cases := []struct {
		name  string
		epoch uint64
	}{
		{name: "epoch lower than current", epoch: 1},
		{name: "epoch higher than current", epoch: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, callErr := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
				DiskId:    diskID,
				Epoch:     tc.epoch,
				ParentIno: 1,
				Mode:      store.S_IFREG | 0o600,
			}))
			requireConnectErrorCodeAndReason(t, callErr, connect.CodeFailedPrecondition, "EBUSY")
		})
	}
}

func TestServerFSRPC_CreateTmpfile_ReadOnlyAndUnmounted_RejectsWrite(t *testing.T) {
	ctx := context.Background()

	t.Run("read-only mount returns epoch 0 which CreateTmpfile rejects as EINVAL", func(t *testing.T) {

		_, client := newTestServerAndClient(t)
		created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "ro-mount-disk",
			MaxBytes: 16 << 20,
		}))
		require.NoError(t, err, "CreateDisk failed")
		diskID := created.Msg.GetDisk().GetDiskId()

		roMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
			DiskId:   diskID,
			ReadOnly: true,
		}))
		require.NoError(t, err, "read-only MountDisk failed")
		require.Zero(t, roMount.Msg.GetEpoch(), "read-only mount must return epoch 0")

		_, callErr := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
			DiskId:    diskID,
			Epoch:     roMount.Msg.GetEpoch(),
			ParentIno: 1,
			Mode:      store.S_IFREG | 0o600,
		}))
		requireConnectErrorCodeAndReason(t, callErr, connect.CodeInvalidArgument, "EINVAL")
	})

	t.Run("unmounted disk rejects epoch 1 with EBUSY", func(t *testing.T) {

		_, client := newTestServerAndClient(t)
		created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "unmounted-disk",
			MaxBytes: 16 << 20,
		}))
		require.NoError(t, err, "CreateDisk failed")
		diskID := created.Msg.GetDisk().GetDiskId()

		_, callErr := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
			DiskId:    diskID,
			Epoch:     1,
			ParentIno: 1,
			Mode:      store.S_IFREG | 0o600,
		}))
		requireConnectErrorCodeAndReason(t, callErr, connect.CodeFailedPrecondition, "EBUSY")
	})

	t.Run("read-only StoreActor rejects CreateTmpfile with EPERM", func(t *testing.T) {

		roDiskID := "018f0000-0000-7000-8000-000000000042"
		roDir := t.TempDir()

		seedActor, err := store.Open(store.Options{Dir: roDir, DiskID: roDiskID})
		require.NoError(t, err, "seed store.Open failed")
		epoch, _, err := seedActor.AdvanceEpoch(ctx)
		require.NoError(t, err, "seed AdvanceEpoch failed")
		require.NoError(t, seedActor.Close(), "seed Close failed")

		roActor, err := store.Open(store.Options{Dir: roDir, DiskID: roDiskID, ReadOnly: true})
		require.NoError(t, err, "read-only store.Open failed")
		t.Cleanup(func() { _ = roActor.Close() })

		baseMgr := control.New(db.NewMemStore(), control.WithStorageRoot(t.TempDir()))
		t.Cleanup(func() { _ = baseMgr.Close() })

		roMgr := &readOnlyActorManager{
			Manager: baseMgr,
			diskID:  roDiskID,
			roActor: roActor,
		}
		srv, err := New(Params{
			Config: config.ServerConfig{
				ServerPort:         0,
				InternalServerPort: 0,
				ServerReadMaxBytes: 8 << 20,
			},
			Control: roMgr,
		})
		require.NoError(t, err, "server.New with readOnlyActorManager failed")

		httpClient := &http.Client{Transport: memoryRoundTripper{handler: srv.Handler()}}
		roClient := storagev1connect.NewStorageServiceClient(httpClient, "http://storage.local")

		_, callErr := roClient.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
			DiskId:    roDiskID,
			Epoch:     epoch,
			ParentIno: 1,
			Mode:      store.S_IFREG | 0o600,
		}))
		requireConnectErrorCodeAndReason(t, callErr, connect.CodePermissionDenied, "EPERM")
	})
}

func TestServerFSRPC_CreateTmpfile_ValidationAndDomainErrors_RejectsInvalidRequests(t *testing.T) {

	ctx := context.Background()
	srv, client := newTestServerAndClient(t)
	diskID, epoch := createAndMountTestDisk(t, client)

	regResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("regular.txt"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err, "Create regular file failed")
	regIno := regResp.Msg.GetInode().GetIno()

	nilUUID := "00000000-0000-0000-0000-000000000000"
	unknownUUID := "018f0000-0000-7000-8000-000000000099"

	cases := []struct {
		name       string
		req        *storagev1.CreateTmpfileRequest
		wantCode   connect.Code
		wantReason string
	}{
		{
			name: "invalid disk_id",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: "invalid-uuid", Epoch: epoch, ParentIno: 1, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "nil disk_id",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: nilUUID, Epoch: epoch, ParentIno: 1, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "unknown disk_id",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: unknownUUID, Epoch: epoch, ParentIno: 1, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeNotFound, wantReason: "ENOENT",
		},
		{
			name: "epoch 0",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: 0, ParentIno: 1, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "epoch above MaxInt64",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: uint64(math.MaxInt64) + 1, ParentIno: 1, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "parent_ino 0",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: epoch, ParentIno: 0, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "parent_ino above MaxInt64",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: epoch, ParentIno: uint64(math.MaxInt64) + 1, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "uid above MaxInt32",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: epoch, ParentIno: 1, Mode: store.S_IFREG | 0o600, Uid: math.MaxInt32 + 1,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "gid above MaxInt32",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: epoch, ParentIno: 1, Mode: store.S_IFREG | 0o600, Gid: math.MaxInt32 + 1,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "directory mode",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: epoch, ParentIno: 1, Mode: store.S_IFDIR | 0o755,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "mode out of range",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: epoch, ParentIno: 1, Mode: 0o200000,
			},
			wantCode: connect.CodeInvalidArgument, wantReason: "EINVAL",
		},
		{
			name: "missing parent directory",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: epoch, ParentIno: 999999, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeNotFound, wantReason: "ENOENT",
		},
		{
			name: "non-directory parent",
			req: &storagev1.CreateTmpfileRequest{
				DiskId: diskID, Epoch: epoch, ParentIno: regIno, Mode: store.S_IFREG | 0o600,
			},
			wantCode: connect.CodeFailedPrecondition, wantReason: "ENOTDIR",
		},
	}

	for _, tc := range cases {
		t.Run("client/"+tc.name, func(t *testing.T) {
			_, callErr := client.CreateTmpfile(ctx, connect.NewRequest(tc.req))
			requireConnectErrorCodeAndReason(t, callErr, tc.wantCode, tc.wantReason)
		})
		t.Run("direct/"+tc.name, func(t *testing.T) {
			_, callErr := srv.CreateTmpfile(ctx, connect.NewRequest(tc.req))
			requireConnectErrorCodeAndReason(t, callErr, tc.wantCode, tc.wantReason)
		})
	}

	t.Run("direct/canceled context", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, callErr := srv.CreateTmpfile(canceledCtx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
			DiskId:    diskID,
			Epoch:     epoch,
			ParentIno: 1,
			Mode:      store.S_IFREG | 0o600,
		}))
		require.Error(t, callErr, "canceled CreateTmpfile must return an error")
		require.Equal(t, connect.CodeCanceled, connect.CodeOf(callErr), "canceled CreateTmpfile must return CodeCanceled: got=%v", callErr)
	})
}
