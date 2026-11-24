package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func TestHandlerFS_FullPOSIXAndStatFSContract(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, &config.MetaConfig{
		AtimeModeVal:              config.DefaultAtimeMode,
		EntryTimeoutSecondsVal:    1.25,
		AttrTimeoutSecondsVal:     2.25,
		DirEntryTimeoutSecondsVal: 3.25,
		OpenKeepCacheVal:          true,
	})

	disk := env.createDisk(t, "posix-disk", 8*1024*1024)
	diskID := disk.GetDiskId()
	branch := defaultBranch()
	mount := env.mountBranchRW(t, diskID, branch, false)
	epoch := mount.GetEpoch()

	dirResp, err := env.client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: engine.RootIno,
		Name:      []byte("sub"),
		Mode:      0o755,
		Uid:       1000,
		Gid:       1000,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	dirIno := dirResp.Msg.GetInode().GetIno()
	require.InDelta(t, 3.25, dirResp.Msg.GetEntryTimeoutSeconds(), 1e-9)
	require.InDelta(t, 2.25, dirResp.Msg.GetAttrTimeoutSeconds(), 1e-9)

	dotLookup, err := env.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Name:      []byte("."),
	}))
	require.NoError(t, err)
	require.Equal(t, dirIno, dotLookup.Msg.GetInode().GetIno())

	dotDotLookup, err := env.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Name:      []byte(".."),
	}))
	require.NoError(t, err)
	require.Equal(t, engine.RootIno, dotDotLookup.Msg.GetInode().GetIno())

	createResp, err := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Name:      []byte("hello.txt"),
		Mode:      0o644,
		Uid:       1000,
		Gid:       1000,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	fileIno := createResp.Msg.GetInode().GetIno()
	require.Equal(t, uint64(1), createResp.Msg.GetFh())
	require.Equal(t, fopenKeepCache, createResp.Msg.GetOpenFlags())
	require.InDelta(t, 1.25, createResp.Msg.GetEntryTimeoutSeconds(), 1e-9)

	payload := []byte("hello world from pebble-ssot")
	writeResp, err := env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
		Offset: 0,
		Data:   payload,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(len(payload)), writeResp.Msg.GetSize())
	require.Equal(t, uint64(len(payload)), writeResp.Msg.GetInode().GetAttr().GetSize())

	readResp, err := env.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
		Offset: 6,
		Size:   5,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("world"), readResp.Msg.GetData())

	newMode := uint32(0o600)
	newSize := uint64(5)
	setAttrResp, err := env.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		DiskId:   diskID,
		Branch:   branch,
		Ino:      fileIno,
		Mode:     &newMode,
		Size:     &newSize,
		MtimeNow: true,
		Epoch:    epoch,
	}))
	require.NoError(t, err)
	require.Equal(t, engine.ModeRegular|0o600, setAttrResp.Msg.GetInode().GetAttr().GetMode())
	require.Equal(t, uint64(5), setAttrResp.Msg.GetInode().GetAttr().GetSize())

	_, err = env.client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
		Offset: 5,
		Length: 10,
		Mode:   0,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	attrAfterFalloc, err := env.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(15), attrAfterFalloc.Msg.GetInode().GetAttr().GetSize())

	linkResp, err := env.client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId:       diskID,
		Branch:       branch,
		Ino:          fileIno,
		NewParentIno: dirIno,
		NewName:      []byte("hello-hard.txt"),
		Epoch:        epoch,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(2), linkResp.Msg.GetInode().GetAttr().GetNlink())
	require.Zero(t, linkResp.Msg.GetInode().GetAttr().GetFlags()&engine.InodeFlagEverLinked)

	symResp, err := env.client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Name:      []byte("sym"),
		Target:    []byte("hello.txt"),
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	rlResp, err := env.client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    symResp.Msg.GetInode().GetIno(),
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("hello.txt"), rlResp.Msg.GetTarget())

	mknodResp, err := env.client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Name:      []byte("pipe"),
		Mode:      engine.ModeFIFO | 0o644,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	require.Equal(t, engine.ModeFIFO, mknodResp.Msg.GetInode().GetAttr().GetMode()&engine.ModeTypeMask)

	_, err = env.client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
		Name:   []byte("user.tag"),
		Value:  []byte("v1"),
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	gxResp, err := env.client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
		Name:   []byte("user.tag"),
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), gxResp.Msg.GetValue())

	lxResp, err := env.client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
	}))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("user.tag")}, lxResp.Msg.GetNames())

	_, err = env.client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
		Name:   []byte("user.tag"),
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	_, err = env.client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	_, err = env.client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    dirIno,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	rdResp, err := env.client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    dirIno,
		Offset: 0,
		Limit:  32,
	}))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rdResp.Msg.GetEntries()), 6)

	rdpResp, err := env.client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    dirIno,
		Offset: 0,
		Limit:  32,
	}))
	require.NoError(t, err)
	for _, ent := range rdpResp.Msg.GetEntries() {
		name := string(ent.GetName())
		if name == "." || name == ".." {
			require.Zero(t, ent.GetEntryTimeoutSeconds())
		} else {
			require.Greater(t, ent.GetEntryTimeoutSeconds(), 0.0)
		}
	}

	tmpResp, err := env.client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Mode:      0o600,
		Exclusive: true,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	tmpIno := tmpResp.Msg.GetInode().GetIno()
	require.Zero(t, tmpResp.Msg.GetInode().GetAttr().GetNlink())

	_, err = env.client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    tmpIno,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	_, err = env.client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		DiskId:       diskID,
		Branch:       branch,
		OldParentIno: dirIno,
		OldName:      []byte("hello-hard.txt"),
		NewParentIno: dirIno,
		NewName:      []byte("hello-renamed.txt"),
		Epoch:        epoch,
	}))
	require.NoError(t, err)

	_, err = env.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Name:      []byte("hello-renamed.txt"),
		Epoch:     epoch,
	}))
	require.NoError(t, err)

	subEmpty, err := env.client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Name:      []byte("empty"),
		Mode:      0o755,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	require.NotZero(t, subEmpty.Msg.GetInode().GetIno())

	_, err = env.client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: dirIno,
		Name:      []byte("empty"),
		Epoch:     epoch,
	}))
	require.NoError(t, err)

	statResp, err := env.client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(8*1024*1024), statResp.Msg.GetTotalBytes())
	require.Equal(t, uint32(4096), statResp.Msg.GetBsize())
	require.Equal(t, uint32(4096), statResp.Msg.GetFrsize())
	require.Equal(t, uint32(255), statResp.Msg.GetNamelen())
	require.Zero(t, statResp.Msg.GetFfree())
	require.GreaterOrEqual(t, statResp.Msg.GetFiles(), uint64(2))

	_, err = env.client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    999999,
	}))
	requireConnectErr(t, err, connect.CodeNotFound, "ENOENT")
}

func TestHandlerFS_StaleEpochFencesAllMutatingPOSIXRPCs(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, nil)

	disk := env.createDisk(t, "fence-disk", 4*1024*1024)
	diskID := disk.GetDiskId()
	branch := defaultBranch()

	mount1 := env.mountBranchRW(t, diskID, branch, false)
	staleEpoch := mount1.GetEpoch()

	fileResp, err := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: engine.RootIno,
		Name:      []byte("f.txt"),
		Mode:      0o644,
		Epoch:     staleEpoch,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	_, err = env.client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: engine.RootIno,
		Name:      []byte("d"),
		Mode:      0o755,
		Epoch:     staleEpoch,
	}))
	require.NoError(t, err)

	_, err = env.client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		DiskId: diskID,
		Branch: branch,
		Ino:    fileIno,
		Name:   []byte("user.k"),
		Value:  []byte("v"),
		Epoch:  staleEpoch,
	}))
	require.NoError(t, err)

	tmpResp, err := env.client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
		Branch:    branch,
		ParentIno: engine.RootIno,
		Mode:      0o600,
		Exclusive: true,
		Epoch:     staleEpoch,
	}))
	require.NoError(t, err)
	tmpIno := tmpResp.Msg.GetInode().GetIno()

	mount2 := env.mountBranchRW(t, diskID, branch, true)
	require.Equal(t, staleEpoch+1, mount2.GetEpoch())

	modeVal := uint32(0o600)
	staleOps := []struct {
		name string
		call func() error
	}{
		{
			name: "SetAttr",
			call: func() error {
				_, cErr := env.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
					DiskId: diskID, Branch: branch, Ino: fileIno, Mode: &modeVal, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Mknod",
			call: func() error {
				_, cErr := env.client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
					DiskId: diskID, Branch: branch, ParentIno: engine.RootIno, Name: []byte("n"), Mode: engine.ModeFIFO | 0o644, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Mkdir",
			call: func() error {
				_, cErr := env.client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
					DiskId: diskID, Branch: branch, ParentIno: engine.RootIno, Name: []byte("d2"), Mode: 0o755, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Symlink",
			call: func() error {
				_, cErr := env.client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
					DiskId: diskID, Branch: branch, ParentIno: engine.RootIno, Name: []byte("s"), Target: []byte("f.txt"), Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Link",
			call: func() error {
				_, cErr := env.client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
					DiskId: diskID, Branch: branch, Ino: fileIno, NewParentIno: engine.RootIno, NewName: []byte("l"), Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Unlink",
			call: func() error {
				_, cErr := env.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
					DiskId: diskID, Branch: branch, ParentIno: engine.RootIno, Name: []byte("f.txt"), Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Rmdir",
			call: func() error {
				_, cErr := env.client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
					DiskId: diskID, Branch: branch, ParentIno: engine.RootIno, Name: []byte("d"), Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Rename",
			call: func() error {
				_, cErr := env.client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
					DiskId: diskID, Branch: branch, OldParentIno: engine.RootIno, OldName: []byte("f.txt"), NewParentIno: engine.RootIno, NewName: []byte("f2.txt"), Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Create",
			call: func() error {
				_, cErr := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
					DiskId: diskID, Branch: branch, ParentIno: engine.RootIno, Name: []byte("c.txt"), Mode: 0o644, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "CreateTmpfile",
			call: func() error {
				_, cErr := env.client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
					DiskId: diskID, Branch: branch, ParentIno: engine.RootIno, Mode: 0o600, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Write",
			call: func() error {
				_, cErr := env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					DiskId: diskID, Branch: branch, Ino: fileIno, Data: []byte("stale"), Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Fsync",
			call: func() error {
				_, cErr := env.client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
					DiskId: diskID, Branch: branch, Ino: fileIno, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "FsyncDir",
			call: func() error {
				_, cErr := env.client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
					DiskId: diskID, Branch: branch, Ino: engine.RootIno, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "Fallocate",
			call: func() error {
				_, cErr := env.client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
					DiskId: diskID, Branch: branch, Ino: fileIno, Offset: 0, Length: 4096, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "SetXattr",
			call: func() error {
				_, cErr := env.client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
					DiskId: diskID, Branch: branch, Ino: fileIno, Name: []byte("user.k2"), Value: []byte("v2"), Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "RemoveXattr",
			call: func() error {
				_, cErr := env.client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
					DiskId: diskID, Branch: branch, Ino: fileIno, Name: []byte("user.k"), Epoch: staleEpoch,
				}))
				return cErr
			},
		},
		{
			name: "ReclaimOrphan",
			call: func() error {
				_, cErr := env.client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
					DiskId: diskID, Branch: branch, Ino: tmpIno, Epoch: staleEpoch,
				}))
				return cErr
			},
		},
	}

	for _, tc := range staleOps {
		t.Run(tc.name, func(t *testing.T) {
			requireConnectErr(t, tc.call(), connect.CodeFailedPrecondition, "EBUSY")
		})
	}
}
