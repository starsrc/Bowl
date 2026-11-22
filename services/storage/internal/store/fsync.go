package store

import "context"

type fsyncOp struct {
	ino uint64
}

func (s *StoreActor) Fsync(ctx context.Context, ino uint64, epoch ...uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return s.submitStaleEpoch(ctx, opFsync, ep)
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}

	cmd := s.acquireCmd()
	cmd.op = opFsync
	cmd.epoch = ep
	cmd.req = &fsyncOp{ino: ino}
	return s.submit(ctx, cmd)
}

func (s *StoreActor) FsyncDir(ctx context.Context, ino uint64, epoch ...uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return s.submitStaleEpoch(ctx, opFsyncDir, ep)
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}

	cmd := s.acquireCmd()
	cmd.op = opFsyncDir
	cmd.epoch = ep
	cmd.req = &fsyncOp{ino: ino}
	return s.submit(ctx, cmd)
}

func (s *StoreActor) validateFsyncCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*fsyncOp)
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	if err := s.checkInodeLive(s.batch, &inode); err != nil {
		return err
	}
	if inode.Mode&S_IFMT == S_IFDIR {
		return ErrIsDir
	}
	return nil
}

func (s *StoreActor) mutateFsyncCmd(cmd *writeCmd) {
	cmd.syncBarrier = true
}

func (s *StoreActor) validateFsyncDirCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*fsyncOp)
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	if err := s.checkInodeLive(s.batch, &inode); err != nil {
		return err
	}
	if inode.Mode&S_IFMT != S_IFDIR {
		return ErrNotDir
	}
	return nil
}

func (s *StoreActor) mutateFsyncDirCmd(cmd *writeCmd) {
	cmd.syncBarrier = true
}
