//go:build linux

package fusefs

import "golang.org/x/sys/unix"

// Linux FUSE 协议通过 FUSE_FALLOCATE 下发预分配与打洞请求。
const (
	fallocFlKeepSize  = unix.FALLOC_FL_KEEP_SIZE
	fallocFlPunchHole = unix.FALLOC_FL_PUNCH_HOLE
)
