//go:build !linux

package fusefs

// macOS FUSE 协议没有 FALLOCATE 操作：这两个取值只用于让共享实现编译通过，
// 非 Linux 平台不会走到 Fallocate 的判定分支。
const (
	fallocFlKeepSize  = 0x01
	fallocFlPunchHole = 0x02
)
