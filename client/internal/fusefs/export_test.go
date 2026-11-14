package fusefs

import "time"

// SetUnmountFlushTimeoutForTest 仅用于单元测试覆盖 OnUnmount 总刷盘超时预算，返回恢复函数。
func SetUnmountFlushTimeoutForTest(d time.Duration) func() {
	prev := unmountFlushTimeout
	unmountFlushTimeout = d
	return func() {
		unmountFlushTimeout = prev
	}
}
