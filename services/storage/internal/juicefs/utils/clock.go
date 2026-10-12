package utils

import "time"

var started = time.Now()

// Now 获取当前系统绝对时间
func Now() time.Time {
	return time.Now()
}

// Clock 获取系统自启动以来经历的时间跨度（用于低开销时延计算）
func Clock() time.Duration {
	return time.Since(started)
}
