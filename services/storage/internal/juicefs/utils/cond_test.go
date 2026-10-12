package utils

import (
	"sync"
	"testing"
	"time"
)

func TestCond(t *testing.T) {
	var m sync.Mutex
	c := NewCond(&m)
	var ready bool
	start := time.Now()
	go func() {
		for i := 0; i < 10; i++ {
			m.Lock()
			ready = true
			c.Signal()
			for ready {
				c.WaitWithTimeout(time.Millisecond * 100)
			}
			m.Unlock()
		}
	}()
	for i := 0; i < 10; i++ {
		m.Lock()
		for !ready {
			c.WaitWithTimeout(time.Millisecond * 100)
		}
		ready = false
		c.Signal()
		m.Unlock()
	}
	if ready {
		t.Fatalf("任务完成时 ready 应当为 false")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("任务执行超时")
	}

	// 测试超时退出
	done := make(chan bool)
	var timeout bool
	go func() {
		m.Lock()
		defer m.Unlock()
		timeout = c.WaitWithTimeout(time.Millisecond * 10)
		done <- true
	}()
	select {
	case <-done:
		if !timeout {
			t.Fatalf("应当触发超时")
		}
	case <-time.After(time.Second):
		t.Fatalf("WaitWithTimeout 超过 1 秒未返回")
	}
}
