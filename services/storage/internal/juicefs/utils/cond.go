package utils

import (
	"sync"
	"time"
)

// Cond 支持超时等待的高级条件变量实现
type Cond struct {
	L      sync.Locker
	signal chan struct{}
}

// Signal 唤醒一个等待者，调用方须持有锁 L
func (c *Cond) Signal() {
	select {
	case c.signal <- struct{}{}:
	default:
	}
}

// Broadcast 广播唤醒所有等待者，调用方须持有锁 L
func (c *Cond) Broadcast() {
	close(c.signal)
	c.signal = make(chan struct{})
}

var timerPool = sync.Pool{}

// WaitWithTimeout 等待信号触发或超时退出，若超时返回 true，正常收到信号返回 false
func (c *Cond) WaitWithTimeout(d time.Duration) bool {
	ch := c.signal
	c.L.Unlock()
	var t *time.Timer
	if e := timerPool.Get(); e == nil {
		t = time.NewTimer(d)
	} else {
		t = e.(*time.Timer)
		t.Reset(d)
	}
	defer func() {
		timerPool.Put(t)
		c.L.Lock()
	}()
	select {
	case <-ch:
		if !t.Stop() {
			<-t.C
		}
		return false
	case <-t.C:
		return true
	}
}

// NewCond 创建关联指定锁的条件变量
func NewCond(lock sync.Locker) *Cond {
	return &Cond{lock, make(chan struct{})}
}
