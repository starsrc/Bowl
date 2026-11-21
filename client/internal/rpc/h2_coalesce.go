package rpc

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

const (
	h2FrameHeaderLen        = 9
	h2FrameTypeHeaders      = 0x01
	h2FlagHeadersEndStream  = 0x01
	h2FlagHeadersEndHeaders = 0x04

	h2MaxHoldHeadersBytes = 1024

	h2ScratchBufferBytes = 8192

	h2CoalesceFlushDelay = 150 * time.Microsecond
)

func isHoldableH2HeadersFrame(p []byte) bool {
	if len(p) < h2FrameHeaderLen || len(p) > h2MaxHoldHeadersBytes {
		return false
	}
	frameLen := int(p[0])<<16 | int(p[1])<<8 | int(p[2])
	if h2FrameHeaderLen+frameLen != len(p) {
		return false
	}
	if p[3] != h2FrameTypeHeaders {
		return false
	}
	const mask = h2FlagHeadersEndHeaders | h2FlagHeadersEndStream
	if p[4]&mask != h2FlagHeadersEndHeaders {
		return false
	}
	streamID := binary.BigEndian.Uint32(p[5:9]) & 0x7fffffff
	return streamID != 0
}

type coalesceTimer interface {
	Stop() bool
	Reset(d time.Duration) bool
}

type h2CoalesceConn struct {
	net.Conn

	mu         sync.Mutex
	pendingBuf [h2MaxHoldHeadersBytes]byte
	pendingLen int
	scratchBuf [h2ScratchBufferBytes]byte
	afterFunc  func(d time.Duration, f func()) coalesceTimer
	timer      coalesceTimer
	asyncErr   error
	closed     bool
}

func defaultCoalesceAfterFunc(d time.Duration, f func()) coalesceTimer {
	return time.AfterFunc(d, f)
}

func newH2CoalesceConn(conn net.Conn) net.Conn {
	return newH2CoalesceConnWithAfterFunc(conn, defaultCoalesceAfterFunc)
}

func newH2CoalesceConnWithAfterFunc(conn net.Conn, afterFn func(d time.Duration, f func()) coalesceTimer) net.Conn {
	if conn == nil {
		return nil
	}
	if afterFn == nil {
		afterFn = defaultCoalesceAfterFunc
	}
	return &h2CoalesceConn{
		Conn:      conn,
		afterFunc: afterFn,
	}
}

func writeFull(w io.Writer, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := w.Write(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func (c *h2CoalesceConn) flushOnTimer() {
	c.mu.Lock()
	if c.closed || c.pendingLen == 0 || c.asyncErr != nil {
		c.mu.Unlock()
		return
	}
	hdrLen := c.pendingLen
	c.pendingLen = 0
	_, err := writeFull(c.Conn, c.pendingBuf[:hdrLen])
	if err != nil {
		c.asyncErr = err
		// 定时冲刷没有调用方可以返回错误：立即关闭底层连接，让 http2 读循环感知断连、Transport 换新连接。
		// 不关闭时，HEADERS 已丢失的那个请求要等到下一次写或自身超时才失败。
		_ = c.Conn.Close()
	}
	c.mu.Unlock()

	if err != nil {

		_ = c.Conn.Close()
	}
}

func (c *h2CoalesceConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.asyncErr != nil {
		return 0, c.asyncErr
	}
	if c.closed {
		return c.Conn.Write(p)
	}

	if c.pendingLen == 0 {
		if isHoldableH2HeadersFrame(p) {
			copy(c.pendingBuf[:len(p)], p)
			c.pendingLen = len(p)
			if c.timer == nil {
				c.timer = c.afterFunc(h2CoalesceFlushDelay, c.flushOnTimer)
			} else {
				c.timer.Reset(h2CoalesceFlushDelay)
			}
			return len(p), nil
		}
		return writeFull(c.Conn, p)
	}

	if c.timer != nil {
		c.timer.Stop()
	}
	hdrLen := c.pendingLen
	c.pendingLen = 0

	if hdrLen+len(p) <= len(c.scratchBuf) {
		copy(c.scratchBuf[:hdrLen], c.pendingBuf[:hdrLen])
		copy(c.scratchBuf[hdrLen:hdrLen+len(p)], p)
		n, err := writeFull(c.Conn, c.scratchBuf[:hdrLen+len(p)])
		if err != nil {
			if n <= hdrLen {
				return 0, err
			}
			return n - hdrLen, err
		}
		return len(p), nil
	}

	bufs := net.Buffers{c.pendingBuf[:hdrLen], p}
	written, err := bufs.WriteTo(c.Conn)
	if err != nil {
		if written <= int64(hdrLen) {
			return 0, err
		}
		return int(written) - hdrLen, err
	}
	return len(p), nil
}

func (c *h2CoalesceConn) Close() error {
	c.mu.Lock()
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
	}
	c.pendingLen = 0
	c.mu.Unlock()
	return c.Conn.Close()
}
