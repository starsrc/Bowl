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

	// h2MaxHoldHeadersBytes 限定允许暂存合并的 HEADERS 帧最大字节数。
	// ConnectRPC Unary 请求头经 HPACK 编码后通常仅 50~220 B。
	h2MaxHoldHeadersBytes = 1024

	// h2ScratchBufferBytes 覆盖 HEADERS + 元数据载荷及 4 KiB 小块写的单次连续缓冲区上限，
	// 避免小 IO 热路径产生堆分配；超过该阈值的 4 MiB 大块写自动走 writev (net.Buffers)。
	h2ScratchBufferBytes = 8192

	// h2CoalesceFlushDelay 为极端连接/流控窗口耗尽导致 writeRequestBody 阻塞时的兜底冲刷时限。
	// 正常 Unary 调用中 writeHeaders 与 writeRequestBody 仅相隔数微秒，不会触发定时器回调。
	h2CoalesceFlushDelay = 150 * time.Microsecond
)

// isHoldableH2HeadersFrame 判断给定切片是否为单个完整且后续紧跟请求体 DATA 帧的 HTTP/2 HEADERS 帧
// （Type=HEADERS, END_HEADERS=1, END_STREAM=0）。
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

// h2CoalesceConn 包装客户端明文 HTTP/2 (h2c) 底层连接，将 Go net/http http2ClientConn
// 在单次 Unary POST 中拆分为两次 Flush 的 HEADERS(END_STREAM=0) 帧与紧随其后的 DATA 帧
// 合并为单次底层 Write/writev 系统调用下发。
//
// 背景：Go 标准库 writeHeaders 与 writeRequestBody 分别执行 cc.bw.Flush()，产生两次微小 TCP 写；
// 在 Linux 默认 net.ipv4.tcp_autocorking=1 与虚拟网卡（virtio-net）TX 完成中断聚合环境下，
// 第 1 帧 HEADERS 触发远端服务端进入 HTTP Handler 等待 Body 并开启 TCP Delayed ACK (~4-10ms)，
// 而第 2 帧小 DATA (< size_goal) 被客户端 tcp_should_autocork 挂起在 TSQ_THROTTLED 状态等待前序包释放，
// 形成跨机 ~9-10ms 伪延迟死锁。合并为单次 Write 后，HEADERS+DATA(END_STREAM) 同包到达服务端，彻底消除死锁并减半系统调用。
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
	defer c.mu.Unlock()
	if c.closed || c.pendingLen == 0 || c.asyncErr != nil {
		return
	}
	hdrLen := c.pendingLen
	c.pendingLen = 0
	if _, err := writeFull(c.Conn, c.pendingBuf[:hdrLen]); err != nil {
		c.asyncErr = err
		// 定时冲刷没有调用方可以返回错误：立即关闭底层连接，让 http2 读循环感知断连、Transport 换新连接。
		// 不关闭时，HEADERS 已丢失的那个请求要等到下一次写或自身超时才失败。
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
