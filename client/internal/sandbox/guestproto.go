package sandbox

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// 宿主机和虚机里的执行代理之间的协议。一条 vsock 连接执行一条命令,双方都只收发帧:
// 1 字节类型 + 4 字节大端长度 + 数据。宿主机先发一个 request 帧,之后发 stdin 帧,
// 长度为 0 的 stdin 帧表示 stdin 结束;代理回 stdout、stderr 帧,最后一个 exit 帧带退出码。

// GuestPort 是执行代理在虚机里监听的 vsock 端口。
const GuestPort = 1024

const (
	frameRequest byte = iota
	frameStdin
	frameStdout
	frameStderr
	frameExit
)

// maxFrame 限制单帧大小,防止对端发来一个错误的长度把内存撑爆。
const maxFrame = 16 << 20

type execRequest struct {
	Argv []string `json:"argv"`
	Cwd  string   `json:"cwd"`
	Env  []string `json:"env,omitempty"`
}

func writeFrame(w io.Writer, typ byte, p []byte) error {
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(p))) //nolint:gosec // 调用方保证 len(p) <= maxFrame
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(p) == 0 {
		return nil
	}
	_, err := w.Write(p)
	return err
}

func readFrame(r io.Reader) (typ byte, payload []byte, err error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > maxFrame {
		return 0, nil, fmt.Errorf("帧长度 %d 超过上限", n)
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(r, p); err != nil {
		return 0, nil, err
	}
	return hdr[0], p, nil
}

// frameWriter 把 Write 变成某一类帧。stdout 和 stderr 共用一条连接,所以共用一把锁。
type frameWriter struct {
	mu  *sync.Mutex
	w   io.Writer
	typ byte
}

func (f frameWriter) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for off := 0; off < len(p); off += maxFrame {
		end := min(off+maxFrame, len(p))
		if err := writeFrame(f.w, f.typ, p[off:end]); err != nil {
			return off, err
		}
	}
	return len(p), nil
}

func exitPayload(code int) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(int32(code))) //nolint:gosec // 退出码在 int32 范围内
	return b[:]
}

func parseExit(p []byte) (int, error) {
	if len(p) != 4 {
		return 0, errors.New("exit 帧格式不对")
	}
	return int(int32(binary.BigEndian.Uint32(p))), nil //nolint:gosec // 原样还原 exitPayload 写入的值
}
