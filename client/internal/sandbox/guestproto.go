package sandbox

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

const GuestPort = 1024

const (
	frameRequest byte = iota
	frameStdin
	frameStdout
	frameStderr
	frameExit
)

const maxFrame = 16 << 20

type execRequest struct {
	Argv  []string    `json:"argv"`
	Cwd   string      `json:"cwd"`
	Env   []string    `json:"env,omitempty"`
	TTY   bool        `json:"tty,omitempty"`
	Rows  uint16      `json:"rows,omitempty"`
	Cols  uint16      `json:"cols,omitempty"`
	Reset *guestReset `json:"reset,omitempty"`
}

func writeFrame(w io.Writer, typ byte, p []byte) error {
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(p)))
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
	binary.BigEndian.PutUint32(b[:], uint32(int32(code)))
	return b[:]
}

func parseExit(p []byte) (int, error) {
	if len(p) != 4 {
		return 0, errors.New("exit 帧格式不对")
	}
	return int(int32(binary.BigEndian.Uint32(p))), nil
}
