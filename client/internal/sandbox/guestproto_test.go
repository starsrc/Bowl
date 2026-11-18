package sandbox

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrames_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	big := bytes.Repeat([]byte("x"), maxFrame+10) // 超过单帧上限时要拆成两帧
	_, err := frameWriter{mu: &mu, w: &buf, typ: frameStdout}.Write(big)
	require.NoError(t, err)
	require.NoError(t, writeFrame(&buf, frameStdin, nil))
	require.NoError(t, writeFrame(&buf, frameExit, exitPayload(-3)))

	var got []byte
	for range 2 {
		typ, p, readErr := readFrame(&buf)
		require.NoError(t, readErr)
		assert.Equal(t, frameStdout, typ)
		got = append(got, p...)
	}
	assert.Equal(t, big, got)

	typ, p, err := readFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, frameStdin, typ)
	assert.Empty(t, p)

	typ, p, err = readFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, frameExit, typ)
	code, err := parseExit(p)
	require.NoError(t, err)
	assert.Equal(t, -3, code)
}

func TestReadFrame_RejectsOversizedLength(t *testing.T) {
	_, _, err := readFrame(bytes.NewReader([]byte{frameStdout, 0xff, 0xff, 0xff, 0xff}))
	assert.Error(t, err)
}

func TestIsMountpoint(t *testing.T) {
	dir := t.TempDir()
	assert.False(t, isMountpoint(dir+"/missing"))
	assert.False(t, isMountpoint(dir))
	assert.True(t, isMountpoint("/dev"), "/dev is its own filesystem on Linux and macOS")
}
