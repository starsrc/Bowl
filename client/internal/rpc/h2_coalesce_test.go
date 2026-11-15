package rpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func buildTestH2HeadersFrame(streamID uint32, flags byte, payload []byte) []byte {
	frame := make([]byte, h2FrameHeaderLen+len(payload))
	n := len(payload)
	frame[0] = byte(n >> 16)
	frame[1] = byte(n >> 8)
	frame[2] = byte(n)
	frame[3] = h2FrameTypeHeaders
	frame[4] = flags
	binary.BigEndian.PutUint32(frame[5:9], streamID&0x7fffffff)
	copy(frame[h2FrameHeaderLen:], payload)
	return frame
}

func TestH2CoalesceConn_GivenHoldableHeadersFollowedBySmallData_CoalescesIntoSingleWrite(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	var createdTimer *manualCoalesceTimer
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(1, h2FlagHeadersEndHeaders, []byte("hpack-hdr"))
	data := []byte("small-data-frame-body")

	// When
	n1, err1 := conn.Write(headers)
	require.NoError(t, err1)
	require.Equal(t, len(headers), n1)
	require.Empty(t, raw.Writes(), "HEADERS(END_STREAM=0) 应暂存等待紧随的 DATA 帧")

	n2, err2 := conn.Write(data)

	// Then
	require.NoError(t, err2)
	require.Equal(t, len(data), n2)
	require.True(t, createdTimer.stopped, "合并下发后必须停止兜底定时器")
	writes := raw.Writes()
	require.Len(t, writes, 1, "HEADERS 与小 DATA 帧必须合并为单次内部 Write")
	require.Equal(t, append(append([]byte(nil), headers...), data...), writes[0])
}

func TestH2CoalesceConn_GivenHoldableHeadersFollowedByLargeData_WritesCombinedPayloadViaWritev(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		return &manualCoalesceTimer{fn: f}
	})
	headers := buildTestH2HeadersFrame(3, h2FlagHeadersEndHeaders, []byte("hdr"))
	largeData := bytes.Repeat([]byte{0x42}, h2ScratchBufferBytes+512)

	// When
	_, err := conn.Write(headers)
	require.NoError(t, err)
	n2, err := conn.Write(largeData)

	// Then
	require.NoError(t, err)
	require.Equal(t, len(largeData), n2)
	combined := bytes.Join(raw.Writes(), nil)
	require.Equal(t, append(append([]byte(nil), headers...), largeData...), combined)
}

func TestH2CoalesceConn_GivenNonHoldableFrame_FlushesImmediatelyWithoutTimer(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	var timerCreated bool
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		timerCreated = true
		return &manualCoalesceTimer{fn: f}
	})
	endStreamHeaders := buildTestH2HeadersFrame(5, h2FlagHeadersEndHeaders|h2FlagHeadersEndStream, []byte("no-body"))
	settingsFrame := []byte{0, 0, 0, 0x04, 0x00, 0, 0, 0, 0}

	// When
	_, err := conn.Write(endStreamHeaders)
	require.NoError(t, err)
	_, err = conn.Write(settingsFrame)
	require.NoError(t, err)

	// Then
	require.False(t, timerCreated, "非暂存帧不得启动合并定时器")
	writes := raw.Writes()
	require.Len(t, writes, 2)
	require.Equal(t, endStreamHeaders, writes[0])
	require.Equal(t, settingsFrame, writes[1])
}

func TestH2CoalesceConn_GivenTimerExpiresBeforeData_FlushesHeldHeadersAndPropagatesAsyncError(t *testing.T) {
	// Given: 虚拟定时器触发时连接写失败，后续 Write 应感知并传播异步错误
	boom := errors.New("broken pipe")
	raw := &recordingNetConn{failAfter: 0, failErr: boom}
	var createdTimer *manualCoalesceTimer
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(7, h2FlagHeadersEndHeaders, []byte("hdr"))

	// When
	_, err := conn.Write(headers)
	require.NoError(t, err)
	createdTimer.Fire()
	_, nextErr := conn.Write([]byte("next"))

	// Then
	require.ErrorIs(t, nextErr, boom)
	require.True(t, raw.IsClosed(), "冲刷失败后必须关闭底层连接，否则 Transport 不会替换这条已毒化的连接")
}

func TestH2CoalesceConn_GivenTimerExpiresWithHealthyConn_FlushesHeldHeaders(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	var createdTimer *manualCoalesceTimer
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(9, h2FlagHeadersEndHeaders, []byte("hdr"))

	// When
	_, err := conn.Write(headers)
	require.NoError(t, err)
	createdTimer.Fire()

	// Then
	writes := raw.Writes()
	require.Len(t, writes, 1)
	require.Equal(t, headers, writes[0])
}

func TestH2CoalesceConn_WhenClosed_StopsTimerAndClosesUnderlyingConn(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	var createdTimer *manualCoalesceTimer
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(11, h2FlagHeadersEndHeaders, []byte("hdr"))
	_, err := conn.Write(headers)
	require.NoError(t, err)

	// When
	require.NoError(t, conn.Close())

	// Then
	require.True(t, createdTimer.stopped)
	require.True(t, raw.IsClosed())
	require.Nil(t, newH2CoalesceConn(nil))
}
