package server

import (
	"bytes"
	"context"
	"runtime"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpccodec"
	"github.com/starsrc/bowl/pkg/validator"
)

const (
	codecTestDiskID = "123e4567-e89b-12d3-a456-426614174000"
	codecTestBranch = "main"
)

func newSampleWritePayload() []byte {
	return bytes.Repeat([]byte{0x11, 0x22, 0x33, 0x44}, maxReadWriteSize/4)
}

func marshalSampleWriteRequest(t *testing.T, codec connect.Codec, payload []byte) ([]byte, *storagev1.WriteRequest) {
	t.Helper()
	orig := &storagev1.WriteRequest{
		Ino:    42,
		Fh:     7,
		Offset: 8 << 20,
		Data:   payload,
		DiskId: codecTestDiskID,
		Epoch:  3,
		Branch: codecTestBranch,
	}
	raw, err := codec.Marshal(orig)
	require.NoError(t, err)
	return raw, orig
}

func unmarshalAndReleaseWritePayload(t *testing.T, codec connect.Codec, raw []byte) {
	t.Helper()
	var m storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(raw, &m))
	releaseWritePayload(&m)
}

func repeatUnmarshalAndReleaseWrite(t *testing.T, codec connect.Codec, raw []byte, count int) {
	t.Helper()
	for range count {
		unmarshalAndReleaseWritePayload(t, codec, raw)
	}
}

func unmarshalAbandonedWriteRequest(t *testing.T, codec connect.Codec, raw []byte) {
	t.Helper()
	msg := &storagev1.WriteRequest{}
	require.NoError(t, codec.Unmarshal(raw, msg))
	require.Equal(t, 1, leasedWritePayloadCount())
	runtime.KeepAlive(msg)
}

func registerAbandonedReadResponse(t *testing.T) {
	t.Helper()
	buf := acquireReadPayloadBuf()
	buf[0] = 0x99
	resp := &storagev1.ReadResponse{
		Data: buf[:4096:4096],
	}
	registerLeasedReadPayload(resp, buf)
	require.Equal(t, 1, leasedReadPayloadCount())
	runtime.KeepAlive(resp)
}

func marshalAndReleaseReadPayload(t *testing.T, codec *rpccodec.ProtoBinaryCodec, dst []byte) []byte {
	t.Helper()
	buf := acquireReadPayloadBuf()
	resp := &storagev1.ReadResponse{
		Data: buf[:maxReadWriteSize:maxReadWriteSize],
	}
	registerLeasedReadPayload(resp, buf)
	out, err := codec.MarshalAppend(dst[:0], resp)
	require.NoError(t, err)
	require.Nil(t, resp.Data)
	require.Zero(t, leasedReadPayloadCount())
	return out
}

func repeatMarshalAndReleaseRead(t *testing.T, codec *rpccodec.ProtoBinaryCodec, dst []byte, count int) []byte {
	t.Helper()
	for range count {
		dst = marshalAndReleaseReadPayload(t, codec, dst)
	}
	return dst
}

func waitForZeroLeases(t *testing.T, countFn func() int, targetName string) {
	t.Helper()
	const maxGCCycles = 200
	for range maxGCCycles {
		runtime.GC()
		runtime.Gosched()
		if countFn() == 0 {
			return
		}
	}
	require.Zero(t, countFn(), "orphan %s lease not collected after %d GC cycles", targetName, maxGCCycles)
}

func assertZeroAllocsMarshalAppend(t *testing.T, codec *rpccodec.ProtoBinaryCodec, dst []byte, resp *storagev1.ReadResponse, checkNonEmpty bool) {
	t.Helper()
	allocs := testing.AllocsPerRun(50, func() {
		out, err := codec.MarshalAppend(dst[:0], resp)
		if err != nil {
			t.Fatalf("MarshalAppend failed: %v", err)
		}
		if checkNonEmpty && len(out) <= maxReadWriteSize {
			t.Fatalf("unexpected non-empty ReadResponse length: %d", len(out))
		}
		if !checkNonEmpty && len(out) != 0 {
			t.Fatalf("expected empty ReadResponse length 0, got %d", len(out))
		}
	})
	require.Zero(t, allocs)
}

func buildPayloadOfSize(sz int) []byte {
	if sz <= 0 {
		return nil
	}
	return bytes.Repeat([]byte{0x5A, 0xA5, 0x3C, 0xC3}, (sz+3)/4)[:sz]
}

func assertReadResponseCompatSize(t *testing.T, codec *rpccodec.ProtoBinaryCodec, dst []byte, sz int) {
	t.Helper()
	payload := buildPayloadOfSize(sz)
	resp := &storagev1.ReadResponse{Data: payload}

	want, err := proto.Marshal(resp)
	require.NoError(t, err)

	got, err := codec.MarshalAppend(dst[:0], resp)
	require.NoError(t, err)
	require.Equal(t, want, got)

	var decoded storagev1.ReadResponse
	require.NoError(t, proto.Unmarshal(got, &decoded))
	if sz == 0 {
		require.Empty(t, decoded.Data)
	} else {
		require.Equal(t, payload, decoded.Data)
	}

	prefix := []byte("frame:")
	wantPrefixed, err := proto.MarshalOptions{}.MarshalAppend(append([]byte(nil), prefix...), resp)
	require.NoError(t, err)
	gotPrefixed, err := codec.MarshalAppend(append([]byte(nil), prefix...), resp)
	require.NoError(t, err)
	require.Equal(t, wantPrefixed, gotPrefixed)
}

func assertWriteWireFallbackCase(t *testing.T, codec connect.Codec, wire []byte, target *storagev1.WriteRequest, wantErr bool, wantData []byte) {
	t.Helper()
	err := codec.Unmarshal(wire, target)
	if wantErr {
		require.Error(t, err)
		require.Zero(t, leasedWritePayloadCount())
		return
	}
	require.NoError(t, err)
	if len(wantData) == 0 {
		require.Empty(t, target.Data)
	} else {
		require.Equal(t, wantData, target.Data)
	}
	require.Zero(t, leasedWritePayloadCount())
}

func TestWriteCodec_UnmarshalAndRelease_IsolatesInputAndReclaimsPool(t *testing.T) {
	codec := newProtoBinaryCodec()
	require.Equal(t, "proto", codec.Name())

	payload := newSampleWritePayload()
	raw, orig := marshalSampleWriteRequest(t, codec, payload)

	rawMut := append([]byte(nil), raw...)
	var decoded storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(rawMut, &decoded))
	clear(rawMut)

	require.Equal(t, orig.Ino, decoded.Ino)
	require.Equal(t, orig.Fh, decoded.Fh)
	require.Equal(t, orig.Offset, decoded.Offset)
	require.Equal(t, orig.DiskId, decoded.DiskId)
	require.Equal(t, orig.Epoch, decoded.Epoch)
	require.Equal(t, orig.Branch, decoded.Branch)
	require.Equal(t, payload, decoded.Data)
	require.Equal(t, maxReadWriteSize, cap(decoded.Data))
	require.Equal(t, 1, leasedWritePayloadCount())

	releaseWritePayload(&decoded)
	require.Nil(t, decoded.Data)
	require.Zero(t, leasedWritePayloadCount())
}

func TestWriteCodec_PooledUnmarshal_StaysWithinLowAllocBudget(t *testing.T) {
	codec := newProtoBinaryCodec()
	raw, _ := marshalSampleWriteRequest(t, codec, newSampleWritePayload())
	unmarshalAndReleaseWritePayload(t, codec, raw)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	repeatUnmarshalAndReleaseWrite(t, codec, raw, 8)

	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10))
	require.Zero(t, leasedWritePayloadCount())
}

func TestWriteCodec_MalformedOrFallbackWire_NeverLeaksPoolLease(t *testing.T) {
	codec := newProtoBinaryCodec()
	payload := newSampleWritePayload()
	raw, _ := marshalSampleWriteRequest(t, codec, payload)

	emptyRaw, err := proto.Marshal(&storagev1.WriteRequest{
		Ino:    9,
		DiskId: codecTestDiskID,
		Epoch:  1,
		Branch: codecTestBranch,
	})
	require.NoError(t, err)

	dupZero := protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), raw...), 4, protowire.BytesType), nil)
	oversized := bytes.Repeat([]byte{0x7e}, maxReadWriteSize+1)
	dupOversized := protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), raw...), 4, protowire.BytesType), oversized)
	unknownTail := protowire.AppendVarint(protowire.AppendTag(append([]byte(nil), raw...), 99, protowire.VarintType), 12345)
	mismatchWire := protowire.AppendFixed64(protowire.AppendTag(append([]byte(nil), raw...), 1, protowire.Fixed64Type), 99)
	truncatedTail := append(append([]byte(nil), raw...), 0xff, 0xff, 0xff)
	illegalWire := protowire.AppendTag(append([]byte(nil), raw...), 1, protowire.Type(6))
	badUTF8Disk := protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), raw...), 5, protowire.BytesType), []byte{0xff, 0xfe})
	badUTF8Branch := protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), raw...), 7, protowire.BytesType), []byte{0xff, 0xfe})

	cases := []struct {
		name     string
		wire     []byte
		target   *storagev1.WriteRequest
		wantErr  bool
		wantData []byte
	}{
		{name: "empty payload does not lease", wire: emptyRaw, target: &storagev1.WriteRequest{}},
		{name: "trailing empty field releases lease", wire: dupZero, target: &storagev1.WriteRequest{}, wantData: nil},
		{name: "trailing oversized field releases lease", wire: dupOversized, target: &storagev1.WriteRequest{}, wantData: oversized},
		{name: "unknown field falls back", wire: unknownTail, target: &storagev1.WriteRequest{}, wantData: payload},
		{name: "mismatched wire falls back", wire: mismatchWire, target: &storagev1.WriteRequest{}, wantData: payload},
		{name: "truncated tail fails cleanly", wire: truncatedTail, target: &storagev1.WriteRequest{}, wantErr: true},
		{name: "illegal wire type fails cleanly", wire: illegalWire, target: &storagev1.WriteRequest{}, wantErr: true},
		{name: "bad UTF-8 disk id fails cleanly", wire: badUTF8Disk, target: &storagev1.WriteRequest{}, wantErr: true},
		{name: "bad UTF-8 branch fails cleanly", wire: badUTF8Branch, target: &storagev1.WriteRequest{}, wantErr: true},
		{name: "nil target fails cleanly", wire: raw, target: nil, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertWriteWireFallbackCase(t, codec, tc.wire, tc.target, tc.wantErr, tc.wantData)
		})
	}
}

func TestWriteCodec_OrphanCleanupAndInterceptorRelease(t *testing.T) {
	codec := newProtoBinaryCodec()
	raw, _ := marshalSampleWriteRequest(t, codec, newSampleWritePayload())

	unmarshalAbandonedWriteRequest(t, codec, raw)
	waitForZeroLeases(t, leasedWritePayloadCount, "WriteRequest")

	msg := &storagev1.WriteRequest{}
	require.NoError(t, codec.Unmarshal(raw, msg))
	require.Equal(t, 1, leasedWritePayloadCount())

	val, err := validator.NewStorageValidator()
	require.NoError(t, err)
	releaseInterceptor := newWritePayloadReleaseInterceptor()
	valInterceptor := NewValidationInterceptor(val)

	handler := releaseInterceptor(valInterceptor(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		require.Equal(t, 1, leasedWritePayloadCount())
		return connect.NewResponse(&storagev1.WriteResponse{}), nil
	}))

	_, err = handler(context.Background(), connect.NewRequest(msg))
	require.NoError(t, err)
	require.Nil(t, msg.Data)
	require.Zero(t, leasedWritePayloadCount())
}

func TestReadCodec_MarshalAppendAndOrphanCleanup(t *testing.T) {
	codec := newProtoBinaryCodec()
	dst := make([]byte, 0, maxReadWriteSize+64)

	for _, sz := range []int{0, 1, 127, 128, 4096, 65536, maxReadWriteSize} {
		assertReadResponseCompatSize(t, codec, dst, sz)
	}

	dst = repeatMarshalAndReleaseRead(t, codec, dst, 8)
	require.NotEmpty(t, dst)

	registerAbandonedReadResponse(t)
	waitForZeroLeases(t, leasedReadPayloadCount, "ReadResponse")

	nonEmptyResp := &storagev1.ReadResponse{Data: buildPayloadOfSize(maxReadWriteSize)}
	assertZeroAllocsMarshalAppend(t, codec, dst, nonEmptyResp, true)
	assertZeroAllocsMarshalAppend(t, codec, dst, &storagev1.ReadResponse{}, false)
}
