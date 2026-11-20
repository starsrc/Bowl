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

const codecTestDiskID = "123e4567-e89b-12d3-a456-426614174000"

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
	}
	raw, err := codec.Marshal(orig)
	require.NoError(t, err, "序列化样例 WriteRequest 失败")
	return raw, orig
}

func unmarshalAndReleaseWritePayload(t *testing.T, codec connect.Codec, raw []byte) {
	t.Helper()
	var m storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(raw, &m), "解包 WriteRequest 失败")
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
	require.NoError(t, codec.Unmarshal(raw, msg), "解包孤儿 WriteRequest 失败")
	require.Equal(t, 1, leasedWritePayloadCount(), "孤儿 WriteRequest 未释放前应持有 1 个租约")
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
	require.Equal(t, 1, leasedReadPayloadCount(), "孤儿 ReadResponse 注册后应持有 1 个租约")
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
	require.NoError(t, err, "MarshalAppend ReadResponse 失败")
	require.Nil(t, resp.Data, "编码完成后必须清空 ReadResponse.Data 引用")
	require.Zero(t, leasedReadPayloadCount(), "编码完成后 ReadResponse 租约必须立即归零")
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
	require.Zero(t, countFn(), "孤儿 %s 租约在 %d 轮 GC 后仍未自动归零", targetName, maxGCCycles)
}

func assertZeroAllocsMarshalAppend(t *testing.T, codec *rpccodec.ProtoBinaryCodec, dst []byte, resp *storagev1.ReadResponse, checkNonEmpty bool) {
	t.Helper()
	allocs := testing.AllocsPerRun(50, func() {
		out, err := codec.MarshalAppend(dst[:0], resp)
		if err != nil {
			t.Fatalf("MarshalAppend 失败: %v", err)
		}
		if checkNonEmpty && len(out) <= maxReadWriteSize {
			t.Fatalf("非空 ReadResponse 编码长度异常: len=%d", len(out))
		}
		if !checkNonEmpty && len(out) != 0 {
			t.Fatalf("空 ReadResponse 编码长度应为 0，实际=%d", len(out))
		}
	})
	require.Zero(t, allocs, "复用足够容量 dst 时 ReadResponse MarshalAppend 必须 0 allocs/op (checkNonEmpty=%v)", checkNonEmpty)
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
	require.NoError(t, err, "标准 proto.Marshal 失败")

	got, err := codec.MarshalAppend(dst[:0], resp)
	require.NoError(t, err, "手写 MarshalAppend 失败 (size=%d)", sz)
	require.Equal(t, want, got, "size=%d 的 MarshalAppend 编码必须与标准 proto.Marshal 一致", sz)

	var decoded storagev1.ReadResponse
	require.NoError(t, proto.Unmarshal(got, &decoded), "标准 proto.Unmarshal 回读失败")
	if sz == 0 {
		require.Empty(t, decoded.Data, "空载荷回读后应为空")
	} else {
		require.Equal(t, payload, decoded.Data, "size=%d 的载荷回读内容不一致", sz)
	}

	prefix := []byte("frame:")
	wantPrefixed, err := proto.MarshalOptions{}.MarshalAppend(append([]byte(nil), prefix...), resp)
	require.NoError(t, err, "标准前缀追加编码失败")
	gotPrefixed, err := codec.MarshalAppend(append([]byte(nil), prefix...), resp)
	require.NoError(t, err, "手写前缀追加编码失败")
	require.Equal(t, wantPrefixed, gotPrefixed, "size=%d 带前缀切片追加必须与标准实现一致", sz)
}

func TestWriteCodec_UnmarshalAndRelease_IsolatesInputAndReclaimsPool(t *testing.T) {

	codec := newProtoBinaryCodec()
	require.Equal(t, "proto", codec.Name())

	payload := newSampleWritePayload()
	raw, orig := marshalSampleWriteRequest(t, codec, payload)

	rawMut := append([]byte(nil), raw...)
	var decoded storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(rawMut, &decoded), "解包 4 MiB WriteRequest 失败")
	clear(rawMut)

	require.Equal(t, orig.Ino, decoded.Ino)
	require.Equal(t, orig.Fh, decoded.Fh)
	require.Equal(t, orig.Offset, decoded.Offset)
	require.Equal(t, orig.DiskId, decoded.DiskId)
	require.Equal(t, orig.Epoch, decoded.Epoch)
	require.Equal(t, payload, decoded.Data)
	require.Equal(t, maxReadWriteSize, cap(decoded.Data))
	require.Equal(t, 1, leasedWritePayloadCount(), "解包后应持有 1 个写缓冲租约")

	releaseWritePayload(&decoded)
	require.Nil(t, decoded.Data, "释放后 Data 必须置空")
	require.Zero(t, leasedWritePayloadCount(), "释放后租约计数必须归零")
}

func TestWriteCodec_PooledUnmarshal_StaysWithinLowAllocBudget(t *testing.T) {

	codec := newProtoBinaryCodec()
	raw, _ := marshalSampleWriteRequest(t, codec, newSampleWritePayload())
	unmarshalAndReleaseWritePayload(t, codec, raw)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	repeatUnmarshalAndReleaseWrite(t, codec, raw, 8)

	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10), "池化复用下 8 次 4 MiB 解包堆分配超限")
	require.Zero(t, leasedWritePayloadCount(), "连续解包归还后不应残留写租约")
}

func TestWriteCodec_MalformedOrFallbackWire_NeverLeaksPoolLease(t *testing.T) {
	codec := newProtoBinaryCodec()
	payload := newSampleWritePayload()
	raw, _ := marshalSampleWriteRequest(t, codec, payload)

	emptyRaw, err := proto.Marshal(&storagev1.WriteRequest{
		Ino:    9,
		DiskId: codecTestDiskID,
		Epoch:  1,
	})
	require.NoError(t, err)

	dupZero := protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), raw...), 4, protowire.BytesType), nil)

	oversized := bytes.Repeat([]byte{0x7e}, maxReadWriteSize+1)
	dupOversized := protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), raw...), 4, protowire.BytesType), oversized)

	unknownTail := protowire.AppendVarint(protowire.AppendTag(append([]byte(nil), raw...), 99, protowire.VarintType), 12345)
	mismatchWire := protowire.AppendFixed64(protowire.AppendTag(append([]byte(nil), raw...), 1, protowire.Fixed64Type), 99)
	truncatedTail := append(append([]byte(nil), raw...), 0xff, 0xff, 0xff)
	illegalWire := protowire.AppendTag(append([]byte(nil), raw...), 1, protowire.Type(6))
	badUTF8 := protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), raw...), 5, protowire.BytesType), []byte{0xff, 0xfe})

	cases := []struct {
		name     string
		wire     []byte
		target   *storagev1.WriteRequest
		wantErr  bool
		wantData []byte
	}{
		{name: "空载荷请求不借用池缓冲", wire: emptyRaw, target: &storagev1.WriteRequest{}},
		{name: "同包后置空字段覆盖时立即归还先借缓冲", wire: dupZero, target: &storagev1.WriteRequest{}, wantData: nil},
		{name: "同包后置超限载荷覆盖时归还池缓冲", wire: dupOversized, target: &storagev1.WriteRequest{}, wantData: oversized},
		{name: "尾部未知字段触发标准回退并归还池缓冲", wire: unknownTail, target: &storagev1.WriteRequest{}, wantData: payload},
		{name: "标量异构线格式触发标准回退并归还池缓冲", wire: mismatchWire, target: &storagev1.WriteRequest{}, wantData: payload},
		{name: "借池后遇截断变长整数报错且零泄漏", wire: truncatedTail, target: &storagev1.WriteRequest{}, wantErr: true},
		{name: "借池后遇非法线类型6报错且零泄漏", wire: illegalWire, target: &storagev1.WriteRequest{}, wantErr: true},
		{name: "非法UTF8磁盘ID报错且零泄漏", wire: badUTF8, target: &storagev1.WriteRequest{}, wantErr: true},
		{name: "空指针目标报错且零泄漏", wire: raw, target: nil, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertWriteWireFallbackCase(t, codec, tc.wire, tc.target, tc.wantErr, tc.wantData)
		})
	}
}

func assertWriteWireFallbackCase(
	t *testing.T,
	codec connect.Codec,
	wire []byte,
	target *storagev1.WriteRequest,
	wantErr bool,
	wantData []byte,
) {
	t.Helper()
	err := codec.Unmarshal(wire, target)
	if wantErr {
		require.Error(t, err, "畸形线格式或空目标必须返回错误")
		require.Zero(t, leasedWritePayloadCount(), "解包报错路径不得泄漏任何写缓冲租约")
		return
	}
	require.NoError(t, err, "可回退线格式必须成功解码")
	require.Zero(t, leasedWritePayloadCount(), "回退路径或超限/空覆盖路径必须零租约残留")
	if len(wantData) == 0 {
		require.Empty(t, target.GetData(), "空载荷字段预期为空")
		return
	}
	require.Equal(t, wantData, target.GetData(), "回退解码后的 Data 内容不匹配")
}

func TestWriteCodec_ReuseSameRequest_ReleasesPriorLease(t *testing.T) {

	codec := newProtoBinaryCodec()
	raw, _ := marshalSampleWriteRequest(t, codec, newSampleWritePayload())
	emptyRaw, err := proto.Marshal(&storagev1.WriteRequest{
		Ino:    9,
		DiskId: codecTestDiskID,
		Epoch:  1,
	})
	require.NoError(t, err)
	unknownTail := protowire.AppendVarint(protowire.AppendTag(append([]byte(nil), raw...), 99, protowire.VarintType), 12345)

	var reused storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(raw, &reused))
	require.Equal(t, 1, leasedWritePayloadCount(), "首次解包应持有 1 个租约")
	require.NoError(t, codec.Unmarshal(emptyRaw, &reused))
	require.Zero(t, leasedWritePayloadCount(), "复用实例解包空载荷后必须归还前次租约")

	require.NoError(t, codec.Unmarshal(raw, &reused))
	require.Equal(t, 1, leasedWritePayloadCount(), "再次解包应持有 1 个租约")
	require.NoError(t, codec.Unmarshal(unknownTail, &reused))
	require.Zero(t, leasedWritePayloadCount(), "复用实例走标准回退路径后必须归还前次租约")
}

func TestWriteCodec_ValidationRejectAndTypedNil_ReclaimsPoolWithoutPanic(t *testing.T) {

	codec := newProtoBinaryCodec()
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	chain := newWritePayloadReleaseInterceptor()(
		NewValidationInterceptor(v)(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
			t.Fatal("非法请求必须在校验拦截器短路返回")
			return nil, nil
		}),
	)

	rawInvalid, err := codec.Marshal(&storagev1.WriteRequest{
		Ino:    10,
		Data:   bytes.Repeat([]byte{0xab}, 1<<20),
		DiskId: codecTestDiskID,
		Epoch:  0,
	})
	require.NoError(t, err)

	req := connect.NewRequest(&storagev1.WriteRequest{})
	require.NoError(t, codec.Unmarshal(rawInvalid, req.Msg))
	require.Equal(t, 1, leasedWritePayloadCount(), "入链前应持有 1 个写缓冲租约")

	_, callErr := chain(context.Background(), req)

	require.Error(t, callErr)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(callErr))
	require.Zero(t, leasedWritePayloadCount(), "校验拒绝路径必须释放写缓冲租约")
	require.Nil(t, req.Msg.Data, "释放后 Data 必须置空")

	_, nilWriteErr := chain(context.Background(), connect.NewRequest[storagev1.WriteRequest](nil))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(nilWriteErr))

	_, nilReadErr := chain(context.Background(), connect.NewRequest[storagev1.ReadRequest](nil))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(nilReadErr))
}

func TestWriteCodec_AbandonedRequest_ReclaimedByGC(t *testing.T) {

	codec := newProtoBinaryCodec()
	orphanRaw, err := codec.Marshal(&storagev1.WriteRequest{
		Ino:    10,
		Data:   bytes.Repeat([]byte{0x5a}, 64<<10),
		DiskId: codecTestDiskID,
		Epoch:  1,
	})
	require.NoError(t, err)

	unmarshalAbandonedWriteRequest(t, codec, orphanRaw)

	waitForZeroLeases(t, leasedWritePayloadCount, "WriteRequest")
}

func TestReadCodec_MarshalAndMarshalStable_ReclaimsPoolAndPreservesBytes(t *testing.T) {

	codec := newProtoBinaryCodec()
	payload := bytes.Repeat([]byte{0x41, 0x42, 0x43, 0x44}, maxReadWriteSize/4)

	buf1 := acquireReadPayloadBuf()
	copy(buf1[:], payload)
	resp1 := &storagev1.ReadResponse{Data: buf1[:maxReadWriteSize:maxReadWriteSize]}
	registerLeasedReadPayload(resp1, buf1)
	require.Equal(t, 1, leasedReadPayloadCount())

	wireBytes, err := codec.Marshal(resp1)

	require.NoError(t, err)
	require.Nil(t, resp1.Data)
	require.Zero(t, leasedReadPayloadCount())

	var checkResp storagev1.ReadResponse
	require.NoError(t, proto.Unmarshal(wireBytes, &checkResp))
	require.Equal(t, payload, checkResp.Data)

	buf2 := acquireReadPayloadBuf()
	copy(buf2[:4096], payload[:4096])
	resp2 := &storagev1.ReadResponse{Data: buf2[:4096:4096]}
	registerLeasedReadPayload(resp2, buf2)
	_, err = codec.MarshalStable(resp2)
	require.NoError(t, err)
	require.Zero(t, leasedReadPayloadCount())
}

func TestReadCodec_PooledMarshalAppend_StaysWithinLowAllocBudget(t *testing.T) {

	codec := newProtoBinaryCodec()
	frameDst := make([]byte, 0, maxReadWriteSize+64)
	frameDst = marshalAndReleaseReadPayload(t, codec, frameDst)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	_ = repeatMarshalAndReleaseRead(t, codec, frameDst, 8)

	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10), "池化 ReadResponse 编码堆分配超限")
	require.Zero(t, leasedReadPayloadCount(), "连续编码归还后不应残留读租约")
}

func TestReadCodec_AbandonedResponse_ReclaimedByGC(t *testing.T) {

	registerAbandonedReadResponse(t)

	waitForZeroLeases(t, leasedReadPayloadCount, "ReadResponse")
}

func TestReadCodec_MarshalAppend_MatchesStandardProtoAndZeroAllocs(t *testing.T) {
	codec := newProtoBinaryCodec()
	dst := make([]byte, 0, maxReadWriteSize+64)

	sizes := []struct {
		name string
		size int
	}{
		{name: "0字节", size: 0},
		{name: "1字节", size: 1},
		{name: "127字节", size: 127},
		{name: "128字节", size: 128},
		{name: "4KiB", size: 4096},
		{name: "256KiB", size: 256 << 10},
		{name: "4MiB上限", size: maxReadWriteSize},
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			assertReadResponseCompatSize(t, codec, dst, tc.size)
		})
	}

	payload4M := bytes.Repeat([]byte{0x7E}, maxReadWriteSize)
	assertZeroAllocsMarshalAppend(t, codec, dst, &storagev1.ReadResponse{Data: payload4M}, true)
	assertZeroAllocsMarshalAppend(t, codec, dst, &storagev1.ReadResponse{}, false)
}
