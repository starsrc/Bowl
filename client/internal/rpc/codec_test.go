package rpc

import (
	"bytes"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func awaitLeasedPayloadDrain(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		runtime.GC()
		return leasedReadPayloadCount() == 0
	}, 2*time.Second, time.Millisecond)
}

func assertSmallPayloadBypassesPool(t *testing.T, src []byte, size int) {
	t.Helper()
	codec := newProtoBinaryCodec()
	smallRaw, err := proto.Marshal(&storagev1.ReadResponse{Data: src[:size]})
	require.NoError(t, err)

	smallMsg := &storagev1.ReadResponse{}
	require.NoError(t, codec.Unmarshal(smallRaw, smallMsg))
	require.Equal(t, 0, leasedReadPayloadCount(), "小载荷 (%d B) 不得注册 4 MiB 租约", size)

	pSmall := TakeReadPayload(smallMsg)
	require.Nil(t, smallMsg.Data)
	require.True(t, bytes.Equal(src[:size], pSmall.Data))
	if size > 0 {
		require.LessOrEqual(t, cap(pSmall.Data), SmallReadPayloadCutoff+64, "小载荷切片不得占用 4 MiB 大数组")
	}
	pSmall.Release()
	pSmall.Release()
	require.Nil(t, pSmall.Data)
}

func assertWriteRequestEquivalence(t *testing.T, tc *storagev1.WriteRequest) {
	t.Helper()
	codec := newProtoBinaryCodec()
	appender, ok := codec.(interface {
		MarshalAppend(dst []byte, msg any) ([]byte, error)
	})
	require.True(t, ok, "protoBinaryCodec 必须实现 MarshalAppend 以复用缓冲池")

	stdWire, err := proto.Marshal(tc)
	require.NoError(t, err)

	gotMarshal, err := codec.Marshal(tc)
	require.NoError(t, err)
	require.True(t, bytes.Equal(stdWire, gotMarshal), "Marshal 编码必须与标准 proto.Marshal 逐字节一致")

	prefix := []byte("prefix-hdr:")
	gotAppend, err := appender.MarshalAppend(append([]byte(nil), prefix...), tc)
	require.NoError(t, err)
	require.Equal(t, prefix, gotAppend[:len(prefix)])
	require.True(t, bytes.Equal(stdWire, gotAppend[len(prefix):]), "MarshalAppend 追加结果必须与标准 proto.Marshal 逐字节一致")

	roundTrip := &storagev1.WriteRequest{}
	require.NoError(t, proto.Unmarshal(gotMarshal, roundTrip))
	require.True(t, proto.Equal(tc, roundTrip), "反序列化后全字段必须完全相等")
}

func TestProtoBinaryCodec_Given4MiBReadResponse_WhenReleased_ReusesPooledBuffer(t *testing.T) {

	codec := newProtoBinaryCodec()
	src := bytes.Repeat([]byte{0x5A, 0xA5, 0x3C, 0xC3}, MaxReadPayloadSize/4)
	raw, err := proto.Marshal(&storagev1.ReadResponse{Data: src})
	require.NoError(t, err)

	msg1 := &storagev1.ReadResponse{}
	require.NoError(t, codec.Unmarshal(raw, msg1))
	require.Equal(t, 1, leasedReadPayloadCount())

	p1 := TakeReadPayload(msg1)
	require.Equal(t, 0, leasedReadPayloadCount())
	require.Nil(t, msg1.Data)
	require.True(t, bytes.Equal(src, p1.Data))
	firstPtr := unsafe.SliceData(p1.Data)
	p1.Release()
	p1.Release()

	msg2 := &storagev1.ReadResponse{}
	require.NoError(t, codec.Unmarshal(raw, msg2))
	p2 := TakeReadPayload(msg2)
	secondPtr := unsafe.SliceData(p2.Data)

	require.Equal(t, firstPtr, secondPtr, "释放后的 4 MiB 池化缓冲应被确定性复用")
	p2.Release()
}

func TestProtoBinaryCodec_GivenUnclaimedPooledRead_WhenGCCollected_DrainsLeaseWithoutUAF(t *testing.T) {

	codec := newProtoBinaryCodec()
	src := bytes.Repeat([]byte{0x5A, 0xA5, 0x3C, 0xC3}, MaxReadPayloadSize/4)
	pooledRaw, err := proto.Marshal(&storagev1.ReadResponse{Data: src[:SmallReadPayloadCutoff+1024]})
	require.NoError(t, err)

	holdUnclaimedSlice := func() []byte {
		unclaimed := &storagev1.ReadResponse{}
		require.NoError(t, codec.Unmarshal(pooledRaw, unclaimed))
		require.Equal(t, 1, leasedReadPayloadCount())
		return unclaimed.GetData()
	}

	retained := holdUnclaimedSlice()
	expectedRetained := append([]byte(nil), retained...)
	awaitLeasedPayloadDrain(t)

	require.Equal(t, expectedRetained, retained, "未接管的切片在 msg 回收后不得被池重用覆盖")
}

func TestProtoBinaryCodec_GivenSmallReadPayloads_Bypasses4MiBLeasePool(t *testing.T) {
	src := bytes.Repeat([]byte{0x5A, 0xA5, 0x3C, 0xC3}, MaxReadPayloadSize/4)
	assertSmallPayloadBypassesPool(t, src, 0)
	assertSmallPayloadBypassesPool(t, src, 4<<10)
	assertSmallPayloadBypassesPool(t, src, 128<<10)
	assertSmallPayloadBypassesPool(t, src, SmallReadPayloadCutoff)
}

func TestProtoBinaryCodec_GivenUnknownFieldInReadResponse_FallsBackToStandardUnmarshal(t *testing.T) {

	codec := newProtoBinaryCodec()
	var rawWithUnknown []byte
	rawWithUnknown = protowire.AppendTag(rawWithUnknown, 1, protowire.BytesType)
	rawWithUnknown = protowire.AppendBytes(rawWithUnknown, []byte("hello-bowl"))
	rawWithUnknown = protowire.AppendTag(rawWithUnknown, 99, protowire.VarintType)
	rawWithUnknown = protowire.AppendVarint(rawWithUnknown, 42)

	msgFallback := &storagev1.ReadResponse{}
	require.NoError(t, codec.Unmarshal(rawWithUnknown, msgFallback))

	require.Equal(t, []byte("hello-bowl"), msgFallback.GetData())
}

func TestProtoBinaryCodec_GivenWriteRequests_MatchesStandardProtoAndZeroAllocs(t *testing.T) {
	codec := newProtoBinaryCodec()
	appender, ok := codec.(interface {
		MarshalAppend(dst []byte, msg any) ([]byte, error)
	})
	require.True(t, ok)

	payload4M := bytes.Repeat([]byte{0x19, 0x84, 0xAB, 0xCD}, MaxReadPayloadSize/4)
	smallReq := &storagev1.WriteRequest{
		Ino:    1,
		Fh:     0,
		Offset: 0,
		Data:   []byte("small-write"),
		DiskId: "11111111-1111-4111-8111-111111111111",
		Epoch:  1,
	}
	bigReq := &storagev1.WriteRequest{
		Ino:    9876543210,
		Fh:     42,
		Offset: 1099511627776,
		Data:   payload4M,
		DiskId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		Epoch:  777,
	}

	assertWriteRequestEquivalence(t, &storagev1.WriteRequest{})
	assertWriteRequestEquivalence(t, smallReq)
	assertWriteRequestEquivalence(t, bigReq)

	_, invalidErr := appender.MarshalAppend(nil, &storagev1.WriteRequest{
		Ino:    1,
		DiskId: string([]byte{0xff, 0xfe}),
		Epoch:  1,
	})
	require.Error(t, invalidErr)

	readReq := &storagev1.ReadRequest{
		Ino:    42,
		Fh:     7,
		Offset: 4096,
		Size:   128 << 10,
		DiskId: "11111111-1111-4111-8111-111111111111",
	}
	reusableBuf := make([]byte, 0, MaxReadPayloadSize+256)
	var (
		smallOutLen int
		smallErr    error
		bigOutLen   int
		bigErr      error
		readOutLen  int
		readErr     error
	)
	require.Zero(t, testing.AllocsPerRun(20, func() {
		var out []byte
		out, smallErr = appender.MarshalAppend(reusableBuf[:0], smallReq)
		smallOutLen = len(out)
	}), "容量充足时小载荷 WriteRequest MarshalAppend 必须达成 0 allocs/op")
	require.NoError(t, smallErr)
	require.Positive(t, smallOutLen)

	require.Zero(t, testing.AllocsPerRun(20, func() {
		var out []byte
		out, bigErr = appender.MarshalAppend(reusableBuf[:0], bigReq)
		bigOutLen = len(out)
	}), "容量充足时 4 MiB WriteRequest MarshalAppend 必须达成 0 allocs/op")
	require.NoError(t, bigErr)
	require.Greater(t, bigOutLen, MaxReadPayloadSize)

	require.Zero(t, testing.AllocsPerRun(20, func() {
		var out []byte
		out, readErr = appender.MarshalAppend(reusableBuf[:0], readReq)
		readOutLen = len(out)
	}), "容量充足时常规 Protobuf 请求 MarshalAppend 必须达成 0 allocs/op")
	require.NoError(t, readErr)
	require.Positive(t, readOutLen)
}
