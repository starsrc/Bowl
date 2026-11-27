package rpc

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	benchPayloadSize         = MaxReadPayloadSize
	benchPayloadFirstByte    = byte(0x5A)
	benchPayloadLastByte     = byte(0xC3)
	benchBulkLoadConcurrency = 4
)

type benchStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
	readPayload []byte
}

func newBenchPayload() []byte {
	payload := bytes.Repeat([]byte{benchPayloadFirstByte, 0xA5, 0x3C, benchPayloadLastByte}, benchPayloadSize/4)
	return payload
}

func (h *benchStorageHandler) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	return connect.NewResponse(&storagev1.WriteResponse{
		Size: uint32(len(req.Msg.GetData())),
	}), nil
}

func (h *benchStorageHandler) Read(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	n := min(int(req.Msg.GetSize()), len(h.readPayload))
	return connect.NewResponse(&storagev1.ReadResponse{
		Data: h.readPayload[:n],
	}), nil
}

func (h *benchStorageHandler) RenewMount(_ context.Context, _ *connect.Request[storagev1.RenewMountRequest]) (*connect.Response[storagev1.RenewMountResponse], error) {
	return connect.NewResponse(&storagev1.RenewMountResponse{
		LeaseTtlSeconds: 30,
	}), nil
}

func startBenchH2CServer(b *testing.B, payload []byte, handlerOpts ...connect.HandlerOption) (endpoint string, shutdown func()) {
	b.Helper()

	path, connectHandler := storagev1connect.NewStorageServiceHandler(
		&benchStorageHandler{readPayload: payload},
		handlerOpts...,
	)
	mux := http.NewServeMux()
	mux.Handle(path, connectHandler)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen h2c benchmark server: %v", err)
	}

	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:           mux,
		Protocols:         &protocols,
		ReadHeaderTimeout: 5 * time.Second,
		HTTP2: &http.HTTP2Config{
			MaxReadFrameSize:              HTTP2MaxReadFrameSize,
			MaxReceiveBufferPerStream:     HTTP2MaxReceiveBufferPerStream,
			MaxReceiveBufferPerConnection: HTTP2DataMaxReceiveBufferPerConnection,
		},
	}

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- srv.Serve(ln)
	}()

	return "http://" + ln.Addr().String(), func() {
		_ = srv.Close()
		serveErr := <-serveErrCh
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			b.Errorf("unexpected h2c benchmark server exit: %v", serveErr)
		}
	}
}

func newBenchCodecClient(b *testing.B, endpoint string, useRPCCodec bool) (client storagev1connect.StorageServiceClient, closeIdle func()) {
	b.Helper()

	tr, err := newHTTPTransport(endpoint, nil, HTTP2DataMaxReceiveBufferPerConnection)
	if err != nil {
		b.Fatalf("create benchmark h2c transport: %v", err)
	}

	opts := []connect.ClientOption{
		connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				req.Header().Set("Accept-Encoding", "identity")
				return next(ctx, req)
			}
		})),
	}
	if useRPCCodec {
		opts = append(opts, connect.WithCodec(newProtoBinaryCodec()))
	}

	client = storagev1connect.NewStorageServiceClient(
		&http.Client{Transport: tr},
		endpoint,
		opts...,
	)
	return client, tr.CloseIdleConnections
}

func reportBenchGC(b *testing.B, before, after *runtime.MemStats, ops int) {
	b.Helper()
	if ops <= 0 {
		return
	}
	b.ReportMetric(float64(after.NumGC-before.NumGC)/float64(ops), "gc/op")
}

func BenchmarkCodec4MiBWrite(b *testing.B) {
	payload := newBenchPayload()
	cases := []struct {
		name        string
		useRPCCodec bool
	}{
		{name: "rpccodec", useRPCCodec: true},
		{name: "std_proto", useRPCCodec: false},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var handlerOpts []connect.HandlerOption
			if tc.useRPCCodec {
				handlerOpts = append(handlerOpts, connect.WithCodec(newProtoBinaryCodec()))
			}
			endpoint, shutdown := startBenchH2CServer(b, payload, handlerOpts...)
			defer shutdown()

			client, closeClient := newBenchCodecClient(b, endpoint, tc.useRPCCodec)
			defer closeClient()

			ctx := context.Background()
			reqMsg := &storagev1.WriteRequest{
				Ino:    42,
				Fh:     7,
				Offset: 0,
				Data:   payload,
				DiskId: "11111111-1111-4111-8111-111111111111",
				Epoch:  1,
				Branch: "main",
			}

			warmResp, warmErr := client.Write(ctx, connect.NewRequest(reqMsg))
			if warmErr != nil {
				b.Fatalf("warmup Write failed: %v", warmErr)
			}
			if warmResp.Msg.GetSize() != uint32(benchPayloadSize) {
				b.Fatalf("warmup Write size mismatch: got %d want %d", warmResp.Msg.GetSize(), benchPayloadSize)
			}

			runtime.GC()
			var beforeGC, afterGC runtime.MemStats
			runtime.ReadMemStats(&beforeGC)

			b.SetBytes(int64(benchPayloadSize))
			b.ReportAllocs()

			ops := 0
			for b.Loop() {
				resp, err := client.Write(ctx, connect.NewRequest(reqMsg))
				if err != nil {
					b.Fatalf("Write RPC failed: %v", err)
				}
				if resp.Msg.GetSize() != uint32(benchPayloadSize) {
					b.Fatalf("Write size mismatch: got %d want %d", resp.Msg.GetSize(), benchPayloadSize)
				}
				ops++
			}

			runtime.ReadMemStats(&afterGC)
			reportBenchGC(b, &beforeGC, &afterGC, ops)
		})
	}
}

func BenchmarkCodec4MiBRead(b *testing.B) {
	payload := newBenchPayload()
	cases := []struct {
		name        string
		useRPCCodec bool
	}{
		{name: "rpccodec", useRPCCodec: true},
		{name: "std_proto", useRPCCodec: false},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var handlerOpts []connect.HandlerOption
			if tc.useRPCCodec {
				handlerOpts = append(handlerOpts, connect.WithCodec(newProtoBinaryCodec()))
			}
			endpoint, shutdown := startBenchH2CServer(b, payload, handlerOpts...)
			defer shutdown()

			client, closeClient := newBenchCodecClient(b, endpoint, tc.useRPCCodec)
			defer closeClient()

			ctx := context.Background()
			reqMsg := &storagev1.ReadRequest{
				Ino:    42,
				Fh:     7,
				Offset: 0,
				Size:   benchPayloadSize,
				DiskId: "11111111-1111-4111-8111-111111111111",
				Branch: "main",
			}

			warmResp, warmErr := client.Read(ctx, connect.NewRequest(reqMsg))
			if warmErr != nil {
				b.Fatalf("warmup Read failed: %v", warmErr)
			}
			warmPayload := TakeReadPayload(warmResp.Msg)
			if len(warmPayload.Data) != benchPayloadSize {
				warmPayload.Release()
				b.Fatalf("warmup Read size mismatch: got %d want %d", len(warmPayload.Data), benchPayloadSize)
			}
			warmPayload.Release()

			runtime.GC()
			var beforeGC, afterGC runtime.MemStats
			runtime.ReadMemStats(&beforeGC)

			b.SetBytes(int64(benchPayloadSize))
			b.ReportAllocs()

			ops := 0
			for b.Loop() {
				resp, err := client.Read(ctx, connect.NewRequest(reqMsg))
				if err != nil {
					b.Fatalf("Read RPC failed: %v", err)
				}
				if tc.useRPCCodec {
					leased := TakeReadPayload(resp.Msg)
					if len(leased.Data) != benchPayloadSize || leased.Data[0] != benchPayloadFirstByte || leased.Data[benchPayloadSize-1] != benchPayloadLastByte {
						leased.Release()
						b.Fatalf("Read payload mismatch: len=%d", len(leased.Data))
					}
					leased.Release()
				} else {
					data := resp.Msg.GetData()
					if len(data) != benchPayloadSize || data[0] != benchPayloadFirstByte || data[benchPayloadSize-1] != benchPayloadLastByte {
						b.Fatalf("Read payload mismatch: len=%d", len(data))
					}
				}
				ops++
			}

			runtime.ReadMemStats(&afterGC)
			reportBenchGC(b, &beforeGC, &afterGC, ops)
		})
	}
}

func BenchmarkMountTransport_4MiBWrite(b *testing.B) {
	payload := newBenchPayload()
	cases := []struct {
		name           string
		isolateControl bool
	}{
		{name: "control_isolated", isolateControl: true},
		{name: "single_connection", isolateControl: false},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			endpoint, shutdown := startBenchH2CServer(b, payload, connect.WithCodec(newProtoBinaryCodec()))
			defer shutdown()

			client, err := NewClientFromConfig(ClientConfig{
				Endpoint:       endpoint,
				RetryConfig:    &RetryConfig{Disable: true},
				IsolateControl: tc.isolateControl,
			})
			if err != nil {
				b.Fatalf("create transport benchmark client: %v", err)
			}
			defer CloseIdleConnections(client)

			ctx := context.Background()
			reqMsg := &storagev1.WriteRequest{
				Ino:    42,
				Fh:     7,
				Offset: 0,
				Data:   payload,
				DiskId: "11111111-1111-4111-8111-111111111111",
				Epoch:  1,
				Branch: "main",
			}

			if _, warmErr := client.Write(ctx, connect.NewRequest(reqMsg)); warmErr != nil {
				b.Fatalf("warmup Write failed: %v", warmErr)
			}

			runtime.GC()
			var beforeGC, afterGC runtime.MemStats
			runtime.ReadMemStats(&beforeGC)

			b.SetBytes(int64(benchPayloadSize))
			b.ReportAllocs()

			ops := 0
			for b.Loop() {
				resp, writeErr := client.Write(ctx, connect.NewRequest(reqMsg))
				if writeErr != nil {
					b.Fatalf("Write RPC failed: %v", writeErr)
				}
				if resp.Msg.GetSize() != uint32(benchPayloadSize) {
					b.Fatalf("Write size mismatch: got %d want %d", resp.Msg.GetSize(), benchPayloadSize)
				}
				ops++
			}

			runtime.ReadMemStats(&afterGC)
			reportBenchGC(b, &beforeGC, &afterGC, ops)
		})
	}
}

func BenchmarkMountTransport_ControlUnder4MiBLoad(b *testing.B) {
	payload := newBenchPayload()
	cases := []struct {
		name           string
		isolateControl bool
	}{
		{name: "control_isolated", isolateControl: true},
		{name: "single_connection", isolateControl: false},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			endpoint, shutdown := startBenchH2CServer(b, payload, connect.WithCodec(newProtoBinaryCodec()))
			defer shutdown()

			client, err := NewClientFromConfig(ClientConfig{
				Endpoint:       endpoint,
				RetryConfig:    &RetryConfig{Disable: true},
				IsolateControl: tc.isolateControl,
			})
			if err != nil {
				b.Fatalf("create transport benchmark client: %v", err)
			}
			defer CloseIdleConnections(client)

			ctx := context.Background()
			renewReq := &storagev1.RenewMountRequest{
				DiskId:  "11111111-1111-4111-8111-111111111111",
				Branch:  "main",
				MountId: "bench-mount",
				Epoch:   1,
			}
			writeReq := &storagev1.WriteRequest{
				Ino:    42,
				Fh:     7,
				Offset: 0,
				Data:   payload,
				DiskId: "11111111-1111-4111-8111-111111111111",
				Epoch:  1,
				Branch: "main",
			}

			if _, warmErr := client.RenewMount(ctx, connect.NewRequest(renewReq)); warmErr != nil {
				b.Fatalf("warmup RenewMount failed: %v", warmErr)
			}
			if _, warmErr := client.Write(ctx, connect.NewRequest(writeReq)); warmErr != nil {
				b.Fatalf("warmup Write failed: %v", warmErr)
			}

			loadCtx, stopLoad := context.WithCancel(ctx)
			var (
				loadWG       sync.WaitGroup
				firstWriteCh = make(chan struct{})
				firstOnce    sync.Once
				bgErrCount   atomic.Int64
			)
			for range benchBulkLoadConcurrency {
				loadWG.Go(func() {
					for loadCtx.Err() == nil {
						_, writeErr := client.Write(loadCtx, connect.NewRequest(writeReq))
						if writeErr != nil {
							if loadCtx.Err() != nil {
								return
							}
							bgErrCount.Add(1)
							return
						}
						firstOnce.Do(func() {
							close(firstWriteCh)
						})
					}
				})
			}

			<-firstWriteCh

			runtime.GC()
			var beforeGC, afterGC runtime.MemStats
			runtime.ReadMemStats(&beforeGC)

			b.ReportAllocs()

			ops := 0
			for b.Loop() {
				resp, renewErr := client.RenewMount(ctx, connect.NewRequest(renewReq))
				if renewErr != nil {
					stopLoad()
					loadWG.Wait()
					b.Fatalf("RenewMount RPC failed: %v", renewErr)
				}
				if resp.Msg.GetLeaseTtlSeconds() == 0 {
					stopLoad()
					loadWG.Wait()
					b.Fatalf("RenewMount returned zero lease TTL")
				}
				ops++
			}

			runtime.ReadMemStats(&afterGC)

			stopLoad()
			loadWG.Wait()
			if bgErrCount.Load() > 0 {
				b.Fatalf("background 4 MiB Write load recorded %d unexpected errors", bgErrCount.Load())
			}

			reportBenchGC(b, &beforeGC, &afterGC, ops)
		})
	}
}
