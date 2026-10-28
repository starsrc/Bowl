package fusefs

import (
	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// MaxRPCChunkSize 定义客户端单次读写 RPC 的最大载荷切片大小 (4 MiB = 4,194,304 字节)。
const MaxRPCChunkSize = 4 * 1024 * 1024

// Read 实现常规文件数据读取，对应 Linux FUSE FUSE_READ 协议。
// 遵循无缓存直通设计与 ADR-0019 规范：
// 1. 映射内核 Inode 为服务端 Inode，并校验句柄所属活跃会话 session_id；
// 2. 目标读取长度以 input.Size 与传入非空 buf 长度二者较小值为准；长度为 0 时立即返回空数据；
// 3. 当 want <= MaxRPCChunkSize 时发起单次 Read RPC；
// 4. 当 want > MaxRPCChunkSize 时按 4 MiB 顺序循环分片读取；
// 5. 遇到 EOF 或短读立即停止分片循环并返回已读数据；中途分片报错但已有数据读取成功时按短读返回 fuse.OK，首片失败返回对应 errno。
func (fs *RawFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return nil, status
	}

	sid, status := fs.ResolveHandle(input.Fh)
	if status != fuse.OK {
		return nil, status
	}

	want := int(input.Size)
	if len(buf) > 0 && len(buf) < want {
		want = len(buf)
	}
	if want == 0 {
		return fuse.ReadResultData(nil), fuse.OK
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	if want <= MaxRPCChunkSize {
		resp, err := fs.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:       ino,
			Fh:        input.Fh,
			Offset:    input.Offset,
			Size:      uint32(want),
			SessionId: &sid,
		}))
		if err != nil {
			return nil, fs.ToStatus(err, cancel)
		}

		data := resp.Msg.GetData()
		if len(buf) >= len(data) {
			copy(buf, data)
			return fuse.ReadResultData(buf[:len(data)]), fuse.OK
		}
		return fuse.ReadResultData(data), fuse.OK
	}

	var out []byte
	if len(buf) >= want {
		out = buf[:0]
	} else {
		out = make([]byte, 0, want)
	}

	var totalRead int
	for totalRead < want {
		chunkReqSize := want - totalRead
		if chunkReqSize > MaxRPCChunkSize {
			chunkReqSize = MaxRPCChunkSize
		}
		curOffset := input.Offset + uint64(totalRead)

		resp, err := fs.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:       ino,
			Fh:        input.Fh,
			Offset:    curOffset,
			Size:      uint32(chunkReqSize),
			SessionId: &sid,
		}))
		if err != nil {
			if totalRead > 0 {
				return fuse.ReadResultData(out[:totalRead]), fuse.OK
			}
			return nil, fs.ToStatus(err, cancel)
		}

		chunkData := resp.Msg.GetData()
		if len(chunkData) == 0 {
			break
		}
		out = append(out, chunkData...)
		totalRead += len(chunkData)

		if len(chunkData) < chunkReqSize {
			break
		}
	}

	return fuse.ReadResultData(out[:totalRead]), fuse.OK
}

// Write 实现常规文件数据写入，对应 Linux FUSE FUSE_WRITE 协议。
// 遵循无缓存直通设计与 ADR-0019 规范：
// 1. --ro 只读挂载下直接拦截返回 EROFS；
// 2. 映射 Inode 并校验句柄所属会话 session_id；
// 3. 当 len(data) <= MaxRPCChunkSize（包含 0 字节写入探针）时发起单次 Write RPC 直通落盘并校验模式；
// 4. 当 len(data) > MaxRPCChunkSize 时按 4 MiB 顺序分片调用，并累加实际写入字节数；
// 5. 遵循 POSIX 短写语义：若中途分片报错（如 ENOSPC）且已有数据写入成功，返回已写入字节数与 fuse.OK；首片即失败返回对应 errno。
func (fs *RawFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	if status := fs.CheckWritable(); status != fuse.OK {
		return 0, status
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return 0, status
	}

	sid, status := fs.ResolveHandle(input.Fh)
	if status != fuse.OK {
		return 0, status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	dataLen := len(data)
	if dataLen <= MaxRPCChunkSize {
		resp, err := fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:       ino,
			Fh:        input.Fh,
			Offset:    input.Offset,
			Data:      data,
			SessionId: &sid,
		}))
		if err != nil {
			return 0, fs.ToStatus(err, cancel)
		}
		return resp.Msg.GetSize(), fuse.OK
	}

	var totalWritten uint32
	for totalWritten < uint32(dataLen) {
		chunkSize := uint32(dataLen) - totalWritten
		if chunkSize > MaxRPCChunkSize {
			chunkSize = MaxRPCChunkSize
		}
		chunk := data[totalWritten : totalWritten+chunkSize]
		curOffset := input.Offset + uint64(totalWritten)

		resp, err := fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:       ino,
			Fh:        input.Fh,
			Offset:    curOffset,
			Data:      chunk,
			SessionId: &sid,
		}))
		if err != nil {
			if totalWritten > 0 {
				return totalWritten, fuse.OK
			}
			return 0, fs.ToStatus(err, cancel)
		}

		n := resp.Msg.GetSize()
		totalWritten += n
		if n < chunkSize {
			break
		}
	}

	return totalWritten, fuse.OK
}
