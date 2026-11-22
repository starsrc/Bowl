package fusefs

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func runHandleOrphanRaceRound(t *testing.T, round int) {
	t.Helper()
	var reclaimCalls atomic.Int32
	table := NewHandleTable(func(uint64) {
		reclaimCalls.Add(1)
	})
	fh, err := table.Open(uint64(1000+round), uint32(syscall.O_RDWR), false)
	require.NoError(t, err)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		table.Release(fh)
	}()
	go func() {
		defer wg.Done()
		<-start
		table.MarkOrphan(uint64(1000 + round))
	}()
	close(start)
	wg.Wait()
	require.Equal(t, int32(1), reclaimCalls.Load(), "并发 Release 与 MarkOrphan 每轮必须恰好回收 1 次 (round=%d)", round)
}

func runSequentialStreamsForBudget(t *testing.T, coord *ReadAheadCoordinator, streams int, fetch ChunkFetcher) {
	t.Helper()
	const fileSize = 64 << 20
	const step = 1 << 20
	var wg sync.WaitGroup
	errs := make([]error, streams)
	for s := range streams {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			fh := uint64(idx + 1)
			ino := uint64(idx + 100)
			buf := make([]byte, step)
			for off := uint64(0); off < fileSize; off += step {
				n, err := coord.Read(context.Background(), fh, ino, off, buf, fetch)
				if err != nil {
					errs[idx] = err
					return
				}
				if n != step {
					errs[idx] = fmt.Errorf("stream %d short read off=%d got=%d", idx, off, n)
					return
				}
			}
		}(s)
	}
	wg.Wait()
	for s := range streams {
		require.NoError(t, errs[s])
	}
}

func runRandomStreamsZeroAmplification(t *testing.T, coord *ReadAheadCoordinator, streams int, fetch ChunkFetcher) int {
	t.Helper()
	offsets := []uint64{12 << 20, 4 << 20, 20 << 20, 8 << 20, 28 << 20, 0}
	var wg sync.WaitGroup
	errs := make([]error, streams)
	for s := range streams {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			fh := uint64(idx + 1000)
			ino := uint64(idx + 2000)
			buf := make([]byte, 512<<10)
			for _, off := range offsets {
				n, err := coord.Read(context.Background(), fh, ino, off, buf, fetch)
				if err != nil || n != len(buf) {
					errs[idx] = fmt.Errorf("stream %d off=%d n=%d: %w", idx, off, n, err)
					return
				}
			}
		}(s)
	}
	wg.Wait()
	for s := range streams {
		require.NoError(t, errs[s])
	}
	return streams * len(offsets)
}

func waitUntilInodeLockRefsAtLeast(t *testing.T, l *inodeLocks, ino uint64, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		l.mu.Lock()
		entry := l.entries[ino]
		refs := 0
		if entry != nil {
			refs = entry.refs
		}
		l.mu.Unlock()
		if refs >= want {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("等待 inodeLocks 引用计数超时: want>=%d got=%d", want, refs)
		}
		runtime.Gosched()
	}
}

func runMonotonicSizeRace(t *testing.T) {
	t.Helper()
	cache := NewNamespaceCache(64)
	cache.configure(1, 1.0, 1.0)
	const ino uint64 = 500
	epoch := cache.version()

	makeInode := func(sz uint64) *storagev1.Inode {
		now := timestamppb.Now()
		return &storagev1.Inode{
			Ino:        ino,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:   uint32(syscall.S_IFREG | 0o644),
				Size:   sz,
				Nlink:  1,
				Blocks: (sz + 511) / 512,
				Atime:  now,
				Mtime:  now,
				Ctime:  now,
			},
		}
	}

	cache.putEntryAt(epoch, 1, "race.bin", ino)
	cache.putInodeAt(epoch, ino, makeInode(0), 1.0, 1.0)
	snapEpoch, snapGen := cache.snapshot()
	cache.stageWriteInodeAt(epoch, ino, 8192, timestamppb.Now())
	cache.refreshInodeFromWriteAt(epoch, ino, makeInode(4096), 8192, 1.0)
	cache.putInodeAtIf(snapGen, snapEpoch, ino, makeInode(0), 1.0, 1.0)

	got, ok := cache.inodeReadOnly(ino)
	require.True(t, ok)
	require.Equal(t, uint64(8192), got.inode.GetAttr().GetSize(), "过期快照回填不得使缓存尺寸回退")

	var stop atomic.Bool
	var wg sync.WaitGroup
	var errMsg atomic.Value

	wg.Go(func() {
		for sz := uint64(8192); sz <= 8192+512*1024; sz += 1024 {
			cache.stageWriteInodeAt(epoch, ino, sz, timestamppb.Now())
			cache.refreshInodeFromWriteAt(epoch, ino, makeInode(sz), sz, 1.0)
		}
		stop.Store(true)
	})

	for range 4 {
		wg.Go(func() {
			var last uint64
			for !stop.Load() {
				cached, _, negative, hit := cache.lookupReadOnly(1, "race.bin")
				if hit && !negative && cached.inode != nil && cached.inode.GetAttr() != nil {
					cur := cached.inode.GetAttr().GetSize()
					if cur < last {
						errMsg.Store(fmt.Sprintf("观测到尺寸非单调回退: prev=%d cur=%d", last, cur))
						return
					}
					last = cur
				}
				runtime.Gosched()
			}
		})
	}
	wg.Wait()
	require.Nil(t, errMsg.Load())
}

func runHandleOrphanRaceSuite(t *testing.T, rounds int) {
	t.Helper()
	for round := range rounds {
		runHandleOrphanRaceRound(t, round)
	}
}

func makeFilledChunk(size int, fill byte) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = fill
	}
	return buf
}

func waitUntilReadAheadSlotWaitersAtLeast(t *testing.T, coord *ReadAheadCoordinator, fh, alignedOffset uint64, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		coord.mu.Lock()
		hra := coord.handles[fh]
		coord.mu.Unlock()

		waiters := 0
		if hra != nil {
			hra.mu.Lock()
			for _, s := range hra.slots {
				if s.offset == alignedOffset {
					waiters = s.waiters
					break
				}
			}
			hra.mu.Unlock()
		}
		if waiters >= want {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("等待单飞槽位等待者数超时: offset=%d want>=%d got=%d", alignedOffset, want, waiters)
		}
		runtime.Gosched()
	}
}

func runConcurrentSingleflightContended(t *testing.T, coord *ReadAheadCoordinator, fh, ino uint64, count int) int32 {
	t.Helper()

	primeChunk := makeFilledChunk(4096, 0x11)
	primeBuf := make([]byte, 4096)
	primeN, primeErr := coord.Read(context.Background(), fh, ino, 0, primeBuf, func(context.Context, uint64, uint32) (rpc.ReadPayload, error) {
		return rpc.ReadPayload{Data: primeChunk}, nil
	})
	require.NoError(t, primeErr)
	require.Equal(t, 4096, primeN)

	var fetchCalls atomic.Int32
	fetchEntered := make(chan struct{})
	fetchGate := make(chan struct{})
	var once sync.Once

	chunk := makeFilledChunk(64*1024, 0x55)
	fetch := func(context.Context, uint64, uint32) (rpc.ReadPayload, error) {
		fetchCalls.Add(1)
		once.Do(func() { close(fetchEntered) })
		<-fetchGate
		return rpc.ReadPayload{Data: chunk}, nil
	}

	var wg sync.WaitGroup
	errs := make([]error, count)
	reads := make([][]byte, count)

	wg.Go(func() {
		buf := make([]byte, 4096)
		n, err := coord.Read(context.Background(), fh, ino, 4096, buf, fetch)
		errs[0] = err
		reads[0] = buf[:n]
	})
	<-fetchEntered

	for i := 1; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			buf := make([]byte, 4096)
			n, err := coord.Read(context.Background(), fh, ino, 4096, buf, fetch)
			errs[idx] = err
			reads[idx] = buf[:n]
		}(i)
	}
	waitUntilReadAheadSlotWaitersAtLeast(t, coord, fh, 0, count)
	close(fetchGate)
	wg.Wait()

	for i := range count {
		require.NoError(t, errs[i], "并发单飞读取第 %d 个协程失败", i)
		require.Len(t, reads[i], 4096)
		require.Equal(t, byte(0x55), reads[i][0])
	}
	return fetchCalls.Load()
}

func makeBudgetProbeFetcher(activeInflight, maxObservedInflight *atomic.Int32, chunk []byte) ChunkFetcher {
	return func(_ context.Context, _ uint64, _ uint32) (rpc.ReadPayload, error) {
		cur := activeInflight.Add(1)
		for {
			prev := maxObservedInflight.Load()
			if cur <= prev || maxObservedInflight.CompareAndSwap(prev, cur) {
				break
			}
		}
		runtime.Gosched()
		activeInflight.Add(-1)
		return rpc.ReadPayload{Data: chunk}, nil
	}
}

func makeInvalidationRetryFetcher(firstEntered chan struct{}, firstGate <-chan struct{}, staleData, freshData []byte) ChunkFetcher {
	var prefetchAttempt atomic.Int32
	var once sync.Once
	return func(ctx context.Context, offset uint64, size uint32) (rpc.ReadPayload, error) {
		if offset < uint64(MaxRPCChunkSize) {
			return rpc.ReadPayload{Data: staleData[:size]}, nil
		}
		if prefetchAttempt.Add(1) == 1 {
			once.Do(func() { close(firstEntered) })
			<-firstGate
			if ctx.Err() != nil {
				return rpc.ReadPayload{}, ctx.Err()
			}
			return rpc.ReadPayload{Data: staleData[:size]}, nil
		}
		return rpc.ReadPayload{Data: freshData[:size]}, nil
	}
}

func TestHandleTable_并发Release与MarkOrphan竞态恰好触发单次回收(t *testing.T) {

	runHandleOrphanRaceSuite(t, 128)
}

func TestReadAhead_全局并发预算与单飞合并及随机零放大与失效重试无泄漏(t *testing.T) {
	t.Run("17个并发读者命中同一窗口单飞合并为单次拉取", func(t *testing.T) {

		coord := NewReadAheadCoordinator()

		calls := runConcurrentSingleflightContended(t, coord, 1, 10, 17)

		require.Equal(t, int32(1), calls, "17 个同窗并发读者必须通过单飞收敛为 1 次 RPC")
	})

	t.Run("16流并发顺序预读严守全局32配额且16流随机读零放大", func(t *testing.T) {

		coordSeq := NewReadAheadCoordinator()
		var activeInflight, maxObservedInflight atomic.Int32
		seqFetch := makeBudgetProbeFetcher(&activeInflight, &maxObservedInflight, make([]byte, MaxRPCChunkSize))

		runSequentialStreamsForBudget(t, coordSeq, 16, seqFetch)

		require.LessOrEqual(t, maxObservedInflight.Load(), int32(ReadAheadMaxGlobalInflight+16), "全局异步预取并发配额不得突破上限")

		coordRand := NewReadAheadCoordinator()
		var randFetches atomic.Int32
		randChunk := make([]byte, MaxRPCChunkSize)
		randFetch := func(context.Context, uint64, uint32) (rpc.ReadPayload, error) {
			randFetches.Add(1)
			return rpc.ReadPayload{Data: randChunk}, nil
		}
		expectedReads := runRandomStreamsZeroAmplification(t, coordRand, 16, randFetch)
		require.Equal(t, int32(expectedReads), randFetches.Load(), "16 流离散随机跳跃读取不得产生额外预读 RPC")
	})

	t.Run("在途拉取遭遇InvalidateInode后等待者重拉成功且不泄漏ContextCanceled", func(t *testing.T) {

		coord := NewReadAheadCoordinator()
		firstEntered := make(chan struct{})
		firstGate := make(chan struct{})
		staleData := make([]byte, MaxRPCChunkSize)
		freshData := makeFilledChunk(MaxRPCChunkSize, 0x7E)
		fetch := makeInvalidationRetryFetcher(firstEntered, firstGate, staleData, freshData)

		primeBuf := make([]byte, 4096)
		_, primeErr1 := coord.Read(context.Background(), 7, 700, 0, primeBuf, fetch)
		require.NoError(t, primeErr1)
		_, primeErr2 := coord.Read(context.Background(), 7, 700, 4096, primeBuf, fetch)
		require.NoError(t, primeErr2)
		<-firstEntered

		type readOutcome struct {
			n    int
			data []byte
			err  error
		}
		done := make(chan readOutcome, 1)
		go func() {
			buf := make([]byte, 4096)
			n, err := coord.Read(context.Background(), 7, 700, uint64(MaxRPCChunkSize), buf, fetch)
			done <- readOutcome{n: n, data: buf[:n], err: err}
		}()
		waitUntilReadAheadSlotWaitersAtLeast(t, coord, 7, uint64(MaxRPCChunkSize), 1)
		coord.InvalidateInode(700)
		close(firstGate)
		res := <-done

		require.NoError(t, res.err, "在途预取遇到 InvalidateInode 不得向前台等待者泄漏 context.Canceled")
		require.Equal(t, 4096, res.n)
		require.Equal(t, byte(0x7E), res.data[0], "失效后重拉必须返回最新数据")
	})
}

func TestNamespaceCache与InodeLocks_并发写查单调尺寸不变式与互斥竞争指标(t *testing.T) {
	t.Run("并发写入推进与快照回填保持尺寸单调不倒退", func(t *testing.T) {
		runMonotonicSizeRace(t)
	})

	t.Run("inodeLocks竞争阻塞释放后准确记录ContendedAcquisitions并清理表项", func(t *testing.T) {

		var locks inodeLocks
		m := NewMetrics()
		locks.bindMetrics(m)
		unlock1 := locks.lock(42)

		done := make(chan struct{})
		go func() {
			unlock2 := locks.lock(42)
			unlock2()
			close(done)
		}()
		waitUntilInodeLockRefsAtLeast(t, &locks, 42, 2)
		unlock1()
		<-done

		snap := m.Snapshot()
		require.Equal(t, uint64(1), snap.InodeLocks.FastAcquisitions)
		require.Equal(t, uint64(1), snap.InodeLocks.ContendedAcquisitions)
		require.Zero(t, locks.activeEntries(), "所有协程解锁后 inodeLocks 活跃条目必须归零")
	})
}

func TestInodeLocks_RLockSharedConcurrencyAndExclusiveContentionMetrics(t *testing.T) {
	var locks inodeLocks
	m := NewMetrics()
	locks.bindMetrics(m)

	rUnlock1 := locks.rlock(77)
	rUnlock2 := locks.rlock(77)
	require.Equal(t, 1, locks.activeEntries())

	wAcquired := make(chan struct{})
	wRelease := make(chan struct{})
	wDone := make(chan struct{})
	go func() {
		wUnlock := locks.lock(77)
		close(wAcquired)
		<-wRelease
		wUnlock()
		close(wDone)
	}()
	waitUntilInodeLockRefsAtLeast(t, &locks, 77, 3)

	select {
	case <-wAcquired:
		t.Fatal("scene=rlock_blocks_exclusive_lock want=blocked got=acquired")
	default:
	}

	rUnlock1()
	rUnlock2()
	<-wAcquired

	rDone := make(chan struct{})
	go func() {
		rUnlock3 := locks.rlock(77)
		rUnlock3()
		close(rDone)
	}()
	waitUntilInodeLockRefsAtLeast(t, &locks, 77, 2)

	close(wRelease)
	<-wDone
	<-rDone

	snap := m.Snapshot()
	require.Equal(t, uint64(2), snap.InodeLocks.FastAcquisitions)
	require.Equal(t, uint64(2), snap.InodeLocks.ContendedAcquisitions)
	require.Zero(t, locks.activeEntries())
}
