package engine

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func runBenchWorkers(b *testing.B, workers int, fn func(workerID int, iter int64)) {
	b.Helper()
	var counter atomic.Int64
	limit := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(id int) {
			defer wg.Done()
			for {
				idx := counter.Add(1) - 1
				if idx >= limit {
					return
				}
				fn(id, idx)
			}
		}(w)
	}
	wg.Wait()
}

func BenchmarkSingleBranch_ConcurrentWrite(b *testing.B) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("W"), 4096)

	for _, workers := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			disk, err := openTestDisk(testDiskOptions{
				Dir:         b.TempDir(),
				BlockSize:   4096,
				MaxPackSize: 64 << 20,
			})
			if err != nil {
				b.Fatalf("openTestDisk: %v", err)
			}
			b.Cleanup(func() {
				if closeErr := disk.Close(); closeErr != nil {
					b.Errorf("close disk: %v", closeErr)
				}
			})

			inos := make([]uint64, workers)
			for i := range workers {
				f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("w-%d.bin", i), 0o644, 0, 0)
				if cErr != nil {
					b.Fatalf("create: %v", cErr)
				}
				inos[i] = f.Ino
			}

			b.ReportAllocs()
			b.ResetTimer()
			runBenchWorkers(b, workers, func(workerID int, iter int64) {
				off := (iter % 16) * 4096
				if _, wErr := disk.Write(ctx, DefaultBranchName, inos[workerID], off, payload); wErr != nil {
					b.Errorf("write: %v", wErr)
				}
			})
			b.StopTimer()
		})
	}
}

func BenchmarkSingleBranch_ConcurrentFsync(b *testing.B) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("F"), 4096)

	for _, workers := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			disk, err := openTestDisk(testDiskOptions{
				Dir:         b.TempDir(),
				BlockSize:   4096,
				MaxPackSize: 64 << 20,
			})
			if err != nil {
				b.Fatalf("openTestDisk: %v", err)
			}
			b.Cleanup(func() {
				if closeErr := disk.Close(); closeErr != nil {
					b.Errorf("close disk: %v", closeErr)
				}
			})

			inos := make([]uint64, workers)
			for i := range workers {
				f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("f-%d.bin", i), 0o644, 0, 0)
				if cErr != nil {
					b.Fatalf("create: %v", cErr)
				}
				inos[i] = f.Ino
			}

			b.ReportAllocs()
			b.ResetTimer()
			runBenchWorkers(b, workers, func(workerID int, _ int64) {
				if _, wErr := disk.Write(ctx, DefaultBranchName, inos[workerID], 0, payload); wErr != nil {
					b.Errorf("write: %v", wErr)
					return
				}
				if sErr := disk.Fsync(ctx, DefaultBranchName, inos[workerID]); sErr != nil {
					b.Errorf("fsync: %v", sErr)
				}
			})
			b.StopTimer()
		})
	}
}

func BenchmarkSingleBranch_MixedWriteAndFsync(b *testing.B) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("M"), 4096)

	for _, workers := range []int{8, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			disk, err := openTestDisk(testDiskOptions{
				Dir:         b.TempDir(),
				BlockSize:   4096,
				MaxPackSize: 64 << 20,
			})
			if err != nil {
				b.Fatalf("openTestDisk: %v", err)
			}
			b.Cleanup(func() {
				if closeErr := disk.Close(); closeErr != nil {
					b.Errorf("close disk: %v", closeErr)
				}
			})

			inos := make([]uint64, workers)
			for i := range workers {
				f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("m-%d.bin", i), 0o644, 0, 0)
				if cErr != nil {
					b.Fatalf("create: %v", cErr)
				}
				inos[i] = f.Ino
			}

			b.ReportAllocs()
			b.ResetTimer()
			runBenchWorkers(b, workers, func(workerID int, iter int64) {
				if iter%4 == 0 {
					if sErr := disk.Fsync(ctx, DefaultBranchName, inos[workerID]); sErr != nil {
						b.Errorf("fsync: %v", sErr)
					}
					return
				}
				off := (iter % 16) * 4096
				if _, wErr := disk.Write(ctx, DefaultBranchName, inos[workerID], off, payload); wErr != nil {
					b.Errorf("write: %v", wErr)
				}
			})
			b.StopTimer()
		})
	}
}

func BenchmarkDeepCommitChain(b *testing.B) {
	ctx := context.Background()
	baseData := bytes.Repeat([]byte("B"), 4096)
	hotData := bytes.Repeat([]byte("H"), 4096)

	for _, depth := range []int{64, 200} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			disk, err := openTestDisk(testDiskOptions{
				Dir:         b.TempDir(),
				BlockSize:   4096,
				MaxPackSize: 64 << 20,
			})
			if err != nil {
				b.Fatalf("openTestDisk: %v", err)
			}
			b.Cleanup(func() {
				if closeErr := disk.Close(); closeErr != nil {
					b.Errorf("close disk: %v", closeErr)
				}
			})

			baseFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "base.bin", 0o644, 0, 0)
			if err != nil {
				b.Fatalf("create base: %v", err)
			}
			if _, err = disk.Write(ctx, DefaultBranchName, baseFile.Ino, 0, baseData); err != nil {
				b.Fatalf("write base: %v", err)
			}
			if _, err = disk.Commit(ctx, DefaultBranchName, "base commit"); err != nil {
				b.Fatalf("commit base: %v", err)
			}

			for i := 1; i < depth; i++ {
				name := fmt.Sprintf("c-%03d.txt", i)
				f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, name, 0o644, 0, 0)
				if cErr != nil {
					b.Fatalf("create %s: %v", name, cErr)
				}
				if _, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte(name)); wErr != nil {
					b.Fatalf("write %s: %v", name, wErr)
				}
				if _, cmtErr := disk.Commit(ctx, DefaultBranchName, name); cmtErr != nil {
					b.Fatalf("commit %d: %v", i, cmtErr)
				}
			}

			hotFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "hot.bin", 0o644, 0, 0)
			if err != nil {
				b.Fatalf("create hot: %v", err)
			}
			if _, err = disk.Write(ctx, DefaultBranchName, hotFile.Ino, 0, hotData); err != nil {
				b.Fatalf("write hot: %v", err)
			}

			b.Run("HotWorkLayer_GetAttr", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, gErr := disk.GetAttr(ctx, DefaultBranchName, hotFile.Ino); gErr != nil {
						b.Fatalf("getattr hot: %v", gErr)
					}
				}
			})

			b.Run("HotWorkLayer_Lookup", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, _, lErr := disk.Lookup(ctx, DefaultBranchName, RootIno, "hot.bin"); lErr != nil {
						b.Fatalf("lookup hot: %v", lErr)
					}
				}
			})

			b.Run("HotWorkLayer_Read", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, rErr := disk.Read(ctx, DefaultBranchName, hotFile.Ino, 0, 4096); rErr != nil {
						b.Fatalf("read hot: %v", rErr)
					}
				}
			})

			b.Run("DeepBase_GetAttr", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, gErr := disk.GetAttr(ctx, DefaultBranchName, baseFile.Ino); gErr != nil {
						b.Fatalf("getattr base: %v", gErr)
					}
				}
			})

			b.Run("DeepBase_Lookup", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, _, lErr := disk.Lookup(ctx, DefaultBranchName, RootIno, "base.bin"); lErr != nil {
						b.Fatalf("lookup base: %v", lErr)
					}
				}
			})

			b.Run("DeepBase_Read", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, rErr := disk.Read(ctx, DefaultBranchName, baseFile.Ino, 0, 4096); rErr != nil {
						b.Fatalf("read base: %v", rErr)
					}
				}
			})

			b.Run("ENOENT_Lookup", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_, _, lErr := disk.Lookup(ctx, DefaultBranchName, RootIno, "does-not-exist.bin")
					if lErr == nil {
						b.Fatal("expected ENOENT")
					}
				}
			})

			b.Run("ReadDir", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					if _, dErr := disk.ReadDir(ctx, DefaultBranchName, RootIno); dErr != nil {
						b.Fatalf("readdir: %v", dErr)
					}
				}
			})
		})
	}
}

func BenchmarkMultiBranch_ConcurrentPackAppendRead(b *testing.B) {
	ctx := context.Background()
	const workers = 8
	payload := bytes.Repeat([]byte("P"), 64<<10)

	disk, err := openTestDisk(testDiskOptions{
		Dir:         b.TempDir(),
		BlockSize:   64 << 10,
		MaxPackSize: 16 << 20,
	})
	if err != nil {
		b.Fatalf("openTestDisk: %v", err)
	}
	b.Cleanup(func() {
		if closeErr := disk.Close(); closeErr != nil {
			b.Errorf("close disk: %v", closeErr)
		}
	})

	branches := make([]string, workers)
	inos := make([]uint64, workers)
	branches[0] = DefaultBranchName
	for i := 1; i < workers; i++ {
		name := fmt.Sprintf("pack-b-%d", i)
		if _, _, fErr := disk.Fork(ctx, DefaultBranchName, name, "bench fork"); fErr != nil {
			b.Fatalf("fork %s: %v", name, fErr)
		}
		branches[i] = name
	}
	for i := range workers {
		f, cErr := disk.Create(ctx, branches[i], RootIno, "pack.bin", 0o644, 0, 0)
		if cErr != nil {
			b.Fatalf("create on %s: %v", branches[i], cErr)
		}
		inos[i] = f.Ino
	}

	b.ReportAllocs()
	b.ResetTimer()
	runBenchWorkers(b, workers, func(workerID int, iter int64) {
		off := (iter % 8) * (64 << 10)
		if _, wErr := disk.Write(ctx, branches[workerID], inos[workerID], off, payload); wErr != nil {
			b.Errorf("write: %v", wErr)
			return
		}
		if _, rErr := disk.Read(ctx, branches[workerID], inos[workerID], off, len(payload)); rErr != nil {
			b.Errorf("read: %v", rErr)
		}
	})
	b.StopTimer()
}
