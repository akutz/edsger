// GOMAXPROCS=4 go test

package edsger

// this test file was taken from https://golang.org/src/sync/sutex_test.go
// and modified to test the Semaphore as it follows the same semantics of the
// Go Stdlib's sync.sutex.

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("EDSGER_LOG_LEVEL", "info")
	os.Exit(m.Run())
}

func parallelReader(sema *Semaphore, clocked, cunlock, cdone chan bool) {
	sema.RLock()
	clocked <- true
	<-cunlock
	sema.RUnlock()
	cdone <- true
}

func doTestParallelReaders(numReaders, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	var sema = Semaphore{
		Name: fmt.Sprintf(
			"TestParallelReaders-%2d%2d",
			numReaders, gomaxprocs,
		),
	}
	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)
	for i := 0; i < numReaders; i++ {
		go parallelReader(&sema, clocked, cunlock, cdone)
	}
	// Wait for all parallel RLock()s to succeed.
	for i := 0; i < numReaders; i++ {
		<-clocked
	}
	for i := 0; i < numReaders; i++ {
		cunlock <- true
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numReaders; i++ {
		<-cdone
	}
}

func TestParallelReaders(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	doTestParallelReaders(1, 4)
	doTestParallelReaders(3, 4)
	doTestParallelReaders(4, 2)
}

func reader(
	sema *Semaphore, numIterations int, activity *int32, cdone chan bool) {

	for i := 0; i < numIterations; i++ {
		sema.RLock()
		n := atomic.AddInt32(activity, 1)
		if n < 1 || n >= 10000 {
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -1)
		sema.RUnlock()
	}
	cdone <- true
}

func writer(
	sema *Semaphore, numIterations int, activity *int32, cdone chan bool) {

	for i := 0; i < numIterations; i++ {
		sema.Lock()
		n := atomic.AddInt32(activity, 10000)
		if n != 10000 {
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -10000)
		sema.Unlock()
	}
	cdone <- true
}

func HammerSemaphore(named bool, gomaxprocs, numReaders, numIterations int) {

	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.

	var (
		activity int32
		cdone    = make(chan bool)
		sema     Semaphore
	)

	if named {
		sema.Name = fmt.Sprintf(
			"TestSemaphore-%d%d%d",
			gomaxprocs, numReaders, numIterations)
	}

	go writer(&sema, numIterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(&sema, numIterations, &activity, cdone)
	}
	go writer(&sema, numIterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go reader(&sema, numIterations, &activity, cdone)
	}
	// Wait for the 2 writers and all readers to finish.
	for i := 0; i < 2+numReaders; i++ {
		<-cdone
	}
}

func TestUnnamedSemaphore(t *testing.T) {
	testSemaphore(t, false)
}

func TestNamedSemaphore(t *testing.T) {
	testSemaphore(t, true)
}

func testSemaphore(t *testing.T, named bool) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 1000
	if testing.Short() {
		n = 5
	}
	HammerSemaphore(named, 1, 1, n)
	HammerSemaphore(named, 1, 3, n)
	HammerSemaphore(named, 1, 10, n)
	HammerSemaphore(named, 4, 1, n)
	HammerSemaphore(named, 4, 3, n)
	HammerSemaphore(named, 4, 10, n)
	HammerSemaphore(named, 10, 1, n)
	HammerSemaphore(named, 10, 3, n)
	HammerSemaphore(named, 10, 10, n)
	HammerSemaphore(named, 10, 5, n)
}

func TestRLocker(t *testing.T) {
	var wl = Semaphore{Name: "TestRLocker"}
	var rl sync.Locker
	wlocked := make(chan bool, 1)
	rlocked := make(chan bool, 1)
	rl = wl.RLocker()
	n := 10
	go func() {
		for i := 0; i < n; i++ {
			rl.Lock()
			rl.Lock()
			rlocked <- true
			wl.Lock()
			wlocked <- true
		}
	}()
	for i := 0; i < n; i++ {
		<-rlocked
		rl.Unlock()
		select {
		case <-wlocked:
			t.Fatal("RLocker() didn't read-lock it")
		default:
		}
		rl.Unlock()
		<-wlocked
		select {
		case <-rlocked:
			t.Fatal("RLocker() didn't respect the write lock")
		default:
		}
		wl.Unlock()
	}
}

func TestUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked Semaphore did not panic")
		}
	}()
	var sema = Semaphore{Name: "TestUnlockPanic"}
	sema.Unlock()
}

func TestUnlockPanic2(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked Semaphore did not panic")
		}
	}()
	var sema = Semaphore{Name: "TestUnlockPanic2"}
	sema.RLock()
	sema.Unlock()
}

func TestRUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked Semaphore did not panic")
		}
	}()
	var sema = Semaphore{Name: "TestRUnlockPanic"}
	sema.RUnlock()
}

func TestRUnlockPanic2(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked Semaphore did not panic")
		}
	}()
	var sema = Semaphore{Name: "TestRUnlockPanic2"}
	sema.Lock()
	sema.RUnlock()
}

func BenchmarkSemaphoreUncontended(b *testing.B) {
	type PaddedSemaphore struct {
		Semaphore
		pad [32]uint32
	}
	var numSema int64
	b.RunParallel(func(pb *testing.PB) {
		var sema = PaddedSemaphore{
			Semaphore: Semaphore{
				Name: fmt.Sprintf(
					"BenchmarkSemaphoreUncontended-%2d",
					atomic.AddInt64(&numSema, 1),
				),
			},
		}
		for pb.Next() {
			sema.RLock()
			sema.RLock()
			sema.RUnlock()
			sema.RUnlock()
			sema.Lock()
			sema.Unlock()
		}
	})
}

func benchmarkSemaphore(b *testing.B, name string, localWork, writeRatio int) {
	var sema = Semaphore{Name: name}
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if foo%writeRatio == 0 {
				sema.Lock()
				sema.Unlock()
			} else {
				sema.RLock()
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				sema.RUnlock()
			}
		}
		_ = foo
	})
}

func BenchmarkSemaphoreWrite100(b *testing.B) {
	benchmarkSemaphore(b, "BenchmarkSemaphoreWrite100", 0, 100)
}

func BenchmarkSemaphoreWrite10(b *testing.B) {
	benchmarkSemaphore(b, "BenchmarkSemaphoreWrite10", 0, 10)
}

func BenchmarkSemaphoreWorkWrite100(b *testing.B) {
	benchmarkSemaphore(b, "BenchmarkSemaphoreWorkWrite100", 100, 100)
}

func BenchmarkSemaphoreWorkWrite10(b *testing.B) {
	benchmarkSemaphore(b, "BenchmarkSemaphoreWorkWrite10", 100, 10)
}
