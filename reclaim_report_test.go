package pogreb

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReclaimLatencyReport(t *testing.T) {
	if os.Getenv("POGREB_RECLAIM_REPORT") == "" {
		t.Skip("set POGREB_RECLAIM_REPORT=1 to run the large reclaim latency report")
	}

	const keys = 100000
	db, err := createTestDB(&Options{
		maxSegmentSize:             256 << 20,
		compactionMinSegmentSize:   1,
		compactionMinFragmentation: 0.01,
	})
	if err != nil {
		t.Fatal(err)
	}

	value := make([]byte, 64)
	for i := 0; i < keys; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k-%08d", i)), value); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < keys; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k-%08d", i)), append(value, 'x')); err != nil {
			t.Fatal(err)
		}
	}

	db.mu.RLock()
	seg := db.datalog.segments[0]
	beforeSize, _ := db.FileSize()
	segSize := seg.size
	db.mu.RUnlock()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var sequence uint64
	var latMu sync.Mutex
	latencies := make([]time.Duration, 0, 200000)
	workers := 1
	if raw := os.Getenv("POGREB_RECLAIM_WRITERS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			workers = parsed
		}
	}
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				seq := atomic.AddUint64(&sequence, 1)
				start := time.Now()
				if err := db.Put([]byte(fmt.Sprintf("w-%012d", seq)), value); err != nil {
					t.Error(err)
					return
				}
				latency := time.Since(start)
				latMu.Lock()
				latencies = append(latencies, latency)
				latMu.Unlock()
			}
		}()
	}

	start := time.Now()
	cr, compactErr := db.compact(seg)
	compactDuration := time.Since(start)
	close(stop)
	wg.Wait()
	if compactErr != nil {
		t.Fatal(compactErr)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}

	for i := 0; i < keys; i += 997 {
		key := []byte(fmt.Sprintf("k-%08d", i))
		got, err := db.Get(key)
		if err != nil || len(got) != len(value)+1 {
			t.Fatalf("key %s: len=%d err=%v", key, len(got), err)
		}
	}

	afterSize, _ := db.FileSize()
	t.Logf("segment_bytes=%d file_before=%d file_after=%d concurrent_writes=%d compact=%s result=%+v",
		segSize, beforeSize, afterSize, atomic.LoadUint64(&sequence), compactDuration, cr)
	t.Logf("write_latency count=%d p50=%s p95=%s p99=%s p999=%s max=%s",
		len(latencies), percentile(.5), percentile(.95), percentile(.99), percentile(.999), latencies[len(latencies)-1])
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
