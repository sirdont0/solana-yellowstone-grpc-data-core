package ingest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"
)

// Opt-in local WAL experiment, not an end-to-end provider acceptance test.
// Rates are offered synthetic records/s, with 1 KiB payloads and batches of 64.
// If disk capacity is insufficient, schedule lag grows instead of dropping
// records. The finite workload drains fully and is verified after reopening.
func TestWALV2SyntheticLoad(t *testing.T) {
	if os.Getenv("CORE_WAL_LOAD") != "1" {
		t.Skip("set CORE_WAL_LOAD=1 for local 1x/3x/10x WAL experiment")
	}
	seconds := 5
	if value := os.Getenv("CORE_WAL_LOAD_SECONDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 300 {
			t.Fatal("CORE_WAL_LOAD_SECONDS must be 1..300 (offered duration per profile; draining may take longer)")
		}
		seconds = parsed
	}
	for _, rate := range []int{1000, 3000, 10000} {
		t.Run(fmt.Sprintf("offered_%d_records_per_second", rate), func(t *testing.T) {
			const batchSize = 64
			count := rate * seconds
			dir := filepath.Join(t.TempDir(), "journal.v2")
			w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			payload := bytes.Repeat([]byte{0x5a}, 1024)
			ctx, cancel := context.WithCancel(context.Background())
			readerDone := make(chan error, 1)
			go func() {
				ticker := time.NewTicker(200 * time.Millisecond)
				defer ticker.Stop()
				var cursor uint64
				for {
					select {
					case <-ctx.Done():
						readerDone <- nil
						return
					case <-ticker.C:
						page, err := w.ReadAfter(cursor, 128)
						if err != nil {
							readerDone <- err
							return
						}
						for _, r := range page {
							cursor++
							if r.Sequence != cursor || r.Detail != strconv.FormatUint(cursor, 10) || !bytes.Equal(r.Raw, payload) {
								readerDone <- fmt.Errorf("slow reader mismatch at %d", cursor)
								return
							}
						}
					}
				}
			}()
			defer cancel()
			latencies := make([]time.Duration, 0, (count+batchSize-1)/batchSize)
			var peakHeap uint64
			var maxScheduleLag time.Duration
			start := time.Now()
			for n := 0; n < count; {
				size := min(batchSize, count-n)
				deadline := start.Add(time.Duration(n+size) * time.Second / time.Duration(rate))
				if delay := time.Until(deadline); delay > 0 {
					time.Sleep(delay)
				}
				batch := make([]Record, size)
				for i := range batch {
					batch[i] = walV2FixtureRecord(0, strconv.Itoa(n+i+1))
					batch[i].Raw = payload
				}
				before := time.Now()
				if err := w.AppendBatch(batch); err != nil {
					t.Fatal(err)
				}
				latencies = append(latencies, time.Since(before))
				maxScheduleLag = max(maxScheduleLag, time.Since(deadline))
				n += size
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				peakHeap = max(peakHeap, mem.HeapAlloc)
			}
			elapsed := time.Since(start)
			cancel()
			if err := <-readerDone; err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			recoveryStart := time.Now()
			var visited uint64
			reopened, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(r Record) error {
				visited++
				if r.Sequence != visited || r.Detail != strconv.FormatUint(visited, 10) || !bytes.Equal(r.Raw, payload) {
					return fmt.Errorf("recovery mismatch at %d", visited)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			recovery := time.Since(recoveryStart)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if visited != uint64(count) {
				t.Fatalf("replayed=%d expected=%d", visited, count)
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			t.Logf("records=%d elapsed=%s achieved_records_per_second=%.1f append_p95=%s append_p99=%s max_schedule_lag=%s peak_sampled_heap_bytes=%d verified_recovery=%s",
				count, elapsed, float64(count)/elapsed.Seconds(), latencies[(len(latencies)-1)*95/100], latencies[(len(latencies)-1)*99/100], maxScheduleLag, peakHeap, recovery)
		})
	}
}
