package ingest

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// BenchmarkWALV2TailSeek isolates metadata lookup cost, using synthetic ranges
// for preceding segments and an empty active tail. It does not measure recovery
// or disk throughput. Tail polling must not allocate in proportion to history.
func BenchmarkWALV2TailSeek(b *testing.B) {
	for _, count := range []int{10, 1000, 100000} {
		b.Run(fmt.Sprintf("segments_%d", count), func(b *testing.B) {
			sealed := make([]walV2Segment, count)
			for i := range sealed {
				sealed[i].meta.LastSequence = uint64(i+1) * 1024
			}
			last := uint64(count) * 1024
			active := walV2Segment{meta: walV2SegmentMeta{LastSequence: last}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				page, err := readWALV2After("", sealed, active, nil, last, 1)
				if err != nil || len(page) != 0 {
					b.Fatalf("tail poll: len=%d err=%v", len(page), err)
				}
			}
		})
	}
}

// BenchmarkWALAppendBatch supplies a reproducible local fsync-throughput
// baseline for both journal formats. It is not a live-provider load test.
func BenchmarkWALAppendBatch(b *testing.B) {
	for _, version := range []int{1, 2} {
		b.Run(fmt.Sprintf("v%d_64x1KiB", version), func(b *testing.B) {
			root := b.TempDir()
			var wal *WAL
			var err error
			if version == 1 {
				wal, err = OpenWAL(filepath.Join(root, "journal.wal"), nil)
			} else {
				wal, err = OpenWALV2(filepath.Join(root, "journal.v2"), "source-a", "identity-a", 256<<20, nil)
			}
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = wal.Close() })
			payload := bytes.Repeat([]byte{0x5a}, 1024)
			batch := make([]Record, 64)
			for i := range batch {
				batch[i] = Record{Type: "marker", Kind: "benchmark", Source: "source-a", Time: time.Now().UTC(), Raw: payload}
			}
			b.SetBytes(int64(len(batch) * len(payload)))
			b.ReportAllocs()
			latencies := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				if err := wal.AppendBatch(batch); err != nil {
					b.Fatal(err)
				}
				latencies = append(latencies, time.Since(started))
			}
			b.StopTimer()
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			b.ReportMetric(float64(latencies[(len(latencies)-1)*95/100].Nanoseconds()), "p95-ns/batch")
			b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100].Nanoseconds()), "p99-ns/batch")
			path := wal.path
			journalID := wal.journalID
			if err := wal.Close(); err != nil {
				b.Fatal(err)
			}
			started := time.Now()
			var replayed uint64
			if err := ReadWAL(path, func(record Record) error {
				replayed++
				if record.Sequence != replayed || record.JournalID != journalID || !bytes.Equal(record.Raw, payload) {
					return fmt.Errorf("replay mismatch at sequence %d", replayed)
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(time.Since(started).Nanoseconds()), "verify-ns")
			if replayed != uint64(b.N)*uint64(len(batch)) {
				b.Fatalf("replay count=%d want=%d", replayed, b.N*len(batch))
			}
		})
	}
}
