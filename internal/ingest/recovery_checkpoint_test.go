package ingest

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func checkpointFixture(t *testing.T) (Config, Checkpoint) {
	t.Helper()
	c := diskDedupeConfig(t, 2)
	c.WALVersion = 2
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]Record, 32)
	for i := range records {
		records[i] = dedupeFixture(c, i)
	}
	if _, err := s.AppendBatch(records); err != nil {
		t.Fatal(err)
	}
	if err := s.Mark("", 0, "continuity_broken", "test historical quality"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return c, s.Snapshot()
}

func TestRecoveryCheckpointReusesStateAndExactDedupe(t *testing.T) {
	c, prior := checkpointFixture(t)
	var observed uint64
	s, err := OpenStoreObserved(c, func(r Record) error { observed++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	if s.recoveredCheckpoint != prior.WALRecords {
		t.Fatalf("snapshot not reused: %d", s.recoveredCheckpoint)
	}
	after := s.Snapshot()
	if !reflect.DeepEqual(after.Sources, prior.Sources) || !reflect.DeepEqual(after.BreakReasons, prior.BreakReasons) || after.Quality != prior.Quality {
		t.Fatalf("recovered state differs: before=%+v after=%+v", prior, after)
	}
	if observed != after.WALRecords {
		t.Fatalf("observer missed history: observed=%d head=%d", observed, after.WALRecords)
	}
	if len(s.dedupe) != 0 {
		t.Fatal("recovery populated hot cache from entire history")
	}
	if ok, err := s.Append(dedupeFixture(c, 0)); err != nil || ok {
		t.Fatalf("historical duplicate lost: %v %v", ok, err)
	}
	conflict := dedupeFixture(c, 31)
	conflict.Digest = "conflict"
	if _, err := s.Append(conflict); !errors.Is(err, ErrDedupeConflict) {
		t.Fatalf("historical conflict lost: %v", err)
	}
}

func TestRecoveryCheckpointReplaysUnindexedTailAcrossRotation(t *testing.T) {
	c, prior := checkpointFixture(t)
	w, err := OpenWALV2(filepath.Join(c.DataDir, "journal.v2"), c.SourceID, c.Identity(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.v2.rotateLocked(); err != nil {
		t.Fatal(err)
	}
	tail := dedupeFixture(c, 100)
	if err := w.Append(tail); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	if s.recoveredCheckpoint != prior.WALRecords {
		t.Fatal("matching old prefix was not reused")
	}
	if got := s.Snapshot().Sources["A"].Received; got != 33 {
		t.Fatalf("tail not applied exactly once: received=%d", got)
	}
	if ok, err := s.Append(tail); err != nil || ok {
		t.Fatalf("tail not added to dedupe: %v %v", ok, err)
	}
}

func TestRecoveryCheckpointInvalidCacheRebuilds(t *testing.T) {
	for _, damage := range []string{"missing", "truncated", "altered-db", "missing-db"} {
		t.Run(damage, func(t *testing.T) {
			c, prior := checkpointFixture(t)
			cachePath := filepath.Join(c.DataDir, recoveryCheckpointFile)
			dbPath := filepath.Join(c.DataDir, dedupeDiskFilename)
			switch damage {
			case "missing":
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				if err := os.Truncate(cachePath, 5); err != nil {
					t.Fatal(err)
				}
			case "missing-db":
				if err := os.Remove(dbPath); err != nil {
					t.Fatal(err)
				}
			case "altered-db":
				db, err := bolt.Open(dbPath, 0600, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Update(func(tx *bolt.Tx) error { return tx.Bucket(dedupeBucket).Delete([]byte(dedupeFixture(c, 0).DedupeKey)) }); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			s, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Abort()
			if s.recoveredCheckpoint != 0 {
				t.Fatal("invalid snapshot was trusted")
			}
			if !reflect.DeepEqual(s.Snapshot().Sources, prior.Sources) {
				t.Fatal("full rebuild differs from snapshot state")
			}
			if ok, err := s.Append(dedupeFixture(c, 0)); err != nil || ok {
				t.Fatalf("rebuild lost key: %v %v", ok, err)
			}
		})
	}
}

func TestRecoveryCheckpointChangedIndexAfterAbortRebuilds(t *testing.T) {
	c, _ := checkpointFixture(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	tail := dedupeFixture(c, 100)
	if _, err := s.Append(tail); err != nil {
		t.Fatal(err)
	}
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	if s.recoveredCheckpoint != 0 {
		t.Fatal("changed index was paired with old snapshot")
	}
	if s.Snapshot().Sources["A"].Received != 33 {
		t.Fatal("full rebuild lost tail")
	}
	if ok, err := s.Append(tail); err != nil || ok {
		t.Fatalf("tail dedupe lost: %v %v", ok, err)
	}
}

func TestRecoveryCheckpointPublicationFailureIsOptional(t *testing.T) {
	c := diskDedupeConfig(t, 2)
	c.WALVersion = 2
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(dedupeFixture(c, 1)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(c.DataDir, recoveryCheckpointFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("cache publication changed successful close: %v", err)
	}
	s, err = OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	if s.recoveredCheckpoint != 0 || s.Snapshot().ContinuityBroken {
		t.Fatal("optional cache failure changed recovery semantics")
	}
	if ok, err := s.Append(dedupeFixture(c, 1)); err != nil || ok {
		t.Fatalf("rebuild lost duplicate: %v %v", ok, err)
	}
}

func BenchmarkWALV2StoreRecovery(b *testing.B) {
	for _, cached := range []bool{false, true} {
		name := "full_rebuild"
		if cached {
			name = "verified_snapshot"
		}
		b.Run(name, func(b *testing.B) {
			c := Config{DataDir: b.TempDir(), WALVersion: 2, WALSegmentBytes: 2 << 20, DedupeDisk: true, DedupeLimit: 128, FilterVersion: "bench", Commitment: "finalized"}
			s, err := OpenStore(c)
			if err != nil {
				b.Fatal(err)
			}
			for start := 0; start < 10000; start += 500 {
				records := make([]Record, 500)
				for i := range records {
					records[i] = dedupeFixture(c, start+i)
				}
				if _, err := s.AppendBatch(records); err != nil {
					b.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}
			// Warm the sealed seek caches in both cases to isolate state/dedupe.
			s, err = OpenStore(c)
			if err != nil {
				b.Fatal(err)
			}
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				if !cached {
					if err := os.Remove(filepath.Join(c.DataDir, recoveryCheckpointFile)); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				s, err = OpenStore(c)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if (s.recoveredCheckpoint > 0) != cached {
					b.Fatal("wrong recovery path")
				}
				if s.Snapshot().Sources["A"].Received != 10000 {
					b.Fatal("recovery count mismatch")
				}
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
