package ingest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func diskDedupeConfig(t *testing.T, hotLimit int) Config {
	t.Helper()
	c := recoveryConfig(t)
	c.DedupeDisk = true
	c.DedupeLimit = hotLimit
	return c
}

func dedupeFixture(c Config, n int) Record {
	return Record{
		Type:      "update",
		Kind:      "transaction",
		Source:    "A",
		Slot:      uint64(100 + n),
		Identity:  c.Identity(),
		DedupeKey: fmt.Sprintf("transaction-%04d", n),
		Digest:    fmt.Sprintf("digest-%04d", n),
	}
}

func TestDiskDedupeExceedsHotCacheAndFindsEvictedKeys(t *testing.T) {
	c := diskDedupeConfig(t, 3)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	records := make([]Record, 64)
	for i := range records {
		records[i] = dedupeFixture(c, i)
	}
	accepted, err := s.AppendBatch(records)
	if err != nil {
		t.Fatal(err)
	}
	for i, ok := range accepted {
		if !ok {
			t.Fatalf("record %d rejected after cache budget", i)
		}
	}
	if got := len(s.dedupe); got != c.DedupeLimit {
		t.Fatalf("hot cache has %d entries, want %d", got, c.DedupeLimit)
	}
	if _, inHotCache := s.dedupe[records[0].DedupeKey]; inHotCache {
		t.Fatal("oldest record was not evicted from the hot cache")
	}
	if ok, err := s.Append(records[0]); err != nil || ok {
		t.Fatalf("late duplicate accepted=%v err=%v", ok, err)
	}
	batchAccepted, err := s.AppendBatch([]Record{records[2], records[3]})
	if err != nil || len(batchAccepted) != 2 || batchAccepted[0] || batchAccepted[1] {
		t.Fatalf("late batch duplicates accepted=%v err=%v", batchAccepted, err)
	}
	if got := len(s.dedupe); got > c.DedupeLimit {
		t.Fatalf("late lookup exceeded cache budget: %d", got)
	}
	conflict := records[1]
	conflict.Digest = "different-payload"
	if ok, err := s.Append(conflict); ok || !errors.Is(err, ErrDedupeConflict) {
		t.Fatalf("late conflict accepted=%v err=%v", ok, err)
	}
	if !s.Snapshot().ContinuityBroken {
		t.Fatal("late conflict did not persist a continuity break")
	}
}

func TestDiskDedupeRebuildsAcrossRestartAndIgnoresStaleIndex(t *testing.T) {
	c := diskDedupeConfig(t, 2)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if ok, err := s.Append(dedupeFixture(c, i)); err != nil || !ok {
			t.Fatalf("append %d accepted=%v err=%v", i, ok, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate an index from an incompatible WAL suffix: a missing durable key,
	// a wrong digest and a phantom key. Recovery must discard all three.
	db, err := bolt.Open(filepath.Join(c.DataDir, dedupeDiskFilename), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dedupeBucket)
		if err := b.Delete([]byte(dedupeFixture(c, 0).DedupeKey)); err != nil {
			return err
		}
		if err := b.Put([]byte(dedupeFixture(c, 1).DedupeKey), []byte("wrong-digest")); err != nil {
			return err
		}
		return b.Put([]byte("phantom"), []byte("phantom-digest"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Abort()
	if len(reopened.dedupe) > c.DedupeLimit {
		t.Fatal("WAL replay exceeded configured hot-cache budget")
	}
	for _, i := range []int{0, 1, 18} {
		if ok, err := reopened.Append(dedupeFixture(c, i)); err != nil || ok {
			t.Fatalf("replayed key %d accepted=%v err=%v", i, ok, err)
		}
	}
	phantom := dedupeFixture(c, 100)
	phantom.DedupeKey = "phantom"
	phantom.Digest = "phantom-digest"
	if ok, err := reopened.Append(phantom); err != nil || !ok {
		t.Fatalf("stale index phantom blocked append accepted=%v err=%v", ok, err)
	}
	conflict := dedupeFixture(c, 19)
	conflict.Digest = "different-after-restart"
	accepted, err := reopened.AppendBatch([]Record{conflict})
	if !errors.Is(err, ErrDedupeConflict) || len(accepted) != 1 || accepted[0] {
		t.Fatalf("late conflict after restart accepted=%v err=%v", accepted, err)
	}
	if got := len(reopened.dedupe); got > c.DedupeLimit {
		t.Fatalf("hot-cache size %d exceeds %d", got, c.DedupeLimit)
	}
}

func TestDiskDedupeRebuildDropsTornWALTailEntry(t *testing.T) {
	c := diskDedupeConfig(t, 1)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	first, torn := dedupeFixture(c, 1), dedupeFixture(c, 2)
	for _, r := range []Record{first, torn} {
		if ok, err := s.Append(r); err != nil || !ok {
			t.Fatalf("append accepted=%v err=%v", ok, err)
		}
	}
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(c.DataDir, "journal.wal")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(walPath, info.Size()-5); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Abort()
	if !reopened.Snapshot().ContinuityBroken {
		t.Fatal("torn WAL tail did not mark continuity broken")
	}
	if ok, err := reopened.Append(first); err != nil || ok {
		t.Fatalf("surviving WAL key accepted=%v err=%v", ok, err)
	}
	if ok, err := reopened.Append(torn); err != nil || !ok {
		t.Fatalf("index retained key from truncated WAL tail accepted=%v err=%v", ok, err)
	}
}

func TestRAMOnlyDedupeStillStopsAtLifetimeLimit(t *testing.T) {
	c := diskDedupeConfig(t, 2)
	c.DedupeDisk = false
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	for i := 0; i < 2; i++ {
		if ok, err := s.Append(dedupeFixture(c, i)); err != nil || !ok {
			t.Fatalf("append %d accepted=%v err=%v", i, ok, err)
		}
	}
	if ok, err := s.Append(dedupeFixture(c, 2)); ok || !errors.Is(err, ErrDedupeLimit) {
		t.Fatalf("RAM-only lifetime limit accepted=%v err=%v", ok, err)
	}
	if _, err := os.Stat(filepath.Join(c.DataDir, dedupeDiskFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default mode unexpectedly created disk index: %v", err)
	}
}
