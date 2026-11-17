package core

import (
	"fmt"
	"testing"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

func TestConsumerLagTracksDurableWALHeadAndACK(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e.stores["a"] = store

	initial, err := e.ConsumerLag("analytics", "a")
	if err != nil {
		t.Fatal(err)
	}
	if initial.AckSequence != 0 || initial.HeadSequence != store.Snapshot().WALRecords || initial.PendingRecords != initial.HeadSequence {
		t.Fatalf("unacknowledged initial WAL lag=%+v", initial)
	}
	cp := store.Snapshot()
	if err := e.Ack("analytics", RawCursor{Source: "a", Identity: cp.Identity, JournalID: cp.JournalID, Sequence: cp.WALRecords}); err != nil {
		t.Fatal(err)
	}
	lag, err := e.ConsumerLag("analytics", "a")
	if err != nil || lag.PendingRecords != 0 || lag.AckSequence != cp.WALRecords {
		t.Fatalf("acknowledged lag=%+v err=%v", lag, err)
	}
	if err := store.Mark("", 0, "more_work", "fixture"); err != nil {
		t.Fatal(err)
	}
	lag, err = e.ConsumerLag("analytics", "a")
	if err != nil || lag.PendingRecords != 1 || lag.HeadSequence != cp.WALRecords+1 {
		t.Fatalf("backlog after append=%+v err=%v", lag, err)
	}
}

func TestConsumerLagReportsOldestPendingAgeAndHistogram(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e.stores["a"] = store
	initial := store.Snapshot()
	if err := e.Ack("analytics", RawCursor{Source: "a", Identity: initial.Identity, JournalID: initial.JournalID, Sequence: initial.WALRecords}); err != nil {
		t.Fatal(err)
	}

	oldestAt := time.Now().UTC().Add(-20 * time.Second)
	record := lab.Marker(toLabConfig(cfg, cfg.Sources[0]), "a", 0, "slow_consumer_fixture", "")
	record.Time = oldestAt
	if _, err := store.Append(record); err != nil {
		t.Fatal(err)
	}

	first, err := e.ConsumerLag("analytics", "a")
	if err != nil {
		t.Fatal(err)
	}
	if first.PendingRecords != 1 || !first.OldestPendingKnown || first.OldestPendingAt != oldestAt || first.OldestPendingAgeMs < 19_000 {
		t.Fatalf("oldest pending age was not reported: %+v", first)
	}
	if first.AgeSampleCount != 1 || first.AgeBuckets[3] != 1 {
		t.Fatalf("20s sample should enter the <=30s bucket: %+v", first)
	}

	second, err := e.ConsumerLag("analytics", "a")
	if err != nil {
		t.Fatal(err)
	}
	if second.AgeSampleCount != 2 || second.AgeBuckets[3] != 2 {
		t.Fatalf("consumer lag histogram did not accumulate scrape samples: %+v", second)
	}

	cp := store.Snapshot()
	if err := e.Ack("analytics", RawCursor{Source: "a", Identity: cp.Identity, JournalID: cp.JournalID, Sequence: cp.WALRecords}); err != nil {
		t.Fatal(err)
	}
	cleared, err := e.ConsumerLag("analytics", "a")
	if err != nil {
		t.Fatal(err)
	}
	if cleared.PendingRecords != 0 || cleared.OldestPendingKnown || cleared.AgeSampleCount != 2 || cleared.AgeBuckets[3] != 2 {
		t.Fatalf("acknowledged consumer should have no pending age sample: %+v", cleared)
	}
}

func TestConsumerLagReadsOldestPendingFromClosedWAL(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e.stores["a"] = store
	initial := store.Snapshot()
	if err := e.Ack("analytics", RawCursor{Source: "a", Identity: initial.Identity, JournalID: initial.JournalID, Sequence: initial.WALRecords}); err != nil {
		t.Fatal(err)
	}
	oldestAt := time.Now().UTC().Add(-2 * time.Minute)
	record := lab.Marker(toLabConfig(cfg, cfg.Sources[0]), "a", 0, "closed_wal_lag_fixture", "")
	record.Time = oldestAt
	if _, err := store.Append(record); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	lag, err := e.ConsumerLag("analytics", "a")
	if err != nil {
		t.Fatal(err)
	}
	if lag.PendingRecords < 1 || !lag.OldestPendingKnown || lag.OldestPendingAt != oldestAt || lag.OldestPendingAgeMs < 119_000 {
		t.Fatalf("closed WAL oldest pending age was not reported: %+v", lag)
	}
}

func TestConsumerLagHistogramHasBoundedConsumerCardinality(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e.stores["a"] = store

	for i := 0; i < maxConsumerLagHistograms+1; i++ {
		consumer := fmt.Sprintf("c%04d", i)
		if _, err := e.ConsumerLag(consumer, "a"); err != nil {
			t.Fatalf("ConsumerLag(%q): %v", consumer, err)
		}
	}
	if got := len(e.consumerLagSamples); got != maxConsumerLagHistograms {
		t.Fatalf("consumer lag histogram cardinality=%d want=%d", got, maxConsumerLagHistograms)
	}
}
