package ingest

import (
	"errors"
	"testing"
)

// A failed fsync must never produce a durable consumer callback or advance the
// advertised cursor. The failed WAL writer remains unusable until recovery.
func TestC2WALV1FsyncFailureDoesNotAcknowledgeBatch(t *testing.T) {
	cfg := Config{
		SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token",
		Commitment: "finalized", FilterVersion: "c2-fsync-fault",
		Programs: []string{PumpProgram}, DataDir: t.TempDir(),
		DedupeLimit: 20, Streams: 1,
	}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Mark("aa", 1, "baseline", "durable prefix"); err != nil {
		t.Fatal(err)
	}
	base := store.Snapshot().WALRecords
	var callbacks int
	store.SetDurableObserver(func(Record) error { callbacks++; return nil })
	injected := errors.New("injected WAL v1 fsync failure")
	store.wal.syncFrame = func() error { return injected }
	rec := Record{Type: "update", Kind: "transaction", Source: "aa", Identity: cfg.Identity(), Generation: 1, Slot: 90, DedupeKey: "candidate", Digest: "candidate"}
	accepted, err := store.AppendBatch([]Record{rec})
	if !errors.Is(err, injected) || accepted != nil {
		t.Fatalf("fsync error=%v accepted=%v", err, accepted)
	}
	if callbacks != 0 {
		t.Fatalf("undurable callback count=%d", callbacks)
	}
	if got := store.Snapshot().WALRecords; got != base {
		t.Fatalf("fsync failure advanced WAL count: %d vs %d", got, base)
	}
	if _, err := store.Append(rec); !errors.Is(err, injected) {
		t.Fatalf("failed writer accepted follow-up: %v", err)
	}
	if err := store.Close(); !errors.Is(err, injected) {
		t.Fatalf("failed Store.Close must preserve sticky fsync error: %v", err)
	}
	var recovered []Record
	reopened, err := OpenStoreObserved(cfg, func(r Record) error { recovered = append(recovered, r); return nil })
	if err != nil {
		t.Fatalf("reopen after fsync rollback: %v", err)
	}
	defer reopened.Close()
	if got := reopened.Snapshot().WALRecords; got < base {
		t.Fatalf("recovery lost committed prefix: count=%d baseline=%d", got, base)
	}
	if len(recovered) < int(base) {
		t.Fatalf("recovery observer lost committed prefix: recovered=%d baseline=%d", len(recovered), base)
	}
	page, err := reopened.ReadAfter(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range append(recovered, page...) {
		if r.DedupeKey == "candidate" {
			t.Fatal("failed batch survived fsync rollback")
		}
	}
}
