package ingest

import (
	"errors"
	"syscall"
	"testing"
)

// Inject ENOSPC at the v1 frame-write boundary without exhausting the host's
// disk. Failed bytes must not be acknowledged or exposed after recovery.
func TestC2WALV1ENOSPCIsStickyAndPreservesPrefix(t *testing.T) {
	cfg := Config{
		SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token",
		Commitment: "finalized", FilterVersion: "c2-enospc",
		Programs: []string{PumpProgram}, DataDir: t.TempDir(),
		DedupeLimit: 20, Streams: 1,
	}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Mark("aa", 1, "baseline", "committed before ENOSPC"); err != nil {
		t.Fatal(err)
	}
	baseline := store.Snapshot().WALRecords
	var callbacks int
	store.SetDurableObserver(func(Record) error { callbacks++; return nil })
	store.wal.writeFrame = func([]byte) (int, error) { return 0, syscall.ENOSPC }
	rec := Record{Type: "update", Kind: "transaction", Source: "aa", Identity: cfg.Identity(), DedupeKey: "enospc-candidate", Digest: "candidate", Slot: 101}
	accepted, err := store.AppendBatch([]Record{rec})
	if !errors.Is(err, syscall.ENOSPC) || accepted != nil {
		t.Fatalf("ENOSPC outcome: accepted=%v err=%v", accepted, err)
	}
	if callbacks != 0 || store.Snapshot().WALRecords != baseline {
		t.Fatalf("failed write produced durable effect: callbacks=%d count=%d", callbacks, store.Snapshot().WALRecords)
	}
	if _, err := store.Append(rec); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("sticky ENOSPC missing: %v", err)
	}
	if err := store.Close(); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("close did not preserve ENOSPC: %v", err)
	}
	reopened, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	page, err := reopened.ReadAfter(0, 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range page {
		if r.DedupeKey == "enospc-candidate" {
			t.Fatal("ENOSPC candidate appeared in durable replay")
		}
	}
	if got := reopened.Snapshot().WALRecords; got < baseline {
		t.Fatalf("reopen lost committed prefix: got=%d baseline=%d", got, baseline)
	}
}
