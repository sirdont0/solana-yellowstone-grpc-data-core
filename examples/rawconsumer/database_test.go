package main

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
	bolt "go.etcd.io/bbolt"
)

func applicationEffectCount(t *testing.T, db *bolt.DB) uint64 {
	t.Helper()
	var count uint64
	if err := db.View(func(tx *bolt.Tx) error {
		if value := tx.Bucket(effectsBucket).Get(effectCountKey); value != nil {
			count = binary.BigEndian.Uint64(value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestApplicationCommitSurvivesReplayBeforeAck(t *testing.T) {
	path := filepath.Join(t.TempDir(), "application.db")
	db, err := openApplicationDB(path)
	if err != nil {
		t.Fatal(err)
	}
	observation := core.RawObservation{
		Version: 1, Type: "marker", Kind: "test",
		Cursor: core.RawCursor{Source: "a", Identity: "identity", JournalID: "journal", Sequence: 1, ConsumerEpoch: "first"},
	}
	if err := commitObservation(db, observation); err != nil {
		t.Fatal(err)
	}
	// Application commit succeeded, but Core ACK was never written. Restart
	// the application and replay the same WAL cursor, even in a new lifecycle.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openApplicationDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	observation.Cursor.ConsumerEpoch = "second"
	for i := 0; i < 3; i++ {
		if err := commitObservation(db, observation); err != nil {
			t.Fatal(err)
		}
	}
	if count := applicationEffectCount(t, db); count != 1 {
		t.Fatalf("redelivery applied %d effects instead of one", count)
	}
	conflicting := observation
	conflicting.Kind = "different-payload"
	if err := commitObservation(db, conflicting); err == nil {
		t.Fatal("conflicting observation was silently accepted")
	}
	if count := applicationEffectCount(t, db); count != 1 {
		t.Fatalf("conflicting transaction changed effect count to %d", count)
	}
	if err := commitObservation(db, observation); err != nil {
		t.Fatalf("failed transaction replaced original observation: %v", err)
	}
	observation.Cursor.Sequence++
	if err := commitObservation(db, observation); err != nil {
		t.Fatal(err)
	}
	observation.Cursor.JournalID = "new-journal"
	observation.Cursor.Sequence = 1
	if err := commitObservation(db, observation); err != nil {
		t.Fatal(err)
	}
	if count := applicationEffectCount(t, db); count != 3 {
		t.Fatalf("different cursor identities were collapsed: effects=%d", count)
	}
}

func TestApplicationRejectsUnboundCursor(t *testing.T) {
	db, err := openApplicationDB(filepath.Join(t.TempDir(), "application.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := commitObservation(db, core.RawObservation{}); err == nil {
		t.Fatal("unbound observation accepted")
	}
	if count := applicationEffectCount(t, db); count != 0 {
		t.Fatalf("invalid observation applied %d effects", count)
	}
}
