package ingest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func recoveryConfig(t *testing.T) Config {
	t.Helper()
	return integrationConfig(t, "grpc://127.0.0.1:1")
}

func appendRecoveryTransaction(t *testing.T, s *Store, c Config, slot, index uint64) {
	t.Helper()
	rec, err := FromUpdate(c, "A", 1, txUpdate(slot, index))
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := s.Append(rec)
	if err != nil || !accepted {
		t.Fatalf("append tx slot=%d accepted=%v err=%v", slot, accepted, err)
	}
}

func TestStoreSnapshotReturnsIndependentCopies(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	appendRecoveryTransaction(t, s, c, 101, 1)
	if err := s.Mark("A", 1, "continuity_broken", "synthetic uncertainty"); err != nil {
		t.Fatal(err)
	}
	first := s.Snapshot()
	if first.WALRecords < 3 || first.UpdatedAt.IsZero() || first.Sources["A"].LastTxSlot != 101 || len(first.BreakReasons) == 0 {
		t.Fatalf("incomplete snapshot: %+v", first)
	}
	first.Sources["A"].LastTxSlot = 999
	first.Sources["B"] = &SourceState{LastSlot: 999}
	first.BreakReasons[0] = "mutated by caller"
	first.Quality = "caller mutation"
	second := s.Snapshot()
	if second.Sources["A"].LastTxSlot != 101 || second.Sources["B"] != nil || second.BreakReasons[0] != "continuity_broken: synthetic uncertainty" || second.Quality != "CONTINUITY_BROKEN" {
		t.Fatalf("Snapshot leaked mutable internal state: %+v", second)
	}
	if second.TxInclusionVerifiedThrough != 0 || second.AccountHistoryVerifiedThrough != 0 {
		t.Fatal("local WAL observations cannot promote admission cursors")
	}
}

func TestStoreCleanCloseRestartRestoresWALWithoutBreak(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	appendRecoveryTransaction(t, s, c, 202, 2)
	if err := s.SaveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cp := s.Snapshot()
	if cp.ContinuityBroken || cp.Quality != "OBSERVING_UNVERIFIED" || cp.Sources["A"].Received != 1 || cp.Sources["A"].LastTxSlot != 202 {
		t.Fatalf("clean restart falsely flagged a gap or lost WAL state: %+v", cp)
	}
	if cp.TxInclusionVerifiedThrough != 0 || cp.AccountHistoryVerifiedThrough != 0 {
		t.Fatal("clean restart must not prove exact coverage")
	}
	var records []Record
	if err := ReadWAL(filepath.Join(c.DataDir, "journal.wal"), func(r Record) error {
		records = append(records, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !hasMarker(records, "session_end") || hasMarker(records, "continuity_broken") {
		t.Fatalf("expected clean session boundary in WAL: %+v", records)
	}
}

func TestStoreUncleanRestartRebuildsPastStaleCheckpoint(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Mark("A", 1, "connection_open", "injected live connection"); err != nil {
		t.Fatal(err)
	}
	appendRecoveryTransaction(t, s, c, 301, 1)
	if err := s.SaveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	appendRecoveryTransaction(t, s, c, 309, 2)
	// Model abrupt process termination after the durable WAL fsync, without
	// Store.Close/session_end/checkpoint update. Releasing the file lock here
	// permits a fresh Store to replay the same on-disk records.
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	checkpointBytes, err := os.ReadFile(filepath.Join(c.DataDir, "checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted Checkpoint
	if err := json.Unmarshal(checkpointBytes, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Sources["A"].LastTxSlot != 301 {
		t.Fatalf("expected stale checkpoint at 301, got %+v", persisted.Sources["A"])
	}
	restarted, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	cp := restarted.Snapshot()
	if cp.Sources["A"].LastTxSlot != 309 || cp.Sources["A"].Received != 2 {
		t.Fatalf("restart trusted stale JSON instead of WAL: %+v", cp.Sources["A"])
	}
	if !cp.ContinuityBroken || cp.Quality != "CONTINUITY_BROKEN" || len(cp.BreakReasons) == 0 || !strings.Contains(strings.Join(cp.BreakReasons, "\n"), "previous process did not end") {
		t.Fatalf("unclean restart was not marked uncertain: %+v", cp)
	}
	if cp.TxInclusionVerifiedThrough != 0 || cp.AccountHistoryVerifiedThrough != 0 {
		t.Fatal("unclean restart must leave both admission cursors unverified")
	}
	if cp.Sources["A"].Connected {
		t.Error("recovered checkpoint still reports pre-crash connection as live")
	}
}

func TestWALTornTailRecoveryIsDurableUncertainty(t *testing.T) {
	for _, tc := range []struct {
		name string
		tail []byte
	}{
		{name: "partial frame header", tail: []byte{0x10, 0x00, 0x00}},
		{name: "partial frame body", tail: func() []byte {
			var head [8]byte
			binary.LittleEndian.PutUint32(head[:4], 100)
			return append(head[:], []byte("incomplete")...)
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := recoveryConfig(t)
			s, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			appendRecoveryTransaction(t, s, c, 400, 1)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(c.DataDir, "journal.wal")
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(tc.tail); err != nil {
				f.Close()
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if err := ReadWAL(path, func(Record) error { return nil }); err == nil {
				t.Fatal("incomplete WAL frame unexpectedly passed strict reader")
			}
			recovered, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			cp := recovered.Snapshot()
			if cp.Sources["A"].Received != 1 || cp.Sources["A"].LastTxSlot != 400 || !cp.ContinuityBroken || cp.Quality != "CONTINUITY_BROKEN" {
				t.Errorf("torn-tail recovery lost valid data or hid uncertainty: %+v", cp)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
			var records []Record
			if err := ReadWAL(path, func(r Record) error {
				records = append(records, r)
				return nil
			}); err != nil {
				t.Fatalf("recovered WAL still invalid: %v", err)
			}
			if !hasMarker(records, "continuity_broken") {
				t.Fatal("WAL truncation was not made durably visible")
			}
		})
	}
}

func TestWALFullFrameChecksumCorruptionIsRejected(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	appendRecoveryTransaction(t, s, c, 500, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.DataDir, "journal.wal")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corruptAt := len(walMagic) + 8 + 10
	if corruptAt >= len(data) {
		t.Fatalf("fixture WAL unexpectedly small: %d bytes", len(data))
	}
	data[corruptAt] ^= 0x01
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(c); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("corrupt fully written WAL frame must fail closed: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, after) {
		t.Fatal("corrupt complete WAL frame was silently modified")
	}
	if err := ReadWAL(path, func(Record) error { return nil }); err == nil {
		t.Fatal("strict WAL reader accepted CRC corruption")
	}
}

func TestStoreFilterIdentityChangeMarksUnverifiedHistory(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	appendRecoveryTransaction(t, s, c, 601, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	other := c
	other.FilterVersion = "different-subscription-v2"
	restarted, err := OpenStore(other)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	cp := restarted.Snapshot()
	if cp.Identity != other.Identity() || !cp.ContinuityBroken || !strings.Contains(strings.Join(cp.BreakReasons, "\n"), "filter_identity_changed") {
		t.Fatalf("filter change did not mark historical uncertainty: %+v", cp)
	}
	if cp.Sources["A"].LastTxSlot != 601 || cp.TxInclusionVerifiedThrough != 0 || cp.AccountHistoryVerifiedThrough != 0 {
		t.Fatalf("filter change lost prior observations or promoted verification: %+v", cp)
	}
	// The original Store did close cleanly. A filter change may invalidate
	// coverage, but it must not fabricate an unclean-shutdown diagnosis.
	if strings.Contains(strings.Join(cp.BreakReasons, "\n"), "previous process did not end") {
		t.Error("filter change on clean restart falsely reported a process crash")
	}
}
