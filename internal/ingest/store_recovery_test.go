package ingest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func recoveryConfig(t *testing.T) Config {
	t.Helper()
	return integrationConfig(t, "grpc://127.0.0.1:1")
}

func TestClosedStoreReaderRejectsReplacedJournal(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			c := recoveryConfig(t)
			c.WALVersion = version
			s, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			// Preserve the old journal for diagnosis, then recreate the path.
			if err := os.Rename(c.DataDir, c.DataDir+"-previous"); err != nil {
				t.Fatal(err)
			}
			replacement, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := replacement.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := s.OpenReader()
			if err == nil {
				_ = r.Close()
				t.Fatal("old Store reader silently adopted a replacement journal")
			}
		})
	}
}

func TestClosedStoreReaderKeepsItsDurableBoundary(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			c := recoveryConfig(t)
			c.WALVersion = version
			s, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			boundary := s.wal.Count()
			later, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := later.AppendBatch([]Record{{Type: "marker", Kind: "later-process"}}); err != nil {
				t.Fatal(err)
			}
			if err := later.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := s.OpenReader()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			page, err := r.ReadAfter(boundary-1, 128)
			if err != nil || len(page) != 1 || page[0].Sequence != boundary {
				t.Fatalf("old Store reader crossed durable boundary %d: records=%v err=%v", boundary, page, err)
			}
			page, err = r.ReadAfter(boundary, 128)
			if err != nil || len(page) != 0 {
				t.Fatalf("old Store reader delivered later-process records: records=%v err=%v", page, err)
			}
		})
	}
}

func TestWALJournalCursorSparseIndexAndLiveWait(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	initial := s.Snapshot()
	if initial.JournalID == "" || initial.WALRecords == 0 {
		t.Fatalf("journal identity/initial marker missing: %+v", initial)
	}
	base := initial.WALRecords
	wake, ready, closed := s.WaitAfter(base)
	if ready || closed || wake == nil {
		t.Fatalf("unexpected initial wait state ready=%v closed=%v", ready, closed)
	}
	markers := make([]Record, 2050)
	for i := range markers {
		markers[i] = Record{Type: "marker", Source: "A", Generation: 1, Kind: "test_record", Detail: "bounded cursor fixture"}
	}
	accepted, err := s.AppendBatch(markers)
	if err != nil {
		t.Fatal(err)
	}
	for i, ok := range accepted {
		if !ok {
			t.Fatalf("marker %d was not accepted", i)
		}
	}
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("WaitAfter notification was lost")
	}
	bounded, err := s.ReadAfter(0, 100000)
	if err != nil || len(bounded) != maxWALReadBatchRecords {
		t.Fatalf("WAL read batch was not record-bounded: len=%d err=%v", len(bounded), err)
	}
	page, err := s.ReadAfter(base+1021, 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 12 {
		t.Fatalf("sparse-index page length=%d want=12", len(page))
	}
	if page[0].Sequence != base+1022 || page[11].Sequence != base+1033 {
		t.Fatalf("sparse-index page boundaries: first=%d last=%d base=%d", page[0].Sequence, page[len(page)-1].Sequence, base)
	}
	journalID := s.Snapshot().JournalID
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadAfter(0, 1); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("closed store ReadAfter err=%v", err)
	}
	reader, err := s.OpenReader()
	if err != nil {
		t.Fatal(err)
	}
	page, err = reader.ReadAfter(base+1021, 12)
	if err != nil || len(page) != 12 || page[0].Sequence != base+1022 {
		t.Fatalf("closed WAL reader page len=%d first=%d err=%v", len(page), firstSequence(page), err)
	}
	page2, err := reader.ReadAfter(page[len(page)-1].Sequence, 4)
	if err != nil || len(page2) != 4 || page2[0].Sequence != page[len(page)-1].Sequence+1 {
		t.Fatalf("sequential WAL reader page=%v err=%v", page2, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Snapshot().JournalID; got != journalID {
		t.Fatalf("journal ID changed across restart: got=%s want=%s", got, journalID)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func firstSequence(records []Record) uint64 {
	if len(records) == 0 {
		return 0
	}
	return records[0].Sequence
}

func TestSourceFreshnessSeparatesTransportDataAndProgress(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	transportAt := time.Date(2026, 11, 16, 1, 0, 0, 0, time.UTC)
	dataAt := transportAt.Add(time.Second)
	progressAt := transportAt.Add(2 * time.Second)
	for _, rec := range []Record{
		{Type: "update", Source: "A", Kind: "ping", Time: transportAt},
		{Type: "update", Source: "A", Kind: "transaction", Time: dataAt},
		{Type: "update", Source: "A", Kind: "slot", Time: progressAt},
	} {
		if accepted, err := s.Append(rec); err != nil || !accepted {
			t.Fatalf("append freshness record accepted=%v err=%v", accepted, err)
		}
	}
	state := s.Snapshot().Sources["A"]
	if !state.LastTransportAt.Equal(progressAt) || !state.LastDataAt.Equal(dataAt) || !state.LastProgressAt.Equal(progressAt) {
		t.Fatalf("freshness scopes were conflated: %+v", state)
	}
}

func TestClosedStoreReaderStopsAtLastAcknowledgedDurableSequence(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	committed := s.Snapshot().WALRecords
	body, err := json.Marshal(Record{Type: "marker", Source: "A", Kind: "uncommitted_tail", Sequence: committed + 1, JournalID: s.Snapshot().JournalID})
	if err != nil {
		t.Fatal(err)
	}
	var frame [8]byte
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(body))
	f, err := os.OpenFile(filepath.Join(c.DataDir, "journal.wal"), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(frame[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := s.OpenReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	records, err := reader.ReadAfter(committed, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("closed store exposed a frame beyond its durable count: %+v", records)
	}
}

func TestWALSyncStatsCountGroupedFsyncBatches(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	_, _, _, before := s.SyncStats()
	batch := []Record{
		{Type: "marker", Source: "A", Kind: "one"},
		{Type: "marker", Source: "A", Kind: "two"},
		{Type: "marker", Source: "A", Kind: "three"},
	}
	if _, err := s.AppendBatch(batch); err != nil {
		t.Fatal(err)
	}
	last, total, maxDuration, count := s.SyncStats()
	if count != before+1 || last < 0 || total < last || maxDuration < last {
		t.Fatalf("invalid fsync metrics before=%d after=%d last=%s total=%s max=%s", before, count, last, total, maxDuration)
	}
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
	cp := s.Snapshot()
	if cp.ContinuityBroken || cp.Quality != "OBSERVING_UNVERIFIED" || cp.Sources["A"].Received != 1 || cp.Sources["A"].LastTxSlot != 202 {
		t.Fatalf("clean restart falsely flagged a gap or lost WAL state: %+v", cp)
	}
	if cp.TxInclusionVerifiedThrough != 0 || cp.AccountHistoryVerifiedThrough != 0 {
		t.Fatal("clean restart must not prove exact coverage")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
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

func TestStoreRejectsFilterIdentityChangeWithoutMutatingWAL(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	appendRecoveryTransaction(t, s, c, 601, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.DataDir, "journal.wal")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	other := c
	other.FilterVersion = "different-subscription-v2"
	if _, err := OpenStore(other); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("filter change should be rejected without mixing journal epochs: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected subscription identity mutated the old WAL")
	}
	restarted, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	cp := restarted.Snapshot()
	if cp.ContinuityBroken || cp.Sources["A"].LastTxSlot != 601 || cp.TxInclusionVerifiedThrough != 0 || cp.AccountHistoryVerifiedThrough != 0 {
		t.Fatalf("old subscription could not recover cleanly: %+v", cp)
	}
}
