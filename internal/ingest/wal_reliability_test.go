package ingest

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func walFixture(t *testing.T, count int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "journal.wal")
	w, err := OpenWAL(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]Record, count)
	for i := range records {
		records[i] = Record{Type: "marker", Kind: "fixture", Detail: "valid WAL record"}
	}
	if err := w.AppendBatch(records); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func appendWALTestBytes(t *testing.T, path string, tail []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(tail); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClosedWALReaderDeliversValidPrefixBeforeDamagedTail(t *testing.T) {
	badBody, err := json.Marshal(Record{Type: "marker", Kind: "corrupt"})
	if err != nil {
		t.Fatal(err)
	}
	badFrame := make([]byte, 8+len(badBody))
	binary.LittleEndian.PutUint32(badFrame[:4], uint32(len(badBody)))
	binary.LittleEndian.PutUint32(badFrame[4:8], crc32.ChecksumIEEE(badBody))
	copy(badFrame[8:], badBody)
	badFrame[len(badFrame)-1] ^= 1

	for _, tc := range []struct {
		name string
		tail []byte
	}{
		{"torn header", []byte{1, 2, 3}},
		{"torn body", []byte{100, 0, 0, 0, 0, 0, 0, 0, 'x'}},
		{"bad CRC", badFrame},
		{"invalid length", []byte{0, 0, 0, 0, 0, 0, 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := walFixture(t, 3)
			appendWALTestBytes(t, path, tc.tail)
			reader, err := OpenWALReader(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			got, err := reader.ReadAfter(0, 10)
			if err != nil || len(got) != 3 || got[0].Sequence != 1 || got[2].Sequence != 3 {
				t.Fatalf("valid prefix was lost: records=%v err=%v", got, err)
			}
			got, err = reader.ReadAfter(3, 10)
			if err == nil || len(got) != 0 {
				t.Fatalf("damaged tail must be reported after prefix delivery: records=%v err=%v", got, err)
			}
		})
	}
}

func TestWALAppendBatchPreflightPreventsPartialCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.wal")
	w, err := OpenWAL(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Kind: "durable"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	bad := Record{Time: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err := w.AppendBatch([]Record{{Kind: "must_not_commit"}, bad}); err == nil {
		t.Fatal("later invalid record was accepted")
	}
	if w.Count() != 1 {
		t.Fatalf("failed batch changed count to %d", w.Count())
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("failed validation left partial batch: before=%d after=%d", before.Size(), after.Size())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var seen []string
	reopened, err := OpenWAL(path, func(r Record) error {
		seen = append(seen, r.Kind)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.RecoveredTail || len(seen) != 1 || seen[0] != "durable" {
		t.Fatalf("unexpected recovered WAL after rejected batch: tail=%v records=%v", reopened.RecoveredTail, seen)
	}
	_ = reopened.Close()
}

func TestWALAppendBatchRollsBackShortWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.wal")
	w, err := OpenWAL(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Kind: "durable"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected second-frame write failure")
	writes := 0
	w.writeFrame = func(frame []byte) (int, error) {
		writes++
		if writes == 2 {
			n, err := w.f.Write(frame[:4])
			if err != nil {
				return n, err
			}
			return n, injected
		}
		return w.f.Write(frame)
	}
	if err := w.AppendBatch([]Record{{Kind: "first_failed"}, {Kind: "second_failed"}}); !errors.Is(err, injected) {
		t.Fatalf("write failure not returned: %v", err)
	}
	if w.Count() != 1 {
		t.Fatalf("failed batch changed count to %d", w.Count())
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("rollback changed committed prefix: before=%d after=%d", before.Size(), after.Size())
	}
	if err := w.Append(Record{Kind: "after_failure"}); !errors.Is(err, injected) {
		t.Fatalf("write failure was not sticky: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var kinds []string
	reopened, err := OpenWAL(path, func(r Record) error {
		kinds = append(kinds, r.Kind)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.RecoveredTail || len(kinds) != 1 || kinds[0] != "durable" {
		t.Fatalf("failed batch survived rollback: tail=%v kinds=%v", reopened.RecoveredTail, kinds)
	}
	_ = reopened.Close()
}

func TestClosedWALReaderUsesSparseIndexAndRejectsStaleCache(t *testing.T) {
	path := walFixture(t, walIndexStride*6+17)
	if _, err := os.Stat(path + ".sidx"); err != nil {
		t.Fatalf("closed WAL did not persist sparse index: %v", err)
	}
	reader, err := OpenWALReader(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.index) < 6 || reader.index[0].Sequence != 1 || reader.index[1].Sequence != walIndexStride+1 {
		t.Fatalf("sparse index not loaded: %+v", reader.index)
	}
	start := uint64(walIndexStride*5 + 9)
	page, err := reader.ReadAfter(start, 4)
	if err != nil || len(page) != 4 || page[0].Sequence != start+1 || page[3].Sequence != start+4 {
		t.Fatalf("indexed resume returned incorrect page: page=%v err=%v", page, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	// A changed WAL must invalidate its old offset cache; a torn tail is
	// still detected once the valid prefix has been delivered.
	appendWALTestBytes(t, path, []byte{1, 2, 3})
	reader, err = OpenWALReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if len(reader.index) != 0 {
		t.Fatalf("stale sparse index was trusted: %+v", reader.index)
	}
	last := uint64(walIndexStride*6 + 17)
	page, err = reader.ReadAfter(last-2, 10)
	if err != nil || len(page) != 2 || page[0].Sequence != last-1 || page[1].Sequence != last {
		t.Fatalf("valid records before torn tail were lost: page=%v err=%v", page, err)
	}
	if _, err := reader.ReadAfter(last, 10); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("torn tail was not detected: %v", err)
	}
}
