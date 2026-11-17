package ingest

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func walV2FixtureRecord(rawBytes int, detail string) Record {
	return Record{
		Type: "update", Time: time.Now().UTC(), Source: "source-a", Kind: "transaction",
		Detail: detail, Raw: bytes.Repeat([]byte{0x5a}, rawBytes),
	}
}

func createRotatedWALV2(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "journal.v2")
	w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(walV2FixtureRecord(1<<20, "first")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(walV2FixtureRecord(1<<20, "second")); err != nil {
		t.Fatal(err)
	}
	if w.Count() != 2 {
		t.Fatalf("WAL v2 count=%d want=2", w.Count())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestWALV2RotationReopenAndCursorPaging(t *testing.T) {
	dir := createRotatedWALV2(t)
	manifest, exists, err := readWALV2Current(dir)
	if err != nil || !exists {
		t.Fatalf("read current manifest exists=%v err=%v", exists, err)
	}
	if len(manifest.Sealed) != 1 || manifest.Active.ID != 2 || manifest.Sealed[0].LastSequence != 1 || manifest.Active.LastSequence != 2 {
		t.Fatalf("rotation manifest is inconsistent: %+v", manifest)
	}

	var replayed []Record
	w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(record Record) error {
		replayed = append(replayed, record)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.Count() != 2 || len(replayed) != 2 || replayed[0].Detail != "first" || replayed[1].Detail != "second" {
		t.Fatalf("reopen replay count=%d records=%+v", w.Count(), replayed)
	}
	page, err := w.ReadAfter(0, 1)
	if err != nil || len(page) != 1 || page[0].Sequence != 1 || page[0].Detail != "first" {
		t.Fatalf("first live page=%+v err=%v", page, err)
	}
	page, err = w.ReadAfter(1, 1)
	if err != nil || len(page) != 1 || page[0].Sequence != 2 || page[0].Detail != "second" {
		t.Fatalf("second live page=%+v err=%v", page, err)
	}
	journalID := w.journalID
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenWALReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err = reader.ReadAfter(1, 10)
	if err != nil || len(page) != 1 || page[0].Sequence != 2 {
		t.Fatalf("offline cursor page=%+v err=%v", page, err)
	}
	var offline []Record
	report, err := reader.Read(false, func(record Record) error {
		offline = append(offline, record)
		return nil
	})
	if err != nil || report.Records != 2 || len(offline) != 2 || offline[0].JournalID != journalID {
		t.Fatalf("offline replay report=%+v records=%d err=%v", report, len(offline), err)
	}
}

func TestWALV2TruncatesOnlyUncommittedActiveTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "journal.v2")
	w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(walV2FixtureRecord(128, "durable")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, _, err := readWALV2Current(dir)
	if err != nil {
		t.Fatal(err)
	}
	activePath := walV2SegmentPath(dir, manifest.Active.ID, false)
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03, 0x04}); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered.RecoveredTail || recovered.Count() != 1 {
		t.Fatalf("recovered tail=%v count=%d", recovered.RecoveredTail, recovered.Count())
	}
	page, err := recovered.ReadAfter(0, 10)
	if err != nil || len(page) != 1 || page[0].Detail != "durable" {
		t.Fatalf("valid prefix after tail recovery=%+v err=%v", page, err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALV2RecoversCrashAfterSealingBeforeManifestPublish(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "journal.v2")
	w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(walV2FixtureRecord(128, "committed-before-rotation")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, _, err := readWALV2Current(dir)
	if err != nil {
		t.Fatal(err)
	}
	active := manifest.Active
	openPath := walV2SegmentPath(dir, active.ID, false)
	sealedPath := walV2SegmentPath(dir, active.ID, true)
	f, err := os.OpenFile(openPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := hashWALV2Prefix(f, active.CommittedBytes)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	footer := encodeWALV2Footer(walV2Footer{
		Segment: active.ID, FirstSequence: active.FirstSequence, LastSequence: active.LastSequence,
		Records: active.Records, DataBytes: uint64(active.CommittedBytes), Digest: digest,
	})
	if _, err := f.WriteAt(footer, active.CommittedBytes); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(openPath, sealedPath); err != nil {
		t.Fatal(err)
	}
	if err := syncWALV2Dir(filepath.Join(dir, "segments")); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Count() != 1 {
		t.Fatalf("recovered count=%d want=1", recovered.Count())
	}
	page, err := recovered.ReadAfter(0, 10)
	if err != nil || len(page) != 1 || page[0].Detail != "committed-before-rotation" {
		t.Fatalf("recovered sealed prefix=%+v err=%v", page, err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALV2FailsClosedOnCorruptSealedSegmentOrManifest(t *testing.T) {
	dir := createRotatedWALV2(t)
	sealedPath := walV2SegmentPath(dir, 1, true)
	f, err := os.OpenFile(sealedPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, headerBytes, err := readWALV2Header(f)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], headerBytes+8+walV2FramePrefixBytes+4); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], headerBytes+8+walV2FramePrefixBytes+4); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil); err == nil {
		t.Fatal("corrupt sealed segment was accepted")
	}

	dir = createRotatedWALV2(t)
	manifest, _, err := readWALV2Current(dir)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "manifests", walV2ManifestFileName(manifest.Generation))
	if err := os.WriteFile(manifestPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil); err == nil {
		t.Fatal("tampered manifest was accepted")
	}
}

func TestStoreCanOptIntoWALV2WithoutReplacingV1Journal(t *testing.T) {
	c := recoveryConfig(t)
	c.WALVersion = 2
	c.WALSegmentBytes = 2 << 20
	c.BatchBytes = 1 << 20
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	journalID := s.Snapshot().JournalID
	if _, err := s.Append(Record{Type: "marker", Source: c.SourceID, Kind: "v2_store_fixture", Detail: "persisted", Time: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.DataDir, "journal.wal")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("WAL v2 unexpectedly replaced or created the v1 journal: stat err=%v", err)
	}

	reopened, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Snapshot().JournalID; got != journalID {
		t.Fatalf("journal ID changed across v2 reopen: got=%s want=%s", got, journalID)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := reopened.OpenReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var found bool
	if _, err := reader.Read(false, func(record Record) error {
		if record.Kind == "v2_store_fixture" && record.Detail == "persisted" {
			found = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("WAL v2 Store replay did not return its persisted marker")
	}
}
