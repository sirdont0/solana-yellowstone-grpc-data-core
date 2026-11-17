package ingest

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestWALV2CorruptActivePrefixDoesNotTruncateTail(t *testing.T) {
	w, dir := newWALV2CommitTestWriter(t)
	if err := w.Append(walV2FixtureRecord(128, "committed")); err != nil {
		t.Fatal(err)
	}
	manifest := w.v2.manifest
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := walV2SegmentPath(dir, manifest.Active.ID, false)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents[len(contents)-1] ^= 0xff // Corrupt the committed frame checksum.
	contents = append(contents, []byte("uncommitted forensic evidence")...)
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	var visited int
	reopened, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(Record) error {
		visited++
		return nil
	})
	if err == nil {
		_ = reopened.Close()
		t.Fatal("corrupt committed frame accepted")
	}
	if visited != 0 {
		t.Fatalf("failed recovery delivered %d records", visited)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents, after) {
		t.Fatal("failed recovery truncated active tail before validating committed prefix")
	}
}

func TestWALV2CorruptSealedHistoryDoesNotTruncateActiveTail(t *testing.T) {
	dir := createRotatedWALV2(t)
	manifest, exists, err := readWALV2Current(dir)
	if err != nil || !exists || len(manifest.Sealed) == 0 {
		t.Fatalf("fixture manifest: exists=%v err=%v", exists, err)
	}
	activePath := walV2SegmentPath(dir, manifest.Active.ID, false)
	extra := []byte("uncommitted-tail-must-survive-rejected-recovery")
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(extra); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatal(err)
	}
	sealed := manifest.Sealed[0]
	sealedPath := walV2SegmentPath(dir, sealed.ID, true)
	sf, err := os.OpenFile(sealedPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sf.WriteAt([]byte{0x00}, sealed.DataBytes); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil); err == nil {
		t.Fatal("corrupt sealed history was accepted")
	}
	after, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected recovery changed the uncommitted active tail")
	}
}

func TestWALV2PrunesOldManifestGenerationsWithoutChangingReplay(t *testing.T) {
	w, dir := newWALV2CommitTestWriter(t)
	for i := 0; i < 12; i++ {
		if err := w.Append(walV2FixtureRecord(64, "manifest-gc")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		manifest, exists, err := readWALV2Current(dir)
		if err != nil || !exists {
			t.Fatalf("CURRENT: exists=%v err=%v", exists, err)
		}
		entries, err := os.ReadDir(filepath.Join(dir, "manifests"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 3 {
			t.Fatalf("retained manifest generations=%d want 3", len(entries))
		}
		for i := uint64(0); i < 3; i++ {
			if _, err := readWALV2ManifestGeneration(dir, manifest.Generation-i); err != nil {
				t.Fatalf("retained generation %d: %v", manifest.Generation-i, err)
			}
		}
	}
	check()
	// Older-version archives can already contain many manifests. Reopen
	// should compact that history without changing CURRENT or record order.
	if err := os.WriteFile(filepath.Join(dir, "manifests", walV2ManifestFileName(1)), []byte("obsolete"), 0600); err != nil {
		t.Fatal(err)
	}
	var replayed int
	reopened, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(Record) error {
		replayed++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	check()
	if replayed != 12 {
		t.Fatalf("replay count=%d want 12", replayed)
	}
}

func createWALV2BootstrapFixture(t *testing.T) (string, walV2Manifest) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "journal.v2")
	if err := os.MkdirAll(filepath.Join(dir, "segments"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "manifests"), 0700); err != nil {
		t.Fatal(err)
	}
	header := walV2Header{Version: walV2Version, Segment: 1, Source: "source-a", Identity: "identity-a", JournalID: "journal-bootstrap", FirstSequence: 1}
	encoded, err := encodeWALV2Header(header)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walV2SegmentPath(dir, 1, false), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	manifest := walV2Manifest{
		Version: walV2Version, Generation: 1, Source: header.Source, Identity: header.Identity, JournalID: header.JournalID,
		Active: walV2ActiveMeta{ID: 1, FirstSequence: 1, CommittedBytes: int64(len(encoded))},
	}
	return dir, manifest
}

func TestWALV2RecoversHeaderOnlyBootstrapWithoutCurrent(t *testing.T) {
	for _, stage := range []string{"header-only", "manifest-written", "temporary-manifest"} {
		t.Run(stage, func(t *testing.T) {
			dir, manifest := createWALV2BootstrapFixture(t)
			switch stage {
			case "manifest-written":
				if err := writeWALV2Manifest(dir, manifest); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(dir, "CURRENT")); err != nil {
					t.Fatal(err)
				}
			case "temporary-manifest":
				if err := os.WriteFile(filepath.Join(dir, "manifests", ".wal-v2-crash.tmp"), []byte("partial"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
			if err != nil {
				t.Fatalf("reopen bootstrap: %v", err)
			}
			if w.journalID != manifest.JournalID || w.Count() != 0 {
				t.Fatalf("recovered bootstrap identity/count = %q/%d", w.journalID, w.Count())
			}
			if err := w.Append(walV2FixtureRecord(16, "after-bootstrap")); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			var visited []Record
			reopened, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(record Record) error {
				visited = append(visited, record)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.Count() != 1 || len(visited) != 1 || visited[0].Detail != "after-bootstrap" {
				t.Fatalf("recovered bootstrap replay count=%d records=%+v", reopened.Count(), visited)
			}
		})
	}
}

func TestWALV2BootstrapWithoutCurrentFailsClosedOnAmbiguity(t *testing.T) {
	for _, corruption := range []string{"extra-frame", "partial-header", "other-segment", "wrong-identity", "mismatched-manifest"} {
		t.Run(corruption, func(t *testing.T) {
			dir, manifest := createWALV2BootstrapFixture(t)
			path := walV2SegmentPath(dir, 1, false)
			switch corruption {
			case "extra-frame":
				f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte("uncommitted record bytes")); err != nil {
					t.Fatal(err)
				}
				_ = f.Close()
			case "partial-header":
				if err := os.Truncate(path, 10); err != nil {
					t.Fatal(err)
				}
			case "other-segment":
				if err := os.WriteFile(walV2SegmentPath(dir, 2, false), []byte("unexpected"), 0600); err != nil {
					t.Fatal(err)
				}
			case "wrong-identity":
				if _, err := OpenWALV2(dir, "source-a", "other-identity", 2<<20, nil); err == nil {
					t.Fatal("different identity was adopted")
				}
				return
			case "mismatched-manifest":
				manifest.JournalID = "another-journal"
				if err := writeWALV2Manifest(dir, manifest); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(dir, "CURRENT")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil); err == nil {
				t.Fatal("ambiguous bootstrap was adopted")
			}
			if _, err := os.Stat(filepath.Join(dir, "CURRENT")); !os.IsNotExist(err) {
				t.Fatalf("failed bootstrap published CURRENT: stat err=%v", err)
			}
		})
	}
}

func createWALV2EmptyRotatedActive(t *testing.T) (string, int64) {
	t.Helper()
	w, dir := newWALV2CommitTestWriter(t)
	if err := w.Append(walV2FixtureRecord(128, "committed-before-rotation")); err != nil {
		t.Fatal(err)
	}
	if err := w.v2.rotateLocked(); err != nil {
		t.Fatal(err)
	}
	headerBytes := w.v2.activeHeaderLen
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, headerBytes
}

func TestWALV2RecoversPartialHeaderForEmptyRotatedActive(t *testing.T) {
	for _, size := range []int64{0, 5, 14, 40, 100} {
		t.Run(strconv.FormatInt(size, 10), func(t *testing.T) {
			dir, headerBytes := createWALV2EmptyRotatedActive(t)
			if size >= headerBytes {
				t.Fatalf("fixture header too short: %d <= %d", headerBytes, size)
			}
			if err := os.Truncate(walV2SegmentPath(dir, 2, false), size); err != nil {
				t.Fatal(err)
			}
			var visited []Record
			recovered, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(record Record) error {
				visited = append(visited, record)
				return nil
			})
			if err != nil {
				t.Fatalf("reopen with %d-byte header: %v", size, err)
			}
			defer recovered.Close()
			if !recovered.RecoveredTail || recovered.Count() != 1 || len(visited) != 1 || visited[0].Detail != "committed-before-rotation" {
				t.Fatalf("partial header repair changed prefix: recovered=%v count=%d visited=%+v", recovered.RecoveredTail, recovered.Count(), visited)
			}
			if err := recovered.Append(walV2FixtureRecord(128, "after-repair")); err != nil {
				t.Fatalf("append after header repair: %v", err)
			}
		})
	}
}

func TestWALV2RejectsWrongPartialHeaderForEmptyRotatedActive(t *testing.T) {
	dir, _ := createWALV2EmptyRotatedActive(t)
	path := walV2SegmentPath(dir, 2, false)
	if err := os.Truncate(path, 8); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("BROKEN!!"), 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil); err == nil {
		t.Fatal("corrupt partial header was accepted")
	}
}

func TestWALV2SealedIntegrityBeforeAnyVisit(t *testing.T) {
	for _, badSegment := range []uint64{1, 2} {
		t.Run(walV2SegmentFileName(badSegment, true), func(t *testing.T) {
			w, dir := newWALV2CommitTestWriter(t)
			for _, detail := range []string{"first", "second", "third"} {
				if err := w.Append(walV2FixtureRecord(1<<20, detail)); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			manifest, exists, err := readWALV2Current(dir)
			if err != nil || !exists || len(manifest.Sealed) != 2 {
				t.Fatalf("expected two sealed segments: exists=%v manifest=%+v err=%v", exists, manifest, err)
			}
			meta := manifest.Sealed[badSegment-1]
			f, err := os.OpenFile(walV2SegmentPath(dir, badSegment, true), os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			footer, err := readWALV2Footer(f, meta.DataBytes)
			if err != nil {
				t.Fatal(err)
			}
			footer.Digest[0] ^= 0xff
			if _, err := f.WriteAt(encodeWALV2Footer(footer), meta.DataBytes); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()

			var visited int
			if _, err := openAndVerifyWALV2Sealed(dir, meta, manifest, func(Record) error {
				visited++
				return nil
			}); err == nil {
				t.Fatal("direct sealed verification accepted a corrupted footer digest")
			}
			if visited != 0 {
				t.Fatalf("direct sealed verification visited %d unverified records", visited)
			}
			if _, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(Record) error {
				visited++
				return nil
			}); err == nil {
				t.Fatal("sealed digest mismatch was accepted")
			}
			if visited != 0 {
				t.Fatalf("visitor observed %d records before sealed chain verification", visited)
			}
		})
	}
}
