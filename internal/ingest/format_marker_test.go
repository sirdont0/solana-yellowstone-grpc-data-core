package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func formatMarkerTestConfig(t *testing.T, version int) Config {
	t.Helper()
	c := recoveryConfig(t)
	c.SourceID = "aa"
	c.WALVersion = version
	return c
}

func readTestWALRecords(t *testing.T, path string) []Record {
	t.Helper()
	var records []Record
	if err := ReadWAL(path, func(record Record) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return records
}

func TestWALFormatMarkerCreatedForV1AndV2(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "v1", 2: "v2"}[version], func(t *testing.T) {
			c := formatMarkerTestConfig(t, version)
			s, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			journalID := s.Snapshot().JournalID
			marker, exists, err := readWALFormatMarker(c.DataDir)
			if err != nil || !exists {
				t.Fatalf("marker exists=%v err=%v", exists, err)
			}
			if marker.SchemaVersion != walFormatMarkerSchemaVersion || marker.WALVersion != version || marker.SourceID != c.SourceID || marker.Identity != c.Identity() || marker.JournalID != journalID {
				t.Fatalf("marker does not bind the opened journal: marker=%+v snapshot journal=%s", marker, journalID)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	// WALVersion=0 is the documented legacy-v1 default and is recorded as v1.
	t.Run("zero-defaults-to-v1", func(t *testing.T) {
		c := formatMarkerTestConfig(t, 0)
		s, err := OpenStore(c)
		if err != nil {
			t.Fatal(err)
		}
		marker, exists, err := readWALFormatMarker(c.DataDir)
		if err != nil || !exists || marker.WALVersion != 1 {
			t.Fatalf("zero WALVersion marker=%+v exists=%v err=%v", marker, exists, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWALFormatMarkerRejectsFormatAndIdentityChanges(t *testing.T) {
	c := formatMarkerTestConfig(t, 1)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	otherFormat := c
	otherFormat.WALVersion = 2
	if opened, err := OpenStore(otherFormat); err == nil {
		_ = opened.Abort()
		t.Fatal("opening a marked v1 source as v2 succeeded")
	}
	if _, err := os.Lstat(filepath.Join(c.DataDir, "journal.v2")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("format conflict created alternate journal: %v", err)
	}

	changedIdentity := c
	changedIdentity.FilterVersion += "-changed"
	if opened, err := OpenStore(changedIdentity); err == nil {
		_ = opened.Abort()
		t.Fatal("opening a marked source with a changed identity succeeded")
	}
}

func TestWALFormatMarkerRejectsMissingMarkedJournalBeforeCreatingAlternate(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "v1", 2: "v2"}[version], func(t *testing.T) {
			c := formatMarkerTestConfig(t, version)
			s, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			selected := filepath.Join(c.DataDir, "journal.wal")
			alternate := filepath.Join(c.DataDir, "journal.v2")
			if version == 2 {
				selected, alternate = alternate, selected
			}
			if version == 2 {
				if err := os.RemoveAll(selected); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(selected); err != nil {
				t.Fatal(err)
			}
			if opened, err := OpenStore(c); err == nil {
				_ = opened.Abort()
				t.Fatal("opening a source with a missing marked journal succeeded")
			}
			if _, err := os.Lstat(alternate); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("missing marked journal created alternate format: %v", err)
			}
		})
	}
}

func TestWALFormatMarkerCorruptionFailsClosed(t *testing.T) {
	c := formatMarkerTestConfig(t, 1)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.DataDir, walFormatMarkerName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var marker walFormatMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatal(err)
	}
	checksumPrefix := "0"
	if marker.Checksum[0] == '0' {
		checksumPrefix = "1"
	}
	marker.Checksum = checksumPrefix + marker.Checksum[1:]
	corrupt, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	corrupt = append(corrupt, '\n')
	if err := os.WriteFile(path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if opened, err := OpenStore(c); err == nil {
		_ = opened.Abort()
		t.Fatal("corrupt WAL format marker was accepted")
	}
}

func TestLegacyJournalWithoutMarkerIsAdoptedAfterReplay(t *testing.T) {
	c := formatMarkerTestConfig(t, 1)
	path := filepath.Join(c.DataDir, "journal.wal")
	w, err := OpenWAL(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: "marker", Kind: "legacy_fixture", Source: c.SourceID, Identity: c.Identity()}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := readWALFormatMarker(c.DataDir); err != nil || exists {
		t.Fatalf("legacy fixture unexpectedly has marker: exists=%v err=%v", exists, err)
	}

	s, err := OpenStore(c)
	if err != nil {
		t.Fatalf("legacy journal adoption failed: %v", err)
	}
	marker, exists, err := readWALFormatMarker(c.DataDir)
	if err != nil || !exists {
		t.Fatalf("adopted marker exists=%v err=%v", exists, err)
	}
	if marker.JournalID != s.Snapshot().JournalID || marker.SourceID != c.SourceID || marker.Identity != c.Identity() {
		t.Fatalf("adopted marker=%+v snapshot=%+v", marker, s.Snapshot())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALFormatMarkerJournalIDMismatchFailsBeforeSessionMarkers(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "v1", 2: "v2"}[version], func(t *testing.T) {
			c := formatMarkerTestConfig(t, version)
			s, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			journalPath := filepath.Join(c.DataDir, "journal.wal")
			if version == 2 {
				journalPath = filepath.Join(c.DataDir, "journal.v2")
			}
			before := readTestWALRecords(t, journalPath)
			wrongID, err := newJournalID()
			if err != nil {
				t.Fatal(err)
			}
			wrongMarker := newWALFormatMarker(version, c.SourceID, c.Identity(), wrongID)
			encoded, err := marshalWALFormatMarker(wrongMarker)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(c.DataDir, walFormatMarkerName), encoded, 0600); err != nil {
				t.Fatal(err)
			}
			if opened, err := OpenStore(c); err == nil {
				_ = opened.Abort()
				t.Fatal("journal ID mismatch was accepted")
			}
			after := readTestWALRecords(t, journalPath)
			if len(after) != len(before) {
				t.Fatalf("journal ID mismatch appended records: before=%d after=%d", len(before), len(after))
			}
			for i := range before {
				if before[i].Sequence != after[i].Sequence || before[i].Kind != after[i].Kind || before[i].JournalID != after[i].JournalID {
					t.Fatalf("journal changed at record %d: before=%+v after=%+v", i, before[i], after[i])
				}
			}
		})
	}
}

func TestWALFormatMarkerRejectsNonCanonicalAndUnknownFields(t *testing.T) {
	c := formatMarkerTestConfig(t, 1)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.DataDir, walFormatMarkerName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte("\n}"), []byte(",\n  \"future_field\": true\n}"), 1)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readWALFormatMarker(c.DataDir); err == nil {
		t.Fatal("unknown marker field was accepted")
	}
}

func TestWALFormatMarkerRequiresIdentityAndJournalBinding(t *testing.T) {
	base := newWALFormatMarker(1, "source-a", formatMarkerTestConfig(t, 1).Identity(), "00112233445566778899aabbccddeeff")
	for _, tc := range []struct {
		name   string
		mutate func(*walFormatMarker)
	}{
		{name: "identity", mutate: func(marker *walFormatMarker) { marker.Identity = "" }},
		{name: "journal", mutate: func(marker *walFormatMarker) { marker.JournalID = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marker := base
			tc.mutate(&marker)
			marker.Checksum = walFormatMarkerChecksum(marker)
			if err := validateWALFormatMarker(marker); err == nil {
				t.Fatal("marker without an identity or journal binding was accepted")
			}
		})
	}
}

func TestWALV1MigrationPublishesV2FormatMarker(t *testing.T) {
	c := formatMarkerTestConfig(t, 1)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	targetRoot := filepath.Join(t.TempDir(), "migrated")
	report, err := MigrateWALV1ToV2(context.Background(), filepath.Join(c.DataDir, "journal.wal"), targetRoot, c.SourceID, c.Identity(), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	marker, exists, err := readWALFormatMarker(report.Target)
	if err != nil || !exists {
		t.Fatalf("migrated marker exists=%v err=%v", exists, err)
	}
	if marker.WALVersion != 2 || marker.SourceID != c.SourceID || marker.Identity != c.Identity() || marker.JournalID != report.NewJournalID {
		t.Fatalf("migrated marker=%+v report=%+v", marker, report)
	}
}
