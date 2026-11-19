package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWALV2StrictFrameMetadataAndRawWirePreservation(t *testing.T) {
	metadata := []byte(`{"type":"update","time":"2026-11-19T12:00:00Z","future_field":true}`)
	body := make([]byte, walV2FramePrefixBytes+len(metadata))
	binary.LittleEndian.PutUint64(body[:8], 1)
	binary.LittleEndian.PutUint32(body[8:12], uint32(len(metadata)))
	copy(body[12:], metadata)
	binary.LittleEndian.PutUint32(body[12+len(metadata):16+len(metadata)], 0)
	if _, err := decodeWALV2Frame(body); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown frame metadata field error = %v", err)
	}

	// Unknown protobuf wire fields live in Raw and must survive the envelope
	// round trip byte-for-byte; WAL v2 does not decode or normalize this blob.
	raw := []byte{0xa0, 0x06, 0x01, 0x98, 0x06, 0x7f}
	frame, err := encodeWALV2Frame(Record{Type: "update", Sequence: 1, Raw: raw})
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeWALV2Frame(frame[8:])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(record.Raw, raw) {
		t.Fatalf("protobuf Raw changed: got %x want %x", record.Raw, raw)
	}
}

func TestWALV2StrictHeaderAndManifestJSON(t *testing.T) {
	t.Run("header", func(t *testing.T) {
		header := walV2Header{Version: walV2Version, Segment: 1, Source: "source-a", Identity: "identity-a", JournalID: "journal-strict", FirstSequence: 1}
		encoded, err := encodeWALV2Header(header)
		if err != nil {
			t.Fatal(err)
		}
		metadataLen := int(binary.LittleEndian.Uint32(encoded[10:14]))
		metadata := append([]byte(nil), encoded[14:14+metadataLen]...)
		metadata = append(metadata[:len(metadata)-1], []byte(`,"future_field":true}`)...)
		encoded, err = encodeWALV2HeaderMetadata(metadata)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "segment.open")
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if _, _, err := readWALV2Header(file); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("unknown header field error = %v", err)
		}
	})

	t.Run("manifest", func(t *testing.T) {
		w, dir := newWALV2CommitTestWriter(t)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		manifest, exists, err := readWALV2Current(dir)
		if err != nil || !exists {
			t.Fatalf("read manifest exists=%v err=%v", exists, err)
		}
		path := filepath.Join(dir, "manifests", walV2ManifestFileName(manifest.Generation))
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		position := bytes.LastIndexByte(encoded, '}')
		if position < 0 {
			t.Fatal("manifest JSON has no closing brace")
		}
		encoded = append(append([]byte(nil), encoded[:position]...), []byte(`,"future_field":true}`)...)
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readWALV2ManifestGeneration(dir, manifest.Generation); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("unknown manifest field error = %v", err)
		}
	})
}

func TestWALV2RejectsUnsupportedPhysicalAndManifestVersions(t *testing.T) {
	t.Run("binary-segment-version", func(t *testing.T) {
		header := walV2Header{Version: walV2Version, Segment: 1, Source: "source-a", Identity: "identity-a", JournalID: "journal-version", FirstSequence: 1}
		encoded, err := encodeWALV2Header(header)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint16(encoded[8:10], walV2Version+1)
		path := filepath.Join(t.TempDir(), "segment.open")
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if _, _, err := readWALV2Header(file); err == nil || !strings.Contains(err.Error(), "unsupported WAL v2 segment format version") {
			t.Fatalf("unsupported binary segment version error = %v", err)
		}
	})

	t.Run("json-header-version", func(t *testing.T) {
		header := walV2Header{Version: walV2Version, Segment: 1, Source: "source-a", Identity: "identity-a", JournalID: "journal-version", FirstSequence: 1}
		encoded, err := encodeWALV2Header(header)
		if err != nil {
			t.Fatal(err)
		}
		metadataLen := int(binary.LittleEndian.Uint32(encoded[10:14]))
		var decoded walV2Header
		if err := json.Unmarshal(encoded[14:14+metadataLen], &decoded); err != nil {
			t.Fatal(err)
		}
		decoded.Version++
		metadata, err := json.Marshal(decoded)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err = encodeWALV2HeaderMetadata(metadata)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "segment.open")
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if _, _, err := readWALV2Header(file); err == nil || !strings.Contains(err.Error(), "unsupported WAL v2 JSON header version") {
			t.Fatalf("unsupported JSON header version error = %v", err)
		}
	})

	t.Run("manifest-version", func(t *testing.T) {
		w, dir := newWALV2CommitTestWriter(t)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		manifest, exists, err := readWALV2Current(dir)
		if err != nil || !exists {
			t.Fatalf("read manifest exists=%v err=%v", exists, err)
		}
		manifest.Version++
		writeWALV2ManifestWithInvalidBinding(t, dir, manifest)
		if _, err := readWALV2ManifestGeneration(dir, manifest.Generation); err == nil || !strings.Contains(err.Error(), "unsupported WAL v2 manifest version") {
			t.Fatalf("unsupported manifest version error = %v", err)
		}
	})

	t.Run("current-missing-generation", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "manifests"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("99\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readWALV2Current(dir); err == nil {
			t.Fatal("CURRENT pointing to a missing manifest generation was accepted")
		}
	})
}

func TestWALV2NewWriterEmitsEnvelopeAndDescriptorBinding(t *testing.T) {
	w, dir := newWALV2CommitTestWriter(t)
	defer w.Close()

	manifest, exists, err := readWALV2Current(dir)
	if err != nil || !exists {
		t.Fatalf("read new manifest exists=%v err=%v", exists, err)
	}
	if manifest.Version != walV2Version || manifest.EnvelopeSchemaVersion != walV2EnvelopeSchemaVersion ||
		manifest.GeyserDescriptorSHA256 != walV2GeyserDescriptorSHA256 || manifest.StorageDescriptorSHA256 != walV2StorageDescriptorSHA256 {
		t.Fatalf("new manifest binding = physical %d envelope %d geyser %s storage %s", manifest.Version, manifest.EnvelopeSchemaVersion, manifest.GeyserDescriptorSHA256, manifest.StorageDescriptorSHA256)
	}

	file, err := os.Open(walV2SegmentPath(dir, manifest.Active.ID, false))
	if err != nil {
		t.Fatal(err)
	}
	header, _, headerErr := readWALV2Header(file)
	_ = file.Close()
	if headerErr != nil {
		t.Fatal(headerErr)
	}
	if header.Version != walV2Version || header.EnvelopeSchemaVersion != walV2EnvelopeSchemaVersion ||
		header.GeyserDescriptorSHA256 != walV2GeyserDescriptorSHA256 || header.StorageDescriptorSHA256 != walV2StorageDescriptorSHA256 {
		t.Fatalf("new header binding = physical %d envelope %d geyser %s storage %s", header.Version, header.EnvelopeSchemaVersion, header.GeyserDescriptorSHA256, header.StorageDescriptorSHA256)
	}
}

func TestWALV2RejectsUnsupportedEnvelopeSchemaAndDescriptorMismatchBeforeReplay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*walV2Manifest)
		want   string
	}{
		{
			name: "unsupported-envelope-schema",
			mutate: func(manifest *walV2Manifest) {
				manifest.EnvelopeSchemaVersion++
			},
			want: "unsupported WAL v2 envelope schema version",
		},
		{
			name: "geyser-descriptor-mismatch",
			mutate: func(manifest *walV2Manifest) {
				manifest.GeyserDescriptorSHA256 = strings.Repeat("0", 64)
			},
			want: "WAL v2 protobuf descriptor mismatch",
		},
		{
			name: "storage-descriptor-mismatch",
			mutate: func(manifest *walV2Manifest) {
				manifest.StorageDescriptorSHA256 = strings.Repeat("0", 64)
			},
			want: "WAL v2 protobuf descriptor mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, dir := newWALV2CommitTestWriter(t)
			if err := w.Append(walV2FixtureRecord(32, "must-not-replay")); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			manifest, exists, err := readWALV2Current(dir)
			if err != nil || !exists {
				t.Fatalf("read manifest exists=%v err=%v", exists, err)
			}
			tc.mutate(&manifest)
			writeWALV2ManifestWithInvalidBinding(t, dir, manifest)
			visited := 0
			if _, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(Record) error {
				visited++
				return nil
			}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("invalid envelope open error = %v", err)
			}
			if visited != 0 {
				t.Fatalf("invalid envelope delivered %d records before rejection", visited)
			}
		})
	}
}

func TestWALV2RejectsSegmentHeaderDescriptorMismatchBeforeReplay(t *testing.T) {
	w, dir := newWALV2CommitTestWriter(t)
	if err := w.Append(walV2FixtureRecord(32, "must-not-replay")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, exists, err := readWALV2Current(dir)
	if err != nil || !exists {
		t.Fatalf("read manifest exists=%v err=%v", exists, err)
	}
	path := walV2SegmentPath(dir, manifest.Active.ID, false)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	header, _, err := readWALV2Header(file)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	header.GeyserDescriptorSHA256 = strings.Repeat("f", 64)
	metadata, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeWALV2HeaderMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	visited := 0
	if _, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(Record) error {
		visited++
		return nil
	}); err == nil || !strings.Contains(err.Error(), "WAL v2 protobuf descriptor mismatch") {
		t.Fatalf("mismatched segment header error = %v", err)
	}
	if visited != 0 {
		t.Fatalf("mismatched segment header delivered %d records", visited)
	}
}

func TestWALV2ReopensLegacyPreFreezeJournalAndUpgradesOnAppend(t *testing.T) {
	dir := createLegacyWALV2Fixture(t, true)
	legacyHeaderPath := walV2SegmentPath(dir, 1, false)
	legacyHeader, err := os.ReadFile(legacyHeaderPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(legacyHeader, []byte("envelope_schema_version")) {
		t.Fatal("legacy fixture unexpectedly contains an envelope schema marker")
	}

	var replayed []Record
	w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(record Record) error {
		replayed = append(replayed, record)
		return nil
	})
	if err != nil {
		t.Fatalf("open legacy pre-freeze journal: %v", err)
	}
	if w.Count() != 1 || len(replayed) != 1 || replayed[0].Detail != "legacy-record" {
		t.Fatalf("legacy replay count=%d records=%+v", w.Count(), replayed)
	}
	if err := w.Append(walV2FixtureRecord(24, "new-record")); err != nil {
		t.Fatalf("append after legacy reopen: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	manifest, exists, err := readWALV2Current(dir)
	if err != nil || !exists {
		t.Fatalf("read upgraded manifest exists=%v err=%v", exists, err)
	}
	if manifest.EnvelopeSchemaVersion != walV2EnvelopeSchemaVersion || manifest.GeyserDescriptorSHA256 != walV2GeyserDescriptorSHA256 || manifest.StorageDescriptorSHA256 != walV2StorageDescriptorSHA256 {
		t.Fatalf("upgraded manifest binding = version %d geyser %s storage %s", manifest.EnvelopeSchemaVersion, manifest.GeyserDescriptorSHA256, manifest.StorageDescriptorSHA256)
	}

	var afterUpgrade []Record
	reopened, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(record Record) error {
		afterUpgrade = append(afterUpgrade, record)
		return nil
	})
	if err != nil {
		t.Fatalf("reopen upgraded legacy journal: %v", err)
	}
	defer reopened.Close()
	if reopened.Count() != 2 || len(afterUpgrade) != 2 || afterUpgrade[0].Detail != "legacy-record" || afterUpgrade[1].Detail != "new-record" {
		t.Fatalf("upgraded legacy replay count=%d records=%+v", reopened.Count(), afterUpgrade)
	}
}

func TestWALV2LegacyHeaderOnlyBootstrapAndOldEmptyHeaderLength(t *testing.T) {
	t.Run("header-only-bootstrap", func(t *testing.T) {
		dir := createLegacyWALV2Fixture(t, false)
		if err := os.Remove(filepath.Join(dir, "CURRENT")); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(dir, "manifests", walV2ManifestFileName(1))); err != nil {
			t.Fatal(err)
		}
		w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
		if err != nil {
			t.Fatalf("recover legacy header-only bootstrap: %v", err)
		}
		if w.Count() != 0 {
			t.Fatalf("legacy header-only bootstrap count=%d", w.Count())
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("repair-partial-legacy-empty-header", func(t *testing.T) {
		dir := createLegacyWALV2Fixture(t, false)
		path := walV2SegmentPath(dir, 1, false)
		if err := os.Truncate(path, 12); err != nil {
			t.Fatal(err)
		}
		w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
		if err != nil {
			t.Fatalf("repair partial legacy empty header: %v", err)
		}
		if !w.RecoveredTail || w.Count() != 0 {
			t.Fatalf("legacy header recovery flag/count = %v/%d", w.RecoveredTail, w.Count())
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("recreate-missing-legacy-empty-header", func(t *testing.T) {
		dir := createLegacyWALV2Fixture(t, false)
		if err := os.Remove(walV2SegmentPath(dir, 1, false)); err != nil {
			t.Fatal(err)
		}
		w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
		if err != nil {
			t.Fatalf("recreate missing legacy empty header: %v", err)
		}
		if w.Count() != 0 {
			t.Fatalf("recreated legacy empty header count=%d", w.Count())
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func createLegacyWALV2Fixture(t *testing.T, withRecord bool) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "legacy-journal.v2")
	if err := os.MkdirAll(filepath.Join(dir, "segments"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "manifests"), 0700); err != nil {
		t.Fatal(err)
	}
	header := walV2Header{Version: walV2Version, Segment: 1, Source: "source-a", Identity: "identity-a", JournalID: "legacy-journal", FirstSequence: 1}
	headerBytes, err := encodeWALV2LegacyHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	manifest := walV2Manifest{
		Version: walV2Version, Generation: 1, Source: header.Source, Identity: header.Identity, JournalID: header.JournalID,
		Active: walV2ActiveMeta{ID: 1, FirstSequence: 1, CommittedBytes: int64(len(headerBytes))},
	}
	contents := append([]byte(nil), headerBytes...)
	if withRecord {
		frames, records, err := encodeWALV2Batch([]Record{walV2FixtureRecord(16, "legacy-record")}, manifest)
		if err != nil {
			t.Fatal(err)
		}
		contents = append(contents, frames[0]...)
		manifest.Active.Records = 1
		manifest.Active.LastSequence = records[0].Sequence
		manifest.Active.CommittedBytes += int64(len(frames[0]))
	}
	if err := os.WriteFile(walV2SegmentPath(dir, 1, false), contents, 0600); err != nil {
		t.Fatal(err)
	}
	writeLegacyWALV2Manifest(t, dir, manifest)
	return dir
}

func writeLegacyWALV2Manifest(t *testing.T, dir string, manifest walV2Manifest) {
	t.Helper()
	manifest.Checksum = ""
	canonical, err := json.Marshal(manifest) // New envelope fields are omitempty and remain absent.
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	manifest.Checksum = hex.EncodeToString(digest[:])
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifests", walV2ManifestFileName(manifest.Generation)), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("1\n"), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeWALV2ManifestWithInvalidBinding(t *testing.T, dir string, manifest walV2Manifest) {
	t.Helper()
	manifest.Checksum = ""
	canonical, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	manifest.Checksum = hex.EncodeToString(digest[:])
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifests", walV2ManifestFileName(manifest.Generation)), encoded, 0600); err != nil {
		t.Fatal(err)
	}
}
