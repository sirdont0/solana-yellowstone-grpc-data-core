package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWALV2GoldenFixtures freezes the byte-level writer contract independently
// from the reader. To intentionally regenerate the reviewed fixtures, run:
// UPDATE_WAL_V2_GOLDEN=1 go test ./internal/ingest -run '^TestWALV2GoldenFixtures$'
func TestWALV2GoldenFixtures(t *testing.T) {
	root := filepath.Join("testdata", "wal_v2_golden")
	artifacts := buildWALV2GoldenArtifacts(t)
	if os.Getenv("UPDATE_WAL_V2_GOLDEN") == "1" {
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
		for name, data := range artifacts {
			if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		return
	}

	for name, want := range artifacts {
		got, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read golden %s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("WAL v2 golden %s changed: got sha256=%s want sha256=%s; format changes require an explicit compatibility review and fixture update", name, sha256Hex(got), sha256Hex(want))
		}
	}

	// Exercise the independent readers against immutable fixture bytes.
	headerBytes := artifacts["header-v2.bin"]
	headerPath := filepath.Join(t.TempDir(), "header.bin")
	if err := os.WriteFile(headerPath, headerBytes, 0600); err != nil {
		t.Fatal(err)
	}
	headerFile, err := os.Open(headerPath)
	if err != nil {
		t.Fatal(err)
	}
	header, headerLen, err := readWALV2Header(headerFile)
	_ = headerFile.Close()
	if err != nil {
		t.Fatalf("decode golden header: %v", err)
	}
	if header.Source != "fixture-source" || header.Segment != 1 || header.JournalID != "00112233445566778899aabbccddeeff" || headerLen != int64(len(headerBytes)) {
		t.Fatalf("decoded golden header differs: %+v len=%d", header, headerLen)
	}

	frame := artifacts["frame-v2.bin"]
	if len(frame) < 8 {
		t.Fatal("golden frame is too short")
	}
	decoded, err := decodeWALV2Frame(frame[8:])
	if err != nil {
		t.Fatalf("decode golden frame: %v", err)
	}
	wantRaw := []byte{0xa0, 0x06, 0x01, 0x98, 0x06, 0x7f}
	if decoded.Sequence != 1 || decoded.Detail != "golden-fixture" || !bytes.Equal(decoded.Raw, wantRaw) {
		t.Fatalf("decoded golden frame differs: %+v raw=%x", decoded, decoded.Raw)
	}

	sealed := artifacts["sealed-segment-v2.bin"]
	sealedPath := filepath.Join(t.TempDir(), "sealed.seg")
	if err := os.WriteFile(sealedPath, sealed, 0600); err != nil {
		t.Fatal(err)
	}
	sealedFile, err := os.Open(sealedPath)
	if err != nil {
		t.Fatal(err)
	}
	footerOffset := int64(len(artifacts["active-segment-v2.bin"]))
	footer, footerErr := readWALV2Footer(sealedFile, footerOffset)
	sealedDigest, digestErr := hashWALV2Prefix(sealedFile, footerOffset)
	stat, statErr := sealedFile.Stat()
	_ = sealedFile.Close()
	if err := errors.Join(footerErr, digestErr, statErr); err != nil {
		t.Fatalf("read golden sealed footer: %v", err)
	}
	if stat.Size() != footerOffset+walV2FooterBytes || footer.Segment != 1 || footer.Records != 1 || footer.Digest != sealedDigest {
		t.Fatalf("golden footer mismatch: %+v size=%d", footer, stat.Size())
	}

	manifestDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(manifestDir, "manifests"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(manifestDir, "CURRENT"), artifacts["current-v2.txt"], 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(manifestDir, "manifests", walV2ManifestFileName(2)), artifacts["manifest-v2.json"], 0600); err != nil {
		t.Fatal(err)
	}
	manifest, exists, err := readWALV2Current(manifestDir)
	if err != nil || !exists {
		t.Fatalf("decode golden manifest exists=%v err=%v", exists, err)
	}
	if manifest.Generation != 2 || len(manifest.Sealed) != 1 || manifest.Active.ID != 2 {
		t.Fatalf("decoded golden manifest differs: %+v", manifest)
	}
}

func buildWALV2GoldenArtifacts(t *testing.T) map[string][]byte {
	t.Helper()
	const (
		source   = "fixture-source"
		identity = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		journal  = "00112233445566778899aabbccddeeff"
	)
	header := walV2Header{Version: walV2Version, Segment: 1, Source: source, Identity: identity, JournalID: journal, FirstSequence: 1}
	headerBytes, err := encodeWALV2Header(header)
	if err != nil {
		t.Fatal(err)
	}

	record := Record{
		Type: "update", Sequence: 1,
		Time:   time.Date(2026, 11, 19, 12, 34, 56, 789000000, time.UTC),
		Source: source, Kind: "transaction", Slot: 471461679, BankID: 42,
		BankIDPresent: true, Signature: "5abcFixtureSignature", TxIndex: 3,
		Commitment: "finalized", FilterVersion: "fixture-v1", Identity: identity,
		Detail: "golden-fixture", Raw: []byte{0xa0, 0x06, 0x01, 0x98, 0x06, 0x7f},
	}
	frameBytes, err := encodeWALV2Frame(record)
	if err != nil {
		t.Fatal(err)
	}
	activeSegment := append(append([]byte(nil), headerBytes...), frameBytes...)
	digest := sha256.Sum256(activeSegment)
	footerBytes := encodeWALV2Footer(walV2Footer{
		Segment: 1, FirstSequence: 1, LastSequence: 1, Records: 1,
		DataBytes: uint64(len(activeSegment)), Digest: digest,
	})
	sealedSegment := append(append([]byte(nil), activeSegment...), footerBytes...)

	activeHeader := walV2Header{Version: walV2Version, Segment: 2, Source: source, Identity: identity, JournalID: journal, FirstSequence: 2}
	activeHeaderBytes, err := encodeWALV2Header(activeHeader)
	if err != nil {
		t.Fatal(err)
	}
	manifest := walV2Manifest{
		Version: walV2Version, Generation: 2, Source: source, Identity: identity, JournalID: journal,
		EnvelopeSchemaVersion:   walV2EnvelopeSchemaVersion,
		GeyserDescriptorSHA256:  walV2GeyserDescriptorSHA256,
		StorageDescriptorSHA256: walV2StorageDescriptorSHA256,
		Sealed: []walV2SegmentMeta{{
			ID: 1, File: walV2SegmentFileName(1, true), FirstSequence: 1, LastSequence: 1,
			Records: 1, DataBytes: int64(len(activeSegment)), Digest: hex.EncodeToString(digest[:]),
		}},
		Active: walV2ActiveMeta{ID: 2, FirstSequence: 2, LastSequence: 1, CommittedBytes: int64(len(activeHeaderBytes))},
	}
	manifestDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(manifestDir, "manifests"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeWALV2Manifest(manifestDir, manifest); err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(manifestDir, "manifests", walV2ManifestFileName(2)))
	if err != nil {
		t.Fatal(err)
	}
	currentBytes, err := os.ReadFile(filepath.Join(manifestDir, "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}

	return map[string][]byte{
		"header-v2.bin":             headerBytes,
		"frame-v2.bin":              frameBytes,
		"active-segment-v2.bin":     activeSegment,
		"sealed-segment-v2.bin":     sealedSegment,
		"next-active-header-v2.bin": activeHeaderBytes,
		"manifest-v2.json":          manifestBytes,
		"current-v2.txt":            currentBytes,
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
