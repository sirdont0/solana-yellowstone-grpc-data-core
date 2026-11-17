package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrateWALV1ToV2PreservesRecordsAndSourceArchive(t *testing.T) {
	c := recoveryConfig(t)
	c.SourceID = "aa"
	c.FilterVersion = "migration-v1"
	store, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		rec := Record{
			Type: "update", Kind: "other", Source: "aa", Identity: c.Identity(),
			Raw: make([]byte, 900<<10),
		}
		rec.Raw[0] = byte(i)
		if accepted, err := store.Append(rec); err != nil || !accepted {
			t.Fatalf("append %d accepted=%v err=%v", i, accepted, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	sourceWAL := filepath.Join(c.DataDir, "journal.wal")
	before, err := os.ReadFile(sourceWAL)
	if err != nil {
		t.Fatal(err)
	}
	var original []Record
	if err := ReadWAL(sourceWAL, func(r Record) error {
		original = append(original, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	targetRoot := filepath.Join(t.TempDir(), "new-root")
	report, err := MigrateWALV1ToV2(context.Background(), sourceWAL, targetRoot, "aa", c.Identity(), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != uint64(len(original)) || report.OldJournalID == report.NewJournalID || report.NewJournalID == "" || report.Identity != c.Identity() {
		t.Fatalf("migration report=%+v", report)
	}
	after, err := os.ReadFile(sourceWAL)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("migration modified original v1 WAL")
	}
	var migrated []Record
	if err := ReadWAL(filepath.Join(report.Target, "journal.v2"), func(r Record) error {
		migrated = append(migrated, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(migrated) != len(original) {
		t.Fatalf("record count changed: %d -> %d", len(original), len(migrated))
	}
	for i := range original {
		original[i].Identity = c.Identity()
		original[i].JournalID = ""
		migrated[i].JournalID = ""
		a, err := jsonBytes(original[i])
		if err != nil {
			t.Fatal(err)
		}
		b, err := jsonBytes(migrated[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("record %d differs after migration", i)
		}
	}
	// New configuration selects only v2 in the fresh source directory.
	newConfig := c
	newConfig.DataDir = report.Target
	newConfig.WALVersion = 2
	newConfig.WALSegmentBytes = 4 << 20
	reopened, err := OpenStore(newConfig)
	if err != nil {
		t.Fatalf("migrated v2 cannot be used by Store: %v", err)
	}
	if got := reopened.Snapshot().WALRecords; got <= report.Records {
		t.Fatalf("reopened v2 did not continue append sequence: %d", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationDoesNotPublishCorruptOrExistingDestination(t *testing.T) {
	c := recoveryConfig(t)
	c.SourceID = "aa"
	store, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	sourceWAL := filepath.Join(c.DataDir, "journal.wal")
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "aa"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := MigrateWALV1ToV2(context.Background(), sourceWAL, root, "aa", c.Identity(), 4<<20); err == nil {
		t.Fatal("overwrote existing destination")
	}
	if err := os.Remove(filepath.Join(root, "aa")); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(sourceWAL, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := MigrateWALV1ToV2(context.Background(), sourceWAL, root, "aa", c.Identity(), 4<<20); err == nil {
		t.Fatal("published torn source WAL")
	}
	if _, err := os.Stat(filepath.Join(root, "aa")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed migration exposed destination: %v", err)
	}
}

func jsonBytes(r Record) ([]byte, error) { return json.Marshal(r) }

func TestMigrationRejectsUnknownLegacyEnvelopeFields(t *testing.T) {
	const source, identity = "aa", "migration-identity"
	root := t.TempDir()
	sourceDir := filepath.Join(root, source)
	if err := os.MkdirAll(sourceDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(sourceDir, "journal.wal")
	record := Record{Type: "marker", Source: source, Identity: identity, Kind: "future_fixture"}
	known, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(known, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["future_field"] = json.RawMessage(`{"value":42}`)
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, len(walMagic)+8+len(body))
	copy(frame, walMagic)
	binary.LittleEndian.PutUint32(frame[len(walMagic):], uint32(len(body)))
	binary.LittleEndian.PutUint32(frame[len(walMagic)+4:], crc32.ChecksumIEEE(body))
	copy(frame[len(walMagic)+8:], body)
	if err := os.WriteFile(path, frame, 0600); err != nil {
		t.Fatal(err)
	}
	destinationRoot := filepath.Join(t.TempDir(), "converted")
	_, err = MigrateWALV1ToV2(context.Background(), path, destinationRoot, source, identity, 4<<20)
	if err == nil || !strings.Contains(err.Error(), "unsupported v1 JSON field") {
		t.Fatalf("lossy migration was accepted or reported ambiguously: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destinationRoot, source)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed migration published a target: %v", err)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, frame) {
		t.Fatalf("failed migration modified source: err=%v", err)
	}
}
