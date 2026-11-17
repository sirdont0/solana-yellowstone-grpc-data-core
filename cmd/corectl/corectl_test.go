package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
)

func fixtureWAL(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "journal.wal")
	w, err := lab.OpenWAL(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := lab.Config{SourceID: "alpha", Endpoint: "example.com:443", Commitment: "finalized", FilterVersion: "test-v1"}
	update := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Ping{Ping: &pb.SubscribeUpdatePing{}}}
	r, err := lab.FromUpdate(cfg, "alpha", 1, update)
	if err != nil {
		t.Fatal(err)
	}
	r.JournalID = "test-journal"
	marker := lab.Marker(cfg, "alpha", 1, "session_end", "fixture")
	marker.JournalID = "test-journal"
	if err := w.AppendBatch([]lab.Record{r, marker}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestVerifyAndReplayClosedWAL(t *testing.T) {
	path := fixtureWAL(t)
	summary, err := verifyJournal(path, 1, "alpha", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Records != 2 || summary.Updates != 1 || summary.Markers != 1 || summary.Kinds["ping"] != 1 || summary.JournalID != "test-journal" {
		t.Fatalf("unexpected verification summary: %+v", summary)
	}
	var output bytes.Buffer
	if err := replayRecords(path, 0, 0, "", false, false, &output); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 NDJSON records, got %d", len(lines))
	}
	var first lab.Record
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || len(first.Raw) == 0 {
		t.Fatalf("first record not faithful: %+v", first)
	}
	output.Reset()
	if err := replayRecords(path, 1, 1, "session_end", true, false, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Count(strings.TrimSpace(output.String()), "\n") != 0 || !strings.Contains(output.String(), `"sequence":2`) {
		t.Fatalf("filtered replay mismatch: %s", output.String())
	}
	if _, err := verifyJournal(path, 1, "wrong", "", false); err == nil {
		t.Fatal("expected source mismatch")
	}
	if _, err := verifyJournal(path, 1, "alpha", "wrong-identity", false); err == nil {
		t.Fatal("expected identity mismatch")
	}
}

func TestStrictCorruptionAndTornTail(t *testing.T) {
	path := fixtureWAL(t)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := verifyJournal(path, 1, "alpha", "", false); err == nil {
		t.Fatal("strict verification accepted a torn tail")
	}
	summary, err := verifyJournal(path, 1, "alpha", "", true)
	if err != nil || !summary.TruncatedTail || summary.Records != 2 {
		t.Fatalf("tolerant valid-prefix scan: %+v, %v", summary, err)
	}
	// A fully written frame with a bad checksum must fail even in tolerant mode.
	path = fixtureWAL(t)
	f, err = os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("X"), 25); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := verifyJournal(path, 1, "alpha", "", true); err == nil {
		t.Fatal("tolerant scan accepted corrupt frame")
	}
}

func TestCaptureConfigIsProgramAgnostic(t *testing.T) {
	f, err := buildCaptureConfig(captureFile{Sources: []sourceFile{{
		ID: "alpha", Endpoint: "example.com:443", Programs: []string{"11111111111111111111111111111111"},
	}}}, ".")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.NewRaw(f); err != nil {
		t.Fatal(err)
	}
	sub := f.Sources[0].Subscription
	if sub == nil || len(sub.Transactions) != 1 || sub.Transactions["program_transactions"].GetVote() || sub.Transactions["program_transactions"].GetFailed() {
		t.Fatalf("unexpected subscription: %+v", sub)
	}
	if sub.Transactions["program_transactions"].Vote != nil || sub.Transactions["program_transactions"].Failed != nil {
		t.Fatal("program shortcut must leave vote/failed tri-state unset")
	}
	if f.Sources[0].Programs != nil {
		t.Fatal("legacy program filters should remain unused")
	}
}

func TestCaptureLoadsArbitrarySubscriptionJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subscription.json")
	const source = `{"transactions":{"selected_transactions":{"accountInclude":["11111111111111111111111111111111"]}},"slots":{"slot_updates":{}},"blocksMeta":{"block_metadata":{}}}`
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := buildCaptureConfig(captureFile{Sources: []sourceFile{{
		ID: "beta", Endpoint: "example.com:443", SubscriptionFile: "subscription.json",
	}}}, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.NewRaw(cfg); err != nil {
		t.Fatal(err)
	}
	s := cfg.Sources[0].Subscription
	if len(s.Transactions) != 1 || len(s.Slots) != 1 || len(s.BlocksMeta) != 1 {
		t.Fatalf("subscription filters lost: %+v", s)
	}
}

func TestCLIInspect(t *testing.T) {
	path := fixtureWAL(t)
	var out, stderr bytes.Buffer
	if err := run([]string{"inspect", "--journal", path, "--source", "alpha"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"records": 2`) || !strings.Contains(out.String(), `"protobuf_checked": 1`) {
		t.Fatalf("inspect output: %s", out.String())
	}
}

func TestCLIHelpExitsSuccessfully(t *testing.T) {
	for _, command := range []string{"capture", "inspect", "verify", "replay", "migrate"} {
		var out, stderr bytes.Buffer
		if err := run([]string{command, "-h"}, &out, &stderr); err != nil {
			t.Fatalf("%s -h: %v", command, err)
		}
		if !strings.Contains(stderr.String(), "Usage of "+command) {
			t.Fatalf("%s -h did not print usage: %s", command, stderr.String())
		}
	}
}

func TestMigrationPreflightDoesNotWrite(t *testing.T) {
	path := fixtureWAL(t)
	var out, stderr bytes.Buffer
	if err := run([]string{"migrate", "--journal", path, "--source", "alpha"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Status             string `json:"status"`
		WritesPerformed    bool   `json:"writes_performed"`
		MigrationAvailable bool   `json:"migration_available"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "preflight_ok" || result.WritesPerformed || !result.MigrationAvailable {
		t.Fatalf("unsafe migration result: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(path), "journal.v2")); !os.IsNotExist(err) {
		t.Fatalf("migration preflight created v2 data: %v", err)
	}
}

func TestMigrationPreflightRejectsUnknownV1EnvelopeFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.wal")
	body := []byte(`{"type":"marker","kind":"legacy","source":"alpha","identity":"test-identity","future_field":{"value":42}}`)
	frame := make([]byte, len("YLABWAL1")+8+len(body))
	copy(frame, "YLABWAL1")
	binary.LittleEndian.PutUint32(frame[len("YLABWAL1"):], uint32(len(body)))
	binary.LittleEndian.PutUint32(frame[len("YLABWAL1")+4:], crc32.ChecksumIEEE(body))
	copy(frame[len("YLABWAL1")+8:], body)
	if err := os.WriteFile(path, frame, 0600); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	err := run([]string{"migrate", "--journal", path, "--source", "alpha"}, &out, &stderr)
	if err == nil || !strings.Contains(err.Error(), "unsupported v1 JSON field") {
		t.Fatalf("lossy migration preflight accepted or misreported: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("failed preflight emitted success output: %s", out.String())
	}
}

func TestMigrationPublishesVerifiedCopyIntoSeparateRoot(t *testing.T) {
	path := fixtureWAL(t)
	sourceBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	destinationRoot := filepath.Join(t.TempDir(), "converted")
	var out, stderr bytes.Buffer
	if err := run([]string{
		"migrate", "--journal", path, "--source", "alpha",
		"--destination-data-dir", destinationRoot,
		"--migration-segment-mib", "4",
	}, &out, &stderr); err != nil {
		t.Fatalf("migration failed: %v; stderr=%s", err, stderr.String())
	}
	var result struct {
		Status          string                 `json:"status"`
		WritesPerformed bool                   `json:"writes_performed"`
		Migration       lab.WALMigrationReport `json:"migration"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "verified_copy_published" || !result.WritesPerformed || result.Migration.Records != 2 {
		t.Fatalf("unexpected migration output: %+v", result)
	}
	newWAL := filepath.Join(destinationRoot, "alpha", "journal.v2")
	if _, err := verifyJournal(newWAL, 2, "alpha", result.Migration.Identity, false); err != nil {
		t.Fatalf("target verification: %v", err)
	}
	sourceAfter, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sourceBefore, sourceAfter) {
		t.Fatal("old WAL changed")
	}
	out.Reset()
	if err := run([]string{
		"migrate", "--journal", path, "--source", "alpha",
		"--destination-data-dir", destinationRoot,
	}, &out, &stderr); err == nil {
		t.Fatal("migration overwrote an existing target")
	}
}

func TestCaptureRejectsUnknownConfigKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rawcore.json")
	if err := os.WriteFile(path, []byte(`{"sources":[],"token":"secret-in-config"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if err := run([]string{"capture", "--config", path, "--dry-run"}, &out, &stderr); err == nil {
		t.Fatal("secret-bearing unknown config key must fail closed")
	}
}

func TestInspectV2ClosedJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.v2")
	cfg := lab.Config{SourceID: "beta", Endpoint: "example.com:443", Commitment: "finalized", FilterVersion: "test-v1"}
	w, err := lab.OpenWALV2(path, "beta", cfg.Identity(), 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	update := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Ping{Ping: &pb.SubscribeUpdatePing{}}}
	record, err := lab.FromUpdate(cfg, "beta", 1, update)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(record); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	summary, err := verifyJournal(path, 2, "beta", cfg.Identity(), false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Records != 1 || summary.Updates != 1 || summary.Kinds["ping"] != 1 {
		t.Fatalf("unexpected v2 summary: %+v", summary)
	}
}
