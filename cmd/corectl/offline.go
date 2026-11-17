package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

type journalSummary struct {
	Path            string            `json:"path"`
	Version         int               `json:"wal_version"`
	Source          string            `json:"source,omitempty"`
	Identity        string            `json:"identity,omitempty"`
	JournalID       string            `json:"journal_id,omitempty"`
	Commitment      string            `json:"commitment,omitempty"`
	FilterVersion   string            `json:"filter_version,omitempty"`
	Records         uint64            `json:"records"`
	ValidBytes      int64             `json:"valid_bytes"`
	Updates         uint64            `json:"updates"`
	Markers         uint64            `json:"markers"`
	ProtobufChecked uint64            `json:"protobuf_checked"`
	Kinds           map[string]uint64 `json:"kinds"`
	FirstSlot       uint64            `json:"first_slot,omitempty"`
	LastSlot        uint64            `json:"last_slot,omitempty"`
	MinSlot         uint64            `json:"min_slot,omitempty"`
	MaxSlot         uint64            `json:"max_slot,omitempty"`
	TruncatedTail   bool              `json:"truncated_tail"`
	SourceLocalOnly bool              `json:"source_local_only"`
}

type journalVerifier struct {
	summary          journalSummary
	expectedSource   string
	expectedIdentity string
}

func (v *journalVerifier) observe(r lab.Record) error {
	if r.Type != "update" && r.Type != "marker" {
		return fmt.Errorf("record %d: unknown WAL record type %q", r.Sequence, r.Type)
	}
	if v.expectedSource != "" && r.Source != "" && r.Source != v.expectedSource {
		return fmt.Errorf("record %d: source %q differs from expected %q", r.Sequence, r.Source, v.expectedSource)
	}
	if v.expectedIdentity != "" && r.Identity != "" && r.Identity != v.expectedIdentity {
		return fmt.Errorf("record %d: identity differs from expected identity", r.Sequence)
	}
	if r.Type == "update" && (r.Source == "" || r.Identity == "" || r.Commitment == "" || r.FilterVersion == "") {
		return fmt.Errorf("record %d: update lacks source, identity, commitment or filter version", r.Sequence)
	}
	for _, field := range []struct{ label, current, next string }{
		{"source", v.summary.Source, r.Source},
		{"identity", v.summary.Identity, r.Identity},
		{"journal ID", v.summary.JournalID, r.JournalID},
		{"commitment", v.summary.Commitment, r.Commitment},
		{"filter version", v.summary.FilterVersion, r.FilterVersion},
	} {
		if field.current != "" && field.next != "" && field.current != field.next {
			return fmt.Errorf("record %d: %s changed inside one WAL", r.Sequence, field.label)
		}
	}
	if v.summary.Source == "" {
		v.summary.Source = r.Source
	}
	if v.summary.Identity == "" {
		v.summary.Identity = r.Identity
	}
	if v.summary.JournalID == "" {
		v.summary.JournalID = r.JournalID
	}
	if v.summary.Commitment == "" {
		v.summary.Commitment = r.Commitment
	}
	if v.summary.FilterVersion == "" {
		v.summary.FilterVersion = r.FilterVersion
	}
	if r.Type == "update" {
		if len(r.Raw) == 0 {
			return fmt.Errorf("record %d: update has no protobuf payload", r.Sequence)
		}
		var update pb.SubscribeUpdate
		if err := proto.Unmarshal(r.Raw, &update); err != nil {
			return fmt.Errorf("record %d: malformed Yellowstone protobuf: %w", r.Sequence, err)
		}
		// A newer provider may send a protobuf variant unknown to these
		// generated bindings. Preserve that valid unknown payload as "other".
		if kind := updateKind(&update); r.Kind != kind {
			return fmt.Errorf("record %d: envelope kind %q disagrees with protobuf %q", r.Sequence, r.Kind, kind)
		}
		v.summary.Updates++
		v.summary.ProtobufChecked++
	} else {
		v.summary.Markers++
	}
	v.summary.Kinds[r.Kind]++
	if r.Slot != 0 {
		if v.summary.FirstSlot == 0 {
			v.summary.FirstSlot = r.Slot
		}
		v.summary.LastSlot = r.Slot
		if v.summary.MinSlot == 0 || r.Slot < v.summary.MinSlot {
			v.summary.MinSlot = r.Slot
		}
		if r.Slot > v.summary.MaxSlot {
			v.summary.MaxSlot = r.Slot
		}
	}
	return nil
}

func updateKind(u *pb.SubscribeUpdate) string {
	switch {
	case u.GetTransaction() != nil:
		return "transaction"
	case u.GetTransactionStatus() != nil:
		return "transaction_status"
	case u.GetAccount() != nil:
		return "account"
	case u.GetSlot() != nil:
		return "slot"
	case u.GetBlockMeta() != nil:
		return "block_meta"
	case u.GetBlockFooter() != nil:
		return "block_footer"
	case u.GetPing() != nil:
		return "ping"
	case u.GetPong() != nil:
		return "pong"
	case u.GetEntry() != nil:
		return "entry"
	case u.GetBlock() != nil:
		return "block"
	default:
		return "other"
	}
}

func journalPath(journal, dataDir, source string) (string, int, error) {
	if journal != "" {
		stat, err := os.Stat(journal)
		if err != nil {
			return "", 0, err
		}
		if stat.IsDir() {
			return journal, 2, nil
		}
		return journal, 1, nil
	}
	if source == "" {
		return "", 0, errors.New("--source is required unless --journal is supplied")
	}
	root := filepath.Join(dataDir, source)
	v1 := filepath.Join(root, "journal.wal")
	v2 := filepath.Join(root, "journal.v2")
	_, e1 := os.Stat(v1)
	_, e2 := os.Stat(v2)
	if e1 == nil && e2 == nil {
		return "", 0, fmt.Errorf("both WAL versions exist for %s; choose one with --journal", source)
	}
	if e1 == nil {
		return v1, 1, nil
	}
	if e2 == nil {
		return v2, 2, nil
	}
	if !errors.Is(e1, os.ErrNotExist) {
		return "", 0, e1
	}
	if !errors.Is(e2, os.ErrNotExist) {
		return "", 0, e2
	}
	return "", 0, fmt.Errorf("no WAL at %s (checked journal.wal and journal.v2)", root)
}

func verifyJournal(path string, version int, expectedSource, expectedIdentity string, tolerant bool) (journalSummary, error) {
	v := journalVerifier{
		summary:        journalSummary{Path: path, Version: version, Kinds: make(map[string]uint64), SourceLocalOnly: true},
		expectedSource: expectedSource, expectedIdentity: expectedIdentity,
	}
	r, err := lab.OpenWALReader(path)
	if err != nil {
		return v.summary, err
	}
	defer r.Close()
	report, err := r.Read(tolerant, v.observe)
	if err != nil {
		return v.summary, err
	}
	v.summary.Records = report.Records
	v.summary.ValidBytes = report.ValidBytes
	v.summary.TruncatedTail = report.TruncatedTail
	if expectedSource != "" && v.summary.Source != "" && v.summary.Source != expectedSource {
		return v.summary, errors.New("WAL source does not match --source")
	}
	return v.summary, nil
}

func offline(command string, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	journal := fs.String("journal", "", "explicit journal.wal file or journal.v2 directory")
	dataDir := fs.String("data-dir", "./run", "WAL root directory")
	source := fs.String("source", "", "source ID (required with --data-dir)")
	identity := fs.String("identity", "", "require this subscription identity")
	tolerant := fs.Bool("tolerant-tail", false, "inspect/replay valid prefix of a torn v1 tail")
	after := fs.Uint64("after", 0, "replay records with sequence greater than this cursor")
	limit := fs.Int("limit", 0, "replay at most N emitted records (0 = unlimited)")
	kind := fs.String("kind", "", "replay only records of this kind")
	omitPayload := fs.Bool("omit-payload", false, "omit base64 protobuf payload from replay NDJSON")
	destinationRoot := fs.String("destination-data-dir", "", "migrate only: create a new v2 source directory under this separate data root")
	migrationSegmentMiB := fs.Int64("migration-segment-mib", 256, "migrate only: physical v2 segment size, 2..4096 MiB")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("unexpected %s arguments: %v", command, fs.Args())
	}
	if *limit < 0 {
		return errors.New("--limit cannot be negative")
	}
	if command == "verify" && *tolerant {
		return errors.New("verify is strict; --tolerant-tail is unavailable")
	}
	if command == "migrate" && *tolerant {
		return errors.New("migration preflight requires a fully intact WAL")
	}
	if command != "migrate" && *destinationRoot != "" {
		return errors.New("--destination-data-dir is only valid for migrate")
	}
	path, version, err := journalPath(*journal, *dataDir, *source)
	if err != nil {
		return err
	}
	if command == "migrate" && version != 1 {
		return errors.New("migration preflight expects a v1 journal.wal")
	}
	summary, err := verifyJournal(path, version, *source, *identity, *tolerant)
	if err != nil {
		return fmt.Errorf("%s: %w", command, err)
	}
	if command == "replay" {
		if *after > summary.Records {
			return fmt.Errorf("--after sequence %d exceeds WAL head %d", *after, summary.Records)
		}
		return replayRecords(path, *after, *limit, *kind, *omitPayload, *tolerant, stdout)
	}
	if command == "migrate" {
		if *destinationRoot != "" {
			if *source == "" {
				return errors.New("migrate --destination-data-dir requires --source")
			}
			if *migrationSegmentMiB < 2 || *migrationSegmentMiB > 4096 {
				return errors.New("--migration-segment-mib must be 2..4096")
			}
			report, err := lab.MigrateWALV1ToV2(context.Background(), path, *destinationRoot, *source, *identity, *migrationSegmentMiB<<20)
			if err != nil {
				return fmt.Errorf("migrate: %w", err)
			}
			return json.NewEncoder(stdout).Encode(struct {
				Status          string                 `json:"status"`
				WritesPerformed bool                   `json:"writes_performed"`
				Migration       lab.WALMigrationReport `json:"migration"`
				SourceJournal   journalSummary         `json:"source_journal"`
			}{"verified_copy_published", true, report, summary})
		}
		// With no destination, migrate remains a safe read-only preflight.
		if err := lab.CheckWALV1MigrationEnvelope(path); err != nil {
			return fmt.Errorf("migrate preflight: %w", err)
		}
		return json.NewEncoder(stdout).Encode(struct {
			Status             string         `json:"status"`
			WritesPerformed    bool           `json:"writes_performed"`
			MigrationAvailable bool           `json:"migration_available"`
			Reason             string         `json:"reason"`
			Journal            journalSummary `json:"journal"`
		}{"preflight_ok", false, true, "pass --destination-data-dir to publish a verified v2 copy under a new data root; old WAL and ACKs remain unchanged", summary})
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(summary)
}

func replayRecords(path string, after uint64, limit int, kind string, omitPayload, tolerant bool, dst io.Writer) error {
	r, err := lab.OpenWALReader(path)
	if err != nil {
		return err
	}
	defer r.Close()
	enc := json.NewEncoder(dst)
	var emitted int
	_, err = r.Read(tolerant, func(rec lab.Record) error {
		if rec.Sequence <= after || (kind != "" && rec.Kind != kind) || (limit > 0 && emitted >= limit) {
			return nil
		}
		if omitPayload {
			rec.Raw = nil
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
		emitted++
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
