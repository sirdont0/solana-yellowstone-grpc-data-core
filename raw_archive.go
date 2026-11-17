package core

import (
	"context"
	"errors"
	"fmt"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

// ReadRawArchive verifies and walks a closed WAL without starting a network
// connection or requiring a previously running Engine. walPath is the v1
// journal.wal file or v2 journal.v2 directory. source is its logical source ID.
//
// The entire archive is verified in order, including records before `after`,
// so source identity, journal binding and prior continuity markers cannot be
// silently lost. The callback receives only records after `after.Sequence`.
// This is a read-only offline operation and is intentionally O(history).
func ReadRawArchive(ctx context.Context, walPath, source string, after RawCursor, visit func(RawObservation) error) error {
	if ctx == nil || visit == nil {
		return errors.New("core: non-nil context and archive callback required")
	}
	if source == "" || walPath == "" {
		return errors.New("core: archive WAL path and source ID required")
	}
	if after.Source != "" && after.Source != source {
		return ErrCursorExpired
	}
	reader, err := lab.OpenWALReader(walPath)
	if err != nil {
		return err
	}
	defer reader.Close()
	var identity, journalID string
	var lastSequence uint64
	// Preflight the entire immutable journal while holding the same shared
	// reader lock. No callback effects are delivered from a corrupt archive.
	_, err = reader.Read(false, func(rec lab.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rec.Sequence != lastSequence+1 {
			return fmt.Errorf("core: archive sequence mismatch at %d", rec.Sequence)
		}
		lastSequence = rec.Sequence
		if rec.Source != "" && rec.Source != source {
			return fmt.Errorf("core: archive contains an unexpected source %q", rec.Source)
		}
		if rec.Identity != "" {
			if identity != "" && identity != rec.Identity {
				return errors.New("core: archive contains multiple subscription identities")
			}
			identity = rec.Identity
		}
		if rec.JournalID != "" {
			if journalID != "" && journalID != rec.JournalID {
				return errors.New("core: archive contains multiple journal IDs")
			}
			journalID = rec.JournalID
		}
		return nil
	})
	if err != nil {
		return err
	}
	if after.Sequence > lastSequence {
		return fmt.Errorf("core: archive cursor sequence %d exceeds head %d", after.Sequence, lastSequence)
	}
	if (after.Sequence > 0 || after.Identity != "" || after.JournalID != "") && (after.Source != source || after.Identity != identity || after.JournalID != journalID) {
		return ErrCursorExpired
	}
	qualityBroken := false
	_, err = reader.Read(false, func(rec lab.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rawContinuityBreak(rec) {
			qualityBroken = true
		}
		if rec.Sequence <= after.Sequence {
			return nil
		}
		cursor := RawCursor{Source: source, Identity: identity, JournalID: journalID, Sequence: rec.Sequence, ConsumerEpoch: after.ConsumerEpoch}
		return visit(rawObservationFromRecord(rec, source, cursor, rawQuality(qualityBroken)))
	})
	return err
}
