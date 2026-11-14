package core

import (
	"context"
	"errors"
	"fmt"
	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	"path/filepath"
)

// ReplayOptions keeps strict CRC/identity verification in both modes.
type ReplayOptions struct {
	// TolerantTail replays a valid prefix without modifying the file. All events
	// from a torn source are marked CONTINUITY_BROKEN before the first delivery.
	TolerantTail bool
}

// Replay preserves source-local order, not the original cross-source order.
// It preflights every source before invoking handlers and rejects active writers.
func (e *Engine) Replay(ctx context.Context) error { return e.ReplayWithOptions(ctx, ReplayOptions{}) }
func (e *Engine) ReplayWithOptions(ctx context.Context, opts ReplayOptions) error {
	if err := e.begin(); err != nil {
		return err
	}
	defer e.running.Store(false)
	if err := ctx.Err(); err != nil {
		return err
	}
	type sourceReader struct {
		source string
		reader *lab.WALReader
		report lab.WALReadReport
	}
	readers := make([]sourceReader, 0, len(e.cfg.Sources))
	defer func() {
		for _, r := range readers {
			_ = r.reader.Close()
		}
	}()
	for _, src := range e.cfg.Sources {
		cfg := toLabConfig(e.cfg, src)
		reader, err := lab.OpenWALReader(filepath.Join(cfg.DataDir, "journal.wal"))
		if err != nil {
			return fmt.Errorf("core: replay source %s: %w", src.ID, err)
		}
		readers = append(readers, sourceReader{source: src.ID, reader: reader})
		report, err := reader.Read(opts.TolerantTail, func(rec lab.Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if (rec.Source != "" && rec.Source != src.ID) || (rec.Type == "update" && rec.Source == "") {
				return fmt.Errorf("core: WAL source mismatch in source %s", src.ID)
			}
			if (rec.Identity != "" && rec.Identity != cfg.Identity()) || (rec.Type == "update" && (rec.Commitment != cfg.Commitment || rec.FilterVersion != cfg.FilterVersion || rec.Identity != cfg.Identity())) {
				return fmt.Errorf("core: WAL identity mismatch in source %s", src.ID)
			}
			if rec.Kind == "transaction" {
				if _, err := decodeRecord(rec, true); errors.Is(err, ErrRecordIntegrity) {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("core: replay source %s: %w", src.ID, err)
		}
		readers[len(readers)-1].report = report
	}
	for _, r := range readers {
		if r.report.TruncatedTail {
			marker := lab.Record{Kind: "continuity_broken", Detail: fmt.Sprintf("offline replay ignored incomplete tail after byte %d", r.report.ValidBytes)}
			e.observeRecord(r.source, marker)
			e.diagnostic(r.source, marker)
		}
		_, err := r.reader.Read(opts.TolerantTail, func(rec lab.Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			e.observeRecord(r.source, rec)
			if rec.Kind == "transaction" {
				return e.dispatch(ctx, input{rec: rec, durable: true})
			}
			return nil
		})
		// Offline observation never implies that its historical socket is live.
		e.observeRecord(r.source, lab.Record{Kind: "session_end"})
		if err != nil {
			return fmt.Errorf("core: replay source %s: %w", r.source, err)
		}
	}
	return nil
}
