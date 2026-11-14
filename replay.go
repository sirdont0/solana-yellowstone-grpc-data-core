package core

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

// Replay decodes stored raw protobuf using the same registered handlers as
// live ingestion. CRC verification is strict. It preserves order within each
// source WAL, but source-by-source replay does not reconstruct the original
// global interleaving of independent Yellowstone streams.
func (e *Engine) Replay(ctx context.Context) error {
	if !e.running.CompareAndSwap(false, true) {
		return errors.New("core: engine already running")
	}
	defer e.running.Store(false)
	e.seen = make(map[string]string)
	e.qualityMu.Lock()
	e.broken = make(map[string]bool)
	e.qualityMu.Unlock()
	for _, s := range e.cfg.Sources {
		cfg := toLabConfig(e.cfg, s)
		path := filepath.Join(cfg.DataDir, "journal.wal")
		err := lab.ReadWAL(path, func(rec lab.Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if rec.Type == "update" && rec.Source != s.ID {
				return fmt.Errorf("core: WAL source mismatch in source %s", s.ID)
			}
			if rec.Identity != "" && rec.Identity != cfg.Identity() {
				return fmt.Errorf("core: WAL identity mismatch in source %s", s.ID)
			}
			e.observeQuality(s.ID, rec)
			if rec.Kind == "transaction" {
				return e.dispatch(ctx, input{rec: rec, durable: true})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("core: replay source %s: %w", s.ID, err)
		}
	}
	return nil
}
