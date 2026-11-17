package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

func TestRawReaderRejectsJournalRecreatedAfterShutdown(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			cfg := configForTest(t)
			cfg.WALVersion = version
			e, err := NewRaw(cfg)
			if err != nil {
				t.Fatal(err)
			}
			lc := toLabConfig(cfg, cfg.Sources[0])
			store, err := lab.OpenStore(lc)
			if err != nil {
				t.Fatal(err)
			}
			e.stores["a"] = store
			r, err := e.OpenRawReader("a", RawCursor{})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			original := r.Cursor()
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(lc.DataDir, lc.DataDir+"-previous"); err != nil {
				t.Fatal(err)
			}
			replacement, err := lab.OpenStore(lc)
			if err != nil {
				t.Fatal(err)
			}
			if err := replacement.Close(); err != nil {
				t.Fatal(err)
			}
			page, err := r.Next(context.Background(), 128)
			if !errors.Is(err, ErrCursorExpired) || len(page) != 0 || r.Cursor() != original {
				t.Fatalf("replacement journal reached old reader: records=%v cursor=%+v err=%v", page, r.Cursor(), err)
			}
		})
	}
}

func TestRawReaderAndReadRawKeepCommittedPrefixBeforeTornTail(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	e.stores["a"] = store
	for i := 0; i < 3; i++ {
		if err := store.Mark("a", 1, "fixture", "committed prefix"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	committed := store.Snapshot().WALRecords
	path := filepath.Join(cfg.DataDir, "a", "journal.wal")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := e.OpenRawReader("a", RawCursor{})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := reader.Next(context.Background(), 20)
	if err != nil || uint64(len(batch)) != committed || batch[len(batch)-1].Cursor.Sequence != committed {
		t.Fatalf("raw reader lost durable prefix: batch=%d committed=%d err=%v", len(batch), committed, err)
	}
	if _, err := reader.Next(context.Background(), 20); !errors.Is(err, io.EOF) {
		t.Fatalf("raw reader escaped the committed frontier: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	var visited int
	if err := e.ReadRaw(context.Background(), "a", RawCursor{}, func(RawObservation) error {
		visited++
		return nil
	}); err != nil || uint64(visited) != committed {
		t.Fatalf("offline ReadRaw lost durable prefix: visited=%d committed=%d err=%v", visited, committed, err)
	}
}
