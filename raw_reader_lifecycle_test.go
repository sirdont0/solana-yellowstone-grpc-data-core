package core

import (
	"context"
	"errors"
	"testing"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

func TestRawReaderCloseWakesBlockedNext(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e.stores["a"] = store
	reader, err := e.OpenRawReader("a", RawCursor{})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := reader.Next(context.Background(), 100)
	if err != nil || len(initial) == 0 {
		t.Fatalf("initial records=%d err=%v", len(initial), err)
	}
	finished := make(chan error, 1)
	go func() {
		_, nextErr := reader.Next(context.Background(), 1)
		finished <- nextErr
	}()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, ErrRawReaderClosed) {
			t.Fatalf("Next after Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake blocked Next")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if got := e.rawReaderCount.Load(); got != 0 {
		t.Fatalf("reader count after close=%d", got)
	}
	if _, err := reader.Next(context.Background(), 1); !errors.Is(err, ErrRawReaderClosed) {
		t.Fatalf("closed Next=%v", err)
	}
}

func TestRawReaderAndReadRawRejectNilContext(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e.stores["a"] = store
	reader, err := e.OpenRawReader("a", RawCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var absent context.Context
	if _, err := reader.Next(absent, 1); err == nil {
		t.Fatal("Next accepted a nil context")
	}
	if err := e.ReadRaw(absent, "a", RawCursor{}, func(RawObservation) error { return nil }); err == nil {
		t.Fatal("ReadRaw accepted a nil context")
	}
}

func TestRawReaderCountIsBoundedAndReusable(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e.stores["a"] = store
	readers := make([]*RawReader, 0, maxOpenRawReaders)
	for i := 0; i < maxOpenRawReaders; i++ {
		r, err := e.OpenRawReader("a", RawCursor{})
		if err != nil {
			t.Fatalf("reader %d: %v", i, err)
		}
		readers = append(readers, r)
	}
	if _, err := e.OpenRawReader("a", RawCursor{}); !errors.Is(err, ErrRawReaderLimit) {
		t.Fatalf("limit=%v", err)
	}
	if err := readers[0].Close(); err != nil {
		t.Fatal(err)
	}
	r, err := e.OpenRawReader("a", RawCursor{})
	if err != nil {
		t.Fatalf("capacity not restored: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	for _, r := range readers[1:] {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.rawReaderCount.Load(); got != 0 {
		t.Fatalf("leaked readers=%d", got)
	}
}
