package core

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

func TestReadRawArchivePreservesRegisteredConsumerEpochForAck(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "v1", 2: "v2"}[version], func(t *testing.T) {
			cfg := configForTest(t)
			cfg.WALVersion = version
			if version == 2 {
				cfg.WALSegmentBytes = 128 << 20
			}
			e, err := NewRaw(cfg)
			if err != nil {
				t.Fatal(err)
			}
			lcfg := toLabConfig(e.cfg, e.cfg.Sources[0])
			store, err := lab.OpenStore(lcfg)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Abort()
			e.stores["a"] = store
			start, err := e.RegisterConsumer("archive", "a", ConsumerFromBeginning)
			if err != nil {
				t.Fatal(err)
			}
			if start.ConsumerEpoch == "" {
				t.Fatal("registered consumer has no epoch")
			}
			if err := store.Mark("a", 1, "probe", "archive record"); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			name := "journal.wal"
			if version == 2 {
				name = "journal.v2"
			}
			var delivered RawCursor
			if err := ReadRawArchive(context.Background(), filepath.Join(lcfg.DataDir, name), "a", start, func(ob RawObservation) error {
				if ob.Cursor.ConsumerEpoch != start.ConsumerEpoch {
					t.Fatalf("archive lost consumer epoch: got=%q want=%q", ob.Cursor.ConsumerEpoch, start.ConsumerEpoch)
				}
				delivered = ob.Cursor
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if delivered.Sequence <= start.Sequence {
				t.Fatalf("no archive observations delivered: %+v", delivered)
			}
			if err := e.Ack("archive", delivered); err != nil {
				t.Fatalf("archive cursor could not be acknowledged: %v", err)
			}
			if got, err := e.ConsumerCursor("archive", "a"); err != nil || got != delivered {
				t.Fatalf("durable ACK got=%+v err=%v want=%+v", got, err, delivered)
			}
		})
	}
}

// A consumer may commit an application effect and crash before acknowledging
// the Core cursor. Archive replay must redeliver that cursor; only the later
// durable ACK permits skipping it. Exercise the public contract for both WALs.
func TestArchiveReplayAcrossLostAckAndDurableAck(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "v1", 2: "v2"}[version], func(t *testing.T) {
			cfg := configForTest(t)
			cfg.WALVersion = version
			if version == 2 {
				cfg.WALSegmentBytes = 128 << 20
			}
			engine, err := NewRaw(cfg)
			if err != nil {
				t.Fatal(err)
			}
			lcfg := toLabConfig(engine.cfg, engine.cfg.Sources[0])
			store, err := lab.OpenStore(lcfg)
			if err != nil {
				t.Fatal(err)
			}
			engine.stores["a"] = store
			start, err := engine.RegisterConsumer("lostack", "a", ConsumerFromBeginning)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Mark("a", 1, "first", "effect committed before crash"); err != nil {
				t.Fatal(err)
			}
			if err := store.Mark("a", 2, "second", "next effect"); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			journal := "journal.wal"
			if version == 2 {
				journal = "journal.v2"
			}
			path := filepath.Join(lcfg.DataDir, journal)
			var firstPass []RawCursor
			if err := ReadRawArchive(context.Background(), path, "a", start, func(o RawObservation) error {
				firstPass = append(firstPass, o.Cursor)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(firstPass) < 3 {
				t.Fatalf("fixture delivered only %d observations", len(firstPass))
			}
			// Simulate application commit at firstPass[1], then process death
			// before the corresponding ACK: the persisted Core cursor is unchanged.
			if got, err := engine.ConsumerCursor("lostack", "a"); err != nil || got != start {
				t.Fatalf("unacknowledged effect advanced Core: got=%+v err=%v", got, err)
			}
			var redelivered []RawCursor
			if err := ReadRawArchive(context.Background(), path, "a", start, func(o RawObservation) error {
				redelivered = append(redelivered, o.Cursor)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(redelivered) != len(firstPass) {
				t.Fatalf("lost ACK skipped records: first=%d replay=%d", len(firstPass), len(redelivered))
			}
			for i := range firstPass {
				if redelivered[i] != firstPass[i] {
					t.Fatalf("replay changed cursor at %d: %+v != %+v", i, redelivered[i], firstPass[i])
				}
			}
			if err := engine.Ack("lostack", firstPass[1]); err != nil {
				t.Fatal(err)
			}
			if err := engine.Ack("lostack", firstPass[0]); !errors.Is(err, ErrCursorRegression) {
				t.Fatalf("stale ACK returned %v", err)
			}
			var resumed []RawCursor
			if err := ReadRawArchive(context.Background(), path, "a", firstPass[1], func(o RawObservation) error {
				resumed = append(resumed, o.Cursor)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(resumed) != len(firstPass)-2 || resumed[0] != firstPass[2] {
				t.Fatalf("durable ACK resume boundary wrong: %+v", resumed)
			}
		})
	}
}
