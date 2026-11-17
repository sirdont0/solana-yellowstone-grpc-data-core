package core

import (
	"context"
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
