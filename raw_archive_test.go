package core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
)

func TestReadRawArchiveFromFreshProcessForBothFormats(t *testing.T) {
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
			u := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 909}}}
			record, err := lab.FromUpdate(lcfg, "a", 1, u)
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := store.Append(record); err != nil || !ok {
				t.Fatalf("append=%v err=%v", ok, err)
			}
			if err := store.Mark("a", 1, "continuity_broken", "test gap"); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			name := "journal.wal"
			if version == 2 {
				name = "journal.v2"
			}
			path := filepath.Join(lcfg.DataDir, name)
			var all []RawObservation
			if err := ReadRawArchive(context.Background(), path, "a", RawCursor{}, func(ob RawObservation) error {
				all = append(all, ob)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(all) < 4 {
				t.Fatalf("archive records=%d", len(all))
			}
			var sawUpdate, sawBreak bool
			for _, ob := range all {
				if ob.Cursor.Source != "a" || ob.Cursor.Identity == "" || ob.Cursor.JournalID == "" {
					t.Fatalf("unbound cursor: %+v", ob.Cursor)
				}
				if ob.Kind == "slot" && ob.Slot == 909 {
					sawUpdate = len(ob.Payload) > 0
				}
				if ob.Kind == "continuity_broken" {
					sawBreak = ob.Quality == "CONTINUITY_BROKEN"
				}
			}
			if !sawUpdate || !sawBreak {
				t.Fatalf("raw evidence not retained: update=%v break=%v", sawUpdate, sawBreak)
			}
			after := all[1].Cursor
			var later []RawObservation
			if err := ReadRawArchive(context.Background(), path, "a", after, func(ob RawObservation) error {
				later = append(later, ob)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(later) != len(all)-2 || later[0].Cursor.Sequence <= after.Sequence {
				t.Fatalf("resume records=%d", len(later))
			}
			bad := after
			bad.JournalID = "another"
			if err := ReadRawArchive(context.Background(), path, "a", bad, func(RawObservation) error { return nil }); !errors.Is(err, ErrCursorExpired) {
				t.Fatalf("accepted foreign cursor: %v", err)
			}
		})
	}
}

func TestReadRawArchivePreflightsBeforeDeliveringAnyEffects(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	lcfg := toLabConfig(e.cfg, e.cfg.Sources[0])
	store, err := lab.OpenStore(lcfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Mark("a", 1, "probe", "good prefix"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(lcfg.DataDir, "journal.wal")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 20 {
		t.Fatal("fixture WAL too short")
	}
	b[len(b)-1] ^= 0x80
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	var delivered int
	err = ReadRawArchive(context.Background(), path, "a", RawCursor{}, func(RawObservation) error {
		delivered++
		return nil
	})
	if err == nil || delivered != 0 {
		t.Fatalf("corrupt WAL delivered %d records before failing: %v", delivered, err)
	}
}
