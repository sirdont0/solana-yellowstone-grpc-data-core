package core

import (
	"context"
	"path/filepath"
	"testing"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

func TestRawQualityIsHistoricalAcrossLiveArchiveAndResume(t *testing.T) {
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
			if err := store.Mark("a", 1, "prefix", "before break"); err != nil {
				t.Fatal(err)
			}
			prefixSeq := store.Snapshot().WALRecords
			if err := store.Mark("a", 1, "continuity_broken", "gap"); err != nil {
				t.Fatal(err)
			}
			breakSeq := store.Snapshot().WALRecords
			if err := store.Mark("a", 1, "suffix", "after break"); err != nil {
				t.Fatal(err)
			}

			liveReader, err := e.OpenRawReader("a", RawCursor{})
			if err != nil {
				t.Fatal(err)
			}
			live, err := liveReader.Next(context.Background(), 64)
			if err != nil {
				t.Fatal(err)
			}
			if err := liveReader.Close(); err != nil {
				t.Fatal(err)
			}
			liveBySeq := make(map[uint64]RawObservation, len(live))
			for _, ob := range live {
				liveBySeq[ob.Cursor.Sequence] = ob
				want := "OBSERVING_UNVERIFIED"
				if ob.Cursor.Sequence >= breakSeq {
					want = "CONTINUITY_BROKEN"
				}
				if ob.Quality != want {
					t.Fatalf("live seq=%d quality=%s want=%s", ob.Cursor.Sequence, ob.Quality, want)
				}
			}
			if liveBySeq[prefixSeq].Kind != "prefix" || liveBySeq[breakSeq].Kind != "continuity_broken" {
				t.Fatalf("test fixture missing marker boundaries: prefix=%+v break=%+v", liveBySeq[prefixSeq], liveBySeq[breakSeq])
			}

			// The checkpoint is already broken, yet a resumed reader before the
			// break must still give the old prefix its historical quality.
			fromBefore := liveBySeq[prefixSeq-1].Cursor
			beforeReader, err := e.OpenRawReader("a", fromBefore)
			if err != nil {
				t.Fatal(err)
			}
			beforeBatch, err := beforeReader.Next(context.Background(), 64)
			if err != nil {
				t.Fatal(err)
			}
			_ = beforeReader.Close()
			if len(beforeBatch) == 0 || beforeBatch[0].Cursor.Sequence != prefixSeq || beforeBatch[0].Quality != "OBSERVING_UNVERIFIED" {
				t.Fatalf("resumed prefix quality: %+v", beforeBatch)
			}

			fromBreak := liveBySeq[breakSeq].Cursor
			postReader, err := e.OpenRawReader("a", fromBreak)
			if err != nil {
				t.Fatal(err)
			}
			postBatch, err := postReader.Next(context.Background(), 64)
			if err != nil {
				t.Fatal(err)
			}
			_ = postReader.Close()
			if len(postBatch) == 0 || postBatch[0].Quality != "CONTINUITY_BROKEN" {
				t.Fatalf("resumed post-break quality: %+v", postBatch)
			}

			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			name := "journal.wal"
			if version == 2 {
				name = "journal.v2"
			}
			var archive []RawObservation
			if err := ReadRawArchive(context.Background(), filepath.Join(lcfg.DataDir, name), "a", RawCursor{}, func(ob RawObservation) error {
				archive = append(archive, ob)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(archive) != len(live)+1 || archive[len(archive)-1].Kind != "session_end" {
				t.Fatalf("archive count=%d live count=%d: expected trailing session_end", len(archive), len(live))
			}
			for _, ob := range archive[:len(live)] {
				if old := liveBySeq[ob.Cursor.Sequence]; old.Kind != ob.Kind || old.Quality != ob.Quality {
					t.Fatalf("archive/live mismatch seq=%d: live=%+v archive=%+v", ob.Cursor.Sequence, old, ob)
				}
			}
			if archive[len(archive)-1].Quality != "CONTINUITY_BROKEN" {
				t.Fatalf("session_end lost sticky quality: %+v", archive[len(archive)-1])
			}

			// ReadRaw uses the closed-store path and must retain the marker state
			// when the caller resumes after its sequence.
			var replayed []RawObservation
			if err := e.ReadRaw(context.Background(), "a", fromBreak, func(ob RawObservation) error {
				replayed = append(replayed, ob)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(replayed) == 0 || replayed[0].Quality != "CONTINUITY_BROKEN" {
				t.Fatalf("closed-store resumed quality: %+v", replayed)
			}
		})
	}
}
