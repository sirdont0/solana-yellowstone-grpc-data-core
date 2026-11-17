package ingest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALV2IndexCacheIsOptionalAndOfflineReadOnly(t *testing.T) {
	dir := createRotatedWALV2(t)
	r, err := OpenWALReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAfter(0, 2); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(dir, "indexes")
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		t.Fatalf("offline reader wrote index directory: %v", err)
	}
	// A file occupying the cache directory forces publication to fail even
	// when the test runs with privileges that bypass permission bits.
	if err := os.WriteFile(cacheDir, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatalf("optional cache failure blocked recovery: %v", err)
	}
	defer w.Close()
	if w.Count() != 2 {
		t.Fatalf("count=%d", w.Count())
	}
}

func TestWALV2PersistentIndexFallbackAndIntegrity(t *testing.T) {
	w, dir := newWALV2CommitTestWriter(t)
	batch := make([]Record, 2050)
	for i := range batch {
		batch[i] = walV2FixtureRecord(32, "indexed")
	}
	if err := w.AppendBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := w.v2.rotateLocked(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	open := func() *WAL {
		t.Helper()
		w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	w = open() // Builds the cache after verifying committed history.
	segment := w.v2.sealed[0]
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := walV2IndexPath(dir, 1)
	for _, damage := range []string{"valid", "missing", "truncated", "wrong-offset", "stale-digest"} {
		t.Run(damage, func(t *testing.T) {
			if err := saveWALV2Index(dir, segment.meta, segment.headerBytes, segment.index); err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				if err := os.Truncate(path, 8); err != nil {
					t.Fatal(err)
				}
			case "wrong-offset":
				entries := append([]walIndex(nil), segment.index...)
				entries[1].Offset++
				if err := saveWALV2Index(dir, segment.meta, segment.headerBytes, entries); err != nil {
					t.Fatal(err)
				}
			case "stale-digest":
				meta := segment.meta
				meta.Digest = "stale"
				if err := saveWALV2Index(dir, meta, segment.headerBytes, segment.index); err != nil {
					t.Fatal(err)
				}
			}
			w := open()
			if w.v2.sealed[0].indexCached != (damage == "valid") {
				t.Fatal("incorrect cache admission")
			}
			page, err := w.ReadAfter(1023, 4)
			if err != nil || len(page) != 4 || page[0].Sequence != 1024 || page[3].Sequence != 1027 {
				t.Fatalf("bad indexed page: %v %v", page, err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			w = open()
			if !w.v2.sealed[0].indexCached {
				t.Fatal("rebuilt cache not reused")
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	// A valid cache cannot hide corruption between its sparse anchors.
	f, err := os.OpenFile(walV2SegmentPath(dir, 1, true), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	offset := segment.index[1].Offset - 1
	if _, err := f.ReadAt(one[:], offset); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := f.WriteAt(one[:], offset); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil); err == nil {
		_ = w.Close()
		t.Fatal("cache hid segment corruption")
	}
}
