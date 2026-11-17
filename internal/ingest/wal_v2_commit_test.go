package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func newWALV2CommitTestWriter(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "journal.v2")
	w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	return w, dir
}

func TestWALV2PublishedCurrentErrorKeepsCommittedPrefix(t *testing.T) {
	w, dir := newWALV2CommitTestWriter(t)
	if err := w.Append(walV2FixtureRecord(128, "first")); err != nil {
		t.Fatal(err)
	}
	w.v2.manifestWriter = func(dir string, manifest walV2Manifest) error {
		if err := writeWALV2Manifest(dir, manifest); err != nil {
			return err
		}
		return errors.New("injected failure after CURRENT publication")
	}
	err := w.Append(walV2FixtureRecord(128, "second"))
	if !errors.Is(err, ErrWALV2CommitUncertain) {
		t.Fatalf("post-publication append error = %v, want uncertain commit", err)
	}
	if again := w.Append(walV2FixtureRecord(128, "third")); !errors.Is(again, ErrWALV2CommitUncertain) {
		t.Fatalf("writer continued after ambiguous commit: %v", again)
	}
	if _, readErr := w.ReadAfter(0, 10); !errors.Is(readErr, ErrWALV2CommitUncertain) {
		t.Fatalf("failed writer exposed an outdated in-memory cursor: %v", readErr)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var recovered []Record
	reopened, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(r Record) error {
		recovered = append(recovered, r)
		return nil
	})
	if err != nil {
		t.Fatalf("published CURRENT no longer reopens: %v", err)
	}
	defer reopened.Close()
	if reopened.Count() != 2 || len(recovered) != 2 || recovered[0].Detail != "first" || recovered[1].Detail != "second" {
		t.Fatalf("published prefix changed: count=%d records=%+v", reopened.Count(), recovered)
	}
	if err := reopened.Append(walV2FixtureRecord(128, "third")); err != nil {
		t.Fatalf("recovered writer cannot advance: %v", err)
	}
}

func TestWALV2PreCurrentErrorRollsBackOnlyProposedBatch(t *testing.T) {
	w, dir := newWALV2CommitTestWriter(t)
	if err := w.Append(walV2FixtureRecord(128, "first")); err != nil {
		t.Fatal(err)
	}
	w.v2.manifestWriter = func(string, walV2Manifest) error {
		return errors.New("injected failure before CURRENT publication")
	}
	if err := w.Append(walV2FixtureRecord(128, "second")); err == nil || errors.Is(err, ErrWALV2CommitUncertain) {
		t.Fatalf("pre-publication error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Count() != 1 {
		t.Fatalf("pre-publication failure changed committed count to %d", reopened.Count())
	}
	page, err := reopened.ReadAfter(0, 10)
	if err != nil || len(page) != 1 || page[0].Detail != "first" {
		t.Fatalf("committed prefix after rollback=%+v err=%v", page, err)
	}
}

func TestWALV2RotationCurrentPublicationErrorRecovers(t *testing.T) {
	for _, publish := range []bool{false, true} {
		t.Run(fmt.Sprintf("published=%t", publish), func(t *testing.T) {
			w, dir := newWALV2CommitTestWriter(t)
			if err := w.Append(walV2FixtureRecord(1<<20, "first")); err != nil {
				t.Fatal(err)
			}
			w.v2.manifestWriter = func(dir string, manifest walV2Manifest) error {
				if publish {
					if err := writeWALV2Manifest(dir, manifest); err != nil {
						return err
					}
				}
				return errors.New("injected rotation publication failure")
			}
			err := w.Append(walV2FixtureRecord(1<<20, "second"))
			if err == nil {
				t.Fatal("rotation fault was not exercised")
			}
			if publish && !errors.Is(err, ErrWALV2CommitUncertain) {
				t.Fatalf("published rotation error was not reported as uncertain: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
			if err != nil {
				t.Fatalf("failed rotation left an unrecoverable journal: %v", err)
			}
			defer reopened.Close()
			if reopened.Count() != 1 {
				t.Fatalf("rotation recovery count=%d want=1", reopened.Count())
			}
			if err := reopened.Append(walV2FixtureRecord(1<<20, "second")); err != nil {
				t.Fatalf("cannot append after rotation recovery: %v", err)
			}
			if reopened.Count() != 2 {
				t.Fatalf("count after rotation recovery append=%d", reopened.Count())
			}
		})
	}
}

func TestWALV2SegmentCapacityBoundaries(t *testing.T) {
	const segmentBytes = int64(2 << 20)
	for _, delta := range []int64{-1, 0, 1} {
		t.Run(fmt.Sprintf("delta=%d", delta), func(t *testing.T) {
			w, dir := newWALV2CommitTestWriter(t)
			base := walV2FixtureRecord(0, "boundary")
			frames, _, err := encodeWALV2Batch([]Record{base}, w.v2.manifest)
			if err != nil {
				t.Fatal(err)
			}
			capacity := walV2DataLimit(segmentBytes) - w.v2.activeHeaderLen
			rawSize := capacity - int64(len(frames[0])) + delta
			if rawSize <= 0 {
				t.Fatalf("invalid test raw size %d", rawSize)
			}
			base.Raw = bytes.Repeat([]byte{0x5a}, int(rawSize))
			err = w.Append(base)
			if delta > 0 {
				if err == nil {
					t.Fatal("one byte over segment limit was accepted")
				}
				if w.Count() != 0 {
					t.Fatalf("oversized record mutated count=%d", w.Count())
				}
			} else {
				if err != nil {
					t.Fatalf("record at capacity %+d rejected: %v", delta, err)
				}
			}
			if err := w.Append(walV2FixtureRecord(128, "following")); err != nil {
				t.Fatalf("size rejection/full segment made writer unusable: %v", err)
			}
			if delta <= 0 {
				sealed := walV2SegmentPath(dir, 1, true)
				stat, err := os.Stat(sealed)
				if err != nil {
					t.Fatal(err)
				}
				if stat.Size() != segmentBytes+delta {
					t.Fatalf("sealed size=%d want=%d", stat.Size(), segmentBytes+delta)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWALV2OversizedBatchDoesNotPrematurelyRotate(t *testing.T) {
	w, dir := newWALV2CommitTestWriter(t)
	if err := w.Append(walV2FixtureRecord(128, "first")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(walV2FixtureRecord(3<<20, "oversized")); err == nil {
		t.Fatal("oversized batch was accepted")
	}
	manifest, exists, err := readWALV2Current(dir)
	if err != nil || !exists {
		t.Fatalf("read manifest exists=%v err=%v", exists, err)
	}
	if len(manifest.Sealed) != 0 || manifest.Active.ID != 1 || manifest.Active.Records != 1 {
		t.Fatalf("size rejection rotated or mutated journal: %+v", manifest)
	}
	if err := w.Append(walV2FixtureRecord(128, "second")); err != nil {
		t.Fatalf("writer stopped after oversized batch rejection: %v", err)
	}
	if w.Count() != 2 {
		t.Fatalf("count=%d want=2", w.Count())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALV2FullBatchCapacityBoundary(t *testing.T) {
	for _, delta := range []int64{0, 1} {
		t.Run(fmt.Sprintf("delta=%d", delta), func(t *testing.T) {
			w, _ := newWALV2CommitTestWriter(t)
			first := walV2FixtureRecord(0, "first")
			second := walV2FixtureRecord(0, "second")
			frames, _, err := encodeWALV2Batch([]Record{first, second}, w.v2.manifest)
			if err != nil {
				t.Fatal(err)
			}
			capacity := walV2DataLimit(2<<20) - w.v2.activeHeaderLen
			rawBudget := capacity - int64(len(frames[0])+len(frames[1])) + delta
			firstSize := rawBudget / 2
			secondSize := rawBudget - firstSize
			first.Raw = bytes.Repeat([]byte{0x5a}, int(firstSize))
			second.Raw = bytes.Repeat([]byte{0x5a}, int(secondSize))
			err = w.AppendBatch([]Record{first, second})
			if delta == 0 && err != nil {
				t.Fatalf("full batch exactly at capacity rejected: %v", err)
			}
			if delta == 1 && err == nil {
				t.Fatal("full batch exceeding capacity by one byte accepted")
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
