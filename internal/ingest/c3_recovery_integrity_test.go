package ingest

import (
	"os"
	"path/filepath"
	"testing"
)

// A matching recovery cache is only an acceleration hint. Committed WAL bytes
// remain authoritative and must be checked before cached state can be used.
func TestC3RecoveryCheckpointCannotHideCommittedActiveCorruption(t *testing.T) {
	c, _ := checkpointFixture(t)
	cachePath := filepath.Join(c.DataDir, recoveryCheckpointFile)
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("fixture did not publish recovery cache: %v", err)
	}
	walDir := filepath.Join(c.DataDir, "journal.v2")
	manifest, exists, err := readWALV2Current(walDir)
	if err != nil || !exists {
		t.Fatalf("read CURRENT: exists=%v err=%v", exists, err)
	}
	path := walV2SegmentPath(walDir, manifest.Active.ID, false)
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, headerLen, err := readWALV2Header(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	if headerLen+12 >= manifest.Active.CommittedBytes {
		f.Close()
		t.Fatal("fixture has no committed record body")
	}
	var b [1]byte
	offset := headerLen + 12
	if _, err = f.ReadAt(b[:], offset); err != nil {
		f.Close()
		t.Fatal(err)
	}
	b[0] ^= 0x80
	if _, err = f.WriteAt(b[:], offset); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenStore(c); err == nil {
		_ = s.Abort()
		t.Fatal("committed WAL corruption was hidden by a matching recovery cache")
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("failed recovery unexpectedly destroyed evidence: %v", err)
	}
}
