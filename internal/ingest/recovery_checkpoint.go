package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// This is a disposable cache of a verified WAL prefix, not a retention base.
// The full dedupe file digest prevents trusting missing, changed or phantom
// keys after an interrupted/mismatched index write. Such cases rebuild from WAL.
type recoveryCheckpoint struct {
	Version      int
	State        Checkpoint
	Last         Record
	Manifest     walV2Manifest
	ActiveDigest string
	DedupeDigest string
	Checksum     string
}

const recoveryCheckpointFile = "recovery-cache.json"

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Caller holds Store.mu. Publish only after syncing the dedupe file that this
// cache describes; the WAL is already durable. Any later index change makes
// this snapshot ineligible, rather than silently trusting ahead-of-WAL keys.
func (s *Store) saveRecoveryCheckpointLocked() error {
	if s.wal.v2 == nil || s.diskDedupe == nil || s.indexErr != nil {
		return nil
	}
	if err := s.diskDedupe.db.Sync(); err != nil {
		return err
	}
	w := s.wal.v2
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failure != nil || w.active == nil {
		return nil
	}
	state := s.cp
	state.WALRecords = w.manifest.Active.LastSequence
	state.JournalID = w.manifest.JournalID
	activeHash, err := hashWALV2Prefix(w.active, w.manifest.Active.CommittedBytes)
	if err != nil {
		return err
	}
	dbHash, err := fileSHA256(s.diskDedupe.path)
	if err != nil {
		return err
	}
	last := s.lastRecord
	last.Raw = nil // Restart classification needs metadata, not another payload copy.
	c := recoveryCheckpoint{Version: 1, State: state, Last: last, Manifest: cloneWALV2Manifest(w.manifest), ActiveDigest: hex.EncodeToString(activeHash[:]), DedupeDigest: dbHash}
	encoded, err := json.Marshal(c)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(encoded)
	c.Checksum = hex.EncodeToString(hash[:])
	encoded, err = json.Marshal(c)
	if err != nil {
		return err
	}
	return writeWALV2Atomic(filepath.Join(s.dir, recoveryCheckpointFile), encoded, 0600)
}

// w owns the exclusive writer lock and has already verified all committed WAL
// bytes. Nothing is restored until the snapshot, prefix and database all match.
func (s *Store) restoreRecoveryCheckpoint(wal *WAL) {
	f, err := os.Open(filepath.Join(s.dir, recoveryCheckpointFile))
	if err != nil {
		return
	}
	encoded, err := io.ReadAll(io.LimitReader(f, maxWALIndexCacheBytes+1))
	_ = f.Close()
	if err != nil || len(encoded) > maxWALIndexCacheBytes {
		return
	}
	var c recoveryCheckpoint
	if json.Unmarshal(encoded, &c) != nil {
		return
	}
	checksum := c.Checksum
	c.Checksum = ""
	canonical, err := json.Marshal(c)
	if err != nil {
		return
	}
	hash := sha256.Sum256(canonical)
	current := wal.v2.manifest
	if checksum != hex.EncodeToString(hash[:]) || c.Version != 1 ||
		c.State.Schema != 1 || c.State.Identity != s.cfg.Identity() || c.State.JournalID != current.JournalID || c.State.Sources == nil ||
		c.Manifest.JournalID != current.JournalID || c.Manifest.Identity != current.Identity || c.Manifest.Source != current.Source ||
		c.State.WALRecords == 0 || c.State.WALRecords > current.Active.LastSequence || c.State.WALRecords != c.Manifest.Active.LastSequence ||
		c.Last.Sequence != c.State.WALRecords || c.Last.JournalID != current.JournalID || validateWALV2Manifest(c.Manifest) != nil {
		return
	}
	for _, source := range c.State.Sources {
		if source == nil {
			return
		}
	}
	if len(c.Manifest.Sealed) > len(current.Sealed) {
		return
	}
	for i, meta := range c.Manifest.Sealed {
		if meta != current.Sealed[i] {
			return
		}
	}
	id := c.Manifest.Active.ID
	sealed := id < current.Active.ID
	if id > current.Active.ID {
		return
	}
	f, err = os.Open(walV2SegmentPath(wal.v2.dir, id, sealed))
	if err != nil {
		return
	}
	activeHash, err := hashWALV2Prefix(f, c.Manifest.Active.CommittedBytes)
	_ = f.Close()
	if err != nil || c.ActiveDigest != hex.EncodeToString(activeHash[:]) {
		return
	}
	path := filepath.Join(s.dir, dedupeDiskFilename)
	dbHash, err := fileSHA256(path)
	if err != nil || dbHash != c.DedupeDigest {
		return
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return
	}
	// Recheck while holding bbolt's exclusive lock; a standalone index writer
	// could have changed the file between the first hash and lock acquisition.
	dbHash, err = fileSHA256(path)
	if err != nil || dbHash != c.DedupeDigest {
		_ = db.Close()
		return
	}
	valid := false
	_ = db.View(func(tx *bolt.Tx) error { valid = tx.Bucket(dedupeBucket) != nil; return nil })
	if !valid {
		_ = db.Close()
		return
	}
	db.NoSync = true
	s.diskDedupe = &diskDedupe{db: db, path: path, pending: make(map[string]string, dedupeReplayBatch)}
	s.cp = c.State
	s.lastRecord = c.Last
	s.journalID = c.State.JournalID
	s.recoveredCheckpoint = c.State.WALRecords
}
