package ingest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	dedupeDiskFilename = "dedupe.db"
	dedupeReplayBatch  = 1024
)

var dedupeBucket = []byte("exact_dedupe_v1")

// diskDedupe is a derived index: the WAL is the only authority for membership.
// Every startup builds a fresh database from WAL's CRC/sequence-validated
// records before replacing the previous database. A crash can therefore leave
// an index either behind or ahead of WAL without changing recovery semantics.
type diskDedupe struct {
	db          *bolt.DB
	path        string
	rebuildPath string
	pending     map[string]string
}

func openDiskDedupeForReplay(dir string) (*diskDedupe, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dir, ".dedupe-rebuild-*.db")
	if err != nil {
		return nil, err
	}
	temporary := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(temporary)
		return nil, err
	}
	db, err := openDedupeDB(temporary)
	if err != nil {
		_ = os.Remove(temporary)
		return nil, err
	}
	return &diskDedupe{
		db:          db,
		path:        filepath.Join(dir, dedupeDiskFilename),
		rebuildPath: temporary,
		pending:     make(map[string]string, dedupeReplayBatch),
	}, nil
}

func openDedupeDB(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	// Every lookup is derived from the fsynced WAL. DB fsync on each append
	// would duplicate WAL's durability cost without adding correctness.
	db.NoSync = true
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dedupeBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (d *diskDedupe) lookup(key string) (digest string, found bool, err error) {
	if v, ok := d.pending[key]; ok {
		return v, true, nil
	}
	err = d.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(dedupeBucket).Get([]byte(key))
		if v != nil {
			digest, found = string(v), true
		}
		return nil
	})
	return
}

func (d *diskDedupe) stageReplay(key, digest string) error {
	d.pending[key] = digest
	if len(d.pending) >= dedupeReplayBatch {
		return d.flushReplay()
	}
	return nil
}

func (d *diskDedupe) flushReplay() error {
	if err := d.putBatch(d.pending); err != nil {
		return err
	}
	clear(d.pending)
	return nil
}

// putBatch is called only after WAL fsync in live operation. A failed index
// update makes Store terminal; the next process rebuilds from WAL.
func (d *diskDedupe) putBatch(entries map[string]string) error {
	if len(entries) == 0 {
		return nil
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(dedupeBucket)
		if bucket == nil {
			return errors.New("dedupe bucket missing")
		}
		for key, digest := range entries {
			k := []byte(key)
			if old := bucket.Get(k); old != nil {
				if string(old) != digest {
					return fmt.Errorf("disk dedupe index conflicts for key %q", key)
				}
				continue
			}
			if err := bucket.Put(k, []byte(digest)); err != nil {
				return err
			}
		}
		return nil
	})
}

// finishReplay replaces any older (possibly ahead-of-WAL) index only after
// OpenWAL has completed a validated replay and truncated any torn tail.
func (d *diskDedupe) finishReplay() error {
	if err := d.flushReplay(); err != nil {
		return err
	}
	if err := d.db.Close(); err != nil {
		d.db = nil
		return err
	}
	d.db = nil
	if err := os.Rename(d.rebuildPath, d.path); err != nil {
		return err
	}
	d.rebuildPath = ""
	db, err := openDedupeDB(d.path)
	if err != nil {
		return err
	}
	d.db = db
	d.pending = nil
	return nil
}

func (d *diskDedupe) close() error {
	if d == nil {
		return nil
	}
	var err error
	if d.db != nil {
		err = d.db.Close()
		d.db = nil
	}
	if d.rebuildPath != "" {
		if removeErr := os.Remove(d.rebuildPath); removeErr != nil && !os.IsNotExist(removeErr) {
			err = errors.Join(err, removeErr)
		}
		d.rebuildPath = ""
	}
	return err
}
