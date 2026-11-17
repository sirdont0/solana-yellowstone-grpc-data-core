package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
	bolt "go.etcd.io/bbolt"
)

var observationsBucket = []byte("observations_v1")
var effectsBucket = []byte("effects_v1")
var effectCountKey = []byte("applied_observations")

// openApplicationDB uses bbolt's durable commits (NoSync is never enabled).
// Application data and its idempotency key commit in one transaction. Core's
// separate ACK follows that commit, so replay after a lost ACK is harmless.
func openApplicationDB(path string) (*bolt.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(observationsBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(effectsBucket)
		return err
	})
	if err == nil {
		var dir *os.File
		dir, err = os.Open(filepath.Dir(path))
		if err == nil {
			err = errors.Join(dir.Sync(), dir.Close())
		}
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func commitObservation(db *bolt.DB, observation core.RawObservation) error {
	cursor := observation.Cursor
	if cursor.Source == "" || cursor.Identity == "" || cursor.JournalID == "" || cursor.Sequence == 0 {
		return errors.New("application commit requires a fully bound WAL cursor")
	}
	// Re-registering a consumer changes its epoch, not the observation identity.
	cursor.ConsumerEpoch = ""
	observation.Cursor = cursor
	key, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		records, effects := tx.Bucket(observationsBucket), tx.Bucket(effectsBucket)
		if records == nil || effects == nil {
			return errors.New("application database schema is missing")
		}
		if existing := records.Get(key); existing != nil {
			if !bytes.Equal(existing, encoded) {
				return errors.New("observation changed at an existing WAL cursor")
			}
			return nil
		}
		var count uint64
		if value := effects.Get(effectCountKey); value != nil {
			if len(value) != 8 {
				return errors.New("invalid application effect count")
			}
			count = binary.BigEndian.Uint64(value)
		}
		if count == ^uint64(0) {
			return errors.New("application effect count exhausted")
		}
		if err := records.Put(key, encoded); err != nil {
			return err
		}
		// Replace this counter with application-owned effects in this same tx.
		// A remote HTTP call cannot participate in this local transaction.
		var next [8]byte
		binary.BigEndian.PutUint64(next[:], count+1)
		return effects.Put(effectCountKey, next[:])
	})
}
