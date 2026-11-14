package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// These errors are terminal for the current journal. Reconnecting cannot
	// resolve a durable identity conflict or replenish a lifetime dedupe budget.
	ErrDedupeConflict = errors.New("dedupe conflict: continuity broken")
	ErrDedupeLimit    = errors.New("dedupe capacity exhausted")
)

type SourceState struct {
	Generation    uint64    `json:"generation"`
	LastSlot      uint64    `json:"last_slot"`
	LastTxSlot    uint64    `json:"last_tx_slot"`
	LastMetaSlot  uint64    `json:"last_meta_slot"`
	LastSignature string    `json:"last_signature,omitempty"`
	LastSeen      time.Time `json:"last_seen,omitempty"`
	Connected     bool      `json:"connected"`
	Reconnects    uint64    `json:"reconnects"`
	Received      uint64    `json:"received"`
	Duplicates    uint64    `json:"duplicates"`
	BankIDZero    uint64    `json:"bank_id_zero"`
	Pings         uint64    `json:"pings"`
	Pongs         uint64    `json:"pongs"`
}

type Checkpoint struct {
	Schema           int                     `json:"schema"`
	UpdatedAt        time.Time               `json:"updated_at"`
	Identity         string                  `json:"identity"`
	FilterVersion    string                  `json:"filter_version"`
	Commitment       string                  `json:"commitment"`
	WALRecords       uint64                  `json:"wal_records"`
	ContinuityBroken bool                    `json:"continuity_broken"`
	BreakReasons     []string                `json:"break_reasons,omitempty"`
	Quality          string                  `json:"quality"`
	Sources          map[string]*SourceState `json:"sources"`
	// These are never promoted by a mere reconnect or sparse slot sequence.
	TxInclusionVerifiedThrough    uint64 `json:"tx_inclusion_verified_through"`
	AccountHistoryVerifiedThrough uint64 `json:"account_history_verified_through"`
}

type Store struct {
	mu         sync.Mutex
	wal        *WAL
	dir        string
	cfg        Config
	cp         Checkpoint
	dedupe     map[string]string
	lastRecord Record
	onDurable  func(Record) error
}

// SetDurableObserver installs a synchronous callback after each WAL append.
// The callback runs under Store's mutex and must not call Store methods.
// Install before starting the Runner; replay existing WAL first.
func (s *Store) SetDurableObserver(fn func(Record) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDurable = fn
}

func (s *Store) notifyDurable(r Record) error {
	if s.onDurable == nil {
		return nil
	}
	return s.onDurable(r)
}

func OpenStore(c Config) (*Store, error) { return OpenStoreObserved(c, nil) }

// OpenStoreObserved observes existing WAL records and subsequent durable writes
// in source order. The callback must not call Store methods.
func OpenStoreObserved(c Config, observer func(Record) error) (*Store, error) {
	s := &Store{onDurable: observer, cfg: c, dir: c.DataDir, dedupe: make(map[string]string), cp: Checkpoint{Schema: 1, Identity: c.Identity(), FilterVersion: c.FilterVersion, Commitment: c.Commitment, Quality: "OBSERVING_UNVERIFIED", Sources: make(map[string]*SourceState)}}
	var previousIdentity string
	w, e := OpenWAL(filepath.Join(c.DataDir, "journal.wal"), func(r Record) error {
		if r.Identity != "" {
			if r.Identity != c.Identity() {
				return errors.New("WAL subscription identity differs from configuration; use a new DataDir or migrate the old journal")
			}
			if previousIdentity != "" && previousIdentity != r.Identity {
				return errors.New("WAL contains multiple subscription identities; migrate or isolate the journal before replay")
			}
			previousIdentity = r.Identity
		}
		if r.DedupeKey != "" {
			if old, ok := s.dedupe[r.DedupeKey]; ok && old != r.Digest {
				return fmt.Errorf("WAL contains conflicting dedupe key %s", r.DedupeKey)
			}
			s.dedupe[r.DedupeKey] = r.Digest
			if len(s.dedupe) > c.DedupeLimit {
				return errors.New("WAL dedupe key limit exceeded during recovery")
			}
		}
		s.apply(r)
		s.lastRecord = r
		if observer != nil {
			return observer(r)
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	s.wal = w
	if previousIdentity != "" && previousIdentity != c.Identity() {
		_ = w.Close()
		return nil, errors.New("WAL subscription identity differs from configuration; use a new DataDir or migrate the old journal")
	}
	wasClean := s.lastRecord.Type == "" || (s.lastRecord.Type == "marker" && s.lastRecord.Kind == "session_end")
	// Replaying WAL records from an earlier process can recover historical state,
	// but no earlier transport connection remains live in this process.
	for _, st := range s.cp.Sources {
		st.Connected = false
	}
	if w.RecoveredTail {
		if e = s.Mark("", 0, "continuity_broken", "torn WAL tail truncated on startup"); e != nil {
			s.Close()
			return nil, e
		}
	}
	if !wasClean {
		if e = s.Mark("", 0, "continuity_broken", "previous process did not end with a clean session_end marker"); e != nil {
			s.Close()
			return nil, e
		}
	}
	if e = s.Mark("", 0, "session_start", "new process session"); e != nil {
		s.Close()
		return nil, e
	}
	return s, nil
}

func (s *Store) state(source string) *SourceState {
	st := s.cp.Sources[source]
	if st == nil {
		st = &SourceState{}
		s.cp.Sources[source] = st
	}
	return st
}
func (s *Store) apply(r Record) {
	if r.Type == "marker" && r.Kind == "session_end" && r.Source == "" {
		for _, st := range s.cp.Sources {
			st.Connected = false
		}
	}
	if r.Source != "" {
		st := s.state(r.Source)
		if r.Generation > st.Generation {
			st.Generation = r.Generation
		}
		if r.Slot > st.LastSlot {
			st.LastSlot = r.Slot
		}
		if r.Kind == "transaction" && r.Slot > st.LastTxSlot {
			st.LastTxSlot = r.Slot
			st.LastSignature = r.Signature
		}
		if r.Kind == "block_meta" && r.Slot > st.LastMetaSlot {
			st.LastMetaSlot = r.Slot
		}
		if !r.Time.IsZero() {
			st.LastSeen = r.Time
		}
		if r.Type == "update" {
			st.Received++
			if r.BankID == 0 && (r.Kind == "transaction" || r.Kind == "account" || r.Kind == "block_meta") {
				st.BankIDZero++
			}
		}
		switch r.Kind {
		case "connection_open":
			st.Connected = true
		case "disconnect", "session_end":
			st.Connected = false
			st.Reconnects++
		case "ping":
			st.Pings++
		case "pong":
			st.Pongs++
		case "duplicate":
			st.Duplicates++
		}
	}
	if r.Kind == "continuity_broken" || r.Kind == "filter_identity_changed" || r.Kind == "replay_retention_exceeded" || r.Kind == "replay_rejected" || r.Kind == "dedupe_conflict" || r.Kind == "dedupe_limit" || r.Kind == "checkpoint_write_failed" || r.Kind == "unreadable_update" || r.Kind == "handler_error" || r.Kind == "evidence_error" {
		s.cp.ContinuityBroken = true
		s.cp.Quality = "CONTINUITY_BROKEN"
		if len(s.cp.BreakReasons) < 50 {
			s.cp.BreakReasons = append(s.cp.BreakReasons, r.Kind+": "+r.Detail)
		}
	}
}

func (s *Store) Append(r Record) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.DedupeKey != "" {
		if old, ok := s.dedupe[r.DedupeKey]; ok {
			if old != r.Digest {
				r.Type = "marker"
				r.Kind = "dedupe_conflict"
				r.Detail = "same source/slot/index/signature but different payload digest"
				r.Raw = nil
				r.DedupeKey = ""
				if err := s.wal.Append(r); err != nil {
					return false, err
				}
				s.apply(r)
				s.lastRecord = r
				if err := s.notifyDurable(r); err != nil {
					return false, err
				}
				return false, ErrDedupeConflict
			}
			m := Marker(s.cfg, r.Source, r.Generation, "duplicate", "exact repeat of a previously durable record")
			m.Slot, m.BankID, m.Signature, m.TxIndex, m.Digest = r.Slot, r.BankID, r.Signature, r.TxIndex, r.Digest
			if err := s.wal.Append(m); err != nil {
				return false, err
			}
			s.apply(m)
			s.lastRecord = m
			if err := s.notifyDurable(m); err != nil {
				return false, err
			}
			return false, nil
		}
		if len(s.dedupe) >= s.cfg.DedupeLimit {
			m := Marker(s.cfg, r.Source, r.Generation, "dedupe_limit", "exact dedupe memory budget exhausted")
			if err := s.wal.Append(m); err != nil {
				return false, err
			}
			s.apply(m)
			s.lastRecord = m
			if err := s.notifyDurable(m); err != nil {
				return false, err
			}
			return false, ErrDedupeLimit
		}
	}
	if err := s.wal.Append(r); err != nil {
		return false, err
	}
	if r.DedupeKey != "" {
		s.dedupe[r.DedupeKey] = r.Digest
	}
	s.apply(r)
	s.lastRecord = r
	if err := s.notifyDurable(r); err != nil {
		return false, err
	}
	return true, nil
}

// AppendBatch commits a sequence under one WAL fsync. The bools correspond to
// original inputs; duplicate inputs become durable markers and return false.
// Any conflict or capacity error persists a marker, returns an error, and
// stops accepting later records from the batch. The caller must end the stream.
func (s *Store) AppendBatch(records []Record) ([]bool, error) {
	accepted := make([]bool, len(records))
	if len(records) == 0 {
		return accepted, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prepared := make([]Record, 0, len(records))
	pending := make(map[string]string)
	var stopErr error
	for i, r := range records {
		if r.DedupeKey != "" {
			old, known := pending[r.DedupeKey]
			if !known {
				old, known = s.dedupe[r.DedupeKey]
			}
			if known {
				if old != r.Digest {
					m := Marker(s.cfg, r.Source, r.Generation, "dedupe_conflict", "same source/slot/index/signature but different payload digest")
					prepared = append(prepared, m)
					stopErr = ErrDedupeConflict
					break
				}
				m := Marker(s.cfg, r.Source, r.Generation, "duplicate", "exact repeat of a previously durable record")
				m.Slot, m.BankID, m.Signature, m.TxIndex, m.Digest = r.Slot, r.BankID, r.Signature, r.TxIndex, r.Digest
				prepared = append(prepared, m)
				continue
			}
			if len(s.dedupe)+len(pending) >= s.cfg.DedupeLimit {
				prepared = append(prepared, Marker(s.cfg, r.Source, r.Generation, "dedupe_limit", "exact dedupe memory budget exhausted"))
				stopErr = ErrDedupeLimit
				break
			}
			pending[r.DedupeKey] = r.Digest
		}
		prepared = append(prepared, r)
		accepted[i] = true
	}
	if err := s.wal.AppendBatch(prepared); err != nil {
		return nil, err
	}
	for _, r := range prepared {
		if r.DedupeKey != "" {
			s.dedupe[r.DedupeKey] = r.Digest
		}
		s.apply(r)
		s.lastRecord = r
		if err := s.notifyDurable(r); err != nil {
			return nil, err
		}
	}
	return accepted, stopErr
}
func (s *Store) Mark(source string, generation uint64, kind, detail string) error {
	_, e := s.Append(Marker(s.cfg, source, generation, kind, detail))
	return e
}
func (s *Store) Snapshot() Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.cp
	c.WALRecords = s.wal.Count()
	c.UpdatedAt = time.Now().UTC()
	c.Sources = make(map[string]*SourceState, len(s.cp.Sources))
	for k, v := range s.cp.Sources {
		cp := *v
		c.Sources[k] = &cp
	}
	c.BreakReasons = append([]string(nil), s.cp.BreakReasons...)
	return c
}
func (s *Store) SaveCheckpoint() error {
	cp := s.Snapshot()
	b, e := json.MarshalIndent(cp, "", "  ")
	if e != nil {
		return e
	}
	path := filepath.Join(s.dir, "checkpoint.json")
	tmp := path + ".tmp"
	f, e := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		return e
	}
	dir, e := os.Open(s.dir)
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if e := s.Mark("", 0, "session_end", "graceful shutdown"); e != nil {
		_ = s.wal.Close()
		return e
	}
	e := s.SaveCheckpoint()
	w := s.wal.Close()
	if e != nil {
		return e
	}
	return w
}

// Abort closes the journal without a graceful session_end marker. The next
// startup must treat the interrupted session as continuity-broken. Use it
// after fatal handler, storage or transport errors.
func (s *Store) Abort() error {
	if s == nil || s.wal == nil {
		return nil
	}
	return s.wal.Close()
}
