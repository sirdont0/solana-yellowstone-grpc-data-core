package ingest

import (
	"crypto/rand"
	"encoding/hex"
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
	// resolve a durable identity conflict or replenish a RAM-only lifetime budget.
	ErrDedupeConflict    = errors.New("dedupe conflict: continuity broken")
	ErrDedupeLimit       = errors.New("dedupe capacity exhausted")
	ErrWALJournalChanged = errors.New("WAL journal changed after Store closed")
)

type SourceState struct {
	Generation          uint64    `json:"generation"`
	LastSlot            uint64    `json:"last_slot"`
	LastTxSlot          uint64    `json:"last_tx_slot"`
	LastTxStatusSlot    uint64    `json:"last_tx_status_slot,omitempty"`
	LastAccountSlot     uint64    `json:"last_account_slot,omitempty"`
	LastSlotUpdateSlot  uint64    `json:"last_slot_update_slot,omitempty"`
	LastMetaSlot        uint64    `json:"last_meta_slot"`
	LastBlockSlot       uint64    `json:"last_block_slot,omitempty"`
	LastEntrySlot       uint64    `json:"last_entry_slot,omitempty"`
	LastBlockFooterSlot uint64    `json:"last_block_footer_slot,omitempty"`
	LastSignature       string    `json:"last_signature,omitempty"`
	LastSeen            time.Time `json:"last_seen,omitempty"`
	LastTransportAt     time.Time `json:"last_transport_at,omitempty"`
	LastDataAt          time.Time `json:"last_data_at,omitempty"`
	LastProgressAt      time.Time `json:"last_progress_at,omitempty"`
	Connected           bool      `json:"connected"`
	Reconnects          uint64    `json:"reconnects"`
	Received            uint64    `json:"received"`
	Duplicates          uint64    `json:"duplicates"`
	BankIDZero          uint64    `json:"bank_id_zero"`
	Pings               uint64    `json:"pings"`
	Pongs               uint64    `json:"pongs"`
}

type Checkpoint struct {
	Schema           int                     `json:"schema"`
	UpdatedAt        time.Time               `json:"updated_at"`
	Identity         string                  `json:"identity"`
	JournalID        string                  `json:"journal_id"`
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
	mu                  sync.Mutex
	wal                 *WAL
	dir                 string
	cfg                 Config
	cp                  Checkpoint
	dedupe              map[string]string
	diskDedupe          *diskDedupe
	hotKeys             []string
	hotNext             int
	indexErr            error
	lastRecord          Record
	recoveredCheckpoint uint64
	journalID           string
	onDurable           func(Record) error
	changed             chan struct{}
	closed              bool
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
	s.signalChanged()
	if s.onDurable == nil {
		return nil
	}
	return s.onDurable(r)
}

func (s *Store) signalChanged() {
	if s.changed == nil {
		s.changed = make(chan struct{})
		return
	}
	close(s.changed)
	s.changed = make(chan struct{})
}

func OpenStore(c Config) (*Store, error) { return OpenStoreObserved(c, nil) }

// Refuse an implicit WAL container switch before opening the dedupe index or
// creating a new journal. The two formats carry independent journal IDs and
// consumer cursors; selecting the other one would silently hide history.
func checkExistingWALFormat(c Config) error {
	conflicting := filepath.Join(c.DataDir, "journal.v2")
	previous, requested := "v2", "v1"
	if c.WALVersion == 2 {
		conflicting = filepath.Join(c.DataDir, "journal.wal")
		previous, requested = "v1", "v2"
	}
	_, err := os.Lstat(conflicting)
	if err == nil {
		return fmt.Errorf("WAL format mismatch: existing %s journal at %s; requested %s. Reopen with the original WALVersion or explicitly migrate history into a new DataDir", previous, conflicting, requested)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check existing WAL format at %s: %w", conflicting, err)
	}
	return nil
}

// OpenStoreObserved observes existing WAL records and subsequent durable writes
// in source order. The callback must not call Store methods.
func OpenStoreObserved(c Config, observer func(Record) error) (*Store, error) {
	// Serialize WAL format admission across both versions. Otherwise two first
	// opens can each pass the opposite-format probe before either path exists.
	if err := os.MkdirAll(c.DataDir, 0700); err != nil {
		return nil, err
	}
	formatLock, err := os.OpenFile(filepath.Join(c.DataDir, ".wal-format.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := lockWALWriter(formatLock); err != nil {
		_ = formatLock.Close()
		return nil, fmt.Errorf("WAL format directory already locked: %w", err)
	}
	store, err := openStoreObservedLocked(c, observer)
	lockErr := formatLock.Close()
	if err != nil {
		return nil, err
	}
	if lockErr != nil {
		_ = store.Abort()
		return nil, fmt.Errorf("release WAL format directory lock: %w", lockErr)
	}
	return store, nil
}

func openStoreObservedLocked(c Config, observer func(Record) error) (*Store, error) {
	if err := checkExistingWALFormat(c); err != nil {
		return nil, err
	}
	s := &Store{onDurable: observer, cfg: c, dir: c.DataDir, dedupe: make(map[string]string), changed: make(chan struct{}), cp: Checkpoint{Schema: 1, Identity: c.Identity(), FilterVersion: c.FilterVersion, Commitment: c.Commitment, Quality: "OBSERVING_UNVERIFIED", Sources: make(map[string]*SourceState)}}
	var verifiedWAL *WAL
	if c.DedupeDisk && c.WALVersion == 2 {
		if c.DedupeLimit < 1 {
			return nil, errors.New("disk dedupe hot-cache limit must be positive")
		}
		var err error
		verifiedWAL, err = OpenWALV2(filepath.Join(c.DataDir, "journal.v2"), c.SourceID, c.Identity(), c.WALSegmentBytes, nil)
		if err != nil {
			return nil, err
		}
		s.restoreRecoveryCheckpoint(verifiedWAL)
	}
	if c.DedupeDisk && s.diskDedupe == nil {
		if c.DedupeLimit < 1 {
			return nil, errors.New("disk dedupe hot-cache limit must be positive")
		}
		index, err := openDiskDedupeForReplay(c.DataDir)
		if err != nil {
			if verifiedWAL != nil {
				_ = verifiedWAL.Close()
			}
			return nil, fmt.Errorf("open disk dedupe rebuild index: %w", err)
		}
		s.diskDedupe = index
	}
	var previousIdentity string
	walPath := filepath.Join(c.DataDir, "journal.wal")
	if c.WALVersion == 2 {
		walPath = filepath.Join(c.DataDir, "journal.v2")
	}
	visit := func(r Record) error {
		if r.Sequence <= s.recoveredCheckpoint && s.recoveredCheckpoint != 0 {
			if observer != nil {
				return observer(r)
			}
			return nil
		}

		if r.Identity != "" {
			if r.Identity != c.Identity() {
				return errors.New("WAL subscription identity differs from configuration; use a new DataDir or migrate the old journal")
			}
			if previousIdentity != "" && previousIdentity != r.Identity {
				return errors.New("WAL contains multiple subscription identities; migrate or isolate the journal before replay")
			}
			previousIdentity = r.Identity
		}
		if r.JournalID != "" {
			if s.journalID != "" && s.journalID != r.JournalID {
				return errors.New("WAL contains multiple journal IDs")
			}
			s.journalID = r.JournalID
		}
		if r.DedupeKey != "" {
			if err := s.recoverDedupe(r.DedupeKey, r.Digest); err != nil {
				return err
			}
		}
		s.apply(r)
		s.lastRecord = r
		if observer != nil {
			return observer(r)
		}
		return nil
	}
	var w *WAL
	var e error
	if verifiedWAL != nil {
		w = verifiedWAL
		sequence := s.recoveredCheckpoint
		if observer != nil {
			sequence = 0
		}
		for sequence < w.Count() {
			var page []Record
			page, e = w.ReadAfter(sequence, maxWALReadBatchRecords)
			if e != nil {
				break
			}
			if len(page) == 0 {
				e = errors.New("WAL ended before recovery frontier")
				break
			}
			for _, r := range page {
				if e = visit(r); e != nil {
					break
				}
				sequence = r.Sequence
			}
			if e != nil {
				break
			}
		}
	} else if c.WALVersion == 2 {
		w, e = OpenWALV2(walPath, c.SourceID, c.Identity(), c.WALSegmentBytes, visit)
	} else {
		w, e = OpenWAL(walPath, visit)
	}
	if e != nil {
		if verifiedWAL != nil {
			_ = verifiedWAL.Close()
		}
		_ = s.diskDedupe.close()
		return nil, e
	}
	if s.diskDedupe != nil {
		if e = s.diskDedupe.finishReplay(); e != nil {
			_ = w.Close()
			_ = s.diskDedupe.close()
			return nil, fmt.Errorf("finish WAL-derived dedupe index: %w", e)
		}
	}
	s.wal = w
	if s.journalID == "" {
		s.journalID = w.journalID
	}
	if s.journalID == "" {
		s.journalID, e = newJournalID()
		if e != nil {
			_ = w.Close()
			_ = s.diskDedupe.close()
			return nil, fmt.Errorf("create WAL journal ID: %w", e)
		}
	}
	s.cp.JournalID = s.journalID
	if previousIdentity != "" && previousIdentity != c.Identity() {
		_ = w.Close()
		_ = s.diskDedupe.close()
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

func (s *Store) rememberDedupe(key, digest string) {
	if s.diskDedupe == nil {
		s.dedupe[key] = digest
		return
	}
	if _, exists := s.dedupe[key]; exists {
		s.dedupe[key] = digest
		return
	}
	if len(s.hotKeys) < s.cfg.DedupeLimit {
		s.hotKeys = append(s.hotKeys, key)
	} else {
		delete(s.dedupe, s.hotKeys[s.hotNext])
		s.hotKeys[s.hotNext] = key
		s.hotNext = (s.hotNext + 1) % len(s.hotKeys)
	}
	s.dedupe[key] = digest
}

func (s *Store) lookupDedupe(key string) (string, bool, error) {
	if digest, ok := s.dedupe[key]; ok {
		return digest, true, nil
	}
	if s.diskDedupe == nil {
		return "", false, nil
	}
	digest, known, err := s.diskDedupe.lookup(key)
	if err == nil && known {
		s.rememberDedupe(key, digest)
	}
	return digest, known, err
}

func (s *Store) recoverDedupe(key, digest string) error {
	old, known, err := s.lookupDedupe(key)
	if err != nil {
		return fmt.Errorf("lookup WAL dedupe key: %w", err)
	}
	if known {
		if old != digest {
			return fmt.Errorf("WAL contains conflicting dedupe key %s", key)
		}
		return nil
	}
	if s.diskDedupe != nil {
		if err := s.diskDedupe.stageReplay(key, digest); err != nil {
			return fmt.Errorf("rebuild WAL dedupe index: %w", err)
		}
	}
	s.rememberDedupe(key, digest)
	if s.diskDedupe == nil && len(s.dedupe) > s.cfg.DedupeLimit {
		return errors.New("WAL dedupe key limit exceeded during recovery")
	}
	return nil
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
		if r.Kind == "transaction_status" && r.Slot > st.LastTxStatusSlot {
			st.LastTxStatusSlot = r.Slot
		}
		if r.Kind == "account" && r.Slot > st.LastAccountSlot {
			st.LastAccountSlot = r.Slot
		}
		if r.Kind == "slot" && r.Slot > st.LastSlotUpdateSlot {
			st.LastSlotUpdateSlot = r.Slot
		}
		if r.Kind == "block_meta" && r.Slot > st.LastMetaSlot {
			st.LastMetaSlot = r.Slot
		}
		if r.Kind == "block" && r.Slot > st.LastBlockSlot {
			st.LastBlockSlot = r.Slot
		}
		if r.Kind == "entry" && r.Slot > st.LastEntrySlot {
			st.LastEntrySlot = r.Slot
		}
		if r.Kind == "block_footer" && r.Slot > st.LastBlockFooterSlot {
			st.LastBlockFooterSlot = r.Slot
		}
		if !r.Time.IsZero() {
			st.LastSeen = r.Time
			st.LastTransportAt = r.Time
			switch r.Kind {
			case "transaction", "transaction_status", "account":
				st.LastDataAt = r.Time
			case "slot", "block_meta", "block", "entry", "block_footer":
				st.LastProgressAt = r.Time
			}
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

func newJournalID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func (s *Store) prepareRecord(r Record, sequence uint64) Record {
	r.Sequence = sequence
	r.JournalID = s.journalID
	return r
}

func (s *Store) Append(r Record) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrWALClosed
	}
	if s.indexErr != nil {
		return false, s.indexErr
	}
	if r.DedupeKey != "" {
		old, known, err := s.lookupDedupe(r.DedupeKey)
		if err != nil {
			s.indexErr = fmt.Errorf("disk dedupe lookup failed: %w", err)
			return false, s.indexErr
		}
		if known {
			if old != r.Digest {
				r.Type = "marker"
				r.Kind = "dedupe_conflict"
				r.Detail = "same source/slot/index/signature but different payload digest"
				r.Raw = nil
				r.DedupeKey = ""
				r = s.prepareRecord(r, s.wal.Count()+1)
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
			m = s.prepareRecord(m, s.wal.Count()+1)
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
		if s.diskDedupe == nil && len(s.dedupe) >= s.cfg.DedupeLimit {
			m := Marker(s.cfg, r.Source, r.Generation, "dedupe_limit", "exact dedupe memory budget exhausted")
			m = s.prepareRecord(m, s.wal.Count()+1)
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
	r = s.prepareRecord(r, s.wal.Count()+1)
	if err := s.wal.Append(r); err != nil {
		return false, err
	}
	if r.DedupeKey != "" {
		if s.diskDedupe != nil {
			if err := s.diskDedupe.putBatch(map[string]string{r.DedupeKey: r.Digest}); err != nil {
				s.indexErr = fmt.Errorf("dedupe index failed after durable WAL append (restart rebuild required): %w", err)
			}
		}
		s.rememberDedupe(r.DedupeKey, r.Digest)
	}
	s.apply(r)
	s.lastRecord = r
	if err := s.notifyDurable(r); err != nil {
		return false, err
	}
	return true, s.indexErr
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
	if s.closed {
		return nil, ErrWALClosed
	}
	if s.indexErr != nil {
		return nil, s.indexErr
	}
	prepared := make([]Record, 0, len(records))
	pending := make(map[string]string)
	var stopErr error
	for i, r := range records {
		if r.DedupeKey != "" {
			old, known := pending[r.DedupeKey]
			if !known {
				var err error
				old, known, err = s.lookupDedupe(r.DedupeKey)
				if err != nil {
					s.indexErr = fmt.Errorf("disk dedupe lookup failed: %w", err)
					return nil, s.indexErr
				}
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
			if s.diskDedupe == nil && len(s.dedupe)+len(pending) >= s.cfg.DedupeLimit {
				prepared = append(prepared, Marker(s.cfg, r.Source, r.Generation, "dedupe_limit", "exact dedupe memory budget exhausted"))
				stopErr = ErrDedupeLimit
				break
			}
			pending[r.DedupeKey] = r.Digest
		}
		prepared = append(prepared, r)
		accepted[i] = true
	}
	nextSequence := s.wal.Count() + 1
	for i := range prepared {
		prepared[i] = s.prepareRecord(prepared[i], nextSequence)
		nextSequence++
	}
	if err := s.wal.AppendBatch(prepared); err != nil {
		return nil, err
	}
	if s.diskDedupe != nil {
		if err := s.diskDedupe.putBatch(pending); err != nil {
			s.indexErr = fmt.Errorf("dedupe index failed after durable WAL append (restart rebuild required): %w", err)
		}
	}
	for _, r := range prepared {
		if r.DedupeKey != "" {
			s.rememberDedupe(r.DedupeKey, r.Digest)
		}
		s.apply(r)
		s.lastRecord = r
		if err := s.notifyDurable(r); err != nil {
			return nil, err
		}
	}
	return accepted, errors.Join(stopErr, s.indexErr)
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

func (s *Store) SyncStats() (last, total, max time.Duration, count uint64) {
	if s == nil || s.wal == nil {
		return 0, 0, 0, 0
	}
	return s.wal.SyncStats()
}

func (s *Store) SyncHistogram() [8]uint64 {
	if s == nil || s.wal == nil {
		return [8]uint64{}
	}
	return s.wal.SyncHistogram()
}

// ReadAfter returns a bounded copy of durable WAL records after sequence.
// While the Store is open it reads under the same lock as append/fsync; after
// close it uses a shared-lock reader for offline catch-up.
func (s *Store) ReadAfter(sequence uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		limit = 128
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrWALClosed
	}
	return s.wal.ReadAfter(sequence, limit)
}

// OpenReader acquires a shared WAL lock after the writer has closed.
func (s *Store) OpenReader() (*WALReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		return nil, errors.New("cannot open shared WAL reader while Store writer is active")
	}
	reader, err := OpenWALReader(s.wal.path)
	if err != nil {
		return nil, err
	}
	reader.maxSequence = s.wal.Count()
	reader.bounded = true
	// A closed Store no longer owns the writer lock. Another process may have
	// recreated its path with a new journal. Check the original durable frontier
	// under this reader's shared lock before returning any replacement records.
	// Use a copy so the v1 sequential reader still begins at sequence zero.
	if reader.maxSequence > 0 {
		probe := *reader
		page, err := probe.ReadAfter(reader.maxSequence-1, 1)
		if err != nil || len(page) != 1 || page[0].JournalID != s.journalID || page[0].Identity != s.cp.Identity {
			_ = reader.Close()
			return nil, errors.Join(ErrWALJournalChanged, err)
		}
	}
	return reader, nil
}

// WaitAfter returns a notification channel when a later durable record may be
// available. The ready/closed flags close the read-then-wait race.
func (s *Store) WaitAfter(sequence uint64) (wake <-chan struct{}, ready, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal.Count() > sequence {
		return nil, true, s.closed
	}
	if s.closed {
		return nil, false, true
	}
	return s.changed, false, false
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
		s.mu.Lock()
		s.closed = true
		s.signalChanged()
		w := s.wal.Close()
		index := s.diskDedupe.close()
		s.mu.Unlock()
		return errors.Join(e, w, index)
	}
	e := s.SaveCheckpoint()
	s.mu.Lock()
	s.closed = true
	s.signalChanged()
	_ = s.saveRecoveryCheckpointLocked()
	w := s.wal.Close()
	index := s.diskDedupe.close()
	s.mu.Unlock()
	return errors.Join(e, w, index)
}

// Abort closes the journal without a graceful session_end marker. The next
// startup must treat the interrupted session as continuity-broken. Use it
// after fatal handler, storage or transport errors.
func (s *Store) Abort() error {
	if s == nil || s.wal == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.signalChanged()
	return errors.Join(s.wal.Close(), s.diskDedupe.close())
}
