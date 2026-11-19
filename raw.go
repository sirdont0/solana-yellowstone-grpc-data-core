package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

var (
	ErrCursorExpired        = errors.New("core: raw cursor belongs to another WAL instance or subscription")
	ErrCursorRegression     = errors.New("core: consumer acknowledgement cannot move backwards")
	ErrConsumerUnregistered = errors.New("core: consumer was explicitly unregistered; register it before acknowledging again")
	ErrRawReaderClosed      = errors.New("core: raw reader is closed")
	ErrRawReaderLimit       = errors.New("core: too many open raw readers")
)

const maxOpenRawReaders = 1024

// RawCursor is a source-local position in one immutable WAL instance. It does
// not imply transaction order across sources or canonical bank completeness.
type RawCursor struct {
	Source    string `json:"source"`
	Identity  string `json:"identity"`
	JournalID string `json:"journal_id"`
	Sequence  uint64 `json:"sequence"`
	// ConsumerEpoch binds an explicitly registered consumer to one lifecycle.
	// Offline/source cursors omit this field. A re-registration gets a new
	// epoch so late ACKs from the earlier registration cannot advance it.
	ConsumerEpoch string `json:"consumer_epoch,omitempty"`
}

// RawConsumerLag is the number of source-local WAL records awaiting a named
// consumer's durable acknowledgement. It includes lifecycle/quality markers;
// it is not a count of canonical transactions or an exact time lag.
type RawConsumerLag struct {
	Source             string    `json:"source"`
	HeadSequence       uint64    `json:"head_sequence"`
	AckSequence        uint64    `json:"ack_sequence"`
	PendingRecords     uint64    `json:"pending_records"`
	OldestPendingAt    time.Time `json:"oldest_pending_at,omitempty"`
	OldestPendingAgeMs int64     `json:"oldest_pending_age_ms,omitempty"`
	OldestPendingKnown bool      `json:"oldest_pending_known"`
	AgeSampleCount     uint64    `json:"age_sample_count"`
	AgeBuckets         [8]uint64 `json:"age_buckets"`
}

// consumerLagHistogram samples the oldest unacknowledged record once per
// ConsumerLag call. It is a scrape histogram, not a distribution over all WAL
// records in the backlog.
type consumerLagHistogram struct {
	Count         uint64
	Buckets       [8]uint64
	LastSampledAt time.Time
}

const maxConsumerLagHistograms = 1024

var consumerLagAgeThresholds = [...]time.Duration{
	time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	time.Minute,
	5 * time.Minute,
	time.Hour,
}

// RawObservation is the decoder-independent envelope around one persisted
// Yellowstone update or lifecycle marker. Payload contains the protobuf
// message re-serialized after gRPC decoding; it is not the original wire frame.
// The payload slice is owned by this observation and may be retained by caller.
type RawObservation struct {
	Version         uint32    `json:"version"`
	Cursor          RawCursor `json:"cursor"`
	Type            string    `json:"type"`
	Source          string    `json:"source"`
	Generation      uint64    `json:"generation,omitempty"`
	Commitment      string    `json:"commitment,omitempty"`
	FilterVersion   string    `json:"filter_version,omitempty"`
	Identity        string    `json:"identity,omitempty"`
	Kind            string    `json:"kind"`
	Slot            uint64    `json:"slot,omitempty"`
	BankID          uint64    `json:"bank_id,omitempty"`
	BankIDPresent   bool      `json:"bank_id_present,omitempty"`
	Failed          bool      `json:"failed,omitempty"`
	StatusPresent   bool      `json:"status_present,omitempty"`
	FeeLamports     uint64    `json:"fee_lamports,omitempty"`
	FeePresent      bool      `json:"fee_present,omitempty"`
	TxIndex         uint64    `json:"tx_index,omitempty"`
	Signature       string    `json:"signature,omitempty"`
	Account         string    `json:"account,omitempty"`
	WriteVersion    uint64    `json:"write_version,omitempty"`
	ReceivedAt      time.Time `json:"received_at,omitempty"`
	SourceCreatedAt time.Time `json:"source_created_at,omitempty"`
	Digest          string    `json:"digest,omitempty"`
	Quality         string    `json:"quality"`
	Payload         []byte    `json:"payload,omitempty"`
}

var consumerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,47}$`)

// ConsumerStart controls the initial durable position of a newly registered
// consumer. It has no effect when the consumer already has a durable cursor.
type ConsumerStart uint8

const (
	// ConsumerFromBeginning starts from the first retained WAL record.
	ConsumerFromBeginning ConsumerStart = iota
	// ConsumerFromHead starts after the current durable WAL head.
	ConsumerFromHead
)

// RegisterConsumer creates a durable cursor which can be used by independent
// retention policies. Re-registering never resets an existing cursor. The
// operation is serialized across processes using the same lock as Ack.
func (e *Engine) RegisterConsumer(consumer, source string, start ConsumerStart) (RawCursor, error) {
	if !consumerNamePattern.MatchString(consumer) {
		return RawCursor{}, errors.New("core: invalid consumer name")
	}
	if start != ConsumerFromBeginning && start != ConsumerFromHead {
		return RawCursor{}, errors.New("core: invalid consumer start mode")
	}
	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	_, cp, _, err := e.rawSourceState(source)
	if err != nil {
		return RawCursor{}, err
	}
	path := e.consumerCursorPath(consumer, source)
	lock, err := lockConsumerCursor(path)
	if err != nil {
		return RawCursor{}, err
	}
	defer lock.Close()
	_, tombstoneErr := os.Stat(path + ".deleted")
	deleted := tombstoneErr == nil
	if tombstoneErr != nil && !errors.Is(tombstoneErr, os.ErrNotExist) {
		return RawCursor{}, tombstoneErr
	}
	if deleted {
		// An explicit new registration starts a fresh cursor. A late Ack from
		// an earlier registration may not resurrect a removed consumer.
		cursor := RawCursor{Source: source, Identity: cp.Identity, JournalID: cp.JournalID}
		cursor.ConsumerEpoch, err = newConsumerEpoch()
		if err != nil {
			return RawCursor{}, err
		}
		if start == ConsumerFromHead {
			cursor.Sequence = cp.WALRecords
		}
		if err := writeCursorAtomic(path, cursor); err != nil {
			return RawCursor{}, err
		}
		if err := os.Remove(path + ".deleted"); err != nil {
			return RawCursor{}, err
		}
		if err := syncConsumerCursorDir(filepath.Dir(path)); err != nil {
			return RawCursor{}, err
		}
		return cursor, nil
	}
	if _, err := os.Stat(path); err == nil {
		return e.consumerCursorLocked(consumer, source, cp)
	} else if !errors.Is(err, os.ErrNotExist) {
		return RawCursor{}, err
	}
	cursor := RawCursor{Source: source, Identity: cp.Identity, JournalID: cp.JournalID}
	cursor.ConsumerEpoch, err = newConsumerEpoch()
	if err != nil {
		return RawCursor{}, err
	}
	if start == ConsumerFromHead {
		cursor.Sequence = cp.WALRecords
	}
	if err := writeCursorAtomic(path, cursor); err != nil {
		return RawCursor{}, err
	}
	return cursor, nil
}

// UnregisterConsumer explicitly releases a durable consumer cursor. Its
// historical position cannot be recovered by re-registering after deletion.
// The stable .lock inode is intentionally retained for cross-process safety.
func (e *Engine) UnregisterConsumer(consumer, source string) error {
	if !consumerNamePattern.MatchString(consumer) {
		return errors.New("core: invalid consumer name")
	}
	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	if _, _, _, err := e.rawSourceState(source); err != nil {
		return err
	}
	path := e.consumerCursorPath(consumer, source)
	lock, err := lockConsumerCursor(path)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := writeConsumerTombstone(path + ".deleted"); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Persist the removal before allowing segment retention to use this state.
	return syncConsumerCursorDir(filepath.Dir(path))
}

// RegisteredConsumers returns the existing durable consumer cursors for a
// source. Corrupt or stale cursors fail closed.
// Calling ConsumerCursor on an unregistered name does not register it.
func (e *Engine) RegisteredConsumers(source string) (map[string]RawCursor, error) {
	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	_, cp, _, err := e.rawSourceState(source)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(e.cfg.DataDir, source, "consumers")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]RawCursor{}, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(ent.Name(), ".json")
		if !consumerNamePattern.MatchString(name) {
			return nil, fmt.Errorf("core: invalid durable consumer cursor filename %q", ent.Name())
		}
		names = append(names, name)
	}
	result := make(map[string]RawCursor, len(names))
	for _, name := range names {
		if _, err := os.Stat(e.consumerCursorPath(name, source) + ".deleted"); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		cursor, err := e.consumerCursorLocked(name, source, cp)
		if err != nil {
			return nil, fmt.Errorf("core: durable consumer %s: %w", name, err)
		}
		result[name] = cursor
	}
	return result, nil
}

// ReadRaw iterates a closed source WAL after the supplied cursor. For a live
// source use OpenRawReader. It does not invoke ALT/CPI normalization or handlers.
func (e *Engine) ReadRaw(ctx context.Context, source string, after RawCursor, visit func(RawObservation) error) error {
	if ctx == nil {
		return errors.New("core: raw read context required")
	}
	if visit == nil {
		return errors.New("core: raw observation callback required")
	}
	if e.running.Load() {
		return errors.New("core: ReadRaw is offline-only; use OpenRawReader during a live Run")
	}
	reader, err := e.OpenRawReader(source, after)
	if err != nil {
		return err
	}
	defer reader.Close()
	for {
		observations, nextErr := reader.Next(ctx, 512)
		for _, observation := range observations {
			if err := visit(observation); err != nil {
				return err
			}
		}
		if errors.Is(nextErr, io.EOF) {
			return nil
		}
		if nextErr != nil {
			return nextErr
		}
	}
}

// RawReader is a bounded source-local WAL consumer. It tails a live Store and
// switches to a shared-lock reader after the writer closes.
type RawReader struct {
	engine *Engine
	source string
	store  *lab.Store
	cursor RawCursor
	// Quality is a property of the WAL prefix ending at cursor.Sequence.
	// It must not depend on markers appended after that sequence.
	qualityBroken bool
	reader        *lab.WALReader
	nextMu        sync.Mutex
	cursorMu      sync.RWMutex
	ioMu          sync.Mutex
	closed        atomic.Bool
	done          chan struct{}
}

func (e *Engine) OpenRawReader(source string, after RawCursor) (*RawReader, error) {
	lc, cp, store, err := e.rawSourceState(source)
	if err != nil {
		return nil, err
	}
	if (after.Sequence > 0 || after.Source != "" || after.Identity != "" || after.JournalID != "") && (after.Source != source || after.Identity != lc.Identity() || after.JournalID != cp.JournalID) {
		return nil, ErrCursorExpired
	}
	if after.Sequence > cp.WALRecords {
		return nil, fmt.Errorf("core: raw cursor sequence %d is beyond WAL end %d", after.Sequence, cp.WALRecords)
	}
	if after.Sequence == 0 {
		after = RawCursor{Source: source, Identity: lc.Identity(), JournalID: cp.JournalID, ConsumerEpoch: after.ConsumerEpoch}
	}
	for {
		current := e.rawReaderCount.Load()
		if current >= maxOpenRawReaders {
			return nil, ErrRawReaderLimit
		}
		if e.rawReaderCount.CompareAndSwap(current, current+1) {
			break
		}
	}
	// Most journals never break continuity and need no prefix scan. When a
	// break is recorded, reconstruct the quality at the requested cursor in
	// bounded batches, stopping at the first break. Reserve reader capacity
	// first so failed/adversarial opens cannot cause unlimited prefix scans.
	qualityBroken := false
	if after.Sequence > 0 && cp.ContinuityBroken {
		qualityBroken, err = rawQualityBefore(store, after.Sequence)
		if err != nil {
			e.rawReaderCount.Add(-1)
			return nil, err
		}
	}
	return &RawReader{engine: e, source: source, store: store, cursor: after, qualityBroken: qualityBroken, done: make(chan struct{})}, nil
}

func (e *Engine) OpenRawReaderFromConsumer(consumer, source string) (*RawReader, error) {
	cursor, err := e.ConsumerCursor(consumer, source)
	if err != nil {
		return nil, err
	}
	return e.OpenRawReader(source, cursor)
}

// Cursor is safe to call concurrently with Next. A RawReader permits only one
// active Next call; Close may interrupt it from another goroutine.
func (r *RawReader) Cursor() RawCursor {
	r.cursorMu.RLock()
	defer r.cursorMu.RUnlock()
	return r.cursor
}

func (r *RawReader) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		close(r.done)
		r.engine.rawReaderCount.Add(-1)
	}
	r.ioMu.Lock()
	defer r.ioMu.Unlock()
	if r.reader == nil {
		return nil
	}
	err := r.reader.Close()
	r.reader = nil
	return err
}

func (r *RawReader) Next(ctx context.Context, limit int) ([]RawObservation, error) {
	if ctx == nil {
		return nil, errors.New("core: raw reader context required")
	}
	r.nextMu.Lock()
	defer r.nextMu.Unlock()
	for {
		if r.closed.Load() {
			return nil, ErrRawReaderClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sequence := r.Cursor().Sequence
		records, err := r.store.ReadAfter(sequence, limit)
		if errors.Is(err, lab.ErrWALClosed) {
			records, err = r.readClosed(limit)
		}
		if err != nil {
			return nil, err
		}
		if len(records) > 0 {
			if r.closed.Load() {
				return nil, ErrRawReaderClosed
			}
			return r.convert(records), nil
		}
		wake, ready, closed := r.store.WaitAfter(r.Cursor().Sequence)
		if ready {
			continue
		}
		if closed {
			records, err = r.readClosed(limit)
			if err != nil {
				return nil, err
			}
			if len(records) == 0 {
				return nil, io.EOF
			}
			return r.convert(records), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.done:
			return nil, ErrRawReaderClosed
		case <-wake:
		}
	}
}

func (r *RawReader) readClosed(limit int) ([]lab.Record, error) {
	r.ioMu.Lock()
	defer r.ioMu.Unlock()
	if r.closed.Load() {
		return nil, ErrRawReaderClosed
	}
	if r.reader == nil {
		reader, err := r.store.OpenReader()
		if err != nil {
			if errors.Is(err, lab.ErrWALJournalChanged) {
				return nil, errors.Join(ErrCursorExpired, err)
			}
			return nil, err
		}
		r.reader = reader
	}
	return r.reader.ReadAfter(r.Cursor().Sequence, limit)
}

func (r *RawReader) convert(records []lab.Record) []RawObservation {
	observations := make([]RawObservation, 0, len(records))
	r.cursorMu.Lock()
	defer r.cursorMu.Unlock()
	for _, rec := range records {
		r.cursor.Sequence = rec.Sequence
		if rawContinuityBreak(rec) {
			r.qualityBroken = true
		}
		observations = append(observations, rawObservationFromRecord(rec, r.source, r.cursor, rawQuality(r.qualityBroken)))
	}
	return observations
}

// rawQualityBefore reconstructs the sticky quality of a retained WAL prefix.
// It scans at most 512 records per read and returns as soon as a break is
// found. No per-record history or unbounded in-memory index is retained.
func rawQualityBefore(store *lab.Store, sequence uint64) (bool, error) {
	const batchSize = 512
	var reader *lab.WALReader
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	var next uint64
	for next < sequence {
		var records []lab.Record
		var err error
		if reader == nil {
			records, err = store.ReadAfter(next, batchSize)
			if errors.Is(err, lab.ErrWALClosed) {
				reader, err = store.OpenReader()
				if err != nil {
					if errors.Is(err, lab.ErrWALJournalChanged) {
						return false, errors.Join(ErrCursorExpired, err)
					}
					return false, err
				}
			}
		}
		if reader != nil {
			records, err = reader.ReadAfter(next, batchSize)
		}
		if err != nil {
			return false, err
		}
		if len(records) == 0 {
			return false, fmt.Errorf("core: raw quality scan ended before sequence %d", sequence)
		}
		for _, rec := range records {
			if rec.Sequence > sequence {
				return false, nil
			}
			if rawContinuityBreak(rec) {
				return true, nil
			}
			next = rec.Sequence
		}
	}
	return false, nil
}

func rawContinuityBreak(rec lab.Record) bool {
	switch rec.Kind {
	case "continuity_broken", "filter_identity_changed", "replay_retention_exceeded",
		"replay_rejected", "dedupe_conflict", "dedupe_limit", "checkpoint_write_failed",
		"unreadable_update", "handler_error", "evidence_error":
		return true
	}
	return false
}

func rawQuality(broken bool) string {
	if broken {
		return "CONTINUITY_BROKEN"
	}
	return "OBSERVING_UNVERIFIED"
}

func rawObservationFromRecord(rec lab.Record, source string, cursor RawCursor, quality string) RawObservation {
	owner := rec.Source
	if owner == "" {
		owner = source
	}
	return RawObservation{
		Version: 1, Cursor: cursor, Type: rec.Type, Source: owner, Generation: rec.Generation,
		Commitment: rec.Commitment, FilterVersion: rec.FilterVersion, Identity: rec.Identity,
		Kind: rec.Kind, Slot: rec.Slot, BankID: rec.BankID, BankIDPresent: rec.BankIDPresent,
		Failed: rec.Failed, StatusPresent: rec.StatusPresent, FeeLamports: rec.FeeLamports, FeePresent: rec.FeePresent,
		TxIndex: rec.TxIndex, Signature: rec.Signature, Account: rec.Account, WriteVersion: rec.WriteVersion,
		ReceivedAt: rec.Time, SourceCreatedAt: rec.SourceCreatedAt, Digest: rec.Digest,
		Quality: quality, Payload: rec.Raw,
	}
}

// ReadRawFromConsumer resumes after the last cursor acknowledged by consumer.
// The callback must commit its own effects before calling Ack. A crash between
// application commit and Ack intentionally causes at-least-once redelivery.
func (e *Engine) ReadRawFromConsumer(ctx context.Context, consumer, source string, visit func(RawObservation) error) error {
	cursor, err := e.ConsumerCursor(consumer, source)
	if err != nil {
		return err
	}
	return e.ReadRaw(ctx, source, cursor, visit)
}

// ConsumerCursor returns the durable source-local acknowledgement for one
// consumer. A missing cursor starts at sequence zero.
func (e *Engine) ConsumerCursor(consumer, source string) (RawCursor, error) {
	if !consumerNamePattern.MatchString(consumer) {
		return RawCursor{}, errors.New("core: invalid consumer name")
	}
	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	_, cp, _, err := e.rawSourceState(source)
	if err != nil {
		return RawCursor{}, err
	}
	lock, err := lockConsumerCursor(e.consumerCursorPath(consumer, source))
	if err != nil {
		return RawCursor{}, err
	}
	defer lock.Close()
	return e.consumerCursorLocked(consumer, source, cp)
}

// ConsumerLag reports source-local backlog relative to the durable WAL head.
// It also samples the oldest pending record age once per call. The cumulative
// buckets describe these API samples, not every record in the backlog.
// Successful WAL delivery does not imply the consumer committed its effects.
func (e *Engine) ConsumerLag(consumer, source string) (RawConsumerLag, error) {
	if !consumerNamePattern.MatchString(consumer) {
		return RawConsumerLag{}, errors.New("core: invalid consumer name")
	}
	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	_, cp, store, err := e.rawSourceState(source)
	if err != nil {
		return RawConsumerLag{}, err
	}
	cursor, err := e.consumerCursorLocked(consumer, source, cp)
	if err != nil {
		return RawConsumerLag{}, err
	}
	lag := RawConsumerLag{
		Source: source, HeadSequence: cp.WALRecords, AckSequence: cursor.Sequence,
		PendingRecords: cp.WALRecords - cursor.Sequence,
	}
	key := consumer + "\x00" + source
	histogram := e.consumerLagSamples[key]
	lag.AgeSampleCount = histogram.Count
	lag.AgeBuckets = histogram.Buckets
	if lag.PendingRecords == 0 {
		return lag, nil
	}
	oldest, ok, err := firstRawRecordAfter(store, cursor.Sequence)
	if err != nil {
		return RawConsumerLag{}, err
	}
	if !ok || oldest.Time.IsZero() {
		return lag, nil
	}
	age := time.Since(oldest.Time)
	if age < 0 {
		age = 0
	}
	lag.OldestPendingAt = oldest.Time
	lag.OldestPendingAgeMs = age.Milliseconds()
	lag.OldestPendingKnown = true
	bucket := len(consumerLagAgeThresholds)
	for i, threshold := range consumerLagAgeThresholds {
		if age <= threshold {
			bucket = i
			break
		}
	}
	if _, exists := e.consumerLagSamples[key]; !exists && len(e.consumerLagSamples) >= maxConsumerLagHistograms {
		var oldestKey string
		var oldestAt time.Time
		for candidate, sample := range e.consumerLagSamples {
			if oldestKey == "" || sample.LastSampledAt.Before(oldestAt) {
				oldestKey, oldestAt = candidate, sample.LastSampledAt
			}
		}
		delete(e.consumerLagSamples, oldestKey)
	}
	histogram.Count++
	histogram.Buckets[bucket]++
	histogram.LastSampledAt = time.Now().UTC()
	e.consumerLagSamples[key] = histogram
	lag.AgeSampleCount = histogram.Count
	lag.AgeBuckets = histogram.Buckets
	return lag, nil
}

func firstRawRecordAfter(store *lab.Store, sequence uint64) (lab.Record, bool, error) {
	records, err := store.ReadAfter(sequence, 1)
	if errors.Is(err, lab.ErrWALClosed) {
		reader, openErr := store.OpenReader()
		if openErr != nil {
			return lab.Record{}, false, openErr
		}
		defer reader.Close()
		records, err = reader.ReadAfter(sequence, 1)
	}
	if err != nil {
		return lab.Record{}, false, err
	}
	if len(records) == 0 {
		return lab.Record{}, false, nil
	}
	return records[0], true, nil
}

func (e *Engine) consumerCursorLocked(consumer, source string, cp lab.Checkpoint) (RawCursor, error) {
	path := e.consumerCursorPath(consumer, source)
	if _, err := os.Stat(path + ".deleted"); err == nil {
		return RawCursor{}, ErrConsumerUnregistered
	} else if !errors.Is(err, os.ErrNotExist) {
		return RawCursor{}, err
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return RawCursor{Source: source, Identity: cp.Identity, JournalID: cp.JournalID}, nil
	}
	if err != nil {
		return RawCursor{}, err
	}
	var cursor RawCursor
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cursor); err != nil {
		return RawCursor{}, fmt.Errorf("core: read consumer cursor: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return RawCursor{}, errors.New("core: consumer cursor contains trailing JSON")
		}
		return RawCursor{}, fmt.Errorf("core: consumer cursor trailing data: %w", err)
	}
	if cursor.Source != source || cursor.Identity != cp.Identity || cursor.JournalID != cp.JournalID || cursor.Sequence > cp.WALRecords {
		return RawCursor{}, ErrCursorExpired
	}
	return cursor, nil
}

// Ack durably advances a consumer cursor after the consumer has committed its
// corresponding effects. It is monotonic and bound to the current WAL instance.
func (e *Engine) Ack(consumer string, cursor RawCursor) error {
	if !consumerNamePattern.MatchString(consumer) {
		return errors.New("core: invalid consumer name")
	}
	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	lc, cp, _, err := e.rawSourceState(cursor.Source)
	if err != nil {
		return err
	}
	if cursor.Sequence == 0 || cursor.Identity != lc.Identity() || cursor.JournalID != cp.JournalID || cursor.Sequence > cp.WALRecords {
		return ErrCursorExpired
	}
	// A per-Engine mutex cannot serialize separate Engine instances or
	// processes sharing DataDir. Lock a stable sibling inode: the JSON cursor
	// itself is replaced by rename, so locking that inode would be ineffective.
	path := e.consumerCursorPath(consumer, cursor.Source)
	lock, err := lockConsumerCursor(path)
	if err != nil {
		return err
	}
	defer lock.Close() // Closing the descriptor releases the advisory lock.
	if _, err := os.Stat(path + ".deleted"); err == nil {
		return ErrConsumerUnregistered
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	current, err := e.consumerCursorLocked(consumer, cursor.Source, cp)
	if err != nil {
		return err
	}
	if cursor.Sequence < current.Sequence {
		return ErrCursorRegression
	}
	if current.ConsumerEpoch != cursor.ConsumerEpoch {
		return ErrCursorExpired
	}
	return writeCursorAtomic(path, cursor)
}

func newConsumerEpoch() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("core: generate consumer registration epoch: %w", err)
	}
	return hex.EncodeToString(entropy[:]), nil
}

func lockConsumerCursor(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = acquireConsumerCursorFileLock(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("core: lock consumer cursor: %w", err)
	}
	return f, nil
}

func (e *Engine) rawSourceState(source string) (lab.Config, lab.Checkpoint, *lab.Store, error) {
	var src *Source
	for i := range e.cfg.Sources {
		if e.cfg.Sources[i].ID == source {
			src = &e.cfg.Sources[i]
			break
		}
	}
	if src == nil {
		return lab.Config{}, lab.Checkpoint{}, nil, fmt.Errorf("core: unknown source %q", source)
	}
	e.healthMu.RLock()
	store := e.stores[source]
	e.healthMu.RUnlock()
	if store == nil {
		return lab.Config{}, lab.Checkpoint{}, nil, errors.New("core: source WAL has not been opened by Run")
	}
	return toLabConfig(e.cfg, *src), store.Snapshot(), store, nil
}

func (e *Engine) consumerCursorPath(consumer, source string) string {
	return filepath.Join(e.cfg.DataDir, source, "consumers", consumer+".json")
}

type cursorAtomicHooks struct {
	syncTemp func(*os.File) error
	rename   func(string, string) error
	syncDir  func(string) error
}

func writeCursorAtomic(path string, cursor RawCursor) error {
	return writeCursorAtomicWithHooks(path, cursor, cursorAtomicHooks{})
}

// writeCursorAtomicWithHooks exposes the three commit boundaries to deterministic
// fault tests. Production calls use the real fsync/rename/directory fsync.
func writeCursorAtomicWithHooks(path string, cursor RawCursor, hooks cursorAtomicHooks) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	b, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".cursor-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		if hooks.syncTemp != nil {
			err = hooks.syncTemp(f)
		} else {
			err = f.Sync()
		}
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if hooks.rename != nil {
		err = hooks.rename(tmp, path)
	} else {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		return err
	}
	if hooks.syncDir != nil {
		return hooks.syncDir(dir)
	}
	return syncConsumerCursorDir(dir)
}

func syncConsumerCursorDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func writeConsumerTombstone(path string) error {
	if err := os.WriteFile(path, []byte("unregistered\n"), 0600); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncConsumerCursorDir(filepath.Dir(path))
}
