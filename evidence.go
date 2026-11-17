package core

import (
	"errors"
	"fmt"
	"strings"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

// EvidenceSnapshot exposes bounded source observations, never exact admission.
type EvidenceSnapshot = lab.MultiSourceSnapshot

// Diagnostic identifies an observation or decoder failure. It contains no raw
// payload. Live decoder diagnostics are also persisted as source WAL markers.
type Diagnostic struct {
	Source     string
	Generation uint64
	Slot       uint64
	Signature  string
	Kind       string
	Detail     string
}

type SourceHealth struct {
	Connected        bool
	Received         uint64
	LastSlot         uint64
	ContinuityBroken bool
	Quality          string
	Generation       uint64
	Reconnects       uint64
	Duplicates       uint64
	Pings            uint64
	Pongs            uint64
	WALRecords       uint64
	LastSeen         time.Time
	LastTransportAt  time.Time
	LastDataAt       time.Time
	LastProgressAt   time.Time
	DecodeFailures   uint64
	// QueueDepth is the shared dispatch queue, not a coverage frontier.
	QueueDepth     int
	QueueCapacity  int
	QueueBytes     int64
	QueueByteLimit int64
	// Queue wait is for shared dispatch work after byte reservation, including
	// channel-capacity backpressure. It is repeated per-source in Health.
	QueueOldestWait      time.Duration
	QueueLastWait        time.Duration
	QueueMaxWait         time.Duration
	QueuePending         int
	SourceQueueBytes     int64
	SourceQueueByteLimit int64
	// Source writer queue age runs from observation decoding until WAL batch
	// completion. Completed samples include failed batches.
	SourceWriterOldestAge  time.Duration
	SourceWriterLastAge    time.Duration
	SourceWriterMaxAge     time.Duration
	SourceWriterPending    int
	SourceWriterAgeBuckets [8]uint64
	LastWALSyncDuration    time.Duration
	TotalWALSyncDuration   time.Duration
	MaxWALSyncDuration     time.Duration
	WALSyncCount           uint64
	WALSyncAgeBuckets      [8]uint64
}

// Health includes durable counters and sticky decode failures. Final counters
// remain available after Run returns; replay does not report a live connection.
func (e *Engine) Health() map[string]SourceHealth {
	e.healthMu.RLock()
	stores := make(map[string]*lab.Store, len(e.stores))
	for id, s := range e.stores {
		stores[id] = s
	}
	failures := make(map[string]uint64, len(e.failures))
	for id, n := range e.failures {
		failures[id] = n
	}
	e.healthMu.RUnlock()
	e.queueMu.RLock()
	depth, capacity := len(e.queue), cap(e.queue)
	budget := e.queueBudget
	sourceBudgets := make(map[string]*lab.ByteBudget, len(e.sourceQueueBudgets))
	for source, sourceBudget := range e.sourceQueueBudgets {
		sourceBudgets[source] = sourceBudget
	}
	runners := make(map[string]*lab.Runner, len(e.sourceRunners))
	for source, runner := range e.sourceRunners {
		runners[source] = runner
	}
	e.queueMu.RUnlock()
	var queueBytes, queueByteLimit int64
	if budget != nil {
		queueBytes, queueByteLimit = budget.Snapshot()
	}
	oldestWait, lastWait, maxWait, pending := e.queueWaitSnapshot()
	out := make(map[string]SourceHealth, len(e.cfg.Sources))
	for _, src := range e.cfg.Sources {
		h := SourceHealth{Quality: e.quality(src.ID), DecodeFailures: failures[src.ID], QueueDepth: depth, QueueCapacity: capacity, QueueBytes: queueBytes, QueueByteLimit: queueByteLimit, QueueOldestWait: oldestWait, QueueLastWait: lastWait, QueueMaxWait: maxWait, QueuePending: pending}
		if sourceBudget := sourceBudgets[src.ID]; sourceBudget != nil {
			h.SourceQueueBytes, h.SourceQueueByteLimit = sourceBudget.Snapshot()
		}
		if runner := runners[src.ID]; runner != nil {
			qs := runner.WriterQueueStats()
			h.SourceWriterOldestAge, h.SourceWriterLastAge, h.SourceWriterMaxAge = qs.OldestAge, qs.LastAge, qs.MaxAge
			h.SourceWriterPending, h.SourceWriterAgeBuckets = qs.Pending, qs.AgeBuckets
		}
		h.ContinuityBroken = h.Quality == "CONTINUITY_BROKEN"
		if store := stores[src.ID]; store != nil {
			cp := store.Snapshot()
			h.LastWALSyncDuration, h.TotalWALSyncDuration, h.MaxWALSyncDuration, h.WALSyncCount = store.SyncStats()
			h.WALSyncAgeBuckets = store.SyncHistogram()
			h.WALRecords = cp.WALRecords
			h.ContinuityBroken = h.ContinuityBroken || cp.ContinuityBroken
			if h.ContinuityBroken {
				h.Quality = "CONTINUITY_BROKEN"
			}
			for _, st := range cp.Sources {
				h.Connected = h.Connected || st.Connected
				h.Received += st.Received
				h.LastSlot = max(h.LastSlot, st.LastSlot)
				h.Generation = max(h.Generation, st.Generation)
				h.Reconnects += st.Reconnects
				h.Duplicates += st.Duplicates
				h.Pings += st.Pings
				h.Pongs += st.Pongs
				if st.LastSeen.After(h.LastSeen) {
					h.LastSeen = st.LastSeen
				}
				if st.LastTransportAt.After(h.LastTransportAt) {
					h.LastTransportAt = st.LastTransportAt
				}
				if st.LastDataAt.After(h.LastDataAt) {
					h.LastDataAt = st.LastDataAt
				}
				if st.LastProgressAt.After(h.LastProgressAt) {
					h.LastProgressAt = st.LastProgressAt
				}
			}
		}
		h.Connected = h.Connected && e.running.Load()
		out[src.ID] = h
	}
	return out
}

// Evidence returns a detached snapshot of durable source-local observations.
// Failure domains are caller declarations; absent declarations share one domain.
func (e *Engine) Evidence() EvidenceSnapshot {
	e.healthMu.RLock()
	c := e.evidence
	e.healthMu.RUnlock()
	if c == nil {
		return EvidenceSnapshot{Quality: lab.MultiSourceUnverified, TransactionInclusionAdmission: "UNVERIFIED", AccountHistoryAdmission: "UNVERIFIED", OrderedEffectsAdmission: "UNVERIFIED"}
	}
	return c.Snapshot()
}

// Diagnostics returns the latest 256 failures; Health retains total counts.
func (e *Engine) Diagnostics() []Diagnostic {
	e.healthMu.RLock()
	defer e.healthMu.RUnlock()
	return append([]Diagnostic(nil), e.diagnostics...)
}

func (e *Engine) redact(s string) string {
	for _, src := range e.cfg.Sources {
		if src.Token != "" {
			s = strings.ReplaceAll(s, src.Token, "[REDACTED]")
		}
	}
	s = strings.NewReplacer("\n", " ", "\r", " ").Replace(s)
	if len(s) > 4096 {
		s = s[:4096] + "...[truncated]"
	}
	return s
}

func (e *Engine) diagnostic(owner string, r lab.Record) {
	e.healthMu.Lock()
	defer e.healthMu.Unlock()
	if r.Kind == "unreadable_update" || r.Kind == "handler_error" {
		e.failures[owner]++
	}
	if len(e.diagnostics) >= 256 {
		copy(e.diagnostics, e.diagnostics[1:])
		e.diagnostics = e.diagnostics[:255]
	}
	e.diagnostics = append(e.diagnostics, Diagnostic{owner, r.Generation, r.Slot, r.Signature, r.Kind, e.redact(r.Detail)})
}

func (e *Engine) observeRecord(owner string, r lab.Record) {
	e.observeQuality(owner, r)
	if r.Kind == "unreadable_update" || r.Kind == "handler_error" || r.Kind == "evidence_error" || r.Kind == "dedupe_conflict" || r.Kind == "dedupe_limit" {
		e.diagnostic(owner, r)
	}
	e.healthMu.RLock()
	c := e.evidence
	e.healthMu.RUnlock()
	if c == nil {
		return
	}
	result, err := c.ObserveWALRecord(owner, r)
	if err != nil {
		// Diagnostics must not abort raw durability or recursively write from a
		// Store callback. Older WALs may lack transport lifecycle records.
		marker := lab.Record{Kind: "evidence_error", Slot: r.Slot, Generation: r.Generation, Detail: err.Error()}
		e.observeQuality(owner, marker)
		e.diagnostic(owner, marker)
		// Source-empty failure is intentionally local to this WAL owner.
		_, _ = c.ObserveWALRecord(owner, lab.Record{Kind: "continuity_broken", Detail: "invalid source evidence"})
	} else if result.Quality == lab.MultiSourceConflict {
		for _, src := range c.Snapshot().Sources {
			if src.Conflict {
				e.observeQuality(src.ID, lab.Record{Kind: "dedupe_conflict"})
			}
		}
	}
}

func (e *Engine) decodeFailure(rec lab.Record, kind string, cause error, policy ErrorPolicy) error {
	detail := e.redact(cause.Error())
	e.healthMu.RLock()
	store := e.stores[rec.Source]
	e.healthMu.RUnlock()
	marker := lab.Record{Type: "marker", Time: time.Now().UTC(), Source: rec.Source, Generation: rec.Generation, Kind: kind, Slot: rec.Slot, Signature: rec.Signature, Commitment: rec.Commitment, FilterVersion: rec.FilterVersion, Identity: rec.Identity, Detail: detail}
	if store != nil && e.running.Load() {
		if _, err := store.Append(marker); err != nil {
			return fmt.Errorf("core: persist decode failure: %w", err)
		}
	} else {
		e.observeRecord(rec.Source, marker)
	}
	if errors.Is(cause, ErrRecordIntegrity) || policy == FailClosed {
		return fmt.Errorf("core: %s: %s", kind, detail)
	}
	if e.cfg.Logger != nil {
		e.cfg.Logger.Warn("transaction skipped", "source", rec.Source, "slot", rec.Slot, "kind", kind, "detail", detail)
	}
	return nil
}
