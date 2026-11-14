// Package core provides an embeddable Solana Yellowstone event engine.
//
// It preserves source-local evidence, raw WAL and explicit uncertainty, and
// lets applications register protocol decoders in-process. Until independent
// bank, account and config completeness is verified, emitted events remain
// observations and MUST NOT be treated as exact canonical state.
package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

// DeliveryMode selects when handlers receive observed transactions.
type DeliveryMode uint8

const (
	// Provisional minimizes latency but may produce events lost on crash.
	Provisional DeliveryMode = iota
	// Durable emits after raw WAL fsync, trading latency for durability.
	Durable
)

type Source struct {
	ID            string
	FailureDomain string // Empty means unspecified; never inferred as independent.
	Endpoint      string
	Token         string `json:"-"` // must be supplied from environment/secret manager
	TokenHeader   string
	Commitment    string
	FilterVersion string
	Programs      []string
}

type Config struct {
	Sources       []Source
	DataDir       string
	Delivery      DeliveryMode
	QueueCapacity int
	DedupeLimit   int
	PingInterval  time.Duration
	IdleTimeout   time.Duration
	MaxMessageMiB int
	// Zero overlap selects 64 slots. Reconnect never proves completeness.
	ReplayOverlap     uint64
	MaxReconnects     int // Zero retries indefinitely.
	FromSlot          *uint64
	DecodeErrorPolicy ErrorPolicy  // FailClosed by default.
	Logger            *slog.Logger // Optional structured, credential-redacted transport logs.
	EvidenceBankLimit int          // Zero selects 256 banks per source.
}

// ErrorPolicy governs decode failures. Storage, identity conflicts and sink
// failures always stop the engine; SkipAndFlag discards the WHOLE transaction.
type ErrorPolicy uint8

const (
	FailClosed ErrorPolicy = iota
	SkipAndFlag
)

type registeredHandler struct {
	handler Handler
	policy  ErrorPolicy
}

type Emission struct {
	Kind    string
	Payload any
}

type Handler interface {
	// ProgramIDs declares the programs the handler understands.
	ProgramIDs() []string
	// Decode receives an immutable normalized transaction and exact CPI
	// occurrence. It should return no effects for unsupported instruction data.
	Decode(*Transaction, Instruction) ([]Emission, error)
}

// Event is source-observed, potentially provisional data with stable CPI
// position and event ordinal. It is not a complete-bank certification.
type Event struct {
	ID            string
	Source        string
	Generation    uint64
	Commitment    string
	FilterVersion string
	Slot          uint64
	BankID        uint64
	TxIndex       uint64
	Signature     string
	Outer         uint32
	Inner         int
	ProgramID     string
	Kind          string
	Payload       any
	Durable       bool
	Quality       string
}

type Sink func(context.Context, Event) error

// Engine has one bounded in-process event queue and one ordered dispatch loop.
// Its output has receive order only, not proven canonical bank order.
type Engine struct {
	cfg         Config
	sink        Sink
	handlers    map[string][]registeredHandler
	mu          sync.Mutex
	running     atomic.Bool
	seen        map[string]string
	healthMu    sync.RWMutex
	stores      map[string]*lab.Store
	qualityMu   sync.RWMutex
	broken      map[string]bool
	evidence    *lab.MultiSourceCoordinator
	diagnostics []Diagnostic
	failures    map[string]uint64
	queueMu     sync.RWMutex
	queue       chan input
}

func New(cfg Config, sink Sink) (*Engine, error) {
	if sink == nil {
		return nil, errors.New("core: sink required")
	}
	if len(cfg.Sources) == 0 || len(cfg.Sources) > 16 {
		return nil, errors.New("core: 1..16 sources required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("core: DataDir required")
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 4096
	}
	if cfg.DedupeLimit <= 0 {
		cfg.DedupeLimit = 2_000_000
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = 15 * time.Second
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 45 * time.Second
	}
	if cfg.MaxMessageMiB <= 0 {
		cfg.MaxMessageMiB = 64
	}
	if cfg.MaxMessageMiB > 256 {
		return nil, errors.New("core: MaxMessageMiB must be 1..256")
	}
	if cfg.DecodeErrorPolicy > SkipAndFlag {
		return nil, errors.New("core: invalid decode error policy")
	}
	if cfg.ReplayOverlap == 0 {
		cfg.ReplayOverlap = 64
	}
	if cfg.EvidenceBankLimit == 0 {
		cfg.EvidenceBankLimit = 256
	}
	if cfg.EvidenceBankLimit < 1 || cfg.EvidenceBankLimit > 4096 {
		return nil, errors.New("core: EvidenceBankLimit must be 1..4096")
	}
	// Own all mutable configuration so callers cannot race a running engine.
	cfg.Sources = append([]Source(nil), cfg.Sources...)
	for i := range cfg.Sources {
		cfg.Sources[i].Programs = append([]string(nil), cfg.Sources[i].Programs...)
	}
	if cfg.FromSlot != nil {
		v := *cfg.FromSlot
		cfg.FromSlot = &v
	}
	if cfg.Delivery != Provisional && cfg.Delivery != Durable {
		return nil, errors.New("core: invalid delivery mode")
	}
	ids := make(map[string]bool)
	for _, s := range cfg.Sources {
		if s.ID == "" || strings.ContainsAny(s.ID, "/\\.") || ids[s.ID] {
			return nil, fmt.Errorf("core: invalid or duplicate source ID %q", s.ID)
		}
		ids[s.ID] = true
		if err := toLabConfig(cfg, s).Validate(); err != nil {
			return nil, fmt.Errorf("core: source %s: %w", s.ID, err)
		}
	}
	return &Engine{cfg: cfg, sink: sink, handlers: make(map[string][]registeredHandler), seen: make(map[string]string), stores: make(map[string]*lab.Store), broken: make(map[string]bool), failures: make(map[string]uint64)}, nil
}

func toLabConfig(cfg Config, s Source) lab.Config {
	commit := s.Commitment
	if commit == "" {
		commit = "finalized"
	}
	header := s.TokenHeader
	if header == "" {
		header = "x-token"
	}
	version := s.FilterVersion
	if version == "" {
		version = "v1"
	}
	return lab.Config{
		SourceID: s.ID, Endpoint: s.Endpoint, Token: s.Token,
		TokenHeader: header, Commitment: commit, FilterVersion: version,
		Programs: append([]string(nil), s.Programs...),
		DataDir:  filepath.Join(cfg.DataDir, s.ID),
		Streams:  1, DedupeLimit: cfg.DedupeLimit,
		ReplayOverlap: cfg.ReplayOverlap, MaxReconnects: cfg.MaxReconnects, FromSlot: cfg.FromSlot, Logger: cfg.Logger,
		PingInterval: cfg.PingInterval, IdleTimeout: cfg.IdleTimeout,
		MaxMessageBytes: cfg.MaxMessageMiB << 20,
	}
}

// Register must be called before Run. Handlers remain application-owned;
// changing one while the engine runs is unsupported.
func (e *Engine) Register(h Handler) error {
	return e.RegisterWithPolicy(h, FailClosed)
}

// RegisterWithPolicy selects explicit isolation for this decoder. A skipped
// decoder failure discards all effects from its transaction, including effects
// prepared by other handlers. The raw observation remains available for replay.
func (e *Engine) RegisterWithPolicy(h Handler, policy ErrorPolicy) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running.Load() {
		return errors.New("core: register while engine is running")
	}
	if h == nil || policy > SkipAndFlag {
		return errors.New("core: invalid handler or error policy")
	}
	ids := append([]string(nil), h.ProgramIDs()...)
	if len(ids) == 0 {
		return errors.New("core: handler must declare program IDs")
	}
	for _, p := range ids {
		if p == "" {
			return errors.New("core: empty program ID")
		}
	}
	unique := make(map[string]bool)
	for _, p := range ids {
		if !unique[p] {
			e.handlers[p] = append(e.handlers[p], registeredHandler{h, policy})
			unique[p] = true
		}
	}
	return nil
}

func (e *Engine) begin() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running.CompareAndSwap(false, true) {
		return errors.New("core: already running")
	}
	e.seen = make(map[string]string)
	e.qualityMu.Lock()
	e.broken = make(map[string]bool)
	e.qualityMu.Unlock()
	specs := make([]lab.MultiSourceSpec, 0, len(e.cfg.Sources))
	for _, src := range e.cfg.Sources {
		lc := toLabConfig(e.cfg, src)
		domain := src.FailureDomain
		if domain == "" {
			domain = "unspecified"
		}
		specs = append(specs, lab.MultiSourceSpec{ID: src.ID, FailureDomain: domain, Identity: lc.Identity(), Commitment: lc.Commitment, FilterVersion: lc.FilterVersion})
	}
	evidence, err := lab.NewMultiSourceCoordinator(specs, e.cfg.EvidenceBankLimit)
	if err != nil {
		e.running.Store(false)
		return err
	}
	e.healthMu.Lock()
	e.stores = make(map[string]*lab.Store)
	e.evidence = evidence
	e.diagnostics = nil
	e.failures = make(map[string]uint64)
	e.healthMu.Unlock()
	return nil
}

type input struct {
	rec     lab.Record
	durable bool
}

// Run owns transports and WALs for this session. It returns only when the
// context ends or a fatal source, storage, or decoder error occurs.
func (e *Engine) Run(parent context.Context) error {
	if err := e.begin(); err != nil {
		return err
	}
	defer e.running.Store(false)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	in := make(chan input, e.cfg.QueueCapacity)
	e.queueMu.Lock()
	e.queue = in
	e.queueMu.Unlock()
	defer func() { e.queueMu.Lock(); e.queue = nil; e.queueMu.Unlock() }()
	errCh := make(chan error, len(e.cfg.Sources))
	var wg sync.WaitGroup
	var opened []*lab.Store
	type preparedSource struct {
		cfg   lab.Config
		store *lab.Store
	}
	var prepared []preparedSource
	for _, src := range e.cfg.Sources {
		lc := toLabConfig(e.cfg, src)
		sourceID := src.ID
		store, err := lab.OpenStoreObserved(lc, func(r lab.Record) error { e.observeRecord(sourceID, r); return nil })
		if err != nil {
			for _, s := range opened {
				_ = s.Close()
			}
			e.healthMu.Lock()
			e.stores = make(map[string]*lab.Store)
			e.healthMu.Unlock()
			return err
		}
		opened = append(opened, store)
		prepared = append(prepared, preparedSource{cfg: lc, store: store})
		if store.Snapshot().ContinuityBroken {
			e.qualityMu.Lock()
			e.broken[src.ID] = true
			e.qualityMu.Unlock()
		}
		e.healthMu.Lock()
		e.stores[src.ID] = store
		e.healthMu.Unlock()
	}
	for _, source := range prepared {
		runner := lab.NewRunner(source.cfg, source.store)
		send := func(rec lab.Record, durable bool) {
			if rec.Kind != "transaction" {
				return
			}
			// The dispatch loop drains this bounded channel even after cancel.
			// Dropping a durable callback here would leave a synced WAL record
			// undelivered while allowing a misleading clean session_end marker.
			in <- input{rec: rec, durable: durable}
		}
		if e.cfg.Delivery == Provisional {
			runner.OnProvisional = func(rec lab.Record) { send(rec, false) }
		} else {
			runner.OnLive = func(rec lab.Record) { send(rec, true) }
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := runner.Run(ctx)
			if err != nil {
				cancel()
			}
			errCh <- err
		}()
	}
	go func() { wg.Wait(); close(in) }()
	var first error
	for item := range in {
		if first != nil {
			continue
		}
		if err := e.dispatch(ctx, item); err != nil {
			first = err
			cancel()
		}
	}
	for range e.cfg.Sources {
		if err := <-errCh; err != nil && first == nil {
			first = err
		}
	}
	for _, s := range opened {
		var err error
		if first != nil {
			err = s.Abort()
		} else {
			err = s.Close()
		}
		if err != nil && first == nil {
			first = err
		}
	}

	if first != nil {
		return first
	}
	return nil
}

func (e *Engine) dispatch(ctx context.Context, item input) error {
	tx, err := decodeRecord(item.rec, item.durable)
	if err != nil {
		return e.decodeFailure(item.rec, "unreadable_update", err, e.cfg.DecodeErrorPolicy)
	}
	key := fmt.Sprintf("%s:%d:%d", tx.Commitment, tx.Slot, tx.Index)
	if old, ok := e.seen[key]; ok {
		if old != tx.Digest {
			return e.decodeFailure(item.rec, "dedupe_conflict", fmt.Errorf("conflicting providers for transaction position %s", key), FailClosed)
		}
		return nil
	}
	if len(e.seen) >= e.cfg.DedupeLimit {
		return e.decodeFailure(item.rec, "dedupe_limit", errors.New("cross-source dedupe capacity exhausted"), FailClosed)
	}
	if tx.Failed {
		e.seen[key] = tx.Digest
		return nil
	}
	// Decode the whole transaction before publishing any effects. A later
	// handler error must not leave an earlier handler's effects half-applied.
	// The Sink may still fail partway through delivery, so consumers must use
	// stable Event IDs for idempotency or supply their own atomic commit.
	var pending []Event
	for _, ix := range tx.Instructions {
		// The ordinal spans all handlers registered for this instruction.
		// Restarting it for each handler would give distinct events the same ID.
		emissionOrdinal := 0
		for _, h := range e.handlers[ix.ProgramID] {
			output, err := h.handler.Decode(tx, ix)
			if err != nil {
				return e.decodeFailure(item.rec, "handler_error", fmt.Errorf("handler %s outer=%d inner=%d: %w", ix.ProgramID, ix.Outer, ix.Inner, err), h.policy)
			}
			for _, emission := range output {
				ev := Event{
					ID:     fmt.Sprintf("%s/%d/%d/%s/%d/%d/%d", tx.Commitment, tx.Slot, tx.Index, tx.Signature, ix.Outer, ix.Inner, emissionOrdinal),
					Source: tx.Source, Generation: tx.Generation,
					Commitment: tx.Commitment, FilterVersion: tx.FilterVersion,
					Slot: tx.Slot, BankID: tx.BankID, TxIndex: tx.Index,
					Signature: tx.Signature, Outer: ix.Outer, Inner: ix.Inner,
					ProgramID: ix.ProgramID, Kind: emission.Kind, Payload: emission.Payload,
					Durable: tx.Durable, Quality: e.quality(tx.Source),
				}
				pending = append(pending, ev)
				emissionOrdinal++
			}
		}
	}
	for _, ev := range pending {
		ev.Quality = e.quality(tx.Source)
		if err := e.sink(ctx, ev); err != nil {
			return err
		}
	}
	e.seen[key] = tx.Digest
	return nil
}

func (e *Engine) observeQuality(sourceID string, r lab.Record) {
	switch r.Kind {
	case "continuity_broken", "filter_identity_changed", "replay_retention_exceeded",
		"replay_rejected", "dedupe_conflict", "dedupe_limit", "checkpoint_write_failed", "unreadable_update", "handler_error", "evidence_error":
		e.qualityMu.Lock()
		e.broken[sourceID] = true
		e.qualityMu.Unlock()
	}
}

func (e *Engine) quality(sourceID string) string {
	e.qualityMu.RLock()
	broken := e.broken[sourceID]
	e.qualityMu.RUnlock()
	if broken {
		return "CONTINUITY_BROKEN"
	}
	return "OBSERVING_UNVERIFIED"
}
