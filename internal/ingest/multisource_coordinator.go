package ingest

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MultiSourceCoordinator is an in-memory, deterministic evidence state machine.
// A mutex serializes observations from independently reconnecting sources.
// The caller owns connection loops, per-source ordered durable ingestion,
// replay and external completeness oracles. No observation
// here can admit exact transaction, account or ordered-effect completeness.
// In particular, a block footer, block_meta or sparse slot progression is not
// a bank-completion witness.
type MultiSourceCoordinator struct {
	mu      sync.Mutex
	sources map[string]*multiSourceState
	order   []string
	limit   int
	issues  []MultiSourceIssue
	seen    map[string]struct{}
}

type MultiSourceSpec struct {
	ID            string
	FailureDomain string // Same provider/upstream means the same failure domain.
	Identity      string // Source-specific subscription identity (may include endpoint).
	Commitment    string
	FilterVersion string // Must identify an equivalent logical filter across sources.
}

type MultiSourceKind string

const (
	MultiSourceAttempt        MultiSourceKind = "connection_attempt"
	MultiSourceConnect        MultiSourceKind = "connect"
	MultiSourceDisconnect     MultiSourceKind = "disconnect"
	MultiSourceEpochReset     MultiSourceKind = "epoch_reset"
	MultiSourceGap            MultiSourceKind = "explicit_gap"
	MultiSourceFailure        MultiSourceKind = "failure"
	MultiSourceConflictSignal MultiSourceKind = "conflict_signal"
	MultiSourceProgress       MultiSourceKind = "slot_progress"
	MultiSourceBlockMeta      MultiSourceKind = "block_meta"
	MultiSourceBlockFooter    MultiSourceKind = "block_footer"
	MultiSourceTransaction    MultiSourceKind = "transaction"
	MultiSourceAccount        MultiSourceKind = "account"
	MultiSourceBankClosed     MultiSourceKind = "bank_closed"
	MultiSourcePartialBank    MultiSourceKind = "partial_bank"
)

// MultiSourceBankClosed requires an explicit upstream lifecycle witness. A
// Yellowstone block_footer may precede transactions and MUST be mapped to
// MultiSourceBlockFooter instead. Even a closed bank remains UNVERIFIED until
// an independent full transaction/account history oracle is implemented.
type MultiSourceObservation struct {
	SourceID      string
	Generation    uint64
	Kind          MultiSourceKind
	Identity      string // Optional assertion; mismatches fail closed.
	Commitment    string // Optional assertion; mismatches fail closed.
	FilterVersion string // Optional assertion; mismatches fail closed.
	Slot          uint64
	BankID        uint64 // Source-local: never compared between providers.
	ParentSlot    uint64
	Blockhash     string
	TxIndex       uint64
	Signature     string
	Digest        string
	Account       string
	WriteVersion  uint64
	GapFrom       uint64
	GapTo         uint64
	Reason        string
}

type MultiSourceQuality string

const (
	MultiSourceUnverified MultiSourceQuality = "OBSERVING_UNVERIFIED"
	MultiSourceBroken     MultiSourceQuality = "CONTINUITY_BROKEN"
	MultiSourceConflict   MultiSourceQuality = "CONFLICT"
)

type MultiSourceIssue struct {
	Kind    string   `json:"kind"`
	Sources []string `json:"sources"`
	Slot    uint64   `json:"slot,omitempty"`
	GapFrom uint64   `json:"gap_from,omitempty"`
	GapTo   uint64   `json:"gap_to,omitempty"`
	Detail  string   `json:"detail,omitempty"`
}

type MultiSourceDecision struct {
	// Processed means the observation changed evidence. It is never exact admission.
	Processed bool               `json:"processed"`
	Duplicate bool               `json:"duplicate"`
	Quality   MultiSourceQuality `json:"quality"`
	Issues    []MultiSourceIssue `json:"issues,omitempty"`
}

// All frontiers below are high-water observations in the current evidence
// epoch, NOT contiguous coverage. Verified frontiers intentionally stay zero.
type MultiSourceHealth struct {
	ID                    string             `json:"id"`
	FailureDomain         string             `json:"failure_domain"`
	Identity              string             `json:"identity"`
	Commitment            string             `json:"commitment"`
	FilterVersion         string             `json:"filter_version"`
	Epoch                 uint64             `json:"epoch"`
	Generation            uint64             `json:"generation"`
	Connected             bool               `json:"connected"`
	ContinuityBroken      bool               `json:"continuity_broken"`
	Conflict              bool               `json:"conflict"`
	Quality               MultiSourceQuality `json:"quality"`
	MaxObservedSlot       uint64             `json:"max_observed_slot"`
	MaxTransactionSlot    uint64             `json:"max_transaction_slot"`
	MaxAccountSlot        uint64             `json:"max_account_slot"`
	MaxMetaSlot           uint64             `json:"max_meta_slot"`
	MaxReportedClosedSlot uint64             `json:"max_reported_closed_slot"`
	Connects              uint64             `json:"connects"`
	Disconnects           uint64             `json:"disconnects"`
	Duplicates            uint64             `json:"duplicates"`
	PartialBanks          uint64             `json:"partial_banks"`
	ExplicitGaps          uint64             `json:"explicit_gaps"`
	EvidenceEvicted       uint64             `json:"evidence_evicted"`
}

type MultiSourceBankEvidence struct {
	SourceID      string `json:"source_id"`
	Epoch         uint64 `json:"epoch"`
	Generation    uint64 `json:"generation"`
	Slot          uint64 `json:"slot"`
	BankID        uint64 `json:"bank_id,omitempty"`
	MetaSeen      bool   `json:"meta_seen"`
	FooterSeen    bool   `json:"footer_seen"`
	ClosureSeen   bool   `json:"closure_seen"`
	Partial       bool   `json:"partial"`
	Blockhash     string `json:"blockhash,omitempty"`
	ParentSlot    uint64 `json:"parent_slot,omitempty"`
	Transactions  int    `json:"transactions"`
	AccountWrites int    `json:"account_writes"`
}

// Agreements compare observed anchors only, never complete histories. Even
// an agreement across independent failure domains is limited corroboration.
type MultiSourceAgreement struct {
	Slot                      uint64 `json:"slot"`
	SourceA                   string `json:"source_a"`
	SourceB                   string `json:"source_b"`
	IndependentFailureDomains bool   `json:"independent_failure_domains"`
	AnchorsAgree              bool   `json:"anchors_agree"`
	BothReportedClosed        bool   `json:"both_reported_closed"`
	ObservedSetsComparable    bool   `json:"observed_sets_comparable"`
	ObservedTxSetsEqual       bool   `json:"observed_tx_sets_equal"`
	// write_version and the account protobuf digest are source-local. No
	// cross-provider account set comparison is admitted without a normalized
	// physical-state/write-occurrence witness.
	ObservedAccountSetsComparable bool `json:"observed_account_sets_comparable"`
	ObservedAccountSetsEqual      bool `json:"observed_account_sets_equal"`
}

type MultiSourceSnapshot struct {
	Quality                       MultiSourceQuality        `json:"quality"`
	Sources                       []MultiSourceHealth       `json:"sources"`
	Banks                         []MultiSourceBankEvidence `json:"banks"`
	Agreements                    []MultiSourceAgreement    `json:"agreements"`
	Issues                        []MultiSourceIssue        `json:"issues"`
	DeclaredFailureDomains        int                       `json:"declared_failure_domains"`
	ConnectedFailureDomains       int                       `json:"connected_failure_domains"`
	TxInclusionVerifiedThrough    uint64                    `json:"tx_inclusion_verified_through"`
	AccountHistoryVerifiedThrough uint64                    `json:"account_history_verified_through"`
	TransactionInclusionAdmission string                    `json:"transaction_inclusion_admission"`
	AccountHistoryAdmission       string                    `json:"account_history_admission"`
	OrderedEffectsAdmission       string                    `json:"ordered_effects_admission"`
}

type multiSourceState struct {
	spec                MultiSourceSpec
	health              MultiSourceHealth
	pendingAttempt      bool
	banks               map[uint64]*multiSourceBank
	bankOrder           []uint64
	lastResetGeneration uint64
}

type multiSourceTransaction struct {
	signature string
	digest    string
}

type multiSourceBank struct {
	slot, generation, bankID, parent uint64
	blockhash, footerDigest          string
	meta, footer, closed, partial    bool
	tx                               map[uint64]multiSourceTransaction
	accounts                         map[string]string
}

func NewMultiSourceCoordinator(specs []MultiSourceSpec, maxBanksPerSource int) (*MultiSourceCoordinator, error) {
	if len(specs) == 0 || maxBanksPerSource < 1 {
		return nil, errors.New("at least one source and a positive bank evidence limit required")
	}
	c := &MultiSourceCoordinator{sources: make(map[string]*multiSourceState, len(specs)), limit: maxBanksPerSource, seen: make(map[string]struct{})}
	for _, spec := range specs {
		if strings.TrimSpace(spec.ID) == "" || strings.TrimSpace(spec.FailureDomain) == "" || spec.Identity == "" || spec.FilterVersion == "" || spec.Commitment == "" {
			return nil, fmt.Errorf("source %q requires ID, failure domain, identity, commitment and filter version", spec.ID)
		}
		if _, ok := c.sources[spec.ID]; ok {
			return nil, fmt.Errorf("duplicate source ID %q", spec.ID)
		}
		c.sources[spec.ID] = &multiSourceState{spec: spec, health: MultiSourceHealth{ID: spec.ID, FailureDomain: spec.FailureDomain, Identity: spec.Identity, Commitment: spec.Commitment, FilterVersion: spec.FilterVersion, Epoch: 1, Quality: MultiSourceUnverified}, banks: make(map[uint64]*multiSourceBank)}
		c.order = append(c.order, spec.ID)
	}
	sort.Strings(c.order)
	return c, nil
}

// Observe never promotes evidence to exact quality. Invalid input is rejected;
// a subscription identity mismatch additionally latches a continuity failure.
func (c *MultiSourceCoordinator) Observe(o MultiSourceObservation) (MultiSourceDecision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := MultiSourceDecision{Quality: c.quality()}
	s := c.sources[o.SourceID]
	if s == nil {
		return result, fmt.Errorf("unknown source %q", o.SourceID)
	}
	if o.Generation == 0 {
		return result, errors.New("generation must be positive")
	}
	if err := validateMultiSourceObservation(o); err != nil {
		return result, err
	}
	if o.Kind == MultiSourceAttempt {
		if o.Generation <= s.health.Generation {
			return result, errors.New("connection attempt must advance generation")
		}
	} else if o.Kind == MultiSourceConnect {
		if o.Generation < s.health.Generation || (o.Generation == s.health.Generation && !s.health.Connected && !s.pendingAttempt) {
			return result, errors.New("stale or reused connection generation")
		}
	} else if o.Kind != MultiSourceEpochReset && o.Generation != s.health.Generation {
		return result, errors.New("observation from non-current generation")
	}
	if o.Kind != MultiSourceEpochReset && ((o.Identity != "" && o.Identity != s.health.Identity) || (o.Commitment != "" && o.Commitment != s.health.Commitment) || (o.FilterVersion != "" && o.FilterVersion != s.health.FilterVersion)) {
		c.breakSource(s, &result, "subscription_identity_mismatch", o.Slot, "observation does not match registered source subscription")
		result.Quality = c.quality()
		return result, errors.New("subscription identity mismatch; explicit epoch reset required")
	}
	if o.Kind == MultiSourceEpochReset {
		if o.Generation != s.health.Generation || s.health.Generation == 0 || s.lastResetGeneration == o.Generation {
			return result, errors.New("epoch reset must refer to current generation")
		}
		c.markOpenPartial(s, &result)
		c.breakSource(s, &result, "epoch_reset", 0, o.Reason)
		s.health.Epoch++
		s.lastResetGeneration = o.Generation
		s.health.Connected = false
		s.pendingAttempt = false
		s.health.MaxObservedSlot, s.health.MaxTransactionSlot, s.health.MaxAccountSlot = 0, 0, 0
		s.health.MaxMetaSlot, s.health.MaxReportedClosedSlot = 0, 0
		if o.Identity != "" {
			s.health.Identity = o.Identity
		}
		if o.Commitment != "" {
			s.health.Commitment = o.Commitment
		}
		if o.FilterVersion != "" {
			s.health.FilterVersion = o.FilterVersion
		}
		s.banks = make(map[uint64]*multiSourceBank)
		s.bankOrder = nil
		result.Processed = true
		result.Quality = c.quality()
		return result, nil
	}
	if o.Kind == MultiSourceAttempt {
		if s.health.Connected {
			c.markOpenPartial(s, &result)
			c.breakSource(s, &result, "implicit_disconnect", 0, "new connection attempt before previous connection ended")
		}
		s.health.Generation = o.Generation
		s.health.Connected = false
		s.pendingAttempt = true
		s.banks = make(map[uint64]*multiSourceBank)
		s.bankOrder = nil
		result.Processed = true
		result.Quality = c.quality()
		return result, nil
	}
	if o.Kind == MultiSourceConnect {
		if o.Generation < s.health.Generation || (o.Generation == s.health.Generation && !s.health.Connected && !s.pendingAttempt) {
			return result, errors.New("stale or reused connection generation")
		}
		if o.Generation == s.health.Generation && s.health.Connected {
			s.health.Duplicates++
			result.Duplicate = true
		} else {
			if s.health.Connected {
				c.markOpenPartial(s, &result)
				c.breakSource(s, &result, "implicit_disconnect", 0, "generation advanced without disconnect")
			}
			s.health.Generation = o.Generation
			s.health.Connected = true
			s.pendingAttempt = false
			s.health.Connects++
			s.banks = make(map[uint64]*multiSourceBank)
			s.bankOrder = nil
			result.Processed = true
		}
		result.Quality = c.quality()
		return result, nil
	}
	if o.Generation != s.health.Generation {
		return result, errors.New("observation from non-current generation")
	}
	if o.Kind == MultiSourceDisconnect {
		if !s.health.Connected {
			if s.pendingAttempt {
				s.pendingAttempt = false
				s.health.Disconnects++
				c.breakSource(s, &result, "connection_failed_before_open", 0, o.Reason)
				result.Processed = true
			} else {
				s.health.Duplicates++
				result.Duplicate = true
			}
		} else {
			s.health.Connected = false
			s.health.Disconnects++
			c.markOpenPartial(s, &result)
			c.breakSource(s, &result, "disconnect", 0, o.Reason)
			result.Processed = true
		}
		result.Quality = c.quality()
		return result, nil
	}
	if o.Kind != MultiSourceGap && o.Kind != MultiSourceFailure && o.Kind != MultiSourceConflictSignal && !s.health.Connected {
		return result, errors.New("data observation requires an active connection")
	}
	if o.Kind == MultiSourceGap {
		s.health.ExplicitGaps++
		c.issue(&result, MultiSourceIssue{Kind: "explicit_gap", Sources: []string{s.spec.ID}, GapFrom: o.GapFrom, GapTo: o.GapTo, Detail: o.Reason})
		s.health.ContinuityBroken = true
		result.Processed = true
	} else if o.Kind == MultiSourceFailure || o.Kind == MultiSourceConflictSignal {
		if o.Kind == MultiSourceConflictSignal {
			c.conflictSource(s, &result, "external_conflict", o.Slot, o.Reason)
		} else {
			c.breakSource(s, &result, "source_failure", o.Slot, o.Reason)
		}
		result.Processed = true
	} else if o.Kind == MultiSourceProgress {
		s.health.MaxObservedSlot = max(s.health.MaxObservedSlot, o.Slot)
		result.Processed = true
	} else {
		s.health.MaxObservedSlot = max(s.health.MaxObservedSlot, o.Slot)
		b := c.bank(s, o.Slot, &result)
		bankIDConflict := b.bankID != 0 && o.BankID != 0 && b.bankID != o.BankID
		if bankIDConflict {
			c.conflictSource(s, &result, "source_bank_id_conflict", o.Slot, "different nonzero bank IDs within one connection")
		} else if b.bankID == 0 {
			b.bankID = o.BankID
		}
		switch o.Kind {
		case MultiSourceBlockMeta:
			s.health.MaxMetaSlot = max(s.health.MaxMetaSlot, o.Slot)
			if b.meta && (b.blockhash != o.Blockhash || b.parent != o.ParentSlot) {
				c.conflictSource(s, &result, "source_anchor_conflict", o.Slot, "same source/slot, conflicting blockhash or parent")
			} else if b.meta {
				if !bankIDConflict {
					s.health.Duplicates++
					result.Duplicate = true
				}
			} else {
				b.meta, b.blockhash, b.parent = true, o.Blockhash, o.ParentSlot
			}
		case MultiSourceBlockFooter:
			if b.footer {
				if b.footerDigest != "" && o.Digest != "" && b.footerDigest != o.Digest {
					c.conflictSource(s, &result, "source_footer_conflict", o.Slot, "same source/slot has different footer payload digests")
				} else if !bankIDConflict {
					s.health.Duplicates++
					result.Duplicate = true
				}
			}
			b.footer = true
			if b.footerDigest == "" {
				b.footerDigest = o.Digest
			}
		case MultiSourceTransaction:
			s.health.MaxTransactionSlot = max(s.health.MaxTransactionSlot, o.Slot)
			v, ok := b.tx[o.TxIndex]
			if ok && (v.signature != o.Signature || v.digest != o.Digest) {
				c.conflictSource(s, &result, "source_transaction_conflict", o.Slot, fmt.Sprintf("transaction index %d has different signature/digest", o.TxIndex))
			} else if ok {
				s.health.Duplicates++
				result.Duplicate = true
			} else {
				if b.closed {
					c.partialBank(s, b, &result, "transaction_after_close")
				}
				b.tx[o.TxIndex] = multiSourceTransaction{o.Signature, o.Digest}
			}
		case MultiSourceAccount:
			s.health.MaxAccountSlot = max(s.health.MaxAccountSlot, o.Slot)
			key := fmt.Sprintf("%s|%d", o.Account, o.WriteVersion)
			v, ok := b.accounts[key]
			if ok && v != o.Digest {
				c.conflictSource(s, &result, "source_account_conflict", o.Slot, "same account/write version has different digest")
			} else if ok {
				s.health.Duplicates++
				result.Duplicate = true
			} else {
				if b.closed {
					c.partialBank(s, b, &result, "account_after_close")
				}
				b.accounts[key] = o.Digest
			}
		case MultiSourceBankClosed:
			s.health.MaxReportedClosedSlot = max(s.health.MaxReportedClosedSlot, o.Slot)
			if b.closed {
				s.health.Duplicates++
				result.Duplicate = true
			}
			b.closed = true
		case MultiSourcePartialBank:
			c.partialBank(s, b, &result, o.Reason)
		}
		c.compareBank(s, b, &result)
		result.Processed = !result.Duplicate || len(result.Issues) > 0
	}
	result.Quality = c.quality()
	return result, nil
}

func validateMultiSourceObservation(o MultiSourceObservation) error {
	switch o.Kind {
	case MultiSourceAttempt, MultiSourceConnect, MultiSourceDisconnect, MultiSourceEpochReset, MultiSourceFailure, MultiSourceConflictSignal:
		return nil
	case MultiSourceGap:
		if o.GapFrom == 0 || o.GapTo < o.GapFrom {
			return errors.New("explicit gap requires valid nonzero [from,to]")
		}
	case MultiSourceProgress, MultiSourceBlockFooter, MultiSourceBankClosed, MultiSourcePartialBank:
		if o.Slot == 0 {
			return errors.New("slot must be nonzero")
		}
	case MultiSourceBlockMeta:
		if o.Slot == 0 || o.Blockhash == "" {
			return errors.New("block meta requires slot and blockhash")
		}
	case MultiSourceTransaction:
		if o.Slot == 0 || o.Signature == "" || o.Digest == "" {
			return errors.New("transaction requires slot, signature and digest")
		}
	case MultiSourceAccount:
		if o.Slot == 0 || o.Account == "" || o.Digest == "" {
			return errors.New("account requires slot, account and digest")
		}
	default:
		return fmt.Errorf("unknown observation kind %q", o.Kind)
	}
	return nil
}

func (c *MultiSourceCoordinator) bank(s *multiSourceState, slot uint64, result *MultiSourceDecision) *multiSourceBank {
	if b := s.banks[slot]; b != nil {
		return b
	}
	if len(s.bankOrder) >= c.limit {
		old := s.bankOrder[0]
		if prior := s.banks[old]; prior != nil && !prior.closed {
			c.partialBank(s, prior, result, "unclosed bank evidence evicted by bounded cache")
		}
		s.bankOrder = s.bankOrder[1:]
		delete(s.banks, old)
		s.health.EvidenceEvicted++
	}
	b := &multiSourceBank{slot: slot, generation: s.health.Generation, tx: make(map[uint64]multiSourceTransaction), accounts: make(map[string]string)}
	s.banks[slot] = b
	s.bankOrder = append(s.bankOrder, slot)
	return b
}

func (c *MultiSourceCoordinator) markOpenPartial(s *multiSourceState, result *MultiSourceDecision) {
	for _, slot := range s.bankOrder {
		b := s.banks[slot]
		if !b.closed {
			c.partialBank(s, b, result, "incomplete bank at connection boundary")
		}
	}
}

func (c *MultiSourceCoordinator) partialBank(s *multiSourceState, b *multiSourceBank, result *MultiSourceDecision, reason string) {
	if b.partial {
		return
	}
	b.partial = true
	s.health.PartialBanks++
	c.breakSource(s, result, "partial_bank", b.slot, reason)
}

func (c *MultiSourceCoordinator) breakSource(s *multiSourceState, result *MultiSourceDecision, kind string, slot uint64, detail string) {
	s.health.ContinuityBroken = true
	c.issue(result, MultiSourceIssue{Kind: kind, Sources: []string{s.spec.ID}, Slot: slot, Detail: detail})
}

func (c *MultiSourceCoordinator) conflictSource(s *multiSourceState, result *MultiSourceDecision, kind string, slot uint64, detail string) {
	s.health.Conflict = true
	c.breakSource(s, result, kind, slot, detail)
}

func (c *MultiSourceCoordinator) compareBank(s *multiSourceState, b *multiSourceBank, result *MultiSourceDecision) {
	for _, otherID := range c.order {
		if otherID == s.spec.ID {
			continue
		}
		other := c.sources[otherID]
		if other.health.Commitment != s.health.Commitment || other.health.FilterVersion != s.health.FilterVersion {
			continue
		}
		ob := other.banks[b.slot]
		if ob == nil {
			continue
		}
		if b.meta && ob.meta && (b.blockhash != ob.blockhash || b.parent != ob.parent) {
			c.crossConflict(s, other, result, "cross_source_anchor_conflict", b.slot, "same slot, different blockhash or parent")
		}
		indices := make([]uint64, 0, len(b.tx))
		for idx := range b.tx {
			indices = append(indices, idx)
		}
		sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
		for _, idx := range indices {
			tx := b.tx[idx]
			if otherTx, ok := ob.tx[idx]; ok && tx != otherTx {
				c.crossConflict(s, other, result, "cross_source_transaction_conflict", b.slot, fmt.Sprintf("transaction index %d differs", idx))
			}
		}
		// Never use source-local write_version or raw account protobuf digests
		// as comparable identities between independent providers.
		if b.closed && ob.closed && !b.partial && !ob.partial && !multiSourceTxEqual(b.tx, ob.tx) {
			c.crossConflict(s, other, result, "closed_bank_tx_set_divergence", b.slot, "reported closed banks contain different observed filtered transaction sets")
		}
	}
}

func multiSourceTxEqual(a, b map[uint64]multiSourceTransaction) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func (c *MultiSourceCoordinator) crossConflict(a, b *multiSourceState, result *MultiSourceDecision, kind string, slot uint64, detail string) {
	a.health.Conflict, a.health.ContinuityBroken = true, true
	b.health.Conflict, b.health.ContinuityBroken = true, true
	c.issue(result, MultiSourceIssue{Kind: kind, Sources: []string{a.spec.ID, b.spec.ID}, Slot: slot, Detail: detail})
}

func (c *MultiSourceCoordinator) issue(result *MultiSourceDecision, issue MultiSourceIssue) {
	sort.Strings(issue.Sources)
	key := multiSourceIssueKey(issue)
	if _, ok := c.seen[key]; ok {
		return
	}
	c.seen[key] = struct{}{}
	const maxIssues = 256
	if len(c.issues) >= maxIssues {
		delete(c.seen, multiSourceIssueKey(c.issues[0]))
		c.issues = c.issues[1:]
	}
	c.issues = append(c.issues, issue)
	result.Issues = append(result.Issues, issue)
}

func multiSourceIssueKey(issue MultiSourceIssue) string {
	return fmt.Sprintf("%s|%s|%d|%d|%d|%s", issue.Kind, strings.Join(issue.Sources, ","), issue.Slot, issue.GapFrom, issue.GapTo, issue.Detail)
}

func (c *MultiSourceCoordinator) quality() MultiSourceQuality {
	q := MultiSourceUnverified
	for _, s := range c.sources {
		if s.health.Conflict {
			return MultiSourceConflict
		}
		if s.health.ContinuityBroken {
			q = MultiSourceBroken
		}
	}
	return q
}

func (c *MultiSourceCoordinator) currentQuality() MultiSourceQuality {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.quality()
}

func (c *MultiSourceCoordinator) Snapshot() MultiSourceSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := MultiSourceSnapshot{Quality: c.quality(), TransactionInclusionAdmission: "UNVERIFIED", AccountHistoryAdmission: "UNVERIFIED", OrderedEffectsAdmission: "UNVERIFIED"}
	declared, connected := make(map[string]struct{}), make(map[string]struct{})
	for _, id := range c.order {
		s := c.sources[id]
		h := s.health
		h.Quality = MultiSourceUnverified
		if h.ContinuityBroken {
			h.Quality = MultiSourceBroken
		}
		if h.Conflict {
			h.Quality = MultiSourceConflict
		}
		out.Sources = append(out.Sources, h)
		declared[h.FailureDomain] = struct{}{}
		if h.Connected {
			connected[h.FailureDomain] = struct{}{}
		}
		for _, slot := range s.bankOrder {
			b := s.banks[slot]
			out.Banks = append(out.Banks, MultiSourceBankEvidence{SourceID: id, Epoch: h.Epoch, Generation: b.generation, Slot: b.slot, BankID: b.bankID, MetaSeen: b.meta, FooterSeen: b.footer, ClosureSeen: b.closed, Partial: b.partial, Blockhash: b.blockhash, ParentSlot: b.parent, Transactions: len(b.tx), AccountWrites: len(b.accounts)})
		}
	}
	out.DeclaredFailureDomains, out.ConnectedFailureDomains = len(declared), len(connected)
	for i, idA := range c.order {
		a := c.sources[idA]
		for _, idB := range c.order[i+1:] {
			b := c.sources[idB]
			if a.health.Commitment != b.health.Commitment || a.health.FilterVersion != b.health.FilterVersion {
				continue
			}
			for _, slot := range a.bankOrder {
				x, y := a.banks[slot], b.banks[slot]
				if y == nil || !x.meta || !y.meta {
					continue
				}
				comparable := x.closed && y.closed && !x.partial && !y.partial
				out.Agreements = append(out.Agreements, MultiSourceAgreement{Slot: slot, SourceA: idA, SourceB: idB, IndependentFailureDomains: a.spec.FailureDomain != b.spec.FailureDomain, AnchorsAgree: x.blockhash == y.blockhash && x.parent == y.parent, BothReportedClosed: x.closed && y.closed, ObservedSetsComparable: comparable, ObservedTxSetsEqual: comparable && multiSourceTxEqual(x.tx, y.tx), ObservedAccountSetsComparable: false, ObservedAccountSetsEqual: false})
			}
		}
	}
	sort.Slice(out.Banks, func(i, j int) bool {
		if out.Banks[i].Slot == out.Banks[j].Slot {
			return out.Banks[i].SourceID < out.Banks[j].SourceID
		}
		return out.Banks[i].Slot < out.Banks[j].Slot
	})
	sort.Slice(out.Agreements, func(i, j int) bool {
		if out.Agreements[i].Slot != out.Agreements[j].Slot {
			return out.Agreements[i].Slot < out.Agreements[j].Slot
		}
		if out.Agreements[i].SourceA != out.Agreements[j].SourceA {
			return out.Agreements[i].SourceA < out.Agreements[j].SourceA
		}
		return out.Agreements[i].SourceB < out.Agreements[j].SourceB
	})
	for _, issue := range c.issues {
		issue.Sources = append([]string(nil), issue.Sources...)
		out.Issues = append(out.Issues, issue)
	}
	return out
}
