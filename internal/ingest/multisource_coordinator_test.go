package ingest

import (
	"sync"
	"testing"
)

func multiSourceSpecs(secondDomain string) []MultiSourceSpec {
	return []MultiSourceSpec{
		{ID: "A", FailureDomain: "publicnode", Identity: "endpoint-a", Commitment: "finalized", FilterVersion: "pump-v1"},
		{ID: "B", FailureDomain: secondDomain, Identity: "endpoint-b", Commitment: "finalized", FilterVersion: "pump-v1"},
	}
}

func mustMultiSource(t *testing.T, specs []MultiSourceSpec, limit int) *MultiSourceCoordinator {
	t.Helper()
	c, err := NewMultiSourceCoordinator(specs, limit)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func observeMultiSource(t *testing.T, c *MultiSourceCoordinator, o MultiSourceObservation) MultiSourceDecision {
	t.Helper()
	d, err := c.Observe(o)
	if err != nil {
		t.Fatalf("observe %+v: %v", o, err)
	}
	return d
}

func connectMultiSource(t *testing.T, c *MultiSourceCoordinator, id string, generation uint64) {
	t.Helper()
	observeMultiSource(t, c, MultiSourceObservation{SourceID: id, Generation: generation, Kind: MultiSourceConnect})
}

func metaMultiSource(t *testing.T, c *MultiSourceCoordinator, id string, generation, slot, bankID, parent uint64, hash string) MultiSourceDecision {
	t.Helper()
	return observeMultiSource(t, c, MultiSourceObservation{SourceID: id, Generation: generation, Kind: MultiSourceBlockMeta, Slot: slot, BankID: bankID, ParentSlot: parent, Blockhash: hash})
}

func txMultiSource(t *testing.T, c *MultiSourceCoordinator, id string, generation, slot, index uint64, sig, digest string) MultiSourceDecision {
	t.Helper()
	return observeMultiSource(t, c, MultiSourceObservation{SourceID: id, Generation: generation, Kind: MultiSourceTransaction, Slot: slot, TxIndex: index, Signature: sig, Digest: digest})
}

func healthMultiSource(t *testing.T, c *MultiSourceCoordinator, id string) MultiSourceHealth {
	t.Helper()
	for _, h := range c.Snapshot().Sources {
		if h.ID == id {
			return h
		}
	}
	t.Fatalf("source %s not found", id)
	return MultiSourceHealth{}
}

func assertNoExactPromotion(t *testing.T, snap MultiSourceSnapshot) {
	t.Helper()
	if snap.Quality != MultiSourceUnverified && snap.Quality != MultiSourceBroken && snap.Quality != MultiSourceConflict {
		t.Fatalf("unrecognized quality: %s", snap.Quality)
	}
	if snap.TxInclusionVerifiedThrough != 0 || snap.AccountHistoryVerifiedThrough != 0 || snap.TransactionInclusionAdmission != "UNVERIFIED" || snap.AccountHistoryAdmission != "UNVERIFIED" || snap.OrderedEffectsAdmission != "UNVERIFIED" {
		t.Fatalf("coordinator silently claimed exact admission: %+v", snap)
	}
}

func TestMultiSourceFailureDomainsAreNotConnections(t *testing.T) {
	for _, tc := range []struct {
		name, secondDomain string
		wantDomains        int
		independent        bool
	}{
		{"two_connections_one_provider", "publicnode", 1, false},
		{"two_independent_providers", "second-provider", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := mustMultiSource(t, multiSourceSpecs(tc.secondDomain), 16)
			connectMultiSource(t, c, "A", 1)
			connectMultiSource(t, c, "B", 1)
			metaMultiSource(t, c, "A", 1, 101, 111, 100, "hash101")
			metaMultiSource(t, c, "B", 1, 101, 999, 100, "hash101") // BankID is source-local.
			txMultiSource(t, c, "A", 1, 101, 4, "sig", "digest")
			txMultiSource(t, c, "B", 1, 101, 4, "sig", "digest")
			for _, id := range []string{"A", "B"} {
				observeMultiSource(t, c, MultiSourceObservation{SourceID: id, Generation: 1, Kind: MultiSourceBankClosed, Slot: 101})
			}
			s := c.Snapshot()
			if s.Quality != MultiSourceUnverified || s.DeclaredFailureDomains != tc.wantDomains || s.ConnectedFailureDomains != tc.wantDomains {
				t.Fatalf("incorrect independent-domain health: %+v", s)
			}
			if len(s.Agreements) != 1 || !s.Agreements[0].AnchorsAgree || !s.Agreements[0].BothReportedClosed || !s.Agreements[0].ObservedTxSetsEqual || s.Agreements[0].IndependentFailureDomains != tc.independent {
				t.Fatalf("agreement is scoped incorrectly: %+v", s.Agreements)
			}
			assertNoExactPromotion(t, s)
		})
	}
}

func TestMultiSourceSameBankConflictsLatchBothSources(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 16)
	connectMultiSource(t, c, "A", 1)
	connectMultiSource(t, c, "B", 1)
	metaMultiSource(t, c, "A", 1, 200, 11, 199, "canonical-a")
	d := metaMultiSource(t, c, "B", 1, 200, 22, 199, "canonical-b")
	if d.Quality != MultiSourceConflict || len(d.Issues) != 1 || d.Issues[0].Kind != "cross_source_anchor_conflict" {
		t.Fatalf("same-slot conflict must be explicit: %+v", d)
	}
	for _, id := range []string{"A", "B"} {
		h := healthMultiSource(t, c, id)
		if !h.Conflict || !h.ContinuityBroken {
			t.Fatalf("conflict not latched for %s: %+v", id, h)
		}
	}
	// Repeating the matching anchor cannot silently heal a fork conflict.
	metaMultiSource(t, c, "A", 1, 200, 11, 199, "canonical-a")
	if c.Snapshot().Quality != MultiSourceConflict {
		t.Fatal("conflict was silently promoted")
	}
	assertNoExactPromotion(t, c.Snapshot())
}

func TestMultiSourceTransactionAndClosedSetDivergence(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("publicnode"), 16)
	connectMultiSource(t, c, "A", 1)
	connectMultiSource(t, c, "B", 1)
	metaMultiSource(t, c, "A", 1, 300, 11, 299, "hash")
	metaMultiSource(t, c, "B", 1, 300, 12, 299, "hash")
	txMultiSource(t, c, "A", 1, 300, 0, "sig0", "digest0")
	txMultiSource(t, c, "B", 1, 300, 0, "sig0", "digest0")
	d := txMultiSource(t, c, "A", 1, 300, 0, "sig0", "digest0")
	if !d.Duplicate || healthMultiSource(t, c, "A").Duplicates != 1 {
		t.Fatalf("duplicate not idempotent: %+v", d)
	}
	txMultiSource(t, c, "A", 1, 300, 1, "sig1", "digest1")
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceBankClosed, Slot: 300})
	if c.Snapshot().Quality != MultiSourceUnverified {
		t.Fatal("one reported closure cannot prove a gap")
	}
	d = observeMultiSource(t, c, MultiSourceObservation{SourceID: "B", Generation: 1, Kind: MultiSourceBankClosed, Slot: 300})
	if d.Quality != MultiSourceConflict || len(d.Issues) != 1 || d.Issues[0].Kind != "closed_bank_tx_set_divergence" {
		t.Fatalf("different observed transaction sets behind closed banks must conflict: %+v", d)
	}
	assertNoExactPromotion(t, c.Snapshot())

	other := mustMultiSource(t, multiSourceSpecs("second-provider"), 16)
	connectMultiSource(t, other, "A", 1)
	connectMultiSource(t, other, "B", 1)
	d = txMultiSource(t, other, "A", 1, 301, 0, "sig", "a")
	d = txMultiSource(t, other, "B", 1, 301, 0, "sig", "b")
	if d.Quality != MultiSourceConflict || d.Issues[0].Kind != "cross_source_transaction_conflict" {
		t.Fatalf("same transaction identity with changed digest must conflict: %+v", d)
	}
}

func TestMultiSourceSparseSlotsNeverImplyCoverageOrGap(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider")[:1], 16)
	connectMultiSource(t, c, "A", 1)
	for _, slot := range []uint64{101, 106, 109, 400} {
		observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceProgress, Slot: slot})
	}
	txMultiSource(t, c, "A", 1, 106, 8, "sig", "digest")
	h := healthMultiSource(t, c, "A")
	if h.MaxObservedSlot != 400 || h.MaxTransactionSlot != 106 || h.ExplicitGaps != 0 || h.ContinuityBroken || len(c.Snapshot().Issues) != 0 {
		t.Fatalf("sparse filtered slots falsely inferred loss: %+v", h)
	}
	assertNoExactPromotion(t, c.Snapshot())
	d := observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceGap, GapFrom: 107, GapTo: 108, Reason: "independently established missing history"})
	if d.Quality != MultiSourceBroken || d.Issues[0].Kind != "explicit_gap" || healthMultiSource(t, c, "A").ExplicitGaps != 1 {
		t.Fatalf("explicit gap must latch: %+v", d)
	}
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceProgress, Slot: 500})
	if c.Snapshot().Quality != MultiSourceBroken {
		t.Fatal("new observations silently healed known gap")
	}
}

func TestMultiSourceEvictionMarksOpenBankPartial(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider")[:1], 1)
	connectMultiSource(t, c, "A", 1)
	txMultiSource(t, c, "A", 1, 100, 0, "sig100", "digest100")
	d := txMultiSource(t, c, "A", 1, 101, 0, "sig101", "digest101")
	h := healthMultiSource(t, c, "A")
	if d.Quality != MultiSourceBroken || h.EvidenceEvicted != 1 || h.PartialBanks != 1 || !h.ContinuityBroken {
		t.Fatalf("open bank eviction should latch incomplete evidence: decision=%+v health=%+v", d, h)
	}
	if len(d.Issues) != 1 || d.Issues[0].Kind != "partial_bank" || d.Issues[0].Slot != 100 {
		t.Fatalf("missing partial evidence for evicted bank: %+v", d.Issues)
	}
}

func TestMultiSourceEvictionOfClosedBankDoesNotMarkPartial(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider")[:1], 1)
	connectMultiSource(t, c, "A", 1)
	txMultiSource(t, c, "A", 1, 100, 0, "sig100", "digest100")
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceBankClosed, Slot: 100})
	d := txMultiSource(t, c, "A", 1, 101, 0, "sig101", "digest101")
	h := healthMultiSource(t, c, "A")
	if d.Quality != MultiSourceUnverified || h.EvidenceEvicted != 1 || h.PartialBanks != 0 || h.ContinuityBroken {
		t.Fatalf("closed bank eviction should only remove bounded evidence: decision=%+v health=%+v", d, h)
	}
}

func TestMultiSourceDisconnectPartialBankAndReconnect(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 16)
	connectMultiSource(t, c, "A", 1)
	connectMultiSource(t, c, "B", 1)
	metaMultiSource(t, c, "A", 1, 500, 15, 499, "hash500")
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceBlockFooter, Slot: 500, BankID: 15})
	if c.Snapshot().Banks[0].ClosureSeen {
		t.Fatal("footer was incorrectly promoted to bank closure")
	}
	d := observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceDisconnect, Reason: "network drop"})
	if d.Quality != MultiSourceBroken || healthMultiSource(t, c, "A").PartialBanks != 1 || healthMultiSource(t, c, "B").ContinuityBroken {
		t.Fatalf("disconnect should degrade only affected source: %+v", c.Snapshot())
	}
	if !c.Snapshot().Banks[0].Partial {
		t.Fatal("open bank did not become partial")
	}
	connectMultiSource(t, c, "A", 2)
	metaMultiSource(t, c, "A", 2, 501, 17, 500, "hash501")
	metaMultiSource(t, c, "B", 1, 501, 18, 500, "hash501")
	if c.Snapshot().Quality != MultiSourceBroken || healthMultiSource(t, c, "A").Generation != 2 {
		t.Fatal("independent reconnect or sticky break was lost")
	}
	if _, err := c.Observe(MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceProgress, Slot: 600}); err == nil {
		t.Fatal("stale generation accepted")
	}
	assertNoExactPromotion(t, c.Snapshot())
}

func TestMultiSourceAttemptFailsBeforeConnectionOpen(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider")[:1], 16)
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceAttempt})
	if h := healthMultiSource(t, c, "A"); h.Generation != 1 || h.Connected || h.Connects != 0 {
		t.Fatalf("attempt did not create a pending generation: %+v", h)
	}
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceDisconnect, Reason: "dial refused"})
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceFailure, Reason: "connection failed before open"})
	if h := healthMultiSource(t, c, "A"); h.Generation != 1 || h.Connected || !h.ContinuityBroken || h.Disconnects != 1 {
		t.Fatalf("pre-connect failure was not preserved: %+v", h)
	}
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 2, Kind: MultiSourceAttempt})
	connectMultiSource(t, c, "A", 2)
	metaMultiSource(t, c, "A", 2, 901, 0, 900, "hash901")
	if h := healthMultiSource(t, c, "A"); h.Generation != 2 || !h.Connected || h.Connects != 1 || !h.ContinuityBroken {
		t.Fatalf("new generation did not recover transport while keeping sticky uncertainty: %+v", h)
	}
	if _, err := c.Observe(MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceConnect}); err == nil {
		t.Fatal("stale connection was accepted after a newer attempt")
	}
}

func TestMultiSourceGlobalSessionEndClearsLiveConnectivity(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 16)
	connectMultiSource(t, c, "A", 1)
	connectMultiSource(t, c, "B", 1)
	if _, err := c.ObserveRecord(Record{Type: "marker", Kind: "session_end", Detail: "graceful shutdown"}); err != nil {
		t.Fatal(err)
	}
	s := c.Snapshot()
	if s.ConnectedFailureDomains != 0 || s.Quality != MultiSourceUnverified {
		t.Fatalf("offline WAL replay incorrectly reports active live connections: %+v", s)
	}
}

func TestMultiSourcePerWALLifecycleCannotBreakOtherProvider(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 16)
	connectMultiSource(t, c, "A", 1)
	connectMultiSource(t, c, "B", 1)
	if _, err := c.ObserveWALRecord("A", Record{Type: "marker", Kind: "continuity_broken", Detail: "torn WAL tail"}); err != nil {
		t.Fatal(err)
	}
	if !healthMultiSource(t, c, "A").ContinuityBroken || healthMultiSource(t, c, "B").ContinuityBroken {
		t.Fatalf("WAL-local break leaked across sources: %+v", c.Snapshot())
	}
	if _, err := c.ObserveWALRecord("A", Record{Type: "marker", Kind: "session_end", Detail: "graceful shutdown"}); err != nil {
		t.Fatal(err)
	}
	if h := healthMultiSource(t, c, "B"); !h.Connected || h.ContinuityBroken {
		t.Fatalf("WAL-local session end disconnected healthy peer: %+v", h)
	}
	if s := c.Snapshot(); s.ConnectedFailureDomains != 1 || s.Quality != MultiSourceBroken {
		t.Fatalf("WAL lifecycle is not isolated: %+v", s)
	}
	if _, err := c.ObserveWALRecord("A", Record{Source: "B", Generation: 1, Kind: "slot", Slot: 5}); err == nil {
		t.Fatal("cross-source WAL record was accepted")
	}
}

func TestMultiSourceEpochResetAndIdentityMismatch(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 8)
	connectMultiSource(t, c, "A", 1)
	metaMultiSource(t, c, "A", 1, 600, 9, 599, "old")
	if _, err := c.Observe(MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceProgress, Slot: 601, Identity: "unexpected"}); err == nil {
		t.Fatal("identity changed without reset")
	}
	if !healthMultiSource(t, c, "A").ContinuityBroken {
		t.Fatal("identity mismatch did not degrade quality")
	}
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceEpochReset, Identity: "new-identity", FilterVersion: "pump-v2", Reason: "filter changed"})
	h := healthMultiSource(t, c, "A")
	if h.Epoch != 2 || h.Connected || h.MaxMetaSlot != 0 || h.Identity != "new-identity" || h.FilterVersion != "pump-v2" || h.PartialBanks != 1 {
		t.Fatalf("epoch reset did not invalidate old evidence: %+v", h)
	}
	if len(c.Snapshot().Banks) != 0 {
		t.Fatal("old evidence survived epoch reset as current")
	}
	connectMultiSource(t, c, "A", 2)
	metaMultiSource(t, c, "A", 2, 600, 0, 599, "new")
	connectMultiSource(t, c, "B", 1)
	metaMultiSource(t, c, "B", 1, 600, 1, 599, "old")
	if len(c.Snapshot().Agreements) != 0 || c.Snapshot().Quality != MultiSourceBroken {
		t.Fatal("different filter epochs compared or old break healed")
	}
	assertNoExactPromotion(t, c.Snapshot())
}

func TestMultiSourceLateRecordsAndPartialExplicit(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("publicnode")[:1], 8)
	connectMultiSource(t, c, "A", 1)
	metaMultiSource(t, c, "A", 1, 700, 0, 699, "hash")
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceBankClosed, Slot: 700})
	d := txMultiSource(t, c, "A", 1, 700, 3, "late", "digest")
	if d.Quality != MultiSourceBroken || healthMultiSource(t, c, "A").PartialBanks != 1 {
		t.Fatalf("late transaction after closure did not invalidate bank: %+v", d)
	}
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourcePartialBank, Slot: 701, Reason: "provider reported partial bank"})
	if healthMultiSource(t, c, "A").PartialBanks != 2 {
		t.Fatal("explicit partial bank missing")
	}
	assertNoExactPromotion(t, c.Snapshot())
}

func TestMultiSourceRecordAdapterAndGlobalBreak(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 8)
	for _, id := range []string{"A", "B"} {
		connectMultiSource(t, c, id, 1)
	}
	rec := Record{Type: "update", Source: "A", Generation: 1, Kind: "block_footer", Slot: 800, Commitment: "finalized", FilterVersion: "pump-v1", Identity: "endpoint-a"}
	if _, err := c.ObserveRecord(rec); err != nil {
		t.Fatal(err)
	}
	if len(c.Snapshot().Banks) != 1 || c.Snapshot().Banks[0].ClosureSeen {
		t.Fatal("record adapter treated footer as completeness")
	}
	if _, err := c.ObserveRecord(Record{Type: "marker", Kind: "continuity_broken", Detail: "torn WAL tail"}); err != nil {
		t.Fatal(err)
	}
	if c.Snapshot().Quality != MultiSourceBroken || !healthMultiSource(t, c, "B").ContinuityBroken {
		t.Fatal("global WAL damage should affect all sources")
	}
	if _, err := c.ObserveRecord(Record{Type: "marker", Source: "A", Generation: 1, Kind: "dedupe_conflict", Detail: "same key different digest"}); err != nil {
		t.Fatal(err)
	}
	if c.Snapshot().Quality != MultiSourceConflict {
		t.Fatal("durable dedupe conflict not propagated")
	}
	assertNoExactPromotion(t, c.Snapshot())
}

func TestMultiSourceBoundedEvidenceAndSnapshotIsolation(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("publicnode")[:1], 2)
	connectMultiSource(t, c, "A", 1)
	for _, slot := range []uint64{900, 901, 902} {
		metaMultiSource(t, c, "A", 1, slot, 0, slot-1, "hash")
	}
	s := c.Snapshot()
	if len(s.Banks) != 2 || s.Banks[0].Slot != 901 || s.Banks[1].Slot != 902 || s.Sources[0].EvidenceEvicted != 1 {
		t.Fatalf("retention not bounded: %+v", s)
	}
	s.Sources[0].Quality = MultiSourceConflict
	s.Banks[0].Blockhash = "mutated"
	s.Issues = append(s.Issues, MultiSourceIssue{Kind: "fake"})
	got := c.Snapshot()
	if healthMultiSource(t, c, "A").Quality != MultiSourceBroken || got.Banks[0].Blockhash != "hash" || len(got.Issues) != 1 || got.Issues[0].Kind != "partial_bank" {
		t.Fatal("snapshot mutated coordinator state")
	}
	assertNoExactPromotion(t, c.Snapshot())
}

func TestMultiSourceInvalidInputs(t *testing.T) {
	for _, specs := range [][]MultiSourceSpec{nil, {{ID: "A", Identity: "id", Commitment: "finalized", FilterVersion: "v"}}, {{ID: "A", FailureDomain: "p", Identity: "id", Commitment: "finalized", FilterVersion: "v"}, {ID: "A", FailureDomain: "q", Identity: "id", Commitment: "finalized", FilterVersion: "v"}}} {
		if _, err := NewMultiSourceCoordinator(specs, 2); err == nil {
			t.Fatalf("invalid configuration accepted: %+v", specs)
		}
	}
	c := mustMultiSource(t, multiSourceSpecs("publicnode")[:1], 4)
	if _, err := c.Observe(MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceTransaction, Slot: 5, Signature: "sig", Digest: "digest"}); err == nil {
		t.Fatal("data before connection")
	}
	connectMultiSource(t, c, "A", 1)
	for _, o := range []MultiSourceObservation{
		{SourceID: "A", Generation: 1, Kind: MultiSourceGap, GapFrom: 8, GapTo: 7},
		{SourceID: "A", Generation: 1, Kind: MultiSourceBlockMeta, Slot: 10},
		{SourceID: "A", Generation: 1, Kind: MultiSourceTransaction, Slot: 10, Signature: "sig"},
		{SourceID: "A", Generation: 1, Kind: "not-a-kind"},
	} {
		if _, err := c.Observe(o); err == nil {
			t.Fatalf("invalid observation accepted: %+v", o)
		}
	}
	if c.Snapshot().Quality != MultiSourceUnverified || len(c.Snapshot().Banks) != 0 {
		t.Fatal("invalid observations mutated evidence")
	}
}

func TestMultiSourceSourceLocalConflictsAndAccountSetUnverified(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second MultiSourceObservation
		want          string
	}{
		{"bank_id", MultiSourceObservation{Kind: MultiSourceBlockMeta, Slot: 1000, BankID: 7, ParentSlot: 999, Blockhash: "hash"}, MultiSourceObservation{Kind: MultiSourceBlockMeta, Slot: 1000, BankID: 8, ParentSlot: 999, Blockhash: "hash"}, "source_bank_id_conflict"},
		{"footer_digest", MultiSourceObservation{Kind: MultiSourceBlockFooter, Slot: 1000, Digest: "digest-a"}, MultiSourceObservation{Kind: MultiSourceBlockFooter, Slot: 1000, Digest: "digest-b"}, "source_footer_conflict"},
		{"account_write", MultiSourceObservation{Kind: MultiSourceAccount, Slot: 1000, Account: "wallet", WriteVersion: 3, Digest: "digest-a"}, MultiSourceObservation{Kind: MultiSourceAccount, Slot: 1000, Account: "wallet", WriteVersion: 3, Digest: "digest-b"}, "source_account_conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := mustMultiSource(t, multiSourceSpecs("publicnode")[:1], 8)
			connectMultiSource(t, c, "A", 1)
			for _, o := range []*MultiSourceObservation{&tc.first, &tc.second} {
				o.SourceID, o.Generation = "A", 1
			}
			observeMultiSource(t, c, tc.first)
			d := observeMultiSource(t, c, tc.second)
			if d.Quality != MultiSourceConflict || d.Duplicate || len(d.Issues) != 1 || d.Issues[0].Kind != tc.want {
				t.Fatalf("source-local conflict not detected: %+v", d)
			}
			assertNoExactPromotion(t, c.Snapshot())
		})
	}

	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 8)
	connectMultiSource(t, c, "A", 1)
	connectMultiSource(t, c, "B", 1)
	for _, id := range []string{"A", "B"} {
		metaMultiSource(t, c, id, 1, 1001, 0, 1000, "same-hash")
	}
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceAccount, Slot: 1001, Account: "wallet", WriteVersion: 1, Digest: "write"})
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceBankClosed, Slot: 1001})
	d := observeMultiSource(t, c, MultiSourceObservation{SourceID: "B", Generation: 1, Kind: MultiSourceBankClosed, Slot: 1001})
	if d.Quality != MultiSourceUnverified || len(d.Issues) != 0 {
		t.Fatalf("source-local account versions were incorrectly used as cross-provider proof: %+v", d)
	}
	if got := c.Snapshot().Agreements; len(got) != 1 || got[0].ObservedAccountSetsComparable || got[0].ObservedAccountSetsEqual {
		t.Fatalf("account completeness was incorrectly claimed: %+v", got)
	}
}

func TestMultiSourceDifferentProviderWriteVersionsAreIncomparable(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 8)
	for _, id := range []string{"A", "B"} {
		connectMultiSource(t, c, id, 1)
		metaMultiSource(t, c, id, 1, 1002, 0, 1001, "same-hash")
	}
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceAccount, Slot: 1002, Account: "wallet", WriteVersion: 7, Digest: "same-state-local-digest-a"})
	observeMultiSource(t, c, MultiSourceObservation{SourceID: "B", Generation: 1, Kind: MultiSourceAccount, Slot: 1002, Account: "wallet", WriteVersion: 2007, Digest: "same-state-local-digest-b"})
	for _, id := range []string{"A", "B"} {
		observeMultiSource(t, c, MultiSourceObservation{SourceID: id, Generation: 1, Kind: MultiSourceBankClosed, Slot: 1002})
	}
	s := c.Snapshot()
	if s.Quality != MultiSourceUnverified || len(s.Issues) != 0 || len(s.Agreements) != 1 || s.Agreements[0].ObservedAccountSetsComparable || s.Agreements[0].ObservedAccountSetsEqual {
		t.Fatalf("local write versions/digests were compared across providers: %+v", s)
	}
	assertNoExactPromotion(t, s)
}

func TestMultiSourceConcurrentIndependentSources(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("second-provider"), 64)
	var wg sync.WaitGroup
	for _, id := range []string{"A", "B"} {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Observe(MultiSourceObservation{SourceID: id, Generation: 1, Kind: MultiSourceConnect}); err != nil {
				t.Error(err)
				return
			}
			for slot := uint64(1100); slot < 1120; slot++ {
				if _, err := c.Observe(MultiSourceObservation{SourceID: id, Generation: 1, Kind: MultiSourceBlockMeta, Slot: slot, ParentSlot: slot - 1, Blockhash: "shared-hash"}); err != nil {
					t.Error(err)
					return
				}
				_ = c.Snapshot()
			}
		}()
	}
	wg.Wait()
	s := c.Snapshot()
	if s.Quality != MultiSourceUnverified || s.ConnectedFailureDomains != 2 || len(s.Agreements) != 20 {
		t.Fatalf("concurrent independent feeds inconsistent: %+v", s)
	}
	assertNoExactPromotion(t, s)
}
