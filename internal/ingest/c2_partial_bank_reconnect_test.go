package ingest

import (
	"testing"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The fake server deliberately omits an unknown transaction in slot 510 after
// reconnect. Neither a footer nor a replayed duplicate can prove inclusion.
func TestC2PartialBankReconnectOmissionRemainsUnverified(t *testing.T) {
	footer := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_BlockFooter{
		BlockFooter: &pb.SubscribeUpdateBlockFooter{Slot: 510},
	}}
	server := &scriptedGeyser{onSubscribe: func(session int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if session == 1 {
			// Yellowstone can deliver a footer before transactions of that bank.
			for _, update := range []*pb.SubscribeUpdate{metaUpdate(510), footer, txUpdate(510, 2)} {
				if err := stream.Send(update); err != nil {
					return err
				}
			}
			return status.Error(codes.Unavailable, "injected mid-bank disconnect")
		}
		// A real provider may replay only what it retained or matched. The test
		// intentionally never sends another transaction from slot 510.
		for _, update := range []*pb.SubscribeUpdate{txUpdate(510, 2), metaUpdate(511), txUpdate(511, 3)} {
			if err := stream.Send(update); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return nil
	}}
	cfg := integrationConfig(t, loopbackGeyser(t, server))
	cfg.Subscription = &pb.SubscribeRequest{
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"tx": {}},
		BlocksMeta:   map[string]*pb.SubscribeRequestFilterBlocksMeta{"meta": {}},
		BlockFooter:  map[string]*pb.SubscribeRequestFilterBlockFooter{"footer": {}},
	}
	got := driveRunner(t, cfg, func(rec Record, _ []Record) bool {
		return rec.Kind == "transaction" && rec.Slot == 511
	})
	reqs := server.recordedRequests()
	if len(reqs) < 2 || reqs[1].FromSlot == nil || reqs[1].GetFromSlot() != 505 {
		t.Fatalf("reconnect did not overlap source-local data slot 510: requests=%v", reqs)
	}
	if !got.checkpoint.ContinuityBroken || !hasMarker(got.wal, "disconnect") || !hasMarker(got.wal, "continuity_broken") {
		t.Fatalf("mid-bank disconnect not durable: checkpoint=%+v", got.checkpoint)
	}
	if st := got.checkpoint.Sources["A"]; st == nil || st.Duplicates < 1 {
		t.Fatalf("overlapped transaction was not deduplicated durably: %+v", st)
	}

	// The source WAL must preserve arrival order, even if that order is not a
	// canonical transaction order and a footer arrives before a transaction.
	metaSeq, footerSeq, txSeq, disconnectSeq := uint64(0), uint64(0), uint64(0), uint64(0)
	for _, r := range got.wal {
		switch {
		case r.Generation == 1 && r.Kind == "block_meta" && r.Slot == 510:
			metaSeq = r.Sequence
		case r.Generation == 1 && r.Kind == "block_footer" && r.Slot == 510:
			footerSeq = r.Sequence
		case r.Generation == 1 && r.Kind == "transaction" && r.Slot == 510:
			txSeq = r.Sequence
		case r.Generation == 1 && r.Kind == "disconnect":
			disconnectSeq = r.Sequence
		}
	}
	if metaSeq == 0 || footerSeq <= metaSeq || txSeq <= footerSeq || disconnectSeq <= txSeq {
		t.Fatalf("source-local WAL order changed: meta=%d footer=%d tx=%d disconnect=%d", metaSeq, footerSeq, txSeq, disconnectSeq)
	}
	var tx511 Record
	for _, r := range got.wal {
		if r.Kind == "transaction" && r.Slot == 511 {
			tx511 = r
			break
		}
	}
	if tx511.Signature == "" || tx511.Digest == "" {
		t.Fatal("test provider did not deliver second-session transaction")
	}

	specs := []MultiSourceSpec{
		{ID: "A", FailureDomain: "publicnode-a", Identity: cfg.Identity(), Commitment: cfg.Commitment, FilterVersion: cfg.FilterVersion},
		{ID: "B", FailureDomain: "independent-b", Identity: "fixture-b", Commitment: cfg.Commitment, FilterVersion: cfg.FilterVersion},
	}
	coordinator := mustMultiSource(t, specs, 16)
	connectMultiSource(t, coordinator, "B", 1)
	// B reports the same known transaction with a different arrival order,
	// but this still cannot witness the omitted member of the full block.
	txMultiSource(t, coordinator, "B", 1, 511, tx511.TxIndex, tx511.Signature, tx511.Digest)
	metaMultiSource(t, coordinator, "B", 1, 511, 999, 510, "hash-511")
	for _, r := range got.wal {
		if _, err := coordinator.ObserveWALRecord("A", r); err != nil {
			t.Fatalf("replay WAL seq=%d kind=%s: %v", r.Sequence, r.Kind, err)
		}
	}
	snap := coordinator.Snapshot()
	if snap.Quality != MultiSourceBroken || !healthMultiSource(t, coordinator, "A").ContinuityBroken || healthMultiSource(t, coordinator, "B").ContinuityBroken {
		t.Fatalf("A's lost bank contaminated B or was silently healed: %+v", snap)
	}
	if healthMultiSource(t, coordinator, "A").PartialBanks == 0 {
		t.Fatalf("mid-bank disconnect did not produce source-local partial evidence: %+v", snap)
	}
	partial510 := false
	for _, issue := range snap.Issues {
		if issue.Kind == "partial_bank" && issue.Slot == 510 && len(issue.Sources) == 1 && issue.Sources[0] == "A" {
			partial510 = true
		}
	}
	if !partial510 {
		t.Fatalf("missing partial_bank evidence for interrupted slot 510: %+v", snap.Issues)
	}
	if len(snap.Agreements) != 1 || !snap.Agreements[0].AnchorsAgree || !snap.Agreements[0].IndependentFailureDomains {
		t.Fatalf("source-local arrival reorder lost the valid A/B anchor agreement: %+v", snap.Agreements)
	}
	for _, agreement := range snap.Agreements {
		if agreement.ObservedSetsComparable || agreement.BothReportedClosed {
			t.Fatalf("footer/replay upgraded a bank to completed: %+v", agreement)
		}
	}
	assertNoExactPromotion(t, snap)
}

// A clean local WAL close is not an upstream bank closure witness. A bank can
// remain open at a graceful process boundary, even without a transport error.
func TestC2SourceLocalSessionEndMarksOpenBankPartial(t *testing.T) {
	coordinator := mustMultiSource(t, multiSourceSpecs("independent-b"), 8)
	for _, source := range []string{"A", "B"} {
		connectMultiSource(t, coordinator, source, 1)
	}
	metaMultiSource(t, coordinator, "A", 1, 700, 0, 699, "hash-700")
	observeMultiSource(t, coordinator, MultiSourceObservation{SourceID: "A", Generation: 1, Kind: MultiSourceBlockFooter, Slot: 700})
	if _, err := coordinator.ObserveWALRecord("A", Record{Type: "marker", Kind: "session_end", Detail: "graceful shutdown"}); err != nil {
		t.Fatal(err)
	}
	snap := coordinator.Snapshot()
	if snap.Quality != MultiSourceBroken || healthMultiSource(t, coordinator, "A").PartialBanks != 1 {
		t.Fatalf("open bank was implicitly treated as non-partial on WAL session_end: %+v", snap)
	}
	if h := healthMultiSource(t, coordinator, "B"); !h.Connected || h.ContinuityBroken {
		t.Fatalf("source-local WAL close affected independent B: %+v", h)
	}
	if len(snap.Banks) != 1 || !snap.Banks[0].Partial || snap.Banks[0].ClosureSeen {
		t.Fatalf("footer/session_end implied false bank closure: %+v", snap.Banks)
	}
	assertNoExactPromotion(t, snap)
}
