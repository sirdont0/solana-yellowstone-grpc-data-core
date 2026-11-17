package ingest

import (
	"context"
	"testing"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func accountUpdate(slot uint64) *pb.SubscribeUpdate {
	return &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Account{Account: &pb.SubscribeUpdateAccount{
		Slot: slot, Account: &pb.SubscribeUpdateAccountInfo{Pubkey: make([]byte, 32), WriteVersion: slot},
	}}}
}

func transactionStatusUpdate(slot uint64) *pb.SubscribeUpdate {
	return &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_TransactionStatus{
		TransactionStatus: &pb.SubscribeUpdateTransactionStatus{Slot: slot, Index: 1, Signature: make([]byte, 64)},
	}}
}

func slotUpdate(slot uint64) *pb.SubscribeUpdate {
	return &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Slot{
		Slot: &pb.SubscribeUpdateSlot{Slot: slot},
	}}
}

func TestTransactionStatusOnlyReconnectUsesStatusFrontier(t *testing.T) {
	server := &scriptedGeyser{onSubscribe: func(session int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if session == 1 {
			if err := stream.Send(transactionStatusUpdate(700)); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "injected disconnect")
		}
		if err := stream.Send(transactionStatusUpdate(701)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, server))
	c.Subscription = &pb.SubscribeRequest{TransactionsStatus: map[string]*pb.SubscribeRequestFilterTransactions{"statuses": {}}}
	got := driveRunner(t, c, func(r Record, _ []Record) bool { return r.Kind == "transaction_status" && r.Slot == 701 })
	reqs := server.recordedRequests()
	if len(reqs) < 2 || reqs[1].FromSlot == nil || reqs[1].GetFromSlot() != 695 {
		t.Fatalf("status-only reconnect must overlap status slot 700: %+v", reqs)
	}
	state := got.checkpoint.Sources["A"]
	if state.LastTxSlot != 0 || state.LastTxStatusSlot != 701 {
		t.Fatalf("transaction status frontier incorrectly merged into transaction frontier: %+v", state)
	}
}

func TestSlotOnlyReconnectUsesSlotUpdateFrontier(t *testing.T) {
	server := &scriptedGeyser{onSubscribe: func(session int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if session == 1 {
			if err := stream.Send(slotUpdate(700)); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "injected disconnect")
		}
		if err := stream.Send(slotUpdate(701)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, server))
	c.Subscription = &pb.SubscribeRequest{Slots: map[string]*pb.SubscribeRequestFilterSlots{"progress": {}}}
	got := driveRunner(t, c, func(r Record, _ []Record) bool { return r.Kind == "slot" && r.Slot == 701 })
	reqs := server.recordedRequests()
	if len(reqs) < 2 || reqs[1].FromSlot == nil || reqs[1].GetFromSlot() != 695 {
		t.Fatalf("slot-only reconnect must overlap slot update 700: %+v", reqs)
	}
	state := got.checkpoint.Sources["A"]
	if state.LastSlotUpdateSlot != 701 || state.LastTxSlot != 0 {
		t.Fatalf("slot updates were not tracked independently: %+v", state)
	}
}

func TestAccountOnlyReconnectUsesAccountFrontier(t *testing.T) {
	server := &scriptedGeyser{onSubscribe: func(session int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if session == 1 {
			if err := stream.Send(accountUpdate(500)); err != nil {
				return err
			}
			if err := stream.Send(metaUpdate(9000)); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "injected disconnect")
		}
		if err := stream.Send(accountUpdate(501)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, server))
	c.Subscription = &pb.SubscribeRequest{
		Accounts:   map[string]*pb.SubscribeRequestFilterAccounts{"all": {}},
		BlocksMeta: map[string]*pb.SubscribeRequestFilterBlocksMeta{"progress": {}},
	}
	got := driveRunner(t, c, func(r Record, _ []Record) bool { return r.Kind == "account" && r.Slot == 501 })
	reqs := server.recordedRequests()
	if len(reqs) < 2 || reqs[0].FromSlot != nil || reqs[1].FromSlot == nil || reqs[1].GetFromSlot() != 495 {
		t.Fatalf("account-only reconnect must overlap account slot 500, ignoring meta 9000: %+v", reqs)
	}
	state := got.checkpoint.Sources["A"]
	if state.LastTxSlot != 0 || state.LastAccountSlot != 501 || state.LastMetaSlot != 9000 {
		t.Fatalf("unexpected durable per-class frontiers: %+v", state)
	}
	reopened, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reloaded := reopened.Snapshot().Sources["A"]; reloaded == nil || reloaded.LastAccountSlot != 501 {
		t.Fatalf("account replay frontier did not survive WAL recovery: %+v", reloaded)
	}
}

func TestMixedSubscriptionUsesEarliestDataFrontier(t *testing.T) {
	server := &scriptedGeyser{onSubscribe: func(session int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if session == 1 {
			for _, update := range []*pb.SubscribeUpdate{txUpdate(100, 1), accountUpdate(90), metaUpdate(9000)} {
				if err := stream.Send(update); err != nil {
					return err
				}
			}
			return status.Error(codes.Unavailable, "injected disconnect")
		}
		if err := stream.Send(txUpdate(101, 2)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, server))
	c.Subscription = &pb.SubscribeRequest{
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"tx": {}},
		Accounts:     map[string]*pb.SubscribeRequestFilterAccounts{"accounts": {}},
		BlocksMeta:   map[string]*pb.SubscribeRequestFilterBlocksMeta{"progress": {}},
	}
	driveRunner(t, c, func(r Record, _ []Record) bool { return r.Kind == "transaction" && r.Slot == 101 })
	reqs := server.recordedRequests()
	if len(reqs) < 2 || reqs[1].FromSlot == nil || reqs[1].GetFromSlot() != 85 {
		t.Fatalf("mixed replay must use earlier account slot 90, then overlap 5: %+v", reqs)
	}
}

func TestAccountProgressCannotAdvanceUnobservedTransactionFrontier(t *testing.T) {
	server := &scriptedGeyser{onSubscribe: func(session int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if session == 1 {
			if err := stream.Send(accountUpdate(500)); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "injected disconnect")
		}
		if err := stream.Send(txUpdate(501, 1)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, server))
	c.Subscription = &pb.SubscribeRequest{
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"tx": {}},
		Accounts:     map[string]*pb.SubscribeRequestFilterAccounts{"accounts": {}},
	}
	driveRunner(t, c, func(r Record, _ []Record) bool { return r.Kind == "transaction" && r.Slot == 501 })
	reqs := server.recordedRequests()
	if len(reqs) < 2 || reqs[1].FromSlot != nil {
		t.Fatalf("account-only progress advanced an unobserved transaction stream: %+v", reqs)
	}
}

func TestCustomReplayFrontierCoversEveryRawSubscriptionClass(t *testing.T) {
	state := &SourceState{
		LastTxSlot: 1000, LastTxStatusSlot: 990, LastAccountSlot: 980,
		LastSlotUpdateSlot: 970, LastMetaSlot: 960, LastBlockSlot: 950,
		LastEntrySlot: 940, LastBlockFooterSlot: 930,
	}
	tests := []struct {
		name string
		sub  *pb.SubscribeRequest
		want uint64
	}{
		{"transactions", &pb.SubscribeRequest{Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"tx": {}}}, 1000},
		{"transaction statuses", &pb.SubscribeRequest{TransactionsStatus: map[string]*pb.SubscribeRequestFilterTransactions{"status": {}}}, 990},
		{"accounts", &pb.SubscribeRequest{Accounts: map[string]*pb.SubscribeRequestFilterAccounts{"account": {}}}, 980},
		{"slots", &pb.SubscribeRequest{Slots: map[string]*pb.SubscribeRequestFilterSlots{"slot": {}}}, 970},
		{"block metadata", &pb.SubscribeRequest{BlocksMeta: map[string]*pb.SubscribeRequestFilterBlocksMeta{"meta": {}}}, 960},
		{"blocks", &pb.SubscribeRequest{Blocks: map[string]*pb.SubscribeRequestFilterBlocks{"block": {}}}, 950},
		{"entries", &pb.SubscribeRequest{Entry: map[string]*pb.SubscribeRequestFilterEntry{"entry": {}}}, 940},
		{"block footers", &pb.SubscribeRequest{BlockFooter: map[string]*pb.SubscribeRequestFilterBlockFooter{"footer": {}}}, 930},
		{"mixed", &pb.SubscribeRequest{
			Transactions:       map[string]*pb.SubscribeRequestFilterTransactions{"tx": {}},
			TransactionsStatus: map[string]*pb.SubscribeRequestFilterTransactions{"status": {}},
			Accounts:           map[string]*pb.SubscribeRequestFilterAccounts{"account": {}},
			Slots:              map[string]*pb.SubscribeRequestFilterSlots{"slot": {}},
			BlocksMeta:         map[string]*pb.SubscribeRequestFilterBlocksMeta{"meta": {}},
			Blocks:             map[string]*pb.SubscribeRequestFilterBlocks{"block": {}},
			Entry:              map[string]*pb.SubscribeRequestFilterEntry{"entry": {}},
			BlockFooter:        map[string]*pb.SubscribeRequestFilterBlockFooter{"footer": {}},
		}, 930},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := replayFrontier(Config{Subscription: test.sub}, state); got != test.want {
				t.Fatalf("replayFrontier()=%d want %d", got, test.want)
			}
		})
	}

	// A mixed subscription must not resume from a class that has progressed
	// while another requested class has no observed frontier yet.
	missing := *state
	missing.LastEntrySlot = 0
	if got := replayFrontier(Config{Subscription: &pb.SubscribeRequest{
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"tx": {}},
		Entry:        map[string]*pb.SubscribeRequestFilterEntry{"entry": {}},
	}}, &missing); got != 0 {
		t.Fatalf("missing requested-class frontier advanced replay to %d", got)
	}

	// The legacy handler profile remains transaction-oriented, with its
	// optional mint-account subscription acting as an additional lower bound.
	if got := replayFrontier(Config{}, state); got != 1000 {
		t.Fatalf("legacy transaction frontier=%d want 1000", got)
	}
	if got := replayFrontier(Config{Mint: "mint"}, state); got != 980 {
		t.Fatalf("legacy transaction/account frontier=%d want 980", got)
	}
}

func TestStoreRebuildsPerClassReplayFrontiersFromWAL(t *testing.T) {
	c := integrationConfig(t, "grpc://127.0.0.1:1")
	c.Subscription = &pb.SubscribeRequest{
		Transactions:       map[string]*pb.SubscribeRequestFilterTransactions{"tx": {}},
		TransactionsStatus: map[string]*pb.SubscribeRequestFilterTransactions{"status": {}},
		Accounts:           map[string]*pb.SubscribeRequestFilterAccounts{"account": {}},
		Slots:              map[string]*pb.SubscribeRequestFilterSlots{"slot": {}},
		BlocksMeta:         map[string]*pb.SubscribeRequestFilterBlocksMeta{"meta": {}},
		Blocks:             map[string]*pb.SubscribeRequestFilterBlocks{"block": {}},
		Entry:              map[string]*pb.SubscribeRequestFilterEntry{"entry": {}},
		BlockFooter:        map[string]*pb.SubscribeRequestFilterBlockFooter{"footer": {}},
	}
	store, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, update := range []*pb.SubscribeUpdate{
		txUpdate(1000, 1),
		transactionStatusUpdate(990),
		accountUpdate(980),
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 970}}},
		metaUpdate(960),
		{UpdateOneof: &pb.SubscribeUpdate_Block{Block: &pb.SubscribeUpdateBlock{Slot: 950}}},
		{UpdateOneof: &pb.SubscribeUpdate_Entry{Entry: &pb.SubscribeUpdateEntry{Slot: 940}}},
		{UpdateOneof: &pb.SubscribeUpdate_BlockFooter{BlockFooter: &pb.SubscribeUpdateBlockFooter{Slot: 930}}},
	} {
		record, err := FromUpdate(c, "A", 1, update)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(record); err != nil {
			t.Fatal(err)
		}
	}
	assertFrontiers := func(where string, state *SourceState) {
		t.Helper()
		if state == nil || state.LastTxSlot != 1000 || state.LastTxStatusSlot != 990 || state.LastAccountSlot != 980 || state.LastSlotUpdateSlot != 970 || state.LastMetaSlot != 960 || state.LastBlockSlot != 950 || state.LastEntrySlot != 940 || state.LastBlockFooterSlot != 930 {
			t.Fatalf("%s replay frontiers=%+v", where, state)
		}
	}
	assertFrontiers("live WAL state", store.Snapshot().Sources["A"])
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertFrontiers("recovered WAL state", reopened.Snapshot().Sources["A"])
}

func TestRecvTimestampSurvivesQueueBackpressure(t *testing.T) {
	sent := make(chan struct{})
	server := &scriptedGeyser{onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if err := stream.Send(txUpdate(123, 1)); err != nil {
			return err
		}
		close(sent)
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, server))
	c.QueueBytes = 6 << 20
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	budget := NewByteBudget(c.QueueBytes)
	if err := budget.Acquire(context.Background(), c.QueueBytes); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runner := NewRunner(c, store)
	runner.QueueBudget = budget
	observed := make(chan Record, 1)
	runner.OnLive = func(r Record) {
		if r.Kind == "transaction" {
			observed <- r
			cancel()
		}
	}
	finished := make(chan error, 1)
	go func() { finished <- runner.Run(ctx) }()
	select {
	case <-sent:
	case <-ctx.Done():
		t.Fatal("server did not send update")
	}
	// The byte budget is held until well after the server has delivered the
	// message. The persisted receipt must reflect Recv, not queue admission.
	time.Sleep(350 * time.Millisecond)
	releasedAt := time.Now()
	budget.Release(c.QueueBytes)
	var received Record
	select {
	case received = <-observed:
	case <-ctx.Done():
		t.Fatal("update was not durably delivered after queue release")
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if !received.Time.Before(releasedAt.Add(-150 * time.Millisecond)) {
		t.Fatalf("receive timestamp was delayed to queue admission: received=%s released=%s", received.Time, releasedAt)
	}
	if source := store.Snapshot().Sources["A"]; source == nil || !source.LastTransportAt.Equal(received.Time) {
		t.Fatalf("checkpoint transport timestamp differs from the received record: %+v", source)
	}
}
