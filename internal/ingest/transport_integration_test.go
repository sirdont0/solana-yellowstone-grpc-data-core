package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// These tests use a real loopback gRPC transport, but never dial a public node.
// The server controls disconnects, replay availability, update order and pongs.
type scriptedGeyser struct {
	pb.UnimplementedGeyserServer
	mu          sync.Mutex
	requests    []*pb.SubscribeRequest
	sessions    atomic.Int32
	replayInfo  func() (*pb.SubscribeReplayInfoResponse, error)
	onSubscribe func(int, *pb.SubscribeRequest, pb.Geyser_SubscribeServer) error
}

func (s *scriptedGeyser) SubscribeReplayInfo(context.Context, *pb.SubscribeReplayInfoRequest) (*pb.SubscribeReplayInfoResponse, error) {
	if s.replayInfo != nil {
		return s.replayInfo()
	}
	v := uint64(0)
	return &pb.SubscribeReplayInfoResponse{FirstAvailable: &v}, nil
}

func (s *scriptedGeyser) Subscribe(stream pb.Geyser_SubscribeServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.requests = append(s.requests, proto.Clone(req).(*pb.SubscribeRequest))
	s.mu.Unlock()
	n := int(s.sessions.Add(1))
	if s.onSubscribe == nil {
		<-stream.Context().Done()
		return nil
	}
	return s.onSubscribe(n, req, stream)
}

func (s *scriptedGeyser) recordedRequests() []*pb.SubscribeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*pb.SubscribeRequest, len(s.requests))
	copy(result, s.requests)
	return result
}

func loopbackGeyser(t *testing.T, s *scriptedGeyser) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterGeyserServer(server, s)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return "grpc://" + lis.Addr().String()
}

func integrationConfig(t *testing.T, endpoint string) Config {
	t.Helper()
	return Config{
		Endpoint: endpoint, TokenHeader: "x-token", Commitment: "finalized",
		FilterVersion: "integration-v1", Programs: []string{PumpProgram, PumpSwapProgram},
		DataDir: t.TempDir(), ReplayOverlap: 5, PingInterval: time.Second,
		IdleTimeout: 5 * time.Second, MaxMessageBytes: 4 << 20,
		Streams: 1, DedupeLimit: 10000,
	}
}

func txUpdate(slot, index uint64) *pb.SubscribeUpdate {
	sig := make([]byte, 64)
	binary.LittleEndian.PutUint64(sig, index+1)
	return &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Transaction{Transaction: &pb.SubscribeUpdateTransaction{
		Slot: slot, Transaction: &pb.SubscribeUpdateTransactionInfo{Signature: sig, Index: index},
	}}}
}

func metaUpdate(slot uint64) *pb.SubscribeUpdate {
	return &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_BlockMeta{BlockMeta: &pb.SubscribeUpdateBlockMeta{
		Slot: slot, ParentSlot: slot - 1, Blockhash: fmt.Sprintf("hash-%d", slot),
	}}}
}

type integrationResult struct {
	checkpoint Checkpoint
	live       []Record
	wal        []Record
}

// driveRunner stops only after the asserted goal was observed; timeout is failure.
func driveRunner(t *testing.T, c Config, goal func(Record, []Record) bool) integrationResult {
	t.Helper()
	if err := c.Validate(); err != nil {
		t.Fatalf("invalid test config: %v", err)
	}
	store, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var mu sync.Mutex
	var live []Record
	var reached atomic.Bool
	runner := NewRunner(c, store)
	runner.OnLive = func(rec Record) {
		mu.Lock()
		live = append(live, rec)
		done := goal(rec, live)
		mu.Unlock()
		if done {
			reached.Store(true)
			cancel()
		}
	}
	finished := make(chan error, 1)
	go func() { finished <- runner.Run(ctx) }()
	select {
	case err = <-finished:
		if err != nil {
			t.Errorf("runner returned error: %v", err)
		}
	case <-ctx.Done():
		select {
		case err = <-finished:
			if err != nil {
				t.Errorf("runner returned error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("runner failed to stop after cancellation")
		}
	}
	if !reached.Load() {
		t.Error("expected goal was not observed before timeout")
	}
	result := integrationResult{checkpoint: store.Snapshot()}
	mu.Lock()
	result.live = append([]Record(nil), live...)
	mu.Unlock()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ReadWAL(c.DataDir+"/journal.wal", func(rec Record) error {
		result.wal = append(result.wal, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return result
}

func updateCount(records []Record) int {
	n := 0
	for _, rec := range records {
		if rec.Type == "update" {
			n++
		}
	}
	return n
}

func hasMarker(records []Record, kind string) bool {
	for _, rec := range records {
		if rec.Type == "marker" && rec.Kind == kind {
			return true
		}
	}
	return false
}

func TestSparseMatchingSlotsDoNotImplyGap(t *testing.T) {
	s := &scriptedGeyser{onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		for _, slot := range []uint64{101, 109, 1000} {
			if err := stream.Send(txUpdate(slot, slot)); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, s))
	got := driveRunner(t, c, func(_ Record, records []Record) bool { return len(records) == 3 })
	if got.checkpoint.ContinuityBroken || hasMarker(got.wal, "continuity_broken") {
		t.Fatalf("sparse matching slots caused false discontinuity: %+v", got.checkpoint)
	}
	if got.checkpoint.Sources["A"].LastTxSlot != 1000 || updateCount(got.wal) != 3 {
		t.Fatalf("unexpected cursor or WAL: %+v", got.checkpoint.Sources["A"])
	}
	if got.checkpoint.TxInclusionVerifiedThrough != 0 || got.checkpoint.AccountHistoryVerifiedThrough != 0 {
		t.Fatal("observed sparse updates were incorrectly promoted to verified coverage")
	}
}

func TestDedupeConflictIsTerminalWithoutReconnect(t *testing.T) {
	s := &scriptedGeyser{onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		first := txUpdate(707, 9)
		conflict := txUpdate(707, 9)
		conflict.GetTransaction().GetTransaction().Meta = &pb.TransactionStatusMeta{Fee: 1}
		if err := stream.Send(first); err != nil {
			return err
		}
		if err := stream.Send(conflict); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, s))
	store, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var delivered atomic.Int32
	runner := NewRunner(c, store)
	runner.OnLive = func(rec Record) {
		if rec.Kind == "transaction" {
			delivered.Add(1)
		}
	}
	err = runner.Run(ctx)
	if !errors.Is(err, errDurableWriterFailed) || !errors.Is(err, ErrDedupeConflict) {
		t.Fatalf("expected terminal durable dedupe conflict, got %v", err)
	}
	if s.sessions.Load() != 1 {
		t.Fatalf("terminal failure triggered %d connection attempts", s.sessions.Load())
	}
	if delivered.Load() != 1 || !store.Snapshot().ContinuityBroken {
		t.Fatalf("accepted prefix not delivered or quality not broken: delivered=%d quality=%+v", delivered.Load(), store.Snapshot())
	}
}

func TestReconnectOverlapAndPersistentDedupe(t *testing.T) {
	s := &scriptedGeyser{onSubscribe: func(n int, req *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if n == 1 {
			for _, slot := range []uint64{102, 106, 109} {
				if err := stream.Send(txUpdate(slot, slot)); err != nil {
					return err
				}
			}
			if err := stream.Send(txUpdate(102, 102)); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "injected server disconnect")
		}
		for _, slot := range []uint64{106, 109, 115} {
			if err := stream.Send(txUpdate(slot, slot)); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, s))
	got := driveRunner(t, c, func(rec Record, _ []Record) bool { return rec.Kind == "transaction" && rec.Slot == 115 })
	reqs := s.recordedRequests()
	if len(reqs) < 2 || reqs[0].FromSlot != nil || reqs[1].FromSlot == nil || reqs[1].GetFromSlot() != 104 {
		t.Fatalf("expected initial head, then replay from 109-5=104; requests=%v", reqs)
	}
	st := got.checkpoint.Sources["A"]
	if st.LastTxSlot != 115 || st.Duplicates != 3 || updateCount(got.wal) != 4 {
		t.Fatalf("overlap accounting incorrect: source=%+v wal_updates=%d", st, updateCount(got.wal))
	}
	if !got.checkpoint.ContinuityBroken || !hasMarker(got.wal, "disconnect") || !hasMarker(got.wal, "continuity_broken") {
		t.Fatal("stream disconnect must be durable and must not prove replay completeness")
	}
	reopened, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	rec, err := FromUpdate(c, "A", st.Generation+1, txUpdate(109, 109))
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := reopened.Append(rec)
	if err != nil || accepted || reopened.Snapshot().Sources["A"].Duplicates != 4 {
		t.Fatalf("dedupe did not survive process restart: accepted=%v err=%v", accepted, err)
	}
}

func TestForcedClientDisconnectTriggersReconnect(t *testing.T) {
	s := &scriptedGeyser{onSubscribe: func(n int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		slot := uint64(500 + n)
		if err := stream.Send(txUpdate(slot, slot)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, s))
	c.ForceDropAfter = 1
	got := driveRunner(t, c, func(rec Record, _ []Record) bool { return rec.Slot == 502 })
	if s.sessions.Load() < 2 || got.checkpoint.Sources["A"].Generation < 2 || !hasMarker(got.wal, "continuity_broken") {
		t.Fatalf("forced disconnect not durably reflected: %+v", got.checkpoint)
	}
	if updateCount(got.wal) != 2 {
		t.Fatalf("expected both distinct session updates, got %d", updateCount(got.wal))
	}
}

func TestReplayInfoUnavailableStillSendsFromSlot(t *testing.T) {
	from := uint64(450)
	s := &scriptedGeyser{
		replayInfo: func() (*pb.SubscribeReplayInfoResponse, error) {
			return nil, status.Error(codes.Unimplemented, "replay info disabled")
		},
		onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
			if err := stream.Send(txUpdate(450, 1)); err != nil {
				return err
			}
			<-stream.Context().Done()
			return nil
		},
	}
	c := integrationConfig(t, loopbackGeyser(t, s))
	c.FromSlot = &from
	got := driveRunner(t, c, func(rec Record, _ []Record) bool { return rec.Kind == "transaction" })
	reqs := s.recordedRequests()
	if len(reqs) != 1 || reqs[0].FromSlot == nil || reqs[0].GetFromSlot() != from || !hasMarker(got.wal, "replay_info_unavailable") {
		t.Fatalf("unavailable replay-info handling wrong: requests=%v", reqs)
	}
	if got.checkpoint.ContinuityBroken {
		t.Fatal("replay-info method unavailability alone should not claim a stream gap")
	}
}

func TestReplayRejectedFallsBackToHeadWithBreakMarker(t *testing.T) {
	from := uint64(650)
	s := &scriptedGeyser{onSubscribe: func(n int, req *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if n == 1 {
			return status.Error(codes.InvalidArgument, "from_slot unsupported")
		}
		if req.FromSlot != nil {
			return status.Error(codes.InvalidArgument, "second attempt must use live head")
		}
		if err := stream.Send(txUpdate(700, 1)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, s))
	c.FromSlot = &from
	got := driveRunner(t, c, func(rec Record, _ []Record) bool { return rec.Kind == "transaction" })
	reqs := s.recordedRequests()
	if len(reqs) < 2 || reqs[0].FromSlot == nil || reqs[0].GetFromSlot() != from || reqs[1].FromSlot != nil {
		t.Fatalf("replay rejection fallback incorrect: requests=%v", reqs)
	}
	if !got.checkpoint.ContinuityBroken || !hasMarker(got.wal, "replay_rejected") {
		t.Fatal("replay rejection must persist a continuity break")
	}
}

func TestReplayRetentionClampsRequestedSlot(t *testing.T) {
	from := uint64(300)
	s := &scriptedGeyser{
		replayInfo: func() (*pb.SubscribeReplayInfoResponse, error) {
			v := uint64(400)
			return &pb.SubscribeReplayInfoResponse{FirstAvailable: &v}, nil
		},
		onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
			if err := stream.Send(txUpdate(400, 1)); err != nil {
				return err
			}
			<-stream.Context().Done()
			return nil
		},
	}
	c := integrationConfig(t, loopbackGeyser(t, s))
	c.FromSlot = &from
	got := driveRunner(t, c, func(rec Record, _ []Record) bool { return rec.Kind == "transaction" })
	reqs := s.recordedRequests()
	if len(reqs) != 1 || reqs[0].FromSlot == nil || reqs[0].GetFromSlot() != 400 || !hasMarker(got.wal, "replay_retention_exceeded") || !got.checkpoint.ContinuityBroken {
		t.Fatalf("retention gap handling incorrect: requests=%v checkpoint=%+v", reqs, got.checkpoint)
	}
}

func TestServerPingClientPingAndPongs(t *testing.T) {
	var idsMu sync.Mutex
	var pingIDs []int32
	s := &scriptedGeyser{onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if err := stream.Send(&pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Ping{Ping: &pb.SubscribeUpdatePing{}}}); err != nil {
			return err
		}
		for {
			req, err := stream.Recv()
			if err != nil {
				return err
			}
			if ping := req.GetPing(); ping != nil {
				idsMu.Lock()
				pingIDs = append(pingIDs, ping.GetId())
				idsMu.Unlock()
				if err := stream.Send(&pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Pong{Pong: &pb.SubscribeUpdatePong{Id: ping.GetId()}}}); err != nil {
					return err
				}
			}
		}
	}}
	c := integrationConfig(t, loopbackGeyser(t, s))
	c.PingInterval = 25 * time.Millisecond
	c.IdleTimeout = 750 * time.Millisecond
	got := driveRunner(t, c, func(_ Record, records []Record) bool {
		pongs := 0
		for _, rec := range records {
			if rec.Kind == "pong" {
				pongs++
			}
		}
		return pongs >= 2
	})
	idsMu.Lock()
	ids := append([]int32(nil), pingIDs...)
	idsMu.Unlock()
	if len(ids) < 2 || ids[0] != 1 || got.checkpoint.Sources["A"].Pongs < 2 {
		t.Fatalf("server/client ping-pong exchange incomplete: ids=%v source=%+v", ids, got.checkpoint.Sources["A"])
	}
	if got.checkpoint.ContinuityBroken {
		t.Fatal("normal ping/pong must not mark continuity broken")
	}
}

func TestTwoIndependentSourcesRetainIdenticalUpdates(t *testing.T) {
	s := &scriptedGeyser{onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if err := stream.Send(txUpdate(801, 1)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, s))
	c.Streams = 2
	got := driveRunner(t, c, func(_ Record, records []Record) bool {
		seen := map[string]bool{}
		for _, rec := range records {
			seen[rec.Source] = true
		}
		return seen["A"] && seen["B"]
	})
	if s.sessions.Load() != 2 || updateCount(got.wal) != 2 || got.checkpoint.Sources["A"].LastTxSlot != 801 || got.checkpoint.Sources["B"].LastTxSlot != 801 {
		t.Fatalf("streams were not independent: sessions=%d checkpoint=%+v", s.sessions.Load(), got.checkpoint)
	}
	if got.checkpoint.ContinuityBroken {
		t.Fatal("healthy independent streams should not mark continuity broken")
	}
}

func TestDurableWriterUnderSlowConsumerBackpressure(t *testing.T) {
	const total = 120
	s := &scriptedGeyser{onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		for i := uint64(0); i < total; i++ {
			if err := stream.Send(txUpdate(900+i/10, i)); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, s))
	got := driveRunner(t, c, func(_ Record, records []Record) bool {
		time.Sleep(time.Millisecond)
		return len(records) == total
	})
	if updateCount(got.wal) != total || len(got.live) != total || got.checkpoint.Sources["A"].Received != total {
		t.Fatalf("lost local updates under backpressure: live=%d wal=%d source=%+v", len(got.live), updateCount(got.wal), got.checkpoint.Sources["A"])
	}
	if got.checkpoint.ContinuityBroken {
		t.Fatal("slow local writer alone triggered continuity break")
	}
}

func TestSubscriptionRequestShapeAndFilterIdentity(t *testing.T) {
	c := integrationConfig(t, "grpc://127.0.0.1:1")
	c.Mint, c.TokenAccountOwner = PumpProgram, Token2022Program
	from := uint64(123)
	req := SubscribeRequest(c, &from, true)
	if req.GetCommitment() != pb.CommitmentLevel_FINALIZED || req.GetFromSlot() != from || len(req.GetSlots()) != 1 || len(req.GetBlocksMeta()) != 1 {
		t.Fatalf("missing commitment/cursor/progress filters: %v", req)
	}
	tx := req.GetTransactions()["pump"]
	if tx == nil || tx.Vote == nil || *tx.Vote || tx.Failed == nil || *tx.Failed || !reflect.DeepEqual(tx.AccountInclude, c.Programs) {
		t.Fatalf("transaction filter does not match two programs: %+v", tx)
	}
	account := req.GetAccounts()["mint_accounts"]
	if account == nil || !reflect.DeepEqual(account.GetOwner(), []string{Token2022Program}) || len(account.GetFilters()) != 1 {
		t.Fatalf("missing owner-filtered token accounts: %+v", account)
	}
	memcmp := account.GetFilters()[0].GetMemcmp()
	if memcmp == nil || memcmp.GetOffset() != 0 || memcmp.GetBase58() != c.Mint {
		t.Fatalf("account mint memcmp must be offset 0 with base58 mint: %+v", memcmp)
	}
	if len(SubscribeRequest(c, nil, false).GetAccounts()) != 0 {
		t.Fatal("accounts should be omitted when disabled")
	}
	withoutMint := c
	withoutMint.Mint = ""
	if len(SubscribeRequest(withoutMint, nil, true).GetAccounts()) != 0 {
		t.Fatal("accounts should be omitted without a target mint")
	}
	withNewToken := c
	withNewToken.Token = "new-secret"
	withNewToken.RPCURL = "https://unrelated-rpc.invalid"
	if c.Identity() != withNewToken.Identity() {
		t.Fatal("credentials/unrelated RPC must not change durable subscription identity")
	}
	for label, change := range map[string]func(*Config){
		"version":       func(x *Config) { x.FilterVersion = "integration-v2" },
		"mint":          func(x *Config) { x.Mint = PumpSwapProgram },
		"program":       func(x *Config) { x.Programs = []string{PumpProgram} },
		"commitment":    func(x *Config) { x.Commitment = "confirmed" },
		"endpoint":      func(x *Config) { x.Endpoint = "grpc://127.0.0.1:2" },
		"account_owner": func(x *Config) { x.TokenAccountOwner = TokenProgram },
	} {
		changed := c
		change(&changed)
		if c.Identity() == changed.Identity() {
			t.Errorf("%s change was not included in identity", label)
		}
	}
}

func TestReplayErrorClassifier(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.OutOfRange, codes.FailedPrecondition, codes.Unimplemented} {
		if !isReplayRejection(status.Error(code, "fault")) {
			t.Errorf("%s should disable replay on next reconnect", code)
		}
	}
	if isReplayRejection(status.Error(codes.Unavailable, "network")) || isReplayRejection(errors.New("plain error")) || strings.Contains(sanitizeErr(errors.New("secret\nnext")), "\n") {
		t.Fatal("network errors must remain retryable; error markers must be single-line")
	}
}
