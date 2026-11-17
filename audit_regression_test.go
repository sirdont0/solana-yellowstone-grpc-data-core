package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type outerHandler struct{}

func (outerHandler) ProgramIDs() []string { return []string{testProgram} }
func (outerHandler) Decode(_ *Transaction, ix Instruction) ([]Emission, error) {
	if ix.Inner != -1 {
		return nil, nil
	}
	return []Emission{{Kind: "outer"}}, nil
}
func auditRecord(t *testing.T, e *Engine, u *pb.SubscribeUpdate) lab.Record {
	t.Helper()
	r, err := lab.FromUpdate(toLabConfig(e.cfg, e.cfg.Sources[0]), "a", 1, u)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func auditEngine(t *testing.T, policy ErrorPolicy, events *[]Event) *Engine {
	t.Helper()
	cfg := configForTest(t)
	cfg.DecodeErrorPolicy = policy
	e, err := New(cfg, func(_ context.Context, ev Event) error { *events = append(*events, ev); return nil })
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestKnownEmptyCPILiveAndReplay(t *testing.T) {
	var events []Event
	e := auditEngine(t, FailClosed, &events)
	if err := e.Register(outerHandler{}); err != nil {
		t.Fatal(err)
	}
	u := makeUpdate(123, false)
	u.GetTransaction().GetTransaction().Meta.InnerInstructions = nil
	rec := auditRecord(t, e, u)
	if err := e.dispatch(context.Background(), input{rec: rec}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("known empty CPI rejected: %+v", events)
	}
	writeAuditWAL(t, e, []lab.Record{rec})
	events = nil
	if err := e.Replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !events[0].Durable {
		t.Fatalf("replay=%+v", events)
	}
}
func TestMissingCPIIsolationDoesNotErasePresenceOrPoisonOtherSource(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(map[bool]string{true: "nil_meta", false: "not_recorded"}[missing], func(t *testing.T) {
			var events []Event
			e := auditEngine(t, SkipAndFlag, &events)
			_ = e.Register(outerHandler{})
			u := makeUpdate(123, false)
			info := u.GetTransaction().GetTransaction()
			if missing {
				info.Meta = nil
			} else {
				info.Meta.InnerInstructions = nil
				info.Meta.InnerInstructionsNone = true
			}
			unreadable := auditRecord(t, e, u)
			if err := e.dispatch(context.Background(), input{rec: unreadable}); err != nil {
				t.Fatal(err)
			}
			if len(events) != 0 || len(e.Diagnostics()) != 1 || e.Health()["a"].DecodeFailures != 1 {
				t.Fatal("missing explicit failure evidence")
			}
			good := makeUpdate(123, false)
			good.GetTransaction().GetTransaction().Meta.InnerInstructions = nil
			complete := auditRecord(t, e, good)
			if unreadable.Digest == complete.Digest {
				t.Fatal("unknown CPI normalized to known empty")
			}
			complete.Source = "b"
			if err := e.dispatch(context.Background(), input{rec: complete}); err != nil {
				t.Fatal(err)
			}
			if len(events) != 1 || events[0].Source != "b" {
				t.Fatal("bad observation poisoned healthy provider")
			}
			next := auditRecord(t, e, makeUpdate(124, false))
			if err := e.dispatch(context.Background(), input{rec: next}); err != nil {
				t.Fatal(err)
			}
			if events[len(events)-1].Quality != "CONTINUITY_BROKEN" {
				t.Fatal("source failure not sticky")
			}
		})
	}
}
func TestHandlerSkipDiscardsWholeTransactionAndContinues(t *testing.T) {
	var events []Event
	e := auditEngine(t, FailClosed, &events)
	_ = e.Register(probeHandler{})
	_ = e.RegisterWithPolicy(failingHandler{}, SkipAndFlag)
	if err := e.dispatch(context.Background(), input{rec: auditRecord(t, e, makeUpdate(123, false))}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 || len(e.seen) != 0 || len(e.Diagnostics()) != 1 {
		t.Fatal("partial handler effects escaped")
	}
	u := makeUpdate(124, false)
	u.GetTransaction().GetTransaction().Meta.InnerInstructions = nil
	if err := e.dispatch(context.Background(), input{rec: auditRecord(t, e, u)}); err != nil {
		t.Fatal(err)
	}
	if len(e.seen) != 1 {
		t.Fatal("subsequent transaction was not processed")
	}
}
func TestSkipNeverBypassesWALIntegrity(t *testing.T) {
	for _, mutate := range []func(*lab.Record){func(r *lab.Record) { r.Digest = "tampered" }, func(r *lab.Record) { r.BankID++ }, func(r *lab.Record) { r.TxIndex++ }} {
		var events []Event
		e := auditEngine(t, SkipAndFlag, &events)
		_ = e.Register(probeHandler{})
		rec := auditRecord(t, e, makeUpdate(123, false))
		mutate(&rec)
		if err := e.dispatch(context.Background(), input{rec: rec}); err == nil {
			t.Fatal("integrity failure skipped")
		}
		if len(events) != 0 {
			t.Fatal("corrupt record emitted")
		}
	}
}
func writeAuditWAL(t *testing.T, e *Engine, records []lab.Record) string {
	t.Helper()
	cfg := toLabConfig(e.cfg, e.cfg.Sources[0])
	s, err := lab.OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"connection_attempt", "connection_open"} {
		if err := s.Mark("a", 1, kind, ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range records {
		if _, err := s.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(cfg.DataDir, "journal.wal")
}
func TestReplayTolerantTailReadOnlyAndStrictCRC(t *testing.T) {
	for _, mode := range []string{"partial_header", "partial_body", "crc"} {
		t.Run(mode, func(t *testing.T) {
			var events []Event
			e := auditEngine(t, FailClosed, &events)
			_ = e.Register(probeHandler{})
			path := writeAuditWAL(t, e, []lab.Record{auditRecord(t, e, makeUpdate(123, false))})
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			modified := append([]byte(nil), original...)
			switch mode {
			case "partial_header":
				modified = append(modified, 1, 2, 3)
			case "partial_body":
				modified = append(modified, 8, 0, 0, 0, 0, 0, 0, 0, 1, 2)
			case "crc":
				modified[len(modified)-1] ^= 1
			}
			if err := os.WriteFile(path, modified, 0600); err != nil {
				t.Fatal(err)
			}
			if err := e.Replay(context.Background()); err == nil || len(events) != 0 {
				t.Fatal("strict replay did not fail before delivery")
			}
			err = e.ReplayWithOptions(context.Background(), ReplayOptions{TolerantTail: true})
			if mode == "crc" {
				if err == nil || len(events) != 0 {
					t.Fatal("CRC corruption tolerated")
				}
			} else {
				if err != nil || len(events) != 1 || events[0].Quality != "CONTINUITY_BROKEN" {
					t.Fatalf("events=%+v err=%v", events, err)
				}
				if len(e.Diagnostics()) != 1 {
					t.Fatalf("tail diagnostic missing: %+v", e.Diagnostics())
				}
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(modified, after) {
				t.Fatal("read-only replay changed WAL")
			}
		})
	}
}
func TestReplayRejectsActiveWriter(t *testing.T) {
	var events []Event
	e := auditEngine(t, FailClosed, &events)
	s, err := lab.OpenStore(toLabConfig(e.cfg, e.cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	if err := e.ReplayWithOptions(context.Background(), ReplayOptions{TolerantTail: true}); err == nil || !strings.Contains(err.Error(), "active writer") {
		t.Fatalf("err=%v", err)
	}
}
func TestConfigOwnershipAndPublicReconnectControls(t *testing.T) {
	cfg := configForTest(t)
	from := uint64(500)
	cfg.FromSlot = &from
	cfg.ReplayOverlap = 81
	cfg.MaxReconnects = 3
	e, err := New(cfg, func(context.Context, Event) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	cfg.Sources[0].Programs[0] = "mutated"
	cfg.Sources[0].Endpoint = "mutated"
	from = 1
	lc := toLabConfig(e.cfg, e.cfg.Sources[0])
	if lc.Programs[0] != testProgram || lc.Endpoint == "mutated" || *lc.FromSlot != 500 || lc.ReplayOverlap != 81 || lc.MaxReconnects != 3 {
		t.Fatalf("cfg=%+v", lc)
	}
	for _, bad := range []Config{{MaxMessageMiB: 257}, {DecodeErrorPolicy: 9}, {MaxReconnects: -1}} {
		good := configForTest(t)
		good.MaxMessageMiB = bad.MaxMessageMiB
		good.DecodeErrorPolicy = bad.DecodeErrorPolicy
		good.MaxReconnects = bad.MaxReconnects
		if _, err := New(good, func(context.Context, Event) error { return nil }); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
}

type auditGeyser struct {
	pb.UnimplementedGeyserServer
	updates []*pb.SubscribeUpdate
	sent    chan struct{}
}

func (s *auditGeyser) Subscribe(stream pb.Geyser_SubscribeServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	for _, u := range s.updates {
		if err := stream.Send(u); err != nil {
			return err
		}
	}
	close(s.sent)
	<-stream.Context().Done()
	return nil
}

func rawAccountUpdate(slot uint64) *pb.SubscribeUpdate {
	pubkey := make([]byte, 32)
	pubkey[0] = 9
	owner := make([]byte, 32)
	owner[0] = 1
	bankID := uint64(0) // Explicit zero still has protobuf presence for this optional field.
	return &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Account{Account: &pb.SubscribeUpdateAccount{
		Account: &pb.SubscribeUpdateAccountInfo{Pubkey: pubkey, Owner: owner, Data: []byte{1, 2, 3}, WriteVersion: 7},
		Slot:    slot, BankId: &bankID,
	}}}
}

func rawSlotUpdate(slot uint64) *pb.SubscribeUpdate {
	bankID := uint64(0)
	return &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: slot, BankId: &bankID}}}
}

func TestRawOnlyEngineStreamsAllUpdateKindsAndDurableConsumerCursors(t *testing.T) {
	createdAt := time.Date(2026, 11, 16, 1, 2, 3, 0, time.UTC)
	missingCPI := makeUpdate(123, false)
	missingCPI.GetTransaction().GetTransaction().Meta = nil
	missingCPI.CreatedAt = timestamppb.New(createdAt)
	failed := makeUpdate(124, true)
	server := &auditGeyser{updates: []*pb.SubscribeUpdate{missingCPI, rawAccountUpdate(125), rawSlotUpdate(126), failed}, sent: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGeyserServer(grpcServer, server)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	cfg := configForTest(t)
	cfg.Sources[0].Endpoint = "grpc://" + listener.Addr().String()
	cfg.Sources[0].Programs = nil
	cfg.Sources[0].Subscription = &pb.SubscribeRequest{
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"raw-transactions": {}}, // nil Failed includes failed transactions.
		Accounts:     map[string]*pb.SubscribeRequestFilterAccounts{"token-accounts": {Owner: []string{lab.TokenProgram}}},
		Slots:        map[string]*pb.SubscribeRequestFilterSlots{"progress": {}},
	}
	cfg.ShutdownTimeout = time.Second
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	select {
	case <-server.sent:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("server did not send raw updates")
	}

	reader, err := e.OpenRawReader("a", RawCursor{})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer reader.Close()
	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()
	var updates []RawObservation
	for len(updates) < 4 {
		batch, nextErr := reader.Next(readCtx, 32)
		if nextErr != nil {
			cancel()
			t.Fatalf("raw reader: %v", nextErr)
		}
		for _, observation := range batch {
			if observation.Type == "update" {
				updates = append(updates, observation)
			}
		}
	}
	if got := []string{updates[0].Kind, updates[1].Kind, updates[2].Kind, updates[3].Kind}; strings.Join(got, ",") != "transaction,account,slot,transaction" {
		t.Fatalf("raw update kinds=%v", got)
	}
	if len(updates[0].Payload) == 0 || !updates[0].SourceCreatedAt.Equal(createdAt) || updates[0].ReceivedAt.IsZero() {
		t.Fatalf("source timestamps/raw payload missing: %+v", updates[0])
	}
	if !updates[1].BankIDPresent || updates[1].BankID != 0 || !updates[2].BankIDPresent {
		t.Fatalf("optional zero bank ID presence lost: account=%+v slot=%+v", updates[1], updates[2])
	}
	if !updates[3].Failed || !updates[3].StatusPresent || !updates[3].FeePresent {
		t.Fatalf("failed transaction status/fee metadata missing: %+v", updates[3])
	}
	health := e.Health()["a"]
	if health.QueueByteLimit == 0 || health.SourceQueueByteLimit == 0 || health.WALSyncCount == 0 {
		t.Fatalf("queue byte budgets are not observable: %+v", health)
	}
	last := updates[3].Cursor
	if last.Sequence == 0 || last.JournalID == "" || last.Identity == "" {
		t.Fatalf("incomplete stable cursor: %+v", last)
	}
	if err := e.Ack("aggregator", last); err != nil {
		cancel()
		t.Fatal(err)
	}
	acknowledged, err := e.ConsumerCursor("aggregator", "a")
	if err != nil || acknowledged != last {
		cancel()
		t.Fatalf("ack cursor=%+v err=%v want=%+v", acknowledged, err, last)
	}
	older := last
	older.Sequence--
	if err := e.Ack("aggregator", older); !errors.Is(err, ErrCursorRegression) {
		cancel()
		t.Fatalf("backward ack err=%v", err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("raw engine shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("raw engine did not stop")
	}
	var sawSessionEnd bool
	for {
		tail, nextErr := reader.Next(context.Background(), 32)
		for _, observation := range tail {
			if observation.Type == "marker" && observation.Kind == "session_end" {
				sawSessionEnd = true
			}
		}
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			t.Fatalf("live reader failed to switch to closed WAL: %v", nextErr)
		}
	}
	if !sawSessionEnd {
		t.Fatal("live raw reader did not drain the final WAL marker after shutdown")
	}
	var replayedUpdates int
	if err := e.ReadRawFromConsumer(context.Background(), "aggregator", "a", func(observation RawObservation) error {
		if observation.Type == "update" {
			replayedUpdates++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if replayedUpdates != 0 {
		t.Fatalf("acknowledged updates were redelivered: %d", replayedUpdates)
	}
}

func TestRunDrainsAcceptedEventsAfterParentCancellation(t *testing.T) {
	server := &auditGeyser{updates: []*pb.SubscribeUpdate{makeUpdate(201, false), makeUpdate(202, false)}, sent: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGeyserServer(grpcServer, server)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()
	cfg := configForTest(t)
	cfg.Sources[0].Endpoint = "grpc://" + listener.Addr().String()
	cfg.QueueCapacity = 1
	cfg.ShutdownTimeout = time.Second
	parent, cancel := context.WithCancel(context.Background())
	var events int
	e, err := New(cfg, func(ctx context.Context, _ Event) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		events++
		if events == 1 {
			<-server.sent // Ensure the second update was accepted by the server before cancelling.
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run(parent) }()
	select {
	case <-server.sent:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("server did not send events")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("graceful drain did not finish")
	}
	if events != 2 {
		t.Fatalf("accepted sink events were lost during cancellation: got=%d want=2", events)
	}
}

func TestRunDrainDeadlineIsReported(t *testing.T) {
	server := &auditGeyser{updates: []*pb.SubscribeUpdate{makeUpdate(211, false)}, sent: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGeyserServer(grpcServer, server)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()
	cfg := configForTest(t)
	cfg.Sources[0].Endpoint = "grpc://" + listener.Addr().String()
	cfg.ShutdownTimeout = 50 * time.Millisecond
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	e, err := New(cfg, func(ctx context.Context, _ Event) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run(parent) }()
	select {
	case <-server.sent:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not send event")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("expected drain timeout, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain deadline did not cancel sink context")
	}
}

func TestRunDrainDeadlineJoinsIndependentSinkError(t *testing.T) {
	server := &auditGeyser{updates: []*pb.SubscribeUpdate{makeUpdate(212, false)}, sent: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGeyserServer(grpcServer, server)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()
	cfg := configForTest(t)
	cfg.Sources[0].Endpoint = "grpc://" + listener.Addr().String()
	cfg.ShutdownTimeout = 10 * time.Millisecond
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	sinkErr := errors.New("independent sink failure")
	e, err := New(cfg, func(_ context.Context, _ Event) error {
		cancel()
		// Simulate a sink doing blocking work without observing cancellation.
		time.Sleep(200 * time.Millisecond)
		return sinkErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run(parent) }()
	select {
	case <-server.sent:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not send event")
	}
	select {
	case err := <-done:
		if !errors.Is(err, sinkErr) || !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("expected both sink error and drain timeout, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked sink did not return")
	}
}

func TestEngineLiveSkipPersistenceEvidenceAndDrain(t *testing.T) {
	for _, delivery := range []DeliveryMode{Durable, Provisional} {
		t.Run(map[DeliveryMode]string{Durable: "durable", Provisional: "provisional"}[delivery], func(t *testing.T) {
			bad := makeUpdate(123, false)
			bad.GetTransaction().GetTransaction().Meta.InnerInstructionsNone = true
			server := &auditGeyser{updates: []*pb.SubscribeUpdate{bad, makeUpdate(124, false)}, sent: make(chan struct{})}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			grpcServer := grpc.NewServer()
			pb.RegisterGeyserServer(grpcServer, server)
			go grpcServer.Serve(listener)
			defer grpcServer.Stop()
			cfg := configForTest(t)
			cfg.Sources[0].Endpoint = "grpc://" + listener.Addr().String()
			cfg.DecodeErrorPolicy = SkipAndFlag
			cfg.Delivery = delivery
			cfg.QueueCapacity = 1
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			var events []Event
			var e *Engine
			e, err = New(cfg, func(_ context.Context, ev Event) error {
				events = append(events, ev)
				_ = e.Health()
				_ = e.Evidence()
				cancel()
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			_ = e.Register(probeHandler{})
			done := make(chan error, 1)
			go func() { done <- e.Run(ctx) }()
			// Concurrent snapshot reads must remain safe while WAL callbacks update them.
			var readers sync.WaitGroup
			readers.Add(1)
			go func() {
				defer readers.Done()
				for ctx.Err() == nil {
					_ = e.Health()
					_ = e.Evidence()
					_ = e.Diagnostics()
					time.Sleep(time.Millisecond)
				}
			}()
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("engine drain deadlocked")
			}
			readers.Wait()
			if err != nil || len(events) != 1 || events[0].Quality != "CONTINUITY_BROKEN" {
				t.Fatalf("events=%+v err=%v", events, err)
			}
			if e.Health()["a"].Connected || e.Health()["a"].Received != 2 || e.Health()["a"].DecodeFailures != 1 {
				t.Fatalf("health=%+v", e.Health())
			}
			snap := e.Evidence()
			if len(snap.Sources) != 1 || snap.Sources[0].Connected || snap.TransactionInclusionAdmission != "UNVERIFIED" || snap.TxInclusionVerifiedThrough != 0 {
				t.Fatalf("evidence=%+v", snap)
			}
			var marker bool
			if err := lab.ReadWAL(filepath.Join(cfg.DataDir, "a", "journal.wal"), func(r lab.Record) error { marker = marker || r.Kind == "unreadable_update"; return nil }); err != nil {
				t.Fatal(err)
			}
			if !marker {
				t.Fatal("decoder failure not persisted")
			}
			events = nil
			if err := e.Replay(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(events) != 1 || events[0].Quality != "CONTINUITY_BROKEN" {
				t.Fatalf("replay=%+v", events)
			}
		})
	}
}

func TestDiagnosticsBoundedAndSecretsRedacted(t *testing.T) {
	var events []Event
	e := auditEngine(t, SkipAndFlag, &events)
	e.cfg.Sources[0].Token = "test-private-secret"
	for i := 0; i < 300; i++ {
		if err := e.decodeFailure(lab.Record{Source: "a"}, "handler_error", errors.New("test-private-secret\nfail"), SkipAndFlag); err != nil {
			t.Fatal(err)
		}
	}
	d := e.Diagnostics()
	if len(d) != 256 || e.Health()["a"].DecodeFailures != 300 {
		t.Fatal("diagnostics unbounded or counts lost")
	}
	if strings.Contains(d[0].Detail, "test-private-secret") || strings.Contains(d[0].Detail, "\n") {
		t.Fatal("secret leaked")
	}
	d[0].Detail = "mutated"
	if e.Diagnostics()[0].Detail == "mutated" {
		t.Fatal("mutable diagnostic snapshot")
	}
}

func BenchmarkDecodeTransaction(b *testing.B) {
	raw, _ := proto.Marshal(makeUpdate(123, false))
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := DecodeRawTransaction(raw, "a", true); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCrossSourcePositionConflictCannotEmitTwice(t *testing.T) {
	for _, mode := range []string{"signature", "failed"} {
		t.Run(mode, func(t *testing.T) {
			var events []Event
			e := auditEngine(t, SkipAndFlag, &events)
			_ = e.Register(probeHandler{})
			first := makeUpdate(123, mode == "failed")
			if err := e.dispatch(context.Background(), input{rec: auditRecord(t, e, first)}); err != nil {
				t.Fatal(err)
			}
			next := makeUpdate(123, false)
			if mode == "signature" {
				next.GetTransaction().GetTransaction().Signature[2] = 99
			}
			r := auditRecord(t, e, next)
			r.Source = "b"
			if err := e.dispatch(context.Background(), input{rec: r}); err == nil || !strings.Contains(err.Error(), "conflicting providers") {
				t.Fatalf("err=%v", err)
			}
			want := 1
			if mode == "failed" {
				want = 0
			}
			if len(events) != want {
				t.Fatalf("conflicting transaction emitted: %+v", events)
			}
		})
	}
}

type blockingRegistration struct{ entered, release chan struct{} }

func (h blockingRegistration) ProgramIDs() []string {
	close(h.entered)
	<-h.release
	return []string{testProgram}
}
func (h blockingRegistration) Decode(*Transaction, Instruction) ([]Emission, error) { return nil, nil }
func TestRegistrationSerializesWithSessionStart(t *testing.T) {
	var events []Event
	e := auditEngine(t, FailClosed, &events)
	h := blockingRegistration{make(chan struct{}), make(chan struct{})}
	registered := make(chan error, 1)
	go func() { registered <- e.Register(h) }()
	<-h.entered
	begun := make(chan error, 1)
	go func() { begun <- e.begin() }()
	select {
	case err := <-begun:
		t.Fatalf("Run overtook registration: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(h.release)
	if err := <-registered; err != nil {
		t.Fatal(err)
	}
	if err := <-begun; err != nil {
		t.Fatal(err)
	}
	defer e.running.Store(false)
	if len(e.handlers[testProgram]) != 1 {
		t.Fatal("handler lost")
	}
	if err := e.Register(probeHandler{}); err == nil {
		t.Fatal("registration while running accepted")
	}
}

func FuzzDecodeRawTransaction(f *testing.F) {
	raw, _ := proto.Marshal(makeUpdate(123, false))
	f.Add(raw)
	f.Add([]byte{0xff})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		tx, err := DecodeRawTransaction(raw, "fuzz", false)
		if err != nil {
			if tx != nil {
				t.Fatal("partial transaction escaped")
			}
			return
		}
		if len(tx.Signature) == 0 || tx.Digest == "" {
			t.Fatal("missing normalized identity")
		}
		for _, ix := range tx.Instructions {
			if ix.ProgramID == "" || ix.Inner < -1 {
				t.Fatal("invalid occurrence")
			}
		}
	})
}
