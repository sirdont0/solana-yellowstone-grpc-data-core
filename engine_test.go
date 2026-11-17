package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mr-tron/base58"
	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

func TestSourceTokenIsNotJSONSerialized(t *testing.T) {
	b, err := json.Marshal(Source{ID: "primary", Endpoint: "example.com:443", Token: "do-not-leak"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "do-not-leak") {
		t.Fatal("provider token was exposed by JSON serialization")
	}
}

func TestPublicDedupeDiskOptionReachesSourceStore(t *testing.T) {
	cfg := configForTest(t)
	cfg.DedupeDisk = true
	cfg.DedupeLimit = 3
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	labCfg := toLabConfig(e.cfg, e.cfg.Sources[0])
	if !labCfg.DedupeDisk || labCfg.DedupeLimit != 3 {
		t.Fatalf("disk dedupe option lost before source Store: %+v", labCfg)
	}
}

func TestMessageLimitBoundaryAndRawOnlyCustomSubscription(t *testing.T) {
	for _, tc := range []struct {
		name string
		mib  int
		want bool
	}{{"default", 0, true}, {"minimum", 1, true}, {"maximum", 95, true}, {"negative", -1, false}, {"over-limit", 96, false}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := configForTest(t)
			cfg.MaxMessageMiB = tc.mib
			_, err := New(cfg, func(context.Context, Event) error { return nil })
			if (err == nil) != tc.want {
				t.Fatalf("MaxMessageMiB=%d err=%v", tc.mib, err)
			}
		})
	}

	cfg := configForTest(t)
	cfg.Sources[0].Programs = nil
	cfg.Sources[0].Subscription = &pb.SubscribeRequest{
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"all": {}},
		Accounts:     map[string]*pb.SubscribeRequestFilterAccounts{"token-accounts": {Owner: []string{lab.TokenProgram}}},
	}
	if _, err := NewRaw(cfg); err != nil {
		t.Fatalf("raw-only custom transaction/account subscription rejected: %v", err)
	}
}

func TestProgramHandlersRequireFinalizedCommitment(t *testing.T) {
	cfg := configForTest(t)
	cfg.Sources[0].Commitment = "processed"
	e, err := New(cfg, func(context.Context, Event) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Register(probeHandler{}); err == nil {
		t.Fatal("processed source was admitted to canonical program handlers")
	}
	raw, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Register(probeHandler{}); err == nil {
		t.Fatal("raw-only engine accepted a program handler")
	}
}

func TestConsumerCursorPersistsAcrossEngineRestartAndBindsJournal(t *testing.T) {
	cfg := configForTest(t)
	e1, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	lc := toLabConfig(cfg, cfg.Sources[0])
	store1, err := lab.OpenStore(lc)
	if err != nil {
		t.Fatal(err)
	}
	e1.stores["a"] = store1
	cp := store1.Snapshot()
	cursor := RawCursor{Source: "a", Identity: cp.Identity, JournalID: cp.JournalID, Sequence: cp.WALRecords}
	if err := e1.Ack("aggregator", cursor); err != nil {
		t.Fatal(err)
	}
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store2, err := lab.OpenStore(lc)
	if err != nil {
		t.Fatal(err)
	}
	e2.stores["a"] = store2
	defer store2.Abort()
	restored, err := e2.ConsumerCursor("aggregator", "a")
	if err != nil || restored != cursor {
		t.Fatalf("consumer cursor not restored after restart: got=%+v want=%+v err=%v", restored, cursor, err)
	}
	stale := cursor
	stale.JournalID = "recreated-journal"
	if err := e2.Ack("aggregator", stale); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("stale journal cursor was accepted: %v", err)
	}
}

const testProgram = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

type probeHandler struct{}

func (probeHandler) ProgramIDs() []string { return []string{testProgram} }
func (probeHandler) Decode(_ *Transaction, ix Instruction) ([]Emission, error) {
	if ix.Inner < 0 {
		return nil, nil
	}
	return []Emission{{Kind: "test.inner", Payload: len(ix.Data)}}, nil
}

type failingHandler struct{}

func (failingHandler) ProgramIDs() []string { return []string{testProgram} }
func (failingHandler) Decode(_ *Transaction, ix Instruction) ([]Emission, error) {
	if ix.Inner >= 0 {
		return nil, errors.New("injected decoder failure")
	}
	return nil, nil
}

func makeUpdate(slot uint64, failed bool) *pb.SubscribeUpdate {
	program, _ := base58.Decode(testProgram)
	key := make([]byte, 32)
	key[0] = 7
	meta := &pb.TransactionStatusMeta{
		InnerInstructions:       []*pb.InnerInstructions{{Index: 0, Instructions: []*pb.InnerInstruction{{ProgramIdIndex: 1, Accounts: []byte{0}, Data: []byte{1, 2, 3}}}}},
		LoadedWritableAddresses: [][]byte{program},
	}
	if failed {
		meta.Err = &pb.TransactionError{Err: []byte{1}}
	}
	sig := make([]byte, 64)
	sig[0] = byte(slot)
	return &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Transaction{Transaction: &pb.SubscribeUpdateTransaction{
		Slot: slot, BankId: 5, Transaction: &pb.SubscribeUpdateTransactionInfo{
			Signature: sig, Index: 2, Meta: meta, Transaction: &pb.Transaction{Message: &pb.Message{
				AccountKeys: [][]byte{key}, Instructions: []*pb.CompiledInstruction{{ProgramIdIndex: 1, Accounts: []byte{0}, Data: []byte{9}}}}}}}}}
}

func configForTest(t *testing.T) Config {
	t.Helper()
	return Config{DataDir: t.TempDir(), Sources: []Source{{ID: "a", Endpoint: "127.0.0.1:1234", TokenHeader: "x-token", Commitment: "finalized", FilterVersion: "sample-1", Programs: []string{testProgram}}}}
}

func TestDecodeRawTransactionCPIAndALT(t *testing.T) {
	raw, err := proto.Marshal(makeUpdate(123, false))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := DecodeRawTransaction(raw, "a", false)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Slot != 123 || tx.Failed || tx.Index != 2 || len(tx.Instructions) != 2 {
		t.Fatalf("bad tx: %+v", tx)
	}
	if tx.Instructions[0].Inner != -1 || tx.Instructions[1].Inner != 0 {
		t.Fatalf("wrong CPI position: %+v", tx.Instructions)
	}
	if tx.Instructions[1].ProgramID != testProgram || len(tx.Instructions[1].Accounts) != 1 {
		t.Fatalf("ALT program/account not resolved: %+v", tx.Instructions[1])
	}
}

func TestDecodeRawTransactionRejectsUnavailableCPIMetadata(t *testing.T) {
	for _, mode := range []string{"missing", "not_recorded"} {
		t.Run(mode, func(t *testing.T) {
			u := makeUpdate(123, false)
			if mode == "missing" {
				u.GetTransaction().GetTransaction().Meta = nil
			} else {
				u.GetTransaction().GetTransaction().Meta.InnerInstructionsNone = true
			}
			raw, _ := proto.Marshal(u)
			if _, err := DecodeRawTransaction(raw, "a", false); err == nil || !strings.Contains(err.Error(), "CPI metadata unavailable") {
				t.Fatalf("partial CPI data accepted: %v", err)
			}
		})
	}
}

func TestDispatchCrossSourceDedupeAndStickyQuality(t *testing.T) {
	var events []Event
	engine, err := New(configForTest(t), func(_ context.Context, e Event) error { events = append(events, e); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	raw, _ := proto.Marshal(makeUpdate(123, false))
	sig := base58.Encode(makeUpdate(123, false).GetTransaction().GetTransaction().GetSignature())
	rec := lab.Record{Kind: "transaction", Raw: raw, Source: "a", Commitment: "finalized", FilterVersion: "sample-1", Slot: 123, BankID: 5, TxIndex: 2, Signature: sig, Digest: testDigest(t, raw)}
	if err := engine.dispatch(context.Background(), input{rec: rec}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Quality != "OBSERVING_UNVERIFIED" {
		t.Fatalf("events=%+v", events)
	}
	duplicate := rec
	duplicate.Source = "b"
	if err := engine.dispatch(context.Background(), input{rec: duplicate}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatal("duplicate emitted again")
	}
	conflict := duplicate
	conflict.Digest = "different"
	if err := engine.dispatch(context.Background(), input{rec: conflict}); err == nil {
		t.Fatal("conflict accepted")
	}
	engine.observeQuality("a", lab.Record{Kind: "continuity_broken"})
	raw2, _ := proto.Marshal(makeUpdate(124, false))
	rec.Raw = raw2
	rec.Slot = 124
	rec.Signature = base58.Encode(makeUpdate(124, false).GetTransaction().GetTransaction().GetSignature())
	rec.Digest = testDigest(t, raw2)
	if err := engine.dispatch(context.Background(), input{rec: rec}); err != nil {
		t.Fatal(err)
	}
	if events[1].Quality != "CONTINUITY_BROKEN" {
		t.Fatalf("gap quality lost: %+v", events[1])
	}
	failedRaw, _ := proto.Marshal(makeUpdate(125, true))
	rec.Raw = failedRaw
	rec.Slot = 125
	rec.Signature = base58.Encode(makeUpdate(125, true).GetTransaction().GetTransaction().GetSignature())
	rec.Digest = testDigest(t, failedRaw)
	if err := engine.dispatch(context.Background(), input{rec: rec}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatal("failed transaction produced effects")
	}
}

func TestDispatchEmissionIDsAreUniqueAcrossHandlers(t *testing.T) {
	var ids []string
	engine, err := New(configForTest(t), func(_ context.Context, ev Event) error {
		ids = append(ids, ev.ID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	update := makeUpdate(123, false)
	raw, err := proto.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	rec := lab.Record{Kind: "transaction", Raw: raw, Source: "a", Commitment: "finalized", FilterVersion: "sample-1", Slot: 123, BankID: 5, TxIndex: 2,
		Signature: base58.Encode(update.GetTransaction().GetTransaction().GetSignature()), Digest: testDigest(t, raw)}
	if err := engine.dispatch(context.Background(), input{rec: rec}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("different handlers produced colliding event IDs: %v", ids)
	}
	rec.Commitment = "confirmed"
	if err := engine.dispatch(context.Background(), input{rec: rec}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 || ids[0] == ids[2] || ids[1] == ids[3] {
		t.Fatalf("different commitments produced colliding event IDs: %v", ids)
	}
}

func TestDispatchDecoderFailureDoesNotPublishPartialTransaction(t *testing.T) {
	var emitted []Event
	engine, err := New(configForTest(t), func(_ context.Context, ev Event) error {
		emitted = append(emitted, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(failingHandler{}); err != nil {
		t.Fatal(err)
	}
	update := makeUpdate(123, false)
	raw, _ := proto.Marshal(update)
	rec := lab.Record{Kind: "transaction", Raw: raw, Source: "a", Commitment: "finalized", FilterVersion: "sample-1", Slot: 123, BankID: 5, TxIndex: 2,
		Signature: base58.Encode(update.GetTransaction().GetTransaction().GetSignature()), Digest: testDigest(t, raw)}
	if err := engine.dispatch(context.Background(), input{rec: rec}); err == nil {
		t.Fatal("expected decoder error")
	}
	if len(emitted) != 0 || len(engine.seen) != 0 {
		t.Fatalf("decoder failure published partial effects: emitted=%d seen=%d", len(emitted), len(engine.seen))
	}
}

func TestDispatchSinkErrorDoesNotMarkTransactionDelivered(t *testing.T) {
	called := 0
	engine, err := New(configForTest(t), func(context.Context, Event) error {
		called++
		if called == 1 {
			return errors.New("sink unavailable")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	update := makeUpdate(123, false)
	raw, _ := proto.Marshal(update)
	rec := lab.Record{Kind: "transaction", Raw: raw, Source: "a", Commitment: "finalized", FilterVersion: "sample-1", Slot: 123, BankID: 5, TxIndex: 2,
		Signature: base58.Encode(update.GetTransaction().GetTransaction().GetSignature()), Digest: testDigest(t, raw)}
	if err := engine.dispatch(context.Background(), input{rec: rec}); err == nil {
		t.Fatal("expected sink error")
	}
	if len(engine.seen) != 0 {
		t.Fatal("failed sink delivery marked transaction as delivered")
	}
	if err := engine.dispatch(context.Background(), input{rec: rec}); err != nil {
		t.Fatal(err)
	}
	if called != 2 || len(engine.seen) != 1 {
		t.Fatalf("expected retry after sink failure, calls=%d seen=%d", called, len(engine.seen))
	}
}

func TestReplayUsesSameHandlerAndChecksWALIdentity(t *testing.T) {
	cfg := configForTest(t)
	var events []Event
	engine, err := New(cfg, func(_ context.Context, e Event) error { events = append(events, e); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	lc := toLabConfig(engine.cfg, engine.cfg.Sources[0])
	store, err := lab.OpenStore(lc)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := lab.FromUpdate(lc, "a", 1, makeUpdate(123, false))
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Append(rec); err != nil || !ok {
		t.Fatalf("append ok=%v err=%v", ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !events[0].Durable {
		t.Fatalf("events=%+v", events)
	}
	cfg.Sources[0].FilterVersion = "different"
	engine2, err := New(cfg, func(context.Context, Event) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := engine2.Replay(context.Background()); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("expected identity rejection, got %v", err)
	}
}

func TestReplayRejectsRecordFromAnotherSource(t *testing.T) {
	cfg := configForTest(t)
	engine, err := New(cfg, func(context.Context, Event) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	lc := toLabConfig(engine.cfg, engine.cfg.Sources[0])
	store, err := lab.OpenStore(lc)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := lab.FromUpdate(lc, "other-source", 1, makeUpdate(123, false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(rec); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Replay(context.Background()); err == nil || !strings.Contains(err.Error(), "WAL source mismatch") {
		t.Fatalf("expected replay source validation failure, got %v", err)
	}
}

func testDigest(t *testing.T, raw []byte) string {
	t.Helper()
	var u pb.SubscribeUpdate
	if err := proto.Unmarshal(raw, &u); err != nil {
		t.Fatal(err)
	}
	r, err := lab.FromUpdate(lab.Config{}, "a", 1, &u)
	if err != nil {
		t.Fatal(err)
	}
	return r.Digest
}
