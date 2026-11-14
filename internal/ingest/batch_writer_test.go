package ingest

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
)

func TestBatchWriterDeliversDurablePrefixBeforeTerminalConflict(t *testing.T) {
	cfg := Config{
		SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token",
		Commitment: "finalized", FilterVersion: "batch-conflict",
		Programs: []string{PumpProgram}, DataDir: t.TempDir(),
		DedupeLimit: 20, Streams: 1,
	}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	var delivered []string
	runner := NewRunner(cfg, store)
	runner.OnLive = func(rec Record) { delivered = append(delivered, rec.Digest) }
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue, done, stopped := runner.startBatchedWriter(cancel)
	first := Record{Type: "update", Kind: "transaction", Source: "aa", DedupeKey: "tx-1", Digest: "original", Identity: cfg.Identity()}
	conflict := first
	conflict.Digest = "different"
	queue <- first
	queue <- conflict
	close(queue)
	if err := <-done; !errors.Is(err, ErrDedupeConflict) {
		t.Fatalf("expected terminal dedupe conflict, got %v", err)
	}
	<-stopped
	if len(delivered) != 1 || delivered[0] != "original" {
		t.Fatalf("durable prefix not delivered before terminal error: %v", delivered)
	}
	if !store.Snapshot().ContinuityBroken {
		t.Fatal("durable conflict did not latch continuity failure")
	}
}

func TestBatchDedupeAndDurableReplay(t *testing.T) {
	cfg := Config{
		SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token",
		Commitment: "finalized", FilterVersion: "batch-test",
		Programs: []string{PumpProgram}, DataDir: t.TempDir(),
		DedupeLimit: 20, Streams: 1,
	}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := Record{Type: "update", Kind: "transaction", Source: "aa", Generation: 1, Slot: 101, Signature: "sig", Digest: "digest-a", DedupeKey: "tx-a", Identity: cfg.Identity()}
	accepted, err := store.AppendBatch([]Record{r, r})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 2 || !accepted[0] || accepted[1] {
		t.Fatalf("accepted=%v", accepted)
	}
	var updates, duplicates int
	if err := ReadWAL(filepath.Join(cfg.DataDir, "journal.wal"), func(x Record) error {
		if x.Kind == "transaction" {
			updates++
		}
		if x.Kind == "duplicate" {
			duplicates++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if updates != 1 || duplicates != 1 {
		t.Fatalf("updates=%d duplicate-markers=%d", updates, duplicates)
	}
	conflict := r
	conflict.Digest = "different"
	if _, err := store.AppendBatch([]Record{conflict}); err == nil {
		t.Fatal("conflicting batch accepted")
	}
	if !store.Snapshot().ContinuityBroken {
		t.Fatal("conflict did not mark history broken")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.Snapshot().ContinuityBroken {
		t.Fatal("quality break not durable")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDedupeSurvivesBackendBankIDChange(t *testing.T) {
	cfg := Config{SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token", Commitment: "finalized", FilterVersion: "x", Programs: []string{PumpProgram}, DataDir: t.TempDir(), DedupeLimit: 20, Streams: 1}
	info := &pb.SubscribeUpdateTransactionInfo{Signature: []byte{1, 2, 3}, Index: 4}
	u1 := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Transaction{Transaction: &pb.SubscribeUpdateTransaction{Slot: 500, BankId: 100, Transaction: info}}}
	u2 := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Transaction{Transaction: &pb.SubscribeUpdateTransaction{Slot: 500, BankId: 200, Transaction: info}}}
	a, err := FromUpdate(cfg, "aa", 1, u1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := FromUpdate(cfg, "aa", 2, u2)
	if err != nil {
		t.Fatal(err)
	}
	if a.DedupeKey != b.DedupeKey || a.Digest != b.Digest {
		t.Fatalf("backend-local bank_id changed transaction identity: %q vs %q", a.DedupeKey, b.DedupeKey)
	}
	acc := &pb.SubscribeUpdateAccountInfo{Pubkey: []byte{2}, WriteVersion: 10}
	bankA, bankB := uint64(100), uint64(200)
	x1 := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Account{Account: &pb.SubscribeUpdateAccount{Slot: 500, BankId: &bankA, Account: acc}}}
	x2 := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Account{Account: &pb.SubscribeUpdateAccount{Slot: 500, BankId: &bankB, Account: acc}}}
	a, err = FromUpdate(cfg, "aa", 1, x1)
	if err != nil {
		t.Fatal(err)
	}
	b, err = FromUpdate(cfg, "aa", 2, x2)
	if err != nil {
		t.Fatal(err)
	}
	if a.DedupeKey == b.DedupeKey {
		t.Fatal("account write_version incorrectly treated as cross-generation identity")
	}
}

func TestAbortDoesNotMasqueradeAsGracefulRestart(t *testing.T) {
	cfg := Config{
		SourceID: "aa", Endpoint: "127.0.0.1:1111",
		TokenHeader: "x-token", Commitment: "finalized",
		FilterVersion: "abort-test", Programs: []string{PumpProgram},
		DataDir: t.TempDir(), DedupeLimit: 20, Streams: 1,
	}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Abort(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.Snapshot().ContinuityBroken {
		t.Fatal("aborted writer incorrectly treated as graceful shutdown")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
