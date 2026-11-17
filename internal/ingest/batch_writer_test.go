package ingest

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

func TestWALV2BatchedWriterSplitsBeforeSegmentOverflow(t *testing.T) {
	c := Config{
		SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token",
		Commitment: "finalized", FilterVersion: "v2-split", Programs: []string{PumpProgram},
		DataDir: t.TempDir(), DedupeLimit: 32, Streams: 1,
		WALVersion: 2, WALSegmentBytes: 4 << 20, MaxMessageBytes: 1 << 20,
		BatchBytes: 4 << 20, QueueBytes: 16 << 20,
		PingInterval: time.Second, IdleTimeout: 5 * time.Second,
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("fixture config: %v", err)
	}
	store, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	initial := store.Snapshot().WALRecords
	runner := NewRunner(c, store)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue, done, stopped, budget := runner.startBatchedWriter(cancel)
	const count = 5
	for i := 0; i < count; i++ {
		rec := Record{
			Type: "update", Kind: "other", Source: "aa", Identity: c.Identity(),
			Raw: make([]byte, 900<<10),
		}
		rec.Raw[0] = byte(i)
		size := RecordQueueBytes(rec)
		if err := budget.Acquire(context.Background(), size); err != nil {
			t.Fatal(err)
		}
		queue <- queuedRecord{record: rec, bytes: size, receivedAt: time.Now()}
	}
	close(queue)
	if err := <-done; err != nil {
		t.Fatalf("writer stopped on a valid multi-segment batch: %v", err)
	}
	<-stopped
	if got := store.Snapshot().WALRecords; got != initial+count {
		t.Fatalf("durable records=%d, want=%d", got, initial+count)
	}
	if used, _ := budget.Snapshot(); used != 0 {
		t.Fatalf("writer leaked queue bytes: %d", used)
	}
}

func TestWriterQueueAgeIncludesPreWriterWaitAndClearsPending(t *testing.T) {
	cfg := Config{SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token", Commitment: "finalized", FilterVersion: "queue-age", Programs: []string{PumpProgram}, DataDir: t.TempDir(), DedupeLimit: 20, Streams: 1}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	runner := NewRunner(cfg, store)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue, done, stopped, budget := runner.startBatchedWriter(cancel)
	rec := Record{Type: "update", Kind: "transaction", Source: "aa", DedupeKey: "tx-queue-age", Digest: "original", Identity: cfg.Identity()}
	size := RecordQueueBytes(rec)
	if err := budget.Acquire(context.Background(), size); err != nil {
		t.Fatal(err)
	}
	queue <- queuedRecord{record: rec, bytes: size, receivedAt: time.Now().Add(-600 * time.Millisecond)}
	close(queue)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-stopped
	stats := runner.WriterQueueStats()
	if stats.Pending != 0 || stats.OldestAge != 0 || stats.LastAge < 500*time.Millisecond || stats.MaxAge < stats.LastAge || stats.AgeBuckets[7] != 1 {
		t.Fatalf("invalid source writer queue age: %+v", stats)
	}
	if got, _ := budget.Snapshot(); got != 0 {
		t.Fatalf("queue budget leaked %d bytes", got)
	}
	_, _, _, syncCount := store.SyncStats()
	var bucketCount uint64
	for _, n := range store.SyncHistogram() {
		bucketCount += n
	}
	if syncCount == 0 || bucketCount != syncCount {
		t.Fatalf("fsync histogram count=%d, sync attempts=%d", bucketCount, syncCount)
	}
}

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
	queue, done, stopped, budget := runner.startBatchedWriter(cancel)
	first := Record{Type: "update", Kind: "transaction", Source: "aa", DedupeKey: "tx-1", Digest: "original", Identity: cfg.Identity()}
	conflict := first
	conflict.Digest = "different"
	for _, rec := range []Record{first, conflict} {
		size := RecordQueueBytes(rec)
		if err := budget.Acquire(context.Background(), size); err != nil {
			t.Fatal(err)
		}
		queue <- queuedRecord{record: rec, bytes: size}
	}
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

func TestProcessedForkObservationsRemainSourceLocal(t *testing.T) {
	c := integrationConfig(t, "grpc://127.0.0.1:1")
	c.Commitment = "processed"
	c.DataDir = t.TempDir()
	u1 := txUpdate(700, 3)
	u2 := proto.Clone(u1).(*pb.SubscribeUpdate)
	u1.GetTransaction().GetTransaction().Meta = &pb.TransactionStatusMeta{Fee: 1}
	u2.GetTransaction().GetTransaction().Meta = &pb.TransactionStatusMeta{Fee: 2}
	r1, err := FromUpdate(c, "A", 1, u1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := FromUpdate(c, "A", 1, u2)
	if err != nil {
		t.Fatal(err)
	}
	if r1.DedupeKey != "" || r2.DedupeKey != "" || r1.Digest == r2.Digest {
		t.Fatalf("processed fork records were canonicalized: r1=%+v r2=%+v", r1, r2)
	}
	store, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	for _, rec := range []Record{r1, r2} {
		if accepted, err := store.Append(rec); err != nil || !accepted {
			t.Fatalf("processed observation rejected: accepted=%v err=%v", accepted, err)
		}
	}
	coordinator, err := NewMultiSourceCoordinator([]MultiSourceSpec{{ID: "A", FailureDomain: "provider-a", Identity: c.Identity(), Commitment: "processed", FilterVersion: c.FilterVersion}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range []Record{r1, r2} {
		if _, err := coordinator.ObserveWALRecord("A", rec); err != nil {
			t.Fatalf("processed fork evidence rejected: %v", err)
		}
	}
	if coordinator.Snapshot().Quality == MultiSourceConflict {
		t.Fatal("two processed banks for one slot caused a canonical conflict")
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
