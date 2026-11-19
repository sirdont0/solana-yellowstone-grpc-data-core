package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A slow OnLive callback keeps the already-durable record in the queue
// budget. Resuming the callback must drain queued work without loss.
func TestC2SlowLiveSinkPreservesQueueBoundAndDrains(t *testing.T) {
	cfg := Config{
		SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token",
		Commitment: "finalized", FilterVersion: "c2-sink-pressure",
		Programs: []string{PumpProgram}, DataDir: t.TempDir(), DedupeLimit: 20, Streams: 1,
	}
	first := Record{Type: "update", Kind: "transaction", Source: "aa", Identity: cfg.Identity(), DedupeKey: "first", Digest: "first"}
	second := Record{Type: "update", Kind: "transaction", Source: "aa", Identity: cfg.Identity(), DedupeKey: "second", Digest: "second"}
	cfg.QueueBytes = RecordQueueBytes(first)
	if got := RecordQueueBytes(second); got != cfg.QueueBytes {
		t.Fatalf("fixture mismatch: %d vs %d", got, cfg.QueueBytes)
	}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	initial := store.Snapshot().WALRecords
	runner := NewRunner(cfg, store)
	started, release := make(chan struct{}), make(chan struct{})
	var delivered []string
	runner.OnLive = func(rec Record) {
		if rec.DedupeKey == "first" {
			close(started)
			<-release
		}
		delivered = append(delivered, rec.DedupeKey)
	}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue, done, stopped, budget := runner.startBatchedWriter(cancel)
	if err := budget.Acquire(context.Background(), RecordQueueBytes(first)); err != nil {
		t.Fatal(err)
	}
	queue <- queuedRecord{record: first, bytes: RecordQueueBytes(first), receivedAt: time.Now()}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("callback never started")
	}
	if used, limit := budget.Snapshot(); used != limit || limit != cfg.QueueBytes {
		t.Fatalf("slow callback reservation=%d/%d", used, limit)
	}
	blockedCtx, stopBlocked := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer stopBlocked()
	if err := budget.Acquire(blockedCtx, RecordQueueBytes(second)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated queue did not block: %v", err)
	}
	close(release)
	admitCtx, stopAdmit := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopAdmit()
	if err := budget.Acquire(admitCtx, RecordQueueBytes(second)); err != nil {
		t.Fatalf("budget did not release: %v", err)
	}
	queue <- queuedRecord{record: second, bytes: RecordQueueBytes(second), receivedAt: time.Now()}
	close(queue)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not drain")
	}
	<-stopped
	if len(delivered) != 2 || delivered[0] != "first" || delivered[1] != "second" {
		t.Fatalf("callback order/count: %v", delivered)
	}
	if got := store.Snapshot().WALRecords; got != initial+2 {
		t.Fatalf("WAL records=%d, want=%d", got, initial+2)
	}
	if used, _ := budget.Snapshot(); used != 0 {
		t.Fatalf("queue budget leaked: %d", used)
	}
	if stats := runner.WriterQueueStats(); stats.Pending != 0 || stats.OldestAge != 0 {
		t.Fatalf("pending work leaked: %+v", stats)
	}
}
