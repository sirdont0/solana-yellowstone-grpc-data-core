package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestByteBudgetBlocksUntilReleaseAndRejectsOversize(t *testing.T) {
	budget := NewByteBudget(10)
	if err := budget.Acquire(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
	if used, limit := budget.Snapshot(); used != 8 || limit != 10 {
		t.Fatalf("budget snapshot=%d/%d", used, limit)
	}
	acquired := make(chan error, 1)
	go func() { acquired <- budget.Acquire(context.Background(), 3) }()
	select {
	case err := <-acquired:
		t.Fatalf("acquire passed byte budget: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	budget.Release(8)
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked acquire was not woken by release")
	}
	if err := budget.Acquire(context.Background(), 11); !errors.Is(err, ErrQueueRecordTooLarge) {
		t.Fatalf("oversized record error=%v", err)
	}
}

func TestByteBudgetAcquireCanStopWhenWriterFails(t *testing.T) {
	budget := NewByteBudget(10)
	if err := budget.Acquire(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- budget.AcquireUntil(context.Background(), 1, stopped) }()
	close(stopped)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("acquire succeeded after writer stopped")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not observe writer stop")
	}
}
