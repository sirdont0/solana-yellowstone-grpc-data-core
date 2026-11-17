package ingest

import (
	"context"
	"errors"
	"sync"
)

var ErrQueueRecordTooLarge = errors.New("record exceeds configured queue byte budget")

// byteBudget bounds retained record payloads in addition to channel item
// counts. It is an operational queue bound, not a hard Go heap/RSS limit.
type ByteBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
	wake  chan struct{}
}

func NewByteBudget(limit int64) *ByteBudget {
	return &ByteBudget{limit: limit, wake: make(chan struct{})}
}

func (b *ByteBudget) Acquire(ctx context.Context, size int64) error {
	return b.AcquireUntil(ctx, size, nil)
}

func (b *ByteBudget) AcquireUntil(ctx context.Context, size int64, stopped <-chan struct{}) error {
	if size <= 0 {
		size = 1
	}
	if size > b.limit {
		return ErrQueueRecordTooLarge
	}
	for {
		b.mu.Lock()
		if b.used+size <= b.limit {
			b.used += size
			b.mu.Unlock()
			return nil
		}
		wake := b.wake
		b.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		case <-stopped:
			return errors.New("queue writer stopped")
		}
	}
}

func (b *ByteBudget) Release(size int64) {
	if size <= 0 {
		return
	}
	b.mu.Lock()
	b.used -= size
	if b.used < 0 {
		b.used = 0
	}
	close(b.wake)
	b.wake = make(chan struct{})
	b.mu.Unlock()
}

func (b *ByteBudget) Snapshot() (used, limit int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.limit
}

func RecordQueueBytes(r Record) int64 {
	// Include fixed envelope overhead so a stream of tiny records is not
	// represented as zero-cost queue work.
	return int64(len(r.Raw) + len(r.Detail) + len(r.Signature) + len(r.Account) + 512)
}
