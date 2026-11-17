package core

import (
	"errors"
	"os"
	"testing"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

func TestConsumerRegistrationPersistsAndNeverResetsACK(t *testing.T) {
	cfg := configForTest(t)
	e1, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e1.stores["a"] = store
	cp := store.Snapshot()
	if cp.WALRecords == 0 {
		t.Fatal("expected initial marker")
	}
	start, err := e1.RegisterConsumer("analytics", "a", ConsumerFromBeginning)
	if err != nil || start.Sequence != 0 {
		t.Fatalf("initial cursor=%+v err=%v", start, err)
	}
	if start.ConsumerEpoch == "" {
		t.Fatal("registered consumer must have an epoch")
	}
	ack := start
	ack.Sequence = cp.WALRecords
	if err := e1.Ack("analytics", ack); err != nil {
		t.Fatal(err)
	}
	if err := store.Mark("a", 1, "more", "fixture"); err != nil {
		t.Fatal(err)
	}
	registered, err := e1.RegisterConsumer("analytics", "a", ConsumerFromBeginning)
	if err != nil || registered.Sequence != cp.WALRecords {
		t.Fatalf("re-register reset ACK: %+v err=%v", registered, err)
	}
	e2, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e2.stores["a"] = store
	got, err := e2.RegisterConsumer("analytics", "a", ConsumerFromHead)
	if err != nil || got != registered {
		t.Fatalf("restart registration=%+v want=%+v err=%v", got, registered, err)
	}
	head, err := e2.RegisterConsumer("fresh", "a", ConsumerFromHead)
	if err != nil || head.Sequence != store.Snapshot().WALRecords {
		t.Fatalf("head registration=%+v err=%v", head, err)
	}
	all, err := e2.RegisteredConsumers("a")
	if err != nil || len(all) != 2 || all["analytics"] != registered || all["fresh"] != head {
		t.Fatalf("registered=%+v err=%v", all, err)
	}
	if err := e2.UnregisterConsumer("analytics", "a"); err != nil {
		t.Fatal(err)
	}
	if err := e1.Ack("analytics", registered); !errors.Is(err, ErrConsumerUnregistered) {
		t.Fatalf("late Ack after unregister = %v", err)
	}
	if _, err := e1.ConsumerCursor("analytics", "a"); !errors.Is(err, ErrConsumerUnregistered) {
		t.Fatalf("removed consumer was implicitly resumed: %v", err)
	}
	all, err = e1.RegisteredConsumers("a")
	if err != nil || len(all) != 1 {
		t.Fatalf("unregister list=%+v err=%v", all, err)
	}
	if err := e1.UnregisterConsumer("analytics", "a"); err != nil {
		t.Fatal(err)
	}
	reset, err := e1.RegisterConsumer("analytics", "a", ConsumerFromBeginning)
	if err != nil || reset.Sequence != 0 || reset.ConsumerEpoch == "" || reset.ConsumerEpoch == registered.ConsumerEpoch {
		t.Fatalf("explicit re-register=%+v err=%v", reset, err)
	}
	if err := e1.Ack("analytics", registered); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("late ACK from a prior registration advanced the new consumer: %v", err)
	}
	if got, err := e1.ConsumerCursor("analytics", "a"); err != nil || got != reset {
		t.Fatalf("stale ACK changed new registration: got=%+v err=%v", got, err)
	}
}

func TestConsumerRegistryFailsClosedOnBadCursor(t *testing.T) {
	cfg := configForTest(t)
	e, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e.stores["a"] = store
	if _, err := e.RegisterConsumer("bad", "a", ConsumerFromBeginning); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(e.consumerCursorPath("bad", "a"), []byte(`{"source":"wrong"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisteredConsumers("a"); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("corrupt/stale cursor should block retention: %v", err)
	}
	if _, err := e.RegisterConsumer("bad", "a", ConsumerFromHead); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("registration must not reset corrupt cursor: %v", err)
	}
	if _, err := e.RegisterConsumer("UPPER", "a", ConsumerFromBeginning); err == nil {
		t.Fatal("accepted invalid name")
	}
	if _, err := e.RegisterConsumer("new", "a", ConsumerStart(10)); err == nil {
		t.Fatal("accepted invalid mode")
	}
}
