package ingest

import (
	"context"
	"testing"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
)

func TestProvisionalPrecedesWALAndDurableCallback(t *testing.T) {
	s := &scriptedGeyser{onSubscribe: func(n int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if err := stream.Send(txUpdate(777, 0)); err != nil {
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
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type count struct {
		stage string
		count uint64
	}
	observed := make(chan count, 2)
	runner := NewRunner(c, store)
	runner.OnProvisional = func(r Record) {
		if r.Kind == "transaction" {
			observed <- count{"provisional", store.Snapshot().WALRecords}
		}
	}
	runner.OnLive = func(r Record) {
		if r.Kind == "transaction" {
			observed <- count{"durable", store.Snapshot().WALRecords}
			cancel()
		}
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 2 {
		t.Fatalf("callbacks seen=%d", len(observed))
	}
	first, second := <-observed, <-observed
	if first.stage != "provisional" || second.stage != "durable" || second.count != first.count+1 {
		t.Fatalf("callback order/durability incorrect: %+v %+v", first, second)
	}
}

func TestCancellationAfterRecvStillPersistsObservation(t *testing.T) {
	s := &scriptedGeyser{onSubscribe: func(_ int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if err := stream.Send(txUpdate(888, 4)); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	runner := NewRunner(c, store)
	seen := false
	runner.OnProvisional = func(rec Record) {
		if rec.Kind == "transaction" {
			seen = true
			cancel() // Happens after Recv, before its WAL enqueue.
		}
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !seen || store.Snapshot().Sources["A"].LastTxSlot != 888 {
		t.Fatal("received update was silently lost during graceful cancellation")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	updates := 0
	if err := ReadWAL(c.DataDir+"/journal.wal", func(rec Record) error {
		if rec.Type == "update" && rec.Kind == "transaction" && rec.Slot == 888 {
			updates++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if updates != 1 {
		t.Fatalf("expected one durable observation, got %d", updates)
	}
}
