package core

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
)

type c2DispatchQueueGeyser struct {
	pb.UnimplementedGeyserServer
	first      *pb.SubscribeUpdate
	second     *pb.SubscribeUpdate
	firstSent  chan struct{}
	sendSecond chan struct{}
	secondSent chan struct{}
}

func (s *c2DispatchQueueGeyser) Subscribe(stream pb.Geyser_SubscribeServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(s.first); err != nil {
		return err
	}
	close(s.firstSent)
	select {
	case <-s.sendSecond:
	case <-stream.Context().Done():
		return nil
	}
	if err := stream.Send(s.second); err != nil {
		return err
	}
	close(s.secondSent)
	<-stream.Context().Done()
	return nil
}

func TestC2EngineDispatchQueueByteBudgetAndDurableReplay(t *testing.T) {
	const payloadBytes = 700 << 10
	updates := []*pb.SubscribeUpdate{makeUpdate(901, false), makeUpdate(902, false)}
	for _, update := range updates {
		update.GetTransaction().GetTransaction().Meta.LogMessages = []string{strings.Repeat("x", payloadBytes)}
	}
	server := &c2DispatchQueueGeyser{
		first: updates[0], second: updates[1],
		firstSent: make(chan struct{}), sendSecond: make(chan struct{}), secondSent: make(chan struct{}),
	}
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
	cfg.Delivery = Durable
	cfg.MaxMessageMiB = 1
	cfg.QueueCapacity = 1
	cfg.QueueBytes = (1 << 20) + 1024
	cfg.SourceQueueBytes = 4 << 20
	cfg.BatchBytes = 4 << 20
	cfg.ShutdownTimeout = 50 * time.Millisecond
	firstRecord := auditRecord(t, &Engine{cfg: cfg}, updates[0])
	firstBytes := lab.RecordQueueBytes(firstRecord)
	if firstBytes*2 <= cfg.QueueBytes || firstBytes >= cfg.QueueBytes {
		t.Fatalf("fixture does not saturate shared queue as intended: record=%d budget=%d", firstBytes, cfg.QueueBytes)
	}

	sinkStarted := make(chan struct{})
	var sinkOnce sync.Once
	e, err := New(cfg, func(ctx context.Context, _ Event) error {
		sinkOnce.Do(func() { close(sinkStarted) })
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Register(probeHandler{}); err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(parent) }()

	select {
	case <-server.firstSent:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("fake Yellowstone did not send the first large transaction")
	}
	select {
	case <-sinkStarted:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("first durable transaction did not reach the blocked sink")
	}
	close(server.sendSecond)
	select {
	case <-server.secondSent:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("fake Yellowstone did not send the second large transaction")
	}

	deadline := time.Now().Add(3 * time.Second)
	var health SourceHealth
	for {
		health = e.Health()["a"]
		if health.Received >= 2 && health.QueueBytes == firstBytes {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("shared queue did not reach bounded backpressure: %+v want first record bytes=%d", health, firstBytes)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if health.QueueBytes > health.QueueByteLimit || health.QueueByteLimit != cfg.QueueBytes {
		cancel()
		t.Fatalf("shared dispatch queue exceeded configured byte budget: %+v", health)
	}
	if health.SourceQueueBytes <= 0 || health.SourceQueueBytes > health.SourceQueueByteLimit || health.SourceQueueByteLimit != cfg.SourceQueueBytes {
		cancel()
		t.Fatalf("source receive-to-WAL queue exceeded configured byte budget: %+v", health)
	}
	t.Logf("bounded pressure: shared=%d/%d bytes; source=%d/%d bytes; durable_received=%d; channel_depth=%d/%d",
		health.QueueBytes, health.QueueByteLimit, health.SourceQueueBytes, health.SourceQueueByteLimit,
		health.Received, health.QueueDepth, health.QueueCapacity)

	cancel()
	select {
	case runErr := <-done:
		if !errors.Is(runErr, ErrDrainTimeout) {
			t.Fatalf("shutdown under bounded queue pressure returned %v, want ErrDrainTimeout", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("context-aware sink did not stop after ShutdownTimeout")
	}

	reader, err := e.OpenRawReader("a", RawCursor{})
	if err != nil {
		t.Fatalf("open durable WAL after bounded dispatch abort: %v", err)
	}
	defer reader.Close()
	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()
	var durableTransactions int
	for {
		batch, readErr := reader.Next(readCtx, 32)
		for _, observation := range batch {
			if observation.Type == "update" && observation.Kind == "transaction" {
				durableTransactions++
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatalf("read durable WAL after pressure shutdown: %v", readErr)
		}
	}
	if durableTransactions != len(updates) {
		t.Fatalf("durable transactions after queue pressure=%d, want=%d", durableTransactions, len(updates))
	}
	t.Logf("replayed %d durable transactions after bounded dispatch shutdown", durableTransactions)
}
