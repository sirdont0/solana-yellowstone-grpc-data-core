package core

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
)

// Exercise the public consumer contract against a live loopback Yellowstone
// stream, a closed archive and a freshly opened Engine/Store for both WALs.
func TestRawLiveCaptureDurableAckAndRestart(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "wal-v1", 2: "wal-v2"}[version], func(t *testing.T) {
			server := &auditGeyser{
				updates: []*pb.SubscribeUpdate{rawSlotUpdate(451), rawSlotUpdate(452)},
				sent:    make(chan struct{}),
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
			cfg.WALVersion = version
			if version == 2 {
				cfg.WALSegmentBytes = 128 << 20
			}
			cfg.Sources[0].Endpoint = "grpc://" + listener.Addr().String()
			cfg.Sources[0].Programs = nil
			cfg.Sources[0].Subscription = &pb.SubscribeRequest{
				Slots: map[string]*pb.SubscribeRequestFilterSlots{"progress": {}},
			}
			cfg.ShutdownTimeout = 2 * time.Second
			first, err := NewRaw(cfg)
			if err != nil {
				t.Fatal(err)
			}
			runCtx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- first.Run(runCtx) }()
			defer cancel()

			deadline := time.After(3 * time.Second)
			var registered RawCursor
			for {
				registered, err = first.RegisterConsumer("durable", "a", ConsumerFromBeginning)
				if err == nil {
					break
				}
				select {
				case <-deadline:
					t.Fatalf("source store did not open: %v", err)
				case <-time.After(10 * time.Millisecond):
				}
			}
			if registered.ConsumerEpoch == "" {
				t.Fatal("new consumer registration omitted lifecycle epoch")
			}
			reader, err := first.OpenRawReaderFromConsumer("durable", "a")
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-server.sent:
			case <-time.After(3 * time.Second):
				t.Fatal("loopback provider did not send updates")
			}
			readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer readCancel()
			var updates int
			var last RawCursor
			for updates < 2 {
				batch, err := reader.Next(readCtx, 32)
				if err != nil {
					t.Fatalf("live raw reader: %v", err)
				}
				for _, observation := range batch {
					if observation.Cursor.ConsumerEpoch != registered.ConsumerEpoch {
						t.Fatalf("reader lost consumer epoch: %+v", observation.Cursor)
					}
					if observation.Type == "update" {
						updates++
						if observation.Kind != "slot" || len(observation.Payload) == 0 {
							t.Fatalf("invalid raw slot observation: %+v", observation)
						}
					}
					last = observation.Cursor
				}
			}
			if err := first.Ack("durable", last); err != nil {
				t.Fatalf("durable ACK: %v", err)
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("graceful capture shutdown: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("capture did not terminate")
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}

			second, err := NewRaw(cfg)
			if err != nil {
				t.Fatal(err)
			}
			store, err := lab.OpenStore(toLabConfig(second.cfg, second.cfg.Sources[0]))
			if err != nil {
				t.Fatalf("reopen persisted WAL: %v", err)
			}
			second.stores["a"] = store
			again, err := second.RegisterConsumer("durable", "a", ConsumerFromBeginning)
			if err != nil || again != last {
				t.Fatalf("consumer ACK changed across restart: got=%+v want=%+v err=%v", again, last, err)
			}
			resumed, err := second.OpenRawReaderFromConsumer("durable", "a")
			if err != nil {
				t.Fatal(err)
			}
			batch, err := resumed.Next(context.Background(), 32)
			if err != nil || len(batch) == 0 {
				t.Fatalf("no post-ACK lifecycle records after restart: %d, %v", len(batch), err)
			}
			for _, observation := range batch {
				if observation.Cursor.Sequence <= last.Sequence || observation.Type == "update" {
					t.Fatalf("acknowledged update was redelivered: %+v", observation)
				}
			}
			if err := resumed.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			journal := "journal.wal"
			if version == 2 {
				journal = "journal.v2"
			}
			var archivedUpdates int
			err = ReadRawArchive(context.Background(), filepath.Join(cfg.DataDir, "a", journal), "a", RawCursor{}, func(ob RawObservation) error {
				if ob.Type == "update" {
					archivedUpdates++
				}
				return nil
			})
			if err != nil || archivedUpdates != 2 {
				t.Fatalf("closed archive contains %d updates, err=%v", archivedUpdates, err)
			}
		})
	}
}
