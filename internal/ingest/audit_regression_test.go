package ingest

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInitialFromSlotSurvivesFailedDial(t *testing.T) {
	from := uint64(850)
	server := &scriptedGeyser{onSubscribe: func(_ int, req *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if req.FromSlot == nil || req.GetFromSlot() != from {
			return errors.New("initial replay cursor lost")
		}
		if err := stream.Send(txUpdate(900, 1)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	cfg := integrationConfig(t, loopbackGeyser(t, server))
	cfg.FromSlot = &from
	cfg.MaxReconnects = 3
	s, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := NewRunner(cfg, s)
	attempts := 0
	r.Dial = func(ctx context.Context, c Config) (*grpc.ClientConn, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("first dial failed")
		}
		return GRPCDial(ctx, c)
	}
	r.OnLive = func(rec Record) {
		if rec.Kind == "transaction" {
			cancel()
		}
	}
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	reqs := server.recordedRequests()
	if attempts != 2 || len(reqs) != 1 || reqs[0].GetFromSlot() != from {
		t.Fatalf("attempts=%d requests=%v", attempts, reqs)
	}
}
func TestReconnectUsesTransactionCursorNotUnfilteredProgress(t *testing.T) {
	server := &scriptedGeyser{onSubscribe: func(n int, _ *pb.SubscribeRequest, stream pb.Geyser_SubscribeServer) error {
		if n == 1 {
			if err := stream.Send(txUpdate(100, 1)); err != nil {
				return err
			}
			if err := stream.Send(metaUpdate(1000)); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "forced disconnect")
		}
		if err := stream.Send(txUpdate(101, 2)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}}
	c := integrationConfig(t, loopbackGeyser(t, server))
	driveRunner(t, c, func(r Record, _ []Record) bool { return r.Kind == "transaction" && r.Slot == 101 })
	reqs := server.recordedRequests()
	if len(reqs) < 2 || reqs[1].GetFromSlot() != 95 {
		t.Fatalf("unfiltered progress advanced resume cursor: %v", reqs)
	}
}
func TestTerminalTransportErrorRedactsToken(t *testing.T) {
	c := integrationConfig(t, "host.invalid:443")
	c.Token = "private-test-token"
	c.MaxReconnects = 1
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := NewRunner(c, s)
	r.Dial = func(context.Context, Config) (*grpc.ClientConn, error) {
		return nil, errors.New("failure private-test-token\r\nnext")
	}
	err = r.Run(context.Background())
	if err == nil || strings.Contains(err.Error(), c.Token) || strings.ContainsAny(err.Error(), "\r\n") {
		t.Fatalf("unsafe error: %v", err)
	}
}
func TestReconnectJitterBounds(t *testing.T) {
	values := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		d := reconnectDelay(20)
		if d < 2*time.Second || d > 4*time.Second {
			t.Fatalf("unbounded delay: %v", d)
		}
		values[d] = true
	}
	if len(values) < 2 {
		t.Fatal("missing retry jitter")
	}
}
func TestWALMismatchDoesNotTruncateTail(t *testing.T) {
	c := recoveryConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.DataDir, "journal.wal")
	b, _ := os.ReadFile(path)
	b = append(b, 1, 2, 3)
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	c.FilterVersion = "other"
	if s, err = OpenStore(c); err == nil {
		s.Abort()
		t.Fatal("wrong identity accepted")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(after, b) {
		t.Fatal("foreign journal changed before identity rejection")
	}
}
func TestWALFailureIsStickyAndCloseConcurrentSafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.wal")
	w, err := OpenWAL(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	bad := Record{Time: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err = w.AppendBatch([]Record{{Kind: "first"}, bad}); err == nil {
		t.Fatal("invalid JSON timestamp accepted")
	}
	size, _ := os.Stat(path)
	if err = w.Append(Record{Kind: "after_failure"}); err == nil {
		t.Fatal("append after failure accepted")
	}
	after, _ := os.Stat(path)
	if size.Size() != after.Size() {
		t.Fatal("failed journal mutated")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Close(); _ = w.Count() }()
	}
	wg.Wait()
	if err = w.Append(Record{Kind: "closed"}); err == nil {
		t.Fatal("append after close accepted")
	}
}
func TestEndpointAndMetadataRejectEmbeddedCredentials(t *testing.T) {
	for _, endpoint := range []string{"https://user:secret@host:443", "https://host:443/?token=secret", "dns:///host:443", "https://host:443/#secret", "host"} {
		c := integrationConfig(t, endpoint)
		if err := c.Validate(); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("endpoint validation=%v", err)
		}
	}
	for _, header := range []string{"x-token\r\n", "grpc-secret", "x-token-bin"} {
		c := integrationConfig(t, "host:443")
		c.TokenHeader = header
		if err := c.Validate(); err == nil {
			t.Fatal("invalid metadata header accepted")
		}
	}
}

func TestRecordPreservesSourceLocalFooterAndStatusBankIDs(t *testing.T) {
	c := integrationConfig(t, "host:443")
	a := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_TransactionStatus{TransactionStatus: &pb.SubscribeUpdateTransactionStatus{Slot: 123, Index: 2, Signature: make([]byte, 64), BankId: 7}}}
	first, err := FromUpdate(c, "a", 1, a)
	if err != nil {
		t.Fatal(err)
	}
	a.GetTransactionStatus().BankId = 8
	second, err := FromUpdate(c, "a", 2, a)
	if err != nil {
		t.Fatal(err)
	}
	if first.BankID != 7 || second.BankID != 8 {
		t.Fatal("source-local status bank ID lost")
	}
	footer := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_BlockFooter{BlockFooter: &pb.SubscribeUpdateBlockFooter{Slot: 123, BankId: 9}}}
	rec, err := FromUpdate(c, "a", 1, footer)
	if err != nil {
		t.Fatal(err)
	}
	if rec.BankID != 9 {
		t.Fatal("footer bank ID lost")
	}
}
