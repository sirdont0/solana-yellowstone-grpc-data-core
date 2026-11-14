package ingest

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockGeyser generates intentionally sparse matching slots, replays duplicates
// after one forced server-side disconnect, and reports bank_id=0.
type MockGeyser struct {
	pb.UnimplementedGeyserServer
	Sessions atomic.Int32
}

func (m *MockGeyser) GetVersion(context.Context, *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {
	return &pb.GetVersionResponse{Version: "mock-richat-test"}, nil
}
func (m *MockGeyser) SubscribeReplayInfo(context.Context, *pb.SubscribeReplayInfoRequest) (*pb.SubscribeReplayInfoResponse, error) {
	v := uint64(100)
	return &pb.SubscribeReplayInfoResponse{FirstAvailable: &v}, nil
}
func (m *MockGeyser) GetSlot(context.Context, *pb.GetSlotRequest) (*pb.GetSlotResponse, error) {
	return &pb.GetSlotResponse{Slot: 115}, nil
}
func (m *MockGeyser) Ping(_ context.Context, p *pb.PingRequest) (*pb.PongResponse, error) {
	return &pb.PongResponse{Count: p.GetCount()}, nil
}
func (m *MockGeyser) Subscribe(stream pb.Geyser_SubscribeServer) error {
	req, e := stream.Recv()
	if e != nil {
		return e
	}
	if req.FromSlot != nil && *req.FromSlot < 100 {
		return status.Error(codes.OutOfRange, "mock replay buffer starts at 100")
	}
	var sendMu sync.Mutex
	send := func(u *pb.SubscribeUpdate) error { sendMu.Lock(); defer sendMu.Unlock(); return stream.Send(u) }
	count := m.Sessions.Add(1)
	// An independent receiver serves pings while the sender sends mock updates.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			r, e := stream.Recv()
			if e != nil {
				return
			}
			if p := r.GetPing(); p != nil {
				_ = send(&pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Pong{Pong: &pb.SubscribeUpdatePong{Id: p.GetId()}}})
			}
		}
	}()
	slots := []uint64{102, 106, 109}
	if count > 1 {
		slots = append(slots, 115)
	}
	for i, slot := range slots {
		meta := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_BlockMeta{BlockMeta: &pb.SubscribeUpdateBlockMeta{Slot: slot, ParentSlot: slot - 1, Blockhash: fmt.Sprintf("mock-block-%d", slot), BankId: 0}}}
		if e = send(meta); e != nil {
			return e
		}
		sig := make([]byte, 64)
		sig[0] = byte(i + 1)
		tx := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Transaction{Transaction: &pb.SubscribeUpdateTransaction{Slot: slot, BankId: 0, Transaction: &pb.SubscribeUpdateTransactionInfo{Signature: sig, Index: uint64(i)}}}}
		if e = send(tx); e != nil {
			return e
		}
		if i == 0 {
			if e = send(tx); e != nil {
				return e
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if count == 1 {
		return status.Error(codes.Unavailable, "mock network severed")
	}
	select {
	case <-stream.Context().Done():
		return nil
	case <-time.After(5 * time.Second):
		return nil
	}
}

func StartMock() (*grpc.Server, string, *MockGeyser, error) {
	lis, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return nil, "", nil, e
	}
	s := grpc.NewServer()
	mock := &MockGeyser{}
	pb.RegisterGeyserServer(s, mock)
	go func() { _ = s.Serve(lis) }()
	return s, "grpc://" + lis.Addr().String(), mock, nil
}
