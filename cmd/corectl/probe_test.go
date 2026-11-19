package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type probeGeyserFixture struct {
	pb.UnimplementedGeyserServer
	mu                    sync.Mutex
	requests              []*pb.SubscribeRequest
	authOnly              bool
	permissionDeniedOnly  bool
	pongOnly              bool
	replayInfoUnsupported bool
	rejectFromBefore      uint64
}

func (m *probeGeyserFixture) GetVersion(context.Context, *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {
	if m.authOnly {
		return nil, status.Error(codes.Unauthenticated, "token=SUPERSECRET must be absent from evidence")
	}
	if m.permissionDeniedOnly {
		return nil, status.Error(codes.PermissionDenied, "token=SUPERSECRET")
	}
	return &pb.GetVersionResponse{Version: "test-backend-14.0.1"}, nil
}

func (m *probeGeyserFixture) GetSlot(_ context.Context, req *pb.GetSlotRequest) (*pb.GetSlotResponse, error) {
	if m.authOnly {
		return nil, status.Error(codes.Unauthenticated, "never print token=SUPERSECRET")
	}
	if m.permissionDeniedOnly {
		return nil, status.Error(codes.PermissionDenied, "token=SUPERSECRET")
	}
	if req.GetCommitment() != pb.CommitmentLevel_FINALIZED {
		return nil, status.Error(codes.InvalidArgument, "wrong commitment")
	}
	return &pb.GetSlotResponse{Slot: 900}, nil
}

func (m *probeGeyserFixture) SubscribeReplayInfo(context.Context, *pb.SubscribeReplayInfoRequest) (*pb.SubscribeReplayInfoResponse, error) {
	if m.authOnly {
		return nil, status.Error(codes.Unauthenticated, "token=SUPERSECRET")
	}
	if m.permissionDeniedOnly {
		return nil, status.Error(codes.PermissionDenied, "token=SUPERSECRET")
	}
	if m.replayInfoUnsupported {
		return nil, status.Error(codes.Unimplemented, "replay info unsupported")
	}
	v := uint64(450)
	return &pb.SubscribeReplayInfoResponse{FirstAvailable: &v}, nil
}

func (m *probeGeyserFixture) Subscribe(s pb.Geyser_SubscribeServer) error {
	req, err := s.Recv()
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.requests = append(m.requests, req)
	m.mu.Unlock()
	if req.FromSlot != nil && m.rejectFromBefore > 0 && req.GetFromSlot() < m.rejectFromBefore {
		return status.Error(codes.OutOfRange, "replay retention exceeded")
	}
	if m.pongOnly {
		if err := s.Send(&pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Pong{Pong: &pb.SubscribeUpdatePong{Id: 1}}}); err != nil {
			return err
		}
		<-s.Context().Done()
		return nil
	}
	if len(req.GetBlockFooter()) > 0 {
		update := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_BlockFooter{
			BlockFooter: &pb.SubscribeUpdateBlockFooter{
				Slot: 500, BankId: 9,
				BlockFinalCert: &pb.BlockFooterFinalCert{Slot: 499,
					FinalAggregate: &pb.BlockFooterVotesAggregate{BlockId: []byte{1, 2, 3}}},
			},
		}}
		if err := s.Send(update); err != nil {
			return err
		}
	} else {
		update := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Slot{
			Slot: &pb.SubscribeUpdateSlot{Slot: 500, Status: pb.SlotStatus_SLOT_FINALIZED},
		}}
		if err := s.Send(update); err != nil {
			return err
		}
	}
	<-s.Context().Done()
	return nil
}

func startProbeFixture(t *testing.T, m *probeGeyserFixture) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterGeyserServer(server, m)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return "grpc://" + listener.Addr().String()
}

func TestProviderProbeReportsObservedFiltersAndOverlap(t *testing.T) {
	fixture := &probeGeyserFixture{}
	endpoint := startProbeFixture(t, fixture)
	var out, errBuf bytes.Buffer
	err := run([]string{
		"probe", "--endpoint", endpoint, "--provider-id", "fixture-primary",
		"--kinds", "slots,block_footer", "--sample", "200ms",
		"--timeout", "200ms", "--max-updates", "1",
		"--from-slot", "450", "--reopen-overlap",
	}, &out, &errBuf)
	if err != nil {
		t.Fatalf("probe: %v stderr=%s", err, errBuf.String())
	}
	var report providerProbeReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("bad probe evidence JSON: %v", err)
	}
	if report.ProviderID != "fixture-primary" || report.Dial.Status != "PASS" ||
		report.GetVersion.Status != "PASS" || report.GetSlot.Status != "PASS" ||
		report.SubscribeReplayInfo.Status != "PASS" ||
		report.SubscribeReplayInfo.FirstAvailable == nil || *report.SubscribeReplayInfo.FirstAvailable != 450 {
		t.Fatalf("wrong probe metadata: %+v", report)
	}
	if report.EndpointSHA256 != probeSHA([]byte(endpoint)) || report.GetVersion.VersionSHA256 == "" {
		t.Fatalf("missing redacted endpoint/version evidence: %+v", report)
	}
	if strings.Contains(out.String(), endpoint) || strings.Contains(out.String(), "test-backend-14.0.1") {
		t.Fatal("sensitive/plain provider details leaked into JSON output")
	}
	for _, kind := range []string{"slots", "block_footer"} {
		p := report.Streams[kind]
		if p.First.Status != "PASS" || p.First.FirstObservedSlot == nil ||
			*p.First.FirstObservedSlot != 500 || p.First.RequestedFromSlot == nil ||
			*p.First.RequestedFromSlot != 450 {
			t.Fatalf("wrong %s first sample: %+v", kind, p.First)
		}
		if p.Reopen == nil || p.Reopen.Status != "PASS" || p.Reopen.RequestedFromSlot == nil ||
			*p.Reopen.RequestedFromSlot != 500 || p.Reopen.MatchingObservedDigestCount != 1 {
			t.Fatalf("wrong %s overlap sample: %+v", kind, p.Reopen)
		}
	}
	if report.Streams["block_footer"].First.FooterWithCertificates != 1 ||
		report.Streams["block_footer"].First.CertificateSlotDiffers != 1 {
		t.Fatal("certificate footer/own-slot evidence was lost")
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.requests) != 4 {
		t.Fatalf("want 4 probe requests; got %d", len(fixture.requests))
	}
	if fixture.requests[2].GetBlockFooter()["probe_footer"].IncludeCertificates == nil ||
		!fixture.requests[2].GetBlockFooter()["probe_footer"].GetIncludeCertificates() {
		t.Fatal("include_certificates filter was not sent")
	}
}

func TestProviderProbeAuthErrorsAreRedactedAndNoFalseStreamPass(t *testing.T) {
	fixture := &probeGeyserFixture{authOnly: true}
	endpoint := startProbeFixture(t, fixture)
	var out bytes.Buffer
	if err := run([]string{"probe", "--endpoint", endpoint, "--kinds", "slots", "--sample", "50ms"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var report providerProbeReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.GetVersion.Status != "FAIL" || report.GetVersion.GRPCCode != "Unauthenticated" ||
		report.GetSlot.Status != "FAIL" || report.SubscribeReplayInfo.GRPCCode != "Unauthenticated" {
		t.Fatalf("wrong auth failure report: %+v", report)
	}
	if report.Streams["slots"].First.Status != "NOT_RUN" {
		t.Fatalf("must not mark untested filters PASS after auth failure: %+v", report.Streams)
	}
	if strings.Contains(out.String(), "SUPERSECRET") || strings.Contains(out.String(), endpoint) {
		t.Fatal("auth error or endpoint leaked into probe evidence")
	}
}

func TestProviderProbePermissionDeniedSkipsFilteredStreams(t *testing.T) {
	fixture := &probeGeyserFixture{permissionDeniedOnly: true}
	endpoint := startProbeFixture(t, fixture)
	var out bytes.Buffer
	if err := run([]string{
		"probe", "--endpoint", endpoint, "--kinds", "slots,block_footer", "--sample", "50ms",
	}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var report providerProbeReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.GetVersion.GRPCCode != "PermissionDenied" ||
		report.SubscribeReplayInfo.GRPCCode != "PermissionDenied" {
		t.Fatalf("permission denial not captured: %+v", report)
	}
	for _, kind := range []string{"slots", "block_footer"} {
		if report.Streams[kind].First.Status != "NOT_RUN" {
			t.Fatalf("%s should be NOT_RUN after auth denial", kind)
		}
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.requests) != 0 {
		t.Fatal("probe sent filtered requests despite denied credentials")
	}
	if strings.Contains(out.String(), "SUPERSECRET") {
		t.Fatal("sensitive gRPC status details leaked")
	}
}

func TestProviderProbeKeepaliveOnlyIsUnknown(t *testing.T) {
	fixture := &probeGeyserFixture{pongOnly: true}
	endpoint := startProbeFixture(t, fixture)
	var out bytes.Buffer
	if err := run([]string{
		"probe", "--endpoint", endpoint, "--kinds", "slots",
		"--sample", "70ms", "--max-updates", "5",
	}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var report providerProbeReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	got := report.Streams["slots"].First
	if got.Status != "UNKNOWN" || got.ObservedUpdates != 0 || got.ObservedKinds["slot"] != 0 {
		t.Fatalf("keepalive produced false slot PASS: %+v", got)
	}
}

func TestProviderProbeReplayRejectionIsNotMisreportedAsSuccess(t *testing.T) {
	fixture := &probeGeyserFixture{replayInfoUnsupported: true, rejectFromBefore: 450}
	endpoint := startProbeFixture(t, fixture)
	var out bytes.Buffer
	if err := run([]string{
		"probe", "--endpoint", endpoint, "--kinds", "slots",
		"--from-slot", "400", "--sample", "100ms",
	}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var report providerProbeReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.SubscribeReplayInfo.Status != "UNSUPPORTED" ||
		report.SubscribeReplayInfo.GRPCCode != "Unimplemented" {
		t.Fatalf("unsupported ReplayInfo misclassified: %+v", report.SubscribeReplayInfo)
	}
	stream := report.Streams["slots"].First
	if stream.Status != "FAIL" || stream.GRPCCode != "OutOfRange" ||
		stream.RequestedFromSlot == nil || *stream.RequestedFromSlot != 400 ||
		stream.ObservedUpdates != 0 {
		t.Fatalf("replay retention rejection misclassified: %+v", stream)
	}
}

func TestProviderProbeFilterConstructionAndValidation(t *testing.T) {
	selected, err := parseProbeKinds("transactions,transactions_status,accounts,blocks,combined")
	if err != nil || !requiresProbeProgram(selected) {
		t.Fatalf("kind parsing: %v %v", selected, err)
	}
	o := providerProbeOptions{
		Program:    "11111111111111111111111111111111",
		Commitment: pb.CommitmentLevel_FINALIZED, IncludeCertificates: true,
	}
	from := uint64(500)
	req := probeSubscribeRequest(o, "combined", &from)
	if req.GetFromSlot() != 500 || len(req.GetTransactions()) != 1 ||
		len(req.GetTransactionsStatus()) != 1 || len(req.GetAccounts()) != 1 ||
		len(req.GetBlocks()) != 1 || len(req.GetBlocksMeta()) != 1 ||
		len(req.GetEntry()) != 1 || len(req.GetBlockFooter()) != 1 ||
		len(req.GetSlots()) != 1 {
		t.Fatalf("combined profile incomplete: %+v", req)
	}
	if req.GetBlockFooter()["probe_footer"].IncludeCertificates == nil ||
		!req.GetBlockFooter()["probe_footer"].GetIncludeCertificates() {
		t.Fatal("certificate flag missing from combined filter")
	}
	if got := probeStreamObservationStatus("combined", map[string]int{"slot": 1}); got != "UNKNOWN" {
		t.Fatalf("partial combined profile falsely passed: %s", got)
	}
	var out bytes.Buffer
	if err := run([]string{"probe", "--endpoint", "grpc://127.0.0.1:1", "--kinds", "transactions"}, &out, &bytes.Buffer{}); err == nil {
		t.Fatal("unbounded transaction probe without a program must be rejected")
	}
	if err := run([]string{"probe", "--endpoint", "grpc://127.0.0.1:1", "--kinds", "unknown-kind"}, &out, &bytes.Buffer{}); err == nil {
		t.Fatal("unknown provider filter must be rejected")
	}
}

func TestProviderProbeRetentionBoundaryEvidence(t *testing.T) {
	fixture := &probeGeyserFixture{rejectFromBefore: 450}
	endpoint := startProbeFixture(t, fixture)
	var out bytes.Buffer
	if err := run([]string{
		"probe", "--endpoint", endpoint, "--kinds", "slots",
		"--sample", "100ms", "--max-updates", "1", "--retention-boundary",
	}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var report providerProbeReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	p := report.Streams["slots"]
	if p.First.Status != "PASS" || p.RetentionEdge == nil ||
		p.RetentionEdge.RequestedFromSlot == nil || *p.RetentionEdge.RequestedFromSlot != 449 ||
		p.RetentionEdge.Status != "FAIL" || p.RetentionEdge.GRPCCode != "OutOfRange" {
		t.Fatalf("retention edge evidence absent or misclassified: %+v", p)
	}
	if strings.Contains(out.String(), endpoint) {
		t.Fatal("provider endpoint leaked")
	}
}
