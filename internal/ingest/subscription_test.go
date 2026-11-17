package ingest

import (
	"path/filepath"
	"testing"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

func customSubscriptionConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Endpoint: "127.0.0.1:1234", TokenHeader: "x-token", Commitment: "finalized",
		FilterVersion: "raw-v1", DataDir: filepath.Join(t.TempDir(), "wal"),
		MaxMessageBytes: 4 << 20, Streams: 1, DedupeLimit: 100,
		PingInterval: time.Second, IdleTimeout: 5 * time.Second,
		Subscription: &pb.SubscribeRequest{
			Transactions: map[string]*pb.SubscribeRequestFilterTransactions{
				"all-transactions": {AccountInclude: []string{PumpProgram}}, // nil Vote/Failed means no restriction.
			},
			Accounts: map[string]*pb.SubscribeRequestFilterAccounts{
				"token-accounts": {Owner: []string{TokenProgram}},
			},
			Slots: map[string]*pb.SubscribeRequestFilterSlots{"progress": {}},
		},
	}
}

func TestCustomSubscriptionPreservesTriStateAndAccountFilters(t *testing.T) {
	c := customSubscriptionConfig(t)
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	from := uint64(123)
	req := SubscribeRequest(c, &from, true)
	filter := req.GetTransactions()["all-transactions"]
	if filter == nil || filter.Vote != nil || filter.Failed != nil {
		t.Fatalf("nil tri-state filters were rewritten: %+v", filter)
	}
	if req.FromSlot == nil || req.GetFromSlot() != from || req.Ping != nil {
		t.Fatalf("runner-owned request fields incorrect: %+v", req)
	}
	if got := req.GetAccounts()["token-accounts"].GetOwner(); len(got) != 1 || got[0] != TokenProgram {
		t.Fatalf("account filters lost: %v", got)
	}
	if got := req.GetSlots()["progress"]; got == nil {
		t.Fatal("slot progress filter lost")
	}
	if req.GetCommitment() != pb.CommitmentLevel_FINALIZED {
		t.Fatalf("commitment not owned by engine: %v", req.GetCommitment())
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}

	baselineIdentity := c.Identity()
	changed := c
	changed.Subscription = proto.Clone(c.Subscription).(*pb.SubscribeRequest)
	failed := false
	changed.Subscription.Transactions["all-transactions"].Failed = &failed
	if baselineIdentity == changed.Identity() {
		t.Fatal("failed=false filter change did not alter WAL identity")
	}
	if c.Identity() != baselineIdentity {
		t.Fatal("subscription identity is not deterministic")
	}

	withoutAccounts := SubscribeRequest(c, nil, false)
	if len(withoutAccounts.GetAccounts()) != 0 {
		t.Fatal("withAccounts=false retained account filters")
	}
}

func TestCustomSubscriptionRejectsRunnerOwnedFields(t *testing.T) {
	c := customSubscriptionConfig(t)
	from := uint64(1)
	c.Subscription.FromSlot = &from
	if err := c.Validate(); err == nil {
		t.Fatal("caller-controlled FromSlot accepted")
	}
}

func TestLegacySubscriptionStillExcludesVoteAndFailedTransactions(t *testing.T) {
	c := Config{Commitment: "finalized", Programs: []string{PumpProgram}}
	req := SubscribeRequest(c, nil, true)
	filter := req.GetTransactions()["pump"]
	if filter == nil || filter.GetVote() || filter.GetFailed() {
		t.Fatalf("legacy defaults changed: %+v", filter)
	}
}

func TestMessageAndQueueByteLimitValidation(t *testing.T) {
	c := customSubscriptionConfig(t)
	c.QueueBytes = 128 << 20
	c.BatchBytes = 16 << 20
	c.MaxMessageBytes = 95 << 20
	if err := c.Validate(); err != nil {
		t.Fatalf("95 MiB message with a safe WAL envelope rejected: %v", err)
	}
	c.MaxMessageBytes = 96 << 20
	if err := c.Validate(); err == nil {
		t.Fatal("96 MiB message limit accepted despite 128 MiB base64 WAL frame")
	}
	c.MaxMessageBytes = 95 << 20
	c.QueueBytes = 95 << 20
	if err := c.Validate(); err == nil {
		t.Fatal("queue budget that cannot hold one max-sized record accepted")
	}
}
