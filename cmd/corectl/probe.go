package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mr-tron/base58"
	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The probe reports only observed wire behavior. It does not create a WAL,
// verify full bank inclusion, or certify historical replay completeness.
// In particular, absence of matching updates during a short sample is UNKNOWN.
type providerProbeOptions struct {
	ProviderID          string
	Endpoint            string
	Token               string
	TokenHeader         string
	Commitment          pb.CommitmentLevel
	CommitmentName      string
	Program             string
	Kinds               []string
	FromSlot            *uint64
	SampleDuration      time.Duration
	CallTimeout         time.Duration
	MaxUpdates          int
	IncludeCertificates bool
	ReopenOverlap       bool
	ProbeRetentionEdge  bool
	CoreRevision        string
}

type probeCall struct {
	Status         string  `json:"status"`
	GRPCCode       string  `json:"grpc_code,omitempty"`
	VersionSHA256  string  `json:"version_sha256,omitempty"`
	VersionLength  int     `json:"version_length,omitempty"`
	Slot           *uint64 `json:"slot,omitempty"`
	FirstAvailable *uint64 `json:"first_available,omitempty"`
}

type probeStream struct {
	Status                      string         `json:"status"`
	GRPCCode                    string         `json:"grpc_code,omitempty"`
	RequestSHA256               string         `json:"request_sha256"`
	RequestedFromSlot           *uint64        `json:"requested_from_slot,omitempty"`
	ObservedUpdates             int            `json:"observed_updates"`
	ObservedKinds               map[string]int `json:"observed_kinds"`
	FirstObservedSlot           *uint64        `json:"first_observed_slot,omitempty"`
	LastObservedSlot            *uint64        `json:"last_observed_slot,omitempty"`
	BankIDPresentUpdates        int            `json:"bank_id_present_updates"`
	FooterWithCertificates      int            `json:"footer_with_certificates"`
	CertificateSlotDiffers      int            `json:"certificate_slot_differs"`
	UnknownFieldBearingUpdates  int            `json:"unknown_field_bearing_updates"`
	ElapsedMillis               int64          `json:"elapsed_ms"`
	MatchingObservedDigestCount int            `json:"matching_observed_digests,omitempty"`
}

type probeProfile struct {
	First         probeStream  `json:"first"`
	Reopen        *probeStream `json:"reopen_overlap,omitempty"`
	RetentionEdge *probeStream `json:"retention_boundary,omitempty"`
}

type providerProbeReport struct {
	SchemaVersion       int                     `json:"schema_version"`
	ProviderID          string                  `json:"provider_id"`
	EndpointSHA256      string                  `json:"endpoint_sha256"`
	CoreRevision        string                  `json:"core_revision,omitempty"`
	StartedAtUTC        string                  `json:"started_at_utc"`
	Commitment          string                  `json:"commitment"`
	AuthConfigured      bool                    `json:"auth_configured"`
	TokenHeader         string                  `json:"token_header"`
	SampleMillis        int64                   `json:"sample_ms"`
	Dial                probeCall               `json:"dial"`
	GetVersion          probeCall               `json:"get_version"`
	GetSlot             probeCall               `json:"get_slot"`
	SubscribeReplayInfo probeCall               `json:"subscribe_replay_info"`
	Streams             map[string]probeProfile `json:"streams"`
	Scope               string                  `json:"scope"`
}

var probeTokenHeaderPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

func probe(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	endpoint := fs.String("endpoint", os.Getenv("YELLOWSTONE_ENDPOINT"), "Yellowstone endpoint (never printed; SHA-256 only)")
	providerID := fs.String("provider-id", "primary", "non-secret provider alias")
	tokenEnv := fs.String("token-env", "YELLOWSTONE_TOKEN", "environment variable holding the provider token")
	tokenHeader := fs.String("token-header", "x-token", "gRPC token metadata header")
	commitment := fs.String("commitment", "finalized", "processed, confirmed, finalized")
	kinds := fs.String("kinds", "slots,blocks_meta,block_footer", "comma-separated: slots,transactions,transactions_status,accounts,blocks_meta,blocks,entry,block_footer,combined or all")
	program := fs.String("program", "", "Solana program pubkey for bounded transactions/accounts/blocks filters")
	fromSlot := fs.String("from-slot", "", "optional historical replay starting slot")
	includeCerts := fs.Bool("include-certificates", true, "request available Alpenglow footer certificates")
	sample := fs.Duration("sample", 5*time.Second, "per-filter sample duration")
	timeout := fs.Duration("timeout", 5*time.Second, "unary RPC timeout")
	maxUpdates := fs.Int("max-updates", 100, "maximum updates per filter")
	reopen := fs.Bool("reopen-overlap", false, "close and reopen each sampled stream from its first observed slot")
	retentionEdge := fs.Bool("retention-boundary", false, "request first_available-1 when replay info reports a positive boundary; bounded negative probe")
	coreRevision := fs.String("core-revision", "", "optional Git SHA of the tested Core build")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(fs.Args()) != 0 {
		return fmt.Errorf("unexpected probe arguments: %q", fs.Args())
	}
	if strings.TrimSpace(*endpoint) == "" {
		return errors.New("--endpoint or YELLOWSTONE_ENDPOINT is required")
	}
	if strings.TrimSpace(*providerID) == "" {
		return errors.New("--provider-id must be nonempty")
	}
	if !probeTokenHeaderPattern.MatchString(strings.ToLower(*tokenHeader)) {
		return errors.New("--token-header must be an ASCII gRPC metadata key")
	}
	if *sample <= 0 || *sample > 2*time.Minute || *timeout <= 0 || *timeout > time.Minute {
		return errors.New("--sample must be 0..2m and --timeout must be 0..1m, both positive")
	}
	if *maxUpdates <= 0 || *maxUpdates > 10000 {
		return errors.New("--max-updates must be 1..10000")
	}
	var level pb.CommitmentLevel
	switch strings.ToLower(*commitment) {
	case "processed":
		level = pb.CommitmentLevel_PROCESSED
	case "confirmed":
		level = pb.CommitmentLevel_CONFIRMED
	case "finalized":
		level = pb.CommitmentLevel_FINALIZED
	default:
		return errors.New("--commitment must be processed, confirmed or finalized")
	}
	var from *uint64
	if *fromSlot != "" {
		s, err := strconv.ParseUint(*fromSlot, 10, 64)
		if err != nil {
			return errors.New("--from-slot must be a uint64")
		}
		from = &s
	}
	selected, err := parseProbeKinds(*kinds)
	if err != nil {
		return err
	}
	if requiresProbeProgram(selected) {
		raw, e := base58.Decode(*program)
		if e != nil || len(raw) != 32 {
			return errors.New("--program must be a valid 32-byte Solana pubkey when probing transactions, accounts, blocks or combined")
		}
	}
	opts := providerProbeOptions{
		ProviderID: *providerID, Endpoint: strings.TrimSpace(*endpoint),
		Token: strings.TrimSpace(os.Getenv(*tokenEnv)), TokenHeader: strings.ToLower(*tokenHeader),
		Commitment: level, CommitmentName: strings.ToLower(*commitment), Program: *program,
		Kinds: selected, FromSlot: from, SampleDuration: *sample, CallTimeout: *timeout,
		MaxUpdates: *maxUpdates, IncludeCertificates: *includeCerts,
		ReopenOverlap: *reopen, ProbeRetentionEdge: *retentionEdge, CoreRevision: *coreRevision,
	}
	report := runProviderProbe(context.Background(), opts)
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func parseProbeKinds(s string) ([]string, error) {
	all := []string{"slots", "transactions", "transactions_status", "accounts", "blocks_meta", "blocks", "entry", "block_footer", "combined"}
	if strings.TrimSpace(s) == "all" {
		return all, nil
	}
	valid := make(map[string]bool, len(all))
	for _, k := range all {
		valid[k] = true
	}
	seen := map[string]bool{}
	var kinds []string
	for _, raw := range strings.Split(s, ",") {
		k := strings.TrimSpace(raw)
		if !valid[k] {
			return nil, fmt.Errorf("unsupported probe kind %q", k)
		}
		if !seen[k] {
			seen[k] = true
			kinds = append(kinds, k)
		}
	}
	if len(kinds) == 0 {
		return nil, errors.New("at least one --kinds item is required")
	}
	return kinds, nil
}

func requiresProbeProgram(kinds []string) bool {
	for _, kind := range kinds {
		switch kind {
		case "transactions", "transactions_status", "accounts", "blocks", "combined":
			return true
		}
	}
	return false
}

func probeSubscribeRequest(o providerProbeOptions, kind string, from *uint64) *pb.SubscribeRequest {
	req := &pb.SubscribeRequest{Commitment: &o.Commitment, FromSlot: from}
	add := func(kind string) {
		switch kind {
		case "slots":
			req.Slots = map[string]*pb.SubscribeRequestFilterSlots{"probe_slots": {}}
		case "transactions":
			req.Transactions = map[string]*pb.SubscribeRequestFilterTransactions{"probe_transactions": {AccountInclude: []string{o.Program}}}
		case "transactions_status":
			req.TransactionsStatus = map[string]*pb.SubscribeRequestFilterTransactions{"probe_status": {AccountInclude: []string{o.Program}}}
		case "accounts":
			req.Accounts = map[string]*pb.SubscribeRequestFilterAccounts{"probe_accounts": {Owner: []string{o.Program}}}
		case "blocks_meta":
			req.BlocksMeta = map[string]*pb.SubscribeRequestFilterBlocksMeta{"probe_meta": {}}
		case "blocks":
			req.Blocks = map[string]*pb.SubscribeRequestFilterBlocks{"probe_blocks": {AccountInclude: []string{o.Program}}}
		case "entry":
			req.Entry = map[string]*pb.SubscribeRequestFilterEntry{"probe_entries": {}}
		case "block_footer":
			incl := o.IncludeCertificates
			req.BlockFooter = map[string]*pb.SubscribeRequestFilterBlockFooter{"probe_footer": {IncludeCertificates: &incl}}
		}
	}
	if kind == "combined" {
		for _, k := range []string{"slots", "transactions", "transactions_status", "accounts", "blocks_meta", "blocks", "entry", "block_footer"} {
			add(k)
		}
	} else {
		add(kind)
	}
	return req
}

func probeAuthContext(ctx context.Context, o providerProbeOptions) context.Context {
	if o.Token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, o.TokenHeader, o.Token)
}

func probeSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func probeResultFor(err error) probeCall {
	if err == nil {
		return probeCall{Status: "PASS"}
	}
	code := status.Code(err)
	result := probeCall{Status: "FAIL", GRPCCode: code.String()}
	switch code {
	case codes.Unimplemented:
		result.Status = "UNSUPPORTED"
	case codes.DeadlineExceeded, codes.Canceled:
		result.Status = "UNKNOWN"
	}
	return result
}

func runProviderProbe(ctx context.Context, o providerProbeOptions) providerProbeReport {
	report := providerProbeReport{
		SchemaVersion: 1, ProviderID: o.ProviderID, EndpointSHA256: probeSHA([]byte(o.Endpoint)),
		CoreRevision: o.CoreRevision, StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano),
		Commitment: o.CommitmentName, AuthConfigured: o.Token != "",
		TokenHeader: o.TokenHeader, SampleMillis: o.SampleDuration.Milliseconds(),
		GetVersion: probeCall{Status: "NOT_RUN"}, GetSlot: probeCall{Status: "NOT_RUN"},
		SubscribeReplayInfo: probeCall{Status: "NOT_RUN"},
		Streams:             map[string]probeProfile{},
		Scope:               "Wire/endpoint observations only; successful RPC, matching update or replay overlap never proves canonical bank/account completeness.",
	}
	conn, err := lab.GRPCDial(ctx, lab.Config{
		Endpoint: o.Endpoint, Token: o.Token, TokenHeader: o.TokenHeader, MaxMessageBytes: 64 << 20,
	})
	report.Dial = probeResultFor(err)
	if err != nil {
		for _, kind := range o.Kinds {
			req := probeSubscribeRequest(o, kind, o.FromSlot)
			report.Streams[kind] = probeProfile{First: emptyProbeStream(req, "NOT_RUN")}
		}
		return report
	}
	defer conn.Close()
	client := pb.NewGeyserClient(conn)
	call := func(f func(context.Context) error) error {
		timed, cancel := context.WithTimeout(ctx, o.CallTimeout)
		defer cancel()
		return f(probeAuthContext(timed, o))
	}
	_ = call(func(callCtx context.Context) error {
		v, e := client.GetVersion(callCtx, &pb.GetVersionRequest{})
		report.GetVersion = probeResultFor(e)
		if e == nil {
			report.GetVersion.VersionSHA256 = probeSHA([]byte(v.GetVersion()))
			report.GetVersion.VersionLength = len(v.GetVersion())
		}
		return e
	})
	_ = call(func(callCtx context.Context) error {
		v, e := client.GetSlot(callCtx, &pb.GetSlotRequest{Commitment: &o.Commitment})
		report.GetSlot = probeResultFor(e)
		if e == nil {
			slot := v.GetSlot()
			report.GetSlot.Slot = &slot
		}
		return e
	})
	_ = call(func(callCtx context.Context) error {
		v, e := client.SubscribeReplayInfo(callCtx, &pb.SubscribeReplayInfoRequest{})
		report.SubscribeReplayInfo = probeResultFor(e)
		if e == nil && v.FirstAvailable != nil {
			slot := v.GetFirstAvailable()
			report.SubscribeReplayInfo.FirstAvailable = &slot
		}
		return e
	})
	authDenied := func(c probeCall) bool {
		return c.GRPCCode == codes.Unauthenticated.String() ||
			c.GRPCCode == codes.PermissionDenied.String()
	}
	authBlocked := authDenied(report.GetVersion) &&
		authDenied(report.GetSlot) &&
		authDenied(report.SubscribeReplayInfo)
	for _, kind := range o.Kinds {
		req := probeSubscribeRequest(o, kind, o.FromSlot)
		if authBlocked {
			report.Streams[kind] = probeProfile{First: emptyProbeStream(req, "NOT_RUN")}
			continue
		}
		first, firstDigests := sampleProbeStream(ctx, client, o, kind, req)
		profile := probeProfile{First: first}
		if o.ReopenOverlap && first.FirstObservedSlot != nil {
			replayFrom := *first.FirstObservedSlot
			reopenReq := probeSubscribeRequest(o, kind, &replayFrom)
			reopened, replayDigests := sampleProbeStream(ctx, client, o, kind, reopenReq)
			for digest := range replayDigests {
				if firstDigests[digest] {
					reopened.MatchingObservedDigestCount++
				}
			}
			profile.Reopen = &reopened
		}
		if o.ProbeRetentionEdge && report.SubscribeReplayInfo.Status == "PASS" &&
			report.SubscribeReplayInfo.FirstAvailable != nil && *report.SubscribeReplayInfo.FirstAvailable > 0 {
			boundary := *report.SubscribeReplayInfo.FirstAvailable - 1
			edgeReq := probeSubscribeRequest(o, kind, &boundary)
			edge, _ := sampleProbeStream(ctx, client, o, kind, edgeReq)
			profile.RetentionEdge = &edge
		}
		report.Streams[kind] = profile
	}
	return report
}

func emptyProbeStream(req *pb.SubscribeRequest, state string) probeStream {
	encoded, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(req)
	return probeStream{
		Status: state, RequestSHA256: probeSHA(encoded), RequestedFromSlot: req.FromSlot,
		ObservedKinds: map[string]int{},
	}
}

func sampleProbeStream(ctx context.Context, client pb.GeyserClient, o providerProbeOptions, kind string, req *pb.SubscribeRequest) (result probeStream, seen map[string]bool) {
	result = emptyProbeStream(req, "UNKNOWN")
	seen = map[string]bool{}
	start := time.Now()
	defer func() { result.ElapsedMillis = time.Since(start).Milliseconds() }()
	timed, cancel := context.WithTimeout(ctx, o.SampleDuration)
	defer cancel()
	stream, err := client.Subscribe(probeAuthContext(timed, o))
	if err != nil {
		call := probeResultFor(err)
		result.Status, result.GRPCCode = call.Status, call.GRPCCode
		return result, seen
	}
	if err := stream.Send(req); err != nil {
		call := probeResultFor(err)
		result.Status, result.GRPCCode = call.Status, call.GRPCCode
		return result, seen
	}
	for result.ObservedUpdates < o.MaxUpdates {
		update, err := stream.Recv()
		if err != nil {
			code := status.Code(err)
			if errors.Is(err, io.EOF) {
				result.GRPCCode = "EOF"
				result.Status = "FAIL"
			} else if timed.Err() != nil && (code == codes.Canceled || code == codes.DeadlineExceeded) {
				result.Status = probeStreamObservationStatus(kind, result.ObservedKinds)
			} else {
				call := probeResultFor(err)
				result.Status, result.GRPCCode = call.Status, call.GRPCCode
			}
			return result, seen
		}
		gotKind, slot, hasSlot, bankPresent := probeUpdateInfo(update)
		if gotKind == "ping" || gotKind == "pong" {
			continue // keepalive cannot prove the requested filter delivered data
		}
		result.ObservedUpdates++
		result.ObservedKinds[gotKind]++
		if hasSlot {
			if result.FirstObservedSlot == nil {
				v := slot
				result.FirstObservedSlot = &v
			}
			v := slot
			result.LastObservedSlot = &v
		}
		if bankPresent {
			result.BankIDPresentUpdates++
		}
		if footer := update.GetBlockFooter(); footer != nil {
			if footer.GetBlockFinalCert() != nil || footer.GetSkipRewardCert() != nil || footer.GetNotarRewardCert() != nil {
				result.FooterWithCertificates++
			}
			for _, certSlot := range []struct {
				present bool
				slot    uint64
			}{
				{footer.GetBlockFinalCert() != nil, footer.GetBlockFinalCert().GetSlot()},
				{footer.GetSkipRewardCert() != nil, footer.GetSkipRewardCert().GetSlot()},
				{footer.GetNotarRewardCert() != nil, footer.GetNotarRewardCert().GetSlot()},
			} {
				if certSlot.present && certSlot.slot != footer.GetSlot() {
					result.CertificateSlotDiffers++
				}
			}
		}
		if len(update.ProtoReflect().GetUnknown()) != 0 {
			result.UnknownFieldBearingUpdates++
		}
		if wire, e := (proto.MarshalOptions{Deterministic: true}).Marshal(update); e == nil {
			seen[probeSHA(wire)] = true
		}
	}
	result.Status = probeStreamObservationStatus(kind, result.ObservedKinds)
	return result, seen
}

func probeStreamObservationStatus(kind string, counts map[string]int) string {
	if kind == "combined" {
		for _, required := range []string{"slot", "transaction", "transaction_status", "account", "block_meta", "block", "entry", "block_footer"} {
			if counts[required] == 0 {
				return "UNKNOWN" // short sample did not demonstrate every requested class
			}
		}
		return "PASS"
	}
	expected := map[string]string{
		"slots": "slot", "transactions": "transaction", "transactions_status": "transaction_status",
		"accounts": "account", "blocks_meta": "block_meta", "blocks": "block",
		"entry": "entry", "block_footer": "block_footer",
	}[kind]
	if counts[expected] > 0 {
		return "PASS"
	}
	return "UNKNOWN"
}

func probeUpdateInfo(u *pb.SubscribeUpdate) (kind string, slot uint64, hasSlot, bankPresent bool) {
	switch v := u.UpdateOneof.(type) {
	case *pb.SubscribeUpdate_Account:
		return "account", v.Account.GetSlot(), true, v.Account.BankId != nil
	case *pb.SubscribeUpdate_Slot:
		return "slot", v.Slot.GetSlot(), true, v.Slot.BankId != nil
	case *pb.SubscribeUpdate_Transaction:
		return "transaction", v.Transaction.GetSlot(), true, true
	case *pb.SubscribeUpdate_TransactionStatus:
		return "transaction_status", v.TransactionStatus.GetSlot(), true, true
	case *pb.SubscribeUpdate_Block:
		return "block", v.Block.GetSlot(), true, true
	case *pb.SubscribeUpdate_BlockMeta:
		return "block_meta", v.BlockMeta.GetSlot(), true, true
	case *pb.SubscribeUpdate_Entry:
		return "entry", v.Entry.GetSlot(), true, true
	case *pb.SubscribeUpdate_BlockFooter:
		return "block_footer", v.BlockFooter.GetSlot(), true, true
	case *pb.SubscribeUpdate_EntryUpdateParent:
		return "entry_update_parent", v.EntryUpdateParent.GetSlot(), true, true
	case *pb.SubscribeUpdate_Ping:
		return "ping", 0, false, false
	case *pb.SubscribeUpdate_Pong:
		return "pong", 0, false, false
	default:
		return "unknown", 0, false, false
	}
}
