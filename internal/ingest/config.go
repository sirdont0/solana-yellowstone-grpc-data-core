package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mr-tron/base58"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

const (
	PumpProgram      = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
	PumpSwapProgram  = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
	TokenProgram     = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	Token2022Program = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
)

type Config struct {
	Logger        *slog.Logger `json:"-"`
	SourceID      string
	Endpoint      string
	Token         string `json:"-"`
	TokenHeader   string
	Commitment    string
	FilterVersion string
	Programs      []string
	// Subscription overrides the legacy Pump-oriented filters. Commitment and
	// FromSlot are owned by the runner; Ping is always generated internally.
	Subscription      *pb.SubscribeRequest
	DataDir           string
	Mint              string
	TokenAccountOwner string
	RPCURL            string `json:"-"`
	RPCMaxTxVersion   uint64
	ReplayOverlap     uint64
	PingInterval      time.Duration
	IdleTimeout       time.Duration
	MaxMessageBytes   int
	QueueBytes        int64
	BatchBytes        int64
	ForceDropAfter    int
	MaxReconnects     int
	Streams           int
	// DedupeDisk enables an exact disk-backed index. In this mode DedupeLimit
	// bounds only the in-memory hot cache, not the journal lifetime.
	DedupeDisk  bool
	DedupeLimit int
	FromSlot    *uint64
	// Replay rejection is terminal by default. Explicit opt-in may continue
	// live with a durable continuity break when independent repair is planned.
	AllowLiveFallbackOnReplayRejection bool
	// WALVersion selects the storage container. Zero and one use the legacy
	// single-file YLABWAL1 journal; two opts into the segmented YLABWAL2 store.
	WALVersion      int
	WALSegmentBytes int64
}

func FromEnv() (Config, error) {
	c := Config{
		Endpoint:          val("YLAB_ENDPOINT", "solana-yellowstone-grpc.publicnode.com:443"),
		Token:             strings.TrimSpace(os.Getenv("YLAB_TOKEN")),
		TokenHeader:       val("YLAB_TOKEN_HEADER", "x-token"),
		Commitment:        val("YLAB_COMMITMENT", "finalized"),
		FilterVersion:     val("YLAB_FILTER_VERSION", "pump-pumpswap-v1"),
		Programs:          split(val("YLAB_PROGRAMS", PumpProgram+","+PumpSwapProgram)),
		DataDir:           val("YLAB_DATA_DIR", "./run"),
		Mint:              strings.TrimSpace(os.Getenv("YLAB_MINT")),
		TokenAccountOwner: val("YLAB_TOKEN_ACCOUNT_OWNER", TokenProgram),
		RPCURL:            strings.TrimSpace(os.Getenv("YLAB_RPC_URL")),
		QueueBytes:        128 << 20,
		BatchBytes:        16 << 20,
		WALVersion:        1,
		WALSegmentBytes:   walV2DefaultSegmentBytes,
	}
	var err error
	if c.RPCMaxTxVersion, err = envUint("YLAB_RPC_MAX_TX_VERSION", 1); err != nil {
		return c, err
	}
	if c.RPCMaxTxVersion > 2 {
		return c, fmt.Errorf("YLAB_RPC_MAX_TX_VERSION must be <=2")
	}
	if c.ReplayOverlap, err = envUint("YLAB_REPLAY_OVERLAP", 64); err != nil {
		return c, err
	}
	if c.PingInterval, err = envDuration("YLAB_PING_INTERVAL", 15*time.Second); err != nil {
		return c, err
	}
	if c.IdleTimeout, err = envDuration("YLAB_IDLE_TIMEOUT", 45*time.Second); err != nil {
		return c, err
	}
	var mib uint64
	if mib, err = envUint("YLAB_MAX_MESSAGE_MIB", 64); err != nil {
		return c, err
	}
	if mib == 0 || mib > 95 {
		return c, errors.New("YLAB_MAX_MESSAGE_MIB must be 1..95 so a protobuf update fits a v1 WAL frame")
	}
	c.MaxMessageBytes = int(mib) << 20
	if mib, err = envUint("YLAB_QUEUE_MIB", uint64(c.QueueBytes>>20)); err != nil {
		return c, err
	}
	if mib == 0 || mib > 4096 {
		return c, errors.New("YLAB_QUEUE_MIB must be 1..4096")
	}
	c.QueueBytes = int64(mib) << 20
	if mib, err = envUint("YLAB_BATCH_MIB", uint64(c.BatchBytes>>20)); err != nil {
		return c, err
	}
	if mib == 0 || mib > 4096 {
		return c, errors.New("YLAB_BATCH_MIB must be 1..4096")
	}
	c.BatchBytes = int64(mib) << 20
	if c.ForceDropAfter, err = envInt("YLAB_FORCE_DROP_AFTER", 0); err != nil {
		return c, err
	}
	if c.MaxReconnects, err = envInt("YLAB_MAX_RECONNECTS", 0); err != nil {
		return c, err
	}
	if c.Streams, err = envInt("YLAB_STREAMS", 1); err != nil {
		return c, err
	}
	if c.DedupeLimit, err = envInt("YLAB_DEDUPE_LIMIT", 2000000); err != nil {
		return c, err
	}
	if raw := strings.TrimSpace(os.Getenv("YLAB_DEDUPE_DISK")); raw != "" {
		if c.DedupeDisk, err = strconv.ParseBool(raw); err != nil {
			return c, fmt.Errorf("YLAB_DEDUPE_DISK: %w", err)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("YLAB_WAL_VERSION")); raw != "" {
		if c.WALVersion, err = strconv.Atoi(raw); err != nil {
			return c, fmt.Errorf("YLAB_WAL_VERSION: %w", err)
		}
	}
	if mib, parseErr := envUint("YLAB_WAL_SEGMENT_MIB", uint64(c.WALSegmentBytes>>20)); parseErr != nil {
		return c, parseErr
	} else {
		c.WALSegmentBytes = int64(mib) << 20
	}
	if s := strings.TrimSpace(os.Getenv("YLAB_FROM_SLOT")); s != "" {
		v, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			return c, fmt.Errorf("YLAB_FROM_SLOT: %w", e)
		}
		c.FromSlot = &v
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.WALVersion < 0 || c.WALVersion > 2 {
		return errors.New("WAL version must be 1 or 2")
	}
	if c.WALVersion == 2 {
		segmentBytes := c.WALSegmentBytes
		if segmentBytes == 0 {
			segmentBytes = walV2DefaultSegmentBytes
		}
		if segmentBytes < 2<<20 || segmentBytes > 4<<30 || (c.BatchBytes > 0 && c.BatchBytes > segmentBytes) {
			return errors.New("WAL v2 segment size must be 2 MiB..4 GiB and fit the configured batch")
		}
		// A valid gRPC update must fit even in a fresh segment. Reserve the
		// maximum v2 record metadata, the frame/header envelopes, and the
		// footer required when this segment is sealed. The batch writer may
		// further limit or split groups; this is the single-message floor.
		if c.MaxMessageBytes > 0 && c.MaxMessageBytes <= 95<<20 {
			const segmentHeaderEnvelope = 18
			minimum := int64(c.MaxMessageBytes) + walV2MaxHeaderBytes + walV2FramePrefixBytes + 8 + walV2MaxHeaderBytes + segmentHeaderEnvelope + walV2FooterBytes
			if segmentBytes < minimum {
				return fmt.Errorf("WAL v2 segment size %d cannot fit an admitted %d-byte message and WAL envelopes (need at least %d bytes)", segmentBytes, c.MaxMessageBytes, minimum)
			}
		}
	}
	if c.SourceID != "" && !regexp.MustCompile(`^[a-z][a-z0-9_-]{0,47}$`).MatchString(c.SourceID) {
		return errors.New("source ID must be a safe lower-case name")
	}
	if c.Endpoint == "" || strings.TrimSpace(c.TokenHeader) == "" || c.FilterVersion == "" || len(c.FilterVersion) > 128 || c.DataDir == "" {
		return errors.New("endpoint, token header, filter version and data dir required")
	}
	target := c.Endpoint
	if strings.Contains(target, "://") {
		u, err := url.Parse(target)
		if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "grpc" && u.Scheme != "grpcs") {
			return errors.New("endpoint must be host:port with optional grpc(s)/http(s) scheme and no credentials or path")
		}
		target = u.Host
	}
	if host, port, err := net.SplitHostPort(target); err != nil || host == "" || port == "" {
		return errors.New("endpoint must include host and port")
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`).MatchString(c.TokenHeader) || strings.HasSuffix(strings.ToLower(c.TokenHeader), "-bin") || strings.HasPrefix(strings.ToLower(c.TokenHeader), "grpc-") {
		return errors.New("invalid token header")
	}
	if strings.ContainsAny(c.Token, "\r\n") {
		return errors.New("invalid token characters")
	}
	if c.Token != "" && (strings.HasPrefix(strings.ToLower(c.Endpoint), "grpc://") || strings.HasPrefix(strings.ToLower(c.Endpoint), "http://")) {
		return errors.New("provider token requires TLS; plaintext gRPC endpoint is forbidden")
	}
	if c.Commitment != "processed" && c.Commitment != "confirmed" && c.Commitment != "finalized" {
		return fmt.Errorf("invalid commitment: %s", c.Commitment)
	}
	if c.Subscription == nil && len(c.Programs) == 0 {
		return errors.New("at least one program required")
	}
	for _, s := range append(append([]string{}, c.Programs...), c.Mint, c.TokenAccountOwner) {
		if s == "" {
			continue
		}
		b, e := base58.Decode(s)
		if e != nil || len(b) != 32 {
			return fmt.Errorf("invalid Solana pubkey %q", s)
		}
	}
	if c.MaxMessageBytes < 1 || c.MaxMessageBytes > 95<<20 {
		return errors.New("max message bytes must be 1..95 MiB to fit the v1 WAL frame")
	}
	if c.QueueBytes < 0 || c.BatchBytes < 0 || (c.QueueBytes > 0 && c.QueueBytes < int64(c.MaxMessageBytes)+1024) || (c.BatchBytes > 0 && c.QueueBytes > 0 && c.BatchBytes > c.QueueBytes) {
		return errors.New("invalid queue capacity/byte budget")
	}
	if c.Subscription != nil {
		if c.Subscription.FromSlot != nil || c.Subscription.Ping != nil {
			return errors.New("subscription must not set FromSlot or Ping; the runner owns both fields")
		}
		if proto.Size(c.Subscription) > 1<<20 {
			return errors.New("custom subscription exceeds 1 MiB")
		}
		if len(c.Subscription.Transactions)+len(c.Subscription.TransactionsStatus)+len(c.Subscription.Accounts)+len(c.Subscription.Slots)+len(c.Subscription.BlocksMeta)+len(c.Subscription.Blocks)+len(c.Subscription.Entry)+len(c.Subscription.BlockFooter)+len(c.Subscription.AccountsDataSlice) == 0 {
			return errors.New("custom subscription must contain at least one filter")
		}
		for name, filter := range c.Subscription.Transactions {
			if name == "" || filter == nil {
				return errors.New("custom transaction filters require nonempty names and values")
			}
		}
		for name, filter := range c.Subscription.Accounts {
			if name == "" || filter == nil {
				return errors.New("custom account filters require nonempty names and values")
			}
		}
		for name, filter := range c.Subscription.TransactionsStatus {
			if name == "" || filter == nil {
				return errors.New("custom transaction-status filters require nonempty names and values")
			}
		}
		for name, filter := range c.Subscription.Slots {
			if name == "" || filter == nil {
				return errors.New("custom slot filters require nonempty names and values")
			}
		}
		for name, filter := range c.Subscription.BlocksMeta {
			if name == "" || filter == nil {
				return errors.New("custom block-meta filters require nonempty names and values")
			}
		}
		for name, filter := range c.Subscription.Blocks {
			if name == "" || filter == nil {
				return errors.New("custom block filters require nonempty names and values")
			}
		}
		for name, filter := range c.Subscription.Entry {
			if name == "" || filter == nil {
				return errors.New("custom entry filters require nonempty names and values")
			}
		}
		for name, filter := range c.Subscription.BlockFooter {
			if name == "" || filter == nil {
				return errors.New("custom block-footer filters require nonempty names and values")
			}
		}
	}
	if c.Streams < 1 || c.Streams > 2 || c.DedupeLimit < 1 || c.ForceDropAfter < 0 || c.MaxReconnects < 0 {
		return errors.New("invalid streams/dedupe/fault configuration")
	}
	if c.PingInterval <= 0 || c.IdleTimeout <= c.PingInterval {
		return errors.New("idle timeout must be greater than ping interval")
	}
	return nil
}

// Identity binds the WAL to the subscription profile without credentials.
// The default subscription keeps the published v0.1.1 identity bytes, so an
// unchanged legacy journal remains readable after upgrading. Explicit custom
// subscriptions are additionally bound to their deterministic protobuf shape.
func (c Config) Identity() string {
	legacy := struct {
		SourceID                                                     string `json:"SourceID,omitempty"`
		Endpoint, Commitment, FilterVersion, Mint, TokenAccountOwner string
		Programs                                                     []string
	}{c.SourceID, c.Endpoint, c.Commitment, c.FilterVersion, c.Mint, c.TokenAccountOwner, c.Programs}
	if c.Subscription == nil {
		b, _ := json.Marshal(legacy)
		return hashIdentityBytes(b)
	}
	subscription := SubscribeRequest(c, nil, true)
	subscription.FromSlot = nil
	subscription.Ping = nil
	subscriptionBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(subscription)
	subscriptionHash := sha256.Sum256(subscriptionBytes)
	v := struct {
		SourceID                                                                   string `json:"SourceID,omitempty"`
		Endpoint, Commitment, FilterVersion, Mint, TokenAccountOwner, Subscription string
		Programs                                                                   []string
	}{c.SourceID, c.Endpoint, c.Commitment, c.FilterVersion, c.Mint, c.TokenAccountOwner, hex.EncodeToString(subscriptionHash[:]), c.Programs}
	b, _ := json.Marshal(v)
	return hashIdentityBytes(b)
}

func hashIdentityBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (c Config) CommitmentEnum() pb.CommitmentLevel {
	switch c.Commitment {
	case "processed":
		return pb.CommitmentLevel_PROCESSED
	case "confirmed":
		return pb.CommitmentLevel_CONFIRMED
	default:
		return pb.CommitmentLevel_FINALIZED
	}
}

func val(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}
func split(s string) []string {
	var out []string
	for _, x := range strings.Split(s, ",") {
		x = strings.TrimSpace(x)
		if x != "" {
			out = append(out, x)
		}
	}
	return out
}
func envUint(k string, d uint64) (uint64, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d, nil
	}
	x, e := strconv.ParseUint(v, 10, 64)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return x, nil
}
func envInt(k string, d int) (int, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d, nil
	}
	x, e := strconv.Atoi(v)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return x, nil
}
func envDuration(k string, d time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d, nil
	}
	x, e := time.ParseDuration(v)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return x, nil
}
