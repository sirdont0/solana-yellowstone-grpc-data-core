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
)

const (
	PumpProgram      = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
	PumpSwapProgram  = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
	TokenProgram     = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	Token2022Program = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
)

type Config struct {
	Logger            *slog.Logger `json:"-"`
	SourceID          string
	Endpoint          string
	Token             string `json:"-"`
	TokenHeader       string
	Commitment        string
	FilterVersion     string
	Programs          []string
	DataDir           string
	Mint              string
	TokenAccountOwner string
	RPCURL            string `json:"-"`
	RPCMaxTxVersion   uint64
	ReplayOverlap     uint64
	PingInterval      time.Duration
	IdleTimeout       time.Duration
	MaxMessageBytes   int
	ForceDropAfter    int
	MaxReconnects     int
	Streams           int
	DedupeLimit       int
	FromSlot          *uint64
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
	if mib == 0 || mib > 256 {
		return c, errors.New("YLAB_MAX_MESSAGE_MIB must be 1..256")
	}
	c.MaxMessageBytes = int(mib) << 20
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
	if c.SourceID != "" && !regexp.MustCompile(`^[a-z][a-z0-9_-]{0,47}$`).MatchString(c.SourceID) {
		return errors.New("source ID must be a safe lower-case name")
	}
	if c.Endpoint == "" || strings.TrimSpace(c.TokenHeader) == "" || c.FilterVersion == "" || c.DataDir == "" {
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
	if len(c.Programs) == 0 {
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
	if c.Streams < 1 || c.Streams > 2 || c.DedupeLimit < 1 || c.ForceDropAfter < 0 || c.MaxReconnects < 0 {
		return errors.New("invalid streams/dedupe/fault configuration")
	}
	if c.PingInterval <= 0 || c.IdleTimeout <= c.PingInterval {
		return errors.New("idle timeout must be greater than ping interval")
	}
	return nil
}

// Identity binds the WAL to the exact subscription shape. It excludes credentials.
func (c Config) Identity() string {
	v := struct {
		SourceID                                                     string `json:"SourceID,omitempty"`
		Endpoint, Commitment, FilterVersion, Mint, TokenAccountOwner string
		Programs                                                     []string
	}{c.SourceID, c.Endpoint, c.Commitment, c.FilterVersion, c.Mint, c.TokenAccountOwner, c.Programs}
	b, _ := json.Marshal(v)
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
