package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
	"github.com/sirdont0/solana-yellowstone-grpc-data-core/examples/pumpfuncreate"
)

// Settings are intentionally non-secret. Provider details and credentials
// come only from the environment, never from config.json.
type settings struct {
	SourceCount         int     `json:"source_count"`
	Commitment          string  `json:"commitment"`
	Delivery            string  `json:"delivery"`
	FilterVersion       string  `json:"filter_version"`
	DataDir             string  `json:"data_dir"`
	QueueCapacity       int     `json:"queue_capacity"`
	DedupeLimit         int     `json:"dedupe_limit"`
	PingIntervalSeconds int     `json:"ping_interval_seconds"`
	IdleTimeoutSeconds  int     `json:"idle_timeout_seconds"`
	ReplayOverlap       uint64  `json:"replay_overlap"`
	MaxReconnects       int     `json:"max_reconnects"`
	FromSlot            *uint64 `json:"from_slot,omitempty"`
	DecodeErrorPolicy   string  `json:"decode_error_policy"`
	HandlerErrorPolicy  string  `json:"handler_error_policy"`
	TolerantReplayTail  bool    `json:"tolerant_replay_tail"`
	providers           []core.Source
}

func defaults() settings {
	return settings{SourceCount: 1, Commitment: "finalized", Delivery: "provisional",
		FilterVersion: "pumpfun-create-v1", DataDir: "./run", QueueCapacity: 4096,
		DedupeLimit: 2_000_000, PingIntervalSeconds: 15, IdleTimeoutSeconds: 45, ReplayOverlap: 64, DecodeErrorPolicy: "fail_closed", HandlerErrorPolicy: "fail_closed"}
}

func getenv(primary, legacy, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(primary)); v != "" {
		return v
	}
	if legacy != "" {
		if v := strings.TrimSpace(os.Getenv(legacy)); v != "" {
			return v
		}
	}
	return fallback
}

func loadSettings() (settings, error) {
	c := defaults()
	path := getenv("CORE_CONFIG", "", "config.json")
	f, err := os.Open(path)
	if err == nil {
		defer f.Close()
		decoder := json.NewDecoder(f)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&c); err != nil {
			return c, fmt.Errorf("read config %s: %w", path, err)
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			return c, fmt.Errorf("config %s: expected a single JSON object", path)
		}
	} else if !os.IsNotExist(err) || os.Getenv("CORE_CONFIG") != "" {
		return c, fmt.Errorf("open config %s: %w", path, err)
	}
	if c.SourceCount < 1 || c.SourceCount > 2 {
		return c, errors.New("source_count must be 1 or 2 (one gRPC stream per configured source)")
	}
	if c.Delivery != "provisional" && c.Delivery != "durable" {
		return c, errors.New("delivery must be provisional or durable")
	}
	if c.Commitment != "processed" && c.Commitment != "confirmed" && c.Commitment != "finalized" {
		return c, errors.New("commitment must be processed, confirmed or finalized")
	}
	if c.FilterVersion == "" || c.DataDir == "" || c.QueueCapacity < 1 || c.DedupeLimit < 1 ||
		c.PingIntervalSeconds < 1 || c.IdleTimeoutSeconds <= c.PingIntervalSeconds {
		return c, errors.New("invalid filter_version, data_dir, queue_capacity, dedupe_limit or timeout settings")
	}
	if c.MaxReconnects < 0 {
		return c, errors.New("max_reconnects must be nonnegative")
	}
	if _, err := parsePolicy(c.DecodeErrorPolicy); err != nil {
		return c, err
	}
	if _, err := parsePolicy(c.HandlerErrorPolicy); err != nil {
		return c, err
	}
	a := core.Source{
		ID: "primary", Endpoint: getenv("CORE_PROVIDER_A_ENDPOINT", "CORE_ENDPOINT", "solana-yellowstone-grpc.publicnode.com:443"),
		Token:       getenv("CORE_PROVIDER_A_TOKEN", "CORE_TOKEN", ""),
		TokenHeader: getenv("CORE_PROVIDER_A_TOKEN_HEADER", "CORE_TOKEN_HEADER", "x-token"),
		Commitment:  c.Commitment, FilterVersion: c.FilterVersion, Programs: []string{pumpfuncreate.ProgramID},
	}
	c.providers = []core.Source{a}
	if os.Getenv("CORE_REPLAY_ONLY") != "1" {
		if err := requirePublicNodeToken(a, "CORE_PROVIDER_A_TOKEN"); err != nil {
			return c, err
		}
	}
	if c.SourceCount == 2 {
		endpoint := getenv("CORE_PROVIDER_B_ENDPOINT", "CORE_ENDPOINT_B", "")
		if endpoint == "" {
			return c, errors.New("source_count=2 requires CORE_PROVIDER_B_ENDPOINT")
		}
		secondary := core.Source{
			ID: "secondary", Endpoint: endpoint,
			Token:       getenv("CORE_PROVIDER_B_TOKEN", "CORE_TOKEN_B", ""),
			TokenHeader: getenv("CORE_PROVIDER_B_TOKEN_HEADER", "", "x-token"),
			Commitment:  c.Commitment, FilterVersion: c.FilterVersion, Programs: []string{pumpfuncreate.ProgramID},
		}
		if os.Getenv("CORE_REPLAY_ONLY") != "1" {
			if err := requirePublicNodeToken(secondary, "CORE_PROVIDER_B_TOKEN"); err != nil {
				return c, err
			}
		}
		c.providers = append(c.providers, secondary)
	}
	return c, nil
}

// PublicNode's Yellowstone endpoint requires a personal token. Fail before
// creating a network connection instead of retrying a permanent auth error.
// Other providers may intentionally allow unauthenticated subscriptions.
func requirePublicNodeToken(source core.Source, envName string) error {
	if strings.EqualFold(source.Endpoint, "solana-yellowstone-grpc.publicnode.com:443") && source.Token == "" {
		return fmt.Errorf("%s is required for PublicNode Yellowstone gRPC (see https://www.allnodes.com/publicnode)", envName)
	}
	return nil
}

func (c settings) coreConfig() core.Config {
	mode := core.Provisional
	if c.Delivery == "durable" {
		mode = core.Durable
	}
	policy, _ := parsePolicy(c.DecodeErrorPolicy)
	return core.Config{
		Sources: c.providers, DataDir: c.DataDir, Delivery: mode,
		ReplayOverlap: c.ReplayOverlap, MaxReconnects: c.MaxReconnects, FromSlot: c.FromSlot, DecodeErrorPolicy: policy,
		Logger:        slog.New(slog.NewJSONHandler(os.Stderr, nil)),
		QueueCapacity: c.QueueCapacity, DedupeLimit: c.DedupeLimit,
		PingInterval: time.Duration(c.PingIntervalSeconds) * time.Second,
		IdleTimeout:  time.Duration(c.IdleTimeoutSeconds) * time.Second,
	}
}

func parsePolicy(s string) (core.ErrorPolicy, error) {
	switch s {
	case "fail_closed":
		return core.FailClosed, nil
	case "skip_and_flag":
		return core.SkipAndFlag, nil
	default:
		return core.FailClosed, errors.New("error policy must be fail_closed or skip_and_flag")
	}
}
