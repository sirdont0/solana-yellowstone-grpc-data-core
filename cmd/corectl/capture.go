package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

type repeatedStrings []string

func (s *repeatedStrings) String() string { return strings.Join(*s, ",") }
func (s *repeatedStrings) Set(v string) error {
	v = strings.TrimSpace(v)
	if v == "" {
		return errors.New("empty --program")
	}
	*s = append(*s, v)
	return nil
}

// No credentials are accepted in config files. A token is read only from its
// named environment variable, avoiding accidental publication of secrets.
type captureFile struct {
	DataDir                            string       `json:"data_dir"`
	WALVersion                         int          `json:"wal_version"`
	WALSegmentMiB                      int          `json:"wal_segment_mib"`
	DedupeDisk                         *bool        `json:"dedupe_disk"`
	MaxMessageMiB                      int          `json:"max_message_mib"`
	QueueMiB                           int          `json:"queue_mib"`
	SourceQueueMiB                     int          `json:"source_queue_mib"`
	BatchMiB                           int          `json:"batch_mib"`
	MaxReconnects                      int          `json:"max_reconnects"`
	ReplayOverlap                      uint64       `json:"replay_overlap"`
	AllowLiveFallbackOnReplayRejection bool         `json:"allow_live_fallback_on_replay_rejection"`
	FromSlot                           *uint64      `json:"from_slot"`
	Sources                            []sourceFile `json:"sources"`
}

type sourceFile struct {
	ID               string   `json:"id"`
	Endpoint         string   `json:"endpoint"`
	TokenEnv         string   `json:"token_env"`
	TokenHeader      string   `json:"token_header"`
	Commitment       string   `json:"commitment"`
	FilterVersion    string   `json:"filter_version"`
	Programs         []string `json:"programs"`
	SubscriptionFile string   `json:"subscription_file"`
	FailureDomain    string   `json:"failure_domain"`
}

func capture(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("capture", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "JSON config for 1..16 independent sources")
	dataDir := fs.String("data-dir", "./run", "WAL root directory")
	sourceID := fs.String("source-id", "primary", "source-local identifier")
	endpoint := fs.String("endpoint", os.Getenv("YELLOWSTONE_ENDPOINT"), "provider host:port or grpc(s)://host:port")
	tokenEnv := fs.String("token-env", "YELLOWSTONE_TOKEN", "name of environment variable holding provider token")
	tokenHeader := fs.String("token-header", "x-token", "provider authentication header")
	commitment := fs.String("commitment", "finalized", "processed, confirmed, finalized")
	filterVersion := fs.String("filter-version", "raw-v1", "user-managed subscription revision")
	subFile := fs.String("subscription", "", "SubscribeRequest protobuf JSON file")
	walVersion := fs.Int("wal-version", 1, "WAL version (1 stable; 2 experimental)")
	segmentMiB := fs.Int("wal-segment-mib", 256, "WAL v2 segment size in MiB")
	dedupeDisk := fs.Bool("dedupe-disk", true, "use WAL-derived disk-backed dedupe index")
	maxMessage := fs.Int("max-message-mib", 64, "maximum Yellowstone protobuf update size in MiB")
	queueMiB := fs.Int("queue-mib", 256, "shared dispatch queue budget in MiB")
	sourceQueueMiB := fs.Int("source-queue-mib", 128, "per-source receive queue budget in MiB")
	batchMiB := fs.Int("batch-mib", 16, "fsync batch budget in MiB")
	maxReconnects := fs.Int("max-reconnects", 0, "maximum reconnects; 0 retries indefinitely")
	replayOverlap := fs.Uint64("replay-overlap", 64, "reconnect replay overlap in slots")
	allowReplayFallback := fs.Bool("allow-live-fallback-on-replay-rejection", false, "explicitly continue live when provider rejects replay, preserving uncertainty marker")
	fromSlot := fs.String("from-slot", "", "initial Yellowstone slot (optional)")
	dryRun := fs.Bool("dry-run", false, "validate settings and print redacted config without connecting")
	var programs repeatedStrings
	fs.Var(&programs, "program", "program ID for transaction account_include (repeatable)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("unexpected capture arguments: %v", fs.Args())
	}
	var file captureFile
	base := "."
	if *configPath != "" {
		var unexpected []string
		fs.Visit(func(f *flag.Flag) {
			if f.Name != "config" && f.Name != "dry-run" {
				unexpected = append(unexpected, f.Name)
			}
		})
		if len(unexpected) > 0 {
			return fmt.Errorf("--config cannot be combined with standalone source flags: %s", strings.Join(unexpected, ", "))
		}
		if err := readStrictJSON(*configPath, &file); err != nil {
			return err
		}
		base = filepath.Dir(*configPath)
	} else {
		if *endpoint == "" {
			return errors.New("--endpoint or YELLOWSTONE_ENDPOINT is required")
		}
		if len(programs) == 0 && *subFile == "" {
			return errors.New("provide --program or --subscription; unrestricted capture requires an explicit subscription JSON")
		}
		if len(programs) > 0 && *subFile != "" {
			return errors.New("choose either --program or --subscription")
		}
		file = captureFile{
			DataDir: *dataDir, WALVersion: *walVersion, WALSegmentMiB: *segmentMiB,
			DedupeDisk: dedupeDisk, MaxMessageMiB: *maxMessage,
			QueueMiB: *queueMiB, SourceQueueMiB: *sourceQueueMiB, BatchMiB: *batchMiB,
			MaxReconnects: *maxReconnects, ReplayOverlap: *replayOverlap,
			AllowLiveFallbackOnReplayRejection: *allowReplayFallback,
			Sources: []sourceFile{{ID: *sourceID, Endpoint: *endpoint, TokenEnv: *tokenEnv,
				TokenHeader: *tokenHeader, Commitment: *commitment, FilterVersion: *filterVersion,
				Programs: programs, SubscriptionFile: *subFile}},
		}
		if *fromSlot != "" {
			slot, err := strconv.ParseUint(*fromSlot, 10, 64)
			if err != nil {
				return fmt.Errorf("--from-slot: %w", err)
			}
			file.FromSlot = &slot
		}
	}
	cfg, err := buildCaptureConfig(file, base)
	if err != nil {
		return err
	}
	engine, err := core.NewRaw(cfg)
	if err != nil {
		return err
	}
	if *dryRun {
		for _, s := range cfg.Sources {
			_, _ = fmt.Fprintf(stdout, "source=%s endpoint=%s commitment=%s filter_version=%s transaction_filters=%d account_filters=%d slot_filters=%d block_meta_filters=%d\n",
				s.ID, s.Endpoint, s.Commitment, s.FilterVersion,
				len(s.Subscription.GetTransactions()), len(s.Subscription.GetAccounts()),
				len(s.Subscription.GetSlots()), len(s.Subscription.GetBlocksMeta()))
		}
		_, _ = fmt.Fprintf(stdout, "data_dir=%s wal_version=%d dedupe_disk=%t sources=%d\n", cfg.DataDir, cfg.WALVersion, cfg.DedupeDisk, len(cfg.Sources))
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	_, _ = fmt.Fprintf(stderr, "capturing %d Yellowstone source(s) into %s; interrupt to stop\n", len(cfg.Sources), cfg.DataDir)
	return engine.Run(ctx)
}

func readStrictJSON(path string, dst any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s: expected exactly one JSON object", path)
	}
	return nil
}

func buildCaptureConfig(f captureFile, base string) (core.Config, error) {
	if len(f.Sources) == 0 || len(f.Sources) > 16 {
		return core.Config{}, errors.New("capture config must contain 1..16 sources")
	}
	if f.DataDir == "" {
		f.DataDir = "./run"
	}
	if f.WALVersion == 0 {
		f.WALVersion = 1
	}
	if f.WALSegmentMiB == 0 {
		f.WALSegmentMiB = 256
	}
	if f.MaxMessageMiB == 0 {
		f.MaxMessageMiB = 64
	}
	if f.QueueMiB == 0 {
		f.QueueMiB = 256
	}
	if f.SourceQueueMiB == 0 {
		f.SourceQueueMiB = 128
	}
	if f.BatchMiB == 0 {
		f.BatchMiB = 16
	}
	if f.ReplayOverlap == 0 {
		f.ReplayOverlap = 64
	}
	dedupe := true
	if f.DedupeDisk != nil {
		dedupe = *f.DedupeDisk
	}
	if f.WALSegmentMiB < 0 || f.MaxMessageMiB < 0 || f.QueueMiB < 0 || f.SourceQueueMiB < 0 || f.BatchMiB < 0 || f.MaxReconnects < 0 {
		return core.Config{}, errors.New("negative capture limits are invalid")
	}
	cfg := core.Config{
		DataDir: f.DataDir, WALVersion: f.WALVersion, WALSegmentBytes: int64(f.WALSegmentMiB) << 20,
		DedupeDisk: dedupe, MaxMessageMiB: f.MaxMessageMiB,
		QueueBytes: int64(f.QueueMiB) << 20, SourceQueueBytes: int64(f.SourceQueueMiB) << 20,
		BatchBytes: int64(f.BatchMiB) << 20, MaxReconnects: f.MaxReconnects,
		ReplayOverlap: f.ReplayOverlap, FromSlot: f.FromSlot,
		AllowLiveFallbackOnReplayRejection: f.AllowLiveFallbackOnReplayRejection,
		ShutdownTimeout:                    15 * time.Second,
	}
	for _, spec := range f.Sources {
		if spec.ID == "" || spec.Endpoint == "" {
			return core.Config{}, errors.New("every source requires id and endpoint")
		}
		if len(spec.Programs) > 0 && spec.SubscriptionFile != "" {
			return core.Config{}, fmt.Errorf("source %s: choose programs or subscription_file", spec.ID)
		}
		if len(spec.Programs) == 0 && spec.SubscriptionFile == "" {
			return core.Config{}, fmt.Errorf("source %s: programs or subscription_file required", spec.ID)
		}
		s := core.Source{
			ID: spec.ID, Endpoint: spec.Endpoint, FailureDomain: spec.FailureDomain,
			TokenHeader: spec.TokenHeader, Commitment: spec.Commitment,
			FilterVersion: spec.FilterVersion,
		}
		if s.TokenHeader == "" {
			s.TokenHeader = "x-token"
		}
		if s.Commitment == "" {
			s.Commitment = "finalized"
		}
		if s.FilterVersion == "" {
			s.FilterVersion = "raw-v1"
		}
		if spec.TokenEnv != "" {
			s.Token = os.Getenv(spec.TokenEnv)
		}
		if spec.SubscriptionFile != "" {
			path := spec.SubscriptionFile
			if !filepath.IsAbs(path) {
				path = filepath.Join(base, path)
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return core.Config{}, err
			}
			var request pb.SubscribeRequest
			if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(b, &request); err != nil {
				return core.Config{}, fmt.Errorf("source %s subscription JSON: %w", spec.ID, err)
			}
			s.Subscription = &request
		} else {
			// An explicit protobuf request keeps Vote/Failed unconstrained. The
			// legacy Programs shortcut intentionally excludes some transactions.
			s.Subscription = &pb.SubscribeRequest{Transactions: map[string]*pb.SubscribeRequestFilterTransactions{
				"program_transactions": {AccountInclude: append([]string(nil), spec.Programs...)},
			}}
		}
		cfg.Sources = append(cfg.Sources, s)
	}
	return cfg, nil
}
