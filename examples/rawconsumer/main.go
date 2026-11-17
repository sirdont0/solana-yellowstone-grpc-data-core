// rawconsumer demonstrates a durable, at-least-once Yellowstone observation
// consumer using the public Core API. It does not decode any protocol.
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
	"strings"
	"syscall"
	"time"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "rawconsumer:", err)
		os.Exit(1)
	}
}

func run() (retErr error) {
	endpoint := flag.String("endpoint", os.Getenv("YELLOWSTONE_ENDPOINT"), "Yellowstone provider host:port")
	program := flag.String("program", "", "transaction account_include program ID")
	subscriptionPath := flag.String("subscription", "", "custom SubscribeRequest protobuf JSON")
	sourceID := flag.String("source-id", "primary", "source ID")
	consumer := flag.String("consumer", "example", "durable consumer cursor name")
	dataDir := flag.String("data-dir", "./run", "Core WAL root")
	output := flag.String("output", "./observations.ndjson", "durable NDJSON output file")
	outputDB := flag.String("output-db", "", "idempotent application database (replaces NDJSON output when set)")
	commitment := flag.String("commitment", "finalized", "Yellowstone commitment")
	tokenHeader := flag.String("token-header", "x-token", "provider credential header")
	filterVersion := flag.String("filter-version", "rawconsumer-v1", "subscription revision")
	walVersion := flag.Int("wal-version", 1, "WAL version, 1 or experimental 2")
	flag.Parse()
	if *endpoint == "" {
		return errors.New("set --endpoint or YELLOWSTONE_ENDPOINT")
	}
	if (*program == "") == (*subscriptionPath == "") {
		return errors.New("choose exactly one of --program and --subscription")
	}
	var request *pb.SubscribeRequest
	if *subscriptionPath != "" {
		b, err := os.ReadFile(*subscriptionPath)
		if err != nil {
			return err
		}
		request = &pb.SubscribeRequest{}
		if err := protojson.Unmarshal(b, request); err != nil {
			return fmt.Errorf("subscription: %w", err)
		}
	} else {
		// Vote and Failed remain unconstrained: no matching transaction class
		// is excluded by this example's filter.
		request = &pb.SubscribeRequest{Transactions: map[string]*pb.SubscribeRequestFilterTransactions{
			"program_transactions": {AccountInclude: []string{*program}},
		}}
	}
	engine, err := core.NewRaw(core.Config{
		Sources: []core.Source{{
			ID: *sourceID, Endpoint: *endpoint, Token: os.Getenv("YELLOWSTONE_TOKEN"),
			TokenHeader: *tokenHeader, Commitment: *commitment, FilterVersion: *filterVersion,
			Subscription: request,
		}},
		DataDir: *dataDir, WALVersion: *walVersion, DedupeDisk: true,
	})
	if err != nil {
		return err
	}
	var commit func(core.RawObservation) error
	if *outputDB != "" {
		db, err := openApplicationDB(*outputDB)
		if err != nil {
			return err
		}
		defer db.Close()
		commit = func(observation core.RawObservation) error { return commitObservation(db, observation) }
	} else {
		if err := os.MkdirAll(filepath.Dir(*output), 0700); err != nil {
			return err
		}
		f, err := os.OpenFile(*output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		commit = func(observation core.RawObservation) error {
			if err := enc.Encode(observation); err != nil {
				return err
			}
			return f.Sync()
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	var runErr error
	go func() {
		runErr = engine.Run(runCtx)
		close(runDone)
	}()
	defer func() {
		cancel()
		<-runDone
		if retErr == nil && runErr != nil && !errors.Is(runErr, context.Canceled) {
			retErr = runErr
		}
	}()

	// Run opens each Store before its gRPC receive loop. Register the durable
	// consumer before opening its reader, so ACKs carry a lifecycle epoch.
	var reader *core.RawReader
	for reader == nil {
		_, err = engine.RegisterConsumer(*consumer, *sourceID, core.ConsumerFromBeginning)
		if err == nil {
			reader, err = engine.OpenRawReaderFromConsumer(*consumer, *sourceID)
		}
		if err == nil {
			break
		}
		// Only the initial Store-open race is transient. An expired cursor,
		// invalid consumer name or storage error requires operator action.
		if !strings.Contains(err.Error(), "source WAL has not been opened by Run") {
			return err
		}
		select {
		case <-runDone:
			if runErr != nil {
				return runErr
			}
			return err
		case <-ctx.Done():
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	defer reader.Close()
	for {
		batch, nextErr := reader.Next(ctx, 128)
		for _, observation := range batch {
			// Commit the application effect before acknowledging Core's cursor.
			// A crash between Sync and Ack causes safe at-least-once redelivery.
			if err := commit(observation); err != nil {
				return err
			}
			if err := engine.Ack(*consumer, observation.Cursor); err != nil {
				return err
			}
		}
		if errors.Is(nextErr, io.EOF) || errors.Is(nextErr, context.Canceled) {
			break
		}
		if nextErr != nil {
			return nextErr
		}
	}
	return nil
}
