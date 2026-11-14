# Solana Yellowstone gRPC Data Core — English

[← Home](../README.md) · [🇬🇧 English](README.en.md) · [🇷🇺 Русский](README.ru.md) · [Architecture](ARCHITECTURE.md) · [Aggregator V2 roadmap](AGGREGATOR_V2_ROADMAP.md) · [Technical debt](TECH_DEBT.md) · [Contributing](../CONTRIBUTING.md) · [Security](../SECURITY.md)

## Overview

Solana Yellowstone gRPC Data Core is an embeddable Go library for processing observed Solana transactions and CPI instructions directly inside an application. It provides the infrastructure needed by an aggregator, trading bot, indexer or analytics service without binding the core to token economics or any one trading strategy.

**What is included:** independently reconnecting Yellowstone sources, per-source provenance and continuity markers, a bounded receive path, CRC-protected WAL with grouped fsync, transaction decoding with loaded address table (ALT) resolution, stable outer/inner CPI positions, basic cross-source transaction deduplication, an in-process handler registry and replay of durable WAL records.

**What is outside this library:** token holder accounting, liquidity/price models, wallet execution, ML features and automatic RPC/SQD recovery. The existing Aggregator V2 application retains these application-specific concerns.

**Current status:** experimental v0. Sources are observations, even at finalized commitment. Core cannot yet certify complete banks or account/config history. Quality remains OBSERVING_UNVERIFIED or CONTINUITY_BROKEN.

## Quick start

Requirements: Go 1.26.11+, Yellowstone gRPC access and provider credentials when required (including the default PublicNode endpoint).

    git clone https://github.com/sirdont0/solana-yellowstone-grpc-data-core.git
    cd solana-yellowstone-grpc-data-core
    cp .env.example .env
    cp config.example.json config.json

Edit **.env** to configure provider A's endpoint, token and token header. If your provider needs no token, leave the token blank. For two sources, set provider B's fields in .env and set source_count to 2 in config.json. Multiple connections to the same provider do not establish independent completeness.

Load the variables into your shell, then run the Pump.fun CreateEvent demonstration:

    set -a
    . ./.env
    set +a
    go test ./...
    go run ./cmd/pumpfun-create

The program prints JSON observations and writes a WAL under run/primary/journal.wal. The default PublicNode address is an example, not a promise of provider access, filter support or replay retention. Stop with Ctrl+C. Offline replay from the same local WAL (only after recording data, with matching source/filter settings):

    CORE_REPLAY_ONLY=1 go run ./cmd/pumpfun-create

The .env file is not read automatically by Go; the shell-loading step is required unless your process supervisor injects those environment variables. Source `.env` only if you trust its contents: sourcing executes shell syntax.

### Configuration

**.env** holds endpoint addresses and secrets: CORE_PROVIDER_A_ENDPOINT, CORE_PROVIDER_A_TOKEN and CORE_PROVIDER_A_TOKEN_HEADER; equivalent CORE_PROVIDER_B_* values are optional. CORE_CONFIG points to the JSON file, and CORE_REPLAY_ONLY=1 selects offline replay.

**config.json** holds non-secret behavior:

| Key | Default | Meaning |
|---|---|---|
| source_count | 1 | One or two independently reconnecting provider streams |
| commitment | finalized | processed, confirmed or finalized |
| delivery | provisional | provisional or durable |
| filter_version | pumpfun-create-v1 | Subscription identity version |
| data_dir | ./run | Local WAL path |
| queue_capacity | 4096 | Bounded application dispatch queue |
| dedupe_limit | 2000000 | Cross-source deduplication budget |
| ping_interval_seconds | 15 | Stream keepalive interval |
| idle_timeout_seconds | 45 | Inactivity watchdog interval |

Each configured provider has one Yellowstone gRPC subscription. This example CLI accepts one or two sources; the embeddable Go API accepts up to 16. The number of providers is the source_count; this setting does not create multiple independent validators out of a single endpoint. Increase queues only after measuring sink latency. Avoid storing passwords or tokens in config.json: the loader rejects unknown JSON fields, including token.

The environment also accepts the legacy CORE_ENDPOINT/CORE_TOKEN and CORE_ENDPOINT_B/CORE_TOKEN_B names for migration. New deployments should use CORE_PROVIDER_A_* and CORE_PROVIDER_B_*.

## Integration

Import the module directly in your Go application. The engine invokes handlers and your sink **in-process**, without another gRPC hop between Core and the consuming application.

The repository is public. Pin the published experimental release for a
reproducible dependency; no `GOPRIVATE` override is needed:

    go get github.com/sirdont0/solana-yellowstone-grpc-data-core@v0.1.0

For local development alongside Aggregator V2, use a sibling directory and a
temporary replace directive instead of depending on a remote branch:

    go mod edit -replace=github.com/sirdont0/solana-yellowstone-grpc-data-core=../solana-yellowstone-grpc-data-core

For reproducible builds, keep an immutable version tag or commit and remove
the local replace directive when no longer needed. The
following code illustrates the API inside a consuming Go function; `endpoint`,
`token`, `consumer` and `ctx` must be supplied by that application. It is not
a complete standalone Go program:

    import (
        "context"
        core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
        "github.com/sirdont0/solana-yellowstone-grpc-data-core/examples/pumpfuncreate"
    )

    engine, err := core.New(core.Config{
        Sources: []core.Source{{
            ID: "primary", Endpoint: endpoint, Token: token,
            Commitment: "finalized", Programs: []string{pumpfuncreate.ProgramID},
        }},
        DataDir: "./run",
        Delivery: core.Provisional,
    }, func(ctx context.Context, event core.Event) error {
        return consumer.Accept(ctx, event)
    })
    if err != nil { return err }
    if err := engine.Register(pumpfuncreate.New()); err != nil { return err }
    return engine.Run(ctx)

Build a custom program or CPI handler by implementing:

    type Handler interface {
        ProgramIDs() []string
        Decode(*core.Transaction, core.Instruction) ([]core.Emission, error)
    }

Register each handler before Run. Instruction.Inner == -1 means an outer instruction; inner indices are zero-based within the parent outer instruction. Instruction.Data is immutable and should be copied only if retained. Return no emissions for unsupported discriminators or versions. Test new schema variants against real fixtures before interpreting fields.

The example handler lives in [examples/pumpfuncreate](../examples/pumpfuncreate/create.go). It parses the verified prefix of Pump.fun CreateEvent and records an unknown trailing tail rather than inventing fields.

### Delivery and quality

**Provisional** emits after receiving a transaction, before its WAL fsync. This minimizes latency but loses durability on crash. **Durable** emits only after its source WAL has been fsynced; it is more conservative and slower.

Every event carries its source, generation, commitment, slot, signature, CPI location, event ID, durability flag and explicit quality. A reconnect may mark continuity broken. Sparse filtered slot sequences (for example 101 → 103 → 109) are normal and are not sufficient evidence of missing data.

WAL replay checks CRC and uses the same registered handlers. Replay preserves per-source receive order; it cannot reconstruct the original cross-provider interleaving or independently prove canonical banks. Replay fails if an expected source WAL is absent or its recorded identity differs from the current source/filter configuration. Full completeness verification and RPC recovery are deferred.

## Repository map

- [engine.go](../engine.go), [transaction.go](../transaction.go), [replay.go](../replay.go): public Go API.
- [internal/ingest](../internal/ingest): Yellowstone client, bounded queues, WAL, source evidence and reconnection.
- [internal/sourcecfg](../internal/sourcecfg): source configuration helpers.
- [proto](../proto): namespaced Yellowstone schemas and generated Go bindings.
- [examples/pumpfuncreate](../examples/pumpfuncreate): independently replaceable example decoder.
- [cmd/pumpfun-create](../cmd/pumpfun-create): CLI demonstration.
- [scripts/regenerate-proto.sh](../scripts/regenerate-proto.sh): reproducible proto regeneration preserving upstream gRPC wire method names.

## Dependencies, licensing and development

The design uses [rpcpool/yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc) protobuf schemas (the yellowstone-grpc-proto subtree is Apache-2.0), [grpc/grpc-go](https://github.com/grpc/grpc-go), [protocolbuffers/protobuf-go](https://github.com/protocolbuffers/protobuf-go) and [mr-tron/base58](https://github.com/mr-tron/base58). These projects remain separate third-party dependencies or protocol sources; this repository does not bundle the Yellowstone validator plugin.

Original Core code and documentation are MIT-licensed. Namespaced protobuf schemas and generated bindings retain upstream Apache-2.0 attribution; see [notices](../THIRD_PARTY_NOTICES.md). The upstream Yellowstone repository has a different default license for parts outside its protobuf subtree.

For contributors:

    gofmt -w ./cmd/pumpfun-create/*.go
    go test ./...
    go test -race ./...
    go vet ./...

If proto definitions change, run [scripts/regenerate-proto.sh](../scripts/regenerate-proto.sh) and verify coexistence with the existing Aggregator protobuf client. See [architecture and acceptance criteria](ARCHITECTURE.md) and [deferred recovery tasks](TECH_DEBT.md). This project is not an official Solana, Triton One or Pump.fun SDK.
