# Solana Yellowstone gRPC Data Core — English

[← Home](../README.md) · [🇬🇧 English](README.en.md) · [🇷🇺 Русский](README.ru.md) · [Architecture](ARCHITECTURE.md) · [Core scope/ADRs](PROJECT_SCOPE.md) · [Acceptance/SLO](CONSUMER_ACCEPTANCE_AND_SLO.md) · [Core roadmap](ROADMAP.md) · [Aggregator V2 roadmap](AGGREGATOR_V2_ROADMAP.md) · [Technical debt](TECH_DEBT.md) · [Contributing](../CONTRIBUTING.md) · [Security](../SECURITY.md)

## Overview

Solana Yellowstone gRPC Data Core is an embeddable Go library for processing observed Solana transactions and CPI instructions directly inside an application. It provides the infrastructure needed by an aggregator, trading bot, indexer or analytics service without binding the core to token economics or any one trading strategy.

**What is included:** independently reconnecting Yellowstone sources, per-source provenance and continuity markers, a bounded receive path, CRC-protected WAL with grouped fsync, transaction decoding with loaded address table (ALT) resolution, stable outer/inner CPI positions, basic cross-source transaction deduplication, an in-process handler registry and replay of durable WAL records.

**What is outside this library:** token holder accounting, liquidity/price models, wallet execution, ML features and automatic RPC/SQD recovery. The existing Aggregator V2 application retains these application-specific concerns.

**Current status:** experimental v0. Sources are observations, even at finalized commitment. Core cannot yet certify complete banks or account/config history. Quality remains OBSERVING_UNVERIFIED or CONTINUITY_BROKEN.

**Agreed planning contracts (2026-11-19, Russian):** [product boundaries and ADRs](PROJECT_SCOPE.md), [source/raw consumer API](SOURCE_CONTRACT.md), [provider probes](PROVIDER_MATRIX.md), [downstream test and SLO profile](CONSUMER_ACCEPTANCE_AND_SLO.md), [Core task roadmap](ROADMAP.md), [release gates](RELEASE_GATES.md), and [upstream references](REFERENCES.md). The numeric SLO targets are provisional planning assumptions, not measured service guarantees.

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

The repository is public. Pin the experimental `v0.1.1` release, which includes
WAL hardening and the 2026-11-15 audit fixes. The old `v0.1.0` tag lacks those
repairs. No `GOPRIVATE` override is needed:

    go get github.com/sirdont0/solana-yellowstone-grpc-data-core@v0.1.1

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

### Raw-only ingestion and consumer cursors

Use `core.NewRaw(config)` when the application needs the complete set of
Yellowstone update types selected by its subscription before choosing a
program decoder. `Source.Subscription` accepts a cloned
`proto.SubscribeRequest`, including transaction, transaction-status, account,
slot, block-meta, block, entry and block-footer filters. The engine overwrites commitment,
`from_slot` and ping fields and includes the normalized filter in the WAL
identity. For transaction filters, `Vote` and `Failed` are tri-state pointers:
`nil` leaves the outcome unfiltered; `false` excludes that class; `true` selects
it. The legacy Pump-oriented filter still excludes votes and failed
transactions by default.

For custom subscriptions, reconnect replay uses the minimum observed slot
frontier across all requested update classes. If one requested class has not
produced an observed slot, the Core does not infer its position from another
class. It uses only an explicitly configured initial `FromSlot` or starts live
from the provider head, and the continuity break remains visible. This does not
prove provider retention or complete historical replay; `SubscribeReplayInfo`
is advisory and replay behavior must be checked for each endpoint/profile.

`NewRaw` writes updates without ALT/CPI decoding, so missing CPI metadata cannot
block raw capture. `OpenRawReader(source, cursor)` tails only fsynced records
with bounded `Next(ctx, limit)` batches. `ReadRaw` performs offline catch-up
after the engine has stopped. Each `RawObservation` includes the protobuf
payload re-serialized after gRPC decoding, receive time, provider `created_at`
when present, source/slot/index/signature and optional bank-ID presence. It
also exposes transaction failure and fee metadata when the provider supplied
transaction metadata. This is a source-local observation feed, not a
canonical transaction order or original wire-frame archive.

Persist progress with `Ack(consumer, observation.Cursor)` only after the
consumer's own transaction commits. ACK files are atomically replaced and
fsynced, monotonic, and scoped by consumer and source. A crash after the
consumer commit but before ACK causes redelivery; the contract is at-least-once.
The cursor is bound to the subscription identity and a random WAL journal ID,
so a cursor from a deleted/recreated WAL fails with `ErrCursorExpired`.
Consumer ACKs do not yet drive WAL retention or compaction.
For long-running raw capture, enable `Config.DedupeDisk = true` (or
`YLAB_DEDUPE_DISK=true` for the lab CLI). This uses an exact, WAL-derived
bbolt index for source-local dedupe; `DedupeLimit` becomes its hot-cache size.
Late duplicates/conflicts survive eviction and restart. Startup still rebuilds
the entire index from WAL and needs temporary disk space. The default RAM-only
mode and the separate decoded-event cross-source limit are unchanged.
`ConsumerLag(consumer, source)` reports the difference between the fsynced WAL
head and the acknowledged sequence. It counts all WAL records, including
quality and lifecycle markers; it is not a canonical transaction count. When
records are pending, it also reports the oldest unacknowledged record's receive
time and age. `AgeBuckets` accumulates one oldest-record-age sample per call
for each consumer/source pair, using `<=1s`, `<=5s`, `<=10s`, `<=30s`, `<=1m`,
`<=5m`, `<=1h`, and `>1h` buckets. This is a scrape-sampled histogram of the
oldest pending record, not a scan of every pending WAL record. Samples are
process-local, bounded to 1,024 active consumer/source pairs, and reset on
restart; the least recently sampled pair is evicted first.
`Health()` also reports shared dispatch queue `QueueOldestWait`, `QueueLastWait`,
`QueueMaxWait` and `QueuePending` after byte reservation. These measurements
exclude upstream source-WAL work and are repeated per source in Health.
ACK read/compare/write is protected by an OS-level lock on supported Unix
systems, including across separate Engine instances.
On clean WAL close, a best-effort `.sidx` seek cache accelerates subsequent
offline cursor catch-up. Readers ignore stale caches and validate WAL frames.

Program handlers are accepted only for `finalized` sources. `processed` and
`confirmed` commitments can be captured through `NewRaw`; competing forks
remain source-local and are not merged into the finalized dedupe/bank evidence
model. For the current defaults, source writer queues are limited to 128 MiB,
the shared dispatch queue to 256 MiB, and fsync batches to 16 MiB. These are
payload budgets, not a hard process-memory limit. `MaxMessageMiB` is capped at
95 MiB to fit the base64 JSON envelope in a 128 MiB WAL v1 frame. Graceful
shutdown allows up to 15 seconds for sink delivery; a timeout returns
`ErrDrainTimeout` and cancels the delivery context. `Health()` exposes source
and dispatch byte usage/limits plus last, total and maximum WAL sync duration
and sync count; source queue age and its histogram are available in the same
snapshot.

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

## Error isolation, recovery and evidence

`Config.DecodeErrorPolicy` defaults to `FailClosed`. Choose `SkipAndFlag` to
retain the raw observation, skip a whole unreadable transaction and latch
`CONTINUITY_BROKEN`. `RegisterWithPolicy(handler, SkipAndFlag)` applies that
choice to a handler. Sink/storage failures and conflicting identities are always
fatal. No partial transaction output is published after a decoder error.

`ReplayWithOptions(ctx, ReplayOptions{TolerantTail:true})` reads a torn WAL's
valid prefix without modifying it. All sources are preflighted before delivery;
CRC/identity corruption and active writers remain errors. Metadata presence
flags are preserved: unrecorded CPI is not known-empty CPI.

Public reconnect controls are `ReplayOverlap` (zero selects 64 slots),
`MaxReconnects` (zero means unlimited) and optional `FromSlot`.
CLI JSON exposes `replay_overlap`, `max_reconnects`, `from_slot`,
`decode_error_policy` / `handler_error_policy` (`fail_closed` or `skip_and_flag`)
and `tolerant_replay_tail`.

Poll `Health()`, `Diagnostics()` and `Evidence()` for detached snapshots.
`Source.FailureDomain` is a caller assertion; omitted domains share one
unspecified group. `Logger` accepts a `*slog.Logger` for redacted transport and
skip diagnostics. Evidence is bounded and never certifies exact chain coverage.
