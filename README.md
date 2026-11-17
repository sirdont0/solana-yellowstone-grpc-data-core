<div align="center">

# ◈ Solana Yellowstone gRPC Data Core

**A reusable, in-process Go foundation for Solana event ingestion.**

Multi-source Yellowstone gRPC · crash-aware WAL · transaction/CPI decoding · pluggable handlers

[![Go 1.26.11+](https://img.shields.io/badge/Go-1.26.11%2B-00ADD8?logo=go&logoColor=white)](go.mod)
[![Project status](https://img.shields.io/badge/status-experimental%20v0-orange)](docs/ARCHITECTURE.md#acceptance-status)
[![License: MIT](https://img.shields.io/badge/code%20license-MIT-2ea44f)](LICENSE)
[![Protocol: Yellowstone](https://img.shields.io/badge/protocol-Yellowstone%20gRPC-7158e2)](https://github.com/rpcpool/yellowstone-grpc)

**[Get started](#quick-start)** · **[Architecture](#architecture-at-a-glance)** · **[Aggregator V2 roadmap](docs/AGGREGATOR_V2_ROADMAP.md)** · **[Documentation](#documentation)** · **[Contributing](CONTRIBUTING.md)** · **[Security](SECURITY.md)**

[🇬🇧 English](docs/README.en.md) · [🇷🇺 Русский](docs/README.ru.md)

</div>

---

Solana Yellowstone gRPC Data Core receives Yellowstone updates from Solana providers, persists them in per-source write-ahead logs (WALs), and exposes both decoder-independent raw observations and optional transaction/CPI handlers through an **in-process Go API**. The included [Pump.fun CreateEvent decoder](examples/pumpfuncreate/create.go) demonstrates the protocol-specific layer.

> [!IMPORTANT]
> **Experimental v0 — observation quality is not canonical completeness.** Reconnect, WAL durability, matching providers and `finalized` commitment do **not** prove full bank or account-history coverage. Do not use this release as the sole source of exact trading balances or token state. See the [acceptance contract](docs/ARCHITECTURE.md#acceptance-status) and [open release gates](docs/TECH_DEBT.md).

## Why this core?

| Capability | What it provides |
| :--- | :--- |
| **Independent Yellowstone sources** | Configurable providers, source identity, reconnect and continuity evidence. |
| **Bounded ingestion** | A controlled receive path with queues and backpressure. |
| **Crash-aware raw WAL** | Per-source append log, CRC validation and grouped `fsync`. |
| **Raw observation API** | Live WAL tailing of transactions, failed statuses, accounts, slots and other subscribed updates without CPI decoding. |
| **Consumer cursors** | Stable source-local WAL sequence + journal ID, with durable monotonic acknowledgements. |
| **Transaction + CPI positions** | Loaded address table (ALT) resolution and stable outer/inner instruction indices. |
| **Pluggable decoders** | Register `Handler` implementations and deliver events to a `Sink` in the same process. |
| **Durable replay** | Reprocess persisted raw updates through the same decoder handlers. |

## Architecture at a glance

```mermaid
flowchart LR
    subgraph S["Yellowstone sources"]
        A["Provider A"]
        B["Provider B · optional"]
    end
    subgraph C["Go Data Core"]
        I["Per-source receive<br/>and reconnect"]
        W[("CRC WAL<br/>batch fsync")]
        R["Raw WAL reader<br/>source-local cursor"]
        D["Normalize transaction<br/>ALT + outer / inner CPI"]
        H["Registered handlers"]
    end
    A --> I
    B --> I
    I --> W
    W --> R --> RO["Raw consumer<br/>aggregator / indexer"]
    I -. "Provisional mode" .-> D
    W -- "Durable mode" --> D
    W -- "Offline replay" --> D
    D --> H
    H --> O["In-process event sink<br/>Aggregator · indexer · bot"]
```

Choose **one delivery mode** per engine run: `Provisional` dispatches before source WAL durability; `Durable` dispatches after successful source WAL `fsync`. The diagram shows the available paths, not simultaneous delivery of both modes. Replay reads previously saved WAL records. [Explore the detailed architecture →](docs/ARCHITECTURE.md)

## Quick start

**Requirements:** Go 1.26.11+ and access to a Yellowstone gRPC endpoint. This source repository is public.

```bash
git clone https://github.com/sirdont0/solana-yellowstone-grpc-data-core.git
cd solana-yellowstone-grpc-data-core
cp .env.example .env
cp config.example.json config.json
```

Set your endpoint and provider token in `.env`. A token is required for the default PublicNode endpoint; other providers may permit anonymous access. The example endpoint does not guarantee the necessary filters, commitment or replay permissions. Keep credentials out of `config.json` and Git. Then:

```bash
set -a
. ./.env
set +a
go test ./...
go run ./cmd/pumpfun-create
```

To replay the locally recorded WAL using the same handler:

```bash
CORE_REPLAY_ONLY=1 go run ./cmd/pumpfun-create
```

The example writes raw data under `run/`; the directory is Git-ignored. The `.env` file must be loaded into your shell (or injected by your process manager). Replay requires WAL already captured with matching source/filter identity. [Read the complete setup guide →](docs/README.en.md#quick-start)

### Universal raw capture (without a Pump.fun decoder)

For a decoder-independent feed, `corectl` captures any supported Yellowstone
`SubscribeRequest` filter directly into a source-local WAL:

```bash
export YELLOWSTONE_ENDPOINT='your-yellowstone-host:443'
export YELLOWSTONE_TOKEN='your-provider-token' # only if required by the provider
export PROGRAM_ID='a-valid-Solana-program-public-key'

go run ./cmd/corectl capture \
  --endpoint "$YELLOWSTONE_ENDPOINT" \
  --program "$PROGRAM_ID" \
  --source-id primary \
  --data-dir ./raw-run
```

Use `--subscription ./subscription.json` for transaction-status, account, slot,
block-meta, block, entry, block-footer or mixed filters; use `--config ./capture.json --dry-run` to
validate a multi-source configuration without connecting. After stopping the
writer, inspect or verify the WAL with `corectl inspect` and `corectl verify`.
The [raw capture quick start](docs/RAW_QUICKSTART.md) and [CLI reference](docs/CLI.md)
cover subscription JSON, credentials, consumer cursors, replay and migration.

## Delivery and guarantees

| | Provisional | Durable | Offline replay |
| :--- | :--- | :--- | :--- |
| **Dispatch point** | On observation, before WAL `fsync` | After source WAL `fsync` | From stored WAL |
| **Latency** | Lower | Includes WAL durability cost | Offline processing |
| **Durability at dispatch** | Not guaranteed | Source WAL has been synced | Input is persisted WAL |
| **Canonical bank proof** | No | No | No |

Filtered slot sequences can legitimately skip numbers. In particular, `101 → 103 → 109` is **not sufficient** evidence of missing data. See [invariants and quality semantics](docs/ARCHITECTURE.md#invariants).

**Delivery limit:** raw consumers can tail a source WAL and persist a monotonic acknowledgement cursor bound to its subscription identity and journal ID. Commit application effects first, then call `Ack`; a crash between those operations causes at-least-once redelivery. The cursor is not atomically committed with an external database transaction, and it does not establish cross-source order or canonical bank completeness. Program handlers require `finalized`; `processed` and `confirmed` are available as source-local raw observations. The raw-only mode does not require CPI metadata. See the [2026-11-16 audit follow-up](docs/AUDIT_FOLLOWUP_2026-11-16.md).

## Documentation

| Guide | Purpose |
| :--- | :--- |
| [🇬🇧 English](docs/README.en.md) · [🇷🇺 Русский](docs/README.ru.md) | Installation, configuration, API integration and examples |
| [Architecture and invariants](docs/ARCHITECTURE.md) | End-to-end flow, evidence, delivery, boundaries and acceptance status |
| [Aggregator V2 Go/Rust roadmap](docs/AGGREGATOR_V2_ROADMAP.md) · [Русский](docs/AGGREGATOR_V2_ROADMAP.ru.md) | Go-first decision, language-neutral boundary, lifecycle, correctness and performance gates |
| [Technical debt / release gates](docs/TECH_DEBT.md) | Full-bank verification, account completeness, RPC/SQD recovery and load acceptance |
| [Audit fixes, 2026-11-15](docs/AUDIT_CLOSURE_2026-11-15.md) | Finding dispositions, regression evidence and remaining release gates |
| [Audit follow-up, 2026-11-16](docs/AUDIT_FOLLOWUP_2026-11-16.md) | Raw subscriptions, cursors/ACK, bounded shutdown, byte budgets and remaining work |
| [Public release audit](docs/PUBLIC_RELEASE_AUDIT_2026-11-14.md) | Verified hardening and remaining release conditions |
| [Contributing](CONTRIBUTING.md) | Local development, tests, PR conventions and decoder rules |
| [Security policy](SECURITY.md) | Responsible disclosure, secrets and data-integrity risks |
| [Raw capture quick start](docs/RAW_QUICKSTART.md) · [CLI reference](docs/CLI.md) | Universal Yellowstone capture, custom filters, multi-source configs and offline WAL tools |
| [Support](SUPPORT.md) · [Code of Conduct](CODE_OF_CONDUCT.md) | Troubleshooting and community expectations |
| [Protocol provenance](PROTO_NOTICE.md) · [Third-party notices](THIRD_PARTY_NOTICES.md) | Origin and licensing of adapted Yellowstone protobuf |

## Project layout

```text
solana-yellowstone-grpc-data-core/
├── engine.go, raw.go, raw_archive.go       # Public Go API and raw consumers
├── transaction.go, replay.go               # Decode and replay API
├── internal/ingest/                       # Transport, WAL, source evidence
├── internal/sourcecfg/                    # Provider configuration
├── proto/                                 # Namespaced Yellowstone protobuf
├── examples/pumpfuncreate/                # Pluggable sample decoder
├── examples/rawconsumer/                  # Durable raw consumer example
├── cmd/corectl/                            # Universal capture and WAL CLI
├── cmd/pumpfun-create/                     # Runnable decoder demonstration
├── docs/                                   # Architecture and guides
└── .github/                                # CI, issue and PR templates
```

## Contribute and report issues

Contributions that improve testable ingest correctness, replay parity, observability, documentation and protocol decoders are welcome from repository collaborators. Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR. Use the [issue templates](https://github.com/sirdont0/solana-yellowstone-grpc-data-core/issues/new/choose) for reproducible bugs and proposals. For vulnerabilities, follow [SECURITY.md](SECURITY.md); do not include secrets or sensitive WAL data in issues.

## Licensing and attribution

Original Core code and documentation: [MIT](LICENSE). Adapted Yellowstone protobuf schemas and generated Go bindings: [Apache-2.0](licenses/Apache-2.0.txt), with [attribution](THIRD_PARTY_NOTICES.md) and [provenance](PROTO_NOTICE.md). The project is independently maintained and is not an official Solana, Triton One or Pump.fun SDK.
