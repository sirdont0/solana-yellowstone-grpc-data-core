# Contributing to Solana Yellowstone gRPC Data Core

[← Home](README.md) · [🇬🇧 English](CONTRIBUTING.md) · [🇷🇺 Русский](docs/CONTRIBUTING.ru.md) · [Architecture](docs/ARCHITECTURE.md) · [Security](SECURITY.md) · [Code of Conduct](CODE_OF_CONDUCT.md)

Thank you for helping improve the Core. We welcome focused issues, bug reports, documentation updates and well-tested changes from contributors with repository access. This is an **experimental** Go library: a good contribution explains what it changes and which guarantees remain unverified.

## Before you start

Read the [architecture and acceptance contract](docs/ARCHITECTURE.md) and [technical debt](docs/TECH_DEBT.md). Yellowstone updates are observations; WAL durability, reconnect or a finalized commitment does not by itself establish canonical completeness.

Open an [issue](https://github.com/sirdont0/solana-yellowstone-grpc-data-core/issues/new/choose) with the problem and expected behavior before starting a large change. For a vulnerability, follow [SECURITY.md](SECURITY.md) and report privately.

## Development setup

You need Go 1.26.11+ and Git. For protobuf changes, you also need `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` and Python 3.

```bash
git clone https://github.com/sirdont0/solana-yellowstone-grpc-data-core.git
cd solana-yellowstone-grpc-data-core
go mod download
go test -count=1 ./...
go test -race ./...
go vet ./...
git diff --check
```

Run `gofmt` on changed Go files. The normal unit tests do not require a live provider. This repository is public; use a pinned module tag as described in the [English integration guide](docs/README.en.md#integration).

## What belongs in Core

**In scope:** Yellowstone sources and reconnect behavior, bounded queues, WAL and replay, transaction/ALT/CPI decoding, source provenance, stable event identity, handler APIs, quality reporting, tests, docs and reproducible tooling.

**Application-specific:** token holder accounting, liquidity models, trading execution, ML features, wallet execution and protocol economics belong in consuming applications or separate handlers.

Add new protocol decoders as replaceable examples where possible. Do not claim complete bank or account history without independent acceptance evidence.

## Contribution workflow

1. Open or link an issue with a reproduction and expected behavior.
2. Create a focused branch (for example, `fix/wal-recovery` or `docs/configuration`).
3. Add tests for success and at least one meaningful failure case when changing behavior.
4. Run formatting, tests, race checks and vet; verify the relevant integration path.
5. Open a PR using the [template](.github/PULL_REQUEST_TEMPLATE.md), describe the before/after behavior and list the checks actually run.

Prefer one concern per PR. Explain compatibility impact on the public Go API, WAL, protobuf descriptors and Yellowstone gRPC wire paths.

## Reliability and correctness expectations

- Preserve explicit source provenance, commitment and continuity quality. Never silently promote an observation to verified canonical state.
- Do not infer missing slots from a sparse program-filtered sequence; `101 → 103 → 109` can be normal.
- Keep source-local `bank_id` and `write_version` separate across backend validators.
- Reconnect, resume, deduplication and WAL recovery fixes need deterministic regression tests.
- A bad or truncated WAL must not silently become a successful clean replay.
- Keep event IDs deterministic across replay and distinct handlers.
- Respect bounded queues, cancellation, failure visibility and backpressure; avoid silent drops.
- Keep decoders strict about unknown layouts and use realistic, sanitized fixtures.
- Never rename the on-wire `/geyser.Geyser/*` methods when regenerating protobuf.

Regenerate protobuf through [scripts/regenerate-proto.sh](scripts/regenerate-proto.sh). Inspect the generated diff and verify coexistence with the older Aggregator Yellowstone protobuf client.

## Security and review

Never commit `.env`, provider tokens, private keys, production WALs or logs containing credentials. Redact endpoint data and sensitive transaction fixtures. Follow the [security policy](SECURITY.md) for confidential reports.

Maintainers review correctness evidence, bounded memory behavior, compatibility, security impact and documentation. Disclose AI-assisted changes when relevant and verify generated code yourself. Review may request narrower scope or additional tests; no response-time SLA is promised.

Original contributions are covered by the repository's [MIT license](LICENSE); modified Yellowstone-derived protobuf files retain upstream [Apache-2.0 attribution](THIRD_PARTY_NOTICES.md).
