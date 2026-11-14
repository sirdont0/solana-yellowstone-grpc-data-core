# Public release readiness audit — 2026-11-14

[← Home](../README.md) · [Architecture](ARCHITECTURE.md) · [Technical debt](TECH_DEBT.md) · [Security](../SECURITY.md)

## Decision

**Conditional for an experimental public source release. Not approved as an
exact or production-grade Solana state engine.** The current API is a useful
building block for source-local Yellowstone observations, WAL capture,
transaction/CPI decoding and in-process handlers. It does not establish
complete bank, account or economic state.

The baseline reviewed was private `main` at `399f90f`. Release hardening was
performed locally against that baseline. GitHub visibility was not changed.

## Verified fixes and checks

| Area | Result |
| --- | --- |
| Provider startup | Default PublicNode example now rejects a missing live token before retrying; offline WAL replay remains tokenless. |
| Transport secrets | Built-in dialer rejects plaintext gRPC with a token; disconnect diagnostics redact the configured token. |
| Dependencies | Go minimum raised to 1.26.11; gRPC and `x/net` updated after vulnerability review. |
| Vulnerability scan | `govulncheck` reported `No vulnerabilities found` after dependency updates. |
| Secret scan | Gitleaks scanned all five reachable baseline commits and found no leaks. A separate review inspected six commits including one unreachable commit and 94 Git blobs without identifying real credentials. |
| Transaction dispatch | All handler decoding completes before the first sink emission; handler errors cannot publish a partial transaction. Failed sink delivery is not marked complete in memory. |
| CPI decoding | Missing/disabled CPI metadata is rejected, inner positions are unique across split groups, and occurrences follow outer-then-CPI execution order. |
| Protocol example | Pump.fun CreateEvent Borsh strings reject invalid UTF-8. |
| WAL replay | Rejects transaction updates claiming a different source from the selected WAL. |
| Multi-source evidence | Evicting an unclosed bank latches partial evidence rather than silently discarding it. |
| Local verification | `go test -count=1 ./...`, `go test -race ./...`, `go vet ./...`, `go build ./...`, `go mod verify`, Markdown-link and YAML parsing checks passed. |
| CI | Go tests, race detector, vet, vulnerability scan and Git-history secret scan are configured. |

The test suite exercises mocked Yellowstone streams, WAL corruption,
reconnect/replay, deduplication and CPI fixtures. **No live credentialed
PublicNode end-to-end acceptance or throughput/stress acceptance was performed
in this audit.** GitHub Actions must be observed on the hardening commit.

## Open acceptance gates

1. **Downstream delivery and atomic application.** `Durable` guarantees raw
   source WAL fsync before dispatch, not a downstream commit. There is no
   persisted delivery cursor or atomic multi-event sink interface. A crash
   can leave WAL records undelivered; a retry can duplicate effects. Consumers
   must replay and apply idempotent IDs, or implement an atomic commit layer.
2. **Canonical completeness.** No independent full-bank transaction inclusion,
   account/config history oracle or accepted reconciliation pipeline exists.
   `finalized`, matching providers and sparse slot progression are insufficient.
3. **Deterministic cross-source replay.** WAL is source-local. Offline replay
   does not recreate the exact interleaving seen during live ingestion.
4. **Provider and load evidence.** Confirm real token access, filters, replay
   retention, reconnect behavior and bounded-load latency with production-like
   data before describing this as production-ready.
5. **Public-facing GitHub operations.** Enable a working private security-report
   channel; verify anonymous clone, `go get`, issue/PR links and CI after the
   repository becomes public. Publish a version tag only after these checks.

## Security disposition

No real credential leak was confirmed in the inspected source or Git objects.
No credential rotation is indicated by this scan alone. If a real provider
token was previously used with plaintext gRPC, rotate it independently of the
repository scan. Do not commit `.env`, private keys, provider logs or raw WAL.
The project remains subject to normal security review as new code is added.
