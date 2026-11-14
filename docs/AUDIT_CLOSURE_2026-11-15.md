# Public Core audit follow-up — 2026-11-15

Baseline: public `881448f84d466fe5fa33a9ef0a595fb71713c710`.
The local checkout previously used a separate private history at `ced1c91`.
Work now starts directly from public/main on `codex/core-audit-2026-11-15`.
The private history was neither merged into the public ancestry nor rewritten.
A `public` remote names the public repository; `origin` still names the private
archive. Publishing must use `public` explicitly.

## Finding-by-finding disposition

| Audit item | Disposition and evidence |
| --- | --- |
| C0: stale local baseline | Resolved for this checkout: all four `881448f` WAL fixes and regressions are ancestors of this branch. Do not publish the old private branch. |
| C1: `inner_instructions_none` means “no CPI” | **Incorrect premise, rejected.** Upstream distinguishes *not recorded* (`None`) from a recorded empty list (`Some([])`). The old strict rejection was intentional. Known-empty transactions already decode; explicit live/replay regressions now prove it. |
| C1: isolate unreadable updates | Implemented opt-in `Config.DecodeErrorPolicy=SkipAndFlag`. Default remains `FailClosed` for compatibility. A skipped whole transaction produces a durable live WAL marker, sticky quality, counters and bounded diagnostics. A healthy provider's later copy is still eligible. Missing metadata never becomes known-empty. |
| C1: canonicalize `None` and empty before digest | **Unsafe recommendation, rejected.** Deterministic protobuf already normalizes absent/empty repeated fields. Presence flags carry real information and remain in digests. Raw envelope identity and digest are now independently verified. |
| C2: `v0.1.0` lacks hardening | Confirmed. The old immutable tag is preserved. Release notes and a tag-triggered, test-gated prerelease workflow are prepared for `v0.1.1`. Publication and fresh remote CI remain an external release step; a local commit is not a published release. Installation guidance no longer recommends the unpatched tag. |
| P1.2: torn-tail offline replay | Implemented `ReplayWithOptions(..., ReplayOptions{TolerantTail:true})`. Keeps bytes unchanged, preflights all sources, labels the affected source broken before any prefix delivery. Bad CRC, JSON, header, identity or full-frame length still fail. Reader locks reject a running writer. |
| P1.3: inaccessible evidence | Integrated durable `MultiSourceCoordinator` observations into Engine recovery/live/replay. Public `Evidence()`, `Diagnostics()` and extended `Health()` expose snapshots. Failure-domain declarations default conservatively to one unspecified domain. All exact admission fields remain UNVERIFIED. |
| P1.4: reconnect controls | Public `ReplayOverlap` (zero selects 64), `MaxReconnects` and `FromSlot`; equivalent CLI JSON settings. Corrected lost initial cursor after a failed dial and unsafe resume from unfiltered slot/meta progress. Resume now uses observed transaction progress. |
| P1.5: handler isolation | `RegisterWithPolicy(h, SkipAndFlag)` skips the whole transaction and records an explicit failure. Other handlers' prepared effects cannot leak. Default registration remains fail-closed. Sink errors and integrity conflicts always stop. |
| Deprecated dial API | Migrated to `grpc.NewClient` with bounded readiness waiting and context cancellation. |
| Server ping should echo an ID | **Incorrect premise, rejected.** `SubscribeUpdatePing` has no ID. Client requests allocate IDs; server `SubscribeUpdatePong` echoes those. Existing ping/pong integration test covers the exchange. |
| Reconnect jitter | Implemented bounded randomized delay (half to full exponential ceiling, maximum four seconds). |
| Duplicate decoders | One transaction/ALT/CPI normalizer now backs the public decoder and occurrence extraction. Account indices, key lengths, signature length and envelope integrity are consistently checked. Split CPI groups retain distinct positions. |
| Observability | Structured optional `slog` transport/skip logging; credential redaction also covers returned terminal errors. Health exposes generation, reconnects, duplicates, pings/pongs, WAL count, latest durable-record time, decode failures and shared dispatch queue depth/capacity. WAL byte-budget, oldest queue age and latency histograms remain W1 work. |
| CI | Build, formatting, tidy-diff, race tests with coverage artifact, vet, pinned Staticcheck, govulncheck and gitleaks. Release job depends on all checks and requires version-specific release notes. |
| Private vulnerability reporting | Enabled through GitHub API on 2026-11-15; independently verified without authentication: `{"enabled":true}`. Security policy links directly to private advisory creation. |
| JSON/base64 WAL, full startup scan | Confirmed design debt, not declared fixed. Segment/index migration requires its own compatibility and crash-recovery work. |

## Additional defects found and fixed

1. Cross-source identity included signature in the lookup key. Different
   signatures at one `(commitment, slot, tx index)` could both emit. The position
   is now the conflict key and the payload digest includes signature. Failed
   transactions also enter conflict detection before their effects are suppressed.
2. Handler registration could race Run/Replay starting. Registration and session
   initialization now share one mutex; configuration slices and FromSlot are owned
   copies. A deterministic concurrency regression covers the start boundary.
3. WAL frame metadata could disagree with raw bank ID or digest. Replay preflight
   and dispatch validate both, including updates that lack CPI metadata.
4. Opening a foreign journal could repair its torn tail before rejecting identity.
   Identity is now checked while visiting records, before truncation can occur.
5. WAL writes could continue after an earlier batch failure. Failure is now sticky;
   subsequent appends fail. Close is safe under concurrent calls. New WAL creation
   syncs its containing directory before accepting records.
6. Error strings returned after exhausting reconnects could disclose a provider
   token even though logs were redacted. Both surfaces now redact it. Endpoint
   embedded credentials/paths and invalid metadata headers are rejected early.
7. The mock server had concurrent gRPC sends. Its sends are now serialized.
8. Block-footer and transaction-status envelopes dropped source-local bank IDs.
   Both now preserve them. No existing stored digest convention is changed.
9. The sibling aggregator import smoke test used an old Go minimum and a fixture
   with missing CPI metadata. Its isolated module now matches Core dependencies,
   tests known-empty metadata and rejects unrecorded CPI, while checking both
   protobuf registries and unchanged Yellowstone RPC paths.

## Verification and practical limits

Go 1.26.11, macOS arm64. Commands run against the final source are recorded in
[verification results](AUDIT_VERIFICATION_2026-11-15.md). Tests use real local
loopback gRPC servers, CRC-protected temporary WALs, corruption/torn-tail faults,
reconnects, live durable/provisional modes, replay and concurrent health reads.
They do not contact production Yellowstone or require provider credentials.

The isolated consumer integration and the existing V2 P0 contract check are also
run. P0: 1,078 rows, contract SPECIFIED, runtime UNVERIFIED. P1 remains BLOCKED and
P2 remains PARTIAL. This work does not implement W2 protocol coverage, W3 canonical
admission/outbox, W4 1×/3×/10× load and 24-hour soak, W5 Rust or W6 wallet graph.
Those are planned architecture milestones, not small defects in this library.

`SkipAndFlag` is deliberate loss of *decoded output*, never a recovery claim.
Raw updates remain journaled, effects from the failed transaction are discarded,
quality stays broken, and operators can fix a decoder and replay later. Replay
can reproduce a failure diagnostic as well as encounter its persisted marker;
counts represent diagnostic observations, not distinct chain transactions.
Historical marker quality remains sticky even after a decoder is improved.

## Primary references

- [Yellowstone issue 867: None means not recorded](https://github.com/rpcpool/yellowstone-grpc/issues/867)
- [Yellowstone change 875: preserve the None flags](https://github.com/rpcpool/yellowstone-grpc/pull/875)
- [Upstream protobuf: ping, pong and metadata fields](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-proto/proto/geyser.proto)
- [Transaction metadata protobuf](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-proto/proto/solana-storage.proto)
- [Public hardening baseline](https://github.com/sirdont0/solana-yellowstone-grpc-data-core/commit/881448f84d466fe5fa33a9ef0a595fb71713c710)
- [Private reporting status API](https://api.github.com/repos/sirdont0/solana-yellowstone-grpc-data-core/private-vulnerability-reporting)
