# Deferred technical debt and release gates

[← Home](../README.md) · [Core architecture](ARCHITECTURE.md) · [Aggregator V2 roadmap](AGGREGATOR_V2_ROADMAP.md)

The initial reusable Core deliberately excludes RPC/SQD recovery and archive
backfill. Disconnects, missing completeness evidence and unrecoverable
account/config changes remain explicit quality failures.

## Reliability changes after `v0.1.0`

The current development branch now stops on fatal WAL writer/dedupe failures
instead of repeatedly reconnecting, preserves and forwards the successfully
fsynced prefix of a batch before a later conflict, drains received updates
through the source writer on graceful cancellation, and rejects reuse of a
WAL directory under an incompatible subscription identity. Regression tests
cover the conflict prefix, terminal writer failure, cancellation boundary and
identity rejection. These repairs **do not** add a persisted application
delivery cursor or make the Core a canonical state engine.

Remaining Core runtime priorities are versioned segmented binary WAL with
v1 compatibility; indexed checkpoint/recovery; durable late-duplicate policy
with bounded memory; byte-budgeted queues and backlog/latency metrics; and
downstream delivery/atomic-commit acknowledgement. The current checkpoint is
diagnostic, and startup still scans the existing WAL to rebuild dedupe state.

## Deferred recovery work

- A verified full-block oracle for produced slots and filtered transaction
  inclusion; parent hash continuity, skipped slots, incomplete banks.
- Session-local bounded RPC repair for missed transactions. Respect provider
  403/429, pruning, backoff and retry budgets.
- A separate historical account/config witness (including Token/Token-2022
  close-to-System, owner changes, recreation and config/TLV evolution).
  `getBlock` alone is insufficient.
- Distinct transaction/account/config coverage frontiers; failed repair must
  never erase an account/history gap.
- True active-active canonical bank admission, one atomic writer and a
  durable accepted-bank/outbox manifest.
- Replay parity against an independent full-block/account oracle after fault
  injection, with p95/p99 stream age and backlog measurements.

## Before production-grade use

- Measure throughput and latency against the previous no-WAL probe and
  representative 10-15 min capture, including forced reconnect and slow sink.
- Verify provider limits, bank closure semantics and proto schema compatibility
  against the actual endpoint version.
- Pin an immutable reviewed version for the consumer; track the currently
  experimental source-release tag and the corresponding compatibility policy.
- Integrate an opt-in Aggregator V2 adapter, compare replay fixtures, then
  switch consumers only after acceptance. Existing V1/V2 handlers stay intact
  until parity is proven.

## Public source release status

The public `v0.1.0` source release, original history isolation, noreply
authorship, anonymous clone/module import and CI were checked on 2026-11-14.
This is a **source publication milestone**, not a production acceptance gate.
The [historical release audit](PUBLIC_RELEASE_AUDIT_2026-11-14.md) records the
pre-publication baseline and its then-open conditions.

## Remaining release and operation gates

- Add a persisted downstream delivery cursor or application-owned atomic
  acknowledgement protocol. Current `Durable` mode protects raw observations,
  but cannot guarantee exactly-once sink effects or automatic redelivery.
- Prove live/replay equivalence for stateful consumers requiring canonical
  ordering; current source WAL replay preserves only source-local order.

- Enable GitHub private vulnerability reporting or publish a working private
  security contact before inviting external security reports.
- Review the full tracked tree and Git history for provider credentials,
  private endpoint details, WAL contents and sensitive fixtures. `.gitignore`
  does not remove secrets from existing history.
- Recheck anonymous clone, pinned `go get`, issue/PR links, security reporting
  and CI for each significant public version. Local unit/race/vet checks do
  not constitute provider compatibility or load acceptance.

None of the above debt is silently treated as complete by ordinary reconnect,
WAL replay or matching A/B observations.
