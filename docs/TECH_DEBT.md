# Deferred technical debt and release gates

The initial reusable Core deliberately excludes RPC/SQD recovery and archive
backfill. Disconnects, missing completeness evidence and unrecoverable
account/config changes remain explicit quality failures.

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

## Before public production use

- Measure throughput and latency against the previous no-WAL probe and
  representative 10-15 min capture, including forced reconnect and slow sink.
- Verify provider limits, bank closure semantics and proto schema compatibility
  against the actual endpoint version.
- Pin a verified tag, add upstream licensing notices and set the public repo
  visibility/license intentionally.
- Integrate an opt-in Aggregator V2 adapter, compare replay fixtures, then
  switch consumers only after acceptance. Existing V1/V2 handlers stay intact
  until parity is proven.

## Public repository release checks

- Add a persisted downstream delivery cursor or application-owned atomic
  acknowledgement protocol. Current `Durable` mode protects raw observations,
  but cannot guarantee exactly-once sink effects or automatic redelivery.
- Prove live/replay equivalence for stateful consumers requiring canonical
  ordering; current source WAL replay preserves only source-local order.

- Confirm that GitHub visibility matches the intended release; update any
  private-repository access instructions if the project becomes public.
- Enable GitHub private vulnerability reporting or publish a working private
  security contact before inviting external security reports.
- Review the full tracked tree and Git history for provider credentials,
  private endpoint details, WAL contents and sensitive fixtures. `.gitignore`
  does not remove secrets from existing history.
- Confirm public clone, `go get` and issue/PR links from an unauthenticated
  account, plus provider-neutral quick-start behavior.
- Verify an immutable release tag and CI status; local unit/race/vet checks do
  not constitute provider compatibility or load acceptance.

None of the above debt is silently treated as complete by ordinary reconnect,
WAL replay or matching A/B observations.
