# Deferred technical debt and release gates

> **Work tracking (2026-11-19):** This file retains detailed historical findings. The normalized task registry and dependencies are in [ROADMAP.md](ROADMAP.md); actual release conditions are in [RELEASE_GATES.md](RELEASE_GATES.md). Raw consumer expectations and measurable SLOs are in [CONSUMER_ACCEPTANCE_AND_SLO.md](CONSUMER_ACCEPTANCE_AND_SLO.md). Historical audit statuses below are not retrospectively upgraded.

Current verification and the ordered public-Core closure checklist:
[2026-11-17 audit](AUDIT_VERIFICATION_2026-11-17.md). It includes additional
recovery/cursor repairs, six process-kill scenarios, an idempotent application
database example, and measured limitations of the local WAL load fixture.
It does not close incremental recovery, ACK-aware retention or full acceptance.

Follow-up on `dev`: [verified recovery cache](RECOVERY_CHECKPOINT.md) reuses
state and unchanged disk dedupe after a verified WAL v2 checkpoint and applies
the remaining tail. General crash-time incremental recovery is still open:
changed indexes rebuild, full WAL hashing remains, and historical observers
still replay history. This cache cannot authorize retention.

[← Home](../README.md) · [Core architecture](ARCHITECTURE.md) · [Aggregator V2 roadmap](AGGREGATOR_V2_ROADMAP.md)

The initial reusable Core deliberately excludes RPC/SQD recovery and archive
backfill. Disconnects, missing completeness evidence and unrecoverable
account/config changes remain explicit quality failures.

## Reliability changes after `v0.1.0`

The current development branch now stops on fatal WAL writer/dedupe failures
instead of repeatedly reconnecting, preserves and forwards the successfully
fsynced prefix of a batch before a later conflict, drains received updates
through the source writer on graceful cancellation, and rejects reuse of a
WAL directory under an incompatible subscription identity. The 2026-11-16
working tree additionally adds raw-only subscriptions, source-local WAL tail
cursors, durable consumer ACK files, byte budgets and bounded sink drain.
These repairs **do not** make the Core a canonical state engine or provide an
atomic transaction spanning an external consumer database and its ACK file.

Remaining raw Core runtime priorities are persistent indexed checkpoint/recovery
and WAL retention/compaction driven by consumer ACKs. A cross-source decoded-event
dedupe policy belongs to the optional decoded-handler layer. The public
`rawconsumer --output-db` example demonstrates application-owned transactional
idempotency followed by a separate Core ACK; Core does not provide a transaction
spanning arbitrary application storage and its ACK files.
The legacy checkpoint is diagnostic. WAL v2 now has a separate verified
state/dedupe recovery cache and persistent sealed seek indexes; neither permits
history deletion or removes full integrity hashing. Cleanly closed v1 WALs
now persist a best-effort `.sidx` seek cache for offline readers; the cache
is invalidated by WAL size/mtime changes. A crash without a current cache
still requires sequential offline catch-up.

An opt-in `DedupeDisk` mode (`YLAB_DEDUPE_DISK=true` in the lab CLI) now uses a
WAL-derived bbolt index per source. `DedupeLimit` bounds its in-memory hot
cache; late source-local duplicates and conflicts remain discoverable after
eviction and restart. The default RAM-only mode still stops at its lifetime
limit. v1 and v2 without an admissible recovery cache **rebuild** the disk index
from the verified full WAL, with O(history) work and temporary space for a second
index. v2 can reuse the unchanged database bound to a verified snapshot;
changed databases still rebuild. bbolt's mapped pages and WAL parsing affect RSS; this is
not a proven whole-process memory bound. Legacy decoded-event dispatch in
`Engine.seen` retains its separate in-memory limit; `NewRaw` avoids that path.

The [WAL v2 contract](WAL_V2_CONTRACT.md) records migration, rotation,
crash-recovery and cursor acceptance rules. This working tree now contains an
opt-in segmented v2 writer (`WALVersion=2`), reader and explicit v1-to-v2
copy/verify migration CLI, with manifest-based committed offsets. Rotation,
restart, cross-segment cursor, uncommitted-tail recovery and sealed-corruption
behavior have focused tests. The v1 format remains the default; v2 is still
experimental. Incremental startup/index recovery, retention, compaction,
the full fault-injected crash matrix and
sustained-load acceptance remain open.
The [2026-11-16 recheck](AUDIT_FOLLOWUP_2026-11-16.md) originally reproduced
five defects in commit recovery, legacy identity, format switching, segment
capacity validation and sparse-index fallback. Subsequent working-tree fixes
now pass all five original regression probes, including the injected
post-`CURRENT`-publication failure. This closes those specific reproductions;
the full v2 crash/fault matrix and load acceptance remain open.

The [2026-11-15 follow-up](AUDIT_CLOSURE_2026-11-15.md) additionally implements
explicit decode isolation, tolerant read-only replay, shared normalization,
public evidence, basic health counters, structured logging and reconnect controls.
The 2026-11-16 working tree adds source/dispatch byte budgets and exposes their
current usage and limits, plus last/total/max fsync duration and count.
The source receive path now reserves `proto.Size(update)+512` queue bytes
before `FromUpdate` allocates an encoded payload; after serialization it
reconciles the reservation to `RecordQueueBytes`. The already-decoded gRPC
message, protobuf scratch allocations and Go runtime overhead still mean
these byte budgets are not exact process RSS limits.
`Health()` now exposes current oldest shared-dispatch wait, last/max completed
wait and pending dispatch count after byte reservation. This is not the age of
source-WAL queue batches. Source writer age is now separately exposed through
`SourceWriterOldestAge`, `SourceWriterLastAge`, `SourceWriterMaxAge`,
`SourceWriterPending`, and eight histogram buckets (<=1/5/10/25/50/100/500ms,
then >500ms). It includes local byte-budget waits and WAL batch flush time,
but not network/provider lag and does not itself prove fsync success. Fsync
attempt durations also have eight exclusive buckets using the same thresholds;
failed fsync attempts count too. `ConsumerLag` now reports the oldest pending
record age and accumulates a per-consumer/source histogram of that age whenever
the API is sampled. The process-local histogram is bounded to 1,024 active
consumer/source pairs and describes scrape observations of the oldest pending
record, not the age distribution of every record in the backlog.
`ConsumerLag(consumer, source)`
exposes current WAL-record backlog against a durable ACK, including markers.
Raw ACK writes now serialize across Engine instances/processes on supported
Unix platforms; an unsupported platform fails closed. WAL batch preflight,
failed-write rollback, damaged-tail prefix delivery, and a cursor-specific
replay rejection classifier have regression coverage. These uncommitted changes are not part of the immutable `v0.1.1`
tag; the old `v0.1.0` tag remains immutable as well.

## Optional downstream recovery work (outside raw Core acceptance)

The following features require additional completeness evidence or application
semantics. They are not mandatory parts of the universal raw ingestion API.

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

An Aggregator V2 adapter and its replay parity are separate downstream work.
They are not release gates for the public raw Core; this audit does not modify
or test that aggregator.

## Public source release status

The public `v0.1.0` source release, original history isolation, noreply
authorship, anonymous clone/module import and CI were checked on 2026-11-14.
This is a **source publication milestone**, not a production acceptance gate.
The [historical release audit](PUBLIC_RELEASE_AUDIT_2026-11-14.md) records the
pre-publication baseline and its then-open conditions.

## Remaining release and operation gates

- Raw consumer ACK cursors are now persisted per source/consumer and bound to
  the WAL journal ID. They provide at-least-once resume when the consumer
  commits first and ACKs second. Add WAL retention/compaction based on the
  slowest consumer, and integrate ACK with the consumer's own transaction
  where its storage engine supports a shared atomic boundary. Current `Durable`
  handler mode still does not guarantee exactly-once sink effects.
- Prove live/replay equivalence for stateful consumers requiring canonical
  ordering; current source WAL replay preserves only source-local order.

- Private vulnerability reporting was enabled and independently verified on
  2026-11-15. Recheck this setting on future repository migrations.
- Review the full tracked tree and Git history for provider credentials,
  private endpoint details, WAL contents and sensitive fixtures. `.gitignore`
  does not remove secrets from existing history.
- Recheck anonymous clone, pinned `go get`, issue/PR links, security reporting
  and CI for each significant public version. Local unit/race/vet checks do
  not constitute provider compatibility or load acceptance.

None of the above debt is silently treated as complete by ordinary reconnect,
WAL replay or matching A/B observations.
