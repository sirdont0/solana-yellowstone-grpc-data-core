# Verified recovery cache for WAL v2

Implemented on the public `dev` branch, 2026-11-17. This is a recovery
optimization for `WALVersion=2` plus `DedupeDisk=true`, not permission to delete
WAL history. v1 and RAM-only recovery retain their previous behavior.

## Publication and admission

On a successful Store close, while holding the Store mutex:

1. The session-end record is already committed to WAL.
2. Sync the derived bbolt dedupe database.
3. Save source state, continuity quality/reasons, last record and WAL frontier.
4. Bind these to the journal/subscription identity, sealed manifest prefix,
   active-prefix SHA-256 and complete dedupe-file SHA-256.
5. Publish checksummed `recovery-cache.json` with atomic rename and directory
   sync. Failure is an optional-cache failure; it does not undo a WAL commit.

On reopen, Core first acquires and verifies the WAL as before. It accepts the
cache only if the checksums, identities, frontiers, immutable segment prefix,
active-prefix digest and dedupe database match. The database digest is checked
again after acquiring bbolt's exclusive lock. Only then is state restored.
Existing records are not reapplied to Store state or reinserted into dedupe;
the tail after the saved frontier is applied in source order. A rotation after
the saved frontier is supported when the old prefix still matches.

The `OpenStoreObserved` historical callback contract is preserved: observers
still receive the complete retained WAL history. A callback is not silently
resumed from a frontier for which it has no saved application state.

## Fallback and limits

- Missing, truncated, inconsistent or stale cache: full state/dedupe rebuild.
- Changed/missing dedupe database: full rebuild, including after Abort with
  newer index writes. Old state is never paired with an unverified newer index.
- Unchanged index plus newly committed WAL tail: reuse verified state/index
  and replay that tail, including the WAL-commit-before-index-write window.
- Corrupt committed WAL: fail closed before cache admission.
- The clean-shutdown marker remains authoritative for interrupted-session
  reporting; using a cache never repairs source continuity by itself.
- Full WAL content hashing remains O(history bytes). The active segment is
  still validated, and observers still require historical records. The index
  file must also be hashed. This is not O(tail)-only startup.
- Cache publication currently happens at clean close, not periodically.
  A changed index after a crash falls back to the full rebuild. General
  incremental crash recovery needs an atomic state/index frontier protocol.

The cache is disposable and depends on retained WAL for recovery. **It is not
an authoritative snapshot for retention.** ACK-based segment deletion must
wait for a separate durable retained-state contract, manifest publication and
consumer-frontier coordination.

## Verification

`recovery_checkpoint_test.go` covers state/quality parity, observer history,
historical duplicates/conflicts after hot-cache eviction, tail replay across
rotation, invalid/missing snapshots, modified/missing database, restart after
Abort with index changes, and optional snapshot-publication failure.

The benchmark compares full rebuild with cache reuse on 10,000 source records,
using the same warmed sealed seek indexes. Setup, checkpoint creation and close
are outside the timed open. It does not measure a live provider or end-to-end
Engine startup:

```sh
go test ./internal/ingest -run '^$' \
  -bench '^BenchmarkWALV2StoreRecovery$' -benchtime=3x -count=1
```

The WAL-only load experiment now accepts `CORE_WAL_LOAD_SECONDS=1..300`
(default 5). This is offered duration **per profile**; overloaded profiles may
take much longer to drain. At 300 seconds, all three profiles offer 4.2 million
1 KiB records plus envelopes, so allow several GiB of temporary disk space.
The experiment is not automatically enabled in the normal unit/race suite.
