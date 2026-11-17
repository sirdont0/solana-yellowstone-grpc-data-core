# WAL v2: design contract and acceptance gates

Status: **experimental implementation; acceptance incomplete** (2026-11-17).
The default remains the `YLABWAL1` single-file journal; `WALVersion=2` opts into
the segmented writer in this working tree. The invariants below are acceptance
requirements, not a claim that the implementation satisfies them. The
[2026-11-16 recheck](AUDIT_FOLLOWUP_2026-11-16.md) originally reproduced five
defects; subsequent fixes pass all five regression probes. Automatic in-place
migration is not implemented. An explicit, verified v1-to-v2 copy into a
separate data root is implemented. Release acceptance still requires the
complete fault/load matrix and a review of the binary envelope and failure
model.

## Invariants

1. Every accepted source-local record has a unique, contiguous sequence within
   `(source, journal_id, subscription_identity)`. Rotation never resets it.
   This sequence does not certify canonical bank order or source completeness.
2. A record is **durable** only after its data and committed manifest have
   been published. A failure before manifest publication rolls back the batch;
   a failure after `CURRENT` may have advanced returns
   `ErrWALV2CommitUncertain` and makes the writer terminal until reopen and
   reconciliation. An observer is never told that an uncertain batch is
   definitely committed.
3. A sealed segment is immutable. Corrupt content or sequence gaps inside a
   sealed segment stop recovery. Only the unsealed, last active segment may
   have a torn tail truncated, and repair emits a durable continuity marker.
   Recovery validates the existing sealed chain and committed active prefix
   before truncating an active tail. Best-effort manifest cleanup retains CURRENT and two predecessor
   generations; it never deletes WAL records.
4. A checkpoint or sparse index is an acceleration hint. Validated WAL frames
   and their identity/sequence chain are authoritative; a stale or corrupt
   hint is rebuilt or ignored. A checkpoint never upgrades source quality.
5. Retention must preserve all records needed by supported consumer cursors,
   source replay/recovery and any configured full-history requirement. Expired
   consumer cursors yield an explicit error and resync path, never a silent jump.

## Proposed on-disk structure

The current opt-in implementation uses `data_dir/source/journal.v2/` with
`CURRENT`, `manifests/`, `segments/` and optional `indexes/`. Verified writer
reopens persist sealed seek indexes. Offline readers use but never write these
caches. The structure below remains an extended target: durable recovery
checkpoints and incremental dedupe recovery are not yet built.

```
data_dir/source/
  journal.wal                   # existing v1; never modified by v2 writer
  v2/
    CURRENT                     # atomic manifest generation pointer
    manifests/00000001.json    # immutable, checksummed manifests
    segments/00000001.open     # one active segment, exclusive writer
    segments/00000002.seg      # sealed immutable segment
    indexes/00000002.sidx     # optional rebuildable sparse index
    checkpoints/00000002.json # optional, WAL-bound acceleration artifact
```

The v2 segment header must include magic/version, journal ID, subscription
identity hash, source ID, first sequence and an envelope schema/descriptor
digest. A frame should contain a bounded length, CRC32C or stronger checksum,
sequence and versioned binary observation envelope. The envelope must preserve
the original raw protobuf payload *as available after gRPC decoding*, record
kind/commitment, source generation, optional bank-ID presence, provider time and
received time. A sealed footer commits record count, last sequence, byte length
and content digest. Exact wire layout and protobuf descriptor are still to be
reviewed; keep this document independent of a speculative struct layout.

Manifest entries bind segment identity, first/last sequences, sealed digest
and schema. The active segment may be referenced with its last **fsynced**
sequence only. A cursor carries `(source, identity, journal_id, sequence)`,
not a physical offset; changing segment names or rebuilding indexes does not
invalidate a cursor. Journal ID must change when the full history is recreated
or intentionally reinitialized, even if sequence numbers repeat.

## Write/rotation/recovery protocol

1. Reject an incompatible schema/identity before opening the writer. Acquire
   one stable OS file lock for the entire source journal.
2. Pre-encode and size-check each bounded batch. Append frames, fsync segment,
   publish its committed manifest and CURRENT durably, then expose accepted
   records. Segment fsync alone does not commit a v2 batch.
3. On rotation, flush the active segment, append a sealed footer, fsync it,
   rename the sealed file, and fsync its directory. Publish a new immutable
   manifest through temp-write, file-fsync, rename and directory-fsync. The
   manifest references the sealed segment and next empty active segment before
   that `.open` file is created. Recovery may recreate the missing empty file
   or finish an exact prefix of its expected header.
4. On startup validate the manifest pointer and chain, then scan from the last
   fully verified checkpoint. Reconcile a sealed segment written before its
   manifest update by validating its header/footer/digest. A missing segment
   referenced by a committed manifest is terminal. Never invent the next
   sequence or silently skip a segment.
5. For the active `.open` file, validate the committed prefix first, then
   truncate bytes beyond the manifest's committed offset. These can include
   complete, fsynced frames whose manifest was never committed. Store persists
   a continuity marker before continuing. CRC/sequence errors inside the
   committed prefix require fail-closed investigation; do not reinterpret
   them as routine tail truncation.
6. A persistent dedupe index is tied to journal ID and an indexed WAL frontier.
   It may lag the WAL and be replayed forward. If it claims a frontier ahead of
   verified WAL, discard/rebuild it. Duplicate/conflicting historical keys
   must remain discoverable after hot-cache eviction and restart.

## v1 compatibility and migration

The v1 reader remains supported. `corectl migrate` now performs a strict
read-only preflight or, with `--destination-data-dir`, creates and verifies a
v2 copy under a separate root. It validates the source frames, compares
record count and content digest, and publishes the destination directory only
after verification. It does not switch an existing writer in place. The
original v1 WAL and its ACK files remain untouched. Legacy records without
explicit sequences are numbered in original frame order. The converted v2
journal gets a new journal ID, so old ACK files cannot be reused silently.
Unknown v1 JSON envelope fields cause an explicit migration error because the
current record schema cannot faithfully encode them in v2.
Any future in-place cutover must explicitly define consumer re-registration,
rollback and journal-pointer publication semantics.

## Required acceptance tests

| Scenario | Required result |
| --- | --- |
| Legacy v1 fixture, including missing sequence and journal ID | Identical payload/order after read or explicit migration; original bytes unchanged |
| Crash before/after segment fsync, seal, rename, directory fsync, manifest rename, `CURRENT` rename | Recover the manifest-committed prefix; preserve uncertain commits for reconciliation; never discard an acknowledged record |
| Torn final frame and corrupt interior/sealed frame | Tail truncation + quality marker only for permitted torn active tail; interior corruption blocks admission |
| Corrupt/missing/stale index or checkpoint | Rebuild from validated WAL; no skipped or invented records |
| Cursor near end of many segments | Bounded seek and page read without scanning all payloads; cross-segment cursor parity |
| Slow reader, retention boundary and expired cursor | Slowest supported cursor protected; otherwise explicit `ErrCursorExpired`/resync gate |
| Late duplicate and conflicting payload after hot-cache eviction/restart | Exact duplicate suppressed, conflict fatal, same result as full WAL replay |
| Concurrent process writers/readers and platform lock failure | One writer only; unsupported lock fails closed |
| Oversize frame, disk full, short write, fsync failure | Bounded allocation, no false durable callback, sticky error and tested rollback semantics |
| Full input replay and 1×/3×/10× capture benchmark | Same source-local identities/records, measured CPU/RSS/p95/p99/fsync and recovery time |

No throughput or production durability claim follows from compilation or the
current local race suite. The full v2 acceptance and fault-injection matrix
remain open; passing rotation/reopen tests alone does not close them.

The [2026-11-17 verification](AUDIT_VERIFICATION_2026-11-17.md) adds real process
kills at six append/rotation publication boundaries, active-prefix corruption
preservation, closed-reader boundary/identity regressions, logarithmic segment
lookup, and a reproducible local 1x/3x/10x WAL experiment. It is not a complete
filesystem fault matrix or a production-provider acceptance result.
