# Raw Core post-fix integration verification — 2026-11-16

Scope: the **uncommitted** `codex/core-audit-2026-11-15` working tree of
`solana-yellowstone-grpc-data-core`, plus the current `sol_agregator` working
tree and its separate Core smoke module. This is not a verification of the
published `v0.1.1` tag or a production acceptance certificate.

## Verified locally

| Check | Result |
| --- | --- |
| Core `go test -race -count=1 ./...` | PASS |
| Core `go test -shuffle=on -count=3 ./...` | PASS |
| Core `go vet ./...` and `go build ./...` | PASS |
| Core `staticcheck@v0.8.2 ./...` | PASS after resolving SA4000/ST1005 |
| Core `go mod tidy -diff`, `gofmt -l .`, `git diff --check` | PASS after updating `go.sum` |
| Original five audit probes (`run_probes.py`, race enabled) | **5/5 PASS** |
| Core CLI `help` and program-agnostic `capture --dry-run` | PASS |
| `govulncheck@v1.9.0 ./...` | No known vulnerabilities found |
| `gitleaks@v8.30.1 git --redact` | No leaks in 11 scanned commits |
| `sol_agregator`: race tests, vet, build | PASS |
| Separate `solana-yellowstone-grpc-core-smoke`: race tests, vet, build | PASS |

## Confirmed defects fixed in this working tree

The earlier [five-probe audit](AUDIT_FOLLOWUP_2026-11-16.md) had exposed:
post-`CURRENT` rollback uncertainty, legacy identity incompatibility, silent
WAL format switching, impossible segment/message capacity combinations, and
untrusted sparse-index offsets. All five original reproductions now pass.

Additional independent review found and the current tests cover:

- WAL v2 validates its entire committed chain before invoking a replay
  visitor; corrupted sealed metadata no longer leaks a verified prefix to
  external callbacks.
- WAL v2 reopens a valid initial header-only journal without `CURRENT` and
  repairs an exact partial header of an empty active segment after rotation;
  ambiguous corruption still fails closed.
- v1-to-v2 migration rejects unknown JSON envelope fields that `Record`
  cannot represent. CLI read-only preflight applies the same compatibility
  check. Failed conversion leaves the source intact and publishes no target.
- Reconnect replay tracks account and transaction-status observations
  separately from transaction progress; account-only, status-only and mixed
  subscriptions have focused integration coverage.
- The timestamp recorded for a Yellowstone update is captured at `Recv`,
  before byte-budget backpressure.
- `ReadRawArchive` retains a registered consumer's epoch in returned cursors,
  allowing subsequent `Ack`.
- Live/restarted RawReader and offline archive agree on historical quality
  before and after a `continuity_broken` marker.

## Remaining release gates

Additional local hardening in this working tree now verifies the sealed WAL v2
chain before recovery can truncate an active tail. A regression test first
reproduced the destructive rejected-reopen behavior and now passes. WAL v2 also
prunes obsolete manifest generations after publication/reopen while retaining
CURRENT and two predecessor generations; pruning is best-effort and does not
turn an already committed batch into an ambiguous failure. These changes have
focused replay/reopen coverage, but are not full crash/power-loss acceptance.

1. **WAL v2 disk lifecycle and recovery scale.** One manifest generation is
   still written per append batch, with obsolete generations now pruned.
   Cold-start verification/index rebuild and some reader paths
   remain proportional to retained history. Persistent checkpoint/frontier,
   bounded seek, and measured long-running recovery are pending.
2. **Retention and consumer lifecycle.** No production retention/compaction
   policy based on the slowest supported consumer ACK exists yet. Expired
   cursors, disk reclamation and dedupe continuity across compaction need an
   end-to-end protocol and crash tests. Historical quality reconstruction for
   a resumed reader may scan O(cursor prefix) records.
3. **Fault and load acceptance.** The local tests cover specific failures,
   but not the full kill/short-write/ENOSPC/fsync/rename matrix, sustained
   1×/3×/10× throughput, slow disk/consumer scenarios, or measured p95/p99,
   RSS, disk growth and restart time. Source completeness still requires an
   independent full-block/account oracle and provider validation.
4. **Provider replay and completeness acceptance.** Class-specific reconnect
   frontiers now cover every supported `SubscribeRequest` update class:
   transactions, transaction statuses, accounts, slots, block-meta, blocks,
   entries and block footers. A mixed subscription uses the minimum observed
   frontier; if any requested class has no observed slot, the Core does not
   infer a cursor from unrelated progress. Unit, WAL-recovery and loopback
   reconnect tests cover these rules. Provider replay rejection, retention
   windows and actual historical completeness remain endpoint-specific and
   require live-provider acceptance; a replay cursor is not a completeness
   certificate.

Two concurrency risks remain to be measured under load: `Store.ReadAfter`
holds the Store mutex during disk reads, and a not-closed offline reader can
hold a shared file lock that delays a new writer. Neither was demonstrated as
a failing production workload in this audit. The full canonical Aggregator V2
bank/account state machine and RPC repair are separate from the raw Core.

No live Yellowstone provider session, release publication, Git commit or push
was performed. The current v2 WAL remains **experimental opt-in**.
