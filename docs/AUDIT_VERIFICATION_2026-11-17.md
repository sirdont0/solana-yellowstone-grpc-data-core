# Public Core verification — 2026-11-17

Scope: `solana-yellowstone-grpc-data-core` only. Branch:
`codex/core-audit-2026-11-15`, base HEAD
`25267f5affb6f0e1de7a6572aa7217b85fcc9cad`. Results apply to the uncommitted
working tree, not to the published v0.1.1 tag. No commit, push or release was
performed. The aggregator was not modified.

**Result: additional defects repaired and local checks passing; the complete
WAL v2 production acceptance plan is still NOT CLOSED.**

## What was checked and repaired

The two previously reported changes are present and their tests pass:

- The sealed chain is verified before active-tail repair. Corrupt sealed
  history must leave the active file unchanged.
- Best-effort manifest cleanup retains CURRENT and two preceding generations,
  both after successful publication and after verified reopen. This removes
  obsolete metadata files, not WAL records. Failed cleanup is not a failed
  data commit. Retained manifests are diagnostic history, not an automatic
  recovery fallback to an older committed prefix.

Three additional defects were reproduced by failing tests before fixing them:

| Defect | Change | Regression evidence |
| --- | --- | --- |
| Corrupt committed active frame plus extra tail: failed open truncated the tail before discovering corruption | Verify active frames/count/sequence before truncation | `TestWALV2CorruptActivePrefixDoesNotTruncateTail`: failed before, passes after; bytes unchanged and visitor never called |
| Reader created from an old, closed Store returned records appended by a subsequent Store in v2 | Cap returned page at the old Store's durable sequence | `TestClosedStoreReaderKeepsItsDurableBoundary`: v2 failed before; v1/v2 now pass |
| After replacing the WAL directory, an old closed Store silently opened the new journal under the old cursor | Validate journal ID and subscription identity at the original durable frontier under the reader lock; public raw reader reports `ErrCursorExpired` | `TestClosedStoreReaderRejectsReplacedJournal`: both versions failed before; public `TestRawReaderRejectsJournalRecreatedAfterShutdown` additionally checks zero delivery and unchanged cursor |

Additional implementation improvements:

- WAL v2 page reads binary-search sealed sequence ranges. They no longer copy
  or linearly walk every prior segment for each page. Frame lookup within a
  chosen segment continues to use its sparse index. This improves live seek,
  **not** startup recovery, which still scans history.
- `examples/rawconsumer --output-db` commits an observation, its idempotency
  key and a sample application effect in one durable bbolt transaction, then
  ACKs Core separately. Reopening after commit without ACK and redelivery do
  not apply the effect twice. Conflicting data at the same cursor rolls back;
  a different journal is a different identity. This is a runnable public API
  example, not a transaction spanning Core ACK and external systems.
- The WAL contract now distinguishes segment fsync from manifest commitment
  and describes the actual empty-active creation/recovery order.

## Crash evidence

`TestWALV2ProcessKillRecovery` starts a separate writer process, waits for an
explicit boundary, kills it without Close, then opens and appends from the
parent process. It verifies record order/count, stable journal ID, OS lock
release, a successful new append and a second reopen.

| Kill boundary | Expected recovery | Result |
| --- | --- | --- |
| Append: data synced, before CURRENT publication | Previous committed prefix only | PASS |
| Append: after CURRENT publication, before Append returns | Includes newly committed record | PASS |
| Append: after successful return | Includes acknowledged record | PASS |
| Rotation: sealed rename completed, before CURRENT publication | Previous record preserved, rotation recovered | PASS |
| Rotation: after CURRENT publication, before new active file creation | Missing empty active recreated; previous record preserved | PASS |
| Rotation and following Append returned | Both committed records preserved | PASS |

These are process-kill tests on macOS, not machine power-loss tests. They do
not exercise every short-write, ENOSPC or file/directory-fsync failure window.
The five earlier overlay regression probes were also rerun with `-race` and
passed, including an error after CURRENT rename and before directory fsync.

## Local measurements

Host: Apple M4 Pro, macOS arm64, Go 1.26.11. These measurements describe this
machine, filesystem and test configuration; they are not provider throughput.

The opt-in `TestWALV2SyntheticLoad` offers 5 seconds of synthetic traffic per
profile, then drains all scheduled writes. It uses 1 KiB payloads, batches of
64, 2 MiB segments and a slow reader taking up to 128 records every 200 ms.
After closing, every record's sequence, detail and payload is checked during
writer recovery. The reported append latency excludes schedule lag, which is
reported separately. No generated production logs or credentials are retained.

An initial run overlapped other checks. The table below uses the **second run,
with no other audit test/build/scan jobs running**:

| Offered records/s | Verified records | Drain elapsed | Achieved records/s | Append p95 / p99 | Max schedule lag | Verified recovery |
| ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 1,000 | 5,000 | 5.051 s | 989.9 | 36.63 / 68.76 ms | 0.071 s | 45.11 ms |
| 3,000 | 15,000 | 7.214 s | 2,079.2 | 37.77 / 67.89 ms | 2.214 s | 108.85 ms |
| 10,000 | 50,000 | 23.473 s | 2,130.1 | 36.94 / 67.89 ms | 18.473 s | 289.54 ms |

All 70,000 records were verified. The 3x and 10x profiles did **not** sustain
their offered rate with these batch/segment settings. A passing data-integrity
test must not be presented as a throughput acceptance pass. This is a WAL
experiment with scheduled synchronous batches, not a measurement of the full
gRPC ingest pipeline, its queue budgets, disk dedupe, or provider completeness.
Peak sampled Go heap was approximately 3.7–3.9 MB; this is neither whole-process
RSS nor a demonstrated memory bound. No long-duration soak test was performed.

The 100-iteration append microbenchmark used 64 x 1 KiB batches:

| Format | Batch mean | Payload throughput | Batch p95 / p99 | Full replay verification |
| --- | ---: | ---: | --- | ---: |
| v1 | 4.95 ms | 13.24 MB/s | 6.01 / 6.12 ms | 68.10 ms |
| v2 | 28.10 ms | 2.33 MB/s | 30.92 / 31.83 ms | 73.88 ms |

Synthetic metadata-only tail-seek benchmarks with 10 / 1,000 / 100,000 sealed
ranges used 416 B and one allocation per poll in all three cases. Measured
times were 219 / 149 / 234 ns respectively (100 iterations, too few for precise
timing claims). These fixtures isolate segment selection and allocate no
historical segment files; they do not establish fast recovery from large WALs.

## Verification

| Check | Result and scope |
| --- | --- |
| `GOWORK=off go test -race -count=1 ./...` | PASS after final runtime/example changes; [saved output](audits/2026-11-17-verification/race.txt) |
| `go test -shuffle=on -count=3 ./...` | PASS after final runtime/example changes; [saved output](audits/2026-11-17-verification/shuffle.txt) |
| `go vet ./...`, staticcheck v0.8.2 | PASS after final runtime/example changes |
| `go build ./...` | PASS |
| Linux amd64 and macOS arm64 builds | PASS compilation; Linux runtime was not executed on this host |
| `go mod tidy -diff`, gofmt, `git diff --check` | PASS |
| govulncheck v1.9.0 | No vulnerabilities found |
| gitleaks v8.30.1, redacted working tree and Git history | No findings; 11 commits scanned in Git history |
| CLI help and command package tests | PASS; no provider session started |
| Earlier five overlay probes | PASS with race detection |
| Synthetic load | Integrity PASS; sustained 3x/10x throughput not met for this fixture |

## Remaining work and closure conditions

The original three large directions remain. The following is a dependency
order, not a claim that those features exist. They are part of the requested
completion work and were **not completed in this audit pass**.

### 1. Recovery and persistent indexes

1. Define and version a durable recovery snapshot binding source state,
   continuity quality, journal/subscription identity, WAL frontier and dedupe
   frontier. Freeze the v2 envelope/schema compatibility policy; the proposed
   header descriptor digest is still absent from the current format.
2. Persist a validated segment seek index and recover the active tail from a
   verified frontier. Current indexes are reconstructed on every v2 open;
   binary-searching segments does not remove that full history scan.
3. Make disk dedupe recover incrementally. It currently rebuilds a new bbolt
   database from the whole WAL on each open, with O(history) work and temporary
   space for a second index. Specify recovery for lagging, ahead-of-WAL,
   missing, corrupt or incompatible checkpoints/indexes.

Closure: compare full replay with accelerated recovery over identical
fixtures, including conflicts after hot-cache eviction; no quality upgrades,
skipped records or altered identities; demonstrate startup I/O/time as a
function of the unrecovered tail and explicitly bound remaining metadata work.

### 2. Retention, compaction and disk admission

4. Establish a durable retention baseline before deleting history. A cache
   described as a disposable hint cannot be the only copy of state/dedupe
   needed after source WAL deletion. Either retain reconstructible history or
   make the snapshot an authoritative, manifest-bound recovery artifact with
   its own durability/integrity contract. Resolve this explicitly first.
5. Compute a deletion frontier from all supported consumer ACKs plus recovery,
   replay, dedupe horizon and full-history requirements. Serialize it with
   register/unregister/ACK lifecycles. A new consumer, removed consumer, stale
   epoch and restored ACK file must each have defined behavior.
6. Publish retained ranges atomically before reclaiming eligible sealed files;
   preserve journal sequence identity and quality at the retained floor.
   Return an explicit expired-cursor/resync error for removed history. Add
   high/low disk watermarks and terminal disk-full behavior that does not
   acknowledge unwritten data or erase unacknowledged consumer history.

Closure: slowest consumer blocks deletion, faster consumers continue, crashes
at every publication/deletion step retain the acknowledged state, stale
cursors cannot silently jump, restart/dedupe remain correct after actual
segment removal. Current manifest cleanup satisfies none of these deletion
requirements because it does not remove records.

### 3. Failure and sustained-load acceptance

7. Complete the I/O failure matrix: partial frame/header/footer writes,
   ENOSPC, failed/ambiguous file and directory fsync, rename failures,
   rollback failures, corrupt CURRENT/manifest/snapshot/index, rotation and
   cleanup interruptions. Verify durable notifications and sticky terminal
   errors against the selected manifest after reopening.
8. Run the full public `NewRaw` pipeline under a defined multi-source profile
   with disk dedupe, reconnects, slow/stalled consumers, cancellation/drain,
   bounded queues and long retention. Record CPU, actual RSS, queue/ACK lag,
   p95/p99 receive-to-durable latency, recovery time and loss/conflict checks.
   Tune batching/publication cost and repeat 1x/3x/10x acceptance. The current
   small WAL experiment cannot substitute for this test.
9. Execute runtime tests on supported Linux/macOS environments and document
   filesystem assumptions. Cross-compilation is not runtime or power-loss
   verification; unsupported platforms continue to fail closed on locking.

Closure: publish reproducible workloads and explicit success thresholds,
including finite resource/backlog bounds at the supported target rate, with
all failure cases classified. Do not label an eventual finite drain as
sustained-rate acceptance.

### External provider gate

10. Run against the actual Yellowstone endpoint/version and intended filters:
    authentication, configured update kinds, replay availability/rejection,
    reconnect and source continuity reporting. Supply a non-secret config
    path and target source/rate/retention profile; keep tokens in environment
    variables. No real provider verification was performed in this audit.

Program instruction interpretation, CPI reconstruction, application token
models, canonical bank admission and RPC/account-history backfill are separate
layers. They are not prerequisites for this raw public Core scope and must
not be added to its closure list as mandatory application functionality.

## Reproduce

Run from the public repository root:

```sh
GOWORK=off go test -race -count=1 ./...
go test -shuffle=on -count=3 ./...
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
python3 docs/audits/2026-11-16-recheck/run_probes.py
go test ./internal/ingest -run '^TestWALV2ProcessKillRecovery$' -count=1 -v
CORE_WAL_LOAD=1 go test ./internal/ingest -run '^TestWALV2SyntheticLoad$' -count=1 -v
go test ./internal/ingest -run '^$' -bench '^BenchmarkWAL' -benchtime=100x -count=1
```

Run load/benchmark commands without other build/test jobs for meaningful
comparisons. The opt-in experiment writes only temporary fixtures, but a
larger target profile requires checking available disk space first.
