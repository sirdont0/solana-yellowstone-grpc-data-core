# CORE-C2-01 — process-crash matrix for durable consumer ACK

**Date:** 2026-11-19. **Scope:** the public raw consumer API and application commit boundary, WAL v1 and WAL v2. **Status:** `PASS` for the cases below; integrated G2 status is recorded in [INTEGRATION_REPORT](INTEGRATION_REPORT.md).

## Evidence and method

- Test: `c2_crash_consumer_test.go`, `TestC2CrashDurableConsumerRecovery`.
- Core HEAD at test time: `1d3558e957b931d85f0fc10ff9088ea20ce872ea` plus existing uncommitted work and this new untracked test.
- Runtime: `go1.26.11 darwin/arm64`, Darwin `27.0.0 arm64`.
- Provider profile / subscription hash: `N/A` — this is a local durable WAL/consumer fixture, with no network/provider traffic.
- WAL versions: v1 and v2 (v2 segment limit 128 MiB); each fixture writes two named marker records, then closes the seed writer.
- Fixture hashes: `N/A` — generated temporary journal IDs, consumer epochs, and marker timestamps vary by run. Assertions bind to actual durable cursor identity and sequences.
- Offered/accepted/persisted/read payloads: two named fixture markers per scenario; both must be recoverable, subject to the acknowledged resume boundary. WAL session/recovery markers can add records. No throughput or p50/p95/p99 measurements were attempted.
- Crash mechanism: a separate copy of the Go test binary opens the real Store and bbolt application DB, emits a readiness line **only after** its chosen operation returns, then blocks with its file handles open. The parent sends `SIGKILL`, verifies signal termination, and opens both files in a new Engine/process context. No child `Close` or deferred cleanup runs.
- Application effect: one bbolt transaction stores the normalized WAL cursor, observation payload, and effect counter. The cursor's `ConsumerEpoch` is excluded from the application's idempotency key. bbolt uses its default synchronous commits (`NoSync=false`).

## Matrix

| Case (each WAL v1/v2) | Kill boundary | Required recovery result | Result |
| --- | --- | --- | --- |
| `commit-before-ack` | bbolt transaction returned; Core ACK not invoked | Application effect count is 1; Core cursor remains at registration start; same WAL record is redelivered; idempotent reapply remains count 1; subsequent ACK survives second reopen | **PASS** |
| `ack-returned` | bbolt commit followed by successful `Engine.Ack` return | Application effect count is 1; ACK cursor survives SIGKILL and another reopen; acknowledged record is not redelivered; following marker remains; regressive ACK rejected | **PASS** |
| `rollback-before-ack` | bbolt transaction was deliberately rolled back; ACK not invoked | Application effect count remains 0; Core cursor remains at start; record is redelivered; successful reapply yields count 1; ACK survives second reopen | **PASS** |
| `ack-write-denied` | bbolt commit returned; cursor directory temporarily made unwritable so ACK's atomic temporary-file creation returned permission denied; permissions restored before kill | ACK error reaches caller; previous Core cursor survives; application effect count remains 1; record is redelivered; idempotent reapply and later ACK survive reopen | **PASS** |

`ack-write-denied` is skipped when the test runner has effective UID 0 because root can bypass directory permissions. It passed on the observed non-root Darwin run.

## Checks executed

- `go test -race ./... -run '^TestC2CrashDurableConsumerRecovery$' -count=1 -v` — **PASS**, 8/8 subcases.
- `go test -race . -run '^TestC2CrashDurableConsumerRecovery$' -count=5` — **PASS**, all five repetitions.
- `go vet .` — **PASS**.
- `git diff --check -- c2_crash_consumer_test.go` — **PASS**.

## Limits / open acceptance work

- The test establishes process-kill behavior **after** bbolt commit return, **after** ACK return, after a deliberate bbolt rollback, and after a permission-denied ACK attempt. It does not model sudden loss of power or storage-controller cache loss.
- Exact cursor temp-fsync/rename/directory-fsync process-kill windows are now covered by [CURSOR_PROCESS_CRASH](CURSOR_PROCESS_CRASH.md).
- WAL v1 partial-frame and fsync-complete-before-return process-kill windows are covered by [WAL_V1_PROCESS_CRASH](WAL_V1_PROCESS_CRASH.md).
- Injected v1 `ENOSPC` and fsync errors, cursor temp-fsync/rename/dir-fsync error paths, and bounded queue pressure have separate C2 audit reports. Physical filesystem exhaustion, power loss, and external nontransactional side effects remain **NOT_RUN**; WAL v2 writer crash tests are separate evidence.
- Source/provider consistency, inter-source ordering and replay acceptance are outside this local consumer test. This result alone does not close `G2` or establish end-to-end exactly-once delivery.
