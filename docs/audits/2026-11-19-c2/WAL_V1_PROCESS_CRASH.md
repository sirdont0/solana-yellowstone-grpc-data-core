# CORE-C2-01: WAL v1 process-crash boundaries

**Date:** 2026-11-19. **Status:** local process-crash matrix PASS; hardware
power-loss acceptance remains outside this test.

Test: `TestC2WALV1ProcessCrashRecovery` in
`internal/ingest/c2_wal_v1_process_crash_test.go`. A separate copy of the Go
test binary opens the real v1 Store and is terminated by the parent with
`Process.Kill` while the writer still owns its WAL lock. The test runs on
Unix; Windows skips the signal-specific cases.

## Matrix

| Kill point | Recovered WAL | Durable observer | Result |
| --- | --- | --- | --- |
| After writing half of a frame, before `writeFrame` returns | Previous committed prefix only; torn tail detected and truncated; `RecoveredTail=true` | Not called for candidate | **PASS** |
| After the real `os.File.Sync()` returns, before `syncFrame` returns to `AppendBatch` | Candidate frame is present on reopen | Not called; caller never observed `AppendBatch` success | **PASS** |
| After `AppendBatch` returns successfully | Candidate frame is present on reopen | Called before child announces ready | **PASS** |

The second row is an important uncertain-outcome case: a process can die after
the kernel reports a successful fsync but before the caller observes a
successful append. The record is recoverable even though the callback did not
run. A consumer must replay from its last acknowledged cursor and use its
application idempotency key; it must not infer the outcome from the missing
callback.

The partial-frame case confirms that a torn final frame does not damage the
previous prefix. Reopening reports the torn tail, truncates it, and a second
open sees a stable repaired prefix. It does not attempt to infer whether the
candidate transaction existed upstream; continuity recovery remains an
explicit quality concern at the Store/consumer layer.

## Verification

```sh
GOWORK=off go test -race -count=5 -timeout=120s ./internal/ingest \
  -run '^TestC2WALV1ProcessCrashRecovery$'
GOWORK=off go vet ./internal/ingest
```

Both commands exited 0. The five race-enabled repetitions passed all three
subcases. Runtime: `go1.26.11 darwin/arm64`, Darwin 27.0.0, Apple Silicon,
14 logical CPUs, 24 GiB RAM.

## Limits

- This is an actual **process** kill while the kernel, filesystem and storage
  device continue running. It is not a power cut, kernel panic, controller
  cache loss, physical disk-full test, or kill injected inside the kernel's
  fsync syscall.
- The fsync boundary case executes a real successful `File.Sync()` and then
  pauses before the Go append call returns. It does not model a stalled or
  interrupted storage-controller flush.
- Physical `ENOSPC` remains NOT_RUN; the separate
  `TestC2WALV1ENOSPCIsStickyAndPreservesPrefix` injects `syscall.ENOSPC` at the
  frame-write boundary and verifies sticky failure/no false durable callback.
- This does not establish hard process RSS, sustained throughput, p99 latency,
  or a provider/network SLO. Those remain operational acceptance work under
  G5.
