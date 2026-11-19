# CORE-C2-01: WAL v1 fsync fault injection

**Date:** 2026-11-19. **Result:** local race regression PASS; integrated G2 status is recorded in [INTEGRATION_REPORT](INTEGRATION_REPORT.md).

Code: `internal/ingest/wal.go` adds the private `syncFrame` injection
seam. The production path continues to call the same `os.File.Sync` if
the seam is nil. It changes neither the on-disk format nor the public API.

Test: `TestC2WALV1FsyncFailureDoesNotAcknowledgeBatch` in
`internal/ingest/c2_fsync_fault_test.go`.

- A baseline batch is durable before the fault.
- A subsequent batch writes frames but receives an injected `fsync` error.
- `Store.AppendBatch` returns an error and no accepted-record result;
  durable observer callbacks do not fire for that batch.
- WAL count and cursor do not advance, and the writer remains sticky-failed.
- `Store.Close` preserves the sticky error while releasing resources.
- Reopening retains the prior committed prefix. A failed candidate record is
  absent. Recovery may append its own lifecycle/uncertainty markers.

Validation:

    GOWORK=off go test -race -count=1 -timeout=40s -run 'TestC2WALV1FsyncFailureDoesNotAcknowledgeBatch|TestC2SlowLiveSinkPreservesQueueBoundAndDrains' ./internal/ingest

Exit status 0. This is injected syscall failure *after* frame writes, not a
physical power cut during a storage controller flush. Actual v1 process-kill
boundaries (partial frame, real fsync completed before AppendBatch returned,
and AppendBatch returned) are covered by
[WAL_V1_PROCESS_CRASH](WAL_V1_PROCESS_CRASH.md). Cursor process-kill boundaries
are covered separately by [CURSOR_PROCESS_CRASH](CURSOR_PROCESS_CRASH.md).

An additional `TestC2WALV1ENOSPCIsStickyAndPreservesPrefix` injects
`syscall.ENOSPC` at the v1 frame-write seam. It verifies an empty accepted
result, zero durable callbacks, sticky failure, preserved prefix and no
failed candidate in reopened replay. Its targeted `-race` run passed.
This is simulated ENOSPC, not a physically exhausted filesystem. Physical
filesystem exhaustion and sudden power loss remain NOT_RUN and are deployment
hardware/filesystem acceptance, not claims established by this local suite.
Cursor atomic-write error and process-crash boundaries have separate
[local evidence](CURSOR_ATOMIC_FAULT.md) and
[process-crash evidence](CURSOR_PROCESS_CRASH.md).
