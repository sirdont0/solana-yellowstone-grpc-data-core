# CORE-C2-03: slow live sink and bounded drain

**Date:** 2026-11-19. **Status:** BYTE-BOUNDED QUEUES LOCALLY VERIFIED;
HARDWARE/SLO ACCEPTANCE OPEN.

Test: TestC2SlowLiveSinkPreservesQueueBoundAndDrains in
internal/ingest/c2_sink_pressure_test.go.

The test uses a real v1 Store, the production batched writer, and a stalled
OnLive callback. It verifies:

1. A durable but callback-blocked observation retains its queue-byte budget.
   A second record cannot be admitted above that limit.
2. After the callback resumes, the writer admits the next record, drains in
   source order and invokes both callbacks exactly once.
3. WAL sequence advances twice, the reservation returns to zero, and the
   writer pending/oldest-age metrics clear.

Executed checks:

    GOWORK=off go test -count=1 -timeout=20s -run '^TestC2SlowLiveSinkPreservesQueueBoundAndDrains$' ./internal/ingest
    GOWORK=off go test -race -count=1 -timeout=40s -run '^TestC2SlowLiveSinkPreservesQueueBoundAndDrains$' ./internal/ingest

Both returned exit status 0. A second end-to-end Engine test,
`TestC2EngineDispatchQueueByteBudgetAndDurableReplay` in
`c2_engine_dispatch_queue_test.go`, exercises the **shared post-WAL dispatch
queue** with two ~700 KiB updates and a context-aware blocked sink. Five
race-enabled repetitions observed exactly 717,585/1,049,600 shared bytes,
717,585/4,194,304 source bytes, two durable updates, `ErrDrainTimeout` after
the configured deadline, and successful replay of both updates after abort.
See [ENGINE_DISPATCH_PRESSURE](ENGINE_DISPATCH_PRESSURE.md).

These tests establish deterministic retained-payload byte bounds and
fail-closed/catch-up behavior for the tested profile. They do not establish
hard process RSS, live throughput, source-writer p99, 1×/3×/10× capacity, or a
24-hour soak; those belong to G5. The public Engine sink receives a context and
must honor cancellation. Go cannot forcibly interrupt a handler that ignores
its context; such a handler can delay return beyond `ShutdownTimeout`, so this
cooperative contract must be respected by consumers.
