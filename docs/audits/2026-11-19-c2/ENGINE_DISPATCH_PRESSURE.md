# CORE-C2-03: Engine shared dispatch queue pressure

**Date:** 2026-11-19. **Status:** deterministic bounded-queue regression PASS;
production SLO remains open under G5.

Test: `TestC2EngineDispatchQueueByteBudgetAndDurableReplay` in
`c2_engine_dispatch_queue_test.go`. It uses a loopback Yellowstone server,
two distinct large transaction updates, the real `Engine`/`Runner`/v1 WAL,
`Delivery=Durable`, and a context-aware sink that deliberately stalls on the
first event.

## Observed profile

| Setting / metric | Observed |
| --- | ---: |
| Max message | 1 MiB |
| Shared dispatch queue byte budget | 1,049,600 bytes |
| Per-source receive-to-WAL byte budget | 4,194,304 bytes |
| Shared dispatch channel capacity | 1 item |
| One retained raw transaction record | 717,585 bytes |
| Shared budget while sink is blocked | 717,585 / 1,049,600 bytes |
| Source budget during the observed wait | 717,585 / 4,194,304 bytes |
| Durable source updates already accepted | 2 |
| Channel depth while first item is being dispatched | 0 / 1 |

The second transaction is fsynced to the source WAL before its live dispatch
callback can reserve the shared queue budget. Its callback blocks rather than
exceeding the configured byte limit or silently dropping the durable record.
The source and shared queue metrics remain within their configured bounds.

After cancellation, the configured 50 ms shutdown drain deadline cancels the
context-aware sink and the Engine returns `ErrDrainTimeout`. Both transactions
remain readable from the closed raw WAL after the dispatch abort. This
demonstrates bounded backpressure and recoverable durable input under this
small deterministic profile.

## Verification

```sh
GOWORK=off go test -race -count=5 -timeout=120s \
  -run '^TestC2EngineDispatchQueueByteBudgetAndDurableReplay$' . -v
```

Exit status 0; all five repetitions passed with identical byte counters and
both durable transactions replayed each time.

## Limits

This is not a hard RSS measurement: queue budgets account for retained record
payload estimates, not all Go heap, gRPC buffers, protobuf decode copies,
allocator overhead, WAL page cache, or OS memory. It is not a 1×/3×/10× live
provider workload, a 24-hour soak, or a p50/p95/p99 latency result. A sink
that ignores its context cannot be forcibly interrupted by Go; consumers must
honor the handler context or isolate their own non-cancellable work. These
remain explicit G5/consumer operational constraints.
