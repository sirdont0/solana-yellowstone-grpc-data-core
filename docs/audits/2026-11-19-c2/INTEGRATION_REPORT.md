# C2/G2 and C1/G3 integration checkpoint

**Date:** 2026-11-19. **Code baseline:** e4d59af3c7b778cd9f3dff39e8b52fc8675bc532 (public/dev),
plus the uncommitted acceptance tests and audit notes listed below.
**Disposition:** C2 software contract and G2 are **LOCALLY_VERIFIED** at the
process-crash/fault/byte-budget layer. G3 remains PARTIAL / NOT_ACCEPTED because
PublicNode replay and independent provider B are unresolved. Hardware power-loss
and G5 production SLO evidence are not claimed.

## Verified local work

- **Consumer process-crash:** 8 real child-process SIGKILL cases across WAL
  v1/v2: application commit before ACK, ACK returned, DB rollback, and
  permission-denied ACK (8/8 on the reported non-root Darwin host). See
  [CRASH_MATRIX](CRASH_MATRIX.md).
- **WAL v1 faults:** injected frame-write ENOSPC and fsync failure,
  sticky failure, no false durable callback, recoverable prior prefix.
  See [WAL_FSYNC_FAULT](WAL_FSYNC_FAULT.md).
- **Atomic cursor file faults:** deterministic injected temporary-file fsync,
  rename and directory fsync failures. The post-rename failure remains an
  explicitly ambiguous physical durability boundary. Four child-process
  SIGKILL points now cover temp-fsync, pre-rename, post-rename/pre-dir-fsync and
  post-dir-fsync. See [CURSOR_ATOMIC_FAULT](CURSOR_ATOMIC_FAULT.md) and
  [CURSOR_PROCESS_CRASH](CURSOR_PROCESS_CRASH.md).
- **WAL v1 process crash:** actual SIGKILL during a partial frame write, after
  real fsync completion but before AppendBatch return, and after successful
  AppendBatch return. Recovery preserves the prefix, detects/truncates a torn
  tail, and replays the fsynced candidate without inventing a durable callback.
  See [WAL_V1_PROCESS_CRASH](WAL_V1_PROCESS_CRASH.md).
- **Partial bank:** fixed source-local and global session_end handling;
  open banks become partial, no false exact promotion, independent sources
  remain isolated. Loopback reconnect, overlap, dedupe and A/B reorder tests
  pass. See [PARTIAL_BANK](PARTIAL_BANK.md).
- **Finite sink pressure:** stalled OnLive callback keeps byte reservation;
  callback release permits ordered, lossless writer drain and clears metrics.
  The Engine-level shared dispatch queue is also bounded under a blocked,
  context-aware sink; both durable updates remain replayable after
  `ErrDrainTimeout`. See [SINK_PRESSURE](SINK_PRESSURE.md) and
  [ENGINE_DISPATCH_PRESSURE](ENGINE_DISPATCH_PRESSURE.md).

Integrated verification on the code SHA above:

    GOWORK=off go test -race -count=1 -timeout=180s ./...
    GOWORK=off go vet ./...
    git diff --check

All commands exited 0; all eight Go packages passed. Provider tests are
separate live observations, not part of this Go test run.

## G3 live findings

Seven authenticated PublicNode probes returned GetVersion, GetSlot and
SubscribeReplayInfo successfully. Selected finalized live update kinds were
observed individually. All six from_slot reopen attempts failed:
four InvalidArgument and two Unavailable. There is no independently configured
Yellowstone provider B. Earlier dual PublicNode and independent Solana RPC
transaction parity applies only to its bounded observation window.
See [G3 report](../2026-11-19-g3/REPORT.md).

## Acceptance blockers / next work

| Workstream | Remaining acceptance evidence |
| --- | --- |
| G2 durable API | Software-level process-crash/fault/byte-budget checks are now covered. Physical disk exhaustion, power-loss/controller-cache behavior and deployment-specific RSS/SLO remain outside this local acceptance and belong to the selected G5 hardware profile. |
| G3 provider | Working filter-specific from_slot overlap or a verified independent repair strategy; a separately operated Yellowstone B; combined continuous filters, quotas/retention, account and full-block witnesses and sustained coverage. |
| C3/G4 | WAL v2 authoritative retention, incremental crash recovery and full fault matrix. Depends on an accepted C2 durability contract. |

No claim of canonical transaction/account completeness or exactly-once external
effects follows from these local tests. A production gate must not be promoted
until its outstanding external and resource evidence is actually obtained.

All code changes were deliberately staged and published independently. Existing
working-tree specifications and anonymized provider diagnostic artifacts were
left local to preserve their draft/review state.
