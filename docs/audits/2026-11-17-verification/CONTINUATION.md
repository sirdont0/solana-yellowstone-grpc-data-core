# Continuation on dev — 2026-11-17

The local and public branch was renamed to `dev`, preserving commit
`115a1d3`. The former `codex/core-audit-2026-11-15` ref was removed in the same
atomic push that created `dev`. Main and release tags were not changed.

## Recovery implementation

See [recovery protocol and limitations](../../RECOVERY_CHECKPOINT.md).
WAL v2 plus disk dedupe can now reuse verified Store state and an unchanged
dedupe database. Only records after the saved frontier update that state/index.
Historical observer delivery is preserved. A missing, changed or damaged cache
or index falls back to WAL rebuild. The cache is published at clean close and
does not authorize WAL retention.

On 10,000 records with warmed sealed seek indexes and three iterations:

| Path | Mean open | Allocated bytes/open | Allocations/open |
| --- | ---: | ---: | ---: |
| Full state/dedupe rebuild | 138.97 ms | 54,445,821 | 523,671 |
| Verified snapshot reuse | 57.63 ms | 12,554,277 | 108,627 |

[Benchmark output](recovery-benchmark.txt). This isolates Store open without a
historical observer. It excludes fixture setup, snapshot creation and close.
It is not an end-to-end Engine or large-archive performance guarantee.

## Extended local WAL experiment

An isolated compiled test binary ran with `CORE_WAL_LOAD=1` and
`CORE_WAL_LOAD_SECONDS=30`. Each profile offers 30 seconds of traffic, then
fully drains scheduled writes. Payloads are 1 KiB; batches contain up to 64
records; segments are 2 MiB. A slow reader concurrently validates its pages,
and all records are checked after reopening. No provider or disk-dedupe Store
is included in this WAL-only experiment.

| Offered rate | Records verified | Write elapsed | Actual rate | Append p95 / p99 | Max schedule lag | Recovery |
| ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 1,000/s | 30,000 | 30.03 s | 998.9/s | 37.07 / 74.32 ms | 0.085 s | 0.349 s |
| 3,000/s | 90,000 | 46.03 s | 1,955.4/s | 39.05 / 74.97 ms | 16.03 s | 1.120 s |
| 10,000/s | 300,000 | 156.18 s | 1,920.8/s | 44.97 / 74.95 ms | 126.18 s | 3.468 s |

[Full measured output](load-30s.txt): 420,000 records verified, no mismatch.
Total process time 238.30 s; CPU 12.70 s user + 27.73 s system; maximum resident
set size 28,049,408 bytes. This is actual process RSS from macOS `time -l`, not
Go heap. It includes the test runtime and all three profiles. It does not bound
the memory of a real gRPC/dedupe/consumer deployment.

Data integrity passes; sustained 3x/10x throughput does not. Four minutes of
local WAL exercise does not close a production soak/fault acceptance gate.

## Remaining gates

Final local verification: full `GOWORK=off go test -race -count=1 ./...` PASS;
checkpoint regressions with shuffle and count=3 PASS; vet/staticcheck PASS;
Linux amd64 and macOS arm64 compilation PASS; tidy/diff checks PASS; gitleaks
working-tree scan reports no findings. These are local checks, not a remote
CI or Linux runtime acceptance result.

- General crash-time incremental recovery, without full rebuild when the
  derived index changed after the saved checkpoint; full historical hashing
  and observer work also remain.
- Authoritative retained state, atomic retention publication and deletion
  frontier coordinated with consumer registration/ACK epochs. No automatic
  WAL segment deletion was implemented in this continuation.
- The complete I/O fault matrix and sustained full public Engine acceptance,
  including configurable disk limits and target workload thresholds.
- A real Yellowstone provider run. Only example configuration files were
  present; `YELLOWSTONE_ENDPOINT/TOKEN` and `CORE_PROVIDER_A/B_ENDPOINT/TOKEN`
  were not configured. Credentials from the aggregator were not accessed.

Program/CPI parsing and the aggregator remain outside this public raw-Core work.
