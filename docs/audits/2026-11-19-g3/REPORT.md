# CORE-C1-03 / G3 — bounded PublicNode capability matrix

**Date:** 2026-11-19 UTC (17:44–17:46).
**Source code:** `solana-yellowstone-grpc-data-core`, `dev`, HEAD `1d3558e`, with pre-existing uncommitted changes; `go1.26.11 darwin/arm64`.
**Provider profile:** authenticated PublicNode Yellowstone, one endpoint hash across seven runs, three observed `GetVersion` hashes. The latter are unary observations and do not establish stable stream-to-backend affinity, geographic diversity, or independent failure domains.
**Commitment/filter:** primarily `finalized`; separate attempted `confirmed` and `processed` `slots,blocks_meta`. Pump.fun program-filtered transaction/account/block requests use `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`.
**Disposition:** **G3 PARTIAL / NOT_ACCEPTED; CORE-C1-03 OPEN**. The new observations increase filter and replay evidence but do not establish end-to-end exact coverage.

These are bounded wire probes (initial profiles: 4 seconds/filter, 8 updates maximum; isolated retries: 5 seconds/filter, 8 updates maximum). Every probe used an independent `corectl probe` process and kept its own results; the first `finalized_kinds` process intentionally tested many sequential streams on one connection. No raw updates, WAL or credentials were captured. Token/header/endpoint were read from an existing ignored local `.env` and passed only through process environment. The JSON outputs contain endpoint/version/request digests and status codes, not token values, raw messages, or private endpoint URLs. Files were written mode `0600`.

## Observed remote results

All seven runs established TLS/gRPC dial and returned `PASS` for `GetVersion`, `GetSlot` and `SubscribeReplayInfo`; all were authenticated. The observed `first_available` values varied with time and backend: 471423011–471423900. This demonstrates a response from the replay-info method, **not** that these slots can be replayed for any specific filter.

| Commitment / subscription | Initial live result | Reopen with `from_slot=first_observed_slot` | Interpretation |
| --- | --- | --- | --- |
| finalized `slots` | **PASS**, 8 updates | `InvalidArgument`, 0 updates | Live slot updates observed; replay overlap failed. |
| finalized Pump.fun `transactions` | **PASS**, 8 | `InvalidArgument`, 0 | Live filtered transactions observed; replay failed. |
| finalized Pump.fun `transactions_status` | **PASS**, 8 | `Unavailable`, 0 | Live status updates observed; replay unproven. |
| finalized `blocks_meta` (fresh connection) | **PASS**, 8 | `InvalidArgument`, 0 | Live metadata observed; replay failed. |
| finalized Pump.fun-owned `accounts` (fresh connection) | **PASS**, 8 | `InvalidArgument`, 0 | Live account updates observed; **account-history completeness is unproven**. |
| finalized `block_footer` with certificates (fresh connection) | **PASS**, 8 | `Unavailable`, 0 | All 8 updates carried typed certificates; 16 certificate-slot-vs-carrier-slot differences observed. **No cryptographic certificate validation**. |
| finalized `blocks`, `entry`, `combined` | `Unavailable`, 0 | NOT_RUN | Capability **UNKNOWN**; an error in this bounded attempt is not proof of unsupported API. |
| confirmed `slots`, `blocks_meta` | `Unavailable`, 0 each | NOT_RUN | Capability and replay **UNKNOWN** at `confirmed`. |
| processed `slots`, `blocks_meta` | `Unavailable`, 0 each | NOT_RUN | Capability and replay **UNKNOWN** at `processed`. |
| finalized retention edge `slots` | Initial `Unavailable`, 0 | `first_available-1`: `Unavailable`, 0 | Retention behavior **UNKNOWN**; error is not a proven retention-expired classification. |

In the multi-filter `finalized_kinds` process, `slots`, `transactions`, and `transactions_status` delivered 8 updates each; every later stream returned `Unavailable` before any update, including `accounts`, `blocks_meta`, `block_footer`, `blocks`, `entry`, and `combined`. A later **fresh connection per filter** delivered `accounts`, `blocks_meta`, and `block_footer` successfully. This is compatible with a transient backend/connection/quotas issue, but the cause was **not established**. No `ResourceExhausted`, 429 or explicit stream-limit detail was captured, so PV-08 remains open. The probes do not demonstrate continuous multi-kind service or a safe production filter combination.

For the six finalized kinds where the initial stream delivered updates, reopening at a fresh observed slot that lay later than the same run's reported `first_available` produced four `InvalidArgument` and two `Unavailable` errors. There was **zero observed replay overlap**. No provider-wide claim of `from_slot` being permanently unsupported is justified; the empirical result is that this target profile has not demonstrated usable replay. `GetVersion`, `GetSlot` and `SubscribeReplayInfo` success must not be used to upgrade replay status.

The `bank_id_present_updates` counter requires care: for `slot` and `account` probes it checks the actual optional `bank_id` field; for other kinds the current diagnostic labels the kind as having a bank identity field by construction. Therefore the reported `8` for block meta/footer/transaction is **not independent presence proof**. More importantly, even a genuine `bank_id` cannot prove a complete set of transactions or account writes in a bank.

## PV matrix status

| PV | Current evidence | Status for G3 |
| --- | --- | --- |
| PV-01 TLS/auth/version | 7/7 authenticated TLS dial + `GetVersion` PASS; earlier [unauthenticated probe](../2026-11-19-c1/PROVIDER_PROBE.md) received `PermissionDenied`. No token leaked in reports. | **PARTIAL**: auth behavior observed; backend identity/version-affinity/limits not established. |
| PV-02 update kinds and joint filter | Live finalized `slots`, `transactions`, `transactions_status`, `accounts`, `blocks_meta`, `block_footer` observed. `blocks`, `entry`, `combined` failed `Unavailable`; other commitments inconclusive. | **PARTIAL** |
| PV-03 wire/schema/certificates | Locally pinned proto [G1](../2026-11-19-c1/PROTOCOL_COMPAT.md); 8 live footers with typed certificates; own certificate slot may differ. No unknown-field observation or semantic signature verification. | **PARTIAL** |
| PV-04 ReplayInfo | 7/7 `SubscribeReplayInfo` PASS, `first_available` returned. | **PASS_METHOD_ONLY**, retention semantics OPEN |
| PV-05 `from_slot` | Six live-kind reopen attempts: 4 `InvalidArgument`, 2 `Unavailable`; no successful overlap. `processed`/`confirmed` replay not exercised. | **FAIL_FOR_TESTED_OVERLAP / OPEN** |
| PV-06 reconnect/retention | Previous [forced local cancellation](../2026-11-19-c1/DUAL_PUBLICNODE_COVERAGE.md) showed B continued. No spontaneous EOF in five-minute observation; the new retention-edge attempt was inconclusive. | **PARTIAL** |
| PV-07 bank/slot evidence | Live finalized slots/accounts and earlier bounded block-meta parent-chain comparison. No claim of full bank closure, complete filtered transactions or account history. | **PARTIAL** |
| PV-08 real quotas/rates | Repeated sequential `Unavailable` but no identified limit or sustained rate/bytes/SLO profile. | **UNKNOWN** |
| PV-09 independent A/B | Both prior streams use the same PublicNode operator; no second independent Yellowstone credential/endpoint configured. | **BLOCKED_EXTERNAL** |
| PV-10 external data witness | Previous [dual-session audit](../2026-11-19-c1/DUAL_PUBLICNODE_COVERAGE.md): 52,865 Pump.fun signatures equal across two PublicNode sessions and independent finalized Solana RPC signature index on a 350-slot interior; three full blocks matched. No full account/config oracle. | **BOUNDED_TX_PASS / EXACT_OPEN** |

The previous C1 parity evidence and five-minute no-EOF observation remain valid within their individual windows. The new filter probes should not be combined with those windows as if they formed one continuously complete multi-kind capture. No ordered economic effects, full CPI, account/config history, ML-known-at parity or 24-hour soak were assessed.

## Reproduction / retained evidence

Built the existing diagnostic without changing its source:

```bash
GOWORK=off go build -o /tmp/g3_corectl_20261119 ./cmd/corectl
```

Then ran the following bounded batches. The example credential path below is a local ignored `.env`; the script reads only the configured provider keys and never serializes their values:

```bash
python3 -B scripts/g3_probe_matrix.py \
  --corectl /tmp/g3_corectl_20261119 \
  --env-file "$AGGREGATOR_ENV" \

  --out-dir docs/audits/2026-11-19-g3 \
  --core-revision 1d3558e --sample 4s --max-updates 8

python3 -B scripts/g3_probe_matrix.py \
  --corectl /tmp/g3_corectl_20261119 \
  --env-file "$AGGREGATOR_ENV" \

  --out-dir docs/audits/2026-11-19-g3 \
  --core-revision 1d3558e --sample 5s --max-updates 8 \
  --profiles finalized_blocks_meta_isolated,finalized_accounts_isolated,finalized_footer_isolated

python3 -B scripts/g3_probe_matrix.py \
  --corectl /tmp/g3_corectl_20261119 \
  --out-dir docs/audits/2026-11-19-g3 --core-revision 1d3558e \
  --summarize-only
```

Raw **redacted** results: `finalized_kinds.json`, `confirmed_replay.json`, `processed_replay.json`, `finalized_retention.json`, `finalized_blocks_meta_isolated.json`, `finalized_accounts_isolated.json`, `finalized_footer_isolated.json`. `manifest.json` lists per-file SHA-256, UTC time, revision and summarized statuses. These files are short provider-status reports, not private Solana transaction captures. The emitted `request_sha256` pins each exact per-kind subscription request; changing filters/commitment creates a different profile.

## Unresolved requirements / next acceptance run

1. **Independent Yellowstone B:** obtain an actual second provider endpoint and credential with a separately verified operator/upstream/failure domain. Two PublicNode sessions or Cloudflare routes do not satisfy this requirement. No independent B is configured in the present environment.
2. **Replay contract with operator:** ask PublicNode which commitment/update kinds can use `from_slot`, actual filter-specific retention, rate/stream limits and explicit errors. Re-test from a fresh valid slot with one stream per connection after provider guidance. A missing replay path requires explicit coverage break plus independently sourced repair before exact admission.
3. **Combined continuous proof:** run a 10–15-minute, bounded, source-local `finalized` transactions/accounts/block-meta capture, then a longer soak, with controlled A-only/B-only disconnects and external RPC/full-bank/account/config witnesses. Separate partial-bank, stale-baseline and dual-outage outcomes. Current probes are much too short and carry no durable WAL.
4. **PV-08/G5:** measure offered/accepted/persisted rates and bytes, p50/p95/p99, backlog, reconnect/catch-up and resource use under an agreed representative profile; no such live SLO evidence exists here.

**Gate decision:** Keep `CORE-C1-03` **OPEN** and `G3` **PARTIAL / NOT_ACCEPTED**. Do not claim `EXACT`, canonical full-bank history, lossless replay, independent dual-provider coverage or production readiness from these results.


## Independent public JSON-RPC backfill feasibility probe (2026-11-19)

A bounded probe was run against `https://api.mainnet-beta.solana.com` using no credentials. At finalized commitment, `getSlot` returned `471463846`; `getBlocks(471463826, 471463846)` returned all 21 slot numbers in the interval. `getBlock(471463846)` with `transactionDetails=full`, `encoding=json`, `rewards=false` and `maxSupportedTransactionVersion=1` returned blockhash `HKLFac13WAioAm6kwM8ehZK7ynBBVDVfesbiv1Qh3yak`, parent slot `471463845`, and 434 transaction entries. The probe is reproducible with `python3 -B scripts/g3_public_rpc_backfill_probe.py --range-slots 20 --out docs/audits/2026-11-19-g3/public_rpc_probe.json`; the redacted output is retained in `public_rpc_probe.json`.

The first bounded `getBlock` attempt used `maxSupportedTransactionVersion=0` and was rejected because the sampled block contained transaction version 1. Retrying with version 1 succeeded. This is useful compatibility evidence and should remain a regression in any future RPC repair client.

**Interpretation:** `getBlocks/getBlock` is a working bounded transaction/block retrieval primitive through a different public RPC endpoint. It does not prove a second Yellowstone provider, an attested independent upstream/failure domain, sustained range throughput, account write history, or account/config bootstrap. The public endpoint has no production quota/SLA guarantee. Core intentionally does not own this RPC/SQD repair; Aggregator V2 must implement it downstream, validate parent/blockhash continuity, deduplicate transaction signatures/indexes, retain coverage gaps on null/error/inconsistency, and never upgrade raw data to `EXACT` based on this probe alone.

## Provider research update

See [PROVIDER_CANDIDATES.md](PROVIDER_CANDIDATES.md). Alchemy is the strongest documented direct replay candidate, but its official pages disagree on replay depth (6,000 vs 432,000 slots). QuickNode documents `fromSlot` for approximately 3,000 slots. Sodae has an approved 5 GB / 7-day trial but no replay contract in the reviewed docs. No local Alchemy, QuickNode, Sodae, Tatum or Chainstack credential was configured, so no second Yellowstone provider was live-tested. `G3` remains `PARTIAL / NOT_ACCEPTED`.
