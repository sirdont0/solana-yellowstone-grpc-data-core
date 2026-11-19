# Dual PublicNode + independent Solana RPC: bounded live coverage test

**Date:** 2026-11-19 UTC
**Core baseline:** local `dev` HEAD `47c529e`, with other pre-existing uncommitted changes preserved.
**Go:** 1.26.11 darwin/arm64.
**Disposition:** **BOUNDED TRANSACTION COVERAGE CORROBORATED; END-TO-END EXACT STATE NOT ACCEPTED.**

The raw run, closed WALs, JSON checks, logs and executable are **local-only** under the ignored directory
`data/provider-probes/dual-publicnode-2026-11-19-run2/`.
No provider credential, private URL, wallet key or raw payload is included in this Markdown report.
Do not commit the ignored capture data.

## Topology and exact scope

- A = `publicnode_a` and B = `publicnode_b`: **two simultaneously active gRPC sessions** to PublicNode Yellowstone, same operator, same advertised hostname and local token, **one failure domain** `publicnode`.
- Both used `finalized` commitment and the same Pump.fun program `account_include` filter:
  `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`.
- Stable WAL v1, separate source-local journals, approximately 80 seconds of capture.
- No account, config, fee/price or PumpSwap data was subscribed in this run.
- Independent witness = `https://api.mainnet-beta.solana.com` over HTTPS JSON-RPC, no authentication token.

A first local run (`run1`) accidentally used a wrong Pump.fun address and observed no transaction updates. That run is **INVALID for data completeness**; the corrected `run2` is the only run used below.

## Runtime observations

| Metric | A | B |
| --- | ---: | ---: |
| Total transaction observations (full capture window) | 54,372 | 54,268 |
| WAL updates (transactions + keepalive) | 54,387 | 54,283 |
| Closed WAL integrity verification | PASS | PASS |
| Transactions inside comparable interior | 52,865 | 52,865 |

Comparable interior: **slots 471402236–471402585 inclusive** (350 slots).
Source A/B equality on this interior:

- Unique signatures: **52,865** in each; same set, **0 A-only, 0 B-only**.
- Slot/transaction-index disagreements: **0**.
- Deterministic protobuf transaction-info digest disagreements: **0**.
- The 104-record difference outside the interior is a capture-boundary/startup/shutdown difference, **not evidence of loss within the interior**.

Two separate authenticated `GetVersion`/slot probes both passed and returned **different version SHA-256 hashes**, but these are unary backend observations. They do **not** establish distinct physical regions or bind the long-lived transaction streams to those backends. Previous local evidence identified distinct PublicNode backend hostnames often ending in `fra`/`fra2`, which suggests Frankfurt rather than independent geographic regions. Cloudflare anycast IPs are not validator identity.

### Forced cancellation / independent session survival

A separate **60-second** finalized `slots + blocks_meta` experiment used the existing
`sol_agregator/cmd/yellowstone-dual-probe`, with session A deliberately canceled at
20 seconds. Its redacted report is in the ignored file `forced_cut_blockmeta.json`:

- `GetVersion` for A reported backend hostname `fra-epyc-022`, B reported `fra-epyc-021` (both `richat 11.0.0`, `proto 14.0.1`). Thus two **different server identifiers**, but both advertise `fra`; a second geographical location is **not** established.
- A observed **88** block-meta blocks before local cancellation; B observed **270** total and **180** after A's cancellation.
- **88** common blocks, **0** conflicting blockhashes, **0** parent mismatches; B showed a valid observed parent chain across the cut.
- Test assessment: **`PASS_LOCAL_CANCEL_ONLY`**. This does not test unexpected EOF, provider-wide outage, `from_slot` replay or account/config completeness.

### Five-minute natural-disconnect observation (no fault injection)

Ran the newly added `-natural` mode of `sol_agregator/cmd/yellowstone-dual-probe`
with `-duration 5m -observe-after 20s`, using two independent Yellowstone gRPC
client connections, `finalized` slots plus block metadata, and **no deliberate
cancel, TCP disruption or network fault** during the observation. Local raw
evidence: ignored file
`data/provider-probes/dual-publicnode-2026-11-19-run2/natural_blockmeta_5m.json`
(permissions `0600`). The 20-second post-disconnect window is enabled only if
a spontaneous failure occurs, and is bounded by the five-minute deadline.

- UTC interval: **2026-11-19 17:04:11.288782–17:09:11.293365**;
  measured wall time **5m0.004583s**; stop reason `duration_elapsed`.
- A backend `epyc-9275f-384gb-fra2`: **1,392** unique block-meta blocks,
  slots **471412696–471414095**, **0** observed parent-chain mismatches.
- B backend `fra-epyc-021`: **1,393** unique block-meta blocks,
  slots **471412694–471414094**, **0** observed parent-chain mismatches.
- **1,391** common blocks with **0** conflicting blockhashes. On the
  comparable interior **471412697–471414093**, neither source had any
  block absent from the other (`A-only=0`, `B-only=0`). Normal skipped Solana
  slots are accounted for by comparing actual parent references, not by
  assuming every slot produces a block.
- Both streams stayed subscribed until the local duration limit and then
  terminated as `local_cancel`. **No natural disconnect was observed.** Both
  connections used the same public endpoint and operator, though their
  advertised backend hostnames differed. This is not geographic/operator
  independence.

**Assessment:** `PASS_NO_NATURAL_DISCONNECT_OBSERVED`. The five-minute
block-metadata streams remained internally continuous and mutually consistent
within the observed comparable window. **Natural-failure survival was not
exercised** because neither stream disconnected. This result must not be
described as a pass for remote EOF, `from_slot` replay, all Pump.fun
transactions/account writes, or provider-wide outages. Earlier finalized
transaction inclusion parity remains a separate 80-second result.

Reproduction command from the `sol_agregator` working directory (choose a
fresh output path; the destination parent must exist):

```bash
GOWORK=off go run ./cmd/yellowstone-dual-probe \
  -natural -duration 5m -observe-after 20s \
  -out /path/to/ignored/probe-report.json
```

## Independent RPC audit

`getSignaturesForAddress(program, commitment=finalized)` paginated **83 pages** to below the compared start slot:

- RPC oracle signatures in the interior: **52,865**.
- Missing from A: **0**; missing from B: **0**; missing from union(A,B): **0**.
- Extra signatures in A/B compared with oracle: **0**.
- This was a complete bounded pagination of the independent RPC **signature index**, not a proof that every account write in Solana was reconstructed.

A separate `getBlocks` + full `getBlock` check found 350 produced blocks in the same interval and deterministically sampled three slots with seed `20261119`:

| Slot | Pump.fun transactions according to full block | A vs block (signature/index) | B vs block |
| ---: | ---: | --- | --- |
| 471402482 | 234 | MATCH, 0 missing/extra/index conflicts | MATCH |
| 471402500 | 126 | MATCH, 0 missing/extra/index conflicts | MATCH |
| 471402548 | 95 | MATCH, 0 missing/extra/index conflicts | MATCH |

The block queries used `transactionDetails=full`, `commitment=finalized`, and `maxSupportedTransactionVersion=1`; loaded ALT addresses were included when interpreting account membership. The remaining blocks have **not** been fully content-checked against `getBlock`.

## What this does and does not prove

**Proven for this exact short profile:** both concurrent PublicNode streams locally persisted identical selected finalized transactions (signature, slot, index, deterministic transaction-info digest) on the 350-slot interior, all of which were corroborated by independent RPC signature pagination; three full blocks additionally verified exact selected transaction membership and positions.

**Not proven:** sustained 10-minute/24-hour completeness; losslessness after forced disconnect, outage or exhausted replay retention; geographic separation; independent operator/failure domain; entire bank/account/config history; exact Pump.fun token state; price, fee or ordered economic effects.

Historical tests in `sol_agregator` already contain counterexamples to broad claims:
- `docs/audits/2026-11-08-v2-continuation/evidence/dual-10m-gaps.json`: 5 parent-chain gaps in a 10-minute run.
- `docs/audits/2026-11-10-p1/evidence/dual-scoped-reconnect-75s.json`: both streams ended with `gapUnrepaired=true` and `replayRejected=true`.
- `yellowstone_ingest_lab/run/multisource/reconnect-5min-2026-11-13-verified-analysis.json`: unrepaired A/B asymmetry after forced reconnect.
- `yellowstone_ingest_lab/run/multisource/pilot-2026-11-13/order-soak-60s-report.json`: observed-data parity, not canonical completeness.

**G3 provider gate remains PARTIAL / NOT_ACCEPTED.** This test supports bounded finalized **TX_INCLUSION** validation; **ACCOUNT_HISTORY**, **CONFIG_HISTORY**, **PRICE**, and complete **ORDERED_EFFECTS** remain UNVERIFIED. Never promote `SOURCE_CORROBORATED` to `CANONICAL_ADMITTED` based on A/B parity alone.

## Third witness and next acceptance profile

The least expensive independent witness is **low-frequency HTTPS JSON-RPC** rather than a third always-on WebSocket stream. A proposed operational profile is two pseudo-random finalized full blocks per 10-minute interval (saved seed) after a 2-minute grace period, plus targeted expansion around A/B conflicts, suspected bank gaps, and dual-source outages. This is **a proposed cadence, not an installed background scheduler**. Its sampling does not prove completeness of unsampled slots.

For repeatable **offline, closed-WAL** checks, use `scripts/audit_dual_publicnode.py`:

```bash
go build -o ./data/corectl ./cmd/corectl
python3 -B scripts/audit_dual_publicnode.py \
  --corectl ./data/corectl \
  --data-dir ./data/provider-probes/dual-publicnode-2026-11-19-run2/wal \
  --source-a publicnode_a --source-b publicnode_b \
  --program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P \
  --sample-blocks 2 \
  --output ./data/provider-probes/dual-publicnode-2026-11-19-run2/reusable_audit.json
```

The utility has no access to Yellowstone credentials. It verifies both WALs before reading, compares A/B signatures, slots, indexes and content digests, pages independent finalized RPC signatures, samples complete blocks, and fails closed to `UNKNOWN` on RPC errors/pending coverage. It does not read an active writer's WAL and is not yet a live observer.

**Historical pagination caveat:** `getSignaturesForAddress` has no direct slot-range seek. A later repeat of the reusable utility hit its **250-page budget** before reaching this older capture, correctly reporting `signature_witness=UNKNOWN`. The amended utility independently completed the sampled full-block comparison (`block_samples=MATCH`) but retained overall `UNKNOWN`, rather than mislabelling the unverified signature interval as complete. The earlier **83-page live-time** signature-index audit did reach the lower boundary and is the bounded signature proof above. Future runs should audit promptly after capturing or use targeted full blocks/SQD archive to avoid long historical pagination.

To use two *different physical PublicNode regions*, the operator must provide region-sticky endpoints or another verified routing mechanism. Merely opening two sockets to the single Cloudflare-facing hostname is **not** evidence of two geographic regions. Both must remain declared with `failure_domain=publicnode`.

**Next gate:** 10–15 minute scoped `finalized` soak with transaction/account/block metadata, independent RPC spot-checks, fault-drop A and B separately, accepted or rejected `from_slot` classified, latency/backlog measured, and explicit unverified state on any closure gap. Keep WAL and checkpoint IDs separate and do not silently fallback to live head.
