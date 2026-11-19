# Independent Yellowstone provider candidates and recovery options

**Research date:** 2026-11-19. This is documentation research, not live endpoint acceptance. No second-provider API key was found in the local environment, so no provider credentials were used and no second Yellowstone endpoint was tested.

## Shortlist

| Provider | Yellowstone interface / auth | Replay evidence | Cost/access evidence | Assessment for this project |
|---|---|---|---|---|
| **Alchemy** | `solana-mainnet.streaming.alchemy.com`, Yellowstone-compatible, `X-Token` | Official Historical Replay guide documents `SubscribeRequest.from_slot` and replay-to-live transition. It says 432,000 slots (~48h), while the Overview page says 6,000 slots. Treat the effective window as **UNKNOWN** until the provider reconciles the docs and a live probe confirms it. | $75/TB streamed; Yellowstone gRPC is not on the Free plan. PAYG/Enterprise access is documented. | Best documented direct replay candidate, but not free and needs a credential plus live per-filter tests.
| **QuickNode** | Yellowstone-compatible Geyser gRPC; endpoint-specific `x-token` | `fromSlot` documented in the Pump.fun monitor guide; up to 3,000 slots (~11 min). `subscribeReplayInfo` docs say about 11 minutes and return `firstAvailable`. | Scale or Business plan required. | Strong alternative for a short-gap replay test; paid and short retention window.
| **Sodae** | `ams.tls.sodae.io:443`, service `geyser.Geyser`, `x-token`; docs list accounts, transactions, slots, blocks, block metadata and entries. | Public docs reviewed do not document `from_slot` or `SubscribeReplayInfo`; capability is UNKNOWN, not unsupported. | 5 GB / 7-day gRPC trial, no card, approval required. The reviewed pricing page lists a 5 GB/7-day approved trial and $0.05/GB data packs from 200 GB; the fixed seat price should be confirmed in the dashboard quote before purchase. | Best low-cost trial candidate for an independent live B stream, but it cannot yet be selected as the replay repair path. Pair with a separately tested RPC/archive backfill.
| **Tatum** | `solana-mainnet-grpc.gateway.tatum.io`, Yellowstone gRPC, `x-token`; official docs show transaction/account/slot/block subscriptions. | No `from_slot` or replay-window contract found in reviewed docs. | Free Solana API key advertises 100k RPC credits; this does **not** establish free gRPC streaming bandwidth. | Possible B stream candidate; replay and actual gRPC allowance need direct confirmation.
| **Chainstack** | Yellowstone Geyser gRPC for Solana. | No replay contract verified in reviewed docs. | Blog lists paid Growth-tier availability; historical article lists $149/month for 7 streams and $449/month for 25 streams. Verify current quote before purchase. | Technically viable stream candidate, but less attractive than Alchemy/QuickNode for replay.

## Recommendation

1. For a direct Yellowstone replay test, request an Alchemy PAYG key first. Before relying on its replay horizon, get written clarification of the 6,000-vs-432,000-slot documentation conflict. Test `from_slot` on the exact finalized Pump.fun transaction filter and required account/block-meta filters, then force a disconnect and verify overlap and catch-up.
2. For a low-cost independent live B stream, Sodae's 5 GB / 7-day approved trial is the most accessible documented trial. It is not a persistent free endpoint and no replay support is documented.
3. QuickNode is the clearest second choice if an approximately 11-minute replay window is enough and a Scale/Business plan is acceptable.
4. No always-free, permanent, public Yellowstone gRPC endpoint was verified in this research. The free offers found are trials or ordinary RPC credits, not a proven perpetual gRPC stream allowance.

## Independent RPC backfill evidence

A bounded live probe against the public Solana JSON-RPC endpoint `api.mainnet-beta.solana.com` succeeded at `finalized`: `getBlocks` returned all 21 slot numbers for `[471463826, 471463846]`; `getBlock` for slot `471463846` returned blockhash `HKLFac13WAioAm6kwM8ehZK7ynBBVDVfesbiv1Qh3yak`, parent slot `471463845`, and 434 full transaction entries. `maxSupportedTransactionVersion` had to be set to `1`; the first request using `0` was correctly rejected because the sampled block contained v1 transactions. The exact probe is recorded in `REPORT.md` and `public_rpc_probe.json`.

This proves a bounded transaction/block retrieval path through a different public RPC endpoint, **not** a second Yellowstone provider and not full account/config write-history recovery. The public endpoint's underlying infrastructure independence from PublicNode is not attested, its public quotas are not an operational guarantee, and a one-shot probe is not a sustained backfill acceptance test.

For the Aggregator V2 consumer, the safe repair design is: persist the last finalized slot; on a detected gap, query `getBlocks` over the missing bounded range; fetch each returned slot with `getBlock` using full transactions and the supported transaction-version floor; validate parent/blockhash continuity; derive transaction inclusion and effects; dedupe by signature plus transaction index; compare overlapping anchors against Yellowstone; and keep the range `PARTIAL/UNKNOWN` if any block is missing, null, pruned or inconsistent. Account/config history still requires an explicit state witness or account snapshot strategy. This repair belongs downstream of this Core library, as required by `PROJECT_SCOPE.md`; it must not silently upgrade raw observations to `EXACT`.

## Official sources

- Alchemy [Quickstart](https://www.alchemy.com/docs/reference/yellowstone-grpc-quickstart), [Historical Replay](https://www.alchemy.com/docs/reference/yellowstone-grpc-historical-replay), [Overview](https://www.alchemy.com/docs/reference/yellowstone-grpc-overview), [pricing](https://support.alchemy.com/articles/5952597732-how-is-solana-grpc-yellowstone-streaming-priced).
- QuickNode [Solana gRPC overview](https://www.quicknode.com/docs/solana/solana-grpc/overview), [`subscribeReplayInfo`](https://www.quicknode.com/docs/solana/solana-grpc/subscribeReplayInfo), [Pump.fun monitor guide and fromSlot replay](https://www.quicknode.com/guides/solana-development/tooling/solana-grpc/solana-grpc).
- Sodae [Yellowstone docs](https://docs.sodae.io/yellowstone/overview), [quickstart](https://docs.sodae.io/quickstart), [pricing](https://sodae.io/pricing).
- Tatum [Solana gRPC reference](https://docs.tatum.io/reference/solana-grpc), [Solana plan overview](https://tatum.io/chain/solana), [pricing](https://tatum.io/pricing).
- Chainstack [Yellowstone gRPC marketplace listing and current stream prices](https://chainstack.com/marketplace/services/yellowstone-grpc-geyser-plugin/).
