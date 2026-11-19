# CORE-C1-03 — local provider evidence

Date: 2026-11-19. Initial status: **BLOCKED_AUTH** for the unauthenticated PublicNode attempt. A subsequent authenticated attempt verified basic PublicNode RPC/streaming access. Second Yellowstone provider remains **NOT_CONFIGURED**. Keep this report local; do not publish provider-specific evidence with code.

## Actual PublicNode unauthenticated run

Started at 2026-11-19T15:14:53Z, finalized commitment, two filter profiles, no configured token. Endpoint fingerprint SHA-256: 398be466723470bd9448e7ac3ceb38ec2f3698977e233a8a256f0bea3c45cdb7.

| Probe | Observation | Conclusion |
| --- | --- | --- |
| TLS/gRPC dial | PASS | Endpoint reachable |
| GetVersion | PermissionDenied | Cannot identify backend version |
| GetSlot | PermissionDenied | No authenticated slot evidence |
| SubscribeReplayInfo | PermissionDenied | Replay capability UNKNOWN |
| slots filter | PermissionDenied | Filter support UNKNOWN |
| block_footer filter, certificate requested | PermissionDenied | Footer support UNKNOWN |
| from_slot, retention, forced reconnect, overlap | NOT_RUN | Requires auth |
| Independent provider B | NOT_CONFIGURED | No independent endpoint/auth |

## Authenticated PublicNode recheck

The existing local token was found in `sol_agregator/.env` as `YELLOWSTONE_X_TOKEN`, with `YELLOWSTONE_ENDPOINT` and `YELLOWSTONE_AUTH_HEADER`. Its value was not printed or copied to this repository. The file is ignored by the Aggregator's Git configuration. A bounded `corectl probe` used the token from process environment, with `finalized` commitment and three filter profiles.

UTC timestamp: `2026-11-19T15:28:07Z`. The redacted JSON evidence is stored only in ignored `data/provider-probes/publicnode-with-local-token-2026-11-19.json`.

| Operation | Result | Caveat |
| --- | --- | --- |
| TLS dial, GetVersion, GetSlot, SubscribeReplayInfo | **PASS** | GetSlot reported 471385942; ReplayInfo `first_available=471384906` at this instant. The reported retention frontier is not a per-filter replay guarantee. |
| `slots` filter | **PASS**, 8 updates | Reopen from slot 471385949 returned `InvalidArgument`. |
| `blocks_meta` filter | **PASS**, 5 updates | Reopen from slot 471385956 returned `InvalidArgument`. |
| `block_footer` with certificates requested | **PASS**, 6 updates | Reopen from slot 471385964 returned `Unavailable`. Certificate presence and crypto semantics not proven by a short sample. |
| Independent Yellowstone provider B | **NOT_CONFIGURED** | Two sessions at PublicNode do not constitute independent failure domains. |

**Interpretation:** PublicNode credential works, but the observed replay errors prevent acceptance of `from_slot`/overlap for these profiles. Do not label replay `UNSUPPORTED` or `PASS` globally from one request; run a controlled per-commitment/filter/slot matrix with error classification.

## SQD as independent historical/transaction witness

The Aggregator's local plan `docs/plans/AGGREGATOR_V2_MULTI_SOURCE_ROLLOUT_2026-11-11.md` explicitly labels SQD Portal as `TX-ORACLE` and possible `TX-REPAIR` source. It is **not** a second Yellowstone gRPC provider, because the SQD Portal offers a separate HTTP streaming/archival API and different data semantics. On 2026-11-19, the public SQD metadata endpoint returned **HTTP 200**, `dataset=solana-mainnet`, `real_time=true`. A bounded `/finalized-stream` request returned HTTP 529; a separate `/stream` request returned **HTTP 529** with `rate_limit_error/overloaded`. Therefore today's SQD data-stream availability was **NOT VERIFIED**, despite historical project evidence that its transaction oracle worked.

## Implemented local probe

The corectl probe command tests bounded unary and streaming calls, filter classes, optional user-selected historical from_slot, a reopen-overlap check, and an opt-in first_available-1 negative retention probe. It emits redacted JSON and does not prove complete bank or account history.

Run only with local environment-provided provider credentials. A second provider must be verified as an independent failure domain. Never publish tokens, provider-specific private URLs, or unredacted stream contents.

**Gate CORE-C1-03/G3: PARTIAL / NOT_ACCEPTED.** Authenticated PublicNode basic access is now verified; independent Yellowstone B and reliable replay remain open. The earlier protocol gate G1 remains LOCALLY_VERIFIED.
