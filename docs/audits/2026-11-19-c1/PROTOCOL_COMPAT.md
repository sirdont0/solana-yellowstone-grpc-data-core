# C1 / G1 — Yellowstone protobuf compatibility evidence

**Date:** 2026-11-19
**Scope:** public `solana-yellowstone-grpc-data-core`, local `dev` worktree.
**Baseline HEAD:** `e3603485881237f1f698f9fc94219ccba2ee1cf2`.
**Evidence level:** `LOCALLY_VERIFIED` — not a real-provider acceptance. Changes in this report are uncommitted at the time of recording; a later release must pin its final commit and rerun gates.

## Upstream provenance and exact comparison

Pinned upstream `rpcpool/yellowstone-grpc` commit:

`be9bdc5b53b5b08c31a4da8c6dd7b68c0b2d4e2f` (`v16.1.2+solana.4.3.0`, `yellowstone-grpc-proto 14.0.1`).

| File | Git blob SHA | Downloaded SHA-256 |
| --- | --- | --- |
| `yellowstone-grpc-proto/proto/geyser.proto` | `d0f96616d515c978a07b16e6409c7706f4429a5a` | `80ee337f361198a0722cb5bdf244fe41838949ab7a3af6d1b182b7fdca9abf02` |
| `yellowstone-grpc-proto/proto/solana-storage.proto` | `8731b3a2092698bd341ca5ac31673217db5b2186` | `e5d16ba9a9e83f6ed8c92a96b6b75dfd64c43f897b976011eb3ec5abe2f3c818` |

Both upstream files were downloaded from immutable raw URLs, SHA-256 hashed and `diff -u` compared with the local `.proto` files. **Only intentional local namespace substitutions and one comment clarification remain:** proto filename/import, package, Go package option and qualified storage names; additionally the local enum comment clarifies that raw Go protobuf accepts unknown numeric enum values and semantic certificate validators must reject unsupported kinds. Reusing upstream `package geyser` without namespacing would conflict with the existing Aggregator client; generated gRPC service paths remain upstream `/geyser.Geyser/*` through `scripts/regenerate-proto.sh`.

Local schema file SHA-256 after refresh:

| File | SHA-256 |
| --- | --- |
| `proto/solanacore-geyser.proto` | `d683c2acf4ba623abbdc169f0adc377e44f2b9f5712296e0f0d0d13093458960` |
| `proto/solanacore-storage.proto` | `956c5d9367d409bd7081bdb63e0e7d0c709424aa610cbbde87d1ed72f10ed4c3` |

Local namespaced `FileDescriptorProto` SHA-256 pins (deterministic encoding) are checked in `proto/schema_pin_test.go`:

- geyser: `20146a8e752395fa29404cb7577b1455626aa6f99ddab8d663f56fc380e43f4d`
- storage: `fb8563bc72daf6d1996ae5b00e5315dece42619b407ab1160f21aa364d110ec5`

## Change and compatibility contract

`SubscribeUpdateBlockFooter` now uses upstream typed `BlockFooterFinalCert`, `BlockFooterSkipRewardCert`, `BlockFooterNotarRewardCert` in fields 9/10/11; 6/7/8 are reserved. Added `BlockFooterVotesAggregate` and `BlockFooterSignatureKind`. Type-level API getters changed from `[]byte` to message pointers. This is an intentional experimental-v0 source-level change and requires consumers that referenced the old getters to update. Old 6/7/8 byte data remains protobuf unknown fields when decoded by the new schema and survives raw WAL replay; it is **not** automatically decoded into new certificate types. Unknown numeric `signature_kind` and incomplete aggregates are retained by raw decoding, not semantically accepted as valid certificates.

The certificate's own `slot` may differ from the carrier footer's slot. No cryptographic certificate signature validation was added to Core. Missing optional certificates are valid protocol observations. A footer is not a barrier/proof of full bank inclusion.

Generator versions: `protoc 36.2`, `protoc-gen-go v1.36.10`, `protoc-gen-go-grpc v1.5.1`. The gRPC generated wire path was checked to remain `/geyser.Geyser/Subscribe` and `/geyser.Geyser/SubscribeReplayInfo`.

## Tests and observed results

| Gate/check | Actual result | Meaning |
| --- | --- | --- |
| Old schema regression before patch | **Expected FAIL** | Test compilation failed because certificate getters returned `[]byte`, not typed messages. |
| `GOWORK=off go test -count=1 ./proto -v` | **PASS** | Typed 9/10/11, legacy 6/7/8 unknown, optional absence, differing certificate slot, unknown signature kind/incomplete aggregate, descriptor pins, 11 structural `SubscribeUpdate` oneof kinds, wire paths. |
| `GOWORK=off go test -count=1 ./ -run TestRawWALPreserves -v` | **PASS** | Existing unknown field test and new typed/legacy footer replay through WAL v1 and v2. |
| `GOWORK=off go test -count=1 ./...` | **PASS** | Uncached complete Go test suite on the local worktree. |
| `GOWORK=off go test -race -count=1 ./...` | **PASS** | Complete local suite with race detector. |
| `GOWORK=off go vet ./...` | **PASS** | Static vet check. |
| `corectl capture --dry-run` with a valid public program and redacted PublicNode endpoint | **PASS** | Local configuration parsing only; no provider connection. |
| Live PublicNode / independent provider capability matrix PV-01…PV-10 | **NOT_RUN** | No claim about remote filter/replay/cert support. |
| Live certificate signature semantics or consensus verification | **NOT_RUN** | Raw protobuf decode is not cryptographic validation. |
| Production load/soak and full crash matrix | **NOT_RUN** | Not addressed by this protocol-only change. |

## Reproduction commands

From repository root:

```bash
bash scripts/regenerate-proto.sh
GOWORK=off go test -count=1 ./proto -v
GOWORK=off go test -count=1 ./...
GOWORK=off go test -race -count=1 ./...
GOWORK=off go vet ./...
git diff --check
```

For source-level upstream verification, fetch the two proto files at the immutable SHA above and compare them to local files. Only namespace transforms and the semantic-validation comment clarification documented in [`PROTO_NOTICE.md`](../../../PROTO_NOTICE.md) are expected. Do not replace namespaced files by running `protoc` directly on upstream files; use the repository regeneration script.

## Remaining gates

This closes the **local, structural protocol compatibility** portion of `CORE-C1-01` and `CORE-C1-02`. `G1` may be recorded as `LOCALLY_VERIFIED` only; it does not establish provider or semantic certificate compatibility. `CORE-C1-03` / `G3` remains `NOT_RUN` until actual authorized endpoint and credential profiles are tested. Partial-bank/replay diagnostics (`CORE-C2-04`), exact canonical bank/account admission, WAL v2 retention and operational SLOs remain separate work.
