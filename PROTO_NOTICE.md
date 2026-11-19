# Protobuf origin and licensing

`proto/solanacore-geyser.proto`, `proto/solanacore-storage.proto` and the
generated Go files were originally adapted from an Aggregator Yellowstone
snapshot. The 2026-11-19 protocol refresh was checked against the **immutable**
upstream `rpcpool/yellowstone-grpc` commit
`be9bdc5b53b5b08c31a4da8c6dd7b68c0b2d4e2f` (tag
`v16.1.2+solana.4.3.0`; `yellowstone-grpc-proto` crate version `14.0.1`).
The upstream files at that commit have Git blob IDs:

| Upstream file | Git blob SHA |
| --- | --- |
| `yellowstone-grpc-proto/proto/geyser.proto` | `d0f96616d515c978a07b16e6409c7706f4429a5a` |
| `yellowstone-grpc-proto/proto/solana-storage.proto` | `8731b3a2092698bd341ca5ac31673217db5b2186` |

The downloaded upstream files were diffed against the local `.proto` files.
After the refresh, differences are limited to **intentional namespace changes**
(file/import names, protobuf package names, Go package option and qualified
storage type names) and one **comment clarification**: Go protobuf raw decoders
preserve unknown numeric enum values; a consumer performing certificate
semantic validation must reject unsupported `signature_kind` values. No
upstream wire field numbers or types are intentionally changed. The namespacing
lets the standalone Core coexist with an existing Yellowstone Go client in one
process.

The refreshed footer uses typed `BlockFooterFinalCert`,
`BlockFooterSkipRewardCert`, and `BlockFooterNotarRewardCert` at fields 9/10/11.
Old wincode byte fields 6/7/8 are reserved. Old raw protobuf bytes may still
survive in the unknown-field set, but the public generated Go getters for those
certificate fields have changed **from `[]byte` to message pointers**. Consumers
using the old getter types must update their code; this is an experimental v0
API compatibility change, not a silent source-compatible upgrade.

The Yellowstone gRPC service and method paths on the wire are unchanged:
`/geyser.Geyser/Subscribe`, `/geyser.Geyser/SubscribeReplayInfo`, etc.
The generated gRPC client is patched after regeneration to preserve those
paths. Use `scripts/regenerate-proto.sh` to regenerate it; running plain
`protoc` without that final patch would break live Yellowstone connections.
The source repository's `LICENSING.md` assigns Apache-2.0 to the proto and
client packages. No Geyser plugin/server code is included.

Original project: https://github.com/rpcpool/yellowstone-grpc

The protocol refresh used `protoc 36.2`, `protoc-gen-go v1.36.10` and
`protoc-gen-go-grpc v1.5.1`. The reviewed **local, namespaced** descriptors
have deterministic `FileDescriptorProto` SHA-256 hashes:

| Descriptor | SHA-256 |
| --- | --- |
| `solanacore-geyser.proto` | `20146a8e752395fa29404cb7577b1455626aa6f99ddab8d663f56fc380e43f4d` |
| `solanacore-storage.proto` | `fb8563bc72daf6d1996ae5b00e5315dece42619b407ab1160f21aa364d110ec5` |

`proto/schema_pin_test.go` checks those hashes; `proto/alpenglow_wire_compat_test.go`
checks hand-built old/new wire fixtures, all currently supported raw update
kinds and service paths; `raw_unknown_fields_test.go` checks WAL v1/v2 replay
of typed and legacy footer payloads. Pinned metadata does not prove that a
particular Yellowstone provider supports certificates, `from_slot` or any
requested filter. Live compatibility remains a separate gate in
[`docs/PROVIDER_MATRIX.md`](docs/PROVIDER_MATRIX.md).

Upstream schema can evolve independently; new updates require a new immutable
pin, compatibility tests and review of generated API changes.
The applicable upstream Apache license and attribution are included in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md) and
[`licenses/Apache-2.0.txt`](licenses/Apache-2.0.txt).
