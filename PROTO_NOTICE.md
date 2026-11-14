# Protobuf origin and licensing

`proto/solanacore-geyser.proto`, `proto/solanacore-storage.proto` and the
generated Go files were adapted from the existing Aggregator Yellowstone
proto snapshot, which derives from `rpcpool/yellowstone-grpc` (proto 14.x
schema family). The descriptor filenames, protobuf package names and Go
package option are namespaced to avoid collisions when the standalone Core
and existing Aggregator are linked into the same Go process.

The Yellowstone gRPC service and method paths on the wire are unchanged:
`/geyser.Geyser/Subscribe`, `/geyser.Geyser/SubscribeReplayInfo`, etc.
The generated gRPC client is patched after regeneration to preserve those
paths. Use `scripts/regenerate-proto.sh` to regenerate it; running plain
`protoc` without that final patch would break live Yellowstone connections.
The source repository's `LICENSING.md` assigns Apache-2.0 to the proto and
client packages. No Geyser plugin/server code is included.

Original project: https://github.com/rpcpool/yellowstone-grpc

The generated code records protoc-gen-go v1.36.11. The source's upstream
schema may evolve independently; upgrading requires compatibility testing.
The applicable upstream Apache license and attribution are included in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md) and
[`licenses/Apache-2.0.txt`](licenses/Apache-2.0.txt).
