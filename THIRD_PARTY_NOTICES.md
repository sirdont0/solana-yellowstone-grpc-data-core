# Third-party notices

This repository is MIT-licensed for its original source code and documentation
(see [LICENSE](LICENSE)). Files derived from the Yellowstone gRPC protobuf
project are licensed separately as described below.

## Yellowstone gRPC protobuf schemas and generated bindings

- **Upstream:** [rpcpool/yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc)
- **Upstream paths:** `yellowstone-grpc-proto/proto/geyser.proto` and
  `yellowstone-grpc-proto/proto/solana-storage.proto`
- **License:** Apache License, Version 2.0; see
  [licenses/Apache-2.0.txt](licenses/Apache-2.0.txt)
- **Upstream licensing declaration:**
  [LICENSING.md](https://github.com/rpcpool/yellowstone-grpc/blob/master/LICENSING.md),
  which explicitly licenses the `yellowstone-grpc-proto` subtree under
  Apache-2.0 (the upstream repository has an AGPL-3.0-only default for other
  components).
- **Derived files included here:** `proto/solanacore-geyser.proto`,
  `proto/solanacore-storage.proto`, and their generated `.pb.go` / `_grpc.pb.go`
  files.
- **Changes:** Namespaced protobuf file and package names and Go import path
  to coexist with other Yellowstone clients in one Go process; the generated
  gRPC service and method paths were preserved as `/geyser.Geyser/*` for wire
  compatibility. The original schemas were imported via an Aggregator
  Yellowstone proto snapshot, not as a literal current-upstream checkout.

Copyright and attribution for the original work remain with the respective
Yellowstone gRPC contributors. No Geyser server/plugin implementation or
upstream AGPL-only directory is included in this repository.

## Other Go dependencies

The package uses [grpc-go](https://github.com/grpc/grpc-go),
[protobuf-go](https://github.com/protocolbuffers/protobuf-go), and
[base58](https://github.com/mr-tron/base58) as external Go modules. Their
licensing and notices are maintained in their respective upstream projects.
