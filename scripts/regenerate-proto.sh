#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

for tool in protoc protoc-gen-go protoc-gen-go-grpc python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing $tool" >&2
    exit 1
  fi
done

protoc -I proto \
  --go_out=proto --go_opt=paths=source_relative \
  --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
  proto/solanacore-storage.proto proto/solanacore-geyser.proto

# Proto file/package names are namespaced so this library and an existing
# Yellowstone Go client can coexist in one process. The server-facing gRPC
# method paths must remain the upstream geyser.Geyser paths.
python3 - <<'PY'
from pathlib import Path

p = Path('proto/solanacore-geyser_grpc.pb.go')
data = p.read_text()
original = data
data = data.replace('"/solanacore_geyser.Geyser/', '"/geyser.Geyser/')
data = data.replace('ServiceName: "solanacore_geyser.Geyser"', 'ServiceName: "geyser.Geyser"')
if data == original:
    raise SystemExit('failed to restore Yellowstone wire service names')
if '"/solanacore_geyser.Geyser/' in data or 'ServiceName: "solanacore_geyser.Geyser"' in data:
    raise SystemExit('left a renamed gRPC service path behind')
p.write_text(data)
PY

gofmt -w proto/*.go
echo "regenerated namespaced Yellowstone protobuf and original gRPC method paths"
