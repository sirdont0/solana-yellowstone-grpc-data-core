# CORE-C3-01 — WAL v2 envelope and schema compatibility

**Date:** 2026-11-19. **Disposition:** `LOCALLY_VERIFIED`; overall `G4` remains `PARTIAL`.

## Implemented contract

- Physical segment format remains `format_version=2` (`YLABV2SG`).
- The observation metadata contract has its own `envelope_schema_version=1`.
- Every new v2 segment header and manifest binds the exact deterministic SHA-256 descriptor digests for both pinned protobuf files: Geyser `20146a8e752395fa29404cb7577b1455626aa6f99ddab8d663f56fc380e43f4d`; storage `fb8563bc72daf6d1996ae5b00e5315dece42619b407ab1160f21aa364d110ec5`.
- The binary frame's JSON metadata, segment header and manifest use strict JSON decoding. Unknown envelope fields fail closed. Raw protobuf bytes are not decoded or normalized by the WAL and unknown protobuf wire fields remain byte-preserved.
- Unsupported envelope versions and descriptor mismatches fail before a replay callback receives any record.
- Pre-freeze experimental v2 journals are accepted only when all three new binding fields are absent; they are normalized to the reviewed v1 envelope and the current pinned descriptors. Partial bindings and descriptor drift fail closed. Legacy empty-header recovery retains its original committed header length.
- `.wal-format.json` binds a source DataDir to WAL format, source ID, subscription identity and journal ID. It is checksummed, strict, atomically published (temp file fsync, rename, directory fsync), and prevents a missing journal from silently becoming a new journal in another format. A markerless legacy journal is adopted only after successful replay. Losing the entire DataDir, including its marker, is outside this protection.
- v2-to-v1 downgrade is unsupported. The supported migration is a verified v1-to-v2 copy into a separate DataDir; the original v1 root remains the rollback source.

## Frozen byte layout

All integer fields are little-endian. CRC uses CRC32C/Castagnoli. SHA-256 uses the standard SHA-256 digest. JSON metadata is Go `encoding/json` output for the declared structs; manifest checksum is SHA-256 of `json.Marshal(manifest)` with `Checksum` set to the empty string. `CURRENT` is the ASCII decimal generation followed by LF.

- Segment header: 8-byte `YLABV2SG`; `uint16 format_version`; `uint32 metadata_length`; exact JSON header bytes; `uint32 CRC32C(metadata)`.
- Frame: `uint32 body_length`; `uint32 CRC32C(body)`; body = `uint64 sequence`; `uint32 metadata_length`; JSON `Record` metadata with `Raw` omitted; `uint32 raw_length`; raw protobuf bytes. The body length covers the body only.
- Sealed footer: 8-byte `YLABV2FT`; five `uint64` values (`segment`, `first_sequence`, `last_sequence`, `record_count`, `data_bytes`); 32-byte SHA-256 of the segment prefix `[0:data_bytes]`; `uint32 CRC32C(all preceding footer bytes)`. Footer interpretation is bound to the physical segment version.
- Manifest: versioned JSON containing generation, source/identity/journal IDs, envelope schema and descriptor digests, sealed segment metadata, active committed prefix, and checksum. `CURRENT` selects exactly one manifest; readers do not fall back to older generations.

## Golden fixtures

`internal/ingest/testdata/wal_v2_golden/` contains fixed header, frame, active segment, sealed segment, next active header, manifest and CURRENT bytes. `TestWALV2GoldenFixtures` verifies exact writer bytes and decodes immutable fixtures, including an unknown protobuf wire-field payload. Fixture SHA-256 values are recorded in that directory's README.

Regeneration is deliberately opt-in: `UPDATE_WAL_V2_GOLDEN=1 go test ./internal/ingest -run '^TestWALV2GoldenFixtures$'`. Do not use this to bless an unreviewed format change.

## Verification performed on the final working tree

- Targeted WAL v2/schema/marker/golden tests: PASS.
- `GOWORK=off go test -race -count=1 ./...`: PASS (all eight Go packages).
- `GOWORK=off go test -shuffle=on -count=3 ./...`: PASS (all eight Go packages).
- `GOWORK=off go vet ./...`: PASS.
- `GOWORK=off go build ./...`: PASS.
- `GOOS=linux GOARCH=amd64 GOWORK=off go build ./...`: PASS.
- `GOOS=darwin GOARCH=arm64 GOWORK=off go build ./...`: PASS.
- `GOWORK=off go mod tidy -diff`: PASS (no module changes).
- `corectl help` and a generated-config `capture --dry-run`: PASS.
- `python3 -B -m unittest discover -s scripts/tests -v`: PASS (9 tests).
- Bounded public RPC `getSlot`/`getBlocks`/`getBlock` probe: PASS; evidence in `../2026-11-19-g3/public_rpc_probe.json`.
- `git diff --check`: PASS.

## Remaining G4 work

This closes the envelope/schema and format-marker slice only. It does not close incremental crash-time recovery, authoritative ACK-aware segment retention/compaction, the complete v2 crash/ENOSPC/uncertain-commit matrix, disk budget under long retention, or operational load/SLO. `G4` therefore remains `PARTIAL`; v2 remains opt-in/experimental.
