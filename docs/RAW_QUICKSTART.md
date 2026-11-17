# Universal Yellowstone raw capture: quick start

This path captures **source-observed Yellowstone updates** without a Pump.fun
decoder, trading handler, or application-specific token model. It uses
`core.NewRaw`, preserving the protobuf payload after gRPC decoding alongside
source identity, slot, receive time, transaction/account metadata and lifecycle
markers. It does **not** certify full bank/account completeness or canonical
cross-provider order.

Requirements: Go 1.26.11+, a Yellowstone gRPC endpoint, and a provider token if
the endpoint requires one. The default PublicNode endpoint requires the user's
own token; no credential is included in this repository.

## Capture one program

From the repository root:

```bash
export YELLOWSTONE_ENDPOINT='your-yellowstone-host:443'
export YELLOWSTONE_TOKEN='your-provider-token'
export PROGRAM_ID='a-valid-Solana-program-public-key'

go run ./cmd/corectl capture \
  --endpoint "$YELLOWSTONE_ENDPOINT" \
  --program "$PROGRAM_ID" \
  --source-id primary \
  --data-dir ./run
```

`--program` is repeatable. It constructs an explicit Yellowstone transaction
`account_include` filter; the program IDs are your choice. `vote` and `failed`
remain *unset* (tri-state), so the CLI does not silently exclude matching
transactions in those classes. It neither adds nor requires a Pump.fun filter.
For slot, account, block and other Yellowstone filters, use a subscription JSON
file instead.

The CLI defaults to stable `YLABWAL1` (`journal.wal`) and enables disk-backed
deduplication. WAL v2 (`--wal-version 2`) is experimental, with outstanding
acceptance gates. Stop capture with Ctrl+C or SIGTERM; source writers close
gracefully if the process can drain within its shutdown limit.

## Capture arbitrary update types

Create `subscription.json` using Yellowstone `SubscribeRequest` protobuf JSON:

```json
{
  "transactions": {
    "selected_transactions": {
      "accountInclude": ["11111111111111111111111111111111"]
    }
  },
  "slots": {"slot_updates": {}},
  "blocksMeta": {"block_metadata": {}}
}
```

The example uses the Solana System Program only as a valid sample public key.
Change the filters to fit the desired data. The provider may reject certain
filters or impose data-rate and replay limits. Core owns commitment, replay
cursor (`from_slot`) and ping fields; omit those from subscription JSON.

```bash
go run ./cmd/corectl capture \
  --endpoint "$YELLOWSTONE_ENDPOINT" \
  --subscription ./subscription.json \
  --filter-version my-subscription-v1 \
  --data-dir ./run
```

The subscription can include `transactionsStatus`, `accounts`, `blocks`,
`entry`, `blockFooter`, and other fields supported by the current generated
protobuf. Unknown JSON fields are rejected. Use `--dry-run` to validate local
settings without connecting.

## Multiple independent source sessions

Create a non-secret config file, for example `rawcore.json`:

```json
{
  "data_dir": "./run",
  "wal_version": 1,
  "dedupe_disk": true,
  "sources": [
    {
      "id": "primary",
      "endpoint": "provider-a.example:443",
      "token_env": "YELLOWSTONE_TOKEN_A",
      "token_header": "x-token",
      "commitment": "finalized",
      "filter_version": "raw-v1",
      "subscription_file": "subscription.json",
      "failure_domain": "provider-a"
    },
    {
      "id": "secondary",
      "endpoint": "provider-b.example:443",
      "token_env": "YELLOWSTONE_TOKEN_B",
      "token_header": "x-token",
      "commitment": "finalized",
      "filter_version": "raw-v1",
      "subscription_file": "subscription.json",
      "failure_domain": "provider-b"
    }
  ]
}
```

`subscription_file` paths are relative to the config file's directory.
`data_dir` is resolved relative to the process working directory. A source may
use a `programs` array instead of `subscription_file`. Each source gets its own
WAL and reconnect loop. Source IDs must remain stable; credentials stay in
environment variables. Multiple connections to the same backend do not prove
independent observation completeness.

```bash
export YELLOWSTONE_TOKEN_A='provider-a-token'
export YELLOWSTONE_TOKEN_B='provider-b-token'
go run ./cmd/corectl capture --config ./rawcore.json --dry-run
go run ./cmd/corectl capture --config ./rawcore.json
```

## Inspect, verify, replay

Once capture has stopped, use the offline commands. The reader refuses a WAL
with an active writer.

```bash
go run ./cmd/corectl inspect --data-dir ./run --source primary
go run ./cmd/corectl verify --data-dir ./run --source primary
go run ./cmd/corectl replay --data-dir ./run --source primary > raw.ndjson
go run ./cmd/corectl replay --data-dir ./run --source primary \
  --after 1000 --kind transaction --limit 10 --omit-payload
```

`verify` checks WAL frame checksums, contiguous source-local sequence,
subscription identity consistency, and whether raw updates decode as the
expected Yellowstone protobuf variant. It does not compare against Solana RPC,
assert a complete bank, or verify all transaction/account semantics. `inspect`
returns a JSON summary; `replay` emits original WAL record envelopes as NDJSON
in **one source's append order**, including base64 `raw` protobuf payloads by
default. `--omit-payload` produces lighter diagnostic output but loses the
protobuf data. Replay preflights the complete WAL before emitting data.

Use `--journal /path/to/journal.wal` or `--journal /path/to/journal.v2` for an
explicit format. If both versions exist under a source directory, choose one
explicitly. `inspect` and `replay` allow `--tolerant-tail` for a valid prefix
of an incomplete v1 tail; `verify` always requires a clean journal.

## Consume with durable cursors

The complete runnable public-API example is in
[`examples/rawconsumer/main.go`](../examples/rawconsumer/main.go):

```bash
go run ./examples/rawconsumer \
  --endpoint "$YELLOWSTONE_ENDPOINT" \
  --program "$PROGRAM_ID" \
  --consumer example \
  --data-dir ./consumer-run \
  --output-db ./observations.db
```

It registers the consumer and opens a `RawReader` from its durable cursor.
With `--output-db`, one bbolt transaction stores the observation under its
source/identity/journal/sequence key and increments an example application
counter. Only after that durable transaction succeeds does it call Core's
`Ack`. A crash between the application commit and `Ack` causes redelivery;
the already committed key prevents a second application effect. A changed
payload at the same key is an error. Consumer re-registration changes the
ACK epoch, but does not change this application idempotency key.

Replace the counter in `examples/rawconsumer/database.go` with effects stored
in the same application transaction. Core ACK is a separate commit, not a
transaction shared with bbolt. Remote calls and effects outside that transaction
require their own idempotency protocol. The test covers reopening after a
commit without ACK, repeated delivery, conflicting payload rollback and a new
journal using the same sequence number.

Without `--output-db`, `--output ./observations.ndjson` selects the simpler
append-and-fsync NDJSON example. It permits duplicate lines and can leave a
partial final line after an abrupt write interruption; it is not an idempotent
application database. When `--output-db` is set, no NDJSON output is written.

Changing endpoint, subscription, source ID or filter version changes the
subscription identity. Existing WALs and consumer cursors must not be silently
reused under a different identity. Preserve old WALs; start a new source/data
directory for the new subscription.

`corectl migrate` without a destination performs read-only preflight. Supply
`--destination-data-dir ./converted-run` to create a **verified v2 copy** in a
new source directory; the old WAL/ACKs stay unchanged and the new journal ID
invalidates old cursors. This requires explicit consumer re-registration and
application-side idempotency. Never simply flip `--wal-version` in an existing
data directory. The v2 runtime is still experimental. See
[`WAL_V2_CONTRACT.md`](WAL_V2_CONTRACT.md) and [`CLI.md`](CLI.md).
