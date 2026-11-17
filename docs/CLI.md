# `corectl` command reference

`go run ./cmd/corectl help` prints command names. Each subcommand accepts
`-h`. All commands are in the module's `cmd/corectl` directory and can be
built with `go build -o corectl ./cmd/corectl`.

## `capture`

Capture runs `core.NewRaw(config).Run(ctx)` and stores source-local Yellowstone
observations. It does not require protocol handlers or a Pump.fun program.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--endpoint` | `YELLOWSTONE_ENDPOINT` | Yellowstone `host:port` or gRPC(S) endpoint |
| `--source-id` | `primary` | Stable source ID |
| `--token-env` | `YELLOWSTONE_TOKEN` | Name of environment variable containing provider token |
| `--token-header` | `x-token` | Provider credential header name |
| `--program` | none | Repeatable transaction `account_include` program key |
| `--subscription` | none | Custom `SubscribeRequest` protobuf JSON file |
| `--commitment` | `finalized` | `processed`, `confirmed`, `finalized` |
| `--filter-version` | `raw-v1` | User-controlled subscription revision |
| `--data-dir` | `./run` | Root for per-source WAL directories |
| `--wal-version` | `1` | `1` stable, `2` experimental segmented WAL |
| `--wal-segment-mib` | `256` | WAL v2 maximum segment size |
| `--dedupe-disk` | `true` | Exact on-disk source-local dedupe index |
| `--max-message-mib` | `64` | Maximum Yellowstone update size (1..95 MiB) |
| `--queue-mib` | `256` | Shared dispatch queue byte budget |
| `--source-queue-mib` | `128` | Per-source pre-WAL receive byte budget |
| `--batch-mib` | `16` | Maximum fsync batch byte budget |
| `--max-reconnects` | `0` | Zero retries indefinitely |
| `--replay-overlap` | `64` | Overlap on reconnect, in slots |
| `--allow-live-fallback-on-replay-rejection` | `false` | Opt in to live continuation when provider rejects replay; uncertainty remains explicit |
| `--from-slot` | unset | Optional initial Yellowstone replay slot |
| `--dry-run` | `false` | Validate and print redacted settings without connecting |
| `--config` | unset | Non-secret multi-source JSON config; standalone source flags are then unavailable |

Standalone mode requires exactly one of `--program` (one or more) and
`--subscription`. With a JSON config, every `sources[]` entry likewise requires
`programs` or `subscription_file`. The config supports 1..16 sources and these
top-level settings: `data_dir`, `wal_version`, `wal_segment_mib`, `dedupe_disk`,
`max_message_mib`, `queue_mib`, `source_queue_mib`, `batch_mib`,
`max_reconnects`, `replay_overlap`, `from_slot`, and
`allow_live_fallback_on_replay_rejection`. Each `sources[]` entry supports
`id`, `endpoint`, `token_env`, `token_header`, `commitment`, `filter_version`,
`programs`, `subscription_file`, `failure_domain`. Unknown JSON fields fail
closed. Tokens are read from the named environment variables, never from the
config file.

The `--program` shortcut builds an explicit protobuf transaction filter with
`accountInclude`; its `vote` and `failed` pointers are unset. It does not
enable an implicit Pump.fun filter, and providers may impose their own filter
limits. `--subscription` can select any generated Yellowstone filter class.

For custom subscriptions, reconnect replay uses the minimum observed slot
frontier across every requested update class. If one class has not produced an
observed slot, the Core does not infer its position from another class; it
uses only an explicit initial `from_slot` or starts live from the provider
head, while preserving the continuity-break marker. Provider replay retention
and historical completeness are endpoint-specific; a successful reconnect
does not certify complete history.

## `inspect`, `verify`, `replay`

All offline commands use a closed WAL, whose path is resolved from
`--data-dir` plus `--source`, or directly from `--journal`. V1 uses
`source/journal.wal`; V2 uses `source/journal.v2`. The reader rejects active
writers and corrupt frames.

Common flags:

| Flag | Meaning |
| --- | --- |
| `--journal PATH` | Explicit v1 WAL file or v2 WAL directory |
| `--data-dir DIR` | Root containing source subdirectories (default `./run`) |
| `--source ID` | Expected source ID; required without `--journal` |
| `--identity HASH` | Require a specific subscription identity |
| `--tolerant-tail` | For inspect/replay only, accept a valid prefix before an incomplete v1 tail |

`inspect` prints a JSON summary including record/update/marker counts, kinds,
source identity, journal ID, observed slot range, valid bytes and whether a
truncated tail was encountered.

`verify` performs strict full-WAL checks for CRC, source-local sequence,
noncontradictory identity/commitment/filter metadata, valid Yellowstone
protobuf payloads and agreement between envelope kind and protobuf variant.
It cannot certify complete canonical history. The command exits nonzero on
failure and does not modify the WAL.

`replay` first performs the same full scan, then emits WAL `Record` envelopes
as NDJSON. Its extra flags are `--after N` (exclusive source-local sequence),
`--limit N` (zero means unlimited), `--kind NAME`, and `--omit-payload`. Record
`raw` bytes are base64-encoded by JSON. Replay is source-local; it does not
reconstruct cross-source arrival order or decode transactions/CPI. The
`--after` flag is a numeric sequence within the selected WAL, so callers must
also retain the source, identity and journal ID when persisting positions.

## `migrate`

Without `--destination-data-dir`, `migrate` performs a strict read-only v1 WAL
preflight and prints `preflight_ok`. With `--destination-data-dir`, it creates
a **new** source directory under that separate data root. The source WAL is
held under a shared reader lock, every source frame is validated, records are
copied in source-local order into v2, and the new archive is read back and
compared by record count and SHA-256 content digest before a final directory
rename publishes it. The original WAL and its consumer ACKs remain untouched.

The new v2 journal deliberately has a **new journal ID**. Existing ACK cursors
therefore do not transfer: consumers must explicitly register in the new data
root and handle possible re-delivery in their own idempotent storage. The
destination source directory must not exist, and conversion never switches an
existing writer's format in place. This migration path is implemented and
locally regression-tested; v2 is still experimental until its full fault and
load acceptance gates pass.

Migration also fails closed if a v1 JSON record contains an unknown envelope
field: the current `Record` schema cannot guarantee preservation of that field
in v2. No destination is published, and the source WAL remains unchanged.

Additional migration flags are `--destination-data-dir DIR` and
`--migration-segment-mib N` (default 256, range 2..4096). `--source` is
required for an actual conversion. `--identity` can require an exact known
subscription identity, and the WAL itself must supply a consistent identity
or the caller must provide one.

Example:

```bash
go run ./cmd/corectl migrate --data-dir ./run --source primary
go run ./cmd/corectl migrate --data-dir ./run --source primary \
  --destination-data-dir ./converted-run --migration-segment-mib 256
```

For application integration, prefer the public `core.NewRaw`,
`OpenRawReader`, `RegisterConsumer`, `Ack`, and `ReadRawArchive` APIs. The
package-level `ReadRawArchive` reads a closed v1/v2 WAL without starting an
Engine or a network connection. The CLI's strict verifier also checks protobuf
kind/identity consistency. See [`RAW_QUICKSTART.md`](RAW_QUICKSTART.md).
