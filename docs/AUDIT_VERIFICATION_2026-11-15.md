# Verification results — 2026-11-15

Baseline public commit: `881448f84d466fe5fa33a9ef0a595fb71713c710`.
Candidate: the commit containing this report, branch `codex/core-audit-2026-11-15`.
Environment: Go 1.26.11, darwin/arm64, Apple M4 Pro; local loopback transport.

| Check | Observed result |
| --- | --- |
| `go build ./...` | PASS |
| `GOOS=linux GOARCH=amd64 go build ./...` | PASS (cross-build, not a Linux runtime test) |
| `go test -json -count=1 ./...` | PASS; 124 passing test/subtest entries, 90 top-level entries including the fuzz seed target |
| `go test -race -count=1 -coverprofile=coverage.out ./...` | PASS; Core 87.0%, ingest 75.0%, sourcecfg 84.2%, example 70.6%, CLI 62.1% statement coverage; generated proto excluded from these package-level comparisons |
| `go vet ./...` | PASS |
| `go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...` | PASS |
| `go mod tidy -diff` | PASS; no dependency drift |
| `gofmt -l .` and `git diff --check` | PASS; no output |
| `go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...` | No vulnerabilities found |
| `gitleaks@v8.30.1 git --redact --pre-commit --staged` | No leaks in staged candidate |
| `gitleaks@v8.30.1 git --redact --log-opts=HEAD` before candidate commit | No leaks; two public baseline commits scanned; private archive refs excluded |
| `go test -run '^$' -fuzz=FuzzDecodeRawTransaction -fuzztime=8s -parallel=4 .` | PASS; 332,633 executions, no panic or escaping partial transaction |
| Sibling consumer `go test -race ./...` | PASS after fixing outdated module minimum and missing-metadata fixture; original and namespaced protobuf coexist, RPC paths and wire round-trip preserved |
| Existing Aggregator V2 P0 contract check | PASS; 1,078 rows, 731 legacy fields; contract SPECIFIED, runtime UNVERIFIED |
| GitHub private vulnerability reporting | HTTP 204 enabling; authenticated and independent anonymous GET both returned `enabled:true` |

The synthetic transaction normalization microbenchmark measured approximately
6,673 ns/op, 3,064 B/op, 38 allocations/op (176-byte fixture). It establishes a
repeatable local benchmark only; it is not a production throughput/latency SLO,
WAL bytes/day measurement or W4 load acceptance.

## Reproducible commands

```sh
go build ./...
go test -count=1 ./...
go test -race -count=1 -coverprofile=coverage.out ./...
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
go mod tidy -diff
go test -run '^$' -fuzz=FuzzDecodeRawTransaction -fuzztime=8s -parallel=4 .
go test -run '^$' -bench=BenchmarkDecodeTransaction -benchmem .
```

GitHub Actions on this new commit, published `v0.1.1` resolution through the
public Go proxy and live provider compatibility are not claimed by these local
results. Release publication requires the separately prepared tag workflow.
No production ingest/trading process was started or switched.
