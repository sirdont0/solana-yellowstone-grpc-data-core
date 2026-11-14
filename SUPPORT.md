# Support and troubleshooting

[← Home](README.md) · [🇬🇧 English](docs/README.en.md#quick-start) · [🇷🇺 Русский](docs/README.ru.md#быстрый-старт) · [Security](SECURITY.md)

This is an experimental library maintained on a best-effort basis. There is no production support SLA.

## Before opening an issue

1. Check the [setup guide](docs/README.en.md#quick-start) and confirm that `.env` variables are actually loaded into the shell or process environment.
2. Verify the configured Yellowstone endpoint, token header, provider permissions and commitment support.
3. Run `go test -count=1 ./...` to check the local build, then reproduce the issue with the bundled Pump.fun CreateEvent example when relevant. Unit tests alone do not verify a live provider.
4. Review the [acceptance status](docs/ARCHITECTURE.md#acceptance-status) and [known technical debt](docs/TECH_DEBT.md). A sparse filtered slot sequence is not by itself a gap.

Use the [bug report template](https://github.com/sirdont0/solana-yellowstone-grpc-data-core/issues/new?template=bug_report.yml) for reproducible problems, or the [feature request template](https://github.com/sirdont0/solana-yellowstone-grpc-data-core/issues/new?template=feature_request.yml) for proposed behavior. Include the Go version, Core commit, delivery mode, sanitized logs and minimal reproduction.

For suspected credential exposure or exploitable data-integrity failures, follow [SECURITY.md](SECURITY.md) and report privately.
