# Security Policy

[← Home](README.md) · [🇬🇧 English](SECURITY.md) · [🇷🇺 Русский](docs/SECURITY.ru.md) · [Architecture](docs/ARCHITECTURE.md)

## Supported status

| Version | Status |
| :--- | :--- |
| `main` / experimental v0 | Maintainer review on a best-effort basis |
| Older commits or forks | No separate support commitment |

There is no production-security or response-time SLA. The project has not yet passed the [canonical completeness and recovery gates](docs/TECH_DEBT.md).

## Reporting a vulnerability

**Do not disclose a security issue, provider credential or exploit in a regular GitHub Issue or PR.**

Use GitHub's **private vulnerability reporting / security advisory** workflow for this repository if it is enabled, or contact the maintainer through an existing private channel. Include a concise impact description, affected commit/version, minimal reproduction and any suggested mitigation. Share sensitive fixtures privately and only when necessary.

For ordinary non-sensitive correctness bugs, use the [bug report form](https://github.com/sirdont0/solana-yellowstone-grpc-data-core/issues/new?template=bug_report.yml).

## Security-sensitive surfaces

- **Credentials and transport:** Provider tokens belong in the environment or a secret manager. The built-in dialer rejects plaintext gRPC when a token is configured. Custom dialers must provide equivalent TLS protection. Never paste tokens into logs or issues.
- **Raw WAL:** Saved updates may contain transaction and account data. Control filesystem access, retention, backup and deletion according to the application that collects them. CRC detects accidental corruption; it is **not** cryptographic authenticity.
- **Data integrity:** Source-local observations, reconnect, de-duplication and replay cannot certify complete Solana banks or account history. Do not promote `OBSERVING_UNVERIFIED` or `CONTINUITY_BROKEN` to exact state.
- **Handlers:** Treat provider data and decoded instruction payloads as untrusted inputs. Bound resource usage; reject malformed and unsupported layouts; avoid logging secrets.

If you accidentally expose a provider credential, revoke or rotate it with the provider immediately. Removing a secret from the latest commit does not remove it from Git history.

## Disclosure process

Maintainers will assess impact and coordinate a fix or mitigation as capacity allows. Please allow time for private review before public disclosure. No bug bounty program is advertised.
