## What changes?

Describe the concrete behavior or documentation change, and link the related Issue (if any).

## Why is it needed?

Explain the failure, limitation or use case. Include a short before/after example when useful.

## Validation

List the commands and scenarios actually verified. Do not mark checks as run if they were not run.

- [ ] `go test -count=1 ./...`
- [ ] `go test -race ./...`
- [ ] `go vet ./...`
- [ ] Relevant integration / replay / fixture checks (if applicable)

## Data integrity and compatibility

- [ ] Existing source, commitment, quality and WAL invariants are preserved or the change is explained.
- [ ] New claims about completeness, recovery or determinism are backed by evidence.
- [ ] Protocol and API compatibility were considered.
- [ ] No credentials, sensitive raw WALs or private keys were committed.
- [ ] Documentation and licensing notices were updated when applicable.

## Remaining limitations

List known risks, follow-up work and any acceptance gates that remain unverified.
