# CORE-C2-01: ACK cursor atomic-write fault boundaries

**Date:** 2026-11-19. **Result:** local race regression PASS; integrated G2 status is recorded in [INTEGRATION_REPORT](INTEGRATION_REPORT.md).

Test: `TestC2CursorAtomicFaultBoundaries` in `c2_cursor_atomic_fault_test.go`.
The production `writeCursorAtomic` path is unchanged; an internal
`writeCursorAtomicWithHooks` helper permits deterministic errors at exact
temporary-file fsync, rename, and directory fsync boundaries.

| Injected failure | Returned error | Durable path after call |
| --- | --- | --- |
| Temporary cursor file fsync | injected error | Previous cursor remains |
| Rename | injected error | Previous cursor remains |
| Directory fsync after rename | injected error | New cursor is already visible; commit outcome is ambiguous under power loss |

All cases assert that no temporary cursor file remains. This verifies
fail-closed error propagation and identifies the post-rename uncertainty
that callers must handle with application-side idempotence and retry/readback.
It does **not** simulate sudden power loss in these windows.

Validation:

    GOWORK=off go test -race -count=1 -timeout=45s -run '^TestC2CursorAtomicFaultBoundaries$' .

Exit status 0.

The same commit boundaries were also exercised with real child-process
termination rather than returned errors. See
[CURSOR_PROCESS_CRASH](CURSOR_PROCESS_CRASH.md): four SIGKILL points cover
after temp-file fsync, before rename, after rename/before directory fsync, and
after directory fsync. The post-rename/pre-directory-fsync state is visible
after process death but remains ambiguous under power loss.
