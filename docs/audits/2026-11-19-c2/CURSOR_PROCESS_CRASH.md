# CORE-C2-01: cursor atomic-write process-crash boundaries

**Date:** 2026-11-19. **Result:** child-process SIGKILL boundary test added.

Test: `TestC2CursorProcessCrashBoundaries` in
`c2_cursor_process_crash_test.go`. Each case starts a child copy of the Go test
binary, pauses it from a `writeCursorAtomicWithHooks` callback, kills it with
`Process.Kill`, waits for process termination, and then inspects the cursor and
temporary files from the parent process.

| Kill point | Canonical cursor visible after SIGKILL | Temporary file |
| --- | --- | --- |
| After temporary-file `Sync` returned, before close/rename | Previous cursor | New cursor remains in `.cursor-*.tmp` |
| Immediately before `os.Rename` | Previous cursor | New cursor remains in `.cursor-*.tmp` |
| After `os.Rename` returned, before directory fsync | New cursor | No temporary file |
| After directory fsync returned, before writer return | New cursor | No temporary file |

The orphan temporary files in the first two cases are expected: `SIGKILL`
prevents the writer's deferred `os.Remove` from running. The test verifies the
temporary file contains the complete new cursor and that the canonical path
still contains the old cursor. At every stage, the canonical path parsed as
one complete old or new cursor; no partially written canonical JSON was
observed.

The post-rename/pre-directory-fsync case is the important ambiguous boundary.
The new cursor is already visible through the filesystem namespace after the
child dies, but the directory fsync has not run. This test cannot establish
which name would survive a machine crash or power loss at that point. The
directory entry may not yet be durable even though the renamed file contents
were fsynced before rename.

This is a process-crash test only. `Process.Kill` terminates the child process
while the operating system, filesystem, kernel page cache, and storage device
remain running. It does not simulate a kernel panic, sudden power loss, loss of
volatile device cache, filesystem recovery after reboot, or physical media
failure. No claim about those outcomes follows from these observations.
Windows skips this test because it asserts Unix signal-termination semantics.

Validation:

```sh
GOWORK=off go test -race -count=5 -timeout=90s -run '^TestC2CursorProcessCrashBoundaries$' .
```
